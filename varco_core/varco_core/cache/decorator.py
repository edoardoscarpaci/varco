"""
varco_core.cache.decorator
===========================
``@cached`` — async look-aside cache decorator.

Analogous to ``functools.lru_cache`` / ``functools.cache`` but for coroutines
and backed by any ``CacheBackend`` (``InMemoryCache``, ``LayeredCache``,
``RedisCache``, …).

Usage — module-level cache (simplest)
--------------------------------------
::

    from varco_core.cache import InMemoryCache, LayeredCache, TTLStrategy, cached
    from varco_redis.cache import RedisCache, RedisCacheSettings

    # Declare once at module level (or in application startup)
    _cache = LayeredCache(
        InMemoryCache(strategy=TTLStrategy(60)),
        RedisCache(RedisCacheSettings(key_prefix="users:")),
        promote_ttl=60,
    )

    @cached(_cache, ttl=300, namespace="users")
    async def get_user(user_id: int) -> dict:
        return await db.fetch_one("SELECT * FROM users WHERE id = $1", user_id)

    # Cache is checked transparently; DB is hit only on a miss.
    user = await get_user(42)

    # Invalidate a specific call's result:
    await get_user.invalidate(42)

    # Invalidate all entries cached by this function:
    await get_user.invalidate_all()

Usage — instance method with ``self._cache``
----------------------------------------------
::

    class PostRepository:
        def __init__(self, cache: CacheBackend) -> None:
            self._cache = cache

        @cached(lambda self: self._cache, ttl=120, namespace="posts")
        async def find_by_id(self, post_id: int) -> dict | None:
            return await db.fetch_one("SELECT * FROM posts WHERE id = $1", post_id)

Key generation
--------------
By default the cache key is::

    "<namespace>:<md5(repr(args[1:]) + repr(sorted(kwargs.items())))[:12]>"

where ``args[0]`` (``self`` / ``cls``) is excluded from the hash.

Override with a custom key callable::

    @cached(_cache, key=lambda post_id: f"post:{post_id}", namespace="posts")
    async def get_post(post_id: int) -> dict:
        ...

    # For methods:
    @cached(lambda self: self._cache, key=lambda self, post_id: f"post:{post_id}")
    async def get_post(self, post_id: int) -> dict:
        ...

Invalidation helpers
---------------------
Both helpers are attached to the wrapper function:

- ``wrapper.invalidate(*args, **kwargs)`` — evict the entry for these
  specific arguments (uses the same key function as the decorator).
- ``wrapper.invalidate_all()`` — call ``cache.clear()`` to flush all entries
  managed by this backend.

Caveats
-------
- ``None`` return values are NOT cached — a ``None`` result always triggers
  a fresh call on the next access.  This prevents a missed DB row from
  being permanently cached.
- The cache must be started before the decorated function is called.  The
  decorator itself never calls ``cache.start()``.
- Thread / async safety inherits from the underlying ``CacheBackend``.
"""

from __future__ import annotations

import functools
import hashlib
import logging
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

from varco_core.cache.base import CacheBackend

_logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Coroutine[Any, Any, Any]])

# Sentinel — distinguishes "no cache argument provided" from ``None``.
_MISSING = object()


# ── Public decorator ──────────────────────────────────────────────────────────


def cached(
    cache: CacheBackend | Callable[..., CacheBackend],
    *,
    key: str | Callable[..., str] | None = None,
    ttl: float | None = None,
    namespace: str = "",
) -> Callable[[F], F]:
    """
    Async look-aside cache decorator.

    Args:
        cache:     A started ``CacheBackend`` instance **or** a callable that
                   accepts the decorated function's first argument (``self`` /
                   ``cls``) and returns the backend.  Use the callable form for
                   instance methods whose cache is stored on ``self``.
        key:       Cache key strategy.  Three options:

                   - ``None`` *(default)* — auto-generated from ``namespace``
                     (or function ``__qualname__``) plus an MD5 hash of the
                     call arguments (excluding ``self`` / ``cls``).
                   - A plain ``str`` — the *same* key is used for every call.
                     Only useful when the function is called with identical
                     arguments every time.
                   - A ``callable`` with the *same signature as the decorated
                     function* — return the desired key string.

        ttl:       Per-entry TTL in seconds passed to ``cache.set()``.
                   ``None`` → fallback to the cache backend's own default.
        namespace: Key prefix.  If omitted the function's ``__qualname__``
                   (``module.ClassName.method``) is used so keys from
                   different functions never collide.

    Returns:
        A decorated coroutine function with two extra attributes:

        - ``invalidate(*args, **kwargs)`` — evict the entry for these
          specific call arguments.
        - ``invalidate_all()`` — flush all entries managed by this backend
          (calls ``cache.clear()``).

    Example::

        @cached(_cache, ttl=300, namespace="users")
        async def get_user(user_id: int) -> dict:
            return await db.fetch_user(user_id)

        user = await get_user(42)
        await get_user.invalidate(42)   # evict user 42
        await get_user.invalidate_all() # flush entire cache
    """

    def decorator(func: F) -> F:
        ns = namespace or f"{func.__module__}.{func.__qualname__}"

        # ── Cache resolver ────────────────────────────────────────────────────

        def _resolve_cache(args: tuple[Any, ...]) -> CacheBackend:
            if callable(cache) and not isinstance(cache, CacheBackend):
                # Factory form — pass first arg (self/cls) to get the backend
                return cache(args[0]) if args else cache()  # type: ignore[return-value]
            return cache  # type: ignore[return-value]

        # ── Key builder ───────────────────────────────────────────────────────

        def _build_key(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
            if key is None:
                # Auto: hash args (skip self/cls) + sorted kwargs
                call_args = args[1:] if _looks_like_method(func) else args
                raw = repr(call_args) + repr(sorted(kwargs.items()))
                h = hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()[:12]
                return f"{ns}:{h}"
            if callable(key):
                return key(*args, **kwargs)
            # Plain string — same key for every call
            return key

        # ── Wrapper ───────────────────────────────────────────────────────────

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            _cache = _resolve_cache(args)
            cache_key = _build_key(args, kwargs)

            cached_val = await _cache.get(cache_key)
            if cached_val is not None:
                _logger.debug("@cached[%s]: hit for key %r.", ns, cache_key)
                return cached_val

            _logger.debug(
                "@cached[%s]: miss for key %r, calling function.", ns, cache_key
            )
            result = await func(*args, **kwargs)
            if result is not None:
                await _cache.set(cache_key, result, ttl=ttl)
            return result

        # ── Invalidation helpers (attached to wrapper) ────────────────────────

        async def invalidate(*args: Any, **kwargs: Any) -> None:
            """Evict the cached result for these specific call arguments."""
            _cache = _resolve_cache(args)
            cache_key = _build_key(args, kwargs)
            await _cache.delete(cache_key)
            _logger.debug("@cached[%s]: invalidated key %r.", ns, cache_key)

        async def invalidate_all() -> None:
            """Flush all entries managed by this cache backend."""
            # Resolve cache without arguments — must be module-level backend
            _cache = cache if isinstance(cache, CacheBackend) else cache(None)  # type: ignore[arg-type]
            await _cache.clear()
            _logger.debug("@cached[%s]: invalidate_all() called.", ns)

        wrapper.invalidate = invalidate  # type: ignore[attr-defined]
        wrapper.invalidate_all = invalidate_all  # type: ignore[attr-defined]
        wrapper.__cache__ = cache  # type: ignore[attr-defined]  — for introspection

        return wrapper  # type: ignore[return-value]

    return decorator


# ── Helpers ───────────────────────────────────────────────────────────────────


def _looks_like_method(func: Callable[..., Any]) -> bool:
    """
    Heuristic: return True when ``func`` looks like an instance/class method.

    Checks whether the first parameter of ``func`` is named ``self`` or ``cls``.
    This is used to exclude ``self`` / ``cls`` from the auto-generated cache key
    so that ``get_user(42)`` and ``other_instance.get_user(42)`` share the same
    key when the backend is the same object.

    Returns ``False`` for plain functions and static methods.
    """
    import inspect

    try:
        params = list(inspect.signature(func).parameters)
    except (ValueError, TypeError):
        return False
    return bool(params) and params[0] in ("self", "cls")


__all__ = ["cached"]
