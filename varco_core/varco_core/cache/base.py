"""
varco_core.cache.base
======================
Core abstractions for the varco cache system.

``AsyncCache[K, V]``
    Runtime-checkable ``Protocol`` for all cache backends.  Covers the
    standard get/set/delete/clear interface.  All methods are ``async def``.

``CacheBackend[K, V]``
    Abstract base class that backends subclass.  Extends ``AsyncCache`` with
    explicit ``start()`` / ``stop()`` lifecycle methods and a default
    async context manager implementation.

``InvalidationStrategy``
    Abstract base class for cache invalidation strategies.  Every strategy —
    including ``CompositeStrategy`` — has ``start()`` and ``stop()`` called by
    the hosting cache backend.  No ``hasattr`` inspection is ever needed.

Design
------
:mermaid:
    AsyncCache (Protocol)
      ↑ implemented by
    CacheBackend (ABC)
      ↑ subclassed by
    InMemoryCache   NoOpCache   RedisCache   LayeredCache

    InvalidationStrategy (ABC)
      ↑ subclassed by
    TTLStrategy   ExplicitStrategy   TaggedStrategy
    EventDrivenStrategy   CompositeStrategy

DESIGN: Protocol + ABC over single ABC
    ✅ ``isinstance(obj, AsyncCache)`` works for structural checking — useful
       in generic code that receives an unknown cache object.
    ✅ Backends inherit ``__aenter__`` / ``__aexit__`` once from ``CacheBackend``.
    ✅ Users who only depend on the protocol can type-hint ``AsyncCache[K, V]``
       without importing the ABC.
    ❌ Two classes instead of one — the ABC layer is thin but non-zero.

Thread safety:  ❌  Not thread-safe.  Use from a single event loop.
Async safety:   ✅  All public methods are ``async def``.
"""

from __future__ import annotations

import abc
import logging
from typing import TYPE_CHECKING, Any, TypeVar

from typing import Protocol, runtime_checkable

if TYPE_CHECKING:
    from varco_core.cache.warming import CacheWarmer

_logger = logging.getLogger(__name__)

K = TypeVar("K")
V = TypeVar("V")


# ── AsyncCache Protocol ────────────────────────────────────────────────────────


@runtime_checkable
class AsyncCache(Protocol[K, V]):
    """
    Structural protocol for async cache backends.

    All methods are ``async def``.  Implementations must satisfy this
    interface structurally — they do NOT need to inherit from ``AsyncCache``.

    Type parameters:
        K: Cache key type (must be hashable for most backends).
        V: Cache value type.

    Thread safety:  ❌  Not thread-safe — implementations may use shared state.
    Async safety:   ✅  All methods are coroutines.

    Edge cases:
        - ``get()`` returns ``None`` for missing keys — callers must distinguish
          a cached ``None`` value from a cache miss using ``exists()`` if needed.
        - ``set()`` with ``ttl=None`` falls back to backend default TTL.
        - ``clear()`` is a full flush — use with caution in production.
    """

    async def get(self, key: K, *, type_hint: type | None = None) -> V | None:
        """
        Return the cached value for ``key``, or ``None`` on a cache miss.

        Args:
            key:       Cache key to look up.
            type_hint: Optional type passed to the deserializer so backends that
                       serialize to bytes (e.g. Redis) can reconstruct typed objects
                       instead of plain dicts.  Ignored by in-memory backends that
                       store Python objects directly.

        Returns:
            The cached value, or ``None`` if the key is absent or expired.
        """
        ...

    async def set(self, key: K, value: V, *, ttl: float | None = None) -> None:
        """
        Store ``value`` under ``key``.

        Args:
            key:   Cache key.
            value: Value to store.
            ttl:   Time-to-live in seconds.  ``None`` → backend default.

        Raises:
            RuntimeError: If the backend has not been started.
        """
        ...

    async def delete(self, key: K) -> None:
        """
        Remove ``key`` from the cache.  Idempotent — no-op if absent.

        Args:
            key: Cache key to remove.

        Raises:
            RuntimeError: If the backend has not been started.
        """
        ...

    async def exists(self, key: K) -> bool:
        """
        Return ``True`` if ``key`` is present and not expired.

        Args:
            key: Cache key to check.

        Returns:
            ``True`` if the key exists and has not expired.

        Raises:
            RuntimeError: If the backend has not been started.
        """
        ...

    async def clear(self) -> None:
        """
        Remove ALL entries from the cache.

        Raises:
            RuntimeError: If the backend has not been started.
        """
        ...

    async def delete_prefix(self, prefix: str) -> None:
        """
        Remove all entries whose key starts with ``prefix``.

        Used by ``CacheServiceMixin`` to scope list-cache invalidation to a
        single tenant — only keys matching ``"<namespace>:<tenant_id>:list:"``
        are evicted, leaving other tenants' list entries intact.

        Args:
            prefix: Key prefix to match.  All keys where
                    ``str(key).startswith(prefix)`` is ``True`` are deleted.

        Raises:
            RuntimeError: If the backend has not been started.

        Edge cases:
            - An empty ``prefix`` removes ALL entries — equivalent to
              ``clear()``.  Callers should guard against this.
            - No-op if no keys match — never raises on an empty result set.
        """
        ...


# ── CacheBackend ABC ───────────────────────────────────────────────────────────


class CacheBackend(abc.ABC):
    """
    Abstract base class for cache backends.

    Extends ``AsyncCache`` semantics with explicit lifecycle management
    (``start()`` / ``stop()``) and a default async context manager.

    All concrete backends inherit ``__aenter__`` / ``__aexit__`` for free —
    no need to re-implement the context manager pattern in each backend.

    Thread safety:  ❌  Not thread-safe.  Subclass docstrings specify safety.
    Async safety:   ✅  All public methods are ``async def``.

    Lifecycle::

        backend = MyBackend(settings)
        await backend.start()
        await backend.set("key", value)
        result = await backend.get("key")
        await backend.stop()

        # Or via context manager:
        async with MyBackend(settings) as backend:
            await backend.set("key", value)
    """

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    @abc.abstractmethod
    async def start(self) -> None:
        """
        Initialise the backend (connect to broker, allocate resources).

        Raises:
            RuntimeError: If already started.
        """

    @abc.abstractmethod
    async def stop(self) -> None:
        """Close connections and release resources.  Idempotent."""

    # ── Warmer registry ────────────────────────────────────────────────────────
    #
    # DESIGN: list stored on the instance, initialised lazily on first add_warmer()
    # call rather than in __init__.  CacheBackend has no __init__ of its own —
    # adding one would force all subclasses to call super().__init__().  The lazy
    # approach is non-breaking: if no warmer is ever added, the attribute is never
    # created and _run_warmers() is a no-op.
    #
    # Thread safety: add_warmer() is not thread-safe — call before start().

    def add_warmer(self, warmer: CacheWarmer) -> None:
        """
        Register a ``CacheWarmer`` to run after ``start()`` completes.

        Warmers are executed in registration order inside ``__aenter__``.
        Must be called BEFORE the backend is started (before ``__aenter__``
        or ``start()``).

        Args:
            warmer: The ``CacheWarmer`` to register.

        Edge cases:
            - Adding the same warmer instance twice causes it to run twice.
            - Warmers added after ``start()`` will NOT run for the current
              lifecycle — they run on the next ``start()``.

        Thread safety:  ⚠️ Not thread-safe — call from a single coroutine
                            before starting the backend.
        """
        if not hasattr(self, "_warmers"):
            # Lazy initialisation — see DESIGN note above.
            object.__setattr__(self, "_warmers", [])
        self._warmers.append(warmer)  # type: ignore[attr-defined]

    async def _run_warmers(self) -> None:
        """
        Execute all registered warmers sequentially, catching and logging errors.

        Called by ``__aenter__`` after ``start()`` completes.  A failed warmer
        is logged as a warning and skipped — startup is NOT aborted, because a
        warm cache is a performance optimisation, not a correctness requirement.

        Edge cases:
            - No warmers registered → instant no-op (attribute may not exist).
            - A warmer that raises logs at WARNING level and is skipped.
            - Individual warmer exceptions do NOT propagate — see the design note
              above for the rationale.

        Async safety:   ✅ Sequential ``await`` on each warmer.
        """
        warmers: list[CacheWarmer] = getattr(self, "_warmers", [])
        for warmer in warmers:
            try:
                _logger.info("CacheBackend: running warmer %r", warmer)
                await warmer.warm(self)
            except Exception as exc:
                # Warming failure is non-fatal — a cold cache is worse than
                # a missing warm, but it should not prevent the service from
                # starting.  Log at WARNING so operators are alerted.
                _logger.warning(
                    "CacheBackend: warmer %r failed (will start with cold cache): %s",
                    warmer,
                    exc,
                    exc_info=True,
                )

    async def __aenter__(self) -> CacheBackend:
        await self.start()
        # Run warmers AFTER start() — the backend is fully operational at this point.
        await self._run_warmers()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.stop()

    # ── Cache operations ───────────────────────────────────────────────────────

    @abc.abstractmethod
    async def get(self, key: Any, *, type_hint: type | None = None) -> Any | None:
        """Return cached value for ``key``, or ``None`` on miss.
        ``type_hint`` is forwarded to the deserializer for backends that serialize
        to bytes — in-memory backends may ignore it."""

    @abc.abstractmethod
    async def set(self, key: Any, value: Any, *, ttl: float | None = None) -> None:
        """Store ``value`` under ``key``.  ``ttl`` overrides backend default."""

    @abc.abstractmethod
    async def delete(self, key: Any) -> None:
        """Remove ``key``.  Idempotent — no-op if absent."""

    @abc.abstractmethod
    async def exists(self, key: Any) -> bool:
        """Return ``True`` if ``key`` is present and not expired."""

    @abc.abstractmethod
    async def clear(self) -> None:
        """Remove ALL entries from this backend."""

    @abc.abstractmethod
    async def delete_prefix(self, prefix: str) -> None:
        """
        Remove all entries whose key starts with ``prefix``.

        Args:
            prefix: Key prefix to match.  Keys where
                    ``str(key).startswith(prefix)`` are removed.

        Edge cases:
            - An empty ``prefix`` is equivalent to ``clear()``.
        """


# ── InvalidationStrategy ABC ───────────────────────────────────────────────────


class InvalidationStrategy(abc.ABC):
    """
    Abstract base class for cache invalidation strategies.

    Every strategy must implement ``start()`` and ``stop()`` — even if they
    are no-ops.  This keeps the lifecycle model uniform and eliminates
    ``hasattr`` inspection in cache backends.

    Strategies are passed to a ``CacheBackend`` at construction time.  The
    backend calls ``start()`` / ``stop()`` during its own lifecycle so
    strategies are always active while the cache is running.

    Multiple strategies are composed via ``CompositeStrategy``:

        strategy = CompositeStrategy(
            TTLStrategy(default_ttl=300),
            EventDrivenStrategy(bus, channel="cache-invalidations"),
        )
        async with InMemoryCache(strategy=strategy) as cache:
            ...

    Thread safety:  ❌  Not thread-safe — strategies may hold mutable state.
    Async safety:   ✅  ``start()`` / ``stop()`` are ``async def``.

    Edge cases:
        - A strategy that does nothing (e.g. a pure TTL backend where TTL
          is enforced by the storage engine itself) can implement ``start()``
          and ``stop()`` as no-ops — just ``pass``.
        - ``should_invalidate()`` is optional — not all strategies can answer
          this synchronously.  Use ``CompositeStrategy`` to combine ones that can.
    """

    @abc.abstractmethod
    async def start(self) -> None:
        """
        Activate the strategy.  Called by the host ``CacheBackend.start()``.

        Raises:
            RuntimeError: If already started.
        """

    @abc.abstractmethod
    async def stop(self) -> None:
        """
        Deactivate the strategy.  Called by the host ``CacheBackend.stop()``.
        Idempotent.
        """

    @abc.abstractmethod
    def should_invalidate(self, key: Any, metadata: dict[str, Any]) -> bool:
        """
        Return ``True`` if the entry identified by ``key`` should be evicted.

        This is a synchronous check called at read-time by the cache backend.
        Strategies that require async I/O (e.g. event-driven) should maintain
        a local invalidated-keys set that is updated asynchronously in ``start()``
        and checked synchronously here.

        Args:
            key:      The cache key being looked up.
            metadata: Backend-provided metadata dict.  Common keys:
                      ``"stored_at"`` (float UNIX timestamp),
                      ``"ttl"`` (float seconds or ``None``),
                      ``"tags"`` (set of strings).

        Returns:
            ``True`` if the entry should be evicted; ``False`` to keep it.
        """


__all__ = [
    "AsyncCache",
    "CacheBackend",
    "InvalidationStrategy",
]
