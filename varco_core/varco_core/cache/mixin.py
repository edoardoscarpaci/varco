"""
varco_core.cache.mixin
=======================
``CacheServiceMixin`` — look-aside caching for ``AsyncService`` subclasses.

Like ``SoftDeleteService`` and ``TenantAwareService``, this mixin is composed
via Python's MRO — it does NOT wrap the service, it *is* the service.

DI integration
--------------
``_cache`` and ``_cache_bus`` are declared as ``ClassVar[Inject[T]]`` so the
providify container sets them automatically — subclasses require **zero extra
``__init__`` parameters** beyond what ``AsyncService`` already needs::

    @Singleton
    class PostService(
        CacheServiceMixin[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO],
        AsyncService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO],
    ):
        _cache_namespace = "post"
        _cache_ttl       = 300

        def __init__(
            self,
            uow_provider: Inject[IUoWProvider],
            authorizer:   Inject[AbstractAuthorizer],
            assembler:    Inject[AbstractDTOAssembler[Post, ...]],
        ) -> None:
            super().__init__(
                uow_provider=uow_provider,
                authorizer=authorizer,
                assembler=assembler,
            )
            # No cache parameter needed — the container resolves
            # CacheBackend and (optionally) AbstractEventBus via ClassVar.

        def _get_repo(self, uow): return uow.posts

Register the backend once and all mixins share it::

    from varco_redis.cache import RedisCacheConfiguration
    from varco_core.event import BusEventProducer, InMemoryEventBus

    container = DIContainer()
    await container.ainstall(RedisCacheConfiguration)  # binds CacheBackend
    # PostService._cache is now automatically the RedisCache singleton.
    # Bind AbstractEventProducer so _cache_producer is injected automatically:
    container.bind(AbstractEventProducer, lambda: BusEventProducer(bus))

Qualifier-based selection
--------------------------
To override which backend a specific service uses, redeclare ``_cache`` with
an ``InjectMeta`` qualifier on the subclass::

    from typing import Annotated, ClassVar
    from providify import Inject, InjectMeta

    class PostService(CacheServiceMixin, AsyncService[...]):
        _cache_namespace = "post"
        # Override to select the "layered" backend explicitly:
        _cache: ClassVar[Annotated[CacheBackend, InjectMeta(qualifier="layered")]]

Runtime-scoped caches (per-request)
-------------------------------------
If the ``CacheBackend`` is ``@RequestScoped``, wrap the injection in
``Live[CacheBackend]`` so the container re-resolves it on every call::

    from typing import Annotated, ClassVar
    from providify import LiveMeta

    class SessionService(CacheServiceMixin, AsyncService[...]):
        _cache_namespace = "session"
        _cache: ClassVar[Annotated[CacheBackend, LiveMeta()]]

Programmatic backend selection with ``Instance``
-------------------------------------------------
When the namespace should determine which backend to use at call time (e.g.,
different TTLs for different entity types backed by different cache instances)::

    from typing import ClassVar
    from providify import Instance

    class PostService(CacheServiceMixin, AsyncService[...]):
        _cache_namespace = "post"
        # InstanceProxy — resolved per call, qualifier chosen dynamically:
        _cache_backends: ClassVar[Instance[CacheBackend]]

        async def _resolve_cache(self) -> CacheBackend:
            return await self._cache_backends.aget(qualifier=self._cache_namespace)

    # Then override the methods that call self._cache to use _resolve_cache() instead.

CRUD intercepts
---------------
- ``get``    — look-aside: return cached ReadDTO; on miss, call ``super().get()`` and cache
- ``list``   — look-aside: stable hash of ``QueryParams`` as key
- ``create`` — call ``super().create()`` then invalidate all list keys
- ``update`` — call ``super().update()`` then invalidate get + list keys
- ``delete`` — call ``super().delete()`` then invalidate get + list keys

Cache key format::

    # Without tenant context (single-tenant / non-tenant services):
    "<_cache_namespace>:get:<pk>"
    "<_cache_namespace>:list:<md5(repr(params))[:12]>"

    # With tenant context (multi-tenant services):
    "<_cache_namespace>:<tenant_id>:get:<pk>"
    "<_cache_namespace>:<tenant_id>:list:<md5(repr(params))[:12]>"

The ``tenant_id`` segment is extracted from ``ctx.metadata["tenant_id"]`` and
inserted as the second path component when present.  This isolates each
tenant's entries so that a write by tenant "acme" never evicts tenant
"globex" entries from the cache.

Tenant-scoped list invalidation
--------------------------------
``_invalidate_list()`` uses ``CacheBackend.delete_prefix()`` when a
``tenant_id`` is available::

    delete_prefix("<namespace>:<tenant_id>:list:")

This removes only the current tenant's list entries.  Without a tenant
context, it falls back to ``cache.clear()`` (full flush) — the original
behaviour for single-tenant / non-tenant services is preserved.

MRO composition
---------------
This mixin MUST appear first in the MRO::

    class PostService(CacheServiceMixin, SoftDeleteService, AsyncService[...]): ...

``CacheServiceMixin.get()`` calls ``super().get()`` which threads through
``SoftDeleteService._check_entity()`` → ``TenantAwareService._scoped_params()``
→ ``AsyncService.get()``.  A cached entry has all cross-cutting checks applied.

Thread safety:  ⚠️ Same contract as ``AsyncService`` singleton.
Async safety:   ✅ All overrides are ``async def``.
"""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Generic, TypeVar

from providify import Inject, InjectMeta

from varco_core.cache.base import CacheBackend
from varco_core.cache.service import CacheInvalidated
from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO
from varco_core.event.producer import AbstractEventProducer
from varco_core.model import DomainModel
from varco_core.service.base import AsyncService, _ANON_CTX
from varco_core.service.mixin import ServiceMixin

if TYPE_CHECKING:
    from varco_core.auth import AuthContext
    from varco_core.query.params import QueryParams

_logger = logging.getLogger(__name__)

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")
C = TypeVar("C", bound=CreateDTO)
R = TypeVar("R", bound=ReadDTO)
U = TypeVar("U", bound=UpdateDTO)


class CacheServiceMixin(
    ServiceMixin, AsyncService[D, PK, C, R, U], Generic[D, PK, C, R, U]
):
    """
    ``AsyncService`` mixin that adds transparent look-aside caching.

    **DI wiring** — declared as ``ClassVar[Inject[T]]`` so providify resolves
    both dependencies automatically at the class level.  Subclasses need no
    extra ``__init__`` parameters.

    Class attributes to configure per-subclass:

    - ``_cache_namespace``   *(required)* — unique key prefix, e.g. ``"post"``.
    - ``_cache_ttl``         *(optional)* — TTL in seconds; ``None`` = cache default.
    - ``_cache_bus_channel`` *(optional)* — channel for ``CacheInvalidated`` events.

    Override ``_cache`` or ``_cache_producer`` on the subclass to change the
    qualifier or injection strategy (see module docstring for examples).

    DESIGN: ``_cache_producer`` over ``_cache_bus``
        ✅ ``AbstractEventProducer`` is the correct abstraction for publishing —
           it is the only layer that is allowed to call ``bus.publish()`` directly.
        ✅ No other class in the cache layer holds a reference to the bus.
        ✅ Consistent with ``AsyncService._producer`` — the same DI binding is
           reused automatically; no extra container registration required.
        ❌ Cross-process invalidation now requires a ``BusEventProducer`` binding
           in the DI container (instead of ``AbstractEventBus``).  Both are
           typically registered together — one extra line.
    """

    # ── DI-injected class attributes ──────────────────────────────────────────
    # The providify container sets these at the class level — all instances of a
    # subclass see the same injected singleton without any __init__ change.
    #
    # To override the qualifier for a specific service:
    #   _cache: ClassVar[Annotated[CacheBackend, InjectMeta(qualifier="layered")]]
    #
    # To use a request-scoped cache (re-resolved per call):
    #   _cache: ClassVar[Annotated[CacheBackend, LiveMeta()]]

    #: Resolved by the DI container — the registered ``CacheBackend`` singleton.
    _cache: ClassVar[Inject[CacheBackend]]

    #: Optional producer for cross-process ``CacheInvalidated`` publishing.
    #: ``None`` when no ``AbstractEventProducer`` is registered in the container.
    #: Only ``AbstractEventProducer`` talks to the bus — the mixin never holds
    #: a direct reference to ``AbstractEventBus``.
    _cache_producer: ClassVar[Annotated[AbstractEventProducer, InjectMeta(optional=True)]] = None  # type: ignore[assignment]

    # ── Per-subclass configuration ────────────────────────────────────────────

    #: Required — unique prefix for all cache keys (e.g. ``"post"``, ``"user"``).
    _cache_namespace: ClassVar[str] = ""

    #: Default TTL in seconds passed to ``cache.set()``.
    #: ``None`` = rely on the cache backend's own default.
    _cache_ttl: ClassVar[float | None] = None

    #: Bus channel on which ``CacheInvalidated`` events are published.
    _cache_bus_channel: ClassVar[str] = "varco.cache.invalidations"

    # ── Internal key helpers ──────────────────────────────────────────────────

    def _cache_key(
        self,
        operation: str,
        suffix: Any,
        *,
        tenant_id: str | None = None,
    ) -> str:
        """
        Build a cache key for an operation on a specific entity (or list).

        Key format:
            - With tenant: ``"<namespace>:<tenant_id>:<operation>:<suffix>"``
            - Without:     ``"<namespace>:<operation>:<suffix>"``

        The ``tenant_id`` segment isolates each tenant's entries so reads and
        writes for tenant "acme" never collide with entries for tenant "globex".

        Args:
            operation: ``"get"`` or ``"list"``.
            suffix:    Entity PK (for ``"get"``) or a stable hash (for ``"list"``).
            tenant_id: Tenant identifier from ``ctx.metadata``.  ``None``
                       produces a non-tenant key — backward-compatible with
                       single-tenant services.

        Returns:
            Fully-qualified cache key string.
        """
        if tenant_id:
            # Tenant segment placed between namespace and operation so that
            # delete_prefix("<namespace>:<tenant_id>:list:") scopes correctly.
            return f"{self._cache_namespace}:{tenant_id}:{operation}:{suffix}"
        return f"{self._cache_namespace}:{operation}:{suffix}"

    def _cache_list_key(
        self,
        params: QueryParams,
        *,
        tenant_id: str | None = None,
    ) -> str:
        """
        Stable cache key for a ``list`` call.

        Hashes the full ``QueryParams`` repr (including any tenant-scoped
        filters added by ``TenantAwareService._scoped_params()``) and
        includes the tenant_id in the key so per-tenant list invalidation
        can target ``delete_prefix("<namespace>:<tenant_id>:list:")``.

        Args:
            params:    The query parameters for this list call.
            tenant_id: Tenant identifier from ``ctx.metadata``.  ``None``
                       produces a non-tenant key.

        Returns:
            Cache key string, e.g. ``"post:acme:list:a3f9b2c1d5e6"``.
        """
        h = hashlib.md5(repr(params).encode(), usedforsecurity=False).hexdigest()[:12]
        return self._cache_key("list", h, tenant_id=tenant_id)

    # ── Overridden read operations ────────────────────────────────────────────

    async def get(self, pk: PK, ctx: AuthContext = _ANON_CTX) -> R:
        """
        Look-aside ``get``: return cached ``ReadDTO`` or delegate to ``super().get()``.

        The cache key includes the tenant_id from ``ctx`` so tenants never
        share cached entries for the same PK.

        Args:
            pk:  Entity primary key.
            ctx: Auth context — ``ctx.metadata["tenant_id"]`` scopes the key.

        Returns:
            ``ReadDTO`` — from cache or freshly fetched.
        """
        # Extract tenant_id once — used for both the lookup key and the write key.
        tenant_id: str | None = ctx.metadata.get("tenant_id") if ctx else None
        key = self._cache_key("get", pk, tenant_id=tenant_id)
        hit = await self._cache.get(key)
        if hit is not None:
            _logger.debug(
                "CacheServiceMixin[%s]: cache hit for key %r.",
                self._cache_namespace,
                key,
            )
            return hit  # type: ignore[return-value]

        _logger.debug(
            "CacheServiceMixin[%s]: cache miss for key %r.", self._cache_namespace, key
        )
        result = await super().get(pk, ctx)  # type: ignore[misc]
        await self._cache.set(key, result, ttl=self._cache_ttl)
        return result

    async def list(self, params: QueryParams, ctx: AuthContext = _ANON_CTX) -> list[R]:
        """
        Look-aside ``list``: stable hash of ``QueryParams`` as the cache key.

        The key includes the tenant_id so that different tenants never
        share the same list cache entry even when filtering by the same fields.
        (``TenantAwareService._scoped_params()`` already injects the tenant
        filter into ``params`` before this method is called, so the hash
        naturally differs per tenant — but the explicit tenant segment in the
        key is required for ``delete_prefix``-based invalidation to work.)

        Args:
            params: Query parameters (filters, sort, pagination).
            ctx:    Auth context — ``ctx.metadata["tenant_id"]`` scopes the key.

        Returns:
            List of ``ReadDTO`` — from cache or freshly fetched.
        """
        tenant_id: str | None = ctx.metadata.get("tenant_id") if ctx else None
        key = self._cache_list_key(params, tenant_id=tenant_id)
        hit = await self._cache.get(key)
        if hit is not None:
            _logger.debug(
                "CacheServiceMixin[%s]: cache hit for list key %r.",
                self._cache_namespace,
                key,
            )
            return hit  # type: ignore[return-value]

        _logger.debug(
            "CacheServiceMixin[%s]: cache miss for list key %r.",
            self._cache_namespace,
            key,
        )
        result = await super().list(params, ctx)  # type: ignore[misc]
        await self._cache.set(key, result, ttl=self._cache_ttl)
        return result

    # ── Overridden write operations ───────────────────────────────────────────

    async def create(self, dto: C, ctx: AuthContext = _ANON_CTX) -> R:
        """
        Delegate to ``super().create()`` then flush the tenant's list cache.

        Only the creating tenant's list entries are evicted — other tenants'
        list caches are untouched.

        Args:
            dto: Create DTO.
            ctx: Auth context — tenant_id is extracted for scoped invalidation.

        Returns:
            ``ReadDTO`` of the newly created entity.
        """
        result = await super().create(dto, ctx)  # type: ignore[misc]
        tenant_id: str | None = ctx.metadata.get("tenant_id") if ctx else None
        await self._invalidate_list(tenant_id=tenant_id)
        _logger.debug(
            "CacheServiceMixin[%s]: post-create list invalidation (tenant=%r).",
            self._cache_namespace,
            tenant_id,
        )
        await self._publish_invalidated([], operation="create")
        return result

    async def update(self, pk: PK, dto: U, ctx: AuthContext = _ANON_CTX) -> R:
        """
        Delegate to ``super().update()`` then evict the get key and tenant's list cache.

        Args:
            pk:  Entity primary key.
            dto: Update DTO.
            ctx: Auth context — tenant_id scopes list invalidation.

        Returns:
            ``ReadDTO`` reflecting the updated state.
        """
        result = await super().update(pk, dto, ctx)  # type: ignore[misc]
        tenant_id: str | None = ctx.metadata.get("tenant_id") if ctx else None
        get_key = self._cache_key("get", pk, tenant_id=tenant_id)
        await self._cache.delete(get_key)
        await self._invalidate_list(tenant_id=tenant_id)
        _logger.debug(
            "CacheServiceMixin[%s]: post-update invalidation for pk=%r (tenant=%r).",
            self._cache_namespace,
            pk,
            tenant_id,
        )
        await self._publish_invalidated([get_key], operation="update")
        return result

    async def delete(self, pk: PK, ctx: AuthContext = _ANON_CTX) -> None:
        """
        Delegate to ``super().delete()`` then evict the get key and tenant's list cache.

        Args:
            pk:  Entity primary key.
            ctx: Auth context — tenant_id scopes list invalidation.
        """
        await super().delete(pk, ctx)  # type: ignore[misc]
        tenant_id: str | None = ctx.metadata.get("tenant_id") if ctx else None
        get_key = self._cache_key("get", pk, tenant_id=tenant_id)
        await self._cache.delete(get_key)
        await self._invalidate_list(tenant_id=tenant_id)
        _logger.debug(
            "CacheServiceMixin[%s]: post-delete invalidation for pk=%r (tenant=%r).",
            self._cache_namespace,
            pk,
            tenant_id,
        )
        await self._publish_invalidated([get_key], operation="delete")

    # ── Invalidation helpers ──────────────────────────────────────────────────

    async def _invalidate_list(self, *, tenant_id: str | None = None) -> None:
        """
        Flush list-cache entries for this namespace, optionally scoped to a tenant.

        List keys include a hash of ``QueryParams`` so they cannot be enumerated
        in advance.  When a ``tenant_id`` is provided we use
        ``CacheBackend.delete_prefix()`` to remove only that tenant's list
        entries — other tenants' caches are untouched.

        Without a ``tenant_id`` (non-tenant service or admin context) we fall
        back to ``cache.clear()`` — the original behaviour for single-tenant
        deployments.

        DESIGN: delete_prefix over clear
            ✅ A create/update/delete by tenant "acme" no longer evicts tenant
               "globex" list entries — avoids unnecessary cache thrashing.
            ✅ ``delete_prefix`` is implemented efficiently: SCAN+DEL in Redis,
               prefix-filtered iteration in ``InMemoryCache``.
            ❌ Requires the cache key format to put tenant_id before the
               ``"list"`` segment so the prefix is ``"<ns>:<tid>:list:"``.
               The ``_cache_key`` signature enforces this layout.

        Args:
            tenant_id: Tenant to scope the list invalidation to.  ``None``
                       falls back to a full ``cache.clear()``.
        """
        if tenant_id:
            # Scoped flush — only this tenant's list entries are evicted.
            prefix = f"{self._cache_namespace}:{tenant_id}:list:"
            await self._cache.delete_prefix(prefix)
        else:
            # No tenant context — fall back to full clear (single-tenant path).
            await self._cache.clear()

    async def _publish_invalidated(self, keys: list[str], *, operation: str) -> None:
        """
        Publish a ``CacheInvalidated`` event via the injected producer (if any).

        No-ops when ``_cache_producer`` is ``None`` (the default when no
        ``AbstractEventProducer`` is registered in the container).

        The mixin never holds a reference to ``AbstractEventBus`` directly —
        all bus interaction is delegated to ``_cache_producer._produce()``,
        keeping the bus-access boundary at the producer layer.

        Args:
            keys:      Cache keys being invalidated.
            operation: Mutation that triggered this (``"create"`` / ``"update"``
                       / ``"delete"``).

        Edge cases:
            - If ``_cache_producer`` raises, the exception is caught and logged
              at WARNING level so that invalidation publish failures never break
              the service call.
        """
        producer = self._cache_producer  # ClassVar — None if not injected
        if producer is None:
            return
        try:
            # _produce() is the internal publishing method on AbstractEventProducer.
            # The mixin calls it here because it is infrastructure code wired
            # tightly to the producer abstraction — consistent with how
            # AsyncService calls self._producer._produce() for domain events.
            await producer._produce(
                CacheInvalidated(
                    keys=keys,
                    namespace=self._cache_namespace,
                    operation=operation,
                ),
                channel=self._cache_bus_channel,
            )
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "CacheServiceMixin[%s]: failed to publish CacheInvalidated: %s",
                self._cache_namespace,
                exc,
                exc_info=True,
            )


__all__ = ["CacheServiceMixin"]
