"""
Unit tests for tenant-aware cache keys and invalidation
========================================================
Covers:

- ``CacheServiceMixin._cache_key()`` — tenant_id included/excluded in key.
- ``CacheServiceMixin._cache_list_key()`` — tenant_id scopes the list key.
- ``InMemoryCache.delete_prefix()`` — prefix-filtered deletion.
- ``CacheServiceMixin.get()`` — cache hit/miss isolation between tenants.
- ``CacheServiceMixin.create()`` — list invalidation scoped to calling tenant.

Test doubles
------------
All doubles are plain Python — no mocking library.

    ``InMemoryPostRepository`` / ``PostAssembler`` — same pattern as
    ``test_service.py``.
    ``CachedPostService`` — composes ``CacheServiceMixin`` + ``AsyncService``.
    ``AllowAllAuthorizer``  — allows every authorization check.

DESIGN: manual ClassVar injection
    ``CacheServiceMixin._cache`` is a ``ClassVar[Inject[CacheBackend]]``.
    In tests we bypass providify and set it directly on the concrete class
    (``CachedPostService._cache = cache``).  This is fine — providify uses
    ``setattr`` internally.  Each test uses a fresh ``InMemoryCache`` and
    resets the class-level attribute via the fixture.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Annotated, Any

import pytest

from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth import AbstractAuthorizer, Action, AuthContext, Resource
from varco_core.cache.mixin import CacheServiceMixin
from varco_core.cache.memory import InMemoryCache
from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO
from varco_core.event.producer import NoopEventProducer
from varco_core.meta import PKStrategy, PrimaryKey, pk_field
from varco_core.model import AuditedDomainModel
from varco_core.query.params import QueryParams
from varco_core.repository import AsyncRepository
from varco_core.service import AsyncService, IUoWProvider
from varco_core.uow import AsyncUnitOfWork


# ── Sentinel timestamp ─────────────────────────────────────────────────────────

_SENTINEL_TS: datetime = datetime(2026, 1, 1, 0, 0, 0)


# ── Domain entity ──────────────────────────────────────────────────────────────


@dataclass
class Post(AuditedDomainModel):
    """
    Minimal domain entity for tenant cache tests.

    ``STR_ASSIGNED`` pk so we can control pk values in fixtures.
    """

    pk: Annotated[str, PrimaryKey(strategy=PKStrategy.STR_ASSIGNED)] = pk_field(
        init=True
    )
    title: str = ""

    class Meta:
        table = "posts"


# ── DTOs ───────────────────────────────────────────────────────────────────────


class CreatePostDTO(CreateDTO):
    title: str


class PostReadDTO(ReadDTO):
    title: str


class UpdatePostDTO(UpdateDTO):
    title: str | None = None


# ── Assembler ──────────────────────────────────────────────────────────────────


class PostAssembler(
    AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]
):
    """Stateless DTO assembler — converts between Post and its DTOs."""

    def to_domain(self, dto: CreatePostDTO) -> Post:
        return Post(title=dto.title)

    def to_read_dto(self, entity: Post) -> PostReadDTO:
        return PostReadDTO(
            pk=entity.pk,
            title=entity.title,
            created_at=entity.created_at or _SENTINEL_TS,
            updated_at=entity.updated_at or _SENTINEL_TS,
        )

    def apply_update(self, entity: Post, dto: UpdatePostDTO) -> Post:
        return replace(
            entity,
            title=dto.title if dto.title is not None else entity.title,
        )


# ── In-memory repository ───────────────────────────────────────────────────────


class InMemoryPostRepository(AsyncRepository[Post, str]):
    """Dict-backed repository; assigns pk on save if absent."""

    def __init__(self) -> None:
        self._store: dict[str, Post] = {}
        self._counter = 0

    async def find_by_id(self, pk: str) -> Post | None:
        return self._store.get(pk)

    async def find_all(self) -> list[Post]:
        return list(self._store.values())

    async def find_by_query(self, params: QueryParams) -> list[Post]:
        return list(self._store.values())

    async def count(self, params: QueryParams | None = None) -> int:
        return len(self._store)

    async def exists(self, pk: str) -> bool:
        return pk in self._store

    async def stream_by_query(self, params: QueryParams):  # type: ignore[override]
        for entity in list(self._store.values()):
            yield entity

    # ── Bulk stubs (required by AsyncRepository ABC) ──────────────────────────

    async def save_many(self, entities):  # type: ignore[override]
        return [await self.save(e) for e in entities]

    async def delete_many(self, entities):  # type: ignore[override]
        for e in entities:
            await self.delete(e)

    async def update_many_by_query(self, params, update):  # type: ignore[override]
        raise NotImplementedError

    async def save(self, entity: Post) -> Post:
        if entity.pk is None:
            self._counter += 1
            entity.pk = str(self._counter)
        entity.created_at = entity.created_at or _SENTINEL_TS
        entity.updated_at = _SENTINEL_TS
        self._store[entity.pk] = entity
        return entity

    async def delete(self, entity: Post) -> None:
        self._store.pop(entity.pk, None)


# ── In-memory UoW ─────────────────────────────────────────────────────────────


class InMemoryUoW(AsyncUnitOfWork):
    """Thin UoW wrapper for tests — wraps the shared repository."""

    def __init__(self, repo: InMemoryPostRepository) -> None:
        self._repo = repo

    async def _begin(self) -> None:
        pass

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    def get_repository(self, *_: Any) -> InMemoryPostRepository:
        return self._repo


class FakeUoWProvider(IUoWProvider):
    def __init__(self, repo: InMemoryPostRepository) -> None:
        self._repo = repo

    def make_uow(self) -> InMemoryUoW:
        return InMemoryUoW(self._repo)


# ── Allow-all authorizer ───────────────────────────────────────────────────────


class AllowAllAuthorizer(AbstractAuthorizer):
    async def authorize(
        self, ctx: AuthContext, action: Action, resource: Resource
    ) -> None:
        return


# ── Concrete cached service ────────────────────────────────────────────────────


class CachedPostService(
    CacheServiceMixin[Post, str, CreatePostDTO, PostReadDTO, UpdatePostDTO],
    AsyncService[Post, str, CreatePostDTO, PostReadDTO, UpdatePostDTO],
):
    """
    Post service that composes ``CacheServiceMixin`` with ``AsyncService``.

    ``CacheServiceMixin`` must be leftmost in the MRO — it intercepts
    ``get()`` / ``list()`` / ``create()`` / ``update()`` / ``delete()``
    and delegates to ``super()`` (which resolves to ``AsyncService``).

    ``_cache`` is injected manually in tests via ``CachedPostService._cache = ...``.
    """

    _cache_namespace: str = "post"

    def __init__(self, uow_provider: FakeUoWProvider) -> None:
        super().__init__(
            uow_provider=uow_provider,
            authorizer=AllowAllAuthorizer(),
            assembler=PostAssembler(),
        )
        # Use a no-op producer so domain events don't interfere with cache tests.
        self._producer = NoopEventProducer()

    def _get_repo(self, uow: AsyncUnitOfWork) -> InMemoryPostRepository:
        return uow.get_repository(Post)


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture()
async def cache() -> InMemoryCache:
    """
    Fresh started ``InMemoryCache`` for each test.

    Yielded inside an ``async with`` block so ``start()`` / ``stop()`` are
    called automatically — prevents the "not started" RuntimeError.
    """
    async with InMemoryCache() as c:
        yield c


@pytest.fixture()
def repo() -> InMemoryPostRepository:
    """Fresh in-memory repository with no pre-seeded data."""
    return InMemoryPostRepository()


@pytest.fixture()
def svc(repo: InMemoryPostRepository, cache: InMemoryCache) -> CachedPostService:
    """
    ``CachedPostService`` with the test cache injected via ClassVar.

    The ClassVar assignment is class-level, so we reset it to ``None`` after
    the test to avoid leaking state into other tests.
    """
    # Bypass providify — set the ClassVar directly (same as what the container does).
    CachedPostService._cache = cache  # type: ignore[assignment]
    CachedPostService._cache_producer = None  # type: ignore[assignment]
    return CachedPostService(FakeUoWProvider(repo))


@pytest.fixture()
def seeded_post(repo: InMemoryPostRepository) -> Post:
    """Pre-seed a Post with a known pk so update/delete tests can find it."""
    post = Post(pk="seed-1", title="original")
    repo._store["seed-1"] = post
    return post


# ── _cache_key unit tests ─────────────────────────────────────────────────────


class TestCacheKey:
    """Unit tests for ``CacheServiceMixin._cache_key()``."""

    def test_key_without_tenant_is_backward_compatible(
        self, svc: CachedPostService
    ) -> None:
        """
        Keys built without ``tenant_id`` keep the pre-existing format
        ``"<namespace>:<operation>:<suffix>"``.

        This preserves backward compatibility for single-tenant services that
        never pass a ``tenant_id``.
        """
        key = svc._cache_key("get", "123")
        assert key == "post:get:123"

    def test_key_with_tenant_includes_tenant_segment(
        self, svc: CachedPostService
    ) -> None:
        """
        Keys built with ``tenant_id`` follow
        ``"<namespace>:<tenant_id>:<operation>:<suffix>"``.

        The tenant segment is second so ``delete_prefix("<ns>:<tid>:list:")``
        can scope list invalidation without touching other tenants' entries.
        """
        key = svc._cache_key("get", "123", tenant_id="acme")
        assert key == "post:acme:get:123"

    def test_different_tenants_produce_different_keys(
        self, svc: CachedPostService
    ) -> None:
        """
        The same pk for two different tenants produces two distinct keys —
        no cross-tenant cache collision.
        """
        acme_key = svc._cache_key("get", "1", tenant_id="acme")
        globex_key = svc._cache_key("get", "1", tenant_id="globex")
        assert acme_key != globex_key

    def test_list_key_with_tenant(self, svc: CachedPostService) -> None:
        """
        ``_cache_list_key()`` with a ``tenant_id`` embeds the tenant between
        the namespace and the ``"list"`` segment.
        """
        params = QueryParams()
        key = svc._cache_list_key(params, tenant_id="acme")
        # Key must start with the tenant-scoped namespace prefix.
        assert key.startswith("post:acme:list:")

    def test_list_key_without_tenant_has_no_tenant_segment(
        self, svc: CachedPostService
    ) -> None:
        """
        ``_cache_list_key()`` without ``tenant_id`` keeps the original
        ``"<namespace>:list:<hash>"`` format.
        """
        params = QueryParams()
        key = svc._cache_list_key(params)
        assert key.startswith("post:list:")
        # Tenant segment must NOT be present.
        parts = key.split(":")
        assert parts[1] == "list"


# ── InMemoryCache.delete_prefix unit tests ───────────────────────────────────


class TestInMemoryCacheDeletePrefix:
    """
    Unit tests for ``InMemoryCache.delete_prefix()``.

    These tests exercise the backend directly — they do NOT go through the
    service layer.  They verify the contract that only prefix-matching keys
    are removed.
    """

    async def test_deletes_matching_keys(self) -> None:
        """All keys starting with the prefix are removed."""
        async with InMemoryCache() as cache:
            await cache.set("post:acme:list:aaa", ["a"])
            await cache.set("post:acme:list:bbb", ["b"])
            await cache.set("post:globex:list:ccc", ["c"])

            await cache.delete_prefix("post:acme:list:")

            assert await cache.get("post:acme:list:aaa") is None
            assert await cache.get("post:acme:list:bbb") is None
            # Globex entry must survive.
            assert await cache.get("post:globex:list:ccc") is not None

    async def test_no_op_when_no_keys_match(self) -> None:
        """Calling ``delete_prefix`` with no matching keys does not raise."""
        async with InMemoryCache() as cache:
            await cache.set("post:acme:get:1", "data")
            # This prefix matches nothing.
            await cache.delete_prefix("post:globex:list:")
            # Unrelated key must still be present.
            assert await cache.get("post:acme:get:1") == "data"

    async def test_empty_prefix_removes_all(self) -> None:
        """
        An empty prefix matches every key — equivalent to ``clear()``.

        This is documented behaviour: callers should guard against an empty
        ``prefix`` to avoid accidental full-cache flush.
        """
        async with InMemoryCache() as cache:
            await cache.set("a", 1)
            await cache.set("b", 2)
            await cache.delete_prefix("")
            assert await cache.get("a") is None
            assert await cache.get("b") is None

    async def test_does_not_mutate_during_iteration(self) -> None:
        """
        Internally, ``delete_prefix`` must not mutate the dict while iterating
        (would raise ``RuntimeError: dictionary changed size during iteration``).

        This test exercises the bug scenario by populating several keys and
        deleting them all in one call.
        """
        async with InMemoryCache() as cache:
            for i in range(20):
                await cache.set(f"post:acme:list:{i:03d}", i)
            # Must not raise.
            await cache.delete_prefix("post:acme:list:")
            assert cache.size == 0

    async def test_raises_if_not_started(self) -> None:
        """
        ``delete_prefix()`` raises ``RuntimeError`` on an unstarted cache —
        consistent with the contract on all other ``CacheBackend`` methods.
        """
        cache = InMemoryCache()
        with pytest.raises(RuntimeError, match="not started"):
            await cache.delete_prefix("post:acme:")


# ── CacheServiceMixin tenant isolation integration tests ──────────────────────


class TestCacheServiceMixinTenantIsolation:
    """
    Integration tests that verify end-to-end tenant isolation through the
    service layer.  Each test uses a real ``InMemoryCache`` and a concrete
    ``CachedPostService``.
    """

    async def test_get_isolates_tenants(
        self, svc: CachedPostService, repo: InMemoryPostRepository
    ) -> None:
        """
        A cached ``get`` for tenant "acme" is not visible to tenant "globex".

        Both tenants query the same PK — they must receive independent cache
        entries.
        """
        # Seed a post accessible to both tenants.
        post = Post(pk="shared-1", title="shared")
        repo._store["shared-1"] = post

        ctx_acme = AuthContext(metadata={"tenant_id": "acme"})
        _ = AuthContext(metadata={"tenant_id": "globex"})

        # First call for "acme" — populates the cache.
        result_acme = await svc.get("shared-1", ctx_acme)
        assert result_acme.pk == "shared-1"

        # Verify that the key for "globex" is absent (cold cache miss).
        globex_key = svc._cache_key("get", "shared-1", tenant_id="globex")
        assert await svc._cache.get(globex_key) is None

        # Verify the "acme" key is present.
        acme_key = svc._cache_key("get", "shared-1", tenant_id="acme")
        assert await svc._cache.get(acme_key) is not None

    async def test_create_only_invalidates_own_tenant_list(
        self, svc: CachedPostService
    ) -> None:
        """
        A ``create()`` by tenant "acme" only evicts "acme" list entries.

        Tenant "globex" has its own list entry that must survive untouched.

        This is the central correctness property of per-tenant list
        invalidation — without it, creating one post for "acme" would thrash
        "globex" caches unnecessarily.
        """
        params = QueryParams()

        # Manually seed list cache entries for both tenants.
        acme_list_key = svc._cache_list_key(params, tenant_id="acme")
        globex_list_key = svc._cache_list_key(params, tenant_id="globex")
        await svc._cache.set(acme_list_key, ["acme-item"])
        await svc._cache.set(globex_list_key, ["globex-item"])

        # Create a post as tenant "acme".
        ctx_acme = AuthContext(metadata={"tenant_id": "acme"})
        await svc.create(CreatePostDTO(title="new-acme-post"), ctx_acme)

        # "acme" list cache must be evicted.
        assert await svc._cache.get(acme_list_key) is None
        # "globex" list cache must survive.
        assert await svc._cache.get(globex_list_key) == ["globex-item"]

    async def test_create_without_tenant_clears_all(
        self, svc: CachedPostService
    ) -> None:
        """
        A ``create()`` without a tenant context falls back to ``cache.clear()``
        — the full-flush behaviour for single-tenant / non-tenant services.

        This ensures backward compatibility: a service that never passes a
        ``tenant_id`` is not broken by this change.
        """
        params = QueryParams()
        key_a = svc._cache_list_key(params, tenant_id="acme")
        key_b = svc._cache_list_key(params, tenant_id="globex")
        await svc._cache.set(key_a, ["a"])
        await svc._cache.set(key_b, ["b"])

        # Create with no tenant in context.
        ctx = AuthContext()  # empty metadata
        await svc.create(CreatePostDTO(title="admin-post"), ctx)

        # Both entries must be gone — full clear was used.
        assert await svc._cache.get(key_a) is None
        assert await svc._cache.get(key_b) is None

    async def test_delete_evicts_only_own_tenant_get_and_list(
        self,
        svc: CachedPostService,
        repo: InMemoryPostRepository,
    ) -> None:
        """
        ``delete()`` evicts only the deleting tenant's get key and list
        entries.  Another tenant's cached entry for the same pk is preserved.

        DESIGN: In a real multi-tenant system, tenants would never share the
        same entity pk.  This test uses a shared pk purely to verify that the
        cache key namespacing works correctly.
        """
        # Seed in repo so delete() can find the entity.
        post = Post(pk="del-1", title="to-delete")
        repo._store["del-1"] = post

        # Manually prime both tenants' get caches for pk "del-1".
        acme_get_key = svc._cache_key("get", "del-1", tenant_id="acme")
        globex_get_key = svc._cache_key("get", "del-1", tenant_id="globex")
        await svc._cache.set(acme_get_key, "acme-cached")
        await svc._cache.set(globex_get_key, "globex-cached")

        # Also prime list keys.
        params = QueryParams()
        acme_list_key = svc._cache_list_key(params, tenant_id="acme")
        globex_list_key = svc._cache_list_key(params, tenant_id="globex")
        await svc._cache.set(acme_list_key, ["acme-list"])
        await svc._cache.set(globex_list_key, ["globex-list"])

        # Delete as "acme".
        ctx_acme = AuthContext(metadata={"tenant_id": "acme"})
        await svc.delete("del-1", ctx_acme)

        # "acme" get and list must be evicted.
        assert await svc._cache.get(acme_get_key) is None
        assert await svc._cache.get(acme_list_key) is None

        # "globex" entries must be intact.
        assert await svc._cache.get(globex_get_key) == "globex-cached"
        assert await svc._cache.get(globex_list_key) == ["globex-list"]
