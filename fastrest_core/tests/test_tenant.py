"""
Unit tests for fastrest_core.tenant
=====================================
Covers: current_tenant, tenant_context, TenantUoWProvider, TenantAwareService.

Test structure
--------------
Fake test doubles (no mocking library at the service layer):

    TenantPost             — DomainModel with a ``tenant_id`` field.
    TenantPostAssembler    — maps TenantPost ↔ DTOs.  Does NOT set tenant_id
                             in to_domain() — verifies the service stamps it.
    InMemoryTenantRepo     — in-memory AsyncRepository that records the last
                             QueryParams it received, so tests can assert that
                             a scoped filter node was built.
    InMemoryTenantUoW      — wraps the repo as ``uow.posts``.
    FakeTenantUoWProvider  — shares one InMemoryTenantRepo across all UoW
                             instances and counts make_uow() calls.
    ConcreteTenantService  — concrete TenantAwareService subclass.
    DenyAllAuthorizer      — raises ServiceAuthorizationError unconditionally.

For TenantUoWProvider routing tests MagicMock is used as the backing
RepositoryProvider — only make_uow() is exercised so no real backend is needed.

DESIGN: uow_created_count for "check before DB" assertions
    _require_tenant() fires before make_uow() for all five operations.
    Asserting uow_created_count == 0 after a missing-tenant failure directly
    verifies that no DB connection was opened for unauthenticated callers.

DESIGN: ServiceNotFoundError (404) for cross-tenant access
    Tests verify that cross-tenant get/update/delete raises ServiceNotFoundError,
    not ServiceAuthorizationError — the 404 contract prevents existence oracles.

Thread safety:  N/A (single-threaded unit tests)
Async safety:   ✅ pytest-asyncio with asyncio_mode = "auto"
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Annotated, Any
from unittest.mock import MagicMock

import pytest

from fastrest_core.assembler import AbstractDTOAssembler
from fastrest_core.auth import AbstractAuthorizer, Action, AuthContext, Resource
from fastrest_core.auth.authorizer import BaseAuthorizer
from fastrest_core.dto import CreateDTO, ReadDTO, UpdateDTO
from fastrest_core.exception.service import (
    ServiceAuthorizationError,
    ServiceNotFoundError,
)
from fastrest_core.meta import PKStrategy, PrimaryKey, pk_field
from fastrest_core.model import AuditedDomainModel
from fastrest_core.providers import RepositoryProvider
from fastrest_core.query.params import QueryParams
from fastrest_core.repository import AsyncRepository
from fastrest_core.service import IUoWProvider
from fastrest_core.service.tenant import (
    TenantAwareService,
    TenantUoWProvider,
    current_tenant,
    tenant_context,
)
from fastrest_core.uow import AsyncUnitOfWork

# Fixed timestamp — avoids datetime.now() and makes assertions deterministic.
_SENTINEL_TS: datetime = datetime(2026, 1, 1)


# ── Domain entity ──────────────────────────────────────────────────────────────


@dataclass
class TenantPost(AuditedDomainModel):
    """
    Minimal audited entity with a ``tenant_id`` column for row-level tests.

    ``tenant_id`` defaults to ``""`` so ``TenantPost()`` is a valid call
    for collection-level test operations that do not yet have a tenant.
    ``dataclasses.replace(entity, tenant_id=tid)`` works because the field
    is declared with an ``init``-parameter default.
    """

    pk: Annotated[str, PrimaryKey(strategy=PKStrategy.STR_ASSIGNED)] = pk_field(
        init=True
    )
    title: str = ""
    # Row-level tenant discriminator — stamped by the service, not the assembler.
    tenant_id: str = ""

    class Meta:
        table = "tenant_posts"


# ── DTOs ───────────────────────────────────────────────────────────────────────


class CreateTenantPostDTO(CreateDTO):
    """POST /posts payload — title only; tenant_id is never accepted from callers."""

    title: str


class TenantPostReadDTO(ReadDTO):
    """
    GET /posts response body.

    Includes ``tenant_id`` so tests can assert the service stamped it correctly
    from ctx, not from the DTO.
    """

    title: str
    tenant_id: str


class UpdateTenantPostDTO(UpdateDTO):
    """PATCH /posts/{id} — all fields optional (None = no change)."""

    title: str | None = None


# ── Assembler ──────────────────────────────────────────────────────────────────


class TenantPostAssembler(
    AbstractDTOAssembler[
        TenantPost, CreateTenantPostDTO, TenantPostReadDTO, UpdateTenantPostDTO
    ]
):
    """
    Concrete assembler for TenantPost.

    ``to_domain()`` deliberately does NOT set ``tenant_id`` — the service
    stamps it via ``dataclasses.replace`` after assembly.  This validates
    the design contract: the assembler is unaware of tenancy.

    Thread safety:  ✅ Stateless — safe to share across tests.
    Async safety:   ✅ No I/O.
    """

    def to_domain(self, dto: CreateTenantPostDTO) -> TenantPost:
        """
        Map CreateTenantPostDTO → unpersisted TenantPost.

        Does NOT set tenant_id — validates that the service stamps it.

        Args:
            dto: Validated create payload.

        Returns:
            Unpersisted TenantPost with pk=None and tenant_id="" (default).
        """
        # No tenant_id here — service is responsible via dataclasses.replace.
        return TenantPost(title=dto.title)

    def to_read_dto(self, entity: TenantPost) -> TenantPostReadDTO:
        """
        Map persisted TenantPost → TenantPostReadDTO.

        Args:
            entity: Persisted TenantPost.

        Returns:
            Fully populated TenantPostReadDTO including tenant_id.
        """
        return TenantPostReadDTO(
            pk=entity.pk,
            title=entity.title,
            tenant_id=entity.tenant_id,
            created_at=entity.created_at or _SENTINEL_TS,
            updated_at=entity.updated_at or _SENTINEL_TS,
        )

    def apply_update(self, entity: TenantPost, dto: UpdateTenantPostDTO) -> TenantPost:
        """
        Apply UpdateTenantPostDTO to entity; None fields mean "no change".

        Does NOT modify tenant_id — tenant is immutable after creation.

        Args:
            entity: Current persisted state.
            dto:    Partial update payload.

        Returns:
            New TenantPost with title patched; tenant_id preserved.
        """
        return replace(
            entity,
            title=dto.title if dto.title is not None else entity.title,
        )


# ── In-memory repository ───────────────────────────────────────────────────────


class InMemoryTenantRepo(AsyncRepository[TenantPost, str]):
    """
    In-memory AsyncRepository[TenantPost, str] for service-level tests.

    Stores entities in a plain dict — no DB, no I/O.  Records the last
    QueryParams received by find_by_query() so tests can assert that the
    tenant filter was correctly built before being passed to the repo.

    Thread safety:  ❌ Single-threaded test use only.
    Async safety:   ✅ All methods are async.

    Edge cases:
        - save() with pk=None assigns a generated key and sets timestamps.
        - find_by_query() ignores params (no query engine) but records them.
        - delete() is idempotent — no error on missing pk.
    """

    def __init__(self) -> None:
        """Initialise with empty store and zero counters."""
        # Backing store — keyed by pk string
        self._store: dict[str, TenantPost] = {}
        # Auto-increment counter for generated pk values
        self._pk_seq: int = 0
        # Tracks INSERT/UPDATE calls
        self.save_count: int = 0
        # Records the last params passed to find_by_query — used to assert
        # that the tenant filter node was prepended by the service.
        self.last_params: QueryParams | None = None

    async def find_by_id(self, pk: str) -> TenantPost | None:
        """Return entity for pk, or None."""
        return self._store.get(pk)

    async def find_all(self) -> list[TenantPost]:
        """Return all stored entities."""
        return list(self._store.values())

    async def save(self, entity: TenantPost) -> TenantPost:
        """
        Persist entity — assigns pk and timestamps if not yet set.

        Args:
            entity: The TenantPost to persist.

        Returns:
            The entity with pk and timestamps set.
        """
        if entity.pk is None:
            # Simulate a DB sequence — mirrors what SA / Beanie would do.
            self._pk_seq += 1
            entity.pk = f"gen-{self._pk_seq}"
            entity.created_at = _SENTINEL_TS
        entity.updated_at = _SENTINEL_TS
        self._store[entity.pk] = entity
        self.save_count += 1
        return entity

    async def delete(self, entity: TenantPost) -> None:
        """
        Remove entity from store.  Idempotent.

        Args:
            entity: Entity to remove.
        """
        # pop is idempotent — no KeyError if already gone
        self._store.pop(entity.pk, None)

    async def find_by_query(self, params: QueryParams) -> list[TenantPost]:
        """
        Record params and return all stored entities.

        Recording params lets tests verify the tenant filter was built
        without needing a real query engine.

        Args:
            params: Recorded in self.last_params; otherwise ignored.

        Returns:
            All stored entities (no real filtering applied).
        """
        # Store params so tests can inspect the scoped node.
        self.last_params = params
        return list(self._store.values())

    async def count(self, params: QueryParams | None = None) -> int:
        """Return total entity count."""
        return len(self._store)

    async def exists(self, pk: str) -> bool:
        """Return True if pk is in the store."""
        return pk in self._store

    async def stream_by_query(  # type: ignore[override]
        self,
        params: QueryParams,  # noqa: ARG002
    ):
        """Yield all stored entities — filtering ignored in tests."""
        for entity in list(self._store.values()):
            yield entity


# ── In-memory UoW ─────────────────────────────────────────────────────────────


class InMemoryTenantUoW(AsyncUnitOfWork):
    """
    In-memory AsyncUnitOfWork for tests.

    Exposes the shared InMemoryTenantRepo as ``.posts`` so
    ``ConcreteTenantService._get_repo()`` can access it.

    Thread safety:  ❌ Single-threaded test use only.
    Async safety:   ✅ begin/commit/rollback are no-ops.
    """

    def __init__(self, repo: InMemoryTenantRepo) -> None:
        """
        Args:
            repo: Shared in-memory repository exposed as self.posts.
        """
        # Attribute name matches what _get_repo() returns: uow.posts
        self.posts: InMemoryTenantRepo = repo

    async def _begin(self) -> None:
        """No-op — in-memory store needs no transaction setup."""

    async def commit(self) -> None:
        """No-op — in-memory store has no transaction to commit."""

    async def rollback(self) -> None:
        """No-op — in-memory store has no transaction to roll back."""


# ── Fake UoW provider ──────────────────────────────────────────────────────────


class FakeTenantUoWProvider(IUoWProvider):
    """
    IUoWProvider that shares one InMemoryTenantRepo across all UoW instances.

    Sharing the repo lets tests seed data before calling the service and
    inspect state after.  uow_created_count lets tests assert that
    _require_tenant() blocked DB access before make_uow() was called.

    Thread safety:  ❌ Single-threaded test use only.

    Edge cases:
        - Each make_uow() call returns a NEW InMemoryTenantUoW wrapping
          the SAME repo — data from one operation is visible in the next.
    """

    def __init__(self) -> None:
        """Initialise with empty shared repository and zero counters."""
        # Shared backing store — seed here before calling the service
        self.repo: InMemoryTenantRepo = InMemoryTenantRepo()
        # Counts make_uow() calls — assert == 0 after missing-tenant failures
        # for any operation, since _require_tenant() always fires first.
        self.uow_created_count: int = 0

    def make_uow(self) -> AsyncUnitOfWork:
        """
        Return a fresh InMemoryTenantUoW wrapping the shared repo.

        Returns:
            A new InMemoryTenantUoW with the shared InMemoryTenantRepo.
        """
        self.uow_created_count += 1
        return InMemoryTenantUoW(repo=self.repo)


# ── Authorizers ────────────────────────────────────────────────────────────────


class DenyAllAuthorizer(AbstractAuthorizer):
    """
    Authorizer that unconditionally denies every action.

    Used to verify that the service correctly propagates authorization
    failures.  Concreteness over Mock makes the intent explicit.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ No I/O.
    """

    async def authorize(
        self,
        ctx: AuthContext,
        action: Action,
        resource: Resource,
    ) -> None:
        """Always raise ServiceAuthorizationError."""
        raise ServiceAuthorizationError(str(action), resource.entity_type)


# ── Concrete tenant service ────────────────────────────────────────────────────


class ConcreteTenantService(
    TenantAwareService[
        TenantPost, str, CreateTenantPostDTO, TenantPostReadDTO, UpdateTenantPostDTO
    ]
):
    """
    Minimal concrete TenantAwareService for testing.

    Only implements _get_repo() — wires service to uow.posts.
    Inherits all row-level isolation enforcement from TenantAwareService.

    Thread safety:  ✅ Stateless — shares collaborators injected at __init__.
    Async safety:   ✅ Each operation opens its own UoW.
    """

    def __init__(
        self,
        uow_provider: IUoWProvider,
        authorizer: AbstractAuthorizer,
        assembler: TenantPostAssembler,
    ) -> None:
        """
        Args:
            uow_provider: Provider for InMemoryTenantUoW instances.
            authorizer:   Allow-all (BaseAuthorizer) or deny-all for test variants.
            assembler:    TenantPostAssembler instance.
        """
        super().__init__(
            uow_provider=uow_provider,
            authorizer=authorizer,
            assembler=assembler,
        )

    def _get_repo(self, uow: Any) -> InMemoryTenantRepo:
        """Return the posts repository from the unit of work."""
        return uow.posts


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture
def uow_provider() -> FakeTenantUoWProvider:
    """Fresh FakeTenantUoWProvider per test — isolated repo and counters."""
    return FakeTenantUoWProvider()


@pytest.fixture
def assembler() -> TenantPostAssembler:
    """Stateless assembler — safe to share."""
    return TenantPostAssembler()


@pytest.fixture
def svc(
    uow_provider: FakeTenantUoWProvider,
    assembler: TenantPostAssembler,
) -> ConcreteTenantService:
    """ConcreteTenantService wired with BaseAuthorizer (allow-all)."""
    return ConcreteTenantService(
        uow_provider=uow_provider,
        authorizer=BaseAuthorizer(),
        assembler=assembler,
    )


@pytest.fixture
def deny_svc(
    uow_provider: FakeTenantUoWProvider,
    assembler: TenantPostAssembler,
) -> ConcreteTenantService:
    """ConcreteTenantService wired with DenyAllAuthorizer."""
    return ConcreteTenantService(
        uow_provider=uow_provider,
        authorizer=DenyAllAuthorizer(),
        assembler=assembler,
    )


@pytest.fixture
def ctx_acme() -> AuthContext:
    """AuthContext for the 'acme' tenant."""
    return AuthContext(metadata={"tenant_id": "acme"})


@pytest.fixture
def ctx_globex() -> AuthContext:
    """AuthContext for the 'globex' tenant."""
    return AuthContext(metadata={"tenant_id": "globex"})


@pytest.fixture
def ctx_no_tenant() -> AuthContext:
    """AuthContext with no tenant_id — simulates a missing or unauthenticated tenant claim."""
    # Empty metadata dict — _require_tenant() must raise ServiceAuthorizationError.
    return AuthContext()


def _seed(
    repo: InMemoryTenantRepo,
    pk: str,
    title: str,
    tenant_id: str,
) -> TenantPost:
    """
    Directly write a TenantPost into the repo, bypassing the service.

    Used to set up cross-tenant and existing-entity scenarios without
    going through the full create() path.

    Args:
        repo:      Target in-memory repository.
        pk:        Primary key to assign.
        title:     Post title.
        tenant_id: Tenant discriminator value.

    Returns:
        The seeded TenantPost instance.
    """
    post = TenantPost(pk=pk, title=title, tenant_id=tenant_id)
    post.created_at = _SENTINEL_TS
    post.updated_at = _SENTINEL_TS
    repo._store[pk] = post
    return post


# ══════════════════════════════════════════════════════════════════════════════
# Part 1 — current_tenant / tenant_context
# ══════════════════════════════════════════════════════════════════════════════


class TestCurrentTenant:
    """Tests for the current_tenant() getter and tenant_context() manager."""

    def test_returns_none_outside_any_context(self) -> None:
        # ContextVar default is None — no tenant set at module level.
        assert current_tenant() is None

    def test_returns_active_tenant_inside_context(self) -> None:
        with tenant_context("acme"):
            assert current_tenant() == "acme"

    def test_restores_none_after_block_exits(self) -> None:
        # Token reset must fire on clean exit.
        with tenant_context("acme"):
            pass
        assert current_tenant() is None

    def test_restores_on_exception_propagation(self) -> None:
        # Token reset must fire even when an exception propagates out.
        try:
            with tenant_context("acme"):
                raise ValueError("simulated error")
        except ValueError:
            pass
        assert current_tenant() is None

    def test_nested_context_inner_overrides_outer(self) -> None:
        # Inner tenant_context overrides outer for its block duration,
        # then outer is restored automatically via ContextVar token.
        with tenant_context("outer"):
            assert current_tenant() == "outer"
            with tenant_context("inner"):
                assert current_tenant() == "inner"
            # Inner exited — outer is restored.
            assert current_tenant() == "outer"
        # Outer exited — None restored.
        assert current_tenant() is None

    async def test_spawned_task_inherits_parent_tenant(self) -> None:
        # asyncio.Task copies the parent's context snapshot — the tenant set
        # in the HTTP adapter propagates automatically into sub-tasks.
        result: list[str | None] = []

        async def child() -> None:
            result.append(current_tenant())

        with tenant_context("acme"):
            task = asyncio.ensure_future(child())
            await task

        assert result == ["acme"]


# ══════════════════════════════════════════════════════════════════════════════
# Part 2 — TenantUoWProvider
# ══════════════════════════════════════════════════════════════════════════════


class TestTenantUoWProvider:
    """Tests for TenantUoWProvider registration, introspection, and routing."""

    # ── Construction ──────────────────────────────────────────────────────────

    def test_empty_on_default_construction(self) -> None:
        # providers defaults to None → empty dict → no tenants registered.
        p = TenantUoWProvider()
        assert p.registered_tenants() == []

    def test_static_construction_with_initial_dict(self) -> None:
        # Providers passed at construction time are immediately available.
        mock_provider = MagicMock(spec=RepositoryProvider)
        p = TenantUoWProvider({"acme": mock_provider})
        assert p.has_tenant("acme")

    def test_static_construction_does_not_mutate_input_dict(self) -> None:
        # TenantUoWProvider copies the input dict — later register() calls
        # must not affect the caller's original mapping.
        original: dict[str, RepositoryProvider] = {}
        p = TenantUoWProvider(original)
        p.register("acme", MagicMock(spec=RepositoryProvider))
        assert "acme" not in original

    # ── register() ────────────────────────────────────────────────────────────

    def test_register_adds_tenant_to_provider(self) -> None:
        p = TenantUoWProvider()
        p.register("acme", MagicMock(spec=RepositoryProvider))
        assert p.has_tenant("acme")

    def test_register_two_tenants_independently(self) -> None:
        p = TenantUoWProvider()
        p.register("acme", MagicMock(spec=RepositoryProvider))
        p.register("globex", MagicMock(spec=RepositoryProvider))
        assert p.has_tenant("acme")
        assert p.has_tenant("globex")

    def test_register_replaces_existing_provider_silently(self) -> None:
        # Replacing is intentional — allows hot-swapping of backends.
        p = TenantUoWProvider()
        first = MagicMock(spec=RepositoryProvider)
        second = MagicMock(spec=RepositoryProvider)
        p.register("acme", first)
        p.register("acme", second)  # silent replacement

        with tenant_context("acme"):
            p.make_uow()

        # Only the second (replacement) provider should have been called.
        second.make_uow.assert_called_once()
        first.make_uow.assert_not_called()

    # ── has_tenant() ──────────────────────────────────────────────────────────

    def test_has_tenant_false_for_unknown_id(self) -> None:
        p = TenantUoWProvider()
        assert not p.has_tenant("nonexistent")

    def test_has_tenant_true_after_register(self) -> None:
        p = TenantUoWProvider()
        p.register("acme", MagicMock())
        assert p.has_tenant("acme")

    def test_has_tenant_case_sensitive(self) -> None:
        # "Acme" and "acme" are different tenants — exact equality only.
        p = TenantUoWProvider()
        p.register("acme", MagicMock())
        assert not p.has_tenant("Acme")
        assert not p.has_tenant("ACME")

    # ── registered_tenants() ──────────────────────────────────────────────────

    def test_registered_tenants_empty_list_when_no_tenants(self) -> None:
        assert TenantUoWProvider().registered_tenants() == []

    def test_registered_tenants_sorted_alphabetically(self) -> None:
        p = TenantUoWProvider()
        p.register("zzz", MagicMock())
        p.register("aaa", MagicMock())
        p.register("mmm", MagicMock())
        # Result must always be sorted — deterministic for error messages.
        assert p.registered_tenants() == ["aaa", "mmm", "zzz"]

    def test_registered_tenants_returns_snapshot(self) -> None:
        # List must be a copy — mutating it does not affect the provider.
        p = TenantUoWProvider()
        p.register("acme", MagicMock())
        tenants = p.registered_tenants()
        tenants.clear()
        assert p.has_tenant("acme")

    # ── make_uow() ────────────────────────────────────────────────────────────

    def test_make_uow_raises_runtime_error_outside_tenant_context(self) -> None:
        # Calling outside tenant_context() is a programming error — fail fast.
        p = TenantUoWProvider()
        p.register("acme", MagicMock())
        with pytest.raises(RuntimeError, match="tenant_context"):
            p.make_uow()

    def test_make_uow_raises_key_error_for_unregistered_tenant(self) -> None:
        p = TenantUoWProvider()
        with tenant_context("ghost"):
            with pytest.raises(KeyError, match="ghost"):
                p.make_uow()

    def test_make_uow_key_error_message_mentions_register(self) -> None:
        # Error must tell the caller how to fix the issue.
        p = TenantUoWProvider()
        with tenant_context("ghost"):
            with pytest.raises(KeyError, match="register"):
                p.make_uow()

    def test_make_uow_delegates_to_correct_provider(self) -> None:
        acme_provider = MagicMock(spec=RepositoryProvider)
        globex_provider = MagicMock(spec=RepositoryProvider)
        p = TenantUoWProvider({"acme": acme_provider, "globex": globex_provider})

        with tenant_context("acme"):
            p.make_uow()

        acme_provider.make_uow.assert_called_once()
        globex_provider.make_uow.assert_not_called()

    def test_make_uow_routes_different_tenants_correctly(self) -> None:
        # Sequential requests for different tenants must each hit their own provider.
        acme_provider = MagicMock(spec=RepositoryProvider)
        globex_provider = MagicMock(spec=RepositoryProvider)
        p = TenantUoWProvider({"acme": acme_provider, "globex": globex_provider})

        with tenant_context("acme"):
            p.make_uow()
        with tenant_context("globex"):
            p.make_uow()

        acme_provider.make_uow.assert_called_once()
        globex_provider.make_uow.assert_called_once()

    # ── __repr__ ──────────────────────────────────────────────────────────────

    def test_repr_shows_registered_tenant_names(self) -> None:
        p = TenantUoWProvider()
        p.register("acme", MagicMock())
        assert "acme" in repr(p)

    def test_repr_shows_empty_list_when_no_tenants(self) -> None:
        p = TenantUoWProvider()
        assert "[]" in repr(p)


# ══════════════════════════════════════════════════════════════════════════════
# Part 3 — TenantAwareService — list()
# ══════════════════════════════════════════════════════════════════════════════


class TestTenantAwareServiceList:
    """Tests for TenantAwareService.list() — tenant filter injection."""

    async def test_list_returns_entities_from_store(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_acme: AuthContext,
    ) -> None:
        _seed(uow_provider.repo, "p1", "Acme post", "acme")
        results = await svc.list(QueryParams(), ctx_acme)
        assert len(results) == 1
        assert results[0].title == "Acme post"

    async def test_list_passes_scoped_filter_node_to_repo(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_acme: AuthContext,
    ) -> None:
        # The repo records the last params — non-None node proves the tenant
        # filter was built and forwarded.
        await svc.list(QueryParams(), ctx_acme)
        assert uow_provider.repo.last_params is not None
        assert uow_provider.repo.last_params.node is not None

    async def test_list_raises_authorization_error_when_no_tenant(
        self,
        svc: ConcreteTenantService,
        ctx_no_tenant: AuthContext,
    ) -> None:
        # _require_tenant() must fire before any DB access.
        with pytest.raises(ServiceAuthorizationError):
            await svc.list(QueryParams(), ctx_no_tenant)

    async def test_list_no_uow_opened_when_tenant_missing(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_no_tenant: AuthContext,
    ) -> None:
        # _require_tenant() fires before make_uow() — no DB connection opened.
        with pytest.raises(ServiceAuthorizationError):
            await svc.list(QueryParams(), ctx_no_tenant)
        assert uow_provider.uow_created_count == 0


# ══════════════════════════════════════════════════════════════════════════════
# Part 4 — TenantAwareService — create()
# ══════════════════════════════════════════════════════════════════════════════


class TestTenantAwareServiceCreate:
    """Tests for TenantAwareService.create() — tenant_id stamping from ctx."""

    async def test_create_stamps_tenant_id_from_ctx(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_acme: AuthContext,
    ) -> None:
        # The assembler's to_domain() returns an entity with tenant_id="",
        # then the service replaces it from ctx.  The returned DTO must carry
        # the ctx tenant_id — not whatever the DTO could have supplied.
        result = await svc.create(CreateTenantPostDTO(title="New post"), ctx_acme)
        assert result.tenant_id == "acme"

    async def test_create_stamps_different_tenant_correctly(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_globex: AuthContext,
    ) -> None:
        result = await svc.create(CreateTenantPostDTO(title="Globex post"), ctx_globex)
        assert result.tenant_id == "globex"

    async def test_create_persists_entity_in_store(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_acme: AuthContext,
    ) -> None:
        await svc.create(CreateTenantPostDTO(title="Test"), ctx_acme)
        assert uow_provider.repo.save_count == 1

    async def test_create_raises_when_no_tenant_in_ctx(
        self,
        svc: ConcreteTenantService,
        ctx_no_tenant: AuthContext,
    ) -> None:
        with pytest.raises(ServiceAuthorizationError):
            await svc.create(CreateTenantPostDTO(title="x"), ctx_no_tenant)

    async def test_create_no_uow_opened_when_tenant_missing(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_no_tenant: AuthContext,
    ) -> None:
        # _require_tenant() fires before make_uow() for create() too.
        with pytest.raises(ServiceAuthorizationError):
            await svc.create(CreateTenantPostDTO(title="x"), ctx_no_tenant)
        assert uow_provider.uow_created_count == 0

    async def test_create_no_uow_opened_when_authorizer_denies(
        self,
        deny_svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_acme: AuthContext,
    ) -> None:
        # Authorizer denial after _require_tenant() still prevents DB access.
        with pytest.raises(ServiceAuthorizationError):
            await deny_svc.create(CreateTenantPostDTO(title="x"), ctx_acme)
        assert uow_provider.uow_created_count == 0


# ══════════════════════════════════════════════════════════════════════════════
# Part 5 — TenantAwareService — get()
# ══════════════════════════════════════════════════════════════════════════════


class TestTenantAwareServiceGet:
    """Tests for TenantAwareService.get() — tenant check before authorization."""

    async def test_get_returns_entity_for_matching_tenant(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_acme: AuthContext,
    ) -> None:
        _seed(uow_provider.repo, "p1", "Acme post", "acme")
        result = await svc.get("p1", ctx_acme)
        assert result.title == "Acme post"
        assert result.tenant_id == "acme"

    async def test_get_raises_not_found_for_cross_tenant(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_globex: AuthContext,
    ) -> None:
        # Entity belongs to "acme", caller is "globex" → 404, not 403.
        # A 403 would reveal that the entity exists in another tenant's data.
        _seed(uow_provider.repo, "p1", "Acme post", "acme")
        with pytest.raises(ServiceNotFoundError):
            await svc.get("p1", ctx_globex)

    async def test_get_cross_tenant_raises_not_found_not_authorization_error(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_globex: AuthContext,
    ) -> None:
        # Explicitly verify the exception type is NOT ServiceAuthorizationError.
        # This is the existence oracle prevention contract.
        _seed(uow_provider.repo, "p1", "Acme post", "acme")
        with pytest.raises(ServiceNotFoundError) as exc_info:
            await svc.get("p1", ctx_globex)
        # ServiceNotFoundError must not be a subclass of ServiceAuthorizationError.
        assert not isinstance(exc_info.value, ServiceAuthorizationError)

    async def test_get_raises_not_found_for_missing_entity(
        self,
        svc: ConcreteTenantService,
        ctx_acme: AuthContext,
    ) -> None:
        with pytest.raises(ServiceNotFoundError):
            await svc.get("nonexistent", ctx_acme)

    async def test_get_raises_when_no_tenant_in_ctx(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_no_tenant: AuthContext,
    ) -> None:
        _seed(uow_provider.repo, "p1", "Post", "acme")
        with pytest.raises(ServiceAuthorizationError):
            await svc.get("p1", ctx_no_tenant)


# ══════════════════════════════════════════════════════════════════════════════
# Part 6 — TenantAwareService — update()
# ══════════════════════════════════════════════════════════════════════════════


class TestTenantAwareServiceUpdate:
    """Tests for TenantAwareService.update() — tenant check before mutation."""

    async def test_update_returns_updated_dto_for_matching_tenant(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_acme: AuthContext,
    ) -> None:
        _seed(uow_provider.repo, "p1", "Old title", "acme")
        result = await svc.update(
            "p1", UpdateTenantPostDTO(title="New title"), ctx_acme
        )
        assert result.title == "New title"

    async def test_update_preserves_tenant_id_after_mutation(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_acme: AuthContext,
    ) -> None:
        # apply_update() does not touch tenant_id — verify it survives the round trip.
        _seed(uow_provider.repo, "p1", "Original", "acme")
        result = await svc.update("p1", UpdateTenantPostDTO(title="Updated"), ctx_acme)
        assert result.tenant_id == "acme"

    async def test_update_raises_not_found_for_cross_tenant(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_globex: AuthContext,
    ) -> None:
        _seed(uow_provider.repo, "p1", "Acme post", "acme")
        with pytest.raises(ServiceNotFoundError):
            await svc.update("p1", UpdateTenantPostDTO(title="Hijacked"), ctx_globex)

    async def test_update_does_not_mutate_store_on_cross_tenant_attempt(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_globex: AuthContext,
    ) -> None:
        # Entity must be unchanged after a rejected cross-tenant update.
        _seed(uow_provider.repo, "p1", "Original", "acme")
        try:
            await svc.update("p1", UpdateTenantPostDTO(title="Hijacked"), ctx_globex)
        except ServiceNotFoundError:
            pass
        assert uow_provider.repo._store["p1"].title == "Original"

    async def test_update_raises_not_found_for_missing_entity(
        self,
        svc: ConcreteTenantService,
        ctx_acme: AuthContext,
    ) -> None:
        with pytest.raises(ServiceNotFoundError):
            await svc.update("ghost", UpdateTenantPostDTO(title="x"), ctx_acme)

    async def test_update_raises_when_no_tenant_in_ctx(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_no_tenant: AuthContext,
    ) -> None:
        _seed(uow_provider.repo, "p1", "Post", "acme")
        with pytest.raises(ServiceAuthorizationError):
            await svc.update("p1", UpdateTenantPostDTO(title="x"), ctx_no_tenant)


# ══════════════════════════════════════════════════════════════════════════════
# Part 7 — TenantAwareService — delete()
# ══════════════════════════════════════════════════════════════════════════════


class TestTenantAwareServiceDelete:
    """Tests for TenantAwareService.delete() — tenant check before removal."""

    async def test_delete_removes_entity_for_matching_tenant(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_acme: AuthContext,
    ) -> None:
        _seed(uow_provider.repo, "p1", "Acme post", "acme")
        await svc.delete("p1", ctx_acme)
        assert "p1" not in uow_provider.repo._store

    async def test_delete_raises_not_found_for_cross_tenant(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_globex: AuthContext,
    ) -> None:
        _seed(uow_provider.repo, "p1", "Acme post", "acme")
        with pytest.raises(ServiceNotFoundError):
            await svc.delete("p1", ctx_globex)

    async def test_delete_does_not_remove_entity_on_cross_tenant_attempt(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_globex: AuthContext,
    ) -> None:
        # Entity must survive a rejected cross-tenant delete attempt.
        _seed(uow_provider.repo, "p1", "Acme post", "acme")
        try:
            await svc.delete("p1", ctx_globex)
        except ServiceNotFoundError:
            pass
        assert "p1" in uow_provider.repo._store

    async def test_delete_raises_not_found_for_missing_entity(
        self,
        svc: ConcreteTenantService,
        ctx_acme: AuthContext,
    ) -> None:
        with pytest.raises(ServiceNotFoundError):
            await svc.delete("nonexistent", ctx_acme)

    async def test_delete_raises_when_no_tenant_in_ctx(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_no_tenant: AuthContext,
    ) -> None:
        _seed(uow_provider.repo, "p1", "Post", "acme")
        with pytest.raises(ServiceAuthorizationError):
            await svc.delete("p1", ctx_no_tenant)

    async def test_delete_no_uow_opened_when_tenant_missing(
        self,
        svc: ConcreteTenantService,
        uow_provider: FakeTenantUoWProvider,
        ctx_no_tenant: AuthContext,
    ) -> None:
        with pytest.raises(ServiceAuthorizationError):
            await svc.delete("p1", ctx_no_tenant)
        assert uow_provider.uow_created_count == 0
