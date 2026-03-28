"""
Unit tests for varco_core.service
======================================
Covers: IUoWProvider, AsyncService (all five CRUD methods), and DI injection.

Test structure
--------------
Fake test doubles (no mocking library):

    InMemoryPostRepository — in-memory AsyncRepository[Post, str].
    InMemoryUoW            — in-memory AsyncUnitOfWork carrying the repo.
    FakeUoWProvider        — IUoWProvider that shares one repository across
                             all UoW instances so tests can pre-seed data.
    PostAssembler          — concrete AbstractDTOAssembler for Post.
    ConcretePostService    — concrete AsyncService[Post, str, ...].
    DenyAllAuthorizer      — raises ServiceAuthorizationError unconditionally.

DESIGN: shared repo inside FakeUoWProvider
    Because make_uow() is called once per service operation, using a fresh
    in-memory store per call would make pre-seeding impossible.  Sharing
    one InMemoryPostRepository across all UoW instances lets tests seed data
    before calling the service and inspect state after.

    Tradeoffs:
      ✅ Tests can pre-seed the repo and assert its state after each call.
      ✅ Mirrors production RepositoryProvider: the store is shared, the
         session (UoW) is fresh per request.
      ❌ Two concurrent service calls would share state — not an issue for
         sequential unit tests.

DESIGN: DenyAllAuthorizer instead of mocking
    A concrete DenyAllAuthorizer is clearer than a Mock that raises — the
    intent is explicit in the class name and no mock assertion is needed.
    It also verifies that ServiceAuthorizationError propagates correctly
    through the service boundary.

DESIGN: uow_created_count for "auth before DB" assertions
    For list() and create(), authorization fires BEFORE make_uow() is called.
    Asserting uow_created_count == 0 after a denied call directly verifies
    the order-of-operations contract documented in the service module.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Annotated, Any

import pytest
from providify import DIContainer, Inject, Singleton

from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth import AbstractAuthorizer, Action, AuthContext, Resource
from varco_core.auth.authorizer import BaseAuthorizer
from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO
from varco_core.exception.service import (
    ServiceAuthorizationError,
    ServiceNotFoundError,
    ServiceValidationError,
)
from varco_core.meta import PKStrategy, PrimaryKey, pk_field
from varco_core.model import AuditedDomainModel
from varco_core.query.params import QueryParams
from varco_core.repository import AsyncRepository
from varco_core.service import AsyncService, AsyncValidatorServiceMixin, IUoWProvider
from varco_core.validation import AsyncDomainModelValidator, ValidationResult
from varco_core.uow import AsyncUnitOfWork


# ── Domain entity ──────────────────────────────────────────────────────────────


@dataclass
class Post(AuditedDomainModel):
    """
    Minimal audited entity for service tests.

    Uses ``AuditedDomainModel`` so ``created_at``/``updated_at`` are available
    for ``PostReadDTO`` — ``ReadDTO`` requires both timestamps.

    Uses ``STR_ASSIGNED`` pk so tests can pass ``pk`` directly in the
    constructor without post-construction attribute assignment.

    Field ordering note:
        ``pk`` has ``default=None`` (from ``pk_field``), so non-default fields
        (``title``) must also have a default to satisfy dataclass ordering rules.
    """

    # STR_ASSIGNED + init=True: pk is part of __init__ with default=None.
    pk: Annotated[str, PrimaryKey(strategy=PKStrategy.STR_ASSIGNED)] = pk_field(
        init=True
    )
    # Default empty string so Post() is valid for collection-level operations.
    title: str = ""

    class Meta:
        table = "posts"


# ── DTOs ───────────────────────────────────────────────────────────────────────


class CreatePostDTO(CreateDTO):
    """POST /posts payload — only the title is required for creation."""

    title: str


class PostReadDTO(ReadDTO):
    """
    GET /posts response body.

    Inherits ``id``, ``created_at``, ``updated_at`` from ``ReadDTO``.
    """

    title: str


class UpdatePostDTO(UpdateDTO):
    """
    PATCH /posts/{id} payload — all fields are optional (None = no change).
    """

    title: str | None = None


# ── Sentinel timestamp — fixed value for assembler in tests ───────────────────

# Fixed datetime used when entity.created_at / updated_at are None (unpersisted).
# Avoids datetime.now() in tests — deterministic, no flakiness from clock drift.
_SENTINEL_TS: datetime = datetime(2026, 1, 1, 0, 0, 0)


# ── Concrete assembler ─────────────────────────────────────────────────────────


@Singleton
class PostAssembler(
    AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]
):
    """
    Concrete assembler for the Post entity used in all service tests.

    Decorated with ``@Singleton`` so the DI container can register it under
    the parameterized generic ``AbstractDTOAssembler[Post, ...]`` and resolve
    it for ``ConcretePostService``'s ``Inject[AbstractDTOAssembler[Post, ...]]``
    hint.  Manual instantiation (``PostAssembler()``) still works normally.

    Thread safety:  ✅ Stateless — safe to share across tests.
    Async safety:   ✅ All methods are synchronous — no I/O.
    """

    def to_domain(self, dto: CreatePostDTO) -> Post:
        """
        Map ``CreatePostDTO`` → a fresh, unpersisted ``Post``.

        ``pk`` is intentionally left as ``None`` — the in-memory repo assigns
        a generated pk on ``save()`` (simulating what a real DB would do).

        Args:
            dto: Validated create payload.

        Returns:
            Unpersisted ``Post`` with ``pk=None`` and ``_raw_orm=None``.
        """
        # No pk here — the repo assigns it on INSERT (mirrors real behaviour).
        return Post(title=dto.title)

    def to_read_dto(self, entity: Post) -> PostReadDTO:
        """
        Map a persisted ``Post`` → ``PostReadDTO``.

        Falls back to ``_SENTINEL_TS`` when timestamps are ``None`` — only
        occurs for test entities that were seeded directly without going
        through ``repo.save()``.

        Args:
            entity: Persisted ``Post`` instance.

        Returns:
            Fully populated ``PostReadDTO``.
        """
        return PostReadDTO(
            pk=entity.pk,
            title=entity.title,
            # Fallback to sentinel — saves us from None checks in every test
            created_at=entity.created_at or _SENTINEL_TS,
            updated_at=entity.updated_at or _SENTINEL_TS,
        )

    def apply_update(self, entity: Post, dto: UpdatePostDTO) -> Post:
        """
        Apply ``UpdatePostDTO`` to ``entity`` and return a new ``Post``.

        ``None`` fields in ``dto`` mean "no change" — the existing value is
        preserved.  ``dataclasses.replace`` copies ``_raw_orm`` automatically
        so the repo treats the result as an UPDATE (not an INSERT).

        Args:
            entity: Current persisted state.
            dto:    Partial update payload.

        Returns:
            New ``Post`` with updated fields; ``_raw_orm`` inherited from
            ``entity``.
        """
        return replace(
            entity,
            # None in dto means "leave unchanged" — preserve existing value
            title=dto.title if dto.title is not None else entity.title,
        )


# ── In-memory repository ───────────────────────────────────────────────────────


class InMemoryPostRepository(AsyncRepository[Post, str]):
    """
    In-memory implementation of ``AsyncRepository[Post, str]`` for tests.

    Stores entities in a plain ``dict[str, Post]`` — no DB, no I/O.
    Tracks ``save_count`` so tests can assert how many persists occurred.

    Thread safety:  ❌ Single-threaded test use only.
    Async safety:   ✅ All methods are async — safe to await in tests.

    Edge cases:
        - ``save()`` assigns a generated pk when ``entity.pk is None``.
        - ``save()`` also sets ``created_at`` and ``updated_at`` so
          ``to_read_dto`` never has to fall back to the sentinel.
        - ``delete()`` is a no-op if pk is not in the store (idempotent).
        - ``find_by_query()`` returns all stored entities (no filtering) —
          sufficient for testing list() behaviour.
    """

    def __init__(self) -> None:
        """Initialise with empty store and zero counters."""
        # In-memory backing store — keyed by pk
        self._store: dict[str, Post] = {}
        # Tracks INSERT/UPDATE calls — lets tests verify persistence occurred
        self.save_count: int = 0
        # Monotonic counter for auto-generated pk values
        self._pk_seq: int = 0

    async def find_by_id(self, pk: str) -> Post | None:
        """
        Look up an entity by pk.

        Args:
            pk: String primary key.

        Returns:
            The stored ``Post``, or ``None`` if not found.
        """
        return self._store.get(pk)

    async def find_all(self) -> list[Post]:
        """Return all stored entities as a list."""
        return list(self._store.values())

    async def save(self, entity: Post) -> Post:
        """
        Insert or update ``entity`` in the in-memory store.

        INSERT (``pk is None``): assigns a generated pk and initial timestamps.
        UPDATE (``pk is not None``): updates ``updated_at`` timestamp.

        Args:
            entity: The ``Post`` to persist.

        Returns:
            The same entity (mutated in place for simplicity) with pk and
            timestamps set.  Always use the returned value.
        """
        if entity.pk is None:
            # Auto-assign pk — mirrors what a real DB sequence would do
            self._pk_seq += 1
            entity.pk = f"gen-{self._pk_seq}"
            entity.created_at = _SENTINEL_TS

        # Always refresh updated_at on every save
        entity.updated_at = _SENTINEL_TS
        self._store[entity.pk] = entity
        self.save_count += 1
        return entity

    async def delete(self, entity: Post) -> None:
        """
        Remove ``entity`` from the store.

        Idempotent — no error if pk is not found (mirrors DELETE ... IF EXISTS).

        Args:
            entity: The entity to remove.

        Raises:
            ValueError: ``entity.pk`` is ``None`` — nothing to delete.
        """
        if entity.pk is None:
            raise ValueError("Cannot delete an entity that has no pk.")
        # pop is idempotent — no KeyError if already gone
        self._store.pop(entity.pk, None)

    async def find_by_query(self, params: QueryParams) -> list[Post]:
        """
        Return all stored entities — filtering/sorting are ignored.

        Sufficient for testing list() service behaviour without implementing
        a full query engine.

        Args:
            params: Ignored.

        Returns:
            All stored entities.
        """
        # Ignoring params intentionally — tests only need to verify the
        # service calls this method and assembles DTOs from the results.
        return list(self._store.values())

    async def count(self, params: QueryParams | None = None) -> int:
        """
        Return total number of stored entities.

        Args:
            params: Ignored.

        Returns:
            Number of entities in the store.
        """
        return len(self._store)

    async def exists(self, pk: str) -> bool:
        """Return True if pk is present in the store."""
        return pk in self._store

    async def stream_by_query(  # type: ignore[override]
        self,
        params: QueryParams,  # noqa: ARG002
    ):
        """Yield all stored entities — filtering ignored in tests."""
        for entity in list(self._store.values()):
            yield entity

    # ── Bulk stubs (required by AsyncRepository ABC) ──────────────────────────
    # These are no-op stubs sufficient for tests that do not exercise bulk ops.

    async def save_many(self, entities):  # type: ignore[override]
        """Stub — delegates to individual save() calls."""
        return [await self.save(e) for e in entities]

    async def delete_many(self, entities):  # type: ignore[override]
        """Stub — delegates to individual delete() calls."""
        for e in entities:
            await self.delete(e)

    async def update_many_by_query(self, params, update):  # type: ignore[override]
        """Stub — raises NotImplementedError; not needed in service tests."""
        raise NotImplementedError("update_many_by_query not used in service tests")


# ── In-memory Unit of Work ─────────────────────────────────────────────────────


class InMemoryUoW(AsyncUnitOfWork):
    """
    In-memory ``AsyncUnitOfWork`` for tests.

    Exposes the shared ``InMemoryPostRepository`` as ``.posts`` so
    ``ConcretePostService._get_repo()`` can access it via ``uow.posts``.

    Thread safety:  ❌ Single-threaded test use only.
    Async safety:   ✅ ``_begin``, ``commit``, ``rollback`` are no-ops.

    Edge cases:
        - ``commit()`` and ``rollback()`` are no-ops — the in-memory store
          does not need transactional semantics for unit tests.
        - Re-entering the same UoW is safe (begin is idempotent) — this
          matches the test pattern where a single UoW instance is reused.
    """

    def __init__(self, repo: InMemoryPostRepository) -> None:
        """
        Args:
            repo: Shared in-memory repository.  Stored as ``self.posts``
                  so service code can access it via ``uow.posts``.
        """
        # Exposed as .posts — ConcretePostService._get_repo returns uow.posts
        self.posts: InMemoryPostRepository = repo

    async def _begin(self) -> None:
        """No-op — in-memory store needs no transaction setup."""

    async def commit(self) -> None:
        """No-op — in-memory store has no transaction to commit."""

    async def rollback(self) -> None:
        """No-op — in-memory store has no transaction to roll back."""


# ── Fake UoW provider ──────────────────────────────────────────────────────────


@Singleton
class FakeUoWProvider(IUoWProvider):
    """
    ``IUoWProvider`` that shares a single ``InMemoryPostRepository`` across
    all ``InMemoryUoW`` instances it creates.

    Decorated with ``@Singleton`` so the DI container can register and resolve
    it as ``IUoWProvider`` via ``container.register(FakeUoWProvider)``.
    Manual instantiation (``FakeUoWProvider()``) still works — used by the
    non-DI fixtures (``uow_provider``, ``svc``, ``deny_svc``).

    Sharing the repo is intentional — tests pre-seed data on ``self.repo``
    before calling the service, and the service sees that data through any
    UoW it creates.

    ``uow_created_count`` lets tests assert that authorization failures
    prevented the service from ever calling ``make_uow()`` (proving auth
    fires before the DB is touched for CREATE and LIST).

    Thread safety:  ❌ Single-threaded test use only.

    Edge cases:
        - Every call to ``make_uow()`` returns a NEW ``InMemoryUoW`` that
          wraps the SAME ``self.repo`` — data persisted by one UoW is visible
          to the next, exactly like a real session factory.
    """

    def __init__(self) -> None:
        """Initialise with an empty shared repository."""
        # Shared across all UoW instances — seed data here before calling svc
        self.repo: InMemoryPostRepository = InMemoryPostRepository()
        # Counts make_uow() calls — assert this is 0 after auth denial for
        # operations that auth before opening the UoW (LIST, CREATE).
        self.uow_created_count: int = 0

    def make_uow(self) -> AsyncUnitOfWork:
        """
        Return a new ``InMemoryUoW`` wrapping the shared ``self.repo``.

        Returns:
            Fresh ``InMemoryUoW`` — new transaction boundary, shared store.
        """
        self.uow_created_count += 1
        return InMemoryUoW(repo=self.repo)


# ── Deny-all authorizer ────────────────────────────────────────────────────────


class DenyAllAuthorizer(AbstractAuthorizer):
    """
    Authorizer that unconditionally raises ``ServiceAuthorizationError``.

    Used in tests that verify the service correctly propagates auth failures.
    A concrete class is clearer than a Mock — the intent is explicit in the
    class name and no mock assertion is needed.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ No I/O.
    """

    async def authorize(
        self,
        ctx: AuthContext,
        action: Action,
        resource: Resource,
    ) -> None:
        """
        Deny every operation unconditionally.

        Args:
            ctx:      Ignored.
            action:   Used in the error message for diagnostics.
            resource: Used in the error message for diagnostics.

        Raises:
            ServiceAuthorizationError: Always — every call is denied.
        """
        raise ServiceAuthorizationError(
            str(action),
            resource.entity_type,
            reason="DenyAllAuthorizer — always denies in tests.",
        )


# ── Concrete service ───────────────────────────────────────────────────────────


@Singleton
class ConcretePostService(
    AsyncService[Post, str, CreatePostDTO, PostReadDTO, UpdatePostDTO]
):
    """
    Concrete ``AsyncService`` for ``Post`` used in all service tests.

    Decorated with ``@Singleton`` so the DI test can register it via
    ``container.register(ConcretePostService)``.  The decorator does NOT
    prevent manual instantiation — ``ConcretePostService(...)`` still works.

    Thread safety:  ⚠️ Singleton; each method opens its own UoW.
    Async safety:   ✅ All public methods are ``async def``.
    """

    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer: Inject[AbstractAuthorizer],
        assembler: Inject[
            AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]
        ],
    ) -> None:
        """
        Args:
            uow_provider: Factory for fresh ``AsyncUnitOfWork`` instances.
            authorizer:   Authorization gate — called for every operation.
            assembler:    DTO ↔ domain mapper for ``Post``.
        """
        super().__init__(
            uow_provider=uow_provider,
            authorizer=authorizer,
            assembler=assembler,
        )

    def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[Post, Any]:
        """
        Return the Post repository from ``uow``.

        Args:
            uow: The open unit of work for the current operation.

        Returns:
            ``uow.posts`` — the ``InMemoryPostRepository`` in tests.
        """
        return uow.posts  # type: ignore[attr-defined]


# ── pytest fixtures ────────────────────────────────────────────────────────────


@pytest.fixture()
def uow_provider() -> FakeUoWProvider:
    """
    Fresh ``FakeUoWProvider`` for each test.

    Returns:
        ``FakeUoWProvider`` with an empty ``InMemoryPostRepository``.
    """
    # Fresh per test — no state bleeds between test cases
    return FakeUoWProvider()


@pytest.fixture()
def assembler() -> PostAssembler:
    """Stateless assembler — safe to reuse across tests."""
    return PostAssembler()


@pytest.fixture()
def svc(uow_provider: FakeUoWProvider, assembler: PostAssembler) -> ConcretePostService:
    """
    Service wired with a permissive (allow-all) ``BaseAuthorizer``.

    Use this fixture for tests that exercise the happy path — all operations
    are allowed, repository behaviour is what matters.

    Args:
        uow_provider: Shared provider — seed data on ``uow_provider.repo``.
        assembler:    Post assembler.

    Returns:
        ``ConcretePostService`` with ``BaseAuthorizer``.
    """
    return ConcretePostService(
        uow_provider=uow_provider,
        authorizer=BaseAuthorizer(),
        assembler=assembler,
    )


@pytest.fixture()
def deny_svc(
    uow_provider: FakeUoWProvider, assembler: PostAssembler
) -> ConcretePostService:
    """
    Service wired with a deny-all ``DenyAllAuthorizer``.

    Uses the same ``uow_provider`` as the default fixture so tests can
    inspect ``uow_created_count`` to verify the UoW was never opened.

    Args:
        uow_provider: Shared provider — same instance as ``svc`` fixture.
        assembler:    Post assembler.

    Returns:
        ``ConcretePostService`` with ``DenyAllAuthorizer``.
    """
    return ConcretePostService(
        uow_provider=uow_provider,
        authorizer=DenyAllAuthorizer(),
        assembler=assembler,
    )


@pytest.fixture()
def ctx() -> AuthContext:
    """Anonymous caller — ``BaseAuthorizer`` allows it; ``DenyAllAuthorizer`` denies it."""
    return AuthContext()


@pytest.fixture()
def seeded_post(uow_provider: FakeUoWProvider) -> Post:
    """
    Pre-seed a ``Post`` in the shared repository.

    Directly writes to ``uow_provider.repo._store`` to bypass the service —
    this is intentional: we want to test what the service DOES with existing
    data, not how it creates it.

    Returns:
        The pre-seeded ``Post`` instance (also available via ``uow_provider.repo``).
    """
    post = Post(pk="post-1", title="original title")
    post.created_at = _SENTINEL_TS
    post.updated_at = _SENTINEL_TS
    # Direct store write — no service call, no assembler, no UoW overhead
    uow_provider.repo._store["post-1"] = post
    return post


# ── TestAsyncServiceEntityType ────────────────────────────────────────────────


class TestAsyncServiceEntityType:
    """``_entity_type()`` derives the domain class from generic type arguments."""

    def test_entity_type_returns_post_class(self, svc: ConcretePostService) -> None:
        # _entity_type() must return Post — the first generic arg of
        # AsyncService[Post, str, CreatePostDTO, PostReadDTO, UpdatePostDTO].
        assert svc._entity_type() is Post

    def test_entity_type_is_cached(self, svc: ConcretePostService) -> None:
        # Second call must return the exact same object — no repeated MRO walk.
        # Caching is on type(svc) so it survives across different svc instances.
        first = svc._entity_type()
        second = svc._entity_type()
        assert first is second


# ── TestAsyncServiceGet ───────────────────────────────────────────────────────


class TestAsyncServiceGet:
    """
    ``get(pk, ctx)`` — fetch by pk, authorize on instance, return ReadDTO.

    Authorization order (per service module design):
        1. Fetch the entity (raises ServiceNotFoundError if missing).
        2. Authorize READ on the fetched instance.
        3. Assemble and return the ReadDTO.

    Consequence: a missing entity always raises ServiceNotFoundError even
    when the caller has no READ permission — prevents an existence oracle.
    """

    async def test_returns_read_dto_when_found(
        self, svc: ConcretePostService, seeded_post: Post, ctx: AuthContext
    ) -> None:
        # Happy path: entity exists, auth passes → ReadDTO returned.
        result = await svc.get("post-1", ctx)
        assert isinstance(result, PostReadDTO)
        assert result.pk == "post-1"
        assert result.title == "original title"

    async def test_raises_not_found_when_missing(
        self, svc: ConcretePostService, ctx: AuthContext
    ) -> None:
        # No entity in the store → ServiceNotFoundError must be raised.
        with pytest.raises(ServiceNotFoundError) as exc_info:
            await svc.get("does-not-exist", ctx)
        # Verify the error carries structured information for the HTTP adapter.
        assert exc_info.value.entity_id == "does-not-exist"
        assert exc_info.value.entity_cls is Post

    async def test_not_found_raised_before_auth(
        self, deny_svc: ConcretePostService, ctx: AuthContext
    ) -> None:
        # A missing entity raises ServiceNotFoundError even with a deny-all
        # authorizer — fetch happens inside the UoW before auth is checked.
        # This prevents an existence oracle: a 403 would leak the entity exists.
        with pytest.raises(ServiceNotFoundError):
            await deny_svc.get("does-not-exist", ctx)

    async def test_raises_auth_error_when_denied(
        self,
        deny_svc: ConcretePostService,
        seeded_post: Post,
        ctx: AuthContext,
    ) -> None:
        # Entity exists but auth denies READ → ServiceAuthorizationError.
        with pytest.raises(ServiceAuthorizationError) as exc_info:
            await deny_svc.get("post-1", ctx)
        assert exc_info.value.operation == "read"
        assert exc_info.value.entity_cls is Post


# ── TestAsyncServiceList ──────────────────────────────────────────────────────


class TestAsyncServiceList:
    """
    ``list(params, ctx)`` — authorize on collection, query, return ReadDTOs.

    Authorization fires BEFORE make_uow() for list() — denied callers never
    open a DB connection.  Verified via uow_provider.uow_created_count == 0.
    """

    async def test_returns_all_dtos_when_data_present(
        self,
        svc: ConcretePostService,
        uow_provider: FakeUoWProvider,
        seeded_post: Post,
        ctx: AuthContext,
    ) -> None:
        # One entity seeded → list() must return exactly one ReadDTO.
        result = await svc.list(QueryParams(), ctx)
        assert len(result) == 1
        assert isinstance(result[0], PostReadDTO)
        assert result[0].pk == "post-1"

    async def test_returns_empty_list_when_no_data(
        self, svc: ConcretePostService, ctx: AuthContext
    ) -> None:
        # Empty store → list() must return an empty list (not None, not error).
        result = await svc.list(QueryParams(), ctx)
        assert result == []

    async def test_raises_auth_error_when_denied(
        self, deny_svc: ConcretePostService, ctx: AuthContext
    ) -> None:
        with pytest.raises(ServiceAuthorizationError) as exc_info:
            await deny_svc.list(QueryParams(), ctx)
        assert exc_info.value.operation == "list"

    async def test_auth_checked_before_db_is_opened(
        self,
        deny_svc: ConcretePostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        # Auth fires before make_uow() for list() — denied callers must never
        # open a DB connection (no UoW created, no session acquired).
        with pytest.raises(ServiceAuthorizationError):
            await deny_svc.list(QueryParams(), ctx)

        # If auth had not fired first, uow_created_count would be 1.
        assert uow_provider.uow_created_count == 0, (
            "make_uow() was called before auth check — "
            "auth must fire BEFORE the DB is touched for list()"
        )


# ── TestAsyncServiceCreate ────────────────────────────────────────────────────


class TestAsyncServiceCreate:
    """
    ``create(dto, ctx)`` — authorize on collection, persist, return ReadDTO.

    Authorization fires BEFORE make_uow() — denied callers never touch the DB.
    """

    async def test_creates_entity_and_returns_dto(
        self,
        svc: ConcretePostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        # Happy path: auth passes, entity is inserted, ReadDTO is returned.
        dto = CreatePostDTO(title="new post")
        result = await svc.create(dto, ctx)

        assert isinstance(result, PostReadDTO)
        assert result.title == "new post"
        # pk must be set (the repo assigns one on INSERT)
        assert result.pk is not None and result.pk != ""

    async def test_entity_is_persisted_after_create(
        self,
        svc: ConcretePostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        # Verify the repo.save_count increased — entity was actually persisted.
        assert uow_provider.repo.save_count == 0
        await svc.create(CreatePostDTO(title="persisted"), ctx)
        assert uow_provider.repo.save_count == 1

    async def test_raises_auth_error_when_denied(
        self, deny_svc: ConcretePostService, ctx: AuthContext
    ) -> None:
        with pytest.raises(ServiceAuthorizationError) as exc_info:
            await deny_svc.create(CreatePostDTO(title="blocked"), ctx)
        assert exc_info.value.operation == "create"

    async def test_auth_checked_before_db_is_opened(
        self,
        deny_svc: ConcretePostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        # Auth fires before make_uow() for create() — denied callers must never
        # open a DB connection or have any entity written.
        with pytest.raises(ServiceAuthorizationError):
            await deny_svc.create(CreatePostDTO(title="blocked"), ctx)

        # Both the UoW and the repo must be untouched.
        assert uow_provider.uow_created_count == 0, (
            "make_uow() was called before auth check — "
            "auth must fire BEFORE the DB is touched for create()"
        )
        assert uow_provider.repo.save_count == 0


# ── TestAsyncServiceUpdate ────────────────────────────────────────────────────


class TestAsyncServiceUpdate:
    """
    ``update(pk, dto, ctx)`` — fetch, authorize on instance, apply, persist.

    Authorization order:
        1. Fetch inside UoW (ServiceNotFoundError if missing).
        2. Authorize UPDATE on the current entity.
        3. Apply dto, persist, return updated ReadDTO.
    """

    async def test_updates_entity_and_returns_dto(
        self,
        svc: ConcretePostService,
        seeded_post: Post,
        ctx: AuthContext,
    ) -> None:
        dto = UpdatePostDTO(title="updated title")
        result = await svc.update("post-1", dto, ctx)

        assert isinstance(result, PostReadDTO)
        assert result.title == "updated title"
        assert result.pk == "post-1"

    async def test_update_persists_change(
        self,
        svc: ConcretePostService,
        uow_provider: FakeUoWProvider,
        seeded_post: Post,
        ctx: AuthContext,
    ) -> None:
        # save_count must increase — the update was actually written to store.
        before = uow_provider.repo.save_count
        await svc.update("post-1", UpdatePostDTO(title="changed"), ctx)
        assert uow_provider.repo.save_count == before + 1

    async def test_update_with_none_fields_preserves_existing(
        self,
        svc: ConcretePostService,
        seeded_post: Post,
        ctx: AuthContext,
    ) -> None:
        # None in dto means "no change" — existing value must be preserved.
        result = await svc.update("post-1", UpdatePostDTO(title=None), ctx)
        assert result.title == "original title"

    async def test_raises_not_found_when_missing(
        self, svc: ConcretePostService, ctx: AuthContext
    ) -> None:
        with pytest.raises(ServiceNotFoundError) as exc_info:
            await svc.update("ghost", UpdatePostDTO(title="x"), ctx)
        assert exc_info.value.entity_id == "ghost"
        assert exc_info.value.entity_cls is Post

    async def test_raises_auth_error_when_denied(
        self,
        deny_svc: ConcretePostService,
        seeded_post: Post,
        ctx: AuthContext,
    ) -> None:
        with pytest.raises(ServiceAuthorizationError) as exc_info:
            await deny_svc.update("post-1", UpdatePostDTO(title="x"), ctx)
        assert exc_info.value.operation == "update"
        assert exc_info.value.entity_cls is Post


# ── TestAsyncServiceDelete ────────────────────────────────────────────────────


class TestAsyncServiceDelete:
    """
    ``delete(pk, ctx)`` — fetch, authorize on instance, delete.

    Authorization order mirrors update(): fetch first (inside UoW), then auth.
    """

    async def test_deletes_entity_successfully(
        self,
        svc: ConcretePostService,
        uow_provider: FakeUoWProvider,
        seeded_post: Post,
        ctx: AuthContext,
    ) -> None:
        # Entity exists before delete, must be gone after.
        assert "post-1" in uow_provider.repo._store
        await svc.delete("post-1", ctx)
        assert "post-1" not in uow_provider.repo._store

    async def test_delete_returns_none(
        self,
        svc: ConcretePostService,
        seeded_post: Post,
        ctx: AuthContext,
    ) -> None:
        # delete() must return None on success — any other return is a bug.
        result = await svc.delete("post-1", ctx)
        assert result is None

    async def test_raises_not_found_when_missing(
        self, svc: ConcretePostService, ctx: AuthContext
    ) -> None:
        with pytest.raises(ServiceNotFoundError) as exc_info:
            await svc.delete("ghost", ctx)
        assert exc_info.value.entity_id == "ghost"
        assert exc_info.value.entity_cls is Post

    async def test_raises_auth_error_when_denied(
        self,
        deny_svc: ConcretePostService,
        seeded_post: Post,
        ctx: AuthContext,
    ) -> None:
        with pytest.raises(ServiceAuthorizationError) as exc_info:
            await deny_svc.delete("post-1", ctx)
        assert exc_info.value.operation == "delete"
        assert exc_info.value.entity_cls is Post

    async def test_entity_not_deleted_when_auth_denied(
        self,
        deny_svc: ConcretePostService,
        uow_provider: FakeUoWProvider,
        seeded_post: Post,
        ctx: AuthContext,
    ) -> None:
        # Auth failure must prevent the actual deletion — entity stays in store.
        with pytest.raises(ServiceAuthorizationError):
            await deny_svc.delete("post-1", ctx)
        # Entity must still be present — ServiceAuthorizationError rolled back.
        assert "post-1" in uow_provider.repo._store


# ── TestAsyncServiceDI ────────────────────────────────────────────────────────


class TestAsyncServiceDI:
    """
    Verify that ``ConcretePostService`` can be wired by the DI container.

    All three test doubles (``FakeUoWProvider``, ``PostAssembler``,
    ``ConcretePostService``) are decorated with ``@Singleton`` and registered
    via ``container.register()``.  Providify's generics support (0.1.3+)
    resolves ``PostAssembler`` against ``Inject[AbstractDTOAssembler[Post, ...]]``
    through the class's generic base — no ``@Provider`` factory needed.

    DESIGN: why a fresh container per test
        Each test builds its own ``DIContainer`` — singleton caches from one
        test must not affect another (same principle as all DI tests).

    Edge cases:
        - All registered classes must be ``@Singleton`` — ``ConcretePostService``
          is ``@Singleton`` and providify enforces that singletons cannot
          depend on DEPENDENT-scope bindings (scope leak violation).
    """

    @pytest.fixture()
    def di_container(self) -> DIContainer:
        """
        Fresh ``DIContainer`` with all dependencies registered.

        Returns:
            Container ready to resolve ``ConcretePostService``.
        """
        container = DIContainer()

        # BaseAuthorizer carries @Singleton(priority=-(2**31)) — scanned
        # from its module so it auto-registers as AbstractAuthorizer.
        container.scan("varco_core.auth.authorizer")

        # FakeUoWProvider is @Singleton — registers as IUoWProvider.
        container.register(FakeUoWProvider)

        # PostAssembler is @Singleton — generics support registers it under
        # AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]
        # so Inject[AbstractDTOAssembler[Post, ...]] on ConcretePostService resolves.
        container.register(PostAssembler)

        # ConcretePostService is @Singleton — Inject[...] annotations on __init__
        # drive auto-wiring; no @Provider factory function needed.
        container.register(ConcretePostService)

        return container

    def test_resolves_service_from_container(self, di_container: DIContainer) -> None:
        # DI must successfully resolve ConcretePostService with all deps wired.
        svc = di_container.get(ConcretePostService)
        assert isinstance(svc, ConcretePostService)

    def test_injected_authorizer_is_base_authorizer(
        self, di_container: DIContainer
    ) -> None:
        # AbstractAuthorizer resolves to BaseAuthorizer (only binding registered).
        svc = di_container.get(ConcretePostService)
        assert isinstance(svc._authorizer, BaseAuthorizer)

    def test_injected_uow_provider_is_fake(self, di_container: DIContainer) -> None:
        # IUoWProvider resolves to FakeUoWProvider (@Singleton on the class).
        svc = di_container.get(ConcretePostService)
        assert isinstance(svc._uow_provider, FakeUoWProvider)

    def test_injected_assembler_is_post_assembler(
        self, di_container: DIContainer
    ) -> None:
        # AbstractDTOAssembler[Post, ...] resolves to PostAssembler.
        svc = di_container.get(ConcretePostService)
        assert isinstance(svc._assembler, PostAssembler)

    def test_service_is_singleton_in_container(self, di_container: DIContainer) -> None:
        # @Singleton scope — same instance returned on every container.get() call.
        first = di_container.get(ConcretePostService)
        second = di_container.get(ConcretePostService)
        assert first is second

    async def test_di_wired_service_executes_end_to_end(
        self, di_container: DIContainer
    ) -> None:
        # Full round-trip: resolve service from DI, call create() + get().
        # Validates that the DI-constructed service is fully functional —
        # not just the right type.
        svc = di_container.get(ConcretePostService)
        ctx = AuthContext()

        # Create a post via the DI-resolved service
        read_dto = await svc.create(CreatePostDTO(title="di-test post"), ctx)
        assert read_dto.title == "di-test post"

        # Retrieve the created post — confirms UoW and repo are wired correctly
        fetched = await svc.get(read_dto.pk, ctx)
        assert fetched.title == "di-test post"
        assert fetched.pk == read_dto.pk


# ── AsyncValidatorServiceMixin ────────────────────────────────────────────────


class _AlwaysPassAsyncValidator(AsyncDomainModelValidator[Post]):
    """Async validator that always passes — used to verify the hook is called."""

    def __init__(self) -> None:
        self.call_count: int = 0

    async def validate(self, value: Post) -> ValidationResult:
        self.call_count += 1
        return ValidationResult.ok()


class _AlwaysFailAsyncValidator(AsyncDomainModelValidator[Post]):
    """Async validator that always fails — used to verify errors propagate."""

    async def validate(self, value: Post) -> ValidationResult:
        return ValidationResult.error("async rule violated", field="title")


class _AsyncValidatedPostService(
    AsyncValidatorServiceMixin[Post, str, CreatePostDTO, PostReadDTO, UpdatePostDTO],
    AsyncService[Post, str, CreatePostDTO, PostReadDTO, UpdatePostDTO],
):
    """Concrete service with AsyncValidatorServiceMixin injected."""

    def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[Post, Any]:
        return uow.posts  # type: ignore[attr-defined]


class TestAsyncValidatorServiceMixin:
    @pytest.fixture()
    def _make_svc(self, uow_provider: FakeUoWProvider, assembler: PostAssembler):
        """Factory that wires a service with a given async validator instance."""

        def _factory(validator):
            svc = _AsyncValidatedPostService(
                uow_provider=uow_provider,
                authorizer=BaseAuthorizer(),
                assembler=assembler,
            )
            _AsyncValidatedPostService._async_validator_entity = validator
            return svc

        yield _factory
        # Reset class-level attribute after each test
        _AsyncValidatedPostService._async_validator_entity = None

    async def test_passing_validator_allows_create(self, _make_svc) -> None:
        """create() succeeds when the async validator passes."""
        validator = _AlwaysPassAsyncValidator()
        svc = _make_svc(validator)
        read_dto = await svc.create(CreatePostDTO(title="hello"), AuthContext())
        assert read_dto.title == "hello"
        assert validator.call_count == 1

    async def test_failing_validator_blocks_create(self, _make_svc) -> None:
        """create() raises ServiceValidationError when async validator fails."""
        svc = _make_svc(_AlwaysFailAsyncValidator())
        with pytest.raises(ServiceValidationError, match="async rule violated"):
            await svc.create(CreatePostDTO(title="bad"), AuthContext())

    async def test_passing_validator_allows_update(
        self, _make_svc, uow_provider: FakeUoWProvider
    ) -> None:
        """update() succeeds when the async validator passes."""
        validator = _AlwaysPassAsyncValidator()
        svc = _make_svc(validator)
        ctx = AuthContext()
        read_dto = await svc.create(CreatePostDTO(title="original"), ctx)
        updated = await svc.update(read_dto.pk, UpdatePostDTO(title="updated"), ctx)
        assert updated.title == "updated"
        # Called once for create and once for update
        assert validator.call_count == 2

    async def test_failing_validator_blocks_update(
        self, _make_svc, uow_provider: FakeUoWProvider
    ) -> None:
        """update() raises ServiceValidationError when async validator fails."""
        # Use a passing validator for create, then swap to failing for update
        svc = _make_svc(_AlwaysPassAsyncValidator())
        ctx = AuthContext()
        read_dto = await svc.create(CreatePostDTO(title="ok"), ctx)

        # Swap validator to always-fail
        _AsyncValidatedPostService._async_validator_entity = _AlwaysFailAsyncValidator()
        with pytest.raises(ServiceValidationError):
            await svc.update(read_dto.pk, UpdatePostDTO(title="blocked"), ctx)

    async def test_no_validator_is_noop(
        self, uow_provider: FakeUoWProvider, assembler: PostAssembler
    ) -> None:
        """When _async_validator_entity is None, create/update work normally."""
        # Explicitly ensure no validator is set
        _AsyncValidatedPostService._async_validator_entity = None
        svc = _AsyncValidatedPostService(
            uow_provider=uow_provider,
            authorizer=BaseAuthorizer(),
            assembler=assembler,
        )
        read_dto = await svc.create(CreatePostDTO(title="no validator"), AuthContext())
        assert read_dto.title == "no validator"
