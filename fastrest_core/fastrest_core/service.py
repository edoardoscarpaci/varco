"""
fastrest_core.service
======================
Abstract service base class — the business logic layer.

The service layer sits between the HTTP adapter and the repository / UoW
layer.  It is the **only** layer that:

- Enforces authorization (via ``AbstractAuthorizer``).
- Orchestrates multi-repository transactions inside an ``AsyncUnitOfWork``.
- Delegates DTO ↔ DomainModel translation to an injected ``AbstractDTOAssembler``.
- Raises typed ``ServiceException`` subclasses instead of raw DB errors.

Two abstractions live in this module:

``IUoWProvider``
    Minimal interface for anything that can produce a fresh
    ``AsyncUnitOfWork``.  ``RepositoryProvider`` already satisfies this
    interface via its ``make_uow()`` method — bind ``RepositoryProvider``
    as ``IUoWProvider`` in the DI container.

``AsyncService[D, PK, C, R, U]``
    Generic abstract service.  Concrete subclasses implement a single
    abstract method (``_get_repo``) to wire the service to the correct
    UoW attribute.  All other concerns are handled by injected collaborators.

Hierarchy::

    AsyncService[D, PK, C, R, U]
    │
    ├── get(pk, ctx)          → R
    ├── list(params, ctx)     → list[R]
    ├── create(dto, ctx)      → R
    ├── update(pk, dto, ctx)  → R
    └── delete(pk, ctx)       → None

    (only one abstract method — implement in subclass)
    └── _get_repo(uow) → AsyncRepository[D, PK]

Type parameters::

    D   — DomainModel subclass (e.g. ``Post``)
    PK  — Primary key type (e.g. ``UUID``, ``int``)
    C   — CreateDTO subclass
    R   — ReadDTO subclass
    U   — UpdateDTO subclass

Generic injection pattern::

    Concrete services declare ``Inject[AbstractDTOAssembler[D, C, R, U]]``
    with the concrete types resolved — this is how the DI container knows
    which assembler to inject.  The base class declares the same annotation
    using TypeVars so the type checker validates correctness end-to-end:

    # Base class (TypeVars → type checker resolves per subclass)
    def __init__(self, assembler: Inject[AbstractDTOAssembler[D, C, R, U]])

    # Concrete class (concrete types → DI resolves correct binding)
    def __init__(self, assembler: Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]])

Minimal concrete service example::

    from dataclasses import replace
    from providify import Inject
    from uuid import UUID

    class PostService(AsyncService[Post, UUID, CreatePostDTO, PostReadDTO, UpdatePostDTO]):

        def __init__(
            self,
            uow_provider: Inject[IUoWProvider],
            authorizer:   Inject[AbstractAuthorizer],
            assembler:    Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]],
        ) -> None:
            super().__init__(
                uow_provider=uow_provider,
                authorizer=authorizer,
                assembler=assembler,
            )

        def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[Post, UUID]:
            return uow.posts  # type: ignore[attr-defined]

DI wiring example (with providify)::

    @Provider(singleton=True)
    def post_assembler() -> AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]:
        return PostAssembler()

    @Provider(singleton=True)
    def post_service(
        uow_provider: IUoWProvider,
        authorizer:   AbstractAuthorizer,
        assembler:    AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO],
    ) -> PostService:
        return PostService(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

DESIGN: Inject[AbstractDTOAssembler[D, C, R, U]] with TypeVars on the base class
    The base class carries TypeVar annotations — the type checker resolves
    D=Post, C=CreatePostDTO, R=PostReadDTO, U=UpdatePostDTO when inspecting
    a concrete subclass.  The concrete class overrides ``__init__`` with
    explicit types so providify's runtime annotation introspection sees
    the concrete generic alias (e.g. AbstractDTOAssembler[Post, ...]).
    ✅ Full static type checking end-to-end.
    ✅ DI container sees concrete generic aliases in the concrete class.
    ❌ Concrete services must repeat the Inject annotations — unavoidable
       because TypeVars are unresolved at runtime in the base class.

DESIGN: authorization before opening the UoW for CREATE / LIST
    For ``create`` and ``list``: authorization is checked before acquiring
    a DB connection — denied callers never touch the DB.
    For ``get``, ``update``, ``delete``: the entity is fetched first (inside
    the UoW) so the authorizer can inspect it for ownership checks, then
    authorization runs, then the operation executes.

Thread safety:  ⚠️ Service is a singleton; each method creates its own UoW.
Async safety:   ✅ All public methods are ``async def``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from providify import Inject

from fastrest_core.assembler import AbstractDTOAssembler
from fastrest_core.auth import AbstractAuthorizer, Action, AuthContext, Resource
from fastrest_core.dto import CreateDTO, ReadDTO, UpdateDTO
from fastrest_core.exception.service import ServiceNotFoundError
from fastrest_core.model import DomainModel
from fastrest_core.uow import AsyncUnitOfWork

if TYPE_CHECKING:
    # Imported only for type hints — avoids pulling in query machinery at
    # runtime for services that never use the query system.
    from fastrest_core.query.params import QueryParams
    from fastrest_core.repository import AsyncRepository

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")
C = TypeVar("C", bound=CreateDTO)
R = TypeVar("R", bound=ReadDTO)
U = TypeVar("U", bound=UpdateDTO)


# ── IUoWProvider ──────────────────────────────────────────────────────────────


class IUoWProvider(ABC):
    """
    Minimal interface for anything that can produce a fresh ``AsyncUnitOfWork``.

    Exists so the DI container can resolve the UoW factory by named type
    rather than requiring a raw ``Callable[[], AsyncUnitOfWork]`` argument.

    ``RepositoryProvider`` already satisfies this interface via its own
    ``make_uow()`` method — bind it as ``IUoWProvider`` in the container::

        @Provider(singleton=True)
        def uow_provider(repo_provider: RepositoryProvider) -> IUoWProvider:
            return repo_provider

    DESIGN: interface over Callable[[], AsyncUnitOfWork]
        ✅ Named type — DI resolves automatically with ``Inject[IUoWProvider]``.
        ✅ Testable — inject a fake that returns an in-memory UoW.
        ✅ ``RepositoryProvider`` satisfies the interface without modification.
        ❌ One extra class vs. a plain callable — justified by DI ergonomics.

    Thread safety:  ✅ Implementations must be stateless singletons.
    Async safety:   ✅ ``make_uow()`` is synchronous — the UoW manages async
                       lifecycle internally via ``__aenter__`` / ``__aexit__``.
    """

    @abstractmethod
    def make_uow(self) -> AsyncUnitOfWork:
        """
        Return a fresh ``AsyncUnitOfWork`` ready for use as an async context
        manager.

        A new UoW — and therefore a new DB session — is returned on every
        call so concurrent requests never share a session.

        Returns:
            A fresh, un-started ``AsyncUnitOfWork``.  Use with
            ``async with uow:`` to begin the transaction.

        Edge cases:
            - The returned UoW is not yet started — ``__aenter__`` must be
              called before any repository operation.
            - Each call produces an independent UoW; two calls give two
              separate sessions.
        """


# ── AsyncService ──────────────────────────────────────────────────────────────


class AsyncService(ABC, Generic[D, PK, C, R, U]):
    """
    Abstract async service for a single domain entity type.

    Concrete subclasses only implement ``_get_repo()`` to connect the service
    to the correct UoW attribute.  All mapping and authorization logic is
    handled by injected collaborators.

    The base class carries ``Inject[AbstractDTOAssembler[D, C, R, U]]`` with
    TypeVars so the **type checker** can verify end-to-end correctness.
    Concrete subclasses **must** override ``__init__`` with explicit concrete
    types so the DI container can resolve the correct binding at runtime::

        # ✅ Concrete — DI resolves AbstractDTOAssembler[Post, CreatePostDTO, ...]
        class PostService(AsyncService[Post, UUID, CreatePostDTO, PostReadDTO, UpdatePostDTO]):
            def __init__(
                self,
                uow_provider: Inject[IUoWProvider],
                authorizer:   Inject[AbstractAuthorizer],
                assembler:    Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]],
            ) -> None:
                super().__init__(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

            def _get_repo(self, uow):
                return uow.posts  # type: ignore[attr-defined]

    Thread safety:  ⚠️ Service is a singleton; each method creates its own UoW.
    Async safety:   ✅ All public methods are ``async def``.
    """

    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer: Inject[AbstractAuthorizer],
        # TypeVars D, C, R, U are bound to this class's generic parameters.
        # The type checker resolves them to concrete types in each subclass.
        # Concrete subclasses must override __init__ with explicit concrete
        # types so the DI container sees the fully resolved generic alias.
        assembler: Inject[AbstractDTOAssembler[D, C, R, U]],
    ) -> None:
        """
        Args:
            uow_provider: Injected ``IUoWProvider``.  Called once per public
                          method to produce a fresh ``AsyncUnitOfWork``.
            authorizer:   Injected ``AbstractAuthorizer``.  Handles all entity
                          types by dispatching on ``resource.entity_type``.
            assembler:    Injected ``AbstractDTOAssembler[D, C, R, U]``.
                          Translates DTOs ↔ domain entities for this service's
                          entity type.

        Edge cases:
            - ``uow_provider`` is stored (not called) — each public method
              calls ``make_uow()`` to get a fresh session.
            - All three injected objects must be stateless — they are shared
              across concurrent requests.
        """
        # Stored as references — make_uow() is called per-operation so each
        # request gets its own isolated DB session.
        self._uow_provider = uow_provider
        self._authorizer = authorizer
        self._assembler = assembler

    # ── Public CRUD methods ───────────────────────────────────────────────────

    async def get(self, pk: PK, ctx: AuthContext) -> R:
        """
        Fetch a single entity by primary key and return its ``ReadDTO``.

        Authorization order:
        1. Fetch the entity (raises ``ServiceNotFoundError`` if missing).
        2. Authorize ``Action.READ`` on the fetched instance.
        3. Assemble and return the ``ReadDTO``.

        Args:
            pk:  Primary key of the entity to fetch.
            ctx: Caller's identity and grants.

        Returns:
            The ``ReadDTO`` for the fetched entity.

        Raises:
            ServiceNotFoundError:      No entity with ``pk`` exists.
            ServiceAuthorizationError: Caller is not allowed to read it.

        Edge cases:
            - ``ServiceNotFoundError`` is raised BEFORE authorization —
              prevents an existence oracle (a 403 would reveal the entity
              exists even when the caller has no permission to read it).
        """
        async with self._uow_provider.make_uow() as uow:
            entity = await self._get_repo(uow).find_by_id(pk)
            if entity is None:
                raise ServiceNotFoundError(pk, self._entity_type())

            # Authorization after fetching so the authorizer can perform
            # ownership checks (e.g. entity.owner_id == ctx.user_id).
            await self._authorizer.authorize(
                ctx,
                Action.READ,
                Resource(entity_type=self._entity_type(), entity=entity),
            )
            return self._assembler.to_read_dto(entity)

    async def list(self, params: QueryParams, ctx: AuthContext) -> list[R]:
        """
        Query entities matching ``params`` and return their ``ReadDTO``\\s.

        Authorization is checked on the collection before any DB access —
        denied callers never open a DB connection.

        Args:
            params: ``QueryParams`` with filter, sort, and pagination.
            ctx:    Caller's identity and grants.

        Returns:
            List of ``ReadDTO``\\s for matching entities.  Empty list when
            nothing matches.

        Raises:
            ServiceAuthorizationError: Caller is not allowed to list.

        Edge cases:
            - ``QueryParams()`` (all defaults) fetches all entities — always
              use pagination (``params.limit``) on large tables.
            - For row-level filtering (return only the caller's own records),
              override this method and narrow ``params`` before calling
              ``_get_repo(uow).find_by_query()``.
        """
        # Authorize before opening the UoW — denied callers never touch the DB
        await self._authorizer.authorize(
            ctx,
            Action.LIST,
            Resource(entity_type=self._entity_type()),
        )

        async with self._uow_provider.make_uow() as uow:
            entities = await self._get_repo(uow).find_by_query(params)
            return [self._assembler.to_read_dto(e) for e in entities]

    async def create(self, dto: C, ctx: AuthContext) -> R:
        """
        Create a new entity from ``dto`` and return its ``ReadDTO``.

        Authorization is checked before opening the UoW — denied callers
        never touch the DB.

        Args:
            dto: The ``CreateDTO`` payload.
            ctx: Caller's identity and grants.

        Returns:
            The ``ReadDTO`` for the newly created entity (with ``pk`` set).

        Raises:
            ServiceAuthorizationError: Caller is not allowed to create.
            ServiceConflictError:      Business-rule violation.
            ServiceValidationError:    Business rule violated by ``dto``.
        """
        # Authorize before opening the UoW — no entity exists yet, so only
        # type-level and wildcard grants apply (no ownership to check).
        await self._authorizer.authorize(
            ctx,
            Action.CREATE,
            Resource(entity_type=self._entity_type()),
        )

        async with self._uow_provider.make_uow() as uow:
            entity = self._assembler.to_domain(dto)
            saved = await self._get_repo(uow).save(entity)
            return self._assembler.to_read_dto(saved)

    async def update(self, pk: PK, dto: U, ctx: AuthContext) -> R:
        """
        Update an existing entity and return its updated ``ReadDTO``.

        Authorization order:
        1. Fetch the entity (raises ``ServiceNotFoundError`` if missing).
        2. Authorize ``Action.UPDATE`` on the current entity state.
        3. Apply ``dto``, persist, and return the ``ReadDTO``.

        Args:
            pk:  Primary key of the entity to update.
            dto: The ``UpdateDTO`` payload.
            ctx: Caller's identity and grants.

        Returns:
            The ``ReadDTO`` reflecting the entity's new state.

        Raises:
            ServiceNotFoundError:      No entity with ``pk`` exists.
            ServiceAuthorizationError: Caller is not allowed to update it.
            ServiceConflictError:      Optimistic-lock conflict or business-
                                       rule violation.
            ServiceValidationError:    Business rule violated by ``dto``.

        Edge cases:
            - ``assembler.apply_update`` must return a *new* entity (via
              ``dataclasses.replace``), never mutate the input — the
              repository UPDATE path relies on ``_raw_orm`` being inherited.
        """
        async with self._uow_provider.make_uow() as uow:
            entity = await self._get_repo(uow).find_by_id(pk)
            if entity is None:
                raise ServiceNotFoundError(pk, self._entity_type())

            # Authorize with the current entity state so ownership checks work
            await self._authorizer.authorize(
                ctx,
                Action.UPDATE,
                Resource(entity_type=self._entity_type(), entity=entity),
            )

            updated = self._assembler.apply_update(entity, dto)
            saved = await self._get_repo(uow).save(updated)
            return self._assembler.to_read_dto(saved)

    async def delete(self, pk: PK, ctx: AuthContext) -> None:
        """
        Delete an entity by primary key.

        Authorization order:
        1. Fetch the entity (raises ``ServiceNotFoundError`` if missing).
        2. Authorize ``Action.DELETE`` on the fetched instance.
        3. Delete and commit.

        Args:
            pk:  Primary key of the entity to delete.
            ctx: Caller's identity and grants.

        Returns:
            ``None`` on success.

        Raises:
            ServiceNotFoundError:      No entity with ``pk`` exists.
            ServiceAuthorizationError: Caller is not allowed to delete it.

        Edge cases:
            - The UoW auto-commits on clean exit and rolls back on exception —
              no explicit ``commit()`` is needed here.
        """
        async with self._uow_provider.make_uow() as uow:
            entity = await self._get_repo(uow).find_by_id(pk)
            if entity is None:
                raise ServiceNotFoundError(pk, self._entity_type())

            # Authorize with the current entity so ownership checks work
            await self._authorizer.authorize(
                ctx,
                Action.DELETE,
                Resource(entity_type=self._entity_type(), entity=entity),
            )

            await self._get_repo(uow).delete(entity)

    # ── Abstract method — implement in concrete subclass ──────────────────────

    @abstractmethod
    def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[D, Any]:
        """
        Return the repository for this service's entity type from ``uow``.

        ``AsyncUnitOfWork`` exposes repositories as named attributes
        (``uow.users``, ``uow.posts``).  This one-line override is the only
        entity-specific piece of knowledge the service itself must declare::

            def _get_repo(self, uow):
                return uow.posts  # type: ignore[attr-defined]

        Args:
            uow: The open unit of work for the current operation.

        Returns:
            The ``AsyncRepository[D, PK]`` for this entity type.
        """

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _entity_type(self) -> type[D]:
        """
        Return the concrete domain class this service manages.

        Derived from the first generic type argument declared on the
        concrete subclass via ``__orig_bases__``.  Cached after the first
        call so MRO traversal happens at most once per service class.

        Returns:
            The bound ``DomainModel`` subclass (e.g. ``Post``).

        Raises:
            TypeError: The concrete service is not parameterized with a
                       DomainModel subclass as the first type argument.

        Edge cases:
            - Cached on ``type(self)`` — not on the ABC — so two concrete
              services with different entity types get independent caches.
            - Works with dynamically created subclasses because
              ``__orig_bases__`` is set by the metaclass at class-creation time.
        """
        import typing

        # Cache on the concrete class — not on the ABC — so PostService and
        # UserService each maintain their own cached entity type independently.
        if "_cached_entity_type" not in type(self).__dict__:
            for base in getattr(type(self), "__orig_bases__", ()):
                args = typing.get_args(base)
                origin = typing.get_origin(base)
                # Use issubclass so subclasses of AsyncService (e.g. TenantAwareService)
                # are also matched — direct `is AsyncService` check would fail when
                # the concrete service extends an intermediate abstract service.
                if (
                    origin is not None
                    and isinstance(origin, type)
                    and issubclass(origin, AsyncService)
                    and args
                ):
                    # First type arg is D — the DomainModel subclass
                    type(self)._cached_entity_type = args[0]  # type: ignore[attr-defined]
                    break
            else:
                raise TypeError(
                    f"{type(self).__name__} must be parameterized with a DomainModel "
                    f"subclass as the first type argument. "
                    f"Example: class {type(self).__name__}(AsyncService[MyEntity, ...]): ..."
                )
        return type(self)._cached_entity_type  # type: ignore[attr-defined]
