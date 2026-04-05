"""
varco_core.service
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
    ├── get(pk, ctx)                    → R
    ├── list(params, ctx)               → list[R]
    ├── count(params, ctx)              → int
    ├── paged_list(params, ctx, ...)    → PagedReadDTO[R]
    ├── create(dto, ctx)                → R
    ├── update(pk, dto, ctx)            → R
    └── delete(pk, ctx)                 → None

    (one required abstract method — implement in subclass)
    └── _get_repo(uow) → AsyncRepository[D, PK]

    (three optional override hooks — chain via super() for mixin composition)
    ├── _scoped_params(params, ctx) → QueryParams
    ├── _check_entity(entity, ctx)  → None
    └── _prepare_for_create(entity, ctx) → D

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

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Annotated, Any, Generic, TypeVar

from providify import Inject, InjectMeta

from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth import AbstractAuthorizer, Action, AuthContext, Resource
from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO
from varco_core.dto.pagination import PagedReadDTO, paged_response
from varco_core.event.base import CHANNEL_DEFAULT, Event
from varco_core.event.domain import (
    EntityCreatedEvent,
    EntityDeletedEvent,
    EntityUpdatedEvent,
)
from varco_core.event.producer import AbstractEventProducer, NoopEventProducer
from varco_core.exception.service import ServiceNotFoundError
from varco_core.model import DomainModel
from varco_core.tracing import current_correlation_id
from varco_core.uow import AsyncUnitOfWork

if TYPE_CHECKING:
    # Imported only for type hints — avoids pulling in query machinery at
    # runtime for services that never use the query system.
    from typing import AsyncIterator

    from varco_core.query.params import QueryParams
    from varco_core.repository import AsyncRepository

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")
C = TypeVar("C", bound=CreateDTO)
R = TypeVar("R", bound=ReadDTO)
U = TypeVar("U", bound=UpdateDTO)

# Anonymous / unauthenticated fallback context — used as the default value for
# all ``ctx`` parameters so callers can omit ``ctx`` entirely when no auth is
# needed (e.g. internal jobs, tests, no-auth endpoints).
#
# DESIGN: module-level singleton instead of ``None`` default
#   ✅ Safe as a default: AuthContext is frozen with only immutable fields
#      (frozenset, tuple) — no mutable-default-argument pitfall.
#   ✅ Authorizer always receives a real AuthContext object — no None guards.
#   ✅ BaseAuthorizer (permissive) passes it through unchanged.
#   ❌ A caller that forgets to pass ctx silently operates as anonymous —
#      only a concern when real auth is wired; permissive default catches it.
_ANON_CTX: AuthContext = AuthContext()


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
        # Optional — defaults to NoopEventProducer so services work without
        # any event infrastructure wired.  Concrete subclasses that want
        # events must add this parameter with InjectMeta(optional=True) so
        # the DI container supplies BusEventProducer when a bus is registered.
        producer: Annotated[AbstractEventProducer, InjectMeta(optional=True)] = None,
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
            producer:     Optional ``AbstractEventProducer``.  When ``None``
                          (default), a ``NoopEventProducer`` is used so no
                          events are emitted.  Inject ``BusEventProducer``
                          to enable event publishing.

        Edge cases:
            - ``uow_provider`` is stored (not called) — each public method
              calls ``make_uow()`` to get a fresh session.
            - All injected objects must be stateless — they are shared
              across concurrent requests.
            - ``producer`` defaults to ``NoopEventProducer`` — services
              never need a ``if self._producer is not None`` guard.
        """
        # Stored as references — make_uow() is called per-operation so each
        # request gets its own isolated DB session.
        self._uow_provider = uow_provider
        self._authorizer = authorizer
        self._assembler = assembler
        # Fall back to NoopEventProducer so _produce() calls are always safe
        # even when no bus is configured — Null Object pattern avoids guards.
        self._producer: AbstractEventProducer = producer or NoopEventProducer()

    # ── Protected event-publish helper ────────────────────────────────────────

    async def _emit(self, event: Event, *, channel: str = CHANNEL_DEFAULT) -> None:
        """
        Publish a single domain event via the injected producer.

        Service subclasses call ``_emit`` instead of accessing
        ``self._producer._produce()`` directly — this keeps the producer
        dependency encapsulated and avoids leaking the knowledge that
        ``_produce`` is a protected method on ``AbstractEventProducer``.

        Typical usage in a service hook::

            async def _after_create(self, entity: Post, read_dto: PostRead, ctx) -> None:
                await super()._after_create(entity, read_dto, ctx)
                await self._emit(PostCreatedEvent(post_id=entity.pk), channel="posts")

        Args:
            event:   The domain event to publish.
            channel: Target channel.  Defaults to ``CHANNEL_DEFAULT``
                     (``"default"``).

        Raises:
            Any exception propagated from the underlying bus implementation.

        Edge cases:
            - When no bus is configured, ``_producer`` is a ``NoopEventProducer``
              which silently discards the call — no guard needed.
            - If ``_produce`` raises, the exception propagates to the caller.
              The entity IS persisted.  For guaranteed delivery, use the outbox
              pattern (``OutboxRepository`` + ``OutboxRelay``).

        Async safety:   ✅ Delegates to ``AbstractEventProducer._produce``.
        Thread safety:  ✅ Stateless — ``_producer`` is an injected singleton.
        """
        await self._producer._produce(event, channel=channel)

    # ── Composable extension hooks ────────────────────────────────────────────
    # Override these in mixin subclasses to inject cross-cutting behaviour
    # (tenant scoping, soft-delete filtering, etc.) without duplicating the
    # full CRUD method bodies.  Always call super() so multiple mixins can
    # chain their hooks via MRO co-operative inheritance.

    def _scoped_params(self, params: QueryParams, ctx: AuthContext) -> QueryParams:
        """
        Narrow ``params`` before any ``list`` or ``count`` query.

        Override in mixin subclasses to prepend extra filter nodes (e.g.
        tenant scoping, soft-delete exclusion).  The base implementation
        returns ``params`` unchanged.

        **Chaining contract**: always end with ``return super()._scoped_params(params, ctx)``
        so multiple mixins in the MRO compose their filters additively.

        Args:
            params: Original ``QueryParams`` from the caller.
            ctx:    Caller's identity — may carry tenant ID or other metadata.

        Returns:
            A (possibly new) ``QueryParams`` with extra filter nodes injected.

        Example — inject a tenant filter::

            def _scoped_params(self, params, ctx):
                tid = ctx.metadata["tenant_id"]
                tenant_node = QueryBuilder().eq("tenant_id", tid).build()
                scoped_node = AndNode(tenant_node, params.node) if params.node else tenant_node
                return super()._scoped_params(dataclasses.replace(params, node=scoped_node), ctx)
        """
        # Base: no extra scoping — return params as-is.
        return params

    def _check_entity(self, entity: D, ctx: AuthContext) -> None:
        """
        Validate a fetched entity before the authorizer runs.

        Called by ``get``, ``update``, and ``delete`` immediately after
        ``find_by_id``.  Raise ``ServiceNotFoundError`` (not
        ``ServiceAuthorizationError``) to prevent existence oracles.

        **Chaining contract**: always end with ``super()._check_entity(entity, ctx)``
        so multiple mixins in the MRO each perform their own check.

        Args:
            entity: The entity fetched from the repository.
            ctx:    Caller's identity.

        Raises:
            ServiceNotFoundError: Entity should be treated as non-existent
                from the caller's perspective (e.g. wrong tenant, soft-deleted).

        Edge cases:
            - Raising ``ServiceAuthorizationError`` here would reveal the
              entity's existence to unauthorised callers — always use
              ``ServiceNotFoundError`` for cross-concern blocking.
        """
        # Base: no extra checks — entity passes through.
        return

    def _prepare_for_create(self, entity: D, ctx: AuthContext) -> D:
        """
        Stamp or transform a freshly assembled entity before it is saved.

        Called by ``create`` after the assembler's ``to_domain()`` returns
        and after authorisation has passed.  Use this hook to inject
        fields that belong to the cross-cutting concern (e.g. tenant_id,
        owner_id) rather than placing them in the DTO or the assembler.

        **Chaining contract**: always end with
        ``return super()._prepare_for_create(entity, ctx)`` so multiple
        mixins can each stamp their own fields.

        Args:
            entity: The unsaved domain entity produced by the assembler.
            ctx:    Caller's identity.

        Returns:
            A (possibly new) domain entity with cross-cutting fields stamped.
            Use ``dataclasses.replace(entity, field=value)`` — never mutate
            the input, as the original is still referenced by the caller.

        Edge cases:
            - The field being stamped must have ``init=True`` on its
              ``dataclass`` declaration so ``dataclasses.replace()`` can set it.
            - ``entity._raw_orm`` is ``None`` at this point — preserved by
              ``dataclasses.replace()`` automatically because it is not in
              ``__init__`` (``init=False``).
        """
        # Base: no stamping — return entity unchanged.
        return entity

    async def _after_create(self, entity: D, read_dto: R, ctx: AuthContext) -> None:
        """
        Hook called AFTER the UoW commits on a successful ``create()``.

        Override in mixin subclasses to react to entity creation without
        duplicating ``create()`` itself.  Typical uses: emit an audit event,
        send a notification, or update a secondary index.

        **Chaining contract**: always end with
        ``await super()._after_create(entity, read_dto, ctx)``
        so multiple mixins in the MRO each run their post-create logic.

        DESIGN: called after commit (not inside the UoW)
            ✅ The entity is durably persisted before any side-effect fires —
               no risk of emitting an audit event for a rolled-back write.
            ✅ Mirrors the ``_publish_domain_event`` position in ``create()``.
            ❌ Cannot roll back the create if the hook fails — treat hook
               logic as best-effort (idempotent, non-transactional).

        Args:
            entity:   The saved domain entity (with ``pk`` assigned).
            read_dto: The ``ReadDTO`` returned to the caller.
            ctx:      Caller's identity and grants.

        Edge cases:
            - If this hook raises, the exception propagates to the caller —
              the entity IS persisted.  Keep hook logic non-transactional.
            - Chain with ``super()`` even if the mixin is a leaf class —
              future mixins added higher in the MRO will chain correctly.
        """
        # Base: no-op.  Subclasses override and chain via super().
        return

    async def _after_update(
        self,
        before_dto: R,
        entity: D,
        read_dto: R,
        ctx: AuthContext,
    ) -> None:
        """
        Hook called AFTER the UoW commits on a successful ``update()``.

        Provides both the pre-update ``ReadDTO`` (``before_dto``) and the
        post-update ``ReadDTO`` (``read_dto``) so audit mixins can record
        exactly what changed.

        **Chaining contract**: always end with
        ``await super()._after_update(before_dto, entity, read_dto, ctx)``

        DESIGN: before_dto captured inside UoW before the update is applied
            ✅ before_dto reflects the entity state AT THE START of the update
               — valid because it is assembled from the entity fetched by
               find_by_id BEFORE apply_update is called.
            ✅ Passing read_dto avoids a second assembler call here.
            ❌ before_dto is a frozen ReadDTO — contains only the fields the
               assembler exposes.  Deep field diffs require domain-layer logic.

        Args:
            before_dto: ``ReadDTO`` of the entity before the update was applied.
            entity:     The saved domain entity after update (with pk).
            read_dto:   The ``ReadDTO`` returned to the caller (post-update).
            ctx:        Caller's identity and grants.

        Edge cases:
            - If this hook raises the exception propagates — the entity IS updated.
            - Chain via super() even in leaf mixin classes.
        """
        # Base: no-op.  Subclasses override and chain via super().
        return

    async def _after_delete(self, pk: Any, ctx: AuthContext) -> None:
        """
        Hook called AFTER the UoW commits on a successful ``delete()``.

        ``pk`` is the primary key of the deleted entity.  The entity itself is
        no longer retrievable from the repository at this point.

        **Chaining contract**: always end with
        ``await super()._after_delete(pk, ctx)``

        Args:
            pk:  Primary key of the deleted entity.
            ctx: Caller's identity and grants.

        Edge cases:
            - Entity is already deleted when this fires — do not attempt to
              fetch it again.
            - Chain via super() even in leaf mixin classes.
        """
        # Base: no-op.  Subclasses override and chain via super().
        return

    def _validate_entity(self, entity: D, ctx: AuthContext) -> None:
        """
        Validate a domain entity's business invariants before it is persisted.

        Called by:
        - ``create``: *after* ``_prepare_for_create`` — the entity is fully
          stamped with cross-cutting fields (``tenant_id``, ``owner_id``, etc.)
          at this point, so validators can check invariants that depend on them.
        - ``update``: *after* ``assembler.apply_update`` — the entity reflects
          the updated state that will be written to the backing store.

        **Why after _prepare_for_create?**
            Stamping (tenant_id, owner_id) happens in ``_prepare_for_create``.
            Running validation after stamping means validators see the complete
            entity — e.g. "owner must belong to this tenant" can be checked here
            without the validator needing direct access to ``ctx``.

        **Chaining contract**: always end with
        ``super()._validate_entity(entity, ctx)`` so multiple mixins in the MRO
        each run their validation step.

        Args:
            entity: The fully-assembled (and stamped) domain entity.
            ctx:    Caller's identity — available if a validator needs it,
                    but validators should prefer inspecting ``entity`` directly.

        Raises:
            ServiceValidationError: A business invariant was violated.

        Edge cases:
            - ``entity._raw_orm`` is ``None`` during ``create`` — the entity
              has not yet been persisted.  Validators must not call
              ``entity.raw()`` (raises ``RuntimeError``).
            - For ``update``, ``entity._raw_orm`` IS set — use
              ``entity.is_persisted()`` to distinguish if needed.
            - The base implementation is a no-op — all entities pass through
              unless a mixin (e.g. ``ValidatorServiceMixin``) overrides it.
        """
        # Base: no validation — all entities pass through.
        return

    async def _validate_entity_async(self, entity: D, ctx: AuthContext) -> None:
        """
        Async complement to ``_validate_entity`` for I/O-bound validation.

        Called by ``create`` and ``update`` immediately after
        ``_validate_entity`` while the unit of work is still open, so async
        validators can safely issue DB queries on the same session.

        The base implementation is a no-op.  Override via
        ``AsyncValidatorServiceMixin`` (or a custom mixin) to add DB-aware
        validation such as uniqueness checks.

        **Chaining contract**: always end with
        ``await super()._validate_entity_async(entity, ctx)`` so every mixin
        in the MRO runs its async validation step.

        Args:
            entity: The fully-assembled (and stamped) domain entity.
            ctx:    Caller's identity.

        Raises:
            ServiceValidationError: An async business invariant was violated.

        Edge cases:
            - ``entity._raw_orm`` is ``None`` during ``create`` — do not call
              ``entity.raw()`` here.
            - For ``update``, ``entity._raw_orm`` IS set.
            - Async validators share the open UoW session — use read-only
              queries here; writes belong in the service method itself.

        Async safety:   ✅ Awaited inside the open UoW.
        """
        return

    def _pre_check(self, ctx: AuthContext) -> None:
        """
        Fast stateless check executed *before* the unit of work is opened.

        Called at the very start of ``get``, ``create``, ``update``, and
        ``delete`` — before ``make_uow()`` is ever invoked.  Use this hook
        for invariants that should short-circuit without touching the DB (e.g.
        verifying a required claim is present in ``ctx``).

        **Why this hook exists** (vs. ``_check_entity``):
            ``_check_entity`` runs after ``find_by_id`` inside an open UoW.
            That means a UoW (and a DB connection) is always acquired first.
            ``_pre_check`` fires before the UoW is opened so it can deny
            the call with zero DB overhead and no open session.

        DESIGN: separate hook over adding the check to ``_scoped_params``
            ``_scoped_params`` is only called for ``list`` / ``count`` —
            it is a query-narrowing hook, not a gate hook.  ``_pre_check``
            is the correct place for "bail-out before any I/O" logic.

        **Chaining contract**: always end with ``super()._pre_check(ctx)``
        so multiple mixins in the MRO each run their pre-check.

        Args:
            ctx: Caller's identity.

        Raises:
            ServiceAuthorizationError: Required claim is absent or invalid.

        Edge cases:
            - ``_pre_check`` runs BEFORE ``_authorizer.authorize()`` for
              ``get``, ``update``, and ``delete`` (where auth happens after
              the entity is fetched).  For ``create`` it runs alongside the
              authorizer call (both are pre-UoW).
            - Raising here is correct — the error propagates before any
              DB session is acquired, so no rollback is needed.
        """
        # Base: no pre-check — all callers pass through.
        return

    # ── Public CRUD methods ───────────────────────────────────────────────────

    async def get(self, pk: PK, ctx: AuthContext = _ANON_CTX) -> R:
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
        # Fast stateless pre-flight check (e.g. tenant ID presence) — fires
        # before the UoW is opened so denied callers never acquire a DB session.
        self._pre_check(ctx)

        async with self._uow_provider.make_uow() as uow:
            entity = await self._get_repo(uow).find_by_id(pk)
            if entity is None:
                raise ServiceNotFoundError(pk, self._entity_type())

            # _check_entity before authorizer — both may raise ServiceNotFoundError
            # (not 403) to prevent existence oracles.  Checks before auth ensures
            # cross-concern blocking (tenant, soft-delete) runs first.
            self._check_entity(entity, ctx)

            # Authorization after fetching so the authorizer can perform
            # ownership checks (e.g. entity.owner_id == ctx.user_id).
            await self._authorizer.authorize(
                ctx,
                Action.READ,
                Resource(entity_type=self._entity_type(), entity=entity),
            )
            return self._assembler.to_read_dto(entity)

    async def list(self, params: QueryParams, ctx: AuthContext = _ANON_CTX) -> list[R]:
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
              override ``_scoped_params`` and narrow ``params`` there — do not
              override the full ``list()`` method.
        """
        # Authorize before opening the UoW — denied callers never touch the DB
        await self._authorizer.authorize(
            ctx,
            Action.LIST,
            Resource(entity_type=self._entity_type()),
        )

        # Apply cross-cutting query filters (tenant scope, soft-delete, etc.)
        # after auth so a denied caller never reaches the filtering code.
        scoped = self._scoped_params(params, ctx)

        async with self._uow_provider.make_uow() as uow:
            entities = await self._get_repo(uow).find_by_query(scoped)
            return [self._assembler.to_read_dto(e) for e in entities]

    async def count(self, params: QueryParams, ctx: AuthContext = _ANON_CTX) -> int:
        """
        Count entities matching ``params`` without fetching their data.

        Designed to be called alongside ``list()`` when the caller needs to
        build a paginated response.  Call both concurrently with
        ``asyncio.gather`` so they share the same authorization check cost
        while running their DB queries in parallel::

            results, total = await asyncio.gather(
                service.list(params, ctx),
                service.count(params, ctx),
            )
            return paged_response(results, total_count=total, params=params, raw_query=raw_query)

        Authorization uses the same ``Action.LIST`` grant as ``list()`` —
        a caller that cannot list entities also cannot count them.  Auth is
        checked before opening the UoW for the same reason as in ``list()``.

        Args:
            params: ``QueryParams`` with filter and pagination.  The ``limit``
                    and ``offset`` fields are ignored for counting — the count
                    reflects ALL matching rows, not just the current page.
            ctx:    Caller's identity and grants.

        Returns:
            Total number of entities matching ``params.node``.  Returns ``0``
            when nothing matches; never raises for empty result sets.

        Raises:
            ServiceAuthorizationError: Caller is not allowed to list / count.

        Edge cases:
            - ``QueryParams()`` (all defaults) counts every entity in the
              table — can be expensive on large tables without a filter.
            - ``params.limit`` and ``params.offset`` have no effect on the
              returned count — the count is always the full matching set.
            - Concurrent inserts between the ``count()`` and ``list()`` calls
              may cause the count to drift from the actual result size.  This
              is a known TOCTOU issue inherent to offset-based pagination.
              Use a single transaction (override this method) for strict
              consistency if required.

        Thread safety:  ⚠️ Service is a singleton; each call opens its own UoW.
        Async safety:   ✅ ``async def`` — safe to ``asyncio.gather`` with
                            ``list()``.
        """
        # Authorize before opening the UoW — same gate as list()
        await self._authorizer.authorize(
            ctx,
            Action.LIST,
            Resource(entity_type=self._entity_type()),
        )

        # Apply the same cross-cutting filters as list() so the count always
        # reflects what list() would return — never the raw unscoped total.
        scoped = self._scoped_params(params, ctx)

        async with self._uow_provider.make_uow() as uow:
            return await self._get_repo(uow).count(scoped)

    async def paged_list(
        self,
        params: QueryParams,
        ctx: AuthContext,
        *,
        raw_query: str | None = None,
    ) -> PagedReadDTO[R]:
        """
        Query entities and return a paginated response envelope.

        Delegates to ``list()`` and ``count()`` concurrently via
        ``asyncio.gather`` so both DB queries run in parallel.  Authorization
        is checked once per call inside each delegate — not duplicated here.

        The ``_scoped_params`` hook is applied inside ``list()`` and
        ``count()`` respectively, so tenant/soft-delete filters compose
        automatically without any extra wiring in this method.

        Args:
            params:    ``QueryParams`` with filter, sort, and pagination.
                       ``params.limit`` defines the page size.
            ctx:       Caller's identity and grants.
            raw_query: Optional raw query string to embed in the ``PageCursor``
                       so callers can copy the cursor for the next page.

        Returns:
            ``PagedReadDTO[R]`` with the current page's items, count,
            total count, and a ``next`` cursor (``None`` on the last page).

        Raises:
            ServiceAuthorizationError: Caller is not allowed to list.

        Edge cases:
            - ``total_count`` is computed by a separate ``count()`` query —
              concurrent inserts between the two may cause slight drift.
            - Pass ``params.limit=None`` for an unbounded response (no cursor).

        Thread safety:  ⚠️ Each gather() call opens two independent UoWs.
        Async safety:   ✅ Safe — both sub-calls are independent async tasks.

        Example::

            results = await service.paged_list(
                QueryParams(limit=20, offset=0),
                ctx,
                raw_query=request.query_params.get("q"),
            )
            return results.model_dump()
        """
        # Run list and count concurrently — each opens its own UoW internally.
        # Authorization is checked inside both calls — no double-auth here.
        results, total = await asyncio.gather(
            self.list(params, ctx),
            self.count(params, ctx),
        )
        return paged_response(
            results, params=params, total_count=total, raw_query=raw_query
        )

    async def exists(self, pk: PK, ctx: AuthContext) -> bool:
        """
        Return ``True`` if an entity with ``pk`` exists and is visible to ``ctx``.

        Authorization uses ``Action.READ`` at collection level — same grant as
        ``get()``.  A caller that cannot read any entities in this collection
        also cannot probe for existence.

        Note: ``_check_entity`` is intentionally NOT called here.  Checking
        entity-level constraints (soft-delete, tenant boundary) requires loading
        the entity, which defeats the purpose of a lightweight existence check.
        If the caller needs to know whether an entity is visible (passes all
        hooks) they should use ``get()`` and handle ``ServiceNotFoundError``.

        Args:
            pk:  Primary key to probe.
            ctx: Caller's identity and grants.

        Returns:
            ``True`` if the backing store contains a record with that PK and
            the caller is authorized to read this entity type.
            ``False`` if no such record exists.

        Raises:
            ServiceAuthorizationError: Caller lacks READ permission on this
                                       entity type.

        Thread safety:  ⚠️ Service is a singleton; each call opens its own UoW.
        Async safety:   ✅ ``async def`` — safe to ``await``.

        Edge cases:
            - Soft-deleted records ARE counted as existing — use ``get()`` if
              you need the service-layer visibility semantics.
            - Tenant-boundary enforcement is NOT applied — ``exists()`` reports
              raw backing-store presence.  Use ``get()`` for tenant-safe probing.
            - Authorization is checked at collection level before any DB access,
              so denied callers never open a DB connection.
        """
        # Fast stateless pre-check fires before the UoW — e.g. tenant presence
        self._pre_check(ctx)

        # Authorize at collection level — no entity to inspect yet
        await self._authorizer.authorize(
            ctx,
            Action.READ,
            Resource(entity_type=self._entity_type()),
        )

        async with self._uow_provider.make_uow() as uow:
            return await self._get_repo(uow).exists(pk)

    async def stream(
        self,
        params: QueryParams,
        ctx: AuthContext = _ANON_CTX,
    ) -> AsyncIterator[R]:
        """
        Yield ``ReadDTO``\\s one at a time without loading all results into memory.

        Designed for large result sets where loading everything into a list
        (as ``list()`` does) would exhaust available memory.  The UoW (and its
        underlying DB session / cursor) stays open for the entire iteration.

        Authorization follows the same rules as ``list()`` — ``Action.LIST``
        is checked before any DB access and ``_scoped_params`` is applied so
        tenant / soft-delete filters compose automatically.

        Args:
            params: ``QueryParams`` with filter, sort, and pagination.
                    ``params.limit`` caps the total number of yielded items.
            ctx:    Caller's identity and grants.

        Returns:
            An ``AsyncIterator[R]`` that yields ``ReadDTO``\\s one at a time.
            Iterate with ``async for dto in service.stream(params, ctx):``.

        Raises:
            ServiceAuthorizationError: Caller lacks LIST permission.

        Thread safety:  ⚠️ The UoW stays open for the duration of iteration.
                           Do not share the iterator across concurrent tasks.
        Async safety:   ✅ Async generator — safe to consume with ``async for``.

        Edge cases:
            - The caller **must** fully consume the iterator or break out of
              the ``async for`` loop cleanly.  Python will call ``aclose()``
              on the generator in both cases, which triggers the UoW's
              ``__aexit__`` and releases the DB connection.
            - For safety, wrap in ``async with contextlib.aclosing(...)`` when
              early exit is expected::

                  from contextlib import aclosing
                  async with aclosing(service.stream(params, ctx)) as it:
                      async for dto in it:
                          if done: break

            - Concurrent inserts during streaming may or may not appear in
              the stream depending on the DB's cursor isolation level.
        """
        # Authorize before opening the UoW — denied callers never touch the DB.
        await self._authorizer.authorize(
            ctx,
            Action.LIST,
            Resource(entity_type=self._entity_type()),
        )

        # Apply cross-cutting filters so the stream respects the same scoping
        # as list() — tenant isolation, soft-delete exclusion, etc.
        scoped = self._scoped_params(params, ctx)

        # DESIGN: async with UoW inside an async generator
        # The UoW stays open for the entire iteration.  When the consumer
        # finishes (or breaks / throws), Python calls aclose() on this
        # generator, which unwinds the async with block via __aexit__.
        # ✅ DB session / cursor stays alive for the whole stream.
        # ✅ Session is released cleanly on completion, break, or exception.
        # ❌ Abandoning the iterator without break (e.g. del it) relies on
        #    GC to trigger aclose() — prefer aclosing() for safety.
        async with self._uow_provider.make_uow() as uow:
            async for entity in self._get_repo(uow).stream_by_query(scoped):
                yield self._assembler.to_read_dto(entity)

    async def create(self, dto: C, ctx: AuthContext = _ANON_CTX) -> R:
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
        # Fast stateless pre-flight check — fires before any I/O.
        # Runs before the authorizer so invariants like "tenant ID present"
        # short-circuit with zero DB overhead.
        self._pre_check(ctx)

        # Authorize before opening the UoW — no entity exists yet, so only
        # type-level and wildcard grants apply (no ownership to check).
        await self._authorizer.authorize(
            ctx,
            Action.CREATE,
            Resource(entity_type=self._entity_type()),
        )

        async with self._uow_provider.make_uow() as uow:
            entity = self._assembler.to_domain(dto)
            # Stamp cross-cutting fields (tenant_id, owner_id, etc.) after auth
            # so the JWT identity is the authoritative source — not the DTO.
            entity = self._prepare_for_create(entity, ctx)
            # Validate business invariants after stamping so validators see the
            # fully-populated entity (including tenant_id, owner_id, etc.).
            # Raises ServiceValidationError if any invariant is violated.
            self._validate_entity(entity, ctx)
            await self._validate_entity_async(entity, ctx)
            saved = await self._get_repo(uow).save(entity)
            # Capture the ReadDTO inside the UoW — assembler.to_read_dto is a
            # pure transform but saved.pk is only valid while the session exists.
            read_dto = self._assembler.to_read_dto(saved)

        # ← UoW committed here.  Publish AFTER commit so consumers never
        # observe an event for a transaction that was rolled back.
        await self._publish_domain_event(
            EntityCreatedEvent(
                entity_type=self._entity_type().__name__,
                pk=saved.pk,
                # Propagate tenant identity so consumers can select the correct
                # per-tenant decryption key when reading the encrypted payload.
                tenant_id=ctx.metadata.get("tenant_id") if ctx else None,
                correlation_id=current_correlation_id(),
                payload=read_dto.model_dump(),
            )
        )
        # Post-create hook — called after commit and domain event publish.
        # Mixins override this to emit audit events, notifications, etc.
        await self._after_create(saved, read_dto, ctx)
        return read_dto

    async def update(self, pk: PK, dto: U, ctx: AuthContext = _ANON_CTX) -> R:
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
        # Fast stateless pre-flight check before the UoW is opened.
        self._pre_check(ctx)

        async with self._uow_provider.make_uow() as uow:
            entity = await self._get_repo(uow).find_by_id(pk)
            if entity is None:
                raise ServiceNotFoundError(pk, self._entity_type())

            # Cross-concern check before authorizer (see get() for rationale).
            self._check_entity(entity, ctx)

            # Authorize with the current entity state so ownership checks work
            await self._authorizer.authorize(
                ctx,
                Action.UPDATE,
                Resource(entity_type=self._entity_type(), entity=entity),
            )

            # Capture the pre-update ReadDTO BEFORE apply_update so _after_update
            # can diff before/after states.  Assembled inside the UoW so the
            # entity's lazy-loaded fields are available.
            before_dto = self._assembler.to_read_dto(entity)

            updated = self._assembler.apply_update(entity, dto)
            # Validate the updated entity's business invariants before saving.
            # Runs after apply_update so validators see the new field values,
            # not the pre-update state.
            self._validate_entity(updated, ctx)
            await self._validate_entity_async(updated, ctx)
            saved = await self._get_repo(uow).save(updated)
            # Capture ReadDTO inside the UoW — same rationale as create().
            read_dto = self._assembler.to_read_dto(saved)

        # ← UoW committed here.  Publish after commit — see create() rationale.
        await self._publish_domain_event(
            EntityUpdatedEvent(
                entity_type=self._entity_type().__name__,
                pk=saved.pk,
                # Same tenant propagation rationale as EntityCreatedEvent above.
                tenant_id=ctx.metadata.get("tenant_id") if ctx else None,
                correlation_id=current_correlation_id(),
                payload=read_dto.model_dump(),
            )
        )
        # Post-update hook — called after commit and domain event publish.
        # before_dto carries the pre-update ReadDTO for diffing in audit mixins.
        await self._after_update(before_dto, saved, read_dto, ctx)
        return read_dto

    async def patch(self, pk: PK, dto: U, ctx: AuthContext = _ANON_CTX) -> R:
        """
        Partial update (JSON Merge Patch) — delegates to ``update()``.

        ``patch`` and ``update`` use the same ``apply_update`` logic; the
        distinction is semantic (HTTP PATCH vs PUT).  ``UpdateDTO`` fields are
        already optional so the assembler's ``apply_update`` naturally handles
        partial payloads by leaving ``None`` fields unchanged.

        Args:
            pk:  Primary key of the entity to update.
            dto: ``UpdateDTO`` with only the fields to change set.
            ctx: Caller's identity and grants.

        Returns:
            The ``ReadDTO`` reflecting the entity's new state.

        Thread safety:  ✅ Delegates entirely to update().
        Async safety:   ✅ Awaited through.
        """
        return await self.update(pk, dto, ctx)

    async def delete(self, pk: PK, ctx: AuthContext = _ANON_CTX) -> None:
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
        # Fast stateless pre-flight check before the UoW is opened.
        self._pre_check(ctx)

        async with self._uow_provider.make_uow() as uow:
            entity = await self._get_repo(uow).find_by_id(pk)
            if entity is None:
                raise ServiceNotFoundError(pk, self._entity_type())

            # Cross-concern check before authorizer (see get() for rationale).
            self._check_entity(entity, ctx)

            # Authorize with the current entity so ownership checks work
            await self._authorizer.authorize(
                ctx,
                Action.DELETE,
                Resource(entity_type=self._entity_type(), entity=entity),
            )

            # Capture pk before the entity is removed — it may be invalidated
            # by the UoW teardown depending on the ORM backend.
            deleted_pk = entity.pk
            await self._get_repo(uow).delete(entity)

        # ← UoW committed here.  Publish after commit — see create() rationale.
        await self._publish_domain_event(
            EntityDeletedEvent(
                entity_type=self._entity_type().__name__,
                pk=deleted_pk,
                # Same tenant propagation rationale as EntityCreatedEvent above.
                tenant_id=ctx.metadata.get("tenant_id") if ctx else None,
                correlation_id=current_correlation_id(),
            )
        )
        # Post-delete hook — called after commit and domain event publish.
        await self._after_delete(deleted_pk, ctx)

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

    async def _publish_domain_event(
        self,
        event: EntityCreatedEvent | EntityUpdatedEvent | EntityDeletedEvent,
    ) -> None:
        """
        Publish a domain lifecycle event via the injected producer.

        The channel is derived from the entity class name (lowercase) so
        consumers can subscribe by entity type independently of operation::

            @listen(EntityCreatedEvent, channel="post")  # only post creates
            @listen(EntityEvent, channel="post")          # all post events
            @listen(EntityCreatedEvent, channel="*")      # all creates

        Args:
            event: The domain event to publish.

        Edge cases:
            - When ``NoopEventProducer`` is injected (default), this is a
              cheap no-op — no overhead on services without a bus configured.
            - Any exception from the producer propagates to the caller.  If
              event delivery must not affect the HTTP response, wrap the
              call site or configure ``ErrorPolicy.FIRE_FORGET``.
        """
        # Channel = entity class name lowercased — e.g. "post", "user", "order".
        # Gives consumers a stable routing key independent of the operation type.
        await self._producer._produce(event, channel=self._entity_channel())

    def _entity_channel(self) -> str:
        """
        Return the default event channel for this service's entity type.

        Derived from the entity class name, lowercased.  For example, a
        service managing ``Post`` entities publishes to channel ``"post"``.

        Returns:
            Lowercased entity class name, e.g. ``"post"``, ``"user"``.
        """
        return self._entity_type().__name__.lower()

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
