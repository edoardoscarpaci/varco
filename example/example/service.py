"""
example.service
===============
Business logic for the ``Post`` entity.

``PostService`` composes four layers via Python MRO:

1. ``CacheServiceMixin``  — transparent look-aside caching (Redis-backed)
2. ``AsyncService``       — CRUD orchestration, auth, UoW management

The mixin must appear FIRST in the MRO so its overrides of ``get``,
``list``, ``create``, ``update``, and ``delete`` wrap the base-class methods
and the chain completes via ``super()``.

Event publishing
----------------
``PostService`` overrides ``_after_create`` and ``_after_delete`` to emit
domain-specific events (``PostCreatedEvent``, ``PostDeletedEvent``) AFTER the
unit-of-work commits.  These are ADDITIONAL to the generic ``EntityCreatedEvent``
that ``AsyncService.create()`` always emits.

Domain events are published via ``self._producer`` (``AbstractEventProducer``),
injected optionally.  When no bus is wired (e.g. in unit tests) the injected
producer is a ``NoopEventProducer`` — no guards needed.

Author identity
---------------
``PostCreate`` omits ``author_id`` — the service stamps it from
``AuthContext.subject`` in ``_prepare_for_create()``.  This enforces that
the author is always the authenticated user, never a client-supplied value.

Cache configuration
-------------------
``_cache_namespace = "post"`` means all cache keys are prefixed with ``"post:"``
(e.g. ``"post:get:<uuid>"``, ``"post:<tenant>:list:<hash>"``).  Keys are
tenant-scoped when ``ctx.metadata["tenant_id"]`` is present.

DESIGN: override ``_after_create`` and ``_after_delete`` hooks over full override
    The service calls ``super().create()`` (CacheServiceMixin → AsyncService)
    which handles UoW, auth, assembly, and list-cache invalidation correctly.
    Only the post-commit side-effect (event publishing) is added in the hook —
    no risk of accidentally skipping auth or cache invalidation.

    ✅ All existing hook chains (auth, cache, validation) run unchanged.
    ✅ Event publishing happens AFTER commit — no event for rolled-back writes.
    ❌ Two event emits per create (generic EntityCreatedEvent + PostCreatedEvent).
       Consumers must subscribe to only the event type they care about.

Thread safety:  ⚠️ Singleton — all methods must be stateless (each creates its
                   own UoW via ``self._uow_provider.make_uow()``).
Async safety:   ✅ All public methods are ``async def``.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from providify import Inject, Singleton

from typing import Annotated

from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth.base import AbstractAuthorizer
from varco_core.cache.mixin import CacheServiceMixin
from varco_core.event.producer import AbstractEventProducer
from varco_core.service.base import AsyncService, IUoWProvider
from providify import InjectMeta

from example.dtos import PostCreate, PostRead, PostUpdate
from example.events import PostCreatedEvent, PostDeletedEvent
from example.models import Post

if TYPE_CHECKING:
    from varco_core.auth import AuthContext


@Singleton
class PostService(
    # CacheServiceMixin MUST be leftmost — its overrides of get/list/create/
    # update/delete must wrap AsyncService's implementations, not the other
    # way around.  Python MRO resolves left-to-right, so leftmost wins.
    CacheServiceMixin[Post, UUID, PostCreate, PostRead, PostUpdate],
    AsyncService[Post, UUID, PostCreate, PostRead, PostUpdate],
):
    """
    CRUD service for ``Post`` entities with caching and event publishing.

    Implements the full varco service contract:
    - Authorization via injected ``AbstractAuthorizer`` (defaults to permissive).
    - DTO ↔ domain translation via injected ``PostAssembler``.
    - Look-aside caching via ``CacheServiceMixin`` (DI-injected ``CacheBackend``).
    - Event publishing via ``AbstractEventProducer`` (optional — no-op if absent).
    - Unit-of-work management via ``IUoWProvider`` (SAModule provides this).

    Class attributes:
        _cache_namespace: Prefix for all Redis cache keys (``"post"``).
        _cache_ttl:       Default cache TTL in seconds (300 = 5 min).

    Thread safety:  ⚠️ Singleton — must be stateless; each call opens its own UoW.
    Async safety:   ✅ All public methods are ``async def``.
    """

    # ── Cache configuration ───────────────────────────────────────────────────
    # Resolved by providify at class level — no __init__ parameter needed.
    # _cache: ClassVar[Inject[CacheBackend]]  ← inherited from CacheServiceMixin
    _cache_namespace = "post"
    _cache_ttl = 300  # 5 minutes

    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer: Inject[AbstractAuthorizer],
        # Concrete generic alias is required here so providify's runtime
        # annotation introspection resolves the correct PostAssembler binding
        # (registered under AbstractDTOAssembler[Post, PostCreate, PostRead, PostUpdate]).
        assembler: Inject[AbstractDTOAssembler[Post, PostCreate, PostRead, PostUpdate]],
        # Optional — defaults to NoopEventProducer when no bus is wired.
        # Must be declared here (not only on AsyncService.__init__) so that
        # providify sees the parameter and injects AbstractEventProducer when
        # setup_producer=True wires BusEventProducer into the container.
        # Without this declaration, providify resolves PostService.__init__
        # only and passes producer=None → AsyncService uses NoopEventProducer.
        producer: Annotated[AbstractEventProducer, InjectMeta(optional=True)] = None,
    ) -> None:
        """
        Args:
            uow_provider: Injected ``IUoWProvider`` — provided by SAModule.
            authorizer:   Injected ``AbstractAuthorizer`` — defaults to
                          ``BaseAuthorizer`` (permissive) if no custom
                          authorizer is registered.
            assembler:    Injected ``PostAssembler`` — handles DTO ↔ Post mapping.
            producer:     Optional ``AbstractEventProducer``.  Injected by the DI
                          container when ``setup_producer=True`` (wires
                          ``BusEventProducer`` → ``AbstractEventBus``).  When
                          absent (unit tests, no bus wired), ``AsyncService``
                          falls back to ``NoopEventProducer``.

        Edge cases:
            - ``_cache`` is injected via ClassVar from ``CacheServiceMixin`` —
              no parameter needed here; providify sets it at the class level.
            - If ``setup_producer=False`` (default), the container has no binding
              for ``AbstractEventProducer``; ``InjectMeta(optional=True)`` makes
              the parameter resolve to ``None`` safely.
        """
        # Forward producer so AsyncService.__init__ uses the injected instance
        # instead of defaulting to NoopEventProducer.
        # CacheServiceMixin contributes no __init__ parameters — its dependencies
        # (_cache, _cache_producer) are ClassVar-injected at the class level.
        super().__init__(
            uow_provider=uow_provider,
            authorizer=authorizer,
            assembler=assembler,
            producer=producer,
        )

    def _get_repo(self, uow):
        """
        Return the ``AsyncRepository[Post]`` from the open unit-of-work.

        ``uow.get_repository(Post)`` is the canonical way to obtain a
        repository — it delegates to the ``RepositoryProvider`` singleton
        which constructs a fresh ``AsyncSession``-backed repository.

        Args:
            uow: The open ``AsyncUnitOfWork`` for this request.

        Returns:
            ``AsyncRepository[Post]`` backed by the current session.
        """
        return uow.get_repository(Post)

    def _prepare_for_create(self, entity: Post, ctx: AuthContext) -> Post:
        """
        Stamp ``author_id`` from ``AuthContext.subject`` onto a new post.

        Called by ``AsyncService.create()`` after ``to_domain()`` and BEFORE
        ``save()`` — the entity is fully assembled but not yet persisted.

        This is the correct place for cross-cutting field injection (not the
        assembler, which has no auth context access).

        Args:
            entity: Freshly assembled ``Post`` (``author_id`` is sentinel value).
            ctx:    Caller's auth context — ``ctx.subject`` is the user's UUID string.

        Returns:
            A new ``Post`` with ``author_id`` set from the JWT subject.
            ``dataclasses.replace()`` preserves ``_raw_orm`` (``None`` at this
            point) so the repository still performs an INSERT.

        Edge cases:
            - If ``ctx.subject`` is not a valid UUID string, ``UUID(ctx.subject)``
              raises ``ValueError`` — this is intentional: a non-UUID subject
              should fail loudly rather than silently store a zero UUID.
            - Always calls ``super()._prepare_for_create(entity, ctx)`` so other
              mixins in the MRO (e.g. ``TenantAwareService``) can stamp their
              own fields after this one.
        """
        entity = super()._prepare_for_create(entity, ctx)

        # Stamp author_id from the authenticated user_id — never from the request body.
        # AuthContext.user_id is the unique identifier from the decoded JWT (sub claim).
        if ctx and ctx.user_id:
            entity = replace(entity, author_id=UUID(ctx.user_id))

        # Stamp timestamps — the SA ORM columns have no server_default so we set
        # them here in Python.  created_at and updated_at are both set on INSERT;
        # update paths should overwrite updated_at only.
        now = datetime.now(UTC)
        entity = replace(entity, created_at=now, updated_at=now)

        return entity

    async def _after_create(
        self, entity: Post, read_dto: PostRead, ctx: AuthContext
    ) -> None:
        """
        Publish ``PostCreatedEvent`` after a successful commit.

        Called by ``AsyncService.create()`` AFTER the UoW commits, so the
        post IS durable before any consumer receives the event.

        ``CacheServiceMixin.create()`` invalidates the list cache before this
        hook fires — the order is: commit → list invalidation → _after_create.

        Args:
            entity:   The persisted ``Post`` (``pk`` is set).
            read_dto: The assembled ``PostRead`` returned to the HTTP caller.
            ctx:      Caller's auth context.

        Edge cases:
            - ``_producer`` is always set (``NoopEventProducer`` if no bus).
              No ``if self._producer`` guard needed — the Null Object handles it.
            - If ``_emit`` raises, the exception propagates to the caller.
              The post IS saved — do not retry the create.
              For guaranteed delivery, use the outbox pattern.
        """
        await super()._after_create(entity, read_dto, ctx)

        # Publish the domain-specific event — carries typed fields rather than
        # the opaque payload dict used by the generic EntityCreatedEvent.
        await self._emit(
            PostCreatedEvent(post_id=entity.pk, author_id=entity.author_id),
            channel="posts",
        )

    async def _after_delete(self, pk: UUID, ctx: AuthContext) -> None:
        """
        Publish ``PostDeletedEvent`` after a successful delete and commit.

        The post is no longer retrievable when this fires — consumers must not
        attempt to fetch it by ``pk``.

        Args:
            pk:  UUID of the deleted post.
            ctx: Caller's auth context.

        Edge cases:
            - Same delivery guarantees as ``_after_create`` — best-effort,
              no retries.  Use the outbox pattern for at-least-once delivery.
        """
        await super()._after_delete(pk, ctx)

        await self._emit(
            PostDeletedEvent(post_id=pk),
            channel="posts",
        )


__all__ = ["PostService"]
