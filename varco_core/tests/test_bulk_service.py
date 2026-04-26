"""
tests.test_bulk_service
=======================
Unit tests for ``BulkServiceMixin`` — atomic multi-entity create and delete.

Test doubles are imported from ``test_service`` to reuse the same domain
fixtures (Post, PostAssembler, InMemoryPostRepository, InMemoryUoW,
FakeUoWProvider, DenyAllAuthorizer, ConcretePostService) rather than
duplicating the entire infrastructure.

Extra fixture: BulkPostService
    Concrete ``BulkServiceMixin`` + ``AsyncService`` for Post that exposes
    ``create_many`` and ``delete_many``.  Wired the same way as
    ``ConcretePostService`` — same repo/UoW/assembler.

DESIGN: InMemoryEventBus for event assertions
    ``create_many`` and ``delete_many`` publish domain events.  Instead of
    monkeypatching ``_publish_domain_event``, we wire a real
    ``InMemoryEventBus`` and ``BusEventProducer`` and inspect the bus's
    in-memory received-events list.  This also exercises the production code
    path that routes events through the producer, not just a stub.

    Tradeoffs:
      ✅ No mock assertions — tests are clear about what events are published.
      ✅ Tests the event-routing code that singles out the entity channel.
      ❌ ``InMemoryEventBus`` must be drained manually in BACKGROUND mode;
         we use SYNC mode here (default) so events are dispatched eagerly.

DESIGN: ``_after_create`` / ``_after_delete`` hook verification via subclass
    We assert that these hooks are called by overriding them in a
    ``HookedBulkService`` subclass that records calls in lists.  No mock
    patching needed — the override proves the hook was reached.

    Tradeoffs:
      ✅ Mirrors real usage — mixins override these hooks with super().
      ✅ No mock assertions — clearer intent.
      ❌ Slightly more setup code vs. ``MagicMock``.
"""

from __future__ import annotations

from typing import Any

import pytest

from varco_core.auth import AuthContext
from varco_core.auth.authorizer import BaseAuthorizer
from varco_core.event import BusEventProducer, InMemoryEventBus
from varco_core.event.domain import EntityCreatedEvent, EntityDeletedEvent
from varco_core.exception.service import (
    ServiceAuthorizationError,
    ServiceNotFoundError,
    ServiceValidationError,
)
from varco_core.service import AsyncService, BulkServiceMixin
from varco_core.uow import AsyncUnitOfWork

# Re-use all test doubles from test_service rather than duplicating them.
from tests.test_service import (
    CreatePostDTO,
    DenyAllAuthorizer,
    FakeUoWProvider,
    InMemoryPostRepository,
    Post,
    PostAssembler,
    PostReadDTO,
    UpdatePostDTO,
)


# ── Concrete BulkServiceMixin subclass ─────────────────────────────────────────


class BulkPostService(
    BulkServiceMixin[Post, str, CreatePostDTO, PostReadDTO, UpdatePostDTO],
    AsyncService[Post, str, CreatePostDTO, PostReadDTO, UpdatePostDTO],
):
    """
    Minimal concrete ``BulkServiceMixin`` for Post used in bulk tests.

    Inherits ``create_many`` / ``delete_many`` from ``BulkServiceMixin`` and
    the full CRUD from ``AsyncService``.  ``_get_repo`` is the only required
    override.

    Thread safety:  ❌ Test use only.
    Async safety:   ✅ All public methods are ``async def``.
    """

    def __init__(
        self,
        uow_provider: FakeUoWProvider,
        assembler: PostAssembler,
        authorizer: Any = None,
        producer: Any = None,
    ) -> None:
        super().__init__(
            uow_provider=uow_provider,
            authorizer=authorizer or BaseAuthorizer(),
            assembler=assembler,
            producer=producer,
        )

    def _get_repo(self, uow: AsyncUnitOfWork) -> InMemoryPostRepository:
        return uow.posts  # type: ignore[attr-defined]


# ── pytest fixtures ────────────────────────────────────────────────────────────


@pytest.fixture()
def uow_provider() -> FakeUoWProvider:
    return FakeUoWProvider()


@pytest.fixture()
def assembler() -> PostAssembler:
    return PostAssembler()


@pytest.fixture()
def bus() -> InMemoryEventBus:
    return InMemoryEventBus()


@pytest.fixture()
def svc(
    uow_provider: FakeUoWProvider,
    assembler: PostAssembler,
    bus: InMemoryEventBus,
) -> BulkPostService:
    return BulkPostService(
        uow_provider=uow_provider,
        assembler=assembler,
        producer=BusEventProducer(bus),
    )


@pytest.fixture()
def deny_svc(
    uow_provider: FakeUoWProvider,
    assembler: PostAssembler,
    bus: InMemoryEventBus,
) -> BulkPostService:
    return BulkPostService(
        uow_provider=uow_provider,
        assembler=assembler,
        authorizer=DenyAllAuthorizer(),
        producer=BusEventProducer(bus),
    )


@pytest.fixture()
def ctx() -> AuthContext:
    return AuthContext()


# ── Helper ─────────────────────────────────────────────────────────────────────


def _seed(uow_provider: FakeUoWProvider, *titles: str) -> list[Post]:
    """Directly write Posts into the shared repo, bypassing the service."""
    posts: list[Post] = []
    for title in titles:
        p = Post(title=title)
        uow_provider.repo._pk_seq += 1
        p.pk = f"gen-{uow_provider.repo._pk_seq}"
        uow_provider.repo._store[p.pk] = p
        posts.append(p)
    return posts


# ── create_many tests ──────────────────────────────────────────────────────────


class TestCreateMany:
    """Tests for ``BulkServiceMixin.create_many``."""

    async def test_empty_returns_empty_list(
        self, svc: BulkPostService, ctx: AuthContext
    ) -> None:
        result = await svc.create_many([], ctx)
        assert result == []

    async def test_empty_does_not_open_uow(
        self, svc: BulkPostService, uow_provider: FakeUoWProvider, ctx: AuthContext
    ) -> None:
        await svc.create_many([], ctx)
        assert uow_provider.uow_created_count == 0

    async def test_creates_all_entities_in_one_uow(
        self,
        svc: BulkPostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        dtos = [
            CreatePostDTO(title="A"),
            CreatePostDTO(title="B"),
            CreatePostDTO(title="C"),
        ]
        await svc.create_many(dtos, ctx)

        # All three persisted in ONE UoW (one UoW open + one bulk save)
        assert uow_provider.uow_created_count == 1
        assert len(uow_provider.repo._store) == 3

    async def test_returns_read_dtos_in_input_order(
        self,
        svc: BulkPostService,
        ctx: AuthContext,
    ) -> None:
        dtos = [CreatePostDTO(title="first"), CreatePostDTO(title="second")]
        result = await svc.create_many(dtos, ctx)

        assert len(result) == 2
        assert result[0].title == "first"
        assert result[1].title == "second"

    async def test_returns_list_of_read_dtos(
        self,
        svc: BulkPostService,
        ctx: AuthContext,
    ) -> None:
        dtos = [CreatePostDTO(title="x")]
        result = await svc.create_many(dtos, ctx)

        assert isinstance(result, list)
        assert isinstance(result[0], PostReadDTO)

    async def test_single_dto_works(
        self,
        svc: BulkPostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        result = await svc.create_many([CreatePostDTO(title="solo")], ctx)

        assert len(result) == 1
        assert result[0].title == "solo"
        assert len(uow_provider.repo._store) == 1

    async def test_authorization_denied_raises_error(
        self,
        deny_svc: BulkPostService,
        ctx: AuthContext,
    ) -> None:
        with pytest.raises(ServiceAuthorizationError):
            await deny_svc.create_many([CreatePostDTO(title="x")], ctx)

    async def test_authorization_fires_before_uow(
        self,
        deny_svc: BulkPostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        with pytest.raises(ServiceAuthorizationError):
            await deny_svc.create_many([CreatePostDTO(title="x")], ctx)

        assert uow_provider.uow_created_count == 0

    async def test_validation_error_rolls_back_all(
        self,
        uow_provider: FakeUoWProvider,
        assembler: PostAssembler,
        bus: InMemoryEventBus,
        ctx: AuthContext,
    ) -> None:
        """A validation failure on any entity rolls back the entire batch."""

        class ValidatingBulkService(BulkPostService):
            def _validate_entity(self, entity: Post, ctx: AuthContext) -> None:
                if entity.title == "bad":
                    raise ServiceValidationError("title", "bad title", Post)

        svc = ValidatingBulkService(
            uow_provider=uow_provider,
            assembler=assembler,
            producer=BusEventProducer(bus),
        )
        dtos = [CreatePostDTO(title="good"), CreatePostDTO(title="bad")]

        with pytest.raises(ServiceValidationError):
            await svc.create_many(dtos, ctx)

        # Entire batch rolled back — nothing persisted
        assert len(uow_provider.repo._store) == 0

    async def test_publishes_entity_created_event_per_entity(
        self,
        svc: BulkPostService,
        bus: InMemoryEventBus,
        ctx: AuthContext,
    ) -> None:
        received: list[EntityCreatedEvent] = []

        from varco_core.event import EventConsumer
        from varco_core.event.consumer import listen

        class _Listener(EventConsumer):
            def __init__(self, b: InMemoryEventBus) -> None:
                self._bus = b
                self.register_to(b)

            @listen(EntityCreatedEvent, channel="post")
            async def on_create(self, event: EntityCreatedEvent) -> None:
                received.append(event)

        _Listener(bus)
        dtos = [CreatePostDTO(title="A"), CreatePostDTO(title="B")]
        await svc.create_many(dtos, ctx)

        assert len(received) == 2
        assert all(isinstance(e, EntityCreatedEvent) for e in received)
        assert all(e.entity_type == "Post" for e in received)

    async def test_after_create_hook_called_per_entity(
        self,
        uow_provider: FakeUoWProvider,
        assembler: PostAssembler,
        bus: InMemoryEventBus,
        ctx: AuthContext,
    ) -> None:
        called: list[tuple[Post, PostReadDTO]] = []

        class HookedSvc(BulkPostService):
            async def _after_create(
                self, entity: Post, read_dto: PostReadDTO, ctx: AuthContext
            ) -> None:
                called.append((entity, read_dto))

        svc = HookedSvc(
            uow_provider=uow_provider,
            assembler=assembler,
            producer=BusEventProducer(bus),
        )
        dtos = [CreatePostDTO(title="X"), CreatePostDTO(title="Y")]
        await svc.create_many(dtos, ctx)

        assert len(called) == 2
        assert called[0][0].title == "X"
        assert called[1][0].title == "Y"

    async def test_prepare_for_create_called_per_entity(
        self,
        uow_provider: FakeUoWProvider,
        assembler: PostAssembler,
        bus: InMemoryEventBus,
        ctx: AuthContext,
    ) -> None:
        stamped: list[str] = []

        class StampSvc(BulkPostService):
            def _prepare_for_create(self, entity: Post, ctx: AuthContext) -> Post:
                entity.title = entity.title + "-stamped"
                stamped.append(entity.title)
                return entity

        svc = StampSvc(
            uow_provider=uow_provider,
            assembler=assembler,
            producer=BusEventProducer(bus),
        )
        dtos = [CreatePostDTO(title="p1"), CreatePostDTO(title="p2")]
        result = await svc.create_many(dtos, ctx)

        assert len(stamped) == 2
        assert result[0].title == "p1-stamped"
        assert result[1].title == "p2-stamped"


# ── delete_many tests ──────────────────────────────────────────────────────────


class TestDeleteMany:
    """Tests for ``BulkServiceMixin.delete_many``."""

    async def test_empty_is_noop(
        self, svc: BulkPostService, uow_provider: FakeUoWProvider, ctx: AuthContext
    ) -> None:
        await svc.delete_many([], ctx)
        assert uow_provider.uow_created_count == 0

    async def test_deletes_all_entities(
        self,
        svc: BulkPostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        posts = _seed(uow_provider, "A", "B", "C")
        pks = [p.pk for p in posts]

        await svc.delete_many(pks, ctx)

        assert len(uow_provider.repo._store) == 0

    async def test_deletes_in_single_uow(
        self,
        svc: BulkPostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        posts = _seed(uow_provider, "A", "B")
        await svc.delete_many([p.pk for p in posts], ctx)

        assert uow_provider.uow_created_count == 1

    async def test_missing_pk_raises_not_found(
        self,
        svc: BulkPostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        [existing] = _seed(uow_provider, "real")

        with pytest.raises(ServiceNotFoundError):
            await svc.delete_many([existing.pk, "nonexistent-pk"], ctx)

    async def test_missing_pk_rolls_back_entire_batch(
        self,
        svc: BulkPostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        posts = _seed(uow_provider, "A", "B")

        with pytest.raises(ServiceNotFoundError):
            await svc.delete_many([posts[0].pk, "no-such-pk"], ctx)

        # "A" must still be in the store — whole batch rolled back
        assert posts[0].pk in uow_provider.repo._store

    async def test_authorization_denied_raises_error(
        self,
        deny_svc: BulkPostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        [post] = _seed(uow_provider, "existing")

        with pytest.raises(ServiceAuthorizationError):
            await deny_svc.delete_many([post.pk], ctx)

    async def test_authorization_checked_per_entity(
        self,
        uow_provider: FakeUoWProvider,
        assembler: PostAssembler,
        bus: InMemoryEventBus,
        ctx: AuthContext,
    ) -> None:
        """Authorization is called once per entity, not once for the whole batch."""
        authorize_calls: list[str] = []

        from varco_core.auth import AbstractAuthorizer, Action, Resource

        class CountingAuthorizer(AbstractAuthorizer):
            async def authorize(
                self, ctx: AuthContext, action: Action, resource: Resource
            ) -> None:
                if resource.entity is not None:
                    authorize_calls.append(str(resource.entity.pk))

        posts = _seed(uow_provider, "X", "Y", "Z")
        svc = BulkPostService(
            uow_provider=uow_provider,
            assembler=assembler,
            authorizer=CountingAuthorizer(),
            producer=BusEventProducer(bus),
        )
        await svc.delete_many([p.pk for p in posts], ctx)

        assert len(authorize_calls) == 3

    async def test_publishes_entity_deleted_event_per_entity(
        self,
        svc: BulkPostService,
        uow_provider: FakeUoWProvider,
        bus: InMemoryEventBus,
        ctx: AuthContext,
    ) -> None:
        received: list[EntityDeletedEvent] = []

        from varco_core.event import EventConsumer
        from varco_core.event.consumer import listen

        class _Listener(EventConsumer):
            def __init__(self, b: InMemoryEventBus) -> None:
                self._bus = b
                self.register_to(b)

            @listen(EntityDeletedEvent, channel="post")
            async def on_delete(self, event: EntityDeletedEvent) -> None:
                received.append(event)

        _Listener(bus)
        posts = _seed(uow_provider, "A", "B")
        await svc.delete_many([p.pk for p in posts], ctx)

        assert len(received) == 2
        assert all(isinstance(e, EntityDeletedEvent) for e in received)
        assert all(e.entity_type == "Post" for e in received)

    async def test_after_delete_hook_called_per_entity(
        self,
        uow_provider: FakeUoWProvider,
        assembler: PostAssembler,
        bus: InMemoryEventBus,
        ctx: AuthContext,
    ) -> None:
        deleted_pks: list[str] = []

        class HookedSvc(BulkPostService):
            async def _after_delete(self, pk: str, ctx: AuthContext) -> None:
                deleted_pks.append(pk)

        svc = HookedSvc(
            uow_provider=uow_provider,
            assembler=assembler,
            producer=BusEventProducer(bus),
        )
        posts = _seed(uow_provider, "A", "B")
        await svc.delete_many([p.pk for p in posts], ctx)

        assert len(deleted_pks) == 2
        assert set(deleted_pks) == {posts[0].pk, posts[1].pk}

    async def test_check_entity_hook_called_per_entity(
        self,
        uow_provider: FakeUoWProvider,
        assembler: PostAssembler,
        bus: InMemoryEventBus,
        ctx: AuthContext,
    ) -> None:
        checked_pks: list[str] = []

        class CheckingSvc(BulkPostService):
            def _check_entity(self, entity: Post, ctx: AuthContext) -> None:
                checked_pks.append(entity.pk)

        svc = CheckingSvc(
            uow_provider=uow_provider,
            assembler=assembler,
            producer=BusEventProducer(bus),
        )
        posts = _seed(uow_provider, "X", "Y")
        await svc.delete_many([p.pk for p in posts], ctx)

        assert len(checked_pks) == 2

    async def test_delete_many_does_not_affect_other_entities(
        self,
        svc: BulkPostService,
        uow_provider: FakeUoWProvider,
        ctx: AuthContext,
    ) -> None:
        posts = _seed(uow_provider, "keep", "del1", "del2")

        await svc.delete_many([posts[1].pk, posts[2].pk], ctx)

        assert posts[0].pk in uow_provider.repo._store
        assert posts[1].pk not in uow_provider.repo._store
        assert posts[2].pk not in uow_provider.repo._store
