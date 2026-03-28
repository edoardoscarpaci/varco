"""
Unit tests for tenant-aware domain events
==========================================
Covers:

- ``EntityEvent.tenant_id`` field — presence, default, and round-trip.
- ``AsyncService.create()`` / ``update()`` / ``delete()`` — verify that
  ``tenant_id`` is populated from ``ctx.metadata["tenant_id"]`` and forwarded
  to the emitted domain event.
- Backward-compatibility — events produced without a tenant context carry
  ``tenant_id=None``.

Test doubles
------------
All doubles are plain Python — no mocking library.

    ``InMemoryPostRepository`` — shares state across UoW instances so pre-seeded
    entities survive across the service call.
    ``PostAssembler``          — maps Post ↔ CreatePostDTO / PostReadDTO.
    ``ConcretePostService``    — minimal AsyncService subclass.
    ``CapturingProducer``      — records every event passed to ``_produce()``.
    ``AllowAllAuthorizer``     — allows every authorization check.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Annotated, Any

import pytest

from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth import AbstractAuthorizer, Action, AuthContext, Resource
from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO
from varco_core.event.domain import (
    EntityCreatedEvent,
    EntityDeletedEvent,
    EntityEvent,
    EntityUpdatedEvent,
)
from varco_core.event.producer import AbstractEventProducer
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
    Minimal domain entity for tenant event tests.

    ``STR_ASSIGNED`` pk so we can control pk values in tests.
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
    """Stores posts in a dict; assigns pk on save if absent."""

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
    """Thin UoW wrapper — exposes the shared repository."""

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


# ── UoW provider ──────────────────────────────────────────────────────────────


class FakeUoWProvider(IUoWProvider):
    """Returns a new UoW wrapping the shared repo on every call."""

    def __init__(self, repo: InMemoryPostRepository) -> None:
        self._repo = repo
        self.created_count = 0

    def make_uow(self) -> InMemoryUoW:
        self.created_count += 1
        return InMemoryUoW(self._repo)


# ── Allow-all authorizer ───────────────────────────────────────────────────────


class AllowAllAuthorizer(AbstractAuthorizer):
    """Allows every authorization check unconditionally."""

    async def authorize(
        self, ctx: AuthContext, action: Action, resource: Resource
    ) -> None:
        return  # Never raises.


# ── Capturing event producer ───────────────────────────────────────────────────


class CapturingProducer(AbstractEventProducer):
    """
    Records every event published via ``_produce()``.

    Used in tests to inspect what the service emits without a real event bus.

    Thread safety:  ❌ Single-threaded test use only.
    Async safety:   ✅ ``_produce`` is ``async def``.
    """

    def __init__(self) -> None:
        self.events: list[tuple[Any, str]] = []

    async def _produce(self, event: Any, *, channel: str = "default") -> None:
        """Append (event, channel) to the captured list."""
        self.events.append((event, channel))

    async def _produce_many(
        self, events: list[Any], *, channel: str = "default"
    ) -> None:
        for e in events:
            await self._produce(e, channel=channel)


# ── Concrete service ───────────────────────────────────────────────────────────


class ConcretePostService(
    AsyncService[Post, str, CreatePostDTO, PostReadDTO, UpdatePostDTO]
):
    """Minimal AsyncService subclass for testing tenant event propagation."""

    def __init__(
        self,
        uow_provider: FakeUoWProvider,
        producer: AbstractEventProducer,
    ) -> None:
        super().__init__(
            uow_provider=uow_provider,
            authorizer=AllowAllAuthorizer(),
            assembler=PostAssembler(),
        )
        # Override the default NoopEventProducer so we can capture events.
        self._producer = producer

    def _get_repo(self, uow: AsyncUnitOfWork) -> InMemoryPostRepository:
        return uow.get_repository(Post)


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture()
def repo() -> InMemoryPostRepository:
    """Fresh in-memory post repository."""
    return InMemoryPostRepository()


@pytest.fixture()
def producer() -> CapturingProducer:
    """Capturing event producer — records every emitted event."""
    return CapturingProducer()


@pytest.fixture()
def svc(
    repo: InMemoryPostRepository, producer: CapturingProducer
) -> ConcretePostService:
    """Fully-wired service with capturing producer."""
    return ConcretePostService(FakeUoWProvider(repo), producer)


@pytest.fixture()
def seeded_post(repo: InMemoryPostRepository) -> Post:
    """Pre-seed a single Post with a known pk for update/delete tests."""
    post = Post(pk="seed-1", title="original")
    repo._store["seed-1"] = post
    return post


# ── EntityEvent field tests ────────────────────────────────────────────────────


class TestEntityEventTenantId:
    """EntityEvent.tenant_id is present, optional, and JSON-round-trips."""

    def test_field_is_present_and_optional(self) -> None:
        """
        ``EntityCreatedEvent`` can be constructed with a ``tenant_id``.

        Verifies that the field was added to ``EntityEvent`` and propagates
        to all subclasses.
        """
        event = EntityCreatedEvent(
            entity_type="Post",
            pk="1",
            tenant_id="acme",
            payload={"title": "hello"},
        )
        assert event.tenant_id == "acme"

    def test_tenant_id_defaults_to_none(self) -> None:
        """
        Backward compatibility — omitting ``tenant_id`` yields ``None``.

        Existing code that constructs ``EntityCreatedEvent`` without ``tenant_id``
        must remain valid.
        """
        event = EntityCreatedEvent(
            entity_type="Post",
            pk="1",
            payload={"title": "hello"},
        )
        assert event.tenant_id is None

    def test_all_subclasses_inherit_tenant_id(self) -> None:
        """
        ``tenant_id`` is defined on ``EntityEvent`` and therefore available on
        all concrete subclasses.
        """
        created = EntityCreatedEvent(entity_type="X", pk=1, tenant_id="t1", payload={})
        updated = EntityUpdatedEvent(entity_type="X", pk=1, tenant_id="t2", payload={})
        deleted = EntityDeletedEvent(entity_type="X", pk=1, tenant_id="t3")

        assert created.tenant_id == "t1"
        assert updated.tenant_id == "t2"
        assert deleted.tenant_id == "t3"

    def test_tenant_id_survives_json_round_trip(self) -> None:
        """
        Events are serialized to JSON for Kafka / Redis transport.
        ``tenant_id`` must survive the round-trip so consumers can read it.
        """
        event = EntityCreatedEvent(
            entity_type="Post",
            pk="42",
            tenant_id="globex",
            payload={"title": "test"},
        )
        # Pydantic models round-trip via model_dump() + model_validate()
        serialized = event.model_dump()
        restored = EntityCreatedEvent.model_validate(serialized)
        assert restored.tenant_id == "globex"

    def test_entity_event_is_instance_of_base(self) -> None:
        """``isinstance`` check — subclass relationship is preserved."""
        event = EntityCreatedEvent(
            entity_type="Post", pk=1, tenant_id="acme", payload={}
        )
        assert isinstance(event, EntityEvent)


# ── Service → event propagation tests ─────────────────────────────────────────


class TestServicePopulatesTenantId:
    """
    AsyncService.create() / update() / delete() populate tenant_id from ctx.
    """

    async def test_create_populates_tenant_id(
        self, svc: ConcretePostService, producer: CapturingProducer
    ) -> None:
        """
        ``create()`` stamps the emitted ``EntityCreatedEvent`` with the
        ``tenant_id`` from ``ctx.metadata``.
        """
        ctx = AuthContext(metadata={"tenant_id": "acme"})
        await svc.create(CreatePostDTO(title="hello"), ctx)

        assert len(producer.events) == 1
        event, _ = producer.events[0]
        assert isinstance(event, EntityCreatedEvent)
        assert event.tenant_id == "acme"

    async def test_create_without_tenant_produces_none(
        self, svc: ConcretePostService, producer: CapturingProducer
    ) -> None:
        """
        ``create()`` with no tenant in ``ctx`` emits an event with
        ``tenant_id=None`` — single-tenant / admin context still works.
        """
        ctx = AuthContext()  # no metadata
        await svc.create(CreatePostDTO(title="hello"), ctx)

        event, _ = producer.events[0]
        assert event.tenant_id is None

    async def test_update_populates_tenant_id(
        self,
        svc: ConcretePostService,
        seeded_post: Post,
        producer: CapturingProducer,
    ) -> None:
        """
        ``update()`` stamps the emitted ``EntityUpdatedEvent`` with the
        caller's ``tenant_id``.
        """
        ctx = AuthContext(metadata={"tenant_id": "globex"})
        await svc.update("seed-1", UpdatePostDTO(title="new"), ctx)

        event, _ = producer.events[0]
        assert isinstance(event, EntityUpdatedEvent)
        assert event.tenant_id == "globex"

    async def test_delete_populates_tenant_id(
        self,
        svc: ConcretePostService,
        seeded_post: Post,
        producer: CapturingProducer,
    ) -> None:
        """
        ``delete()`` stamps the emitted ``EntityDeletedEvent`` with the
        caller's ``tenant_id``.
        """
        ctx = AuthContext(metadata={"tenant_id": "initech"})
        await svc.delete("seed-1", ctx)

        event, _ = producer.events[0]
        assert isinstance(event, EntityDeletedEvent)
        assert event.tenant_id == "initech"

    async def test_different_tenants_produce_different_events(
        self, svc: ConcretePostService, producer: CapturingProducer
    ) -> None:
        """
        Two creates by different tenants produce two events with distinct
        ``tenant_id`` values — no cross-contamination.
        """
        ctx_acme = AuthContext(metadata={"tenant_id": "acme"})
        ctx_globex = AuthContext(metadata={"tenant_id": "globex"})

        await svc.create(CreatePostDTO(title="acme-post"), ctx_acme)
        await svc.create(CreatePostDTO(title="globex-post"), ctx_globex)

        assert len(producer.events) == 2
        event_acme, _ = producer.events[0]
        event_globex, _ = producer.events[1]
        assert event_acme.tenant_id == "acme"
        assert event_globex.tenant_id == "globex"
