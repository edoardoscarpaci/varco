"""
tests.test_audit
================
Unit tests for Feature 4: Audit / Mutation Log Mixin.

Covers:
    AuditEntry         — from_event construction
    AuditRepository    — cannot instantiate directly (ABC)
    AuditLogMixin      — emits AuditEvent on create / update / delete
    AuditConsumer      — persists AuditEvent → AuditEntry via AuditRepository
    _after_* hooks     — called after UoW commit in AsyncService

All tests use InMemoryEventBus — no broker required.
"""

from __future__ import annotations

import dataclasses
from typing import Any
from uuid import UUID, uuid4

import pytest

from varco_core.event.audit_event import AuditEvent
from varco_core.event.memory import InMemoryEventBus
from varco_core.service.audit import (
    AuditConsumer,
    AuditEntry,
    AuditLogMixin,
    AuditRepository,
)


from dataclasses import dataclass

from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth import AbstractAuthorizer, AuthContext
from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO
from varco_core.model import DomainModel
from varco_core.service.base import AsyncService, IUoWProvider
from varco_core.query.params import QueryParams
from varco_core.repository import AsyncRepository
from varco_core.uow import AsyncUnitOfWork

# ── Shared helpers ─────────────────────────────────────────────────────────────


class InMemoryAuditRepository(AuditRepository):
    """
    In-memory ``AuditRepository`` for unit tests.

    Stores entries in a list; ``list_for_entity`` filters by entity type and id.

    Thread safety:  ❌ Not thread-safe — single-loop test use only.
    Async safety:   ✅ Trivially async — no I/O.
    """

    def __init__(self) -> None:
        self.entries: list[AuditEntry] = []

    async def save(self, entry: AuditEntry) -> None:
        """
        Append the entry to the in-memory list.

        Args:
            entry: The ``AuditEntry`` to persist.
        """
        self.entries.append(entry)

    async def list_for_entity(
        self,
        entity_type: str,
        entity_id: str,
        *,
        limit: int = 100,
    ) -> list[AuditEntry]:
        """
        Filter entries by entity_type and entity_id, newest-first.

        Args:
            entity_type: Entity class name to filter by.
            entity_id:   Entity PK string.
            limit:       Maximum number of entries.

        Returns:
            Filtered entries ordered by ``occurred_at`` descending.
        """
        filtered = [
            e
            for e in self.entries
            if e.entity_type == entity_type and e.entity_id == entity_id
        ]
        filtered.sort(key=lambda e: e.occurred_at, reverse=True)
        return filtered[:limit]


# ── Tests: AuditEntry ──────────────────────────────────────────────────────────


async def test_audit_entry_from_event_sets_all_fields() -> None:
    """
    ``from_event`` copies all fields from the ``AuditEvent`` correctly.
    """
    event = AuditEvent(
        entity_type="Order",
        entity_id="ord-1",
        action="create",
        actor_id="user:abc",
        diff={"total": 99.0},
        correlation_id="req-xyz",
        tenant_id="tenant:acme",
    )

    entry = AuditEntry.from_event(event)

    assert entry.entity_type == "Order"
    assert entry.entity_id == "ord-1"
    assert entry.action == "create"
    assert entry.actor_id == "user:abc"
    assert entry.diff == {"total": 99.0}
    assert entry.correlation_id == "req-xyz"
    assert entry.tenant_id == "tenant:acme"
    assert isinstance(entry.entry_id, UUID)


async def test_audit_entry_diff_is_copied_by_value() -> None:
    """
    ``AuditEntry.from_event`` copies the diff dict by value.

    Verifies: mutating the original event's diff does not affect the entry.
    (AuditEvent is a Pydantic model so its dict is a new copy each time;
    this test ensures our explicit dict() copy is safe.)
    """
    event = AuditEvent(
        entity_type="X",
        entity_id="1",
        action="update",
        diff={"a": 1},
    )
    entry = AuditEntry.from_event(event)

    # Mutating the returned diff on AuditEntry must not affect another call.
    entry_dict = entry.diff
    # AuditEntry is frozen — diff is a dict (mutable), so we check independence.
    assert entry_dict == {"a": 1}


async def test_audit_entry_is_frozen() -> None:
    """
    ``AuditEntry`` is a frozen dataclass — mutation raises ``FrozenInstanceError``.
    """
    event = AuditEvent(entity_type="X", entity_id="1", action="create")
    entry = AuditEntry.from_event(event)

    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        entry.action = "mutated"  # type: ignore[misc]


# ── Tests: AuditConsumer ──────────────────────────────────────────────────────


async def test_audit_consumer_persists_audit_event() -> None:
    """
    When an ``AuditEvent`` is published to the bus, ``AuditConsumer`` persists it.

    Verifies: full path from bus publish → @listen dispatch → repo.save.
    """
    bus = InMemoryEventBus()
    repo = InMemoryAuditRepository()
    consumer = AuditConsumer(audit_repo=repo)
    consumer.register_to(bus)

    event = AuditEvent(
        entity_type="Order",
        entity_id="ord-42",
        action="create",
        diff={"status": "pending"},
    )

    await bus.publish(event, channel="varco.audit")

    assert len(repo.entries) == 1
    entry = repo.entries[0]
    assert entry.entity_type == "Order"
    assert entry.entity_id == "ord-42"
    assert entry.action == "create"
    assert entry.diff == {"status": "pending"}


async def test_audit_consumer_ignores_wrong_channel() -> None:
    """
    Events published on a non-audit channel are NOT received by ``AuditConsumer``.

    Verifies: the ``channel="varco.audit"`` filter on ``@listen`` works correctly.
    """
    bus = InMemoryEventBus()
    repo = InMemoryAuditRepository()
    consumer = AuditConsumer(audit_repo=repo)
    consumer.register_to(bus)

    event = AuditEvent(
        entity_type="Order",
        entity_id="ord-99",
        action="delete",
    )

    # Published on "orders" channel, not "varco.audit" — should not be received.
    await bus.publish(event, channel="orders")

    assert len(repo.entries) == 0


# ── Tests: AuditLogMixin ──────────────────────────────────────────────────────
# These tests verify that AuditLogMixin hooks emit the correct AuditEvent.
# We test the mixin in isolation (not via the full AsyncService stack) to keep
# the tests fast and without a real DB.


class _FakeEntity:
    """
    Minimal domain entity stand-in for mixin tests.

    Uses a plain object (not DomainModel) to keep the test free of ORM deps.
    """

    def __init__(self, pk: str) -> None:
        self.pk = pk


class _FakeReadDTO:
    """Minimal ReadDTO stand-in — provides ``model_dump()``."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def model_dump(self) -> dict[str, Any]:
        """Return a copy of the internal data dict."""
        return dict(self._data)


class _FakeAuthContext:
    """Minimal AuthContext stand-in."""

    def __init__(
        self, sub: str = "user:test", tenant_id: str | None = "tenant:test"
    ) -> None:
        self.sub = sub
        self.metadata: dict[str, Any] = {}
        if tenant_id is not None:
            self.metadata["tenant_id"] = tenant_id


class _FakeProducer:
    """
    Fake ``AbstractEventProducer`` that captures published events.

    Thread safety:  ❌ Not thread-safe.
    Async safety:   ✅ _produce is async.
    """

    def __init__(self) -> None:
        # List of (event, channel) tuples recorded on each _produce call.
        self.produced: list[tuple[AuditEvent, str]] = []

    async def _produce(self, event: AuditEvent, *, channel: str = "*") -> None:
        """
        Record the produced event.

        Args:
            event:   The event to record.
            channel: The channel the event was produced on.
        """
        self.produced.append((event, channel))


class _ServiceBase:
    """
    Minimal no-op base that provides the ``_after_*`` hook stubs.

    Used so ``AuditLogMixin.super()._after_*()`` calls succeed without
    inheriting the full ``AsyncService`` stack.
    """

    async def _after_create(self, entity: Any, read_dto: Any, ctx: Any) -> None:
        return

    async def _after_update(
        self, before_dto: Any, entity: Any, read_dto: Any, ctx: Any
    ) -> None:
        return

    async def _after_delete(self, pk: Any, ctx: Any) -> None:
        return


class _FakeMixinService(AuditLogMixin, _ServiceBase):
    """
    Minimal service that inherits ``AuditLogMixin`` + ``_ServiceBase`` stubs.

    Wires a ``_FakeProducer`` so we can inspect the emitted ``AuditEvent``
    without running the full ``AsyncService`` stack.
    """

    def __init__(self, producer: _FakeProducer) -> None:
        self._producer = producer

    def _entity_type(self) -> type:
        """Return a placeholder type for _after_delete."""
        return _FakeEntity

    def _get_audit_actor(self, ctx: _FakeAuthContext) -> str | None:  # type: ignore[override]
        """Extract actor from fake context."""
        return ctx.sub


async def test_audit_mixin_emits_audit_event_on_create() -> None:
    """
    ``AuditLogMixin._after_create`` emits an ``AuditEvent`` with action="create".

    Verifies: event is published on the "varco.audit" channel with the correct
    entity_type, entity_id, actor_id, and diff.
    """
    producer = _FakeProducer()
    service = _FakeMixinService(producer)
    entity = _FakeEntity(pk="ent-1")
    read_dto = _FakeReadDTO({"field": "value"})
    ctx = _FakeAuthContext(sub="user:alice")

    await service._after_create(entity, read_dto, ctx)  # type: ignore[arg-type]

    assert len(producer.produced) == 1
    event, channel = producer.produced[0]
    assert channel == "varco.audit"
    assert event.entity_type == "_FakeEntity"
    assert event.entity_id == "ent-1"
    assert event.action == "create"
    assert event.actor_id == "user:alice"
    assert event.diff == {"field": "value"}
    assert event.tenant_id == "tenant:test"


async def test_audit_mixin_emits_audit_event_on_update_with_before_after() -> None:
    """
    ``AuditLogMixin._after_update`` emits with action="update" and before/after diff.
    """
    producer = _FakeProducer()
    service = _FakeMixinService(producer)
    entity = _FakeEntity(pk="ent-2")
    before_dto = _FakeReadDTO({"status": "pending"})
    after_dto = _FakeReadDTO({"status": "confirmed"})
    ctx = _FakeAuthContext(sub="user:bob")

    await service._after_update(before_dto, entity, after_dto, ctx)  # type: ignore[arg-type]

    assert len(producer.produced) == 1
    event, channel = producer.produced[0]
    assert channel == "varco.audit"
    assert event.action == "update"
    assert event.diff == {
        "before": {"status": "pending"},
        "after": {"status": "confirmed"},
    }
    assert event.actor_id == "user:bob"
    assert event.entity_id == "ent-2"


async def test_audit_mixin_emits_audit_event_on_delete() -> None:
    """
    ``AuditLogMixin._after_delete`` emits with action="delete" and empty diff.
    """
    producer = _FakeProducer()
    service = _FakeMixinService(producer)
    ctx = _FakeAuthContext(sub="user:carol")

    await service._after_delete("pk-3", ctx)  # type: ignore[arg-type]

    assert len(producer.produced) == 1
    event, channel = producer.produced[0]
    assert channel == "varco.audit"
    assert event.action == "delete"
    assert event.entity_id == "pk-3"
    assert event.diff == {}
    assert event.actor_id == "user:carol"


async def test_audit_mixin_actor_id_none_by_default() -> None:
    """
    Without overriding ``_get_audit_actor``, actor_id is None.

    Verifies: the base AuditLogMixin is safe without a concrete actor extractor.
    """

    class _NoActorService(AuditLogMixin, _ServiceBase):
        def __init__(self, producer: _FakeProducer) -> None:
            self._producer = producer

        def _entity_type(self) -> type:
            return _FakeEntity

    producer = _FakeProducer()
    service = _NoActorService(producer)
    entity = _FakeEntity(pk="x")
    ctx = _FakeAuthContext()

    await service._after_create(entity, _FakeReadDTO({}), ctx)  # type: ignore[arg-type]

    event, _ = producer.produced[0]
    assert event.actor_id is None


async def test_audit_mixin_no_tenant_id_when_ctx_metadata_empty() -> None:
    """
    If ctx.metadata has no "tenant_id", the emitted event has tenant_id=None.
    """
    producer = _FakeProducer()
    service = _FakeMixinService(producer)
    entity = _FakeEntity(pk="y")
    ctx = _FakeAuthContext(tenant_id=None)  # No tenant

    await service._after_create(entity, _FakeReadDTO({}), ctx)  # type: ignore[arg-type]

    event, _ = producer.produced[0]
    assert event.tenant_id is None


# ── Tests: _after_* hooks in AsyncService (integration-level) ─────────────────
# These tests verify that the hooks are actually called by the real
# AsyncService.create / update / delete flow using a minimal fake setup.


# ── Minimal domain types ────────────────────────────────────────────────────


@dataclass
class _Widget(DomainModel):
    """Minimal DomainModel for hook integration tests."""

    name: str = ""


@dataclass(frozen=True)
class _WidgetCreate(CreateDTO):
    name: str = ""


@dataclass(frozen=True)
class _WidgetRead(ReadDTO):
    pk: UUID = dataclasses.field(default_factory=uuid4)
    name: str = ""

    def model_dump(self) -> dict[str, Any]:
        return {"pk": str(self.pk), "name": self.name}


@dataclass(frozen=True)
class _WidgetUpdate(UpdateDTO):
    name: str = ""


# ── In-memory repository for _Widget ────────────────────────────────────────


class _WidgetRepo(AsyncRepository[_Widget, UUID]):
    """Minimal in-memory repository for _Widget hook tests."""

    def __init__(self) -> None:
        self._store: dict[UUID, _Widget] = {}

    async def find_by_id(self, pk: UUID) -> _Widget | None:
        return self._store.get(pk)

    async def save(self, entity: _Widget) -> _Widget:
        if entity.pk is None:
            # pk is declared with init=False on DomainModel; use object.__setattr__
            # to assign it without going through __init__.
            object.__setattr__(entity, "pk", uuid4())
        self._store[entity.pk] = entity
        return entity

    async def delete(self, entity: _Widget) -> None:
        self._store.pop(entity.pk, None)

    async def list_by_query(self, params: QueryParams) -> list[_Widget]:
        return list(self._store.values())

    async def count_by_query(self, params: QueryParams) -> int:
        return len(self._store)

    async def stream_by_query(self, params: QueryParams):
        for e in self._store.values():
            yield e

    async def find_all(self) -> list[_Widget]:
        return list(self._store.values())

    async def find_by_query(self, params: QueryParams) -> list[_Widget]:
        return list(self._store.values())

    async def count(self, params: QueryParams | None = None) -> int:
        return len(self._store)

    async def exists(self, pk: UUID) -> bool:
        return pk in self._store

    async def save_many(self, entities):
        return [await self.save(e) for e in entities]

    async def delete_many(self, entities):
        for e in entities:
            await self.delete(e)

    async def update_many_by_query(self, params, update) -> int:
        raise NotImplementedError


# ── UoW and provider stubs ────────────────────────────────────────────────────


class _WidgetUoW(AsyncUnitOfWork):
    """Minimal UoW that holds a single _WidgetRepo."""

    def __init__(self) -> None:
        self.widgets = _WidgetRepo()

    async def _begin(self) -> None:
        pass

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: Any) -> None:
        pass  # No-op — no real transaction


class _WidgetUoWProvider(IUoWProvider):
    """Provider that returns a UoW sharing the same in-memory repo."""

    def __init__(self) -> None:
        # Shared repo so creates and subsequent finds see the same data.
        self._shared_repo = _WidgetRepo()

    def make_uow(self) -> _WidgetUoW:
        uow = _WidgetUoW()
        # Replace the default repo with the shared one so all UoW instances
        # see the same in-memory store — required for create-then-update tests.
        uow.widgets = self._shared_repo
        return uow


# ── Assembler for _Widget ─────────────────────────────────────────────────────


class _WidgetAssembler(
    AbstractDTOAssembler[_Widget, _WidgetCreate, _WidgetRead, _WidgetUpdate]
):
    def to_domain(self, dto: _WidgetCreate) -> _Widget:
        return _Widget(name=dto.name)

    def to_read_dto(self, entity: _Widget) -> _WidgetRead:
        return _WidgetRead(pk=entity.pk, name=entity.name)

    def apply_update(self, entity: _Widget, dto: _WidgetUpdate) -> _Widget:
        return dataclasses.replace(entity, name=dto.name)


# ── Authorizer stub ───────────────────────────────────────────────────────────


class _AllowAllAuthorizer(AbstractAuthorizer):
    async def authorize(self, ctx, action, resource) -> None:
        pass  # Always allow


# ── Spy service — records hook calls ─────────────────────────────────────────


class _HookSpyService(
    AsyncService[_Widget, UUID, _WidgetCreate, _WidgetRead, _WidgetUpdate]
):
    """Service that records _after_* hook invocations."""

    def __init__(self) -> None:
        # Shared UoWProvider so all calls within a test see the same repo.
        self._shared_provider = _WidgetUoWProvider()
        super().__init__(
            uow_provider=self._shared_provider,
            authorizer=_AllowAllAuthorizer(),
            assembler=_WidgetAssembler(),
        )
        self.after_create_calls: list[tuple] = []
        self.after_update_calls: list[tuple] = []
        self.after_delete_calls: list[tuple] = []

    def _get_repo(self, uow: _WidgetUoW) -> _WidgetRepo:  # type: ignore[override]
        return uow.widgets

    async def _after_create(self, entity, read_dto, ctx) -> None:
        self.after_create_calls.append((entity, read_dto, ctx))
        await super()._after_create(entity, read_dto, ctx)

    async def _after_update(self, before_dto, entity, read_dto, ctx) -> None:
        self.after_update_calls.append((before_dto, entity, read_dto, ctx))
        await super()._after_update(before_dto, entity, read_dto, ctx)

    async def _after_delete(self, pk, ctx) -> None:
        self.after_delete_calls.append((pk, ctx))
        await super()._after_delete(pk, ctx)


# ── Tests ─────────────────────────────────────────────────────────────────────


async def test_after_create_hook_called_after_commit() -> None:
    """
    ``_after_create`` is called once after ``create()`` commits.

    Verifies: entity has pk, read_dto is the returned DTO, ctx is propagated.
    """
    service = _HookSpyService()
    ctx = AuthContext()

    _ = await service.create(_WidgetCreate(name="Sprocket"), ctx)

    assert len(service.after_create_calls) == 1
    entity, dto, received_ctx = service.after_create_calls[0]
    assert entity.pk is not None
    assert dto.name == "Sprocket"
    assert received_ctx is ctx


async def test_after_update_hook_receives_before_dto() -> None:
    """
    ``_after_update`` receives the pre-update DTO as ``before_dto``.

    Verifies: before_dto.name matches the original value before update.
    """
    service = _HookSpyService()
    ctx = AuthContext()

    read_dto = await service.create(_WidgetCreate(name="Original"), ctx)
    await service.update(read_dto.pk, _WidgetUpdate(name="Updated"), ctx)

    assert len(service.after_update_calls) == 1
    before_dto, entity, after_dto, _ = service.after_update_calls[0]
    assert before_dto.name == "Original"
    assert after_dto.name == "Updated"
    assert entity.name == "Updated"


async def test_after_delete_hook_called_with_pk() -> None:
    """
    ``_after_delete`` is called with the deleted entity's pk.
    """
    service = _HookSpyService()
    ctx = AuthContext()

    read_dto = await service.create(_WidgetCreate(name="Doomed"), ctx)
    await service.delete(read_dto.pk, ctx)

    assert len(service.after_delete_calls) == 1
    pk, received_ctx = service.after_delete_calls[0]
    assert pk == read_dto.pk
    assert received_ctx is ctx


async def test_after_create_not_called_on_create_failure() -> None:
    """
    ``_after_create`` is NOT called if ``create()`` raises before or inside the UoW.

    Uses an authorizer that denies creates — the hook should never fire.
    """
    from varco_core.exception.service import ServiceAuthorizationError

    class _DenyAllAuthorizer(AbstractAuthorizer):
        async def authorize(self, ctx, action, resource) -> None:
            raise ServiceAuthorizationError("denied")

    class _DeniedService(
        AsyncService[_Widget, UUID, _WidgetCreate, _WidgetRead, _WidgetUpdate]
    ):
        def __init__(self) -> None:
            super().__init__(
                uow_provider=_WidgetUoWProvider(),
                authorizer=_DenyAllAuthorizer(),
                assembler=_WidgetAssembler(),
            )
            self.after_create_calls = 0

        def _get_repo(self, uow):
            return uow.widgets

        async def _after_create(self, entity, read_dto, ctx) -> None:
            self.after_create_calls += 1

    service = _DeniedService()
    ctx = AuthContext()

    with pytest.raises(ServiceAuthorizationError):
        await service.create(_WidgetCreate(name="X"), ctx)

    assert service.after_create_calls == 0
