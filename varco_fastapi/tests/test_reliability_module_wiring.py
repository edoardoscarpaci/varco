"""
Plan 050 / Step 10 — `OutboxRelay`/`AuditConsumer` get DI-wired serializer +
proper reverse-order shutdown (§D-wire row 1). Docker-free.

RED MODE: `ReliabilityPreset` has no `serializer` field, `ReliabilityLifecycle`
resolves no `Serializer[Event]` at all, and shutdown never unregisters the
`AuditConsumer` — every assertion below is expected to fail.
"""

from __future__ import annotations

from uuid import UUID

from providify import DIContainer
from varco_core.di import bind_instance
from varco_core.di import bootstrap as core_bootstrap
from varco_core.event.base import AbstractEventBus, Event
from varco_core.event.cloudevents import (
    CloudEventsJsonSerializer,
    CloudEventsSettings,
    bind_cloudevents_serializer,
)
from varco_core.event.memory import InMemoryEventBus
from varco_core.event.serializer import JsonEventSerializer
from varco_core.reliability.preset import ReliabilityPreset
from varco_core.serialization import Serializer
from varco_core.service.audit import AuditEntry, AuditRepository
from varco_core.service.outbox import OutboxEntry, OutboxRepository
from varco_fastapi.modules.lifecycle import ReliabilityModule


class _InMemoryOutbox(OutboxRepository):
    async def save(self, entry: OutboxEntry) -> None:
        return None

    async def get_pending(self, *, limit: int = 100) -> list[OutboxEntry]:
        return []

    async def delete(self, entry_id: UUID) -> None:
        return None


class _InMemoryAudit(AuditRepository):
    async def save(self, entry: AuditEntry) -> None:
        return None

    async def list_for_entity(
        self, entity_type: str, entity_id: str, **kwargs: object
    ) -> list[AuditEntry]:
        return []


def test_reliability_preset_has_a_serializer_field() -> None:
    # An additive frozen-dataclass field, per §D-wire.
    preset = ReliabilityPreset(serializer=JsonEventSerializer())  # type: ignore[call-arg]
    assert preset.serializer is not None  # type: ignore[attr-defined]


async def test_outbox_relay_serializer_is_the_bound_cloudevents_serializer() -> None:
    container = DIContainer()
    core_bootstrap(container)
    bind_instance(container, AbstractEventBus, InMemoryEventBus())
    bind_cloudevents_serializer(container, CloudEventsSettings(source="/varco/tests"))
    bind_instance(container, OutboxRepository, _InMemoryOutbox())

    module = ReliabilityModule(ReliabilityPreset(outbox=True))
    await module.start(container)
    try:
        relay = module.relay  # type: ignore[attr-defined]
        assert relay is not None
        assert relay._serializer is container.get(Serializer[Event])
        assert isinstance(relay._serializer, CloudEventsJsonSerializer)
    finally:
        await module.stop(container)


async def test_explicit_preset_serializer_wins_over_the_bound_one() -> None:
    container = DIContainer()
    core_bootstrap(container)
    bind_instance(container, AbstractEventBus, InMemoryEventBus())
    bind_cloudevents_serializer(container, CloudEventsSettings(source="/varco/tests"))
    bind_instance(container, OutboxRepository, _InMemoryOutbox())

    explicit = JsonEventSerializer()
    module = ReliabilityModule(ReliabilityPreset(outbox=True, serializer=explicit))  # type: ignore[call-arg]
    await module.start(container)
    try:
        assert module.relay._serializer is explicit  # type: ignore[attr-defined]
    finally:
        await module.stop(container)


async def test_audit_consumer_is_unregistered_from_the_bus_on_stop() -> None:
    container = DIContainer()
    core_bootstrap(container)
    bus = InMemoryEventBus()
    bind_instance(container, AbstractEventBus, bus)
    bind_instance(container, AuditRepository, _InMemoryAudit())

    module = ReliabilityModule(ReliabilityPreset(audit=True))
    before_count = len(bus.subscriptions) if hasattr(bus, "subscriptions") else None

    await module.start(container)
    consumer = module._lifecycle._audit_consumer  # type: ignore[attr-defined]
    assert consumer is not None
    assert consumer.is_registered_to(bus) is True

    await module.stop(container)
    assert consumer.is_registered_to(bus) is False, (
        "AuditConsumer is never unregistered on shutdown() today (§Context row)"
    )
    if before_count is not None:
        assert len(bus.subscriptions) == before_count
