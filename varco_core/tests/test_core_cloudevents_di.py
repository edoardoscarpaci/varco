"""
Plan 050 / Step 15 REPAIR (§D-wire's guard-test paragraph) — regression guard:
``bind_cloudevents_serializer()`` must reach ``OutboxRelay`` through
``ReliabilityModule`` (§D-wire row 1), in the ``test_redis_cloudevents_di.py``
shape.

``WebhookDispatcher`` is deliberately **not** covered here — CLAUDE.md's
CloudEvents section corrects an earlier claim that it was: the dispatcher
never held a ``Serializer[Event]`` at all, delivering over HTTP with its own
signing scheme rather than the event bus's wire format. Likewise,
``InMemoryDeadLetterQueue`` (the in-process DLQ) stores the raw ``Event``
object rather than serialized bytes, so there is no serializer parameter to
guard on that class. This file's positive/negative pair therefore targets
the one object in ``varco_core``/``varco_fastapi`` that actually resolves
``Serializer[Event]`` from the container: the outbox relay built by
``ReliabilityModule``.

Docker-free — an ``InMemoryEventBus`` and a stub ``OutboxRepository``; no
broker, no database.
"""

from __future__ import annotations

from uuid import UUID

from providify import DIContainer
from varco_core.di import bind_instance
from varco_core.di import bootstrap as core_bootstrap
from varco_core.event.base import AbstractEventBus
from varco_core.event.cloudevents import (
    CloudEventsJsonSerializer,
    CloudEventsSettings,
    bind_cloudevents_serializer,
)
from varco_core.event.memory import InMemoryEventBus
from varco_core.event.serializer import JsonEventSerializer
from varco_core.service.outbox import OutboxEntry, OutboxRelay, OutboxRepository


class _InMemoryOutbox(OutboxRepository):
    async def save(self, entry: OutboxEntry) -> None:
        return None

    async def get_pending(self, *, limit: int = 100) -> list[OutboxEntry]:
        return []

    async def delete(self, entry_id: UUID) -> None:
        return None


def _container() -> DIContainer:
    container = DIContainer()
    core_bootstrap(container)
    bind_instance(container, AbstractEventBus, InMemoryEventBus())
    bind_cloudevents_serializer(container, CloudEventsSettings(source="/varco/tests"))
    bind_instance(container, OutboxRepository, _InMemoryOutbox())
    return container


async def test_regression_bound_serializer_reaches_the_outbox_relay() -> None:
    """
    User-visible symptom this pins: an app calls ``bind_cloudevents_serializer()``
    and expects the outbox relay to publish CloudEvents envelopes — before
    Plan 050 / Step 10 the relay always constructed a plain
    ``JsonEventSerializer()`` regardless of what was bound.
    """
    from varco_core.reliability.preset import ReliabilityPreset
    from varco_fastapi.modules.lifecycle import ReliabilityModule

    container = _container()
    module = ReliabilityModule(ReliabilityPreset(outbox=True))
    await module.start(container)
    try:
        relay = module.relay  # type: ignore[attr-defined]
        assert relay is not None
        assert isinstance(relay._serializer, CloudEventsJsonSerializer)  # type: ignore[attr-defined]
    finally:
        await module.stop(container)


async def test_no_binding_leaves_the_json_default_untouched() -> None:
    """
    Edge case: the hand-constructed form (no container involved at all) still
    defaults to ``JsonEventSerializer`` when ``serializer=`` is omitted — only
    ``ReliabilityModule``'s container resolution wires a bound one in
    automatically.
    """
    relay = OutboxRelay(outbox=_InMemoryOutbox(), bus=InMemoryEventBus())
    assert isinstance(relay._serializer, JsonEventSerializer)  # type: ignore[attr-defined]
