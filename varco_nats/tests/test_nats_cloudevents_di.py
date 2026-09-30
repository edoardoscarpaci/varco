"""
Plan 050 / Step 15 REPAIR (§D-wire's guard-test paragraph) — regression guard:
``bind_cloudevents_serializer()`` must reach ``NatsDLQConfiguration.nats_dlq``,
in the ``test_redis_cloudevents_di.py`` shape.

No Docker required — NATS I/O is monkeypatched to a no-op (the same
``_noop_start`` trick ``test_nats_dlq_disposes.py`` uses), so resolving the
provider never opens a socket.
"""

from __future__ import annotations

from typing import Annotated, Any

import pytest
from providify import DIContainer
from varco_core.event.cloudevents import (
    CloudEventsJsonSerializer,
    CloudEventsSettings,
    bind_cloudevents_serializer,
)
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_core.event.serializer import JsonEventSerializer
from varco_nats.dlq import NatsDLQ, NatsDLQConfiguration


async def _noop_start(self: Any) -> None:
    self._nc = object()
    self._js = object()
    self._started = True


async def test_regression_bound_serializer_reaches_the_nats_dlq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    User-visible symptom this pins: an app calls ``bind_cloudevents_serializer()``
    and expects every DLQ backend to write CloudEvents envelopes — before this
    guard a dropped ``serializer`` parameter on ``nats_dlq`` would silently
    keep writing plain JSON.
    """
    monkeypatch.setattr(NatsDLQ, "start", _noop_start)

    container = DIContainer()
    bind_cloudevents_serializer(container, CloudEventsSettings(source="/varco/tests"))
    await container.ainstall(NatsDLQConfiguration)

    dlq = await container.aget(AbstractDeadLetterQueue)
    assert isinstance(dlq, NatsDLQ)
    assert isinstance(dlq._serializer, CloudEventsJsonSerializer)  # type: ignore[attr-defined]


async def test_no_binding_leaves_the_json_default_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Edge case: nothing bound → the optional parameter stays ``None`` → ``JsonEventSerializer``."""
    monkeypatch.setattr(NatsDLQ, "start", _noop_start)

    container = DIContainer()
    await container.ainstall(NatsDLQConfiguration)

    dlq = await container.aget(AbstractDeadLetterQueue)
    assert isinstance(dlq._serializer, JsonEventSerializer)  # type: ignore[attr-defined]


async def test_negative_provider_copy_without_declared_serializer_loses_the_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Pins the exact failure shape a future refactor must not reintroduce: a
    ``@Provider`` method that does NOT declare ``serializer`` never receives
    the bound one, regardless of what else is bound — a copy of ``nats_dlq``
    with the parameter removed, installed into a fresh container.
    """
    from providify import Configuration, Fallback, InjectMeta, Provider
    from varco_core.di import BACKEND_PRIORITY
    from varco_core.di.select import selected_by
    from varco_nats.config import NatsEventBusSettings

    monkeypatch.setattr(NatsDLQ, "start", _noop_start)

    @Configuration
    class _NoSerializerConfiguration:
        @Fallback
        @selected_by("dlq", "nats")
        @Provider(singleton=True, priority=BACKEND_PRIORITY)
        async def nats_dlq(
            self,
            settings: Annotated[NatsEventBusSettings | None, InjectMeta(optional=True)] = None,
        ) -> AbstractDeadLetterQueue:
            settings = settings or NatsEventBusSettings.from_env()
            dlq = NatsDLQ(settings)
            await dlq.start()
            return dlq

    container = DIContainer()
    bind_cloudevents_serializer(container, CloudEventsSettings(source="/varco/tests"))
    await container.ainstall(_NoSerializerConfiguration)

    dlq = await container.aget(AbstractDeadLetterQueue)
    assert isinstance(dlq._serializer, JsonEventSerializer)  # type: ignore[attr-defined]
