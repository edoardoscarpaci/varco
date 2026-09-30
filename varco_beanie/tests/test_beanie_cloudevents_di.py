"""
Plan 050 / Step 15 REPAIR (§D-wire's guard-test paragraph) — regression guard:
``bind_cloudevents_serializer()`` must reach ``BeanieDeadLetterQueue``, in the
``test_redis_cloudevents_di.py`` shape.

``BeanieDeadLetterQueue`` is a scanned ``@Singleton`` (unlike the SA/Kafka/NATS
DLQs, which are ``@Provider``-method-produced) — providify injects every
declared constructor parameter for a scanned singleton with no risk of a
"provider method forgot to declare it" gap (there is no separate provider
method to forget). The "negative" half of this guard therefore checks the
one place that gap CAN still occur for this backend: the un-bound case
(nothing bound → the optional parameter stays ``None``).

Docker-free — ``BeanieDeadLetterQueue`` opens no connection until ``init_beanie()``
registers it (which this test never calls); resolving the DI binding is pure
construction.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_core.event.cloudevents import (
    CloudEventsJsonSerializer,
    CloudEventsSettings,
    bind_cloudevents_serializer,
)
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_core.event.serializer import JsonEventSerializer


def _container(monkeypatch: pytest.MonkeyPatch) -> DIContainer:
    from varco_beanie.di import bootstrap

    monkeypatch.setenv("VARCO_DLQ_BACKEND", "beanie")
    container = bootstrap(DIContainer())
    bind_cloudevents_serializer(container, CloudEventsSettings(source="/varco/tests"))
    return container


async def test_regression_bound_serializer_reaches_the_beanie_dlq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    User-visible symptom this pins: an app calls ``bind_cloudevents_serializer()``
    and expects every DLQ backend to write CloudEvents envelopes.
    """
    from varco_beanie.dlq import BeanieDeadLetterQueue

    container = _container(monkeypatch)
    dlq = await container.aget(AbstractDeadLetterQueue)

    assert isinstance(dlq, BeanieDeadLetterQueue)
    assert isinstance(dlq._serializer, CloudEventsJsonSerializer)  # type: ignore[attr-defined]


async def test_no_binding_leaves_the_json_default_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Edge case: nothing bound → the optional parameter stays ``None`` → ``JsonEventSerializer``."""
    from varco_beanie.di import bootstrap

    monkeypatch.setenv("VARCO_DLQ_BACKEND", "beanie")
    container = bootstrap(DIContainer())

    dlq = await container.aget(AbstractDeadLetterQueue)
    assert isinstance(dlq._serializer, JsonEventSerializer)  # type: ignore[attr-defined]
