"""
Plan 050 / Step 15 REPAIR (§D-wire's guard-test paragraph) — regression guard:
``bind_cloudevents_serializer()`` must reach ``SAConfiguration.dead_letter_queue``
(N1), in the ``test_redis_cloudevents_di.py`` shape.

Why this file exists
--------------------
``SAConfiguration.dead_letter_queue`` (``varco_sa/di.py``) already declares an
optional ``serializer: Annotated[Serializer[Event] | None, InjectMeta(optional=True)]``
parameter — this is the regression guard that pins it, so a future refactor
that drops the parameter (the exact "an undeclared ``Serializer[Event]``
silently never arrives" failure CLAUDE.md's CloudEvents section warns about)
fails a named test instead of silently reverting every SA-backed CloudEvents
app to plain JSON dead letters.

Docker-free — sqlite in-memory ``AsyncEngine``; no connection is opened by
resolving the provider (only ``push``/``pop_batch`` touch the database).
"""

from __future__ import annotations

import pytest
from providify import DIContainer, Inject
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_core.event.cloudevents import (
    CloudEventsJsonSerializer,
    CloudEventsSettings,
    bind_cloudevents_serializer,
)
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_core.event.serializer import JsonEventSerializer


def _sa_config() -> object:
    from varco_sa.config import SAConfig

    class _Base(DeclarativeBase):
        pass

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    return SAConfig(engine=engine, base=_Base, entity_classes=())


def _container(monkeypatch: pytest.MonkeyPatch) -> DIContainer:
    from varco_core.di import bind_instance
    from varco_sa.config import SAConfig
    from varco_sa.di import bootstrap

    monkeypatch.setenv("VARCO_DLQ_BACKEND", "sa")

    container = bootstrap(DIContainer())
    bind_instance(container, SAConfig, _sa_config())
    bind_cloudevents_serializer(container, CloudEventsSettings(source="/varco/tests"))
    return container


def test_regression_bound_serializer_reaches_the_sa_dlq(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    User-visible symptom this pins: an app calls ``bind_cloudevents_serializer()``
    and expects every DLQ backend to write CloudEvents envelopes — before this
    guard, a dropped ``serializer`` parameter on ``dead_letter_queue`` would do
    so silently.
    """
    from varco_sa.dlq import SADeadLetterQueue

    container = _container(monkeypatch)
    dlq = container.get(AbstractDeadLetterQueue)

    assert isinstance(dlq, SADeadLetterQueue)
    assert isinstance(dlq._serializer, CloudEventsJsonSerializer)  # type: ignore[attr-defined]


def test_no_binding_leaves_the_json_default_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    """Edge case: nothing bound → the optional parameter stays ``None`` → ``JsonEventSerializer``."""
    from varco_core.di import bind_instance
    from varco_sa.config import SAConfig
    from varco_sa.di import bootstrap

    monkeypatch.setenv("VARCO_DLQ_BACKEND", "sa")
    container = bootstrap(DIContainer())
    bind_instance(container, SAConfig, _sa_config())

    dlq = container.get(AbstractDeadLetterQueue)
    assert isinstance(dlq._serializer, JsonEventSerializer)  # type: ignore[attr-defined]


def test_negative_provider_copy_without_declared_serializer_loses_the_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Pins the exact failure shape a future refactor must not reintroduce: a
    ``@Provider`` method that does NOT declare ``serializer`` never receives
    the bound one, regardless of what else is bound in the container — this
    is a copy of ``dead_letter_queue`` with the parameter removed, installed
    into a fresh container.
    """
    from providify import Configuration, Fallback, Provider
    from varco_sa.config import SAConfig
    from varco_sa.di import BACKEND_PRIORITY, selected_by
    from varco_sa.dlq import SADeadLetterQueue

    @Configuration
    class _NoSerializerConfiguration:
        @Fallback
        @selected_by("dlq", "sa")
        @Provider(singleton=True, priority=BACKEND_PRIORITY)
        def dead_letter_queue(
            self,
            config: Inject[SAConfig],
        ) -> AbstractDeadLetterQueue:
            return SADeadLetterQueue(config.engine)

    monkeypatch.setenv("VARCO_DLQ_BACKEND", "sa")
    container = DIContainer()
    from varco_core.di import bind_instance

    bind_instance(container, SAConfig, _sa_config())
    bind_cloudevents_serializer(container, CloudEventsSettings(source="/varco/tests"))
    container.install(_NoSerializerConfiguration)

    dlq = container.get(AbstractDeadLetterQueue)
    assert isinstance(dlq._serializer, JsonEventSerializer)  # type: ignore[attr-defined]
