"""
DI wiring tests for ``varco_nats``.

``NatsChannelManagerSettings`` is a pydantic ``BaseSettings`` subclass.  It used
to carry ``@Singleton`` on the class itself, which cannot work: providify
injects the constructor, and pydantic's ``BaseSettings.__init__`` signature is
``(**values: Any)``.  Resolution therefore died with
``LookupError: Cannot resolve 'values: typing.Any'``, taking every consumer of
those settings (``NatsStreamManager``) down with it.

Settings classes must be registered through a ``@Provider`` instead — the same
rule already applied in ``varco_casbin/di.py`` and ``varco_fastapi/di.py``.

No NATS server is required: only container resolution is exercised.  The stream
manager itself is *not* resolved here — its ``@PostConstruct`` opens a
connection, which belongs to the integration suite.
"""

from __future__ import annotations

import sys

import pydantic
import pytest
from providify import DIContainer, Provider, Singleton
from varco_conformance.providify_health import assert_no_structural_di_issues
from varco_core.event import AbstractEventBus
from varco_core.event.channel import ChannelManager
from varco_core.event.config import EventBusSettings
from varco_nats.bus import NatsEventBus
from varco_nats.channel import NatsChannelManagerSettings, NatsStreamManager
from varco_nats.config import NatsEventBusSettings


@Provider(singleton=True, priority=100)
def _custom_settings() -> NatsChannelManagerSettings:
    """App-supplied override — module scope so its lazy annotation resolves."""
    return NatsChannelManagerSettings(stream_name="custom-stream")


class TestNatsChannelManagerSettingsDI:
    def test_regression_settings_resolve_from_a_scanned_container(self) -> None:
        """
        User-visible symptom: ``container.get(ChannelManager, qualifier="nats")``
        raised ``LookupError: Cannot resolve 'values: typing.Any'``.  Correct
        behaviour is a ready-to-use settings instance, because a pydantic
        ``BaseSettings`` must be built by a factory, never constructor-injected.
        """
        from varco_nats.di import bootstrap

        container = DIContainer()
        bootstrap(container)

        settings = container.get(NatsChannelManagerSettings)

        assert isinstance(settings, NatsChannelManagerSettings)
        assert settings.stream_name

    def test_settings_are_a_singleton(self) -> None:
        from varco_nats.di import bootstrap

        container = DIContainer()
        bootstrap(container)

        assert container.get(NatsChannelManagerSettings) is container.get(
            NatsChannelManagerSettings
        )

    def test_user_supplied_settings_win_over_the_default(self) -> None:
        """The default provider registers at the lowest possible priority."""
        from varco_nats.di import bootstrap

        container = DIContainer()
        container.provide(_custom_settings)
        bootstrap(container)

        assert container.get(NatsChannelManagerSettings).stream_name == "custom-stream"

    def test_stream_manager_binding_is_registered(self) -> None:
        """
        The manager is not *resolved* here (its ``@PostConstruct`` connects to
        a NATS server) — only its binding presence is asserted, so a future
        refactor cannot silently drop it.
        """
        container = DIContainer()
        container.scan("varco_nats", recursive=True)

        implementations = {getattr(b, "implementation", None) for b in container._bindings}
        assert NatsStreamManager in implementations
        assert any(b.interface in (ChannelManager, NatsStreamManager) for b in container._bindings)


class TestNatsContainerValidates:
    def test_regression_every_binding_resolves_its_annotations(self) -> None:
        """
        User-visible symptom: bootstrapping an app that scanned ``varco_nats``
        died with ``AnnotationResolutionError`` on ``NatsEventBus.__init__``
        parameter ``serializer``, because the ``EventSerializer`` alias was a quoted
        forward reference bound to a ``str`` at runtime, making
        ``EventSerializer | None`` unevaluatable.

        ``validate_bindings()`` resolves annotations for *every* registered
        binding, so it catches an unresolvable constructor hint on any singleton
        in the package — not just the ones a test happens to resolve directly.
        """
        container = DIContainer()
        container.scan("varco_nats", recursive=True)

        container.validate_bindings()
        assert_no_structural_di_issues(container)


class TestNatsEventBusSettingsCharacterization:
    """
    Plan 014 / Part A (F1) — characterization test pinning **current** behaviour.

    See ``varco_kafka/tests/test_kafka_di.py``'s sibling class for the full
    rationale; this mirrors it exactly for ``NatsEventBusSettings``.
    """

    def test_characterization_settings_resolve_through_the_container(self) -> None:
        from varco_nats.di import bootstrap

        container = DIContainer()
        bootstrap(container)

        settings = container.get(NatsEventBusSettings)

        assert isinstance(settings, NatsEventBusSettings)
        assert settings.servers

    def test_characterization_settings_are_a_singleton(self) -> None:
        from varco_nats.di import bootstrap

        container = DIContainer()
        bootstrap(container)

        assert container.get(NatsEventBusSettings) is container.get(NatsEventBusSettings)

    async def test_characterization_event_bus_resolves_with_injected_settings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        Load-bearing test — see the kafka sibling for why ``start()`` is
        stubbed to a no-op and ``await container.aget(...)`` is used instead
        of the synchronous ``container.get()``.
        """

        async def _noop_start(self: NatsEventBus) -> None:
            return None

        monkeypatch.setattr(NatsEventBus, "start", _noop_start)

        from varco_nats.di import bootstrap

        container = DIContainer()
        bootstrap(container)

        bus = await container.aget(AbstractEventBus, qualifier="nats")

        assert isinstance(bus, NatsEventBus)
        assert bus._config is container.get(NatsEventBusSettings)


class RequiredFieldSettingsForTest(EventBusSettings):
    """Module-scope settings subclass with one non-defaulted field — Step 5."""

    required_value: str


@Singleton
class SingletonRequiredFieldSettingsForTest(RequiredFieldSettingsForTest):
    pass


class TestNatsRequiredFieldCharacterization:
    def test_characterization_required_field_raises_validation_error_not_lookup_error(
        self,
    ) -> None:
        """
        Corrects ``audits/001-audit-di-wiring.md:19``'s predicted ``LookupError``
        — see the kafka sibling test for the full rationale.
        """
        container = DIContainer()
        container.scan(sys.modules[__name__])

        with pytest.raises(pydantic.ValidationError):
            container.get(SingletonRequiredFieldSettingsForTest)


@Provider(singleton=True, priority=100)
def _custom_event_bus_settings() -> NatsEventBusSettings:
    """App-supplied override — module scope so its lazy annotation resolves."""
    return NatsEventBusSettings(servers="nats://custom:4222")


class TestNatsEventBusSettingsConvertedShapeInvariants:
    """Step 7 — invariants the ``@Singleton`` → ``@Provider`` conversion must not break."""

    def test_app_supplied_settings_win_over_the_default(self) -> None:
        from varco_nats.di import bootstrap

        container = DIContainer()
        container.provide(_custom_event_bus_settings)
        bootstrap(container)

        assert container.get(NatsEventBusSettings).servers == "nats://custom:4222"

    def test_settings_resolve_through_their_base_interface(self) -> None:
        from varco_nats.di import bootstrap

        container = DIContainer()
        bootstrap(container)

        assert isinstance(container.get(EventBusSettings), NatsEventBusSettings)


class TestSettingsBoundOnceAndFailFast:
    """Plan 048 / Step 10 — NatsEventBusSettings goes through bind_settings()."""

    def test_settings_bound_once_after_two_bootstrap_calls(self) -> None:
        from providify import DIContainer
        from varco_nats.config import NatsEventBusSettings
        from varco_nats.di import bootstrap

        c = DIContainer()
        bootstrap(c)
        bootstrap(c)

        assert len(c.get_all_bindings(NatsEventBusSettings)) == 1

    def test_bad_env_fails_bootstrap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from providify import DIContainer
        from varco_core.settings import SettingsValidationError
        from varco_nats.di import bootstrap

        monkeypatch.setenv("VARCO_NATS_MAX_DELIVER", "notanint")

        with pytest.raises(SettingsValidationError) as exc:
            bootstrap(DIContainer())

        assert "VARCO_NATS_MAX_DELIVER" in str(exc.value)
