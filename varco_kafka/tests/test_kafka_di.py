"""
DI wiring tests for ``varco_kafka``.

``KafkaChannelManagerSettings`` is a pydantic ``BaseSettings`` subclass.  It
used to carry ``@Singleton`` on the class itself, which cannot work: providify
injects the constructor, and pydantic's ``BaseSettings.__init__`` signature is
``(**values: Any)``.  Resolution therefore died with
``LookupError: Cannot resolve 'values: typing.Any'``, taking every consumer of
those settings (``KafkaChannelManager``) down with it.

Settings classes must be registered through a ``@Provider`` instead — the same
rule already applied in ``varco_casbin/di.py`` and ``varco_fastapi/di.py``.

No broker is required: only container resolution is exercised.  The channel
manager itself is *not* resolved here — its ``@PostConstruct`` opens an admin
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
from varco_kafka.bus import KafkaEventBus
from varco_kafka.channel import KafkaChannelManager, KafkaChannelManagerSettings
from varco_kafka.config import KafkaEventBusSettings


@Provider(singleton=True, priority=100)
def _custom_settings() -> KafkaChannelManagerSettings:
    """App-supplied override — module scope so its lazy annotation resolves."""
    return KafkaChannelManagerSettings(topic_prefix="custom.")


class TestKafkaChannelManagerSettingsDI:
    def test_regression_settings_resolve_from_a_scanned_container(self) -> None:
        """
        User-visible symptom: ``container.get(ChannelManager, qualifier="kafka")``
        raised ``LookupError: Cannot resolve 'values: typing.Any'``.  Correct
        behaviour is a ready-to-use settings instance, because a pydantic
        ``BaseSettings`` must be built by a factory, never constructor-injected.
        """
        container = DIContainer()
        container.scan("varco_kafka", recursive=True)

        settings = container.get(KafkaChannelManagerSettings)

        assert isinstance(settings, KafkaChannelManagerSettings)
        assert settings.bootstrap_servers

    def test_settings_are_a_singleton(self) -> None:
        container = DIContainer()
        container.scan("varco_kafka", recursive=True)

        assert container.get(KafkaChannelManagerSettings) is container.get(
            KafkaChannelManagerSettings
        )

    def test_user_supplied_settings_win_over_the_default(self) -> None:
        """The default provider registers at the lowest possible priority."""
        container = DIContainer()
        container.scan("varco_kafka", recursive=True)
        container.provide(_custom_settings)

        assert container.get(KafkaChannelManagerSettings).topic_prefix == "custom."

    def test_channel_manager_binding_is_registered(self) -> None:
        """
        The manager is not *resolved* here (its ``@PostConstruct`` connects to
        a broker) — only its binding presence is asserted, so a future refactor
        cannot silently drop it.
        """
        container = DIContainer()
        container.scan("varco_kafka", recursive=True)

        implementations = {getattr(b, "implementation", None) for b in container._bindings}
        assert KafkaChannelManager in implementations
        assert any(
            b.interface in (ChannelManager, KafkaChannelManager) for b in container._bindings
        )


class TestKafkaContainerValidates:
    def test_regression_every_binding_resolves_its_annotations(self) -> None:
        """
        User-visible symptom: bootstrapping an app that scanned ``varco_kafka``
        died with ``AnnotationResolutionError`` on
        ``KafkaEventBus.__init__`` parameter ``serializer``, because
        the ``EventSerializer`` alias was a quoted forward reference bound to a
        ``str`` at runtime, making ``EventSerializer | None`` unevaluatable.

        ``validate_bindings()`` is the single call that resolves annotations for
        *every* registered binding, so it catches an unresolvable constructor
        hint on any singleton in the package — not just the ones a test happens
        to resolve directly.
        """
        container = DIContainer()
        container.scan("varco_kafka", recursive=True)

        container.validate_bindings()
        assert_no_structural_di_issues(container)


class TestKafkaEventBusSettingsCharacterization:
    """
    Plan 014 / Part A (F1) — characterization test pinning **current** behaviour.

    ``audits/001-audit-di-wiring.md:19`` observes that no test anywhere calls
    ``container.get(KafkaEventBusSettings)`` — only ``validate_bindings()``
    exercises the class, and that call resolves annotations without ever
    constructing the class, so it cannot catch a broken constructor-injection
    path. These tests close that gap by actually building the settings (and,
    below, the bus that depends on them) through a real container.
    """

    def test_characterization_settings_resolve_through_the_container(self) -> None:
        container = DIContainer()
        container.scan("varco_kafka", recursive=True)

        settings = container.get(KafkaEventBusSettings)

        assert isinstance(settings, KafkaEventBusSettings)
        assert settings.bootstrap_servers

    def test_characterization_settings_are_a_singleton(self) -> None:
        container = DIContainer()
        container.scan("varco_kafka", recursive=True)

        assert container.get(KafkaEventBusSettings) is container.get(KafkaEventBusSettings)

    async def test_characterization_event_bus_resolves_with_injected_settings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        The load-bearing test. ``KafkaEventBus.__init__`` declares
        ``config: Inject[KafkaEventBusSettings]`` with no default
        (``varco_kafka/varco_kafka/bus.py``), so if the settings binding
        cannot be constructed, the documented bootstrap call
        (``bus = await container.aget(AbstractEventBus)``) is broken too.

        ``KafkaEventBus.start`` is an ``async`` ``@PostConstruct`` that opens a
        real producer connection, so it is stubbed to a no-op here — this test
        proves DI construction/injection, not broker connectivity (no Docker
        broker is required). Resolving synchronously via ``container.get()``
        would raise before ever reaching the network (providify refuses to run
        an async ``@PostConstruct`` synchronously), so the documented
        ``await container.aget(...)`` bootstrap path is used instead.
        """

        async def _noop_start(self: KafkaEventBus) -> None:
            return None

        monkeypatch.setattr(KafkaEventBus, "start", _noop_start)

        container = DIContainer()
        container.scan("varco_kafka", recursive=True)

        bus = await container.aget(AbstractEventBus, qualifier="kafka")

        assert isinstance(bus, KafkaEventBus)
        assert bus._config is container.get(KafkaEventBusSettings)


class _RequiredFieldSettings(EventBusSettings):
    """Module-scope settings subclass with one non-defaulted field — Step 5."""

    required_value: str


@Singleton
class SingletonRequiredFieldSettingsForTest(_RequiredFieldSettings):
    pass


class TestKafkaRequiredFieldCharacterization:
    def test_characterization_required_field_raises_validation_error_not_lookup_error(
        self,
    ) -> None:
        """
        Corrects ``audits/001-audit-di-wiring.md:19``'s prediction that adding a
        *required* field to a ``@Singleton``-decorated ``BaseSettings`` subclass
        would reproduce ``LookupError: Cannot resolve 'values: typing.Any'``.

        It does not: a pydantic field is not a constructor parameter — pydantic
        collects it through ``**values``, which providify's per-parameter
        resolver skips outright (``providify/_annotations.py:583-592``). The
        real failure is a pydantic ``ValidationError`` at construction, raised
        identically whether the class carries ``@Singleton`` or ``@Provider``.
        """
        container = DIContainer()
        container.scan(sys.modules[__name__])

        with pytest.raises(pydantic.ValidationError):
            container.get(SingletonRequiredFieldSettingsForTest)


@Provider(singleton=True, priority=100)
def _custom_event_bus_settings() -> KafkaEventBusSettings:
    """App-supplied override — module scope so its lazy annotation resolves."""
    return KafkaEventBusSettings(bootstrap_servers="custom:9092")


class TestKafkaEventBusSettingsConvertedShapeInvariants:
    """
    Step 7 — invariants the ``@Singleton`` → ``@Provider`` conversion (Step 8)
    must not break. These pass under ``@Singleton`` too (that is intended):
    they must stay green before *and* after the conversion.
    """

    def test_app_supplied_settings_win_over_the_default(self) -> None:
        container = DIContainer()
        container.scan("varco_kafka", recursive=True)
        container.provide(_custom_event_bus_settings)

        assert container.get(KafkaEventBusSettings).bootstrap_servers == "custom:9092"

    def test_settings_resolve_through_their_base_interface(self) -> None:
        container = DIContainer()
        container.scan("varco_kafka", recursive=True)

        assert isinstance(container.get(EventBusSettings), KafkaEventBusSettings)
