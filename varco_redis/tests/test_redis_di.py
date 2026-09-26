"""
DI wiring tests for ``varco_redis``.

Why this file exists
--------------------
``varco_redis`` had a fully green test suite while its container was, in fact,
unbootstrappable: ``RedisEventBus.__init__``'s ``serializer`` parameter was
annotated ``Annotated[EventSerializer | None, InjectMeta(optional=True)]``, and
the ``EventSerializer`` alias was a quoted forward reference bound to a ``str``
at runtime — so the annotation evaluated ``str | None`` and raised.  No existing redis test
happened to hit a resolution path that resolves binding annotations, so the
defect was invisible here and only surfaced in ``varco_kafka``/``varco_nats``.

``validate_bindings()`` resolves annotations for *every* registered binding, so
one call covers all present and future singletons in the package.  This is the
coverage gap closer, not a re-test of the alias itself (that lives in
``varco_core/tests/test_event_serializer_alias.py``).

No Redis server is required: only container registration and annotation
resolution are exercised — nothing is instantiated.
"""

from __future__ import annotations

import sys

import pydantic
import pytest
from providify import DIContainer, Provider, Singleton
from varco_conformance.providify_health import assert_no_structural_di_issues
from varco_core.event import AbstractEventBus
from varco_core.event.config import EventBusSettings
from varco_redis.bus import RedisEventBus
from varco_redis.config import RedisEventBusSettings
from varco_redis.di import bootstrap


class TestRedisContainerValidates:
    def test_regression_every_binding_resolves_its_annotations(self) -> None:
        """
        User-visible symptom: an app calling ``varco_redis.di.bootstrap()`` and
        then resolving anything died at startup with
        ``AnnotationResolutionError: Cannot resolve type hints for
        'RedisEventBus.__init__' parameter 'serializer'``.

        Correct behaviour is a container that validates cleanly.
        """
        container = DIContainer()
        container.scan("varco_redis", recursive=True)

        container.validate_bindings()
        assert_no_structural_di_issues(container)

    def test_regression_streams_bus_bindings_also_validate(self) -> None:
        """
        The streams-backed bus is selected by ``VARCO_REDIS_USE_STREAMS`` at
        resolution time, but its binding is registered by the same scan — so it
        must resolve its annotations regardless of the env var.
        """
        container = DIContainer()
        container.scan("varco_redis", recursive=True)

        implementations = {
            getattr(b, "implementation", None).__name__
            for b in container._bindings
            if getattr(b, "implementation", None) is not None
        }

        assert "RedisEventBus" in implementations
        container.validate_bindings()
        assert_no_structural_di_issues(container)

    def test_regression_cache_backplane_bindings_also_validate(self) -> None:
        """
        Plan 010 (cache hardening) added ``RedisBackplaneSettings``
        (``@Provider``) and ``RedisPubSubBackplane`` (``@Singleton``) —
        both must be discovered by the same ``scan()`` and resolve their
        annotations cleanly, same regression class as the two tests above.
        """
        container = DIContainer()
        container.scan("varco_redis", recursive=True)

        implementations = {
            getattr(b, "implementation", None).__name__
            for b in container._bindings
            if getattr(b, "implementation", None) is not None
        }

        assert "RedisPubSubBackplane" in implementations
        container.validate_bindings()
        assert_no_structural_di_issues(container)


class TestRedisEventBusSettingsCharacterization:
    """
    Plan 014 / Part A (F1) — characterization test pinning **current** behaviour.

    See ``varco_kafka/tests/test_kafka_di.py``'s sibling class for the full
    rationale; this mirrors it exactly for ``RedisEventBusSettings``.

    **Plan 048 / DI-6, Step 8 note:** these now call ``bootstrap()`` rather
    than a bare ``container.scan("varco_redis", recursive=True)`` —
    ``RedisEventBusSettings`` is registered by ``bootstrap()``'s
    ``bind_settings()`` call, not by a module-level scanned provider
    anymore (§Migration: "``container.scan("varco_x")`` alone no longer
    registers settings — call ``varco_x.bootstrap(container)``").
    """

    def test_characterization_settings_resolve_through_the_container(self) -> None:
        container = DIContainer()
        bootstrap(container)

        settings = container.get(RedisEventBusSettings)

        assert isinstance(settings, RedisEventBusSettings)
        assert settings.url

    def test_characterization_settings_are_a_singleton(self) -> None:
        container = DIContainer()
        bootstrap(container)

        assert container.get(RedisEventBusSettings) is container.get(RedisEventBusSettings)

    async def test_characterization_event_bus_resolves_with_injected_settings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        Load-bearing test — see the kafka sibling for why ``start()`` is
        stubbed to a no-op and ``await container.aget(...)`` is used instead
        of the synchronous ``container.get()``.
        """

        async def _noop_start(self: RedisEventBus) -> None:
            return None

        monkeypatch.setattr(RedisEventBus, "start", _noop_start)

        container = DIContainer()
        bootstrap(container)

        bus = await container.aget(AbstractEventBus, qualifier="redis")

        assert isinstance(bus, RedisEventBus)
        assert bus._config is container.get(RedisEventBusSettings)


class RequiredFieldSettingsForTest(EventBusSettings):
    """Module-scope settings subclass with one non-defaulted field — Step 5."""

    required_value: str


@Singleton
class SingletonRequiredFieldSettingsForTest(RequiredFieldSettingsForTest):
    pass


class TestRedisRequiredFieldCharacterization:
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
def _custom_event_bus_settings() -> RedisEventBusSettings:
    """App-supplied override — module scope so its lazy annotation resolves."""
    return RedisEventBusSettings(url="redis://custom:6379/0")


class TestRedisEventBusSettingsConvertedShapeInvariants:
    """Step 7 — invariants the ``@Singleton`` → ``@Provider`` conversion must not break."""

    def test_app_supplied_settings_win_over_the_default(self) -> None:
        container = DIContainer()
        container.provide(_custom_event_bus_settings)
        bootstrap(container)

        assert container.get(RedisEventBusSettings).url == "redis://custom:6379/0"

    def test_settings_resolve_through_their_base_interface(self) -> None:
        container = DIContainer()
        bootstrap(container)

        assert isinstance(container.get(EventBusSettings), RedisEventBusSettings)


class TestSettingsBoundOnceAndFailFast:
    """Plan 048 / Step 8 — RedisEventBusSettings goes through bind_settings()."""

    def test_settings_bound_once_after_two_bootstrap_calls(self) -> None:
        from providify import DIContainer
        from varco_redis.config import RedisEventBusSettings
        from varco_redis.di import bootstrap

        c = DIContainer()
        bootstrap(c)
        bootstrap(c)

        assert len(c.get_all_bindings(RedisEventBusSettings)) == 1

    def test_bad_env_fails_bootstrap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from providify import DIContainer
        from varco_core.settings import SettingsValidationError
        from varco_redis.di import bootstrap

        monkeypatch.setenv("VARCO_REDIS_SOCKET_TIMEOUT", "x")

        with pytest.raises(SettingsValidationError) as exc:
            bootstrap(DIContainer())

        assert "VARCO_REDIS_SOCKET_TIMEOUT" in str(exc.value)
