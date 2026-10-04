"""
Regression tests for ``varco_core.observability.di`` — the *DI resolution* path.

Why this file exists
--------------------
``TestOtelConfiguration`` in ``test_observability.py`` builds ``TracerProvider``
/ ``MeterProvider`` by calling ``_build_resource()`` directly.  It never puts
``OtelConfiguration`` into a ``DIContainer``, so nothing in the suite covered
the one thing every application actually does::

    container.install(OtelConfiguration)
    container.get(TracerProvider)          # ← never exercised before

A user reported that once a *second* ``@Configuration`` module shares the
container with ``OtelConfiguration``, resolving ``TracerProvider`` fails with
``TypeError: tracer_provider() missing 1 required positional argument:
'config'`` — i.e. ``config: Inject[OtelConfig]`` is silently not injected —
and worked around it by calling the provider methods by hand.

The tests below pin the DI contract that makes that workaround unnecessary,
plus the *documented* bootstrap API (which did not match providify's real
signatures — see the docstring of ``varco_core.observability.di``).

Global-state hygiene: ``OtelConfiguration`` calls
``trace.set_tracer_provider()`` / ``metrics.set_meter_provider()`` — process
wide, one-shot.  OTel logs "Overriding of current TracerProvider is not
allowed" and keeps the first one; these tests therefore assert on the object
returned by the container, never on the OTel global.
"""

from __future__ import annotations

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider
from providify import Configuration, DIContainer, Provider
from varco_core.observability.config import OtelConfig
from varco_core.observability.di import OtelConfiguration

# ── Fixtures / helpers ────────────────────────────────────────────────────────


@pytest.fixture
def app_config_provider():
    """A ``@Provider``-decorated ``OtelConfig`` factory (the documented override)."""

    @Provider(singleton=True)
    def otel_config() -> OtelConfig:
        return OtelConfig(service_name="orders-svc", service_version="9.9.9")

    return otel_config


@Configuration
class _SiblingModule:
    """
    A second, unrelated ``@Configuration`` module sharing the container.

    Deliberately declared inside ``varco_core``'s test suite rather than
    importing ``varco_fastapi.di.VarcoFastAPIConfiguration``: the reported failure is
    generic to *any* pair of ``@Configuration`` modules, and ``varco_core``
    must not gain a dependency on ``varco_fastapi``.
    """

    @Provider(singleton=True)
    def sibling_setting(self) -> _SiblingSetting:
        return _SiblingSetting(name="sibling")


class _SiblingSetting:
    def __init__(self, name: str) -> None:
        self.name = name


# ── The reported bug ──────────────────────────────────────────────────────────


class TestRegressionOtelDependencyInjection:
    """
    User reports: with ``VarcoFastAPIConfiguration`` and ``OtelConfiguration`` in the
    same container, ``container.get(TracerProvider)`` raises ``TypeError:
    tracer_provider() missing 1 required positional argument: 'config'``.

    Correct behaviour: ``Inject[OtelConfig]`` resolves and the provider returns
    a ``TracerProvider`` built from that config — because that is the only
    contract that lets an application configure OTel through DI instead of
    calling ``OtelConfiguration().tracer_provider(cfg)`` by hand.
    """

    def test_regression_tracer_provider_resolves_with_injected_config(
        self, app_config_provider
    ) -> None:
        container = DIContainer()
        container.provide(app_config_provider)
        container.install(OtelConfiguration)

        provider = container.get(TracerProvider)

        assert isinstance(provider, TracerProvider)
        # Proves `config` was really injected (not defaulted, not skipped).
        assert provider.resource.attributes["service.name"] == "orders-svc"

    def test_regression_tracer_provider_resolves_with_sibling_configuration(
        self, app_config_provider
    ) -> None:
        """The exact reported trigger: two ``@Configuration`` modules, one container."""
        container = DIContainer()
        container.provide(app_config_provider)
        container.install(_SiblingModule)
        container.install(OtelConfiguration)

        provider = container.get(TracerProvider)

        assert isinstance(provider, TracerProvider)
        assert provider.resource.attributes["service.name"] == "orders-svc"
        # The sibling module still works too — no ordering casualty.
        assert container.get(_SiblingSetting).name == "sibling"

    def test_regression_tracer_provider_resolves_with_reversed_install_order(
        self, app_config_provider
    ) -> None:
        """Install order of the two modules must not decide whether DI works."""
        container = DIContainer()
        container.provide(app_config_provider)
        container.install(OtelConfiguration)
        container.install(_SiblingModule)

        assert isinstance(container.get(TracerProvider), TracerProvider)

    def test_regression_meter_provider_resolves_with_injected_config(
        self, app_config_provider
    ) -> None:
        container = DIContainer()
        container.provide(app_config_provider)
        container.install(_SiblingModule)
        container.install(OtelConfiguration)

        provider = container.get(MeterProvider)

        assert isinstance(provider, MeterProvider)

    def test_regression_global_attributes_provider_resolves(self, app_config_provider) -> None:
        """``observability_attributes`` also takes ``Inject[OtelConfig]``."""
        from varco_core.observability.attributes import GlobalAttributes

        container = DIContainer()
        container.provide(app_config_provider)
        container.install(OtelConfiguration)

        assert isinstance(container.get(GlobalAttributes), GlobalAttributes)


# ── The documented bootstrap API ──────────────────────────────────────────────


class TestDocumentedBootstrapApi:
    """
    Pins the API shapes the module docstring shows.  Both "documented" forms
    that shipped before this fix raised immediately, which is what pushed the
    reporter onto the by-hand workaround in the first place.
    """

    def test_default_config_is_used_when_nothing_is_provided(self) -> None:
        container = DIContainer()
        container.install(OtelConfiguration)

        assert container.get(OtelConfig).service_name == "varco"

    def test_provide_before_install_overrides_the_default(self, app_config_provider) -> None:
        container = DIContainer()
        container.provide(app_config_provider)
        container.install(OtelConfiguration)

        assert container.get(OtelConfig).service_name == "orders-svc"

    def test_provide_after_install_wins_in_any_order(self) -> None:
        """
        Plan 045: ``OtelConfiguration.otel_config`` is declared ``@Fallback``
        — a plain, non-fallback ``OtelConfig`` provider registered AFTER
        ``install()`` still wins unconditionally, with no need to raise its
        priority. This replaces the pre-045 "equal-priority ties resolve to
        the first registered" behaviour this test used to document.
        """

        @Provider(singleton=True)
        def late_default() -> OtelConfig:
            return OtelConfig(service_name="late")

        container = DIContainer()
        container.install(OtelConfiguration)
        container.provide(late_default)
        assert container.get(OtelConfig).service_name == "late"

    def test_install_does_not_take_a_config_keyword(self) -> None:
        """
        ``container.install(OtelConfiguration, config=...)`` was documented for
        two releases but ``DIContainer.install()`` only accepts the module
        class.  Pinned so the docstring cannot drift back to it.
        """
        container = DIContainer()
        with pytest.raises(TypeError):
            container.install(OtelConfiguration, config=OtelConfig(service_name="x"))  # type: ignore[call-arg]

    def test_provide_requires_a_provider_decorated_callable(self) -> None:
        """
        ``container.provide(lambda: OtelConfig(...))`` was documented but
        providify rejects undecorated callables.  Pinned for the same reason.
        """
        container = DIContainer()
        with pytest.raises(Exception, match="Provider"):
            container.provide(lambda: OtelConfig(service_name="x"))


# ── Root-cause guard: a poisoned binding must not disable ALL injection ───────


class TestBindingInterfaceHealth:
    """
    Root cause of the report: providify registers a binding whose *interface*
    is a plain ``str`` when a ``@Provider``'s return annotation cannot be
    resolved at registration time (under PEP 563 a quoted annotation
    ``-> "Foo"`` round-trips to the string ``"'Foo'"``, and providify's
    fallback ``eval`` yields ``'Foo'`` — a ``str``, not a type).

    ``DIContainer._build_localns()`` then raises ``AttributeError: 'str' object
    has no attribute '__name__'``, and ``_collect_kwargs_sync()`` swallows that
    with ``except Exception: hints = {}`` — so **every** provider in the
    container is subsequently called with zero arguments.  One bad provider
    anywhere silently disables injection everywhere.

    varco cannot fix providify from here, but it must never *ship* such a
    binding.  These tests fail loudly if any varco module starts to.
    """

    def test_varco_core_bindings_all_expose_a_usable_interface(self) -> None:
        container = DIContainer()
        container.scan("varco_core", recursive=True)
        container.install(OtelConfiguration)

        for binding in container._bindings:
            assert hasattr(binding.interface, "__name__") or hasattr(
                binding.interface, "__origin__"
            ), f"{binding!r} registered an unusable interface {binding.interface!r}"

    def test_varco_core_never_registers_a_str_interface(self) -> None:
        container = DIContainer()
        container.scan("varco_core", recursive=True)
        container.install(OtelConfiguration)

        offenders = [b for b in container._bindings if isinstance(b.interface, str)]
        assert not offenders, (
            "Bindings with a str interface poison DIContainer._build_localns() "
            f"and silently disable injection container-wide: {offenders!r}"
        )

    def test_localns_build_never_raises_for_varco_core(self) -> None:
        """
        The single choke point: if this raises, every ``Inject[...]`` parameter
        of every provider/constructor in the container is silently dropped.
        """
        container = DIContainer()
        container.scan("varco_core", recursive=True)
        container.install(OtelConfiguration)

        container._build_localns()  # must not raise
