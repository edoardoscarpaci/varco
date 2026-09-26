"""
Red-mode tests for Plan 011, guard test #9 — DI binding health for the new
i18n/timezone bindings.

Plan line (step 36 / Testing §Guard tests #9): "MessageCatalog,
I18nSettings, TimezoneSettings, TenantDefaultsProvider all resolve;
catches the quoted-@Provider-return-annotation pitfall, which poisons
EVERY binding in the container, not just the offending one."
"""

from __future__ import annotations

from providify import DIContainer
from varco_core.context.defaults import TenantDefaultsProvider
from varco_core.i18n.catalog import MessageCatalog
from varco_core.i18n.settings import I18nSettings
from varco_core.tz.settings import TimezoneSettings


def _build_container() -> DIContainer:
    # Plan 045: setup_varco_defaults() is gone and container.scan("varco_fastapi",
    # recursive=True) already auto-installs VarcoFastAPIModule (its
    # @Configuration providers, including TaskSerializer, are all @Fallback
    # and self-registering) — the redundant explicit install() is dropped.
    #
    # Plan 048 / DI-6: I18nSettings/TimezoneSettings moved off a scanned
    # @Fallback @Provider onto bind_core_settings() (§D-core-list), called
    # from varco_fastapi.di.bootstrap() — a bare scan("varco_fastapi") no
    # longer registers them (§Migration), so this helper calls bootstrap()
    # instead of scanning both packages by hand.
    from varco_fastapi.di import bootstrap

    container = DIContainer()
    bootstrap(container)
    return container


def test_message_catalog_resolves_to_null_by_default() -> None:
    container = _build_container()
    catalog = container.get(MessageCatalog)
    assert catalog is not None


def test_i18n_settings_resolves() -> None:
    container = _build_container()
    settings = container.get(I18nSettings)
    assert isinstance(settings, I18nSettings)


def test_timezone_settings_resolves() -> None:
    container = _build_container()
    settings = container.get(TimezoneSettings)
    assert isinstance(settings, TimezoneSettings)


def test_tenant_defaults_provider_resolves_to_null_by_default() -> None:
    container = _build_container()
    provider = container.get(TenantDefaultsProvider)
    assert provider is not None


def test_localns_build_never_raises_with_i18n_tz_bindings_present() -> None:
    # The quoted-@Provider-annotation pitfall poisons every binding in the
    # container, not just the offending one — so this must be asserted
    # against the FULL container, same discipline as the existing
    # TestContainerLocalnsHealth in test_di_binding_health.py.
    container = _build_container()
    container._build_localns()  # must not raise

    poisoned = [b for b in container._bindings if isinstance(b.interface, str)]
    assert not poisoned, f"str-interface bindings registered: {poisoned!r}"
