"""
Plan 045 / Step 11 — the varco_fastapi default-binding rule (§D-rule).

Replaces `test_di_priority_direction.py` (removed by this same step) — that
module's `len(all_instances) == 2` and "the framework default sorts first"
assertions were pinning the exact order-dependent behaviour this plan
removes. Under `@Fallback`:

- A plain app `@Provider` registered AFTER `container.scan("varco_fastapi")`
  wins resolution outright (unchanged conclusion, new mechanism).
- `container.get_all(T)` returns ONLY the app's binding — the framework
  default is excluded once it is shadowed
  (`providify/container.py:1683-1686`).
- `container.validate()` reports one `FALLBACK_SHADOWED` INFO per shadowed
  provider default, naming the app's provider.
- `varco_fastapi.di.bootstrap()` no longer accepts `setup_defaults=` at all
  (the kwarg — and `setup_varco_defaults()` itself — are removed, §D-fastapi).

These are passing contract pins, proven against `VarcoFastAPIModule`'s
providers (declared `@Fallback` by Step 8) and `bootstrap()`'s removed
`setup_defaults=` kwarg.
"""

from __future__ import annotations

import pytest
from providify import DIContainer, IssueKind, Provider
from varco_fastapi.auth.trust_store import TrustStore
from varco_fastapi.client.base import ClientProfile
from varco_fastapi.di import bootstrap
from varco_fastapi.middleware.cors import CORSConfig


def _app_trust_store_provider():
    @Provider(singleton=True)
    def app_trust_store() -> TrustStore:
        return TrustStore(include_system_cas=False)

    return app_trust_store, lambda ts: ts.include_system_cas is False


def _app_cors_config_provider():
    @Provider(singleton=True)
    def app_cors_config() -> CORSConfig:
        return CORSConfig(allow_origins=("https://app.example.com",))

    return app_cors_config, lambda cc: cc.allow_origins == ("https://app.example.com",)


def _app_client_profile_provider():
    @Provider(singleton=True)
    def app_client_profile() -> ClientProfile:
        return ClientProfile.development(timeout=1.0)

    return app_client_profile, lambda cp: cp.timeout == 1.0


FRAMEWORK_DEFAULT_FAMILIES = [
    pytest.param(TrustStore, _app_trust_store_provider, id="TrustStore"),
    pytest.param(CORSConfig, _app_cors_config_provider, id="CORSConfig"),
    pytest.param(ClientProfile, _app_client_profile_provider, id="ClientProfile"),
]


@pytest.mark.parametrize("interface, make_app_provider", FRAMEWORK_DEFAULT_FAMILIES)
def test_app_binding_registered_after_scan_wins_over_framework_default(
    interface: type, make_app_provider
) -> None:
    app_provider_fn, is_app_instance = make_app_provider()

    container = DIContainer()
    bootstrap(container)
    container.provide(app_provider_fn)

    resolved = container.get(interface)

    assert is_app_instance(resolved)


@pytest.mark.parametrize("interface, make_app_provider", FRAMEWORK_DEFAULT_FAMILIES)
def test_get_all_returns_only_the_app_binding_once_shadowed(
    interface: type, make_app_provider
) -> None:
    app_provider_fn, is_app_instance = make_app_provider()

    container = DIContainer()
    bootstrap(container)
    container.provide(app_provider_fn)

    results = container.get_all(interface)

    assert len(results) == 1
    assert is_app_instance(results[0])


@pytest.mark.parametrize("interface, make_app_provider", FRAMEWORK_DEFAULT_FAMILIES)
def test_validate_reports_shadowed_framework_default(interface: type, make_app_provider) -> None:
    app_provider_fn, _ = make_app_provider()

    container = DIContainer()
    bootstrap(container)
    container.provide(app_provider_fn)

    report = container.validate(raise_on_error=False)
    shadow_issues = [i for i in report.issues if i.kind == IssueKind.FALLBACK_SHADOWED]

    matching = [i for i in shadow_issues if app_provider_fn.__name__ in str(i)]
    assert len(matching) == 1


def test_bootstrap_rejects_setup_defaults_kwarg() -> None:
    # setup_varco_defaults() and bootstrap(setup_defaults=) are removed
    # entirely (§D-fastapi) — the kwarg must no longer exist at all.
    with pytest.raises(TypeError):
        bootstrap(setup_defaults=True)  # type: ignore[call-arg]
