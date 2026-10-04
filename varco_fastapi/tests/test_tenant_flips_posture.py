"""
Plan 055 / FLIP-6, FLIP-7 — production refusal under 054's derived
``enforce`` for the implicit legacy fallback, the explicit legacy escape
hatch (until acknowledged), and the ``on_missing_claim=allow`` opt-out.

Opts out of the package-wide ``_posture_warn_only`` fixture (054
§D-flip2-tests). Assertions look only at ``tenant.*`` ids in the refusal
message: unrelated default findings (e.g. ``posture.authorizer_unbound``)
may also block a bare test app, and are not what these tests are about.

Red until Plan 055: ``TenancyModule`` has no ``catalog=``, requires ``pool=``
and contributes no tenant collector (so no ``tenant.*`` id ever reaches the
report through it).
"""

from __future__ import annotations

import re

import pytest
from providify import DIContainer


class _FakeCatalog:
    async def get(self, tenant_id: str):
        from varco_core.tenancy.catalog import TenantDescriptor

        return TenantDescriptor(tenant_id=tenant_id, status="active")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "VARCO_SECURITY_ENFORCE",
        "VARCO_SECURITY_ENV",
        "VARCO_SECURITY_SUPPRESS",
        "VARCO_TENANT_SOURCES",
        "VARCO_TENANT_MEMBERSHIP_ON_MISSING",
    ):
        monkeypatch.delenv(name, raising=False)


async def _blocking_tenant_ids(container: DIContainer, tenancy_module) -> set[str]:
    """Start the app; return the tenant.* ids named by a posture refusal (empty if none)."""
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.modules.defaults import default_modules

    app = create_varco_app(
        container, modules=[*default_modules(), tenancy_module], routers=[], validate=False
    )
    try:
        async with app.router.lifespan_context(app):
            return set()
    except RuntimeError as exc:
        return set(re.findall(r"tenant\.[a-z_]+", str(exc)))


async def test_implicit_legacy_refuses_production_start() -> None:
    import warnings

    from varco_fastapi.modules import TenancyModule

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)  # the implicit path warns
        ids = await _blocking_tenant_ids(DIContainer(), TenancyModule())

    assert {"tenant.no_chain", "tenant.legacy_source_implicit"} <= ids


async def test_explicit_legacy_refuses_until_suppressed(monkeypatch: pytest.MonkeyPatch) -> None:
    # The opt-out and its acknowledgement: two greppable statements.
    from varco_fastapi.modules import TenancyModule

    monkeypatch.setenv("VARCO_TENANT_SOURCES", "legacy")
    refused = await _blocking_tenant_ids(DIContainer(), TenancyModule())
    assert "tenant.legacy_source_explicit" in refused

    monkeypatch.setenv(
        "VARCO_SECURITY_SUPPRESS", "tenant.legacy_source_explicit,tenant.no_membership_provider"
    )
    assert await _blocking_tenant_ids(DIContainer(), TenancyModule()) == set()


async def test_membership_allow_optout_refuses_under_refuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # HIGH is never demoted by development — the opt-out needs its suppression.
    # 054's derivation makes development `warn`, so "every environment" means
    # "under refuse in every environment" (security-posture.md's severity
    # table, column 4) — the refuse mode is stated explicitly here.
    from varco_core.tenancy.di import enable_tenant_membership
    from varco_fastapi.modules import TenancyModule

    monkeypatch.setenv("VARCO_SECURITY_ENV", "development")
    monkeypatch.setenv("VARCO_SECURITY_ENFORCE", "refuse")
    monkeypatch.setenv("VARCO_TENANT_SOURCES", "jwt")
    monkeypatch.setenv("VARCO_TENANT_MEMBERSHIP_ON_MISSING", "allow")
    container = DIContainer()
    enable_tenant_membership(container)

    ids = await _blocking_tenant_ids(container, TenancyModule(catalog=_FakeCatalog()))

    assert "tenant.membership_missing_claim_allows" in ids


async def test_jwt_chain_with_membership_starts_clean_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The recommended 4.0 shape: no tenant finding blocks (INFO only).
    from varco_core.tenancy.di import enable_tenant_membership
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.modules import SecurityPostureModule, TenancyModule
    from varco_fastapi.modules.defaults import default_modules
    from varco_fastapi.posture import PostureSeverity

    monkeypatch.setenv("VARCO_TENANT_SOURCES", "jwt")
    container = DIContainer()
    enable_tenant_membership(container)
    module = TenancyModule(catalog=_FakeCatalog())

    assert await _blocking_tenant_ids(container, module) == set()

    # And through a warn-only run, the tenant findings that exist are INFO.
    monkeypatch.setenv("VARCO_SECURITY_ENFORCE", "warn")
    container = DIContainer()
    enable_tenant_membership(container)
    posture = SecurityPostureModule()
    app = create_varco_app(
        container,
        modules=[
            *default_modules(exclude=(SecurityPostureModule,)),
            posture,
            TenancyModule(catalog=_FakeCatalog()),
        ],
        routers=[],
        validate=False,
    )
    async with app.router.lifespan_context(app):
        tenant = [f for f in posture.report.findings if f.check.startswith("tenant.")]

    assert tenant, "TenancyModule must contribute the tenant collector"
    assert all(f.severity is PostureSeverity.INFO for f in tenant), tenant
