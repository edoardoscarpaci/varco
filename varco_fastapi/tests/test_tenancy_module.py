"""
Plan 055 / FLIP-6 (§D-flip6-module, §D-flip6-slot) — red-mode tests for
``TenancyModule`` as the composition root for tenant resolution.

Before Plan 055 ``TenancyModule(*, pool, ...)`` only wraps
``TenancyLifecycle``: it has no ``middleware()``/``configure_app()``, no
``catalog=``/``chain=``/``server_auth=``/``membership=`` keywords, no
``validate()`` refusal, and ``EdgeSlot.TENANT_RESOLUTION`` does not exist.
Expected red reasons: ``TypeError`` (unexpected keyword), ``AttributeError``
(``middleware``/``TENANT_RESOLUTION``), or an assertion on missing behaviour.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_fastapi.modules import TenancyModule


class _FakeCatalog:
    async def get(self, tenant_id: str):
        from varco_core.tenancy.catalog import TenantDescriptor

        return TenantDescriptor(tenant_id=tenant_id, status="active")


class _CountingPool:
    def __init__(self) -> None:
        self.ensure_calls = 0
        self.sweeper_started = False

    async def ensure(self, tenant_id: str):
        self.ensure_calls += 1
        return object()

    async def start_sweeper(self) -> None:
        self.sweeper_started = True

    async def aclose(self) -> None:
        pass


class _FakeServerAuth:
    async def __call__(self, request):
        from varco_core.auth.base import AuthContext

        return AuthContext()


@pytest.fixture(autouse=True)
def _no_tenant_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Every test states its own source configuration; never inherit the shell's.
    monkeypatch.delenv("VARCO_TENANT_SOURCES", raising=False)


def _bind_sources(container: DIContainer, *names: str) -> None:
    from varco_core.settings import bind_settings
    from varco_core.tenancy.settings import TenantProvenanceSettings

    bind_settings(
        container, TenantProvenanceSettings, instance=TenantProvenanceSettings(sources=names)
    )


def _only_spec(module: TenancyModule, container: DIContainer):
    specs = list(module.middleware(container))
    assert len(specs) == 1
    return specs[0]


async def test_module_contributes_middleware_at_tenant_resolution_slot() -> None:
    from varco_fastapi.middleware.slots import EdgeSlot
    from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware

    container = DIContainer()
    module = TenancyModule(catalog=_FakeCatalog(), pool=_CountingPool())

    spec = _only_spec(module, container)

    assert spec.slot is EdgeSlot.TENANT_RESOLUTION
    assert spec.target is TenantResolutionMiddleware


async def test_module_resolves_chain_from_container() -> None:
    from varco_core.tenancy.sources import JwtClaimTenantSource

    container = DIContainer()
    _bind_sources(container, "jwt")
    module = TenancyModule(catalog=_FakeCatalog(), pool=_CountingPool())

    chain = _only_spec(module, container).kwargs["chain"]

    assert chain is not None
    assert [type(s) for s in chain.sources] == [JwtClaimTenantSource]


async def test_module_passes_multibound_pool_source() -> None:
    # The property the hand-wired path structurally cannot have: plugin sources.
    from varco_plugin_example import PartnerHeaderTenantSource

    container = DIContainer()
    container.scan("varco_plugin_example")
    _bind_sources(container, "partner")
    module = TenancyModule(catalog=_FakeCatalog(), pool=_CountingPool())

    chain = _only_spec(module, container).kwargs["chain"]

    assert any(isinstance(s, PartnerHeaderTenantSource) for s in chain.sources)


async def test_explicit_chain_kwarg_wins() -> None:
    # Explicit code beats env/settings (048 precedence); never merged.
    from varco_core.tenancy.source import TenantSourceChain
    from varco_core.tenancy.sources import LegacyTenantSource

    explicit = TenantSourceChain(sources=(LegacyTenantSource(),))
    container = DIContainer()
    _bind_sources(container, "jwt")
    module = TenancyModule(catalog=_FakeCatalog(), pool=_CountingPool(), chain=explicit)

    assert _only_spec(module, container).kwargs["chain"] is explicit


async def test_module_resolves_server_auth_and_membership_when_bound() -> None:
    from varco_core.di import bind_instance
    from varco_core.tenancy.di import enable_tenant_membership
    from varco_core.tenancy.membership import ClaimTenantMembership
    from varco_fastapi.auth.server_auth import AbstractServerAuth

    container = DIContainer()
    auth = _FakeServerAuth()
    bind_instance(container, AbstractServerAuth, auth)
    enable_tenant_membership(container)
    module = TenancyModule(catalog=_FakeCatalog(), pool=_CountingPool())

    kwargs = _only_spec(module, container).kwargs

    assert kwargs["server_auth"] is auth
    assert isinstance(kwargs["membership"], ClaimTenantMembership)


async def test_module_without_pool_skips_lifecycle() -> None:
    # SHARED app, no control plane: no TenancyLifecycle, start/stop are no-ops.
    container = DIContainer()
    module = TenancyModule(pool=None)

    await module.start(container)
    await module.stop(container)


async def test_module_with_pool_still_runs_lifecycle() -> None:
    # Control for the above: a pool keeps 049's TenancyLifecycle behaviour.
    container = DIContainer()
    pool = _CountingPool()
    module = TenancyModule(pool=pool)

    await module.start(container)
    await module.stop(container)

    assert pool.sweeper_started is True


async def test_validate_refuses_pool_none_under_schema_isolation() -> None:
    from varco_core.settings import bind_settings
    from varco_core.tenancy import TenancySettings, TenantIsolation

    schema = DIContainer()
    bind_settings(
        schema, TenancySettings, instance=TenancySettings(isolation=TenantIsolation.SCHEMA)
    )
    shared = DIContainer()
    bind_settings(
        shared, TenancySettings, instance=TenancySettings(isolation=TenantIsolation.SHARED)
    )

    problems = list(TenancyModule(pool=None).validate(schema))

    assert len(problems) == 1
    assert "pool=" in problems[0]
    assert tuple(TenancyModule(pool=None).validate(shared)) == ()


async def test_full_app_order_invariant() -> None:
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.middleware.error import ErrorMiddleware
    from varco_fastapi.middleware.request_context import RequestContextMiddleware
    from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware
    from varco_fastapi.modules.defaults import default_modules

    container = DIContainer()
    app = create_varco_app(
        container,
        modules=[*default_modules(), TenancyModule(catalog=_FakeCatalog(), pool=_CountingPool())],
        validate=False,
    )

    # app.user_middleware is outermost-first.
    classes = [m.cls for m in app.user_middleware]
    assert TenantResolutionMiddleware in classes
    tenant_idx = classes.index(TenantResolutionMiddleware)
    assert classes.index(ErrorMiddleware) < tenant_idx
    assert tenant_idx < classes.index(RequestContextMiddleware)

    from varco_fastapi.middleware.localization import LocalizationMiddleware

    if LocalizationMiddleware in classes:
        assert tenant_idx < classes.index(LocalizationMiddleware)
    from varco_fastapi.middleware.rate_limit import RateLimitMiddleware

    for idx, entry in enumerate(app.user_middleware):
        if entry.cls is RateLimitMiddleware and "PRE" in str(entry.kwargs.get("stage", "")).upper():
            assert idx < tenant_idx


# ── Plan 055 repair round 1 ──────────────────────────────────────────────────


async def test_schema_isolation_without_pool_refuses_before_start() -> None:
    # §D-flip6-module: pool=None under SCHEMA must refuse startup under the
    # default strict_validation=False — "optional" never silently means "unrouted".
    from varco_core.settings import bind_settings
    from varco_core.tenancy import TenancySettings, TenantIsolation
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.modules.defaults import default_modules
    from varco_fastapi.validation import ConfigurationError

    container = DIContainer()
    bind_settings(
        container, TenancySettings, instance=TenancySettings(isolation=TenantIsolation.SCHEMA)
    )
    started: list[str] = []

    class _Probe(TenancyModule):
        async def start(self, container: DIContainer) -> None:  # pragma: no cover - must not run
            started.append("tenancy")
            await super().start(container)

    app = create_varco_app(
        container,
        modules=[_Probe(chain=_jwt_chain()), *default_modules()],
        routers=[],
        validate=False,
    )

    with pytest.raises(ConfigurationError, match="pool="):
        async with app.router.lifespan_context(app):
            pass  # pragma: no cover - startup must refuse

    assert started == []


def _jwt_chain():
    from varco_core.tenancy.source import TenantSourceChain
    from varco_core.tenancy.sources import JwtClaimTenantSource

    return TenantSourceChain(sources=(JwtClaimTenantSource(),))


async def test_collector_and_middleware_agree_when_code_settings_and_env_differ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Settings bound in code with no sources + VARCO_TENANT_SOURCES=jwt exported:
    # the collector must report the chain the middleware actually runs.
    from fastapi import FastAPI
    from varco_core.di.collect import collect
    from varco_core.posture import PostureCollector
    from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware
    from varco_fastapi.posture import TenantPostureCollector

    container = DIContainer()
    _bind_sources(container)  # sources=() bound in code
    monkeypatch.setenv("VARCO_TENANT_SOURCES", "jwt")
    module = TenancyModule(catalog=_FakeCatalog(), pool=_CountingPool())

    module.configure_app(FastAPI(), container)
    spec = _only_spec(module, container)
    middleware = TenantResolutionMiddleware(FastAPI(), **spec.kwargs)
    collector = next(
        c
        for c in collect(container, PostureCollector)  # type: ignore[type-abstract]
        if isinstance(c, TenantPostureCollector)
    )
    ids = {f.check for f in collector.collect()}

    assert middleware.chain_implicit is False
    assert collector.chain_implicit is middleware.chain_implicit
    assert "tenant.no_chain" not in ids
    assert "tenant.legacy_source_implicit" not in ids
