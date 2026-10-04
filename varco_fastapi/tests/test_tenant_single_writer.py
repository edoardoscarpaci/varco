"""
Plan 055 / FLIP-9 (§D-flip9, §D-flip9-shared) — the one HTTP writer of
``current_tenant()`` is ``TenantResolutionMiddleware._continue()``.

Red until Plan 055 lands:
- ``test_only_one_http_writer_of_tenant_context`` — ``request_context.py``
  still calls ``tenant_context(...)`` (the unchained claim setter).
- the ``catalog=None, pool=None`` regression variant — ``_continue()``
  still dereferences ``catalog``.
- the module-path variant — ``TenancyModule()`` has no middleware.

``test_current_tenant_is_set_by_the_chain_path_with_catalog`` is a CONTROL:
it passes today and must stay green after the setter is removed.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


class _CountingServerAuth:
    def __init__(self, ctx):
        self._ctx = ctx
        self.calls = 0

    async def __call__(self, request):
        self.calls += 1
        return self._ctx


class _FakeCatalog:
    async def get(self, tenant_id: str):
        from varco_core.tenancy.catalog import TenantDescriptor

        return TenantDescriptor(tenant_id=tenant_id, status="active")


class _CountingPool:
    async def ensure(self, tenant_id: str):
        return object()


def _jwt_ctx():
    from varco_core.auth.base import AuthContext

    return AuthContext(user_id="u1", metadata={"tenant_id": "acme"})


def _add_probe_route(app: FastAPI, captured: dict) -> None:
    @app.get("/probe")
    async def probe():
        from varco_core.service.tenant import current_tenant
        from varco_core.tenancy.provenance import current_tenant_provenance
        from varco_fastapi.context import get_auth_context_or_none

        prov = current_tenant_provenance()
        captured["auth"] = get_auth_context_or_none()
        return {
            "tenant": current_tenant(),
            "provenance_tenant": prov.tenant_id if prov else None,
        }


def _stacked_app(*, server_auth, catalog, pool, captured):
    from varco_core.tenancy.source import TenantSourceChain
    from varco_core.tenancy.sources import JwtClaimTenantSource
    from varco_fastapi.middleware.request_context import RequestContextMiddleware
    from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware

    app = FastAPI()
    # last-added = outermost: TenantResolution runs OUTSIDE RequestContext.
    app.add_middleware(RequestContextMiddleware, server_auth=server_auth)
    app.add_middleware(
        TenantResolutionMiddleware,
        catalog=catalog,
        pool=pool,
        chain=TenantSourceChain(sources=(JwtClaimTenantSource(),)),
        server_auth=server_auth,
    )
    _add_probe_route(app, captured)
    return app


def _assert_chain_path_set_tenant(app, server_auth, ctx, captured) -> None:
    response = TestClient(app).get("/probe")  # no X-Tenant-Id header

    assert response.status_code == 200
    assert response.json() == {"tenant": "acme", "provenance_tenant": "acme"}
    # The deferral guarantee survives the setter removal: auth runs once.
    assert server_auth.calls == 1
    assert captured["auth"] is ctx


def test_current_tenant_is_set_by_the_chain_path_with_catalog() -> None:
    # CONTROL (passes today): the chain path already writes current_tenant().
    ctx = _jwt_ctx()
    server_auth = _CountingServerAuth(ctx)
    captured: dict = {}
    app = _stacked_app(
        server_auth=server_auth, catalog=_FakeCatalog(), pool=_CountingPool(), captured=captured
    )

    _assert_chain_path_set_tenant(app, server_auth, ctx, captured)


def test_current_tenant_is_set_by_the_chain_path_after_setter_removal() -> None:
    # THE regression test: the SHARED-app replacement (no catalog, no pool)
    # still yields current_tenant() via the chain — the class of app the
    # removed RequestContextMiddleware setter used to serve.
    ctx = _jwt_ctx()
    server_auth = _CountingServerAuth(ctx)
    captured: dict = {}
    app = _stacked_app(server_auth=server_auth, catalog=None, pool=None, captured=captured)

    _assert_chain_path_set_tenant(app, server_auth, ctx, captured)


def test_current_tenant_is_set_through_tenancy_module_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The module path (TenancyModule() + VARCO_TENANT_SOURCES=jwt) agrees with
    # the hand-wired path.
    from providify import DIContainer
    from varco_core.di import bind_instance
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.auth.server_auth import AbstractServerAuth
    from varco_fastapi.modules import TenancyModule
    from varco_fastapi.modules.defaults import default_modules

    monkeypatch.setenv("VARCO_TENANT_SOURCES", "jwt")
    ctx = _jwt_ctx()
    server_auth = _CountingServerAuth(ctx)
    container = DIContainer()
    bind_instance(container, AbstractServerAuth, server_auth)
    app = create_varco_app(
        container, modules=[*default_modules(), TenancyModule()], routers=[], validate=False
    )
    captured: dict = {}
    _add_probe_route(app, captured)

    with TestClient(app) as client:
        response = client.get("/probe")

    assert response.status_code == 200
    assert response.json() == {"tenant": "acme", "provenance_tenant": "acme"}
    assert server_auth.calls == 1


def _calls_tenant_context(source: str) -> bool:
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name == "tenant_context":
                return True
    return False


def test_only_one_http_writer_of_tenant_context() -> None:
    # Structural single-decision-point property (AST, so docstrings/comments
    # mentioning tenant_context(...) do not count). Never add a second writer.
    import varco_fastapi

    root = Path(varco_fastapi.__file__).parent
    writers = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*.py")
        if _calls_tenant_context(path.read_text(encoding="utf-8"))
    }

    assert writers == {"middleware/tenant_resolution.py"}
