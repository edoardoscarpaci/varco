"""
tests.test_audit_router
=========================
Plan 009, Phase 10 (R6) — varco_fastapi.admin.audit_router.

RED until ``varco_fastapi/admin/audit_router.py`` lands.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from varco_core.service.audit import AuditEntry, AuditRepository
from varco_core.service.tenant import tenant_context

from tests._admin_auth import HeaderAuth, ctx_auth

# Plan 054 / FLIP-3: server_auth is required (and admin_role enforced on every
# route); the pre-054 tests below use this stub. No cross-tenant role, so the
# tenant-scoping behaviour they pin is unchanged.
_ADMIN_AUTH = ctx_auth(["reliability-admin"])


class InMemoryAuditRepository(AuditRepository):
    def __init__(self) -> None:
        self.entries: list[AuditEntry] = []

    async def save(self, entry: AuditEntry) -> None:
        self.entries.append(entry)

    async def list_for_entity(self, entity_type, entity_id, *, limit=100, tenant_id=None):
        return [
            e for e in self.entries if e.entity_type == entity_type and e.entity_id == entity_id
        ][:limit]

    async def list(self, **filters):
        results = list(self.entries)
        if filters.get("entity_type"):
            results = [e for e in results if e.entity_type == filters["entity_type"]]
        return results[: filters.get("limit", 100)]


class TestAuditRouterFilters:
    def test_entries_endpoint_narrows_by_entity_type(self) -> None:
        from varco_fastapi.admin.audit_router import build_audit_router

        repo = InMemoryAuditRepository()
        router = build_audit_router(repo, server_auth=_ADMIN_AUTH)
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        # Plan 036 (S4b) / §D-S4-scope: an omitted `tenant_id` with no
        # ambient tenant context now raises (CrossTenantAccessError -> 403)
        # rather than silently sweeping every tenant — this route needs a
        # resolved tenant to reach 200.
        with tenant_context("t1"):
            resp = client.get("/audit/entries", params={"entity_type": "Order"})
        assert resp.status_code == 200

    def test_entry_by_id_returns_404_when_absent(self) -> None:
        import uuid

        from varco_fastapi.admin.audit_router import build_audit_router

        repo = InMemoryAuditRepository()
        router = build_audit_router(repo, server_auth=_ADMIN_AUTH)
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        resp = client.get(f"/audit/entries/{uuid.uuid4()}")
        assert resp.status_code == 404

    def test_verify_chain_returns_200_and_verified_true_for_empty_repo(self) -> None:
        """Plan 009 Phase 12 (R8) landed -- verify-chain is real now, not a
        501 stub. An empty (or fully unchained) repository verifies
        vacuously true."""
        from varco_fastapi.admin.audit_router import build_audit_router

        repo = InMemoryAuditRepository()
        router = build_audit_router(repo, server_auth=_ADMIN_AUTH)
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        # Plan 036 (S4b) / §D-S4-scope: same omitted-tenant_id rule as above.
        with tenant_context("t1"):
            resp = client.post("/audit/verify-chain")
        assert resp.status_code == 200
        assert resp.json()["verified"] is True


class TestAuditRouterAllowDelete:
    def test_delete_hidden_by_default(self) -> None:
        from varco_fastapi.admin.audit_router import build_audit_router

        repo = InMemoryAuditRepository()
        router = build_audit_router(repo, allow_delete=False, server_auth=_ADMIN_AUTH)
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        resp = client.delete("/audit/entries")
        # GET /audit/entries is always registered, so a missing DELETE
        # handler on the SAME path is Starlette's own "405 Method Not
        # Allowed" — not a 404. 404 is reserved for a path that does not
        # exist at all (see the DLQ router's absent-redriver test, where the
        # whole path is genuinely unregistered).
        assert resp.status_code == 405

    def test_delete_available_when_allow_delete_true(self) -> None:
        from varco_fastapi.admin.audit_router import build_audit_router

        repo = InMemoryAuditRepository()
        router = build_audit_router(repo, allow_delete=True, server_auth=_ADMIN_AUTH)
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        resp = client.request("DELETE", "/audit/entries", params={"entity_type": "Order"})
        assert resp.status_code != 404


# ── Plan 054 / FLIP-3 (§D-flip3, §D-flip3-enforce) — required server_auth + per-route admin_role ──


# The Request-taking stub lives in the shared helper (Plan 054 Step 8).
_HeaderAuth = HeaderAuth


def _route_cases(router) -> list[tuple[str, str]]:
    """Every (method, concrete path) of *router* — built from ``router.routes``
    so a newly added route cannot dodge the guard."""
    import re as _re
    import uuid as _uuid

    from fastapi.routing import APIRoute

    cases: list[tuple[str, str]] = []
    for route in router.routes:
        if not isinstance(route, APIRoute):
            continue
        path = _re.sub(
            r"\{([a-z_]+)\}",
            lambda m: str(_uuid.UUID(int=1)) if m.group(1).endswith("id") else "x",
            route.path,
        )
        for method in sorted(route.methods):
            cases.append((method, path))
    return cases


_ADMIN_HEADERS = {"X-Test-Auth": "reliability-admin,cross-tenant-admin"}


import pytest  # noqa: E402


def _full_audit_router(server_auth):
    from varco_fastapi.admin.audit_router import build_audit_router

    return build_audit_router(InMemoryAuditRepository(), server_auth=server_auth, allow_delete=True)


def _audit_cases() -> list[tuple[str, str]]:
    return _route_cases(_full_audit_router(_HeaderAuth()))


class TestFlip3AuditRouterServerAuthRequired:
    def test_build_audit_router_refuses_server_auth_none(self) -> None:
        from varco_fastapi.admin.audit_router import build_audit_router

        with pytest.raises(ValueError, match="requires server_auth") as exc:
            build_audit_router(InMemoryAuditRepository(), server_auth=None)
        assert "build_audit_router" in str(exc.value)

    def test_build_audit_router_requires_server_auth_keyword(self) -> None:
        import inspect

        from varco_fastapi.admin.audit_router import build_audit_router

        param = inspect.signature(build_audit_router).parameters["server_auth"]
        assert param.default is inspect.Parameter.empty
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        with pytest.raises(TypeError, match="server_auth"):
            build_audit_router(InMemoryAuditRepository())


class TestFlip3AuditRouterEnforcesAdminRoleOnEveryRoute:
    @pytest.mark.parametrize(("method", "path"), _audit_cases())
    def test_no_credentials_is_server_auth_401(self, method: str, path: str) -> None:
        app = FastAPI()
        app.include_router(_full_audit_router(_HeaderAuth()))

        resp = TestClient(app).request(method, path)

        assert resp.status_code == 401

    @pytest.mark.parametrize(("method", "path"), _audit_cases())
    def test_credentials_without_role_is_403(self, method: str, path: str) -> None:
        app = FastAPI()
        app.include_router(_full_audit_router(_HeaderAuth()))

        resp = TestClient(app).request(method, path, headers={"X-Test-Auth": "someone"})

        assert resp.status_code == 403
        assert resp.json()["detail"] == "'reliability-admin' role required."

    @pytest.mark.parametrize(("method", "path"), _audit_cases())
    def test_with_role_passes_the_guard(self, method: str, path: str) -> None:
        app = FastAPI()
        app.include_router(_full_audit_router(_HeaderAuth()))

        with tenant_context("t1"):
            resp = TestClient(app).request(method, path, headers=_ADMIN_HEADERS)

        assert resp.status_code not in (401, 403)

    def test_server_auth_resolved_once_per_request(self) -> None:
        auth = _HeaderAuth()
        app = FastAPI()
        app.include_router(_full_audit_router(auth))

        with tenant_context("t1"):
            resp = TestClient(app).get("/audit/entries", headers=_ADMIN_HEADERS)

        assert resp.status_code == 200
        assert auth.calls == 1
