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
        router = build_audit_router(repo)
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
        router = build_audit_router(repo)
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
        router = build_audit_router(repo)
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
        router = build_audit_router(repo, allow_delete=False)
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
        router = build_audit_router(repo, allow_delete=True)
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        resp = client.request("DELETE", "/audit/entries", params={"entity_type": "Order"})
        assert resp.status_code != 404
