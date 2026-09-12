"""
varco_fastapi.admin.audit_router
===================================
``build_audit_router`` — REST admin/query surface over an
``AuditRepository`` (Plan 009, Phase 10 / R6).

``allow_delete=False`` by default: an audit log you can ``DELETE`` over HTTP
is not an audit log. Retention belongs to the CLI/sweep job
(``varco retention prune``, ``varco_core/cli/retention.py``).

Thread safety:  N/A — stateless route handlers.
Async safety:   ✅ All handlers are ``async def``.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from varco_fastapi.admin._tenant_scope import resolve_ctx, resolve_tenant_scope

if TYPE_CHECKING:
    from varco_core.service.audit import AuditRepository


def _entry_to_dict(entry: Any) -> dict[str, Any]:
    return {
        "entry_id": str(entry.entry_id),
        "entity_type": entry.entity_type,
        "entity_id": entry.entity_id,
        "action": entry.action,
        "actor_id": entry.actor_id,
        "diff": entry.diff,
        "occurred_at": entry.occurred_at.isoformat() if entry.occurred_at else None,
        "correlation_id": entry.correlation_id,
        "tenant_id": entry.tenant_id,
    }


def build_audit_router(
    audit_repo: AuditRepository,
    *,
    server_auth: Any | None = None,
    admin_role: str = "reliability-admin",
    cross_tenant_role: str = "cross-tenant-admin",
    prefix: str = "/audit",
    allow_delete: bool = False,
) -> APIRouter:
    """
    Build the audit-log admin ``APIRouter``.

    Args:
        cross_tenant_role: Role required to omit ``tenant_id`` and reach
            every tenant, or to supply a ``tenant_id`` other than the
            resolved one (§D-S4-scope). Same semantics as
            ``build_dlq_router``'s kwarg of the same name.

    Routes:
        GET  {prefix}/entries                          all list() filters as query params
        GET  {prefix}/entries/{entry_id}                404 when absent
        GET  {prefix}/entries/{entity_type}/{entity_id} list_for_entity
        POST {prefix}/verify-chain                      Plan 009 Phase 12 (R8) hash-chain verification
        DELETE {prefix}/entries                          retention sweep; only when allow_delete=True
    """
    router = APIRouter(prefix=prefix, tags=["audit-admin"])

    @router.get("/entries")
    async def list_entries(
        request: Request,
        actor_id: str | None = None,
        action: str | None = None,
        entity_type: str | None = None,
        entity_id: str | None = None,
        tenant_id: str | None = None,
        correlation_id: str | None = None,
        occurred_from: str | None = None,
        occurred_to: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        ctx = await resolve_ctx(server_auth, request)
        scoped_tenant_id = resolve_tenant_scope(tenant_id, ctx, cross_tenant_role)

        limit = min(limit, 1000)
        if occurred_from and occurred_to and occurred_from > occurred_to:
            raise HTTPException(status_code=422, detail="occurred_from must be <= occurred_to.")
        try:
            entries = await audit_repo.list(
                actor_id=actor_id,
                action=action,
                entity_type=entity_type,
                entity_id=entity_id,
                tenant_id=scoped_tenant_id,
                correlation_id=correlation_id,
                occurred_from=(datetime.fromisoformat(occurred_from) if occurred_from else None),
                occurred_to=(datetime.fromisoformat(occurred_to) if occurred_to else None),
                limit=limit,
                offset=offset,
            )
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc
        return [_entry_to_dict(e) for e in entries]

    @router.get("/entries/{entry_id}")
    async def get_entry(entry_id: UUID) -> dict[str, Any]:
        try:
            entries = await audit_repo.list(limit=1000)
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc
        for e in entries:
            if str(e.entry_id) == str(entry_id):
                return _entry_to_dict(e)
        raise HTTPException(status_code=404, detail="Audit entry not found.")

    @router.get("/entries/{entity_type}/{entity_id}")
    async def get_for_entity(
        entity_type: str, entity_id: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        entries = await audit_repo.list_for_entity(entity_type, entity_id, limit=min(limit, 1000))
        return [_entry_to_dict(e) for e in entries]

    @router.post("/verify-chain")
    async def verify_chain(
        request: Request,
        entity_type: str | None = None,
        entity_id: str | None = None,
        tenant_id: str | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """
        Verify the audit hash chain (Plan 009, Phase 12 / R8) over the
        entries matched by the given filters (all optional — omitting every
        filter verifies up to ``limit`` entries).

        Returns ``{"verified": true}`` on an unbroken chain, or
        ``{"verified": false, "findings": [...]}`` naming each
        ``ChainGap``/``HashMismatch`` found. 501 if this repository does not
        implement ``list()`` at all (concrete-but-raising on the ABC).
        """
        from varco_core.service.audit import AuditRepository, ChainGap, HashMismatch

        ctx = await resolve_ctx(server_auth, request)
        scoped_tenant_id = resolve_tenant_scope(tenant_id, ctx, cross_tenant_role)

        try:
            entries = await audit_repo.list(
                entity_type=entity_type,
                entity_id=entity_id,
                tenant_id=scoped_tenant_id,
                limit=min(limit, 10_000),
            )
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc

        result = AuditRepository.verify_chain(entries)
        if result is True:
            return {"verified": True, "findings": []}

        findings: list[dict[str, Any]] = []
        for f in result:
            if isinstance(f, ChainGap):
                findings.append(
                    {
                        "type": "chain_gap",
                        "expected_seq": f.expected_seq,
                        "found_seq": f.found_seq,
                    }
                )
            elif isinstance(f, HashMismatch):
                findings.append(
                    {
                        "type": "hash_mismatch",
                        "seq": f.seq,
                        "expected_prev_hash": f.expected_prev_hash,
                        "actual_prev_hash": f.actual_prev_hash,
                    }
                )
        return {"verified": False, "findings": findings}

    if allow_delete:

        @router.delete("/entries")
        async def delete_where(
            request: Request,
            older_than: str | None = None,
            entity_type: str | None = None,
            tenant_id: str | None = None,
            limit: int | None = None,
        ) -> dict[str, Any]:
            ctx = await resolve_ctx(server_auth, request)
            scoped_tenant_id = resolve_tenant_scope(tenant_id, ctx, cross_tenant_role)

            try:
                count = await audit_repo.delete_where(
                    older_than=(datetime.fromisoformat(older_than) if older_than else None),
                    entity_type=entity_type,
                    tenant_id=scoped_tenant_id,
                    limit=limit,
                )
            except NotImplementedError as exc:
                raise HTTPException(status_code=501, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            return {"deleted": count}

    return router


__all__ = ["build_audit_router"]
