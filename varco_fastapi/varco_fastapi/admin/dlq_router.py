"""
varco_fastapi.admin.dlq_router
=================================
``build_dlq_router`` — REST admin surface over an ``AbstractDeadLetterQueue``
(Plan 009, Phase 10 / R6).

A plain ``APIRouter``, not a ``VarcoRouter`` — the same
``build_policy_router``/``build_tenant_router`` precedent: a standalone admin
surface with hand-written JSON handlers, no service/repository generic.

``NotImplementedError`` (RD-4's capability-gap signal — a stream-shaped
backend like Kafka/NATS) is mapped to HTTP 501 with the backend name in the
detail, so a capability gap reads as a capability gap, not a generic 500.

Thread safety:  N/A — stateless route handlers, one DLQ instance shared
                   across requests (same as the DLQ itself).
Async safety:   ✅ All handlers are ``async def``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from varco_fastapi.admin._tenant_scope import resolve_ctx, resolve_tenant_scope

if TYPE_CHECKING:
    from varco_core.event.dlq import AbstractDeadLetterQueue
    from varco_core.event.redrive import DlqRedriver


def _entry_to_dict(entry: Any) -> dict[str, Any]:
    return {
        "entry_id": str(entry.entry_id),
        "channel": entry.channel,
        "handler_name": entry.handler_name,
        "source": str(entry.source),
        "error_type": entry.error_type,
        "error_message": entry.error_message,
        "attempts": entry.attempts,
        "tenant_id": entry.tenant_id,
    }


def build_dlq_router(
    dlq: AbstractDeadLetterQueue,
    *,
    redriver: DlqRedriver | None = None,
    server_auth: Any | None = None,
    admin_role: str = "reliability-admin",
    cross_tenant_role: str = "cross-tenant-admin",
    prefix: str = "/dlq",
) -> APIRouter:
    """
    Build the DLQ admin ``APIRouter``.

    Args:
        dlq:         The ``AbstractDeadLetterQueue`` to administer.
        redriver:    A ``DlqRedriver`` bound to ``dlq``. ``None`` (default)
                     — the redrive routes are not registered at all (RD-4/
                     DESIGN: an absent capability should not appear in the
                     OpenAPI schema, not surface as a 501 on every call).
        server_auth: Auth strategy — ``admin_role`` enforcement is still
                     left to the caller's own ``dependencies=`` (unchanged),
                     but ``server_auth`` is now resolved here as well, to
                     answer §D-S4-scope's cross-tenant question.
        admin_role:  Documented role requirement (enforced by the caller's
                     own ``dependencies=`` on ``app.include_router``).
        cross_tenant_role: Role required to omit ``tenant_id`` and reach
                     every tenant, or to supply a ``tenant_id`` other than
                     the resolved one (§D-S4-scope). An omitted ``tenant_id``
                     without this role means "my tenant", not "every
                     tenant".
        prefix:      URL prefix. Defaults to ``"/dlq"``.

    Routes:
        GET    {prefix}/entries                    list_entries() filters
        GET    {prefix}/entries/{entry_id}          404 when absent, 501 on a stream backend
        POST   {prefix}/entries/{entry_id}/redrive  501 without a redriver (only registered when redriver is set)
        POST   {prefix}/redrive                     batch; body carries filters + dry_run
        DELETE {prefix}/entries/{entry_id}          delete()
        DELETE {prefix}/entries                     delete_where(); 501 on Kafka/NATS
        GET    {prefix}/stats                       count() + count_by_channel() when supported
    """
    router = APIRouter(prefix=prefix, tags=["dlq-admin"])

    @router.get("/entries")
    async def list_entries(
        request: Request,
        channel: str | None = None,
        source: str | None = None,
        tenant_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        from varco_core.event.dlq import DeadLetterSource

        ctx = await resolve_ctx(server_auth, request)
        scoped_tenant_id = resolve_tenant_scope(tenant_id, ctx, cross_tenant_role)

        limit = min(limit, 1000)
        try:
            entries = await dlq.list_entries(
                limit=limit,
                offset=offset,
                channel=channel,
                source=DeadLetterSource(source) if source else None,
                tenant_id=scoped_tenant_id,
            )
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc
        return [_entry_to_dict(e) for e in entries]

    @router.get("/entries/{entry_id}")
    async def get_entry(entry_id: UUID) -> dict[str, Any]:
        try:
            entry = await dlq.get(entry_id)
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc
        if entry is None:
            raise HTTPException(status_code=404, detail="Dead letter not found.")
        return _entry_to_dict(entry)

    @router.delete("/entries/{entry_id}")
    async def delete_entry(entry_id: UUID) -> dict[str, Any]:
        await dlq.delete(entry_id)
        return {"deleted": True}

    @router.delete("/entries")
    async def delete_where(
        request: Request,
        older_than: str | None = None,
        channel: str | None = None,
        tenant_id: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        from datetime import datetime

        ctx = await resolve_ctx(server_auth, request)
        scoped_tenant_id = resolve_tenant_scope(tenant_id, ctx, cross_tenant_role)

        try:
            count = await dlq.delete_where(
                older_than=datetime.fromisoformat(older_than) if older_than else None,
                channel=channel,
                tenant_id=scoped_tenant_id,
                limit=limit,
            )
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"deleted": count}

    @router.get("/stats")
    async def stats() -> dict[str, Any]:
        count = await dlq.count()
        by_channel: dict[str, int] | None = None
        try:
            by_channel = await dlq.count_by_channel()
        except NotImplementedError:
            by_channel = None
        return {"count": count, "by_channel": by_channel}

    # RD-4/DESIGN: absent capability → absent route (not 501-on-call).
    if redriver is not None:

        @router.post("/entries/{entry_id}/redrive")
        async def redrive_one(entry_id: UUID, dry_run: bool = False) -> dict[str, Any]:
            from varco_core.event.redrive import DeadLetterNotAddressable

            try:
                outcome = await redriver.redrive(entry_id, dry_run=dry_run)
            except DeadLetterNotAddressable as exc:
                raise HTTPException(status_code=501, detail=str(exc)) from exc
            return {
                "entry_id": str(outcome.entry_id),
                "published": outcome.published,
                "acked": outcome.acked,
                "error": outcome.error,
            }

        @router.post("/redrive")
        async def redrive_batch(
            request: Request, body: dict[str, Any] | None = None
        ) -> dict[str, Any]:
            from varco_core.event.dlq import DeadLetterSource

            ctx = await resolve_ctx(server_auth, request)
            body = body or {}
            scoped_tenant_id = resolve_tenant_scope(body.get("tenant_id"), ctx, cross_tenant_role)
            source = body.get("source")
            report = await redriver.redrive_batch(
                limit=body.get("limit", 10),
                channel=body.get("channel"),
                source=DeadLetterSource(source) if source else None,
                tenant_id=scoped_tenant_id,
                dry_run=body.get("dry_run", False),
            )
            return {
                "attempted": report.attempted,
                "succeeded": report.succeeded,
                "failed": report.failed,
                "dry_run": report.dry_run,
            }

    return router


__all__ = ["build_dlq_router"]
