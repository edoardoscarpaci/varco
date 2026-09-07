"""
varco_fastapi.admin._tenant_scope
====================================
Shared cross-tenant resolution helper for the reliability admin routers
(``dlq_router.py``/``audit_router.py``) — Plan 036 (S4b) / Phase 1, §D-S4-scope,
§D-S4-role.

Not part of the public API (no ``__all__`` re-export from
``varco_fastapi.admin``) — an internal module shared by two sibling routers
so the mapping logic exists in exactly one place, per §D-S4-bola's "single
except in the router so it cannot drift route-to-route" rule.

Thread safety:  N/A — pure functions / stateless coroutines.
Async safety:   ✅ ``_resolve_ctx`` is the only I/O-shaped call (invokes the
                   caller-supplied ``server_auth``), awaited directly.
"""

from __future__ import annotations

import inspect
from typing import Any

from fastapi import HTTPException, Request

__all__: list[str] = []


async def resolve_ctx(server_auth: Any, request: Request) -> Any:
    """
    Resolve ``server_auth`` to its context object, or ``None`` when no auth
    strategy is configured — mirrors ``webhook.router._resolve_ctx``
    (duplicated rather than shared across packages; both are small, and a
    cross-router-family import would be a heavier coupling than the
    duplication itself).

    Returns:
        The resolved context, or ``None`` when ``server_auth is None``.
    """
    if server_auth is None:
        return None
    if inspect.signature(server_auth).parameters:
        return (
            await server_auth(request)
            if inspect.iscoroutinefunction(server_auth)
            else server_auth(request)
        )
    return await server_auth() if inspect.iscoroutinefunction(server_auth) else server_auth()


def allow_cross_tenant(ctx: Any, cross_tenant_role: str) -> bool:
    """§D-S4-role: ``ctx is None`` ⇒ ``False``, unconditionally."""
    return ctx is not None and cross_tenant_role in getattr(ctx, "roles", [])


def resolve_tenant_scope(
    requested: str | None,
    ctx: Any,
    cross_tenant_role: str,
) -> str | None:
    """
    Resolve the ``tenant_id`` a reliability-admin route must scope to
    (§D-S4-scope) — the single mapping point ``list_entries``/
    ``delete_where``/``redrive_batch``/``verify_chain`` all go through.

    Args:
        requested: The ``tenant_id`` query parameter as supplied by the
            caller, or ``None`` when omitted.
        ctx: The resolved auth context (``None`` when unauthenticated).
        cross_tenant_role: The role that authorizes an unscoped sweep.

    Returns:
        - ``requested is None`` and the caller holds ``cross_tenant_role``:
          ``None`` — an explicit, privileged, unscoped sweep (today's
          behaviour, restored exactly for a caller who opts in).
        - ``requested is None`` otherwise: ``current_tenant()`` via
          ``assert_tenant_matches(None, ...)`` — "mine", not "everyone".
        - ``requested`` given: ``assert_tenant_matches(requested, ...)``.

    Raises:
        HTTPException: 403, mapped from ``CrossTenantAccessError`` — no
            tenant resolved and none requested, or a requested tenant that
            disagrees with the resolved one and the caller lacks the role.
    """
    from varco_core.tenancy import CrossTenantAccessError, assert_tenant_matches

    allow = allow_cross_tenant(ctx, cross_tenant_role)
    if requested is None and allow:
        # An explicit, privileged, unscoped sweep — the pre-guard default,
        # restored only for a caller who holds the role.
        return None
    try:
        return assert_tenant_matches(requested, allow_cross_tenant=allow)
    except CrossTenantAccessError:
        raise HTTPException(status_code=403, detail="Cross-tenant access denied.") from None
