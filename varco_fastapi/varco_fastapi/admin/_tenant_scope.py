"""
varco_fastapi.admin._tenant_scope
====================================
Shared auth + cross-tenant helpers for the reliability admin routers
(``dlq_router.py``/``audit_router.py``) — Plan 036 (S4b) / Phase 1, §D-S4-scope,
§D-S4-role; ``admin_guard``/``require_server_auth`` added by Plan 054 /
FLIP-3 (§D-flip3, §D-flip3-enforce).

Not part of the public API (no ``__all__`` re-export from
``varco_fastapi.admin``) — an internal module shared by two sibling routers
so the mapping logic exists in exactly one place, per §D-S4-bola's "single
except in the router so it cannot drift route-to-route" rule.

Thread safety:  N/A — pure functions / stateless coroutines.
Async safety:   ✅ ``resolve_ctx`` is the only I/O-shaped call (invokes the
                   caller-supplied ``server_auth``), awaited directly; the
                   per-request ctx is stashed on ``request.state`` (request-
                   scoped, never shared across requests).
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import HTTPException, Request

__all__: list[str] = []

#: ``request.state`` attribute the router-level guard stashes the resolved
#: ctx under, so per-route ``resolve_ctx()`` calls never invoke
#: ``server_auth`` a second time for the same request.
_CTX_STATE_ATTR = "varco_admin_ctx"


def require_server_auth(server_auth: Any, *, owner: str, surface: str) -> None:
    """
    Refuse ``server_auth=None`` at build time (Plan 054 / FLIP-3/4).

    Args:
        server_auth: The caller-supplied auth strategy.
        owner: The public function name, for the message (e.g.
            ``"build_dlq_router"``).
        surface: What the surface can do, for the message.

    Raises:
        ValueError: ``server_auth is None`` — a guard that can never be
            satisfied is a startup error, not a per-request one (the
            ``build_tenant_router`` precedent).
    """
    if server_auth is None:
        raise ValueError(
            f"{owner}() requires server_auth — an admin surface that can {surface} "
            f"must never mount unauthenticated (4.0, Plan 054 / FLIP-3/4). Behind a "
            f"gateway that already verified the caller, pass "
            f"PassthroughAuth(acknowledge_unverified=True)."
        )


async def resolve_ctx(server_auth: Any, request: Request) -> Any:
    """
    Resolve ``server_auth`` to its context object — mirrors
    ``webhook.router._resolve_ctx`` (duplicated rather than shared across
    packages; both are small, and a cross-router-family import would be a
    heavier coupling than the duplication itself).

    Returns:
        The ctx already stashed on ``request.state`` by `admin_guard` (so
        ``server_auth`` runs once per request), else the freshly resolved
        context. ``None`` when ``server_auth is None`` (unreachable through
        the routers since 4.0 — they refuse it) or when ``server_auth``
        itself returns ``None`` for an anonymous caller.
    """
    if hasattr(request.state, _CTX_STATE_ATTR):
        return getattr(request.state, _CTX_STATE_ATTR)
    if server_auth is None:
        return None
    if inspect.signature(server_auth).parameters:
        return (
            await server_auth(request)
            if inspect.iscoroutinefunction(server_auth)
            else server_auth(request)
        )
    return await server_auth() if inspect.iscoroutinefunction(server_auth) else server_auth()


def admin_guard(server_auth: Any, admin_role: str) -> Callable[[Request], Awaitable[Any]]:
    """
    Build the router-level dependency every reliability admin route runs
    (Plan 054 / §D-flip3-enforce): resolve ``server_auth``, enforce
    ``admin_role``, and stash the ctx on ``request.state``.

    Args:
        server_auth: The (required, non-``None``) auth strategy — a callable
            taking either ``(request)`` or nothing, sync or async.
        admin_role: The role every route requires.

    Returns:
        An ``async (request) -> ctx`` callable for ``Depends(...)``.

    Raises:
        HTTPException: whatever ``server_auth`` raises (typically 401);
            403 ``"'<admin_role>' role required."`` when the ctx lacks the
            role — the same text as the webhook router's ``_require_role``.

    Edge cases:
        - A ``server_auth`` that returns ``None`` is an
          authenticated-but-anonymous caller — it fails the role check
          (403) and ``allow_cross_tenant`` stays ``False``.
        - As a router-level dependency it runs before per-route parameter
          validation, so an unauthenticated malformed request is 401/403,
          never 422 — never validate for a caller you have not
          authenticated.
    """

    async def _guard(request: Request) -> Any:
        ctx = await resolve_ctx(server_auth, request)
        if admin_role not in getattr(ctx, "roles", []):
            raise HTTPException(status_code=403, detail=f"{admin_role!r} role required.")
        setattr(request.state, _CTX_STATE_ATTR, ctx)
        return ctx

    return _guard


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
