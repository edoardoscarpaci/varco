"""
varco_fastapi.admin.mount
============================
``mount_reliability_admin`` — the single way to expose the DLQ/audit admin
surfaces on a running app (Plan 009, Phase 10 / R6).

Mirrors Plan 007 RD-9 (the tenant control plane) verbatim: this surface can
*replay* messages onto the bus and *delete* audit records — at least as
privileged as the tenant admin — so it requires an explicit
``acknowledge_bundled_admin=True`` and there is deliberately **no**
env var that mounts it. Since 4.0 (Plan 054 / FLIP-3) ``server_auth`` is a
required keyword too — the same barrier-2 rule as ``mount_tenant_admin``.

Thread safety:  N/A — mounting happens once at startup.
Async safety:   N/A — synchronous FastAPI route registration.
"""

from __future__ import annotations

import weakref
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI
    from varco_core.event.dlq import AbstractDeadLetterQueue
    from varco_core.event.redrive import DlqRedriver
    from varco_core.service.audit import AuditRepository

# Tracks which FastAPI app instances already had the admin surface mounted
# — refuses a second mount rather than silently duplicating routes. Same
# pattern as varco_fastapi.tenancy.mount._MOUNTED_APPS.
#
# A WeakSet, not a ``set[int]`` of ``id(app)`` (Plan 022 / RIDER-2): ``id()``
# is unique only among *live* objects, so once an app was collected its
# address could be reused by a new, unrelated FastAPI instance, which would
# then match a stale entry and have its surface **silently not mounted**.
# WeakSet entries vanish when the app is collected, so no stale identity can
# ever be matched, and membership stays identity-based without keeping the
# app alive.
_MOUNTED_APPS: weakref.WeakSet[Any] = weakref.WeakSet()


def mount_reliability_admin(
    app: FastAPI,
    *,
    server_auth: Any,
    audit_repo: AuditRepository | None = None,
    dlq: AbstractDeadLetterQueue | None = None,
    redriver: DlqRedriver | None = None,
    acknowledge_bundled_admin: bool = False,
    admin_role: str = "reliability-admin",
    cross_tenant_role: str = "cross-tenant-admin",
    prefix: str = "/reliability",
    dependencies: Sequence[Any] | None = None,
) -> None:
    """
    Mount the audit/DLQ admin routers under ``prefix``.

    Args:
        app:         The FastAPI app to mount onto.
        audit_repo:  Mounts ``{prefix}/audit/*`` when given.
        dlq:         Mounts ``{prefix}/dlq/*`` when given.
        redriver:    Passed through to ``build_dlq_router`` — enables the
                     redrive routes.
        acknowledge_bundled_admin: Required ``True`` or this raises
                     ``ValueError`` (RD-9) — bundling admin-adjacent
                     privilege into the app pod's own environment is a
                     deliberate, friction-gated choice, not a default.
        server_auth: **Required** (4.0, Plan 054 / FLIP-3). Auth strategy
                     forwarded to both routers, which resolve it and enforce
                     ``admin_role`` on every route. Behind a gateway that
                     already verified the caller, pass
                     ``PassthroughAuth(acknowledge_unverified=True)``.
        admin_role:  Role required on every mounted route (403 otherwise).
        cross_tenant_role: Role required to reach every tenant with an
                     omitted ``tenant_id``, or a ``tenant_id`` other than
                     the resolved one (§D-S4-scope). Forwarded unchanged to
                     both routers.
        prefix:      URL prefix for the whole admin surface.
        dependencies: Extra FastAPI dependencies (e.g. an IP allowlist,
                     an mTLS check) applied to every mounted route —
                     **additive** to the built-in ``admin_role`` guard, never
                     a replacement for it.

    Raises:
        ValueError: ``acknowledge_bundled_admin`` is not ``True``, this
            app was already mounted once (a router was actually included on
            a previous call — see Edge cases), or ``server_auth is None``
            (checked in that order, before anything is included).
        TypeError: ``server_auth`` omitted (Python's own required-keyword
            error).

    Edge cases:
        - ``server_auth=None`` → ``ValueError`` before any router is
          included, so a refusal never half-mounts and never records the app
          in the double-mount guard (until 4.0 it mounted unauthenticated
          with one WARNING).
        - With neither ``audit_repo`` nor ``dlq`` a non-``None``
          ``server_auth`` is still required — a guard that can never be
          satisfied is refused even when it would guard nothing.
        - Mounting with neither ``audit_repo`` nor ``dlq`` given mounts
          nothing and does **not** poison the app for a later real mount —
          the app id is only recorded when at least one router was actually
          included. This is a deliberate deviation from
          ``mount_tenant_admin()``, which has no "mount nothing" case.
        - The double-mount guard is a ``weakref.WeakSet`` keyed by app
          identity. A collected app drops out of the set on its own, so a
          later, unrelated ``FastAPI`` instance can never collide with it.
          (Until 3.0.0 the guard was a ``set[int]`` of ``id(app)``, which
          could reuse a collected app's address and silently skip mounting —
          Plan 022 / RIDER-2, fixed in both this module and
          ``mount_tenant_admin()`` together.)

    DESIGN: required ``server_auth``, no acknowledgement kwarg (Plan 054 /
        FLIP-3)
        ✅ The three admin mounts are identical on barrier 2 —
           ``build_tenant_router``/``mount_tenant_admin`` already refused
           ``None``; ``ValueError`` matches the two other refusals here.
        ✅ The one legitimate "no varco auth" deployment (a verifying
           gateway) already has its acknowledgement kwarg:
           ``PassthroughAuth(acknowledge_unverified=True)``, reported by
           ``auth.passthrough_bound``.
        ❌ Every call site gains a keyword (a test stub is three lines).
        Rejected: an ``acknowledge_unauthenticated=True`` kwarg — every
        existing ``acknowledge_*`` gates a shape that is *sometimes* right;
        an unauthenticated replay/delete surface never is, and it would be
        a second knob for the gateway case ``PassthroughAuth`` already
        covers.
    """
    if not acknowledge_bundled_admin:
        raise ValueError(
            "mount_reliability_admin() requires acknowledge_bundled_admin=True. "
            "This surface can replay messages onto the bus and delete audit "
            "records — at least as privileged as the tenant control plane "
            "(Plan 007 RD-9). Pass it only after confirming a standalone "
            "deployment genuinely isn't justified."
        )

    if app in _MOUNTED_APPS:
        raise ValueError(
            "mount_reliability_admin() was already called for this app — "
            "refusing to mount a second time (would duplicate routes)."
        )

    from varco_fastapi.admin._tenant_scope import require_server_auth

    # Before any include_router — a refusal never half-mounts (and never
    # records the app in _MOUNTED_APPS).
    require_server_auth(
        server_auth,
        owner="mount_reliability_admin",
        surface="replay messages onto the bus and delete audit records",
    )

    mounted_any = False

    if audit_repo is not None:
        from varco_fastapi.admin.audit_router import build_audit_router

        app.include_router(
            build_audit_router(
                audit_repo,
                server_auth=server_auth,
                admin_role=admin_role,
                cross_tenant_role=cross_tenant_role,
                prefix=f"{prefix}/audit",
            ),
            dependencies=list(dependencies) if dependencies else None,
        )
        mounted_any = True

    if dlq is not None:
        from varco_fastapi.admin.dlq_router import build_dlq_router

        app.include_router(
            build_dlq_router(
                dlq,
                redriver=redriver,
                server_auth=server_auth,
                admin_role=admin_role,
                cross_tenant_role=cross_tenant_role,
                prefix=f"{prefix}/dlq",
            ),
            dependencies=list(dependencies) if dependencies else None,
        )
        mounted_any = True

    # Only poison the app id when something was actually mounted — a
    # no-op call (neither audit_repo nor dlq given) must not refuse a
    # later, legitimate mount on the same app (see Edge cases above).
    if mounted_any:
        _MOUNTED_APPS.add(app)


__all__ = ["mount_reliability_admin"]
