"""
varco_fastapi.tenancy.mount
==============================
``mount_tenant_admin()`` — the bundled-deployment opt-in for the tenant
provisioning admin surface (Plan 007, Phase 5, step 9 — RD-9).

DESIGN: ``mount_tenant_admin(app, ...)`` over a ``create_varco_app(tenant_admin=...)`` kwarg
    See the plan's "DESIGN: mount_tenant_admin(app, ...) over a
    create_varco_app(tenant_admin=...) kwarg" section for the full
    rationale. Summary: grep-able, matches the ``SkillAdapter.mount()`` /
    ``build_policy_router()`` precedents, and — the load-bearing point —
    **impossible to enable by environment alone**. There is deliberately no
    ``VARCO_TENANCY_MOUNT_ADMIN`` env var anywhere in this codebase
    (asserted by ``varco_core``'s settings tests and this module's own
    behaviour): an app that merely has ``VARCO_TENANCY_ADMIN_DSN`` set in
    its environment exposes **no** provisioning route until this function
    is called explicitly, with an explicit acknowledgement.

Three independent barriers against accidental bundling:
    1. ``acknowledge_bundled_admin`` defaults ``False`` -> ``ValueError``.
    2. ``server_auth`` is mandatory (no default) -> ``TypeError``/``ValueError``
       (since 4.0 the same rule holds for ``mount_reliability_admin``/
       ``mount_webhook_admin`` — Plan 054 / FLIP-3/4).
    3. No env-var path exists at all — this function is the *only* way to
       mount the router.
"""

from __future__ import annotations

import logging
import weakref
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from varco_fastapi.tenancy.router import build_tenant_router

if TYPE_CHECKING:
    from fastapi import FastAPI
    from fastapi.params import Depends as DependsType

    from varco_fastapi.auth.server_auth import AbstractServerAuth

logger = logging.getLogger(__name__)

# Tracks which FastAPI app instances already had the admin surface mounted
# — refuses a second mount rather than silently duplicating routes.
#
# A WeakSet, not a ``set[int]`` of ``id(app)`` (Plan 022 / RIDER-2): ``id()``
# is unique only among *live* objects, so once an app was collected its
# address could be reused by a new, unrelated FastAPI instance, which would
# then match a stale entry and have its surface **silently not mounted**.
# WeakSet entries vanish when the app is collected, so no stale identity can
# ever be matched, and membership stays identity-based without keeping the
# app alive.
_MOUNTED_APPS: weakref.WeakSet[Any] = weakref.WeakSet()


def mount_tenant_admin(
    app: FastAPI,
    control_service: Any,
    *,
    acknowledge_bundled_admin: bool = False,
    server_auth: AbstractServerAuth | None = None,
    admin_role: str = "tenant-admin",
    prefix: str = "/tenancy",
    dependencies: Sequence[DependsType] = (),
) -> None:
    """
    Mount the tenant provisioning admin router into ``app`` — the
    explicit, opt-in "bundled" deployment shape (RD-9).

    Args:
        app:              The FastAPI app to mount into.
        control_service:  A ``TenantControlService``-shaped object.
        acknowledge_bundled_admin: Must be ``True`` — the friction is the
                          point (see module DESIGN note). Defaults ``False``
                          -> ``ValueError`` naming the standalone
                          alternative.
        server_auth:      Auth strategy. **Required.**
        admin_role:       Role required on every route. Defaults
                          ``"tenant-admin"`` — deliberately distinct from a
                          generic ``"admin"``.
        prefix:           URL prefix. Defaults ``"/tenancy"`` — a dedicated
                          prefix so an ingress can independently gate it.
        dependencies:     Extra FastAPI dependencies applied to every
                          mounted route (e.g. an IP allowlist, an mTLS
                          check) — a network-level gate independent of the
                          role guard.

    Raises:
        ValueError: ``acknowledge_bundled_admin`` is not ``True``, or this
            app was already mounted once.

    Edge cases:
        - Exactly one WARNING is logged at mount time, naming the
          trade-off, the role, and the prefix.
        - Underprivileged/unauthenticated calls are 403, never 500.
        - There is no environment-variable path to this function — the DSN
          alone (``VARCO_TENANCY_ADMIN_DSN``) mounts nothing.
    """
    if not acknowledge_bundled_admin:
        raise ValueError(
            "mount_tenant_admin() refuses to mount without "
            "acknowledge_bundled_admin=True. Bundling the tenant "
            "provisioning admin surface into an app pod puts the admin "
            "credential/DSN in that pod's own environment — the privilege "
            "RD-4 otherwise keeps out of app pods. Prefer the standalone "
            "control-plane deployment (a small app mounting only this "
            "router) unless a second container is genuinely not "
            "justified. If it truly is, re-call with "
            "acknowledge_bundled_admin=True."
        )

    if app in _MOUNTED_APPS:
        raise ValueError(
            "mount_tenant_admin() was already called for this app — "
            "refusing to mount a second time (would duplicate routes)."
        )

    # build_tenant_router() itself refuses a None server_auth — re-raised
    # here unchanged so this function's own Raises: doc stays accurate.
    router = build_tenant_router(
        control_service,
        server_auth=server_auth,
        admin_role=admin_role,
        prefix=prefix,
    )

    app.include_router(router, dependencies=list(dependencies))
    _MOUNTED_APPS.add(app)

    logger.warning(
        "mount_tenant_admin(): the tenant provisioning admin surface is "
        "now BUNDLED into this app at prefix=%r, guarded by role=%r. This "
        "puts cluster-DDL-adjacent privilege in this process's own "
        "environment. Prefer the standalone control-plane deployment "
        "unless a second container is genuinely not justified (RD-9).",
        prefix,
        admin_role,
    )
