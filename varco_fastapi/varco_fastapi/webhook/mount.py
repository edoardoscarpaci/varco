"""
varco_fastapi.webhook.mount
==============================
``mount_webhook_admin`` — the single way to expose the webhook subscription
admin surface on a running app (Plan 031 / D4d, Step 17, §D-D4-admin).

Same shape as ``mount_reliability_admin``
(``varco_fastapi/admin/mount.py``): can create/delete subscriptions and
rotate secrets, so it requires an explicit ``acknowledge_bundled_admin=True``
and there is deliberately **no** env var that mounts it (CLAUDE.md's
``mount_*`` taxonomy, RD-9). Never a ``create_varco_app()`` kwarg either.
Since 4.0 (Plan 054 / FLIP-4) ``server_auth`` is a required keyword.

Thread safety:  N/A — mounting happens once at startup.
Async safety:   N/A — synchronous FastAPI route registration.
"""

from __future__ import annotations

import weakref
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI
    from varco_core.event.redrive import DlqRedriver
    from varco_core.webhook.base import WebhookSubscriptionRepository

# Same WeakSet double-mount guard as mount_reliability_admin/mount_tenant_admin
# (Plan 022 / RIDER-2) — identity-based, never id()-based.
_MOUNTED_APPS: weakref.WeakSet[Any] = weakref.WeakSet()

__all__ = ["mount_webhook_admin"]


def mount_webhook_admin(
    app: FastAPI,
    *,
    repository: WebhookSubscriptionRepository,
    server_auth: Any,
    redriver: DlqRedriver | None = None,
    acknowledge_bundled_admin: bool = False,
    admin_role: str = "webhook-admin",
    cross_tenant_role: str = "cross-tenant-admin",
    prefix: str = "/webhooks",
) -> None:
    """
    Mount the webhook subscription admin router under ``prefix``.

    Args:
        app:         The FastAPI app to mount onto.
        repository:  The ``WebhookSubscriptionRepository`` to administer.
        redriver:    Optional ``DlqRedriver`` — enables the replay route.
        acknowledge_bundled_admin: Required ``True`` or this raises
                     ``ValueError`` (RD-9) — this surface can create/delete
                     subscriptions and rotate secrets.
        server_auth: **Required** (4.0, Plan 054 / FLIP-4). Auth strategy —
                     ``admin_role`` is enforced on every route. Behind a
                     gateway that already verified the caller, pass
                     ``PassthroughAuth(acknowledge_unverified=True)``.
        admin_role:  Role required on every route (403 otherwise).
        cross_tenant_role: Role required to address a subscription outside
                     the resolved tenant (§D-S4-role). Forwarded to
                     ``build_webhook_router`` unchanged.
        prefix:      URL prefix for the whole admin surface.

    Raises:
        ValueError: ``acknowledge_bundled_admin`` is not ``True``, this app
            was already mounted once, or ``server_auth is None`` (checked in
            that order, before the router is included).
        TypeError: ``server_auth`` omitted.

    Edge cases:
        - ``server_auth=None`` → ``ValueError`` before anything is mounted
          (until 4.0 it mounted unauthenticated with a WARNING); same rule
          and reasoning as ``mount_reliability_admin`` (its DESIGN block).
    """
    if not acknowledge_bundled_admin:
        raise ValueError(
            "mount_webhook_admin() requires acknowledge_bundled_admin=True. "
            "This surface can create/delete webhook subscriptions and "
            "rotate secrets — a privileged control surface, same posture as "
            "mount_reliability_admin()/mount_tenant_admin() (RD-9). Pass it "
            "only after confirming a standalone deployment genuinely isn't "
            "justified."
        )

    if app in _MOUNTED_APPS:
        raise ValueError(
            "mount_webhook_admin() was already called for this app — "
            "refusing to mount a second time (would duplicate routes)."
        )

    from varco_fastapi.webhook.router import _require_server_auth, build_webhook_router

    # Before include_router — a refusal never half-mounts or poisons _MOUNTED_APPS.
    _require_server_auth(server_auth, owner="mount_webhook_admin")

    app.include_router(
        build_webhook_router(
            repository,
            redriver=redriver,
            server_auth=server_auth,
            admin_role=admin_role,
            cross_tenant_role=cross_tenant_role,
            prefix=prefix,
        )
    )
    _MOUNTED_APPS.add(app)
