"""
varco_fastapi.webhook
========================
Outbound webhook admin mount (Plan 031 / D4d, §D-D4-admin,
§D-D4-home) plus, as of Plan 038 / S19, the inbound verification route
dependency (``verify_webhook``/``VerifiedWebhook``, §D-S19-seam) — the only
FastAPI-specific pieces of either plan; everything portable lives in
``varco_core.webhook``/``varco_core.webhook.inbound``.
"""

from __future__ import annotations

from varco_fastapi.webhook.inbound import VerifiedWebhook, verify_webhook
from varco_fastapi.webhook.mount import mount_webhook_admin
from varco_fastapi.webhook.router import build_webhook_router

__all__ = [
    "mount_webhook_admin",
    "build_webhook_router",
    "verify_webhook",
    "VerifiedWebhook",
]
