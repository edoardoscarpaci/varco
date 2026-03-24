"""
varco_core.service
=====================
Async service base, tenant-aware service variant, and outbox pattern.
"""

from varco_core.service.base import AsyncService, IUoWProvider
from varco_core.service.outbox import OutboxEntry, OutboxRelay, OutboxRepository
from varco_core.service.tenant import (
    TenantAwareService,
    TenantUoWProvider,
    current_tenant,
    tenant_context,
)
from varco_core.service.validation import ValidatorServiceMixin

__all__ = [
    "AsyncService",
    "IUoWProvider",
    # ── Outbox pattern ────────────────────────────────────────────────────────
    "OutboxEntry",
    "OutboxRepository",
    "OutboxRelay",
    # ── Tenant-aware service ──────────────────────────────────────────────────
    "TenantAwareService",
    "TenantUoWProvider",
    "current_tenant",
    "tenant_context",
    "ValidatorServiceMixin",
]
