"""
varco_core.service
=====================
Async service base and tenant-aware service variant.
"""

from varco_core.service.base import AsyncService, IUoWProvider
from varco_core.service.tenant import (
    TenantAwareService,
    TenantUoWProvider,
    current_tenant,
    tenant_context,
)

__all__ = [
    "AsyncService",
    "IUoWProvider",
    "TenantAwareService",
    "TenantUoWProvider",
    "current_tenant",
    "tenant_context",
]
