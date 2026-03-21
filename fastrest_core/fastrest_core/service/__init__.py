"""
fastrest_core.service
=====================
Async service base and tenant-aware service variant.
"""

from fastrest_core.service.base import AsyncService, IUoWProvider
from fastrest_core.service.tenant import (
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
