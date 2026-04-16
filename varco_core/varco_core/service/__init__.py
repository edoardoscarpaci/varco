"""
varco_core.service
=====================
Async service base, tenant-aware service variant, and outbox pattern.
"""

from varco_core.service.base import AsyncService, IUoWProvider
from varco_core.service.conversation import (
    AbstractConversationStore,
    ConversationTurn,
    InMemoryConversationStore,
)
from varco_core.service.mixin import ServiceMixin
from varco_core.service.outbox import OutboxEntry, OutboxRelay, OutboxRepository
from varco_core.service.tenant import (
    TenantAwareService,
    TenantUoWProvider,
    current_tenant,
    tenant_context,
)
from varco_core.service.async_validation import AsyncValidatorServiceMixin
from varco_core.service.validation import ValidatorServiceMixin

__all__ = [
    "AsyncService",
    "IUoWProvider",
    "ServiceMixin",
    # ── Conversation store ────────────────────────────────────────────────────
    "AbstractConversationStore",
    "ConversationTurn",
    "InMemoryConversationStore",
    # ── Outbox pattern ────────────────────────────────────────────────────────
    "OutboxEntry",
    "OutboxRepository",
    "OutboxRelay",
    # ── Tenant-aware service ──────────────────────────────────────────────────
    "TenantAwareService",
    "TenantUoWProvider",
    "current_tenant",
    "tenant_context",
    # ── Validator mixins ──────────────────────────────────────────────────────
    "ValidatorServiceMixin",
    "AsyncValidatorServiceMixin",
]
