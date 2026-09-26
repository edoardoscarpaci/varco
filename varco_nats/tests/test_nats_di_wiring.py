"""
Plan 046 / Step 4 — `varco_nats`'s `DIWiringConformance` subclass (§D-suite).

Absorbs 045's `test_nats_fallback_tier.py` (B1's fallback-tier assertions
are now `test_declared_defaults_are_fallbacks`/
`test_every_default_is_overridable_after_bootstrap`) and
`test_validate_clean_scan.py` (folded into `test_validate_is_clean`'s
`known_warnings`) — both are deleted by this step.
"""

from __future__ import annotations

import pytest
from providify import DIContainer, IssueKind
from varco_conformance.di_wiring import DIWiringConformance, Selectable
from varco_core.auth.authorizer import AbstractAuthorizer
from varco_core.cache.base import CacheBackend
from varco_core.event import AbstractEventBus
from varco_core.event.base import Event
from varco_core.event.channel import ChannelManager
from varco_core.event.deduplication import AbstractDeduplicator
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_core.event.producer import AbstractEventProducer
from varco_core.event.serializer import Serializer
from varco_core.flags import AbstractFeatureFlags
from varco_core.idempotency.base import AbstractIdempotencyStore
from varco_core.job.base import AbstractJobStore
from varco_core.lock import AbstractDistributedLock
from varco_core.observability import OtelConfig
from varco_core.revocation import AbstractTokenRevocationStore
from varco_core.service.conversation import AbstractConversationStore
from varco_core.tenancy.membership import AbstractTenantMembership
from varco_nats.channel import NatsChannelManagerSettings
from varco_nats.config import NatsEventBusSettings


class TestVarcoNatsDIWiringConformance(DIWiringConformance):
    settings_classes = (NatsEventBusSettings, NatsChannelManagerSettings)
    expected_defaults = (
        (CacheBackend, "in_memory"),
        (AbstractEventBus, "in_memory"),
        (AbstractDeadLetterQueue, "in_memory"),
        (AbstractDeduplicator, "in_memory"),
        (AbstractDistributedLock, "in_memory"),
        (AbstractConversationStore, "in_memory"),
        (AbstractFeatureFlags, None),
        (AbstractTokenRevocationStore, None),
        (AbstractTenantMembership, None),
        (AbstractEventProducer, None),
        (Serializer[Event], None),
        (AbstractAuthorizer, None),
        (OtelConfig, None),
        # Plan 047 §D-vocab — the idempotency/job_store seam defaults.
        (AbstractIdempotencyStore, "in_memory"),
        (AbstractJobStore, "in_memory"),
        (AbstractEventBus, "nats"),
        (ChannelManager, "nats"),
        (NatsChannelManagerSettings, None),
        (NatsEventBusSettings, None),
        (AbstractDeadLetterQueue, None),
    )

    # Plan 047 §D-suite — gated backend bindings this package serves.
    selectable = (
        Selectable("event_bus", "nats", AbstractEventBus, "nats", "NatsEventBus"),
        Selectable("dlq", "nats", AbstractDeadLetterQueue, None, "dlq"),
    )

    known_warnings = frozenset(
        {
            # Settings-shaped gaps — Plan 048's bind_settings() closes these,
            # not this plan (§Risks: "048 for a settings MISSING_BINDING_DEFAULTED").
            (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
            (IssueKind.MISSING_BINDING_DEFAULTED, "NatsEventBus.__init__"),
        }
    )

    @pytest.fixture
    def bootstrap(self):
        def _bootstrap(container: DIContainer) -> DIContainer:
            from varco_core.di import bootstrap as core_bootstrap
            from varco_nats.di import bootstrap as nats_bootstrap

            core_bootstrap(container)
            nats_bootstrap(container)
            return container

        return _bootstrap
