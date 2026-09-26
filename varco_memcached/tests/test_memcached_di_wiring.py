"""
Plan 046 / Step 6 — `varco_memcached`'s `DIWiringConformance` subclass
(§D-suite).

`async_bootstrap` is **not** exercised here — it opens a real connection
pool (`bootstrap` fixture only calls the sync `bootstrap()`), matching the
plan's Docker-free rule for this suite.
"""

from __future__ import annotations

import pytest
from providify import DIContainer, IssueKind
from varco_conformance.di_wiring import DIWiringConformance, Selectable
from varco_core.auth.authorizer import AbstractAuthorizer
from varco_core.cache.base import CacheBackend
from varco_core.event import AbstractEventBus
from varco_core.event.base import Event
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
from varco_memcached.cache import MemcachedCacheSettings


class TestVarcoMemcachedDIWiringConformance(DIWiringConformance):
    settings_classes = (MemcachedCacheSettings,)
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
        (MemcachedCacheSettings, None),
        (CacheBackend, None),
    )

    # Plan 047 §D-suite.
    selectable = (Selectable("cache", "memcached", CacheBackend, None, "memcached_cache"),)

    known_warnings = frozenset(
        {
            (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
        }
    )

    @pytest.fixture
    def bootstrap(self):
        def _bootstrap(container: DIContainer) -> DIContainer:
            from varco_core.di import bootstrap as core_bootstrap
            from varco_memcached.di import bootstrap as memcached_bootstrap

            core_bootstrap(container)
            memcached_bootstrap(container)
            return container

        return _bootstrap
