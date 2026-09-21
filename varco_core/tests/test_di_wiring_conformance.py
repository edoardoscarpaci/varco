"""
Plan 046 / Step 3 — `varco_core`'s `DIWiringConformance` subclass (§D-suite).

`expected_defaults` copied from 045's landed `test_fallback_defaults.py`
(A1-A13, Step 0(e)).

RED MODE: `varco_core.di.bootstrap` does not exist yet (Step 1 adds it) —
every test in the inherited suite is expected to fail with
`AttributeError`/`ImportError`.
"""

from __future__ import annotations

import pytest
from providify import DIContainer, IssueKind
from varco_conformance.di_wiring import DIWiringConformance
from varco_core.auth.authorizer import AbstractAuthorizer
from varco_core.cache.base import CacheBackend
from varco_core.event import AbstractEventBus
from varco_core.event.base import Event
from varco_core.event.deduplication import AbstractDeduplicator
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_core.event.producer import AbstractEventProducer
from varco_core.event.serializer import Serializer
from varco_core.flags import AbstractFeatureFlags
from varco_core.lock import AbstractDistributedLock
from varco_core.observability import OtelConfig
from varco_core.revocation import AbstractTokenRevocationStore
from varco_core.service.conversation import AbstractConversationStore
from varco_core.tenancy.membership import AbstractTenantMembership


class TestVarcoCoreDIWiringConformance(DIWiringConformance):
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
    )

    # InMemoryEventBus.__init__'s error_policy/dispatch_mode params have no
    # matching binding — a settings-shaped gap Plan 048's bind_settings()
    # recipe is expected to close (§Risks: "048 for a settings
    # MISSING_BINDING_DEFAULTED"), not something this plan fixes.
    known_warnings = frozenset(
        {
            (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
        }
    )

    @pytest.fixture
    def bootstrap(self):
        def _bootstrap(container: DIContainer) -> DIContainer:
            from varco_core.di import bootstrap as core_bootstrap

            return core_bootstrap(container)

        return _bootstrap
