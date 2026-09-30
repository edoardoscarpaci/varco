"""
Plan 046 / Step 4 — `varco_ws`'s `DIWiringConformance` subclass (§D-suite),
plus `bind_websocket_adapter`/`bind_sse_adapter` `qualifier=` coverage.

`expected_defaults` measured directly (Step 0(d) preflight): D1-2 —
`WebSocketEventBus`/`SSEEventBus` are self-bound fallbacks (scanned
`@Singleton` classes with no ABC of their own). Core supplies
`AbstractEventBus` (`InMemoryEventBus`) so `validate()` is clean without a
real bus.

RED MODE: `varco_core.di.bootstrap` does not exist yet — every test in the
inherited suite is expected to fail with `ImportError`. The qualifier tests
fail because `bind_websocket_adapter`/`bind_sse_adapter` do not yet accept a
`qualifier=` kwarg and do not raise `AlreadyBoundError` on a repeat call —
they still silently "replace" (append + first-registered-wins).
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
from varco_core.idempotency.base import AbstractIdempotencyStore
from varco_core.job.base import AbstractJobStore
from varco_core.lock import AbstractDistributedLock
from varco_core.observability import OtelConfig
from varco_core.revocation import AbstractTokenRevocationStore
from varco_core.service.conversation import AbstractConversationStore
from varco_core.tenancy.membership import AbstractTenantMembership
from varco_ws.sse import SSEEventBus
from varco_ws.websocket import WebSocketEventBus

CORE_DEFAULTS = (
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
)


from varco_ws.modules import WsModule  # noqa: E402


class TestVarcoWsDIWiringConformance(DIWiringConformance):
    module_cls = WsModule

    # Plan 050 / §D-suite: WsModule.start()/stop() owns the two push
    # adapters' lifecycle directly (never via @Disposes/@PreDestroy) — see
    # WsModule's own docstring. Declared here so a future
    # bind_websocket_adapter()/bind_sse_adapter() call in this suite's
    # bootstrap fixture is never mistaken for an orphaned disposer.
    module_owned = frozenset({WebSocketEventBus, SSEEventBus})

    expected_defaults = CORE_DEFAULTS + (
        (WebSocketEventBus, None),
        (SSEEventBus, None),
    )

    # Same core gap as TestVarcoCoreDIWiringConformance — Plan 048's
    # bind_settings() is expected to close it, not this plan.
    known_warnings = frozenset(
        {
            (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
            # SSEEventBus/WebSocketEventBus's scanned @Singleton constructors
            # request event_type/channel/max_queue_size/backpressure_policy
            # with no matching binding — a settings-shaped gap Plan 048's
            # bind_settings() recipe is expected to close, not this plan.
            (IssueKind.MISSING_BINDING_DEFAULTED, "SSEEventBus.__init__"),
            (IssueKind.MISSING_BINDING_DEFAULTED, "WebSocketEventBus.__init__"),
        }
    )

    @pytest.fixture
    def bootstrap(self):
        def _bootstrap(container: DIContainer) -> DIContainer:
            from varco_core.di import bootstrap as core_bootstrap
            from varco_ws.di import bootstrap as ws_bootstrap

            core_bootstrap(container)
            ws_bootstrap(container)
            return container

        return _bootstrap


class TestBindWebsocketAdapterQualifier:
    """§D-bind — `bind_websocket_adapter`/`bind_sse_adapter` return the
    container, gain `qualifier=`, and raise `AlreadyBoundError` on an
    unqualified repeat."""

    def test_bind_websocket_adapter_returns_the_container(self) -> None:
        from varco_core.di import bootstrap as core_bootstrap
        from varco_ws.di import bind_websocket_adapter

        container = DIContainer()
        core_bootstrap(container)

        result = bind_websocket_adapter(container)

        assert result is container

    def test_bind_websocket_adapter_twice_unqualified_raises(self) -> None:
        from varco_core.di import AlreadyBoundError
        from varco_core.di import bootstrap as core_bootstrap
        from varco_ws.di import bind_websocket_adapter

        container = DIContainer()
        core_bootstrap(container)
        bind_websocket_adapter(container)

        with pytest.raises(AlreadyBoundError):
            bind_websocket_adapter(container)

    def test_bind_websocket_adapter_twice_qualified_binds_two(self) -> None:
        from varco_core.di import bootstrap as core_bootstrap
        from varco_ws.di import bind_websocket_adapter
        from varco_ws.websocket import WebSocketEventBus

        container = DIContainer()
        core_bootstrap(container)
        bind_websocket_adapter(container, qualifier="orders", channel="orders")
        bind_websocket_adapter(container, qualifier="users", channel="users")

        orders = container.get(WebSocketEventBus, qualifier="orders")
        users = container.get(WebSocketEventBus, qualifier="users")
        assert orders is not users

    def test_bind_sse_adapter_returns_the_container(self) -> None:
        from varco_core.di import bootstrap as core_bootstrap
        from varco_ws.di import bind_sse_adapter

        container = DIContainer()
        core_bootstrap(container)

        result = bind_sse_adapter(container)

        assert result is container

    def test_bind_sse_adapter_twice_unqualified_raises(self) -> None:
        from varco_core.di import AlreadyBoundError
        from varco_core.di import bootstrap as core_bootstrap
        from varco_ws.di import bind_sse_adapter

        container = DIContainer()
        core_bootstrap(container)
        bind_sse_adapter(container)

        with pytest.raises(AlreadyBoundError):
            bind_sse_adapter(container)

    def test_bind_sse_adapter_twice_qualified_binds_two(self) -> None:
        from varco_core.di import bootstrap as core_bootstrap
        from varco_ws.di import bind_sse_adapter
        from varco_ws.sse import SSEEventBus

        container = DIContainer()
        core_bootstrap(container)
        bind_sse_adapter(container, qualifier="orders", channel="orders")
        bind_sse_adapter(container, qualifier="users", channel="users")

        orders = container.get(SSEEventBus, qualifier="orders")
        users = container.get(SSEEventBus, qualifier="users")
        assert orders is not users
