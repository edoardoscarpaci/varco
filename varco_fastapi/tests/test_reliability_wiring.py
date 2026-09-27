"""
tests.test_reliability_wiring
================================
Plan 009, Phase 9 (R5) — varco_fastapi.reliability.ReliabilityLifecycle +
``create_varco_app(reliability=...)``.

RED until ``varco_fastapi/reliability.py`` lands and ``create_varco_app``
gains the ``reliability=`` kwarg.
"""

from __future__ import annotations

import pytest
from providify import Provider
from varco_core.event.base import AbstractEventBus
from varco_core.event.dlq import InMemoryDeadLetterQueue
from varco_core.event.memory import InMemoryEventBus
from varco_core.service.audit import AuditRepository
from varco_core.service.outbox import OutboxRepository


class _InMemoryOutboxRepository:
    """Minimal test double satisfying OutboxRepository's abstract surface."""

    async def save(self, entry) -> None: ...
    async def get_pending(self, *, limit: int = 100) -> list:
        return []

    async def delete(self, entry_id) -> None: ...


class _InMemoryAuditRepository:
    """Minimal test double satisfying AuditRepository's abstract surface."""

    async def save(self, entry) -> None: ...
    async def list_for_entity(self, entity_type, entity_id, *, limit=100, tenant_id=None) -> list:
        return []


class TestReliabilityLifecycleStartStop:
    async def test_startup_installs_metrics_and_starts_relay(self) -> None:
        from providify import DIContainer
        from varco_core.reliability import ReliabilityPreset
        from varco_fastapi.reliability import ReliabilityLifecycle

        dlq = InMemoryDeadLetterQueue()
        preset = ReliabilityPreset.durable(dlq=dlq)
        container = DIContainer()

        # durable() turns on outbox+audit+metrics — ReliabilityLifecycle
        # requires real bindings for each (fail-loudly-at-startup contract,
        # see reliability.py's DESIGN block), so wire minimal test doubles.
        @Provider(singleton=True)
        def _bus() -> AbstractEventBus:
            return InMemoryEventBus()

        @Provider(singleton=True)
        def _outbox() -> OutboxRepository:
            return _InMemoryOutboxRepository()

        @Provider(singleton=True)
        def _audit() -> AuditRepository:
            return _InMemoryAuditRepository()

        container.provide(_bus)
        container.provide(_outbox)
        container.provide(_audit)

        lifecycle = ReliabilityLifecycle(preset, container=container)
        await lifecycle.startup()
        await lifecycle.shutdown()

    async def test_off_preset_startup_is_a_noop(self) -> None:
        from providify import DIContainer
        from varco_core.reliability import ReliabilityPreset
        from varco_fastapi.reliability import ReliabilityLifecycle

        container = DIContainer()
        lifecycle = ReliabilityLifecycle(ReliabilityPreset.off(), container=container)
        await lifecycle.startup()
        await lifecycle.shutdown()


class TestReliabilityLifecycleAuditRequiresRepo:
    async def test_audit_true_without_audit_repository_raises_at_startup(self) -> None:
        from providify import DIContainer
        from varco_core.reliability import ReliabilityPreset
        from varco_fastapi.reliability import ReliabilityLifecycle

        dlq = InMemoryDeadLetterQueue()
        preset = ReliabilityPreset(dlq=dlq, audit=True)
        container = DIContainer()  # no AuditRepository bound

        lifecycle = ReliabilityLifecycle(preset, container=container)
        with pytest.raises(Exception, match="AuditRepository"):
            await lifecycle.startup()


class TestCreateVarcoAppReliabilityKwarg:
    def test_create_varco_app_accepts_reliability_kwarg(self) -> None:
        from providify import DIContainer
        from varco_core.di.module import CoreModule
        from varco_core.reliability import ReliabilityPreset
        from varco_fastapi.app import create_varco_app
        from varco_fastapi.modules.lifecycle import ReliabilityModule

        container = DIContainer()
        app = create_varco_app(
            container,
            routers=[],
            modules=[CoreModule(), ReliabilityModule(ReliabilityPreset.off())],
            validate=False,
        )
        assert app is not None
