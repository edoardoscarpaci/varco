"""
Plan 051 (DI-5) / §D-plugin — the third-party contribution story, proved
in-tree via ``testkit/varco_plugin_example`` (never packaged).

Every assertion here exercises ``varco_core.di.bootstrap(c)`` +
``c.scan("varco_plugin_example")`` — no ``bind_*``/framework edit of any
kind. This file is the "no varco_* edit was needed" proof; see the plan's
Step 11 verify command (``git diff --stat varco_*/varco_*`` for this step
is empty).
"""

from __future__ import annotations

from datetime import timedelta

from providify import DIContainer
from varco_core.di import bootstrap
from varco_core.di.collect import acollect, collect
from varco_core.event.consumer import EventConsumer
from varco_core.posture import PostureCollector
from varco_core.retention.policy import RetentionPolicy
from varco_core.tenancy.settings import TenantProvenanceSettings, build_tenant_source_chain
from varco_core.tenancy.source import TenantSource


def _scanned_container() -> DIContainer:
    container = DIContainer()
    bootstrap(container)
    container.scan("varco_plugin_example")
    return container


class TestPluginTenantSourceContribution:
    def test_partner_source_is_collectible(self) -> None:
        container = _scanned_container()

        pool = collect(container, TenantSource)
        assert any(s.name == "partner" for s in pool)

    def test_partner_source_joins_chain_when_named(self) -> None:
        container = _scanned_container()
        pool = collect(container, TenantSource)

        settings = TenantProvenanceSettings(sources=("jwt", "partner"))
        chain = build_tenant_source_chain(settings, sources=pool)

        assert chain is not None
        assert [s.name for s in chain.sources] == ["jwt", "partner"]


class TestPluginRetentionContribution:
    async def test_plugin_dlq_policy_is_collectible(self) -> None:
        container = _scanned_container()

        policies = await acollect(container, RetentionPolicy)
        names = {p.name for p in policies}
        assert "plugin-dlq" in names
        plugin_policy = next(p for p in policies if p.name == "plugin-dlq")
        assert plugin_policy.older_than == timedelta(days=30)
        assert plugin_policy.dry_run is True


class TestPluginPostureContribution:
    def test_plugin_collector_is_collectible(self) -> None:
        container = _scanned_container()

        collectors = collect(container, PostureCollector)
        assert any(c.name == "plugin" for c in collectors)

        plugin_collector = next(c for c in collectors if c.name == "plugin")
        findings = plugin_collector.collect()
        assert any(f.check == "plugin.example" for f in findings)


class TestPluginEventConsumerContribution:
    async def test_plugin_consumer_is_collectible_and_auto_registered(self) -> None:
        from varco_core.di.module import CoreModule
        from varco_core.event.base import AbstractEventBus

        container = _scanned_container()
        core = CoreModule()
        core.register(container)

        await core.start(container)

        consumers = await acollect(container, EventConsumer)
        plugin_consumers = [c for c in consumers if type(c).__name__ == "PluginConsumer"]
        assert len(plugin_consumers) == 1

        bus = container.get(AbstractEventBus)
        assert plugin_consumers[0].is_registered_to(bus) is True
