"""
varco_plugin_example
=======================
Plan 051 (DI-5) / §D-plugin — a third-party-shaped example package proving
that a package which never imports ``varco_fastapi`` (or any other
``varco_*`` package beyond ``varco_core``) can contribute to every
``varco_core`` collection seam with plain ``container.scan()`` + an
ordinary ``@Singleton``/``@Configuration`` — **no** ``bind_*`` verb, no
framework edit.

Never packaged, never published — reached only via each participating
package's ``pythonpath = ["../testkit"]`` pytest setting, the same
``testkit/`` convention every other conformance/chaos helper uses.

Four seams, one package:

- ``PartnerHeaderTenantSource`` — a ``TenantSource`` (``varco_core.tenancy.source``).
- ``PluginPostureCollector`` — a ``PostureCollector`` (``varco_core.posture``).
- ``PluginConsumer`` — an ``EventConsumer`` (``varco_core.event.consumer``).
- ``PluginRetention`` — a ``@Configuration`` contributing a ``RetentionPolicy``
  (``varco_core.retention.policy``), its target's ``AbstractDeadLetterQueue``
  *injected* by DI.

``varco_core.di.bootstrap(c); c.scan("varco_plugin_example")`` is all it
takes for every one of these to become collectible via
``collect()``/``acollect()`` — proved, with an empty ``git diff
varco_*/varco_*``, by ``varco_core/tests/test_plugin_contributions.py`` and
``varco_fastapi/tests/test_plugin_posture_contribution.py``.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from providify import Configuration, Inject, Provider, Singleton
from varco_core.event.base import Event
from varco_core.event.consumer import EventConsumer, listen
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_core.posture import PostureCollector, PostureFinding, PostureSeverity
from varco_core.retention.policy import RetentionPolicy
from varco_core.retention.targets import DlqRetentionTarget
from varco_core.tenancy.source import TenantClaim, TenantSource, TenantTrust

if TYPE_CHECKING:
    from varco_core.tenancy.source import TenantRequest

__all__ = [
    "PartnerHeaderTenantSource",
    "PluginPostureCollector",
    "PluginEvent",
    "PluginConsumer",
    "PluginRetention",
]


class PluginEvent(Event):
    """A trivial domain event the example consumer subscribes to."""


@Singleton
class PartnerHeaderTenantSource(TenantSource):
    """
    An out-of-tree ``TenantSource`` — a plain scanned ``@Singleton``
    subclass, no ``bind_*`` call needed. Inert unless named in
    ``VARCO_TENANT_SOURCES`` (bound ≠ active, §D-tenant).
    """

    name = "partner"
    trust = TenantTrust.MEDIUM

    def resolve(self, request: TenantRequest) -> TenantClaim | None:
        tenant_id = request.headers.get("x-partner-tenant")
        if tenant_id is None:
            return None
        return TenantClaim(tenant_id=tenant_id, source=self.name, trust=self.trust)


@Singleton
class PluginPostureCollector(PostureCollector):
    """An out-of-tree ``PostureCollector`` — proves a plugin never has to
    import ``varco_fastapi`` to contribute a posture finding."""

    name = "plugin"

    def collect(self) -> list[PostureFinding]:
        return [
            PostureFinding(
                check="plugin.example",
                severity=PostureSeverity.INFO,
                detail="varco_plugin_example's PluginPostureCollector ran.",
                remediation="No action required — this is the plugin proof collector.",
            )
        ]


@Singleton
class PluginConsumer(EventConsumer):
    """An out-of-tree ``EventConsumer`` — a plain scanned ``@Singleton``,
    auto-registered by ``CoreModule.start()`` with no ``@PostConstruct``."""

    def __init__(self) -> None:
        self.received: list[PluginEvent] = []

    @listen(PluginEvent, channel="plugin")
    async def on_plugin_event(self, event: PluginEvent) -> None:
        self.received.append(event)


@Configuration
class PluginRetention:
    """
    An out-of-tree ``RetentionPolicy`` contribution — a scanned
    ``@Configuration`` whose ``@Provider`` method has its target's
    ``AbstractDeadLetterQueue`` *injected* by DI (the DI-10 direction Plan
    050 wants for hand-constructed infra).
    """

    @Provider(singleton=True, returns=RetentionPolicy)
    def policy(self, dlq: Inject[AbstractDeadLetterQueue]) -> RetentionPolicy:
        return RetentionPolicy(
            name="plugin-dlq",
            target=DlqRetentionTarget(dlq=dlq, acknowledge_dead_letter_deletion=True),
            cron_expr="0 3 * * *",
            timezone="UTC",
            dry_run=True,
            older_than=timedelta(days=30),
        )
