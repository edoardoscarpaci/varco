"""
varco_core.tenancy.control.service
=====================================
``TenantControlService`` — the tenant onboarding/offboarding orchestrator
(Plan 007, Phase 5, step 1-2; extended by Plan 008, Phase 2, RD-14/RD-16/
RD-17). Reachable from both the REST admin surface (``build_tenant_router``)
and the event-driven consumer (``TenantProvisionConsumer``).

DESIGN: idempotent ``provision()`` — the ONLY new-mechanism-free idempotency
    A second ``provision()`` call on an already-``ACTIVE`` tenant performs
    **no** provisioner call — status is the idempotency check, no new dedup
    mechanism is invented (RD-1). A provisioner failure leaves the status
    at whatever it was (``PENDING`` on first provision) and re-raises —
    never a half-``active`` tenant.

DESIGN: RD-14 — ``request_provision()``/``request_deprovision()`` are
broadcast-only and explicitly do NOT include the caller
    They emit the command and do nothing else: no catalog write, no local
    DDL. A node that must also provision itself calls ``provision()``
    **first** (so a local failure surfaces synchronously), then broadcasts.
    ✅ Matches RD-4/RD-9: the standalone control plane holds the admin DSN
       and is not an app pod, so "ask the fleet" and "do it here" are
       genuinely different operations.
    ✅ Ordering (local first, broadcast second) gives the operator a
       synchronous error before the fleet is told.
    ❌ A bundled node that calls only ``request_provision()`` never
       provisions itself — documented on both methods.

DESIGN: RD-16 — under fan-out the catalog has one authority; workers do
not write it
    ``catalog_authority=False`` (worker mode): ``provision()`` runs local
    DDL, emits ``TenantNodeReady``, and performs **no** catalog write.
    ✅ Kills the premature-``ACTIVE`` race a multi-consumer fleet would
       otherwise hit under Phase 1 alone — with N workers each writing the
       shared catalog, the first to finish flips ``ACTIVE`` while the rest
       are still provisioning.
    ✅ A worker still *reads* the catalog and refuses to provision a
       ``DELETED``/``DEPROVISIONING`` tenant — a replayed old command
       cannot resurrect a deleted tenant.
    ❌ Worker idempotency no longer comes from the status check; it comes
       from the provisioner's own ``IF NOT EXISTS`` semantics plus the
       consumer's event dedup.
    ❌ ``catalog_authority=False`` with no coordinator and no manual
       terminator leaves the tenant ``PENDING`` forever — mitigated by a
       WARNING at construction and by shipping both terminators (Phase 3
       coordinator, manual ``POST …/activate``).
"""

from __future__ import annotations

import logging
import os
import socket
from typing import TYPE_CHECKING, Any

from varco_core.tenancy.catalog import TenantNotFoundError
from varco_core.tenancy.control.events import (
    CHANNEL_TENANCY,
    TenantCatalogChanged,
    TenantDeprovisionRequested,
    TenantNodeReady,
    TenantProvisionRequested,
)
from varco_core.tenancy.provisioner import DestructiveOperationRefused
from varco_core.tenancy.settings import TenantStatus

if TYPE_CHECKING:
    from varco_core.event.producer import AbstractEventProducer
    from varco_core.tenancy.catalog import AbstractTenantCatalog, TenantDescriptor
    from varco_core.tenancy.provisioner import AbstractTenantProvisioner

logger = logging.getLogger(__name__)


class TenantControlService:
    """
    Orchestrates tenant provisioning/deprovisioning/suspend/resume, and
    (Plan 008) fleet broadcast + worker-mode provisioning.

    Args:
        catalog:     The durable ``AbstractTenantCatalog``.
        provisioner: The storage provisioner (``ExternalTenantProvisioner``,
                     ``SASchemaProvisioner``, ``SADatabaseProvisioner``, ...).
        producer:    ``AbstractEventProducer`` used to emit
                     ``TenantCatalogChanged``/``TenantNodeReady`` after every
                     transition, and to broadcast commands from
                     ``request_provision``/``request_deprovision``.
                     **Required** — a service that cannot emit is a
                     construction-time error, not a per-call one.
        supervisor:  Optional ``TenantFanoutSupervisor``-shaped object with
                     ``on_tenant_deactivated(tenant_id)`` — stopped before
                     destructive deprovision DDL. ``None`` (default) — no
                     fan-out wired.
        pool:        Optional resource-pool-shaped object with
                     ``evict(tenant_id)`` — evicted before destructive
                     deprovision DDL. ``None`` (default) — no pool wired.
        node_id:     Stable identifier for this process, stamped as
                     ``origin`` on broadcast commands (RD-15) and as
                     ``TenantNodeReady.node_id`` in worker mode. Defaults to
                     ``VARCO_TENANCY_NODE_ID`` or a process-stable
                     ``f"{hostname}:{pid}"``.
        store_id:    The logical store this node/service owns (RD-17 — a
                     store, not a pod). Defaults to
                     ``VARCO_TENANCY_STORE_ID``. Only meaningful under
                     ``catalog_authority=False``.
        catalog_authority: ``True`` (default) — today's behaviour,
                     byte-identical: this service is the single catalog
                     writer. ``False`` — worker mode (RD-16): ``provision()``
                     never writes the catalog; it emits ``TenantNodeReady``
                     instead. Logs one WARNING at construction naming the
                     required terminator (a ``TenantReadinessCoordinator``
                     or the manual ``POST …/activate`` route).

    Raises:
        RuntimeError: ``producer`` is ``None`` — this service can never
            emit ``TenantCatalogChanged``/broadcast commands.
    """

    def __init__(
        self,
        *,
        catalog: AbstractTenantCatalog,
        provisioner: AbstractTenantProvisioner,
        producer: AbstractEventProducer | None,
        supervisor: Any | None = None,
        pool: Any | None = None,
        node_id: str | None = None,
        store_id: str | None = None,
        catalog_authority: bool = True,
    ) -> None:
        if producer is None:
            raise RuntimeError(
                "TenantControlService requires producer= — it cannot emit "
                "TenantCatalogChanged/TenantNodeReady or broadcast "
                "request_provision()/request_deprovision() commands without one."
            )
        self._catalog = catalog
        self._provisioner = provisioner
        self._producer = producer
        self._supervisor = supervisor
        self._pool = pool

        # Plan 048 / §D-loose: VARCO_TENANCY_NODE_ID/STORE_ID are now fields
        # on TenancySettings rather than loose os.environ.get() reads —
        # constructing TenancySettings() here is equivalent (same env
        # names, same fallback-to-None-or-hostname behaviour) and gives
        # 052's `varco diagnose` one settings object to introspect instead
        # of an invisible read buried in this constructor.
        from varco_core.tenancy.settings import TenancySettings

        _settings = TenancySettings()
        self.node_id: str = node_id or _settings.node_id or f"{socket.gethostname()}:{os.getpid()}"
        self.store_id: str | None = store_id or _settings.store_id
        self.catalog_authority: bool = catalog_authority

        if not catalog_authority:
            logger.warning(
                "TenantControlService(node_id=%r) constructed with "
                "catalog_authority=False (worker mode) — this service will "
                "never flip a tenant to ACTIVE on its own. Wire a "
                "TenantReadinessCoordinator, or a terminator will never run "
                "and tenants will stay PENDING forever.",
                self.node_id,
            )

    async def provision(self, tenant_id: str, **kwargs: object) -> TenantDescriptor:
        """
        Provision ``tenant_id``.

        Under ``catalog_authority=True`` (default): idempotent — a no-op if
        already ``ACTIVE``; drives ``PENDING -> ACTIVE`` and emits
        ``TenantCatalogChanged``.

        Under ``catalog_authority=False`` (worker mode, RD-16): reads the
        catalog to refuse a ``DELETED``/``DEPROVISIONING`` tenant, always
        calls the provisioner (idempotency is the provisioner's own
        ``IF NOT EXISTS`` responsibility here, not a status check), and
        emits ``TenantNodeReady`` — never writes the catalog.

        Returns:
            The tenant's descriptor **after** the transition. The REST
            surface (``POST /tenancy/tenants``) renders this directly, so
            the return value is part of the control-plane contract, not a
            convenience — callers that only care about the side effect
            (``TenantProvisionConsumer``) may ignore it.

        Raises:
            Exception: Whatever the provisioner raises — the authority path
                leaves status as-is (never advanced to ``ACTIVE`` on failure)
                and the error propagates for the caller (REST handler /
                consumer retry loop) to handle.
            Exception: Whatever the catalog raises on ``get()`` other
                than ``TenantNotFoundError`` — a catalog **outage** must not be
                mistaken for an unknown tenant and silently become
                ``add(PENDING)``.

        Edge cases:
            - Already ``ACTIVE`` → the existing descriptor is returned
              unchanged; no provisioner call, no event.
            - Worker mode on a ``DELETED``/``DEPROVISIONING`` tenant → the
              refused descriptor is returned as-is (nothing was run).
            - Worker mode on a tenant the catalog has never heard of → a
              synthesized ``PENDING`` descriptor is returned; a worker never
              writes the catalog, so nothing is persisted (RD-16).
        """
        if not self.catalog_authority:
            return await self._provision_worker(tenant_id, **kwargs)

        try:
            descriptor = await self._catalog.get(tenant_id)
        except TenantNotFoundError:
            from varco_core.tenancy.catalog import TenantDescriptor

            descriptor = TenantDescriptor(tenant_id=tenant_id, status=TenantStatus.PENDING)
            await self._catalog.add(descriptor)

        if descriptor.status == TenantStatus.ACTIVE:
            return descriptor

        await self._provisioner.provision(tenant_id, **kwargs)
        await self._catalog.update_status(tenant_id, TenantStatus.ACTIVE)
        await self._emit_catalog_changed(tenant_id)
        # Re-read rather than dataclasses.replace(): a durable catalog's
        # provisioning step may have filled in schema/database/dsn_ref, and
        # the REST response must show what was actually persisted.
        return await self._catalog.get(tenant_id)

    async def _provision_worker(self, tenant_id: str, **kwargs: object) -> TenantDescriptor:
        from varco_core.tenancy.catalog import TenantDescriptor

        try:
            descriptor = await self._catalog.get(tenant_id)
        except TenantNotFoundError:
            descriptor = None

        if descriptor is not None and descriptor.status in (
            TenantStatus.DELETED,
            TenantStatus.DEPROVISIONING,
        ):
            logger.warning(
                "Worker node %r refusing to provision tenant %r: catalog "
                "status is %r (replayed-command safety).",
                self.node_id,
                tenant_id,
                descriptor.status.value,
            )
            return descriptor

        await self._provisioner.provision(tenant_id, **kwargs)
        await self._producer._produce(
            TenantNodeReady(
                tenant_id=tenant_id,
                node_id=self.node_id,
                store_id=self.store_id,  # type: ignore[arg-type]
            ),
            channel=CHANNEL_TENANCY,
        )
        # A worker never writes the catalog (RD-16), so the descriptor it
        # returns is the pre-existing row, or a synthesized PENDING one when
        # the authority has not created the row yet. Deliberately NOT
        # persisted — the terminator (coordinator / POST …/activate) owns
        # the transition to ACTIVE.
        return descriptor or TenantDescriptor(tenant_id=tenant_id, status=TenantStatus.PENDING)

    async def deprovision(self, tenant_id: str, *, confirm: bool = False) -> None:
        """
        Deprovision ``tenant_id``: ``active -> deprovisioning -> deleted``.

        Stops the tenant's fan-out children and evicts its pool entry
        **before** the destructive provisioner call.

        Raises:
            DestructiveOperationRefused: ``confirm`` is not ``True``.
        """
        if not confirm:
            raise DestructiveOperationRefused(
                f"Refusing to deprovision tenant {tenant_id!r} without confirm=True."
            )

        await self._catalog.update_status(tenant_id, TenantStatus.DEPROVISIONING)
        await self._emit_catalog_changed(tenant_id)

        if self._supervisor is not None:
            stop = getattr(self._supervisor, "on_tenant_deactivated", None)
            if stop is not None:
                await stop(tenant_id)
        if self._pool is not None:
            evict = getattr(self._pool, "evict", None)
            if evict is not None:
                await evict(tenant_id)

        await self._provisioner.deprovision(tenant_id, confirm_destroy=True)

        await self._catalog.update_status(tenant_id, TenantStatus.DELETED)
        await self._emit_catalog_changed(tenant_id)

    async def suspend(self, tenant_id: str) -> None:
        """Transition ``tenant_id`` to ``SUSPENDED``."""
        await self._catalog.update_status(tenant_id, TenantStatus.SUSPENDED)
        await self._emit_catalog_changed(tenant_id)

    async def resume(self, tenant_id: str) -> None:
        """Transition a ``SUSPENDED`` tenant back to ``ACTIVE``."""
        await self._catalog.update_status(tenant_id, TenantStatus.ACTIVE)
        await self._emit_catalog_changed(tenant_id)

    async def list_tenants(
        self, *, status: TenantStatus | None = TenantStatus.ACTIVE
    ) -> list[TenantDescriptor]:
        """
        Read-through to ``AbstractTenantCatalog.list_tenants``.

        DESIGN: the control service is the control plane's single facade
            ✅ ``build_tenant_router()`` takes one collaborator, not a
               (control_service, catalog) pair — the read and write sides of
               the admin surface cannot drift apart or be wired to two
               different catalogs.
            ✅ Keeps the seam rule intact: ``varco_fastapi.tenancy`` never
               needs to name an ``AbstractTenantCatalog`` implementation.
            ❌ One more method to keep in step with the catalog ABC. Accepted
               — it is a pure delegation with no added behaviour.

        Args:
            status: Status filter, forwarded verbatim. Defaults to
                    ``TenantStatus.ACTIVE`` (the catalog's own default —
                    "routable tenants"). Pass ``None`` for every status,
                    including tombstoned/deleted.

        Returns:
            Descriptors sorted deterministically by ``tenant_id``.
        """
        return await self._catalog.list_tenants(status=status)

    async def mark_active(self, tenant_id: str) -> TenantDescriptor:
        """
        Authority-only terminator: flip ``tenant_id`` to ``ACTIVE`` and emit
        ``TenantCatalogChanged``. Used by ``TenantReadinessCoordinator``
        once every expected store reports ``TenantNodeReady``, and by the
        manual ``POST …/activate`` route.

        If ``tenant_id`` is unknown to the catalog (e.g. a worker fleet
        onboarded it purely by DDL with no prior REST/bus command reaching
        this authority), it is added directly as ``ACTIVE`` rather than
        raising — the terminator's job is to make the tenant routable, not
        to require a specific prior catalog state.

        Returns:
            The tenant's descriptor after the flip — rendered directly by
            ``POST /tenancy/tenants/{id}/activate``.

        Raises:
            ValueError: ``catalog_authority`` is ``False`` — a worker must
                never write the catalog (RD-16).
        """
        if not self.catalog_authority:
            raise ValueError(
                "mark_active() requires catalog_authority=True — this "
                "service is a worker (catalog_authority=False) and must "
                "not write the catalog."
            )
        try:
            await self._catalog.update_status(tenant_id, TenantStatus.ACTIVE)
        except TenantNotFoundError:
            from varco_core.tenancy.catalog import TenantDescriptor

            await self._catalog.add(
                TenantDescriptor(tenant_id=tenant_id, status=TenantStatus.ACTIVE)
            )
        await self._emit_catalog_changed(tenant_id)
        return await self._catalog.get(tenant_id)

    async def request_provision(self, tenant_id: str) -> None:
        """
        Broadcast-only (RD-14): emit ``TenantProvisionRequested`` with
        ``origin=self.node_id``. Performs **no** local catalog write and
        **no** local provisioner call — the caller is explicitly NOT
        included. A node that must also provision itself calls
        ``provision()`` first, then this method.
        """
        await self._producer._produce(
            TenantProvisionRequested(tenant_id=tenant_id, origin=self.node_id),
            channel=CHANNEL_TENANCY,
        )

    async def request_deprovision(self, tenant_id: str, *, confirm: bool = False) -> None:
        """
        Broadcast-only (RD-14) mirror of ``request_provision``.

        Raises:
            DestructiveOperationRefused: ``confirm`` is not ``True`` —
                refuses to broadcast a command that would only DLQ
                fleet-wide.
        """
        if not confirm:
            raise DestructiveOperationRefused(
                f"Refusing to broadcast deprovision for tenant {tenant_id!r} without confirm=True."
            )
        await self._producer._produce(
            TenantDeprovisionRequested(tenant_id=tenant_id, confirm=True, origin=self.node_id),
            channel=CHANNEL_TENANCY,
        )

    async def _emit_catalog_changed(self, tenant_id: str) -> None:
        await self._producer._produce(
            TenantCatalogChanged(tenant_id=tenant_id), channel=CHANNEL_TENANCY
        )
