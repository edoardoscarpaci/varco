"""
varco_core.tenancy.control.consumer
======================================
``TenantProvisionConsumer`` — the event-driven onboarding/offboarding
consumer (Plan 007, Phase 5, step 4-5; converged onto ``TenantControlService``
by Plan 008, Phase 1).

DESIGN: RD-11 — the consumer routes through ``TenantControlService``; there
is exactly one catalog writer
    Before Plan 008 this consumer called ``AbstractTenantProvisioner``
    directly, so a bus-onboarded tenant's storage existed but its catalog
    row did not — every routing lookup (``routing.py``,
    ``TenantResolutionMiddleware``) 404'd it forever. Routing through
    ``TenantControlService`` closes that gap for provision *and*
    deprovision at once, and reaches the fan-out-supervisor-stop +
    pool-eviction steps that ``TenantControlService.deprovision()`` already
    performs before destructive DDL — previously unreachable from the bus.
    ✅ One catalog writer, one place to change when the transition grows.
    ❌ Breaking constructor change — mitigated by the RD-12 shim below.

DESIGN: RD-12 — ``provisioner=`` survives one minor release as a shim that
*requires* a catalog; ``provisioner=`` alone is a ``ValueError``, not a
warning
    ``TenantProvisionConsumer(provisioner=…, catalog=…, producer=…)``
    builds a ``TenantControlService`` internally and raises
    ``DeprecationWarning``. ``provisioner=`` with no ``catalog=`` raises
    ``ValueError`` naming the two-line fix — the only behaviour a bare
    ``provisioner=`` can produce is the defect itself; there is no correct
    thing to do with it.
    ✅ Fails at construction/startup, not at 3 a.m. when the first
       bus-onboarded tenant 404s.
    ✅ Keeps the deprecation window honest — the shim is a *working* path.
    ❌ A hard break for anyone on a ``main`` checkout — accepted: this tree
       was still uncommitted/unreleased at the time RD-11/RD-12 landed.

DESIGN: a tenancy-specific retry wrapper, not the generic ``@listen`` one
    The generic ``_make_retry_wrapper`` (``varco_core.event.consumer``)
    leaves ``DeadLetterEntry.source_ref`` at its default (``None``) for
    consumer-sourced entries — reasonable in general (the event itself
    carries identity), but the tenant control plane needs the tenant id
    directly on the entry so an operator triaging the DLQ does not have to
    open every event payload. This module builds its own thin retry+DLQ
    wrapper (mirroring the generic one's attempt/back-off loop) that stamps
    ``source_ref=event.tenant_id``.

DESIGN: bounded, insertion-ordered ``_processed_event_ids`` (LRU)
    A ``set[Any]`` would grow for the process's lifetime — a slow leak on a
    long-lived control plane. ``collections.OrderedDict`` used as an LRU
    (``max_tracked_event_ids: int = 4096``) bounds memory; cross-restart /
    beyond-window idempotency is the durable inbox's job (RD-1), not this
    cache's.

DESIGN: RD-15 — origin skip
    A command whose ``origin`` equals this consumer's control service's
    ``node_id`` is skipped (one DEBUG log) — it was already handled
    synchronously by the broadcaster's own ``provision()``/``deprovision()``
    call before it called ``request_provision()``/``request_deprovision()``.
    ``origin is None`` (external publisher) is always handled normally.
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import Callable
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from varco_core.deprecation import warn_deprecated
from varco_core.event.consumer import EventConsumer, listen
from varco_core.resilience import RetryPolicy
from varco_core.tenancy.control.events import (
    CHANNEL_TENANCY,
    TenantDeprovisionRequested,
    TenantProvisionRequested,
)
from varco_core.tenancy.control.service import TenantControlService
from varco_core.tenancy.provisioner import DestructiveOperationRefused

if TYPE_CHECKING:
    from varco_core.event.base import AbstractEventBus, Event, Subscription
    from varco_core.event.dlq import AbstractDeadLetterQueue
    from varco_core.event.producer import AbstractEventProducer
    from varco_core.tenancy.catalog import AbstractTenantCatalog
    from varco_core.tenancy.provisioner import AbstractTenantProvisioner

logger = logging.getLogger(__name__)

# Sentinel distinguishing "omitted" from an explicit None override — same
# pattern AuditConsumer uses for its register_to() kwargs.
_UNSET = object()


class _NullProducer:
    """
    No-op ``AbstractEventProducer`` used by the RD-12 shim when no
    ``producer=`` is supplied.

    ``_produce`` logs exactly one WARNING per call rather than at
    construction time — the message describes the actual, per-transition
    consequence ("TenantCatalogChanged was NOT emitted for this
    transition"), not a generic construction-time notice.
    """

    async def _produce(self, event: Event, *, channel: str = CHANNEL_TENANCY) -> None:
        logger.warning(
            "TenantProvisionConsumer shim has no producer= configured — "
            "TenantCatalogChanged was NOT emitted for this transition; "
            "other pods' CachedTenantCatalog entries will go stale until "
            "catalog_ttl_s elapses (see CachedTenantCatalog)."
        )

    async def _produce_many(self, events: list[tuple[Event, str]]) -> None:
        for event, channel in events:
            await self._produce(event, channel=channel)


class TenantProvisionConsumer(EventConsumer):
    """
    ``EventConsumer`` that provisions/deprovisions tenants on the
    ``"varco.tenancy"`` channel by driving a ``TenantControlService`` —
    the same catalog transition the REST admin surface uses (RD-11).

    Args:
        control_service: The ``TenantControlService`` to drive. Required
                         unless the deprecated ``provisioner=``/``catalog=``
                         shim is used instead (RD-12).
        dlq:             Optional default DLQ applied by ``register_to()``
                         unless overridden there.
        provisioner:     **Deprecated shim** (RD-12) — pairs with
                         ``catalog=`` to build a ``TenantControlService``
                         internally. Raises ``DeprecationWarning``. Alone
                         (no ``catalog=``) raises ``ValueError``.
        catalog:         **Deprecated shim** companion to ``provisioner=``.
        producer:        **Deprecated shim** companion to ``provisioner=``.
                         Omitted → a private ``_NullProducer`` is used and
                         each emission attempt logs one WARNING (see
                         ``_NullProducer``).
        max_tracked_event_ids: Bound on the in-process ``_processed_event_
                         ids`` LRU. Defaults to ``4096``.

    Raises:
        ValueError: Neither ``control_service`` nor ``provisioner`` is
            given, or ``provisioner`` is given without ``catalog``.
    """

    _default_retry_policy: RetryPolicy = RetryPolicy.durable_delivery()

    def __init__(
        self,
        *,
        control_service: TenantControlService | None = None,
        dlq: AbstractDeadLetterQueue | None = None,
        # deprecated shim (RD-12) — removed one minor release after this lands
        provisioner: AbstractTenantProvisioner | None = None,
        catalog: AbstractTenantCatalog | None = None,
        producer: AbstractEventProducer | None = None,
        max_tracked_event_ids: int = 4096,
    ) -> None:
        if control_service is not None:
            self._control = control_service
        elif provisioner is not None:
            if catalog is None:
                raise ValueError(
                    "TenantProvisionConsumer(provisioner=...) requires "
                    "catalog= too (the RD-12 shim needs both to build a "
                    "TenantControlService internally) — or pass "
                    "control_service=TenantControlService(catalog=..., "
                    "provisioner=..., producer=...) directly instead."
                )
            # Argument-conditional, so @deprecated (whole-callable) does not
            # fit (Plan 022 / Phase 2 step 14, the one recorded §D-DEP
            # deviation — still correct): only the provisioner=/catalog= shape
            # is deprecated, while control_service= is the supported path
            # through the same constructor. warn_deprecated() is the
            # parameter-path helper (Plan 053) and carries the literal
            # since=/removed_in= keywords the floor test reads
            # (test_deprecation_floor.py, Plan 058). stacklevel=3 counts from
            # inside the helper: warn_deprecated (1) → __init__ (2) → the
            # constructing caller (3) — the frame the pre-058 direct
            # warnings.warn(stacklevel=2) blamed.
            warn_deprecated(
                "TenantProvisionConsumer(provisioner=..., catalog=...)",
                since="3.0.0",
                removed_in="5.0.0",
                replacement=(
                    "control_service=TenantControlService(catalog=..., provisioner=..., producer=...)"
                ),
                stacklevel=3,
            )
            effective_producer: AbstractEventProducer = (
                producer if producer is not None else _NullProducer()  # type: ignore[assignment]
            )
            self._control = TenantControlService(
                catalog=catalog, provisioner=provisioner, producer=effective_producer
            )
        else:
            raise ValueError(
                "TenantProvisionConsumer requires control_service= (e.g. "
                "TenantProvisionConsumer(control_service=TenantControlService"
                "(catalog=..., provisioner=..., producer=...))). The "
                "deprecated provisioner=/catalog= shim additionally requires "
                "catalog= alongside provisioner=."
            )

        self._dlq = dlq
        self._max_tracked_event_ids = max_tracked_event_ids
        # In-process dedup only, bounded LRU — see module DESIGN note.
        self._processed_event_ids: OrderedDict[Any, None] = OrderedDict()

    def _is_processed(self, event_id: Any) -> bool:
        return event_id in self._processed_event_ids

    def _mark_processed(self, event_id: Any) -> None:
        self._processed_event_ids[event_id] = None
        self._processed_event_ids.move_to_end(event_id)
        while len(self._processed_event_ids) > self._max_tracked_event_ids:
            self._processed_event_ids.popitem(last=False)

    def _is_own_broadcast(self, origin: str | None) -> bool:
        return origin is not None and origin == self._control.node_id

    def register_to(
        self,
        bus: AbstractEventBus,
        *,
        retry_policy: RetryPolicy | None = _UNSET,  # type: ignore[assignment]
        dlq: AbstractDeadLetterQueue | None = _UNSET,  # type: ignore[assignment]
    ) -> list[Subscription]:
        effective_dlq = self._dlq if dlq is _UNSET else dlq
        effective_retry_policy = (
            self._default_retry_policy if retry_policy is _UNSET else retry_policy
        )

        provision_sub = bus.subscribe(
            TenantProvisionRequested,
            _make_tenant_retry_wrapper(
                self.on_provision_requested,
                effective_retry_policy,
                effective_dlq,
                CHANNEL_TENANCY,
            ),
            channel=CHANNEL_TENANCY,
        )
        deprovision_sub = bus.subscribe(
            TenantDeprovisionRequested,
            _make_tenant_retry_wrapper(
                self.on_deprovision_requested,
                effective_retry_policy,
                effective_dlq,
                CHANNEL_TENANCY,
            ),
            channel=CHANNEL_TENANCY,
        )
        subscriptions = [provision_sub, deprovision_sub]
        if not hasattr(self, "_subscriptions"):
            self._subscriptions: list[Subscription] = []
        self._subscriptions.extend(subscriptions)
        return subscriptions

    @listen(TenantProvisionRequested, channel=CHANNEL_TENANCY)
    async def on_provision_requested(self, event: Event) -> None:
        """Provision the tenant named in ``event``. Idempotent on redelivery."""
        assert isinstance(event, TenantProvisionRequested)
        if self._is_own_broadcast(event.origin):
            logger.debug(
                "TenantProvisionConsumer skipping own broadcast (origin=%r) for tenant %r",
                event.origin,
                event.tenant_id,
            )
            return
        if self._is_processed(event.event_id):
            return
        await self._control.provision(event.tenant_id)
        self._mark_processed(event.event_id)

    @listen(TenantDeprovisionRequested, channel=CHANNEL_TENANCY)
    async def on_deprovision_requested(self, event: Event) -> None:
        """
        Deprovision the tenant named in ``event``.

        Note: ``TenantControlService.deprovision`` re-checks ``confirm``
        itself — belt and braces, deliberate. The explicit check here fires
        first (better message) and before dedup marking.

        Raises:
            DestructiveOperationRefused: ``event.confirm`` is not ``True`` —
                rejected rather than silently executed; with a DLQ wired
                the rejected event lands there instead of being retried
                forever.
        """
        assert isinstance(event, TenantDeprovisionRequested)
        if self._is_own_broadcast(event.origin):
            logger.debug(
                "TenantProvisionConsumer skipping own broadcast (origin=%r) for tenant %r",
                event.origin,
                event.tenant_id,
            )
            return
        if not event.confirm:
            raise DestructiveOperationRefused(
                f"TenantDeprovisionRequested for {event.tenant_id!r} arrived "
                "without confirm=True — refusing to execute."
            )
        if self._is_processed(event.event_id):
            return
        await self._control.deprovision(event.tenant_id, confirm=True)
        self._mark_processed(event.event_id)


def _make_tenant_retry_wrapper(
    handler: Callable[[Event], Any],
    policy: RetryPolicy | None,
    dlq: AbstractDeadLetterQueue | None,
    channel: str,
) -> Callable[[Event], Any]:
    """
    Retry + DLQ wrapper mirroring ``varco_core.event.consumer._make_retry_
    wrapper``'s attempt/back-off loop, but stamping ``source_ref=tenant_id``
    on the resulting ``DeadLetterEntry`` (see module DESIGN note).
    """
    max_attempts = policy.max_attempts if policy is not None else 1
    handler_name = getattr(handler, "__qualname__", repr(handler))

    async def wrapper(event: Event) -> None:
        from varco_core.event.dlq import DeadLetterEntry

        last_exc: BaseException | None = None
        first_failed_at: datetime | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                await handler(event)
                return
            except BaseException as exc:  # noqa: BLE001
                if policy is not None and not policy.is_retryable(exc):
                    raise

                now = datetime.now(tz=UTC)
                if first_failed_at is None:
                    first_failed_at = now
                last_exc = exc

                if attempt < max_attempts:
                    delay = policy.compute_delay(attempt - 1) if policy is not None else 0.0
                    logger.warning(
                        "TenantProvisionConsumer handler %r failed (attempt "
                        "%d/%d) on channel %r — retrying in %.2fs: %s",
                        handler_name,
                        attempt,
                        max_attempts,
                        channel,
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)

        assert last_exc is not None
        assert first_failed_at is not None

        logger.error(
            "TenantProvisionConsumer handler %r exhausted %d attempt(s) on channel %r: %s",
            handler_name,
            max_attempts,
            channel,
            last_exc,
        )

        if dlq is not None:
            tenant_id = getattr(event, "tenant_id", None)
            entry = DeadLetterEntry.from_failure(
                event=event,
                channel=channel,
                handler_name=handler_name,
                last_exc=last_exc,
                attempts=max_attempts,
                first_failed_at=first_failed_at,
            )
            import dataclasses

            entry = dataclasses.replace(entry, source_ref=tenant_id)
            await dlq.push(entry)
        else:
            from varco_core.resilience.retry import RetryExhaustedError

            # BUG (surfaced by RL-6's mypy gate, plans/017): this call was
            # missing the required `attempts`/`last_exc` positional args —
            # a TypeError at construction time (masking the real retry
            # failure with a confusing unrelated traceback) on any code path
            # that reaches this no-DLQ-configured branch. Fixed to construct
            # the error correctly per its documented signature.
            raise RetryExhaustedError(
                f"{handler_name} on channel {channel!r}",
                attempts=max_attempts,
                last_exc=last_exc,
            ) from last_exc

    return wrapper


# Exposed purely for introspection (documents the safe-by-default retry+DLQ
# contract without depending on decorator-time instance state, which is not
# available at class-body evaluation time — see test_tenant_provision_consumer.py).
TenantProvisionConsumer.on_provision_requested._listen_metadata = SimpleNamespace(
    retry_policy=TenantProvisionConsumer._default_retry_policy,
    dlq=object(),
)
