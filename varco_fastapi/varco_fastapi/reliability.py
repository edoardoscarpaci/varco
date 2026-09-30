"""
varco_fastapi.reliability
============================
``ReliabilityLifecycle`` — the FastAPI-side startup/shutdown wiring for a
``ReliabilityPreset`` (Plan 009, Phase 9 / R5): installs reliability metrics,
starts an ``OutboxRelay``, and registers an ``AuditConsumer``, according to
what the preset asks for.

Imports only ``varco_core.reliability`` (a core seam) — never a backend, same
rule as every other ``varco_fastapi`` lifecycle component.

DESIGN: explicit, all-or-nothing resolution — no silent skip
    ✅ A preset that asks for ``outbox=True``/``audit=True`` but whose
       container cannot resolve the required bindings fails LOUDLY at
       startup, not silently at the first event (or never) — the entire
       point of this feature is "opt into durability once" without needing
       to double-check every deployment wired it correctly.
    ❌ An app must bind ``OutboxRepository``/``AuditRepository``/
       ``AbstractEventBus`` before passing an ``outbox=True``/``audit=True``
       preset — no "best effort" half-wiring is supported.

Thread safety:  ⚠️ Not thread-safe — construct/use from the app's own
                    startup/shutdown lifespan hooks, single-threaded.
Async safety:   ✅ ``startup()``/``shutdown()`` are ``async def``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from varco_core.reliability import ReliabilityPreset

if TYPE_CHECKING:
    from varco_core.service.audit import AuditConsumer
    from varco_core.service.outbox import OutboxRelay


class ReliabilityLifecycle:
    """
    Drives a ``ReliabilityPreset``'s startup/shutdown side effects.

    Args:
        preset:    The ``ReliabilityPreset`` to apply.
        container: A ``providify.DIContainer`` used to resolve
                   ``OutboxRepository``/``AuditRepository``/
                   ``AbstractEventBus`` when the preset asks for them.

    Edge cases:
        - ``ReliabilityPreset.off()`` → ``startup()``/``shutdown()`` are
          both no-ops (byte-identical to not wiring this at all).
    """

    def __init__(self, preset: ReliabilityPreset, *, container: Any) -> None:
        self._preset = preset
        self._container = container
        self._relay: OutboxRelay | None = None
        self._audit_consumer: AuditConsumer | None = None

    async def startup(self) -> None:
        """
        Apply the preset: install metrics, start the outbox relay, wire the
        audit consumer — whichever the preset asks for.

        Raises:
            LookupError: ``preset.outbox``/``preset.audit`` is ``True`` but a
                required binding (``OutboxRepository``, ``AuditRepository``,
                or ``AbstractEventBus``) is not resolvable from the
                container. The message names the missing interface.
        """
        if self._preset.metrics is not None:
            from varco_core.observability.reliability import install_reliability_metrics

            outbox_repo = None
            if self._preset.outbox:
                from varco_core.service.outbox import OutboxRepository

                outbox_repo = await self._resolve(OutboxRepository)
            install_reliability_metrics(
                dlq=self._preset.dlq,
                outbox_repo=outbox_repo,
                config=self._preset.metrics,
            )

        if self._preset.outbox:
            from varco_core.event.base import AbstractEventBus, Event
            from varco_core.serialization import Serializer
            from varco_core.service.outbox import OutboxRelay, OutboxRepository

            outbox_repo = await self._resolve(OutboxRepository)
            bus = await self._resolve(AbstractEventBus)
            # Plan 050 / §D-wire row 1: an explicit preset.serializer always
            # wins; otherwise inject whatever Serializer[Event] the
            # container resolves — the 045 @Fallback default is
            # JsonEventSerializer, so this always resolves and a
            # bind_cloudevents_serializer() swap reaches the outbox with no
            # code change here.
            serializer = self._preset.serializer
            if serializer is None and self._container.is_resolvable(Serializer[Event]):
                serializer = await self._container.aget(Serializer[Event])
            self._relay = OutboxRelay(
                outbox=outbox_repo,
                bus=bus,
                retry_policy=self._preset.retry_policy,
                dlq=self._preset.dlq,
                max_attempts=self._preset.outbox_max_attempts,
                serializer=serializer,
            )
            await self._relay.start()

        if self._preset.audit:
            from varco_core.event.base import AbstractEventBus
            from varco_core.service.audit import AuditConsumer, AuditRepository

            audit_repo = await self._resolve(AuditRepository)
            bus = await self._resolve(AbstractEventBus)
            self._audit_consumer = AuditConsumer(audit_repo=audit_repo)
            self._audit_consumer.register_to(bus)

    async def shutdown(self) -> None:
        """
        Stop the audit consumer, then the outbox relay — the reverse of
        ``startup()`` (Plan 050 / §Context: the ``AuditConsumer`` used to
        never be stopped here, leaking its subscription on every
        shutdown).
        """
        if self._audit_consumer is not None:
            await self._audit_consumer.stop()
            self._audit_consumer = None
        if self._relay is not None:
            await self._relay.stop()
            self._relay = None

    # ``start``/``stop`` aliases — the plan's documented public API is
    # ``startup``/``shutdown``, but ``VarcoLifespan`` drives every lifecycle
    # component through ``AbstractLifecycle``'s ``start``/``stop`` names
    # (see ``varco_fastapi.lifespan``); this lets ReliabilityModule wire a
    # ReliabilityLifecycle in alongside MigrationsModule/TenancyModule.
    async def start(self) -> None:
        await self.startup()

    async def stop(self) -> None:
        await self.shutdown()

    async def _resolve(self, interface: Any) -> Any:
        """
        Resolve ``interface`` from the container, or raise a message naming
        it explicitly — ``providify``'s own ``LookupError`` already names
        the interface, but we re-raise with the preset context attached.
        """
        try:
            return await self._container.aget(interface)
        except LookupError as exc:
            raise LookupError(
                f"ReliabilityPreset requires a binding for {interface.__name__} "
                f"(outbox={self._preset.outbox}, audit={self._preset.audit}) — "
                f"bind it in the container before passing this preset to "
                f"ReliabilityModule(preset). Original error: {exc}"
            ) from exc


__all__ = ["ReliabilityLifecycle"]
