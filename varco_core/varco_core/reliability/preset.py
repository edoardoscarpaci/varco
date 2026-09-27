"""
varco_core.reliability.preset
================================
``ReliabilityPreset`` — a frozen bundle of "opt into durability once" config
(Plan 009, Phase 9 / R5).

Composes three previously-separate concerns behind one object: retry+DLQ
(``varco_core.event``/``varco_core.resilience``), the transactional outbox
(``varco_core.service.outbox``), and the audit trail
(``varco_core.service.audit``) — plus the Phase 1 reliability metrics pack.

DESIGN: a frozen config object, not a `@Configuration`
    ✅ A scanned `@Configuration` auto-activates on `container.scan()`
       (`technical_docs/features/casbin-authorization.md`'s "Policy
       authorizer silently active" pitfall is the same class of mistake for
       a different feature) — durability silently turning on is as bad as
       it silently staying off.
    ✅ `ReliabilityModule(preset)` (Plan 049's fastapi wiring, passed to
       `create_varco_app(container, modules=[...])`) is one explicit line.
    ❌ Not injectable-by-scan — that is the point.

DESIGN: the preset does not construct the DLQ
    ✅ The DLQ is backend-specific; `varco_core` must not know concrete
       types (the same layer rule as everywhere else in this codebase).
    ❌ Two lines instead of one at the call site — unavoidable given the
       layer rule.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from varco_core.resilience.retry import RetryPolicy

if TYPE_CHECKING:
    from varco_core.event.dlq import AbstractDeadLetterQueue
    from varco_core.observability.reliability import ReliabilityMetricsConfig


@dataclass(frozen=True)
class ReliabilityPreset:
    """
    A named, composable bundle of reliability settings.

    Attributes:
        retry_policy: Applied to every bare ``@listen`` handler that neither
            declares its own ``retry_policy=``/``dlq=`` nor is overridden by
            an instance-level ``register_to(retry_policy=, dlq=)`` fallback.
        dlq:          Same scope as ``retry_policy`` — see
            ``EventConsumer.register_to()``'s resolution order.
        outbox:       Whether ``ReliabilityLifecycle.startup()`` should start
            an ``OutboxRelay``.
        audit:        Whether ``ReliabilityLifecycle.startup()`` should wire
            an ``AuditConsumer``.
        metrics:      Optional ``ReliabilityMetricsConfig`` — installed via
            ``install_reliability_metrics()`` at startup.
        outbox_max_attempts: Passed through to ``OutboxRelay`` verbatim.

    Raises:
        ValueError: ``outbox_max_attempts`` is set without a ``dlq`` —
            mirrors ``OutboxRelay.__init__``'s refusal to configure silent
            data loss (deleting a poison entry with nowhere durable to put
            it).
    """

    retry_policy: RetryPolicy | None = None
    dlq: AbstractDeadLetterQueue | None = None
    outbox: bool = False
    audit: bool = False
    metrics: ReliabilityMetricsConfig | None = None
    outbox_max_attempts: int | None = None

    def __post_init__(self) -> None:
        if self.outbox_max_attempts is not None and self.dlq is None:
            raise ValueError(
                "ReliabilityPreset(outbox_max_attempts=...) requires a dlq= — "
                "deleting a poison outbox entry with nowhere durable to put "
                "it is silent data loss (mirrors OutboxRelay's own refusal)."
            )

    @classmethod
    def off(cls) -> ReliabilityPreset:
        """
        The default preset — byte-identical to pre-Plan-009 behaviour.

        Every bare ``@listen`` handler keeps re-raising on exhaustion (no
        retry, no DLQ); no outbox relay, no audit consumer, no metrics are
        started by ``ReliabilityLifecycle``.
        """
        return cls()

    @classmethod
    def best_effort(cls, *, dlq: AbstractDeadLetterQueue) -> ReliabilityPreset:
        """A moderate default retry policy + DLQ, no outbox/audit/metrics."""
        return cls(retry_policy=RetryPolicy(max_attempts=3, base_delay=0.5), dlq=dlq)

    @classmethod
    def durable(cls, *, dlq: AbstractDeadLetterQueue) -> ReliabilityPreset:
        """
        "Opt into durability once": ``RetryPolicy.durable_delivery()`` +
        DLQ + outbox + audit + metrics all on.
        """
        from varco_core.observability.reliability import ReliabilityMetricsConfig

        return cls(
            retry_policy=RetryPolicy.durable_delivery(),
            dlq=dlq,
            outbox=True,
            audit=True,
            metrics=ReliabilityMetricsConfig(),
        )


__all__ = ["ReliabilityPreset"]
