"""
varco_core.observability.retention
=====================================
``install_retention_metrics()`` — opt-in counters for the retention
subsystem (Plan 039 / S20, §D-S20-obs). ``install_*`` shape (a) from the DI
wiring verb taxonomy: a process-global side effect taking no container,
identical to ``install_reliability_metrics``/``install_cache_metrics``.

DESIGN: counts, a log line, an opt-in metric — no event, no audit row
    ✅ ⛔ **No event.** Publishing would require an ``AbstractEventBus``/
       ``AbstractEventProducer`` inside ``varco_core.retention``, and the
       standing rule (CLAUDE.md's Event system section) is that only
       ``OutboxRelay``, ``EventConsumer.register_to()``, and
       ``DlqRedriver`` hold a bus. A retention sweeper is not on that list.
    ✅ ⛔ **No audit row.** A purge that prunes the audit log would write
       audit rows about pruning audit rows — an unbounded feedback loop,
       and ``AuditConsumer`` is event-driven, which reintroduces the bus.
       Stated explicitly so nobody "fixes" it later.
    ✅ This module records *counts*, opt-in — an operator who wants more
       than a log line calls ``install_retention_metrics()`` once.
    ❌ Without the opt-in metric an operator sees only INFO logs. Accepted
       — identical to reliability metrics' own default posture.

``Metric`` (``varco_core.observability.metric``) is lazily backed — safe to
instantiate at module scope before any ``MeterProvider`` is configured.

Thread safety:  ✅ ``Metric.add()`` is documented idempotent-safe under the
                   CPython GIL — same story as every other counter in this
                   package.
Async safety:   ✅ Synchronous, no I/O — call from anywhere, including
                   inside ``RetentionScheduler``'s sweep loop.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.observability.metric import Metric

if TYPE_CHECKING:
    from varco_core.retention.policy import RetentionResult

__all__ = ["install_retention_metrics", "record_retention_result"]

# Module-level Metric instances — safe before DI configures a MeterProvider
# (Metric's own instrument creation is lazy, on first .add()).
_purged_rows = Metric(
    "varco.retention.purged_rows",
    kind="counter",
    description="Rows purged by a RetentionTarget, by kind/policy",
)
_sweep_errors = Metric(
    "varco.retention.sweep_errors",
    kind="counter",
    description="RetentionResult.error occurrences, by kind/policy",
)
_dry_run_skips = Metric(
    "varco.retention.dry_run_skips",
    kind="counter",
    description="dry_run=True calls that a target skipped (no preview support)",
)

_installed = False


def install_retention_metrics() -> None:
    """
    Opt into the retention metrics pack.

    Idempotent — calling this more than once has no additional effect; the
    underlying ``Metric`` instruments are created lazily and shared.

    Returns:
        ``None``. After this call, ``record_retention_result()`` (called by
        ``RetentionScheduler``/the CLI internally — an app does not need to
        call it itself) reports to the three counters above instead of
        being a no-op.

    Edge cases:
        - Calling this before a ``MeterProvider`` is configured is safe —
          ``Metric`` degrades to OTel's no-op meter until one is installed.
    """
    global _installed
    _installed = True


def record_retention_result(result: RetentionResult) -> None:
    """
    Record one ``RetentionResult`` (one policy execution) to the metrics
    pack, if ``install_retention_metrics()`` was called.

    Args:
        result: The result to record.

    Edge cases:
        - A no-op until ``install_retention_metrics()`` has been called —
          this function is always safe to call unconditionally from
          ``RetentionScheduler``/the CLI, whether or not metrics are on.
    """
    if not _installed:
        return
    if result.deleted:
        _purged_rows.add(result.deleted, kind=result.kind, policy=result.policy)
    if result.error is not None:
        _sweep_errors.add(kind=result.kind, policy=result.policy)
    if result.skipped_reason is not None:
        _dry_run_skips.add(kind=result.kind, policy=result.policy)
