"""
varco_core.retention.posture
===============================

``inspect_retention_posture()`` — a **pure, read-only** introspection
function over a ``RetentionRegistry`` (Plan 039 / S20, §D-S20-posture).

Mirrors ``varco_core.revocation.posture.inspect_revocation_posture()``'s
shape exactly: takes explicit objects (never a global), never raises,
reports facts rather than judgements. Plan 036's ``SecurityPosture`` harness
owns aggregation/thresholds/wiring — this module exports only the pure
inspector (a parked follow-up, §Parked).

Why this differs from Plan 038's decision *against* an inbound-webhook
inspector (checked against each of 038's three grounds, not assumed):
    - *"no process-global registry to read"* — ❌ does not apply: a
      ``RetentionRegistry`` is an explicit, app-constructed, DI-bindable
      object, exactly like ``TrustedIssuerRegistry``.
    - *"an app with no route would get a permanent, meaningless finding"* —
      ⚠️ partly: resolved the same way revocation does — ``configured=False``
      is reported as a fact, never a finding, so an app without retention
      gets no noise.
    - *"the question is already answered at construction, loudly"* — ❌
      does not apply: ``RetentionPolicy``'s ``ValueError``s cover
      *malformed* policies, not "is any policy actually deleting?", "which
      are platform-wide?", or "has this been dry-running since March?" —
      exactly what an operator needs and only a read of the live registry
      can answer.

Thread safety:  ✅ Pure function — no shared state, no I/O.
Async safety:   ✅ Synchronous, no I/O — safe to call from anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_core.retention.policy import RetentionRegistry

__all__ = ["RetentionPostureReport", "inspect_retention_posture"]

_SHORT_RETENTION_FLOOR = timedelta(hours=24)


@dataclass(frozen=True)
class RetentionPostureReport:
    """
    A snapshot of facts about the retention wiring an app has assembled.

    Attributes:
        configured: ``True`` when a ``RetentionRegistry`` was given —
            independent of how many policies it holds (even zero).
        policy_count: Total registered policies.
        destructive_count: Policies with ``dry_run=False`` — the ones that
            actually delete.
        dry_run_count: Policies with ``dry_run=True`` — preview-only.
        platform_wide_policies: Names of policies with ``tenant_ids is
            None`` — the genuinely security-relevant finding
            (§D-S20-tenancy): a cross-tenant delete an operator may not
            have realised they configured.
        dlq_policies: Names of policies whose target's ``kind == "dlq"``.
        short_retention_policies: Names of policies whose ``older_than`` is
            set and under 24h (i.e. those that needed
            ``acknowledge_short_retention=True`` to construct at all).
        kinds: The set of distinct target ``kind`` values across every
            registered policy.
    """

    configured: bool
    policy_count: int = 0
    destructive_count: int = 0
    dry_run_count: int = 0
    platform_wide_policies: tuple[str, ...] = field(default_factory=tuple)
    dlq_policies: tuple[str, ...] = field(default_factory=tuple)
    short_retention_policies: tuple[str, ...] = field(default_factory=tuple)
    kinds: frozenset[str] = field(default_factory=frozenset)


def inspect_retention_posture(registry: RetentionRegistry | None = None) -> RetentionPostureReport:
    """
    Report facts about the retention wiring assembled so far.

    Args:
        registry: The application's ``RetentionRegistry``, if constructed.
            ``None`` (default) is a valid, common state — an app that has
            not opted into retention at all.

    Returns:
        A ``RetentionPostureReport``. Never raises.

    Example::

        report = inspect_retention_posture(registry=my_registry)
        if report.platform_wide_policies:
            ...  # Plan 036's judgement, not this function's
    """
    if registry is None:
        return RetentionPostureReport(configured=False)

    try:
        names = list(registry)
    except Exception:  # noqa: BLE001 — never raises, mirrors revocation/posture.py
        return RetentionPostureReport(configured=False)

    destructive = 0
    dry_run = 0
    platform_wide: list[str] = []
    dlq: list[str] = []
    short_retention: list[str] = []
    kinds: set[str] = set()

    for name in names:
        policy = registry.get(name)
        if policy is None:  # pragma: no cover - defensive, registry is internally consistent
            continue
        if policy.dry_run:
            dry_run += 1
        else:
            destructive += 1
        if policy.tenant_ids is None:
            platform_wide.append(name)
        try:
            kind = policy.target.kind
        except Exception:  # noqa: BLE001 — never raises
            kind = "unknown"
        kinds.add(kind)
        if kind == "dlq":
            dlq.append(name)
        if policy.older_than is not None and policy.older_than < _SHORT_RETENTION_FLOOR:
            short_retention.append(name)

    return RetentionPostureReport(
        configured=True,
        policy_count=len(names),
        destructive_count=destructive,
        dry_run_count=dry_run,
        platform_wide_policies=tuple(platform_wide),
        dlq_policies=tuple(dlq),
        short_retention_policies=tuple(short_retention),
        kinds=frozenset(kinds),
    )
