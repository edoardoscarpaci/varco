"""
varco_core.schedule.entity
============================
``Schedule`` — a recurring cron schedule that a ``ScheduleMaterializer``
turns into concrete ``Job`` rows (Plan 032 / D6).

**No execution path lives here.** ``Schedule`` only describes *when* and
*what* to materialize; the existing ``AbstractJobRunner`` runs the produced
``Job`` rows exactly as it always has (§Non-goals — "if this phase starts
writing a second runner, it has gone wrong").
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID, uuid4

from varco_core.meta import PKStrategy, PrimaryKey, pk_field
from varco_core.model import DomainModel
from varco_core.tz.schedule import GapPolicy, OverlapPolicy

__all__ = ["CatchUpPolicy", "Schedule"]


class CatchUpPolicy(StrEnum):
    """
    What a ``ScheduleMaterializer`` that missed one or more occurrences does
    about it (§D-D6-cron).

    ``SKIP`` is the default: a "send the nightly digest" schedule that
    missed three nights should send tonight's digest, not three at once —
    the surprising-and-expensive failure mode is the one to avoid by
    default. A schedule with real catch-up semantics (billing) opts into
    ``FIRE_ONCE``/``BACKFILL_ALL`` consciously.
    """

    #: Materialize only the most recent due occurrence; earlier missed ones
    #: are lost. Default.
    SKIP = "skip"
    #: Materialize exactly one job representing the missed window, then
    #: resume normal per-occurrence materialization on the next call.
    FIRE_ONCE = "fire_once"
    #: Materialize every missed occurrence, oldest first, bounded by
    #: ``max_backfill``.
    BACKFILL_ALL = "backfill_all"


@dataclass(kw_only=True)
class Schedule(DomainModel):
    """
    A recurring cron schedule.

    Attributes:
        schedule_id: Stable domain identity, independent of ``pk`` (which is
            ``None`` until persisted — see ``DomainModel``). Used as the
            join key for materialized ``Job.metadata["schedule_id"]`` and
            for the SA/Beanie repositories' ``UNIQUE(schedule_id, run_at)``
            index (Step 10) that makes cross-process double-materialization
            impossible rather than merely unlikely.
        tenant_id: Owning tenant, or ``None`` for a platform-wide schedule
            (Open question 2 — ``Meta`` below defaults to
            ``TenantScope.TENANT``; a genuinely global maintenance schedule
            overrides ``Meta.tenant_scope = TenantScope.GLOBAL`` on a
            subclass or per-deployment registration, the existing
            mechanism — no new one).
        cron_expr: A 5-field cron expression (``varco_core.schedule.cron``).
        timezone: IANA zone name the cron expression's wall-clock fields are
            interpreted in (e.g. ``"America/New_York"``) — this is what
            makes spring-forward/fall-back resolution meaningful; a naive
            "just compute UTC" schedule would fire at the wrong wall-clock
            hour twice a year.
        enabled: A disabled schedule materializes nothing at all.
        gap_policy: Forwarded to ``resolve_zoned()`` for a nonexistent
            (spring-forward) wall time. Default ``NEXT_VALID``.
        overlap_policy: Forwarded to ``resolve_zoned()`` for an ambiguous
            (fall-back) wall time. Default ``FIRST``.
        catchup_policy: See ``CatchUpPolicy``. Default ``SKIP``.
        max_backfill: Upper bound on occurrences materialized in one
            ``BACKFILL_ALL`` call. Ignored by ``SKIP``/``FIRE_ONCE``.
        last_materialized_at: The wall-clock (or UTC — the materializer
            normalizes) time of the most recent materialization, or
            ``None`` for a schedule that has never run. The materializer
            never mutates this field itself — the repository/caller updates
            it after persisting the returned jobs (same "storage is dumb"
            split as every other ``DomainModel`` in this codebase).
        payload: Arbitrary data merged into each materialized ``Job``'s
            ``metadata`` — the schedule owner's own invocation arguments.
        callback_url: Forwarded verbatim to each materialized ``Job``.
        task_name: Plan 039 (S20) / §D-S20-driver "(1)" — the ``TaskRegistry``
            name to invoke for each materialized occurrence. ``None`` (the
            default) is **byte-identical to today**: ``_build_job`` emits
            ``task_payload=None``, exactly as before this field existed —
            pinned by
            ``test_schedule_materializer.py::TestTaskNameProducesTaskPayload
            ::test_task_name_none_produces_no_task_payload_pinned`` before
            the change landed. When set, ``_build_job`` emits
            ``TaskPayload(task_name=schedule.task_name,
            kwargs=dict(schedule.payload))``, which is what makes a
            ``Schedule`` (previously "when", never "what") reachable through
            ``JobRunner.recover()``'s ``task_payload is not None`` filter —
            closing the gap Plan 032 left open (nothing called
            ``materialize()`` and nothing it produced was executable).

    Edge cases:
        - ``last_materialized_at=None`` is treated as "never run" — the
          materializer computes the latest/earliest due occurrence relative
          to ``now`` rather than requiring a synthetic epoch sentinel.
    """

    pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()

    schedule_id: UUID = field(default_factory=uuid4)
    tenant_id: str | None = None

    cron_expr: str
    timezone: str

    enabled: bool = True
    gap_policy: GapPolicy = GapPolicy.NEXT_VALID
    overlap_policy: OverlapPolicy = OverlapPolicy.FIRST
    catchup_policy: CatchUpPolicy = CatchUpPolicy.SKIP
    max_backfill: int = 100

    last_materialized_at: datetime | None = None

    payload: dict[str, Any] = field(default_factory=dict)
    callback_url: str | None = None
    task_name: str | None = None

    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    class Meta:
        table = "schedules"
        # tenant_scope intentionally omitted — TenantScope.TENANT is the
        # global default read by varco_core.tenancy.global_scope when the
        # attribute is absent (Open question 2's chosen default). A
        # platform-wide schedule sets TenantScope.GLOBAL explicitly.
