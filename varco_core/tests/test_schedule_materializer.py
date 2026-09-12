"""
Unit tests for varco_core.schedule (Plan 032 / D6) — Schedule entity +
materializer.

Core coverage per plan Step 11:
  - a spring-forward gap and a fall-back ambiguity resolved per
    GapPolicy/OverlapPolicy (the whole reason the zoned Job fields exist)
  - all three catch-up policies (SKIP/FIRE_ONCE/BACKFILL_ALL)
  - concurrent materializers produce exactly one job per occurrence
  - a disabled schedule produces none

A minimal in-memory AbstractJobStore fake backs every test — only the five
abstract methods are implemented, no framework/backend dependency.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from varco_core.job.base import AbstractJobStore, Job, JobStatus
from varco_core.tz.schedule import GapPolicy, OverlapPolicy, datetime_exists


class FakeJobStore(AbstractJobStore):
    """Minimal in-memory AbstractJobStore — just enough for materializer tests."""

    def __init__(self) -> None:
        self._jobs: dict[UUID, Job] = {}
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def save(self, job: Job, *, expected_epoch: int | None = None) -> None:
        async with self._get_lock():
            self._jobs[job.job_id] = job

    async def get(self, job_id: UUID) -> Job | None:
        return self._jobs.get(job_id)

    async def list_by_status(self, status: JobStatus, *, limit: int = 100) -> list[Job]:
        return [j for j in self._jobs.values() if j.status == status][:limit]

    async def delete(self, job_id: UUID) -> None:
        self._jobs.pop(job_id, None)

    async def try_claim(
        self, job_id: UUID, *, owner_id: str | None = None, lease_ttl: float | None = None
    ) -> Job | None:
        async with self._get_lock():
            job = self._jobs.get(job_id)
            if job is None or job.status != JobStatus.PENDING:
                return None
            claimed = job.as_running() if hasattr(job, "as_running") else job
            self._jobs[job_id] = claimed
            return claimed

    def all_jobs(self) -> list[Job]:
        return list(self._jobs.values())


def _import_schedule():
    from varco_core.schedule.entity import CatchUpPolicy, Schedule  # noqa: PLC0415
    from varco_core.schedule.materializer import ScheduleMaterializer  # noqa: PLC0415

    return Schedule, CatchUpPolicy, ScheduleMaterializer


@pytest.fixture
def schedule_module():
    return _import_schedule()


class TestScheduleDisabled:
    async def test_disabled_schedule_produces_no_jobs(self, schedule_module) -> None:
        Schedule, _, ScheduleMaterializer = schedule_module
        store = FakeJobStore()
        schedule = Schedule(
            cron_expr="* * * * *",
            timezone="UTC",
            enabled=False,
        )
        materializer = ScheduleMaterializer(job_store=store)
        jobs = await materializer.materialize(schedule, now=datetime(2026, 1, 1, tzinfo=UTC))
        assert jobs == []
        assert store.all_jobs() == []


class TestDstGapAndOverlap:
    async def test_spring_forward_gap_resolved_per_gap_policy(self, schedule_module) -> None:
        # US spring-forward 2026: clocks jump 02:00 -> 03:00 on 2026-03-08 in
        # America/New_York. A cron that would fire at the nonexistent
        # 02:30 must resolve per GapPolicy.NEXT_VALID, matching
        # resolve_zoned()'s own behaviour exactly (same primitive, reused).
        Schedule, _, ScheduleMaterializer = schedule_module
        zone = ZoneInfo("America/New_York")
        nonexistent_wall = datetime(2026, 3, 8, 2, 30)
        assert not datetime_exists(nonexistent_wall, zone)

        store = FakeJobStore()
        schedule = Schedule(
            cron_expr="30 2 8 3 *",
            timezone="America/New_York",
            gap_policy=GapPolicy.NEXT_VALID,
            overlap_policy=OverlapPolicy.FIRST,
        )
        materializer = ScheduleMaterializer(job_store=store)
        jobs = await materializer.materialize(
            schedule,
            now=datetime(2026, 3, 9, 0, 0, tzinfo=UTC),
        )
        assert len(jobs) == 1
        job = jobs[0]
        assert job.run_at_wall == nonexistent_wall
        assert job.run_at_tz == "America/New_York"
        # The materialized UTC instant must never fall inside the gap itself.
        assert job.run_at is not None

    async def test_fall_back_ambiguity_resolved_per_overlap_policy(self, schedule_module) -> None:
        # US fall-back 2026: 01:30 occurs twice on 2026-11-01 in
        # America/New_York. OverlapPolicy.FIRST (the default) must pin fold=0.
        Schedule, _, ScheduleMaterializer = schedule_module
        store = FakeJobStore()
        schedule = Schedule(
            cron_expr="30 1 1 11 *",
            timezone="America/New_York",
            gap_policy=GapPolicy.NEXT_VALID,
            overlap_policy=OverlapPolicy.FIRST,
        )
        materializer = ScheduleMaterializer(job_store=store)
        jobs = await materializer.materialize(
            schedule,
            now=datetime(2026, 11, 2, 0, 0, tzinfo=UTC),
        )
        assert len(jobs) == 1
        job = jobs[0]
        assert job.run_at_wall == datetime(2026, 11, 1, 1, 30)
        assert job.run_at_fold == 0


class TestCatchUpPolicies:
    async def test_skip_materializes_only_the_next_future_occurrence(self, schedule_module) -> None:
        Schedule, CatchUpPolicy, ScheduleMaterializer = schedule_module
        store = FakeJobStore()
        # Hourly schedule, materializer "was down" for 5 hours.
        schedule = Schedule(
            cron_expr="0 * * * *",
            timezone="UTC",
            catchup_policy=CatchUpPolicy.SKIP,
            last_materialized_at=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )
        materializer = ScheduleMaterializer(job_store=store)
        jobs = await materializer.materialize(schedule, now=datetime(2026, 1, 1, 5, 30, tzinfo=UTC))
        assert len(jobs) == 1

    async def test_fire_once_materializes_a_single_catchup_job(self, schedule_module) -> None:
        Schedule, CatchUpPolicy, ScheduleMaterializer = schedule_module
        store = FakeJobStore()
        schedule = Schedule(
            cron_expr="0 * * * *",
            timezone="UTC",
            catchup_policy=CatchUpPolicy.FIRE_ONCE,
            last_materialized_at=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )
        materializer = ScheduleMaterializer(job_store=store)
        jobs = await materializer.materialize(schedule, now=datetime(2026, 1, 1, 5, 30, tzinfo=UTC))
        assert len(jobs) == 1

    async def test_backfill_all_materializes_every_missed_occurrence_bounded(
        self, schedule_module
    ) -> None:
        Schedule, CatchUpPolicy, ScheduleMaterializer = schedule_module
        store = FakeJobStore()
        schedule = Schedule(
            cron_expr="0 * * * *",
            timezone="UTC",
            catchup_policy=CatchUpPolicy.BACKFILL_ALL,
            max_backfill=3,
            last_materialized_at=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )
        materializer = ScheduleMaterializer(job_store=store)
        # 5 hourly occurrences missed (01:00..05:00), but max_backfill=3 bounds it.
        jobs = await materializer.materialize(schedule, now=datetime(2026, 1, 1, 5, 30, tzinfo=UTC))
        assert len(jobs) == 3


class TestConcurrentMaterializers:
    async def test_two_concurrent_materializers_produce_exactly_one_job(
        self, schedule_module
    ) -> None:
        # UNIQUE(schedule_id, run_at) + the fenced-lease primitives must
        # prevent double-materialization when two instances race.
        Schedule, _, ScheduleMaterializer = schedule_module
        store = FakeJobStore()
        schedule = Schedule(
            cron_expr="* * * * *",
            timezone="UTC",
            last_materialized_at=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )
        materializer_a = ScheduleMaterializer(job_store=store)
        materializer_b = ScheduleMaterializer(job_store=store)

        now = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
        results = await asyncio.gather(
            materializer_a.materialize(schedule, now=now),
            materializer_b.materialize(schedule, now=now),
        )
        total_jobs = sum(len(r) for r in results)
        assert total_jobs == 1
        assert len(store.all_jobs()) == 1


# ── Schedule.task_name / Job.task_payload (Plan 039 §D-S20-driver, Step 10) ──


class TestTaskNameProducesTaskPayload:
    async def test_task_name_none_produces_no_task_payload_pinned(self, schedule_module) -> None:
        """task_name=None (the default) must be byte-identical to today: a
        materialized Job carries task_payload=None. Pinned BEFORE the change
        lands — this sub-test must PASS right now."""
        Schedule, _, ScheduleMaterializer = schedule_module
        store = FakeJobStore()
        schedule = Schedule(
            cron_expr="* * * * *",
            timezone="UTC",
        )
        materializer = ScheduleMaterializer(job_store=store)
        jobs = await materializer.materialize(schedule, now=datetime(2026, 1, 1, tzinfo=UTC))
        assert len(jobs) == 1
        assert jobs[0].task_payload is None

    async def test_task_name_set_produces_task_payload(self, schedule_module) -> None:
        """task_name="x" + payload={"a": 1} produces
        TaskPayload(task_name="x", kwargs={"a": 1})."""
        from varco_core.job.task import TaskPayload  # noqa: PLC0415

        Schedule, _, ScheduleMaterializer = schedule_module
        store = FakeJobStore()
        schedule = Schedule(
            cron_expr="* * * * *",
            timezone="UTC",
            task_name="x",
            payload={"a": 1},
        )
        materializer = ScheduleMaterializer(job_store=store)
        jobs = await materializer.materialize(schedule, now=datetime(2026, 1, 1, tzinfo=UTC))
        assert len(jobs) == 1
        assert jobs[0].task_payload == TaskPayload(task_name="x", kwargs={"a": 1})

    async def test_job_id_unchanged_by_task_name(self, schedule_module) -> None:
        """The occurrence job id stays uuid5(schedule_id, wall) regardless of
        task_name — materializer.py:76-78 is the only id source."""
        Schedule, _, ScheduleMaterializer = schedule_module
        now = datetime(2026, 1, 1, tzinfo=UTC)

        store_a = FakeJobStore()
        schedule_a = Schedule(cron_expr="* * * * *", timezone="UTC")
        materializer_a = ScheduleMaterializer(job_store=store_a)
        jobs_a = await materializer_a.materialize(schedule_a, now=now)

        store_b = FakeJobStore()
        schedule_b = Schedule(
            schedule_id=schedule_a.schedule_id,
            cron_expr="* * * * *",
            timezone="UTC",
            task_name="x",
        )
        materializer_b = ScheduleMaterializer(job_store=store_b)
        jobs_b = await materializer_b.materialize(schedule_b, now=now)

        assert jobs_a[0].job_id == jobs_b[0].job_id
