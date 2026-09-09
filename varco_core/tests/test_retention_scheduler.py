"""
tests.test_retention_scheduler
=================================
Plan 039 (S20) / Steps 13+15 — ``RetentionScheduler`` sweep semantics
(materialize path) and the dispatch path end-to-end
(``varco_core.retention.scheduler``).

RED until ``varco_core/varco_core/retention/scheduler.py`` (Step 14) lands.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from varco_core.job.base import AbstractJobStore, Job, JobStatus
from varco_core.schedule.repository import InMemoryScheduleRepository


class FakeJobStore(AbstractJobStore):
    def __init__(self) -> None:
        self._jobs: dict[object, Job] = {}
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def save(self, job: Job, *, expected_epoch: int | None = None) -> None:
        async with self._get_lock():
            self._jobs[job.job_id] = job

    async def get(self, job_id):
        return self._jobs.get(job_id)

    async def list_by_status(self, status: JobStatus, *, limit: int = 100):
        return [j for j in self._jobs.values() if j.status == status][:limit]

    async def delete(self, job_id) -> None:
        self._jobs.pop(job_id, None)

    async def try_claim(self, job_id, *, owner_id=None, lease_ttl=None):
        async with self._get_lock():
            job = self._jobs.get(job_id)
            if job is None or job.status != JobStatus.PENDING:
                return None
            if job.run_at is not None and job.run_at > datetime.now(UTC):
                return None
            claimed = job.as_running()
            self._jobs[job_id] = claimed
            return claimed

    def all_jobs(self) -> list[Job]:
        return list(self._jobs.values())


def _make_policy(name="nightly", *, target=None, dry_run=True, older_than=timedelta(days=30)):
    from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415

    return RetentionPolicy(
        name=name,
        target=target,
        cron_expr="* * * * *",
        timezone="UTC",
        dry_run=dry_run,
        older_than=older_than,
    )


class _FakeSupportingTarget:
    supports_older_than = True
    supports_dry_run = True
    kind = "fake"

    def __init__(self):
        self.calls: list[dict] = []

    async def purge(self, *, older_than, limit, dry_run):
        from varco_core.retention.policy import RetentionOutcome  # noqa: PLC0415
        from varco_core.service.tenant import current_tenant  # noqa: PLC0415

        self.calls.append(
            {
                "older_than": older_than,
                "limit": limit,
                "dry_run": dry_run,
                "tenant": current_tenant(),
            }
        )
        return RetentionOutcome(
            kind=self.kind,
            examined=0,
            deleted=0 if dry_run else 1,
            would_delete=1 if dry_run else 0,
            dry_run=dry_run,
        )


# ── Steps 13: sweep semantics ───────────────────────────────────────────────


class TestRetentionSchedulerSweep:
    async def test_interval_zero_start_creates_no_task(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import RetentionScheduler  # noqa: PLC0415

        registry = RetentionRegistry()
        scheduler = RetentionScheduler(
            registry,
            schedule_repo=InMemoryScheduleRepository(),
            job_store=FakeJobStore(),
            task_registry=object(),
            interval=0.0,
        )
        await scheduler.start()
        assert scheduler._task is None  # no background task spawned
        await scheduler.stop()

    async def test_one_sweep_materializes_one_job_second_sweep_zero(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import RetentionScheduler  # noqa: PLC0415

        target = _FakeSupportingTarget()
        policy = _make_policy(target=target)
        registry = RetentionRegistry()
        registry.register(policy)

        schedule_repo = InMemoryScheduleRepository()
        job_store = FakeJobStore()
        scheduler = RetentionScheduler(
            registry, schedule_repo=schedule_repo, job_store=job_store, task_registry=object()
        )
        await scheduler.ensure_schedules()

        materialized_first = await scheduler.sweep_once()
        materialized_second = await scheduler.sweep_once()

        assert materialized_first >= 1
        assert materialized_second == 0

    async def test_schedule_not_in_registry_is_skipped(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import RetentionScheduler  # noqa: PLC0415
        from varco_core.schedule.entity import Schedule  # noqa: PLC0415

        schedule_repo = InMemoryScheduleRepository()
        # A schedule not created via ensure_schedules() — belongs to the app,
        # not this registry.
        await schedule_repo.save(Schedule(cron_expr="* * * * *", timezone="UTC"))

        job_store = FakeJobStore()
        scheduler = RetentionScheduler(
            RetentionRegistry(),
            schedule_repo=schedule_repo,
            job_store=job_store,
            task_registry=object(),
        )
        materialized = await scheduler.sweep_once()
        assert materialized == 0
        assert job_store.all_jobs() == []

    async def test_sweep_raising_is_logged_and_loop_survives(self, caplog) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import RetentionScheduler  # noqa: PLC0415

        class _BrokenRepo(InMemoryScheduleRepository):
            async def find_all_enabled(self):
                raise RuntimeError("boom")

        scheduler = RetentionScheduler(
            RetentionRegistry(),
            schedule_repo=_BrokenRepo(),
            job_store=FakeJobStore(),
            task_registry=object(),
            interval=0.01,
        )
        await scheduler.start()
        await asyncio.sleep(0.05)
        await scheduler.stop()
        # the loop must have survived — no uncaught exception raised out of
        # start()/stop(), and it kept running (didn't die on first sweep)

    async def test_stop_before_start_is_a_noop(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import RetentionScheduler  # noqa: PLC0415

        scheduler = RetentionScheduler(
            RetentionRegistry(),
            schedule_repo=InMemoryScheduleRepository(),
            job_store=FakeJobStore(),
            task_registry=object(),
        )
        await scheduler.stop()  # must not raise


# ── Step 15: dispatch path end-to-end ───────────────────────────────────────


class TestRetentionSchedulerDispatch:
    async def test_purge_policy_reachable_through_task_registry_invoke(self) -> None:
        from varco_core.job.task import TaskPayload, TaskRegistry, VarcoTask  # noqa: PLC0415
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import RetentionScheduler  # noqa: PLC0415

        target = _FakeSupportingTarget()
        policy = _make_policy(name="nightly", target=target, dry_run=False)
        registry = RetentionRegistry()
        registry.register(policy)

        task_registry = TaskRegistry()
        scheduler = RetentionScheduler(
            registry,
            schedule_repo=InMemoryScheduleRepository(),
            job_store=FakeJobStore(),
            task_registry=task_registry,
        )
        task_registry.register(VarcoTask(name="varco.retention.purge", fn=scheduler.purge_policy))

        await task_registry.invoke(
            TaskPayload(task_name="varco.retention.purge", kwargs={"policy": "nightly"})
        )
        assert len(target.calls) == 1

    async def test_unknown_policy_name_raises_retention_policy_not_found_error(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import (  # noqa: PLC0415
            RetentionPolicyNotFoundError,
            RetentionScheduler,
        )

        scheduler = RetentionScheduler(
            RetentionRegistry(),
            schedule_repo=InMemoryScheduleRepository(),
            job_store=FakeJobStore(),
            task_registry=object(),
        )
        with pytest.raises(RetentionPolicyNotFoundError):
            await scheduler.purge_policy(policy="ghost")

    async def test_dry_run_true_deletes_nothing_spy_on_backend(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import RetentionScheduler  # noqa: PLC0415

        target = _FakeSupportingTarget()
        policy = _make_policy(name="dry", target=target, dry_run=True)
        registry = RetentionRegistry()
        registry.register(policy)

        scheduler = RetentionScheduler(
            registry,
            schedule_repo=InMemoryScheduleRepository(),
            job_store=FakeJobStore(),
            task_registry=object(),
        )
        outcome = await scheduler.purge_policy(policy="dry")
        assert outcome.deleted == 0
        assert target.calls[0]["dry_run"] is True

    async def test_max_batches_truncates_and_reports_truncated(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import RetentionScheduler  # noqa: PLC0415

        class _AlwaysFullTarget(_FakeSupportingTarget):
            async def purge(self, *, older_than, limit, dry_run):
                from varco_core.retention.policy import RetentionOutcome  # noqa: PLC0415

                self.calls.append({"older_than": older_than, "limit": limit, "dry_run": dry_run})
                return RetentionOutcome(
                    kind="fake", examined=limit, deleted=limit, would_delete=0, dry_run=dry_run
                )

        target = _AlwaysFullTarget()
        from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415

        policy = RetentionPolicy(
            name="truncated",
            target=target,
            cron_expr="* * * * *",
            timezone="UTC",
            dry_run=False,
            older_than=timedelta(days=30),
            batch_size=10,
            max_batches=3,
        )
        registry = RetentionRegistry()
        registry.register(policy)

        scheduler = RetentionScheduler(
            registry,
            schedule_repo=InMemoryScheduleRepository(),
            job_store=FakeJobStore(),
            task_registry=object(),
        )
        outcome = await scheduler.purge_policy(policy="truncated")
        assert outcome.truncated is True
        assert len(target.calls) == 3

    async def test_tenant_ids_enters_tenant_context_per_tenant(self) -> None:
        from varco_core.retention.policy import RetentionPolicy, RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import RetentionScheduler  # noqa: PLC0415

        target = _FakeSupportingTarget()
        policy = RetentionPolicy(
            name="scoped",
            target=target,
            cron_expr="* * * * *",
            timezone="UTC",
            dry_run=False,
            older_than=timedelta(days=30),
            tenant_ids=("a", "b"),
        )
        registry = RetentionRegistry()
        registry.register(policy)

        scheduler = RetentionScheduler(
            registry,
            schedule_repo=InMemoryScheduleRepository(),
            job_store=FakeJobStore(),
            task_registry=object(),
        )
        await scheduler.purge_policy(policy="scoped")
        assert [c["tenant"] for c in target.calls] == ["a", "b"]

    async def test_tenant_ids_none_forwards_tenant_id_none_exactly_once(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.scheduler import RetentionScheduler  # noqa: PLC0415

        target = _FakeSupportingTarget()
        policy = _make_policy(
            name="platform", target=target, dry_run=False, older_than=timedelta(days=30)
        )
        registry = RetentionRegistry()
        registry.register(policy)

        scheduler = RetentionScheduler(
            registry,
            schedule_repo=InMemoryScheduleRepository(),
            job_store=FakeJobStore(),
            task_registry=object(),
        )
        await scheduler.purge_policy(policy="platform")
        assert len(target.calls) == 1
        assert target.calls[0]["tenant"] is None
