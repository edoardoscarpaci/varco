"""
tests.test_retention_integration
===================================
Plan 039 (S20) / Step 25 — retention adapters against real Postgres
(session-scoped ``postgres_url`` fixture, CLAUDE.md shared-container
convention). Every test namespaces its own data with ``uuid4().hex[:8]``
since the container is shared across the whole session.

RED until ``varco_core/varco_core/retention/{base,targets}.py`` (Steps 5,
9) and the SA ``task_name`` migration ``0008`` (Step 12) land.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from varco_core.event import Event
from varco_core.job.base import Job
from varco_core.service.audit import AuditEntry
from varco_sa.audit import SAAuditRepository, audit_metadata
from varco_sa.dlq import SADeadLetterQueue
from varco_sa.job_store import SAJobStore

pytestmark = pytest.mark.integration


class SampleEvent(Event):
    __event_type__ = "test.retention_integration.sample"


def _run_id() -> str:
    return uuid4().hex[:8]


class TestDlqRetentionTargetIntegration:
    async def test_delete_where_only_matching_rows_deleted(self, postgres_url: str) -> None:
        from varco_core.event.dlq import DeadLetterEntry  # noqa: PLC0415
        from varco_core.retention.targets import DlqRetentionTarget  # noqa: PLC0415

        engine = create_async_engine(postgres_url)
        try:
            dlq = SADeadLetterQueue(engine)
            await dlq.ensure_table()

            run_id = _run_id()
            channel = f"orders-{run_id}"
            other_channel = f"other-{run_id}"

            for _ in range(3):
                await dlq.push(
                    DeadLetterEntry(
                        event=SampleEvent(),
                        channel=channel,
                        handler_name="H.h",
                        error_type="E",
                        error_message="msg",
                        attempts=1,
                    )
                )
            await dlq.push(
                DeadLetterEntry(
                    event=SampleEvent(),
                    channel=other_channel,
                    handler_name="H.h",
                    error_type="E",
                    error_message="msg",
                    attempts=1,
                )
            )

            # channel= scopes this policy's target to this test's own
            # namespaced channel — the shared-container rule (CLAUDE.md's
            # Test Conventions): older_than alone would match every entry
            # in the shared dead_letters table, including other_channel's
            # (and any leftover row from a concurrently-run test session).
            target = DlqRetentionTarget(
                dlq=dlq, acknowledge_dead_letter_deletion=True, channel=channel
            )
            outcome = await target.purge(
                older_than=datetime.now(UTC) + timedelta(days=1), limit=100, dry_run=False
            )
            assert outcome.deleted == 3

            remaining = await dlq.count_by_channel()
            assert remaining.get(channel, 0) == 0
            assert remaining.get(other_channel, 0) == 1
        finally:
            await engine.dispose()

    async def test_dry_run_true_deletes_nothing(self, postgres_url: str) -> None:
        from varco_core.event.dlq import DeadLetterEntry  # noqa: PLC0415
        from varco_core.retention.targets import DlqRetentionTarget  # noqa: PLC0415

        engine = create_async_engine(postgres_url)
        try:
            dlq = SADeadLetterQueue(engine)
            await dlq.ensure_table()

            run_id = _run_id()
            channel = f"orders-{run_id}"
            for _ in range(2):
                await dlq.push(
                    DeadLetterEntry(
                        event=SampleEvent(),
                        channel=channel,
                        handler_name="H.h",
                        error_type="E",
                        error_message="msg",
                        attempts=1,
                    )
                )

            before = (await dlq.count_by_channel()).get(channel, 0)
            target = DlqRetentionTarget(dlq=dlq, acknowledge_dead_letter_deletion=True)
            await target.purge(
                older_than=datetime.now(UTC) + timedelta(days=1), limit=100, dry_run=True
            )
            after = (await dlq.count_by_channel()).get(channel, 0)
            assert after == before == 2
        finally:
            await engine.dispose()

    async def test_chunked_sweep_with_small_batch_size_converges(self, postgres_url: str) -> None:
        from varco_core.event.dlq import DeadLetterEntry  # noqa: PLC0415
        from varco_core.retention.targets import DlqRetentionTarget  # noqa: PLC0415

        engine = create_async_engine(postgres_url)
        try:
            dlq = SADeadLetterQueue(engine)
            await dlq.ensure_table()

            run_id = _run_id()
            channel = f"orders-{run_id}"
            for _ in range(5):
                await dlq.push(
                    DeadLetterEntry(
                        event=SampleEvent(),
                        channel=channel,
                        handler_name="H.h",
                        error_type="E",
                        error_message="msg",
                        attempts=1,
                    )
                )

            # channel= scopes this policy to this test's own namespaced
            # channel — see the identical comment in the prior test.
            target = DlqRetentionTarget(
                dlq=dlq, acknowledge_dead_letter_deletion=True, channel=channel
            )
            total_deleted = 0
            for _ in range(10):
                outcome = await target.purge(
                    older_than=datetime.now(UTC) + timedelta(days=1), limit=2, dry_run=False
                )
                total_deleted += outcome.deleted
                if outcome.deleted == 0:
                    break
            assert total_deleted == 5
        finally:
            await engine.dispose()


class TestAuditRetentionTargetIntegration:
    async def test_hash_chained_table_raises_without_allow_chain_break(
        self, postgres_url: str
    ) -> None:
        from varco_core.retention.targets import AuditRetentionTarget  # noqa: PLC0415

        engine = create_async_engine(postgres_url)
        try:
            async with engine.begin() as conn:
                await conn.run_sync(audit_metadata.create_all)

            from sqlalchemy.ext.asyncio import async_sessionmaker

            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            repo = SAAuditRepository(session_factory, hash_chain=True)

            entity_type = f"Order-{_run_id()}"
            await repo.save(
                AuditEntry(
                    entity_type=entity_type,
                    entity_id="1",
                    action="create",
                    occurred_at=datetime(2020, 1, 1, tzinfo=UTC),
                )
            )

            target = AuditRetentionTarget(repo=repo)
            with pytest.raises(ValueError):
                await target.purge(
                    older_than=datetime.now(UTC) + timedelta(days=1), limit=100, dry_run=False
                )
        finally:
            await engine.dispose()


class TestJobRetentionTargetIntegration:
    async def test_deletes_only_expected_rows(self, postgres_url: str) -> None:
        from varco_core.retention.targets import JobRetentionTarget  # noqa: PLC0415

        engine = create_async_engine(postgres_url)
        try:
            store = SAJobStore(engine)
            await store.ensure_table()

            completed_job = Job(job_id=uuid4())
            await store.save(completed_job)
            running = await store.try_claim(completed_job.job_id)
            completed = running.as_completed(b"ok")
            await store.save(completed)

            pending_job = Job(job_id=uuid4())
            await store.save(pending_job)

            target = JobRetentionTarget(store=store)
            outcome = await target.purge(
                older_than=datetime.now(UTC) + timedelta(days=1), limit=100, dry_run=False
            )
            assert outcome.deleted >= 1

            remaining_pending = await store.get(pending_job.job_id)
            assert remaining_pending is not None
        finally:
            await engine.dispose()


class TestScheduleTaskNameMigrationIntegration:
    async def test_task_name_round_trips_through_sa_schedule_repository(
        self, postgres_url: str
    ) -> None:
        from varco_core.schedule.entity import Schedule  # noqa: PLC0415
        from varco_sa.schedule import SAScheduleRepository  # noqa: PLC0415

        repo = SAScheduleRepository(url=postgres_url)
        await repo.start()
        try:
            schedule = Schedule(
                cron_expr="0 3 * * *",
                timezone="UTC",
                task_name="varco.retention.purge",
                payload={"policy": f"nightly-{_run_id()}"},
            )
            saved = await repo.save(schedule)
            fetched = await repo.find_by_id(saved.pk)
            assert fetched is not None
            assert fetched.task_name == "varco.retention.purge"
        finally:
            await repo.stop()
