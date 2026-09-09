"""
varco_sa.schedule
====================
``SAScheduleRepository`` — SQLAlchemy async ``AbstractScheduleRepository``
(Plan 032 / D6, Step 10).

Same framework-table shape as ``varco_sa.webhook``/``varco_sa.idempotency``:
own ``Table``, own ``MetaData``, ``register_framework_metadata()``, a
manual dataclass↔row mapping — never the ``@register`` ORM generator.

``schedule_id`` carries a ``UNIQUE`` constraint — one domain identity, one
row. This is deliberately **not** the ``UNIQUE(schedule_id, run_at)`` index
the plan's design narrative mentions for double-materialization safety:
that pair describes *occurrences* (a schedule_id + a materialized run_at),
which live on the *jobs* table, not here — ``Schedule`` has no ``run_at``
column at all. ``varco_core.schedule.materializer``'s own DESIGN note
covers the occurrence-level safety net (a deterministic, uuid5-derived
``Job.job_id`` that makes cross-process double-materialization converge on
one physical row) — no schema change to the jobs table was needed for it.

Usage::

    from varco_sa.schedule import SAScheduleRepository

    repo = SAScheduleRepository(url="postgresql+asyncpg://...")
    await repo.start()
    ...
    await repo.stop()

Thread safety:  ✅ ``AsyncEngine`` connection pool is coroutine-safe.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, MetaData, String, Table
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from varco_core.schedule.entity import CatchUpPolicy, Schedule
from varco_core.schedule.repository import AbstractScheduleRepository
from varco_core.tz.schedule import GapPolicy, OverlapPolicy

from varco_sa.metadata import register_framework_metadata as _register_fw_metadata

__all__ = ["SAScheduleRepository", "schedule_metadata"]

# Separate MetaData — never pollutes the application's Base.metadata, same
# convention as every other framework table in this package.
schedule_metadata = MetaData()

_schedules_table = Table(
    "schedules",
    schedule_metadata,
    Column("pk", PGUUID(as_uuid=True).with_variant(String(36), "sqlite"), primary_key=True),
    Column(
        "schedule_id",
        PGUUID(as_uuid=True).with_variant(String(36), "sqlite"),
        nullable=False,
        unique=True,
        index=True,
    ),
    Column("tenant_id", String(255), nullable=True, index=True),
    Column("cron_expr", String(255), nullable=False),
    Column("timezone", String(64), nullable=False),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("gap_policy", String(32), nullable=False),
    Column("overlap_policy", String(32), nullable=False),
    Column("catchup_policy", String(32), nullable=False),
    Column("max_backfill", Integer, nullable=False, default=100),
    Column("last_materialized_at", DateTime(timezone=True), nullable=True),
    Column("payload", JSON, nullable=False),
    Column("callback_url", String(2048), nullable=True),
    # Plan 039 (S20) / Step 12: nullable, no backfill — added by migration
    # 0008_schedule_task_name.py, after 0007_schedules_table.py.
    Column("task_name", String(255), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

_register_fw_metadata("varco_sa.schedule", schedule_metadata)


def _ensure_tz(dt: datetime) -> datetime:
    """Coerce naive datetimes (SQLite) to UTC — same helper as ``varco_sa.dlq``."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def _ensure_tz_optional(dt: datetime | None) -> datetime | None:
    return _ensure_tz(dt) if dt is not None else None


class SAScheduleRepository(AbstractScheduleRepository):
    """
    SQLAlchemy async ``AbstractScheduleRepository`` backed by ``schedules``.

    Args:
        url:           Async SQLAlchemy connection URL.
        engine_kwargs: Extra kwargs forwarded to ``create_async_engine()``.

    Edge cases:
        - Call ``await repo.start()`` before any other method.
        - Call ``await repo.stop()`` to dispose the engine's pool.
    """

    def __init__(self, *, url: str, **engine_kwargs: Any) -> None:
        self._url = url
        self._engine_kwargs = engine_kwargs
        self._engine: AsyncEngine | None = None

    def _require_engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError(
                "SAScheduleRepository method called before start(). "
                "Call `await repo.start()` first."
            )
        return self._engine

    async def start(self) -> None:
        """Create the engine and ensure ``schedules`` exists."""
        if self._engine is None:
            self._engine = create_async_engine(self._url, **self._engine_kwargs)
        async with self._engine.begin() as conn:
            await conn.run_sync(schedule_metadata.create_all, checkfirst=True)

    async def stop(self) -> None:
        """Dispose the engine's connection pool. Safe to call if never started."""
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None

    def _row_to_entity(self, row: Any) -> Schedule:
        entity = Schedule(
            schedule_id=row.schedule_id
            if isinstance(row.schedule_id, UUID)
            else UUID(str(row.schedule_id)),
            tenant_id=row.tenant_id,
            cron_expr=row.cron_expr,
            timezone=row.timezone,
            enabled=row.enabled,
            gap_policy=GapPolicy(row.gap_policy),
            overlap_policy=OverlapPolicy(row.overlap_policy),
            catchup_policy=CatchUpPolicy(row.catchup_policy),
            max_backfill=row.max_backfill,
            last_materialized_at=_ensure_tz_optional(row.last_materialized_at),
            payload=dict(row.payload),
            callback_url=row.callback_url,
            task_name=row.task_name,
            created_at=_ensure_tz(row.created_at),
            updated_at=_ensure_tz(row.updated_at),
        )
        entity.pk = row.pk if isinstance(row.pk, UUID) else UUID(str(row.pk))
        entity._raw_orm = row
        return entity

    async def save(self, schedule: Schedule) -> Schedule:
        """See ``AbstractScheduleRepository.save()``."""
        engine = self._require_engine()
        now = datetime.now(UTC)

        if schedule.pk is None:
            schedule.pk = uuid4()
            async with engine.begin() as conn:
                await conn.execute(
                    sa.insert(_schedules_table).values(
                        pk=str(schedule.pk),
                        schedule_id=str(schedule.schedule_id),
                        tenant_id=schedule.tenant_id,
                        cron_expr=schedule.cron_expr,
                        timezone=schedule.timezone,
                        enabled=schedule.enabled,
                        gap_policy=schedule.gap_policy.value,
                        overlap_policy=schedule.overlap_policy.value,
                        catchup_policy=schedule.catchup_policy.value,
                        max_backfill=schedule.max_backfill,
                        last_materialized_at=schedule.last_materialized_at,
                        payload=dict(schedule.payload),
                        callback_url=schedule.callback_url,
                        task_name=schedule.task_name,
                        created_at=now,
                        updated_at=now,
                    )
                )
        else:
            async with engine.begin() as conn:
                await conn.execute(
                    sa.update(_schedules_table)
                    .where(_schedules_table.c.pk == str(schedule.pk))
                    .values(
                        tenant_id=schedule.tenant_id,
                        cron_expr=schedule.cron_expr,
                        timezone=schedule.timezone,
                        enabled=schedule.enabled,
                        gap_policy=schedule.gap_policy.value,
                        overlap_policy=schedule.overlap_policy.value,
                        catchup_policy=schedule.catchup_policy.value,
                        max_backfill=schedule.max_backfill,
                        last_materialized_at=schedule.last_materialized_at,
                        payload=dict(schedule.payload),
                        callback_url=schedule.callback_url,
                        task_name=schedule.task_name,
                        updated_at=now,
                    )
                )
        schedule._raw_orm = object()
        found = await self.find_by_id(schedule.pk)
        assert found is not None  # we just wrote it
        return found

    async def find_by_id(self, pk: object) -> Schedule | None:
        """See ``AbstractScheduleRepository.find_by_id()``."""
        engine = self._require_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                sa.select(_schedules_table).where(_schedules_table.c.pk == str(pk))
            )
            row = result.fetchone()
        return self._row_to_entity(row) if row is not None else None

    async def find_all_enabled(self) -> list[Schedule]:
        """See ``AbstractScheduleRepository.find_all_enabled()``."""
        engine = self._require_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                sa.select(_schedules_table).where(_schedules_table.c.enabled.is_(True))
            )
            rows = result.fetchall()
        return [self._row_to_entity(r) for r in rows]

    async def delete(self, pk: object) -> None:
        """See ``AbstractScheduleRepository.delete()``."""
        engine = self._require_engine()
        async with engine.begin() as conn:
            await conn.execute(sa.delete(_schedules_table).where(_schedules_table.c.pk == str(pk)))
