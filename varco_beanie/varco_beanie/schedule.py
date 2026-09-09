"""
varco_beanie.schedule
========================
``BeanieScheduleRepository`` — MongoDB/Beanie ``AbstractScheduleRepository``
(Plan 032 / D6, Step 10).

Same self-managed ``AsyncIOMotorClient`` + ``init_beanie`` shape as
``varco_beanie.webhook`` — ``Schedule`` is a small, standalone,
framework-owned resource, not an application document sharing the app's
primary ``init_beanie()`` call. See that module's docstring for the full
DESIGN rationale (mirrored here).

Thread safety:  ⚠️ One repository instance owns one Motor client — do not
                   share across event loops.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from beanie import Document, init_beanie
from pydantic import Field
from pymongo import ASCENDING, IndexModel
from varco_core.schedule.entity import CatchUpPolicy, Schedule
from varco_core.schedule.repository import AbstractScheduleRepository
from varco_core.tz.schedule import GapPolicy, OverlapPolicy

__all__ = ["BeanieScheduleRepository", "ScheduleDocument"]


class ScheduleDocument(Document):
    """
    Beanie document backing ``BeanieScheduleRepository``.

    Register it in your ``init_beanie()`` call if sharing the app's
    connection instead of using this repository's self-managed
    ``start()``/``stop()`` lifecycle::

        await init_beanie(database=db, document_models=[..., ScheduleDocument])
    """

    # Beanie's own default `id` type is `PydanticObjectId` — overridden to a
    # plain UUID so it matches `DomainModel.pk`'s `UUID_AUTO` strategy, same
    # convention as every other UUID-keyed document in this package.
    id: UUID = Field(default_factory=uuid4)  # type: ignore[assignment]

    schedule_id: UUID = Field(default_factory=uuid4)
    tenant_id: str | None = None
    cron_expr: str
    timezone: str
    enabled: bool = True
    gap_policy: str = GapPolicy.NEXT_VALID.value
    overlap_policy: str = OverlapPolicy.FIRST.value
    catchup_policy: str = CatchUpPolicy.SKIP.value
    max_backfill: int = 100
    last_materialized_at: datetime | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    callback_url: str | None = None
    # Plan 039 (S20) / Step 12: nullable, no backfill, mirrors varco_sa's
    # migration 0008_schedule_task_name.py — Beanie has no schema migration
    # so an existing document simply lacks the field until next save().
    task_name: str | None = None
    created_at: datetime
    updated_at: datetime

    class Settings:
        name = "schedules"
        indexes = [
            # Mirrors SAScheduleRepository's UNIQUE(schedule_id) — one
            # domain identity, one document.
            IndexModel([("schedule_id", ASCENDING)], unique=True),
            IndexModel([("tenant_id", ASCENDING)]),
        ]


class BeanieScheduleRepository(AbstractScheduleRepository):
    """
    MongoDB/Beanie ``AbstractScheduleRepository``.

    Args:
        url:     Mongo connection URL.
        db_name: Database name to initialize Beanie against.

    Edge cases:
        - Call ``await repo.start()`` before any other method — it opens
          the Motor client and calls ``init_beanie()`` scoped to
          ``ScheduleDocument`` only.
        - Call ``await repo.stop()`` to close the client.
    """

    def __init__(self, *, url: str, db_name: str) -> None:
        self._url = url
        self._db_name = db_name
        self._client: Any = None

    async def start(self) -> None:
        """Open the Motor client and initialize Beanie for this document."""
        from motor.motor_asyncio import AsyncIOMotorClient

        self._client = AsyncIOMotorClient(self._url)
        await init_beanie(
            database=self._client[self._db_name],
            document_models=[ScheduleDocument],
        )

    async def stop(self) -> None:
        """Close the Motor client. Safe to call if never started."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def _doc_to_entity(self, doc: ScheduleDocument) -> Schedule:
        entity = Schedule(
            schedule_id=doc.schedule_id,
            tenant_id=doc.tenant_id,
            cron_expr=doc.cron_expr,
            timezone=doc.timezone,
            enabled=doc.enabled,
            gap_policy=GapPolicy(doc.gap_policy),
            overlap_policy=OverlapPolicy(doc.overlap_policy),
            catchup_policy=CatchUpPolicy(doc.catchup_policy),
            max_backfill=doc.max_backfill,
            last_materialized_at=doc.last_materialized_at,
            payload=dict(doc.payload),
            callback_url=doc.callback_url,
            task_name=doc.task_name,
            created_at=doc.created_at,
            updated_at=doc.updated_at,
        )
        entity.pk = doc.id if isinstance(doc.id, UUID) else UUID(str(doc.id))
        entity._raw_orm = doc
        return entity

    async def save(self, schedule: Schedule) -> Schedule:
        """See ``AbstractScheduleRepository.save()``."""
        now = datetime.now(UTC)
        if schedule.pk is None:
            doc = ScheduleDocument(
                schedule_id=schedule.schedule_id,
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
            await doc.insert()
            schedule.pk = doc.id
        else:
            doc = await ScheduleDocument.get(schedule.pk)
            if doc is None:
                raise ValueError(f"Schedule with pk={schedule.pk!r} not found")
            doc.tenant_id = schedule.tenant_id
            doc.cron_expr = schedule.cron_expr
            doc.timezone = schedule.timezone
            doc.enabled = schedule.enabled
            doc.gap_policy = schedule.gap_policy.value
            doc.overlap_policy = schedule.overlap_policy.value
            doc.catchup_policy = schedule.catchup_policy.value
            doc.max_backfill = schedule.max_backfill
            doc.last_materialized_at = schedule.last_materialized_at
            doc.payload = dict(schedule.payload)
            doc.callback_url = schedule.callback_url
            doc.task_name = schedule.task_name
            doc.updated_at = now
            await doc.save()
        return self._doc_to_entity(doc)

    async def find_by_id(self, pk: object) -> Schedule | None:
        """See ``AbstractScheduleRepository.find_by_id()``."""
        doc = await ScheduleDocument.get(pk)
        return self._doc_to_entity(doc) if doc is not None else None

    async def find_all_enabled(self) -> list[Schedule]:
        """See ``AbstractScheduleRepository.find_all_enabled()``."""
        docs = await ScheduleDocument.find(ScheduleDocument.enabled == True).to_list()  # noqa: E712
        return [self._doc_to_entity(d) for d in docs]

    async def delete(self, pk: object) -> None:
        """See ``AbstractScheduleRepository.delete()``."""
        doc = await ScheduleDocument.get(pk)
        if doc is not None:
            await doc.delete()
