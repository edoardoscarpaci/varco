"""
varco_sa.job_store
==================
SQLAlchemy async implementation of ``AbstractJobStore``.

Uses raw SQLAlchemy Core (not ORM / SAModelFactory) so it has no circular
dependency on the application's ``DeclarativeBase`` or ``DomainModel``.

Table
-----
``varco_jobs`` — created idempotently via ``ensure_table()`` or Alembic::

    from varco_sa.job_store import jobs_metadata
    target_metadata = [Base.metadata, jobs_metadata]

Usage::

    from sqlalchemy.ext.asyncio import create_async_engine
    from varco_sa.job_store import SAJobStore

    engine = create_async_engine("postgresql+asyncpg://...")
    store = SAJobStore(engine)
    await store.ensure_table()

    job = Job()
    await store.save(job)
    running = await store.try_claim(job.job_id)

DESIGN: raw Core over ORM / SAModelFactory
    ✅ No dependency on application ``DeclarativeBase`` — infrastructure table.
    ✅ ``ensure_table()`` for zero-migration startup convenience.
    ✅ Works with any async SQLAlchemy engine (PostgreSQL, SQLite for tests).
    ❌ No Alembic auto-detection unless ``jobs_metadata`` is added to target.

DESIGN: try_claim() uses a single transaction (no SKIP LOCKED in this impl)
    ✅ Works on SQLite (unit tests) and PostgreSQL alike.
    ✅ Correct for single-process runners — only one runner claims each job.
    ❌ Not distributed-safe under concurrent multi-replica restarts.
       Feature 12 (SAJobStore try_claim hardening) adds SELECT FOR UPDATE
       SKIP LOCKED for true distributed safety on PostgreSQL.

Thread safety:  ✅ AsyncEngine connection pool is coroutine-safe.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🐍 https://docs.sqlalchemy.org/en/20/core/connections.html#asyncio-support
  SQLAlchemy async Core — connection and transaction patterns
- 📐 https://use-the-index-luke.com/sql/select-for-update/postgresql-skip-locked
  PostgreSQL SELECT FOR UPDATE SKIP LOCKED — distributed job-queue pattern
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import (
    Column,
    DateTime,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
)
from sqlalchemy.ext.asyncio import AsyncEngine

from varco_core.job.base import AbstractJobStore, Job, JobStatus
from varco_core.job.task import TaskPayload

_logger = logging.getLogger(__name__)

# ── Table schema ──────────────────────────────────────────────────────────────

# Separate MetaData so varco_jobs never pollutes the application's Base.metadata.
# Users opt into Alembic management by including jobs_metadata in target_metadata.
_metadata = MetaData()

_jobs_table = Table(
    "varco_jobs",
    _metadata,
    # Primary key — matches Job.job_id (UUIDv4).
    Column("job_id", sa.Uuid, primary_key=True),
    # StrEnum value — PENDING / RUNNING / COMPLETED / FAILED / CANCELLED.
    Column("status", String(32), nullable=False),
    # UTC timestamps for lifecycle tracking.
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=True),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    # Opaque bytes — caller decides serialization format.
    Column("result", LargeBinary, nullable=True),
    # Human-readable error message on FAILED — None otherwise.
    Column("error", Text, nullable=True),
    # Optional webhook URL for completion notification.
    Column("callback_url", Text, nullable=True),
    # JSON-serialized AuthContext snapshot — None for anonymous/unauthenticated.
    Column("auth_snapshot", Text, nullable=True),
    # Raw Bearer JWT — for audit trail and callback authentication.
    Column("request_token", Text, nullable=True),
    # JSON-serialized Job.metadata dict (extra data; not part of equality).
    Column("job_metadata", Text, nullable=True),
    # JSON-serialized TaskPayload.to_dict() — None for non-recoverable jobs.
    Column("task_payload", Text, nullable=True),
)

# Expose metadata for Alembic integration — include in target_metadata.
jobs_metadata = _metadata


# ── Serialization helpers ─────────────────────────────────────────────────────


def _dt_to_str(dt: datetime | None) -> str | None:
    """Serialize datetime to ISO-8601 string for JSON/Text column storage."""
    return dt.isoformat() if dt is not None else None


def _str_to_dt(s: str | None) -> datetime | None:
    """Deserialize an ISO-8601 string back to a timezone-aware UTC datetime."""
    if s is None:
        return None
    dt = datetime.fromisoformat(s)
    # Coerce naive datetimes from SQLite (no timezone stored) to UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _job_to_row(job: Job) -> dict[str, Any]:
    """
    Convert a ``Job`` frozen dataclass to a flat dict of column values.

    All complex fields (auth_snapshot, metadata, task_payload) are
    JSON-serialized to Text.  ``result`` (bytes) is stored as-is in
    LargeBinary.  Datetimes are stored as timezone-aware ISO strings.

    Args:
        job: The ``Job`` instance to convert.

    Returns:
        Dict mapping column names to Python values ready for SQLAlchemy Core.
    """
    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "result": job.result,
        "error": job.error,
        "callback_url": job.callback_url,
        "auth_snapshot": (
            json.dumps(job.auth_snapshot) if job.auth_snapshot is not None else None
        ),
        "request_token": job.request_token,
        "job_metadata": json.dumps(job.metadata),
        "task_payload": (
            json.dumps(job.task_payload.to_dict())
            if job.task_payload is not None
            else None
        ),
    }


def _row_to_job(row: Any) -> Job:
    """
    Convert a SQLAlchemy Core row to a ``Job`` frozen dataclass.

    Reverses ``_job_to_row``: deserializes JSON fields, parses datetimes,
    and coerces the status string back to ``JobStatus``.

    Args:
        row: A row returned by ``conn.execute(select(_jobs_table))``.

    Returns:
        An immutable ``Job`` value object.

    Edge cases:
        - ``row.started_at`` / ``row.completed_at`` may be ``None`` (not yet started).
        - ``row.auth_snapshot`` may be ``None`` for anonymous contexts.
        - SQLite returns naive datetimes — coerced to UTC by ``_str_to_dt`` logic
          applied below via ``_ensure_tz()``.
    """

    def _ensure_tz(dt: datetime | None) -> datetime | None:
        """Coerce naive datetimes (SQLite) to UTC."""
        if dt is None:
            return None
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

    task_payload: TaskPayload | None = None
    if row.task_payload is not None:
        task_payload = TaskPayload.from_dict(json.loads(row.task_payload))

    auth_snapshot: dict[str, Any] | None = None
    if row.auth_snapshot is not None:
        auth_snapshot = json.loads(row.auth_snapshot)

    metadata: dict[str, Any] = {}
    if row.job_metadata is not None:
        metadata = json.loads(row.job_metadata)

    return Job(
        job_id=row.job_id,
        status=JobStatus(row.status),
        created_at=_ensure_tz(row.created_at),
        started_at=_ensure_tz(row.started_at),
        completed_at=_ensure_tz(row.completed_at),
        result=row.result,
        error=row.error,
        callback_url=row.callback_url,
        auth_snapshot=auth_snapshot,
        request_token=row.request_token,
        metadata=metadata,
        task_payload=task_payload,
    )


# ── SAJobStore ────────────────────────────────────────────────────────────────


class SAJobStore(AbstractJobStore):
    """
    SQLAlchemy async implementation of ``AbstractJobStore``.

    Each method opens a fresh connection from the engine's pool and
    auto-commits.  There is no shared session — the job store is infrastructure
    (like ``OutboxRelay``) and does not participate in application UoW.

    DESIGN: AsyncEngine directly (not async_sessionmaker)
        ✅ Cleaner for infrastructure — no "session factory" wiring needed.
        ✅ ``ensure_table()`` only needs the engine, not a session.
        ✅ Consistent with ``SAEncryptionKeyStore`` pattern in this codebase.
        ❌ Slightly lower-level than ORM sessions — explicit SQL everywhere.

    Thread safety:  ✅ AsyncEngine pool is coroutine-safe; connections are per-call.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        engine: Async SQLAlchemy engine for the target database.

    Edge cases:
        - Call ``await store.ensure_table()`` at startup, or add ``jobs_metadata``
          to your Alembic ``target_metadata``.
        - ``save()`` uses DELETE + INSERT (idempotent upsert) compatible with
          SQLite and PostgreSQL alike.
        - ``try_claim()`` atomicity: correct for single-process deployments.
          For distributed multi-replica deployments, apply Feature 12
          (SELECT FOR UPDATE SKIP LOCKED on PostgreSQL).

    Example::

        engine = create_async_engine("postgresql+asyncpg://...")
        store = SAJobStore(engine)
        await store.ensure_table()

        job = Job()
        await store.save(job)
        claimed = await store.try_claim(job.job_id)
    """

    def __init__(self, engine: AsyncEngine) -> None:
        """
        Args:
            engine: Async SQLAlchemy engine — shared across all store operations.
        """
        self._engine = engine

    # ── Schema management ─────────────────────────────────────────────────────

    async def ensure_table(self) -> None:
        """
        Create the ``varco_jobs`` table if it does not already exist.

        Idempotent — safe to call at every startup.  For production deployments,
        prefer Alembic migrations (include ``jobs_metadata`` in target_metadata).

        Async safety: ✅ Uses ``engine.begin()`` — auto-commits the DDL.
        """
        async with self._engine.begin() as conn:
            await conn.run_sync(_metadata.create_all, checkfirst=True)
        _logger.debug("SAJobStore.ensure_table: varco_jobs table is ready.")

    # ── AbstractJobStore implementation ───────────────────────────────────────

    async def save(self, job: Job) -> None:
        """
        Persist or update a job (upsert semantics).

        Implemented as DELETE + INSERT to be compatible with both SQLite
        (unit tests) and PostgreSQL (production).  Each call is its own
        committed transaction.

        Args:
            job: The ``Job`` to persist.

        Edge cases:
            - Saving a terminal job (COMPLETED, FAILED, CANCELLED) is valid.
            - Concurrent saves to the same ``job_id`` → last write wins
              (DELETE+INSERT within a transaction prevents partial writes).

        Async safety: ✅ Uses ``engine.begin()`` — one atomic transaction.
        """
        row = _job_to_row(job)
        async with self._engine.begin() as conn:
            # DELETE existing row — silent no-op if not found.
            await conn.execute(
                sa.delete(_jobs_table).where(_jobs_table.c.job_id == job.job_id)
            )
            # INSERT fresh row — the DELETE ensures no IntegrityError.
            await conn.execute(sa.insert(_jobs_table).values(**row))
        _logger.debug("SAJobStore.save: job_id=%s status=%s", job.job_id, job.status)

    async def get(self, job_id: UUID) -> Job | None:
        """
        Retrieve a ``Job`` by its ``job_id``.

        Args:
            job_id: The unique job identifier.

        Returns:
            The ``Job`` if found, or ``None`` if not found.

        Async safety: ✅ Uses ``engine.connect()`` — read-only, no commit.
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(
                sa.select(_jobs_table).where(_jobs_table.c.job_id == job_id)
            )
            row = result.fetchone()
        if row is None:
            return None
        return _row_to_job(row)

    async def list_by_status(
        self,
        status: JobStatus,
        *,
        limit: int = 100,
    ) -> list[Job]:
        """
        Return up to ``limit`` jobs with the given ``status``, oldest first.

        Args:
            status: Filter by this lifecycle state.
            limit:  Maximum number of results.

        Returns:
            List of matching ``Job`` objects, ordered by ``created_at ASC``.

        Async safety: ✅ Uses ``engine.connect()`` — read-only, no commit.
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(
                sa.select(_jobs_table)
                .where(_jobs_table.c.status == status.value)
                .order_by(_jobs_table.c.created_at.asc())
                .limit(limit)
            )
            rows = result.fetchall()
        jobs = [_row_to_job(r) for r in rows]
        _logger.debug(
            "SAJobStore.list_by_status: status=%s returned %d jobs",
            status,
            len(jobs),
        )
        return jobs

    async def delete(self, job_id: UUID) -> None:
        """
        Remove a ``Job`` from the store.  Silent no-op for unknown IDs.

        Args:
            job_id: The unique job identifier.

        Async safety: ✅ Uses ``engine.begin()`` — single committed transaction.
        """
        async with self._engine.begin() as conn:
            await conn.execute(
                sa.delete(_jobs_table).where(_jobs_table.c.job_id == job_id)
            )
        _logger.debug("SAJobStore.delete: job_id=%s", job_id)

    async def try_claim(self, job_id: UUID) -> Job | None:
        """
        Atomically transition a PENDING job to RUNNING state.

        On **PostgreSQL**, uses ``SELECT FOR UPDATE SKIP LOCKED`` so that
        concurrent workers never block each other: if the target row is
        already locked by another process, the select returns no rows and
        this method returns ``None`` immediately rather than waiting.

        On **SQLite** (and other dialects), falls back to a plain
        ``SELECT + UPDATE`` within a single transaction — sufficient for
        single-process deployments and unit tests.

        DESIGN: dialect-conditional SKIP LOCKED
            ✅ True distributed safety on PostgreSQL — no mutual blocking
               between concurrent runners claiming the same job.
            ✅ Backward-compatible — SQLite unit tests use the simple path.
            ✅ Both paths are fully atomic (``engine.begin()``).
            ❌ Dialect detection uses ``engine.dialect.name`` — callers
               using a non-PostgreSQL dialect that DOES support SKIP LOCKED
               (e.g. CockroachDB) won't get it automatically.  Override
               ``_use_skip_locked()`` or subclass if needed.

        Args:
            job_id: The UUID of the PENDING job to claim.

        Returns:
            The claimed ``Job`` in RUNNING state, or ``None`` if:
            - The job is not found.
            - The job is not in PENDING state.
            - (PostgreSQL only) The row is already locked by a concurrent
              worker (SKIP LOCKED skips it).

        Async safety: ✅ Uses ``engine.begin()`` — fully atomic transaction.
        """
        use_skip_locked = self._engine.dialect.name == "postgresql"

        async with self._engine.begin() as conn:
            select_stmt = sa.select(_jobs_table).where(_jobs_table.c.job_id == job_id)
            if use_skip_locked:
                # PostgreSQL: acquire a row-level lock; skip if already locked.
                # Another runner holding this lock is currently claiming the job —
                # we return None immediately instead of waiting for its commit.
                select_stmt = select_stmt.with_for_update(skip_locked=True)

            result = await conn.execute(select_stmt)
            row = result.fetchone()

            if row is None or row.status != JobStatus.PENDING:
                # Job not found, already locked by another worker (SKIP LOCKED),
                # or already in a non-PENDING state — not claimable.
                return None

            # Transition PENDING → RUNNING within the same transaction.
            now = datetime.now(timezone.utc)
            await conn.execute(
                sa.update(_jobs_table)
                .where(_jobs_table.c.job_id == job_id)
                .values(status=JobStatus.RUNNING, started_at=now)
            )
            # Re-build the Job with updated fields (frozen — can't mutate).
            job = _row_to_job(row)

        running_job = job.as_running()
        _logger.debug(
            "SAJobStore.try_claim: claimed job_id=%s → RUNNING (skip_locked=%s)",
            job_id,
            use_skip_locked,
        )
        return running_job

    def __repr__(self) -> str:
        return f"SAJobStore(engine={self._engine!r})"


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SAJobStore",
    "jobs_metadata",
]
