"""
varco_beanie.job_store
======================
Beanie (pymongo / MongoDB) implementation of ``AbstractJobStore``.

MongoDB differs from relational databases in several ways relevant to job storage:

- **Schema-less**: ``JobDocument`` stores complex fields (``auth_snapshot``,
  ``metadata``, ``task_payload``) as native BSON subdocuments — no JSON
  serialization to Text columns required.
- **Atomic claim**: MongoDB's ``find_one_and_update`` (``findAndModify``)
  provides an atomic PENDING → RUNNING transition in a single round-trip.
  This replaces PostgreSQL's ``SELECT FOR UPDATE SKIP LOCKED`` pattern.
- **No explicit transactions needed**: ``try_claim`` is atomic by default —
  MongoDB's ``findAndModify`` is the built-in primitive for this pattern.

Collection
----------
``JobDocument`` maps to the ``varco_jobs`` MongoDB collection.
It must be included in your ``init_beanie()`` or
``BeanieRepositoryProvider.register()`` call::

    from varco_beanie.job_store import JobDocument

    # Option A — pass to init_beanie directly
    await init_beanie(database=db, document_models=[..., JobDocument])

    # Option B — register with BeanieRepositoryProvider before provider.init()
    provider.register(JobDocument)
    await provider.init()

Usage::

    from varco_beanie.job_store import BeanieJobStore

    store = BeanieJobStore()
    job = Job()
    await store.save(job)
    running = await store.try_claim(job.job_id)

DESIGN: native BSON dict storage over JSON-in-Text
    ✅ Richer queries possible — filter on ``task_payload.task_name`` etc.
    ✅ No serialization overhead — MongoDB stores dicts as BSON directly.
    ✅ Consistent with ``BeanieOutboxRepository`` and ``BeanieInboxRepository``.
    ❌ ``result`` (bytes) is stored as BSON Binary — same as SAJobStore's
       LargeBinary, but BSON Binary carries a subtype byte overhead.

DESIGN: try_claim() uses find_one_and_update (MongoDB findAndModify)
    ✅ Atomic PENDING → RUNNING in one server-side operation — no TOCTOU race.
    ✅ No explicit transaction required — ``findAndModify`` is MongoDB's
       built-in atomic update primitive.
    ✅ Returns the AFTER document so we can build the Job without a second fetch.
    ❌ findAndModify acquires a write lock on the matched document — under very
       high contention this adds latency.  For typical job-runner workloads
       (small number of PENDING jobs recovered on restart) this is negligible.

Thread safety:  ✅ Beanie operates on the Motor/pymongo async client pool — no
                    shared mutable state in ``BeanieJobStore`` itself.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🔍 https://beanie-odm.dev/tutorial/defining-a-document/
  Beanie Document — class definition and collection configuration
- 🔍 https://www.mongodb.com/docs/manual/reference/command/findAndModify/
  MongoDB findAndModify — atomic read-modify-write command
- 🐍 https://motor.readthedocs.io/en/stable/api-asyncio/asyncio_motor_collection.html
  Motor AsyncIOMotorCollection — pymongo async interface
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from beanie import Document, UpdateResponse
from beanie.operators import Set
from pydantic import Field

from varco_core.job.base import AbstractJobStore, Job, JobStatus
from varco_core.job.task import TaskPayload

_logger = logging.getLogger(__name__)


# ── JobDocument ───────────────────────────────────────────────────────────────


class JobDocument(Document):
    """
    Beanie document representing a single background job.

    Maps to the ``varco_jobs`` MongoDB collection.

    Register this document in your ``init_beanie()`` or
    ``BeanieRepositoryProvider.register()`` call before using
    ``BeanieJobStore``.

    DESIGN: UUID primary key over ObjectId
        ✅ Matches ``Job.job_id`` (UUIDv4) — no separate mapping needed.
        ✅ Consistent with ``OutboxDocument`` and ``InboxDocument`` patterns.
        ✅ Job IDs can be generated client-side and returned in the 202 response
           before the document is persisted.
        ❌ UUID keys are slightly larger than ObjectId (16 vs 12 bytes).

    DESIGN: dict fields (auth_snapshot, job_metadata, task_payload) as BSON
        ✅ MongoDB stores Python dicts as BSON subdocuments natively — no JSON
           serialization to string (unlike the SQLAlchemy implementation).
        ✅ Enables rich queries on subdocument fields in the future.
        ❌ BSON subdocuments cannot be easily diffed via string comparison.

    Thread safety:  ✅ Document class is a static definition — no mutable state.
    Async safety:   ✅ All Beanie methods are ``async def``.

    Attributes:
        id:            UUIDv4 — matches ``Job.job_id``.
        status:        String value of ``JobStatus`` enum (e.g. ``"pending"``).
        created_at:    UTC timestamp of job creation.
        started_at:    UTC timestamp when the job transitioned to RUNNING.
                       ``None`` until claimed.
        completed_at:  UTC timestamp of terminal transition.  ``None`` while running.
        result:        Opaque bytes — caller decides serialization format.
                       ``None`` for non-result jobs (fire-and-forget, FAILED).
        error:         Human-readable failure message.  ``None`` on success.
        callback_url:  Optional webhook URL for completion notification.
        auth_snapshot: Serialized ``AuthContext`` as a BSON subdocument.
                       ``None`` for anonymous/unauthenticated jobs.
        request_token: Raw Bearer JWT for audit and callback authentication.
        job_metadata:  Free-form ``dict`` of extra data (e.g. tenant_id, source).
        task_payload:  ``TaskPayload.to_dict()`` as a BSON subdocument.
                       ``None`` for non-recoverable (no-retry) jobs.

    Edge cases:
        - ``JobDocument`` must be registered with Beanie before any method is
          called.  Beanie raises ``CollectionWasNotInitialized`` otherwise.
        - ``result`` is stored as BSON Binary (subtype 0).  Round-trip is lossless.
        - ``created_at`` from MongoDB may arrive as a naive datetime if the
          pymongo BSON codec strips timezone info.  ``BeanieJobStore`` coerces
          all naive datetimes to UTC when converting to ``Job``.
    """

    # UUID primary key — overrides Beanie's default ObjectId pk.
    id: UUID = Field(default_factory=uuid4)

    status: str
    """String value of JobStatus (e.g. "pending", "running", "completed")."""

    created_at: datetime
    """UTC creation timestamp."""

    started_at: datetime | None = None
    """UTC timestamp of PENDING → RUNNING transition; None until claimed."""

    completed_at: datetime | None = None
    """UTC timestamp of terminal state; None while still running."""

    result: bytes | None = None
    """Opaque result bytes; None for fire-and-forget or failed jobs."""

    error: str | None = None
    """Human-readable failure message; None on success."""

    callback_url: str | None = None
    """Optional webhook URL for completion notification."""

    # BSON subdocument — stored as-is; no JSON serialization needed.
    auth_snapshot: dict[str, Any] | None = None
    """Serialized AuthContext for background workers; None for anonymous jobs."""

    request_token: str | None = None
    """Raw Bearer JWT for audit trail and callback auth."""

    # BSON subdocument — default empty dict avoids None checks downstream.
    job_metadata: dict[str, Any] = Field(default_factory=dict)
    """Free-form extra data dict (e.g. tenant_id, trace_id)."""

    task_payload: dict[str, Any] | None = None
    """TaskPayload.to_dict() as BSON subdocument; None for non-recoverable jobs."""

    class Settings:
        """Beanie collection configuration."""

        # Collection name in MongoDB — all varco job entries live here.
        # DESIGN: "varco_jobs" matches SAJobStore table name for consistency.
        name = "varco_jobs"

        # DESIGN: no indexes declared here — callers can add status+created_at
        # compound index via Beanie's index management or a separate migration.
        # Avoiding surprise schema side-effects on first import.
        indexes: list = []

    def __repr__(self) -> str:
        return (
            f"JobDocument("
            f"id={self.id}, "
            f"status={self.status!r}, "
            f"created_at={self.created_at!r})"
        )


# ── Serialization helpers ─────────────────────────────────────────────────────


def _ensure_tz(dt: datetime | None) -> datetime | None:
    """
    Coerce a naive ``datetime`` to UTC.

    MongoDB pymongo may strip timezone info from datetimes depending on the
    codec configuration.  This helper ensures all datetimes have a tzinfo.

    Args:
        dt: A ``datetime`` that may or may not have ``tzinfo`` set.

    Returns:
        The same datetime if already timezone-aware, or a UTC-aware copy if naive.
        ``None`` if the input is ``None``.

    Edge cases:
        - Naive datetime → replaced with UTC tzinfo.
        - Already timezone-aware → returned unchanged (no conversion).
        - ``None`` → ``None``.
    """
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _job_to_doc(job: Job) -> JobDocument:
    """
    Convert a ``Job`` frozen dataclass to a ``JobDocument``.

    Complex fields (``task_payload``) are stored as BSON subdocuments via
    ``TaskPayload.to_dict()``.  The ``result`` bytes field maps directly to
    BSON Binary.

    Args:
        job: The ``Job`` instance to convert.

    Returns:
        A ``JobDocument`` ready for Beanie ``insert()`` or comparison.

    Edge cases:
        - ``job.task_payload`` is ``None`` → ``task_payload`` field is ``None``.
        - ``job.metadata`` may be empty dict → stored as ``{}`` (not ``None``).
    """
    return JobDocument(
        id=job.job_id,
        status=job.status.value,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        result=job.result,
        error=job.error,
        callback_url=job.callback_url,
        # BSON subdocument — stored as-is; None for anonymous.
        auth_snapshot=job.auth_snapshot,
        request_token=job.request_token,
        # Always a dict (empty dict for jobs with no metadata).
        job_metadata=job.metadata,
        # to_dict() converts TaskPayload to a plain dict for BSON storage.
        task_payload=(
            job.task_payload.to_dict() if job.task_payload is not None else None
        ),
    )


def _doc_to_job(doc: JobDocument) -> Job:
    """
    Convert a ``JobDocument`` to a ``Job`` frozen dataclass.

    Reverses ``_job_to_doc``: coerces naive datetimes to UTC, reconstructs
    ``TaskPayload`` from the stored dict, and re-creates ``JobStatus`` enum.

    Args:
        doc: A ``JobDocument`` returned by a Beanie query.

    Returns:
        An immutable ``Job`` value object.

    Edge cases:
        - ``doc.started_at`` / ``doc.completed_at`` may be ``None``.
        - ``doc.auth_snapshot`` may be ``None`` for anonymous contexts.
        - ``doc.created_at`` from MongoDB may arrive as naive — coerced to UTC.
    """
    task_payload: TaskPayload | None = None
    if doc.task_payload is not None:
        # Reconstruct TaskPayload from the BSON subdocument dict.
        task_payload = TaskPayload.from_dict(doc.task_payload)

    return Job(
        job_id=doc.id,
        status=JobStatus(doc.status),
        created_at=_ensure_tz(doc.created_at),
        started_at=_ensure_tz(doc.started_at),
        completed_at=_ensure_tz(doc.completed_at),
        result=doc.result,
        error=doc.error,
        callback_url=doc.callback_url,
        auth_snapshot=doc.auth_snapshot,
        request_token=doc.request_token,
        # job_metadata defaults to {} — never None on the domain side.
        metadata=doc.job_metadata,
        task_payload=task_payload,
    )


# ── BeanieJobStore ────────────────────────────────────────────────────────────


class BeanieJobStore(AbstractJobStore):
    """
    Beanie (pymongo / MongoDB) implementation of ``AbstractJobStore``.

    Persists ``Job`` objects in the ``varco_jobs`` MongoDB collection via
    ``JobDocument``.

    All methods are self-contained — no shared session or UoW.  The store
    operates as a standalone infrastructure component, analogous to
    ``SAJobStore`` on the SQLAlchemy side.

    ``try_claim()`` uses MongoDB's ``findAndModify`` (via Beanie's
    ``find_one(...).update_one(..., response_type=UpdateResponse.NEW_DOCUMENT)``)
    to atomically transition a PENDING job to RUNNING in a single server-side
    operation — the natural MongoDB equivalent of PostgreSQL's
    ``SELECT FOR UPDATE SKIP LOCKED``.

    DESIGN: no shared session
        ✅ Simpler API — callers do not need to manage session lifetimes.
        ✅ Infrastructure component: job store is independent of domain UoW.
        ✅ ``try_claim()`` atomicity does not require a session — MongoDB's
           ``findAndModify`` is atomic by itself.
        ❌ Each method opens its own connection from the Motor pool.  For very
           high-throughput scenarios, batching via a session may be faster.

    DESIGN: upsert via delete + insert (save())
        ✅ Works correctly on Beanie 2.x — no dialect-specific upsert needed.
        ✅ Atomic within MongoDB's single-document guarantees (delete and insert
           are separate operations, but the document model is a single doc).
        ❌ Two round-trips per save() — accept this for simplicity and
           cross-version compatibility.

    Thread safety:  ✅ No mutable instance state — Motor pool is coroutine-safe.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        (none) — ``BeanieJobStore`` uses the Beanie global document registry
                 initialized by ``init_beanie()`` or ``provider.init()``.

    Edge cases:
        - ``JobDocument`` must be registered with Beanie before any method is
          called.  Add it to ``init_beanie(document_models=[..., JobDocument])``.
        - ``save()`` uses delete + insert (upsert via two round-trips).
        - ``try_claim()`` atomicity: correct under concurrent multi-replica
          deployments — MongoDB's ``findAndModify`` ensures exactly one caller
          transitions PENDING → RUNNING.

    Example::

        store = BeanieJobStore()
        job = Job()
        await store.save(job)
        claimed = await store.try_claim(job.job_id)
    """

    # ── AbstractJobStore implementation ───────────────────────────────────────

    async def save(self, job: Job) -> None:
        """
        Persist or update a job (upsert semantics via delete + insert).

        Deletes any existing document with the same ``job_id`` before
        inserting the new state, ensuring the stored document always matches
        the caller's intent (including terminal states like COMPLETED or FAILED).

        Args:
            job: The ``Job`` to persist.

        Raises:
            beanie.exceptions.DocumentWasNotSaved: If Beanie fails to insert.
            RuntimeError: If ``JobDocument`` was not registered with Beanie.

        Edge cases:
            - Saving a terminal job (COMPLETED, FAILED, CANCELLED) is valid.
            - Concurrent saves to the same ``job_id``: last-write-wins (the
              delete+insert pair is NOT atomic across both operations; see
              ``try_claim()`` for the distributed-safe claim primitive).
            - If the delete succeeds but insert fails, the job is lost —
              acceptable for the transient in-flight case (recover via
              ``list_by_status(RUNNING)`` on restart).

        Async safety: ✅ Awaits both delete and insert sequentially.
        """
        doc = _job_to_doc(job)

        # Delete any existing row first (silent no-op if not found).
        # This is the same pattern as SAJobStore's delete + insert upsert,
        # adapted for Beanie's Document API.
        await JobDocument.find(JobDocument.id == job.job_id).delete()

        # Insert the fresh state.
        await doc.insert()

        _logger.debug(
            "BeanieJobStore.save: job_id=%s status=%s", job.job_id, job.status
        )

    async def get(self, job_id: UUID) -> Job | None:
        """
        Retrieve a ``Job`` by its ``job_id``.

        Args:
            job_id: The unique job identifier.

        Returns:
            The ``Job`` if found, or ``None`` if not found.

        Raises:
            RuntimeError: If ``JobDocument`` was not registered with Beanie.

        Edge cases:
            - Unknown ``job_id`` → returns ``None``.
            - ``created_at`` / ``started_at`` naive datetimes from MongoDB are
              coerced to UTC by ``_doc_to_job``.

        Async safety: ✅ Awaits ``find_one()``.
        """
        doc = await JobDocument.find_one(JobDocument.id == job_id)
        if doc is None:
            return None
        return _doc_to_job(doc)

    async def list_by_status(
        self,
        status: JobStatus,
        *,
        limit: int = 100,
    ) -> list[Job]:
        """
        Return up to ``limit`` jobs with the given ``status``, oldest first.

        Uses ``find(...).sort(+created_at).limit(n)`` — an in-memory sort
        unless a compound ``{ status: 1, created_at: 1 }`` index exists.

        Args:
            status: Filter by this lifecycle state.
            limit:  Maximum number of results.

        Returns:
            List of matching ``Job`` objects, ordered by ``created_at ASC``.
            Empty list if no matching documents.

        Raises:
            RuntimeError: If ``JobDocument`` was not registered with Beanie.

        Edge cases:
            - Sort without an index is O(N scan) — acceptable for small
              collections (<10k jobs).  Add a status+created_at index for
              production workloads.
            - ``limit`` applies after the sort; MongoDB may fetch more docs
              internally depending on cursor plan.

        Async safety: ✅ Awaits the Beanie find chain.
        """
        docs = (
            await JobDocument.find(JobDocument.status == status.value)
            .sort(+JobDocument.created_at)
            .limit(limit)
            .to_list()
        )
        jobs = [_doc_to_job(d) for d in docs]
        _logger.debug(
            "BeanieJobStore.list_by_status: status=%s returned %d jobs",
            status,
            len(jobs),
        )
        return jobs

    async def delete(self, job_id: UUID) -> None:
        """
        Remove a ``Job`` from the store.  Silent no-op for unknown IDs.

        Args:
            job_id: The unique job identifier.

        Raises:
            RuntimeError: If ``JobDocument`` was not registered with Beanie.

        Edge cases:
            - Unknown ``job_id`` → Beanie ``find().delete()`` deletes 0
              documents and returns without error.

        Async safety: ✅ Awaits ``find().delete()``.
        """
        await JobDocument.find(JobDocument.id == job_id).delete()
        _logger.debug("BeanieJobStore.delete: job_id=%s", job_id)

    async def try_claim(self, job_id: UUID) -> Job | None:
        """
        Atomically transition a PENDING job to RUNNING state.

        Uses MongoDB's ``findAndModify`` (via Beanie's
        ``find_one(...).update_one(..., response_type=UpdateResponse.NEW_DOCUMENT)``)
        to atomically locate a PENDING job and update it to RUNNING in a single
        server-side operation.

        If no document matches (job not found or not PENDING), the operation
        returns ``None`` immediately — there is no blocking, no queue, and no
        side effects.

        DESIGN: findAndModify over find_one + update (two-step)
            ✅ Atomic — MongoDB guarantees exactly one concurrent caller
               transitions PENDING → RUNNING, even across multi-replica deployments.
            ✅ Single round-trip on both success and no-match paths.
            ✅ Returns the AFTER document (``NEW_DOCUMENT``) so we can build the
               ``Job`` without a second ``get()`` round-trip.
            ❌ ``findAndModify`` acquires a write lock; under very high contention
               (many workers racing on the same PENDING job) this serializes them.
               In practice, a PENDING job is claimed by only one runner — contention
               is minimal.

        Args:
            job_id: The UUID of the PENDING job to claim.

        Returns:
            The claimed ``Job`` in RUNNING state, or ``None`` if:
            - The job is not found.
            - The job is not in PENDING state.

        Raises:
            RuntimeError: If ``JobDocument`` was not registered with Beanie.

        Edge cases:
            - Unknown ``job_id`` → ``find_one_and_update`` matches nothing → ``None``.
            - RUNNING / COMPLETED / FAILED / CANCELLED → not PENDING →
              filter does not match → ``None``.
            - Concurrent ``try_claim()`` on the same job: only the first request
              that reaches MongoDB transitions the status; all others see no match
              and return ``None``.

        Async safety: ✅ Single ``findAndModify`` call — no explicit locking needed.
        """
        now = datetime.now(timezone.utc)

        # Atomically: find a PENDING document for this job_id AND update it to
        # RUNNING in one MongoDB findAndModify round-trip.
        # UpdateResponse.NEW_DOCUMENT → returns the document AFTER the update.
        # If no document matches (not found or not PENDING), Beanie returns None.
        updated_doc: JobDocument | None = await JobDocument.find_one(
            JobDocument.id == job_id,
            JobDocument.status == JobStatus.PENDING.value,
        ).update_one(
            Set(
                {
                    JobDocument.status: JobStatus.RUNNING.value,
                    JobDocument.started_at: now,
                }
            ),
            response_type=UpdateResponse.NEW_DOCUMENT,
        )

        if updated_doc is None:
            # Job not found or not PENDING — not claimable.
            return None

        _logger.debug("BeanieJobStore.try_claim: claimed job_id=%s → RUNNING", job_id)
        return _doc_to_job(updated_doc)

    def __repr__(self) -> str:
        return "BeanieJobStore()"


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "BeanieJobStore",
    "JobDocument",
]
