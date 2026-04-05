"""
varco_core.job.base
===================
Background job domain types and abstract runner/store contracts.

Problem
-------
Some HTTP operations are too slow to complete within a request/response
cycle (bulk imports, PDF rendering, ML inference, etc.).  The async job
pattern provides a standard way to:

1. Accept a slow request immediately (202 Accepted + job_id).
2. Execute the work in a background asyncio task.
3. Poll for status / result at GET /jobs/{job_id}.
4. Optionally call back a webhook URL on completion.

Components
----------
``JobStatus``
    StrEnum of lifecycle states: PENDING → RUNNING → COMPLETED / FAILED / CANCELLED.

``Job``
    Frozen dataclass representing one background job.  All state transitions
    return a NEW ``Job`` instance via ``dataclasses.replace()``.

``AbstractJobStore``
    Persistence ABC for ``Job`` objects.  Implemented in varco_fastapi
    (InMemoryJobStore) or backend packages (Redis, SQL).

``AbstractJobRunner``
    Execution ABC that submits, tracks, and cancels asyncio coroutines.
    Implemented in varco_fastapi (JobRunner) which manages asyncio.Tasks.

``auth_context_to_snapshot`` / ``auth_context_from_snapshot``
    Serialization helpers for ``AuthContext``.  Background workers must
    execute with the same identity/grants as the originating HTTP request,
    but ``AuthContext`` contains ``frozenset`` values that are not directly
    JSON-serializable — these functions bridge that gap.

DESIGN: Job is immutable (frozen dataclass) with transition methods
    ✅ Safe to pass between asyncio Tasks without locking
    ✅ State transitions are explicit and traceable (new instance per change)
    ✅ Hashable — can be stored in sets for deduplication
    ❌ More verbose than mutating a dict — mitigated by transition helpers

DESIGN: auth_snapshot is dict[str, Any] rather than AuthContext
    ✅ JSON-serializable — can be persisted to any backend without custom codecs
    ✅ Decoupled from AuthContext schema changes (snapshot is a stable format)
    ✅ Can be passed as a POST body to webhook callbacks for re-authentication
    ❌ Type safety is weaker — mitigated by auth_context_from_snapshot helper

Thread safety:  ⚠️  ``AbstractJobRunner.start()`` must be called from within
                    the running event loop.  ``stop()`` cancels all tasks.
Async safety:   ✅  All methods are ``async def``.

📚 Docs
- 🐍 https://docs.python.org/3/library/asyncio-task.html
  asyncio.create_task — background task creation pattern
- 📐 https://restfulapi.net/rest-api-design-tutorial-with-example/
  202 Accepted pattern — async job acceptance
"""

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Coroutine, TYPE_CHECKING
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from varco_core.auth.base import AuthContext
    from varco_core.job.task import TaskPayload, TaskRegistry, VarcoTask


# ── JobStatus ──────────────────────────────────────────────────────────────────


class JobStatus(StrEnum):
    """
    Lifecycle states for a background job.

    State machine::

        PENDING
            ↓ (runner picks it up)
        RUNNING
            ↓ (coroutine completes)         ↓ (coroutine raises)      ↓ (client cancels)
        COMPLETED                          FAILED                   CANCELLED

    Terminal states: COMPLETED, FAILED, CANCELLED — no further transitions.

    Thread safety:  ✅ StrEnum members are immutable singletons.
    Async safety:   ✅ Pure value; no I/O.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Return True if this status cannot transition to another state."""
        return self in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)


# ── Job ────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Job:
    """
    Immutable value object representing one background job.

    State transitions return a NEW ``Job`` instance via ``dataclasses.replace()``.
    Never mutate ``Job`` fields directly — use the ``as_*`` helper methods.

    The ``auth_snapshot`` and ``request_token`` fields capture the original
    HTTP request's identity so background workers execute with the same
    authorization grants as the originating request.

    Attributes:
        job_id:          Unique identifier for this job.
        status:          Current lifecycle state (default: PENDING).
        created_at:      UTC timestamp when the job was created.
        started_at:      UTC timestamp when execution began (None if PENDING).
        completed_at:    UTC timestamp when execution ended (None if PENDING/RUNNING).
        result:          Serialized result payload (None until COMPLETED).
                         Format is opaque bytes — callers decide serialization.
        error:           Error message string (None unless FAILED).
        callback_url:    Optional webhook URL to POST completion notification to.
        auth_snapshot:   Serialized ``AuthContext`` at request time.
                         JSON-safe dict — see ``auth_context_to_snapshot()``.
        request_token:   Raw Bearer JWT for audit trail and callback authentication.
        metadata:        Arbitrary extra data (excluded from equality and hashing).

    Thread safety:  ✅ frozen=True — immutable after construction.
    Async safety:   ✅ Pure value object; safe to share across tasks.

    Edge cases:
        - ``created_at`` defaults to ``datetime.now(timezone.utc)`` — always UTC.
        - ``result`` is opaque bytes.  The caller that submitted the job is
          responsible for knowing how to deserialize it.
        - Two ``Job`` instances with different ``metadata`` but identical other
          fields compare as equal (metadata is ``compare=False``).

    Example::

        job = Job(job_id=uuid4(), created_at=datetime.now(timezone.utc))
        job = job.as_running()
        # ... work happens ...
        job = job.as_completed(result=b'{"ok": true}')
    """

    # Required: unique job identifier
    job_id: UUID = field(default_factory=uuid4)

    # Current lifecycle state
    status: JobStatus = JobStatus.PENDING

    # UTC creation timestamp
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # UTC timestamp when execution began (None if not yet started)
    started_at: datetime | None = None

    # UTC timestamp when execution ended (None if not yet finished)
    completed_at: datetime | None = None

    # Serialized result payload — set on COMPLETED, None otherwise
    result: bytes | None = None

    # Error message — set on FAILED, None otherwise
    error: str | None = None

    # Optional webhook URL for completion notification
    callback_url: str | None = None

    # Serialized AuthContext at request time — plain dict for JSON-safety.
    # frozenset fields in AuthContext (roles, scopes) are serialized as sorted lists.
    auth_snapshot: dict[str, Any] | None = None

    # Raw Bearer JWT from the originating request — for audit trail + callback auth
    request_token: str | None = None

    # Arbitrary extra data — excluded from equality and hashing
    metadata: dict[str, Any] = field(default_factory=dict, compare=False, hash=False)

    # Serialized task invocation for named-task recovery.
    # When set, the job runner can re-invoke the task after restart by looking
    # up the function name in the TaskRegistry and calling it with stored args.
    # None for jobs submitted via the legacy coroutine-only path (non-recoverable).
    task_payload: TaskPayload | None = field(default=None, compare=False, hash=False)

    # ── State transitions (return new Job via dataclasses.replace) ─────────────

    def as_running(self) -> Job:
        """
        Transition this job to RUNNING state.

        Returns:
            New ``Job`` with ``status=RUNNING`` and ``started_at`` set to
            current UTC time.

        Raises:
            ValueError: If the current status is not PENDING.

        Edge cases:
            - Calling on a job already in RUNNING is a programming error —
              raises ``ValueError`` to catch double-start bugs early.
        """
        if self.status != JobStatus.PENDING:
            raise ValueError(
                f"Cannot transition job {self.job_id} to RUNNING from {self.status!r}. "
                "Only PENDING jobs can be started."
            )
        return dataclasses.replace(
            self,
            status=JobStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )

    def as_completed(self, result: bytes | None) -> Job:
        """
        Transition this job to COMPLETED state.

        Args:
            result: Serialized result payload.  Format is caller-defined.

        Returns:
            New ``Job`` with ``status=COMPLETED``, ``result`` set, and
            ``completed_at`` set to current UTC time.

        Raises:
            ValueError: If the current status is not RUNNING.

        Edge cases:
            - ``result`` may be empty bytes (``b""``) for void operations.
            - The previous ``error`` field is not cleared — it should already
              be ``None`` for a RUNNING → COMPLETED transition.
        """
        if self.status != JobStatus.RUNNING:
            raise ValueError(
                f"Cannot transition job {self.job_id} to COMPLETED from {self.status!r}. "
                "Only RUNNING jobs can complete."
            )
        return dataclasses.replace(
            self,
            status=JobStatus.COMPLETED,
            result=result,
            completed_at=datetime.now(timezone.utc),
        )

    def as_failed(self, error: str) -> Job:
        """
        Transition this job to FAILED state.

        Args:
            error: Human-readable error message describing the failure.

        Returns:
            New ``Job`` with ``status=FAILED``, ``error`` set, and
            ``completed_at`` set to current UTC time.

        Raises:
            ValueError: If the current status is not RUNNING.

        Edge cases:
            - ``error`` should be sanitized (no stack traces) before being
              stored — the raw exception message may contain sensitive data.
        """
        if self.status != JobStatus.RUNNING:
            raise ValueError(
                f"Cannot transition job {self.job_id} to FAILED from {self.status!r}. "
                "Only RUNNING jobs can fail."
            )
        return dataclasses.replace(
            self,
            status=JobStatus.FAILED,
            error=error,
            completed_at=datetime.now(timezone.utc),
        )

    def as_cancelled(self) -> Job:
        """
        Transition this job to CANCELLED state.

        Returns:
            New ``Job`` with ``status=CANCELLED`` and ``completed_at`` set
            to current UTC time.

        Raises:
            ValueError: If the job is already in a terminal state.

        Edge cases:
            - Cancellation is allowed from both PENDING and RUNNING states.
            - Cancelling a COMPLETED/FAILED job is an error — use this check
              to prevent accidental cancellation of finished work.
        """
        if self.status.is_terminal:
            raise ValueError(
                f"Cannot cancel job {self.job_id}: already in terminal state {self.status!r}."
            )
        return dataclasses.replace(
            self,
            status=JobStatus.CANCELLED,
            completed_at=datetime.now(timezone.utc),
        )


# ── Auth snapshot serialization ────────────────────────────────────────────────


def auth_context_to_snapshot(ctx: AuthContext) -> dict[str, Any]:
    """
    Serialize an ``AuthContext`` to a JSON-safe dict for job persistence.

    Background workers must run with the same identity/grants as the HTTP
    request that submitted them.  ``AuthContext`` contains ``frozenset``
    values which are not JSON-serializable — this function normalizes them.

    Serialization rules:
    - ``frozenset[str]`` (roles, scopes) → ``sorted(list(...))``
    - ``ResourceGrant`` tuple → list of ``{"resource": ..., "actions": [...]}`` dicts
    - ``metadata`` → included as-is (must already be JSON-safe; callers responsible)

    Args:
        ctx: The ``AuthContext`` to serialize.

    Returns:
        A ``dict[str, Any]`` that is safe to pass to ``json.dumps()`` and
        to store in any backend (in-memory dict, Redis, SQL JSONB column).

    Edge cases:
        - ``user_id`` is ``None`` for anonymous contexts — stored as ``None``.
        - ``metadata`` is included verbatim — the caller must ensure its values
          are JSON-serializable (str, int, float, bool, list, dict, None).
        - ``ResourceGrant.actions`` (frozenset[Action]) → sorted list of str.

    Thread safety:  ✅ Pure function; no shared state.
    Async safety:   ✅ Pure function; no I/O.

    Example::

        snapshot = auth_context_to_snapshot(ctx)
        # snapshot == {
        #     "user_id": "usr_123",
        #     "roles": ["admin", "editor"],
        #     "scopes": ["write:posts"],
        #     "grants": [{"resource": "posts", "actions": ["create", "read"]}],
        #     "metadata": {"tenant_id": "t1"},
        # }
    """
    return {
        "user_id": ctx.user_id,
        "roles": sorted(ctx.roles),
        "scopes": sorted(ctx.scopes),
        "grants": [
            {
                "resource": g.resource,
                "actions": sorted(str(a) for a in g.actions),
            }
            for g in ctx.grants
        ],
        "metadata": ctx.metadata,
    }


def auth_context_from_snapshot(snapshot: dict[str, Any]) -> AuthContext:
    """
    Reconstruct an ``AuthContext`` from a stored snapshot dict.

    Inverse of ``auth_context_to_snapshot()``.  Used by ``JobRunner`` to
    restore the original request's auth context before executing the job
    coroutine, so it runs with the correct identity and grants.

    Args:
        snapshot: A dict produced by ``auth_context_to_snapshot()``.

    Returns:
        An ``AuthContext`` with all fields restored from the snapshot.

    Raises:
        KeyError: If expected keys are missing from the snapshot.
        TypeError: If values have unexpected types.

    Edge cases:
        - Missing ``"metadata"`` key defaults to an empty dict — tolerates
          snapshots written before metadata was added.
        - Unknown keys in ``snapshot`` are ignored — forward-compatible.
        - Actions in ``grants`` are stored as raw strings; they compare
          equal to ``Action`` enum members (``Action`` is a ``StrEnum``).

    Thread safety:  ✅ Pure function; no shared state.
    Async safety:   ✅ Pure function; no I/O.
    """
    # Import here to avoid making job.base depend on auth at module level.
    # TYPE_CHECKING guard at the top covers type hints only.
    from varco_core.auth.base import Action, AuthContext, ResourceGrant

    grants = tuple(
        ResourceGrant(
            resource=g["resource"],
            actions=frozenset(Action(a) for a in g["actions"]),
        )
        for g in snapshot.get("grants", [])
    )

    return AuthContext(
        user_id=snapshot.get("user_id"),
        roles=frozenset(snapshot.get("roles", [])),
        scopes=frozenset(snapshot.get("scopes", [])),
        grants=grants,
        metadata=snapshot.get("metadata", {}),
    )


# ── AbstractJobStore ───────────────────────────────────────────────────────────


class AbstractJobStore(ABC):
    """
    Abstract persistence contract for ``Job`` objects.

    Implement this to provide job persistence:
    - ``InMemoryJobStore`` (varco_fastapi): dict-backed, no durability.
    - Redis store (future): TTL-based, suitable for distributed deployments.
    - SQL store (future): durable, queryable, backed by varco_sa.

    The HTTP layer reads/writes jobs via this interface.  Backend implementations
    must ensure that ``save()`` on an existing ``job_id`` replaces the stored value
    (upsert semantics).

    Thread safety:  ⚠️ Implementations must document their own thread safety.
                       ``InMemoryJobStore`` uses a lazy ``asyncio.Lock`` for safety.
    Async safety:   ✅ All methods are ``async def``.
    """

    @abstractmethod
    async def save(self, job: Job) -> None:
        """
        Persist or update a ``Job``.  Upsert semantics — if a job with the
        same ``job_id`` exists, it is replaced.

        Args:
            job: The job to save.

        Raises:
            Any backend-specific persistence error.

        Edge cases:
            - Saving a terminal job (COMPLETED, FAILED, CANCELLED) is valid —
              implementations should not reject terminal-state saves.
            - Concurrent saves for the same ``job_id`` are implementation-defined;
              last-write-wins is acceptable for the RUNNING → COMPLETED transition
              since only one task should ever hold a job.
        """

    @abstractmethod
    async def get(self, job_id: UUID) -> Job | None:
        """
        Retrieve a ``Job`` by its ``job_id``.

        Args:
            job_id: The unique job identifier.

        Returns:
            The ``Job`` if found, or ``None`` if not found.

        Edge cases:
            - Returns ``None`` for unknown job IDs — callers must check for None.
        """

    @abstractmethod
    async def list_by_status(
        self,
        status: JobStatus,
        *,
        limit: int = 100,
    ) -> list[Job]:
        """
        Return up to ``limit`` jobs matching ``status``, ordered by ``created_at``.

        Used by ``JobPoller`` to recover stale RUNNING jobs after restart.

        Args:
            status: Filter by this lifecycle state.
            limit:  Maximum number of results to return.

        Returns:
            List of matching ``Job`` objects, oldest first.

        Edge cases:
            - Returns an empty list if no matching jobs exist.
        """

    @abstractmethod
    async def delete(self, job_id: UUID) -> None:
        """
        Remove a ``Job`` from the store.

        Called by cleanup tasks to purge old completed jobs.

        Args:
            job_id: The unique job identifier.

        Edge cases:
            - Deleting an unknown ``job_id`` should be a silent no-op.
        """

    @abstractmethod
    async def try_claim(self, job_id: UUID) -> Job | None:
        """
        Atomically transition a PENDING job to RUNNING state.

        This is the distributed-safety primitive for job recovery.  When multiple
        runner instances start concurrently (e.g. after a rolling restart or in a
        multi-replica deployment), each calls ``try_claim()`` on every PENDING job
        they discover.  Only one runner succeeds — the others get ``None`` and skip.

        Implementations MUST guarantee atomicity:
        - In-memory: use an ``asyncio.Lock`` that wraps the read + write together.
        - Redis: use SET NX (set-if-not-exists) on a claim key, or WATCH + MULTI.
        - SQL: use ``SELECT ... FOR UPDATE SKIP LOCKED`` (PostgreSQL/MySQL).

        DESIGN: try_claim() over optimistic locking (check-then-act)
            ✅ Single round-trip on the "success" path for Redis/SQL
            ✅ Prevents double-execution with zero coordination between runners
            ✅ No distributed lock manager needed — store is the source of truth
            ❌ Requires store implementations to support atomic CAS (Redis, SQL, etc.)
               Simple dict-based stores must use a Lock, which only guards a single process.

        Args:
            job_id: The UUID of the job to claim.

        Returns:
            The claimed ``Job`` in RUNNING state if the claim succeeded.
            ``None`` if the job does not exist, is not in PENDING state, or was
            already claimed by another runner.

        Thread safety:  ✅ Implementations must ensure atomicity of the PENDING → RUNNING
                           check-and-set operation under concurrent callers.
        Async safety:   ✅ Must be ``async def``.

        Edge cases:
            - Unknown ``job_id`` → returns ``None`` (not an error).
            - Job in a non-PENDING state → returns ``None`` (already running or terminal).
            - Concurrent calls on the same ``job_id`` → exactly one returns the Job,
              all others return ``None``.
        """


# ── AbstractJobRunner ──────────────────────────────────────────────────────────


class AbstractJobRunner(ABC):
    """
    Abstract execution contract for background job coroutines.

    The runner wraps each submitted coroutine in an ``asyncio.Task``,
    tracks it by ``job_id``, and handles cancellation.

    The concrete ``JobRunner`` in varco_fastapi:
    - Creates one ``asyncio.Task`` per submitted coroutine.
    - Updates ``Job`` status in ``AbstractJobStore`` on start/complete/fail.
    - Emits ``JobProgressEvent`` events for SSE streaming.
    - Forwards auth context so the coroutine runs with the correct identity.

    DESIGN: AbstractJobRunner as ABC in varco_core (not varco_fastapi)
        ✅ Services in varco_core can declare ``Inject[AbstractJobRunner]``
           without depending on the HTTP layer.
        ✅ Testable with a stub implementation — no asyncio.Task needed in unit tests.
        ❌ Concrete implementations (asyncio tasks, Celery, etc.) still live
           in backend packages — this ABC adds a small indirection layer.

    DESIGN: enqueue() as the primary submission API (not submit())
        ✅ Saves the job to the store before scheduling the task — crash-safe.
           If the process dies between save and task creation, the JobPoller
           finds the PENDING job and can mark it FAILED or re-queue it.
        ✅ Single call site — callers cannot forget to persist before submitting.
        ❌ The save and task creation are not truly atomic across store + event loop.
           A crash *between* store.save() and asyncio.create_task() leaves a
           PENDING zombie — acceptable because the JobPoller recovers it.

    Thread safety:  ⚠️  ``start()`` must be called from within the running event loop.
    Async safety:   ✅  All methods are ``async def``.
    """

    @abstractmethod
    async def enqueue(
        self,
        job: "Job",
        coro: Coroutine[Any, Any, Any],
    ) -> None:
        """
        Persist ``job`` to the store as PENDING, then schedule ``coro`` for
        background execution.

        This is the **only correct way** to submit a job.  Callers must not
        call ``submit()`` directly — use ``enqueue()`` so the job is durably
        persisted before the asyncio.Task is created.  This ensures that a
        process crash between submission and execution leaves a recoverable
        PENDING record in the store rather than a silent loss.

        Steps performed by implementations:
        1. ``store.save(job)`` — persists PENDING record.
        2. ``submit(job.job_id, coro)`` — schedules the asyncio.Task.

        Args:
            job:  A ``Job`` instance in PENDING state.
            coro: The coroutine to execute in the background.

        Raises:
            Any store-specific error from ``store.save()``.

        Edge cases:
            - If ``store.save()`` raises, ``coro`` is closed immediately (no leak).
            - If the process crashes after ``save()`` but before the task starts,
              the ``JobPoller`` will find the PENDING job on restart and mark it
              FAILED (since it never transitions to RUNNING).

        Async safety:   ✅ Returns after scheduling; does not wait for completion.
        """

    @abstractmethod
    async def submit(
        self,
        job_id: UUID,
        coro: Coroutine[Any, Any, Any],
    ) -> None:
        """
        Schedule a coroutine for background execution under a pre-existing ``job_id``.

        **Prefer ``enqueue()`` over calling this directly.**  This method assumes
        the job already exists in the store as PENDING.  Calling it without a
        prior ``store.save()`` will cause ``_run_job`` to log an error and
        silently drop the coroutine.

        The runner:
        1. Loads the job from the store (must already exist in PENDING state).
        2. Transitions it to RUNNING and saves.
        3. Executes the coroutine inside an asyncio.Task.
        4. On completion → ``job.as_completed(result)`` saved to store.
        5. On failure → ``job.as_failed(str(exc))`` saved to store.

        Args:
            job_id: The ID of the pre-existing PENDING job.
            coro:   The coroutine to execute.

        Edge cases:
            - ``job_id`` not in store → task logs an error and closes ``coro``.
            - Job not in PENDING state → ``as_running()`` raises ValueError inside task.
            - Runner stopped before task finishes → task cancelled → FAILED in store.

        Async safety:   ✅ Returns immediately after scheduling the task.
        """

    @abstractmethod
    async def cancel(self, job_id: UUID) -> bool:
        """
        Cancel the running task for ``job_id``.

        Args:
            job_id: The job to cancel.

        Returns:
            ``True`` if the task was found and cancellation was requested.
            ``False`` if no active task exists for this job (already completed
            or never submitted to this runner instance).

        Edge cases:
            - Cancellation is cooperative — the coroutine must check for
              ``asyncio.CancelledError`` (which is raised automatically at
              the next ``await`` point).
            - ``cancel()`` returns immediately; the task may not have finished
              by the time this returns.  Poll the job store for final status.
        """

    @abstractmethod
    async def start(self) -> None:
        """
        Start the job runner.  Idempotent.

        Must be called from within a running event loop (i.e. inside an
        ``async`` function or at application startup).

        Edge cases:
            - Calling ``start()`` twice is safe — the second call is a no-op.
        """

    @abstractmethod
    async def enqueue_task(
        self,
        task: "VarcoTask",
        *args: Any,
        callback_url: str | None = None,
        auth_snapshot: dict[str, Any] | None = None,
        request_token: str | None = None,
        **kwargs: Any,
    ) -> UUID:
        """
        Submit a named task for background execution and return its job ID.

        Unlike ``enqueue(job, coro)`` which takes a bare coroutine, this method
        accepts a ``VarcoTask`` and its call arguments.  The runner serializes
        the invocation as a ``TaskPayload`` and stores it in the ``Job`` record,
        making the job recoverable after a process restart via ``recover()``.

        Steps performed by implementations:
        1. Build a ``TaskPayload`` from ``task.payload(*args, **kwargs)``.
        2. Create a ``Job`` in PENDING state with ``task_payload`` set.
        3. ``store.save(job)`` — persist the PENDING record with payload.
        4. ``submit(job.job_id, task(*args, **kwargs))`` — schedule the task.
        5. Return ``job.job_id``.

        Args:
            task:          The ``VarcoTask`` to execute.
            *args:         Positional arguments forwarded to the task function.
            callback_url:  Optional webhook URL to call on completion.
            auth_snapshot: Serialized ``AuthContext`` from the originating request.
            request_token: Raw Bearer JWT for audit trail and callback auth.
            **kwargs:      Keyword arguments forwarded to the task function.

        Returns:
            The ``UUID`` of the newly created job.

        Raises:
            Any store-specific error from ``store.save()``.

        Edge cases:
            - All values in ``args`` and ``kwargs`` must be JSON-serializable.
              If they are not, the job will fail at recovery time (not submission time
              unless the implementation calls ``payload.validate_serializable()``).
            - Process crash after ``store.save()`` but before the task runs →
              job stays PENDING; ``recover()`` will re-invoke on next startup.

        Async safety:   ✅ Returns after scheduling; does not wait for task completion.
        """

    @abstractmethod
    async def recover(self, registry: "TaskRegistry") -> int:
        """
        Re-submit all PENDING jobs that have a ``task_payload`` using ``try_claim()``.

        Called at application startup to resume jobs that were in-flight when the
        process died.  For each PENDING job with a ``task_payload``:

        1. ``store.try_claim(job_id)`` — atomically claim PENDING → RUNNING.
        2. If claim succeeds, look up the task in ``registry``.
        3. Re-invoke the task with the stored args — scheduling a new asyncio.Task.
        4. If the task name is not in the registry, log a warning and leave the
           job in RUNNING state (it will not progress until manually resolved).

        **Distributed safety**: ``try_claim()`` ensures that even when multiple
        runner instances start concurrently (rolling restart, multi-replica), each
        PENDING job is claimed by exactly one runner.

        Args:
            registry: The ``TaskRegistry`` containing all recoverable tasks.
                      Must be populated (via ``VarcoCRUDRouter.build_router()``)
                      before ``recover()`` is called.

        Returns:
            The number of jobs successfully re-submitted.

        Edge cases:
            - No PENDING jobs with ``task_payload`` → returns 0, no-op.
            - ``try_claim()`` returns ``None`` → job was claimed by another instance; skip.
            - Task name not in registry → logs a warning; returns 0 for that job.
            - ``task_payload`` is ``None`` → job was submitted via legacy path; skip
              (no recovery possible without a serialized payload).

        Async safety:   ✅ All I/O is awaited.  Safe to call from ``startup`` lifespan.
        """

    @abstractmethod
    async def stop(self, *, timeout: float = 30.0) -> None:
        """
        Stop the runner, cancelling all in-flight tasks.  Idempotent.

        Args:
            timeout: Seconds to wait for in-flight tasks to finish after
                     cancellation.  Tasks that don't finish within timeout
                     are abandoned.

        Edge cases:
            - Calling ``stop()`` before ``start()`` is a silent no-op.
            - In-flight jobs are transitioned to FAILED with a cancellation
              message in the store so callers can observe the final state.
        """


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "Job",
    "JobStatus",
    "AbstractJobStore",
    "AbstractJobRunner",
    "auth_context_to_snapshot",
    "auth_context_from_snapshot",
]

# VarcoTask / TaskRegistry are defined in task.py (same package) to avoid a
# circular import.  AbstractJobRunner references them via TYPE_CHECKING only.
# At runtime the method signatures use string literals ("VarcoTask", "TaskRegistry").
