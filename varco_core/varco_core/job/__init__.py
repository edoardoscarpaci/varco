"""
varco_core.job
==============
Background job primitives — domain types and ABCs for async job execution.

The job layer provides the protocol layer for background operations that are
too slow to serve synchronously in an HTTP request (e.g. file processing,
bulk imports, long-running computations). The HTTP adapter (varco_fastapi)
implements concrete job runners and stores on top of these ABCs.

Public API
----------
``JobStatus``       — StrEnum of job lifecycle states
``Job``             — Immutable value object representing a background job
``AbstractJobStore``— ABC for job persistence (in-memory, Redis, SQL, ...)
``AbstractJobRunner``— ABC for job execution (asyncio tasks, Celery, ...)

``auth_context_to_snapshot()``  — Serialize AuthContext → JSON-safe dict
``auth_context_from_snapshot()``— Reconstruct AuthContext from a snapshot

Usage (from HTTP layer)::

    # Submit a job
    job = Job(job_id=uuid4(), created_at=datetime.now(UTC))
    await store.save(job)
    await runner.submit(job.job_id, my_async_coro())

    # Check status
    job = await store.get(job_id)
    if job.status == JobStatus.COMPLETED:
        result_bytes = job.result

Thread safety:  ⚠️  ``AbstractJobRunner`` implementations must document their
                    own thread safety.  ``start()`` must be called from within
                    the running event loop.
Async safety:   ✅  All ABC methods are ``async def``.
"""

from varco_core.job.base import (
    AbstractJobRunner,
    AbstractJobStore,
    Job,
    JobStatus,
    auth_context_from_snapshot,
    auth_context_to_snapshot,
)
from varco_core.job.task import (
    DEFAULT_SERIALIZER,
    DefaultTaskSerializer,
    TaskPayload,
    TaskRegistry,
    TaskSerializer,
    VarcoTask,
    varco_task,
)

__all__ = [
    "AbstractJobRunner",
    "AbstractJobStore",
    "Job",
    "JobStatus",
    "auth_context_from_snapshot",
    "auth_context_to_snapshot",
    # Named-task serialization
    "TaskPayload",
    "TaskRegistry",
    "VarcoTask",
    "varco_task",
    # Argument serialization
    "TaskSerializer",
    "DefaultTaskSerializer",
    "DEFAULT_SERIALIZER",
]
