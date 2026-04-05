"""
varco_fastapi.job.response
===========================
Pydantic response models for the job lifecycle HTTP API.

These models are returned by endpoints that submit or poll background jobs.

``JobAcceptedResponse`` — returned immediately when a job is submitted (HTTP 202).
``JobStatusResponse``   — returned by ``GET /jobs/{job_id}`` and callback POSTs.
``JobProgressEvent``    — domain event emitted on each job state transition.

DESIGN: separate Pydantic models for HTTP concerns vs. domain Job dataclass
    ✅ ``Job`` (domain) carries internal fields (auth_snapshot, raw token) that
       must NOT appear in HTTP responses — keeping them separate avoids leaking secrets
    ✅ ``JobStatusResponse`` is the stable, serializable contract for API consumers
    ✅ ``JobAcceptedResponse`` includes convenience URLs (poll_url, events_url) that
       depend on the request URL, not the domain model
    ❌ One extra mapping step from Job → JobStatusResponse

Thread safety:  ✅ Frozen Pydantic models — immutable after construction.
Async safety:   ✅ Pure value objects; no I/O.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from varco_core.job.base import JobStatus


# ── JobAcceptedResponse ────────────────────────────────────────────────────────


class JobAcceptedResponse(BaseModel):
    """
    HTTP 202 response returned immediately when a job is submitted.

    Tells the client:
    - The ``job_id`` to poll for status
    - A convenience ``poll_url`` (``GET /jobs/{job_id}``)
    - An optional ``events_url`` for SSE progress streaming

    Attributes:
        job_id:     UUID of the submitted job.
        status:     Initial status (always ``PENDING`` at acceptance time).
        poll_url:   URL to poll for job status (e.g. ``"/jobs/{job_id}"``).
        events_url: URL for SSE progress stream, or ``None`` if the router
                    was not configured with an ``event_bus``.

    Thread safety:  ✅ Immutable Pydantic model.
    Async safety:   ✅ Pure value object.
    """

    job_id: UUID = Field(..., description="UUID of the submitted job")
    status: JobStatus = Field(
        default=JobStatus.PENDING,
        description="Initial job status (always PENDING at acceptance time)",
    )
    poll_url: str = Field(
        default="",
        description="URL to GET for job status (e.g. /jobs/{job_id})",
    )
    events_url: str | None = Field(
        default=None,
        description="URL for SSE progress stream (null if event_bus not configured)",
    )

    model_config = {"use_enum_values": True}


# ── JobStatusResponse ─────────────────────────────────────────────────────────


class JobStatusResponse(BaseModel):
    """
    HTTP 200 response returned by ``GET /jobs/{job_id}`` and callback POSTs.

    Encodes the full lifecycle state of a job including timing, result, and error.

    Attributes:
        job_id:       UUID of the job.
        status:       Current ``JobStatus``.
        created_at:   When the job was submitted.
        started_at:   When execution began, or ``None`` if still pending.
        completed_at: When execution finished, or ``None`` if not yet complete.
        progress:     Progress fraction 0.0–1.0, or ``None`` if not reported
                      by the job coroutine via ``job_progress()``.
        result:       Deserialized result payload, or ``None`` if not completed.
        error:        Error message if status is ``FAILED``, else ``None``.

    Note: ``result`` is typed as ``Any`` — job results can be arbitrary JSON.
          Callers should narrow the type based on the specific job type they submitted.

    Thread safety:  ✅ Immutable Pydantic model.
    Async safety:   ✅ Pure value object.

    Edge cases:
        - ``result`` and ``error`` are both ``None`` for PENDING / RUNNING jobs
        - ``progress`` is always ``None`` unless the job calls ``job_progress()``
        - ``completed_at`` is set for both COMPLETED and FAILED jobs
    """

    job_id: UUID = Field(..., description="UUID of the job")
    status: JobStatus = Field(..., description="Current job lifecycle status")
    created_at: datetime = Field(..., description="When the job was submitted (UTC)")
    started_at: datetime | None = Field(
        default=None,
        description="When execution began (UTC), or null if still pending",
    )
    completed_at: datetime | None = Field(
        default=None,
        description="When execution finished (UTC), or null if not yet complete",
    )
    progress: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Progress fraction 0.0–1.0 (null if not reported by the job)",
    )
    result: Any | None = Field(
        default=None,
        description="Deserialized result payload (null if not yet completed)",
    )
    error: str | None = Field(
        default=None,
        description="Error message if status is FAILED, else null",
    )

    model_config = {"use_enum_values": True}


# ── JobProgressEvent ───────────────────────────────────────────────────────────


class JobProgressEvent(BaseModel):
    """
    Domain event emitted by ``JobRunner`` on each job state transition.

    Published to the event bus so SSE clients can stream progress in real-time
    at ``GET /jobs/{job_id}/events``.

    Attributes:
        job_id:   UUID of the job.
        status:   New ``JobStatus`` after the transition.
        progress: Progress fraction 0.0–1.0, or ``None``.
        message:  Optional human-readable status message
                  (e.g. ``"Processing item 42/100"``).
        error:    Set only when ``status == FAILED``.

    DESIGN: Pydantic BaseModel over DomainEvent dataclass
        ✅ Easy JSON serialization for SSE payload (model_dump_json())
        ✅ Pydantic validation on construction
        ✅ No dependency on varco_core.event.DomainEvent base class
        ❌ Cannot be directly published to AbstractEventBus without wrapping
           (use as SSE payload directly instead)

    Thread safety:  ✅ Immutable Pydantic model.
    Async safety:   ✅ Pure value object.
    """

    job_id: UUID = Field(..., description="UUID of the job")
    status: JobStatus = Field(..., description="New job status after state transition")
    progress: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Progress fraction 0.0–1.0, or null",
    )
    message: str | None = Field(
        default=None,
        description="Human-readable status message (e.g. 'Processing item 42/100')",
    )
    error: str | None = Field(
        default=None,
        description="Error message when status is FAILED",
    )

    model_config = {"use_enum_values": True}


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "JobAcceptedResponse",
    "JobStatusResponse",
    "JobProgressEvent",
]
