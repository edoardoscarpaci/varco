"""
Tests for varco_core.job.base — Job, JobStatus, auth snapshot helpers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from varco_core.auth.base import Action, AuthContext, ResourceGrant
from varco_core.job.base import (
    Job,
    JobStatus,
    auth_context_from_snapshot,
    auth_context_to_snapshot,
)


# ── JobStatus ──────────────────────────────────────────────────────────────────


def test_job_status_terminal_states():
    """COMPLETED, FAILED, CANCELLED are terminal; PENDING, RUNNING are not."""
    assert JobStatus.COMPLETED.is_terminal
    assert JobStatus.FAILED.is_terminal
    assert JobStatus.CANCELLED.is_terminal
    assert not JobStatus.PENDING.is_terminal
    assert not JobStatus.RUNNING.is_terminal


# ── Job transitions ────────────────────────────────────────────────────────────


def test_job_as_running():
    """as_running() transitions PENDING → RUNNING and sets started_at."""
    job = Job(job_id=uuid4(), created_at=datetime.now(timezone.utc))
    running = job.as_running()
    assert running.status == JobStatus.RUNNING
    assert running.started_at is not None
    assert running.job_id == job.job_id  # same job ID


def test_job_as_completed():
    """as_completed() transitions RUNNING → COMPLETED and sets result."""
    job = Job(job_id=uuid4(), status=JobStatus.RUNNING)
    completed = job.as_completed(result=b'{"ok": true}')
    assert completed.status == JobStatus.COMPLETED
    assert completed.result == b'{"ok": true}'
    assert completed.completed_at is not None


def test_job_as_failed():
    """as_failed() transitions RUNNING → FAILED and sets error."""
    job = Job(job_id=uuid4(), status=JobStatus.RUNNING)
    failed = job.as_failed(error="Connection timeout")
    assert failed.status == JobStatus.FAILED
    assert failed.error == "Connection timeout"
    assert failed.completed_at is not None


def test_job_as_cancelled_from_pending():
    """as_cancelled() transitions PENDING → CANCELLED."""
    job = Job(job_id=uuid4(), status=JobStatus.PENDING)
    cancelled = job.as_cancelled()
    assert cancelled.status == JobStatus.CANCELLED
    assert cancelled.completed_at is not None


def test_job_as_cancelled_from_running():
    """as_cancelled() transitions RUNNING → CANCELLED."""
    job = Job(job_id=uuid4(), status=JobStatus.RUNNING)
    cancelled = job.as_cancelled()
    assert cancelled.status == JobStatus.CANCELLED


def test_job_transitions_are_immutable():
    """Job.as_*() returns a new Job instance; original is unchanged."""
    job = Job(job_id=uuid4())
    running = job.as_running()
    assert job.status == JobStatus.PENDING  # original unchanged
    assert running.status == JobStatus.RUNNING


def test_job_as_running_raises_on_non_pending():
    """as_running() raises ValueError if job is not PENDING."""
    job = Job(job_id=uuid4(), status=JobStatus.RUNNING)
    with pytest.raises(ValueError, match="Only PENDING"):
        job.as_running()


def test_job_as_completed_raises_on_non_running():
    """as_completed() raises ValueError if job is not RUNNING."""
    job = Job(job_id=uuid4(), status=JobStatus.PENDING)
    with pytest.raises(ValueError, match="Only RUNNING"):
        job.as_completed(result=b"")


def test_job_as_cancelled_raises_on_terminal():
    """as_cancelled() raises ValueError if job is already in a terminal state."""
    job = Job(job_id=uuid4(), status=JobStatus.COMPLETED)
    with pytest.raises(ValueError, match="terminal state"):
        job.as_cancelled()


# ── Auth snapshot serialization ────────────────────────────────────────────────


def test_auth_context_to_snapshot_serializes_all_fields():
    """auth_context_to_snapshot() produces a JSON-safe dict."""
    ctx = AuthContext(
        user_id="usr_1",
        roles=frozenset({"admin", "editor"}),
        scopes=frozenset({"write:posts"}),
        grants=(
            ResourceGrant(
                resource="posts",
                actions=frozenset({Action.CREATE, Action.READ}),
            ),
        ),
        metadata={"tenant_id": "t1"},
    )
    snapshot = auth_context_to_snapshot(ctx)

    assert snapshot["user_id"] == "usr_1"
    assert sorted(snapshot["roles"]) == ["admin", "editor"]
    assert snapshot["scopes"] == ["write:posts"]
    assert snapshot["grants"][0]["resource"] == "posts"
    assert sorted(snapshot["grants"][0]["actions"]) == ["create", "read"]
    assert snapshot["metadata"] == {"tenant_id": "t1"}


def test_auth_context_round_trip():
    """Serializing and deserializing an AuthContext preserves all fields."""
    original = AuthContext(
        user_id="usr_2",
        roles=frozenset({"manager"}),
        scopes=frozenset({"read:reports"}),
        grants=(
            ResourceGrant(
                resource="reports",
                actions=frozenset({Action.LIST, Action.READ}),
            ),
        ),
        metadata={"department": "finance"},
    )
    snapshot = auth_context_to_snapshot(original)
    restored = auth_context_from_snapshot(snapshot)

    assert restored.user_id == original.user_id
    assert restored.roles == original.roles
    assert restored.scopes == original.scopes
    assert len(restored.grants) == 1
    assert restored.grants[0].resource == "reports"
    assert Action.READ in restored.grants[0].actions
    assert restored.metadata == original.metadata


def test_auth_context_from_snapshot_handles_missing_metadata():
    """auth_context_from_snapshot() defaults to empty dict for missing metadata."""
    snapshot = {"user_id": "usr_3", "roles": [], "scopes": [], "grants": []}
    ctx = auth_context_from_snapshot(snapshot)
    assert ctx.user_id == "usr_3"
    assert ctx.metadata == {}


def test_auth_context_from_snapshot_handles_anonymous():
    """auth_context_from_snapshot() handles None user_id (anonymous)."""
    snapshot = auth_context_to_snapshot(AuthContext())
    restored = auth_context_from_snapshot(snapshot)
    assert restored.is_anonymous()


# ── VarcoLifespan ─────────────────────────────────────────────────────────────


async def test_varco_lifespan_starts_and_stops_components():
    """VarcoLifespan starts components in order and stops them in reverse."""
    from fastapi import FastAPI

    from varco_fastapi.lifespan import VarcoLifespan

    events: list[str] = []

    class FakeComponent:
        def __init__(self, name: str):
            self.name = name

        async def start(self):
            events.append(f"start:{self.name}")

        async def stop(self):
            events.append(f"stop:{self.name}")

    c1 = FakeComponent("first")
    c2 = FakeComponent("second")

    lifespan = VarcoLifespan(c1, c2)

    app = FastAPI(lifespan=lifespan)

    async with lifespan(app):
        pass  # Simulate app running

    assert events == ["start:first", "start:second", "stop:second", "stop:first"]


async def test_varco_lifespan_register_raises_for_invalid_component():
    """VarcoLifespan.register() raises TypeError for objects without start/stop."""
    from varco_fastapi.lifespan import VarcoLifespan

    lifespan = VarcoLifespan()
    with pytest.raises(TypeError, match="AbstractLifecycle"):
        lifespan.register(object())
