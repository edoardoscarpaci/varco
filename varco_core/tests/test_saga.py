"""
tests.test_saga
===============
Unit tests for varco_core.service.saga.

Covers:
    SagaStep            — frozen, callable-based
    SagaState           — immutable, default fields
    SagaStatus          — enum values
    InMemorySagaRepository — save / load
    SagaOrchestrator    — happy path, step failure + compensation,
                           compensation failure → FAILED, resume,
                           empty-steps error

All tests use InMemorySagaRepository — no external dependencies.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from varco_core.service.saga import (
    InMemorySagaRepository,
    SagaOrchestrator,
    SagaState,
    SagaStatus,
    SagaStep,
)


# ── Helpers ────────────────────────────────────────────────────────────────────


def make_step(
    name: str,
    *,
    execute_raises: Exception | None = None,
    compensate_raises: Exception | None = None,
    record_in: list[str] | None = None,
) -> SagaStep:
    """
    Build a SagaStep that optionally raises and/or records calls.

    Args:
        name:               Step name.
        execute_raises:     If set, ``execute`` raises this exception.
        compensate_raises:  If set, ``compensate`` raises this exception.
        record_in:          List to append "execute:{name}" / "compensate:{name}" to.
    """

    async def execute(ctx: dict) -> None:
        if record_in is not None:
            record_in.append(f"execute:{name}")
        if execute_raises is not None:
            raise execute_raises

    async def compensate(ctx: dict) -> None:
        if record_in is not None:
            record_in.append(f"compensate:{name}")
        if compensate_raises is not None:
            raise compensate_raises

    return SagaStep(name=name, execute=execute, compensate=compensate)


# ── SagaStatus ─────────────────────────────────────────────────────────────────


def test_saga_status_values() -> None:
    """All expected SagaStatus members must exist."""
    assert SagaStatus.PENDING == "PENDING"
    assert SagaStatus.RUNNING == "RUNNING"
    assert SagaStatus.COMPLETED == "COMPLETED"
    assert SagaStatus.COMPENSATING == "COMPENSATING"
    assert SagaStatus.COMPENSATED == "COMPENSATED"
    assert SagaStatus.FAILED == "FAILED"


# ── SagaStep ───────────────────────────────────────────────────────────────────


def test_saga_step_is_frozen() -> None:
    """SagaStep is immutable — assignment raises."""

    async def noop(ctx):
        pass

    step = SagaStep(name="s", execute=noop, compensate=noop)
    with pytest.raises(Exception):
        step.name = "other"  # type: ignore[misc]


def test_saga_step_attributes() -> None:
    """SagaStep stores name, execute, and compensate."""

    async def noop(ctx):
        pass

    step = SagaStep(name="pay", execute=noop, compensate=noop)
    assert step.name == "pay"
    assert callable(step.execute)
    assert callable(step.compensate)


# ── SagaState ─────────────────────────────────────────────────────────────────


def test_saga_state_defaults() -> None:
    """SagaState has sensible defaults."""
    state = SagaState()
    assert state.status == SagaStatus.PENDING
    assert state.completed_steps == 0
    assert state.context == {}
    assert state.error is None
    assert state.saga_id is not None


def test_saga_state_is_frozen() -> None:
    """SagaState is immutable."""
    state = SagaState()
    with pytest.raises(Exception):
        state.status = SagaStatus.RUNNING  # type: ignore[misc]


# ── InMemorySagaRepository ────────────────────────────────────────────────────


async def test_repo_load_unknown_returns_none() -> None:
    """load() returns None for an unknown saga_id."""
    repo = InMemorySagaRepository()
    result = await repo.load(uuid4())
    assert result is None


async def test_repo_save_and_load_roundtrip() -> None:
    """save() then load() returns the same state."""
    repo = InMemorySagaRepository()
    state = SagaState(status=SagaStatus.RUNNING, completed_steps=1)
    await repo.save(state)
    loaded = await repo.load(state.saga_id)
    assert loaded == state


async def test_repo_save_replaces_existing() -> None:
    """Second save() for the same saga_id replaces the previous state."""
    repo = InMemorySagaRepository()
    state_v1 = SagaState(status=SagaStatus.RUNNING)
    await repo.save(state_v1)

    state_v2 = SagaState(saga_id=state_v1.saga_id, status=SagaStatus.COMPLETED)
    await repo.save(state_v2)

    loaded = await repo.load(state_v1.saga_id)
    assert loaded.status == SagaStatus.COMPLETED


# ── SagaOrchestrator — happy path ─────────────────────────────────────────────


async def test_orchestrator_happy_path_completes() -> None:
    """
    All steps succeed → final status is COMPLETED.
    """
    calls: list[str] = []
    steps = [
        make_step("A", record_in=calls),
        make_step("B", record_in=calls),
        make_step("C", record_in=calls),
    ]
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator(steps, repo)
    state = await orch.run()

    assert state.status == SagaStatus.COMPLETED
    assert calls == ["execute:A", "execute:B", "execute:C"]


async def test_orchestrator_happy_path_persists_final_state() -> None:
    """
    After completion, the persisted state must reflect COMPLETED.
    """
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator([make_step("X")], repo)
    state = await orch.run()

    loaded = await repo.load(state.saga_id)
    assert loaded is not None
    assert loaded.status == SagaStatus.COMPLETED


async def test_orchestrator_passes_context_between_steps() -> None:
    """
    Steps can write to ``context`` and the value is visible in later steps.
    """

    async def step_a(ctx):
        ctx["from_a"] = "hello"

    async def step_b(ctx):
        assert ctx["from_a"] == "hello", "step_b should see step_a's output"
        ctx["from_b"] = "world"

    steps = [
        SagaStep("A", execute=step_a, compensate=lambda ctx: None),
        SagaStep("B", execute=step_b, compensate=lambda ctx: None),
    ]
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator(steps, repo)
    final = await orch.run(initial_context={"seed": 42})

    assert final.status == SagaStatus.COMPLETED
    assert final.context["from_a"] == "hello"
    assert final.context["from_b"] == "world"
    assert final.context["seed"] == 42


# ── SagaOrchestrator — failure + compensation ─────────────────────────────────


async def test_orchestrator_step_failure_triggers_compensation() -> None:
    """
    When a step fails, compensations run for all previously completed steps
    in reverse order.
    """
    calls: list[str] = []
    steps = [
        make_step("A", record_in=calls),
        make_step("B", record_in=calls),
        make_step("C", execute_raises=RuntimeError("C failed"), record_in=calls),
        make_step("D", record_in=calls),  # should never execute
    ]
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator(steps, repo)
    state = await orch.run()

    assert state.status == SagaStatus.COMPENSATED
    # A, B executed; C failed (no compensate for C); compensate B then A.
    assert calls == [
        "execute:A",
        "execute:B",
        "execute:C",  # raises, no compensate:C
        "compensate:B",
        "compensate:A",
    ]
    assert "D" not in " ".join(calls), "step D should not have executed"


async def test_orchestrator_first_step_fails_no_compensation() -> None:
    """
    When the first step fails, no compensations run (nothing to undo).
    """
    calls: list[str] = []
    steps = [
        make_step("first", execute_raises=ValueError("first failed"), record_in=calls),
        make_step("second", record_in=calls),
    ]
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator(steps, repo)
    state = await orch.run()

    assert state.status == SagaStatus.COMPENSATED
    assert calls == ["execute:first"]


async def test_orchestrator_failure_records_error_message() -> None:
    """
    The returned SagaState must include an error string describing the failure.
    """
    steps = [make_step("boom", execute_raises=RuntimeError("disk full"))]
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator(steps, repo)
    state = await orch.run()

    assert state.error is not None
    assert "disk full" in state.error


async def test_orchestrator_compensation_failure_results_in_failed_status() -> None:
    """
    When a compensation step also raises, the final status is FAILED (manual
    intervention required).
    """
    calls: list[str] = []
    steps = [
        make_step(
            "pay",
            record_in=calls,
            compensate_raises=IOError("refund API down"),
        ),
        make_step("reserve", execute_raises=RuntimeError("no stock"), record_in=calls),
    ]
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator(steps, repo)
    state = await orch.run()

    assert state.status == SagaStatus.FAILED
    # The compensation for "pay" was attempted even though it failed.
    assert "compensate:pay" in calls


# ── SagaOrchestrator — resume ─────────────────────────────────────────────────


async def test_orchestrator_resume_skips_completed_steps() -> None:
    """
    After a crash mid-saga, resume() starts from the first incomplete step.
    """
    calls: list[str] = []
    steps = [
        make_step("A", record_in=calls),
        make_step("B", record_in=calls),
        make_step("C", record_in=calls),
    ]
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator(steps, repo)

    # Simulate: run has completed 2 steps (A, B) and crashed before C.
    partial_state = SagaState(
        status=SagaStatus.RUNNING,
        completed_steps=2,
        context={"data": "preserved"},
    )
    await repo.save(partial_state)

    state = await orch.resume(partial_state.saga_id)
    assert state is not None
    assert state.status == SagaStatus.COMPLETED
    # Only C should have been executed — A and B are already done.
    assert calls == ["execute:C"]


async def test_orchestrator_resume_unknown_saga_returns_none() -> None:
    """resume() returns None for an unknown saga_id."""
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator([make_step("X")], repo)
    result = await orch.resume(uuid4())
    assert result is None


async def test_orchestrator_resume_terminal_state_returns_unchanged() -> None:
    """
    Resuming a COMPLETED saga returns the existing state without re-executing.
    """
    calls: list[str] = []
    steps = [make_step("X", record_in=calls)]
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator(steps, repo)

    # Save a terminal state directly.
    terminal = SagaState(status=SagaStatus.COMPLETED, completed_steps=1)
    await repo.save(terminal)

    result = await orch.resume(terminal.saga_id)
    assert result is not None
    assert result.status == SagaStatus.COMPLETED
    # No steps should have re-run.
    assert calls == []


# ── SagaOrchestrator — edge cases ─────────────────────────────────────────────


def test_orchestrator_empty_steps_raises() -> None:
    """SagaOrchestrator with no steps raises ValueError."""
    repo = InMemorySagaRepository()
    with pytest.raises(ValueError, match="at least one"):
        SagaOrchestrator([], repo)


async def test_orchestrator_assigns_custom_saga_id() -> None:
    """run() with an explicit saga_id must use that ID."""
    repo = InMemorySagaRepository()
    orch = SagaOrchestrator([make_step("X")], repo)
    custom_id = uuid4()
    state = await orch.run(saga_id=custom_id)
    assert state.saga_id == custom_id


async def test_orchestrator_initial_context_is_preserved() -> None:
    """initial_context values must be visible in the final context."""
    repo = InMemorySagaRepository()

    async def write_ctx(ctx):
        ctx["written"] = True

    orch = SagaOrchestrator(
        [SagaStep("W", execute=write_ctx, compensate=lambda ctx: None)],
        repo,
    )
    state = await orch.run(initial_context={"seed": "original"})
    assert state.context["seed"] == "original"
    assert state.context["written"] is True
