"""
varco_core.service.saga
========================
Saga orchestration for long-running distributed transactions.

Problem
-------
The event consumer pattern supports choreography — each service reacts to
events independently.  But long-running transactions that span multiple
services (e.g. order → payment → fulfilment) need explicit sequencing and
failure recovery:

    1. ``order-service`` creates the order.
    2. ``payment-service`` charges the card.
    3. ``fulfilment-service`` reserves inventory.
    4. If step 3 fails, we must refund the card (compensate step 2) and
       cancel the order (compensate step 1).

Without a saga, a crash between steps 2 and 3 leaves the card charged but
no inventory reserved — silent partial corruption.

Solution
--------
``Saga`` is a sequence of ``SagaStep`` values.  Each step has an ``execute``
async callable and a ``compensate`` async callable.  The
``SagaOrchestrator`` drives execution:

    - Steps execute in order.  On failure, the orchestrator runs compensations
      in **reverse** order for all steps that already succeeded.
    - ``SagaState`` (persisted) tracks which steps completed so the orchestrator
      can skip already-done work on resume after a crash.

DESIGN: orchestration over choreography for long-running flows
    ✅ Explicit sequencing — no implicit event ordering assumptions.
    ✅ Failure handling is centralised — no per-service rollback logic.
    ✅ Persisted state enables crash-safe resume.
    ❌ Single orchestrator is a bottleneck for very high throughput.
    ❌ Synchronous step model — does not natively support parallel steps.
       Compose parallel steps by wrapping ``asyncio.gather`` inside one step.

Components
----------
``SagaStep``
    Frozen dataclass: ``(name, execute, compensate)``.  Both callables are
    ``async def (context: dict) -> None``.  ``context`` is a shared mutable
    dict for passing data between steps.

``SagaStatus``
    Enum: PENDING → RUNNING → COMPLETED | COMPENSATING → COMPENSATED | FAILED.

``SagaState``
    Frozen dataclass: saga-level state (saga_id, status, completed_steps,
    context, error).  Persisted by ``AbstractSagaRepository``.

``AbstractSagaRepository``
    ABC — two abstract methods: ``save(state)`` and ``load(saga_id)``.

``InMemorySagaRepository``
    Dict-backed implementation for tests.

``SagaOrchestrator``
    Drives the saga: executes steps, persists state after each step,
    compensates on failure in reverse order.

Usage::

    from varco_core.service.saga import (
        SagaStep, SagaOrchestrator, InMemorySagaRepository,
    )

    async def charge_card(ctx):
        ctx["payment_id"] = await payment_service.charge(ctx["amount"])

    async def refund_card(ctx):
        await payment_service.refund(ctx["payment_id"])

    async def reserve_inventory(ctx):
        ctx["reservation_id"] = await inventory.reserve(ctx["sku"])

    async def release_inventory(ctx):
        await inventory.release(ctx["reservation_id"])

    steps = [
        SagaStep("charge", execute=charge_card, compensate=refund_card),
        SagaStep("reserve", execute=reserve_inventory, compensate=release_inventory),
    ]

    repo = InMemorySagaRepository()
    orchestrator = SagaOrchestrator(steps, repo)

    final_state = await orchestrator.run({"amount": 99.99, "sku": "WIDGET-1"})

Thread safety:  ⚠️ SagaOrchestrator is stateless — safe to share across
                    coroutines.  SagaState is frozen — immutable value object.
Async safety:   ✅ All step callables and repository methods are ``async def``.

📚 Docs
- 📐 https://microservices.io/patterns/data/saga.html
  Saga pattern — original reference for distributed transaction management.
- 📐 https://docs.microsoft.com/en-us/azure/architecture/reference-architectures/saga/saga
  Microsoft Saga reference — orchestration vs choreography comparison.
- 🐍 https://docs.python.org/3/library/asyncio-task.html
  asyncio tasks — used in step execution.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from dataclasses import field as dfield
from enum import StrEnum
from typing import Any, Callable, Awaitable
from uuid import UUID, uuid4

_logger = logging.getLogger(__name__)


# ── SagaStatus ────────────────────────────────────────────────────────────────


class SagaStatus(StrEnum):
    """
    Lifecycle states of a saga execution.

    Transitions::

        PENDING → RUNNING → COMPLETED           (happy path)
                          → COMPENSATING → COMPENSATED   (failure, all compensations OK)
                          → COMPENSATING → FAILED         (failure, some compensations also fail)

    Attributes:
        PENDING:      Saga created but not yet started.
        RUNNING:      Steps are being executed.
        COMPLETED:    All steps completed successfully.
        COMPENSATING: One step failed; running compensations in reverse.
        COMPENSATED:  All compensations completed successfully.
        FAILED:       A step failed AND at least one compensation also failed.
                      Manual intervention required.

    Edge cases:
        - FAILED is a terminal state — the orchestrator does not retry.
          Operators must inspect the persisted ``SagaState.error`` to decide
          how to proceed.
        - COMPENSATED is a successful terminal state — no steps succeeded;
          all intermediate effects were undone.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    FAILED = "FAILED"


# ── SagaStep ──────────────────────────────────────────────────────────────────

# Type alias for step callables — ``async def step(context: dict) -> None``.
# ``context`` is a shared mutable dict for passing data between steps.
StepCallable = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass(frozen=True)
class SagaStep:
    """
    A single step in a saga: one forward action and its compensation.

    Both callables receive the shared ``context`` dict.  Steps may read and
    write to it to pass data downstream (e.g. storing a payment_id for the
    compensation step to use for refund).

    DESIGN: callable-based step over a class hierarchy
        ✅ Steps are plain async functions — trivial to test in isolation.
        ✅ No mandatory inheritance — steps can be closures, lambdas, or methods.
        ✅ ``context`` dict is the only coupling between steps — explicit and auditable.
        ❌ Type system cannot enforce the dict's shape — consider TypedDict if
           the context grows complex.

    Thread safety:  ✅ Frozen — immutable value object.
    Async safety:   ✅ Callables are awaited by the orchestrator.

    Attributes:
        name:       Human-readable step name for logging and state tracking.
        execute:    Async callable run on the forward path.  Signature:
                    ``async def execute(context: dict) -> None``.
        compensate: Async callable run on the compensation path.  Signature:
                    ``async def compensate(context: dict) -> None``.
                    Must NOT raise — if it does, the saga status is FAILED
                    (manual intervention required).

    Edge cases:
        - ``compensate`` is called ONLY for steps whose ``execute`` succeeded.
          A step whose ``execute`` raised is NOT compensated.
        - ``compensate`` must be idempotent — it may be called more than once
          on saga resume after a crash.

    Example::

        SagaStep(
            name="charge_card",
            execute=lambda ctx: payment_service.charge(ctx["amount"]),
            compensate=lambda ctx: payment_service.refund(ctx["payment_id"]),
        )
    """

    name: str
    execute: StepCallable
    compensate: StepCallable


# ── SagaState ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SagaState:
    """
    Immutable snapshot of a saga's execution state.

    Persisted by ``AbstractSagaRepository`` after each step so the orchestrator
    can resume after a crash.

    DESIGN: immutable snapshot over mutable state object
        ✅ Each state transition produces a new object — no in-place mutation.
        ✅ Persistence is a simple replace — no diff or versioning needed.
        ✅ Frozen → safe to pass across async boundaries without copying.
        ❌ Creates one extra allocation per state transition — negligible.

    Thread safety:  ✅ Frozen — immutable.
    Async safety:   ✅ Value object; no I/O.

    Attributes:
        saga_id:         Unique ID for this saga execution.
        status:          Current ``SagaStatus``.
        completed_steps: Number of steps that have executed successfully.
                         Used by the orchestrator to skip already-done steps
                         on resume and to know which steps to compensate on failure.
        context:         Shared data dict passed between steps.  May contain
                         intermediate results (e.g. payment_id, reservation_id).
        error:           Human-readable error string from the step that failed.
                         ``None`` if no failure has occurred.

    Edge cases:
        - ``completed_steps=0`` means no steps have run yet (PENDING).
        - ``completed_steps=N`` and ``status=COMPENSATING`` means steps
          0 … N-1 succeeded and step N failed — compensate N-1 down to 0.
        - ``context`` is mutable by steps — the persisted snapshot captures
          the context at the moment of the last state transition.

    Example::

        initial = SagaState(saga_id=uuid4(), status=SagaStatus.PENDING)
        running = SagaState(
            saga_id=initial.saga_id,
            status=SagaStatus.RUNNING,
            completed_steps=0,
            context={"amount": 99.99},
        )
    """

    saga_id: UUID = dfield(default_factory=uuid4)
    status: SagaStatus = SagaStatus.PENDING
    completed_steps: int = 0
    context: dict[str, Any] = dfield(default_factory=dict)
    error: str | None = None


# ── AbstractSagaRepository ────────────────────────────────────────────────────


class AbstractSagaRepository(ABC):
    """
    Abstract persistence interface for saga state.

    Implementations store ``SagaState`` objects so the orchestrator can
    resume after a crash.

    DESIGN: save/load over event sourcing
        ✅ Simple — one row per saga, replaced on each transition.
        ✅ Compatible with SQLAlchemy, Beanie, Redis (any key/value store).
        ❌ No history — only the current state is stored.  If audit history
           is needed, use event sourcing or a separate audit log.

    Thread safety:  ⚠️ Subclass-defined.
    Async safety:   ✅ All methods are ``async def``.
    """

    @abstractmethod
    async def save(self, state: SagaState) -> None:
        """
        Persist a ``SagaState`` snapshot.  Replaces any existing state for
        the same ``saga_id``.

        Args:
            state: The saga state to persist.

        Raises:
            Exception: Implementation-specific storage errors.

        Edge cases:
            - Calling ``save`` twice with the same ``saga_id`` replaces the
              previous state — last-write-wins.
        """

    @abstractmethod
    async def load(self, saga_id: UUID) -> SagaState | None:
        """
        Load the current state for a saga.

        Args:
            saga_id: The saga ID to look up.

        Returns:
            The persisted ``SagaState``, or ``None`` if not found.

        Edge cases:
            - Returns ``None`` for unknown saga IDs — callers must handle this.
        """


# ── InMemorySagaRepository ────────────────────────────────────────────────────


class InMemorySagaRepository(AbstractSagaRepository):
    """
    In-memory saga repository backed by a dict.

    Suitable for tests and single-process scenarios.  State is lost on
    process restart — not suitable for production.

    Thread safety:  ⚠️ Not thread-safe across OS threads.
    Async safety:   ✅ No actual async I/O; safe to await.

    Edge cases:
        - Calling ``load`` for an unknown saga_id returns ``None``.
        - Multiple calls to ``save`` for the same saga_id replace the state.
    """

    def __init__(self) -> None:
        # Maps UUID → SagaState — the "database" for test scenarios.
        self._store: dict[UUID, SagaState] = {}

    async def save(self, state: SagaState) -> None:
        """
        Store the saga state.  Replaces any existing entry for the same ID.

        Args:
            state: The ``SagaState`` to persist.
        """
        self._store[state.saga_id] = state

    async def load(self, saga_id: UUID) -> SagaState | None:
        """
        Return the state for ``saga_id``, or ``None`` if not found.

        Args:
            saga_id: The saga ID to look up.

        Returns:
            ``SagaState`` if found; ``None`` otherwise.
        """
        return self._store.get(saga_id)

    def __repr__(self) -> str:
        return f"InMemorySagaRepository(sagas={len(self._store)})"


# ── SagaOrchestrator ──────────────────────────────────────────────────────────


class SagaOrchestrator:
    """
    Drives a sequence of ``SagaStep`` objects, persisting state after each
    step and compensating in reverse order on failure.

    The orchestrator is **stateless** — it receives the step list and repository
    at construction time and operates only on the mutable ``context`` dict that
    is passed to each step callable.

    DESIGN: step index tracking over a state machine
        ✅ Simple: ``completed_steps`` is an integer counter.  Resume logic
           is ``execute steps[completed_steps:]``.
        ✅ Compensation is just ``reversed(steps[:completed_steps])``.
        ✅ No complex graph — linear step sequences cover most saga use cases.
        ❌ Does not support parallel steps or branching.  Wrap ``asyncio.gather``
           inside a single step if parallelism is needed.

    Crash recovery
    --------------
    If the process crashes mid-saga, the caller can create a new
    ``SagaOrchestrator`` with the same steps and repository, then call
    ``resume(saga_id)``.  The orchestrator loads the state from the repository
    and continues from ``completed_steps``.

    Thread safety:  ✅ Stateless — safe to share across coroutines.
    Async safety:   ✅ All step callables and repo methods are awaited.

    Args:
        steps:      Ordered list of ``SagaStep`` values.  At least one required.
        repository: ``AbstractSagaRepository`` for persisting state transitions.

    Edge cases:
        - Empty step list raises ``ValueError`` at construction time.
        - If all compensations succeed → ``SagaStatus.COMPENSATED``.
        - If any compensation raises → ``SagaStatus.FAILED`` (manual intervention).
        - Compensation is always attempted in reverse for ALL completed steps —
          a failing compensation is logged and skipped, but the loop continues.
          The final status is FAILED if any compensation raised.

    Example::

        orchestrator = SagaOrchestrator(steps=my_steps, repository=repo)
        final = await orchestrator.run(initial_context={"order_id": "ord-1"})
        print(final.status)  # SagaStatus.COMPLETED or SagaStatus.COMPENSATED
    """

    def __init__(
        self,
        steps: list[SagaStep],
        repository: AbstractSagaRepository,
    ) -> None:
        """
        Args:
            steps:      Ordered sequence of saga steps.  Must be non-empty.
            repository: Repository for persisting state after each transition.

        Raises:
            ValueError: If ``steps`` is empty.
        """
        if not steps:
            raise ValueError(
                "SagaOrchestrator requires at least one SagaStep. "
                "An empty saga has no effect and is likely a programming error."
            )
        self._steps = list(steps)
        self._repo = repository

    async def run(
        self,
        initial_context: dict[str, Any] | None = None,
        *,
        saga_id: UUID | None = None,
    ) -> SagaState:
        """
        Execute the saga from the beginning with a fresh context.

        Creates an initial ``SagaState`` (PENDING), transitions it to RUNNING,
        and executes each step in order.  On success returns COMPLETED.  On
        step failure, runs compensations in reverse and returns COMPENSATED or
        FAILED.

        Args:
            initial_context: Initial data dict passed to the first step.
                             Steps may add to this dict to pass data downstream.
                             Defaults to an empty dict.
            saga_id:         Optional saga ID.  Defaults to a new UUID.

        Returns:
            Final ``SagaState`` after all steps (or compensations) complete.

        Raises:
            Nothing — all exceptions from steps and compensations are caught
            and encoded in the returned ``SagaState``.

        Async safety:   ✅ Awaits each step callable sequentially.

        Edge cases:
            - If the very first step fails, no compensations are needed.
            - context is mutable during execution — the returned state captures
              the context at the point of completion or failure.
        """
        context: dict[str, Any] = dict(initial_context or {})
        saga_id = saga_id or uuid4()

        # Initial state
        state = SagaState(
            saga_id=saga_id,
            status=SagaStatus.PENDING,
            context=context,
        )
        await self._repo.save(state)

        # Transition to RUNNING
        state = SagaState(
            saga_id=saga_id,
            status=SagaStatus.RUNNING,
            completed_steps=0,
            context=context,
        )
        await self._repo.save(state)

        return await self._execute_steps(state)

    async def resume(self, saga_id: UUID) -> SagaState | None:
        """
        Resume a saga from its last persisted state.

        Loads the state for ``saga_id`` and continues execution from
        ``completed_steps``.  Steps that already completed are skipped.

        Args:
            saga_id: The saga ID to resume.

        Returns:
            Final ``SagaState`` after resumption, or ``None`` if the saga ID
            is not found in the repository.

        Raises:
            Nothing — exceptions are encoded in the returned state.

        Edge cases:
            - If the saga is already COMPLETED / COMPENSATED / FAILED (terminal
              states), the state is returned unchanged — no re-execution.
            - If the saga is COMPENSATING, compensation is resumed from where
              it left off.

        Async safety:   ✅ All I/O is awaited.
        """
        state = await self._repo.load(saga_id)
        if state is None:
            _logger.warning("SagaOrchestrator.resume: saga_id=%s not found.", saga_id)
            return None

        # Terminal states — nothing to do.
        terminal = {SagaStatus.COMPLETED, SagaStatus.COMPENSATED, SagaStatus.FAILED}
        if state.status in terminal:
            _logger.info(
                "SagaOrchestrator.resume: saga_id=%s already in terminal state %s.",
                saga_id,
                state.status,
            )
            return state

        if state.status == SagaStatus.COMPENSATING:
            # Resume compensation from the top — compensate all completed steps.
            return await self._compensate(
                state, error=state.error or "Resumed during compensation"
            )

        # PENDING or RUNNING — resume forward execution.
        return await self._execute_steps(state)

    # ── Internal: forward execution ────────────────────────────────────────────

    async def _execute_steps(self, state: SagaState) -> SagaState:
        """
        Execute steps starting from ``state.completed_steps``.

        Called from ``run()`` (all steps) and ``resume()`` (partial).

        Args:
            state: Current saga state.  Steps before ``completed_steps`` are skipped.

        Returns:
            Final saga state after all steps or compensation.
        """
        context = dict(state.context)  # mutable copy for this execution

        for i, step in enumerate(self._steps):
            if i < state.completed_steps:
                # Already completed on a previous run — skip.
                _logger.debug(
                    "SagaOrchestrator: skipping completed step [%d] %r",
                    i,
                    step.name,
                )
                continue

            _logger.info(
                "SagaOrchestrator: executing step [%d/%d] %r (saga_id=%s)",
                i + 1,
                len(self._steps),
                step.name,
                state.saga_id,
            )

            try:
                await step.execute(context)
            except Exception as exc:
                # Step failed — record the error and begin compensation.
                error = f"Step [{i}] {step.name!r} failed: {exc}"
                _logger.error("SagaOrchestrator: %s (saga_id=%s)", error, state.saga_id)

                # Persist COMPENSATING state before running compensations.
                state = SagaState(
                    saga_id=state.saga_id,
                    status=SagaStatus.COMPENSATING,
                    completed_steps=i,  # steps 0 … i-1 completed; i failed
                    context=context,
                    error=error,
                )
                await self._repo.save(state)
                return await self._compensate(state, error=error)

            # Step succeeded — increment counter and persist.
            state = SagaState(
                saga_id=state.saga_id,
                status=SagaStatus.RUNNING,
                completed_steps=i + 1,
                context=context,
            )
            await self._repo.save(state)

        # All steps completed.
        state = SagaState(
            saga_id=state.saga_id,
            status=SagaStatus.COMPLETED,
            completed_steps=len(self._steps),
            context=context,
        )
        await self._repo.save(state)
        _logger.info(
            "SagaOrchestrator: saga_id=%s COMPLETED (%d steps)",
            state.saga_id,
            len(self._steps),
        )
        return state

    # ── Internal: compensation ─────────────────────────────────────────────────

    async def _compensate(self, state: SagaState, *, error: str) -> SagaState:
        """
        Run compensations in reverse for all completed steps.

        Compensation iterates steps[0 … completed_steps-1] in reverse.
        Each compensation is tried regardless of whether the previous one
        failed — we want to undo as much as possible before giving up.

        Args:
            state: Current saga state (COMPENSATING).
            error: The original step error message.

        Returns:
            SagaState with status COMPENSATED (all compensations OK) or
            FAILED (at least one compensation raised).
        """
        context = dict(state.context)
        any_compensation_failed = False

        # Iterate completed steps in REVERSE — compensate last-first.
        # Only steps 0 … completed_steps-1 executed successfully.
        for i in reversed(range(state.completed_steps)):
            step = self._steps[i]
            _logger.info(
                "SagaOrchestrator: compensating step [%d] %r (saga_id=%s)",
                i,
                step.name,
                state.saga_id,
            )
            try:
                await step.compensate(context)
            except Exception as comp_exc:
                # Compensation failed — log and mark FAILED, but continue
                # compensating remaining steps to undo as much as possible.
                _logger.error(
                    "SagaOrchestrator: compensation for step [%d] %r failed: %s "
                    "(saga_id=%s) — manual intervention required.",
                    i,
                    step.name,
                    comp_exc,
                    state.saga_id,
                )
                any_compensation_failed = True

        final_status = (
            SagaStatus.FAILED if any_compensation_failed else SagaStatus.COMPENSATED
        )
        final_state = SagaState(
            saga_id=state.saga_id,
            status=final_status,
            completed_steps=state.completed_steps,
            context=context,
            error=error,
        )
        await self._repo.save(final_state)
        _logger.info(
            "SagaOrchestrator: saga_id=%s finished compensation → %s",
            state.saga_id,
            final_status,
        )
        return final_state


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SagaStatus",
    "SagaStep",
    "SagaState",
    "AbstractSagaRepository",
    "InMemorySagaRepository",
    "SagaOrchestrator",
]
