"""
varco_beanie.saga
=================
Beanie/MongoDB implementation of ``AbstractSagaRepository``.

Stores each ``SagaState`` as a single ``SagaDocument`` in the
``varco_sagas`` MongoDB collection.

Usage::

    from beanie import init_beanie
    from varco_beanie.saga import BeanieSagaRepository, SagaDocument

    await init_beanie(database=db, document_models=[SagaDocument])

    repo = BeanieSagaRepository()
    state = SagaState(saga_id=uuid4())
    await repo.save(state)
    loaded = await repo.load(state.saga_id)

DESIGN: Beanie Document over raw pymongo collection operations
    ✅ Consistent with the rest of the varco_beanie layer.
    ✅ Built-in async methods (``insert``, ``find_one``, ``delete``) — no raw
       BSON marshalling.
    ✅ Optionally accepts ``AsyncClientSession`` so saves participate in the
       caller's MongoDB transaction (replica set only).
    ❌ Requires ``init_beanie()`` at startup — cannot be used without it.

DESIGN: upsert via find_one + delete + insert (conditional)
    ✅ Works on single-node and replica-set MongoDB.
    ✅ Simple to follow — no aggregate $merge or bulkWrite needed.
    ❌ Not atomic without a session/transaction (replica set only).
       The saga orchestrator runs steps sequentially, so concurrent saves
       for the same saga_id are not expected in normal operation.

Thread safety:  ✅ Document class is a static definition — no mutable state.
Async safety:   ✅ All Beanie methods are ``async def``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from beanie import Document
from pydantic import Field

from varco_core.service.saga import AbstractSagaRepository, SagaState, SagaStatus

if TYPE_CHECKING:
    from pymongo.asynchronous.client_session import AsyncClientSession

_logger = logging.getLogger(__name__)


# ── SagaDocument ──────────────────────────────────────────────────────────────


class SagaDocument(Document):
    """
    Beanie document representing a saga's current state.

    Maps to the ``varco_sagas`` MongoDB collection.

    Register this document in your ``init_beanie()`` call before using
    ``BeanieSagaRepository``.

    DESIGN: UUID primary key over ObjectId
        ✅ Matches ``SagaState.saga_id`` directly — no translation layer.
        ✅ UUIDs are stable across services — useful if saga IDs are shared
           externally (e.g. as correlation IDs in events).
        ❌ Beanie stores UUID as Binary (BSON subtype 3) by default, which is
           different from native ObjectId — aware consumers must handle this.

    Thread safety:  ✅ Static document class — no mutable state.
    Async safety:   ✅ All Beanie methods are ``async def``.

    Attributes:
        id:              UUIDv4 — matches ``SagaState.saga_id``.
        status:          Saga lifecycle state (string value of ``SagaStatus``).
        completed_steps: Number of steps that have executed successfully.
        context:         Shared step data dict — stored as an embedded document.
        error:           Error message if the saga failed, else ``None``.
        updated_at:      UTC timestamp of the last state transition.

    Edge cases:
        - ``context`` may grow large if steps store large payloads.  Keep values
          small (IDs, amounts) — not full documents.
        - ``updated_at`` is set by the application, not MongoDB — no server-side
          timestamp update.  This is intentional: it reflects the application's
          view of the update time, not network latency.
    """

    id: UUID = Field(default_factory=uuid4)

    status: str
    """String value of ``SagaStatus`` — e.g. ``"PENDING"``, ``"RUNNING"``."""

    completed_steps: int = 0
    """Number of steps that completed successfully; 0 = not started."""

    context: dict[str, Any] = Field(default_factory=dict)
    """Shared mutable data dict passed between steps."""

    error: str | None = None
    """Human-readable error from the failing step; ``None`` if not yet failed."""

    updated_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    """UTC timestamp of the last ``save()`` call."""

    class Settings:
        """Beanie collection configuration."""

        name = "varco_sagas"
        indexes: list = []

    def __repr__(self) -> str:
        return (
            f"SagaDocument(id={self.id}, status={self.status!r}, "
            f"completed_steps={self.completed_steps})"
        )


# ── BeanieSagaRepository ──────────────────────────────────────────────────────


class BeanieSagaRepository(AbstractSagaRepository):
    """
    Beanie implementation of ``AbstractSagaRepository``.

    Persists saga state in the ``varco_sagas`` MongoDB collection via
    ``SagaDocument``.  Optionally accepts an ``AsyncClientSession`` so
    operations participate in the caller's MongoDB transaction (replica set only).

    Pass ``session=None`` for single-node MongoDB or when the orchestrator
    manages its own lifecycle outside a transactional boundary.

    Thread safety:  ⚠️ If ``session`` is provided, the same constraints as
                        ``AsyncClientSession`` apply — one per request/task.
                        ``session=None`` instances are safe to share.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        session: Optional pymongo ``AsyncClientSession``.  Pass the session
                 from a Beanie transaction when running inside a replica set
                 transaction.  ``None`` for non-transactional use.

    Edge cases:
        - ``SagaDocument`` must be registered with Beanie (via ``init_beanie()``)
          before any method is called.  ``RuntimeError`` is raised by Beanie if
          not registered.
        - Concurrent saves for the same ``saga_id`` are not expected — the saga
          orchestrator runs steps sequentially.  If two processes race to save
          the same saga, last-write-wins.
        - Without a session, ``save()`` is not atomic — a crash between the
          ``delete()`` and ``insert()`` leaves the saga absent.  Acceptable:
          the orchestrator will attempt to resume from the previous state on
          the next run.

    Example::

        repo = BeanieSagaRepository()
        state = SagaState(saga_id=uuid4(), status=SagaStatus.PENDING)
        await repo.save(state)

        loaded = await repo.load(state.saga_id)
        assert loaded.status == SagaStatus.PENDING
    """

    def __init__(
        self,
        *,
        session: AsyncClientSession | None = None,
    ) -> None:
        """
        Args:
            session: Optional ``AsyncClientSession`` for transaction support.
                     Pass ``None`` for non-transactional / single-node use.
        """
        self._session = session

    async def save(self, state: SagaState) -> None:
        """
        Persist a ``SagaState`` snapshot (upsert semantics).

        Replaces any existing document for the same ``saga_id``.

        DESIGN: delete-then-insert over ``replace_one(upsert=True)``
            ✅ Simpler — relies on Beanie's ``insert()`` rather than raw pymongo.
            ✅ Works with and without a session.
            ❌ Two operations — not atomic without a session/transaction.

        Args:
            state: The ``SagaState`` to persist.

        Raises:
            beanie.exceptions.DocumentWasNotSaved: If Beanie fails to insert.
            RuntimeError: If ``SagaDocument`` was not registered with Beanie.

        Async safety: ✅ Awaits ``find_one()``, ``delete()``, and ``insert()``.
        """
        find_kwargs: dict = {}
        if self._session is not None:
            find_kwargs["session"] = self._session

        existing = await SagaDocument.find_one(
            SagaDocument.id == state.saga_id,
            **find_kwargs,
        )
        if existing is not None:
            await existing.delete(**find_kwargs)

        doc = SagaDocument(
            id=state.saga_id,
            status=state.status.value,
            completed_steps=state.completed_steps,
            context=dict(state.context),
            error=state.error,
            updated_at=datetime.now(tz=timezone.utc),
        )
        await doc.insert(**find_kwargs)

        _logger.debug(
            "BeanieSagaRepository.save: saga_id=%s status=%s",
            state.saga_id,
            state.status,
        )

    async def load(self, saga_id: UUID) -> SagaState | None:
        """
        Load the current state for a saga.

        Args:
            saga_id: The saga ID to look up.

        Returns:
            The persisted ``SagaState``, or ``None`` if not found.

        Raises:
            RuntimeError: If ``SagaDocument`` was not registered with Beanie.

        Async safety: ✅ Single ``find_one()`` call.
        """
        find_kwargs: dict = {}
        if self._session is not None:
            find_kwargs["session"] = self._session

        doc = await SagaDocument.find_one(
            SagaDocument.id == saga_id,
            **find_kwargs,
        )
        if doc is None:
            return None

        return SagaState(
            saga_id=doc.id,
            status=SagaStatus(doc.status),
            completed_steps=doc.completed_steps,
            context=dict(doc.context),
            error=doc.error,
        )

    def __repr__(self) -> str:
        return f"BeanieSagaRepository(session={self._session!r})"


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SagaDocument",
    "BeanieSagaRepository",
]
