"""
varco_core.retention.targets
===============================
The five in-tree ``RetentionTarget`` adapters, one per shipped bulk-delete
verb (§D-S20-seam). Plan 039 (S20) / Step 9.

Every adapter's ``purge(dry_run=True)`` path on a ``supports_dry_run=True``
target uses a **non-destructive read** (``list_entries``/``list``/
``list_by_status``) to compute ``would_delete`` — it never calls the
backend's own delete method under ``dry_run=True``. This is the single most
important property in the whole plan
(``varco_core/tests/test_retention_targets.py``'s spy assertions).

Thread safety:  ✅ Each adapter is a thin, stateless wrapper — the same
                   thread-safety story as the backend it wraps.
Async safety:   ✅ All I/O is awaited.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from varco_core.retention.base import RetentionTarget
from varco_core.retention.policy import RetentionOutcome
from varco_core.service.tenant import current_tenant

if TYPE_CHECKING:
    from varco_core.event.dlq import AbstractDeadLetterQueue
    from varco_core.idempotency.base import AbstractIdempotencyStore
    from varco_core.job.base import AbstractJobStore
    from varco_core.revocation.base import AbstractTokenRevocationStore
    from varco_core.service.audit import AuditRepository

__all__ = [
    "AuditRetentionTarget",
    "DlqRetentionTarget",
    "IdempotencyRetentionTarget",
    "JobRetentionTarget",
    "RevocationRetentionTarget",
]


class DlqRetentionTarget(RetentionTarget):
    """
    Wraps ``AbstractDeadLetterQueue.delete_where()``
    (``varco_core/varco_core/event/dlq.py:450``) — this adapter exists so
    the ABC itself gains no method, see CLAUDE.md's ``BulkCache``/
    ``AsyncCache`` rule.

    Args:
        dlq: The DLQ to prune.
        acknowledge_dead_letter_deletion: **Required ``True``.** CLAUDE.md,
            standing: *"Dead letters must never be silently deleted (no TTL
            index by default)."* A dead letter is the only remaining copy of
            an event that already failed every retry — this is a second,
            named acknowledgement distinct from ``dry_run=False`` (that says
            "I mean to delete"; this says "I mean to delete **dead
            letters**"), §D-S20-dlq.
        channel: Optional channel filter, forwarded to every call.

    Raises:
        ValueError: ``acknowledge_dead_letter_deletion`` was not set.

    Edge cases:
        - Kafka/NATS-backed DLQs raise ``NotImplementedError`` from
          ``delete_where()`` naming their own retention mechanism
          (``retention.ms`` / JetStream ``MaxAge``) — this propagates
          unchanged; the scheduler continues to the next policy.
    """

    supports_older_than = True
    supports_dry_run = True

    def __init__(
        self,
        *,
        dlq: AbstractDeadLetterQueue,
        acknowledge_dead_letter_deletion: bool = False,
        channel: str | None = None,
    ) -> None:
        if not acknowledge_dead_letter_deletion:
            raise ValueError(
                "DlqRetentionTarget requires acknowledge_dead_letter_deletion=True — "
                "dead letters are the last remaining copy of an event that already "
                "failed every retry; deleting them is not routine housekeeping "
                "(CLAUDE.md's standing DLQ rule)."
            )
        self._dlq = dlq
        self._channel = channel

    @property
    def kind(self) -> str:
        return "dlq"

    async def purge(
        self, *, older_than: datetime | None, limit: int, dry_run: bool
    ) -> RetentionOutcome:
        tenant_id = current_tenant()
        if dry_run:
            entries = await self._dlq.list_entries(
                limit=limit, channel=self._channel, tenant_id=tenant_id, older_than=older_than
            )
            return RetentionOutcome(
                kind="dlq",
                examined=len(entries),
                deleted=0,
                would_delete=len(entries),
                dry_run=True,
            )
        deleted = await self._dlq.delete_where(
            older_than=older_than, channel=self._channel, tenant_id=tenant_id, limit=limit
        )
        return RetentionOutcome(
            kind="dlq", examined=None, deleted=deleted, would_delete=0, dry_run=False
        )


class AuditRetentionTarget(RetentionTarget):
    """
    Wraps ``AuditRepository.delete_where()``
    (``varco_core/varco_core/service/audit.py:339``).

    Args:
        repo: The audit repository to prune.
        allow_chain_break: Forwarded verbatim to ``delete_where()``.
            **Not set by default** — a hash-chained table (``hash_chain=True``)
            refuses to prune without it (``audit.py:360-366``), because a
            deleted row is indistinguishable from a ``ChainGap`` at
            ``verify_chain()`` time forever after. This is an explicit
            per-target ctor arg, never auto-set by the scheduler.

    Edge cases:
        - On a hash-chained table without ``allow_chain_break=True``, the
          backend's own ``ValueError`` propagates unchanged.
    """

    supports_older_than = True
    supports_dry_run = True

    def __init__(self, *, repo: AuditRepository, allow_chain_break: bool = False) -> None:
        self._repo = repo
        self._allow_chain_break = allow_chain_break

    @property
    def kind(self) -> str:
        return "audit"

    async def purge(
        self, *, older_than: datetime | None, limit: int, dry_run: bool
    ) -> RetentionOutcome:
        tenant_id = current_tenant()
        if dry_run:
            entries = await self._repo.list(
                occurred_to=older_than, tenant_id=tenant_id, limit=limit
            )
            return RetentionOutcome(
                kind="audit",
                examined=len(entries),
                deleted=0,
                would_delete=len(entries),
                dry_run=True,
            )
        deleted = await self._repo.delete_where(
            older_than=older_than,
            tenant_id=tenant_id,
            limit=limit,
            allow_chain_break=self._allow_chain_break,
        )
        return RetentionOutcome(
            kind="audit", examined=None, deleted=deleted, would_delete=0, dry_run=False
        )


class IdempotencyRetentionTarget(RetentionTarget):
    """
    Wraps ``AbstractIdempotencyStore.delete_expired()``
    (``varco_core/varco_core/idempotency/base.py:202``) — this adapter
    exists so the ABC itself gains no method, see CLAUDE.md's ``BulkCache``/
    ``AsyncCache`` rule.

    ``delete_expired()`` takes no cutoff (expiry is intrinsic to each
    record) and has no non-destructive preview — ``supports_older_than`` and
    ``supports_dry_run`` are both ``False``. A backend with native TTL
    support (Redis's ``PX``, Mongo's TTL index) may legitimately return
    ``0`` — that is reported as ``deleted=0``, never treated as an error.
    """

    supports_older_than = False
    supports_dry_run = False

    def __init__(self, *, store: AbstractIdempotencyStore) -> None:
        self._store = store

    @property
    def kind(self) -> str:
        return "idempotency"

    async def purge(
        self, *, older_than: datetime | None, limit: int, dry_run: bool
    ) -> RetentionOutcome:
        if older_than is not None:
            raise ValueError(
                "IdempotencyRetentionTarget does not support older_than — "
                "delete_expired() has no cutoff parameter, expiry is intrinsic "
                "to each reservation."
            )
        if dry_run:
            return RetentionOutcome(
                kind="idempotency",
                deleted=0,
                would_delete=0,
                dry_run=True,
                skipped_reason=(
                    "delete_expired() has no non-destructive preview — skipping "
                    "under dry_run=True rather than actually delete."
                ),
            )
        deleted = await self._store.delete_expired()
        return RetentionOutcome(
            kind="idempotency", examined=None, deleted=deleted, would_delete=0, dry_run=False
        )


class RevocationRetentionTarget(RetentionTarget):
    """
    Wraps ``AbstractTokenRevocationStore.delete_expired()``
    (``varco_core/varco_core/revocation/base.py:173``) — same shape as
    ``IdempotencyRetentionTarget``, see its docstring for the reasoning.
    """

    supports_older_than = False
    supports_dry_run = False

    def __init__(self, *, store: AbstractTokenRevocationStore) -> None:
        self._store = store

    @property
    def kind(self) -> str:
        return "revocation"

    async def purge(
        self, *, older_than: datetime | None, limit: int, dry_run: bool
    ) -> RetentionOutcome:
        if older_than is not None:
            raise ValueError(
                "RevocationRetentionTarget does not support older_than — "
                "delete_expired() has no cutoff parameter, expiry is intrinsic "
                "to each entry."
            )
        if dry_run:
            return RetentionOutcome(
                kind="revocation",
                deleted=0,
                would_delete=0,
                dry_run=True,
                skipped_reason=(
                    "delete_expired() has no non-destructive preview — skipping "
                    "under dry_run=True rather than actually delete."
                ),
            )
        deleted = await self._store.delete_expired()
        return RetentionOutcome(
            kind="revocation", examined=None, deleted=deleted, would_delete=0, dry_run=False
        )


class JobRetentionTarget(RetentionTarget):
    """
    Wraps ``AbstractJobStore.delete_where()``
    (``varco_core/varco_core/job/base.py:880``), mapping ``older_than`` to
    ``completed_before`` and restricting to ``JobStatus.COMPLETED``.

    ⚠️ **``JobPoller(retention_sweep=True)``
    (``varco_fastapi/varco_fastapi/job/poller.py:75-88``) may already cover
    this** — it is an already-shipped, off-by-default periodic job-store
    sweep. This target exists for completeness (a uniform ``RetentionTarget``
    surface across every subsystem) but running both against the same store
    double-sweeps; deletion is idempotent so this is not unsafe, only
    redundant (documented in the Pitfalls table).
    """

    supports_older_than = True
    supports_dry_run = True

    def __init__(self, *, store: AbstractJobStore) -> None:
        self._store = store

    @property
    def kind(self) -> str:
        return "job"

    async def purge(
        self, *, older_than: datetime | None, limit: int, dry_run: bool
    ) -> RetentionOutcome:
        from varco_core.job.base import JobStatus  # noqa: PLC0415

        if dry_run:
            candidates = await self._store.list_by_status(JobStatus.COMPLETED, limit=limit)
            matched = [
                j
                for j in candidates
                if older_than is None
                or (j.completed_at is not None and j.completed_at < older_than)
            ]
            return RetentionOutcome(
                kind="job",
                examined=len(candidates),
                deleted=0,
                would_delete=len(matched),
                dry_run=True,
            )
        deleted = await self._store.delete_where(
            status=JobStatus.COMPLETED, completed_before=older_than, limit=limit
        )
        return RetentionOutcome(
            kind="job", examined=None, deleted=deleted, would_delete=0, dry_run=False
        )
