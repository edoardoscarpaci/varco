"""
tests.test_retention_targets
===============================
Plan 039 (S20) / Steps 7+8 — a parametrized contract table over all six
``RetentionTarget`` implementations (§D-S20-seam / §D-S20-conformance),
plus per-target specifics against in-memory backends.

RED until ``varco_core/varco_core/retention/base.py`` (Step 5) and
``varco_core/varco_core/retention/targets.py`` (Step 9) land.

The single most important assertion in the whole plan: a ``dry_run=True``
sweep must provably call no delete method on any target — including the
two (Idempotency, Revocation) that cannot preview and are therefore
SKIPPED.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from varco_core.event import Event
from varco_core.event.dlq import DeadLetterEntry, InMemoryDeadLetterQueue
from varco_core.idempotency.memory import InMemoryIdempotencyStore
from varco_core.job.base import Job, JobStatus
from varco_core.revocation.memory import InMemoryTokenRevocationStore
from varco_core.service.audit import AuditEntry, AuditRepository

# ── shared fakes ────────────────────────────────────────────────────────────


class SampleEvent(Event):
    __event_type__ = "test.retention_targets.sample"


class FakeAuditRepository(AuditRepository):
    """A minimal in-memory AuditRepository implementing list()/delete_where()
    (varco_core's own InMemoryAuditRepository test double does not)."""

    def __init__(self, *, hash_chain: bool = False) -> None:
        self.hash_chain = hash_chain
        self.entries: list[AuditEntry] = []

    async def save(self, entry: AuditEntry) -> None:
        self.entries.append(entry)

    async def list_for_entity(self, entity_type, entity_id, *, limit=100, tenant_id=None):
        return [e for e in self.entries if e.entity_type == entity_type][:limit]

    async def list(
        self,
        *,
        entity_type=None,
        tenant_id=None,
        occurred_from=None,
        occurred_to=None,
        limit=100,
        offset=0,
    ):
        result = self.entries
        if entity_type is not None:
            result = [e for e in result if e.entity_type == entity_type]
        if occurred_from is not None:
            result = [e for e in result if e.occurred_at >= occurred_from]
        return result[offset : offset + limit]

    async def delete_where(
        self,
        *,
        older_than=None,
        entity_type=None,
        tenant_id=None,
        limit=None,
        allow_chain_break=False,
    ) -> int:
        if older_than is None and entity_type is None and tenant_id is None:
            raise ValueError("delete_where() requires at least one predicate.")
        if self.hash_chain and not allow_chain_break:
            raise ValueError("hash-chained table — pass allow_chain_break=True")
        matches = [e for e in self.entries if older_than is None or e.occurred_at < older_than]
        if limit is not None:
            matches = matches[:limit]
        for e in matches:
            self.entries.remove(e)
        return len(matches)


class FakeJobStore:
    """Minimal AbstractJobStore-shaped fake with delete_where(), enough for
    JobRetentionTarget tests."""

    def __init__(self) -> None:
        self._jobs: dict[object, Job] = {}

    async def save(self, job: Job, *, expected_epoch=None) -> None:
        self._jobs[job.job_id] = job

    async def get(self, job_id):
        return self._jobs.get(job_id)

    async def list_by_status(self, status, *, limit=100):
        return [j for j in self._jobs.values() if j.status == status][:limit]

    async def delete(self, job_id) -> None:
        self._jobs.pop(job_id, None)

    async def delete_where(
        self, *, status=None, completed_before=None, expires_before=None, limit=None
    ):
        if status is None and completed_before is None and expires_before is None:
            raise ValueError("delete_where() requires at least one predicate.")
        matches = list(self._jobs.values())
        if status is not None:
            statuses = (status,) if isinstance(status, JobStatus) else tuple(status)
            matches = [j for j in matches if j.status in statuses]
        if completed_before is not None:
            matches = [
                j
                for j in matches
                if j.completed_at is not None and j.completed_at < completed_before
            ]
        if limit is not None:
            matches = matches[:limit]
        for j in matches:
            del self._jobs[j.job_id]
        return len(matches)


async def _dlq_target(supports_older_than=True):
    from varco_core.retention.targets import DlqRetentionTarget  # noqa: PLC0415

    dlq = InMemoryDeadLetterQueue()
    for _ in range(3):
        await dlq.push(
            DeadLetterEntry(
                event=SampleEvent(),
                channel="orders",
                handler_name="H.h",
                error_type="E",
                error_message="msg",
                attempts=1,
            )
        )
    target = DlqRetentionTarget(dlq=dlq, acknowledge_dead_letter_deletion=True)
    return target, dlq


async def _audit_target(hash_chain=False):
    from varco_core.retention.targets import AuditRetentionTarget  # noqa: PLC0415

    repo = FakeAuditRepository(hash_chain=hash_chain)
    for i in range(3):
        repo.entries.append(
            AuditEntry(
                entity_type="Order",
                entity_id=str(i),
                action="create",
                occurred_at=datetime(2020, 1, 1, tzinfo=UTC),
            )
        )
    target = AuditRetentionTarget(repo=repo, allow_chain_break=hash_chain)
    return target, repo


async def _idempotency_target():
    from varco_core.retention.targets import IdempotencyRetentionTarget  # noqa: PLC0415

    store = InMemoryIdempotencyStore()
    target = IdempotencyRetentionTarget(store=store)
    return target, store


async def _revocation_target():
    from varco_core.retention.targets import RevocationRetentionTarget  # noqa: PLC0415

    store = InMemoryTokenRevocationStore()
    target = RevocationRetentionTarget(store=store)
    return target, store


async def _job_target():
    from varco_core.retention.targets import JobRetentionTarget  # noqa: PLC0415

    store = FakeJobStore()
    for _ in range(3):
        job = Job(job_id=__import__("uuid").uuid4())
        completed = job.as_running().as_completed(b"ok")
        await store.save(completed)
    target = JobRetentionTarget(store=store)
    return target, store


TARGET_FACTORIES = {
    "dlq": _dlq_target,
    "audit": _audit_target,
    "idempotency": _idempotency_target,
    "revocation": _revocation_target,
    "job": _job_target,
}


# ── the parametrized contract table (Step 7) ────────────────────────────────


@pytest.mark.parametrize("kind", list(TARGET_FACTORIES))
class TestRetentionTargetContract:
    async def test_kind_is_non_empty(self, kind) -> None:
        target, _ = await TARGET_FACTORIES[kind]()
        assert target.kind
        assert isinstance(target.kind, str)

    async def test_dry_run_with_dry_run_support_calls_no_delete_method(self, kind) -> None:
        target, backend = await TARGET_FACTORIES[kind]()
        if not target.supports_dry_run:
            pytest.skip("no dry-run support — covered by the next test")

        delete_spy = AsyncMock(wraps=None)
        delete_attr = "delete_where" if hasattr(backend, "delete_where") else "delete_expired"
        original = getattr(backend, delete_attr)
        delete_spy.side_effect = original
        setattr(backend, delete_attr, delete_spy)

        outcome = await target.purge(
            older_than=datetime.now(UTC) if target.supports_older_than else None,
            limit=1000,
            dry_run=True,
        )
        assert outcome.deleted == 0
        assert outcome.would_delete >= 0
        delete_spy.assert_not_called()

    async def test_dry_run_without_dry_run_support_is_skipped(self, kind) -> None:
        target, backend = await TARGET_FACTORIES[kind]()
        if target.supports_dry_run:
            pytest.skip("has dry-run support — covered by the previous test")

        delete_spy = AsyncMock(wraps=None)
        original = backend.delete_expired
        delete_spy.side_effect = original
        backend.delete_expired = delete_spy

        outcome = await target.purge(older_than=None, limit=1000, dry_run=True)
        assert outcome.deleted == 0
        assert outcome.skipped_reason
        delete_spy.assert_not_called()

    async def test_limit_is_forwarded(self, kind) -> None:
        target, _ = await TARGET_FACTORIES[kind]()
        outcome = await target.purge(
            older_than=datetime.now(UTC) if target.supports_older_than else None,
            limit=1,
            dry_run=False,
        )
        assert outcome.deleted <= 1

    async def test_purge_propagates_backend_value_error(self, kind, monkeypatch) -> None:
        """§D-S20-safety guard 7 (`RetentionTarget.purge()` docstring's
        Raises section, `base.py:120-123`): the underlying verb's own
        no-predicate ``ValueError`` (or any other ``ValueError`` it raises)
        is never swallowed by an adapter — it propagates out of ``purge()``
        unchanged, same type and same message. This currently holds only
        "by construction" (no adapter wraps its delegated call in
        ``try``/``except``) — this test makes that a regression guard rather
        than an unstated invariant."""
        target, backend = await TARGET_FACTORIES[kind]()
        delete_attr = "delete_where" if hasattr(backend, "delete_where") else "delete_expired"

        async def _boom(*args: object, **kwargs: object) -> int:
            raise ValueError("boom-from-backend")

        monkeypatch.setattr(backend, delete_attr, _boom)

        with pytest.raises(ValueError, match="boom-from-backend"):
            await target.purge(
                older_than=(
                    datetime.now(UTC) + timedelta(days=1) if target.supports_older_than else None
                ),
                limit=10,
                dry_run=False,
            )


# ── per-target specifics (Step 8) ───────────────────────────────────────────


class TestDlqRetentionTargetSpecifics:
    async def test_requires_acknowledge_dead_letter_deletion(self) -> None:
        from varco_core.retention.targets import DlqRetentionTarget  # noqa: PLC0415

        dlq = InMemoryDeadLetterQueue()
        with pytest.raises(ValueError):
            DlqRetentionTarget(dlq=dlq)

    async def test_forwards_predicates_to_delete_where(self) -> None:
        from varco_core.retention.targets import DlqRetentionTarget  # noqa: PLC0415

        dlq = InMemoryDeadLetterQueue()
        for _ in range(2):
            await dlq.push(
                DeadLetterEntry(
                    event=SampleEvent(),
                    channel="orders",
                    handler_name="H.h",
                    error_type="E",
                    error_message="msg",
                    attempts=1,
                )
            )
        target = DlqRetentionTarget(dlq=dlq, acknowledge_dead_letter_deletion=True)
        outcome = await target.purge(
            older_than=datetime.now(UTC) + timedelta(days=1), limit=10, dry_run=False
        )
        assert outcome.deleted == 2

    async def test_delete_where_not_implemented_error_propagates(self, monkeypatch) -> None:
        """A Kafka/NATS-backed DLQ refuses ``delete_where()`` with
        ``NotImplementedError`` (``dlq.py:500-504``, and the §Edge cases
        bullet on ``DlqRetentionTarget``'s own docstring) — the adapter must
        not swallow it."""
        from varco_core.retention.targets import DlqRetentionTarget  # noqa: PLC0415

        dlq = InMemoryDeadLetterQueue()
        target = DlqRetentionTarget(dlq=dlq, acknowledge_dead_letter_deletion=True)

        async def _refuses(*args: object, **kwargs: object) -> int:
            raise NotImplementedError(f"{type(dlq).__name__} does not support delete_where()")

        monkeypatch.setattr(dlq, "delete_where", _refuses)

        with pytest.raises(NotImplementedError):
            await target.purge(older_than=datetime.now(UTC), limit=10, dry_run=False)


class TestAuditRetentionTargetSpecifics:
    async def test_allow_chain_break_not_set_by_default(self) -> None:
        from varco_core.retention.targets import AuditRetentionTarget  # noqa: PLC0415

        repo = FakeAuditRepository(hash_chain=True)
        target = AuditRetentionTarget(repo=repo)
        with pytest.raises(ValueError):
            await target.purge(
                older_than=datetime.now(UTC) + timedelta(days=1), limit=10, dry_run=False
            )

    async def test_allow_chain_break_forwarded_when_set(self) -> None:
        from varco_core.retention.targets import AuditRetentionTarget  # noqa: PLC0415

        repo = FakeAuditRepository(hash_chain=True)
        for i in range(2):
            repo.entries.append(
                AuditEntry(
                    entity_type="Order",
                    entity_id=str(i),
                    action="create",
                    occurred_at=datetime(2020, 1, 1, tzinfo=UTC),
                )
            )
        target = AuditRetentionTarget(repo=repo, allow_chain_break=True)
        outcome = await target.purge(
            older_than=datetime.now(UTC) + timedelta(days=1), limit=10, dry_run=False
        )
        assert outcome.deleted == 2


class TestIdempotencyAndRevocationTargetsSpecifics:
    async def test_idempotency_rejects_older_than(self) -> None:
        from varco_core.retention.targets import IdempotencyRetentionTarget  # noqa: PLC0415

        target = IdempotencyRetentionTarget(store=InMemoryIdempotencyStore())
        with pytest.raises(ValueError):
            await target.purge(older_than=datetime.now(UTC), limit=10, dry_run=False)

    async def test_revocation_rejects_older_than(self) -> None:
        from varco_core.retention.targets import RevocationRetentionTarget  # noqa: PLC0415

        target = RevocationRetentionTarget(store=InMemoryTokenRevocationStore())
        with pytest.raises(ValueError):
            await target.purge(older_than=datetime.now(UTC), limit=10, dry_run=False)

    async def test_idempotency_returns_native_zero_as_deleted_zero(self) -> None:
        from varco_core.retention.targets import IdempotencyRetentionTarget  # noqa: PLC0415

        target = IdempotencyRetentionTarget(store=InMemoryIdempotencyStore())
        outcome = await target.purge(older_than=None, limit=10, dry_run=False)
        assert outcome.deleted == 0


class TestJobRetentionTargetSpecifics:
    async def test_older_than_maps_to_completed_before(self) -> None:
        from varco_core.retention.targets import JobRetentionTarget  # noqa: PLC0415

        store = FakeJobStore()
        job = Job(job_id=__import__("uuid").uuid4())
        completed = job.as_running().as_completed(b"ok")
        await store.save(completed)

        target = JobRetentionTarget(store=store)
        outcome = await target.purge(
            older_than=datetime.now(UTC) + timedelta(days=1), limit=10, dry_run=False
        )
        assert outcome.deleted == 1


class TestCallableRetentionTargetSpecifics:
    async def test_purge_propagates_callable_exception(self) -> None:
        """§D-S20-safety guard 7's counterpart for the out-of-tree escape
        hatch: ``CallableRetentionTarget.purge()`` delegates straight to the
        caller-supplied ``fn`` (``base.py:190``) with no ``try``/``except`` —
        a raised exception must propagate unchanged."""
        from varco_core.retention.base import CallableRetentionTarget  # noqa: PLC0415

        async def _boom(*, older_than, limit, dry_run) -> int:  # noqa: ARG001
            raise ValueError("callable-boom")

        target = CallableRetentionTarget(kind="custom", fn=_boom)

        with pytest.raises(ValueError, match="callable-boom"):
            await target.purge(older_than=datetime.now(UTC), limit=10, dry_run=False)
