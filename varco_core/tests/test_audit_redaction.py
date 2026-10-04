"""
tests.test_audit_redaction
============================
Plan 040 / S21, Phase 3, Step 12 — the audit hook the docstring already
promised, made real: ``AuditLogMixin._audit_redactor`` / ``_audit_diff()``.

Covers:
    - default (Plan 057 / FLIP-040-2): ``PolicyRedactor(AUDIT_REDACTION_POLICY)``
      -> secret-named keys redacted, domain keys and non-JSON leaves untouched;
      ``_audit_redactor = None`` opts out byte-identically to 3.2.
    - ``_audit_redactor = PolicyRedactor()`` -> sensitive fields redacted,
      siblings survive.
    - a subclass overriding ``_audit_diff`` wins.
    - the hook runs once per emission, before ``_produce`` (assert on the
      produced ``AuditEvent``, never on a stored row).
    - hash-chain correctness (§D-S21-hashchain): a chain spanning the
      redactor's enablement verifies as True; recomputing over a *mutated*
      stored diff proves why read-path redaction is forbidden.
"""

from __future__ import annotations

from datetime import UTC
from typing import Any
from uuid import uuid4

from varco_core.event.audit_event import AuditEvent
from varco_core.service.audit import AuditEntry, AuditLogMixin, AuditRepository

# ── Shared fakes, mirroring test_audit.py's established shape ──────────────


class _FakeEntity:
    def __init__(self, pk: str) -> None:
        self.pk = pk


class _FakeReadDTO:
    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def model_dump(self) -> dict[str, Any]:
        return dict(self._data)


class _FakeAuthContext:
    def __init__(self, sub: str = "user:test", tenant_id: str = "tenant:test") -> None:
        self.sub = sub
        self.metadata = {"tenant_id": tenant_id}


class _FakeProducer:
    def __init__(self) -> None:
        self.produced: list[tuple[AuditEvent, str]] = []

    async def _produce(self, event: AuditEvent, *, channel: str = "*") -> None:
        self.produced.append((event, channel))


class _ServiceBase:
    async def _after_create(self, entity: Any, read_dto: Any, ctx: Any) -> None:
        return

    async def _after_update(self, before_dto: Any, entity: Any, read_dto: Any, ctx: Any) -> None:
        return

    async def _after_delete(self, pk: Any, ctx: Any) -> None:
        return


class _FakeMixinService(AuditLogMixin, _ServiceBase):
    def __init__(self, producer: _FakeProducer) -> None:
        self._producer = producer

    def _entity_type(self) -> type:
        return _FakeEntity

    def _get_audit_actor(self, ctx: _FakeAuthContext) -> str | None:  # type: ignore[override]
        return ctx.sub


class InMemoryAuditRepository(AuditRepository):
    def __init__(self) -> None:
        self.entries: list[AuditEntry] = []

    async def save(self, entry: AuditEntry) -> None:
        self.entries.append(entry)

    async def list_for_entity(
        self,
        entity_type: str,
        entity_id: str,
        *,
        limit: int = 100,
        tenant_id: str | None = None,
    ) -> list[AuditEntry]:
        return [e for e in self.entries if e.entity_type == entity_type]


# ── Default: redacted through AUDIT_REDACTION_POLICY (Plan 057 / FLIP-040-2) ─


async def test_audit_default_precondition_word_mode() -> None:
    """§D-order — FLIP-040-2 is conditional on FLIP-040-1; this test is the gate.

    Cherry-picking the audit flip without the word-boundary matcher would
    redact ``shipping_address``/``author`` in persisted audit rows
    (040 §D-S21-falsepos) — this test turns that into a red run instead.
    """
    from varco_core.redaction import (
        AUDIT_REDACTION_POLICY,
        DEFAULT_REDACT_PATTERNS,
        PolicyRedactor,
        is_sensitive_key,
    )

    assert AUDIT_REDACTION_POLICY.match_mode == "word"
    assert AUDIT_REDACTION_POLICY.render_non_json is False
    assert AUDIT_REDACTION_POLICY.patterns == DEFAULT_REDACT_PATTERNS
    assert is_sensitive_key("shipping_address", AUDIT_REDACTION_POLICY) is False
    assert is_sensitive_key("author", AUDIT_REDACTION_POLICY) is False
    service = _FakeMixinService(_FakeProducer())
    assert service._audit_redactor == PolicyRedactor(AUDIT_REDACTION_POLICY)


async def test_create_diff_redacted_by_default() -> None:
    producer = _FakeProducer()
    service = _FakeMixinService(producer)
    read_dto = _FakeReadDTO({"password": "hunter2", "field": "value"})

    await service._after_create(_FakeEntity(pk="ent-1"), read_dto, _FakeAuthContext())  # type: ignore[arg-type]

    event, _channel = producer.produced[0]
    assert event.diff == {"password": "[REDACTED]", "field": "value"}


async def test_update_diff_redacted_in_both_halves_by_default() -> None:
    producer = _FakeProducer()
    service = _FakeMixinService(producer)
    before = _FakeReadDTO({"password": "old", "name": "alice"})
    after = _FakeReadDTO({"password": "new", "name": "alice"})

    await service._after_update(before, _FakeEntity(pk="ent-2"), after, _FakeAuthContext())  # type: ignore[arg-type]

    event, _channel = producer.produced[0]
    assert event.diff == {
        "before": {"password": "[REDACTED]", "name": "alice"},
        "after": {"password": "[REDACTED]", "name": "alice"},
    }


async def test_default_does_not_redact_domain_keys() -> None:
    # The word matcher is what makes on-by-default safe for domain keys.
    producer = _FakeProducer()
    service = _FakeMixinService(producer)
    read_dto = _FakeReadDTO({"shipping_address": "1 Main St", "author": "alice", "pin": "1234"})

    await service._after_create(_FakeEntity(pk="ent-d"), read_dto, _FakeAuthContext())  # type: ignore[arg-type]

    event, _channel = producer.produced[0]
    assert event.diff == {
        "shipping_address": "1 Main St",
        "author": "alice",
        "pin": "[REDACTED]",
    }


async def test_default_leaves_non_json_leaves_byte_identical() -> None:
    # Premise 2: plain PolicyRedactor() would render these as "<datetime>" —
    # a silent, hash-attested corruption. render_non_json=False prevents it.
    from datetime import date, datetime
    from decimal import Decimal

    created = datetime(2026, 1, 1, tzinfo=UTC)
    pk = uuid4()
    amount = Decimal("12.50")
    day = date(2026, 1, 2)
    producer = _FakeProducer()
    service = _FakeMixinService(producer)
    read_dto = _FakeReadDTO({"created_at": created, "pk": pk, "amount": amount, "day": day})

    await service._after_create(_FakeEntity(pk="ent-n"), read_dto, _FakeAuthContext())  # type: ignore[arg-type]

    event, _channel = producer.produced[0]
    assert event.diff["created_at"] is created
    assert event.diff["pk"] is pk
    assert event.diff["amount"] is amount
    assert event.diff["day"] is day


async def test_delete_diff_is_empty_by_default() -> None:
    producer = _FakeProducer()
    service = _FakeMixinService(producer)

    await service._after_delete("pk-3", _FakeAuthContext())  # type: ignore[arg-type]

    event, _channel = producer.produced[0]
    assert event.diff == {}


class _OptedOutMixinService(_FakeMixinService):
    _audit_redactor = None


async def test_opt_out_with_none_is_byte_identical() -> None:
    # The 3.2 behaviour, reproduced verbatim behind the explicit opt-out.
    producer = _FakeProducer()
    service = _OptedOutMixinService(producer)
    ctx = _FakeAuthContext()

    await service._after_create(  # type: ignore[arg-type]
        _FakeEntity(pk="ent-o1"), _FakeReadDTO({"password": "hunter2", "field": "value"}), ctx
    )
    await service._after_update(  # type: ignore[arg-type]
        _FakeReadDTO({"password": "old"}),
        _FakeEntity(pk="ent-o2"),
        _FakeReadDTO({"password": "new"}),
        ctx,
    )
    await service._after_delete("pk-o3", ctx)  # type: ignore[arg-type]

    diffs = [event.diff for event, _ in producer.produced]
    assert diffs == [
        {"password": "hunter2", "field": "value"},
        {"before": {"password": "old"}, "after": {"password": "new"}},
        {},
    ]


async def test_chain_over_default_redacted_rows_verifies_true() -> None:
    # Redact-at-write is chain-safe: rows hash what they store.
    import dataclasses

    producer = _FakeProducer()
    service = _FakeMixinService(producer)
    ctx = _FakeAuthContext()
    await service._after_create(  # type: ignore[arg-type]
        _FakeEntity(pk="ent-c"), _FakeReadDTO({"password": "hunter2", "name": "a"}), ctx
    )
    await service._after_update(  # type: ignore[arg-type]
        _FakeReadDTO({"password": "hunter2", "name": "a"}),
        _FakeEntity(pk="ent-c"),
        _FakeReadDTO({"password": "hunter3", "name": "b"}),
        ctx,
    )

    entries: list[AuditEntry] = []
    prev_hash: str | None = None
    for seq, (event, _channel) in enumerate(producer.produced, start=1):
        entry = dataclasses.replace(AuditEntry.from_event(event), seq=seq, prev_hash=prev_hash)
        entries.append(entry)
        prev_hash = entry.entry_hash()

    assert AuditRepository.verify_chain(entries) is True
    assert entries[0].diff["password"] == "[REDACTED]"


# ── Opt-in: PolicyRedactor set as a class attribute ─────────────────────────


class _RedactedService(AuditLogMixin, _ServiceBase):
    def __init__(self, producer: _FakeProducer, redactor: Any) -> None:
        self._producer = producer
        self._audit_redactor = redactor

    def _entity_type(self) -> type:
        return _FakeEntity


async def test_create_diff_redacted_when_audit_redactor_set() -> None:
    from varco_core.redaction import PolicyRedactor

    producer = _FakeProducer()
    service = _RedactedService(producer, PolicyRedactor())
    entity = _FakeEntity(pk="ent-4")
    read_dto = _FakeReadDTO({"password": "hunter2", "field": "value"})
    ctx = _FakeAuthContext()

    await service._after_create(entity, read_dto, ctx)  # type: ignore[arg-type]

    event, _channel = producer.produced[0]
    assert event.diff == {"password": "[REDACTED]", "field": "value"}


async def test_update_diff_redacted_in_both_before_and_after() -> None:
    from varco_core.redaction import PolicyRedactor

    producer = _FakeProducer()
    service = _RedactedService(producer, PolicyRedactor())
    entity = _FakeEntity(pk="ent-5")
    before = _FakeReadDTO({"password": "old", "name": "alice"})
    after = _FakeReadDTO({"password": "new", "name": "alice"})
    ctx = _FakeAuthContext()

    await service._after_update(before, entity, after, ctx)  # type: ignore[arg-type]

    event, _channel = producer.produced[0]
    assert event.diff == {
        "before": {"password": "[REDACTED]", "name": "alice"},
        "after": {"password": "[REDACTED]", "name": "alice"},
    }


# ── Overriding _audit_diff wins over the class attribute ───────────────────


async def test_overriding_audit_diff_hook_wins() -> None:
    class _CustomHookService(AuditLogMixin, _ServiceBase):
        def __init__(self, producer: _FakeProducer) -> None:
            self._producer = producer

        def _entity_type(self) -> type:
            return _FakeEntity

        def _audit_diff(self, action: str, diff: dict[str, Any]) -> dict[str, Any]:
            return {"custom": True, "action": action}

    producer = _FakeProducer()
    service = _CustomHookService(producer)
    entity = _FakeEntity(pk="ent-6")
    read_dto = _FakeReadDTO({"password": "hunter2"})
    ctx = _FakeAuthContext()

    await service._after_create(entity, read_dto, ctx)  # type: ignore[arg-type]

    event, _channel = producer.produced[0]
    assert event.diff == {"custom": True, "action": "create"}


# ── Called once, before _produce ────────────────────────────────────────────


async def test_audit_diff_hook_called_exactly_once_per_emission() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class _CountingHookService(AuditLogMixin, _ServiceBase):
        def __init__(self, producer: _FakeProducer) -> None:
            self._producer = producer

        def _entity_type(self) -> type:
            return _FakeEntity

        def _audit_diff(self, action: str, diff: dict[str, Any]) -> dict[str, Any]:
            calls.append((action, dict(diff)))
            return diff

    producer = _FakeProducer()
    service = _CountingHookService(producer)
    entity = _FakeEntity(pk="ent-7")
    read_dto = _FakeReadDTO({"field": "value"})
    ctx = _FakeAuthContext()

    await service._after_create(entity, read_dto, ctx)  # type: ignore[arg-type]

    assert len(calls) == 1
    assert calls[0][0] == "create"
    # Hook ran before _produce — the producer already has the (possibly
    # transformed) result, proving ordering via the single recorded event.
    assert len(producer.produced) == 1


# ── Hash-chain correctness (§D-S21-hashchain) ───────────────────────────────


def _entry(diff: dict[str, Any], *, seq: int, prev_hash: str | None) -> AuditEntry:
    return AuditEntry(
        entry_id=uuid4(),
        entity_type="Order",
        entity_id="ord-1",
        action="update",
        actor_id="user:test",
        diff=diff,
        seq=seq,
        prev_hash=prev_hash,
    )


def test_chain_spanning_redactor_enablement_verifies_true() -> None:
    # Entry 1: written before the redactor was enabled (unredacted).
    entry1 = _entry({"password": "plain"}, seq=1, prev_hash=None)
    # Entry 2: written after the redactor was enabled.
    entry2 = _entry({"password": "[REDACTED]"}, seq=2, prev_hash=entry1.entry_hash())
    # Entry 3: another post-redaction entry.
    entry3 = _entry({"password": "[REDACTED]", "extra": "x"}, seq=3, prev_hash=entry2.entry_hash())

    result = AuditRepository.verify_chain([entry1, entry2, entry3])
    assert result is True


def test_entry_hash_is_a_function_of_stored_diff_mutation_breaks_chain() -> None:
    # Proves why read-path redaction is forbidden: recomputing entry_hash()
    # after mutating a *returned* AuditEntry's diff produces a mismatch
    # against the next entry's recorded prev_hash.
    entry1 = _entry({"password": "plain"}, seq=1, prev_hash=None)
    original_hash = entry1.entry_hash()
    entry2 = _entry({"field": "x"}, seq=2, prev_hash=original_hash)

    # Simulate a read-path redactor mutating the diff before verification.
    import dataclasses

    mutated_entry1 = dataclasses.replace(entry1, diff={"password": "[REDACTED]"})
    mutated_hash = mutated_entry1.entry_hash()

    assert mutated_hash != original_hash

    result = AuditRepository.verify_chain([mutated_entry1, entry2])
    assert result != True  # noqa: E712 - list of findings, not the sentinel True
    assert isinstance(result, list)
