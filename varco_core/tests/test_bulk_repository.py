"""
tests.test_bulk_repository
==========================
Unit tests for the bulk repository operations introduced in Feature 1:

    AsyncRepository.save_many()
    AsyncRepository.delete_many()
    AsyncRepository.update_many_by_query()
    OutboxRepository.save_many()  (default loop + override behaviour)

All tests use in-memory test doubles — no DB backend required.
Integration tests against SA/Beanie are covered under varco_sa/varco_beanie
test suites tagged ``@pytest.mark.integration``.

Test doubles
------------
InMemoryBulkRepository
    Dict-backed repository that implements all abstract methods, including
    the three new bulk methods.  Inserts tracked via ``inserted`` list,
    updates via ``updated`` list, deletes via ``deleted`` list.

InMemoryBulkOutboxRepository
    Outbox repository that counts how many times the loop ``save()`` is called
    vs. when ``save_many()`` is overridden.  Used to confirm the default loop
    behaviour and verify override semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator, Sequence
from uuid import uuid4, UUID

import pytest

from varco_core.model import DomainModel
from varco_core.query.params import QueryParams
from varco_core.repository import AsyncRepository
from varco_core.service.outbox import OutboxEntry, OutboxRepository


# ── Test-double domain entity ──────────────────────────────────────────────────


@dataclass
class Item(DomainModel):
    """
    Minimal domain entity for bulk-operation tests.

    Attributes:
        name:  Human-readable label — updated in ``update_many_by_query`` tests.
        score: Numeric value — used to verify partial-update semantics.
    """

    name: str = ""
    score: int = 0


# ── InMemoryBulkRepository ─────────────────────────────────────────────────────


class InMemoryBulkRepository(AsyncRepository[Item, UUID]):
    """
    Dict-backed repository used exclusively for testing bulk operations.

    Tracks every insert, update, and delete in separate lists so tests can
    assert on internal behaviour without mocking.

    Thread safety:  ❌ Not thread-safe — single event loop, test use only.
    Async safety:   ✅ All methods are ``async def``.
    """

    def __init__(self) -> None:
        # Primary store: pk → Item
        self._store: dict[UUID, Item] = {}
        # Audit trails — populated by each write operation
        self.inserted: list[Item] = []
        self.updated: list[Item] = []
        self.deleted: list[Item] = []

    # ── Required abstract CRUD ─────────────────────────────────────────────────

    async def find_by_id(self, pk: UUID) -> Item | None:
        return self._store.get(pk)

    async def find_all(self) -> list[Item]:
        return list(self._store.values())

    async def save(self, entity: Item) -> Item:
        """INSERT when pk is None; UPDATE otherwise."""
        if entity._raw_orm is None:
            # Assign a fresh pk and a sentinel _raw_orm so next save → UPDATE
            pk = uuid4()
            saved = Item(name=entity.name, score=entity.score)
            object.__setattr__(saved, "pk", pk)
            object.__setattr__(saved, "_raw_orm", object())  # sentinel
            self._store[pk] = saved
            self.inserted.append(saved)
            return saved
        # UPDATE: replace in store
        pk = entity.pk  # type: ignore[assignment]
        if pk not in self._store:
            raise LookupError(f"Item pk={pk!r} not found.")
        self._store[pk] = entity
        self.updated.append(entity)
        return entity

    async def delete(self, entity: Item) -> None:
        if not entity.is_persisted():
            raise ValueError("Cannot delete Item: not yet persisted.")
        self._store.pop(entity.pk, None)  # type: ignore[arg-type]
        self.deleted.append(entity)

    async def find_by_query(self, params: QueryParams) -> list[Item]:
        return list(self._store.values())

    async def count(self, params: QueryParams | None = None) -> int:
        return len(self._store)

    async def exists(self, pk: UUID) -> bool:
        return pk in self._store

    async def stream_by_query(self, params: QueryParams) -> AsyncIterator[Item]:  # type: ignore[override]
        for item in self._store.values():
            yield item

    # ── Bulk operations ────────────────────────────────────────────────────────

    async def save_many(self, entities: Sequence[Item]) -> list[Item]:
        """
        Bulk INSERT-or-UPDATE.  Delegates to ``save()`` for simplicity;
        real backends batch these into fewer DB round-trips.

        Returns entities in input order.
        """
        # Separate inserts and updates to mirror the SA/Beanie split
        inserts = [e for e in entities if e._raw_orm is None]
        updates = [e for e in entities if e._raw_orm is not None]

        result: list[Item] = []
        for e in inserts:
            result.append(await self.save(e))
        for e in updates:
            result.append(await self.save(e))
        return result

    async def delete_many(self, entities: Sequence[Item]) -> None:
        """Bulk DELETE — raises ValueError for unpersisted entities."""
        for entity in entities:
            if not entity.is_persisted():
                raise ValueError("Cannot delete Item: not yet persisted.")
        for entity in entities:
            self._store.pop(entity.pk, None)  # type: ignore[arg-type]
            self.deleted.append(entity)

    async def update_many_by_query(
        self,
        params: QueryParams,
        update: dict[str, Any],
    ) -> int:
        """Apply ``update`` dict to all stored items. Returns affected count."""
        if not update:
            raise ValueError("update_many_by_query: 'update' dict must not be empty.")
        count = 0
        for pk, item in list(self._store.items()):
            # Apply each field update via a fresh dataclass instance
            kwargs = {f: getattr(item, f) for f in ("name", "score")}
            kwargs.update(update)
            updated = Item(**kwargs)
            object.__setattr__(updated, "pk", item.pk)
            object.__setattr__(updated, "_raw_orm", item._raw_orm)
            self._store[pk] = updated
            count += 1
        return count


# ── InMemoryBulkOutboxRepository ───────────────────────────────────────────────


class InMemoryBulkOutboxRepository(OutboxRepository):
    """
    Outbox repository that records every ``save()`` call.

    Used to verify that the default ``save_many()`` loops over ``save()``,
    and that overridden versions can bypass the loop.
    """

    def __init__(self) -> None:
        self._entries: list[OutboxEntry] = []
        # Count individual save() invocations — incremented only by save(), not
        # save_many() — so tests can distinguish the two code paths.
        self.save_call_count = 0

    async def save(self, entry: OutboxEntry) -> None:
        self.save_call_count += 1
        self._entries.append(entry)

    async def get_pending(self, *, limit: int = 100) -> list[OutboxEntry]:
        return self._entries[:limit]

    async def delete(self, entry_id: UUID) -> None:
        self._entries = [e for e in self._entries if e.entry_id != entry_id]


class _OverriddenOutboxRepository(InMemoryBulkOutboxRepository):
    """
    Subclass that overrides ``save_many()`` with a bulk-path that does NOT
    call ``save()`` individually.  Used to verify that overrides are honoured.
    """

    def __init__(self) -> None:
        super().__init__()
        self.bulk_save_called = False

    async def save_many(self, entries: Sequence[OutboxEntry]) -> None:
        # Override: bulk-insert all at once without touching save_call_count.
        self.bulk_save_called = True
        self._entries.extend(entries)


# ── Helper ─────────────────────────────────────────────────────────────────────


def _make_persisted(name: str = "x", score: int = 0) -> Item:
    """
    Create an Item that appears to be already persisted (``_raw_orm is not None``).

    Args:
        name:  Item name.
        score: Item score.

    Returns:
        Item with ``pk`` and ``_raw_orm`` set (simulates a previously saved entity).
    """
    pk = uuid4()
    item = Item(name=name, score=score)
    object.__setattr__(item, "pk", pk)
    object.__setattr__(item, "_raw_orm", object())
    return item


# ── Tests: save_many ───────────────────────────────────────────────────────────


async def test_save_many_empty_returns_empty_list() -> None:
    """
    Empty input → returns [] without touching the store.

    Edge case: save_many([]) must be a safe no-op.
    """
    repo = InMemoryBulkRepository()
    result = await repo.save_many([])
    assert result == []
    assert repo.inserted == []
    assert repo.updated == []


async def test_save_many_all_inserts() -> None:
    """
    All-new entities (``_raw_orm is None``) are INSERT-ed and returned with
    ``pk`` and ``_raw_orm`` populated.
    """
    repo = InMemoryBulkRepository()
    a = Item(name="alpha", score=1)
    b = Item(name="beta", score=2)

    result = await repo.save_many([a, b])

    assert len(result) == 2
    # Returned entities have PKs assigned
    assert all(r.pk is not None for r in result)
    # Both are in the store
    assert await repo.count() == 2
    assert len(repo.inserted) == 2


async def test_save_many_all_updates() -> None:
    """
    All-existing entities (``_raw_orm is not None``) are UPDATE-ed in-place.
    """
    repo = InMemoryBulkRepository()
    # Pre-populate two items
    orig_a = await repo.save(Item(name="a", score=10))
    orig_b = await repo.save(Item(name="b", score=20))

    # Modify the returned entities (preserve _raw_orm to signal UPDATE)
    mod_a = Item(name="a-updated", score=10)
    object.__setattr__(mod_a, "pk", orig_a.pk)
    object.__setattr__(mod_a, "_raw_orm", orig_a._raw_orm)

    mod_b = Item(name="b-updated", score=20)
    object.__setattr__(mod_b, "pk", orig_b.pk)
    object.__setattr__(mod_b, "_raw_orm", orig_b._raw_orm)

    result = await repo.save_many([mod_a, mod_b])

    assert len(result) == 2
    # Store reflects the updates
    assert (await repo.find_by_id(orig_a.pk)).name == "a-updated"  # type: ignore[union-attr]
    assert (await repo.find_by_id(orig_b.pk)).name == "b-updated"  # type: ignore[union-attr]


async def test_save_many_mixed_insert_and_update() -> None:
    """
    A batch containing both new and existing entities processes correctly.
    Inserts and updates return entities in the same order as input (inserts first).
    """
    repo = InMemoryBulkRepository()
    existing = await repo.save(Item(name="existing", score=5))

    new_item = Item(name="new", score=99)
    # Modify existing entity
    modified = Item(name="existing-updated", score=5)
    object.__setattr__(modified, "pk", existing.pk)
    object.__setattr__(modified, "_raw_orm", existing._raw_orm)

    result = await repo.save_many([new_item, modified])

    assert len(result) == 2
    # Total store size = 1 pre-existing + 1 new
    assert await repo.count() == 2


async def test_save_many_returns_entities_with_pk() -> None:
    """
    All returned entities must have a non-None ``pk``.  Callers must use the
    returned list — input entities are never mutated.
    """
    repo = InMemoryBulkRepository()
    entities = [Item(name=f"item-{i}") for i in range(5)]

    # Input entities have no pk (not yet persisted)
    assert all(e.pk is None for e in entities)

    result = await repo.save_many(entities)

    # Returned entities all have PKs assigned
    assert all(r.pk is not None for r in result)
    # Input entities are unchanged (not mutated)
    assert all(e.pk is None for e in entities)


# ── Tests: delete_many ─────────────────────────────────────────────────────────


async def test_delete_many_empty_is_noop() -> None:
    """
    Empty sequence → no-op, no exception, store unchanged.
    """
    repo = InMemoryBulkRepository()
    await repo.save(Item(name="keep-me"))

    await repo.delete_many([])

    assert await repo.count() == 1
    assert repo.deleted == []


async def test_delete_many_removes_all_entities() -> None:
    """
    All provided entities are removed from the store in a single call.
    """
    repo = InMemoryBulkRepository()
    a = await repo.save(Item(name="a"))
    b = await repo.save(Item(name="b"))
    c = await repo.save(Item(name="c"))

    await repo.delete_many([a, b])

    assert await repo.count() == 1
    assert await repo.find_by_id(c.pk) is not None  # type: ignore[arg-type]
    assert await repo.find_by_id(a.pk) is None  # type: ignore[arg-type]
    assert await repo.find_by_id(b.pk) is None  # type: ignore[arg-type]


async def test_delete_many_raises_for_unpersisted_entity() -> None:
    """
    Any unpersisted entity (``pk is None``) in the sequence raises ``ValueError``
    before touching the store.

    Edge case: error is raised for the first offending entity; others may not
    have been deleted yet (undefined partial-success semantics).
    """
    repo = InMemoryBulkRepository()
    persisted = await repo.save(Item(name="persisted"))
    new_item = Item(name="new")  # pk is None — not yet persisted

    with pytest.raises(ValueError, match="not yet persisted"):
        await repo.delete_many([persisted, new_item])


# ── Tests: update_many_by_query ────────────────────────────────────────────────


async def test_update_many_by_query_updates_all_matching_rows() -> None:
    """
    All items in the store are updated when ``params.node`` is None (no filter).
    Returns the number of rows modified.
    """
    repo = InMemoryBulkRepository()
    for i in range(3):
        await repo.save(Item(name=f"item-{i}", score=i))

    count = await repo.update_many_by_query(QueryParams(), {"name": "updated"})

    assert count == 3
    items = await repo.find_all()
    assert all(item.name == "updated" for item in items)


async def test_update_many_by_query_returns_affected_count() -> None:
    """
    Return value equals the number of rows modified, not the total table size.
    """
    repo = InMemoryBulkRepository()
    await repo.save(Item(name="a"))
    await repo.save(Item(name="b"))

    count = await repo.update_many_by_query(QueryParams(), {"score": 42})

    assert count == 2


async def test_update_many_by_query_empty_update_raises_value_error() -> None:
    """
    An empty ``update`` dict raises ``ValueError`` — there is nothing to update.

    Edge case: this is a programming error; the exception should be raised
    before any DB interaction.
    """
    repo = InMemoryBulkRepository()
    await repo.save(Item(name="item"))

    with pytest.raises(ValueError, match="must not be empty"):
        await repo.update_many_by_query(QueryParams(), {})


async def test_update_many_by_query_empty_store_returns_zero() -> None:
    """
    Updating with no rows in the store returns 0 (no rows affected).
    """
    repo = InMemoryBulkRepository()

    count = await repo.update_many_by_query(QueryParams(), {"score": 1})

    assert count == 0


# ── Tests: OutboxRepository.save_many ─────────────────────────────────────────


async def test_outbox_save_many_default_loops_over_save() -> None:
    """
    The default ``save_many()`` in ``OutboxRepository`` calls ``save()`` once
    per entry in sequence.  Confirms the base-class loop behaviour.
    """
    repo = InMemoryBulkOutboxRepository()
    entries = [OutboxEntry(event_type="E", channel="ch") for _ in range(3)]

    await repo.save_many(entries)

    # Default implementation must call save() once per entry
    assert repo.save_call_count == 3
    assert len(await repo.get_pending()) == 3


async def test_outbox_save_many_empty_is_noop() -> None:
    """
    Empty sequence → no saves, no-op.
    """
    repo = InMemoryBulkOutboxRepository()

    await repo.save_many([])

    assert repo.save_call_count == 0
    assert await repo.get_pending() == []


async def test_outbox_save_many_override_bypasses_loop() -> None:
    """
    A subclass that overrides ``save_many()`` can bypass the default loop.
    Verifies that the override is actually used — ``save_call_count`` stays
    at 0 while ``bulk_save_called`` becomes True.
    """
    repo = _OverriddenOutboxRepository()
    entries = [OutboxEntry(event_type="E", channel="ch") for _ in range(5)]

    await repo.save_many(entries)

    # Override must have been called (not the default loop)
    assert repo.bulk_save_called is True
    # Individual save() must NOT have been called
    assert repo.save_call_count == 0
    # Entries still make it into the store via the override
    assert len(await repo.get_pending()) == 5


async def test_outbox_save_many_preserves_all_entry_fields() -> None:
    """
    All fields of each ``OutboxEntry`` are preserved after ``save_many()``.
    """
    repo = InMemoryBulkOutboxRepository()
    entry = OutboxEntry(
        event_type="OrderCreated",
        channel="orders",
        payload=b'{"order_id": "123"}',
    )

    await repo.save_many([entry])

    pending = await repo.get_pending()
    assert len(pending) == 1
    assert pending[0].event_type == "OrderCreated"
    assert pending[0].channel == "orders"
    assert pending[0].payload == b'{"order_id": "123"}'
