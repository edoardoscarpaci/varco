"""
varco_beanie.repository
===========================
Async Beanie CRUD + query repository.

Provides the full ``AsyncRepository`` implementation backed by Beanie
Documents (Motor / MongoDB).  Supports filtering via ``BeanieQueryCompiler``,
sort directives, and limit/skip pagination.

Thread safety:  ✅ Motor connection pool is thread-safe.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Generic, Sequence, TypeVar

from varco_core.mapper import AbstractMapper
from varco_core.model import DomainModel
from varco_core.query.params import QueryParams
from varco_core.query.type import SortOrder
from varco_core.repository import AsyncRepository

from varco_beanie.query.compiler import BeanieQueryCompiler

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")


class AsyncBeanieRepository(AsyncRepository[D, PK], Generic[D, PK]):
    """
    Async CRUD + query repository backed by Beanie Documents (Motor/MongoDB).

    No session injection — Beanie manages its Motor connection pool globally
    via ``beanie.init_beanie()`` called at application startup.

    INSERT vs UPDATE detection
    --------------------------
    Uses ``entity._raw_orm is None`` — identical logic to
    ``AsyncSQLAlchemyRepository``, correct for all PK strategies including
    ``STR_ASSIGNED`` with ``pk_field(init=True)``.

    Composite PK
    ------------
    ``find_by_id(pk_tuple)`` issues ``find_one({field: val, ...})`` using the
    mapper's ``_pk_orm_attrs`` — MongoDB has no native composite ``_id``.

    Query system
    ------------
    ``find_by_query`` uses ``BeanieQueryCompiler`` to translate the AST into
    a MongoDB filter dict, then passes it to ``Document.find()``.

    Thread safety:  ✅ Motor connections are thread-safe.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        mapper: Auto-generated mapper produced by ``BeanieModelFactory``.
    """

    def __init__(self, mapper: AbstractMapper[D, Any]) -> None:
        """
        Initialise the repository.

        Args:
            mapper: Bidirectional domain ↔ Beanie Document mapper.
        """
        self._mapper = mapper

    # ── CRUD ───────────────────────────────────────────────────────────────────

    async def find_by_id(self, pk: PK) -> D | None:
        """
        Retrieve a single entity by primary key.

        For composite PKs, issues ``find_one`` with a filter dict built from
        the mapper's ``_pk_orm_attrs`` list.

        Args:
            pk: Scalar PK or tuple for composite PKs.

        Returns:
            Domain entity, or ``None`` if not found.
        """
        if isinstance(pk, tuple):
            # Composite PK: build a {field: value, ...} filter dict
            query = {attr: val for attr, val in zip(self._mapper._pk_orm_attrs, pk)}
            orm_obj = await self._mapper._orm_cls.find_one(query)
        else:
            # Single PK: Beanie's .get() uses the _id field
            orm_obj = await self._mapper._orm_cls.get(pk)
        return None if orm_obj is None else self._mapper.from_orm(orm_obj)

    async def find_all(self) -> list[D]:
        """
        Retrieve all documents with no filtering or pagination.

        Returns:
            List of all domain entities.  Empty when the collection is empty.

        Edge cases:
            - Avoid on large collections — use ``find_by_query`` with pagination.
        """
        orm_objects = await self._mapper._orm_cls.find_all().to_list()
        return [self._mapper.from_orm(obj) for obj in orm_objects]

    async def save(self, entity: D) -> D:
        """
        INSERT or UPDATE based on ``entity._raw_orm``.

        Args:
            entity: Domain entity to persist.

        Returns:
            Fresh domain entity with ``pk`` and ``_raw_orm`` populated.
            Always use the returned value — the input is never mutated.
        """
        if entity._raw_orm is None:
            # ── INSERT ────────────────────────────────────────────────────────
            orm_obj = self._mapper.to_orm(entity)
            await orm_obj.insert()
            return self._mapper.from_orm(orm_obj)

        # ── UPDATE ────────────────────────────────────────────────────────────
        raw: Any = entity._raw_orm
        self._mapper.sync_to_orm(entity, raw)
        await raw.save()
        return self._mapper.from_orm(raw)

    async def delete(self, entity: D) -> None:
        """
        Delete the document from MongoDB.

        Args:
            entity: Domain entity to delete.

        Raises:
            ValueError: Entity has not been persisted yet (no PK).
        """
        if not entity.is_persisted():
            raise ValueError(
                f"Cannot delete {type(entity).__name__}: not yet persisted (pk is None)."
            )
        raw: Any = entity._raw_orm
        if raw is not None:
            await raw.delete()
            return

        # _raw_orm may be None after a find_by_id round-trip through a
        # detached mapper — fall back to re-fetching by PK
        orm_obj = await self._mapper._orm_cls.get(entity.pk)
        if orm_obj is not None:
            await orm_obj.delete()

    # ── Query + pagination ─────────────────────────────────────────────────────

    async def find_by_query(self, params: QueryParams) -> list[D]:
        """
        Execute a filtered, sorted, paginated query against the MongoDB collection.

        Translates the AST node into a MongoDB filter dict via
        ``BeanieQueryCompiler``, then chains ``.sort()``, ``.skip()``, and
        ``.limit()`` onto the Beanie ``FindMany`` object.

        Args:
            params: ``QueryParams`` containing the AST node, sort directives,
                    and pagination.

        Returns:
            List of matching domain entities.  Empty when nothing matches.

        Edge cases:
            - ``params.node is None`` → no filter applied (finds all documents).
            - ``params.sort`` empty   → default MongoDB insertion order.
            - Dotted field paths in AST are supported (MongoDB dot-notation).
        """
        # Build the MongoDB filter dict from the AST (or empty dict = no filter)
        mongo_filter: dict[str, Any] = {}
        if params.node is not None:
            mongo_filter = BeanieQueryCompiler().visit(params.node)

        find_query = self._mapper._orm_cls.find(mongo_filter)

        # Apply sort directives — Beanie accepts [(field, direction), ...]
        # direction: 1 = ascending, -1 = descending (pymongo convention)
        if params.sort:
            sort_list = [
                (sf.field, -1 if sf.order == SortOrder.DESC else 1)
                for sf in params.sort
            ]
            find_query = find_query.sort(sort_list)

        # Apply pagination
        if params.offset is not None:
            find_query = find_query.skip(params.offset)
        if params.limit is not None:
            find_query = find_query.limit(params.limit)

        orm_objects = await find_query.to_list()
        return [self._mapper.from_orm(obj) for obj in orm_objects]

    async def count(self, params: QueryParams | None = None) -> int:
        """
        Count documents matching the optional filter in ``params``.

        Uses Beanie's ``.count()`` — does not load any documents.

        Args:
            params: Optional ``QueryParams``.  Sort and pagination are ignored.
                    When ``None`` or ``params.node is None``, counts all documents.

        Returns:
            Integer count.  ``0`` when no documents match.
        """
        mongo_filter: dict[str, Any] = {}
        if params is not None and params.node is not None:
            mongo_filter = BeanieQueryCompiler().visit(params.node)

        # find() with empty dict = no filter; .count() returns total matching
        return await self._mapper._orm_cls.find(mongo_filter).count()

    async def exists(self, pk: PK) -> bool:
        """
        Return ``True`` if a document with ``pk`` exists, using Beanie's count.

        Issues a lightweight ``count()`` instead of fetching the full document.

        Args:
            pk: Scalar PK or tuple for composite PKs.

        Returns:
            ``True`` if at least one document matches; ``False`` otherwise.

        Thread safety:  ✅ Motor connections are thread-safe.
        Async safety:   ✅ ``async def`` — awaitable.

        Edge cases:
            - Composite PKs: issues ``find_one`` with a filter dict built from
              ``mapper._pk_orm_attrs``; for scalar PKs uses Beanie's ``find``
              with the ``_id`` filter.
        """
        if isinstance(pk, tuple):
            # Composite PK — build a {field: value, ...} filter dict.
            # count() avoids loading the document body.
            query = {attr: val for attr, val in zip(self._mapper._pk_orm_attrs, pk)}
            count = await self._mapper._orm_cls.find(query).count()
        else:
            # Scalar PK — filter on _id (Beanie's primary key field)
            count = await self._mapper._orm_cls.find({"_id": pk}).count()
        return count > 0

    # ── Bulk operations ────────────────────────────────────────────────────────

    async def save_many(self, entities: Sequence[D]) -> list[D]:
        """
        Bulk INSERT or UPDATE a sequence of entities.

        Inserts are sent as a single ``Document.insert_many()`` call — one
        Motor round-trip for the whole batch.  Updates fall back to individual
        ``raw.save()`` calls because Beanie has no bulk-update API that
        integrates with the mapper's ``sync_to_orm()`` field-by-field sync.

        DESIGN: insert_many() for inserts, individual save() for updates
          ✅ Insert batch: single Motor ``insertMany`` command.
          ✅ Update: existing ``_raw_orm`` reference is reused — no re-fetch.
          ❌ Update batch: N individual ``save()`` calls — not a single command.
             Acceptable because typical update batches are small; for very
             large update batches consider ``update_many_by_query`` instead.

        Args:
            entities: Sequence of domain entities.  Order is preserved.

        Returns:
            List of persisted entities with ``pk`` and ``_raw_orm`` populated.

        Thread safety:  ✅ Motor connections are thread-safe.
        Async safety:   ✅ All awaits are sequential within this coroutine.

        Edge cases:
            - Empty sequence → returns ``[]``.
            - Mixed INSERT / UPDATE in the same call is supported.
        """
        if not entities:
            return []

        inserts = [e for e in entities if e._raw_orm is None]
        updates = [e for e in entities if e._raw_orm is not None]

        results: list[D] = []

        if inserts:
            # Convert domain entities to Beanie Document objects, then bulk-insert.
            orm_objs = [self._mapper.to_orm(e) for e in inserts]
            # insert_many() sends a single Motor insertMany command.
            await self._mapper._orm_cls.insert_many(orm_objs)
            results.extend(self._mapper.from_orm(obj) for obj in orm_objs)

        if updates:
            for entity in updates:
                raw: Any = entity._raw_orm
                self._mapper.sync_to_orm(entity, raw)
                await raw.save()
                results.append(self._mapper.from_orm(raw))

        return results

    async def delete_many(self, entities: Sequence[D]) -> None:
        """
        Bulk DELETE a sequence of entities using a single ``$in`` query.

        Issues one Motor ``deleteMany`` command rather than N ``delete()``
        calls.

        DESIGN: find + delete over individual delete loop
          ✅ Single DB round-trip.
          ❌ No cascade support — related documents are not cleaned up.

        Args:
            entities: Sequence of persisted domain entities.

        Raises:
            ValueError: Any entity has not been persisted yet.

        Thread safety:  ✅ Motor connections are thread-safe.
        Async safety:   ✅ Awaited once.

        Edge cases:
            - Empty sequence → no-op.
            - PKs absent from the collection are silently ignored.
        """
        if not entities:
            return

        for entity in entities:
            if not entity.is_persisted():
                raise ValueError(
                    f"Cannot delete {type(entity).__name__}: "
                    "not yet persisted (pk is None)."
                )

        # Build a MongoDB $in filter on the _id (Beanie primary key field).
        pks = [e.pk for e in entities]
        await self._mapper._orm_cls.find({"_id": {"$in": pks}}).delete()

    async def update_many_by_query(
        self,
        params: QueryParams,
        update: dict[str, Any],
    ) -> int:
        """
        Apply a partial ``$set`` update to all documents matching ``params``.

        Uses Beanie's ``find(filter).update({"$set": …})`` — one Motor
        ``updateMany`` command regardless of how many documents match.

        DESIGN: $set updateMany over fetch-and-save loop
          ✅ Single Motor round-trip.
          ✅ Scales to large collections without memory growth.
          ❌ Skips domain-model validation and service-layer hooks.

        Args:
            params: ``QueryParams`` whose ``node`` selects target documents.
                    Sort and pagination are ignored for bulk updates.
                    ``params.node is None`` → updates every document.
            update: Field name → new value.  Must not be empty.

        Returns:
            Number of documents modified (Motor's ``modified_count``).

        Raises:
            ValueError: ``update`` is an empty dict.

        Thread safety:  ✅ Motor connections are thread-safe.
        Async safety:   ✅ Awaited once.

        Edge cases:
            - ``params.node is None`` → full-collection UPDATE (use with caution).
            - Unknown field names are silently ignored by MongoDB — no error raised.
        """
        if not update:
            raise ValueError(
                "update_many_by_query: 'update' dict must not be empty. "
                "Provide at least one field name → value pair."
            )

        # Translate the AST filter to a MongoDB filter dict
        mongo_filter: dict[str, Any] = {}
        if params.node is not None:
            mongo_filter = BeanieQueryCompiler().visit(params.node)

        result = await self._mapper._orm_cls.find(mongo_filter).update({"$set": update})
        # Motor's UpdateResult.modified_count = documents actually changed
        return result.modified_count  # type: ignore[return-value]

    async def stream_by_query(  # type: ignore[override]
        self,
        params: QueryParams,
    ) -> AsyncIterator[D]:
        """
        Yield documents one at a time by iterating over a Beanie ``FindMany``.

        Motor does not expose named server-side cursors in the same way as SQL
        DBs, but async iteration over a ``FindMany`` object fetches documents
        in batches from the MongoDB cursor, keeping memory usage bounded.

        Args:
            params: ``QueryParams`` with filter, sort, and pagination.

        Returns:
            ``AsyncIterator[D]`` that yields domain entities one at a time.

        Thread safety:  ✅ Motor connections are thread-safe.
        Async safety:   ✅ Async generator — safe to iterate with ``async for``.

        Edge cases:
            - Unlike the SQL streaming cursor, abandoning the iterator does not
              leak a connection — Motor's cursor is automatically garbage-
              collected by the driver.
            - ``params.limit`` still caps the total number of yielded documents.
        """
        # Build the same FindMany query as find_by_query — reuses all filter,
        # sort, and pagination logic, but yields instead of calling to_list()
        mongo_filter: dict[str, Any] = {}
        if params.node is not None:
            mongo_filter = BeanieQueryCompiler().visit(params.node)

        find_query = self._mapper._orm_cls.find(mongo_filter)

        if params.sort:
            sort_list = [
                (sf.field, -1 if sf.order == SortOrder.DESC else 1)
                for sf in params.sort
            ]
            find_query = find_query.sort(sort_list)

        if params.offset is not None:
            find_query = find_query.skip(params.offset)
        if params.limit is not None:
            find_query = find_query.limit(params.limit)

        # DESIGN: async for over FindMany instead of to_list()
        # ✅ Beanie/Motor fetches documents in internal batches — bounded memory.
        # ✅ Caller controls back-pressure — can stop iterating at any point.
        # ❌ FindMany does not expose explicit cursor control — cleanup is
        #    handled by Motor's internal GC, not by aclose().
        async for doc in find_query:
            yield self._mapper.from_orm(doc)
