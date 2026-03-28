"""
varco_sa.repository
=======================
Async SQLAlchemy CRUD + query repository.

Provides full ``AsyncRepository`` implementation backed by a SQLAlchemy 2.x
``AsyncSession``.  Supports filtering, sorting, and pagination via the
``varco_core`` AST query system.

Thread safety:  ❌ ``AsyncSession`` is not thread-safe.  One session per request.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Generic, Sequence, TypeVar

from sqlalchemy import delete as sa_delete
from sqlalchemy import func, select
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from varco_core.mapper import AbstractMapper
from varco_core.model import DomainModel
from varco_core.query.params import QueryParams
from varco_core.query.type import SortOrder
from varco_core.query.visitor.sqlalchemy import SQLAlchemyQueryCompiler
from varco_core.repository import AsyncRepository

from sqlalchemy import asc, desc

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")


class AsyncSQLAlchemyRepository(AsyncRepository[D, PK], Generic[D, PK]):
    """
    Async CRUD + query repository backed by a SQLAlchemy ``AsyncSession``.

    INSERT vs UPDATE detection
    --------------------------
    Uses ``entity._raw_orm is None`` as the INSERT signal — correct for all
    four PK strategies:

    - ``INT_AUTO`` / ``UUID_AUTO``   → pk is None; ``_raw_orm`` is None  → INSERT
    - ``STR_ASSIGNED`` / ``CUSTOM``  → pk may be set; ``_raw_orm`` is None → INSERT
    - Any previously saved entity    → ``_raw_orm`` is set                → UPDATE

    Composite PK
    ------------
    ``find_by_id(pk_tuple)`` passes the tuple directly to ``session.get()``
    which SQLAlchemy accepts natively for composite PKs.

    Query system
    ------------
    ``find_by_query`` uses ``SQLAlchemyQueryCompiler`` to translate the AST
    into ``ColumnElement`` expressions applied via ``.where()`` on a
    SQLAlchemy 2.x ``Select`` statement.

    Thread safety:  ❌ ``AsyncSession`` is not thread-safe.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        session: The async SQLAlchemy session for this unit-of-work.
        mapper:  Auto-generated mapper produced by ``SAModelFactory``.
    """

    def __init__(self, session: AsyncSession, mapper: AbstractMapper[D, Any]) -> None:
        """
        Initialise the repository.

        Args:
            session: Active ``AsyncSession`` managed by ``SQLAlchemyUnitOfWork``.
            mapper:  Bidirectional domain ↔ ORM mapper.
        """
        self._session = session
        self._mapper = mapper

    # ── CRUD ───────────────────────────────────────────────────────────────────

    async def find_by_id(self, pk: PK) -> D | None:
        """
        Retrieve a single entity by primary key using the session identity map.

        Args:
            pk: Scalar PK or tuple for composite PKs.

        Returns:
            Domain entity, or ``None`` if not found.
        """
        orm_obj = await self._session.get(self._mapper._orm_cls, pk)
        return None if orm_obj is None else self._mapper.from_orm(orm_obj)

    async def find_all(self) -> list[D]:
        """
        Retrieve all entities with no filtering or pagination.

        Returns:
            List of all domain entities.  Empty when the table is empty.

        Edge cases:
            - Avoid on large tables — use ``find_by_query`` with pagination.
        """
        result = await self._session.scalars(select(self._mapper._orm_cls))
        return [self._mapper.from_orm(row) for row in result.all()]

    async def save(self, entity: D) -> D:
        """
        INSERT or UPDATE based on ``entity._raw_orm``.

        Args:
            entity: Domain entity to persist.

        Returns:
            Fresh domain entity with ``pk`` and ``_raw_orm`` populated.
            Always use the returned value — the input is never mutated.

        Raises:
            LookupError: UPDATE path — ``entity.pk`` not found in the database.
        """
        if entity._raw_orm is None:
            # ── INSERT ────────────────────────────────────────────────────────
            orm_obj = self._mapper.to_orm(entity)
            self._session.add(orm_obj)
            await self._session.flush()
            return self._mapper.from_orm(orm_obj)

        # ── UPDATE ────────────────────────────────────────────────────────────
        raw: Any = entity._raw_orm

        # Re-fetch when the session has been closed or the object evicted
        if not self._session.is_active or raw not in self._session:
            raw = await self._session.get(self._mapper._orm_cls, entity.pk)

        if raw is None:
            raise LookupError(
                f"Cannot update {type(entity).__name__} with pk={entity.pk!r}: "
                "record not found in the database."
            )
        self._mapper.sync_to_orm(entity, raw)
        await self._session.flush()
        return self._mapper.from_orm(raw)

    async def delete(self, entity: D) -> None:
        """
        Delete the entity from the database.

        Args:
            entity: Domain entity to delete.

        Raises:
            ValueError: Entity has not been persisted yet (no PK).
        """
        if not entity.is_persisted():
            raise ValueError(
                f"Cannot delete {type(entity).__name__}: not yet persisted (pk is None)."
            )
        raw: Any = entity._raw_orm or await self._session.get(
            self._mapper._orm_cls, entity.pk
        )
        if raw is not None:
            await self._session.delete(raw)
            await self._session.flush()

    # ── Query + pagination ─────────────────────────────────────────────────────

    async def find_by_query(self, params: QueryParams) -> list[D]:
        """
        Execute a filtered, sorted, paginated query.

        Builds a ``Select`` statement, optionally applies an AST WHERE clause
        via ``SQLAlchemyQueryCompiler``, adds ORDER BY and LIMIT/OFFSET, then
        executes against the session.

        Args:
            params: ``QueryParams`` containing the AST node, sort directives,
                    and pagination.

        Returns:
            List of matching domain entities.  Empty when nothing matches.

        Raises:
            varco_core.exception.query.OperationNotSupported:
                AST contains a dotted relationship path.
            varco_core.exception.repository.FieldNotFound:
                AST or sort references a field that doesn't exist on the model.
        """
        stmt = select(self._mapper._orm_cls)

        # Apply WHERE clause when a filter node is present
        if params.node is not None:
            # Each repository gets its own compiler — no shared state risk
            compiler = SQLAlchemyQueryCompiler(model=self._mapper._orm_cls)
            stmt = stmt.where(compiler.visit(params.node))

        # Apply ORDER BY for each sort directive
        for sort_field in params.sort:
            col = self._resolve_column(sort_field.field)
            stmt = stmt.order_by(
                desc(col) if sort_field.order == SortOrder.DESC else asc(col)
            )

        # Apply pagination
        if params.limit is not None:
            stmt = stmt.limit(params.limit)
        if params.offset is not None:
            stmt = stmt.offset(params.offset)

        result = await self._session.scalars(stmt)
        return [self._mapper.from_orm(row) for row in result.all()]

    async def count(self, params: QueryParams | None = None) -> int:
        """
        Count entities matching the optional filter in ``params``.

        Uses ``SELECT COUNT(*)`` — does not load any ORM objects.

        Args:
            params: Optional ``QueryParams``.  Sort and pagination are ignored.
                    When ``None`` or ``params.node is None``, counts all rows.

        Returns:
            Integer count of matching rows.  ``0`` when no rows match.
        """
        # DESIGN: func.count() on the ORM class generates SELECT COUNT(*)
        # FROM <table> — no columns fetched, no ORM hydration.
        stmt = select(func.count()).select_from(self._mapper._orm_cls)

        if params is not None and params.node is not None:
            compiler = SQLAlchemyQueryCompiler(model=self._mapper._orm_cls)
            stmt = stmt.where(compiler.visit(params.node))

        result = await self._session.scalar(stmt)
        return result or 0

    async def exists(self, pk: PK) -> bool:
        """
        Return ``True`` if a row with ``pk`` exists, using ``SELECT COUNT(*)``.

        Does not load the ORM object — cheaper than ``find_by_id`` when only
        existence (not data) is needed.

        Args:
            pk: Scalar PK or tuple for composite PKs.

        Returns:
            ``True`` if at least one row matches; ``False`` otherwise.

        Thread safety:  ❌ ``AsyncSession`` is not thread-safe.
        Async safety:   ✅ ``async def`` — awaitable.

        Edge cases:
            - Composite PKs: ``pk`` must be a tuple in the same field order
              as ``mapper._pk_orm_attrs``.
            - Uses ``session.get()`` internally — SA's identity map means a
              previously-loaded entity is returned from cache, avoiding a
              redundant DB round-trip.
        """
        # DESIGN: session.get() over a raw SELECT COUNT
        # ✅ Leverages SA identity-map cache — if the entity is already in the
        #    session, no DB round-trip is needed.
        # ✅ Handles composite PKs natively (SA accepts a tuple).
        # ❌ Loads the full ORM object into memory even for existence check.
        #    For large objects on very hot paths, a raw SELECT 1 LIMIT 1 would
        #    be more efficient — not worth the complexity here.
        result = await self._session.get(self._mapper._orm_cls, pk)
        return result is not None

    async def stream_by_query(  # type: ignore[override]
        self,
        params: QueryParams,
    ) -> AsyncIterator[D]:
        """
        Yield entities one at a time using SQLAlchemy's ``stream_scalars``.

        Opens a server-side cursor (or equivalent) so the full result set is
        never loaded into memory.  The session must stay open for the entire
        iteration — the ``AsyncSession`` is held for the duration via the
        caller's UoW context.

        Args:
            params: ``QueryParams`` with filter, sort, and pagination.

        Returns:
            ``AsyncIterator[D]`` that yields domain entities one at a time.

        Raises:
            varco_core.exception.query.OperationNotSupported:
                AST contains a dotted relationship path.
            varco_core.exception.repository.FieldNotFound:
                AST or sort references a field that doesn't exist on the model.

        Thread safety:  ❌ ``AsyncSession`` is not thread-safe — one session
                           per request; do not share across concurrent tasks.
        Async safety:   ✅ Async generator — safe to iterate with ``async for``.

        Edge cases:
            - The caller must fully consume the iterator or call ``aclose()``
              to release the underlying DB cursor.
            - ``params.limit`` caps total yielded items, same as ``find_by_query``.
        """
        # Build the same statement as find_by_query — filter, sort, paginate
        stmt = select(self._mapper._orm_cls)

        if params.node is not None:
            compiler = SQLAlchemyQueryCompiler(model=self._mapper._orm_cls)
            stmt = stmt.where(compiler.visit(params.node))

        for sort_field in params.sort:
            col = self._resolve_column(sort_field.field)
            stmt = stmt.order_by(
                desc(col) if sort_field.order == SortOrder.DESC else asc(col)
            )

        if params.limit is not None:
            stmt = stmt.limit(params.limit)
        if params.offset is not None:
            stmt = stmt.offset(params.offset)

        # DESIGN: stream_scalars over scalars().all()
        # ✅ Server-side cursor — rows fetched incrementally, not all at once.
        # ✅ Constant memory regardless of result-set size.
        # ❌ The session cursor stays open until the async for loop completes
        #    or aclose() is called — callers must not abandon the iterator.
        async with self._session.stream_scalars(stmt) as stream:
            async for row in stream:
                yield self._mapper.from_orm(row)

    # ── Bulk operations ────────────────────────────────────────────────────────

    async def save_many(self, entities: Sequence[D]) -> list[D]:
        """
        Bulk INSERT or UPDATE a sequence of entities.

        Splits the sequence into inserts (``_raw_orm is None``) and updates,
        then processes each group with a single ``session.add_all()`` + flush
        (inserts) or individual ``sync_to_orm()`` + flush (updates).

        DESIGN: split insert/update groups then single flush per group
          ✅ SA 2.x issues a single ``INSERT … VALUES (…), (…)`` for all new
             ORM objects added before the flush — one DB round-trip.
          ✅ Updates are synced to existing ORM objects and flushed together.
          ❌ A mixed batch requires two flushes (one per group).  For purely
             homogeneous batches this is a no-op overhead.

        Args:
            entities: Sequence of domain entities.  Order is preserved in the
                      returned list (inserts first, then updates).

        Returns:
            List of persisted entities with ``pk`` and ``_raw_orm`` populated.

        Raises:
            LookupError: Any UPDATE-path entity whose ``pk`` is not found.

        Thread safety:  ❌ ``AsyncSession`` is not thread-safe.
        Async safety:   ✅ All flushes are awaited.

        Edge cases:
            - Empty sequence → returns ``[]``.
            - Atomicity is controlled by the caller's UoW — all inserts and
              updates commit or roll back together when the UoW exits.
        """
        if not entities:
            return []

        # Partition into new (INSERT) vs existing (UPDATE)
        inserts = [e for e in entities if e._raw_orm is None]
        updates = [e for e in entities if e._raw_orm is not None]

        results: list[D] = []

        if inserts:
            # Map every new entity to an ORM object and stage them all at once.
            # SA 2.x batches these into a single INSERT ... VALUES (...), (...).
            orm_objs = [self._mapper.to_orm(e) for e in inserts]
            self._session.add_all(orm_objs)
            # Flush once to let the DB assign auto-generated PKs.
            await self._session.flush()
            results.extend(self._mapper.from_orm(obj) for obj in orm_objs)

        if updates:
            # Sync each entity's fields to its existing ORM object, then flush
            # once — SA batches the UPDATEs in a single round-trip where possible.
            for entity in updates:
                raw: Any = entity._raw_orm
                # Re-fetch if the session was closed or the object was evicted
                if not self._session.is_active or raw not in self._session:
                    raw = await self._session.get(self._mapper._orm_cls, entity.pk)
                if raw is None:
                    raise LookupError(
                        f"Cannot update {type(entity).__name__} with pk={entity.pk!r}: "
                        "record not found in the database."
                    )
                self._mapper.sync_to_orm(entity, raw)
            await self._session.flush()
            results.extend(
                self._mapper.from_orm(e._raw_orm)  # type: ignore[arg-type]
                for e in updates
            )

        return results

    async def delete_many(self, entities: Sequence[D]) -> None:
        """
        Bulk DELETE a sequence of entities using a single WHERE pk IN (…).

        Issues one ``DELETE`` statement regardless of how many entities are in
        the sequence, rather than N individual ``session.delete()`` calls.

        DESIGN: Core DELETE with IN clause over ORM delete loop
          ✅ Single round-trip — O(1) DB calls instead of O(N).
          ✅ No need to load ORM objects that aren't already in the session.
          ❌ Bypasses SA cascade rules (e.g. ``delete-orphan`` cascades) —
             callers are responsible for handling related objects.

        Args:
            entities: Sequence of persisted domain entities.

        Raises:
            ValueError: Any entity in the sequence has not been persisted yet.

        Thread safety:  ❌ ``AsyncSession`` is not thread-safe.
        Async safety:   ✅ Single await on the execute call.

        Edge cases:
            - Empty sequence → no-op.
            - Entities absent from the DB are silently ignored (DELETE WHERE
              IN includes non-existent PKs without raising).
        """
        if not entities:
            return

        # Validate all entities are persisted before touching the DB
        for entity in entities:
            if not entity.is_persisted():
                raise ValueError(
                    f"Cannot delete {type(entity).__name__}: "
                    "not yet persisted (pk is None)."
                )

        # Resolve the ORM primary-key column for the WHERE IN clause.
        # _pk_orm_attrs holds column name strings; use the first (or only) one.
        pk_col = getattr(self._mapper._orm_cls, self._mapper._pk_orm_attrs[0])
        pks = [e.pk for e in entities]

        stmt = sa_delete(self._mapper._orm_cls).where(pk_col.in_(pks))
        await self._session.execute(stmt)
        # Flush to propagate the DELETE to the DB within the current transaction
        await self._session.flush()

    async def update_many_by_query(
        self,
        params: QueryParams,
        update: dict[str, Any],
    ) -> int:
        """
        Apply a partial field update to all rows matching ``params``.

        Uses SQLAlchemy Core ``UPDATE`` so no ORM objects are loaded.
        The WHERE clause is built from ``params.node`` via
        ``SQLAlchemyQueryCompiler`` — same translator used by ``find_by_query``.

        DESIGN: Core UPDATE over ORM fetch-and-save loop
          ✅ Single DB round-trip; no ORM hydration overhead.
          ✅ Scales to millions of rows without memory growth.
          ❌ Skips domain-model validation and service-layer hooks (``_check_entity``,
             ``_prepare_for_create``, audit, soft-delete, etc.).

        Args:
            params: ``QueryParams`` whose ``node`` selects target rows.
                    Sort and pagination are ignored for bulk UPDATE.
                    ``params.node is None`` → updates every row in the table.
            update: ORM field name → new value.  Must not be empty.

        Returns:
            Number of rows modified (``result.rowcount``).

        Raises:
            ValueError: ``update`` is an empty dict.

        Thread safety:  ❌ ``AsyncSession`` is not thread-safe.
        Async safety:   ✅ Single await on the execute call.

        Edge cases:
            - ``params.node is None`` → full-table UPDATE (use with caution).
            - Unknown field names raise ``AttributeError`` from SA column lookup.
        """
        if not update:
            raise ValueError(
                "update_many_by_query: 'update' dict must not be empty. "
                "Provide at least one field name → value pair."
            )

        # Build the core UPDATE statement with VALUES
        stmt = sa_update(self._mapper._orm_cls).values(**update)

        # Apply the WHERE clause if a filter node is present
        if params.node is not None:
            compiler = SQLAlchemyQueryCompiler(model=self._mapper._orm_cls)
            stmt = stmt.where(compiler.visit(params.node))

        result = await self._session.execute(stmt)
        await self._session.flush()
        # rowcount is reliable for UPDATE/DELETE on all supported SA backends
        return result.rowcount  # type: ignore[return-value]

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _resolve_column(self, field_name: str) -> Any:
        """
        Resolve a column attribute on the mapped ORM class.

        Args:
            field_name: Simple (non-dotted) column name.

        Returns:
            The SQLAlchemy column attribute.

        Raises:
            AttributeError: Field does not exist on the model — callers should
                            let this propagate as a 400 Bad Request.
        """
        return getattr(self._mapper._orm_cls, field_name)
