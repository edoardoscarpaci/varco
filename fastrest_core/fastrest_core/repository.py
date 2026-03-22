"""
fastrest_core.repository
=========================
Backend-neutral async CRUD + query interface.

Application code depends **only** on this ABC — never on the concrete
SQLAlchemy or Beanie subclass.  This keeps domain logic portable across
storage backends.

Thread safety:  ⚠️ Delegates to the underlying ORM session/connection.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, AsyncIterator, Generic, TypeVar

from fastrest_core.model import DomainModel

if TYPE_CHECKING:
    # Imported only for type hints — avoids importing query machinery at runtime
    # for callers that never use the query system.
    from fastrest_core.query.params import QueryParams

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")


class AsyncRepository(ABC, Generic[D, PK]):
    """
    Abstract async CRUD + query repository for a single domain entity type.

    DESIGN: one ABC for both CRUD and query
      ✅ Callers get a single injection point — no need for a separate
         ``QueryRepository`` interface to compose.
      ✅ ``find_by_query`` has a default implementation that raises
         ``NotImplementedError`` — backends that don't support the query
         system can still implement the basic CRUD methods.
      ❌ A backend that only supports CRUD must still declare the abstract
         query methods (but they can raise ``NotImplementedError``).

    Thread safety:  ⚠️ Session-level thread safety depends on the backend.
    Async safety:   ✅ All methods are ``async def``.

    Edge cases:
        - ``find_by_id`` with a non-existent PK → ``None`` (never raises).
        - ``save`` with ``entity._raw_orm is None`` → INSERT.
        - ``save`` with ``entity._raw_orm is not None`` → UPDATE.
        - ``delete`` on an unsaved entity → raises ``ValueError``.
        - ``find_all`` has no pagination — avoid on large tables; prefer
          ``find_by_query`` with a ``QueryParams(limit=N)``.
        - ``find_by_query(QueryParams())`` is equivalent to ``find_all()``.
        - ``count(QueryParams())`` returns the total row count.
    """

    def __class_getitem__(cls, params: Any) -> Any:
        if not isinstance(params, tuple):
            params = (params, Any)
        return super().__class_getitem__(params)

    # ── Basic CRUD ─────────────────────────────────────────────────────────────

    @abstractmethod
    async def find_by_id(self, pk: PK) -> D | None:
        """
        Retrieve a single entity by primary key.

        Args:
            pk: Primary key value.  For composite PKs pass a tuple.

        Returns:
            The domain entity, or ``None`` if no record with that PK exists.
        """

    @abstractmethod
    async def find_all(self) -> list[D]:
        """
        Retrieve all entities of this type with no filtering or pagination.

        Returns:
            List of domain entities.  Empty list when none exist.

        Edge cases:
            - Avoid on large tables — use ``find_by_query`` with pagination.
        """

    @abstractmethod
    async def save(self, entity: D) -> D:
        """
        Persist the entity (INSERT or UPDATE).

        INSERT/UPDATE detection is based on ``entity._raw_orm``:
        - ``_raw_orm is None`` → INSERT
        - ``_raw_orm is not None`` → UPDATE

        Args:
            entity: Domain entity to persist.

        Returns:
            Fresh domain entity with ``pk`` and ``_raw_orm`` populated.
            Always use the returned value — the input is never mutated.

        Raises:
            LookupError: UPDATE path — PK not found in the database.
        """

    @abstractmethod
    async def delete(self, entity: D) -> None:
        """
        Delete the entity from the backing store.

        Args:
            entity: The domain entity to delete.

        Raises:
            ValueError: Entity has not been persisted yet (no PK).
        """

    # ── Query + pagination ─────────────────────────────────────────────────────

    @abstractmethod
    async def find_by_query(self, params: QueryParams) -> list[D]:
        """
        Execute a filtered, sorted, paginated query and return matching entities.

        Args:
            params: ``QueryParams`` bundle containing:
                    - ``node``   — optional AST filter (``None`` = no filter).
                    - ``sort``   — ordered sort directives (empty = default order).
                    - ``limit``  — max results (``None`` = no limit).
                    - ``offset`` — rows to skip (``None`` = start from first).

        Returns:
            List of matching domain entities.  Empty list when nothing matches.

        Edge cases:
            - ``QueryParams()`` (all defaults) → equivalent to ``find_all()``.
            - ``params.node is None`` and no sort/pagination → full table scan.
            - ``params.limit=0`` → empty list.
        """

    @abstractmethod
    async def count(self, params: QueryParams | None = None) -> int:
        """
        Count entities matching the optional filter in ``params``.

        Args:
            params: Optional ``QueryParams``.  When ``None`` or
                    ``params.node is None``, counts all rows.

        Returns:
            Total number of matching rows/documents.

        Edge cases:
            - ``count(None)`` → total row count (equivalent to ``SELECT COUNT(*)``.
            - Sort and pagination in ``params`` are intentionally ignored —
              they have no effect on a count.
        """

    @abstractmethod
    async def exists(self, pk: PK) -> bool:
        """
        Return ``True`` if an entity with ``pk`` exists in the backing store.

        Intended to be cheaper than ``find_by_id`` — implementations should
        avoid loading the full ORM object when the backing store supports a
        lightweight existence check (e.g. ``SELECT COUNT(*) WHERE pk = ?``).

        Args:
            pk: Primary key value.  For composite PKs pass a tuple.

        Returns:
            ``True`` if a record with that PK exists; ``False`` otherwise.

        Edge cases:
            - Soft-deleted records ARE counted as existing at this layer —
              the service layer's ``_check_entity`` hook is responsible for
              filtering soft-deleted entities from the caller's perspective.
            - For composite PKs the caller must pass a tuple in the same
              field order as the mapper's ``_pk_orm_attrs``.
        """

    @abstractmethod
    def stream_by_query(self, params: QueryParams) -> AsyncIterator[D]:
        """
        Yield entities one at a time without loading all results into memory.

        Implementations must be ``async def`` functions with ``yield`` (async
        generators).  The abstract declaration uses a plain ``def`` returning
        ``AsyncIterator[D]`` so the abstract method contract is satisfied by
        any async generator — ``AsyncGenerator[D, None]`` is a subtype of
        ``AsyncIterator[D]``.

        DESIGN: plain ``def`` abstract over ``async def`` abstract
            Declaring this as ``async def`` without ``yield`` would make it a
            coroutine factory, not an async generator factory.  Subclasses
            that implement it as ``async def`` with ``yield`` would have a
            different call-site signature (iterator, not coroutine).  Keeping
            the abstract method as a plain ``def`` avoids this mismatch.
            ✅ Concrete classes implement as ``async def ... yield`` — correct.
            ✅ Callers iterate with ``async for entity in repo.stream_by_query(p)``.
            ❌ Mypy may warn about the async/sync override — suppress with
               ``# type: ignore[override]`` on the concrete method.

        Args:
            params: ``QueryParams`` with filter, sort, and (optional) pagination.
                    Pagination limits the total stream length — ``params.limit``
                    is still respected.

        Returns:
            An ``AsyncIterator[D]`` that yields domain entities one at a time.

        Edge cases:
            - The backing session / cursor must remain open for the duration
              of iteration.  The caller must fully iterate or call ``aclose()``
              on the iterator to release the DB cursor.
            - Abandoning the iterator without calling ``aclose()`` may leave a
              DB cursor open until GC runs — prefer ``async for`` or
              ``async with contextlib.aclosing(repo.stream_by_query(...)):``.
            - Sort and pagination in ``params`` are honoured — this is not a
              full-table stream by default; callers should set ``params.limit``
              if they only want a bounded stream.
        """
