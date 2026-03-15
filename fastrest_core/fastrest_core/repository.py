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
from typing import TYPE_CHECKING, Any, Generic, TypeVar

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
