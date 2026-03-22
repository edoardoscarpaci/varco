"""
varco_core.query.applicator.applicator
==========================================
Abstract base class for query applicators.

An applicator takes a backend-native query object (e.g. SQLAlchemy ``Select``,
Beanie ``FindMany``) and applies an AST node, sort fields, and pagination
parameters to it, returning the modified query object.

Thread safety:  ⚠️ Depends on the concrete implementation.
Async safety:   ✅ All methods are synchronous wrappers — actual execution is
                   deferred to the caller's async context.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from varco_core.query.type import SortField, TransformerNode

# Generic query-object type (Select, FindMany, etc.)
_T = TypeVar("_T")


class QueryApplicator(ABC):
    """
    Abstract strategy for applying an AST query, sort, and pagination to a
    backend-native query object.

    DESIGN: strategy pattern over inline application in the repository
      ✅ Applicator logic can be tested independently of repository plumbing.
      ✅ Swappable — a custom applicator can extend ``SQLAlchemyQueryApplicator``
         without changing the repository.
      ❌ Extra indirection for simple repositories that don't need swappability.

    Thread safety:  ⚠️ Delegate to subclass docs.
    Async safety:   ✅ All methods are synchronous.

    Args:
        allowed_fields: Optional whitelist of field names permitted in queries
                        and sort directives.  Empty set means no restriction.
    """

    def __init__(
        self, *args: Any, allowed_fields: set[str] | None = None, **kwargs: Any
    ) -> None:
        """
        Initialise the applicator.

        Args:
            allowed_fields: Field name whitelist.  ``None`` means unrestricted.
            args:           Passed to super (cooperative multiple inheritance).
            kwargs:         Passed to super.
        """
        # Store a copy so external mutation cannot affect us
        self.allowed_fields: set[str] = set(allowed_fields) if allowed_fields else set()

    @abstractmethod
    def apply_query(
        self, query: _T, node: TransformerNode, *args: Any, **kwargs: Any
    ) -> _T:
        """
        Apply an AST filter node to ``query`` and return the modified query.

        Args:
            query: Backend-native query object to filter.
            node:  Root AST node representing the filter expression.
            args:  Passed through to the backend.
            kwargs: Passed through to the backend.

        Returns:
            The modified query object (may be a new instance or mutated in-place,
            depending on the backend).
        """

    @abstractmethod
    def apply_pagination(
        self,
        query: _T,
        limit: int | None,
        offset: int | None,
        *args: Any,
        **kwargs: Any,
    ) -> _T:
        """
        Apply ``LIMIT`` and ``OFFSET`` to ``query`` and return it.

        Args:
            query:  Backend-native query object.
            limit:  Maximum rows/documents to return.  ``None`` → no limit.
            offset: Number of rows/documents to skip.  ``None`` → no skip.
            args:   Passed through.
            kwargs: Passed through.

        Returns:
            The modified query object.
        """

    @abstractmethod
    def apply_sort(
        self,
        query: _T,
        sort_fields: list[SortField] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> _T:
        """
        Apply one or more sort directives to ``query`` and return it.

        Args:
            query:       Backend-native query object.
            sort_fields: Ordered list of ``SortField`` directives.  ``None`` or
                         empty list → no sorting applied.
            args:        Passed through.
            kwargs:      Passed through.

        Returns:
            The modified query object.
        """
