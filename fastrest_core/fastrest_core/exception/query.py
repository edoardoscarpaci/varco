"""
fastrest_core.exception.query
================================
Exceptions raised by the query system (AST building, visitor dispatch,
type coercion, and compilation).

All exceptions inherit from ``QueryException`` so callers can catch the
entire family with a single ``except QueryException`` clause.

Thread safety:  ✅ Exception objects are immutable after construction.
Async safety:   ✅ Safe to raise and catch in async contexts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Imported only for type annotations in exception constructors
    from fastrest_core.query.type import TransformerNode


class QueryException(Exception):
    """Base class for all query-system exceptions."""


class OperationNotFound(QueryException):
    """
    Raised when a query string contains an operator not in ``Operation``.

    Attributes:
        op: The unrecognised operator string.

    Example::

        raise OperationNotFound("~~")  # "Operation '~~' not found"
    """

    def __init__(self, op: str) -> None:
        """
        Args:
            op: The unrecognised operator string from the query grammar.
        """
        self.op = op
        super().__init__(
            f"Operation {op!r} not found. "
            "Check the query grammar for supported operators."
        )


class OperationNotSupported(QueryException):
    """
    Raised when a valid operation cannot be executed by the current backend.

    For example, nested relationship traversal (``"profile.city"``) is not
    supported by the SQLAlchemy compiler's ``_resolve_column`` helper.

    Attributes:
        detail: Human-readable description of what is not supported.
    """

    def __init__(self, detail: str) -> None:
        """
        Args:
            detail: What is not supported and why.
        """
        self.detail = detail
        super().__init__(f"{detail} is not supported by this backend.")


class WrongNodeVisited(QueryException):
    """
    Raised by ``ASTVisitor`` when a node is dispatched to the wrong handler.

    This indicates a programming error — the visitor received a node type
    it did not expect.

    Attributes:
        received_node_cls: The class of the node that was actually received.
        expected_node_cls: The class of the node that was expected.
    """

    def __init__(
        self,
        received_node_cls: type[TransformerNode],
        expected_node_cls: type[TransformerNode],
    ) -> None:
        """
        Args:
            received_node_cls: Actual node class received by the visitor.
            expected_node_cls: Node class the visitor expected.
        """
        self.received_node_cls = received_node_cls
        self.expected_node_cls = expected_node_cls
        super().__init__(
            f"Visitor received {received_node_cls.__name__!r} "
            f"but expected {expected_node_cls.__name__!r}. "
            "This is a programming error — check the dispatcher logic."
        )


class CoercionError(QueryException):
    """
    Raised when type coercion of a query value fails.

    Wraps the original coercion exception as the ``__cause__`` so the
    full context is preserved in tracebacks.

    Example::

        raise CoercionError("Failed to coerce 'abc' to int") from ValueError(...)
    """
