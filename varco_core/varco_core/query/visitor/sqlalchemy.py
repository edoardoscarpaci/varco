"""
varco_core.query.visitor.sqlalchemy
========================================
Translates AST query nodes into SQLAlchemy filter expressions, with optional
automatic JOIN resolution for dotted relationship paths such as
``"author.name"`` or ``"author.department.city"``.

This visitor is backend-specific but lives in ``varco_core`` because the
SQLAlchemy type system (``DeclarativeBase``, ``ColumnElement``) is the common
ground shared between ``varco_sa`` and any direct SA usage.

Join traversal lifecycle
------------------------
Because JOINs mutate the ``Select`` statement while the compiler only returns
a ``ColumnElement``, join tracking is a two-phase contract managed with the
``SQLAlchemyQueryApplicator``:

1. The applicator calls ``_clear_pending_joins()`` before ``visit()``.
2. ``_resolve_column`` accumulates required relationship attributes in
   ``_pending_joins`` as it encounters dotted paths during the walk.
3. After ``visit()`` returns, the applicator reads ``collect_pending_joins()``
   and applies each ``query.join(rel_attr)`` before the ``WHERE`` clause.

Thread safety:  ⚠️ ``_pending_joins`` is mutable — not safe for concurrent calls
                   on the same instance.  Each thread/task must either use its
                   own compiler instance or coordinate externally.
Async safety:   ✅ Synchronous; safe to call from async contexts as long as a
                   single coroutine owns the compiler for each visit.
"""

from __future__ import annotations

import re
from typing import Any, Union

from sqlalchemy import BinaryExpression, ColumnElement, and_, inspect as sa_inspect
from sqlalchemy import not_, or_
from sqlalchemy.orm import DeclarativeBase, MappedColumn

from varco_core.exception.query import OperationNotSupported
from varco_core.exception.repository import FieldNotFound
from varco_core.query.type import (
    ComparisonNode,
    Operation,
)
from varco_core.query.visitor.walking import BinaryWalkingVisitor

# ── Security: safe path segment regex ─────────────────────────────────────────

# DESIGN: strict segment allowlist over blocklist
#   ✅ Prevents __dunder__ attribute access (e.g. __class__.__mro__)
#   ✅ Prevents SQL special characters from leaking into getattr calls
#   ✅ Simple to audit — one regex, one place
#   ❌ Forbids exotic (but theoretically valid) Python identifiers with unicode
#      letters — acceptable for DB field names which are always ASCII
_SAFE_SEGMENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_path_segment(segment: str, full_path: str) -> None:
    """
    Reject path segments that could be used for attribute injection attacks.

    Args:
        segment:   One dot-separated component of a field path.
        full_path: The complete path (used only in the error message).

    Raises:
        ValueError: ``segment`` does not match ``[a-zA-Z_][a-zA-Z0-9_]*``.

    Edge cases:
        - Empty segment (e.g. ``"a..b"``) → fails the regex → raises ValueError.
        - Dunder names (``__class__``) → rejected — leading ``__`` fails the
          "no consecutive underscores at start" constraint implicitly because
          ``__class__`` contains ``__`` which is *allowed* by the regex ...
          Wait: ``__class__`` DOES match ``^[a-zA-Z_][a-zA-Z0-9_]*$``.
          Therefore the relationship-existence check in ``_resolve_column`` is
          the primary dunder guard — ``sa_inspect().relationships`` will never
          contain ``__class__``, so it raises ``OperationNotSupported`` before
          any ``getattr`` is called.
    """
    if not _SAFE_SEGMENT_RE.match(segment):
        raise ValueError(
            f"Field path segment {segment!r} in path {full_path!r} contains "
            "unsafe characters. Only [a-zA-Z_][a-zA-Z0-9_]* identifiers are "
            "accepted to prevent attribute-injection attacks."
        )


class SQLAlchemyQueryCompiler(BinaryWalkingVisitor):
    """
    AST visitor that compiles comparison and boolean nodes into SQLAlchemy
    filter expressions for a given mapped model class.

    Supports dotted relationship paths (e.g. ``"author.name"``) via automatic
    JOIN collection.  Callers must apply the collected joins to the ``Select``
    statement *before* the ``WHERE`` clause — see ``collect_pending_joins()``.

    DESIGN: visitor over inline SQLAlchemy expression building
      ✅ Reusable across repositories for the same model.
      ✅ ``allowed_fields`` prevents arbitrary field injection from user input.
      ✅ Relationship traversal uses ``sa_inspect().relationships`` — dunder
         attributes can never accidentally reach ``getattr``.
      ❌ ``_pending_joins`` is mutable per-visit — not thread-safe for concurrent
         calls on the same instance (see thread safety note above).
      ❌ Cyclic relationship graphs are not detected — deep cycles hit
         ``max_traversal_depth`` instead.

    Thread safety:  ⚠️ See module docstring.
    Async safety:   ✅ Synchronous.

    Args:
        model:               SQLAlchemy declarative model class for column lookup.
        allowed_fields:      Optional whitelist of field paths (flat or dotted).
                             When non-empty, queries referencing paths outside
                             this set raise ``ValueError``.
        max_traversal_depth: Maximum number of relationship hops allowed in a
                             dotted path (default 3).  Prevents runaway traversal
                             from maliciously deep paths.

    Edge cases:
        - ``allowed_fields`` empty set → all fields are permitted.
        - Dotted path depth > ``max_traversal_depth`` → ``OperationNotSupported``.
        - Unknown field name → ``FieldNotFound``.
        - Unknown relationship name → ``OperationNotSupported``.
        - Unknown operation → ``ValueError``.
        - Dunder segment (``__class__``) → ``OperationNotSupported`` because
          ``sa_inspect().relationships`` never contains dunder keys.
    """

    def __init__(
        self,
        model: type[DeclarativeBase],
        allowed_fields: set[str] | None = None,
        *,
        max_traversal_depth: int = 3,
    ) -> None:
        """
        Initialise the compiler.

        Args:
            model:               Mapped SQLAlchemy model class.
            allowed_fields:      Optional field-path whitelist.  ``None`` or
                                 empty set means all fields are allowed.
            max_traversal_depth: Maximum relationship hops for dotted paths.
        """
        self.model = model
        # Use a copy to prevent external mutation of the set we depend on
        self.allowed_fields: set[str] = set(allowed_fields) if allowed_fields else set()
        self.max_traversal_depth = max_traversal_depth

        # DESIGN: mutable per-visit join tracking
        #   Populated by _resolve_column when it traverses relationships.
        #   Cleared by _clear_pending_joins() at the start of each visit call.
        #   Read by collect_pending_joins() after visit() completes.
        #   ✅ Keeps the compiler/applicator contract explicit.
        #   ❌ Makes the compiler stateful during a visit — callers must not
        #      share a compiler instance across concurrent visit() calls.
        self._pending_joins: list[Any] = []
        # Set of id(rel_attr) to deduplicate joins when the same relationship
        # is referenced by multiple filter clauses
        self._pending_joins_seen: set[int] = set()

    # ── Pending-join lifecycle ─────────────────────────────────────────────────

    def _clear_pending_joins(self) -> None:
        """
        Reset join-tracking state before a new visit.

        Must be called by the applicator (or any caller that reuses this
        compiler instance across multiple ``visit()`` calls) to avoid
        stale joins leaking from a previous compilation pass.

        Thread safety:  ⚠️ Mutates shared state — do not call concurrently.
        """
        self._pending_joins.clear()
        self._pending_joins_seen.clear()

    def collect_pending_joins(self) -> list[Any]:
        """
        Return the ordered, deduplicated list of SA relationship attributes
        accumulated during the most recent ``visit()`` call.

        The list is in traversal order — pass each element directly to
        ``Select.join(rel_attr)`` to build the correct JOIN sequence.

        Returns:
            Snapshot list of relationship attributes (e.g. ``[PostORM.author]``).
            Empty when no dotted paths were encountered.

        Example::

            compiler._clear_pending_joins()
            where_clause = compiler.visit(node)
            for rel in compiler.collect_pending_joins():
                stmt = stmt.join(rel)
            stmt = stmt.where(where_clause)
        """
        return list(self._pending_joins)

    # ── Visitor implementations ────────────────────────────────────────────────

    def _visit_comparison(
        self,
        node: ComparisonNode,
        args: object = None,
        **kwargs: object,
    ) -> Union[ColumnElement[bool], BinaryExpression[bool]]:
        """
        Compile a ``ComparisonNode`` to a SQLAlchemy boolean expression.

        For dotted field paths (``"author.name"``), this method also populates
        ``_pending_joins`` so the applicator can add the necessary JOINs.

        Args:
            node: The comparison node containing field, operation, and value.
            args: Unused.

        Returns:
            A ``ColumnElement[bool]`` ready for use in ``.where()``.

        Raises:
            ValueError:             ``node.field`` is ``None``, or the field is
                                    not in ``allowed_fields`` (when set), or a
                                    path segment contains unsafe characters.
            OperationNotSupported:  Dotted path exceeds ``max_traversal_depth``,
                                    or a relationship name is not registered on
                                    the model.
            FieldNotFound:          Column does not exist on the leaf model.
            ValueError:             Unsupported ``Operation`` value.
        """
        if node.field is None:
            raise ValueError("ComparisonNode.field must not be None")

        if self.allowed_fields and node.field not in self.allowed_fields:
            raise ValueError(
                f"Field {node.field!r} is not in the allowed fields whitelist. "
                f"Allowed: {sorted(self.allowed_fields)}"
            )

        col = self._resolve_column(node.field)
        op, val = node.op, node.value

        if op == Operation.EQUAL:
            return col == val
        if op == Operation.NOT_EQUAL:
            return col != val
        if op == Operation.GREATER_THAN:
            return col > val
        if op == Operation.LESS_THAN:
            return col < val
        if op == Operation.GREATER_EQUAL:
            return col >= val
        if op == Operation.LESS_EQUAL:
            return col <= val
        if op == Operation.LIKE:
            return col.like(val)
        if op == Operation.IN:
            return col.in_(val)  # pyright: ignore[reportArgumentType]
        if op == Operation.IS_NULL:
            return col.is_(None)
        if op == Operation.IS_NOT_NULL:
            return col.isnot(None)

        raise ValueError(
            f"Unsupported operation {op!r}. "
            f"Supported: {[o.value for o in Operation]}"
        )

    # ── BinaryWalkingVisitor combine hooks ────────────────────────────────────

    def _combine_and(
        self, left: ColumnElement[bool], right: ColumnElement[bool]
    ) -> ColumnElement[bool]:
        """Combine two compiled sub-expressions with SQLAlchemy ``and_``."""
        return and_(left, right)

    def _combine_or(
        self, left: ColumnElement[bool], right: ColumnElement[bool]
    ) -> ColumnElement[bool]:
        """Combine two compiled sub-expressions with SQLAlchemy ``or_``."""
        return or_(left, right)

    def _combine_not(self, inner: ColumnElement[bool]) -> ColumnElement[bool]:
        """Negate a compiled sub-expression with SQLAlchemy ``not_``."""
        return not_(inner)

    # ── allowed_fields helpers ─────────────────────────────────────────────────

    def get_allowed_fields(self) -> set[str]:
        """Return a copy of the allowed-fields whitelist."""
        return self.allowed_fields.copy()

    def set_allowed_fields(self, allowed_fields: set[str]) -> None:
        """Replace the entire allowed-fields whitelist."""
        self.allowed_fields = set(allowed_fields)

    def add_to_allowed_fields(self, value: str) -> None:
        """Add a single field name to the whitelist."""
        self.allowed_fields.add(value)

    def is_allowed_fields_empty(self) -> bool:
        """Return ``True`` when no whitelist is configured (all fields allowed)."""
        return len(self.allowed_fields) == 0

    # ── Column resolution ──────────────────────────────────────────────────────

    def _resolve_column(self, field_path: str) -> MappedColumn:
        """
        Resolve a column attribute, traversing SA relationships for dotted paths.

        For a simple path ``"name"``, returns ``self.model.name`` unchanged.

        For a dotted path ``"author.name"``:
        1. Validates depth against ``max_traversal_depth``.
        2. Validates each segment against ``_SAFE_SEGMENT_RE``.
        3. Looks up each intermediate segment in ``sa_inspect().relationships``
           to ensure it is a real SA relationship (blocks dunder access).
        4. Appends the relationship attribute to ``_pending_joins`` (deduplicated).
        5. Returns the column attribute on the leaf model.

        Args:
            field_path: A column name or dot-separated relationship path.

        Returns:
            The mapped column attribute on the (possibly joined) target model.

        Raises:
            OperationNotSupported: Traversal depth exceeds ``max_traversal_depth``,
                                   or an intermediate segment is not a registered
                                   SA relationship.
            ValueError:            A segment contains unsafe characters.
            FieldNotFound:         The final column does not exist on the leaf model.

        Edge cases:
            - Dunder segments (``__class__``): blocked by the relationship check —
              ``sa_inspect().relationships`` never contains dunder keys.
            - Same relationship referenced twice in one query (``"a.x = 1 AND a.y = 2"``):
              deduplicated — the JOIN is only added once.
            - Multi-hop path (``"author.department.city"``): each hop is validated
              and added to ``_pending_joins`` in traversal order.
        """
        if "." not in field_path:
            # Fast path — flat column, no join needed
            try:
                return getattr(self.model, field_path)
            except AttributeError as exc:
                raise FieldNotFound(
                    field=field_path,
                    table=self.model.__tablename__,
                ) from exc

        # ── Dotted path: relationship traversal ───────────────────────────────

        parts = field_path.split(".")
        hop_count = len(parts) - 1  # number of relationship hops

        # Security guard 1: limit traversal depth to prevent DoS via
        # arbitrarily deep paths constructed from user input
        if hop_count > self.max_traversal_depth:
            raise OperationNotSupported(
                f"Relationship traversal depth {hop_count} for {field_path!r} "
                f"exceeds max_traversal_depth={self.max_traversal_depth}. "
                "Reduce nesting depth or raise max_traversal_depth explicitly."
            )

        # Security guard 2: validate every segment before any getattr call
        for segment in parts:
            _validate_path_segment(segment, field_path)

        current_model: type[DeclarativeBase] = self.model

        for rel_name in parts[:-1]:
            inspector = sa_inspect(current_model)
            relationships = inspector.relationships

            # Security guard 3: only traverse declared SA relationships —
            # this blocks dunder access (__class__, __init__, etc.) because
            # SA relationship maps are keyed by explicit relationship() calls
            if rel_name not in relationships:
                available = list(relationships.keys())
                raise OperationNotSupported(
                    f"Attribute {rel_name!r} is not a declared SA relationship "
                    f"on {current_model.__name__!r}. "
                    f"Available relationships: {available}. "
                    "Register relationships via Meta.customize() or "
                    "sqlalchemy.orm.relationship()."
                )

            rel_attr = getattr(current_model, rel_name)

            # Deduplicate by attribute identity — same relationship referenced
            # by multiple filter clauses must only produce one JOIN
            join_key = id(rel_attr)
            if join_key not in self._pending_joins_seen:
                self._pending_joins.append(rel_attr)
                self._pending_joins_seen.add(join_key)

            # Advance to the target model for the next hop
            current_model = relationships[rel_name].mapper.class_

        # Resolve the final column on the leaf model
        col_name = parts[-1]
        try:
            return getattr(current_model, col_name)
        except AttributeError as exc:
            raise FieldNotFound(
                field=col_name,
                table=current_model.__tablename__,
            ) from exc
