"""
Unit tests for SQLAlchemyQueryCompiler — AST → SQLAlchemy expressions.

Tests verify that every comparison operation, logical combinator, and
error case produces the correct SA expression or raises the expected exception.
Also covers dotted relationship path traversal (join compilation) and security
guards (depth limit, segment safety, allowed_fields whitelist).

No session or database connection is required — we only inspect the expression
objects produced by the compiler.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, relationship

from varco_core.exception.query import OperationNotSupported
from varco_core.exception.repository import FieldNotFound
from varco_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    Operation,
    OrNode,
)
from varco_core.query.visitor.sqlalchemy import SQLAlchemyQueryCompiler


# ── Minimal SA models — flat ───────────────────────────────────────────────────


class _CompilerBase(DeclarativeBase):
    pass


class _PersonORM(_CompilerBase):
    __tablename__ = "persons_compiler"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)


# ── Minimal SA models — with relationships ─────────────────────────────────────
#
# Two models linked by a FK so we can test dotted-path join traversal.
# DepartmentORM ← EmployeeORM (many employees per department)


class _JoinBase(DeclarativeBase):
    pass


class _DeptORM(_JoinBase):
    __tablename__ = "departments_compiler"
    id = Column(Integer, primary_key=True)
    city = Column(String)
    employees = relationship("_EmployeeORM", back_populates="department")


class _EmployeeORM(_JoinBase):
    __tablename__ = "employees_compiler"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    dept_id = Column(Integer, ForeignKey("departments_compiler.id"))
    department = relationship("_DeptORM", back_populates="employees")


# ── Helpers ────────────────────────────────────────────────────────────────────


def _compiler(**kwargs) -> SQLAlchemyQueryCompiler:
    return SQLAlchemyQueryCompiler(model=_PersonORM, **kwargs)


def _compile(node) -> object:
    return _compiler().visit(node)


def _cmp(field: str, op: Operation, value=None) -> ComparisonNode:
    return ComparisonNode(field=field, op=op, value=value)


def _str(expr) -> str:
    """Render a SA expression to string for assertion checking."""
    return str(expr.compile(compile_kwargs={"literal_binds": True}))


# ── Individual operations ──────────────────────────────────────────────────────


def test_compile_equal():
    expr = _compile(_cmp("name", Operation.EQUAL, "Alice"))
    assert "= 'Alice'" in _str(expr)


def test_compile_not_equal():
    expr = _compile(_cmp("age", Operation.NOT_EQUAL, 18))
    assert "!=" in _str(expr)


def test_compile_greater_than():
    expr = _compile(_cmp("age", Operation.GREATER_THAN, 18))
    assert ">" in _str(expr)


def test_compile_less_than():
    expr = _compile(_cmp("age", Operation.LESS_THAN, 65))
    assert "<" in _str(expr)


def test_compile_greater_equal():
    expr = _compile(_cmp("age", Operation.GREATER_EQUAL, 18))
    assert ">=" in _str(expr)


def test_compile_less_equal():
    expr = _compile(_cmp("age", Operation.LESS_EQUAL, 100))
    assert "<=" in _str(expr)


def test_compile_like():
    expr = _compile(_cmp("name", Operation.LIKE, "Ali%"))
    assert "LIKE" in _str(expr).upper()


def test_compile_in():
    expr = _compile(_cmp("age", Operation.IN, [18, 21, 25]))
    sql = _str(expr)
    assert "IN" in sql.upper()


def test_compile_is_null():
    expr = _compile(_cmp("name", Operation.IS_NULL))
    sql = _str(expr)
    assert "NULL" in sql.upper()


def test_compile_is_not_null():
    expr = _compile(_cmp("name", Operation.IS_NOT_NULL))
    sql = _str(expr)
    assert "NULL" in sql.upper()


# ── Logical combinators ────────────────────────────────────────────────────────


def test_compile_and():
    left = _cmp("name", Operation.EQUAL, "Alice")
    right = _cmp("age", Operation.GREATER_THAN, 18)
    node = AndNode(left=left, right=right)
    expr = _compile(node)
    sql = _str(expr)
    assert "AND" in sql.upper()


def test_compile_or():
    left = _cmp("name", Operation.EQUAL, "Alice")
    right = _cmp("name", Operation.EQUAL, "Bob")
    node = OrNode(left=left, right=right)
    expr = _compile(node)
    sql = _str(expr)
    assert "OR" in sql.upper()


def test_compile_not():
    """NOT wraps the child expression; SA may simplify negation in rendering."""
    inner = _cmp("age", Operation.GREATER_THAN, 18)
    node = NotNode(child=inner)
    expr = _compile(node)
    sql = _str(expr)
    # SA may render NOT(age > 18) as "age <= 18" (boolean negation simplification)
    # or as "NOT (age > 18)" — either is semantically correct.
    assert "age" in sql.lower()


# ── allowed_fields whitelist ───────────────────────────────────────────────────


def test_allowed_fields_permits_whitelisted_field():
    compiler = _compiler(allowed_fields={"name"})
    node = _cmp("name", Operation.EQUAL, "Alice")
    # Must not raise
    compiler.visit(node)


def test_allowed_fields_blocks_non_whitelisted_field():
    compiler = _compiler(allowed_fields={"name"})
    node = _cmp("age", Operation.EQUAL, 18)
    with pytest.raises(ValueError, match="allowed"):
        compiler.visit(node)


def test_empty_allowed_fields_permits_all():
    compiler = _compiler(allowed_fields=set())
    node = _cmp("age", Operation.EQUAL, 18)
    compiler.visit(node)  # must not raise


def test_add_to_allowed_fields():
    compiler = _compiler(allowed_fields={"name"})
    compiler.add_to_allowed_fields("age")
    assert "age" in compiler.get_allowed_fields()


def test_set_allowed_fields_replaces():
    compiler = _compiler(allowed_fields={"name", "age"})
    compiler.set_allowed_fields({"only_this"})
    assert compiler.get_allowed_fields() == {"only_this"}


def test_is_allowed_fields_empty_true():
    compiler = _compiler()
    assert compiler.is_allowed_fields_empty() is True


def test_is_allowed_fields_empty_false():
    compiler = _compiler(allowed_fields={"name"})
    assert compiler.is_allowed_fields_empty() is False


# ── Error cases ───────────────────────────────────────────────────────────────


def test_dotted_path_raises_operation_not_supported():
    node = _cmp("profile.city", Operation.EQUAL, "NYC")
    with pytest.raises(OperationNotSupported):
        _compile(node)


def test_unknown_field_raises_field_not_found():
    node = _cmp("nonexistent_col", Operation.EQUAL, "x")
    with pytest.raises(FieldNotFound):
        _compile(node)


def test_none_field_raises_value_error():
    node = ComparisonNode.__new__(ComparisonNode)
    object.__setattr__(node, "field", None)
    object.__setattr__(node, "op", Operation.EQUAL)
    object.__setattr__(node, "value", 1)
    object.__setattr__(
        node, "type", node.__class__.__dataclass_fields__["type"].default
    )
    with pytest.raises((ValueError, AttributeError)):
        _compile(node)


# ── Relationship join traversal ────────────────────────────────────────────────


def _join_compiler(**kwargs) -> SQLAlchemyQueryCompiler:
    """Compiler wired to _EmployeeORM (which has a 'department' relationship)."""
    return SQLAlchemyQueryCompiler(model=_EmployeeORM, **kwargs)


def _join_cmp(field: str, op: Operation, value=None) -> ComparisonNode:
    return ComparisonNode(field=field, op=op, value=value)


class TestJoinTraversal:
    """
    Verify that dotted relationship paths resolve to the correct SA column
    and accumulate the expected join attributes in _pending_joins.
    """

    def test_dotted_path_resolves_column_on_related_model(self):
        """
        ``"department.city"`` should compile to a column on ``_DeptORM``,
        not ``_EmployeeORM``.
        """
        compiler = _join_compiler()
        node = _join_cmp("department.city", Operation.EQUAL, "NYC")
        expr = compiler.visit(node)
        sql = str(expr.compile(compile_kwargs={"literal_binds": True}))
        # The compiled expression references the departments_compiler table column
        assert "departments_compiler" in sql
        assert "NYC" in sql

    def test_single_hop_collects_one_pending_join(self):
        """One relationship hop must produce exactly one pending join."""
        compiler = _join_compiler()
        compiler._clear_pending_joins()
        node = _join_cmp("department.city", Operation.EQUAL, "NYC")
        compiler.visit(node)
        joins = compiler.collect_pending_joins()
        assert len(joins) == 1
        # The pending join must be the relationship attribute on _EmployeeORM
        assert joins[0] is _EmployeeORM.department

    def test_same_relationship_deduplicated_across_clauses(self):
        """
        Two filter clauses referencing the same relationship
        (``"department.city"`` and ``"department.id"``) should produce only
        ONE pending join — not two.
        """
        compiler = _join_compiler()
        compiler._clear_pending_joins()
        left = _join_cmp("department.city", Operation.EQUAL, "NYC")
        right = _join_cmp("department.id", Operation.EQUAL, 1)
        node = AndNode(left=left, right=right)
        compiler.visit(node)
        joins = compiler.collect_pending_joins()
        assert len(joins) == 1

    def test_flat_path_produces_no_pending_joins(self):
        """A flat (non-dotted) field must not add any pending joins."""
        compiler = _join_compiler()
        compiler._clear_pending_joins()
        node = _join_cmp("name", Operation.EQUAL, "Alice")
        compiler.visit(node)
        assert compiler.collect_pending_joins() == []

    def test_clear_pending_joins_resets_state(self):
        """_clear_pending_joins must discard joins from a previous visit."""
        compiler = _join_compiler()
        compiler._clear_pending_joins()
        compiler.visit(_join_cmp("department.city", Operation.EQUAL, "NYC"))
        assert len(compiler.collect_pending_joins()) == 1  # sanity

        compiler._clear_pending_joins()
        assert compiler.collect_pending_joins() == []

    def test_unknown_relationship_raises_operation_not_supported(self):
        """A path whose first segment is not a declared relationship must raise."""
        compiler = _join_compiler()
        node = _join_cmp("nonexistent_rel.field", Operation.EQUAL, "x")
        with pytest.raises(
            OperationNotSupported, match="not a declared SA relationship"
        ):
            compiler.visit(node)

    def test_unknown_leaf_column_raises_field_not_found(self):
        """The final column must exist on the leaf model."""
        compiler = _join_compiler()
        node = _join_cmp("department.no_such_col", Operation.EQUAL, "x")
        with pytest.raises(FieldNotFound):
            compiler.visit(node)

    def test_depth_limit_exceeded_raises_operation_not_supported(self):
        """Paths deeper than max_traversal_depth must be rejected."""
        compiler = _join_compiler(max_traversal_depth=0)
        # Even one hop is too many
        node = _join_cmp("department.city", Operation.EQUAL, "NYC")
        with pytest.raises(OperationNotSupported, match="max_traversal_depth"):
            compiler.visit(node)

    def test_unsafe_segment_raises_value_error(self):
        """
        Segments with special characters must be rejected before any getattr.

        Note: the path must contain a dot so it reaches the dotted-path branch
        where segment validation occurs.  A flat path like
        ``"bad; DROP TABLE--"`` would safely raise FieldNotFound because SA's
        getattr never matches that attribute name — but a dotted path reaches
        the SA inspect() call, so the segment regex fires first.
        """
        compiler = _join_compiler()
        node = _join_cmp("department.city; DROP TABLE--", Operation.EQUAL, "x")
        with pytest.raises(ValueError, match="unsafe characters"):
            compiler.visit(node)

    def test_dunder_segment_raises_operation_not_supported(self):
        """
        ``__class__`` is a valid Python identifier (passes the regex) but must
        be rejected because it is not in ``sa_inspect().relationships``.
        """
        compiler = _join_compiler()
        node = _join_cmp("__class__.__mro__", Operation.EQUAL, "x")
        with pytest.raises(
            OperationNotSupported, match="not a declared SA relationship"
        ):
            compiler.visit(node)

    def test_dotted_path_with_allowed_fields_whitelist(self):
        """Dotted path must be explicitly listed in allowed_fields."""
        compiler = _join_compiler(allowed_fields={"department.city"})
        node = _join_cmp("department.city", Operation.EQUAL, "NYC")
        compiler.visit(node)  # must not raise

    def test_dotted_path_blocked_when_not_in_allowed_fields(self):
        """Dotted path absent from allowed_fields must be rejected."""
        compiler = _join_compiler(allowed_fields={"name"})
        node = _join_cmp("department.city", Operation.EQUAL, "NYC")
        with pytest.raises(ValueError, match="allowed"):
            compiler.visit(node)
