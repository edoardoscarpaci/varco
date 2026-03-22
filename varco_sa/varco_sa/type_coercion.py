"""
varco_sa.type_coercion
==========================
SQLAlchemy-aware factory for ``TypeCoercionRegistry``.

Kept in ``varco_sa`` (not ``varco_core``) because it depends on
``sqlalchemy`` — which is not a ``varco_core`` dependency.
"""

from __future__ import annotations

from typing import Callable, Any

from sqlalchemy.orm import DeclarativeBase

from varco_core.query.visitor.type_coercion import (
    TypeCoercionRegistry,
    default_field_coercions,
)


def registry_from_sa_model(
    model: type[DeclarativeBase],
    field_coercions: dict[str, Callable[[Any], Any]] | None = None,
) -> TypeCoercionRegistry:
    """
    Build a ``TypeCoercionRegistry`` by introspecting a SQLAlchemy model.

    Coercer resolution order per column:
    1. ``Column.info['coercer']`` — explicit per-column override.
    2. ``field_coercions[field_name]`` — caller-provided overrides.
    3. ``default_field_coercions[python_type]`` — module-level defaults.

    Args:
        model:           SQLAlchemy declarative model class.
        field_coercions: Optional ``{field_name: coercer}`` overrides.

    Returns:
        Populated ``TypeCoercionRegistry``.

    Edge cases:
        - Columns with no matching coercer are silently skipped.
        - ``column.type.python_type`` may raise ``NotImplementedError``
          for exotic SA types — those columns are skipped.
    """
    field_coercions = field_coercions or {}
    registry = TypeCoercionRegistry()

    for column in model.__table__.columns:
        field_name = column.name
        try:
            python_type = column.type.python_type
        except NotImplementedError:
            continue

        coercer: Callable[[Any], Any] | None = None
        try:
            if hasattr(column, "info"):
                coercer = column.info.get("coercer")
        except Exception:
            coercer = None

        coercer = (
            coercer
            or field_coercions.get(field_name)
            or default_field_coercions.get(python_type)
        )
        if coercer is not None:
            registry.register_field(field_name, python_type, coercer)

    return registry
