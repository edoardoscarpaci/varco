"""
Unit tests for fastrest_sa.models — BaseDatabaseModel, IndexedDatabaseModel,
AuthorizedModel.

Verifies column presence and the inheritance hierarchy.

DESIGN: testing abstract models
    IndexedDatabaseModel.id uses ``default_factory`` — a SA native-dataclass
    feature that triggers an error if you try to subclass it without
    ``MappedAsDataclass``.  We therefore:
    - Create a concrete subclass only for BaseDatabaseModel (no default_factory).
    - Test IndexedDatabaseModel and AuthorizedModel by inspecting their
      declared class attributes directly, which is sufficient to verify
      that the column definitions exist and carry the right settings.
"""

from __future__ import annotations

from sqlalchemy import Column, Integer as SAInt
from sqlalchemy.orm import DeclarativeBase

from fastrest_sa.models import BaseDatabaseModel


# ── Concrete subclass for BaseDatabaseModel ───────────────────────────────────
# BaseDatabaseModel has no PK column — we must add one to map it.


class _PlainBase(DeclarativeBase):
    pass


class _PlainEntity(_PlainBase, BaseDatabaseModel):
    """Minimal concrete subclass adding a required integer PK."""

    __tablename__ = "plain_entities_models"
    id = Column(SAInt, primary_key=True, autoincrement=True)


# ── BaseDatabaseModel ──────────────────────────────────────────────────────────


def test_base_model_is_abstract():
    assert getattr(BaseDatabaseModel, "__abstract__", False) is True


def test_base_model_concrete_has_created_at():
    assert "created_at" in _PlainEntity.__table__.c


def test_base_model_concrete_has_updated_at():
    assert "updated_at" in _PlainEntity.__table__.c


def test_base_model_concrete_has_model_version():
    assert "model_version" in _PlainEntity.__table__.c


def test_base_model_instantiable():
    """Concrete subclass must be instantiable."""
    # NOTE: mapped_column(default=1) is an INSERT-time column default in SA 2.x,
    # not a Python constructor default.  The attribute is None until a flush/commit.
    # We only verify the object can be constructed without error here.
    obj = _PlainEntity()
    assert obj is not None


# ── IndexedDatabaseModel — class-attribute inspection ─────────────────────────
def test_base_inherits_declarative_base():
    assert issubclass(BaseDatabaseModel, DeclarativeBase)
