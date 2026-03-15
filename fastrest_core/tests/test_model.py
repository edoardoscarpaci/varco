"""
Tests for DomainModel, AuditedDomainModel, VersionedDomainModel, and cast_raw().

Covers:
- is_persisted() — False before, True after backing ORM set
- raw() — happy path and RuntimeError guard
- cast_raw() — typed access, TypeError on wrong ORM type
- AuditedDomainModel and VersionedDomainModel default field values

Thread safety:  N/A (unit tests, no concurrency)
Async safety:   N/A (all synchronous)
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from fastrest_core.model import (
    AuditedDomainModel,
    DomainModel,
    VersionedDomainModel,
    cast_raw,
)


# ── Minimal concrete subclasses for testing ───────────────────────────────────


@dataclass
class _Widget(DomainModel):
    """Minimal concrete DomainModel for testing base-class behaviour."""

    name: str = ""


@dataclass
class _Audited(AuditedDomainModel):
    """Concrete AuditedDomainModel subclass for field-presence tests."""

    title: str = ""


@dataclass
class _Versioned(VersionedDomainModel):
    """Concrete VersionedDomainModel subclass for field-presence tests."""

    label: str = ""


# ── Helpers ───────────────────────────────────────────────────────────────────


class _FakeOrm:
    """Stand-in ORM class used to test cast_raw() type checking."""


class _WrongOrm:
    """Different ORM class — used to trigger cast_raw() TypeError."""


# ── DomainModel.is_persisted() ────────────────────────────────────────────────


def test_is_persisted_false_on_fresh_entity() -> None:
    """Freshly constructed entity has no backing ORM → is_persisted is False."""
    widget = _Widget(name="gear")
    assert widget.is_persisted() is False


def test_is_persisted_false_when_raw_orm_is_none() -> None:
    """Explicitly setting _raw_orm to None keeps is_persisted False."""
    widget = _Widget(name="gear")
    # _raw_orm is None by default; confirm the field exists and is None
    assert widget._raw_orm is None
    assert widget.is_persisted() is False


def test_is_persisted_true_after_backing_orm_set() -> None:
    """is_persisted returns True as soon as _raw_orm is assigned."""
    widget = _Widget(name="gear")
    widget._raw_orm = _FakeOrm()
    assert widget.is_persisted() is True


def test_is_persisted_false_after_clearing_raw_orm() -> None:
    """
    Clearing _raw_orm resets is_persisted to False.

    Edge case: unlikely in production but a valid state transition
    to document — is_persisted is derived purely from _raw_orm.
    """
    widget = _Widget(name="gear")
    widget._raw_orm = _FakeOrm()
    assert widget.is_persisted() is True

    widget._raw_orm = None
    assert widget.is_persisted() is False


# ── DomainModel.raw() ─────────────────────────────────────────────────────────


def test_raw_returns_backing_orm_object() -> None:
    """raw() returns the backing ORM object when the entity is persisted."""
    orm = _FakeOrm()
    widget = _Widget(name="gear")
    widget._raw_orm = orm

    assert widget.raw() is orm


def test_raw_raises_runtime_error_on_fresh_entity() -> None:
    """raw() raises RuntimeError when called on an unpersisted entity."""
    widget = _Widget(name="gear")

    with pytest.raises(RuntimeError, match="raw\\(\\) called before entity was loaded"):
        widget.raw()


def test_raw_error_message_includes_class_name() -> None:
    """RuntimeError message includes the subclass name for easy debugging."""
    widget = _Widget(name="gear")

    with pytest.raises(RuntimeError, match="_Widget"):
        widget.raw()


# ── cast_raw() ────────────────────────────────────────────────────────────────


def test_cast_raw_returns_typed_orm_object() -> None:
    """cast_raw returns the raw ORM object typed as orm_type."""
    orm = _FakeOrm()
    widget = _Widget(name="gear")
    widget._raw_orm = orm

    result = cast_raw(widget, _FakeOrm)
    assert result is orm


def test_cast_raw_raises_type_error_for_wrong_orm_type() -> None:
    """
    cast_raw raises TypeError when the backing object is not an instance
    of the requested ORM type.

    Edge case: caller accidentally passes the wrong ORM class — should get
    a clear error rather than a silent None or AttributeError later.
    """
    widget = _Widget(name="gear")
    widget._raw_orm = _FakeOrm()  # backed by _FakeOrm, not _WrongOrm

    with pytest.raises(TypeError, match="_WrongOrm"):
        cast_raw(widget, _WrongOrm)


def test_cast_raw_raises_runtime_error_when_not_persisted() -> None:
    """cast_raw propagates RuntimeError from raw() when entity is unpersisted."""
    widget = _Widget(name="gear")

    with pytest.raises(RuntimeError):
        cast_raw(widget, _FakeOrm)


def test_cast_raw_error_message_names_expected_and_actual_types() -> None:
    """TypeError message names both expected and actual ORM types."""
    widget = _Widget(name="gear")
    widget._raw_orm = _FakeOrm()

    with pytest.raises(TypeError) as exc_info:
        cast_raw(widget, _WrongOrm)

    msg = str(exc_info.value)
    # Must name what was requested and what was found
    assert "_WrongOrm" in msg
    assert "_FakeOrm" in msg


# ── AuditedDomainModel field defaults ────────────────────────────────────────


def test_audited_domain_model_has_none_timestamps_on_construction() -> None:
    """AuditedDomainModel has created_at and updated_at defaulted to None."""
    entity = _Audited(title="test")
    # Both timestamps are None until a repository operation populates them
    assert entity.created_at is None
    assert entity.updated_at is None


def test_audited_domain_model_inherits_pk_and_raw_orm() -> None:
    """AuditedDomainModel still has pk and _raw_orm from DomainModel."""
    entity = _Audited(title="test")
    assert entity.pk is None
    assert entity._raw_orm is None
    assert entity.is_persisted() is False


def test_audited_domain_model_is_persisted_tracks_raw_orm() -> None:
    """AuditedDomainModel.is_persisted() delegates to _raw_orm check."""
    entity = _Audited(title="test")
    entity._raw_orm = MagicMock()
    assert entity.is_persisted() is True


# ── VersionedDomainModel field defaults ──────────────────────────────────────


def test_versioned_domain_model_has_default_field_values() -> None:
    """VersionedDomainModel starts with known sentinel values for system fields."""
    entity = _Versioned(label="v")
    # definition_version starts at 1 (schema not yet migrated)
    assert entity.definition_version == 1
    # row_version starts at 0 (not yet persisted — row_version 1 is set on INSERT)
    assert entity.row_version == 0


def test_versioned_domain_model_inherits_audit_timestamps() -> None:
    """VersionedDomainModel inherits created_at / updated_at from AuditedDomainModel."""
    entity = _Versioned(label="v")
    assert entity.created_at is None
    assert entity.updated_at is None


def test_versioned_domain_model_is_persisted_tracks_raw_orm() -> None:
    """VersionedDomainModel.is_persisted() delegates to _raw_orm check."""
    entity = _Versioned(label="v")
    assert entity.is_persisted() is False

    entity._raw_orm = MagicMock()
    assert entity.is_persisted() is True


# ── DomainModel pk field ──────────────────────────────────────────────────────


def test_pk_defaults_to_none() -> None:
    """pk is None on a freshly constructed entity — not in constructor."""
    widget = _Widget(name="gear")
    assert widget.pk is None


def test_pk_can_be_set_directly() -> None:
    """
    pk can be set directly (needed by STR_ASSIGNED / CUSTOM strategies).

    Not using pk_field(init=True) here — just verifying field is mutable.
    """
    widget = _Widget(name="gear")
    widget.pk = "custom-key"
    assert widget.pk == "custom-key"
