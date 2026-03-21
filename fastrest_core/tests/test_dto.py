"""
Unit tests for fastrest_core.dto — CreateDTO, ReadDTO, UpdateDTO, UpdateOperation.

DTOs are Pydantic models — tests verify field presence, defaults,
validation, and the UpdateOperation enum.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from fastrest_core.dto import (
    CreateDTO,
    ReadDTO,
    UpdateDTO,
    UpdateOperation,
)


# ── UpdateOperation ────────────────────────────────────────────────────────────


def test_update_operation_values():
    assert UpdateOperation.REPLACE == "REPLACE"
    assert UpdateOperation.EXTEND == "EXTEND"
    assert UpdateOperation.REMOVE == "REMOVE"
    assert UpdateOperation.MERGE == "MERGE"


# ── CreateDTO ─────────────────────────────────────────────────────────────────


def test_create_dto_is_base_class():
    """CreateDTO should be subclass-able and instantiable."""

    class _CreateUser(CreateDTO):
        username: str
        email: str

    dto = _CreateUser(username="alice", email="alice@example.com")
    assert dto.username == "alice"
    assert dto.email == "alice@example.com"


def test_create_dto_empty_subclass():
    """A CreateDTO subclass with no extra fields is valid."""

    class _EmptyCreate(CreateDTO):
        pass

    obj = _EmptyCreate()
    assert isinstance(obj, CreateDTO)


def test_create_dto_validation_error_on_wrong_type():
    class _CreateUser(CreateDTO):
        age: int

    with pytest.raises(ValidationError):
        _CreateUser(age="not_a_number")  # type: ignore[arg-type]


# ── ReadDTO ───────────────────────────────────────────────────────────────────


_NOW = datetime.now(tz=timezone.utc)


def test_read_dto_requires_id_and_timestamps():
    class _ReadUser(ReadDTO):
        username: str

    dto = _ReadUser(
        pk="123",
        username="alice",
        created_at=_NOW,
        updated_at=_NOW,
    )
    assert dto.pk == "123"
    assert dto.username == "alice"
    assert dto.created_at == _NOW
    assert dto.updated_at == _NOW


def test_read_dto_missing_id_raises():
    class _ReadUser(ReadDTO):
        username: str

    with pytest.raises(ValidationError):
        _ReadUser(username="alice", created_at=_NOW, updated_at=_NOW)  # type: ignore[call-arg]


def test_read_dto_missing_created_at_raises():
    class _ReadUser(ReadDTO):
        pass

    with pytest.raises(ValidationError):
        _ReadUser(pk="1", updated_at=_NOW)  # type: ignore[call-arg]


def test_read_dto_accepts_string_datetime():
    """Pydantic should coerce ISO-8601 strings to datetime."""

    class _ReadItem(ReadDTO):
        pass

    dto = _ReadItem(
        pk="42",
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-06-01T12:00:00Z",
    )
    assert isinstance(dto.created_at, datetime)
    assert isinstance(dto.updated_at, datetime)


# ── UpdateDTO ─────────────────────────────────────────────────────────────────


def test_update_dto_default_op_is_replace():
    class _UpdateUser(UpdateDTO):
        email: str | None = None

    dto = _UpdateUser()
    assert dto.op == UpdateOperation.REPLACE


def test_update_dto_op_can_be_overridden():
    class _UpdateTags(UpdateDTO):
        tags: list[str] | None = None

    dto = _UpdateTags(op=UpdateOperation.EXTEND, tags=["new"])
    assert dto.op == UpdateOperation.EXTEND
    assert dto.tags == ["new"]


def test_update_dto_invalid_op_raises():
    class _UpdateUser(UpdateDTO):
        email: str | None = None

    with pytest.raises(ValidationError):
        _UpdateUser(op="INVALID_OP")  # type: ignore[arg-type]
