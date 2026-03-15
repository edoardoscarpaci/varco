"""
fastrest_core.dto
==================
Base DTO (Data Transfer Object) classes for the API layer.

DTOs separate the HTTP contract from the domain model.  They are Pydantic
models so they validate and coerce input automatically.

Hierarchy::

    BaseModel
    ├── CreateDTO   — payload for POST (create) requests
    ├── ReadDTO     — response body for GET requests (includes timestamps + id)
    └── UpdateDTO   — payload for PATCH / PUT requests (includes operation type)

DESIGN: DTOs live in ``fastrest_core`` (not ``fastrest_sa`` or ``fastrest_beanie``)
  ✅ DTOs are backend-agnostic — they describe the API contract, not persistence.
  ✅ A single DTO layer works regardless of which storage backend is in use.
  ❌ Pydantic is now a hard dependency of ``fastrest_core``, but this is
     acceptable since DTOs are almost always needed alongside the domain layer.

Thread safety:  ✅ Pydantic models are effectively immutable after validation.
Async safety:   ✅ No I/O; pure value objects.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TypeVar

from pydantic import BaseModel, Field


# ── Update operation enum ──────────────────────────────────────────────────────


class UpdateOperation(StrEnum):
    """
    Describes how an UPDATE operation should be applied to a field.

    Attributes:
        REPLACE: Overwrite the existing value entirely (default).
        EXTEND:  Append to (or union with) the existing value.
        REMOVE:  Remove the specified value(s) from the field.
        MERGE:   Deep-merge dicts / objects.

    Usage::

        class MyUpdate(UpdateDTO):
            tags: list[str] | None = None

        payload = MyUpdate(op=UpdateOperation.EXTEND, tags=["new-tag"])
    """

    REPLACE = "REPLACE"
    EXTEND = "EXTEND"
    REMOVE = "REMOVE"
    MERGE = "MERGE"


# ── Base DTO classes ───────────────────────────────────────────────────────────


class CreateDTO(BaseModel):
    """
    Base class for CREATE (POST) request payloads.

    Subclass and add the fields required for entity creation::

        class CreateUserDTO(CreateDTO):
            username: str
            email: str

    Thread safety:  ✅ Pydantic model — effectively immutable after validation.
    Async safety:   ✅ No I/O.
    """


class ReadDTO(BaseModel):
    """
    Base class for GET response bodies.

    Includes the standard ``id``, ``created_at``, and ``updated_at`` fields
    that every persisted entity exposes.

    Attributes:
        id:         String-encoded primary key of the entity.
        updated_at: ISO-8601 timestamp of the most recent update.
        created_at: ISO-8601 timestamp of initial creation.

    Thread safety:  ✅ Pydantic model — effectively immutable after validation.
    Async safety:   ✅ No I/O.

    Example::

        class UserReadDTO(ReadDTO):
            username: str
            email: str
    """

    id: str = Field(..., description="String-encoded primary key of the entity.")
    updated_at: datetime = Field(..., description="ISO-8601 timestamp of last update.")
    created_at: datetime = Field(..., description="ISO-8601 timestamp of creation.")


class UpdateDTO(BaseModel):
    """
    Base class for UPDATE (PATCH / PUT) request payloads.

    Attributes:
        op: The update strategy.  Defaults to ``REPLACE``.

    Subclass and add the fields that can be changed::

        class UpdateUserDTO(UpdateDTO):
            email: str | None = None
            username: str | None = None

    Thread safety:  ✅ Pydantic model — effectively immutable after validation.
    Async safety:   ✅ No I/O.
    """

    op: UpdateOperation = Field(
        default=UpdateOperation.REPLACE,
        description="How to apply this update (REPLACE, EXTEND, REMOVE, MERGE).",
    )


# ── TypeVars ───────────────────────────────────────────────────────────────────

# Bound TypeVars used by generic mapper / assembler signatures so the
# type checker can propagate the concrete DTO subtype through the chain.
TCreateDTO = TypeVar("TCreateDTO", bound=CreateDTO)
TReadDTO = TypeVar("TReadDTO", bound=ReadDTO)
TUpdateDTO = TypeVar("TUpdateDTO", bound=UpdateDTO)
