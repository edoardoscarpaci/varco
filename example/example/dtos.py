"""
example.dtos
============
Pydantic DTOs for the Post API contract.

Three DTOs mirror the ``AsyncService[D, PK, C, R, U]`` type parameters:

    ``PostCreate``  (C) — POST /posts body.  author_id is intentionally absent —
                         it is stamped from the JWT in the service layer.
    ``PostRead``    (R) — GET response body.  All fields, including server-assigned
                         ``pk``, ``author_id``, and ``created_at``.
    ``PostUpdate``  (U) — PATCH /posts/{id} body.  All fields optional so partial
                         updates are supported without re-sending unchanged data.

DESIGN: DTOs are separate from the domain model
    ✅ API contract is independent of persistence — fields can differ (e.g.
       ``author_id`` absent in ``PostCreate``, present in ``PostRead``).
    ✅ Pydantic validation (length, format) lives here, not in the domain model.
    ✅ Swagger / OpenAPI schema is generated from these types, not the dataclass.
    ❌ One extra class per entity — justified by the clean SRP separation.

Thread safety:  ✅ Pydantic models are effectively immutable after construction.
Async safety:   ✅ Pure value objects — no I/O.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO


class PostCreate(CreateDTO):
    """
    Payload for ``POST /posts``.

    ``author_id`` is NOT included — the service stamps it from
    ``AuthContext.subject`` so clients cannot forge authorship.

    Args:
        title: Post headline, 1–255 characters.
        body:  Post body text.

    Raises:
        ValidationError: ``title`` is empty or exceeds 255 characters.
    """

    title: str
    body: str


class PostRead(ReadDTO):
    """
    Response body for ``GET /posts/{id}`` and ``GET /posts``.

    ``pk`` is the canonical entity identifier returned in API responses.
    ``created_at`` and ``updated_at`` are UTC — clients should display
    in their local timezone.

    Args:
        pk:         Post UUID assigned by the repository on INSERT.
        title:      Post headline.
        body:       Post body text.
        author_id:  UUID of the user who created the post.
        created_at: UTC timestamp when the post was first created.
        updated_at: UTC timestamp of the most recent update.
    """

    pk: UUID
    title: str
    body: str
    author_id: UUID | None
    created_at: datetime
    updated_at: datetime


class PostUpdate(UpdateDTO):
    """
    Payload for ``PATCH /posts/{id}``.

    All fields are optional — only supplied fields are changed.  ``None``
    means "no change", matching the ``apply_update`` convention in
    ``PostAssembler``.

    Args:
        title: New headline.  ``None`` = keep existing.
        body:  New body text.  ``None`` = keep existing.

    Edge cases:
        - Sending ``{}`` (empty body) is valid and produces a no-op update.
        - Fields set to ``null`` in JSON are distinct from absent fields
          in Pydantic v2 with ``model_config = ConfigDict(extra="ignore")``.
    """

    title: str | None = None
    body: str | None = None


__all__ = ["PostCreate", "PostRead", "PostUpdate"]
