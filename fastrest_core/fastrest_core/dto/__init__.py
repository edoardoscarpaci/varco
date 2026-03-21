"""
fastrest_core.dto
=================
DTO layer — base classes, factory, and pagination envelope.
"""

from fastrest_core.dto.base import (
    CreateDTO,
    ReadDTO,
    UpdateDTO,
    UpdateOperation,
    TCreateDTO,
    TReadDTO,
    TUpdateDTO,
)
from fastrest_core.dto.factory import DTOSet, generate_dtos
from fastrest_core.dto.pagination import (
    PageCursor,
    PagedReadDTO,
    SortCursorField,
    paged_response,
)

__all__ = [
    "CreateDTO",
    "ReadDTO",
    "UpdateDTO",
    "UpdateOperation",
    "TCreateDTO",
    "TReadDTO",
    "TUpdateDTO",
    "DTOSet",
    "generate_dtos",
    "PageCursor",
    "PagedReadDTO",
    "SortCursorField",
    "paged_response",
]
