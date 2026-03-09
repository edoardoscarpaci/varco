from typing import List, Optional
from dataclasses import dataclass, field
from fastrest.query.type import SortField


@dataclass(frozen=True)
class SortBy:
    sort_fields: List[SortField] = field(default_factory=list)


@dataclass(frozen=True)
class Filter(SortBy):
    query: str = field(default="")


@dataclass(frozen=True)
class Pagination(Filter):
    limit: Optional[int] = field(default=None)
    offset: Optional[int] = field(default=None)
