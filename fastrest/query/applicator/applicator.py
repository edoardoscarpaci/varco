from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, TypeVar, Optional

if TYPE_CHECKING:
    from fastrest.query.type import TransformerNode, SortField

_T = TypeVar("_T")


class QueryApplicator(ABC):
    def __init__(self, *args, allowed_fields: set[str] = set(), **kwargs):
        self.allowed_fields = allowed_fields
        pass

    @abstractmethod
    def apply_query(self, query: _T, node: TransformerNode, *args, **kwargs) -> _T:
        pass

    @abstractmethod
    def apply_pagination(
        self, query: _T, limit: Optional[int], offset: Optional[int], *args, **kwargs
    ) -> _T:
        pass

    @abstractmethod
    def apply_sort(
        self, query: _T, filters: List[SortField] = list(), *args, **kwargs
    ) -> _T:
        pass
