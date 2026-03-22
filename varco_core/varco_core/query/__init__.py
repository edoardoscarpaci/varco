"""
varco_core.query
====================
AST-based query system — backend-agnostic filtering, sorting, and pagination.

Public surface::

    from varco_core.query import QueryBuilder, QueryParams, QueryParser
    from varco_core.query import SortField, SortOrder, Operation

Sub-packages
------------
- ``type``        — AST node types (ComparisonNode, AndNode, …)
- ``builder``     — Fluent immutable QueryBuilder
- ``params``      — QueryParams value object
- ``parser``      — QueryParser (string → AST)
- ``transformer`` — Lark transformer (grammar tree → AST nodes)
- ``visitor``     — Visitor pattern implementations
- ``applicator``  — Backend query application strategies
"""

from varco_core.query.builder import QueryBuilder
from varco_core.query.params import QueryParams
from varco_core.query.parser import QueryParser
from varco_core.query.type import Operation, SortField, SortOrder

__all__ = [
    "QueryBuilder",
    "QueryParams",
    "QueryParser",
    "SortField",
    "SortOrder",
    "Operation",
]
