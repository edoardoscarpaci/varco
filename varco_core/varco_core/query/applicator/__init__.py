"""
varco_core.query.applicator
================================
Strategy classes that apply AST nodes, sort directives, and pagination to
backend-native query objects.

    QueryApplicator          — abstract strategy base
    SQLAlchemyQueryApplicator — concrete SA 2.x ``Select`` applicator

Import from the sub-modules directly::

    from varco_core.query.applicator.applicator import QueryApplicator
    from varco_core.query.applicator.sqlalchemy import SQLAlchemyQueryApplicator
"""

from varco_core.query.applicator.applicator import QueryApplicator
from varco_core.query.applicator.sqlalchemy import SQLAlchemyQueryApplicator

__all__ = [
    "QueryApplicator",
    "SQLAlchemyQueryApplicator",
]
