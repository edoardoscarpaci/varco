"""
varco_beanie.query
======================
MongoDB query compiler — translates ``varco_core`` AST nodes into
MongoDB filter documents compatible with Beanie's ``Document.find()`` API.

    BeanieQueryCompiler — ASTVisitor subclass producing ``dict[str, Any]``

Import directly::

    from varco_beanie.query.compiler import BeanieQueryCompiler
"""

from varco_beanie.query.aggregation import BeanieAggregationApplicator, Pipeline
from varco_beanie.query.compiler import BeanieQueryCompiler

__all__ = [
    "BeanieQueryCompiler",
    "BeanieAggregationApplicator",
    "Pipeline",
]
