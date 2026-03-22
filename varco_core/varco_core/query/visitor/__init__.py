"""
varco_core.query.visitor
=============================
AST visitor implementations.

    ASTVisitor          — abstract base (visitor pattern)
    ASTQueryOptimizer   — double-NOT elimination, AND flattening
    ASTTypeCoercion     — coerces comparison values to the model's field types
    SQLAlchemyQueryCompiler — compiles AST → SQLAlchemy ColumnElement expressions

Import directly from the sub-modules for the concrete visitors::

    from varco_core.query.visitor.sqlalchemy import SQLAlchemyQueryCompiler
    from varco_core.query.visitor.type_coercion import ASTTypeCoercion, TypeCoercionRegistry
    from varco_core.query.visitor.query_optimizer import ASTQueryOptimizer
"""

from varco_core.query.visitor.ast_visitor import ASTVisitor
from varco_core.query.visitor.query_optimizer import ASTQueryOptimizer
from varco_core.query.visitor.type_coercion import (
    ASTTypeCoercion,
    TypeCoercionRegistry,
)

__all__ = [
    "ASTVisitor",
    "ASTQueryOptimizer",
    "ASTTypeCoercion",
    "TypeCoercionRegistry",
]
