"""
fastrest_beanie.query
======================
MongoDB query compiler — translates ``fastrest_core`` AST nodes into
MongoDB filter documents compatible with Beanie's ``Document.find()`` API.

    BeanieQueryCompiler — ASTVisitor subclass producing ``dict[str, Any]``

Import directly::

    from fastrest_beanie.query.compiler import BeanieQueryCompiler
"""

from fastrest_beanie.query.compiler import BeanieQueryCompiler

__all__ = ["BeanieQueryCompiler"]
