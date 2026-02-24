from __future__ import annotations
from typing import TYPE_CHECKING

from fastrest.registry.registry import Registry

if TYPE_CHECKING:
    from fastrest.model_assembler import ModelAssembler
    from fastrest.models.database_model import TDatabaseModel
    from fastrest.models.entity import TEntity
    from fastrest.models.dto import TCreateDTO, TReadDTO


class ModelAssemblerRegistry(Registry[ModelAssembler[TEntity, TDatabaseModel, TCreateDTO, TReadDTO]]):
    """Registry for ``ModelAssembler`` instances.

    This is a typed wrapper around :class:`~fastrest.registry.registry.Registry`
    specialized for ``ModelAssembler`` objects. The registry is implemented as
    a singleton (via ``SingletonMeta``) so a single global store of assemblers
    is available to the application.

    Use ``register(key, assembler)`` to add an assembler and ``get(key)`` to
    retrieve it by the entity type the assembler handles.
    """
    pass