from __future__ import annotations

from fastrest.registry.registry import Registry
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastrest.repository.base_repository import Repository
    from fastrest.models.database_model import TDatabaseModel
    from fastrest.models.entity import TEntity
    from fastrest.models.dto import TCreateDTO, TReadDTO, TUpdateDTO


class RepositoryRegistry(
    Registry[Repository[TEntity, TDatabaseModel, TCreateDTO, TReadDTO, TUpdateDTO]]
):
    """Registry for ``Repository`` instances.

    This is a typed wrapper around :class:`~fastrest.registry.registry.Registry`
    specialized for ``Repository`` objects. The registry is implemented as
    a singleton (via ``SingletonMeta``) so a single global store of repositories
    is available to the application.

    Use ``register(key, repository)`` to add a repository and ``get(key)`` to
    retrieve it by the entity type the repository handles.
    """

    pass
