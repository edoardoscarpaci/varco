from __future__ import annotations

import warnings
import inspect
from abc import ABCMeta,abstractmethod
from typing import TYPE_CHECKING,Generic,Type
from fastrest.exception.registry import RegistrationFailedWarning

if TYPE_CHECKING:
    from fastrest.models.database_model import TDatabaseModel
    from fastrest.models.entity import TEntity
    from fastrest.models.dto import TCreateDTO,TReadDTO


class AutoRegisterRepositoryMeta(ABCMeta):
    def __new__(cls, name, bases, attrs):
        from fastrest.registry.repository import RepositoryRegistry
        new_cls = super().__new__(cls, name, bases, attrs)
        if inspect.isabstract(new_cls):
            return new_cls

        entity_cls = getattr(new_cls,"entity_cls",None)
        if entity_cls is None:
            warnings.warn(f"Missing entity_cls skipping autoregistration for {name}",RegistrationFailedWarning)
            return new_cls
            
        instance = new_cls()  # create instance
        RepositoryRegistry().register(entity_cls, instance)
        return new_cls

class Repository(Generic[TEntity,TDatabaseModel,TCreateDTO,TReadDTO],metaclass=AutoRegisterRepositoryMeta):
    entity_class : Type[TEntity]
    database_model_class : Type[TDatabaseModel]
    createTEntity_class : Type[TCreateDTO]
    readTEntity_class : Type[TReadDTO]

    def __init__(self,*args,**kwargs):
        pass

    async def inizialize(self,*args,**kwargs):
        pass

    @abstractmethod
    async def create(self,entity : TEntity,*args,**kwargs) -> TEntity:
        pass

    @abstractmethod
    async def get(self, entity_id : str, *args,**kwargs) -> TEntity:
        pass

    @abstractmethod
    async def delete(self,entity_id : str,*args,**kwargs) -> TEntity:
        pass

    @abstractmethod
    async def update(self,entity : TEntity,*args,**kwargs) -> TEntity:
        pass


