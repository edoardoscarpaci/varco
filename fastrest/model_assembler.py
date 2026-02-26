from abc import abstractmethod,ABCMeta
from typing import TYPE_CHECKING,Generic,Type
import warnings
from fastrest.exception.model_assembler import FieldNotFoundInEntity
from fastrest.exception.registry import RegistrationFailedWarning
import inspect

from fastrest.models.dto import UpdateDTO
if TYPE_CHECKING:
    from fastrest.models.database_model import TDatabaseModel
    from fastrest.models.entity import TEntity
    from fastrest.models.dto import TCreateDTO,TReadDTO,TUpdateDTO

class AutoRegisterModelAssemblerMeta(ABCMeta):
    def __new__(cls, name, bases, attrs):
        from fastrest.registry.model_assembler import ModelAssemblerRegistry
        new_cls = super().__new__(cls, name, bases, attrs)
        if inspect.isabstract(new_cls):
            return new_cls

        entity_cls = getattr(new_cls,"entity_cls",None)
        if entity_cls is None:
            warnings.warn(f"Missing entity_cls skipping autoregistration for {name}",RegistrationFailedWarning)
            return new_cls
            
        instance = new_cls()  # create instance
        ModelAssemblerRegistry().register(entity_cls, instance)
        return new_cls

class ModelAssembler(Generic[TEntity,TDatabaseModel,TCreateDTO,TReadDTO],metaclass=AutoRegisterModelAssemblerMeta):
    entity_class : Type[TEntity]
    database_model_class : Type[TDatabaseModel]
    create_entity_class : Type[TCreateDTO]
    read_entity_class : Type[TReadDTO]

    def __init__(self,*args,**kwargs) -> None:
        super().__init__()

    @abstractmethod
    def to_entity(self,model: TDatabaseModel) -> TEntity:
        pass
    
    @abstractmethod
    def to_model(self,entity: TEntity)-> TDatabaseModel:
        pass
   
    @abstractmethod
    def to_model_from_create_dto(self,dto : TCreateDTO) -> TDatabaseModel:
        pass

    @abstractmethod
    def to_read_dto(self,model: TDatabaseModel) -> TReadDTO:
        pass
    
    def to_read_dto_from_entity(self, entity: TEntity) -> TReadDTO:
        data =  {}
        for field_name,field_info in self.read_entity_class.model_fields.items():
            if hasattr(entity, field_name):
                data[field_name] = getattr(entity, field_name)
            elif field_info.is_required():
                raise FieldNotFoundInEntity(field_name=field_name,entity_class=type(entity))

        return self.read_entity_class(**data)
    
    def to_create_dto_from_entity(self,entity: TEntity) -> TCreateDTO:
        data =  {}
        for field_name,field_info in self.create_entity_class.model_fields.items():
            if hasattr(entity, field_name):
                data[field_name] = getattr(entity, field_name)
            elif field_info.is_required():
                raise FieldNotFoundInEntity(field_name=field_name,entity_class=type(entity))

        return self.create_entity_class(**data)