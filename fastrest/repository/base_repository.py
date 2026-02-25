from __future__ import annotations

import warnings
import inspect
from abc import ABCMeta,abstractmethod
from typing import TYPE_CHECKING, AsyncGenerator,Generic,List,Type,Optional,Any,Collection
from fastrest.exception.registry import RegistrationFailedWarning
from fastrest.model_assembler import ModelAssembler
from fastrest.registry.model_assembler import ModelAssemblerRegistry
from fastrest.exception.repository import FieldNotFound,RepositoryClassCreationFailed,EntityNotFound

if TYPE_CHECKING:
    from fastrest.models.database_model import TDatabaseModel
    from fastrest.models.entity import TEntity
    from fastrest.repository.pagination import Pagination,Filter
    from fastrest.models.dto import TCreateDTO,TReadDTO,TUpdateDTO,UpdateOperation


class AutoRegisterRepositoryMeta(ABCMeta):
    """
    Metaclass to automatically register repository instances for their entity classes.
    """
    def __new__(cls, name, bases, attrs):
        from fastrest.registry.repository import RepositoryRegistry
        new_cls = super().__new__(cls, name, bases, attrs)
        
        database_model_class = getattr(new_cls,"database_model_class",None)
        if database_model_class is None:
            raise RepositoryClassCreationFailed(f"Failed to create {new_cls.__name__} missing 'database_model_class' ClassVar make sure to set when subclassing Repository",repository_cls=new_cls)

        if inspect.isabstract(new_cls):
            return new_cls

        entity_class = getattr(new_cls,"entity_class",None)
        if entity_class is None:
            warnings.warn(f"Missing entity_class skipping autoregistration for {name}",RegistrationFailedWarning)
            return new_cls
            
        instance = new_cls()  # create instance
        RepositoryRegistry().register(entity_class, instance)
        return new_cls

class Repository(Generic[TEntity,TDatabaseModel,TCreateDTO,TReadDTO,TUpdateDTO],metaclass=AutoRegisterRepositoryMeta):
    """
    Base repository class for async CRUD operations with DTO conversion.
    """
    entity_class : Type[TEntity]
    database_model_class : Type[TDatabaseModel]
    #To remove maybe
    create_entity_class : Type[TCreateDTO]
    read_entity_class : Type[TReadDTO]
    update_entity_class : Type[TUpdateDTO]

    def __init__(self,*args,model_assembler : Optional[ModelAssembler[TEntity,TDatabaseModel,TCreateDTO,TReadDTO]] = None,**kwargs):
        self.model_assembler = model_assembler or ModelAssemblerRegistry.instance().get(key=self.entity_class)

    
    async def initialize(self,*args,**kwargs):
        """
        Optional async initialization hook.
        """
        pass

    # -------------------
    # Abstract CRUD Methods
    # -------------------
    @abstractmethod
    async def create(self, entity: TCreateDTO, *args, db_session: Optional[Any] = None, **kwargs) -> TReadDTO:
        """
        Create a new entity in the database.
        """
        pass

    @abstractmethod
    async def _get_model(self, entity_id: str, *args, db_session: Optional[Any] = None, **kwargs) -> TDatabaseModel:
        """
        Fetch the database model instance by ID.
        Raise EntityNotFound if not found.
        """
        pass

    @abstractmethod
    async def delete(self, entity_id: str, *args, db_session: Optional[Any] = None, **kwargs) -> TReadDTO:
        """
        Delete an entity by ID and return its DTO.
        """
        pass

    @abstractmethod
    async def _save(self, model: TDatabaseModel, *args, db_session: Optional[Any] = None, **kwargs) -> TDatabaseModel:
        """
        Persist changes to the model in the database.
        """
        pass

    @abstractmethod
    async def list(self, pagination: Pagination, *args, db_session: Optional[Any] = None, **kwargs) -> List[TReadDTO]:
        """
        Return a paginated list of DTOs.
        """
        pass

    @abstractmethod
    async def stream(self, pagination: Filter, *args, db_session: Optional[Any] = None, **kwargs) -> AsyncGenerator[TReadDTO, None]:
        """
        Stream DTOs asynchronously.
        """
        pass

    @abstractmethod
    async def count(self, pagination: Filter, *args, db_session: Optional[Any] = None, **kwargs) -> int:
        """
        Count entities matching a filter.
        """
        pass

    async def exists(self, entity_id: str, *args, db_session: Optional[Any] = None, **kwargs) -> bool:
        """
        Check if an entity exists by ID.
        """
        try:
            model = await self._get_model(entity_id=entity_id, *args,db_session=db_session, **kwargs)
            #Just to make sure that if someone use the _get without the exception it still works
            if not model:
                return False
            
            return True
        except EntityNotFound:
            return False

    #Already Implemented Method 
    async def get(self, entity_id : str, *args, db_session: Optional[Any] = None,**kwargs) -> TReadDTO:
        model_db = await self._get_model(*args,entity_id=entity_id,db_session=db_session,**kwargs)
        return self.model_assembler.to_read_dto(model=model_db)

    async def update(self,entity_id : str, update_dto : TUpdateDTO, *args, db_session: Optional[Any] = None,**kwargs) -> TReadDTO:
        """
        Update fields of an entity based on the DTO and persist changes.
        """
        model = await self._get_model(*args,entity_id=entity_id,db_session=db_session,**kwargs)
        update_data = update_dto.model_dump(exclude_unset=True)
        for field_name,field_value in update_data.items():
            self._set_attribute(model=model,field_name=field_name,field_value=field_value,op=update_dto.op)
        
        saved_model = await self._save(*args,model=model,db_session=db_session,**kwargs)
        return self.model_assembler.to_read_dto(saved_model)

    def _set_attribute(self, model: TDatabaseModel, field_name: str, field_value: Any, op: UpdateOperation):
        """
        Set an attribute on the model with support for EXTEND, REMOVE, MERGE operations for collections.
        """
        if not hasattr(model, field_name):
            raise FieldNotFound(field=field_name, table=self.database_model_class.__tablename__)

        existing_attr = getattr(model, field_name)
        # Handle list fields
        if isinstance(existing_attr, list):
            if op == UpdateOperation.EXTEND:
                if isinstance(field_value, list):
                    existing_attr.extend(field_value)
                else:
                    existing_attr.append(field_value)
                value_to_set = existing_attr
            elif op == UpdateOperation.REMOVE:
                if isinstance(field_value, list):
                    for v in field_value:
                        if v in existing_attr:
                            existing_attr.remove(v)
                else:
                    if field_value in existing_attr:
                        existing_attr.remove(field_value)
                value_to_set = existing_attr
            else:  # REPLACE
                value_to_set = list(field_value) if not isinstance(field_value, list) else field_value

        # Handle dict fields
        elif isinstance(existing_attr, dict) and op == UpdateOperation.MERGE:
            if not isinstance(field_value, dict):
                raise TypeError(f"Cannot merge non-dict into dict field '{field_name}'")
            existing_attr.update(field_value)
            value_to_set = existing_attr

        # Scalar fields
        else:
            value_to_set = field_value

        setattr(model, field_name, value_to_set)
