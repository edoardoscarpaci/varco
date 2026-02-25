from typing import Type

from fastrest.registry.repository import Repository

class RepositoryException(Exception):
    pass

class RepositoryClassCreationFailed(RepositoryException):
    def __init__(self,message : str , repository_cls : Type,*args,**kwargs):
        super().__init__(message,*args,**kwargs)
        self.repository_cls = repository_cls

class FieldNotFound(RepositoryException):
    def __init__(self, field : str, table : str,*args,**kwargs):
        super().__init__(f"Field {field} not found in {table}",*args,**kwargs)

class EntityNotFound(RepositoryException):
    def __init__(self, entity_id : str, table : str,*args,**kwargs):
        super().__init__(f"Entity({entity_id}) not found in {table}",*args,**kwargs)