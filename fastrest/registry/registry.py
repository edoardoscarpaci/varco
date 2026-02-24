from __future__ import annotations
import warnings
from typing import Generic,TypeVar,Type,TYPE_CHECKING

from fastrest.exception.registry import KeyNotFound,KeyAlreadyInRegistryWarning
from fastrest.singleton import SingletonMeta

if TYPE_CHECKING:
    from fastrest.models.entity import Entity

_T = TypeVar('_T')
class Registry(Generic[_T],metaclass=SingletonMeta):
    def __init__(self,*args,**kwargs) -> None:
        self._instance = {}

    def register(self,key : Type['Entity'],value : _T) -> None:
        if key in self._instance:
            warnings.warn(KeyAlreadyInRegistryWarning.get_message(key=key,registry=self.__class__),KeyAlreadyInRegistryWarning)
        self._instance[key] = value

    def get(self,key: Type['Entity']) -> _T :
        if key not in self._instance:
            raise KeyNotFound(key=key,registry=self.__class__)
        return self._instance[key]
    
    def exists(self,key :  Type['Entity']) -> bool:
        return key in self._instance