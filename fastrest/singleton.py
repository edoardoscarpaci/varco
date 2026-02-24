from typing import Any, Dict, Type, TypeVar

_T = TypeVar("_T")
class SingletonMeta(type):
    _instances: Dict[type, Any] = {}

    def __call__(cls, *args, **kwargs) -> _T:
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]