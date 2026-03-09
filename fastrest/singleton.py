from __future__ import annotations
from typing import Any, Dict, Type, TypeVar
from threading import RLock

T = TypeVar("T")  # instance type


class SingletonMeta(type):
    _instances: Dict[Type[Any], Any] = {}
    _lock = RLock()

    def __call__(cls, *args, **kwargs):
        raise TypeError(
            f"{cls.__name__} is a singleton. Use `{cls.__name__}.instance()`"
        )

    # Only enable if the normal construcotr is needed
    # def __call__(cls: Type[_T], *args, **kwargs) -> _T:
    #    with SingletonMeta._lock:
    #        if cls not in SingletonMeta._instances:
    #            SingletonMeta._instances[cls] = super().__call__(*args, **kwargs)
    #    return SingletonMeta._instances[cls]

    def _clear_instance(cls, target_cls: type) -> None:
        cls._instances.pop(target_cls, None)

    def instance(cls: Type[T], *args, **kwargs) -> T:
        with SingletonMeta._lock:
            if cls not in SingletonMeta._instances:
                # Use type.__call__ to avoid super() metaclass typing issues
                SingletonMeta._instances[cls] = type.__call__(cls, *args, **kwargs)
        return SingletonMeta._instances[cls]
