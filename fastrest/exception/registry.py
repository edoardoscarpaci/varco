from typing import Any, Literal,Type

class RegistryWarning(Warning):
    @classmethod 
    def get_message(cls,*args,**kwargs) -> str:
        return ""
class KeyAlreadyInRegistryWarning(RegistryWarning):
    @classmethod
    def get_message(cls,key : Any,registry : Type)->str:
        return f"Key {key} already found in {registry.__name__}"

class RegistrationFailedWarning(RegistryWarning):
    pass

class RegistryException(Exception):
    pass

class RegistrationFailed(RegistryException):
    pass

class KeyNotFound(RegistryException):
    def __init__(self,key : Any,registry : Type):
        super().__init__(f"Key {str(key)} not found in registry {registry.__name__}")

class KeyAlreadyInRegistry(RegistryException):
    def __init__(self,key : Any,registry : Type):
        super().__init__(f"Key {str(key)} already found in registry {registry.__name__}")
