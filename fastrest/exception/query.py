from typing import Type,TYPE_CHECKING

if TYPE_CHECKING:
    from fastrest.query.type import TransformerNode

class QueryException(Exception):
    pass

class OperationNotFound(QueryException):
    def __init__(self,op : str):
        super().__init__(f"Operation {op} not found")

class OperationNotSupported(QueryException):
    def __init__(self,op : str):
        super().__init__(f"{op} not supported")

class WrongNodeVisited(QueryException):
    def __init__(self, received_node_cls : Type[TransformerNode], expected_node_cls: Type[TransformerNode]):
        super().__init__(f"Received the wrong node class {received_node_cls.__name__} to visit, expected class {expected_node_cls.__name__}")

class CoercionError(QueryException):
    pass