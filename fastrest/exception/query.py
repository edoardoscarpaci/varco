class QueryException(Exception):
    pass

class OperationNotFound(QueryException):
    def __init__(self,op : str):
        super().__init__(f"Operation {op} not found")

class OperationNotSupported(QueryException):
    def __init__(self,op : str):
        super().__init__(f"{op} not supported")
