from typing import Type

class ModelException(Exception):
    pass

class FieldNotFoundInEntity(Exception):
    def __init__(self, field_name : str, entity_class : Type,*args,**kwargs):
        super().__init__(f"Field {field_name} not found in {entity_class}",*args,**kwargs)
        self.field_name = field_name
        self.entity_class = entity_class