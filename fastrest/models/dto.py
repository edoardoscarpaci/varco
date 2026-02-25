from typing import TypeVar
from pydantic import BaseModel,Field
from datetime import datetime
from enum import StrEnum

class UpdateOperation(StrEnum):
    REPLACE = "REPLACE"
    EXTEND = "EXTEND"
    REMOVE = "REMOVE"
    MERGE = "MERGE"

class CreateDTO(BaseModel):
    pass

class ReadDTO(BaseModel):
    id: str = Field(...,description="The id of the entity")
    updated_at : datetime = Field(...,description="When it was last updated")
    created_at : datetime = Field(...,description="When the entity was created")

class UpdateDTO(BaseModel):
    op: UpdateOperation 

TCreateDTO = TypeVar("TCreateDTO",bound=CreateDTO)
TReadDTO = TypeVar("TReadDTO",bound=ReadDTO)
TUpdateDTO = TypeVar("TUpdateDTO",bound=UpdateDTO)