from typing import TypeVar
from pydantic import BaseModel,Field
from datetime import datetime

class CreateDTO(BaseModel):
    pass

class ReadDTO(BaseModel):
    id: str = Field(...,description="The id of the entity")
    updated_at : datetime = Field(...,description="When it was last updated")
    created_at : datetime = Field(...,description="When the entity was created")

TCreateDTO = TypeVar("CreateDTO",bound=CreateDTO)
TReadDTO = TypeVar("ReadDTO",bound=ReadDTO)