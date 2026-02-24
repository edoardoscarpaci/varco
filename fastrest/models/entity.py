from typing import Optional,TypeVar
from dataclasses import dataclass,field
from datetime import datetime

@dataclass
class Entity():
    id : Optional[str] = field(default=None)
    updated_at : Optional[datetime] = field(default=None)
    created_at : Optional[datetime] = field(default=None)

TEntity = TypeVar("TEntity",bound=Entity)
