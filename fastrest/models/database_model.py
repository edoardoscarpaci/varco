from datetime import datetime
from sqlalchemy.orm import DeclarativeBase,mapped_column,Mapped
from sqlalchemy import DateTime, func
from uuid import uuid4
from typing import TypeVar

class BaseDatabaseModel(DeclarativeBase):
    __abstract__ = True
        
    created_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),server_default=func.now())
    updated_at : Mapped[datetime] = mapped_column(DateTime(timezone=True),server_default=func.now(),onupdate=func.now())
    model_version : Mapped[int] = mapped_column(default=1)

class DatabaseModel(BaseDatabaseModel):
    id : Mapped[str] = mapped_column(primary_key=True,default_factory=lambda: str(uuid4()))

class AuthorizedModel(DatabaseModel):
    auth_context : Mapped[str] = mapped_column()

TDatabaseModel = TypeVar('DatabaseModel',bound=BaseDatabaseModel)