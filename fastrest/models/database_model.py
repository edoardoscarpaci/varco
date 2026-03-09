"""Base SQLAlchemy declarative models used across the project.

Provides common fields and sensible defaults for timestamping and ids.
"""

from datetime import datetime
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from sqlalchemy import DateTime, func
from uuid import uuid4
from typing import TypeVar


class BaseDatabaseModel(DeclarativeBase):
    """Abstract base model with timestamp and versioning fields."""

    __abstract__ = True

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    model_version: Mapped[int] = mapped_column(default=1)


class DatabaseModel(BaseDatabaseModel):
    """Concrete base model that adds an `id` primary key."""

    id: Mapped[str] = mapped_column(
        primary_key=True, default_factory=lambda: str(uuid4())
    )


class AuthorizedModel(DatabaseModel):
    """Model mixin for resources that include an authorization context."""

    auth_context: Mapped[str] = mapped_column()


TDatabaseModel = TypeVar("DatabaseModel", bound=BaseDatabaseModel)
