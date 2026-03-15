"""
fastrest_sa.provider
=====================
Concrete ``RepositoryProvider`` for SQLAlchemy async.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from fastrest_sa.factory import SAModelFactory
from fastrest_core.model import DomainModel
from fastrest_core.providers import RepositoryProvider
from fastrest_core.repository import AsyncRepository

D = TypeVar("D", bound=DomainModel)


class SQLAlchemyRepositoryProvider(RepositoryProvider):
    """
    ``RepositoryProvider`` backed by SQLAlchemy async.

    Usage::

        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
        from sqlalchemy.orm import DeclarativeBase
        from fastrest_sa.provider import SQLAlchemyRepositoryProvider

        class Base(DeclarativeBase): pass

        engine   = create_async_engine("postgresql+asyncpg://...")
        sessions = async_sessionmaker(engine, expire_on_commit=False)

        provider = SQLAlchemyRepositoryProvider(base=Base, session_factory=sessions)
        provider.register(User, Post)   # ← or autodiscover("myapp.models")

        async with provider.make_uow() as uow:
            user = await uow.users.save(User(name="Edo", email="..."))

    DESIGN: one ``SAModelFactory`` per provider instance
      ✅ Generated ORM classes share the same ``DeclarativeBase`` — required
         for ``Base.metadata.create_all()`` to include all tables
      ✅ Factory cache is scoped to the provider instance
      ❌ Two providers with the same ``Base`` would conflict on tablenames —
         use one provider per ``Base`` per process

    Args:
        base:            Shared ``DeclarativeBase`` subclass.
        session_factory: ``async_sessionmaker`` or any ``() → AsyncSession``.

    Edge cases:
        - Call ``register()`` / ``autodiscover()`` before
          ``Base.metadata.create_all()`` so all generated tables are included.
        - ``make_uow()`` derives repo attribute names automatically:
          ``User`` → ``uow.users``, ``UserRole`` → ``uow.userroles``.
    """

    def __init__(
        self,
        base: Any,
        session_factory: Callable,
    ) -> None:
        self._base = base
        self._session_factory = session_factory
        self._factory = SAModelFactory(base=base)
        self._built: dict[type, tuple[type, Any]] = {}

    def register(self, *domain_classes: type[DomainModel]) -> None:
        for cls in domain_classes:
            if cls not in self._built:
                self._built[cls] = self._factory.build(cls)

    def get_repository(self, entity_cls: type[D]) -> AsyncRepository[D, Any]:
        from fastrest_sa.repository import AsyncSQLAlchemyRepository

        _, mapper = self._get_built(entity_cls)
        session = self._session_factory()
        return AsyncSQLAlchemyRepository(session=session, mapper=mapper)

    def make_uow(self) -> Any:
        """
        Return a ``SQLAlchemyUnitOfWork`` with all registered repos pre-wired.

        Repository attribute names: ``User`` → ``uow.users``,
        ``Post`` → ``uow.posts``, ``UserRole`` → ``uow.userroles``.
        """
        from fastrest_sa.repository import AsyncSQLAlchemyRepository
        from fastrest_sa.uow import SQLAlchemyUnitOfWork

        repo_factories = {
            _repo_attr(cls): (
                lambda s, m=mapper: AsyncSQLAlchemyRepository(session=s, mapper=m)
            )
            for cls, (_, mapper) in self._built.items()
        }
        return SQLAlchemyUnitOfWork(
            session_factory=self._session_factory,
            repo_factories=repo_factories,
        )

    def _get_built(self, entity_cls: type) -> tuple[type, Any]:
        try:
            return self._built[entity_cls]
        except KeyError:
            raise KeyError(
                f"{entity_cls.__name__!r} is not registered. "
                "Call provider.register(EntityClass) "
                "or provider.autodiscover('myapp.models') first."
            ) from None


def _repo_attr(cls: type) -> str:
    """``User`` → ``'users'``, ``UserRole`` → ``'userroles'``."""
    return cls.__name__.lstrip("_").lower() + "s"
