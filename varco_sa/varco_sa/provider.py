"""
varco_sa.provider
=====================
Concrete ``RepositoryProvider`` for SQLAlchemy async.
"""

from __future__ import annotations

import sys
from typing import Any, TypeVar

from providify import InjectMeta, Singleton
from typing import Annotated
from sqlalchemy.ext.asyncio import async_sessionmaker

from varco_sa.config import SAConfig
from varco_sa.factory import SAModelFactory
from varco_core.model import DomainModel
from varco_core.providers import RepositoryProvider
from varco_core.repository import AsyncRepository

D = TypeVar("D", bound=DomainModel)


@Singleton(priority=-sys.maxsize, qualifier="sa")
class SQLAlchemyRepositoryProvider(RepositoryProvider):
    """
    ``RepositoryProvider`` backed by SQLAlchemy async.

    Usage::

        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
        from sqlalchemy.orm import DeclarativeBase
        from varco_sa.provider import SQLAlchemyRepositoryProvider

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
        config: Annotated[SAConfig, InjectMeta(optional=True)] = None,
        *,
        base: Any | None = None,
        session_factory: Any | None = None,
    ) -> None:
        """
        Args:
            config:          Injected ``SAConfig`` — provides the engine,
                             declarative base, entity classes, and session
                             options.  Used by the DI container.
            base:            Legacy keyword arg — ``DeclarativeBase`` subclass.
                             Accepted for backward compatibility with direct
                             construction (tests, non-DI usage).
            session_factory: Legacy keyword arg — ``async_sessionmaker`` or
                             any ``() → AsyncSession`` callable.

        DESIGN: dual-path constructor over separate factory method
            ✅ Backward-compatible — existing tests and ``SAFastrestApp`` pass
               ``base`` / ``session_factory`` directly and keep working.
            ✅ DI path uses ``Inject[SAConfig]`` — single clean injection point.
            ❌ Two code paths — accepted to avoid breaking the public API.

        Raises:
            TypeError: Neither ``config`` nor (``base`` + ``session_factory``)
                       is provided.
        """
        if config is not None:
            # DI path: everything from the injected SAConfig value object.
            self._base = config.base
            self._session_factory = async_sessionmaker(
                config.engine,
                **config.session_options,
            )
            self._factory = SAModelFactory(base=config.base)
            self._built: dict[type, tuple[type, Any]] = {}
            # Register entities upfront so ORM tables are mapped
            # before the first make_uow() call.
            if config.entity_classes:
                self.register(*config.entity_classes)
        elif base is not None and session_factory is not None:
            # Legacy path: direct construction with explicit base + factory.
            # Used by SAFastrestApp, tests, and non-DI bootstrap code.
            self._base = base
            self._session_factory = session_factory
            self._factory = SAModelFactory(base=base)
            self._built = {}
        else:
            raise TypeError(
                "SQLAlchemyRepositoryProvider requires either a ``SAConfig`` "
                "injected via DI or explicit ``base`` + ``session_factory`` "
                "keyword arguments for direct construction."
            )

    def register(self, *domain_classes: type[DomainModel]) -> None:
        for cls in domain_classes:
            if cls not in self._built:
                self._built[cls] = self._factory.build(cls)

    def get_repository(self, entity_cls: type[D]) -> AsyncRepository[D, Any]:
        from varco_sa.repository import AsyncSQLAlchemyRepository

        _, mapper = self._get_built(entity_cls)
        session = self._session_factory()
        return AsyncSQLAlchemyRepository(session=session, mapper=mapper)

    def make_uow(self) -> Any:
        """
        Return a ``SQLAlchemyUnitOfWork`` with all registered repos pre-wired.

        Repository attribute names: ``User`` → ``uow.users``,
        ``Post`` → ``uow.posts``, ``UserRole`` → ``uow.userroles``.
        """
        from varco_sa.repository import AsyncSQLAlchemyRepository
        from varco_sa.uow import SQLAlchemyUnitOfWork

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
