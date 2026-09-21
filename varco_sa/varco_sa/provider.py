"""
varco_sa.provider
=====================
Concrete ``RepositoryProvider`` for SQLAlchemy async.
"""

from __future__ import annotations

from typing import Annotated, Any, TypeVar

from providify import Fallback, InjectMeta, Singleton
from sqlalchemy.ext.asyncio import async_sessionmaker
from varco_core.di import BACKEND_PRIORITY
from varco_core.model import DomainModel
from varco_core.providers import RepositoryProvider
from varco_core.repository import AsyncRepository

from varco_sa.config import SAConfig
from varco_sa.factory import SAModelFactory

D = TypeVar("D", bound=DomainModel)


@Fallback
@Singleton(priority=BACKEND_PRIORITY, qualifier="sa")
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

        provider = SQLAlchemyRepositoryProvider.from_components(
            base=Base, session_factory=sessions
        )
        provider.register(User, Post)   # ← or autodiscover("myapp.models")

        async with provider.make_uow() as uow:
            user = await uow.users.save(User(name="Edo", email="..."))

    DESIGN: one ``SAModelFactory`` per provider instance
      ✅ Generated ORM classes share the same ``DeclarativeBase`` — required
         for ``Base.metadata.create_all()`` to include all tables
      ✅ Factory cache is scoped to the provider instance
      ❌ Two providers with the same ``Base`` would conflict on tablenames —
         use one provider per ``Base`` per process

    When constructing directly (without DI), use :meth:`from_components` and
    pass ``base`` (shared ``DeclarativeBase`` subclass) and ``session_factory``
    (``async_sessionmaker`` or any ``() → AsyncSession``).

    Edge cases:
        - Call ``register()`` / ``autodiscover()`` before
          ``Base.metadata.create_all()`` so all generated tables are included.
        - ``make_uow()`` derives repo attribute names automatically:
          ``User`` → ``uow.users``, ``UserRole`` → ``uow.userroles``.
    """

    def __init__(
        self,
        config: Annotated[SAConfig | None, InjectMeta(optional=True)] = None,
    ) -> None:
        """
        DI constructor — builds the provider from an injected ``SAConfig``.

        This is the path the providify container uses: it resolves ``SAConfig``
        and passes it here.  For direct (non-DI) construction with an explicit
        base + session factory, use :meth:`from_components` instead.

        Args:
            config: Injected ``SAConfig`` — provides the engine, declarative
                    base, entity classes, and session options.  ``optional=True``
                    so the container does not fail when no ``SAConfig`` is bound;
                    in that case this constructor raises ``TypeError`` to point
                    the caller at :meth:`from_components`.

        Raises:
            TypeError: ``config`` is ``None`` (no ``SAConfig`` bound).  Use
                       :meth:`from_components` for direct construction.

        Example:
            # DI: the container injects SAConfig automatically.
            provider = container.get(SQLAlchemyRepositoryProvider)
        """
        if config is None:
            # No SAConfig bound — the caller is constructing directly without DI.
            # Direct construction belongs in from_components(), which bypasses
            # this DI-only constructor; guide them there instead of guessing.
            raise TypeError(
                "SQLAlchemyRepositoryProvider() requires an injected ``SAConfig``. "
                "For direct (non-DI) construction use "
                "SQLAlchemyRepositoryProvider.from_components(base=..., "
                "session_factory=...)."
            )

        # DI path: derive the session factory from the engine, then converge on
        # the shared field-init used by from_components().
        session_factory = async_sessionmaker(
            config.engine,
            **config.session_options,
        )
        self._init_from(config.base, session_factory)
        # Register entities upfront so ORM tables are mapped before the
        # first make_uow() call.
        if config.entity_classes:
            self.register(*config.entity_classes)

    @classmethod
    def from_components(
        cls,
        *,
        base: Any,
        session_factory: Any,
    ) -> SQLAlchemyRepositoryProvider:
        """
        Build a provider directly from a declarative base + session factory.

        This is the non-DI / test / manual-bootstrap entry point (used by
        ``SAFastrestApp`` and unit tests).  It bypasses the DI-only ``__init__``
        via ``cls.__new__`` so there is no ``SAConfig`` requirement.

        DESIGN: classmethod over a dual-path ``__init__``
            ✅ ``__init__`` stays single-responsibility — the DI path only.
            ✅ Direct construction is explicit and self-documenting at call sites.
            ✅ Both paths converge on ``_init_from`` — one place builds the state.
            ❌ Uses ``cls.__new__`` to skip ``__init__`` — a deliberate, documented
               bypass (the only way to keep ``__init__`` DI-only while providify
               always calls it with ``config``).

        Args:
            base:            Shared ``DeclarativeBase`` subclass.
            session_factory: ``async_sessionmaker`` or any ``() → AsyncSession``.

        Returns:
            A ready provider.  Call ``register()`` / ``autodiscover()`` to map
            entities before ``make_uow()``.

        Example:
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            provider = SQLAlchemyRepositoryProvider.from_components(
                base=Base, session_factory=sessions
            )
            provider.register(User, Post)
        """
        # Bypass the DI-only __init__ (which requires a SAConfig).
        self = cls.__new__(cls)
        self._init_from(base, session_factory)
        return self

    def _init_from(self, base: Any, session_factory: Any) -> None:
        """
        Set the four instance fields shared by both construction paths.

        Args:
            base:            Declarative base the model factory builds against.
            session_factory: Callable returning an ``AsyncSession``.
        """
        self._base = base
        self._session_factory = session_factory
        self._factory = SAModelFactory(base=base)
        self._built: dict[type, tuple[type, Any]] = {}

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

        # `m=mapper` binds the loop variable eagerly (the classic Python
        # late-binding-closure trap) — see varco_beanie.provider's identical
        # pattern for the same reasoning. Explicit `dict[str, Any]` avoids
        # mypy misreading the lambda's defaulted extra param as a second
        # required positional.
        repo_factories: dict[str, Any] = {
            _repo_attr(cls): (lambda s, m=mapper: AsyncSQLAlchemyRepository(session=s, mapper=m))
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
