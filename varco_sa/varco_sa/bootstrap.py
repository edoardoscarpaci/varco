"""
varco_sa.bootstrap
======================
One-stop bootstrap for SQLAlchemy + varco applications.

``SAConfig``
    Frozen value object describing everything needed to create a working
    ``SQLAlchemyRepositoryProvider``: engine, declarative base, entity classes,
    and optional session keyword arguments.

``SAFastrestApp``
    Thin coordinator that wires ``SAConfig`` into the provider, exposes the
    provider as ``uow_provider``, and adds convenience coroutines for schema
    management (``create_all``, ``check_schema``).

Minimal usage::

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import DeclarativeBase
    from varco_sa.bootstrap import SAConfig, SAFastrestApp
    from myapp.domain import Post, User

    class Base(DeclarativeBase): pass

    config = SAConfig(
        engine=create_async_engine("postgresql+asyncpg://..."),
        base=Base,
        entity_classes=(User, Post),
    )
    app = SAFastrestApp(config)
    await app.create_all()  # run once at startup

    # Inject app.uow_provider into services
    service = PostService(uow_provider=app.uow_provider, ...)

DESIGN: SAConfig as a frozen dataclass over keyword arguments on SAFastrestApp
    ✅ Config is serializable and introspectable — useful for logging and
       health endpoints.
    ✅ Config can be built and validated before the app object is created —
       separates construction from initialization.
    ✅ Frozen prevents accidental mutation after construction.
    ❌ Requires importing ``SAConfig`` alongside ``SAFastrestApp`` — minor
       added ceremony compared to passing kwargs directly.

Thread safety:  ⚠️ ``SAFastrestApp.__init__`` calls ``provider.register()``
                which is write-safe at startup but not mid-request.
Async safety:   ✅ ``create_all`` and ``check_schema`` are ``async def``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from varco_core.model import DomainModel
from varco_sa.provider import SQLAlchemyRepositoryProvider
from varco_sa.schema_guard import SchemaGuard, SchemaDriftReport

if TYPE_CHECKING:
    # IUoWProvider is used as the type for uow_provider — guard to avoid
    # importing the core hierarchy at runtime for users who only need SAConfig.
    pass


# ── SAConfig ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SAConfig:
    """
    Immutable configuration for an SA-backed varco application.

    Attributes:
        engine:           Async SQLAlchemy engine.  Use
                          ``create_async_engine("dialect+driver://...")`` to
                          create one.
        base:             Shared ``DeclarativeBase`` subclass.  All SA ORM
                          classes generated from ``entity_classes`` are
                          attached to this base.
        entity_classes:   Domain model classes to register.  Passed directly
                          to ``provider.register()`` at construction time.
        session_options:  Keyword arguments forwarded to ``async_sessionmaker``.
                          Common keys: ``expire_on_commit`` (default: ``False``),
                          ``autoflush``.

    Thread safety:  ✅ frozen=True — immutable after construction.
    Async safety:   ✅ Pure configuration value object; no I/O.

    Edge cases:
        - ``entity_classes`` is stored as a ``tuple`` — ordering matters only
          for ``SAModelFactory.build()`` call order, not for correctness.
        - ``session_options`` defaults to ``{"expire_on_commit": False}``
          which is the recommended setting for async SQLAlchemy (avoids lazy
          loading errors after commit).

    Example::

        SAConfig(
            engine=create_async_engine("postgresql+asyncpg://user:pw@host/db"),
            base=Base,
            entity_classes=(User, Post, Comment),
            session_options={"expire_on_commit": False, "autoflush": False},
        )
    """

    # Connected async engine — the source of database sessions
    engine: AsyncEngine

    # Shared declarative base — all generated ORM classes inherit from this
    base: type[DeclarativeBase]

    # Domain classes to register at startup
    entity_classes: tuple[type[DomainModel], ...]

    # Extra keyword args for async_sessionmaker.
    # DESIGN: default expire_on_commit=False — prevents implicit lazy-load
    # errors after commit in async contexts where I/O is not allowed.
    session_options: dict[str, Any] = field(
        default_factory=lambda: {"expire_on_commit": False}
    )


# ── SAFastrestApp ─────────────────────────────────────────────────────────────


class SAFastrestApp:
    """
    Bootstrap coordinator for SQLAlchemy + varco applications.

    Wires ``SAConfig`` into ``SQLAlchemyRepositoryProvider``, registers all
    entity classes, and exposes the provider as ``uow_provider`` for injection
    into services.

    Attributes:
        uow_provider: Ready-to-use ``SQLAlchemyRepositoryProvider``.
                      Inject this into ``AsyncService.__init__`` via
                      ``Inject[IUoWProvider]`` or pass directly.

    Thread safety:  ⚠️ Construct once at startup; ``uow_provider`` is safe
                    to share across concurrent request handlers after init.
    Async safety:   ✅ ``create_all`` and ``check_schema`` are ``async def``.

    Edge cases:
        - ``create_all()`` uses ``Base.metadata.create_all()`` — does NOT run
          Alembic migrations.  Use it only for tests or prototypes; prefer
          Alembic for production schema management.
        - ``check_schema()`` reports drift but does NOT auto-migrate.  Raise
          on ``SchemaDriftReport.has_drift`` to block startup in production.

    Example::

        app = SAFastrestApp(config)
        await app.create_all()         # creates tables if they don't exist

        # Non-raising health check:
        report = await app.check_schema()
        if report.has_drift:
            logger.error(report.format())
    """

    def __init__(self, config: SAConfig) -> None:
        """
        Construct the app and register all entity classes.

        Args:
            config: Fully specified ``SAConfig`` instance.

        Edge cases:
            - Registration calls ``SAModelFactory.build()`` for each entity
              class.  This is O(n) at startup; negligible in practice.
        """
        # Merge caller's session_options — expire_on_commit=False is already
        # in the config default, but callers may override it.
        session_factory = async_sessionmaker(config.engine, **config.session_options)

        # Build the provider and register all entity classes immediately.
        # Registration must happen before any make_uow() call.
        self._provider = SQLAlchemyRepositoryProvider(
            base=config.base,
            session_factory=session_factory,
        )
        self._provider.register(*config.entity_classes)

        # Keep config for schema helpers that need the engine and base
        self._config = config

    @property
    def uow_provider(self) -> SQLAlchemyRepositoryProvider:
        """
        Return the ready-to-use ``SQLAlchemyRepositoryProvider``.

        Inject this into service constructors::

            PostService(uow_provider=app.uow_provider, authorizer=..., assembler=...)

        Returns:
            The configured ``SQLAlchemyRepositoryProvider``.
        """
        return self._provider

    async def create_all(self) -> None:
        """
        Create all registered tables in the database if they do not exist.

        Uses ``Base.metadata.create_all()`` — safe for new tables but does NOT
        run incremental migrations.  Suitable for tests and early prototypes;
        use Alembic for production schema management.

        Edge cases:
            - Tables that already exist are not modified (``checkfirst=True``
              is the SQLAlchemy default for ``create_all``).
            - Index and constraint definitions are applied on first creation
              only — pre-existing tables are not updated.

        Async safety:   ✅ Uses ``conn.run_sync()`` to call the synchronous
                        ``create_all`` API without blocking the event loop.
        """
        async with self._config.engine.begin() as conn:
            # run_sync delegates sync SQLAlchemy I/O to the async driver
            # without blocking the event loop.
            await conn.run_sync(self._config.base.metadata.create_all)

    async def check_schema(self) -> SchemaDriftReport:
        """
        Compare expected schema (metadata) against the live database.

        Returns a ``SchemaDriftReport`` describing missing tables and columns.
        Does NOT modify the database.

        Returns:
            ``SchemaDriftReport`` — inspect ``has_drift`` and call ``format()``
            for a human-readable diff summary.

        Raises:
            SchemaDrift: If ``SchemaGuard.check()`` is used instead of
                         ``SchemaGuard.report()`` — see ``SchemaGuard`` docs.

        Async safety:   ✅ Delegates to ``SchemaGuard.report()`` which uses
                        ``conn.run_sync()`` internally.

        Example::

            report = await app.check_schema()
            if report.has_drift:
                raise RuntimeError(f"Schema drift detected:\\n{report.format()}")
        """
        guard = SchemaGuard(self._config.base)
        return await guard.report(self._config.engine)


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SAConfig",
    "SAFastrestApp",
]
