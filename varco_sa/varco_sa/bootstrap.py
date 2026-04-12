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

import os
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import async_sessionmaker

# SAConfig moved to config.py to break the circular import:
# provider.py → Inject[SAConfig] must not import bootstrap.py
# (which imports provider.py).  Both now import from config.py.
from varco_sa.config import SAConfig  # noqa: F401 — re-exported for backward compat
from varco_sa.pool_metrics import SAPoolMetrics, pool_metrics as _pool_metrics
from varco_sa.provider import SQLAlchemyRepositoryProvider
from varco_sa.schema_guard import SchemaGuard, SchemaDriftReport

if TYPE_CHECKING:
    from sqlalchemy.orm import DeclarativeBase
    from varco_core.model import DomainModel

# Re-export SAConfig at the module level so ``from varco_sa.bootstrap import SAConfig``
# keeps working for existing code that used the old location.
__all__ = ["SAConfig", "SAFastrestApp", "make_sa_provider"]


# ── make_sa_provider ──────────────────────────────────────────────────────────


def make_sa_provider(
    base: type[DeclarativeBase],
    *entity_classes: type[DomainModel],
    env_var: str = "DATABASE_URL",
    echo: bool = False,
    session_options: dict[str, Any] | None = None,
) -> Any:
    """
    Build a ``@Provider``-decorated factory that creates an ``SAConfig``
    from the ``DATABASE_URL`` environment variable.

    This is a one-line alternative to the boilerplate ``@Provider`` + engine +
    ``SAConfig`` pattern::

        # Before (13 lines):
        @Provider(singleton=True)
        def _sa_config() -> SAConfig:
            return SAConfig(
                engine=create_async_engine(os.environ["DATABASE_URL"], echo=False),
                base=Base,
                entity_classes=(Post,),
            )
        container.provide(_sa_config)

        # After (1 line):
        container.provide(make_sa_provider(Base, Post))

    The returned provider is lazy — the engine and ``SAConfig`` are not
    constructed until the first time ``DIContainer.get(SAConfig)`` is called.

    Args:
        base:            Shared ``DeclarativeBase`` subclass.  All SA ORM
                         classes generated from ``entity_classes`` are
                         attached to this base.
        *entity_classes: ``DomainModel`` subclasses to register.  Forwarded
                         to ``SAConfig.entity_classes``.
        env_var:         Name of the environment variable holding the
                         database URL.  Defaults to ``"DATABASE_URL"``.
        echo:            Forward to ``create_async_engine(echo=...)``.
                         Set ``True`` for verbose SQL logging in development.
        session_options: Extra kwargs forwarded to ``async_sessionmaker``.
                         Defaults to ``{"expire_on_commit": False}`` when
                         ``None``.

    Returns:
        A ``@Provider(singleton=True)``-decorated factory function ready to
        pass to ``container.provide()``.

    Raises:
        KeyError: ``DATABASE_URL`` (or the custom ``env_var``) is not set at
                  provider resolution time.

    Edge cases:
        - The env var is read lazily (at first resolution), not at call time —
          the function can be called before the environment is fully configured.
        - ``entity_classes`` is captured in a closure over the tuple at call
          time — adding new entity classes after this call has no effect.

    Thread safety:  ✅ Provider is called once (singleton=True).
    Async safety:   ✅ Factory is synchronous — no async I/O at construction.

    Example::

        container = DIContainer()
        container.provide(make_sa_provider(Base, Post, User))
        sa_bootstrap(container)   # scan + bind repos
    """
    try:
        from providify import Provider  # noqa: PLC0415
    except ImportError:
        raise ImportError(
            "make_sa_provider requires providify. "
            "Install it with: pip install providify"
        ) from None

    from sqlalchemy.ext.asyncio import create_async_engine  # noqa: PLC0415

    # Capture all args in the closure — the factory is called lazily later.
    _base = base
    _entity_classes = tuple(entity_classes)
    _env_var = env_var
    _echo = echo
    _session_options = session_options

    @Provider(singleton=True)
    def _sa_config_factory() -> SAConfig:
        """Auto-generated SAConfig provider reading DATABASE_URL from env."""
        url = os.environ[_env_var]
        return SAConfig(
            engine=create_async_engine(url, echo=_echo),
            base=_base,
            entity_classes=_entity_classes,
            # When session_options is None, let SAConfig use its own default
            # ({"expire_on_commit": False}) by not passing the field at all.
            **(
                {"session_options": _session_options}
                if _session_options is not None
                else {}
            ),
        )

    return _sa_config_factory


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

        # Use the legacy keyword-arg path — SAFastrestApp is a non-DI coordinator
        # that constructs the provider directly with an already-built session factory.
        self._provider = SQLAlchemyRepositoryProvider(
            base=config.base,
            session_factory=session_factory,
        )
        self._provider.register(*config.entity_classes)

        # Keep config for schema helpers that need the engine and base.
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

    def pool_metrics(self) -> SAPoolMetrics:
        """
        Return a snapshot of the connection pool statistics.

        Reads pool counters synchronously from the underlying ``AsyncEngine``.
        Safe to call from any async context (no I/O performed).

        Returns:
            ``SAPoolMetrics`` snapshot with counts of checked-out, idle, and
            overflow connections, plus a saturation flag.

        Thread safety:  ✅ Stateless read — no mutations.
        Async safety:   ✅ Synchronous; call from any context.

        Edge cases:
            - ``NullPool`` (e.g. test engines): all counters are 0.
            - Snapshot is stale the moment it is returned — use for monitoring,
              not for admission control.

        Example::

            metrics = app.pool_metrics()
            if metrics.is_saturated:
                logger.error("DB pool saturated: %s", metrics)
        """
        return _pool_metrics(self._config.engine)


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SAConfig",
    "SAFastrestApp",
]
