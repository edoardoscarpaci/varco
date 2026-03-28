"""
varco_sa.di
===========
Providify DI integration for the SQLAlchemy async backend.

Wires ``SQLAlchemyRepositoryProvider``, ``SQLAlchemyQueryApplicator``, and
per-entity ``AsyncRepository[D]`` bindings into a ``DIContainer`` with a
minimal setup API.

This mirrors the ``BeanieModule`` pattern from ``varco_beanie.di`` exactly,
so apps that run both backends experience a consistent wiring ceremony.

Typical usage::

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import DeclarativeBase

    from providify import DIContainer, Provider
    from varco_sa.bootstrap import SAConfig
    from varco_sa.di import SAModule, bind_repositories
    from myapp.models import User, Post          # your DomainModel subclasses

    class Base(DeclarativeBase): pass

    container = DIContainer()

    # 1. Provide configuration (sync @Provider — install() stays synchronous)
    @Provider(singleton=True)
    def sa_config() -> SAConfig:
        return SAConfig(
            engine=create_async_engine("postgresql+asyncpg://..."),
            base=Base,
            entity_classes=(User, Post),
        )

    container.provide(sa_config)

    # 2. Install the module — injects SAConfig, registers providers
    container.install(SAModule)

    # 3. Bind per-entity AsyncRepository[D] — must come after install()
    bind_repositories(container, User, Post)

    # 4. Resolve anywhere in your app
    repo = await container.aget(AsyncRepository[User])

DESIGN: reuse SAConfig as the injectable settings object rather than
introducing a parallel SASettings dataclass
    ✅ SAConfig is already exported, well-documented, and frozen — it is
       the natural configuration value object for this backend.
    ✅ Avoids a second "settings vs config" duality that would confuse users.
    ❌ Ties the DI wiring to varco_sa.bootstrap — if SAConfig ever changes,
       SAModule.repository_provider() must be updated.  Acceptable: they
       evolve together.

Thread safety:  ✅ All binding registrations happen at startup before concurrent access.
Async safety:   ✅ repository_provider is synchronous — SQLAlchemy has no async
                    init step equivalent to Beanie's init_beanie().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from providify import Configuration, Provider
from varco_core.health import HealthCheck
from varco_core.model import DomainModel
from varco_core.providers import RepositoryProvider
from varco_core.repository import AsyncRepository
from varco_core.query.applicator.sqlalchemy import SQLAlchemyQueryApplicator
from varco_sa.bootstrap import SAConfig
from varco_sa.health import SAHealthCheck
from varco_sa.provider import SQLAlchemyRepositoryProvider

if TYPE_CHECKING:
    # Avoid a hard circular import — DIContainer is only needed for the
    # bind_repositories() type hint, not at runtime.
    from providify import DIContainer


# ── Configuration module ──────────────────────────────────────────────────────


@Configuration
class SAModule:
    """
    Providify ``@Configuration`` module for the SQLAlchemy async backend.

    When installed in a ``DIContainer``, registers the following bindings:

    +-------------------------------+--------+-------+
    | Type                          | Scope  | Async |
    +===============================+========+=======+
    | ``RepositoryProvider``        | SINGLE | no    |
    +-------------------------------+--------+-------+
    | ``SQLAlchemyQueryApplicator`` | SINGLE | no    |
    +-------------------------------+--------+-------+

    Per-entity ``AsyncRepository[D]`` bindings are NOT added here — call
    ``bind_repositories(container, *entity_classes)`` after ``install()``.
    They are separate because the set of entity classes is determined at
    app startup, not hardcoded in the module.

    Prerequisites
    -------------
    ``SAConfig`` must be registered in the container before calling
    ``container.install(SAModule)`` — the container injects it into
    this class's ``__init__``.

    Thread safety:  ✅ Module instance is created once at install() time.
    Async safety:   ✅ Both providers are synchronous — SQLAlchemy requires
                       no async initialisation at construction time.

    Args:
        config: Injected ``SAConfig`` from the container.
    """

    def __init__(self, config: SAConfig) -> None:
        # config is injected by the container at install() time.
        # Stored so @Provider methods can access engine, base, entity_classes.
        self._config = config

    @Provider(singleton=True)
    def repository_provider(self) -> RepositoryProvider:
        """
        Create, configure, and return the ``SQLAlchemyRepositoryProvider``.

        Builds an ``async_sessionmaker`` from the injected engine and session
        options, then registers all ``entity_classes`` so that ORM tables
        are mapped before the first ``make_uow()`` call.

        DESIGN: sync @Provider over async
            SQLAlchemy's ``async_sessionmaker`` and ``SQLAlchemyRepositoryProvider``
            are pure in-memory construction — no network I/O or async init step
            exists.  A sync provider keeps ``container.install()`` synchronous,
            allowing callers to use ``install()`` instead of ``ainstall()``.
            ✅ Simpler setup — no ``await container.ainstall(SAModule)`` required.
            ✅ ``async_sessionmaker`` and the provider are reusable across tasks.
            ❌ If a future SA version adds async engine initialisation, this
               will need to become async.  Track SQLAlchemy changelog.

        Returns:
            A fully configured ``SQLAlchemyRepositoryProvider`` cast to the
            abstract ``RepositoryProvider`` interface — callers should inject
            ``RepositoryProvider``, not the concrete type.

        Edge cases:
            - If ``entity_classes`` is empty in the config, no entities are
              registered.  Call ``provider.register(*classes)`` manually before
              the first ``make_uow()`` or add them to ``SAConfig``.
            - ``session_options`` defaults to ``{"expire_on_commit": False}``
              in ``SAConfig`` — prevents lazy-load errors after commit in async
              contexts.  Do not override this unless you understand the implications.
        """
        # Build the session factory once — shared across all UoW instances
        # created by this provider.  expire_on_commit=False is in the SAConfig
        # default, matching the recommended async SQLAlchemy setup.
        session_factory = async_sessionmaker(
            self._config.engine,
            **self._config.session_options,
        )

        provider = SQLAlchemyRepositoryProvider(
            base=self._config.base,
            session_factory=session_factory,
        )

        if self._config.entity_classes:
            # Register all domain classes upfront — ORM table mappings are
            # generated lazily by SAModelFactory on first register() call.
            provider.register(*self._config.entity_classes)

        return provider

    @Provider(singleton=True)
    def query_applicator(self) -> SQLAlchemyQueryApplicator:
        """
        Provide a shared ``SQLAlchemyQueryApplicator`` instance.

        ``SQLAlchemyQueryApplicator`` is stateless — all public methods are pure
        functions of their inputs — so a single instance is safe to share
        across all repositories and concurrent requests.

        Returns:
            A singleton ``SQLAlchemyQueryApplicator``.

        Thread safety:  ✅ Stateless — no instance state to protect.
        Async safety:   ✅ All visitor methods are synchronous.
        """
        # DESIGN: singleton over creating a new applicator per repository call
        #   ✅ Zero allocation overhead at query time
        #   ✅ Thread-safe because there is no mutable state
        #   ❌ If a future subclass adds state, callers must be updated
        return SQLAlchemyQueryApplicator()

    @Provider(singleton=True)
    def sa_health_check(self) -> HealthCheck:
        """
        Provide an ``SAHealthCheck`` for liveness/readiness probes.

        Uses the same engine already stored in ``self._config`` — no additional
        configuration is needed.

        Returns:
            An ``SAHealthCheck`` bound to the ``HealthCheck`` interface.

        Example::

            check = container.get(HealthCheck)
            result = await check.check()
            assert result.status == HealthStatus.HEALTHY
        """
        # Sync provider — SAHealthCheck holds only the engine reference;
        # no async init step is required.
        return SAHealthCheck(self._config.engine)


# ── Per-entity repository binding helper ──────────────────────────────────────


def bind_repositories(
    container: DIContainer,
    *entity_classes: type[DomainModel],
) -> None:
    """
    Register an ``AsyncRepository[D]`` binding for each domain model class.

    After calling this, ``await container.aget(AsyncRepository[User])``
    resolves to a ``SQLAlchemyRepositoryProvider``-backed repository for
    ``User``.

    Each factory is a **synchronous DEPENDENT-scoped** ``@Provider`` — a fresh
    repository instance is returned per resolution.  Repositories are stateless
    wrappers around a newly-created ``AsyncSession``, so this is safe.

    DESIGN: per-entity @Provider functions over a single generic factory
      ✅ Each binding has a concrete generic alias (AsyncRepository[User])
         that the container can match exactly via _is_generic_subtype()
      ✅ Works with container.aget(AsyncRepository[User]) type resolution
      ❌ N @Provider registrations for N entity classes — scales linearly
         with the model count, but that is typically small (<50)

    Prerequisites
    -------------
    ``SAModule`` must be installed before calling this function —
    the generated providers inject ``RepositoryProvider`` which is registered
    by ``SAModule.repository_provider()``.

    Args:
        container:       The ``DIContainer`` to register bindings into.
        *entity_classes: One or more ``DomainModel`` subclasses.

    Raises:
        ValueError: Called with no ``entity_classes``.

    Edge cases:
        - Calling twice with the same entity class adds a second binding.
          The container will pick the higher-priority one; avoid duplicates.
        - Empty ``entity_classes`` raises immediately — likely a programming error.

    Example::

        bind_repositories(container, User, Post, Tag)
        repo = await container.aget(AsyncRepository[User])

    Thread safety:  ✅ Called once at startup before concurrent access.
    Async safety:   ✅ The generated providers are synchronous — a fresh
                       ``AsyncSession`` is created per resolution but no
                       I/O happens at construction time.
    """
    if not entity_classes:
        raise ValueError(
            "bind_repositories() requires at least one entity class. "
            "Example: bind_repositories(container, User, Post)"
        )

    for entity_cls in entity_classes:
        container.provide(_make_repo_provider(entity_cls))


def _make_repo_provider(entity_cls: type[DomainModel]) -> Any:
    """
    Build a ``@Provider``-decorated sync factory for ``AsyncRepository[entity_cls]``.

    The factory's return-type annotation is patched at runtime so that
    providify registers the binding under the precise generic alias
    ``AsyncRepository[entity_cls]`` (e.g. ``AsyncRepository[User]``).

    DESIGN: dynamic annotation patching over Protocol / overloads
      ✅ No boilerplate per entity — one call per class
      ✅ providify's _is_generic_subtype() matches on the generic alias
      ❌ Annotation patching is non-standard and invisible to static checkers
         (mypy/pyright will not infer the return type from __annotations__)

    Args:
        entity_cls: The ``DomainModel`` subclass to build a provider for.

    Returns:
        A function with ``@Provider`` metadata stamped and the return
        annotation set to ``AsyncRepository[entity_cls]``.

    Thread safety:  ✅ Pure function — creates a new closure each call.
    Async safety:   ✅ The returned factory is synchronous — repository
                       construction itself is synchronous; I/O is lazy.
    """

    def _repo_factory(provider: RepositoryProvider) -> AsyncRepository:  # type: ignore[type-arg]
        # provider is injected by the container (resolved as RepositoryProvider
        # singleton from SAModule.repository_provider()).
        # get_repository() returns the correct AsyncSQLAlchemyRepository
        # subtype for entity_cls, creating a fresh AsyncSession per call.
        return provider.get_repository(entity_cls)

    # Patch the return annotation with the concrete generic alias so providify
    # registers this binding under AsyncRepository[entity_cls], not the bare
    # unparameterised AsyncRepository.  Without this patch, all entity repos
    # would collide under the same unparameterised interface.
    _repo_factory.__annotations__["return"] = AsyncRepository[entity_cls]

    # Give the closure a descriptive __name__ for debugging / describe() output.
    _repo_factory.__name__ = f"_repo_factory_{entity_cls.__name__}"

    # Stamp @Provider metadata — DEPENDENT scope (default) so a fresh repo
    # wrapper is returned each time; AsyncSession is created per resolution.
    return Provider(_repo_factory)


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SAModule",
    "bind_repositories",
]
