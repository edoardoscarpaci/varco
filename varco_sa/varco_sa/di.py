"""
varco_sa.di
===========
Providify DI integration for the SQLAlchemy async backend.

``SQLAlchemyRepositoryProvider``, ``SAHealthCheck``, and
per-entity ``AsyncRepository[D]`` bindings are wired into a ``DIContainer``
with a minimal setup API.

``SAModule`` is a scan-marker ``@Configuration``.  The concrete singletons
(``SQLAlchemyRepositoryProvider``, ``SAHealthCheck``) are registered
automatically when ``container.scan("varco_sa")`` is called — no explicit
``@Provider`` factories are needed.

Typical usage::

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import DeclarativeBase

    from providify import DIContainer, Provider
    from varco_sa.bootstrap import SAConfig
    from varco_sa.di import SAModule, bind_repositories
    from myapp.models import User, Post          # your DomainModel subclasses

    class Base(DeclarativeBase): pass

    container = DIContainer()

    # 1. Provide SAConfig — injected into SQLAlchemyRepositoryProvider and SAHealthCheck
    @Provider(singleton=True)
    def sa_config() -> SAConfig:
        return SAConfig(
            engine=create_async_engine("postgresql+asyncpg://..."),
            base=Base,
            entity_classes=(User, Post),
        )

    container.provide(sa_config)

    # 2. Install the module (scan-marker) and scan varco_sa
    container.install(SAModule)
    container.scan("varco_sa", recursive=True)

    # 3. Bind per-entity AsyncRepository[D] — must come after scan
    bind_repositories(container, User, Post)

    # 4. Resolve anywhere in your app
    repo = await container.aget(AsyncRepository[User])

Thread safety:  ✅ All binding registrations happen at startup before concurrent access.
Async safety:   ✅ All providers are synchronous — SQLAlchemy has no async
                    init step equivalent to Beanie's init_beanie().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from providify import Configuration, Inject, Provider
from varco_core.model import DomainModel
from varco_core.providers import RepositoryProvider
from varco_core.repository import AsyncRepository
from varco_core.service.base import IUoWProvider

if TYPE_CHECKING:
    # Avoid a hard circular import — DIContainer is only needed for the
    # bind_repositories() type hint, not at runtime.
    from providify import DIContainer


# ── Configuration module ──────────────────────────────────────────────────────


@Configuration
class SAModule:
    """
    Providify ``@Configuration`` module for the SQLAlchemy async backend.

    ``SQLAlchemyRepositoryProvider`` and ``SAHealthCheck`` are registered
    automatically via their ``@Singleton`` decorators when
    ``container.scan("varco_sa")`` is called.  This module serves as a
    scan marker and provides the ``bind_repositories()`` helper.

    Per-entity ``AsyncRepository[D]`` bindings are NOT added here — call
    ``bind_repositories(container, *entity_classes)`` after ``install()``.
    They are separate because the set of entity classes is determined at
    app startup, not hardcoded in the module.

    Prerequisites
    -------------
    ``SAConfig`` must be registered in the container before calling
    ``container.install(SAModule)`` — it is injected into
    ``SQLAlchemyRepositoryProvider`` and ``SAHealthCheck`` via ``Inject[SAConfig]``.

    Thread safety:  ✅ Module instance is created once at install() time.
    Async safety:   ✅ All providers are synchronous — SQLAlchemy requires
                       no async initialisation at construction time.
    """

    @Provider(singleton=True)
    def uow_provider(self, repo_provider: Inject[RepositoryProvider]) -> IUoWProvider:
        """
        Expose ``SQLAlchemyRepositoryProvider`` as the ``IUoWProvider`` interface.

        ``AsyncService.__init__`` injects ``IUoWProvider`` — this binding
        satisfies that requirement automatically when ``SAModule`` is installed.

        DESIGN: explicit provider over RepositoryProvider extending IUoWProvider
            ✅ Avoids a circular import:
               ``providers.py`` → ``service.base`` → ``service/__init__``
               → ``service/tenant`` → ``providers.py``.
            ✅ DI container resolves ``IUoWProvider`` independently of
               ``RepositoryProvider`` — both types remain injectable separately.
            ✅ The ``RepositoryProvider`` singleton is reused — no second
               instance is created.
            ❌ Requires this explicit @Provider method on SAModule;
               cannot be auto-discovered by scanning alone.

        Returns:
            The same ``RepositoryProvider`` singleton, typed as ``IUoWProvider``.
        """
        # Return the same singleton — RepositoryProvider's make_uow() satisfies
        # IUoWProvider without any wrapping.
        return repo_provider


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
