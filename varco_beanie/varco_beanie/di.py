"""
varco_beanie.di
===================
Providify DI integration for the Beanie (pymongo / MongoDB) backend.

``BeanieRepositoryProvider``, ``BeanieQueryCompiler``, and ``BeanieHealthCheck``
are registered automatically via their ``@Singleton`` decorators when
``container.scan("varco_beanie")`` is called.  ``BeanieModule`` is a
scan-marker ``@Configuration`` — it no longer contains ``@Provider`` factories
for classes that can self-register.

Typical usage::

    from pymongo import AsyncMongoClient
    from providify import DIContainer, Provider

    from varco_beanie.di import BeanieModule, bind_repositories
    from varco_beanie.config import BeanieSettings
    from myapp.models import User, Post          # your DomainModel subclasses

    container = DIContainer()

    # 1. Provide settings — injected into BeanieRepositoryProvider and BeanieHealthCheck
    @Provider(singleton=True)
    def beanie_settings() -> BeanieSettings:
        return BeanieSettings(
            mongo_client=AsyncMongoClient("mongodb://localhost:27017"),
            db_name="myapp",
            entity_classes=(User, Post),
        )

    container.provide(beanie_settings)

    # 2. Install the module (scan-marker) and scan varco_beanie
    container.install(BeanieModule)
    container.scan("varco_beanie", recursive=True)

    # 3. Bind per-entity AsyncRepository[D] — must come after scan
    bind_repositories(container, User, Post)

    # 4. Resolve anywhere in your app
    repo = await container.aget(AsyncRepository[User])

Thread safety:  ✅ All binding registrations happen at startup before concurrent access.
Async safety:   ✅ BeanieRepositoryProvider.init() is async; resolution via aget().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from providify import Configuration, Provider
from varco_core.model import DomainModel
from varco_core.providers import RepositoryProvider
from varco_core.repository import AsyncRepository

# Re-export BeanieSettings from config so existing imports of
# ``from varco_beanie.di import BeanieSettings`` keep working.
from varco_beanie.config import BeanieSettings  # noqa: F401

if TYPE_CHECKING:
    # Avoid a hard circular import — DIContainer is only needed for the
    # bind_repositories() type hint, not at runtime.
    from providify import DIContainer


# ── Configuration module ──────────────────────────────────────────────────────


@Configuration
class BeanieModule:
    """
    Providify ``@Configuration`` module for the Beanie (MongoDB) backend.

    ``BeanieRepositoryProvider``, ``BeanieQueryCompiler``, and
    ``BeanieHealthCheck`` are registered automatically via their
    ``@Singleton`` decorators when ``container.scan("varco_beanie")`` is
    called.  This module serves as a scan marker and exports the
    ``bind_repositories()`` helper.

    Per-entity ``AsyncRepository[D]`` bindings are NOT added here — call
    ``bind_repositories(container, *entity_classes)`` after ``install()``.
    They are separate because the set of entity classes is determined at
    app startup, not hardcoded in the module.

    Prerequisites
    -------------
    ``BeanieSettings`` must be registered in the container before calling
    ``container.install(BeanieModule)`` — it is injected into
    ``BeanieRepositoryProvider`` and ``BeanieHealthCheck`` via
    ``Inject[BeanieSettings]``.

    Thread safety:  ✅ Module instance is created once at install() time.
    Async safety:   ✅ ``BeanieRepositoryProvider.init()`` is called via
                       ``@PostConstruct`` — the container awaits it during
                       warm-up.
    """


# ── Per-entity repository binding helper ──────────────────────────────────────


def bind_repositories(
    container: DIContainer,
    *entity_classes: type[DomainModel],
) -> None:
    """
    Register an ``AsyncRepository[D]`` binding for each domain model class.

    After calling this, ``await container.aget(AsyncRepository[User])``
    resolves to a ``BeanieRepositoryProvider``-backed repository for ``User``.

    Each factory is an **async DEPENDENT-scoped** ``@Provider`` — a fresh
    repository instance is returned per resolution.  Repositories are
    stateless wrappers around the Beanie connection pool, so this is safe.

    DESIGN: per-entity @Provider functions over a single generic factory
      ✅ Each binding has a concrete generic alias (AsyncRepository[User])
         that the container can match exactly via _is_generic_subtype()
      ✅ Works with container.aget(AsyncRepository[User]) type resolution
      ❌ N @Provider registrations for N entity classes — scales linearly
         with the model count, but that is typically small (<50)

    Prerequisites
    -------------
    ``BeanieModule`` must be installed before calling this function —
    the generated providers inject ``RepositoryProvider`` which is
    registered via ``@Singleton`` on ``BeanieRepositoryProvider``.

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
    Async safety:   ✅ The generated providers are async coroutine functions.
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
    Build a ``@Provider``-decorated async factory for ``AsyncRepository[entity_cls]``.

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
        An async function with ``@Provider`` metadata stamped and the
        return annotation set to ``AsyncRepository[entity_cls]``.

    Thread safety:  ✅ Pure function — creates a new closure each call.
    Async safety:   ✅ The returned factory is async.
    """

    async def _repo_factory(provider: RepositoryProvider) -> AsyncRepository:  # type: ignore[type-arg]
        # provider is injected by the container (resolved as RepositoryProvider
        # singleton from @Singleton on BeanieRepositoryProvider).
        # get_repository() returns the correct AsyncBeanieRepository subtype
        # for entity_cls without needing any additional parameters.
        return provider.get_repository(entity_cls)

    # Patch the return annotation with the concrete generic alias so providify
    # registers this binding under AsyncRepository[entity_cls], not the bare
    # unparameterised AsyncRepository.  Without this patch, all entity repos
    # would collide under the same unparameterised interface.
    _repo_factory.__annotations__["return"] = AsyncRepository[entity_cls]

    # Give the closure a descriptive __name__ for debugging / describe() output.
    _repo_factory.__name__ = f"_repo_factory_{entity_cls.__name__}"

    # Stamp @Provider metadata — DEPENDENT scope (default) so a fresh repo
    # wrapper is returned each time; repositories carry no state between calls.
    return Provider(_repo_factory)


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "BeanieModule",
    "BeanieSettings",
    "bind_repositories",
]
