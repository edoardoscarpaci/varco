"""
varco_beanie.di
===================
providify DI integration for the Beanie (Motor / MongoDB) backend.

Wires ``BeanieRepositoryProvider``, ``BeanieQueryCompiler``, and per-entity
``AsyncRepository[D]`` bindings into a ``DIContainer`` with a minimal setup API.

Typical usage::

    from motor.motor_asyncio import AsyncIOMotorClient
    from providify import DIContainer, Provider

    from varco_beanie.di import BeanieModule, BeanieSettings, bind_repositories
    from myapp.models import User, Post          # your DomainModel subclasses

    container = DIContainer()

    # 1. Provide settings (sync @Provider — so install() stays synchronous)
    @Provider(singleton=True)
    def beanie_settings() -> BeanieSettings:
        return BeanieSettings(
            motor_client=AsyncIOMotorClient("mongodb://localhost:27017"),
            db_name="myapp",
            entity_classes=(User, Post),
        )

    container.provide(beanie_settings)

    # 2. Install the module — injects BeanieSettings, registers providers
    #    Use ainstall if BeanieSettings itself comes from an async @Provider
    container.install(BeanieModule)

    # 3. Bind per-entity AsyncRepository[D] — must come after install()
    bind_repositories(container, User, Post)

    # 4. Resolve anywhere in your app
    repo = await container.aget(AsyncRepository[User])

Thread safety:  ✅ All binding registrations happen at startup before concurrent access.
Async safety:   ✅ Repository provider method is async; resolution via aget().
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from motor.motor_asyncio import AsyncIOMotorClient

from providify import Configuration, Provider
from varco_core.model import DomainModel
from varco_core.providers import RepositoryProvider
from varco_core.repository import AsyncRepository
from varco_beanie.provider import BeanieRepositoryProvider
from varco_beanie.query.compiler import BeanieQueryCompiler

if TYPE_CHECKING:
    # Avoid a hard circular import — DIContainer is only needed for the
    # bind_repositories() type hint, not at runtime.
    from providify import DIContainer


# ── Settings ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BeanieSettings:
    """
    Immutable configuration bundle for the Beanie DI module.

    Frozen so it can safely be shared across threads and cached as a
    singleton without defensive copying.

    DESIGN: separate settings object over constructor injection on BeanieModule
      ✅ All config grouped in one place — easy to swap for tests
      ✅ Frozen dataclass is hashable — can be used as a dict key if needed
      ✅ Keeps BeanieModule.__init__ to a single parameter (DI-friendly)
      ❌ One extra level of indirection vs passing motor_client directly

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ No async state.

    Args:
        motor_client:   A connected ``AsyncIOMotorClient`` instance.
        db_name:        MongoDB database name.
        entity_classes: Domain model classes to register at module init time.
                        Can also be registered later via
                        ``provider.register(*classes)`` before first use.
        transactional:  Wrap all UoW operations in a MongoDB transaction.
                        Requires a replica set or sharded cluster — standalone
                        instances do not support multi-document transactions.

    Edge cases:
        - Empty ``entity_classes`` is allowed — register entities later.
        - ``transactional=True`` on a standalone node raises at runtime
          when the first transaction begins, not at construction time.
    """

    motor_client: AsyncIOMotorClient
    db_name: str
    entity_classes: tuple[type[DomainModel], ...] = field(default_factory=tuple)
    transactional: bool = False


# ── Configuration module ──────────────────────────────────────────────────────


@Configuration
class BeanieModule:
    """
    providify ``@Configuration`` module for the Beanie (MongoDB) backend.

    When installed in a ``DIContainer``, registers the following bindings:

    +-------------------------------+--------+-------+
    | Type                          | Scope  | Async |
    +===============================+========+=======+
    | ``RepositoryProvider``        | SINGLE | yes   |
    +-------------------------------+--------+-------+
    | ``BeanieQueryCompiler``       | SINGLE | no    |
    +-------------------------------+--------+-------+

    Per-entity ``AsyncRepository[D]`` bindings are NOT added here — call
    ``bind_repositories(container, *entity_classes)`` after ``install()``.
    They are separate because the set of entity classes is determined at
    app startup, not hardcoded in the module.

    Prerequisites
    -------------
    ``BeanieSettings`` must be registered in the container before calling
    ``container.install(BeanieModule)`` — the container injects it into
    this class's ``__init__``.

    Thread safety:  ✅ Module instance is created once at install() time.
    Async safety:   ✅ ``repository_provider`` is async — use aget() to resolve.

    Args:
        settings: Injected ``BeanieSettings`` from the container.
    """

    def __init__(self, settings: BeanieSettings) -> None:
        # settings is injected by the container at install() time.
        # We store it so @Provider methods can access it via self._settings.
        self._settings = settings

    @Provider(singleton=True)
    async def repository_provider(self) -> RepositoryProvider:
        """
        Create, configure, and initialise the ``BeanieRepositoryProvider``.

        Calls ``await provider.init()`` so Beanie's document models are
        registered with Motor before any repository operation runs.

        Returns:
            A fully initialised ``BeanieRepositoryProvider`` cast to the
            abstract ``RepositoryProvider`` interface — callers should
            inject ``RepositoryProvider``, never the concrete type.

        Raises:
            Any exception raised by ``beanie.init_beanie()`` propagates
            directly (e.g. connection refused from Motor).

        Edge cases:
            - If ``entity_classes`` is empty in settings, callers must call
              ``provider.register(*classes)`` and ``await provider.init()``
              manually before using repositories.
            - ``init()`` is idempotent — safe to call more than once.
        """
        provider = BeanieRepositoryProvider(
            motor_client=self._settings.motor_client,
            db_name=self._settings.db_name,
            transactional=self._settings.transactional,
        )

        if self._settings.entity_classes:
            # Register all domain classes so ORM document types are built
            # before init_beanie() is called — Beanie needs the full list upfront.
            provider.register(*self._settings.entity_classes)

        await provider.init()
        return provider

    @Provider(singleton=True)
    def query_compiler(self) -> BeanieQueryCompiler:
        """
        Provide a shared ``BeanieQueryCompiler`` instance.

        ``BeanieQueryCompiler`` is stateless — every public method is a pure
        function of its inputs — so a single instance is safe to share
        across all repositories and requests.

        Returns:
            A singleton ``BeanieQueryCompiler``.

        Thread safety:  ✅ Stateless — no instance state to protect.
        Async safety:   ✅ All visitor methods are synchronous.
        """
        # DESIGN: singleton over creating a new compiler per repository call
        #   ✅ Zero allocation overhead at query time
        #   ✅ Thread-safe because there is no mutable state
        #   ❌ If a future subclass adds state, callers must be updated
        return BeanieQueryCompiler()


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
    the generated providers inject ``RepositoryProvider`` which is registered
    by ``BeanieModule.repository_provider()``.

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
        # singleton from BeanieModule.repository_provider()).
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
