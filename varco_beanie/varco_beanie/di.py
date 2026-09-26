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

from providify import Configuration, Fallback, Provider
from varco_core.di import BACKEND_PRIORITY
from varco_core.di.select import selected_by
from varco_core.idempotency.base import AbstractIdempotencyStore
from varco_core.job.base import AbstractJobStore
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

    @Fallback
    @selected_by("idempotency", "beanie")
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    def idempotency_store(self) -> AbstractIdempotencyStore:
        """
        Plan 047 N4 — live only when ``VARCO_IDEMPOTENCY_BACKEND=beanie``.

        Docstring note: ``IdempotencyDocument`` must be registered with
        the app's ``init_beanie()`` before use — this provider does not
        call ``init_beanie()`` itself.

        Returns:
            A ``BeanieIdempotencyStore()``.
        """
        from varco_beanie.idempotency import BeanieIdempotencyStore  # noqa: PLC0415

        return BeanieIdempotencyStore()

    @Fallback
    @selected_by("job_store", "beanie")
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    def job_store(self) -> AbstractJobStore:
        """
        Plan 047 N5 — live only when ``VARCO_JOB_STORE_BACKEND=beanie``.

        Docstring note: ``JobDocument`` must be registered with the app's
        ``init_beanie()`` before use.

        Returns:
            A ``BeanieJobStore()``.
        """
        from varco_beanie.job_store import BeanieJobStore  # noqa: PLC0415

        return BeanieJobStore()


# ── Per-entity repository binding helper ──────────────────────────────────────


def bind_repositories(
    container: DIContainer,
    *entity_classes: type[DomainModel],
) -> DIContainer:
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

    Returns:
        The same ``container`` passed in.

    Raises:
        ValueError: Called with no ``entity_classes``.
        AlreadyBoundError: A non-fallback ``AsyncRepository[entity_cls]``
            binding already exists for one of the given classes (§D-bind).

    Edge cases:
        - Empty ``entity_classes`` raises immediately — likely a programming error.

    Example::

        bind_repositories(container, User, Post, Tag)
        repo = await container.aget(AsyncRepository[User])

    Thread safety:  ✅ Called once at startup before concurrent access.
    Async safety:   ✅ The generated providers are async coroutine functions.
    """
    from varco_core.di import AlreadyBoundError, is_user_bound  # noqa: PLC0415

    if not entity_classes:
        raise ValueError(
            "bind_repositories() requires at least one entity class. "
            "Example: bind_repositories(container, User, Post)"
        )

    for entity_cls in entity_classes:
        interface = AsyncRepository[entity_cls]  # type: ignore[valid-type, type-arg]
        if is_user_bound(container, interface):
            raise AlreadyBoundError(
                f"A non-fallback AsyncRepository[{entity_cls.__name__}] binding already "
                "exists — use providify's di_overrides/ContainerOverrides in a test to "
                "replace it."
            )
        container.provide(_make_repo_provider(entity_cls))

    return container


def _make_repo_provider(entity_cls: type[DomainModel]) -> Any:
    """
    Build a ``@Provider``-decorated async factory for ``AsyncRepository[entity_cls]``.

    The factory is stamped with providify's native ``@Provider(returns=...)``
    decoration-time override so it registers under the precise generic alias
    ``AsyncRepository[entity_cls]`` (e.g. ``AsyncRepository[User]``), not the
    bare unparameterised ``AsyncRepository`` — otherwise every entity's repo
    would collide under one interface. This function is a plain,
    **container-less** builder: it returns the decorated-but-unregistered
    factory, tested standalone (``varco_beanie/tests/test_beanie_di.py``)
    before it is ever handed to ``container.provide()`` by
    ``bind_repositories()``. Since ``returns=`` is applied at decoration time
    (not by mutating ``__annotations__``), the override survives being
    returned unregistered and passed to ``container.provide()`` later — no
    annotation patching is needed.

    Args:
        entity_cls: The ``DomainModel`` subclass to build a provider for.

    Returns:
        An async function with ``@Provider(returns=AsyncRepository[entity_cls])``
        metadata stamped.

    Thread safety:  ✅ Pure function — creates a new closure each call.
    Async safety:   ✅ The returned factory is async.
    """

    async def _repo_factory(provider: RepositoryProvider) -> AsyncRepository[Any, Any]:
        # provider is injected by the container (resolved as RepositoryProvider
        # singleton from @Singleton on BeanieRepositoryProvider).
        # get_repository() returns the correct AsyncBeanieRepository subtype
        # for entity_cls without needing any additional parameters.
        return provider.get_repository(entity_cls)

    # Give the closure a descriptive __name__ for debugging / describe() output.
    _repo_factory.__name__ = f"_repo_factory_{entity_cls.__name__}"

    # Stamp @Provider metadata with the explicit returns= override — DEPENDENT
    # scope (default) so a fresh repo wrapper is returned each time;
    # repositories carry no state between calls.
    return Provider(returns=AsyncRepository[entity_cls])(_repo_factory)  # type: ignore[valid-type, misc]


# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(container: DIContainer) -> DIContainer:
    """
    Bootstrap ``varco_beanie`` into a ``DIContainer``.

    Installs :class:`BeanieModule` and calls
    ``container.scan("varco_beanie", recursive=True)`` to discover
    ``BeanieRepositoryProvider``, ``BeanieQueryCompiler``, and
    ``BeanieHealthCheck``. Call :func:`bind_repositories` separately for
    per-entity ``AsyncRepository[D]`` bindings (Plan 046 §D-contract removes
    the ``*entity_classes`` convenience — a re-run ``bootstrap()`` must be
    idempotent, and ``bind_repositories()``'s own rule is "raise on repeat",
    which is the wrong behaviour for that).

    ``BeanieSettings`` **must** be registered in the container before calling
    this function — it is injected into both ``BeanieRepositoryProvider``
    and ``BeanieHealthCheck``::

        from pymongo import AsyncMongoClient
        from providify import DIContainer, Provider
        from varco_beanie.di import bind_repositories, bootstrap, BeanieSettings
        from myapp.models import User, Post

        container = DIContainer()

        @Provider(singleton=True)
        def beanie_settings() -> BeanieSettings:
            return BeanieSettings(
                mongo_client=AsyncMongoClient("mongodb://localhost:27017"),
                db_name="myapp",
                entity_classes=(User, Post),
            )

        container.provide(beanie_settings)
        bootstrap(container)
        bind_repositories(container, User, Post)

        # BeanieRepositoryProvider.init() is async — warm up the container
        await container.awarm_up()

    Args:
        container: The ``DIContainer`` to install into. Required — this
            function never creates one and never reads
            the process-global container (Plan 046 §D-locator).

    Returns:
        The same ``container`` passed in, after installation and scanning.

    Raises:
        LookupError: Raised lazily at resolution time if ``BeanieSettings`` is
                     not registered before this call.

    Edge cases:
        - ``BeanieRepositoryProvider.init()`` is async — call
          ``await container.awarm_up()`` after ``bootstrap()`` to trigger
          its ``@PostConstruct`` and run ``init_beanie()`` before serving requests.
        - Calling twice is safe — scanning and ``install`` are idempotent.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ⚠️ This function is synchronous, but
                       ``BeanieRepositoryProvider`` is initialised asynchronously
                       via ``@PostConstruct``.  Always call
                       ``await container.awarm_up()`` afterwards.
    """
    # BeanieModule is an empty backward-compat @Configuration — install() is a no-op.
    # All @Singleton classes in varco_beanie are discovered by scan automatically.
    container.scan("varco_beanie", recursive=True)

    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "BeanieModule",
    "BeanieSettings",
    "bind_repositories",
    "bootstrap",
]
