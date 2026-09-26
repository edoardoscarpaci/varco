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

from typing import TYPE_CHECKING, Annotated, Any

from providify import Configuration, Fallback, Inject, InjectMeta, PostConstruct, Provider
from varco_core.di import BACKEND_PRIORITY
from varco_core.di.select import selected_by
from varco_core.event.base import Event
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_core.idempotency.base import AbstractIdempotencyStore
from varco_core.job.base import AbstractJobStore
from varco_core.lock import AbstractDistributedLock
from varco_core.model import DomainModel
from varco_core.providers import RepositoryProvider
from varco_core.repository import AsyncRepository
from varco_core.serialization import Serializer
from varco_core.service.base import IUoWProvider
from varco_core.tenancy.settings import TenancySettings

from varco_sa.advisory_lock import SAAdvisoryLock, SAXactAdvisoryLock
from varco_sa.config import SAConfig
from varco_sa.dlq import SADeadLetterQueue
from varco_sa.idempotency import SAIdempotencyStore
from varco_sa.job_store import SAJobStore

if TYPE_CHECKING:
    # Avoid a hard circular import — DIContainer is only needed for the
    # bind_repositories() type hint, not at runtime.
    from providify import DIContainer


# ── Configuration module ──────────────────────────────────────────────────────


@Configuration
class SAModule:
    """
    ``@Configuration`` module for the SQLAlchemy async backend.

    Discovered and auto-installed by ``container.scan("varco_sa", recursive=True)``.
    No explicit ``container.install(SAModule)`` call is required.

    Registers:
        - ``IUoWProvider`` → ``SQLAlchemyRepositoryProvider`` singleton (via
          ``uow_provider`` below).  ``AsyncService.__init__`` injects this.
        - ``AbstractDistributedLock`` → ``SAAdvisoryLock`` singleton (via
          ``sa_advisory_lock`` below) — the default upgrade-safe binding
          (Plan 005 Phase 5 / U-16). See that provider's docstring for the
          override recipe if you want ``SAXactAdvisoryLock`` instead.
        - ``SAXactAdvisoryLock`` singleton, directly injectable via
          ``Inject[SAXactAdvisoryLock]`` regardless of which class wins the
          ``AbstractDistributedLock`` binding (via ``sa_xact_advisory_lock``
          below).

    ``SQLAlchemyRepositoryProvider`` and ``SAHealthCheck`` are also registered
    automatically via their ``@Singleton`` decorators when scan discovers them.

    Per-entity ``AsyncRepository[D]`` bindings are NOT added here — call
    ``bind_repositories(container, *entity_classes)`` after scanning.
    They remain separate because the set of entity classes is determined at
    app startup, not hardcoded in the module.

    Thread safety:  ✅ Module instance is created once at install() time.
    Async safety:   ✅ All providers are synchronous.
    """

    def __init__(self, settings: TenancySettings | None) -> None:
        """
        Args:
            settings: The app's ``TenancySettings``, injected as *optional*
                      (``TenancySettings | None``) specifically so an app
                      that never provides ``TenancySettings`` at all — the
                      byte-identical-by-default case — sees no error and
                      ``_install_rls_tenant_hook_if_enabled`` below installs
                      nothing. ``@PostConstruct`` methods take no injected
                      parameters of their own (providify calls them with no
                      arguments) — constructor injection is the only way a
                      ``@Configuration`` module can see a dependency before
                      its ``@PostConstruct`` hook runs.
        """
        self._tenancy_settings = settings

    @PostConstruct
    def _install_rls_tenant_hook_if_enabled(self) -> None:
        """
        Install the RLS ``after_begin`` GUC-setter hook when
        ``TenancySettings.rls_set_tenant`` is ``True`` (Plan 037 / S12c,
        §D-S12-hook).

        Runs eagerly at ``container.scan("varco_sa", recursive=True)``/
        ``container.install(SAModule)`` time — a ``@Configuration``'s
        ``@PostConstruct`` executes as soon as the module itself is
        installed, unlike a ``@Singleton``'s ``@PostConstruct`` (deferred
        until first resolution).

        DESIGN: scope the hook to ``AsyncSession`` itself, not a specific
        session factory (Plan 037 / §D-S12-hook)
            ✅ Avoids resolving ``SQLAlchemyRepositoryProvider`` (and
               therefore ``SAConfig``) from this ``@PostConstruct`` —
               ``SAConfig`` is provided by the *application*, often after
               ``varco_sa`` is scanned, and a hard dependency here would
               force construction order that today's docs and tests do not
               require.
            ✅ ``install_rls_tenant_hook(AsyncSession, ...)`` resolves to
               ``AsyncSession.sync_session_class`` — the shared base
               ``Session`` class every ``async_sessionmaker`` uses unless it
               overrides ``sync_session_class`` itself — so this covers
               every session an app builds from any engine, the common
               single-database-per-process shape this flag targets.
            ❌ A process with multiple, independently-configured session
               factories that want different RLS behaviour cannot get that
               from this DI wiring path — call
               ``varco_sa.tenancy.rls_session.install_rls_tenant_hook()``
               directly against the specific factory instead; this
               ``@PostConstruct`` is the common-case default, not the only
               way to install the hook.

        Thread safety:  ✅ Runs once, at single-threaded startup (scan time).
        Async safety:   ✅ Synchronous — no I/O.
        """
        settings = self._tenancy_settings
        if settings is None or not settings.rls_set_tenant:
            return

        from sqlalchemy.ext.asyncio import AsyncSession  # noqa: PLC0415

        from varco_sa.tenancy.rls_session import install_rls_tenant_hook  # noqa: PLC0415

        install_rls_tenant_hook(AsyncSession, require_tenant=settings.rls_require_tenant)

    @Fallback
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    def uow_provider(
        self,
        repo_provider: Inject[RepositoryProvider],
    ) -> IUoWProvider:
        """
        Re-expose ``SQLAlchemyRepositoryProvider`` as the ``IUoWProvider`` interface.

        ``AsyncService.__init__`` injects ``IUoWProvider`` — this binding satisfies
        that requirement as soon as scan auto-installs ``SAModule``.

        DESIGN: type re-export via @Configuration over RepositoryProvider subclassing IUoWProvider
            ✅ Avoids a circular import:
               ``providers.py`` → ``service.base`` → ``service/__init__``
               → ``service/tenant`` → ``providers.py``.
            ✅ DI container resolves ``IUoWProvider`` independently of
               ``RepositoryProvider`` — both types remain injectable separately.
            ✅ The ``RepositoryProvider`` singleton is reused — no second
               instance is created.

        Args:
            repo_provider: ``SQLAlchemyRepositoryProvider`` singleton resolved
                           from the container.

        Returns:
            The same singleton typed as ``IUoWProvider``.

        Thread safety:  ✅ Called once at singleton resolution time.
        Async safety:   ✅ Synchronous — no I/O.
        """
        # Return the same singleton — RepositoryProvider.make_uow() satisfies
        # IUoWProvider without wrapping.
        return repo_provider  # type: ignore[return-value]

    @Fallback
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    def sa_advisory_lock(self, config: Inject[SAConfig]) -> AbstractDistributedLock:
        """
        Bind ``AbstractDistributedLock`` → ``SAAdvisoryLock`` (session-level,
        Plan 005 Phase 5 / U-16).

        This is the **default** ``AbstractDistributedLock`` binding contributed
        by ``varco_sa`` — kept as ``SAAdvisoryLock`` (not the newer
        ``SAXactAdvisoryLock``) specifically so that upgrading to a
        Plan-005-Phase-5 release does not silently change behaviour for any
        app already resolving ``Inject[AbstractDistributedLock]``. Read
        ``SAAdvisoryLock``'s class docstring before deploying behind a
        transaction-mode connection pooler (PgBouncer ``pool_mode=transaction``
        and equivalents) — that topology is NOT supported by this default.

        **Override recipe**: this binding is ``@Fallback`` — register your own
        ``AbstractDistributedLock`` binding in any order, before or after
        ``scan("varco_sa", recursive=True)``, and it wins unconditionally:

        ```python
        @Provider(singleton=True)
        def my_lock(config: Inject[SAConfig]) -> AbstractDistributedLock:
            return SAXactAdvisoryLock(config.engine)

        container.provide(my_lock)          # any order — wins over the default
        container.scan("varco_sa", recursive=True)
        ```

        ``SAXactAdvisoryLock`` itself is always resolvable directly via
        ``Inject[SAXactAdvisoryLock]`` regardless of which class wins the
        ``AbstractDistributedLock`` binding — see ``sa_xact_advisory_lock``
        below.

        Args:
            config: ``SAConfig`` singleton — supplies the ``AsyncEngine``.

        Returns:
            A fresh ``SAAdvisoryLock`` wrapping ``config.engine``.

        Thread safety:  ✅ Called once at singleton resolution time.
        Async safety:   ✅ Synchronous — no I/O at construction time.
        """
        return SAAdvisoryLock(config.engine)

    @Fallback
    @Provider(singleton=True)
    def sa_xact_advisory_lock(self, config: Inject[SAConfig]) -> SAXactAdvisoryLock:
        """
        Register ``SAXactAdvisoryLock`` (transaction-level, Plan 005 Phase 5
        / U-16) as a directly-injectable singleton — ``Inject[SAXactAdvisoryLock]``.

        Deliberately bound under its OWN concrete type, not under
        ``AbstractDistributedLock`` — ``sa_advisory_lock`` above already owns
        that interface binding to preserve upgrade behaviour (see its
        docstring). This provider exists so ``SAXactAdvisoryLock`` — the
        transaction-pooling-safe sibling and the **recommended default** per
        its own class docstring — is reachable from the container without
        every caller having to construct it manually, and so the override
        recipe documented on ``sa_advisory_lock`` has a ready-made instance
        to delegate to.

        ``@Fallback`` (Plan 045): because ``SAXactAdvisoryLock`` is a
        concrete subclass of ``AbstractDistributedLock``, providify's
        ``issubclass``-based interface matching (``_binding_serves``) makes
        this binding an ACCIDENTAL candidate for a bare, unqualified
        ``Inject[AbstractDistributedLock]`` request too — not just for its
        own concrete type. Without ``@Fallback`` here, this binding (a plain
        non-fallback at priority 0) would win that request over
        ``sa_advisory_lock`` above (a ``@Fallback`` at ``BACKEND_PRIORITY``)
        the moment ``sa_advisory_lock`` became a fallback, silently reversing
        the "kept as ``SAAdvisoryLock`` so upgrading does not change
        behaviour" guarantee. Both are fallbacks now, so ``sa_advisory_lock``
        still wins the unqualified `AbstractDistributedLock` request by
        priority (``BACKEND_PRIORITY`` beats the bare default); an
        `Inject[SAXactAdvisoryLock]` request is unaffected — this binding is
        its only candidate either way.

        Args:
            config: ``SAConfig`` singleton — supplies the ``AsyncEngine`` used
                    only by the ``try_acquire``/``release`` ABC shape (``xact()``
                    does not need an engine at all — see its docstring).

        Returns:
            A fresh ``SAXactAdvisoryLock`` wrapping ``config.engine``.

        Thread safety:  ✅ Called once at singleton resolution time.
        Async safety:   ✅ Synchronous — no I/O at construction time.
        """
        return SAXactAdvisoryLock(config.engine)

    @Fallback
    @selected_by("dlq", "sa")
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    def dead_letter_queue(
        self,
        config: Inject[SAConfig],
        serializer: Annotated[Serializer[Event] | None, InjectMeta(optional=True)] = None,
    ) -> AbstractDeadLetterQueue:
        """
        Plan 047 N1 — live only when ``VARCO_DLQ_BACKEND=sa``.

        Closes the CLAUDE.md ``SADeadLetterQueue``/``OutboxRelay``
        "pass ``serializer=`` explicitly or they keep the
        ``JsonEventSerializer`` default" pitfall **for this DI path**:
        an optionally-injected ``Serializer[Event]`` is forwarded so a
        bound serializer (e.g. ``CloudEventsJsonSerializer``) is honoured
        automatically. ``SADeadLetterQueue`` itself still defaults to
        ``JsonEventSerializer()`` when constructed by hand.

        Args:
            config: The scanned ``SAConfig`` singleton — ``.engine`` is
                forwarded verbatim.
            serializer: Optionally-injected ``Serializer[Event]``. ``None``
                when nothing is bound, in which case ``SADeadLetterQueue``
                falls back to ``JsonEventSerializer()`` internally.

        Returns:
            A ``SADeadLetterQueue(config.engine, serializer=serializer)``.
        """
        return SADeadLetterQueue(config.engine, serializer=serializer)

    @Fallback
    @selected_by("idempotency", "sa")
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    def idempotency_store(self, config: Inject[SAConfig]) -> AbstractIdempotencyStore:
        """
        Plan 047 N2 — live only when ``VARCO_IDEMPOTENCY_BACKEND=sa``.

        Uses ``SAIdempotencyStore``'s additive ``engine=`` kwarg to reuse
        the app's own pool (``config.engine``) rather than opening a
        second, private engine.

        ⚠️ Lifecycle is Plan 050's — nothing here calls
        ``await store.start()`` (which also ensures the
        ``varco_idempotency`` table exists); call it yourself until 050
        wires it.

        Args:
            config: The scanned ``SAConfig`` singleton.

        Returns:
            A ``SAIdempotencyStore(engine=config.engine)``.
        """
        return SAIdempotencyStore(engine=config.engine)

    @Fallback
    @selected_by("job_store", "sa")
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    def job_store(self, config: Inject[SAConfig]) -> AbstractJobStore:
        """
        Plan 047 N3 — live only when ``VARCO_JOB_STORE_BACKEND=sa``.

        Args:
            config: The scanned ``SAConfig`` singleton.

        Returns:
            A ``SAJobStore(config.engine)``.
        """
        return SAJobStore(config.engine)


# ── Per-entity repository binding helper ──────────────────────────────────────


def bind_repositories(
    container: DIContainer,
    *entity_classes: type[DomainModel],
) -> DIContainer:
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

    Returns:
        The same ``container`` passed in.

    Raises:
        ValueError: Called with no ``entity_classes``.
        AlreadyBoundError: A non-fallback ``AsyncRepository[entity_cls]``
            binding already exists for one of the given classes (§D-bind) —
            qualify differently is not applicable here; use providify's
            ``di_overrides``/``ContainerOverrides`` in a test to replace it.

    Edge cases:
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
        _bind_repo_provider(container, entity_cls)

    return container


def _bind_repo_provider(container: DIContainer, entity_cls: type[DomainModel]) -> None:
    """
    Register a sync ``AsyncRepository[entity_cls]`` provider on *container*.

    The factory is registered with an explicit ``returns=`` override so that
    providify registers the binding under the precise generic alias
    ``AsyncRepository[entity_cls]`` (e.g. ``AsyncRepository[User]``), not the
    bare unparameterised ``AsyncRepository`` — otherwise every entity's repo
    would collide under one interface. providify's ``@Provider(returns=…)`` /
    ``container.provide(fn, returns=…)`` apply this override at
    decoration/registration time, so no return-annotation patching is needed.

    Args:
        container:  The ``DIContainer`` to register the binding into.
        entity_cls: The ``DomainModel`` subclass to build a provider for.

    Returns:
        None — the provider is registered as a side effect on *container*.

    Raises:
        AlreadyBoundError: A non-fallback ``AsyncRepository[entity_cls]``
            binding already exists (§D-bind).

    Thread safety:  ✅ Pure function — creates a new closure each call.
    Async safety:   ✅ The registered factory is synchronous — repository
                       construction itself is synchronous; I/O is lazy.
    """
    from varco_core.di import AlreadyBoundError, is_user_bound  # noqa: PLC0415

    interface = AsyncRepository[entity_cls]  # type: ignore[valid-type, type-arg]
    if is_user_bound(container, interface):
        raise AlreadyBoundError(
            f"A non-fallback AsyncRepository[{entity_cls.__name__}] binding already "
            "exists — use providify's di_overrides/ContainerOverrides in a test to "
            "replace it."
        )

    def _repo_factory(provider: RepositoryProvider) -> AsyncRepository[Any, Any]:
        # provider is injected by the container (resolved as RepositoryProvider
        # singleton from SAModule.repository_provider()).
        # get_repository() returns the correct AsyncSQLAlchemyRepository
        # subtype for entity_cls, creating a fresh AsyncSession per call.
        return provider.get_repository(entity_cls)

    # DEPENDENT scope (default, singleton=False) — a fresh repo wrapper is
    # returned each time; AsyncSession is created per resolution.
    #
    # __name__ is set explicitly (not subsumed by returns=) so per-entity
    # closures built in this loop are distinguishable in describe() output.
    _repo_factory.__name__ = f"_repo_factory_{entity_cls.__name__}"
    container.provide(
        Provider(singleton=False)(_repo_factory),
        returns=AsyncRepository[entity_cls],  # type: ignore[valid-type, misc]
    )


# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(container: DIContainer) -> DIContainer:
    """
    Bootstrap ``varco_sa`` into a ``DIContainer``.

    Installs :class:`SAModule` and calls
    ``container.scan("varco_sa", recursive=True)`` to discover
    ``SQLAlchemyRepositoryProvider`` and ``SAHealthCheck``. Call
    :func:`bind_repositories` separately for per-entity ``AsyncRepository[D]``
    bindings (Plan 046 §D-contract removes the ``*entity_classes`` convenience
    — a re-run ``bootstrap()`` must be idempotent, and ``bind_repositories()``'s
    own rule is "raise on repeat", which is the wrong behaviour for that).

    ``SAConfig`` **must** be registered in the container before calling
    this function — it is injected into both ``SQLAlchemyRepositoryProvider``
    and ``SAHealthCheck``::

        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy.orm import DeclarativeBase
        from providify import DIContainer, Provider
        from varco_sa.bootstrap import SAConfig
        from varco_sa.di import bind_repositories, bootstrap
        from myapp.models import User, Post

        class Base(DeclarativeBase): pass

        container = DIContainer()

        @Provider(singleton=True)
        def sa_config() -> SAConfig:
            return SAConfig(
                engine=create_async_engine("postgresql+asyncpg://..."),
                base=Base,
                entity_classes=(User, Post),
            )

        container.provide(sa_config)
        bootstrap(container)
        bind_repositories(container, User, Post)

    Args:
        container: The ``DIContainer`` to install into. Required — this
            function never creates one and never reads
            the process-global container (Plan 046 §D-locator).

    Returns:
        The same ``container`` passed in, after installation and scanning.

    Raises:
        LookupError: Raised lazily at resolution time if ``SAConfig`` is not
                     registered before this call.

    Edge cases:
        - Calling twice is safe — scanning and ``install`` are idempotent.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ All operations are synchronous — SQLAlchemy has no
                       async init step at construction time.
    """
    # SAModule is an empty backward-compat @Configuration — install() is a no-op.
    # _sa_uow_provider is a module-level @Provider discovered by scan automatically,
    # so no explicit install() is needed.
    container.scan("varco_sa", recursive=True)

    return container


# ── create_tables helper ──────────────────────────────────────────────────────


async def create_tables(container: DIContainer) -> None:
    """
    Create all SQLAlchemy-mapped tables in the connected database.

    This is a convenience wrapper for the standard ``base.metadata.create_all``
    pattern.  It:

    1. Resolves ``SQLAlchemyRepositoryProvider`` from the container — this
       populates ``base.metadata`` with all generated ORM table mappings.
       **Must happen before DDL** or the metadata is empty.
    2. Resolves ``SAConfig`` to get the engine and base.
    3. Runs ``base.metadata.create_all`` via the async engine.

    Idempotent — issues ``CREATE TABLE IF NOT EXISTS`` so it is safe to call
    on every restart.  Not suitable for schema migrations — use Alembic instead.

    ::

        from varco_sa.di import create_tables

        async def _bootstrap() -> None:
            await create_tables(container)   # one line instead of 15

    Args:
        container: The ``DIContainer`` to resolve ``SAConfig`` and
                   ``SQLAlchemyRepositoryProvider`` from. Required — this
                   function never reads the process-global container (Plan 046
                   §D-locator).

    Raises:
        LookupError: ``SAConfig`` is not registered in the container.
        sqlalchemy.exc.OperationalError: Database is unreachable.

    Edge cases:
        - Must be called after ``bootstrap()`` so ``SQLAlchemyRepositoryProvider``
          and ``SAConfig`` are already registered in the container.
        - Calling before ``bootstrap()`` raises ``LookupError`` on the
          ``SQLAlchemyRepositoryProvider`` resolution step.

    Thread safety:  ✅ Intended for single-threaded startup only.
    Async safety:   ✅ ``async def`` — safe to ``await`` from any async context.
    """
    from varco_core.providers import (
        RepositoryProvider as _RepoProvider,
    )  # noqa: PLC0415

    from varco_sa.bootstrap import SAConfig as _SAConfig  # noqa: PLC0415

    # Resolve RepositoryProvider first — its __init__ calls
    # provider.register(*entity_classes) which populates base.metadata with
    # the generated ORM table mappings.  DDL must come AFTER this step or
    # base.metadata is empty and create_all() produces no DDL.
    container.get(_RepoProvider)

    sa_config: _SAConfig = container.get(_SAConfig)
    async with sa_config.engine.begin() as conn:
        # run_sync delegates synchronous DDL to the async driver without
        # blocking the event loop — standard pattern for SQLAlchemy async.
        await conn.run_sync(sa_config.base.metadata.create_all)


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SAModule",
    "bind_repositories",
    "bootstrap",
    "create_tables",
]
