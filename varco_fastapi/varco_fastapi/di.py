"""
varco_fastapi.di
==================
Central DI module for varco_fastapi.

``VarcoFastAPIModule`` is a ``@Configuration`` class that registers the default
varco_fastapi providers in a ``DIContainer``.  Install it alongside backend
modules (SA, Redis, etc.) at application bootstrap::

    container = DIContainer()
    container.scan("varco_kafka", recursive=True)   # discovers the Kafka bus
    container.install(SAModule)
    container.install(VarcoFastAPIModule)
    bind_clients(container, OrderClient, UserClient)  # ⚠️ currently broken — see bind_clients() docstring

Registered defaults (all ``@Fallback`` — any application binding of the same
interface wins regardless of registration order,
`technical_docs/features/dependency-injection.md`):

    AbstractJobStore   → InMemoryJobStore
    AbstractJobRunner  → JobRunner(store)
    AbstractServerAuth → JwtBearerAuth (requires TrustedIssuerRegistry in container)
    TrustStore         → TrustStore.from_env()
    CORSConfig         → CORSConfig.from_env()
    ClientProfile      → ClientProfile.from_env()
    TaskRegistry       → TaskRegistry() (singleton)
    TaskSerializer     → DefaultTaskSerializer (singleton)

``setup_varco_defaults()`` / ``bootstrap(setup_defaults=)`` were removed in
Plan 045 — ``TaskSerializer`` is now registered directly by
``VarcoFastAPIModule.task_serializer`` (a ``@Fallback @Provider``, the same
shape as every other default above), scanned automatically by
``container.scan("varco_fastapi", recursive=True)`` / ``bootstrap()``.

Override in any order::

    container.bind(TaskSerializer, MySerializer)  # overrides the default

``bind_clients()`` mirrors ``bind_repositories()`` from ``varco_sa.di`` — both go
through ``container.provide(Provider(...)(factory), returns=...)``, whose
decoration-time ``returns=`` override lets providify resolve
``Inject[VarcoClient[OrderRouter]]`` correctly at injection time without any
annotation patching.

DESIGN: bind_clients() helper over manual provider registration
    ✅ Same pattern as bind_repositories() — familiar to varco users
    ✅ Handles annotation patching for generic type resolution automatically
    ✅ One call registers multiple clients
    ❌ Requires concrete client classes (not dynamic) — use RestClientBuilder for that

Thread safety:  ✅ DI registration is single-threaded at bootstrap time.
Async safety:   ✅ No I/O during registration; providers are called lazily.
"""

from __future__ import annotations

from typing import Any

from providify import Configuration, Fallback, Provider

# ── Plan 011 — i18n / timezone default bindings ─────────────────────────
from varco_core.context.defaults import NullTenantDefaults, TenantDefaultsProvider

# Event producer types — also needed by setup_event_producer() for the same reason.
from varco_core.event.base import AbstractEventBus
from varco_core.event.producer import AbstractEventProducer, BusEventProducer
from varco_core.i18n.catalog import MessageCatalog, NullMessageCatalog
from varco_core.i18n.settings import I18nSettings
from varco_core.job.serializer import DefaultTaskSerializer, TaskSerializer
from varco_core.job.task import TaskRegistry

# JWT claim-transform config types (Plan 002 Phase 2 step 20) — module-level
# import for the same get_type_hints() reason documented above.
from varco_core.jwt.transform.config import JwtTransformConfig, JwtTransformSettings
from varco_core.jwt.transform.registry import ClaimTransformerRegistry
from varco_core.tz.settings import TimezoneSettings

# ── Module-level imports for DI type resolution ───────────────────────────────
# These must live at module scope because ``from __future__ import annotations``
# turns all annotations into strings.  When providify calls
# ``typing.get_type_hints(provider_fn)`` it resolves those strings using the
# function's ``__globals__`` (== this module's globals).  Any type only imported
# locally would be missing from ``__globals__`` and ``get_type_hints`` would
# raise NameError, which providify surfaces as "Provider must declare a return
# type hint".
from varco_fastapi.auth.trust_store import TrustStore
from varco_fastapi.client.base import ClientProfile
from varco_fastapi.middleware.cors import CORSConfig
from varco_fastapi.middleware.profiling import ProfilingSettings


def bind_clients(container: Any, *client_classes: type) -> None:
    """
    Register ``AsyncVarcoClient`` subclasses in the DI container.

    Creates a singleton ``@Provider`` for each client class and registers it
    under ``VarcoClient[RouterType]`` so injections like
    ``Inject[VarcoClient[OrderRouter]]`` resolve correctly.

    Implementation (same pattern as ``bind_repositories()`` in ``varco_sa.di``):

    1. Read ``_router_class`` ClassVar to get the router type.
    2. Create a plain factory function.
    3. Register it via ``container.provide(Provider(singleton=True)(factory),
       returns=VarcoClient[router_type])`` — the decoration-time ``returns=``
       override means the factory's placeholder return annotation never
       needs patching.

    Args:
        container:     ``DIContainer`` instance to register into.
        *client_classes: One or more ``AsyncVarcoClient`` subclasses to register.

    Usage::

        container = DIContainer()
        container.install(VarcoFastAPIModule)
        bind_clients(container, OrderClient, UserClient)

        # Now injectable:
        class ShippingService:
            def __init__(self, orders: Inject[VarcoClient[OrderRouter]]) -> None:
                self._orders = orders

    Edge cases:
        - Client without ``_router_class`` → ``TypeError`` with helpful message.
        - Same client registered twice → the **first** registration wins, not
          the second. `DIContainer.get()` picks the highest-*priority* binding
          for an interface and both registrations share the same (default)
          priority, so ties resolve to the earliest-registered candidate — the
          binding is appended, never replaced (same first-registered-wins rule
          as ``container.provide()`` / ``container.install()``).
        - No configurator → client will need an explicit ``base_url`` at construction.

    Thread safety:  ✅ Registration happens at bootstrap; no concurrent access.
    Async safety:   ✅ No I/O during registration.
    """
    from varco_fastapi.client.base import AsyncVarcoClient

    for client_cls in client_classes:
        router_cls = getattr(client_cls, "_router_class", None)
        if router_cls is None:
            raise TypeError(
                f"{client_cls.__name__} has no _router_class. "
                "Parameterize it with a VarcoRouter: "
                f"class {client_cls.__name__}(VarcoClient[YourRouter]): ..."
            )

        # Create a typed alias for the registration key (VarcoClient[RouterType])
        # This is the same trick bind_repositories() uses in varco_sa.di
        client_alias = AsyncVarcoClient[router_cls]  # type: ignore[valid-type]

        # Capture client_cls in closure to avoid late-binding
        _cls = client_cls

        def _factory(__cls: type = _cls) -> Any:
            """Singleton factory — DI container calls this once."""
            return __cls()

        # Registration goes through providify's native
        # container.provide(Provider(...)(factory), returns=...) — the
        # decoration-time returns= override means the factory's placeholder
        # return annotation never needs patching. (Same pattern
        # bind_repositories() in varco_sa.di uses.)
        #
        # DESIGN: no fallback chain around registration.
        #
        # `provide()` only accepts @Provider-decorated callables, and `bind()`
        # rejects a function outright (it calls `issubclass()` on it). Three
        # nested `except Exception` fallbacks used to sit here and could
        # therefore never succeed — they only converted a precise error into a
        # confusing one from the last branch, and would silently swallow a
        # genuine registration failure if any branch ever did succeed by
        # accident.
        #
        #   ✅ A registration failure surfaces immediately, naming the real cause.
        #   ✅ One code path — what is tested is what runs.
        #   ❌ No "best effort" partial registration; a bad client class aborts
        #      the whole bind_clients() call (intended — a half-wired container
        #      fails later, further from the cause).
        container.provide(Provider(singleton=True)(_factory), returns=client_alias)


def bind_clients_from(container: Any, *router_classes: type) -> None:
    """
    ``client_class_for()`` each router, then ``bind_clients()`` the results
    (Plan 009, Phase 3 / C1).

    The front-door counterpart to ``bind_clients()`` — call sites that only
    have router classes (not hand-built ``AsyncVarcoClient`` subclasses) use
    this instead.

    Args:
        container:       ``DIContainer`` instance to register into.
        *router_classes: One or more ``VarcoRouter`` subclasses.

    Usage::

        bind_clients_from(container, OrderRouter, UserRouter)
        orders_client = await container.aget(VarcoClient[OrderRouter])
    """
    from varco_fastapi.client.configurator import ClientConfigurator
    from varco_fastapi.client.front_door import client_class_for

    class _DeferredUrlConfigurator(ClientConfigurator[Any]):
        """
        Resolves to an empty URL rather than raising ``NotImplementedError``
        at construction time.

        ``bind_clients()``'s generated factory calls the client class with
        zero arguments — a dynamically-built ``client_class_for()`` class has
        no hand-authored ``__init__`` supplying a ``base_url``, unlike a
        manually declared ``AsyncVarcoClient[R]`` subclass. Without SOME
        configurator, ``AsyncVarcoClient.__init__`` raises ``ValueError``
        immediately. This preserves the plan's documented "deferred error at
        first request" contract instead: the real failure — "no base_url" —
        surfaces on the first actual HTTP call, naming
        ``client_for(..., base_url=)`` / (Phase 11) ``VARCO_PEER_<NAME>_URL``,
        not at DI-resolution time.
        """

        def default_url(self) -> str:
            return ""

    # Subclass (not mutate) the memoized client_class_for() class — that
    # class is shared with client_for(), and setting _configurator on it
    # in place would silently change client_for()'s own no-base_url
    # behaviour for every other caller of the same router.
    client_classes = tuple(
        type(
            f"{base.__name__}Bound",
            (base,),
            {"_configurator": _DeferredUrlConfigurator},
        )
        for base in (client_class_for(r) for r in router_classes)
    )
    bind_clients(container, *client_classes)


# ── VarcoFastAPIModule ────────────────────────────────────────────────────────


@Configuration
class VarcoFastAPIModule:
    """
    ``@Configuration`` module for varco_fastapi defaults.

    Discovered and auto-installed by ``container.scan("varco_fastapi", recursive=True)``.
    No explicit ``container.install(VarcoFastAPIModule)`` call is required.

    Registers (all overrideable via ``container.bind()`` before scanning):
        - ``TrustStore``               — from ``VARCO_TRUST_STORE_DIR`` env vars
        - ``CORSConfig``               — from ``VARCO_CORS_ORIGINS`` env var
        - ``ClientProfile``            — from ``VARCO_CLIENT_TIMEOUT`` env vars
        - ``JwtTransformSettings``     — from ``VARCO_JWT_TRANSFORM_*`` env vars
        - ``ClaimTransformerRegistry`` — from ``JwtTransformConfig.from_env()``
        - ``TaskRegistry``             — shared singleton across all routers

    ``RequestContext``/``JwtContext`` are registered by
    ``varco_fastapi.context``'s own module-level ``@Provider`` functions
    (``get_request_context``/``get_jwt_context``), not by this class — see
    that module for the per-request ContextVar bindings.

    Thread safety:  ✅ Module instance is created once at install() time.
    Async safety:   ✅ All providers are synchronous.
    """

    @Fallback
    @Provider(singleton=True)
    def trust_store(self) -> TrustStore:
        """
        TLS trust store loaded from env vars (``VARCO_TRUST_STORE_DIR``, etc.).

        Returns:
            A ``TrustStore`` configured from the environment.
        """
        return TrustStore.from_env()

    @Fallback
    @Provider(singleton=True)
    def cors_config(self) -> CORSConfig:
        """
        CORS configuration from ``VARCO_CORS_ORIGINS`` env var. An unset
        var yields an empty origin list — no cross-origin access (AB-5).

        Returns:
            A ``CORSConfig`` configured from the environment.
        """
        return CORSConfig.from_env()

    @Fallback
    @Provider(singleton=True)
    def client_profile(self) -> ClientProfile:
        """
        Default HTTP client profile from ``VARCO_CLIENT_TIMEOUT`` env vars.

        Returns:
            A ``ClientProfile`` configured from the environment.
        """
        return ClientProfile.from_env()

    @Fallback
    @Provider(singleton=True)
    def profiling_settings(self) -> ProfilingSettings:
        """
        Diagnostic profiler settings loaded from ``VARCO_PROFILER_*`` env vars.

        ⚠️ The return annotation must NOT be quoted and ``ProfilingSettings``
        must stay imported at module scope (see the import block at the top of
        this file).  Under PEP 563 a quoted annotation ``-> "ProfilingSettings"``
        is stored as the *string* ``"'ProfilingSettings'"``; when providify's
        ``get_type_hints()`` path fails it falls back to ``eval()``, which then
        yields the plain string ``'ProfilingSettings'`` and registers it as the
        binding *interface*.  ``DIContainer._build_localns()`` subsequently
        raises ``AttributeError: 'str' object has no attribute '__name__'``,
        which ``_collect_kwargs_sync()`` swallows with ``hints = {}`` — after
        which **every** provider in that container is invoked with zero
        injected arguments (``TypeError: ... missing 1 required positional
        argument``).  One quoted annotation silently disables DI container-wide.

        Returns:
            A ``ProfilingSettings`` instance configured from the environment.
        """
        return ProfilingSettings()

    @Fallback
    @Provider(singleton=True)
    def jwt_transform_settings(self) -> JwtTransformSettings:
        """
        Flat ``VARCO_JWT_TRANSFORM_*`` claim-transform settings, loaded from
        the environment (Plan 002 Phase 2).

        ⚠️ ``@Provider``, never ``@Singleton`` — pydantic ``BaseSettings``
        takes ``**values`` in its constructor; providify cannot resolve that
        as an injectable signature (see ``feedback_di_defaults`` /
        ``varco_casbin/di.py`` for the established precedent).

        Returns:
            A ``JwtTransformSettings`` instance built via ``.from_env()``.
        """
        return JwtTransformSettings.from_env()

    @Fallback
    @Provider(singleton=True)
    def claim_transformer_registry(self) -> ClaimTransformerRegistry:
        """
        The fully parsed claim-transformer registry (global mapping + every
        per-issuer override), built from the environment.

        This is a pure provider — it does NOT install itself as the
        process-global registry ``JwtParser`` reads from
        (``varco_core.jwt.transform.runtime``).  ``create_varco_app()``
        calls ``configure_jwt_from_env()`` separately at startup so the
        DI-resolved object and the process-global registry stay in sync
        without this provider having a side effect on construction (pure
        providers are easier to test in isolation and safe to resolve more
        than once).

        Returns:
            A ``ClaimTransformerRegistry`` built via
            ``JwtTransformConfig.from_env().to_registry()``.
        """
        return JwtTransformConfig.from_env().to_registry()

    @Fallback
    @Provider(singleton=True)
    def task_registry(self) -> TaskRegistry:
        """
        Shared ``TaskRegistry`` singleton used by all ``VarcoCRUDRouter`` instances.

        All ``build_router()`` calls register their CRUD action tasks here so
        ``JobRunner.recover()`` can re-submit PENDING jobs after a restart.

        Returns:
            A new ``TaskRegistry`` shared across all routers.
        """
        return TaskRegistry()

    @Fallback
    @Provider(singleton=True)
    def i18n_settings(self) -> I18nSettings:
        """
        ``I18nSettings`` from ``VARCO_I18N_*`` env vars — off by default
        (Plan 011, RD-1's I2 row).

        ⚠️ ``@Provider``, never ``@Singleton`` — pydantic ``BaseSettings``
        takes ``**values``; same rule as ``jwt_transform_settings`` above.
        """
        return I18nSettings()

    @Fallback
    @Provider(singleton=True)
    def timezone_settings(self) -> TimezoneSettings:
        """``TimezoneSettings`` from ``VARCO_TZ_*`` env vars — off by default."""
        return TimezoneSettings()

    @Fallback
    @Provider(singleton=True)
    def message_catalog(self) -> MessageCatalog:
        """
        Framework-default ``MessageCatalog`` binding — ``NullMessageCatalog``
        (zero I/O, ``get_message`` always ``None``). Lowest priority so any
        app-provided catalog (``DictMessageCatalog``, ``GettextMessageCatalog``,
        or a custom implementation) wins regardless of registration order.
        """
        return NullMessageCatalog()

    @Fallback
    @Provider(singleton=True)
    def tenant_defaults_provider(self) -> TenantDefaultsProvider:
        """
        Framework-default ``TenantDefaultsProvider`` binding —
        ``NullTenantDefaults`` (RD-2's zero-I/O default).
        """
        return NullTenantDefaults()

    # Plan 045 / §D-validate: `request_context`/`jwt_context` @Provider
    # methods that used to live here are REMOVED — they were byte-identical
    # duplicates of `varco_fastapi.context.get_request_context`/
    # `get_jwt_context` (both `@Provider`-decorated module-level functions,
    # discovered by the same `container.scan("varco_fastapi", recursive=True)`
    # call). Under the pre-045 floor-priority model this duplication was
    # invisible: this class's copies were registered at the old negative-
    # ceiling priority floor and always lost silently to the bare
    # (priority 0) `context.py` originals. Marking both `@Fallback` made the
    # duplication visible for the first time, as a genuine
    # `FALLBACK_SHADOWED` INFO — one varco default shadowing another varco
    # default at equal (fallback) tier, the exact defect §D-validate's
    # `report.infos == ()` assertion exists to catch. Deleting the dead
    # copies here (rather than allowlisting the INFO) is the correct fix:
    # `context.py`'s originals already fully cover both bindings.

    @Fallback
    @Provider(singleton=True)
    def task_serializer(self) -> TaskSerializer:
        """
        Framework-default ``TaskSerializer`` — ``DefaultTaskSerializer``
        (Plan 045: replaces the removed ``setup_varco_defaults()`` helper,
        which used to bind this via an imperative ``container.bind()`` call
        the caller had to remember to make). ``@Fallback`` — any application
        binding of ``TaskSerializer`` wins regardless of registration order.

        Returns:
            A ``DefaultTaskSerializer`` instance.
        """
        return DefaultTaskSerializer()


def create_varco_container(*packages: str) -> Any:
    """
    Create (or return) the global ``DIContainer`` and scan varco packages.

    Uses ``DIContainer.current()`` to access the process-level singleton
    container — all calls within the same process share the same instance.
    The container is scanned for ``@Singleton`` / ``@Component`` classes
    in every package passed.  Pass your application's top-level package
    last to ensure backend classes are registered before app-level overrides.

    DESIGN: DIContainer.current() over explicit passing
        ✅ Application code never needs to pass the container around —
           it is retrieved from the global context whenever needed.
        ✅ Consistent with how FastAPI route factories call
           ``DIContainer.current().get(...)`` to resolve dependencies.
        ❌ Harder to use in tests that need an isolated container — for tests,
           create a fresh ``DIContainer()`` directly and scope it to the test.

    Args:
        *packages: Package names to scan (e.g. ``"varco_core"``,
                   ``"varco_redis"``, ``"myapp"``).  The container scans
                   each package recursively to discover all scope-annotated
                   classes (``@Singleton``, ``@Component``, etc.).

    Returns:
        The global ``DIContainer`` instance after scanning.

    Example::

        # Application bootstrap
        container = create_varco_container(
            "varco_core",   # InMemoryEventBus, InMemoryLock, etc.
            "varco_redis",  # RedisEventBus, RedisHealthCheck, etc.
            "varco_sa",     # SQLAlchemyRepositoryProvider, SAHealthCheck, etc.
            "myapp",        # application-level @Singleton services
        )
        container.install(VarcoFastAPIModule)
        bind_repositories(container, User, Post)
        await container.awarm_up()  # triggers all @PostConstruct methods

    Edge cases:
        - If ``packages`` is empty, no scanning occurs — the container is
          returned as-is with only manually registered bindings.
        - Calling twice with the same package is safe — scanning is idempotent;
          already-registered classes are not re-registered.
        - Returns ``None`` if providify is not installed.

    Thread safety:  ✅ Intended for single-threaded bootstrap only.
    Async safety:   ✅ No async operations — scanning is synchronous.
    """
    try:
        from providify import DIContainer
    except ImportError:
        return None

    container = DIContainer.current()
    for package in packages:
        # Recursive scan — discovers all @Singleton / @Component classes
        # in every submodule of the given package.
        container.scan(package, recursive=True)

    return container


def setup_event_producer(container: Any) -> None:
    """
    Bind ``BusEventProducer`` as the default ``AbstractEventProducer`` implementation.

    Call once after an event bus has been registered (e.g. via
    ``container.scan("varco_redis")`` / ``bootstrap()``) to wire the producer
    abstraction that ``AsyncService`` and ``CacheServiceMixin`` depend on::

        container = DIContainer()
        container.scan("varco_redis", recursive=True)  # binds AbstractEventBus
        container.install(SAModule)
        container.install(VarcoFastAPIModule)
        setup_event_producer(container)  # binds AbstractEventProducer → BusEventProducer

    After this call, all ``AsyncService`` subclasses with an optional
    ``AbstractEventProducer`` injection will receive a live ``BusEventProducer``
    instead of the fallback ``NoopEventProducer``.  ``CacheServiceMixin``
    will also emit ``CacheInvalidated`` events cross-process.

    Override before or after this call to supply a custom producer::

        setup_event_producer(container)
        container.bind(AbstractEventProducer, MyCustomProducer)  # wins

    DESIGN: separate helper over auto-wiring inside VarcoFastAPIModule
        ✅ The event bus module must be installed BEFORE the producer is created
           (``BusEventProducer`` holds a reference to ``AbstractEventBus``).
           A helper called explicitly after bus install enforces this order.
        ✅ Apps that do not use events skip this call entirely — zero overhead.
        ❌ One extra bootstrap call — small ergonomic cost, large safety gain.

    Args:
        container: The ``DIContainer`` to register the binding into.
                   Must have an ``AbstractEventBus`` binding already registered
                   (from a scanned bus package like ``varco_redis``).

    Raises:
        LookupError: Raised lazily at resolution time if ``AbstractEventBus``
                     is not registered when ``AbstractEventProducer`` is first
                     resolved.  Install the bus module before this call to
                     surface the error at startup.

    Edge cases:
        - Calling twice adds a second binding; the container uses the
          higher-priority one.  Safe but unnecessary — call once.
        - If no bus module is installed, ``BusEventProducer`` construction
          will raise ``LookupError`` at first resolution.  Always install a
          bus module first.
        - ``NoopEventProducer`` is still used in service subclasses that are
          constructed before this binding is resolved — the Null Object
          pattern in ``AsyncService.__init__`` applies only as a constructor
          fallback.  With a proper DI container, resolution happens lazily
          so the binding will always be found before any service is used.

    Thread safety:  ✅ Registration is expected at bootstrap (single-threaded).
    Async safety:   ✅ No I/O during registration.
    """
    # AbstractEventProducer, BusEventProducer, AbstractEventBus, Provider, Singleton
    # are all imported at module level — no local imports needed here.

    # BusEventProducer is already declared `@Fallback @Singleton` at its own
    # definition (varco_core.event.producer) — no re-stamp needed here.

    # Register a @Provider that injects AbstractEventBus into BusEventProducer.
    # Named function (not lambda) for clean describe() output in the container.
    @Provider(singleton=True)
    def _bus_event_producer(bus: AbstractEventBus) -> AbstractEventProducer:
        """Default BusEventProducer — wraps the registered AbstractEventBus."""
        return BusEventProducer(bus=bus)

    container.provide(_bus_event_producer)


__all__ = [
    "VarcoFastAPIModule",
    "bind_clients",
    "bootstrap",
    "create_varco_container",
    "setup_event_producer",
    # MCP / Skill adapter DI helpers — re-exported from their modules for
    # one-stop import: from varco_fastapi.di import bind_mcp_adapter
    "bind_mcp_adapter",
    "bind_skill_adapter",
]


# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(
    container: Any = None,
    *packages: str,
    setup_producer: bool = False,
) -> Any:
    """
    Bootstrap ``varco_fastapi`` into a ``DIContainer``.

    Installs :data:`VarcoFastAPIModule`, calls
    ``container.scan("varco_fastapi", recursive=True)`` plus any
    additional ``packages``, and optionally runs
    :func:`setup_event_producer`. Every framework default — including
    ``TaskSerializer → DefaultTaskSerializer`` — is registered by the scan
    itself (each is a ``@Fallback``-marked binding on ``VarcoFastAPIModule``
    or a scanned ``@Singleton`` class); there is no longer a separate
    "setup defaults" step to opt into or skip.

    This is a convenience wrapper around :func:`create_varco_container`
    that also installs the FastAPI module — the minimum required setup
    for a ``varco_fastapi``-based application::

        from varco_fastapi.di import bootstrap

        container = bootstrap("myapp")   # scans varco_fastapi + myapp
        # All VarcoFastAPIModule providers are available immediately

    Full application bootstrap with a Redis event bus::

        from varco_redis.di import bootstrap as redis_bootstrap
        from varco_fastapi.di import bootstrap as fastapi_bootstrap

        container = await redis_bootstrap()
        fastapi_bootstrap(container, "myapp", setup_producer=True)
        # AbstractEventProducer → BusEventProducer is now bound

    Args:
        container:       An existing ``DIContainer`` to install into.
                         When ``None``, ``DIContainer.current()`` is used —
                         the process-level singleton.
        *packages:       Additional package names to scan after
                         ``"varco_fastapi"`` — typically your application
                         package (e.g. ``"myapp"``).
        setup_producer:  When ``True``, calls :func:`setup_event_producer`
                         to bind ``AbstractEventProducer → BusEventProducer``.
                         Only meaningful after a bus module is installed.

    Returns:
        The ``DIContainer`` after installation, scanning, and optional
        default bindings.

    Edge cases:
        - ``setup_producer=True`` without a bus module installed causes a
          ``LookupError`` at first ``AbstractEventProducer`` resolution.
          Always install a bus module before passing ``setup_producer=True``.
        - ``VarcoFastAPIModule`` is ``None`` when providify is not installed —
          ``bootstrap()`` returns ``None`` in that case.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ No I/O during installation; providers are called lazily.
    """
    try:
        from providify import DIContainer
    except ImportError:
        return None

    if container is None:
        # Use the process-level singleton container — consistent with
        # create_varco_container().
        container = DIContainer.current()

    # Scan varco_fastapi itself, then any caller-supplied packages.
    # The scanner discovers VarcoFastAPIModule (@Configuration) and calls
    # container.install(VarcoFastAPIModule) automatically — no explicit
    # install() call needed.  varco_fastapi is scanned first so its
    # @Singleton classes are registered before application-level overrides.
    all_packages = ("varco_fastapi", *packages)
    for pkg in all_packages:
        container.scan(pkg, recursive=True)

    if setup_producer:
        setup_event_producer(container)

    return container


# ── Re-export adapter DI helpers for one-stop import convenience ──────────────


def bind_mcp_adapter(*args: Any, **kwargs: Any) -> None:
    """
    Re-export of ``varco_fastapi.router.mcp.bind_mcp_adapter``.

    Convenience alias so DI wiring code can import everything from one place::

        from varco_fastapi.di import (
            VarcoFastAPIModule,
            bind_clients,
            bind_mcp_adapter,
            bind_skill_adapter,
        )

    See ``varco_fastapi.router.mcp.bind_mcp_adapter`` for full documentation.
    """
    from varco_fastapi.router.mcp import bind_mcp_adapter as _impl

    return _impl(*args, **kwargs)


def bind_skill_adapter(*args: Any, **kwargs: Any) -> None:
    """
    Re-export of ``varco_fastapi.router.skill.bind_skill_adapter``.

    Convenience alias so DI wiring code can import everything from one place.

    See ``varco_fastapi.router.skill.bind_skill_adapter`` for full documentation.
    """
    from varco_fastapi.router.skill import bind_skill_adapter as _impl

    return _impl(*args, **kwargs)
