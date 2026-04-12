"""
varco_fastapi.di
==================
Central DI module for varco_fastapi.

``VarcoFastAPIModule`` is a ``@Configuration`` class that registers the default
varco_fastapi providers in a ``DIContainer``.  Install it alongside backend
modules (SA, Redis, etc.) at application bootstrap::

    container = DIContainer()
    await container.ainstall(KafkaEventBusConfiguration)
    container.install(SAModule)
    container.install(VarcoFastAPIModule)
    bind_clients(container, OrderClient, UserClient)

Registered defaults (all overrideable via ``container.bind()``):

    AbstractJobStore   → InMemoryJobStore
    AbstractJobRunner  → JobRunner(store)
    AbstractServerAuth → JwtBearerAuth (requires TrustedIssuerRegistry in container)
    TrustStore         → TrustStore.from_env()
    CORSConfig         → CORSConfig.from_env()
    ClientProfile      → ClientProfile.from_env()
    TaskRegistry       → TaskRegistry() (singleton)

``setup_varco_defaults(container)`` — call once after ``container.install(VarcoFastAPIModule)``
to register singleton default implementations for framework ABCs::

    container.install(VarcoFastAPIModule)
    setup_varco_defaults(container)   # binds TaskSerializer → DefaultTaskSerializer

Defaults registered by ``setup_varco_defaults``:
    TaskSerializer → DefaultTaskSerializer (@Singleton, priority=-sys.maxsize-1)

Override before or after calling ``setup_varco_defaults``::

    container.bind(TaskSerializer, MySerializer)  # overrides default

``bind_clients()`` mirrors ``bind_repositories()`` from ``varco_sa.di`` — patches
``__annotations__["return"]`` so providify resolves ``Inject[VarcoClient[OrderRouter]]``
correctly at injection time.

DESIGN: bind_clients() helper over manual provider registration
    ✅ Same pattern as bind_repositories() — familiar to varco users
    ✅ Handles annotation patching for generic type resolution automatically
    ✅ One call registers multiple clients
    ❌ Requires concrete client classes (not dynamic) — use RestClientBuilder for that

Thread safety:  ✅ DI registration is single-threaded at bootstrap time.
Async safety:   ✅ No I/O during registration; providers are called lazily.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

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
from varco_fastapi.context import (
    JwtContext,
    RequestContext,
    get_jwt_context,
    get_request_context,
)
from varco_fastapi.middleware.cors import CORSConfig
from varco_core.job.task import TaskRegistry
from varco_core.job.serializer import DefaultTaskSerializer, TaskSerializer

# Event producer types — also needed by setup_event_producer() for the same reason.
from varco_core.event.base import AbstractEventBus
from varco_core.event.producer import AbstractEventProducer, BusEventProducer

from providify import Configuration, Provider, Singleton

if TYPE_CHECKING:
    pass


# ── Singleton stamps for varco_core classes (no providify dep there) ──────────
# varco_core must stay providify-free — apply scope decorators here, once, at
# module import time.  Idempotent: stamping an already-decorated class is harmless.
#
# priority = -sys.maxsize - 1  →  lowest possible, so any user-supplied binding
# for these types wins automatically (user priority > default).
Singleton(priority=-sys.maxsize - 1)(DefaultTaskSerializer)
Singleton(priority=-sys.maxsize - 1)(TaskRegistry)


def bind_clients(container: Any, *client_classes: type) -> None:
    """
    Register ``AsyncVarcoClient`` subclasses in the DI container.

    Creates a singleton ``@Provider`` for each client class and registers it
    under ``VarcoClient[RouterType]`` so injections like
    ``Inject[VarcoClient[OrderRouter]]`` resolve correctly.

    Implementation (same pattern as ``bind_repositories()`` in ``varco_sa.di``):

    1. Read ``_router_class`` ClassVar to get the router type.
    2. Create a ``@Provider(singleton=True)`` factory function.
    3. Patch ``factory.__annotations__["return"]`` to ``VarcoClient[router_type]``.
    4. Register in the container.

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
        - Same client registered twice → second registration wins (DI container
          replaces the earlier binding).
        - No configurator → client will need an explicit ``base_url`` at construction.

    Thread safety:  ✅ Registration happens at bootstrap; no concurrent access.
    Async safety:   ✅ No I/O during registration.
    """
    from varco_fastapi.client.base import AsyncVarcoClient  # noqa: PLC0415

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

        # Patch return annotation so providify generic resolution works
        # (same pattern as bind_repositories: patches __annotations__["return"])
        _factory.__annotations__["return"] = client_alias

        # Register as a singleton provider under the generic alias
        try:
            # providify uses container.bind() with a factory callable
            container.bind(client_alias, _factory)
        except Exception:
            # Fall back to container.provide() if bind() doesn't accept callables
            try:
                container.provide(_factory)
            except Exception:
                # Last resort: register the concrete class directly
                container.bind(client_cls, client_cls)


# ── VarcoFastAPIModule ────────────────────────────────────────────────────────


@Configuration
class VarcoFastAPIModule:
    """
    ``@Configuration`` module for varco_fastapi defaults.

    Discovered and auto-installed by ``container.scan("varco_fastapi", recursive=True)``.
    No explicit ``container.install(VarcoFastAPIModule)`` call is required.

    Registers (all overrideable via ``container.bind()`` before scanning):
        - ``TrustStore``       — from ``VARCO_TRUST_STORE_DIR`` env vars
        - ``CORSConfig``       — from ``VARCO_CORS_ORIGINS`` env var
        - ``ClientProfile``    — from ``VARCO_CLIENT_TIMEOUT`` env vars
        - ``TaskRegistry``     — shared singleton across all routers
        - ``RequestContext``   — per-request ContextVar (non-singleton)
        - ``JwtContext``       — per-request JWT ContextVar (non-singleton)

    Thread safety:  ✅ Module instance is created once at install() time.
    Async safety:   ✅ All providers are synchronous.
    """

    @Provider(singleton=True)
    def trust_store(self) -> TrustStore:
        """
        TLS trust store loaded from env vars (``VARCO_TRUST_STORE_DIR``, etc.).

        Returns:
            A ``TrustStore`` configured from the environment.
        """
        return TrustStore.from_env()

    @Provider(singleton=True)
    def cors_config(self) -> CORSConfig:
        """
        CORS configuration from ``VARCO_CORS_ORIGINS`` env var.

        Returns:
            A ``CORSConfig`` configured from the environment.
        """
        return CORSConfig.from_env()

    @Provider(singleton=True)
    def client_profile(self) -> ClientProfile:
        """
        Default HTTP client profile from ``VARCO_CLIENT_TIMEOUT`` env vars.

        Returns:
            A ``ClientProfile`` configured from the environment.
        """
        return ClientProfile.from_env()

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

    @Provider
    def request_context(self) -> RequestContext:
        """
        Current request context — auth, request ID, raw token.

        Non-singleton: ``RequestContext`` is per-request state stored in a
        ``ContextVar``.  The container wraps it in ``Live[RequestContext]`` so
        singletons always receive the current request's values.

        Returns:
            The ``RequestContext`` for the current asyncio task.
        """
        return get_request_context()

    @Provider
    def jwt_context(self) -> JwtContext:
        """
        Parsed JWT payload for the current request.

        Non-singleton for the same reason as ``request_context`` above.

        Returns:
            The ``JwtContext`` for the current asyncio task.
        """
        return get_jwt_context()


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
        await container.ainstall(RedisEventBusConfiguration)
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
        from providify import DIContainer  # noqa: PLC0415
    except ImportError:
        return None

    container = DIContainer.current()
    for package in packages:
        # Recursive scan — discovers all @Singleton / @Component classes
        # in every submodule of the given package.
        container.scan(package, recursive=True)

    return container


def setup_varco_defaults(container: Any) -> None:
    """
    Bind framework ABC defaults into the DI container.

    Call once after ``container.install(VarcoFastAPIModule)`` to register
    default implementations for framework ABCs that use ``@Singleton`` /
    ``@Component`` scope on the class rather than a ``@Provider`` factory.

    Registered bindings:
        ``TaskSerializer`` → ``DefaultTaskSerializer`` (@Singleton, lowest priority)

    Override before or after calling this function::

        setup_varco_defaults(container)
        container.bind(TaskSerializer, MyCustomSerializer)  # wins — higher priority

    Args:
        container: The ``DIContainer`` to register defaults into.
                   Must have ``VarcoFastAPIModule`` already installed.

    Edge cases:
        - Calling twice is safe — ``ClassBinding`` for the same interface is
          appended, and the container picks the highest-priority match.
        - If ``VarcoFastAPIModule`` was not installed (providify absent), this
          function is a no-op — the absence of a binding is handled gracefully
          by ``VarcoCRUDRouter._task_serializer.is_resolvable()`` falling back
          to ``DEFAULT_SERIALIZER``.

    Thread safety:  ✅ Registration is expected at bootstrap (single-threaded).
    Async safety:   ✅ No I/O during registration.
    """
    # DefaultTaskSerializer is @Singleton (stamped at module level above).
    # TaskSerializer and DefaultTaskSerializer are imported at module level, but
    # we re-use the module-level names here — no local import needed.
    # container.bind() respects class-level scope metadata — no singleton=True
    # needed on the @Provider side because scope lives on the class itself.
    container.bind(TaskSerializer, DefaultTaskSerializer)


def setup_event_producer(container: Any) -> None:
    """
    Bind ``BusEventProducer`` as the default ``AbstractEventProducer`` implementation.

    Call once after an event bus module is installed
    (e.g. ``RedisEventBusConfiguration``, ``KafkaEventBusConfiguration``)
    to wire the producer abstraction that ``AsyncService`` and
    ``CacheServiceMixin`` depend on::

        container = DIContainer()
        await container.ainstall(RedisEventBusConfiguration)  # binds AbstractEventBus
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
        ✅ Mirrors the ``setup_varco_defaults`` pattern — consistent API.
        ❌ One extra bootstrap call — small ergonomic cost, large safety gain.

    Args:
        container: The ``DIContainer`` to register the binding into.
                   Must have an ``AbstractEventBus`` binding already registered
                   (from a bus module like ``RedisEventBusConfiguration``).

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

    # Apply @Singleton to BusEventProducer so DI creates a single shared
    # producer for all services.  BusEventProducer is stateless once
    # constructed (it just delegates to the bus) — safe to share.
    #
    # priority = -sys.maxsize - 1 → lowest possible, so any user-supplied
    # AbstractEventProducer binding wins automatically.
    Singleton(priority=-sys.maxsize - 1)(BusEventProducer)

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
    "setup_varco_defaults",
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
    setup_defaults: bool = True,
    setup_producer: bool = False,
) -> Any:
    """
    Bootstrap ``varco_fastapi`` into a ``DIContainer``.

    Installs :data:`VarcoFastAPIModule`, calls
    ``container.scan("varco_fastapi", recursive=True)`` plus any
    additional ``packages``, and optionally runs
    :func:`setup_varco_defaults` and :func:`setup_event_producer`.

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
        setup_defaults:  When ``True`` (default), calls
                         :func:`setup_varco_defaults` to bind
                         ``TaskSerializer → DefaultTaskSerializer``.
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
        from providify import DIContainer  # noqa: PLC0415
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

    if setup_defaults:
        setup_varco_defaults(container)

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
    from varco_fastapi.router.mcp import bind_mcp_adapter as _impl  # noqa: PLC0415

    return _impl(*args, **kwargs)


def bind_skill_adapter(*args: Any, **kwargs: Any) -> None:
    """
    Re-export of ``varco_fastapi.router.skill.bind_skill_adapter``.

    Convenience alias so DI wiring code can import everything from one place.

    See ``varco_fastapi.router.skill.bind_skill_adapter`` for full documentation.
    """
    from varco_fastapi.router.skill import bind_skill_adapter as _impl  # noqa: PLC0415

    return _impl(*args, **kwargs)
