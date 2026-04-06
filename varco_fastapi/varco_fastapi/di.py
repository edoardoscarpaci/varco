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

from typing import TYPE_CHECKING, Any

# ── Module-level imports for DI type resolution ───────────────────────────────
# These must live at module scope — NOT inside _try_build_module() — because
# ``from __future__ import annotations`` turns all annotations into strings.
# When providify calls ``typing.get_type_hints(provider_method)`` it resolves
# those strings using the function's ``__globals__`` (== this module's globals).
# If a type is only imported locally inside _try_build_module(), it is never
# added to ``__globals__`` and ``get_type_hints`` raises NameError, which
# providify surfaces as "Provider must declare a return type hint".
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
from varco_core.job.serializer import DefaultTaskSerializer

# Event producer types — also needed by setup_event_producer() for the same reason.
from varco_core.event.base import AbstractEventBus
from varco_core.event.producer import AbstractEventProducer, BusEventProducer

if TYPE_CHECKING:
    pass


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


def _try_build_module() -> type | None:
    """
    Attempt to build the ``VarcoFastAPIModule`` ``@Configuration`` class.

    Returns ``None`` if providify is not installed.

    DESIGN: lazy import + None fallback over hard dependency on providify
        ✅ varco_fastapi can be used without DI (pass explicit instances)
        ✅ No ImportError at module load time if providify is absent
        ❌ Users must check if VarcoFastAPIModule is None before installing
    """
    try:
        from providify import Configuration, Provider, Singleton  # noqa: PLC0415
    except ImportError:
        return None

    # ── Apply @Singleton to DefaultTaskSerializer ─────────────────────────────
    # (All type imports have been promoted to module level — see the comment near
    # the top of this file for why ``from __future__ import annotations`` forces
    # them into __globals__ for get_type_hints() to resolve provider return types.)
    # varco_core has no dependency on providify, so the scope decorator cannot
    # live in that package.  We apply it here, where providify IS available,
    # as a one-time idempotent side-effect.
    #
    # priority = -sys.maxsize - 1  →  lowest possible priority, so any
    # user-supplied binding for TaskSerializer wins automatically.
    #
    # DESIGN: side-effect in _try_build_module (module import time)
    #   ✅ Applied exactly once — _try_build_module is only called once at module load
    #   ✅ Idempotent — calling Singleton() on an already-decorated class is harmless
    #   ✅ Keeps varco_core zero-dependency (no providify import there)
    #   ❌ Slightly non-obvious — mitigated by this comment
    import sys as _sys  # noqa: PLC0415

    Singleton(priority=-_sys.maxsize - 1)(DefaultTaskSerializer)

    @Configuration
    class VarcoFastAPIModule:
        """
        Default DI bindings for varco_fastapi.

        Override individual providers by registering a replacement binding
        in your application's ``@Configuration`` class before installing this
        module, or by calling ``container.bind()`` after installation.

        Registered providers:
            AbstractJobStore  → InMemoryJobStore (singleton)
            AbstractJobRunner → JobRunner(store) (singleton)
            TrustStore        → TrustStore.from_env() (singleton)
            CORSConfig        → CORSConfig.from_env() (singleton)
            ClientProfile     → ClientProfile.from_env() (singleton)
            TaskRegistry      → TaskRegistry() (singleton, shared across all routers)
            RequestContext    → get_request_context() (Live — always current request)
            JwtContext        → get_jwt_context() (Live — always current request)

        ``TaskSerializer`` is NOT registered here — call ``setup_varco_defaults(container)``
        after installation to bind ``TaskSerializer → DefaultTaskSerializer``.

        Thread safety:  ✅ Provider methods are called once (singleton) by the container.
        Async safety:   ✅ No async providers here — all constructions are sync.
        """

        @Provider(singleton=True)
        def trust_store(self) -> TrustStore:
            """TLS trust store from env vars (``VARCO_TRUST_STORE_DIR``, etc.)."""
            return TrustStore.from_env()

        @Provider(singleton=True)
        def cors_config(self) -> CORSConfig:
            """CORS configuration from env vars (``VARCO_CORS_ORIGINS``, etc.)."""
            return CORSConfig.from_env()

        @Provider(singleton=True)
        def client_profile(self) -> ClientProfile:
            """Default client profile from env vars (``VARCO_CLIENT_TIMEOUT``, etc.)."""
            return ClientProfile.from_env()

        @Provider(singleton=True)
        def task_registry(self) -> TaskRegistry:
            """
            Shared singleton task registry.

            All ``VarcoCRUDRouter.build_router()`` calls register their CRUD
            action tasks here, making them available to ``JobRunner.recover()``.
            """
            return TaskRegistry()

        @Provider
        def request_context(self) -> RequestContext:
            """
            Current request context — auth, request ID, raw token.

            Wrapped in ``Live[RequestContext]`` by the DI container so singletons
            always get the current request's values, not a stale snapshot.
            """
            return get_request_context()

        @Provider
        def jwt_context(self) -> JwtContext:
            """
            Parsed JWT for the current request.

            Wrapped in ``Live[JwtContext]`` by the DI container.
            Token is parsed without signature verification.
            """
            return get_jwt_context()

    return VarcoFastAPIModule


# Build once at import time; will be None if providify is not installed
VarcoFastAPIModule = _try_build_module()


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
    try:
        from varco_core.job.serializer import (
            DefaultTaskSerializer,
            TaskSerializer,
        )  # noqa: PLC0415
    except ImportError:
        return  # varco_core not installed — nothing to bind

    # DefaultTaskSerializer is @Singleton (stamped in _try_build_module above).
    # container.bind() respects the class-level scope metadata — no singleton=True
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
    # AbstractEventProducer, BusEventProducer, AbstractEventBus are already
    # imported at module level so get_type_hints() can resolve them.
    try:
        from providify import Provider, Singleton  # noqa: PLC0415
    except ImportError:
        # providify not installed — graceful no-op.
        return

    # Apply @Singleton to BusEventProducer so DI creates a single shared
    # producer for all services.  BusEventProducer is stateless once
    # constructed (it just delegates to the bus) — safe to share.
    #
    # priority = -sys.maxsize - 1 → lowest possible, so any user-supplied
    # AbstractEventProducer binding wins automatically.
    import sys as _sys  # noqa: PLC0415

    Singleton(priority=-_sys.maxsize - 1)(BusEventProducer)

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
    "create_varco_container",
    "setup_varco_defaults",
    "setup_event_producer",
    # MCP / Skill adapter DI helpers — re-exported from their modules for
    # one-stop import: from varco_fastapi.di import bind_mcp_adapter
    "bind_mcp_adapter",
    "bind_skill_adapter",
]


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
