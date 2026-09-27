"""
varco_fastapi.di
==================
Central DI module for varco_fastapi.

``VarcoFastAPIConfiguration`` is a ``@Configuration`` class that registers the default
varco_fastapi providers in a ``DIContainer``.  Install it alongside backend
modules (SA, Redis, etc.) at application bootstrap::

    container = DIContainer()
    container.scan("varco_kafka", recursive=True)   # discovers the Kafka bus
    container.install(SAModule)
    container.install(VarcoFastAPIConfiguration)
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
``VarcoFastAPIConfiguration.task_serializer`` (a ``@Fallback @Provider``, the same
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

from providify import Configuration, DIContainer, Fallback, Provider

# ── Plan 011 — i18n / timezone default bindings ─────────────────────────
from varco_core.context.defaults import NullTenantDefaults, TenantDefaultsProvider
from varco_core.i18n.catalog import MessageCatalog, NullMessageCatalog
from varco_core.job.serializer import DefaultTaskSerializer, TaskSerializer
from varco_core.job.task import TaskRegistry

# JWT claim-transform config types (Plan 002 Phase 2 step 20) — module-level
# import for the same get_type_hints() reason documented above.
from varco_core.jwt.transform.config import JwtTransformConfig
from varco_core.jwt.transform.registry import ClaimTransformerRegistry

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
from varco_fastapi.client.settings import ClientProfileSettings
from varco_fastapi.middleware.cors import CORSConfig
from varco_fastapi.middleware.profiling import ProfilingSettings


def bind_clients(container: DIContainer, *client_classes: type) -> DIContainer:
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
        container.install(VarcoFastAPIConfiguration)
        bind_clients(container, OrderClient, UserClient)

        # Now injectable:
        class ShippingService:
            def __init__(self, orders: Inject[VarcoClient[OrderRouter]]) -> None:
                self._orders = orders

    Returns:
        The same ``container`` passed in.

    Raises:
        TypeError: A client has no ``_router_class``.
        AlreadyBoundError: A non-fallback ``VarcoClient[RouterType]`` binding
            already exists for one of the given clients (§D-bind).

    Edge cases:
        - No configurator → client will need an explicit ``base_url`` at construction.

    Thread safety:  ✅ Registration happens at bootstrap; no concurrent access.
    Async safety:   ✅ No I/O during registration.
    """
    from varco_core.di import AlreadyBoundError, is_user_bound

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

        if is_user_bound(container, client_alias):
            raise AlreadyBoundError(
                f"A non-fallback VarcoClient[{router_cls.__name__}] binding already "
                "exists — use providify's di_overrides/ContainerOverrides in a test "
                "to replace it."
            )

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

    return container


def bind_clients_from(container: DIContainer, *router_classes: type) -> DIContainer:
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

    Returns:
        The same ``container`` passed in (delegates to :func:`bind_clients`).
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
    return bind_clients(container, *client_classes)


# ── VarcoFastAPIConfiguration ────────────────────────────────────────────────────────


@Configuration
class VarcoFastAPIConfiguration:
    """
    ``@Configuration`` module for varco_fastapi defaults.

    Discovered and auto-installed by ``container.scan("varco_fastapi", recursive=True)``.
    No explicit ``container.install(VarcoFastAPIConfiguration)`` call is required.

    Registers (all overrideable via ``container.bind()`` before scanning):
        - ``TrustStore``               — from ``VARCO_TLS_TRUST_STORE_DIR`` env vars
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
        TLS trust store loaded from env vars (``VARCO_TLS_TRUST_STORE_DIR``, etc.).

        Returns:
            A ``TrustStore`` configured from the environment.
        """
        return TrustStore.from_env()

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
        Diagnostic profiler settings loaded from ``VARCO_PROFILING_*`` env vars.

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

    # Plan 048 / DI-6, Step 13 — `jwt_transform_settings` used to live here as
    # a `@Fallback @Provider`; `JwtTransformSettings` is now registered by
    # `bind_core_settings()` (§D-core-list), called from `bootstrap()` below.

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

    # Plan 048 / DI-6, Step 13 — `i18n_settings`/`timezone_settings` used to
    # live here too; `I18nSettings`/`TimezoneSettings` are now registered by
    # `bind_core_settings()` as well (§D-core-list).

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


__all__ = [
    "VarcoFastAPIConfiguration",
    "bind_clients",
    "bind_clients_from",
    "bootstrap",
    # MCP / Skill adapter DI helpers — true re-exports from their modules for
    # one-stop import: from varco_fastapi.di import bind_mcp_adapter
    "bind_mcp_adapter",
    "bind_skill_adapter",
]

# ── Re-export adapter DI helpers for one-stop import convenience ──────────────
# True re-exports (not wrapping *args/**kwargs functions, Plan 046 §D-removals)
# so `inspect.signature(varco_fastapi.di.bind_mcp_adapter)` shows the real
# `qualifier=` parameter instead of an opaque `(*args, **kwargs)`.
from varco_fastapi.router.mcp import bind_mcp_adapter  # noqa: E402
from varco_fastapi.router.skill import bind_skill_adapter  # noqa: E402

# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(container: DIContainer) -> DIContainer:
    """
    Bootstrap ``varco_fastapi`` into a ``DIContainer``.

    Installs :data:`VarcoFastAPIConfiguration` and calls
    ``container.scan("varco_fastapi", recursive=True)``. Every framework
    default — including ``TaskSerializer → DefaultTaskSerializer`` — is
    registered by the scan itself (each is a ``@Fallback``-marked binding on
    ``VarcoFastAPIConfiguration`` or a scanned ``@Singleton`` class); there is no
    separate "setup defaults" step to opt into or skip.

    Plan 046 §D-removals deletes ``*packages``/``setup_producer=`` and the
    ``create_varco_container``/``setup_event_producer`` helpers they used to
    delegate to — scan your own application package separately, and call
    ``varco_core.di.bootstrap(container)`` for ``AbstractEventProducer →
    BusEventProducer`` (it is a ``varco_core`` Class-A ``@Fallback`` default,
    045's A10)::

        from providify import DIContainer
        from varco_core.di import bootstrap as core_bootstrap
        from varco_redis.di import bootstrap as redis_bootstrap
        from varco_fastapi.di import bootstrap as fastapi_bootstrap

        container = DIContainer()
        core_bootstrap(container)      # AbstractEventProducer → BusEventProducer, etc.
        redis_bootstrap(container)     # AbstractEventBus → RedisEventBus
        fastapi_bootstrap(container)   # VarcoFastAPIConfiguration defaults
        container.scan("myapp", recursive=True)

    Args:
        container: The ``DIContainer`` to install into. Required — this
            function never creates one and never reads
            the process-global container (Plan 046 §D-locator).

    Returns:
        The same ``container`` passed in, after installation and scanning.

    Edge cases:
        - Calling twice is safe — scanning is idempotent.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ No I/O during installation; providers are called lazily.
    """
    # Plan 048 / DI-6, Step 13 — every settings class now goes through
    # bind_settings()/bind_core_settings(), never a scanned @Provider (the
    # `cors_config` etc. methods this module used to carry are gone). Call
    # this BEFORE scan() so a settings class is already resolvable to any
    # @Singleton the scan discovers that injects one.
    from varco_core.settings import bind_settings
    from varco_core.settings.core import bind_core_settings

    from varco_fastapi.middleware.body_limit import BodyLimitSettings
    from varco_fastapi.middleware.rate_limit import RateLimitSettings
    from varco_fastapi.middleware.security_headers import SecurityHeadersSettings
    from varco_fastapi.posture import SecurityPostureSettings

    bind_core_settings(container)
    for settings_cls in (
        CORSConfig,
        SecurityPostureSettings,
        SecurityHeadersSettings,
        BodyLimitSettings,
        RateLimitSettings,
        ClientProfileSettings,
    ):
        bind_settings(container, settings_cls)

    # The scanner discovers VarcoFastAPIConfiguration (@Configuration) and calls
    # container.install(VarcoFastAPIConfiguration) automatically — no explicit
    # install() call needed.
    container.scan("varco_fastapi", recursive=True)

    return container
