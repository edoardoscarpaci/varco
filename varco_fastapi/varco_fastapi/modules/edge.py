"""
varco_fastapi.modules.edge
=============================
The eleven HTTP-edge ``VarcoModule``s (Plan 049 / DI-1, §D-http /
§D-signature): every ``create_varco_app`` keyword that used to install one
middleware (or one middleware pair) is now one module, naming an
``EdgeSlot`` rather than an implicit position.

Every module here implements ``HttpContributor`` (``middleware()`` and,
where relevant, ``configure_app()``) — see ``varco_fastapi.modules.base``.

Thread safety:  ✅ Each module holds only its constructor arguments.
Async safety:   ✅ ``middleware()``/``configure_app()`` are synchronous —
                    building the spec list does no I/O; the middleware
                    classes themselves do their own I/O per request.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from varco_core.di.lifecycle import Phase
from varco_core.di.module import CoreModule, VarcoModule

from varco_fastapi.middleware.slots import EdgeSlot, MiddlewareSpec

if TYPE_CHECKING:
    from providify import DIContainer
    from varco_core.redaction import Redactor

    from varco_fastapi.middleware.body_limit import BodyLimitSettings
    from varco_fastapi.middleware.rate_limit import RateLimitBundle
    from varco_fastapi.middleware.security_headers import SecurityHeadersSettings


def _has_error_middleware(container: DIContainer) -> bool:
    """
    Whether an ``ErrorEnvelopeModule`` is registered and enabled on
    *container* — read from the ``ModuleRegistry`` (§D-http:
    "``has_error_middleware=`` is read from the registry: ``ErrorEnvelopeModule``
    present and enabled"), so ``BodyLimitModule``/``RateLimitModule`` render
    their own over-limit responses correctly whether or not the app also
    installed ``ErrorEnvelopeModule``.

    Args:
        container: The container modules were registered on.

    Returns:
        ``True`` iff an ``ErrorEnvelopeModule`` instance is recorded in the
        ``ModuleRegistry`` and was enabled at registration time. ``False``
        (never raises) when no ``ModuleRegistry`` exists yet (this module
        registered before any other, or is the only module in the list).
    """
    from varco_core.di.module import ModuleRegistry  # noqa: PLC0415

    if not container.is_resolvable(ModuleRegistry):
        return False
    registry: ModuleRegistry = container.get(ModuleRegistry)
    return any(isinstance(m, ErrorEnvelopeModule) and registry.enabled(m) for m in registry.modules)


class CorsModule(VarcoModule):
    """
    Installs Starlette's ``CORSMiddleware`` via ``install_cors`` (the
    callable-installer ``MiddlewareEntry`` shape) — absorbs the pre-049
    ``cors=`` keyword and ``_resolve_cors()``.

    Args:
        config: ``None`` (default) resolves ``CORSConfig`` from the
            container if bound, else ``CORSConfig.from_env()`` — the exact
            pre-049 ``_resolve_cors()`` fallback chain.
    """

    name = "cors"
    phase = Phase.INFRA
    # no lifecycle; declared so the suite's phase test is uniform

    def __init__(self, config: Any | None = None, *, enabled: bool | None = None) -> None:
        super().__init__(enabled=enabled)
        self._config = config

    def _register(self, container: DIContainer) -> None:
        pass

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        if not self.is_enabled(container):
            return []
        from varco_fastapi.middleware.cors import CORSConfig, install_cors  # noqa: PLC0415

        config = self._config
        if config is None:
            try:
                config = container.get(CORSConfig)
            except Exception:  # noqa: BLE001 — no CORSConfig bound; env fallback
                config = CORSConfig.from_env()
        return [MiddlewareSpec(slot=EdgeSlot.CORS, target=install_cors, kwargs={"config": config})]

    def configure_app(self, app: Any, container: DIContainer) -> None:
        pass


class SecurityHeadersModule(VarcoModule):
    """
    Installs ``SecurityHeadersMiddleware`` (Plan 035 / §D-S7-default) —
    default-on, ``BALANCED`` preset.

    Args:
        settings: A ``SecurityHeadersSettings`` instance, bound
            (non-fallback) via ``bind_settings(..., instance=settings)``
            so ``container.get(SecurityHeadersSettings)`` returns it
            afterward. ``None`` (default) leaves the module's own
            ``@Fallback`` default (bound by ``FastAPIModule``/``bootstrap``)
            in place.
    """

    name = "security_headers"
    phase = Phase.INFRA
    # no lifecycle; declared so the suite's phase test is uniform

    def __init__(
        self,
        *,
        settings: SecurityHeadersSettings | None = None,
        enabled: bool | None = None,
    ) -> None:
        super().__init__(enabled=enabled)
        self._settings = settings

    def _register(self, container: DIContainer) -> None:
        if self._settings is not None:
            # §D-enabled says "bind_settings(..., instance=...)" — proven
            # unsafe in practice: FastAPIModule's own bootstrap() already
            # calls bind_settings(container, SecurityHeadersSettings) (a
            # plain @Fallback) as part of the standard default set, and
            # bind_settings(..., instance=...) raises
            # SettingsAlreadyBoundError the moment ANY prior bind_settings
            # call (fallback or not) touched this (cls, qualifier) — which
            # is always true once FastAPIModule has registered. bind_instance
            # (Plan 046's "already-constructed object" verb) uses a
            # different bookkeeping registry and only raises if a
            # NON-fallback binding already exists — exactly what "a code
            # decision beats a default" requires here.
            from varco_core.di import bind_instance  # noqa: PLC0415

            from varco_fastapi.middleware.security_headers import (  # noqa: PLC0415
                SecurityHeadersSettings,
            )

            bind_instance(container, SecurityHeadersSettings, self._settings)

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        if not self.is_enabled(container):
            return []
        from varco_fastapi.middleware.security_headers import (  # noqa: PLC0415
            SecurityHeadersMiddleware,
            SecurityHeadersSettings,
        )

        settings = self._settings
        if settings is None:
            try:
                settings = container.get(SecurityHeadersSettings)
            except Exception:  # noqa: BLE001
                settings = SecurityHeadersSettings()
        return [
            MiddlewareSpec(
                slot=EdgeSlot.SECURITY_HEADERS,
                target=SecurityHeadersMiddleware,
                kwargs={"settings": settings},
            )
        ]

    def configure_app(self, app: Any, container: DIContainer) -> None:
        pass


class BodyLimitModule(VarcoModule):
    """Installs ``BodyLimitMiddleware`` (Plan 035 / §D-S8-default) — default-on, 10 MiB."""

    name = "body_limit"
    phase = Phase.INFRA
    # no lifecycle; declared so the suite's phase test is uniform

    def __init__(
        self, *, settings: BodyLimitSettings | None = None, enabled: bool | None = None
    ) -> None:
        super().__init__(enabled=enabled)
        self._settings = settings

    def _register(self, container: DIContainer) -> None:
        if self._settings is not None:
            # See SecurityHeadersModule._register()'s comment — bind_instance,
            # not bind_settings(..., instance=...), for the same reason.
            from varco_core.di import bind_instance  # noqa: PLC0415

            from varco_fastapi.middleware.body_limit import BodyLimitSettings  # noqa: PLC0415

            bind_instance(container, BodyLimitSettings, self._settings)

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        if not self.is_enabled(container):
            return []
        from varco_fastapi.middleware.body_limit import (  # noqa: PLC0415
            BodyLimitMiddleware,
            BodyLimitSettings,
        )

        settings = self._settings
        if settings is None:
            try:
                settings = container.get(BodyLimitSettings)
            except Exception:  # noqa: BLE001
                settings = BodyLimitSettings()
        return [
            MiddlewareSpec(
                slot=EdgeSlot.BODY_LIMIT,
                target=BodyLimitMiddleware,
                kwargs={
                    "settings": settings,
                    "has_error_middleware": _has_error_middleware(container),
                },
            )
        ]

    def configure_app(self, app: Any, container: DIContainer) -> None:
        pass


class ErrorEnvelopeModule(VarcoModule):
    """
    Installs ``ErrorMiddleware`` and registers the exception handlers
    (``add_exception_handlers``) — absorbs ``enable_error_middleware=``.

    ``configure_app()`` calls ``add_exception_handlers`` — it needs the raw
    ``FastAPI`` app (not a middleware spec), exactly like the pre-049
    ``create_varco_app`` Step 6.
    """

    name = "error_envelope"
    phase = Phase.INFRA
    # no lifecycle; declared so the suite's phase test is uniform

    def _register(self, container: DIContainer) -> None:
        pass

    def _resolve_catalog(self, container: DIContainer) -> Any | None:
        from varco_core.i18n import I18nSettings  # noqa: PLC0415
        from varco_core.i18n.catalog import MessageCatalog  # noqa: PLC0415

        try:
            i18n_settings: I18nSettings = container.get(I18nSettings)
        except Exception:  # noqa: BLE001
            i18n_settings = I18nSettings()
        if not i18n_settings.enabled:
            return None
        try:
            return container.get(MessageCatalog)
        except Exception:  # noqa: BLE001
            return None

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        if not self.is_enabled(container):
            return []
        from varco_core.i18n import I18nSettings  # noqa: PLC0415

        from varco_fastapi.middleware.error import ErrorMiddleware  # noqa: PLC0415

        try:
            i18n_settings: I18nSettings = container.get(I18nSettings)
        except Exception:  # noqa: BLE001
            i18n_settings = I18nSettings()
        return [
            MiddlewareSpec(
                slot=EdgeSlot.ERROR,
                target=ErrorMiddleware,
                kwargs={
                    "message_catalog": self._resolve_catalog(container),
                    "set_content_language": i18n_settings.set_content_language,
                },
            )
        ]

    def configure_app(self, app: Any, container: DIContainer) -> None:
        if not self.is_enabled(container):
            return
        from varco_core.i18n import I18nSettings  # noqa: PLC0415

        from varco_fastapi.exceptions import add_exception_handlers  # noqa: PLC0415

        try:
            i18n_settings: I18nSettings = container.get(I18nSettings)
        except Exception:  # noqa: BLE001
            i18n_settings = I18nSettings()
        add_exception_handlers(
            app,
            message_catalog=self._resolve_catalog(container),
            set_content_language=i18n_settings.set_content_language,
        )


class TracingModule(VarcoModule):
    """Installs ``TracingMiddleware`` — absorbs ``enable_tracing=``."""

    name = "tracing"
    phase = Phase.INFRA
    # no lifecycle; declared so the suite's phase test is uniform

    def _register(self, container: DIContainer) -> None:
        pass

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        if not self.is_enabled(container):
            return []
        from varco_fastapi.middleware.tracing import TracingMiddleware  # noqa: PLC0415

        return [MiddlewareSpec(slot=EdgeSlot.TRACING, target=TracingMiddleware, kwargs={})]

    def configure_app(self, app: Any, container: DIContainer) -> None:
        pass


class LoggingModule(VarcoModule):
    """
    Installs ``RequestLoggingMiddleware`` — absorbs ``enable_logging=`` and
    lands **RO-4** (``BACKLOG.md:112``): the redactor keyword 040 refused to
    add to ``app.py`` directly.

    Args:
        redactor: ``None`` (default) means **no redaction** — 040's rule
            stands: this never falls back to ``default_redactor()``.
    """

    name = "logging"
    phase = Phase.INFRA
    # no lifecycle; declared so the suite's phase test is uniform

    def __init__(self, *, redactor: Redactor | None = None, enabled: bool | None = None) -> None:
        super().__init__(enabled=enabled)
        self._redactor = redactor

    def _register(self, container: DIContainer) -> None:
        pass

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        if not self.is_enabled(container):
            return []
        from varco_fastapi.middleware.logging import RequestLoggingMiddleware  # noqa: PLC0415

        return [
            MiddlewareSpec(
                slot=EdgeSlot.LOGGING,
                target=RequestLoggingMiddleware,
                kwargs={"redactor": self._redactor},
            )
        ]

    def configure_app(self, app: Any, container: DIContainer) -> None:
        pass


class MetricsModule(VarcoModule):
    """Installs ``MetricsMiddleware`` and mounts ``MetricsRouter`` — opt-in."""

    name = "metrics"
    phase = Phase.INFRA
    # no lifecycle; declared so the suite's phase test is uniform

    def _register(self, container: DIContainer) -> None:
        pass

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        if not self.is_enabled(container):
            return []
        from varco_fastapi.middleware.metrics import MetricsMiddleware  # noqa: PLC0415

        return [MiddlewareSpec(slot=EdgeSlot.METRICS, target=MetricsMiddleware, kwargs={})]

    def configure_app(self, app: Any, container: DIContainer) -> None:
        if not self.is_enabled(container):
            return
        try:
            from varco_fastapi.router.metrics import MetricsRouter  # noqa: PLC0415

            app.include_router(MetricsRouter().build_router())
        except ImportError:
            pass


class ProfilingModule(VarcoModule):
    """Installs ``ProfilingMiddleware`` — opt-in, innermost (``EdgeSlot.PROFILING``)."""

    name = "profiling"
    phase = Phase.INFRA
    # no lifecycle; declared so the suite's phase test is uniform

    def _register(self, container: DIContainer) -> None:
        pass

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        if not self.is_enabled(container):
            return []
        from varco_fastapi.middleware.profiling import (  # noqa: PLC0415
            ProfilingMiddleware,
            ProfilingSettings,
        )

        return [
            MiddlewareSpec(
                slot=EdgeSlot.PROFILING,
                target=ProfilingMiddleware,
                kwargs={"settings": ProfilingSettings()},
            )
        ]

    def configure_app(self, app: Any, container: DIContainer) -> None:
        pass


class RateLimitModule(VarcoModule):
    """
    Installs up to two ``RateLimitMiddleware`` instances from one
    ``RateLimitBundle`` — the "one middleware, two positions" design (035)
    survives unchanged: ``IP``/``GLOBAL`` rules at ``RATE_LIMIT_PRE_AUTH``,
    ``SUBJECT``/``TENANT`` rules at ``RATE_LIMIT_POST_AUTH``.

    Args:
        bundle: The ``RateLimitBundle`` to partition and install.
    """

    name = "rate_limit"
    phase = Phase.INFRA
    # no lifecycle; declared so the suite's phase test is uniform

    def __init__(self, bundle: RateLimitBundle, *, enabled: bool | None = None) -> None:
        super().__init__(enabled=enabled)
        self._bundle = bundle

    def _register(self, container: DIContainer) -> None:
        pass

    def _partition(self, *, post_auth: bool) -> tuple[Any, ...]:
        from varco_fastapi.middleware.rate_limit import RateLimitScope  # noqa: PLC0415

        post_auth_scopes = {RateLimitScope.SUBJECT, RateLimitScope.TENANT}
        return tuple(
            rule for rule in self._bundle.rules if (rule.scope in post_auth_scopes) == post_auth
        )

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        if not self.is_enabled(container):
            return []
        from varco_fastapi.middleware.rate_limit import (  # noqa: PLC0415
            RateLimitMiddleware,
            RateLimitStage,
        )

        # §D-http: "RateLimitModule yields two specs (PRE/POST)" — one per
        # stage that has at least one matching rule. §PLAN-AMBIGUITY
        # (resolved): a literal, unconditional two-specs-always reading
        # would install a `RateLimitMiddleware(rules=())` pass-through into
        # `app.user_middleware` for a stage with zero rules — which
        # `test_middleware_order.py::
        # test_rate_limit_bundle_with_only_tenant_rule_yields_one_entry_inside`
        # (an existing, plan-locked byte-identical-order characterization
        # test) proves is NOT the pre-049 behaviour: a bundle with only a
        # TENANT rule must install exactly ONE RateLimitMiddleware, not two.
        # Conditional-per-stage is what keeps that test green, so it is the
        # resolution — "two specs" means "up to two", one per non-empty
        # partition.
        specs: list[MiddlewareSpec] = []
        pre_auth_rules = self._partition(post_auth=False)
        if pre_auth_rules:
            specs.append(
                MiddlewareSpec(
                    slot=EdgeSlot.RATE_LIMIT_PRE_AUTH,
                    target=RateLimitMiddleware,
                    kwargs={
                        "rules": pre_auth_rules,
                        "stage": RateLimitStage.PRE_AUTH,
                        "settings": self._bundle.settings,
                        "acknowledge_unbounded_keyspace": (
                            self._bundle.acknowledge_unbounded_keyspace
                        ),
                        "has_error_middleware": _has_error_middleware(container),
                    },
                )
            )
        post_auth_rules = self._partition(post_auth=True)
        if post_auth_rules:
            specs.append(
                MiddlewareSpec(
                    slot=EdgeSlot.RATE_LIMIT_POST_AUTH,
                    target=RateLimitMiddleware,
                    kwargs={
                        "rules": post_auth_rules,
                        "stage": RateLimitStage.POST_AUTH,
                        "settings": self._bundle.settings,
                        "acknowledge_unbounded_keyspace": (
                            self._bundle.acknowledge_unbounded_keyspace
                        ),
                        "has_error_middleware": _has_error_middleware(container),
                    },
                )
            )
        return specs

    def configure_app(self, app: Any, container: DIContainer) -> None:
        pass


class LocalizationModule(VarcoModule):
    """
    Installs one ``LocalizationMiddleware`` when either i18n or timezone
    resolution is enabled (RD-3), and starts an ``I18nLifecycle`` for the
    resolved ``MessageCatalog`` when i18n is enabled.

    Args:
        i18n: ``None`` (default) → ``I18nSettings()`` (env-driven; disabled
            unless ``VARCO_I18N_ENABLED=true``).
        timezone: ``None`` (default) → ``TimezoneSettings()`` (env-driven).
    """

    name = "localization"
    phase = Phase.INFRA

    def __init__(
        self,
        *,
        i18n: Any | None = None,
        timezone: Any | None = None,
        enabled: bool | None = None,
    ) -> None:
        super().__init__(enabled=enabled)
        self._i18n = i18n
        self._timezone = timezone
        self._catalog: Any | None = None

    def _register(self, container: DIContainer) -> None:
        # Bind whichever settings instance was passed in — non-fallback,
        # via bind_instance (never bind_settings(..., instance=...); see
        # SecurityHeadersModule._register()'s comment for why) — so
        # ErrorEnvelopeModule reads the SAME resolved I18nSettings this
        # module's own middleware() uses (both read `.enabled`/
        # `.set_content_language` — pre-049's app.py shared one
        # `_resolved_i18n_settings` local variable between the two; this
        # is how two independent modules stay in sync on the same fact).
        from varco_core.di import bind_instance  # noqa: PLC0415

        if self._i18n is not None:
            from varco_core.i18n import I18nSettings  # noqa: PLC0415

            bind_instance(container, I18nSettings, self._i18n)
        if self._timezone is not None:
            from varco_core.tz.settings import TimezoneSettings  # noqa: PLC0415

            bind_instance(container, TimezoneSettings, self._timezone)

    def _resolved_settings(self) -> tuple[Any, Any]:
        from varco_core.i18n import I18nSettings  # noqa: PLC0415
        from varco_core.tz.settings import TimezoneSettings  # noqa: PLC0415

        i18n = self._i18n if self._i18n is not None else I18nSettings()
        timezone = self._timezone if self._timezone is not None else TimezoneSettings()
        return i18n, timezone

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        if not self.is_enabled(container):
            return []
        i18n, timezone = self._resolved_settings()
        if not (i18n.enabled or timezone.enabled):
            return []
        from varco_fastapi.middleware.localization import LocalizationMiddleware  # noqa: PLC0415

        return [
            MiddlewareSpec(
                slot=EdgeSlot.LOCALIZATION,
                target=LocalizationMiddleware,
                kwargs={"i18n_settings": i18n, "timezone_settings": timezone},
            )
        ]

    async def start(self, container: DIContainer) -> None:
        i18n, _timezone = self._resolved_settings()
        if not i18n.enabled:
            return
        from varco_core.i18n.catalog import MessageCatalog  # noqa: PLC0415

        from varco_fastapi.i18n import I18nLifecycle  # noqa: PLC0415

        try:
            catalog: MessageCatalog = container.get(MessageCatalog)
        except Exception:  # noqa: BLE001 — no catalog bound
            return
        self._catalog = catalog
        await I18nLifecycle(catalog).start()

    async def stop(self, container: DIContainer) -> None:
        if self._catalog is None:
            return
        from varco_fastapi.i18n import I18nLifecycle  # noqa: PLC0415

        await I18nLifecycle(self._catalog).stop()
        self._catalog = None

    def configure_app(self, app: Any, container: DIContainer) -> None:
        pass


class FastAPIModule(VarcoModule):
    """
    ``varco_fastapi``'s own bootstrap + JWT process-global configuration +
    ``RequestContextMiddleware`` (populates ``AuthContext``/
    ``current_tenant()``).

    ``RequestContextMiddleware`` is **unconditionally** contributed —
    with ``AbstractServerAuth`` bound, it runs the app's real auth strategy;
    with nothing bound, it still runs, in *anonymous mode*
    (``RequestContextMiddleware.__init__``'s own ``server_auth or
    AnonymousAuth()`` default, ``middleware/request_context.py:129-134``).
    This restores pre-Plan-049 behaviour — the deleted
    ``_try_add_request_context_middleware`` (``git show
    HEAD~N:varco_fastapi/varco_fastapi/app.py`` around line 1133, pre-049)
    had an ``except`` branch that added the middleware with no
    ``server_auth`` at all, so the middleware — and therefore a populated
    ``AuthContext``/``correlation_id``/``current_tenant()`` — was *always*
    present, auth configured or not.

    DESIGN: unconditional contribution vs. "no ``AbstractServerAuth`` bound
    ⇒ no middleware at all" (049's original, since-reverted gating):
    ✅ Byte-identical to pre-049 — a bare ``create_varco_app(DIContainer())``
       (no auth wired anywhere) still gets ``AuthContext``/``correlation_id``
       populated for every request, which downstream code (audit, tracing
       attributes, ``current_tenant()`` callers) already assumes is true.
    ✅ No new silent behaviour: the container-resolvable check
       (``container.is_resolvable(AbstractServerAuth)``) still decides
       *which* auth strategy runs — this only changes what happens when the
       answer is "none": anonymous mode, not "nothing at all".
    ❌ Operator-facing risk: an app that forgot to bind
       ``AbstractServerAuth`` gets a *working*, unauthenticated app instead
       of a loud failure — every request is treated as anonymous rather
       than rejected. ⚠️ Neither ``inspect_http_edge()`` nor the security
       posture preflight (``varco_fastapi.posture``) currently emits a
       finding for "no ``AbstractServerAuth`` bound" — verified absent from
       both modules' `check=` ids as of this writing. This module does not
       add one; a collector for this specific gap is a candidate for a
       future plan, not invented here.

    Args:
        configure_jwt: When ``True`` (default), calls
            ``configure_jwt_from_env()`` at registration time.
    """

    name = "fastapi"
    phase = Phase.INFRA
    depends_on = (CoreModule,)

    def __init__(self, *, configure_jwt: bool = True, enabled: bool | None = None) -> None:
        super().__init__(enabled=enabled)
        self._configure_jwt = configure_jwt

    def _register(self, container: DIContainer) -> None:
        from varco_fastapi.di import bootstrap  # noqa: PLC0415

        bootstrap(container)
        if self._configure_jwt:
            from varco_core.jwt.transform.runtime import (  # noqa: PLC0415
                configure_jwt_from_env,
            )

            configure_jwt_from_env()

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        """
        Always contribute ``RequestContextMiddleware`` at
        ``EdgeSlot.REQUEST_CONTEXT`` — with the container's bound
        ``AbstractServerAuth`` when one is resolvable, or with no
        ``server_auth`` kwarg at all otherwise (anonymous mode — see the
        class docstring's DESIGN block). Never returns ``[]`` on the
        "unresolvable" branch; that branch is what pre-049 parity requires.

        Args:
            container: The container modules were registered on.

        Returns:
            ``[]`` only when this module itself is disabled
            (``enabled=False``); otherwise exactly one
            ``MiddlewareSpec`` for ``RequestContextMiddleware``.
        """
        if not self.is_enabled(container):
            return []
        from varco_fastapi.auth.server_auth import AbstractServerAuth  # noqa: PLC0415
        from varco_fastapi.middleware.request_context import (  # noqa: PLC0415
            RequestContextMiddleware,
        )

        kwargs: dict[str, Any] = {}
        if container.is_resolvable(AbstractServerAuth):
            kwargs["server_auth"] = container.get(AbstractServerAuth)
        return [
            MiddlewareSpec(
                slot=EdgeSlot.REQUEST_CONTEXT,
                target=RequestContextMiddleware,
                kwargs=kwargs,
            )
        ]

    def configure_app(self, app: Any, container: DIContainer) -> None:
        pass


__all__ = [
    "BodyLimitModule",
    "CorsModule",
    "ErrorEnvelopeModule",
    "FastAPIModule",
    "LocalizationModule",
    "LoggingModule",
    "MetricsModule",
    "ProfilingModule",
    "RateLimitModule",
    "SecurityHeadersModule",
    "TracingModule",
]
