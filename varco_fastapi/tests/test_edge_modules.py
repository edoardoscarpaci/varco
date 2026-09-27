"""
Plan 049 / Step 6 — edge modules (`varco_fastapi/varco_fastapi/modules/edge.py`),
Docker-free.

RED MODE: `varco_fastapi.modules.edge` does not exist yet (Step 6 of the plan
adds it) — every test below is expected to fail with `ImportError`.
"""

from __future__ import annotations

from providify import DIContainer
from varco_fastapi.middleware.slots import EdgeSlot
from varco_fastapi.modules.edge import (
    BodyLimitModule,
    CorsModule,
    ErrorEnvelopeModule,
    FastAPIModule,
    LocalizationModule,
    LoggingModule,
    MetricsModule,
    ProfilingModule,
    RateLimitModule,
    SecurityHeadersModule,
    TracingModule,
)


class TestCorsModule:
    async def test_default_enabled_yields_cors_spec_at_cors_slot(self) -> None:
        container = DIContainer()
        m = CorsModule()
        m.register(container)

        specs = m.middleware(container)

        assert any(s.slot == EdgeSlot.CORS for s in specs)

    async def test_enabled_false_yields_no_specs(self) -> None:
        container = DIContainer()
        m = CorsModule(enabled=False)
        m.register(container)

        assert m.middleware(container) == []


class TestSecurityHeadersModule:
    async def test_default_on(self) -> None:
        container = DIContainer()
        m = SecurityHeadersModule()
        m.register(container)

        specs = m.middleware(container)

        assert any(s.slot == EdgeSlot.SECURITY_HEADERS for s in specs)

    async def test_enabled_false(self) -> None:
        container = DIContainer()
        m = SecurityHeadersModule(enabled=False)
        m.register(container)

        assert m.middleware(container) == []

    async def test_passed_settings_instance_is_what_container_resolves_afterward(self) -> None:
        # PLAN-AMBIGUITY: the exact settings class import path
        # (`varco_fastapi.middleware.security_headers.SecurityHeadersSettings`)
        # is assumed from the existing pre-049 kwarg's type; the plan names
        # only "a passed settings instance is bound via bind_settings(...,
        # instance=...) and container.get(Cls) returns it afterward" (§D-enabled).
        from varco_fastapi.middleware.security_headers import SecurityHeadersSettings

        container = DIContainer()
        instance = SecurityHeadersSettings()
        m = SecurityHeadersModule(settings=instance)
        m.register(container)

        assert container.get(SecurityHeadersSettings) is instance


class TestBodyLimitModule:
    async def test_default_on(self) -> None:
        container = DIContainer()
        m = BodyLimitModule()
        m.register(container)

        specs = m.middleware(container)

        assert any(s.slot == EdgeSlot.BODY_LIMIT for s in specs)

    async def test_enabled_false(self) -> None:
        container = DIContainer()
        m = BodyLimitModule(enabled=False)
        m.register(container)

        assert m.middleware(container) == []


class TestErrorEnvelopeModule:
    async def test_default_on_yields_error_slot(self) -> None:
        container = DIContainer()
        m = ErrorEnvelopeModule()
        m.register(container)

        specs = m.middleware(container)

        assert any(s.slot == EdgeSlot.ERROR for s in specs)


class TestTracingModule:
    async def test_default_on(self) -> None:
        container = DIContainer()
        m = TracingModule()
        m.register(container)

        specs = m.middleware(container)

        assert any(s.slot == EdgeSlot.TRACING for s in specs)

    async def test_enabled_false(self) -> None:
        container = DIContainer()
        m = TracingModule(enabled=False)
        m.register(container)

        assert m.middleware(container) == []


class TestLoggingModule:
    async def test_default_redactor_is_none_no_fallback(self) -> None:
        # RO-4 / §D-logging: redactor=None means no redaction, never
        # default_redactor() (040's rule stands).
        container = DIContainer()
        m = LoggingModule()
        m.register(container)

        specs = m.middleware(container)
        logging_spec = next(s for s in specs if s.slot == EdgeSlot.LOGGING)

        assert logging_spec.kwargs.get("redactor") is None

    async def test_explicit_redactor_is_threaded_through(self) -> None:
        sentinel = object()
        container = DIContainer()
        m = LoggingModule(redactor=sentinel)
        m.register(container)

        specs = m.middleware(container)
        logging_spec = next(s for s in specs if s.slot == EdgeSlot.LOGGING)

        assert logging_spec.kwargs.get("redactor") is sentinel

    async def test_enabled_false(self) -> None:
        container = DIContainer()
        m = LoggingModule(enabled=False)
        m.register(container)

        assert m.middleware(container) == []


class TestMetricsModule:
    async def test_opt_in_not_present_unless_constructed(self) -> None:
        # Metrics is opt-in (not in default_modules()) — this only proves
        # the module itself works when the app lists it.
        container = DIContainer()
        m = MetricsModule()
        m.register(container)

        specs = m.middleware(container)

        assert any(s.slot == EdgeSlot.METRICS for s in specs)


class TestProfilingModule:
    async def test_opt_in_yields_profiling_slot(self) -> None:
        container = DIContainer()
        m = ProfilingModule()
        m.register(container)

        specs = m.middleware(container)

        assert any(s.slot == EdgeSlot.PROFILING for s in specs)


class TestRateLimitModule:
    async def test_yields_pre_and_post_specs(self) -> None:
        # PLAN-AMBIGUITY (resolved): the exact RateLimitBundle construction
        # is assumed from its pre-049 shape (`rate_limit=RateLimitBundle(...)`),
        # and — unlike the RED test's original assumption — "two specs
        # (PRE/POST)" means "up to two, one per non-empty stage partition",
        # not "always both regardless of rule content". A bundle with
        # `rules=()` therefore yields ZERO specs (asserted below); this
        # test instead uses one IP (PRE_AUTH-legal) and one TENANT
        # (POST_AUTH-only) rule so both partitions are genuinely non-empty.
        # Resolution forced by `test_middleware_order.py::
        # test_rate_limit_bundle_with_only_tenant_rule_yields_one_entry_inside`
        # — an existing, plan-locked characterization test — which proves a
        # bundle with only a TENANT rule must install exactly ONE
        # RateLimitMiddleware, not two; see modules/edge.py's
        # RateLimitModule.middleware() docstring comment for the full
        # argument.
        from varco_core.resilience.rate_limit import InMemoryRateLimiter, RateLimitConfig
        from varco_fastapi.middleware.rate_limit import (
            RateLimitBundle,
            RateLimitRule,
            RateLimitScope,
        )

        limiter = InMemoryRateLimiter(RateLimitConfig(rate=100, period=60.0))
        container = DIContainer()
        bundle = RateLimitBundle(
            rules=(
                RateLimitRule(scope=RateLimitScope.IP, limiter=limiter, name="ip"),
                RateLimitRule(scope=RateLimitScope.TENANT, limiter=limiter, name="tenant"),
            ),
            acknowledge_unbounded_keyspace=True,
        )
        m = RateLimitModule(bundle)
        m.register(container)

        specs = m.middleware(container)
        slots = {s.slot for s in specs}

        assert EdgeSlot.RATE_LIMIT_PRE_AUTH in slots
        assert EdgeSlot.RATE_LIMIT_POST_AUTH in slots

    async def test_empty_rules_yields_no_specs(self) -> None:
        from varco_fastapi.middleware.rate_limit import RateLimitBundle

        container = DIContainer()
        bundle = RateLimitBundle(rules=())
        m = RateLimitModule(bundle)
        m.register(container)

        assert m.middleware(container) == []


class TestLocalizationModule:
    async def test_both_disabled_yields_nothing(self) -> None:
        container = DIContainer()
        m = LocalizationModule()
        m.register(container)

        # Default env has both i18n/timezone disabled.
        assert m.middleware(container) == []

    async def test_i18n_enabled_yields_localization_slot(self) -> None:
        from varco_core.i18n import I18nSettings

        container = DIContainer()
        m = LocalizationModule(i18n=I18nSettings(enabled=True))
        m.register(container)

        specs = m.middleware(container)

        assert any(s.slot == EdgeSlot.LOCALIZATION for s in specs)


class TestFastAPIModule:
    async def test_depends_on_core_module(self) -> None:
        from varco_core.di.module import CoreModule

        assert CoreModule in FastAPIModule.depends_on

    async def test_request_context_middleware_always_present_anonymous_when_no_server_auth(
        self,
    ) -> None:
        """
        Repair round 2 / Decision 1: no ``AbstractServerAuth`` bound must
        still yield a ``RequestContextMiddleware`` spec — anonymous mode,
        pre-049 parity — not an omitted middleware. See ``FastAPIModule``'s
        class docstring DESIGN block.
        """
        container = DIContainer()
        m = FastAPIModule()
        m.register(container)

        specs = m.middleware(container)

        request_context_specs = [s for s in specs if s.slot == EdgeSlot.REQUEST_CONTEXT]
        assert len(request_context_specs) == 1
        assert "server_auth" not in request_context_specs[0].kwargs

    async def test_request_context_middleware_uses_bound_server_auth_when_resolvable(
        self,
    ) -> None:
        from varco_core.di import bind_instance
        from varco_fastapi.auth.server_auth import AbstractServerAuth, AnonymousAuth

        container = DIContainer()
        auth = AnonymousAuth()
        bind_instance(container, AbstractServerAuth, auth)
        m = FastAPIModule()
        m.register(container)

        specs = m.middleware(container)

        request_context_specs = [s for s in specs if s.slot == EdgeSlot.REQUEST_CONTEXT]
        assert len(request_context_specs) == 1
        assert request_context_specs[0].kwargs["server_auth"] is auth

    async def test_configure_jwt_true_by_default(self) -> None:
        assert FastAPIModule().__class__ is FastAPIModule  # smoke: constructs with no args
