"""
Red-mode / characterization tests for Plan 035 / Phase 2, Step 6 (§D-order) —
and its Phase-3/4/5 extensions (Steps 13, 19, 24).

Step 6 is a CHARACTERIZATION test: it must be GREEN today, pinning the
CURRENT verified middleware order (§D-order's "Verified current execution
order") BEFORE any of Plan 035's three new middlewares are added. It also
pins the Starlette ``add_middleware`` invariant this whole plan depends on:
the LAST ``add_middleware()`` call ends up OUTERMOST
(``user_middleware[0]``).

Steps 13/19/24 extend this file with the exact index each new middleware
lands at once Phases 3-5 land — they are written now and MUST fail until
the corresponding phase's ``create_varco_app`` keyword exists.
"""

from __future__ import annotations

from typing import Any

from providify import DIContainer
from starlette.applications import Starlette
from varco_core.i18n.settings import I18nSettings
from varco_fastapi.app import create_varco_app

# §D-order's verified CURRENT order (outermost -> innermost), BEFORE Plan 035:
_PHASE_2_BASELINE_ORDER = [
    "CORSMiddleware",
    "_Marker",
    "ErrorMiddleware",
    "RequestLoggingMiddleware",
    "TracingMiddleware",
    "MetricsMiddleware",
    "RequestContextMiddleware",
    "LocalizationMiddleware",
    "ProfilingMiddleware",
]


class _Marker:
    """A no-op ASGI middleware standing in for an app's ``extra_middleware=``."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        await self.app(scope, receive, send)


def _build_baseline_app(**extra_kwargs: Any) -> Any:
    # §D-S7-default/§D-S8-default: SecurityHeadersMiddleware/BodyLimitMiddleware
    # are ON by default in create_varco_app (security_headers=None /
    # body_limit=None) — see test_http_edge_introspect.py's default-app
    # expectations. This helper's OWN default is the Phase-2 baseline (all
    # three new middlewares off), so each Step 13/19/24 test below turns ON
    # exactly the one feature it exercises via **extra_kwargs, and the
    # "shift by exactly one" / "restores the Phase-2 list" comparisons stay
    # meaningful against the unchanged _PHASE_2_BASELINE_ORDER.
    #
    # Plan 049 / DI-1 migration: ONLY this helper's construction call
    # changes — every call site below (`_build_baseline_app(security_headers=
    # None)`, etc.) is untouched; the old `security_headers=`/`body_limit=`/
    # `rate_limit=` keyword *names* are kept as this helper's own kwargs and
    # translated into modules here. Position is now determined entirely by
    # `EdgeSlot` (varco_fastapi.middleware.slots) — module *list* order is
    # irrelevant to the resulting `app.user_middleware` order, which is the
    # byte-identical-order proof this file exists to pin.
    defaults: dict[str, Any] = {
        "security_headers": False,
        "body_limit": False,
        "rate_limit": None,
    }
    defaults.update(extra_kwargs)
    security_headers = defaults["security_headers"]
    body_limit = defaults["body_limit"]
    rate_limit = defaults["rate_limit"]

    from varco_core.di.module import CoreModule, ObservabilityModule
    from varco_fastapi.modules import (
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
    from varco_fastapi.modules.base import MiddlewareModule

    if security_headers is False:
        security_headers_module = SecurityHeadersModule(enabled=False)
    elif security_headers is None:
        security_headers_module = SecurityHeadersModule()
    else:
        security_headers_module = SecurityHeadersModule(settings=security_headers)

    if body_limit is False:
        body_limit_module = BodyLimitModule(enabled=False)
    elif body_limit is None:
        body_limit_module = BodyLimitModule()
    else:
        body_limit_module = BodyLimitModule(settings=body_limit)

    modules: list[Any] = [
        CoreModule(),
        ObservabilityModule(),
        FastAPIModule(),
        CorsModule(),
        security_headers_module,
        MiddlewareModule([_Marker]),
        ErrorEnvelopeModule(),
        body_limit_module,
        LoggingModule(),
        TracingModule(),
        MetricsModule(),
        LocalizationModule(i18n=I18nSettings(enabled=True)),
        ProfilingModule(),
    ]
    if rate_limit is not None:
        modules.append(RateLimitModule(rate_limit))

    # FastAPIModule always yields a RequestContextMiddleware spec — with the
    # bound AbstractServerAuth when one is resolvable, or in anonymous mode
    # (no server_auth kwarg, RequestContextMiddleware's own
    # `server_auth or AnonymousAuth()` default) otherwise. This restores
    # pre-049 parity (`_try_add_request_context_middleware`'s anonymous-mode
    # except branch) — see FastAPIModule's class docstring in
    # varco_fastapi/modules/edge.py — so no AbstractServerAuth binding is
    # needed here to keep RequestContextMiddleware present in the baseline.
    container = DIContainer()

    return create_varco_app(
        container,
        routers=[],
        modules=modules,
        validate=False,
    )


def test_verified_current_middleware_order_before_plan_035() -> None:
    """§D-order's Phase-2 baseline — GREEN today, pinned before any addition."""
    app = _build_baseline_app()
    names = [m.cls.__name__ for m in app.user_middleware]
    assert names == _PHASE_2_BASELINE_ORDER


def test_starlette_add_middleware_prepend_invariant() -> None:
    """
    The whole plan's ordering math rests on this: the LAST ``add_middleware``
    call ends up FIRST in ``user_middleware`` (outermost). A Starlette
    upgrade that inverts this must fail here, not silently reorder every
    varco app's stack.
    """

    class _A:
        def __init__(self, app: Any) -> None:
            self.app = app

    class _B:
        def __init__(self, app: Any) -> None:
            self.app = app

    app = Starlette()
    app.add_middleware(_A)
    app.add_middleware(_B)

    names = [m.cls.__name__ for m in app.user_middleware]
    assert names == ["_B", "_A"], "last add_middleware() call must be outermost"


# ── Step 13 (Phase 3, S7) — SecurityHeadersMiddleware at index 1 ────────────


def test_security_headers_middleware_lands_at_index_1_of_d_order() -> None:
    app = _build_baseline_app(security_headers=None)
    names = [m.cls.__name__ for m in app.user_middleware]
    assert names[0] == "CORSMiddleware"
    assert names[1] == "SecurityHeadersMiddleware"
    # Everything else shifts by exactly one, but is otherwise untouched.
    assert names[2:] == _PHASE_2_BASELINE_ORDER[1:]


def test_security_headers_false_restores_phase_2_list_exactly() -> None:
    app = _build_baseline_app(security_headers=False)
    names = [m.cls.__name__ for m in app.user_middleware]
    assert names == _PHASE_2_BASELINE_ORDER


# ── Step 19 (Phase 4, S8) — BodyLimitMiddleware immediately inside ErrorMiddleware


def test_body_limit_middleware_lands_immediately_inside_error_middleware() -> None:
    app = _build_baseline_app(body_limit=None)
    names = [m.cls.__name__ for m in app.user_middleware]
    error_index = names.index("ErrorMiddleware")
    assert names[error_index + 1] == "BodyLimitMiddleware"


def test_body_limit_false_removes_it() -> None:
    app = _build_baseline_app(body_limit=False)
    names = [m.cls.__name__ for m in app.user_middleware]
    assert "BodyLimitMiddleware" not in names


# ── Step 24 (Phase 5, S10) — two RateLimitMiddleware entries at positions 9/11


def test_rate_limit_bundle_with_ip_and_tenant_yields_two_entries_at_9_and_11() -> None:
    from varco_core.resilience.rate_limit import InMemoryRateLimiter, RateLimitConfig
    from varco_fastapi.middleware.rate_limit import (
        RateLimitBundle,
        RateLimitRule,
        RateLimitScope,
    )

    limiter = InMemoryRateLimiter(RateLimitConfig(rate=100, period=60.0))
    bundle = RateLimitBundle(
        rules=(
            RateLimitRule(scope=RateLimitScope.IP, limiter=limiter, name="ip"),
            RateLimitRule(scope=RateLimitScope.TENANT, limiter=limiter, name="tenant"),
        ),
        settings=None,
        # §D-S10-keyspace, Drift 1 fix: create_varco_app no longer forges this
        # acknowledgement — an IP-scoped InMemoryRateLimiter rule requires it
        # explicitly, same as a hand-registered RateLimitMiddleware.
        acknowledge_unbounded_keyspace=True,
    )
    app = _build_baseline_app(rate_limit=bundle)
    names = [m.cls.__name__ for m in app.user_middleware]
    rate_limit_indices = [i for i, n in enumerate(names) if n == "RateLimitMiddleware"]
    assert len(rate_limit_indices) == 2
    request_context_index = names.index("RequestContextMiddleware")
    # One outside RequestContextMiddleware (PRE_AUTH), one inside (POST_AUTH).
    assert rate_limit_indices[0] < request_context_index < rate_limit_indices[1]


def test_rate_limit_bundle_with_only_tenant_rule_yields_one_entry_inside() -> None:
    from varco_core.resilience.rate_limit import InMemoryRateLimiter, RateLimitConfig
    from varco_fastapi.middleware.rate_limit import (
        RateLimitBundle,
        RateLimitRule,
        RateLimitScope,
    )

    limiter = InMemoryRateLimiter(RateLimitConfig(rate=100, period=60.0))
    bundle = RateLimitBundle(
        rules=(RateLimitRule(scope=RateLimitScope.TENANT, limiter=limiter, name="tenant"),),
        settings=None,
    )
    app = _build_baseline_app(rate_limit=bundle)
    names = [m.cls.__name__ for m in app.user_middleware]
    rate_limit_indices = [i for i, n in enumerate(names) if n == "RateLimitMiddleware"]
    assert len(rate_limit_indices) == 1
    request_context_index = names.index("RequestContextMiddleware")
    assert rate_limit_indices[0] > request_context_index


def test_rate_limit_none_yields_no_rate_limit_middleware() -> None:
    app = _build_baseline_app(rate_limit=None)
    names = [m.cls.__name__ for m in app.user_middleware]
    assert "RateLimitMiddleware" not in names
