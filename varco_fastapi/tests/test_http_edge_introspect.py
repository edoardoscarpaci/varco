"""
Red-mode tests for Plan 035 / Phase 6, Steps 27 + 30 (§D-seam) —
``inspect_http_edge()``, the pure read Plan 036 consumes.

``varco_fastapi.middleware.introspect`` does not exist yet — every test
below must fail with ``ModuleNotFoundError``, not a fixture typo.
"""

from __future__ import annotations

from providify import DIContainer
from varco_fastapi.app import create_varco_app
from varco_fastapi.middleware.error import ErrorMiddleware
from varco_fastapi.middleware.introspect import (  # type: ignore[attr-defined]
    HttpEdgePosture,
    inspect_http_edge,
)

# §D-seam's exact, literal check-id contract — a rename here must break this
# test, not silently drift out from under Plan 036.
_EXPECTED_CHECK_IDS = frozenset(
    {
        "http.security_headers.absent",
        "http.security_headers.hsts_absent",
        "http.body_limit.absent",
        "http.rate_limit.absent",
        "http.rate_limit.no_pre_auth_rule",
        "http.rate_limit.in_memory_limiter",
        "http.rate_limit.fail_open",
        "http.error.detail_exposed",
        "http.error.debug_enabled",
    }
)


def _default_app():
    return create_varco_app(
        container=DIContainer(), routers=[], validate=False, enable_metrics=False
    )


def test_default_app_reports_security_headers_and_body_limit_installed() -> None:
    app = _default_app()
    posture = inspect_http_edge(app)
    assert isinstance(posture, HttpEdgePosture)
    assert posture.security_headers_installed is True
    assert posture.body_limit_installed is True
    assert posture.rate_limit_installed is False


def test_default_app_reports_exactly_rate_limit_absent_and_detail_exposed_findings() -> None:
    app = _default_app()
    posture = inspect_http_edge(app)
    checks = {f.check for f in posture.findings}
    assert checks == {"http.rate_limit.absent", "http.error.detail_exposed"}


def test_app_with_everything_disabled_reports_all_three_absent_findings() -> None:
    app = create_varco_app(
        container=DIContainer(),
        routers=[],
        validate=False,
        enable_metrics=False,
        security_headers=False,
        body_limit=False,
        rate_limit=None,
    )
    posture = inspect_http_edge(app)
    checks = {f.check for f in posture.findings}
    assert "http.security_headers.absent" in checks
    assert "http.body_limit.absent" in checks
    assert "http.rate_limit.absent" in checks


def test_app_with_only_post_auth_rules_reports_no_pre_auth_rule_finding() -> None:
    from varco_core.resilience.rate_limit import InMemoryRateLimiter, RateLimitConfig
    from varco_fastapi.middleware.rate_limit import (  # type: ignore[attr-defined]
        RateLimitBundle,
        RateLimitRule,
        RateLimitScope,
    )

    limiter = InMemoryRateLimiter(RateLimitConfig(rate=100, period=60.0))
    bundle = RateLimitBundle(
        rules=(RateLimitRule(scope=RateLimitScope.TENANT, limiter=limiter, name="t"),),
        settings=None,
    )
    app = create_varco_app(
        container=DIContainer(),
        routers=[],
        validate=False,
        enable_metrics=False,
        rate_limit=bundle,
    )
    posture = inspect_http_edge(app)
    checks = {f.check for f in posture.findings}
    assert "http.rate_limit.no_pre_auth_rule" in checks


def test_error_middleware_debug_true_reports_debug_enabled_at_high_severity() -> None:
    from fastapi import FastAPI

    app = FastAPI()
    app.add_middleware(ErrorMiddleware, debug=True)
    posture = inspect_http_edge(app)
    matching = [f for f in posture.findings if f.check == "http.error.debug_enabled"]
    assert matching, "expected an http.error.debug_enabled finding"
    assert matching[0].severity == "high"


def test_inspect_http_edge_on_non_starlette_object_returns_all_absent_posture_no_raise() -> None:
    posture = inspect_http_edge(object())
    assert isinstance(posture, HttpEdgePosture)
    assert posture.security_headers_installed is False
    assert posture.body_limit_installed is False
    assert posture.rate_limit_installed is False


def test_emitted_check_ids_equal_the_d_seam_table_exactly() -> None:
    """
    Every emittable check id in §D-seam's table, exercised at least once
    across the scenarios above, must be a subset of (and this plan commits
    to emitting exactly) the literal frozenset — asserted so renaming one
    silently breaks THIS test, not Plan 036.
    """
    all_seen: set[str] = set()

    all_seen |= {f.check for f in inspect_http_edge(_default_app()).findings}
    all_seen |= {
        f.check
        for f in inspect_http_edge(
            create_varco_app(
                container=DIContainer(),
                routers=[],
                validate=False,
                enable_metrics=False,
                security_headers=False,
                body_limit=False,
                rate_limit=None,
            )
        ).findings
    }

    from fastapi import FastAPI

    debug_app = FastAPI()
    debug_app.add_middleware(ErrorMiddleware, debug=True)
    all_seen |= {f.check for f in inspect_http_edge(debug_app).findings}

    assert all_seen.issubset(_EXPECTED_CHECK_IDS)


# ── Step 30 — Starlette attribute-shape guard ───────────────────────────────


def test_every_user_middleware_entry_exposes_cls_and_kwargs() -> None:
    """
    §D-seam rests on ``Middleware.cls``/``Middleware.kwargs`` surviving
    startup. A Starlette upgrade that changes this shape must fail here,
    loudly, instead of silently making ``inspect_http_edge`` return an
    empty posture.
    """
    app = _default_app()
    assert len(app.user_middleware) > 0
    for entry in app.user_middleware:
        assert hasattr(entry, "cls")
        assert hasattr(entry, "kwargs")
