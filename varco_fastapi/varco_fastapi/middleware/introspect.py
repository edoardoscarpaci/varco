"""
varco_fastapi.middleware.introspect
====================================
``inspect_http_edge()`` — Plan 035 / Phase 6, Steps 27-28 (§D-seam).

**This is a seam for Plan 036's ``SecurityPosture`` preflight and is not
itself a preflight.** It reports what of Plan 035's HTTP edge hardening is
actually wired into a given app — it never warns at startup, never refuses
to boot, and is a pure, side-effect-free read. Per the index
(``plans/000-index-3-2-security-release.md:82-84``), Plan 035 defines and
exports this check; Plan 036 builds the harness that consumes it.

**Stable ``check`` ids this module commits to emitting** (Plan 036 may
render, group, or escalate them; it must not redefine them):

    http.security_headers.absent        warn   No SecurityHeadersMiddleware
    http.security_headers.hsts_absent   info   Installed, hsts_max_age == 0
    http.body_limit.absent              warn   No BodyLimitMiddleware
    http.rate_limit.absent              warn   No RateLimitMiddleware
    http.rate_limit.no_pre_auth_rule    warn   Installed, every rule POST_AUTH
    http.rate_limit.in_memory_limiter   info   A rule uses InMemoryRateLimiter
    http.rate_limit.fail_open           info   fail_open=True (the default)
    http.error.detail_exposed           warn   ErrorEnvelopeSettings.include_detail is True
    http.error.debug_enabled            high   ErrorMiddleware(debug=True)

DESIGN: a pure read over ``app.user_middleware``, returning a frozen
    dataclass (§D-seam)
    ✅ Starlette's ``Middleware`` exposes ``.cls``/``.kwargs`` and
       ``user_middleware`` survives startup — no registry, no global
       state, no cooperation needed from the middleware instances
       themselves.
    ✅ Frozen dataclass + pure function: Plan 036 consumes this with no
       import from itself into ``varco_fastapi.middleware``, and no shared
       mutable state. Same shape as Plan 037's ``RlsPosture``.
    ✅ Never raises and never touches the network — an unrecognised
       ``app`` object (missing/empty ``user_middleware``) returns an
       all-``False`` posture with ``http.*.absent`` findings, degrading
       gracefully (the index already calls this edge "soft").
    ❌ Reads a Starlette private-ish attribute (``Middleware.cls``/
       ``.kwargs``) with no documented public alternative.
       ``add_middleware()`` already raises after startup, so there is no
       supported alternative; ``test_http_edge_introspect.py``'s Step 30
       guards the attribute shape so a Starlette upgrade that changes it
       fails loudly here instead of silently returning an empty posture.
    Rejected — a module-global registry each middleware writes to on
    ``__init__``: ❌ process-global mutable state, wrong under multiple
    apps in one process, and would report a middleware from a *different*
    app.
    Rejected — building the preflight here: ⛔ Plan 036 owns it; the index
    forbids this plan from doing so.

Thread safety:  ✅ Pure function — no shared state, safe to call from any
                thread/coroutine.
Async safety:   ✅ Synchronous; no I/O, no ``await``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from varco_core.exception.settings import ErrorEnvelopeSettings
from varco_core.resilience.rate_limit import InMemoryRateLimiter

__all__ = ["HttpEdgeFinding", "HttpEdgePosture", "inspect_http_edge"]


@dataclass(frozen=True)
class HttpEdgeFinding:
    """
    One observation about the HTTP edge's configuration.

    Attributes:
        check:       Stable id — the contract Plan 036 keys on. One of the
                     nine strings in this module's docstring table.
        severity:    ``"info"`` | ``"warn"`` | ``"high"``.
        detail:      What was observed, in plain English.
        remediation: The exact env var or kwarg that changes it.
    """

    check: str
    severity: str
    detail: str
    remediation: str


@dataclass(frozen=True)
class HttpEdgePosture:
    """
    A snapshot of Plan 035's HTTP edge hardening as actually wired into an app.

    Attributes:
        security_headers_installed: Whether ``SecurityHeadersMiddleware`` is
            in the stack.
        security_headers_preset:    ``"balanced"`` | ``"strict"`` | ``None``
            (not installed).
        hsts_enabled:                Whether HSTS would ever be sent
            (``hsts_max_age > 0``).
        csp_enabled:                 Whether a CSP would ever be sent
            (``STRICT`` preset with a non-``None`` ``csp``).
        body_limit_installed:        Whether ``BodyLimitMiddleware`` is in
            the stack.
        body_limit_max_bytes:        The configured ceiling, or ``None`` if
            not installed.
        rate_limit_installed:        Whether any ``RateLimitMiddleware`` is
            in the stack.
        rate_limit_scopes:           Every distinct ``RateLimitScope`` value
            across all installed rules, e.g. ``("ip", "tenant")``.
        rate_limit_fail_open:        Whether a limiter failure allows the
            request through (``True``, the default) rather than 503.
        rate_limit_distributed:      ``True`` when every rule's limiter is
            NOT an ``InMemoryRateLimiter`` (i.e. safe across replicas).
        error_detail_exposed:        ``ErrorEnvelopeSettings().include_detail``
            — D-S3b's 4.0 flip candidate.
        error_debug_enabled:         Whether any ``ErrorMiddleware`` in the
            stack was constructed with ``debug=True``.
        findings:                    Every ``HttpEdgeFinding`` this posture
            triggered — see this module's docstring table for every
            possible ``check`` id.
    """

    security_headers_installed: bool
    security_headers_preset: str | None
    hsts_enabled: bool
    csp_enabled: bool
    body_limit_installed: bool
    body_limit_max_bytes: int | None
    rate_limit_installed: bool
    rate_limit_scopes: tuple[str, ...]
    rate_limit_fail_open: bool
    rate_limit_distributed: bool
    error_detail_exposed: bool
    error_debug_enabled: bool
    findings: tuple[HttpEdgeFinding, ...]


def _absent_posture() -> HttpEdgePosture:
    """The degraded, all-``False`` posture for an unrecognised ``app`` object."""
    return HttpEdgePosture(
        security_headers_installed=False,
        security_headers_preset=None,
        hsts_enabled=False,
        csp_enabled=False,
        body_limit_installed=False,
        body_limit_max_bytes=None,
        rate_limit_installed=False,
        rate_limit_scopes=(),
        rate_limit_fail_open=True,
        rate_limit_distributed=False,
        error_detail_exposed=ErrorEnvelopeSettings().include_detail,
        error_debug_enabled=False,
        findings=(
            HttpEdgeFinding(
                check="http.security_headers.absent",
                severity="warn",
                detail="No SecurityHeadersMiddleware in the stack.",
                remediation="create_varco_app(security_headers=None) (the default) "
                "or app.add_middleware(SecurityHeadersMiddleware).",
            ),
            HttpEdgeFinding(
                check="http.body_limit.absent",
                severity="warn",
                detail="No BodyLimitMiddleware in the stack.",
                remediation="create_varco_app(body_limit=None) (the default) "
                "or app.add_middleware(BodyLimitMiddleware).",
            ),
            HttpEdgeFinding(
                check="http.rate_limit.absent",
                severity="warn",
                detail="No RateLimitMiddleware in the stack.",
                remediation="create_varco_app(rate_limit=RateLimitBundle(...)).",
            ),
        ),
    )


def _cls_name(entry: Any) -> str:
    """The class name of a ``Middleware`` entry, or ``''`` on any unexpected shape."""
    cls = getattr(entry, "cls", None)
    return getattr(cls, "__name__", "") if cls is not None else ""


def inspect_http_edge(app: Any) -> HttpEdgePosture:
    """
    Report which of Plan 035's HTTP edge hardening is wired into ``app``.

    Args:
        app: A ``FastAPI``/``Starlette`` application (or any object — an
             unrecognised shape degrades to an all-absent posture).

    Returns:
        A frozen ``HttpEdgePosture`` — never raises.

    Edge cases:
        - ``app`` has no (or an empty) ``user_middleware`` — returns the
          all-absent posture, findings included.
        - A middleware registered without an explicit ``settings=`` kwarg
          (as several unit tests do) is treated as if constructed with
          that settings class's own defaults — the same defaults the
          middleware itself would apply.
        - Any unexpected exception while reading ``app`` is swallowed and
          treated as "unrecognised" (Edge case: "``inspect_http_edge()`` on
          a non-Starlette object" — the index's soft-edge contract).
    """
    try:
        return _inspect(app)
    except Exception:  # noqa: BLE001 — this seam must never raise (§D-seam)
        return _absent_posture()


def _inspect(app: Any) -> HttpEdgePosture:
    from varco_fastapi.middleware.body_limit import BodyLimitSettings
    from varco_fastapi.middleware.rate_limit import RateLimitSettings, RateLimitStage
    from varco_fastapi.middleware.security_headers import SecurityHeadersSettings

    middleware_list = list(getattr(app, "user_middleware", None) or [])

    security_entries = [m for m in middleware_list if _cls_name(m) == "SecurityHeadersMiddleware"]
    body_limit_entries = [m for m in middleware_list if _cls_name(m) == "BodyLimitMiddleware"]
    rate_limit_entries = [m for m in middleware_list if _cls_name(m) == "RateLimitMiddleware"]
    error_entries = [m for m in middleware_list if _cls_name(m) == "ErrorMiddleware"]

    findings: list[HttpEdgeFinding] = []

    # ── Security headers ─────────────────────────────────────────────────
    security_headers_installed = bool(security_entries)
    security_headers_preset: str | None = None
    hsts_enabled = False
    csp_enabled = False
    if security_headers_installed:
        settings = security_entries[0].kwargs.get("settings") or SecurityHeadersSettings()
        security_headers_preset = settings.preset.value
        hsts_enabled = settings.hsts_max_age > 0
        csp_enabled = settings.preset.value == "strict" and settings.csp is not None
        if not hsts_enabled:
            findings.append(
                HttpEdgeFinding(
                    check="http.security_headers.hsts_absent",
                    severity="info",
                    detail="SecurityHeadersMiddleware is installed but hsts_max_age == 0.",
                    remediation="SecurityHeadersSettings(hsts_max_age=31536000) or "
                    "VARCO_SECURITY_HEADERS_HSTS_MAX_AGE=31536000.",
                )
            )
    else:
        findings.append(
            HttpEdgeFinding(
                check="http.security_headers.absent",
                severity="warn",
                detail="No SecurityHeadersMiddleware in the stack.",
                remediation="create_varco_app(security_headers=None) (the default) "
                "or app.add_middleware(SecurityHeadersMiddleware).",
            )
        )

    # ── Body limit ────────────────────────────────────────────────────────
    body_limit_installed = bool(body_limit_entries)
    body_limit_max_bytes: int | None = None
    if body_limit_installed:
        settings = body_limit_entries[0].kwargs.get("settings") or BodyLimitSettings()
        body_limit_max_bytes = settings.max_bytes
    else:
        findings.append(
            HttpEdgeFinding(
                check="http.body_limit.absent",
                severity="warn",
                detail="No BodyLimitMiddleware in the stack.",
                remediation="create_varco_app(body_limit=None) (the default) "
                "or app.add_middleware(BodyLimitMiddleware).",
            )
        )

    # ── Rate limit ────────────────────────────────────────────────────────
    rate_limit_installed = bool(rate_limit_entries)
    rate_limit_scopes: tuple[str, ...] = ()
    rate_limit_fail_open = True
    rate_limit_distributed = False
    if rate_limit_installed:
        scopes: set[str] = set()
        limiters: list[Any] = []
        has_pre_auth = False
        settings_for_fail_open = None
        for entry in rate_limit_entries:
            kwargs = entry.kwargs
            stage = kwargs.get("stage")
            if stage == RateLimitStage.PRE_AUTH:
                has_pre_auth = True
            for rule in kwargs.get("rules", ()) or ():
                scopes.add(rule.scope.value)
                limiters.append(rule.limiter)
            if settings_for_fail_open is None and kwargs.get("settings") is not None:
                settings_for_fail_open = kwargs["settings"]
        rate_limit_scopes = tuple(sorted(scopes))
        settings_obj = settings_for_fail_open or RateLimitSettings()
        rate_limit_fail_open = settings_obj.fail_open
        rate_limit_distributed = bool(limiters) and all(
            not isinstance(limiter, InMemoryRateLimiter) for limiter in limiters
        )

        if not has_pre_auth:
            findings.append(
                HttpEdgeFinding(
                    check="http.rate_limit.no_pre_auth_rule",
                    severity="warn",
                    detail="RateLimitMiddleware is installed, but every rule is "
                    "POST_AUTH — an unauthenticated flood is unlimited.",
                    remediation="Add an IP or GLOBAL scoped RateLimitRule at stage=PRE_AUTH.",
                )
            )
        if any(isinstance(limiter, InMemoryRateLimiter) for limiter in limiters):
            findings.append(
                HttpEdgeFinding(
                    check="http.rate_limit.in_memory_limiter",
                    severity="info",
                    detail="A rate-limit rule uses InMemoryRateLimiter — per-process, "
                    "so the effective limit multiplies by replica count.",
                    remediation="Use varco_redis.rate_limit.RedisRateLimiter before "
                    "scaling past one replica.",
                )
            )
        if rate_limit_fail_open:
            findings.append(
                HttpEdgeFinding(
                    check="http.rate_limit.fail_open",
                    severity="info",
                    detail="fail_open=True (the default) — a limiter outage disables "
                    "enforcement rather than returning 503.",
                    remediation="RateLimitSettings(fail_open=False) or "
                    "VARCO_RATE_LIMIT_FAIL_OPEN=false.",
                )
            )
    else:
        findings.append(
            HttpEdgeFinding(
                check="http.rate_limit.absent",
                severity="warn",
                detail="No RateLimitMiddleware in the stack.",
                remediation="create_varco_app(rate_limit=RateLimitBundle(...)).",
            )
        )

    # ── Error envelope ────────────────────────────────────────────────────
    error_debug_enabled = any(bool(e.kwargs.get("debug", False)) for e in error_entries)
    if error_debug_enabled:
        findings.append(
            HttpEdgeFinding(
                check="http.error.debug_enabled",
                severity="high",
                detail="ErrorMiddleware(debug=True) — a stack-adjacent repr is "
                "included in every 500 response body.",
                remediation="ErrorMiddleware(debug=False) (the default) in production.",
            )
        )

    error_detail_exposed = ErrorEnvelopeSettings().include_detail
    if error_detail_exposed:
        findings.append(
            HttpEdgeFinding(
                check="http.error.detail_exposed",
                severity="warn",
                detail="ErrorEnvelopeSettings.include_detail is True — the "
                "application-authored str(exc) is echoed in every error body.",
                remediation="VARCO_ERROR_INCLUDE_DETAIL=false (4.0 flip candidate).",
            )
        )

    return HttpEdgePosture(
        security_headers_installed=security_headers_installed,
        security_headers_preset=security_headers_preset,
        hsts_enabled=hsts_enabled,
        csp_enabled=csp_enabled,
        body_limit_installed=body_limit_installed,
        body_limit_max_bytes=body_limit_max_bytes,
        rate_limit_installed=rate_limit_installed,
        rate_limit_scopes=rate_limit_scopes,
        rate_limit_fail_open=rate_limit_fail_open,
        rate_limit_distributed=rate_limit_distributed,
        error_detail_exposed=error_detail_exposed,
        error_debug_enabled=error_debug_enabled,
        findings=tuple(findings),
    )
