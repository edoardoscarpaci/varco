"""
varco_fastapi.middleware
========================
ASGI middleware stack for varco_fastapi applications.

**This module is the single normative home for the verified middleware
order** (Plan 035 / §D-order, Phase 2). ``varco_fastapi/app.py``'s module
docstring and its ``create_varco_app`` inline comments point back here
instead of restating it — a prior copy in each of those three places had
drifted out of sync with what ``create_varco_app`` actually builds (see
§D-order-bugs; two of the three questions raised there are filed as
BACKLOG rows, not fixed here — this plan does not reorder the stack).

Verified execution order (outermost → innermost) as built by
``create_varco_app`` with every optional middleware enabled — **new
entries from Plan 035 in bold**::

    CORSMiddleware
    **SecurityHeadersMiddleware**       (opt-out: security_headers=False)
    extra_middleware=[...]              (caller-supplied, OUTSIDE ErrorMiddleware —
                                          see §D-order-bugs: a ServiceException
                                          raised here is NOT rendered by the
                                          error envelope)
    ErrorMiddleware
    **BodyLimitMiddleware**             (opt-out: body_limit=False)
    RequestLoggingMiddleware
    TracingMiddleware
    MetricsMiddleware                   (INSIDE Tracing — Plan 041 / §D-S17-decision:
                                          exemplars need a current span)
    **RateLimitMiddleware(stage=PRE_AUTH)**   (opt-in: rate_limit=RateLimitBundle(...);
                                                only IP/GLOBAL-scoped rules legal here)
    RequestContextMiddleware            (populates AuthContext / current_tenant())
    **RateLimitMiddleware(stage=POST_AUTH)**  (opt-in, same bundle; SUBJECT/TENANT rules)
    LocalizationMiddleware
    IdempotencyMiddleware               (opt-in, unchanged — Plan 029)
    ProfilingMiddleware
    route handler

Why each new entry sits where it does (full DESIGN blocks + rejected
alternatives): ``technical_docs/features/http-edge-hardening.md``'s
ordering-contract section, and Plan 035's §D-order /
§D-order (one-RateLimitMiddleware-two-positions design).

``create_varco_app`` never moves an existing ``add_middleware`` call to make
room for these three — each is inserted at its own, separately-tested
position (``varco_fastapi/tests/test_middleware_order.py``).

The low-level ``app.add_middleware`` API is a **stack** — each call inserts
the middleware at the front, so the last ``add_middleware`` call ends up
*outermost*.  This is counter-intuitive when registering middlewares manually.
Use ``install_middleware_stack`` to register them in the natural outermost-first
order without thinking about reversal::

    install_middleware_stack(app, [
        ErrorMiddleware,                                # outermost — first in list
        (install_cors, {}),                             # callable installer
        (RequestContextMiddleware, {"server_auth": auth}),  # innermost — last in list
    ])
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from varco_fastapi.middleware.body_limit import BodyLimitMiddleware, BodyLimitSettings
from varco_fastapi.middleware.cors import CORSConfig, install_cors
from varco_fastapi.middleware.error import ErrorMiddleware
from varco_fastapi.middleware.idempotency import IdempotencyMiddleware
from varco_fastapi.middleware.introspect import HttpEdgeFinding, HttpEdgePosture, inspect_http_edge
from varco_fastapi.middleware.logging import RequestLoggingMiddleware
from varco_fastapi.middleware.metrics import MetricsMiddleware
from varco_fastapi.middleware.profiling import ProfilingMiddleware, ProfilingSettings
from varco_fastapi.middleware.rate_limit import (
    RateLimitBundle,
    RateLimitMiddleware,
    RateLimitRule,
    RateLimitScope,
    RateLimitSettings,
    RateLimitStage,
)
from varco_fastapi.middleware.request_context import RequestContextMiddleware
from varco_fastapi.middleware.security_headers import (
    SecurityHeadersMiddleware,
    SecurityHeadersPreset,
    SecurityHeadersSettings,
)
from varco_fastapi.middleware.session import (
    SessionMiddleware,
    get_container,
    get_session_dependency,
)
from varco_fastapi.middleware.tracing import TracingMiddleware

# ── MiddlewareEntry type ──────────────────────────────────────────────────────
# Each entry is one of:
#   - A bare middleware class       → app.add_middleware(cls) with no kwargs
#   - (cls, kwargs)                 → app.add_middleware(cls, **kwargs)
#   - (callable_installer, kwargs)  → callable_installer(app, **kwargs)
#
# The callable_installer form lets non-standard installers like install_cors
# participate in the stack without needing a wrapper.
MiddlewareEntry = type | tuple[type | Callable[..., None], dict[str, Any]]


def install_middleware_stack(
    app: Any,
    middlewares: list[MiddlewareEntry],
) -> None:
    """
    Register middlewares in outermost-to-innermost order.

    Starlette's ``add_middleware`` prepends to the middleware stack — the last
    call becomes the outermost middleware.  When registering several middlewares
    manually, this means they must be added in *reverse* order, which is
    confusing.  ``install_middleware_stack`` accepts the natural ordering
    (first entry = outermost) and internally reverses the list before calling
    ``add_middleware``.

    Each entry in ``middlewares`` is one of:

    - A bare class — ``app.add_middleware(cls)`` with no extra kwargs.
    - ``(cls, kwargs)`` — ``app.add_middleware(cls, **kwargs)`` with kwargs.
    - ``(callable_installer, kwargs)`` — ``callable_installer(app, **kwargs)``.
      Use this for helpers like ``install_cors`` that expand a config object
      into ``CORSMiddleware`` kwargs internally.

    DESIGN: MiddlewareEntry union over a single class-only list
        ✅ Supports install_cors (and similar helpers) without wrapping.
        ✅ Outermost-first list is the intuitive mental model for middleware.
        ✅ Internally reversing keeps the caller unaware of Starlette internals.
        ❌ Three-variant union is slightly more complex to document.

    Args:
        app:         The ``FastAPI`` (or Starlette) application.
        middlewares: Ordered list of middleware entries (outermost first).

    Raises:
        TypeError: An entry is not a type, a 2-tuple, or a callable.

    Edge cases:
        - Empty ``middlewares`` is a no-op — safe to call with ``[]``.
        - An installer callable receives ``app`` as its first argument and
          ``**kwargs`` as keyword arguments.
        - The reversal is intentional — first entry ends up outermost.
        - Calling twice with the same entry wraps that middleware twice.

    Thread safety:  ✅ Called once at startup before requests arrive.
    Async safety:   ✅ Synchronous — no I/O.

    Example::

        from varco_fastapi.middleware import (
            install_middleware_stack,
            ErrorMiddleware,
            RequestContextMiddleware,
            install_cors,
        )

        install_middleware_stack(app, [
            ErrorMiddleware,
            (install_cors, {"config": CORSConfig.from_env()}),
            (RequestContextMiddleware, {"server_auth": server_auth}),
        ])
    """
    # Reverse so the first entry (outermost intent) ends up truly outermost after
    # Starlette's prepend-each-call semantics.
    for entry in reversed(middlewares):
        if isinstance(entry, tuple):
            # Two-tuple: (class_or_callable, kwargs_dict)
            cls_or_installer, kwargs = entry
            if isinstance(cls_or_installer, type):
                # Standard Starlette add_middleware path — cls + keyword args.
                app.add_middleware(cls_or_installer, **kwargs)
            else:
                # Callable installer (e.g. install_cors) — passes app as first arg.
                cls_or_installer(app, **kwargs)
        elif isinstance(entry, type):
            # Bare class with no kwargs.
            app.add_middleware(entry)
        else:
            raise TypeError(
                f"install_middleware_stack: expected a middleware type or "
                f"(type, kwargs) tuple, got {entry!r}. "
                "Wrap callables as (callable, {}) or (callable, kwargs_dict)."
            )


__all__ = [
    "ErrorMiddleware",
    "IdempotencyMiddleware",
    "MetricsMiddleware",
    "ProfilingMiddleware",
    "ProfilingSettings",
    "RequestContextMiddleware",
    "TracingMiddleware",
    "RequestLoggingMiddleware",
    "CORSConfig",
    "install_cors",
    "SessionMiddleware",
    "get_container",
    "get_session_dependency",
    "install_middleware_stack",
    "MiddlewareEntry",
    # Plan 035 / S7 — security headers
    "SecurityHeadersMiddleware",
    "SecurityHeadersPreset",
    "SecurityHeadersSettings",
    # Plan 035 / S8 — request body limits
    "BodyLimitMiddleware",
    "BodyLimitSettings",
    # Plan 035 / S10 — HTTP rate limiting
    "RateLimitBundle",
    "RateLimitMiddleware",
    "RateLimitRule",
    "RateLimitScope",
    "RateLimitSettings",
    "RateLimitStage",
    # Plan 035 / §D-seam — the Plan 036 introspection seam
    "HttpEdgeFinding",
    "HttpEdgePosture",
    "inspect_http_edge",
]
