"""
varco_fastapi.middleware
========================
ASGI middleware stack for varco_fastapi applications.

Recommended middleware order (outermost to innermost)::

    app = FastAPI(lifespan=lifespan)

    # 1. CORS — must be outermost so preflight OPTIONS requests are handled
    install_cors(app, CORSConfig.from_env())

    # 2. Error handling — catches errors from ALL middleware below
    app.add_middleware(ErrorMiddleware)

    # 3. Tracing — wraps requests in OTel spans (optional)
    app.add_middleware(TracingMiddleware)

    # 4. Logging — logs after tracing sets up correlation ID
    app.add_middleware(RequestLoggingMiddleware, skip_paths={"/health"})

    # 5. Request context — sets auth + tenant ContextVars
    app.add_middleware(RequestContextMiddleware, server_auth=my_auth)

    # 6. Session — DI container per request (optional, advanced)
    app.add_middleware(SessionMiddleware, container=my_container)

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

from varco_fastapi.middleware.cors import CORSConfig, install_cors
from varco_fastapi.middleware.error import ErrorMiddleware
from varco_fastapi.middleware.logging import RequestLoggingMiddleware
from varco_fastapi.middleware.request_context import RequestContextMiddleware
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
]
