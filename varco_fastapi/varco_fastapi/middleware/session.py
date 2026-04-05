"""
varco_fastapi.middleware.session
================================
Request-scoped DI / DB session lifecycle middleware.

This middleware bridges FastAPI's per-request lifecycle with the varco providify
DI container.  It provides two complementary approaches:

1. **Middleware approach** (via ``SessionMiddleware``):
   ASGI middleware that stores the DI container in a ContextVar at the start
   of each request.  Downstream code can read it via ``get_container()``.

2. **Depends approach** (recommended for most use cases):
   ``get_session_dependency(session_factory)`` creates a FastAPI ``Depends()``
   factory that yields an async DB session per request and closes it afterwards.
   This is the simpler approach and does not require middleware.

The two approaches can be combined: use ``SessionMiddleware`` for container
access and the ``Depends`` factory for individual session injection.

DESIGN: FastAPI Depends(yield) over providify create_scope()
    ✅ Simpler — Depends(yield) is a FastAPI first-class feature
    ✅ No changes to providify needed — works with the current API
    ✅ Compatible with all FastAPI features (background tasks, streaming, etc.)
    ✅ Same pattern as most FastAPI tutorials and SQLAlchemy integrations
    ❌ Session must be passed as a Depends() argument to each route that needs it
       — RequestContextMiddleware cannot inject it transparently (use middleware
       approach for that, when providify supports create_scope())

Thread safety:  ✅ ContextVar is task-local — no shared state between requests.
Async safety:   ✅ All generators and context managers are ``async``.
"""

from __future__ import annotations

import logging
from typing import Any, TypeVar, Optional, Final
from uuid import uuid4
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from providify import DIContainer

_logger = logging.getLogger(__name__)

_REQUEST_ID_HEADER: Final[str] = "X-Session-Id"
T = TypeVar("T")


def _generate_session_id():
    return str(uuid4())


# ── ContextVar accessors ───────────────────────────────────────────────────────


def get_container() -> Any | None:
    """
    Return the DI container set by ``SessionMiddleware``, or ``None``.

    Returns ``None`` if ``SessionMiddleware`` is not installed or has not yet
    set the container for this request.

    Thread safety:  ✅ ContextVar read — task-local.
    Async safety:   ✅ Pure read; no I/O.
    """
    return DIContainer.current()


def get_session_dependency(session_factory: Any) -> Any:
    """
    Create a FastAPI ``Depends()`` callable that provides a scoped session.

    Yields a session from ``session_factory`` within the request lifetime.
    The session is closed in the ``finally`` block whether the handler succeeds
    or raises.

    Args:
        session_factory: A zero-argument callable (e.g. a SQLAlchemy
                         ``sessionmaker`` or ``async_sessionmaker``) that
                         returns (or async-yields) a session object.

    Returns:
        A FastAPI dependency (async generator) suitable for use with
        ``Depends(get_session_dependency(factory))``.

    Edge cases:
        - If ``session_factory`` is an ``async_sessionmaker``, the dependency
          must be ``async def``; this implementation returns an async generator.
        - Exceptions from the handler propagate naturally — the session is
          always closed in ``finally``.
    """

    async def _session_dep() -> Any:
        """FastAPI dependency that yields a database session."""
        session = session_factory()
        try:
            yield session
        finally:
            if hasattr(session, "aclose"):
                await session.aclose()
            elif hasattr(session, "close"):
                session.close()

    return _session_dep


# ── SessionMiddleware ──────────────────────────────────────────────────────────


class DISessionMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware that stores the DI container in a ContextVar per request.

    Use this to spawn a scoped DI to use scoped identifier ScopeRequest and ScopeSession

    Args:
        app:       The ASGI application to wrap.
        container: The providify ``DIContainer`` to store per request.

    Thread safety:  ✅ Stores container reference (not a copy) — container
                       itself must be thread-safe for concurrent reads.
    Async safety:   ✅ ContextVar is task-local.

    Edge cases:
        - The container reference is the SAME object for all requests —
          per-request scoping requires the container to support ``create_scope()``
          (future providify feature).  Currently, this just makes the container
          accessible from ContextVar without per-request isolation.
    """

    def __init__(self, app, *, container: Optional[DIContainer] = None) -> None:
        super().__init__(app)
        self._container = container or DIContainer.current()

    async def dispatch(self, request: Request, call_next) -> Response:
        """Store the container in the ContextVar for this request."""
        session_id = request.headers.get(_REQUEST_ID_HEADER) or _generate_session_id()
        async with self._container.asession(session_id):
            async with self._container.arequest():
                return await call_next(request)


# Alias for backwards compatibility — DISessionMiddleware was the original name
SessionMiddleware = DISessionMiddleware

__all__ = [
    "DISessionMiddleware",
    "SessionMiddleware",
]
