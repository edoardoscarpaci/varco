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
"""

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
]
