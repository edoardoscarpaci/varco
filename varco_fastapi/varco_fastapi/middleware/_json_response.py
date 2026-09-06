"""
varco_fastapi.middleware._json_response
========================================
Shared, private "send a standalone JSON error response" helper for
middleware that must render an error **without** relying on
``ErrorMiddleware`` being present in the stack — ``RateLimitMiddleware``
(its 429/503, which always self-renders so its extra headers like
``Retry-After``/``RateLimit-Policy`` have somewhere to attach, Plan 035 /
§D-S10-headers) and ``BodyLimitMiddleware`` (its 413, only when
``ErrorMiddleware`` is absent — Plan 035 / §Edge cases: *"Both must then
emit a plain JSON response themselves rather than raise"*).

Factored out here rather than duplicated, and rather than living in
``_forwarded.py`` (which is scoped to the trusted-proxy/``X-Forwarded-*``
concern shared by ``SecurityHeadersMiddleware``/``RateLimitMiddleware`` —
a different, unrelated seam) so each private helper module has exactly one
reason to change.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from starlette.responses import JSONResponse
from varco_core.tracing import current_correlation_id, generate_correlation_id

if TYPE_CHECKING:
    from starlette.types import Receive, Scope, Send

__all__ = ["send_json_error"]


async def send_json_error(
    scope: Scope,
    receive: Receive,
    send: Send,
    *,
    status_code: int,
    code: str,
    message: str,
    headers: dict[str, str] | None = None,
) -> None:
    """
    Send a standalone JSON error response, bypassing ``ErrorMiddleware``.

    Always carries ``correlation_id`` — the ambient one if set, a freshly
    generated one otherwise (the same fallback ``ErrorMiddleware`` uses,
    Plan 035 / §D-S3a) — so the response is no less correlatable for not
    going through the envelope machinery.

    Args:
        scope, receive, send: The ASGI call's own arguments.
        status_code: HTTP status to send.
        code:        Stable machine error code for the body's ``code`` field.
        message:     Human-readable message for the body's ``message`` field.
        headers:     Extra response headers (e.g. ``Retry-After``).

    Thread safety: ✅ Pure function, no shared state.
    Async safety:  ✅ Awaits only the ASGI ``send`` callable it is given.
    """
    body: dict[str, Any] = {
        "code": code,
        "message": message,
        "correlation_id": current_correlation_id() or generate_correlation_id(),
    }
    response = JSONResponse(status_code=status_code, content=body, headers=headers)
    await response(scope, receive, send)
