"""
varco_fastapi.middleware.body_limit
====================================
``BodyLimitMiddleware`` — Plan 035 / Phase 4, Step 17 (S8, §D-S8-default).

Enforces a hard ceiling on request-body bytes via two checks, per brief 008
§2:

1. A ``Content-Length`` pre-check — cheap, but spoofable or absent under
   chunked transfer-encoding, so it is an early rejection, not the real
   enforcement.
2. A cumulative count over every ``http.request`` ASGI message's ``body``
   bytes as they stream in through a wrapped ``receive()`` — this is the
   real enforcement, and it rejects **before buffering completes** (brief
   008 §2's stated reason to do this in ASGI middleware rather than after
   Starlette has already materialized the body).

Raises ``varco_core.exception.RequestBodyTooLargeError`` (a
``ServiceException``, HTTP 413) rather than returning a bare ``Response`` —
so the 413 renders through the one error envelope with ``correlation_id``,
which is exactly why §D-order places this middleware **inside**
``ErrorMiddleware``.

DESIGN: self-render the 413 when ``ErrorMiddleware`` is absent (§Edge cases)
    ✅ ``create_varco_app(enable_error_middleware=False)`` leaves nothing to
       catch a raised ``ServiceException`` — an uncaught raise becomes an
       unhandled 500, the opposite of this middleware's purpose. The
       constructor's ``has_error_middleware`` flag (set by
       ``create_varco_app`` from its own ``enable_error_middleware``, and
       ``True`` by default for a hand-registered instance — the common case
       is registering alongside ``ErrorMiddleware``, per every existing
       test in this suite) picks between the two behaviours.
    ✅ Reuses ``RateLimitMiddleware``'s own self-render helper
       (``middleware._json_response.send_json_error``) rather than a second
       JSON-construction path — the same envelope shape (``code``/
       ``message``/``correlation_id``) either way.
    ✅ When ``ErrorMiddleware`` **is** present the raise-through-the-envelope
       behaviour is unchanged byte-for-byte — this is the verified, tested
       path and nothing about it moves.
    ❌ Two response-construction code paths inside one middleware. Accepted:
       the alternative (always self-render) would stop the 413 from ever
       carrying ``correlation_id`` propagation semantics an app's own
       ``ErrorMiddleware`` configuration provides (localization, debug mode).

**Installed by default** at ``max_bytes = 10 MiB`` (§D-S8-default) —
deliberately deviating from brief 008's "opt-in to avoid breakage"
suggestion: the breakage here is loud, immediate, and self-describing (a
413 whose message names the ceiling and the env var), which the
blast-radius rule treats as a cheap caller-side fix. ``exempt_paths`` and
``VARCO_BODY_LIMIT_MAX_BYTES``/``VARCO_BODY_LIMIT_ENABLED`` cover the real
upload-endpoint case.

DESIGN: pure ASGI, never BaseHTTPMiddleware (§D-S8-default)
    ✅ ``BaseHTTPMiddleware`` cannot intercept ``receive()`` — it can only
       inspect a body Starlette has already fully buffered, which is
       exactly the failure mode brief 008 §2 warns about (memory
       exhaustion happens during buffering, not after). Wrapping
       ``receive`` is the only way to reject mid-stream.
    ✅ A route that never reads its body (e.g. it 404s before touching
       ``request.body()``) never triggers the cumulative check at all —
       the wrapped ``receive`` is inert until something calls it.

DESIGN: 10 MiB, not 1 MiB (nginx's default) or opt-in (brief 008's note)
    ✅ AWS API Gateway's hard 10 MB ceiling is a large share of production
       APIs already living under this exact number (brief 008 §2's
       reference table) — the conservative end of "will not surprise
       anyone", not the aggressive end.
    ❌ A file-upload endpoint above 10 MiB breaks on upgrade — mitigated by
       ``exempt_paths``, one env var, and a prominent upgrade note.

Thread safety:  ✅ Stateless per request — the byte counter lives in a
                closure local to one ``__call__`` invocation, never shared.
Async safety:   ✅ Pure ``async def``; no shared mutable state, no locks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic_settings import SettingsConfigDict
from varco_core.config import VarcoSettings
from varco_core.exception import RequestBodyTooLargeError
from varco_core.exception.http import error_code_for

from varco_fastapi.middleware._json_response import send_json_error

if TYPE_CHECKING:
    from starlette.types import Receive, Scope, Send

__all__ = ["BodyLimitMiddleware", "BodyLimitSettings"]


class BodyLimitSettings(VarcoSettings):
    """
    Configuration for ``BodyLimitMiddleware``, loaded from environment
    variables under the ``VARCO_BODY_LIMIT_`` prefix.

    Attributes:
        enabled:              Install the middleware's behaviour at all
                              (default ``True`` — §D-S8-default: on by
                              default). ``False`` is byte-identical to not
                              registering the middleware.
        max_bytes:            The byte ceiling. Default ``10 * 1024 * 1024``
                              (10 MiB, §D-S8-default).
        exempt_paths:         Request path prefixes exempt from the ceiling
                              entirely (e.g. a dedicated upload endpoint).
                              Default empty.
        trust_content_length: Whether to perform the cheap ``Content-Length``
                              pre-check at all. Default ``True`` — it is an
                              optimisation (reject before reading any body
                              bytes), never the sole check; the cumulative
                              ``receive()`` count always runs regardless.

    Thread safety:  ✅ Frozen pydantic model.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_BODY_LIMIT_", frozen=True)

    enabled: bool = True
    max_bytes: int = 10 * 1024 * 1024
    exempt_paths: tuple[str, ...] = ()
    trust_content_length: bool = True


class _BodyLimitSelfRendered(Exception):
    """
    Private sentinel used only to unwind out of ``self.app(...)`` once
    ``BodyLimitMiddleware`` has already sent a self-rendered 413 (the
    ``has_error_middleware=False`` path). Never escapes ``__call__`` —
    caught immediately after ``self.app()`` returns/raises.
    """


class BodyLimitMiddleware:
    """
    Pure-ASGI middleware enforcing a request-body byte ceiling.

    Args:
        app:      The wrapped ASGI application.
        settings: ``BodyLimitSettings`` instance. Defaults to reading from
                  the environment (``BodyLimitSettings()``).
        has_error_middleware: Whether an ``ErrorMiddleware`` sits further
                  out in the stack to catch and render the raised
                  ``RequestBodyTooLargeError``. Default ``True`` — a
                  hand-registered instance is overwhelmingly paired with
                  one, matching every pre-existing test in this suite.
                  ``create_varco_app`` passes its own
                  ``enable_error_middleware`` value here. When ``False``,
                  the middleware self-renders a plain JSON 413 instead of
                  raising (§Edge cases — the raise-through-the-envelope
                  path requires a catcher; with none, an uncaught raise is
                  an unhandled 500, the opposite of this middleware's
                  purpose).

    Raises:
        RequestBodyTooLargeError: When ``has_error_middleware=True`` (the
            default) and the declared ``Content-Length`` exceeds
            ``max_bytes`` (rejected before any body byte is read), or the
            cumulative count over ``receive()`` crosses it (rejected
            mid-stream, before Starlette finishes buffering). Never raised
            when ``has_error_middleware=False`` — see ``Edge cases``.

    Edge cases:
        - A non-``http`` scope, ``enabled=False``, or a path matching
          ``exempt_paths`` passes through untouched — no wrapping at all.
        - A body exactly at ``max_bytes`` passes (the check is ``>``, not
          ``>=``).
        - A route that never reads its body never triggers the cumulative
          check — the wrapped ``receive`` is inert until called.
        - A streaming *response* is untouched — this middleware only wraps
          ``receive`` (the request), never ``send`` (the response).
        - ``has_error_middleware=False``: over-limit requests get a
          self-rendered plain JSON 413 (``code``/``message``/
          ``correlation_id``) instead of a raised exception — see this
          module's ``DESIGN: self-render the 413...`` note.

    Thread safety:  ✅ Stateless — settings are read-only after construction.
    Async safety:   ✅ Pure ``async def``; the byte counter is a closure
                    local to one request, never shared across requests.
    """

    def __init__(
        self,
        app: Any,
        *,
        settings: BodyLimitSettings | None = None,
        has_error_middleware: bool = True,
    ) -> None:
        self.app = app
        self._settings = settings or BodyLimitSettings()
        self._has_error_middleware = has_error_middleware

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        settings = self._settings
        if scope["type"] != "http" or not settings.enabled:
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if any(path.startswith(prefix) for prefix in settings.exempt_paths):
            await self.app(scope, receive, send)
            return

        if settings.trust_content_length:
            declared = self._declared_content_length(scope)
            if declared is not None and declared > settings.max_bytes:
                await self._reject(scope, receive, send, settings.max_bytes)
                return

        total_bytes = 0

        async def receive_wrapper() -> Any:
            nonlocal total_bytes
            message = await receive()
            if message["type"] == "http.request":
                total_bytes += len(message.get("body", b""))
                if total_bytes > settings.max_bytes:
                    # Reject mid-stream, before Starlette finishes buffering
                    # the body (brief 008 §2) — this is the real
                    # enforcement; the Content-Length pre-check above is
                    # only a cheap optimisation.
                    if self._has_error_middleware:
                        raise RequestBodyTooLargeError(max_bytes=settings.max_bytes)
                    # No ErrorMiddleware to render a raised exception —
                    # self-render right here (receive_wrapper still has
                    # scope/receive/send in closure) and raise a private
                    # sentinel purely to unwind out of self.app(), which
                    # __call__ below swallows (§Edge cases).
                    await self._reject(scope, receive, send, settings.max_bytes)
                    raise _BodyLimitSelfRendered
            return message

        try:
            await self.app(scope, receive_wrapper, send)
        except _BodyLimitSelfRendered:
            pass

    async def _reject(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        max_bytes: int,
    ) -> None:
        """
        Reject an over-limit request found by the ``Content-Length``
        pre-check: raise (for ``ErrorMiddleware`` to render) when present,
        self-render a plain JSON 413 otherwise (§Edge cases —
        ``enable_error_middleware=False``).
        """
        exc = RequestBodyTooLargeError(max_bytes=max_bytes)
        if self._has_error_middleware:
            raise exc
        await send_json_error(
            scope,
            receive,
            send,
            status_code=413,
            code=error_code_for(exc).code,
            message=str(exc),
        )

    @staticmethod
    def _declared_content_length(scope: Scope) -> int | None:
        """Parse the declared ``Content-Length`` from the ASGI scope, if present and valid."""
        headers = dict(scope.get("headers") or [])
        raw = headers.get(b"content-length")
        if raw is None:
            return None
        try:
            return int(raw)
        except ValueError:
            # A malformed Content-Length is not this middleware's concern —
            # the cumulative receive() count is the real enforcement either way.
            return None
