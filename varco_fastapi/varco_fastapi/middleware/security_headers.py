"""
varco_fastapi.middleware.security_headers
==========================================
``SecurityHeadersMiddleware`` — Plan 035 / Phase 3, Step 11 (S7, §D-S7-default).

Sets a baseline of security-relevant HTTP response headers on **every**
response, including error responses (it sits outside ``ErrorMiddleware``,
§D-order). Two presets:

    BALANCED (default) — four headers, safe for a JSON API:
        X-Content-Type-Options: nosniff
        X-Frame-Options: DENY
        Referrer-Policy: strict-origin-when-cross-origin
        Strict-Transport-Security: max-age=<n>; includeSubDomains  (HTTPS only)

    STRICT (opt-in) — BALANCED plus:
        Content-Security-Policy: default-src 'none'; frame-ancestors 'none'
        Cross-Origin-Opener-Policy: same-origin
        Cross-Origin-Resource-Policy: same-origin
        Permissions-Policy: geolocation=(), microphone=(), camera=()

**Deliberately not sent, ever**: ``X-XSS-Protection`` (deprecated and
harmful per brief 008 §1 — modern browsers ignore it, and it has a history
of introducing XSS via its own filter heuristics), ``Server``/
``X-Powered-By`` (set by uvicorn below the ASGI application — a middleware
running inside the app cannot reliably remove a header the server layer
adds after the app returns).

**Installed by default** by ``create_varco_app`` (§D-S7-default) — every
varco app gains the four BALANCED headers on upgrade to 3.2.
``VARCO_SECURITY_HEADERS_ENABLED=false`` (or ``security_headers=False``)
turns it off entirely; any individual header field can be set to ``None``
to omit just that one.

DESIGN: BALANCED omits CSP and CORP (§D-S7-default, citing brief 008 §1)
    ✅ FastAPI ships ``/docs``/``/redoc`` loading a CDN script — a
       ``default-src 'none'`` CSP breaks them, and the "safer" permissive
       ``default-src 'self'`` alternative (secure.py's own default) breaks
       them too (the CDN is not same-origin) while protecting close to
       nothing on a JSON body. A header that is either broken or
       meaningless is worse than no header — so BALANCED sends none, and
       STRICT sends the real one alongside an ``exclude_paths`` default
       covering ``/docs``/``/redoc``/``/openapi.json``.
    ✅ ``Cross-Origin-Resource-Policy: same-origin`` instructs the browser
       to block cross-origin *reads* of the response — exactly what a
       CORS-enabled API's ``allow_origins`` exists to permit. Shipping it
       in BALANCED would silently defeat a configured CORS policy for
       every browser caller. COOP is inert on a JSON response (it only
       matters for top-level documents), so it rides into STRICT with CORP
       rather than adding a no-op header everywhere.
    ❌ An operator wanting defence-in-depth against framing/MIME-sniffing/
       cross-origin reads on an HTML-serving varco app must explicitly opt
       into STRICT. Accepted — a JSON API is the common case this default
       is tuned for.

DESIGN: HSTS is scheme-guarded, and X-Forwarded-Proto is trusted only from a
    configured proxy CIDR (§D-S7-default, §D-S10-ip's trust rule reused)
    ✅ Sending HSTS over plaintext is a lie about the connection — browsers
       ignore it anyway (brief 008 §1) — so it is emitted only when
       ``scope["scheme"] == "https"``, or when ``X-Forwarded-Proto: https``
       arrives from a peer that ``trusted_proxies`` names explicitly.
    ❌ An operator behind a TLS-terminating proxy who forgets
       ``VARCO_SECURITY_HEADERS_TRUSTED_PROXIES`` never gets HSTS — loud in
       the sense that a security scanner will flag it, but not
       self-diagnosing at request time. Documented as a Pitfall.

DESIGN: pure ASGI, never BaseHTTPMiddleware
    ✅ Only needs to mutate ``http.response.start`` headers — never touches
       the body, so wrapping ``receive``/buffering (what
       ``BaseHTTPMiddleware`` does under the hood) is pure overhead this
       middleware has no use for.
    ✅ Costs nothing extra on a streaming response — the body iterator is
       never touched.

DESIGN: setdefault semantics — a route or downstream middleware wins
    ✅ Additive by construction: this middleware only fills in headers the
       response does not already carry, so an app with its own, more
       specific header value is never overridden.

Thread safety:  ✅ Stateless per request — settings are read-only after
                construction (frozen ``SecurityHeadersSettings``).
Async safety:   ✅ Pure ``async def __call__``; no shared mutable state.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic_settings import SettingsConfigDict
from starlette.datastructures import MutableHeaders
from varco_core.config import VarcoSettings

from varco_fastapi.middleware._forwarded import peer_is_trusted_proxy, resolve_forwarded_proto

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = ["SecurityHeadersMiddleware", "SecurityHeadersPreset", "SecurityHeadersSettings"]


class SecurityHeadersPreset(StrEnum):
    """Which header set ``SecurityHeadersMiddleware`` sends — see module docstring."""

    BALANCED = "balanced"
    STRICT = "strict"


class SecurityHeadersSettings(VarcoSettings):
    """
    Configuration for ``SecurityHeadersMiddleware``, loaded from environment
    variables under the ``VARCO_SECURITY_HEADERS_`` prefix.

    Attributes:
        enabled:  Install the middleware's behaviour at all (default
                  ``True`` — §D-S7-default: on by default). ``False`` is
                  byte-identical to not registering the middleware.
        preset:   ``BALANCED`` (default, four headers) or ``STRICT``
                  (additionally CSP/COOP/CORP/Permissions-Policy).
        x_content_type_options: Value for ``X-Content-Type-Options``, or
                  ``None`` to omit it. Default ``"nosniff"``.
        x_frame_options: Value for ``X-Frame-Options``, or ``None`` to
                  omit it. Default ``"DENY"`` — the single most likely
                  breakage on upgrade (an app that frames its own HTML);
                  set to ``"SAMEORIGIN"`` to allow same-origin framing.
        referrer_policy: Value for ``Referrer-Policy``, or ``None`` to
                  omit it. Default ``"strict-origin-when-cross-origin"``.
        hsts_max_age: ``max-age`` seconds for ``Strict-Transport-Security``.
                  Default ``31536000`` (1 year, OWASP's recommendation).
                  ``0`` disables HSTS entirely (reported as
                  ``http.security_headers.hsts_absent`` by
                  ``inspect_http_edge()``).
        hsts_include_subdomains: Append ``; includeSubDomains``. Default
                  ``True``.
        csp: ``Content-Security-Policy`` value sent only under ``STRICT``.
                  Default ``"default-src 'none'; frame-ancestors 'none'"``.
        permissions_policy: ``Permissions-Policy`` value sent only under
                  ``STRICT``. Default
                  ``"geolocation=(), microphone=(), camera=()"``.
        exclude_paths: Request paths exempt from **every** header this
                  middleware would send — used under ``STRICT`` to keep
                  ``/docs``/``/redoc``/``/openapi.json`` usable despite the
                  CSP. Default covers exactly those three.
        trusted_proxies: CIDRs whose ``X-Forwarded-Proto`` is trusted for
                  the HSTS scheme check (§D-S10-ip's trust rule, reused).
                  Default empty — the header is ignored with no
                  configuration.

    Thread safety:  ✅ Frozen pydantic model.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_SECURITY_HEADERS_", frozen=True)

    enabled: bool = True
    preset: SecurityHeadersPreset = SecurityHeadersPreset.BALANCED
    x_content_type_options: str | None = "nosniff"
    x_frame_options: str | None = "DENY"
    referrer_policy: str | None = "strict-origin-when-cross-origin"
    hsts_max_age: int = 31536000
    hsts_include_subdomains: bool = True
    csp: str | None = "default-src 'none'; frame-ancestors 'none'"
    permissions_policy: str | None = "geolocation=(), microphone=(), camera=()"
    exclude_paths: tuple[str, ...] = ("/docs", "/redoc", "/openapi.json")
    trusted_proxies: tuple[str, ...] = ()

    def hsts_value(self) -> str | None:
        """Render the HSTS header value, or ``None`` when ``hsts_max_age == 0``."""
        if self.hsts_max_age <= 0:
            return None
        value = f"max-age={self.hsts_max_age}"
        if self.hsts_include_subdomains:
            value += "; includeSubDomains"
        return value


class SecurityHeadersMiddleware:
    """
    Pure-ASGI middleware that stamps a baseline of security headers on
    every HTTP response.

    Args:
        app:      The wrapped ASGI application.
        settings: ``SecurityHeadersSettings`` instance. Defaults to reading
                  from the environment (``SecurityHeadersSettings()``).

    Edge cases:
        - A non-``http`` scope (``lifespan``, ``websocket``) passes through
          untouched.
        - ``enabled=False`` short-circuits before any header computation —
          byte-identical to the middleware not being registered at all.
        - A preflight ``OPTIONS`` handled by ``CORSMiddleware`` never
          reaches this middleware when it sits inside CORS (§D-order) —
          documented, not enforced here.
        - ``scope["path"]`` matching ``exclude_paths`` (prefix match) skips
          **every** header, not just CSP — simplest contract, and the only
          real case (``/docs``/``/redoc``) wants no headers touched at all.

    Thread safety:  ✅ Stateless — settings are read-only after construction.
    Async safety:   ✅ ``__call__`` is ``async def``; no shared mutable state.
    """

    def __init__(self, app: Any, *, settings: SecurityHeadersSettings | None = None) -> None:
        self.app = app
        self._settings = settings or SecurityHeadersSettings()

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http" or not self._settings.enabled:
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if any(path.startswith(prefix) for prefix in self._settings.exclude_paths):
            await self.app(scope, receive, send)
            return

        headers_to_set = self._headers_for(scope)

        async def send_wrapper(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start" and headers_to_set:
                mutable_headers = MutableHeaders(raw=message["headers"])
                for name, value in headers_to_set:
                    # setdefault semantics — a route/downstream value wins.
                    mutable_headers.setdefault(name, value)
            await send(message)

        await self.app(scope, receive, send_wrapper)

    def _headers_for(self, scope: dict[str, Any]) -> list[tuple[str, str]]:
        """Build the (name, value) pairs to send for this request's scheme/preset."""
        settings = self._settings
        headers: list[tuple[str, str]] = []

        if settings.x_content_type_options is not None:
            headers.append(("X-Content-Type-Options", settings.x_content_type_options))
        if settings.x_frame_options is not None:
            headers.append(("X-Frame-Options", settings.x_frame_options))
        if settings.referrer_policy is not None:
            headers.append(("Referrer-Policy", settings.referrer_policy))

        if self._is_https(scope):
            hsts = settings.hsts_value()
            if hsts is not None:
                headers.append(("Strict-Transport-Security", hsts))

        if settings.preset is SecurityHeadersPreset.STRICT:
            if settings.csp is not None:
                headers.append(("Content-Security-Policy", settings.csp))
            headers.append(("Cross-Origin-Opener-Policy", "same-origin"))
            headers.append(("Cross-Origin-Resource-Policy", "same-origin"))
            if settings.permissions_policy is not None:
                headers.append(("Permissions-Policy", settings.permissions_policy))

        return headers

    def _is_https(self, scope: dict[str, Any]) -> bool:
        """
        Whether the effective scheme is HTTPS — direct or via a trusted proxy.

        §D-S10-ip's trust rule, reused: ``X-Forwarded-Proto`` is consulted
        only when the immediate peer (``scope["client"]``) matches
        ``trusted_proxies``. With no configuration, the header is ignored
        entirely.
        """
        if scope.get("scheme") == "https":
            return True

        client = scope.get("client")
        client_host = client[0] if client else None
        if not peer_is_trusted_proxy(client_host, self._settings.trusted_proxies):
            return False

        headers = dict(scope.get("headers") or [])
        raw_proto = headers.get(b"x-forwarded-proto")
        if raw_proto is None:
            return False
        return resolve_forwarded_proto(raw_proto.decode("latin-1")) == "https"
