"""
varco_fastapi.middleware.localization
========================================
``LocalizationMiddleware`` — RD-3's **one** middleware resolving locale
(I2) and/or timezone (T1) in a single ASGI pass, with **two independent
toggles**.

I2 and T1 both need "read the request, resolve a value, put it in
``RequestContext``, unset it after". Two middlewares means two ``ContextVar``
tokens whose nesting must be right, and two places for the same
``?lang=``-vs-header precedence bug. This middleware sets **one** merged
``RequestContext`` token and resets it in ``finally``.

**Ordering hazard (RD-3), and why the ``request.state`` mirror exists.**
``ErrorMiddleware`` sits *outside* this middleware. When a handler raises,
the exception propagates out through this middleware's ``finally`` — which
resets the ``ContextVar`` token — before ``ErrorMiddleware`` ever formats
the response body. A ``ContextVar`` set by an inner middleware is only
visible to an outer one while the inner one has not yet reset it, so by the
time ``ErrorMiddleware`` runs, ``current_request_context()`` is already
empty. This middleware therefore **also** stashes the resolved
``RequestContext`` on ``request.state.varco_request_context`` — callers
that need the resolved context for error rendering read ``request.state``
first, ``current_request_context()`` second.

Both built-in error-rendering paths — ``varco_fastapi.exceptions.
add_exception_handlers()`` and ``varco_fastapi.middleware.error.
ErrorMiddleware`` — read this mirror and, when given a ``message_catalog=``
(threaded from ``LocalizationModule(i18n=...)``'s resolved
``MessageCatalog``), pass ``message_resolver=catalog.format_message`` into
``error_message_for()`` and set the ``Content-Language`` header themselves.
This was **not** true before Plan 011's drift-fix pass — an earlier
version of this docstring claimed it was already discharged, which a direct
grep of ``varco_fastapi/exceptions.py``/``middleware/error.py`` showed was
false (neither string, ``varco_request_context`` nor ``message_resolver``,
appeared in either module). With no ``message_catalog=`` supplied (the
default), both paths are byte-identical to pre-fix behaviour: the body
still carries the untranslated ``default_message``.

It must be inserted **after** ``TenantResolutionMiddleware`` in request
order (i.e. registered — via ``add_middleware`` — *before*
``TenantResolutionMiddleware``, since Starlette's last-added middleware is
outermost/runs-first) so the tenant-default precedence step sees
``current_tenant()`` already populated.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp
from varco_core.context.defaults import NullTenantDefaults, TenantDefaultsProvider
from varco_core.context.request import request_context
from varco_core.i18n.resolve import resolve_locale
from varco_core.i18n.settings import I18nSettings
from varco_core.service.tenant import current_tenant
from varco_core.tz.resolve import resolve_timezone
from varco_core.tz.settings import TimezoneSettings

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

__all__ = ["LocalizationMiddleware"]


class LocalizationMiddleware(BaseHTTPMiddleware):
    """
    Resolves locale and/or timezone into one merged ``RequestContext``.

    Args:
        app: The ASGI application to wrap.
        i18n_settings: Controls the locale half. ``enabled=False`` (the
            class default) means locale is never resolved.
        timezone_settings: Controls the timezone half. Same default-off
            rule.
        tenant_defaults_provider: RD-2's per-tenant defaults lookup —
            ``NullTenantDefaults()`` (zero I/O) unless supplied.

    Edge cases:
        - Both settings disabled -> this middleware should not even be
          added to the stack (``create_varco_app`` enforces this); if it
          *is* added with both disabled, it is a no-op pass-through.
        - ``Content-Language`` is set on every response when I2 is enabled,
          including when the resolved locale is the fallback (brief 003).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        i18n_settings: I18nSettings | None = None,
        timezone_settings: TimezoneSettings | None = None,
        tenant_defaults_provider: TenantDefaultsProvider | None = None,
    ) -> None:
        super().__init__(app)
        self._i18n_settings = i18n_settings or I18nSettings()
        self._timezone_settings = timezone_settings or TimezoneSettings()
        self._tenant_defaults_provider: TenantDefaultsProvider = (
            tenant_defaults_provider or NullTenantDefaults()
        )

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        locale: str | None = None
        zone: ZoneInfo | None = None
        tenant_id = current_tenant()

        if self._i18n_settings.enabled:
            settings = self._i18n_settings
            query_param = request.query_params.get(settings.query_param)
            user_profile_locale = _auth_ctx_claim(request, "locale")
            resolved = await resolve_locale(
                query_param=query_param,
                user_profile_locale=user_profile_locale,
                tenant_id=tenant_id,
                tenant_defaults_provider=self._tenant_defaults_provider,
                accept_language_header=request.headers.get("accept-language"),
                supported_locales=settings.supported_locales,
                default_locale=settings.default_locale,
            )
            locale = resolved.value if resolved is not None else settings.default_locale

        if self._timezone_settings.enabled:
            tz_settings = self._timezone_settings
            query_param = request.query_params.get(tz_settings.query_param)
            header = request.headers.get(tz_settings.header)
            user_profile_zoneinfo = _auth_ctx_claim(request, "zoneinfo")
            resolved_tz = await resolve_timezone(
                query_param=query_param,
                header=header,
                user_profile_zoneinfo=user_profile_zoneinfo,
                tenant_id=tenant_id,
                tenant_defaults_provider=self._tenant_defaults_provider,
                default_timezone=tz_settings.default_timezone,
            )
            zone = resolved_tz.value if resolved_tz is not None else None

        with request_context(locale=locale, timezone=zone) as ctx:
            # RD-3's request.state mirror — see the module docstring.
            request.state.varco_request_context = ctx
            response = await call_next(request)

        if self._i18n_settings.enabled and self._i18n_settings.set_content_language and locale:
            response.headers["Content-Language"] = locale

        return response


def _auth_ctx_claim(request: Request, claim: str) -> str | None:
    """Best-effort read of an ``AuthContext``-shaped claim from
    ``request.state.auth_context`` / ``request.state.ctx``, if present."""
    for attr in ("auth_context", "ctx"):
        auth_ctx: Any = getattr(request.state, attr, None)
        if auth_ctx is None:
            continue
        extra_claims = getattr(auth_ctx, "extra_claims", None)
        if extra_claims and claim in extra_claims:
            claim_value: str = extra_claims[claim]
            return claim_value
        direct = getattr(auth_ctx, claim, None)
        if isinstance(direct, str):
            return direct
    return None
