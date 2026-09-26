"""
varco_fastapi.middleware.cors
=============================
CORS configuration and middleware helper.

``CORSConfig`` is a frozen dataclass that captures CORS policy.
``install_cors(app, config)`` adds Starlette's ``CORSMiddleware`` to a
``FastAPI`` app with the resolved configuration.

Env vars (read by ``CORSConfig.from_env()``)::

    VARCO_CORS_ORIGINS     — comma-separated allowed origins (e.g. "https://app.example.com,http://localhost:3000")
    VARCO_CORS_METHODS     — comma-separated methods (default: GET,POST,PUT,PATCH,DELETE,OPTIONS)
    VARCO_CORS_HEADERS     — comma-separated allowed headers (default: see below)
    VARCO_CORS_CREDENTIALS — "true" / "false" (default: "true")
    VARCO_CORS_MAX_AGE     — preflight cache in seconds (default: 600)

DESIGN: frozen config over direct middleware kwargs
    ✅ Testable — CORSConfig is a plain data object with no side effects
    ✅ from_env() + from_dict() cover all use cases
    ✅ Reusable across tests/dev/prod without code changes
    ❌ One extra indirection vs. passing kwargs directly to CORSMiddleware

Thread safety:  ✅ frozen=True — immutable after construction.
Async safety:   ✅ ``install_cors`` is synchronous (called at startup).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import AliasChoices, Field
from pydantic_settings import SettingsConfigDict
from varco_core.config import VarcoSettings
from varco_core.settings.types import CommaSeparated

if TYPE_CHECKING:
    from fastapi import FastAPI


class CORSConfig(VarcoSettings):
    """
    CORS policy configuration.

    **BREAKING in 3.0.0 (Plan 022 / AB-5): ``allow_origins`` now defaults to
    ``()`` — no cross-origin request is allowed until you list origins.**
    It previously defaulted to ``("*",)``, which combined with the
    (unchanged) ``allow_credentials=True`` default and ``create_varco_app()``'s
    unconditional ``install_cors()`` call meant every app that set no
    ``VARCO_CORS_*`` env var shipped a reflect-any-origin-with-credentials
    policy.  See the Edge cases for exactly what that did.  Opting back in is
    still supported and unchanged — pass ``allow_origins=("*",)`` explicitly,
    which is now a visible decision rather than a silent default.

    Attributes:
        allow_origins:      Allowed origin strings.  Defaults to ``()`` — no
                            cross-origin access.  ``("*",)`` means "any
                            origin"; see the Edge cases below for what that
                            actually does when combined with credentials.
        allow_methods:      Allowed HTTP methods.
        allow_headers:      Allowed request headers.
        allow_credentials:  If ``True``, allow cookies and auth headers.
        max_age:            Preflight response cache in seconds.

    Edge cases:
        - ⚠️ ``allow_origins=("*",)`` with ``allow_credentials=True`` — no
          longer the default, but still reachable if you set it — does
          **not** raise, and is **not** blocked by the browser.  Starlette's
          ``CORSMiddleware`` sets ``preflight_explicit_allow_origin = not
          allow_all_origins or allow_credentials``, so with credentials on it
          stops sending the literal ``*`` and instead **reflects the request's
          own ``Origin`` header** alongside ``access-control-allow-credentials:
          true`` and ``vary: Origin``.  That combination is valid to a browser,
          so any site on the internet can make credentialed cross-origin reads.
          Verified against Starlette 1.0.0: ``install_cors(app, CORSConfig())``
          answers a request carrying ``Origin: https://evil.example.com`` with
          ``access-control-allow-origin: https://evil.example.com`` and
          ``access-control-allow-credentials: true``.
          (An earlier version of this docstring claimed the combination was
          "invalid per the CORS spec" and that "Starlette will raise at app
          startup".  Both claims were false — Plan 022 / Phase 0.  The
          browser-level prohibition applies only to the literal ``*`` value,
          which Starlette deliberately avoids emitting here.)
          Never pair the two.  List origins explicitly instead.
        - An empty ``allow_origins`` tuple blocks all cross-origin requests.
          This is the default as of 3.0.0, and is what
          ``CORSConfig.restrictive()`` has always returned — the two are now
          equivalent for ``allow_origins``. ``restrictive()`` is kept as the
          explicit, self-documenting spelling.

    **Plan 048 / DI-6: converted from a frozen dataclass to a
    ``VarcoSettings``** — same class name, same field names, every default
    literal byte-identical; ``from_env()``/``from_dict()`` are now the
    inherited ``VarcoSettings`` versions (equivalent to ``cls()``/
    ``cls.model_validate(d)``). Positional construction is gone — it was
    never used in this codebase's own call sites (all keyword), but a
    downstream ``CORSConfig("a")`` needs ``CORSConfig(allow_origins=("a",))``.

    Thread safety:  ✅ frozen=True.
    Async safety:   ✅ Pure value object; no I/O.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_CORS_", frozen=True, populate_by_name=True)

    allow_origins: CommaSeparated = Field(
        default=(), validation_alias=AliasChoices("VARCO_CORS_ORIGINS")
    )
    allow_methods: CommaSeparated = Field(
        default=("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"),
        validation_alias=AliasChoices("VARCO_CORS_METHODS"),
    )
    allow_headers: CommaSeparated = Field(
        default=(
            "Authorization",
            "Content-Type",
            "X-Request-ID",
            "X-Correlation-ID",
            "X-API-Key",
        ),
        validation_alias=AliasChoices("VARCO_CORS_HEADERS"),
    )
    allow_credentials: bool = Field(
        default=True, validation_alias=AliasChoices("VARCO_CORS_CREDENTIALS")
    )
    max_age: int = 600

    allow_origin_regex: str | None = None

    allow_private_network: bool = False

    expose_headers: CommaSeparated = ("X-Total-Count", "X-Page-Size", "Link")

    # ── Factory methods ────────────────────────────────────────────────────────

    @classmethod
    def restrictive(cls) -> CORSConfig:
        """
        Return a restrictive ``CORSConfig`` with no allowed origins.

        Origins must be explicitly listed via ``from_env()`` or the constructor.
        Use this as a safe default in production and add origins explicitly.

        Returns:
            A ``CORSConfig`` with ``allow_origins=()``.
        """
        return cls(allow_origins=())


def install_cors(app: FastAPI, config: CORSConfig | None = None) -> None:
    """
    Add Starlette ``CORSMiddleware`` to ``app`` using ``CORSConfig``.

    Args:
        app:    The ``FastAPI`` application to add middleware to.
        config: CORS configuration.  If ``None``, reads from environment
                via ``CORSConfig.from_env()``.

    Edge cases:
        - Must be called BEFORE the app starts handling requests
          (i.e. at startup, not inside a route handler).
        - Adding ``CORSMiddleware`` more than once wraps it twice — call
          ``install_cors`` exactly once.
    """
    from starlette.middleware.cors import CORSMiddleware

    cfg = config or CORSConfig.from_env()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cfg.allow_origins),
        allow_methods=list(cfg.allow_methods),
        allow_headers=list(cfg.allow_headers),
        allow_credentials=cfg.allow_credentials,
        expose_headers=cfg.expose_headers,
        allow_private_network=cfg.allow_private_network,
        allow_origin_regex=cfg.allow_origin_regex,
        max_age=cfg.max_age,
    )


__all__ = ["CORSConfig", "install_cors"]
