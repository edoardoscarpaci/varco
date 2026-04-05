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

import os
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CORSConfig:
    """
    CORS policy configuration.

    Attributes:
        allow_origins:      Allowed origin strings.  Use ``("*",)`` for
                            unrestricted access (dev only — never production
                            when ``allow_credentials=True``).
        allow_methods:      Allowed HTTP methods.
        allow_headers:      Allowed request headers.
        allow_credentials:  If ``True``, allow cookies and auth headers.
                            NOTE: Incompatible with ``allow_origins=("*",)``
                            per the CORS spec — browsers will block it.
        max_age:            Preflight response cache in seconds.

    Edge cases:
        - ``allow_origins=("*",)`` with ``allow_credentials=True`` is invalid
          per the CORS spec.  Starlette will raise at app startup.  Use
          ``CORSConfig.restrictive()`` and list origins explicitly.
        - An empty ``allow_origins`` tuple blocks all cross-origin requests.

    Thread safety:  ✅ frozen=True.
    Async safety:   ✅ Pure value object; no I/O.
    """

    allow_origins: tuple[str, ...] = ("*",)
    allow_methods: tuple[str, ...] = (
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS",
    )
    allow_headers: tuple[str, ...] = (
        "Authorization",
        "Content-Type",
        "X-Request-ID",
        "X-Correlation-ID",
        "X-API-Key",
    )
    allow_credentials: bool = True
    max_age: int = 600

    # ── Factory methods ────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> CORSConfig:
        """
        Build a ``CORSConfig`` from standard environment variables.

        Reads ``VARCO_CORS_*`` env vars.  Unset vars fall back to the
        dataclass defaults.

        Returns:
            A fully populated ``CORSConfig``.
        """

        def _split(value: str | None, default: tuple[str, ...]) -> tuple[str, ...]:
            if not value:
                return default
            return tuple(v.strip() for v in value.split(",") if v.strip())

        allow_origins = _split(
            os.environ.get("VARCO_CORS_ORIGINS"),
            ("*",),
        )
        allow_methods = _split(
            os.environ.get("VARCO_CORS_METHODS"),
            ("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"),
        )
        allow_headers = _split(
            os.environ.get("VARCO_CORS_HEADERS"),
            (
                "Authorization",
                "Content-Type",
                "X-Request-ID",
                "X-Correlation-ID",
                "X-API-Key",
            ),
        )
        credentials_str = os.environ.get("VARCO_CORS_CREDENTIALS", "true").lower()
        allow_credentials = credentials_str not in ("false", "0", "no")

        max_age_str = os.environ.get("VARCO_CORS_MAX_AGE", "600")
        try:
            max_age = int(max_age_str)
        except ValueError:
            max_age = 600

        return cls(
            allow_origins=allow_origins,
            allow_methods=allow_methods,
            allow_headers=allow_headers,
            allow_credentials=allow_credentials,
            max_age=max_age,
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> CORSConfig:
        """
        Build a ``CORSConfig`` from a dict.

        Args:
            d: Dict with optional keys matching the dataclass field names.
               Tuple-typed fields also accept lists.

        Returns:
            A ``CORSConfig`` with values from the dict merged over defaults.
        """
        return cls(
            allow_origins=tuple(d.get("allow_origins", ("*",))),
            allow_methods=tuple(
                d.get(
                    "allow_methods",
                    ("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"),
                )
            ),
            allow_headers=tuple(
                d.get(
                    "allow_headers",
                    (
                        "Authorization",
                        "Content-Type",
                        "X-Request-ID",
                        "X-Correlation-ID",
                        "X-API-Key",
                    ),
                )
            ),
            allow_credentials=bool(d.get("allow_credentials", True)),
            max_age=int(d.get("max_age", 600)),
        )

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


def install_cors(app, config: CORSConfig | None = None) -> None:
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
        max_age=cfg.max_age,
    )


__all__ = ["CORSConfig", "install_cors"]
