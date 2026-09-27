"""
app.py
======
Application factory for the ``24-custom-route-params`` example.

A **service-free** FastAPI app showing that custom ``@route`` handlers accept the
full FastAPI parameter surface — ``Query``/``Body``/``Depends``/``Request`` plus
type-coerced path params — while ``ctx`` injection and ``RouteGuard`` still work.

Auth is intentionally trivial: an ``ApiKeyAuth`` mapping two ``X-API-Key`` values
to pre-built ``AuthContext``s.  No JWT/JWK ceremony — the point is the parameters,
not the auth strategy.

Run locally::

    cd examples/24-custom-route-params
    uv run uvicorn app:app --reload

Try it::

    curl localhost:8000/catalog/health
    curl -H 'X-API-Key: reader-key' localhost:8000/catalog/items/42?currency=eur
    curl -H 'X-API-Key: reader-key' localhost:8000/catalog/reports/summary?window=7
    curl -X POST -H 'X-API-Key: reader-key' -H 'content-type: application/json' \
         -d '{"name":"widget","price_cents":500}' localhost:8000/catalog/items

Thread safety:  ✅ ``create_app()`` intended to be called once per process.
Async safety:   ✅ Synchronous factory; no event loop required at call time.
"""

from __future__ import annotations

from fastapi import FastAPI
from providify import DIContainer
from router import CatalogRouter
from varco_core.auth.base import AuthContext
from varco_fastapi.app import create_varco_app
from varco_fastapi.auth import ApiKeyAuth

# API keys → AuthContext.  The reader holds the "catalog:read" scope required by
# the guarded /reports/summary route; the guest does not.
_READER = AuthContext(user_id="reader", scopes=frozenset({"catalog:read"}))
_GUEST = AuthContext(user_id="guest", scopes=frozenset())


def create_app() -> FastAPI:
    """
    Build and return the configured FastAPI application.

    Assigns ``CatalogRouter._auth`` before ``create_varco_app`` calls
    ``build_router()`` (which reads ``_auth`` at call time), so ``ctx`` and the
    ``require_scopes`` guard have an ``AuthContext`` available.

    Returns:
        A configured ``FastAPI`` app ready for an ASGI server.

    Edge cases:
        - ``required=False`` lets anonymous callers reach ``allow_anonymous``
          routes (``GET /catalog/health``) without an immediate 401.
        - Each call produces an independent ``FastAPI`` instance — safe for tests.
    """
    # required=False → anonymous callers fall through to an anonymous AuthContext
    # instead of being rejected at the middleware, so /catalog/health stays open.
    server_auth = ApiKeyAuth(
        keys={"reader-key": _READER, "guest-key": _GUEST},
        required=False,
    )
    # build_router() reads _auth at call time — set it before create_varco_app.
    CatalogRouter._auth = server_auth  # type: ignore[attr-defined]

    return create_varco_app(
        DIContainer(),
        routers=[CatalogRouter],
        title="Custom Route Params Example",
        version="0.1.0",
        description="Full FastAPI parameter injection on custom @route handlers.",
        validate=False,
    )


# Module-level app so ``uvicorn app:app`` works without ``--factory``.
app = create_app()
