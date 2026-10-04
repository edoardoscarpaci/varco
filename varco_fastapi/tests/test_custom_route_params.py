"""
Tests for full FastAPI parameter injection on custom ``@route`` handlers.

Historically a custom ``@route`` method could only receive ``ctx``/``auth``/
``context`` (the ``AuthContext``) plus raw-string path params — declaring any
``Query``/``Body``/``Depends``/``Request`` param produced a 500.  The handler is
now registered with a synthesized ``__signature__`` mirroring the user's method,
so FastAPI parses and injects everything natively.

Covers:
- Query params (validation + coercion, 422 on invalid).
- Request body via a Pydantic model (422 on bad payload).
- Arbitrary ``Depends(...)`` dependencies.
- Type-coerced path params (``int`` in ``/{id}`` arrives as ``int``, not ``str``).
- ``Request`` injection into the handler.
- Backward-compat: ``ctx`` still injected from ``_auth``; ``RouteGuard`` still runs.
- Async offload (``?with_async=true``) still works with the new signature.
- Return annotation drives the OpenAPI schema (response model surfaces).
- Unit test for the signature-synthesis helper.
"""

from __future__ import annotations

from fastapi import Body, Depends, Query, Request
from fastapi.testclient import TestClient
from providify import DIContainer
from pydantic import BaseModel
from varco_core.auth.api_key import hash_api_key
from varco_core.auth.base import AuthContext
from varco_fastapi.app import create_varco_app
from varco_fastapi.auth import AnonymousAuth, ApiKeyAuth
from varco_fastapi.auth.guard import require_scopes
from varco_fastapi.router.base import _synthesize_custom_signature
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter

# ── Fixtures ──────────────────────────────────────────────────────────────────


class EchoBody(BaseModel):
    """Simple request-body model used by the Body() tests."""

    message: str
    times: int = 1


class SummaryResponse(BaseModel):
    """Response model used to assert OpenAPI schema generation."""

    total: int


def _dependency_value() -> str:
    """A trivial FastAPI dependency — returns a constant to prove injection works."""
    return "injected"


# ── Query params ──────────────────────────────────────────────────────────────


def test_query_param_is_parsed_and_coerced():
    """A ``Query(...)`` param is coerced to its annotated type and validated."""

    class R(GenericRouter):
        _prefix = "/q"

        @route("GET", "/window")
        async def window(self, days: int = Query(30, ge=1, le=365)) -> dict:
            # If FastAPI parsed correctly, ``days`` is an int, not the raw string.
            return {"days": days, "is_int": isinstance(days, int)}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.get("/q/window", params={"days": "7"})
    assert resp.status_code == 200
    assert resp.json() == {"days": 7, "is_int": True}


def test_query_param_default_applies():
    """Omitting an optional query param uses its declared default."""

    class R(GenericRouter):
        _prefix = "/q"

        @route("GET", "/window")
        async def window(self, days: int = Query(30, ge=1)) -> dict:
            return {"days": days}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.get("/q/window")
    assert resp.status_code == 200
    assert resp.json() == {"days": 30}


def test_query_param_validation_rejects_invalid():
    """An out-of-range query value is rejected by FastAPI with 422."""

    class R(GenericRouter):
        _prefix = "/q"

        @route("GET", "/window")
        async def window(self, days: int = Query(30, ge=1)) -> dict:
            return {"days": days}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.get("/q/window", params={"days": "0"})  # ge=1 violated
    assert resp.status_code == 422


# ── Body (Pydantic model) ─────────────────────────────────────────────────────


def test_body_model_is_parsed():
    """A Pydantic-model body param is parsed and validated."""

    class R(GenericRouter):
        _prefix = "/b"

        @route("POST", "/echo")
        async def echo(self, payload: EchoBody = Body(...)) -> dict:
            return {"echo": payload.message * payload.times}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.post("/b/echo", json={"message": "hi", "times": 3})
    assert resp.status_code == 200
    assert resp.json() == {"echo": "hihihi"}


def test_body_validation_rejects_bad_payload():
    """A body missing a required field is rejected with 422."""

    class R(GenericRouter):
        _prefix = "/b"

        @route("POST", "/echo")
        async def echo(self, payload: EchoBody = Body(...)) -> dict:
            return {"echo": payload.message}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.post("/b/echo", json={"times": 2})  # missing "message"
    assert resp.status_code == 422


# ── Depends ───────────────────────────────────────────────────────────────────


def test_depends_dependency_is_injected():
    """An arbitrary ``Depends(...)`` is resolved and injected."""

    class R(GenericRouter):
        _prefix = "/d"

        @route("GET", "/thing")
        async def thing(self, dep: str = Depends(_dependency_value)) -> dict:
            return {"dep": dep}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.get("/d/thing")
    assert resp.status_code == 200
    assert resp.json() == {"dep": "injected"}


# ── Typed path params ─────────────────────────────────────────────────────────


def test_path_param_is_type_coerced():
    """A typed path param arrives coerced to its annotation (was raw string before)."""

    class R(GenericRouter):
        _prefix = "/p"

        @route("GET", "/item/{item_id}")
        async def item(self, item_id: int) -> dict:
            return {"item_id": item_id, "is_int": isinstance(item_id, int)}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.get("/p/item/42")
    assert resp.status_code == 200
    assert resp.json() == {"item_id": 42, "is_int": True}


def test_path_param_invalid_type_rejected():
    """A non-int path segment on an ``int`` param is rejected with 422."""

    class R(GenericRouter):
        _prefix = "/p"

        @route("GET", "/item/{item_id}")
        async def item(self, item_id: int) -> dict:
            return {"item_id": item_id}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.get("/p/item/not-a-number")
    assert resp.status_code == 422


# ── Request injection ─────────────────────────────────────────────────────────


def test_request_is_injected():
    """A handler declaring ``request: Request`` receives the real request object."""

    class R(GenericRouter):
        _prefix = "/r"

        @route("GET", "/echo-header")
        async def echo_header(self, request: Request) -> dict:
            return {"ua": request.headers.get("user-agent", "")}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.get("/r/echo-header", headers={"User-Agent": "varco-test"})
    assert resp.status_code == 200
    assert resp.json() == {"ua": "varco-test"}


# ── Combined: everything at once ──────────────────────────────────────────────


def test_all_param_kinds_together():
    """
    A single handler mixing a typed path param, ctx, a Query, a Body and a Depends
    resolves every argument correctly.
    """
    ctx_admin = AuthContext(user_id="adm", scopes=frozenset({"reports:read"}))
    auth = ApiKeyAuth(hashed_keys={hash_api_key("admin-key"): ctx_admin}, required=False)

    class R(GenericRouter):
        _prefix = "/reports"
        _auth = auth

        @route("POST", "/{report_id}/summary", requires=require_scopes("reports:read"))
        async def summary(
            self,
            report_id: int,
            ctx: AuthContext,
            payload: EchoBody = Body(...),
            window: int = Query(30, ge=1),
            dep: str = Depends(_dependency_value),
        ) -> dict:
            return {
                "report_id": report_id,
                "user": ctx.user_id,
                "message": payload.message,
                "window": window,
                "dep": dep,
            }

    client = TestClient(
        create_varco_app(DIContainer(), routers=[R], validate=False), raise_server_exceptions=False
    )
    resp = client.post(
        "/reports/7/summary",
        params={"window": "12"},
        json={"message": "hello"},
        headers={"X-API-Key": "admin-key"},
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "report_id": 7,
        "user": "adm",
        "message": "hello",
        "window": 12,
        "dep": "injected",
    }


def test_combined_handler_still_enforces_guard():
    """The RouteGuard still denies an under-privileged caller on a rich handler."""
    ctx_unpriv = AuthContext(user_id="u", scopes=frozenset())
    auth = ApiKeyAuth(hashed_keys={hash_api_key("unpriv-key"): ctx_unpriv}, required=False)

    class R(GenericRouter):
        _prefix = "/reports"
        _auth = auth

        @route("POST", "/{report_id}/summary", requires=require_scopes("reports:read"))
        async def summary(
            self,
            report_id: int,
            ctx: AuthContext,
            window: int = Query(30, ge=1),
        ) -> dict:
            return {"ok": True}

    client = TestClient(
        create_varco_app(DIContainer(), routers=[R], validate=False), raise_server_exceptions=False
    )
    resp = client.post(
        "/reports/7/summary",
        params={"window": "12"},
        headers={"X-API-Key": "unpriv-key"},
    )
    assert resp.status_code == 403


# ── Backward compatibility ────────────────────────────────────────────────────


def test_ctx_only_handler_still_works():
    """The historical ``ctx``-only signature still receives the AuthContext."""

    class R(GenericRouter):
        _prefix = "/legacy"
        _auth = AnonymousAuth()

        @route("GET", "/whoami")
        async def whoami(self, ctx: AuthContext) -> dict:
            return {"anonymous": ctx.user_id is None}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.get("/legacy/whoami")
    assert resp.status_code == 200
    assert resp.json() == {"anonymous": True}


def test_no_param_handler_still_works():
    """A handler with no params (only ``self``) still serves requests."""

    class R(GenericRouter):
        _prefix = "/legacy"

        @route("GET", "/ping")
        async def ping(self) -> dict:
            return {"pong": True}

    client = TestClient(create_varco_app(DIContainer(), routers=[R], validate=False))
    resp = client.get("/legacy/ping")
    assert resp.status_code == 200
    assert resp.json() == {"pong": True}


# ── OpenAPI schema ────────────────────────────────────────────────────────────


def test_return_annotation_drives_openapi_schema():
    """The handler's return annotation surfaces as the response model in OpenAPI."""

    class R(GenericRouter):
        _prefix = "/schema"

        @route("GET", "/summary")
        async def summary(self, n: int = Query(1)) -> SummaryResponse:
            return SummaryResponse(total=n)

    app = create_varco_app(DIContainer(), routers=[R], validate=False)
    client = TestClient(app)

    # Runtime: response is serialized through the model.
    resp = client.get("/schema/summary", params={"n": "5"})
    assert resp.status_code == 200
    assert resp.json() == {"total": 5}

    # Schema: the query param and the response model both appear in OpenAPI.
    schema = client.get("/openapi.json").json()
    op = schema["paths"]["/schema/summary"]["get"]
    param_names = {p["name"] for p in op.get("parameters", [])}
    assert "n" in param_names
    assert "SummaryResponse" in schema["components"]["schemas"]


# ── Async offload still works ─────────────────────────────────────────────────


async def test_async_offload_still_offloads_with_new_signature():
    """
    An ``async_capable`` route with ``?with_async=true`` offloads to the job runner
    (body carries a ``job_id``) while inline calls still parse the Query param and
    return the computed result — the new synthesized signature preserves offload.
    """
    from fastapi import FastAPI
    from varco_fastapi.job.runner import JobRunner
    from varco_fastapi.job.store import InMemoryJobStore

    runner = JobRunner(store=InMemoryJobStore())
    await runner.start()

    class R(GenericRouter):
        _prefix = "/jobs"
        _job_runner = runner

        @route("POST", "/run", async_capable=True)
        async def run(self, factor: int = Query(2)) -> dict:
            return {"result": factor * 10}

    app = FastAPI()
    app.include_router(R().build_router())
    client = TestClient(app, raise_server_exceptions=False)

    # Inline (no with_async) → Query param parsed, computed result returned.
    inline = client.post("/jobs/run", params={"factor": "3"})
    assert inline.status_code == 200
    assert inline.json() == {"result": 30}

    # Offloaded → job accepted, body carries a job id.
    offloaded = client.post("/jobs/run", params={"factor": "3", "with_async": "true"})
    assert offloaded.status_code == 200
    assert "job_id" in offloaded.json()

    await runner.stop()


# ── Signature-synthesis helper (unit) ─────────────────────────────────────────


def test_synthesize_signature_maps_auth_and_passthrough():
    """
    ``_synthesize_custom_signature`` maps a ctx param to the auth kwarg and passes
    other params through, resolving string annotations to real types.
    """
    auth = AnonymousAuth()

    class _Holder:
        @route("GET", "/x/{item_id}")
        async def handler(
            self,
            item_id: int,
            ctx: AuthContext,
            limit: int = Query(10),
        ) -> dict:
            return {}

    spec = _synthesize_custom_signature(_Holder.handler, auth, can_offload=False)
    params = spec.signature.parameters

    # self is dropped; the three declared params are present, plus no hidden request.
    assert "self" not in params
    assert set(params) == {"item_id", "ctx", "limit"}
    # ctx becomes the auth kwarg and flows through (not hidden).
    assert spec.auth_kwarg == "ctx"
    assert "ctx" not in spec.hidden_kwargs
    # String annotations resolved to real runtime types.
    assert params["item_id"].annotation is int
    assert params["ctx"].annotation is AuthContext


def test_synthesize_signature_injects_hidden_auth_when_ctx_absent():
    """
    With ``_auth`` set but no ctx param, a hidden auth param is added so the guard /
    offload snapshot still get an AuthContext — and it is marked hidden.
    """
    auth = AnonymousAuth()

    class _Holder:
        @route("GET", "/y")
        async def handler(self) -> dict:
            return {}

    spec = _synthesize_custom_signature(_Holder.handler, auth, can_offload=False)
    assert spec.auth_kwarg == "__varco_auth"
    assert "__varco_auth" in spec.hidden_kwargs
    assert "__varco_auth" in spec.signature.parameters


def test_synthesize_signature_drops_ctx_when_no_auth():
    """With no ``_auth``, a ctx param is dropped (preserves historical behavior)."""

    class _Holder:
        @route("GET", "/z")
        async def handler(self, ctx: AuthContext) -> dict:
            return {}

    spec = _synthesize_custom_signature(_Holder.handler, None, can_offload=False)
    assert spec.auth_kwarg is None
    assert "ctx" not in spec.signature.parameters
