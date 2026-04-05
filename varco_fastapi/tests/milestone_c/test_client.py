"""
Milestone C tests — client layer.

Tests cover:
- ClientProfile — frozen dataclass, factory methods, immutable builders
- ClientConfigurator — service name derivation, env-var resolution, profile resolution
- _VarcoClientMeta — router type resolution from generic param, method injection
- AsyncVarcoClient — typed method generation, CRUD dispatch, no-URL error
- SyncVarcoClient — sync wrappers, with_async raises SyncClientAsyncError
- JobHandle — status polling, wait, cancel, stream_progress fallback
- bind_clients — registration helper
- ClientProtocol — structural typing check
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
import httpx

from varco_fastapi.client.base import AsyncVarcoClient, ClientProfile, VarcoClient
from varco_fastapi.client.configurator import ClientConfigurator
from varco_fastapi.client.handle import JobFailedError, JobHandle
from varco_fastapi.client.middleware import (
    CorrelationIdMiddleware,
    HeadersMiddleware,
    PreparedRequest,
    RetryMiddleware,
    TimeoutMiddleware,
)
from varco_fastapi.client.protocol import ClientProtocol
from varco_fastapi.client.sync import SyncClientAsyncError, SyncVarcoClient
from varco_fastapi.router.base import VarcoRouter
from varco_fastapi.router.presets import AllRouteMixin
from pydantic import BaseModel


# ── Test models and routers ────────────────────────────────────────────────────


class ItemCreate(BaseModel):
    name: str


class ItemRead(BaseModel):
    id: UUID
    name: str


class ItemUpdate(BaseModel):
    name: str | None = None


class ItemRouter(
    AllRouteMixin, VarcoRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]
):
    _prefix = "/items"
    _tags = ["items"]
    _service = MagicMock()


# ── PreparedRequest ───────────────────────────────────────────────────────────


def test_prepared_request_is_frozen():
    """PreparedRequest is a frozen dataclass — mutation raises."""
    req = PreparedRequest(
        method="GET",
        url="/items",
        headers={},
        body=None,
        query_params={},
    )
    with pytest.raises((AttributeError, TypeError)):
        req.method = "POST"  # type: ignore[misc]


def test_prepared_request_with_header():
    """with_header() returns new instance with added header."""
    req = PreparedRequest(method="GET", url="/", headers={}, body=None, query_params={})
    updated = req.with_header("X-Foo", "bar")
    assert updated.headers["X-Foo"] == "bar"
    assert req.headers == {}  # original unchanged


def test_prepared_request_with_query():
    """with_query() returns new instance with added query param."""
    req = PreparedRequest(method="GET", url="/", headers={}, body=None, query_params={})
    updated = req.with_query("limit", "20")
    assert updated.query_params["limit"] == "20"
    assert req.query_params == {}


def test_prepared_request_with_url():
    """with_url() returns new instance with changed URL."""
    req = PreparedRequest(method="GET", url="/", headers={}, body=None, query_params={})
    updated = req.with_url("/items/123")
    assert updated.url == "/items/123"
    assert req.url == "/"


# ── ClientProfile ──────────────────────────────────────────────────────────────


def test_client_profile_is_frozen():
    """ClientProfile is a frozen dataclass."""
    profile = ClientProfile()
    with pytest.raises((AttributeError, TypeError)):
        profile.timeout = 99.0  # type: ignore[misc]


def test_client_profile_defaults():
    """ClientProfile defaults to 30s timeout, no middleware, no trust store."""
    profile = ClientProfile()
    assert profile.timeout == 30.0
    assert profile.middleware == ()
    assert profile.trust_store is None


def test_client_profile_with_middleware():
    """with_middleware() returns new profile with appended middleware."""
    profile = ClientProfile()
    mw = HeadersMiddleware({"X-Test": "1"})
    updated = profile.with_middleware(mw)
    assert len(updated.middleware) == 1
    assert updated.middleware[0] is mw
    assert profile.middleware == ()  # original unchanged


def test_client_profile_with_timeout():
    """with_timeout() returns new profile with updated timeout."""
    profile = ClientProfile(timeout=30.0)
    updated = profile.with_timeout(5.0)
    assert updated.timeout == 5.0
    assert profile.timeout == 30.0  # original unchanged


def test_client_profile_development_factory():
    """ClientProfile.development() creates a lightweight dev profile."""
    profile = ClientProfile.development()
    # Should include logging and correlation middleware
    mw_types = {type(mw).__name__ for mw in profile.middleware}
    assert "LoggingMiddleware" in mw_types or "CorrelationIdMiddleware" in mw_types
    assert profile.timeout == 10.0


def test_client_profile_from_env_default(monkeypatch):
    """ClientProfile.from_env() with no env vars uses defaults."""
    # Clear relevant env vars so we get pure defaults
    for key in ["VARCO_CLIENT_TIMEOUT", "VARCO_CLIENT_RETRY_MAX"]:
        monkeypatch.delenv(key, raising=False)
    profile = ClientProfile.from_env()
    assert profile.timeout == 30.0


def test_client_profile_from_env_timeout(monkeypatch):
    """ClientProfile.from_env() reads VARCO_CLIENT_TIMEOUT."""
    monkeypatch.setenv("VARCO_CLIENT_TIMEOUT", "15")
    profile = ClientProfile.from_env()
    assert profile.timeout == 15.0


def test_client_profile_from_env_retry(monkeypatch):
    """ClientProfile.from_env() adds RetryMiddleware when VARCO_CLIENT_RETRY_MAX > 0."""
    monkeypatch.setenv("VARCO_CLIENT_RETRY_MAX", "3")
    monkeypatch.setenv("VARCO_CLIENT_RETRY_DELAY", "0.1")
    profile = ClientProfile.from_env()
    retry_mws = [mw for mw in profile.middleware if isinstance(mw, RetryMiddleware)]
    assert len(retry_mws) == 1


# ── ClientConfigurator ─────────────────────────────────────────────────────────


def test_configurator_service_name_from_prefix():
    """_derive_service_name() derives from router._prefix."""
    name = ClientConfigurator._derive_service_name(ItemRouter)
    assert name == "ITEMS"


def test_configurator_service_name_from_class_name():
    """_derive_service_name() falls back to class name when prefix is empty."""

    class OrderRouter(VarcoRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]):
        _prefix = ""
        _service = MagicMock()

    name = ClientConfigurator._derive_service_name(OrderRouter)
    assert name == "ORDER"


def test_configurator_service_name_hyphen_prefix():
    """_derive_service_name() converts hyphens to underscores."""

    class MyRouter(VarcoRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]):
        _prefix = "/user-profiles"
        _service = MagicMock()

    name = ClientConfigurator._derive_service_name(MyRouter)
    assert name == "USER_PROFILES"


def test_configurator_get_url_explicit():
    """get_url() returns explicit base_url when provided."""

    class MyCfg(ClientConfigurator[ItemRouter]):
        pass

    cfg = MyCfg(base_url="https://items.example.com")
    assert cfg.get_url() == "https://items.example.com"


def test_configurator_get_url_from_env(monkeypatch):
    """get_url() reads VARCO_CLIENT_ITEMS_URL from env."""
    monkeypatch.setenv("VARCO_CLIENT_ITEMS_URL", "https://items.prod.internal")

    class MyCfg(ClientConfigurator[ItemRouter]):
        pass

    cfg = MyCfg()
    assert cfg.get_url() == "https://items.prod.internal"


def test_configurator_get_url_default_raises():
    """get_url() raises NotImplementedError when no URL is configured."""

    class MyCfg(ClientConfigurator[ItemRouter]):
        pass

    cfg = MyCfg()
    # Ensure env var is NOT set — use fresh env
    orig = os.environ.pop("VARCO_CLIENT_ITEMS_URL", None)
    try:
        with pytest.raises(NotImplementedError):
            cfg.get_url()
    finally:
        if orig:
            os.environ["VARCO_CLIENT_ITEMS_URL"] = orig


def test_configurator_default_url_override():
    """Subclass can override default_url()."""

    class MyCfg(ClientConfigurator[ItemRouter]):
        def default_url(self) -> str:
            return "https://items.hardcoded.internal"

    cfg = MyCfg()
    assert cfg.get_url() == "https://items.hardcoded.internal"


def test_configurator_get_headers_merged(monkeypatch):
    """get_headers() merges default_headers + env + explicit."""
    monkeypatch.setenv("VARCO_CLIENT_ITEMS_HEADERS", '{"X-Env": "env"}')

    class MyCfg(ClientConfigurator[ItemRouter]):
        def default_headers(self) -> dict[str, str]:
            return {"X-Default": "default"}

    cfg = MyCfg(extra_headers={"X-Explicit": "explicit"})
    headers = cfg.get_headers()
    assert headers["X-Default"] == "default"
    assert headers["X-Env"] == "env"
    assert headers["X-Explicit"] == "explicit"


def test_configurator_get_timeout_env(monkeypatch):
    """get_timeout() reads VARCO_CLIENT_ITEMS_TIMEOUT."""
    monkeypatch.setenv("VARCO_CLIENT_ITEMS_TIMEOUT", "8")

    class MyCfg(ClientConfigurator[ItemRouter]):
        pass

    cfg = MyCfg()
    assert cfg.get_timeout() == 8.0


def test_configurator_get_proxy_env(monkeypatch):
    """get_proxy() reads VARCO_CLIENT_ITEMS_PROXY."""
    monkeypatch.setenv("VARCO_CLIENT_ITEMS_PROXY", "http://proxy:3128")

    class MyCfg(ClientConfigurator[ItemRouter]):
        pass

    cfg = MyCfg()
    assert cfg.get_proxy() == "http://proxy:3128"


# ── _VarcoClientMeta / AsyncVarcoClient ────────────────────────────────────────


def test_varco_client_router_class_resolved():
    """Metaclass resolves _router_class from generic parameter."""

    class ItemClient(AsyncVarcoClient[ItemRouter]):
        pass

    assert ItemClient._router_class is ItemRouter


def test_varco_client_generates_crud_methods():
    """Metaclass generates all 6 CRUD methods on the client."""

    class ItemClient(AsyncVarcoClient[ItemRouter]):
        pass

    for method_name in ("create", "read", "update", "patch", "delete", "list"):
        assert callable(
            getattr(ItemClient, method_name, None)
        ), f"Method '{method_name}' not generated on ItemClient"


async def test_varco_client_no_url_raises():
    """_request() raises RuntimeError when no base URL is configured."""

    class ItemClient(AsyncVarcoClient[ItemRouter]):
        pass

    client = ItemClient()
    with pytest.raises(RuntimeError, match="no base URL"):
        await client._request("GET", "/", body=None, path_params={}, query_params={})


async def test_varco_client_request_sends_correct_http():
    """_request() builds and sends the correct httpx request."""

    class ItemClient(AsyncVarcoClient[ItemRouter]):
        pass

    # Mock the httpx.AsyncClient.request method
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": str(uuid4()), "name": "test"}
    mock_response.raise_for_status = MagicMock()

    client = ItemClient("https://api.example.com")
    client._client = MagicMock()
    client._client.request = AsyncMock(return_value=mock_response)

    _result = await client._request(
        "GET",
        "/{id}",
        body=None,
        path_params={"id": "123"},
        query_params={},
        response_model=ItemRead,
    )
    # Verify httpx was called with correct substituted path
    client._client.request.assert_called_once()
    call_kwargs = client._client.request.call_args
    assert call_kwargs.kwargs["url"] == "/123" or "/123" in str(call_kwargs)


async def test_varco_client_create_method_posts():
    """create() method calls POST /."""

    class ItemClient(AsyncVarcoClient[ItemRouter]):
        pass

    item_id = uuid4()
    mock_response = MagicMock()
    mock_response.status_code = 201
    mock_response.json.return_value = {"id": str(item_id), "name": "widget"}
    mock_response.raise_for_status = MagicMock()

    client = ItemClient("https://api.example.com")
    client._client = MagicMock()
    client._client.request = AsyncMock(return_value=mock_response)

    body = ItemCreate(name="widget")
    _result = await client.create(body)

    call_args = client._client.request.call_args
    assert (
        call_args.kwargs.get("method") == "POST"
        or call_args[1].get("method") == "POST"
        or "POST" in str(call_args)
    )


# ── VarcoClient alias ─────────────────────────────────────────────────────────


def test_varco_client_is_alias():
    """VarcoClient is an alias for AsyncVarcoClient."""
    assert VarcoClient is AsyncVarcoClient


# ── SyncVarcoClient ───────────────────────────────────────────────────────────


def test_sync_client_with_async_raises():
    """SyncVarcoClient.create(with_async=True) raises SyncClientAsyncError."""

    class ItemClient(AsyncVarcoClient[ItemRouter]):
        pass

    async_client = ItemClient("https://api.example.com")
    sync_client = SyncVarcoClient(async_client)
    with pytest.raises(SyncClientAsyncError):
        sync_client.create(ItemCreate(name="test"), with_async=True)


def test_sync_client_context_manager():
    """SyncVarcoClient can be used as a context manager."""

    class ItemClient(AsyncVarcoClient[ItemRouter]):
        pass

    async_client = ItemClient("https://api.example.com")

    with SyncVarcoClient(async_client) as client:
        assert client._client is not None
    assert client._client is None


# ── JobHandle ──────────────────────────────────────────────────────────────────


async def test_job_handle_status_calls_poll_url():
    """JobHandle.status() calls GET poll_url via the client."""
    job_id = uuid4()

    mock_client = MagicMock()
    mock_client._request = AsyncMock(
        return_value={"status": "running", "progress": 0.5}
    )

    handle = JobHandle(
        job_id=job_id,
        poll_url=f"/jobs/{job_id}",
        events_url=None,
        client=mock_client,
    )
    result = await handle.status()
    assert result["status"] == "running"
    mock_client._request.assert_called_once_with(
        "GET",
        f"/jobs/{job_id}",
        body=None,
        path_params={},
        query_params={},
        response_model=None,
    )


async def test_job_handle_wait_completes():
    """JobHandle.wait() returns result when job reaches COMPLETED."""
    job_id = uuid4()

    call_count = 0
    responses = [
        {"status": "running", "progress": 0.5},
        {"status": "completed", "result": {"done": True}},
    ]

    async def _mock_request(*args, **kwargs):
        nonlocal call_count
        resp = responses[min(call_count, len(responses) - 1)]
        call_count += 1
        return resp

    mock_client = MagicMock()
    mock_client._request = _mock_request

    handle = JobHandle(
        job_id=job_id,
        poll_url=f"/jobs/{job_id}",
        events_url=None,
        client=mock_client,
    )
    result = await handle.wait(poll_interval=0.01, timeout=5.0)
    assert result == {"done": True}


async def test_job_handle_wait_raises_on_failure():
    """JobHandle.wait() raises JobFailedError when job reaches FAILED."""
    job_id = uuid4()

    mock_client = MagicMock()
    mock_client._request = AsyncMock(return_value={"status": "failed", "error": "boom"})

    handle = JobHandle(
        job_id=job_id,
        poll_url=f"/jobs/{job_id}",
        events_url=None,
        client=mock_client,
    )
    with pytest.raises(JobFailedError, match="boom"):
        await handle.wait(poll_interval=0.01, timeout=5.0)


async def test_job_handle_cancel_returns_true_on_success():
    """JobHandle.cancel() returns True when DELETE succeeds."""
    job_id = uuid4()
    mock_client = MagicMock()
    mock_client._request = AsyncMock(return_value=None)

    handle = JobHandle(
        job_id=job_id,
        poll_url=f"/jobs/{job_id}",
        events_url=None,
        client=mock_client,
    )
    result = await handle.cancel()
    assert result is True


async def test_job_handle_cancel_returns_false_on_error():
    """JobHandle.cancel() returns False when DELETE fails."""
    job_id = uuid4()
    mock_client = MagicMock()
    mock_client._request = AsyncMock(side_effect=Exception("404 Not Found"))

    handle = JobHandle(
        job_id=job_id,
        poll_url=f"/jobs/{job_id}",
        events_url=None,
        client=mock_client,
    )
    result = await handle.cancel()
    assert result is False


async def test_job_handle_stream_progress_fallback_to_polling():
    """stream_progress() falls back to polling when events_url is None."""
    job_id = uuid4()
    call_count = 0
    responses = [
        {"status": "running", "progress": 0.3},
        {"status": "completed", "result": None},
    ]

    async def _mock_request(*args, **kwargs):
        nonlocal call_count
        resp = responses[min(call_count, len(responses) - 1)]
        call_count += 1
        return resp

    mock_client = MagicMock()
    mock_client._request = _mock_request

    handle = JobHandle(
        job_id=job_id,
        poll_url=f"/jobs/{job_id}",
        events_url=None,
        client=mock_client,
    )
    events = []
    async for event in handle.stream_progress():
        events.append(event)

    assert len(events) >= 2
    assert events[-1]["status"] == "completed"


# ── ClientProtocol structural typing ──────────────────────────────────────────


def test_client_protocol_satisfied_by_mock():
    """Any object with the right methods satisfies ClientProtocol structurally."""
    # ClientProtocol is a Protocol — structural check only (no isinstance needed)
    # Verify the protocol has the expected methods
    for method in ("create", "read", "update", "patch", "delete", "list"):
        assert hasattr(ClientProtocol, method), f"ClientProtocol missing {method}"


def test_async_varco_client_satisfies_client_protocol():
    """AsyncVarcoClient subclass satisfies ClientProtocol (structural check)."""

    class ItemClient(AsyncVarcoClient[ItemRouter]):
        pass

    client = ItemClient("https://api.example.com")
    # Verify all protocol methods exist on the client
    for method_name in ("create", "read", "update", "patch", "delete", "list"):
        assert callable(
            getattr(client, method_name, None)
        ), f"ItemClient missing protocol method '{method_name}'"


# ── bind_clients ──────────────────────────────────────────────────────────────


def test_bind_clients_raises_for_missing_router():
    """bind_clients() raises TypeError for client without _router_class."""
    from varco_fastapi.di import bind_clients

    class BrokenClient(AsyncVarcoClient):
        pass

    BrokenClient._router_class = None  # force missing

    mock_container = MagicMock()
    with pytest.raises(TypeError, match="_router_class"):
        bind_clients(mock_container, BrokenClient)


def test_bind_clients_calls_container_bind():
    """bind_clients() calls container.bind() for each client."""
    from varco_fastapi.di import bind_clients

    class ItemClient(AsyncVarcoClient[ItemRouter]):
        pass

    mock_container = MagicMock()
    bind_clients(mock_container, ItemClient)
    # Container bind or provide should have been called
    assert mock_container.bind.called or mock_container.provide.called


# ── Middleware ─────────────────────────────────────────────────────────────────


async def test_headers_middleware_injects_headers():
    """HeadersMiddleware adds headers to the request."""
    mw = HeadersMiddleware({"X-Test": "value"})
    seen_headers: dict[str, str] = {}

    async def next_fn(req: PreparedRequest) -> httpx.Response:
        seen_headers.update(req.headers)
        return MagicMock(spec=httpx.Response)

    req = PreparedRequest(method="GET", url="/", headers={}, body=None, query_params={})
    await mw(req, next_fn)
    assert seen_headers.get("X-Test") == "value"


async def test_timeout_middleware_applies_timeout():
    """TimeoutMiddleware wraps next() with asyncio.wait_for."""
    import asyncio

    mw = TimeoutMiddleware(timeout=0.001)  # Very short timeout

    async def slow_next(req: PreparedRequest) -> httpx.Response:
        await asyncio.sleep(10.0)  # Will be cancelled
        return MagicMock(spec=httpx.Response)

    req = PreparedRequest(method="GET", url="/", headers={}, body=None, query_params={})
    with pytest.raises(asyncio.TimeoutError):
        await mw(req, slow_next)


async def test_correlation_id_middleware_propagates_id():
    """CorrelationIdMiddleware injects X-Correlation-ID when available."""
    from varco_core.tracing import correlation_context

    mw = CorrelationIdMiddleware()
    seen_headers: dict[str, str] = {}

    async def next_fn(req: PreparedRequest) -> httpx.Response:
        seen_headers.update(req.headers)
        return MagicMock(spec=httpx.Response)

    req = PreparedRequest(method="GET", url="/", headers={}, body=None, query_params={})
    async with correlation_context("test-corr-id"):
        await mw(req, next_fn)
    assert seen_headers.get("X-Correlation-ID") == "test-corr-id"


async def test_retry_middleware_retries_on_5xx():
    """RetryMiddleware retries on 502/503/504 responses."""
    from varco_core.resilience import RetryPolicy

    call_count = 0
    responses = [
        MagicMock(status_code=503),
        MagicMock(status_code=200),
    ]

    async def next_fn(req: PreparedRequest) -> httpx.Response:
        nonlocal call_count
        resp = responses[call_count]
        call_count += 1
        return resp

    mw = RetryMiddleware(RetryPolicy(max_attempts=3, base_delay=0.0))
    req = PreparedRequest(method="GET", url="/", headers={}, body=None, query_params={})
    result = await mw(req, next_fn)
    assert result.status_code == 200
    assert call_count == 2  # first attempt fails, second succeeds


# ── Public API surface ────────────────────────────────────────────────────────


def test_public_api_imports():
    """All public client symbols are accessible from varco_fastapi top-level."""
    import varco_fastapi

    for symbol in (
        "AsyncVarcoClient",
        "VarcoClient",
        "ClientProfile",
        "ClientConfigurator",
        "ClientProtocol",
        "SyncVarcoClient",
        "JobHandle",
        "JobFailedError",
        "PreparedRequest",
        "AbstractClientMiddleware",
        "HeadersMiddleware",
        "CorrelationIdMiddleware",
        "AuthForwardMiddleware",
        "JwtMiddleware",
        "RetryMiddleware",
        "LoggingMiddleware",
        "TimeoutMiddleware",
        "OTelClientMiddleware",
        "bind_clients",
    ):
        assert hasattr(varco_fastapi, symbol), f"varco_fastapi.{symbol} not exported"
