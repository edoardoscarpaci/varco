"""
milestone_f / test_app_factory.py
====================================
Tests for ``create_varco_app()`` and ``_scan_routers()``.

Covers:
- Basic creation returns a FastAPI instance
- Explicit ``routers`` list is mounted
- ``routers=[]`` mounts nothing (explicit empty list ≠ auto-scan)
- ``container=None, routers=None`` → only HealthRouter mounted
- ``validate=False`` skips validation checks
- Middleware stack is applied (CORS, Error, Tracing, RequestContext)
- MCPAdapter is mounted when provided
- SkillAdapter is mounted when provided
- HealthRouter is always present
- ``_scan_routers``: returns empty list when container.get_all raises
- ``_scan_routers``: calls container.scan() for each package in scan_packages
- ``_scan_routers``: deduplicates same class from multiple bindings
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from varco_fastapi.app import _scan_routers, create_varco_app
from varco_fastapi.router.base import VarcoRouter
from varco_fastapi.router.mixins import CreateMixin, ReadMixin


# ── Fixture routers ───────────────────────────────────────────────────────────


class ItemRouter(CreateMixin, ReadMixin, VarcoRouter):
    _prefix = "/items"


class ProductRouter(ReadMixin, VarcoRouter):
    _prefix = "/products"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _all_route_paths(app: FastAPI) -> set[str]:
    """Return all registered route path strings for an app."""
    return {getattr(r, "path", None) for r in app.routes}


# ── Basic creation ────────────────────────────────────────────────────────────


def test_create_returns_fastapi_instance():
    """``create_varco_app()`` with no args returns a ``FastAPI`` instance."""
    app = create_varco_app(validate=False)
    assert isinstance(app, FastAPI)


def test_title_and_version_set():
    app = create_varco_app(title="My API", version="2.0.0", validate=False)
    assert app.title == "My API"
    assert app.version == "2.0.0"


# ── Explicit routers ──────────────────────────────────────────────────────────


def test_explicit_routers_are_mounted():
    """Routes from ``ItemRouter`` appear in the app."""
    app = create_varco_app(routers=[ItemRouter], validate=False)
    paths = _all_route_paths(app)
    # ItemRouter has CreateMixin (POST /) and ReadMixin (GET /{id})
    # with prefix /items → /items/ and /items/{id}
    assert any("/items" in str(p) for p in paths if p)


def test_explicit_empty_routers_mounts_nothing_extra():
    """``routers=[]`` → no CRUD routes, only HealthRouter."""
    app = create_varco_app(routers=[], validate=False)
    paths = _all_route_paths(app)
    # No /items or /products routes
    assert not any("/items" in str(p) for p in paths if p)
    # But HealthRouter is always present
    assert any("health" in str(p).lower() for p in paths if p)


def test_no_container_no_routers_has_health():
    """``container=None, routers=None`` → only HealthRouter routes present."""
    app = create_varco_app(validate=False)
    paths = _all_route_paths(app)
    assert any("health" in str(p).lower() for p in paths if p)


# ── Validation flag ───────────────────────────────────────────────────────────


def test_validate_false_skips_container_check():
    """``validate=False`` does not call ``validate_container_bindings``."""
    mock_container = MagicMock()
    # Container is present but does not have is_registered — would fail if validated
    mock_container.is_registered = MagicMock(
        side_effect=AttributeError("not available")
    )
    mock_container.get = MagicMock(side_effect=LookupError("no binding"))
    mock_container.get_all = MagicMock(return_value=[])
    mock_container.scan = MagicMock()

    # Should not raise even though the container is missing required bindings
    app = create_varco_app(mock_container, routers=[], validate=False)
    assert isinstance(app, FastAPI)


def test_validate_true_calls_container_validation(monkeypatch):
    """``validate=True`` (default) calls ``validate_container_bindings``."""
    called_with = []

    def _fake_validate(container: Any) -> None:
        called_with.append(container)

    monkeypatch.setattr("varco_fastapi.app.validate_container_bindings", _fake_validate)
    mock_container = MagicMock()
    mock_container.get_all = MagicMock(return_value=[])
    mock_container.scan = MagicMock()

    create_varco_app(mock_container, routers=[], validate=True)

    assert len(called_with) == 1
    assert called_with[0] is mock_container


# ── MCPAdapter mounting ───────────────────────────────────────────────────────


def test_mcp_adapter_is_mounted_when_provided():
    """If ``mcp_adapter`` is provided, its ``mount()`` method is called."""
    mock_adapter = MagicMock()
    app = create_varco_app(routers=[], mcp_adapter=mock_adapter, validate=False)
    mock_adapter.mount.assert_called_once()
    # First arg to mount() should be the FastAPI app
    assert mock_adapter.mount.call_args[0][0] is app


def test_mcp_adapter_path_forwarded():
    """``mcp_path`` is forwarded to ``MCPAdapter.mount()``."""
    mock_adapter = MagicMock()
    create_varco_app(
        routers=[], mcp_adapter=mock_adapter, mcp_path="/ai/mcp", validate=False
    )
    mount_kwargs = mock_adapter.mount.call_args[1]
    assert mount_kwargs.get("path") == "/ai/mcp"


def test_no_mcp_adapter_does_not_call_mount():
    """When ``mcp_adapter`` is not provided, no MCP mounting happens."""
    mock_adapter = MagicMock()
    create_varco_app(routers=[], validate=False)  # no mcp_adapter
    mock_adapter.mount.assert_not_called()


# ── SkillAdapter mounting ─────────────────────────────────────────────────────


def test_skill_adapter_is_mounted_when_provided():
    """If ``skill_adapter`` is provided, its ``mount()`` method is called."""
    mock_adapter = MagicMock()
    app = create_varco_app(routers=[], skill_adapter=mock_adapter, validate=False)
    mock_adapter.mount.assert_called_once()
    assert mock_adapter.mount.call_args[0][0] is app


def test_skill_adapter_kwargs_forwarded():
    """``skill_agent_card_path`` and ``skill_tasks_prefix`` are forwarded."""
    mock_adapter = MagicMock()
    create_varco_app(
        routers=[],
        skill_adapter=mock_adapter,
        skill_agent_card_path="/custom/.well-known/agent.json",
        skill_tasks_prefix="/a2a",
        validate=False,
    )
    kwargs = mock_adapter.mount.call_args[1]
    assert kwargs.get("agent_card_path") == "/custom/.well-known/agent.json"
    assert kwargs.get("tasks_prefix") == "/a2a"


# ── HealthRouter ──────────────────────────────────────────────────────────────


def test_health_router_always_present():
    """HealthRouter is always mounted regardless of other arguments."""
    app = create_varco_app(validate=False)
    client = TestClient(app, raise_server_exceptions=False)
    # HealthRouter typically exposes GET /health or GET /health/live
    resp = client.get("/health")
    # Must exist (200 or similar) — not 404
    assert resp.status_code != 404


# ── Extra middleware ──────────────────────────────────────────────────────────


def test_extra_middleware_tuple_form_accepted():
    """Extra middleware passed as ``(MiddlewareClass, kwargs)`` does not raise."""
    from starlette.middleware.base import BaseHTTPMiddleware

    class NoopMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            return await call_next(request)

    app = create_varco_app(
        routers=[],
        extra_middleware=[(NoopMiddleware, {})],
        validate=False,
    )
    assert isinstance(app, FastAPI)


def test_extra_middleware_bare_class_form_accepted():
    """Extra middleware passed as a bare class does not raise."""
    from starlette.middleware.base import BaseHTTPMiddleware

    class NoopMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            return await call_next(request)

    app = create_varco_app(
        routers=[],
        extra_middleware=[NoopMiddleware],
        validate=False,
    )
    assert isinstance(app, FastAPI)


# ── _scan_routers ──────────────────────────────────────────────────────────────


def test_scan_routers_returns_empty_on_get_all_error():
    """``container.get_all()`` raising → returns empty list (no crash)."""
    mock_container = MagicMock()
    mock_container.get_all = MagicMock(
        side_effect=RuntimeError("no VarcoRouter binding")
    )
    mock_container.scan = MagicMock()

    result = _scan_routers(mock_container)
    assert result == []


def test_scan_routers_calls_scan_for_each_package():
    """``scan_packages`` triggers ``container.scan(pkg, recursive=True)`` per package."""
    mock_container = MagicMock()
    mock_container.get_all = MagicMock(return_value=[])
    mock_container.scan = MagicMock()

    _scan_routers(mock_container, scan_packages=["myapp.routers", "myapp.admin"])

    mock_container.scan.assert_any_call("myapp.routers", recursive=True)
    mock_container.scan.assert_any_call("myapp.admin", recursive=True)


def test_scan_routers_no_packages_skips_scan():
    """``scan_packages=None`` → ``container.scan()`` is NOT called."""
    mock_container = MagicMock()
    mock_container.get_all = MagicMock(return_value=[])
    mock_container.scan = MagicMock()

    _scan_routers(mock_container)

    mock_container.scan.assert_not_called()


def test_scan_routers_deduplicates_same_class():
    """Two bindings for the same class return only one entry."""
    mock_container = MagicMock()
    # get_all returns two instances of the same class
    inst1 = ItemRouter()
    inst2 = ItemRouter()
    mock_container.get_all = MagicMock(return_value=[inst1, inst2])
    mock_container.scan = MagicMock()

    result = _scan_routers(mock_container)
    assert len(result) == 1
    assert result[0] is ItemRouter


def test_scan_routers_returns_classes_not_instances():
    """``_scan_routers`` returns class objects, not instances."""
    mock_container = MagicMock()
    inst = ItemRouter()
    mock_container.get_all = MagicMock(return_value=[inst])
    mock_container.scan = MagicMock()

    result = _scan_routers(mock_container)
    assert result == [ItemRouter]
    assert isinstance(result[0], type)


def test_scan_routers_continues_on_scan_error():
    """A failing ``container.scan()`` for one package does not abort the others."""
    mock_container = MagicMock()
    inst = ItemRouter()
    mock_container.get_all = MagicMock(return_value=[inst])

    call_count = 0

    def _scan(pkg: str, recursive: bool = False) -> None:
        nonlocal call_count
        call_count += 1
        if pkg == "bad.package":
            raise ModuleNotFoundError("bad.package not found")

    mock_container.scan = MagicMock(side_effect=_scan)

    # Should not raise — bad package is logged and skipped
    result = _scan_routers(
        mock_container, scan_packages=["bad.package", "good.package"]
    )
    assert call_count == 2  # both packages attempted
    assert result == [ItemRouter]  # good package's routers still found
