"""
Milestone E — router split tests (VarcoRouter vs VarcoCRUDRouter).

Tests cover:
- VarcoRouter is instantiable without a service
- VarcoCRUDRouter requires a service; handles ClassVar fallback
- VarcoCRUDRouter._make_http_handler dispatches CRUD actions
- VarcoCRUDRouter.build_router registers CRUD tasks when registry is provided
- Preset classes (CRUDRouter, ReadOnlyRouter, etc.) extend VarcoCRUDRouter
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from varco_fastapi.router.base import VarcoRouter
from varco_fastapi.router.crud import VarcoCRUDRouter
from varco_fastapi.router.mixins import CreateMixin, ListMixin, ReadMixin
from varco_fastapi.router.presets import (
    CRUDRouter,
    NoDeleteRouter,
    ReadOnlyRouter,
    WriteRouter,
)
from varco_core.job.task import TaskRegistry


class ItemCreate(BaseModel):
    name: str


class ItemRead(BaseModel):
    id: UUID
    name: str


class ItemUpdate(BaseModel):
    name: str | None = None


class _MockService:
    async def create(self, body: Any, auth: Any = None) -> Any:
        return ItemRead(id=UUID("00000000-0000-0000-0000-000000000001"), name=body.name)

    async def get(self, pk: Any, auth: Any = None) -> Any:
        # Renamed from read() to match AsyncService.get() in varco_core
        return ItemRead(id=pk, name="x")

    async def list(self, params: Any = None, auth: Any = None) -> list[Any]:
        return []

    async def count(self, params: Any = None, auth: Any = None) -> int:
        return 0

    async def update(self, pk: Any, body: Any, auth: Any = None) -> Any:
        return ItemRead(id=pk, name=body.name)

    async def patch(self, pk: Any, body: Any, auth: Any = None) -> Any:
        return ItemRead(id=pk, name=body.name or "x")

    async def delete(self, pk: Any, auth: Any = None) -> None:
        pass


# ── VarcoRouter is pure infrastructure ────────────────────────────────────────


class TestVarcoRouterBase:
    def test_instantiable_without_service(self):
        """VarcoRouter can be instantiated with no arguments."""
        router = VarcoRouter()
        assert router is not None

    def test_no_crud_dispatch_without_service(self):
        """VarcoRouter without _service returns noop (501) for CRUD routes."""

        class PureRouter(
            ListMixin, VarcoRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]
        ):
            _prefix = "/pure"

        from fastapi.testclient import TestClient
        from fastapi import FastAPI

        router_obj = PureRouter()
        api_router = router_obj.build_router()
        # Route is registered (GET /) but the handler returns 501 Not Implemented
        assert len(api_router.routes) == 1

        app = FastAPI()
        app.include_router(api_router)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/pure/")
        # 501 from noop handler (service is None — no CRUD dispatch)
        assert resp.status_code == 501

    def test_crud_dispatch_with_class_var_service(self):
        """VarcoRouter with _service ClassVar still dispatches CRUD (backward compat)."""

        class CompatRouter(
            ReadMixin, VarcoRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]
        ):
            _prefix = "/compat"
            _service = _MockService()

        from fastapi.testclient import TestClient
        from fastapi import FastAPI

        app = FastAPI()
        app.include_router(CompatRouter().build_router())
        client = TestClient(app)
        resp = client.get(f"/compat/{UUID('00000000-0000-0000-0000-000000000001')}")
        assert resp.status_code == 200


# ── VarcoCRUDRouter ────────────────────────────────────────────────────────────


class TestVarcoCRUDRouter:
    def test_inherits_varco_router(self):
        """VarcoCRUDRouter is a subclass of VarcoRouter."""
        assert issubclass(VarcoCRUDRouter, VarcoRouter)

    def test_instantiable_with_class_var_service(self):
        """VarcoCRUDRouter(service=None) uses the ClassVar _service fallback."""

        class MyRouter(
            ReadMixin, VarcoCRUDRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]
        ):
            _prefix = "/my"
            _service = _MockService()

        router = MyRouter()
        assert router._service is not None

    def test_crud_dispatch_via_make_http_handler(self):
        """VarcoCRUDRouter dispatches CRUD actions to typed factory functions."""

        class MyRouter(
            CreateMixin, VarcoCRUDRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]
        ):
            _prefix = "/items"
            _service = _MockService()

        from fastapi.testclient import TestClient
        from fastapi import FastAPI

        app = FastAPI()
        app.include_router(MyRouter().build_router())
        client = TestClient(app)
        resp = client.post("/items/", json={"name": "Widget"})
        assert resp.status_code == 201

    def test_build_router_registers_tasks_when_registry_set(self):
        """build_router() registers CRUD tasks in _task_registry if available."""
        registry = TaskRegistry()

        class TaskedRouter(
            CreateMixin,
            ReadMixin,
            VarcoCRUDRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate],
        ):
            _prefix = "/tasked"
            _service = _MockService()
            _task_registry = registry  # type: ignore[assignment]

        TaskedRouter().build_router()

        # Tasks should be registered under "TaskedRouter.create" and "TaskedRouter.read"
        assert registry.get("TaskedRouter.create") is not None
        assert registry.get("TaskedRouter.read") is not None

    def test_build_router_no_registry_no_error(self):
        """build_router() works fine when _task_registry is not set."""

        class RegistrylessRouter(
            ReadMixin, VarcoCRUDRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]
        ):
            _prefix = "/noregistry"
            _service = _MockService()

        # Should not raise even without a registry
        api_router = RegistrylessRouter().build_router()
        # ReadMixin provides only GET /{id} (not GET /) — list is a separate ListMixin
        assert len(api_router.routes) == 1


# ── Preset classes ─────────────────────────────────────────────────────────────


class TestPresets:
    def test_crud_router_extends_varco_crud_router(self):
        """CRUDRouter's MRO includes VarcoCRUDRouter."""
        assert issubclass(CRUDRouter, VarcoCRUDRouter)

    def test_read_only_router_extends_varco_crud_router(self):
        assert issubclass(ReadOnlyRouter, VarcoCRUDRouter)

    def test_write_router_extends_varco_crud_router(self):
        assert issubclass(WriteRouter, VarcoCRUDRouter)

    def test_no_delete_router_extends_varco_crud_router(self):
        assert issubclass(NoDeleteRouter, VarcoCRUDRouter)

    def test_crud_router_has_six_routes(self):
        """CRUDRouter with a mock service registers 6 CRUD routes."""

        class OrderRouter(CRUDRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]):
            _prefix = "/orders"
            _service = _MockService()

        api_router = OrderRouter().build_router()
        assert len(api_router.routes) == 6

    def test_read_only_router_has_two_routes(self):
        """ReadOnlyRouter registers only GET / and GET /{id}."""

        class OrderRouter(ReadOnlyRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]):
            _prefix = "/orders"
            _service = _MockService()

        api_router = OrderRouter().build_router()
        assert len(api_router.routes) == 2
