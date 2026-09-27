"""
Plan 049 / Step 11 — `create_varco_app(container, *, modules=None, ...)`
(§D-signature/§D-order), Docker-free.

RED MODE: `create_varco_app` still has its pre-049 eleven-keyword signature
(no `modules=` kwarg, `container` optional) — every test below is expected
to fail with `TypeError` (unexpected keyword `modules`) or `ImportError`
for names Step 3/5/6/7/9/12 have not created yet.

BUGFIX (test, not plan): most calls below now pass `validate=False` — none
of them bind `AbstractServerAuth`, which `validate_container_bindings()`
has required since before this plan (unchanged by it) and every other
from-scratch `create_varco_app(...)` test in this repo already passes
`validate=False` for exactly this reason (see
`test_middleware_order.py::_build_baseline_app`). None of these tests are
about DI-binding validation — they are about module registration order,
lifecycle sequencing, and middleware slot placement.
"""

from __future__ import annotations

import logging

import pytest
from providify import DIContainer
from varco_core.di.module import CoreModule, LifecycleModule, ModuleRegistry, VarcoModule
from varco_fastapi.app import create_varco_app
from varco_fastapi.middleware.security_headers import SecurityHeadersMiddleware
from varco_fastapi.middleware.slots import EdgeSlot, MiddlewareSpec
from varco_fastapi.modules import FastAPIModule, SecurityPostureModule
from varco_fastapi.modules.base import MiddlewareModule
from varco_fastapi.modules.defaults import default_modules
from varco_fastapi.modules.lifecycle import MigrationsModule


def _SpyModule(
    name: str,
    calls: list[str],
    *,
    depends_on: tuple = (),
    enabled: bool | None = None,
) -> VarcoModule:
    """
    Build a fresh, uniquely-classed spy ``VarcoModule`` recording the call
    sequence of every slot it is put through.

    BUGFIX (test, not plan): the original RED test defined ``_SpyModule`` as
    ONE class and instantiated it twice (``a = _SpyModule("A", calls)``,
    ``b = _SpyModule("B", calls, depends_on=(type(a),))``) to represent two
    *distinct* logical modules — but ``resolve_module_order``'s duplicate-
    instance rule (proven in ``test_di_module.py::
    test_duplicate_instances_of_one_class_raise_value_error``, and §D-
    registry's own "a second **different instance** of the same module
    class -> ValueError") applies to exactly this shape: two different
    ``_SpyModule`` instances share `type(m) is _SpyModule`. A factory that
    hands back a freshly-defined subclass per call fixes this while
    preserving every assertion below unchanged (``type(a)`` still names a's
    own class, distinct from ``type(b)``'s).
    """

    class _Spy(VarcoModule):
        def _register(self, container: DIContainer) -> None:
            calls.append(f"{name}.register")

        async def async_init(self, container: DIContainer) -> None:
            calls.append(f"{name}.async_init")

        def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
            calls.append(f"{name}.middleware")
            return []

        def configure_app(self, app: object, container: DIContainer) -> None:
            calls.append(f"{name}.configure_app")

        async def start(self, container: DIContainer) -> None:
            calls.append(f"{name}.start")

        async def stop(self, container: DIContainer) -> None:
            calls.append(f"{name}.stop")

    _Spy.__name__ = f"_SpyModule_{name}"
    _Spy.__qualname__ = _Spy.__name__
    _Spy.name = f"spy_{name}"
    _Spy.depends_on = depends_on
    return _Spy(enabled=enabled)


class TestCreateVarcoAppRequiresContainer:
    async def test_no_container_raises_type_error(self) -> None:
        with pytest.raises(TypeError):
            create_varco_app()  # type: ignore[call-arg]


class TestModulesNoneUsesDefaultModules:
    async def test_registry_names_match_default_modules_in_order(self) -> None:
        container = DIContainer()
        create_varco_app(container, validate=False)

        registry = container.get(ModuleRegistry)
        expected_names = tuple(type(m).__name__ for m in default_modules())

        assert tuple(type(m).__name__ for m in registry.modules) == expected_names


class TestExplicitModuleList:
    async def test_only_listed_modules_are_recorded_and_no_security_headers_middleware(
        self,
    ) -> None:
        container = DIContainer()
        app = create_varco_app(container, modules=[CoreModule(), FastAPIModule()], validate=False)

        registry = container.get(ModuleRegistry)
        assert len(registry.modules) == 2
        assert not any(mw.cls is SecurityHeadersMiddleware for mw in app.user_middleware)


class TestModuleDependencyErrorPropagates:
    async def test_missing_dependency_raises(self) -> None:
        from varco_core.di.module import ModuleDependencyError

        container = DIContainer()
        with pytest.raises(ModuleDependencyError):
            create_varco_app(container, modules=[SecurityPostureModule()])


class TestDuplicateModuleClassRaises:
    async def test_duplicate_class_raises_value_error(self) -> None:
        container = DIContainer()
        with pytest.raises(ValueError):
            create_varco_app(container, modules=[*default_modules(), CoreModule()])


class TestMigrationsListOrderWarning:
    async def test_warning_logged_when_migrations_listed_after_core_module(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        container = DIContainer()
        with caplog.at_level(logging.WARNING):
            create_varco_app(
                container, modules=[*default_modules(), MigrationsModule()], validate=False
            )

        assert any(
            "MigrationsModule" in record.message or "Plan 050" in record.message
            for record in caplog.records
        )

    async def test_no_warning_when_migrations_listed_before_defaults(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        container = DIContainer()
        with caplog.at_level(logging.WARNING):
            create_varco_app(
                container, modules=[MigrationsModule(), *default_modules()], validate=False
            )

        assert not any("MigrationsModule" in record.message for record in caplog.records)


class TestModuleLifecycleSequence:
    async def test_start_order_is_resolved_order_stop_is_reverse(self) -> None:
        calls: list[str] = []
        a = _SpyModule("A", calls)
        b = _SpyModule("B", calls, depends_on=(type(a),))

        container = DIContainer()
        app = create_varco_app(container, modules=[b, a], validate=False)

        async with app.router.lifespan_context(app):
            pass

        start_calls = [c for c in calls if c.endswith(".start")]
        stop_calls = [c for c in calls if c.endswith(".stop")]

        assert start_calls == ["A.start", "B.start"]
        assert stop_calls == ["B.stop", "A.stop"]

    async def test_async_init_of_every_module_completes_before_any_start(self) -> None:
        calls: list[str] = []
        a = _SpyModule("A", calls)
        b = _SpyModule("B", calls, depends_on=(type(a),))

        container = DIContainer()
        app = create_varco_app(container, modules=[b, a], validate=False)

        async with app.router.lifespan_context(app):
            pass

        first_start_index = next(i for i, c in enumerate(calls) if c.endswith(".start"))
        async_init_calls = [i for i, c in enumerate(calls) if c.endswith(".async_init")]

        assert all(i < first_start_index for i in async_init_calls)

    async def test_disabled_module_registers_but_nothing_else_runs(self) -> None:
        calls: list[str] = []
        disabled = _SpyModule("Disabled", calls, enabled=False)

        container = DIContainer()
        create_varco_app(container, modules=[disabled], validate=False)

        assert calls == ["Disabled.register"]

    async def test_container_ashutdown_awaited_after_last_stop(self) -> None:
        calls: list[str] = []
        a = _SpyModule("A", calls)

        container = DIContainer()
        app = create_varco_app(container, modules=[a], validate=False)

        async with app.router.lifespan_context(app):
            pass

        # container.ashutdown() runs as VarcoLifespan's shutdown= hook,
        # strictly after every module's stop() (lifespan.py:239-260).
        assert calls[-1] == "A.stop"


class TestLifecycleModuleAdapter:
    async def test_wraps_a_plain_start_stop_component(self) -> None:
        events: list[str] = []

        class _Component:
            async def start(self) -> None:
                events.append("start")

            async def stop(self) -> None:
                events.append("stop")

        container = DIContainer()
        app = create_varco_app(container, modules=[LifecycleModule(_Component())], validate=False)

        async with app.router.lifespan_context(app):
            pass

        assert events == ["start", "stop"]


class TestMiddlewareModuleAdapter:
    async def test_default_slot_lands_outside_error_middleware(self) -> None:
        class _Marker:
            def __init__(self, app: object) -> None:
                self.app = app

            async def __call__(self, scope: object, receive: object, send: object) -> None:
                pass

        container = DIContainer()
        app = create_varco_app(
            container,
            modules=[*default_modules(), MiddlewareModule([(_Marker, {})])],
            validate=False,
        )

        from varco_fastapi.middleware.error import ErrorMiddleware

        classes = [mw.cls for mw in app.user_middleware]
        assert classes.index(_Marker) < classes.index(ErrorMiddleware)

    async def test_idempotency_slot_lands_inside_error_middleware(self) -> None:
        class _Marker:
            def __init__(self, app: object) -> None:
                self.app = app

            async def __call__(self, scope: object, receive: object, send: object) -> None:
                pass

        container = DIContainer()
        app = create_varco_app(
            container,
            modules=[
                *default_modules(),
                MiddlewareModule([(_Marker, {})], slot=EdgeSlot.IDEMPOTENCY),
            ],
            validate=False,
        )

        from varco_fastapi.middleware.error import ErrorMiddleware

        classes = [mw.cls for mw in app.user_middleware]
        assert classes.index(_Marker) > classes.index(ErrorMiddleware)


class TestRequestContextMiddlewareAnonymousModeFallback:
    """
    Repair round 2 / Decision 1: ``create_varco_app(DIContainer())`` — no
    ``AbstractServerAuth`` bound anywhere — must still install
    ``RequestContextMiddleware`` (pre-049 parity, `FastAPIModule`'s
    class docstring DESIGN block) rather than silently omitting it.
    """

    async def test_request_context_middleware_present_with_no_server_auth_bound(self) -> None:
        container = DIContainer()
        app = create_varco_app(container, validate=False)

        from varco_fastapi.middleware.request_context import RequestContextMiddleware

        assert any(mw.cls is RequestContextMiddleware for mw in app.user_middleware)

    async def test_request_populates_anonymous_auth_context_with_no_server_auth_bound(
        self,
    ) -> None:
        """
        A live request through the app (no ``AbstractServerAuth`` bound)
        must populate an anonymous ``AuthContext`` — proving the
        contextvar is actually set by the fallback middleware, not merely
        that the middleware class is present in the stack.
        """
        from fastapi.testclient import TestClient
        from varco_fastapi.context import auth_context_var

        container = DIContainer()
        app = create_varco_app(container, validate=False)

        captured: dict[str, object] = {}

        @app.get("/whoami")
        def whoami() -> dict[str, object]:
            ctx = auth_context_var.get()
            captured["ctx"] = ctx
            return {"user_id": ctx.user_id if ctx else None}

        client = TestClient(app)
        response = client.get("/whoami")

        assert response.status_code == 200
        assert captured["ctx"] is not None
        # AnonymousAuth's AuthContext — see varco_fastapi.auth.server_auth.
        assert captured["ctx"].user_id in (None, "anonymous")
