"""
varco_core.job.task — unit tests.

Tests cover:
- TaskPayload construction, round-trip serialization, validate_serializable()
- VarcoTask construction, __call__, payload()
- TaskRegistry register, get, invoke(), unknown-name error
- @varco_task decorator — bare form and parameterized form
"""

from __future__ import annotations

import pytest

from varco_core.job.task import TaskPayload, TaskRegistry, VarcoTask, varco_task


# ── TaskPayload ────────────────────────────────────────────────────────────────


class TestTaskPayload:
    def test_defaults(self):
        """Default args and kwargs are empty."""
        p = TaskPayload(task_name="foo")
        assert p.task_name == "foo"
        assert p.args == []
        assert p.kwargs == {}

    def test_equality(self):
        """Two payloads with the same fields are equal."""
        p1 = TaskPayload(task_name="foo", args=[1], kwargs={"k": "v"})
        p2 = TaskPayload(task_name="foo", args=[1], kwargs={"k": "v"})
        assert p1 == p2

    def test_inequality_different_name(self):
        assert TaskPayload(task_name="a") != TaskPayload(task_name="b")

    def test_to_dict(self):
        p = TaskPayload(task_name="my_task", args=["x"], kwargs={"y": 2})
        d = p.to_dict()
        assert d == {"task_name": "my_task", "args": ["x"], "kwargs": {"y": 2}}

    def test_from_dict_round_trip(self):
        """to_dict() → from_dict() produces an equal payload."""
        p = TaskPayload(
            task_name="OrderRouter.create", args=["body"], kwargs={"auth": None}
        )
        assert TaskPayload.from_dict(p.to_dict()) == p

    def test_from_dict_missing_args_defaults(self):
        """Missing 'args' and 'kwargs' keys default to empty."""
        p = TaskPayload.from_dict({"task_name": "foo"})
        assert p.args == []
        assert p.kwargs == {}

    def test_from_dict_missing_task_name_raises(self):
        """Missing 'task_name' raises KeyError."""
        with pytest.raises(KeyError):
            TaskPayload.from_dict({"args": []})

    def test_validate_serializable_ok(self):
        """validate_serializable() does not raise for JSON-safe values."""
        p = TaskPayload(task_name="t", args=[1, "x", None, True], kwargs={"k": [1, 2]})
        p.validate_serializable()  # should not raise

    def test_validate_serializable_raises_for_non_json(self):
        """validate_serializable() raises TypeError for non-JSON-safe values."""
        from uuid import uuid4

        p = TaskPayload(task_name="t", args=[uuid4()])  # UUID is not JSON-serializable
        with pytest.raises(TypeError):
            p.validate_serializable()

    def test_immutable(self):
        """TaskPayload is frozen — mutation raises FrozenInstanceError."""
        p = TaskPayload(task_name="t")
        with pytest.raises(Exception):  # dataclasses.FrozenInstanceError
            p.task_name = "other"  # type: ignore[misc]


# ── VarcoTask ──────────────────────────────────────────────────────────────────


class TestVarcoTask:
    async def test_call_delegates_to_fn(self):
        """VarcoTask.__call__ returns the wrapped function's coroutine."""

        async def double(x: int) -> int:
            return x * 2

        task = VarcoTask(name="double", fn=double)
        result = await task(5)
        assert result == 10

    def test_payload_returns_task_payload(self):
        """payload() creates a TaskPayload with the task name and given args."""

        async def noop() -> None: ...

        task = VarcoTask(name="my.noop", fn=noop)
        p = task.payload(1, 2, key="val")
        assert p.task_name == "my.noop"
        assert p.args == [1, 2]
        assert p.kwargs == {"key": "val"}

    def test_name_stored(self):
        async def fn() -> None: ...

        task = VarcoTask(name="stable_name", fn=fn)
        assert task.name == "stable_name"

    def test_repr_contains_name(self):
        async def fn() -> None: ...

        task = VarcoTask(name="OrderRouter.create", fn=fn)
        assert "OrderRouter.create" in repr(task)


# ── TaskRegistry ───────────────────────────────────────────────────────────────


class TestTaskRegistry:
    def test_register_and_get(self):
        """register() adds the task; get() retrieves it by name."""

        async def fn() -> None: ...

        registry = TaskRegistry()
        task = VarcoTask(name="my_task", fn=fn)
        registry.register(task)
        assert registry.get("my_task") is task

    def test_get_unknown_returns_none(self):
        """get() returns None for unknown names — never raises."""
        registry = TaskRegistry()
        assert registry.get("nonexistent") is None

    def test_register_duplicate_overwrites(self):
        """Re-registering the same name replaces the previous entry."""

        async def fn1() -> None: ...
        async def fn2() -> None: ...

        registry = TaskRegistry()
        t1 = VarcoTask(name="task", fn=fn1)
        t2 = VarcoTask(name="task", fn=fn2)
        registry.register(t1)
        registry.register(t2)
        assert registry.get("task") is t2

    async def test_invoke_calls_task_with_args(self):
        """invoke() re-invokes the task using the stored payload."""
        results: list[int] = []

        async def accumulate(x: int, y: int) -> int:
            total = x + y
            results.append(total)
            return total

        registry = TaskRegistry()
        registry.register(VarcoTask(name="add", fn=accumulate))

        payload = TaskPayload(task_name="add", args=[3, 7])
        result = await registry.invoke(payload)
        assert result == 10
        assert results == [10]

    async def test_invoke_unknown_raises_key_error(self):
        """invoke() raises KeyError with a helpful message for unknown tasks."""
        registry = TaskRegistry()
        payload = TaskPayload(task_name="ghost")
        with pytest.raises(KeyError, match="ghost"):
            await registry.invoke(payload)

    def test_repr_contains_task_names(self):
        async def fn() -> None: ...

        registry = TaskRegistry()
        registry.register(VarcoTask(name="alpha", fn=fn))
        registry.register(VarcoTask(name="beta", fn=fn))
        assert "alpha" in repr(registry)
        assert "beta" in repr(registry)


# ── @varco_task decorator ──────────────────────────────────────────────────────


class TestVarcoTaskDecorator:
    async def test_bare_form_uses_qualname(self):
        """@varco_task (bare) uses fn.__qualname__ as the task name."""

        @varco_task
        async def my_fn(x: int) -> int:
            return x

        assert isinstance(my_fn, VarcoTask)
        # qualname for module-level function is just the function name
        assert "my_fn" in my_fn.name
        result = await my_fn(42)
        assert result == 42

    async def test_parameterized_form_with_explicit_name(self):
        """@varco_task(name=...) uses the given name."""

        @varco_task(name="orders.create")
        async def create_order(body: dict) -> dict:
            return {"created": True}

        assert isinstance(create_order, VarcoTask)
        assert create_order.name == "orders.create"
        result = await create_order({"name": "Widget"})
        assert result == {"created": True}

    def test_parameterized_form_auto_registers(self):
        """@varco_task(registry=...) auto-registers into the given registry."""
        registry = TaskRegistry()

        @varco_task(name="auto.registered", registry=registry)
        async def auto_fn() -> None: ...

        assert registry.get("auto.registered") is auto_fn

    def test_parameterized_form_name_none_falls_back_to_qualname(self):
        """@varco_task(name=None) falls back to fn.__qualname__."""

        @varco_task(name=None)
        async def my_fallback() -> None: ...

        assert "my_fallback" in my_fallback.name

    async def test_payload_from_decorated_task(self):
        """Decorated VarcoTask.payload() works correctly."""

        @varco_task(name="sum.it")
        async def sum_fn(a: int, b: int) -> int:
            return a + b

        p = sum_fn.payload(10, 20)
        assert p.task_name == "sum.it"
        assert p.args == [10, 20]

        # Verify round-trip
        registry = TaskRegistry()
        registry.register(sum_fn)
        result = await registry.invoke(p)
        assert result == 30
