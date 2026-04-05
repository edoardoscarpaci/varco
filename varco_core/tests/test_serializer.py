"""
varco_core.job.serializer — unit tests.

Tests cover:
- DefaultTaskSerializer.serialize() for all supported types
- DefaultTaskSerializer.deserialize() with type hints for all supported types
- Optional / X | None unwrapping
- Generic list[T] / tuple[T, ...] / dict[K, V] round-trips
- Pydantic BaseModel round-trip (skipped when Pydantic not installed)
- VarcoTask.payload() auto-serializes args
- VarcoTask.invoke() auto-deserializes back to typed values
- @varco_task(serializer=...) threads custom serializer through
- TaskSerializer Protocol structural check
- Full end-to-end: enqueue with typed args, invoke from payload, correct types
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any, Optional

import pytest

from varco_core.job.serializer import (
    DEFAULT_SERIALIZER,
    DefaultTaskSerializer,
    TaskSerializer,
)
from varco_core.job.task import TaskRegistry, VarcoTask, varco_task

# ── Module-level Pydantic models ───────────────────────────────────────────────
# IMPORTANT: must be at module level so typing.get_type_hints() can resolve them.
# get_type_hints() evaluates string annotations in fn.__globals__ (module dict),
# so any model used as a type hint in a VarcoTask function must live here.
try:
    from pydantic import BaseModel as _PydanticBase

    class _Item(_PydanticBase):
        name: str
        item_id: uuid.UUID

    class _Address(_PydanticBase):
        city: str

    class _User(_PydanticBase):
        name: str
        address: _Address

    class _Payload(_PydanticBase):
        user_id: uuid.UUID
        name: str

    class _Body(_PydanticBase):
        name: str
        count: int

except ImportError:
    pass


# ── DefaultTaskSerializer.serialize ───────────────────────────────────────────


class TestSerialize:
    def setup_method(self):
        self.s = DefaultTaskSerializer()

    def test_none_passthrough(self):
        assert self.s.serialize(None) is None

    def test_bool_passthrough(self):
        # bool must not be confused with int
        assert self.s.serialize(True) is True
        assert self.s.serialize(False) is False

    def test_int_passthrough(self):
        assert self.s.serialize(42) == 42

    def test_float_passthrough(self):
        assert self.s.serialize(3.14) == 3.14

    def test_str_passthrough(self):
        assert self.s.serialize("hello") == "hello"

    def test_uuid_to_str(self):
        uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
        result = self.s.serialize(uid)
        assert result == "12345678-1234-5678-1234-567812345678"
        assert isinstance(result, str)

    def test_datetime_to_isoformat(self):
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = self.s.serialize(dt)
        assert result == "2024-01-15T10:30:00+00:00"
        assert isinstance(result, str)

    def test_date_to_isoformat(self):
        d = date(2024, 6, 1)
        result = self.s.serialize(d)
        assert result == "2024-06-01"
        assert isinstance(result, str)

    def test_datetime_subclass_before_date(self):
        """datetime IS-A date — serializer must handle datetime before date."""
        dt = datetime(2024, 3, 1, 12, 0, 0)
        result = self.s.serialize(dt)
        # Should contain time component (datetime), not just date
        assert "12:00:00" in result

    def test_list_of_uuids_serialized(self):
        uids = [uuid.uuid4(), uuid.uuid4()]
        result = self.s.serialize(uids)
        assert isinstance(result, list)
        assert all(isinstance(r, str) for r in result)

    def test_tuple_becomes_list(self):
        """Tuples are serialized as lists (JSON has no tuple type)."""
        result = self.s.serialize((1, 2, 3))
        assert result == [1, 2, 3]
        assert isinstance(result, list)

    def test_dict_values_serialized(self):
        uid = uuid.uuid4()
        result = self.s.serialize({"key": uid})
        assert isinstance(result["key"], str)

    def test_unknown_type_passthrough(self):
        """Unrecognised types are returned unchanged (for validate_serializable to catch)."""

        class Custom:
            pass

        obj = Custom()
        assert self.s.serialize(obj) is obj

    def test_pydantic_model(self):
        """Pydantic models serialize to a JSON-safe dict."""
        pytest.importorskip("pydantic")
        uid = uuid.uuid4()
        item = _Item(name="Widget", item_id=uid)
        result = self.s.serialize(item)

        assert isinstance(result, dict)
        assert result["name"] == "Widget"
        # model_dump(mode="json") converts UUID → str
        assert result["item_id"] == str(uid)

    def test_pydantic_nested_model(self):
        """Nested Pydantic models serialize recursively."""
        pytest.importorskip("pydantic")
        user = _User(name="Alice", address=_Address(city="Rome"))
        result = self.s.serialize(user)
        assert result["address"]["city"] == "Rome"


# ── DefaultTaskSerializer.deserialize ─────────────────────────────────────────


class TestDeserialize:
    def setup_method(self):
        self.s = DefaultTaskSerializer()

    def test_none_always_none(self):
        assert self.s.deserialize(None, int) is None
        assert self.s.deserialize(None, None) is None

    def test_no_hint_passthrough(self):
        assert self.s.deserialize("abc", None) == "abc"
        assert self.s.deserialize(42, None) == 42

    def test_str_to_uuid(self):
        uid_str = "12345678-1234-5678-1234-567812345678"
        result = self.s.deserialize(uid_str, uuid.UUID)
        assert isinstance(result, uuid.UUID)
        assert str(result) == uid_str

    def test_str_to_datetime(self):
        iso = "2024-01-15T10:30:00+00:00"
        result = self.s.deserialize(iso, datetime)
        assert isinstance(result, datetime)
        assert result.year == 2024

    def test_str_to_date(self):
        iso = "2024-06-01"
        result = self.s.deserialize(iso, date)
        assert isinstance(result, date)
        assert result.month == 6

    def test_optional_uuid(self):
        """Optional[UUID] / UUID | None should deserialize the inner UUID."""
        uid_str = "12345678-1234-5678-1234-567812345678"
        result = self.s.deserialize(uid_str, Optional[uuid.UUID])
        assert isinstance(result, uuid.UUID)

    def test_optional_none_passthrough(self):
        assert self.s.deserialize(None, Optional[uuid.UUID]) is None

    def test_list_of_uuid(self):
        uid_strs = [str(uuid.uuid4()), str(uuid.uuid4())]
        result = self.s.deserialize(uid_strs, list[uuid.UUID])
        assert all(isinstance(r, uuid.UUID) for r in result)
        assert isinstance(result, list)

    def test_tuple_of_int(self):
        result = self.s.deserialize([1, 2, 3], tuple[int, ...])
        assert result == (1, 2, 3)
        assert isinstance(result, tuple)

    def test_dict_str_to_uuid(self):
        uid_str = str(uuid.uuid4())
        result = self.s.deserialize({"key": uid_str}, dict[str, uuid.UUID])
        assert isinstance(result["key"], uuid.UUID)

    def test_pydantic_model(self):
        """dict → Pydantic model when type_hint is a BaseModel subclass."""
        pytest.importorskip("pydantic")
        result = self.s.deserialize({"name": "Widget", "count": 5}, _Body)
        assert isinstance(result, _Body)
        assert result.name == "Widget"
        assert result.count == 5

    def test_unknown_hint_passthrough(self):
        """Unrecognised type hint — raw value returned unchanged."""

        class Custom:
            pass

        assert self.s.deserialize("raw", Custom) == "raw"


# ── Round-trip ─────────────────────────────────────────────────────────────────


class TestRoundTrip:
    def setup_method(self):
        self.s = DefaultTaskSerializer()

    def test_uuid_round_trip(self):
        uid = uuid.uuid4()
        assert self.s.deserialize(self.s.serialize(uid), uuid.UUID) == uid

    def test_datetime_round_trip(self):
        dt = datetime(2024, 5, 20, 14, 0, 0, tzinfo=timezone.utc)
        assert self.s.deserialize(self.s.serialize(dt), datetime) == dt

    def test_date_round_trip(self):
        d = date(2025, 1, 1)
        assert self.s.deserialize(self.s.serialize(d), date) == d

    def test_list_uuid_round_trip(self):
        uids = [uuid.uuid4(), uuid.uuid4()]
        serialized = self.s.serialize(uids)
        restored = self.s.deserialize(serialized, list[uuid.UUID])
        assert restored == uids

    def test_pydantic_round_trip(self):
        pytest.importorskip("pydantic")
        original = _Payload(user_id=uuid.uuid4(), name="Alice")
        serialized = self.s.serialize(original)
        restored = self.s.deserialize(serialized, _Payload)
        assert restored == original


# ── TaskSerializer Protocol ────────────────────────────────────────────────────


class TestTaskSerializerABC:
    def test_default_serializer_is_subclass(self):
        """DefaultTaskSerializer is a concrete subclass of TaskSerializer."""
        assert issubclass(DefaultTaskSerializer, TaskSerializer)
        assert isinstance(DefaultTaskSerializer(), TaskSerializer)

    def test_custom_subclass_satisfies_abc(self):
        """Explicit subclass with both methods satisfies the ABC."""

        class MySerializer(TaskSerializer):
            def serialize(self, value: Any) -> Any:
                return value

            def deserialize(self, value: Any, type_hint: type | None) -> Any:
                return value

        assert isinstance(MySerializer(), TaskSerializer)

    def test_cannot_instantiate_abc_directly(self):
        """TaskSerializer itself cannot be instantiated — it is abstract."""
        with pytest.raises(TypeError, match="abstract"):
            TaskSerializer()  # type: ignore[abstract]

    def test_missing_method_raises_on_instantiation(self):
        """A subclass that omits serialize() raises TypeError at instantiation."""

        class Incomplete(TaskSerializer):
            def serialize(self, value: Any) -> Any:
                return value

            # deserialize() intentionally missing

        with pytest.raises(TypeError):
            Incomplete()

    def test_module_singleton_is_default_serializer(self):
        assert isinstance(DEFAULT_SERIALIZER, DefaultTaskSerializer)


# ── VarcoTask integration ──────────────────────────────────────────────────────


class TestVarcoTaskWithSerializer:
    async def test_payload_serializes_uuid(self):
        """payload() converts UUID args to strings."""

        async def fn(item_id: uuid.UUID) -> None: ...

        task = VarcoTask(name="t", fn=fn)
        uid = uuid.uuid4()
        p = task.payload(uid)
        assert p.args == [str(uid)]
        # Resulting payload must be JSON-safe
        p.validate_serializable()

    async def test_invoke_deserializes_uuid(self):
        """invoke() reconstructs UUID from stored string."""
        results: list[uuid.UUID] = []

        async def capture(item_id: uuid.UUID) -> None:
            results.append(item_id)

        task = VarcoTask(name="t", fn=capture)
        uid = uuid.uuid4()
        p = task.payload(uid)
        await task.invoke(p)
        assert results == [uid]
        assert isinstance(results[0], uuid.UUID)

    async def test_payload_invoke_round_trip_datetime(self):
        """datetime arg round-trips through payload/invoke."""
        seen: list[datetime] = []

        async def fn(ts: datetime) -> None:
            seen.append(ts)

        task = VarcoTask(name="t", fn=fn)
        dt = datetime(2024, 3, 15, 9, 0, 0, tzinfo=timezone.utc)
        p = task.payload(dt)
        await task.invoke(p)
        assert seen == [dt]

    async def test_payload_invoke_pydantic(self):
        """Pydantic model round-trips through payload/invoke."""
        pytest.importorskip("pydantic")
        received: list[_Body] = []

        # fn must reference the module-level _Body so get_type_hints() can resolve it
        async def fn(body: _Body) -> None:
            received.append(body)

        task = VarcoTask(name="t", fn=fn)
        body = _Body(name="Widget", count=3)
        p = task.payload(body)
        p.validate_serializable()
        await task.invoke(p)
        assert received == [body]

    async def test_no_annotation_raw_passthrough(self):
        """Args with no type annotation are passed as-is (no deserialization)."""
        received: list = []

        async def fn(x) -> None:  # no annotation
            received.append(x)

        task = VarcoTask(name="t", fn=fn)
        p = task.payload("raw_value")
        await task.invoke(p)
        assert received == ["raw_value"]

    async def test_custom_serializer_used(self):
        """Custom serializer is applied when set on VarcoTask."""

        class UpperSerializer(TaskSerializer):
            def serialize(self, value: Any) -> Any:
                return value.upper() if isinstance(value, str) else value

            def deserialize(self, value: Any, type_hint: type | None) -> Any:
                return value.lower() if isinstance(value, str) else value

        received: list[str] = []

        async def fn(s: str) -> None:
            received.append(s)

        task = VarcoTask(name="t", fn=fn, serializer=UpperSerializer())
        p = task.payload("hello")
        # Serialized to uppercase
        assert p.args == ["HELLO"]
        await task.invoke(p)
        # Deserialized back to lowercase
        assert received == ["hello"]

    async def test_varco_task_decorator_serializer_param(self):
        """@varco_task(serializer=...) threads the serializer through."""

        class NoOpSerializer(TaskSerializer):
            def serialize(self, v: Any) -> Any:
                return v

            def deserialize(self, v: Any, h: type | None) -> Any:
                return v

        s = NoOpSerializer()

        @varco_task(name="decorated", serializer=s)
        async def fn(x: int) -> int:
            return x

        assert fn._serializer is s

    async def test_registry_invoke_uses_task_serializer(self):
        """TaskRegistry.invoke delegates to task.invoke() — serializer is applied."""
        seen: list[uuid.UUID] = []

        async def fn(item_id: uuid.UUID) -> None:
            seen.append(item_id)

        registry = TaskRegistry()
        task = VarcoTask(name="fn", fn=fn)
        registry.register(task)

        uid = uuid.uuid4()
        payload = task.payload(uid)  # serialize UUID → str
        await registry.invoke(payload)  # deserialize str → UUID

        assert seen == [uid]
        assert isinstance(seen[0], uuid.UUID)
