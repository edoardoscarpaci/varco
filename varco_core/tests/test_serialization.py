"""
Unit tests for varco_core.serialization
=========================================
Covers the general-purpose serialization utilities.

Sections
--------
- ``Serializer`` protocol   — isinstance checks, structural typing
- ``JsonSerializer``        — serialize/deserialize round-trips (primitives,
                              dict, list, Pydantic model), type_hint=None returns
                              raw object, invalid JSON raises
- ``NoOpSerializer``        — bytes pass-through, non-bytes raises TypeError,
                              type_hint ignored on deserialize
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest
from pydantic import BaseModel

from varco_core.serialization import JsonSerializer, NoOpSerializer, Serializer


# ── Serializer protocol ───────────────────────────────────────────────────────


class TestSerializerProtocol:
    def test_class_with_serialize_and_deserialize_satisfies(self) -> None:
        class MySerializer:
            def serialize(self, value: object) -> bytes:
                return b""

            def deserialize(self, data: bytes, type_hint: type | None = None) -> object:
                return None

        assert isinstance(MySerializer(), Serializer)

    def test_class_missing_serialize_does_not_satisfy(self) -> None:
        class Incomplete:
            def deserialize(self, data: bytes, type_hint: type | None = None) -> object:
                return None

        assert not isinstance(Incomplete(), Serializer)

    def test_json_serializer_satisfies_protocol(self) -> None:
        assert isinstance(JsonSerializer(), Serializer)

    def test_noop_serializer_satisfies_protocol(self) -> None:
        assert isinstance(NoOpSerializer(), Serializer)


# ── JsonSerializer ────────────────────────────────────────────────────────────


class TestJsonSerializer:
    @pytest.fixture
    def s(self) -> JsonSerializer:
        return JsonSerializer()

    def test_serialize_returns_bytes(self, s: JsonSerializer) -> None:
        data = s.serialize({"key": "value"})
        assert isinstance(data, bytes)

    def test_serialize_deserialize_dict_roundtrip(self, s: JsonSerializer) -> None:
        original = {"name": "Alice", "age": 30}
        data = s.serialize(original)
        back = s.deserialize(data)
        assert back == original

    def test_serialize_deserialize_list_roundtrip(self, s: JsonSerializer) -> None:
        original = [1, 2, 3, "four"]
        data = s.serialize(original)
        back = s.deserialize(data)
        assert back == original

    def test_serialize_int(self, s: JsonSerializer) -> None:
        assert s.deserialize(s.serialize(42)) == 42

    def test_serialize_float(self, s: JsonSerializer) -> None:
        assert abs(s.deserialize(s.serialize(3.14)) - 3.14) < 1e-6

    def test_serialize_string(self, s: JsonSerializer) -> None:
        assert s.deserialize(s.serialize("hello")) == "hello"

    def test_serialize_none(self, s: JsonSerializer) -> None:
        data = s.serialize(None)
        assert data == b"null"
        assert s.deserialize(data) is None

    def test_serialize_uuid_to_string(self, s: JsonSerializer) -> None:
        uid = uuid.uuid4()
        data = s.serialize(uid)
        # UUID serializes to a JSON string
        parsed = json.loads(data)
        assert parsed == str(uid)

    def test_serialize_datetime_to_iso(self, s: JsonSerializer) -> None:
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        data = s.serialize(dt)
        assert isinstance(json.loads(data), str)

    def test_serialize_empty_dict(self, s: JsonSerializer) -> None:
        assert s.deserialize(s.serialize({})) == {}

    def test_deserialize_with_type_hint_pydantic_model(self, s: JsonSerializer) -> None:
        class User(BaseModel):
            name: str
            age: int

        user = User(name="Alice", age=30)
        data = s.serialize(user)
        back = s.deserialize(data, type_hint=User)
        assert back == user
        assert isinstance(back, User)

    def test_deserialize_without_type_hint_returns_raw_object(
        self, s: JsonSerializer
    ) -> None:
        data = b'{"key": 42}'
        result = s.deserialize(data)
        assert isinstance(result, dict)
        assert result["key"] == 42

    def test_deserialize_invalid_json_raises_value_error(
        self, s: JsonSerializer
    ) -> None:
        with pytest.raises(Exception):  # ValueError or json.JSONDecodeError
            s.deserialize(b"NOT_JSON{{{")

    def test_repr(self, s: JsonSerializer) -> None:
        assert "JsonSerializer" in repr(s)


# ── NoOpSerializer ────────────────────────────────────────────────────────────


class TestNoOpSerializer:
    @pytest.fixture
    def s(self) -> NoOpSerializer:
        return NoOpSerializer()

    def test_serialize_returns_same_bytes(self, s: NoOpSerializer) -> None:
        data = b"raw bytes"
        assert s.serialize(data) is data

    def test_serialize_non_bytes_raises_type_error(self, s: NoOpSerializer) -> None:
        with pytest.raises(TypeError, match="bytes"):
            s.serialize("not bytes")  # type: ignore[arg-type]

    def test_serialize_int_raises_type_error(self, s: NoOpSerializer) -> None:
        with pytest.raises(TypeError):
            s.serialize(42)  # type: ignore[arg-type]

    def test_deserialize_returns_same_bytes(self, s: NoOpSerializer) -> None:
        data = b"some payload"
        assert s.deserialize(data) is data

    def test_deserialize_ignores_type_hint(self, s: NoOpSerializer) -> None:
        data = b"payload"
        # type_hint is ignored — always returns bytes
        result = s.deserialize(data, type_hint=str)
        assert result is data

    def test_deserialize_returns_bytes_unchanged(self, s: NoOpSerializer) -> None:
        raw = b"\x00\x01\x02\xff"
        assert s.deserialize(raw) == raw

    def test_repr(self, s: NoOpSerializer) -> None:
        assert "NoOpSerializer" in repr(s)
