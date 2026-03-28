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

from dataclasses import dataclass

from varco_core.serialization import (
    JsonSerializer,
    NoOpSerializer,
    Serializer,
    TypedJsonSerializer,
)


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


# ── TypedJsonSerializer ───────────────────────────────────────────────────────


# Module-level fixtures — must be importable for the round-trip to work.
# Inner/local classes defined inside test methods cannot be re-imported by
# _import_class, so we define them here at module scope.
class _SampleModel(BaseModel):
    name: str
    value: int


@dataclass(frozen=True)
class _SampleDataclass:
    x: float
    y: float


class _PlainClass:
    """
    Plain Python class (not Pydantic, not dataclass) — serialized via __dict__.

    The type annotation on ``__init__`` is required so that Pydantic v2's
    ``TypeAdapter`` can reconstruct an instance from ``{"label": "..."}``
    without a schema.  Without annotations, ``PydanticSchemaGenerationError``
    is raised and deserialization falls back to a plain ``dict``.
    """

    def __init__(self, label: str) -> None:
        self.label = label

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _PlainClass) and self.label == other.label


class TestTypedJsonSerializer:
    """
    Tests for TypedJsonSerializer — self-describing envelope that embeds the
    class name so deserialization needs no type_hint.

    Sections:
    - Round-trips for Pydantic models, dataclasses, and plain classes
    - Primitives (int, str, list, None)
    - type_hint override takes precedence over embedded __type__
    - Backwards-compat: non-envelope bytes delegate to JsonSerializer
    - allow_modules rejects disallowed module prefixes
    - Satisfies the Serializer protocol
    - __repr__ includes class name
    """

    @pytest.fixture
    def s(self) -> TypedJsonSerializer:
        return TypedJsonSerializer()

    # ── Pydantic model round-trip ─────────────────────────────────────────────

    def test_pydantic_model_roundtrip_no_type_hint(
        self, s: TypedJsonSerializer
    ) -> None:
        original = _SampleModel(name="Alice", value=42)
        data = s.serialize(original)
        back = s.deserialize(data)
        # Must recover the correct type without any hint
        assert isinstance(back, _SampleModel)
        assert back == original

    def test_pydantic_model_envelope_contains_type_key(
        self, s: TypedJsonSerializer
    ) -> None:
        data = s.serialize(_SampleModel(name="Bob", value=1))
        envelope = json.loads(data)
        # __type__ must encode the full qualified path, __data__ the fields
        assert "__type__" in envelope
        assert "__data__" in envelope
        assert "test_serialization._SampleModel" in envelope["__type__"]

    # ── Frozen dataclass round-trip ───────────────────────────────────────────

    def test_dataclass_roundtrip_no_type_hint(self, s: TypedJsonSerializer) -> None:
        original = _SampleDataclass(x=1.5, y=2.5)
        data = s.serialize(original)
        back = s.deserialize(data)
        assert isinstance(back, _SampleDataclass)
        assert back == original

    # ── Plain class round-trip ────────────────────────────────────────────────

    def test_plain_class_serializes_with_type_embedded(
        self, s: TypedJsonSerializer
    ) -> None:
        # Plain classes without a Pydantic schema serialize via __dict__ and embed
        # __type__ in the envelope.  On deserialization, TypeAdapter raises
        # PydanticSchemaGenerationError — the serializer falls back gracefully
        # to returning the raw dict (data is preserved, type is lost).
        # This matches the documented limitation of JsonSerializer for the same
        # input — plain classes need a Pydantic model or dataclass for full
        # round-trip fidelity.
        original = _PlainClass(label="hello")
        data = s.serialize(original)
        envelope = json.loads(data)
        # __type__ must still be embedded in the envelope
        assert "__type__" in envelope
        assert "_PlainClass" in envelope["__type__"]
        # deserialization falls back to raw dict — data is intact
        back = s.deserialize(data)
        assert isinstance(back, dict)
        assert back["label"] == "hello"

    # ── Primitives ────────────────────────────────────────────────────────────

    def test_int_roundtrip(self, s: TypedJsonSerializer) -> None:
        data = s.serialize(99)
        back = s.deserialize(data)
        assert back == 99

    def test_str_roundtrip(self, s: TypedJsonSerializer) -> None:
        data = s.serialize("hello world")
        back = s.deserialize(data)
        assert back == "hello world"

    def test_list_roundtrip(self, s: TypedJsonSerializer) -> None:
        original = [1, 2, 3]
        data = s.serialize(original)
        back = s.deserialize(data)
        assert back == original

    def test_none_roundtrip(self, s: TypedJsonSerializer) -> None:
        data = s.serialize(None)
        back = s.deserialize(data)
        assert back is None

    # ── type_hint override ────────────────────────────────────────────────────

    def test_type_hint_overrides_embedded_type(self, s: TypedJsonSerializer) -> None:
        # Serialize as _SampleModel but deserialize with an explicit type_hint —
        # the embedded __type__ must be ignored in favour of the override.
        class AltModel(BaseModel):
            name: str
            value: int

        original = _SampleModel(name="Carol", value=7)
        data = s.serialize(original)
        # AltModel has the same schema as _SampleModel — override should succeed.
        back = s.deserialize(data, type_hint=AltModel)
        assert isinstance(back, AltModel)
        assert back.name == "Carol"
        assert back.value == 7

    # ── Backwards-compat: non-envelope bytes ─────────────────────────────────

    def test_non_envelope_bytes_delegate_to_json_serializer(
        self, s: TypedJsonSerializer
    ) -> None:
        # Bytes produced by the plain JsonSerializer have no __type__ key.
        # TypedJsonSerializer must handle these gracefully.
        raw = JsonSerializer().serialize({"key": "value"})
        result = s.deserialize(raw)
        # Without type_hint the result is a plain dict — same as JsonSerializer.
        assert result == {"key": "value"}

    def test_non_envelope_bytes_with_type_hint_delegates(
        self, s: TypedJsonSerializer
    ) -> None:
        raw = JsonSerializer().serialize({"name": "Dave", "value": 3})
        result = s.deserialize(raw, type_hint=_SampleModel)
        assert isinstance(result, _SampleModel)
        assert result.name == "Dave"

    # ── allow_modules allowlist ───────────────────────────────────────────────

    def test_allow_modules_permits_matching_prefix(self) -> None:
        # pytest imports test files as "tests.test_serialization" (not under
        # the varco_core package namespace), so the allowlist must match that
        # prefix — not "varco_core.".
        s = TypedJsonSerializer(allow_modules={"tests."})
        original = _SampleModel(name="Eve", value=5)
        data = s.serialize(original)
        back = s.deserialize(data)
        assert back == original

    def test_allow_modules_rejects_disallowed_prefix(self) -> None:
        # Only allow "safe." prefix — the test module ("varco_core.") must be rejected.
        s = TypedJsonSerializer(allow_modules={"safe."})
        data = s.serialize(_SampleModel(name="Mallory", value=0))
        with pytest.raises(ValueError, match="allow_modules"):
            s.deserialize(data)

    # ── Serializer protocol ───────────────────────────────────────────────────

    def test_satisfies_serializer_protocol(self, s: TypedJsonSerializer) -> None:
        assert isinstance(s, Serializer)

    # ── __repr__ ──────────────────────────────────────────────────────────────

    def test_repr_contains_class_name(self, s: TypedJsonSerializer) -> None:
        assert "TypedJsonSerializer" in repr(s)

    def test_repr_with_allow_modules(self) -> None:
        s = TypedJsonSerializer(allow_modules={"myapp."})
        assert "myapp." in repr(s)
