"""
varco_core.serialization
========================
General-purpose serialization protocol for varco.

Provides a pluggable ``Serializer[T]`` protocol used by both the event bus
backends (``varco_kafka``, ``varco_redis``) and the cache backends
(``varco_redis.cache``, ``varco_memcached``).

Separating serialization from the bus/cache implementations means:

- Backends are not tied to JSON — a user can plug in msgpack, protobuf, etc.
- ``JsonSerializer`` is stateless and safe to share across many components.
- ``NoOpSerializer`` enables raw-bytes usage without any encoding overhead.

DESIGN: Protocol over ABC for the serializer contract
    ✅ Structural typing — any object with the right method signatures satisfies
       the protocol without inheriting from a base class.  Third-party serializers
       (orjson, msgpack, protobuf) can be wrapped without subclassing.
    ✅ ``runtime_checkable`` enables ``isinstance`` guards in bus/cache code.
    ❌ Runtime ``isinstance`` only checks method *existence*, not signatures.
       Full safety requires a static type checker (mypy / pyright).
    ❌ No default implementations in the Protocol itself — callers that want
       behaviour should depend on ``JsonSerializer``, not ``Serializer`` directly.

Thread safety:  ✅ All built-in implementations are stateless — safe to share.
Async safety:   ✅ No I/O — serialization is synchronous and CPU-bound.

📚 Docs
- 🐍 https://docs.python.org/3/library/typing.html#typing.Protocol
  Protocol — structural subtyping (static duck typing)
- 🔍 https://docs.pydantic.dev/latest/concepts/serialization/
  Pydantic serialization — ``TypeAdapter``, ``model_dump``, ``dump_json``
"""

from __future__ import annotations

import json
from typing import Any, Protocol, TypeVar, runtime_checkable

from pydantic import TypeAdapter

# T is the Python type this serializer handles.
# Covariant would be ideal but Protocols with covariant type vars and
# methods that take T as input are not valid — invariant is correct here.
T = TypeVar("T")


# ── Serializer Protocol ────────────────────────────────────────────────────────


@runtime_checkable
class Serializer(Protocol[T]):
    """
    Structural protocol for pluggable value serializers.

    Any object that exposes ``serialize`` and ``deserialize`` with compatible
    signatures satisfies this protocol — no inheritance required.

    Type parameter:
        T: The Python type this serializer handles.

    Implementations must be stateless and thread-safe unless explicitly
    documented otherwise.

    DESIGN: ``type_hint`` is optional (``None`` default)
        Allows self-describing serializers (e.g. ``JsonEventSerializer`` that
        embeds the event type name in the JSON envelope) to implement the
        protocol without using the ``type_hint`` parameter.  Non-self-describing
        serializers (e.g. ``JsonSerializer`` for cache values) require
        ``type_hint`` to reconstruct the correct Python type.

    Thread safety:  ✅ Stateless by contract — implementations must not hold
                       mutable shared state.
    Async safety:   ✅ No I/O — serialization is synchronous.

    Example::

        s: Serializer[MyModel] = JsonSerializer()
        raw = s.serialize(my_model)
        back = s.deserialize(raw, type_hint=MyModel)
        assert back == my_model
    """

    def serialize(self, value: T) -> bytes:
        """
        Convert ``value`` to a ``bytes`` representation.

        Args:
            value: The object to serialize.

        Returns:
            Serialized bytes.

        Raises:
            TypeError:  If ``value`` contains a type that cannot be serialized.
            ValueError: If serialization fails for semantic reasons.
        """
        ...

    def deserialize(self, data: bytes, type_hint: type[T] | None = None) -> T:
        """
        Reconstruct a ``T`` from its ``bytes`` representation.

        Args:
            data:      Bytes previously produced by ``serialize()``.
            type_hint: The expected return type.  Required for generic or
                       structured types (e.g. Pydantic models, dataclasses).
                       May be ``None`` for self-describing formats where
                       the type is embedded in the data itself.

        Returns:
            A deserialized instance of type ``T``.

        Raises:
            ValueError:        If ``data`` is malformed or missing required keys.
            ValidationError:   (Pydantic) If ``type_hint`` is a Pydantic model
                               and the data does not match its schema.
        """
        ...


# ── JsonSerializer ────────────────────────────────────────────────────────────


class JsonSerializer:
    """
    Serializer that encodes values to UTF-8 JSON using Pydantic's ``TypeAdapter``.

    Handles any type supported by Pydantic's serialization layer, including
    ``BaseModel`` subclasses, dataclasses, primitives, ``datetime``, ``UUID``,
    ``list``, ``dict``, and arbitrarily nested combinations of the above.

    DESIGN: Pydantic TypeAdapter over ``json.dumps(default=...)``
        ✅ Handles Pydantic models, dataclasses, datetime, UUID automatically —
           no custom encoder function needed.
        ✅ Validates on deserialization — catches schema mismatches early.
        ✅ ``dump_json(mode="json")`` guarantees JSON-serializable output even
           for complex nested types.
        ❌ ``TypeAdapter(type(value))`` is constructed per-call — small overhead
           that is negligible for typical call rates.  Cache the adapter externally
           if calling this in a tight inner loop (>100k calls/second).

    Thread safety:  ✅ Stateless — each call constructs its own TypeAdapter.
    Async safety:   ✅ No I/O.

    Example::

        s = JsonSerializer()
        data = s.serialize(my_pydantic_model)
        back = s.deserialize(data, type_hint=type(my_pydantic_model))
        assert back == my_pydantic_model
    """

    def serialize(self, value: Any) -> bytes:
        """
        Serialize ``value`` to UTF-8 JSON bytes.

        Uses ``pydantic.TypeAdapter(type(value)).dump_json()`` — handles
        Pydantic models, dataclasses, datetimes, UUIDs, and primitives.

        Args:
            value: Any Pydantic-serializable Python value.

        Returns:
            UTF-8 JSON bytes.

        Raises:
            TypeError:  If ``value`` contains an unserializable type.
            ValueError: If Pydantic's serializer encounters a semantic error.

        Edge cases:
            - ``None`` → ``b"null"``
            - ``datetime`` → ISO-8601 string bytes.
            - ``UUID`` → hex string bytes.
            - Empty ``dict`` → ``b"{}"``
        """
        # TypeAdapter(type(value)) builds a per-call adapter — acceptable for
        # typical usage rates.  The adapter knows the full schema of the type
        # so nested models serialize correctly without a custom default hook.
        adapter: TypeAdapter[Any] = TypeAdapter(type(value))
        return adapter.dump_json(value)

    def deserialize(self, data: bytes, type_hint: type[Any] | None = None) -> Any:
        """
        Deserialize UTF-8 JSON bytes back to a Python value.

        Args:
            data:      UTF-8 JSON bytes produced by ``serialize()``.
            type_hint: The expected return type.  If ``None``, returns a raw
                       Python object (``dict``, ``list``, ``str``, etc.).
                       Provide a Pydantic model class to get a validated instance.

        Returns:
            Deserialized Python value — type matches ``type_hint`` when provided.

        Raises:
            ValueError:      If ``data`` is not valid JSON.
            ValidationError: (Pydantic) If ``type_hint`` is a Pydantic model and
                             the data does not conform to its schema.

        Edge cases:
            - ``type_hint=None`` → falls back to ``json.loads`` → plain dicts/lists.
            - ``b"null"`` with ``type_hint=None`` → returns Python ``None``.
        """
        if type_hint is None:
            # No schema to validate against — return raw Python objects.
            # json.loads is used directly here since TypeAdapter(Any) does the
            # same thing but with unnecessary overhead.
            return json.loads(data)

        # TypeAdapter validates the parsed JSON against the provided schema,
        # coercing types (e.g. str → UUID, str → datetime) as needed.
        adapter: TypeAdapter[Any] = TypeAdapter(type_hint)
        return adapter.validate_json(data)

    def __repr__(self) -> str:
        return "JsonSerializer()"


# ── NoOpSerializer ────────────────────────────────────────────────────────────


class NoOpSerializer:
    """
    Pass-through serializer for values that are already serialized ``bytes``.

    ``serialize`` returns its input unchanged; ``deserialize`` returns the
    raw ``bytes`` unchanged.  Useful when:

    - The value is already in bytes form (pre-serialized payloads).
    - You want to bypass any encoding layer entirely.
    - Benchmarking without serialization overhead.

    DESIGN: identity transform
        ✅ Zero overhead — no encoding or decoding performed.
        ❌ Only safe for ``bytes`` values — ``serialize(non_bytes)`` raises
           ``TypeError`` immediately to surface the mistake.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ No I/O.

    Edge cases:
        - ``serialize(non_bytes)`` raises ``TypeError`` — intentional guard.
          The caller must ensure the value is already bytes.
        - ``deserialize`` always returns the raw bytes regardless of ``type_hint``.
          ``type_hint`` is accepted only for API compatibility and is ignored.
    """

    def serialize(self, value: bytes) -> bytes:
        """
        Return ``value`` unchanged.

        Args:
            value: Must be ``bytes``.

        Returns:
            The same ``bytes`` object.

        Raises:
            TypeError: If ``value`` is not ``bytes``.
        """
        if not isinstance(value, bytes):
            raise TypeError(
                f"NoOpSerializer.serialize() expects bytes, "
                f"got {type(value).__name__!r}. "
                f"Use JsonSerializer for non-bytes values."
            )
        return value

    def deserialize(self, data: bytes, type_hint: type[Any] | None = None) -> bytes:
        """
        Return ``data`` unchanged.

        Args:
            data:      Raw bytes.
            type_hint: Accepted for API compatibility — always ignored.

        Returns:
            The same ``bytes`` object passed in.
        """
        # type_hint is intentionally ignored — this serializer is a pure passthrough.
        _ = type_hint
        return data

    def __repr__(self) -> str:
        return "NoOpSerializer()"


__all__ = [
    "Serializer",
    "JsonSerializer",
    "NoOpSerializer",
]
