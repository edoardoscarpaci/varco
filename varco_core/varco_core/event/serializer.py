"""
varco_core.event.serializer
============================
JSON serialization and deserialization for ``Event`` objects.

``JsonEventSerializer`` converts ``Event`` instances to UTF-8 JSON bytes
and back.  It is used by ``varco_kafka`` and ``varco_redis`` to serialize
events before sending them to the broker and to reconstruct typed ``Event``
objects when messages are received.

Serialization format
--------------------
Each serialized event is a flat JSON object.  An extra ``"__event_type__"``
key is injected at the top level to identify the event class during
deserialization::

    {
        "__event_type__": "entity.created",
        "event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "timestamp": "2025-01-01T00:00:00+00:00",
        "entity_type": "Post",
        "pk": "abc123",
        "correlation_id": null,
        "payload": {"title": "Hello"}
    }

The ``Event`` subclass must have been imported (so ``__init_subclass__``
fires) before ``deserialize`` is called.  In practice this means importing
your event modules at application start-up, which is normally done anyway
for FastAPI / DI wiring.

DESIGN: flat JSON over nested envelope
    ✅ Human-readable in Kafka/Redis inspector tools.
    ✅ ``model_dump(mode="json")`` handles nested Pydantic types, datetimes,
       and UUIDs correctly — no custom encoding step needed.
    ❌ ``__event_type__`` is a reserved top-level key — event classes must
       not declare a field named ``__event_type__`` (it's a ClassVar, so
       Pydantic already ignores it, but the name collision must be avoided
       in the JSON payload).

Thread safety:  ✅ Stateless — all methods are ``@classmethod``.
Async safety:   ✅ No I/O — pure CPU-bound serialization.
"""

from __future__ import annotations

import json
from typing import Any

from varco_core.event.base import Event


# ── JsonEventSerializer ────────────────────────────────────────────────────────


class JsonEventSerializer:
    """
    Serialize and deserialize ``Event`` objects to/from UTF-8 JSON bytes.

    Uses ``Event._registry`` (populated by ``Event.__init_subclass__``) for
    type-safe deserialization without a separate registration step.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ No I/O.

    Example::

        data  = JsonEventSerializer.serialize(my_event)
        event = JsonEventSerializer.deserialize(data)
        assert isinstance(event, type(my_event))
    """

    # Key injected into every serialized JSON object to carry the event type.
    # Must not clash with any Pydantic field name on Event subclasses.
    TYPE_KEY: str = "__event_type__"

    @classmethod
    def serialize(cls, event: Event) -> bytes:
        """
        Serialize ``event`` to UTF-8 JSON bytes.

        The output is a flat JSON object with an extra ``"__event_type__"``
        key prepended for deserialization.  All Pydantic field values are
        serialized using ``model_dump(mode="json")`` — datetimes become ISO
        strings, UUIDs become strings.

        Args:
            event: The event to serialize.

        Returns:
            UTF-8 encoded JSON bytes.

        Edge cases:
            - Events with nested Pydantic models in their fields are
              serialized recursively by Pydantic — no extra handling needed.
            - The ``timestamp`` field is serialized to ISO-8601 UTC string.
            - The ``event_id`` field is serialized to a UUID hex string.
        """
        # mode="json" ensures datetime/UUID fields are JSON-serializable
        # without a custom default encoder.
        payload: dict[str, Any] = {
            cls.TYPE_KEY: event.event_type_name(),
            **event.model_dump(mode="json"),
        }
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> Event:
        """
        Deserialize UTF-8 JSON bytes to a typed ``Event`` subclass instance.

        Looks up the event class in ``Event._registry`` using the
        ``"__event_type__"`` key embedded in the JSON.  The remaining JSON
        keys are passed to the event class constructor.

        Args:
            data: UTF-8 JSON bytes produced by ``serialize()``.

        Returns:
            A fully typed ``Event`` subclass instance.

        Raises:
            ValueError: If ``"__event_type__"`` is missing from the JSON.
            KeyError:   If the event type is not in ``Event._registry`` —
                        the event class was not imported before deserialization.
            ValidationError: (Pydantic) If the JSON fields don't match the
                             event class model.

        Edge cases:
            - If the producer and consumer are separate processes, the
              consumer must import all event modules before calling this
              method so that ``__init_subclass__`` fires and populates the
              registry.
            - ``"__event_type__"`` is stripped before Pydantic construction —
              it is not a field on any ``Event`` subclass.
        """
        raw: dict[str, Any] = json.loads(data.decode("utf-8"))

        # Extract the event type key — pop so it's not passed to the constructor
        event_type_name = raw.pop(cls.TYPE_KEY, None)
        if event_type_name is None:
            raise ValueError(
                f"Cannot deserialize event — '{cls.TYPE_KEY}' key is missing "
                f"from the JSON payload.  Was this produced by JsonEventSerializer?"
            )

        # Look up the registered class
        event_cls = Event._registry.get(event_type_name)
        if event_cls is None:
            known = sorted(Event._registry.keys())
            raise KeyError(
                f"Unknown event type {event_type_name!r} — "
                f"the event class was not imported before deserialization, "
                f"or the type name has changed.  "
                f"Known types ({len(known)}): {known}"
            )

        # Pydantic validates fields and applies any field-level coercions
        # (e.g. str → UUID, str → datetime).
        return event_cls.model_validate(raw)
