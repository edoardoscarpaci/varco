"""
varco_core.event.serializer
============================
Serialization contract and JSON implementation for ``Event`` objects.

``Serializer[Event]`` (from ``varco_core.serialization``) is the contract that
defines how events are encoded to bytes for Kafka/Redis/NATS transport and
decoded back.  Bus backends annotate their ``serializer`` constructor parameter
with that interface, so any implementation can be plugged in via DI.

``JsonEventSerializer`` is the default implementation.  It subclasses
``Serializer[Event]`` explicitly (rather than relying on structural Protocol
satisfaction) so providify registers it under the interface, and carries
``@Fallback`` — the in-process default, so it is available out of the box but
any application binding of ``Serializer[Event]`` wins regardless of
registration order (`technical_docs/features/dependency-injection.md`)::

    @Provider(singleton=True)
    def my_serializer() -> Serializer[Event]:
        return MyCompactSerializer()

    container.provide(my_serializer)      # wins over JsonEventSerializer

⚠️ There is deliberately **no** ``EventSerializer`` type alias.  It previously
existed as a quoted forward reference (``EventSerializer: TypeAlias =
"Serializer[Event]"``), which bound the *string* to the module-level name at
runtime and made every ``EventSerializer | None`` annotation unevaluatable —
silently disabling DI for the parameter on all three bus backends.  Annotate
with ``Serializer[Event]`` directly.

The default implementation produces a flat
JSON object with an extra ``"__event_type__"`` key injected at the top level
to identify the event class during deserialization.

Serialization format
--------------------
::

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
fires and the class registers itself) before ``deserialize`` is called.
In practice this means importing your event modules at application start-up,
which is normally done anyway for FastAPI / DI wiring.

DESIGN: flat JSON over nested envelope
    ✅ Human-readable in Kafka/Redis inspector tools.
    ✅ ``model_dump(mode="json")`` handles nested Pydantic types, datetimes,
       and UUIDs correctly — no custom encoding step needed.
    ❌ ``__event_type__`` is a reserved top-level key — event classes must
       not declare a field named ``__event_type__`` (it's a ClassVar, so
       Pydantic already ignores it, but the name collision must be avoided
       in the JSON payload).

DESIGN: instance methods over classmethods
    ✅ Satisfies the ``Serializer[Event]`` Protocol — Protocol methods are
       instance methods.  Classmethods would require a separate Protocol
       definition and break structural compatibility.
    ✅ Enables stateful serializers as drop-in replacements (e.g. a serializer
       that caches ``TypeAdapter`` instances).
    ✅ Consistent with ``JsonSerializer`` in ``varco_core.serialization``.
    ❌ Callers must hold an instance — the old classmethod API no longer works.
       Migration: replace ``JsonEventSerializer.serialize(e)`` with
       ``JsonEventSerializer().serialize(e)`` or inject an instance.

Thread safety:  ✅ Stateless — all instance state is read-only (TYPE_KEY).
Async safety:   ✅ No I/O — pure CPU-bound serialization.

📚 Docs
- 🐍 https://docs.python.org/3/library/typing.html#typing.Protocol
  Protocol — structural subtyping
- 🔍 https://docs.pydantic.dev/latest/concepts/serialization/
  Pydantic model_dump and TypeAdapter serialization
"""

from __future__ import annotations

import json
from typing import Any

from providify import Fallback, Singleton

# DESIGN: ``Event`` is imported at RUNTIME, not under ``TYPE_CHECKING``.
#
# ``Serializer[Event]`` is used as a DI binding interface and appears in bus
# constructor annotations as ``Serializer[Event] | None``.  Both uses require
# ``Event`` to be a real type object at runtime: a ``TYPE_CHECKING``-only import
# forces the reference to be quoted, and a quoted name evaluates to a ``str``,
# making ``… | None`` a TypeError that silently disables injection.
#
#   ✅ ``get_type_hints()`` (and therefore providify's binding validation) can
#      resolve every annotation mentioning ``Serializer[Event]``.
#   ✅ No import cycle: ``varco_core.event.base`` does not import this module.
#   ❌ Importing this module now also loads ``base.py`` — acceptable, since all
#      three bus backends already import ``base.py`` at module scope anyway.
from varco_core.event.base import Event
from varco_core.serialization import Serializer

# ── JsonEventSerializer ────────────────────────────────────────────────────────


# DESIGN: minimum priority so this default always loses to an app-supplied
# serializer registered at the default priority.
#   ✅ Works out of the box — no wiring needed for the common case.
#   ✅ Fully overridable — `container.provide(my_serializer)` wins, in any order.
#   ❌ Registers a binding even in apps that never use the event system (cheap:
#      the instance is only constructed on first resolution).
@Fallback
@Singleton
class JsonEventSerializer(Serializer[Event]):
    """
    Serialize and deserialize ``Event`` objects to/from UTF-8 JSON bytes.

    Uses ``Event._registry`` (populated by ``Event.__init_subclass__``) for
    type-safe deserialization without a separate registration step.  The event
    type name is embedded in the JSON as ``"__event_type__"`` so the class can
    be looked up without the caller providing a ``type_hint``.

    This class implements the ``Serializer[Event]`` Protocol, subclassing it
    explicitly so DI can bind it under that interface.  The ``type_hint``
    parameter of ``deserialize`` is
    accepted for API compatibility but ignored — the event type is always
    derived from the embedded ``"__event_type__"`` key.

    DESIGN: self-describing JSON via __event_type__ key
        ✅ No type hint needed at deserialization time — type is in the payload.
        ✅ Readable in any Kafka/Redis inspector tool.
        ❌ Every event class must be imported before deserialization — the
           ``__init_subclass__`` registry only fires on import.

    Thread safety:  ✅ Stateless — ``TYPE_KEY`` is a class-level constant.
    Async safety:   ✅ No I/O.

    Example::

        serializer = JsonEventSerializer()
        data  = serializer.serialize(my_event)
        event = serializer.deserialize(data)
        assert isinstance(event, type(my_event))
    """

    # Key injected into every serialized JSON object to carry the event type.
    # Must not clash with any Pydantic field name on Event subclasses.
    TYPE_KEY: str = "__event_type__"

    def serialize(self, value: Event) -> bytes:
        """
        Serialize ``event`` to UTF-8 JSON bytes.

        The output is a flat JSON object with an extra ``"__event_type__"``
        key prepended for deserialization.  All Pydantic field values are
        serialized using ``model_dump(mode="json")`` — datetimes become ISO
        strings, UUIDs become strings.

        Args:
            value: The event to serialize.

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
            self.TYPE_KEY: value.event_type_name(),
            **value.model_dump(mode="json"),
        }
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def deserialize(
        self,
        data: bytes,
        type_hint: type[Event] | None = None,
    ) -> Event:
        """
        Deserialize UTF-8 JSON bytes to a typed ``Event`` subclass instance.

        Looks up the event class in ``Event._registry`` using the
        ``"__event_type__"`` key embedded in the JSON.  The remaining JSON
        keys are passed to the event class constructor.

        ``type_hint`` is accepted for ``Serializer[Event]`` Protocol compliance
        but is always ignored — the type is derived from the JSON payload.

        Args:
            data:      UTF-8 JSON bytes produced by ``serialize()``.
            type_hint: Ignored.  Accepted for ``Serializer[Event]`` protocol
                       compatibility only.

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
            - ``type_hint`` is silently ignored — the JSON is always
              self-describing via ``"__event_type__"``.
        """
        # Import at call time to avoid a circular import at module level:
        # serializer.py → base.py → (no circular), but keeping the import
        # local keeps the module-level dependency graph clean.
        from varco_core.event.base import Event  # noqa: PLC0415

        # type_hint is intentionally ignored — type is embedded in the payload.
        _ = type_hint

        raw: dict[str, Any] = json.loads(data.decode("utf-8"))

        # Extract the event type key — pop so it's not passed to the constructor
        event_type_name = raw.pop(self.TYPE_KEY, None)
        if event_type_name is None:
            raise ValueError(
                f"Cannot deserialize event — '{self.TYPE_KEY}' key is missing "
                f"from the JSON payload.  Was this produced by JsonEventSerializer?"
            )

        # Look up the registered class in the shared Event registry
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

    def __repr__(self) -> str:
        return "JsonEventSerializer()"


__all__ = [
    "JsonEventSerializer",
]
