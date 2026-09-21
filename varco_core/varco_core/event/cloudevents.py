"""
varco_core.event.cloudevents
=============================
CloudEvents v1.0.2 **structured-mode** envelope for ``Event`` objects.

This module ships a *second* implementation of ``Serializer[Event]`` — the seam
``varco_core/event/serializer.py`` already documents — so an application can put
every event it publishes inside a spec-compliant CloudEvents JSON envelope by
binding one object.  Nothing about ``Event``, the buses, the DLQ, the outbox or
the audit trail changes (Plan 022 §D-CE1, reserved-seams RS-1).

Opt in explicitly — never auto-active::

    from providify import DIContainer
    from varco_core.event.cloudevents import (
        CloudEventsSettings,
        bind_cloudevents_serializer,
    )

    container = DIContainer()
    bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/orders"))

or construct it directly and hand it to a bus::

    serializer = CloudEventsJsonSerializer(CloudEventsSettings(source="/svc/orders"))
    bus = KafkaEventBus(settings, serializer=serializer)

Wire format (structured mode, ``application/cloudevents+json``)::

    {
        "specversion": "1.0",
        "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "source": "/svc/orders",
        "type": "order.placed",
        "time": "2026-01-01T00:00:00+00:00",
        "datacontenttype": "application/json",
        "correlationid": "corr-1",          # extension, only when the event has one
        "tenantid": "acme",                 # extension, best-effort — see below
        "data": {"order_id": "o-1", "total": 9.5}
    }

DESIGN: a second ``Serializer[Event]`` over changing ``Event``
    ✅ Zero change to ``Event`` — no field added, no ``model_dump()`` shape moved,
       so no DLQ / outbox / audit consumer sees a byte move.
    ✅ Zero change to any bus's *behaviour* — every bus accepts a ``serializer=``
       constructor kwarg and resolves ``Serializer[Event]`` optionally through DI,
       so binding is the whole opt-in on Kafka, NATS and both Redis shapes.
       (Redis needed one wiring fix to get there: its two implementations come
       from a ``@Provider``, and providify injects only what the provider *method*
       declares — see ``RedisEventBusSelectorConfiguration.bus()``.)
    ✅ Coverage is uniform across buses, all five DLQ backends and the outbox:
       each takes the serializer as a parameter rather than constructing
       ``JsonEventSerializer()`` as a literal, so dead letters and outbox rows are
       stored in the same wire format the bus publishes.
    ✅ Reversible per deployment: rebind and restart.
    ❌ A CloudEvents-serialized and a native-serialized event cannot share a
       channel mid-migration unless the consumer sniffs.  Mitigation is the
       three-phase dual-emit rollout in
       ``technical_docs/features/cloudevents-envelope.md``.
    Rejected — CloudEvents fields on ``Event`` itself: ❌ every event in every app
    changes shape and ``source`` has no correct process-wide default.
    Rejected — a per-bus ``cloudevents=True`` flag: ❌ three settings classes and
    three code paths duplicating a DI mechanism that already exists.

DESIGN: hand-rolled, **no** ``cloudevents`` SDK dependency (§D-CE3 / §D-N2-sdk)
    ✅ ``varco_core`` takes no new runtime dependency — the standing repo rule.
    ✅ The CNCF SDK carries its own "breaking changes possible with every update"
       disclaimer (research 005 §3); depending on it to avoid ~120 lines against a
       spec stable since 2022 is the wrong trade in both directions.
    ❌ varco owns spec compliance forever.  Mitigated: CloudEvents v1.0.2 is CNCF
       *Graduated* and new optional attributes arrive only in MINOR versions.

DESIGN: no module-level ``@Singleton`` / ``@Provider`` decorator
    ✅ ``container.scan("varco_core", recursive=True)`` is a documented, in-use
       pattern that auto-registers every decorated class *and every module-level*
       ``@Provider`` function (providify ``scanner.py:_scan_module``).  A decorator
       here would silently change the wire bytes of every app that scans
       ``varco_core`` — the exact opposite of §D-CE1's "opt-in, never auto-active".
    ✅ ``bind_cloudevents_serializer()`` registers a non-fallback binding,
       which therefore beats ``JsonEventSerializer``'s ``@Fallback`` default
       in any registration order.
    ❌ One more wiring call for the adopter.  Accepted: an explicit call is the
       only honest way to say "change my wire format".

⚠️ **``tenantid`` is best-effort by design.**  It is read from
``current_tenant()`` — CLAUDE.md's single source of truth — and a serializer runs
on whatever task the publish happens on, so an ``OutboxRelay``-driven publish has
no ambient tenant and emits no ``tenantid``.  Do **not** "fix" this by adding a
tenant field to ``Event``.

⚠️ **Structured mode only.**  CloudEvents *binary* mode needs a header channel,
and ``AbstractEventBus.publish()`` is promised never to gain ``headers=``
(reserved-seams RS-2); binary mode arrives later through a separate
``MessageEncoder`` Protocol (§D-CE2).  A consequence is that the Kafka binding's
``content-type: application/cloudevents+json`` header is *not* set today — the
message body is spec-correct, the transport header is not reachable.

Thread safety:  ✅ Stateless apart from a frozen settings object.
Async safety:   ✅ No I/O — pure CPU-bound (de)serialization.  ``current_tenant()``
                   is a ``ContextVar`` read, correct on any task.

📚 Docs
- 🌩️ https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
  CloudEvents v1.0.2 — context attributes and extension naming rules
- 🌩️ https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/formats/json-format.md
  JSON Event Format — structured mode, ``data`` vs ``data_base64``
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Final

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# DESIGN: ``Event`` imported at RUNTIME, not under TYPE_CHECKING — identical
# reasoning to serializer.py: ``Serializer[Event]`` is a DI binding interface and
# appears in bus constructor annotations, so ``Event`` must be a real object.
from varco_core.event.base import Event
from varco_core.serialization import Serializer

if TYPE_CHECKING:  # pragma: no cover — typing only
    from providify import DIContainer

# ── Constants ──────────────────────────────────────────────────────────────────

#: The one legal ``specversion`` value for this release of the spec.
CLOUDEVENTS_SPEC_VERSION: Final[str] = "1.0"

#: Media type of a structured-mode CloudEvent.  Documented for adopters wiring a
#: transport that *can* carry a content type; varco never sets a header itself
#: (RS-2 — ``publish()`` gains no ``headers=``).
CLOUDEVENTS_CONTENT_TYPE: Final[str] = "application/cloudevents+json"

#: varco's own named, versioned Redis Streams convention (§D-CE4 convention 1):
#: the **whole** envelope occupies a single stream field called ``ce``, never one
#: field per CloudEvents attribute.  No official AsyncAPI/CloudEvents Redis
#: binding exists, so varco defines one — see
#: ``technical_docs/features/cloudevents-envelope.md``.
CLOUDEVENTS_STREAM_FIELD: Final[str] = "ce"

#: Extension-attribute naming rule: lowercase ASCII letters and digits only, and
#: a *recommended* maximum of 20 characters (research 005 §3).  Enforced at
#: serialize time so an invented extension cannot ship an illegal name.
_EXTENSION_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9]{1,20}$")

# Envelope-owned event fields.  They live in the envelope as ``id`` / ``time``
# and must not be duplicated inside ``data``.
_ENVELOPE_OWNED_FIELDS: Final[tuple[str, ...]] = ("event_id", "timestamp")

# ── Settings ───────────────────────────────────────────────────────────────────


class CloudEventsSettings(BaseSettings):
    """
    Configuration for :class:`CloudEventsJsonSerializer`.

    ``source`` is **required and has no default**.  There is no correct default
    for "who am I": the spec makes ``source`` + ``id`` the uniqueness key for an
    event, so a shared placeholder such as ``"varco"`` would make two unrelated
    services collide.  Construction fails loudly instead (§D-CE4).

    Attributes:
        source:          Non-empty URI-reference identifying the producer,
                         e.g. ``"/svc/orders"`` or
                         ``"https://orders.example.com"``.  Env:
                         ``VARCO_CLOUDEVENTS_SOURCE``.
        datacontenttype: Media type of ``data``.  Defaults to
                         ``"application/json"`` and must end in ``json`` or
                         ``+json`` — see the validator.  Env:
                         ``VARCO_CLOUDEVENTS_DATACONTENTTYPE``.

    Raises:
        ValidationError: If ``source`` is missing or empty, or if
                         ``datacontenttype`` is not a JSON media type.

    Example::

        CloudEventsSettings(source="/svc/orders")
        CloudEventsSettings()                      # ValidationError — no source
    """

    model_config = SettingsConfigDict(
        env_prefix="VARCO_CLOUDEVENTS_",
        extra="ignore",
    )

    source: str = Field(min_length=1)
    datacontenttype: str = "application/json"

    @field_validator("datacontenttype")
    @classmethod
    def _must_be_a_json_media_type(cls, value: str) -> str:
        """
        Reject any media type that would require ``data_base64``.

        The JSON Event Format makes the choice normative: ``data`` when
        ``datacontenttype`` ends in ``json`` or ``+json``, ``data_base64``
        otherwise — and the two are mutually exclusive (research 005 §3).  varco
        emits ``data`` only, so a non-JSON content type is a configuration error
        rather than a silent spec violation.

        Args:
            value: The configured media type.

        Returns:
            The value unchanged.

        Raises:
            ValueError: If the media type does not end in ``json``/``+json``.
        """
        if not _is_json_media_type(value):
            raise ValueError(
                f"datacontenttype must end in 'json' or '+json' (got {value!r}). "
                "varco's CloudEvents serializer emits structured `data` only; "
                "`data_base64` (binary payloads) is not supported."
            )
        return value


def _is_json_media_type(value: str) -> bool:
    """
    Return ``True`` when *value* selects the ``data`` member, not ``data_base64``.

    Args:
        value: A media type such as ``"application/json"``.

    Returns:
        ``True`` if the media type ends in ``json`` or ``+json``.
    """
    stripped = value.split(";", 1)[0].strip().lower()
    return stripped.endswith(("json", "+json"))


# ── Serializer ─────────────────────────────────────────────────────────────────


class CloudEventsJsonSerializer(Serializer[Event]):
    """
    Encode/decode ``Event`` objects as CloudEvents v1.0.2 structured JSON.

    Attribute mapping (§D-CE4):

    ========================  ===================================================
    CloudEvents attribute     varco source
    ========================  ===================================================
    ``specversion``           the literal ``"1.0"``
    ``id``                    ``Event.event_id`` (UUID → string)
    ``source``                ``CloudEventsSettings.source``
    ``type``                  ``Event.event_type_name()``
    ``time``                  ``Event.timestamp`` (RFC 3339, aware UTC)
    ``datacontenttype``       ``CloudEventsSettings.datacontenttype``
    ``data``                  ``model_dump(mode="json")`` minus id/timestamp
    ``correlationid``         ``event.correlation_id`` when the event has one
    ``tenantid``              ``current_tenant()`` when a tenant is ambient
    ========================  ===================================================

    ``correlationid`` and ``tenantid`` are *extension* attributes: lowercase
    ASCII alphanumerics, no underscores or hyphens.  No registered CloudEvents
    extension covers either concept (research 005 §3), so varco defines these two
    names and validates them against the spec's rule at serialize time.

    Attributes:
        stream_field: The Redis Streams field name a bus should use for the whole
                      envelope (``"ce"``).  Read duck-typed by
                      ``varco_redis.streams.RedisStreamEventBus`` — a bus with no
                      stream concept simply ignores it.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ No I/O.

    Example::

        serializer = CloudEventsJsonSerializer(CloudEventsSettings(source="/svc"))
        wire = serializer.serialize(OrderPlaced(order_id="o-1"))
        assert json.loads(wire)["specversion"] == "1.0"
        event = serializer.deserialize(wire)
    """

    #: See ``CLOUDEVENTS_STREAM_FIELD`` — varco's named Redis Streams convention.
    stream_field: ClassVar[str] = CLOUDEVENTS_STREAM_FIELD

    def __init__(self, settings: CloudEventsSettings) -> None:
        """
        Args:
            settings: Producer identity and content type.  Required — there is
                      no zero-argument construction, because ``source`` has no
                      correct default.
        """
        self._settings = settings

    # ── serialize ──────────────────────────────────────────────────────────────

    def serialize(self, value: Event) -> bytes:
        """
        Serialize *value* into a structured-mode CloudEvents JSON envelope.

        Args:
            value: The event to serialize.

        Returns:
            UTF-8 encoded JSON bytes — the whole envelope, which is exactly what
            the broker carries (and exactly what a dead letter stores, see the
            feature doc's DLQ section).

        Raises:
            ValueError: If an extension attribute name would violate the spec's
                        naming rule, or if ``datacontenttype`` is not a JSON
                        media type (which would require ``data_base64``).

        Edge cases:
            - ``event_id``/``timestamp`` are stripped from ``data`` — they are
              carried by the envelope's ``id``/``time`` and duplicating them
              would let the two copies disagree.
            - A naive ``timestamp`` is interpreted as UTC: RFC 3339 requires an
              offset, and every varco ``Event`` defaults to aware UTC anyway.
            - ``tenantid`` is emitted only when ``current_tenant()`` is set —
              absent under an ``OutboxRelay``-driven publish, by design.
        """
        content_type = self._settings.datacontenttype
        # Assert rather than assume (research 005 §3): the settings validator
        # already enforces this, but a future configurable content type must not
        # silently produce an envelope with `data` where the spec wants
        # `data_base64`.
        if not _is_json_media_type(content_type):
            raise ValueError(
                f"datacontenttype {content_type!r} requires `data_base64`, which "
                "this serializer does not emit."
            )

        data: dict[str, Any] = value.model_dump(mode="json")
        for owned in _ENVELOPE_OWNED_FIELDS:
            data.pop(owned, None)

        envelope: dict[str, Any] = {
            "specversion": CLOUDEVENTS_SPEC_VERSION,
            "id": str(value.event_id),
            "source": self._settings.source,
            "type": value.event_type_name(),
            "time": _to_rfc3339(value.timestamp),
            "datacontenttype": content_type,
        }

        for name, extension in self._extensions(value).items():
            if not _EXTENSION_NAME_RE.match(name):
                raise ValueError(
                    f"illegal CloudEvents extension attribute name {name!r} — "
                    "lowercase ASCII letters and digits only, at most 20 characters"
                )
            envelope[name] = extension

        # `data` last so the envelope reads attributes-then-payload, and so a
        # malformed extension can never leave a half-built payload behind.
        envelope["data"] = data
        return json.dumps(envelope, ensure_ascii=False).encode("utf-8")

    def _extensions(self, value: Event) -> dict[str, str]:
        """
        Collect the extension attributes for *value*.

        Args:
            value: The event being serialized.

        Returns:
            A mapping of extension name → string value.  Empty when the event
            carries no correlation id and no tenant is ambient.

        Edge cases:
            - ``correlation_id`` is read with ``getattr`` because it is a field
              some ``Event`` subclasses declare and the base class does not.
            - Both values are stringified: CloudEvents extension values are
              typed, and ``String`` is the only type varco needs here.
        """
        # Imported at call time: `service.tenant` pulls in the service layer,
        # which must never be a module-scope dependency of the event package.
        from varco_core.service.tenant import current_tenant  # noqa: PLC0415

        extensions: dict[str, str] = {}

        correlation_id = getattr(value, "correlation_id", None)
        if correlation_id is not None:
            extensions["correlationid"] = str(correlation_id)

        tenant_id = current_tenant()
        if tenant_id is not None:
            extensions["tenantid"] = str(tenant_id)

        return extensions

    # ── deserialize ────────────────────────────────────────────────────────────

    def deserialize(
        self,
        data: bytes,
        type_hint: type[Event] | None = None,
    ) -> Event:
        """
        Reconstruct an ``Event`` from a structured-mode CloudEvents envelope.

        The event class is looked up in ``Event._registry`` by the envelope's
        ``type``, exactly as ``JsonEventSerializer`` looks it up by
        ``__event_type__`` — so the producing module must have been imported.

        Args:
            data:      UTF-8 JSON bytes produced by :meth:`serialize`.
            type_hint: Ignored.  Accepted for ``Serializer[Event]`` protocol
                       compatibility only — the envelope is self-describing.

        Returns:
            A fully typed ``Event`` subclass instance whose ``event_id`` and
            ``timestamp`` are restored from the envelope's ``id``/``time``.

        Raises:
            ValueError: If ``specversion`` is missing or unsupported, if ``type``
                        is missing, or if the envelope carries ``data_base64``.
            KeyError:   If the event type is not registered — the event class was
                        not imported before deserialization.
            ValidationError: (Pydantic) If ``data`` does not match the model.

        Edge cases:
            - Extension attributes (``tenantid``, ``correlationid``) are *not*
              written back onto the model: ``tenantid`` is ambient metadata and
              is not a field, and ``correlation_id`` — when the event declares
              one — already round-trips inside ``data``.
            - An envelope with no ``data`` member deserializes to an event with
              only its defaults, which is spec-legal (``data`` is OPTIONAL).
        """
        _ = type_hint  # self-describing envelope — the hint is never consulted

        envelope: dict[str, Any] = json.loads(data.decode("utf-8"))

        spec_version = envelope.get("specversion")
        if spec_version != CLOUDEVENTS_SPEC_VERSION:
            raise ValueError(
                f"unsupported CloudEvents specversion {spec_version!r} — "
                f"expected {CLOUDEVENTS_SPEC_VERSION!r}"
            )
        if "data_base64" in envelope:
            raise ValueError(
                "CloudEvents `data_base64` (binary payload) is not supported — "
                "varco emits and consumes structured JSON `data` only."
            )

        event_type_name = envelope.get("type")
        if not event_type_name:
            raise ValueError(
                "Cannot deserialize CloudEvent — the required `type` attribute "
                "is missing.  Was this produced by CloudEventsJsonSerializer?"
            )

        event_cls = Event._registry.get(event_type_name)
        if event_cls is None:
            known = sorted(Event._registry.keys())
            raise KeyError(
                f"Unknown event type {event_type_name!r} — "
                f"the event class was not imported before deserialization, "
                f"or the type name has changed.  "
                f"Known types ({len(known)}): {known}"
            )

        payload: dict[str, Any] = dict(envelope.get("data") or {})
        # Restore the two envelope-owned fields; the envelope is authoritative
        # for both, and both are ordinary model fields on the way back in.
        if (event_id := envelope.get("id")) is not None:
            payload["event_id"] = event_id
        if (occurred_at := envelope.get("time")) is not None:
            payload["timestamp"] = occurred_at

        return event_cls.model_validate(payload)

    def __repr__(self) -> str:
        return f"CloudEventsJsonSerializer(source={self._settings.source!r})"


def _to_rfc3339(value: datetime) -> str:
    """
    Render *value* as an RFC 3339 timestamp with an explicit offset.

    Args:
        value: The event timestamp.  Aware UTC for every varco ``Event``.

    Returns:
        An ISO-8601/RFC-3339 string, e.g. ``"2026-01-01T00:00:00+00:00"``.

    Edge cases:
        - A naive datetime is interpreted as UTC rather than rejected: RFC 3339
          requires an offset, and silently dropping the attribute would be worse
          than assuming the one timezone varco ever stores.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()


# ── Opt-in DI wiring ───────────────────────────────────────────────────────────


def bind_cloudevents_serializer(
    container: DIContainer,
    settings: CloudEventsSettings,
) -> None:
    """
    Bind the CloudEvents serializer (and its settings) into *container*.

    This is the supported opt-in path.  Both registrations use
    ``container.provide()`` — a **provider**, never ``@Singleton`` — which is
    also the rule for any pydantic ``BaseSettings`` class (CLAUDE.md; providify
    cannot inject pydantic's ``**values`` constructor).

    After this call every bus that *resolves* ``Serializer[Event]`` gets the
    CloudEvents envelope, because this binding is a non-fallback and
    ``JsonEventSerializer`` is declared ``@Fallback``.

    Every bus resolves it: ``KafkaEventBus`` and ``NatsEventBus`` inject it as
    scanned singletons, and ``RedisEventBus``/``RedisStreamEventBus`` receive it
    from ``RedisEventBusSelectorConfiguration.bus()``, which forwards the binding
    explicitly (providify injects only what a ``@Provider`` method declares).
    The three ``@Configuration``-wired DLQs forward it the same way.

    Args:
        container: The DI container to mutate.
        settings:  Producer identity — construct it explicitly so a missing
                   ``source`` fails at wiring time, not at first publish.

    Returns:
        ``None`` — the container is mutated in place (``bind_*`` verb, see
        CLAUDE.md's DI wiring verb taxonomy).

    Edge cases:
        - Calling this twice registers two bindings; the last one wins at the
          same priority.  Call it once, at bootstrap.
        - It changes the **wire format** of every event the app publishes on a
          bus that resolves ``Serializer[Event]``.  Roll it out with the
          three-phase dual-emit plan in
          ``technical_docs/features/cloudevents-envelope.md``.
        - Dead letters follow the bus.  Each ``AbstractDeadLetterQueue``
          implementation re-serializes ``DeadLetterEntry.event`` with the
          serializer it was given, so a redrive republishes the same wire format
          the app publishes.  A DLQ populated *before* the swap still holds the
          old format — drain its backlog first (the feature doc's migration
          timeline covers the ordering).
        - ``SADeadLetterQueue`` and ``OutboxRelay`` are constructed by hand, not
          by a ``@Configuration``; pass ``serializer=`` explicitly to keep them
          in step with the bus.

    Example::

        container = DIContainer()
        bind_cloudevents_serializer(
            container, CloudEventsSettings(source="/svc/orders")
        )
    """
    serializer = CloudEventsJsonSerializer(settings)

    # `returns=` is providify's native interface override (>= 2.0.0) — required
    # because a lambda/closure has no useful return annotation to read.
    container.provide(lambda: settings, returns=CloudEventsSettings)
    container.provide(lambda: serializer, returns=Serializer[Event])


__all__ = [
    "CLOUDEVENTS_CONTENT_TYPE",
    "CLOUDEVENTS_SPEC_VERSION",
    "CLOUDEVENTS_STREAM_FIELD",
    "CloudEventsJsonSerializer",
    "CloudEventsSettings",
    "bind_cloudevents_serializer",
]
