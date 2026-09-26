"""
Unit tests for the CloudEvents structured-mode serializer (Plan 030 / Phase 0, N2).
===================================================================================

RED-MODE TDD: these tests are written *before* ``varco_core/event/cloudevents.py``
exists.  They encode plan 030 Step 4 and its prior design
(``plans/022-api-freeze-and-standards-alignment.md`` §D-CE1/§D-CE4,
``design/api-freeze-and-standards/reserved-seams.md`` RS-1/RS-3).

Contract under test (structured mode only — no binary/header mode, RS-2):

    from varco_core.event.cloudevents import (
        CloudEventsJsonSerializer,   # Serializer[Event]
        CloudEventsSettings,         # source REQUIRED, no default
    )

    serializer = CloudEventsJsonSerializer(CloudEventsSettings(source="/svc/orders"))
    envelope: bytes = serializer.serialize(event)     # whole CloudEvents JSON
    event = serializer.deserialize(envelope)
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

import pytest
from varco_core.event import Event
from varco_core.service.tenant import tenant_context

# ── Test event types ───────────────────────────────────────────────────────────


class CePlacedEvent(Event):
    __event_type__ = "ce.order.placed"
    order_id: str
    total: float = 0.0


class CeCorrelatedEvent(Event):
    __event_type__ = "ce.order.correlated"
    order_id: str
    correlation_id: str | None = None


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture
def serializer() -> Any:
    """The opt-in CloudEvents serializer, constructed with an explicit source."""
    from varco_core.event.cloudevents import (  # noqa: PLC0415
        CloudEventsJsonSerializer,
        CloudEventsSettings,
    )

    return CloudEventsJsonSerializer(CloudEventsSettings(source="/varco/tests/orders"))


def envelope_of(serializer: Any, event: Event) -> dict[str, Any]:
    """Decode the wire bytes into the CloudEvents JSON envelope dict."""
    return json.loads(serializer.serialize(event).decode("utf-8"))


# ── REQUIRED attributes (§D-CE4) ───────────────────────────────────────────────


class TestRequiredAttributes:
    @pytest.mark.parametrize("attribute", ["id", "source", "specversion", "type"])
    async def test_required_attribute_is_present_and_non_empty(
        self, serializer: Any, attribute: str
    ) -> None:
        # The four CloudEvents REQUIRED attributes; each MUST be a non-empty string.
        envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1"))

        assert attribute in envelope
        assert isinstance(envelope[attribute], str)
        assert envelope[attribute] != ""

    async def test_specversion_is_the_literal_1_0(self, serializer: Any) -> None:
        # §D-CE4 pins the literal "1.0" — never the settings' or the SDK's idea of it.
        envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1"))

        assert envelope["specversion"] == "1.0"

    async def test_id_is_the_event_id(self, serializer: Any) -> None:
        event = CePlacedEvent(order_id="o-1")

        envelope = envelope_of(serializer, event)

        assert envelope["id"] == str(event.event_id)

    async def test_type_is_the_event_type_name(self, serializer: Any) -> None:
        envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1"))

        assert envelope["type"] == CePlacedEvent.event_type_name()

    async def test_source_comes_from_settings(self, serializer: Any) -> None:
        envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1"))

        assert envelope["source"] == "/varco/tests/orders"

    async def test_time_is_rfc_3339_and_matches_event_timestamp(self, serializer: Any) -> None:
        # RFC 3339 with an explicit offset — parseable by datetime.fromisoformat.
        event = CePlacedEvent(order_id="o-1")

        envelope = envelope_of(serializer, event)

        parsed = datetime.fromisoformat(envelope["time"])
        assert parsed.tzinfo is not None
        assert parsed == event.timestamp


# ── data vs data_base64 (§D-N2-attrs item 2) ───────────────────────────────────


class TestDataSelection:
    async def test_datacontenttype_is_application_json(self, serializer: Any) -> None:
        envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1"))

        assert envelope["datacontenttype"] == "application/json"

    async def test_data_is_used_and_data_base64_absent_for_json_content_type(
        self, serializer: Any
    ) -> None:
        # Normative: a `+json`/`json` datacontenttype MUST use `data`, never `data_base64`.
        envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1", total=9.5))

        assert "data" in envelope
        assert "data_base64" not in envelope

    async def test_data_and_data_base64_are_mutually_exclusive(self, serializer: Any) -> None:
        # The spec forbids both being present; assert it rather than assume it.
        envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1"))

        assert not ("data" in envelope and "data_base64" in envelope)

    async def test_data_carries_the_payload_without_envelope_duplication(
        self, serializer: Any
    ) -> None:
        # §D-CE4: data is model_dump(mode="json") minus event_id/timestamp — those
        # live in the envelope as `id`/`time` and must not be duplicated inside.
        envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1", total=9.5))

        assert envelope["data"] == {"order_id": "o-1", "total": 9.5}


# ── Extension attributes (§D-N2-attrs item 1) ──────────────────────────────────

_EXTENSION_NAME_RE = re.compile(r"^[a-z0-9]{1,20}$")

_SPEC_ATTRIBUTES = frozenset(
    {
        "specversion",
        "id",
        "source",
        "type",
        "time",
        "datacontenttype",
        "dataschema",
        "subject",
        "data",
        "data_base64",
    }
)


class TestExtensionAttributes:
    async def test_every_extension_name_matches_the_naming_rule(self, serializer: Any) -> None:
        # Lowercase ASCII alnum, 1..20 chars — brief 005 §3's tightening of §D-CE4.
        with tenant_context("acme"):
            envelope = envelope_of(
                serializer, CeCorrelatedEvent(order_id="o-1", correlation_id="corr-1")
            )

        extensions = set(envelope) - _SPEC_ATTRIBUTES
        assert extensions
        for name in extensions:
            assert _EXTENSION_NAME_RE.match(name), f"illegal CloudEvents extension name: {name!r}"

    async def test_correlationid_extension_carries_the_event_correlation_id(
        self, serializer: Any
    ) -> None:
        # Name is `correlationid` — no underscore; underscores are illegal.
        envelope = envelope_of(serializer, CeCorrelatedEvent(order_id="o-1", correlation_id="c-9"))

        assert envelope["correlationid"] == "c-9"
        assert "correlation_id" not in envelope

    async def test_correlationid_absent_when_event_carries_none(self, serializer: Any) -> None:
        envelope = envelope_of(serializer, CeCorrelatedEvent(order_id="o-1"))

        assert "correlationid" not in envelope

    async def test_tenantid_present_with_an_ambient_tenant(self, serializer: Any) -> None:
        # tenantid comes from current_tenant() only — never RequestContext (§D-CE4).
        with tenant_context("acme"):
            envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1"))

        assert envelope["tenantid"] == "acme"

    async def test_tenantid_absent_without_an_ambient_tenant(self, serializer: Any) -> None:
        # Documented best-effort behaviour (an OutboxRelay publish has no ambient
        # tenant).  Asserted so it cannot regress into a silent default.
        envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1"))

        assert "tenantid" not in envelope

    async def test_tenant_is_never_folded_into_source(self, serializer: Any) -> None:
        # §D-CE4 convention 2: the tenant is an extension, never encoded in
        # `source` or `subject` — `source` must stay stable per producer.
        with tenant_context("acme"):
            envelope = envelope_of(serializer, CePlacedEvent(order_id="o-1"))

        assert envelope["source"] == "/varco/tests/orders"
        assert "acme" not in envelope.get("subject", "")


# ── Round-trip ─────────────────────────────────────────────────────────────────


class TestRoundTrip:
    async def test_round_trip_reconstructs_the_same_event_type_and_payload(
        self, serializer: Any
    ) -> None:
        event = CePlacedEvent(order_id="o-42", total=12.5)

        restored = serializer.deserialize(serializer.serialize(event))

        assert isinstance(restored, CePlacedEvent)
        assert restored.order_id == "o-42"
        assert restored.total == 12.5

    async def test_round_trip_preserves_event_id_and_timestamp(self, serializer: Any) -> None:
        # id/time live only in the envelope, so the round-trip has to read them
        # back out of it — the one place this serializer can silently lose data.
        event = CePlacedEvent(order_id="o-42")

        restored = serializer.deserialize(serializer.serialize(event))

        assert restored.event_id == event.event_id
        assert restored.timestamp == event.timestamp

    async def test_round_trip_ignores_tenantid_extension(self, serializer: Any) -> None:
        # tenantid is ambient metadata, not an Event field — deserialize must not
        # try to set it on the frozen model.
        with tenant_context("acme"):
            wire = serializer.serialize(CePlacedEvent(order_id="o-1"))

        restored = serializer.deserialize(wire)

        assert isinstance(restored, CePlacedEvent)


# ── Settings (§D-CE4: no default source) ───────────────────────────────────────


class TestCloudEventsSettings:
    async def test_construction_fails_without_a_source(self) -> None:
        # There is no correct default for "who am I" — construction must fail
        # loudly rather than emit something like "varco".
        from pydantic import ValidationError  # noqa: PLC0415
        from varco_core.event.cloudevents import CloudEventsSettings  # noqa: PLC0415

        with pytest.raises(ValidationError):
            CloudEventsSettings()

    async def test_construction_fails_on_an_empty_source(self) -> None:
        # `source` is a non-empty URI-reference per the spec.
        from pydantic import ValidationError  # noqa: PLC0415
        from varco_core.event.cloudevents import CloudEventsSettings  # noqa: PLC0415

        with pytest.raises(ValidationError):
            CloudEventsSettings(source="")

    async def test_source_is_accepted_when_supplied(self) -> None:
        from varco_core.event.cloudevents import CloudEventsSettings  # noqa: PLC0415

        assert CloudEventsSettings(source="/svc/orders").source == "/svc/orders"


# ── Opt-in wiring (§D-CE1) ─────────────────────────────────────────────────────


class TestOptInByDefault:
    async def test_serializer_is_not_registered_at_minimum_priority(self) -> None:
        # JsonEventSerializer is @Fallback (Plan 045) — a candidate only when
        # no non-fallback Serializer[Event] binding exists. The CloudEvents
        # provider registered by bind_cloudevents_serializer() is a plain,
        # non-fallback binding, so it shadows JsonEventSerializer once bound —
        # it is never auto-active on its own (opt-in only, §D-CE1).
        from providify import DIContainer  # noqa: PLC0415
        from varco_core.event.cloudevents import (  # noqa: PLC0415
            CloudEventsSettings,
            bind_cloudevents_serializer,
        )
        from varco_core.serialization import Serializer  # noqa: PLC0415

        container = DIContainer()
        container.scan("varco_core", recursive=True)
        bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/test"))

        binding = container.get_binding(Serializer[Event])
        assert binding.fallback is False

    async def test_bind_cloudevents_serializer_returns_the_container(self) -> None:
        from providify import DIContainer  # noqa: PLC0415
        from varco_core.event.cloudevents import (  # noqa: PLC0415
            CloudEventsSettings,
            bind_cloudevents_serializer,
        )

        container = DIContainer()
        result = bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/test"))

        assert result is container

    async def test_bind_cloudevents_serializer_twice_raises(self) -> None:
        # §D-bind — a second call used to silently swap the wire format
        # again (last-registered-wins at equal priority); it now raises.
        import pytest  # noqa: PLC0415
        from providify import DIContainer  # noqa: PLC0415
        from varco_core.di import AlreadyBoundError  # noqa: PLC0415
        from varco_core.event.cloudevents import (  # noqa: PLC0415
            CloudEventsSettings,
            bind_cloudevents_serializer,
        )

        container = DIContainer()
        bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/test"))

        with pytest.raises(AlreadyBoundError):
            bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/test-2"))


# ── Open question 1 — the DLQ round-trip decision (plan §Open questions) ───────


class TestDeadLetterPayloadIsTheEnvelope:
    """
    ⚠️ This class encodes plan 030's **Step-3 decision** for Open question 1:

        "If a CloudEvents-serialized event dead-letters, is the stored payload
         the envelope or the inner ``data``?"

    The decision asserted here is **the whole CloudEvents envelope** — a dead
    letter stores exactly the bytes that were on the wire, so a redrive
    re-publishes an identical, still-spec-compliant message and an operator
    inspecting the DLQ sees the same document the broker carried.  The
    implementer must honour this; do not relax these assertions.
    """

    async def test_dlq_payload_is_the_full_envelope_not_the_inner_data(
        self, serializer: Any
    ) -> None:
        from varco_core.event.dlq import (  # noqa: PLC0415
            DeadLetterEntry,
            InMemoryDeadLetterQueue,
        )

        event = CePlacedEvent(order_id="o-7", total=1.0)
        dlq = InMemoryDeadLetterQueue()

        await dlq.push(DeadLetterEntry(event=event, payload=serializer.serialize(event)))

        (entry,) = await dlq.list_entries()
        assert entry.payload is not None
        stored = json.loads(entry.payload.decode("utf-8"))
        assert stored["specversion"] == "1.0"
        assert stored["type"] == CePlacedEvent.event_type_name()
        assert stored["data"] == {"order_id": "o-7", "total": 1.0}

    async def test_dlq_stored_envelope_deserializes_back_to_the_event(
        self, serializer: Any
    ) -> None:
        # Redrive must be able to reconstruct the event from what was stored.
        from varco_core.event.dlq import (  # noqa: PLC0415
            DeadLetterEntry,
            InMemoryDeadLetterQueue,
        )

        event = CePlacedEvent(order_id="o-7")
        dlq = InMemoryDeadLetterQueue()

        await dlq.push(DeadLetterEntry(event=event, payload=serializer.serialize(event)))

        (entry,) = await dlq.list_entries()
        assert entry.payload is not None
        restored = serializer.deserialize(entry.payload)
        assert isinstance(restored, CePlacedEvent)
        assert restored.event_id == event.event_id


class TestBindCloudeventsSerializerRoutesThroughBindSettings:
    """
    Plan 048 / Step 14 — bind_cloudevents_serializer() must route its
    settings through bind_settings(container, CloudEventsSettings,
    instance=settings) (non-fallback), so a second call raises
    SettingsAlreadyBoundError instead of silently rebinding.
    """

    async def test_second_call_raises_settings_already_bound_error(self) -> None:
        from providify import DIContainer
        from varco_core.event.cloudevents import (
            CloudEventsSettings,
            bind_cloudevents_serializer,
        )
        from varco_core.settings import SettingsAlreadyBoundError

        container = DIContainer()
        bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/orders"))

        with pytest.raises(SettingsAlreadyBoundError):
            bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/other"))

    async def test_settings_resolvable_via_container_get(self) -> None:
        from providify import DIContainer
        from varco_core.event.cloudevents import (
            CloudEventsSettings,
            bind_cloudevents_serializer,
        )

        container = DIContainer()
        bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/orders"))

        resolved = container.get(CloudEventsSettings)
        assert resolved.source == "/svc/orders"
