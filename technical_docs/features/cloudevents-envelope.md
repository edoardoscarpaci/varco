# CloudEvents envelope — `varco_core.event.cloudevents`

Plan 030 / Phase 0 (BACKLOG 3.1, row **N2**). Prior design:
`plans/022-api-freeze-and-standards-alignment.md` §D-CE1–§D-CE4, seams recorded in
`design/api-freeze-and-standards/reserved-seams.md` RS-1/RS-2/RS-3. Evidence:
`design/research/005-idempotency-webhooks-and-cloudevents.md` §3.

Closes: "our events are only readable by another varco process — a partner platform, an
Eventarc/EventBridge consumer, or a Knative sink cannot parse them."

## The seam: a second `Serializer[Event]`, nothing else

`varco_core/event/serializer.py` already documents its own extension point — `JsonEventSerializer`
is registered `@Fallback @Singleton` (Plan 045) and loses to any app-supplied
`Serializer[Event]` at any registration order. CloudEvents ships as a *second implementation* of
that Protocol and changes nothing else:

- **no change to `Event`** — no field added, no `model_dump()` shape moved, so no DLQ, outbox or
  audit consumer sees a byte move;
- **no change to any bus** — every bus accepts a `serializer=` constructor kwarg and resolves
  `Serializer[Event]` optionally through DI, so binding it is the whole opt-in on Kafka, NATS and
  both Redis shapes;
- **no change to the DLQ or outbox contract** — both take the serializer as a parameter, so dead
  letters and outbox rows are stored in whatever format the bus publishes;
- **reversible per deployment** — rebind and restart.

⚠️ Rolling out on a live channel is still a three-phase migration, and an existing DLQ backlog must
be drained first — nothing converts already-stored rows. See "Migration" below.

## Opt in — it is never auto-active

```python
from providify import DIContainer
from varco_core.event.cloudevents import CloudEventsSettings, bind_cloudevents_serializer

container = DIContainer()
bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/orders"))
```

`bind_cloudevents_serializer(container, settings)` requires an explicit `container` and returns
it (Plan 046 §D-bind — both the `CloudEventsSettings` and `Serializer[Event]` registrations go
through `varco_core.di.bind_instance()`). Call it **once**: a second call raises
`varco_core.di.AlreadyBoundError` instead of silently changing the wire format again — previously
a repeat call appended a second competing binding and the *first*-registered one silently kept
winning (providify's `provide()` appends; ties resolve first-registered, never "last one wins").
Use providify's `di_overrides`/`ContainerOverrides` in a test to swap the serializer instead of
calling this twice.

or, without DI:

```python
serializer = CloudEventsJsonSerializer(CloudEventsSettings(source="/svc/orders"))
bus = KafkaEventBus(settings, serializer=serializer)
```

⚠️ **`cloudevents.py` deliberately carries no module-level `@Singleton` and no module-level
`@Provider`.** providify's scanner (`scanner.py::_scan_module`) auto-registers *both* shapes, and
`container.scan("varco_core", recursive=True)` is a documented, in-use pattern — a decorator here
would silently change the wire format of every app that scans `varco_core`. `bind_cloudevents_serializer()`
registers at providify's **default** priority, which is what makes it beat `JsonEventSerializer`.

`CloudEventsSettings` is registered through `container.provide(...)` — a provider, never a
`@Singleton` — per CLAUDE.md's pydantic-`BaseSettings` rule and RS-3's note.

## Attribute mapping

| CloudEvents | varco source | Notes |
|---|---|---|
| `specversion` | literal `"1.0"` | REQUIRED |
| `id` | `Event.event_id` | REQUIRED. UUID → string |
| `source` | `CloudEventsSettings.source` | REQUIRED, **no default** — see below |
| `type` | `Event.event_type_name()` | REQUIRED. `__event_type__` or the class name |
| `time` | `Event.timestamp` | RFC 3339 with an explicit offset; already aware-UTC |
| `datacontenttype` | `CloudEventsSettings.datacontenttype` (default `application/json`) | Must end in `json`/`+json` |
| `data` | `model_dump(mode="json")` minus `event_id`/`timestamp` | Those two live in the envelope; duplicating them lets the copies disagree |
| `correlationid` | `event.correlation_id`, when the event declares one | **Extension** — no underscore |
| `tenantid` | `current_tenant()`, when a tenant is ambient | **Extension** — best-effort, see below |

**`source` has no default, and construction fails without one.** The spec makes `source` + `id`
the uniqueness key for an event, so a shared placeholder like `"varco"` would make two unrelated
services collide. `CloudEventsSettings()` raises `ValidationError`; set it explicitly or via
`VARCO_CLOUDEVENTS_SOURCE`.

### Extension attribute names

CloudEvents restricts extension names to **lowercase ASCII letters and digits** — no underscores,
no hyphens — with a **recommended maximum of 20 characters** (research 005 §3). `correlationid`
(13) and `tenantid` (8) both fit. The serializer validates every extension name it emits against
`^[a-z0-9]{1,20}$` and raises `ValueError` rather than shipping an illegal name, so a future
extension cannot be invented over the limit by accident.

No registered CloudEvents extension covers tenancy or correlation, so varco defines these two
names. Two registered extensions varco does **not** yet emit and must never rename: `partitionkey`
and `traceparent` — parked in plan 030's *Parked* table precisely so a home-grown equivalent is
never invented.

### `data` vs `data_base64`

The choice is normative, not stylistic: use `data` when `datacontenttype` ends in `json` or
`+json`, `data_base64` otherwise, and **the two are mutually exclusive**. varco always emits
`data`. That is enforced twice — a `field_validator` on `CloudEventsSettings.datacontenttype`, and
an assertion inside `serialize()` so a future configurable content type cannot silently produce a
spec violation. `deserialize()` rejects an envelope carrying `data_base64` with a `ValueError`
rather than guessing.

### ⚠️ `tenantid` is best-effort, by design

It is read from `current_tenant()` — CLAUDE.md's single source of truth, never `RequestContext`. A
serializer runs on whatever task the publish happens on, so an **`OutboxRelay`-driven publish has
no ambient tenant** and emits no `tenantid`. This is asserted by a unit test in both directions so
it cannot regress into a silent default.

Do **not** "fix" this by adding a tenant field to `Event` — that is §D-CE1's rejected alternative
wearing a different hat. And the tenant is **never** folded into `source` or `subject`: `source`
must stay stable per producer for the spec's `source` + `id` uniqueness rule.

## Per-transport status

| Backend | Structured mode | Binary mode | Note |
|---|---|---|---|
| **NATS** | ✅ fully spec-compliant with the serializer swap alone | ❌ impossible **by spec** — the NATS binding supports structured only | — |
| **Redis** | ⚠️ compliant with **varco's own convention** (below), but only when the bus is constructed with `serializer=` — the DI binding does not reach it | n/a | No official CloudEvents Redis binding exists |
| **Kafka** | ⚠️ body is spec-correct; the binding's `content-type` header is not set | ❌ needs `ce_`-prefixed headers | See below |

### ⚠️ Kafka: the body is right, the header is missing

The Kafka protocol binding additionally requires a `content-type` header starting with
`application/cloudevents` for a receiver to *detect* structured mode. varco does not set it:
`AbstractEventBus.publish()` is promised never to gain `headers=` (**RS-2**, and that promise is
load-bearing for the whole standards story), and `varco_kafka/bus.py` calls
`send_and_wait(topic, value=value)` with no headers argument.

Resolution path, already decided and not re-litigable here (§D-CE2): header support arrives as a
**new optional `MessageEncoder` Protocol** (`encode(event) -> tuple[bytes, Mapping[str, str]]`)
resolved through an optional, defaulted constructor kwarg on the buses that have a native header
channel. Never through `publish()`.

Practical consequence today: a Kafka consumer must be *told* the topic carries structured
CloudEvents rather than sniffing the header. The media type it should assume is
`CLOUDEVENTS_CONTENT_TYPE` (`application/cloudevents+json`), exported for exactly this purpose.

### Redis Streams — varco's named convention, v1

No official CloudEvents Redis binding exists, so varco defines one and versions it here:

> **varco Redis Streams CloudEvents convention, v1** — the **whole** CloudEvents JSON envelope
> occupies a **single stream field named `ce`**. Never one field per CloudEvents attribute.

Rationale: per-attribute fields would put spec-owned names (`id`, `type`, `source`, …) into the
`XADD` field namespace, where they can collide with any future varco field; one opaque field
cannot.

Mechanically, `RedisStreamEventBus` reads an optional `stream_field` attribute off the bound
serializer (`CloudEventsJsonSerializer.stream_field == "ce"`) and writes that field; a serializer
without the attribute keeps the historical `"payload"` field byte-for-byte. varco_redis never
imports the CloudEvents module — the transport stays ignorant of the envelope format, and any
out-of-tree serializer can adopt the convention with one class attribute.

Reading is **dual**: the bus accepts either field name, so entries already pending in a stream
when the serializer is swapped still drain instead of being logged as unparseable and acknowledged
away.

Redis Pub/Sub needs no convention — the message body *is* the envelope.

### Redis and DI — one wiring subtlety worth knowing

`bind_cloudevents_serializer()` reaches both Redis buses, but it did not always, and the reason is
worth writing down because it will recur for any future `@Provider`-produced bus.

Kafka and NATS bind their bus as a scanned `@Singleton`, so providify injects every constructor
parameter — including the optional `serializer` — with no extra work. Redis is different: both
implementations are produced by `RedisEventBusSelectorConfiguration.bus()` (`varco_redis/bus.py`),
and **providify injects only what the `@Provider` method itself declares.** The method originally
declared `settings` alone, so the binding silently never arrived and each bus fell back to its own
`JsonEventSerializer()`. `RedisStreamEventBus` compounded it by annotating its `serializer`
parameter with the *concrete* `JsonEventSerializer | None` rather than `Serializer[Event] | None`.

Both are fixed: the provider declares and forwards `Serializer[Event]`, and the annotation is
widened. Current behaviour, guarded by `varco_redis/tests/test_redis_cloudevents_di.py`:

| Bus | Serializer resolved from the container |
|---|---|
| `KafkaEventBus`, `NatsEventBus` | ✅ `CloudEventsJsonSerializer` — injected as scanned singletons |
| `RedisEventBus`, `RedisStreamEventBus` | ✅ `CloudEventsJsonSerializer` — forwarded by the selector provider |

⛔ **Rule for a new `@Provider`-produced bus or DLQ: declare `serializer: Annotated[Serializer[Event]
| None, InjectMeta(optional=True)] = None` on the provider method and forward it.** A binding that
is never declared is never injected, and the failure is silent — the wrong wire format, no error.

Do not work around a missing binding by adding a `@Singleton` to `cloudevents.py`: that would make
the envelope auto-active for every app that scans `varco_core`, the one thing this design forbids.

## Dead letters — what is actually stored

**Plan 030's Open question 1 asked: does a dead letter hold the envelope or the inner `data`?**
Answer: **the whole envelope**, exactly what the broker carried, never the inner `data`.

Every DLQ backend takes its serializer as a parameter rather than constructing
`JsonEventSerializer()` as a literal, so dead letters are stored in the same wire format the bus
publishes. The three `@Configuration`-wired backends (`RedisDLQ`, `KafkaDLQ`, `NatsDLQ`) forward the
container binding automatically; `BeanieDeadLetterQueue` injects it as a scanned `@Singleton`.

| Path | What lands in the DLQ |
|---|---|
| A **consumer handler** exhausts its retries | `DeadLetterEntry.from_failure()` stores the typed `event` and leaves `payload=None`; the backend re-serializes it with **the serializer it was given** — the CloudEvents envelope when one is bound |
| **`OutboxRelay`** cannot deserialize a row | `payload=OutboxEntry.payload` — the bytes the outbox stored. `OutboxEntry.from_event()` and `OutboxRelay` both accept `Serializer[Event]`; pass your bus's serializer to keep them in step |
| A caller constructs `DeadLetterEntry(payload=serializer.serialize(event))` explicitly | The whole envelope — asserted by `varco_core/tests/test_cloudevents_serializer.py::TestDeadLetterPayloadIsTheEnvelope` |

- ✅ A redrive re-publishes an identical, still-spec-compliant message.
- ✅ An operator sees `source`/`tenantid`/`correlationid`, which the inner `data` does not carry.
- ✅ `serializer.deserialize(entry.payload)` reconstructs the original event including its
  `event_id` and `timestamp`, so a redrive is byte-stable rather than a re-creation.
- ❌ A DLQ row is larger than the inner payload. Accepted — the envelope is a few hundred bytes and
  the alternative loses the attributes that make the row diagnosable.

⚠️ **Ordering matters on rollout.** A DLQ populated *before* the serializer swap still holds the old
format, and nothing converts it. Drain the existing backlog before binding the new serializer —
this is the same reason the migration below is three-phase rather than a flip.

⚠️ **Two constructed by hand, not by DI:** `SADeadLetterQueue(engine, serializer=...)` and
`OutboxRelay(..., serializer=...)` have no `@Configuration` to forward the binding for them. Pass
`serializer=` explicitly at the call site, or they keep the `JsonEventSerializer` default.

## Migration: three phases, never a flip

A CloudEvents-serialized event and a native-serialized event **cannot share a channel** unless the
consumer sniffs the body. Roll out in three phases:

1. **Dual-emit.** Publish to a second channel (e.g. `orders.ce`) with a second bus instance holding
   the CloudEvents serializer. Nothing consumes it yet. Verify the wire bytes with a raw consumer.
2. **Switch consumers.** Move each downstream consumer onto the CloudEvents channel one at a time.
   Both channels stay live; a rollback is a consumer-side redeploy with no producer change.
3. **Retire the native channel.** Only once every consumer has moved *and* the native channel's
   retention window has fully elapsed — a stream/topic can still hold un-acked entries in the old
   format long after the last publish.

An in-place serializer swap on a live channel is the one shape to avoid: pending entries written
before the swap deserialize with the wrong serializer.

## Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| Binding the serializer on a live channel with pending messages | Consumers log deserialization errors and (Streams) acknowledge poison entries away | Use the three-phase dual-emit rollout; never flip a live channel |
| Expecting `tenantid` on every event | The attribute is missing on outbox-relayed events | It is best-effort by design — `current_tenant()` is empty on the relay task. Read the tenant from `data` if your event carries one |
| Expecting a Kafka `content-type: application/cloudevents+json` header | A strict CloudEvents Kafka receiver does not detect structured mode | Not reachable today (RS-2). Configure the consumer to assume structured mode on that topic |
| Adding a per-attribute Redis Streams field ("it is more readable in `XRANGE`") | Spec attribute names collide with varco's own field namespace, and the convention forks | One field named `ce`, always. It is a versioned convention downstreams implement against |
| Adding a `@Provider`-produced bus or DLQ without declaring `serializer` on the provider method | The DI binding silently never arrives — wrong wire format, no error anywhere | providify injects only what the provider *method* declares. Declare and forward `Serializer[Event]`; `varco_redis`'s selector is the worked example |
| Binding the serializer while a DLQ backlog exists | Old dead letters are native JSON, new ones are envelopes; a redrive republishes a mix | Drain the DLQ before the swap — nothing converts stored rows |
| Constructing `SADeadLetterQueue`/`OutboxRelay` without `serializer=` | Those two are hand-wired, so they keep the JSON default while the bus speaks CloudEvents | Pass `serializer=` explicitly at the call site |
| Inventing a `trace_parent` / `partition_key` extension | Underscores are illegal, and both concepts already have **registered** extension names | Use `traceparent` / `partitionkey` when they land — see plan 030's Parked table |
| Binding the serializer and expecting **Redis** to pick it up | Redis keeps publishing native varco JSON with a `payload` field | `varco_redis`'s selector constructs both buses with `config=` only — construct the bus yourself with `serializer=` (see "Redis and DI") |
| Expecting a dead letter to hold a CloudEvents envelope | The DLQ row is native varco JSON | Every DLQ backend and the outbox hard-code `JsonEventSerializer` — see "Dead letters — what is actually stored" |
| An external tool reading Redis Streams entries by the literal field name `payload` | It reads nothing once CloudEvents is bound | The field is `ce` under CloudEvents. Read whichever of the two is present, as `RedisStreamEventBus` itself does |
| `CloudEventsSettings()` with no `source` | `ValidationError` at wiring time | Deliberate — there is no correct default for "who am I". Set `VARCO_CLOUDEVENTS_SOURCE` |
| Adding the `cloudevents` PyPI SDK | — | Rejected (§D-CE3/§D-N2-sdk): `varco_core` takes no new runtime dependency, and the SDK disclaims its own stability |

## See also

- `design/api-freeze-and-standards/reserved-seams.md` — RS-1 (the serializer seam), RS-2 (why
  `publish()` never gains `headers=`), RS-3 (reserved names).
- `plans/022-api-freeze-and-standards-alignment.md` §D-CE1–§D-CE4 — the original decisions.
- `design/research/005-idempotency-webhooks-and-cloudevents.md` §3 — spec evidence.
- CloudEvents v1.0.2: [spec](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md),
  [JSON format](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/formats/json-format.md),
  [Kafka binding](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/kafka-protocol-binding.md).
