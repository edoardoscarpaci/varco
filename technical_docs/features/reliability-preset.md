# Reliability Preset — "opt into durability once"

Plan 009, Phase 9 (R5). `varco_core.reliability` composes three
previously-separate, individually-wired concerns — retry+DLQ
(`varco_core.event`/`varco_core.resilience`), the transactional outbox
(`varco_core.service.outbox`), and the audit trail
(`varco_core.service.audit`), plus the Phase 1 reliability metrics pack
(`varco_core.observability.reliability`) — behind one frozen config object
and one FastAPI lifespan component.

## Why a new subpackage, not `varco_core.resilience`

`ReliabilityPreset` needs `varco_core.event` (for `AbstractDeadLetterQueue`),
`varco_core.resilience` (for `RetryPolicy`), and `varco_core.service`
(outbox/audit) all at once. Putting it inside `resilience/` would make
`resilience` import `event` — but `event.consumer` already imports
`resilience` (for `@listen`'s `retry_policy=`), which would create a cycle.
`varco_core/reliability/` sits above both, and `varco_fastapi` imports only
`varco_core.reliability` — never a backend (same seam rule as
`AbstractEventBus`/`AbstractMigrator`).

## `ReliabilityPreset`

```python
from varco_core.reliability import ReliabilityPreset


@dataclass(frozen=True)
class ReliabilityPreset:
    retry_policy: RetryPolicy | None = None
    dlq: AbstractDeadLetterQueue | None = None
    outbox: bool = False
    audit: bool = False
    metrics: ReliabilityMetricsConfig | None = None
    outbox_max_attempts: int | None = None
    serializer: Serializer[Event] | None = None  # Plan 050 / DI-10, additive

    @classmethod
    def off(cls) -> ReliabilityPreset: ...  # the default — byte-identical to pre-Plan-009

    @classmethod
    def best_effort(cls, *, dlq: AbstractDeadLetterQueue) -> ReliabilityPreset: ...

    # RetryPolicy(max_attempts=3, base_delay=0.5) + dlq; no outbox/audit/metrics

    @classmethod
    def durable(cls, *, dlq: AbstractDeadLetterQueue) -> ReliabilityPreset: ...

    # RetryPolicy.durable_delivery() + dlq + outbox=True + audit=True + metrics=ReliabilityMetricsConfig()
```

`ReliabilityPreset(outbox_max_attempts=N, dlq=None)` raises `ValueError` at
construction — mirrors `OutboxRelay.__init__`'s own refusal to configure
silent data loss (deleting a poison entry with nowhere durable to put it).

**The preset never constructs a DLQ instance.** `varco_core` must not know
concrete backend types — you build a `SADeadLetterQueue`/`RedisDLQ`/etc.
yourself and pass it in:

```python
from varco_core.reliability import ReliabilityPreset
from varco_sa.dlq import SADeadLetterQueue

dlq = SADeadLetterQueue(engine)
preset = ReliabilityPreset.durable(dlq=dlq)
```

## Wiring it into FastAPI

```python
from varco_fastapi import create_varco_app
from varco_fastapi import default_modules
from varco_fastapi.modules import ReliabilityModule

app = create_varco_app(
    container, routers=[...], modules=[*default_modules(), ReliabilityModule(preset)]
)
```

No `ReliabilityModule` in `modules=` (the default) registers nothing —
byte-identical to not calling this feature at all. Passing a preset wires a
`ReliabilityLifecycle` (`varco_fastapi.reliability.ReliabilityLifecycle`, built by
`ReliabilityModule.start()`) into the app's startup/shutdown sequence:

```python
class ReliabilityLifecycle:
    def __init__(self, preset: ReliabilityPreset, *, container: Any) -> None: ...
    async def startup(self) -> None: ...  # metrics + OutboxRelay + AuditConsumer, per preset
    async def shutdown(self) -> None: ...  # stops the OutboxRelay it started
```

`startup()` resolves `OutboxRepository`/`AuditRepository`/`AbstractEventBus`
from the container **only when the preset asks for them**
(`preset.outbox`/`preset.audit`), and fails **loudly at startup** —
`LookupError` naming the missing interface — rather than silently at the
first event, or never. This is deliberate: the entire point of the feature
is "opt into durability once" without needing to double-check every
deployment wired it correctly.

**`preset.serializer` (Plan 050 / DI-10)** — when `outbox=True`, `ReliabilityModule.start()`
builds the `OutboxRelay` with `preset.serializer` if given, else the container's bound
`Serializer[Event]` when resolvable (the 045 `@Fallback` default always resolves to
`JsonEventSerializer`), so `bind_cloudevents_serializer()` reaches the outbox with no
`serializer=` to remember. An explicit `ReliabilityPreset(serializer=...)` always wins over the
bound one — the same "explicit argument beats DI" rule as everywhere else. `ReliabilityLifecycle.
shutdown()` also now stops the `AuditConsumer` (when `preset.audit=True`), before the relay, the
exact reverse of `startup()` — pre-050 it was never stopped at all, leaking its bus subscription.

## `@listen`'s global default — the `_UNSET` sentinel (RD-7)

`set_default_reliability_preset(preset)` makes every **bare** `@listen(...)`
handler (one that declares neither `retry_policy=` nor `dlq=`, on a consumer
whose `register_to()` call also carries no instance-level override) inherit
`preset.retry_policy`/`preset.dlq`:

```python
from varco_core.reliability import ReliabilityPreset, set_default_reliability_preset

set_default_reliability_preset(ReliabilityPreset.durable(dlq=my_dlq))


class OrderConsumer(EventConsumer):
    @listen(OrderPlacedEvent, channel="orders")  # inherits the durable preset
    async def on_order(self, event: OrderPlacedEvent) -> None: ...
```

The default preset is `ReliabilityPreset.off()`, so today's behaviour (no
retry, no DLQ, re-raise on exhaustion) is unaffected unless
`set_default_reliability_preset()` is called. To make "explicit `None`" still
mean "no retry, ignore the global preset" (distinguishable from *omitting*
the parameter), `@listen`'s `retry_policy=`/`dlq=` defaults changed from
`None` to a private `_UNSET` sentinel. Resolution order, evaluated at
`EventConsumer.register_to()` time (not at `@listen` decoration time):

```
explicit @listen(retry_policy=..., dlq=...)   → always wins
  ↓ (if _UNSET)
explicit register_to(retry_policy=..., dlq=...) instance-level override
  ↓ (if also unset)
process-wide default preset (get_default_reliability_preset())
  ↓ (if off())
nothing — re-raise on exhaustion (today's behaviour)
```

Because resolution is deferred to `register_to()`, calling
`set_default_reliability_preset()` **after** a `@listen`-decorated class is
already defined still applies — the decorator only stores `_UNSET` at
class-definition time.

```python
@listen(OrderPlacedEvent, channel="orders", retry_policy=None)  # explicit opt-out
async def on_order(self, event: OrderPlacedEvent) -> None: ...


# no retry, no DLQ — even with a durable() default preset process-wide
```

## `ReliabilityPreset.durable()` — what it turns on

| Component | Behaviour |
|---|---|
| `retry_policy` | `RetryPolicy.durable_delivery()` — `max_attempts=20, base_delay=15.0, max_delay=3600.0` |
| `dlq` | The instance you passed in |
| `outbox=True` | `ReliabilityLifecycle.startup()` builds and starts an `OutboxRelay(outbox=..., bus=..., retry_policy=..., dlq=..., max_attempts=preset.outbox_max_attempts)` |
| `audit=True` | `ReliabilityLifecycle.startup()` builds an `AuditConsumer(audit_repo=...)` and calls `register_to(bus)` |
| `metrics=ReliabilityMetricsConfig()` | `install_reliability_metrics(dlq=preset.dlq, outbox_repo=..., config=preset.metrics)` — see the observability doc's "Reliability metrics" section |

## Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| `outbox_max_attempts` set without `dlq` | `ValueError` at `ReliabilityPreset()` construction | Pass a `dlq=` alongside `outbox_max_attempts` |
| `preset.audit=True` with no `AuditRepository` bound | `LookupError` at app startup, not at first event | Bind `AuditRepository` in the container before passing the preset |
| `set_default_reliability_preset` called twice (composite deployment) | Last writer wins — the global default is process-wide, unlike each app's own `ReliabilityLifecycle` | Share one preset across composed apps, or avoid the global default and set `retry_policy=`/`dlq=` per `@listen` instead |
| `varco.dlq.depth` reports nothing for a real backend (Redis/Mongo/Kafka) | The metric exists but has zero data points; a DEBUG log shows `got Future … attached to a different loop` | `install_reliability_metrics()` was called **outside** the running event loop, so it could not capture the loop that owns the DLQ's async client. The gauge callback is synchronous and runs on OTel's exporter thread; it hands `count()` back to the owning loop via `run_coroutine_threadsafe`, which needs that loop. Call it from async startup (`ReliabilityLifecycle.startup()` / the ASGI lifespan) — never at module import time |
| Expecting `ReliabilityPreset` to read env vars | It doesn't — by design, it holds a *live* DLQ instance, which env vars cannot name without re-introducing concrete-type knowledge into `varco_core` | Build the DLQ yourself, pass it to `ReliabilityPreset.durable(dlq=...)` |
| **Per-call breaker for a peer service** | Circuit never opens for a flaky peer — building a fresh `PeerRegistry`/`CircuitBreaker` per request instead of reusing a singleton registry | Construct `PeerRegistry` once (module scope or a DI singleton via `bind_peers`); it caches one `CircuitBreaker` per peer *name*, never per call |

## Tests

`varco_core/tests/test_reliability_preset.py` — `off()` leaves `@listen`
behaviour unchanged; `durable()` gives a bare `@listen` handler a retry
policy and DLQ; explicit `retry_policy=None` wins over a durable default;
`outbox_max_attempts` without `dlq` raises; a late
`set_default_reliability_preset()` call still applies.
`varco_fastapi/tests/test_reliability_wiring.py` — `ReliabilityModule(preset)`
starts/stops the relay and consumer, installs metrics, and raises on a missing
`AuditRepository`.
