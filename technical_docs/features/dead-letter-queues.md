# Dead Letter Queues — one concept, three producers

Plan 005, Phase 3 (gap U-6). Closes: "the outbox relay silently drops an
undeserializable/unpublishable entry, and there is no bounded-retry path for
either the relay or the job store."

## One `DeadLetterEntry`, three sources

Before this phase, `DeadLetterEntry` was event-shaped — it always carried a
`DomainEvent`. Two more producers need to dead-letter things that are not
typed events: `OutboxRelay` (a raw `OutboxEntry` whose payload may not even
deserialize) and the job runner (a `Job`). Rather than build a second,
job-specific DLQ abstraction — explicitly rejected by U-17 §4 — `DeadLetterEntry`
was generalised **additively**:

```python
class DeadLetterSource(StrEnum):
    CONSUMER = "consumer"  # an EventConsumer handler exhausted retries (existing)
    OUTBOX_RELAY = "outbox_relay"  # OutboxRelay could not deserialize/publish an entry
    JOB = "job"  # a job exhausted max_attempts in JobRunner


# DeadLetterEntry gains, all defaulted:
source: DeadLetterSource = DeadLetterSource.CONSUMER
source_ref: str | None = None  # outbox entry_id / job_id, as str
payload: bytes | None = None  # raw bytes when `event` could not be deserialized
```

`event: DomainEvent` became `DomainEvent | None`. Every pre-Phase-3
construction site — positional or keyword, without `source=` — keeps working
unchanged; `source` defaults to `CONSUMER`, which is simply the new explicit
name for what a consumer's retry-exhaustion push already was.

```
                              push()
EventConsumer (@listen retries exhausted) ──┐
OutboxRelay (deserialize/publish failure) ──┼──► AbstractDeadLetterQueue
JobRunner (max_attempts exhausted)        ──┘         │
                                                       ▼
                                    InMemoryDeadLetterQueue (tests)
                                    RedisDLQ / KafkaDLQ (backends)
                                    SADeadLetterQueue (varco_sa, this phase)
```

## The `push()` contract — restated, now triply load-bearing

`AbstractDeadLetterQueue.push()` **must never raise**. This was already true
for the consumer retry wrapper (`_make_retry_wrapper` in
`varco_core.event.consumer` cannot recover from a DLQ failure), and is now
equally true for `OutboxRelay._relay_entry()` and `JobRunner._handle_job_failure()`
— neither has anywhere to put a `push()` failure either. Every implementation
(`InMemoryDeadLetterQueue`, `RedisDLQ`, `KafkaDLQ`, `SADeadLetterQueue`) logs
and swallows exceptions inside `push()`.

## `OutboxRelay` retry + dead-letter path

`OutboxEntry` gained `attempts: int = 0`, `last_error: str | None = None`,
`next_attempt_at: datetime | None = None` — all defaulted, so `from_event()`
and every pre-existing construction site is unaffected. `OutboxRepository`
gained a **concrete** (not abstract) `mark_failed()` — a repository written
before this method existed keeps importing; the default implementation logs
one warning per repository class and no-ops, degrading the relay to today's
unbounded-retry behaviour.

`OutboxRelay.__init__` gained `retry_policy`, `dlq`, `max_attempts`, all
`None`-defaulted:

- With **no** `retry_policy`: byte-identical to today — an entry that fails
  to publish is left in place, logged, and retried on the next tick.
- With a `retry_policy`: on publish failure, `attempts` increments and
  `next_attempt_at = now + retry_policy.compute_delay(attempts)` is written
  via `mark_failed()`. `_relay_once()` client-side-filters entries whose
  `next_attempt_at > now` — this needs no `get_pending()` signature change,
  so it works against every existing `OutboxRepository`, not just SA/Beanie.
- Once `attempts >= max_attempts`, the entry is pushed to the `dlq`
  (`source=OUTBOX_RELAY`, `source_ref=str(entry_id)`) **and deleted**.
  Constructing `OutboxRelay(max_attempts=...)` without a `dlq` raises
  `ValueError` at construction — deleting a poison entry with nowhere to put
  it is silent data loss, so the relay refuses that configuration outright.
- The pre-existing "payload failed to deserialize → delete" branch now
  dead-letters first (`event=None`, `payload=entry.payload`) when a `dlq` is
  wired, then deletes — today it deletes silently, which is a real, unreported
  loss this phase closes.

**Why dead-lettering deletes the entry** (rather than leaving it for a
relay/DLQ tandem to reconcile): per-tenant FIFO delivery means a single
poison row at the head of the queue stops the whole stream behind it — the
exact failure U-6 §1 names. Deleting after a successful `dlq.push()` is what
unblocks the stream; the DLQ is now the durable record of the poison entry.

## `durable_delivery()` preset — report, not request

U-6 §3 observes the shipped `RetryPolicy` default (`max_attempts=3`,
≈7 seconds total) is a poor fit for durable delivery, compared to
Oban's 20 attempts or Sidekiq's 25. This is filed as a **report**, not a
request to change the global default — every existing caller of the bare
`RetryPolicy()` default keeps its exact timing. Instead, a named preset:

```python
@classmethod
def durable_delivery(cls) -> RetryPolicy:
    return cls(max_attempts=20, base_delay=15.0, max_delay=3600.0, jitter=True)
```

`OutboxRelay` and `AuditConsumer` use this preset when the caller does not
supply an explicit policy of their own — see below.

## `AuditConsumer` — safe-by-default, opt out explicitly

U-6 §2: "safe-by-default is the right polarity for an audit trail."
`AuditConsumer` gained a class attribute
`_default_retry_policy = RetryPolicy.durable_delivery()` and an `__init__(...,
dlq=None)`; `register_to()` passes both **unless the caller overrode them** on
`@listen` or the `register_to()` call itself. Fire-and-forget is the
explicit opt-out: pass `retry_policy=None` deliberately if you want the old
best-effort behaviour back.

## `varco_sa.dlq.SADeadLetterQueue` — a durable DLQ, built anyway

⚠️ **Scope note.** U-6 explicitly states a durable DLQ is *not* an upstream
ask — the filer builds their own over the ABC. This module ships anyway
because `OutboxRelay`'s new dead-letter path (above) now needs somewhere
durable to put poison entries, and the only DLQ shipped before this phase was
`InMemoryDeadLetterQueue` — a `deque(maxlen=10_000)` lost on every restart.
`SADeadLetterQueue` makes the relay's dead-letter path useful out of the box
without changing what the filer asked for.

```python
from sqlalchemy.ext.asyncio import create_async_engine
from varco_sa.dlq import SADeadLetterQueue

engine = create_async_engine("postgresql+asyncpg://...")
dlq = SADeadLetterQueue(engine)
await dlq.ensure_table()  # creates varco_dead_letters idempotently

relay = OutboxRelay(
    outbox=relay_repo,
    bus=bus,
    retry_policy=RetryPolicy.durable_delivery(),
    dlq=dlq,
    max_attempts=20,
)
```

Table `varco_dead_letters`: `entry_id`, `source`, `source_ref`, `channel`,
`handler_name`, `event_type`, `payload`, `error_type`, `error_message`,
`attempts`, `first_failed_at`, `last_failed_at`. Raw Core (not ORM) over a
dedicated `MetaData`, same pattern as `SAJobStore`/`varco_sa.outbox` — no
dependency on the application's `DeclarativeBase`. `push()` never raises: any
exception is logged and swallowed, per the ABC contract.

⚠️ This repository does not ship an Alembic environment for its own
infrastructure tables — see the note in `technical_docs/features/crypto-shredding.md`.
Generate the migration from `dead_letters_metadata` via `autogenerate`
against your application's Alembic environment.

The SA outbox table gained `attempts INT NOT NULL DEFAULT 0`,
`last_error TEXT NULL`, `next_attempt_at TIMESTAMPTZ NULL` in the same
change, and `SAOutboxRepository`/`SARelayOutboxRepository` both implement
`mark_failed()` natively as a single `UPDATE`.

## Backend support matrix (Plan 009)

| Backend | `push`/`pop_batch`/`ack`/`count` | `get`/`list_entries` (random access) | `delete`/`delete_where`/`count_by_channel` | `tenant_id` filter |
|---|---|---|---|---|
| `InMemoryDeadLetterQueue` | ✅ | ✅ (`supports_random_access = True`) | ✅ | ✅ |
| `SADeadLetterQueue` (varco_sa) | ✅ | ✅ | ✅ | ✅ |
| `RedisDLQ` (varco_redis) | ✅ | ✅ | ✅ | ✅ |
| `BeanieDeadLetterQueue` (varco_beanie) | ✅ | ✅ | ✅ | ✅ |
| `KafkaDLQ` (varco_kafka) | ✅ | ❌ `NotImplementedError` (RD-4) | ❌ `NotImplementedError` naming `retention.ms` | — (redrive via `redrive_batch` only) |
| `NatsDLQ` (varco_nats) | ✅ | ❌ `NotImplementedError` (RD-4) | ❌ `NotImplementedError` naming JetStream `MaxAge` | — (redrive via `redrive_batch` only) |

`AbstractDeadLetterQueue.supports_random_access: ClassVar[bool] = False` is
the capability flag every backend above overrides. Stream-shaped stores
(Kafka topic, NATS subject) genuinely cannot address a single message by id
— only `pop_batch()` is meaningful there, so `get`/`list_entries`/
`delete_where`/`count_by_channel` raise `NotImplementedError` with a message
naming the backend's own retention mechanism instead of pretending to
support an operation that would either be destructive or impossible.

### Beanie (`varco_beanie.dlq.BeanieDeadLetterQueue`) — RD-2, no TTL by default

```python
from varco_beanie.dlq import BeanieDeadLetterQueue, DeadLetterDocument

await init_beanie(database=db, document_models=[Order, DeadLetterDocument])
dlq = BeanieDeadLetterQueue()  # ttl_seconds=None — no auto-expiry (RD-2)
```

Collection `varco_dead_letters` — matches `SADeadLetterQueue`'s table name
and the `varco_audit_log` precedent. **There is deliberately no TTL index by
default.** A TTL index silently deletes dead letters — precisely the failure
mode this whole release exists to fix ("nobody notices it died"), made worse
here because the delete happens *without an operator ever seeing the entry*.
Retention is explicit: `delete_where()` / `varco retention prune`.
`ttl_seconds=` is an opt-in escape hatch that logs one WARNING at
construction naming the data-loss implication — and the index it declares
still has to be **built** by `varco migrate index --create` (never inside
the request path or lifespan, Plan 006's `index_mode="check"` precedent);
declaring `ttl_seconds=` alone does not build the index.

`entry_id` is Mongo's `_id` — `get()`/`ack()` are O(1) `_id` lookups, and a
duplicate `push()` of the same `entry_id` is a `DuplicateKeyError` treated as
"already stored" (idempotent on redelivery — a property `SADeadLetterQueue`
only gets on Postgres via `ON CONFLICT`). Ack semantics mirror
`SADeadLetterQueue` exactly: `pop_batch()` is a non-destructive read, `ack()`
deletes — there is no visibility window (SA has none either); the
single-relay assumption is documented on the class, not silently assumed.

⚠️ **Verification status**: this backend is logic-complete and covered by
`varco_beanie/tests/test_beanie_dlq.py`, but that suite is
`@pytest.mark.integration` and requires a real MongoDB container. It has not
been run against a live Mongo in this environment (no Docker available this
session) — treat it as unit-reviewed, not integration-verified, until that
suite has actually been run.

### NATS (`varco_nats.dlq.NatsDLQ`) — `ack()` is a synchronous round trip, not fire-and-forget

`NatsDLQ.ack()` calls nats-py's `Msg.ack_sync(timeout=2.0)`, never the plain
`Msg.ack()`. The plain form only publishes to the message's reply subject
and returns immediately — the JetStream server may not have processed the
ack yet when the coroutine returns, so an immediate `count()` after `ack()`
could still see the entry (observed taking up to ~1 s to clear), and a
process that exits right after `ack()` can lose the ack outright, which
would make `DlqRedriver`'s publish-then-ack policy redeliver a dead letter
that was already handled. `ack_sync()` waits for the server's confirmation,
so `ack()`'s postcondition ("not returned by a future `pop_batch()`") holds
by the time the call returns — at the cost of one network round trip per
ack instead of zero, the correct trade for a durability primitive.

If the server does not confirm within the 2.0 s timeout, `ack()` does not
raise: it logs a warning and keeps the entry in its in-process `_in_flight`
tracking so a later `ack()` call retries it. A duplicate ack is harmless to
JetStream; a silently dropped one is not. `push()` is unaffected by this —
it remains the documented fire-and-forget-safe, never-raises primitive.

## Redrive — `DlqRedriver`

Plan 009, Phase 4 (R1). The backlog's literal ask was `redrive(entry_id)`
**on the ABC**; that was rejected (see the DESIGN block in
`varco_core/event/dlq.py`) because it would force `AbstractDeadLetterQueue`
to hold an `AbstractEventBus`, inverting the documented "DLQ is independent
of the bus" invariant on every backend. Instead, the ABC gained only
read/delete primitives (`get`, `list_entries`, `delete`, `delete_where`), and
a separate `DlqRedriver` (`varco_core.event.redrive`) owns the redrive
*policy*:

```python
from varco_core.event.redrive import DlqRedriver

redriver = DlqRedriver(dlq, bus, default_channel="orders")

outcome = await redriver.redrive(entry_id)  # single entry
report = await redriver.redrive_batch(limit=10)  # works on every backend, incl. Kafka/NATS
```

`DlqRedriver` is one of the **very few** classes permitted to hold an
`AbstractEventBus` directly — it joins `OutboxRelay` and
`EventConsumer.register_to()` on that list (see CLAUDE.md's layer-rule
paragraph): this is infrastructure, not application logic.

### Algorithm — publish-then-ack, never ack-then-publish

1. Resolve the entry (`get(entry_id)`, or `list_entries()`/`pop_batch()` for
   a batch).
2. Reject a **payload-only** entry (`event is None`) —
   `error="payload-only entry; not republishable"` — and a **job-sourced**
   entry (`source == DeadLetterSource.JOB`) —
   `error="job-sourced entry; re-enqueue via the job store"`. Neither is
   acked.
3. `await bus.publish(entry.event, channel=entry.channel or default_channel)`.
4. On success: `await dlq.ack(entry.entry_id)`.
5. On publish failure: the entry is **not** acked — it stays in the DLQ, and
   the error is recorded on the outcome.
6. `dry_run=True` skips steps 3–4 entirely; the report says what *would*
   have happened.

Publish-then-ack (never the reverse) means a crash between the two
re-delivers the dead letter — **at-least-once**, the correct bias for a
message you already nearly lost. A duplicate republish is possible; the
inbox/dedup primitives handle it, same as everywhere else in this codebase.

### Stream-backed stores — single-entry redrive is unsupported (RD-4)

```python
redriver.redrive(entry_id)  # Kafka/NATS DLQ → DeadLetterNotAddressable
```

`DeadLetterNotAddressable` names the backend class and points at
`redrive_batch()` / the CLI's `--batch` flag, both of which work everywhere:
`redrive_batch()` falls back to `pop_batch()` (the one portable read every
backend has) when `list_entries()` raises `NotImplementedError`.

**⚠️ Redrive can re-poison the stream.** If the handler that originally
failed is still broken, a redriven entry comes straight back to the DLQ.
There is deliberately no automatic/scheduled redrive (parked, see the
plan's Non-goals) — use `dry_run=True` first, and watch the
`varco.dlq.redriven{status}` counter (see the observability doc).

### CLI

```bash
varco dlq list    -t module:factory [--channel C] [--source S] [--limit N]
varco dlq redrive -t module:factory -b module:factory \
                   (--entry-id UUID | --batch [--limit N] [--channel C] [--source S]) \
                   [--dry-run]
varco dlq purge   -t module:factory --before ISO8601 [--limit N]
```

`-t/--target` and `-b/--bus` name an importable zero-arg factory (or a bare
instance) returning the `AbstractDeadLetterQueue` / `AbstractEventBus` — the
CLI has no access to your app's DI container.

## Retention — `delete()` / `delete_where()` / `count_by_channel()`

Plan 009, Phase 2 (R3). Following the `AbstractJobStore.delete_where`
precedent (portable default where a correct one exists, concrete-but-raising
where none does):

| Member | Choice | Why |
|---|---|---|
| `delete(entry_id)` | **portable default** → `await self.ack(entry_id)` | Every backend's `ack()` already means "never return this entry again" — exactly the storage-semantics meaning of "delete". `ack` is the message-semantics name for the same operation. |
| `delete_where(older_than=, source=, channel=, tenant_id=, limit=)` | concrete-but-raising | Any `pop_batch()`-based default would have to `ack()` non-matching entries just to reach matching ones — silent data loss. Refusing is strictly safer. |
| `count_by_channel()` | concrete-but-raising | `count()` itself is already `-1` (an approximation) on Kafka — a per-channel breakdown isn't portable even in principle there. |

`delete_where()` with **no predicate at all** raises `ValueError` — refusing
to silently truncate the whole DLQ, same rule as `AbstractJobStore.delete_where`.
`limit=` caps rows deleted per call; chunk a large sweep with repeated calls
rather than one unbounded delete (each SA chunk is its own transaction under
a transaction-mode pooler). `KafkaDLQ`/`NatsDLQ.delete_where()` both check this
"no predicate" case *before* their own backend-support `NotImplementedError` —
an earlier bug (KI-2/KI-7) let a no-predicate call on either backend fall
straight through to `NotImplementedError`, skipping the ABC's refusal.

⚠️ **`BeanieDeadLetterQueue.count_by_channel()` bypasses beanie's own
aggregation cursor.** On beanie 2.0.1 + motor 3.7.1,
`Document.aggregate(pipeline).to_list()` raises `TypeError: object
AsyncIOMotorLatentCommandCursor can't be used in 'await' expression` —
beanie's `AggregationQuery.get_cursor()` unconditionally `await`s the
collection's `aggregate()` call, but this motor version returns its cursor
synchronously, not as a coroutine (KI-6). `count_by_channel()` works around
it by driving `DeadLetterDocument.get_pymongo_collection().aggregate(pipeline)`
directly (with an `inspect.isawaitable()` guard so it still works if a future
driver version *does* return a coroutine) and iterating with `async for`
instead of `.to_list()`. If your own application code calls
`SomeDocument.aggregate(pipeline).to_list()` directly against this
beanie/motor combination, expect the same `TypeError` — use the same
`get_pymongo_collection().aggregate(pipeline)` bypass until beanie fixes the
incompatibility upstream.

```bash
varco retention prune --type dlq --before 2026-01-01T00:00:00Z --limit 5000 --chunk 1000 \
    --target module:factory   # or set VARCO_RETENTION_TARGET
varco retention prune --type dlq --before ... --dry-run   # prints the count, deletes nothing
```

The default sweep behaviour loops `delete_where(..., limit=chunk)` until it
returns `0`. `--target` is required unless `VARCO_RETENTION_TARGET` is set —
mirrors `varco migrate`'s own config-resolution pattern.

**Kafka/NATS**: `delete_where()` raises `NotImplementedError` naming
`retention.ms` (Kafka topic retention) / JetStream `MaxAge` as the correct
mechanism — retention on a stream-backed store is not a varco concern.

⚠️ **Beanie: `older_than` is evaluated at BSON millisecond resolution, not
Python microsecond resolution.** `last_failed_at` is stored as a BSON
`UTCDateTime`, which pymongo truncates (floors) to the millisecond on write
— and floors the query operand the same way. A raw `$lt` on a
full-microsecond cutoff is therefore evaluated as `stored_ms <
floor_ms(cutoff)`, which excludes every entry sharing a millisecond with the
cutoff even though the value the store itself reports for it is genuinely
`< cutoff`. A chunked sweep re-passing a fixed cutoff would report `0` and
stop while such an entry remained. `BeanieDeadLetterQueue.delete_where()`
and `.list_entries()` fix this by widening only `older_than`'s operand up to
the next whole millisecond before it reaches pymongo
(`varco_beanie._bson_time.ceil_to_bson_millisecond`) — `newer_than`'s `$gt`
is deliberately left alone, since pymongo's floor is already the correct
rounding for a lower bound. This is Mongo-specific: `SADeadLetterQueue` and
`InMemoryDeadLetterQueue` store full microsecond precision and need no such
adjustment.

## Multitenancy (Plan 009, Phase 6 / R4)

`DeadLetterEntry.tenant_id: str | None = None` (new, defaulted, appended —
non-breaking on the dataclass) is stamped from the ambient
`varco_core.tenancy.tenant_context()` at push time by every producer
(`EventConsumer`'s retry wrapper, `OutboxRelay`'s dead-letter path) — never a
constructor parameter a caller fills in by hand.

```python
async with tenant_context("acme"):
    ...  # a handler that dead-letters here produces an entry with tenant_id="acme"

entries = await dlq.list_entries(tenant_id="acme")  # excludes entries with tenant_id=None
entries = await dlq.list_entries()  # no tenant filter — operator/global view
```

⚠️ **The `None`-tenant asymmetry is deliberate, not a bug**: an entry pushed
outside any tenant context (e.g. an outbox deserialize failure at boot) has
`tenant_id=None` and is visible only to the unscoped/global view — it never
matches an explicit `tenant_id="acme"` filter. A `tenant_id=UNSCOPED`
sentinel was considered and rejected as over-engineering for an admin-only
surface. `tenant_id` was deliberately **not** made mandatory: a
framework-level failure genuinely has no tenant.

### Postgres RLS on `varco_dead_letters`

```python
# inside a reviewed Alembic revision — nothing calls this automatically
from varco_sa.rls_framework import framework_rls_upgrade, framework_rls_downgrade


def upgrade() -> None:
    framework_rls_upgrade(op)  # both framework tables by default


def downgrade() -> None:
    framework_rls_downgrade(op)
```

`framework_rls_upgrade`/`framework_rls_downgrade`
(`varco_sa.rls_framework`, `FRAMEWORK_RLS_TABLES = ("varco_audit_log", "varco_dead_letters")`)
wrap `varco_sa.rls.render_rls_ddl()` directly so the correct
`(SELECT current_setting(..., true))` InitPlan form is always used — never a
bare `current_setting()` call, which is not `LEAKPROOF` and silently forces
a sequential scan on Postgres. **Nothing in varco enables this
automatically** — paste it into your own reviewed migration, per
`technical_docs/features/postgres-rls.md`'s "RLS enabled by a startup hook" pitfall. See
`technical_docs/features/multitenancy.md` for the full RLS table list.

## REST admin surface

Plan 009, Phase 10 (R6). `build_dlq_router(dlq, redriver=None, ...)`
(`varco_fastapi.admin.dlq_router`) — a plain `APIRouter`, same
`build_policy_router`/`build_tenant_router` precedent (hand-written JSON
handlers, no service/repository generic):

| Method | Path | Notes |
|---|---|---|
| GET | `/dlq/entries` | `list_entries()` filters as query params |
| GET | `/dlq/entries/{entry_id}` | 404 when absent; 501 on a stream backend |
| POST | `/dlq/entries/{entry_id}/redrive` | only registered when `redriver=` is given — an absent capability doesn't appear in the OpenAPI schema |
| POST | `/dlq/redrive` | batch; body carries filters + `dry_run` |
| DELETE | `/dlq/entries/{entry_id}` | `delete()` |
| DELETE | `/dlq/entries` | `delete_where()`; 501 on Kafka/NATS |
| GET | `/dlq/stats` | `count()` + `count_by_channel()` when supported |

Mount it (and the audit admin router) together via
`mount_reliability_admin()` — the single way to expose either surface:

```python
from varco_fastapi.admin.mount import mount_reliability_admin

mount_reliability_admin(
    app,
    dlq=dlq,
    redriver=redriver,  # omit to hide the redrive routes entirely
    acknowledge_bundled_admin=True,  # required — ValueError without it (RD-9)
    server_auth=auth,
    admin_role="reliability-admin",
    prefix="/reliability",
)
```

Mirrors Plan 007 RD-9 (the tenant control plane) exactly: this surface can
**replay messages onto the bus** and **delete DLQ entries** — at least as
privileged as the tenant admin — so there is deliberately **no** env var
that mounts it, ever. `server_auth=None` mounts unauthenticated and logs one
WARNING at mount time naming the risk.

## Selecting a DLQ backend via env (Plan 047)

`VARCO_DLQ_BACKEND=<kafka|nats|redis|redis-streams|sa|beanie>` selects the live
`AbstractDeadLetterQueue` backend once its package is bootstrapped — bootstrapping
`varco_kafka`/`varco_sa`/etc. no longer makes it live by itself. Unset means
`InMemoryDeadLetterQueue`. `sa`/`beanie` are *new* DI bindings as of Plan 047 (`SADeadLetterQueue`
was previously hand-constructed only). See
`technical_docs/features/dependency-injection.md`'s "Selecting a backend" section for the
precedence rule and the dotted-path escape hatch.

## Pitfalls

| Pitfall | Fix |
|---|---|
| A poison outbox row silently stops a stream | Wire `retry_policy=` + `dlq=` on `OutboxRelay` |
| `max_attempts` set without a `dlq` | `ValueError` by design — refuse to configure silent data loss |
| `AuditConsumer` never retries a transient DB error | It does by default now (`durable_delivery()`); pass `retry_policy=None` for the old fire-and-forget behaviour |
| A DLQ implementation raises from `push()` | Contract violation — callers (relay, job runner, retry wrapper) cannot recover; fix the implementation to log+swallow |
| `redrive(entry_id)` on Kafka/NATS | `DeadLetterNotAddressable` — use `redrive_batch()` / `--batch` instead (RD-4) |
| Redriving an entry whose handler is still broken | It comes straight back to the DLQ — expected; use `dry_run=True` first and watch `varco.dlq.redriven{status}` |
| `delete_where()` with no predicate | `ValueError` by design — refuses to delete the whole DLQ |
| `list_entries(tenant_id="acme")` misses a framework-level entry | Correct — a `None`-tenant entry is never "every tenant"; use no filter for the operator/global view |
| `mount_reliability_admin()` without `acknowledge_bundled_admin=True` | `ValueError` — bundling admin-adjacent privilege into the app pod is a deliberate, friction-gated choice |

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **Poison outbox row silently stops a stream** | `OutboxRelay` retries the same undeliverable entry forever, blocking every entry queued behind it | No `retry_policy`/`dlq` wired — today's default is unbounded retry-in-place | Wire `retry_policy=` + `dlq=` on `OutboxRelay`; exhausted entries are pushed to the DLQ and deleted so the stream unblocks |
| **`OutboxRelay(max_attempts=...)` without a `dlq`** | `ValueError` at construction | Deleting a poison entry with nowhere durable to put it is silent data loss — refused by design | Pass a `dlq=` (e.g. `SADeadLetterQueue`) alongside `max_attempts` |
| **`list_entries(tenant_id="acme")` misses a framework-level dead letter** | An entry produced outside any tenant context (e.g. a boot-time outbox deserialize failure) never shows up under any explicit `tenant_id=` filter | `DeadLetterEntry.tenant_id=None` is deliberately never matched by `tenant_id="acme"` — a `None` tenant is not "every tenant" (Plan 009, RD-4/R4) | Use no `tenant_id` filter at all for the operator/global view; a `None`-tenant entry is correct, expected behaviour, not a bug |
| **`redrive(entry_id)` called on Kafka/NATS** | `DeadLetterNotAddressable` | Stream-backed stores cannot address a single message by id — `supports_random_access=False` (RD-4) | Use `redrive_batch()` / the CLI's `--batch` flag, which work on every backend |
| **Beanie chunked sweep reports `0` while an entry stored right at the cutoff still matches** | `delete_where(older_than=cutoff, limit=...)`/`list_entries(older_than=cutoff)` misses an entry whose own reported `last_failed_at` is strictly before `cutoff` | BSON is millisecond-precision and pymongo floors the query operand too, so a raw `$lt` was evaluated as `stored_ms < floor_ms(cutoff)` — excluding every entry sharing a millisecond with the cutoff | Fixed — `BeanieDeadLetterQueue` widens `older_than`'s operand to the next whole millisecond (`_bson_time.ceil_to_bson_millisecond`) before querying; `newer_than` is untouched since pymongo's floor is already correct for a lower bound |
| **`mount_reliability_admin()` without `acknowledge_bundled_admin=True`** | `ValueError` at mount time, nothing mounted | This surface can replay bus messages and delete audit/DLQ records — at least as privileged as the tenant control plane (RD-9) | Pass it only after confirming a standalone deployment isn't justified — same rule as `mount_tenant_admin()` |
| **`mount_reliability_admin()` called twice** | Second call silently doubles/duplicates the DLQ+audit admin routes (same `prefix` doubles routes; a different `prefix` produces a second live surface) | No `id(app)` double-mount guard, unlike `mount_tenant_admin()` | Plan 014 / audit F4 — a second call for the same app now raises `ValueError`, same rule as `mount_tenant_admin()`. Calling with neither `audit_repo` nor `dlq` mounts nothing and does not poison the app for a later real mount (deliberate deviation from `mount_tenant_admin()`) |
