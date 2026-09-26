# Cache Hardening (R1) — singleflight, L1 coherence backplane, observability, SWR/jitter/negative caching

Plan 010 (`plans/010-cache-hardening-r1.md`) executes the R1 "hardening" cut
of BACKLOG's cache track: **C2** (stampede protection via singleflight),
**C1** (`LayeredCache` L1 coherence backplane), **C3** (cache observability
pack), **C4** (stale-while-revalidate, TTL jitter, opt-in negative caching).

## What R1 changes and what it does not

**Posture: hardening, not features.** Every new capability is reached by
passing an explicit object or keyword — `CachePolicy()` (the identity
policy), no `singleflight=`, no `backplane=`, no `install_cache_metrics()`
call reproduces today's `varco_core.cache` behaviour byte-for-byte:

- No envelope is ever written to the cache backend.
- A `None` loader result is never cached.
- `LayeredCache.set()`/`delete()`/`clear()`/`delete_prefix()` still issue a
  single `asyncio.gather()` across all layers — no ordering, no publish.
- Every `record_*` cache metric call is a no-op (`install_cache_metrics()`
  was never called).

All four features share one algorithm, `varco_core.cache.readthrough.
read_through()`, because C2 (singleflight) and C4 (stale-while-revalidate)
interact: coalescing a background refresh while concurrently serving a
stale value is a single race that has to be designed once, not twice. See
`varco_core/varco_core/cache/readthrough.py`'s module docstring for the full
6-step algorithm.

```python
from varco_core.cache import CachePolicy, InMemoryCache, Singleflight, read_through

policy = CachePolicy(ttl=300.0, ttl_jitter=0.1, soft_ttl=240.0, singleflight=True)
sf = Singleflight(name="users")


async def loader() -> dict:
    return await db.fetch_one(...)


async with InMemoryCache() as cache:
    value = await read_through(cache, "tenant:acme:User:42", loader, policy, singleflight=sf)
```

`read_through()` never constructs or namespaces the cache key — callers
(`@cached`, `CacheServiceMixin`) own key construction, including any
tenant-namespacing.

## Singleflight (C2)

`varco_core.cache.singleflight.Singleflight` coalesces N concurrent misses
on the same key into one recompute per process. The first caller becomes
the **leader** (runs the loader); every other concurrent caller for the
same key becomes a **follower** and `await asyncio.shield(leader_future)` —
one follower's own `@timeout`/cancellation can never cancel the leader's
shared recompute.

- **Per-process only (Decision D-3).** A `SingleflightProtocol` seam is
  left for a future distributed (cross-pod) implementation
  (`RedisSingleflight`) — not shipped in R1. Per-process coalescing still
  takes concurrent recomputes from `N_pods × C_concurrency` down to
  `N_pods`, at zero added infrastructure.
- **Shared-instance rule.** A `Singleflight` is per-cache-namespace state —
  a per-call instance coalesces nothing, the same defect class as a
  per-call `CircuitBreaker`/`Bulkhead`. `@cached` creates exactly one
  `Singleflight` per decorated function at decoration time;
  `CacheServiceMixin` creates one per service instance (lazily, on first
  use — never in `__init__`, so no `asyncio.Lock` is built outside a
  running event loop).
- **Tenant safety.** The key passed to `Singleflight.do()`/`spawn_refresh()`
  is always the **final**, already-namespaced cache key (the one that has
  already gone through `tenancy_cache_key()` / `CacheServiceMixin.
  _cache_key()`, i.e. it already carries `tenant:{tenant_id}:`).
  `read_through()`/`Singleflight` never build keys themselves — coalescing
  on a pre-namespaced key would let two tenants share one recompute, a
  cross-tenant data leak. Guarded by
  `varco_core/tests/test_cache_singleflight_tenancy.py`.
- **Non-idempotent loaders.** Because a follower never re-runs the loader —
  it only awaits the leader's result — a loader with side effects (e.g. one
  that also increments an external counter) has those side effects run
  **once**, not once per concurrent caller. This is usually what you want
  for a stampede-protected read, but it means singleflight is not a safe
  place to hide a loader that intentionally needs per-caller side effects;
  keep those outside the cached function.

```python
from varco_core.cache import CachePolicy, cached


@cached(cache, policy=CachePolicy(ttl=300.0), singleflight=True, namespace="users")
async def get_user(user_id: int) -> dict:
    return await db.fetch_one("SELECT * FROM users WHERE id = $1", user_id)


# 100 concurrent await get_user(42) on a cold key → loader runs once.
await get_user.aclose()  # drains any outstanding background SWR refresh tasks
```

## Cache observability pack (C3)

`varco_core.observability.cache.install_cache_metrics()` mirrors
`install_reliability_metrics()` exactly: a manual install function,
deliberately **not** a scanned `@Configuration` (a scanned config
auto-activates on `container.scan()`), idempotent, `enabled=False` makes
the whole pack (and every `record_*` call) a cheap no-op.

| Name | Kind | Unit | Attributes |
|---|---|---|---|
| `varco.cache.hits` | counter | `1` | `cache`, `layer`, `kind` (`positive`/`negative`/`stale`) |
| `varco.cache.misses` | counter | `1` | `cache`, `layer` |
| `varco.cache.evictions` | counter | `1` | `cache`, `layer`, `reason` (`capacity`/`ttl`/`explicit`/`backplane`) |
| `varco.cache.duration` | histogram | `ms` | `cache`, `operation` |
| `varco.cache.stampede_suppressed` | counter | `1` | `cache` |
| `varco.cache.stale_served` | counter | `1` | `cache`, `reason` (`soft_ttl`/`error`) |
| `varco.cache.backplane.published` | counter | `1` | `kind` |
| `varco.cache.backplane.received` | counter | `1` | `kind` |
| `varco.cache.backplane.dropped` | counter | `1` | `reason` (`publish_failed`/`decode_failed`) |

Hit **ratio** is derived at query time (`hits / (hits + misses)`) rather
than emitted as its own series (research brief 003 §2, Uptrace's
recommendation).

**Cardinality deny-list** (brief 003 §4): the only attribute values this
pack ever emits are `cache` (a bounded instance/policy name — see
`CachePolicy.name`), `layer` (`l1`/`l2`/…), `operation`
(`get`/`set`/`delete`/`clear`), `kind`, and `reason`. **Never** the cache
key, a tenant id, a user id, or a correlation id.

```python
from varco_core.observability.cache import CacheMetricsConfig, install_cache_metrics

install_cache_metrics(config=CacheMetricsConfig(meter_name="varco", by_layer=True))
```

**`varco.`-prefix rationale (Decision D-2):** consistent with every other
varco instrument (`varco.dlq.pushed`, `varco.outbox.published`,
`varco.job.lease_reaps`); it deliberately leaves the unprefixed `cache.*`
namespace free for a future OTel semantic convention to be adopted
additively rather than by renaming varco's own series.

**Semconv migration path:** no OTel semantic convention for application
cache metrics exists as of v1.44.0 (Aug 2026);
`open-telemetry/semantic-conventions#1747` proposes span attributes, not
metrics, and remains open. If a convention is approved later, realign via
the module-level `METRIC_NAMES` / `ATTR_KEYS` tables in
`varco_core/varco_core/observability/cache.py` (a one-line edit per name),
or an OTel View — do not rename series ad hoc.

**Per-layer hit/miss/eviction instrumentation (Plan 010 step 22).**
`LayeredCache.get()` calls `record_cache_hit(layer=...)`/
`record_cache_miss(layer=...)` for every layer it probes, tagged with the
bounded label of the layer that answered (`"l1"`/`"l2"`/…) — a full miss
therefore records one miss per layer probed, which is what makes a
per-layer hit ratio derivable (`hits_l1 / (hits_l1 + misses_l1)`).
`_on_backplane_message()` calls `record_cache_eviction(layer=...,
reason="backplane")` for each LOCAL layer it evicts on a received
invalidation — never for the last (authoritative) layer, which a received
message never touches. Both call sites pass the label unconditionally;
`_layer_attr()` in `varco_core/varco_core/observability/cache.py` drops the
attribute when `CacheMetricsConfig.by_layer` is `False`. A bare
`InMemoryCache` still emits `evictions{reason=capacity|ttl}` independently
(wired at the eviction sites in `memory.py`), and `read_through()` still
emits `hits`/`misses`/`duration`/`stampede_suppressed`/`stale_served` for
whatever backend it is pointed at (including a `LayeredCache`, but without
a `layer=` attribute — `read_through` has no visibility into which of a
`LayeredCache`'s internal layers actually answered).

## L1 coherence backplane (C1)

A `LayeredCache` in a multi-pod deployment silently serves stale L1 entries
after another pod's write — until now. `varco_core.cache.backplane.
CacheBackplane` is the ABC (same layer split as `AbstractEventBus`/
`RedisEventBus`); `varco_redis.backplane.RedisPubSubBackplane` is the
concrete Redis Pub/Sub implementation (Decision D-1 — settled by research
brief 002: `redis.asyncio` has no `CLIENT TRACKING` support, redis-py issue
#3916 is open with no ETA, and the sync-client workaround would defeat the
point of an async framework). `InMemoryBackplane` (in `varco_core`) is the
standard backplane for unit tests — two `LayeredCache` "nodes" sharing one
`InMemoryBackplane` exchange invalidations synchronously with no Docker.

```python
from varco_core.cache import InMemoryCache, LayeredCache
from varco_redis.backplane import RedisPubSubBackplane
from varco_redis.cache import RedisCache, RedisCacheSettings

l1 = InMemoryCache()
l2 = RedisCache(RedisCacheSettings(url="redis://localhost:6379/0"))
backplane = RedisPubSubBackplane()

async with LayeredCache(l1, l2, promote_ttl=60, backplane=backplane) as cache:
    await cache.set("product:1", product, ttl=300)  # other pods' L1 is invalidated
```

Five design rules, each closing a hazard named in research brief 002:

1. **`publish()` must never raise** — identical contract to
   `AbstractDeadLetterQueue.push()`. By the time a backplane publish
   happens, the authoritative L2 write has already landed and cannot be
   unwound; a failed publish is swallowed and recorded as
   `backplane.dropped{reason="publish_failed"}`.
2. **Publish happens strictly after the authoritative-layer write.**
   `LayeredCache.set()`/`delete()`/`delete_prefix()`/`clear()` take an
   ordered path (`await last_layer.op(...)` then `gather(*faster_layers,
   publish())`) **only when a backplane is wired** — the no-backplane
   `asyncio.gather` path is untouched. Cost: write latency goes from
   `max(L1,L2)` to `L2 + max(faster)`. Publishing before the authoritative
   write would let a fast receiving node re-read L2, see the *old* value,
   and re-promote it — making the staleness permanent, strictly worse than
   no backplane.
3. **A received message evicts local layers only** — never the last
   (authoritative) layer. Propagating a received invalidation back to L2
   would amplify one write into a fleet-wide storm.
4. **Echo suppression** — a node skips messages whose origin is its own.
5. **Bounded staleness enforced at construction** —
   `LayeredCache(..., backplane=X, promote_ttl=None)` raises `ValueError`.
   Pub/Sub is fire-and-forget: a subscriber disconnected at publish time
   never receives the message, with no queue and no replay. A short,
   mandatory L1 TTL bounds the damage — the industry answer (Redisson,
   FusionCache).
6. **Flush L1 on reconnect** — `RedisPubSubBackplane` emits a synthetic
   `kind="clear"` to its local handler after re-subscribing following a
   connection loss, since any invalidation published while disconnected was
   silently dropped.

**Key-name exposure.** A backplane broadcasts key *names* on a shared
channel, and varco keys are already tenant-namespaced
(`tenant:{tenant_id}:Entity:pk`). Under `TenantIsolation.SHARED` every pod
already serves every tenant, so nothing new is exposed. Under
`SCHEMA`/`DATABASE` with per-tenant pods this is new cross-tenant activity
metadata. Two opt-outs:

- `RedisPubSubBackplane(channel_for=lambda key: ...)` — derive the channel
  from the key's `tenant:{id}:` segment so a node subscribes only to the
  tenants it hosts.
- `RedisPubSubBackplane(hash_keys=True)` — publish `sha256(key)[:16]`
  instead of the raw key. `kind="prefix"` degrades to a local `"clear"`
  under this mode (a hash cannot be prefix-matched) — documented, not
  silent.

The default is one channel with plaintext keys (matching Redisson/
FusionCache).

**When to prefer Redis Streams instead (R2).** A Pub/Sub backplane is
best-effort by construction — a disconnected subscriber loses messages
with no replay. If your deployment needs durable, replayable invalidation
(e.g. a node that is down for minutes must still catch up exactly, not just
rely on a bounded `promote_ttl`), a Streams-backed backplane is the
appropriate mechanism — deferred to R2 behind the same `CacheBackplane`
ABC, so no call site changes when it ships.

## Stale-while-revalidate, jitter, negative caching (C4)

All three share `CachePolicy` and the envelope wire format
(`varco_core.cache.envelope.CacheEnvelope`), frozen in Phase 0 even though
only C4 activates `soft_expires_at`.

```python
from varco_core.cache import CachePolicy

policy = CachePolicy(
    ttl=300.0,  # hard TTL
    ttl_jitter=0.1,  # ±10% randomized TTL — avoids a synchronized expiry cliff
    soft_ttl=240.0,  # stale-while-revalidate window (must be < ttl)
    negative_ttl=30.0,  # opt-in negative caching (D-4) — shorter than ttl
    stale_if_error=600.0,  # serve stale if the loader raises within this window of hard expiry
    singleflight=True,
)
```

- **Soft/hard TTL model.** A soft-expired entry is served **immediately**
  (the caller never awaits a refresh) and exactly one background refresh is
  triggered through the *same* `Singleflight` slot a cold miss would use
  (the C2×C4 interaction). `refresh_mode="blocking"` awaits the refresh
  instead of returning stale. A hard-expired entry blocks and recomputes,
  same as a cold miss.
- **C2×C4 interaction.** Concurrent soft-stale readers land on one
  `Singleflight` slot → exactly one refresh runs; a cold reader arriving
  during an in-flight refresh becomes a follower of that same future rather
  than starting a second recompute. This is why `read_through()` is one
  function, not two independent features.
- **Negative caching is opt-in (Decision D-4).** `CachePolicy().
  negative_ttl` defaults to `None` — a `None` loader result is never
  cached, identical to pre-Plan-010 `@cached`. Setting `negative_ttl`
  caches the `None` result (as a negative envelope) for that many seconds;
  a subsequent read returns `None` without calling the loader and records
  `hits{kind="negative"}`. **Choose `negative_ttl` shorter than the
  positive `ttl`** — a `negative_ttl` longer than your fix loop keeps
  serving "not found" after the row exists; invalidate explicitly
  (`cache.delete(key)`) on create if you can't wait it out.

### The two-step rolling-deploy recipe for enabling envelope mode (D-5)

`CacheEnvelope` is written **only** when the active policy needs it (any of
`soft_ttl`/`negative_ttl`/`stale_if_error` set). `read_through()` tolerates
both shapes on read — a payload without the `__varco_cache__` marker is
treated as a fresh legacy value, so a **new** pod reading an **old** pod's
entries is always safe.

The unsafe direction is the reverse: an **old** pod (running pre-Plan-010
`varco_core`, or a Plan-010 pod whose policy never sets an envelope-
requiring field) reading a **new** pod's envelope returns the raw wrapper
dict (`{"__varco_cache__": 1, "v": ..., ...}`) to the application, not the
unwrapped value.

Therefore, enabling an envelope-requiring policy field (`soft_ttl`,
`negative_ttl`, or `stale_if_error`) against a **shared L2** cache is a
two-step deploy:

1. Roll out the new varco version to every pod, with the envelope-requiring
   policy fields still off (`CachePolicy(ttl=...)` only).
2. Once every pod is running code that can `unwrap()` an envelope, turn the
   flags on (`soft_ttl=`/`negative_ttl=`/`stale_if_error=`).

A single-step alternative — prefixing envelope keys with `v2:` so the two
wire shapes never collide — was considered and rejected: it cold-starts the
entire cache on enable *and* on rollback, permanently doubles the key
namespace, and bakes a wire version into every application-visible key. A
documented two-step deploy costs one paragraph and no runtime complexity.

## Bulk operations — `BulkCache`, `get_many`/`set_many`/`delete_many` (Plan 011 / C5)

Closes: "a list endpoint that caches N items makes N round trips instead of
one." Off by default in the only sense that matters here — nothing calls
the batch path unless a caller opts in (see the `CacheServiceMixin` status
note below).

### Why a separate `BulkCache` Protocol, not new methods on `AsyncCache` (D-11)

`AsyncCache` is `@runtime_checkable`, and a `runtime_checkable` Protocol's
`isinstance()` check tests **method presence**. Adding `get_many`/
`set_many`/`delete_many` directly to `AsyncCache` would silently flip
`isinstance(some_third_party_cache, AsyncCache)` to `False` for every
out-of-tree implementation that only has the original five methods — a
worse defect than the one C5 fixes, and invisible until some unrelated code
path's `isinstance` check breaks. So:

- `AsyncCache` is **unchanged** — not one line.
- `BulkCache` (`varco_core.cache.base`, `@runtime_checkable`) is a new,
  additive Protocol: `get_many(keys, *, type_hint=None) -> dict`,
  `set_many(items, *, ttl=None) -> None`, `delete_many(keys) -> None`.
- `CacheBackend` (the ABC every shipped backend subclasses) gets the three
  methods as **concrete, portable defaults** — loops over `get`/`set`/
  `delete`. Every shipped backend satisfies `BulkCache` immediately,
  correctly, at today's performance; each backend then *overrides* with its
  native batch command as an optimization, never as a correctness fix. Same
  "portable default vs. concrete-but-raising" discipline Plan 009 applied
  to `AbstractDeadLetterQueue`/`AbstractJobStore` — every `BulkCache` method
  has a correct portable default, so nothing here is concrete-but-raising.

| Backend | Native override |
|---|---|
| `InMemoryCache` | Loop (already O(1) per key — no native batch primitive to reach for) |
| `RedisCache` (`varco_redis`) | `MGET` for `get_many`; a pipelined `SET`/`EXPIRE` sequence for `set_many`; pipelined `DEL` for `delete_many` |
| `MemcachedCache` (`varco_memcached`) | `get_multi`/`set_multi` (native `aiomcache` batch commands) |
| `LayeredCache` | Per key, walking L1→Ln with the same promotion semantics as `get()` (see D-12 below for `set_many`/`delete_many`'s backplane behaviour) |

### The serializer seam — reused, not reinvented

`CacheBackend.__init__(self, *, serializer: Serializer[Any] | None = None)`
reuses the **existing** `varco_core.serialization.Serializer` Protocol
(`serialize(value) -> bytes` / `deserialize(data, type_hint=None) -> T`) —
whose own docstring already names `JsonSerializer` for cache values as its
motivating case. Inventing a second cache-specific serializer protocol
would have been the "implement the same interface twice" mistake
CLAUDE.md's pre-implementation checklist exists to catch. `serializer=None`
(the default) preserves each backend's *exact* current behaviour:

| Backend | Default serializer |
|---|---|
| `RedisCache` | `JsonSerializer` |
| `MemcachedCache` | Its existing bytes codec |
| `InMemoryCache` | `NoOpSerializer` — raw Python objects, no serialization |

### D-12 — `set_many` under a `LayeredCache` backplane: N messages, not a batch verb

`LayeredCache.set_many()`/`delete_many()` obey Plan 010's write-ordering
rule verbatim — authoritative (last) layer first, then faster layers, then
publish — and emit **one `InvalidationMessage(kind="key", ...)` per key**,
never a new `kind="keys"` carrying a list.

Rejected alternative: a batched `kind="keys"` message. ✅ one Pub/Sub
message instead of N. ❌ Plan 010 froze `InvalidationMessage`'s wire format
*deliberately* (D-5 there: "adding a field to it later would be a second,
avoidable rolling-deploy hazard") — a Plan-010-era subscriber receiving
`kind="keys"` drops it as undecodable, meaning a mixed-version fleet
silently loses invalidations during a rolling deploy, precisely the defect
class Plan 010 C1 exists to close. N cheap messages beat a coherence
regression. A future batched-invalidation wire format is a deliberate,
versioned rollout, not a drive-by addition here.

### `read_through_many()` — shares `Singleflight` slots with `read_through()`

```python
from varco_core.cache.readthrough import read_through_many

results = await read_through_many(cache, keys, loader, policy, singleflight=sf)
# {key: value_or_None} — every requested key present, even ones the loader omitted
```

Same `Singleflight` **instance** as plain `read_through()` — one in-flight
slot per key — so a bulk read and a concurrent single-key read of the same
key coalesce with each other rather than racing. Fresh/negative hits
resolve immediately; a soft-stale key is returned now and spawns at most
one background refresh through the same per-key slot `read_through()` uses;
the loader is invoked **once**, with only the keys that are actually
missing after singleflight coalescing removes followers. RD-6/Plan 010's
tenant rule still binds and is retested here: the coalescing key passed to
`Singleflight` must always be the final, already-namespaced cache key.

### `CacheServiceMixin._use_bulk_cache` — `list()` routes through `read_through_many()`

`CacheServiceMixin._use_bulk_cache: ClassVar[bool] = False` gates whether
`list()` takes the `BulkCache` batch path. When `True` **and** `self._cache`
satisfies `BulkCache` (`isinstance(self._cache, BulkCache)`), `list()` calls
`read_through_many()` with its single, already-namespaced list key instead
of the plain `cache.get()`/`cache.set()` pair — reusing the existing C5
batch primitive (`get_many`/`set_many`) rather than a second implementation.
`list()` still caches its *entire* result under one hashed key (see
`_cache_list_key()`), so today the batch path buys `read_through_many()`'s
envelope/SWR/negative-caching/singleflight machinery for that one key rather
than a genuine N-key round trip — a service that wants a true one-round-trip
multi-entity batch read should call `read_through_many()` directly with its
own per-entity keys. `False` (default) — the existing loop-shaped body runs
verbatim, byte-identical to pre-Plan-011 behaviour (RD-1).

## Framework table & migration impact

None. R1 adds no framework table, no Alembic revision, and no CLI verb.
C5 (Plan 011) adds none either — `BulkCache`'s bulk methods have no
persistent shape of their own; they compose with whatever storage the
underlying `CacheBackend` already has. The
only persistent-shape change is the **value** written into an existing
cache key when an envelope-requiring policy field is enabled — handled by
the two-step deploy recipe above, not a schema change.

## See also

- `plans/010-cache-hardening-r1.md` — the authoritative plan (Design,
  Decisions, Edge cases, Risks sections).
- `plans/011-i18n-timezone-and-cache-bulk-ops.md` — C5 (bulk ops), D-11,
  D-12.
- `varco_core/varco_core/cache/readthrough.py` — the one read-through
  algorithm all four features share.
- `varco_core/varco_core/cache/singleflight.py`,
  `varco_core/varco_core/cache/policy.py`,
  `varco_core/varco_core/cache/envelope.py`,
  `varco_core/varco_core/cache/backplane.py`,
  `varco_core/varco_core/observability/cache.py`,
  `varco_redis/varco_redis/backplane.py`.

## Selecting a cache backend via env (Plan 047)

`VARCO_CACHE_BACKEND=<redis|layered|memcached>` selects the live `CacheBackend` — bootstrapping
`varco_redis`/`varco_memcached` no longer makes their cache live by itself; unset means
`InMemoryCache`. `redis` and `layered` used to tie whenever `varco_redis` was scanned recursively
(`AMBIGUOUS_BINDING`); each is now its own gated value, so exactly one is ever active. See
`technical_docs/features/dependency-injection.md`'s "Selecting a backend" section.

## Pitfalls

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **Cache metrics never appear** | Dashboards for `varco.cache.*` stay empty even though the cache is being hit | `install_cache_metrics()` (`varco_core.observability.cache`) was never called — same rule as `install_reliability_metrics()`: a manual install function, deliberately not a scanned `@Configuration` | Call `install_cache_metrics()` once at startup |
| **`LayeredCache` in multi-pod without a backplane** | Each pod's L1 silently serves stale entries after another pod's write/delete — the shipped bug Plan 010 / C1 closes | No `backplane=` wired — the default `LayeredCache(l1, l2, promote_ttl=...)` has no cross-node invalidation channel | Wire `backplane=RedisPubSubBackplane()` (`varco_redis.backplane`) for any `LayeredCache` shared across more than one process |
| **`LayeredCache(backplane=..., promote_ttl=None)`** | `ValueError` at construction | A Pub/Sub backplane is best-effort (message loss on subscriber disconnect); an unbounded L1 TTL behind it means a missed invalidation has no bound on how long it can serve stale data | Pass a `promote_ttl=` alongside `backplane=` — mirrors `OutboxRelay(max_attempts=...)` refusing to run without a `dlq=` |
| **Backplane key names visible fleet-wide** | Under a per-tenant-pod topology (`SCHEMA`/`DATABASE` isolation), every subscriber learns which tenant touched which entity id | The default `RedisPubSubBackplane` publishes one plaintext channel with raw key names (`tenant:{id}:Entity:pk`) | Use `channel_for=` (subscribe only to hosted tenants) or `hash_keys=True` (publish a key hash — degrades `delete_prefix()` invalidation to a local `clear` on receivers, documented not silent) |
| **`soft_ttl >= ttl`** | `ValueError` at `CachePolicy` construction | A soft TTL at or beyond the hard TTL can never fire — the SWR window would be dead code | Set `soft_ttl` strictly less than `ttl` |
| **Enabling envelope mode mid-rolling-deploy** | An **old** pod (or a pod whose policy doesn't set `soft_ttl`/`negative_ttl`/`stale_if_error`) reads a **new** pod's envelope and returns the raw `{"__varco_cache__": 1, ...}` wrapper dict to the application instead of the unwrapped value | `CacheEnvelope` is only tolerant on read in the safe direction (new pod reading old pod's legacy value) — the reverse direction is unsafe by design (D-5) | Roll out the new varco version to every pod with envelope-requiring policy fields off first, then turn them on — see the two-step deploy recipe in `technical_docs/features/cache-hardening.md` |
| **Negative caching hiding a fixed row** | A "not found" response keeps being served long after the underlying row was created | `negative_ttl` was set longer than the operational fix loop for the missing row | Keep `negative_ttl` short (shorter than `ttl`), or invalidate explicitly (`cache.delete(key)`) when the row is created |
