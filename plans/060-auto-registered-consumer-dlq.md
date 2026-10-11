# Plan 060 — Auto-registered `EventConsumer`s receive the app's DLQ (RO-14)

Covers BACKLOG 4.0.0-feature-extension row **`RO-14`** (`BACKLOG.md:56` — *"Auto-registered
`EventConsumer`s receive the app's DLQ — `CoreModule.start()`'s auto-`register_to()` passes the
bound `AbstractDeadLetterQueue`"*, 🔴 must, S; rationale *"Failing handlers past retry currently
lose the message; breaking, so it rides the major"*), pulled into 4.0.0 by the locked decision at
`BACKLOG.md:36`. Origin: Plan 050 §Non-goals (`plans/050-phased-lifecycle-and-di-wired-infrastructure.md:77-79`)
and §Parked (`:939-940`), which deferred it as *"a behaviour change for every consumer"*.

One of eleven plans (059–069) in the 4.0.0 feature extension (index
`plans/000-index-4-0-feature-extension.md`). Breaking changes are allowed outright
(`BACKLOG.md:31,43`); the done criteria include a `3-to-4.md` section and a CHANGELOG `[4.0.0]`
BREAKING block.

**Sources of record read for this plan (2026-10-07, every claim below re-read in source):**
`varco_core/varco_core/di/module.py:580-916` (`CoreModule`); `varco_core/varco_core/event/consumer.py:116-140`
(`_UNSET`), `:474-667` (`_make_retry_wrapper`), `:673-1101` (`EventConsumer`);
`varco_core/varco_core/reliability/{preset,wiring}.py`; `varco_fastapi/varco_fastapi/reliability.py:38-140`;
`varco_core/varco_core/webhook/{dispatcher,module}.py`; `varco_core/varco_core/service/audit.py:699-811`;
`varco_core/varco_core/tenancy/control/{readiness,consumer}.py`; `varco_core/varco_core/di/{seams,select,report,diagnose,__init__}.py`;
`varco_core/varco_core/event/{dlq,memory}.py`; `varco_kafka/varco_kafka/bus.py:490-565`;
`varco_redis/varco_redis/streams.py:200-224,655-714`; `varco_nats/varco_nats/bus.py:640-729`;
`varco_fastapi/varco_fastapi/posture.py:1175-1230`, `modules/lifecycle.py:578-581,776-778`;
`varco_core/tests/test_event_consumer_autoregister.py`, `test_fallback_defaults.py:55-97`,
`test_webhook_module.py`, `test_reliability_preset.py:24-57`; `testkit/varco_conformance/di_wiring.py:529-545`;
`CHANGELOG.md:1-24,295-327`; `technical_docs/migrations/3-to-4.md:213-231`;
`technical_docs/features/dead-letter-queues.md` (headings), `dependency-injection.md:590,905-906`;
`README.md:1900,4364-4389`; `examples/00-full-stack-post-api/example/consumer.py:62-171`.

> ⚠️ **Three premises in the row's literal wording were corrected from source before anything was
> decided.**
> (1) **"`register_to()` passes the bound DLQ" cannot mean the existing `dlq=` keyword.** Tier 2 of
> the resolution order is all-or-nothing (`consumer.py:957-959`: `elif retry_policy is not None or
> dlq is not None: effective_retry_policy = retry_policy; effective_dlq = dlq`), so
> `register_to(bus, dlq=<bound>)` would make every bare handler skip tier 3 — silently **dropping a
> user's `set_default_reliability_preset()` retry policy**. And four in-tree overrides give an
> explicit `register_to(dlq=)` precedence over their constructor DLQ (`webhook/dispatcher.py:213-221,240-241`,
> `service/audit.py:796`, `tenancy/control/readiness.py:163`, `tenancy/control/consumer.py:232`) —
> passing the bound DLQ there would overwrite a deliberately chosen one. (§D-carrier)
> (2) **A DLQ without a retry policy short-circuits broker redelivery.** With `policy=None` and a
> `dlq`, the wrapper runs the handler **once** and dead-letters on first failure
> (`consumer.py:517-518,539`). Applied to every bare handler, that converts Redis Streams' PEL
> redelivery (`streams.py:661-671`, unlimited by default `:208-210`), NATS JetStream's `nak()` up to
> `max_deliver` (`nats/bus.py:651-680`) and an `InMemoryEventBus` SYNC publisher's
> `ExceptionGroup` (`memory.py:126-131`) into single-attempt dead-lettering — and the default DLQ
> is the in-process one (`seams.py:81-87`, `dlq.py:523-525`). The row's own rationale is *"past
> retry"*; §D-tier honours exactly that.
> (3) **The DLQ-only wrapper also swallows `asyncio.CancelledError`.** `except BaseException`
> (`consumer.py:589`) re-raises only via `policy.is_retryable()` (`:593`); with `policy=None` a
> cancelled handler is dead-lettered and swallowed. §D-tier never routes the *default* through
> that path, so this plan does not promote the latent bug — it is reported as an open question,
> not fixed here (Non-goals).

## Goal

After this plan, an `EventConsumer` that `CoreModule` auto-registers (`module.py:877-894`)
dead-letters every `@listen` handler that **retries and then exhausts** into the container's bound
`AbstractDeadLetterQueue`, unless that handler, its `register_to()` call, or the process-wide
`ReliabilityPreset` already chose a DLQ — via a new lowest-precedence `register_to(...,
default_dlq=)` keyword. Bare handlers (no retry policy resolved) keep their 3.x semantics. Three
opt-outs exist (handler `dlq=None`, consumer `use_default_dlq = False`, app
`CoreModule(consumer_dlq=False)`); the four in-tree `register_to` overrides honour the keyword;
`CoreModule.validate()` warns when an env-selected durable bus feeds the in-process default DLQ;
README, both feature docs, CLAUDE.md, CHANGELOG `[4.0.0]` and `3-to-4.md` describe it.

## Non-goals

- **Dead-lettering bare handlers** (no retry policy resolved). Rejected in §D-tier — it would break
  broker redelivery on Streams/JetStream. A user who wants it sets a preset retry policy
  (`ReliabilityPreset(retry_policy=…)`, `dlq=None` → the app DLQ fills in) or `@listen(dlq=…)`.
- **Fixing the `CancelledError` swallow in the DLQ-only wrapper path** (premise (3)). Pre-existing,
  not reached by this plan's default; reported as Open question 3 for its own `RO-*` row.
- **Changing `ReliabilityPreset` / `set_default_reliability_preset()` semantics** (tier 3's
  all-or-nothing rule at `consumer.py:929-967` and the RD-7 sentinel at `:119-140` are untouched).
- **Hand-registered consumers** — `ReliabilityLifecycle`'s `AuditConsumer` (`varco_fastapi/reliability.py:116-117`),
  `varco_sa/audit.py`/`varco_beanie/audit.py`, `EventConsumer.start()`'s `_bus` path
  (`consumer.py:1042-1101`), `auto_register = False` consumers. They gain the keyword and may pass
  it; nothing passes it for them.
- **Wrapping `WebhookDispatcher._on_trigger`** with the default DLQ (§D-overrides): a redrive of a
  whole trigger would re-deliver to subscriptions that already succeeded.
- **A new posture finding.** `di.in_process_backend` already reports an in-memory `dlq` seam through
  the opt-in `DIPostureCollector` (`posture.py:1175-1230`); Plan 054 decided in-process backends are
  a legitimate shape and kept it opt-in (`modules/lifecycle.py:776-778`). Not re-decided.
- **Bus-level DLQs** (`RedisStreamEventBus(dlq=, max_delivery_count=)`, `streams.py:208-212`) and
  `OutboxRelay`/`JobRunner` dead-letter paths — unchanged.
- **DLQ-depth metrics for the container DLQ** — `install_reliability_metrics(dlq=preset.dlq)`
  (`varco_fastapi/reliability.py:78-82`) still observes only the preset's DLQ.
- **An integration test.** The change is entirely in-process wrapper composition: the broker sees a
  handler that *returns* instead of *raises*, and each bus's own suite already proves its
  return-path ack/commit. The broker-side consequence is documented (§Behaviour matrix), not
  re-tested.
- **Neighbour scope:** 059 (`TEST-1`, test speed — this plan's tests use zero-delay policies so they
  need no fake clock), 061 (`ACID-2`), 062/063 (`ACID-1` — whether a handler runs in a
  transaction), 064–066 (`GDPR-*`), 067/068 (`STREAM-*`), 069 (`ORCH-1` — the job runner's own DLQ
  path).
- **Any change to providify.**

## Context (verified)

| Fact | Where |
|---|---|
| `_start_consumers()` collects via `acollect(container, EventConsumer)` and calls `consumer.register_to(bus)` with no `dlq` for every `auto_register and not is_registered_to(bus)` consumer | `di/module.py:877-894` |
| It runs in the `core.consumers` component at `Phase.CONSUMERS`, after `core.stores` (`INFRA`, which already resolves and starts `AbstractDeadLetterQueue`) and `core.bus` (`BUS`) | `di/module.py:717-755`, `:757-794` (DLQ at `:781`) |
| `CoreModule.__init__(*, enabled=None, job_runner=True)`; `validate()` reads `get_all_bindings(EventConsumer)` only (never resolves) | `di/module.py:606-619`, `:670-690` |
| `register_to(bus, *, retry_policy=None, dlq=None)`; three tiers: `@listen` own params (an entry that set **either** field, `_UNSET`→`None` for the other) > `register_to` params (all-or-nothing when either non-`None`) > global preset | `event/consumer.py:795-801`, `:915-967` |
| A wrapper is built only when retry, DLQ or deduplicator is set; otherwise the raw method is subscribed | `consumer.py:987-1001` |
| DLQ-only mode = one attempt, then DLQ; no DLQ = `RetryExhaustedError` re-raised to the bus; non-retryable exception under a policy propagates | `consumer.py:517-521`, `:539`, `:589-594`, `:634-665` |
| `auto_register: ClassVar[bool] = True` | `consumer.py:765-769` |
| The preset defaults to `off()` (both `None`); nothing in-tree calls `set_default_reliability_preset()`; `ReliabilityLifecycle` never sets it | `reliability/wiring.py:25-53`, `reliability/preset.py:76-101`; `rg 'set_default_reliability_preset\('` → definition only; `varco_fastapi/reliability.py:59-117` |
| Every `bootstrap()` container resolves an unqualified `AbstractDeadLetterQueue` → `InMemoryDeadLetterQueue` (`@Fallback @Singleton(qualifier="in_memory")`, scanned) | `di/__init__.py:265`, `event/dlq.py:523-525`, `tests/test_fallback_defaults.py:58-60,85-97`; seam `VARCO_DLQ_BACKEND` default `memory` (`di/seams.py:81-87`) |
| `WebhookModule`'s provider injects the bound DLQ into `WebhookDispatcher(dlq=)` (optional), and the dispatcher is auto-registered by `CoreModule` | `webhook/module.py:140-154`, `:156-164` |
| `WebhookDispatcher.register_to` captures `dlq` as instance state and calls `super().register_to(bus)` **bare** — deliberately not activating the generic wrapper; its own per-subscription loop dead-letters | `webhook/dispatcher.py:13-25`, `:213-242`, `:254-292`, `:373-418` |
| Three more overrides resolve `_UNSET`-sentinel `retry_policy`/`dlq` (constructor DLQ unless overridden) and pass explicit values to `super()` / their own wrapper | `service/audit.py:89,754-811`; `tenancy/control/readiness.py:64,117,143,154-167`; `tenancy/control/consumer.py:93,149,225-254` |
| Module `validate()` messages become `module.<name>.validate` **WARNING** findings; a WARNING refuses startup only under `strict=True` | `di/report.py:793-800`; `di/diagnose.py:247-298` |
| `inspect_backend_selection()` reads `os.environ` + `is_user_bound()` — no resolution | `di/select.py:445-504` |
| The full-stack example teaches a private module-level `InMemoryDeadLetterQueue` in `@listen(dlq=_dlq)` — dead letters the admin mount and `DlqRedriver` can never see | `examples/00-full-stack-post-api/example/consumer.py:62-71,126-131,161-166` |

## Design

### §D-tier — the bound DLQ is the lowest tier, per-field, and only for handlers that retry

**Chosen: a fourth, lowest tier that fills only the DLQ slot.** After tiers 1–3 resolve
`(effective_retry_policy, effective_dlq)` exactly as today, one rule runs:

```
effective_dlq = default_dlq   iff   effective_dlq is None
                                 and the @listen entry did not pass dlq= itself   (entry.dlq is _UNSET)
                                 and effective_retry_policy is not None            ("past retry")
                                 and default_dlq is not None
```

It is one pure helper, `_fill_default_dlq(*, dlq, retry_policy, default_dlq, dlq_explicit) ->
AbstractDeadLetterQueue | None` in `event/consumer.py`, so the base `register_to` and the three
`_UNSET` overrides apply the *same* rule (precedent for a private cross-module helper inside
`varco_core`: `consumer.py:1009` imports `_make_inbox_wrapper`). In the base class
`dlq_explicit = entry.dlq is not _UNSET`; tier 2's `dlq=None` and the preset's `dlq=None` mean
"no preference" (the base signature cannot express an explicit `None` there — documented).

#### Behaviour matrix (an auto-registered consumer, `CoreModule(consumer_dlq=True)`, default)

| Declaration | Preset | 3.x on exhaustion | 4.0 |
|---|---|---|---|
| `@listen(E)` | `off()` | exception → bus | **unchanged** (raw handler, no wrapper) |
| `@listen(E)` | `retry=R, dlq=None` | R attempts → `RetryExhaustedError` → bus | R attempts → **app DLQ** |
| `@listen(E)` | `retry=R, dlq=P` | → P | unchanged |
| `@listen(E, retry_policy=R)` | any | R → `RetryExhaustedError` → bus | R → **app DLQ** |
| `@listen(E, retry_policy=R, dlq=None)` | any | → bus | unchanged — the per-handler opt-out |
| `@listen(E, dlq=X)` / `@listen(E, retry_policy=R, dlq=X)` | any | → X | unchanged |
| `@listen(E, retry_policy=None)` | any | one attempt → bus | unchanged (no retry → no fill) |
| `@listen(E, deduplicator=d)` only | `off()` | raw-with-dedup → bus | unchanged |
| consumer `use_default_dlq = False`, or `CoreModule(consumer_dlq=False)` | any | — | exactly 3.x |

#### What the bus sees for a retrying handler past retry (documentation, not code)

| Bus | 3.x | 4.0 |
|---|---|---|
| `InMemoryEventBus` SYNC (`memory.py:126-131`) | `ExceptionGroup` reaches the publisher | `publish()` returns; entry in the DLQ |
| `RedisStreamEventBus` (`streams.py:661-671`) | not `XACK`ed, redelivered from the PEL up to `max_delivery_count` (`0` = unlimited) | DLQ push, then `XACK` |
| NATS JetStream (`nats/bus.py:651-680`) | `nak()` until `max_deliver`, then `term()` (lost) | DLQ push, then `ack()` |
| Kafka `AT_LEAST_ONCE` (`kafka/bus.py:494-499,544-562`) | logged and skipped; offset left uncommitted | DLQ push, then commit |
| Kafka `EXACTLY_ONCE` (`:506-511,534-542`) | transaction aborted | DLQ push (**outside** the Kafka transaction), then commit |

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: lowest tier, DLQ slot only, only when a retry policy resolved** | The row's literal rationale ("past retry"); every explicit choice (handler, `register_to`, preset) still wins; bare handlers keep broker redelivery and SYNC error propagation; never enters the DLQ-only path, so premise (3) is not promoted; a preset with `dlq=None` now "just works" — the bound DLQ feeds it | Per-field at tier 1 is asymmetric with tier 3 (the preset still never fills one field of an entry that declared the other) — stated in the `_UNSET` docstring and the feature doc; a bare handler's failure on a non-redelivering bus is still lost (Non-goals, workaround documented) |
| (2) the row literally: `register_to(bus, dlq=<bound>)` | Zero new API | Premise (1): drops a preset's retry policy (tier 2 is all-or-nothing) and overrides four classes' constructor DLQs. Rejected |
| (3) fill every entry, bare included | Catches every failure | Premise (2): single-attempt DLQ for every bare handler — defeats Streams/JetStream redelivery into an in-memory DLQ by default; every SYNC in-memory publisher stops seeing handler errors; promotes the `CancelledError` swallow. Rejected |
| (4) `CoreModule` calls `set_default_reliability_preset(preset with dlq=<bound>)` | Reuses tier 3 | A process-global, last-writer-wins mutation (`wiring.py:39-45`) from one container's lifecycle — two containers in one process clobber each other; and it would still miss `@listen(retry_policy=R)` handlers. Rejected |
| (5) skip the fill when the DLQ is in-process but the bus is durable | Never lands a durable bus's failures in memory | A hidden, selection-dependent rule; the operator learns nothing. Rejected — surfaced by §D-report instead |

### §D-carrier — a new `default_dlq=` keyword on `register_to()`

`register_to(self, bus, *, retry_policy=None, dlq=None, default_dlq=None)`. `CoreModule` passes
**only** `default_dlq=`, so tiers 1–3 resolve exactly as today. The keyword is passed only when
there is something to pass (§D-optout), so a consumer that opts out never sees it.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: keyword-only `default_dlq=`** | Explicit, typed, greppable; `register_to` is already the documented imperative wiring seam (`consumer.py:820-823`); a hand caller (tests, scripts, fan-out) can use it too | An out-of-tree subclass overriding `register_to` with the 3.x signature gets `TypeError` at start — mitigated: `CoreModule.validate()` names the class and both fixes before start (§D-report), and `use_default_dlq = False` keeps the 3.x signature working; 4.0 permits the break (`BACKLOG.md:43`) |
| (2) a `ContextVar` set around `_start_consumers`'s loop, read by the base `register_to` | No signature change | Ambient wiring state nobody can see in a signature; tests must enter the context; a new use of the `AmbientVar` pattern for wiring-time config. Rejected |
| (3) `CoreModule` sets an attribute on the consumer before `register_to(bus)` | No signature change | Framework mutating user objects from outside; state lingers after registration (a later hand `register_to` silently inherits it). Rejected |

### §D-optout — three opt-outs, one per level

- **Handler:** `@listen(..., dlq=None)` — already the RD-7 sentinel meaning (`consumer.py:124-128`).
  `retry_policy=None` alone opts out of retries only (and therefore of the fill — no retry, no fill).
- **Consumer:** `use_default_dlq: ClassVar[bool] = True` on `EventConsumer`, beside
  `auto_register` (`consumer.py:765-769`). `CoreModule` omits `default_dlq=` for a `False` class.
- **App:** `CoreModule(*, enabled=None, job_runner=True, consumer_dlq=True)` — the same shape as
  `job_runner=` (`module.py:606-619`); `False` restores 3.x exactly. Code-controlled, never an env
  var (CLAUDE.md: never an env read inside `bootstrap()`; a module kwarg is the feature-flag shape
  049 uses).

### §D-overrides — the four in-tree `register_to` overrides

| Class | Rule after this plan |
|---|---|
| `WebhookDispatcher` (`webhook/dispatcher.py:223-242`) | accepts `default_dlq`; `if dlq is not None: self._dlq = dlq` (unchanged) `elif self._dlq is None and default_dlq is not None: self._dlq = default_dlq`; **still** `return super().register_to(bus)` — `default_dlq` is never forwarded, so `_on_trigger` stays unwrapped and the dispatcher has exactly one DLQ path (its per-subscription loop, `:373-418`). Under `WebhookModule` `_dlq` is already the bound DLQ by injection (`webhook/module.py:144-149`), so the keyword is a no-op there — no double wiring. A DESIGN comment records why it is not forwarded |
| `AuditConsumer` (`service/audit.py:772-811`) | accepts `default_dlq`; the explicit fire-and-forget branch (`retry_policy is None and effective_dlq is None`, `:798-806`) runs **before** the fill and is unchanged; otherwise `effective_dlq = _fill_default_dlq(dlq=effective_dlq, retry_policy=effective_retry_policy, default_dlq=default_dlq, dlq_explicit=dlq is not _UNSET)` |
| `TenantReadinessCoordinator` (`tenancy/control/readiness.py:154-167`) | same fill, `dlq_explicit = dlq is not _UNSET` |
| `TenantProvisionConsumer` (`tenancy/control/consumer.py:225-254`) | same fill before building its own `_make_tenant_retry_wrapper` |

Constructor DLQ and explicit `register_to(dlq=)` keep winning in all four.

### §D-report — `CoreModule.validate()` gains two messages; no posture finding

Both are pure binding/env reads (the existing `validate()` contract, `module.py:670-690`) and
surface as `module.core.validate` WARNING findings (`report.py:793-800`) in the startup report and
`varco diagnose`:

1. **Override without the keyword.** For each `EventConsumer` binding whose class has
   `auto_register` and `use_default_dlq` true, `register_to` overridden, and
   `inspect.signature(cls.register_to)` having neither a `default_dlq` parameter nor `**kwargs`:
   *"`<Cls>.register_to()` does not accept `default_dlq=` — CoreModule passes it at start (Plan
   060). Add the keyword (it is the lowest-tier DLQ; see `EventConsumer.register_to`) or set
   `use_default_dlq = False`."* Start then fails with Python's own `TypeError` — loud, and the
   report line printed first names the fix.
2. **Durable bus, in-process DLQ.** Emitted iff `consumer_dlq` is on, ≥ 1 eligible consumer binding
   exists, the `dlq` selection is `value == "memory"` with `source != "code"`, and the
   `event_bus` selection has `source in {"env", "env-dotted"}` and `value != "memory"` (via
   `inspect_backend_selection()`, `select.py:445-504`; a code-bound bus is never judged — it may be
   an in-memory test bus). Text: *"N auto-registered consumer(s) dead-letter exhausted retries to
   the in-process DLQ while VARCO_EVENT_BUS_BACKEND=<value> — dead letters are lost on restart.
   Set VARCO_DLQ_BACKEND, bind a durable AbstractDeadLetterQueue, or CoreModule(consumer_dlq=False)."*
   The condition is a pure helper `_dlq_durability_mismatch(selections) -> bool` so it is
   unit-tested on synthetic `SeamSelection`s.

✅ One owner (the module that changes the behaviour reports it); ✅ an all-in-memory dev/test app
emits nothing (054's "in-process is legitimate" stands); ❌ under `strict_validation=True` message 2
refuses startup — intended (it is the one new data-loss window this plan opens) and Open question 2
asks the user to confirm.

## Steps

All commands from `/home/edoardo/projects/varco`. Tests-first: 1→2, 3→4, 5→6. Docs steps 7–10
depend on 2/4/6. Unit tests use `InMemoryEventBus` (SYNC) + `InMemoryDeadLetterQueue`, failing
handlers raising `RuntimeError`, and `RetryPolicy(max_attempts=2, base_delay=0.0, jitter=False)` —
no sleeps, no Docker. Every test touching the preset uses the autouse reset fixture pattern from
`varco_core/tests/test_reliability_preset.py:49-57`.

0. [ ] **Preflight** — write `/tmp/claude-1000/-home-edoardo-projects-varco/<session>/scratchpad/060-preflight.txt`:
       (a) re-read every `file:line` in §Context, note any drift; (b) `rg -n 'def register_to\('
       varco_*/varco_* examples testkit` → expect exactly the five sites (`event/consumer.py`,
       `webhook/dispatcher.py`, `service/audit.py`, `tenancy/control/readiness.py`,
       `tenancy/control/consumer.py`); (c) tests that may depend on a CoreModule-registered,
       retrying handler raising: `rg -ln 'CoreModule|create_varco_app|in_memory_modules'
       varco_*/tests examples | xargs rg -ln 'retry_policy='` — list each and whether it asserts
       an exception from `publish()`; (d) confirm `WebhookDispatcher` is reached by
       `acollect(container, EventConsumer)` (run `test_webhook_module.py` + a one-off
       `acollect` check); (e) confirm `is_user_bound()` does not resolve (read `di/__init__.py`);
       (f) baseline `uv run pytest varco_core/tests -q -p no:randomly | tail -1`.
       **Verify**: file has (a)–(f); every ⚠️ in §Risks has a measured answer.

1. [ ] `varco_core/tests/test_consumer_default_dlq.py` (**new, red**) — resolution matrix on a hand
       `register_to(bus, default_dlq=D)`, no container: (a) `@listen(E, retry_policy=R)` → `publish`
       returns, `D.entries` has 1 entry, `attempts == 2`, `handler_name` is the method's qualname;
       (b) bare `@listen(E)`, preset off → `publish` raises `ExceptionGroup`, `D` empty;
       (c) bare + `set_default_reliability_preset(ReliabilityPreset(retry_policy=R))` → `D` gets it;
       (d) bare + preset `retry=R, dlq=P` → `P` gets it, `D` empty; (e) `@listen(E, retry_policy=R,
       dlq=None)` → raises (`RetryExhaustedError` inside the group), `D` empty; (f) `@listen(E,
       retry_policy=R, dlq=X)` → `X`; (g) `@listen(E, dlq=X)` → `X`, `D` empty; (h) `register_to(bus,
       retry_policy=R, default_dlq=D)` → `D`; (i) `register_to(bus, retry_policy=R, dlq=Y,
       default_dlq=D)` → `Y`; (j) `register_to(bus, retry_policy=R)` (no keyword) → raises, as 3.x;
       (k) `RetryPolicy(retryable_on=(ValueError,))` + handler raising `TypeError` → propagates,
       `D` empty; (l) `@listen(E, deduplicator=InMemoryDeduplicator())` only → no fill;
       (m) inside `tenant_context("t1")` the default-path entry carries `tenant_id == "t1"`;
       (n) `EventConsumer.use_default_dlq is True`; (o) `_fill_default_dlq` truth table (8 rows over
       `dlq`/`dlq_explicit`/`retry_policy`/`default_dlq`).
       **Verify**: `uv run pytest varco_core/tests/test_consumer_default_dlq.py -q` → red
       (`TypeError: unexpected keyword 'default_dlq'` / `ImportError: _fill_default_dlq`).

2. [ ] `varco_core/varco_core/event/consumer.py` — (i) module-level `_fill_default_dlq()` with full
       docstring (Args/Returns/Edge cases) and a `DESIGN:` block summarising §D-tier's table;
       (ii) `use_default_dlq: ClassVar[bool] = True` beside `auto_register` (`:765-769`) with a
       comment naming `CoreModule`; (iii) `register_to(..., default_dlq: AbstractDeadLetterQueue |
       None = None)`, `Args:` entry stating the four conditions and that tier-2/preset `None` means
       "no preference"; (iv) after `:967`, track `dlq_explicit` per branch and call the helper; extend
       the resolution comment at `:915-928` to "4. the container's DLQ (Plan 060 / RO-14) — DLQ slot
       only, only past retry"; (v) the `_Unset` docstring (`:119-137`) gains one ✅/❌ line on the
       tier-1 per-field asymmetry; (vi) the class docstring (`:689-713`) gains one paragraph on the
       auto-registered DLQ and the three opt-outs.
       **Verify**: Step 1 green; `uv run pytest varco_core/tests/test_reliability_preset.py
       varco_core/tests/test_event_consumer_autoregister.py -q` green (tiers 1–3 unchanged);
       `uv run mypy varco_core/varco_core/event/consumer.py` clean.

3. [ ] `varco_core/tests/test_event_consumer_autoregister.py` (**extend, red**) — each with
       `CoreModule().register(container)` + `container.register(@Singleton consumer)`: (a) a
       retrying failing handler, `await core.start(container)`, publish on the resolved bus →
       `container.get(AbstractDeadLetterQueue)` (an `InMemoryDeadLetterQueue`) holds 1 entry;
       (b) the same through `for c in core.components(container): await c.start()` (phase path);
       (c) bare failing handler → `publish` raises, DLQ empty; (d) `use_default_dlq = False` →
       raises, DLQ empty; (e) `CoreModule(consumer_dlq=False)` → same; (f)
       `bind_instance(container, AbstractDeadLetterQueue, user_dlq)` before start → `user_dlq` gets
       it; (g) a consumer already `register_to(bus)`-ed by hand before start → skipped, no DLQ
       (documents the `@PostConstruct` pitfall); (h) a subclass overriding `register_to(self, bus,
       *, retry_policy=None, dlq=None)` → `core.validate(container)` has a message containing the
       class name, `default_dlq=` and `use_default_dlq = False`; `await core.start(container)` raises
       `TypeError`; the same class with `use_default_dlq = False` → no message, starts;
       (i) `_dlq_durability_mismatch` on synthetic `SeamSelection` tuples: dlq memory/default +
       bus `redis`/env → True; bus memory/default → False; bus `code` → False; dlq `code` → False;
       dlq `redis` → False; (j) `validate()` with `inspect_backend_selection` monkeypatched to the
       True case and one eligible consumer → the mismatch message; with no consumer bindings → none.
       **Verify**: new tests red; the seven existing tests (`:32-141`) still green.

4. [ ] `varco_core/varco_core/di/module.py` — `CoreModule.__init__(..., consumer_dlq: bool = True)`
       (docstring `Args:`), stored as `self._consumer_dlq`; `_start_consumers()` (`:877-894`)
       resolves `AbstractDeadLetterQueue` once (`is_resolvable` + `aget`, only when
       `self._consumer_dlq`) and calls `consumer.register_to(bus, default_dlq=dlq)` iff `dlq is not
       None and type(consumer).use_default_dlq`, else `consumer.register_to(bus)`; a `DESIGN:` block
       (✅ explicit choices win, broker redelivery kept for bare handlers; ❌ the in-process default
       DLQ — §D-report); `_dlq_durability_mismatch()` module-level helper; `validate()` (`:670-690`)
       appends §D-report's two messages. Class docstring (`:580-596`) gains one sentence.
       **Verify**: Step 3 green; `uv run pytest varco_core/tests/test_webhook_module.py
       varco_core/tests/test_di_wiring_conformance.py -q` green.

5. [ ] **Override tests (red)** — `varco_core/tests/test_webhook_dispatcher.py`: (a)
       `WebhookDispatcher(repository=repo)` + `register_to(bus, default_dlq=D)` → `_dlq is D` and an
       exhausted delivery (stubbed `_send` failing) lands in `D`; (b) constructor `dlq=C` + default
       `D` → `C`; (c) `register_to(bus, dlq=X, default_dlq=D)` → `X`; (d) `repo.find_active_matching`
       raising → `publish` raises, `D` empty (`_on_trigger` unwrapped — no double wiring);
       `varco_core/tests/test_webhook_module.py`: (e) `CoreModule` + `WebhookModule`, `await
       core.start(container)` → `dispatcher._dlq is container.get(AbstractDeadLetterQueue)` and
       `dispatcher.registered_bus_count == 1`. `varco_core/tests/test_audit.py` (reuse its
       `instant_backoff`): (f) failing repo + `register_to(bus, default_dlq=D)` → `D`; (g) ctor
       `dlq=C` wins; (h) `register_to(bus, retry_policy=None, default_dlq=D)` → fire-and-forget,
       no raise, `D` empty; (i) `register_to(bus, dlq=None, default_dlq=D)` → `D` empty.
       `varco_core/tests/test_tenant_readiness_coordinator.py` and
       `varco_core/tests/test_tenant_provision_consumer.py`: (j) default fills when ctor `dlq`
       omitted; (k) ctor `dlq` wins; (l) explicit `dlq=None` → no fill.
       **Verify**: new tests red (`TypeError` on the keyword).

6. [ ] **Overrides** — `varco_core/varco_core/webhook/dispatcher.py:223-242` (accept + fill-if-`None`,
       never forwarded, `DESIGN:` comment citing §D-overrides; docstring `Args:`);
       `varco_core/varco_core/service/audit.py:772-811`, `tenancy/control/readiness.py:154-167`,
       `tenancy/control/consumer.py:225-254` (accept `default_dlq`, call `_fill_default_dlq` with
       `dlq_explicit=dlq is not _UNSET`, fire-and-forget branch first in `AuditConsumer`; docstring
       `Args:` in each). Module docstring of `dispatcher.py:7-11` updated: "wired by
       `CoreModule`'s auto-registration" replaces "`@PostConstruct`".
       **Verify**: Step 5 green; `make test PKG=varco_core` green; `uv run mypy varco_core/varco_core` clean.

7. [ ] `examples/00-full-stack-post-api/example/consumer.py` — delete the private
       `_dlq = InMemoryDeadLetterQueue()` (`:62-71`) and `dlq=_dlq` (`:130,165`); keep
       `retry_policy=_RETRY_POLICY`; docstrings (`:27`, `:90`, `:146`) say "routed to the app's DLQ
       (`VARCO_DLQ_BACKEND`), visible to `mount_reliability_admin`/`DlqRedriver`". Add one test in
       `examples/00-full-stack-post-api/example/tests/` (beside `test_post_service.py`) — app
       container, handler patched to raise, publish → the container DLQ holds the entry.
       **Verify**: `uv run pytest examples/00-full-stack-post-api -q` green; `make asyncapi-check`
       unchanged (DLQ is not part of the AsyncAPI document — `asyncapi/generator.py` has no
       `dlq`/`retry` reference).

8. [ ] **Feature docs** — `technical_docs/features/dead-letter-queues.md`: new section "Auto-registered
       consumers and the app DLQ (Plan 060 / RO-14)" after "`AuditConsumer` — safe-by-default" —
       the four-tier order, both §D-tier tables, the three opt-outs, the bus table; Pitfalls rows:
       *bare handler failures still not dead-lettered* → set a preset retry policy or
       `@listen(dlq=…)`; *durable bus + in-memory DLQ* → `module.core.validate` warning, fix;
       *kept a `@PostConstruct register_to`* → that consumer gets no default DLQ, delete it;
       *Streams bus-level `dlq=`/`max_delivery_count` never fires for retrying handlers* → pick one
       layer; *Kafka `EXACTLY_ONCE`: the DLQ push is outside the transaction*.
       `technical_docs/features/dependency-injection.md`: the `EventConsumer` row (`:590`) gains
       "… and passes the bound `AbstractDeadLetterQueue` as `default_dlq=` (060)"; Pitfalls row
       `:906` gains "and it then receives no default DLQ"; one new Pitfalls row for the
       `register_to` override `TypeError`.
       **Verify**: `rg -n 'default_dlq' technical_docs/features/dead-letter-queues.md
       technical_docs/features/dependency-injection.md` ≥ 3; `make docs-strict` green.

9. [ ] **README + CLAUDE.md** — `README.md` "Consumer — EventConsumer + @listen" (`:1900`): one
       paragraph + snippet showing `@listen(E, retry_policy=…)` with no `dlq=` landing in the app
       DLQ and the three opt-outs; "Dead Letter Queue" (`:4364-4389`): one sentence pointing at it.
       `CLAUDE.md` §Event system, after the "`@listen` is declarative / `register_to` is imperative"
       paragraph — one line: *"`CoreModule` passes the bound DLQ as `register_to(default_dlq=)` —
       the lowest tier, DLQ slot only, only past retry (Plan 060 / RO-14); framework code never
       passes the app DLQ through `dlq=` (it would override explicit choices). Opt-outs: `@listen(dlq=None)`,
       `use_default_dlq = False`, `CoreModule(consumer_dlq=False)`."* §Dead Letter Queue: append
       "auto-registered retrying handlers dead-letter here by default (060)". Nothing else.
       **Verify**: `rg -n 'default_dlq' CLAUDE.md README.md` ≥ 2; `rg -n '@PostConstruct.*register_to'
       README.md` → no new hits.

10. [ ] **CHANGELOG + migration guide** — `CHANGELOG.md` `[4.0.0]`: a block
       `### BREAKING — auto-registered consumers dead-letter exhausted retries to the app DLQ (Plan 060, RO-14)`
       placed **immediately after** the Plan 051 block (`:295-327`; the header comment `:10-15` fixes
       migration-guide order, and this lands in §7) — the behaviour matrix in three bullets, the
       `register_to` keyword + `TypeError` for 3.x-signature overrides, the three opt-outs, the
       `validate()` message. `technical_docs/migrations/3-to-4.md` §7 (`:215-228`): three rows —
       (1) *"`@listen(E, retry_policy=R)` on a DI-bound consumer: exhausted events raised to the bus
       (redelivered on Streams/JetStream, lost elsewhere)"* → *"dead-lettered to the bound DLQ, then
       acked; set `VARCO_DLQ_BACKEND` to a durable backend, or opt out with `dlq=None` /
       `use_default_dlq = False` / `CoreModule(consumer_dlq=False)`"*; (2) *"a consumer overriding
       `register_to(self, bus, *, retry_policy=None, dlq=None)`"* → *"add `default_dlq=None` and
       forward or apply it, or set `use_default_dlq = False`"*; (3) *"a private
       `InMemoryDeadLetterQueue` passed to `@listen(dlq=)` only so failures are kept"* → *"drop it;
       the app DLQ is reachable by `mount_reliability_admin`/`DlqRedriver`"*; and a "Not changed"
       sentence: bare handlers, the preset, hand-registered consumers. Plan tag `Plan 060` on each.
       **Verify**: `rg -n 'Plan 060' CHANGELOG.md technical_docs/migrations/3-to-4.md` ≥ 4;
       `uv run pytest varco_core/tests/test_deprecation_floor.py -q` green (heading shape intact).

11. [ ] **Gates** — `make lint`, `make type-check`, `make test` (all eleven suites); the snapshot gates
       must report **no change** (no `__all__`, function signature, binding, settings field,
       module `name`/`phase`/`depends_on`, or consumer channel moves): `uv run python
       scripts/api_surface.py --check`, `uv run python scripts/di_catalogue.py --check`,
       `uv run python scripts/settings_reference.py --check`, `uv run python scripts/entry_points.py
       --check`, `make asyncapi-check`. If any reports a change, stop — it is not this plan's
       (regenerate only after attributing it). Record M1 delta (`rg -o 'type: ignore'
       varco_*/varco_* | wc -l`, expected 0).
       **Verify**: all exit 0.

## Edge cases

- **No unqualified DLQ resolvable** (a container `CoreModule` did not bootstrap) → `default_dlq` not
  passed; 3.x behaviour, no error.
- **The bound DLQ is a user `bind_instance`** → it is what `aget` returns; it wins over the in-memory
  fallback (Step 3(f)).
- **A consumer registered by hand before `CoreModule` starts** (`@PostConstruct`, legacy) → the
  `is_registered_to` guard (`module.py:893`) skips it; it gets **no** default DLQ. Documented
  pitfall; fix is deleting the `@PostConstruct` (051's recommendation anyway).
- **`auto_register = False` / fan-out to a second bus by hand** → nothing passed; the hand caller
  may pass `default_dlq=` itself.
- **`EventConsumer.start()` (`_bus` path via `LifecycleModule`)** → `register_to(self._bus)` bare; no
  default (Non-goals).
- **Non-retryable exception under a policy** (`retryable_on` excludes it) → propagates as today
  (`consumer.py:593-594`); never dead-lettered.
- **`asyncio.CancelledError` in a retrying handler** → `is_retryable` is false for a
  non-`Exception` (`retry.py:158-164,214-226`) → propagates; the default path never enters
  DLQ-only mode.
- **`InMemoryDeadLetterQueue` full** (`max_size=10_000`) → oldest evicted with a WARNING
  (`dlq.py:612-619`) — unchanged; the more traffic now reaches it, the more this matters; the
  §D-report message is the nudge to a durable backend.
- **Redis Streams with a bus-level `dlq=`/`max_delivery_count`** → a retrying handler's failure is
  dead-lettered by the consumer wrapper and the message `XACK`ed, so the bus-level DLQ never sees
  it; bare handlers still reach it. Documented ("pick one layer").
- **Kafka `EXACTLY_ONCE`** → DLQ push is not inside the Kafka transaction; a crash between push and
  commit yields a redelivery and a second DLQ entry (at-least-once into the DLQ). Documented.
- **Redrive** → `DlqRedriver` republishes to the channel, so every subscriber of that channel sees
  the event again (existing behaviour, `dead-letter-queues.md` §Redrive); more entries now reach the
  DLQ, so the note is linked from the new section.
- **Two containers in one process** → each `CoreModule` passes its own container's DLQ; no global
  state (why §D-tier rejected candidate 4).
- **Preset changed after start** → no effect on already-registered consumers (resolution is at
  `register_to()` time, `wiring.py:9-12`) — unchanged.
- **`strict_validation=True` + env-selected durable bus + default in-memory DLQ** → startup refuses
  on the `module.core.validate` WARNING; escape: select a durable DLQ, bind one, or
  `CoreModule(consumer_dlq=False)`.
- **`use_default_dlq = False` on a class whose `register_to` keeps the 3.x signature** → no message,
  no keyword passed, starts fine — the zero-edit migration path for out-of-tree overriders.

## Verification

```bash
uv run pytest varco_core/tests/test_consumer_default_dlq.py varco_core/tests/test_event_consumer_autoregister.py -q
uv run pytest varco_core/tests/test_webhook_dispatcher.py varco_core/tests/test_webhook_module.py \
  varco_core/tests/test_audit.py varco_core/tests/test_tenant_readiness_coordinator.py \
  varco_core/tests/test_tenant_provision_consumer.py varco_core/tests/test_reliability_preset.py -q
uv run pytest examples/00-full-stack-post-api -q
rg -n 'register_to\(bus, dlq=' varco_core/varco_core/di/        # 0 — framework never uses tier 2
rg -n 'default_dlq' varco_core/varco_core | wc -l               # >= 10 (consumer, module, 4 overrides)
make test && make lint && make type-check && make docs-strict
uv run python scripts/api_surface.py --check                    # no change
uv run python scripts/di_catalogue.py --check                   # no change
uv run python scripts/settings_reference.py --check             # no change
uv run python scripts/entry_points.py --check                   # no change
make asyncapi-check                                             # no change
uv run pytest varco_core/tests/test_deprecation_floor.py -q     # CHANGELOG headings intact
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — `WebhookDispatcher` (provided with `returns=WebhookDispatcher`, `webhook/module.py:151-154`) is collected by `acollect(container, EventConsumer)`; the claim is the 050 docstring at `:160-164`, not re-proved | The dispatcher receives `default_dlq` through `CoreModule` exactly once | Step 0(d); Step 5(e) asserts `registered_bus_count == 1` and `_dlq` identity |
| ⚠️ ASSUMPTION — `is_user_bound()` (used by `inspect_backend_selection`, `select.py:486,493`) never resolves a binding | `validate()` stays a pure read (its own contract, `module.py:670-690`) | Step 0(e); if it resolves, compute the mismatch from `get_binding(...)` + `os.environ` directly |
| ⚠️ ASSUMPTION — no in-tree test asserts that a CoreModule-registered handler **with a retry policy** raises from `publish()` | `make test` stays green without weakening an assertion | Step 0(c) lists them; each is updated to assert the DLQ entry instead (the behaviour change is the point), or opts out with `consumer_dlq=False` if the test is about propagation |
| ⚠️ ASSUMPTION — the DLQ `aget` at `CONSUMERS` returns the same singleton `core.stores` started at `INFRA` (fallback and backend DLQ bindings are singletons) | One DLQ instance, started once | Step 3(f) asserts identity with `container.get(AbstractDeadLetterQueue)`; Step 0(a) re-reads the backend providers' `singleton=True` |
| ⚠️ ASSUMPTION — Kafka `AT_LEAST_ONCE` "skipped" row: the docstring (`kafka/bus.py:494-499`) says the offset stays uncommitted; whether a later commit advances past it is inferred, not tested | The bus table is accurate | Phrase the doc row as the docstring states it; no stronger claim |
| **Out-of-tree `register_to` overrides break with `TypeError`** | A 3.x-signature override fails loudly, never silently loses the DLQ | §D-report message 1 printed before start; migration-guide row; `use_default_dlq = False` keeps 3.x code working unchanged |
| **Durable-bus apps start dead-lettering into memory** | No new silent data-loss window | §D-report message 2 (WARNING); migration guide row 1; README; posture's opt-in `di.in_process_backend` unchanged |
| **Tier semantics drift between base and the three `_UNSET` overrides** | One rule, one helper | `_fill_default_dlq` shared; Step 1(o) truth table + Step 5 per-class tests |
| **`_on_trigger` accidentally wrapped** (double DLQ path for webhooks) | `WebhookDispatcher` never forwards `default_dlq` to `super()` | Step 5(d) asserts the raw handler (exception reaches the bus, `D` empty) |
| Snapshot gates move | No `__all__`, function signature, binding, settings, module ClassVar or channel changes in this plan | Step 11 runs all five `--check`s; a change means foreign dirt — stop |

## Done criteria

- [ ] `register_to(..., default_dlq=)` + `_fill_default_dlq` + `use_default_dlq` in
      `event/consumer.py`; Step 1's matrix green; tiers 1–3 unchanged (`test_reliability_preset.py`).
- [ ] `CoreModule(consumer_dlq=True)` passes the bound DLQ; `validate()` emits §D-report's two
      messages; Step 3 green.
- [ ] The four overrides accept the keyword with constructor/explicit precedence intact;
      `WebhookDispatcher._on_trigger` unwrapped; Step 5 green.
- [ ] Full-stack example uses the app DLQ; its suite + `asyncapi-check` green.
- [ ] `dead-letter-queues.md` section + Pitfalls, `dependency-injection.md` row/Pitfalls, README,
      CLAUDE.md one-liners, CHANGELOG `[4.0.0]` BREAKING block after Plan 051's, `3-to-4.md` §7 rows.
- [ ] `make lint` / `type-check` / `test` / `docs-strict` green; all five snapshot `--check`s report
      no change.

## Open questions (for the user)

1. **Scope of the fill — "past retry" only (chosen) vs every handler.** Bare handlers keep 3.x
   semantics so Streams/JetStream redelivery and SYNC error propagation survive. Confirm, or ask
   for candidate (3) (every handler, single-attempt DLQ) knowing its costs (§D-tier).
2. **`module.core.validate` WARNING for env-selected durable bus + in-process DLQ** refuses startup
   under `strict_validation=True`. Keep WARNING, or downgrade to an INFO `WiringFinding`?
3. **Latent bug found, not fixed here:** `_make_retry_wrapper`'s DLQ-only mode catches
   `BaseException` (`consumer.py:589-594`), so an explicit `@listen(dlq=X)` handler without a retry
   policy dead-letters and swallows `asyncio.CancelledError`/`KeyboardInterrupt`. Two-line fix
   (re-raise non-`Exception`s first). File as its own `RO-*` row, or fold into this plan?
4. **`WebhookDispatcher._on_trigger`** — a repository lookup failure still loses the trigger
   (as in 3.x); wrapping it would risk duplicate deliveries on redrive. Accept as-is?
