# Plan 069a — Cross-instance `JobDispatcher`: a live, atomic `claim_next()`, the `JobQueue` enqueue API, tenant propagation (ORCH-1, part 1 of 2; absorbs RO-9)

**Row:** `ORCH-1` (BACKLOG.md:63) — *"Task orchestration, first stage — cross-instance `JobDispatcher`
poll loop over `claim_next()` (absorbs `RO-9`), general schedule driver (absorbs `RO-6`), job
chaining (`on_success` → next), fan-in groups with completion callback"*. Locked decision
BACKLOG.md:32: **first stage only** — worker + schedule driver + chaining + fan-in; durable workflow
replay stays parked (BACKLOG.md:112, *"Full durable workflows"*). Ships **inside 4.0.0**
(BACKLOG.md:31) — breaking allowed, each break with a `3-to-4.md` entry and a CHANGELOG `[4.0.0]`
block.

**Why two files.** ORCH-1 is ~30 atomic steps; per the coordinator's ≤ ~12-step rule it is split
along the one seam that is genuinely independent:

| File | Scope | Depends on |
|---|---|---|
| **069a (this file)** | *Execution*: `claim_next()` hardening on all four stores, atomic fenced `save()`, `Job.tenant_id`, `current_job()`, `JobSpec`/`JobQueue.enqueue()`, `JobDispatcher`, `JobRunner` coexistence, `JobDispatcherModule` | — |
| `plans/069b-schedule-driver-chaining-and-fan-in.md` | *Composition*: `ScheduleDriver` (RO-6), `Schedule.concurrency`, `JobStatus.BLOCKED`, chaining, fan-in groups, `get_many()` | 069a merged (uses its fenced-write CAS, `JobQueue`, dispatcher hooks) |

Index: `plans/000-index-4-0-feature-extension.md` (rows 059–069). Neighbours that touch this one:
062/063 (`@transactional` — a handler may later run inside one; **no transaction design here**),
066 (GDPR erasure enqueues jobs through `JobQueue` — §D-enqueue is written for it), 059 (`TEST-1`
fake loop clock — used by the dispatcher tests if landed first).

---

## Goal

After this plan, any process — FastAPI or not — that lists `JobDispatcherModule()` claims due,
task-backed jobs from the shared `AbstractJobStore` through `claim_next()` with a lease and a
heartbeat, runs them through the `TaskRegistry` **under the job's own tenant**, retries/dead-letters
them with the exact decision table `JobRunner` uses, reaps dead workers' leases, and hands in-flight
work back on shutdown. `JobQueue.enqueue(JobSpec)` is the one idempotent, tenant-stamping enqueue API
every pod (and Plan 066) uses, and `claim_next()` is live, atomic, and payload-filtered on all four
stores.

## Non-goals

- **Chaining, fan-in, the schedule driver, `JobStatus.BLOCKED`** → `plans/069b`.
- **Durable workflow replay, durable timers, signals, compensation** — the parked *"Full durable
  workflows"* row (BACKLOG.md:112). Nothing here checkpoints a handler's intermediate state.
- **Transactions.** `JobQueue.enqueue()` does **not** join an ambient `@transactional` boundary
  (062/063). A handler may open one itself; the dispatcher never does (§Risks R6).
- **Cross-instance cancellation of a RUNNING job.** `JobRunner.cancel()` stays in-process only.
- **Restoring `AuthContext` from `auth_snapshot`** — a handler that needs it reads
  `current_job().auth_snapshot` and calls `auth_context_from_snapshot()` itself.
- **Priority queues, named queues/routing, per-task concurrency limits, push wake-up
  (`LISTEN/NOTIFY`, Redis keyspace notifications)** — Parked table.
- **A metrics surface** (`install_job_metrics()`) — Parked table.
- **Removing or moving `JobRunner`/`JobPoller`** — both keep working; §D-coexist.
- **A single-statement `UPDATE … RETURNING` claim on SA** — Parked table.
- **Poison-pill detection across lease reaps** — Parked table (§Risks R4).
- **Any HTTP surface** (admin mount, job routes).
- **`RO-7`** (the materializer DESIGN block) — not in scope (and already corrected in source,
  `materializer.py:29-42`).
- **Editing BACKLOG.md** — the coordinator owns it.

---

## Context — verified against source (2026-10-07)

| Claim | Evidence | Consequence |
|---|---|---|
| `AbstractJobStore.claim_next(*, owner_id, lease_ttl, now)` is a concrete default: `list_by_status(PENDING, limit=100)` → client-side `run_at` filter → `try_claim` loop | `varco_core/varco_core/job/base.py:829-878`, the `limit=100` at `:870` | ⛔ **Head-of-line starvation**: if the 100 oldest PENDING rows are future-dated (retry back-off — `as_retry` keeps `created_at`, `:473-509`) no due job is ever claimed |
| Its docstring says SA/Redis/Beanie override it "with a single round-trip" | `base.py:840-845` | ⛔ **False for two of three**: `RedisJobStore.claim_next` (`varco_redis/varco_redis/job_store.py:688-696`) and `BeanieJobStore.claim_next` (`varco_beanie/varco_beanie/job_store.py:821-829`) are verbatim copies of the default, same `limit=100` |
| `SAJobStore.claim_next` selects one id `ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED` inside `engine.connect()`, then calls `try_claim` after the block exits | `varco_sa/varco_sa/job_store.py:718-741` | Live (native `run_at` predicate) but the row lock is released before `try_claim` → a lost race returns `None` while other rows are due (spurious idle) |
| No store can filter "has a `task_payload`" at claim time | all four `claim_next` bodies | ⛔ A poll loop would claim bare-coroutine jobs (`JobRunner.enqueue(job, coro)`) it cannot execute — §D-claim |
| Fenced `save(expected_epoch=)` contract: implementations MUST refuse with `StaleLeaseError` | `base.py:682-716` | — |
| `SAJobStore.save` fenced path: plain `SELECT lease_epoch`, then `DELETE`+`INSERT`, one transaction, **no `FOR UPDATE`** | `varco_sa/varco_sa/job_store.py:380-396` | On Postgres the concurrent loser gets `IntegrityError` (duplicate PK), not `StaleLeaseError` |
| `BeanieJobStore.save` fenced path: `find_one` → compare → `delete` → `insert`, **non-atomic** | `varco_beanie/varco_beanie/job_store.py:474-491` | ⛔ Two concurrent fenced writes at the same epoch both pass — fencing is advisory on Mongo |
| `RedisJobStore.save` fenced path uses `WATCH`/`MULTI` | `varco_redis/varco_redis/job_store.py:284-320` | ✅ already atomic |
| `InMemoryJobStore` does everything under one lazily-created lock; `renew()` does **not** bump `lease_epoch`; reap bumps it and returns RUNNING→PENDING with `attempt` unchanged | `varco_core/varco_core/job/memory.py:297-316`, `:381-390`, `:430-453` | The `Job.lease_epoch` docstring (`base.py:294-299`, "incremented on every claim/renew") over-promises; the dispatcher must track the epoch **returned by** `renew()` |
| The conformance suite has **no** `claim_next` liveness/concurrency/filter test and no concurrent-fence test | `testkit/varco_conformance/job_store.py:41-150` (only `:75-82` touches `claim_next`) | The two ⛔ rows above were never caught |
| `JobRunner._run_job` loads, `as_running()`, `save()` — **no claim** | `varco_fastapi/varco_fastapi/job/runner.py:795-804` | ⛔ With any second claimant on the store, an `enqueue_task()` job can run twice (Plan 039 §D-S20-dispatch already rejected `submit()` for this reason, `plans/039:332-334`) |
| `JobRunner.recover()` leaves an unknown-task job RUNNING forever | `runner.py:566-574` | The dispatcher must not copy this (§D-unknown) |
| `JobRunner` retry/DLQ decision table; result encoding `json.dumps` → fallback `str()` | `runner.py:686-745`; `:657-662`, `:820-825` | One decision table, extracted and shared (§D-failure) |
| `JobRunner` holds `AbstractEventBus` (progress/SSE) and fires `callback_url` with `httpx` | `runner.py:169`, `:925-980` | Both are HTTP-layer concerns — the reason the dispatcher is **not** a refactored `JobRunner` (§D-home) |
| `varco_core` has no `httpx` dependency | `varco_core/pyproject.toml:22-63` | `callback_url` firing stays in `varco_fastapi`, offered as a hook (§D-hooks) |
| `Job` has no tenant field; `TaskPayload` must be JSON-safe | `base.py:146-334`; `job/task.py:97-101` | A job claimed on another pod has no way to know its tenant today (§D-tenant) |
| `current_tenant()` / `tenant_context(tenant_id)` (sync CM) / `AmbientVar[T]` | `varco_core/varco_core/service/tenant.py:151`, `:165`; `varco_core/varco_core/context/ambient.py:56-127` | The mechanisms §D-tenant and §D-context reuse |
| `CoreModule` starts the resolved `AbstractJobRunner` at `Phase.SCHEDULERS` (`core.job_runner`) | `varco_core/varco_core/di/module.py:746-754`, `:902-916`; `Phase` at `varco_core/varco_core/di/lifecycle.py:60-86` | Dispatcher sits at `SCHEDULERS + 50` — after every task registrant, stopped before them (§D-di) |
| A `VarcoModule` may live in `varco_core` and register a `@Fallback` default itself | `varco_core/varco_core/webhook/module.py:75-154`, fallback at `:123-138` | `JobDispatcherModule` copies `WebhookModule`'s shape |
| `TaskRegistry` is bound only by `varco_fastapi`'s configuration | `varco_fastapi/varco_fastapi/di.py:351-361`; `varco_fastapi/varco_fastapi/retention.py:103-136` builds a private one when unresolvable | A FastAPI-free worker has no registry → module registers a `@Fallback` one (§D-di) |
| `bind_core_settings()` is the audited list of `varco_core` settings classes | `varco_core/varco_core/settings/core.py:42-105` | `JobDispatcherSettings` is added there |
| `RetryPolicy()` defaults: `max_attempts=3, base_delay=1.0, max_delay=60.0, exponential_base=2.0, jitter=True` | `varco_core/varco_core/resilience/retry.py:143-158` | The dispatcher's default back-off curve |
| SA migration head is `0008_schedule_task_name`; every framework-column revision is idempotent via `_has_column` | `varco_sa/varco_sa/migrations/versions/0008_schedule_task_name.py:15-59` | `0009_job_tenant_id` copies it |
| Plan 039 rejected the dispatcher *"as a second execution path for jobs [that] belongs to Plan 005's subsystem"*, trigger *"an app needs durable scheduled jobs without a startup `recover()`"* | `plans/039-retention-and-purge-automation.md:338-340`, `:924` | This plan is that row, inside the job subsystem; §D-home answers the "second path" objection |
| Research: *"Celery canvas (chain/group/chord) and Dramatiq remain for simple fan-out but offer no durable replay"*; Postgres-backed library execution (DBOS) is established | `design/research/019-streaming-orchestration-and-test-performance-2026.md:41-52` | Confirms the first-stage shape (a durable queue + worker, no replay). ⚠️ The brief's DBOSify citation (`:43`) is unverified and is **not** relied on |

---

## Design

```
 web pod / worker pod (any process)                       worker pod(s) — JobDispatcherModule
 ─────────────────────────────────                        ──────────────────────────────────────────
 JobQueue.enqueue(JobSpec)                                 JobDispatcher  (SCHEDULERS + 50)
   ├─ validate (JSON-safe, aware times, attempts ≥ 1)        poll loop ── sem.acquire()
   ├─ tenant_id = current_tenant()                             └─ store.claim_next(owner_id, lease_ttl,
   ├─ job_id given & exists → return it (idempotent)                         require_task_payload=True)
   └─ store.save(Job(PENDING, task_payload, ...))                   │ None → sleep(poll ± jitter)
                     │                                              ▼ Job(RUNNING, owner, epoch e)
                     ▼                                         _execute(job)
          ┌──────────────────────┐                               ├─ heartbeat: renew() every ttl/3
          │   AbstractJobStore   │◄──────────────────────────────┤     (None ⇒ fenced out ⇒ cancel, no write)
          │ (memory/SA/Beanie/   │                               ├─ tenant_context(job.tenant_id)
          │  Redis)              │                               ├─ current_job.scope(job)
          └──────────────────────┘                               ├─ registry.invoke(payload)
                     ▲                                           │    unknown name ⇒ as_released(run_at+backoff)
                     │                                           ├─ ok   ⇒ save(as_completed, expected_epoch=e')
       reap loop (every reap_interval):                          ├─ err  ⇒ terminalize_failure(): retry/DEAD/FAILED
       store.reap_expired_leases()                               ├─ shutdown cancel ⇒ save(as_released, fenced)
                                                                 └─ on_terminal(job) hook (never raises)
```

### §D-home — the dispatcher lives in `varco_core.job`; `JobRunner` stays where it is

| ID | Choice | Consequence |
|---|---|---|
| D-home | New `varco_core/varco_core/job/dispatcher.py` (`JobDispatcher`), plus `queue.py` (`JobSpec`, `JobQueue`), `context.py` (`current_job()`), `outcome.py` (shared result-encoding + failure decision), `settings.py` (`JobDispatcherSettings`), `module.py` (`JobDispatcherModule`). `varco_fastapi.job.JobRunner` keeps its in-process role and gains only coexistence fixes (§D-coexist) | A worker needs `varco-core` + a store backend — no FastAPI, no event bus, no `httpx` |

✅ The caller's constraint holds by construction: nothing under `varco_core.job` imports
   `varco_fastapi` (guarded by a test, Step 10). Same seam rule CLAUDE.md states for
   `varco_core.migration`/`varco_core.tenancy`.
✅ The dispatcher holds **no** `AbstractEventBus` — CLAUDE.md's bus rule needs no new exception.
✅ Answers Plan 039's "second execution path" objection on its own terms: the path is inside the job
   subsystem (Plan 005's), it shares `JobRunner`'s retry/DLQ decision through one function
   (§D-failure), and both paths claim atomically from the same store (§D-coexist) — two front doors,
   one lock.
❌ Two executors exist (`JobRunner` in-process, `JobDispatcher` cross-instance). Accepted and
   documented as a two-row table in the feature doc: "run it here, now, with SSE progress" vs "queue
   it durably for any worker".
  Rejected — **give `JobRunner` a poll loop**: ❌ a worker would need `varco_fastapi`; ❌ `JobRunner`
  claims without leases and saves unfenced (`runner.py:555`, `:665`), so it would need the same
  rewrite anyway; ❌ drags its `AbstractEventBus` dependency into the durable path.
  Rejected — **move `JobRunner` to `varco_core` and extend it**: ❌ its SSE progress events and
  `httpx` callbacks are HTTP-layer concerns (CLAUDE.md layer rules); ❌ a breaking move of a class
  every FastAPI app wires, for no behavioural gain.
  Rejected — **`AbstractJobRunner.recover()` on a timer** (what `RetentionScheduler` does,
  `retention/scheduler.py:367-368`): ❌ no lease, no heartbeat, no fencing — a stalled pod's job is
  only noticed by `JobPoller`'s wall-clock threshold and then **failed**, never retried; ❌
  `list_by_status(PENDING)` + `try_claim` is the starving scan this plan fixes.

### §D-claim — `claim_next()` becomes the one dispatch primitive: live, atomic, payload-filtered

| ID | Choice | Consequence |
|---|---|---|
| D-claim | Signature becomes `claim_next(*, owner_id=None, lease_ttl=None, now=None, require_task_payload: bool = False)`. `require_task_payload=True` excludes rows whose `task_payload` is NULL. Liveness: ABC default scans `claim_scan_limit: ClassVar[int] = 1000`; Redis pages its PENDING sorted set in windows of 100 up to `claim_scan_limit`; **Beanie gets a native atomic `find_one_and_update`**; SA adds the predicate and tries up to 10 candidate ids per call; in-memory adds the predicate | ⚠️ **BREAKING** for an out-of-tree store that overrides `claim_next` (must accept the new kwarg) — `3-to-4.md` row |

✅ `require_task_payload` is what makes a poll loop **safe next to `JobRunner`**: a bare-coroutine job
   (`JobRunner.enqueue(job, coro)`) is PENDING in the store for a moment (`runner.py:370-375`) and
   only that process holds its coroutine. Claiming it would strand it: the dispatcher cannot run it,
   and releasing it back livelocks against `JobRunner`, which has already given up. Excluding it at
   the query is the only shape with no race.
✅ A materialized schedule occurrence with no `task_name` (`materializer.py:247-251` → `task_payload=None`)
   is likewise invisible — exactly Plan 039's contract for "an app's own dispatch".
✅ The liveness fix is the precondition of a poll loop: today 100 jobs in retry back-off stall the
   whole queue on Redis and Mongo.
✅ Beanie's native claim is one round trip and atomic — the same `find_one_and_update` primitive its
   `try_claim` already uses (`varco_beanie/varco_beanie/job_store.py:684-790`), with
   `{status: "pending", $or: [{run_at: null}, {run_at: {$lte: now}}], task_payload: {$ne: null}?}`
   and `sort=[("created_at", 1)]`.
❌ The ABC default stays a bounded scan (it cannot page — `list_by_status` has no offset). Documented
   on `claim_scan_limit`; every in-tree store is native or paged.
❌ SA's 10-candidate loop is still SELECT-then-claim. A single `UPDATE … WHERE job_id = (SELECT …
   FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *` is better under heavy contention — parked with the
   trigger "measured claim contention" (it is a dialect-split rewrite of the hottest query, not a
   correctness fix).
  Rejected — **dispatcher-side filtering** (claim, see no payload, release): ⛔ the livelock above.
  Rejected — **filtering by task name** (`claim_next(task_names=…)`, Celery-queue style): ❌
  `task_payload` is a JSON text column on SA — a `LIKE` on it is neither indexable nor exact; ❌
  routing is a Non-goal.

### §D-fence — a fenced `save()` is atomic in every backend

| ID | Choice | Consequence |
|---|---|---|
| D-fence | Beanie: the fenced path becomes `replace_one({"_id": id, "lease_epoch": expected}, doc)` with `matched_count == 0` ⇒ `StaleLeaseError`. SA: on PostgreSQL the epoch `SELECT` takes `FOR UPDATE`, and an `IntegrityError` on the fenced path is re-raised as `StaleLeaseError` (belt and braces). `AbstractJobStore.save` docstring states the atomicity requirement explicitly and that **a writer that wants CAS semantics bumps `lease_epoch`** | The dispatcher's terminal write and 069b's release/cancel CAS are sound on all four backends |

✅ The contract already says MUST (`base.py:689-696`); this makes Mongo honour it under concurrency.
✅ Unfenced `save()` (every existing caller) is untouched — byte-identical.
❌ Beanie's unfenced path stays `delete`+`insert` (non-atomic last-write-wins, documented at
   `job_store.py:465-470`). Not this plan's concern: no new code path relies on unfenced atomicity.
⚠️ ASSUMPTION — the Beanie 2.x collection accessor is `get_pymongo_collection()` (Beanie ≥ 2.0,
   async PyMongo) rather than `get_motor_collection()`; Step 0 checks the installed version and the
   existing `try_claim` body, which already reaches the raw collection for `find_one_and_update`.

### §D-tenant — `Job.tenant_id`, stamped from `current_tenant()` at enqueue

| ID | Choice | Consequence |
|---|---|---|
| D-tenant | `Job.tenant_id: str \| None = None` (persisted: SA column, Beanie field, Redis JSON key). `JobQueue.enqueue()` and `JobRunner.enqueue_task()` stamp `current_tenant()`. The dispatcher enters `tenant_context(job.tenant_id)` around the handler when non-`None`; `None` runs platform-wide (no context entered) | A tenant-aware service called from a handler on another pod sees the right tenant |

✅ `current_tenant()` stays the single source of truth for *who* the tenant is (CLAUDE.md) — the job
   records it at the one moment it is known, exactly like the DLQ tenant stamp, and nothing else ever
   writes it.
✅ No `tenant_id` keyword on `JobSpec`: to enqueue for another tenant, the caller enters
   `tenant_context(tid)` — one mechanism, already the house rule for "act as tenant X".
✅ `None` is honest and explicit (Plan 039 §D-S20-tenancy's lesson): a job enqueued outside any tenant
   runs outside any tenant; it never inherits a stale ambient value on the worker.
❌ One new nullable column on the jobs table ⇒ `0009_job_tenant_id`. Nullable, no backfill: every
   existing row is `NULL` = platform-wide = today's behaviour.
  Rejected — **`metadata["tenant_id"]`**: ❌ stringly-typed, excluded from equality
  (`base.py:239`), and invisible to any future index or RLS policy.
  Rejected — **derive from `auth_snapshot["metadata"]["tenant_id"]`**: ❌ that is an auth claim, not
  the resolved tenant; Plan 033's provenance chain may have chosen a different source.

### §D-context — `current_job()`

| ID | Choice | Consequence |
|---|---|---|
| D-context | `varco_core/varco_core/job/context.py`: a module-scope `AmbientVar[Job]` and `current_job() -> Job \| None`. Set by `JobDispatcher` around the handler; set by `JobRunner._run_job`/`_run_claimed_job` too | A handler can read its own `job_id`, `attempt`, `auth_snapshot`, `tenant_id` (and in 069b its `depends_on`) without the runner being passed in |

✅ Module-scope `ContextVar` construction is correct (CLAUDE.md's note on PEP 567); no lock involved.
✅ Same shape as `current_tenant()` — no new pattern to learn.
❌ `varco_fastapi`'s private `_current_job_id_var` (`runner.py:64`) stays (it drives `job_progress()`);
   two vars, one of them private. Accepted — merging them is a refactor with no user value.

### §D-enqueue — `JobSpec` + `JobQueue` (the API Plan 066 consumes)

```python
@dataclass(frozen=True)
class JobSpec:
    task: TaskPayload                  # build with my_task.payload(*args, **kwargs)
    job_id: UUID | None = None         # deterministic id ⇒ idempotent enqueue
    run_at: datetime | None = None     # aware; None = due now
    max_attempts: int = 1              # > 1 opts into retry (§D-failure)
    expires_at: datetime | None = None
    callback_url: str | None = None    # fired only by an on_terminal hook (§D-hooks)
    metadata: Mapping[str, Any] = field(default_factory=dict)  # frozen copy in __post_init__

@Fallback
@Singleton
class JobQueue:
    def __init__(self, store: Inject[AbstractJobStore]) -> None: ...
    async def enqueue(self, spec: JobSpec) -> Job: ...        # 069b adds enqueue_chain/enqueue_group/cancel
```

| ID | Choice | Consequence |
|---|---|---|
| D-enqueue | `JobSpec` is a frozen value object; `JobQueue.enqueue()` validates (`task.validate_serializable()`, aware `run_at`/`expires_at`, `max_attempts >= 1`), stamps `tenant_id`, and — when `job_id` is given and already stored — **returns the stored job unchanged** (never overwrites). `JobQueue` is a scanned `@Fallback @Singleton` in `varco_core` | Every pod can enqueue (web pods included) without listing `JobDispatcherModule`; a retried request enqueues once |

✅ Control knobs live on `JobSpec`, task arguments live in `TaskPayload` — no kwarg-namespace
   collision (the shape `enqueue_task(task, *args, callback_url=…, **kwargs)` has, `runner.py:411-419`).
✅ Skip-if-exists with a deterministic id is the materializer's established convergence rule
   (`materializer.py:143-148`) — one idempotency model in the job subsystem.
✅ Scanning `JobQueue` is side-effect free (no task, no I/O, no lifecycle) — the
   `tls`/`cloudevents`/`redaction` "never scan" rules are about side effects; this has none.
❌ `get()`-then-`save()` leaves a window where two *concurrent* enqueuers of the same id both save an
   identical PENDING row — convergent (same id, same content), same accepted window as the
   materializer. The dangerous case (overwriting a row that already progressed) is excluded because
   the check happens first and a progressed row exists.
❌ A new default binding ⇒ `di-catalogue` and `TestVarcoCoreDIWiringConformance.expected_defaults`
   change (Steps 13, 16).
  Rejected — **`AbstractJobRunner.enqueue_task()` as the enqueue API**: ❌ it also *executes*
  in-process (`runner.py:482-491`) — wrong for a web pod handing work to workers; ❌ kwarg collision.
  Rejected — **module-level `enqueue(store, spec)` functions**: ❌ not injectable; 066 would have to
  resolve the store itself.

### §D-loop — the dispatcher loop

| ID | Choice | Consequence |
|---|---|---|
| D-loop | One poll task + one reap task + one task per in-flight job, all created lazily in `start()`. Capacity is a lazily-created `asyncio.Semaphore(max_concurrent)` acquired **before** claiming. Claim with `owner_id` + `lease_ttl`; heartbeat `renew()` every `lease_ttl / 3`, tracking the **returned** job's `lease_epoch`; terminal write `save(..., expected_epoch=<tracked epoch>)`. Idle ⇒ `poll_interval × U(0.9, 1.1)`; a claim ⇒ loop again immediately; a store error ⇒ capped exponential back-off (≤ 30 s). Reap task calls `reap_expired_leases(limit=100)` every `reap_interval`. `start()` fails fast with `RuntimeError` if the store raises `NotImplementedError` from `reap_expired_leases`. `run_once()` (claim one job, await its execution, return the terminal job or `None`) is the public deterministic seam for tests and scripts | Cross-instance exactly-one-owner execution with at-least-once delivery |

Shutdown (`stop()`): stop claiming → wait `shutdown_grace` for in-flight handlers → cancel the rest →
each cancelled job is written back with `Job.as_released()` (RUNNING → PENDING, `owner_id`/lease
cleared, `lease_epoch + 1`, **`attempt` unchanged**) under the fenced write, so another pod picks it up
at once instead of after lease expiry.

Lost lease: a heartbeat that gets `None` from `renew()` means the job was reaped and possibly
re-claimed — the dispatcher cancels its own handler task and **writes nothing** (any write would be
refused anyway by §D-fence). WARNING log.

✅ Semaphore-before-claim means a pod never holds a lease on a job it has no slot for.
✅ Jitter keeps N pods from polling in lock-step; the store-error back-off keeps a DB outage from
   becoming a log flood.
✅ `as_released()` on shutdown turns a rolling deploy from "jobs stall for `lease_ttl`" into "jobs
   move to the next pod immediately", without charging the job an attempt it did not fail.
✅ Tracking the epoch `renew()` returns makes the dispatcher correct whether a backend bumps the epoch
   on renew or not (`memory.py:385-390` does not; the docstring says it does) — and Step 1 pins that
   contract in conformance.
❌ At-least-once, not exactly-once: a handler that finishes and then loses its terminal write (crash,
   fenced out) runs again elsewhere. Handlers must be idempotent — stated in the feature doc, the
   `JobDispatcher` docstring, and the Pitfalls table.
❌ Polling, not push. At `poll_interval=1.0` and 10 pods that is 10 claim queries/s at idle. Push
   wake-up is parked.
  Rejected — **claim-many (`claim_batch(n)`)**: ❌ a new store method on four backends for a
  throughput gain nobody has measured; one claim per free slot is already bounded by
  `max_concurrent`.

### §D-failure — one retry/DLQ decision, shared with `JobRunner`

| ID | Choice | Consequence |
|---|---|---|
| D-failure | `varco_core/varco_core/job/outcome.py`: `encode_job_result(value) -> bytes \| None` (the `json.dumps`-else-`str()` rule, verbatim from `runner.py:657-662`) and `async terminalize_failure(store, job, exc, *, retry_policy, dlq, expected_epoch) -> Job` (the decision table of `runner.py:686-745`: retry iff `retry_policy` and `attempt + 1 < max_attempts`, else DEAD+DLQ push or FAILED). `JobRunner._handle_job_failure` calls it (keeping its own progress/span calls); the dispatcher calls it with `retry_policy = configured or RetryPolicy()` | No second retry model (CLAUDE.md's resilience rule); identical semantics on both executors |

✅ Retry stays opt-in **per job** (`JobSpec.max_attempts`, default 1) — the dispatcher's default
   `RetryPolicy()` only shapes the back-off curve once a job asked to be retried.
✅ `JobRunner` passes `expected_epoch=None` (it holds no lease) ⇒ byte-identical writes for it.
✅ DLQ `push()` never raises (CLAUDE.md DLQ contract) — `terminalize_failure` relies on it, as
   `JobRunner` does today.
❌ `RetryPolicy.retryable_on` stays ignored, exactly as `JobRunner` ignores it today — parity first;
   honouring it is a one-line, separately-reviewable behaviour change (Parked table).

### §D-unknown — an unknown task name is released with back-off, not failed

| ID | Choice | Consequence |
|---|---|---|
| D-unknown | `registry.get(name) is None` ⇒ if `now - job.created_at < unknown_task_timeout` (default 24 h): `as_released(run_at=now + unknown_task_retry)` (default 60 s), ERROR log naming the job and the registry's known names, no attempt consumed; else terminalize as a failure with `error="unknown task <name>"` | A rolling deploy where new pods enqueue a task old pods do not know yet loses nothing |

✅ Fixes the `recover()` behaviour (`runner.py:566-574`: job left RUNNING forever) instead of
   copying it.
✅ Bounded and loud: one ERROR per minute per such job, terminal after a day.
❌ A typo'd task name costs a day before it fails. Accepted — the ERROR log names the fix on every
   release.
  Rejected — **fail immediately**: ❌ turns every rolling deploy that introduces a task into failed
  jobs (old pods claim them first). Rejected — **retry forever**: ❌ an invisible-by-design
  infinite loop.

### §D-coexist — `JobRunner` and `JobDispatcher` on one store

| ID | Choice | Consequence |
|---|---|---|
| D-coexist | `JobRunner._run_job`: when the stored job **has** a `task_payload`, transition via `store.try_claim(job_id)` (not `get`→`as_running`→`save`); `None` ⇒ claimed elsewhere ⇒ close the coroutine, DEBUG log, return. Bare-coroutine jobs keep today's path (no dispatcher can see them, §D-claim). `enqueue_task()` stamps `tenant_id`; `_run_claimed_job` enters `tenant_context(job.tenant_id)`; both set `current_job` | Every task-backed job runs exactly once whichever executor wins |

✅ When the dispatcher wins the race, the job still runs — from its payload, under its stamped tenant.
✅ Bare jobs with a future `run_at` keep running immediately in-process, exactly as today — the
   change is scoped to the one path that can race.
❌ ⚠️ **Behaviour change**: an `enqueue_task()` job may now run on a *different* pod than the request
   that enqueued it (whenever a dispatcher shares the store), losing request-scoped context the
   coroutine closed over. `3-to-4.md` row + Pitfall: "task handlers must not rely on request
   contextvars — only on their arguments, `current_tenant()` and `current_job()`".

### §D-hooks — `on_terminal`, and `callback_url`

| ID | Choice | Consequence |
|---|---|---|
| D-hooks | `JobDispatcher(on_terminal: Callable[[Job], Awaitable[None]] \| None = None)` — awaited after every terminal write (COMPLETED/FAILED/DEAD), exceptions logged and swallowed. `varco_fastapi.job.callbacks.fire_job_callback(job)` is `JobRunner._fire_callback`'s body extracted to a module function (result decoded from `job.result`), reused by `JobRunner` and passable as `JobDispatcherModule(on_terminal=fire_job_callback)`. With no hook, a job carrying `callback_url` logs one WARNING at completion | `callback_url` works on dispatched jobs without `varco_core` gaining `httpx` |

✅ One hook, not a listener framework — the minimum that keeps HTTP in the HTTP package.
❌ An app must opt in to callback firing on dispatched jobs. Accepted; the WARNING names the fix.
  Rejected — **publish `JobProgressEvent` from the dispatcher**: ⛔ the dispatcher would hold
  `AbstractEventBus` — not one of CLAUDE.md's named bus holders, and not infrastructure that needs to be.

### §D-di — `JobDispatcherModule`, `JobDispatcherSettings`, `JOB_DISPATCH_PHASE`

| ID | Choice | Consequence |
|---|---|---|
| D-di | `JobDispatcherModule(VarcoModule)` in `varco_core/varco_core/job/module.py`: `name="job_dispatcher"`, `phase=JOB_DISPATCH_PHASE` (`Phase.SCHEDULERS + 50`), `depends_on=(CoreModule,)`. Ctor: `settings=`, `registry=`, `retry_policy=`, `on_terminal=`, `enabled=`. `_register`: `bind_settings(…, instance=)` if given; `bind_instance(TaskRegistry, registry)` if given and not user-bound; else a `@Fallback` `TaskRegistry` provider when unresolvable (the `WebhookModule` pattern). `_enabled_from_settings` ⇒ `JobDispatcherSettings.enabled`. `start()` resolves store/registry (+ `AbstractDeadLetterQueue` iff `dead_letter=True`), builds and starts the dispatcher; `stop()` stops it; `.dispatcher` property for tests/diagnose. Settings: `JobDispatcherSettings(VarcoSettings)`, env prefix `VARCO_JOB_DISPATCHER_`, bound by `bind_core_settings()`. `JobDispatcherModule` is exported from `varco_core.job` only (the `WebhookModule` precedent — not top-level) | Worker pods list the module; web pods may list it with `VARCO_JOB_DISPATCHER_ENABLED=false` |

| Setting (`VARCO_JOB_DISPATCHER_…`) | Default | Validation |
|---|---|---|
| `ENABLED` | `true` | — |
| `MAX_CONCURRENT` | `10` | ≥ 1 |
| `POLL_INTERVAL_SECONDS` | `1.0` | > 0 |
| `LEASE_TTL_SECONDS` | `30.0` | > 0 (heartbeat = ttl / 3) |
| `REAP_INTERVAL_SECONDS` | `30.0` | > 0 |
| `SHUTDOWN_GRACE_SECONDS` | `30.0` | ≥ 0 |
| `UNKNOWN_TASK_RETRY_SECONDS` | `60.0` | > 0 |
| `UNKNOWN_TASK_TIMEOUT_SECONDS` | `86400.0` | > 0 |
| `DEAD_LETTER` | `false` | — |
| `OWNER_ID` | `None` ⇒ `"{hostname}:{pid}:{uuid4().hex[:8]}"` | non-empty if set |

✅ CLAUDE.md: "a switchable subsystem is a `VarcoModule` … never a `create_varco_app` keyword"; a
   whole-phase follower takes an integer offset, never a `depends_on` edge.
✅ `SCHEDULERS + 50` starts after `core.job_runner` and `RetentionModule` (both `SCHEDULERS`), so
   every task they register exists before the first claim; reverse stop drains handlers **before**
   any `INFRA` store closes.
✅ `dead_letter` is a bool, not "always resolve the DLQ": every container resolves an
   `AbstractDeadLetterQueue` (the in-memory `@Fallback`), and silently dead-lettering into process
   memory would lose the poison record on restart.
✅ The dispatcher itself is constructed in `start()` (the `RetentionModule` shape,
   `varco_fastapi/varco_fastapi/modules/lifecycle.py:493-503`), not bound as a provider — so the
   `di_wiring` disposer rule has nothing to own.
❌ Not a `Feature` (`varco_core/varco_core/di/features.py:52-89`): features are binding-shaped
   `enable_*` swaps; this is a lifecycle subsystem. Stated in the doc so nobody adds an
   `enable_job_dispatcher()`.

### Alternatives considered (summary)

- **Celery / Dramatiq / SAQ adapter instead of an in-tree dispatcher**: ✅ mature; ❌ a second
  persistence model beside `AbstractJobStore` (the store, leases, fencing and retention already ship);
  ❌ research 019 §B (`:52`): canvas offers no durable replay either, so it buys nothing toward the
  parked workflow stage. Rejected.
- **Make the dispatcher an `AbstractJobRunner`**: ❌ that ABC's contract is "run this coroutine
  in-process" (`base.py:1109-1140`); the dispatcher never receives coroutines. Rejected.
- **`AbstractJobStore.supports_leases` `ClassVar`** for the startup check: ✅ declarative; ❌ another
  flag every out-of-tree store must remember; the probe call is exact. Rejected.

---

## Steps

TDD-ordered: every phase lands its failing tests first. Per CLAUDE.md's conformance discipline, a
conformance test that a shipped backend fails is first committed as a `strict=True` xfail naming a
new `COVERAGE.md` **Conformance findings register** row (`BUG: KI-N`), and the fixing step removes the
marker and closes the row — never a silent in-place fix, never a weakened shared assertion.

### Phase 0 — Preflight

0. [ ] **Re-verify the Context table** and record results in the PR description:
   `rg -n "async def claim_next" varco_*/varco_* testkit/` (expect 4 overrides + the ABC);
   `rg -n "expected_epoch is not None" varco_*/varco_*/job_store.py varco_core/varco_core/job/memory.py`;
   `ls varco_sa/varco_sa/migrations/versions/` (head must still be `0008_schedule_task_name`; if a
   sibling plan landed `0009`, renumber this plan's revision to the next free number and set
   `down_revision` to the then-head); `uv run python -c "import beanie; print(beanie.__version__)"`
   (resolves §D-fence's ⚠️ accessor ASSUMPTION); confirm the jobs table name in
   `varco_sa/varco_sa/job_store.py` (`varco_jobs` per the Beanie collection comment,
   `varco_beanie/varco_beanie/job_store.py:231-233`); whether Plan 059's fake-clock fixture has landed
   (`rg -n "looptime" varco_core/pyproject.toml`) — if so, Phase D tests use it; otherwise every
   interval in tests is ≤ 0.05 s.

### Phase A — the store contract (claim + fence)

1. [ ] `testkit/varco_conformance/job_store.py` — new failing tests (each confines itself to job ids
   it created; never assumes an empty store):
   - `test_claim_next_claims_due_pending_with_lease` — owner/`lease_expires_at`/`lease_epoch > 0` set.
   - `test_claim_next_not_starved_by_future_dated_head` — 150 PENDING, `created_at = now − 1 h`,
     `run_at = now + 1 h`, all with payloads; then one due job; loop `claim_next(require_task_payload=True)`
     ≤ 200 times until it returns ours or `None`; assert ours was claimed.
   - `test_claim_next_require_task_payload_skips_bare_jobs` — oldest due job has no payload; it is
     never returned with the flag, and still claimable with the flag off.
   - `test_concurrent_claim_next_never_double_claims` — 10 due jobs, `asyncio.gather` of 20 calls;
     no job id appears twice among non-`None` results.
   - `test_renewed_epoch_is_the_fencing_token` — claim → `renew()` → `save(as_completed(…),
     expected_epoch=renewed.lease_epoch)` succeeds.
   - `test_concurrent_fenced_saves_exactly_one_wins` — claim; two `save(replace(job,
     lease_epoch=e + 1), expected_epoch=e)` gathered; exactly one raises `StaleLeaseError`.
   Run in-memory now (`uv run pytest varco_core/tests/test_conformance_inmemory.py -q`) and record
   which backends fail under `-m integration`; add `strict=True` xfails + register rows for each
   observed Kind-A failure (expected: Redis/Beanie liveness, Beanie concurrent fence, SA fence
   exception type — but only what actually fails, nothing pre-emptive).
2. [ ] `varco_core/varco_core/job/base.py` — `claim_next(..., require_task_payload: bool = False)`;
   `claim_scan_limit: ClassVar[int] = 1000` used by the default; correct the false "native override"
   sentence (`:840-845`); `save()` docstring: atomicity requirement + "bump `lease_epoch` for CAS";
   `lease_epoch` docstring: "renew may or may not bump it — always fence with the epoch `renew()`
   returned". `varco_core/varco_core/job/memory.py` — the predicate in `claim_next` (`:340-356`).
   **Verify**: in-memory conformance green.
3. [ ] `varco_sa/varco_sa/job_store.py` — `claim_next`: `.where(task_payload.isnot(None))` when
   requested; select up to 10 candidate ids and `try_claim` each in order. `save()` fenced path:
   `with_for_update()` on the epoch `SELECT` for `postgresql`; `IntegrityError` ⇒ `StaleLeaseError`
   when `expected_epoch is not None`. Remove SA xfails + close register rows.
4. [ ] `varco_beanie/varco_beanie/job_store.py` — native atomic `claim_next` (`find_one_and_update`,
   reusing `try_claim`'s update document builder; `return_document=AFTER`); fenced `save()` via
   `replace_one` with the epoch filter (§D-fence). Remove xfails + close rows.
5. [ ] `varco_redis/varco_redis/job_store.py` — `claim_next` pages `ZRANGE status:pending` in windows
   of 100 up to `claim_scan_limit`, applying both predicates client-side. Remove xfails + close rows.
   **Verify (Phase A)**: `uv run pytest varco_sa/tests/ varco_redis/tests/ varco_beanie/tests/ -m integration -k Conformance`
   green with zero new xfails; `rg -n "BUG: KI" testkit/ varco_*/tests/` shows no row opened by
   Step 1 still live.

### Phase B — `Job.tenant_id`, `as_released()`, `current_job()`

6. [ ] Failing tests: `varco_core/tests/test_job_tenant_and_release.py` — `Job.tenant_id` defaults
   `None`; `as_released()` from RUNNING → PENDING, clears `owner_id`/`lease_expires_at`, `lease_epoch
   + 1`, `attempt` unchanged, optional `run_at=`; raises `ValueError` from any other status.
   `varco_core/tests/test_job_context.py` — `current_job()` is `None` outside a scope, the job
   inside, restored after. Conformance: `test_tenant_id_round_trip` (save/get preserves it; a row
   written without it reads back `None`).
7. [ ] `varco_core/varco_core/job/base.py` — `tenant_id` field + `as_released()` (full docstring);
   `varco_core/varco_core/job/context.py` — `current_job()`; persistence:
   `varco_sa/varco_sa/job_store.py` (`Column("tenant_id", String(255), nullable=True)`,
   `_job_to_row`/`_row_to_job` at `:169-282`), `varco_beanie/varco_beanie/job_store.py`
   (`JobDocument.tenant_id: str | None = None` + both converters), `varco_redis/varco_redis/job_store.py`
   (JSON key, absent ⇒ `None`); new revision
   `varco_sa/varco_sa/migrations/versions/0009_job_tenant_id.py` (copy `0008`'s `_has_column` guard
   verbatim). Export `current_job` from `varco_core/varco_core/job/__init__.py` and the top-level lazy
   map/`__all__` (lazy only). **Verify**: the migration round-trip test the repo already runs for
   framework revisions (`rg -ln "0008_schedule_task_name" varco_sa/tests/`) extended to `0009`; unit
   + conformance green.

### Phase C — the enqueue API

8. [ ] Failing tests `varco_core/tests/test_job_queue.py`: `enqueue()` returns a PENDING job with the
   payload; `tenant_id == "t1"` inside `tenant_context("t1")`, `None` outside; non-JSON args ⇒
   `TypeError` before any write; naive `run_at`/`expires_at` ⇒ `ValueError`; `max_attempts=0` ⇒
   `ValueError`; given `job_id` already stored **as RUNNING** ⇒ returns the stored job and the store
   still holds RUNNING; `JobSpec.metadata` is not aliased (mutating the caller's dict after construction
   changes nothing); `JobQueue` resolves from a bootstrapped container
   (`varco_core.di.bootstrap(DIContainer())`).
9. [ ] `varco_core/varco_core/job/queue.py` — `JobSpec`, `JobQueue` (`@Fallback @Singleton`), DESIGN
   blocks for §D-enqueue. Export `JobSpec`, `JobQueue` from `varco_core/varco_core/job/__init__.py`
   and the top-level lazy map/`__all__` of `varco_core/varco_core/__init__.py` (lazy only — no eager
   import added).

### Phase D — the dispatcher

10. [ ] Failing tests `varco_core/tests/test_job_dispatcher.py` (InMemoryJobStore + a local
    `TaskRegistry`; `JobDispatcher.run_once()` is the deterministic seam; the loop tests use tiny
    intervals or 059's fake clock):
    - completes a task; `result` bytes = `encode_job_result(return_value)`; `owner_id` = dispatcher's.
    - two dispatchers on one store, 30 jobs ⇒ every job executed exactly once (counter per id).
    - `max_concurrent=2` ⇒ never more than 2 handlers in flight (gate with `asyncio.Event`).
    - future `run_at` not executed before due; a bare (payload-less) PENDING job never executed.
    - handler sees `current_tenant() == job.tenant_id`; `None` ⇒ no context entered; `current_job()`
      is the claimed job.
    - failure, `max_attempts=1` ⇒ FAILED; with `dlq` ⇒ DEAD + one `DeadLetterEntry(source=JOB)`;
      `max_attempts=3` ⇒ PENDING, `attempt=1`, `run_at` in the future.
    - heartbeat: a handler running > `lease_ttl/3` gets `lease_expires_at` advanced.
    - fenced out: externally reap/re-claim mid-run (bump the stored epoch) ⇒ handler cancelled, no
      write by this dispatcher, nothing raised out of the loop.
    - unknown task ⇒ PENDING, `run_at ≈ now + 60 s`, `attempt` unchanged; with
      `created_at` older than the timeout ⇒ FAILED with `"unknown task"` in `error`.
    - `stop()` with an in-flight handler beyond `shutdown_grace` ⇒ job back to PENDING, `attempt`
      unchanged, `owner_id is None`; within grace ⇒ COMPLETED.
    - `on_terminal` awaited once per terminal state; a raising hook is logged, not propagated;
      `callback_url` set and no hook ⇒ one WARNING.
    - `claim_next` raising ⇒ loop survives and retries with back-off.
    - store without lease support (a stub raising `NotImplementedError` from `reap_expired_leases`)
      ⇒ `start()` raises `RuntimeError` naming the store class; `stop()` before `start()` is a no-op;
      second `start()` raises.
    - layering guard `varco_core/tests/test_job_dispatch_layering.py`: an AST scan of
      `varco_core/varco_core/job/*.py` finds no `varco_fastapi` import and no `@Configuration`.
    `varco_core/tests/test_job_outcome.py`: `encode_job_result` parity cases (dict, `None`,
    non-JSON object ⇒ `str()` bytes); `terminalize_failure` decision table rows.
11. [ ] `varco_core/varco_core/job/outcome.py` and `varco_core/varco_core/job/dispatcher.py` —
    implement §D-loop/§D-failure/§D-unknown/§D-hooks. Every `asyncio.Semaphore`/`Task` created in
    `start()`/first use, never `__init__` (CLAUDE.md lazy rule). Module DESIGN blocks for each §D.
    Export `JobDispatcher` from `varco_core.job` and the top-level lazy map. **Verify**: Step 10
    green; `uv run mypy varco_core/varco_core/job`.
12. [ ] `varco_fastapi/varco_fastapi/job/runner.py` + new `varco_fastapi/varco_fastapi/job/callbacks.py`
    — tests first in `varco_fastapi/tests/test_job_runner_dispatch_coexistence.py`: an
    `enqueue_task()` job already claimed by a dispatcher is **not** run by `JobRunner` (coroutine
    closed, no `RuntimeWarning`); a bare `enqueue(job, coro, run_at=future)` job still runs
    immediately (pinned before the change); `enqueue_task()` stamps `tenant_id`; recovered jobs see
    `current_tenant()`/`current_job()`; `_handle_job_failure` behaviour unchanged across its existing
    tests. Then: `_run_job` per §D-coexist; `_handle_job_failure` → `terminalize_failure`
    (`expected_epoch=None`) + its own progress/span; `_fire_callback` → `fire_job_callback(job, result=…)`
    in `callbacks.py`, exported from `varco_fastapi.job`. **Verify**:
    `uv run pytest varco_fastapi/tests/ -q -k "job or runner or skill"`.

### Phase E — DI

13. [ ] Tests first `varco_core/tests/test_job_dispatcher_module.py`: module registers on
    `in_memory_modules(JobDispatcherModule())`; `TaskRegistry` resolvable after `register()` with no
    other binder; a user-bound registry wins; `VARCO_JOB_DISPATCHER_ENABLED=false` ⇒ recorded disabled
    and `components()` start nothing; phase is `Phase.SCHEDULERS + 50`; `start()`/`stop()` through
    `LifecycleRunner` start and drain the dispatcher; `dead_letter=True` resolves the bound DLQ.
    `varco_core/tests/test_settings_inventory.py` passes with the new class registered.
    `varco_core/tests/test_di_wiring_conformance.py` — add `(JobQueue, None)` to `expected_defaults`.
    Then implement `varco_core/varco_core/job/settings.py` (`JobDispatcherSettings`, field validators
    per §D-di), `varco_core/varco_core/settings/core.py` (`_classes()` gains it),
    `varco_core/varco_core/job/module.py` (`JobDispatcherModule`, `JOB_DISPATCH_PHASE`). Export
    `JobDispatcherSettings` top-level (lazy); `JobDispatcherModule`/`JOB_DISPATCH_PHASE` from
    `varco_core.job` only.

### Phase F — cross-instance integration

14. [ ] `varco_sa/tests/test_job_dispatcher_integration.py`,
    `varco_redis/tests/test_job_dispatcher_integration.py`,
    `varco_beanie/tests/test_job_dispatcher_integration.py` (`pytestmark = pytest.mark.integration`),
    each with an isolated namespace per test (SA: a `uuid4().hex[:8]` Postgres schema via
    `search_path` + `ensure_table()`; Redis: a unique key prefix; Beanie: a unique database name):
    - **two pods**: two store instances on the same backend, two `JobDispatcher`s with distinct
      owner ids, 40 jobs ⇒ each executed exactly once, all COMPLETED.
    - **dead pod**: `store.claim_next(owner_id="dead-pod", lease_ttl=1.0, require_task_payload=True)`
      (a claimant that never heartbeats), then start a dispatcher with `reap_interval=0.5` ⇒ the job
      is reaped and completed by the live dispatcher (`owner_id` is the live one).
    - **tenant** survives the hop: enqueued inside `tenant_context("t9")` on one store instance,
      executed by the other, handler observes `"t9"`.

### Phase G — documentation (same change, never a follow-up — CLAUDE.md / memory)

15. [ ] Docs:
    - `technical_docs/features/job-scheduling-and-leases.md` — new sections after "Leases and
      fencing": **"Cross-instance dispatch (`JobDispatcher`)"** (the §Design diagram, the
      JobRunner-vs-JobDispatcher two-row table, at-least-once + idempotent-handler rule, settings
      table, shutdown/release semantics, unknown-task policy) and **"Enqueueing (`JobQueue`)"**
      (idempotent ids, tenant stamping, "enqueue does not join a transaction — use the outbox for
      enqueue-iff-commit"); fix the `claim_next` paragraph; Pitfalls rows: non-idempotent handler,
      handler relying on request contextvars, `dead_letter=False` with `max_attempts>1` (FAILED, not
      DEAD), `callback_url` silent without a hook, out-of-tree `claim_next` override missing the kwarg.
    - `README.md` "Background Jobs" (`:4290`) — a worker snippet
      (`create_varco_app(container, modules=[*default_modules(), JobDispatcherModule()])` and a
      FastAPI-free `LifecycleRunner` worker) + a `JobQueue.enqueue` snippet; env-var table rows.
    - `ARCHITECTURE.md` "Job Store" (`:995`) — the new modules in the package map and type
      hierarchy.
    - `CLAUDE.md` — Decision Tree, one line: *"Run a job on any worker / durable background work? →
      `varco_core.job` (`JobQueue.enqueue` + `JobDispatcherModule`); in-process with SSE progress →
      `varco_fastapi` `JobRunner`"*; the "Background jobs" section gains one sentence linking the
      feature doc. Nothing else (link, don't inline).
    - `technical_docs/migrations/3-to-4.md` — a `### Background jobs (Plans 069a/069b)` subsection at
      the end of §6 (no renumbering): `claim_next` kwarg for store implementers; fenced-save
      atomicity for store implementers; `JobRunner` cross-pod execution of `enqueue_task()` jobs;
      `alembic upgrade` / `varco migrate` for `0009`; Beanie/Redis need no migration.
    - `CHANGELOG.md` `[4.0.0]` — `### BREAKING — job dispatch: claim_next(require_task_payload=),
      atomic fenced save, JobRunner claims task jobs (Plan 069a, ORCH-1 / RO-9)` after the last
      existing BREAKING block; `### Added` (package-grouped) and `### Fixed` (Redis/Beanie
      `claim_next` starvation, Beanie non-atomic fence, SA fence exception type).
    **Verify**: `rg -n "Plan 069a" CHANGELOG.md technical_docs/ README.md ARCHITECTURE.md` ≥ 5 hits;
    `rg -n "JobDispatcher" CLAUDE.md` ≤ 2 lines.

### Phase H — snapshots and gates

16. [ ] Regenerate and commit, in this order: `uv run python scripts/api_surface.py` (new top-level
    names); `make settings-reference` (`JobDispatcherSettings`); `make di-catalogue` (`JobQueue`
    default); `make entry-points` (`JobDispatcherModule` — also rewrites `ARCHITECTURE.md`'s marked
    block); `make asyncapi-check` (expected unchanged — regenerate only if it moves);
    `uv run python scripts/import_budget.py --check --warn-only` (must not warn — the new modules are
    lazy).
17. [ ] Full gate: `make lint` (ruff check + format check + mypy strict + every snapshot `--check`),
    `make test`, `make integration-test PKG=varco_sa`, `PKG=varco_redis`, `PKG=varco_beanie`.

---

## Edge cases

| Input / state | Expected behaviour |
|---|---|
| 1 000 PENDING jobs in retry back-off, one due job | Claimed on every backend (Step 1 liveness test) |
| A bare `JobRunner.enqueue(job, coro)` job | Never claimed by a dispatcher; runs in-process as today |
| `enqueue_task()` job, dispatcher wins the claim | Runs once on the dispatcher; `JobRunner` closes its coroutine |
| Handler finishes, pod dies before the terminal write | Lease expires → reaped → re-run elsewhere (at-least-once) |
| Heartbeat finds the lease gone (stall > ttl) | Handler cancelled, no write; the new owner's outcome stands |
| Graceful shutdown, handler longer than grace | Cancelled → `as_released()` → PENDING, `attempt` unchanged |
| Unknown task name during a rolling deploy | Released with 60 s back-off until a pod knows it; FAILED after 24 h |
| `JobSpec(job_id=X)` enqueued twice | One row; the second call returns the stored job (any status) |
| Enqueue inside `tenant_context("a")`, worker has no ambient tenant | Handler runs under `"a"` |
| Enqueue with no ambient tenant | `tenant_id=None`; handler runs with no tenant context (platform-wide) |
| Store without leases (out-of-tree) | `start()` raises `RuntimeError` naming the class |
| Out-of-tree store overriding `claim_next` without the kwarg | `TypeError` on the first poll — `3-to-4.md` row |
| `max_attempts=3`, no `retry_policy` configured | Retries with `RetryPolicy()` back-off |
| `dead_letter=False`, attempts exhausted | FAILED (not DEAD) |
| `on_terminal` raises | Logged at ERROR; job state unaffected |
| `VARCO_JOB_DISPATCHER_ENABLED=false` | Module recorded disabled; `JobQueue` and `TaskRegistry` still resolvable |
| A job deleted while RUNNING | Terminal fenced write raises `StaleLeaseError` ("row not found") → logged, dropped |

## Verification

```bash
uv run pytest varco_core/tests/test_conformance_inmemory.py varco_core/tests/test_job_queue.py \
  varco_core/tests/test_job_dispatcher.py varco_core/tests/test_job_outcome.py \
  varco_core/tests/test_job_context.py varco_core/tests/test_job_tenant_and_release.py \
  varco_core/tests/test_job_dispatcher_module.py varco_core/tests/test_job_dispatch_layering.py -q
uv run pytest varco_fastapi/tests/ -q
uv run pytest varco_sa/tests/ varco_redis/tests/ varco_beanie/tests/ -m integration -k "job or Conformance"
uv run mypy varco_core/varco_core varco_fastapi/varco_fastapi varco_sa/varco_sa varco_beanie/varco_beanie varco_redis/varco_redis
uv run python scripts/api_surface.py --check
make lint && make test
rg -n "async def claim_next" varco_*/varco_* | wc -l          # 5, every one with require_task_payload
rg -n "varco_fastapi" varco_core/varco_core/job/              # no output
rg -n "BUG: KI" testkit/ varco_*/tests/                       # no row opened by this plan still live
```

## Risks

| # | Risk | Likelihood | Invariant / mitigation |
|---|---|---|---|
| R1 | Non-idempotent handlers double-execute (at-least-once) | Medium | Documented in four places; deterministic `job_id` + idempotent side effects is the recipe. Exactly-once needs the parked workflow stage |
| R2 | Out-of-tree `AbstractJobStore` breaks on the new kwarg / non-atomic fence | Low | `3-to-4.md` row; conformance suite is the check an implementer runs |
| R3 | `JobRunner`-enqueued task jobs now run on another pod and lose request contextvars | Medium | Behaviour change in CHANGELOG BREAKING + Pitfall; only when a dispatcher shares the store |
| R4 | A job that kills its worker (OOM) is reaped and re-claimed forever (`reap` keeps `attempt`, `memory.py:442-447`) | Low | Parked "poison-pill detection"; operator sees repeated reaps in logs. Invariant: reaping never consumes an attempt (unchanged) |
| R5 | SA `claim_next` still SELECT-then-claim: spurious idle under contention | Low | 10-candidate loop; `UPDATE … RETURNING` parked with a trigger |
| R6 | 062/063 lands `@transactional` and users expect `enqueue()` to roll back with the business transaction | Medium | Non-goal stated; feature doc says "use the outbox for enqueue-iff-commit"; 062/063 owns whether `SAJobStore` ever enlists |
| R7 | Migration number collision with a sibling plan | Medium | Step 0 renumbers; `down_revision` = then-head; the idempotent `_has_column` guard makes order irrelevant to the schema |
| R8 | ⚠️ ASSUMPTION — Beanie ≥ 2 raw-collection accessor name | Low | Step 0 checks; `try_claim` already uses the right one |
| R9 | ⚠️ ASSUMPTION — 10 pods × 1 poll/s idle load is acceptable on a shared Postgres | Low | `POLL_INTERVAL_SECONDS` is a setting; push wake-up parked with a trigger |
| R10 | `claim_next` ordering is only approximately FIFO across backends (Redis scores by `created_at` float) | Low | Documented; no test asserts cross-backend order beyond liveness |

## Out of scope / Parked

| Item | Why | Trigger |
|---|---|---|
| Push wake-up (`LISTEN/NOTIFY`, Redis keyspace events, change streams) | Polling is correct and simple | Idle poll load measured as a problem, or sub-second pickup latency required |
| `install_job_metrics()` (claims, in-flight, durations, releases, reaps) | M-sized; logs cover first stage | A consumer operating the dispatcher in production asks for dashboards |
| SA single-statement claim (`UPDATE … RETURNING`, SKIP LOCKED subselect) | Correct today; perf only | Measured claim contention |
| Poison-pill detection (`reap_count`, auto-DEAD after N reaps) | Needs a new column + policy | One reported reap loop |
| Honouring `RetryPolicy.retryable_on` in job retries | Parity with `JobRunner` first | A consumer needs non-retryable exception classes |
| Named queues / task routing / priorities | Not first stage | Two workloads that must not share workers |
| Cross-instance cancel of RUNNING jobs | Needs a cancel-request column + heartbeat check | A consumer must abort long jobs from an admin surface |

## Done — this plan

- [ ] Step 1's six conformance tests green on all four stores, no live `BUG: KI` row from this plan.
- [ ] `JobDispatcher`, `JobQueue`, `JobSpec`, `current_job`, `JobDispatcherModule`,
      `JobDispatcherSettings` shipped with complete docstrings and DESIGN blocks.
- [ ] Two-pod, dead-pod and tenant-hop integration tests green on SA, Redis, Beanie.
- [ ] `JobRunner` coexistence tests green; existing `varco_fastapi` suite unchanged otherwise.
- [ ] Docs, CHANGELOG, `3-to-4.md`, CLAUDE.md one-liner landed in the same change.
- [ ] `make lint` (every snapshot gate) and `make test` green.

## Open questions (for the user, via the coordinator)

1. **Unknown task policy** (§D-unknown): release-with-back-off for up to 24 h (chosen) vs. fail
   immediately. The choice favours rolling deploys over fast feedback on typos.
2. **`JobQueue` as a scanned `@Fallback` default in `varco_core`** (§D-enqueue): always resolvable
   (chosen, so Plan 066 and web pods need no module) vs. bound only by `JobDispatcherModule`.
3. **`callback_url` on dispatched jobs** (§D-hooks): opt-in hook + WARNING (chosen) vs. auto-wiring
   `fire_job_callback` whenever `varco_fastapi` is importable.
