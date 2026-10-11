# Plan 069b — General schedule driver (absorbs RO-6), job chaining and fan-in groups (ORCH-1, part 2 of 2)

**Row:** `ORCH-1` (BACKLOG.md:63), first stage only (BACKLOG.md:32); ships inside 4.0.0
(BACKLOG.md:31). Part 1 — `plans/069a-job-dispatcher-and-enqueue-api.md` — **must be merged first**:
this plan builds on its atomic fenced `save()` (069a §D-fence), `JobQueue`/`JobSpec` (§D-enqueue),
`JobDispatcher` and its terminal-write path (§D-loop), `Job.tenant_id` (§D-tenant), `current_job()`
(§D-context) and `JobDispatcherModule`/`JobDispatcherSettings` (§D-di).

Index: `plans/000-index-4-0-feature-extension.md`. Consumer contract: Plan 066 (GDPR erasure) builds
its per-contributor fan-out on `JobQueue.enqueue_group()` (§D-enqueue-dag); nothing in this plan
designs transactions (062/063).

---

## Goal

After this plan, `JobDispatcherModule(…)` with `VARCO_JOB_DISPATCHER_SCHEDULE_INTERVAL_SECONDS > 0`
materializes **every** enabled, task-bearing `Schedule` on every pod (convergent, no lease) and the
dispatcher executes the occurrences; a `Schedule` may forbid overlapping occurrences. Apps compose
jobs with `JobQueue.enqueue_chain([a, b, c])` (b runs after a succeeds, …) and
`JobQueue.enqueue_group(members, on_complete=cb, rule=…)` (cb runs once, after every member is done),
backed by a new non-claimable `JobStatus.BLOCKED`, released eagerly by the dispatcher and by a
crash-safe sweep.

## Non-goals

- **Durable workflow replay, durable timers, signals, compensation/sagas** — parked (BACKLOG.md:112).
  A chain is a sequence of independent jobs, not a replayed function.
- **Implicit result passing** (Celery-chain style "parent's return value is the child's first arg") —
  §D-results; handlers read predecessor results explicitly.
- **Arbitrary DAGs and nested composition** (a group of chains, a chain containing a group) — only a
  flat chain and a flat fan-in group; Parked table.
- **Relative delays between chain steps** ("run b 5 minutes after a") — a step's `run_at` is absolute.
- **Cross-instance cancellation of RUNNING jobs** (069a Non-goal) — `JobQueue.cancel()` covers
  PENDING/BLOCKED only.
- **RRULE, seconds-precision cron** — standing parks.
- **`Schedule` concurrency `REPLACE`** (cancel the running occurrence) — needs cross-instance cancel.
- **Changing `RetentionScheduler`'s own loop** — documented coexistence only (§D-retention).
- **HTTP/admin surface for chains or groups.**
- **Editing BACKLOG.md.**

---

## Context — verified against source (2026-10-07)

| Claim | Evidence | Consequence |
|---|---|---|
| Nothing but `RetentionScheduler` drives `ScheduleMaterializer`; it only materializes schedules whose id it derived from a policy name, then calls `JobRunner.recover()` | `varco_core/varco_core/retention/scheduler.py:341-370`, skip rule `:356-360`, `recover` `:367-368` | An app's own `Schedule` rows are never materialized — RO-6 |
| Plan 039 rejected a general driver because it *"must decide catch-up/backpressure/ownership questions for arbitrary app schedules"* | `plans/039-retention-and-purge-automation.md:289-292`, BACKLOG row `:923` | §D-driver answers all three explicitly |
| Catch-up is already a per-schedule policy (`SKIP` default, `FIRE_ONCE`, `BACKFILL_ALL` bounded by `max_backfill`) | `varco_core/varco_core/schedule/materializer.py:153-205`; `schedule/entity.py:124-125` | Catch-up is decided — the driver honours it unchanged |
| Occurrence ids are deterministic (`uuid5(schedule_id, wall)`), and `materialize()` skips an existing id | `materializer.py:89-91`, `:143-148` | Ownership: every pod may drive; convergence is the shipped mechanism. Also gives FORBID its "previous occurrence" lookup |
| `Schedule.tenant_id` exists but is not copied onto materialized jobs | `schedule/entity.py:116`; `_build_job` `materializer.py:252-261` | One-line stamp onto 069a's `Job.tenant_id` |
| A `Schedule` with no `task_name` materializes a payload-less job | `materializer.py:247-251` | The driver skips such schedules (nothing to run); 069a's `require_task_payload` keeps them invisible anyway |
| `JobStatus` has six members; terminal = COMPLETED/FAILED/CANCELLED/DEAD; `as_cancelled()` allowed from any non-terminal state; `as_running()` only from PENDING | `varco_core/varco_core/job/base.py:97-140`, `:447-471`, `:360-384` | A seventh, non-terminal, never-claimable status fits without touching any claim query |
| Every claim path selects `status == PENDING` | `memory.py:299`, `:342`; `varco_sa/varco_sa/job_store.py:724`; Beanie `try_claim` filter `job_store.py:775`; Redis PENDING sorted set | A BLOCKED row is invisible to every claimant by construction |
| SA `status` is `String(32)`; Beanie `status: str`; Redis keeps one sorted set per status and `delete_where(status=None)` iterates `tuple(JobStatus)` | `varco_sa/varco_sa/job_store.py:93-94`; `varco_beanie/varco_beanie/job_store.py:147`; `varco_redis/varco_redis/job_store.py:241-243`, `:505-506` | No enum-type migration; Redis gains `status:blocked` automatically |
| The A2A skill surface maps PENDING/RUNNING → "working", COMPLETED → result, **everything else → failure** | `varco_fastapi/varco_fastapi/router/skill.py:997-1002` | ⛔ A BLOCKED job would be reported as failed — must map to "working" |
| `JobStatusResponse.status` is typed `JobStatus` | `varco_fastapi/varco_fastapi/job/response.py:58` | The OpenAPI enum gains `"blocked"` — API-visible (`3-to-4.md`) |
| `JobPoller` only touches RUNNING rows; `delete_where` filters by `completed_before`/`expires_before`/status | `varco_fastapi/varco_fastapi/job/poller.py:44-127`; `job/base.py:880-1001` | BLOCKED rows are never reaped or age-failed; retention can delete a *completed predecessor* — §D-resolve's orphan rule |
| No batched read exists on `AbstractJobStore` | `base.py:655-1103` | Fan-in evaluation of N members = N round trips — §D-getmany |
| `RetentionLifecycle` registers `varco.retention.purge` into the resolvable `TaskRegistry` (private one if unresolvable) | `varco_fastapi/varco_fastapi/retention.py:103-136` | With 069a's `@Fallback` `TaskRegistry`, retention occurrences are executable by the dispatcher |
| Research: *"Celery canvas (chain/group/chord) … remain for simple fan-out but offer no durable replay"*; Prefect 3 offers dynamic `.map()` fan-out | `design/research/019-streaming-orchestration-and-test-performance-2026.md:50-52` | Chain/group/chord is the established *first-stage* vocabulary. ⚠️ The brief does not detail canvas semantics; the specifics this plan cites are ASSUMPTIONS (Risks R8) |

---

## Design

```
enqueue_chain([A, B, C])                          enqueue_group([M1..Mn], on_complete=CB, rule)
  save C  BLOCKED  depends_on=(B,)                  save CB BLOCKED  depends_on=(M1..Mn), rule
  save B  BLOCKED  depends_on=(A,) dependents=(C,)  save Mi PENDING  dependents=(CB,)   (i = 1..n)
  save A  PENDING                  dependents=(B,)
        (tail first, head last — §D-enqueue-dag)

JobDispatcher, after any terminal write of job J            dependency sweep (every 15 s, every pod)
  for D in J.dependents:  resolve_blocked(D)  ── eager       list_by_status(BLOCKED, limit=500)
                                                            for D: resolve_blocked(D)   ── backstop
resolve_blocked(D):
  preds = store.get_many(D.depends_on)
  verdict = evaluate(D.dependency_rule, preds, D.created_at, now, orphan_grace)
  RELEASE ⇒ CAS  BLOCKED → PENDING     (fenced save, lease_epoch + 1)
  CANCEL  ⇒ CAS  BLOCKED → CANCELLED(error=reason) ⇒ cascade to D.dependents
  WAIT    ⇒ nothing

ScheduleDriver (every schedule_interval, every pod)
  for s in schedule_repo.find_all_enabled():
     no task_name ⇒ skip ;  s.concurrency == FORBID and previous occurrence unfinished ⇒ skip
     materialize(s)  (unchanged: catch-up policy, uuid5 ids, skip-if-exists)  → PENDING jobs
     save(s with last_materialized_at=now)
```

### §D-driver — `ScheduleDriver`: every enabled, task-bearing schedule, on every pod

| ID | Choice | Consequence |
|---|---|---|
| D-driver | `varco_core/varco_core/schedule/driver.py`: `ScheduleDriver(schedule_repo, job_store, *, interval=0.0)` — `start()`/`stop()`/`sweep_once(*, now=None) -> int`. `interval <= 0` ⇒ no task (the `ScheduleRematerializer`/`RetentionScheduler` precedent). Per sweep: `find_all_enabled()`; skip schedules with no `task_name` (INFO once per `schedule_id`); apply §D-concurrency; `materialize()` unchanged; on any new job, `save(replace(s, last_materialized_at=now))`. One schedule's exception is logged and the sweep continues. **No `recover()` call** — execution is the dispatcher's | RO-6 closed |

**The three questions Plan 039 left open, answered:**

| Question | Answer |
|---|---|
| Ownership — which schedules? | Every enabled schedule with a `task_name`. A schedule without one has no executable body (and stays the app's own business, as Plan 039 defined it) |
| Catch-up — after driver downtime? | The schedule's own `CatchUpPolicy` (`SKIP` default = coalesce to the latest missed occurrence). The driver adds nothing |
| Backpressure — slow occurrence, fast cron? | Per schedule: `ScheduleConcurrency.ALLOW` (default, today's materializer behaviour) or `FORBID` (§D-concurrency) |

✅ It computes nothing: cron, occurrences, ids and DST all stay in `ScheduleMaterializer` — "no second
   scheduler" stays literally true (CLAUDE.md's recurring-schedules rule; the materializer adds **no
   lease** and neither does this).
✅ Runs on every pod; two pods converge on the same `uuid5` rows (`materializer.py:24-28`); the
   dispatcher's claim makes execution exclusive.
❌ Every pod calls `find_all_enabled()` per sweep. Accepted (Plan 039 accepted the same for retention);
   `interval` is a setting.
  Rejected — **leader election / a lease row for the driver**: ⛔ CLAUDE.md, recurring schedules: a
  synthetic lease row corrupts every `list_by_status`/`delete_where` caller.
  Rejected — **driver calls `JobRunner.recover()`** (the retention shape): ❌ unleased execution, the
  starving scan 069a fixed, and a `varco_fastapi` dependency.

### §D-concurrency — `Schedule.concurrency: ScheduleConcurrency = ALLOW`

| ID | Choice | Consequence |
|---|---|---|
| D-concurrency | New `StrEnum ScheduleConcurrency {ALLOW, FORBID}` and `Schedule.concurrency` (persisted: SA `schedules.concurrency String(16) NULL` ⇒ ALLOW; Beanie field). `FORBID`: before materializing, the driver derives the previous occurrence's id — `cron.at_or_before(last_materialized_at` in the schedule's zone`)` → `uuid5` via a now-public `ScheduleMaterializer.occurrence_job_id(schedule, wall)` — and skips the schedule this sweep if that job exists and is not terminal. Deleted/never-materialized previous ⇒ proceed | A per-minute schedule whose job takes 5 minutes no longer piles up when the author says so |

✅ Default `ALLOW` = byte-identical to the shipped materializer.
✅ Combined with the default `CatchUpPolicy.SKIP`, `FORBID` coalesces: when the slow run finishes, the
   next sweep materializes only the latest due occurrence.
✅ No extra index or query — one `get()` on a deterministic id.
❌ `FORBID` + `BACKFILL_ALL` checks only the latest previously-materialized occurrence. Documented
   (backfill and forbid are contradictory intents; the feature doc says so).
❌ Schema change on the `schedules` framework table (in `0010`, below).
⚠️ ASSUMPTION — the vocabulary mirrors Kubernetes CronJob `concurrencyPolicy: Allow | Forbid |
   Replace` (Replace parked here); verify against the Kubernetes docs before writing the feature doc.
  Rejected — **a driver-wide flag**: ❌ backpressure is a property of one schedule's workload.
  Rejected — **"use `AbstractDistributedLock` in the handler" as the only answer**: ✅ still a valid
  recipe (documented); ❌ jobs still pile up as PENDING rows behind the lock.

### §D-retention — coexistence with `RetentionModule`

| ID | Choice | Consequence |
|---|---|---|
| D-retention | No code change to retention. Documented: with the schedule driver on, configure `RetentionModule(interval=0.0)` — `RetentionLifecycle` still upserts its schedules and registers `varco.retention.purge`; the driver materializes them; the dispatcher executes them. Running both loops is correct (convergent ids, atomic claims) but redundant | One materializer loop per deployment, by documentation |

✅ No cross-module coupling (`RetentionModule` reading `ModuleRegistry` for a sibling would be the
   first such dependency in the tree).
❌ Easy to leave both on. Pitfalls row; harmless when it happens.

### §D-blocked — `JobStatus.BLOCKED` and three dependency fields

| ID | Choice | Consequence |
|---|---|---|
| D-blocked | `JobStatus.BLOCKED = "blocked"` — non-terminal, never claimable (no claim path selects it). `Job.depends_on: tuple[UUID, ...] = ()` (predecessors — the predicate), `Job.dependents: tuple[UUID, ...] = ()` (successors — the eager-release pointers), `Job.dependency_rule: DependencyRule = ALL_SUCCEEDED` (`StrEnum {ALL_SUCCEEDED, ALL_DONE}`). New transitions: `as_unblocked()` (BLOCKED → PENDING) and `as_cancelled(error: str \| None = None)` (additive kwarg). All persisted (SA: two JSON-text columns + `dependency_rule String(16)`; Beanie fields; Redis JSON) | ⚠️ **API-visible**: a new status value in `JobStatusResponse`/OpenAPI and for any exhaustive `match` on `JobStatus` |

✅ "Not claimable" is enforced by the absence of a predicate, not by a new one — zero change to the
   four claim implementations 069a just hardened.
✅ Both pointer directions are written once, at enqueue, when every id is known — no row's pointers
   are ever rewritten later.
❌ A new enum member is a break for clients that switch exhaustively. `3-to-4.md` row; varco's own
   switch (A2A, `skill.py:999`) is fixed in Step 11.
  Rejected — **keep PENDING + a "blocked" predicate (`depends_on` non-empty and unresolved)**: ❌ every
  claim query on four backends gains a predicate it cannot index cheaply; ❌ a store override that
  forgets it runs jobs out of order.
  Rejected — **create the successor only when the predecessor completes** (Celery's chain model:
  the next task is not sent until the parent finishes): ❌ the "complete parent, then create child"
  pair is two writes; a crash between them silently truncates the chain, and a recovery sweep would
  need a "successor not yet created" marker anyway; ❌ an observer cannot see the pipeline's future
  steps.
  Rejected — name `WAITING`: ❌ reads like "waiting to be claimed", i.e. PENDING. (Open question 1.)

### §D-cas — release and cascade-cancel are fenced, epoch-bumping writes

| ID | Choice | Consequence |
|---|---|---|
| D-cas | One helper, `_cas(store, before, after)`: `store.save(replace(after, lease_epoch=before.lease_epoch + 1), expected_epoch=before.lease_epoch)`; `StaleLeaseError` ⇒ `None` ("someone else moved it — re-read next time") | Two pods releasing the same job cannot both succeed; a release can never overwrite a job a dispatcher already claimed |

✅ The dangerous interleaving — pod A releases, a dispatcher claims (epoch bumps), pod B's stale
   release lands — is refused by 069a's atomic fence on all four backends.
✅ No new store method: the CAS is the shipped `save(expected_epoch=)` contract, used as documented in
   069a ("bump `lease_epoch` for CAS").
❌ `lease_epoch` now also counts dependency transitions, not only lease events. Docstring updated;
   nothing reads it as a lease counter.
  Rejected — **`try_release(job_id)` store primitive** (a `try_claim` twin): ❌ a fifth method across
  four backends + conformance for what the fence already guarantees.

### §D-resolve — eager release plus a crash-safe sweep

Evaluation (`varco_core/varco_core/job/dependencies.py`, `evaluate_dependencies(rule, predecessors,
*, created_at, now, orphan_grace) -> DependencyVerdict`):

| Predecessor states | `ALL_SUCCEEDED` (default — chains, Celery-chord-like) | `ALL_DONE` (summarize partial failure) |
|---|---|---|
| all COMPLETED | RELEASE | RELEASE |
| any FAILED / DEAD / CANCELLED | **CANCEL** `"dependency <id> ended <status>"` (immediately) | WAIT until all terminal, then RELEASE |
| any PENDING / RUNNING / BLOCKED (none failed) | WAIT | WAIT |
| any missing, `now − created_at < orphan_grace` | WAIT (enqueue may still be in flight) | WAIT |
| any missing, older than `orphan_grace` | CANCEL `"dependency <id> missing"` | CANCEL (malformed group) |

`resolve_blocked(store, job, …)`: skip unless `job.status is BLOCKED`; `get_many(depends_on)`;
evaluate; apply via §D-cas; a CANCEL cascades to `job.dependents` through an iterative work-list with
a visited set (bounded by chain length; no recursion).

Triggers:

1. **Eager** — `JobDispatcher`, right after any terminal write (COMPLETED/FAILED/DEAD; not after
   `as_retry` or `as_released`), resolves each id in the finished job's `dependents`. Also
   `JobQueue.cancel()` (§D-cancel).
2. **Sweep** — a third dispatcher task every `dependency_sweep_interval_seconds` (default 15):
   `list_by_status(BLOCKED, limit=dependency_sweep_limit)` (default 500) → `resolve_blocked` each.
   `0` disables it (tests only; documented as unsafe).

| ID | Choice | Consequence |
|---|---|---|
| D-resolve | Eager release is the latency path; the sweep is the **correctness** path — release state is always recomputable from the store, so a crash between a terminal write and its eager release, a job terminalized by `JobRunner`/`JobPoller`/an HTTP cancel, or a lost CAS race all converge within one sweep | No step's outcome depends on an in-memory event surviving |

✅ Order is "terminal write, then release" — never the reverse — so a released successor can never
   observe a predecessor that is still RUNNING.
✅ Cancel-on-first-failure under `ALL_SUCCEEDED` frees the callback as soon as the outcome is decided.
✅ `ALL_DONE` gives Plan 066 its "erase everywhere you can, then report which contributors failed"
   callback without a second mechanism.
❌ The sweep evaluates only the 500 oldest BLOCKED rows per pass. A deployment with more than 500
   legitimately-waiting BLOCKED jobs relies on eager release for the newer ones until the old ones
   drain. Documented with the setting; ⚠️ Risks R3.
❌ Fan-in evaluation is O(members) per evaluation (one `get_many`). With short-circuit at the first
   non-terminal member, members finishing roughly in order stay cheap; the bound is `MAX_FAN_IN`.
  Rejected — **an atomic per-group counter** (decrement on member completion — the Redis-counter chord
  strategy): ✅ O(1); ❌ a new atomic-increment primitive on four stores; ❌ a counter is not
  recomputable after a lost decrement, so it still needs this sweep as a backstop.

### §D-getmany — `AbstractJobStore.get_many()`

| ID | Choice | Consequence |
|---|---|---|
| D-getmany | Concrete default `async def get_many(self, job_ids: Sequence[UUID]) -> dict[UUID, Job]` (loops `get`; missing ids absent). Native overrides: SA `WHERE job_id IN (…)`, Beanie `{"_id": {"$in": …}}`, Redis pipelined `GET`s, in-memory dict lookups under its lock | One round trip per fan-in evaluation on every in-tree store |

✅ The house "Compatibility posture": a new ABC method ships with a correct portable default, never
   as `@abstractmethod` (`base.py:823-827`).
❌ A fifth conformance concern. Accepted — two tests.

### §D-enqueue-dag — `enqueue_chain`, `enqueue_group`, save order, idempotency

```python
async def enqueue_chain(self, specs: Sequence[JobSpec]) -> list[Job]: ...      # head first
async def enqueue_group(self, members: Sequence[JobSpec], *, on_complete: JobSpec,
                        rule: DependencyRule = DependencyRule.ALL_SUCCEEDED) -> JobGroup: ...

@dataclass(frozen=True)
class JobGroup:
    callback_id: UUID
    member_ids: tuple[UUID, ...]
```

| ID | Choice | Consequence |
|---|---|---|
| D-enqueue-dag | Ids are fixed up front (`spec.job_id` or `uuid4()`). **Either every spec carries a `job_id` or none does** (`ValueError` otherwise). Save order: the dependent rows first (chain: tail → head-exclusive as BLOCKED; group: the callback as BLOCKED), the claimable rows **last** (chain head / group members as PENDING). With explicit ids, each row is skip-if-exists (069a's rule), so retrying a half-written enqueue completes it. Limits: `1 <= len(specs) <= MAX_CHAIN = 1000`; `1 <= len(members) <= MAX_FAN_IN = 1000` (`ClassVar`s on `JobQueue`). All rows share one `tenant_id` (`current_tenant()`, 069a) | A crash mid-enqueue leaves only non-claimable rows; nothing runs out of order, ever |

✅ The invariant that matters — **no step can run before its predecessors exist** — holds at every
   instant, because a row only becomes claimable after everything it feeds is already stored.
✅ A partial enqueue is repairable (retry with the same ids) or, if abandoned, cleaned up by the
   orphan rule (CANCEL after `orphan_grace`).
✅ Plan 066's erasure: `job_id = uuid5(request_id, contributor_name)` per member and
   `uuid5(request_id, "complete")` for the callback makes a retried DSAR request converge.
❌ A chain/group enqueue is N writes, not one transaction — Non-goal (062/063); the save order and
   the orphan rule are the substitute.
❌ A step's `run_at` is absolute; it is honoured after release (a released job with a future `run_at`
   waits). No relative delay.

### §D-results — no implicit result passing

| ID | Choice | Consequence |
|---|---|---|
| D-results | A task's arguments are fixed at enqueue. A handler that needs its predecessors' outcomes calls `await queue.dependency_results(job=None) -> dict[UUID, Job]` (`job` defaults to `current_job()`; returns the predecessor `Job`s — status, `result` bytes, `error` — via `get_many`) | Data flow is explicit; ordering and data are separate concerns |

✅ `TaskPayload` stays exactly what the enqueuer wrote and JSON-safe (`job/task.py:97-101`); results
   are opaque bytes (`base.py:216-217`) that would not be JSON-safe arguments anyway.
✅ Fan-in needs no N-way argument splice into the callback's row.
❌ More code in a handler than Celery's chained signature. ⚠️ ASSUMPTION (Risks R8): Celery passes the
   parent's return value as the child's first argument unless the signature is immutable (`.si()`), and
   a chord callback receives the list of header results — varco deliberately implements the `.si()`
   shape plus an explicit read.
  Rejected — **splice predecessor results into the dependent's payload at release**: ❌ the release
  becomes a payload rewrite on a CAS path; ❌ unbounded row growth for a fan-in.

### §D-cancel — `JobQueue.cancel()` for PENDING and BLOCKED

| ID | Choice | Consequence |
|---|---|---|
| D-cancel | `async def cancel(self, job_id: UUID, *, reason: str = "cancelled") -> bool` — CAS PENDING/BLOCKED → CANCELLED; `False` for RUNNING/terminal/missing; on success resolves the job's dependents eagerly (cascade under `ALL_SUCCEEDED`, release-when-all-done under `ALL_DONE`) | Cancelling a chain's head cancels the chain; cancelling a group member resolves the callback |

✅ Uses the same §D-cas helper — a cancel racing a claim loses cleanly (`False`), never cancels a
   running job behind the runner's back.
❌ RUNNING jobs cannot be cancelled cross-instance (069a Non-goal).

### §D-surface — what a client sees

`skill.py:999` maps BLOCKED → "working" (with PENDING/RUNNING). `JobStatusResponse` simply exposes the
new value. `JobPoller` and retention are unchanged; the Pitfalls table states that deleting a
COMPLETED predecessor before its dependents are released (retention window shorter than
`orphan_grace` + sweep interval) cancels them as orphans.

### Settings and DI (additions to 069a's `JobDispatcherSettings` / `JobDispatcherModule`)

| Setting (`VARCO_JOB_DISPATCHER_…`) | Default | Meaning |
|---|---|---|
| `SCHEDULE_INTERVAL_SECONDS` | `0.0` | `> 0` adds the `job_dispatcher.schedule_driver` component at `Phase.SCHEDULERS` |
| `DEPENDENCY_SWEEP_INTERVAL_SECONDS` | `15.0` | `0` disables the backstop sweep |
| `DEPENDENCY_SWEEP_LIMIT` | `500` | BLOCKED rows evaluated per sweep |
| `DEPENDENCY_ORPHAN_GRACE_SECONDS` | `3600.0` | Missing-predecessor tolerance |

`JobDispatcherModule` gains `schedule_repo: AbstractScheduleRepository | None = None`; `validate()`
reports *"schedule driver enabled but no AbstractScheduleRepository is bound — pass
JobDispatcherModule(schedule_repo=…) or bind one"* when `SCHEDULE_INTERVAL_SECONDS > 0` and none is
given or resolvable. `components()` returns the driver component (if enabled) and the dispatcher
component (`SCHEDULERS + 50`) — exact-reverse stop drains the dispatcher before the driver.

### Alternatives considered (summary)

- **Adopt a workflow engine now (DBOS-style `@workflow`/`@step`)**: ✅ the end state; ❌ locked
  decision BACKLOG.md:32 — first stage only; ❌ XL (BACKLOG.md:112). Chains/groups over plain jobs are
  the stepping stone the park names.
- **Model a group as a parent "group job" that polls its children** (Celery's `chord_unlock`
  polling-task shape, ⚠️ ASSUMPTION R8): ❌ a RUNNING poller holds a lease and a worker slot for the
  whole fan-out; the sweep does the same job with no slot.
- **Prefect-style `wait_for=` on a single enqueue** (⚠️ ASSUMPTION R8: Prefect 3 tasks accept
  `wait_for`, with `allow_failure` for done-regardless semantics): ✅ maps exactly onto
  `depends_on` + `ALL_DONE`; ❌ exposing raw `depends_on` on `JobSpec` invites arbitrary DAGs whose
  save-order invariant the caller would have to maintain. Kept internal; parked as "general DAG
  enqueue" with a trigger.

---

## Steps

Same discipline as 069a: failing tests first; a backend conformance failure is a `strict=True` xfail +
`COVERAGE.md` register row, closed by the step that fixes it.

### Phase 0 — Preflight

0. [ ] 069a merged (`rg -n "require_task_payload" varco_core/varco_core/job/base.py`); migration head
   recorded (`ls varco_sa/varco_sa/migrations/versions/`) — this plan's revision is the next free
   number (`0010` if 069a took `0009`); re-verify the Context table's `skill.py:997-1002` and
   `retention.py:103-136` line ranges.

### Phase A — store: `BLOCKED`, dependency fields, `get_many`

1. [ ] `testkit/varco_conformance/job_store.py` — failing tests: `test_blocked_job_never_claimed`
   (`claim_next` with and without `require_task_payload`, and `try_claim`, all return `None`/skip it);
   `test_dependency_fields_round_trip` (`depends_on`, `dependents`, `dependency_rule`; a row written
   without them reads back `()`/`()`/`ALL_SUCCEEDED`); `test_list_by_status_blocked`;
   `test_get_many_returns_present_ids_only`; `test_cas_release_exactly_one_wins` (two gathered
   epoch-bumping fenced saves BLOCKED → PENDING; exactly one `StaleLeaseError`).
   `varco_core/tests/test_job_dependency_transitions.py` — `as_unblocked()` only from BLOCKED;
   `as_cancelled(error=…)` sets `error`, works from BLOCKED; `JobStatus.BLOCKED.is_terminal is False`.
2. [ ] `varco_core/varco_core/job/base.py` — `JobStatus.BLOCKED` (state-machine docstring updated),
   `DependencyRule`, the three fields, `as_unblocked()`, `as_cancelled(error=None)`, `get_many()`
   default; `varco_core/varco_core/job/memory.py` — `get_many` override. **Verify**: in-memory
   conformance green.
3. [ ] Persistence + native `get_many`: `varco_sa/varco_sa/job_store.py` (columns `depends_on Text`,
   `dependents Text` — JSON lists of UUID strings — and `dependency_rule String(16)`, all nullable;
   converters; `IN` query); `varco_beanie/varco_beanie/job_store.py` (`list[UUID]` fields defaulting
   empty, `dependency_rule: str | None`; `$in`); `varco_redis/varco_redis/job_store.py` (JSON keys;
   pipelined `GET`). New revision `varco_sa/varco_sa/migrations/versions/0010_job_dependencies.py` —
   the three `varco_jobs` columns **and** `schedules.concurrency String(16) NULL` (§D-concurrency),
   each behind 0008's `_has_column` guard. **Verify**: `-m integration -k Conformance` green on all
   three packages; the framework-revision round-trip test covers `0010`.

### Phase B — evaluation and resolution

4. [ ] Failing tests `varco_core/tests/test_job_dependencies.py`: every row of §D-resolve's table for
   both rules (pure `evaluate_dependencies`, injected `now`); `resolve_blocked` applies RELEASE/CANCEL
   via CAS; a lost CAS returns `None` and writes nothing; cascade cancels a 3-deep chain in one call;
   a non-BLOCKED job is a no-op; visited-set guard on a (malformed) cycle.
5. [ ] `varco_core/varco_core/job/dependencies.py` — `DependencyVerdict` (frozen), 
   `evaluate_dependencies`, `resolve_blocked`, `_cas`; DESIGN blocks for §D-cas/§D-resolve.

### Phase C — the composition API

6. [ ] Failing tests in `varco_core/tests/test_job_queue_composition.py`: `enqueue_chain` writes the
   documented statuses/pointers and returns head-first; save order observed through a recording store
   wrapper (claimable rows last); mixed explicit/implicit ids ⇒ `ValueError`; empty or oversize ⇒
   `ValueError`; retrying a chain whose head was never saved (simulate: pre-save the tail rows) completes
   it without duplicating; `enqueue_group` — callback BLOCKED with `depends_on == member_ids` and the
   rule; members PENDING with `dependents == (callback_id,)`; all rows share the ambient tenant;
   `cancel()` truth table (PENDING/BLOCKED ⇒ `True` + cascade; RUNNING/terminal/missing ⇒ `False`);
   `dependency_results()` inside a `current_job` scope returns predecessor jobs, outside with no
   argument ⇒ `ValueError`.
7. [ ] `varco_core/varco_core/job/queue.py` — `enqueue_chain`, `enqueue_group`, `cancel`,
   `dependency_results`, `JobGroup`, `MAX_CHAIN`/`MAX_FAN_IN`; exports in `varco_core/varco_core/job/__init__.py`
   and the top-level lazy map (`JobGroup`, `DependencyRule`).

### Phase D — dispatcher integration

8. [ ] Failing tests `varco_core/tests/test_job_dispatcher_dependencies.py` (in-memory, two
   dispatchers): a 3-step chain runs strictly in order, each step once; a failing middle step (no
   retry) cancels the tail with the documented error; a retried middle step (`max_attempts=2`) does
   **not** release the tail until it succeeds; a 25-member group's callback runs exactly once, after
   the last member, and `dependency_results()` inside it sees 25 COMPLETED jobs; `ALL_DONE` with one
   failing member runs the callback; eager path disabled (monkeypatched) ⇒ the sweep alone completes
   the chain; a job terminalized behind the dispatcher's back (`store.save(as_failed)`) is picked up by
   the sweep; an orphaned BLOCKED row (missing predecessor, `created_at` older than grace) is
   cancelled.
9. [ ] `varco_core/varco_core/job/dispatcher.py` — eager resolution after terminal writes; the sweep
   task (lazily created in `start()`, cancelled in `stop()`); `varco_core/varco_core/job/settings.py`
   — the three `DEPENDENCY_*` fields with validators.

### Phase E — the schedule driver

10. [ ] Failing tests `varco_core/tests/test_schedule_driver.py` (`InMemoryScheduleRepository` +
    `InMemoryJobStore`, injected `now`): `interval=0` ⇒ `start()` creates no task; a task-bearing
    schedule is materialized and `last_materialized_at` saved; a schedule without `task_name` is
    skipped and logged once; disabled schedule skipped; two drivers on one store produce one job per
    occurrence; `FORBID` with an unfinished previous occurrence ⇒ nothing materialized, and once it is
    COMPLETED the next sweep materializes only the latest occurrence (`SKIP` coalescing); `FORBID`
    with the previous job deleted ⇒ proceeds; `ALLOW` pinned byte-identical to the materializer's
    current output (written **before** Step 11 touches `materializer.py`); materialized jobs carry
    `Schedule.tenant_id`; one schedule with an invalid cron ⇒ error logged, the others still
    materialize. `varco_core/tests/test_job_dispatcher_module.py` — module with
    `SCHEDULE_INTERVAL_SECONDS > 0` plans two components in order driver → dispatcher; `validate()`
    message when no schedule repository.
11. [ ] `varco_core/varco_core/schedule/entity.py` (`ScheduleConcurrency`, `Schedule.concurrency`),
    `varco_core/varco_core/schedule/materializer.py` (public `occurrence_job_id()`; `_build_job` stamps
    `tenant_id=schedule.tenant_id`), `varco_core/varco_core/schedule/driver.py` (`ScheduleDriver`),
    `varco_sa/varco_sa/schedule.py` and `varco_beanie/varco_beanie/schedule.py` (map `concurrency`),
    `varco_core/varco_core/job/module.py` (driver component, `schedule_repo=`, `validate()`),
    `varco_core/varco_core/job/settings.py` (`SCHEDULE_INTERVAL_SECONDS`).

### Phase F — `varco_fastapi` touchpoints

12. [ ] Test first in `varco_fastapi/tests/` (the existing A2A skill test module — locate with
    `rg -ln "_working_response" varco_fastapi/tests/`): a BLOCKED job renders as "working". Then
    `varco_fastapi/varco_fastapi/router/skill.py:999` adds `JobStatus.BLOCKED`. Audit every other
    `JobStatus` switch: `rg -n "JobStatus\.(PENDING|RUNNING)" varco_*/varco_*` — each hit either
    handles BLOCKED or is documented as intentionally excluding it.

### Phase G — cross-instance integration

13. [ ] `varco_sa/tests/test_job_orchestration_integration.py`,
    `varco_redis/tests/test_job_orchestration_integration.py`,
    `varco_beanie/tests/test_job_orchestration_integration.py` (`integration` marker; per-test schema /
    key prefix / database, as in 069a Step 14): two store instances + two dispatchers — a 3-step chain
    in order exactly once; a 20-member `ALL_SUCCEEDED` group → callback once; one failing member →
    callback CANCELLED; `ALL_DONE` → callback runs. SA and Beanie also: two `ScheduleDriver`s on their
    schedule repositories (`varco_sa/varco_sa/schedule.py`, `varco_beanie/varco_beanie/schedule.py`)
    over one occurrence window ⇒ exactly one job per occurrence, executed once; Redis uses
    `InMemoryScheduleRepository` (no Redis schedule repository ships).

### Phase H — documentation

14. [ ] Docs (same change):
    - `technical_docs/features/job-scheduling-and-leases.md` — **"Chaining and fan-in"** (diagram,
      §D-resolve table, save-order invariant, orphan rule, `dependency_results()`, the Celery/Prefect
      vocabulary mapping marked with its verification status) and **"Schedules on workers"**; replace
      the "Recurrence — … cron deliberately not shipped" paragraph's stale claim if it contradicts the
      driver. Pitfalls rows: retention deleting predecessors; `>DEPENDENCY_SWEEP_LIMIT` waiting jobs;
      chains need a dispatcher on at least one pod (BLOCKED forever otherwise); both retention and
      driver loops on; `FORBID` + `BACKFILL_ALL`.
    - `technical_docs/features/recurring-schedules.md` — the driver, `concurrency`, tenant stamping;
      correct its "materializer downtime" pitfall to point at the driver.
    - `README.md` — "Background Jobs" (`:4290`): chain + group snippets; "Recurring schedules"
      (`:4893`): enabling the driver; env-var table rows.
    - `ARCHITECTURE.md` — "Job Store" (`:995`) state machine with BLOCKED; "Recurring schedules"
      (`:1391`) the driver.
    - `CLAUDE.md` — Decision Tree, under "Timezone / scheduling? → recurring cron schedule": *"driven
      on workers by `JobDispatcherModule` (`ScheduleDriver`) — never a second materializer loop"*;
      one new line: *"Run B after A / run C after all of N? → `JobQueue.enqueue_chain`/`enqueue_group`
      — never a hand-rolled poll for completion"*. The "Recurring schedules" section's sentence
      "the existing `AbstractJobRunner` runs the produced jobs unchanged" is corrected to name the
      dispatcher.
    - `technical_docs/migrations/3-to-4.md` — the 069a subsection gains: `JobStatus.BLOCKED` (API
      clients, exhaustive matches), `0010`, `RetentionModule(interval=0.0)` recommendation.
    - `CHANGELOG.md` `[4.0.0]` — `### BREAKING — JobStatus.BLOCKED (Plan 069b, ORCH-1)`; `### Added`
      — `ScheduleDriver` (RO-6), `Schedule.concurrency`, chains/groups, `get_many`, `JobQueue.cancel`.
    **Verify**: `rg -n "Plan 069b" CHANGELOG.md technical_docs/ README.md ARCHITECTURE.md` ≥ 5.

### Phase I — snapshots and gates

15. [ ] Regenerate/commit: `uv run python scripts/api_surface.py`; `make settings-reference`;
    `make di-catalogue` (expected unchanged — verify); `make entry-points`; `make asyncapi-check`
    (`JobProgressEvent.status` gains a value — regenerate if it moves);
    `uv run python scripts/import_budget.py --check --warn-only`. Then `make lint`, `make test`,
    `make integration-test PKG=varco_sa` / `varco_redis` / `varco_beanie`.

---

## Edge cases

| Input / state | Expected behaviour |
|---|---|
| Chain head fails, `max_attempts=1` | Head FAILED → tail steps CANCELLED with `"dependency <id> ended failed"` |
| Chain middle step retrying | Tail stays BLOCKED until the step is terminal |
| Group member CANCELLED via `JobQueue.cancel` | `ALL_SUCCEEDED` callback CANCELLED; `ALL_DONE` callback runs once all are terminal |
| Pod crashes after a terminal write, before eager release | Next sweep (≤ 15 s) releases the successor |
| Two pods sweep the same BLOCKED job | One CAS wins; the other gets `StaleLeaseError` internally → no-op |
| Crash halfway through `enqueue_group` (callback + some members saved) | Saved members run; callback WAITs; retry with the same ids completes the group, else after `orphan_grace` the callback is CANCELLED |
| Retention deletes a COMPLETED predecessor before release | Successor treated as orphan → CANCELLED after grace (Pitfall: retention window ≥ grace + sweep interval) |
| Released step with a future `run_at` | PENDING, claimed at `run_at` |
| `enqueue_group` with 0 or 1001 members | `ValueError` |
| `enqueue_chain` mixing explicit and generated ids | `ValueError` |
| `dependency_results()` outside a job, no argument | `ValueError` naming the fix |
| A2A client polling a BLOCKED job | "working" |
| Schedule without `task_name` | Never materialized by the driver; INFO logged once |
| `FORBID`, previous occurrence RUNNING across three cron ticks | Nothing materialized; after completion, only the latest due occurrence |
| Driver and `RetentionScheduler` both materializing retention schedules | Same rows (uuid5); executed once |
| `Schedule.tenant_id="t1"` | Occurrence job runs under `tenant_context("t1")` |
| `SCHEDULE_INTERVAL_SECONDS > 0`, no schedule repository | `validate()` problem → startup refused with the message |

## Verification

```bash
uv run pytest varco_core/tests/test_conformance_inmemory.py varco_core/tests/test_job_dependencies.py \
  varco_core/tests/test_job_dependency_transitions.py varco_core/tests/test_job_queue_composition.py \
  varco_core/tests/test_job_dispatcher_dependencies.py varco_core/tests/test_schedule_driver.py \
  varco_core/tests/test_schedule_materializer.py varco_core/tests/test_job_dispatcher_module.py -q
uv run pytest varco_fastapi/tests/ -q
uv run pytest varco_sa/tests/ varco_redis/tests/ varco_beanie/tests/ -m integration -k "job or schedule or Conformance"
uv run mypy varco_core/varco_core varco_fastapi/varco_fastapi varco_sa/varco_sa varco_beanie/varco_beanie varco_redis/varco_redis
make lint && make test
rg -n "JobStatus\.(PENDING|RUNNING)" varco_*/varco_*   # every switch audited (Step 12)
rg -n "BUG: KI" testkit/ varco_*/tests/                # no row opened by this plan still live
```

## Risks

| # | Risk | Likelihood | Invariant / mitigation |
|---|---|---|---|
| R1 | Clients with exhaustive `JobStatus` handling break on `"blocked"` | Medium | CHANGELOG BREAKING + `3-to-4.md`; varco's own switch fixed (Step 12) |
| R2 | Retention deletes predecessors before release → dependents cancelled as orphans | Low | Pitfall with the sizing rule; eager release normally fires within milliseconds of the terminal write |
| R3 | > `DEPENDENCY_SWEEP_LIMIT` legitimately-waiting BLOCKED rows starve the backstop for newer rows | Low | Eager release still runs; limit is a setting; ⚠️ ASSUMPTION that first-stage pipelines stay well under 500 concurrent waiting steps |
| R4 | `enqueue_*` is not atomic (N writes) | Medium | Invariant: claimable rows are written last, so nothing runs early; orphan rule cleans abandoned enqueues; atomicity belongs to 062/063 |
| R5 | `lease_epoch` semantics widened (counts dependency CAS writes) | Low | Docstring; nothing interprets the number, only equality |
| R6 | Migration number collision with a sibling plan | Medium | Step 0 renumbers; idempotent guards |
| R7 | Both retention and driver loops on | Medium | Correct by convergence; Pitfall row |
| R8 | ⚠️ ASSUMPTIONS from outside the repo, not verified at plan time (research 019 §B does not detail them): Celery chain passes the parent result unless `.si()`; chord callback receives the header results and fails on a header failure; Celery's `chord_unlock` polling vs. Redis-counter strategies; Prefect 3 `wait_for`/`allow_failure`; Kubernetes CronJob `concurrencyPolicy`; Airflow `all_success`/`all_done` trigger rules as the model for the two `DependencyRule`s | — | Used only as vocabulary/precedent, never as a behavioural dependency. Step 14 re-checks each against its upstream docs (`docs.celeryq.dev/en/stable/userguide/canvas.html`, `docs.prefect.io`, `kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/`, Airflow "Trigger Rules") before the feature doc cites it, and drops any that do not hold. Research 019's DBOSify citation (`:43`) is unverified and not cited |

## Out of scope / Parked

| Item | Why | Trigger |
|---|---|---|
| General DAG enqueue (raw `depends_on` on `JobSpec`) and nested composition (group of chains, chain of groups) | The save-order invariant needs care per shape | A consumer needs a shape chain+group cannot express |
| Implicit result passing | §D-results | Repeated boilerplate reported by two consumers |
| Relative step delays | Absolute `run_at` covers scheduled pipelines | A pipeline needs "N minutes after the previous step" |
| `ScheduleConcurrency.REPLACE` | Needs cross-instance cancel of RUNNING | Cross-instance cancel ships |
| Atomic group counter | Sweep is the correctness path | Fan-in evaluation cost measured as a problem (> `MAX_FAN_IN` raised) |
| Durable workflows (replay, timers, signals, compensation) | Locked decision | BACKLOG.md:112's trigger: `ORCH-1` shipped **plus** a consumer whose workflow must survive a crash mid-step |

## Done — this plan

- [ ] Conformance additions green on all four stores; no live `BUG: KI` row from this plan.
- [ ] Chains, groups, `cancel`, `dependency_results`, `get_many`, `JobStatus.BLOCKED` shipped with
      complete docstrings and DESIGN blocks.
- [ ] `ScheduleDriver` + `Schedule.concurrency` shipped; RO-6's three questions answered in the
      feature doc.
- [ ] Two-pod chain/group/driver integration tests green on SA, Redis (driver: in-memory repo),
      Beanie.
- [ ] A2A mapping fixed; every `JobStatus` switch audited.
- [ ] Docs, CHANGELOG, `3-to-4.md`, CLAUDE.md one-liners in the same change; `make lint` and
      `make test` green.

## Open questions (for the user, via the coordinator)

1. **Status name**: `BLOCKED` (chosen) vs. `WAITING` — visible in every job-status API response.
2. **Default `Schedule.concurrency`**: `ALLOW` (chosen — Celery-beat-like, byte-identical to today)
   vs. `FORBID` (APScheduler's `max_instances=1`-like, ⚠️ unverified) — `FORBID` is safer for slow
   jobs but changes behaviour for anyone who already materializes schedules.
3. **Retention coexistence**: documentation-only (chosen) vs. `RetentionModule` automatically going
   materialize-free when `JobDispatcherModule`'s driver is on (introduces the first module-to-module
   runtime coupling).
