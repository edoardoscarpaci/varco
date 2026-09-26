# Job scheduling, leases, and retention

Plan 005, Phase 4 (gaps U-17, U-11) + Phase 6 (U-18, U-19 — retention and
token-reference columns, same migration, different phase for the API).
Closes: "the job store has no time dimension (no delay/schedule, no bounded
retry), and `try_claim()` has no lease — a worker that stalls past the point
another runner reclaims its job can resume and silently overwrite the
second worker's result."

⚠️ Source correction: U-11's register entry says `try_claim` "accepts a TTL
and ignores it" — that TTL lives on a different class
(`SAAdvisoryLock.try_acquire(key, *, ttl)`). Pre-Phase-4 `try_claim(job_id)`
took **no** lease-related arguments at all. Everything below is an addition,
not the activation of a dormant parameter.

## Compatibility posture

Every new `Job` field and every new keyword-only parameter is defaulted so
an unchanged caller gets today's behaviour exactly:

- `run_at=None` claims immediately.
- `lease_ttl=None` (on `try_claim`/`claim_next`) takes no lease.
- `max_attempts=1` (the `Job` field default) fails terminally on the first
  failure, exactly like every job before this phase.
- `JobRunner(retry_policy=None, dlq=None)` reproduces today's exact
  terminal-FAILED-on-first-failure behaviour.
- `JobPoller(lease_aware=True)` is the one default that changes runtime
  behaviour for a store that has native lease support (see the pitfall table
  below) — the fallback to the old wall-clock age check only fires when the
  store raises `NotImplementedError` from `reap_expired_leases()`.

## `run_at` / `delay` — the time dimension (U-17 §1-2)

```python
from datetime import timedelta

# Schedule for a specific time
job = Job(run_at=tomorrow_9am)

# Or via the runner's enqueue() convenience — mutually exclusive with run_at
await runner.enqueue(job, coro, delay=timedelta(minutes=30))
```

`claim_next()`/`try_claim()` honour `run_at IS NULL OR run_at <= now` — a
PENDING job scheduled for the future is simply not eligible yet. The
predicate uses the **store's** notion of "now" (the database server's clock
on `varco_sa`, the poller's clock elsewhere) — don't assume worker-clock
precision for very tight schedules.

## Retry binding — `attempt` / `max_attempts` (U-17 §3)

```python
from varco_core.resilience import RetryPolicy
from varco_core.event.dlq import AbstractDeadLetterQueue

runner = JobRunner(
    store=store,
    retry_policy=RetryPolicy(max_attempts=5, base_delay=2.0),
    dlq=my_dlq,  # optional — see decision table below
)

job = Job(max_attempts=5)  # must opt in per-job too — 1 means terminal-on-first-failure
```

On a job coroutine failure, `JobRunner._handle_job_failure()`:

| Condition | Outcome |
|---|---|
| `retry_policy` set AND `attempt + 1 < max_attempts` | `Job.as_retry(run_at=now + retry_policy.compute_delay(attempt))` — back to PENDING for a later reclaim |
| Attempts exhausted, `dlq` wired | `Job.as_dead(error)` + `DeadLetterEntry(source=JOB, source_ref=str(job_id))` pushed |
| Attempts exhausted, no `dlq` | `Job.as_failed(error)` — today's exact terminal behaviour |

This reuses `varco_core.resilience.RetryPolicy` — the plan's explicit ask was
not to invent a second retry model for jobs.

## Leases and fencing (U-11)

A **lease** is a time-boxed claim: `try_claim(owner_id=..., lease_ttl=...)`
sets `owner_id`, `lease_expires_at = now + lease_ttl`, and increments
`lease_epoch` (the fencing token). The owner must `renew()` before the lease
expires to keep the job alive; a lease that is not renewed in time is
reclaimed by `reap_expired_leases()`, which bumps `lease_epoch` again —
fencing out the stalled owner.

```python
claimed = await store.try_claim(job_id, owner_id="worker-7", lease_ttl=30.0)
...
renewed = await store.renew(job_id, owner_id="worker-7", epoch=claimed.lease_epoch, lease_ttl=30.0)
if renewed is None:
    # Fenced out — another process reaped this job. Stop working on it.
    raise StaleLeaseError(...)
...
await store.save(claimed.as_completed(result), expected_epoch=claimed.lease_epoch)
# raises StaleLeaseError if the epoch moved on since the claim/last renew —
# the Kleppmann point: fencing happens at the point of WRITE, not merely at
# claim time. A worker that stalls past its lease and resumes must be
# rejected here, even if it never noticed it was fenced.
```

### TTL vs heartbeat guidance (stated verbatim, per U-11)

**TTL ≥ 3× heartbeat interval, plus 2× worst-case pause. Renew at
50–75% of remaining TTL, jittered.** A lease that expires before three missed
heartbeats false-positives on a GC pause or a slow network blip; a renewal
that always fires at exactly 90% of TTL synchronizes every worker's renewal
traffic. Example: heartbeat every 10s, worst-case GC/network pause 20s →
`lease_ttl ≥ 3×10 + 2×20 = 70s`; renew somewhere in the `[35s, 52.5s]` window
of a 70s lease, randomized per worker.

### `AbstractJobStore.renew()` / `reap_expired_leases()` raise, they don't degrade

Unlike `claim_next()` (which has a correct, if slower, portable default —
`list_by_status(PENDING)` + a `try_claim` loop), `renew()` and
`reap_expired_leases()` raise `NotImplementedError("<cls> does not support
leases")` by default. There is no correct fallback for a lease renewal or
expiry check — a silent no-op heartbeat is strictly worse than an error,
because it masks a lease that is about to (or already did) expire.

### `JobPoller(lease_aware=True)` — the default that changes behaviour

```python
poller = JobPoller(store=store, lease_aware=True)  # default
```

When the store supports leases, each poll tick runs **both** death signals,
over disjoint sets of RUNNING jobs:

1. `store.reap_expired_leases()` reaps a RUNNING job **holding a lease that
   expired** (`lease_expires_at <= now`) back to PENDING with a fenced
   `lease_epoch`. This fixes the regression U-11 §2 names: age is "correct
   for short jobs, wrong for anything whose legitimate duration can exceed
   the threshold" — a job with a **live** lease is never touched here no
   matter how old `started_at` is, however long the legitimate work takes.
2. The wall-clock age check (`stale_threshold`) still runs every tick and
   still owns RUNNING jobs with **no lease at all**
   (`lease_expires_at IS NULL`).

⚠️ **A RUNNING job with no lease at all is SKIPPED by lease reaping, not
reaped.** `reap_expired_leases()` reads `lease_expires_at <= now`; a `NULL`
lease is not an expired lease, it is the absence of the signal that method
reads (Plan 005 Step 48 scopes it to "RUNNING rows whose
`lease_expires_at <= now`" — NULL fails that comparison). Unleased jobs
remain governed by the age threshold exactly as they were pre-Phase-4. This
is deliberate: enabling `lease_aware=True` (the default) must not
retroactively reclassify unleased in-flight jobs, which is what keeps
leases opt-in — a deployment that never passes `lease_ttl` sees byte-
identical behaviour to before this phase.

When the store raises `NotImplementedError` from `reap_expired_leases()` (no
lease support — an external `AbstractJobStore` that predates this phase),
the poller skips the lease-reap step entirely and relies solely on
`stale_threshold` — the pre-Phase-4 behaviour, unchanged.

## Recurrence — expressible as re-enqueue, cron deliberately not shipped

There is no scheduler/cron primitive in this plan — U-17's own scoping
explicitly excludes it ("Recurrence/cron for jobs is explicitly not
requested and not built"). A recurring job is a self-scheduling one-shot:

```python
class SyncTask(VarcoTask):
    async def __call__(self, *, interval_seconds: float = 3600.0) -> None:
        await do_the_sync_work()
        # Re-enqueue myself for the next run instead of a cron entry.
        await runner.enqueue_task(
            self,
            interval_seconds=interval_seconds,
            run_at=None,  # or thread run_at through if you added it to enqueue_task
        )
```

This keeps the job store as the single source of truth for "what runs next"
— no second scheduling subsystem, no drift between a cron table and the job
table.

## Retention — `delete_where` (U-18, Phase 6)

```python
# Chunked sweep recipe — never enumerate the whole table under transaction
# pooling (PgBouncer pool_mode=transaction pins a connection for the whole
# sweep otherwise):
deleted = 1
while deleted:
    deleted = await store.delete_where(
        status=JobStatus.COMPLETED,
        completed_before=cutoff,
        limit=1000,
    )
```

`delete_where(status=None, completed_before=None, expires_before=None,
limit=None)` is concrete on the ABC (portable default over
`list_by_status` + `delete`) so external stores keep working; `SAJobStore`
overrides it as a single `DELETE ... WHERE ...`. Calling it with **no
predicate at all raises `ValueError`** — refusing to truncate the table by
accident is the point. `JobPoller(retention_sweep=True)` runs
`delete_where(expires_before=now, limit=...)` each tick; default `False` — no
deployment starts deleting rows on upgrade.

⚠️ **Beanie: `completed_before`/`expires_before` are evaluated at BSON
millisecond resolution, not Python microsecond resolution** — the same
`AbstractDeadLetterQueue.delete_where(older_than=)` issue described in
`dead-letter-queues.md`'s Retention section applies verbatim to
`BeanieJobStore.delete_where()`'s two exclusive-upper-bound predicates.
pymongo floors both the stored `completed_at`/`expires_at` and the query
operand, so a raw `$lt` cutoff could exclude a job stored in the cutoff's
own millisecond even though the store's own reported timestamp for it is
`< cutoff`. Fixed by widening only those two operands to the next whole
millisecond (`varco_beanie._bson_time.ceil_to_bson_millisecond`) before
querying. The `$lte` lease (`lease_expires_at`) and `run_at` predicates
elsewhere in `BeanieJobStore` are deliberately **not** adjusted — pymongo's
floor is already correct for an inclusive bound, and widening it would fire
a lease reap or a schedule up to 1ms early. `SAJobStore`/`InMemoryJobStore`
store full microsecond precision and need no such adjustment.

## Credential-at-rest — `store_raw_token=False` (U-19, Phase 6)

`request_token` is discouraged, not deprecated: a JWT is base64-encoded, not
encrypted, so any PII in its claims is readable at rest wherever the jobs
table lives (OWASP/NIST finding). `request_token` itself is **not** removed
or blanked by default — Source correction 4:
`varco_fastapi/varco_fastapi/job/runner.py` forwards `job.request_token` as
`Authorization: Bearer` on the completion callback, so flipping the default
would silently break callback auth for every existing deployment.

```python
job = Job(request_token=raw_jwt, store_raw_token=False)
# job.request_token is None; job.request_token_hash is sha256(raw_jwt).hexdigest()
```

Setting `store_raw_token=False` requires the completion callback to
authenticate with a **service credential** instead of replaying the caller's
token — which also removes a token-replay surface as a side benefit.

`store_raw_token` is threaded through every job-creation call site, all
defaulting `True`:

- `Job(..., store_raw_token=False)` — the field itself; `__post_init__`
  does the hash-and-clear transform.
- `JobRunner.enqueue_task(..., store_raw_token=False)` — forwarded straight
  into the `Job` it builds.
- `VarcoRouter._store_raw_token = False` — a router-level `ClassVar` read by
  `router.base._submit_job()` when it auto-populates the `Job` for
  async-offloaded (`?with_async=true`) CRUD/custom routes. Set it once on
  the router class rather than per-request.

```python
class OrderRouter(CRUDRouter[...]):
    _prefix = "/orders"
    _store_raw_token = False  # opt out for every async-offloaded route on this router
```

## Zoned schedules (Plan 011 / T2)

DST-safe wall-clock + IANA-zone scheduling for one-shot jobs, additive on
top of everything above — see `technical_docs/features/timezone-handling.md`
for the full policy design. Summary for this doc's audience:

`Job` gains three defaulted fields next to `run_at`:
`run_at_wall: datetime | None`, `run_at_tz: str | None`, `run_at_fold: int
= 0`. **`run_at` is materialized, not replaced** — it keeps its exact
current meaning as the UTC claim predicate; `(run_at_wall, run_at_tz,
run_at_fold)` is the *intent* it was computed from. A row with
`run_at_tz IS NULL` (every existing row) is byte-identical to today. No new
index — the claim predicate and `ix_varco_jobs_claim` are unchanged.

**RD-5 — a store must declare support.**
`AbstractJobStore.supports_zoned_schedules: ClassVar[bool] = False` on the
base; `SAJobStore`, `BeanieJobStore`, and the in-memory store in
`varco_fastapi.job.store` all set it `True` and persist all three
fields/columns. `AbstractJobRunner._prepare_zoned_job(job, store, ...)` is
the reusable guard + materialization step every concrete `enqueue()`
implementation is expected to call before `store.save()` — it raises
`ValueError` naming the store class when a zone is supplied to a store that
hasn't opted in. Two-line upgrade for a third-party store: add the three
columns/fields, then set `supports_zoned_schedules = True`.

**Wired into the shipped `JobRunner`** (Plan 011 drift-fix pass).
`varco_fastapi.job.runner.JobRunner.enqueue(job, coro, *, run_at=, delay=,
run_at_wall=, tz=, fold=, gap=, overlap=)` now calls
`AbstractJobRunner._prepare_zoned_job(job, self._store, ...)` before
`self._store.save(job)`, so the RD-5 guard runs on the standard submission
path — a zoned schedule (`run_at_wall=`/`tz=`) targeting a store that
hasn't declared `supports_zoned_schedules = True` raises `ValueError`
naming the store class, and `coro` is closed first so no coroutine is
leaked. `tz=None` (the default when the new kwargs are omitted) is a pure
passthrough — every pre-existing `enqueue(job, coro)` call site is
byte-identical to before. See
`technical_docs/features/timezone-handling.md`'s T2 section for the full
gap/overlap policy table.

**`ScheduleRematerializer`** (`varco_core.job.reschedule`) is the opt-in
recompute-on-read sweeper — pending zoned jobs inside a bounded horizon get
`run_at` recomputed from current tzdata and written back only when changed,
fenced with `save(expected_epoch=...)`. Default `interval=0.0` = never
started, byte-identical to not using the feature. Operator note: pin
`tzdata` in the sweeper's image for reproducible rematerialization
decisions across pods. **Adopt-then-upgrade ordering** applies here exactly
as it does to the framework-table Alembic branch above: if you're adding
T2's three columns via a fresh revision against an existing deployment,
land the column-adding revision first, deploy it everywhere, *then* start
using `run_at_wall=`/`run_at_tz=` — an old pod reading a new pod's zoned row
is safe either way (T2 has no two-step-deploy requirement, unlike Plan
010's cache envelope), but a revision that hasn't run yet obviously can't
persist the columns.

## Migration

**One** Alembic revision for the job table
(`xxxx_job_lease_schedule_retention`), covering every column both phases
need: `run_at`, `attempt`, `max_attempts`, `owner_id`, `lease_expires_at`,
`lease_epoch`, `expires_at`, `request_issuer`, `request_subject`,
`request_token_hash` — all nullable or server-defaulted (`lease_epoch` →
`0`, `attempt` → `0`, `max_attempts` → `1`). No backfill needed; every new
behaviour requires the caller to opt in. Three new indexes ride the same
revision: `ix_varco_jobs_claim (status, run_at, created_at)` — note there was
previously **no index at all** on `status`, so this is a free performance fix
— `ix_varco_jobs_lease (status, lease_expires_at)`, and
`ix_varco_jobs_expires (expires_at)` for the retention sweep. Build them
`CONCURRENTLY` on a live Postgres table.

⚠️ This repository does not ship an Alembic environment for its own
infrastructure tables — see `technical_docs/features/crypto-shredding.md`.
Generate the revision from `jobs_metadata` via `autogenerate`.

## Selecting a job store via env (Plan 047)

`VARCO_JOB_STORE_BACKEND=<redis|sa|beanie>` selects the live `AbstractJobStore` — all three are
*new* DI bindings as of Plan 047 (`RedisJobStore`/`SAJobStore`/`BeanieJobStore` previously
carried no binding at all). Unset means `InMemoryJobStore`, whose canonical home moved to
`varco_core.job.memory` (the old `varco_fastapi.job.store` import path still works, via a
re-export). See `technical_docs/features/dependency-injection.md`'s "Selecting a backend"
section.

## Pitfalls

| Pitfall | Fix |
|---|---|
| Long job killed at 5 minutes | Enable leases (`lease_ttl=`) so the poller judges liveness by lease expiry, not wall-clock age |
| Stalled worker resumes and overwrites a completed result | Pass `expected_epoch=` on every write; catch `StaleLeaseError` and abort |
| External `AbstractJobStore` subclass breaks after upgrading | Add the `owner_id`/`lease_ttl` kwargs to your `try_claim()` override — `renew()`/`reap_expired_leases()` are concrete but raise `NotImplementedError` until you override them |
| Retention sweep starves the connection pool | Chunk with `delete_where(..., limit=1000)` in a loop, not one unbounded call |
| Raw JWT readable in the jobs table at rest | `store_raw_token=False` + authenticate callbacks with a service credential |
| Long-running unleased job killed at `stale_threshold` although leases are enabled | Unleased RUNNING jobs are always governed by the age check, never by lease reaping — adopt `lease_ttl` on every claim to switch it to lease-based liveness, or raise `stale_threshold` |
| `enqueue(tz=...)` raises `ValueError` naming the store class | The target `AbstractJobStore` has not opted into zoned schedules (`supports_zoned_schedules = False`, the default) — this is the RD-5 guard working as designed, not a bug | Use a store that sets `supports_zoned_schedules = True` (`SAJobStore`, `BeanieJobStore`, the in-memory store), or add the three columns/fields to a custom store and opt in |
| `GapPolicy.SKIP` job never runs | By design — it transitions to a terminal state with `ScheduleGapError` rather than staying silently PENDING; not a bug |

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **Long job killed at 5 minutes** | A legitimately long-running job is marked FAILED mid-work | `JobPoller`'s old wall-clock `stale_threshold` doesn't know the job is still alive | Enable leases (`try_claim(lease_ttl=...)`) — `JobPoller(lease_aware=True)` (default) judges liveness by lease expiry, not age |
| **Stalled worker resumes and overwrites a completed result** | A worker that stalled past its lease window resumes and clobbers another worker's write | Writes were not fenced against the lease epoch | Pass `expected_epoch=` on every `store.save()` call; catch `StaleLeaseError` and abort |
| **External `AbstractJobStore` subclass breaks on `lease_ttl`** | `TypeError: unexpected keyword argument 'owner_id'` on `try_claim()` | Pre-Phase-4 overrides only declared `try_claim(self, job_id)` — the lease kwargs are additions, not activation of a dormant parameter | Add `owner_id`/`lease_ttl` to your `try_claim()` override before enabling leases |
| **`JobPoller` reaps a legitimately-running unleased job** | A RUNNING job with no lease is immediately returned to PENDING regardless of age | `lease_aware=True` (default) treats "no lease at all" as evidence of a failed claim, not as "healthy, just old" | Adopt `lease_ttl` on every claim, or construct `JobPoller(lease_aware=False)` to keep the old age-based check |
| **Retention sweep starves the pool** | A cleanup job/maintenance script pins a connection for minutes while deleting a huge backlog | `delete_where()` called once with no `limit` (or the caller manually enumerated ids one at a time) under a transaction-mode pooler | Loop bounded `delete_where(..., limit=1000)` calls (each its own short transaction) until it returns `0` — the chunked-sweep recipe on `AbstractJobStore.delete_where` |
| **Raw JWT readable in the jobs table** | An operator with read access to the jobs table/collection can read PII straight out of `request_token`'s claims | A JWT is base64-encoded, not encrypted — `store_raw_token=True` (default) stores it verbatim | Pass `store_raw_token=False` (`Job(...)`, `JobRunner.enqueue_task(...)`, or `VarcoRouter._store_raw_token = False`) and switch the completion callback to a service-credential/mTLS/signed-URL auth scheme instead of replaying the caller's token |
| **`enqueue(tz=...)` raises `ValueError` naming the store class** | A zoned-schedule `enqueue()` call fails at the store name instead of scheduling the job | Plan 011 / RD-5's `_prepare_zoned_job()` guard, now wired into the shipped `varco_fastapi.job.runner.JobRunner.enqueue()`, refuses a zoned schedule (`run_at_wall=`/`tz=`) targeting a store whose `supports_zoned_schedules` is `False` (the default) | Use a store that opts in (`SAJobStore`, `BeanieJobStore`, the in-memory store), or add the three columns/fields to a custom store and set `supports_zoned_schedules = True` |
| **Beanie `delete_where(completed_before=cutoff)` misses a job stored right at the cutoff** | A chunked purge sweep returns `0` while a job whose reported `completed_at`/`expires_at` is strictly before `cutoff` still matches | BSON is millisecond-precision and pymongo floors the query operand too — a raw `$lt` was evaluated as `stored_ms < floor_ms(cutoff)` | Fixed — `BeanieJobStore` widens `completed_before`/`expires_before` to the next whole millisecond (`_bson_time.ceil_to_bson_millisecond`) before querying; the `$lte` lease/`run_at` predicates are untouched (pymongo's floor is already correct for an inclusive bound) |
