# Plan 039 — Retention & purge automation (S20)

Covers BACKLOG 3.2-extension row **S20** (🟡 should, S–M — *a `RetentionPolicy` registry
materialized onto the shipped cron→Job path*), `BACKLOG.md:81`.

**No research brief backs this row.** It is composition of two shipped varco subsystems, so **the
repo is the evidence**. Every claim below about `varco_core.schedule`, `varco_core.job`, the CLI,
or any cleanup verb carries a `file:line` citation that was opened and read while writing this
plan. Anything that could not be verified from source is marked `⚠️ ASSUMPTION` in §Risks and
nowhere else.

⚠️ **Read §D-S20-exists first.** The row's premise is *half wrong in two directions*: one target
(job store) already has a shipped periodic sweep, one named target (outbox) has **no cleanup verb
at all**, and the "shipped cron→Job path" is missing **both** its driver and its executable
payload. The plan is shaped around those findings, not around the row's sentence.

## Scope and siblings

One of five plans covering the 3.2 extension rows `S17`, `S19`–`S23` (`BACKLOG.md:77-82`).

| Plan | Rows | Boundary with this plan |
|---|---|---|
| 038 | S19 — inbound webhook verification | ✅ **Already planned.** It reuses `AbstractIdempotencyStore.reserve/complete/release` as a replay cache and states — twice, at `plans/038-inbound-webhook-verification.md:54-55` and `:730` — that it builds **no** retention registry and that *"the replay store's `delete_expired()` is a candidate `RetentionPolicy` target once 039 ships"*. **This plan honours that exactly**: `IdempotencyRetentionTarget` (§D-S20-seam) is the adapter 038 deferred, it wraps the *store*, not 038's `WebhookReplayGuard`, and this plan touches **no** file under `varco_core/webhook/` or `varco_fastapi/webhook/` |
| 040 | S21 — unified redaction seam | ⚠️ **Real adjacency, one shared file.** 040 touches `varco_core/varco_core/service/audit.py` for **payload redaction** (what is written into `before`/`after` JSON). This plan touches the *same file* only to read `AuditRepository.delete_where()`'s signature (`audit.py:339-382`) — and in fact **adds nothing to it**; `AuditRetentionTarget` lives in `varco_core/retention/targets.py` and calls the shipped method. Two plans, one file, two unrelated concerns: **040 owns what goes in, 039 owns what goes out (by age).** Neither may change the other's half |
| 041 | S17 + S22 — `MetricsMiddleware` ordering, JWKS background refresh | ⚠️ **Real adjacency.** 041 adds a `varco_fastapi` lifespan component for the JWKS refresher; this plan adds `RetentionLifecycle`. **Neither plan may restructure `varco_fastapi/varco_fastapi/lifespan.py`'s registration model out from under the other.** Both add a component that follows the shipped `MigrationLifecycle`/`ReliabilityLifecycle`/`TenancyLifecycle`/`SecurityPostureLifecycle` pattern **as-is**: `startup()`/`shutdown()` plus `start()`/`stop()` aliases (`varco_fastapi/varco_fastapi/reliability.py:114-123`), appended to `lifespan_components` in `create_varco_app` exactly like reliability (`varco_fastapi/varco_fastapi/app.py:411-422`). This plan adds **no middleware** and edits **no** ordering table |
| 042 | S23 — conformance-guard recovery | Touches `testkit/` + CLAUDE.md only. This plan adds one new ABC (`RetentionTarget`) and therefore owes `testkit/varco_conformance/COVERAGE.md` a **note row** explaining why it gets no suite (§D-S20-conformance). **042 does not write it — this plan does.** ⚠️ Both plans edit `COVERAGE.md`; see Risks |

**Position in the build order:** independent of all four siblings. Its files are
`varco_core/retention/**` (new), `varco_core/schedule/{entity,materializer}.py`,
`varco_core/cli/retention.py`, `varco_fastapi/{retention.py,app.py,job/runner.py}`,
`varco_sa`/`varco_beanie` schedule models + one alembic revision. The only file any sibling also
edits is `COVERAGE.md` (042) and `app.py`'s keyword list (041 does not touch it — it adds a
lifespan component the app already accepts via `extra_lifespan_components`).

---

## Goal

An operator declares *"prune dead letters older than 30 days at 03:00 Europe/Rome"* once, as a
`RetentionPolicy`, and varco materializes it onto the shipped cron→`Job` path and runs it — with
the same DST-safe cron semantics, the same deterministic-`uuid5` cross-process convergence, and
the same `AbstractJobRunner` as every other job. Every shipped cleanup verb (DLQ, audit,
idempotency, revocation, job store) gets an adapter. **Nothing is scheduled and nothing is deleted
by default**, and a policy cannot be written that deletes everything.

## Non-goals

- **No second scheduler and no second runner.** ⛔ Standing rule, CLAUDE.md §Recurring schedules:
  *"`Schedule` … is materialized into ordinary `Job` rows by `ScheduleMaterializer` — **no second
  execution path**; the existing `AbstractJobRunner` runs the produced jobs unchanged."* Cron
  parsing stays `varco_core.schedule.cron`; occurrence computation stays
  `ScheduleMaterializer._compute_occurrences` (`materializer.py:140-192`); execution stays
  `AbstractJobRunner`. This plan writes **zero** lines of cron or claim logic.
- **No new abstract method on any shipped ABC.** Not on `AbstractDeadLetterQueue`, not on
  `AbstractIdempotencyStore`, not on `AbstractTokenRevocationStore`, not on `AuditRepository`, not
  on `AbstractJobStore`. Adapters only — the standing `BulkCache`-off-`AsyncCache` rule (Plan 011 /
  D-11) and the `RateLimiter.remaining()` precedent (Plan 035 §D-S10-headers).
- **No new runtime dependency in `varco_core`** (or anywhere). Stdlib + what is already imported.
- **No outbox retention.** `OutboxRepository` exposes `save`/`get_pending`/`delete(entry_id)` and
  nothing else (`varco_core/varco_core/service/outbox.py:62-64`, `:353`) — the row names "outbox
  pruning" as an existing verb and **it does not exist**. Adding one would be ABC surgery, and
  deleting outbox rows by age is *event loss by construction* (§D-S20-outbox).
- **No encryption-key retention.** `EncryptionKeyStore.delete(kid)` exists
  (`varco_core/varco_core/encryption_store.py:346`, `:471`) but destroying a DEK is
  crypto-shredding — a deliberate, irreversible operator act (CLAUDE.md §Field-level encryption).
  A scheduler must never do it.
- **No webhook-delivery retention.** `WebhookDelivery` has **no repository and no persistence** —
  it appears only in `varco_core/varco_core/webhook/models.py` and `settings.py` (verified: `rg
  WebhookDelivery varco_*/varco_*` matches those two files only). Nothing to purge.
- **No retention admin HTTP surface.** No `mount_retention_admin`. RD-9's "never a
  `create_varco_app` kwarg for an admin mount" cuts both ways: an unrequested destructive REST
  surface is worse than none. The CLI (§D-S20-cli) is the operator seam.
- **No cache/TTL management.** `AsyncCache` entries expire natively; retention is about *durable
  rows*.
- **No new `varco_core` top-level export.** `varco_core.retention` is imported from its submodules,
  same PEP 562 import-budget reasoning as `varco_core.schedule`
  (`varco_core/varco_core/schedule/__init__.py:10-16`).
- **No RRULE, no new cron features.** Parked exactly where Plan 032 left them.

---

## Design

### §D-S20-exists — what already exists, verified against source

Every row was opened and read.

| Fact | Location | Consequence for this plan |
|---|---|---|
| **`varco retention prune --type {dlq,audit} --before <ISO> [--limit] [--chunk] [--dry-run] --target module:factory` already ships** | `varco_core/varco_core/cli/retention.py:39-60` | ✅ One-shot manual pruning **is solved** for two subsystems. The gap is *scheduling* and *coverage*, not the sweep |
| Its sweep loops `delete_where(older_than=cutoff, limit=chunk)` until `0` | `cli/retention.py:96-100` | The chunked-sweep contract this plan reuses verbatim |
| Its `--dry-run` needs `count()` and prints the **whole-store total**, not the matching count | `cli/retention.py:85-92` | ⚠️ A weak preview. §D-S20-safety's `RetentionResult.would_delete` is the real one |
| Only two `--type` values exist; `--target` is a `module:callable` string | `cli/retention.py:49`, `:63-79` | The registry gives the CLI a third resolution mode (`--policy <name>`) with no new mechanism |
| **`JobPoller(retention_sweep=True, retention_batch_size=N)` already runs a periodic job-store sweep, off by default** | `varco_fastapi/varco_fastapi/job/poller.py:75-88`, `:114-115` | ✅ The job store is **already automated**. `JobRetentionTarget` exists for completeness but the docs must say "you probably already have this" |
| `AbstractDeadLetterQueue.delete_where(older_than=, source=, channel=, tenant_id=, limit=)` — concrete-but-raising; `ValueError` when **no** predicate | `varco_core/varco_core/event/dlq.py:450-504`, refusal at `:494-499` | Target #1. The no-predicate refusal is a floor this plan never bypasses |
| `AuditRepository.delete_where(older_than=, entity_type=, tenant_id=, limit=, allow_chain_break=False)`; `ValueError` on no predicate; **raises on a hash-chained table unless `allow_chain_break=True`** | `varco_core/varco_core/service/audit.py:339-382`, `:360-366`, `:376-381` | Target #2, and the hash-chain flag must be an explicit policy field, never auto-set |
| `AbstractIdempotencyStore.delete_expired() -> int` — **abstract**, takes **no** `older_than` (expiry is intrinsic); may legitimately return `0` on a native-TTL backend | `varco_core/varco_core/idempotency/base.py:201-220` | Target #3, and the reason `RetentionTarget.purge()` must tolerate `older_than=None` |
| `AbstractTokenRevocationStore.delete_expired() -> int` — **abstract**, no `older_than` | `varco_core/varco_core/revocation/base.py:172-183` | Target #4, same shape |
| `AbstractJobStore.delete_where(status=, completed_before=, expires_before=, limit=)` — concrete portable default; `ValueError` on no predicate; documents the chunked-sweep recipe | `varco_core/varco_core/job/base.py:880-1001`, refusal at `:965-971`, recipe at `:896-916` | Target #5 |
| **`OutboxRepository` has no bulk/age delete at all** — only `save`/`get_pending`/`delete(entry_id)` | `varco_core/varco_core/service/outbox.py:62-64`, `:353` | ⛔ The row's "outbox pruning" premise is **wrong**. §D-S20-outbox |
| **`WebhookDelivery` has no repository** (`rg WebhookDelivery varco_*/varco_*` → `models.py`, `settings.py` only) | verified absence | Not a target |
| `EncryptionKeyStore.delete(kid)` exists | `varco_core/varco_core/encryption_store.py:346`, `:471` | Deliberately **not** a target |
| `ScheduleMaterializer.materialize(schedule, now=)` → `list[Job]`, DST-safe, deterministic `uuid5` job id, per-schedule lazy `asyncio.Lock` | `varco_core/varco_core/schedule/materializer.py:99-138`, id at `:76-78`, lock at `:66-73` | The engine. Used **unchanged** |
| Its DESIGN block states the no-fenced-lease model and why: a synthetic lease row *"would corrupt every job-count invariant downstream code relies on"*; cross-process safety is the deterministic id + `save()` upsert + `UNIQUE(schedule_id, run_at)` | `materializer.py:13-42`, especially `:24-30` and `:31-37` | §D-S20-multiproc rests on this. **Never add a lease** |
| `_build_job()` sets `job_id`/`run_at`/zoned fields/`callback_url`/`metadata` — and **not `task_payload`** | `materializer.py:226-234` | ⛔ **Gap 1: a materialized job has no executable body** |
| `JobRunner.recover(registry)` re-submits PENDING jobs, filtering **`j.task_payload is not None`**, claiming with `try_claim()`, invoking with `registry.invoke(payload)` | `varco_fastapi/varco_fastapi/job/runner.py:522-556`, filter at `:524`, claim at `:533`, invoke at `:555` | Confirms Gap 1: a materialized job is invisible to the only dispatcher varco ships |
| **Nothing anywhere calls `ScheduleMaterializer.materialize()` outside tests** (`rg ScheduleMaterializer` → the class, its `__all__`, docstrings, `test_schedule_materializer.py`) | verified absence | ⛔ **Gap 2: Plan 032 shipped an engine with no driver** |
| **`recover()` ignores `run_at`** — it claims every PENDING task-payload job with `try_claim()`, never `claim_next()`, so a job scheduled for next week fires now | `runner.py:523-533` vs. `job/base.py:252-255` (*"earliest time this job is eligible to be claimed"*) | ⛔ **Gap 3: a shipped contract violation.** Phase 1 fixes it, test-first |
| `AbstractJobRunner.submit(job_id, coro)` — "schedule a coroutine under a pre-existing `job_id`"; loads, transitions PENDING→RUNNING | `varco_core/varco_core/job/base.py:1288-1319` | Considered and rejected as the dispatch seam (§D-S20-dispatch) — it does not claim, so two pods double-execute |
| `ScheduleRematerializer(store, *, interval=0.0, horizon=…)` — `interval=0.0` means `start()` **creates no task at all**; `_run_forever` catches and logs every sweep exception | `varco_core/varco_core/job/reschedule.py:50-88`, off-by-default at `:66-67` | The **exact in-repo shape** `RetentionScheduler` copies |
| `AbstractScheduleRepository` = `save`/`find_by_id`/`find_all_enabled`/`delete`, plus `InMemoryScheduleRepository` | `varco_core/varco_core/schedule/repository.py:32-76`, `:79-119` | The sweep's entry point; SA (`varco_sa/varco_sa/schedule.py`, migration `0007_schedules_table.py`) and Beanie (`varco_beanie/varco_beanie/schedule.py`) both ship |
| `tenant_context(tenant_id)` — a sync context manager setting the ambient tenant | `varco_core/varco_core/service/tenant.py:165`; `current_tenant()` at `:151` | §D-S20-tenancy's mechanism |
| `inspect_revocation_posture(registry=None, store=None, settings=None) -> RevocationPostureReport`, *"never raises"*, takes explicit objects — **no global registry** | `varco_core/varco_core/revocation/posture.py:72-126`, `:92` | The exact shape `inspect_retention_posture()` copies (§D-S20-posture) |
| `bind_trust_store(container, store)` registers an *already-constructed, already-owned* object with no lifecycle side effect | CLAUDE.md §TLS trust store | The verb precedent for `bind_retention_registry` (§D-S20-verb) |
| `ReliabilityLifecycle` = `startup()`/`shutdown()` + `start()`/`stop()` aliases, resolved from the container, `off()` preset ⇒ both no-ops | `varco_fastapi/varco_fastapi/reliability.py:59-123`, aliases at `:114-123` | The lifecycle shape (§D-S20-lifecycle) |
| `create_varco_app(..., reliability=None)` appends `ReliabilityLifecycle` only when non-`None`; `migrations`/`tenancy` prepend | `varco_fastapi/varco_fastapi/app.py:128`, `:407-422` | The keyword shape |
| `varco_core/varco_core/__init__.py` exports **no** `retention`/`schedule`/`idempotency`/`revocation` name (grep: zero matches) | verified absence | `api_surface.py --check` should stay clean (Step 27) |

**Net gap, precisely stated.** Of the six things the row implies are missing, one ships
(job-store sweep), one cannot exist (outbox), and the real work is: a **policy registry**, **five
adapters**, and **the three missing links in Plan 032's own subsystem** (a driver, a `task_payload`
on materialized jobs, and `recover()`'s `run_at` bug).

### §D-S20-shape — `RetentionPolicy` + `RetentionRegistry`, in `varco_core.retention`

```
varco_core/retention/
├── __init__.py     # re-exports only; ⛔ no @Singleton/@Provider/@Configuration (see below)
├── policy.py       # RetentionPolicy (frozen), RetentionRegistry, RetentionResult (frozen)
├── base.py         # RetentionTarget (ABC), CallableRetentionTarget
├── targets.py      # Dlq/Audit/Idempotency/Revocation/Job adapters
├── scheduler.py    # RetentionScheduler — materialize + dispatch, both shipped mechanisms
├── posture.py      # inspect_retention_posture() -> RetentionPostureReport
└── di.py           # bind_retention_registry(container, registry)
```

```python
@dataclass(frozen=True)
class RetentionPolicy:
    name: str                       # stable; the uuid5 seed AND the Job payload key
    target: RetentionTarget         # an object the app constructed — never a string
    cron_expr: str                  # parsed by varco_core.schedule.cron, unchanged
    timezone: str                   # IANA
    dry_run: bool                   # ⚠️ REQUIRED — no default (§D-S20-safety)
    older_than: timedelta | None = None
    enabled: bool = True
    batch_size: int = 1000
    max_batches: int = 100
    acknowledge_short_retention: bool = False
    tenant_ids: tuple[str, ...] | None = None   # §D-S20-tenancy
```

| ID | Choice | Consequence |
|---|---|---|
| D-S20-shape | A policy holds a **`RetentionTarget` object**, not a `module:callable` string. The *registry* maps `name → policy`; only the **name** (a `str`) is ever serialized into a `Job` | `varco_core` never imports a backend, and a `TaskPayload` stays JSON-safe (`job/task.py:97-101`) |

✅ **This is how a policy names a target without `varco_core` importing `varco_sa`/`varco_redis`.**
   The app builds `DlqRetentionTarget(dlq=my_redis_dlq, ...)` at wiring time and hands it in; the
   registry is an ordinary object the app owns. Identical to `bind_trust_store`'s
   already-constructed-object model.
✅ The `Job` payload is `{"policy": "<name>"}` — a string, so `TaskPayload`'s JSON constraint
   (`job/task.py:97-101`, `:47-49`) is met with nothing to serialize.
✅ `name` doubles as the `uuid5` seed for the policy's `Schedule.schedule_id`, which is what makes
   §D-S20-multiproc's convergence work across pods that never share memory.
❌ A policy is not declarable from pure configuration (a YAML file cannot name a target). Accepted:
   the same is true of every `bind_*` in the verb taxonomy, and a target needs a live repository
   handle anyway.
  Rejected — **`target: str` as a `module:callable`, resolved like the CLI's `_resolve`**
  (`cli/retention.py:63-79`): ❌ moves a startup wiring error to 03:00 in production; ❌ imports
  arbitrary modules inside a background task; ❌ the CLI does it because a CLI *has no container* —
  a running app does.
  Rejected — **a module-global `_REGISTRY` populated by a decorator**: ⛔ process-global mutable
  state, wrong under two apps in one process — the same reason Plan 035 §D-seam and Plan 038
  §D-S19-posture both rejected a global registry.

**Rule (test-enforced, Step 6): no module-level `@Singleton`/`@Provider`/`@Configuration` anywhere
under `varco_core.retention`.** `container.scan("varco_core", recursive=True)` is a documented,
in-use pattern that auto-activates both shapes; a scanned `@Configuration` here would start a
*deletion* loop in every app that scans `varco_core`. Same rule CLAUDE.md states for
`varco_core.tls` and `varco_core.event.cloudevents` — and here the blast radius is data loss, not
wire bytes.

### §D-S20-seam — a `RetentionTarget` ABC with one adapter per shipped verb

| ID | Choice | Consequence |
|---|---|---|
| D-S20-seam | `RetentionTarget` (ABC): `kind` property, `supports_older_than`/`supports_dry_run` `ClassVar[bool]`, `async def purge(*, older_than, limit, dry_run) -> RetentionOutcome`. Five in-tree adapters + `CallableRetentionTarget` as the escape hatch | Adapters wrap shipped verbs; **no shipped ABC gains a method** |

```python
class RetentionTarget(abc.ABC):
    supports_older_than: ClassVar[bool] = True
    supports_dry_run: ClassVar[bool] = True

    @property
    @abc.abstractmethod
    def kind(self) -> str: ...          # "dlq" | "audit" | "idempotency" | ...

    @abc.abstractmethod
    async def purge(self, *, older_than: datetime | None,
                    limit: int, dry_run: bool) -> RetentionOutcome: ...
```

| Adapter | Wraps | `supports_older_than` | `supports_dry_run` | Note |
|---|---|---|---|---|
| `DlqRetentionTarget` | `AbstractDeadLetterQueue.delete_where` (`dlq.py:450`) | ✅ | ✅ via `list_entries`/`count_by_channel` | **Requires `acknowledge_dead_letter_deletion=True`** (§D-S20-dlq) |
| `AuditRetentionTarget` | `AuditRepository.delete_where` (`audit.py:339`) | ✅ | ✅ via `list()` | `allow_chain_break` is an explicit ctor arg, forwarded (`audit.py:360-366`) |
| `IdempotencyRetentionTarget` | `AbstractIdempotencyStore.delete_expired` (`idempotency/base.py:202`) | ❌ | ❌ | Intrinsic expiry; may return `0` on a native-TTL backend |
| `RevocationRetentionTarget` | `AbstractTokenRevocationStore.delete_expired` (`revocation/base.py:173`) | ❌ | ❌ | Idem |
| `JobRetentionTarget` | `AbstractJobStore.delete_where` (`job/base.py:880`) | ✅ (`completed_before`/`expires_before`) | ✅ via `list_by_status` | ⚠️ Docs must say `JobPoller(retention_sweep=True)` may already cover this |
| `CallableRetentionTarget` | any `async (older_than, limit, dry_run) -> int` | declared by the caller | declared by the caller | The out-of-tree escape hatch |

✅ **Adapters, never ABC surgery.** Adding `purge()` to `AbstractDeadLetterQueue` would break every
   out-of-tree implementation — the standing rule that keeps `BulkCache` off `AsyncCache`
   (Plan 011 / D-11) and `remaining()` off `RateLimiter` (Plan 035 §D-S10-headers). Five wrappers
   cost ~30 lines each and cost an out-of-tree implementer nothing.
✅ **The two `ClassVar` capability flags are load-bearing, not decoration.** Two of the five verbs
   are `delete_expired()` with no cutoff and no preview. Without `supports_dry_run=False`, a
   `dry_run=True` policy pointed at an idempotency store would call `delete_expired()` and
   **actually delete** — a dry run that deletes is the worst possible bug in this plan. The
   scheduler **skips** such a target under `dry_run=True` and reports `skipped_reason="no dry-run
   support"` (asserted, Step 8). Likewise `supports_older_than=False` + a policy carrying
   `older_than` is a `ValueError` at **registry construction**, not a silently ignored field.
✅ One ABC gives the CLI, the posture inspector, and the metric a uniform `kind`/counts surface.
❌ A sixth in-tree target is a new class. Accepted — it is a ~30-line adapter, and
   `CallableRetentionTarget` covers the one-off case with zero new surface.
  Rejected — **a registry of plain `async` callables** (no ABC): ❌ no place to declare
  `supports_dry_run`, which is the safety-critical fact above; ❌ nothing to introspect for
  `inspect_retention_posture()`/the CLI's `list` verb; ❌ every call site re-invents the
  chunked-sweep loop. A callable is *available* as `CallableRetentionTarget`, so the simplicity is
  not lost — it is just not the default shape.
  Rejected — **`purge()` on the shipped ABCs**: ⛔ forbidden by the standing rule; see above.
  Rejected — **reusing `AbstractMigrator`'s contract shape**: ❌ unrelated lifecycle
  (plan/apply/lock), no age dimension.

### §D-S20-driver — closing Plan 032's three missing links (the honest part of this plan)

The row calls this *"composition of two shipped subsystems, not new mechanism"*. That is true of
the **cron and occurrence logic**, and false of the **wiring between them**: §D-S20-exists proves
`materialize()` has no caller, materialized jobs carry no `task_payload`, and `recover()` violates
`run_at`. Retention cannot ride a path that does not connect.

| ID | Choice | Consequence |
|---|---|---|
| D-S20-driver | Fix all three **inside the subsystems that own them**, additively: (1) `Schedule.task_name: str \| None = None` + `_build_job` emits a `TaskPayload` when it is set; (2) `RetentionScheduler` — a `ScheduleRematerializer`-shaped loop, `interval=0.0` ⇒ not started; (3) `JobRunner.recover()` honours `run_at` | Every schedule user benefits, not just retention. Default behaviour is byte-identical in all three |

**(1) `Schedule.task_name` + `Job.task_payload`.** `Schedule` gains one optional field
(`entity.py`, after `payload`); `_build_job` (`materializer.py:205-234`) gains:

```python
task_payload=(TaskPayload(task_name=schedule.task_name, kwargs=dict(schedule.payload))
              if schedule.task_name else None)
```

✅ `task_name=None` (the default) ⇒ `task_payload=None` ⇒ **byte-identical to today** for every
   existing `Schedule` row and every existing test (pinned by Step 10 before the change lands).
✅ It completes Plan 032 rather than working around it: without this, `varco_core.schedule` cannot
   express *what to run*, only *when* — and `Job.callback_url` (`materializer.py:232`) is a
   completion webhook, not an invocation.
✅ `TaskPayload` is frozen and JSON-safe (`job/task.py:88-120`); `schedule.payload` is already a
   plain `dict[str, Any]` (`entity.py:115`), so nothing new must be serializable.
❌ A new column on the shipped `schedules` framework table ⇒ `varco_sa/varco_sa/schedule.py`,
   `varco_beanie/varco_beanie/schedule.py`, and a new alembic revision `0008_schedule_task_name.py`
   (after `0007_schedules_table.py`). Nullable, no backfill, no data migration.
❌ `materializer.py` gains a module-scope import of `varco_core.job.task`. ⚠️ Cycle check is a
   **step**, not an assumption (Step 11) — `job.base` imports `job.task` only under `TYPE_CHECKING`
   (`job/base.py:77-79`), so a runtime import must be proven, and falls back to a function-body
   import if it is not.

**(2) `RetentionScheduler`.** A loop, and *only* a loop:

```
every `interval` seconds:
    for schedule in schedule_repo.find_all_enabled():        # repository.py:57-66
        if schedule.schedule_id not in registry:  continue   # never touches an app's own schedule
        jobs = await materializer.materialize(schedule)      # materializer.py:99 — UNCHANGED
        if jobs: await schedule_repo.save(replace(schedule, last_materialized_at=now))
    await runner.recover(task_registry)                      # runner.py:493 — UNCHANGED
```

✅ Copies `ScheduleRematerializer` exactly: `interval: float = 0.0` ⇒ `start()` creates **no task**
   (`reschedule.py:66-67`); `_run_forever` logs and continues on any exception
   (`reschedule.py:82-88`); `asyncio.Task` created inside `start()`, never `__init__`.
✅ It **computes nothing**. No cron evaluation, no occurrence maths, no claim logic — every
   decision is delegated to a shipped method. That is what keeps "no second scheduler" literally
   true, not just rhetorically.
✅ It skips schedules whose `schedule_id` is not in this registry, so an app that also uses
   `varco_core.schedule` for its own work is untouched.
❌ It runs on every pod. Correct and intended — see §D-S20-multiproc.
  Rejected — **put the loop in `varco_core.schedule.sweeper` as a general driver**: ✅ arguably its
  right home, ❌ but a general schedule driver must decide catch-up/backpressure/ownership
  questions for *arbitrary* app schedules that this plan has no mandate to settle. Filed as a
  BACKLOG row instead (§BACKLOG entries) — `RetentionScheduler` is deliberately the *narrow* one.

**(3) `recover()` honours `run_at`.** One filter at `runner.py:524`:
`and (j.run_at is None or j.run_at <= now)`.

✅ This is a **bug fix against the shipped contract**, not a new behaviour: `Job.run_at` is
   documented as *"earliest time this job is eligible to be claimed"* (`job/base.py:252-255`) and
   `claim_next()`'s default implementation enforces exactly this predicate
   (`job/base.py:872-874`). `recover()` uses `try_claim` and skips it.
✅ Without it, §D-S20-driver's dispatch step would fire *future* occurrences immediately — the one
   way this plan could break an unrelated app.
❌ An app that (knowingly or not) relies on `recover()` firing future-dated jobs at startup changes
   behaviour. Accepted: that behaviour contradicts the field's own docstring, and Step 2 pins both
   directions.

### §D-S20-dispatch — how a materialized `Job` reaches the purge handler

| ID | Choice | Consequence |
|---|---|---|
| D-S20-dispatch | `TaskPayload(task_name="varco.retention.purge", kwargs={"policy": name})` → `TaskRegistry` → `JobRunner.recover()`'s `try_claim` + `registry.invoke()`. The handler is a **bound method** on a `RetentionRunner` the app owns, registered as `registry.register(VarcoTask(runner.purge_policy, name="varco.retention.purge"))` | Zero new dispatch mechanism; zero global state |

Mechanism, with evidence: `TaskRegistry.register(task)` (`job/task.py:468-482`), `.get(name)`
(`:484-497`), `.invoke(payload)` (`:499-…`); `JobRunner.recover()` filters `task_payload is not
None` (`runner.py:524`), claims atomically with `try_claim()` (`:533`), looks the name up
(`:543`), and invokes (`:555`).

✅ **A bound method, not a module-level `@varco_task`.** A module-level task function would need a
   module-global registry to find the policy — the state §D-S20-shape rejected. A `VarcoTask`
   wrapping `runner.purge_policy` closes over the app's own `RetentionRegistry`, so two apps in one
   process are independent, and a test constructs its own.
✅ The payload carries **only a policy name**, so `TaskPayload`'s JSON constraint (`job/task.py:47-49`)
   holds and a `Job` row never contains a repository handle, a cutoff, or a secret.
✅ `try_claim()` (`runner.py:533`) means exactly one pod executes each occurrence — the distributed
   guarantee is the shipped one.
❌ A policy renamed between materialization and execution orphans an in-flight job:
   `registry.get()` returns `None`, `recover()` logs a warning and leaves it **RUNNING forever**
   (`runner.py:544-552`). Documented as a Pitfall; the handler additionally raises a named
   `RetentionPolicyNotFoundError` if the policy vanished after invocation.
❌ Requires `varco_fastapi`'s `JobRunner` (or another `AbstractJobRunner` implementing `recover`).
   Accepted and stated: an app with no job runner uses the CLI (§D-S20-cli).
  Rejected — **`AbstractJobRunner.submit(job_id, coro)`** (`job/base.py:1288-1319`): ❌ it does not
  claim, so two pods that both materialized the same occurrence would both execute it — precisely
  the double-delete this plan must not enable.
  Rejected — **`enqueue_task()`** (`job/base.py:1354-1402`): ❌ it creates its **own** `Job` row, so
  the materializer's deterministic-`uuid5` convergence (`materializer.py:76-78`) would be bypassed
  and every pod would enqueue a duplicate.
  Rejected — **a new `JobDispatcher` poll loop** over `claim_next()`: ✅ arguably what varco is
  missing generally, ⛔ but it is a second execution path for jobs and belongs to Plan 005's
  subsystem, not to a retention row. BACKLOG row instead.

### §D-S20-safety — nothing stops `retention=0 days` today; four things will

⚠️ **This is the row's central risk and the reason it is the most dangerous in the extension set.**
The 3.2 cycle's locked *"split by blast radius"* decision (`BACKLOG.md:62`) is applied here as:
the **declaring** half may be on once configured; the **deleting** half needs a separate, explicit,
greppable act.

| # | Guard | Where | Fails |
|---|---|---|---|
| 1 | `dry_run: bool` is a **required field with no default** | `RetentionPolicy` | At construction — `TypeError` from the dataclass |
| 2 | `older_than <= 0` ⇒ `ValueError`, **no acknowledgement escape** | `RetentionPolicy.__post_init__` | At construction |
| 3 | `older_than < 24 h` ⇒ `ValueError` unless `acknowledge_short_retention=True` | `RetentionPolicy.__post_init__` | At construction |
| 4 | `older_than` set on a `supports_older_than=False` target, or **absent** on a `supports_older_than=True` target ⇒ `ValueError` | `RetentionRegistry.register()` | At wiring |
| 5 | `dry_run=True` on a `supports_dry_run=False` target ⇒ **skip, report, delete nothing** | `RetentionScheduler` | At run, loudly, non-destructively |
| 6 | `batch_size` + `max_batches` bound one occurrence's blast radius | `RetentionScheduler` | Always — the sweep stops and reports `truncated=True` |
| 7 | The underlying verb's own no-predicate `ValueError` is never bypassed | `dlq.py:494`, `audit.py:376`, `job/base.py:965` | At the backend |
| 8 | A DLQ target needs `acknowledge_dead_letter_deletion=True`; an audit target on a hash-chained table needs `allow_chain_break=True` | ctor args (§D-S20-dlq) | At construction |

✅ **`dry_run` has no default at all — deliberately, over `dry_run=True`.** A default of `True`
   would be safe but invites the "everyone flips it in the first hour" failure, and would leave a
   4.0 flip question hanging. A required field makes every policy in every repo state its own blast
   radius **in the source, greppable**, and can never silently change meaning across a varco
   version. The `import_budget --warn-only` precedent (CLAUDE.md) is the *right* shape for a
   measurement that will become a gate; this is not that — it will never become a gate, so
   "required forever" beats "default that flips".
✅ Guards 2–4 fire at **construction/wiring**, i.e. in the app's own startup test, not at 03:00.
✅ Guard 5 is the non-obvious one and the reason `supports_dry_run` exists at all (§D-S20-seam).
✅ Guard 6 makes a misconfigured window survivable: a policy that would match ten million rows
   deletes at most `batch_size × max_batches` per occurrence and reports the truncation, instead of
   pinning a pooled connection for an unbounded `DELETE` — the exact rationale `AbstractJobStore`
   already documents (`job/base.py:896-916`).
❌ Eight guards is a lot of ceremony for "delete old rows". Accepted, and it is the point: this is
   the only feature in the 3.2 set whose failure mode is *irreversible data loss*.
❌ `acknowledge_short_retention` could become boilerplate. Mitigated: it is greppable
   (`rg acknowledge_short_retention`), it is in the Pitfalls table, and the ⚠️ un-park trigger for
   revisiting is "it appears in more than one policy in a consumer's repo".
  Rejected — **a warn-only first run that deletes on the second**: ❌ non-deterministic, untestable,
  and "it worked in staging" would mean nothing.
  Rejected — **a global `VARCO_RETENTION_ENABLED` kill switch**: ❌ an env var that turns deletion
  on is exactly the shape CLAUDE.md forbids for `mount_tenant_admin` (*"there is deliberately no
  `VARCO_TENANCY_MOUNT_ADMIN` env var, ever"*) — a destructive capability must be enabled in code,
  in review.

### §D-S20-dlq — dead letters need a second, named acknowledgement

CLAUDE.md, standing: *"Dead letters must never be silently deleted (no TTL index by default)."*

| ID | Choice | Consequence |
|---|---|---|
| D-S20-dlq | `DlqRetentionTarget(dlq=..., acknowledge_dead_letter_deletion=True, ...)` — `ValueError` naming the rule otherwise. No default, no env var | A DLQ retention policy cannot exist by accident |

✅ Same "explicit acknowledgement kwarg for a footgun we will not remove" shape as
   `mount_*(acknowledge_bundled_admin=True)` and
   `RateLimitMiddleware(acknowledge_unbounded_keyspace=True)` (Plan 035 §D-S10-keyspace).
✅ A DLQ entry is *the only remaining copy* of an event that already failed every retry. Deleting
   it is not housekeeping.
❌ Redundant with guard 1 (`dry_run`) at first glance. Not redundant: `dry_run=False` says "I mean
   to delete"; this says "I mean to delete **dead letters**". They are separate decisions.
  Rejected — **excluding the DLQ from the registry entirely**: ❌ it is the row's first named
  target and the most common real request (a DLQ grows forever); refusing it would push operators
  straight back to hand-written jobs.

### §D-S20-outbox — outbox is excluded, and that is a finding, not an omission

| ID | Choice | Consequence |
|---|---|---|
| D-S20-outbox | No `OutboxRetentionTarget`. A Non-goal line, a Pitfalls row, and a BACKLOG row correcting the premise | No ABC surgery, no event loss |

✅ `OutboxRepository` exposes no bulk/age delete (`outbox.py:62-64`, `:353`) — adding one is a new
   abstract method on a shipped ABC, forbidden.
✅ **Semantics forbid it even if the verb existed.** `OutboxRelay` deletes an entry *on successful
   publish* (`outbox.py:39`). An entry that is still there is one that has **not** been published —
   deleting it by age is silently dropping the event the outbox pattern exists to guarantee. That
   is a DLQ concern (which `OutboxRelay` already has, via `dlq=`), not a retention concern.
❌ An operator with a stuck outbox has no scheduled remedy. Correct: the remedy is the DLQ +
   `DlqRedriver`, not deletion.

### §D-S20-tenancy — a scheduled purge has no ambient tenant; say so and scope explicitly

⚠️ `current_tenant()` (`service/tenant.py:151`) is the single source of truth for *who the tenant
is* — and inside a background job there is **no request**, so it returns `None`. A purge that
reads `current_tenant()` and finds `None` would silently become a **cross-tenant delete**.

| ID | Choice | Consequence |
|---|---|---|
| D-S20-tenancy | `RetentionPolicy.tenant_ids: tuple[str, ...] \| None`. `None` = an explicit, documented **platform-wide** purge (`tenant_id=None` forwarded to the verb). A non-empty tuple = the scheduler loops the tenants, entering `tenant_context(tid)` (`service/tenant.py:165`) and forwarding `tenant_id=tid` to the verb. `()` (empty tuple) ⇒ `ValueError` | The ambiguity is removed at the type level: there is no state that means "whatever tenant happens to be ambient" |

✅ The three targets that accept a tenant already do (`dlq.py:456`, `audit.py:344`); this forwards
   an *explicit* value rather than relying on ambient state that cannot exist here.
✅ `tenant_context(tid)` is entered **as well**, so anything downstream that reads
   `current_tenant()` (RLS session hooks, `tenancy_cache_key()`) sees the right value — composition
   by the shipped mechanism, no new one.
✅ `None` meaning "platform-wide" is honest and greppable, and the posture inspector reports it
   (§D-S20-posture) so an operator can see which policies cross tenants.
❌ A fleet of 5 000 tenants makes a per-tenant policy a 5 000-iteration loop. Accepted for this
   plan and named: `TenantFanoutSupervisor` (`varco_core/varco_core/tenancy/fanout.py:36-44`) is
   the shipped fan-out primitive and wiring retention through it is a **parked** follow-up, not a
   silent limitation.
  Rejected — **read `current_tenant()` inside the purge**: ⛔ it is always `None` in a background
  job, so this is a cross-tenant delete wearing a tenancy-shaped costume.
  Rejected — **one `Schedule` per tenant per policy**: ❌ multiplies framework-table rows by the
  tenant count and makes the `uuid5` seed tenant-dependent, for no isolation gain over the loop.

### §D-S20-obs — counts, a log line, an opt-in metric; no event, no audit row

| ID | Choice | Consequence |
|---|---|---|
| D-S20-obs | `RetentionOutcome`/`RetentionResult` (frozen) carry `policy`, `kind`, `examined`, `deleted`, `would_delete`, `batches`, `truncated`, `dry_run`, `skipped_reason`, `duration_s`, `error`. One INFO log per occurrence. `install_retention_metrics()` — **opt-in**, `install_*` shape (a) from the verb taxonomy. ⛔ No event published. ⛔ No audit-trail row | Observable without new coupling |

✅ **`would_delete` is the real preview** the CLI's `--dry-run` never had (it prints the store's
   whole-store `count()`, `cli/retention.py:87-91`). `dry_run=True` populates `would_delete` and
   leaves `deleted=0` — that is "what would be deleted before it is".
✅ ⛔ **No event.** Publishing would require an `AbstractEventBus`/`AbstractEventProducer` inside
   `varco_core.retention`, and the standing rule is that only `OutboxRelay`,
   `EventConsumer.register_to()` and `DlqRedriver` hold a bus. A retention sweeper is not on that
   list and does not deserve to be.
✅ ⛔ **No audit row.** A purge that prunes the audit log would write audit rows about pruning audit
   rows — an unbounded feedback loop, and `AuditConsumer` is event-driven, which reintroduces the
   bus. Stated explicitly so nobody "fixes" it later.
✅ `install_retention_metrics()` matches taxonomy shape (a): a process-global side effect taking
   no container, exactly like `install_cache_metrics`/`install_reliability_metrics`.
❌ Without the opt-in metric an operator sees only logs. Accepted — identical to reliability
   metrics' posture today.

### §D-S20-multiproc — the materializer's `uuid5` convergence already covers two pods

⚠️ **Confirmed against source, not assumed.** `materializer.py:24-30` states the model and
`:31-37`/`:38-42` state its limits: the occurrence `Job.job_id` is `uuid5(NAMESPACE_URL,
f"varco:schedule:{schedule_id}:{wall.isoformat()}")` (`materializer.py:76-78`); two materializers
computing the same occurrence produce the **same physical row** via `AbstractJobStore.save()`'s
documented upsert semantics (`job/base.py:682-687`, *"if a job with the same `job_id` exists, it is
replaced"*), reinforced by `UNIQUE(schedule_id, run_at)`; `materialize()` additionally skips an
occurrence whose `job_id` already exists (`materializer.py:130-135`). The per-schedule
`asyncio.Lock` (`:66-73`, `:125`) is explicitly documented as **in-process only** (`:38-42`).

| ID | Choice | Consequence |
|---|---|---|
| D-S20-multiproc | `RetentionScheduler` runs on **every** pod, adds **no lease**, and relies on (a) the `uuid5` + upsert convergence for *materialization* and (b) `recover()`'s `try_claim()` (`runner.py:533`) for *execution* | Exactly-once purge per occurrence, with zero new coordination |

✅ Both halves are shipped guarantees, cited above. The plan adds nothing to either.
✅ ⛔ **Never add a lease to `Schedule`.** `materializer.py:19-23`: a synthetic lease row *"is
   indistinguishable from a real job to every caller of `list_by_status()`/`delete_where()`"* — and
   `delete_where()` is precisely what `JobRetentionTarget` calls, so a lease row would be
   *deleted by this plan's own feature*. CLAUDE.md's §Recurring schedules rule, made concrete.
❌ `schedule_repo.save(last_materialized_at=...)` is last-write-wins across pods. Accepted: it is
   a `CatchUpPolicy` hint (`entity.py:83-88`), not a correctness input — the occurrence id is.
❌ Two pods redundantly call `find_all_enabled()` + `recover()`. Accepted: cheap, and idempotent by
   construction.
  ⚠️ **ASSUMPTION** (Risks): the `UNIQUE(schedule_id, run_at)` index named at `materializer.py:29`
  was not re-verified in `varco_sa/varco_sa/migrations/versions/0007_schedules_table.py`. Step 12
  verifies it and, if absent, files a BACKLOG row rather than adding one here (an index on a
  shipped framework table is Plan 032's business).

### §D-S20-verb — `bind_retention_registry(container, registry)`

Against the taxonomy table (CLAUDE.md §DI wiring verb taxonomy):

| Candidate | Verdict |
|---|---|
| `bootstrap()` | ❌ one per *package*, wraps `container.scan(pkg)`. `varco_core.retention` must never be scanned into life (§D-S20-shape) |
| `enable_*` | ❌ "flips on an opt-in binding that would **shadow an app default**". There is no default `RetentionRegistry` to shadow — nothing is registered at all |
| `mount_*` | ❌ an ASGI privileged surface; there is none (§Non-goals) |
| `install_*` | Used for `install_retention_metrics()` only — shape (a), process-global side effect, no container (§D-S20-obs) |
| **`bind_*`** | ✅ *"registers N typed bindings unknowable before app startup"* — the registry holds live repository handles known only at wiring time. Exact precedent: `varco_core.tls.bind_trust_store(container, store)`, which CLAUDE.md justifies as registering an *already-constructed, already-owned* object with **no lifecycle side effect** — the same reason this cannot be a scanned `@Configuration` |

### §D-S20-lifecycle — `RetentionLifecycle`, following the shipped pattern **as-is**

| ID | Choice | Consequence |
|---|---|---|
| D-S20-lifecycle | `varco_fastapi/varco_fastapi/retention.py` — `RetentionLifecycle(registry, *, container, interval=0.0)` with `startup()`/`shutdown()` **and** `start()`/`stop()` aliases; `create_varco_app(retention=None)` **appends** it when non-`None`, exactly like `reliability` | One more component of an established shape |

✅ Byte-for-byte the `ReliabilityLifecycle` shape: aliases at `reliability.py:114-123` with the
   comment explaining why they exist; container resolution with a `LookupError` that names the
   missing interface (`reliability.py:125-139`); `create_varco_app` append at `app.py:411-422`.
✅ **Appended, not prepended** — like reliability, and for the same reason: it resolves an
   `AbstractJobRunner`/`AbstractJobStore`/`AbstractScheduleRepository` that earlier components
   create. Migrations and tenancy prepend (`app.py:375-378`, `:394-395`); this must not.
✅ ⛔ **`varco_fastapi/varco_fastapi/lifespan.py` is not modified by this plan.** Neither is Plan
   041's JWKS refresher allowed to modify it. Both components satisfy the existing
   `AbstractLifecycle` protocol (`lifespan.py:73-89`) and are registered through the existing
   `register()` (`lifespan.py:162-183`). **Stated here so a rebase conflict is a surprise, not a
   silent semantic change.** DoD item 6 makes it checkable.
✅ `retention=None` (the default) registers nothing and `interval=0.0` starts nothing — a default
   `create_varco_app()` schedules nothing and deletes nothing, which is load-bearing.
❌ A second way to start the scheduler (lifecycle vs. constructing it yourself). Accepted: the
   lifecycle is a thin wrapper; the class stays usable standalone, as `OutboxRelay` is.
  Rejected — **auto-starting from `bind_retention_registry`**: ⛔ a DI binding that starts a
  deletion loop is exactly the `varco_core.tls` scanned-`@Configuration` failure mode, with data
  loss instead of a stray file watcher.

### §D-S20-cli — extend the shipped `varco retention`, do not replace it

| ID | Choice | Consequence |
|---|---|---|
| D-S20-cli | Add `--policy <name>` as a **third** resolution mode alongside `--type`/`--target` (`cli/retention.py:49-60`), plus `varco retention list --target module:factory` printing the registry (name, kind, cron, tz, window, dry_run, tenants). `--type`/`--target` behaviour is **unchanged** | The existing invocations in `dead-letter-queues.md:352-355` and `database-auditing.md:270-274` keep working verbatim |

✅ Designed *with* the shipped CLI, not around it: `_resolve()` (`cli/retention.py:63-79`) resolves
   a `RetentionRegistry` factory exactly as it already resolves a DLQ/audit factory.
✅ `--policy` executes one policy **once, now**, honouring its `dry_run`/floors/acknowledgements —
   so the operator's manual run and the scheduled run take the identical code path. No third
   behaviour to reason about.
✅ Gives an app with **no job runner** a complete story (a Kubernetes `CronJob` calling
   `varco retention prune --policy nightly-dlq`), which is also the fallback if §D-S20-dispatch's
   runner requirement is not met.
❌ Three resolution modes on one subcommand. Mitigated: mutually exclusive, `argparse`-enforced,
   one `--help` line each.

### §D-S20-posture — **yes**, `inspect_retention_posture()` — and why this differs from 038

Plan 038 argued *against* an inspector for its off-by-default path (`plans/038-…:409-433`), on
three grounds. Each is checked here rather than assumed:

| 038's argument | Does it apply to retention? |
|---|---|
| *"an inbound verifier is a per-route `Depends(...)` with no process-global registry to read — inspecting it would require **building** one"* | ❌ **No.** A `RetentionRegistry` is an explicit, app-constructed, DI-bound object. The inspector reads what already exists — like `inspect_revocation_posture(registry=…)` (`revocation/posture.py:72-76`), which takes its objects as arguments and never raises (`:92`) |
| *"an app with no inbound webhook route would get a permanent, meaningless finding"* | ⚠️ **Partly.** Resolved the same way revocation does: `configured=False` is reported as a **fact**, never a finding, so an app without retention gets no noise |
| *"the question is already answered at construction, loudly, by a `ValueError`"* | ❌ **No.** The `ValueError`s here cover *malformed* policies (§D-S20-safety). They cannot answer *"is any policy actually deleting?"*, *"which policies are platform-wide?"*, or *"has this policy been dry-running since March?"* — which is exactly what an operator needs |

| ID | Choice | Consequence |
|---|---|---|
| D-S20-posture | `varco_core/retention/posture.py`: `inspect_retention_posture(registry=None) -> RetentionPostureReport` — a pure read, never raises, no globals. Fields: `configured`, `policy_count`, `destructive_count` (`dry_run=False`), `dry_run_count`, `platform_wide_policies` (`tenant_ids is None`), `dlq_policies`, `short_retention_policies`, `kinds`. **Plan 036 owns the harness**; this plan exports only the inspector | Consistent with the house pattern, no new cross-plan seam |

✅ House-consistent: `inspect_http_edge()`/`inspect_rls_posture()`/`inspect_revocation_posture()`.
✅ `platform_wide_policies` is the genuinely security-relevant finding (§D-S20-tenancy) — a
   cross-tenant delete an operator did not realise they configured.
❌ Plan 036's `SecurityPosture` must eventually call it. Out of scope here; the pure function is
   the whole deliverable, and a BACKLOG row records the wiring.

### §D-S20-conformance — no testkit suite; a `COVERAGE.md` note row

`RetentionTarget` is a new ABC. `testkit/varco_conformance` covers eight `varco_core` ABCs
(CLAUDE.md §Test Conventions).

| ID | Choice | Consequence |
|---|---|---|
| D-S20-conformance | **No** new `testkit/varco_conformance` module. Instead: one parametrized table in `varco_core/tests/test_retention_targets.py` running the same contract assertions across all six in-tree targets, plus a **note row in `COVERAGE.md`** stating why and the un-park trigger | The house rule (*"a new implementation of one of the ABCs either subclasses its suite or gets a row in `COVERAGE.md` explaining why not"*) is satisfied |

✅ All six implementations are in-tree, thin, and Docker-free against in-memory backends.
✅ `testkit` is never packaged, so a suite cannot reach an out-of-tree implementer anyway (the
   argument Plan 016 §RL-3d used to decline re-exporting providify's fixtures, and Plan 038
   §D-S19-conformance reused).
❌ A future out-of-tree `RetentionTarget` gets no shared contract test. Un-park trigger: **the
   first out-of-tree target, or a seventh in-tree one.**

### Alternatives considered (plan-level)

- **CLI-only: extend `varco retention prune` with `--policy` and stop there** (operator schedules
  via `cron`/Kubernetes `CronJob`). ✅ Zero new execution surface, zero risk of a runaway loop, no
  need to touch Plan 032's or Plan 005's gaps. ❌ It is *not the row*: "nothing schedules any of
  them" stays true, and every operator still hand-writes the CronJob per subsystem — the wiring
  varco exists to own. **Rejected as the whole answer, adopted as a component** (§D-S20-cli), and
  it remains the documented fallback for an app with no job runner.
- **A dedicated `RetentionSweeper` that evaluates cron itself on a timer.** ⛔ A second scheduler.
  Forbidden by CLAUDE.md's standing rule and by every line of `materializer.py`'s DESIGN block.
- **`APScheduler`/`croniter` as a dependency.** ❌ A new `varco_core` runtime dependency for logic
  varco already ships zero-dependency (`varco_core/varco_core/schedule/cron.py`), and an
  import-budget cost for nothing.
- **A `retention_days` field on each ABC's settings object, swept by each subsystem itself.** ❌
  Five independent sweepers, five sets of safety guards to get right, no shared preview/metric/
  posture, and it is the per-subsystem hand-wiring the row exists to remove.
- **Put the registry in `varco_fastapi`.** ❌ Inverted seam: retention is transport-agnostic (a CLI,
  a worker, a Lambda). Same rule as `varco_core.tls` vs. `varco_fastapi.auth`.

---

## Steps

### Phase 1 — fix `recover()`'s `run_at` contract violation (🔴 must, S)

1. [x] `varco_fastapi/tests/milestone_e/test_task_runner.py` (extend, **failing first**) — a PENDING
       job with `task_payload` and `run_at = now + 1h` is **not** claimed by `recover()`; the same
       job with `run_at = now - 1s` **is**; `run_at=None` is claimed (today's behaviour, pinned);
       the returned count reflects only claimed jobs.
2. [x] `varco_fastapi/varco_fastapi/job/runner.py:523-524` — add the `run_at` predicate to the
       `recoverable` filter, with a comment citing `job/base.py:252-255` and `claim_next()`'s own
       predicate (`job/base.py:872-874`). No signature change.

⛔ **CHECKPOINT** — `uv run pytest varco_fastapi/tests/milestone_e/test_task_runner.py -q`

### Phase 2 — the core seam: policy, registry, target ABC (🟡 S–M)

3. [x] `varco_core/tests/test_retention_policy.py` (new, **failing first**) — the eight guards of
       §D-S20-safety: missing `dry_run` ⇒ `TypeError`; `older_than=timedelta(0)` and a negative
       value ⇒ `ValueError` **with no acknowledgement escape**; `older_than=timedelta(hours=1)` ⇒
       `ValueError` naming `acknowledge_short_retention`, and accepted with it set;
       `older_than` on a `supports_older_than=False` target ⇒ `ValueError` at `register()`;
       `older_than=None` on a `supports_older_than=True` target ⇒ `ValueError`; `tenant_ids=()` ⇒
       `ValueError`; duplicate `name` in one registry ⇒ `ValueError`; the policy is frozen.
4. [x] `varco_core/varco_core/retention/policy.py` (new) — `RetentionPolicy`
       (`@dataclass(frozen=True)`, `__post_init__` guards 2–3), `RetentionRegistry`
       (`register`/`get`/`__contains__`/`__iter__`/`schedule_id_for(name)` = `uuid5` of the name),
       `RetentionOutcome`/`RetentionResult` (frozen). Module docstring carries the §D-S20-safety
       and §D-S20-shape `DESIGN:` blocks with ✅/❌; every public member has
       `Args:`/`Returns:`/`Raises:`/`Edge cases:`/`Thread safety:`/`Async safety:`.
5. [x] `varco_core/varco_core/retention/base.py` (new) — `RetentionTarget` (ABC) with
       `kind`/`supports_older_than`/`supports_dry_run`/`purge`, and `CallableRetentionTarget`.
6. [x] `varco_core/tests/test_retention_no_di_side_effect.py` (new) — importing every
       `varco_core.retention` submodule registers nothing, and
       `container.scan("varco_core", recursive=True)` gains **no** binding for any retention name
       and starts **no** task (same shape as the cloudevents/tls guards).

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_retention_policy.py varco_core/tests/test_retention_no_di_side_effect.py -q`

### Phase 3 — the five adapters (🟡 S–M)

7. [x] `varco_core/tests/test_retention_targets.py` (new, **failing first**) — a **parametrized
       contract table** across all six targets (§D-S20-conformance): `kind` is non-empty and
       unique; `purge(dry_run=True)` on a `supports_dry_run=True` target returns
       `would_delete >= 0` and `deleted == 0` **and provably calls no delete method** (spy);
       `purge(dry_run=True)` on a `supports_dry_run=False` target returns
       `skipped_reason` and `deleted == 0`; `limit` is forwarded; a target never swallows the
       backend's `ValueError`/`NotImplementedError`.
8. [x] `varco_core/tests/test_retention_targets.py` (extend, **failing first**) — per-target
       specifics against in-memory backends: `DlqRetentionTarget` ⇒ `ValueError` without
       `acknowledge_dead_letter_deletion=True`, and forwards `older_than`/`channel`/`tenant_id`/
       `limit` to `delete_where` (`dlq.py:450`); `AuditRetentionTarget` forwards
       `allow_chain_break` and **does not set it by default** (`audit.py:360-366`);
       `Idempotency`/`Revocation` targets reject an `older_than` and return the store's own count
       (`idempotency/base.py:202`, `revocation/base.py:173`), including `0` from a native-TTL
       backend; `JobRetentionTarget` maps `older_than` → `completed_before`
       (`job/base.py:880-916`).
9. [x] `varco_core/varco_core/retention/targets.py` (new) — the five adapters. Each docstring names
       the shipped method it wraps with its `file:line`, and states *"this adapter exists so the
       ABC gains no method — see CLAUDE.md's `BulkCache`/`AsyncCache` rule"*.
       `JobRetentionTarget`'s docstring carries the ⚠️ *"`JobPoller(retention_sweep=True)`
       (`poller.py:75-88`) may already do this — do not run both"*.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_retention_targets.py -q`

### Phase 4 — close Plan 032's driver gaps (🟡 S–M)

10. [x] `varco_core/tests/test_schedule_materializer.py` (extend, **failing first**) — a `Schedule`
        with `task_name=None` (the default) produces a `Job` with `task_payload is None` —
        **byte-identical to today, pinned before the change**; with `task_name="x"` and
        `payload={"a": 1}` it produces `TaskPayload(task_name="x", kwargs={"a": 1})`; the job id is
        **unchanged** by `task_name` (still `uuid5` of `(schedule_id, wall)`, `materializer.py:76-78`).
11. [x] `varco_core/varco_core/schedule/entity.py` — add `task_name: str | None = None` after
        `payload` (`entity.py:115`), with a full `Attributes:` entry citing §D-S20-driver.
        `varco_core/varco_core/schedule/materializer.py:226-234` — emit `task_payload`.
        ⚠️ **Verify the import is acyclic** (`job.base` imports `job.task` only under
        `TYPE_CHECKING`, `job/base.py:77-79`): `uv run python -c "import
        varco_core.schedule.materializer"`. If it cycles, move the import into `_build_job`'s body
        and record why in a comment.
12. [x] `varco_sa/varco_sa/schedule.py` + `varco_beanie/varco_beanie/schedule.py` — the nullable
        `task_name` column/field; `varco_sa/varco_sa/migrations/versions/0008_schedule_task_name.py`
        (new, `down_revision="0007"`). **Also verify** `0007_schedules_table.py` actually creates
        the `UNIQUE(schedule_id, run_at)` index `materializer.py:29` claims; if it does not, file a
        BACKLOG row (do **not** add it here — §D-S20-multiproc).
13. [x] `varco_core/tests/test_retention_scheduler.py` (new, **failing first**) — with
        `InMemoryScheduleRepository` + `InMemoryJobStore` + a fake runner: `interval=0.0` ⇒
        `start()` creates **no task** (`reschedule.py:66-67`'s contract); one sweep materializes
        exactly one job for a due policy and **zero** on an immediate second sweep (the `uuid5`
        convergence, `materializer.py:130-135`); a schedule not in the registry is **skipped**; a
        sweep that raises is logged and the loop survives (`reschedule.py:82-88`); `stop()` before
        `start()` is a no-op.
14. [x] `varco_core/varco_core/retention/scheduler.py` (new) — `RetentionScheduler(registry, *,
        schedule_repo, job_store, task_registry, job_runner=None, interval=0.0)` with
        `start`/`stop`/`sweep_once`/`purge_policy`, the lazy-task discipline of
        `reschedule.py:63-88`, and `ensure_schedules()` (idempotent upsert of one `Schedule` per
        policy, `schedule_id=registry.schedule_id_for(name)`,
        `task_name="varco.retention.purge"`, `payload={"policy": name}`).
15. [x] `varco_core/tests/test_retention_scheduler.py` (extend, **failing first**) — the dispatch
        path end-to-end with a real `TaskRegistry`: `purge_policy("nightly")` is reachable through
        `registry.invoke(TaskPayload("varco.retention.purge", kwargs={"policy": "nightly"}))`; an
        unknown policy name raises `RetentionPolicyNotFoundError`; `dry_run=True` deletes nothing
        (spy on the backend); `max_batches` truncates and reports `truncated=True`; `tenant_ids`
        enters `tenant_context` per tenant (assert `current_tenant()` inside the target,
        `service/tenant.py:151`, `:165`) and forwards `tenant_id=`; `tenant_ids=None` forwards
        `tenant_id=None` exactly once.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_schedule_materializer.py varco_core/tests/test_retention_scheduler.py varco_sa/tests/test_schedule_repository.py -q`

### Phase 5 — DI, posture, metrics (🟢 S)

16. [x] `varco_core/tests/test_retention_di.py` (new, **failing first**) —
        `bind_retention_registry(container, registry)` makes `RetentionRegistry` resolvable and
        starts nothing; calling it twice replaces the binding; it never touches
        `DIContainer.current()`.
17. [x] `varco_core/varco_core/retention/di.py` (new) — `bind_retention_registry`, docstring
        arguing the verb against the taxonomy table and citing `bind_trust_store` (§D-S20-verb).
18. [x] `varco_core/tests/test_retention_posture.py` (new, **failing first**) —
        `inspect_retention_posture()` with no argument returns `configured=False` and **never
        raises**; with a registry it reports `destructive_count`, `dry_run_count`,
        `platform_wide_policies` (only `tenant_ids is None`), `dlq_policies`,
        `short_retention_policies`, and the `kinds` set.
19. [x] `varco_core/varco_core/retention/posture.py` (new) — `RetentionPostureReport` (frozen) +
        `inspect_retention_posture(registry=None)`, mirroring
        `revocation/posture.py:35-126` including the *never raises* guarantee.
20. [x] `varco_core/varco_core/observability/retention.py` (new) — `install_retention_metrics()`,
        `install_*` shape (a), modelled on `install_reliability_metrics`
        (used at `varco_fastapi/varco_fastapi/reliability.py:71-82`). Counters: purged rows by
        `kind`/`policy`, sweep errors, dry-run skips.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_retention_di.py varco_core/tests/test_retention_posture.py -q`

### Phase 6 — FastAPI lifecycle + CLI (🟡 S)

21. [x] `varco_fastapi/tests/test_retention_lifecycle.py` (new, **failing first**) —
        `create_varco_app()` with **no** `retention=` registers no retention component and starts
        no task (**the load-bearing default**); `retention=RetentionLifecycle(...)` with
        `interval=0.0` registers it and still starts no task; with `interval>0` starts and stops
        cleanly; a missing `AbstractScheduleRepository`/`AbstractJobStore` binding raises a
        `LookupError` **naming the interface** (`reliability.py:132-139`'s shape); the component is
        **appended**, not prepended (assert its index relative to a tenancy component).
22. [x] `varco_fastapi/varco_fastapi/retention.py` (new) — `RetentionLifecycle` with
        `startup`/`shutdown` + `start`/`stop` aliases, carrying the same explanatory comment as
        `reliability.py:114-118`.
        `varco_fastapi/varco_fastapi/app.py` — a `retention: Any | None = None` keyword
        (beside `reliability`, `app.py:128`) appended exactly like `app.py:411-422`.
        ⛔ **`varco_fastapi/varco_fastapi/lifespan.py` is not touched** (§D-S20-lifecycle; Plan 041
        shares this constraint).
23. [x] `varco_core/tests/test_retention_cli.py` (extend, **failing first**) — the existing
        `--type dlq`/`--type audit` invocations still work **byte-identically**;
        `--policy <name> --target module:registry_factory` runs one policy once and honours its
        `dry_run`/floors/acknowledgements; `--policy` together with `--type` is a usage error
        (exit 2); an unknown policy name exits 2 naming it; `varco retention list` prints one line
        per policy including `dry_run`.
24. [x] `varco_core/varco_core/cli/retention.py` — add `--policy` and the `list` verb. **No change**
        to `_resolve` (`:63-79`), `_run_prune`'s existing branch (`:82-109`), or the
        `--type`/`--target` arguments (`:48-60`).

⛔ **CHECKPOINT** — `uv run pytest varco_fastapi/tests/test_retention_lifecycle.py varco_core/tests/test_retention_cli.py -q`

### Phase 7 — integration coverage (🟡 S, `-m integration`)

25. [x] `varco_sa/tests/test_retention_integration.py` (new, `@pytest.mark.integration`) — against
        the session-scoped `postgres_url` fixture, with a `uuid4().hex[:8]`-namespaced schema per
        the shared-container rule: `SADeadLetterQueue` + `SAAuditRepository` + `SAJobStore`
        adapters actually delete the expected rows and only those; `dry_run=True` deletes nothing
        (row count unchanged); a chunked sweep with `batch_size=2` converges; migration `0008`
        applies and `task_name` round-trips through `SAScheduleRepository`;
        `AuditRetentionTarget` on a `hash_chain=True` table raises without `allow_chain_break`.
        ⚠️ Redis/Beanie equivalents are **parked**, not forgotten (see §Parked).

⛔ **CHECKPOINT** — `uv run pytest varco_sa/tests/test_retention_integration.py -m integration -q`

### Phase 8 — docs, gates (🟡 S — **same commit as the code**)

26. [x] `technical_docs/features/retention-and-purge.md` (new). Sections: the module map; the
        target table with each wrapped verb's `file:line`; §D-S20-safety's eight guards as a table;
        the cron→`Job` path diagram naming the three gaps this plan closed (§D-S20-driver); the
        tenancy scoping rule (§D-S20-tenancy); the multi-process convergence argument citing
        `materializer.py:24-30`; why outbox/encryption keys/webhook deliveries are **not** targets.
        **Pitfalls table**, at minimum: `dry_run` is required and there is no default — a policy
        that does not say deletes nothing until you say so · a `supports_dry_run=False` target is
        **skipped** under `dry_run=True`, so a dry run over an idempotency store reports nothing
        rather than previewing · `JobPoller(retention_sweep=True)` and `JobRetentionTarget` overlap
        — run one · a DLQ policy needs `acknowledge_dead_letter_deletion=True` and dead letters are
        the last copy of a failed event · a hash-chained audit table refuses to prune without
        `allow_chain_break=True`, and pruning it makes `verify_chain()` report `ChainGap` forever ·
        `tenant_ids=None` means **platform-wide**, not "the ambient tenant" — there is no ambient
        tenant in a background job · renaming a policy orphans in-flight jobs as permanently
        RUNNING (`runner.py:544-552`) · `older_than` on a `delete_expired()` target is a wiring
        error, not a filter · a Beanie backend's BSON-millisecond widening applies to a chunked
        sweep exactly as it does to `delete_where` (`job/base.py:953-961`) · the outbox is **not**
        prunable and an entry still present is an *unpublished* event.
27. [x] `README.md` — a "Retention & purge automation" section: a runnable registry + policy +
        `create_varco_app(retention=...)` snippet, the `varco retention list`/`--policy` CLI block,
        and a target table. `ARCHITECTURE.md` — `RetentionTarget`/the six adapters/
        `RetentionPolicy`/`RetentionRegistry`/`RetentionScheduler`/`RetentionPostureReport` in a
        new "Retention" type hierarchy, plus the `Schedule.task_name` note under "Recurring
        schedules".
28. [x] `CLAUDE.md` — pointer-only, two edits: (a) a **§Retention & purge automation** section with
        the pointer and exactly three Rules that change agent behaviour — *retention is adapters
        over shipped verbs, never a new abstract method*; *`RetentionPolicy.dry_run` has no default
        and never will*; *the scheduler adds no lease — `materializer.py:24-30` is why*; (b) a
        Decision-Tree branch: *"scheduled cleanup of a framework table? → `varco_core.retention`
        (`bind_retention_registry` + `create_varco_app(retention=...)`), never a hand-written job
        and never a second scheduler; outbox? → **not a target**, see the feature doc"*. Also add
        the one-line `Schedule.task_name` note to the existing §Recurring schedules section.
29. [x] `testkit/varco_conformance/COVERAGE.md` — the §D-S20-conformance note row for
        `RetentionTarget` (why no suite; the un-park trigger). ⚠️ Coordinate with Plan 042 and
        Plan 038, which also append rows.
30. [x] `CHANGELOG.md` `## [Unreleased]`. `### Added`: `varco_core.retention` (policy, registry,
        `RetentionTarget` + six adapters, scheduler, posture, `bind_retention_registry`),
        `install_retention_metrics`, `varco_fastapi.RetentionLifecycle` +
        `create_varco_app(retention=)`, `varco retention --policy`/`list`, `Schedule.task_name`,
        SA migration `0008`. `### Fixed`: `JobRunner.recover()` now honours `Job.run_at`
        (Phase 1) — flagged as a **behaviour change** with the one-line rationale.
        `BACKLOG.md` — mark `S20` `✅ planned → plans/039-retention-and-purge-automation.md` and add
        the rows in §BACKLOG entries.
31. [x] `uv run python scripts/api_surface.py` then `--check`. **Expected: no diff** —
        `varco_core/__init__.py` exports no retention name (verified absence) and the surface is
        reached via `varco_core.retention.*`. ⚠️ `varco_fastapi`'s `__all__` **does** gain
        `RetentionLifecycle`; regenerate and **commit both snapshot files** (hard CI gate, CLAUDE.md).
32. [x] `uv run python scripts/import_budget.py --check --warn-only` — `varco_core.retention` is not
        imported from `varco_core/__init__.py` (PEP 562 lazy), so no change is expected; per
        CLAUDE.md this is **checked, not assumed**.

⛔ **CHECKPOINT** — `make lint`, `make type-check`, `make test`.

---

## Edge cases

- **A default `import varco_core`** → no retention module imported, nothing registered
  (Step 6, Step 32).
- **A default `create_varco_app()`** → no retention component, no task, no deletion (Step 21).
- **`interval=0.0`** → `start()` creates no `asyncio.Task` at all (`reschedule.py:66-67`'s
  contract, asserted Step 13).
- **`dry_run` omitted** → `TypeError` at construction, before anything runs.
- **`older_than=timedelta(0)` / negative** → `ValueError`, no escape hatch.
- **`older_than < 24 h` without acknowledgement** → `ValueError` naming the field.
- **`dry_run=True` on `delete_expired()`-backed target** → **skipped**, `deleted=0`,
  `skipped_reason` set; the store's delete method is provably never called (spy, Step 7).
- **A backend with native TTL returns `0` from `delete_expired()`** → reported as `deleted=0`, not
  an error (`idempotency/base.py:206-215`).
- **A DLQ backend that refuses `delete_where` (Kafka/NATS)** → `NotImplementedError` propagates,
  named in the result's `error`, the sweep continues to the next policy (`dlq.py:500-504`).
- **A hash-chained audit table without `allow_chain_break`** → the backend's `ValueError`
  propagates (`audit.py:360-366`); the scheduler never sets the flag for you.
- **No predicate reaches the backend** → the backend's own `ValueError` fires (`dlq.py:494`,
  `audit.py:376`, `job/base.py:965`); the registry never constructs such a call.
- **Two pods, same occurrence** → one `Job` row (deterministic `uuid5` + upsert,
  `materializer.py:24-30`, `:130-135`), one executor (`try_claim`, `runner.py:533`).
- **A pod dies mid-purge** → the job is RUNNING with an expired lease; `JobPoller` reaps it
  (`poller.py:59-74`). The purge is idempotent by construction — deleting already-deleted rows is a
  no-op.
- **A future occurrence exists when `recover()` runs** → **not** claimed, after Phase 1.
- **A policy renamed between materialization and execution** → `registry.get()` → `None`,
  `recover()` warns and the job stays RUNNING (`runner.py:544-552`); Pitfalls row.
- **`tenant_ids=("a","b")`** → two purge calls, each inside `tenant_context(tid)`, each forwarding
  `tenant_id=tid`.
- **`tenant_ids=None`** → one platform-wide call with `tenant_id=None`, reported by
  `inspect_retention_posture().platform_wide_policies`.
- **`tenant_ids=()`** → `ValueError` (an empty scope is always a typo).
- **`max_batches` reached** → sweep stops, `truncated=True`; the next occurrence continues.
- **An app with no job runner** → `job_runner=None`; materialization still happens and the CLI
  (`--policy`) executes; documented, not silent.
- **`container.scan("varco_core", recursive=True)`** → no binding, no task, no deletion (Step 6).

## Verification

```bash
uv sync --all-packages --all-extras

uv run pytest varco_fastapi/tests/milestone_e/test_task_runner.py \
              varco_fastapi/tests/test_retention_lifecycle.py -q

uv run pytest varco_core/tests/test_retention_policy.py \
              varco_core/tests/test_retention_targets.py \
              varco_core/tests/test_retention_scheduler.py \
              varco_core/tests/test_retention_di.py \
              varco_core/tests/test_retention_posture.py \
              varco_core/tests/test_retention_no_di_side_effect.py \
              varco_core/tests/test_retention_cli.py \
              varco_core/tests/test_schedule_materializer.py \
              varco_core/tests/test_schedule_rematerializer.py \
              varco_core/tests/test_job.py -q

uv run pytest varco_sa/tests/test_schedule_repository.py \
              varco_beanie/tests/ -q
uv run pytest varco_sa/tests/test_retention_integration.py -m integration -q

uv run python scripts/api_surface.py && uv run python scripts/api_surface.py --check
uv run python scripts/import_budget.py --check --warn-only
make lint && make type-check && make test
```

**DoD:**
1. A default `import varco_core` and a default `create_varco_app()` schedule nothing and delete
   nothing — asserted, not asserted-by-inspection (Steps 6, 21).
2. No `RetentionPolicy` can be constructed without stating `dry_run`, and none with a
   non-positive or sub-24 h window without an explicit acknowledgement (Step 3).
3. A `dry_run=True` sweep provably calls **no** delete method on any target, including the two that
   cannot preview (Step 7).
4. `git diff` shows **no new `@abstractmethod`** on `AbstractDeadLetterQueue`,
   `AbstractIdempotencyStore`, `AbstractTokenRevocationStore`, `AuditRepository`, or
   `AbstractJobStore`.
5. `git diff` shows **no** cron-parsing, occurrence-computation, or job-claim logic outside the
   shipped modules — `RetentionScheduler` only calls `materialize()`, `find_all_enabled()`,
   `save()`, and `recover()`.
6. `varco_fastapi/varco_fastapi/lifespan.py` is **untouched** (`git diff --stat`) — the constraint
   this plan shares with Plan 041.
7. Step 31's `api_surface.py --check` is clean **after** regenerating and committing;
   Step 32 shows no import-budget breach.

## Parked

| Item | Why | Un-park trigger |
|---|---|---|
| A general `varco_core.schedule.sweeper` driver for arbitrary app schedules | Needs catch-up/backpressure/ownership decisions this row has no mandate to settle (§D-S20-driver) | A consumer uses `varco_core.schedule` for non-retention work |
| A generic `JobDispatcher` poll loop over `claim_next()` | A second execution path for jobs; belongs to Plan 005's subsystem (§D-S20-dispatch) | An app needs durable scheduled jobs without a startup `recover()` |
| `TenantFanoutSupervisor`-backed per-tenant retention | A 5 000-tenant loop is a real limit but not this row's (§D-S20-tenancy) | A consumer has >100 tenants with per-tenant policies |
| Redis/Beanie retention integration tests | Phase 7 covers SA; the adapters are backend-agnostic by construction | The first backend-specific retention bug |
| `SecurityPosture` harness wiring for `inspect_retention_posture()` | Plan 036 owns the harness (§D-S20-posture) | Plan 036's harness gains a registration seam |
| A `testkit/varco_conformance/retention_target.py` suite | §D-S20-conformance | The first out-of-tree target, or a seventh in-tree one |
| An `OutboxRepository` age-delete | Deleting an unpublished event is event loss (§D-S20-outbox) | Never, unless the outbox gains an explicit "published-and-retained" state |
| A retention admin HTTP surface | An unrequested destructive REST surface (§Non-goals) | An operator asks, with an argued authorization model |

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| **A misconfigured window deletes production data.** The row's central risk | **Critical — irreversible** | §D-S20-safety's eight guards, six of which fire at construction/wiring; `dry_run` required with no default; `batch_size × max_batches` bounds one occurrence; every backend's own no-predicate `ValueError` is preserved. Steps 3, 7, 15 pin all of it |
| **A `dry_run=True` sweep that actually deletes**, because two targets have no preview | **Critical** | `supports_dry_run=False` ⇒ the scheduler **skips** the target; Step 7 spies on the backend and asserts zero delete calls. This is the single most important assertion in the plan |
| **A scanned `@Configuration` in `varco_core.retention` starts a deletion loop in every app that scans `varco_core`** | **Critical** | The §D-S20-shape rule, guarded by Step 6 with the same shape as the tls/cloudevents guards |
| ⚠️ **Phase 1 changes shipped `recover()` behaviour.** An app relying on future-dated jobs firing at startup changes | Medium | It contradicts `Job.run_at`'s own docstring (`job/base.py:252-255`) and `claim_next()`'s predicate (`:872-874`); Step 1 pins both directions before the change; `CHANGELOG` flags it under `### Fixed` as a behaviour change |
| ⚠️ **ASSUMPTION — `materializer.py` can import `varco_core.job.task` at module scope without a cycle.** `job.base` imports it only under `TYPE_CHECKING` (`job/base.py:77-79`), which is suggestive of one | Medium — blocks Step 11 | Step 11 **verifies** with an explicit `python -c "import …"` rather than assuming, and names the fallback (function-body import) in the step itself |
| ⚠️ **ASSUMPTION — `0007_schedules_table.py` creates the `UNIQUE(schedule_id, run_at)` index** `materializer.py:29` claims. Not re-verified | Medium — it is half of §D-S20-multiproc's cross-process guarantee | Step 12 verifies it; if absent, a BACKLOG row (Plan 032's business), and the in-process lock + `get()`-then-skip (`materializer.py:130-135`) still hold within a pod |
| **`JobRetentionTarget` double-sweeps with `JobPoller(retention_sweep=True)`** (`poller.py:75-88`) | Low — deletion is idempotent, but the counts mislead | Named in the adapter docstring, the Pitfalls table, and the README target table |
| **A renamed policy orphans an in-flight job as permanently RUNNING** (`runner.py:544-552`) | Low | `RetentionPolicyNotFoundError` from the handler; Pitfalls row; `JobPoller`'s stale sweep eventually fails it |
| **`Schedule` gains a column on a shipped framework table** | Medium — every deployment runs a migration | Nullable, no backfill, no default change; `task_name=None` is byte-identical (Step 10 pins it first) |
| **`COVERAGE.md` edit collides with Plans 038 and 042** | Low | One appended row; whichever lands last rebases. Named here so it is expected |
| **Scope creep into Plan 041's lifespan model** | Low | §D-S20-lifecycle forbids touching `lifespan.py`; DoD item 6 makes the absence of a diff a checked condition |
| **Scope creep into Plan 040's `audit.py` edits** | Low | This plan adds **nothing** to `audit.py` — it only calls the shipped `delete_where`. Verifiable: `git diff varco_core/varco_core/service/audit.py` should be empty |
| **Eight guards make the feature feel unusable and drive operators back to hand-written jobs** | Medium | The README snippet is a complete, copy-pasteable, correct policy — the ceremony is visible once, in one place. Un-park trigger for revisiting: a consumer reports it |

## Open questions

1. **Should `RetentionScheduler` call `runner.recover()` at all, or should dispatch be entirely the
   app's job?** `recover()` sweeps **every** PENDING task-payload job, not just retention's — after
   Phase 1 that is correct-but-broad. Lean **keep it, behind `job_runner=None` meaning "do not
   dispatch"**, and document that an app with its own dispatch passes `job_runner=None`. Decide at
   Step 14.
2. **Should `RetentionOutcome.examined` be mandatory?** Two targets (`delete_expired()`) cannot
   report it. Lean `examined: int | None = None`, with `None` meaning "the backend cannot say" —
   never a fake `0`. Decide at Step 4.
3. **Should `acknowledge_short_retention` be per-policy or a registry-wide flag?** Per-policy is
   more precise and more verbose. Lean **per-policy** (a registry-wide flag would license every
   future policy). Decide at Step 4.
4. **Does `varco retention list` belong on the CLI, or is it posture's job?** Both would print
   overlapping facts. Lean **both, deliberately**: the CLI reads a resolved factory for an operator
   with a shell; the inspector reads a live registry for a startup preflight. Decide at Step 24.
5. **Should `install_retention_metrics()` live in `varco_core/observability/retention.py` or in
   `varco_core/retention/metrics.py`?** The former matches `install_cache_metrics`/
   `install_reliability_metrics`; the latter keeps the feature in one directory. Lean the former
   (house consistency). Decide at Step 20.

## BACKLOG entries this plan creates

| ID | Row | Where |
|---|---|---|
| — | `S20` → `✅ planned → plans/039-retention-and-purge-automation.md` | `BACKLOG.md:81` |
| — | ⚠️ **Correction to `S20`'s premise**: *"outbox pruning"* is listed as an existing cleanup verb and **does not exist** (`service/outbox.py:62-64`), and the job store is *already* swept by `JobPoller(retention_sweep=True)` (`poller.py:75-88`) | Amend the `S20` rationale cell |
| new | **`ScheduleMaterializer` had no driver and produced jobs with no `task_payload`** — Plan 032 shipped an engine that nothing called and nothing could execute. Closed by this plan for retention; a *general* schedule driver is still absent | Live table, 🟡 |
| new | **`JobRunner.recover()` ignored `Job.run_at`** — fixed in Phase 1; filed so the regression is greppable | Answered/Shipped table |
| new | **No generic `JobDispatcher`** — varco has no poll loop that claims and executes durable PENDING jobs between restarts; `recover()` is startup-shaped and over-broad | Live table, 🟡 |
| new | **Verify `UNIQUE(schedule_id, run_at)` exists in `0007_schedules_table.py`** — `materializer.py:29` claims it as the cross-process backstop | Live table, 🟡 (drop if Step 12 confirms it) |
| new | *`TenantFanoutSupervisor`-backed per-tenant retention* — parked, trigger recorded | Parked table |
| new | *`SecurityPosture` harness wiring for `inspect_retention_posture()`* — parked, Plan 036 owns the harness | Parked table |
