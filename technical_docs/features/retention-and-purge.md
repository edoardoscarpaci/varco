# Retention & purge automation

Plan 039 (BACKLOG row `S20`). A `RetentionPolicy` registry materialized onto the shipped
cron→`Job` path (`varco_core.schedule` + `AbstractJobRunner`) — an operator declares *"prune
dead letters older than 30 days at 03:00 Europe/Rome"* once, and varco schedules and runs it
with the same DST-safe cron semantics, deterministic-`uuid5` cross-process convergence, and
`AbstractJobRunner` as every other job.

**Nothing is scheduled and nothing is deleted by default.**

## Module map

```
varco_core/retention/
├── __init__.py     # re-exports nothing — no @Singleton/@Provider/@Configuration, ever
├── policy.py        # RetentionPolicy, RetentionRegistry, RetentionOutcome, RetentionResult
├── base.py           # RetentionTarget (ABC), CallableRetentionTarget
├── targets.py        # Dlq/Audit/Idempotency/Revocation/Job adapters
├── scheduler.py      # RetentionScheduler + execute_policy() + RetentionPolicyNotFoundError
├── posture.py         # inspect_retention_posture()
└── di.py               # bind_retention_registry(container, registry)

varco_core/observability/retention.py   # install_retention_metrics() (opt-in)
varco_core/cli/retention.py             # varco retention prune/list (extended)
varco_fastapi/retention.py              # RetentionLifecycle
```

## The target table

| Adapter | Wraps | `supports_older_than` | `supports_dry_run` | Note |
|---|---|---|---|---|
| `DlqRetentionTarget` | `AbstractDeadLetterQueue.delete_where` (`varco_core/varco_core/event/dlq.py:450`) | ✅ | ✅ via `list_entries` | **Requires `acknowledge_dead_letter_deletion=True`** — dead letters are the last remaining copy of an event that already failed every retry |
| `AuditRetentionTarget` | `AuditRepository.delete_where` (`varco_core/varco_core/service/audit.py:339`) | ✅ | ✅ via `list()` | `allow_chain_break` is an explicit ctor arg — never auto-set |
| `IdempotencyRetentionTarget` | `AbstractIdempotencyStore.delete_expired` (`varco_core/varco_core/idempotency/base.py:202`) | ❌ | ❌ | Intrinsic expiry; may legitimately return `0` on a native-TTL backend |
| `RevocationRetentionTarget` | `AbstractTokenRevocationStore.delete_expired` (`varco_core/varco_core/revocation/base.py:173`) | ❌ | ❌ | Same shape |
| `JobRetentionTarget` | `AbstractJobStore.delete_where` (`varco_core/varco_core/job/base.py:880`) | ✅ (`completed_before`) | ✅ via `list_by_status` | ⚠️ `JobPoller(retention_sweep=True)` (`varco_fastapi/varco_fastapi/job/poller.py:75-88`) may already cover this — do not run both |
| `CallableRetentionTarget` | any `async (older_than, limit, dry_run) -> int` | declared by the caller | declared by the caller | The out-of-tree escape hatch |

**Not targets, deliberately:**

- **Outbox** — `OutboxRepository` exposes only `save`/`get_pending`/`delete(entry_id)`
  (`varco_core/varco_core/service/outbox.py:62-64`). Adding a bulk-delete would be ABC surgery,
  and deleting an outbox row by age is deleting an **unpublished** event — the remedy for a
  stuck outbox is the DLQ + `DlqRedriver`, not deletion.
- **Encryption keys** — `EncryptionKeyStore.delete(kid)` exists but is crypto-shredding, a
  deliberate irreversible operator act (CLAUDE.md's Field-level encryption section). A scheduler
  must never do it.
- **Webhook deliveries** — `WebhookDelivery` has no repository and no persistence at all.

## §D-S20-safety's eight guards

| # | Guard | Where | Fails |
|---|---|---|---|
| 1 | `dry_run: bool` is a **required field with no default** | `RetentionPolicy` | At construction — `TypeError` |
| 2 | `older_than <= 0` ⇒ `ValueError`, no acknowledgement escape | `RetentionPolicy.__post_init__` | At construction |
| 3 | `older_than < 24h` ⇒ `ValueError` unless `acknowledge_short_retention=True` | `RetentionPolicy.__post_init__` | At construction |
| 4 | `older_than` capability mismatch (present on a `supports_older_than=False` target, or absent on a `True` one) ⇒ `ValueError` | `RetentionRegistry.register()` | At wiring |
| 5 | `dry_run=True` on a `supports_dry_run=False` target ⇒ **skip, report, delete nothing** | The target's own `purge()` | At run, loudly, non-destructively |
| 6 | `batch_size` × `max_batches` bounds one occurrence's blast radius | `execute_policy()` | Always — reports `truncated=True` |
| 7 | The underlying verb's own no-predicate `ValueError` is never bypassed | `dlq.py:494`, `audit.py:376`, `job/base.py:965` | At the backend |
| 8 | A DLQ target needs `acknowledge_dead_letter_deletion=True`; a hash-chained audit target needs `allow_chain_break=True` | Adapter constructors | At construction |

Guard 5 is the single most important assertion in the whole feature: a `dry_run=True` sweep
must provably call **no** delete method on any target, including the two that cannot preview
(`IdempotencyRetentionTarget`, `RevocationRetentionTarget`) — those are *skipped* under
`dry_run=True`, never invoked.

## The cron→`Job` path — three gaps this plan closed

Plan 032 (`varco_core.schedule`) shipped `ScheduleMaterializer` and `Schedule`, but three links
were missing before retention could ride the path:

```
Schedule (cron + timezone + task_name)
   │  ScheduleMaterializer.materialize()   [Plan 032, unchanged]
   ▼
Job (task_payload = TaskPayload(task_name, kwargs))   ← GAP 1, closed: Schedule.task_name
   │  AbstractJobStore.save()  — deterministic uuid5 upsert
   ▼
RetentionScheduler.sweep_once()  → find_all_enabled() → materialize()   ← GAP 2, closed: nothing called materialize() before
   │
   ▼
JobRunner.recover()  — try_claim() + registry.invoke()   ← GAP 3, closed: recover() now honours run_at
   │
   ▼
RetentionScheduler.purge_policy(policy=name)  →  execute_policy()  →  RetentionTarget.purge()
```

1. **`Schedule.task_name`** (nullable, `None` by default — byte-identical to every existing
   row) + `_build_job()` now emits a `TaskPayload` when it is set.
2. **`RetentionScheduler`** — a `ScheduleRematerializer`-shaped loop (`interval=0.0` ⇒ `start()`
   creates no task) that calls `materialize()`/`find_all_enabled()`/`save()`/`recover()`
   unchanged. It computes nothing itself.
3. **`JobRunner.recover()` now honours `Job.run_at`** — previously it claimed every PENDING
   task-payload job regardless of schedule, violating `run_at`'s own documented contract
   (`job/base.py:252-255`). Fixed as defence-in-depth at the runner layer: `InMemoryJobStore.
   try_claim()` already enforced this internally, so the bug was not observable through the
   in-memory store every unit test uses — but the `AbstractJobRunner` ABC makes no such guarantee
   for a store whose `try_claim()` does not itself filter.

## Tenancy scoping

`RetentionPolicy.tenant_ids: tuple[str, ...] | None`:

- `None` — an explicit, documented **platform-wide** purge. No `tenant_context()` is entered;
  the target reads `current_tenant()` itself (typically `None` in a background job) and
  forwards it as-is.
- A non-empty tuple — the scheduler loops the tenants, entering `tenant_context(tid)` for each
  and letting the target read `current_tenant()` inside that block.
- `()` — `ValueError` at construction. There is no state that means "whatever tenant happens to
  be ambient" — a scheduled purge has no request, so there is no ambient tenant to fall back to.

`inspect_retention_posture().platform_wide_policies` reports every policy with `tenant_ids is
None`, so an operator can see at a glance which policies cross tenants.

## Multi-process convergence

`RetentionScheduler` runs on **every** pod, adds **no lease**, and relies entirely on shipped
guarantees:

- **Materialization**: `Job.job_id = uuid5(NAMESPACE_URL, f"varco:schedule:{schedule_id}:{wall.isoformat()}")`
  (`materializer.py:76-78`) — two materializers computing the same occurrence converge on the
  same physical row via `AbstractJobStore.save()`'s documented upsert semantics
  (`job/base.py:682-687`).
- **Execution**: `JobRunner.recover()`'s `try_claim()` (`runner.py:533`) — exactly one pod wins
  each occurrence.

⚠️ **The `UNIQUE(schedule_id, run_at)` index materializer.py:29's DESIGN block cited as
reinforcement does not — and cannot — exist** in `varco_sa/varco_sa/migrations/versions/0007_schedules_table.py`
— verified absent (the `schedules` table has no `run_at` column; only materialized `Job` rows do,
and those already carry the deterministic id above). Filed as a BACKLOG row, not added here — a
schema change to a shipped framework table is Plan 032's business, not a retention row's. The
deterministic-id + upsert convergence above holds regardless.

⛔ **Never add a lease to `Schedule`.** A synthetic lease row saved through `AbstractJobStore.save()`
would be indistinguishable from a real job to every caller of `list_by_status()`/`delete_where()`
— including `JobRetentionTarget` itself, which would then delete the lease row it depends on.

## Usage

```python
from datetime import timedelta

from varco_core.retention.policy import RetentionPolicy, RetentionRegistry
from varco_core.retention.targets import DlqRetentionTarget, AuditRetentionTarget
from varco_core.retention.di import bind_retention_registry
from varco_fastapi.retention import RetentionLifecycle
from varco_fastapi.app import create_varco_app

registry = RetentionRegistry()
registry.register(
    RetentionPolicy(
        name="nightly-dlq",
        target=DlqRetentionTarget(dlq=my_dlq, acknowledge_dead_letter_deletion=True),
        cron_expr="0 3 * * *",
        timezone="Europe/Rome",
        dry_run=True,               # flip to False only after reviewing would_delete
        older_than=timedelta(days=30),
    )
)
registry.register(
    RetentionPolicy(
        name="audit-1y",
        target=AuditRetentionTarget(repo=my_audit_repo),
        cron_expr="0 4 * * *",
        timezone="UTC",
        dry_run=False,
        older_than=timedelta(days=365),
    )
)

bind_retention_registry(container, registry)

app = create_varco_app(
    container,
    retention=RetentionLifecycle(registry, container=container, interval=300.0),
)
```

CLI, one policy, one run, no scheduler needed (e.g. a Kubernetes `CronJob`):

```bash
varco retention prune --policy nightly-dlq --target myapp.wiring:build_retention_registry
varco retention list --target myapp.wiring:build_retention_registry
```

`--type {dlq,audit}`/`--target module:factory` (the pre-existing manual sweep) is unchanged and
still works verbatim.

## Pitfalls

| Pitfall | Why |
|---|---|
| `dry_run` is required and there is no default | A policy that does not say deletes nothing until you say so |
| A `supports_dry_run=False` target is **skipped** under `dry_run=True` | A dry run over an idempotency/revocation store reports `skipped_reason`, never a preview |
| `JobPoller(retention_sweep=True)` and `JobRetentionTarget` overlap | Run one — deletion is idempotent so running both is not unsafe, only redundant/confusing counts |
| ⚠️ There is **no** `UNIQUE(schedule_id, run_at)` index backing cross-process convergence, despite `materializer.py`'s DESIGN block having once claimed one | The `schedules` table has no `run_at` column, so the constraint is structurally impossible there, not merely missing (`0007_schedules_table.py` creates only `UNIQUE(schedule_id)`). Convergence rests entirely on the deterministic `uuid5` job id + `save()`'s upsert + the `get()`-then-skip at `materializer.py:130-135`, which does hold. **Do not weaken the deterministic id on the assumption a database constraint is standing behind it**, and do not "restore" the index onto `schedules` — it would have to go on the `Job` side, which is Plan 032's business. Filed in BACKLOG |
| A DLQ policy needs `acknowledge_dead_letter_deletion=True` | Dead letters are the last copy of a failed event |
| A hash-chained audit table refuses to prune without `allow_chain_break=True` | Pruning it makes `verify_chain()` report a `ChainGap` forever after |
| `tenant_ids=None` means **platform-wide**, not "the ambient tenant" | There is no ambient tenant in a background job |
| Renaming a policy orphans an in-flight job as permanently RUNNING | `runner.py:544-552` — `registry.get()` returns `None`, the job never progresses; `JobPoller`'s lease reap eventually notices it stalled |
| `older_than` on a `delete_expired()`-backed target is a wiring error, not a filter | `IdempotencyRetentionTarget`/`RevocationRetentionTarget` raise `ValueError` |
| A Beanie backend's BSON-millisecond widening applies to a chunked sweep | Same caveat as `delete_where()` itself (`job/base.py:953-961`) |
| The outbox is **not** prunable | An entry still present is an *unpublished* event — deleting it is silent event loss |

## See also

- CLAUDE.md's Retention & purge automation section.
- README's Retention & purge automation section for the runnable snippet + CLI reference.
- `technical_docs/features/recurring-schedules.md` for the underlying `Schedule`/
  `ScheduleMaterializer` design this feature rides on.
