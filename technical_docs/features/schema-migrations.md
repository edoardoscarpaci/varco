# Schema migrations — Postgres/Alembic, MongoDB/Beanie, and auto-on-startup

Plan 006. One backend-agnostic contract (`varco_core.migration.AbstractMigrator`)
with two concrete engines — `varco_sa.migration.AlembicMigrator` (Postgres, wrapping
Alembic) and `varco_beanie.migration.BeanieMigrator` (MongoDB) — plus an ASGI
lifespan component (`varco_fastapi.MigrationLifecycle`) and a `varco migrate` CLI.

**Nothing in this feature runs by default.** `VARCO_MIGRATE_MODE` defaults to
`off`; with no `MigrationsModule` in `modules=` (the default) `create_varco_app` builds an
identical lifespan to the one it built before this feature existed. Every posture below
is opt-in.

---

## 1. The three modes — and which one to actually deploy

`MigrationSettings.mode` (env `VARCO_MIGRATE_MODE`) has three values:

| mode | What happens at startup | Who should use it |
|---|---|---|
| `off` (default) | nothing — no lifespan component is registered | anyone running migrations out-of-band already |
| `check` | resolve pending revisions; **fail startup** if the schema is behind; never writes DDL | **recommended production posture** |
| `upgrade` | acquire the lock → apply pending revisions → release | single-instance / dev / small deployments; PaaS with no pre-deploy hook |

The headline feature is `upgrade` — `uvicorn app:app` migrates the schema before the
first request. **The posture we recommend deploying is `check`.**

These are two different requirements and only the second is universally correct:

- "run migrations automatically at startup" — convenient, and a lock convoy at high
  replica counts (see §9).
- "never let a pod serve traffic against a schema it doesn't understand" — always
  correct.

`check` gives you the second without the first. Run migrations in a pre-deploy job
or init container (`varco migrate upgrade`), then let every pod prove the schema is
current before it accepts traffic.

---

## 2. Wiring auto-on-startup in ten lines

```python
from sqlalchemy.ext.asyncio import create_async_engine
from varco_fastapi import create_varco_app
from varco_fastapi import default_modules
from varco_fastapi.modules import MigrationsModule
from varco_sa.migration import AlembicMigrator

engine = create_async_engine("postgresql+asyncpg://…")
migrator = AlembicMigrator(engine, script_location="alembic")

app = create_varco_app(
    container,
    routers=[...],
    modules=[MigrationsModule(migrator), *default_modules()],  # ← list it before the defaults
)
```

```bash
VARCO_MIGRATE_MODE=upgrade uvicorn myapp:app
```

`MigrationLifecycle` is **prepended** to the lifespan component list — it runs before
the event bus, before the `OutboxRelay`, before the `JobRunner` — so nothing touches a
table that does not exist yet.

`MigrationsModule(*migrators, settings=None)` accepts a single `AbstractMigrator` or several;
several run **sequentially, in the order given** (a dual-backend service passing its Postgres
and Mongo migrators). `settings=` overrides the env-resolved `MigrationSettings`.

> **`VARCO_MIGRATE_MODE` set but no `MigrationsModule` in `modules=`** logs one WARNING naming
> the env var and runs nothing. Silently ignoring a set env var is the failure mode that
> wastes the most operator time, so it is loud.

`varco_fastapi` imports **only** `varco_core.migration` — never `varco_sa`,
`varco_beanie`, or `alembic`. The concrete migrator is constructed by the app and
passed in, exactly like `AbstractEventBus`.

---

## 3. The contract

`varco_core.migration` is contracts-only, with zero third-party dependencies.

```python
@dataclass(frozen=True)
class Revision:
    id: str  # alembic rev hash, or a Beanie version string
    label: str  # human name / message
    branch: str | None = None  # "varco" for framework revisions, None for app


@dataclass(frozen=True)
class MigrationPlan:
    current: tuple[str, ...]  # heads currently applied ( () on a virgin DB )
    pending: tuple[Revision, ...]

    @property
    def is_empty(self) -> bool: ...
    def format(self) -> str: ...


@dataclass(frozen=True)
class MigrationReport:
    applied: tuple[Revision, ...]
    duration_s: float
    skipped_locked: bool = False  # another holder did the work — not a failure


class AbstractMigrator(ABC):
    async def plan(self) -> MigrationPlan: ...  # abstract
    async def upgrade(self, target="heads", *, dry_run=False) -> MigrationReport: ...  # abstract
    async def downgrade(self, target: str) -> MigrationReport: ...  # abstract
    async def stamp(self, target: str = "heads") -> None: ...  # abstract
    async def check(self) -> MigrationPlan: ...  # CONCRETE — plan() + raise if pending
    async def close(self) -> None: ...  # CONCRETE no-op; engines override
```

`check()` and `close()` are **concrete, not abstract**, so a third-party migrator
written against an earlier version is not broken by their addition — the same rule
Plan 005 applied to `AbstractJobStore`.

### Exceptions

All inherit `MigrationError`, so one `except MigrationError` catches the family:

| Exception | Raised when |
|---|---|
| `PendingMigrationsError` | `check()` found pending revisions — carries the `MigrationPlan` |
| `MigrationLockTimeout` | lock not acquired within the timeout **and** revisions are still pending — carries `lock_key`, `waited_s` |
| `IrreversibleMigrationError` | `downgrade()` hit a migration with no `down()` |
| `MigrationBackendUnavailable` | an optional extra is missing — the message names the exact `pip install` line |

### Renamed in 3.0.0: `SchemaMigrationError` / `SchemaMigrationPlan` (Plan 022 / AB-2)

`varco_core.migrator` — the pre-existing, **unrelated** domain-model data/field
migration module — already owns the bare names `MigrationError`/`MigrationPlan` at the
package's top level. Since 3.0.0 the schema-migration pair is therefore named
`SchemaMigrationError`/`SchemaMigrationPlan` and **is** re-exported from `varco_core`, so an
import site says which concept it means:

```python
from varco_core import SchemaMigrationError, SchemaMigrationPlan  # ✅ schema migrations
from varco_core import MigrationError  # ⚠️ the OTHER one (varco_core.migrator)
```

`varco_core.migration.MigrationError` / `.MigrationPlan` still resolve, to the identical
objects, with a `DeprecationWarning` — **removed in 5.0.0** (floor: 2027-08-31; the
12-month window from 3.0.0's release put 4.0.0 out of reach — `CONTRIBUTING.md` §Removal
window). The bare names in the snippets above are the 3.0 shapes; read them as the
`Schema*` names.

Everything else — `AbstractMigrator`, `Revision`, `MigrationReport`,
`MigrationSettings`, `InMemoryMigrator`, and the other three exceptions — is
available directly from `varco_core`.

### `InMemoryMigrator`

The standard unit-test double, mirroring `InMemoryEventBus` / `InMemoryDeadLetterQueue`:
constructed with a list of `Revision`s and a starting position, records every call, and
can be told to raise on the Nth `upgrade`. Use it for any test of migration *wiring* —
no database required.

---

## 4. Multi-pod exclusion: the held-open lock transaction

Postgres exclusion uses a **transaction-scoped advisory lock held open across Alembic's
own transactions**. The lock lives in the database being migrated — the correct failure
domain, and it needs no extra infrastructure.

```
conn_lock  ──┐  BEGIN                              (dedicated NullPool connection)
             │  SET LOCAL idle_in_transaction_session_timeout = 0
             │  SELECT pg_try_advisory_xact_lock(hash('varco:migrate'))  → true
             │
conn_ddl   ──┼──▶ alembic upgrade heads   (its OWN connection; transaction_per_migration
             │                             = True, so each revision commits independently)
             │
             └  COMMIT   ← this IS the release. No release() call exists.
```

Three mechanics carry the whole design, and each has a specific failure mode:

**`SET LOCAL idle_in_transaction_session_timeout = 0` is mandatory.** The lock
transaction sits `idle in transaction` for the entire migration. A server- or role-level
`idle_in_transaction_session_timeout` would otherwise kill the lock holder mid-migration
and *silently un-exclude* the critical section — two pods running DDL concurrently, with
no error anywhere. This has a dedicated regression test
(`varco_sa/tests/test_migration_lock.py`).

**Alembic runs on a separate connection.** It manages its own transaction boundaries;
the lock transaction simply stays open around it. Sharing one connection would let
Alembic's commits release the lock.

**The lock connection comes from a dedicated `NullPool` engine.** A pooled connection
subject to `pool_recycle` could be recycled mid-hold, un-excluding concurrent DDL the
same way.

**Why transaction-scoped, not session-scoped:** `SAAdvisoryLock`'s `release()` can be
routed to a *different physical connection* behind a transaction-mode pooler (PgBouncer
`pool_mode=transaction`), leaking the lock — the U-16 defect documented in
`technical_docs/features/distributed-locks.md`'s Pitfalls section.
`SAXactAdvisoryLock.xact()` is released by the caller's own
COMMIT/ROLLBACK, and **also by process death**: an OOM-killed pod mid-migration leaves
no orphaned lock and there is no TTL to size.

**Accepted cost:** one held snapshot for the migration's duration — a pinned xmin
horizon that blocks vacuum on that database while the migration runs.

**Non-Postgres dialects** (SQLite in unit tests) short-circuit to "acquired" with a
one-time INFO log. SQLite is single-writer, so this is honest rather than a silent
downgrade.

**Using Redis instead:** `AlembicMigrator(lock=RedisLock(...))` and
`migration_lock(..., lock=...)` accept any `AbstractDistributedLock`. This is supported
for fleets already standardised on `RedisLock`, but it is not the default: it puts the
lock in the wrong failure domain (a Redis outage blocks a migration Postgres could
serve), and the TTL must exceed the longest migration — which is unknowable, and expiry
mid-migration means concurrent DDL.

### MongoDB: a lock document, with the TTL problem intact

MongoDB has no advisory locks, so `BeanieMigrator` uses a lock document
(`{_id: "__lock__"}` in the `varco_migrations` collection) acquired with a conditional
`find_one_and_update` upsert — `_id` uniqueness supplies the atomicity — carrying
`owner`, `acquired_at`, `expires_at`, `heartbeat_at`. A background heartbeat renews
`expires_at` (interval ≤ `ttl / 3`, cancelled in a `finally`); a crashed holder is
reclaimed after TTL expiry. `release()` is **owner-fenced** — a reclaimed holder cannot
delete the new holder's lock.

This *does* have the TTL-sizing problem that disqualified Redis for Postgres. It is
unavoidable on MongoDB, and it is one more reason Mongo index builds belong in the CLI
path rather than the startup path (§7).

---

## 5. Failure semantics

`MigrationSettings.on_failure` (env `VARCO_MIGRATE_ON_FAILURE`) is `fail` by default.

On `fail`, `MigrationLifecycle.start()` raises → `VarcoLifespan` stops already-started
components and re-raises → FastAPI startup fails → uvicorn exits non-zero → the
orchestrator never routes traffic to the pod. That is the entire point.

`warn` logs at ERROR and continues. **It is a foot-gun by construction** — the pod will
serve traffic against a schema it may not understand. It exists for the deployment that
would rather serve degraded than not at all.

### Lock contention is the normal case, not an error

```
acquire lock (deadline = lock_timeout, default 30s, poll every 0.5s)
├── acquired  → run upgrade under it (deadline = timeout, default 300s) → release → serve
└── timed out → re-evaluate plan()
        ├── empty      → another pod finished the work → serve   (the common rolling-deploy path)
        └── non-empty  → on_failure applies (default: raise MigrationLockTimeout)
```

A pod that times out on the lock and then finds nothing pending has **succeeded** —
`MigrationReport.skipped_locked=True`, and it serves traffic.

### Partial failure is explicit, not hidden

`transaction_per_migration=True` means a failure in revision N leaves N-1 applied and
`alembic_version` at N-1. A re-run resumes at N. There is **no automatic downgrade,
ever** — an automatic rollback of a data-destructive revision is strictly worse than a
failed deploy. `downgrade` is a deliberate, human-invoked CLI action.

The documented exception: DDL that cannot run in a transaction
(`CREATE INDEX CONCURRENTLY`, `ALTER TYPE … ADD VALUE` on older PG) must use Alembic's
`autocommit_block()`, and **can leave an INVALID index behind on failure**, requiring a
manual `DROP INDEX`. There is no automatic cleanup.

---

## 6. The two-branch Alembic model — and `upgrade heads` (plural)

`varco_sa` ships its own Alembic revisions **inside the wheel**, under
`varco_sa/migrations/versions/`, with `branch_labels = ("varco",)`.

You never list framework metadata in `env.py`. Instead:

- `varco migrate upgrade` (default `--to heads`) applies **both** the app branch and
  the `varco` branch.
- `pip install -U varco-sa` that adds a column to `varco_jobs` brings its revision with
  it; the next `upgrade heads` applies it. Nothing in the app repo changes.

`AlembicMigrator(include_framework_branch=True)` (the default) appends the packaged
directory to `version_locations` automatically.

### ⚠️ `heads`, not `head`

With two branches present, `alembic upgrade head` (singular) applies **one branch only**.
Every varco-provided command defaults to `heads`, and every example here uses `heads`. If
you have typed `alembic upgrade head` for years, this is the one habit to break.

### Three lines in an existing `env.py`

So that your app's own `autogenerate` never tries to re-declare a framework table:

```python
# alembic/env.py
from varco_sa.migration.env_template import include_object, configure_kwargs

context.configure(
    connection=connection,
    target_metadata=target_metadata,
    include_object=include_object,  # ← filters out framework-owned tables
    **configure_kwargs(),  # ← transaction_per_migration=True, compare_type=True
)
```

### The framework tables

Nine, all registered automatically:

`varco_outbox` · `varco_inbox` · `varco_jobs` · `varco_sagas` ·
`varco_conversation_turns` · `varco_dedup_log` · `varco_audit_log` ·
`varco_dead_letters` · `varco_encryption_keys`

`varco_sa.framework_metadata()` returns one merged `MetaData` containing all of them;
`framework_table_names()` returns just the names (what `include_object` uses). Each
owning module self-registers at import time via `register_framework_metadata()`, so a
framework table added in a future varco release is picked up on upgrade **with no code
change on your side**. `varco_sa/tests/test_framework_metadata.py` fails the day someone
adds a framework table without registering it.

### Single-branch escape hatch

For shops with a policy against vendored revisions or multiple Alembic heads:

```python
migrator = AlembicMigrator(engine, script_location="alembic", include_framework_branch=False)

# alembic/env.py — take ownership of framework tables in your own revisions instead
target_metadata = get_target_metadata(User, Post, include_framework=True)
```

This is fully supported. The cost is that you must remember to `autogenerate` after every
`pip install -U varco-sa`; forget, and you get a runtime column-missing error instead of a
migration.

---

## 7. `ensure_table()` reconciliation and the one-time `adopt` step

Six `varco_sa` classes ship an `ensure_table()` that calls
`metadata.create_all(checkfirst=True)`: `SAEncryptionKeyStore`, `SADeadLetterQueue`,
`SAJobStore`, `SASagaRepository`, `SAConversationStore`, `SADeduplicator`.

`ensure_table()` is **not removed and not deprecated** — it is genuinely the right choice
for tests and single-file demos. But it and migration management are **mutually exclusive
per deployment**, and the hazard is directional: `create_all(checkfirst=True)` against an
Alembic-managed table is a harmless no-op, while an Alembic `CREATE TABLE` against a table
`ensure_table()` already made **fails**.

Two things make them coexist safely:

1. **Framework baseline revisions are idempotent** — every `create_table` is guarded by a
   `has_table()`/`checkfirst` check, so a database built by `ensure_table()` upgrades
   cleanly instead of erroring.
2. **`adopt` stamps the branch head** so subsequent upgrades start from the right place.

### The one manual step in this whole feature

An **existing deployment** that has been using `ensure_table()` runs this **once**, before
its first `upgrade`:

```bash
varco migrate adopt -t myapp.db:migrator
```

or, in code:

```python
adopted = await migrator.adopt_framework_tables()  # → ['varco_jobs', 'varco_outbox', …]
```

It inspects the live database; if framework tables exist and the `varco` branch has no
head recorded, it stamps `varco@head` **without executing any DDL**. It is idempotent —
running it again returns `[]`.

**Order matters: adopt, then upgrade.** The residual risk is a *column* added by a future
framework revision to a table `ensure_table()` created and never stamped; `adopt` records
the stamp so that revision applies against the right baseline.

---

## 8. MongoDB: hand-written migrations + index reconciliation

MongoDB is schemaless — there is nothing to diff except indexes. There is **no
autogenerate**, and deliberately no document-shape differ: it would compare varco's
generated `Document` shape against sampled documents and produce guesses.

Two independent mechanisms sit behind one `AbstractMigrator`:

### (a) Versioned `Migration` scripts

```python
from varco_beanie.migration import Migration, MigrationRegistry, BeanieMigrator


class BackfillOrderStatus(Migration):
    version = "20260812_001"  # sortable; uniqueness validated at register() time
    name = "backfill order status"

    async def up(self, db) -> None:
        await db.orders.update_many({"status": None}, {"$set": {"status": "pending"}})

    async def down(self, db) -> None:  # optional — omit and downgrade raises
        await db.orders.update_many({"status": "pending"}, {"$set": {"status": None}})


registry = MigrationRegistry()
registry.register(BackfillOrderStatus)
# or: registry.discover("myapp.migrations")   # importlib walk, mirrors container.scan

migrator = BeanieMigrator(db, registry)
```

Applied migrations are recorded one document per migration in the `varco_migrations`
collection (`{_id: version, name, checksum, applied_at, duration_ms, applied_by}`). The
collection is created lazily on first use.

A recorded migration whose **source no longer matches its stored checksum** raises —
tamper detection for "someone edited an already-applied migration". Opt out with
`BeanieMigrator(..., verify_checksums=False)`.

A migration with no `down()` raises `IrreversibleMigrationError` on `downgrade`.

### (b) ⚠️ Index reconciliation is `check` by default — even in `upgrade` mode

`index_mode: Literal["off", "check", "create"] = "check"` is **independent of `mode`**.
Running `mode="upgrade"` does **not** silently start building indexes.

**This is a rule, not a caveat.** An index build on a large collection is
minutes-to-hours of work; on a replica set it replicates and can stall secondaries; and
it would happen exactly when a rolling deploy is starting N new pods. Wiring that into a
startup hook by default would be the single most dangerous thing in this feature.

- `index_mode="check"` (default) — reports drift via the existing `BeanieIndexGuard` and
  lets `on_failure` decide. Missing indexes appear in `MigrationPlan.pending` as
  `Revision(id="index:<collection>:<label>", branch="index")`, so `mode="check"` reports
  migrations and indexes uniformly.
- `index_mode="create"` — applies **missing** indexes only. It never drops
  `unexpected_indexes`: dropping an index someone added deliberately is destructive.
  ✅ **Fixed (Plan 024 / C3, RT9)**: `upgrade()` reconciles indexes **whether or
  not** migration revisions are pending — not only when the migration
  registry has pending work. Before this fix, an empty (or fully-applied)
  registry made `upgrade()` return before ever reaching the index block, so
  `plan()` (which always reports index drift independently) and `upgrade()`
  silently disagreed. Index reconciliation happens **under the migration
  lock** — same lock, same serialization guarantee as a hand-written
  migration — so `createIndex`'s own idempotency (MongoDB 4.4+) is a
  performance property, not a substitute for the lock's cross-process
  exclusion. The common no-drift startup path (no pending migrations, no
  index drift) still takes **no lock at all** — one extra `listIndexes`
  round-trip is paid only when the migration registry is empty, which is
  exactly the case that needs it to detect drift.
- `index_mode="off"` — skip reconciliation entirely.

Run index creation from the CLI as a pre-deploy job instead:

```bash
varco migrate index -t myapp.db:migrator --create
```

Hand-written `Migration.up(db)` scripts run under `mode="upgrade"` normally — the
restriction is specific to *reconciled* indexes, the ones varco derives implicitly and
could therefore surprise an operator with.

---

## 9. Composite deployments

`create_composite_app`'s `CompositeLifespan` already drives each sub-app's own lifespan,
so each service's `MigrationLifecycle` runs with its own settings and its own database.
**No composite-level code is needed.** Two properties matter operationally:

- **Startup time is the sum, not the max.** N services migrate sequentially — composite
  startup is serial by design (fail-fast). Budget `N × timeout` in the readiness probe's
  `initialDelaySeconds`.
- **Two services sharing one database converge on the same lock automatically.** The
  default lock key is the literal `"varco:migrate"` and Postgres advisory locks are
  already scoped per-database, so two composite members on the same DB serialize with no
  configuration. Two services on *different* databases never contend. Use `lock_key=` /
  `VARCO_MIGRATE_LOCK_KEY` for schema-per-service setups wanting finer granularity.

One service failing to migrate aborts the whole composite startup. That is the documented
composite fail-fast behaviour, and it is correct here.

---

## 10. Environment variables

| Env var | Default | Effect |
|---|---|---|
| `VARCO_MIGRATE_MODE` | `off` | `off` / `check` / `upgrade` — an unknown value raises `ValueError` naming the three legal values |
| `VARCO_MIGRATE_ON_FAILURE` | `fail` | `fail` (raise, abort startup) / `warn` (log ERROR, keep serving) |
| `VARCO_MIGRATE_LOCK_KEY` | `varco:migrate` | distributed lock key |
| `VARCO_MIGRATE_LOCK_TIMEOUT` | `30.0` | seconds to poll for the lock before re-checking `plan()` |
| `VARCO_MIGRATE_TIMEOUT` | `300.0` | overall seconds budget for the whole migration run (all migrators) |
| `VARCO_MIGRATE_TARGET_REV` | `heads` | revision target for `upgrade` |
| `VARCO_MIGRATE_DRY_RUN` | `false` | render what would run without applying it |

`MigrationSettings.from_env(env=...)` takes an injectable mapping, so tests never mutate
`os.environ` and composite services can pass a scoped mapping via
`build_service(prefix, factory, env={...})`.

---

## 11. CLI reference

`varco_core` ships a `varco` console script. Subcommands are discovered through the
`varco.commands` entry-point group, so `varco_core` keeps zero sibling dependencies and a
third-party backend can contribute verbs the same way.

The migrator is resolved from a `module:callable` target, mirroring `uvicorn app:app` and
Alembic's `env.py` — the CLI does not invent a second config-discovery mechanism. The
target may be an `AbstractMigrator` instance or a callable returning one.

```bash
varco migrate current   -t myapp.db:migrator            # applied heads
varco migrate pending   -t myapp.db:migrator            # exit 1 if pending → CI gate
varco migrate check     -t myapp.db:migrator            # raises/exits 1 if behind
varco migrate upgrade   -t myapp.db:migrator [--to heads] [--dry-run]
varco migrate downgrade -t myapp.db:migrator --to <rev> --yes
varco migrate stamp     -t myapp.db:migrator [--to heads]
varco migrate adopt     -t myapp.db:migrator            # the ensure_table() bridge (§7)
```

Backend-contributed verbs (present when that package is installed):

```bash
varco migrate revision  -t myapp.db:migrator -m "add column" [--autogenerate]   # varco_sa
varco migrate heads     -t myapp.db:migrator                                    # varco_sa
varco migrate index     -t myapp.db:migrator [--create]                         # varco_beanie
varco migrate new       --name "backfill status" [--out myapp/migrations]       # varco_beanie
```

**Exit codes:** `0` ok · `1` migration/drift error · `2` usage error.

`--json` is accepted on every core verb and emits parseable JSON for CI consumption
(`current` / `pending` / `check` render the plan; `upgrade` / `downgrade` render the
report; `adopt` renders the adopted table list).

`downgrade` **refuses to run without `--yes`** (exit `2`). It is a deliberate,
human-invoked action.

### CI gate

```yaml
- name: Fail if migrations are pending
  run: uv run varco migrate pending -t myapp.db:migrator
```

`pending` exits `1` when anything is pending — a one-line gate that catches "someone
changed a model and forgot to autogenerate".

### Pre-deploy job (the recommended shape)

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: myapp-migrate
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: myapp:{{ .Values.image.tag }}
          command: ["varco", "migrate", "upgrade", "-t", "myapp.db:migrator", "--to", "heads"]
          envFrom: [{ secretRef: { name: myapp-db } }]
```

…paired with `VARCO_MIGRATE_MODE=check` on the Deployment's own pods, so every pod proves
the schema is current before it serves. An `initContainer` running the same command is the
equivalent shape when you cannot express a pre-deploy hook.

---

## 12. Operations

### Where to run migrations — the decision table

| Deployment shape | Recommended | Why |
|---|---|---|
| Kubernetes / anything with pre-deploy hooks | pre-deploy `Job` running `varco migrate upgrade`, pods on `mode=check` | migration runs once, exactly, before any new pod starts; pods still refuse to serve a stale schema |
| Kubernetes without a pre-deploy hook | `initContainer` running `varco migrate upgrade`, pods on `mode=check` | same guarantee, expressed per-pod; the lock serialises the redundant runs |
| PaaS with a release phase (Heroku-style) | release command `varco migrate upgrade`, `mode=check` | the platform's own pre-deploy slot |
| PaaS with no hook at all, few replicas | `mode=upgrade` | the convenience this feature exists for |
| Single instance / dev / demo | `mode=upgrade` | no coordination problem to solve |
| CI | `varco migrate pending` as a gate | catches forgotten autogenerate |

**Do not run `mode=upgrade` at high replica counts.** 100 pods all waiting 30 s on one
lock is a convoy: the leader migrates while 99 pods burn their `lock_timeout`, re-plan,
and (usually) proceed — but any that re-plan while the leader is still working raise
`MigrationLockTimeout`, exit, and are restarted by the orchestrator. It converges, but it
is noisy and slow. `check` + a pre-deploy job has none of this.

### When a migration fails in production

1. **The database is at N-1.** `transaction_per_migration=True` means revisions 1…N-1
   committed and `alembic_version` reflects that. Nothing is half-applied *within* a
   revision, except the `autocommit_block()` case below.
2. **Fix forward. Never auto-downgrade.** Write the corrective revision. An automatic
   downgrade of a data-destructive revision is strictly worse than a failed deploy, which
   is why this feature will not do it for you.
3. **Re-run.** `upgrade heads` resumes at revision N.
4. **If revision N used `autocommit_block()`** (`CREATE INDEX CONCURRENTLY`,
   `ALTER TYPE … ADD VALUE` on older PG), check for an INVALID index left behind:
   ```sql
   SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;
   DROP INDEX CONCURRENTLY <name>;   -- manual; there is no automatic cleanup
   ```

### Postgres-side timeouts

Set these on the DDL connection so a migration blocked behind a long-running query fails
loudly rather than hanging startup forever:

| Setting | Suggested | What it bounds |
|---|---|---|
| `lock_timeout` | `10s` | how long a DDL statement waits for a table lock before giving up |
| `statement_timeout` | = `VARCO_MIGRATE_TIMEOUT` | how long any single statement may run |

Both can be set to `0` to disable. A DDL statement that waits indefinitely for a table
lock will queue every subsequent query on that table behind it — `lock_timeout` is what
turns that from an outage into a failed deploy.

### Taking a service out of the load balancer for a long migration

For a migration you know will take minutes:

1. Scale the deployment to 0, or mark the pods unready (fail the readiness probe).
2. Run `varco migrate upgrade` as a standalone job.
3. Scale back up with `VARCO_MIGRATE_MODE=check`.

`mode=upgrade` on a rolling deploy cannot express this — the migration runs while old
pods are still serving. For a schema change that is not backward-compatible with the
running code, that is a correctness problem, not a performance one. Prefer
backward-compatible (expand/contract) migrations wherever possible.

### Applying RLS in a revision

Row-Level Security DDL belongs in a reviewed revision, never a startup hook:

```python
from varco_sa.migration.ops import rls_upgrade, rls_downgrade


def upgrade() -> None:
    rls_upgrade(op, "orders")


def downgrade() -> None:
    rls_downgrade(op, "orders")
```

`rls_upgrade` renders exactly the statements `render_rls_ddl()` produces — same single
source of truth for the `(SELECT current_setting(..., true))` InitPlan form. Both are
no-ops with a logged WARNING on a non-Postgres dialect, so the same revision runs against
SQLite in CI. See [Postgres RLS](postgres-rls.md).

---

## 13. Installing

```bash
pip install "varco-sa[migrations]"      # adds alembic>=1.13
```

`alembic` is an **optional extra**. Installs that never touch migrations gain no
dependency. Importing `varco_sa.migration` without it raises
`MigrationBackendUnavailable` naming that exact `pip install` line.

`varco_beanie.migration` needs no extra — it uses the `pymongo`/`beanie` dependency the
package already has.

---

## 14. Edge-case reference

| Input / state | Behaviour |
|---|---|
| `VARCO_MIGRATE_MODE` unset | nothing registered, nothing runs — identical to before this feature |
| `VARCO_MIGRATE_MODE=upgrade`, no `MigrationsModule` in `modules=` | one WARNING naming the env var; nothing runs |
| `VARCO_MIGRATE_MODE=nonsense` | `ValueError` naming the three legal values |
| Virgin database, `mode="upgrade"` | full `upgrade heads` from base; both branches applied |
| Virgin database, `mode="check"` | `PendingMigrationsError` listing every revision; startup fails |
| N pods start simultaneously, `mode="upgrade"` | exactly one holds the lock and migrates; the others time out, re-plan, find nothing pending, and serve |
| N pods, migration outlasts `lock_timeout` | waiters re-plan, still find pending, raise `MigrationLockTimeout` → exit → orchestrator restarts them, by which time the leader has finished |
| Pod OOM-killed mid-migration | the lock transaction dies with the connection → lock released immediately; `alembic_version` sits at the last committed revision; the next pod resumes |
| Server/role sets `idle_in_transaction_session_timeout` | overridden by `SET LOCAL … = 0` inside the lock transaction |
| Revision 3 of 5 raises | 1-2 committed, `alembic_version` at 2, process exits non-zero, re-run resumes at 3 |
| `CREATE INDEX CONCURRENTLY` revision fails | may leave an INVALID index; manual `DROP INDEX` required |
| Existing DB built by `ensure_table()` | baseline revision's guards make `upgrade` a no-op; `adopt` stamps the branch head |
| `alembic upgrade head` (singular) with the framework branch | applies one branch only — always use `heads` |
| `alembic` not installed, `varco_sa.migration` imported | `MigrationBackendUnavailable` naming `pip install "varco-sa[migrations]"` |
| SQLite target for `migration_lock` | short-circuits to "acquired" with one INFO log (SQLite is single-writer) |
| Mongo lock holder crashes | reclaimable after `ttl`; `release()` is owner-fenced so the dead holder cannot delete the new holder's lock |
| Mongo `index_mode="create"` on a 200M-doc collection | builds the index at startup and blocks the lifespan — unsafe; default is `check` |
| Mongo migration with no `down()` | `downgrade` raises `IrreversibleMigrationError` |
| Mongo applied-record checksum mismatch | raises; `verify_checksums=False` opts out |
| Composite app, service B's migration fails | whole process aborts startup — documented composite fail-fast |
| Composite app, two services, one database | both use the default lock key and serialize automatically |
| `on_failure="warn"` and migrations fail | ERROR logged, startup continues, pod serves against a stale schema — the deliberately dangerous option |
| `dry_run=True` / `--dry-run` | renders what would run; no writes. Mongo `dry_run` lists what would run and applies nothing |

---

## 15. What this feature deliberately is not

- **Not a new migration engine.** Alembic is the Postgres engine; varco wraps it and
  never reimplements revision-graph resolution, autogenerate, or offline SQL rendering.
- **No autogenerate for MongoDB.** Hand-written `up()`/`down()` plus index
  reconciliation is the honest surface for a schemaless store.
- **No automatic downgrade on failure. Ever.**
- **No removal of `ensure_table()`** — reconciled (§7), not deleted.
- **No auto-enabling of RLS** — `rls_upgrade` makes it usable inside a reviewed
  revision; it never runs from a startup hook.
- **No data-migration/backfill framework.** Alembic revisions and Mongo `up(db)` can
  already run arbitrary DML; a resumable, throttled, progress-tracked batch orchestrator
  is a separate piece.
- **No multi-tenant schema-per-tenant fan-out.** The `lock_key` design leaves room for
  it; it is not built.

---

## 16. Known test-environment limitations

Two integration tests do not pass in a default containerised test environment. Neither
indicates a defect in the shipped code:

- `varco_sa/tests/test_migration_lock.py::test_two_migrators_concurrent_upgrade_exactly_one_applies`
  — the test constructs migrators with **zero** Alembic revisions registered, so neither
  migrator can apply anything and the "exactly one applies" assertion cannot hold. A
  test-authoring gap (a missing revision fixture). The other three lock-mechanics tests —
  including the `SET LOCAL idle_in_transaction_session_timeout` regression test that
  guards §4's core invariant — pass against real Postgres.
- `varco_sa/tests/test_rls_migration_integration.py` — the `testcontainers` Postgres
  default role is a **superuser**, and PostgreSQL superusers bypass RLS regardless of
  `FORCE ROW LEVEL SECURITY`. An environment limitation of the fixture's role setup. The
  statement-level regression guard (`test_rls_migration_ops.py`, which asserts
  `rls_upgrade` renders byte-identical DDL to `render_rls_ddl`, InitPlan form included)
  passes.

## Pitfalls

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **`mode="upgrade"` in a large multi-pod deployment** | Rolling deploys crawl; pods log `MigrationLockTimeout`, exit, and get restarted before eventually serving | Every replica races for one migration lock — the leader migrates while the rest burn `lock_timeout`, re-plan, and (if the leader is still working) raise | Deploy `VARCO_MIGRATE_MODE=check` on the pods and run `varco migrate upgrade` in a pre-deploy job / init container. `upgrade` is the small-deployment and dev convenience, not the production posture |
| **`ensure_table()` and migrations both active** | Alembic's `CREATE TABLE` fails against a table `ensure_table()` already created | The two mechanisms are mutually exclusive *per deployment*, and the hazard is directional (`create_all(checkfirst=True)` against an Alembic table is a harmless no-op; the reverse is not) | Run `varco migrate adopt` (or `AlembicMigrator.adopt_framework_tables()`) **once**, then pick one mechanism. Order matters: adopt, then upgrade |
| **`upgrade head` (singular) with the framework branch present** | Only the app's tables (or only the framework's) get migrated; the other branch silently stays behind | `varco_sa` ships its own Alembic branch (`branch_labels=("varco",)`) in the wheel, so there are **two** heads | Always `heads` (plural) — every varco command defaults to it. Or opt out with `include_framework_branch=False` + `get_target_metadata(..., include_framework=True)` |
| **`index_mode="create"` on a large Mongo collection** | Pod startup blocks for minutes-to-hours; on a replica set the build replicates and can stall secondaries | An index build is real work, and `create` runs it inside the ASGI lifespan at exactly the moment a rolling deploy starts N new pods | Leave `index_mode="check"` (the default, independent of `mode`) and run `varco migrate index -t … --create` as a pre-deploy job |
| **`VARCO_MIGRATE_MODE` set but no `MigrationsModule` in `modules=`** | Env var is set, nothing migrates, no error | `create_varco_app` has no migrator to run — it now logs one WARNING naming the env var rather than staying silent | Add `MigrationsModule(<AbstractMigrator>)` to `modules=`, listed before `*default_modules()` |
| **`from varco_core import MigrationError`** gets the wrong class | `except MigrationError` never catches a schema-migration failure | `varco_core.migrator` (domain data/field migration) already owns the top-level `MigrationError`/`MigrationPlan` names, so the schema-migration versions are deliberately **not** re-exported | `from varco_core.migration import MigrationError, MigrationPlan`. Every other migration name (`AbstractMigrator`, `Revision`, `MigrationReport`, `MigrationSettings`, `InMemoryMigrator`, the other exceptions) *is* on `varco_core` |
| **`varco migrate upgrade` without `--all-tenants` under db-per-tenant** | Only the global/framework schema advances; every tenant is left N revisions behind with no error | The default target is the single, non-fanned-out migrator | Use `varco migrate upgrade --all-tenants` (or `--tenant <id>` for one), which runs global-then-fan-out via `TenantFanoutMigrator` |
| **Global migration run after the tenant fan-out** | Tenant-table foreign keys to global tables fail mid-migration | `TenantFanoutMigrator` orders the global/framework run before every tenant's specifically to prevent this | Never pass `--skip-global` unless you have already run the global migration separately and know it is current |
