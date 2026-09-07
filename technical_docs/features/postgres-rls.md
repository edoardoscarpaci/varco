# Postgres Row-Level Security: the InitPlan cliff, `SET LOCAL`, and two fail-open seams

Plan 005, Phase 8 (gap U-5). Originally filed as a **report, not a request** —
"we build RLS ourselves"; `varco_sa/varco_sa/rls.py`'s two helpers remain a
per-table, opt-in primitive, not something any generated table gets by
default. RLS is wired by the *application's own* Alembic revisions, same as
day one.

Plan 007 (see [Multitenancy](multitenancy.md)) later added the actual
tenancy **layer** this document once described as absent: `TenantIsolation.
SHARED` (± `enforce_rls`) is one of three selectable isolation strategies,
alongside the schema-per-tenant and database-per-tenant strategies §3
below covers. RLS itself is unchanged by that — still assert-only, still
per-table, still opt-in.

## 1. The InitPlan finding — the 150× cliff

`current_setting()` is a **`VOLATILE`** Postgres function, and it is **not
`LEAKPROOF`**. Both properties matter to the planner independently of each
other, and together they explain a performance cliff that is invisible in
every functional test and catastrophic under production data volumes.

The obvious way to write a tenant-isolation policy is:

```sql
CREATE POLICY orders_tenant_isolation ON orders
    USING (tenant_id = current_setting('rls.tenant_id')::uuid);
```

This is **correct** — it returns the right rows — and it is also what causes
the planner to fall back to a **sequential scan** on every RLS-protected
query, regardless of whatever index exists on `tenant_id`. Because
`current_setting()` is volatile, Postgres cannot assume it returns the same
value for every row being evaluated within the same query, so it cannot
safely push the filter below an index scan the way it would push down a
literal or a stable-function result. The planner re-evaluates (or must assume
it might need to re-evaluate) the call per row.

The fix is a one-line rewrite — wrap the call in a scalar subquery:

```sql
CREATE POLICY orders_tenant_isolation ON orders
    USING (tenant_id = (SELECT current_setting('rls.tenant_id', true)::uuid));
```

Wrapping `current_setting(...)` in `(SELECT ...)` gives the planner an
**InitPlan**: a subquery the planner can prove is uncorrelated with the outer
query, so it is evaluated **once per query**, not once per row (or,
functionally, treated as if it were once per row without the rewrite). One
documented production query went from **8 100 ms to 94 ms** — an ~86×
improvement — from this rewrite alone, with the returned rows byte-identical
before and after.

**This is invisible in tests.** At development/test data volumes (dozens to
low thousands of rows), a sequential scan and an index scan both complete in
single-digit milliseconds — nothing in a functional test suite distinguishes
the two forms. The regression only shows up against a production-sized table,
which is exactly why it is dangerous: it ships clean and then costs an
incident.

> **Renamed in 3.0.0 (Plan 022 / AB-1):** this function was called
> `enable_rls_ddl()` until 3.0.0. It was never a member of the DI opt-in
> `enable_*` family — it touches no container, registers no binding and
> performs no I/O — so it is now `render_rls_ddl()`, which says what it does.
> `enable_rls_ddl` remains as a deprecated alias until 4.0.0.

**Any varco RLS helper MUST emit the `(SELECT …)` form.**
`varco_sa.rls.render_rls_ddl()` does this unconditionally — it is not a
configurable option, because there is no correct reason to emit the naive
form. `varco_sa/tests/test_rls.py` asserts the literal substring `"(SELECT "`
is present in the generated DDL as a permanent regression test; treat that
assertion as load-bearing, not decorative.

The second argument to `current_setting`, `true`, is the **missing-ok** flag:
`current_setting('rls.tenant_id', true)` returns `NULL` instead of raising
when the GUC has never been set on this connection — which is what makes "a
session that never called `set_tenant_local()` sees zero rows" the failure
mode, rather than every unscoped query raising a Postgres error.

**`NULLIF(..., '')` around the missing-ok form — the reset-to-empty-string
trap.** Postgres does not reset a `SET LOCAL`/`set_config(..., true)` GUC to
`NULL` when the transaction ends — it resets it to the **empty string**.
Without the `NULLIF` wrapper, the very next statement issued on a pooled
connection that previously ran a tenant-scoped transaction evaluates
`''::uuid` (or `''::text` for a `text`-cast policy) inside the InitPlan and
**raises** (`invalid input syntax for type uuid: ""`) instead of returning
zero rows — the opposite of "no tenant set, hide everything" and, on a
transaction-mode pooler, indistinguishable from a real outage the moment the
next logical caller reuses that connection. `render_rls_ddl()` therefore
emits:

```sql
USING (tenant_id = (SELECT NULLIF(current_setting('rls.tenant_id', true), '')::uuid))
```

`NULLIF(current_setting(...), '')` maps *both* "GUC never set on this
connection" (missing-ok `NULL`) and "GUC was set earlier this session, then
reset to `''` at the end of a `SET LOCAL` transaction" to the same `NULL`,
so the comparison never matches and the query fails **closed** — zero rows,
no crash — in either case. The wrapper stays inside the scalar subquery, so
the InitPlan optimisation from above is unaffected; this is additive, not a
different rewrite.

## 2. `SET LOCAL` vs `SET` — the same defect class as U-16

Setting the tenant GUC has two forms with very different pooling behaviour:

```sql
SET rls.tenant_id = '...';                         -- session-scoped
SELECT set_config('rls.tenant_id', '...', true);   -- transaction-scoped (is_local = true)
```

`SET` (or `set_config(..., false)`) is **session-scoped** — it survives past
the current transaction and is visible to every subsequent statement on that
physical connection until it is changed or the session ends. Under a
**transaction-mode** connection pooler (PgBouncer `pool_mode=transaction`,
pgcat, Supavisor in transaction mode), "the session" is a fiction: the
pooler returns the physical connection to its pool as soon as the
transaction commits, and the *next* logical caller to borrow that connection
inherits whatever GUC value was left set — silently applying the wrong
tenant's filter (or no filter, if RLS is bypassed by a stale unset value) to
someone else's queries.

This is **the same defect class as U-16**'s `SAAdvisoryLock` finding (see
`technical_docs/features/distributed-locks.md`): a construct whose safety
depends on "my session" being a stable, dedicated physical connection breaks
silently the moment a transaction-mode pooler is introduced between the
application and Postgres.

`set_config(..., true)` (`is_local = true`, i.e. `SET LOCAL`'s functional
equivalent as a callable) is scoped to the **current transaction only** — it
is unset automatically at `COMMIT`/`ROLLBACK`, regardless of what pooler sits
in front of the connection, because the reset happens as part of the
transaction boundary rather than relying on session lifetime.
`varco_sa.rls.set_tenant_local()` always uses this form — there is no
session-scoped variant offered, for the same reason `SAXactAdvisoryLock` is
the recommended primitive over the session-scoped `SAAdvisoryLock`: the
transaction-scoped form is pooling-safe unconditionally, and the
session-scoped form is only safe under a topology (direct connections, no
transaction-mode pooler) that is easy to assume and expensive to get wrong.

## 3. Schema-per-tenant: the supported mechanism is `schema_translate_map`, not `search_path`

`varco_sa/varco_sa/connection.py:236` sets `search_path` via
`server_settings={"search_path": self.schema_name}` **once, at connection
init time**, from a single deployment-wide `schema_name` setting. This is
correct and sufficient for the topology it targets: **one schema per
install**. It is not, and was never designed to be, a per-request or
per-tenant routing mechanism.

**Schema-per-tenant is now implemented** (`TenantIsolation.SCHEMA`,
Plan 007 — see [Multitenancy](multitenancy.md)), and the chosen routing
mechanism is `varco_sa.tenancy.router.SASchemaRouter`, built on SQLAlchemy's
**`schema_translate_map`** rather than `SET LOCAL search_path`:

```python
engine.execution_options(schema_translate_map={"tenant": "t_acme"})
```

`schema_translate_map` rewrites schema-qualified table references at the
SQL-**compile** layer, per session — it never touches the database
connection's session state at all, so it sidesteps the `SET`-vs-`SET LOCAL`
pooling hazard from §2 entirely rather than needing the transaction-scoped
form to stay safe. The decisive property over `SET LOCAL search_path`: a
table whose ORM class carries no symbolic schema token simply is not
translated, so a forgotten routing call **fails closed** — a compile/DB
error, not a silent read of the wrong tenant's schema. `SET LOCAL
search_path` would fail open on the same mistake: a session that never set
it silently falls back to the default schema and returns **another
tenant's rows, successfully**. That asymmetry is why `schema_translate_map`
is the primary mechanism and `SET LOCAL search_path` is kept only as a
documented, explicitly-opted-into escape hatch
(`SASchemaRouter(mechanism="search_path")`) for raw `text()` SQL that
`schema_translate_map` cannot reach — and even in that escape-hatch mode,
`SASchemaRouter` always emits `set_config(..., true)` (transaction-scoped),
**never** a bare session-scoped `SET`.

**Raw `text()` SQL is not translated by `schema_translate_map`** — it must
self-qualify its own schema. This is a real, only-partly-mitigable caveat;
see [Multitenancy](multitenancy.md) for the full guidance.

## 4. `TenantAwareService._scoped_params` fails open

`varco_core/varco_core/service/tenant.py:424`'s `_scoped_params` hook —
the mixin that prepends `tenant_id = <tid>` to every query issued through
`AsyncService.list()`/`.read()` for a `TenantAwareService` subclass — is
**application-layer** isolation. Stated plainly, because it is the whole
point of this section: **any query path that bypasses the mixin returns
cross-tenant rows.** This is not a bug in the mixin; it has no visibility
into queries that never go through it — a raw repository call from a script,
a report query built by hand, an admin tool, a future code path a reviewer
missed. `TenantAwareService` filters what passes through it and enforces
nothing below the service layer.

Postgres RLS is the correct **defense-in-depth** answer to this specific
fail-open surface under `TenantIsolation.SHARED` (± RLS): a policy applied
via `render_rls_ddl()` enforces tenant scoping at the database itself, so a
query that bypasses `TenantAwareService` still cannot see another tenant's
rows — the isolation no longer depends on every code path remembering to
filter correctly. RLS does not replace `TenantAwareService` (the mixin's
query shaping, scoped pagination, etc. are still needed at the application
layer); it closes the gap for the paths that skip it.

The **structural** fix for the same fail-open surface is `TenantIsolation.
SCHEMA` or `TenantIsolation.DATABASE` (§3 above, [Multitenancy](
multitenancy.md)): under those strategies a query that bypasses
`TenantAwareService` still cannot reach another tenant's rows, because
there *is* no other tenant's rows reachable from the routed
schema/connection — isolation is enforced by what the query can even see,
not by an application-layer filter or a database policy evaluated per row.
Pick RLS when tenants must stay in one shared schema (unbounded tenant
count, cheap to run); pick `SCHEMA`/`DATABASE` when a wrong query must
*error* rather than merely be *filtered*.

`assert_rls_enabled()` (`varco_sa.tenancy.rls_check`,
`TenancySettings(enforce_rls=True)`) is the automated version of "did I
apply RLS to every table that needs it" — it reads `pg_policies`/
`pg_class.relrowsecurity` and raises naming any table missing a policy. It
**skips `TenantScope.GLOBAL` tables and the ten framework tables** rather
than flagging them: a shared reference table legitimately carries no
`tenant_id` and needs no RLS policy, and without the skip the assertion
would report every such table as "missing a policy" and be unusable in any
deployment with global data. See [Multitenancy](multitenancy.md) for the
full `TenantScope` model.

## Applying RLS via a migration

RLS is DDL. It must be ordered after table creation and reviewed like any
other schema change, so it belongs in a **revision** — never in a startup
hook. Nothing in varco applies an RLS policy automatically, and there is no
mode, flag, or env var that makes it do so.

`varco_sa.migration.ops` provides the two operations (Plan 006 Phase 6):

```python
from alembic import op
from varco_sa.migration.ops import rls_upgrade, rls_downgrade


def upgrade() -> None:
    rls_upgrade(op, "orders")


def downgrade() -> None:
    rls_downgrade(op, "orders")
```

`rls_upgrade(op, table, *, tenant_column="tenant_id", policy_name=None,
setting="rls.tenant_id")` issues `op.execute()` for each statement
`render_rls_ddl()` builds — it does not duplicate the SQL, so the
InitPlan-form `(SELECT current_setting(..., true))` guarantee of §1 holds
identically here. `varco_sa/tests/test_rls_migration_ops.py` asserts the
rendered statements match `render_rls_ddl()`'s output exactly; treat that
assertion as load-bearing.

`rls_downgrade(op, table, *, policy_name=None)` issues the matching
`DROP POLICY IF EXISTS` + `ALTER TABLE … DISABLE ROW LEVEL SECURITY`. Pass
the same `policy_name` you passed to `rls_upgrade` if you overrode the
default `f"{table}_tenant_isolation"` scheme.

**Both are no-ops with a logged `WARNING` on a non-PostgreSQL dialect**
rather than raising, so a project that runs the same revisions against
SQLite in CI and PostgreSQL in production does not crash on the SQLite leg.
This also means a CI run proves nothing about the policy — the RLS
integration test needs real PostgreSQL **and a non-superuser role**
(superusers bypass RLS regardless of `FORCE ROW LEVEL SECURITY`).

See [Schema migrations](schema-migrations.md) for how revisions are applied,
the framework Alembic branch, and the operations guidance around running DDL
in production.

## Using the helpers directly

```python
from varco_sa.rls import render_rls_ddl, set_tenant_local


# The lower-level form — rls_upgrade() above wraps exactly this. Use it when
# you need the raw statements (inspection, a non-Alembic migration tool):
def upgrade() -> None:
    for stmt in render_rls_ddl("orders"):
        op.execute(stmt)


# In request/transaction setup, before issuing tenant-scoped queries:
async with session.begin():
    await set_tenant_local(session, tenant_id)
    # ... queries within this transaction only see tenant_id's rows
```

`render_rls_ddl(table, *, tenant_column="tenant_id", setting="rls.tenant_id",
policy_name=None, cast_type="uuid")` returns three DDL statements, **in this
order** (reordered in Plan 037 / S12a — see "Statement order: policy before
enable" below): `CREATE POLICY` with the InitPlan-form `USING`/`WITH CHECK`
clause, then `ENABLE ROW LEVEL SECURITY`, then `FORCE ROW LEVEL SECURITY`
(without `FORCE`, Postgres exempts the table owner — often the
migration/ORM role — from the policy entirely, which is itself a silent
bypass — **but see the superuser caveat below**, `FORCE` does not close
every exemption). It performs no I/O — the caller runs the statements
inside their own Alembic revision.

`cast_type` controls what Postgres type the (always-`text`) GUC value is
cast to before comparison with `tenant_column`: default `"uuid"` matches the
common case of a real `UUID` tenant column. **Pass `cast_type="text"` for a
`VARCHAR`/`TEXT` tenant column** — a mismatched cast aborts the migration
with `operator does not exist: character varying = uuid`. This is not a
theoretical footgun: varco's own two framework tables
(`varco_audit_log`, `varco_dead_letters`) declare `tenant_id` as
`String(255)` (`AuditEntry.tenant_id`/`DeadLetterEntry.tenant_id` are
`str | None`, never `UUID`), so `varco_sa.rls_framework.framework_rls_upgrade()`
defaults to `cast_type="text"` rather than inheriting `render_rls_ddl()`'s
`"uuid"` default — see `varco_sa/varco_sa/rls_framework.py`. Before this
parameter existed, `framework_rls_upgrade()` could not apply at all: every
call aborted with the cast error above.

**RLS does not stop a superuser or a `rolbypassrls` role — this is a hard
Postgres rule, not a varco gap.** `FORCE ROW LEVEL SECURITY` only revokes
the *table-owner* exemption; `rolbypassrls`/superuser connections bypass RLS
**unconditionally**, `FORCE` or not. A test (or an operator) that connects
as the database's own superuser role — the default for most local/CI
Postgres containers — will see RLS appear to do nothing, not because the
policy is broken but because the connecting role was never subject to it.
Verify RLS behaviour (and write RLS integration tests) using a dedicated
non-superuser, non-`BYPASSRLS` application role; see
`varco_sa/tests/test_rls.py`/`test_framework_rls.py` for the fixture that
provisions one.

`set_tenant_local(session, tenant_id, *, setting="rls.tenant_id")` executes a
single `SELECT set_config(:setting, :value, true)` — call it as the first
statement inside the transaction whose queries should be tenant-scoped; the
setting does not survive past that transaction (see §2), by design.

## What is opt-in and what is not

Nothing in varco applies an RLS policy to any table it generates.
`render_rls_ddl()` is a pure DDL-string generator, and `rls_upgrade()` only
runs when an application's own revision calls it — a table has no RLS at
all until an application writes a revision that does so and runs it. In
particular, no `VARCO_MIGRATE_MODE` value enables RLS: `mode="upgrade"`
applies whatever revisions exist, and a revision that does not call
`rls_upgrade()` will never produce a policy. This keeps RLS adoption a per-table, per-application decision
matching each table's actual isolation requirements, rather than a
one-size-fits-all default that could break a table that legitimately needs
cross-tenant reads (e.g. an admin reporting table).

## Statement order: policy before enable (Plan 037 / S12a, §D-S12-order)

Brief 007 §5 is direct: *"When RLS is enabled without a policy, a **default-deny policy**
applies — all rows become invisible and immutable to non-superuser roles"*, and *"If policies
are created before RLS is enabled, no gap exists."* `render_rls_ddl()` therefore returns, in
order: `CREATE POLICY`, `ENABLE ROW LEVEL SECURITY`, `FORCE ROW LEVEL SECURITY` — the same three
statements as before Plan 037, only reordered. `CREATE POLICY` on a table with RLS still
disabled is legal Postgres — the policy has no effect until `ENABLE` runs.

This matters because `render_rls_ddl()` is a **documented standalone generator** (see "Using the
helpers directly" above): a caller that does not run all three statements inside one transaction
previously had an unbounded default-deny window between `ENABLE` and `CREATE POLICY`. Every
in-repo caller already executes the whole list in order, so this is not a behaviour change for
them; a caller that indexes the returned list positionally (`render_rls_ddl(t)[0]`) now gets a
different statement — see the CHANGELOG's `### Changed` entry.

## Nullable tenant columns are refused, not silently hidden (§D-S12-nullable)

`tenant_id = (SELECT NULLIF(current_setting(...), '')::t)` is `NULL` — never `TRUE` — for a row
whose `tenant_id` IS `NULL`. Enabling RLS on a table with a nullable tenant column therefore makes
every untenanted row **invisible to every connection, permanently, with no error**. Three
framework tables are in exactly that shape (`varco_dead_letters`, `varco_schedules`, the
encryption-key-store table).

`varco_sa.rls_autogen.plan_tenant_rls()` inspects the column and raises `ValueError` (naming the
table, the column, and both remedies) when the tenant column is nullable, unless the caller opts
explicitly into `NullTenantPolicy.VISIBLE` (fail-open — adds `OR {col} IS NULL` to both `USING`
and `WITH CHECK`) or `NullTenantPolicy.HIDDEN` (today's `render_rls_ddl()` behaviour, spelled out
explicitly rather than silently inherited). `render_rls_ddl()` itself is unchanged — it takes a
table *name* and has no column to inspect; the check lives at the generator, where the
`sqlalchemy.Table` and `column.nullable` are actually available.

## The generated-for-you DDL path: `varco_sa.rls_autogen` (Plan 037 / S12b)

Working out each table's `cast_type` and nullability by hand does not scale past a handful of
tables. `varco_sa.rls_autogen` walks a set of domain classes and produces an inspectable,
printable plan before any DDL exists:

```python
from varco_sa.rls_autogen import (
    NullTenantPolicy, plan_tenant_rls, render_tenant_rls_ddl,
    tenant_rls_upgrade, tenant_rls_downgrade,
)

# In code review, before writing any DDL:
plans = plan_tenant_rls([User, Order, Invoice], base=Base)
print(plans)  # every skip is visible; resolve them before proceeding

# In an application's own reviewed Alembic revision:
def upgrade() -> None:
    tenant_rls_upgrade(op, plans=plans)

def downgrade() -> None:
    tenant_rls_downgrade(op, plans=plans)
```

`plan_tenant_rls()` reads `ParsedMeta.tenant_scope` — only `TenantScope.TENANT` classes are
candidates; a `TenantScope.GLOBAL` class is silently absent from the result (never a skip entry).
It derives each table's Postgres cast from the column's SQLAlchemy type (`Uuid`/`UUID` → `uuid`;
`String`/`Text`/`Unicode` → `text`; `Integer`/`BigInteger` → `bigint`) — an unmappable type is a
`skipped_reason`, never a guess, matching the same already-experienced footgun `cast_type`
exists to prevent (see "Using the helpers directly" above). A `TENANT`-scoped class with no
tenant column, or one never registered with `SAModelFactory`, is also a `skipped_reason` — never
a silent omission and never a `KeyError`. `varco_tenants` is hard-excluded, always — its
`tenant_id` is the table's primary key, not a filterable column.

Every statement `render_tenant_rls_ddl()` emits still comes from `varco_sa.rls.render_rls_ddl()`
— the InitPlan form is never re-derived in `rls_autogen`. **No revision ships in `varco_sa` for
this** — see "What is opt-in and what is not" above and CLAUDE.md's Rule: enabling RLS, including
on varco's own framework tables, stays an application-authored, reviewed revision.

## Automatic tenant GUC-setting: `install_rls_tenant_hook` (Plan 037 / S12c, §D-S12-hook)

Calling `set_tenant_local()` by hand at every transaction boundary is easy to forget, and easy to
get subtly wrong across a commit: `set_config(..., true)` is scoped to the transaction, so a
session that commits and then issues another query on the same connection has **no tenant set**
unless something re-sets it. `varco_sa.tenancy.rls_session.install_rls_tenant_hook()` wires this
automatically via a SQLAlchemy **`after_begin`** event listener, which brief 007 §6 names as the
supported SQLAlchemy 2.x hook: *"fires at the start of every transaction, including nested
transactions"*, registered on the **sync `Session` class** so `AsyncSession` inherits it.

```python
from sqlalchemy.ext.asyncio import AsyncSession
from varco_sa.tenancy.rls_session import install_rls_tenant_hook

uninstall = install_rls_tenant_hook(AsyncSession, require_tenant=False)
# every transaction opened on any session built from AsyncSession now sets
# rls.tenant_id from current_tenant() the moment its first statement runs
```

This covers all three ways a session is produced in `varco_sa` — `SQLAlchemyUnitOfWork`,
`SQLAlchemyRepositoryProvider.get_repository()`, and app code holding the session factory
directly — where an imperative call from `SQLAlchemyUnitOfWork._begin()` alone would miss the
latter two and silently break after the first `commit()` in a session. Turn it on with
`TenancySettings(rls_set_tenant=True)` (env `VARCO_TENANCY_RLS_SET_TENANT`); with
`rls_require_tenant=True` (env `VARCO_TENANCY_RLS_REQUIRE_TENANT`) the hook raises `RuntimeError`
naming `tenant_context()` instead of clearing the GUC when no tenant is ambient — off by default
because a background job, an `OutboxRelay` poll, a migration, and a health check all legitimately
run with no tenant. A non-Postgres dialect: the hook still installs but skips the `set_config()`
call with one `WARNING` per engine.

## Connection pooler survival (brief 007 §4)

`set_tenant_local()`'s `set_config(..., true)` form is the transaction-scoped ("`SET LOCAL`"-
equivalent) primitive §2 above already argues for. Its safety under a pooler still depends on
which pooler:

| Pooler | `SET LOCAL`/`set_config(..., true)` | Notes |
|---|---|---|
| PgBouncer, transaction mode | ✅ safe | Reverted on `COMMIT`/`ROLLBACK` before the connection returns to the pool — the default, recommended shape |
| RDS Proxy | ⚠️ session-pinning | `SET LOCAL` causes RDS Proxy to **pin** the connection to the client until `RESET ALL`, defeating multiplexing for that client — a performance cost, not a correctness one |
| Supavisor | ⚠️ untested | Supavisor's documented multi-tenant pattern embeds the tenant in the connection **username**, not a session variable; `SET LOCAL` behaviour under Supavisor is not documented — test thoroughly before relying on it, or use its tenant-in-username pattern instead |

`rls_set_tenant` is opt-in specifically so an RDS Proxy deployment can decline the hook and keep
app-layer scoping (`TenantAwareService`) plus `enforce_rls` assertions instead.

## Applying RLS to an existing table: locking and rollback

`ALTER TABLE ... ENABLE ROW LEVEL SECURITY` takes an **`AccessExclusiveLock`** — brief 007 §5:
*"the most restrictive lock level ... held briefly — typically milliseconds for a small table"*.
No measurement exists for a very large table (an open evidence gap), so apply in a maintenance
window, largest tables last, with a short `lock_timeout`. This is exactly why no revision ships
for any table — including varco's own framework tables — from this repository: the operator, not
the framework, decides the maintenance window.

**Adopting RLS on an existing database, in order:** read `inspect_rls_posture()` first (stop if
`is_superuser`/`rolbypassrls` is `True` for the app role — a policy would be a no-op); turn on
`rls_set_tenant` and deploy *before* writing any policy (a harmless extra `set_config` per
transaction with no policies yet, and it means the next step cannot take the app to zero rows);
review `plan_tenant_rls()`'s output and resolve every `skipped_reason`/nullable column; apply one
reviewed revision calling `tenant_rls_upgrade(op, plans=...)`; verify as the app role, never as a
superuser.

**Rollback.** `tenant_rls_downgrade(op, plans=...)` (`DROP POLICY IF EXISTS` +
`DISABLE ROW LEVEL SECURITY`) restores full visibility and takes the same brief lock. Safe to run
with the hook still installed. Rolling back only the application while leaving policies in place
is also safe *provided* `rls_set_tenant` stays on — remove the policies before removing the
GUC-setter, never the other way round.

## Finding a silent no-op: `inspect_rls_posture()` (Plan 037 / S12d, §D-S12-posture)

`assert_rls_enabled()` only reads `pg_class.relrowsecurity` — a table can pass that check and
still be fully unprotected if the connecting role bypasses RLS unconditionally
(superuser/`BYPASSRLS`), or owns the table without `FORCE`. Brief 007 §1 names this *"the cause
of production RLS failures"*.

```python
from varco_sa.tenancy.rls_check import inspect_rls_posture

posture = await inspect_rls_posture(conn, tables=["orders", "invoices"])
posture.is_superuser       # bypasses RLS unconditionally, on every table
posture.rolbypassrls       # same unconditional bypass, without being a superuser
posture.owned_tables       # bypasses unless that table's FORCE bit is set
posture.tables["orders"]   # TablePosture(rls_enabled, rls_forced, has_policy)
```

`inspect_rls_posture()` **never raises** — it is a report, and it logs one `WARNING` per finding.
`assert_rls_enabled()`'s raise condition is unchanged by its existence: making it raise on a
missing `FORCE` or a bypassing role would be an upgrade-time behaviour change for every existing
`enforce_rls=True` deployment, and would fail every local/CI Postgres container, whose default
role *is* a superuser. Plan 036's `SecurityPosture` preflight is the intended consumer of this
report.

## Pitfalls

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **Hand-written RLS policy uses bare `current_setting(...)`** | A query on an RLS-protected table that flies at test data volumes goes from milliseconds to seconds in production (one documented case: 8 100 ms) | `current_setting()` is `VOLATILE` and not `LEAKPROOF` — without a scalar-subquery wrapper the Postgres planner cannot push the predicate below an index scan and falls back to a sequential scan | Always use `varco_sa.rls.render_rls_ddl()`, which emits the `(SELECT current_setting(..., true))` InitPlan form; never hand-write `USING (tenant_id = current_setting(...)::uuid)` — see `technical_docs/features/postgres-rls.md` |
| **RLS tenant GUC set with `SET` instead of `SET LOCAL`** | Under a transaction-mode pooler (PgBouncer), one tenant's queries leak into a session that was actually serving a different tenant's next transaction | Session-scoped `SET`/`set_config(..., false)` survives past the transaction on a pooled connection — same defect class as `SAAdvisoryLock`'s session-scoped release (U-16) | Use `varco_sa.rls.set_tenant_local(session, tenant_id)` — `set_config(..., true)` (`is_local`) scopes the setting to the current transaction only |
| **`TenantAwareService._scoped_params` bypassed** | Cross-tenant rows returned from a query path that skipped the service mixin (e.g. a raw repository call, an ad-hoc script) | The mixin fails open by design — it only filters queries that actually go through it, there is no enforcement below the service layer | Enable Postgres RLS as defense-in-depth (`varco_sa.rls.render_rls_ddl()`) on any table where a query bypassing the service layer would leak data across tenants |
| **`render_rls_ddl()` on a `VARCHAR`/`TEXT` tenant column** | Every migration using the policy aborts with `operator does not exist: character varying = uuid` — this is exactly what made `varco_sa.rls_framework.framework_rls_upgrade()` inapplicable before its fix | `render_rls_ddl()`'s `cast_type` defaults to `"uuid"`, matching a real `UUID` tenant column; a `String`/`VARCHAR` column needs the GUC cast to match | Pass `cast_type="text"` (`render_rls_ddl(..., cast_type="text")`); `framework_rls_upgrade()` already does this for the two framework tables, whose `tenant_id` is `String(255)` |
| **RLS test/connection uses a superuser role** | RLS policies appear to do nothing — every row is visible regardless of the tenant GUC — even though `pg_class.relforcerowsecurity` is `True` and the policy is correctly applied | `FORCE ROW LEVEL SECURITY` only revokes the *table-owner* exemption; `rolbypassrls`/superuser connections bypass RLS **unconditionally**, `FORCE` or not — this is a hard Postgres rule, not a varco gap | Connect (and write RLS tests) as a dedicated non-superuser, non-`BYPASSRLS` application role — see `varco_sa/tests/test_rls.py`/`test_framework_rls.py`'s fixture |
| **RLS enabled by a startup hook** | Policies appear/disappear depending on which process booted last; unreviewed DDL in production | RLS is schema DDL that must be ordered after table creation and reviewed like any other change | Put it in a reviewed revision with `varco_sa.migration.ops.rls_upgrade(op, "orders")` / `rls_downgrade`. Nothing in varco auto-enables RLS, and no `VARCO_MIGRATE_MODE` value does either |
| **Owner-bypass no-op** | RLS looks fully configured (`relrowsecurity=True`, a correct policy) but a query from the migration/app role still returns every tenant's rows | The connecting role **owns** the table and `FORCE ROW LEVEL SECURITY` was never applied — Postgres exempts owners from RLS by default | Run `varco_sa.rls_autogen.tenant_rls_upgrade`/`render_rls_ddl()` as shipped (they always include `FORCE`); check `inspect_rls_posture().owned_tables` and `.tables[t].rls_forced` to confirm |
| **`FORCE` is set but the role is `BYPASSRLS`/superuser anyway** | Every query from that connection sees every tenant's rows despite `relforcerowsecurity=True` and a correct policy | `BYPASSRLS`/superuser roles bypass RLS **unconditionally** — `FORCE` only revokes the table-owner exemption, nothing more | `inspect_rls_posture().is_superuser`/`.rolbypassrls` — if either is `True` for the app's connecting role, fix the role (own tables with a privileged migration role, run the app as a non-privileged role) before trusting any policy |
| **Policy-before-enable ordering violated** (only relevant to hand-written DDL, not `render_rls_ddl()`) | Every row on the table becomes invisible and immutable to non-superuser roles for the duration of the window | `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` applies a **default-deny** policy the instant it runs if no policy exists yet | Always emit `CREATE POLICY` before `ENABLE`/`FORCE` — `render_rls_ddl()`/`render_tenant_rls_ddl()` already return statements in this order (Plan 037 / §D-S12-order); never hand-order RLS DDL differently |
| **Nullable tenant column → rows invisible forever** | Rows whose tenant column is `NULL` vanish from every query, on every connection, with no error — permanently | `NULLIF(current_setting(...), '')` is `NULL`, never `TRUE`, for a `NULL` tenant column — the comparison never matches | `plan_tenant_rls()` refuses (raises `ValueError`) by default; choose `NullTenantPolicy.VISIBLE` (fail-open, `OR col IS NULL`) or `NullTenantPolicy.HIDDEN` (today's behaviour, explicit) deliberately, per table |
| **RDS Proxy session pinning** | Connection pool utilization climbs; RDS Proxy stops multiplexing connections for clients that set the tenant GUC | `SET LOCAL`/`set_config(..., true)` causes RDS Proxy to **pin** the connection to that client until `RESET ALL` | Known tradeoff, documented in the pooler table above; `rls_set_tenant` is opt-in, so an RDS Proxy deployment can decline it and keep app-layer scoping + `enforce_rls` assertions instead |
| **`current_tenant()` value doesn't match the tenant column's type** | The very first query in the transaction raises `invalid input syntax for type uuid: "acme"` instead of the documented "RLS hides every row" | `render_rls_ddl()`'s InitPlan form casts the GUC (always `text`) to `cast_type` — a non-UUID tenant id against a `uuid`-cast policy fails the cast, not the comparison | Ensure `current_tenant()` always yields a value the protected column's type can parse (a real UUID string for a `uuid` column); the fix is the tenant-id format, never the policy |
