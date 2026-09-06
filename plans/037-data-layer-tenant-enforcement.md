# Plan 037 — Data-layer tenant enforcement: RLS by default (S12) + an AST tenant-filter guard (S15)

Covers BACKLOG 3.2 rows **S12** (🟡 should, M — *RLS-by-default for `TenantScope.TENANT` tables*)
and **S15** (🟢 nice, M–L — *Applicator-level tenant-filter assertion*), and answers the cycle's
**open question 4** (§D-S12-oq4).

**Research brief backing this plan:**
`design/research/007-postgres-rls-enforcement-mechanics.md`, written for these two rows. Every
externally-grounded claim below cites it by section. Brief 006's Evidence Gap 2 is cited once,
for S15.

## Scope and siblings

One of five plans in the 3.2 security release. This slice is deliberately isolated because it is
**the risky, hard-to-reverse one**: it generates DDL and touches an existing database. It must be
reviewable and mergeable entirely on its own.

| Plan | Rows | Boundary with this plan |
|---|---|---|
| 033 | S6, S5, S16 | **Owns how `current_tenant()` gets SET.** This plan treats `current_tenant()` as an *input contract* and changes nothing about how it is populated |
| 034 | S1, S2, S13, S14 | No overlap |
| 035 | S3, S7, S8, S10 | No overlap |
| 036 | S4, S9, S11 | **Owns the `SecurityPosture` preflight.** §D-S12-posture defines and exports the RLS posture *check*; 036 builds the harness that reports it. This plan must not build a preflight |

**The locked sequencing decision is binding and is not relitigated here** (`BACKLOG.md:49`):
S12 lands first as the production backstop; S15 is the portable dev-time guard and **may slip**;
shipping S15 alone is rejected. Phases 0–4 are S12. Phase 5 is S15 and is **separable and
droppable** — nothing in Phases 0–4 imports it, and §D-S15-cut states the exact fallback.

## Goal

A varco app on Postgres can turn on database-enforced tenant isolation for every
`TenantScope.TENANT` table with **one call in a reviewed Alembic revision** plus **one settings
flag**, instead of hand-writing a policy per table and remembering `set_tenant_local()` at every
transaction boundary. When the app forgets a tenant filter, Postgres — not application code —
refuses to return the rows. When the deployment has silently defeated its own policies by
connecting as a `BYPASSRLS`/superuser role, an operator can find out from a supported check
instead of from an incident.

## Non-goals

- **Nothing is enabled by default, in this plan, for anybody.** `TenancySettings()` stays
  `isolation=SHARED`, `enforce_rls=False`; the two new flags default `False`; no revision is
  auto-generated; no DDL is emitted at startup, ever. An app that upgrades and changes no code
  gets **byte-identical behaviour** — restated per phase in §Migration and upgrade note.
- **No startup DDL.** `technical_docs/features/postgres-rls.md`'s "RLS enabled by a startup hook"
  pitfall stands unchanged. "Generated-for-you" means *the statements are generated for you*, not
  *the statements are applied for you*.
- **No Mongo/Beanie RLS.** RLS is a Postgres feature; there is no equivalent. `varco_beanie` is
  touched only by Phase 5 (S15), which is portable by construction.
- **No `SecurityPosture` preflight** — Plan 036 owns it (§D-S12-posture).
- **No change to how `current_tenant()` is populated** — Plan 033 owns it.
- **No compiled-SQL analyzer.** Rejected on evidence in §D-S15-shape.
- **No new conformance module.** The five `testkit/varco_conformance` suites cover `varco_core`
  ABCs; RLS is a Postgres-only concern behind no ABC, and Phase 5's guard is a pure function.
  No new implementation of one of the five ABCs appears here, so **no `COVERAGE.md` row is
  owed** (CLAUDE.md's rule triggers on a new ABC implementation, which this is not). Stated so
  the absence is a recorded decision rather than an oversight.

---

## Design

### What already exists — and the two backlog corrections it forces

Scout-verified, spot-checked against source while writing this plan:

| Fact | Location | Consequence for S12 |
|---|---|---|
| `render_rls_ddl()` already emits **ENABLE + FORCE + CREATE POLICY**, in the `(SELECT …)` InitPlan form, with a per-call `cast_type` | `varco_sa/varco_sa/rls.py:73-182` | S12 is **not** "add FORCE". It is **wiring and defaulting** |
| `set_tenant_local()` already uses `SELECT set_config(:setting, :value, true)` with **bound parameters** | `varco_sa/varco_sa/rls.py:226-231` | Already the pooler-safe, injection-safe form brief 007 §3/§4 prescribes. **Verify, do not replace** |
| `assert_rls_enabled()` reads `pg_class`/`pg_policies`, skips `GLOBAL` + framework tables, raises `TenantIsolationError` when `enforce=True` | `varco_sa/varco_sa/tenancy/rls_check.py:51-129` | The startup assertion exists. It checks `relrowsecurity` but **not `relforcerowsecurity`**, and **nothing about the connecting role** |
| `framework_rls_upgrade()` covers exactly two tables, hand-listed | `varco_sa/varco_sa/rls_framework.py:38` | `FRAMEWORK_RLS_TABLES = ("varco_audit_log", "varco_dead_letters")` is **stale** — at least `varco_schedules`, `varco_webhook_subscriptions` and the encryption-key-store table also carry `tenant_id` (`schedule.py:67`, `webhook.py:61`, `encryption_store.py:100`) |
| Framework `tenant_id` columns are `String(255)`, not `uuid`, and several are **nullable** | `dlq.py:123`, `schedule.py:67`, `encryption_store.py:100` (nullable); `webhook.py:61` (not null) | A generator must derive `cast_type`, and must have an answer for nullable tenant columns (§D-S12-nullable) |
| The SA repository's four query sites use `SQLAlchemyQueryCompiler` **directly** — never `SQLAlchemyQueryApplicator` | `varco_sa/varco_sa/repository.py:190, 225, 301, 489` | ⚠️ **Correction 3, and it is new**: an "applicator-level" assertion would not cover varco's own read path at all. See §D-S15-hook |

### Phase order

```
P0  S12a  🟡 S  correctness of the existing primitives — statement ORDER, nullable
                tenant columns, a derived (not hand-listed) framework table set
P1  S12b  🟡 M  varco_sa.rls_autogen — the generated-for-you DDL path
P2  S12c  🟡 M  automatic set_tenant_local() via SQLAlchemy after_begin  ← the wiring row
P3  S12d  🟡 S  inspect_rls_posture() — the BYPASSRLS / owner / FORCE footgun made findable
P4  ——    🟡 S  docs, README, CLAUDE.md, CHANGELOG, api-surface, BACKLOG OQ4  (same commit)
────────────────────────── S12 ends here; everything below is droppable ──────────────────────
P5  S15   🟢 L  varco_core.query.applicator.tenant_guard — opt-in, dev-time, AST-level
```

**P0 is first because it is the only phase that changes an existing output**, and every later
phase builds on it. If the plan is cut short it must not be cut between P1 (which generates
policies) and P2 (which sets the GUC those policies read) — a deployment with policies and no
GUC-setter sees **zero rows on every query**. That pair is the one indivisible seam here.

### §D-S12-order — policies BEFORE enable. This is the migration-ordering landmine, and today's order is wrong.

`render_rls_ddl()` returns, in order (`rls.py:174-182`):

```
1. ALTER TABLE t ENABLE ROW LEVEL SECURITY
2. ALTER TABLE t FORCE ROW LEVEL SECURITY
3. CREATE POLICY t_tenant_isolation ON t USING (…) WITH CHECK (…)
```

Brief 007 §5 is explicit: *"When RLS is enabled without a policy, a **default-deny policy**
applies — all rows become invisible and immutable to non-superuser roles"*, and *"If policies
are created before RLS is enabled, no gap exists."*

Inside one Alembic revision the whole list runs in one transaction and the window is invisible.
But `render_rls_ddl()` is **documented as a supported standalone generator** for exactly the
callers who do not have that transaction —
`technical_docs/features/postgres-rls.md`'s "Using the helpers directly" section, and its own
docstring's *"for a non-Alembic migration tool"*. For those callers the current order opens a
default-deny window of unbounded length between statement 1 and statement 3.

| ID | Choice | Consequence |
|---|---|---|
| D-S12-order | **Reorder to `CREATE POLICY`, `ENABLE`, `FORCE`.** Same three statements, same text, different index | Every documented call shape becomes gap-free. `CREATE POLICY` on a table with RLS still disabled is legal Postgres — the policy simply has no effect until `ENABLE` |

**DESIGN: reorder the returned list rather than documenting the hazard**

✅ Brief 007 §5's ordering rule, applied at the one place that can enforce it for every caller.
✅ The failure the current order can produce is a **total outage of the table** (default-deny),
   which is the worst-shaped failure in this plan. A doc note does not prevent it.
✅ Contents are byte-identical; only order changes. No caller that executes the whole list in
   order is affected, and that is every in-repo caller (`migration/ops.py:74-80`,
   `rls_framework.py:89-91`).
❌ A caller that indexes the list (`render_rls_ddl(t)[0]`) silently gets a different statement.
   ⚠️ ASSUMPTION, filed in Risks: no such consumer exists. In-repo verification is Step 1's grep;
   out-of-repo consumers get a `### Changed` CHANGELOG entry.
❌ Reversing it in `rls_downgrade` is unaffected (it already drops the policy first,
   `migration/ops.py:110-112`).
  Rejected — **leave the order and document the hazard**: ❌ the hazard is silent and total, and
  the caller who would read the doc is the one already reading a generator's return value.
  Rejected — **return a `dataclass` with named statements instead of a list**: ❌ a signature
  change to a public, snapshot-tracked function to fix an ordering bug; disproportionate, and it
  breaks every existing `for stmt in render_rls_ddl(...)` loop.

### §D-S12-nullable — a nullable tenant column is refused, not silently hidden

`tenant_id = (SELECT NULLIF(current_setting(…), '')::t)` is `NULL` — never `TRUE` — for a row
whose `tenant_id` is `NULL`. So enabling RLS on a table with a nullable tenant column makes every
untenanted row **invisible to every connection**, permanently, with no error. Three framework
tables are in exactly that shape today (`dlq.py:123`, `schedule.py:67`,
`encryption_store.py:100`), and `framework_rls_upgrade()` already produces that outcome for
`varco_dead_letters` — a dead letter recorded outside a tenant context becomes unreadable.

| ID | Choice | Consequence |
|---|---|---|
| D-S12-nullable | The **generator** (Phase 1) inspects the column and **raises `ValueError`** naming the table, the column and both remedies when the tenant column is nullable, unless the caller passes `null_tenant=NullTenantPolicy.VISIBLE` (adds `OR {col} IS NULL` to `USING`/`WITH CHECK`) or `NullTenantPolicy.HIDDEN` (today's behaviour, explicitly chosen). `render_rls_ddl()` itself is **unchanged** — it takes a table *name* and cannot inspect anything | The footgun is caught where the information exists, and the choice is recorded in the app's own revision |

**DESIGN: refuse by default at the generator, keep `render_rls_ddl()` a dumb string builder**

✅ The generator has the `sqlalchemy.Table` and therefore `column.nullable`; `render_rls_ddl()`
   has a `str`. Putting the check where the data is avoids inventing a schema-reflection
   dependency inside a pure function.
✅ `VISIBLE` is fail-**open** for untenanted rows and must never be a default; making it an
   explicit enum value spelled at the call site is the point.
✅ Existing `framework_rls_upgrade()` behaviour is untouched (it takes names, calls
   `render_rls_ddl` directly, `rls_framework.py:87-91`) — no upgrade surprise.
❌ Two ways to get RLS DDL now (name-based and metadata-based). Accepted and documented: the
   name-based one is the escape hatch for tables varco did not generate.
  Rejected — **always emit `OR col IS NULL`**: ❌ silently fail-open; a row with no tenant becomes
  readable by every tenant, which is the exact leak this plan exists to prevent.
  Rejected — **make the tenant column `NOT NULL` in a migration**: ❌ this plan does not alter
  existing tables (§D-S12-oq4), and a backfill of unknown tenant ids is not something a framework
  can do for an application.

### §D-S12-autogen — one call, all `TENANT` tables, cast type derived per table

New module `varco_sa/varco_sa/rls_autogen.py`:

```python
@dataclass(frozen=True)
class RlsTablePlan:
    table: str
    tenant_column: str
    cast_type: str          # derived from the column's SA type
    nullable: bool
    skipped_reason: str | None = None   # set ⇒ no DDL emitted for this table

def plan_tenant_rls(domain_classes, *, base, tenant_column="tenant_id", …) -> list[RlsTablePlan]
def render_tenant_rls_ddl(plans, *, null_tenant=NullTenantPolicy.REFUSE, …) -> list[str]
def tenant_rls_upgrade(op, *, plans) -> None
def tenant_rls_downgrade(op, *, plans) -> None
```

**Scope enumeration** comes from `ParsedMeta.tenant_scope` (`varco_core/varco_core/meta.py:802-825`
— `TENANT` is the fail-closed default; `GLOBAL` is opt-in), the same source
`SAModelFactory._symbolic_schema_for` already keys on (`varco_sa/varco_sa/factory.py:513`).

**Cast-type derivation** maps the SA column type to a Postgres cast:
`Uuid`/`UUID` → `uuid`; `String`/`Text`/`Unicode` → `text`; `Integer`/`BigInteger` → `bigint`.
Anything else → a `skipped_reason`, never a guess.

| ID | Choice | Consequence |
|---|---|---|
| D-S12-autogen | A **plan/render/apply split**: `plan_tenant_rls()` is pure and returns an inspectable, printable list; `render_tenant_rls_ddl()` turns it into strings; `tenant_rls_upgrade()` executes them through `op`. A `TENANT`-scoped table with **no tenant column** is `skipped_reason`-ed, never silently omitted | An operator can `print(plan_tenant_rls(...))` in a review before any DDL exists, and every skip is visible |

**DESIGN: derive the cast type, never default it**

✅ Scout-verified and already bitten once: framework tables are `String(255)`, and a hardcoded
   `::uuid` *"made `framework_rls_upgrade()` abort every migration that used it"*
   (`rls.py:146-153`). A generator that repeats that mistake at ten tables' scale is worse.
✅ Brief 007 §2: *"Always cast `current_setting()` to the target column type"* — the cast is
   correctness, not style, and the column is the only authority on what it should be.
✅ Brief 007 §8: an index-friendly `tenant_id = <value>` policy is what keeps overhead at 1–3%;
   a mismatched implicit cast is what turns it into a sequential scan.
❌ Reflection over `base.metadata` means the generator only works for tables varco generated or
   the app registered. Accepted — `render_rls_ddl()` by name remains the escape hatch, and the
   skip list makes the boundary visible.
  Rejected — **`cast_type="uuid"` default, as `render_rls_ddl` has**: ❌ the documented,
  already-experienced failure above.
  Rejected — **`tenant_id::text = current_setting(…)`** (cast the column, not the setting):
  ❌ brief 007 §2 marks it "RISKY"; it also defeats a `tenant_id` index, contradicting §8.

**The tenant catalog is never a candidate.** `varco_tenants`
(`varco_sa/varco_sa/tenancy/models.py:40`) has `tenant_id` as its **primary key** — it is the
control-plane table that *lists* tenants. An RLS policy on it would make the catalog show each
connection exactly one row and break provisioning, fan-out and `assert_rls_enabled` itself. It
is hard-excluded, by name, with a test.

### §D-S12-hook — `after_begin`, not `SQLAlchemyUnitOfWork._begin()`

The row asks to "wire `set_tenant_local()` into the UoW automatically". The obvious hook is
`SQLAlchemyUnitOfWork._begin()` (`varco_sa/varco_sa/uow.py:45-49`). It is the wrong one.

`SQLAlchemyUnitOfWork.commit()` (`uow.py:51-53`) ends the transaction; the *next* statement on
that session autobegins a **new** transaction in which the `set_config(…, true)` GUC is gone
(brief 007 §3: reverted on COMMIT *or* ROLLBACK). A `_begin()`-only wiring is therefore silently
correct until the first commit-then-read and silently returns **zero rows** after it. Today's
own test suite already demonstrates the mechanic (`varco_sa/tests/test_rls.py:210-226`).

| ID | Choice | Consequence |
|---|---|---|
| D-S12-hook | `varco_sa/varco_sa/tenancy/rls_session.py::install_rls_tenant_hook(target, *, setting="rls.tenant_id", require_tenant=False) -> Callable[[], None]` — registers a SQLAlchemy **`after_begin`** listener on a `Session`/`sessionmaker`/`async_sessionmaker` and returns an uninstall callable. Every transaction on that factory, including post-commit ones and nested `begin()`s, sets the GUC from `current_tenant()` | Correct across the commit boundary; also covers sessions built by `get_repository()` (`provider.py:175-180`), which never see a UoW at all |

**DESIGN: an `after_begin` event listener over an imperative UoW call**

✅ Brief 007 §6 names `after_begin` as the SQLAlchemy 2.x hook, *"fires at the start of every
   transaction, including nested transactions"*, and instructs registering on the **sync
   `Session` class** so `AsyncSession` inherits it.
✅ Covers all three ways a session is produced in `varco_sa` — `SQLAlchemyUnitOfWork`
   (`uow.py:47`), `SQLAlchemyRepositoryProvider.get_repository` (`provider.py:179`), and any app
   code holding the `async_sessionmaker` directly. `_begin()` covers one of the three.
✅ Reuses `set_tenant_local()` unchanged (`rls.py:185-231`) — bound parameters, `is_local=true`,
   PgBouncer-transaction-mode safe (brief 007 §4's table). Nothing about the SQL is reinvented.
✅ Returning an uninstaller keeps it testable and reversible; no process-global registry.
❌ **The verb is a third `install_*` shape.** CLAUDE.md's taxonomy records two — (a) a
   process-global side effect taking no argument, (b) an ASGI-app mutation. This is (c): a
   mutation of a *caller-supplied SQLAlchemy object*, container-free. Plan 022's AB-3 verdict was
   `leave-and-document`, and the fix it actually applied was **correcting the table to match
   reality**. Same treatment: Step 22 adds shape (c) to the table. `bind_*` is wrong (it does not
   touch a container), `enable_*` is wrong (it registers no DI binding), `mount_*` is wrong (no
   HTTP surface).
❌ An event listener is action at a distance — a `SELECT set_config(...)` appears in the query log
   at every `BEGIN`. Documented, and it is one round trip inside a transaction that was opening
   anyway.
  Rejected — **call `set_tenant_local()` from `SQLAlchemyUnitOfWork._begin()`**: ❌ breaks after
  the first `commit()`; ❌ misses `get_repository()` sessions entirely.
  Rejected — **`before_execute` per statement** (brief 007 §7's Mechanism 1): ❌ one extra round
  trip per statement rather than per transaction; the GUC is transaction-scoped, so per-statement
  setting buys nothing.

**Fail-closed is opt-in, deliberately.** With `require_tenant=False` (the default) and no ambient
tenant, the hook sets the GUC to the empty string, which `render_rls_ddl`'s existing
`NULLIF(…, '')` maps to `NULL` (`rls.py:159-173`) → RLS hides every row, no crash. With
`require_tenant=True` it raises `RuntimeError` naming the missing `tenant_context()` — brief 007
§6/§7's "fail if context not set" posture. It is **not** the default because a background job,
an `OutboxRelay` poll, a migration and a health check all legitimately run with no tenant, and
flipping that is exactly the "needs real application work" half of the locked blast-radius rule
(`BACKLOG.md:45`) — warn-only in 3.2, candidate for 4.0.

**Two new `TenancySettings` fields**, additive, both `False`
(`varco_core/varco_core/tenancy/settings.py:110-119`, a plain frozen dataclass with `from_env`):

| Field | Env | Default | Meaning |
|---|---|---|---|
| `rls_set_tenant` | `VARCO_TENANCY_RLS_SET_TENANT` | `False` | install the `after_begin` hook |
| `rls_require_tenant` | `VARCO_TENANCY_RLS_REQUIRE_TENANT` | `False` | the hook raises instead of clearing when no tenant is ambient |

**DESIGN: two new flags, `enforce_rls` untouched**

✅ `enforce_rls` means one thing today — *"assert Postgres RLS is enabled on every routed table"*
   (`settings.py:84-86`, implemented at `rls_check.py:51-129`). Widening it to also mean "and set
   the GUC for me" would change runtime behaviour for every existing `enforce_rls=True`
   deployment on upgrade, which is precisely the silent-change class this cycle exists to stop.
✅ Independent knobs match reality: an app can want the assertion without the hook (it sets the
   GUC itself today) or the hook without the assertion (rolling RLS out table by table).
✅ AB-7 already reviewed `enforce_rls=False` and returned `leave-and-document`
   (`design/api-freeze-and-standards/api-break-candidates.md:49`). Leaving it alone honours that.
❌ Three RLS-related flags on one settings object. Accepted; the alternative is one flag that
   means three things.

### §D-S12-posture — make the `BYPASSRLS`/owner no-op findable; let Plan 036 report it

Brief 007 §1 names this the *"cause of production RLS failures"*: superusers and `BYPASSRLS`
roles bypass **unconditionally**, and table owners bypass unless `FORCE` is set — *"common when
migrations run as the app user"*. `assert_rls_enabled()` today checks `relrowsecurity` only
(`rls_check.py:41-48`); a deployment can pass it and be entirely unprotected.

| ID | Choice | Consequence |
|---|---|---|
| D-S12-posture | Add `varco_sa/varco_sa/tenancy/rls_check.py::inspect_rls_posture(conn, *, tables, …) -> RlsPosture` — a frozen dataclass reporting `current_role`, `is_superuser`, `rolbypassrls`, `owned_tables`, and per table `rls_enabled`/`rls_forced`/`has_policy`. `assert_rls_enabled()` **keeps its exact current raise condition** and additionally logs one WARNING per new finding | An operator (and Plan 036) can see the no-op; no existing `enforce_rls=True` deployment starts failing on upgrade |

**DESIGN: report-and-warn in 3.2; raising is 4.0's decision**

✅ Locked blast-radius rule (`BACKLOG.md:45`): the fix for a missing `FORCE` is an
   `ALTER TABLE … FORCE ROW LEVEL SECURITY` in a reviewed revision — and applying it can make an
   app that never sets the GUC go to zero rows. That is "real application work", so it is
   warn-only now.
✅ Brief 007 §1's remedy is a *configuration* answer (own tables with a privileged migration role,
   run the app as a non-owner, or use `FORCE`), which a library can detect but must not impose.
✅ A frozen dataclass return keeps Plan 036's consumption a pure read — no shared mutable state,
   no import from `varco_fastapi` into `varco_sa`.
❌ A deployment that only reads the raise, not the log, stays unprotected. Mitigated by Plan 036's
   preflight, which is the row that exists to surface exactly this, and by the Pitfalls table.
❌ `RlsPosture` is a new public type in the api-surface snapshot. Accepted; Step 21 regenerates.
  Rejected — **make `assert_rls_enabled` raise on a missing `FORCE` or a bypassing role**:
  ❌ an upgrade-time behaviour change for existing `enforce_rls=True` users, and the "role
  bypasses" branch would fail every local/CI Postgres container, whose default role *is* a
  superuser (`postgres-rls.md:314-324`).
  Rejected — **build the preflight here**: ❌ Plan 036 owns it; two preflights is worse than one.

### §D-S12-oq4 — BACKLOG open question 4: **no, and the migration story is still mandatory**

> *Does `S12` (RLS-by-default) change existing generated DDL? If so it needs a migration story
> for tables that already exist without policies, not just a new-table default.*

**Answer: it changes no existing DDL.** Brief 007 §5 is direct: *"`ENABLE ROW LEVEL SECURITY` and
`FORCE ROW LEVEL SECURITY` are pure DDL operations on the table definition, not data migrations.
The table's columns, indexes, constraints, and stored data are unchanged."* varco's own generated
`CREATE TABLE` for a `TENANT` model is byte-identical after this plan — Phase 1 adds *additional*
statements that an application's own revision chooses to emit. `SAModelFactory` is not modified.

**The migration story is still mandatory**, because the ordering landmine (§D-S12-order) and the
GUC dependency (§D-S12-hook) make "structurally additive" and "safe to apply blind" different
claims. The full story is §Migration and upgrade note below; the decision it encodes:

| ID | Choice | Consequence |
|---|---|---|
| D-S12-oq4 | **No revision ships in `varco_sa/migrations/versions/`.** Enabling RLS stays an application-authored revision, exactly as `postgres-rls.md`'s "What is opt-in and what is not" already promises. varco ships the *generator*, the *ordering guarantee*, the *posture check* and a documented recipe | The framework never applies a policy the operator did not review — and never takes an existing table dark on `alembic upgrade head` |

**DESIGN: no shipped revision, even for varco's own framework tables**

✅ A varco-owned revision applies on `VARCO_MIGRATE_MODE=upgrade` — which CLAUDE.md documents as a
   real production posture — and would enable RLS on `varco_dead_letters`, `varco_audit_log`,
   `varco_schedules`, `varco_webhook_subscriptions` and the encryption key store on somebody's
   next deploy. Three of those have **nullable** `tenant_id` (§D-S12-nullable), so existing
   untenanted rows would vanish, and nothing yet sets the GUC. That is a data-visibility outage
   shipped as a patch.
✅ It preserves the single load-bearing promise the RLS docs already make
   (`postgres-rls.md:331-343`): *"a table has no RLS at all until an application writes a revision
   that does so and runs it."* Breaking that promise inside a security release would be a poor
   trade.
✅ `ALTER TABLE … ENABLE ROW LEVEL SECURITY` takes an **`AccessExclusiveLock`** (brief 007 §5) —
   brief, but it blocks all reads and writes on the table, and brief 007's Evidence Gap 5 records
   that no downtime estimate exists for a very large table. That belongs in an operator's own
   maintenance window, not in a framework revision.
❌ "RLS by default" ends up meaning "one reviewed call away", not "on". Stated plainly in the
   docs and the CHANGELOG so nobody reads the row title as a promise it does not keep.
  Rejected — **ship `0008_tenant_rls.py`**: ❌ every ✅ above, inverted.
  Rejected — **auto-apply behind `VARCO_TENANCY_ENFORCE_RLS=true`**: ❌ turns a *settings* flag
  into a DDL emitter, contradicting `postgres-rls.md`'s startup-hook pitfall and RD-6's
  assert-only rule (`rls_check.py:4-14`).

### §D-S15-shape — the S15 tension, resolved: an **AST** guard, never a compiled-SQL analyzer

The backlog wants S15 (🟢, honest that it is leaky). Brief 007 §7 **advises against** it: a
whitebox assertion over compiled SQL is *"not a reliable static or runtime check"*, fooled by
implicit casts, join conditions on another table, subqueries and raw SQL; *"No mainstream
framework … ships a built-in tenant-filter assertion"*, and brief 006's Evidence Gap 2 found the
same independently. Its verdict: **opt-in, never a hard gate, documented false negatives.**

Both are right about different objects. The brief's objection is to analyzing **compiled SQL**.
varco has something the brief's survey did not consider: **its own typed, frozen AST**
(`varco_core.query`, `TransformerNode`), built *before* any backend sees it.

| Property | Compiled-SQL assertion (rejected) | AST assertion (chosen) |
|---|---|---|
| Implicit casts / type coercion | fooled (brief 007 §7) | n/a — no SQL yet |
| Joins, CTEs, subqueries | fooled (brief 007 §7) | n/a — the AST has none of these constructs |
| SQLAlchemy-version fragility | *"fragile and SQLAlchemy-version-dependent"* (§7) | none — varco owns the AST and it is frozen |
| Backend-agnostic | no (Mongo has no compilation phase at all) | **yes** — one implementation, pre-backend |
| Raw SQL / a path that builds no `QueryParams` | fooled | **fooled — the one real false negative** |

| ID | Choice | Consequence |
|---|---|---|
| D-S15-shape | `varco_core/varco_core/query/applicator/tenant_guard.py::assert_tenant_predicate(node, *, tenant_field, entity)` — walks the **AST**, requiring an equality comparison on `tenant_field` on the top-level `AND` spine. Off by default. Raises `TenantFilterError` (a `ServiceException` subclass) when absent | One implementation, genuinely portable, with exactly one documented false-negative class instead of five |

**DESIGN: AST-level, opt-in, dev-time — and never described as a security control**

✅ It sidesteps every false-negative mode brief 007 §7 actually enumerates, because none of them
   (casts, joins, CTEs, subqueries) exist in varco's AST.
✅ It is the only shape that makes the backlog's "backend-agnostic" claim true. There is no Beanie
   analogue of SQLAlchemy's compilation phase — a compiled-artifact check would have been two
   unrelated implementations, and the Mongo one would inspect a hand-built `dict`.
✅ AST nodes are `@dataclass(frozen=True)` (CLAUDE.md) — hashable, immutable, safe to walk and
   to cache the verdict on. **Phase 5 adds no field to any node** and does not touch the frozen
   contract.
✅ Requiring the predicate on the **top-level `AND` spine** avoids the obvious false *positive*:
   `tenant_id = X OR status = 'public'` contains a tenant predicate that constrains nothing.
❌ It is fooled by any path that does not build a `QueryParams` — raw `session.execute(text(...))`,
   a hand-written `Select`, `get(pk)`, an aggregation pipeline. **This is why it is not a security
   control, and why S12 lands first.**
❌ It moves the check away from the literal wording of the backlog row ("applicator-level"). Named
   as such in the CHANGELOG and BACKLOG update; §D-S15-hook has the verified reason.
  Rejected — **compiled-SQL analysis over `Select`/`Compiled`**: ❌ brief 007 §7 + brief 006
  Evidence Gap 2; no prior art; SQLAlchemy-version-fragile; not portable to Mongo.
  Rejected — **defer S15 entirely**: a legitimate outcome, and it stays the fallback
  (§D-S15-cut). Not chosen, because the AST reframing answers the brief's objection rather than
  overriding it, and the cost is one pure function plus call sites.

⛔ **Wording rule, and it is a review gate.** Nothing in the README, the feature doc, the
docstrings, the CHANGELOG or an error message may describe this guard as *enforcing*,
*guaranteeing* or *securing* tenant isolation. The sanctioned phrasing is *"a development-time
assertion that a tenant-scoped query was built with a tenant filter; it is not a security
control — Postgres RLS (§S12) is"*. Step 26 asserts the docstring contains the disclaimer.

### §D-S15-hook — the guard hooks the repository's `QueryParams`, because the applicator is not on the read path

⚠️ **Correction 3.** `AsyncSQLAlchemyRepository` never constructs a `SQLAlchemyQueryApplicator`.
Its four query sites build a `SQLAlchemyQueryCompiler` directly:
`repository.py:190`, `:225`, `:301`, `:489`. An assertion installed on the applicator would guard
a strategy object that varco's own read path does not use — it would look shipped and do nothing,
the worst outcome for a feature whose entire risk is over-claiming.

| ID | Choice | Consequence |
|---|---|---|
| D-S15-hook | The pure function lives in `varco_core.query.applicator.tenant_guard` (backend-agnostic, per CLAUDE.md's layer rule). It is invoked from the **repository** query sites, which hold the mapper and therefore the entity's `ParsedMeta.tenant_scope`. `QueryApplicator.__init__` also gains an optional `tenant_guard=None` keyword so a custom applicator can opt in — but that is the secondary path, not the one that makes it work | The guard covers the path varco actually reads through, and stays out of `varco_sa` as a module |

✅ CLAUDE.md's layer rule is satisfied: the check itself is in `varco_core.query`, backend-agnostic;
   only the two-line call sites are in the backends.
✅ The repository already has `self._mapper._orm_cls` at each site (`repository.py:190`) and the
   mapper is built from `ParsedMeta`, so the scope and the tenant field are reachable without new
   plumbing through the provider.
❌ Four call sites in `varco_sa` plus the Beanie equivalents. That is the M–L the backlog priced,
   and it is why this phase is the droppable one.

### §D-S15-cut — the explicit cut line

If Phase 5 slips, **cut it whole** and file S15 back to `BACKLOG.md` with this un-park trigger:

> Un-park when a consumer reports a cross-tenant read that RLS did not catch **because the
> deployment is not on Postgres** — that is the only gap S15 closes which S12 does not. On
> Postgres, S15's dev-time value is real but strictly additive to a backstop that already holds.

Phases 0–4 import nothing from Phase 5. Cutting it requires deleting one new `varco_core` module,
one settings field and the call sites — no unpicking.

### Alternatives considered (plan-level)

- **Ship a `0008_tenant_rls.py` revision so RLS is genuinely on by default** — ❌ §D-S12-oq4:
  `AccessExclusiveLock` on someone's largest tables, three nullable tenant columns going dark,
  and no GUC-setter wired yet.
- **Flip `enforce_rls` to `True`** — ❌ AB-7's reviewed `leave-and-document` verdict; it breaks
  every non-Postgres deployment and every Postgres one that never ran the DDL.
- **A `SET LOCAL`-based hook instead of `set_config()`** — ❌ brief 007 §3: `SET LOCAL` cannot take
  a bind parameter, so it is a string-concatenation injection surface. `rls.py:226-231` already
  does the right thing.
- **Per-tenant database roles / `SET ROLE` instead of a GUC** — ❌ brief 007 §4 records this as
  Supavisor's tenant-in-username pattern; it multiplies roles by tenants and does not compose with
  `TenantIsolation.SHARED`, which is the strategy S12 exists to harden.
- **A `STABLE` SQL helper function (`get_current_tenant_id()`) in the policy** (brief 007 §8)
  — ❌ it needs a `CREATE FUNCTION` in the app's schema, and the existing InitPlan form already
  gets the planner win with no owned object. Parked, with a trigger.

---

## Steps

### Phase 0 — S12a: correctness of the existing primitives (🟡 should, S)

1. [ ] `rg -n "render_rls_ddl\(" . && rg -n "render_rls_ddl\(.*\)\[" .` — confirm no in-repo caller
       indexes the returned list (§D-S12-order's ❌). Record the result in the commit message.
2. [ ] `varco_sa/tests/test_rls.py` (extend, **failing first**) — `render_rls_ddl()` returns
       `CREATE POLICY` at index 0, `ENABLE` at 1, `FORCE` at 2; the existing `len(ddl) == 3`
       (`:102-106`) and `(SELECT` InitPlan assertions still pass unchanged.
3. [ ] `varco_sa/varco_sa/rls.py:174-182` — reorder to `CREATE POLICY`, `ENABLE`, `FORCE` per
       §D-S12-order. Update the docstring's numbered `Returns:` list (`:117-127`) and add a
       `DESIGN:` comment citing brief 007 §5's default-deny finding.
4. [ ] `varco_sa/varco_sa/rls_framework.py` — replace the hand-listed `FRAMEWORK_RLS_TABLES`
       (`:38`) with a **derived** helper `framework_rls_tables() -> tuple[str, ...]`: walk
       `framework_metadata()` (`varco_sa/varco_sa/metadata.py:86`) and select tables carrying the
       tenant column, **excluding `varco_tenants`** (§D-S12-autogen). Keep the old constant as a
       module attribute computed from it so no import breaks.
5. [ ] `varco_sa/tests/test_framework_rls.py` (extend) — the derived set contains
       `varco_dead_letters`, `varco_audit_log`, `varco_schedules`,
       `varco_webhook_subscriptions` and the encryption-key-store table; it **never** contains
       `varco_tenants`; a completeness walk asserts every `framework_metadata()` table with a
       tenant column is either in the set or in a named, asserted exclusion list — so a
       fourteenth framework table cannot be forgotten silently.
6. [ ] `varco_sa/tests/test_rls.py` (extend, **integration**, `@pytest.mark.integration`) — using
       the existing non-superuser role fixture (`provision_rls_app_url`, `test_rls.py:126-136`):
       execute the three statements **one transaction each, in the returned order**, and assert
       the table is never invisible-with-no-policy between them. This is the regression test for
       §D-S12-order and it must run as the app role, not the container's superuser
       (`postgres-rls.md:314-324`).

⛔ **CHECKPOINT** — `uv run pytest varco_sa/tests/test_rls.py varco_sa/tests/test_framework_rls.py`

### Phase 1 — S12b: the generated-for-you DDL path (🟡 should, M)

7. [ ] `varco_sa/tests/test_rls_autogen.py` (new, **failing first**) — `plan_tenant_rls()`:
       a `TENANT`-scoped model with a `UUID` tenant column → `cast_type="uuid"`; a `String(255)`
       one → `"text"`; an `Integer` one → `"bigint"`; a `TenantScope.GLOBAL` model → absent
       entirely; a `TENANT` model with **no** tenant column → present with a `skipped_reason` and
       **no DDL**; an unmappable column type → `skipped_reason`, never a guessed cast;
       `varco_tenants` → hard-excluded; a nullable tenant column → `ValueError` naming table,
       column and both remedies (§D-S12-nullable), `NullTenantPolicy.VISIBLE` → the emitted policy
       contains `OR … IS NULL`, `HIDDEN` → it does not; determinism (two calls, same order).
8. [ ] `varco_sa/varco_sa/rls_autogen.py` (new) — `RlsTablePlan` (`@dataclass(frozen=True)`),
       `NullTenantPolicy` (`StrEnum`: `REFUSE`/`HIDDEN`/`VISIBLE`), `plan_tenant_rls()`,
       `render_tenant_rls_ddl()`, `tenant_rls_upgrade()`, `tenant_rls_downgrade()`. Every string
       comes from `render_rls_ddl()` — **the InitPlan form is never re-derived here** (the same
       single-source rule `migration/ops.py:11-16` already states). Full docstrings with
       `Args`/`Returns`/`Raises`/`Edge cases`, a `DESIGN:` block per §D-S12-autogen and
       §D-S12-nullable.
9. [ ] `varco_sa/varco_sa/rls_autogen.py` — `plan_tenant_rls()` reads
       `ParsedMeta.tenant_scope` (`varco_core/varco_core/meta.py:802-825`) and resolves each
       domain class to its generated `Table` via the provider's `base.metadata`. A domain class
       that was never registered is a `skipped_reason`, not a `KeyError`.
10. [ ] `varco_sa/tests/test_rls_autogen_integration.py` (new, **integration**) — against real
        Postgres and the **non-superuser app role**: build two `TENANT` models (one `UUID`, one
        `String` tenant column) and one `GLOBAL` model in a `uuid4().hex[:8]`-namespaced schema;
        run `tenant_rls_upgrade`; assert tenant A sees only A's rows on both tables, the `GLOBAL`
        table is unaffected, a cross-tenant `INSERT` is rejected by `WITH CHECK`, and
        `tenant_rls_downgrade` restores full visibility.
11. [ ] `varco_sa/varco_sa/__init__.py` — export the new public names; `__all__` updated.

⛔ **CHECKPOINT** — do not proceed to Phase 2 without Step 10 green; and **do not stop here**:
policies without Phase 2's GUC-setter mean zero rows for an app that has not wired
`set_tenant_local()` itself.

### Phase 2 — S12c: automatic `set_tenant_local()` (🟡 should, M) — the wiring row

12. [ ] `varco_core/tests/test_tenancy_settings.py` (extend, **failing first**) — `rls_set_tenant`
        and `rls_require_tenant` default `False`; `VARCO_TENANCY_RLS_SET_TENANT=true` /
        `VARCO_TENANCY_RLS_REQUIRE_TENANT=1` parse through the existing `_bool` helper
        (`settings.py:148-152`); the existing "defaults are byte-identical" assertions
        (`:19`, `:42`) still pass.
13. [ ] `varco_core/varco_core/tenancy/settings.py` — the two fields (§D-S12-hook's table) plus
        `from_env()` wiring and docstring `Args:` entries. **No other default moves.**
14. [ ] `varco_sa/tests/test_rls_session_hook.py` (new, **failing first**, unit) — using a SQLite
        in-memory engine and a recording listener: the hook fires once per transaction; it fires
        **again after `commit()`** on the same session (the §D-S12-hook defect, asserted
        directly); the returned uninstaller removes it and is idempotent; `require_tenant=True`
        with no ambient tenant raises `RuntimeError` whose message names `tenant_context()`;
        `require_tenant=False` with no ambient tenant sets the empty string and does not raise;
        installing twice on the same target registers **one** listener.
15. [ ] `varco_sa/varco_sa/tenancy/rls_session.py` (new) — `install_rls_tenant_hook()` per
        §D-S12-hook. Registers on the **sync `Session` class** underlying the target so
        `AsyncSession` inherits it (brief 007 §6); the listener body calls the existing
        `varco_sa.rls.set_tenant_local` machinery with bound parameters and **never** builds SQL
        by concatenation. Module docstring states: ⚠️ brief 007 §6's snippet shows an `async def`
        listener — SQLAlchemy does not await a sync event hook; the listener must execute
        synchronously on the greenlet-adapted `Connection` the event provides. Record whichever
        shape the empirical check in Step 16 establishes.
16. [ ] `varco_sa/tests/test_rls_session_hook.py` (extend, **integration**) — real Postgres, the
        **non-superuser app role**, a table from Phase 1's generator: with the hook installed and
        `tenant_context("A")` active, a plain `repo.list()` returns only A's rows **with no
        `set_tenant_local()` call anywhere in the test**; after `commit()`, a second query in the
        same session still returns only A's rows (the regression this design exists for); with no
        tenant context, zero rows and no exception; with `require_tenant=True`, `RuntimeError`.
17. [ ] `varco_sa/varco_sa/di.py` — when `TenancySettings.rls_set_tenant` is `True`, install the
        hook on the session factory at provider construction. When `False`, **nothing is
        registered and no listener exists** — assert this explicitly in a test, as
        `test_tls_di.py` does for the watcher (Plan 026 / Step 14).
18. [ ] `varco_sa/tests/test_rls_session_hook.py` (extend) — `container.scan("varco_sa",
        recursive=True)` with default settings installs **no** listener; with
        `rls_set_tenant=True` it installs exactly one.

⛔ **CHECKPOINT** — the P1+P2 pair is now complete and coherent.

### Phase 3 — S12d: the owner / `BYPASSRLS` footgun, made findable (🟡 should, S)

19. [ ] `varco_sa/tests/test_rls_posture.py` (new, **failing first**, integration) — provision
        three roles in a `uuid4().hex[:8]`-namespaced setup: the container superuser, a
        `BYPASSRLS` role, and the plain app role from `provision_rls_app_url`. Assert
        `inspect_rls_posture()` reports `is_superuser`/`rolbypassrls` correctly for each; reports
        `rls_forced=False` for a table with `ENABLE` but no `FORCE`; reports `has_policy=False`
        for a table with neither; and that **the superuser leg sees every row despite a correct
        policy** — the assertion that proves the test itself is not lying (`postgres-rls.md:314-324`).
20. [ ] `varco_sa/varco_sa/tenancy/rls_check.py` — add `RlsPosture` (`@dataclass(frozen=True)`) and
        `inspect_rls_posture()` per §D-S12-posture, querying `pg_roles` for
        `rolsuper`/`rolbypassrls` and `pg_class.relrowsecurity`/`relforcerowsecurity`/`relowner`.
        `assert_rls_enabled()`'s **raise condition is not touched** (`:116-127`); it gains one
        WARNING per new finding and a docstring note pointing at `inspect_rls_posture()` and at
        Plan 036's preflight as the reporting consumer. Non-Postgres dialect → the same
        skip-with-one-WARNING contract as today (`:95-103`).
21. [ ] `uv run python scripts/api_surface.py` — regenerate both snapshot files and **commit them
        in this commit** (CI gate, `make lint`'s no-`PKG` path). New `varco_sa` rows are expected:
        `RlsTablePlan`, `NullTenantPolicy`, `RlsPosture`, `plan_tenant_rls`,
        `render_tenant_rls_ddl`, `tenant_rls_upgrade`, `tenant_rls_downgrade`,
        `install_rls_tenant_hook`, `inspect_rls_posture`, `framework_rls_tables`. Additions are
        notes and never fail `--check`; run `--check` anyway before committing.

⛔ **CHECKPOINT** — S12 is functionally complete.

### Phase 4 — docs, CHANGELOG, backlog (🟡 should, S — same commit as the code)

22. [ ] `CLAUDE.md` — (a) the **`install_*` shape (c)** row in the DI-wiring verb taxonomy
        (§D-S12-hook's ❌), naming `install_rls_tenant_hook` as its example; (b) one Decision-Tree
        branch under multitenancy (*RLS DDL for tenant tables? → `varco_sa.rls_autogen`; the
        per-transaction GUC? → `varco_sa.tenancy.rls_session`; is my deployment actually
        protected? → `inspect_rls_posture()`*); (c) a **Rule** line: *never ship a varco-owned
        Alembic revision that enables RLS* (§D-S12-oq4). Pointers only — no design prose.
23. [ ] `technical_docs/features/postgres-rls.md` (**the primary home** — it already carries the
        InitPlan finding, the `SET LOCAL` section and a Pitfalls table at `:344-353`; extending it
        rather than duplicating into `multitenancy.md` is CLAUDE.md's "one home per fact"). Add:
        the generator recipe; the §D-S12-order ordering rule with brief 007 §5's citation; the
        nullable-tenant decision; the `after_begin` wiring with brief 007 §6; the pooler table
        from brief 007 §4 (**PgBouncer transaction mode ✅ safe; RDS Proxy ⚠️ session-pinning;
        Supavisor ⚠️ untested**); the `AccessExclusiveLock` note and the rollback path. **New
        Pitfalls rows**: owner-bypass no-op; `FORCE` present but the role is `BYPASSRLS`/superuser
        anyway; policy-before-enable ordering; nullable tenant column → rows invisible forever;
        `RDS Proxy` session pinning; a `current_tenant()` value whose format does not match the
        tenant column's type (e.g. `"acme"` against a `uuid` column → `invalid input syntax`).
24. [ ] `technical_docs/features/multitenancy.md` — a short **"Database-enforced isolation"**
        subsection linking to the above (no restatement) and one Pitfalls row: *`enforce_rls=True`
        asserts a policy exists; it does not prove the connecting role is subject to it — see
        `inspect_rls_posture()`*.
25. [ ] `README.md` (multi-tenancy section) + `varco_sa/README.md` (`:303-321` already shows the
        manual helpers) — the two new env vars in the reference table, the one-call generator
        recipe, and the hook. `CHANGELOG.md` `## [Unreleased]`: `### Added` (the generator, the
        hook, `inspect_rls_posture`, the two settings — "Plan 037 / S12"); `### Changed`
        (`render_rls_ddl()` statement **order**, with the §D-S12-order reason and the
        list-indexing caveat; `FRAMEWORK_RLS_TABLES` now derived and wider). `BACKLOG.md` — replace
        open question 4 (`:106-107`) with its answer pointing at §D-S12-oq4, in the
        answered-not-deleted style Plan 024 used.

⛔ **CHECKPOINT** — `make lint`, `make type-check`, `make test`, `make integration-test PKG=varco_sa`.
**S12 is shippable here. Everything below is separable and droppable (§D-S15-cut).**

### Phase 5 — S15: the AST tenant-filter guard (🟢 nice, L — **DROPPABLE**)

26. [ ] `varco_core/tests/test_query_tenant_guard.py` (new, **failing first**) —
        `assert_tenant_predicate()`: a top-level `AND` containing `tenant_id == X` passes; a bare
        `tenant_id == X` passes; `status == "x"` alone raises `TenantFilterError` naming the
        entity and the field; `tenant_id == X OR status == "public"` **raises** (the predicate
        does not constrain — §D-S15-shape); a nested `AND` under the top-level `AND` passes;
        `tenant_id != X` and `tenant_id IN (...)` raise (only equality constrains to one tenant);
        a custom `tenant_field="org_id"` (`README.md:543-547`) works; `node=None` raises; and
        **the docstring contains the "not a security control" disclaimer** (§D-S15-shape's ⛔
        wording rule, asserted mechanically).
27. [ ] `varco_core/varco_core/query/applicator/tenant_guard.py` (new) — the pure walker plus
        `TenantFilterError` (a `ServiceException` subclass with a `code` and a `message_key`, per
        `varco_core.exception`'s taxonomy). **No AST node gains a field**; nothing is mutated.
        Module docstring carries the §D-S15-shape comparison table and the brief 007 §7 /
        brief 006 Evidence Gap 2 citations for why the compiled-SQL variant was rejected.
28. [ ] `varco_core/varco_core/tenancy/settings.py` — `assert_tenant_filter: bool = False`
        (`VARCO_TENANCY_ASSERT_TENANT_FILTER`), documented as **development-time only**.
29. [ ] `varco_sa/varco_sa/repository.py` — call the guard at the four `params.node` sites
        (`:190`, `:225`, `:301`, `:489`) when enabled and the entity is `TenantScope.TENANT`,
        reading the scope from the mapper's `ParsedMeta`. `varco_beanie`'s equivalent sites get
        the identical two-line call (grep `params.node` in `varco_beanie/varco_beanie/`).
30. [ ] `varco_core/varco_core/query/applicator/applicator.py` — an optional
        `tenant_guard=None` keyword on `QueryApplicator.__init__` (`:46-56`) for custom
        applicators, defaulting to no assertion. Byte-identical when unset.
31. [ ] `varco_sa/tests/test_rls_tenant_guard.py` + the Beanie mirror (new) — with the flag off,
        a tenant-less `list()` behaves **exactly as today** (the byte-identical proof); with it on,
        the same call raises; a `TenantAwareService`-scoped call passes on both backends; a
        `TenantScope.GLOBAL` entity is never asserted; a raw `session.execute(text(...))` is
        **documented and asserted to pass unguarded** — the false negative, proven rather than
        claimed.
32. [ ] `technical_docs/features/multitenancy.md` + README — the guard, its single false-negative
        class, and the sentence that it is a dev aid and **RLS is the security control**.
        CHANGELOG `### Added` with the same framing. `scripts/api_surface.py` regenerated.

⛔ **CHECKPOINT** — if this phase is cut, apply §D-S15-cut: delete nothing from Phases 0–4, and
file S15 back to `BACKLOG.md` with the stated un-park trigger.

---

## Migration and upgrade note (existing deployments — read before shipping)

**For an app that upgrades to 3.2 and changes no code and no environment: nothing happens.**
No DDL is generated, no listener is installed, no policy is created, no query plan changes, and
`assert_rls_enabled()` raises on exactly the same condition as before. The only observable delta
is a new WARNING log line if `enforce_rls=True` **and** the deployment is already unprotected
(§D-S12-posture) — which is information it did not have and needed.

**Adopting RLS on an existing database, in order:**

1. **Read the posture first.** `inspect_rls_posture()` — if `is_superuser` or `rolbypassrls` is
   `True` for the app's role, stop: policies will be a no-op (brief 007 §1). Fix the role before
   writing any DDL.
2. **Wire the GUC before the policies, not after.** Set `VARCO_TENANCY_RLS_SET_TENANT=true` and
   deploy. With no policies yet this is a harmless extra `set_config` per transaction — and it
   means step 4 cannot take the app to zero rows.
3. **Plan and review.** `print(plan_tenant_rls(...))` in a review. Resolve every `skipped_reason`
   and every nullable tenant column **before** generating DDL (§D-S12-nullable).
4. **One reviewed revision**, application-owned (§D-S12-oq4), calling `tenant_rls_upgrade(op,
   plans=...)`. Statements are ordered policy → enable → force (§D-S12-order), so there is no
   default-deny window even if Alembic is configured without a per-migration transaction.
   ⚠️ `ALTER TABLE … ENABLE ROW LEVEL SECURITY` takes an **`AccessExclusiveLock`** — brief 007 §5
   says it is held briefly (*"typically milliseconds"*), and its Evidence Gap 5 says nobody has
   measured a 1B-row table. Apply in a maintenance window, largest tables last, and set a short
   `lock_timeout`.
5. **Verify as the app role, never as a superuser.** A smoke test that connects as the container's
   default role will pass while proving nothing (`postgres-rls.md:314-324`).
6. **Then, optionally**, `VARCO_TENANCY_ENFORCE_RLS=true` so a future table without a policy fails
   startup, and `VARCO_TENANCY_RLS_REQUIRE_TENANT=true` once every background path is known to
   establish a tenant context.

**Rollback.** `tenant_rls_downgrade(op, plans=...)` (`DROP POLICY IF EXISTS` +
`DISABLE ROW LEVEL SECURITY`) restores full visibility and takes the same brief lock. It is safe
to run with the hook still installed. Rolling back **only** the application while leaving policies
in place is also safe *provided* `rls_set_tenant` stays on — remove the policies before removing
the GUC-setter, never the other way round.

**Framework tables.** varco ships no revision for them (§D-S12-oq4). `framework_rls_tables()` now
reports the true, derived set (Step 4) — five-plus tables, not two — and
`framework_rls_upgrade(op)` remains the one-call helper for an app that wants them protected.
Three of them have **nullable** `tenant_id`; choose `NullTenantPolicy` deliberately.
`varco_tenants` is never a candidate.

---

## Edge cases

- **A `TENANT`-scoped model with no tenant column** → `skipped_reason`, no DDL, visible in the
  plan output. Never a silent omission and never a `KeyError`.
- **`current_tenant()` returns a value the column type cannot parse** (e.g. `"acme"` against a
  `uuid` column) → Postgres raises `invalid input syntax for type uuid` on the *first query*, not
  on `set_config`. Documented as a Pitfall; the fix is the tenant-id format, not the policy.
- **No ambient tenant, `require_tenant=False`** → GUC set to `''`, which `NULLIF` maps to `NULL`
  (`rls.py:159-173`) → zero rows, no crash. This existing behaviour is load-bearing and must not
  be "simplified" away.
- **Nested `session.begin_nested()`** → `after_begin` fires again; the GUC is re-set to the same
  value. Idempotent, one extra round trip.
- **A transaction that spans a `tenant_context()` change** → the GUC keeps the value from
  `BEGIN`. Do not change tenants mid-transaction; documented.
- **Non-Postgres dialect** → the hook still installs (it is dialect-agnostic) but `set_config` is
  Postgres-only. The hook must **skip on a non-Postgres dialect with one WARNING**, matching
  `rls_check.py:95-103` and `migration/ops.py:66-72`. Asserted on SQLite.
- **`TenantIsolation.SCHEMA`/`DATABASE`** → RLS is orthogonal and additive; the generator emits
  unqualified table names, so `schema_translate_map` resolution still applies
  (`postgres-rls.md:142-181` — never `search_path`).
- **A `GLOBAL` table** → excluded by `plan_tenant_rls`, and already excluded by
  `assert_rls_enabled` (`rls_check.py:105`). Both paths agree; asserted.
- **Running the generator twice** → the second `CREATE POLICY` fails with *"policy already
  exists"*, unchanged from `render_rls_ddl`'s documented edge case (`rls.py:128-134`). The
  generator does not add `IF NOT EXISTS`; pair with `tenant_rls_downgrade` for idempotency.

## Verification

```bash
uv sync --all-packages --all-extras

# unit
uv run pytest varco_sa/tests/test_rls.py varco_sa/tests/test_rls_autogen.py \
              varco_sa/tests/test_framework_rls.py varco_sa/tests/test_rls_session_hook.py \
              varco_core/tests/test_tenancy_settings.py -q
uv run pytest varco_core/tests/test_query_tenant_guard.py -q          # Phase 5 only

# integration — real Postgres, and the app role, not the superuser
uv run pytest varco_sa/tests/ -m integration -q
make integration-test PKG=varco_sa

uv run python scripts/api_surface.py --check     # MUST be clean before committing
make lint && make type-check && make test
```

**DoD:**
1. Step 6 proves the reordered statements never leave a table default-deny between transactions.
2. Step 10 and Step 16 both run **as a non-superuser, non-`BYPASSRLS`, non-owner role** — a green
   run as the container's default role proves nothing and is a review-blocking defect in the test.
3. Step 12/18/31's byte-identical assertions prove a no-code-change upgrade is inert.
4. `api_surface.py --check` green with the regenerated snapshot committed.

## Parked

| Item | Why | Un-park trigger |
|---|---|---|
| A shipped `0008_tenant_rls.py` revision | §D-S12-oq4 — `AccessExclusiveLock`, nullable columns, no GUC-setter | Never on current evidence; the recipe is the deliverable |
| `STABLE` SQL helper function in the policy (brief 007 §8) | The InitPlan form already gets the planner win without an owned database object | A policy needs logic beyond one equality comparison |
| RLS overhead benchmark in `benchmarks/` | Brief 007's Evidence Gap 1 — 1–3% is cited, never measured for varco | ⛔ a benchmark must not need a container (CLAUDE.md), so this needs a different harness, not a `bench_*.py` |
| Flipping `rls_require_tenant`/`enforce_rls` to `True` | Locked blast-radius rule: needs real application work → warn in 3.2, flip in 4.0 | The 4.0 window, with the posture check's field evidence |
| Supavisor validation | Brief 007 §4/Evidence Gap 2 — `SET LOCAL` behaviour untested at scale | A consumer deploys on Supavisor |
| Mongo/Beanie server-side tenant enforcement | No RLS analogue exists | MongoDB ships row-level filtering |

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| **A generated policy is a silent no-op** because the app role owns the tables or holds `BYPASSRLS`/superuser (brief 007 §1) | **Critical** — the feature looks shipped and protects nothing | §D-S12-posture's `inspect_rls_posture()`; Step 19's three-role integration test; the WARNING from `assert_rls_enabled`; Plan 036's preflight as the reporting surface; two Pitfalls rows |
| **Enabling RLS takes a table dark** — policy missing, GUC unset, or a nullable tenant column | **Critical** — a data-visibility outage | §D-S12-order's reordering + Step 6; §D-S12-nullable's refuse-by-default; the migration note's mandatory "wire the GUC *before* the policies" ordering; §D-S12-oq4 (varco ships no revision) |
| ⚠️ **ASSUMPTION — no consumer indexes `render_rls_ddl()`'s returned list.** Verified in-repo by Step 1; out-of-repo is an assumption | Medium | `### Changed` CHANGELOG entry; contents are unchanged, so only positional access breaks, and it breaks loudly (wrong statement, immediate Postgres error) |
| ⚠️ **ASSUMPTION — SQLAlchemy's `after_begin` listener can execute the `set_config` synchronously under `AsyncSession`.** Brief 007 §6 gives the *hook* but its snippet is an `async def` listener with `await`, which SQLAlchemy does **not** await for a sync event | **High — it is the mechanism Phase 2 rests on** | Verify empirically at Step 15 **before** building on it, exactly as Plan 026 handled the `httpx` pinning assumption. If the sync form does not work, the fallback is an explicit call from `SQLAlchemyUnitOfWork._begin()` **plus** a documented post-commit caveat — not dropping the phase. Do not delete Step 14's post-commit test; change the mechanism |
| ⚠️ **ASSUMPTION — `framework_metadata()` column reflection identifies every tenant column.** Derived in Step 4 from column names; a framework table naming it something else would be missed | Medium | Step 5's completeness walk asserts every framework table is in the derived set or a named exclusion list — a new table cannot be silently dropped |
| **RDS Proxy session pinning** (brief 007 §4) — `SET LOCAL`/`set_config` pins the connection, degrading pooling | Medium — performance, not correctness | Documented in the pooler table and a Pitfalls row; `rls_set_tenant` is opt-in, so an RDS Proxy deployment can decline it and keep app-layer scoping + `enforce_rls` assertions |
| **RLS overhead exceeds the cited 1–3%** (brief 007 §8, Evidence Gap 1 — never measured for varco) | Medium | The InitPlan form is mandatory and already regression-tested (`rls.py:24-26`); `tenant_id` indexes are a documented prerequisite; the parked benchmark row |
| ⚠️ **ASSUMPTION — a `TenantScope.TENANT` model's tenant column is `tenant_id`.** `TenantAwareService._tenant_field` is overridable (`README.md:543-547`) | Low | `plan_tenant_rls(tenant_column=…)` is a parameter; a mismatch surfaces as a `skipped_reason`, never a wrong policy |
| **S15 is read as a security guarantee** despite every disclaimer | Medium — it would let a team skip S12 | §D-S15-shape's ⛔ wording rule, asserted mechanically in Step 26; S15 ships only after S12 (locked, `BACKLOG.md:49`); Step 31 proves the raw-SQL false negative rather than describing it |
| **Phase 5 slips and the plan looks half-done** | Low | §D-S15-cut: cut whole, file back with an un-park trigger. Phases 0–4 import nothing from it |

## Open questions

1. **Should `install_rls_tenant_hook` skip on a non-Postgres dialect at install time or per
   transaction?** Per-transaction is safer (the dialect is known only once a connection exists)
   but costs a check per `BEGIN`. Decide at Step 15 — lean per-transaction with the result cached
   on the engine, and one WARNING per engine rather than per transaction.
2. **Does `inspect_rls_posture()` belong in `rls_check.py` or its own module?** `rls_check.py` is
   currently a single-function module with a tight RD-6 "assert-only" docstring. Adding a reporting
   function beside an asserting one is defensible; splitting is cleaner. Decide at Step 20 — lean
   same module, with the module docstring extended to name both roles.
