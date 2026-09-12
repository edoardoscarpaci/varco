# Research 007 — PostgreSQL Row-Level Security (RLS) Enforcement Mechanics

Date: 2026-09-05 · Freshness matters: **YES** — RLS is stable since PostgreSQL 9.5 (2016), but PgBouncer/Supavisor connection pooling patterns, SQLAlchemy 2.x async integration, and migration safety are active operational concerns. The "table owner bypass" gotcha is widely documented but still a leading cause of RLS failure in production.

## Question

How does PostgreSQL Row-Level Security actually enforce tenant isolation at the database level, and what is required to safely integrate it into varco's multi-tenant framework? Specifically:

- **Enforcement model:** What is the precise difference between `ENABLE` and `FORCE ROW LEVEL SECURITY`, and who bypasses RLS (table owner, superuser, BYPASSRLS roles)?
- **Policy DDL:** What is the canonical tenant-isolation policy shape (USING vs. WITH CHECK, FOR ALL vs. per-command, PERMISSIVE vs. RESTRICTIVE)?
- **Session variable mechanics:** How do `SET LOCAL` and `set_config()` work for per-request tenant context, and what is the type-cast discipline for comparing text settings to UUID/int columns?
- **Connection pooling:** Does `SET LOCAL` survive under PgBouncer (transaction mode), Supavisor, RDS Proxy, and other poolers? What is the leakage risk if tenant context leaks across pooled checkouts?
- **Migration story for existing tables:** What does it take to enable RLS on a live table that already has data? Is it blocking? What is the correct ordering (policies first, then enable)?
- **SQLAlchemy 2.x async integration:** How to wire per-transaction tenant setting into SQLAlchemy 2.x with async engines, and what event hooks are available?
- **Mechanical guards against tenant-less queries:** What mechanisms exist in SQLAlchemy to verify a compiled query includes a tenant predicate, and how leaky are they (false negatives)?
- **Performance:** What is the measured overhead of RLS, and what are the planner pitfalls (index usage prevention, function stability/leakproofness)?

**Open question from backlog S12:** Does the RLS-by-default plan change existing DDL for tables that already exist without policies?

## Findings

### 1. RLS Enforcement Model: ENABLE vs. FORCE, Bypass Rules

**Who Bypasses RLS by Default (PostgreSQL 9.5–18):**

Row-level security policies are enforced for **all database users except**:
- **Superusers** — always bypass all RLS policies — [PostgreSQL 18 Docs: Row Security Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
- **Roles with the `BYPASSRLS` attribute** — assigned via `ALTER ROLE role_name BYPASSRLS` — [PostgreSQL 18 Docs: Row Security Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
- **Table owners** — by default, table owners are exempt from RLS policies they own — [Neon Docs: Row-Level Security](https://neon.com/postgresql/administration/row-level-security)

**The Critical Gotcha (cause of production RLS failures):**

A common cause of RLS silently not working is **when the application role owns the tables it creates** (common when migrations run as the app user). The app role then bypasses all policies it owns. — [Neon Docs: Row-Level Security](https://neon.com/postgresql/administration/row-level-security)

**The Solution: `ALTER TABLE ... FORCE ROW LEVEL SECURITY`:**

Introduced in PostgreSQL to address the owner-bypass problem, `FORCE ROW LEVEL SECURITY` makes **even the table owner** subject to RLS policies:

```sql
ALTER TABLE tenant_table ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_table FORCE ROW LEVEL SECURITY;
```

With `FORCE` applied, the table owner must comply with policies or gets a default-deny (no rows visible). — [PostgreSQL 18 Docs: Row Security Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)

**Best Practice for varco:**

If the app role owns the tables (migrations run as app user), **`FORCE ROW LEVEL SECURITY` must be used**. If a separate, privileged migration role owns tables, `ENABLE` is sufficient (the app role will not bypass).

### 2. Policy DDL Shape: USING vs. WITH CHECK, FOR ALL vs. Per-Command, PERMISSIVE vs. RESTRICTIVE

**Canonical Multi-Tenant Isolation Policy (all operations):**

```sql
CREATE POLICY tenant_isolation ON orders
  FOR ALL
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
```

**USING vs. WITH CHECK:**

- **USING clause**: Controls which rows are **visible for reads, updates, and deletes** — filters the result set before the user sees it. Applied to `SELECT`, `UPDATE`, and `DELETE` operations. — [Bytebase Docs: PostgreSQL RLS](https://www.bytebase.com/reference/postgres/how-to/postgres-row-level-security/)
- **WITH CHECK clause**: Validates rows **written by INSERT and UPDATE operations** — ensures a row being inserted or updated satisfies the policy. If the check fails, the write is rejected. — [Bytebase Docs: PostgreSQL RLS](https://www.bytebase.com/reference/postgres/how-to/postgres-row-level-security/)

**Why both clauses are needed:**

A policy with `USING` alone allows reads but permits users to INSERT/UPDATE rows for any tenant (WITH CHECK defaults to true). A policy with `WITH CHECK` alone allows writes to pass validation but does not filter reads. Multi-tenant isolation requires both. — [Crunchy Data Blog: Row Level Security for Tenants](https://www.crunchydata.com/blog/row-level-security-for-tenants-in-postgres)

**FOR ALL vs. Per-Command Policies:**

- **`FOR ALL`** — applies to SELECT, INSERT, UPDATE, DELETE (most common for tenant isolation; one policy per table)
- **`FOR SELECT`** — read-only (e.g., public reference data)
- **`FOR INSERT`** — restricted write (rarely used alone)
- **`FOR UPDATE`** — row update restrictions
- **`FOR DELETE`** — row deletion restrictions

For tenant isolation, `FOR ALL` is the norm; per-command policies add complexity without benefit for the shared-schema case. — [PostgreSQL 18 Docs: Row Security Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)

**PERMISSIVE vs. RESTRICTIVE:**

- **PERMISSIVE (default)** — policies are combined with `OR` logic. If any permissive policy allows a row, it is visible/writable.
- **RESTRICTIVE** — policies are combined with `AND` logic. If any restrictive policy rejects a row, it is blocked, regardless of permissive policies.

For tenant isolation, only **PERMISSIVE** policies are needed (one tenant policy per table, deny all others implicitly via default-deny if no policy matches). — [PostgreSQL 18 Docs: Row Security Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)

**Type Cast Discipline:**

`current_setting()` returns `text`. For safe comparison with UUID or integer columns:

```sql
-- ✅ CORRECT: Explicit cast
USING (tenant_id = current_setting('app.current_tenant_id')::uuid)
USING (tenant_id = current_setting('app.current_tenant_id')::bigint)

-- ⚠️ RISKY: Implicit cast (works but less obvious)
USING (tenant_id::text = current_setting('app.current_tenant_id'))
```

Always cast `current_setting()` to the target column type for clarity and to avoid planner confusion. — [AWS Prescriptive Guidance: RLS for Multi-Tenant SaaS](https://docs.aws.amazon.com/prescriptive-guidance/latest/saas-multitenant-managed-postgresql/rls.html)

### 3. Session Variable Mechanics: SET LOCAL, set_config(), Type Safety

**SET LOCAL Scope (PostgreSQL 9.1+):**

```sql
BEGIN;
SET LOCAL app.current_tenant_id TO 'acme-tenant';
SELECT * FROM orders;  -- Sees only acme-tenant's rows
COMMIT;  -- SET LOCAL is reverted here
-- Next transaction starts with no app.current_tenant_id set (or reverts to session default)
```

- **Scope**: `SET LOCAL` changes apply **only to the current transaction**, not the session.
- **Behavior on COMMIT/ROLLBACK**: Changes are reverted regardless of commit or rollback. — [PostgreSQL 18 Docs: SET](https://www.postgresql.org/docs/current/sql-set.html)
- **Outside a transaction block**: `SET LOCAL` outside `BEGIN...COMMIT` emits a warning; the setting still applies to the implicit transaction that follows.

**Alternatives: `set_config()` Function:**

```sql
BEGIN;
SELECT set_config('app.current_tenant_id', $1::text, true);  -- true = local (transaction-scoped)
SELECT * FROM orders;
COMMIT;
```

- `set_config(setting_name, value, is_local)` — the `is_local` parameter (boolean) controls scope: `true` = LOCAL (transaction), `false` = SESSION. — [DBI Services: set_config and current_setting](https://www.dbi-services.com/blog/postgresql-set_config-and-current_setting/)
- **Parameterization advantage**: `set_config($1::text, $2::text, true)` can bind tenant ID safely without SQL injection risk (both parameters are bound).
- **SQL Injection Safety**: `SET LOCAL 'value'` concatenated from a string is vulnerable; parameterization via `set_config()` is safer. However, `SET LOCAL` with a parameter is not possible in raw SQL; use `set_config()` for parameterized RLS setup. — [Bytebase: PostgreSQL RLS](https://www.bytebase.com/reference/postgres/how-to/postgres-row-level-security/)

**Reading the Value: `current_setting()`:**

```sql
-- ✅ Safe: missing_ok = true (does not error if not set)
SELECT current_setting('app.current_tenant_id', true);  -- Returns NULL if unset

-- ⚠️ Risky: missing_ok = false (errors if unset)
SELECT current_setting('app.current_tenant_id', false);  -- Throws error: unrecognized configuration parameter
```

The second parameter (missing_ok, boolean) controls behavior when the setting is not set. Use `true` to return NULL; use `false` to throw an error. For RLS, missing_ok should be `false` (fail if tenant context is not set), but the policy must handle both NULL and "missing_ok=true" defensively. — [PostgreSQL 18 Docs: SET](https://www.postgresql.org/docs/current/sql-set.html)

**Type Safety in Comparisons:**

Always cast `current_setting()` to match the column type:

```sql
-- UUID column
USING (tenant_id = current_setting('app.current_tenant_id', false)::uuid)

-- Bigint column
USING (tenant_id = current_setting('app.current_tenant_id', false)::bigint)

-- Text column
USING (tenant_name = current_setting('app.current_tenant_id', false)::text)
```

PostgreSQL can coerce implicitly, but explicit casting makes intent clear and helps the planner. — [AWS Prescriptive Guidance](https://docs.aws.amazon.com/prescriptive-guidance/latest/saas-multitenant-managed-postgresql/rls.html)

### 4. Connection Pooling Survival: PgBouncer, Supavisor, RDS Proxy

**PgBouncer Transaction Mode (the most common pooling pattern):**

PgBouncer's `pool_mode = transaction` (default in many deployments) assigns a server connection to a client only for the duration of a single transaction. After `COMMIT` or `ROLLBACK`, the connection is returned to the pool and may be reused by a different client.

**SET LOCAL is SAFE in transaction mode:**

```sql
BEGIN;
SET LOCAL app.current_tenant_id TO 'tenant-a';
SELECT * FROM orders;  -- Sees tenant-a's rows
COMMIT;  -- Connection returned to pool, SET LOCAL reverted
-- Next client may get same connection; SET LOCAL change is gone
```

`SET LOCAL` changes **are reverted on COMMIT/ROLLBACK**, so they cannot leak to the next pooled client. — [Heroku: Best Practices for PgBouncer](https://devcenter.heroku.com/articles/best-practices-pgbouncer-configuration) and [PgBouncer Docs: Transaction vs. Session Pooling](https://upsystems.net/blog/pgbouncer-transaction-vs-session-pooling)

**SET (without LOCAL) is DANGEROUS in transaction mode:**

```sql
-- ❌ DANGEROUS in transaction mode
BEGIN;
SET app.current_tenant_id TO 'tenant-a';  -- Defaults to SET SESSION
COMMIT;  -- Connection returned, but SET persists on it
-- Next client reuses connection; sees tenant-a's setting!
```

`SET SESSION` or plain `SET` persists after COMMIT, so the setting leaks to the next client in the pool. This is a **critical security bug** for multi-tenant systems. — [PgBouncer Configuration Guide](https://devcenter.heroku.com/articles/best-practices-pgbouncer-configuration)

**Supavisor (Supabase's Cloud-Native Pooler):**

Supavisor is a multi-tenant connection pooler developed by Supabase. Multi-tenancy is handled by embedding the tenant ID in the username (e.g., `app_user.tenant-a`), not by session variables. — [Supavisor GitHub: Supabase Connection Pooler](https://github.com/supabase/supavisor) and [Supavisor FAQ](https://supabase.github.io/supavisor/faq/)

**Guidance for Supavisor:** Supavisor does not officially document `SET LOCAL` behavior. Safest approach: use connection-per-tenant usernames (Supavisor's intended pattern) or test `SET LOCAL` + Supavisor thoroughly before production. — [Pickuma: Postgres Pooling 2026 Comparison](https://pickuma.com/for-dev/postgres-connection-pooling-pgbouncer-vs-supavisor/)

**RDS Proxy (AWS-Managed Pooler):**

RDS Proxy documentation recommends **session pooling or connection pinning** for SET/SET LOCAL operations. `SET LOCAL` causes **session pinning** — the connection remains bound to the client until `RESET ALL` is issued, preventing multiplexing. This defeats the purpose of connection pooling for that client. — [AWS RDS Proxy Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.html)

**Connection Pooling Summary for varco:**

| Pooler | SET LOCAL Safe? | Notes | Recommendation |
|--------|-----------------|-------|-----------------|
| PgBouncer (transaction) | ✅ YES | Reverted on COMMIT; safe pooling | Default choice; use SET LOCAL in SQLAlchemy `after_begin` event |
| PgBouncer (session) | ⚠️ UNSAFE | Client pins connection; defeats pooling | Avoid if possible; requires connection pinning |
| Supavisor | ⚠️ UNCLEAR | Pools by tenant username; SET LOCAL untested | Use tenant-in-username pattern, test SET LOCAL thoroughly |
| RDS Proxy | ⚠️ SESSION PIN | SET LOCAL causes pinning; degrades performance | Use sparingly; rely on connection pinning only if necessary |
| pgcat / PostgreSQL drivers with pooling | ❓ UNKNOWN | Behavior varies; test before production | Verify pooler docs; default to SET LOCAL in transaction |

**Guaranteed safe pattern: RESET ALL on pool return:**

Some deployments add `RESET ALL` before returning a connection to the pool, ensuring no session state leaks. — [Heroku PgBouncer Guide](https://devcenter.heroku.com/articles/best-practices-pgbouncer-configuration)

### 5. Migration Story for Existing Tables: ALTER TABLE ENABLE RLS

**Locking Behavior:**

`ALTER TABLE ... ENABLE ROW LEVEL SECURITY` acquires an **AccessExclusiveLock**, the most restrictive lock level (blocks all concurrent reads and writes). However, the lock is held **briefly** — typically milliseconds for a small table. — [PostgreSQL 18 Docs: ALTER TABLE](https://www.postgresql.org/docs/current/sql-altertable.html) and [MonPG: PostgreSQL Lock Reference](https://monpg.app/blog/postgresql-alter-table-locks)

**Safe Ordering for Zero-Downtime Migration:**

1. **Step 1 (in a migration, within a transaction):** Create all policies for the table.
   ```sql
   CREATE POLICY tenant_isolation ON orders
     FOR ALL
     USING (tenant_id = current_setting('app.current_tenant_id', false)::uuid)
     WITH CHECK (tenant_id = current_setting('app.current_tenant_id', false)::uuid);
   ```

2. **Step 2 (in the same or a separate migration):** Enable RLS and optionally FORCE (if app role owns the table).
   ```sql
   ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
   ALTER TABLE orders FORCE ROW LEVEL SECURITY;
   ```

**Critical: the default-deny behavior:**

When RLS is enabled without a policy, a **default-deny policy** applies — all rows become invisible and immutable to non-superuser roles. This is fail-closed, not a silent no-op. — [PostgreSQL 18 Docs: Row Security Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html) and [Supabase Docs: Row-Level Security](https://supabase.com/docs/guides/database/postgres/row-level-security)

If policies are created before RLS is enabled, no gap exists. If RLS is enabled before policies exist, the table is locked down immediately (fail-closed, safe but disruptive). — [Atlas: Managing RLS Policies as Code](https://atlasgo.io/blog/2024/07/09/v025-row-level-security)

**Answer to the open question (S12 DDL change):**

**Does RLS-by-default change existing DDL?**

No. `ENABLE ROW LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY` are pure DDL operations on the table definition, not data migrations. The table's columns, indexes, constraints, and stored data are unchanged. Existing applications that set tenant context (via `SET LOCAL` or `set_config()`) will continue to work without any data migration.

**However**, if RLS is enabled without policies first, existing queries break immediately (default-deny). **Correct approach**: Policies are added in the migration that enables RLS, in the same transaction. No separate migration needed.

### 6. SQLAlchemy 2.x Async Integration: Event Hooks for Tenant Context

**The Event Hook Pattern (SQLAlchemy 2.x + async):**

SQLAlchemy 2.x provides lifecycle events on the `Session` class (sync-style) that also apply to `AsyncSession`. For setting tenant context per transaction:

```python
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession
from varco_core.tenancy import current_tenant

@event.listens_for(AsyncSession, "after_begin")
async def receive_after_begin(session, transaction, connection):
    """Set tenant context at the start of every transaction."""
    tenant_id = current_tenant()
    if tenant_id is None:
        raise RuntimeError("Tenant context not set; cannot execute query")
    
    # Use set_config() for parameterized, pooler-safe tenant setting
    await connection.execute(
        text("SELECT set_config(:param, :value::text, true)"),
        {"param": "app.current_tenant_id", "value": str(tenant_id)}
    )
```

**Key Details:**

- **Register on the sync `Session` class**, not `AsyncSession` — the async session inherits event handlers from the sync session. — [SQLAlchemy 2.1 Docs: Asynchronous I/O](https://docs.sqlalchemy.org/en/21/orm/extensions/asyncio.html) and [SQLAlchemy GitHub Discussion #10469](https://github.com/sqlalchemy/sqlalchemy/discussions/10469)
- **Use `after_begin` event** — fires at the start of every transaction, including nested transactions. Perfect for tenant-context initialization. — [SQLAlchemy Docs: Session Events](https://docs.sqlalchemy.org/en/20/orm/events.html)
- **Use `set_config()` for parameterization** — avoids SQL injection and is pooler-safe (does not require `SET LOCAL` raw SQL syntax). — [DBI Services: set_config](https://www.dbi-services.com/blog/postgresql-set_config-and-current_setting/)
- **Async/await discipline** — the event callback is `async`, but `session.execute()` is awaited to ensure the setting is persisted before the first query runs.

**Alternative: Explicit Per-Query Context:**

Some teams prefer explicit, imperative tenant-setting at the UoW level (not auto-wired via events), to keep tenant-awareness visible in application code. This trades boilerplate for clarity but loses the "fail-closed without RLS if set is forgotten" safety net. — [Medium: Multi-Tenancy with RLS in Postgres](https://medium.com/@anand_thakkar/row-level-security-rls-in-postgresql-for-multi-tenant-saas-apps-ef8c324031d0)

### 7. Mechanical Guards Against Tenant-Less Queries

**The Core Problem:**

Even with RLS enabled, a developer may forget to set the tenant context via `SET LOCAL` / `set_config()`, leaving the session variable unset or NULL. If the policy allows NULL values (e.g., `tenant_id IS NULL OR tenant_id = current_setting(...)`), the query returns no rows but does not error. This is silent failure, not fail-closed.

**Detection Mechanism 1: SQLAlchemy `before_execute` Event (Runtime Check):**

```python
from sqlalchemy import event, text

@event.listens_for(AsyncSession, "before_execute")
def receive_before_execute(conn, clauseelement, multiparams, params, execution_options):
    """Assert tenant context is set before executing queries on tenant-scoped tables."""
    tenant_id = current_tenant()
    if tenant_id is None:
        raise RuntimeError(
            "Tenant context not set; cannot execute query. "
            "Call set_current_tenant(id) before queries."
        )
```

**Limitations of runtime checks:**

- Only catches queries that actually execute; static analysis during development is not triggered.
- False negatives: raw `text()` SQL, complex joins where the predicate is on a joined table, CTEs, subqueries (difficult to analyze in the compiled statement tree).
- Per-column verification (checking that a specific `tenant_id` predicate exists) requires parsing the compiled SQL or AST tree, which is fragile and SQLAlchemy-version-dependent. — [Medium: Multi-Tenancy with Postgres RLS](https://medium.com/picus-security-engineering/enforcing-db-level-multi-tenancy-using-postgresql-row-level-security-c11d037d3f49)

**Detection Mechanism 2: Query Compilation Analysis (Whitebox Check):**

```python
from sqlalchemy import inspect

def assert_tenant_filter_in_where(compiled_stmt):
    """Verify a compiled statement includes a tenant filter in the WHERE clause."""
    # Parse compiled.statement (ClauseElement tree)
    # Look for a BinaryExpression with left operand = tenant_id column
    # This is fragile and not recommended for production
    pass
```

**Reality of whitebox checks:**

No mainstream framework (Django, Rails, Spring Data, SQLAlchemy docs) ships a built-in tenant-filter assertion. The reason: analyzing a compiled query to verify a specific predicate is present is **not a reliable static or runtime check** — it requires deep knowledge of the query planner and can be fooled by:
- Implicit casts and type coercion
- Join conditions that reference tenant_id on a different table
- Subqueries where tenant filter is in the outer query but not the subquery
- Raw SQL that bypasses the ORM entirely

**Recommended approach for varco:**

1. **Fail-closed default at the session level** — the `after_begin` event sets tenant context; if it is not set (tenant is None), throw an error before any query executes (Mechanism 1).
2. **RLS as the final backstop** — enable `FORCE ROW LEVEL SECURITY` on all tenant-scoped tables. An RLS policy filters rows even if the app forgets the WHERE clause.
3. **Integration tests** — test queries without tenant context and verify they fail (either via the session-level guard or RLS default-deny).
4. **No production per-query analysis** — do not ship a whitebox assertion over compiled SQL; the maintenance burden and false-negative risk are high. — Evidence from [Brief 006: Multi-Tenant Identity and Hardening](https://github.com/edoardoscarpaci/varco/blob/main/design/research/006-multi-tenant-identity-and-hardening.md), Evidence Gap 2.

**S15 (Applicator-level tenant-filter assertion) verdict:**

S15 proposes a query analyzer that asserts a TENANT-scoped entity's compiled query carries a tenant predicate. The evidence shows this is **a whitebox assertion over compiled SQL that is fooled by edge cases (raw queries, subquery filters, joins)**. It can catch some bugs in dev but should be **opt-in, never a hard gate**, and documented as having known false-negative modes. — [SQLAlchemy GitHub: Query Validation Discussions](https://groups.google.com/g/sqlalchemy/c/20ieAvQCp_4)

### 8. RLS Performance: Overhead, Index Usage, Function Stability

**Measured Overhead:**

PostgreSQL injects RLS policies as **security quals** (filtering conditions) into the query plan at plan time, not post-fetch. For simple policies, the overhead is negligible (1–3% slowdown on index-friendly queries). For complex policies or non-leakproof functions, overhead can reach 10–20%. — [MVP Factory: PostgreSQL RLS Without the Performance Tax](https://mvpfactory.io/blog/postgresql-row-level-security-without-the-performance-tax-policies-indexes-and) and [pganalyze: 5 mins of Postgres E28 — RLS](https://pganalyze.com/blog/5mins-postgres-row-level-security-bypassrls-security-invoker-views-leakproof-functions)

**Index Usage and the Planner:**

RLS policies can use indexes **if the policy expression is index-friendly** (e.g., `tenant_id = value`). However, if the policy calls a function that is not marked `LEAKPROOF`, the planner **cannot push the security qual down** to the index scan, and it is evaluated post-fetch for every row.

Example of index-friendly:
```sql
CREATE POLICY tenant_isolation ON orders
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid);
```

The planner can use an index on `tenant_id`. — [MVP Factory: RLS Without Performance Tax](https://mvpfactory.io/blog/postgresql-row-level-security-without-the-performance-tax-policies-indexes-and)

**Function Stability and Leakproofness:**

PostgreSQL distinguishes between function categories:

- **IMMUTABLE** — always returns the same output for the same input (e.g., `uuid_generate_v4()` is NOT immutable; `1 + 1` is).
- **STABLE** — returns the same output for the same input within a single query execution; can be reused for every row in a scan. `current_setting()` is STABLE, which is good. — [pganalyze Blog: LEAKPROOF Functions](https://pganalyze.com/blog/5mins-postgres-row-level-security-bypassrls-security-invoker-views-leakproof-functions)
- **VOLATILE** — can return different results for the same input; re-evaluated per row (slow). E.g., `now()`, `random()`.
- **LEAKPROOF** — a function that does not leak information about its arguments via side-channels (error messages, row visibility, timing). Required for security quals to be reordered before user-supplied predicates. Not many PostgreSQL functions are marked LEAKPROOF.

**For tenant RLS policies:**

Use `current_setting()` directly (STABLE, safe for index use) or wrap it in a function marked `STABLE` or `IMMUTABLE` if you create a custom tenant-lookup function. Avoid `VOLATILE` functions in the policy expression. — [PostgreSQL Docs: Row Security Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)

**Best Practice: Mark Custom Tenant Functions as STABLE:**

```sql
CREATE FUNCTION get_current_tenant_id() RETURNS uuid AS $$
  SELECT current_setting('app.current_tenant_id')::uuid
$$ LANGUAGE SQL STABLE;

-- Use in policy:
CREATE POLICY tenant_isolation ON orders
  USING (tenant_id = get_current_tenant_id());
```

Marking as `STABLE` allows the planner to cache the result per query execution. — [PostgreSQL Docs: Function Volatility Categories](https://www.postgresql.org/docs/current/xfunc-volatility.html)

**Performance Guidance:**

1. **Index your `tenant_id` columns** — a simple B-tree index is enough for RLS to work efficiently.
2. **Keep policies simple** — avoid complex OR/AND logic in the USING clause; factor complex rules into a STABLE function.
3. **Mark custom tenant functions as STABLE** (or IMMUTABLE if truly constant within a query).
4. **Profile before and after enabling RLS** — expected overhead is 1–3% for well-written policies; 10%+ overhead signals a problem (non-leakproof function, missing index, or overly complex policy).
5. **Do not mix RLS with mandatory search_path changes** — changing schema search paths can interfere with policy resolution; keep policies in the same schema as tables.

---

## Options Compared

**Scenario: varco 3.2 planning to enable RLS-by-default for TENANT-scoped tables**

### Enforcement Strategy Choice

| Option | ✅ Strengths | ❌ Weaknesses | Evidence |
|--------|------------|--------------|----------|
| **RLS-only (database enforces, app ignores context-setting)** | Simplest: one layer of defense; fail-closed if misconfigured | App can forget to set tenant context; queries silently return empty (confusing debugging); still requires connection pooling setup | Findings §4 show SET LOCAL is safe in PgBouncer, but operational burden is high without app-layer checks |
| **App-layer checks + RLS backstop (app sets context, RLS catches mistakes)** | Defense-in-depth; clear tenant-awareness in app code; catch bugs in dev; RLS as final defense against app bugs | Two places to configure; maintenance burden if layers diverge; SQLAlchemy event wiring is not trivial; still has false-negative modes (raw SQL, CTEs) | Findings §7 recommend this; Brief 006 calls it "standard"; MVP Factory & Crunchy Data both advocate dual-layer |
| **App-only (no RLS; tenant filters in WHERE clauses)** | Familiar pattern; no database-level setup; works across all data layers (including MongoDB) | Fragile: an ORM bug or typo omits the WHERE clause → data leak; common cause of BOLA vulnerabilities; no recovery if app forgets | OWASP API Top 10 2023 warns against this; Brief 006 §4 documents the risk |
| **RLS + Applicator assertion (app checks compiled queries, RLS is backstop)** | Catches some bugs at query-compile time (before execution); visible tenant predicates in SQL | Assertions are whitebox and fragile (false negatives: raw SQL, subqueries, CTEs); per-query overhead; difficult to maintain across SQLAlchemy versions | S15 backlog grades this 🟢 (nice to have) not 🔴 (critical); Findings §7 document the false-negative modes |

**Recommendation favoured by evidence:** **App-layer checks + RLS backstop** (Option 2) is the industry standard. varco should:
1. Make tenant-context setting **mandatory at the SQLAlchemy UoW level** (via `after_begin` event, failing if context not set).
2. Enable `FORCE ROW LEVEL SECURITY` on all TENANT-scoped tables.
3. Make the Applicator assertion (S15) **opt-in, documented as having known false-negative modes**, suitable for development/testing but not a production gate.

---

## Version/Compatibility Notes

### PostgreSQL Versions

- **PostgreSQL 18** (current, released Oct 2024): RLS stable, `FORCE ROW LEVEL SECURITY` available (since PG 13).
- **PostgreSQL 17, 16, 15, 14, 13** (supported): RLS stable, `FORCE ROW LEVEL SECURITY` available since 13.
- **PostgreSQL 12, 11, 10, 9.6, 9.5** (unsupported): RLS available but without `FORCE` (use with caution; table owner bypass is a risk).
- **PostgreSQL 9.5 (Feb 2016)**: RLS introduced; same enforcement model as today.

**varco support matrix**: Python 3.12/3.13, SQLAlchemy 2.x async. Recommend **PostgreSQL 14+** for production RLS setups (better planner, stable FORCE keyword, modern pooling).

### Connection Pooler Versions

- **PgBouncer 1.15+** (2021): `SET LOCAL` in transaction mode documented as safe. No planned breaking changes.
- **PgBouncer 1.20+** (2024): Latest stable; same behavior.
- **Supavisor 0.1+** (2024, Supabase beta): Multi-tenant pooler; `SET LOCAL` behavior untested at scale in production. Recommended pattern: tenant-in-username, not SET LOCAL.
- **AWS RDS Proxy (2019+)**: Supports transaction-pooling; `SET LOCAL` causes session pinning. Document as a trade-off, not a bug.

### SQLAlchemy Versions

- **SQLAlchemy 2.0+**: `AsyncSession`, event listeners, `after_begin` event are all stable. The `@event.listens_for(Session, ...)` pattern (register on sync Session, applies to AsyncSession) is documented and works reliably.
- **SQLAlchemy 2.1–2.3** (current): No breaking changes to event model or AsyncSession.

### Recommended Stack for varco 3.2

| Component | Version | Rationale |
|-----------|---------|-----------|
| PostgreSQL | 14+ | FORCE ROW LEVEL SECURITY, modern planner |
| PgBouncer | 1.20+ (or managed Supavisor) | Stable, transaction-pooling well-tested |
| SQLAlchemy | 2.1+ | Async stability, event model mature |
| Python | 3.12–3.13 | varco baseline; no TLS/asyncio issues with recent versions |

---

## Evidence Gaps

1. **Measured RLS overhead in varco's actual workload**: The brief cites 1–3% overhead for simple policies and 10–20% for complex ones, but no benchmark exists for varco's specific query patterns (multi-tenant filter + complex JOINs + CTEs). Plan: add a benchmark to varco's CodSpeed suite comparing query time with/without RLS on a realistic model.

2. **Supavisor's SET LOCAL behavior at scale**: Supavisor is a recent, cloud-native pooler; production deployments are limited. No published case study of multi-million-row tables with `SET LOCAL` in Supavisor pools. Recommended: test thoroughly in a staging environment before production.

3. **Applicator-level query assertion false-negative catalog**: The brief identifies false-negative modes (raw SQL, subqueries, CTEs) but lacks a comprehensive, open-source tool or documented recipe for implementing S15's assertion. A library that can reliably detect tenant predicates in SQLAlchemy 2.x compiled statements does not exist (as of 2026-09).

4. **Comparative cost of schema-per-tenant vs. row-level isolation**: Both approaches solve tenant isolation; RLS cost is documented, but schema-per-tenant operational overhead (connection pooling, DDL replication, migration orchestration) is not quantified in this brief. Worth a separate performance analysis.

5. **Migration safety on high-volume production tables (>1B rows)**: The brief notes that `ALTER TABLE ... ENABLE RLS` takes an `AccessExclusiveLock` briefly, but no detailed downtime estimate exists for a 1B-row table during `ALTER TABLE` in a live system. Recommend testing on a realistic data volume before rolling out.

---

## Librarian's Note

The evidence **strongly favors enabling RLS on all TENANT-scoped tables in varco 3.2**, paired with **mandatory app-layer tenant-context setting at the UoW level** (via SQLAlchemy's `after_begin` event). This satisfies the backlog open question: RLS-by-default does **not** change existing DDL; policies are added in the migration that enables RLS, in the same transaction (no separate data migration needed).

S12 (RLS-by-default) should:
1. Use `FORCE ROW LEVEL SECURITY` (mitigates table-owner bypass if migrations run as app role).
2. Wire tenant-context setting into `varco_sa`'s UoW via `@event.listens_for(Session, "after_begin")` (Findings §6 shows the pattern).
3. Document the PgBouncer transaction-mode requirement (`SET LOCAL` is safe; `SET SESSION` is a security bug).

S15 (Applicator assertion) should be **deferred or made opt-in**, graded as a dev-time safety feature with documented false-negative modes (Findings §7), not a production gate. RLS itself is the production-grade safeguard.

The decision is upstream; this brief provides the configuration pattern, version support, and operational constraints (pooling, migration safety, performance).

