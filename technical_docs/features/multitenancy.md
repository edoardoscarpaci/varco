# Multitenancy — isolation strategies, control plane, and global scope

Plan 007, extended by Plan 008 (control-plane entry-point convergence, fleet
broadcast/fan-out, and fleet readiness). Tenant data isolation is a **selectable deployment strategy**
(`varco_core.tenancy.TenantIsolation`), not a single hard-coded shape. A
**dynamic tenant control plane** (REST + event-driven onboarding/offboarding,
backed by a durable catalog) can be deployed standalone or bundled into an
app. **Globally-scoped (shared, non-tenant-routed) entities**
(`varco_core.tenancy.TenantScope`) are a first-class concept under every
strategy.

With every default (`TenancySettings()`), a deployment is **byte-identical**
to pre-Plan-007 behaviour: no pool, no extra engine/client, no symbolic
schema, no control-plane surface. Nothing here runs unless you opt in.

## Layering

```mermaid
flowchart TD
    Core["varco_core.tenancy<br/>contracts only, zero third-party deps"]
    SA["varco_sa.tenancy<br/>SASchemaRouter · SAEngineRegistry · SATenantCatalog<br/>rls_check · global_scope · admin/"]
    Beanie["varco_beanie.tenancy<br/>BeanieTenantPool · BeanieTenantBinding<br/>BeanieDatabaseProvisioner · BeanieTenantCatalog"]
    FastAPI["varco_fastapi.tenancy<br/>TenancyLifecycle · TenantResolutionMiddleware<br/>build_tenant_router · mount_tenant_admin"]

    Core --> SA
    Core --> Beanie
    SA --> FastAPI
    Beanie --> FastAPI
```

`varco_fastapi.tenancy` imports **only** `varco_core.tenancy` — never
`varco_sa`, `varco_beanie`, `sqlalchemy`, or `pymongo` (enforced by an
import-guard test). Same seam rule as `AbstractEventBus`/`AbstractMigrator`.

## Where the tenant comes from

Everything above assumes the tenant id is already known and trustworthy. Plan 033 / S6 answers
the separate, upstream question — *which of the request's signals (a header, a subdomain, a JWT
claim) is allowed to name the tenant, and what happens when two of them disagree* — via
`varco_core.tenancy.source.TenantSourceChain` and
`varco_fastapi.middleware.tenant_resolution.TenantResolutionMiddleware(chain=...)`. Full design,
the trust-ranking table, the two cross-check modes, membership binding, and act-as delegation:
`technical_docs/features/tenant-provenance.md`. Nothing here changes: `TenantResolutionMiddleware
(chain=None)` — the only shape that existed before Plan 033 — is byte-identical to today.

## The six strategies

| # | Backend | Strategy |
|---|---------|----------|
| 1 | Postgres | shared schema + discriminator column (**default, unchanged**) |
| 2 | Postgres | shared schema + discriminator + **RLS asserted** |
| 3 | Postgres | **schema-per-tenant** (`schema_translate_map`) |
| 4 | Postgres | **database-per-tenant** (bounded per-tenant engine pool + fan-out) |
| 5 | MongoDB | shared collection + discriminator (**default, unchanged**) |
| 6 | MongoDB | **database-per-tenant** (per-tenant Document clones + `init_beanie`) |

`enforce_rls: bool` is a hardening flag on `SHARED`, not a fourth enum value
— strategies 1 and 2 differ only by it. Only three `TenantIsolation` values
exist: `SHARED`, `SCHEMA` (Postgres only), `DATABASE` (Postgres + Mongo).

### Decision table

Ceiling figures are **order-of-magnitude, environment-dependent guidance —
varco enforces no cap and does not choose a strategy for you.**

| | 1. PG shared | 2. PG shared + RLS | 3. PG schema/tenant | 4. PG database/tenant | 5. Mongo shared | 6. Mongo database/tenant |
|---|---|---|---|---|---|---|
| **Isolation strength** | weakest — app-layer only, fails open on any bypassing query | strong — DB-enforced per row | strong — separate namespace; a wrong query errors rather than leaks | strongest — separate DB, separate credentials possible | weakest — app-layer only | strongest available on Mongo |
| **Blast radius of a bug** | all tenants | one tenant | one tenant | one tenant | all tenants | one tenant |
| **Ops cost** | none | one reviewed RLS revision per table | schema provisioning per tenant | DB provisioning, per-DB backup, connection budgeting, relay fan-out | none | DB provisioning per tenant, relay fan-out |
| **Tenant-count guidance** | ~unbounded | ~unbounded | 1000s (catalog bloat, slower `pg_dump`) | 10s–100s (connection + poller budget) | ~unbounded | 10s–100s (client/clone budget) |
| **Migration cost** | 1 run | 1 run | 1 global + N | 1 global + N | 1 run | 1 global + N |
| **Global / shared data** | trivial | trivial — skipped by RLS assertion | ✅ same-database join in one transaction | ⚠️ separate DB → dual UoW, no cross-scope transaction; read-only by default | trivial | ⚠️ separate DB → dual UoW |
| **Framework tables** | shared | shared | shared, untranslated schema | per-tenant — requires the fan-out supervisor | shared | per-tenant — requires the fan-out supervisor |
| **Control plane** | optional | optional | recommended | **required** | optional | **required** |
| **Pick it when** | internal / low-risk | **default recommendation** | namespace separation, per-tenant `pg_dump`, frequent global↔tenant joins | contractual hard isolation, few large tenants, per-tenant restore | internal / low-risk | per-tenant hard isolation, `dropDatabase` erasure |

**Headline recommendation:** strategy **2** (shared schema + RLS asserted)
suits almost everyone — the only option that is both DB-enforced and
scale-unbounded. Strategies 3/4/6 are for contractual, regulatory, or
per-tenant-restore requirements, not performance.

---

## `TenancySettings`

`varco_core.tenancy.TenancySettings` (frozen dataclass, `from_env(env=None)`
mirroring `MigrationSettings`):

| Field | Env var | Default |
|---|---|---|
| `isolation` | `VARCO_TENANCY_ISOLATION` | `shared` |
| `enforce_rls` | `VARCO_TENANCY_ENFORCE_RLS` | `false` |
| `schema_template` | `VARCO_TENANCY_SCHEMA_TEMPLATE` | `t_{tenant_id}` |
| `db_template` | `VARCO_TENANCY_DB_TEMPLATE` | `db_{tenant_id}` |
| `max_entries` | `VARCO_TENANCY_MAX_ENTRIES` | `50` |
| `idle_ttl_s` | `VARCO_TENANCY_IDLE_TTL` | `300.0` |
| `catalog_ttl_s` | `VARCO_TENANCY_CATALOG_TTL` | `60.0` |
| `fanout_framework_tables` | `VARCO_TENANCY_FANOUT_FRAMEWORK_TABLES` | `false` |
| `global_dsn` | `VARCO_TENANCY_GLOBAL_DSN` | `None` (falls back to the app's own DSN) |
| `global_writable` | `VARCO_TENANCY_GLOBAL_WRITABLE` | `false` |

⚠️ **No `VARCO_TENANCY_MOUNT_ADMIN` env var exists, anywhere.** The admin
surface can only be mounted via an explicit, acknowledged code call — see
[Standalone vs bundled control plane](#standalone-vs-bundled-control-plane).

---

## Recipe 1 — strategy 1/2: shared schema, optionally RLS-asserted

This is the pre-Plan-007 default. Nothing to wire for strategy 1. For
strategy 2, turn on the assertion (never DDL — see
[Postgres RLS](postgres-rls.md)):

```python
from varco_core.tenancy import TenancySettings, TenantIsolation

settings = TenancySettings(isolation=TenantIsolation.SHARED, enforce_rls=True)
```

```python
from varco_sa.tenancy.rls_check import assert_rls_enabled

async with engine.connect() as conn:
    await assert_rls_enabled(
        conn,
        tables={"orders", "invoices"},
        global_tables={"reference_data"},  # TenantScope.GLOBAL — always skipped
        framework_tables=framework_table_names(),
        enforce=True,  # raises TenantIsolationError naming the fix + doc path
    )
```

`assert_rls_enabled()` never emits DDL. Apply RLS itself with
`varco_sa.migration.ops.rls_upgrade(op, "orders")` in a reviewed revision —
see [Postgres RLS](postgres-rls.md) for the full guide, including why
`(SELECT current_setting(...))` matters and the `SET`-vs-`SET LOCAL` hazard.

**Framework tables — `varco_audit_log` and `varco_dead_letters` (Plan 009,
Phase 6 / R4).** Both tables gained a `tenant_id` column in the
`0002_dlq_audit_tenant_id` revision and are RLS-eligible the same way any
app table is; `framework_table_names()` already includes them, so passing it
as `framework_tables=` to `assert_rls_enabled()` covers both automatically.
Applying RLS to them uses a dedicated one-call helper rather than the
generic `rls_upgrade(op, "orders")` shown above, since there are two fixed
table names to enable in one revision:

```python
from varco_sa.rls_framework import framework_rls_upgrade, framework_rls_downgrade


def upgrade() -> None:
    framework_rls_upgrade(op)  # varco_audit_log + varco_dead_letters, both by default


def downgrade() -> None:
    framework_rls_downgrade(op)
```

Same rule as every other RLS helper in this codebase: nothing calls this
automatically — paste it into a reviewed migration. See
`technical_docs/features/dead-letter-queues.md` and
`technical_docs/features/database-auditing.md`'s "Multitenancy" sections for
the `tenant_id`-stamping behaviour these tables now carry (including the
deliberate `None`-tenant asymmetry on `varco_dead_letters`).

## Recipe 2 — strategy 3: Postgres schema-per-tenant

```python
from varco_sa.tenancy.router import SASchemaRouter

router = SASchemaRouter(schema_template="t_{tenant_id}")
session_factory = router.session_factory_for(engine, tenant_id="acme")
# every session from this factory resolves the symbolic "tenant" schema
# token to "t_acme"; global + framework tables (no symbolic token) resolve
# to the untranslated default schema.
```

`SAModelFactory.build(..., isolation="schema")` stamps the symbolic schema
token onto every `TENANT`-scoped model's generated `__table__.schema`. Under
`SHARED` (the default), `__table__.schema is None` — byte-identical to
today. `GLOBAL` models and the ten framework tables never carry the token,
regardless of `isolation`.

**⚠️ Raw `text()` SQL is NOT translated by `schema_translate_map`** — it
resolves via the connection's real `search_path`, so a hand-written query
must self-qualify its own schema (`SELECT * FROM t_acme.orders`, not
`SELECT * FROM orders`). This is the one caveat `schema_translate_map`
cannot close; only code review can. See [Postgres RLS §3](
postgres-rls.md#3-schema-per-tenant-the-supported-mechanism-is-schema_translate_map-not-search_path)
for the full "why not `SET LOCAL search_path`" rationale.

Provisioning a schema:

```python
from varco_sa.tenancy.provisioner import SASchemaProvisioner

provisioner = SASchemaProvisioner(engine=engine)
await provisioner.provision("acme")  # CREATE SCHEMA IF NOT EXISTS
await provisioner.deprovision("acme", confirm_destroy=True)  # DROP SCHEMA ... CASCADE
```

## Recipe 3 — strategy 4: Postgres database-per-tenant

```python
from varco_sa.tenancy.engine_registry import SAEngineRegistry

registry = SAEngineRegistry(
    db_template="db_{tenant_id}",
    pool_size=1,
    max_overflow=2,  # per-tenant engines are mostly idle
    max_entries=50,
)
engine = await registry.ensure("acme")  # cached, LRU-evicted, refcounted
```

Cluster DDL (`CREATE DATABASE`/`DROP DATABASE`) is **confined to the control
plane** (RD-4) — see [Admin confinement](#admin-confinement-rd-4) below.
Framework tables (outbox/jobs/audit) live in each tenant's own database
under this strategy — you **must** enable [fan-out](#fan-out-rd-8) or those
rows are never published.

## Recipe 4 — strategy 6: Mongo database-per-tenant

```python
from varco_beanie.tenancy.pool import BeanieTenantPool

pool = BeanieTenantPool(
    client=mongo_client,
    db_template="db_{tenant_id}",
    document_models=[UserDocument, OrderDocument],
    max_entries=50,
)
binding = await pool.ensure("acme")  # BeanieTenantBinding — per-tenant Document clones
UserClone = binding.clone_for(UserDocument)
```

`TenantIsolation.SCHEMA` raises `ValueError` on `BeanieTenantPool` — MongoDB
has no schema-per-tenant equivalent; it is caught at construction, not
silently downgraded to `SHARED`.

### Resource cost of Mongo database-per-tenant (RD-7)

`init_beanie()` binds each Document **class** to one database via
class-level state, and `BeanieDocRegistry` is process-global, keyed by
domain class. A second `init_beanie()` call with a different database would
therefore **rebind every Document class globally** — the last tenant to
onboard would silently make every tenant read one database. To avoid this,
`varco_beanie.tenancy.binding.build_tenant_binding()` clones each Document
class **per tenant** and calls `init_beanie()` once per tenant, against
that tenant's own clone set. Clones never register in `BeanieDocRegistry`
— `BeanieDocRegistry.get(User)` keeps returning the **base** class; that is
the documented contract, not a bug.

**The cost, stated plainly:** `N_active_tenants × N_models` live Document
classes. Worked example: 6 Document models, `max_entries=50` (the pool
default) → **at most 300 classes resident at once**, however many tenants
have ever onboarded. `BeanieTenantPool.active_clone_count()` observes the
resident tenant count directly (`× N_models` gives the class count) —
export it as a gauge if you need to watch it in production.

⚠️ **Known limitation:** `build_tenant_binding()`'s `init_beanie()` call is
**best-effort** — a failure (most commonly no live Mongo client wired, e.g.
a unit-test/bootstrap context) is logged (`logger.debug(..., exc_info=True)`)
and swallowed rather than propagated, so the function always returns a
binding with real clone classes even without a reachable database. In
production this means a genuinely misconfigured deployment does **not**
fail at binding-build time — it fails later and loudly, the first time a
repository operation tries to use an uninitialized collection. Exercise the
real-Mongo path (`varco_beanie/tests/test_beanie_tenant_integration.py`,
`-m integration`) before relying on binding-build failures to catch a
misconfiguration.

---

## Global/shared scope

Declare an entity `TenantScope.GLOBAL` when every tenant should read one
shared copy:

```python
class ReferenceData(DomainModel):
    class Meta:
        tenant_scope = TenantScope.GLOBAL  # default: TENANT
```

The default (`TENANT`, when `Meta.tenant_scope` is absent) is **fail-closed**
by design: a forgotten declaration routes per-tenant — worst case, a shared
table needlessly duplicated per tenant (visible, fixable) — rather than
un-routed, whose worst case is a tenant table landing in the shared schema
where every tenant reads every row (a silent cross-tenant leak). The ten
framework tables are **forced** `GLOBAL` regardless of declaration.

### Per-strategy semantics

- **`SHARED` (± RLS).** A global entity simply carries no `tenant_id` and no
  RLS policy — `GLOBAL` is a storage no-op. It is **not** a validation
  no-op: `assert_rls_enabled()` skips `GLOBAL` tables (see [Postgres RLS](
  postgres-rls.md)).
- **`SCHEMA`.** Global tables stay in the untranslated default schema — no
  symbolic token. Because tenant schemas and the global schema live in one
  database, a query may **join a tenant table to a global table inside one
  transaction, on one connection** — an advantage over `DATABASE`.
- **`DATABASE`** (Postgres and Mongo). Global entities live in the shared/
  control-plane database (same physical database as the control plane by
  default; `VARCO_TENANCY_GLOBAL_DSN` overrides). Reading them needs a
  **second, non-routed UoW** — **a single transaction cannot span a tenant
  database and the global database; there is no 2PC.** Any workflow needing
  to change both atomically goes through the transactional outbox or the
  saga primitives instead.

### The dual-UoW API

```python
from varco_core.tenancy import GlobalUoWProvider

class ArtifactService(AsyncService[Artifact, UUID, ...]):
    def __init__(self, uow_provider: Inject[GlobalUoWProvider], ...):
        ...
    def _get_repo(self, uow):
        return uow.artifacts
```

`GlobalUoWProvider` is a **distinct DI-token type**, not a parameter on
`IUoWProvider` — no change to that ABC's signature, so every existing
implementation (including third-party ones) keeps working untouched. It
works, identically, whether or not a `tenant_context()` is active — it
never consults `current_tenant()`. A service needing both scopes injects
both `GlobalUoWProvider` and the tenant-routed provider and sequences two
transactions; that is the truth of the underlying system, not an API
artifact.

**Service mixin guard.** A global-entity service must **not** mix in
`TenantAwareService` — `_scoped_params` would filter on a non-existent
`tenant_id`. There is no `GlobalScopedService` marker mixin (a marker with
an empty body is noise); instead `validate_service_scope()` catches the
mistake structurally:

```python
from varco_core.tenancy.scope_guard import validate_service_scope

validate_service_scope(
    ArtifactService,
    entity_cls=Artifact,
    tenant_scope=TenantScope.GLOBAL,
)
# raises TenantIsolationError if ArtifactService mixes in TenantAwareService
```

The reverse mismatch (a `TENANT`-scoped entity served with no tenant
filtering) only WARNs — under `SHARED` it is a real but non-fatal risk;
under `SCHEMA`/`DATABASE` isolation is often structural anyway.

### Writes to global data are read-only by default (RD-10)

The app-facing global credential is **read-only by default** — writes go
through the control plane, not an app pod. This is enforced at the
credential, not by a code convention: a denied write from Postgres surfaces
as a bare SQLSTATE `42501`, which `varco_sa.tenancy.global_scope` translates
into a legible `GlobalScopeReadOnlyError` naming the entity and both
remedies:

```
GlobalScopeReadOnlyError: Write to global-scoped entity 'ReferenceData' was
refused: the global credential is read-only by default (RD-10). Remedies:
(1) route the write through the tenant control plane instead of an app pod,
or (2) opt in explicitly with TenancySettings(global_writable=True) /
VARCO_TENANCY_GLOBAL_WRITABLE=true.
```

A `42501` raised through a **tenant** UoW is **not** translated — it is a
genuine permission bug, not the read-only-global design, and must not be
mislabelled as one.

### Caching

`tenancy_cache_key()` closes a symmetric pitfall pair: a `TENANT`-scoped key
that is *not* namespaced is a cross-tenant leak; a `GLOBAL`-scoped key that
*is* namespaced is N× cache waste and N× DB load.

```python
from varco_core.tenancy import tenancy_cache_key

tenancy_cache_key(Order, "42")  # "tenant:acme:Order:42" — inside tenant_context()
tenancy_cache_key(ReferenceData, "42")  # "global:ReferenceData:42" — identical across tenants
tenancy_cache_key(Order, "42")  # RuntimeError outside tenant_context() — fails closed
```

---

## Control plane

Onboarding/offboarding is **fully dynamic** — REST or event-driven, backed
by the durable catalog:

```mermaid
sequenceDiagram
    participant Client
    participant Router as build_tenant_router
    participant Bus as AbstractEventBus
    participant Consumer as TenantProvisionConsumer
    participant Svc as TenantControlService
    participant Catalog as AbstractTenantCatalog

    Client->>Router: POST /tenancy/tenants {tenant_id}
    Router->>Svc: provision(tenant_id)
    Bus-->>Consumer: TenantProvisionRequested (alternate entry point)
    Consumer->>Svc: provision(tenant_id)
    Svc->>Catalog: status: pending -> active
    Svc->>Bus: emit TenantCatalogChanged
    Bus-->>Catalog: every pod's CachedTenantCatalog invalidates
```

### Durable catalog + status lifecycle

`SATenantCatalog` (`varco_tenants`, the **tenth framework table** — self-
registers via `register_framework_metadata()`, no separate migration file
needed) and `BeanieTenantCatalog` (the `varco_tenants` collection) are the
authoritative production catalogs; `StaticTenantCatalog` is the test/
bootstrap double.

| status | in `list_tenants()` default | request routing | migration fan-out |
|---|---|---|---|
| `pending` | no | rejected (503) | no |
| `active` | yes | routed normally | yes |
| `suspended` | no | rejected (403) | yes (kept current, so resume is instant) |
| `deprovisioning` | no | rejected (410) | no |
| `deleted` | no | rejected (404); tombstoned, not removed | no |

Routing consults the catalog **before** `pool.ensure()` — a non-`active`
tenant never causes an engine/binding to be created.

`dsn_ref` on `TenantDescriptor` must be a **secret reference**, never a
literal DSN (RD-2) — `SATenantCatalog.add()`/`BeanieTenantCatalog.add()`
reject values that look like a literal connection string
(`postgresql://user:pass@host/db`) unless `allow_literal_dsn=True` (test/
bootstrap only).

**Cross-pod visibility** (`CachedTenantCatalog`) combines three mechanisms:
`TenantCatalogChanged` event invalidation (sub-second propagation),
a `catalog_ttl_s` TTL re-read (self-heals a **dropped** invalidation event —
buses do drop messages), and read-through on a cache miss (instant even on
a pod that missed the onboarding event entirely, with negative-cache rate
limiting for a persistently-unknown id).

### REST + event-driven onboarding — both entry points converge (Plan 008, RD-11)

```python
from varco_fastapi.tenancy.router import build_tenant_router

router = build_tenant_router(
    control_service,
    server_auth=admin_auth,
    admin_role="tenant-admin",
)
```

`POST /tenancy/tenants`, `GET /tenancy/tenants` (`status=` filter),
`GET /tenancy/tenants/{id}`, `PATCH /tenancy/tenants/{id}`
(`{"action": "suspend"|"resume"}`), `DELETE /tenancy/tenants/{id}`
(requires `{"confirm": true}` — omitted/false is 400, nothing happens),
`POST /tenancy/tenants/{id}/migrate`. Every route requires `admin_role` —
unauthorised calls are **403, never 500**. A duplicate `POST` is idempotent
(200, not 201). (Plan 008 adds three more routes — see [Fleet fan-out](
#fleet-fan-out-provision-vs-request_provision) and [Fleet readiness](
#fleet-readiness) below.)

Or drive it over the bus:

```python
from varco_core.tenancy.control.events import TenantProvisionRequested

await producer._produce(TenantProvisionRequested(tenant_id="acme"), channel="varco.tenancy")
```

```python
from varco_core.tenancy.control.consumer import TenantProvisionConsumer
from varco_core.tenancy.control.service import TenantControlService


class TenancyWiring:
    def __init__(
        self,
        bus: Inject[AbstractEventBus],
        catalog: Inject[AbstractTenantCatalog],
        provisioner: Inject[AbstractTenantProvisioner],
        producer: Inject[AbstractEventProducer],
    ):
        self._control_service = TenantControlService(
            catalog=catalog,
            provisioner=provisioner,
            producer=producer,
        )
        self._consumer = TenantProvisionConsumer(
            control_service=self._control_service,
            dlq=my_dlq,
        )
        self._bus = bus

    @PostConstruct
    def _setup(self) -> None:
        self._consumer.register_to(self._bus)
```

`TenantProvisionConsumer` is **safe-by-default**: `RetryPolicy.
durable_delivery()` + a `dlq` are wired unless explicitly overridden —
following `AuditConsumer`'s precedent, because a dropped provision event
means a paying tenant that never exists. `provision()` itself is idempotent
(a second call on an already-`ACTIVE` tenant performs no provisioner call —
the status *is* the idempotency check; no new dedup mechanism is invented).
A `TenantDeprovisionRequested` without `confirm=True` is rejected and, with
a DLQ wired, DLQ'd rather than retried forever.

**Both entry points drive exactly one catalog transition.** Before Plan 008,
`TenantProvisionConsumer` called `AbstractTenantProvisioner` directly —
storage was created but the catalog row never was, so `routing.py` /
`TenantResolutionMiddleware`'s catalog lookup 404'd the tenant forever
(deprovision had the mirror-image defect: destructive DDL ran while the
catalog still said `ACTIVE`, and the fan-out-supervisor-stop / pool-eviction
steps in `TenantControlService.deprovision()` were never reached from the
bus). Plan 008 (RD-11) makes the consumer take a `control_service=`
instead — the bus path now calls `TenantControlService.provision()` /
`.deprovision()`, the exact same transition `POST /tenancy/tenants` drives.
An event-onboarded tenant is routable the moment `TenantCatalogChanged` is
emitted, same as a REST-onboarded one.

> ### Migration box: consumer construction, before → after
>
> **Before (Plan 007, unroutable-bus-tenant defect):**
> ```python
> consumer = TenantProvisionConsumer(provisioner=provisioner, dlq=my_dlq)
> ```
> **After (Plan 008):**
> ```python
> control_service = TenantControlService(
>     catalog=catalog, provisioner=provisioner, producer=producer,
> )
> consumer = TenantProvisionConsumer(control_service=control_service, dlq=my_dlq)
> ```
> A one-release shim keeps `provisioner=` working: `TenantProvisionConsumer(
> provisioner=provisioner, catalog=catalog, producer=producer)` builds a
> `TenantControlService` internally and raises `DeprecationWarning`.
> `provisioner=` **without** `catalog=` raises `ValueError` naming
> `control_service=` as the fix — there is no correct thing the shim can do
> with a provisioner and no catalog, so it refuses to guess. If the shim
> path omits `producer=` too, a private no-op producer is used and **every**
> emission attempt logs one WARNING that `TenantCatalogChanged` was not
> emitted (other pods' `CachedTenantCatalog` entries go stale until
> `catalog_ttl_s`).

⚠️ **A tenant onboarded purely over the bus before this fix is
permanently unroutable** — its storage exists but its catalog row does
not, so every request 404s regardless of how long you wait. The repair is
one idempotent `POST /tenancy/tenants` (or a direct `control_service.
provision(tenant_id)` call) per affected tenant: `provision()` re-reads the
catalog, finds no row, adds one as `PENDING`, and — because the
provisioner's own `IF NOT EXISTS`/checkfirst semantics make re-running it a
no-op — drives straight to `ACTIVE` without re-running destructive or
duplicate DDL.

⚠️ **Multi-consumer fleets: a premature-`ACTIVE` window.** If more than one
`TenantProvisionConsumer` (each wired to an *authority* `TenantControlService`,
`catalog_authority=True`, the default) is subscribed to the same
`"varco.tenancy"` channel, the **first** one to finish provisioning flips the
catalog to `ACTIVE` and starts routing traffic while the others are still
running their own DDL against pods that may not have it yet. This is new
with Plan 008 — before it, the consumer never touched the catalog at all, so
no race existed (at the cost of the permanent-404 defect above; a short race
is strictly better than a permanent outage). If you run more than one
consumer under `TenantIsolation.SCHEMA`/`DATABASE`, do not treat "add another
consumer" as free horizontal scaling — use worker mode
(`catalog_authority=False`, see [Fleet fan-out](
#fleet-fan-out-provision-vs-request_provision)) plus the
[readiness coordinator](#fleet-readiness) instead, so only one authority ever
writes the catalog and `ACTIVE` is not asserted until every store has
reported in.

**No-op / DBA workflow path** (RD-4): `ExternalTenantProvisioner` records
intent and returns — a DBA or Terraform pipeline creates the real
schema/database out of band, and the `status` lifecycle (`pending` until
someone flips it) tracks when that external work completes.

### Standalone vs bundled control plane

Two supported deployment shapes, no eleventh workspace package:

```
STANDALONE (default, recommended)          BUNDLED (explicit opt-in)
┌──────────────┐  ┌──────────────┐         ┌───────────────────────────┐
│ app pod      │  │ control plane│         │ one app                   │
│ no admin DSN │  │ ADMIN_DSN ✔  │         │ ADMIN_DSN ✔ + ack ✔       │
│ no admin rt  │  │ admin router │         │ tenant routes + admin rt  │
└──────────────┘  └──────────────┘         └───────────────────────────┘
 privilege absent from app pods             privilege present — guarded, logged
```

| | Standalone | Bundled |
|---|---|---|
| Blast radius | app pods hold zero admin privilege | admin DSN lives in the app pod's own environment |
| Credential scope | isolated to a small, dedicated deployable | shared with everything else the app does |
| Ops cost | one more container/deployment | zero extra containers |
| When to bundle | — | single-container/PaaS deploys, dev, small installs where a second container isn't justified |

Bundle explicitly, with `mount_tenant_admin`:

```python
from varco_fastapi.tenancy.mount import mount_tenant_admin

app = create_varco_app(container, routers=[...])  # tenant traffic
mount_tenant_admin(
    app,
    control_service,
    acknowledge_bundled_admin=True,  # required; ValueError without it
    server_auth=admin_auth,
    admin_role="tenant-admin",  # deliberately distinct from a generic "admin"
    prefix="/tenancy",
    dependencies=[Depends(ip_allowlist)],  # optional extra network-level gate
)
```

**Three independent barriers against accidental bundling:** (1) there is
**no** env-var path — `VARCO_TENANCY_ADMIN_DSN` alone mounts nothing; (2)
`acknowledge_bundled_admin` defaults `False` → `ValueError`; (3)
`server_auth` is mandatory (a guard that can never be satisfied is a
startup error). One WARNING is logged at every bundled mount, naming the
trade-off, role, and prefix. Mounting twice is refused. `build_tenant_router`
itself refuses `server_auth=None`.

### Admin confinement (RD-4)

Cluster DDL (`CREATE DATABASE`/`DROP DATABASE`, schema provisioning) is
**confined to the control plane** — an app pod cannot reach it even if it
wanted to:

- `SADatabaseProvisioner` **cannot be constructed** without an explicit
  `VARCO_TENANCY_ADMIN_DSN` — `ValueError` otherwise.
- It **refuses** an admin DSN equal to the request-path `SAConfig.engine`'s
  URL — an app pod must not be its own admin.
- `SAAdminEngine` is short-lived, `NullPool`-backed, and disposed in a
  `finally` — no admin connection lingers in a pool between calls.
- A process with an admin DSN present but no acknowledged
  `mount_tenant_admin` call logs one WARNING recommending the standalone
  topology — the DSN alone exposes nothing, but it is the wrong topology.

**Required grants:** the control-plane role needs `CREATEDB` (and
`CREATEROLE` only for per-tenant Postgres roles — out of scope, not built);
the app role needs **neither**, plus `CONNECT` on tenant databases,
`USAGE`/DML on tenant schemas only, and **read-only** on the global schema
by default (RD-10).

---

## Fleet fan-out: `provision()` vs `request_provision()`

Plan 008 adds a **broadcast API**, distinct from the local `provision()`/
`deprovision()` orchestration calls documented above. It exists for
`SCHEMA`/`DATABASE` deployments where more than one service must each run
its own local DDL for the same tenant (e.g. an `orders` service and a
`billing` service, each owning its own schema/database, both need to
provision `"acme"`).

### Topologies

| Topology | `catalog_authority` | Onboarding call | Coordinator |
|---|---|---|---|
| Single control plane (standalone, RD-9 default) | `True` | `provision()` (REST) or a bus command from an external system | not needed |
| Bundled control plane + app pod, `SHARED` isolation | `True` | `provision()` | not needed |
| Fleet fan-out, `SCHEMA`/`DATABASE` isolation | control plane `True`, every app service `False` | control plane `request_provision()` | required (or manual `POST …/activate`) |

Under the default `TenantIsolation.SHARED`, none of this section is wired —
there is exactly one store, so a single `provision()` call already covers
the whole deployment.

### Command/fact diagram

```mermaid
flowchart TD
    REST["REST POST /tenancy/tenants"] -->|"provision() — local: catalog + DDL"| Ctrl["TenantControlService<br/>(catalog_authority=True)"]
    ReqAPI["request_provision() / request_deprovision()"] -->|"broadcast-only, no local effect"| Cmd["TenantProvisionRequested /<br/>TenantDeprovisionRequested (origin=node_id)"]
    Cmd --> Consumer["TenantProvisionConsumer"]
    Consumer -->|"origin == own node_id?"| Skip["skip (DEBUG log)"]
    Consumer -->|"origin != own node_id"| Ctrl
    Ctrl -->|"catalog_authority=True"| Fact1["TenantCatalogChanged"]
    Ctrl -->|"catalog_authority=False (worker)"| Fact2["TenantNodeReady(store_id)"]
    Fact1 --> Cache["CachedTenantCatalog invalidation"]
    Fact2 --> Coord["TenantReadinessCoordinator"]
    Coord -->|"every expected store seen"| MarkActive["control_service.mark_active()"]
    MarkActive --> Fact1
```

This is the acyclic command/fact graph RD-13 requires: commands
(`TenantProvisionRequested`/`TenantDeprovisionRequested`) may produce facts
(`TenantCatalogChanged`/`TenantNodeReady`); facts may only produce a
terminal action (cache invalidation, `mark_active()`) or nothing. **No
handler anywhere emits a command** — `provision()` never re-emits
`TenantProvisionRequested`, which is what keeps the two entry points from
looping into each other now that they share one code path.

### The broadcaster is not included — call `provision()` first

```python
# On the control plane / a node that must ALSO provision itself locally:
await control_service.provision(tenant_id)  # 1. local, synchronous — surfaces
#    a DDL failure to the caller
await control_service.request_provision(tenant_id)  # 2. broadcast — tells the rest
#    of the fleet
```

`request_provision()`/`request_deprovision()` emit the command event and do
**nothing else** — no catalog write, no local provisioner call. A node that
calls only `request_provision()` and skips step 1 never provisions itself;
that is the documented, tested behaviour, not an oversight — provisioning
this node too is a call the caller must make explicitly.

```python
# Worker-mode service (e.g. the "orders" microservice's own control service)
worker_service = TenantControlService(
    catalog=catalog,  # still reads the catalog (refuses DELETED/DEPROVISIONING)
    provisioner=orders_provisioner,
    producer=producer,
    catalog_authority=False,  # never writes the catalog — RD-16
    store_id="orders",  # or VARCO_TENANCY_STORE_ID
)
worker_consumer = TenantProvisionConsumer(control_service=worker_service, dlq=my_dlq)
worker_consumer.register_to(bus)  # from @PostConstruct
```

`request_deprovision(tenant_id, confirm=False)` raises
`DestructiveOperationRefused` and broadcasts nothing — refusing to fan a
command out fleet-wide that would only DLQ on arrival everywhere. A service
built with `producer=None` raises `RuntimeError` at construction, not on the
first broadcast attempt.

### `origin`/`node_id`/`store_id` reference

| Field / env var | Where | Meaning |
|---|---|---|
| `origin` (event field) | `TenantProvisionRequested`/`TenantDeprovisionRequested` | The broadcaster's `node_id`, or `None` for an externally-published command. A consumer whose own `node_id` matches `origin` skips the event (RD-15) — it already acted synchronously before broadcasting. |
| `node_id` (`TenantControlService.node_id`) / `VARCO_TENANCY_NODE_ID` | Every `TenantControlService` | Stable per-process identifier stamped as `origin` on broadcasts. Defaults to `f"{hostname}:{pid}"` if the env var is unset. Two processes sharing a `node_id` over-skip each other's broadcasts — documented, not guarded against. |
| `store_id` (`TenantControlService.store_id`) / `VARCO_TENANCY_STORE_ID` | Worker-mode `TenantControlService`s | The logical store this node/service owns — stamped on `TenantNodeReady`. Only meaningful under `catalog_authority=False`; unset by default. |

REST equivalents: `POST /tenancy/tenants/{id}/request-provision` (202,
broadcast-only) and `DELETE /tenancy/tenants/{id}?broadcast=true` (with the
same `{"confirm": true}` body the local delete requires) call
`request_provision()`/`request_deprovision()` instead of the local
orchestration methods. Both are `admin_role`-guarded like every other
tenancy admin route.

---

## Fleet readiness

RD-16's worker mode (`catalog_authority=False`) needs a terminator — a
worker never flips a tenant to `ACTIVE` on its own, so without one a
fan-out tenant stays `PENDING` (→ 503) forever. `TenantReadinessCoordinator`
is that terminator, aggregating per-store `TenantNodeReady` facts.

```python
from varco_core.tenancy.control.readiness import TenantReadinessCoordinator

coordinator = TenantReadinessCoordinator(
    control_service=authority_service,  # catalog_authority=True — required
    expected_stores=frozenset({"orders", "billing", "inventory"}),
    timeout_s=900.0,  # None disables the watchdog
    dlq=my_dlq,
)
coordinator.register_to(bus)  # from @PostConstruct — never __init__
```

### Why the unit is a store, not a pod

Ten `orders` pods share one `orders` database — the first pod's
`TenantNodeReady(store_id="orders")` makes the store ready, and the other
nine reports are idempotent no-ops (a duplicate report from an
already-`seen` store never re-counts). Autoscaling the `orders` deployment
from 3 pods to 30 changes nothing about what the coordinator is waiting
for. `expected_stores` is therefore **static deploy-time config** — you
change it only when you add or remove a *service*, the same kind of change
as adding that service's deployment manifest, not something that fluctuates
on every autoscale event.

**Worked example:** three microservices — `orders`, `billing`, `inventory`
— each with its own Postgres schema/database, each running N pods.
`expected_stores = frozenset({"orders", "billing", "inventory"})` — always
size 3, regardless of whether N is 3 or 30 per service. The tenant
activates the moment all **3** distinct `store_id`s have reported, not
after some multiple of pod count.

Constructing without `expected_stores` raises `ValueError` — there is no
default and no auto-discovery (RD-17): a distributed pod registry with a
TTL and heartbeat would reintroduce the exact lease-sizing problem Plan 005
already paid the cost of solving once, for no benefit here (the store set
does not change on the timescale a registry would need to track).

### The restart caveat and its one-verb recovery

Readiness state (`dict[str, set[str]]` of stores seen per tenant) is
**in-memory only** — a coordinator restart loses all partial progress for
tenants mid-onboarding (RD-18). There is no eleventh framework table and no
migration backing this: persisting a window measured in seconds is not
worth a new durable contract when the recovery is one already-idempotent
command. `GET /tenancy/tenants/{id}/readiness` right after a restart
answers **404** for a tenant that was mid-onboarding — the reset is made
visible, not hidden behind a stale-looking snapshot. The 404 means "this
coordinator holds no readiness state for that tenant"; it says nothing
about whether the tenant exists in the catalog, which
`GET /tenancy/tenants?status=pending` answers. (`readiness()` likewise
raises `TenantNotFoundError` for a tenant it has never observed — a tenant
becomes *observed* on its first `TenantNodeReady`, including one carrying
an unexpected `store_id`, after which the route returns 200 with a
possibly-empty `seen` set.)

Recovery is a single re-broadcast:

```python
await control_service.request_provision(tenant_id)
```

Every layer downstream is already idempotent — the bus-level dedup, the
worker's catalog status check (refuses `DELETED`/`DEPROVISIONING`), and the
provisioner's own `IF NOT EXISTS` semantics — so re-broadcasting is safe
even for stores that had already reported before the restart; they simply
re-emit `TenantNodeReady` and the coordinator counts them again.

### A timeout never activates

If `timeout_s` elapses with a store still missing, the coordinator logs one
ERROR naming the missing stores and **leaves the tenant `PENDING`** — it
never flips `ACTIVE` on a fleet known to be incomplete, no matter how long
you wait. There is no code path that auto-activates on a timeout; the only
two ways a tenant reaches `ACTIVE` under worker mode are every expected
store reporting `TenantNodeReady`, or an operator calling the manual
terminator (`await control_service.mark_active(tenant_id)` /
`POST /tenancy/tenants/{id}/activate`, requires `catalog_authority=True` —
`ValueError` otherwise).

An **unexpected** `store_id` (not in `expected_stores`) is ignored with one
WARNING naming both sets and never counts toward completion — the mitigation
for `expected_stores` drift is that this WARNING (plus the full
expected/seen set logged at every partial step) surfaces the mismatch
immediately rather than silently under-counting.

### Under `TenantIsolation.SHARED`, none of this is wired

The default strategy has exactly one store — there is nothing for a
readiness coordinator to aggregate, and wiring one anyway is pure overhead.
`build_tenant_router(..., coordinator=None)` (the default) does not
register the `GET …/readiness` route at all; pass `coordinator=` only for a
fan-out (`SCHEMA`/`DATABASE`) deployment that has opted into worker mode.

---

## Fan-out (RD-8)

Under `DATABASE`, a tenant's `OutboxEntry`/`Job`/`AuditEntry` rows live in
*that tenant's own* database — the single process-wide `OutboxRelay`/
`JobPoller`/`AuditConsumer` never polls it, so **events would silently never
be published** unless fan-out is enabled.

```python
from varco_core.tenancy.fanout import TenantFanoutSupervisor

supervisor = TenantFanoutSupervisor(
    child_factory=lambda tid: build_outbox_relay_for(tid),  # your own factory
    max_entries=50,  # mirrors the resource pool's cap — same bound, no second knob
    enabled=settings.fanout_framework_tables,  # False by default
)
await supervisor.start()
```

`TenantFanoutSupervisor` reuses `OutboxRelay`/`JobPoller`/`AuditConsumer`
**verbatim**, one instance per active, pool-resident tenant. Each child runs
in its own supervised task: a crash is caught, logged, and restarted with
capped exponential backoff, so one tenant's failing relay never blocks
another's. Children are staggered on startup (tenant *i*'s first tick offset
by `i × poll_interval/N`) to avoid a connection/query thundering herd, and
are bounded by the same `max_entries` the resource pool already enforces —
no second cap to size.

`varco_sa.tenancy.guard.guard_fanout_configuration()` is the **config
guard**: constructing a db-per-tenant deployment with a relay/runner/audit
consumer wired but `fanout_framework_tables=False` raises
`TenantIsolationError` naming `VARCO_TENANCY_FANOUT_FRAMEWORK_TABLES` as the
flag that fixes it — this is enforced at construction time, not discovered
later as "outbox rows are stranded."

`TenancyLifecycle` starts the supervisor **after** the pool's sweeper and
stops it **before** `pool.aclose()` — a relay must never outlive the engine
it polls.

---

## Connection budget (informational — no enforced cap)

**varco does not decide the tenant-count ceiling; the operator does.**

```
max_entries × (pool_size + max_overflow) × n_pods ≤ 0.8 × max_connections
```

Recommended per-tenant sizing for `DATABASE` is `pool_size=1,
max_overflow=2` (per-tenant engines are mostly idle). At `max_entries=50`
and `n_pods=3` that is `50 × 3 × 3 = 450` connections budgeted — compare
against `0.8 × max_connections` for your Postgres instance. This is
guidance, not a limit varco enforces: `TenantResourcePool` has a **soft**
cap that fails open under sustained pressure (every entry busy → the cap is
exceeded with one WARNING per breach, never a hard rejection) — resource
pressure fails open; isolation never does.

---

## Pitfalls

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **A token's `tenant_id` claim silently activates a suspended/deleted tenant's context** | With `create_varco_app` + a container, `RequestContextMiddleware` (on by default) enters `tenant_context()` from the claim with no catalog-status check and no `pool.ensure()` — the exact check `TenantResolutionMiddleware` performs | Two independent tenant setters existed pre-Plan-033 (the header-reading `TenantResolutionMiddleware` and the claim-reading `RequestContextMiddleware`), with no cross-check between them | Adopt a `TenantSourceChain` (Plan 033 / S6) — `TenantResolutionMiddleware` becomes the single decision point and `RequestContextMiddleware` defers to it; full design: `technical_docs/features/tenant-provenance.md` |
| **Raw `text()` SQL under `TenantIsolation.SCHEMA`** | A hand-written query returns rows from the wrong tenant's schema, or errors on a missing table | `schema_translate_map` rewrites schema references at SQL-compile time — it never touches raw `text()` SQL | Self-qualify the schema in the raw SQL, or route the query through the ORM so it carries the symbolic `"tenant"` token |
| **`SET` used instead of `SET LOCAL`/`set_config(..., true)` for schema routing** | Under a transaction-mode pooler, one tenant's schema routing leaks into the next logical caller's session | Session-scoped `SET` survives past the transaction on a pooled connection — same defect class as `SAAdvisoryLock`'s U-16 finding | Use `SASchemaRouter`'s default `mechanism="translate_map"`; if you must use the `"search_path"` escape hatch, it already emits `set_config(..., true)`, never a bare `SET` |
| **Per-tenant engine/binding never `dispose()`d** | Connections/clients accumulate until the pool or the process runs out | A caller evicts a tenant outside `TenantResourcePool`/`SAEngineRegistry`/`BeanieTenantPool`, bypassing their `closer` | Always go through the pool's `evict()`/`aclose()` — never hold a raw engine/client reference past eviction |
| **Unbounded per-tenant pool** | Connection exhaustion under `TenantIsolation.DATABASE` at even moderate tenant counts | `max_entries × (pool_size + max_overflow) × n_pods` was never checked against `max_connections` | Follow the sizing worksheet in `technical_docs/features/multitenancy.md`; varco enforces no cap (RD-5) — this is an operator responsibility |
| **`init_beanie()` rebinds every tenant to one database** | All Mongo tenants silently read the same database, no error | A second `init_beanie()` call with a different database rebinds the Document **class** globally — `BeanieDocRegistry` is process-global, keyed by domain class | Always go through `varco_beanie.tenancy.binding.build_tenant_binding()`, which clones each Document class per tenant instead of calling `init_beanie()` against the shared base classes |
| **`BeanieDocRegistry.get(User)` expected to return a tenant's clone** | A repository built from `BeanieDocRegistry.get(User)` silently operates on the wrong (base) database | Clones are deliberately never registered in `BeanieDocRegistry` — that registry's contract is "return the base class" | Use `binding.clone_for(User)` from the tenant's `BeanieTenantBinding`, not `BeanieDocRegistry.get(User)` |
| **`TenantIsolation.DATABASE` without `fanout_framework_tables`** | Outbox/job/audit rows accumulate in each tenant's database and are never published/claimed/persisted | The process-wide `OutboxRelay`/`JobPoller`/`AuditConsumer` only polls the app's own (non-tenant) database | `guard_fanout_configuration()` raises at construction naming `VARCO_TENANCY_FANOUT_FRAMEWORK_TABLES` — set it, which wires `TenantFanoutSupervisor` |
| **`TenantIsolation.SCHEMA` on `varco_beanie`** | Expecting a Mongo equivalent of Postgres schemas | MongoDB has no schema-per-tenant primitive | `BeanieTenantPool` raises `ValueError` at construction naming MongoDB — use `SHARED` or `DATABASE` instead |
| **`TENANT`-scoped cache key built outside `tenant_context()`** | Expecting a graceful unnamespaced fallback | `tenancy_cache_key()` fails closed by design — an unnamespaced `TENANT` key is a cross-tenant leak waiting to happen | Wrap the call in `with tenant_context(tenant_id): ...`, or catch the `RuntimeError` and treat it as a real bug, not something to paper over |
| **`GLOBAL`-scoped cache key namespaced by tenant** | N× cache waste and N× DB load for reference data every tenant reads identically | A hand-rolled cache key included `tenant_id` for an entity that doesn't need it | Use `tenancy_cache_key(entity_cls, key)` — it detects `TenantScope.GLOBAL` via `is_global_entity()` and omits the tenant segment automatically |
| **`TenantAwareService` mixed into a `GLOBAL`-entity service** | `_scoped_params` filters on a `tenant_id` field that doesn't exist on the entity — an error at best, silently empty results at worst | The service's MRO includes `TenantAwareService` while its entity is declared `TenantScope.GLOBAL` | `validate_service_scope()` raises `TenantIsolationError` at wiring time naming both the service and the entity — drop the mixin |
| **Expecting one transaction across a tenant DB and the global DB** | `AttributeError`/design confusion trying to share a UoW across `Inject[IUoWProvider]` and `Inject[GlobalUoWProvider]` | Under `TenantIsolation.DATABASE` these are genuinely two different physical databases — there is no 2PC | Sequence two transactions, or route the atomic-looking write through the transactional outbox/saga primitives instead |
| **Global write raises `GlobalScopeReadOnlyError`** | A write to a `GLOBAL`-scoped entity from an app pod fails with SQLSTATE `42501` translated into a legible error | The app-facing global credential is read-only by default (RD-10) | Route the write through the tenant control plane, or opt in explicitly with `TenancySettings(global_writable=True)` / `VARCO_TENANCY_GLOBAL_WRITABLE=true` |
| **Literal DSN stored in `varco_tenants`** | `ValueError` on `catalog.add()` naming RD-2 | `dsn_ref` must be a secret **reference** (resolved by your own secret-manager hook), never a literal connection string | Store a reference, not a credential; pass `allow_literal_dsn=True` only for tests/bootstrap |
| **Admin DSN present in an app pod that never mounted the admin surface** | Nothing is actually exposed — but the credential sits unused in the wrong process | `VARCO_TENANCY_ADMIN_DSN` alone grants no route; only `mount_tenant_admin()` mounts one | One WARNING is logged recommending the standalone topology; prefer moving the DSN to a dedicated control-plane deployment |
| **`mount_tenant_admin()` without `acknowledge_bundled_admin=True`** | `ValueError` at mount time, nothing mounted | The friction is intentional — bundling puts admin-adjacent privilege in the app pod's own environment | Pass `acknowledge_bundled_admin=True` only after confirming the standalone deployment genuinely isn't justified |
| **Bundled admin router left ungated at the ingress** | The `/tenancy/*` admin surface is reachable from wherever the app itself is reachable | Role-guarding (`admin_role="tenant-admin"`) is an application-layer control, not a network one | Use the dedicated `prefix=` to deny it at the ingress, and/or pass `dependencies=[Depends(ip_allowlist)]`/an mTLS check |
| **Redelivered `TenantProvisionRequested` assumed unique** | Worry about double-provisioning on broker redelivery | `provision()` is idempotent (status is the check) and the consumer additionally dedups same-process redelivery by `event_id`; compose with the durable inbox for cross-restart idempotency | No action needed for the common case — redelivery is a documented, tested no-op, not a hazard to work around manually |
| **Bus-onboarded tenant 404s** | A tenant provisioned purely via `TenantProvisionRequested` is unroutable forever, even though its schema/database was created | Pre-Plan-008: the consumer called the provisioner directly, never wrote the catalog row `routing.py`/`TenantResolutionMiddleware` look up | Upgrade to a `control_service=`-based `TenantProvisionConsumer` (Plan 008), then repair each affected tenant with one idempotent `POST /tenancy/tenants` — re-running `provision()` finds the missing catalog row, adds it, and the provisioner's own idempotency means no duplicate/destructive DDL runs |
| **Consumer constructed with `provisioner=`** | `ValueError` at construction (bare `provisioner=`), or a `DeprecationWarning` you're ignoring (`provisioner=`+`catalog=`) | `provisioner=` is a Plan 008 (RD-12) deprecated shim scheduled for removal one minor release after landing | Pass `control_service=TenantControlService(catalog=..., provisioner=..., producer=...)` directly instead |
| **Bundled node called only `request_provision()` and never provisioned itself** | The broadcasting node's own local storage was never created — it relies on some other node's DDL that never runs there | `request_provision()`/`request_deprovision()` are broadcast-only by design (RD-14) — they deliberately exclude the caller | Call `provision()` **first** (local, synchronous), then `request_provision()` (broadcast) — this ordering also surfaces a local DDL failure before the fleet is told |
| **Missing store in `expected_stores`** | A tenant activates one store early — traffic is routed to a store that never got its own DDL | A service was added without updating the coordinator's `expected_stores` set | Update `expected_stores` when adding a service to the fleet; the coordinator logs the full expected/seen set at every partial step and on every unexpected-`store_id` WARNING to make the drift visible |
| **Counting pods instead of stores** | Expecting `expected_stores` to change on every autoscale event, or sizing it by pod count | The readiness unit is a **store** (RD-17), not a pod — ten pods of one service provision the same database, so nine of their `TenantNodeReady` reports are idempotent no-ops | Size `expected_stores` to the number of distinct services/databases, never to pod count; it changes only when a service is added or removed |
| **Expecting readiness to survive a restart** | A coordinator restart appears to "forget" tenants mid-onboarding — `GET …/readiness` answers 404 (`readiness()` raises `TenantNotFoundError` for a tenant it has not observed since the restart) | Readiness state is in-memory only (RD-18) — no durable/persisted readiness contract exists. The 404 is about the *coordinator's* state, not the catalog's — check `GET /tenancy/tenants?status=pending` for tenant existence | Re-broadcast `request_provision(tenant_id)` — every downstream layer is already idempotent, so this is the documented one-verb recovery, not data loss |
