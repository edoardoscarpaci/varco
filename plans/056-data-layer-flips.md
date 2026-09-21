# Plan 056 — Data-layer default flips (FLIP-12, FLIP-13): RLS asserted for `TENANT` tables, webhook secrets never plaintext

Covers BACKLOG 4.0 rows **`FLIP-12`** (`BACKLOG.md:74` — *"`enforce_rls=True` and `FORCE ROW LEVEL
SECURITY` asserted for `TENANT` tables on Postgres … posture ids `data.rls_disabled`/`data.rls_not_forced`.
Needs the migration story (research 007: policies before `ENABLE`)"*) and **`FLIP-13`** (`:72` —
*"`SAWebhookSubscriptionRepository(encryptor=None)` refuses; an encryptor is required … `varco_sa`
owns it; `posture.webhook_secrets_plaintext`"*). Flip track, slice row 056
(`plans/000-index-4-0-di-standardization.md:35`): independently mergeable, no plan dependency in
the index — but it *targets* the shapes 049/050/051/054/055 land (`SAModule` as a `VarcoModule`
with `start()`, `SAConfiguration.webhook_repository` injecting `FieldEncryptor`, the
`PostureCollector` ABC + `contribute()`, the derived-`refuse` default + severity table, the
"a module contributes the collector for the facts it owns" rule), and every step names its
pre-/post- form.

**Neighbours (do not absorb):** 055 owns every `tenant.*` id and `TenancyModule` — this plan never
touches `varco_fastapi/tenancy` or the tenant-resolution middleware. 054 owns the severity table's
*mechanism* and `enforce`'s derivation — this plan only adds/corrects `data.*` and
`posture.webhook_secrets_plaintext` rows in it. 051 owns the `PostureCollector` ABC and
`contribute()`/`acollect()` — this plan adds one subclass in `varco_sa`, exactly as 051's handoff
(`plans/051-…md:693-694`) says. 050 owns the DI path that injects `FieldEncryptor` into the two
durable webhook repositories — this plan owns only the *constructor-level* refusal 050 deferred
(`plans/050-…md:67-68, 340, 763`). 037 is the design of record for every RLS primitive (nothing
in `rls.py`/`rls_autogen.py`/`rls_session.py` changes shape). 058 assembles this plan's §Migration
into the one 3.x → 4.0 guide.

**Design of record:** `plans/037-data-layer-tenant-enforcement.md` §D-S12-order (`:95-118` — policy
→ `ENABLE` → `FORCE`), §D-S12-hook (`:223-293` — `after_begin`, *"fail-closed is opt-in"*, *"two
new flags, `enforce_rls` untouched"*), §D-S12-posture (`:295-323` — *"report-and-warn in 3.2;
raising is 4.0's decision"*; rejected: raising on a bypassing role because *"it would fail every
local/CI Postgres container, whose default role is a superuser"*), §D-S12-oq4 (`:325-359` — **no
varco-owned revision, ever**), the migration note (`:686-727`), Parked row *"Flipping
`rls_require_tenant`/`enforce_rls` to `True` … warn in 3.2, flip in 4.0"* (`:790`);
`plans/031-outbound-webhooks.md` §D-D4-signing (*"no new crypto path"* — `active_secrets` go
through the existing `FieldEncryptor`); `technical_docs/features/security-posture.md:165-166`
(flip rows 12/13); `design/research/007-postgres-rls-enforcement-mechanics.md` §1 (`:22-48` — who
bypasses: superusers, `BYPASSRLS`, **owners unless `FORCE`**; *"the cause of production RLS
failures"*), §5 (`:216-250` — `AccessExclusiveLock`, default-deny when enabled without a policy,
policies first); `design/research/011-security-platform-table-stakes-2026.md` §A (`:22` — the
`FieldEncryptor` family is what varco ships for application-level encryption; plaintext at rest is
the anti-pattern); CLAUDE.md's rules — *never ship a varco-owned Alembic revision that enables
RLS*, `enforce_rls` is an additive flag on `SHARED`, `TenancySettings()` defaults are
byte-identical to pre-Plan-007 (**this plan retires that sentence — §D-flip12-default**),
`varco_fastapi.tenancy` imports only `varco_core.tenancy`.

> ⚠️ **Six premises corrected from source while writing this plan; each reshapes a §D block.**
> (1) **`enforce_rls` is read by nothing.** `rg -n 'enforce_rls|assert_rls_enabled\(' varco_*/varco_*`
> hits only `varco_core/varco_core/tenancy/settings.py` (the field, `:158`, and `from_env`, `:208`)
> and `varco_sa/varco_sa/tenancy/rls_check.py` (the function's own definition and docstrings).
> `assert_rls_enabled()` is called **only by tests** (`varco_sa/tests/test_rls_assertion.py`). The
> docs (`multitenancy.md:109-137`, `postgres-rls.md:215-224`) describe a hand-called assertion the
> app is expected to write itself. Flipping the default to `True` therefore changes **nothing** by
> itself — FLIP-12 must first *wire a reader* (§D-flip12-reader); the flip is the reader's default.
> (2) **`_collect_data()` cannot consume a real `RlsPosture`.** `varco_fastapi/varco_fastapi/posture.py:636,646`
> read `t.enabled`/`t.forced`, but `TablePosture`'s fields are `rls_enabled`/`rls_forced`
> (`rls_check.py:187-189`) — an `AttributeError` on first contact. No test passes a real posture
> (`test_posture_collectors.py` uses the function only with `rls_posture=None`). The mapping is
> rewritten as one pure function in `varco_sa` (§D-flip12-mapping) and the `varco_fastapi` class
> delegates to it. (3) **The degenerate non-Postgres posture would fire a false HIGH.**
> `inspect_rls_posture()` returns every table as `TablePosture(False, False, False)` on a
> non-Postgres dialect (`rls_check.py:287-301`), which `_collect_data`'s `any(not t.enabled …)`
> would report as `data.rls_disabled` — HIGH — on SQLite. §D-flip12-mapping gates on dialect.
> (4) **`data.role_bypasses_rls` over-reports.** `posture.py:656` fires HIGH whenever
> `owned_tables` is non-empty, but brief 007 §1 (`:35-44`) and `inspect_rls_posture()` itself
> (`rls_check.py:352-360`) say an owner bypasses *only* when `FORCE` is absent — an owned, forced
> table is the textbook-correct shape (`provision_rls_app_url`'s role owns every table it creates,
> `varco_sa/tests/conftest.py:250-251`). Narrowed in §D-flip12-mapping. (5) **The task brief's
> "the RLS posture collector belongs in `TenancyModule`" conflicts with two written rules.**
> `TenancyModule` lives in `varco_fastapi` (`plans/049-…md`, `plans/055-…md:216-254`) and CLAUDE.md
> forbids `varco_fastapi.tenancy` importing `varco_sa`; the check needs an `AsyncEngine`
> (`SAConfig`) and `SAModelRegistry`, both `varco_sa`-only; and 051's handoff already assigns it:
> *"056 — a live RLS collector is a `PostureCollector` in `varco_sa`, contributed by `SAModule`"*
> (`plans/051-…md:693-694`). §D-flip12-collector follows 051 and the seam rule; the *rule* 055
> established (*"the module that owns the fact contributes the collector"*, `plans/055-…md:378-380`)
> is honoured — the fact here is a database's catalog state, and `SAModule` owns the database.
> (6) **The 3.x plaintext rows are not readable through a real encryptor.** With `encryptor=None`
> the repositories store the raw secret (`varco_sa/varco_sa/webhook.py:134-136`); with an
> encryptor they store `encrypt(...).hex()` and read `decrypt(bytes.fromhex(stored))` (`:139-142`).
> A 3.x deployment that upgrades and binds a `FernetFieldEncryptor` gets `ValueError: non-hexadecimal
> number` (or `EncryptionError`) on its first `list()`. A flip that makes existing data unreadable
> without a story is incomplete — §D-flip13-reencrypt ships the one-shot re-encryption method.

## Goal

After this plan: a varco app whose `SAModule` finds `TenantScope.TENANT` tables carrying a tenant
column, on Postgres, under `SHARED` isolation, has its RLS posture **read live at startup**
(`SAModule.start()`, one connection checkout, three catalog queries) and reported through
`SecurityPosture` — `data.rls_disabled` (HIGH), `data.rls_not_forced` (WARN),
`data.role_bypasses_rls` (HIGH, narrowed to actual bypass), `data.rls_enabled_without_policy`
(WARN, default-deny), `data.rls_guc_setter_absent` (WARN) — so that under 054's derived `refuse`
a production app with unprotected tenant tables **refuses to start**, naming every table and the
`rls_autogen` recipe. `TenancySettings.enforce_rls` defaults `True`; `VARCO_TENANCY_ENFORCE_RLS=false`
is the one greppable hatch (a WARN, `data.rls_not_enforced`, says it was pulled). A single-tenant
app (no tenant column anywhere), a SQLite/MySQL app, a `SCHEMA`/`DATABASE`-isolated app, and an
app whose database is unreachable at preflight never see a blocking `data.*` finding —
INFO/NOT_ASSESSED only. varco still ships **no** revision that enables RLS.
`SAWebhookSubscriptionRepository` and `BeanieWebhookSubscriptionRepository` **require** a
`FieldEncryptor` (`encryptor` is a required keyword; `None` or a non-`FieldEncryptor` →
`TypeError` naming the Fernet one-liner, the 050 DI path, and the dev hatch);
`PlaintextFieldEncryptor(acknowledge_plaintext=True)` is the greppable dev/test hatch and is what
`posture.webhook_secrets_plaintext` (WARN) now detects; `reencrypt_legacy_plaintext()` migrates
3.x plaintext rows in place, once. Each flip has exactly one escape hatch, posture ids, a
§Migration row, and tests for default, hatch, posture, and (FLIP-12) the hook end-to-end on a
real Postgres as a non-superuser role.

## Non-goals

- **Shipping any Alembic revision that enables RLS**, for application *or* framework tables —
  037 §D-S12-oq4, CLAUDE.md's rule. varco asserts; the operator writes the revision.
- **Flipping `rls_set_tenant` or `rls_require_tenant`** — 037's "two flags, `enforce_rls`
  untouched" reasoning still holds in reverse: an app that sets the GUC itself (its own
  `after_begin`, or `set_tenant_local()` calls) must not get a second setter; RDS Proxy pinning
  (brief 007 §4) is why the hook stays opt-in. The GUC gap is *reported* (§D-flip12-guc), not
  auto-fixed.
- **Making `assert_rls_enabled()` raise on `FORCE`/role findings, or calling it from a module.**
  Posture is the single refusal path (§D-flip12-reader ✅); `assert_rls_enabled()` keeps its
  exact 3.x contract as a hand-callable helper.
- **Framework-table RLS posture** (`varco_dead_letters`, `varco_audit_log`, `varco_schedules`,
  `webhook_subscriptions`, the encryption key store). Three have nullable `tenant_id`
  (037 §D-S12-nullable); `assert_rls_enabled()` skips them today. Parked with a trigger.
- **Mongo/Beanie server-side tenant enforcement** — no RLS analogue (037 Parked).
- **Changing `render_rls_ddl`/`rls_autogen`/`install_rls_tenant_hook`/`inspect_rls_posture`
  signatures.** `RlsPosture` gains one trailing defaulted field (§D-flip12-mapping); nothing else
  in 037's surface moves.
- **An encryptor on `InMemoryWebhookSubscriptionRepository`** — §D-flip13-inmemory.
- **A `varco webhook reencrypt` CLI verb** — the repository method is the deliverable; a CLI is
  one `asyncio.run()` away and named in §Parked.
- **Any `tenant.*` id, `TenancyModule`, `mount_tenant_admin`** — 055.
- **A second environment variable for either flip.** `VARCO_TENANCY_ENFORCE_RLS`,
  `VARCO_TENANCY_RLS_SET_TENANT`, `VARCO_SECURITY_SUPPRESS`, `VARCO_SECURITY_ENFORCE`,
  `VARCO_SECURITY_ENV` already exist and are the only knobs this plan reads.

## Context (verified from source, 2026-09-18)

| Fact | Where | Why it matters |
|---|---|---|
| `TenancySettings` — frozen dataclass + `from_env()`; `enforce_rls: bool = False` (`:158`), `rls_set_tenant: bool = False`, `rls_require_tenant: bool = False` (`:167-168`); `from_env` reads `VARCO_TENANCY_ENFORCE_RLS` via `_bool` (`:198-208`); docstring *"With every default, the deployment is byte-identical to today's behaviour"* (`:99-101`); `Args: enforce_rls — assert Postgres RLS is enabled on every routed table` (`:106-108`); `DESIGN: two new flags, enforce_rls untouched` (`:145-154`) | `varco_core/varco_core/tenancy/settings.py` | FLIP-12's field. Post-048 the class is a pydantic `VarcoSettings` (`plans/055-…md:122` records the same for `TenantProvenanceSettings`); the default flips in the field either way. Three docstring passages become false and are rewritten |
| **Nothing reads `enforce_rls`** (premise 1) | `rg -n 'enforce_rls' varco_*/varco_*` → `settings.py:26,106,137,146,149,158,208`; `rls_check.py:55,164` (prose) | §D-flip12-reader |
| `assert_rls_enabled(conn, *, tables, global_tables, framework_tables, enforce) -> list[str]` — non-Postgres → `[]` + one WARNING (`:133-141`); skips `global_tables`/`framework_tables` (`:143`); raises `TenantIsolationError` only when `enforce and missing` (`:154-165`) | `varco_sa/varco_sa/tenancy/rls_check.py:89-167` | Unchanged; the docs stop presenting it as *the* automated path |
| `TablePosture(rls_enabled, rls_forced, has_policy)` (`:170-189`); `RlsPosture(current_role, is_superuser, rolbypassrls, owned_tables, tables)` (`:192-220`); `inspect_rls_posture(conn, *, tables) -> RlsPosture` — non-Postgres → degenerate all-`False` posture with `current_role=""` (`:287-301`); logs the owner-bypass WARNING only for `rls_enabled and not rls_forced and relname in owned_tables` (`:352-360`); `tables=[]` still queries the role (`:279-281`) | `rls_check.py:170-368` | Premises 2/3/4. `RlsPosture` gains `dialect: str = "postgresql"` (trailing, defaulted — positional construction in `test_rls_posture.py` keeps working) so the mapping can gate without a second dialect read |
| `_collect_data(*, rls_posture=None)` — `data.rls_not_checked` NOT_ASSESSED when `None` (`:620-631`); `.enabled`/`.forced` (`:636,646`); `owned_tables` unconditional HIGH (`:656`); module comment *"Severity assignment lives here, never in the sibling — the one exception is 035's nine ids"* (`:404-409`); post-051 this is `DataPostureCollector(rls_posture=None).collect()` (`plans/051-…md:324`) | `varco_fastapi/varco_fastapi/posture.py:404-425, 604-668` | Body replaced by a delegation (§D-flip12-mapping); the comment gains a second exception |
| `_collect_local(... webhook_repository=None ...)` — `posture.webhook_secrets_plaintext` WARN iff `getattr(webhook_repository, "_encryptor", None) is None` (`:717-726`) | `posture.py:671-726` (post-051 `LocalPostureCollector`) | FLIP-13's finding; detection changes (§D-flip13-posture) |
| `@Configuration class SAModule(settings: TenancySettings \| None)` — `@PostConstruct _install_rls_tenant_hook_if_enabled()` installs the hook on `AsyncSession` when `rls_set_tenant` (`:128-174`); sync, cannot await | `varco_sa/varco_sa/di.py:80-174` | Pre-049 form. 049 renames it `SAConfiguration` (`plans/049-…md:606,891`) and adds `varco_sa/varco_sa/modules.py::SAModule(config=None, *, entities=())` (`:576-578`); 050 gives it `ensure_tables=True`, `phase=Phase.INFRA`, and a `start()` (`plans/050-…md:394-406`). The live check rides `SAModule.start()` (§D-flip12-collector) |
| `SAModelRegistry` — process-level `{domain_cls: orm_cls}`; `get()` only, no public iteration (`:165-185`) | `varco_sa/varco_sa/factory.py:150-185` | Gains `registered() -> tuple[type, ...]` so the candidate set is *every* class `SAModelFactory` built — including ones bound by a hand `bind_repositories()` call, not just `SAModule(entities=)` (§D-flip12-candidates) |
| `plan_tenant_rls(domain_classes, *, base, tenant_column="tenant_id", table_lookup=None, null_tenant=REFUSE)` — GLOBAL skipped silently (`:259-261`), no-tenant-column → `skipped_reason`, `varco_tenants` hard-excluded; `_default_table_lookup` via `SAModelRegistry` (`:186-200`); `null_tenant=REFUSE` **raises** on a nullable column | `varco_sa/varco_sa/rls_autogen.py:186-262` | The scope + column derivation is factored into `rls_candidate_tables()` (pure, never raises) and reused by `plan_tenant_rls` (§D-flip12-candidates) |
| `install_rls_tenant_hook(target, *, setting="rls.tenant_id", require_tenant=False, …) -> uninstall`; idempotent via `session_cls.__dict__[_MARKER]` (`:260-264`) | `varco_sa/varco_sa/tenancy/rls_session.py:208-264` | The marker is how `data.rls_guc_setter_absent` detects a hand-installed hook on the default session class (§D-flip12-guc) |
| Integration fixtures: `postgres_container` (session), `postgres_url`, `provision_rls_app_url()` (non-superuser, **owns** its tables, `:236-292`), `provision_rls_reader_url()` (owns nothing, `:295-314`), `asyncpg_url()`; the container's own role is `rolsuper=True, rolbypassrls=True` (`:244-247`) | `varco_sa/tests/conftest.py:230-314` | Exactly the three role shapes the collector must distinguish; per-test `uuid4().hex[:8]` table names (CLAUDE.md namespacing rule) |
| `test_rls_posture.py` — three-role integration test for `inspect_rls_posture()` (037 Step 19) | `varco_sa/tests/test_rls_posture.py` | The scaffold the collector integration test extends |
| `SAWebhookSubscriptionRepository.__init__(*, url, encryptor: FieldEncryptor \| None = None, **engine_kwargs)` (`:101-111`); module docstring ⚠️ *"`encryptor=None` (the default) stores secrets in plaintext — a deliberate, documented escape hatch for local dev/tests"* (`:16-18`); usage snippet without an encryptor (`:24`); `_encrypt_secret`/`_decrypt_secret` `None` branches (`:134-142`); table `webhook_subscriptions`, `active_secrets` JSON list (`:57-73`) | `varco_sa/varco_sa/webhook.py` | FLIP-13's SA site. 050 adds `engine: AsyncEngine \| None = None` and makes `url` optional (`plans/050-…md:340`) — this plan's signature change composes with it |
| `BeanieWebhookSubscriptionRepository.__init__(*, url, db_name, encryptor: FieldEncryptor \| None = None)` (`:104-114`); same `None` branches (`:132-140`) | `varco_beanie/varco_beanie/webhook.py` | FLIP-13's Beanie site (050 adds `client=`) |
| `InMemoryWebhookSubscriptionRepository()` — dict of `WebhookSubscription` objects, no encryptor parameter, *"single-process only"* (`:135-152`) | `varco_core/varco_core/webhook/base.py` | §D-flip13-inmemory |
| `FieldEncryptor` — `@runtime_checkable Protocol` with `encrypt(plaintext, *, context=None) -> bytes` / `decrypt(...)` (`:91-135`); `FernetFieldEncryptor(key: bytes)` — lazy `cryptography` import, `ImportError` naming `varco-core[crypto]` (`:270-323`) | `varco_core/varco_core/encryption.py` | `isinstance(x, FieldEncryptor)` is a legal structural check → the constructor guard (§D-flip13-refuse); `PlaintextFieldEncryptor` joins this module (§D-flip13-hatch) |
| In-repo constructor sites: `varco_sa/tests/test_webhook_subscription_repository_integration.py:35,45`; `varco_beanie/tests/test_webhook_subscription_repository_integration.py:35,45`; the `webhook.py:24` docstring snippet; README `:4360`; `outbound-webhooks.md:116-118,148`. The example app wires **no** webhook repository (`rg -n 'Webhook' examples/ --glob '*.py'` → 0) | grep | Sweep list for Step 13; the example app needs no change for FLIP-13 |
| 050: `SAConfiguration.webhook_repository`/`BeanieConfiguration.webhook_repository` — `encryptor: Inject[FieldEncryptor]` **required**; `validate()` → `MISSING_BINDING` when absent; *"056 — the DI path never passes `encryptor=None`; the refusal is constructor-only"* | `plans/050-…md:340, 628-632, 763` | The DI path is already fail-closed; this plan closes the hand path and gives the DI path its dev hatch (a bound `PlaintextFieldEncryptor`) |
| 051: `varco_core.posture.PostureCollector` (`name: ClassVar[str]`, **sync** `collect() -> list[PostureFinding]`); `contribute(container, PostureCollector, instance)`; `SecurityPostureModule.start()` → `await acollect(container, PostureCollector)`; `SecurityPostureLifecycle(collectors=…)` accepts instances or zero-arg callables; the six default collectors are contributed by `SecurityPostureModule.configure_app()`, `DataPostureCollector(rls_posture=None)` among them | `plans/051-…md:314-333` | `collect()` is sync → the live fetch happens earlier, in `SAModule.start()` (INFRA), and the collector reads the stored result (§D-flip12-collector) |
| 054: `enforce` derives `refuse` unless `VARCO_SECURITY_ENV=development`; HIGH blocks everywhere, unsuppressed WARN blocks in `production`; INFO/NOT_ASSESSED never; severity-table rows `data.rls_disabled`/`data.role_bypasses_rls` HIGH *"(flip: 056)"*, `data.rls_not_forced` WARN, `data.rls_not_checked` NOT_ASSESSED (`:294-296`); `posture.webhook_secrets_plaintext` WARN *"(flip: 055/`varco_sa`)"* (`:268` — **wrong owner, corrected to 056 here**); `SecurityPostureModule` starts **last** (`:478`); autouse `_posture_warn_only` fixture in `varco_fastapi/tests/conftest.py` (`:239-251`); `test_every_shipped_check_id_has_a_severity_table_row` (`:616-620`) | `plans/054-…md` | Every new `data.*` id needs a severity-table row or 054's register test goes red |
| 055 §D-collector: *"`tenant.*` exists iff `TenancyModule` is present"*; a hand-wired middleware is *"unobserved by posture — a Pitfalls row"* | `plans/055-…md:356-390` | The same rule, applied: `data.*` exists iff `SAModule` is present (§D-flip12-collector ❌) |
| `varco_core/tests/test_tenancy_settings.py:19,42,157` assert `enforce_rls is False`; `:105` comment *"`enforce_rls` means one thing only"* | that file | Flip to `True`; comment rewritten |
| CLAUDE.md `:1070-1074` *"Default is byte-identical to pre-Plan-007 behaviour … `enforce_rls=False`"*; `:1080-1082` the no-revision rule; `multitenancy.md:93` env row `false`, `:109-137` Recipe 1; `postgres-rls.md:215-224` (`assert_rls_enabled` as the automated check), `:333-344` "What is opt-in", `:509-524` Pitfalls; `security-posture.md:165-166` rows 12/13; `outbound-webhooks.md:116-118,148`; README `:4360` | docs | Step 17's edit list |

## Design

### §D-flip12-reader — `enforce_rls` gates a live posture read in `SAModule.start()`; posture is the only refusal path

Premise (1): the flag has no reader. **Chosen: `enforce_rls=True` means "at `SAModule.start()`,
read the RLS catalog state of every candidate table on the app's own engine and hand it to the
`RlsPostureCollector`; the `SecurityPosture` refuses (054) or logs it."** No `TenantIsolationError`
is raised anywhere; `assert_rls_enabled()` is untouched.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: posture-only enforcement** | One refusal mechanism, one message shape, one escape-hatch model (`VARCO_SECURITY_ENFORCE=warn`, `VARCO_SECURITY_SUPPRESS=<id>`, `VARCO_SECURITY_ENV=development`) shared with every other 4.0 flip; `development` gets logged-not-blocked for free (054 matrix) — which is exactly the *"would fail every local/CI Postgres container"* concern 037 §D-S12-posture raised, answered by the environment derivation instead of by never raising; the row's own wording names posture ids as the enforcement | An app that disabled `SecurityPostureModule` (`VARCO_SECURITY_POSTURE_ENABLED=false`, 049) gets no enforcement — accepted: it opted out of *every* preflight, and `varco diagnose` shows the module absent |
| (2) `SAModule.start()` calls `assert_rls_enabled(enforce=settings.enforce_rls)` and raises `TenantIsolationError` | The 3.x helper, verbatim | A second refusal path with its own exception, no environment awareness (dev refuses too), no suppression, and it checks `relrowsecurity` only — blind to `FORCE` and to the role (the row says both are asserted). Rejected |
| (3) both (raise at INFRA *and* report) | Belt and braces | Two errors for one fact, in two log blocks, at two times; 054 §D-compose's "never double-report" rule. Rejected |

### §D-flip12-default — `enforce_rls: bool = True`; `VARCO_TENANCY_ENFORCE_RLS=false` is the hatch; the byte-identical sentence is retired

`settings.py:158` → `True`; `from_env` unchanged (`_bool` reads the env, default now `True`).
Post-048: the pydantic field default. **Every other `TenancySettings` default is unchanged**, so
the sentence becomes: *"Every `TenancySettings()` default except `enforce_rls` is byte-identical to
pre-Plan-007 behaviour; `enforce_rls=True` (4.0, Plan 056 / FLIP-12) has an effect only where
`SAModule` finds `TENANT`-scoped tables carrying a tenant column, on Postgres, under `SHARED`."*
(CLAUDE.md `:1070-1074`, `settings.py:99-101`, `multitenancy.md`'s "This is the pre-Plan-007 default").

**Escape hatch (one greppable line):** `VARCO_TENANCY_ENFORCE_RLS=false` (env) or
`TenancySettings(enforce_rls=False)` (code). With it, no catalog query runs; if candidate tables
exist, one **WARN** `data.rls_not_enforced` (*"`enforce_rls=false` — N TENANT tables with a tenant
column are not checked for RLS"*) fires — under 054's rule it blocks a production start unless
`VARCO_SECURITY_SUPPRESS=data.rls_not_enforced`. Two visible operator statements: the same
"opt-out plus acknowledgement" shape 054 chose for `include_detail=true` and 055 for
`on_missing_claim=allow`. With no candidate tables (single-tenant app), the hatch is inert and
nothing fires.

- ✅ Brief 010 §3's fail-closed rule (via 054); the row verbatim. ✅ `rls_set_tenant`/
  `rls_require_tenant` stay `False` (§Non-goals). ❌ The one `TenancySettings` default that is
  not byte-identical — stated in three docs and CLAUDE.md, never implied.
- Rejected — **`enforce_rls: bool | None = None` derived from `isolation`/environment** (053/054's
  sentinel shape): the flag is a plain boolean whose reader already applies every gate
  (§D-flip12-candidates); a sentinel would add provenance for a value that has no derivation.

### §D-flip12-candidates — which tables are asserted: `TENANT` scope **and** a tenant column, from `SAModelRegistry`, under `SHARED`, on Postgres

`varco_sa/varco_sa/rls_autogen.py::rls_candidate_tables(domain_classes, *, tenant_column="tenant_id",
table_lookup=None) -> tuple[str, ...]` — pure, never raises: for each class, `TenantScope.TENANT`
(via the existing `_read_tenant_scope`) **and** the resolved table has `tenant_column` **and** the
table is not `varco_tenants` and not a framework table (`framework_metadata()` names). A class the
lookup cannot resolve is skipped. `plan_tenant_rls()` is refactored to call it for the scope/column
step (its `skipped_reason` entries and `REFUSE` behaviour are unchanged — the helper returns names;
`plan_tenant_rls` still walks each class for the plan record). `SAModelRegistry.registered()`
(new classmethod, tuple snapshot) supplies `domain_classes` at `SAModule.start()` time.

The gates, in order, each a distinct outcome:

| Gate | Outcome | Finding |
|---|---|---|
| `settings.isolation is not SHARED` | no query | `data.rls_not_applicable` INFO — *"isolation=schema/database — RLS is additive and not asserted"* |
| engine dialect `!= "postgresql"` | no query | `data.rls_not_applicable` INFO — *"dialect 'sqlite' — RLS is a Postgres feature"* |
| candidate set empty | no query | **nothing** (single-tenant app; documented — a table with no tenant column has nothing to filter on, matching `plan_tenant_rls`'s skip) |
| `enforce_rls is False` | no query | `data.rls_not_enforced` WARN |
| fetch raises (`OSError`, `sqlalchemy.exc.OperationalError`/`InterfaceError`/`DBAPIError`, `asyncio.TimeoutError`) | logged once at WARNING with the exception class | `data.rls_not_checked` NOT_ASSESSED — **never refuse for an unreachable database**; readiness/health owns that failure, and NOT_ASSESSED never blocks (036 §D-S9-enforce) |
| `refresh()` never ran (`varco diagnose`, or a hand-wired lifecycle that skipped `SAModule`) | — | `data.rls_not_checked` NOT_ASSESSED — *"SAModule.start() has not run"* |
| fetched | §D-flip12-mapping | `data.rls_*` per table state |

- ✅ The single-tenant blast radius is zero by construction (no `tenant_id` → no candidate → no
  finding), and a multi-tenant app is identified by the fact that *makes* it multi-tenant at the
  data layer, not by whether `TenancyModule` happens to be listed (055's cross-module-knowledge
  rejection). ✅ `SCHEMA`/`DATABASE` isolation is not falsely flagged (037 Edge cases: RLS is
  orthogonal there). ✅ Cost when it runs: one pooled connection, three catalog queries
  (`pg_roles`, owned tables, table state — `rls_check.py:223-250`), single digits of milliseconds;
  when any gate short-circuits, zero I/O.
- ❌ A multi-tenant app whose tenant column is not named `tenant_id` and that overrides
  `TenantAwareService._tenant_field` is invisible to the default — `SAModule(rls_tenant_column=…)`
  is the additive kwarg (mirrors `plan_tenant_rls(tenant_column=)`); the same ⚠️ assumption 037
  recorded.
- Rejected — **candidates from `SAModule(entities=)` only**: a hand `bind_repositories()` after
  the module registers would be unasserted silently. `SAModelRegistry` is the provider's own record
  of every generated table (`rls_autogen.py:186-200`'s reasoning).

### §D-flip12-mapping — one pure `rls_findings()` in `varco_sa`; the `varco_fastapi` class delegates; three narrowings

`varco_sa/varco_sa/tenancy/rls_posture.py::rls_findings(posture: RlsPosture | None, *,
guc_setter_present: bool | None = None, not_checked_reason: str | None = None) ->
list[PostureFinding]` (imports `varco_core.posture` — 051's home for `PostureFinding`/`PostureSeverity`):

| `check` | Severity | Fires when | Detail / remediation |
|---|---|---|---|
| `data.rls_not_checked` | NOT_ASSESSED (unchanged) | `posture is None` | `not_checked_reason` (the exception class, or "SAModule.start() has not run", or 3.x's "no RlsPosture supplied") / *"Start the app with SAModule; or `await inspect_rls_posture(conn, tables=...)` and pass `rls_posture=`"* |
| `data.rls_not_applicable` | INFO (**new**) | `posture.dialect != "postgresql"` (premise 3) | dialect named / no action |
| `data.rls_disabled` | HIGH (unchanged) | any table `not rls_enabled` | names every such table / *"`plan_tenant_rls()` → `tenant_rls_upgrade(op, plans=…)` in an application-authored, reviewed revision (never varco-owned) — policy, then ENABLE, then FORCE (postgres-rls.md)"* |
| `data.rls_enabled_without_policy` | WARN (**new**) | any table `rls_enabled and not has_policy` | *"default-deny: every non-bypassing query on <t> returns zero rows (brief 007 §5)"* / *"CREATE POLICY … (the generator always emits it before ENABLE)"* |
| `data.rls_not_forced` | WARN (unchanged) | any table `rls_enabled and not rls_forced` | names tables / *"`ALTER TABLE <t> FORCE ROW LEVEL SECURITY` in a reviewed revision"* |
| `data.role_bypasses_rls` | HIGH (unchanged severity, **narrowed**, premise 4) | `is_superuser` **or** `rolbypassrls` **or** any owned table `rls_enabled and not rls_forced` | *"role <r> is a superuser / has BYPASSRLS / owns unforced table(s) <t> — RLS is silently ineffective"* / *"run the app as a non-superuser, non-BYPASSRLS role; FORCE every owned table"* |
| `data.rls_guc_setter_absent` | WARN (**new**, §D-flip12-guc) | `guc_setter_present is False` and any table `rls_enabled and has_policy` | — |
| `data.rls_not_enforced` | WARN (**new**, §D-flip12-default) | emitted by the collector, not by `rls_findings` (no posture exists) | — |

`RlsPosture` gains `dialect: str = "postgresql"` (trailing, defaulted); `inspect_rls_posture()`
sets it from `conn.dialect.name` on both paths. `varco_fastapi.posture.DataPostureCollector(rls_posture=None).collect()`
(pre-051: `_collect_data`) becomes `return rls_findings(rls_posture, not_checked_reason=<3.x text>)`
inside the existing lazy-import/absent-sibling guard — the public class stays for hand wiring;
its body is no longer a second copy. The `posture.py:404-409` comment gains: *"the second
exception is 056's `data.*` ids, assigned in `varco_sa.tenancy.rls_posture` because the live
collector lives there (051's handoff)"*.

- ✅ One mapping, one severity assignment, two entry points (live module, hand-wired class).
  ✅ Premises 2/3/4 fixed in one place, each pinned by a Docker-free unit test over hand-built
  `RlsPosture`s. ✅ An owned-and-forced table (the `provision_rls_app_url` shape, the textbook
  correct one) no longer yields a HIGH.
- ❌ Two new WARN ids block a production start (054's rule). Both name a real hole or outage:
  default-deny is an outage that ships silently; a missing GUC-setter is the zero-rows failure 037
  §D-S12-hook exists for. Both suppressible by id.
- Rejected — **keep the mapping in `varco_fastapi/posture.py` and have `varco_sa` produce only
  the `RlsPosture`**: ❌ the sync `collect()` would then need the fetched posture handed across
  modules through the container (a bound `RlsPosture` instance whose lifetime is one startup) —
  more machinery than moving 40 lines to the package that already owns the query.

### §D-flip12-collector — `RlsPostureCollector` in `varco_sa`, contributed by `SAModule.register()`, refreshed by `SAModule.start()`

```python
# varco_sa/varco_sa/tenancy/rls_posture.py
class RlsPostureCollector(PostureCollector):
    name = "data"

    def __init__(self, *, settings: TenancySettings, engine_factory: Callable[[], AsyncEngine] | None,
                 tenant_column: str = "tenant_id", posture: RlsPosture | None = None) -> None: ...
        # posture= is the hand-wiring seam (pre-fetched, same shape as DataPostureCollector(rls_posture=))

    async def refresh(self) -> None:
        """Called by SAModule.start(). Applies the §D-flip12-candidates gates in order; stores
        either an RlsPosture, a not-applicable marker, or a not-checked reason. Never raises."""

    def collect(self) -> list[PostureFinding]:
        # sync — reads what refresh() stored; refresh never ran → data.rls_not_checked
```

`SAModule` (049/050's `varco_sa/varco_sa/modules.py`): `register()` builds the collector from the
bound `TenancySettings` (048's `bind_settings`, or `TenancySettings.from_env()` when nothing is
bound — the same optional-settings stance `di.py:112-126` takes today) and an
`engine_factory=lambda: container.get(SAConfig).engine`, then `contribute(container,
PostureCollector, collector)`; `start()` (INFRA, before `SecurityPostureModule`'s last-position
`start()`, 054 `:478`) does `await collector.refresh()` **after** `ensure_table()` work (a
missing framework table is unrelated to RLS, but the order keeps one connection lifecycle).
`SecurityPostureModule.configure_app()` **stops contributing `DataPostureCollector(rls_posture=None)`**
(051's default set drops from six to five) — it only ever produced a NOT_ASSESSED line, and with
the live collector present it would duplicate the id.

**Pre-049/050 form** (if this plan lands before them): no module exists to call `refresh()`. The
collector is constructed and refreshed by the app — `collector = RlsPostureCollector(settings=…,
engine_factory=lambda: engine)`; `await collector.refresh()` in a lifespan hook;
`SecurityPostureLifecycle(collectors=[collector, …])`. Step 1 records which form applies; the
module wiring step (Step 8) is written for the post-050 form and names the fallback.

- ✅ 051's handoff verbatim; the seam rule (no `varco_sa` import in `varco_fastapi.tenancy`)
  intact; 055's "the module that owns the fact contributes the collector" rule applied to the
  module that owns the engine. ✅ `varco diagnose` (052) never starts modules → `collect()` reports
  NOT_ASSESSED, exit code unaffected, and the report header shows `enforce_rls=true` from 048's
  settings provenance. ✅ Test seam: `posture=` pre-fetched → every state testable without Docker.
- ❌ An app on `varco_sa` that hand-wires everything without `SAModule` gets **no** `data.*`
  finding at all (055's accepted shape; Pitfalls row + `varco diagnose` listing `SAModule: absent`).
  ❌ `collect()` cannot re-read after `start()` — a table enabled after boot is seen at the next
  boot; documented (RLS is DDL in a reviewed revision, which is a deploy).
- Rejected — **contribute from `TenancyModule`** (the task brief): premise (5). Rejected —
  **an `acollect()`-aware async `PostureCollector.acollect()`**: a 051 seam change for one
  consumer; the INFRA-phase prefetch needs no seam change. Rejected — **run the fetch lazily inside
  `collect()` with `asyncio.run_coroutine_threadsafe`**: `collect()` runs on the loop; a nested
  run is a deadlock or a second loop.

### §D-flip12-guc — the GUC gap is reported (`data.rls_guc_setter_absent`, WARN), never auto-fixed

With RLS live (`rls_enabled and has_policy` on any candidate) and nothing setting the GUC, every
transaction sees `NULLIF('', '')` → `NULL` → **zero rows, no error** (037 Edge cases `:738-740`).
`refresh()` computes `guc_setter_present = settings.rls_set_tenant or
(_MARKER in AsyncSession.sync_session_class.__dict__)` — the second clause detects a hand
`install_rls_tenant_hook(AsyncSession)`/`(async_sessionmaker)` on the default session class
(`rls_session.py:262`, exposed via a small `varco_sa.tenancy.rls_session.hook_installed(target) ->
bool` read). An app that sets the GUC by its own means (its own listener, explicit
`set_tenant_local()` calls, a pooler-side setting) suppresses the id — the one-line acknowledgement.

- ✅ The migration note's step 2 (*"wire the GUC before the policies"*, 037 `:699-701`) becomes a
  mechanical check instead of a sentence. ✅ No coupling of `enforce_rls` to `rls_set_tenant`
  (037's "two flags" reasoning, §Non-goals). ❌ False positive for a self-setting app → one
  suppression; the alternative (INFO) would let the zero-rows outage ship silently, which is the
  worse failure. Rejected — **flip `rls_set_tenant=True` too**: RDS Proxy pinning (brief 007 §4)
  and an unrequested behaviour change on every transaction of every Postgres app.

### §D-flip12-migration — the 3.x → 4.0 story for a deployment with tenant tables and no RLS

A pre-3.x-schema multi-tenant app (tenant tables, `TenantAwareService` scoping, no policies)
upgrades: `SAModule.start()` finds candidates, reads the catalog, `data.rls_disabled` HIGH →
**refuses in production** with the table list and the recipe. The operator's two paths:

1. **Adopt RLS** — 037's migration note (`:694-715`), unchanged and now enforced in order:
   `inspect_rls_posture()` first (fix a superuser/`BYPASSRLS` role → `data.role_bypasses_rls`);
   `VARCO_TENANCY_RLS_SET_TENANT=true` and deploy (the GUC before the policies →
   `data.rls_guc_setter_absent` clears); `print(plan_tenant_rls(...))`, resolve every
   `skipped_reason` and nullable column; one application-authored revision calling
   `tenant_rls_upgrade(op, plans=…)` — policy → `ENABLE` → `FORCE` (§D-S12-order), in a
   maintenance window with a short `lock_timeout` (`AccessExclusiveLock`, brief 007 §5); verify as
   the app role. `data.*` is then clean.
2. **Defer** — `VARCO_TENANCY_ENFORCE_RLS=false` + `VARCO_SECURITY_SUPPRESS=data.rls_not_enforced`
   (or `VARCO_SECURITY_ENFORCE=warn` wholesale). Both greppable; `varco diagnose` shows them.

`development` needs neither: the derived `warn` logs the HIGH and starts.

### §D-flip13-refuse — `encryptor: FieldEncryptor` is a required keyword; `None`/non-encryptor → `TypeError`

```python
class SAWebhookSubscriptionRepository(WebhookSubscriptionRepository):
    def __init__(self, *, url: str | None = None, engine: AsyncEngine | None = None,   # 050's shape
                 encryptor: FieldEncryptor, **engine_kwargs: Any) -> None:
        if not isinstance(encryptor, FieldEncryptor):        # runtime_checkable Protocol; None fails
            raise TypeError(_ENCRYPTOR_REQUIRED.format(cls="SAWebhookSubscriptionRepository", got=type(encryptor).__name__))
```

Message (one shared constant in `varco_core/varco_core/webhook/base.py`, `_ENCRYPTOR_REQUIRED`,
used by both backends so the two texts cannot drift):

```
SAWebhookSubscriptionRepository: encryptor must be a FieldEncryptor (got NoneType) — webhook signing
secrets are never stored in plaintext (4.0, Plan 056 / FLIP-13).
  Production:  encryptor=FernetFieldEncryptor(key)   # key = Fernet.generate_key(), kept in your secret store
  DI (Plan 050): bind_instance(container, FieldEncryptor, FernetFieldEncryptor(key)) — SAConfiguration.webhook_repository injects it
  Dev/test only: encryptor=PlaintextFieldEncryptor(acknowledge_plaintext=True)   # reported as posture.webhook_secrets_plaintext
  Existing 3.x plaintext rows: await repo.reencrypt_legacy_plaintext() once after upgrading.
```

Omitting the keyword is Python's own `TypeError: … missing 1 required keyword-only argument:
'encryptor'` — visible to mypy under `strict`. `_encrypt_secret`/`_decrypt_secret` lose their
`None` branches (`webhook.py:134-142`, Beanie `:132-140`).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: required keyword + `isinstance(FieldEncryptor)` → `TypeError`** | The annotation (`FieldEncryptor`, not `\| None`) and the runtime agree — 053 §D-passthrough's reasoning (*"a `bool` whose only legal value is `True` is a lie in the annotation"*, applied to `\| None`); catches the second real mistake — passing the Fernet **key** instead of the encryptor — with the same message; no sibling `ValueError` exists in these constructors (054 chose `ValueError` for `server_auth` *because* the mounts already raised `ValueError` twice); one message constant, two backends | `TypeError` for a `None` value is unusual — argued in the docstring's DESIGN block, same as 053 |
| (2) `encryptor: FieldEncryptor \| None = None` + `ValueError` on `None` | The four `acknowledge_*` precedents raise `ValueError` | Keeps `None` legal in the signature; mypy blind; a `bytes` key passes the `None` check and fails at first `encrypt()`. Rejected |
| (3) an `acknowledge_plaintext_secrets: bool = False` kwarg on each repository | Precedent shape | Two repositories × one kwarg, keeps both `None` branches, and is **unreachable through 050's provider** (which injects the bound `FieldEncryptor`) — the DI path would have no dev hatch at all. Rejected in favour of §D-flip13-hatch, which serves both paths |

### §D-flip13-hatch — `PlaintextFieldEncryptor(*, acknowledge_plaintext: Literal[True])`, a real `FieldEncryptor` that encrypts nothing, greppable, posture-visible

`varco_core/varco_core/encryption.py` (beside `FernetFieldEncryptor`; exported from
`varco_core.encryption` and the top-level lazy map):

```python
class PlaintextFieldEncryptor:
    """⛔ DEV/TEST ONLY. Identity FieldEncryptor — encrypt() and decrypt() return their input.
    Exists so a durable WebhookSubscriptionRepository can be constructed without a real key in a
    local run, and so that choice is greppable (`rg -n PlaintextFieldEncryptor`) and reported by
    SecurityPosture as posture.webhook_secrets_plaintext (WARN — blocks a production start)."""
    def __init__(self, *, acknowledge_plaintext: Literal[True]) -> None:
        if acknowledge_plaintext is not True:
            raise TypeError("PlaintextFieldEncryptor: acknowledge_plaintext must be literally True — this encryptor stores plaintext")
    def encrypt(self, plaintext: bytes, *, context: str | None = None) -> bytes: return plaintext
    def decrypt(self, ciphertext: bytes, *, context: str | None = None) -> bytes: return ciphertext
    def __repr__(self) -> str: return "PlaintextFieldEncryptor(<PLAINTEXT — dev/test only>)"
```

- ✅ One constructor contract (`encryptor` is always a `FieldEncryptor`); the hatch works for the
  hand path **and** the DI path (`bind_instance(container, FieldEncryptor,
  PlaintextFieldEncryptor(acknowledge_plaintext=True))` satisfies 050's required injection);
  `Literal[True]` + `TypeError` is 053's exact `acknowledge_unverified` shape; the wire format is
  the same hex-of-bytes envelope as every real encryptor, so switching to Fernet later needs no
  format change — only `reencrypt_legacy_plaintext()` (§D-flip13-reencrypt) for rows written by
  it, which the method handles identically to 3.x rows.
- ❌ A class named `…Encryptor` that encrypts nothing — mitigated by the name, the `⛔`
  docstring, the shouting `__repr__`, the `Literal[True]` acknowledgement, and the WARN finding.
  ❌ Nothing stops it in production except posture; under 054's derived `refuse` that is a
  start-blocker unless suppressed — the intended two-statement shape.
- Rejected — **a `NullFieldEncryptor` with no acknowledgement**: silent by construction; the
  three-word name is the whole point. Rejected — **placing it in `varco_core.webhook`**: it is a
  `FieldEncryptor`; `encryption.py` is where implementations live (`FernetFieldEncryptor`,
  `MultiKeyEncryptorRegistry`, …) and `MultiKeyEncryptorRegistry`-style composition stays possible.

### §D-flip13-posture — `posture.webhook_secrets_plaintext` stays WARN; detection is `isinstance(PlaintextFieldEncryptor)` or a missing/`None` encryptor

`LocalPostureCollector.collect()` (pre-051 `_collect_local`, `posture.py:717-726`): fires when
`webhook_repository` is not `None` and `getattr(webhook_repository, "_encryptor", None)` is `None`
(an out-of-tree repository that never took the flip) **or** an instance of `PlaintextFieldEncryptor`.
Detail → *"The bound WebhookSubscriptionRepository stores signing secrets in plaintext
(`PlaintextFieldEncryptor(acknowledge_plaintext=True)` is bound, or the repository has no
encryptor)."*; remediation → *"Bind a real `FieldEncryptor` (`FernetFieldEncryptor(key)`); if the
plaintext run is deliberate (dev/test), `VARCO_SECURITY_SUPPRESS=posture.webhook_secrets_plaintext`."*
The `InMemoryWebhookSubscriptionRepository` has no `_encryptor` attribute → `getattr` default
`None` → would fire falsely: the check first requires `isinstance(webhook_repository,
InMemoryWebhookSubscriptionRepository)` to be **false** (§D-flip13-inmemory). Severity WARN
(unchanged — an explicit opt-out needs an explicit suppression, 054/057's rule); 054's severity
table row's owner cell is corrected from *"055/`varco_sa`"* to **056**.

### §D-flip13-inmemory — `InMemoryWebhookSubscriptionRepository` takes no encryptor and is not a finding

It holds `WebhookSubscription` objects in a dict (`base.py:150-152`) — the secrets live in process
memory only, are never at rest, and die with the process. "Encryption at rest" has no referent;
refusing construction or requiring an encryptor would be theatre and would break 050's `@Fallback`
default and 052's in-memory profile. Its docstring gains one sentence (*"Secrets are held in
memory only — the FLIP-13 encryptor requirement applies to the durable backends; using this
repository in production is reported by `di.in_process_backend`, not by
`posture.webhook_secrets_plaintext`"*). ✅ Zero blast radius on tests and the in-memory profile.
❌ A reader might infer "in-memory = plaintext-safe"; the sentence says why it is not the same
question.

### §D-flip13-reencrypt — `reencrypt_legacy_plaintext()` on both durable repositories, run once

`async def reencrypt_legacy_plaintext(self) -> int` on `SAWebhookSubscriptionRepository` and
`BeanieWebhookSubscriptionRepository`: walk every row; for each entry of `active_secrets`, try
`self._decrypt_secret(entry)`; on `ValueError` (non-hex) or `EncryptionError`/the backend's
invalid-token error, treat the entry as a 3.x plaintext secret and rewrite it as
`self._encrypt_secret(entry)`; persist rows that changed; return the number of rows rewritten.
Idempotent (a second run decrypts everything cleanly and rewrites nothing). Documented as the
one-shot upgrade step in §Migration and the CHANGELOG; not called automatically (a data rewrite
belongs to the operator's deploy, not to `start()`).

- ✅ Closes premise (6) with ~25 lines per backend and one shared decision rule (a Fernet token
  that fails authentication *is* not ciphertext; a plaintext secret that happens to be valid hex
  **and** a valid Fernet token is not a real-world case). ✅ Works for rows written under
  `PlaintextFieldEncryptor` too (they are hex — `decrypt` returns bytes that then fail nothing;
  the method treats a *successful* round-trip as already-encrypted, so a Plaintext→Fernet switch
  needs the operator to run it with the **Fernet** repository, where the plaintext-hex entries fail
  Fernet decryption and are rewritten — stated in the docstring).
- ❌ The method must construct the repository with the *new* encryptor and connect — it is
  ordinary app code the operator runs (`asyncio.run(...)`), named in §Migration. A CLI verb is
  §Parked.
- Rejected — **tolerant `_decrypt_secret` that falls back to plaintext on failure**: accepts
  plaintext forever, silently — the exact state the flip ends.

### §D-snapshots — what must and must not be regenerated

| Artefact | Effect | Action |
|---|---|---|
| `design/api-freeze-and-standards/measurements/api-surface.{json,md}` | **Additions** (notes, never a failure): `varco_core.PlaintextFieldEncryptor`; `varco_sa.RlsPostureCollector`, `varco_sa.rls_findings`, `varco_sa.rls_candidate_tables`, `varco_sa.hook_installed`. The two repository classes, `RlsPosture` (dataclass field), `SAModelRegistry.registered` (classmethod) and `TenancySettings` are classes — invisible to `--check` (CLAUDE.md "Known limitation") → `inspect.signature()`/field-default guard tests (Steps 2, 11) | Regenerate + commit in Step 15; `--check` clean at Step 19 |
| `design/api-freeze-and-standards/measurements/asyncapi-example.json` | none | not regenerated |
| `design/async-performance-patterns/measurements/import-budget.json` | `varco_sa/__init__.py` gains one import (`rls_posture`, which imports `varco_core.posture` — light); `varco_core.encryption` gains a class with no new imports | `--check --warn-only` must show no breach (Step 19); if it does, make the `varco_sa` re-export lazy |
| 048's settings-reference snapshot (if landed) | `enforce_rls` default `false` → `true` | regenerate in Step 3 |
| 054's severity table (`security-posture.md`) | five new/changed `data.*` rows; the webhook row's owner | Step 17; `test_every_shipped_check_id_has_a_severity_table_row` green |

## Steps

All commands run from `/home/edoardo/projects/varco`. Phase A (FLIP-12) and Phase B (FLIP-13) are
independent; Docs and Verification last.

1. [ ] **Preflight.** Record in `/tmp/claude-1000/-home-edoardo-projects-varco/625af1a2-0cf8-4199-b924-0b3f986861e6/scratchpad/056-preflight.txt`:
       every `file:line` in §Context re-read (`settings.py:99-101,106-108,145-158,167-168,198-208`;
       `rls_check.py:89-167,170-220,253-368` incl. `:287-301,352-360`; `posture.py:404-425,604-668,671-726`;
       `di.py:80-174`; `factory.py:150-185`; `rls_autogen.py:186-262`; `rls_session.py:208-264`;
       `webhook.py:16-24,101-142` (SA); `webhook.py:104-140` (Beanie); `base.py:135-152`;
       `encryption.py:91-135,270-323`; `conftest.py:230-314`); the premise-(1) `rg` reproduced
       (`enforce_rls` read nowhere outside `settings.py`); which of 048 (`bind_settings`,
       `provenance_of`, settings snapshot), 049 (`varco_sa/varco_sa/modules.py::SAModule`,
       `SAConfiguration` rename), 050 (`SAModule.start()`, `ensure_tables`, `phase`,
       `SAConfiguration.webhook_repository` with `encryptor: Inject[FieldEncryptor]`, the
       `engine=`/`client=` kwargs), 051 (`varco_core.posture.PostureCollector`, `contribute`,
       `DataPostureCollector`, `LocalPostureCollector`, `SecurityPostureModule.configure_app`),
       053 (`Literal[True]` `TypeError` precedent shipped?), 054 (`_posture_warn_only`, the severity
       table section, `test_every_shipped_check_id_has_a_severity_table_row`) and 055
       (`TenancyModule` shape — confirm it has **no** engine) are in the tree — each later step
       names its pre-/post- form. Confirm `contribute()` may be called from a module's
       `register()` (051 §D-verbs) — if only from `configure_app()`, Step 8 uses that hook.
       Measure: `uv run python -c "from varco_sa.factory import SAModelRegistry; print(hasattr(SAModelRegistry,'registered'))"`
       → `False`. **Verify**: the file exists with ≥ 20 confirmations and the pre-/post- matrix.

### Phase A — FLIP-12

2. [ ] `varco_core/tests/test_tenancy_settings.py` (red) — `:19,42,157` → `enforce_rls is True`;
       **add** `test_enforce_rls_env_false_is_the_hatch` (`VARCO_TENANCY_ENFORCE_RLS=false` via the
       `env=` mapping → `False`; `"0"`/`"no"`/`"off"` too), `test_enforce_rls_default_is_true_field_guard`
       (dataclass: `fields(TenancySettings)` default `True`; post-048: `model_fields["enforce_rls"].default is True`),
       `test_every_other_default_is_unchanged` (pin `isolation=SHARED`, `rls_set_tenant=False`,
       `rls_require_tenant=False`, `assert_tenant_filter=False`, `fanout_framework_tables=False`,
       `global_writable=False`, the templates and numerics — the "byte-identical except one"
       claim, mechanical). `:105` comment rewritten. **Verify**: red on the three flipped
       assertions and the field guard.

3. [ ] `varco_core/varco_core/tenancy/settings.py` — `:158` → `True`; docstring `:99-101` →
       §D-flip12-default's sentence; `Args: enforce_rls` (`:106-108`) → *"Default `True` since 4.0
       (Plan 056 / FLIP-12): at `SAModule.start()` the RLS posture of every `TENANT`-scoped table
       with a tenant column is read on Postgres under `SHARED` isolation and reported via
       `SecurityPosture` (`data.rls_*`). `false` skips the read and is itself reported
       (`data.rls_not_enforced`, WARN). Never emits DDL."*; the `DESIGN: two new flags` block
       (`:145-154`) gains a trailing *"4.0: the default flipped; the meaning did not — it still
       never sets the GUC (Plan 056 §D-flip12-guc)"*. Post-048: the same edits on the pydantic
       field. If 048's settings snapshot exists, regenerate. **Verify**: Step 2 green;
       `uv run pytest varco_core/tests/test_tenancy_settings.py -q`.

4. [ ] `varco_sa/tests/test_rls_candidates.py` (new, red, Docker-free) — `rls_candidate_tables()`:
       a `TenantScope.TENANT` class with `tenant_id` → included; a `GLOBAL` class → excluded; a
       `TENANT` class **without** the column → excluded (the single-tenant shape); a class the
       lookup raises `KeyError` for → excluded, no raise; `tenant_column="org_id"` honoured;
       `varco_tenants` and every `framework_rls_tables()` name excluded; `plan_tenant_rls()`'s
       existing `test_rls_autogen.py` suite still green **unmodified** (the refactor is
       behaviour-preserving); `SAModelRegistry.registered()` returns a tuple containing a class
       after `provider.register(cls)` and is a snapshot (mutating the tuple is impossible; a later
       registration is not reflected in an earlier snapshot). Use a `table_lookup=` stub so no
       engine is needed. **Verify**: red (`ImportError` on the two names).

5. [ ] `varco_sa/varco_sa/rls_autogen.py` — `rls_candidate_tables()` per §D-flip12-candidates
       (docstring: `Args`/`Returns`/`Edge cases` — *"never raises; a nullable tenant column is a
       candidate here (posture reads catalog state; the nullable decision is `plan_tenant_rls`'s)"*);
       `plan_tenant_rls()` calls it for the scope/column step, its own record-building loop
       unchanged. `varco_sa/varco_sa/factory.py` — `SAModelRegistry.registered()` (classmethod,
       `tuple(cls._registry)`, docstring: *"read-only snapshot; the candidate source for the RLS
       posture collector"*). `varco_sa/__init__.py` `__all__` gains `rls_candidate_tables`.
       **Verify**: Step 4 green; `uv run pytest varco_sa/tests/test_rls_autogen.py varco_sa/tests/test_rls_candidates.py -q`.

6. [ ] `varco_sa/tests/test_rls_findings.py` (new, red, Docker-free) — `rls_findings()` over
       hand-built `RlsPosture`s (helper `_posture(**tables, role=..., dialect=...)`):
       `test_none_is_not_checked` (NOT_ASSESSED, `not_checked_reason` echoed);
       `test_non_postgres_is_info_only` (`dialect="sqlite"`, all-False tables → exactly
       `{data.rls_not_applicable}` at INFO — premise 3 pinned); `test_disabled_is_high_and_names_tables`;
       `test_enabled_without_policy_is_warn`; `test_enabled_not_forced_is_warn`;
       `test_owned_and_forced_is_clean` (owned table, `rls_forced=True` → **no**
       `role_bypasses_rls` — premise 4 pinned); `test_owned_and_unforced_is_high_and_warn`;
       `test_superuser_is_high` / `test_bypassrls_is_high`; `test_guc_setter_absent_fires_only_with_live_rls`
       (`guc_setter_present=False` + a table enabled with policy → WARN; `None` → nothing; all
       disabled → nothing); `test_clean_posture_yields_nothing`; `test_every_id_is_in_the_severity_register`
       (the ids this module can emit ⊆ ids in `security-posture.md`'s table — the local twin of
       054's register test, so a red here points at this plan). Then
       `varco_sa/tests/test_rls_posture_collector.py` (new, red, Docker-free): with `posture=`
       pre-fetched → `collect()` equals `rls_findings(...)`; never refreshed → NOT_ASSESSED
       *"SAModule.start() has not run"*; `refresh()` on a SQLite engine (`sqlite+aiosqlite:///:memory:`,
       already a test dep) with a registered TENANT class → `data.rls_not_applicable`, **no**
       catalog query attempted (spy on `inspect_rls_posture`); `refresh()` with an
       `engine_factory` whose engine raises `OperationalError` on connect → NOT_ASSESSED naming
       the class, one WARNING in `caplog`, **no exception**; `settings.isolation=SCHEMA` →
       not-applicable, no query; empty candidate set → `collect() == []`, no query;
       `enforce_rls=False` + candidates → `data.rls_not_enforced` WARN, no query. **Verify**: red.

7. [ ] Implementation — `varco_sa/varco_sa/tenancy/rls_check.py`: `RlsPosture.dialect: str = "postgresql"`
       (trailing; both return sites set it; docstring `Attributes` entry). New module
       `varco_sa/varco_sa/tenancy/rls_posture.py`: `rls_findings()` (§D-flip12-mapping, every
       id's detail/remediation text as tabled, a `DESIGN:` block with the three narrowings and
       the "severity assigned here, not in `varco_fastapi`" exception), `RlsPostureCollector`
       (§D-flip12-collector; `refresh()` applies the gates **in the tabled order**, catches the
       named exception classes only — never bare `Exception` — and stores `(posture | None,
       not_applicable_reason | None, not_checked_reason | None, guc_setter_present)`; `collect()`
       sync; complete docstrings incl. `Async safety: refresh() must be awaited once, at startup,
       before collect(); collect() is a pure read`). `varco_sa/varco_sa/tenancy/rls_session.py`:
       `hook_installed(target) -> bool` (reads `_MARKER`, `Args`/`Returns`). `varco_sa/__init__.py`
       and `varco_sa/tenancy/__init__.py` exports. `varco_fastapi/varco_fastapi/posture.py`:
       `DataPostureCollector.collect()` (pre-051: `_collect_data`) body → the lazy import + `return
       rls_findings(rls_posture, not_checked_reason=…)`; comment `:404-409` extended. **Verify**:
       Step 6 green; `uv run pytest varco_fastapi/tests/test_posture_collectors.py -q` green
       unmodified (it only exercises `rls_posture=None`).

8. [ ] **Module wiring** — `varco_sa/tests/test_sa_module_rls.py` (new, red; skipped with the
       Step 1 reason if 049/050's `SAModule` is absent): `[CoreModule(), SAModule(config=sqlite)]`
       → after `register()`, `collect(container, PostureCollector)` contains exactly one
       `RlsPostureCollector`; after `start()` its `collect()` yields `data.rls_not_applicable`
       (sqlite); with `TenancySettings(enforce_rls=False)` bound (048 `bind_settings`, else the
       module's `settings=` kwarg) and a registered TENANT class → `data.rls_not_enforced`;
       `SecurityPostureModule` no longer contributes a `DataPostureCollector` (exactly one
       collector named `"data"` in the collected set — no duplicate NOT_ASSESSED); full-app
       composition: `create_varco_app(container, modules=[*default_modules(), SAModule(...)])`
       with `VARCO_SECURITY_ENFORCE` `delenv`ed (054 §D-flip2-tests) and a **pre-fetched** HIGH
       posture injected via the collector's `posture=` seam → lifespan raises 054's
       `RuntimeError` naming `data.rls_disabled`; the same with `VARCO_SECURITY_ENV=development`
       → starts, finding at HIGH in the report, `report.enforce == "warn"`; with
       `VARCO_SECURITY_SUPPRESS=data.rls_disabled` → starts. Then `varco_sa/varco_sa/modules.py`:
       `SAModule.__init__` gains `rls_tenant_column: str = "tenant_id"` (additive kw-only);
       `register()` builds + `contribute()`s the collector (settings from the container when
       bound, else `TenancySettings.from_env()`; engine via a factory closing over the container
       so `SAConfig` is resolved at `start()`, not at `register()` — `di.py:141-148`'s ordering
       concern); `start()` → `await collector.refresh()` after the `ensure_table` loop; docstring
       `Registers:` list + a `DESIGN:` block (§D-flip12-collector's ✅/❌ and the two rejected
       placements). `varco_fastapi`'s `SecurityPostureModule.configure_app()` drops the
       `DataPostureCollector(rls_posture=None)` contribution (051's list is now five; its docstring
       says why). **Pre-050 fallback**: skip the module edits, keep the test skipped, and add the
       hand-wiring snippet to `postgres-rls.md` (Step 17) as the documented path. **Verify**:
       green or skipped-with-reason; `make test PKG=varco_sa`; `make test PKG=varco_fastapi`.

9. [ ] `varco_sa/tests/test_rls_posture_collector_integration.py` (new, `@pytest.mark.integration`,
       `postgres_container`, tables named `f"t_{uuid4().hex[:8]}"`, DDL from
       `render_rls_ddl()` so the statement order is the shipped one) — parametrised over the
       three roles (`provision_rls_app_url` owner, `provision_rls_reader_url` non-owner,
       `asyncpg_url` superuser) where meaningful: **(a)** table with no RLS, app role →
       `data.rls_disabled` HIGH naming the table; **(b)** `CREATE POLICY` + `ENABLE`, no `FORCE`:
       app role (owner) → `rls_not_forced` WARN **and** `role_bypasses_rls` HIGH; reader role →
       `rls_not_forced` WARN **only**; **(c)** policy + `ENABLE` + `FORCE`, app role → none of
       disabled/not_forced/bypass; `rls_set_tenant=False` → `data.rls_guc_setter_absent` WARN;
       with `install_rls_tenant_hook(async_sessionmaker)` installed (marker present) → clean;
       **(d)** `ENABLE` without a policy (one statement, own transaction) →
       `rls_enabled_without_policy` WARN; **(e)** superuser role on (c)'s table →
       `role_bypasses_rls` HIGH; **(f)** end-to-end hook: (c)'s table, `rls_set_tenant=True`
       through the module (or `install_rls_tenant_hook` pre-050), rows for tenants A and B
       inserted as the superuser, then as the app role inside `tenant_context("A")` a plain
       `SELECT` via the session factory returns only A's rows **with no `set_tenant_local()` call
       in the test**, and the collector reports clean — the assertion that ties FLIP-12's report to
       the mechanism it asserts (037 Step 16's shape, reused). Each leg runs `refresh()` on an
       engine built from that role's DSN and asserts the **exact** id set, not a subset. **Verify**:
       `uv run pytest varco_sa/tests/test_rls_posture_collector_integration.py -m integration -q`.

### Phase B — FLIP-13

10. [ ] `varco_core/tests/test_plaintext_field_encryptor.py` (new, red) — `PlaintextFieldEncryptor()`
        → `TypeError` matching `acknowledge_plaintext`; `PlaintextFieldEncryptor(acknowledge_plaintext=False)`
        `# type: ignore[arg-type]` → `TypeError` containing "literally True"; `(acknowledge_plaintext=True)`
        → `isinstance(x, FieldEncryptor)`; `encrypt(b"s") == b"s"`, `decrypt(b"s") == b"s"`,
        `context=` ignored; `repr` contains `PLAINTEXT`; signature guard
        (`inspect.signature(PlaintextFieldEncryptor.__init__).parameters["acknowledge_plaintext"]`
        is `KEYWORD_ONLY`, no default, annotation `Literal[True]`). Then
        `varco_sa/tests/test_webhook_repository_encryptor_required.py` (new, red, **sqlite**,
        Docker-free — the existing `varco_sa` unit pattern): `SAWebhookSubscriptionRepository(url=sqlite)`
        → `TypeError: … missing 1 required keyword-only argument: 'encryptor'`;
        `(url=…, encryptor=None)` `# type: ignore[arg-type]` → `TypeError` whose message contains
        `FernetFieldEncryptor(key)`, `bind_instance`, `PlaintextFieldEncryptor(acknowledge_plaintext=True)`
        and `reencrypt_legacy_plaintext`; `(url=…, encryptor=b"not-an-encryptor")` → the same
        `TypeError` (`got bytes`); `test_signature_guard` (`parameters["encryptor"]`:
        `KEYWORD_ONLY`, `default is inspect.Parameter.empty`, annotation names `FieldEncryptor`
        without `None`); round-trip with `PlaintextFieldEncryptor` (stored value is hex of the
        secret — `bytes.fromhex(stored).decode() == secret`, read via raw `SELECT`); round-trip
        with `FernetFieldEncryptor(Fernet.generate_key())` (`pytest.importorskip("cryptography")`)
        — stored value is not the secret; `reencrypt_legacy_plaintext()`: insert a row by raw
        `INSERT` with `active_secrets=["whsec_legacy"]` (the 3.x shape), construct the repository
        with a Fernet encryptor, `get()` raises before, `reencrypt_legacy_plaintext() == 1`,
        `get()` returns `"whsec_legacy"` after, a second run returns `0`; a row written under
        `PlaintextFieldEncryptor` is rewritten by a Fernet repository the same way. Beanie twin
        in `varco_beanie/tests/test_webhook_repository_encryptor_required.py`: the constructor
        tests are Docker-free (no `start()`); the round-trip/reencrypt legs are
        `@pytest.mark.integration` on `mongo_url` with `db_name=f"wh_{uuid4().hex[:8]}"`.
        **Verify**: red.

11. [ ] Implementation — `varco_core/varco_core/encryption.py`: `PlaintextFieldEncryptor`
        (§D-flip13-hatch; `from typing import Literal`; full docstring with the ⛔ line, `Args`,
        `Raises: TypeError`, a `DESIGN: Literal[True] + TypeError, the 053 shape` block);
        `__all__` + `varco_core/__init__.py` lazy map. `varco_core/varco_core/webhook/base.py`:
        `_ENCRYPTOR_REQUIRED` message constant (§D-flip13-refuse text, `{cls}`/`{got}`
        placeholders) and a helper `require_field_encryptor(encryptor, *, cls_name) -> FieldEncryptor`
        (the `isinstance` check + raise — one implementation for both backends; private-by-name
        `_require_field_encryptor` is fine, it is imported by siblings, so make it public but
        underscore-free and add it to no top-level `__all__`); the `InMemoryWebhookSubscriptionRepository`
        docstring sentence (§D-flip13-inmemory). `varco_sa/varco_sa/webhook.py`: signature per
        §D-flip13-refuse composed with 050's (`url`/`engine`), the guard first in `__init__`, the
        two `None` branches deleted, `reencrypt_legacy_plaintext()` (§D-flip13-reencrypt; docstring
        `Returns`/`Edge cases`/`Async safety` and the Plaintext→Fernet note), module docstring
        `:12-18` rewritten (*"`encryptor` is required since 4.0 (Plan 056 / FLIP-13) … dev/test:
        `PlaintextFieldEncryptor(acknowledge_plaintext=True)`"*), usage `:24` gains `encryptor=`,
        `Args: encryptor` → *"**Required.** …"*, `Raises: TypeError`. `varco_beanie/varco_beanie/webhook.py`:
        the same. **Verify**: Step 10 green (unit legs); `make test PKG=varco_core`.

12. [ ] `varco_fastapi/tests/test_posture_collectors.py` (extend, red first) — `LocalPostureCollector`/
        `_collect_local` with `webhook_repository=` a stub whose `_encryptor` is
        `PlaintextFieldEncryptor(acknowledge_plaintext=True)` → `posture.webhook_secrets_plaintext`
        WARN with `PlaintextFieldEncryptor` in the detail; `_encryptor is None` (out-of-tree) →
        fires; a `FernetFieldEncryptor`-shaped stub → does not; `InMemoryWebhookSubscriptionRepository()`
        → does **not** (§D-flip13-inmemory); under 054 with variables `delenv`ed and the
        Plaintext stub → `SecurityPostureLifecycle` refuses naming the id;
        `VARCO_SECURITY_SUPPRESS=posture.webhook_secrets_plaintext` → starts. Then
        `varco_fastapi/varco_fastapi/posture.py:717-726` → §D-flip13-posture's check and texts.
        **Verify**: green.

13. [ ] In-repo sweep — `varco_sa/tests/test_webhook_subscription_repository_integration.py:35,45`
        and the Beanie twin `:35,45` gain `encryptor=PlaintextFieldEncryptor(acknowledge_plaintext=True)`
        (the conformance subclass tests the repository contract, not the cipher — the Fernet
        round-trip lives in Step 10); 050's `test_webhook_module.py`/DI guard tests: where a
        `FieldEncryptor` is bound for the durable path, unchanged; add one case binding
        `PlaintextFieldEncryptor(acknowledge_plaintext=True)` → the provider resolves and the
        posture finding fires (the DI-path hatch, end to end). `rg -n 'encryptor=None' varco_*/`
        → only the explicit `TypeError` tests. **Verify**: `make test PKG=varco_sa`;
        `make test PKG=varco_beanie`; `uv run pytest varco_sa/tests -m integration -k webhook -q`
        and the Beanie twin when Docker is available.

14. [ ] `varco_sa/tests/test_webhook_repository_encryptor_required.py` — the Beanie constructor
        tests and the SA signature guard double as the class-`__init__` guard `api_surface.py
        --check` cannot provide (§D-snapshots); add the same guard for
        `BeanieWebhookSubscriptionRepository.__init__` and for `RlsPosture` (`fields(RlsPosture)[-1].name == "dialect"`,
        default `"postgresql"`) in `varco_sa/tests/test_rls_findings.py`. **Verify**: green.

15. [ ] `uv run python scripts/api_surface.py` — regenerate; the `.md` diff shows **only
        additions** (`PlaintextFieldEncryptor`, `RlsPostureCollector`, `rls_findings`,
        `rls_candidate_tables`, `hook_installed`) and no removed/changed function signature.
        Commit both files. **Verify**: `uv run python scripts/api_surface.py --check` clean.

### Docs (same commit as the code — feedback memory)

16. [ ] `CHANGELOG.md` — under `## [Unreleased]`, above the existing blocks:
        `### BREAKING — Data-layer default flips (Plan 056, FLIP-12 / FLIP-13)` with bullets:
        FLIP-12 (`enforce_rls` default `False` → `True`, **and now has a reader** — 3.x never read
        it; what is asserted and the four gates; the ids added/narrowed with severities;
        `RlsPosture.dialect`; `SAModelRegistry.registered()`; `rls_candidate_tables()`;
        `SAModule(rls_tenant_column=)`; `SecurityPostureModule` no longer contributes
        `DataPostureCollector`; the escape hatch and its WARN; **no revision ships**); FLIP-13
        (`encryptor` required on both durable repositories, `TypeError` on `None`/non-encryptor and
        when omitted; `PlaintextFieldEncryptor(acknowledge_plaintext=True)`;
        `reencrypt_legacy_plaintext()` and **why it is mandatory for a 3.x deployment with existing
        rows**; the posture detection change; `InMemoryWebhookSubscriptionRepository` unchanged).
        End with *"Migration: this plan's §Migration (assembled into the 4.0 guide by Plan 058)."*
        `BACKLOG.md:72,74` — **only if the launching thread has not already done it**: status →
        `done (Plan 056)`. **Verify**: `rg -n 'Plan 056' CHANGELOG.md BACKLOG.md` → ≥ 1 / ≥ 2.

17. [ ] Feature docs, README, ARCHITECTURE, CLAUDE.md —
      - `technical_docs/features/postgres-rls.md`: `:215-224` (`assert_rls_enabled` "is the
        automated version…") → *"is the hand-callable helper; since 4.0 the automated path is
        `SAModule.start()` → `RlsPostureCollector` → `SecurityPosture` (Plan 056 / FLIP-12)"* with
        the gate table from §D-flip12-candidates and the id table from §D-flip12-mapping; `:333-344`
        "What is opt-in and what is not" keeps its promise verbatim and gains one paragraph: *"What
        changed in 4.0 is not whether varco applies RLS (it never does) but whether it **checks** —
        `enforce_rls=True` by default"*; §"Automatic tenant GUC-setting" gains the
        `data.rls_guc_setter_absent` paragraph; a new §"Upgrading a 3.x deployment with tenant
        tables" = §D-flip12-migration; the pre-050 hand-wiring snippet if Step 8 fell back;
        **Pitfalls** (`:509-524`) rows: *"Production start refuses with `data.rls_disabled` after
        upgrading → tenant tables exist without RLS → adopt (recipe) or `VARCO_TENANCY_ENFORCE_RLS=false`
        + suppress `data.rls_not_enforced`"*; *"`data.role_bypasses_rls` on a local docker Postgres →
        the container role is a superuser → `VARCO_SECURITY_ENV=development`, or a non-superuser
        role"*; *"`data.rls_guc_setter_absent` but the app sets the GUC itself → suppress the id"*;
        *"A hand-wired app without `SAModule` sees no `data.*` finding at all → wire `SAModule`, or
        the `RlsPostureCollector(posture=…)` hand form"*; *"RLS enabled by a startup hook"* row
        (`:518`) gains *"(still true in 4.0 — the check is a read)"*; the RDS-Proxy row's
        *"`enforce_rls` assertions"* wording updated.
      - `technical_docs/features/multitenancy.md`: `:93` env row default `true` + meaning; Recipe
        1 (`:109-137`): *"This is the pre-Plan-007 default"* → the §D-flip12-default sentence; the
        hand `assert_rls_enabled()` snippet stays with *"(optional since 4.0 — `SAModule` checks
        automatically)"*.
      - `technical_docs/features/outbound-webhooks.md`: `:116-118` → the required-encryptor
        sentence + the hatch; Pitfalls `:148` row → *"`encryptor=None` → `TypeError` since 4.0 →
        `FernetFieldEncryptor(key)`; dev: `PlaintextFieldEncryptor(acknowledge_plaintext=True)`"*;
        **add** *"Upgraded from 3.x with plaintext rows → `EncryptionError`/`ValueError` on first
        read → run `await repo.reencrypt_legacy_plaintext()` once"* and *"`posture.webhook_secrets_plaintext`
        blocks a production start → a `PlaintextFieldEncryptor` is bound → bind a real one, or
        suppress for a deliberate dev run"*.
      - `technical_docs/features/security-posture.md`: flip list `:165-166` rows 12/13 → *"**landed,
        Plan 056**"*; the severity table (054's section): `data.rls_disabled` HIGH (056, emission
        narrowed to Postgres+SHARED+candidates), `data.role_bypasses_rls` HIGH (056, narrowed to
        actual bypass), `data.rls_not_forced` WARN, `data.rls_not_checked` NOT_ASSESSED (now also
        "DB unreachable at preflight"), **new** `data.rls_not_applicable` INFO,
        `data.rls_enabled_without_policy` WARN, `data.rls_guc_setter_absent` WARN,
        `data.rls_not_enforced` WARN, each with owner **056** and its escape; the
        `posture.webhook_secrets_plaintext` row's owner cell → 056, escape → *"bind a real
        `FieldEncryptor`; suppress for a deliberate dev run"*; the collector list: *"`data.*` is
        contributed by `SAModule`, never by `SecurityPostureModule` (Plan 056), and exists only when
        `SAModule` is present"*.
      - `README.md`: `:4360` (the `encryptor=None` sentence) → required + hatch + `reencrypt_legacy_plaintext()`;
        the Multi-tenancy env table row for `VARCO_TENANCY_ENFORCE_RLS` (locate: `rg -n
        'VARCO_TENANCY_ENFORCE_RLS' README.md`) → default `true` + one sentence; the Multi-tenancy
        section's "byte-identical" sentence, if present, → the §D-flip12-default sentence; a
        "What a 4.0 production app with tenant tables must have" sentence pointing at
        `postgres-rls.md`'s upgrade section.
      - `ARCHITECTURE.md` `:1171` (`rls_check.assert_rls_enabled (skips GLOBAL)`) gains
        `rls_posture.RlsPostureCollector / rls_findings` on the next line; the `varco_sa.webhook`
        entry (locate: `rg -n 'SAWebhookSubscriptionRepository' ARCHITECTURE.md`) gains
        `(encryptor required)`; `varco_core.encryption`'s listing gains `PlaintextFieldEncryptor`.
      - `CLAUDE.md`: `:1070-1074` → the §D-flip12-default sentence (**Default is byte-identical
        except `enforce_rls`**); §"Multitenancy" Rules **add** *"`enforce_rls` is enforced through
        `SecurityPosture` (`data.*`, contributed by `SAModule`), never by a raise in a module —
        one refusal path (Plan 056 / FLIP-12); never add `assert_rls_enabled()` to a startup
        hook"* and *"the RLS posture collector lives in `varco_sa`, never `varco_fastapi.tenancy`
        (the seam rule) — `TenancyModule` owns `tenant.*`, `SAModule` owns `data.*`"*; §"Outbound
        and inbound webhooks" Rules: the *"`encryptor=None` is a documented dev/test-only
        default"* sentence → *"`encryptor` is **required** on both durable repositories (Plan 056 /
        FLIP-13, `TypeError`); the dev/test hatch is `PlaintextFieldEncryptor(acknowledge_plaintext=True)`,
        reported by `posture.webhook_secrets_plaintext`; never add an `encryptor=None` branch back"*;
        Decision Tree multitenancy branch: *"is my deployment actually protected? →
        `inspect_rls_posture()`"* gains *"(automatic since 4.0 via `SAModule` → `data.*`)"*. One
        line each — link, don't inline.
      **Verify**: `rg -n 'encryptor=None|byte-identical to pre-Plan-007|enforce_rls=False' README.md technical_docs/ CLAUDE.md ARCHITECTURE.md varco_*/varco_*`
      → 0 hits outside CHANGELOG history and the explicit "except `enforce_rls`" sentences;
      `rg -n 'plaintext otherwise|stores secrets in plaintext' varco_*/varco_*` → 0.

### Verification

18. [ ] `make test` (all eleven suites; the 054 autouse fixture keeps default apps booting; the new
        refusal tests `delenv` it explicitly); `uv run pytest varco_sa/tests -m integration -q` and
        `uv run pytest varco_beanie/tests -m integration -q` when Docker is available (Step 9's
        legs must run as the **non-superuser** roles — a green run as the container role proves
        nothing, 037 DoD 2).

19. [ ] `make lint` (no-`PKG` path: `api-check` clean **with the regenerated snapshot**,
        `asyncapi-check` unchanged, `import-budget --check --warn-only` no breach — if `varco_sa`
        breaches, make the `rls_posture` re-export lazy and re-measure); `make type-check` clean
        (the two `# type: ignore[arg-type]` in tests are the only new suppressions; M1 +2,
        directional only); `rg -n 'enforce_rls' varco_*/varco_*` now hits `settings.py`,
        `rls_posture.py`, `modules.py` (the reader exists); `rg -n 'assert_rls_enabled\(' varco_*/varco_*`
        → still only its definition (no module calls it); `rg -n 'PlaintextFieldEncryptor' varco_*/varco_*`
        → `encryption.py`, `base.py` (message), `posture.py` (detection) only;
        `git diff --stat -- design/api-freeze-and-standards/measurements/asyncapi-example.json` → empty;
        `rg -n 'versions/.*rls' varco_sa/varco_sa/migrations/` → 0 (no revision shipped).

## Edge cases

- **Single-tenant SA app on Postgres, `TenancySettings` never constructed** → `SAModule` builds
  `TenancySettings.from_env()` (`enforce_rls=True`), candidate set is empty (no `tenant_id`
  column anywhere) → no query, no finding. Byte-identical.
- **Multi-tenant app on SQLite in tests** → dialect gate → `data.rls_not_applicable` INFO; no
  catalog query; nothing blocks.
- **`isolation=SCHEMA` with `tenant_id` columns present** → not-applicable INFO; RLS is
  additive there (037 Edge cases) and never asserted.
- **Database down at startup** → `refresh()` catches the connection error, logs one WARNING,
  `data.rls_not_checked` NOT_ASSESSED; the posture module does not refuse; whatever health/
  readiness path the app has reports the outage. Never a refusal for an unreachable DB.
- **`varco diagnose`** → modules never start → NOT_ASSESSED *"SAModule.start() has not run"*;
  exit code unaffected (052).
- **Owned + forced table** (`provision_rls_app_url`'s shape) → clean. **Owned + unforced** →
  `rls_not_forced` WARN *and* `role_bypasses_rls` HIGH. **Non-owner + unforced** → WARN only.
- **Superuser role with a perfect policy** → `role_bypasses_rls` HIGH — the local-docker case;
  `VARCO_SECURITY_ENV=development` logs it (derived `warn`); an explicit
  `VARCO_SECURITY_ENFORCE=refuse` in development refuses, as in 3.2's opt-in.
- **`enforce_rls=False` + candidates** → `data.rls_not_enforced` WARN, no query; production
  blocks unless suppressed. **`enforce_rls=False` + no candidates** → nothing.
- **RLS enabled, policy present, `rls_set_tenant=False`, hook installed by hand on the default
  `AsyncSession` class** → marker present → no `guc_setter_absent`. Installed on a *custom*
  `sync_session_class` → not detected → WARN → suppress (documented).
- **A table enabled after boot** → seen at the next start; RLS DDL is a deploy.
- **`tenant_id` column nullable** → still a candidate for the *check* (catalog state is what it
  is); the nullable decision remains `plan_tenant_rls`'s at DDL time.
- **`SAWebhookSubscriptionRepository(url=…)` with no `encryptor`** → Python `TypeError`
  (missing keyword). **`encryptor=None`** → the §D-flip13-refuse `TypeError`. **`encryptor=Fernet.generate_key()`**
  (the key, not the encryptor) → the same `TypeError`, `got bytes`.
- **`PlaintextFieldEncryptor(acknowledge_plaintext=1)`** → `1 is not True` → `TypeError`
  (identity, 053's rule).
- **3.x rows read through a Fernet repository before `reencrypt_legacy_plaintext()`** →
  `ValueError`/`EncryptionError` on first read — loud, named in §Migration; the method fixes it in
  one pass; a second pass rewrites `0`.
- **050's provider with a bound `PlaintextFieldEncryptor`** → resolves; `posture.webhook_secrets_plaintext`
  WARN; production refuses unless suppressed (two statements).
- **`InMemoryWebhookSubscriptionRepository` in production** → `di.in_process_backend` (047/054),
  never `posture.webhook_secrets_plaintext`.
- **An out-of-tree `WebhookSubscriptionRepository` with `_encryptor = None`** → still reported
  (the `None` branch of the detection stays for exactly this case).

## Migration (3.x → 4.0; Plan 058 assembles this section verbatim)

| Before (3.2) | After (4.0) | Action if you relied on the old behaviour |
|---|---|---|
| `enforce_rls=False`, and **nothing read the flag** — RLS was checked only if you called `assert_rls_enabled()` yourself | `enforce_rls=True`; `SAModule.start()` reads the RLS catalog state of every `TENANT`-scoped table with a tenant column (Postgres, `SHARED`) and `SecurityPosture` reports `data.rls_disabled` (HIGH), `data.rls_not_forced` (WARN), `data.role_bypasses_rls` (HIGH), `data.rls_enabled_without_policy` (WARN), `data.rls_guc_setter_absent` (WARN). Under the 4.0 `refuse` default a production app with unprotected tenant tables **does not start**; `development` logs and starts. Single-tenant, non-Postgres, `SCHEMA`/`DATABASE` and unreachable-DB apps never block | Adopt RLS: `postgres-rls.md` §"Upgrading a 3.x deployment" (fix the role → `VARCO_TENANCY_RLS_SET_TENANT=true` → `plan_tenant_rls()` → one reviewed revision, policy/ENABLE/FORCE → verify as the app role). Defer: `VARCO_TENANCY_ENFORCE_RLS=false` **and** `VARCO_SECURITY_SUPPRESS=data.rls_not_enforced` (or `VARCO_SECURITY_ENFORCE=warn` wholesale). You set the GUC yourself: suppress `data.rls_guc_setter_absent` |
| `data.role_bypasses_rls` fired for any owned table | Fires only for a superuser, `BYPASSRLS`, or an owned table **without** `FORCE` | Nothing (fewer false HIGHs) |
| `SecurityPostureModule` contributed `DataPostureCollector(rls_posture=None)` → always `data.rls_not_checked` | `SAModule` contributes the live `RlsPostureCollector`; no `SAModule` → no `data.*` at all | Hand-wired apps: list `SAModule`, or `SecurityPostureLifecycle(collectors=[RlsPostureCollector(posture=await inspect_rls_posture(...))])` |
| `RlsPosture(current_role, is_superuser, rolbypassrls, owned_tables, tables)` | Trailing `dialect: str = "postgresql"` | Nothing (additive) |
| `SAWebhookSubscriptionRepository(url=…)` / `BeanieWebhookSubscriptionRepository(url=…, db_name=…)` stored `active_secrets` in **plaintext** | `encryptor` is a **required** keyword on both; omitted → `TypeError` (missing keyword); `None` or a non-`FieldEncryptor` → `TypeError` naming the fixes | Production: `encryptor=FernetFieldEncryptor(key)` (or bind `FieldEncryptor` for 050's provider). Dev/test: `PlaintextFieldEncryptor(acknowledge_plaintext=True)` — reported as `posture.webhook_secrets_plaintext` (WARN, blocks production unless suppressed) |
| Existing rows written under `encryptor=None` were readable | **Unreadable** through a real encryptor (`ValueError`/`EncryptionError` on first read) | Once, after deploying the encryptor: `await repo.reencrypt_legacy_plaintext()` (returns the row count); idempotent |
| `posture.webhook_secrets_plaintext` fired on `_encryptor is None` | Fires on a bound `PlaintextFieldEncryptor` or a `None`/absent encryptor; never on the in-memory repository | Nothing |

## Docs

Listed in Step 17 (feature docs, README, ARCHITECTURE, CLAUDE.md) and Step 16 (CHANGELOG,
BACKLOG). No new `technical_docs/features/*.md` — `postgres-rls.md` already owns RLS,
`outbound-webhooks.md` owns the repositories, `security-posture.md` owns the ids. Plan 058 folds
§Migration into the consolidated guide.

## Verification

```bash
# Preflight (Step 1)
rg -n 'enforce_rls|assert_rls_enabled\(' varco_*/varco_*          # settings.py + rls_check.py prose only, today

# Phase A — unit (no Docker)
uv run pytest varco_core/tests/test_tenancy_settings.py varco_sa/tests/test_rls_candidates.py \
              varco_sa/tests/test_rls_findings.py varco_sa/tests/test_rls_posture_collector.py \
              varco_sa/tests/test_sa_module_rls.py varco_sa/tests/test_rls_autogen.py \
              varco_fastapi/tests/test_posture_collectors.py -q
# Phase A — integration (non-superuser roles; Docker)
uv run pytest varco_sa/tests/test_rls_posture_collector_integration.py -m integration -q

# Phase B
uv run pytest varco_core/tests/test_plaintext_field_encryptor.py \
              varco_sa/tests/test_webhook_repository_encryptor_required.py \
              varco_beanie/tests/test_webhook_repository_encryptor_required.py \
              varco_fastapi/tests/test_posture_collectors.py -q
uv run pytest varco_sa/tests varco_beanie/tests -m integration -k webhook -q

# Package + gates
make test
uv run python scripts/api_surface.py            # additions only
make lint                                        # api-check clean / asyncapi-check unchanged / import-budget no breach
make type-check

# Boundary checks
rg -n 'assert_rls_enabled\(' varco_*/varco_*                          # definition only — no module calls it
rg -n 'PlaintextFieldEncryptor' varco_*/varco_*                        # encryption.py, webhook/base.py, posture.py
rg -n 'encryptor=None|plaintext otherwise' varco_*/varco_* README.md technical_docs/   # 0
rg -n 'byte-identical to pre-Plan-007' CLAUDE.md technical_docs/ README.md              # only the "except enforce_rls" sentences
ls varco_sa/varco_sa/migrations/versions/ | rg -i rls                  # empty — no shipped RLS revision
git diff --stat -- design/api-freeze-and-standards/measurements/asyncapi-example.json   # empty
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| **A 4.0 multi-tenant production deployment with tenant tables and no RLS stops booting** — the row's purpose and this plan's widest break | The refusal is loud, names every table and the recipe; two greppable hatches exist; `development` never blocks by default | §Migration row 1 first; CHANGELOG BREAKING; `postgres-rls.md` upgrade section; `varco diagnose` shows `enforce_rls=true` and the derived `refuse` |
| **A single-tenant app is flagged** | Zero `data.*` findings when no table carries the tenant column | §D-flip12-candidates gate 3; `test_rls_posture_collector.py` empty-candidate case; Step 4's no-column exclusion |
| **A false HIGH on non-Postgres** (premise 3) | `data.rls_disabled` is impossible when `posture.dialect != "postgresql"` | `RlsPosture.dialect`; `test_non_postgres_is_info_only` |
| ⚠️ ASSUMPTION — 050's `SAModule.start()` runs in `INFRA` **before** `SecurityPostureModule.start()` (last, 054 `:478`) so `refresh()` precedes `collect()` | The collector never reports NOT_ASSESSED in a normally started app | Step 1 reads 050's phase table; Step 8's full-app test asserts a live finding, not NOT_ASSESSED |
| ⚠️ ASSUMPTION — 051's `contribute()` is callable from `SAModule.register()` (051 §D-posture shows it from `configure_app()`) | The collector reaches `acollect(container, PostureCollector)` | Step 1; fallback: contribute from `SAModule`'s own `configure_app()` if it is an `HttpContributor`, else from `start()` before `refresh()` (still before the posture module starts) |
| ⚠️ ASSUMPTION — `SAConfig.engine` is the app's runtime engine (the role whose posture matters), not a migration engine | `role_bypasses_rls` reflects the app role | Documented; an app with a separate migration role sees the app role, which is the correct subject |
| ⚠️ ASSUMPTION — the `sqlalchemy.exc` classes named in §D-flip12-candidates cover asyncpg/psycopg connection failures at `engine.connect()` | An unreachable DB is NOT_ASSESSED, never an unhandled exception in `start()` | `test_rls_posture_collector.py`'s raising-engine case; if a driver raises a different class, add it to the tuple (never a bare `except Exception`) |
| **`data.rls_guc_setter_absent` false positive** for an app that sets the GUC by its own means | One suppression line restores the start | §D-flip12-guc; Pitfalls row |
| **`reencrypt_legacy_plaintext()` misclassifies** a ciphertext as plaintext | Only a decrypt *failure* triggers a rewrite; a rewrite of a real ciphertext would require Fernet to fail on its own token | Docstring; the idempotency test (second run rewrites `0`) |
| **A 3.x operator misses the re-encryption step** and reads fail in production | The failure is loud (`EncryptionError`/`ValueError`), the CHANGELOG and the `TypeError` message both name the method | §Migration row 6; the `_ENCRYPTOR_REQUIRED` text |
| `api_surface.py --check` red after Step 11 | Additions only; snapshot regenerated | Step 15; Step 19 |
| Import budget breach in `varco_sa` from the new top-level re-export | No breach | Step 19; lazy re-export fallback |
| Two flips' escape hatches interact with 054 (`VARCO_TENANCY_ENFORCE_RLS=false` needs a suppression to start in production) | Each is independently visible: the WARN names the variable, the refusal names the WARN | Edge cases; §Migration; Pitfalls |
| Docs drift — a doc still says `enforce_rls=False` is the default or `encryptor=None` is a dev default | No surviving 3.2 claim | Step 17's `rg` checks |

## Done

- [ ] `TenancySettings.enforce_rls` defaults `True`; every other default pinned unchanged;
      docstrings and CLAUDE.md's byte-identical sentence rewritten.
- [ ] `rls_candidate_tables()`, `SAModelRegistry.registered()`, `RlsPosture.dialect`,
      `rls_findings()`, `RlsPostureCollector`, `hook_installed()` shipped with docstrings; the
      three narrowings (premises 2/3/4) pinned by Docker-free tests; `DataPostureCollector`
      delegates.
- [ ] `SAModule` contributes and refreshes the collector (or the pre-050 hand form is documented);
      `SecurityPostureModule` no longer contributes `DataPostureCollector`; the full-app refusal /
      development / suppression tests green.
- [ ] Integration legs (a)-(f) green as the non-superuser roles, including the hook end-to-end.
- [ ] `PlaintextFieldEncryptor`; `encryptor` required on both durable repositories with the
      shared `TypeError` text; `reencrypt_legacy_plaintext()` on both; posture detection updated;
      in-repo sites swept; api-surface snapshot regenerated and `--check` clean.
- [ ] Severity table carries every `data.*` id and the corrected webhook row; 054's register test
      green; docs per Step 17; CHANGELOG per Step 16; BACKLOG rows `FLIP-12`, `FLIP-13` →
      `done (Plan 056)`.
- [ ] `make test`, `make lint`, `make type-check` green; no RLS revision under
      `varco_sa/migrations/versions/`.

## Out of scope / Parked

- **Framework-table RLS posture** (`data.framework_rls_*`) — un-park when a consumer asks for
  RLS on `varco_dead_letters`/`varco_audit_log` specifically; needs the nullable-`tenant_id`
  decision per table (037 §D-S12-nullable) before any finding could be more than INFO.
- **Flipping `rls_set_tenant`/`rls_require_tenant`** — 037's reasoning stands; revisit if
  `data.rls_guc_setter_absent` suppression counts show the self-setting app is rare.
- **A `varco webhook reencrypt-secrets` CLI verb** — one `asyncio.run(repo.reencrypt_legacy_plaintext())`;
  add if a second backend or a dry-run mode is requested.
- **Detecting a hook installed on a custom `sync_session_class`** — `hook_installed()` reads the
  default class only; a per-factory registry is more machinery than the false positive costs.
- **A `Literal[True]` sweep of the four `acknowledge_*` `ValueError` sites** — 053 parked it;
  `PlaintextFieldEncryptor` follows 053's shape, not the older one.
- **Re-tiering `data.rls_disabled` to WARN** — considered (app-layer scoping still runs), rejected:
  `postgres-rls.md` §4 records that `TenantAwareService._scoped_params` fails open below the
  service layer, which is a hole class, not a non-adoption; and `development` never blocks by
  default. Recorded so it is not re-proposed as a "softening".
- **`TenancyModule` contributing the RLS collector** — premise (5); recorded so the task brief's
  placement is not re-proposed without an engine-free design.
