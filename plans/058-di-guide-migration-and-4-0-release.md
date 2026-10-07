# Plan 058 — The one DI guide, the 3.x → 4.0 migration guide, the deprecation-floor re-label, and the `v4.0.0` release (DI-14, Open Question 3, the release)

Covers BACKLOG 4.0 row **`DI-14`** (`BACKLOG.md:79` — *"One DI guide —
`technical_docs/features/dependency-injection.md` (the contract, the default model, selection,
multi-binding, lifecycle phases, testing), a README 'Wiring an app' section, an ARCHITECTURE.md
entry-point table generated from the same source as `scripts/api_surface.py`, and the CLAUDE.md
taxonomy table rewritten to the new shape — plus the 3.x → 4.0 migration guide"*), **Open
Question 3** (`BACKLOG.md:135-138` — *"Does 4.0.0 also renumber the 3.x → 4.0 deprecation
windows? … Confirm the nine existing `removed_in="4.0.0"` sites stay un-removed in 4.0.0
(12-month floor: not before 2027-08-31) and get re-labelled, or the `--check` gate in
`scripts/api_surface.py` will read a removal as a break"*), and **the 4.0.0 release mechanics**
(`plans/000-index-4-0-di-standardization.md:59,73`).

**Last plan of the 4.0 set** (slice row 058). It **assembles** what Plans 045–057 produced and
**decides nothing** those plans decided; where two plans disagree, §Cross-plan reconciliation
names the winner and the docs describe *that* shape. It depends on all thirteen and must be built
after every one of them has landed (Step 0 proves it).

**Sources of record read for this plan (2026-09-20):** `plans/000` (the index; "Done for the whole
set" at `:61-73`); `plans/045`–`057` (each one's §Design, §Migration, §Docs, and every "058"
handoff — grep'd, all 47 mentions read); `BACKLOG.md:16-138` (the live cycle), `:59-101` (the
rows), `:135-138` (OQ3), `:226-267` (answered decisions); `CONTRIBUTING.md:68-134` (SemVer,
lockstep, the 12-month removal floor at `:98-105`, the four worked examples at `:112-118`);
`varco_core/varco_core/deprecation.py` (the helper: `_build_message` `:75-97`, `deprecated`
`:100-176`, `deprecated_alias` `:266-347`; `__all__` `:65`); every `removed_in="4.0.0"` hit
(§Context); `scripts/api_surface.py:96-235` (what `--check` records); `scripts/bump.py:362-383`
(`--set`, `--dry-run`, `--no-lock`, `--check`; runs `uv lock`); `CHANGELOG.md:1-49` (the
`[Unreleased]` shape; release-date headings `:49,425,823`); `README.md:81-209` (the TOC) and its
four DI-shaped sections (`:709`, `:1951`, `:3479`, `:3714`); `ARCHITECTURE.md:7-173` (package
map), `:1560-1627` (the stale "DI Wiring" section); `CLAUDE.md` §"DI wiring (providify)" + §"DI
wiring verb taxonomy"; `mkdocs.yml:3,103-147` (`docs_dir: technical_docs`, the nav — a
"Migration Notes" group already exists at `:145-146`); `scripts/gen_guide_pages.py` (the
`docs/` → `guides/` copy, a literal tuple); `.github/workflows/docs.yml` (mike `<major>.<minor>` +
`latest`, the final-tag guard regex at `:98`); `.github/workflows/test.yml:40-73` (the `lint` job's
steps); `Makefile:157-227` (the no-`PKG` lint block at `:163-167`);
`design/varco-1-0-release/release-runbook.md` §5–§6 (the rc rehearsal + final-tag shape);
`UPSTREAM-GAPS.md` (P24–P27 open at plan time).

> ⚠️ **Four premises corrected from source before anything was decided.**
> (1) **"Nine `removed_in="4.0.0"` sites" is five code sites plus four docstring examples.**
> `rg -n 'removed_in="4\.0\.0"' varco_*/varco_*` → 9 hits: `varco_beanie/bootstrap.py:177`,
> `varco_beanie/__init__.py:133`, `varco_sa/rls.py:259`, `varco_core/migration/__init__.py:66,71`
> (the five deprecations Plan 022 shipped — AB-1, AB-2 ×2, AB-4 ×2) and
> `varco_core/deprecation.py:148,285,292,295` (the helper's own docstring examples). All nine are
> re-labelled; only the five change behaviour (the message text). (2) **052's "eight direct
> `DeprecationWarning` sites" is three.** `rg -n DeprecationWarning varco_*/varco_*` outside
> `deprecation.py` → 18 hits, of which **15 are prose** ("no `DeprecationWarning`, no removal
> scheduled"); the ad-hoc `warnings.warn(..., DeprecationWarning)` calls are exactly
> `varco_fastapi/auth/trust_store.py:128-135`, `varco_core/tenancy/control/consumer.py:183-190`,
> and `varco_fastapi/middleware/tenant_resolution.py:150-162` — the last is converted by Plan 055
> (§D-flip6-floor); this plan converts the other two (§D-relabel). (3) **`api_surface.py --check`
> cannot see a re-label.** It records, per name in each package's top-level `__all__`, only
> `kind`/`module`/(functions) `signature` (`scripts/api_surface.py:181-213`); every deprecated name
> is deliberately *outside* `__all__` (`rls.py:253-256`, `beanie/__init__.py:127-128`,
> `bootstrap.py:172`, `migration/__init__.py:45-57`), and `removed_in=` is never read. OQ3's fear
> — "the `--check` gate will read a removal as a break" — is true only if a name were *removed*;
> re-labelling changes the snapshot by zero bytes (Step 3 proves it). (4) **CLAUDE.md's claim that
> `asyncapi-check` "rides in `test.yml`'s existing `lint` job" is not true of the file today.**
> `test.yml:53-73` runs ruff, mypy, `api_surface.py --check`, `import_budget.py --check --warn-only`
> — no `export-asyncapi --check` step; it runs only via `make lint` (`Makefile:166`). ⚠️ Step 0
> re-checks after 047/048 add their steps; Step 5 adds the missing step beside the new
> `entry_points.py --check` so the documented claim becomes true (one line, same job).

> **Handoff from Plan 046 (landed):** §Migration in `plans/046-wiring-verb-collapse-and-explicit-container.md` is the DI-2/DI-8/DI-13 CHANGELOG entry, already landed verbatim in `CHANGELOG.md`'s `[Unreleased]` section; the CLAUDE.md DI wiring verb taxonomy table is in its final shape for this plan to only polish, not restructure.

## Goal

After this plan: **`technical_docs/features/dependency-injection.md` is the single DI document**,
in the section order §D-guide fixes, with every plan's section in place, the 045 "Plan 058
completes this document" banner gone, and **one** merged Pitfalls table; **`README.md` has one
"Wiring an app" chapter** (the 8-line default app, backend selection, settings, feature enabling,
contributing from your own package, `varco diagnose`, testing) and the TOC lists it; **the
entry-point table is generated** by `scripts/entry_points.py` from the same `scripts/packages.sh`
derivation as `api_surface.py`, written into `ARCHITECTURE.md` (marked block) and
`technical_docs/entry-points.md`, with a JSON snapshot and a `--check` wired into `make lint`'s
no-`PKG` path and CI's `lint` job; **CLAUDE.md's DI sections are a routing table again** (the
taxonomy table is a pointer to the generated one; every rule the thirteen plans added is one line
+ link); **`technical_docs/migrations/3-to-4.md` is the one 3.x → 4.0 migration guide**, every
plan's §Migration assembled, de-duplicated, ordered by what a user hits first, ending with "run
`varco diagnose --check`"; **the nine `removed_in="4.0.0"` sites read `5.0.0`**, the two ad-hoc
`warnings.warn` deprecations go through `warn_deprecated()`, and
`varco_core/tests/test_deprecation_floor.py` proves — from `CHANGELOG.md`'s release-date headings
— that no `removed_in` earlier than the 12-month floor allows exists; **`CHANGELOG.md` has a
`[4.0.0]` section** in the fixed order §D-changelog defines, linking the migration guide, with an
`Upstream` block naming every providify ask the cycle filed or closed; **`BACKLOG.md` is rolled
up** (every `DI-*`/`FLIP-*` row → Shipped, `FLIP-14` designed, OQ1/OQ2/OQ3 answered, the cycle's
parks recorded); **`UPSTREAM-GAPS.md` lists exactly the open asks with a report and a strict xfail
each**; every snapshot `--check` is green; **`v4.0.0` is tagged** via `scripts/bump.py`, published
by `release.yml`, and the docs site serves `4.0` + `latest`.

## Non-goals

- **Re-deciding anything in 045–057.** Where their outputs conflict, §Cross-plan reconciliation
  records the winner *those plans already chose*; this plan never picks a third option.
- **Removing any deprecated symbol.** The floor (`CONTRIBUTING.md:98`) puts the earliest removal
  at 2027-08-31 (`since="3.0.0"`) / 2027-09-12 (`since="3.2.0"`); 4.0.0 *re-labels* and *starts*
  windows, it sweeps none (`BACKLOG.md:54`, `plans/000:10-12`).
- **Any change to providify.** No new ask is filed here; the ledger is *audited* (§D-upstream).
- **Wrapping or re-exporting providify's pytest fixtures** (`BACKLOG.md:222`, RL-3) — the Testing
  chapter documents 052's recipe verbatim.
- **A `Literal[True]` sweep of the four `acknowledge_*` `ValueError` sites** (053/056 parked it
  "for 058 to schedule") — **not scheduled**; parked with a trigger in BACKLOG (Step 14).
- **Deprecating `varco_fastapi.posture.PostureFinding`** (051: "058 may deprecate the
  `varco_fastapi` one later") — **not done**; it stays a silent re-export (soft deprecation per
  `CONTRIBUTING.md:120-124`), recorded in §Cross-plan reconciliation.
- **A `register_webhook_signer()` verb mirroring 051's `register_webhook_verifier()`** ("058 can
  mirror it if asked") — nobody asked; parked.
- **Flipping `import_budget.py --check` from warn-only to a gate** — Plan 028 Phase 2's own
  criterion (≥ 10 CI observations) is unmet; stays `--warn-only`.
- **`varco diagnose --check` as a `make lint` gate** — 052 §D-cli rejected it (environment-
  dependent); the migration guide tells the *user* to run it in *their* CI.
- **An umbrella `varco` meta-package or per-package versions** — standing parks.
- **Rewriting pre-052 tests onto `InMemoryProfile`** (054 called it "058-scale work") — not this
  cycle; a BACKLOG row is *not* opened either (no defect, pure style).

## Context

### The deprecation inventory, from source (2026-09-20)

| # | Site | Helper | `since` | `removed_in` today | Action |
|---|---|---|---|---|---|
| 1 | `varco_sa/varco_sa/rls.py:257-262` (`enable_rls_ddl`, AB-1) | `deprecated()` | `3.0.0` | `4.0.0` | → `5.0.0`; comment `:256` |
| 2 | `varco_core/varco_core/migration/__init__.py:62-73` (`MigrationPlan`, AB-2) | `deprecated_alias()` | `3.0.0` | `4.0.0` | → `5.0.0`; docstring `:16` |
| 3 | same file, `:67-72` (`MigrationError`, AB-2) | `deprecated_alias()` | `3.0.0` | `4.0.0` | → `5.0.0` |
| 4 | `varco_beanie/varco_beanie/__init__.py:129-134` (`BeanieConfig`, AB-4) | `deprecated_alias()` | `3.0.0` | `4.0.0` | → `5.0.0`; comment `:128` |
| 5 | `varco_beanie/varco_beanie/bootstrap.py:173-178` (`BeanieConfig`, AB-4) | `deprecated_alias()` | `3.0.0` | `4.0.0` | → `5.0.0`; comment `:172` |
| 6–9 | `varco_core/varco_core/deprecation.py:148,285,292,295` | docstring examples | `3.0.0` | `4.0.0` | → `5.0.0` (examples must not teach a violated floor) |
| 10 | `varco_fastapi/varco_fastapi/auth/trust_store.py:128-135` (legacy `TrustStore.__post_init__`, Plan 026) | **ad-hoc** `warnings.warn`, `stacklevel=3`, text "will be removed in 4.0.0" | 3.1.0 (`CHANGELOG.md:425`, 2026-09-05) | prose | → `warn_deprecated("varco_fastapi.auth.TrustStore", since="3.1.0", removed_in="5.0.0", replacement="varco_core.tls.TrustStore", stacklevel=3)`; module docstring `:5` |
| 11 | `varco_core/varco_core/tenancy/control/consumer.py:183-190` (`TenantProvisionConsumer(provisioner=, catalog=)`, Plan 008/022) | **ad-hoc** `warnings.warn`, text "deprecated since 3.0.0 and will be removed in 4.0.0" — the one recorded §D-DEP deviation (`:173-182`: argument-conditional, so `@deprecated` cannot decorate it) | `3.0.0` | prose | → `warn_deprecated("TenantProvisionConsumer(provisioner=..., catalog=...)", since="3.0.0", removed_in="5.0.0", replacement="control_service=TenantControlService(...)")` — the comment block's reason for not using `@deprecated` stays true; the *parameter-path* helper 053 added is exactly its shape |
| 12 | `varco_fastapi/varco_fastapi/middleware/tenant_resolution.py:150-162` | ad-hoc | `3.2.0` | prose "4.0.0" | **done by 055** (`since="3.2.0", removed_in="5.0.0"`) — Step 0 confirms, Step 2 does not touch |
| 13 | `varco_core/varco_core/tenancy/sources.py:255` (`LegacyTenantSource` "is removed in 4.0") | docstring | — | prose | **done by 055** (the explicit source is never removed) |
| — | `ApiKeyAuth(keys=)` (053) | `warn_deprecated()` | `4.0.0` | `5.0.0` | already correct; the floor test's "since = pending release" case |

Prose that says "until/in 4.0.0" and must move to 5.0.0 with the floor sentence:
`CONTRIBUTING.md:113` (+ the four bullets `:115-118`), `ARCHITECTURE.md:307,1144`,
`CLAUDE.md:397,1048`, `technical_docs/features/postgres-rls.md:65`,
`technical_docs/features/tls-trust-and-hot-reload.md:280`, `technical_docs/features/schema-migrations.md`
(⚠️ Step 0 greps — the rename story lives there), `varco_fastapi/auth/trust_store.py:5`,
`varco_core/migration/__init__.py:16`. `tenant-provenance.md:151` is 055's.

**Release-date facts** (`CHANGELOG.md` headings, the only in-tree record): `3.0.0 — 2026-08-31`
(`:823`), `3.1.0 — 2026-09-05` (`:425`), `3.2.0 — 2026-09-12` (`:49`). The 12-month floor
(`CONTRIBUTING.md:98`): `since="3.0.0"` → not before **2027-08-31**; `3.1.0` → **2027-09-05**;
`3.2.0` → **2027-09-12**; `4.0.0` (the pending release, ~2026-09) → not before ~2027-09.
**Conclusion, OQ3: every window that names 4.0.0 as its removal moves to 5.0.0; the
major-version gate is necessary, not sufficient** (`BACKLOG.md:54` already says so; this plan
makes it mechanical — §D-floor).

### What each plan hands this one (the assembly inputs)

| Plan | Guide section (final, written by that plan) | §Migration entry | Snapshot(s) it added | Handoff note verbatim |
|---|---|---|---|---|
| 045 | §Default bindings (+ Pitfalls) | DI-3/DI-15 | `api-surface` (regen iff `setup_varco_defaults` reported) | *"§Default bindings is final; §Migration below is the 4.0 guide's DI-3 entry"* |
| 046 | §Wiring contract (+ Pitfalls); CLAUDE.md taxonomy table "final in shape" | DI-2/DI-8/DI-13 | `api-surface` | *"§Migration below is the DI-2/DI-8/DI-13 entry; the CLAUDE.md table is final in shape"* (058 polishes) |
| 047 | §Selecting a backend, §Enabling a feature (+ Pitfalls); `technical_docs/di-catalogue.md` **generated** | DI-4/DI-12/DI-17 | `di-catalogue.json` + `make di-catalogue-check` | *"§Selecting a backend / §Enabling a feature are final; … `di-catalogue.md` is generated, link it, never edit it"* |
| 048 | §Settings (+ Pitfalls); `technical_docs/settings-reference.md` **generated**; README's 67 hand-written `VARCO_*` rows deleted | DI-6/DI-16 | `settings-reference.json` + `make settings-check` | *"058 polishes"* (the README per-feature context) |
| 049 | §Modules (+ Pitfalls); README "Wiring an app" **v1**; ARCHITECTURE Modules table | DI-1/FLIP-1/RO-4 | `api-surface`, `settings-reference` | *"058 — §Migration below"* |
| 050 | §Lifecycle phases (+ Pitfalls); ARCHITECTURE Modules table gains `phase` | DI-9/DI-10 | `api-surface`, `di-catalogue` | *"058 — §Migration below"* |
| 051 | §Multi-binding (+ Pitfalls); README "Contributing from your own package" | DI-5 | `api-surface` | *"058 — §Migration below"*; two parks handed over (§Non-goals) |
| 052 | §Diagnostics, §Testing (final; + Pitfalls); README "varco diagnose", "Testing a varco app" | DI-7/DI-11/RO-2 | `api-surface`, `settings-reference` | *"§Diagnostics/§Testing are final; … if P28 was filed, list it under open gaps"*; "Converting the eight direct `DeprecationWarning` sites — 058's re-label sweep" |
| 053 | — (feature docs only) | FLIP-5, FLIP-034-1..4 | `api-surface` (+ `warn_deprecated`), `settings-reference`, `di-catalogue` | *"Plan 058 folds §Migration into the consolidated guide"*; parked the `Literal[True]` sweep "058 may schedule" |
| 054 | — (severity table in `security-posture.md`) | FLIP-2/3/4/10 | `api-surface` | *"Plan 058 folds §Migration into the consolidated guide"* |
| 055 | — (FLIP-14 design in `admin-surface-tenancy.md`) | FLIP-6/7/9 | `api-surface`, `settings-reference` | *"Plan 058 folds §Migration … and adds the `chain=None` `TypeError` to 5.0.0's removal list"* |
| 056 | — | FLIP-12/13 | `api-surface`, `settings-reference` | *"Plan 058 folds §Migration into the consolidated guide"* |
| 057 | — | FLIP-040-1/2, RO-5 | `api-surface` | list-valued-leaves question: *"otherwise leave for 058's rollup"* |

### Cross-plan reconciliation

Where two plans describe one thing differently, the later/owning decision wins and **the docs
describe only the winner**. The builder of this plan must not re-open any row.

| # | Topic | What one plan said | What the other did | Winner (docs describe this) |
|---|---|---|---|---|
| R1 | Tenant-resolution middleware wiring | 049 §D-http's `EdgeSlot` table had no tenant slot; `TenancyModule` wrapped `TenancyLifecycle` only | 055 §D-flip6-slot/§D-flip6-module added `EdgeSlot.TENANT_RESOLUTION = 95` and made `TenancyModule` an `HttpContributor` (+ `catalog`/`chain`/`server_auth`/`membership`/`header`/`reject_status`, `pool` optional, `validate()`) | **055**. The `EdgeSlot` table in the guide and ARCHITECTURE has 15 members |
| R2 | Where the RLS posture collector lives | The task brief for 056 said `TenancyModule` | 056 §D-flip12-collector: `RlsPostureCollector` in `varco_sa`, contributed by `SAModule` (051's handoff `:693-694`; `varco_fastapi.tenancy` may not import `varco_sa`) | **056** |
| R3 | `BACKEND_PRIORITY` | 045 set `100` | 047 §D-precedence: **not amended**; `ENV_OVERRIDE_PRIORITY = 200` above it; code tier wins by the fallback post-step, never by priority | **047 confirming 045**: three tiers `0 / 100 / 200`, all `@Fallback` |
| R4 | `EventConsumer` wiring rule | CLAUDE.md (3.x): `register_to()` "typically in a `@PostConstruct` method"; 049's example `PostAppModule.start()` called `consumer.start()` | 051 §D-consumer: `@Multibound EventConsumer`, auto-`register_to` by `CoreModule` after the bus starts; 050 §D-components phased it (`core.bus` at `BUS`, `core.consumers` at `CONSUMERS`) | **051 + 050**. No doc may say `@PostConstruct` for `register_to` |
| R5 | Deprecation bookkeeping | 052 §D-deprecations: a process ledger (`fired_deprecations()`, `_note()` in the helper) | 053 §D-apikey: `warn_deprecated()` for parameter-path deprecations, calling `_note()` | **Both compose**; this plan's sweep uses `warn_deprecated()` so the two ad-hoc sites land in the ledger too |
| R6 | Refusal exception shapes | 053: `PassthroughAuth(acknowledge_unverified: Literal[True])` → `TypeError`; 056: `encryptor` required → `TypeError`, `PlaintextFieldEncryptor(acknowledge_plaintext: Literal[True])` → `TypeError` | 054: `server_auth=None` → `ValueError` (matches the sibling mounts' existing `ValueError`s); the four older `acknowledge_*: bool = False` → `ValueError` sites untouched | **Each plan's own**; the migration guide states each literally; no unification in 4.0 (parked, Step 14) |
| R7 | Who contributes the RO-2 collectors | 051 Step 20 handoff: `JwksRefreshModule`/`RetentionModule` | 052 §D-RO2: `CoreModule` contributes both with lazy suppliers (the blind-spot argument) | **052** |
| R8 | Posture collector ownership (final map) | 049 `_default_collectors()` (five callables); 057 added `_collect_redaction` via `partial` | 051: six `PostureCollector` classes contributed by `SecurityPostureModule.configure_app()`; 055 moved the tenant collector to `TenancyModule`; 056 moved the data collector to `SAModule` (live RLS); 052 added jwks/retention from `CoreModule` | **`SecurityPostureModule` → `HttpEdgeCollector`, `LocalPostureCollector`, `AuthPostureCollector`, `RedactionPostureCollector`; `CoreModule` → `JwksPostureCollector`, `RetentionPostureCollector`; `TenancyModule` → `TenantPostureCollector`; `SAModule` → `RlsPostureCollector`.** A non-tenant, non-SA app emits no `tenant.*`/`data.*` finding. `DIPostureCollector` (`di.in_process_backend`) has no owner — opt-in, app-contributed (054 decision) |
| R9 | `posture.webhook_secrets_plaintext` owner cell in 054's severity table | 054 wrote "055/`varco_sa`" | 056 §D-flip13-posture corrected it to **056** | **056** |
| R10 | `http.rate_limit.absent` severity | 052 §Non-goals: "no severity assigned in `varco_fastapi/posture.py` moves" | 054 §D-ratelimit re-tiered it WARN → INFO **in `introspect.py`** (035's seam), because a recorded non-flip must not become a start-blocker | **054** |
| R11 | Module ordering for `MigrationsModule` | 049 §D-order: list order + a WARNING when listed after `CoreModule` | 050 §D-phase: `Phase.INFRA` orders it; the WARNING and its test deleted | **050** |
| R12 | `setup_event_producer` | 045 left it to 046 | 046 deleted it; `BusEventProducer` is the `@Fallback` `AbstractEventProducer` (045 A10) via `varco_core.di.bootstrap()` | **046** |
| R13 | `bootstrap(streams=True)` replacement | 046 §D-streams: `bind_instance(container, RedisEventBusSettings, RedisEventBusSettings(use_streams=True))` | 048 §D-precedence: `bind_settings(container, RedisEventBusSettings, instance=…)` | **048** — the guide/README show only the `bind_settings` form; 046's form is mentioned once in the migration guide as "046-era, equivalent" |
| R14 | The in-memory test profile and FLIP-5 | 052 §D-testprofile: `InMemoryProfile` binds nothing | 053 §D-flip5-tests: the profile must `bind_instance(AbstractAuthorizer, BaseAuthorizer())` explicitly so it boots with no env vars | **053** (052 accommodates); the Testing chapter says so |
| R15 | Test-suite env discipline | 053: per-test `monkeypatch.setenv("VARCO_SECURITY_ENV", "development")`, never autouse | 054 §D-flip2-tests: an autouse `_posture_warn_only` (`VARCO_SECURITY_ENFORCE=warn`) per lifespan-entering test package | **Both** — different variables, different reasons; the Testing chapter's "env you need in a test" list has both |
| R16 | FLIP-6's `TypeError` | BACKLOG `:98` said `TenantResolutionMiddleware(chain=None)` → `TypeError` in 4.0 | 055 §D-flip6-floor: forbidden by the floor (`since="3.2.0"` → not before 2027-09-12); deprecated for **5.0.0** | **055**; §D-removals-5 lists it |
| R17 | `InMemoryJobStore`'s home | 045 A14: `varco_fastapi.job.store` | 047 Step 4 moved it to `varco_core.job.memory` (re-export kept) | **047** |
| R18 | The testkit's private `_bindings` read | 046 §D-suite: `varco_conformance.di_wiring.fallback_bindings()` — "the one private read" | 047 §D-introspect relocated it to `varco_core.di.introspect.inspect_bindings()`; the testkit name is an alias; P29 still names it | **047** |
| R19 | "Eight direct `DeprecationWarning` sites" | 052 §D-deprecations counted 8 files | Source: 3 real call sites (premise (2)); one converted by 055, two by this plan | **This plan's count**; 052's driver window still exists for out-of-tree/unknown sites |
| R20 | `PostureFinding`/`PostureSeverity` import paths | 051: canonical `varco_core.posture`; "058 may deprecate the `varco_fastapi` one" | — | **Not deprecated** — a silent re-export (soft deprecation, `CONTRIBUTING.md:120-124`); no warning, no `removed_in` |
| R21 | `CoreModule.start()` starting a backend's bus | 049 §D-core: "the seam's owner starts what resolves"; handed to 050 as a re-homing candidate | 050 §D-components: `core.bus` component at `Phase.BUS`, still in `CoreModule`; Kafka/NATS `@PostConstruct` removed (§D-bus) | **050** |
| R22 | Validate-clean allowlists | 046 `known_warnings` entries naming 050 | 050 §D-disposes: all deleted; `known_warnings == frozenset()` for the three disposer kinds | **050**; `COVERAGE.md` says the three kinds are never allowlisted again |

## Design

### §D-floor — OQ3 answered: re-label to `5.0.0`; the floor is computed from `CHANGELOG.md`'s release dates and enforced by a test

**Chosen: every `removed_in="4.0.0"` becomes `removed_in="5.0.0"`; the two ad-hoc sites become
`warn_deprecated()` calls carrying `since=`/`removed_in=`; a new Docker-free test
`varco_core/tests/test_deprecation_floor.py` walks every `varco_*/varco_*/**/*.py` for
`since="…", removed_in="…"` pairs (regex over source — no import, no execution), looks each
`since` up in `CHANGELOG.md`'s `## [X.Y.Z] — YYYY-MM-DD` headings, and asserts the floor.**

The invariant, precisely (one function, `floor_violations(pairs, release_dates, pending) ->
list[str]`, unit-tested on synthetic inputs before it is pointed at the tree):

1. `removed_in` is a *major* (`X.0.0`) with `X > major(since)` — the `CONTRIBUTING.md:93` gate.
2. `since` is either a version with a CHANGELOG heading, or exactly the **pending** version
   (the workspace version read the way `scripts/bump.py` reads it — `varco_core/pyproject.toml`
   — whose heading may not exist yet). Anything else is a typo → failure naming the site.
3. If `removed_in` **is** the pending version (a removal scheduled for *this* release) or has a
   heading already, its date (heading date, or `date.today()` when pending and un-dated) must be
   `>= since_date + 12 months` (`since_date.replace(year=+1)`, Feb 29 → Mar 1). Today, every
   `removed_in="4.0.0"` site fails this — that is the red-first proof.
4. If `removed_in`'s major is **below or equal to** a *released* major (a heading exists for
   `X.0.0` and it is not pending), the symbol should already be gone → failure ("overdue
   removal or stale label").

| Candidate for "how the date is known" | ✅ | ❌ |
|---|---|---|
| **(1) chosen: parse `CHANGELOG.md` release headings** | Already the canonical in-tree record (055 cited `CHANGELOG.md:49` for exactly this arithmetic); zero new fields, zero new tables, nothing to keep in sync; the test fails loudly if a `since` has no heading (a typo or an un-dated release — both real defects) | Couples the test to the heading format `## [X.Y.Z] — YYYY-MM-DD` — pinned by a fixture test that parses the three known headings; Keep-a-Changelog's format has been stable across 3.0/3.1/3.2 |
| (2) a `since_date=` keyword on `deprecated()`/`deprecated_alias()`/`warn_deprecated()` | Self-contained | Four signature changes, ~12 call sites, and a date the author must *remember* at the moment of deprecation — the exact "unfalsifiable removal version" rot `control/consumer.py:179-182` records fixing; and the date is knowable only *after* the release ships. Rejected |
| (3) a `RELEASE_DATES` table in `CONTRIBUTING.md` or `deprecation.py` | Explicit | A second copy of the CHANGELOG headings that drifts; `CONTRIBUTING.md:104-105` deliberately promises "no code or `removed_in=` string encodes the number" | Rejected |
| (4) `git log -1 --format=%cI vX.Y.Z` | Ground truth | CI checks out with `fetch-depth: 1` and no tags (`test.yml`), so the test would skip or fail in the one place it matters; `docs.yml` alone fetches full history. Rejected |

- ✅ The test is the mechanical form of the answered decision (`BACKLOG.md:54`, "the major-version
  gate is necessary, not sufficient"). ✅ It also catches the *next* cycle's mistake (a 5.0.0
  planner who tries to remove a `since="4.0.0"` symbol before ~2027-09). ✅ Regex over source is
  what `rg 'removed_in="…"'` (`deprecation.py:11-15`) already promised was possible.
- ❌ A deprecation whose `since=`/`removed_in=` are not string literals (built from a variable)
  is invisible to the regex — the helper's docstring already forbids that shape implicitly; the
  test's docstring states it, and every in-tree site is a literal (§Context).

**The two ad-hoc sites** (§Context rows 10–11) are converted to `warn_deprecated()` (053's helper,
`varco_core/varco_core/deprecation.py`) so they (a) carry keywords the floor test sees, (b) land
in 052's ledger, (c) use `_build_message()`'s one sentence. `trust_store.py` keeps `stacklevel=3`
(a dataclass `__post_init__` is two frames from the caller — the existing tuning); the
`TenantProvisionConsumer` site keeps `stacklevel=2` and its `:173-182` comment block, edited to
say the shape it now uses is the parameter-path helper 053 added (the deviation from `@deprecated`
is unchanged and still correct). `since` for the `TrustStore` shim is `"3.1.0"` (Plan 026 shipped
in 3.1.0, `CHANGELOG.md:425`) — ⚠️ Step 0 confirms against the 3.1.0 `### Deprecated` block.

### §D-guide — the final `dependency-injection.md`: one document, fixed section order, one Pitfalls table

**Chosen: the accumulated document is restructured (not rewritten) into this order; each section
keeps the owning plan's text as its body; the 045 banner is deleted; the per-section Pitfalls
tables are merged into one at the end, de-duplicated, each row tagged with its source plan; a
new 1-screen "The contract" section at the top states the ten rules and links down.**

```
technical_docs/features/dependency-injection.md
 1  The contract (NEW, 058) — ten rules, one line each, each a link to its section
 2  Wiring contract                (046)  bootstrap(container) -> DIContainer; bind_instance; AlreadyBoundError; no locator
 3  Default bindings               (045)  @Fallback rule; tiers 0/100/200; in_memory escape hatch; FALLBACK_SHADOWED; P28 note
 4  Selecting a backend            (047)  VARCO_<SEAM>_BACKEND vocabulary; dotted path; precedence code > env-dotted > env > default; check_backend_selection
 5  Settings                       (048)  bind_settings recipe; per-field precedence; .env rule; DI-16 named instances; generated reference pointer
 6  Modules                        (049)  VarcoModule slots; tri-state enabled; depends_on; default_modules(); *Module vs *Configuration; EdgeSlot (15 members, R1); adapters
 7  Lifecycle phases               (050)  Phase table; components(); LifecycleRunner invariants; @Disposes rule; ownership; disposer=
 8  Multi-binding                  (051)  @Multibound seams; contribute/collect/acollect; ordering is never priority; empty-means table; plugin snippet
 9  Enabling a feature             (047)  the convention; on/off matrix; authorization_audit exception; tri-state kwargs (pointer to §6)
10  Generated references           (058)  di-catalogue.md · settings-reference.md · entry-points.md — what each answers; "never hand-edit; make <target>"
11  Diagnostics                    (052)  WiringReport sections; varco diagnose; startup validate() table; VARCO_DIAGNOSE_*; provenance vocabulary; findings table
12  Testing                        (052)  InMemoryProfile + in_memory_modules(); the di_container/di_overrides recipe; env you need in a test (R15); clear_backend_selection
13  Pitfalls (MERGED)              (all)  one table: symptom → cause → fix → plan
14  See also                       (058)  README "Wiring an app"; CLAUDE.md rules; the migration guide
```

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: one file, fixed order, merged Pitfalls, bodies untouched** | DI-14's literal ask ("one DI guide"); readers meet the ideas in dependency order (a contract before defaults, defaults before selection, modules before phases, phases before multi-binding); every plan's text survives verbatim so nothing decided is re-argued; one Pitfalls table is one place to grep a symptom; the "never restate" rule (CLAUDE.md §What this file is) is honoured — README keeps snippets, CLAUDE.md keeps rules | A ~1,200-line document. Accepted: mkdocs renders a TOC, and the "contract" section is the one-screen entry |
| (2) split into `di/` sub-pages (contract, defaults, modules, …) | Shorter pages | DI-14 says *one* guide; cross-references between eight pages are the "documented in five places" problem restated; CLAUDE.md/README pointers would need eight targets | Rejected |
| (3) keep the accumulated order (045 → 046 → … as appended) | Zero edits | Defaults before the wiring contract, features before modules, testing before lifecycle — the accretion order, not the reading order | Rejected |

**Pitfalls merge rule:** rows are keyed by *symptom*; two plans' rows with the same symptom merge
into one row citing both; a row whose fix a later plan changed (R4 `@PostConstruct`, R11 "list
`MigrationsModule` first", R13 `bind_instance` for streams) keeps only the winner's fix. Expected
size: ~40 rows (045: 4, 046: 5, 047: 6, 048: 5, 049: 5, 050: 5, 051: 8, 052: 6 → ≤ 44 minus
merges).

### §D-readme — one "Wiring an app" chapter, placed after Quickstart, TOC updated

049 wrote v1 ("Wiring an app" before the per-subsystem sections); 047 added "Choosing a backend"
/ "Enabling a feature" under the DI section; 048 "Settings and environment variables"; 051
"Contributing from your own package"; 052 "varco diagnose" / "Testing a varco app". Today's
README also has four older DI-shaped sections (`:709` "DI wiring" under Service Layer, `:1951`
SA "Bootstrap (one-liner setup)", `:3479` FastAPI "Bootstrap helpers", `:3714` "OtelConfig and DI
wiring").

**Chosen: one top-level `## Wiring an app` chapter immediately after `## Quickstart — Example
App`, with these subsections in this order, each ≤ 40 lines of snippet + prose, each ending with
a link to the guide section:** `The 8-line default app` · `Adding modules (the idiom
[*default_modules(), …])` · `Choosing a backend` · `Settings and environment variables` ·
`Enabling a feature` · `Writing your own module` · `Contributing from your own package` ·
`Lifecycle phases in one paragraph` · `varco diagnose` · `Testing a varco app`. The four older
sections become **pointers** (three lines each: "wiring moved to *Wiring an app* → link; what
stays here is the backend-specific part"), never a second copy. README = worked snippets;
`technical_docs` = design + reference; CLAUDE.md = rules + pointers (the split the task fixes).
The TOC (`README.md:81-209`) gains the chapter with its ten anchors as the **second** entry.

✅ A newcomer reads Quickstart, then "Wiring an app", and can build a real app before reading a
single subsystem section; ✅ every snippet is one that a test in the tree runs (049's
`test_create_varco_app_modules.py`, 052's `test_single_override_recipe.py` — the README says
which). ❌ The README grows ~250 lines net after the four old sections shrink; accepted.

### §D-entrypoints — `scripts/entry_points.py`: a sibling of `api_surface.py`, three outputs, one `--check`

**Chosen: a new script (not a flag on `api_surface.py`), copying `discover_packages()` verbatim
(the 047/048 precedent: execute `scripts/packages.sh`, RL-18), walking every module of every
package with `pkgutil.walk_packages` (048's `settings_reference.py` precedent), and classifying
every public wiring entry point into one of eight families.**

| Family | Selector (all: public name, `obj.__module__ == the module being walked`) |
|---|---|
| `bootstrap` / `async_bootstrap` | module-level function named exactly so |
| `bind` | function name `bind_*` |
| `enable` | function name `enable_*` |
| `mount` | function name `mount_*` |
| `install` | function name `install_*` |
| `module` | class, `issubclass(cls, varco_core.di.VarcoModule)`, not `VarcoModule` itself |
| `configuration` | class carrying providify's `@Configuration` marker (`providify.metadata._has_configuration_module` — the reader 049's `test_modules_never_scanned.py` already imports) |

Columns per row: `family` · `package` · `name` · `module` · `signature` (functions only, via
`inspect.signature`, `_ADDRESS_RE`-stripped — the same interpreter-stability rule as
`api_surface.py:194-198`; classes get **no** signature) · for `module` rows the three `ClassVar`s
`name` / `phase` (as `Phase.NAME` when it is a `Phase` member, else the int) / `depends_on`
(class names) · for `configuration` rows `depends_on` (from providify's `ConfigurationMetadata`).
Plain class attributes are interpreter-stable, unlike synthesised `__init__` signatures.

Exclusions live in one frozen table `_NOT_ENTRY_POINTS: Mapping[str, str]` (`"module:name" →
reason`) mirroring 048's `_EXCLUDED_SETTINGS` — seeded with the router-internal
`enable_subscription`/`install_*` methods 046 §Context excluded and the `varco_plugin_example`
testkit package (never packaged). A function or class the selectors match that is neither
listed nor excluded is **not** an error (additions are notes, like `api_surface.py`); a
`VarcoModule` subclass found by walking `VarcoModule.__subclasses__()` transitively that the walk
did *not* reach through a package module is an **error** (the completeness guard — a module
defined somewhere the packages do not import is a bug, not a note).

Outputs (all sorted, byte-stable):
1. `design/api-freeze-and-standards/measurements/entry-points.json` — what `--check` diffs
   (exact; no additive tier — 047/048's rule).
2. `technical_docs/entry-points.md` — header "generated by `make entry-points`, never
   hand-edited"; one table per family; in `mkdocs.yml` nav under a new **"Generated reference"**
   group beside 047's `di-catalogue.md` and 048's `settings-reference.md`.
3. The same tables written into `ARCHITECTURE.md` between `<!-- entry-points:begin -->` /
   `<!-- entry-points:end -->` markers inside the "DI Wiring" section (`:1560`), replacing the
   stale hand-written block (`:1562-1626`) — `--check` also diffs this block against the
   regenerated text, so ARCHITECTURE.md cannot drift.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: separate script, `walk_packages`, three outputs, marked block in ARCHITECTURE.md** | Same derivation as `api_surface.py` (DI-14's literal requirement) without overloading a gate whose scope CLAUDE.md documents precisely ("removals and function signature changes only"); `walk_packages` sees `<pkg>.di`/`<pkg>.modules`/`varco_core.webhook.module`/`varco_core.tls.module`/`varco_core.testing` — names that are not in a top-level `__all__` and would be invisible to an `api_surface.py` mode; ARCHITECTURE.md carries the table DI-14 asked for *and* it is generated; the site gets it too | A third copy of `discover_packages()` (047/048 each copied it; a shared `scripts/_packages.py` would be a refactor of three landed scripts — parked, Step 14); a generator writing into a hand-edited file needs markers (new pattern in this repo — the `--check` makes it safe) |
| (2) `api_surface.py --entry-points` | One script | `api_surface.py` walks `__all__` only; it would need a second walker and a second snapshot inside a gate whose CI step is named "api surface --check"; a failure would be ambiguous. Rejected |
| (3) hand-write the table once | Zero code | The BACKLOG row exists *because* the hand-written taxonomy table rotted into "a list of exceptions" (`BACKLOG.md:75`). Rejected outright |
| (4) `technical_docs/entry-points.md` only, ARCHITECTURE links to it | No markers | DI-14 says the ARCHITECTURE.md table is generated; a link is not a table. Rejected — but the page *also* exists for the site |

Wiring: `Makefile` targets `entry-points` (regenerate) / `entry-points-check` in the `api-check`
shape (`:176-178`), `entry-points-check` added to `lint`'s no-`PKG` block (`:163-167`, beside
047's `di-catalogue-check` and 048's `settings-check`), `make help` lines; `.github/workflows/test.yml`
one step after `api surface --check` (`:64-65`) — and, per premise (4), the missing
`export-asyncapi --check` step in the same job if Step 0 confirms it is absent.

### §D-claude — CLAUDE.md back to a routing table

After 045–057, §"DI wiring (providify)" carries: 045's three rule lines, 046's rewritten
six-row taxonomy table + three rule lines, 047's three rule lines + a Commands line + a decision-
tree branch + two bullet amendments, 048's four-line Settings pointer + a Commands line, 049's
"Modules" paragraph + a decision-tree branch, 050's "Lifecycle phases" paragraph + branch edits,
051's six touch points, 052's two rule lines + a Commands line + a branch. That is ~60 lines of
rules in one section — correct in content, no longer "readable in one screen" (CLAUDE.md's own
editing rule).

**Chosen:** (a) the six-row taxonomy table (046) is **replaced by a four-line pointer**: *"Every
wiring entry point — the ten `bootstrap()`s, every `bind_*`/`enable_*`/`mount_*`/`install_*`, every
`VarcoModule` and `@Configuration` — is listed with its signature in
`technical_docs/entry-points.md` (generated; `make entry-points`, gated by `make lint`). The
verb meanings: `dependency-injection.md` §Wiring contract. The AB-1/AB-3 name-collision notes
stay here"* (they change agent behaviour); (b) the rule lines from all plans are **kept**, each
reduced to one line + the guide anchor, grouped under five bold leads — *Contract · Defaults &
selection · Settings · Modules & lifecycle · Multi-binding · Diagnostics & testing*; (c) the
Commands section lists every snapshot target in one table (`api-check`, `asyncapi-check`,
`import-budget` (warn-only), `settings-check`, `di-catalogue-check`, `entry-points-check`) with
its regenerator and what moves it; (d) the decision tree keeps 047/049/050/051/052's branches
(they route); (e) §"Public API surface snapshot" gains one sentence: *"a deprecated name outside
`__all__` is invisible to `--check`; the floor is enforced by `test_deprecation_floor.py`, not by
this gate"*; (f) the stale claim in premise (4) is fixed to match the file. Nothing else — every
"why" lives in the guide.

✅ The section is one screen again; ✅ no rule is lost (Step 9's verify greps each plan's rule
sentence fragment); ❌ an agent must click through for the verb table — that is the point.

### §D-migration — `technical_docs/migrations/3-to-4.md`: every §Migration, verbatim then de-duplicated, in the order a user hits them

**Chosen: one file in the existing `technical_docs/migrations/` directory (nav group "Migration
Notes", `mkdocs.yml:145-146`), assembled from the thirteen §Migration sections.** Order — "what
you hit first when you run a 3.x app against 4.0":

```
0  Before you start           — pin providify>=2.1.0 (045); read the CHANGELOG [4.0.0] header; back up webhook subscriptions (056)
1  Your bootstrap no longer runs    — bootstrap(container) required & returns it; create_varco_container/setup_event_producer/get_container gone; bind_*/enable_* return the container, AlreadyBoundError (046)
2  create_varco_app takes modules   — container required; modules=[*default_modules(), …]; the 25-row kwarg → module table; the three *Configuration renames (049); LifecycleModule/MiddlewareModule adapters
3  Backends must be selected        — VARCO_<SEAM>_BACKEND per package (047's per-package table); unset means in-memory; the redis/redis-streams asymmetry
4  Environment variables renamed    — FASTREST_AUTHORIZATION__ → VARCO_AUTHORIZATION__; VARCO_PROFILER_ → VARCO_PROFILING_; TLS names; JWKS names (048); VARCO_LIFECYCLE_DISCOVERY_WARN gone (049)
5  Settings and defaults            — bind_settings; from_env(env) parameter gone; CORSConfig/ClientProfile pydantic; @Fallback replaces priority floors, get_all() excludes shadowed defaults (045/048)
6  Lifecycle and startup            — phases; Kafka/NATS bus start is explicit; validate() in the lifespan refuses on ERROR (050/052); the startup wiring report and VARCO_DIAGNOSE_*
7  Collections and consumers        — RetentionRegistry.register() gone → contribute(); @PostConstruct register_to → auto; posture collectors → classes; VARCO_TENANT_SOURCES open (051)
8  Security defaults that flipped   — one subsection per flip, each with: before · after · escape hatch · posture id (053 → 054 → 055 → 056 → 057, in that order — credentials, HTTP edge, tenancy, data layer, redaction)
9  Deprecations started or re-labelled — ApiKeyAuth(keys=) (053); chain=None implicit legacy (055); the five 3.0.0 windows → 5.0.0 (058); legacy env-name warnings (048)
10 What 5.0.0 will remove (the list) — the five AB-* names; the TrustStore shim; TenantProvisionConsumer(provisioner=); ApiKeyAuth(keys=); TenantResolutionMiddleware(chain=None) implicit fallback — each with its floor date
11 Verify: run `varco diagnose --modules … --check` (052) — what a clean report looks like; then start the app and read the SecurityPosture lines
```

Rules: (a) each plan's table is copied **first as-is** into a scratch file, then rows are merged
by *old shape* (e.g. `bootstrap()` appears in 045's and 046's tables; the row keeps 046's
replacement and cites both); (b) a row whose "New" a later plan superseded (R13) keeps only the
winner; (c) every row keeps a `(Plan NNN)` tag so the design of record is one click away; (d)
the flip section's subsections keep the three-column "Before / After / Action" shape 053–057
share; (e) no design prose — the guide has it; the migration guide answers "what do I change".
`CHANGELOG.md`'s `[4.0.0]` header links it; `README.md`'s "Changelog summary" section and the
"Wiring an app" chapter link it; `docs.yml` publishes it under `4.0`.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: one file, user-hits-first order, verbatim-then-dedup** | A 3.x maintainer follows it top to bottom and each step's failure mode is the one they would see next; the flip section keeps each escape hatch beside its posture id (054's severity table is linked, not copied) | Long (~400 lines). Accepted — the alternative is thirteen pages |
| (2) keep each plan's §Migration in the CHANGELOG only | No new file | The CHANGELOG is chronological by plan, not by what a user hits; 4.0's BREAKING blocks are already ~13 screens | Rejected |
| (3) `docs/migration/3-to-4.md` (the `docs/` guides tree) | Beside the other how-tos | `docs/` is published only through `gen_guide_pages.py`'s literal tuple; `technical_docs/migrations/` already exists with 009's note and is in the nav natively | Rejected |

### §D-changelog — the `[4.0.0]` section: fixed order, per-plan BREAKING blocks reordered, everything else merged

**Chosen:** the `[Unreleased]` content (043/044's two blocks + every plan's blocks) becomes
`## [4.0.0] — <date>` with this **fixed section order**, and the `[Unreleased]` header is kept
above it, empty:

```
## [4.0.0] — YYYY-MM-DD
> **Migration guide:** technical_docs/migrations/3-to-4.md — read it before upgrading.
> Requires providify >= 2.1.0. Twelve months before any deprecated symbol is removed (CONTRIBUTING.md §Removal window); nothing deprecated in 3.x is removed here.

### BREAKING — one wiring contract … (Plan 046, DI-2/DI-8/DI-13)          ← blocks kept verbatim,
### BREAKING — VarcoModule and create_varco_app(modules=…) (Plan 049, …)    reordered to the
### BREAKING — config-driven backend selection … (Plan 047, …)              migration guide's
### BREAKING — settings standardization … (Plan 048, …)                     order (§D-migration 1→9),
### BREAKING — @Fallback framework defaults, providify >= 2.1.0 (Plan 045)  never merged
### BREAKING — phased lifecycle, @Disposes reachability, DI-wired infrastructure (Plan 050)
### BREAKING — runtime validate() in the lifespan (Plan 052)
### BREAKING — multi-binding for collection seams (Plan 051)
### BREAKING — credential and authorization default flips (Plan 053)
### BREAKING — HTTP-edge and admin-surface default flips (Plan 054)
### BREAKING — tenant-provenance default flips (Plan 055)
### BREAKING — data-layer default flips (Plan 056)
### BREAKING — redaction flips (Plan 057)
### Added          ← merged; every bullet keeps its "(Plan NNN)" tag; grouped by package
### Changed        ← merged
### Deprecated     ← started (ApiKeyAuth(keys=) 053; chain=None 055) + re-labelled (the five 3.0.0 windows, the TrustStore shim, TenantProvisionConsumer — 058) — each with its since/removed_in/floor date
### Removed        ← merged: setup_varco_defaults, setup_defaults=, create_varco_container, setup_event_producer, get_container(), streams=/setup_cache=/setup_producer=/*packages/*entity_classes, bind_retention_registry + retention/di.py, RetentionRegistry.register(), _LEGAL_TENANT_SOURCES, _default_collectors/_collect_*, enable_tenant_context/tenant_field/_maybe_tenant_context, VARCO_LIFECYCLE_DISCOVERY_WARN, RedisScanNullTokenRevocationStoreDefault, the 16 providify guards, the three *Module→*Configuration names, @PostConstruct on Kafka/NATS start(), the 045 priority floors
### Fixed          ← merged (050's lifespan:253 rollback order, AuditConsumer never stopped, OTel providers never flushed, Redis client leaks; 056's data.role_bypasses_rls over-report; 057's word-mode tokeniser; …)
### Security       ← one paragraph pointing at the flip blocks + the severity table
### Upstream       ← closed: P24/P25/P26/P27 (providify 2.1.0, Plan 045); filed: P28 (045 or 052, iff measured), P29 (046), P-DISP/P-INJ (050, real numbers from the ledger) — each: report path + guard test id
### Documentation  ← the DI guide, the migration guide, entry-points, generated references, CLAUDE.md polish (058); FLIP-14 design (055)
```

Ordering rule (recorded in the file's top comment so 5.0.0 follows it): **BREAKING blocks first,
in migration-guide order, never merged (each names its plan and rows); then the Keep-a-Changelog
sections in the fixed order Added → Changed → Deprecated → Removed → Fixed → Security → Upstream
→ Documentation; inside a merged section, bullets are grouped by package and keep their plan tag.**
✅ A reader upgrading reads the BREAKING blocks in the order they will hit them; a reader
auditing a package finds it under one heading; ✅ `Upstream` is where the P-ledger's "Recently
closed" evidence lives after the ledger is cleared (CLAUDE.md's rule). ❌ Two conventions in one
file (3.x sections were per-plan throughout) — accepted; the top comment says 4.0.0 introduced the
merged shape.

### §D-backlog — the rollup

- Every `DI-*` and `FLIP-*` row in the Live table (`BACKLOG.md:65-101`) is **deleted** and the
  `## ✅ Shipped` list gains one bullet per plan in the "Most recent ten individually" register
  (`:142-151`) — thirteen bullets, older ones rolled into "4.0 — DI standardization + the flip
  list: 27 rows shipped (plans/045–058)". `FLIP-14` → the Parked section: *"provisioning-authority
  model for `mount_tenant_admin` — **designed** (Plan 055 §D-flip14, recorded in
  `admin-surface-tenancy.md`) · ⏵ its own plan; trigger: a second control-plane consumer, or an
  IdP that can mint a provisioning profile"*.
- `RO-2`, `RO-4`, `RO-5` → Shipped bullets (inside 052, 049, 057). `RO-1`, `RO-3`, `RO-6`–`RO-13`
  stay live, unchanged; 050's new `RO-*` row ("auto-registered consumers get no `dlq=`") stays
  live.
- "Open questions (this cycle)" (`:123-138`) → moved into **Answered decisions** as three bullets:
  **4.0 OQ1** (Plan 049 §D-shape: a varco ABC, never a `@Configuration` — scan activation and
  container-only construction); **4.0 OQ2** (Plan 047 §D-mechanism: providify 2.1.0 ships
  `@Requires(env=, value=)`; used directly, the P25 profile translation is superseded); **4.0 OQ3**
  (this plan §D-floor: yes — every 3.x window naming 4.0.0 is re-labelled 5.0.0; the floor is
  computed from CHANGELOG release dates and enforced by `test_deprecation_floor.py`; the
  `api_surface` gate is structurally unaffected because deprecated names are outside `__all__`).
- Parked (this cycle) gains: the `Literal[True]` sweep of the four `acknowledge_*` sites
  (⏵ a fifth acknowledgement kwarg is added, or mypy `strict` flags a `bool` site); a shared
  `scripts/_packages.py` for the four `discover_packages()` copies (⏵ a fifth script); redacting
  list-valued leaves in `redact_mapping()` (057 — ⏵ a DTO dump with a nested list of secrets
  reported); `register_webhook_signer()` (⏵ an out-of-tree signing scheme); deprecating
  `varco_fastapi.posture.PostureFinding` (⏵ 5.0.0 planning); an HTTP endpoint for the wiring
  report (052's trigger); per-phase stop timeout (050's trigger). The 4.0-horizon durable-
  execution note (`:195`) is left as is.
- The header (`:3-9`) → "No cycle is live. 4.0 (DI standardization) shipped as `v4.0.0` on
  <date>." The cycle section (`:16-58`) is compressed to its "Locked decisions" table (kept —
  they are answered decisions) and a one-line pointer to `plans/000`.

### §D-upstream — the ledger audit

At plan time `UPSTREAM-GAPS.md` lists P24–P27 open. After 045–052: P24–P27 closed (four
"Recently closed" bullets, 045 §D-closure); P28 filed **iff** 045 Step 1(e)/052 Step 1(c)
measured the scanned-class `FALLBACK_SHADOWED` blind spot as real; P29 filed by 046
(`BindingDescriptor.fallback`); two more filed by 050 (`provide(disposer=)`, provider `inject=`
override — numbered from the ledger). This plan **verifies**, for every open row: (a) the report
file exists and has the eight sections; (b) the guard test exists, is `strict=True`, names the row
in `reason=`, and is currently `xfailed` (never `xpassed`) in `make test`; (c) the CHANGELOG
`Upstream` block names it with the same id. Then the ledger's "Recently closed" bullets are
**moved** into the CHANGELOG `Upstream` block and deleted from the ledger (the clearing the
ledger's own header describes — the report files keep the evidence). `rg -n 'GAP: P' varco_*/tests/`
must equal the open-row count exactly. No new ask is filed by this plan.

### §D-release — `v4.0.0`, the runbook shape, with a rehearsal

**Chosen: the 3.0.0 runbook sequence (`release-runbook.md` §5–§6) reused verbatim with the
version substituted, plus one rehearsal tag.** Order: all gates green on the release branch →
`uv run python scripts/bump.py --set 4.0.0 --dry-run` (review the ten diffs: version +
`~=4.0` sibling pins) → `--set 4.0.0` (runs `uv lock`) → CHANGELOG date → one commit "Release
4.0.0" → PR → `all-green` → merge → `git tag -s v4.0.0rc1` (rehearsal: `release.yml` ten legs
publish pre-releases with attestations; `docs.yml`'s `release` job is **skipped** by the guard
regex at `docs.yml:98` — a pre-release never moves `latest`) → fix-and-`rc2` if any leg
fails, **never** patch on the final tag → `git tag -s v4.0.0` → verify: ten PyPI pages at 4.0.0,
`pip install "varco-fastapi==4.0.0"` resolves `varco-core~=4.0`, docs site serves `4.0` +
`latest` + `dev` (mike: `deploy --update-aliases 4.0 latest`, `docs.yml:135`), `git tag -v`.

- ✅ `bump.py` is the *only* version writer (`CONTRIBUTING.md:79-81`); its `--check` is a live
  unit test, so a half-bumped tree cannot pass `make test`. ✅ The rc rehearsal is the runbook's
  own de-risking step (§5: "Never patch during the real release tag"). ✅ mike's `<major>.<minor>`
  scheme means `4.0` is a new version entry; `3.2` stays browsable (brief 006 §5).
- ❌ A pre-release on PyPI is permanent; accepted (3.0.0 did the same). ❌ `--set 4.0.0` moves ten
  files + `uv.lock` in one commit — that is the lockstep contract.
- Rejected — **`--bump major`**: identical result; `--set` is explicit and matches the runbook's
  wording. Rejected — **skip the rc**: the release touches every package's public surface; the
  runbook's reason for the rc has never been stronger.

**Runbook update:** a new `## 7. Release: v4.0.0` section in `release-runbook.md` recording the
outcome (the runbook is the durable record of operator steps — CLAUDE.md §CI), and the
"Version naming" comment in `docs.yml` gains "4.0 — 2026-09-xx" as a worked example.

### §D-removals-5 — the 5.0.0 removal list is written down now, with dates

The migration guide's §10 and `BACKLOG.md`'s Parked section carry the list, so the 5.0.0 planner
inherits it rather than rediscovering it:

| Symbol | `since` | Earliest removal (floor) |
|---|---|---|
| `varco_sa.rls.enable_rls_ddl` (→ `render_rls_ddl`) | 3.0.0 | 2027-08-31 |
| `varco_core.migration.MigrationError` / `.MigrationPlan` (→ `Schema*`) | 3.0.0 | 2027-08-31 |
| `varco_beanie.BeanieConfig` / `varco_beanie.bootstrap.BeanieConfig` (→ `BeanieSettings`) | 3.0.0 | 2027-08-31 |
| `TenantProvisionConsumer(provisioner=, catalog=)` (→ `control_service=`) | 3.0.0 | 2027-08-31 |
| `varco_fastapi.auth.TrustStore` (→ `varco_core.tls.TrustStore`) | 3.1.0 | 2027-09-05 |
| `TenantResolutionMiddleware(chain=None)` implicit `LegacyTenantSource` fallback (→ explicit chain / `VARCO_TENANT_SOURCES`) — the `TypeError` 055 deferred | 3.2.0 | 2027-09-12 |
| `ApiKeyAuth(keys=)` (→ `hashed_keys=`) | 4.0.0 | 4.0.0's date + 12 months |
| 048's legacy env-name warnings (`FASTREST_AUTHORIZATION__`, `VARCO_PROFILER_`, `VARCO_CA_CERT`…, `VARCO_JWKS_*`) | 4.0.0 | same |

## Steps

All commands from `/home/edoardo/projects/varco`. Step 0 first. Phase A (1–3), B (4–6) and C
(7–12) are independent of each other after Step 0; D (13–15) needs A–C; E (16–17) needs D; F
(18–22) is the release and is strictly sequential. Every doc step's **Verify** is an `rg` that
proves a stale claim did not survive.

### Phase 0 — Preflight

0. [x] **Prove the thirteen plans landed; record the tree.** Write
       `/tmp/claude-1000/…/scratchpad/058-preflight.txt` with, in order:
       (a) providify: `uv run python -c "import importlib.metadata as m; print(m.version('providify'))"` → `2.1.0`.
       (b) One Done-criterion grep per plan, all green: 045 `rg -n 'sys\.maxsize|2\*\*31' varco_*/varco_*` → 0 and `rg -c '@Fallback' varco_*/varco_*` ≥ 55; 046 `rg -n 'def bootstrap\(container: DIContainer\) -> DIContainer' varco_*/varco_*/di.py varco_core/varco_core/di/__init__.py | wc -l` → 10 and `rg -n 'DIContainer\.current\(' varco_*/varco_*` → 0; 047 `ls scripts/di_catalogue.py technical_docs/di-catalogue.md` and `rg -n 'selected_by\(' varco_*/varco_* | wc -l` ≥ 20; 048 `ls scripts/settings_reference.py technical_docs/settings-reference.md varco_core/varco_core/settings/bind.py` and `rg -n 'FASTREST_|VARCO_PROFILER_' --type py` → 0; 049 `rg -n 'class VarcoModule' varco_core/varco_core/di/module.py` and `rg -n 'extra_lifespan_components|enable_logging=' varco_*/varco_* README.md` → 0; 050 `rg -n 'class LifecycleRunner' varco_core/varco_core/di/lifecycle.py` and `rg -n '@PostConstruct' varco_kafka/varco_kafka/bus.py varco_nats/varco_nats/bus.py` → 0; 051 `rg -c '@Multibound' varco_*/varco_*` → 5 files and `rg -n 'bind_retention_registry' varco_*/varco_*` → 0; 052 `ls varco_core/varco_core/di/report.py varco_core/varco_core/di/diagnose.py varco_core/varco_core/cli/diagnose.py varco_core/varco_core/testing.py` and `rg -n 'def warn_deprecated' varco_core/varco_core/deprecation.py`; 053 `rg -n 'Requires\(env="VARCO_SECURITY_ENV"' varco_core/varco_core/auth/authorizer.py`; 054 `rg -n 'effective_enforce' varco_fastapi/varco_fastapi/posture.py`; 055 `rg -n 'TENANT_RESOLUTION' varco_fastapi/varco_fastapi/middleware/slots.py` → `= 95` and `rg -n 'removed_in="5\.0\.0"' varco_fastapi/varco_fastapi/middleware/tenant_resolution.py` → 1; 056 `rg -n 'class PlaintextFieldEncryptor' varco_core/varco_core/encryption.py`; 057 `rg -n 'AUDIT_REDACTION_POLICY' varco_core/varco_core/redaction/`. **Any red → stop; that plan is not landed.**
       (c) The deprecation inventory re-run: `rg -n 'removed_in="4\.0\.0"' varco_*/varco_*` (expect 9), `rg -n 'warnings\.warn' varco_*/varco_* | rg -v 'deprecation\.py'` (expect the two ad-hoc sites — `trust_store.py`, `control/consumer.py` — plus any *non-deprecation* `warnings.warn` sites, listed with a one-word category), `rg -n 'removed in 4\.0|until 4\.0\.0|in 4\.0\.0' CLAUDE.md README.md ARCHITECTURE.md CONTRIBUTING.md technical_docs docs varco_*/varco_*` (the prose list). Confirm `since` for the `TrustStore` shim from `CHANGELOG.md`'s `[3.1.0] ### Deprecated` block (`:697`).
       (d) `CHANGELOG.md`: list every `###` heading under `## [Unreleased]` with its plan tag (expect ≥ 15 blocks; 043/044's two at the bottom); confirm the three release headings `:49,425,823` still parse as `## [X.Y.Z] — YYYY-MM-DD`.
       (e) `UPSTREAM-GAPS.md` open rows and their ids (P28? P29? the two 050 numbers); `rg -n 'GAP: P' varco_*/tests/` count; `uv run pytest varco_core/tests/test_providify_contracts.py -q -rxX` → `N xfailed, 0 xpassed`, N = open rows.
       (f) `technical_docs/features/dependency-injection.md`: the `##` heading list in current (accumulated) order; the 045 banner line number; the count of `Pitfalls` tables.
       (g) `README.md`: `rg -n '^## Wiring an app|^### (Choosing a backend|Enabling a feature|Settings and environment variables|Contributing from your own package|varco diagnose|Testing a varco app|DI wiring|Bootstrap helpers|Bootstrap \(one-liner setup\)|OtelConfig and DI wiring)' README.md` — the sections to consolidate; the TOC's current DI entries.
       (h) `CLAUDE.md` §"DI wiring (providify)": line span and line count (the "one screen" baseline); the taxonomy table's row count.
       (i) `Makefile:163-167`'s lint block and `.github/workflows/test.yml`'s `lint` steps as they are after 047/048 — specifically whether `export-asyncapi --check` is present (premise (4)).
       (j) `mkdocs.yml` nav: whether `features/dependency-injection.md`, `di-catalogue.md`, `settings-reference.md` are listed; the "Migration Notes" group.
       (k) `ls varco_core/tests/test_bump_script.py` and `uv run python scripts/bump.py --check` → coherent at 3.2.0.
       **Verify**: the file has (a)–(k); every ⚠️ ASSUMPTION in §Risks has a measured value.

### Phase A — OQ3: the deprecation floor

1. [x] `varco_core/tests/test_deprecation_floor.py` (**new, failing first**) — per §D-floor:
       `parse_release_dates(changelog_text) -> dict[str, date]` (the `## [X.Y.Z] — YYYY-MM-DD`
       regex; `[Unreleased]` skipped), `find_deprecation_sites(root) -> list[Site(path, line,
       since, removed_in)]` (one regex over each `varco_*/varco_*/**/*.py` matching
       `since="(\d+\.\d+\.\d+)"` … `removed_in="(\d+\.\d+\.\d+)"` within one call — multi-line, the
       keyword order either way), `pending_version(root)` (read `varco_core/pyproject.toml`'s
       `[project] version` — the same field `scripts/bump.py:272` reads), `floor_violations(...)`
       (rules 1–4). Tests: (a) the three known headings parse to the three known dates; (b)
       synthetic sites: `since="3.0.0", removed_in="4.0.0"` with `4.0.0` pending and today <
       2027-08-31 → one violation naming the site; the same with `removed_in="5.0.0"` → none;
       `removed_in="4.1.0"` → violation ("not a major"); `removed_in="3.0.0"` → violation;
       `since="9.9.9"` (no heading, not pending) → violation ("unknown release"); `since=` equal
       to the pending version → allowed; Feb-29 `since` date → floor Mar 1; (c) **the tree**:
       `floor_violations(find_deprecation_sites(REPO_ROOT), parse_release_dates(...), pending)`
       is empty — **red today** (nine `4.0.0` labels; the message lists all nine with
       `file:line`); (d) `len(find_deprecation_sites(REPO_ROOT)) >= 9` (a regex that silently
       matches nothing is not a guard). Docstring: the invariant, the literal-only limitation,
       and the OQ3 pointer. **Verify**: `uv run pytest varco_core/tests/test_deprecation_floor.py -q`
       → (a)(b)(d) green, (c) red with nine sites named.

2. [x] **Re-label + convert.** (i) `varco_sa/varco_sa/rls.py:259` → `removed_in="5.0.0"`, comment
       `:256` → "Removed in 5.0.0 (deprecated since 3.0.0; the 12-month floor put 4.0.0 out of
       reach — CONTRIBUTING.md §Removal window)"; (ii) `varco_core/varco_core/migration/__init__.py:66,71`
       + docstring `:16`; (iii) `varco_beanie/varco_beanie/__init__.py:133` + comment `:128`;
       (iv) `varco_beanie/varco_beanie/bootstrap.py:177` + comment `:172`; (v)
       `varco_core/varco_core/deprecation.py:148,285,292,295` (docstring examples → `5.0.0`; add
       one sentence under `removed_in:` in both `Args:` blocks: *"must respect the 12-month floor
       from the `since` release's CHANGELOG date — `test_deprecation_floor.py` enforces it"*);
       (vi) `varco_fastapi/varco_fastapi/auth/trust_store.py:128-135` → `warn_deprecated(
       "varco_fastapi.auth.TrustStore", since="3.1.0", removed_in="5.0.0",
       replacement="varco_core.tls.TrustStore (recursive, wider cert globs, any backend — see
       technical_docs/features/tls-trust-and-hot-reload.md)", stacklevel=3)`; drop `import
       warnings` if unused; module docstring `:5` → "removed in 5.0.0"; (vii)
       `varco_core/varco_core/tenancy/control/consumer.py:183-190` → `warn_deprecated(
       "TenantProvisionConsumer(provisioner=..., catalog=...)", since="3.0.0", removed_in="5.0.0",
       replacement="control_service=TenantControlService(catalog=..., provisioner=...,
       producer=...)", stacklevel=2)`; the `:173-182` comment: replace "NOT migrated to
       @deprecated … applied by hand" with "argument-conditional, so `@deprecated` (whole-callable)
       does not fit; `warn_deprecated()` is the parameter-path helper (Plan 053) and carries the
       keywords the floor test reads". Tests: every existing test that matches the old message
       text (`rg -n 'removed in 4\.0\.0|will be removed in 4' varco_*/tests`) → `5.0.0`;
       `varco_fastapi/tests` for the `TrustStore` shim: `pytest.warns(DeprecationWarning,
       match=r"deprecated since 3\.1\.0 and will be removed in 5\.0\.0")`; the
       `TenantProvisionConsumer` test likewise with `3.0.0`; add a test per converted site that
       the record appears in 052's `fired_deprecations()` with `since`/`removed_in` populated.
       **Verify**: Step 1(c) green; `rg -n 'removed_in="4\.0\.0"' varco_*` → 0;
       `rg -n 'removed_in="5\.0\.0"' varco_*/varco_*` → 8 (five sites + `trust_store` + `consumer`
       + 055's `tenant_resolution`; + 053's `ApiKeyAuth` = 9); `rg -n 'warnings\.warn\(' varco_*/varco_*
       | rg -v deprecation.py | rg -i deprecat` → 0; `make test PKG=varco_core`, `PKG=varco_sa`,
       `PKG=varco_beanie`, `PKG=varco_fastapi` green.

3. [x] **Prose + the `api_surface` proof.** `CONTRIBUTING.md:112-118` → "Plan 022 shipped four
       worked examples, all `since="3.0.0"`; **re-labelled `removed_in="5.0.0"` in 4.0.0** — the
       12-month floor (2027-08-31) fell after 4.0.0's date, which is the case the floor exists
       for: a major bump is necessary, never sufficient. `test_deprecation_floor.py` computes the
       floor from this file's policy and CHANGELOG's release dates"; `ARCHITECTURE.md:307,1144`,
       `CLAUDE.md:397,1048`, `technical_docs/features/postgres-rls.md:65`,
       `tls-trust-and-hot-reload.md:280`, `schema-migrations.md` (the rename story) → `5.0.0`
       with a parenthetical "(floor: 2027-08-31)". Then: `uv run python scripts/api_surface.py --check`
       → **no change** (premise (3)); paste the output into the preflight file as the OQ3 proof.
       **Verify**: `rg -n 'removed in 4\.0|until 4\.0\.0|in 4\.0\.0' CLAUDE.md README.md ARCHITECTURE.md CONTRIBUTING.md technical_docs docs varco_*/varco_*`
       → only `CHANGELOG.md`-style historical mentions (none in these files); `api_surface --check` exit 0.

### Phase B — the generated entry-point table

4. [x] `varco_core/tests/test_entry_points_script.py` (**new, failing first**) — runs the
       script's functions in-process against `varco_core` only (048/047's script-test shape):
       (a) every `bootstrap`/`bind_*`/`enable_*` module-level function in `varco_core.di`,
       `varco_core.tls.di`, `varco_core.flags.di`, `varco_core.tenancy.di`, `varco_core.revocation.di`,
       `varco_core.auth.di`, `varco_core.event.cloudevents` appears with its family and a
       signature; (b) `CoreModule`, `ObservabilityModule`, `LifecycleModule`, `WebhookModule`,
       `TrustStoreModule`, `InMemoryProfile` appear as `module` rows with `name`/`phase`/
       `depends_on`; `VarcoModule` itself does not; (c) `OtelConfiguration` appears as a
       `configuration` row; (d) a `VarcoModule` subclass defined in a test-local module that the
       walk cannot reach → `EntryPointsError` naming it (the completeness guard); (e) output is
       deterministic across two runs and sorted; (f) `--check` exit codes: 0 on match, 1 on a
       diff naming the family/name, 2 on a missing snapshot; (g) the ARCHITECTURE.md marker
       block: `render_block()` output equals what `--check` compares; a file with no markers →
       error naming the markers; (h) `_NOT_ENTRY_POINTS` keys all resolve (a stale exclusion is an
       error, as 048's exclusion test). Then `scripts/entry_points.py` (**new**) per
       §D-entrypoints: `discover_packages()` copied verbatim from `api_surface.py:96-139` (same
       DESIGN block), `walk_entry_points(packages)`, `classify()`, `render_markdown()`,
       `render_block()`, `write_snapshot()`, `check()`, `main()` with `--check`, `--snapshot PATH`,
       `--markdown PATH`, `--architecture PATH`, `--packages PKG…`. Full docstrings + a `DESIGN:`
       block with §D-entrypoints' four-row table. **Verify**: test green; `uv run python
       scripts/entry_points.py --packages varco_core` prints ≥ 12 rows.

5. [x] **Generate + wire.** `uv run python scripts/entry_points.py` → commit
       `design/api-freeze-and-standards/measurements/entry-points.json`,
       `technical_docs/entry-points.md`, and the marked block in `ARCHITECTURE.md` (replace
       `:1562-1626`'s hand-written code block and the four "Corrected DI call shapes" bullets —
       all four are 3.x claims: `install(SAModule)`, "first one registered", "override must run
       before install()" — with the markers; keep the quoted-annotation landmine paragraph
       `:1595-1625`, it is still true and not DI-wiring). `Makefile`: `entry-points` /
       `entry-points-check` targets in the `api-check` shape (`:176-178`), `entry-points-check`
       in `lint`'s `ifeq` block (`:163-167`), two `make help` lines (`:102-106`).
       `.github/workflows/test.yml`: one step `entry points --check` after `api surface --check`
       (`:64-65`); **iff** Step 0(i) found no `export-asyncapi --check` step, add it in the same
       place (`uv run varco export-asyncapi $ASYNCAPI_ARGS --check` — copy the args from
       `Makefile:216-219`) so CLAUDE.md's claim is true. `mkdocs.yml` nav: a new group
       `- Generated reference:` with `DI catalogue: di-catalogue.md`, `Settings reference:
       settings-reference.md`, `Entry points: entry-points.md` (moving 047/048's entries if they
       were placed elsewhere), and `- Dependency Injection: features/dependency-injection.md`
       under Features (first) iff Step 0(j) found it absent. **Verify**: `uv run python
       scripts/entry_points.py --check` → 0; `make lint` runs it and `make lint PKG=varco_core`
       does not; `make docs-strict` (the nav/page pair cannot drift — `gen_guide_pages.py:33-34`'s
       rule).

6. [x] **Counts that the table must reproduce (the DI-14 acceptance):** the generated table
       lists exactly ten `bootstrap` rows (one per package), two `async_bootstrap` (`varco_redis`,
       `varco_memcached`), every `bind_*` 046 §Context enumerated (rows 9, 10, 13, 16, 18, 19,
       25–30 + `bind_instance`, `bind_settings`, `bind_core_settings`, `bind_env_overrides`,
       `bind_trust_store`, `bind_cloudevents_serializer`, `bind_repositories` ×2, `bind_clients`,
       `bind_clients_from`, `bind_peers`, `bind_websocket_adapter`, `bind_sse_adapter`,
       `bind_mcp_adapter`, `bind_skill_adapter`), the six `enable_*`, the three `mount_*`, the
       `install_*` family, every `*Module` 049/050/052 named (`default_modules()`'s eleven, the
       ten package modules, `MigrationsModule`/`TenancyModule`/`ReliabilityModule`/`RetentionModule`/
       `JwksRefreshModule`/`SecurityPostureModule`/`LifecycleModule`/`MiddlewareModule`/
       `WebhookModule`/`TrustStoreModule`/`InMemoryProfile`), and every `*Configuration`. Add
       `test_entry_points_snapshot_covers_the_taxonomy` to Step 4's file asserting these names
       are present in the committed snapshot (a literal list — a removal is a visible diff).
       **Verify**: green.

### Phase C — the documents

7. [x] **`technical_docs/features/dependency-injection.md` — restructure per §D-guide.** (a)
       Delete the 045 banner; (b) reorder the `##` sections to the fourteen-entry order (bodies
       moved, not edited — `git diff --stat` on this file should show mostly moved hunks); (c)
       write §1 "The contract" (ten rules, each one sentence + anchor: container required &
       returned · every varco binding of a varco seam is `@Fallback`, a user binding always wins ·
       a backend is live only when `VARCO_<SEAM>_BACKEND` names it · settings through
       `bind_settings()`, code > env > default per field · a module is a plain `VarcoModule`,
       never scanned · lifecycle is phased, stop is the exact reverse · a collection seam is
       `@Multibound`, contributions are never `@Fallback` and never carry `priority` · a feature is
       `enable_*()` or `VARCO_<FEATURE>_ENABLED`, code beats env · `validate()` runs in the
       lifespan and an ERROR refuses startup · tests swap one binding with providify's
       `di_overrides` on `in_memory_modules()`); (d) write §10 "Generated references" (three
       pages, what each answers, the `make` target, "never hand-edit"); (e) merge every Pitfalls
       table into §13 (the merge rule; each row tagged); (f) write §14 "See also"; (g) apply the
       reconciliation winners wherever a moved body still says the loser's shape: R1 (`EdgeSlot`
       has `TENANT_RESOLUTION=95`), R4 (no `@PostConstruct` for `register_to`), R7/R8 (the
       collector-ownership map as one table in §Multi-binding), R11 (no "list `MigrationsModule`
       first"), R13 (`bind_settings(instance=)` for streams), R17 (`varco_core.job.memory`), R18
       (`inspect_bindings()` is the private read). **Verify**: `rg -n 'Plan 058 completes|058 completes|banner' technical_docs/features/dependency-injection.md`
       → 0; `rg -c '^## ' …` → 14; `rg -c '^## Pitfalls|^### Pitfalls' …` → 1;
       `rg -n 'PostConstruct' …` → only the "do not" sentence; `rg -n 'list it before|before \*default_modules' …`
       → 0; `rg -n 'bind_instance\(container, RedisEventBusSettings' …` → 0; `make docs-strict`.

8. [x] **`README.md` — the "Wiring an app" chapter (§D-readme).** Move 049/047/048/051/052's
       subsections under one `## Wiring an app` after `## Quickstart — Example App` (`:50-80`),
       in §D-readme's order, trimming each to its snippet + ≤ 6 lines of prose + a guide link;
       reduce `:709` "DI wiring", `:1951` SA "Bootstrap (one-liner setup)", `:3479` "Bootstrap
       helpers", `:3714` "OtelConfig and DI wiring" to pointer paragraphs (the backend-specific
       remainder stays: `SAModule(config=…, entities=…)`, `BeanieModule(...)`, `OtelConfig`
       fields). Add the "env you need in a test" box (R15) to "Testing a varco app". TOC
       (`:81-209`): insert the chapter as the second entry with its ten anchors; remove the
       sub-entries that moved. "Changelog summary" (`:4968`) gains a 4.0.0 paragraph linking the
       migration guide. **Verify**: `rg -n '^## Wiring an app' README.md` → 1 (line < 100);
       `rg -c '^### ' README.md` unchanged ± 10; every snippet in the chapter names its
       backing test in a trailing comment (`rg -n '# tested by' README.md` ≥ 8); `rg -n
       'setup_varco_defaults|create_varco_container|setup_event_producer|-sys\.maxsize|DIContainer\.current\(\)|priority floor|eleven hand-passed|extra_lifespan_components|enable_tenant_context|bind_retention_registry|registry\.register\(|_redis_active_bus|streams=True|setup_producer' README.md`
       → 0; `python - <<'EOF'` link check: every `(#anchor)` in the TOC resolves to a heading
       (a 15-line script in the preflight file).

9. [x] **`CLAUDE.md` — §D-claude.** (a) §"DI wiring (providify)": replace the "Typical app
       bootstrap" block with the 8-line default app (049 already did — confirm), replace the
       six-row taxonomy table with the four-line pointer, regroup every plan's rule lines under
       the five bold leads (one line + anchor each; keep the AB-1/AB-3 bullets); (b) §Commands:
       one table of the six snapshot gates (target · regenerator · what moves it · CI step) and
       the `varco diagnose --check` one-liner (052); (c) §"Public API surface snapshot": the
       floor sentence (§D-claude (e)); (d) §"AsyncAPI snapshot gate": fix premise (4)'s claim to
       match `test.yml` after Step 5; (e) §Test Conventions: "nine conformance modules" (046) and
       the `di_wiring` sentence mention `module_cls`/`selectable`/`module_owned`/
       `known_warnings == frozenset()` (049/047/050); the `@PostConstruct` bullet in the
       Pre-Implementation checklist → 051's rule (confirm 051 did it); (f) §CloudEvents ⚠️
       paragraph — confirm 050 rewrote it; (g) the "Common Pitfalls" pointer paragraph mentions
       the merged DI Pitfalls table; (h) the header "Quick reference" gains
       `technical_docs/features/dependency-injection.md` and `technical_docs/migrations/3-to-4.md`.
       Nothing else — no restated "why". **Verify**: the §"DI wiring (providify)" span is ≤ 60
       lines (Step 0(h) baseline − the table); `rg -n '\| `bootstrap` \|' CLAUDE.md` → 0 (the table
       is gone); `rg -n 'setup_varco_defaults|-sys\.maxsize|DIContainer\.current\(\)|priority floor|eleven hand-passed|typically in a `@PostConstruct`|must be wired from `@PostConstruct`|hand-constructed, not DI-wired|pass `serializer=` explicitly|eight conformance modules|rides in `test\.yml`' CLAUDE.md`
       → 0 (the last only if Step 5 added the step; otherwise the sentence is rewritten to "runs
       in `make lint` only"); every rule sentence fragment from 045–052's CLAUDE.md steps still
       greps (`@Fallback`, `AlreadyBoundError`, `selected_by`, `bind_settings`, `never a
       `@Configuration``, `exact reverse`, `@Multibound`, `di_overrides`) → ≥ 1 each.

10. [x] **`ARCHITECTURE.md` polish.** (a) The "DI Wiring" section (`:1560`) = one paragraph +
        the marked generated block (Step 5) + the landmine paragraph; (b) "Package Overview"
        (`:7-173`): `varco_core.di` (package: `__init__`, `seams`, `select`, `features`,
        `introspect`, `module`, `lifecycle`, `collect`, `report`, `diagnose`), `varco_core.settings`,
        `varco_core.posture`, `varco_core.testing`, `<pkg>/modules.py` per package, `varco_fastapi.modules`,
        `varco_fastapi.middleware.slots`, `varco_fastapi.diagnose`, `varco_fastapi.testing` — confirm
        each plan added its entry, add any missing; (c) the Modules table (049 + 050's `phase`
        column + 055's `TenancyModule` shape + 050's `WebhookModule`/`TrustStoreModule` + 052's
        `InMemoryProfile`) is **replaced by a pointer** to the generated `module` family table —
        one table, generated, never two; (d) the `EdgeSlot` list shows 15 members (R1); (e) the
        collector-ownership map (R8) appears once, in the "Authorization — policy engine"/posture
        area, as a pointer to the guide's table; (f) `:223`/`:824`/`:1309` (045's priority prose)
        — confirm gone. **Verify**: `rg -n 'install\(SAModule\)|first one registered|before `install\(\)`|priority=-sys|SAModule\b' ARCHITECTURE.md | rg -v 'SAModule\(VarcoModule\)|class SAModule'`
        → 0; `rg -n 'entry-points:begin' ARCHITECTURE.md` → 1; `uv run python scripts/entry_points.py --check` → 0.

11. [x] **`technical_docs/migrations/3-to-4.md` (§D-migration).** (a) Scratch: concatenate the
        thirteen §Migration sections verbatim into `/tmp/claude-1000/…/scratchpad/058-migration-raw.md`
        (in plan order) — the audit trail; (b) write the guide in §D-migration's eleven-part
        order, applying the four rules; (c) §10 = §D-removals-5's table; (d) §11 = the `varco
        diagnose --modules app.wiring:modules --check` invocation from 052's README section plus
        the two things a clean run prints; (e) `mkdocs.yml` nav "Migration Notes" gains
        `"3.x → 4.0": migrations/3-to-4.md` (first). Cross-check: every `Old`/`Before` cell from
        the raw file appears in the guide (a scripted set-difference over the first column of
        every table row — paste the empty diff into the preflight file). **Verify**:
        `rg -c '^\| ' technical_docs/migrations/3-to-4.md` ≥ the sum of the thirteen tables' rows
        − merges (record both numbers); `rg -n 'Plan 0(4[5-9]|5[0-7])' technical_docs/migrations/3-to-4.md | wc -l` ≥ 13;
        `rg -n 'varco diagnose' technical_docs/migrations/3-to-4.md` ≥ 2; `make docs-strict`.

12. [x] **The stale-claim sweep (the whole tree).** One script in the preflight file, run and
        pasted; every hit outside `CHANGELOG.md`, `plans/`, `design/` (history) is fixed in this
        step:
        ```
        rg -n 'setup_varco_defaults|setup_defaults=|create_varco_container|setup_event_producer|get_container\(' README.md ARCHITECTURE.md CLAUDE.md CONTRIBUTING.md technical_docs docs examples varco_*/varco_*
        rg -n '\-sys\.maxsize|2\*\*31|priority floor|lowest priority|lowest possible priority|first-registered wins|first-scanned|scan order' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs examples varco_*/varco_*
        rg -n 'DIContainer\.current\(\)|returns None if providify|providify is not installed' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs examples varco_*/varco_*
        rg -n 'eleven hand-passed|extra_lifespan_components|extra_middleware=|enable_logging=|enable_tracing=|enable_error_middleware=|migration_settings=|VARCO_LIFECYCLE_DISCOVERY_WARN' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs examples varco_*/varco_*
        rg -n 'typically in a .@PostConstruct|wired from .@PostConstruct|@PostConstruct.*register_to|register_to.*@PostConstruct' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs examples
        rg -n 'bind_retention_registry|registry\.register\(|_LEGAL_TENANT_SOURCES|_default_collectors|_collect_(http|local|auth|tenant|data|redaction)\(' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs examples varco_*/varco_*
        rg -n 'hand-constructed, not DI-wired|pass .serializer=. explicitly|_redis_active_bus|streams=True|setup_cache=|setup_producer=' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs examples varco_*/varco_*
        rg -n 'FASTREST_|VARCO_PROFILER_|VARCO_JWKS_|VARCO_CA_CERT|VARCO_CLIENT_CERT|VARCO_CLIENT_KEY|VARCO_TRUST_STORE_DIR' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs examples varco_*/varco_*   # only the migration guide + 048's warning table
        rg -n 'enable_tenant_context|tenant_field=|server_auth=None|include_detail.*True by default|warn-only|4\.0 flip candidate|encryptor=None|opt-in.*audit redaction|assert_rls_enabled\(' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs examples   # only the migration guide's "Before" cells
        rg -n 'removed in 4\.0|until 4\.0\.0|removed_in="4' README.md ARCHITECTURE.md CLAUDE.md CONTRIBUTING.md technical_docs docs varco_*/varco_*
        rg -n 'VarcoFastAPIModule|from varco_sa\.di import SAModule|from varco_beanie\.di import BeanieModule' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs examples varco_*/varco_*
        rg -n 'eight conformance modules|five conformance|six conformance' CLAUDE.md testkit README.md
        ```
        **Verify**: every command prints nothing (or only the allowed files named in its trailing
        comment); the output is pasted into the preflight file.

### Phase D — CHANGELOG, BACKLOG, the ledger

13. [x] **`CHANGELOG.md` — the `[4.0.0]` section (§D-changelog).** (a) Rename `## [Unreleased]`
        (`:10`) to `## [4.0.0] — <placeholder, set in Step 18>` and insert a fresh, empty
        `## [Unreleased]` above it; (b) the two-line header quote (migration guide, providify
        floor sentence); (c) reorder the `### BREAKING — …` blocks to §D-changelog's list, bodies
        untouched; (d) merge the remaining per-plan `### Added`/`### Changed`/`### Fixed`/
        `### Tests`/`### Documentation` blocks into the fixed sections, each bullet keeping or
        gaining `(Plan NNN)`; (e) write `### Deprecated` (started + re-labelled, with floor
        dates), `### Removed` (the §D-changelog list — each item cites its plan), `### Security`
        (one paragraph + the severity-table link), `### Upstream` (closed P24–P27 with the
        providify 2.1.0 CHANGELOG lines 045 cited; every open ask from Step 0(e) with report path
        + guard test id; the 043/044 filing blocks fold in here as "filed → closed"), `###
        Documentation`; (f) a top-of-file comment (after `:8`) stating the 4.0.0 ordering rule.
        **Verify**: `rg -n '^## \[' CHANGELOG.md | head -3` → `[Unreleased]`, `[4.0.0]`,
        `[3.2.0]`; `rg -c '^### BREAKING' CHANGELOG.md` (under 4.0.0) = 13; `rg -n '^### (Added|Changed|Deprecated|Removed|Fixed|Security|Upstream|Documentation)$' CHANGELOG.md`
        — the first eight hits are in that order and above `## [3.2.0]`; `rg -n 'migrations/3-to-4.md' CHANGELOG.md` ≥ 1;
        `rg -n 'P2[4-9]|P3[0-9]' CHANGELOG.md` covers every id from Step 0(e).

14. [x] **`BACKLOG.md` rollup (§D-backlog).** Delete the shipped `DI-*`/`FLIP-*` rows; thirteen
        Shipped bullets; `FLIP-14` → Parked (designed); OQ1/OQ2/OQ3 → Answered decisions; the new
        parks (the `Literal[True]` sweep, `scripts/_packages.py`, list-valued redaction,
        `register_webhook_signer`, `PostureFinding` deprecation, the wiring-report endpoint, the
        per-phase stop timeout); `:221` ("three barriers per mount, uniformly, since Plan 054") —
        confirm 054 did it; `:249-252` T3 gains "(enforced by `test_modules_never_scanned.py`
        since Plan 049)" — confirm 049 did it; the header and the cycle section compressed.
        **Verify**: `rg -n '^\| `(DI|FLIP)-' BACKLOG.md` → 0; `rg -n 'OQ3|Open Question 3' BACKLOG.md`
        → in "Answered decisions" only; `rg -c 'plans/05[0-8]' BACKLOG.md` ≥ 13;
        `rg -n '^## Open questions' BACKLOG.md` → 0.

15. [x] **`UPSTREAM-GAPS.md` audit (§D-upstream).** For each open row: `ls` the report; `rg -n
        'GAP: <ID>' varco_*/tests/` → exactly one strict xfail; `rg -n '^## ' design/upstream-gaps/<file>`
        → eight sections; the CHANGELOG `Upstream` block names it. Move "Recently closed" bullets
        into the CHANGELOG and delete them here; footer → `**Last updated:** <date> · Plan 058 /
        4.0.0 audit — N open (…ids…); P22, P24–P27 closed (moved to CHANGELOG [4.0.0] §Upstream)`.
        **Verify**: `rg -c '^\| \*\*P' UPSTREAM-GAPS.md` == `rg -n 'GAP: P' varco_*/tests/ | wc -l`
        == the CHANGELOG `Upstream` "filed" count; `uv run pytest varco_core/tests/test_providify_contracts.py -q -rxX`
        → `N xfailed, 0 xpassed`.

### Phase E — snapshots and gates

16. [x] **Every snapshot regenerated and checked, in this order** (each may move because of Steps
        2–12's edits — e.g. `warn_deprecated` already in `__all__` from 053; the entry-points
        snapshot from Step 5): `uv run python scripts/api_surface.py` → `git diff --stat
        design/api-freeze-and-standards/measurements/api-surface.*` → **expected empty** (this
        plan adds no public name; a non-empty diff is another branch's dirt — stop and look);
        `uv run python scripts/settings_reference.py --check` → 0; `uv run python scripts/di_catalogue.py --check`
        → 0; `uv run python scripts/entry_points.py --check` → 0; `make asyncapi-check` → 0;
        `uv run python scripts/import_budget.py --check --warn-only` → no breach (this plan adds
        no import; `varco_core/tests/test_deprecation_floor.py` imports nothing from `varco_*`);
        `uv run python scripts/bump.py --check` → coherent. **Verify**: all seven exit 0; the
        api-surface diff is empty.

17. [x] **Gates + the index.** `make lint` (no-`PKG`: ruff + `api-check` + `import-budget` +
        `asyncapi-check` + `settings-check` + `di-catalogue-check` + `entry-points-check`);
        `make type-check` (M1 delta recorded — expected 0); `make test` (all eleven suites on the
        local interpreter; `0 xpassed`; `xfailed` = Step 15's N); `make docs-strict`; on a
        Docker host `make integration-test-clean` (the six `VARCO_TEST_*` unset — the clean-room
        contract) — green or every red named as a pre-existing BACKLOG/nightly signal (never a
        blocker by CLAUDE.md §CI, but a *new* red introduced by 050's two integration tests is
        050's bug, not this plan's, and is reported). Then tick every box in
        `plans/000-index-4-0-di-standardization.md` "Done for the whole set" (`:61-73`) with the
        proving command beside it. **Verify**: the five `make` targets green; the index's list
        has no unticked box.

### Phase F — the release (strictly sequential; §D-release)

18. [ ] **Bump.** `uv run python scripts/bump.py --set 4.0.0 --dry-run` → review: ten
        `pyproject.toml` `version = "4.0.0"` + every sibling requirement `~=4.0` (`CONTRIBUTING.md:86-89`);
        then `uv run python scripts/bump.py --set 4.0.0` (runs `uv lock`); `uv run python
        scripts/bump.py --check` → coherent; `CHANGELOG.md`'s `## [4.0.0] — <placeholder>` → the
        release date (the day the tag will be cut); `uv run pytest varco_core/tests/test_bump_script.py
        varco_core/tests/test_deprecation_floor.py -q` (the floor test now sees `4.0.0` as pending
        **with** a heading date — rule 3 applies to `ApiKeyAuth`'s `since="4.0.0"` only as a
        `since`, never as a `removed_in`). One commit: `Release 4.0.0` (no co-author trailer —
        `feedback_no_coauthor`). **Verify**: `git diff --stat HEAD~1` lists exactly the ten
        `pyproject.toml`s, `uv.lock`, `CHANGELOG.md`.

19. [ ] **PR → `all-green`.** Open the PR from `3.3.0-plan` (or the release branch) to `main`;
        `Tests / All tests passed` is the only required check (CLAUDE.md §CI); the `lint` job now
        runs `entry points --check` (+ `export-asyncapi --check` iff Step 5 added it). Merge.
        **Verify**: the merge commit on `main` has a green `all-green`; `integration.yml`'s
        `push: main` run is observed (not required; a red is a nightly signal — record it).

20. [ ] **Rehearsal — `v4.0.0rc1`.** `git tag -s v4.0.0rc1 -m "Release candidate 1 for v4.0.0"
        && git push origin v4.0.0rc1`. Expect: `release.yml` green across all ten `publish` legs
        (pre-releases with PEP 740 attestations on PyPI); `docs.yml`'s `guard` classifies it
        pre-release and `release` is **skipped** (`docs.yml:98`); `dev` docs unaffected. A red
        leg → fix on `main`, `rc2`; **never** patch during the final tag. Record the outcome in
        `release-runbook.md` §7 (new). **Verify**: ten PyPI project pages show `4.0.0rc1` as a
        pre-release; `latest` docs alias still points at `3.2`.

21. [ ] **Release — `v4.0.0`.** `git tag -s v4.0.0 -m "Release v4.0.0" && git push origin v4.0.0`.
        Verify (runbook §6's list, substituted): ten PyPI pages show `4.0.0` with an attestation
        badge; a downloaded wheel's `METADATA` shows `Requires-Dist: varco-core~=4.0` for a sibling
        and the SBOM at `.dist-info/sboms/` (Plan 030); `pip install "varco-fastapi==4.0.0"` in a
        clean venv resolves `varco-core~=4.0` and `providify>=2.1.0`; the docs site serves `4.0`,
        `latest` (→ `4.0`), `3.2`, `dev` with a working switcher (`docs.yml:135-137`);
        `git tag -v v4.0.0` shows a good signature; `technical_docs/migrations/3-to-4.md` is
        reachable under `/varco/4.0/migrations/3-to-4/`. **Verify**: each bullet ticked in the
        runbook §7.

22. [ ] **Post-release.** `release-runbook.md` §7: outcomes of Steps 20–21 and the one-line
        operator delta vs 3.0.0 (none expected: same ten environments, same trusted publishers,
        same Pages source); `docs.yml`'s "Version naming" comment gains the `4.0` example;
        `BACKLOG.md` header → "No cycle is live … shipped as `v4.0.0` on <date>";
        `plans/000-index-4-0-di-standardization.md` gains a final line "Shipped as v4.0.0 on
        <date>". **Verify**: `rg -n 'v4\.0\.0' release-runbook.md BACKLOG.md plans/000-index-4-0-di-standardization.md`
        ≥ 3.

## Edge cases

- **Step 0 finds a plan not landed** → stop; this plan cannot assemble what does not exist. Do
  not "pre-write" a section for a missing plan.
- **`removed_in="4.0.0"` count ≠ 9 at Step 0(c)** (a plan added or removed one) → the floor test
  is the source of truth; re-label whatever it lists; update premise (1)'s count in this plan.
- **A `since` version with no CHANGELOG heading** (e.g. a plan wrote `since="3.3.0"`) → the floor
  test fails naming it; fix the site's `since` (there was no 3.3.0), never the test.
- **The pending version is still `3.2.0` when Step 1 runs** (Step 18 has not bumped) → rule 3
  treats `4.0.0` as "no heading, not pending" → violation "unknown release" for
  `removed_in="4.0.0"` sites and for 053's `since="4.0.0"`. The test therefore accepts
  `major(pending) + 1 == major(version)` as "the next major, un-dated" **only** for a
  `removed_in` (a *future* removal never needs a date) and for a `since` **only** when the
  `[Unreleased]` section is non-empty (a release is being prepared). Both branches are
  unit-tested in Step 1(b). After Step 18 the ambiguity disappears.
- **`api_surface --check` reports a change at Step 3 or 16** → not caused by this plan (no
  `__all__` moves); another branch's dirt — stop and look; never regenerate to make it pass.
- **A `VarcoModule` subclass lives in `examples/`** → `examples` is not a distribution package
  (`packages.sh`), so it is not walked and not a completeness error; documented in the script.
- **`walk_packages` import side effects** (a module that opens a socket at import) → the same
  cost `api_surface.py`/`settings_reference.py` already pay; a package that fails to import →
  `EntryPointsError` naming it, never a shorter snapshot.
- **The ARCHITECTURE.md markers are edited by hand between them** → `--check` fails naming the
  block; the fix is `make entry-points`, never a hand edit.
- **Two plans' Pitfalls rows have the same symptom but different fixes** → §D-guide's merge
  rule: the later plan's fix wins and the row cites both; if neither is "later" (unrelated
  plans), keep both rows with distinct symptoms.
- **The `[Unreleased]` section contains a block from a plan that did not ship** → it is not in
  the tree (Step 0(b) would have stopped); if it is a docs-only block from 043/044, it folds into
  `Upstream` as "filed → closed".
- **`bump.py --set 4.0.0` fails `uv lock`** (network) → `--no-lock`, then `uv lock` by hand;
  the commit must still contain `uv.lock`.
- **A `publish` leg fails on the final tag** → runbook Edge cases: re-run that leg via
  `workflow_dispatch` on the same tag; **never re-tag** (the tag ruleset blocks it anyway).
- **`docs.yml` `release` runs but `latest` does not move** → `mike deploy --update-aliases 4.0
  latest` is idempotent; re-run the job.
- **A nightly `integration.yml` goes red after the merge** → a BACKLOG signal, not a release
  blocker (CLAUDE.md §CI); record it in §7.

## Verification

```bash
# Phase A — OQ3
uv run pytest varco_core/tests/test_deprecation_floor.py -q
rg -n 'removed_in="4\.0\.0"' varco_*                                                # 0
rg -n 'removed_in="5\.0\.0"' varco_*/varco_* | wc -l                                # 9 (5 sites + trust_store + consumer + tenant_resolution + ApiKeyAuth)
rg -n 'warnings\.warn\(' varco_*/varco_* | rg -v deprecation.py | rg -i deprecat    # 0
uv run python scripts/api_surface.py --check                                        # 0, no change

# Phase B — entry points
uv run pytest varco_core/tests/test_entry_points_script.py -q
uv run python scripts/entry_points.py --check                                       # 0
make lint                                   # runs entry-points-check; make lint PKG=varco_core must not
rg -n 'entry points --check' .github/workflows/test.yml                             # 1

# Phase C — docs (the stale-claim sweep of Step 12, all empty)
rg -c '^## ' technical_docs/features/dependency-injection.md                        # 14
rg -n '^## Wiring an app' README.md                                                 # 1
rg -n 'migrations/3-to-4.md' CHANGELOG.md README.md mkdocs.yml CLAUDE.md            # >= 4
make docs-strict

# Phase D
rg -n '^## \[' CHANGELOG.md | head -3                                               # [Unreleased], [4.0.0], [3.2.0]
rg -c '^### BREAKING' CHANGELOG.md                                                  # 13 under 4.0.0
rg -n '^\| `(DI|FLIP)-' BACKLOG.md                                                  # 0
rg -c '^\| \*\*P' UPSTREAM-GAPS.md; rg -n 'GAP: P' varco_*/tests/ | wc -l           # equal
uv run pytest varco_core/tests/test_providify_contracts.py -q -rxX                  # N xfailed, 0 xpassed

# Phase E — every gate
make lint && make type-check && make test && make docs-strict
uv run python scripts/settings_reference.py --check && uv run python scripts/di_catalogue.py --check
uv run python scripts/entry_points.py --check && make asyncapi-check
uv run python scripts/import_budget.py --check --warn-only
uv run python scripts/bump.py --check
make integration-test-clean                                                         # Docker host

# Phase F — release
uv run python scripts/bump.py --set 4.0.0 --dry-run
uv run python scripts/bump.py --set 4.0.0 && uv run python scripts/bump.py --check
git tag -s v4.0.0rc1 -m "Release candidate 1 for v4.0.0" && git push origin v4.0.0rc1
git tag -s v4.0.0 -m "Release v4.0.0" && git push origin v4.0.0
git tag -v v4.0.0
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — all thirteen plans have landed on the branch when this plan is built | Every §Cross-plan winner exists in code | Step 0(b): one Done-criterion grep per plan; any red stops the plan |
| ⚠️ ASSUMPTION — `since` for the `varco_fastapi.auth.TrustStore` shim is `3.1.0` (Plan 026 shipped in 3.1.0) | The floor date for that shim is right (2027-09-05) | Step 0(c) reads `CHANGELOG.md:697`'s `[3.1.0] ### Deprecated` block |
| ⚠️ ASSUMPTION — `test.yml`'s `lint` job lacks an `export-asyncapi --check` step (premise (4)) and 047/048 add theirs after `api surface --check` | CLAUDE.md's CI claims match the file | Step 0(i) reads the job after 047/048; Step 5 adds the missing step iff absent |
| ⚠️ ASSUMPTION — `CHANGELOG.md`'s release headings keep the `## [X.Y.Z] — YYYY-MM-DD` shape | `test_deprecation_floor.py` can read the dates | Step 1(a) pins the three known headings; the top-of-file comment (Step 13(f)) states the format is load-bearing |
| ⚠️ ASSUMPTION — `providify.metadata._has_configuration_module` is the reader for the `@Configuration` marker (049 §Context cites `metadata.py:536-547`) | The `configuration` family is complete | Step 4 imports it under the same name 049's `test_modules_never_scanned.py` uses; if it moved, use that test's import |
| ⚠️ ASSUMPTION — `technical_docs/di-catalogue.md` and `settings-reference.md` are in `mkdocs.yml`'s nav (047/048 said so) | The "Generated reference" group has three pages | Step 0(j); Step 5 moves/creates entries |
| ⚠️ ASSUMPTION — `warn_deprecated()` (053) exists with `stacklevel=` (053 §D-apikey's signature) | The two converted sites blame the caller | Step 0(b); if 053's signature differs, use its real one — the message shape is `_build_message()`'s either way |
| **The re-label is read as "the windows were extended arbitrarily"** | `CONTRIBUTING.md:98`'s floor was always the binding rule; 4.0.0 merely arrived before it | CONTRIBUTING's Mechanism paragraph (Step 3), the CHANGELOG `Deprecated` block with floor dates, BACKLOG's OQ3 answer |
| **The floor test's regex misses a deprecation whose arguments are not literals** | Every in-tree site is a literal; the docstring says so | Step 1(d) asserts ≥ 9 sites found; a future non-literal site is a review item, and the `deprecation.py` `Args:` sentence names the test |
| **Restructuring the DI guide loses a sentence** | Every plan's body moves, never edits | `git diff --stat` mostly moved hunks; Step 7's per-plan fragment greps; the raw scratch copy of every §Migration for the guide |
| **The README chapter and the guide drift into two copies** | README = snippets that name their test; guide = design | Step 8's `# tested by` count; the "See also" section links, never restates |
| **CLAUDE.md polish deletes a rule an agent needed** | Every rule sentence fragment from 045–052 still greps | Step 9's fragment list |
| **A generated block in ARCHITECTURE.md is hand-edited** | `--check` diffs the block | Step 5; the markers say "generated — `make entry-points`" |
| **`make integration-test-clean` is red on the release branch** | Integration is never a merge gate; a *new* red from this cycle is that plan's bug | Step 17 names the owner; the release proceeds only if the red is pre-existing or fixed |
| **A `publish` leg fails on `v4.0.0`** | Never re-tag | Runbook Edge cases; the rc rehearsal (Step 20) exists to catch it first |
| **`latest` moves to `4.0` while 3.x users need 3.2 docs** | mike keeps every `<major>.<minor>` version browsable | `docs.yml`'s scheme; the migration guide lives under `4.0` and links `3.2` |
| **The `Upstream` CHANGELOG block and the ledger disagree** | One id set, three places (ledger rows, `GAP:` reasons, CHANGELOG) | Step 15's three-way count |
| **BACKLOG rollup deletes a park or an answered decision** | Parks and decisions are never deleted (`BACKLOG.md:11-12`) | §D-backlog moves OQ1–OQ3 *into* Answered decisions; only shipped work rows are deleted |
| M1 (`type: ignore` count) moves | Directional only | Step 17 records the delta (expected 0) |

## Out of scope / Parked

- **Removing any deprecated symbol** — 5.0.0, per §D-removals-5's dates.
- **`Literal[True]` at the four `acknowledge_*` `ValueError` sites** (053/056) — parked; trigger
  in §D-backlog.
- **A shared `scripts/_packages.py`** replacing the four verbatim `discover_packages()` copies —
  parked; trigger: a fifth script.
- **Redacting list-valued leaves** (057) — parked with 057's trigger.
- **`register_webhook_signer()`** (051) — parked; trigger: an out-of-tree signing scheme.
- **Deprecating `varco_fastapi.posture.PostureFinding`** (051) — not in 4.0; a 5.0.0 planning item.
- **An HTTP endpoint for the wiring report**, **per-phase stop timeout**, **`varco diagnose
  --check` as a repo gate** — 052/050's own parks, restated in BACKLOG.
- **`import_budget.py` as a real gate** — Plan 028 Phase 2's criterion (≥ 10 observations) is
  unmet.
- **Rewriting pre-052 tests onto `InMemoryProfile`** — style, not a defect; no row.
- **A `--check` for `technical_docs/migrations/3-to-4.md`** against the plans' §Migration
  sections — the plans are history once 4.0.0 ships; the raw scratch copy (Step 11(a)) is the
  one-time audit.

## Done — this plan

- [ ] `test_deprecation_floor.py` green; `rg 'removed_in="4\.0\.0"' varco_*` → 0; the two ad-hoc
      sites go through `warn_deprecated()`; `api_surface --check` unchanged by the re-label
      (pasted proof); CONTRIBUTING/CLAUDE/ARCHITECTURE/feature-doc prose says `5.0.0` with the
      floor date.
- [ ] `scripts/entry_points.py` + snapshot + `technical_docs/entry-points.md` + the ARCHITECTURE
      marked block; `--check` in `make lint` (no-`PKG`) and `test.yml`'s `lint` job; the taxonomy
      count test green.
- [ ] `dependency-injection.md` in the fourteen-section order, banner gone, one Pitfalls table;
      README "Wiring an app" chapter + TOC; CLAUDE.md DI section ≤ 60 lines with every rule kept
      and the taxonomy table replaced by a pointer; ARCHITECTURE.md's DI section generated.
- [ ] `technical_docs/migrations/3-to-4.md` assembled (every §Migration row accounted for), in
      the nav, linked from CHANGELOG/README/CLAUDE.md, ending with `varco diagnose --check`.
- [ ] `CHANGELOG.md [4.0.0]` in §D-changelog's order with `Upstream`; `BACKLOG.md` rolled up
      (rows gone, OQ1–OQ3 answered, `FLIP-14` designed, parks recorded); `UPSTREAM-GAPS.md`
      audited (three-way count equal).
- [ ] Every snapshot `--check` green; `make lint`/`type-check`/`test`/`docs-strict` green on the
      release commit; `integration-test-clean` green or its red attributed.
- [ ] `bump.py --set 4.0.0`; `v4.0.0rc1` rehearsed; `v4.0.0` tagged, ten packages on PyPI with
      attestations, docs `4.0` + `latest`; runbook §7 written.

## Done for the whole 4.0 set (mirrors `plans/000-index-4-0-di-standardization.md:61-73`, each with its proving command)

- [ ] Every `DI-*` and `FLIP-*` row in BACKLOG's 4.0 Live table is shipped or explicitly parked
      with a trigger; `RO-2`/`RO-4`/`RO-5` shipped inside 052/049/057; `RO-1`, `RO-3`,
      `RO-6`–`RO-13` (+ 050's new row) remain live — `rg -n '^\| `(DI|FLIP)-' BACKLOG.md` → 0;
      `rg -n '^\| `RO-' BACKLOG.md` → 11.
- [ ] `make lint`, `make type-check`, `make test` green on 3.12/3.13 (CI's `unit` matrix);
      `api_surface.py --check`, `asyncapi-check`, `import_budget.py --check --warn-only`,
      `settings_reference.py --check`, `di_catalogue.py --check`, `entry_points.py --check` all
      green — Step 16/17.
- [ ] `testkit/varco_conformance/di_wiring.py` subclassed by every package with `module_cls`,
      `selectable`, `module_owned` set and `known_warnings == frozenset()`; `COVERAGE.md` has a row
      per package — `rg -n 'module_cls = ' varco_*/tests/test_*_di_wiring*.py | wc -l` → 10;
      `rg -n '050' varco_*/tests/test_*_di_wiring*.py` → 0.
- [ ] A default varco container's `container.validate()` has zero ERROR/WARNING —
      `uv run pytest varco_fastapi/tests/test_fastapi_di_wiring.py varco_fastapi/tests/test_startup_wiring_report.py -q`.
- [ ] `technical_docs/features/dependency-injection.md` is the single DI document; CLAUDE.md's
      taxonomy table is a pointer to the generated entry-point table; the 3.x → 4.0 migration
      guide covers every breaking row — Steps 7, 9, 11.
- [ ] `v4.0.0` tagged via `scripts/bump.py`; the nine `removed_in` sites read `5.0.0` — Steps 2, 21.
