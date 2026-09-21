# Plan index — 4.0 dependency-injection standardization + the absorbed flip list

**Cycle**: `# 4.0 — dependency-injection standardization` (BACKLOG.md:16-138, discover 2026-09-12).
**Trigger for this planning run (2026-09-13)**: providify **2.1.0** shipped every upstream ask Plans
043/044 filed — `@Requires`, `@Fallback` (+ `FALLBACK_SHADOWED`/`Severity.INFO`), open-generic
provider bindings, and the module-scoped `@Disposes` fix (P24/P25/P26/P27 all resolved). Nothing
in this cycle is blocked on upstream any more.

**Version**: 4.0.0 (locked, BACKLOG.md:54). Breaking wiring changes are allowed outright; every
breaking plan carries its own §Migration that Plan 058 assembles into the one 3.x → 4.0 guide.
The nine `removed_in="4.0.0"` deprecation sites are **not** removed (12-month floor, 2027-08-31)
— 058 re-labels them to `5.0.0`.

**Research briefs backing the set** (no new briefs were needed — every external claim is covered):
010 (default-implementation conventions), 014 (multi-binding / qualifiers / conditional DI), 015
(modules / settings binding / test profiles), 016 (open generics), 017 (fallback timing +
diagnostics), 006 (tenant identity), 007 (RLS mechanics), 008 (HTTP hardening), 009 (revocation /
credential storage). providify 2.1.0 semantics are cited from its source tree, never from README.

## Slices

| # | Plan | BACKLOG rows | Depends on |
|---|---|---|---|
| 045 | [providify-2-1-adoption-and-fallback-defaults](045-providify-2-1-adoption-and-fallback-defaults.md) — **DONE** | DI-15 close-out, DI-3 | — |
| 046 | [wiring-verb-collapse-and-explicit-container](046-wiring-verb-collapse-and-explicit-container.md) | DI-2, DI-8, DI-13 | 045 |
| 047 | [config-driven-backend-selection-and-catalogue](047-config-driven-backend-selection-and-catalogue.md) | DI-4, DI-12, DI-17 | 045, 046 |
| 048 | [settings-standardization](048-settings-standardization.md) | DI-6, DI-16 | 045 |
| 049 | [varco-module-and-composable-app](049-varco-module-and-composable-app.md) | DI-1, FLIP-1, RO-4 | 045, 046, 048 |
| 050 | [phased-lifecycle-and-di-wired-infrastructure](050-phased-lifecycle-and-di-wired-infrastructure.md) | DI-9, DI-10 | 049 |
| 051 | [multi-binding-registries](051-multi-binding-registries.md) | DI-5 | 045 |
| 052 | [diagnose-report-and-test-profile](052-diagnose-report-and-test-profile.md) | DI-7, DI-11, RO-2 | 047, 049, 051 |
| 053 | [credential-and-auth-flips](053-credential-and-auth-flips.md) | FLIP-5, FLIP-034-1/2/3/4 | 045 |
| 054 | [http-edge-and-admin-surface-flips](054-http-edge-and-admin-surface-flips.md) | FLIP-2, FLIP-3, FLIP-4, FLIP-10 | — |
| 055 | [tenant-provenance-flips](055-tenant-provenance-flips.md) | FLIP-6, FLIP-7, FLIP-9, FLIP-14 (design only) | — |
| 056 | [data-layer-flips](056-data-layer-flips.md) | FLIP-12, FLIP-13 | — |
| 057 | [redaction-flips](057-redaction-flips.md) | FLIP-040-1, FLIP-040-2, RO-5 | — |
| 058 | [di-guide-migration-and-4-0-release](058-di-guide-migration-and-4-0-release.md) | DI-14, Open Q3, the release | all of the above |

## Build order

```
DI track:    045 ─► 046 ─► 047 ─┐
               │        └► 048 ─┼─► 049 ─► 050 ─┐
               └────────────► 051 ──────────────┼─► 052 ─► 058
Flip track:  053 (after 045) · 054 · 055 · 056 · 057   (each independently mergeable, any time before 058)
```

- **045 first, alone.** It moves the providify pin, flips four strict xfails to real tests, and
  puts `@Fallback` on every framework default — every later plan assumes that contract.
- **046 next.** The `bootstrap()` return contract and the `DIContainer.current()` removal are what
  047/049's module `register()` and 052's provenance report build on.
- **047 and 048 in parallel** (both need 046's explicit-container shape; neither needs the other).
- **049** needs 048's settings recipe and 046's verbs; **050** and **051** follow 049 / 045.
- **052** reads 047's catalogue, 049's modules, 051's multi-bound inspectors — it is the last code
  plan.
- **053–057** touch security defaults, not wiring. They are isolated on purpose (uneven risk: each
  is a behaviour break with its own escape hatch) and can be built and merged in any order; 053
  waits for 045 because FLIP-5 rides the `@Fallback` BaseAuthorizer binding.
- **058 last** — docs, migration guide, deprecation re-labelling, `scripts/bump.py --set 4.0.0`.

## Plan 045 handoff notes (Step 17 — recorded here, one paragraph per neighbour)

- **046** — the four `*_fallback_tier.py` seeds (`varco_kafka`/`varco_nats`/`varco_redis`/
  `varco_sa`'s `tests/test_*_fallback_tier.py`) and the `test_validate_clean_*` legs
  (`varco_fastapi`/`varco_kafka`/`varco_nats`) are to be folded into `di_wiring.py`;
  `varco_sa`'s validate-clean leg was dropped (`SAConfig` needs a real engine — no bare-scan
  default is possible) with a note in `varco_sa/tests/test_validate_clean_scan.py` for 046 to
  design a fixture-backed version. The redundant `install()`-after-scan pattern removed from
  `test_di_binding_health.py`/`test_di_binding_health_i18n_tz.py` turned out **not** to be an
  `AMBIGUOUS_BINDING` risk — providify 2.1.0 already dedupes a second `install()` of an
  already-scanned `@Configuration` (measured at Step 1(e); a correction to the plan's own
  ⚠️ ASSUMPTION).
- **047** — `varco_core.di.BACKEND_PRIORITY` (100) has generous headroom for the env-driven
  override tier; the redis three-level shape (`RedisEventBusSelectorConfiguration.bus` at
  `BACKEND_PRIORITY`, `RedisEventBus` bare, `qualifier="redis"`) is built and pinned by
  `test_redis_fallback_tier.py`; two backends scanned for one seam still tie (unchanged).
- **050** — P24 is closed upstream; `DIContainer.provide()` returns the `ProviderBinding` it
  registered.
- **051** — `get_all()`/`aget_all()` exclusion of a shadowed fallback is pinned at
  `varco_core/tests/test_providify_contracts.py::TestP27FallbackBinding::test_get_all_excludes_shadowed_fallback`.
- **052** — read `technical_docs/features/dependency-injection.md`'s "Reading `FALLBACK_SHADOWED`"
  section before building the diagnose report: the scanned-class blind spot (P28,
  `design/upstream-gaps/providify-fallback-shadowed-scanned-class.md`) means 22 of varco's own
  default sites never emit the INFO — the report must fall back to a static catalogue for those.
- **053** — `BaseAuthorizer` is `@Fallback @Singleton` with no priority constant at all (the removed
  `_FALLBACK_PRIORITY`); the posture message at `varco_fastapi/varco_fastapi/posture.py` (the
  `base_authorizer_bound` finding's `remediation` string) was reworded to "register any
  `AbstractAuthorizer` binding" — no more priority-number advice.
- **058** — `technical_docs/features/dependency-injection.md`'s **Default bindings** section is
  final as of this plan; §Migration in `plans/045-providify-2-1-adoption-and-fallback-defaults.md`
  is the 4.0 guide's `DI-3`/`DI-15` entry, ready to assemble verbatim.

### A genuine, newly-diagnosed bug fixed in the course of 045 (not upstream, not pre-planned)

`varco_sa.di.SAModule.sa_xact_advisory_lock` (a plain, non-fallback `@Provider` returning the
concrete `SAXactAdvisoryLock`) was an **accidental** candidate for an unqualified
`Inject[AbstractDistributedLock]` request all along, via providify's `issubclass`-based interface
matching — invisible while `sa_advisory_lock` (the intended `AbstractDistributedLock` default) was
also a plain non-fallback binding at the same priority (registration order broke the tie in
`sa_advisory_lock`'s favour). The moment `sa_advisory_lock` became `@Fallback`, this accidental
sibling — still non-fallback — started winning the ABC slot outright, reversing the documented
"kept as `SAAdvisoryLock` so upgrading does not change behaviour" guarantee. Fixed by also marking
`sa_xact_advisory_lock` `@Fallback` (both now tie as fallbacks; `sa_advisory_lock` wins by
`BACKEND_PRIORITY`); `Inject[SAXactAdvisoryLock]` is unaffected either way. Guarded by
`varco_sa/tests/test_sa_fallback_tier.py::test_sa_advisory_lock_outranks_in_memory_default`
(asserts the winning binding's identity) and
`::test_user_lock_provider_registered_after_install_wins` (asserts an app override still wins over
both SA bindings). Neighbours building on `varco_sa.di.SAModule` should be aware any concrete-typed
`@Provider` whose return type happens to subclass an ABC other bindings target needs the same
`@Fallback` treatment, or it will accidentally compete for that ABC's unqualified slot.

## Done for the whole set

- Every `DI-*` and `FLIP-*` row in BACKLOG.md's 4.0 Live table is shipped or explicitly parked with
  a trigger; RO-2/RO-4/RO-5 are shipped inside their host plans; RO-1/3/6–13 remain live.
- `make lint`, `make type-check`, `make test` green on 3.12/3.13; `api_surface.py --check`,
  `asyncapi-check`, `import_budget.py --check --warn-only`, and the new settings/catalogue/
  entry-point `--check` snapshots all green.
- `testkit/varco_conformance/di_wiring.py` subclassed by every package; COVERAGE.md has a row per
  package.
- A default varco container's `container.validate()` has zero ERROR/WARNING issues.
- `technical_docs/features/dependency-injection.md` is the single DI document; CLAUDE.md's taxonomy
  table is a pointer to it; the 3.x → 4.0 migration guide covers every breaking row.
- `v4.0.0` tagged via `scripts/bump.py`; the nine `removed_in` sites read `5.0.0`.

## Findings that surfaced during planning (read before building)

Each planner verified its premises against source; these are the ones that change another plan
or the backlog, all reconciled in 058's §Cross-plan reconciliation table (058 is the authority
where two plans disagree):

- **FLIP-6 cannot land as `TypeError` in 4.0** (055): 3.2.0 was tagged 2026-09-12, so the
  12-month floor protects `TenantResolutionMiddleware(chain=None)` until 2027-09-12. The default
  flips to the configured chain; the implicit `LegacyTenantSource` path stays as a
  `warn_deprecated(since="3.2.0", removed_in="5.0.0")` warning that blocks a production start
  via posture. Same floor logic governs 058's re-labelling of the nine `removed_in="4.0.0"` sites.
- **OQ1 answered (049)**: `VarcoModule` is a plain varco ABC, never a providify `@Configuration`
  — the scanner auto-installs every public `@Configuration`, `depends_on` orders install only,
  and `install()` constructs the class itself.
- **OQ2 superseded (047)**: providify 2.1.0's `@Requires(env=, value=)` replaces Plan 043's
  `activate_profile()` translation outright. Precedence code > env-dotted > env-named > default is
  carried by the `@Fallback` post-step, not by priority — 045's `BACKEND_PRIORITY` stands.
- **Scout counts corrected** (046): `DIContainer.current()` is 13 code sites in 10 files (the 32
  includes docs/comments); providify is already a hard dependency (16 `try: import providify`
  guards are dead code); `_redis_active_bus` is a docstring ghost (047).
- **New upstream asks filed as strict xfails**: P28 (045→052, conditional: scanned-class
  defaults never emit `FALLBACK_SHADOWED`), P29 (046: `BindingDescriptor` lacks a `fallback`
  field), P-DISP `provide(disposer=)` and P-INJ runtime-type inject override (050). None blocks a
  plan; each has a varco-side interim path.
- **Latent bug caught in composition** (055): 049 + 051 + 054 together would make every
  non-tenant 4.0 app refuse in production (`chain=None` → two WARNs); fixed by re-homing
  `TenantPostureCollector` into `TenancyModule` and treating `NullTenantMembership` as "no provider".
- **Real bugs found while planning, fixed in-plan**: `lifespan.py:253` rolls back a partial
  start in forward order (050); reliability admin routers enforce `admin_role` on 6 of 14 routes
  (054); `enforce_rls` was read by nothing (056); `AuditConsumer` was never stopped (050).
- **Ordering fact**: providify orders `get_all()` by ASCENDING priority, ties by registration
  order (051, container.py:1704-1707) — `priority` is never a collection order key.

## Out of scope (stay live in BACKLOG)

`RO-1`, `RO-3`, `RO-6`, `RO-7`, `RO-8`, `RO-9`, `RO-10`, `RO-11`, `RO-12`, `RO-13` — none is
DI-shaped and no DI row depends on them. `FLIP-14` gets a recorded design in 055 but no
implementation steps.
