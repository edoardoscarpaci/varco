# Plan 052 — `varco diagnose`, the startup wiring report, RO-2 posture collectors, and the in-memory test profile (DI-7, DI-11, RO-2)

Covers BACKLOG 4.0 rows **`DI-7`** (`BACKLOG.md:78` — *"`varco diagnose` + startup wiring report —
for every ABC: the live implementation, where it came from (default / env / code override /
profile), active deprecations, disabled features; `container.validate()` wired into `VarcoLifespan`.
Reuses the shipped `inspect_*()` inspectors and `SecurityPosture` rather than a second harness"*),
**`DI-11`** (`BACKLOG.md:92` — *"Test profile / override story — an in-memory profile of a whole
varco app (every seam's in-process default) plus documented `ContainerOverrides`/`di_overrides`
usage, so a test swaps one binding without constructing the other fourteen"*) and **`RO-2`**
(`BACKLOG.md:110` — *"Wire `inspect_jwks_posture()` and `inspect_retention_posture()` into
`SecurityPosture`"*).

Slice row 052 of `plans/000-index-4-0-di-standardization.md` — **the last code plan of the DI
track**; it reads what 045–051 built and adds no new wiring primitive. **Fixed inputs, referenced by
number and never re-decided**: 045 (`@Fallback` on every varco default; `FALLBACK_SHADOWED`/
`CONDITION_INACTIVE` at `Severity.INFO`; §D-proof's scanned-class blind spot handed to this plan as
a conditional P28 filing), 046 (`bootstrap(container) -> DIContainer`; `bind_instance`/
`is_user_bound`; zero `DIContainer.current()`; the `di_wiring` conformance suite; P29 — the private
`_bindings` read centralised in one shim), 047 (`varco_core.di.catalogue()` + the v1 JSON schema
"the committed snapshot and 052's runtime report are the same `Catalogue.to_dict()`";
`inspect_bindings()`/`BindingRecord`; `inspect_backend_selection()` → `SeamSelection.source`;
`check_backend_selection()`; `feature_default()`/`FEATURES`; the `di.in_process_backend` posture
finding), 048 (`bind_settings()`; `provenance_of(container, Cls) -> SettingsProvenance` backed by a
per-container `WeakKeyDictionary`; `bind_core_settings()`), 049 (`VarcoModule` with
`register/async_init/validate/start/stop`; `ModuleRegistry` — *"what 052's `varco diagnose` prints
under Modules"*; `validate(container) -> Sequence[str]` — *"052 sequences"*; `create_varco_app`'s
`validate`/`strict_validation` kwargs kept as *build-time* checks — *"052 owns the runtime
`validate()`"*; `_warn_unowned_settings()` — *"052 may fold into `validate()`"*), 050 (`Phase`,
`LifecycleComponent`, `LifecycleRunner.plan`/`last_failures` — *"the data 052 prints"*;
`phased_components()`), 051 (`collect()`/`acollect()`/`collection_seams()`;
`varco_core.posture.PostureCollector` `@Multibound` — *"052's jwks/retention collectors"*;
`contribute()`), 057 (`RedactionPostureCollector` already contributed). Neighbours: **054** flips
`SecurityPostureSettings.enforce` — this plan *reports* the effective value, never changes it;
**058** assembles the guide — this plan writes `dependency-injection.md`'s §Diagnostics and
§Testing sections in final form.

**Research**: 010 §4 (Spring Boot `--debug` auto-configuration report and `/actuator/conditions`;
Django `check --deploy`; *"FastAPI/Starlette: no built-in report … evidence gap"*; the varco
implication verbatim: *"which implementation of each ABC is live, whether defaults were used,
configuration source"*), 015 §4 (only Spring ships a standard "what did I auto-configure" report),
015 §5 (Quarkus `@TestProfile`, NestJS `overrideProvider`, Spring `@TestConfiguration`; *"Varco's
testing gap: testing one override requires constructing 15 objects minus one"*), 015 §Librarian #3
(*"(1) which implementation of each ABC is live, (2) where it came from, (3) any deprecations
active, (4) which features are disabled … wire the `inspect_*()` functions into a startup report"*)
and #4 (test-profile composition), 015 Evidence Gap 3 (*"no template for what a varco 'diagnose'
report should contain"* — varco designs the shape; §D-model is that design).

**providify source of record**: `/home/edoardo/projects/providify` at **2.1.0** (the tree 045
pinned). Every `providify/…:line` below was read there on 2026-09-17; Step 1 re-confirms each on
the installed copy.

> ⚠️ **Five premises corrected from source before anything was decided.**
> (1) **The providify override verb is `ContainerOverrides.instance(interface, obj)`**, not
> `override()` (`providify/testing.py:121-150`); it calls `reset_binding(interface)` (drops *every*
> binding of the interface, any qualifier — `:148`, docstring `:80-83`) then `provide(lambda: obj,
> returns=interface)` at `DEPENDENT` scope. So a test double never ties with a `@Fallback` default
> — the default is gone, not shadowed. §D-testprofile's recipe uses the real name.
> (2) **`BindingDescriptor` carries no `fallback`, `conditions`, `profiles` or `owner`**
> (`providify/descriptor.py:49-54`: `interface`, `implementation`, `scope`, `qualifier`,
> `priority`, `dependencies`) — `container.describe()` is *not* the export the report reads. The
> report reads 047's `inspect_bindings()` records (public `b.fallback`/`b.conditions`/`b.profiles`
> attributes, `providify/binding.py:202-212,716-727`, over the one P29-guarded private walk).
> (3) **`varco_core.deprecation` records nothing** — `deprecated()`/`deprecated_alias()` call
> `warnings.warn(..., DeprecationWarning, stacklevel=2)` and return (`deprecation.py:213,253,260,341`);
> and **eight files** raise `DeprecationWarning` directly, outside the helper
> (`rg -c DeprecationWarning varco_*/varco_*` → 9 files, 22 hits). "Active deprecations" needs a
> minimal hook in the helper *plus* a capture window for the direct sites (§D-deprecations).
> (4) **`create_varco_app(validate=True, strict_validation=False)` never calls providify's
> `validate()`** today — `validate_container_bindings()` asserts three bindings and
> `validate_router_class()` checks routers (`varco_fastapi/app.py:342-367`, `validation.py:233-292`).
> 049 keeps both kwargs as build-time checks; §D-startup gives them their runtime half.
> (5) **`validate()`'s `owner` labels are `ClassName` / `@Provider(fn.__name__)`**
> (`providify/container.py:519-546`) — `fn.__name__`, not `__qualname__`. A closure-based provider
> (046's `bind_instance`, 051's `contribute`) is labelled `@Provider(_factory)` unless its
> `__name__` was stamped. The report therefore never keys on `owner`; it keys on
> `(interface, qualifier)` and carries `owner` as display text (⚠️ Step 1 measures what 046/051
> actually stamped).

## Goal

After this plan: `varco_core.di.build_wiring_report(container, …) -> WiringReport` is one frozen,
JSON-serialisable answer to "what is this app running and why" — per seam (047's six + every
catalogued default + 051's five collection seams): the live binding, its provenance
(`code`/`profile`/`env-dotted`/`env`/`default`/`absent`), what it shadows and what is inactive; per
module: phase, enabled and why; per settings class: which fields came from env vs code vs default;
every deprecation that fired; providify's `ValidationReport` folded in verbatim; the "bound but not
wired" class surfaced through the shipped `inspect_*()` functions. `varco diagnose` renders it
(text or `--json`) from an app's module list without starting anything, and `--check` exits
non-zero on any ERROR/WARNING. Every `create_varco_app` lifespan runs `container.validate()` after
`async_init` and before the first `start()`, logs the report at INFO (opt-out via
`VARCO_DIAGNOSE_STARTUP_REPORT=false`), and refuses to start on an ERROR (on a WARNING too when
`strict_validation=True`). `inspect_jwks_posture()` and `inspect_retention_posture()` run inside
`SecurityPosture` as two `PostureCollector`s (RO-2). `varco_core.testing.InMemoryProfile` +
`in_memory_modules()` give a downstream test a whole varco app on in-process defaults with no
scheduler, no JWKS refresh and a warn-only posture, and the documented recipe swaps **one** binding
through providify's own `di_container` + `di_overrides` — never a varco wrapper (RL-3). The P28
scanned-class blind spot is measured and filed (or recorded as already closed).

## Non-goals

- **Any change to providify.** 2.1.0 as shipped. At most one upstream gap is *filed* (P28,
  §D-P28) with a strict xfail; nothing waits on it — the report computes shadowing from public
  reads and never depends on the missing INFO.
- **Flipping `enforce`** (FLIP-2) — 054. The report prints `SecurityPostureSettings.enforce`'s
  effective value and source; it never sets it.
- **A second posture harness.** Every inspector is consumed exactly once: as a `PostureCollector`
  (RO-2, `SecurityPosture` judges) *or* as a report section (`varco diagnose` shows facts). No new
  `inspect_*()` function is written; no severity assigned in `varco_fastapi/posture.py` moves.
- **An HTTP endpoint for the report** (`/actuator/conditions`). Parked with a trigger (§Parked) —
  the JSON shape is stable and an app can mount `app.state.varco_wiring_report` itself.
- **A worker/CLI host that *starts* modules** — `varco diagnose` deliberately never calls
  `start()`; RO-9's driver is not built here.
- **Wrapping or re-exporting providify's four pytest fixtures** — `BACKLOG.md:222` "Verified as
  already sound": document, never wrap. `varco_core.testing` ships a module + a profile, no
  fixture.
- **A `create_test_app()` helper** — decided against (§D-testprofile); `create_varco_app(container,
  modules=in_memory_modules())` is already one line.
- **Rendering the committed `di-catalogue.json`** — the report calls `catalogue(container)` at
  runtime; the file under `design/` is a dev-tree snapshot (047 §D-introspect).
- **Converting the eight direct `warnings.warn(DeprecationWarning)` sites to `deprecated()`** —
  058's re-label sweep touches them; here they are *captured*, not rewritten.
- **A `varco_core` top-level eager import** — `varco_core.testing`, `varco_core.di.report` and
  `varco_core.di.diagnose` enter the PEP 562 lazy map only (import budget).

## Context

### providify 2.1.0 facts this plan rides on (verified in the tree)

| Primitive | Where | What the plan does with it |
|---|---|---|
| `Severity` — `ERROR`/`WARNING`/`INFO`; INFO *"exists so a wiring report can explain why a candidate is not the live one"* | `providify/validation.py:41-60` (`:49-52`) | Reused as the report's own severity vocabulary (§D-model) — providify wrote the INFO tier *for* this report |
| `IssueKind` — 14 kinds incl. `CONDITION_INACTIVE` (`:110-114`), `FALLBACK_SHADOWED` (`:115-119`) | `validation.py:63-119` | Folded verbatim; the text renderer groups by kind |
| `ValidationIssue(kind, severity, owner, message, param_name, requested, qualifier, candidates, shadowed_by)`; `to_dict()` | `validation.py:127-223` | Embedded as-is under `validation.issues` |
| `ValidationReport(issues, checked_bindings)`; `errors`/`warnings`/`infos`; `ok = not errors`; `to_dict()`; grouped `__repr__` | `validation.py:226-329` | `ok` is never read (045's U-8 finding: warnings do not affect it); the report keeps the three tuples separately |
| `validate(*, raise_on_error=True)` — instantiates nothing; passes 1/1b/1c/1d/2/3; raises `ContainerValidationError` iff `raise_on_error and report.errors` | `container.py:6006-6075`, `:6770-6771` | Always called with `raise_on_error=False`; refusal is varco's decision (§D-startup) |
| `_owner_label()` — `ClassName` / `@Provider(fn.__name__)` | `container.py:519-546` | Premise (5): display text only |
| `BindingDescriptor` fields; `DIContainerDescriptor.render()` | `descriptor.py:49-54`, `:189-232` | Not used for the report (premise 2); `describe()` stays the "dependency tree" tool |
| `ClassBinding.profiles`/`.fallback`/`.conditions`; `ProviderBinding` same | `binding.py:202-212`, `:716-727` | Read through 047's `inspect_bindings()`; `profiles` is the only field 047's `BindingRecord` may lack (⚠️ Step 1) |
| `DIContainer.active_profiles` | `container.py:2261` | The `profile` provenance value (§D-provenance) |
| `get_binding()` / `get_all_bindings()` — pure reads honouring activation + the fallback post-step | `container.py:7422-7491` | The "who is live" reads; never instantiate |
| `ContainerOverrides.instance()` / `.bind()` / `.factory()` / `.remove()` / `.profiles()` / `.alternative()`; snapshot on first mutation, restore on exit | `providify/testing.py:46-275` | The override API the recipe documents (premise 1) |
| `di_container` / `di_acontainer` / `di_overrides` / `di_global` — function-scoped, non-autouse; a conftest may redefine `di_container` (`:46-50` shows `app_container.copy()`) | `providify/pytest_plugin.py:38-129` | Used under providify's names; the recipe's conftest override is the plugin's own documented shape |
| `DIContainer.copy()` / `snapshot()` / `restore()` | `container.py:7800`, `:7918`, `:7960` | `copy()` is what makes "bootstrap once per session, fresh per test" cheap (§D-testprofile) |
| `@Profile` markers read off `__dict__`; `activate_profile()` | `metadata.py:726-823`; `container.py:2281` | varco itself uses no profiles; a user binding under `@Profile` is reported as `profile` |

### What each predecessor exposes for this plan (read from the plans; Step 1 confirms in code)

| Source | Read | Used for |
|---|---|---|
| 047 `varco_core.di.introspect.inspect_bindings(container) -> tuple[BindingRecord, ...]` (`interface`, `implementation`, `owner`, `qualifier`, `priority`, `fallback`, `exact_only`, `conditions`, `active`) | the one private `_bindings` walk | every binding, active or not — the *shadowed* and *inactive* lists |
| 047 `catalogue(container) -> Catalogue` (`seams`/`features`/`other_defaults`/`settings_defaults_count`; `to_dict()` = schema v1) | public | the declared defaults; embedded under `"catalogue"` |
| 047 `inspect_backend_selection(container) -> tuple[SeamSelection, ...]` (`value`, `source: code\|env-dotted\|env\|default`, `live_owner`) | public | the six seams' provenance |
| 047 `check_backend_selection(container) -> BackendSelectionReport` (raises `BackendSelectionError` on a typo; logs WARNING on bootstrapped-but-unselected) | public | one `WiringFinding` per warning; the error aborts diagnose with exit 1 |
| 047 `FEATURES` registry, `feature_default()` | public | the disabled-features list |
| 048 `provenance_of(container, Cls, qualifier=None) -> SettingsProvenance` (`cls`, `qualifier`, `env_prefix`, `fields`, `source: fallback\|instance\|external`) | public | per-settings-class section — **needs the enumerator this plan adds** (§D-settings-hook) |
| 049 `ModuleRegistry` (`modules`: ordered `(cls → instance, enabled)`), `VarcoModule.validate(container) -> Sequence[str]`, `resolve_module_order()`, `create_varco_app(container, *, modules, validate, strict_validation, …)`, `_warn_unowned_settings()` | public/internal | the Modules section; the module-validate findings; the lifespan hook |
| 050 `Phase`, `LifecycleComponent(name, phase, start, stop)`, `VarcoModule.components(container)`, `phased_components(modules, container)`, `LifecycleRunner.plan`/`last_failures` | public | the planned lifecycle order (diagnose) / realised order (post-start reads) |
| 051 `collection_seams()`, `collect()`, `contribute()`, `varco_core.posture.PostureCollector`/`PostureFinding`/`PostureSeverity`; `SecurityPostureModule` contributing six collectors from `configure_app()` | public | the Collections section; RO-2's two contributions |
| `inspect_revocation_posture(registry, store, settings) -> RevocationPostureReport` (`store_bound`, `registry_wired`, …) | `varco_core/revocation/posture.py:72-109` | the "bound but not wired" finding |
| `inspect_jwks_posture(registry) -> JwksPostureReport` (`registry_present`, `remote_source_count`, `refresher_running`, `ttl_seconds`, …) | `varco_core/authority/posture.py:80-131` | RO-2 collector #1 |
| `inspect_retention_posture(registry) -> RetentionPostureReport` (`configured`, `policy_count`, `destructive_count`, `platform_wide_policies`, `short_retention_policies`, …) | `varco_core/retention/posture.py:82-140` | RO-2 collector #2 |
| `inspect_tenant_provenance`, `inspect_redaction_posture`, `inspect_auth_posture`, `inspect_http_edge` | `rg -n '^def inspect_' varco_*/varco_*` → 7 functions | already collectors after 051/057; the report lists their *facts* section only through `SecurityPosture` (no duplication) |
| `SecurityPostureSettings` (`environment` ← `VARCO_SECURITY_ENV`, `enforce`, `suppress`) | `varco_fastapi/posture.py:154-192` | the report header's environment + the effective `enforce` value |
| `varco` CLI: built-ins registered in `_build_parser()` (`main.py:37-41`), plugins via the `varco.commands` entry-point group (`:46-58`); `export-asyncapi`'s `--check` shape (`cli/asyncapi.py:56-212`: `set_defaults(_run=)`, exit `0/1/2`, `module:attr` targets via `_import_attr`) | `varco_core/cli/` | `diagnose` copies the shape and registers as a built-in |
| `varco_core/tests/test_providify_pytest_plugin.py` | exists | the precedent for "use the four fixtures unwrapped"; the recipe test lands beside it |

## Design

### §D-model — the wiring report is frozen dataclasses in `varco_core.di.report`, one `to_dict()`, schema v1

```python
# varco_core/varco_core/di/report.py — every class @dataclass(frozen=True)
Provenance = Literal["code", "profile", "env-dotted", "env", "default", "absent"]

class BindingSummary:      # one binding, display-ready (from 047's BindingRecord)
    owner: str; implementation: str; package: str; qualifier: str | None; priority: int
    fallback: bool; shape: Literal["class", "provider"]; conditions: tuple[str, ...]
    profiles: tuple[str, ...]; active: bool

class SeamStatus:          # one row of the Seams / Features / Other defaults / Collections sections
    kind: Literal["seam", "feature", "other", "collection"]
    name: str              # "event_bus" / "feature_flags" / interface qualname / collection qualname
    interface: str; qualifier: str | None
    env: str | None; env_value: str | None          # VARCO_<SEAM>_BACKEND / VARCO_<FEATURE>_ENABLED
    live: BindingSummary | None; source: Provenance
    shadowed: tuple[BindingSummary, ...]            # active fallbacks that lost
    inactive: tuple[BindingSummary, ...]            # @Requires/@Profile-off candidates, with why
    contributors: tuple[BindingSummary, ...]        # collections only — 051's collect() set
    enabled: bool | None                            # features only

class ModuleStatus:
    name: str; cls: str; package: str; enabled: bool
    enabled_source: Literal["code", "env", "default"]; phase: int; phase_name: str
    depends_on: tuple[str, ...]; components: tuple[tuple[str, int], ...]
    validate_messages: tuple[str, ...]              # 049's validate() slot output

class SettingsStatus:      # from 048's SettingsProvenance
    cls: str; qualifier: str | None; env_prefix: str
    source: Literal["fallback", "instance", "external"]
    fields: Mapping[str, Literal["code", "env", "default"]]

class DeprecationRecord:   # §D-deprecations
    subject: str; since: str | None; removed_in: str | None; replacement: str | None
    count: int; first_location: str

class ValidationSummary:
    ran: bool; checked_bindings: int
    errors: tuple[dict, ...]; warnings: tuple[dict, ...]; infos: tuple[dict, ...]   # ValidationIssue.to_dict()

class WiringFinding:       # varco-side findings (never a posture finding — those stay in SecurityPosture)
    check: str; severity: Severity; detail: str; remediation: str

class LifecyclePlan:
    components: tuple[tuple[str, int], ...]         # (name, phase) in planned start order
    realised: tuple[str, ...] | None                # LifecycleRunner.plan when available, else None
    failures: tuple[tuple[str, str], ...]           # LifecycleRunner.last_failures (name, repr)

class WiringReport:
    schema_version: Literal[1]; varco_version: str; providify_version: str
    environment: str; enforce: str; enforce_source: Literal["code", "env", "default"]
    modules: tuple[ModuleStatus, ...]; seams: tuple[SeamStatus, ...]; features: tuple[SeamStatus, ...]
    other_defaults: tuple[SeamStatus, ...]; collections: tuple[SeamStatus, ...]
    settings: tuple[SettingsStatus, ...]; deprecations: tuple[DeprecationRecord, ...]
    validation: ValidationSummary; findings: tuple[WiringFinding, ...]; lifecycle: LifecyclePlan
    catalogue: Mapping[str, Any]                    # 047's Catalogue.to_dict(), embedded unchanged
    def to_dict(self) -> dict[str, Any]: ...        # JSON-safe; same "no additive tier" rule as 047's snapshot
    def render(self) -> str: ...                    # the text form varco diagnose and the startup log print
    def worst(self) -> Severity | None: ...         # max over validation.errors/warnings + findings
```

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: varco-owned frozen dataclasses; providify's `Severity` reused; 047's catalogue embedded, not re-derived** | Evidence Gap 3 says no Python precedent exists — the shape is varco's to fix, and fixing it as data (not log lines) is what makes `--json`, `--check`, `app.state`, and tests all read one object (brief 010 §4: Spring's report is both a log *and* `/actuator/conditions` JSON); one severity vocabulary across providify issues and varco findings; the catalogue's schema v1 contract (047 §D-introspect) is honoured by embedding its dict verbatim under one key — 047's `--check` snapshot and this report never disagree about what a default *is*; every field is derivable from a public read except the one P29 walk 047 already owns | A second `schema_version` (the report's) beside the catalogue's — both `1`; documented as independent |
| (2) render `container.describe()` + `validate().__repr__()` and call it a report | Zero new types | Premise (2): `describe()` cannot say *fallback*, *inactive*, *why*; no module/settings/deprecation sections; not a stable JSON contract; brief 015 §Librarian #3's four questions go unanswered. Rejected |
| (3) log lines only (Spring `--debug` literal) | Smallest | Un-checkable in CI, un-testable without log scraping; 049/050 explicitly exposed `ModuleRegistry`/`LifecycleRunner.plan` as *data* for this plan. Rejected |

The **text renderer** (`render()`) prints, in this order: header (`varco <ver> · providify <ver> ·
environment=<env> · enforce=<value> (<source>)`), **Modules** (`name · phase · enabled (source) ·
components`), **Seams** (one line per seam: `event_bus  AbstractEventBus → KafkaEventBus
[varco_kafka]  source=env (VARCO_EVENT_BUS_BACKEND=kafka)`; indented `shadowed:` / `inactive:` lines
with the `@Requires` clause rendered from `BindingRecord.conditions`), **Features** (`feature_flags
→ InMemoryFeatureFlags  enabled=true  source=env`), **Other defaults**, **Collections**
(`PostureCollector ×8: http_edge, local, auth, tenant, data, redaction, jwks, retention`),
**Settings** (`RedisEventBusSettings  fallback  url=env  use_streams=default …`),
**Deprecations** (`n fired` + one line each), **Lifecycle** (planned order by phase; realised order
when known), **Validation** (`E errors, W warnings, I infos`; each ERROR/WARNING line as providify's
`__repr__` prints it; INFOs summarised as counts per kind with `--verbose`/`log level DEBUG` for
the full list), **Findings** (`[SEVERITY] check: detail (fix: remediation)`). Width-agnostic, no
colour, no tables — greppable in a container log.

### §D-provenance — one rule per section, computed from public reads; the P28 INFO is never load-bearing

| Section | Live binding | `source` |
|---|---|---|
| **Seams** (047's six) | `get_binding(interface)` (unqualified request) | 047's `inspect_backend_selection().source` verbatim (`code`/`env-dotted`/`env`/`default`); **`profile`** when the winner is a non-fallback whose `profiles` is non-empty and intersects `container.active_profiles` (a code binding that exists *because* a profile is on — brief 010 §4's "DI override" reads better as its own word); `absent` when `get_binding()` is `None` |
| **Features** (047's `FEATURES`) | `get_binding(interface)` | `code` if `is_user_bound()` (an `enable_*` or the app's own); `env` if the live binding is the gated twin (`fallback and priority == BACKEND_PRIORITY and conditions`); else `default`; `enabled` = live is not the Null default |
| **Other defaults** (047's `other_defaults` bucket) | `get_binding(interface, qualifier=q)` | `code` iff `is_user_bound(interface, qualifier=q)` else `default` |
| **Collections** (051's five) | — | `contributors` = `BindingSummary` per non-fallback binding in `get_all_bindings(seam)`; `source` = `code` if any, else `absent`; a collection is never "default" (045: contributions are never `@Fallback`) |

`shadowed` = every **active** `fallback` record for the same `(interface, qualifier-compatible)`
request that is *not* the live one — computed from `inspect_bindings()` + the request-relativity
rule (an unqualified request sees qualified candidates, `container.py:2096-2101`), **not** from
`FALLBACK_SHADOWED` INFOs. ✅ Correct for scanned-class defaults today (045 §D-proof: the INFO is
missing for 22 of them); ✅ still correct if providify later emits the INFO. `inactive` = records
with `active is False`, each with its rendered condition (047's `BindingRecord.conditions` are
`marker.describe()` strings) or `profile=<expr>`.

### §D-settings-hook — the one enumerator 048 did not ship

048 records provenance per `(cls, qualifier)` in a module-level
`WeakKeyDictionary[DIContainer, dict[tuple[type, str | None], SettingsProvenance]]` and exposes
only a point read, `provenance_of(container, Cls, qualifier=None)`. This plan adds **exactly one
function**, `varco_core.settings.all_provenance(container) -> tuple[SettingsProvenance, ...]`
(sorted by `cls.__qualname__`, then qualifier; empty tuple for a container `bind_settings()` never
touched) — a read over the same dict, no new state. ✅ The minimal hook; ❌ none. Rejected —
reconstructing provenance from `inspect_bindings()` + `model_fields_set`: cannot tell `env` from
`default` for a field (pydantic does not record the env source), which is the whole point.

### §D-deprecations — a ledger in the helper plus a capture window for the direct sites

`varco_core/deprecation.py` gains `DeprecationRecord` (frozen, the §D-model shape), a module-level
`_FIRED: dict[str, DeprecationRecord]` under a `threading.Lock` (sync, module-level — the
lazy-`asyncio.Lock` rule is about event loops and does not apply; `warnings.warn` itself is sync),
`_note(subject, *, since, removed_in, replacement)` called immediately **before** each of the six
`warnings.warn(...)` sites (`:213,253,260,341` and the two in `_deprecate_callable`'s async branch),
`fired_deprecations() -> tuple[DeprecationRecord, ...]` and `reset_fired_deprecations()` (tests
only; documented as such). `count` increments per fire; `first_location` is `"<file>:<line>"` from
`sys._getframe(2)` at the first fire. The ledger records **regardless of the warnings filter** —
under pytest or a `-W ignore::DeprecationWarning` process the warning may be swallowed, the ledger
is not. ✅ Survives the default filter; ✅ zero cost when nothing fires. ❌ Process-global state
(a ledger is inherently process-scoped, like `warnings.filters`); reset only in tests.

The **eight direct sites** (`trust_store.py`, `tenant_resolution.py`, `migration/__init__.py`,
`exception/codes.py`, `exception/http.py`, `job/base.py`, `tenancy/control/consumer.py`,
`observability/params.py`) are captured by the diagnose driver's window:
`warnings.catch_warnings(record=True)` + `simplefilter("always", DeprecationWarning)` around
`register()`/`async_init()`, mapped to `DeprecationRecord(subject=str(w.message), since=None,
removed_in=None, replacement=None, count=1, first_location=f"{w.filename}:{w.lineno}")` and merged
with the ledger (dedup on `subject`). The startup variant (§D-startup) opens the same window around
its own `setup`. A deprecation that fired at *import* time before either window exists is visible
only if it went through the helper — the report says so in a footnote when any direct-site record
is present ("direct `warnings.warn` sites are captured during bootstrap only"). Rejected —
converting the eight sites now: 058's sweep (Non-goals).

### §D-wired — "bound but not wired" reuses the inspectors; findings are varco's, facts are theirs

The class of bug DI-7 names — a store bound in DI but never handed to the object that must use it
— is reported by calling the shipped inspector with objects **resolved from the container only when
`container.is_resolvable(...)`** (049's rule for `SecurityPostureModule`), and turning one field
into one `WiringFinding`:

| Finding `check` | Inspector | Condition | Severity |
|---|---|---|---|
| `di.revocation_store_not_wired` | `inspect_revocation_posture(registry=<TrustedIssuerRegistry?>, store=<AbstractTokenRevocationStore?>, settings=<JwtVerificationSettings?>)` | `store_bound and not registry_wired` | WARNING — remediation names `TrustedIssuerRegistry(revocation_store=...)` |
| `di.bootstrapped_unselected` | 047's `check_backend_selection()` (its WARNING log stays) | per seam it warns about | WARNING |
| `settings.unowned` | 049's `_warn_unowned_settings()` logic (`VARCO_MIGRATE_MODE` set, no `MigrationsModule`; `VARCO_TENANCY_ISOLATION` set, no `TenancyModule`) — **moved** here, the function deleted | per variable | WARNING (049 said "052 may fold") |
| `module.<name>.validate` | 049's `VarcoModule.validate(container)` slot, sequenced after `async_init` (051 §D-empty's messages ride this) | one per returned string | WARNING |
| `deprecation.fired` | §D-deprecations | one per record | INFO |

Resolving `TrustedIssuerRegistry`/`AbstractTokenRevocationStore` instantiates a singleton if it
was not yet built — the same cost `SecurityPostureModule.configure_app()` already pays (051), and
never a `start()`. ✅ One harness for judgement (`SecurityPosture`), one for facts (this report),
the same seven inspectors under both; ✅ the revocation footgun CLAUDE.md documents is now a
greppable check id. ❌ The report and the posture can both mention revocation — deliberately: the
posture says *"revocation is off"* (034's finding), the report says *"you bound a store and it is
not wired"* (a wiring fact). Rejected — a second harness over the ABCs ("is every `Inject[X]`
satisfied by a *started* object?"): that is providify `validate()`'s job plus 050's lifecycle;
duplicating it is the "second harness" the row forbids.

### §D-driver — how `varco diagnose` obtains a container: register → `async_init` → `validate` → report, never `start()`

`varco_core.di.diagnose.diagnose(container, modules, *, async_init: bool = True, validate: bool =
True) -> WiringReport` (async):

```
order = resolve_module_order(modules)                    # 049 — dependency order, errors before anything registers
for m in order: m.register(container)                    # 049 template — idempotent via ModuleRegistry; a pre-registered module is skipped
active = [m for m in order if registry.enabled(m)]
if async_init: await _async_init_all(active, container)  # 049/050's own function — ainstall()'d @Configurations, load_all(), DDL
messages = {m: m.validate(container) for m in active}    # 049 slot, sequenced here as 049 said 052 would
report_v = container.validate(raise_on_error=False) if validate else None
components = phased_components(active, container)        # 050 — the *planned* order; nothing is started
return build_wiring_report(container, modules=active, lifecycle=components, validation=report_v, ...)
```

The **split** is 049's (`register`/`async_init`/`validate` are wiring; `start`/`stop` are runtime)
and 050's (a phased `LifecycleRunner` owns start/stop; `setup=` runs before any component). Diagnose
stops exactly where the runner would begin. The caller owns `await container.ashutdown()`
(the CLI does it in `finally`).

| Candidate for `async_init` | ✅ | ❌ |
|---|---|---|
| **(1) chosen: run it by default; `--static` skips it** | Full fidelity — an `ainstall()`'d `@Configuration`'s providers exist, so `validate()` reports the real graph and `MISSING_BINDING` is trustworthy; this is `django check --deploy`'s model (loads settings and apps, runs checks); the report is what production would see | Needs the environment the app would need (a Redis URL that answers, a DB for `create_tables`) — `varco diagnose` on a laptop with no Redis fails in `async_init` with the backend's own error (exit 1, message names the module). `--static` is the Docker-free form: register + validate only; the report carries a banner line *"static: async_init skipped — providers registered by an `ainstall()`'d configuration are absent, so a MISSING_BINDING naming one of them may be spurious"* and `validation.ran` is still `True` |
| (2) never run `async_init` | Always Docker-free | Every `ainstall()`-registered provider looks missing; a report that is wrong by construction for every Redis/Memcached app. Rejected as the default, kept as `--static` |
| (3) run `start()` too and read `LifecycleRunner.plan` | Realised order | Starts consumers, schedulers, JWKS fetches — the exact thing a *diagnostic* must not do; the realised order is already logged by the startup variant. Rejected |

**Inputs the CLI accepts** (one of, exit 2 otherwise):
- `--modules module:callable` — a zero-arg callable returning `Sequence[VarcoModule]` (the same
  list the app passes to `create_varco_app`; the README's "wiring an app" example already has one).
  A fresh `DIContainer()` is created.
- `--container module:callable` — a callable returning an already-bootstrapped `DIContainer`
  (`export-asyncapi --source`'s shape, `cli/asyncapi.py:294-307`); `--modules` may be combined to
  add modules; with neither module list the Modules section reads from the container's
  `ModuleRegistry` if one is bound, else is empty.
- `--app module:attr` — a `FastAPI` object built by `create_varco_app`; the CLI reads
  `app.state.varco_container` and `app.state.varco_modules` (**added by this plan** to
  `create_varco_app` — two attributes, set once, documented; ⚠️ Step 1 checks whether 049 already
  stashes them under another name and uses that name if so). `register()` already ran at build
  time; the driver's `register()` is a no-op through the registry.
- `--path DIR` (repeatable, prepended to `sys.path`) — `export-asyncapi`'s option, same semantics.

### §D-cli — `varco diagnose [--modules M | --container C | --app A] [--path DIR]… [--static] [--json] [--verbose] [--check]`

`varco_core/cli/diagnose.py` in `export-asyncapi`'s shape (`register(subparsers)`,
`set_defaults(_run=_run)`, `_import_attr`, exit codes), registered as a **built-in** in
`main.py:_build_parser()` beside `migrate`/`tenant`/`dlq`/`retention` (it depends only on
`varco_core.di`; the entry-point group is for sibling packages, `main.py:7-12`).

| Exit | When |
|---|---|
| `0` | Report produced. **Without `--check` the exit code never reflects the report's content** — a report is a report (Spring `--debug` never fails a boot) |
| `1` | `--check` and `report.worst()` is `ERROR` or `WARNING`; or the driver failed (a module's `async_init` raised, `BackendSelectionError` from 047's check, `ModuleDependencyError`/`ModuleCycleError` from 049) — the exception's message is printed to stderr, one line, then the traceback only under `--verbose` |
| `2` | Usage: none of `--modules`/`--container`/`--app`; a target without `:`; a target that is not the expected type |

`--json` prints `json.dumps(report.to_dict(), indent=2, sort_keys=True)`; `--check` composes with
both forms. No `--output`/snapshot diffing: a wiring report is environment-dependent by design
(env vars, which packages are installed) and would drift on every host — 047's catalogue is the
environment-*independent* snapshot and already has its own `--check`. Rejected — `varco diagnose
--check` as a `make lint` gate: same reason; it belongs in the app's own CI, and the README shows
that invocation.

### §D-startup — the lifespan runs `validate()` after `async_init`, before the first `start()`; kwargs gain their runtime half

`create_varco_app`'s `setup=` coroutine (049 §D-order's `_async_init_all`) becomes:

```
await _async_init_all(active, container)
report = await run_wiring_preflight(container, modules=active, lifecycle=components,
                                    validate=validate, strict=strict_validation)   # varco_core.di.diagnose
app.state.varco_wiring_report = report
```

`run_wiring_preflight()` = module `validate()` slots → `container.validate(raise_on_error=False)`
(iff `validate`) → `build_wiring_report()` → log → decide:

| `validate` | `strict_validation` | ERROR present | WARNING present | Result |
|---|---|---|---|---|
| `True` (default) | `False` (default) | yes | — | `ConfigurationError` (the existing `varco_fastapi.validation` class) listing every ERROR issue by owner/message; startup aborts exactly as a failing `start()` would (050's runner never began, `shutdown` hook still runs) |
| `True` | `False` | no | yes | logged at WARNING (one line per issue/finding); startup proceeds |
| `True` | `True` | no | yes | `ConfigurationError` — the WARNING tier is now blocking too |
| `False` | any | — | — | no `container.validate()` call; `validation.ran is False`; the report is still built and logged (module `validate()` slots still run — they are 049's, not providify's) |

- ✅ **One switch, both halves**: `validate=True` already meant "check my wiring at build time";
  the runtime half is the same intent (brief 015 §4: ASP.NET `ValidateOnStart` — *"if config is
  invalid, the app refuses to start"*). An ERROR is providify's own definition of *"guaranteed to
  fail at resolution time"* (`validation.py:44-45`); refusing at startup moves that failure from
  the first unlucky request to the deploy. Every plan in this track requires a default container
  to validate clean (index "Done for the whole set"), so the default app is unaffected.
- ✅ `strict_validation` keeps its shape (a "promote the soft tier" boolean, as it is for
  `validate_router_class(strict=)`).
- ❌ **Behaviour change** (§Migration): an app with a latent `MISSING_BINDING` in a singleton
  nobody resolves used to boot; now it refuses. Escape: fix the binding, or `validate=False`.
  Deliberate — the index's "zero ERROR/WARNING" goal is a gate only if something gates.
- Rejected — a `DiagnoseModule` in `default_modules()` at a new pre-`INFRA` phase: ✅ "everything is
  a module"; ❌ a module's `start()` cannot run *between* `async_init` and the first component's
  `start()` and still see the planned component list; and 049 §D-signature's own rule keeps
  `validate`/`strict_validation` as `create_varco_app` kwargs *because* they are the composition
  root's build-time checks, not a subsystem — the runtime half belongs to the same owner.
- Rejected — refuse on ERROR only when `strict_validation=True`: ❌ then the default app silently
  boots into a guaranteed `LookupError`; the "warn" tier for an ERROR is a contradiction in
  providify's own vocabulary.

**The log**: `logger.info("varco wiring report\n%s", report.render())` from
`varco_core.di.diagnose` (logger name `varco_core.di.diagnose`), one record, after `validate()`,
**before** any `start()`; every ERROR/WARNING issue and every WARNING finding is *also* emitted as
its own `logger.warning`/`logger.error` line so a log pipeline that drops INFO still sees them.
Opt-out: `DiagnoseSettings` (`VarcoSettings`, `env_prefix="VARCO_DIAGNOSE_"`, bound by 048's
`bind_core_settings()`): `startup_report: bool = True` (log the rendered report — the *problem
lines* are never suppressed by it), `startup_report_json: bool = False` (log `to_dict()` as one
JSON line instead — for structured-log shippers). Code override:
`bind_settings(container, DiagnoseSettings, instance=DiagnoseSettings(startup_report=False))`
before `create_varco_app` — 048's recipe, no new kwarg. ✅ Presentation is env-controlled, the gate
is code-controlled (`validate=`), same split as `SecurityPostureSettings.environment` vs `enforce`
(036). The **post-start** facts (`LifecycleRunner.plan`/`last_failures`) are not in the pre-start
report by construction; `varco_fastapi.diagnose.wiring_report(app) -> WiringReport` re-renders
`app.state.varco_wiring_report` with `lifecycle.realised`/`failures` filled from
`app.state.varco_lifespan.last_runner` when present (050 stores `self.last_runner`) — a pure read
for tests and an app-mounted endpoint.

### §D-RO2 — `JwksPostureCollector` and `RetentionPostureCollector` live beside their inspectors, contributed by `CoreModule`

Two `PostureCollector` subclasses, ~15 lines each, in the inspector's own module:

| Collector (`name`) | Where | Constructor | Findings (ids are new and never renamed; severities assigned here, 036's rule "severity lives in the collector, never in the inspector") |
|---|---|---|---|
| `JwksPostureCollector` (`"jwks"`) | `varco_core/authority/posture.py` | `registry: Callable[[], TrustedIssuerRegistry \| None]` — a **supplier**, evaluated inside `collect()` | `auth.jwks_refresh_off` — `remote_source_count > 0 and not refresher_running` → **WARN** (*"N remote JWKS sources and no background refresher — a key an issuer removes stays trusted; fix: `JwksRefreshModule(registry)` or `VARCO_JWT_JWKS_TTL_SECONDS>0` + `start_refresh()`"*); `auth.jwks_keysets_unloaded` — `registry_present and remote_source_count > 0 and keysets_loaded == 0` → **INFO** (*"no keyset loaded yet — `load_all()` has not run"*); `registry_present=False` or no remote sources → no finding (a fact, not noise — 039 §D-S20-posture's rule for `configured=False`) |
| `RetentionPostureCollector` (`"retention"`) | `varco_core/retention/posture.py` | `registry: Callable[[], RetentionRegistry \| None]` | `retention.platform_wide` — `platform_wide_policies` non-empty → **WARN** naming them (039: *"the genuinely security-relevant finding"*); `retention.dry_run_only` — `policy_count > 0 and destructive_count == 0` → **INFO** (*"every policy is dry-run — nothing is purged"*); `retention.short_retention` — non-empty → **INFO**; `configured=False` → no finding |

**Contributed by `CoreModule._register()`** (guarded by `self.is_enabled(container)`, 049's
registration-time value) with lazy suppliers: `lambda: container.get(TrustedIssuerRegistry) if
container.is_resolvable(TrustedIssuerRegistry) else None` and the same for `RetentionRegistry`.
`collect()` runs at 050's `EDGE` phase (`SecurityPostureModule.start()`, after every `async_init`
and every `start()`), so the suppliers see the registry `JwksRefreshModule` started and the
`RetentionRegistry` `RetentionModule.async_init()` built.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: `CoreModule` contributes both, suppliers resolve lazily** | The decisive JWKS finding (*registry present, remote sources, refresher not running*) is **precisely the case where `JwksRefreshModule` is absent** — a collector contributed by that module would never fire for the app it exists to warn; same for an app that built a `RetentionRegistry` by hand and never added `RetentionModule`. `CoreModule` is the owner of `varco_core`, where both inspectors live; `SecurityPostureModule` stays ignorant of them (051's plugin story: a collector needs no `varco_fastapi` edit). A disabled `CoreModule` contributes nothing | 051 Step 20's handoff said *"contributed by `JwksRefreshModule`/`RetentionModule`"* — corrected here for the blind-spot reason; `RetentionRegistry` must be **bound** for the supplier to find it (⚠️ ASSUMPTION 051's `RetentionModule.async_init()` `bind_instance`s the registry it builds — Step 1 reads; if not, this plan adds that one line so `varco retention` and the supplier share one object) |
| (2) 051's handoff literally | Owner-module symmetry | The blind spot above. Rejected |
| (3) `SecurityPostureModule` resolves and contributes them (the six-collector shape) | One contribution site | Puts `varco_core` inspectors back behind a `varco_fastapi` edit — the R2 layering 051 §D-posture fixed. Rejected |

`security-posture.md` gains the five ids; `SecurityPosture` gains no new API. The
`di_wiring` suite's `varco_core` subclass asserts both collectors appear in
`collect(container, PostureCollector)` after `CoreModule().register(container)` with fake
registries bound (§D-tests).

### §D-testprofile — `varco_core.testing.InMemoryProfile` + `in_memory_modules()`; providify's fixtures unwrapped; one override, fourteen defaults

**`varco_core/varco_core/testing.py`** (a real, packaged module — `testkit/` is never packaged,
so a downstream test could not import it; PEP 562 lazy-map entry, imports providify only inside
function bodies):

```python
class InMemoryProfile(VarcoModule):
    """The whole-app in-memory profile (brief 015 §5, Quarkus @TestProfile)."""
    name = "in_memory_profile"; phase = Phase.EDGE; depends_on = (CoreModule,)
    def _register(self, c): ...                     # nothing to bind — the defaults ARE the profile
    def validate(self, c) -> Sequence[str]:        # the profile's whole job: assert the shape
        # every 047 seam: inspect_backend_selection(c) source in {"default", "env"(=memory)} else message
        # every 047 feature: live is the Null default unless the test enabled it in code (source == "code")
        # no LifecycleComponent at Phase.SCHEDULERS is planned (job runner, retention, JWKS refresh)
    def components(self, c): return ()

def in_memory_modules(*extra: VarcoModule) -> list[VarcoModule]:
    return [CoreModule(job_runner=False), *extra, InMemoryProfile()]

def register_modules(container, modules) -> DIContainer:      # resolve_module_order + register(); no async_init, no start
def clear_backend_selection(monkeypatch) -> None:              # delenv every VARCO_<SEAM>_BACKEND / VARCO_<FEATURE>_ENABLED (raising=False)
```

`varco_fastapi/varco_fastapi/testing.py`: `in_memory_modules(*extra)` = `[*default_modules(
exclude=(SecurityPostureModule,)), SecurityPostureModule(settings=SecurityPostureSettings(
environment="development", enforce="warn")), *extra, InMemoryProfile()]` with
`CoreModule(job_runner=False)` substituted — the FastAPI default set on in-process defaults, posture
warn-only, no schedulers. ⚠️ ASSUMPTION `CoreModule` has (or 050 gave it) a way to omit the
`core.job_runner` component; if not, this plan adds `CoreModule(job_runner: bool = True)`
(documented: "opt out of the `SCHEDULERS`-phase component; the store stays bound").

**The recipe** (README "Testing a varco app"; `dependency-injection.md` §Testing; the test in Step
12 *is* the recipe):

```python
# conftest.py — providify's own documented override shape (pytest_plugin.py:46-50); NOT a varco fixture
@pytest.fixture(scope="session")
def app_container():
    c = DIContainer(); register_modules(c, in_memory_modules()); return c

@pytest.fixture
def di_container(app_container):        # redefine providify's fixture: fresh copy per test, no re-scan
    c = app_container.copy(); yield c; c.shutdown()

# test_orders.py
async def test_uses_fake_cache(di_container, di_overrides):
    di_overrides.instance(CacheBackend, FakeCache())         # ONE binding swapped; the other fourteen stay on their defaults
    svc = await di_container.aget(OrderService)              # only OrderService + FakeCache are constructed
    ...
```

- ✅ Zero varco fixture: `di_container`/`di_overrides` are providify's names, redefined the way
  providify's own docstring shows (RL-3, `BACKLOG.md:222`); the profile is a `VarcoModule` like any
  other, so `create_varco_app(container, modules=in_memory_modules())` + `TestClient` is the
  integration form with no second API.
- ✅ `instance()` drops every binding of the interface (premise 1) — the fake never ties with the
  `in_memory` default and never appears beside it in `get_all()`; `validate()` on the overridden
  container is clean (a `DEPENDENT` provider satisfies every `Inject[CacheBackend]`).
- ✅ "Fourteen not constructed": providify instantiates lazily; `register()` binds and constructs
  nothing (046); the profile's `validate()` is a pure read.
- ❌ Env leakage: a developer's shell exporting `VARCO_CACHE_BACKEND=redis` makes 047's gate
  select Redis *if `varco_redis` is bootstrapped* — the profile bootstraps only `varco_core`
  (+ whatever `*extra` the test adds), so an unregistered name fails loudly in
  `check_backend_selection()` (047), never silently. The recipe shows `clear_backend_selection(
  monkeypatch)` for tests that add a backend module on purpose. The profile never mutates
  `os.environ` (046's rule).
- Rejected — **`create_test_app(container, ...)`**: ✅ one fewer import in the README; ❌ a second
  app constructor is exactly the "two ways to build an app" 049 collapsed, and it would have to
  mirror every `create_varco_app` kwarg forever. `create_varco_app(container,
  modules=in_memory_modules())` is one line.
- Rejected — **a varco `pytest11` plugin with `varco_container`**: RL-3, and `testkit` is never
  packaged; a second name for providify's fixture is "pure confusion" (CLAUDE.md §Test
  Conventions).
- Rejected — **`VarcoTestProfile.get_overrides() -> dict[ABC, impl]`** (brief 015 §Librarian #4's
  sketch): ✅ Quarkus-literal; ❌ re-implements `ContainerOverrides` with a dict; overrides belong
  to providify's snapshot/restore, not to a module.

### §D-P28 — measure first, then file or record

045 §D-proof predicted (from `container.py:1248` + the `FALLBACK_SHADOWED` emitter) that a
**scanned** `@Fallback @Singleton class Impl(ABC)` shadowed by `bind(ABC, Other)` yields **zero**
`FALLBACK_SHADOWED` INFOs, and told 045 Step 4 to file P28 *if* Step 1(e) confirmed it; 046 then
numbered its descriptor gap "P29 if 045 filed P28, else P28". Step 1 here (a) reads the ledger to
learn what happened, and (b) **re-measures** on the installed 2.1.0 (four lines: register the
scanned class, bind the ABC, `validate(raise_on_error=False)`, count). Outcomes:

| Ledger | Measurement | Action |
|---|---|---|
| P28 filed by 045 | confirmed | Nothing to file; §D-provenance already does not depend on the INFO; the report's docs say so (Step 14); the existing xfail stays |
| not filed | confirmed | File it per CLAUDE.md §"When you hit a providify limitation": `design/upstream-gaps/providify-fallback-shadowed-scanned-class.md` (eight sections, the P27 file's shape; §1 cite `container.py:1248` and the emitter's natural-request line; §2 the 22 scanned-class defaults from 045's site table; §3 the xfail body; §4 the two candidate fixes with ✅/❌ — walk `Impl.__mro__` bases that have another active binding, or record the ABC at `register()`; §5 one INFO per `(base, qualifier)` actually shadowed; §6 partly ours? **No** — but §7 says plainly that varco's report *works around the symptom* from public reads, which is why the ask is `should`, not `must`), one `UPSTREAM-GAPS.md` row (next free number — read the ledger, never assume), a strict xfail `raises=AssertionError` in `varco_core/tests/test_providify_contracts.py`, a CHANGELOG line |
| either | **not** confirmed (2.1.0 emits it) | Record the measurement in the preflight file and in the report docs ("providify reports scanned-class shadowing; the report cross-checks with its own computation") — no filing |

### §D-docs — where each sentence goes (058 assembles; this plan writes the two sections final)

- **`technical_docs/features/dependency-injection.md`** — two new sections, final form:
  **§Diagnostics** (the report's sections and what each answers — brief 015 §Librarian #3's four
  questions mapped to sections; `varco diagnose` synopsis + the three input forms + `--static`
  + exit codes; the startup variant, the four-row `validate`/`strict_validation` table, the
  `VARCO_DIAGNOSE_*` knobs, `app.state.varco_wiring_report`; reading `to_dict()`; the
  provenance vocabulary; the "bound but not wired" findings table; a **Pitfalls** table: "ran
  `varco diagnose` with no Redis → `async_init` failed, use `--static`"; "`--check` in CI without
  `--static` needs the brokers"; "a MISSING_BINDING under `--static` may be an `ainstall()`
  provider"; "deprecation fired at import before the window — only helper sites are ledgered";
  "`validate=False` skips the runtime gate too"; "startup refused on an ERROR that never bit in
  3.x") and **§Testing** (the profile, the recipe verbatim, the conftest redefinition, *why no
  varco fixture* (RL-3), `clear_backend_selection`, integration form with `TestClient`, the
  `instance()`-drops-all-qualifiers note, `copy()` per test). The banner's list of sections
  gains both.
- **`README.md`** — "**varco diagnose**" (synopsis, one text-output excerpt, `--json --check` in
  CI) and "**Testing a varco app**" (the recipe) subsections under the DI section; the
  `create_varco_app` snippet's prose gains one sentence on the startup report + `VARCO_DIAGNOSE_STARTUP_REPORT`.
- **`CLAUDE.md`** — §"DI wiring (providify)": two lines (*"Rule: `varco diagnose` /
  `build_wiring_report()` is the only wiring report — never a second `inspect_*` harness; a new
  wiring fact is a `WiringFinding` there. Rule: tests swap a binding with providify's
  `di_overrides.instance()` on a `varco_core.testing.in_memory_modules()` container — never a varco
  fixture."*); Commands: `varco diagnose --modules … --check` one line; decision tree: one branch
  (*"'What is live and why?' → `varco_core.di.build_wiring_report` / `varco diagnose`; 'is it
  safe?' → `SecurityPosture`; 'swap one binding in a test?' → `varco_core.testing` +
  `di_overrides`"*); the revocation two-step pitfall gains "`varco diagnose` reports it as
  `di.revocation_store_not_wired`". Nothing else — the taxonomy table is 046/058's.
- **`technical_docs/features/security-posture.md`** — the five RO-2 ids in the findings table;
  one paragraph "collectors contributed from `varco_core`" (`CoreModule`).
- **`ARCHITECTURE.md`** — `varco_core.di` listing gains `report.py`/`diagnose.py`;
  `varco_core.testing`; the `varco` CLI verb table gains `diagnose`.
- **`technical_docs/common-pitfalls.md`** — one row: "startup refused with `ConfigurationError:
  validate() reported N ERROR(s)` → read the report, fix the binding; `validate=False` is the
  escape".
- **`CHANGELOG.md` `[Unreleased]`** — `### Added (Plan 052, DI-7 / DI-11 / RO-2)` and
  `### BREAKING — runtime validate() in the lifespan (Plan 052)` (= §Migration). `BACKLOG.md:78,92,110`
  → `done (Plan 052)`.

## Steps

All commands from `/home/edoardo/projects/varco`. Steps 1–3 first, in order; 4–9 build the model,
hooks and driver (TDD each); 10–13 the CLI, startup, RO-2, profile; 14–17 docs, gates, handoff.
`make test PKG=varco_core` / `PKG=varco_fastapi` after every step touching them.

### Preflight and hooks

1. [ ] **Preflight — confirm the landed surface, measure every ⚠️, decide P28.** Record in
       `/tmp/claude-1000/…/scratchpad/052-preflight.txt`:
       (a) providify `2.1.0` installed; re-read every `providify/…:line` in §Context on the
       installed copy; fix drift in this plan first.
       (b) The predecessors' names, from code: `rg -n 'def (inspect_bindings|catalogue|inspect_backend_selection|check_backend_selection|provenance_of|resolve_module_order|phased_components|collection_seams|collect|contribute)\b' varco_core/varco_core/{di,settings}/ varco_core/varco_core/di/*.py`;
       `BindingRecord`'s fields (does it carry `profiles`? if not, add it in Step 4 — one attribute
       read off `b.profiles`); `ModuleRegistry`'s read API and whether `VarcoModule` stores the
       constructor `enabled` tri-state under a readable name (else Step 4 adds a read-only
       `enabled_override` property); `_async_init_all`'s name/signature; whether 049/050's
       `create_varco_app` stashes container/modules on `app.state` (name them); whether 050's
       `VarcoLifespan` exposes `last_runner`; whether `CoreModule` can omit the job-runner
       component; whether 051's `RetentionModule` binds the `RetentionRegistry` it builds; what
       `bind_instance`/`contribute` stamp on the closure (`__name__` vs `__qualname__` — premise 5).
       (c) **P28**: `rg -n 'P28|P29' UPSTREAM-GAPS.md design/upstream-gaps/ CHANGELOG.md` → what
       045/046 filed; then measure: `@Fallback @Singleton class Impl(Base)` via `container.register`,
       `container.bind(Base, Other)` (`Other` a plain `@Singleton`), `validate(raise_on_error=False)`
       → count `FALLBACK_SHADOWED`. Decide the §D-P28 row.
       (d) The deprecation inventory: `rg -n 'DeprecationWarning' varco_*/varco_*` → the direct
       sites (expect 8 files outside `deprecation.py`).
       (e) A default `create_varco_app(DIContainer(), modules=default_modules())` container's
       `validate(raise_on_error=False)` → `errors`/`warnings` counts (expect 0/0 — the index's done
       criterion; if not, list `(kind, owner)` and the owning plan; §D-startup's refusal must not
       fire for the default app).
       (f) Whether `--static` and full mode differ for a `varco_redis`-bootstrapped container with no
       server: count `MISSING_BINDING`s per mode (the §D-driver banner's justification).
       **Verify**: the file has (a)–(f); every ⚠️ ASSUMPTION in §Risks has a measured value.

2. [ ] `varco_core/tests/test_deprecation_ledger.py` (**new, failing first**) → then
       `varco_core/varco_core/deprecation.py` (§D-deprecations): `DeprecationRecord`, `_FIRED`
       + lock, `_note()` before each `warnings.warn`, `fired_deprecations()`,
       `reset_fired_deprecations()`; `__all__` += the three public names. Tests: a `@deprecated`
       function called twice → one record, `count == 2`, `since/removed_in/replacement` populated,
       `first_location` ends with this test file's name; a `deprecated_alias` access → recorded;
       the record is present **even under `warnings.simplefilter("ignore")`**; `reset` empties;
       thread-safety smoke (two threads × 100 calls → `count == 200`).
       **Verify**: green; `uv run python scripts/import_budget.py --check --warn-only` unchanged.

3. [ ] `varco_core/tests/test_settings_bind.py` — add (**failing first**) `test_all_provenance_lists_every_bound_class`
       (two classes bound, one qualified → three records, sorted; empty container → `()`) → then
       `varco_core/varco_core/settings/bind.py`: `all_provenance(container)` (§D-settings-hook);
       re-export in `varco_core/settings/__init__.py` and the lazy map. **Verify**: green.

### The report model and the driver

4. [ ] `varco_core/tests/test_wiring_report.py` (**new, failing first**) — against a container
       built by `varco_core.di.bootstrap(c)` + `CoreModule().register(c)` (no Docker): (a)
       `build_wiring_report(c, validation=c.validate(raise_on_error=False))` returns a
       `WiringReport` whose `seams` has one row per 047 seam, every `source == "default"`,
       `live.fallback is True`, `shadowed == ()`; (b) after `bind_instance(c, AbstractEventBus,
       FakeBus())` → that seam's `source == "code"`, `shadowed` names `InMemoryEventBus`, and this
       holds **whether or not** providify emitted a `FALLBACK_SHADOWED` INFO (assert on the row,
       never on `validation.infos`); (c) `monkeypatch.setenv("VARCO_EVENT_BUS_BACKEND", "memory")`
       → `source == "env"`, `env_value == "memory"`; (d) a user `@Profile("test") @Singleton` bus
       + `c.activate_profile("test")` → `source == "profile"`; (e) `features` rows: `feature_flags`
       `enabled is False`, `source == "default"`; after `enable_feature_flags(c)` → `code`,
       `enabled is True`; with `VARCO_FEATURE_FLAGS_ENABLED=true` (fresh container) → `env`; (f)
       `other_defaults` covers every `catalogue(c).other_defaults` entry; (g) `collections` has one
       row per `collection_seams()`, `PostureCollector.contributors` non-empty (RO-2 — Step 11 may
       land first; if not, assert the row exists); (h) `modules` has `core` with `enabled_source ==
       "default"`, `phase_name == "INFRA"` or the four `components` (050's `CoreModule` shape);
       `CoreModule(enabled=False)` → `enabled_source == "code"`; (i) `settings` has one
       `SettingsStatus` per `all_provenance(c)` record; (j) `validation.ran is True`,
       `errors == ()`; with `validation=None` → `ran is False`; (k) `deprecations` contains a
       record after a `@deprecated` call; (l) `to_dict()` round-trips through `json.dumps`;
       `["catalogue"] == catalogue(c).to_dict()`; `["schema_version"] == 1`; (m) `render()`
       contains every section header and the seam line `event_bus`; (n) `worst()` is `None` for
       the clean container and `Severity.WARNING` after a `WiringFinding` at WARNING is injected
       via the builder's `extra_findings=` kwarg. Then `varco_core/varco_core/di/report.py` per
       §D-model/§D-provenance (+ the `BindingRecord.profiles` addition in `introspect.py` if Step 1
       found it missing; + `VarcoModule.enabled_override` if missing). Every class: full docstring
       + `DESIGN:` block. Lazy-map entries in `varco_core/__init__.py`.
       **Verify**: green; import budget unchanged; `make type-check`.

5. [ ] `varco_core/tests/test_wiring_findings.py` (**new, failing first**) → the finding
       producers in `report.py` (§D-wired): (a) `bind_instance(c, AbstractTokenRevocationStore,
       InMemoryTokenRevocationStore())` + a `TrustedIssuerRegistry` bound **without**
       `revocation_store=` → one `di.revocation_store_not_wired` WARNING whose remediation names
       `TrustedIssuerRegistry(revocation_store=...)`; with the store wired → none; no registry → none;
       (b) `VARCO_MIGRATE_MODE=check` with no `MigrationsModule` in `modules=` → `settings.unowned`
       WARNING; with the module → none (port 049's two cases; `_warn_unowned_settings` deleted from
       `app.py` in Step 10 and its test moved here); (c) a module whose `validate()` returns one
       string → `module.<name>.validate` WARNING with that text; (d) 047's bootstrapped-but-unselected
       case (scan `varco_kafka` with the var unset) → `di.bootstrapped_unselected` WARNING; a typo
       value → `BackendSelectionError` propagates out of the builder (the CLI maps it to exit 1).
       **Verify**: green.

6. [ ] `varco_core/tests/test_diagnose_driver.py` (**new, failing first**) → `varco_core/varco_core/di/diagnose.py`
       (§D-driver): `diagnose(container, modules, *, async_init=True, validate=True)` and
       `run_wiring_preflight(container, *, modules, lifecycle, validate, strict) -> WiringReport`
       (raises `WiringValidationError(RuntimeError)` carrying the report — `varco_fastapi` maps it to
       `ConfigurationError`), plus the deprecation capture window. Tests (Docker-free, modules =
       `in_memory_modules()` once Step 12 lands — until then `[CoreModule()]`): (a) a spy module
       records `register`, `async_init`, `validate` called and **`start` never called**; (b)
       `async_init=False` → `async_init` not called, report banner line present in `render()`,
       `validation.ran is True`; (c) `validate=False` → `validation.ran is False`, module
       `validate()` still called; (d) a module whose `validate()` returns a message → finding at
       WARNING; `run_wiring_preflight(strict=True)` raises `WiringValidationError`; `strict=False`
       does not; (e) a container with a deliberate `MISSING_BINDING` (a `@Singleton` injecting an
       unbound ABC) → `run_wiring_preflight(validate=True)` raises regardless of `strict`; the
       exception message lists the issue's `owner` and `message`; (f) a direct
       `warnings.warn("x", DeprecationWarning)` inside a module's `register()` → captured as a
       `DeprecationRecord` with `since is None`; (g) `lifecycle.components` equals
       `phased_components(active, c)`'s `(name, phase)` list; `realised is None`; (h) the driver
       registers an already-registered module without error (idempotent via `ModuleRegistry`).
       **Verify**: green.

### CLI, startup, RO-2, profile

7. [ ] `varco_core/tests/test_cli_diagnose.py` (**new, failing first**) → `varco_core/varco_core/cli/diagnose.py`
       + one line in `cli/main.py:_build_parser()` (§D-cli). Tests drive `main([...])` like
       `test_cli_asyncapi*.py` does, with a `tmp_path` module exposing `modules()` (returns
       `in_memory_modules()`), `container()` (returns a bootstrapped container) and, in
       `varco_fastapi/tests/test_cli_diagnose_app.py`, `app` (a `create_varco_app(...)` object):
       (a) `--modules mod:modules` → exit 0, stdout has "Seams" and `event_bus`; (b) `--json` →
       stdout parses, `["schema_version"] == 1`; (c) `--check` on a clean profile → 0; with a
       module returning a `validate()` message → 1 and the message on stdout; without `--check` the
       same input → 0; (d) `--static` → banner present; (e) no source → 2, message on stderr;
       `--modules mod` (no colon) → 2; `--modules mod:not_a_list` → 2; (f) `mod:raising` whose
       `async_init` raises → 1, one-line stderr, no traceback unless `--verbose`; (g) `--container
       mod:container` works; (h) `--app` reads `app.state.varco_*` (varco_fastapi test); (i)
       `varco --help` lists `diagnose`; (j) `--path` prepends. **Verify**: green.

8. [ ] `varco_fastapi/tests/test_startup_wiring_report.py` (**new, failing first**) →
       `varco_fastapi/varco_fastapi/app.py` (§D-startup): the `setup=` coroutine calls
       `run_wiring_preflight` after `_async_init_all`, maps `WiringValidationError` →
       `ConfigurationError`, stores `app.state.varco_wiring_report`, `app.state.varco_container`,
       `app.state.varco_modules` (or 049's names); `_warn_unowned_settings` deleted (its logic moved
       in Step 5); `varco_fastapi/varco_fastapi/diagnose.py` (**new**): `wiring_report(app) ->
       WiringReport` (re-renders with `LifecycleRunner.plan`/`last_failures` when
       `app.state.varco_lifespan.last_runner` exists). `DiagnoseSettings` in
       `varco_core/varco_core/di/report.py`, added to 048's `bind_core_settings()` list; regenerate
       `technical_docs/settings-reference.md` + snapshot (`make settings-reference` or 048's
       target name). Tests: (a) `with TestClient(app)` on a default app → `caplog` has one INFO
       record from `varco_core.di.diagnose` containing "varco wiring report" and "Validation: 0
       errors"; (b) `VARCO_DIAGNOSE_STARTUP_REPORT=false` (monkeypatch, fresh container) → no such
       record; a WARNING finding is *still* logged at WARNING; (c) `startup_report_json=true` → the
       record's message parses as JSON; (d) an app whose container carries a `MISSING_BINDING` →
       `TestClient(app)` raises `ConfigurationError` naming the owner; `validate=False` → boots;
       (e) a WARNING-only issue → boots with `strict_validation=False`, raises with `True`; (f)
       `app.state.varco_wiring_report.lifecycle.realised is None` before start and
       `wiring_report(app).lifecycle.realised` equals the started component names after the client
       entered; (g) the report is built **before** the first `start()` — a spy component asserts
       `app.state.varco_wiring_report` is set when its `start()` runs; (h) the existing
       `test_app_factory*`/`test_middleware_order` suites stay green unchanged (no construction
       change). **Verify**: `make test PKG=varco_fastapi`.

9. [ ] `varco_core/tests/test_posture_collectors_ro2.py` (**new, failing first**) →
       `varco_core/varco_core/authority/posture.py` + `varco_core/varco_core/retention/posture.py`
       (§D-RO2 classes; `__all__` += each) + `CoreModule._register()` contributions (+ the
       `RetentionModule.async_init()` `bind_instance` line iff Step 1(b) found the registry unbound).
       Tests with **fake registries** (a `TrustedIssuerRegistry` stub exposing `_entries` with one
       `JwksUrlSource` entry and `_refresh_task = None`; a `RetentionRegistry` built from two
       policies, one `tenant_ids=None`): (a) `JwksPostureCollector(lambda: reg).collect()` → one
       `auth.jwks_refresh_off` WARN; after `start_refresh()` semantics (`refresher_running=True`
       via the stub) → none; `lambda: None` → `[]`; (b) `RetentionPostureCollector(lambda:
       reg).collect()` → `retention.platform_wide` WARN naming the policy, `retention.dry_run_only`
       INFO when all dry-run; `lambda: None` → `[]`; (c) `CoreModule().register(c)` then
       `collect(c, PostureCollector)` contains both by `name`; `CoreModule(enabled=False)` → neither;
       (d) `SecurityPostureLifecycle(collectors=[JwksPostureCollector(...)])` runs it (051's
       `PostureCollector` acceptance) and the finding appears in `.report`; (e) the
       `varco_core` `di_wiring` subclass gains `test_ro2_collectors_are_contributed`. Update
       `varco_fastapi/tests/test_security_posture*.py` counts if a test asserts the number of
       collectors. **Verify**: `make test PKG=varco_core`, `PKG=varco_fastapi`.

10. [ ] `varco_core/tests/test_testing_profile.py` (**new, failing first**) →
        `varco_core/varco_core/testing.py` + `varco_fastapi/varco_fastapi/testing.py`
        (§D-testprofile; `CoreModule(job_runner=)` iff Step 1(b) found it missing). Tests, all
        Docker-free: (a) `register_modules(DIContainer(), in_memory_modules())` → every 047 seam
        `source == "default"`, `validate(raise_on_error=False).errors == () and .warnings == ()`,
        `InMemoryProfile().validate(c) == ()`; (b) `phased_components(...)` has **no**
        `Phase.SCHEDULERS` component; (c) with `varco_kafka.di.bootstrap` added via a `KafkaModule()`
        extra and `VARCO_EVENT_BUS_BACKEND=kafka` → `InMemoryProfile.validate()` returns one message
        naming the seam (the profile *asserts* the shape, never fixes it); (d) `clear_backend_selection(monkeypatch)`
        removes the six + three names; (e) `varco_fastapi.testing.in_memory_modules()` contains
        `SecurityPostureModule` with `enforce == "warn"` and `environment == "development"`, no
        `JwksRefreshModule`/`RetentionModule`; `create_varco_app(DIContainer(), modules=...)` +
        `TestClient` boots and `/health` answers; (f) `test_modules_never_scanned.py` (049) still
        passes — `InMemoryProfile` carries no providify marker. **Verify**: green.

11. [ ] `varco_core/tests/test_single_override_recipe.py` (**new**) — the recipe verbatim
        (§D-testprofile): a module-local `conftest`-style `app_container` fixture (session-scoped,
        `register_modules(c, in_memory_modules())`), a **redefined `di_container`** yielding
        `app_container.copy()`, then `test_uses_fake_cache(di_container, di_overrides)`:
        `di_overrides.instance(CacheBackend, FakeCache())`; `aget(OrderService)` (a local
        `@Singleton` injecting `Inject[CacheBackend]` and `Inject[AbstractEventBus]`) → its cache
        **is** the fake; an instantiation-counting `@Singleton` bound but never injected is **not**
        constructed; `get_all_bindings(CacheBackend)` has exactly one binding (the fake —
        `instance()` dropped the qualified default too); `validate(raise_on_error=False).errors ==
        ()`; a second test in the same module sees `InMemoryCache` again (restore worked). Docstring:
        "this test is the README recipe; keep them identical". Mention in the module docstring that
        `test_providify_pytest_plugin.py` is the fixtures' own contract test. **Verify**: green.

12. [ ] `testkit/varco_conformance/di_wiring.py` — add `test_diagnose_runs_clean_on_bootstrapped_package`:
        `report = build_wiring_report(container, validation=container.validate(raise_on_error=False))`
        → `report.worst() is None` (no ERROR/WARNING from either providify or varco findings, modulo
        `known_warnings`); every `(interface, qualifier)` in `expected_defaults` appears in
        `seams`/`features`/`other_defaults` with `source == "default"` (or `"env"` for the package's
        `selectable` rows under its conftest fixture); `json.dumps(report.to_dict())` succeeds. Ten
        subclasses inherit it; `COVERAGE.md`'s `di_wiring` row mentions the new test.
        **Verify**: `make test` (all eleven suites).

13. [ ] **P28 filing or record** per §D-P28's outcome table (report file, ledger row, strict
        xfail in `test_providify_contracts.py`, CHANGELOG line — or the preflight note + a sentence
        in the docs). **Verify**: `rg -c '^\| \*\*P2' UPSTREAM-GAPS.md` matches the decision;
        `uv run pytest varco_core/tests/test_providify_contracts.py -q -rxX` → `0 xpassed`.

### Docs, changelog, gates

14. [ ] Docs per §D-docs: `dependency-injection.md` §Diagnostics + §Testing (final), README's two
        subsections, CLAUDE.md's lines/branch/command, `security-posture.md`, `ARCHITECTURE.md`,
        `common-pitfalls.md` row, `varco_core/cli/main.py` module docstring (lists `diagnose`),
        `docs/` nav if `technical_docs/features/*.md` are listed. **Verify**: `rg -n 'varco diagnose'
        README.md CLAUDE.md technical_docs ARCHITECTURE.md` ≥ 6 hits; `make docs`.

15. [ ] `CHANGELOG.md` `[Unreleased]`: `### Added (Plan 052, DI-7 / DI-11 / RO-2)` (report model,
        `varco diagnose`, startup report + `VARCO_DIAGNOSE_*`, `all_provenance`, the deprecation
        ledger, two RO-2 collectors + five ids, `varco_core.testing`/`varco_fastapi.testing`,
        `app.state.varco_*`, the `di_wiring` test; P28 line if filed); `### BREAKING — runtime
        validate() in the lifespan (Plan 052)` = §Migration. `BACKLOG.md:78,92,110` → `done (Plan
        052)`; the index's row 052 unchanged. **Verify**: `rg -n 'Plan 052' CHANGELOG.md BACKLOG.md` ≥ 4.

16. [ ] **Gates.** `uv run python scripts/api_surface.py` (new top-level names in
        `varco_core.__all__`: `build_wiring_report`, `WiringReport`, `DiagnoseSettings`,
        `fired_deprecations`, `all_provenance`, `InMemoryProfile`, `in_memory_modules`,
        `JwksPostureCollector`, `RetentionPostureCollector` — additions are notes, never failures;
        commit the snapshot in this step); 048's settings snapshot regenerated (Step 8); 047's
        catalogue snapshot **unchanged** (no new default; `make di-catalogue-check` green);
        `make lint` (no-`PKG`: `api-check`, `asyncapi-check`, `import-budget --warn-only`,
        `settings-reference-check`, `di-catalogue-check` all green); `make type-check`; `make test`
        (eleven suites, `0 xpassed`, `xfailed` = P28 iff filed).

17. [ ] **Handoff paragraph** appended to `plans/000-index-4-0-di-standardization.md` row 052:
        **054** — the report's `enforce`/`enforce_source` fields are where the flipped default
        becomes visible; nothing else to touch. **058** — §Diagnostics/§Testing are final;
        §Migration below is the DI-7 entry; the `varco_core.testing` module is the "Testing" chapter's
        source; if P28 was filed, list it under open gaps.

## Edge cases

- **`--modules` callable returns a container instead of a list** → exit 2 with "use
  `--container` for a callable that returns a DIContainer".
- **Two module instances of one class** in the list → 049's `ValueError` → exit 1, message verbatim.
- **`async_init` raises for one module** → exit 1; stderr `"<ModuleName>.async_init failed: <exc>"`;
  the container is still `ashutdown()` in `finally`.
- **`BackendSelectionError` (typo)** → exit 1; the message already lists known values (047).
- **A module's `validate()` raises** (not returns) → the exception propagates (a bug in the module,
  not a finding); the CLI reports exit 1 naming the module.
- **`container.validate()` raises `ConditionEvaluationError`** (a raising `@Requires` predicate,
  047 §Context) → propagates; exit 1; the startup variant lets it abort startup as today.
- **No `ModuleRegistry` bound** (`--container` from a hand-wired app) → Modules section empty with
  the line "no VarcoModule registered — container was wired by hand"; every other section works.
- **A seam with no binding at all** (`--container` with only `varco_fastapi` scanned) →
  `source == "absent"`, `live is None`; not a finding by itself (providify's `MISSING_BINDING`
  reports it if anything injects it).
- **A shadowed fallback with an already-cached singleton** (045's not-evicted caveat) → the report
  shows the *binding* that wins now; a footnote is added when `container` has a cached instance for
  a shadowed fallback's implementation (⚠️ only if a public read exists — else omitted; Step 1(b)).
- **`InMemoryProfile` with `enabled=False`** → registers nothing, validates nothing; the report
  lists it as disabled (source `code`).
- **Report built twice on one container** → equal `to_dict()` (no accumulated state; the
  deprecation ledger may grow between calls — `deprecations` is documented as "since process
  start").
- **`VARCO_DIAGNOSE_STARTUP_REPORT=false` and an ERROR** → the refusal still happens and the ERROR
  lines are still logged; only the rendered report is suppressed.
- **`app.state.varco_wiring_report` read before the lifespan ran** → `AttributeError`;
  `wiring_report(app)` raises `RuntimeError("lifespan has not run")`.

## Verification

```bash
# Preflight
uv run python -c "import importlib.metadata as m; print(m.version('providify'))"   # 2.1.0
rg -n 'P28|P29' UPSTREAM-GAPS.md design/upstream-gaps/                              # the numbering decision
rg -n 'DeprecationWarning' varco_*/varco_* | rg -v deprecation.py | wc -l           # the direct sites

# New tests
uv run pytest varco_core/tests/test_deprecation_ledger.py varco_core/tests/test_settings_bind.py -q
uv run pytest varco_core/tests/test_wiring_report.py varco_core/tests/test_wiring_findings.py \
              varco_core/tests/test_diagnose_driver.py varco_core/tests/test_cli_diagnose.py -q
uv run pytest varco_core/tests/test_posture_collectors_ro2.py varco_core/tests/test_testing_profile.py \
              varco_core/tests/test_single_override_recipe.py -q
uv run pytest varco_fastapi/tests/test_startup_wiring_report.py varco_fastapi/tests/test_cli_diagnose_app.py -q
uv run pytest varco_core/tests/test_providify_contracts.py -q -rxX                  # 0 xpassed

# The CLI itself, against the example app
uv run varco diagnose --path examples/00-full-stack-post-api --modules example.app:modules --static
uv run varco diagnose --path examples/00-full-stack-post-api --modules example.app:modules --static --json --check; echo $?

# Gates
uv run python scripts/api_surface.py            # then commit the snapshot
make lint && make type-check && make test && make docs
uv run python scripts/import_budget.py --check --warn-only
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — 047's `BindingRecord` lacks `profiles` | `source == "profile"` is computable | Step 1(b); Step 4 adds one attribute read off `b.profiles` (`binding.py:202,716`) |
| ⚠️ ASSUMPTION — `VarcoModule` exposes the constructor `enabled` tri-state readably | `enabled_source` is honest | Step 1(b); a read-only property added in Step 4 if not |
| ⚠️ ASSUMPTION — 049/050's `create_varco_app` does not already stash container/modules on `app.state` | One name per fact | Step 1(b) reads; reuse 049's names if present |
| ⚠️ ASSUMPTION — `CoreModule` can omit its `SCHEDULERS` component | `in_memory_modules()` plans no scheduler | Step 1(b); `CoreModule(job_runner=)` added in Step 10 if not |
| ⚠️ ASSUMPTION — 051's `RetentionModule` binds the registry it builds | `RetentionPostureCollector`'s supplier finds it | Step 1(b); one `bind_instance` line in Step 9 if not |
| ⚠️ ASSUMPTION — `bind_instance`/`contribute` closures are labelled `@Provider(_factory)` by `validate()` (premise 5) | Report rows key on `(interface, qualifier)`, never on `owner`; `owner` is display text | Step 1(b) records; if 046 stamped `__name__`, nothing changes |
| ⚠️ ASSUMPTION — a default `create_varco_app` container validates with 0 ERROR/0 WARNING (index done-criterion) | §D-startup's refusal never fires for the default app | Step 1(e) measures; any residue is a plan-attributed `known_warnings`-style allowlist **in the test only**, and the refusal is not shipped until the residue is 0 ERROR |
| ⚠️ ASSUMPTION — the P28 blind spot's status in the ledger | The report never depends on `FALLBACK_SHADOWED` for correctness | §D-provenance computes shadowing from `inspect_bindings()`; Step 1(c) decides the filing |
| ⚠️ ASSUMPTION — `--static` vs full mode differ only by `ainstall()`-registered providers | The banner's claim is true | Step 1(f) measures on `varco_redis` |
| The refusal-on-ERROR breaks an app that booted on 3.x with a dangling binding | An ERROR is providify's "guaranteed to fail at resolution" (`validation.py:44-45`) | §Migration names `validate=False`; CHANGELOG BREAKING block |
| Startup log volume — one multi-line INFO record per boot | Problem lines are also emitted individually; the rendered block is one record | `VARCO_DIAGNOSE_STARTUP_REPORT=false`; `startup_report_json` for shippers |
| Deprecation ledger is process-global | Records only; reset only in tests | `reset_fired_deprecations()` documented as test-only; the ledger holds no references to caller objects |
| `di_overrides.instance()` dropping the `in_memory`-qualified default surprises a test that later asks `get(CacheBackend, qualifier="in_memory")` | Documented in §Testing's Pitfalls; `remove(qualifier=)`/`bind()` are the finer tools (`testing.py:194-212`) | Recipe test asserts exactly one binding remains |
| Two RO-2 collectors from `CoreModule` change `SecurityPosture`'s collector count in existing tests | ids stable; counts may move | Step 9 updates any count assertion; `security-posture.md` lists eight collectors |
| `api_surface --check` fails on the new names | Additions are notes; the snapshot is regenerated in Step 16, same commit | Step 16 |

## Migration (3.x → 4.0, DI-7 / DI-11 / RO-2 entry — Plan 058 assembles)

**BREAKING — `create_varco_app(validate=True)` now also runs `container.validate()` inside the
lifespan, after every module's `async_init()` and before the first `start()`.** An `ERROR`-severity
issue (`MISSING_BINDING`, `AMBIGUOUS_BINDING`, `CIRCULAR_DEPENDENCY`, `SCOPE_LEAK`, `LIVE_REQUIRED`,
`UNRESOLVED_ANNOTATION`) refuses startup with `ConfigurationError` listing each issue. With
`strict_validation=True`, `WARNING` issues refuse too. An app that booted on 3.x with a latent
binding error that never bit at runtime must fix the binding or pass `validate=False`. INFO issues
(`FALLBACK_SHADOWED`, `CONDITION_INACTIVE`) never block.

**New** — every lifespan logs a wiring report at INFO from logger `varco_core.di.diagnose`;
`VARCO_DIAGNOSE_STARTUP_REPORT=false` silences the block (never the problem lines);
`VARCO_DIAGNOSE_STARTUP_REPORT_JSON=true` emits one JSON line instead. `app.state.varco_wiring_report`,
`app.state.varco_container`, `app.state.varco_modules` are set by `create_varco_app`.

**New** — `varco diagnose` (built-in verb); `varco_core.di.build_wiring_report`/`WiringReport`;
`varco_core.settings.all_provenance`; `varco_core.deprecation.fired_deprecations`;
`varco_core.testing` (`InMemoryProfile`, `in_memory_modules`, `register_modules`,
`clear_backend_selection`) and `varco_fastapi.testing.in_memory_modules`.

**New posture ids (RO-2)** — `auth.jwks_refresh_off` (WARN), `auth.jwks_keysets_unloaded` (INFO),
`retention.platform_wide` (WARN), `retention.dry_run_only` (INFO), `retention.short_retention`
(INFO); `VARCO_SECURITY_SUPPRESS` applies as to any id. Contributed by `CoreModule`; an app with
`modules=` that excludes `CoreModule` gets neither.

**Removed** — `create_varco_app`'s build-time `VARCO_MIGRATE_MODE`/`VARCO_TENANCY_ISOLATION`
"set but unowned" log warnings are now report findings (`settings.unowned`) at the same severity —
same text, new place.

## Parked

- **An HTTP endpoint for the report** (`/actuator/conditions`, brief 010 §4). Trigger: a second
  consumer beyond logs and CLI (a dashboard, a fleet tool). `app.state.varco_wiring_report` +
  `to_dict()` make it a ten-line app-side route today; shipping it means an auth decision
  (`mount_*` rules) this plan should not make.
- **A snapshot/`--check` diff for `varco diagnose --json`** — environment-dependent by design;
  047's catalogue is the environment-independent gate.
- **A "realised lifecycle order" section in the *pre-start* report** — impossible by construction;
  `varco_fastapi.diagnose.wiring_report(app)` is the post-start read.
- **Converting the eight direct `DeprecationWarning` sites** — 058's re-label sweep.

## Done criteria

- [ ] `build_wiring_report()` returns a frozen `WiringReport`; `to_dict()` is JSON-safe with
      `schema_version: 1` and embeds 047's catalogue verbatim; `render()` prints every section.
- [ ] Provenance per row is one of `code`/`profile`/`env-dotted`/`env`/`default`/`absent` and is
      computed from public reads (+047's single private walk); shadowing is correct for
      scanned-class defaults with or without `FALLBACK_SHADOWED`.
- [ ] `all_provenance()` and `fired_deprecations()` exist with tests; the driver captures direct
      `DeprecationWarning` sites during bootstrap.
- [ ] `varco diagnose` accepts `--modules`/`--container`/`--app`, honours `--static`/`--json`/
      `--check`, exits `0/1/2` as specified, never calls `start()`.
- [ ] Every `create_varco_app` lifespan validates after `async_init`, before the first `start()`;
      the four-row `validate`/`strict_validation` table is test-pinned; the report is logged at INFO
      with the env opt-out; `app.state.varco_wiring_report` is set.
- [ ] `JwksPostureCollector`/`RetentionPostureCollector` are contributed by `CoreModule`, run inside
      `SecurityPosture`, with the five ids documented in `security-posture.md`.
- [ ] `varco_core.testing.in_memory_modules()` boots with zero Docker and `validate()` clean; the
      single-override recipe test passes using providify's `di_container`/`di_overrides` unwrapped.
- [ ] The `di_wiring` suite's `test_diagnose_runs_clean_on_bootstrapped_package` is green for all
      ten packages.
- [ ] P28 measured; filed (report + ledger + strict xfail) or recorded as closed.
- [ ] `dependency-injection.md` §Diagnostics/§Testing final; README, CLAUDE.md, ARCHITECTURE.md,
      `security-posture.md`, pitfalls, CHANGELOG, BACKLOG (`DI-7`, `DI-11`, `RO-2` → done) updated;
      `api_surface` + settings snapshots regenerated; `make lint`/`type-check`/`test`/`docs` green.
