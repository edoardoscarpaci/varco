# Plan 053 — Credential and authorization default flips (FLIP-5, FLIP-034-1, FLIP-034-2, FLIP-034-3, FLIP-034-4)

Covers BACKLOG 4.0 rows **`FLIP-5`** (`BACKLOG.md:69` — *"`BaseAuthorizer` refuses to bind outside
`development`"*), **`FLIP-034-1`** (`:84` — *"`ApiKeyAuth(keys=...)` plaintext path **deprecated**
(`since="4.0.0", removed_in="5.0.0"`), `hashed_keys=` the documented path"*), **`FLIP-034-2`**
(`:85` — *"`JwtAuthority`-minted tokens carry `jti` by default"*), **`FLIP-034-3`** (`:86` —
*"`revocation_require_jti` defaults `True` when a non-Null store is wired"*) and **`FLIP-034-4`**
(`:73` — *"`PassthroughAuth` requires explicit `acknowledge_unverified=True`"*). Flip track, slice
row 053 (`plans/000-index-4-0-di-standardization.md:32`): independently mergeable, **after Plan
045** only — FLIP-5 rides the `@Fallback` `BaseAuthorizer` binding 045 declares.

**Neighbours (do not absorb):** Plan 054 owns `FLIP-2` (`enforce="refuse"` in production) — this
plan emits findings and never touches `enforce`; Plan 051 moved the posture collectors to
`varco_core.posture.PostureCollector` subclasses — this plan targets that shape; Plan 052 owns the
`WiringReport`/startup `validate()` and the deprecation ledger — this plan's findings and warnings
ride them without editing them; Plan 058 assembles this plan's §Migration into the one 3.x → 4.0
guide and re-labels the nine pre-existing `removed_in="4.0.0"` sites (not this plan's).

**Design of record:** `plans/036-authorization-surface-and-posture.md` §D-S9-flip row 5 (`:554`),
§D-S9-oq1 (`:241-277`, `VARCO_SECURITY_ENV` — explicit, default `production`, presentation only);
`plans/034-credential-and-token-lifecycle.md` flip list #1–#4 (`:885-892`), §D-S13-jti (`:368-389`,
the rejected-in-3.2 "require_jti=True when a store is bound" alternative at `:386-389`);
`design/research/009-token-revocation-and-credential-storage.md` §2 (jti availability per IdP —
Auth0/Keycloak/Cognito ship none by default), §5 (skew); `design/research/010-default-implementation-di-conventions.md`
§3 (*"for authorization, the default should refuse to start if no authorizer is configured"*);
`CONTRIBUTING.md:91-105` (removal window, 12-month floor). providify **2.1.1** semantics cited from
`/home/edoardo/projects/providify` (`pyproject.toml:3`), never from its README.

> ⚠️ **Five premises corrected from source while writing this plan; each reshapes a §D block.**
> (1) The task brief's `TrustedIssuerRegistry(revocation_store=..., revocation_require_jti=...)`
> does not exist — the constructor takes only `revocation_store` (`varco_core/varco_core/authority/registry.py:186-192`);
> `revocation_require_jti` is a **`JwtVerificationSettings` field** (`varco_core/varco_core/jwt/config.py:111`,
> env `VARCO_JWT_REVOCATION_REQUIRE_JTI`) plus a **per-call `verify()` kwarg** (`registry.py:776`),
> merged in `_check_revocation()` (`registry.py:1017-1019`). The computed default therefore lives in
> the settings field's `None` sentinel and is resolved in `_check_revocation()` (§D-require-jti).
> (2) `BaseAuthorizer`'s default is a **scanned `@Singleton` class** (`varco_core/varco_core/auth/authorizer.py:65-66`;
> post-045 `@Fallback @Singleton`), not a `VarcoFastAPIModule` provider — 045 premise (1) already
> said so; the gate goes on the class. (3) `container.bind(AbstractAuthorizer, BaseAuthorizer)` (what
> `varco_fastapi/tests/test_posture_collectors.py:111` does) builds a `ClassBinding` that reads
> `@Requires`/`@Fallback` **off the implementation class** (`providify/binding.py:207,212`, per 045/047)
> — so a class-keyed explicit bind is gated exactly like the scanned one; only an *instance*/provider
> binding is not (§D-flip5-escape). (4) varco's four `acknowledge_*=True` precedents raise
> **`ValueError`** (`varco_core/tls/install.py:150-154`, `varco_fastapi/webhook/mount.py:73-75`,
> `varco_fastapi/admin/mount.py:103-105`, `varco_fastapi/tenancy/mount.py:20`); FLIP-034-4 uses
> **`TypeError`** deliberately and says why (§D-passthrough). (5) providify's `validate()` reports a
> seam whose only binding is `@Requires`-inactive as **`MISSING_BINDING` (`ERROR`)** at every eager
> injection point *plus* one `CONDITION_INACTIVE` (`INFO`) for the binding — never as a single
> "unresolvable" kind; the ERROR's message literally says *"no binding is registered"*
> (`providify/container.py:6673-6697`) and the INFO beside it is what names `VARCO_SECURITY_ENV`
> (§D-flip5).

## Goal

After this plan: in any process where `VARCO_SECURITY_ENV` is not exactly `development`, varco's
permissive `BaseAuthorizer` is **not a binding at all** — `container.get(AbstractAuthorizer)` raises
`LookupError`, `validate()` reports `MISSING_BINDING` at every service that injects it (so 052's
startup preflight refuses), and `SecurityPosture` reports `posture.authorizer_unbound` at `HIGH`;
every `JwtAuthority`-minted token carries a uuid4 `jti` unless the authority was constructed with
`mint_jti=False`; a `TrustedIssuerRegistry` wired to a non-`Null` revocation store rejects a
`jti`-less token at `TOKEN` scope unless `VARCO_JWT_REVOCATION_REQUIRE_JTI=false`; `ApiKeyAuth(keys=...)`
emits one `DeprecationWarning` (`since="4.0.0", removed_in="5.0.0"`) attributed to the caller and
`hashed_keys=` is the only documented path; `PassthroughAuth()` is a `TypeError` and
`PassthroughAuth(acknowledge_unverified=True)` is the only way to construct one. Each flip has
exactly one greppable escape hatch, a posture id, a §Migration row, and tests for default,
escape hatch, and posture.

## Non-goals

- **`SecurityPostureSettings.enforce`** — 054's FLIP-2. This plan never reads or changes it; its
  findings refuse startup only through 054's flip (or an app's own `enforce="refuse"`).
- **Removing `ApiKeyAuth(keys=...)`** — 5.0.0, after the 12-month floor. This plan starts the
  window; nothing is deleted.
- **A `jti` on `JwtBuilder().encode()`** (no authority) — the row says *`JwtAuthority`-minted*;
  a bare builder is the caller's own object and is unchanged.
- **Auto-wiring the revocation store into the registry** (the §D-S13-di two-step) — 052's
  `di.revocation_store_not_wired` finding reports it; 050/049 own wiring. This plan only changes
  what `require_jti` means once a store *is* wired.
- **Any change to `SecurityPostureSettings`** (a `Literal` type on `environment`, a new field) —
  048 owns settings; `VARCO_SECURITY_ENV` keeps its 036 shape and `str` type.
- **A second environment variable for FLIP-5** — §D-flip5-escape says why not.
- **The nine pre-existing `removed_in="4.0.0"` deprecation sites** — 058 re-labels them.
- **Converting the ~35 in-repo test call sites that construct `ApiKeyAuth(keys=...)` incidentally**
  is **in** scope (§D-apikey); converting `router/mcp.py`'s two *docstring* snippets (`:371`,
  `:867`) is a docs edit, listed in §Docs.

## Context (verified from source, 2026-09-17)

| Fact | Where | Why it matters |
|---|---|---|
| `BaseAuthorizer` — `@Singleton(priority=_FALLBACK_PRIORITY)`, `-(2**31)`; module docstring argues *for* permissive-by-default and suggests a hand-written production `assert` | `varco_core/varco_core/auth/authorizer.py:62-66`; docstring `:8-45`, the `assert` `:38-39`; class docstring `:74-78` | 045 makes it `@Fallback @Singleton` and deletes the constant; this plan adds the gate and rewrites both docstrings — the hand-written `assert` becomes what the framework does |
| Every `AsyncService` is an eager injection point: `authorizer: Inject[AbstractAuthorizer]` | `varco_core/varco_core/service/base.py:294` (also `:79,:105,:279`) | `validate()` pass 2 reports `MISSING_BINDING` (`ERROR`) for each DI-registered service in production with no app authorizer → 052's `run_wiring_preflight()` raises `ConfigurationError` before any `start()` |
| `enable_authorization_audit()` resolves the live `AbstractAuthorizer` eagerly | `varco_core/varco_core/auth/di.py:82` | In production with no app authorizer this now raises `LookupError` at wiring time — correct (auditing a permissive authorizer is meaningless); §Edge cases |
| `SecurityPostureSettings.environment` ← `VARCO_SECURITY_ENV`, default `"production"`, type `str`; only the exact string `"development"` demotes WARN→INFO; `enforce="refuse"` blocks on `HIGH` anywhere and on `WARN` in `production` | `varco_fastapi/varco_fastapi/posture.py:154-196` (field `:187-190`), demotion `:351-354`, refuse `:378-393` | The same literal `"development"` is what `@Requires(env=, value=)` compares against — one variable, two readers, one spelling; `HIGH` is never demoted, so `posture.authorizer_unbound` blocks in every environment under `refuse` |
| `posture.base_authorizer_bound` — `container.get(AbstractAuthorizer)` inside `try`, any exception → `NOT_ASSESSED`; `type(x) is BaseAuthorizer` → WARN; remediation still says "priority higher than -(2**31)" | `varco_fastapi/varco_fastapi/posture.py:743-769` (pre-051 `_collect_local`; post-051 `LocalPostureCollector.collect()`, body unchanged per 051 §D-posture) | The `except Exception` must split `LookupError` (→ new `HIGH` finding) from everything else (→ `NOT_ASSESSED`); the remediation is rewritten (049 §D-posture already noted it is stale) |
| `_collect_auth` — revocation block: `auth.revocation_registry_unwired` (WARN), `auth.revocation_unbound` (INFO), `auth.revocation_fail_open` (HIGH); nothing reports `token_scope_usable` | `posture.py:547-580` (post-051 `AuthPostureCollector`) | One new INFO id for the explicit opt-out of the derived `require_jti` (§D-require-jti) |
| `ApiKeyAuth.__init__` — XOR on `keys`/`hashed_keys`; plaintext hashed at construction; `_constructed_from_plaintext` flag read by `inspect_auth_posture()` | `varco_fastapi/varco_fastapi/auth/server_auth.py:408-465` (hashing `:439-442`, flag `:465`); `auth/posture.py:146` | The deprecation warning goes right after the XOR check; the flag and the `auth.api_key_plaintext` WARN are unchanged |
| `PassthroughAuth.__init__(self, *, required: bool = False)`; docstring warns "Never use this on public-facing endpoints" | `server_auth.py:522-548` | The acknowledgement kwarg is added keyword-only; the docstring's warning becomes the constructor's contract |
| `inspect_auth_posture()` records `passthrough_auth_bound` by `isinstance` | `varco_fastapi/varco_fastapi/auth/posture.py:150-151` | Unchanged — an acknowledged `PassthroughAuth` is still reported (`auth.passthrough_bound`, WARN) |
| `JwtAuthority.token()` → `JwtBuilder().issuer(self._issuer)`; `sign()` uses the builder "as-is — it is NOT overwritten here"; `__init__`/`from_pem` keyword-only | `varco_core/varco_core/authority/jwt_authority.py:267-287`, `:289-323` (contract `:298-300`), `:125-160`, `:164-246` | `token()` is the seeding point (it already seeds `iss`); `sign()` keeps its contract; the kwarg lands on both constructors |
| `MultiKeyAuthority.token()`/`sign()` delegate to the active `JwtAuthority` | `varco_core/varco_core/authority/multi_key_authority.py:148-171` | Inherits the flip per authority; no kwarg of its own |
| `JwtBuilder.token_id(jti)` / `with_random_jti()` (uuid4) / `build()` | `varco_core/varco_core/jwt/builder.py:203-227`, `:341-367` | `with_random_jti()` is the minting primitive; a later `token_id()` wins (last write) — the per-token override |
| `JwtVerificationSettings.revocation_require_jti: bool = False`; docstring cites brief 009 §2 | `varco_core/varco_core/jwt/config.py:111`, `:69-76`; `from_env()` read per call at `registry.py:1016` | The flip site: `bool \| None = None` |
| `_check_revocation()` — runs only when `self._revocation_store is not None`; `effective_require_jti = require_jti if not None else settings.revocation_require_jti`; `jti is None` → `TokenRevokedError(reason="revocation_require_jti=True and token has no jti claim")` | `registry.py:956-972`, `:977-1032` (merge `:1017-1019`, check `:1026-1032`) | Where the derived default is computed; the `reason` string is kept greppable |
| `NullTokenRevocationStore` — `@Singleton(priority=-sys.maxsize - 1)` (045 → `@Fallback @Singleton`) | `varco_core/varco_core/revocation/null.py:35-36` | "Non-Null" is `not isinstance(store, NullTokenRevocationStore)` — already the predicate `inspect_revocation_posture()` uses |
| `inspect_revocation_posture()` — `store_bound`/`registry_wired` both via `isinstance(…, NullTokenRevocationStore)`; `require_jti`/`token_scope_usable` copy the settings bool | `varco_core/varco_core/revocation/posture.py:105-128` (isinstance `:107-111`, jti `:126-127`) | Gains the derived computation and one additive field; the two isinstance sites collapse onto one helper |
| `varco_core.deprecation` — `deprecated()` (functions/classes), `deprecated_alias()`; `_build_message()` is the one canonical sentence; `_CALLER_STACKLEVEL = 2` | `varco_core/varco_core/deprecation.py:75-97`, `:100-176`, `:266-347`, `:72`; `__all__` `:65` | Neither helper can deprecate a *parameter path*; a third, `warn_deprecated()`, reuses `_build_message()` (§D-apikey). 052 §D-deprecations adds `_note()` to the helper — the new function calls it too when present |
| pytest `filterwarnings` ignore lists per package; no `error` rule | e.g. `varco_fastapi/pyproject.toml:136-140` | An `error:ApiKeyAuth\(keys=:DeprecationWarning` rule makes "no in-repo test relies on the deprecated path" mechanical |
| In-repo `ApiKeyAuth(keys=` sites: ~35 in `varco_fastapi/tests/{milestone_a/test_server_auth.py, milestone_a/test_middleware.py:174, milestone_f/test_mcp_auth_middleware.py, test_custom_route_params.py:222,267, test_route_guard.py:277, auth/test_auth_posture.py:22,30,38,107}`; two docstrings `router/mcp.py:371,867` | `rg -n 'ApiKeyAuth\(keys' varco_*/` | Sweep list for Step 11 |
| In-repo `PassthroughAuth(` sites: `varco_fastapi/tests/milestone_a/test_server_auth.py:181,190,401,423`; `auth/test_auth_posture.py:55,64,72,97` | `rg -n 'PassthroughAuth\(' varco_*/tests` | Sweep list for Step 14 |
| In-repo tests that resolve `AbstractAuthorizer` from a container holding only the fallback: `varco_core/tests/test_base_authorizer.py:236-260` (fixture `:65-77`), `varco_core/tests/test_service.py:981-1007`, `varco_casbin/tests/test_di.py:20-55`, `varco_fastapi/tests/test_posture_collectors.py:103-116` | grep above | Each gets an explicit `monkeypatch.setenv("VARCO_SECURITY_ENV", "development")` — never a global autouse fixture (§D-flip5-tests) |
| Example app 00 binds `PostAuthorizer` at priority 0 (non-fallback); its tests construct `BaseAuthorizer()` directly; docstrings still say `-(2**31)` | `examples/00-full-stack-post-api/example/{app.py:216, di.py:37, authorizer.py:29,97, service.py:127-128, tests/test_post_service.py:231}` | Runtime unaffected; docstrings updated; **no AsyncAPI snapshot regeneration** (consumer wiring untouched) |
| Examples 01/06/07/08/09/10/17/22 rely on the scanned `BaseAuthorizer` | `rg -n BaseAuthorizer examples/` | Each run snippet gains `VARCO_SECURITY_ENV=development`; not in `make test` |
| providify: `@Requires(*, condition=None, env=None, value=None)` — lazy on every `_filter()`, never cached; `__dict__`-only (subclass does not inherit); no singleton eviction on flip; `env=`+`value=` → unset OR any other string → `False` | `providify/decorator/scope.py:818-967` (lazy `:849-857`, inheritance `:868-870`, eviction `:872-876`); `providify/metadata.py:265-334` (`:320-331`) | The whole §D-flip5 mechanism; case-sensitivity and the no-eviction caveat are edge cases |
| providify: `@Fallback` composes with `@Requires` by AND — *"a `@Fallback` binding that is itself inactive … is simply invisible"* | `providify/decorator/scope.py:976-1005` (`:982-986`) | The gated fallback never enters the candidate set; a non-fallback app binding wins in every environment |
| providify: `_binding_is_active()` third conjunct; `_get_best_candidate()` → `LookupError("No binding found for 'AbstractAuthorizer'. Did you forget container.bind() or container.provide()?")` | `providify/container.py:1904-2005` (`:2005`), `:2814-2853` (`:2848-2851`) | The exact runtime error text an un-gated caller sees — quoted in §Migration so it is greppable |
| providify: `validate()` pass 1c `CONDITION_INACTIVE` (`INFO`, message names the variable and its current state); pass 2 `MISSING_BINDING` (`ERROR`) through `memo_filter` → `_filter` (activation-aware) | `providify/container.py:6006-6065` (1c `:6034-6044`), `:6193-6209`, `:6653-6697`; `providify/validation.py:41-60`, `:73-75`, `:110-114` | Premise (5): two issues, not one; the ERROR is what 052 refuses on, the INFO is the explanation |
| providify: `get_binding()` pure read; `is_resolvable()` | `providify/container.py:7422`, `:2172` | Used by tests that must not instantiate; the collector keeps `container.get()` because it must see an *instance's* type (§D-flip5-posture) |

## Design

### §D-flip5 — `@Requires(env="VARCO_SECURITY_ENV", value="development")` on the `@Fallback` `BaseAuthorizer`; refusal is providify's absence, surfaced by 052's `validate()` and this plan's posture finding

**Chosen (a): the binding itself is conditional.** `varco_core/auth/authorizer.py` becomes

```python
@Fallback
@Requires(env="VARCO_SECURITY_ENV", value="development")
@Singleton
class BaseAuthorizer(AbstractAuthorizer): ...
```

Semantics, all from providify 2.1.1 source: `RequiresMarker.is_satisfied()` reads `os.environ`
on every `_filter()` call (`metadata.py:320-331`, `scope.py:849-857`) — unset, `""`, `production`,
`dev`, `Development` all → `False`; `_binding_is_active()` returns `False` at `container.py:2005`;
the binding never enters the candidate set, so for a request `AbstractAuthorizer`:

| Container state | `get(AbstractAuthorizer)` | `validate()` | Where the refusal lands |
|---|---|---|---|
| `development`, no app binding | `BaseAuthorizer` (the 3.x behaviour) | one `FALLBACK_SHADOWED`? **No** — nothing shadows it; clean | — (posture: `posture.base_authorizer_bound` WARN → demoted to INFO by `development`) |
| any other value, no app binding, ≥1 DI-registered `AsyncService` | `LookupError` (`container.py:2848-2851`) | `CONDITION_INACTIVE` INFO for `BaseAuthorizer` *"(VARCO_SECURITY_ENV is unset)"* / *"('dev')"* **+** one `MISSING_BINDING` ERROR per service (`:6673-6697`) | **052's startup preflight** → `ConfigurationError`, before any `start()` (052 §D-startup row 1) |
| any other value, no app binding, no DI-registered injector (hand-built services, or `validate=False`) | `LookupError` at the first `get()` | `CONDITION_INACTIVE` INFO only | **this plan's `posture.authorizer_unbound` (`HIGH`)** at `SecurityPostureModule.start()`; under 054's `refuse` → `RuntimeError`; under `warn` → logged, and the first service construction raises `LookupError` (the permissive authorizer never runs — fail-closed either way) |
| any value, an app `@Singleton`/`@Provider`/`bind()` of a non-fallback `AbstractAuthorizer` | the app's | clean (`FALLBACK_SHADOWED` is not emitted for a scanned-class default — 045 premise (2)) | — |

| Candidate | ✅ | ❌ |
|---|---|---|
| **(a) chosen: `@Requires` on the `@Fallback` class** | *Literally* "refuses to bind": outside development the binding does not exist, for every consumer at once (`get`/`aget`/`get_all`/`is_resolvable`/`validate`); zero container state, no varco predicate code, lazy (a test's `monkeypatch.setenv` is honoured on the next `get()`); providify's own `validate()` explains it (`CONDITION_INACTIVE` renders the variable's current value) and 052's report/`varco diagnose` show it as *inactive: condition* without any 053 code; the escape hatch is the variable 036 already defined; independent of 054 (refusal does not depend on `enforce`) and of 049 (does not depend on the posture module being on); brief 010 §3's varco implication made mechanical | The runtime `LookupError` text (`:2848-2851`) says "Did you forget container.bind()" — providify cannot know a gated binding was the intended one; mitigated by quoting the text in §Migration/CHANGELOG and by `posture.authorizer_unbound`'s remediation. A code-configured `SecurityPostureSettings(environment="development")` does **not** open the gate (§D-flip5-escape). A `BaseAuthorizer` singleton cached in `development` is not evicted by an env flip (`scope.py:872-876`), but the gate is re-checked on every lookup, so after a flip away from `development` it is invisible (`LookupError` — fails closed) and flipping back returns the same cached instance — irrelevant to a real process, named in §Edge cases |
| (b) provider stays unconditional; the posture collector escalates `posture.base_authorizer_bound` to `ERROR`/`HIGH` outside development and 054's `refuse` blocks | No DI change; one finding | Does **not** refuse to bind — under `enforce="warn"` (any app that took 054's escape hatch, or `VARCO_SECURITY_POSTURE_ENABLED=false`, or a hand-wired container with no posture module) the permissive authorizer silently serves production: the exact 3.2 state the row exists to end. Makes FLIP-5 a presentation change hostage to 054 **and** 049 both being on; the slice map says 053–057 are independently mergeable. Rejected |
| (c) unconditional binding whose `__init__` raises outside development | Refuses for bare-container users with a varco-authored message | `validate()` is clean while construction is guaranteed to fail — breaks providify's own `ERROR` contract (*"guaranteed to fail at resolution time"*, `validation.py:44-45`); 052's report names `BaseAuthorizer` as the live implementation — a lie; the posture collector's `try/except Exception` swallows it into `NOT_ASSESSED`; providify has the exact primitive for "this binding does not exist here". Rejected |
| (d) `@Requires(condition=lambda: SecurityPostureSettings().environment == "development")` | Reads through pydantic | A pydantic model construction on **every** `_filter()` call for `AbstractAuthorizer` (providify never caches, `scope.py:849-854`); still env-only (a zero-arg predicate cannot see a container-bound settings instance — 047 §D-features row 3's reason); `CONDITION_INACTIVE` renders `condition=<lambda>` instead of the variable name. Rejected — `env=`/`value=` is the same truth with a better message and no per-lookup cost |

### §D-flip5-escape — `VARCO_SECURITY_ENV=development` is the only switch; explicit code is the only other path, and posture reports it

- **One variable, two readers, one spelling.** `SecurityPostureSettings.environment` (pydantic,
  presentation) and the `@Requires` marker (providify, binding) both read `VARCO_SECURITY_ENV` and
  both compare against the exact string `"development"`. Any other value — `production`, unset,
  `""`, `dev`, `staging`, `Development` — is strict for **both**: no permissive binding, no WARN
  demotion. The two can never disagree.
- **No `VARCO_AUTHORIZER_ALLOW_PERMISSIVE` (or any second variable).** ✅ 036 §D-S9-oq1: *"One
  variable, not two. A separate … alongside an environment name is two knobs for one axis and
  they will disagree."* ✅ 034 §D-S2-param's rejection of an env var that re-enables a weakened
  posture: *"a security-weakening default an operator could flip without a code review, invisible
  at any call site."* A permissive authorizer in production is the largest such weakening varco
  has; it must be a code-review artefact, not an environment one.
- **The code path exists and is reported.** An app that genuinely wants permissive authorization
  in production binds an *instance* — `bind_instance(AbstractAuthorizer, BaseAuthorizer())`
  (046's verb) or a `@Provider` returning `BaseAuthorizer()` — which carries no class markers
  (`binding.py:723` reads them off the provider function) and therefore resolves in every
  environment. It is greppable, it is in a diff, and `posture.base_authorizer_bound` (WARN) fires
  on it because the collector checks the *instance's* type; in production under 054's `refuse`
  it needs `VARCO_SECURITY_SUPPRESS=posture.base_authorizer_bound` — a second, visible,
  operator-owned statement. A subclass (`class Permissive(BaseAuthorizer): pass`, registered as
  an app `@Singleton`) is the same story minus the finding (`type(x) is BaseAuthorizer` is
  `False`) — documented, not chased; it is an app-authored authorizer by definition.
- **Why a code-configured `SecurityPostureSettings(environment="development")` does not open the
  gate.** The marker is a zero-arg predicate evaluated inside `_filter()` with no container in
  reach; teaching it to find a settings instance would mean varco code resolving from the
  container during resolution — the recursion 047 §D-features row 3 rejected. Tests use
  `monkeypatch.setenv`; a developer's shell exports the variable once. Documented in the Pitfalls
  table.

### §D-flip5-posture — two ids: `posture.base_authorizer_bound` (WARN, meaning narrows) and `posture.authorizer_unbound` (HIGH, new)

In `LocalPostureCollector.collect()` (051's class; the same body at `posture.py:743-769` if 051 has
not landed), the authorizer block becomes:

```python
if container is not None:
    try:
        authorizer = container.get(AbstractAuthorizer)
    except LookupError:                      # no ACTIVE binding — the FLIP-5 production state
        findings.append(PostureFinding(check="posture.authorizer_unbound", severity=HIGH, ...))
    except Exception:                        # noqa: BLE001 — construction failed: NOT_ASSESSED, never a crash
        findings.append(PostureFinding(check="posture.not_assessed.base_authorizer_bound", ...))
    else:
        if type(authorizer) is BaseAuthorizer:
            findings.append(PostureFinding(check="posture.base_authorizer_bound", severity=WARN, ...))
```

| `check` | Severity | Fires when | Remediation text |
|---|---|---|---|
| `posture.authorizer_unbound` (**new**) | `HIGH` (never demoted — `posture.py:351` demotes WARN only; blocks under `refuse` in every environment) | `container.get(AbstractAuthorizer)` raises `LookupError` — i.e. `VARCO_SECURITY_ENV` is not `development` and the app bound no `AbstractAuthorizer` | "No AbstractAuthorizer is bound. varco's permissive BaseAuthorizer binds only under VARCO_SECURITY_ENV=development (4.0, FLIP-5). Register your own AbstractAuthorizer (an app @Singleton, enable_policy_authorizer(), …); for a development process set VARCO_SECURITY_ENV=development. Every service that injects AbstractAuthorizer will fail with LookupError until then." |
| `posture.base_authorizer_bound` (unchanged spelling) | `WARN` (→ INFO in `development`) | The resolved instance is exactly `BaseAuthorizer` — in `development` (the normal case) or via an explicit instance/provider binding in production | "AbstractAuthorizer resolves to BaseAuthorizer — every operation is allowed unconditionally. In production this can only be an explicit bind_instance()/@Provider of BaseAuthorizer(); remove it or, if deliberate, suppress with VARCO_SECURITY_SUPPRESS=posture.base_authorizer_bound." |

- ✅ `container.get()` stays (not `get_binding()`): the WARN needs the *instance's* type to catch
  the explicit-instance path; the singleton would be built by the first service anyway, and 049's
  module already pays this cost. ✅ Splitting `LookupError` out of the blanket `except` costs one
  clause and turns 036's ⚠️ ("resolution failure is `NOT_ASSESSED`") into an honest three-way.
  ❌ A `LookupError` raised *inside* a user authorizer's `__init__` (a missing dependency of the
  app's own authorizer) is indistinguishable from "no binding" and is reported as
  `authorizer_unbound` — the remediation text is still correct (fix the binding), and 052's
  `validate()` names the real missing parameter. Accepted, named in §Edge cases.
- Rejected — escalating `posture.base_authorizer_bound` itself to `HIGH` outside development: the
  finding can only fire in production through an explicit code decision now, and a deliberate,
  reviewed, greppable decision at WARN-with-suppression is the same tier 057 chose for
  `redaction.audit.disabled` ("an explicit opt-out … should need an explicit suppression").
- Rejected — a new id for the development case ("base_authorizer_bound_dev"): 040/057's rule —
  ids are never renamed once shipped, and the meaning "the permissive authorizer is live" has not
  changed; only the set of states in which it can be true has shrunk.

### §D-flip5-tests — the in-repo blast radius is four test files and the 052 test profile; no autouse fixture

Every in-repo test that resolves `AbstractAuthorizer` from a container holding only the fallback
(§Context row) gets an explicit `monkeypatch.setenv("VARCO_SECURITY_ENV", "development")` in its
own fixture or body. ✅ Greppable: `rg -n 'VARCO_SECURITY_ENV' varco_*/tests` lists exactly the
tests that depend on the permissive default. ❌ Four small edits instead of one conftest line.
Rejected — a session-scoped autouse fixture in every `conftest.py`: it would silently change
`SecurityPostureSettings()` in every posture test (WARN → INFO demotion) and hide the flip from
the suite that is supposed to prove it.

**052's in-memory test profile** (`varco_core.testing.in_memory_modules()` / `InMemoryProfile`,
052 §D-profile) must keep booting with zero env vars: it binds the permissive authorizer
**explicitly** — `bind_instance(AbstractAuthorizer, BaseAuthorizer())` (or the provider-closure
form if 046's verb is named differently at implementation time) — with a docstring saying it is
the test profile's own, deliberate choice, exactly the code path §D-flip5-escape describes. ⚠️
Conditional on 052 having landed (build order: 052 is the last DI plan; 053 may land first). If
052 is not in the tree, Step 5 leaves a one-line note in `plans/052-…md` Step for the profile and
this plan's Done criterion carries it.

### §D-jti — `JwtAuthority(mint_jti=True)`: `token()` seeds `with_random_jti()`; `sign()` is untouched

`JwtAuthority.__init__` and `from_pem()` gain `mint_jti: bool = True` (keyword-only, both already
keyword-only). `token()` becomes `b = JwtBuilder().issuer(self._issuer); return b.with_random_jti()
if self._mint_jti else b`. `sign()` keeps its documented contract (*"the builder … is used as-is"*,
`jwt_authority.py:298-300`). `MultiKeyAuthority` delegates and needs nothing — each authority in
the ring carries its own flag (a rotated-in authority constructed with the default mints;
document that a ring mixing `mint_jti` values is the operator's choice, not an error).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: seed in `token()`** | `token()` is already the seeding point (`iss`); the `jti` is visible on the builder before signing, so `.token_id("x")` (last write wins, `builder.py:203-214`) is the per-token override; `sign()`'s contract is untouched; a hand-built `JwtBuilder()` passed to `sign()` is the per-token opt-out (no `jti`) — explicit, no new API | A caller who never calls `token()` gets no `jti` — documented; that caller is not using the authority's minting path |
| (2) inject in `sign()` when `builder._jti is None` | Covers hand-built builders | Reads/mutates another object's private state, breaks the "as-is" contract, no per-token opt-out short of a new `JwtBuilder.without_jti()` method. Rejected |
| (3) `JwtBuilder.__init__` mints by default | Every token, everywhere | Changes the signed bytes of `JwtBuilder().encode()` users who never touch an authority — outside the row's scope ("`JwtAuthority`-minted"); 034 §D-S13-jti's blast-radius argument applies in full. Rejected |

**Escape hatch:** `JwtAuthority.from_pem(..., mint_jti=False)` (one greppable kwarg). **Posture:**
none — a `jti` on a minted token is not a posture fact; the *consumer-side* fact is
§D-require-jti's. **Interaction with §D-require-jti:** a 4.0 varco issuer + a non-Null store gets
`TOKEN`-scope revocation working with zero configuration, and the derived `require_jti=True` never
rejects a 4.0-minted token. Tokens minted by a **3.x** authority that are still unexpired at
upgrade time carry no `jti` and *are* rejected once a non-Null store is wired — the §Migration row
gives the one-token-lifetime bridge (`VARCO_JWT_REVOCATION_REQUIRE_JTI=false`, then unset).

### §D-require-jti — `revocation_require_jti: bool | None = None`; `None` derives `True` iff the registry's wired store is non-Null; computed in `_check_revocation()`

`JwtVerificationSettings.revocation_require_jti` becomes `bool | None = None` (`config.py:111`).
`TrustedIssuerRegistry._check_revocation()` (`registry.py:1017-1019`) becomes:

```python
effective_require_jti = require_jti if require_jti is not None else settings.revocation_require_jti
if effective_require_jti is None:
    effective_require_jti = not is_null_revocation_store(self._revocation_store)   # §D-null
```

`_check_revocation()` is reached only when `self._revocation_store is not None`
(`registry.py:960`), so the three-valued merge is: per-call kwarg > env/settings > derived.

- ✅ The row verbatim: *"defaults to `True` when a non-Null store is wired"* — and **only** then. A
  `NullTokenRevocationStore` handed to the registry (the DI default, `container.get(AbstractTokenRevocationStore)`
  in an app that never opted in) keeps verifying `jti`-less tokens — otherwise binding the default
  would start rejecting every Auth0/Keycloak/Cognito token, the exact outage 034 `:386-389`
  refused to ship. ✅ One line, one place, no registry constructor change (premise (1)). ✅ The
  3.2 explicit values keep their meaning: `VARCO_JWT_REVOCATION_REQUIRE_JTI=true|false` and the
  per-call kwarg override the derivation. ❌ `inspect_revocation_posture()` can no longer copy a
  bool — it computes the same derivation (Step 9) and gains `require_jti_derived: bool = False`
  (an appended, defaulted field on the frozen dataclass — additive) so 052's report can say
  *"derived"*. ❌ A foreign issuer with no `jti` + a real store now fails closed by default (brief
  009 §2: Auth0, Keycloak, Cognito) — that is the row, argued in 034 as "behind a warn-only
  period"; the period was 3.2, the escape hatch is one env var, and the §Migration row names the
  three IdPs.
- Rejected — deriving from the *DI-bound* store rather than the *registry-wired* store: the
  registry is the only object that knows what it will consult; a store bound in DI but never
  wired is 052's `di.revocation_store_not_wired`, not this rule's input.
- Rejected — `True` unconditionally (the BACKLOG row's literal "defaults True" read without the
  qualifier): rejects `jti`-less tokens even when the store is the Null Object — an outage for
  doing nothing.

**Posture (in `AuthPostureCollector` / `_collect_auth`, `posture.py:547-580`):** one new INFO id,
`auth.revocation_jti_not_required`, when `rev.registry_wired and not rev.require_jti` — i.e. a real
store is wired and the operator (or a per-call kwarg — not visible here) opted out of the derived
default: detail *"A revocation store is wired but revocation_require_jti is False — TOKEN-scope
revocation silently never matches a jti-less token (SUBJECT/TENANT/ISSUER scopes still apply)"*,
remediation *"Unset VARCO_JWT_REVOCATION_REQUIRE_JTI (the 4.0 default derives True from a wired
store), or configure your issuer to emit jti."* The derived-`True` case emits nothing — it is the
default working as designed, and 052's report carries `require_jti_derived`.

### §D-null — "non-Null" is `isinstance(store, NullTokenRevocationStore)`, through one helper

`varco_core/revocation/null.py` gains `is_null_revocation_store(store: AbstractTokenRevocationStore
| None) -> bool` (`store is None or isinstance(store, NullTokenRevocationStore)`), used by
`_check_revocation()` and by `inspect_revocation_posture()`'s two existing `isinstance` sites
(`posture.py:107-111`).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: `isinstance` behind one helper** | Already the definition `inspect_revocation_posture()` uses — one fact, one home; an out-of-tree store is non-Null by construction (correct: it does something); a user subclass of `NullTokenRevocationStore` is Null (correct: it inherits the no-op contract); zero ABC change | A user *wrapper* around the Null store (a logging decorator) is non-Null → derives `True` → rejects `jti`-less tokens. Documented in §Edge cases; the wrapper author opts out with the env var or subclasses the Null class |
| (2) `is_null: ClassVar[bool] = False` on the ABC, `True` on the Null class | Explicit, no import of the concrete class into `authority/` | A new member on an ABC every out-of-tree store implements (CLAUDE.md's standing "never a new abstract method" rule in spirit — a `ClassVar` is not abstract, but it is surface); a second definition of the same fact beside the `isinstance` the posture inspector already ships; a subclass can set it wrongly. Rejected |
| (3) `type(store) is NullTokenRevocationStore` | Wrappers and subclasses both non-Null | A user subclass of the Null Object (the documented "I want a no-op store with a log line" shape) would start rejecting tokens. Rejected |

The helper is imported inside `_check_revocation()`'s body (the registry already imports
`JwtVerificationSettings` there, `registry.py:1014`) — `varco_core.authority` gains no
module-level import of `varco_core.revocation.null`.

### §D-apikey — `warn_deprecated()` + one call in `ApiKeyAuth.__init__`; `hashed_keys=` the only documented path; a pytest `error` rule proves no in-repo test relies on `keys=`

`varco_core/deprecation.py` gains a third helper:

```python
def warn_deprecated(subject: str, *, since: str, removed_in: str, replacement: str | None = None,
                    stacklevel: int = _CALLER_STACKLEVEL) -> None
```

— `_build_message()` (the one canonical sentence, `:75-97`) + `warnings.warn(..., DeprecationWarning,
stacklevel=stacklevel)` (+ 052's `_note()` when present, so it lands in the ledger). It exists
because neither `deprecated()` (a symbol) nor `deprecated_alias()` (a name) can deprecate *one
parameter path* of a constructor that stays. ✅ Same message shape, same `removed_in=` discipline
(`rg 'removed_in="5\.' varco_*/varco_*` now finds it); ✅ three lines of code. ❌ One more public
name in `varco_core.deprecation.__all__` (an *addition* — `api_surface.py --check` reports a
note, never a failure; the snapshot is regenerated in the same step).

`ApiKeyAuth.__init__`, right after the XOR check (`server_auth.py:418-423`):
`if keys is not None: warn_deprecated("ApiKeyAuth(keys=...)", since="4.0.0", removed_in="5.0.0",
replacement="ApiKeyAuth(hashed_keys=...) with varco_core.auth.api_key.hash_api_key()", stacklevel=3)`
— `stacklevel=3` because the helper is one frame and `__init__` a second; the warning must blame
the `ApiKeyAuth(...)` call site (the helper's own rule, `deprecation.py:69-72`). Behaviour is
otherwise byte-identical: the keys are still hashed at construction, `_constructed_from_plaintext`
still feeds `auth.api_key_plaintext` (WARN, unchanged severity; remediation text gains
"deprecated since 4.0.0, removed in 5.0.0").

**Why `since="4.0.0", removed_in="5.0.0"`** (BACKLOG `:84`): 034 said "warn in 3.3, remove in
4.0"; there was no 3.3, and `CONTRIBUTING.md:98-105`'s 12-month floor runs from the release that
*deprecates* — 4.0.0 can only open the window.

**Mechanical guard:** every package whose tests construct `ApiKeyAuth` (`rg -l 'ApiKeyAuth\('
varco_*/tests` at implementation time — `varco_fastapi` at plan time) adds
`"error:ApiKeyAuth\\(keys=:DeprecationWarning"` to its `[tool.pytest.ini_options] filterwarnings`.
The ~35 incidental sites are swept to `hashed_keys={hash_api_key("k"): ctx}` (deterministic without
a pepper); the handful of tests *about* the `keys=` path (`test_server_auth.py:76-102,493`,
`test_auth_posture.py:22-38,107`) wrap the construction in `pytest.warns(DeprecationWarning,
match=r"ApiKeyAuth\(keys=")`. ✅ No test can regress onto the deprecated path silently; ✅ the
`error` rule is scoped to this one message, not to all `DeprecationWarning`s from `varco_fastapi`
(the `TrustStore` shim and others are 058's).

### §D-passthrough — `PassthroughAuth(*, acknowledge_unverified: Literal[True], required: bool = False)`; `TypeError`, deliberately not the `ValueError` precedent

```python
def __init__(self, *, acknowledge_unverified: Literal[True], required: bool = False) -> None:
    if acknowledge_unverified is not True:
        raise TypeError("PassthroughAuth: acknowledge_unverified must be literally True — this strategy "
                        "trusts caller-supplied identity with no signature verification; construct it "
                        "only behind a gateway that already verified the token.")
```

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: required keyword-only `Literal[True]`, `TypeError`** | `PassthroughAuth()` fails with Python's own `TypeError: … missing 1 required keyword-only argument: 'acknowledge_unverified'` — **statically visible to mypy** under `strict` (a missing required kwarg is a type error), unlike the four `bool = False` + `ValueError` sites, which are runtime-only; `acknowledge_unverified=False` is likewise a *type* violation of `Literal[True]`, so `TypeError` is the honest class for both shapes — one exception type, one reason; the task brief asked for `TypeError` | Deviates from the `acknowledge_*: bool = False` → `ValueError` convention (premise (4)) — recorded here and in the class docstring as a deliberate, argued deviation; the four existing sites are **not** changed (out of scope; a later sweep may adopt `Literal[True]` if 058 agrees) |
| (2) `acknowledge_unverified: bool = False` + `ValueError` | Matches the four precedents verbatim | `PassthroughAuth()` still *constructs syntactically* and fails only at runtime; mypy is blind; a `bool` whose only legal value is `True` is a lie in the annotation. Rejected |
| (3) delete `PassthroughAuth` | Smallest attack surface | It has a real use (behind a verifying gateway, `server_auth.py:526-528`); the row asks for an acknowledgement, not a removal. Rejected |

**Posture:** `auth.passthrough_bound` (WARN) is unchanged — an acknowledged instance is still a
fact worth reporting; the acknowledgement is a code-review artefact, the finding is an operator
one; `VARCO_SECURITY_SUPPRESS=auth.passthrough_bound` is the deliberate-deployment answer.
**Escape hatch:** none other than the kwarg — the row's whole point.

### §D-snapshots — what must and must not be regenerated

| Artefact | Effect of this plan | Action |
|---|---|---|
| `design/api-freeze-and-standards/measurements/api-surface.{json,md}` | One **addition** (`warn_deprecated` in `varco_core.deprecation.__all__`; if `varco_core.__init__`'s lazy map re-exports the deprecation helpers, there too — Step 10 checks). `PassthroughAuth`/`ApiKeyAuth`/`JwtAuthority`/`JwtVerificationSettings` are classes — `--check` cannot see their `__init__`/field changes (CLAUDE.md "Known limitation") | Regenerate + commit in Step 10; `--check` clean at Step 19. The class-signature changes get **dedicated `inspect.signature()` regression tests** (034's §D-034-gate precedent, `server_auth.py:507-516`) |
| `design/api-freeze-and-standards/measurements/asyncapi-example.json` | None — no consumer wiring moves | **Not regenerated**; `make asyncapi-check` must stay clean |
| `design/async-performance-patterns/measurements/import-budget.json` | None — `authorizer.py` already imports `providify`; `Literal` is `typing` | `--check --warn-only` no breach |
| 048's settings-reference snapshot (if landed) | `revocation_require_jti` default `False` → `None` | Regenerate with 048's script in Step 8 |
| 047's DI catalogue snapshot (if landed) | `BaseAuthorizer` row gains a condition | Regenerate with 047's `di_catalogue.py` in Step 3 |

## Steps

All commands run from `/home/edoardo/projects/varco`. Phase A (FLIP-5) → Phase B (FLIP-034-2/3,
share the registry tests) → Phase C (FLIP-034-1) → Phase D (FLIP-034-4) → Docs → Verification.
Phases B–D are independent of A and of each other; A first because it has the widest in-repo
blast radius.

### Phase A — FLIP-5

1. [x] **Preflight.** Confirm 045 landed: `rg -n '@Fallback' varco_core/varco_core/auth/authorizer.py`
       → 1 hit and `rg -n '_FALLBACK_PRIORITY' varco_*/` → 0 hits (if either fails, stop — this plan
       builds on 045). Confirm providify: `uv run python -c "import importlib.metadata as m;
       print(m.version('providify'))"` → `2.1.1`; re-read and record in
       `/tmp/claude-1000/-home-edoardo-projects-varco/625af1a2-0cf8-4199-b924-0b3f986861e6/scratchpad/053-preflight.txt`
       every providify `file:line` in §Context (`scope.py:818-967,976-1005`,
       `metadata.py:265-334`, `container.py:1904-2005,2814-2853,6006-6065,6193-6209,6653-6697,7422`,
       `validation.py:41-60,73-75,110-114`). Record whether 046 (`bind_instance` name), 048
       (`bind_settings`/settings snapshot), 049/051 (`LocalPostureCollector`,
       `AuthPostureCollector`, `SecurityPostureModule`) and 052 (`in_memory_modules`, `_note`,
       `run_wiring_preflight`) are in the tree — each later step names its pre-/post- form.
       Execute the two-line semantic check the whole design rests on:
       `uv run python -c "import os; from providify import DIContainer; from varco_core.auth.base import AbstractAuthorizer; c=DIContainer(); c.scan('varco_core.auth.authorizer'); print(c.is_resolvable(AbstractAuthorizer))"`
       → `True` today (records the pre-flip state). **Verify**: the scratchpad file exists with
       ≥ 12 confirmations and the pre-flip `True`.

2. [x] `varco_core/tests/test_base_authorizer.py` (red) — (a) `fresh_container` fixture (`:65-77`)
       gains `monkeypatch.setenv("VARCO_SECURITY_ENV", "development")` with a comment naming FLIP-5;
       `TestBaseAuthorizerDI` (`:236-260`) otherwise unchanged. (b) New `class TestBaseAuthorizerGate`
       (all `async def`, a fresh container per test via `container.scan("varco_core.auth.authorizer")`):
       `test_unset_env_is_not_a_binding` — `monkeypatch.delenv("VARCO_SECURITY_ENV", raising=False)`;
       `container.is_resolvable(AbstractAuthorizer) is False`; `pytest.raises(LookupError, match="No binding found for 'AbstractAuthorizer'")`
       on `get`; `test_production_is_not_a_binding` (`=production`); `test_case_sensitive`
       (`=Development` → `LookupError`; docstring cites `metadata.py:328-331`);
       `test_empty_string_is_strict` (`=""`); `test_development_binds` (`=development` →
       `isinstance(get(AbstractAuthorizer), BaseAuthorizer)`, same instance twice);
       `test_gate_is_lazy` — unset → `LookupError`; `setenv(development)` → resolves on the very next
       `get()` with no re-scan (cites `scope.py:849-857`); `test_flip_hides_cached_singleton_without_evicting` —
       `development` → `get()`; `setenv(production)` → `get()` raises `LookupError` (the gate is
       re-checked per lookup — fails closed); `setenv(development)` → `get()` returns the **same**
       cached instance (cites `scope.py:872-876`; documents the no-eviction caveat rather than
       pretending it away — measured against providify 2.1.1);
       `test_class_keyed_bind_is_gated` — production; `container.bind(AbstractAuthorizer, BaseAuthorizer)`
       → still `LookupError` (premise (3)); `test_instance_binding_is_not_gated` — production;
       a `Provider(singleton=True)` closure returning `BaseAuthorizer()` with `returns=AbstractAuthorizer`
       (046's `bind_instance` if present) → resolves; `test_subclass_is_not_gated` — production;
       `@Singleton class Permissive(BaseAuthorizer): pass` registered → resolves to `Permissive`
       (cites `scope.py:868-870`); `test_app_authorizer_wins_in_every_environment` —
       parametrised over `{unset, production, development}`; an app `@Singleton class Deny(AbstractAuthorizer)`
       registered after the scan → `get()` is `Deny`; `test_validate_explains_the_gate` —
       production; register `@Singleton class Svc` with `authorizer: Inject[AbstractAuthorizer]`;
       `report = container.validate(raise_on_error=False)`; exactly one issue with
       `kind is IssueKind.MISSING_BINDING` and `severity is Severity.ERROR` whose `requested`
       names `AbstractAuthorizer`, and exactly one `CONDITION_INACTIVE` `INFO` whose `message`
       contains `"VARCO_SECURITY_ENV"` and `"is unset"`; in `development` the same container
       validates with neither. **Verify**: `uv run pytest varco_core/tests/test_base_authorizer.py -q`
       → red on every `TestBaseAuthorizerGate` test except `test_development_binds`,
       `test_app_authorizer_wins_in_every_environment`, `test_instance_binding_is_not_gated`,
       `test_subclass_is_not_gated` (which pass today and are the controls).

3. [x] `varco_core/varco_core/auth/authorizer.py` — add `from providify import Requires` (beside
       045's `Fallback`); stack `@Requires(env="VARCO_SECURITY_ENV", value="development")` on the
       class (order relative to `@Fallback`/`@Singleton` is irrelevant, `scope.py:1001-1005`).
       Rewrite the module docstring `:1-49`: the "permissive by default" DESIGN block becomes
       "DESIGN: development-only binding (Plan 053 / FLIP-5)" — ✅ fail-closed (brief 010 §3);
       ✅ one variable shared with `SecurityPostureSettings.environment`, same literal; ✅ lazy,
       no varco predicate; ❌ a bare-container caller sees providify's generic `LookupError`
       text (quoted) — read `validate()`'s `CONDITION_INACTIVE` or `varco diagnose`; ❌ a
       code-configured settings object cannot open the gate (§D-flip5-escape's reason). Delete the
       hand-written production `assert` (`:38-39`) — it is what the framework now does. Class
       docstring: "When to replace" → "How to get this binding: `VARCO_SECURITY_ENV=development`
       (the only switch). In any other environment this class is not a binding; register your own
       `AbstractAuthorizer`. To run permissive in production deliberately, bind an *instance*
       (`bind_instance(AbstractAuthorizer, BaseAuthorizer())`) — reported by `posture.base_authorizer_bound`."
       If 047's catalogue snapshot exists, regenerate it. **Verify**: Step 2 green;
       `uv run pytest varco_core/tests/test_service.py -q` — `:981-1007` now red (fixed in Step 4);
       the Step 1 one-liner prints `False`.

4. [x] In-repo sweep, explicit per test (§D-flip5-tests): `varco_core/tests/test_service.py:981-1007`
       (`monkeypatch.setenv(... "development")` before the scan, one-line comment "FLIP-5");
       `varco_casbin/tests/test_di.py:20-55` — the fixture sets `development`; **add**
       `test_policy_authorizer_needs_no_development_env`: `monkeypatch.delenv("VARCO_SECURITY_ENV",
       raising=False)`; `bootstrap` + `enable_policy_authorizer(c)` → `aget(AbstractAuthorizer)` is a
       `PolicyEngineAuthorizer`; `varco_core/tests/test_authorizing_audit.py` (or wherever
       `enable_authorization_audit` is tested — `rg -l enable_authorization_audit varco_core/tests`)
       — **add** `test_enable_authorization_audit_in_production_without_authorizer_raises`:
       env unset, only the fallback scanned → `enable_authorization_audit(c)` raises `LookupError`
       (documents §Edge cases; the existing tests set `development` or bind their own).
       **Verify**: `make test PKG=varco_core`, `make test PKG=varco_casbin` green.

5. [x] `varco_fastapi/tests/test_posture_collectors.py` (red first): `:103-116` sets
       `development` and keeps asserting `posture.base_authorizer_bound` present (severity now
       `INFO` after demotion — assert the pre-demotion severity through the collector directly,
       `LocalPostureCollector(container=…).collect()` / `_collect_local(container=…)`, is `WARN`);
       **add** `test_production_without_authorizer_is_unbound_high` — env unset; a container with
       only the fallback scanned → collector yields exactly one finding `check ==
       "posture.authorizer_unbound"`, `severity is PostureSeverity.HIGH`, remediation mentions
       `VARCO_SECURITY_ENV=development` and `AbstractAuthorizer`; no `base_authorizer_bound`, no
       `not_assessed`; `test_unbound_refuses_under_refuse_in_any_environment` — parametrised over
       `environment in {"production", "development"}` with `SecurityPostureSettings(enforce="refuse",
       environment=…)` + that collector → `await lifecycle.start()` raises `RuntimeError` naming
       `posture.authorizer_unbound` (HIGH is never demoted, `posture.py:351,384`);
       `test_explicit_instance_binding_in_production_is_warn` — env unset; provider closure
       returning `BaseAuthorizer()` → `posture.base_authorizer_bound` WARN, remediation mentions
       `VARCO_SECURITY_SUPPRESS`; `test_construction_failure_is_still_not_assessed` — an app
       `@Singleton` authorizer whose `__init__` raises `RuntimeError` → `posture.not_assessed.base_authorizer_bound`.
       Then `varco_fastapi/varco_fastapi/posture.py` — the authorizer block (§D-flip5-posture's
       code, in `LocalPostureCollector.collect()` post-051 or `_collect_local` `:743-769` pre-051):
       split `except LookupError` before `except Exception`; new finding; both remediation texts
       from the §D-flip5-posture table; the module-level id list/docstring gains
       `posture.authorizer_unbound`. **052's test profile** (if `varco_core.testing.in_memory_modules`
       exists): bind `BaseAuthorizer()` explicitly per §D-flip5-tests with a docstring, and add
       `test_in_memory_profile_boots_with_no_env` asserting `get(AbstractAuthorizer)` resolves with
       `VARCO_SECURITY_ENV` deleted; otherwise append one line to `plans/052-…md`'s profile step:
       "bind `BaseAuthorizer()` explicitly (Plan 053 §D-flip5-tests)". **Verify**:
       `uv run pytest varco_fastapi/tests/test_posture_collectors.py varco_fastapi/tests/test_posture_core.py -q`
       green; `make test PKG=varco_fastapi`; `make test PKG=examples` (example 00 binds
       `PostAuthorizer`; must be green **unmodified**).

### Phase B — FLIP-034-2 and FLIP-034-3

6. [x] `varco_core/tests/test_jwt_authority.py` (locate: `rg -l 'JwtAuthority' varco_core/tests`;
       add a `TestMintJti` class, red): `test_token_carries_uuid4_jti_by_default` — `authority.token()
       .build().jti` parses as `uuid.UUID(jti, version=4)`; `test_two_tokens_differ`;
       `test_signed_token_carries_jti` — `authority.verify(authority.sign(...))`'s raw claims contain
       `jti`; `test_token_id_overrides_minted` — `.token()..token_id("fixed")` → `"fixed"`;
       `test_hand_built_builder_is_not_minted` — `authority.sign(JwtBuilder().issuer(authority.issuer)
       .subject("u").expires_in(...))` → no `jti` (the per-token opt-out); `test_mint_jti_false`
       (`from_pem(..., mint_jti=False)` and the direct constructor) → `jti is None`;
       `test_multi_key_authority_inherits_per_authority` — a ring with one `mint_jti=True` and one
       `False`; `rotate()` flips the observed behaviour; `test_signature_default_is_true` —
       `inspect.signature(JwtAuthority.from_pem).parameters["mint_jti"].default is True` and
       `KEYWORD_ONLY` (the §D-034-gate-style guard for a class the api-surface check cannot see).
       **Verify**: red on all but `test_hand_built_builder_is_not_minted`.

7. [x] `varco_core/varco_core/authority/jwt_authority.py` — `__init__` (`:125-160`) and `from_pem()`
       (`:164-246`) gain `mint_jti: bool = True` (keyword-only; `__slots__` `:120-123` gains
       `"_mint_jti"`); `token()` (`:267-287`) seeds `with_random_jti()` when set; docstrings: `Args`
       for both constructors, `token()`'s `Returns` ("… with `iss` and, unless `mint_jti=False`, a
       fresh uuid4 `jti`"), and a "DESIGN: jti minted in `token()`, not `sign()` (Plan 053 / FLIP-034-2)"
       block with §D-jti's ✅/❌ (per-token override = `token_id()`, per-token opt-out = a hand-built
       builder, `sign()`'s as-is contract untouched). `multi_key_authority.py:148-155` `token()`
       docstring: one sentence ("`jti` minting follows the *active* authority's `mint_jti`").
       **Verify**: Step 6 green; the rest of the authority/JWT suites unmodified and green
       (`uv run pytest varco_core/tests -q -k "jwt or authority or registry"`).

8. [x] `varco_core/tests/test_registry_revocation.py` (locate: `rg -l '_check_revocation\|revocation_require_jti' varco_core/tests`;
       add `TestDerivedRequireJti`, red) — a registry whose `verify()` reaches `_check_revocation`
       (reuse the file's existing HS/RS fixture and a `jti`-less token built with `JwtBuilder()`
       directly): `test_settings_default_is_none` — `JwtVerificationSettings().revocation_require_jti is None`
       (with `VARCO_JWT_REVOCATION_REQUIRE_JTI` deleted); `test_null_store_wired_derives_false` —
       `TrustedIssuerRegistry(revocation_store=NullTokenRevocationStore())` verifies the `jti`-less
       token; `test_in_memory_store_wired_derives_true` — `InMemoryTokenRevocationStore()` →
       `TokenRevokedError` whose `reason` contains `"revocation_require_jti"`; the same token
       **with** a `jti` verifies; `test_env_false_overrides_derivation` — `setenv(... "false")` +
       in-memory store → verifies; `test_env_true_with_null_store_still_requires` — `"true"` + Null →
       `TokenRevokedError` (explicit beats derived, both directions); `test_per_call_kwarg_overrides`
       — `verify(raw, revocation_require_jti=False)` with the in-memory store → verifies;
       `test_null_subclass_is_null_and_wrapper_is_not` — `class QuietNull(NullTokenRevocationStore)`
       → derives `False`; a delegating wrapper implementing the ABC over a Null instance → derives
       `True` (§D-null's documented edge, pinned so it is a choice, not an accident);
       `test_no_store_is_unchanged` — `revocation_store=None` never consults anything (byte-identical
       to 3.2). Then `varco_core/tests/test_revocation_posture.py` (locate) — `inspect_revocation_posture(
       registry=<in-memory wired>)` → `require_jti is True and require_jti_derived is True and
       token_scope_usable is True`; `registry=<Null wired>` → `False/True/False`; explicit env
       `false` + in-memory → `False/False/False`; `registry=None, store=None` → `require_jti False,
       require_jti_derived True` (no store: derived `False`). **Verify**: red on the derivation and
       posture tests.

9. [x] Implementation: `varco_core/varco_core/jwt/config.py:111` → `revocation_require_jti: bool |
       None = None`; docstring `:69-76` → "``None`` (the 4.0 default, Plan 053 / FLIP-034-3): derived
       at check time — ``True`` iff the registry's wired store is not a ``NullTokenRevocationStore``.
       ``true``/``false`` (env or instance) override the derivation in either direction. Auth0,
       Keycloak, Cognito emit no ``jti`` by default (brief 009 §2): set
       ``VARCO_JWT_REVOCATION_REQUIRE_JTI=false`` for such an issuer, or use SUBJECT/TENANT/ISSUER scope."
       `varco_core/varco_core/revocation/null.py` — `is_null_revocation_store()` (§D-null, with a
       docstring naming the subclass/wrapper edge) + `__all__`. `registry.py:1017-1019` — the
       three-way merge (§D-require-jti's snippet), body-local import; `_check_revocation`'s
       docstring `Args: require_jti` and `verify()`'s `:813-814` updated ("``None`` → settings;
       settings ``None`` → derived"). `varco_core/varco_core/revocation/posture.py` — both
       `isinstance` sites (`:107-111`) use the helper; compute `effective = settings.revocation_require_jti
       if not None else registry_wired`; `require_jti=effective`, `token_scope_usable=effective`,
       new appended field `require_jti_derived: bool = False` set to `settings.revocation_require_jti is None`;
       docstring rows for the new field and the changed meaning. `varco_fastapi/varco_fastapi/posture.py`
       `AuthPostureCollector`/`_collect_auth` (`:547-580`) — the new INFO `auth.revocation_jti_not_required`
       per §D-require-jti, + a test in `test_posture_collectors.py` (`registry_wired` + env `false`
       → present at INFO; derived → absent; Null → absent). If 048's settings snapshot exists,
       regenerate. **Verify**: Step 8 green; `make test PKG=varco_core`; `make test PKG=varco_fastapi`;
       `uv run pytest varco_redis/tests -q -k revocation` (the Redis store is non-Null — its unit
       tests, if any construct a registry, must set the env or carry a `jti`).

### Phase C — FLIP-034-1

10. [x] `varco_core/tests/test_deprecation.py` (locate; add, red): `test_warn_deprecated_message_shape`
        — `pytest.warns(DeprecationWarning, match=r"^Thing\(x=\.\.\.\) is deprecated since 4\.0\.0 and will be removed in 5\.0\.0\. Use Other instead\.$")`;
        `test_warn_deprecated_blames_caller` — `warnings.catch_warnings(record=True)` → `w.filename`
        is the test file; `test_warn_deprecated_stacklevel_passthrough` — `stacklevel=3` from a
        one-frame helper still names the test file; if 052's ledger exists,
        `fired_deprecations()` contains the record. Then `varco_core/varco_core/deprecation.py` —
        `warn_deprecated()` per §D-apikey (docstring with `Args`/`Raises`/`Edge cases`: "the only
        helper for a deprecated *parameter path*; `stacklevel` counts from the helper's caller"),
        `__all__` `:65`, module docstring's "Two public helpers" → three, and the `rg` line becomes
        `rg 'removed_in="[45]\\.' varco_*/varco_*`. Check `rg -n '"deprecated' varco_core/varco_core/__init__.py`
        — if the lazy map re-exports the helpers, add `warn_deprecated` beside them. Then
        `uv run python scripts/api_surface.py` (regenerate; one addition). **Verify**: green;
        `uv run python scripts/api_surface.py --check` clean.

11. [x] `varco_fastapi/tests/milestone_a/test_server_auth.py` — the tests *about* `keys=`
        (`:76-102`, `:493`, `:536-537`) wrap construction in `pytest.warns(DeprecationWarning,
        match=r"ApiKeyAuth\(keys=")`; **add** `test_keys_path_warns_once_with_since_and_removed_in`
        (exact message, `w.filename` is the test file — proves `stacklevel=3`) and
        `test_hashed_keys_path_emits_no_warning` (`warnings.simplefilter("error")` inside
        `catch_warnings`). Every other in-repo site (§Context row: `test_server_auth.py:204,215,226,443,452,520,526`,
        `test_middleware.py:174`, `test_mcp_auth_middleware.py` ×11, `test_custom_route_params.py:222,267`,
        `test_route_guard.py:277`, `auth/test_auth_posture.py:22,30,38,107` — the last file's
        plaintext-source tests keep one `pytest.warns`-wrapped `keys=` construction because that is
        what they test) → `hashed_keys={hash_api_key("k"): ctx}` (`from varco_core.auth.api_key
        import hash_api_key`). `varco_fastapi/pyproject.toml:136-140` (and any other package the
        `rg -l` in Step 1 found) gains `"error:ApiKeyAuth\\(keys=:DeprecationWarning"`. Then
        `varco_fastapi/varco_fastapi/auth/server_auth.py:418-423` — the `warn_deprecated(...)` call
        (§D-apikey); class docstring `:341-344` → "``hashed_keys=`` is the path; ``keys=`` is
        **deprecated since 4.0.0 and removed in 5.0.0** — it still hashes at construction and emits
        one ``DeprecationWarning``"; `Args: keys` reworded; `posture.py:526-535`'s remediation gains
        the deprecation window. **Verify**: `make test PKG=varco_fastapi` green **with the `error`
        rule in place** (the rule is the proof); `rg -n 'ApiKeyAuth\(keys' varco_*/tests` lists only
        `pytest.warns`-wrapped sites.

### Phase D — FLIP-034-4

12. [x] `varco_fastapi/tests/milestone_a/test_server_auth.py` (red): `test_passthrough_requires_acknowledgement`
        — `PassthroughAuth()` → `pytest.raises(TypeError, match="acknowledge_unverified")`;
        `test_passthrough_false_is_a_type_error` — `PassthroughAuth(acknowledge_unverified=False)`  # type: ignore[arg-type]
        → `TypeError` whose message contains "literally True"; `test_passthrough_signature_is_required_keyword_only`
        — `inspect.signature(PassthroughAuth.__init__).parameters["acknowledge_unverified"]`:
        `.default is inspect.Parameter.empty`, `.kind is KEYWORD_ONLY` (the class-signature guard);
        `test_passthrough_acknowledged_behaves_as_before` — the four existing behaviour tests
        (`:181,190,401,423`) updated to `PassthroughAuth(acknowledge_unverified=True, ...)` and
        otherwise verbatim. `varco_fastapi/tests/auth/test_auth_posture.py:55,64,72,97` updated the
        same way; **add** `test_acknowledged_passthrough_is_still_reported` (`passthrough_auth_bound
        is True`). `test_posture_collectors.py`: an acknowledged instance → `auth.passthrough_bound`
        WARN unchanged. **Verify**: red on the first three.

13. [x] `varco_fastapi/varco_fastapi/auth/server_auth.py:547-548` — §D-passthrough's `__init__`
        (`from typing import Literal`); docstring `:533-541`: `Args: acknowledge_unverified` ("must
        be literally ``True``; the parameter *is* the warning that used to live in this docstring"),
        `Raises: TypeError` (both shapes), a "DESIGN: ``Literal[True]`` + ``TypeError``, not the
        ``bool = False`` + ``ValueError`` precedent (Plan 053 / FLIP-034-4)" block with the
        §D-passthrough ✅/❌. Add the same `__annotations__` restoration trick as `:507-516` **only
        if** the signature test needs a real `Literal` object at runtime (it does not — it checks
        `.default`/`.kind`; skip unless mypy or the test demands it). **Verify**: Step 12 green;
        `make test PKG=varco_fastapi`; `make type-check` (the `# type: ignore[arg-type]` in the
        `False` test is the only new ignore — M1 +1, directional).

### Docs (same commit as the code — feedback memory)

14. [x] `technical_docs/features/credential-and-token-lifecycle.md` — §"S2 / S14" (`:20`): `keys=`
        deprecated 4.0.0 → 5.0.0, `hashed_keys=` the only documented path, the `warn_deprecated`
        shape; §"varco does not mint `jti` automatically" (`:155-161`) → "varco mints `jti` by
        default (4.0)": `mint_jti=False`, per-token override/opt-out, the 3.x-token bridge;
        new §"`require_jti` is derived from the wired store (4.0)": the three-valued merge, the
        Null rule, the wrapper/subclass edge, `auth.revocation_jti_not_required`; §Pitfalls
        (`:163-173`): row 5 ("No `jti` from Auth0/Keycloak/Cognito") → symptom is now "every token
        401s the moment a real store is wired" with fix `VARCO_JWT_REVOCATION_REQUIRE_JTI=false`
        or issuer config; **add** rows: "3.x-minted tokens still valid at upgrade → 401 until
        expiry → set the env var `false` for one token lifetime, then unset"; "`PassthroughAuth()`
        → `TypeError` → `acknowledge_unverified=True`, and expect `auth.passthrough_bound`";
        "`ApiKeyAuth(keys=)` → `DeprecationWarning` → pre-hash with `hash_api_key()`"; "a
        logging wrapper around `NullTokenRevocationStore` derives `require_jti=True` → subclass
        the Null class or set the env var".
      - `technical_docs/features/security-posture.md` — collector table `:62`: `_collect_local`/
        `LocalPostureCollector` gains `posture.authorizer_unbound`; `_collect_auth`/`AuthPostureCollector`
        gains `auth.revocation_jti_not_required`; §`:97-107` (the headline check) rewritten for
        the three-way (`LookupError` → HIGH / other → NOT_ASSESSED / instance → WARN); §"`VARCO_SECURITY_ENV`
        — presentation only" (`:109`) → "**presentation, and the one binding it gates**": the
        036 promise "no finding is ever suppressed by the environment" still holds; what changed
        is that the variable now also decides whether `BaseAuthorizer` exists (one variable, two
        readers, same literal; case-sensitive; a code-configured settings object does not open
        the gate); flip list `:147-170` row 5 → "**landed, Plan 053**"; §Pitfalls (`:173+`):
        "`VARCO_SECURITY_ENV=dev`/`Development` → strict for both readers, and `varco diagnose`
        shows `(VARCO_SECURITY_ENV='dev')`"; "`SecurityPostureModule(settings=SecurityPostureSettings(
        environment='development'))` does not bind `BaseAuthorizer` → export the env var".
      - `technical_docs/features/jwt-claim-transformer.md` §Pitfalls (`:383-387`) — one row:
        "Valid, correctly-signed foreign token → 401 `TokenRevokedError` after wiring a revocation
        store → the issuer emits no `jti` and 4.0 derives `require_jti=True` from a wired non-Null
        store → `VARCO_JWT_REVOCATION_REQUIRE_JTI=false` or map/enable `jti` at the IdP (brief 009 §2)".
      - `technical_docs/features/authorization-audit.md` §Wiring (`:19`): "in production with no
        app authorizer, `enable_authorization_audit()` raises `LookupError` (4.0, FLIP-5) — bind
        the real authorizer first, as the 'call it last' rule already says"; §Pitfalls (`:112`) the
        same as a row.
      - `README.md` — §BaseAuthorizer (`:877-891`): fix the stale `from varco_core.base_authorizer`
        import to `varco_core.auth.authorizer`; replace the "Production guard" `assert` with
        "Development only: `VARCO_SECURITY_ENV=development`. In any other environment this class is
        not a binding — a service that injects `AbstractAuthorizer` fails at startup
        (`ConfigurationError` from the wiring preflight) or at construction (`LookupError`), and
        `SecurityPosture` reports `posture.authorizer_unbound` (HIGH)"; the explicit-instance
        opt-in and its suppression id; §Revocation env table `:4140`: default `unset (derived)`,
        meaning rewritten; one sentence after the wiring snippet `:4133`: "Since 4.0 a wired real
        store derives `require_jti=True`"; §Authority/JWT (the `JwtAuthority` snippet) — one line
        "tokens carry a uuid4 `jti` by default; `from_pem(..., mint_jti=False)` to opt out";
        §API-key hashing (`:4148-4178`): "`keys=` is deprecated (4.0.0 → 5.0.0)"; `:2913`'s
        `PassthroughAuth` mention gains `(acknowledge_unverified=True)`; `:4867` `VARCO_SECURITY_ENV`
        row: "presentation, **and gates the permissive `BaseAuthorizer` binding**".
      - `ARCHITECTURE.md:612` (`BaseAuthorizer … priority -(2**31)` — stale since 045) →
        "`@Fallback @Requires(env=VARCO_SECURITY_ENV, value=development)` — a binding only in
        development"; `:597` ("never depends on a `jti` claim some issuers … do not emit") →
        add "a wired non-Null store derives `require_jti=True` (4.0)"; `:1894-1901` — `keys=`
        deprecated; `PassthroughAuth` acknowledgement.
      - `CLAUDE.md` — §"Authorization — policy engine" Rules: **add** "`BaseAuthorizer` is a
        binding only under `VARCO_SECURITY_ENV=development` (Plan 053 / FLIP-5) — never add a
        second env var to re-enable it; a deliberate permissive production app binds an
        *instance* and suppresses `posture.base_authorizer_bound`." §"Credential and token
        lifecycle" Rules: **add** "`revocation_require_jti` is derived from the *wired* store
        (`None` → `True` iff non-Null); never make the Null store non-Null by wrapping it" and
        "`ApiKeyAuth(keys=)` is deprecated (4.0.0 → 5.0.0); `PassthroughAuth` needs
        `acknowledge_unverified=True`". §"Authority / JWT" snippet: `mint_jti=False` in one
        comment. Keep each to one line — link, don't inline.
      - `examples/00-full-stack-post-api/example/{app.py:216, di.py:37, authorizer.py:29,97,
        service.py:127-128, tests/test_post_service.py:15}` — replace every `-(2**31)`/"auto-
        registered permissive" sentence with the 4.0 statement (a non-fallback app authorizer
        wins in every environment; `BaseAuthorizer` binds only in development). Examples
        01/06/07/08/09/10/17/22 (`rg -n BaseAuthorizer examples/`): each `README.md` run snippet
        gets `VARCO_SECURITY_ENV=development` and the `di.py`/`service.py` docstrings the same
        sentence. `varco_fastapi/varco_fastapi/router/mcp.py:371,867` docstring snippets →
        `hashed_keys=`.
      **Verify**: `rg -n '2\*\*31' README.md ARCHITECTURE.md examples/ varco_*/varco_*` → 0;
      `rg -n 'PassthroughAuth\(\)' README.md technical_docs/ examples/ varco_*/varco_*` → 0;
      `rg -n 'does not mint' technical_docs/ README.md` → 0.

15. [x] `CHANGELOG.md` — under `## [Unreleased]` (`:10`), above the existing blocks:
        `### BREAKING — credential and authorization default flips (Plan 053, FLIP-5 / FLIP-034-1 / FLIP-034-2 / FLIP-034-3 / FLIP-034-4)`
        with five bullets, each naming default-before, default-after, the escape hatch, and the
        posture id: FLIP-5 (quote providify's `LookupError` text and the `ConfigurationError`
        path; `VARCO_SECURITY_ENV=development`; `posture.authorizer_unbound` HIGH); FLIP-034-2
        (`mint_jti=False`; the 3.x-token bridge); FLIP-034-3 (`None`-derived; the three IdPs;
        `VARCO_JWT_REVOCATION_REQUIRE_JTI=false`; `auth.revocation_jti_not_required` INFO;
        `RevocationPostureReport.require_jti_derived`); FLIP-034-1 (`DeprecationWarning`,
        `since="4.0.0", removed_in="5.0.0"`; `warn_deprecated()` added); FLIP-034-4
        (`TypeError`; `acknowledge_unverified=True`). End with "Migration: this plan's §Migration
        (assembled into the 4.0 guide by Plan 058)." `BACKLOG.md:69,73,84,85,86` — **only if the
        launching thread has not already done it**: status → `done (Plan 053)`. **Verify**:
        `rg -n 'Plan 053' CHANGELOG.md BACKLOG.md` → ≥ 1 / ≥ 5.

### Verification

16. [x] `make test` (all eleven suites); `make lint` (no-`PKG` path: `api-check` clean **with the
        regenerated snapshot**, `asyncapi-check` **unchanged**, `import-budget --warn-only` no
        breach); `make type-check` clean (one new `# type: ignore[arg-type]` in a test);
        `rg -n 'VARCO_SECURITY_ENV' varco_*/tests examples/` lists only the explicit per-test
        `setenv`s and example READMEs (no autouse fixture); `rg -n 'removed_in="5\.' varco_*/varco_*`
        → exactly the `ApiKeyAuth` site; `git diff --stat -- varco_fastapi/varco_fastapi/app.py
        varco_fastapi/varco_fastapi/middleware/ design/api-freeze-and-standards/measurements/asyncapi-example.json`
        → empty; the Step 1 one-liner prints `False` with the variable unset and `True` with
        `VARCO_SECURITY_ENV=development`.

## Edge cases

- **`VARCO_SECURITY_ENV=Development` / `dev` / `staging` / `""`** → strict for both readers
  (binding absent, no WARN demotion); `validate()`'s `CONDITION_INACTIVE` message shows the actual
  value (`container.py:6285-6294` per 047). Never "fixed" by normalising — `@Profile` normalises,
  `@Requires` deliberately does not (`metadata.py:253-258`), and `SecurityPostureSettings` never
  did.
- **`SecurityPostureModule(settings=SecurityPostureSettings(environment="development"))` with the
  env var unset** → posture demotes, `BaseAuthorizer` still absent → `posture.authorizer_unbound`
  HIGH (never demoted) is emitted and 052's preflight refuses. The Pitfalls row says "export the
  variable".
- **`BaseAuthorizer` resolved in development, then the variable flipped in-process** → the gate
  is re-checked on every lookup (providify 2.1.1), so the next `get()` raises `LookupError` (fails
  closed); the cached singleton is not evicted (`scope.py:872-876`) — flipping back to
  `development` returns the same instance. Pinned by
  `test_flip_hides_cached_singleton_without_evicting`. A real process never flips its own
  environment; a test that must uses a fresh container.
- **`container.bind(AbstractAuthorizer, BaseAuthorizer)` in production** → still gated (premise
  (3)) — pinned. An operator who copied 3.2's README snippet gets the same `LookupError`, and the
  §Migration row points at `bind_instance`.
- **An app authorizer whose own `__init__` raises `LookupError`** (its dependency is unbound) →
  reported as `posture.authorizer_unbound`; the remediation ("register/fix the binding") is still
  right and 052's `validate()` names the actual missing parameter. Documented in the collector
  docstring.
- **`enable_authorization_audit()` in production with no app authorizer** → `LookupError` at the
  call (`di.py:82`); correct, pinned in Step 4, documented in `authorization-audit.md`.
- **`get_all(AbstractAuthorizer)` / a multi-bound consumer** → the gated fallback is simply absent
  (invisible, not shadowed — `scope.py:982-986`); nothing in-tree multi-binds authorizers.
- **`MultiKeyAuthority` ring mixing `mint_jti` values** → the active authority decides per token;
  not an error; documented on `token()`.
- **A 3.x-minted, still-valid token after upgrade, with a non-Null store wired** → rejected
  (`TokenRevokedError`, reason `"revocation_require_jti=True and token has no jti claim"`) until it
  expires; bridge: `VARCO_JWT_REVOCATION_REQUIRE_JTI=false` for one token lifetime.
- **`verify(raw, revocation_require_jti=None)` explicitly** → identical to omitting it (settings
  → derived); the kwarg's `None` already meant "use settings" in 3.2.
- **Null store wired + `VARCO_JWT_REVOCATION_REQUIRE_JTI=true`** → `jti`-less tokens are rejected
  even though the store never revokes (explicit beats derived, as in 3.2 — pinned).
- **A wrapper around `NullTokenRevocationStore`** → non-Null → derived `True` (§D-null, pinned).
- **`ApiKeyAuth(keys={})`** → still legal, still warns once (an empty mapping is still the
  plaintext path).
- **`PassthroughAuth(acknowledge_unverified=1)`** → `1 is not True` → `TypeError` (identity, not
  truthiness — the annotation is `Literal[True]`).
- **`-W ignore::DeprecationWarning` processes** → the `keys=` warning is swallowed; 052's ledger
  (when landed) still records it via `_note()`.

## Migration (3.x → 4.0; Plan 058 assembles this section verbatim)

| Before (3.2) | After (4.0) | Action if you relied on the old behaviour |
|---|---|---|
| `BaseAuthorizer` (permissive) was auto-bound as `AbstractAuthorizer` in every process; `posture.base_authorizer_bound` warned | Bound **only** when `VARCO_SECURITY_ENV=development` (exact, case-sensitive). Otherwise: startup `ConfigurationError` from the wiring preflight (`MISSING_BINDING … 'authorizer' requests AbstractAuthorizer but no binding is registered`), or `LookupError: No binding found for 'AbstractAuthorizer'. Did you forget container.bind() or container.provide()?` at the first service construction; `SecurityPosture` reports `posture.authorizer_unbound` (HIGH — blocks under `enforce="refuse"` in every environment) | Register a real `AbstractAuthorizer` (an app `@Singleton`, `enable_policy_authorizer()`, …). Development/tests: `export VARCO_SECURITY_ENV=development` (tests: `monkeypatch.setenv`). A deliberate permissive production app: `bind_instance(AbstractAuthorizer, BaseAuthorizer())` **and** `VARCO_SECURITY_SUPPRESS=posture.base_authorizer_bound`. `container.bind(AbstractAuthorizer, BaseAuthorizer)` does **not** work (class markers) |
| `SecurityPostureSettings(environment="development")` in code demoted findings | Still demotes; does **not** bind `BaseAuthorizer` | Export the env var; the code form is presentation only |
| `enable_authorization_audit()` wrapped `BaseAuthorizer` when nothing else was bound | Raises `LookupError` in production with no app authorizer | Bind the real authorizer first ("call it last" was already the rule) |
| `JwtAuthority.token()` minted no `jti`; `.with_random_jti()` was opt-in | Every `token()` carries a uuid4 `jti` | Nothing (signed bytes grow by one claim). Opt out per authority: `from_pem(..., mint_jti=False)`; per token: pass a hand-built `JwtBuilder()` to `sign()`; fixed id: `.token_id("…")` |
| `revocation_require_jti` default `False` — a wired store silently never matched `jti`-less tokens at `TOKEN` scope | Default `None` → **`True` iff the registry's wired store is not `NullTokenRevocationStore`**; Auth0/Keycloak/Cognito tokens (no `jti` by default) are rejected the moment a real store is wired; `RevocationPostureReport.require_jti_derived` says which | Foreign issuer without `jti`: `VARCO_JWT_REVOCATION_REQUIRE_JTI=false` (reported as `auth.revocation_jti_not_required`, INFO) or enable `jti` at the IdP. **Upgrade bridge for 3.x-minted tokens still in flight:** set it `false` for one token lifetime, then unset |
| `ApiKeyAuth(keys={...})` — plaintext hashed at construction, silent | Same behaviour + one `DeprecationWarning` (`since 4.0.0 … removed in 5.0.0`), attributed to your call site; `auth.api_key_plaintext` (WARN) unchanged | `ApiKeyAuth(hashed_keys={hash_api_key(raw, pepper=…): ctx})` — hash offline, never ship the plaintext. Removed in 5.0.0 |
| `PassthroughAuth()` / `PassthroughAuth(required=True)` | `TypeError` — `acknowledge_unverified=True` is a required keyword-only `Literal[True]` | `PassthroughAuth(acknowledge_unverified=True, required=…)`; `auth.passthrough_bound` (WARN) still fires — suppress by id if the gateway deployment is deliberate |

## Docs

Listed in Step 14 (feature docs, README, ARCHITECTURE, CLAUDE.md, examples) and Step 15
(CHANGELOG, BACKLOG). No new `technical_docs/features/*.md` — every flip extends the doc that
already owns its seam. Plan 058 folds §Migration into the consolidated guide.

## Verification

```bash
# Preflight (Step 1) — True today, False after Step 3 with the variable unset
uv run python -c "from providify import DIContainer; from varco_core.auth.base import AbstractAuthorizer; c=DIContainer(); c.scan('varco_core.auth.authorizer'); print(c.is_resolvable(AbstractAuthorizer))"
VARCO_SECURITY_ENV=development uv run python -c "from providify import DIContainer; from varco_core.auth.base import AbstractAuthorizer; c=DIContainer(); c.scan('varco_core.auth.authorizer'); print(c.is_resolvable(AbstractAuthorizer))"   # True

# Phase gates
uv run pytest varco_core/tests/test_base_authorizer.py varco_core/tests/test_service.py -q                                   # A
uv run pytest varco_casbin/tests/test_di.py varco_fastapi/tests/test_posture_collectors.py varco_fastapi/tests/test_posture_core.py -q
uv run pytest varco_core/tests -q -k "jwt or authority or registry or revocation"                                             # B
uv run pytest varco_core/tests/test_deprecation.py varco_fastapi/tests -q                                                    # C, D (error rule active)

# Package + gates
make test
uv run python scripts/api_surface.py            # regenerate (one addition: warn_deprecated)
make lint                                        # api-check clean / asyncapi-check unchanged / import-budget no breach
make type-check

# Boundary checks
rg -n 'VARCO_SECURITY_ENV' varco_*/tests examples/                       # explicit per-test setenvs + example READMEs only
rg -n 'removed_in="5\.' varco_*/varco_*                                  # the ApiKeyAuth site only
rg -n 'ApiKeyAuth\(keys' varco_*/tests                                   # pytest.warns-wrapped sites only
rg -n '2\*\*31' README.md ARCHITECTURE.md examples/ varco_*/varco_*      # empty
git diff --stat -- design/api-freeze-and-standards/measurements/asyncapi-example.json   # empty
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| **A production deployment with no app authorizer stops booting** — the row's purpose, and the largest behaviour break in the flip track | The refusal is loud at *startup* (052's preflight) or at *first construction* (`LookupError`), never a silently permissive request; the fix is one binding or one env var | §Migration row 1 quotes both error texts verbatim; `posture.authorizer_unbound` remediation names the variable; CHANGELOG |
| **providify's `LookupError` text blames the caller** ("Did you forget container.bind()") | The operator can find the real reason in ≤ 1 step | `validate()`'s `CONDITION_INACTIVE` names `VARCO_SECURITY_ENV` and its value; 052's report and `varco diagnose` render it; README/Pitfalls quote the `LookupError` text so it is greppable |
| ⚠️ ASSUMPTION — 052's `run_wiring_preflight()` runs `container.validate()` **before** `SecurityPostureModule.start()` and refuses on `MISSING_BINDING` by default (052 §D-startup row 1) | The startup refusal is the first line; posture is the second | Step 1 records whether 052 is in the tree; if not, the posture HIGH + the first-construction `LookupError` are the two lines until it lands — both fail closed |
| ⚠️ ASSUMPTION — 052's in-memory test profile exists as `varco_core.testing.in_memory_modules()` and needs an authorizer for any `AsyncService` it registers | The profile boots with no env vars | Step 5 binds `BaseAuthorizer()` explicitly there, or leaves the note in 052's plan |
| ⚠️ ASSUMPTION — 046's `testkit/varco_conformance/di_wiring.py` has a "every default resolves" test that enumerates `AbstractAuthorizer` | The suite is green in CI where `VARCO_SECURITY_ENV` is unset | If it enumerates the authorizer, that one test sets `development` explicitly, or the suite's default list excludes conditional defaults (047's catalogue knows which are gated) — decided at Step 5 by reading the suite |
| ⚠️ ASSUMPTION — no in-tree `varco_redis`/`varco_sa`/`varco_beanie` unit test constructs a `TrustedIssuerRegistry` with a real store and a `jti`-less token | `make test` green after Step 9 | Step 9's verify runs `-k revocation` across packages; a hit gets a `jti` or the env var |
| **Foreign-issuer outage on upgrade** (Auth0/Keycloak/Cognito + a real store) | Exactly one env var restores 3.2 behaviour; posture explains it | §Migration row 5; Pitfalls rows in two docs; `auth.revocation_jti_not_required` tells the operator when the escape is in effect |
| **3.x tokens in flight at upgrade** | Rejected only for `TOKEN` scope until expiry; bounded by token lifetime | §Migration bridge; CHANGELOG |
| `Literal[True]` + `TypeError` deviates from the `ValueError` convention | One exception type per guard; the deviation is argued in the docstring | §D-passthrough; the four precedents are untouched |
| The `error:ApiKeyAuth\(keys=` pytest rule regex must match the message *start* | The rule fires on every un-wrapped site | `_build_message()` puts the subject first; Step 11's verify runs the suite with the rule active |
| `warn_deprecated` `stacklevel=3` blames the wrong frame if `ApiKeyAuth.__init__` gains a wrapper (e.g. `deprecated()` on the class) | The warning's `filename` is the caller's | Step 11's `w.filename` assertion |
| `api_surface.py --check` red after Step 10 | Snapshot regenerated with the one addition | Step 10 regenerates; Step 16 runs `make lint` |
| Docs drift — README still shows the 3.2 `assert` and a stale module path | No surviving "auto-registered permissive" claim | Step 14's three `rg` checks |

## Done

- [x] `BaseAuthorizer` carries `@Requires(env="VARCO_SECURITY_ENV", value="development")`;
      `is_resolvable(AbstractAuthorizer)` is `False` with the variable unset and `True` with it;
      the eleven `TestBaseAuthorizerGate` tests pass; `validate()` in production shows one
      `MISSING_BINDING` ERROR per injecting service + one `CONDITION_INACTIVE` INFO.
- [x] `posture.authorizer_unbound` (HIGH) and the narrowed `posture.base_authorizer_bound` (WARN)
      exist with tests, remediation texts, and doc rows; `refuse` blocks on the former in every
      environment.
- [x] `JwtAuthority(mint_jti=True)`: `token()` mints a uuid4 `jti`; `from_pem(..., mint_jti=False)`
      opts out; `sign()` untouched; signature guard test in place.
- [x] `JwtVerificationSettings.revocation_require_jti: bool | None = None`; derived in
      `_check_revocation()` via `is_null_revocation_store()`; `RevocationPostureReport.require_jti_derived`;
      `auth.revocation_jti_not_required` INFO; nine registry/posture tests green.
- [x] `warn_deprecated()` in `varco_core.deprecation`; `ApiKeyAuth(keys=)` warns
      `since="4.0.0", removed_in="5.0.0"` at the caller; the `error:` pytest rule is in place and
      the suite is green under it; api-surface snapshot regenerated.
- [x] `PassthroughAuth(*, acknowledge_unverified: Literal[True], required=False)`; both
      `TypeError` shapes tested; signature guard test in place; in-repo sites updated.
- [x] No autouse `VARCO_SECURITY_ENV` fixture anywhere; the 052 profile (if present) binds the
      permissive authorizer explicitly.
- [x] Docs per Step 14; CHANGELOG per Step 15; BACKLOG rows `FLIP-5`, `FLIP-034-1..4` → `done (Plan 053)`.
- [x] `make test`, `make lint`, `make type-check` green; AsyncAPI snapshot untouched.

## Out of scope / Parked

- **A `Literal["production", "development"]` type on `SecurityPostureSettings.environment`** —
  048's settings recipe; would turn `dev` into a startup error instead of a strict-mode surprise.
  Worth doing there; not here.
- **Adopting `Literal[True]` at the four `acknowledge_*` `ValueError` sites** — a consistency
  sweep 058 may schedule; this plan records the deviation and its reason.
- **Removing `ApiKeyAuth(keys=)`** — 5.0.0, ≥ 12 months after 4.0.0 ships (`CONTRIBUTING.md:98`).
- **`JwtBuilder.without_jti()`** — not needed: a hand-built builder is the per-token opt-out;
  add only if a caller who *must* use `authority.token()` (for a pre-seeded profile/claims) also
  needs no `jti` — no such caller exists in-tree.
- **Auto-wiring the DI-bound revocation store into `TrustedIssuerRegistry`** — 050/049; 052
  reports the gap as `di.revocation_store_not_wired`.
- **A posture finding for "`mint_jti=False` on a production authority"** — a `jti`-less issuer is
  not a security fact by itself (SUBJECT/TENANT/ISSUER scopes still work); revisit if a consumer
  asks.
