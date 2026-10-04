# Tenant identity provenance & delegation — `TenantSource` chain, membership binding, act-as

Plan 033 (3.2 security release, BACKLOG rows **S6**, **S5**, **S16**). Research brief backing
this feature: `design/research/006-multi-tenant-identity-and-hardening.md`.

**Since 4.0 (Plan 055) tenant resolution defaults to the chain.** `TenantResolutionMiddleware(chain=None)`
means *the configured chain* — `VARCO_TENANT_SOURCES` when hand-wired, the container-resolved
chain when `TenancyModule` builds it. Only with nothing configured does it fall back to the
implicit `X-Tenant-Id` `LegacyTenantSource` — deprecated (`since="3.2.0"`, `removed_in="5.0.0"`),
and reported as `tenant.no_chain` + `tenant.legacy_source_implicit` (WARN, a production
start-blocker). Membership fails closed (`on_missing_claim=DENY`), and `RequestContextMiddleware`
never sets the tenant. See [the 4.0 flip list](#the-40-flip-list) and
[Wiring tenant resolution in 4.0](#wiring-tenant-resolution-in-40).

## Multi-binding the pool (Plan 051 / DI-5)

`TenantSource` is `@Multibound` — an out-of-tree source is a scanned `@Singleton` (or a
`varco_core.di.collect.contribute()` call), never a framework edit. **Bound ≠ active**: a
contributed source only joins the chain when its `name` is also listed in
`VARCO_TENANT_SOURCES`/`TenantProvenanceSettings.sources` — the allowlist (which origins may
speak at all) stays the operator's, never the scanner's. `build_tenant_source_chain(settings, *,
delegation_policy=None, sources=())` gained the `sources=` pool argument; a name in
`settings.sources` matching neither a built-in nor a pool source raises `ValueError` naming both
sets; a pool source whose `name` collides with a built-in (`jwt`/`subdomain`/`legacy`/`act_as`)
is refused even if that built-in is not itself active; two pool sources sharing one `name` is
refused. `resolve_tenant_source_chain(container)` (`varco_core.tenancy`) does the container
reading for you — settings, the contributed pool, and an optional bound `DelegationPolicy` — and
logs every inert (bound but unnamed) pool source at `INFO`. A non-built-in source in the active
chain declaring `TenantTrust.HIGH` or higher emits the posture token
`tenant.third_party_source_high_trust` at `INFO` — the one way a plugin can *elevate* itself into
the identity chain, surfaced to the operator, never blocked.

## The trust-ranking table (brief 006 §1)

| Source | `TenantTrust` | Why |
|---|---|---|
| A bare client-supplied header (`X-Tenant-Id`) | `LOW` | No auth binding — any client that can reach the service can claim any active tenant |
| A trusted-proxy-supplied value (`X-Forwarded-Host`) | `MEDIUM` | One extra, unverifiable hop |
| A subdomain read from the connection's own `Host` | `HIGH` | Anchored in the TCP/TLS connection, but `Host` is still client-controlled |
| A signed, issuer-bound JWT claim | `HIGHEST` | Anchored in the token's signature |

`varco_core.tenancy.source.TenantTrust` is a real `IntEnum` — `LOW < MEDIUM < HIGH < HIGHEST` —
so the chain's winner-among-agreeing-claims and equal-trust tie-break are ordinary comparisons.

## The chain, in one picture

```
                 ┌──────────────────┐
Host / headers → │  TenantSource(s)  │ → TenantClaim | None   (never raises, §D-S6-abc)
AuthContext    → │  (JWT / subdomain  │
(verified)       │   / legacy / act_as)│
                 └──────────────────┘
                          │
                          ▼
              TenantSourceChain.resolve()
                          │
                          ▼
                  TenantProvenance
        (tenant_id, winner, claims, conflict,
         rejected, rejection_reason,
         membership, delegation)
```

`TenantSource.resolve()` is **sync, pure, and must never raise** — it takes a frozen,
transport-neutral `TenantRequest` (headers already lower-cased, `Host` already port-stripped,
an already-**verified** `AuthContext` or `None`), so `varco_core.tenancy` never imports
`starlette`/`fastapi` (the same seam rule as `varco_core.migration`). `TenantSourceChain.resolve()`
runs every configured source, drops anything below `min_trust`, and returns one `TenantProvenance`
verdict — turning that verdict into an HTTP response is the adapter's job
(`varco_fastapi.middleware.tenant_resolution.TenantResolutionMiddleware`).

## The two cross-check modes (§D-S6-oq2)

| Mode | Rule |
|---|---|
| `LENIENT` (default) | Two present values that **disagree** reject. Absence never rejects. |
| `STRICT` | Additionally: if **any** source produced a value, at least **two** must agree. |

**Zero claims is always a pass-through, in both modes.** `STRICT` only bites when ≥1 source
spoke and <2 agreed — this is what makes it deployable at all: `GET /health`, `GET /metrics`,
the OpenAPI JSON, an OAuth callback on the apex domain, and every unauthenticated public route
produce zero claims and are structurally exempt, with no path allowlist to maintain and get
wrong. `LENIENT` still rejects the disagreement case an attacker constructs (a forged header
that disagrees with a real token, or vice versa) — `STRICT` adds coverage of a *stripping*
attack (suppress the stronger source so only a weaker one remains), which needs the weaker
source in the chain at all. `STRICT` is a 4.0 default candidate, not a 3.2 one — see the flip
list below.

## The subdomain algorithm and the base-domain rule (§D-S6-oq3)

`SubdomainTenantSource(base_domains=..., *, trust_forwarded_host=False, forwarded_host_header=
"X-Forwarded-Host", reserved_labels=frozenset({"www","api","app","admin","static","cdn"}))`.
`base_domains` is **required** — empty raises `ValueError` at construction; there is no
public-suffix-list dependency (`publicsuffix2` was rejected — it is a data file that goes stale,
and the failure mode of a stale entry is the exact tenant-confusion bug this source exists to
avoid).

1. Take `request.host` (the connection's own `Host`). Read `forwarded_host_header` **only** when
   `trust_forwarded_host=True`, and then emit `MEDIUM` rather than `HIGH`.
2. Normalise: lower-case, strip the port, strip one trailing dot, then `label.encode("idna")`
   per label (stdlib codec only). Any `UnicodeError` (empty/over-63-char label) → `None`, never
   raise.
3. Exact suffix match against `base_domains`, **longest first** — `eu.example.com` wins over
   `example.com` for `acme.eu.example.com`.
4. The remaining `label` must be a **single DNS label** — `a.b.example.com` → `None`. Never join,
   never take the leftmost.
5. `label` in `reserved_labels`, or the bare base domain itself → `None` (not an error).

⛔ **`SubdomainTenantSource` is not by itself a security control. Its security value is that it
disagrees with a forged JWT claim, and that a forged host disagrees with a real one. Deploy it
behind `TrustedHostMiddleware` (`allowed_hosts=[...]`, Starlette's own) or an edge that rejects
unknown `Host` values.**

## The membership model — fail-closed since 4.0

`varco_core.tenancy.membership.AbstractTenantMembership.check(ctx, tenant_id) ->
MembershipDecision` decides whether the authenticated subject may act as the tenant a chain
resolved. `NullTenantMembership` is the scanned DI default (always allows); opt in
`ClaimTenantMembership` via `enable_tenant_membership(container)`.

`ClaimTenantMembership` allows when:
- `tenant_id` is in the `tenants` metadata list (the normal multi-org case); **or**
- there is **no** `tenants` claim at all **and** the token's own `metadata["tenant_id"]` equals
  `tenant_id` (a single-tenant token is its own membership proof, `reason="self_tenant"`); **or**
- the claim is entirely absent and `on_missing_claim=ALLOW` (an explicit opt-out since 4.0 —
  `reason="claim_absent"`, one process-level `WARNING`, posture `tenant.membership_missing_claim_allows`
  HIGH).

Everything else denies — **including** a `tenants` list that is present but does not contain the
requested tenant, even if `metadata["tenant_id"]` happens to equal it. An explicit membership
list is authoritative once present; falling back to "well, `tenant_id` matches" in that case
would make the multi-org list decorative.

`CanonicalClaim.TENANTS` (`varco_core.jwt.transform`) is the claim-mapping member that gets a
foreign `tenants`/`organizations`/`orgs` claim into `AuthContext.metadata["tenants"]` — reusing
the existing per-issuer claim-transform mechanism (`VARCO_JWT_TRANSFORM_TENANTS_FIELD` /
`VARCO_JWT_TRANSFORM__<LABEL>__TENANTS_FIELD`), never a second one.

**Fail-closed since 4.0 (Plan 055 / FLIP-7).** `on_missing_claim` defaults to `DENY` at all
three sites (the settings field, `ClaimTenantMembership.__init__`, the env default) — a caller
whose token has no `tenants` claim is denied with `reason="claim_absent"`, and one process-level
`WARNING` names the policy so an operator can find out why a fleet 403s. **The self-tenant path
is unchanged**: a single-tenant token whose `tenant_id` equals the resolved tenant is still its
own proof, so a single-tenant fleet keeps working with **no IdP change**. The 3.2 behaviour is
one explicit opt-out away — `VARCO_TENANT_MEMBERSHIP_ON_MISSING=allow` — and because
`tenant.membership_missing_claim_allows` is HIGH it blocks a `refuse` start until acknowledged
with `VARCO_SECURITY_SUPPRESS=tenant.membership_missing_claim_allows` (the sanctioned, time-boxed
IdP-rollout shape).

## The two Plan-036 seams

⚠️ **This plan defines and exports both seams below. It builds neither the cross-tenant admin
guard nor the introspection preflight — Plan 036 owns both.**

**(a) `assert_tenant_matches(requested, *, allow_cross_tenant=False) -> str` /
`CrossTenantAccessError`** (`varco_core.tenancy.provenance`) — the seam Plan 036 / S4's
cross-tenant admin guard consumes, one call per admin route:
`tenant = assert_tenant_matches(body.get("tenant_id"), allow_cross_tenant=ctx.has_role(role))`.
`CrossTenantAccessError.error_params()` returns `{"requested": ...}` **only** — the resolved
tenant is never included, the same exfiltration rule as `ServiceAuthorizationError` excluding
`reason`.

**(b) `inspect_tenant_provenance(chain, *, membership=None, delegation=None,
request_context_sets_tenant=False) -> TenantProvenancePosture`** (`varco_core.tenancy.posture`) —
the seam Plan 036 / S9's preflight reports on. Pure: no I/O, no ambient reads, safe to call at
startup. Its `findings` tuple is a pinned, stable set of tokens (`tenant.no_chain`,
`tenant.legacy_source_implicit`, `tenant.legacy_source_explicit`, `tenant.single_source`,
`tenant.cross_check_lenient`, `tenant.no_membership_provider`,
`tenant.membership_missing_claim_allows`, `tenant.subdomain_trusts_forwarded_host`,
`tenant.unchained_claim_tenant_setter`, `tenant.delegation_unbound`) — 036 formats these into
human text without string-matching this plan's prose. Since 4.0 `request_context_sets_tenant`
defaults `False` (varco's own middleware never sets the tenant); pass `True` only for a
hand-wired setter. The pure inspector reports what it is given — the `NullTenantMembership`
narrowing lives in `TenantPostureCollector`, not here.

## `LegacyTenantSource`: what it does and does not give you

`LegacyTenantSource` (default header `X-Tenant-Id`) is the named, documented escape hatch —
**it binds nothing to the authenticated caller**. Any client that can reach the service can
claim any active tenant. It is `TenantTrust.LOW`, the ❌ LOWEST row of the trust table above, and
it is safe **only** behind an ingress that strips a client-supplied `X-Tenant-Id` and re-appends
a verified value over an authenticated channel.

The **explicit** source — `TenantSourceChain(sources=(LegacyTenantSource(),))` in code, or
`VARCO_TENANT_SOURCES=legacy` — is permanent: it warns not at all, and reports
`tenant.legacy_source_explicit` (WARN), so production also needs
`VARCO_SECURITY_SUPPRESS=tenant.legacy_source_explicit` (the opt-out and its acknowledgement).
The **implicit** fallback (`chain=None` with nothing configured) is deprecated since 3.2.0 and
removed in **5.0.0** — `CONTRIBUTING.md`'s 12-month floor from 3.2.0 (2026-09-12) forbids
removing it before 2027-09-12, so the `TypeError` the 3.2 warning promised moves to 5.0.0.

## Act-as / RFC 8693 (S16)

varco **consumes** an already-issued, already-verified delegation token — it never issues one
(token exchange is the IdP's job; Okta/Entra/Keycloak/ZITADEL all ship it). `act` is already
parsed into `AuthContext.metadata["actor"]` by `varco_core.jwt.parser`; this feature adds policy
and mandatory audit on top of that already-parsed claim.

```python
from varco_core.auth.delegation import ActorContext, AllowlistDelegationPolicy
from varco_core.tenancy.sources import ActAsTenantSource

policy = AllowlistDelegationPolicy(grants={"svc-billing": frozenset({"acme", "beta"})})
source = ActAsTenantSource(policy, requested_from="X-Act-As-Tenant")
```

`ActAsTenantSource` emits a claim at `TenantTrust.HIGHEST` **only** when the token carries an
`act` claim *and* the bound policy allows the requested tenant — a bare impersonation token
(`sub` swapped, no `act`) can never take this path (delegation, not impersonation, is the only
supported shape — brief 006 §3 marks impersonation "⚠️ Risky; rarely used for tenant scoping").

**Deny-by-default and unlogged-is-impossible** (§D-S16-shape, motivated by CVE-2025-55241, where
Entra's actor tokens were issued with no audit trail of who asked to impersonate whom): no policy
bound → no claim, ever; and **every** decision — allow *and* deny* — emits a `DelegationRecord`
(logged at INFO with both principal and actor, and attached to `TenantProvenance.delegation`).
A denied or unbound act-as attempt **rejects the request** (`rejection_reason ==
"delegation_denied"`) rather than silently falling through to "no tenant resolved".

No token-exchange **endpoint** ships — issuing an actor token is the IdP's job. An app whose IdP
lacks RFC 8693 can build its own internal issuer with the existing
`JwtBuilder.claim("act", {"sub": "svc-a"})` — no builder change needed.

## The 4.0 flip list

| # | 4.0 change | 3.2 signal |
|---|---|---|
| 1 | `TenantResolutionMiddleware(chain=None)` → `TypeError`; the implicit `LegacyTenantSource` fallback is removed. **Landed with the floor, Plan 055** — the default is the configured chain; the implicit fallback is deprecated `since="3.2.0", removed_in="5.0.0"` (not before 2027-09-12); production blocks via `tenant.no_chain`/`tenant.legacy_source_implicit` | `DeprecationWarning` at construction + posture finding `tenant.legacy_source_implicit` |
| 2 | `ClaimTenantMembership.on_missing_claim` default `ALLOW` → `DENY` — **landed, Plan 055** | one `WARNING` per process + `reason="claim_absent"` on every decision |
| 3 | `RequestContextMiddleware.enable_tenant_context` defaults `True` → `False`; setting the tenant from a claim without a chain, a catalog check or a membership check is removed — **landed, Plan 055**: both kwargs removed; `TenancyModule` + `VARCO_TENANT_SOURCES=jwt` is the replacement; `catalog`/`pool` optional for `SHARED` | posture finding `tenant.unchained_claim_tenant_setter` |
| 4 | `CrossCheckMode.STRICT` becomes the default — **recorded non-flip** (036 row 8); `tenant.cross_check_lenient` stays INFO | posture finding `tenant.cross_check_lenient` |

## Wiring tenant resolution in 4.0

`TenancyModule` (`varco_fastapi.modules`) is the composition root: it contributes
`TenantResolutionMiddleware` at `EdgeSlot.TENANT_RESOLUTION` (`95` — inside `ErrorMiddleware`
and the pre-auth rate limit, outside `RequestContextMiddleware` and `LocalizationMiddleware`)
and contributes the `TenantPostureCollector`, both from the same resolved inputs:

```python
from varco_fastapi import default_modules
from varco_fastapi.modules import TenancyModule

# SHARED isolation, JWT tenant claim, no control plane: catalog/pool are optional.
app = create_varco_app(container, modules=[TenancyModule(), *default_modules()])
# + VARCO_TENANT_SOURCES=jwt   (+ VARCO_TENANT_CLAIM_METADATA_KEY=<claim> if not "tenant_id")

# With a control plane: TenancyModule(catalog=catalog, pool=pool, supervisor=...)
```

`chain=` / `server_auth=` / `membership=` default to the container's resolution
(`resolve_tenant_source_chain(container)` — which sees 051's multi-bound `TenantSource`
plugins and a bound `DelegationPolicy` — the bound `AbstractServerAuth`, the bound
`AbstractTenantMembership`); an explicit value wins; when the container yields no chain the module reads
`VARCO_TENANT_SOURCES` itself and passes the result explicitly, so posture and the middleware
always agree. `pool=None` under `SCHEMA`/`DATABASE` isolation **refuses startup**
(`ConfigurationError` from `async_init()`, before any `start()`, regardless of
`strict_validation`). `tenant.*` posture findings exist **only** when `TenancyModule`
is present — a non-tenant app emits none.

The **hand-wired** form — `MiddlewareModule([(TenantResolutionMiddleware, {...})])` (slot
`EXTRA`, the 3.x position, outside `ErrorMiddleware`) — still works: `chain=None` reads
`VARCO_TENANT_SOURCES` itself. It cannot see multi-bound plugin sources (naming one is a
`ValueError` pointing at `chain=`/`TenancyModule`), and it is invisible to posture.

## Enabling tenant membership via env (Plan 047)

`VARCO_TENANT_MEMBERSHIP_ENABLED=true` is the env twin of
`enable_tenant_membership(container, settings=None)` — a `@Requires`-gated `@Fallback` provider
(`tenant_membership_when_enabled`, `varco_core.tenancy.di`) that reads
`TenantMembershipSettings.from_env()` directly (the same default `enable_tenant_membership()`
itself uses when no explicit `settings=` is given). Code always wins over the env form. See
`technical_docs/features/dependency-injection.md`'s "Enabling a feature" section.

## Pitfalls

| Pitfall | Why it bites |
|---|---|
| Two tenant setters existed pre-3.2 | `TenantResolutionMiddleware` (header) and `RequestContextMiddleware` (JWT claim, on by default) both set the tenant, with no cross-check — the inner one silently won. Since 4.0 the claim setter is gone; `TenantResolutionMiddleware` is the only HTTP writer of `current_tenant()` |
| A hand-wired placement at `EdgeSlot.EXTRA` sits **outside** `ErrorMiddleware` | An `HTTPException` from `server_auth` inside `TenantResolutionMiddleware` would otherwise surface as Starlette's default plain-text response — this middleware catches it itself and returns JSON. `TenancyModule`'s slot is inside `ErrorMiddleware` |
| `TenancyModule()` with nothing configured refuses in production | `tenant.no_chain` + `tenant.legacy_source_implicit` (WARN) block a `production` start, and so does `tenant.no_membership_provider` (WARN) unless membership is enabled — **fix:** `VARCO_TENANT_SOURCES=jwt` + `enable_tenant_membership(container)`, or explicit legacy (`VARCO_TENANT_SOURCES=legacy`) + `VARCO_SECURITY_SUPPRESS=tenant.legacy_source_explicit` (and membership, or suppress `tenant.no_membership_provider`) |
| Tokens without a `tenants` claim 403 with `claim_absent` since 4.0 | `on_missing_claim` defaults `DENY` — **fix:** Emit the claim (or rely on self-tenant), or `VARCO_TENANT_MEMBERSHIP_ON_MISSING=allow` + suppress the HIGH id during rollout |
| `current_tenant()` is `None` after upgrade | The `RequestContextMiddleware` JWT-claim setter is gone — **fix:** `TenancyModule` + `VARCO_TENANT_SOURCES=jwt` (+ `VARCO_TENANT_CLAIM_METADATA_KEY` if you used `tenant_field`) |
| Hand-wired `TenantResolutionMiddleware` is invisible to posture | Only `TenancyModule` contributes the `TenantPostureCollector` — **fix:** Use `TenancyModule` |
| Rejected requests now appear in logs/metrics/traces | The module slot is inside `ErrorMiddleware`/logging/tracing/metrics — **fix:** Intended; `MiddlewareModule([...], slot=EXTRA)` keeps the 3.x position |
| `VARCO_TENANT_SOURCES=partner` (a plugin source) with a hand-wired middleware | `ValueError` at construction — the env-only builder cannot see multi-bound sources — **fix:** The module path (`TenancyModule`), or pass `chain=` |
| Every tenant app must adopt membership or suppress | `NullTenantMembership` (the scanned default) counts as *no* provider → `tenant.no_membership_provider` WARN — **fix:** `enable_tenant_membership(container)`, or suppress the id for a deliberate single-tenant deployment |
| `TenancyModule` **and** a `MiddlewareModule`-placed `TenantResolutionMiddleware` | Two instances at two slots; each resolves. Not guarded — **fix:** Pick one — `varco diagnose` shows both specs |
| `Host` is client-controlled | Nothing in the repo validates it — `SubdomainTenantSource` is not a security control by itself (see above) |
| `base_domains=("co.uk",)` | `example.co.uk` yields tenant `"example"` — varco trusts your base-domain list the way it trusts your DSN, and cannot detect a public-suffix mistake |
| `a.b.example.com` yields nothing, by design | Never `"a"`, never `"a.b"` — the single-label rule closes the multi-label-prefix variant |
| `STRICT` + a single-API-host client | A fleet-wide 403 for any legitimate caller that only ever presents one source. Enumerate every one-source path before flipping the mode |
| `on_missing_claim=allow` set explicitly | An opted-in app whose IdP silently stops emitting `tenants` degrades to no enforcement — reported as HIGH `tenant.membership_missing_claim_allows` |
| A `tenants` claim silently absent from a re-issued token | Since 4.0: every cross-tenant request 403s with `reason="claim_absent"` (plus one process-level `WARNING`) — fail-closed, not silent |
| Two different `server_auth` instances across the two middlewares | The outer (chain) middleware's verdict wins; pass the same instance (or `None` to the chain middleware, restoring independent authentication) |
| A delegated request with no policy bound | **Denied**, not allowed — deny-by-default, never an `allow_all` |
| Impersonation (`sub` swapped, no `act`) expected to work as act-as | Unsupported **by design** — `ActorContext.from_metadata` returns `None` without an `act` claim, so a bare impersonation token can never reach `ActAsTenantSource`'s policy check at all |
| An unlogged delegation decision | Never happens — every `ActAsTenantSource.resolve()` call (allow **and** deny **and** unbound) emits exactly one `DelegationRecord` and one INFO log line naming both principal and actor; treat a missing log line as a bug, not a quiet feature (§D-S16-shape, CVE-2025-55241) |
| Assuming delegation is allow-all unless configured otherwise | It is the opposite — no `DelegationPolicy` bound means every act-as attempt is denied; `AllowlistDelegationPolicy` grants must be written per actor, and `"*"` must be explicit |

## Env vars

See README's "Tenant identity provenance" section for the full `VARCO_TENANT_*` table.
`VARCO_TENANT_MEMBERSHIP_ON_MISSING` defaults to `deny` since 4.0 (Plan 055 / FLIP-7).

## See also

- `varco_core/varco_core/tenancy/source.py`, `sources.py`, `provenance.py`, `posture.py`,
  `membership.py`, `settings.py`, `di.py`
- `varco_core/varco_core/auth/delegation.py`
- `varco_fastapi/varco_fastapi/middleware/tenant_resolution.py`,
  `middleware/request_context.py`
- `technical_docs/features/multitenancy.md` — "Where the tenant comes from"
- `technical_docs/features/jwt-claim-transformer.md` — `CanonicalClaim.TENANTS`
