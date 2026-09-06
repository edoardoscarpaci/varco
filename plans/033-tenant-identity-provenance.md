# Plan 033 — Tenant identity provenance & delegation: `TenantSource` chain (S6), membership binding (S5), act-as (S16)

Covers BACKLOG 3.2 rows **S6** (🔴 must, M–L — ⭐ *the centerpiece*), **S5** (🔴 must, M —
*tenant↔subject membership binding*) and **S16** (🟢 nice, M — *act-as / RFC 8693*), and answers
the cycle's **open questions 2** (§D-S6-oq2) and **3** (§D-S6-oq3).

**Research brief backing this plan:**
`design/research/006-multi-tenant-identity-and-hardening.md`. Every externally-grounded claim
below cites it by section (`brief 006 §N`).

## Scope and siblings

One of five plans in the 3.2 security release. This slice is the **longest pole** and the one that
changes a security *model*, not just a default. It builds third (index §Build order) but is
planned early.

| Plan | Rows | Boundary with this plan |
|---|---|---|
| 034 | S1, S2, S13, S14 | No overlap. S1 makes `algorithms=` required on `JwtParser.parse()`; this plan never calls `parse()` directly — it consumes an already-verified `AuthContext` |
| 035 | S3, S7, S8, S10 | No overlap. ⚠️ §D-S6-wiring's error path depends on S3's opaque-error fix landing *or not*; it is written to be correct either way |
| 036 | S4, S9, S11 | **Hard edge, both directions of information.** This plan **defines and exports** the resolved-tenant seam (S4) and the introspection seam (S9), and **builds neither the admin guard nor the preflight**. §D-036-seams is the contract |
| 037 ✅ | S12, S15 | **Soft, one-way.** 037 consumes `current_tenant()` as an input contract. This plan changes *what feeds it*; `current_tenant()`/`tenant_context()` themselves are **not touched** |

**The locked decisions are binding and are not relitigated here** (`BACKLOG.md:51-58`):
`LegacyTenantSource` ships as a named escape hatch; sources in scope are JWT claim + subdomain +
legacy header (mTLS parked); membership is a signed-claim list with **no external lookup**.

Phases 0–2 are S6. Phase 3 is S5 (it depends on S6 — `BACKLOG.md:80`). Phases 4–5 are the 036
seams and docs. **Phase 6 is S16 and is separable and droppable** — nothing in Phases 0–5 imports
it, and §D-S16-cut states the exact fallback and the un-park trigger.

## Goal

A varco app can state, in configuration, **where a request's tenant identity is allowed to come
from** — an ordered chain of sources ranked by trust — and get a request rejected when two sources
disagree, when the authenticated subject is not a member of the tenant it asked for, or when a
service tried to act for a tenant it was never delegated. The trusted value varco already parses
(`AuthContext.metadata["tenant_id"]`) becomes connected to the routing decision it has never
been connected to. An app that changes nothing gets byte-identical behaviour plus one construction-
time warning.

## Non-goals

- **Nothing flips by default in 3.2.** `TenantResolutionMiddleware(chain=None)` — the only shape
  that exists today — keeps reading `X-Tenant-Id` and behaves identically, now through an
  internally-constructed `LegacyTenantSource`. The flip is 4.0 (§D-S6-blast, and the flip list).
- **No `SecurityPosture` preflight and no admin guard.** Plan 036 owns both (§D-036-seams).
- **No change to `current_tenant()` / `tenant_context()`** (`varco_core/varco_core/service/tenant.py:151-199`).
  This plan changes what *calls* them. 037's input contract is preserved verbatim.
- **`RequestContext` still never holds the tenant** (CLAUDE.md). `TenantProvenance` is a separate
  `AmbientVar` describing *how the tenant was decided*; `current_tenant()` remains the single
  answer to *who the tenant is*. §D-S6-provenance argues this is composition-by-ordering, not
  containment.
- **No second claim-mapping mechanism.** Per-issuer tenant-claim naming is
  `varco_core.jwt.transform`, which already ships it (`transform/config.py:59-81` maps
  `CanonicalClaim.TENANT_ID` with `VARCO_JWT_TRANSFORM__<LABEL>__TENANT_FIELD`). Phase 3 adds
  **one enum member** to that mechanism; it does not build a new one.
- **No mTLS / `X-Forwarded-Client-Cert` source, no path-based source, no DPoP, no SPIFFE.**
  Parked (`BACKLOG.md:96-100`). The ABC is shaped so each is an additive out-of-tree
  implementation — asserted by §D-S6-abc's transport-neutral `TenantRequest`.
- **No RFC 8693 token-exchange *endpoint*.** Phase 6 *consumes* an exchanged token's `act` claim.
  Issuing one is the IdP's job (brief 006 §3: *"Use RFC 8693 token exchange natively if your IdP
  supports it"*).
- **No new runtime dependency anywhere.** In particular no `publicsuffix2` (§D-S6-oq3) and no
  `idna` package — stdlib `str.encode("idna")` only.
- **No new conformance module.** Decided explicitly in §D-S6-conformance, with two
  `COVERAGE.md` rows so the absence is a record rather than an oversight.

---

## Design

### What already exists — and the four corrections it forces

Scout-verified; every line below was read while writing this plan.

| Fact | Location | Consequence |
|---|---|---|
| `TenantResolutionMiddleware` reads the header and feeds `tenant_context()` after a catalog-status check only | `varco_fastapi/varco_fastapi/middleware/tenant_resolution.py:59-85` (header read at `:60-61`) | The bug the cycle exists to fix. It is opt-in already (no `create_varco_app` slot) |
| **`RequestContextMiddleware` ALREADY sets the tenant from the JWT claim** — `ctx.metadata[tenant_field]` → `_maybe_tenant_context()` — and it is **on by default** (`enable_tenant_context=True`) | `varco_fastapi/varco_fastapi/middleware/request_context.py:103-154`, helper at `:160-179` | ⚠️ **Correction 1, and it reframes the row.** There are **two** tenant setters today, not one |
| `extra_middleware=` is added **after** `ErrorMiddleware`/`RequestContextMiddleware`, so an app-supplied `TenantResolutionMiddleware` runs **outside** both | `varco_fastapi/varco_fastapi/app.py:492-544`; documented and verified in `technical_docs/features/timezone-handling.md:306-334` | ⚠️ **Correction 2.** The header tenant is entered *first*; the JWT-claim tenant then **nests inside and silently overrides it**. Nobody documents this. It also means auth exceptions raised there escape `ErrorMiddleware` |
| `AuthContext.metadata` is populated with **exactly two** keys — `tenant_id` and `actor` | `varco_core/varco_core/jwt/parser.py:383-395` | ⚠️ **Correction 3.** A `tenants`/`orgs` **list** claim promoted via `ClaimMapping.metadata_fields` lands in `canonical` (`transform/mapping.py:298-301`) but **never reaches `AuthContext.metadata`**. S5's default has no data source without Phase 3's one-line addition |
| The RFC 8693 `act` claim is **already** mapped — `CanonicalClaim.ACTOR`, default source `"act"`, → `metadata["actor"]` | `transform/mapping.py:53,62`; `transform/config.py:65,79`; `parser.py:362,386-387` | S16 is **not** "parse `act`". It is *policy + audit + a source* on top of an already-parsed claim |
| Per-issuer claim mapping exists in full — flat `VARCO_JWT_TRANSFORM_*` and labelled `VARCO_JWT_TRANSFORM__<LABEL>__*`, selected by `iss` | `transform/config.py:46-56`; `parser.py:257-259` | brief 006 §1's *"no standardized tenant claim name"* is already solved. **Reuse, add nothing** |
| **Nothing in the repo reads `X-Forwarded-*` or installs `TrustedHostMiddleware`** — zero hits across all ten packages | `rg "X-Forwarded\|TrustedHost\|forwarded_allow" --type py` → no matches | A subdomain source must design its own `Host`-spoofing posture from scratch (§D-S6-oq3) |
| `AbstractServerAuth.__call__(request) -> AuthContext` is a plain callable, run by `RequestContextMiddleware` at middleware time (not only as a route dependency) | `varco_fastapi/varco_fastapi/auth/server_auth.py:76-107`; called at `request_context.py:141` | A *verified* `AuthContext` **is** obtainable at middleware time. §D-S6-wiring depends on this |
| `AmbientVar[T]` is the sanctioned request-scoped ambient primitive (`get`/`set_for_task`/`scope`) | `varco_core/varco_core/context/ambient.py:56-127` | `TenantProvenance` uses it; no hand-rolled `ContextVar` |
| `webhook/router.py` trusts `X-Tenant-Id` **independently of any middleware**, and `create_subscription` takes `tenant_id` from the request **body** | `varco_fastapi/varco_fastapi/webhook/router.py:134-136`, `:151-152` | S4's target. This plan exports the seam; **036 fixes the routes** |
| `TenancySettings` is a plain frozen dataclass with a hand-written `from_env()` | `varco_core/varco_core/tenancy/settings.py:72-170` | The house shape for `TenantProvenanceSettings`. ⚠️ **Correction 4**: it is **not** pydantic — do not copy `JwtTransformSettings`' `VarcoSettings` shape into `varco_core.tenancy` |

### Phase order

```
P0  S6a  🔴 M  varco_core.tenancy.source — the transport-neutral primitives
               (TenantRequest, TenantClaim, TenantTrust, TenantSource, chain, provenance)
P1  S6b  🔴 M  the three sources: JwtClaimTenantSource, SubdomainTenantSource,
               LegacyTenantSource   ← answers OQ3
P2  S6c  🔴 M  varco_fastapi wiring — TenantResolutionMiddleware(chain=),
               RequestContextMiddleware deferral, TenantProvenanceSettings
───────────────────────────── S6 ends here ────────────────────────────────────
P3  S5   🔴 M  CanonicalClaim.TENANTS + AbstractTenantMembership + the two
               implementations + enable_tenant_membership()
P4  ——   🔴 S  the two Plan-036 seams + api-surface regeneration
P5  ——   🟡 S  docs, README, CLAUDE.md, CHANGELOG, upgrade note, BACKLOG OQ2/OQ3 (same commit)
────────────── S6+S5 shippable here; everything below is droppable ────────────
P6  S16  🟢 M  act-as: ActorContext, DelegationPolicy, ActAsTenantSource,
               mandatory delegation audit   ← DROPPABLE (§D-S16-cut)
```

**P0→P1→P2 is one indivisible seam.** P0 alone ships an ABC nothing implements; P1 alone ships
sources nothing calls. If the plan is cut short, cut at a phase boundary ≥ P2, never inside it.
P3 depends on P2 (`BACKLOG.md:80`: membership binding needs a resolved requested-tenant).

---

### §D-S6-abc — a transport-neutral `TenantSource`, so the parked sources stay additive

`varco_core.tenancy` must not import `starlette` (CLAUDE.md's seam rule:
`varco_fastapi.tenancy` imports **only** `varco_core.tenancy`, and the reverse is forbidden). So
the source contract takes a frozen, transport-neutral input record built by the HTTP adapter.

```python
# varco_core/varco_core/tenancy/source.py

class TenantTrust(IntEnum):            # ordering is brief 006 §1's table, verbatim
    LOW     = 10   # a bare client-supplied header, no auth binding  ("❌ LOWEST")
    MEDIUM  = 20   # a trusted-proxy-supplied value (X-Forwarded-Host)  ("⚠️ MEDIUM")
    HIGH    = 30   # subdomain read from the connection's own Host      ("✅ HIGH")
    HIGHEST = 40   # a signed, issuer-bound JWT claim                   ("✅ HIGHEST")

@dataclass(frozen=True)
class TenantRequest:
    """Everything a TenantSource may look at. No HTTP types."""
    headers: Mapping[str, str]              # lower-cased keys, adapter-normalised
    host: str | None = None                 # the connection's own Host, port stripped by the adapter
    path: str = "/"
    auth: AuthContext | None = None         # already VERIFIED — never an unverified parse

@dataclass(frozen=True)
class TenantClaim:
    tenant_id: str
    source: str                             # the producing TenantSource.name
    trust: TenantTrust

class TenantSource(ABC):
    name: ClassVar[str]
    trust: ClassVar[TenantTrust]
    @abstractmethod
    def resolve(self, request: TenantRequest) -> TenantClaim | None: ...
```

| ID | Choice | Consequence |
|---|---|---|
| D-S6-abc | `TenantSource.resolve()` is **sync, pure, total** — it returns `None` for "I have nothing to say" and **must never raise**. All I/O-shaped concerns (membership, delegation, catalog status) live outside it | An mTLS source, a path source or a Redis-cached domain→tenant source are all additive out-of-tree implementations, exactly as the parked rows promise (`BACKLOG.md:96-97`) |

**DESIGN: a frozen `TenantRequest` record instead of passing the framework `Request`**

✅ Keeps `varco_core.tenancy` free of `starlette`, satisfying the seam rule that already governs
   `varco_fastapi.tenancy` (`varco_fastapi/varco_fastapi/tenancy/__init__.py:7-10`) and
   `varco_core.migration`/`varco_fastapi.migrate`.
✅ Makes every source unit-testable with a two-line literal — no `TestClient`, no ASGI scope. The
   adversarial matrix (§Edge cases) is 20 tiny table-driven cases instead of 20 HTTP round trips.
✅ `auth` carries an **already-verified** `AuthContext`, which is what makes
   `JwtClaimTenantSource` trustworthy at all. The type says so.
✅ Frozen + `Mapping` means a source physically cannot mutate the request — asserted in tests.
❌ The adapter must normalise header case and strip the port before constructing it; a second
   adapter (a future gRPC one) would have to repeat that. Accepted — it is six lines, and the
   alternative leaks `starlette` into `varco_core`.
  Rejected — **`resolve(request: Request)` taking the Starlette object**: ❌ breaks the seam rule;
  ❌ makes every source test an HTTP test.
  Rejected — **`async def resolve()`**: ❌ nothing shipped needs I/O, and an `async` signature
  invites an out-of-tree source to do a database lookup on the request-routing hot path, which is
  the exact shape the membership park rejects (`BACKLOG.md:97` — *"Shipping both invites apps to
  pick the slower one by default"*). A cached domain→tenant lookup belongs in an async
  `AbstractTenantCatalog`, which already exists.
  Rejected — **letting a source raise to signal rejection**: ❌ then the chain's rejection policy
  lives in N sources instead of one place, and a buggy out-of-tree source can 500 every request.

### §D-S6-oq2 — BACKLOG open question 2: **agreement-by-default, with a precisely-scoped strict mode**

> *Does S6's cross-check fail-closed when only one source is present? A request carrying a JWT
> claim but no matching subdomain: agreement-by-default or rejection?*

**Answer: agreement-by-default (`CrossCheckMode.LENIENT`) is the default. `STRICT` is available
and is defined so that it cannot break a health check.**

```python
class CrossCheckMode(StrEnum):
    LENIENT = "lenient"   # default: two present values that DISAGREE reject. Absence never rejects.
    STRICT  = "strict"    # additionally: if ANY source produced a value, at least TWO must agree.
```

**The precise `STRICT` rule, and why the wording matters:** *if zero sources produced a value, the
request resolves to `tenant_id=None` and passes through untouched — exactly as today.* `STRICT`
only bites when ≥1 source spoke and <2 agreed. That single clause is what makes `STRICT`
deployable: `GET /health`, `GET /metrics`, the OpenAPI JSON, an OAuth callback on the apex domain
and every unauthenticated public route produce zero claims and are structurally exempt, with no
path allowlist to maintain and get wrong.

| ID | Choice | Consequence |
|---|---|---|
| D-S6-oq2 | `LENIENT` default; `STRICT` = "any ⇒ at least two agreeing"; zero-claims is always pass-through in both modes | The mode is a one-word config change with a bounded, explainable blast radius, and neither mode can reject a request that carries no tenant signal at all |

**DESIGN: a missing source is not a conflicting source**

✅ brief 006 §1's hybrid recommendation is stated as an **inequality**: *"Fail if subdomain ≠ claim
   tenant."* It is not "fail if subdomain is absent". Rejecting on absence is a stricter rule than
   the evidence supports, applied to the default path.
✅ Real deployments legitimately present one source: a mobile/SPA client on a single `api.example.com`
   host with a JWT `org_id`; an internal service-to-service call over a cluster-local DNS name; a
   custom-domain (vanity CNAME) tenant whose host is not under any configured base domain. Making
   the two-source case mandatory by default would reject all three on upgrade — and the backlog
   flags this as *"the decision most likely to break a real deployment quietly"*
   (`BACKLOG.md:107-110`).
✅ The security value brief 006 §1 actually attributes to the hybrid is the *cross-check*, and the
   cross-check still runs in `LENIENT`. The disagreement case — the one an attacker constructs —
   is rejected in **both** modes. `STRICT` adds coverage of a *stripping* attack (suppress the
   subdomain so only the weaker source is left), which is real but requires the weaker source to
   be in the chain at all.
✅ Under the locked blast-radius rule (`BACKLOG.md:54`) a default needing real application work is
   warn-only in 3.2. `STRICT` needs an operator to know every legitimate one-source path in their
   fleet — that is real application work. It is therefore opt-in, reported by 036, and a 4.0
   candidate, not a 3.2 default.
❌ A deployment that configures `jwt` + `subdomain` and expects "both, always" gets "either" unless
   it also writes `VARCO_TENANT_CROSS_CHECK=strict`. Mitigated three ways: the posture seam
   reports the mode (§D-036-seams), the feature doc leads with the two-mode table, and the
   `LENIENT` resolution records **every** contributing claim in `TenantProvenance.claims`, so an
   audit can see afterwards that only one source spoke.
❌ Two modes rather than one. Accepted — one mode would be wrong for half the fleet.
  Rejected — **fail-closed on a missing source by default**: ❌ every ✅ above, inverted; it is a
  silent, fleet-wide 403 on a patch upgrade for anyone on a single API host.
  Rejected — **a per-source `required: bool`**: ❌ it looks more flexible and is strictly worse —
  `required=True` on the subdomain source rejects `/health` too, so it immediately needs a path
  allowlist, which is the maintained-and-got-wrong artifact `STRICT`'s zero-claims clause avoids.
  Rejected — **a third `PARANOID` mode requiring every configured source**: ❌ unreachable in
  practice the moment `LegacyTenantSource` is in the chain, and nobody asked.

### §D-S6-oq3 — BACKLOG open question 3: **explicit base domains, no PSL dependency, and the `Host` is an allowlist match**

> *Deriving the tenant from `tenant.example.com` requires knowing where the registrable domain
> ends; getting it wrong on a multi-level TLD is a tenant-confusion bug. Explicit base-domain
> config avoids a `publicsuffix2` dependency — confirm that is acceptable.*

**Answer: confirmed, and the config is not merely acceptable — it is strictly safer than a PSL.**

```python
SubdomainTenantSource(
    base_domains: Sequence[str],                    # REQUIRED; empty ⇒ ValueError at construction
    *,
    trust_forwarded_host: bool = False,             # never True by default
    forwarded_host_header: str = "X-Forwarded-Host",
    reserved_labels: frozenset[str] = frozenset({"www", "api", "app", "admin", "static", "cdn"}),
)
```

Resolution algorithm, in order:

1. Take `request.host` (the connection's own `Host`). Read `forwarded_host_header` **only** when
   `trust_forwarded_host=True`, and then emit `TenantTrust.MEDIUM` rather than `HIGH` — brief 006
   §1 ranks a trusted-proxy header MEDIUM, and this is one.
2. Normalise: lower-case, strip the port, strip one trailing dot, then `label.encode("idna")` per
   label using the stdlib codec. Any `UnicodeError` → **return `None`**, never raise (§D-S6-abc).
3. **Exact suffix match against the configured base domains**, longest first, so
   `eu.example.com` wins over `example.com` for `acme.eu.example.com`. The host must equal
   `f"{label}.{base}"` for exactly one base.
4. `label` must be a **single DNS label** — if it still contains a dot (`a.b.example.com`), return
   `None`. Never join, never take the leftmost.
5. `label` in `reserved_labels`, or the bare base domain itself, → `None` (not an error).

| ID | Choice | Consequence |
|---|---|---|
| D-S6-oq3 | `base_domains` is **required, explicit, and has no default**. varco never *derives* a registrable domain, so there is no public-suffix algorithm to get wrong. The match against `base_domains` is simultaneously the parse and a host allowlist | `publicsuffix2` is not a dependency; a multi-level TLD is handled by the operator writing `base_domains=("example.co.uk",)`; a host outside every base domain contributes **nothing**, so a spoofed `Host` cannot invent a tenant namespace |

**DESIGN: explicit base domains over a bundled public-suffix list**

✅ CLAUDE.md's standing **zero-new-runtime-dependencies rule for `varco_core`** is decisive on its
   own — it is the *stated* reason RRULE/RFC 5545 stays parked (`BACKLOG.md:167`), and
   `truststore` was rejected outright on the same axis (`BACKLOG.md:173`). A tenant-routing
   parser is a far weaker case than either.
✅ A PSL is a **data file that goes stale**. `publicsuffix2` ships a snapshot; correctness of
   tenant routing would then depend on a transitive dependency's release cadence, and the failure
   mode of a stale entry is *tenant confusion* — the exact bug the question is about. Explicit
   config cannot go stale, because the operator's own DNS is the thing it describes.
✅ It removes the class of bug rather than implementing it carefully: with `base_domains` there is
   no "where does the registrable domain end" computation anywhere in the code path.
✅ Step 4's single-label rule closes the sneaky variant. With `base_domains=("example.com",)`, a
   host of `victim.attacker.example.com` yields **nothing**, not `"victim"` and not
   `"victim.attacker"`. A wildcard TLS cert (`*.example.com`) does not cover that host anyway, but
   the parser does not rely on that being true.
✅ Requiring the argument (no default) means the source cannot be constructed into a
   match-everything state. `SubdomainTenantSource()` is a `ValueError`, not a silent
   "every host has a tenant".
❌ An operator running `acme.example.com` **and** `acme.example.co.uk` must list both. Documented,
   and it is a two-element tuple.
❌ The operator can still write `base_domains=("co.uk",)` and get `example` as the tenant for
   `example.co.uk`. varco cannot detect this and does not try — it is a Pitfalls row, stated as
   *"varco trusts your base-domain list the way it trusts your DSN"*.
  Rejected — **`publicsuffix2` / `tldextract`**: ❌ a runtime dependency in `varco_core` with a
  stale-data failure mode whose symptom is tenant confusion; ❌ `tldextract` additionally does
  network refreshes by default, which is disqualifying in a request path.
  Rejected — **"strip the first label" with no base domain**: ❌ `api.example.com` becomes tenant
  `"api"`; `example.com` becomes tenant `"example"`. Both are silent tenant-confusion bugs.
  Rejected — **a regex per deployment**: ❌ hands the operator the exact anchoring mistake
  (`.` unescaped, missing `$`) this design exists to prevent.

**`Host` spoofing, designed against explicitly.** `Host` is client-controlled and nothing in this
repo validates it today (verified: zero `TrustedHost`/`X-Forwarded` hits). Four defences, and one
honest statement:

1. `trust_forwarded_host=False` by default — `X-Forwarded-Host` is *never* read unless the
   operator asserts their edge strips and re-appends it (brief 006 §1's *"Legitimate only if:
   ingress strips all user-supplied headers AND re-appends with a verified value"*).
2. When it *is* trusted, the claim is emitted at `MEDIUM`, not `HIGH` — the trust ranking encodes
   the extra hop.
3. The `base_domains` match is a host allowlist: a spoofed `Host: acme.evil.com` produces no claim
   at all.
4. A spoofed `Host: victim.example.com` **does** produce `tenant="victim"` — and is then caught by
   the cross-check against the JWT claim (§D-S6-oq2) and by membership binding (§D-S5-claim).
   ⛔ **Statement of fact, required in the docs**: *`SubdomainTenantSource` is not by itself a
   security control. Its security value is that it disagrees with a forged JWT claim, and that a
   forged host disagrees with a real one. Deploy it with `TrustedHostMiddleware`
   (`allowed_hosts=[...]`, Starlette's own) or an edge that rejects unknown `Host` values.* The
   README and feature doc carry this verbatim; a step asserts the docstring contains it.

### §D-S6-chain — the chain is a pure function; rejection lives in exactly one place

```python
@dataclass(frozen=True)
class TenantSourceChain:
    sources: tuple[TenantSource, ...]
    mode: CrossCheckMode = CrossCheckMode.LENIENT
    min_trust: TenantTrust = TenantTrust.LOW

    def resolve(self, request: TenantRequest) -> TenantProvenance: ...   # never raises

@dataclass(frozen=True)
class TenantProvenance:
    tenant_id: str | None
    winner: TenantClaim | None
    claims: tuple[TenantClaim, ...]                  # every source that spoke, chain order
    conflict: tuple[TenantClaim, TenantClaim] | None  # the first disagreeing pair
    mode: CrossCheckMode
    membership: MembershipDecision | None = None      # filled by Phase 3, never by resolve()
    delegation: DelegationRecord | None = None        # filled by Phase 6, never by resolve()

    @property
    def rejected(self) -> bool: ...
    @property
    def rejection_reason(self) -> str | None: ...     # stable, opaque-safe, never echoes a value
```

| ID | Choice | Consequence |
|---|---|---|
| D-S6-chain | `resolve()` **returns a verdict, never raises**. The winner is the highest-`trust` claim; a tie between equal-trust sources is broken by chain order. Rejection is a *field*, and turning a rejection into an HTTP status is the adapter's job | One place decides; the ABC stays total; the same verdict object is what 036 reads and what the audit records |

**DESIGN: a frozen verdict object rather than an exception from `resolve()`**

✅ 036's S4 guard and S11 authz-audit both need the *reason* and the *contributing claims*, not
   just a boolean — an exception would force them to parse a message string.
✅ It makes the adversarial matrix assertable without `pytest.raises` gymnastics: one call, then
   assert on four fields.
✅ Mirrors 037's `RlsPosture` precedent — a frozen dataclass return keeps the cross-plan
   consumption a pure read with no shared mutable state.
❌ The adapter must remember to check `.rejected`. Mitigated: the one adapter varco ships does,
   and it is asserted; and `TenantProvenance` is only ever published into the ambient var by that
   adapter, after the check.
  Rejected — **`resolve()` raises `TenantResolutionConflict`**: ❌ loses the claims list at the
  point 036 needs it; ❌ a source-level bug becomes a 500 instead of a verdict.

### §D-S6-provenance — a dedicated `AmbientVar`, and why this does not violate the `RequestContext` rule

```python
# varco_core/varco_core/tenancy/provenance.py
_provenance: Final[AmbientVar[TenantProvenance]] = AmbientVar("varco_tenant_provenance")

def current_tenant_provenance() -> TenantProvenance | None: ...
@contextmanager
def provenance_context(prov: TenantProvenance) -> Iterator[None]: ...
```

CLAUDE.md is categorical: **`RequestContext` never holds the tenant**; `current_tenant()` is the
single source of truth, and composition is by *ordering*, never containment.

| ID | Choice | Consequence |
|---|---|---|
| D-S6-provenance | `TenantProvenance` goes in **its own `AmbientVar`**, built on `varco_core.context.AmbientVar` (`context/ambient.py:56-127`) — **not** in `RequestContext`, and **not** as a field on `AuthContext` | The rule is honoured to the letter: `current_tenant()` still answers *who*, and the new var answers *how it was decided*. Two vars, one ordering, zero containment |

**DESIGN: a second ambient var over extending `RequestContext` or `AuthContext`**

✅ The forbidden thing is putting the *tenant identity* in `RequestContext`. Provenance is decision
   metadata: it is meaningful only *because* `current_tenant()` already holds the answer, and it
   is `None` on every code path where no chain ran.
✅ `AmbientVar` exists for exactly this ("the generic request-scoped ambient-value primitive
   `RequestContext`/`resolve_precedence()` build on") — using it directly is the documented path,
   not an exception to it.
✅ It doubles as the **coordination marker** §D-S6-wiring needs: `current_tenant_provenance() is
   not None` is the unambiguous, varco-owned signal that a chain already decided, so
   `RequestContextMiddleware` must not decide again. No `request.state` key, no Starlette-internals
   assumption, no new public API for the marker.
✅ Module-scope `ContextVar` construction inside `AmbientVar` is correct per PEP 567 and is
   explicitly blessed by CLAUDE.md's note — it is not an exception to the lazy-`asyncio.Lock` rule.
❌ A third ambient concept for a reader to hold (tenant, request context, provenance). Mitigated by
   one Decision-Tree line and a three-row table in the feature doc.
  Rejected — **a `provenance` field on `RequestContext`**: ❌ `RequestContext` is the locale/
  timezone carrier and `LocalizationMiddleware` runs *inside* the tenant middleware — the value
  would not exist yet when `RequestContext` is built.
  Rejected — **a field on `AuthContext`**: ❌ `AuthContext` is the *token's* snapshot; provenance
  includes a header and a hostname, which the token never saw. It is also frozen and part of the
  api-surface snapshot with many out-of-tree constructors.

### §D-S6-wiring — one middleware decides; `RequestContextMiddleware` defers

This is the resolution of **Corrections 1 and 2**: today two middlewares set the tenant, the outer
one from an unbound header and the inner one from a signed claim, and the inner silently wins.

```python
TenantResolutionMiddleware(
    app, *, catalog, pool,
    header: str = "X-Tenant-Id",              # unchanged, still honoured when chain is None
    chain: TenantSourceChain | None = None,   # NEW
    server_auth: AbstractServerAuth | None = None,   # NEW — needed to have a verified AuthContext
    membership: AbstractTenantMembership | None = None,   # NEW (Phase 3)
    reject_status: int = 403,
)
```

| ID | Choice | Consequence |
|---|---|---|
| D-S6-wiring | `TenantResolutionMiddleware` becomes **the one tenant decision point**. Given `chain=`, it builds a `TenantRequest`, runs `chain.resolve()`, runs membership, publishes `TenantProvenance`, and *then* does its existing catalog-status + `pool.ensure()` work. `RequestContextMiddleware` skips **both** its `server_auth` call and `_maybe_tenant_context()` when `current_tenant_provenance()` is already set and `auth_context_var` is already populated | The header/claim override ambiguity disappears; auth is verified once per request; `chain=None` is byte-identical to today on both middlewares |

**DESIGN: verify the token in the outer middleware and let the inner one reuse it**

✅ `AbstractServerAuth` is a plain callable already invoked at middleware time
   (`request_context.py:141`) — nothing new is invented, and the `AuthContext` it returns is
   **verified**, which is the entire premise of `JwtClaimTenantSource`.
✅ Reuse is keyed on varco's own provenance var, not on a heuristic: provenance set ⇒ the chain
   middleware ran ⇒ it also entered `auth_context()`. If provenance is set but
   `auth_context_var.get()` is `None` (a chain installed with `server_auth=None`),
   `RequestContextMiddleware` authenticates normally. Both branches are asserted.
✅ It preserves the RD-3 ordering guarantee unchanged — `LocalizationMiddleware` still sees
   `current_tenant()` populated (`technical_docs/features/timezone-handling.md:325-334`).
✅ `chain=None` short-circuits before any of it: same header read, same `tenant_context()`, same
   response codes. The existing `varco_fastapi/tests/test_tenant_resolution_middleware.py` and
   `test_tenant_event_path_middleware.py` must pass **unmodified** — that is the byte-identical
   proof, and it is a review gate.
❌ ⚠️ **`extra_middleware=` sits OUTSIDE `ErrorMiddleware`** (`app.py:530-544`, verified). An
   `HTTPException` raised by `server_auth` inside this middleware would therefore **not** be
   formatted by `ErrorMiddleware` and would surface as Starlette's default plain-text 500/401.
   **Mitigation is mandatory, not optional**: the middleware catches `HTTPException` from
   `server_auth` and returns a `JSONResponse` itself — it already does exactly this for a
   non-routable tenant (`tenant_resolution.py:76-79`). Asserted by a test. The feature doc
   additionally recommends `install_middleware_stack` placement *inside* `ErrorMiddleware` as the
   better shape.
❌ An app that passes a *different* `server_auth` to each middleware gets the outer one's verdict.
   Documented as a Pitfalls row; the recommended wiring passes the same instance (or `None` to the
   chain middleware, which restores independent authentication).
  Rejected — **parse the token unverified in the middleware (`JwtParser.parse_unverified`) and
  re-check after `RequestContextMiddleware` verifies it**: ❌ **this is the bug, restated.** It
  would make a routing decision, a catalog lookup and a `pool.ensure()` — resource allocation — on
  an unsigned claim. Recorded here so it is never re-proposed as an optimisation.
  Rejected — **move the chain into `RequestContextMiddleware`**: ❌ inverted ordering — the
  catalog-status check and `pool.ensure()` must run *before* the request reaches a handler, and
  `TenantResolutionMiddleware` is outside; ❌ it would put multitenancy routing in the middleware
  every app gets by default, breaking the "changes nothing by default" property.
  Rejected — **a `create_varco_app(tenant_sources=...)` kwarg**: ❌ Plan 007 deliberately gave
  `TenantResolutionMiddleware` no `create_varco_app` slot; adding one now would create a second
  wiring path for the same object. Apps continue to use `extra_middleware=` /
  `install_middleware_stack`.

### §D-S6-settings — env-driven configuration, and why it is not the RD-9 case

`TenantProvenanceSettings` is a **plain frozen dataclass with a hand-written `from_env()`**,
matching `TenancySettings` (`tenancy/settings.py:72-170`) — *not* the pydantic `VarcoSettings`
shape used in `varco_core.jwt.transform` (Correction 4).

```python
def build_tenant_source_chain(
    settings: TenantProvenanceSettings | None = None,
) -> TenantSourceChain | None:      # None when VARCO_TENANT_SOURCES is unset ⇒ nothing changes
```

⚠️ **This is not the RD-9 case.** RD-9 forbids a `VARCO_TENANCY_MOUNT_ADMIN` env var *forever*
because it would let a bare environment variable expose a privileged HTTP surface. These vars do
the opposite: they *restrict* where a tenant identity may come from, and they expose nothing. The
one thing that stays code-only, per RD-9's actual reasoning, is anything that mounts a surface —
this plan mounts nothing. Stated explicitly so a later reader does not read the two as
inconsistent.

### §D-S5-claim — membership: a signed list, `AuthContext`-shaped, fail-open in 3.2

**Correction 3 must be fixed first**: `AuthContext.metadata` carries only `tenant_id` and `actor`
(`parser.py:383-395`), so a `tenants`/`orgs` list claim has nowhere to land. Phase 3 adds
**one enum member** to the existing per-issuer mechanism:

```python
class CanonicalClaim(StrEnum):     # transform/mapping.py:35-63
    ...
    TENANTS = "tenants"            # NEW → AuthContext.metadata["tenants"], a list
```
plus `_TARGET_FIELD_PREFIX[TENANTS] = "tenants"`, `_DEFAULT_CANONICAL_SOURCE[TENANTS] = "tenants"`
(`transform/config.py:59-81`), and two lines in `_build_auth_ctx` (`parser.py:361-395`). Auth0's
`org_id`, Entra's `tid`, Cognito's `custom:tenant_id` and a Keycloak `organization` list are then
all reachable with `VARCO_JWT_TRANSFORM__<LABEL>__TENANTS_FIELD=...` — the mechanism brief 006 §1
says must exist because *no OIDC standard for an org/tenant claim exists*.

```python
# varco_core/varco_core/tenancy/membership.py
@dataclass(frozen=True)
class MembershipDecision:
    allowed: bool
    reason: str                 # stable machine token, e.g. "not_in_claim", "no_provider"
    provider: str
    tenant_id: str
    subject: str | None

class AbstractTenantMembership(ABC):
    name: ClassVar[str]
    @abstractmethod
    async def check(self, ctx: AuthContext, tenant_id: str) -> MembershipDecision: ...

class NullTenantMembership(AbstractTenantMembership):   # DI default — always allowed
class ClaimTenantMembership(AbstractTenantMembership):  # opt-in via enable_tenant_membership()

class MissingClaimPolicy(StrEnum):
    ALLOW = "allow"    # 3.2 default — allow + one WARNING, reported by 036
    DENY  = "deny"     # 4.0 default
```

`ClaimTenantMembership.check()` allows when **any** of:
- `tenant_id` is in the `tenants` metadata list (the normal multi-org case); or
- the token's own `metadata["tenant_id"]` equals `tenant_id` (a single-tenant token is its own
  membership proof); or
- the membership claim is absent **and** `on_missing_claim=ALLOW`.

Everything else denies. `async def` **even though the shipped implementations do no I/O** — the
parked repository-backed resolver (`BACKLOG.md:97`) is an out-of-tree implementation of this exact
ABC and needs it.

| ID | Choice | Consequence |
|---|---|---|
| D-S5-claim | `NullTenantMembership` is the scanned DI default; `enable_tenant_membership(container)` opts in `ClaimTenantMembership`. `on_missing_claim` defaults to `ALLOW` in 3.2 with one warning, and is on the 4.0 flip list | Same `enable_*` verb shape as `varco_casbin.di.enable_policy_authorizer` and `varco_core.flags.enable_feature_flags`. Zero behaviour change for an app that does nothing; 036 reports both facts |

**DESIGN: a Null Object DI default plus an `enable_*` opt-in, fail-open in 3.2**

✅ Exactly the shape CLAUDE.md already records twice — `NullFeatureFlags` scanned + `enable_feature_flags()`,
   and the `enable_policy_authorizer` rule that an opt-in must **not** be a scanned `@Configuration`
   because scan auto-activates those and would silently shadow an app's own binding.
✅ Fail-open-with-a-warning is the locked blast-radius rule applied honestly: adding a `tenants`
   claim to every token in a fleet is *real application work at the IdP*, so it warns in 3.2 and
   flips in 4.0 (`BACKLOG.md:54`).
✅ The self-membership branch means a large class of existing single-tenant deployments become
   *correctly* enforced the moment they opt in, with no IdP change at all.
✅ Trust stays anchored in the signature — no lookup, no per-request query, per the locked
   membership decision (`BACKLOG.md:57`).
❌ `ALLOW`-on-missing means an opted-in app whose IdP silently stops emitting `tenants` degrades to
   no enforcement. Mitigated: the WARNING is emitted once per process **and** the decision's
   `reason` is `"claim_absent"`, which lands in `TenantProvenance.membership` and therefore in
   036's S11 audit — so it is visible per request, not only at startup.
❌ ⚠️ Adding `CanonicalClaim.TENANTS` **widens `_build_auth_ctx`'s materialisation trigger**: a
   token carrying only a `tenants` claim now produces an `AuthContext` where it previously produced
   `None`. This is the same class of change Plan 002 already made and documented for
   `tenant_id`/`actor` (`parser.py:347-350`). Filed in Risks; asserted by a dedicated test.
  Rejected — **`ClaimTenantMembership` as the scanned default**: ❌ a scanned default that denies
  is a fleet-wide 403 on upgrade; a scanned default that allows is indistinguishable from
  `NullTenantMembership` while being harder to reason about.
  Rejected — **`on_missing_claim=DENY` in 3.2**: ❌ needs every token in a fleet reissued; textbook
  "real application work" under the locked rule.
  Rejected — **reading the membership list from `JsonWebToken.extra_claims`**: ❌ the seam is
  `AbstractServerAuth.__call__ -> AuthContext` (`server_auth.py:89`); the `JsonWebToken` is not
  available to the middleware, and threading it through would change that ABC's return type.
  Rejected — **a `tenants` field on `AuthContext`**: ❌ a frozen, api-surface-tracked dataclass
  with many out-of-tree constructors; `metadata` is the documented home for extra claims
  (`auth/base.py:203`, `:248-249`).

### §D-S6-blast — the blast-radius decision, made here rather than inherited

The locked rule (`BACKLOG.md:54`) splits by cost of the caller-side fix. Adopting a provenance
chain requires an operator to know their IdP's tenant claim name, their base domains, and every
one-source path in their fleet. That is unambiguously **"real application work"**.

| ID | Choice | Consequence |
|---|---|---|
| D-S6-blast | **The mechanism ships in 3.2; the default does not flip.** `chain=None` keeps today's header behaviour, now routed through an internally-built `LegacyTenantSource` so there is one code path. It emits **one `DeprecationWarning` at construction** (never per request), matching the `varco_fastapi.auth.TrustStore` shim shape (`auth/trust_store.py:39-43,133`). An **explicit** `LegacyTenantSource` in a caller-supplied chain warns **not at all** — it is the named, documented escape hatch the locked decision promises (`BACKLOG.md:55`) | No app rejects traffic it accepted before. The loud half is 036's `SecurityPosture`, fed by §D-036-seams |

**4.0 flip list — write it down (Step 31 adds these rows to `BACKLOG.md`):**

| # | 4.0 change | 3.2 signal |
|---|---|---|
| 1 | `TenantResolutionMiddleware(chain=None)` → `TypeError`; the implicit `LegacyTenantSource` fallback is removed | `DeprecationWarning` at construction + posture finding `legacy_source_implicit` |
| 2 | `ClaimTenantMembership.on_missing_claim` default `ALLOW` → `DENY` | one WARNING per process + `reason="claim_absent"` on every decision |
| 3 | `RequestContextMiddleware.enable_tenant_context` defaults `True` → `False`; setting the tenant from a claim without a chain, a catalog check or a membership check is removed | posture finding `unchained_claim_tenant_setter` |
| 4 | `CrossCheckMode.STRICT` becomes the default | posture finding `cross_check_lenient` |

⚠️ **Row 3 is the sharp one and must be in the upgrade note in 3.2, not discovered in 4.0.** It is
the second, *undocumented* tenant setter (Correction 1): today, on by default, a token's
`tenant_id` claim enters `tenant_context()` with **no catalog status check and no `pool.ensure()`**,
so a suspended or deleted tenant's token still activates its tenant context on any app using
`create_varco_app` with a container. That is a genuine, separate finding of this plan. It is **not
fixed in 3.2** (fixing it is exactly a silent-403 flip), it **is** reported by the posture seam,
and it **is** fixed for anyone who adopts the chain, because §D-S6-wiring makes the chain
middleware the single decision point.

### §D-036-seams — the two seams this plan owes Plan 036, with exact signatures

⚠️ **This plan defines and exports them. It builds neither the admin guard nor the preflight.**

**(a) The resolved-tenant seam — what S4's cross-tenant admin guard consumes.**

```python
# varco_core/varco_core/tenancy/provenance.py     (exported from varco_core.tenancy and varco_core)

def current_tenant_provenance() -> TenantProvenance | None:
    """The verdict of the TenantSourceChain for this request, or None when no chain ran."""

class CrossTenantAccessError(ServiceException):
    """Raised when a caller addressed a tenant other than the resolved one."""
    message_key = "varco.error.cross_tenant_denied"
    def __init__(self, requested: str, resolved: str | None) -> None: ...
    def error_params(self) -> dict[str, Any]: ...   # {"requested": ...} ONLY — never `resolved`

def assert_tenant_matches(
    requested: str | None,
    *,
    allow_cross_tenant: bool = False,
) -> str:
    """
    Return the tenant an operation must run against, or raise.

    - requested is None            -> current_tenant(), or CrossTenantAccessError if unset
    - requested == current_tenant() -> requested
    - allow_cross_tenant is True    -> requested (the caller asserts it checked a role)
    - otherwise                     -> raise CrossTenantAccessError
    """
```

036's S4 work is then, per admin route: `tenant = assert_tenant_matches(body.get("tenant_id"),
allow_cross_tenant=ctx.has_role(cross_tenant_role))`. That covers both verified sites —
`webhook/router.py:134` (header for reads) and `:151-152` (body for writes) — plus the other two
`mount_*` surfaces. 036 owns the role name, the HTTP mapping and the wiring.
⚠️ `error_params()` deliberately excludes `resolved` — it is a new exfiltration surface and
CLAUDE.md's `ServiceAuthorizationError` precedent applies (never `vars(exc)`).

**(b) The introspection seam — what S9's preflight reports on.**

```python
# varco_core/varco_core/tenancy/posture.py       (exported from varco_core.tenancy and varco_core)

@dataclass(frozen=True)
class TenantProvenancePosture:
    chain_configured: bool                  # False ⇒ header-only, today's behaviour
    source_names: tuple[str, ...]           # chain order, e.g. ("jwt", "subdomain")
    highest_trust: TenantTrust | None
    legacy_source_active: bool
    legacy_source_implicit: bool            # True ⇒ chain=None fell back to the header
    cross_check_mode: CrossCheckMode
    subdomain_base_domains: tuple[str, ...]
    subdomain_trusts_forwarded_host: bool
    membership_provider: str | None         # None ⇒ no provider bound at all
    membership_on_missing_claim: str | None
    delegation_policy: str | None           # Phase 6; None when S16 is cut
    unchained_claim_tenant_setter: bool     # RequestContextMiddleware still sets tenant alone
    findings: tuple[str, ...]               # stable tokens, see below

def inspect_tenant_provenance(
    chain: TenantSourceChain | None,
    *,
    membership: AbstractTenantMembership | None = None,
    delegation: DelegationPolicy | None = None,
    request_context_sets_tenant: bool = True,
) -> TenantProvenancePosture:
    """Pure: no I/O, no ambient reads, no logging. Safe to call at startup or in a test."""
```

**Stable finding tokens** (036 formats them; this plan guarantees the strings and a test pins
them): `tenant.no_chain`, `tenant.legacy_source_implicit`, `tenant.legacy_source_explicit`,
`tenant.single_source`, `tenant.cross_check_lenient`, `tenant.no_membership_provider`,
`tenant.membership_missing_claim_allows`, `tenant.subdomain_trusts_forwarded_host`,
`tenant.unchained_claim_tenant_setter`, `tenant.delegation_unbound`.

**DESIGN: a pure inspector returning a frozen record, mirroring 037's `inspect_rls_posture`**

✅ Identical precedent one plan over (`plans/037` §D-S12-posture) — a frozen dataclass keeps 036's
   consumption a pure read, with no import from `varco_fastapi` into `varco_core`.
✅ Pure and ambient-free means 036 can call it at startup, before any request exists.
✅ Stable finding tokens mean 036 formats strings without string-matching this plan's prose.
❌ Two new public types in the api-surface snapshot. Accepted; Step 27 regenerates.
  Rejected — **036 introspects the chain object itself**: ❌ it would depend on private attribute
  shapes across a plan boundary and break the moment a source gains a field.

### §D-S6-conformance — no new conformance module, and the reason is that the testkit is never packaged

`TenantSource` and `AbstractTenantMembership` are new ABCs whose *expected* growth path is
out-of-tree implementations — the mTLS source and the repository-backed membership resolver are
both parked with exactly that wording (`BACKLOG.md:96-97`). That is normally the strongest case for
a conformance suite.

| ID | Choice | Consequence |
|---|---|---|
| D-S6-conformance | **No sixth or seventh suite.** `testkit/varco_conformance` is **never packaged** (`COVERAGE.md:3`), so the only audience a suite would serve here — the out-of-tree implementer — structurally cannot reach it. Instead: two **Stated absences** rows in `COVERAGE.md`, and the contract invariants asserted directly against the shipped implementations | The five-suite count in CLAUDE.md (×2), `COVERAGE.md` and the per-package `pythonpath` lines all stay untouched |

✅ The five existing suites all cover ABCs whose implementations do **I/O against an external
   system**, where "does this backend really behave the same" is genuinely hard and where every
   implementation is in-tree. `TenantSource.resolve()` is a pure function over a frozen dataclass
   with three in-tree implementations sharing one test module.
✅ The invariants that *would* be in a suite (never raises; returns `None` not `""`; never mutates
   the `TenantRequest`; `name`/`trust` are class-level and unique) are asserted once, parametrised
   over all shipped sources, in `varco_core/tests/test_tenant_source.py` — a `@pytest.mark.parametrize`
   over the source list, which a new in-tree source joins automatically.
❌ An out-of-tree source could violate an invariant silently. Accepted and documented: the invariants
   are written in the `TenantSource` docstring's *Edge cases* section, which is what an
   implementer actually reads.
  Rejected — **ship `testkit/varco_conformance/tenant_source.py`**: ❌ unusable by the only audience
  that needs it; ❌ costs a "five" → "six" edit in four documents for a suite that duplicates a
  parametrised test.

### §D-S16-shape — act-as consumes an exchanged token; it never issues one

`act` is already parsed into `metadata["actor"]` (verified, `parser.py:386-387`). Phase 6 adds
policy, audit and a source:

```python
# varco_core/varco_core/auth/delegation.py
@dataclass(frozen=True)
class ActorContext:
    subject: str                       # act.sub — WHO is acting
    chain: tuple[str, ...]             # nested act.act.sub…, outermost first (RFC 8693 §4.1)
    @classmethod
    def from_metadata(cls, metadata: Mapping[str, Any]) -> ActorContext | None: ...

class DelegationPolicy(ABC):
    name: ClassVar[str]
    @abstractmethod
    async def allows(self, actor: ActorContext, principal: str | None, tenant_id: str) -> bool: ...

class AllowlistDelegationPolicy(DelegationPolicy):     # deny-by-default, explicit (actor -> tenants)
    def __init__(self, grants: Mapping[str, frozenset[str] | Literal["*"]]) -> None: ...

@dataclass(frozen=True)
class DelegationRecord:
    actor: str
    actor_chain: tuple[str, ...]
    principal: str | None
    tenant_id: str
    allowed: bool
    policy: str
```

`ActAsTenantSource(policy, *, requested_from="X-Act-As-Tenant")` emits a claim at
`TenantTrust.HIGHEST` **only** when the token carries an `act` claim *and* the policy allows the
requested tenant. It is the sanctioned replacement for `LegacyTenantSource`: the request still
names the tenant in a header, but the right to name it is signed and policy-checked.

| ID | Choice | Consequence |
|---|---|---|
| D-S16-shape | Delegation is **deny-by-default and unlogged-is-impossible**: no policy bound ⇒ no claim, ever; and **every** decision (allow *and* deny) emits a `DelegationRecord` — attached to `TenantProvenance.delegation` and logged at INFO with both principal and actor | brief 006 §3's two hard requirements — *"never grant unrestricted delegation"* and *"audit logs must capture both principal and actor"* — are structural, not conventions |

**DESIGN: mandatory audit at the point of use, motivated by CVE-2025-55241**

✅ brief 006 §3/§6: Entra's actor tokens were issued *"with no logs; no audit trail of who asked to
   impersonate whom"*, and that was the escalation vector. *"Impersonation must be logged at
   issuance AND use."* varco does not issue, so **use** is the half varco owns, and it is not
   optional.
✅ `DelegationRecord` on the provenance object means 036's S11 authz-decision audit gets it for
   free, with principal, actor and tenant already assembled — the exact fields that row asks for.
✅ Delegation (`sub` = principal, `act` = service) is preferred over impersonation (`sub` replaced,
   no `act`) per brief 006 §3 — `ActorContext.from_metadata` returns `None` when there is no `act`,
   so a bare impersonation token can never take this path.
✅ `AllowlistDelegationPolicy` supports per-tenant scoping (`{"svc-billing": frozenset({"acme"})}`)
   and an explicit `"*"`, which must be *written* — brief 006 §3's *"scope it"*.
❌ Logging at INFO on every delegated request is volume. Accepted — a delegated request is by
   construction rare, and a silent one is the CVE.
❌ No token-exchange endpoint means an app whose IdP lacks RFC 8693 must build the internal issuer
   itself (brief 006 §3 names this fallback). Documented; `JwtBuilder.claim("act", {...})`
   (`jwt/builder.py:308`) is sufficient and needs no builder change.
  Rejected — **support impersonation (`sub` swapped, no `act`)**: ❌ brief 006 §3 marks it *"⚠️
  Risky; rarely used for tenant scoping"* and it is indistinguishable from the principal in an
  audit log — the CVE's shape.
  Rejected — **an `allow_all` default on `AllowlistDelegationPolicy`**: ❌ unrestricted delegation,
  explicitly forbidden by brief 006 §3.

### §D-S16-cut — the explicit cut line

If Phase 6 slips, **cut it whole**. Nothing in Phases 0–5 imports it; `TenantProvenance.delegation`
stays `None` and `TenantProvenancePosture.delegation_policy` stays `None` (both already default
that way). File S16 back to `BACKLOG.md`'s parked table with this trigger:

> **Un-park when a consumer needs an internal service to operate for an arbitrary tenant** — i.e.
> when `LegacyTenantSource` is being used *specifically* for service-to-service tenant selection
> rather than as an un-migrated legacy path. Until then `LegacyTenantSource` is the documented
> migration path and is load-bearing (`BACKLOG.md:81`), which is precisely why S16 is 🟢: ranked on
> **absence of evidence, not low value**. ⚠️ Cutting S16 makes row 1 of the 4.0 flip list
> (removing the implicit legacy fallback) **conditional** on S16 shipping first — a 4.0 that
> removes the header path without a sanctioned delegation path strands exactly the callers the
> park identifies. Record that dependency in the parked row.

### Alternatives considered (plan-level)

- **Fix only the header bug: make `TenantResolutionMiddleware` cross-check the header against
  `AuthContext.metadata["tenant_id"]` and reject on mismatch.** ❌ It is ~15 lines and closes the
  headline bug, but it hardcodes one pair of sources, ships no seam for 036, cannot express
  subdomain routing, and leaves Correction 1's second setter untouched. It is also **not what the
  locked decisions bought** — three sources, a trust ranking and a named escape hatch. Rejected,
  but recorded as the honest minimum if the whole cycle had to shrink.
- **Put tenant resolution in a FastAPI dependency instead of middleware.** ❌ `pool.ensure()` and
  the catalog-status check must run before the handler and outside the route's own error handling;
  and a dependency cannot wrap the handler in `tenant_context()` without relying on FastAPI's
  `AsyncExitStack` task semantics, which is exactly the kind of framework-internals assumption
  §D-S6-provenance avoids.
- **Make `TenantSource` a Protocol rather than an ABC.** ❌ `name`/`trust` are class-level contract
  data an implementation must *declare*; an ABC enforces that at subclass time. (Contrast
  `AsyncCache`, a `runtime_checkable` Protocol, where the isinstance-for-out-of-tree-caches
  argument applies — it does not here.)
- **Add tenant provenance to the `RequestContext`.** ❌ CLAUDE.md's rule, and the ordering makes it
  impossible anyway (§D-S6-provenance).
- **Emit the delegation audit through `AbstractEventProducer`.** ❌ a `TenantSource` is not a
  service and must not hold a producer; and a bus failure must never fail a request. A structured
  log plus the record on the provenance object is the shape S11 consumes.

---

## Steps

### Phase 0 — S6a: the transport-neutral primitives (🔴 must, M)

1. [ ] `varco_core/tests/test_tenant_source.py` (new, **failing first**) — `TenantTrust` ordering
       is `LOW < MEDIUM < HIGH < HIGHEST` (an `IntEnum`, comparable); `TenantRequest` and
       `TenantClaim` are frozen and reject mutation; a `TenantSource` subclass that omits
       `name`/`trust` fails loudly; `TenantSource.resolve` is abstract.
2. [ ] `varco_core/varco_core/tenancy/source.py` (new) — `TenantTrust`, `TenantRequest`,
       `TenantClaim`, `TenantSource` per §D-S6-abc. Full docstrings with
       `Args`/`Returns`/`Raises`/`Edge cases`/`Thread safety`, a `DESIGN:` block per §D-S6-abc, and
       the four implementer invariants (never raises · `None` not `""` · never mutates the request ·
       `name`/`trust` are `ClassVar`) written into `TenantSource`'s *Edge cases* — §D-S6-conformance
       makes that docstring the contract's only home.
3. [ ] `varco_core/tests/test_tenant_chain.py` (new, **failing first**) — `TenantSourceChain.resolve()`
       over stub sources: zero claims → `tenant_id=None`, `winner=None`, `rejected is False` in
       **both** modes; one claim → that claim wins in `LENIENT`, **rejects** in `STRICT`; two
       agreeing → winner is the higher-trust one, no rejection in either mode; two disagreeing →
       `rejected`, `conflict` names both, in **both** modes; three sources where two agree and one
       disagrees → rejected; equal-trust tie broken by chain order; `min_trust` filters a
       below-floor claim out entirely (it does not become a conflict); `resolve()` **never raises**
       even when a stub source raises internally (the chain catches, logs, and treats it as no
       claim — asserted); determinism (two calls, identical result).
4. [ ] `varco_core/varco_core/tenancy/source.py` — `CrossCheckMode`, `TenantSourceChain`,
       `TenantProvenance` per §D-S6-chain. `rejection_reason` returns a **stable token**
       (`"conflict"`, `"insufficient_sources"`, `"not_a_member"`, `"delegation_denied"`), never a
       formatted string containing a tenant id.
5. [ ] `varco_core/tests/test_tenant_provenance.py` (new, **failing first**) —
       `current_tenant_provenance()` is `None` outside any context; `provenance_context()` sets and
       restores it; nesting restores the outer value; a value set in a parent task is visible in a
       child task (the `BaseHTTPMiddleware` propagation this design relies on) and **not** visible
       in a sibling.
6. [ ] `varco_core/varco_core/tenancy/provenance.py` (new) — the `AmbientVar`,
       `current_tenant_provenance()`, `provenance_context()`, with the §D-S6-provenance `DESIGN:`
       block stating in prose why this does not violate CLAUDE.md's `RequestContext`-never-holds-
       the-tenant rule.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_tenant_source.py varco_core/tests/test_tenant_chain.py varco_core/tests/test_tenant_provenance.py`

### Phase 1 — S6b: the three sources (🔴 must, M) — answers OQ3

7. [ ] `varco_core/tests/test_tenant_sources_builtin.py` (new, **failing first**) — the full
       adversarial matrix, table-driven over `TenantRequest` literals. Minimum cases:
       - `JwtClaimTenantSource`: claim present → `HIGHEST`; `auth=None` → `None`; `metadata` empty
         → `None`; a non-`str` claim value → `None` (never `str()`-coerced); a custom
         `metadata_key`.
       - `SubdomainTenantSource`: `acme.example.com` + base `example.com` → `"acme"`;
         `example.com` → `None`; `www.example.com` → `None` (reserved); **`a.b.example.com` →
         `None`** (single-label rule, never `"a"` and never `"a.b"`); `ACME.Example.COM:8443` →
         `"acme"` (case + port + host normalisation); `acme.example.com.` → `"acme"` (trailing
         dot); **`acme.example.co.uk` with `base_domains=("example.co.uk",)` → `"acme"`** (the
         multi-level-TLD case); the same host with `base_domains=("co.uk",)` → `"example"`
         (documented operator error, asserted so the behaviour is pinned); `acme.eu.example.com`
         with both bases configured → `"acme"` via the **longer** base; a punycode/IDNA host;
         a host with an empty or >63-char label → `None`, **no exception**; `base_domains=()` →
         `ValueError` at construction.
       - **Spoofing**: `X-Forwarded-Host: victim.example.com` with a real `Host: api.example.com`
         → `None` by default; with `trust_forwarded_host=True` → `"victim"` at **`MEDIUM`**, not
         `HIGH`.
       - `LegacyTenantSource`: header present → `LOW`; absent → `None`; a custom header name;
         empty-string header value → `None`.
       - **All sources**: `resolve()` does not mutate the `TenantRequest` (compare a deep copy).
8. [ ] `varco_core/varco_core/tenancy/sources.py` (new) — `JwtClaimTenantSource`,
       `SubdomainTenantSource`, `LegacyTenantSource`. `SubdomainTenantSource` carries the
       §D-S6-oq3 `DESIGN:` block, the five-step algorithm as a numbered docstring, and — verbatim,
       asserted by Step 9 — the sentence *"SubdomainTenantSource is not by itself a security
       control; deploy it behind TrustedHostMiddleware or an edge that rejects unknown Host
       values."* `LegacyTenantSource`'s docstring carries the §Security properties statement
       (Step 29's doc text, in short form) and does **not** warn on construction (§D-S6-blast).
9. [ ] `varco_core/tests/test_tenant_sources_builtin.py` (extend) — parametrised invariant sweep
       over all three shipped sources (§D-S6-conformance): unique `name`, declared `trust`, never
       raises on an empty `TenantRequest`, never returns `""`. Plus the mechanical docstring
       assertions for the two required sentences.
10. [ ] `varco_core/tests/test_tenant_chain_integration.py` (new) — end-to-end over real sources,
        no HTTP: **claim says A and host says B → rejected, `conflict` names both**; **header
        claims A with a token for B → rejected** when the legacy source is in the chain, and
        **`tenant_id="B"` with the header ignored** when it is not (both asserted — the second is
        the "why the chain, not just a cross-check" proof); claim A + no subdomain → `"A"` under
        `LENIENT`, **rejected** under `STRICT`; no sources at all → `None` in both modes.
11. [ ] `varco_core/varco_core/tenancy/__init__.py` + `varco_core/varco_core/__init__.py` — export
        the new names. ⚠️ `varco_core/__init__.py` is **PEP 562 lazy**: every name needs an
        `_LAZY` entry *and* an `__all__` entry (`__init__.py:461-712`; `_LAZY` is contractually
        equal to `__all__`, asserted by an existing test). Add **no** top-level import — run
        `uv run python scripts/import_budget.py --check --warn-only` and record the delta.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/ -k "tenant_source or tenant_chain or tenant_provenance"`

### Phase 2 — S6c: the FastAPI wiring (🔴 must, M)

12. [ ] `varco_core/tests/test_tenant_provenance_settings.py` (new, **failing first**) —
        `TenantProvenanceSettings()` defaults produce `build_tenant_source_chain() is None`;
        `VARCO_TENANT_SOURCES="jwt,subdomain"` without `VARCO_TENANT_BASE_DOMAINS` →
        `ValueError` naming the missing var; a full parse round-trip for every var in the
        §Env vars table; an unknown source name → `ValueError` listing the legal set;
        `VARCO_TENANT_CROSS_CHECK=strict` parses.
13. [ ] `varco_core/varco_core/tenancy/settings.py` — `TenantProvenanceSettings`
        (`@dataclass(frozen=True)` + hand-written `from_env()`, matching `TenancySettings`'s shape
        at `:72-170`, **not** pydantic — Correction 4) and `build_tenant_source_chain()`. Add the
        §D-S6-settings note about why this is not the RD-9 case. **No field on `TenancySettings`
        moves or is added** — asserted by the existing byte-identical-defaults test.
14. [ ] `varco_fastapi/tests/test_tenant_resolution_middleware.py` — **run it unmodified and
        confirm green** before touching the middleware, then extend it. This file plus
        `test_tenant_event_path_middleware.py` are the byte-identical proof for `chain=None` and
        must never be edited to accommodate the new code path. Record the result in the commit
        message.
15. [ ] `varco_fastapi/tests/test_tenant_chain_middleware.py` (new, **failing first**) — through a
        real `TestClient`: `chain=None` → today's behaviour **and exactly one
        `DeprecationWarning` at construction, none per request** (`pytest.warns` + a
        second/third request asserting no further warning); an explicit chain containing
        `LegacyTenantSource` → **no warning at all**; a conflicting request → **403** with an
        opaque body carrying no tenant id; a 404/503/403/410 catalog-status response is unchanged;
        `pool.ensure()` is called **at most once** and only for a routable tenant;
        `current_tenant()` inside the handler equals the chain's winner; `current_tenant_provenance()`
        inside the handler is the same object the middleware published.
16. [ ] `varco_fastapi/varco_fastapi/middleware/tenant_resolution.py` — the `chain=`/`server_auth=`/
        `reject_status=` keywords per §D-S6-wiring. Order inside `dispatch`: build `TenantRequest`
        (lower-case headers, strip the port from `Host`) → run `server_auth` if given, catching
        `HTTPException` and returning a `JSONResponse` itself (the ❌ in §D-S6-wiring — asserted by
        Step 17) → `chain.resolve()` → on `rejected`, return `reject_status` with an opaque body →
        `provenance_context(prov)` → the **existing, unchanged** catalog-status +
        `pool.ensure()` + `tenant_context()` block. `chain=None` builds
        `TenantSourceChain((LegacyTenantSource(self._header),))` once in `__init__` and emits the
        single `DeprecationWarning` there.
17. [ ] `varco_fastapi/tests/test_tenant_chain_middleware.py` (extend) — a `server_auth` that
        raises `HTTPException(401)` produces a **JSON** 401 from this middleware even though it
        sits outside `ErrorMiddleware` (`app.py:530-544`); a `server_auth` returning an anonymous
        context yields no JWT claim and falls through to the remaining sources.
18. [ ] `varco_fastapi/tests/test_request_context_deferral.py` (new, **failing first**) — with no
        chain installed, `RequestContextMiddleware` behaves **byte-identically** (auth runs, the
        claim tenant is entered); with a chain installed upstream, it runs `server_auth`
        **exactly once total** (a counting fake asserts one call across the whole request) and does
        **not** re-enter `tenant_context()` (a chain winner of `"A"` and a token claim of `"B"`
        leaves `current_tenant() == "A"` in the handler — the Correction-2 regression, asserted
        directly); with provenance set but `auth_context_var` unset, it authenticates normally.
19. [ ] `varco_fastapi/varco_fastapi/middleware/request_context.py` — the deferral per
        §D-S6-wiring, guarded by `current_tenant_provenance() is not None`. Update the class
        docstring's numbered "Order of operations" (`:65-78`) to describe both branches. **No new
        constructor keyword.**
20. [ ] `varco_fastapi/varco_fastapi/middleware/__init__.py` — no new export is required
        (the middleware class is already exported); confirm and note it.

⛔ **CHECKPOINT** — S6 is functionally complete. `uv run pytest varco_fastapi/tests/ varco_core/tests/ -q`

### Phase 3 — S5: tenant↔subject membership binding (🔴 must, M)

21. [ ] `varco_core/tests/test_jwt_tenants_claim.py` (new, **failing first**) — a token with
        `{"tenants": ["a","b"]}` → `auth_ctx.metadata["tenants"] == ["a","b"]`; a foreign name
        via `VARCO_JWT_TRANSFORM_TENANTS_FIELD=organizations` → same result; the **per-issuer**
        form `VARCO_JWT_TRANSFORM__ACME__TENANTS_FIELD` selected by `iss` → same result (this is
        the "reuse, do not rebuild" proof); a scalar `"tenants": "a"` normalises to `["a"]`;
        ⚠️ a token carrying **only** `tenants` now materialises an `AuthContext` where it
        previously returned `None` (the widened trigger, asserted deliberately); a token with no
        `tenants` claim is **byte-identical** to before (`metadata` has exactly the keys it had).
22. [ ] `varco_core/varco_core/jwt/transform/mapping.py` — `CanonicalClaim.TENANTS` (+ its
        `Members:` docstring line at `:44-54`); `varco_core/varco_core/jwt/transform/config.py` —
        `_TARGET_FIELD_PREFIX`/`_DEFAULT_CANONICAL_SOURCE` entries (`:59-81`) and the seven
        `tenants_*` settings fields (`:106-137`); `varco_core/varco_core/jwt/parser.py` —
        `_build_auth_ctx` reads `canonical.get("tenants")` into `metadata` and joins the
        materialisation trigger (`:361-395`), with the `Edge cases:` note extended in the same
        style as the existing Plan-002 widening note (`:347-350`). ⚠️ `TENANTS` must **not** be
        added to `_SCALAR_TARGETS` (`transform/mapping.py:68`) — it is a list target.
23. [ ] `varco_core/tests/test_tenant_membership.py` (new, **failing first**) —
        `NullTenantMembership` always allows with `reason="no_provider"`; `ClaimTenantMembership`:
        `tenants=["a","b"]` + requested `"b"` → allowed; requested `"c"` → denied,
        `reason="not_in_claim"`; **no `tenants` claim but `metadata["tenant_id"] == requested`** →
        allowed, `reason="self_tenant"` (single-tenant tokens); no claim at all with
        `on_missing_claim=ALLOW` → allowed, `reason="claim_absent"`, **and exactly one WARNING per
        process** (asserted with `caplog` across three calls); the same with `DENY` → denied; an
        anonymous `AuthContext` → denied under `DENY`, allowed under `ALLOW`; a non-list `tenants`
        value → denied, never a `TypeError`; `check()` **never raises** for any input.
24. [ ] `varco_core/varco_core/tenancy/membership.py` (new) — `MembershipDecision`,
        `AbstractTenantMembership`, `NullTenantMembership`, `ClaimTenantMembership`,
        `MissingClaimPolicy`, `TenantMembershipError` (a `ServiceException` subclass with `code`
        and `message_key`, per `varco_core.exception`'s taxonomy — and an `error_params()` that
        excludes the *resolved* tenant, matching §D-036-seams' exfiltration rule). `DESIGN:` block
        per §D-S5-claim.
25. [ ] `varco_core/varco_core/tenancy/di.py` (new, or the existing tenancy DI module if one is
        found) — `NullTenantMembership` as the scanned `@Singleton` default and
        `enable_tenant_membership(container, settings=None)` as the opt-in, following
        `varco_core.flags`' `enable_feature_flags` shape exactly. ⛔ **Never a scanned
        `@Configuration`** — `container.scan("varco_core", recursive=True)` auto-activates those
        (CLAUDE.md), which would silently bind a membership provider in every app that scans
        `varco_core`. Assert this with a test that scans `varco_core` and checks the bound
        provider is `NullTenantMembership`.
26. [ ] `varco_fastapi/tests/test_tenant_chain_middleware.py` (extend) — with `membership=` given:
        a chain winner the subject is **not** a member of → **403**, opaque body, and
        `provenance.membership.allowed is False`; a member → 200 and the decision is attached;
        `membership=None` → no check runs and `provenance.membership is None` (the
        byte-identical branch). Membership runs **after** the chain and **before** the catalog
        lookup and `pool.ensure()` — asserted with a recording catalog, because provisioning a
        pool entry for a tenant the caller may not use is itself a (small) resource-exhaustion
        surface.

⛔ **CHECKPOINT** — S5 + S6 complete. `make test`

### Phase 4 — the Plan-036 seams (🔴 must, S)

27. [ ] `varco_core/tests/test_tenant_posture.py` (new, **failing first**) — `assert_tenant_matches`:
        `requested=None` inside `tenant_context("a")` → `"a"`; `requested="a"` inside the same →
        `"a"`; `requested="b"` → `CrossTenantAccessError` whose `error_params()` contains
        `requested` and **not** the resolved tenant (the exfiltration assertion);
        `allow_cross_tenant=True` → `"b"`; `requested=None` with no tenant context →
        `CrossTenantAccessError`. `inspect_tenant_provenance`: `chain=None` → `chain_configured
        is False` and finding `tenant.no_chain`; a legacy-only chain → `legacy_source_active`;
        `membership=None` → `tenant.no_membership_provider`; every finding token in the
        §D-036-seams list is produced by at least one configuration, and **the exact string set is
        pinned by the test** so 036 can rely on it; the function performs **no I/O and reads no
        ambient var** (asserted by calling it with no context active).
28. [ ] `varco_core/varco_core/tenancy/posture.py` (new) + `CrossTenantAccessError` and
        `assert_tenant_matches` in `varco_core/varco_core/tenancy/provenance.py`, per §D-036-seams.
        Each carries a docstring line naming **Plan 036 / S4** and **Plan 036 / S9** as the
        intended consumer, and stating that this plan deliberately builds neither the guard nor
        the preflight.
29. [ ] `varco_core/varco_core/tenancy/__init__.py` + `varco_core/varco_core/__init__.py` —
        export the Phase 3/4 names (`_LAZY` + `__all__`, per Step 11's rule).
30. [ ] `uv run python scripts/api_surface.py` — regenerate **both** snapshot files and commit them
        **in this commit** (hard CI gate on `make lint`'s no-`PKG` path). Expected new `varco_core`
        rows: `TenantTrust`, `TenantRequest`, `TenantClaim`, `TenantSource`, `TenantSourceChain`,
        `CrossCheckMode`, `TenantProvenance`, `JwtClaimTenantSource`, `SubdomainTenantSource`,
        `LegacyTenantSource`, `TenantProvenanceSettings`, `build_tenant_source_chain`,
        `current_tenant_provenance`, `provenance_context`, `assert_tenant_matches`,
        `CrossTenantAccessError`, `MembershipDecision`, `AbstractTenantMembership`,
        `NullTenantMembership`, `ClaimTenantMembership`, `MissingClaimPolicy`,
        `TenantMembershipError`, `enable_tenant_membership`, `TenantProvenancePosture`,
        `inspect_tenant_provenance`. Additions are notes and never fail `--check`; run `--check`
        anyway before committing. ⚠️ `CanonicalClaim` gains a member — that is **not** a signature
        change and is invisible to the gate, so a test pins the member set explicitly.

⛔ **CHECKPOINT** — `make lint && make type-check && make test`

### Phase 5 — docs, CHANGELOG, backlog (🟡 should, S — same commit as the code)

31. [ ] `technical_docs/features/tenant-provenance.md` (**new — the primary home**). Contents:
        the trust-ranking table (brief 006 §1, cited); the chain diagram; the two cross-check modes
        with §D-S6-oq2's zero-claims clause stated plainly; the subdomain algorithm and §D-S6-oq3's
        base-domain rule; the membership model and its 3.2 fail-open; the two 036 seams named as
        seams; the four-row **4.0 flip list**; and — mandatory — a section
        **"`LegacyTenantSource`: what it does and does not give you"** stating in writing: *it
        binds nothing to the authenticated caller; any client that can reach the service can claim
        any active tenant; it is `TenantTrust.LOW` and is the ❌ LOWEST row of brief 006 §1's
        table; it is safe only behind an ingress that strips client-supplied `X-Tenant-Id` and
        re-appends a verified value over an authenticated channel; it exists so a deployment can
        upgrade without an outage, and it is removed in 4.0.* Plus a **Pitfalls** table with at
        least these rows: two tenant setters pre-chain (Correction 1); `extra_middleware=` sits
        outside `ErrorMiddleware`; a `Host` is client-controlled; `base_domains=("co.uk",)` is
        operator error varco cannot detect; `a.b.example.com` yields nothing by design;
        `STRICT` plus a single-API-host client is a fleet-wide 403; membership fails **open**
        in 3.2; a `tenants` claim absent from a re-issued token silently degrades enforcement;
        two different `server_auth` instances across the two middlewares; a delegated request with
        no policy bound is denied, not allowed.
32. [ ] `technical_docs/features/multitenancy.md` — a short **"Where the tenant comes from"**
        subsection linking to the above with no restatement (CLAUDE.md's *one home per fact*), plus
        one Pitfalls row pointing at Correction 1.
        `technical_docs/features/jwt-claim-transformer.md` — one row for `CanonicalClaim.TENANTS`
        and its env vars.
        `technical_docs/common-pitfalls.md` — one cross-cutting row: *a tenant read from a request
        without a signed binding is not a tenant*.
33. [ ] `README.md` — a "Tenant identity provenance" section under multi-tenancy: the wiring
        snippet (`extra_middleware=` and the recommended `install_middleware_stack` placement
        inside `ErrorMiddleware`), the `enable_tenant_membership` snippet, and the **`VARCO_*`
        env-var reference table** (§Env vars below, verbatim). Repeat the
        `SubdomainTenantSource`-is-not-a-security-control sentence.
34. [ ] `CLAUDE.md` — pointers only, no design prose: (a) a **Rule** line — *tenant provenance is
        `varco_core.tenancy.source`; `current_tenant()` stays the single source of truth and this
        plan changes only what feeds it; `TenantProvenance` is its own `AmbientVar` and must never
        move into `RequestContext`*; (b) a Decision-Tree branch under multitenancy (*where may a
        tenant come from? → `varco_core.tenancy.source`; is this subject allowed that tenant? →
        `varco_core.tenancy.membership`; may this service act for that tenant? →
        `varco_core.auth.delegation`; is my deployment still header-only? →
        `inspect_tenant_provenance()`*); (c) one line in the DI verb taxonomy for
        `enable_tenant_membership` under the existing `enable_*` row.
35. [ ] `testkit/varco_conformance/COVERAGE.md` — two **Stated absences** bullets for
        `TenantSource` and `AbstractTenantMembership`, carrying §D-S6-conformance's reasoning
        (never-packaged testkit ⇒ the out-of-tree implementer cannot reach a suite). ⚠️ Do **not**
        change the "five suites" count anywhere.
36. [ ] `CHANGELOG.md` `## [Unreleased]` — `### Added` (the chain, the three sources, membership,
        `CanonicalClaim.TENANTS`, the two 036 seams — "Plan 033 / S6, S5"); `### Deprecated`
        (`TenantResolutionMiddleware(chain=None)`, with the 4.0 removal date and the escape hatch
        named); `### Security` (the two-setter finding, Correction 1, stated as a finding with the
        3.2 mitigation and the 4.0 fix). `BACKLOG.md` — replace open questions 2 and 3 (`:107-114`)
        with their answers pointing at §D-S6-oq2 / §D-S6-oq3, in the answered-not-deleted style
        Plan 024 used; add the four 4.0 flip-list rows to the answered-decisions section; mark S6
        and S5 `✅ planned`.

⛔ **CHECKPOINT** — `make lint`, `make type-check`, `make test`.
**S6 + S5 are shippable here. Everything below is separable and droppable (§D-S16-cut).**

### Phase 6 — S16: act-as / RFC 8693 (🟢 nice, M — **DROPPABLE**)

37. [ ] `varco_core/tests/test_delegation.py` (new, **failing first**) — `ActorContext.from_metadata`:
        `{"actor": {"sub": "svc-a"}}` → `subject="svc-a"`, empty chain; a nested
        `{"sub":"svc-a","act":{"sub":"svc-b"}}` → `chain == ("svc-a","svc-b")` outermost-first
        (RFC 8693 §4.1); `metadata` with no `actor` → `None`; a malformed `actor` (a string, a
        list, a dict with no `sub`) → `None`, **never an exception**.
        `AllowlistDelegationPolicy`: an unlisted actor → denied; a listed actor for an unlisted
        tenant → denied; `"*"` → allowed; an empty grants map → denies everything.
38. [ ] `varco_core/varco_core/auth/delegation.py` (new) — `ActorContext`, `DelegationPolicy`,
        `AllowlistDelegationPolicy`, `DelegationRecord`, per §D-S16-shape. Docstrings cite brief
        006 §3 and name CVE-2025-55241 as the reason the audit is mandatory.
39. [ ] `varco_core/tests/test_act_as_source.py` (new, **failing first**) — `ActAsTenantSource`:
        no `act` claim → `None` even when the header is present (a bare impersonation token can
        never take this path); `act` present + policy allows → `HIGHEST`; `act` present + policy
        denies → `None` **and** a `DelegationRecord` with `allowed=False`; no policy bound →
        `None`, `delegation_unbound`; **every** branch emits exactly one INFO log record
        containing both the principal and the actor (asserted with `caplog` — this is the
        CVE-2025-55241 requirement and is a review gate).
        ⚠️ `DelegationPolicy.allows` is `async` while `TenantSource.resolve` is sync (§D-S6-abc).
        Resolve at implementation time and record the outcome: lean toward
        `ActAsTenantSource` taking a **pre-resolved decision** computed by the middleware (an
        `await`ed step before `chain.resolve()`, passed in on the `TenantRequest` or as a
        constructor-bound per-request closure), rather than making `resolve()` async for one
        source — see Open questions.
40. [ ] `varco_core/varco_core/tenancy/sources.py` — `ActAsTenantSource` and the
        `TenantProvenance.delegation` wiring; `varco_fastapi/.../tenant_resolution.py` — the
        `delegation=` keyword and the pre-resolution step.
41. [ ] `varco_fastapi/tests/test_act_as_middleware.py` (new) — through a `TestClient`: a token
        with `act` + an allowlisted actor + `X-Act-As-Tenant: acme` → 200 with
        `current_tenant() == "acme"`; the same token for a non-allowlisted tenant → **403**;
        the same request with **no** delegation policy configured → 403; the delegated request's
        `provenance.delegation` carries principal, actor and tenant.
42. [ ] `technical_docs/features/tenant-provenance.md` + README + CHANGELOG + `scripts/api_surface.py`
        — the act-as section (with the "varco consumes an exchanged token, it never issues one"
        statement and the `JwtBuilder.claim("act", …)` fallback recipe), three Pitfalls rows
        (impersonation-without-`act` is unsupported by design; an unlogged delegation is a bug not
        a feature; delegation is deny-by-default), and the snapshot regenerated. Mark S16
        `✅ planned` in `BACKLOG.md`.

⛔ **CHECKPOINT** — if this phase is cut, apply §D-S16-cut: delete nothing from Phases 0–5, file
S16 into `BACKLOG.md`'s parked table with the stated trigger, **and** annotate 4.0 flip-list row 1
as conditional on S16.

---

## Env vars (the README table, verbatim)

All read by `TenantProvenanceSettings.from_env()`. **Every one is unset by default and an unset
`VARCO_TENANT_SOURCES` means `build_tenant_source_chain()` returns `None` — nothing changes.**

| Var | Default | Meaning |
|---|---|---|
| `VARCO_TENANT_SOURCES` | *(unset)* | Ordered, comma-separated: `jwt`, `subdomain`, `legacy`, `act_as`. Unset ⇒ no chain |
| `VARCO_TENANT_CROSS_CHECK` | `lenient` | `lenient` \| `strict` (§D-S6-oq2) |
| `VARCO_TENANT_MIN_TRUST` | `low` | `low` \| `medium` \| `high` \| `highest` — claims below the floor are discarded |
| `VARCO_TENANT_CLAIM_METADATA_KEY` | `tenant_id` | Key in `AuthContext.metadata` the JWT source reads |
| `VARCO_TENANT_BASE_DOMAINS` | *(unset)* | Comma-separated. **Required** when `subdomain` is in the chain (§D-S6-oq3) |
| `VARCO_TENANT_TRUST_FORWARDED_HOST` | `false` | Read `X-Forwarded-Host`; the claim drops to `MEDIUM` trust |
| `VARCO_TENANT_FORWARDED_HOST_HEADER` | `X-Forwarded-Host` | |
| `VARCO_TENANT_RESERVED_LABELS` | `www,api,app,admin,static,cdn` | Subdomain labels that are never a tenant |
| `VARCO_TENANT_LEGACY_HEADER` | `X-Tenant-Id` | Header the legacy source reads |
| `VARCO_TENANT_MEMBERSHIP` | *(unset)* | `null` \| `claim`. Unset ⇒ `NullTenantMembership` |
| `VARCO_TENANT_MEMBERSHIP_CLAIM` | `tenants` | `AuthContext.metadata` key holding the membership list |
| `VARCO_TENANT_MEMBERSHIP_ON_MISSING` | `allow` | `allow` \| `deny`. **Flips to `deny` in 4.0** |
| `VARCO_TENANT_ACT_AS_HEADER` | `X-Act-As-Tenant` | Phase 6 only |
| `VARCO_JWT_TRANSFORM_TENANTS_FIELD` | *(unset)* | Foreign name for the membership-list claim; the per-issuer form `VARCO_JWT_TRANSFORM__<LABEL>__TENANTS_FIELD` also exists (existing mechanism) |

---

## Migration and upgrade note (existing deployments — read before shipping)

**For an app that upgrades to 3.2 and changes no code and no environment, exactly two things
change, and neither rejects traffic:**

1. One `DeprecationWarning` at `TenantResolutionMiddleware` construction (only if the app installs
   it at all — it has no `create_varco_app` slot and is opt-in today).
2. A token carrying **only** a `tenants` claim now materialises an `AuthContext` where it
   previously produced `None` (the §D-S5-claim widening). No token in the wild has that claim
   unless the app configured it, and the widened trigger is the same class of change Plan 002
   already made for `tenant_id`/`actor`.

Response codes, resolved tenants, `pool.ensure()` calls and `current_tenant()` values are all
unchanged.

⚠️ **Read this even if you adopt nothing.** Today, if you use `create_varco_app` with a container,
`RequestContextMiddleware` enters `tenant_context()` from the token's `tenant_id` claim with **no
catalog status check and no membership check** (`request_context.py:145-148`). A token issued for a
*suspended or deleted* tenant still activates that tenant's context. This is not new in 3.2 and is
not changed by 3.2; adopting a chain fixes it (§D-S6-wiring), and 4.0 flip-list row 3 removes the
unchained path. If you cannot adopt a chain yet, set `enable_tenant_context=False` and set the
tenant yourself, or accept the exposure knowingly.

**Adopting a provenance chain, in order:**

1. **Find your claim name.** Auth0 `org_id`, Entra `tid`, Okta `org_id`, Keycloak `org_id`, Cognito
   a custom attribute (brief 006 §1). Set `VARCO_JWT_TRANSFORM__<LABEL>__TENANT_FIELD`, per issuer.
   Verify with a decoded token before enabling anything.
2. **Start with `VARCO_TENANT_SOURCES=jwt,legacy` and `CROSS_CHECK=lenient`.** Both sources are
   present, so a mismatched header now **rejects** — which is the headline fix — while a request
   with only one of them still works.
3. **Watch for 403s.** Any legitimate caller sending a header that disagrees with its token is a
   real finding, not a regression.
4. **Drop `legacy`** → `VARCO_TENANT_SOURCES=jwt` (add `subdomain` if you route by host). This is
   the step that closes the bug; everything before it is preparation.
5. **Bind membership**: issue a `tenants` claim, then `enable_tenant_membership(container)` with
   `VARCO_TENANT_MEMBERSHIP=claim`. Leave `ON_MISSING=allow` until every token carries the claim,
   then set `deny`.
6. **Optionally** `VARCO_TENANT_CROSS_CHECK=strict` — only after enumerating every legitimate
   one-source path in your fleet (§D-S6-oq2).

**Rollback** is one environment variable: unset `VARCO_TENANT_SOURCES`. No data, no schema, no DDL,
nothing persisted. This is the whole reason the chain is configuration rather than code.

---

## Edge cases

- **Zero sources produce a value** → `tenant_id=None`, request passes through untouched, in both
  cross-check modes. Public routes, `/health`, `/metrics` and the OpenAPI JSON keep working with no
  path allowlist.
- **Two sources agree** → no rejection; the winner is the higher-trust claim, so `TenantProvenance.
  winner.trust` reports the *best* evidence, not the first.
- **Two sources disagree** → rejected in both modes. `conflict` names both claims; the HTTP body is
  opaque and names neither.
- **Equal-trust tie** (two `HIGH` sources agreeing) → chain order decides the `winner`; the value is
  identical either way, so this only affects the reported `source`.
- **A source raises internally** → the chain catches it, logs at ERROR, and treats it as "no claim".
  A buggy out-of-tree source degrades the chain; it never 500s the request.
- **Host has no port / a port / uppercase / a trailing dot / punycode** → all normalise. An empty or
  over-long DNS label makes `str.encode("idna")` raise `UnicodeError`, which is caught → `None`.
- **`a.b.example.com` with base `example.com`** → no claim. Never `"a"`, never `"a.b"`.
- **`base_domains=("co.uk",)`** → `example.co.uk` yields tenant `"example"`. varco cannot detect
  this and does not try; it is a documented Pitfall.
- **`X-Forwarded-Host` present, `trust_forwarded_host=False`** → ignored entirely.
- **JWT claim is not a `str`** (a number, a list) → no claim. Never coerced with `str()`, because
  `str(["a"])` would produce a plausible-looking tenant id.
- **The membership claim is absent** → `ALLOW` in 3.2 with one process-level WARNING and a
  per-decision `reason="claim_absent"` that reaches 036's audit.
- **The subject is anonymous** and a chain resolved a tenant from a header/subdomain → membership
  denies under `DENY`, allows under `ALLOW`. This is the migration cliff and is called out in the
  upgrade note.
- **A conflicting request never reaches `pool.ensure()`** and never performs a catalog lookup —
  rejection is before both, asserted in Step 26.
- **`server_auth` raises `HTTPException`** inside the chain middleware → a `JSONResponse` produced
  by the middleware itself, because it sits outside `ErrorMiddleware` (`app.py:530-544`).
- **A chain is installed but `RequestContextMiddleware` is not** (a container-free app) →
  provenance and `tenant_context()` are still set by the chain middleware; nothing depends on the
  inner middleware existing.
- **Nested `tenant_context()`** — unchanged semantics (`service/tenant.py:178-182`); the chain
  middleware enters exactly one.
- **An `act` claim present but no delegation policy bound** → denied (no claim), logged. Fail-closed,
  and safe because the whole path is new surface.

## Verification

```bash
uv sync --all-packages --all-extras

# unit — core
uv run pytest varco_core/tests/test_tenant_source.py varco_core/tests/test_tenant_chain.py \
              varco_core/tests/test_tenant_provenance.py \
              varco_core/tests/test_tenant_sources_builtin.py \
              varco_core/tests/test_tenant_chain_integration.py \
              varco_core/tests/test_tenant_provenance_settings.py \
              varco_core/tests/test_jwt_tenants_claim.py \
              varco_core/tests/test_tenant_membership.py \
              varco_core/tests/test_tenant_posture.py -q
uv run pytest varco_core/tests/test_delegation.py varco_core/tests/test_act_as_source.py -q  # P6 only

# unit — the HTTP wiring, including the two byte-identical files run UNMODIFIED
uv run pytest varco_fastapi/tests/test_tenant_resolution_middleware.py \
              varco_fastapi/tests/test_tenant_event_path_middleware.py \
              varco_fastapi/tests/test_tenant_chain_middleware.py \
              varco_fastapi/tests/test_request_context_deferral.py -q

uv run python scripts/api_surface.py --check          # MUST be clean before committing
uv run python scripts/import_budget.py --check --warn-only
make lint && make type-check && make test
```

**DoD:**
1. `varco_fastapi/tests/test_tenant_resolution_middleware.py` and `test_tenant_event_path_middleware.py`
   pass **with zero edits**. An edit to either is a review-blocking defect, not a fix.
2. Step 10 and Step 18 both assert an adversarial case that fails on today's code — a green run
   against an unmodified tree means the test is wrong.
3. `scripts/api_surface.py --check` green with the regenerated snapshot committed.
4. `inspect_tenant_provenance`'s finding-token set is pinned by Step 27, so Plan 036 can be written
   against it without reading this plan's prose.
5. `rg -n "parse_unverified" varco_core/varco_core/tenancy varco_fastapi/varco_fastapi/middleware`
   returns **nothing** — the rejected alternative in §D-S6-wiring must not have crept in.

## Parked

| Item | Why | Un-park trigger |
|---|---|---|
| mTLS / `X-Forwarded-Client-Cert` `TenantSource` | `BACKLOG.md:96` — Envoy-specific, its own trust problem | varco commits to service-mesh guidance, or a consumer asks. §D-S6-abc keeps it a pure out-of-tree `TenantSource` |
| Path-based `TenantSource` (`/api/t/{id}/…`) | brief 006 §1 ranks it MEDIUM with *"no framework default"*; the locked source list is three | A consumer cannot use subdomains and cannot change their claim shape. Additive |
| Repository-backed `AbstractTenantMembership` | `BACKLOG.md:97` — the signed-claim default covers the real case with no per-request query | Memberships that genuinely cannot fit in a token. Out-of-tree, so waiting costs nothing |
| A `TenantSource` conformance suite | §D-S6-conformance — the testkit is never packaged, so the out-of-tree implementer it would serve cannot reach it | `testkit/varco_chaos`/`varco_conformance` is packaged (itself a standing park, `BACKLOG.md:169`) |
| Caching the subdomain→tenant lookup | Not needed: parsing is pure string work, no I/O. brief 006 §1's *"cached domain→tenant lookup in Redis"* assumes a database-backed mapping, which explicit `base_domains` avoids | A deployment needs vanity/custom domains, which **is** a database-backed mapping — and is then an out-of-tree source over `AbstractTenantCatalog` |
| A `TrustedHostMiddleware` of varco's own | Starlette ships one; wrapping it adds a name and no behaviour | 035's security-header work finds a reason to own the host allowlist |
| RFC 8693 token-exchange **endpoint** | brief 006 §3: the IdP's job; Okta/Entra/Keycloak/ZITADEL all ship it | A consumer needs the internal-issuer fallback and `JwtBuilder.claim("act", …)` proves insufficient |
| DPoP · SPIFFE · CAEP/SSF | `BACKLOG.md:98-100`, unchanged by this plan | As recorded there |

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| **The chain looks adopted and protects nothing** because the app left `enable_tenant_context=True` and no chain installed — the second, undocumented setter (Correction 1) | **Critical** — the exact "documented-but-unguarded insecure default" class this cycle exists to kill | §D-S6-wiring makes the chain middleware authoritative and the inner one defer; Step 18 asserts the override directly; the posture seam reports `unchained_claim_tenant_setter`; the upgrade note carries it under a ⚠️; 4.0 flip-list row 3 removes it |
| **A spoofed `Host` invents a tenant** where the chain has only a subdomain source | **High** | §D-S6-oq3's four defences; the mandatory not-a-security-control sentence asserted in the docstring and the docs; membership binding (Phase 3) catches it for an authenticated caller; `LENIENT` still rejects a forged host that disagrees with a real claim |
| **`STRICT` rejects legitimate one-source traffic** and it is discovered in production | **High** | §D-S6-oq2's zero-claims exemption; `LENIENT` is the default; the migration path makes `strict` step 6 of 6; the posture seam reports the mode so 036 can say "you are lenient" rather than an operator assuming otherwise |
| ⚠️ **ASSUMPTION — a `ContextVar` set in an outer `BaseHTTPMiddleware.dispatch` is visible to an inner one.** Starlette runs `call_next` in a child task, which copies the context at creation | Medium — Phase 2's deferral mechanism rests on it | **Strong in-repo precedent, not a guess**: `tenant_resolution.py:83` already wraps `call_next` in `tenant_context()` and inner code reads `current_tenant()`. Step 5 asserts parent→child propagation directly, and Step 18 asserts it across the real middleware pair before anything is built on it. Fallback if it fails: a `request.scope["state"]` key, which is the same information with a Starlette-internals dependency |
| ⚠️ **ASSUMPTION — no out-of-tree caller subclasses `TenantResolutionMiddleware` or `RequestContextMiddleware`** | Medium | Both gain only keyword-only parameters with defaults (`tenant_resolution.py:51-53` is already `*`-guarded); `dispatch()` keeps its signature; `api_surface.py` does not record class signatures (a documented limitation), so a test pins both `__init__` signatures explicitly |
| ⚠️ **ASSUMPTION — adding `CanonicalClaim.TENANTS` breaks no existing transform config.** It appends a member to a `StrEnum` iterated by `_TARGET_FIELD_PREFIX` | Medium | A target with no configured `*_field` produces no rule at all (`transform/config.py:161-166`), so an unconfigured deployment is unchanged; Step 21's byte-identical assertion proves it; the widened `_build_auth_ctx` trigger is asserted deliberately rather than discovered |
| **Auth runs twice per request** if an app passes `server_auth` to both middlewares with different instances | Medium — cost and a confusing audit trail | The deferral keys on provenance, so the common case (one instance, or `None` to the chain) verifies once; Step 18 counts calls; a Pitfalls row and the recommended wiring in README |
| **An auth 401 escapes `ErrorMiddleware`** because `extra_middleware=` sits outside it | Medium | Mandatory `HTTPException` catch in the chain middleware (§D-S6-wiring ❌), asserted by Step 17; the docs recommend `install_middleware_stack` placement inside `ErrorMiddleware` |
| ⚠️ **ASSUMPTION — `TenantProvenance` on its own `AmbientVar` is accepted as compatible with CLAUDE.md's `RequestContext`-never-holds-the-tenant rule** | Medium — it is a *rule*, and a plan that bends one is a bad precedent | §D-S6-provenance argues it from the rule's own reasoning (composition by ordering; `current_tenant()` unchanged as the single source of truth for *who*); Step 34 writes the distinction into CLAUDE.md so the next reader sees the rule extended deliberately, not eroded |
| **Membership fails open in 3.2** and a deployment believes it is enforced | Medium | `reason="claim_absent"` on every decision reaches 036's audit; one WARNING per process; the posture finding `tenant.membership_missing_claim_allows`; it is 4.0 flip-list row 2 |
| **S16 slips and 4.0's flip list is left incoherent** — removing the header path with no sanctioned delegation path | Medium | §D-S16-cut makes flip-list row 1 explicitly conditional on S16, and says so in the parked row |
| ⚠️ **ASSUMPTION — `str.encode("idna")` is sufficient normalisation for every host varco will see.** It rejects empty and >63-char labels with `UnicodeError` and does not implement IDNA2008/UTS-46 | Low | Every failure path returns `None`, never raises (Step 7 asserts); a non-normalising fallback would be *more* permissive, not less; a mismatch produces "no tenant", which is fail-closed |
| **Plan 036 needs a seam this plan did not export** | Low | §D-036-seams gives both seams with exact signatures and a pinned finding-token set (Step 27); 036 is built last and can request an additive change with a one-line follow-up |

## Open questions

1. **Does `ActAsTenantSource` force `TenantSource.resolve()` to become async?** `DelegationPolicy.allows`
   is `async` (it must be, for an out-of-tree policy that queries a store) while `resolve()` is
   deliberately sync (§D-S6-abc). Decide at Step 39 — **lean toward the middleware `await`ing the
   policy once, before `chain.resolve()`, and passing the resulting `DelegationRecord` in**, so one
   droppable source does not make the whole ABC async. If that proves awkward, the fallback is an
   `async def aresolve()` **default-implemented on the ABC** as `return self.resolve(request)`, so
   no existing source changes.
2. **Should `TenantProvenanceSettings` live in `tenancy/settings.py` beside `TenancySettings`, or in
   its own module?** `settings.py` is currently one cohesive dataclass plus enums. Decide at
   Step 13 — lean same module, with the module docstring extended to name both, mirroring 037's
   identical open question about `rls_check.py`.
3. **Is `403` the right rejection status for a provenance conflict, or `400`?** `403` says "you may
   not act as that tenant" (membership, delegation); `400` says "your request contradicts itself"
   (conflict). Decide at Step 16 — lean **one code, `403`, configurable via `reject_status=`**, on
   the grounds that distinguishing the two hands an attacker an oracle for which source varco
   believed.
