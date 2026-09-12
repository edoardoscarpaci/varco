# Index — 3.2 security release

The `# 3.2 — security release (discover, 2026-09-05)` cycle in [BACKLOG.md](../BACKLOG.md) is
**16 rows (S1–S16) split across five plans**. This file is the map: what each slice owns, the
dependency edges between them, the order to build them in, and what "done" means for the set.

It exists so the slices do not overlap and so a reader can see the whole cycle without opening
five plan files. **It carries no design content of its own** — each plan owns its decisions.

---

## Why this is split at all

Split gate (all four conditions held, any one would have sufficed):

- **2+ independently shippable features.** Security headers ship with nothing else; the tenant
  provenance chain is a self-contained trust model.
- **Far past ~12 steps and 3+ subsystems.** One plan would have spanned `varco_core.jwt`,
  `varco_core.tenancy`, `varco_core.auth`, `varco_fastapi.middleware`, `varco_fastapi.auth`,
  the three admin mounts, `varco_sa` RLS, and the query layer.
- **Uneven risk.** Postgres DDL against a live database (037) sits beside pure-addition
  middleware (035). Isolating the irreversible slice is the point.
- **Sequencing.** S9's preflight checks the defaults the other four plans establish, so 036 is
  not even designable until they exist.

Carved along **deliverable boundaries, not layers** — each plan is independently implementable,
testable and mergeable.

---

## The slices

| Plan | Rows | Deliverable | Risk | Brief |
|---|---|---|---|---|
| [033](033-tenant-identity-provenance.md) ✅ | S6, S5, S16 | Tenant identity provenance & delegation — `TenantSource` chain, `AbstractTenantMembership`, `LegacyTenantSource`, act-as/RFC 8693 | 🔴 centerpiece; changes a security default | 006 |
| [034](034-credential-and-token-lifecycle.md) ✅ | S1, S2, S13, S14 | Credential & token lifecycle — required `algorithms=`, `?api_key=` off by default, `TokenRevocationStore` seam, hashed API keys | 🔴 two breaking flips | 009, 006 |
| [035](035-http-edge-hardening.md) ✅ | S3, S7, S8, S10 | HTTP edge hardening — error-leak fix, security headers, body-size limits, rate-limit middleware | 🟢 three of four are pure additions | 008 |
| [036](036-authorization-surface-and-posture.md) ✅ | S4, S9, S11 | Authorization surface & posture — cross-tenant admin guard, `SecurityPosture` preflight, authz-decision audit | 🟡 must land last | 006 |
| [037](037-data-layer-tenant-enforcement.md) ✅ | S12, S15 | Data-layer enforcement — RLS-by-default, AST tenant-filter guard | 🔴 irreversible DDL; isolated | 007 |

Briefs are in `design/research/`: **006** multi-tenant identity and hardening (backs the cycle),
**007** Postgres RLS enforcement mechanics, **008** HTTP hardening conventions 2026,
**009** token revocation and credential storage.

### Three grouping decisions worth recording

- **S16 (act-as) joins the centerpiece rather than getting its own file.** RFC 8693 token
  exchange is the *sanctioned* version of exactly what `LegacyTenantSource` does unsafely — the
  delegation half of the same trust model. Splitting them would have put one trust model in two
  files. It is 033's final, separable, droppable phase.
- **S2 joins S14, not the HTTP plan.** Both change `ApiKeyAuth`'s constructor in the same file;
  splitting them would mean two plans editing one class.
- **S3 joins the HTTP plan despite being 🔴 while its neighbours are 🟡.** It is an `error.py`
  middleware change; severity does not override subsystem cohesion. It is 035's first phase so
  it can merge ahead of the additions.

---

## Dependency edges

```
035 (HTTP edge)  ──┐
                   ├──► 036 (posture) ◄── S9 checks defaults set by 033, 034, 035
034 (credentials) ─┤
                   │
033 (provenance) ──┴──► 036 (S4 consumes 033's resolved tenant)

037 (RLS) ─────────────► 036 (S9 reports 037's BYPASSRLS/owner-bypass check)
037 ◄── consumes current_tenant() as an INPUT CONTRACT; 033 owns how it is SET
```

| Edge | Nature | Detail |
|---|---|---|
| 033 → 036 | **Hard.** S4 cannot bind admin routes to "the resolved tenant" until 033 defines what that is | 033 exposes the seam; 036 consumes it |
| 033 → 037 | **Soft, one-way.** 037 reads `current_tenant()`, 033 sets it | Already stable today; 037 is not blocked on 033 |
| 034 → 036 | **Hard.** S9 checks for `PassthroughAuth` on a public app and `?api_key=` enabled | 034 exposes introspection; 036 reports it |
| 035 → 036 | **Soft.** S9 may report "no rate limiter on a public app" | 036 degrades gracefully if 035 slips |
| 037 → 036 | **Soft.** 037 defines the RLS posture *check*; 036 builds the harness | 037 ships the check standalone-usable |
| S12 → S15 | **Hard and locked** (`BACKLOG.md:49`) | S15 ships only after S12, never instead of it |
| S6 → S5 | **Hard**, internal to 033 | Membership binding needs a resolved requested-tenant |

⚠️ **Every arrow into 036 is a seam obligation on the source plan.** Each of 033/034/035/037
defines and exports its check or seam; **none of them builds the preflight**. That is 036's
sole ownership, and it is why 036 is planned and built last.

**034's exported seam, verbatim (§D-034-seam) — so 036's planner can consume it without opening
`plans/034-credential-and-token-lifecycle.md`:**

```python
# varco_fastapi.auth.posture
@dataclass(frozen=True)
class AuthPostureReport:
    components: tuple[str, ...]
    api_key_query_fallback_enabled: bool
    api_key_query_param_name: str | None
    api_key_plaintext_source: bool
    api_key_pepper_configured: bool
    websocket_token_query_fallback_enabled: bool
    passthrough_auth_bound: bool

def inspect_auth_posture(auth: AbstractServerAuth) -> AuthPostureReport: ...

# varco_core.revocation.posture
@dataclass(frozen=True)
class RevocationPostureReport:
    store_bound: bool
    store_kind: str
    registry_wired: bool
    failure_mode: str
    require_jti: bool
    token_scope_usable: bool

def inspect_revocation_posture(
    registry: TrustedIssuerRegistry | None = None,
    store: AbstractTokenRevocationStore | None = None,
    settings: JwtVerificationSettings | None = None,
) -> RevocationPostureReport: ...
```

Both are pure, read-only, never raise, never log. `inspect_auth_posture()` recurses into
`CompositeServerAuth.strategies` and `WebSocketAuth.inner` (identity-set-guarded against a
cyclic composite). `passthrough_auth_bound` reports presence, not publicness — 036 owns the
"is this app public" judgement.

---

## Build order

1. **035** — HTTP edge hardening. Lowest risk, three pure additions, no upstream dependency.
   Lands first to build confidence before the risky slices merge. S3 (🔴) can merge on its own.
2. **034** — Credential & token lifecycle. Independent of 033. S1/S2 are 🔴 and cheap.
3. **033** — Tenant identity provenance. The centerpiece and the longest pole. Start planning
   early even though it builds third.
4. **037** — Data-layer enforcement. Independent of all the above; merge on its own so the DDL
   change is reviewed in isolation. ✅ *planned*
5. **036** — Authorization surface & posture. **Must be last** — S9 enumerates the defaults the
   other four establish.

1–4 may proceed in parallel across separate branches; only 036 is genuinely gated.

---

## Definition of done for the whole set

- [ ] All 16 rows S1–S16 are either implemented or carry a written deferral with an un-park
      trigger recorded in BACKLOG.md's parked table. **A dropped row must be argued, not
      silently missing** — this applies to the two rows already flagged as droppable
      (S15 in 037, S16 in 033).
- [ ] `make lint`, `make type-check`, `make test` green; `make integration-test-clean` green
      (RLS and rate-limit rows have real-broker/real-Postgres tests).
- [ ] `scripts/api_surface.py` regenerated and committed. ⚠️ **Corrected (034 §D-034-gate,
      verified against `scripts/api_surface.py:6-8` and the committed snapshot): the gate is
      structurally blind to both of 034's flips** — `JwtParser.parse` is a `classmethod` and
      `ApiKeyAuth.__init__` is a class constructor, and `--check` only records
      `inspect.signature()` for top-level `function`-kind exports. `--check` passes on 034's two
      flips silently; the real regression guard is a dedicated `inspect.signature()` test in each
      owning package's own suite (`varco_core/tests/test_jwt.py`,
      `varco_fastapi/tests/milestone_a/test_server_auth.py`). 033/034/035/037 all add public
      names, which `--check` **does** catch as additive notes.
- [ ] Every new subsystem has a `technical_docs/features/*.md` with a **Pitfalls** table, a
      README usage section with an env-var table, a one-line CLAUDE.md pointer, and a CHANGELOG
      entry — in the same commit, never a follow-up.
- [ ] The **blast-radius rule** is honoured and auditable: every default that flipped in 3.2 had
      a cheap caller-side fix and a named escape hatch; every default needing real application
      work ships **warn-only via `SecurityPosture`** and is scheduled to flip in 4.0. The 4.0
      flip list is written down.
- [ ] The four cycle open questions are each answered in a `DESIGN:` block, not left implicit:
      (1) `SecurityPosture`'s production signal → 036; (2) S6 cross-check when only one source
      is present → 033; (3) subdomain public-suffix trap → 033; (4) does RLS-by-default change
      existing DDL → **037, answered (§D-S12-oq4: no, and the migration story is still
      mandatory)**.
- [ ] An upgrade note exists for every slice that changes observable behaviour (033, 034, 035's
      S3, 037).

---

## Status

| Plan | State |
|---|---|
| 033 | ✅ written — `plans/033-tenant-identity-provenance.md` |
| 034 | ✅ written — `plans/034-credential-and-token-lifecycle.md` |
| 035 | ✅ written — `plans/035-http-edge-hardening.md` |
| 036 | ✅ written — `plans/036-authorization-surface-and-posture.md` |
| 037 | ✅ written — `plans/037-data-layer-tenant-enforcement.md` |

**All five plans are written.** The cycle's four open questions are each answered in a `DESIGN:`
block: (1) `SecurityPosture`'s production signal → `036 §D-S9-oq1`; (2) S6's cross-check with one
source present → `033 §D-S6-oq2`; (3) the subdomain public-suffix trap → `033 §D-S6-oq3`;
(4) does RLS-by-default change existing DDL → `037 §D-S12-oq4`. The **consolidated 4.0 flip list**
required by the definition of done lives in `036 §D-S9-flip`.

Backlog rows are marked `✅ planned` in BACKLOG.md's Live table only as each plan file lands.
