# Plan 034 — Credential & token lifecycle: required `algorithms=` (S1), `?api_key=` off by default (S2), a `TokenRevocationStore` seam (S13), hashed API keys (S14)

Covers BACKLOG 3.2 rows **S1** (🔴 must, S), **S2** (🔴 must, S), **S13** (🟡 should, M) and
**S14** (🟢 nice, S–M).

**Research brief backing this plan:**
`design/research/009-token-revocation-and-credential-storage.md`, written for S13/S14. Every
externally-grounded claim below cites it as `brief 009 §N`. Brief 006 §5 (input handling /
secrets management) is cited for S2 and S14's threat statement.

## Scope and siblings

One of five plans in the 3.2 security release (`plans/000-index-3-2-security-release.md`). This
slice **builds second**, after 035 and independently of 033.

| Plan | Rows | Boundary with this plan |
|---|---|---|
| 033 | S6, S5, S16 | No overlap. 033 owns tenant *provenance*; this plan owns *credential* lifecycle. The one adjacency: `RevocationScope.TENANT` reads `tenant_id` off the verified token's claims, never from `current_tenant()` — see §D-S13-scope |
| 035 | S3, S7, S8, S10 | No overlap. ⚠️ One shared hazard: S3 fixes an error-detail leak in `middleware/error.py`; §D-S13-error applies the *same* discipline independently inside `JwtBearerAuth`'s own 401 detail, which S3 does not touch |
| 036 | S4, S9, S11 | **Owns the `SecurityPosture` preflight.** Phase 4 here *defines and exports* two read-only introspection functions; **this plan must not build a preflight, a startup hook, or a warning emitter on top of them** |
| 037 ✅ | S12, S15 | No overlap. Its §D-S12-posture is the precedent Phase 4 copies: each source plan exports its own typed report dataclass in its own package; 036 aggregates |

**S2 is in this plan and not 035 by a locked grouping decision** (index §"Three grouping decisions
worth recording"): S2 and S14 change the same constructor of the same class, and are therefore
designed here as **one coherent constructor change** (§D-S2S14-ctor), not two.

## Goal

After this plan: no varco entry point can decode a JWT under a silently-defaulted HMAC algorithm;
no varco auth component accepts a credential in a URL query string unless the application named
that parameter on purpose; a varco app can invalidate a JWT before its `exp` — per token, per
subject, per tenant, per issuer — through one ABC with three in-tree implementations; and an
`ApiKeyAuth` never holds a raw API key in memory past its own constructor, comparing digests with
`hmac.compare_digest` instead of raw strings.

## Non-goals

- **No `SecurityPosture` preflight, no startup warning, no CLI verb reporting posture.** Phase 4
  exports `inspect_auth_posture()` / `inspect_revocation_posture()` as *pure functions returning
  frozen dataclasses*, and stops. Plan 036 owns everything that consumes them.
- **No DPoP / sender-constrained tokens (RFC 9449)** — parked at `BACKLOG.md:98`, not relitigated.
- **No CAEP / SSF client.** Parked at `BACKLOG.md:99`. §D-S13-caep states the one property the
  seam must preserve so CAEP can later be a *provider* against it, and nothing more.
- **No RFC 7662 introspection backend.** Brief 009 §3 shows it is a 50–500 ms per-request HTTP
  round trip to the authorization server; that is an out-of-tree implementation of the shipped
  ABC, and §D-S13-backends records the deferral.
- **No RFC 7009 revocation *endpoint*.** varco is a resource server here. Brief 009 §3: the
  resource server cannot call RFC 7009 — it is not the token issuer.
- **No SQL/Mongo revocation backends in 3.2** — §D-S13-backends, with an un-park trigger.
- **varco does not start minting `jti` automatically** — §D-S13-jti. It is on the 4.0 flip list.
- **No new runtime dependency anywhere.** `hashlib`, `hmac`, `secrets`, `abc`, `datetime` only
  (brief 009 §Librarian's Note states the same constraint for `varco_core`).
- **No change to `JwtParser.parse_unverified()`.** Verified: it takes no `algorithms` argument
  (`varco_core/varco_core/jwt/parser.py:165-172`) and never reaches `_jwt.decode()`'s algorithm
  path. It is untouched by S1.

---

## Design

### What already exists — verified against source while writing this plan

| Fact | Location | Consequence |
|---|---|---|
| `JwtParser.parse()` defaults `algorithms` to `["HS256"]` behind an `if algorithms is None:` with the comment *"always pass algorithms explicitly in production"* | `varco_core/varco_core/jwt/parser.py:136-138` | S1's target. The comment is an admission, not a control |
| `parse_unverified()` has **no** `algorithms` parameter | `parser.py:165-172` | Confirmed unaffected by S1 |
| `TrustedIssuerRegistry.verify()` derives `algorithms` from the resolved key: `"algorithms": [pyjwk.algorithm_name]` | `varco_core/varco_core/authority/registry.py:643` | ⚠️ **This is the load-bearing blast-radius fact.** The registry path — which is what `JwtBearerAuth` uses (`server_auth.py:285`) — never touched the `["HS256"]` default. S1's real exposure is direct `JwtParser.parse()` callers only |
| `verify()` ends at `return JwtParser._from_raw_claims(raw)` | `registry.py:682` | The single funnel CLAUDE.md names. The revocation check goes **immediately before** it, after `iss` enforcement — §D-S13-hook |
| `JsonWebToken.jti` already exists and is populated from `raw.get("jti")` | `jwt/model.py:186`, `parser.py:300` | No model change needed for revocation |
| `JwtBuilder.with_random_jti()` / `.token_id(jti)` already exist, both opt-in | `jwt/builder.py:203-226` | §D-S13-jti: the minting side needs docs, not code |
| `ApiKeyAuth.__init__` stores `self._keys = keys` — a raw `dict[str, AuthContext]` — and `__call__` does `self._keys.get(api_key)` | `varco_fastapi/varco_fastapi/auth/server_auth.py:333-344`, `:369` | S14's target. Raw key in memory; `dict.get` is not a constant-time comparison (brief 009 §9) |
| `api_key = request.headers.get(self._header) or request.query_params.get(self._param)` — one line, no toggle, no warning | `server_auth.py:358` | S2's target |
| `WebSocketAuth` query fallback logs at **`_logger.debug`**, not `warning` | `server_auth.py:626-632` | ⚠️ **Backlog correction 1.** `BACKLOG.md:67` says this fallback "at least warns". It does not — it emits a `debug` record, which is invisible under any default logging config. The asymmetry with `ApiKeyAuth` is smaller than the backlog states, and the sub-protocol alternative at `:614-623` is the real mitigation |
| `JwtBearerAuth.__call__` wraps **every** exception from `registry.verify()` into `HTTPException(401, detail=f"Invalid or expired token: {exc}")` | `server_auth.py:284-292` | ⚠️ **Correction 2.** A new `TokenRevokedError` would leak its reason string straight to the client through this `f`-string, and a store outage would be reported as a 401 (a lie). §D-S13-error |
| `JwtVerificationSettings` is the established `VARCO_JWT_*` home, `frozen=True`, with an `AliasChoices` precedent for a non-derivable env name | `varco_core/varco_core/jwt/config.py:61-76` | Revocation settings go here — one settings class, not a second one |
| `NullFeatureFlags` is a scanned `@Singleton(priority=-sys.maxsize - 1)`; `enable_feature_flags(container)` opts in the real one | `varco_core/varco_core/flags/null.py:27`, `flags/di.py:58` | The exact precedent §D-S13-di follows |
| `testkit/varco_conformance/` holds **seven** modules today, not five | `event_bus`, `cache`, `job_store`, `dlq`, `channel_manager`, `idempotency_store`, `webhook_subscription` | ⚠️ **Correction 3.** CLAUDE.md and `COVERAGE.md:3` both still say "five". Plans 029 and 031 added two without updating either. Fixed in Phase 5 |

### ⚠️ Correction 4 — the api-surface gate is **blind to both of this plan's flips**

`plans/000-index-3-2-security-release.md:112-113` states that 034 "narrows a function signature and
is the hard-gate case" for `scripts/api_surface.py --check`. **Verified false.**

`scripts/api_surface.py` records `inspect.signature()` **only for names whose kind is `function`**
(`scripts/api_surface.py:6-8`, and the documented limitation at CLAUDE.md's "Known limitation,
deliberate"). Both of this plan's flips are *methods on exported classes*, and the committed
snapshot proves it:

```
design/api-freeze-and-standards/measurements/api-surface.json:563   "JwtParser":  {"kind": "class", "module": "varco_core.jwt.parser"}
design/api-freeze-and-standards/measurements/api-surface.json:1173  "ApiKeyAuth": {"kind": "class", "module": "varco_fastapi.auth.server_auth"}
```

No `signature` key on either. `JwtParser.parse` is a `classmethod`; `ApiKeyAuth.__init__` is the
documented class blind spot verbatim. **`--check` will pass on both flips, silently.**

| ID | Choice | Consequence |
|---|---|---|
| D-034-gate | **Do not extend `api_surface.py`** to walk class members. Instead, add one dedicated test per flip that asserts the narrowed shape via `inspect.signature()` directly, in the owning package's test suite | The gate keeps its documented, interpreter-stable contract; the two changes this plan actually makes get a real, failing-first guard that runs in `make test` and CI's `unit` job |

**DESIGN: a targeted test over teaching the snapshot about class members**

✅ The reason class signatures are excluded is stated and still true: they are synthesised from
   `__init__`/`__new__` and, for pydantic models and dataclasses, from generated code whose
   rendering is not guaranteed identical across the 3.12/3.13 matrix. Adding classmethods would
   drag that instability in for two symbols' benefit.
✅ A test in `varco_core/tests/` and `varco_fastapi/tests/` fails on the exact regression that
   matters (someone restoring a default), in the job that already runs.
✅ Honest: the index's claim is corrected in writing rather than worked around.
❌ The gate remains blind for the *next* plan too. Accepted — recorded here and in
   `technical_docs/features/credential-and-token-lifecycle.md`, not silently.
  Rejected — **extend `api_surface.py` to record classmethod signatures**: ❌ reintroduces the
  interpreter-dependent rendering that made `--check` unrunnable in CI, to catch two symbols.
  Rejected — **rely on mypy**: ❌ mypy catches in-repo callers only; the break is for *out-of-tree*
  callers, which is precisely what the snapshot exists for and precisely what it cannot see here.

The snapshot **is still regenerated and committed** in Phase 5 — Phase 4 adds public names
(`inspect_auth_posture`, the revocation surface) which `--check` reports as additive notes, and
Phase 1 widens `ApiKeyAuth`'s first parameter to optional, which is invisible for the same reason.

### Measured blast radius (S1, S2, S14) — the grep results, not an estimate

**S1 — `JwtParser.parse()`** (`rg -n "JwtParser\.parse\(" --glob '**/*.py'`, 52 occurrences / 8 files):

| Class | Count | Where |
|---|---|---|
| **Production call sites in any of the ten packages** | **0** | — |
| **Call sites in `examples/`** | **0** | — |
| Docstring / prose occurrences in shipped source | 4 | `jwt/util.py:56,79,195` (3), `jwt/parser.py:132`, `jwt/transform/runtime.py:26` |
| Executable test call sites | **46** | `varco_core/tests/test_jwt.py` (17), `test_jwt_transform_config.py` (21), `test_jwt_transform.py` (5), `test_jwt_profiles.py` (3) |
| …of which already pass `algorithms=` | **1** | `test_jwt.py:278` |
| Docs prose to update | 5 | `README.md:2692,2709`, `varco_core/README.md:144,183,196,202`, `technical_docs/features/jwt-claim-transformer.md:94,262`, `technical_docs/features/token-profiles.md:112` |

**Conclusion: S1 breaks zero varco production code and zero examples.** All 45 unfixed call sites
are in `varco_core/tests/`, and the fix is `, algorithms=["HS256"]` appended. `JwtBearerAuth` and
`PassthroughAuth` are untouched: the former goes through `registry.verify()`, which supplies
`[pyjwk.algorithm_name]` (`registry.py:643`); the latter through `parse_unverified()`, which has no
algorithms parameter. This is what makes S1 a legitimate blast-radius **flip**.

**S2 — `?api_key=` / `?token=` query fallbacks:**

| Class | Count | Where |
|---|---|---|
| In-repo tests exercising the `api_key` query fallback | **0** | verified: `rg -n "api_key=\|params=\{\"api_key\|\?api_key"` returns only BACKLOG/docs/plan hits |
| In-repo tests exercising `WebSocketAuth`'s `?token=` | to confirm in Step 8's grep | `varco_fastapi/tests/` |
| Doc/prose mentions of `?api_key=` | 3 | `server_auth.py:15`, `:308`, `auth/__init__.py:9` |

**S14 — `ApiKeyAuth(` construction** (23 occurrences / 8 files):

| Class | Count | Where |
|---|---|---|
| Production call sites | **0** | `mcp.py:371` and `:867` are **docstring examples** (verified: inside a `Usage::` block); `validation.py:281` is an error-message string |
| `examples/` call sites | **1** | `examples/24-custom-route-params/app.py:63` — `keys={...}, required=False` |
| Test call sites | **20** | `varco_fastapi/tests/` across 6 files, all `keys=` + `required=` |

**Every one of those 21 keeps working unchanged** under §D-S2S14-ctor, because the plaintext `keys=`
path survives and is hashed at construction. S14 is therefore genuinely **additive**; only S2's
query-fallback removal is a flip, and it has zero in-repo consumers.

### Phase order

```
P0  S1    🔴 S  require algorithms= on JwtParser.parse()            ← flip, isolated, mergeable alone
P1  S2+S14 🔴 S  ONE ApiKeyAuth constructor change: query fallback off
                 + keys hashed at construction; WebSocketAuth aligned  ← flip + addition, one class
P2  S13a  🟡 M  varco_core.revocation — the ABC, the entry/verdict types,
                 settings, Null + InMemory, the two new exceptions      ← pure addition
P3  S13b  🟡 M  wire into TrustedIssuerRegistry.verify(); JwtBearerAuth
                 503 mapping; RedisTokenRevocationStore; conformance    ← pure addition
P4  ——    🟡 S  the 036 seam: inspect_auth_posture() +
                 inspect_revocation_posture(). Definitions only.
P5  ——    🟡 S  docs, README, CLAUDE.md, CHANGELOG, api-surface,
                 COVERAGE.md, BACKLOG                                   (same commit as P4)
```

**P0 and P1 are independently mergeable and should be merged first** — they are the 🔴 rows, they
are small, and neither depends on anything below. P2→P3 is the one indivisible pair: P2 alone ships
an ABC nothing consults, which is worse than nothing (it advertises a guarantee it does not make).
If the plan must be cut, cut **after P1** or **after P3**, never between P2 and P3.

### §D-S1-required — required keyword, not a defaulted-to-`None`-that-raises

| ID | Choice | Consequence |
|---|---|---|
| D-S1-required | `algorithms` becomes a **required keyword-only argument**: `def parse(cls, token, secret=None, *, algorithms: list[str], ...)`. A caller who omits it gets a `TypeError` from Python itself, at the call site, with the parameter name in the message | The strongest possible signal, at zero runtime cost, with no new error type |

**DESIGN: required keyword over `None` + `raise ValueError`**

✅ The failure is a **static** one: mypy (`strict = true`, Plan 021) flags every in-repo omission
   at `make type-check` time, before a single test runs. A `ValueError` is only found at runtime,
   on the unhappy path, possibly in production.
✅ IDE/`inspect.signature()` state the requirement; the current docstring's *"Always specify
   explicitly in production"* (`parser.py:80-82`) becomes enforced rather than advisory.
✅ Keyword-only, so no positional-argument reshuffling: `JwtParser.parse(raw, secret)` becomes
   `JwtParser.parse(raw, secret, algorithms=["HS256"])` — a strictly additive edit at every site.
❌ A `TypeError` is less self-explanatory than a curated message. Mitigated: the docstring, the
   CHANGELOG's BREAKING entry and the upgrade note all carry the exact one-line fix, and the
   parameter name appears verbatim in Python's own message.
❌ Dynamic callers doing `parse(*args, **kwargs)` fail at runtime, not type-check time. ⚠️
   ASSUMPTION, filed in Risks: no such caller exists in the repo (Step 1's grep confirms).
  Rejected — **keep the parameter optional but raise on `None`**: ❌ gives up the mypy-time catch,
  which is the whole reason this is cheap; ❌ and a `ValueError` at request time in a hot auth path
  is a worse first encounter than a `TypeError` at import/dev time.
  Rejected — **default to `["RS256"]` instead of `["HS256"]`**: ❌ still a silent default, still
  algorithm confusion in the other direction (an app that genuinely signs with HS256 now fails
  mysteriously), and it does not close the row.
  Rejected — **derive algorithms from the token header's `alg`**: ❌ that *is* the algorithm-
  confusion attack. Named here only so nobody proposes it later.

**The escape hatch is the argument itself.** There is no env var and there must not be one: a
`VARCO_JWT_DEFAULT_ALGORITHMS` would restore exactly the ambient, invisible default the row exists
to remove, and would be settable by an operator who never reads this code. The named fix is
`algorithms=["HS256"]` (or the app's real list) at the call site — one line, greppable, reviewable.

### §D-S2S14-ctor — one constructor change, three knobs, no break

`ApiKeyAuth.__init__` today (`server_auth.py:333-344`):

```python
def __init__(self, keys: dict[str, AuthContext], *, header="X-API-Key",
             param: str = "api_key", required: bool = True) -> None:
```

After:

```python
def __init__(
    self,
    keys: Mapping[str, AuthContext] | None = None,      # plaintext — hashed at construction
    *,
    hashed_keys: Mapping[str, AuthContext] | None = None,  # digest -> ctx, production path
    pepper: bytes | str | None = None,                  # or VARCO_API_KEY_PEPPER
    header: str = "X-API-Key",
    param: str | None = None,                           # ← was "api_key"; None disables the fallback
    required: bool = True,
) -> None:
```

| ID | Choice | Consequence |
|---|---|---|
| D-S2-param | **`param: str \| None = None`** — a single knob. `None` means *there is no query parameter*; naming one re-enables the fallback for that name | The escape hatch is `param="api_key"` — one line, self-documenting at the call site, greppable across a codebase, and impossible to set by accident from an environment |
| D-S2-ws | `WebSocketAuth.token_query_param` becomes `str \| None = None`, same rule (`server_auth.py:589`). Its `_logger.debug` becomes `_logger.warning` **when the fallback is used**, matching what `BACKLOG.md:67` believed was already true | The two components align, and the `Sec-WebSocket-Protocol: bearer.<token>` path (`:614-623`) — which already exists and is browser-compatible — becomes the documented default for browser clients |
| D-S14-hash | `keys=` is **hashed at construction** into the same internal `dict[digest, AuthContext]` that `hashed_keys=` populates directly. Nothing retains a raw key | Every one of the 21 existing in-repo call sites gets constant-time digest comparison with **no code change**; S14 is additive by construction |
| D-S14-algo | **SHA-256, or HMAC-SHA-256 when a pepper is configured.** Never argon2/bcrypt/scrypt | Brief 009 §6 settles this: NIST SP 800-63B Rev 4 (2024) — a look-up secret with ≥112 bits of entropy SHALL be hashed with an approved one-way function; only sub-112-bit secrets need a KDF. Brief 009 §Part-B-options calls argon2/bcrypt for API keys *"a misapplication of password-hashing guidance… accepted as an anti-pattern"* |
| D-S14-compare | Digest → `dict` lookup for O(1) candidate selection, **then** `hmac.compare_digest()` on the digests before returning the context | Brief 009 §9: `==` on a credential is a timing oracle. The dict lookup selects; `compare_digest` is the actual accept decision |
| D-S14-both | `keys=` and `hashed_keys=` together → `ValueError`. Neither → `ValueError` naming both | No silent precedence rule to remember |

**DESIGN: `param: str | None` over a separate `allow_query_param: bool`**

✅ One knob cannot disagree with itself. `allow_query_param=False, param="api_key"` would be a
   readable-but-meaningless state a reviewer has to reason about.
✅ It preserves the *intent* of the only sane existing caller shape: anyone who today writes
   `ApiKeyAuth(keys, param="k")` explicitly named a query parameter and keeps the fallback — their
   code is unchanged and still correct. Only callers who never mentioned `param` lose it, which is
   exactly the population the row targets ("no warning and no toggle").
✅ Mirrors `WebSocketAuth.token_query_param` one-for-one, so the two read identically.
❌ The type of an existing public parameter changes `str` → `str | None`. That is a *widening* for
   callers passing a string and invisible to `api_surface.py --check` (§D-034-gate) — hence Step 9's
   dedicated test.
❌ `param=None` reads slightly less loudly than `allow_query_param=False` in a diff. Mitigated: the
   CHANGELOG BREAKING entry and the docs both lead with the fix string.
  Rejected — **keep the fallback and log a warning**: ❌ the row's whole point is that a credential
  in a URL is already in the access log, the proxy log and the `Referer` header by the time anything
  is logged (brief 006 §5). A warning does not un-log a key.
  Rejected — **an env var `VARCO_API_KEY_ALLOW_QUERY=true`**: ❌ the same objection as
  §D-S1-required's — a security-weakening default that an operator can flip without a code review,
  invisible at the call site.

**DESIGN: a process-wide pepper (HMAC) over a per-key random salt**

Brief 009 §Part-B recommends *SHA-256 + per-key random salt* for a **repository-backed** key store,
and §7 pairs it with a plaintext prefix index for O(1) lookup. `ApiKeyAuth` is neither: it is an
in-memory map, it does not own the key format, and it cannot impose a `sk_live_`-style prefix
convention on keys the application already issued.

✅ With a per-key salt and no prefix convention, verification is an **O(n) scan** over every
   configured key on every request — the exact cost brief 009 §6 says fast hashing exists to avoid.
✅ HMAC-SHA-256 under a single process pepper is deterministic, so the digest is directly
   dict-indexable: O(1), no scan, no key-format requirement. Brief 009 §Part-B lists this as its
   second option ("same performance as SHA-256; adds a server-side pepper… reducing rainbow-table
   risk without per-key overhead").
✅ A rainbow table over 128-bit random API keys is not a threat model that exists; the salt's real
   job (defeating precomputation against *low-entropy* secrets) does not apply here — the same
   entropy argument brief 009 §6 uses to reject argon2.
❌ The pepper must be identical wherever `hash_api_key()` is called offline and wherever
   `ApiKeyAuth` runs. A mismatch rejects every key. **Pitfalls-table row, and the error message
   must say so** without echoing the pepper or the key.
❌ Pepper rotation invalidates every stored digest at once. Accepted and documented: rotation means
   re-hashing from the plaintext source, exactly as brief 009 §10's key-rotation workflow describes
   (multiple active keys with `expires_at`, which the application owns, not varco).
  Rejected — **per-key salt + linear scan**: ❌ O(n) per request in the auth hot path, for a
  precomputation defence that high-entropy secrets do not need.
  Rejected — **require a Stripe/GitHub-style prefix (brief 009 §7, §8)**: ❌ varco does not issue
  these keys and cannot impose a format on keys already in circulation. Documented in the feature
  doc as the recommended *application-side* convention (with CRC32 checksums, brief 009 §8) rather
  than a varco requirement.

`pepper=None` (the default) → plain SHA-256, still hashed-at-rest, still constant-time compared.
Digests are stored prefixed (`"sha256$<hex>"` / `"hmac-sha256$<hex>"`) so a future scheme is
distinguishable and a mismatch is diagnosable.

### §D-S13-shape — the ABC, and what a token must carry to be revocable at all

Brief 009 §1 ranks five mechanisms; §2 is the finding that shapes this design: **`jti` is not
universally present.** Okta and Entra ID emit it by default; **Auth0, Keycloak and Cognito do not**
(brief 009 §2's table). A store keyed only on `jti` "will silently fail if deployed against Auth0,
Keycloak, or Cognito without explicit configuration" (brief 009 §2, ⚠️ CRITICAL DESIGN IMPLICATION).

So the seam is **four-scoped**, not `jti`-only:

```python
class RevocationScope(StrEnum):
    TOKEN   = "token"    # key = jti.        denylist.  needs jti.
    SUBJECT = "subject"  # key = f"{iss}|{sub}".  not-valid-before watermark.  needs iat.
    TENANT  = "tenant"   # key = tenant_id.       not-valid-before watermark.  needs iat.
    ISSUER  = "issuer"   # key = iss.             not-valid-before watermark.  needs iat.
```

`TOKEN` is brief 009 §1's *`jti` denylist* row. `SUBJECT` is its *per-subject "not valid before"*
row (global logout). `TENANT` and `ISSUER` are its *per-tenant / per-issuer kill switch* row — the
two things `BACKLOG.md:78` names as missing ("no logout, no compromise response, no per-tenant kill
switch") map onto `SUBJECT` and `TENANT` respectively, and **neither needs a `jti`**.

```python
@dataclass(frozen=True)
class RevocationEntry:
    scope: RevocationScope
    key: str
    revoked_at: datetime           # aware UTC. for non-TOKEN scopes this is the watermark
    expires_at: datetime | None    # None = indefinite (kill switches)
    reason: str | None = None      # operator note. NEVER returned to a client (§D-S13-error)

@dataclass(frozen=True)
class RevocationVerdict:
    revoked: bool
    scope: RevocationScope | None = None
    key: str | None = None
    reason: str | None = None
```

```python
class AbstractTokenRevocationStore(abc.ABC):
    async def revoke(self, entry: RevocationEntry) -> None: ...
    async def unrevoke(self, scope: RevocationScope, key: str) -> bool: ...
    async def is_revoked(
        self, *, jti: str | None, subject: str | None, issuer: str | None,
        tenant_id: str | None, issued_at: datetime | None,
    ) -> RevocationVerdict: ...
    async def list_entries(self, scope: RevocationScope | None = None) -> Sequence[RevocationEntry]: ...
    async def delete_expired(self) -> int: ...
```

| ID | Choice | Consequence |
|---|---|---|
| D-S13-shape | One `is_revoked()` taking **all five claim inputs**, not five methods | One round trip per verification, whatever the backend. A Redis implementation issues one `MGET` over four candidate keys; an in-memory one does four dict lookups. Five methods would mean five round trips |
| D-S13-nvb | Non-`TOKEN` scopes are **not-valid-before watermarks**: revoked iff `issued_at < revoked_at` | Brief 009 §1: "compares `iat` against stored timestamp… invalidates all old tokens at once". A token minted *after* the kill switch is set is valid — which is what makes a tenant kill switch survivable rather than terminal |
| D-S13-noiat | A token with **no `iat`** is treated as revoked by any matching non-`TOKEN` entry | Fail-closed on the ambiguous case: "issued at an unknown time" cannot be shown to be after the watermark. Stated in the ABC docstring as a contract, and asserted by the conformance suite |
| D-S13-ttl | `expires_at` for a `TOKEN` entry SHOULD be `token_exp + clock_skew` | Brief 009 §5: `revocation_entry_ttl = (token_exp - now_utc) + clock_skew_tolerance_seconds`, so the entry outlives the window a lagging verifier would still accept the token in. `revocation_skew_seconds` defaults to `60.0` (brief 009 §5: OWASP/RFC 7519 recommend 30–60 s) |

**Rejected — `sub + iat` hash as a synthetic `jti` fallback** (brief 009 §Librarian's Note item 5
suggests it): ❌ it is not a token identifier — two tokens minted for the same subject in the same
second collide, and a re-issued token with a copied `iat` is silently pre-revoked; ❌ it changes
what "revoke this token" means depending on which IdP is in front, which is the opposite of a seam;
✅ and it is unnecessary, because `SUBJECT` scope covers the same operational need ("log this user
out") using only standard claims. The brief's own §1 table already ranks per-subject NBF as a
first-class mechanism; there is no reason to smuggle it in disguised as `TOKEN` scope.

### §D-S13-jti — varco does not start minting `jti` in 3.2

| ID | Choice | Consequence |
|---|---|---|
| D-S13-jti | `JwtAuthority`/`JwtBuilder` are **unchanged**. `require_jti` defaults **`False`**: a token with no `jti` is simply not revocable at `TOKEN` scope; the other three scopes still apply. `VARCO_JWT_REVOCATION_REQUIRE_JTI=true` fails closed on a `jti`-less token | Additive. On the 4.0 flip list, reported warn-only by 036 via `inspect_revocation_posture()` |

**DESIGN: opt-in `jti`, not automatic**

✅ Auto-injecting `jti` changes the **signed bytes of every token varco mints**, for every app,
   including those that never bind a revocation store. That is a strictly larger blast radius than
   either of this plan's two 🔴 flips, in service of a 🟡 row.
✅ `with_random_jti()` already exists (`jwt/builder.py:216-226`) — the caller-side fix is one
   builder call, which is the blast-radius rule's own standard for what may flip.
✅ A `jti` is only *useful* alongside a store; minting one universally is cost with no benefit for
   the majority of deployments.
❌ An app that binds a store and forgets `.with_random_jti()` gets silently weaker revocation
   (`SUBJECT`/`TENANT` only). Mitigated: `inspect_revocation_posture()` reports
   `token_scope_usable`, and 036 surfaces it.
  Rejected — **default `require_jti=True` when a store is bound**: ❌ turns binding a store into an
  instant outage for every Auth0/Keycloak/Cognito deployment (brief 009 §2), i.e. it punishes the
  app for doing the right thing. It goes on the 4.0 list *behind* a warn-only period, per the
  blast-radius rule.

### §D-S13-fail — fail-closed by default, one env var to change it, and why

This is the decision most likely to take down a production login flow, so it is argued explicitly.

Brief 009 §4 tabulates three modes and states plainly: **"No industry consensus on a 'right'
answer"**, recommending varco *"document all three options in `TokenRevocationStore`'s ABC and let
applications choose via configuration"*.

| ID | Choice | Consequence |
|---|---|---|
| D-S13-fail | `RevocationFailureMode.FAIL_CLOSED` is the default; `FAIL_OPEN` is one env var away (`VARCO_JWT_REVOCATION_FAILURE_MODE=fail_open`) | A store outage returns **503**, not 401 (§D-S13-error), and every occurrence logs at `error` with the store class name |

**DESIGN: fail-closed default, opt-out to fail-open**

✅ **Nobody is affected on upgrade.** The DI default is `NullTokenRevocationStore`, which cannot
   fail — it performs no I/O. The failure mode only ever applies to an app that *explicitly bound a
   real store*, i.e. one that has stated it cares about revocation. Choosing the weak mode on that
   app's behalf would silently negate the decision it just made.
✅ It is the house pattern, consistently: `tenancy_cache_key()` raises rather than silently
   un-namespacing; `JwtBearerAuth` refuses to construct without an audience; `enforce_issuer`
   defaults `True`. A framework that fails open by default here would be the outlier in its own
   codebase.
✅ The failure is **loud, immediate and correctly classified** — a 503 with a store-outage log line
   is diagnosable in minutes. Fail-open's failure is *silent acceptance of revoked tokens*,
   discovered during an incident review, if ever (brief 009 §4: "Revocation SLA becomes: revocation
   time + outage duration").
✅ The escape hatch is a single env var an operator can set during an incident without a deploy.
❌ A Redis outage becomes an auth outage. This is real and is the strongest argument against.
   Mitigated three ways: (1) the mode is per-deployment configurable, as brief 009 §4 requires;
   (2) the feature doc's Pitfalls table states the trade-off in the operator's own terms and points
   at `FAIL_OPEN` for availability-first deployments; (3) brief 009 §4's own baseline
   recommendation — *"fail-open with short-lived tokens"* — is documented as the supported
   alternative posture, with the token-lifetime precondition spelled out, because fail-open without
   short-lived tokens is the genuinely bad combination.
❌ Two modes, not brief 009 §4's three. See below.
  Rejected — **`FAIL_OPEN` as the default**: ❌ silently converts "revocation is on" into
  "revocation is on when Redis feels like it", with no signal. If varco is going to pick, it must
  pick the one whose failure is visible.
  Rejected — **no default; require an explicit mode when binding a store**: ❌ a third required
  argument on a 🟡 feature; and the answer would be copy-pasted from the docs anyway.

**`FAIL_OPEN_WITHIN_GRACE` (brief 009 §4's third row, "cached denylist + bounded staleness") is
parked, not implemented.** Brief 009's own **Evidence Gap 2** says the quantity that would size the
grace window — cache-miss rates and Redis latency at 1–10 K QPS — is unmeasured, and there is *"no
benchmark"*. Shipping a tunable whose only honest default is a guess is worse than shipping two
modes that mean exactly what they say. Un-park trigger recorded in §Parked.

### §D-S13-hook — the check goes in `verify()`, after `iss`, before `_from_raw_claims`

```
registry.verify(token_str)
  ├─ get_unverified_header → kid → _resolve_key            registry.py:597-619
  ├─ PyJWK → _jwt.decode(...)   signature + exp + aud      registry.py:630-659
  ├─ enforce_issuer: token iss == matched_entry.iss        registry.py:670-679
  ├─ ▸ NEW: revocation check (jti / sub / tenant / iss)    ← here
  └─ JwtParser._from_raw_claims(raw)                       registry.py:682
```

| ID | Choice | Consequence |
|---|---|---|
| D-S13-hook | `TrustedIssuerRegistry.__init__` gains `revocation_store: AbstractTokenRevocationStore \| None = None` (default `None` → **no check, no `await`, no cost**), and `verify()` gains `check_revocation: bool \| None = None` | Zero-config behaviour is byte-identical. `JwtBearerAuth` needs **no change** for the happy path — the store rides on the registry it already holds (`server_auth.py:285`) |
| D-S13-order | After `iss` enforcement, never before | A forged token must fail on its signature/issuer, not on a revocation lookup — otherwise the store becomes an oracle answering questions about unverified input, and every unauthenticated request costs a Redis round trip (a trivial DoS amplifier) |
| D-S13-notinparse | `JwtParser.parse()` does **not** gain a revocation check | It is a stateless classmethod with no I/O and no async — `Thread safety: ✅ Stateless` / `Async safety: ✅ No async operations` (`parser.py:17-18`, `:55-56`). Adding an `await` would break both stated contracts and every synchronous caller |

**Rejected — a revocation check inside `JwtBearerAuth`**: ❌ it would cover HTTP only, leaving every
non-HTTP verification path (a consumer verifying a token off an event, a CLI, a test) unprotected —
the same layering argument that put `enforce_issuer` in the registry rather than the middleware.
**Rejected — a DI-resolved store inside `TrustedIssuerRegistry.from_env()`**: ❌ `varco_core` must
not reach for `DIContainer.current()`; the registry is app-constructed and the store is passed
explicitly, exactly like every other varco_core seam.

### §D-S13-error — the revoked-token 401 must not become an exfiltration surface

Two new exceptions in `varco_core/varco_core/authority/exceptions.py`, beside the three existing
`AuthorityError` subclasses (`:32`, `:69`, `:89`):

- `TokenRevokedError(AuthorityError)` — carries `scope`/`key`/`reason` as **attributes**, and its
  `str()` is the fixed string `"Token has been revoked."` and nothing else.
- `RevocationStoreUnavailableError(AuthorityError)` — raised only under `FAIL_CLOSED`.

`JwtBearerAuth.__call__` currently interpolates *any* exception into the client-visible detail:
`detail=f"Invalid or expired token: {exc}"` (`server_auth.py:290`). So:

| ID | Choice | Consequence |
|---|---|---|
| D-S13-error | `TokenRevokedError.__str__` is constant; `scope`/`key`/`reason` reach the **log only**. `RevocationStoreUnavailableError` is caught **before** the generic handler and mapped to **503** with a fixed detail, not 401 | A client learns "revoked", never *why* or *at what scope* — the same discipline CLAUDE.md applies to `error_params()` and to `ServiceAuthorizationError` excluding `reason`. And a store outage is reported as an outage, not as a bad credential |

This overlaps in *spirit* with 035's S3 but touches a different file and a different code path; the
two are independent and must not be merged.

### §D-S13-scope — `RevocationScope.TENANT` reads the token, never `current_tenant()`

The tenant used for the `TENANT` lookup is the `tenant_id` claim on the token being verified
(canonical, post-transform — `parser.py`'s claim pipeline). It is **not** `current_tenant()`.

✅ `verify()` runs before any tenant is resolved; there is frequently no ambient tenant at all.
✅ A kill switch must key on what the *token* asserts, or a compromised token could dodge the switch
   by being presented on a request that resolves a different ambient tenant.
✅ Keeps the 033/034 boundary clean: 033 owns how the ambient tenant is *set*; this plan never reads
   it. Note this is a *narrower* rule than CLAUDE.md's "`current_tenant()` is the single source of
   truth for who is the tenant" — that rule governs application/data scoping, not the verification
   of a bearer credential that has not yet produced a tenant.

### §D-S13-di — `NullTokenRevocationStore` is the scanned default; `enable_*` opts in

Exactly the `varco_core.flags` shape (`flags/null.py:27`, `flags/di.py:58`):

| Verb | Function | Binds |
|---|---|---|
| scanned `@Singleton(priority=-sys.maxsize - 1)` | `NullTokenRevocationStore` | `AbstractTokenRevocationStore` — never revokes, no I/O, cannot fail |
| `enable_*` | `varco_core.revocation.di.enable_token_revocation(container)` | `InMemoryTokenRevocationStore` (dev/test/single-process) |
| `enable_*` | `varco_redis.di.enable_redis_token_revocation(container)` | `RedisTokenRevocationStore` (production) |

`enable_*` is the correct verb per CLAUDE.md's DI taxonomy: *"flips on an opt-in DI **binding** that
would shadow an app default if auto-registered"* — the same row `varco_casbin.di.enable_policy_authorizer`
and `varco_core.flags.di.enable_feature_flags` occupy.

⚠️ **Binding a store in the container does not by itself enable checking** — the registry must also
receive it (`TrustedIssuerRegistry(..., revocation_store=...)`). That two-step is a deliberate
consequence of §D-S13-hook's "no DI reach-in from varco_core", and it is a **Pitfalls-table row**
and a `inspect_revocation_posture()` field (`store_bound_but_registry_unwired`), because it is the
single most likely way to think revocation is on when it is not.

### §D-S13-backends — three in-tree, SQL/Mongo/introspection deferred

| Backend | Ships in 3.2 | Reason |
|---|---|---|
| `NullTokenRevocationStore` (`varco_core`) | ✅ | The DI default. Null Object |
| `InMemoryTokenRevocationStore` (`varco_core`) | ✅ | Dev/test; single process. Lazily-created `asyncio.Lock` per CLAUDE.md |
| `RedisTokenRevocationStore` (`varco_redis`) | ✅ | Brief 009 §1/§Librarian's Note: the production default. Native `SET … PX` gives the §5 TTL rule for free, and one `MGET` covers all four candidate keys |
| `SATokenRevocationStore` / `BeanieTokenRevocationStore` | ❌ deferred | Brief 009 §5's TTL model is native to Redis and hand-rolled in SQL/Mongo (a sweep job). Both are additive out-of-tree implementations of a shipped ABC; shipping them now, unused, adds two migration surfaces to a 🟡 row. **Un-park trigger**: a consumer needs a revocation denylist that survives a cache flush, or `delete_expired()` needs to be driven by the existing `AbstractJobRunner` |
| RFC 7662 introspection | ❌ parked | Brief 009 §3: 50–500 ms per request and a hard dependency on the authorization server. An out-of-tree implementation of the same ABC |

⚠️ **Redis durability is a real caveat, stated in the Pitfalls table**: a Redis instance without
persistence loses the denylist on restart, resurrecting revoked `TOKEN` entries until their `exp`.
Kill-switch scopes are affected identically. This is the honest cost of the deferral above.

### §D-S13-caep — the one property that keeps CAEP a later provider

CAEP/SSF is parked (`BACKLOG.md:99`). The seam must not foreclose it. Brief 009 §3's "Local
Revocation Store" variant describes exactly the shape: *the authorization server publishes
revocation events (via webhook, SSF/CAEP protocol, or polling) to the resource server; the resource
server stores revoked `jti` values… on each validation, checks the local store.*

The single property that must hold: **`revoke()` is a plain write on the ABC, callable by anything,
and nothing in the verification path assumes who called it.** A future `CaepReceiver` is then an
ordinary `EventConsumer` translating a SET event into `store.revoke(RevocationEntry(...))` — no ABC
change, no verification-path change. Recorded here so a later refactor does not quietly couple
`revoke()` to an HTTP admin surface or to a `RequestContext`.

**No admin mount ships in this plan.** A `mount_revocation_admin()` would be a fourth privileged
HTTP surface under CLAUDE.md's `mount_*` rules and belongs with a dedicated design; the ABC is
directly callable from application code, a CLI verb, or a consumer.

### §D-034-seam — the exact seam Plan 036 consumes

Two pure functions, two frozen dataclasses, **no side effects, no logging, no raising**. This
mirrors 037's §D-S12-posture precedent (`inspect_rls_posture()` → `RlsPosture`, in `varco_sa`):
each source plan exports its own typed report in its own package; 036 aggregates.

```python
# varco_fastapi/varco_fastapi/auth/posture.py       ← NEW MODULE, exported from varco_fastapi.auth
@dataclass(frozen=True)
class AuthPostureReport:
    components: tuple[str, ...]                      # class names walked, in order
    api_key_query_fallback_enabled: bool             # ← S9's "?api_key= is enabled"
    api_key_query_param_name: str | None
    api_key_plaintext_source: bool                   # keys= (plaintext) rather than hashed_keys=
    api_key_pepper_configured: bool
    websocket_token_query_fallback_enabled: bool
    passthrough_auth_bound: bool                     # ← S9's "PassthroughAuth on a public app"

def inspect_auth_posture(auth: AbstractServerAuth) -> AuthPostureReport: ...
```

```python
# varco_core/varco_core/revocation/posture.py       ← exported from varco_core.revocation
@dataclass(frozen=True)
class RevocationPostureReport:
    store_bound: bool                 # a non-Null store is bound to the registry
    store_kind: str                   # concrete class name, or "NullTokenRevocationStore"
    registry_wired: bool              # the §D-S13-di two-step footgun, made reportable
    failure_mode: str                 # "fail_closed" | "fail_open"
    require_jti: bool
    token_scope_usable: bool          # require_jti or the app documented jti minting

def inspect_revocation_posture(
    registry: TrustedIssuerRegistry | None = None,
    store: AbstractTokenRevocationStore | None = None,
    settings: JwtVerificationSettings | None = None,
) -> RevocationPostureReport: ...
```

`inspect_auth_posture()` **walks wrappers**: `CompositeServerAuth`'s members and `WebSocketAuth`'s
`inner`, recursively, so a `PassthroughAuth` hidden inside a composite is still reported. That
recursion is the only non-trivial logic in Phase 4 and it gets its own test.

⚠️ `passthrough_auth_bound` reports **presence, not publicness**. Whether the app is "public" is not
knowable from an auth object — it is a deployment fact. 036 owns that judgement and the
"production" signal (the index's cycle open question 1). This plan reports the fact and stops.

### Alternatives considered (plan-level)

- **Split S2 out into 035 with the other HTTP rows.** Rejected: ❌ two plans editing
  `ApiKeyAuth.__init__` in the same release; ✅/❌ the locked grouping decision at the index already
  settles it, and §D-S2S14-ctor shows the two changes are literally one signature.
- **Ship S13 as a `varco_fastapi` middleware instead of a registry hook.** Rejected: ❌ covers HTTP
  only; ❌ duplicates verification; ✅ the registry is already the single funnel (CLAUDE.md).
- **Defer S13 entirely to 4.0 and ship only S1/S2/S14.** Rejected: ❌ leaves the cycle's only
  "invalidate a credential" capability unbuilt while three credential-hardening rows land, and the
  seam is additive with a Null default — the cheapest possible time to introduce it.
- **One shared `SecurityFinding` type in `varco_core` for all four source plans.** Rejected: ❌ 037
  already landed its own package-local report type; a retrofit would make this plan edit 037's
  shipped surface; ✅ 036 can normalise four small dataclasses trivially.

---

## Steps

### Phase 0 — S1: require `algorithms=` on `JwtParser.parse()` (🔴 must, S)

1. [x] `rg -n "JwtParser\.parse\(" --glob '**/*.py'` and `rg -n "parse\(\*args|parse\(\*\*" varco_*/ examples/`
       — confirm the measured blast-radius table above (0 production, 0 examples, 46 test call
       sites) and that no caller invokes `parse()` through `*args`/`**kwargs` (§D-S1-required's ❌).
       Record both results in the commit message.
2. [x] `varco_core/tests/test_jwt.py` (extend, **failing first**) — `JwtParser.parse(signed, secret)`
       with no `algorithms` raises `TypeError` whose message contains `"algorithms"`; a dedicated
       signature test asserts `inspect.signature(JwtParser.parse).parameters["algorithms"].default
       is inspect.Parameter.empty` and `.kind is KEYWORD_ONLY` — **this is the §D-034-gate test**,
       covering what `api_surface.py --check` structurally cannot see. Its docstring says so and
       cites §D-034-gate.
3. [x] `varco_core/varco_core/jwt/parser.py:60-71` — make `algorithms: list[str]` a required
       keyword-only parameter; delete the `if algorithms is None:` block (`:136-138`). Rewrite the
       `algorithms:` docstring entry (`:80-82`) from advice to contract, and add a `DESIGN:` block
       per §D-S1-required naming algorithm confusion and the rejected alternatives.
4. [x] `varco_core/tests/` — add `algorithms=["HS256"]` to the 45 call sites that lack it
       (`test_jwt.py` 16, `test_jwt_transform_config.py` 21, `test_jwt_transform.py` 5,
       `test_jwt_profiles.py` 3). **Mechanical, no assertion changes.** Every one of these tests
       signs with HS256 already, so the value is correct by construction.
5. [x] `varco_core/varco_core/jwt/util.py:56`, `jwt/parser.py:132`, `jwt/transform/runtime.py:26`
       — update the docstring examples to the new call shape. (`util.py:79` and `:195` are prose
       references, not call shapes; leave them.)
6. [x] `varco_core/tests/test_jwt.py` (extend) — a **regression guard**: `parse_unverified()` still
       accepts no `algorithms` argument and its signature is unchanged (asserted via
       `inspect.signature`), so a future refactor cannot "helpfully" propagate the requirement into
       the one method for which it is meaningless.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/ -k jwt` green; `uv run mypy varco_core/varco_core`
clean. P0 is independently mergeable here.

### Phase 1 — S2 + S14: one `ApiKeyAuth` constructor change (🔴 must, S)

7. [x] `varco_core/varco_core/auth/api_key.py` (**new**, `varco_core` — not `varco_fastapi`) —
       `hash_api_key(raw: str, *, pepper: bytes | str | None = None) -> str` returning
       `"sha256$<hex>"` / `"hmac-sha256$<hex>"`, and `verify_api_key(raw, digest, *, pepper) -> bool`
       using `hmac.compare_digest`. Stdlib only (`hashlib`, `hmac`). Full docstring with a `DESIGN:`
       block per §D-S14-hash/§D-S14-algo/§D-S14-compare, citing brief 009 §6 (NIST SP 800-63B Rev 4,
       ≥112-bit look-up secrets), §7 (why the prefix-index pattern does not transfer here) and §9
       (`hmac.compare_digest`, never `==`). It lives in `varco_core` so a non-FastAPI caller can
       pre-hash keys offline without importing a web framework.
8. [x] `rg -n "token_query_param|\"token\"" varco_fastapi/tests/` — enumerate any test relying on
       `WebSocketAuth`'s `?token=` default and record the count; each gets `token_query_param="token"`
       added in Step 11 (the named escape hatch, exercised by the tests that need it).
9. [x] `varco_core/tests/test_api_key_hash.py` (**new**, failing first) — `hash_api_key` is
       deterministic; differs with and without a pepper; the scheme prefix is present; `verify_api_key`
       is `True` for a match and `False` for a one-character difference; a wrong pepper never
       verifies; empty string raises `ValueError`.
10. [x] `varco_fastapi/tests/milestone_a/test_server_auth.py` (extend, **failing first**) —
        (a) a key in `?api_key=` is **rejected** by default (`ApiKeyAuth(keys={"k": ctx})` + query
        `?api_key=k` → 401); (b) `param="api_key"` re-enables it; (c) `hashed_keys=` accepts the
        matching raw key and rejects a near-miss; (d) `keys=` and `hashed_keys=` together →
        `ValueError`; (e) neither → `ValueError`; (f) after construction with `keys=`, **no raw key
        appears in `vars(auth)`** — walk `auth.__dict__` and assert no configured plaintext key is
        present in any value; (g) the **§D-034-gate signature test**: `param`'s default is `None` and
        its annotation includes `None`, asserted via `inspect.signature`, with a docstring citing
        §D-034-gate; (h) the four existing `ApiKeyAuth` tests (`:73-105`) still pass **unchanged**.
11. [x] `varco_fastapi/varco_fastapi/auth/server_auth.py:306-375` — implement §D-S2S14-ctor:
        the new signature; `ValueError` on both/neither; hash `keys=` at construction via
        `hash_api_key`; store one `dict[digest, AuthContext]`; `pepper` falls back to
        `VARCO_API_KEY_PEPPER`; `__call__` reads the header, and the query param **only when
        `self._param is not None**; lookup by digest then `hmac.compare_digest` before returning.
        Rewrite the class docstring (`:307-331`) — Args, the DESIGN block (currently "static dict
        over DB lookup"), and Edge cases (`"Header check takes priority over query param"` becomes
        `"The query fallback is disabled unless param= names it"`). Delete the `?api_key=` claim from
        the module header (`:15`) and from `auth/__init__.py:9`.
12. [x] `varco_fastapi/varco_fastapi/auth/server_auth.py:585-632` — `WebSocketAuth.token_query_param`
        becomes `str | None = None`; the fallback branch (`:626-632`) is skipped when `None` and
        promotes `_logger.debug` → `_logger.warning` when used (§D-S2-ws, backlog correction 1).
        Update the class docstring's Args (`:564-565`) and its `❌ Query param token is visible in
        server access logs — warn in docs` DESIGN line to say the fallback is now opt-in, and point
        browser clients at the existing sub-protocol path.
13. [x] `varco_fastapi/tests/` — add `token_query_param="token"` to whichever tests Step 8 found.
14. [x] `varco_fastapi/tests/milestone_a/test_server_auth.py` (extend) — `WebSocketAuth`: the
        `?token=` fallback is off by default (401 with no header and no sub-protocol); the
        sub-protocol path still works untouched; `token_query_param="token"` restores the fallback
        and emits a **`warning`**-level record (asserted with `caplog`).

⛔ **CHECKPOINT** — `uv run pytest varco_fastapi/tests/ varco_core/tests/test_api_key_hash.py` green;
`uv run pytest examples/00-full-stack-post-api` green. P0+P1 (both 🔴 rows) are mergeable here.

### Phase 2 — S13a: the `varco_core.revocation` seam (🟡 should, M) — pure addition

15. [x] `varco_core/tests/test_revocation_model.py` (**new**, failing first) — `RevocationScope`
        members; `RevocationEntry` is frozen and rejects a naive `datetime` (aware-UTC only, the
        house rule); `RevocationVerdict(revoked=False)` has `scope is None`;
        `RevocationEntry.for_token(jti, exp, skew=60)` sets `expires_at == exp + 60s`
        (brief 009 §5's TTL rule, asserted as an arithmetic property).
16. [x] `varco_core/varco_core/revocation/__init__.py`, `model.py`, `base.py` (**new package**) —
        `RevocationScope`, `RevocationEntry`, `RevocationVerdict`, `RevocationFailureMode`,
        `AbstractTokenRevocationStore` per §D-S13-shape. The ABC docstring is the contract document:
        the four scopes and their lookup keys, the `issued_at < revoked_at` watermark rule
        (§D-S13-nvb), the **missing-`iat` → treated as revoked** rule (§D-S13-noiat), the TTL
        convention (brief 009 §5), the three failure modes brief 009 §4 requires be documented, and
        `Thread safety:` / `Async safety:` blocks. `from __future__ import annotations`; imports
        limited to `abc`, `dataclasses`, `datetime`, `enum`, `typing`.
17. [x] `varco_core/tests/test_revocation_memory.py` (**new**, failing first) — `InMemoryTokenRevocationStore`:
        `TOKEN` revoke→`is_revoked` true, unknown `jti` false; `SUBJECT` revokes a token with
        `iat < revoked_at` and **not** one with `iat > revoked_at`; `TENANT` and `ISSUER` likewise;
        a token with `iat=None` and a matching non-`TOKEN` entry → revoked (§D-S13-noiat); an expired
        entry stops matching; `delete_expired()` returns the count removed; `unrevoke()` returns
        `False` for an absent key; concurrent `revoke()`s do not lose entries.
18. [x] `varco_core/varco_core/revocation/memory.py`, `null.py` (**new**) —
        `InMemoryTokenRevocationStore` (dict of `(scope, key) -> RevocationEntry`, a **lazily
        created** `asyncio.Lock`, never at `__init__`) and `NullTokenRevocationStore` (a scanned
        `@Singleton(priority=-sys.maxsize - 1)`, always `RevocationVerdict(revoked=False)`, no I/O).
        `null.py`'s docstring states it is a Null Object that deliberately violates the ABC's
        revoke→is_revoked contract, and points at the `COVERAGE.md` row Step 39 adds.
19. [x] `varco_core/varco_core/revocation/di.py` (**new**) — `enable_token_revocation(container)`
        per §D-S13-di and CLAUDE.md's `enable_*` row. Docstring notes the ⚠️ two-step: binding the
        store does not wire the registry.
20. [x] `varco_core/varco_core/jwt/config.py` — add to `JwtVerificationSettings` (**not** a second
        settings class): `revocation_failure_mode: RevocationFailureMode = FAIL_CLOSED`,
        `revocation_require_jti: bool = False`, `revocation_skew_seconds: float = 60.0`,
        `revocation_enabled: bool = True` (meaning "consult the store *if one is wired*"). Env names
        `VARCO_JWT_REVOCATION_*` — check whether the `env_prefix` + field-name rule yields the
        intended names and add `AliasChoices` where it does not, following the `enforce_issuer`
        precedent (`config.py:69-75`). Extend the class docstring's `Attributes:` block for each.
21. [x] `varco_core/tests/test_jwt_verification_settings.py` (extend or new) — every new field's
        default; each env var parses; **the existing "defaults are byte-identical" assertions still
        pass**; `RevocationFailureMode` parses case-insensitively from `fail_open`/`FAIL_OPEN`.
22. [x] `varco_core/varco_core/authority/exceptions.py` — `TokenRevokedError` and
        `RevocationStoreUnavailableError` per §D-S13-error, both `AuthorityError` subclasses.
        `TokenRevokedError.__str__` returns the fixed string; `scope`/`key`/`reason` are attributes
        only. Docstrings state the exfiltration rule explicitly and cite CLAUDE.md's `error_params()`
        warning as the same discipline.
23. [x] `varco_core/tests/test_revocation_errors.py` (**new**) — `str(TokenRevokedError(scope=...,
        key="jti-1", reason="compromised"))` contains **neither** `"jti-1"` nor `"compromised"`;
        the attributes do carry them; both are `AuthorityError` subclasses (so existing
        `except AuthorityError` handlers keep working).
24. [x] `varco_core/varco_core/__init__.py` — add the new public names to `_LAZY` **and** `__all__`
        (CLAUDE.md: a name in one but not the other is the trap the lazy-import design calls out).
        Verify with `uv run python scripts/import_budget.py --check --warn-only` that
        `varco_core`'s import cost is unchanged — the new package must be reachable only lazily.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/ -k "revocation or jwt"`; `make import-budget`
shows no regression. **Do not stop here** — an ABC nothing consults advertises a guarantee it does
not make.

### Phase 3 — S13b: wiring, the Redis backend, conformance (🟡 should, M)

25. [x] `varco_core/tests/test_registry_revocation.py` (**new**, failing first) — with a stub
        registry entry and an `InMemoryTokenRevocationStore`: `verify()` with **no** store is
        byte-identical to today (assert the store is never touched via a spy); a revoked `jti`
        raises `TokenRevokedError`; a `SUBJECT` watermark revokes an older token and admits a newer
        one; `check_revocation=False` bypasses; a store raising under `FAIL_CLOSED` →
        `RevocationStoreUnavailableError`, under `FAIL_OPEN` → the token verifies and an `error`
        record is logged (`caplog`); `require_jti=True` + a `jti`-less token → `TokenRevokedError`;
        `require_jti=False` + a `jti`-less token with no matching non-`TOKEN` entry → verifies;
        **the revocation lookup never happens for a token that fails `iss` enforcement** (spy asserts
        zero calls — §D-S13-order).
26. [x] `varco_core/varco_core/authority/registry.py` — `__init__` gains
        `revocation_store: AbstractTokenRevocationStore | None = None`; `verify()` gains
        `check_revocation: bool | None = None` and performs the check **between** the `iss` block
        (`:670-679`) and `return JwtParser._from_raw_claims(raw)` (`:682`). Extend `verify()`'s
        docstring `Args:`/`Raises:`/`Edge cases:` — including that a `TENANT`-scope lookup reads the
        token's `tenant_id` claim and never `current_tenant()` (§D-S13-scope).
27. [x] `varco_core/varco_core/authority/registry.py` — `from_env()` gains a
        `revocation_store=` passthrough. No env var may construct a store: a store is an object with
        a connection, not a string.
28. [x] `varco_fastapi/tests/milestone_a/test_server_auth.py` (extend, failing first) —
        `JwtBearerAuth`: a `TokenRevokedError` from `verify()` → **401** whose detail contains
        neither the reason nor the scope nor the key; a `RevocationStoreUnavailableError` → **503**
        with a fixed detail; the existing exact-args mock assertion at `:270-282`
        (`test_jwt_bearer_auth_calls_registry_verify`) still passes **unchanged** — no new kwarg is
        added to the `verify()` call.
29. [x] `varco_fastapi/varco_fastapi/auth/server_auth.py:284-292` — add
        `except RevocationStoreUnavailableError` → `HTTPException(503, detail="Token verification is
        temporarily unavailable.")` and `except TokenRevokedError` → `HTTPException(401,
        detail="Token has been revoked.")`, both **before** the existing generic handler, and both
        logging the full exception server-side. The generic `f"Invalid or expired token: {exc}"`
        branch is otherwise untouched (035/S3 owns the wider question).
30. [x] `testkit/varco_conformance/token_revocation.py` (**new**) —
        `TokenRevocationStoreConformance`, one abstract `store` fixture, deliberately **not** named
        `Test*`. Covers the ABC contract: revoke→is_revoked per scope; the watermark rule; the
        missing-`iat` rule; expiry; `unrevoke`; `delete_expired`; `list_entries` filtering;
        idempotent double-`revoke`.
31. [x] `varco_core/tests/test_conformance_inmemory.py` (extend) — subclass it for
        `InMemoryTokenRevocationStore`. No Docker.
32. [x] `varco_redis/tests/test_redis_revocation.py` (**new**, `@pytest.mark.integration`) —
        subclass `TokenRevocationStoreConformance` against a real Redis using the session-scoped
        `redis_url` fixture and a `uuid4().hex[:8]` key namespace (CLAUDE.md's per-test namespacing
        rule). Plus Redis-specific assertions: a `TOKEN` entry's Redis TTL is within a second of
        `expires_at - now`; an `expires_at=None` kill switch has **no** TTL.
33. [x] `varco_redis/varco_redis/revocation.py` (**new**) — `RedisTokenRevocationStore`:
        `SET <ns>:<scope>:<key> <payload> PX <ttl>` for the TTL rule (brief 009 §5), one `MGET` over
        the four candidate keys in `is_redis`… `is_revoked()` (§D-S13-shape's one-round-trip
        property). Configurable key namespace. Docstring states the ⚠️ persistence caveat from
        §D-S13-backends.
34. [x] `varco_redis/varco_redis/di.py` — `enable_redis_token_revocation(container)`; `__all__`
        updated. ⚠️ CLAUDE.md's CloudEvents rule applies by analogy: if the store is produced by a
        `@Provider`, the method must **declare** every dependency it needs, or providify silently
        injects nothing.
35. [x] `varco_redis/tests/test_redis_revocation_di.py` (**new**) — with default settings,
        `container.scan("varco_redis", recursive=True)` binds **`NullTokenRevocationStore`** (from
        `varco_core`'s scan) and no Redis store exists; after `enable_redis_token_revocation`,
        exactly one Redis store is bound. Mirrors `test_tls_di.py`'s "nothing is registered by
        default" assertion shape.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/ varco_redis/tests/`; then
`make integration-test-clean PKG=varco_redis`.

### Phase 4 — the 036 seam (🟡 should, S) — definitions only

36. [x] `varco_fastapi/tests/auth/test_auth_posture.py` (**new**, failing first) —
        `inspect_auth_posture()` on: a default `ApiKeyAuth` → `api_key_query_fallback_enabled=False`;
        with `param="api_key"` → `True` and `api_key_query_param_name="api_key"`; constructed with
        `keys=` → `api_key_plaintext_source=True`, with `hashed_keys=` → `False`; a bare
        `PassthroughAuth` → `passthrough_auth_bound=True`; a `PassthroughAuth` **nested inside a
        `CompositeServerAuth` inside a `WebSocketAuth`** → still `True` (the recursion test);
        `components` lists every class walked, in order; the function **never raises** on an
        unknown `AbstractServerAuth` subclass and never logs.
37. [x] `varco_fastapi/varco_fastapi/auth/posture.py` (**new**) — `AuthPostureReport` +
        `inspect_auth_posture()` per §D-034-seam. Export from `varco_fastapi/auth/__init__.py`'s
        `__all__` and the package `__init__`. Module docstring states the boundary in one line:
        *"Reports facts. Plan 036 owns the judgement, the thresholds and the startup wiring — do not
        add a warning, a raise or a lifespan hook here."*
38. [x] `varco_core/tests/test_revocation_posture.py` (**new**) + `varco_core/varco_core/revocation/posture.py`
        — `RevocationPostureReport` + `inspect_revocation_posture()`. Covers the §D-S13-di two-step:
        a store bound in DI but a registry constructed without one → `store_bound=True`,
        `registry_wired=False`. Same "facts only" docstring boundary.

### Phase 5 — docs, snapshot, backlog (🟡 should, S — **same commit as Phase 4**)

39. [x] `technical_docs/features/credential-and-token-lifecycle.md` (**new**) — the four rows'
        design narrative, the revocation scope table, the failure-mode trade-off in operator terms,
        and a **Pitfalls table** with at least: *pepper mismatch between offline hashing and
        runtime* → every key 401s; *store bound in DI but registry not wired* → revocation silently
        never runs (§D-S13-di); *Redis without persistence* → revoked tokens resurrect on restart;
        *`FAIL_CLOSED` + a Redis outage* → 503s, and the `FAIL_OPEN` + short-token alternative
        (brief 009 §4); *no `jti` from Auth0/Keycloak/Cognito* → `TOKEN` scope silently unusable
        (brief 009 §2); *`param="api_key"` restored for convenience* → the key is back in every
        access log; *the api-surface gate does not see either flip* (§D-034-gate).
40. [x] `README.md` + `varco_core/README.md` — a "Token revocation" usage section and an API-key
        hashing snippet; a `VARCO_*` env-var table covering `VARCO_JWT_REVOCATION_FAILURE_MODE`,
        `VARCO_JWT_REVOCATION_REQUIRE_JTI`, `VARCO_JWT_REVOCATION_SKEW_SECONDS`,
        `VARCO_JWT_REVOCATION_ENABLED`, `VARCO_API_KEY_PEPPER`. Fix every `JwtParser.parse()`
        example listed in the blast-radius table (`README.md:2692,2709`;
        `varco_core/README.md:144,183,196,202`; `technical_docs/features/jwt-claim-transformer.md:94,262`;
        `technical_docs/features/token-profiles.md:112`).
41. [x] `CLAUDE.md` — a one-line pointer to the new feature doc under the Authority/JWT section; a
        decision-tree entry (*"Revoke a credential before its natural expiry? → `varco_core.revocation`,
        never a second verification path"*; *"Store an API key? → `varco_core.auth.api_key.hash_api_key`,
        never a raw dict"*); and **update the "five conformance modules" count to eight** in both
        places it appears (correction 3).
42. [x] `testkit/varco_conformance/COVERAGE.md` — add the `token_revocation` row
        (`InMemoryTokenRevocationStore` ✅, `RedisTokenRevocationStore` ✅) and a **Stated absence**
        entry for `NullTokenRevocationStore` (Null Object, same shape as `NoopEventBus`). Correct
        its own "five suites" header to eight (`COVERAGE.md:3`, `:5`, `:12`).
43. [x] `CHANGELOG.md` `[Unreleased]` — a **BREAKING** section for S1 and S2 with the exact one-line
        fixes (`algorithms=["HS256"]`; `param="api_key"`; `token_query_param="token"`), the measured
        blast radius, and ⚠️ the note that `api_surface.py --check` does **not** catch either
        (§D-034-gate) so out-of-tree callers must read this entry. An `### Added` section for S13/S14.
44. [x] `uv run python scripts/api_surface.py` — regenerate and commit both outputs (Phase 4 adds
        public names). Then `uv run python scripts/api_surface.py --check` must pass.
45. [x] `BACKLOG.md` — mark S1, S2, S13, S14 `✅ planned → plans/034-…`; add the **deferred
        revocation backends** (SA/Beanie/introspection) and **`FAIL_OPEN_WITHIN_GRACE`** rows to the
        parked table with the un-park triggers from §D-S13-backends and §D-S13-fail; correct the S2
        row's *"`WebSocketAuth`'s `?token=` fallback at least warns"* claim (it logs at `debug`).
46. [x] `plans/000-index-3-2-security-release.md` — set 034's status to ✅ written; correct line
        112-113's api-surface claim per §D-034-gate; record the exported seam names so 036's planner
        can consume them without opening this file.

---

## Migration and upgrade note (existing deployments — read before shipping)

| Change | Who is affected | The fix |
|---|---|---|
| **S1** `JwtParser.parse()` requires `algorithms=` | Anyone calling `JwtParser.parse()` **directly**. Not `JwtBearerAuth`, not `PassthroughAuth`, not `TrustedIssuerRegistry.verify()` — all three are structurally unaffected (`registry.py:643`, `parser.py:165-172`) | `JwtParser.parse(raw, secret, algorithms=["HS256"])` — use the algorithm you actually sign with. `TypeError` at the call site, caught by mypy |
| **S2a** `ApiKeyAuth` query fallback off | Anyone whose clients send `?api_key=`. Zero in-repo consumers | `ApiKeyAuth(..., param="api_key")`. **Better**: move clients to the `X-API-Key` header — the key is otherwise in every access log, proxy log and `Referer` |
| **S2b** `WebSocketAuth` `?token=` off | Browser WS clients that cannot set an `Authorization` header | Preferred: `Sec-WebSocket-Protocol: bearer.<token>` (already supported, `server_auth.py:614-623`). Escape hatch: `WebSocketAuth(inner, token_query_param="token")` |
| **S14** keys hashed at construction | Nobody. `keys=` still accepted and hashed in place | Optional hardening: pre-hash with `hash_api_key()` and pass `hashed_keys=`, so plaintext never enters the process |
| **S13** revocation | Nobody. `NullTokenRevocationStore` is the default and performs no I/O; `TrustedIssuerRegistry(revocation_store=None)` skips the check entirely | Opt in: `enable_token_revocation(container)` (dev) or `enable_redis_token_revocation(container)` (prod) **and** pass the store to the registry — both steps |

**4.0 flip list** (recorded here, reported warn-only by 036 in 3.2):

1. `ApiKeyAuth(keys=...)` plaintext path → `DeprecationWarning` in 3.3, removed in 4.0;
   `hashed_keys=` becomes the only path.
2. `JwtAuthority`-minted tokens carry a `jti` by default (§D-S13-jti).
3. `revocation_require_jti` defaults to `True` **when a non-Null store is wired**.
4. `PassthroughAuth` requires an explicit `acknowledge_unverified=True` (036 reports it in 3.2;
   the flip itself is not this plan's to schedule).

---

## Edge cases

- `JwtParser.parse(raw, secret, algorithms=[])` → PyJWT's own error propagates unchanged. varco does
  not add an empty-list check: an empty list is an explicit, auditable statement, unlike a default.
- `ApiKeyAuth(keys={})` (used by 5 existing tests) → still legal; every key 401s. Unchanged.
- `ApiKeyAuth(param="")` → `ValueError`. An empty parameter name is a typo, not a disable.
- Header **and** query param both present, with `param=` set → header wins, as today (`:358`'s
  `or` semantics preserved).
- `hashed_keys` containing a digest with an unknown scheme prefix → `ValueError` at construction,
  naming the offending scheme, never the digest.
- A revocation entry whose `expires_at` is in the past → `is_revoked()` returns `False`; it is
  garbage, not a revocation. `delete_expired()` reclaims it.
- `revoke()` called twice for the same `(scope, key)` → last write wins, idempotent. Conformance
  asserts it.
- A `TOKEN`-scope revocation for a token that never had a `jti` → impossible to express; the caller
  gets a `ValueError` from `RevocationEntry.for_token(jti=None, …)`.
- `RevocationScope.TENANT` with no `tenant_id` claim on the token → that lookup is skipped; the
  other three still run.
- A token with `iat` in the future (clock skew) and a matching watermark → **not** revoked
  (`iat >= revoked_at`). Documented; the skew allowance lives on the entry's TTL, per brief 009 §5.
- `inspect_auth_posture()` on a cyclic composite (an auth object containing itself) → terminates via
  an identity-set guard; asserted.
- `inspect_revocation_posture(registry=None, store=None)` → a report with `store_bound=False` and
  `registry_wired=False`; never raises.

---

## Verification

```bash
# Per phase
uv run pytest varco_core/tests/ -k "jwt or revocation or api_key"
uv run pytest varco_fastapi/tests/milestone_a/test_server_auth.py varco_fastapi/tests/auth/
uv run pytest varco_redis/tests/test_redis_revocation_di.py

# Whole set
make lint                 # ruff check + ruff format --check + api-check + asyncapi-check + import-budget
make type-check           # mypy strict over the ten dirs
make test                 # all eleven suites, accumulating
make integration-test-clean PKG=varco_redis   # the Redis revocation conformance leg

# Gates this plan specifically touches
uv run python scripts/api_surface.py --check          # must pass (additions only — see §D-034-gate)
uv run python scripts/import_budget.py --check --warn-only   # varco_core import cost unchanged
uv run pytest varco_core/tests/test_bump_script.py    # version coherence, untouched
```

Manual, one-time, recorded in the commit message:

```bash
rg -n "JwtParser\.parse\(" --glob '**/*.py'    # expect 0 production, 0 examples (Step 1)
rg -n "ApiKeyAuth\(" --glob '**/*.py'          # expect 0 production, 1 example (Step 8)
rg -n "algorithms" varco_core/varco_core/jwt/parser.py   # no defaulting branch remains
```

---

## Parked

| Item | Why | Un-park trigger |
|---|---|---|
| `SATokenRevocationStore` / `BeanieTokenRevocationStore` | §D-S13-backends — brief 009 §5's TTL model is native to Redis and hand-rolled elsewhere; both are additive out-of-tree implementations of a shipped ABC | A consumer needs a denylist surviving a cache flush, or `delete_expired()` gets driven by `AbstractJobRunner` |
| RFC 7662 introspection backend | Brief 009 §3: 50–500 ms/request and a hard dependency on the authorization server | A regulated consumer needs source-of-truth revocation and accepts the latency |
| `FAIL_OPEN_WITHIN_GRACE` (brief 009 §4 row 3) | Brief 009 **Evidence Gap 2**: the cache-miss/latency data that would size the window does not exist. A tunable whose default is a guess is worse than two honest modes | A performance brief measures it, per that evidence gap's own suggestion |
| `mount_revocation_admin()` | A fourth privileged HTTP surface under CLAUDE.md's `mount_*` rules; needs its own design | An operator asks for revocation over HTTP rather than via code/CLI |
| Automatic `jti` minting | §D-S13-jti — changes signed bytes for every app | 4.0, after a 036 warn-only period |
| API-key prefix + CRC32 checksum convention (brief 009 §7/§8) | varco does not issue these keys and cannot impose a format | varco grows a key-*issuance* API rather than a key-*verification* one |
| CAEP / SSF receiver | `BACKLOG.md:99`. §D-S13-caep records the one property that keeps it additive | Unchanged from the backlog row |

---

## Risks

- ⚠️ **ASSUMPTION — no dynamic `JwtParser.parse(*args, **kwargs)` caller exists.** Step 1's grep
  verifies in-repo; out-of-tree callers get a `TypeError` at runtime rather than type-check time.
  The CHANGELOG BREAKING entry is the only mitigation available. **Invariant**: `algorithms` must
  never regain a default, in any form, including an env var.
- ⚠️ **ASSUMPTION — no out-of-tree caller passes `ApiKeyAuth`'s `param` positionally.** It is
  already keyword-only today (`server_auth.py:336`), so this is verified for `param`; the risk is
  limited to callers relying on the *default*, which is exactly the flip.
- **The api-surface gate is blind to both flips** (§D-034-gate, verified against
  `api-surface.json:563,1173`). Mitigation: Steps 2 and 10(g) add real signature tests.
  **Invariant**: those two tests must never be deleted "because the gate covers it".
- **`FAIL_CLOSED` can turn a Redis outage into an auth outage.** Argued in §D-S13-fail; mitigated by
  the Null default (no existing app is exposed), the one-env-var escape hatch, and the Pitfalls row.
  **Invariant**: the default only ever applies to an app that explicitly wired a non-Null store.
- **Redis without persistence resurrects revoked tokens on restart** (§D-S13-backends). Documented,
  not fixed in 3.2. **Invariant**: the `TOKEN`-scope TTL never exceeds `exp + skew`, so the
  resurrection window is bounded by the token's own lifetime.
- ⚠️ **ASSUMPTION — `hmac.compare_digest` over hex digest *strings* is the intended stdlib usage.**
  Brief 009 §9 says it "works with bytes or strings (converted to bytes internally)". Both operands
  here are ASCII hex of fixed length, so the length-leak concern does not arise. If review disagrees,
  compare `bytes.fromhex()` values instead — a one-line change confined to `verify_api_key`.
- **`RevocationEntry.reason` is a new operator-authored string that reaches logs.** It must never
  reach a client (§D-S13-error, asserted by Step 23). **Invariant**: `TokenRevokedError.__str__` is
  a constant.
- **Phase 4's report dataclasses will be consumed by 036 and are therefore public API from day one.**
  Adding a field later is additive and safe; renaming one is not. The field names in §D-034-seam are
  the contract, and 036's planner should be given them verbatim.
- ⚠️ **ASSUMPTION — `VARCO_JWT_` + field name yields the intended `VARCO_JWT_REVOCATION_*` env
  names.** Step 20 verifies against pydantic-settings' actual behaviour and adds `AliasChoices`
  where it does not, following `enforce_issuer`'s precedent (`config.py:69-75`). Not asserted from
  memory.

## Open questions

1. **Should `revocation_enabled=False` skip the check even when a store is wired?** Shipped as yes
   (an incident kill-switch for the kill-switch), but it is a foot-gun an operator could leave set.
   Reconsider if 036 wants to report it as a finding — it probably does.
2. **Does `enable_token_revocation()` belong in `varco_core.revocation.di` or `varco_core.di`?**
   Followed the `varco_core.flags.di` precedent (per-subsystem `di.py`). Flagged only because
   `varco_core` now has several.
