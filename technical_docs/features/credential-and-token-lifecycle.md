# Credential & token lifecycle — required `algorithms=`, `?api_key=` off by default, token revocation, hashed API keys

Plan 034 (3.2 security release, BACKLOG rows **S1**, **S2**, **S13**, **S14**). Research brief
backing S13/S14: `design/research/009-token-revocation-and-credential-storage.md`.

## S1 — `JwtParser.parse()` requires `algorithms=`

`algorithms` is now a **required keyword-only** argument (`TypeError` at the call site if
omitted, caught by mypy `strict = true` before a single test runs). There is no default and never
will be — no `VARCO_JWT_DEFAULT_ALGORITHMS` env var either, because that would restore exactly
the ambient, invisible default this row removes.

**Zero varco production code and zero examples are affected.** `JwtBearerAuth` goes through
`TrustedIssuerRegistry.verify()`, which derives `algorithms` from the resolved key
(`[pyjwk.algorithm_name]`) and never touched the old default; `PassthroughAuth` goes through
`parse_unverified()`, which has no `algorithms` parameter at all. The fix, everywhere it applies,
is one line: `JwtParser.parse(raw, secret, algorithms=["HS256"])` (or the algorithm you actually
sign with).

## S2 / S14 — one `ApiKeyAuth` constructor change

`ApiKeyAuth`'s query-parameter fallback and its plaintext key storage were two independent
security gaps that happen to live on the same constructor, so they were designed and shipped as
one coherent change:

```python
ApiKeyAuth(
    keys: Mapping[str, AuthContext] | None = None,       # plaintext, hashed at construction
    *,
    hashed_keys: Mapping[str, AuthContext] | None = None, # digest -> ctx, the production path
    pepper: bytes | str | None = None,                    # or VARCO_API_KEY_PEPPER
    header: str = "X-API-Key",
    param: str | None = None,                             # None = the query fallback is OFF
    required: bool = True,
)
```

- **`param=None` is the default — the `?api_key=` fallback is off.** A credential in a URL query
  string is already in the access log, the proxy log, and the `Referer` header by the time
  anything could warn about it (brief 006 §5) — a warning does not un-log a key. Name
  `param="api_key"` to opt back in, one line, greppable. `WebSocketAuth.token_query_param` got the
  identical treatment (`None` by default; naming a value re-enables it and now logs at
  `warning`, not the `debug` level it used before — a since-corrected `BACKLOG.md` claim that the
  fallback "at least warns").
- **`keys=` is hashed immediately at construction**, into the same internal
  `dict[digest, AuthContext]` that `hashed_keys=` populates directly — no code change needed for
  any of the 21 existing in-repo `ApiKeyAuth(keys=...)` call sites; they get constant-time digest
  comparison for free. `varco_core.auth.api_key.hash_api_key()`/`verify_api_key()` are the
  stdlib-only (`hashlib`, `hmac`) primitives behind this — SHA-256, or HMAC-SHA-256 under a
  process-wide pepper, never argon2/bcrypt/scrypt (NIST SP 800-63B Rev 4 only requires a KDF for
  *low*-entropy look-up secrets; a varco-issued API key is high-entropy, so a KDF is a liability
  on the auth hot path, not a hardening).
- `keys=` and `hashed_keys=` are mutually exclusive — passing both, or neither, raises
  `ValueError`.

## S13 — `varco_core.revocation`: invalidate a token before its `exp`

### The seam is four-scoped, not `jti`-only

`jti` is not universally present — Okta and Entra ID emit it by default, **Auth0, Keycloak, and
Cognito do not** (brief 009 §2). A store keyed only on `jti` would silently do nothing for those
deployments. So `RevocationScope` has four members:

| Scope | Lookup key | Rule | Needs |
|---|---|---|---|
| `TOKEN` | `jti` | Denylist — revoked iff a live entry exists for this `jti` | `jti` |
| `SUBJECT` | `f"{iss}\|{sub}"` | Not-valid-before watermark — revoked iff `iat < revoked_at` | `iat` |
| `TENANT` | `tenant_id` | Watermark — a per-tenant kill switch | `iat` |
| `ISSUER` | `iss` | Watermark — a per-issuer kill switch | `iat` |

`SUBJECT`/`TENANT`/`ISSUER` need only the standard `iat` claim, so a global-logout or kill-switch
capability exists even against an IdP that never emits `jti`. A token with **no `iat`** is
treated as revoked by any matching non-`TOKEN` entry — fail-closed on the ambiguous case, because
"issued at an unknown time" cannot be shown to be after the watermark.

`AbstractTokenRevocationStore.is_revoked()` takes all five claim inputs (`jti`, `subject`,
`issuer`, `tenant_id`, `issued_at`) and returns one `RevocationVerdict` in a single round trip —
a Redis backend issues one `MGET` over the four candidate keys; an in-memory one does four dict
lookups. Five separate methods would mean five round trips on every verified token.

### Where the check runs

```
registry.verify(token_str)
  ├─ signature + exp/aud verification (PyJWT)
  ├─ iss enforcement (if enabled)
  ├─ ▸ revocation check (jti / sub / tenant / iss)   ← here, only when a store is bound
  └─ return the typed token
```

The check runs **after** `iss` enforcement, never before — a forged/misrouted token must fail on
its signature/issuer first, or the revocation store becomes an oracle answering questions about
unverified input (a trivial DoS amplifier: every unauthenticated request would otherwise cost a
Redis round trip). `TrustedIssuerRegistry(revocation_store=None)` (the default) means **no check,
no `await`, no cost** — zero-config behaviour is byte-identical to before this feature existed.

### Three in-tree backends, two deferred

| Backend | Ships | Notes |
|---|---|---|
| `NullTokenRevocationStore` | ✅ | The scanned DI default. Never revokes, no I/O |
| `InMemoryTokenRevocationStore` | ✅ | Dev/test/single-process |
| `varco_redis.revocation.RedisTokenRevocationStore` | ✅ | Production. Native `SET ... PX` gives the TTL rule (below) for free |
| `SATokenRevocationStore` / `BeanieTokenRevocationStore` | ❌ deferred | The TTL model is native to Redis and would need a hand-rolled sweep job elsewhere. Un-park trigger: a consumer needs a denylist surviving a cache flush |
| RFC 7662 introspection | ❌ parked | 50-500ms per request and a hard dependency on the authorization server (brief 009 §3) |

### TTL rule

A `TOKEN`-scope entry's `expires_at` should be `token_exp + clock_skew_tolerance_seconds`
(`RevocationEntry.for_token(jti, exp, skew=60)`), so the entry outlives the window a lagging
verifier would still accept the token in (brief 009 §5; OWASP/RFC 7519 recommend 30-60s skew).
Watermark entries (`SUBJECT`/`TENANT`/`ISSUER`) typically have `expires_at=None` — a kill switch
has no natural expiry.

### Failure modes — fail-closed by default

| Mode | Behaviour | Env var |
|---|---|---|
| `FAIL_CLOSED` (default) | A store outage raises `RevocationStoreUnavailableError` → HTTP **503** | — |
| `FAIL_OPEN` | A store outage is logged at `error` and verification proceeds | `VARCO_JWT_REVOCATION_FAILURE_MODE=fail_open` |

Nobody is affected on upgrade — the failure mode only ever applies to an app that explicitly
wired a non-`Null` store. Brief 009 §4 states plainly there is "no industry consensus on a
'right' answer" here; varco picks the mode whose failure is loud and diagnosable, and documents
the alternative (`FAIL_OPEN`, paired with short-lived tokens) as the supported availability-first
posture.

### The two-step wiring — the single most likely footgun

Binding a store in DI does **not** by itself turn checking on:

```python
# Step 1 — bind a store
enable_token_revocation(container)                    # dev: InMemoryTokenRevocationStore
# or: enable_redis_token_revocation(container, url=...) # prod

# Step 2 — REQUIRED — pass it to the registry
store = await container.aget(AbstractTokenRevocationStore)
registry = TrustedIssuerRegistry(revocation_store=store)
```

`varco_core` never reaches for `DIContainer.current()` internally (the same rule that keeps
`TrustedIssuerRegistry.from_env()` from magically constructing a store from an env var — a store
is an object with a connection, not a string). `inspect_revocation_posture()`'s `store_bound` vs.
`registry_wired` fields exist specifically to make this two-step state reportable.

### Error mapping — never an exfiltration surface

`TokenRevokedError.__str__()` is the fixed string `"Token has been revoked."` —
`scope`/`key`/`reason` are attributes for the log only, never interpolated into the message.
`JwtBearerAuth` maps `TokenRevokedError` → 401 with that fixed string, and
`RevocationStoreUnavailableError` → 503 with a fixed detail (never the underlying exception
message, which may carry connection strings or stack traces).

### `varco does not mint `jti` automatically

`JwtAuthority`/`JwtBuilder` are unchanged; `with_random_jti()` already exists and remains
opt-in. `revocation_require_jti` defaults `False` — requiring one by default would break every
Auth0/Keycloak/Cognito deployment the moment a store is wired, punishing the app for doing the
right thing. Automatic minting is on the 4.0 flip list, reported warn-only by Plan 036's
`inspect_revocation_posture()`.

## Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| Pepper mismatch between offline hashing and runtime | Every configured API key 401s | Ensure `VARCO_API_KEY_PEPPER` (or the explicit `pepper=`) is identical everywhere `hash_api_key()`/`ApiKeyAuth` run |
| Store bound in DI but registry not wired | Revocation silently never runs — no error, no log | Pass the DI-resolved store to `TrustedIssuerRegistry(revocation_store=...)` explicitly (§D-S13-di) |
| Redis without persistence | A restart resurrects revoked tokens (including kill switches) until their own `exp` | Enable AOF/RDB persistence on the revocation Redis instance, or accept the bounded exposure window for `TOKEN`-scope entries |
| `FAIL_CLOSED` + a Redis outage | Every request gets a 503 | Either fix the outage, or set `VARCO_JWT_REVOCATION_FAILURE_MODE=fail_open` for an availability-first posture (pair with short-lived tokens) |
| No `jti` from Auth0/Keycloak/Cognito | `TOKEN`-scope revocation silently never matches | Use `SUBJECT`/`TENANT`/`ISSUER` scope instead, or call `JwtBuilder.with_random_jti()` when minting |
| `param="api_key"` restored "for convenience" | The key is back in every access log, proxy log, and `Referer` header | Prefer the `X-API-Key` header; only re-enable the query param for a client that genuinely cannot set headers |
| The api-surface gate (`scripts/api_surface.py --check`) does not see either S1/S2 flip | A narrowed method/`__init__` signature ships without warning from `--check` | Rely on the dedicated `inspect.signature()` regression tests in `varco_core`/`varco_fastapi`'s own test suites instead — they are the real guard here (§D-034-gate) |

## Non-goals

- No DPoP/sender-constrained tokens (RFC 9449) — parked.
- No CAEP/SSF client — parked. The one property the seam preserves so CAEP can later be a
  *provider* against it: `revoke()` is a plain write on the ABC, callable by anything, and nothing
  in the verification path assumes who called it.
- No RFC 7662 introspection backend, no RFC 7009 revocation *endpoint* (varco is a resource
  server, not a token issuer).
- No SQL/Mongo revocation backends in 3.2.
- No `mount_revocation_admin()` — a fourth privileged HTTP surface needs its own design; the ABC
  is directly callable from application code, a CLI verb, or an `EventConsumer`.
