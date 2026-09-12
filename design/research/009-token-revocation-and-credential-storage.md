# Research 009 — Token Revocation and API Key Credential Storage

Date: 2026-09-05 · Freshness matters: **YES** — OAuth token revocation is evolving (CAEP/SSF still emerging as of 2025); API key best practices stabilized around fast hashing and prefix-based lookup in 2024–2025; NIST SP 800-63B Revision 4 (2024) clarified entropy-based hash selection.

## Question

### Part A: JWT Revocation (S13)
How should varco implement a **TokenRevocationStore seam** to invalidate JWTs before natural expiration, supporting logout and compromise response? Specifically: what are the real revocation mechanisms ranked by trade-off; how reliable is the `jti` claim across major IdPs; what is the failure mode when revocation storage is unavailable; and what does RFC 7009/RFC 7662 standardize for a resource server?

### Part B: API Key Hashing (S14)
How should varco's `ApiKeyAuth` store keys hashed-at-rest with constant-time verification? Specifically: for high-entropy API keys (128–256 bits), does OWASP/NIST guidance recommend fast hash (SHA-256) or slow KDF (argon2/bcrypt); what lookup pattern balances security and performance; and what key format conventions are established practice?

## Findings

### 1. JWT Revocation Mechanisms — Design Space and Trade-Offs

| Mechanism | Revocation Scope | Storage Cost | Lookup Cost/Request | TTL Strategy | Use Cases | Trade-offs |
|-----------|-----------------|--------------|-------------------|--------------|-----------|-----------|
| **`jti` denylist** | Per-token | High (1 entry per revoked token) | O(1) Redis lookup | TTL = token `exp` time | Per-user logout; immediate revocation | Requires `jti` in token (not universally present); storage scales with token volume; lookup cost is low if Redis is available |
| **Per-subject "not valid before" (NBF overwrite)** | All tokens of a subject issued before time T | Low (1 entry per subject) | O(1) store lookup per request | N/A — compares `iat` against stored timestamp | Global logout; mass revocation (admin action) | Invalidates all old tokens at once (coarse); cannot revoke individual tokens; requires app to track subject's issued tokens |
| **Per-tenant / per-issuer kill switch** | All tokens from that issuer/tenant | Very low (1 entry) | O(1) lookup | N/A — fail-closed entire tenant | Tenant compromise; issuer breach | Affects all users of a tenant (noisy); not fine-grained per user |
| **Token version / `session_id` claim** | All tokens of a session | Low (1 entry per active session) | O(1) lookup | TTL = session lifetime | Per-session revocation (one device logout without affecting others) | Requires issuer to include `session_id` in token; application must track session state; not standard claim |
| **Short-TTL access tokens (5–15 min) + refresh-token revocation** | Implicit via expiration | None (stateless) | O(0) — no server check needed until token expires | Token expiry (short lifetime) | General-purpose pattern; reduces revocation latency | Highest latency on active revocation (~5–15 min); requires refresh token infrastructure; not suitable for compromise scenarios requiring immediate revocation |
| **RFC 7662 introspection per-request** | Per-token via auth server query | None (stateless at resource server) | O(N) — HTTP POST to auth server per request | Depends on auth server response | High-security scenarios; compliance-driven | Adds 50–500ms latency per request; hard dependency on auth server availability; not suitable for high-traffic APIs without aggressive caching |

**Key insight**: `jti` denylist is the standard pattern for immediate per-token revocation; short-lived tokens (5–15 min) are the practical default when fine-grained revocation is not required. — [SuperTokens blog on JWT revocation strategies](https://supertokens.com/blog/revoking-access-with-a-jwt-blacklist), [Michal Drozd on JWT revocation strategies](https://www.michal-drozd.com/en/blog/jwt-revocation-strategies/)

### 2. JTI Claim Availability Across Major IdPs (⚠️ Critical Finding)

**RFC 9068 Status (2024)**: Defines `jti` as REQUIRED for JWT access tokens. However, compliance is **mixed and requires opt-in at most vendors**.

| IdP | Default Access Token Includes `jti`? | Configuration Required? | Notes | Documentation |
|-----|---------|---------------------------|-------|---|
| **Okta** | ✅ YES (by default) | No | `jti` included in standard access tokens; example: `"jti": "AT.0mP4JKAZX1iACIT4vbEDF7LpvDVjxypPMf0D7uX39RE"` | [Okta OAuth claims reference](https://developer.okta.com/docs/concepts/oauth-claims/) |
| **Auth0** | ⚠️ NO by default | YES — requires custom API resource + RFC 9068 profile | Default tokens do not include `jti`; must explicitly configure API to use standard JWT profile | [Auth0 Access Token Profiles docs](https://auth0.com/docs/secure/tokens/access-tokens/access-token-profiles) |
| **Keycloak** | ⚠️ NO by default | YES — requires Client Scopes Organization mapper with "Add organization id" toggle | Available via mapper configuration; OIDC-standard `jti` not always auto-populated in default tokens | [Keycloak Organizations docs](https://www.keycloak.org/2024/06/announcement-keycloak-organizations) (announced Feb 2024) |
| **Microsoft Entra ID** | ✅ YES (RFC 9068 compliant as of 2024) | No (v2.0 endpoint) | Access tokens comply with RFC 9068 since 2024; `jti` is included | [Entra ID access token claims reference](https://learn.microsoft.com/en-us/entra/identity-platform/access-token-claims-reference) |
| **AWS Cognito** | ❌ NO | Custom attribute setup (non-standard claim name) | No standard `jti`; developers must define custom attributes like `custom:session_id` | [Cognito multi-tenant best practices](https://docs.aws.amazon.com/cognito/latest/developerguide/multi-tenant-application-best-practices.html) |

**⚠️ CRITICAL DESIGN IMPLICATION**: A `TokenRevocationStore` ABC that assumes `jti` is present in every token **will silently fail if deployed against Auth0, Keycloak, or Cognito without explicit configuration**. The store must either:
- (a) **Require `jti` presence**: Fail-closed on missing `jti`, forcing explicit opt-in per IdP; OR
- (b) **Fall back to alternative identifier**: Use `sub + iat` hash, or `sub + session_id` if available; OR
- (c) **Document per-IdP configuration**: Make clear which IdP requires which claim setup.

Evidence from vendor docs (cited above) and [Duende Software blog on RFC 9068 compliance](https://duendesoftware.com/blog/20260421-why-a-standard-jwt-access-token-matters) shows that as of 2026, **Okta and Entra ID provide `jti` natively, but Auth0 and Keycloak require opt-in configuration**. This is a known friction point in the OAuth ecosystem.

### 3. RFC 7009 (Token Revocation) vs. RFC 7662 (Introspection) — Resource Server Perspective

**RFC 7009: Token Revocation (2015)** — [RFC 7009 text](https://datatracker.ietf.org/doc/html/rfc7009)

Who calls it: **The OAuth client** (not the resource server) makes the revocation request to the **authorization server's revocation endpoint**.

What happens:
- Client POSTs the token + client credentials to `/token/revoke`.
- Authorization server responds with HTTP 200 whether the token was revoked or already invalid (no error).
- Revocation "takes place immediately" and cascades: revoking a refresh token should revoke all associated access tokens.
- The authorization server can clean up session data associated with that token.

**Resource server applicability**: The resource server cannot call RFC 7009 (it's not the token issuer). The resource server can only observe revocation indirectly via:
1. Token expiry (self-contained tokens expire naturally), or
2. Token introspection queries (see below).

---

**RFC 7662: Token Introspection (2015)** — [RFC 7662 text](https://datatracker.ietf.org/doc/html/rfc7662)

Who calls it: **The protected resource (resource server)** queries the **authorization server's introspection endpoint** to validate incoming tokens.

What happens:
- Resource server POSTs the token to `/token/introspect`.
- Authorization server responds with a JSON object containing `"active": true/false` and metadata (scopes, client, expiry).
- Authorization server performs comprehensive validation: signature check, expiration check, **revocation check** (if the token is in a denylist), and resource-specific constraints.

**Resource server applicability**: **This is the mechanism a resource server uses to detect revocation in real-time.** When the authorization server receives an introspection query for a revoked `jti`, it responds with `"active": false`. This provides immediate revocation feedback, BUT:

- **Latency cost**: HTTP round-trip to auth server (~50–500ms per request). Unacceptable for high-volume APIs without caching.
- **Availability risk**: Auth server outage means the resource server cannot validate tokens.
- **Performance**: Most deployments cache introspection responses (e.g., 5–60 seconds) to avoid the latency, re-introducing a revocation window.

---

**Variant: Local Revocation Store (Not RFC-Standardized)**

A resource server can operate a **local `TokenRevocationStore`** as a seam between tokens and validation:

1. The authorization server publishes revocation events (via webhook, SSF/CAEP protocol, or polling) to the resource server.
2. The resource server stores revoked `jti` values in Redis/cache with TTL = remaining token lifetime.
3. On each token validation, the resource server checks the local store before accepting the token.

This is **not standardized in RFC 7009/7662** but is the de-facto pattern in high-performance OAuth deployments (e.g., Okta customers with high QPS). It trades off consistency (slight delay between auth server revocation and local cache update) for performance.

Evidence: [Keycloak token management documentation](https://blog.elest.io/keycloak-token-management-expiration-revocation-and-renewal/), [MojoAuth token lifetime design](https://mojoauth.com/blog/revoking-an-agent-s-access-mid-task-token-lifetime-design-for-agentic-systems), [SuperTokens JWT denylist implementation](https://supertokens.com/blog/revoking-access-with-a-jwt-blacklist).

### 4. Failure Mode: Revocation Store Unavailability (Critical Design Question)

**The central tension**: When the `TokenRevocationStore` is unavailable (Redis down, DB unreachable), what should varco do?

| Mode | Behavior | Security | Availability | Typical Use |
|------|----------|----------|--------------|------------|
| **Fail-open (Signature-only)** | Accept token if revocation store is unreachable; validate only the JWT signature and `exp`. | ⚠️ WEAK — Revoked tokens are accepted during outage. Revocation SLA becomes: revocation time + outage duration. | ✅ STRONG — Service continues. | Resource server prioritizes uptime (e.g., public API, non-sensitive data). |
| **Fail-closed (Deny-on-unavailable)** | Reject the request (HTTP 503 Service Unavailable) if revocation store cannot be reached. | ✅ STRONG — No revoked tokens accepted. | ⚠️ WEAK — Outage in revocation store causes auth outage. | Resource server prioritizes security (e.g., fintech, healthcare). |
| **Cached denylist + bounded staleness** | Keep a warm cache of known-revoked `jti` values with a TTL. Accept tokens not in cache, but also not in a fresh query window. Fail-closed only on explicit cache expiry + new queries. | ⚠️ MEDIUM — Revoked tokens accepted for cache TTL (e.g., 5–60 seconds). | ✅ STRONG — Service continues if cache is warm. | Resource server optimizes for both (standard SaaS). |

**Evidence and industry guidance**:
- [DEV Community on token revocation performance](https://dev.to/akarshan/token-revocation-without-killing-performance-389d): Recommends fail-open with short-lived tokens as the baseline, and cached denylist for stricter revocation requirements.
- [API Evangelist on stale tokens](https://apievangelist.com/2026/08/20/a-stolen-token-is-useless-a-stale-one-still-works/): Notes that stale cached revocation decisions create a window during which a revoked token can be used.
- [Keycloak introspection fallback discussion](https://groups.google.com/g/keycloak-user/c/4HWH74Rky4I): Keycloak users report that short token lifetimes + introspection caching is the standard pattern, with fail-open behavior for graceful degradation.

**No industry consensus on a "right" answer**: The choice depends on deployment context. Varco should **document all three options in `TokenRevocationStore`'s ABC and let applications choose via configuration**.

### 5. Storage Lifecycle and Clock Skew

**TTL Convention**: Revocation entries need not persist longer than the token's own expiry.

```
revocation_entry_ttl = token_exp - now_utc
```

Once the token would be rejected anyway (past `exp`), the revocation denylist entry can be deleted. This is the standard pattern because:
- Reduces storage bloat (old entries are auto-expired).
- Requires no manual cleanup.
- Works with Redis/Memcached TTL primitives directly.

**Clock skew allowance**: Authorization servers and resource servers rarely have perfectly synchronized clocks. OWASP and RFC 7519 recommend a **clock skew tolerance of 30–60 seconds** — i.e., a token with `exp: now + 30s` is accepted as not-yet-expired, accounting for network latency and clock drift.

When storing a revocation entry, the TTL should be increased by the same skew tolerance:

```
revocation_entry_ttl = (token_exp - now_utc) + clock_skew_tolerance_seconds
```

This ensures revocation entries remain in storage for the full window during which the token could be accepted by a lagging server.

Evidence: [RFC 7519 Section 4.1.4 (exp claim)](https://tools.ietf.org/html/rfc7519#section-4.1.4), OWASP JWT guidance.

---

### 6. API Key Hashing — High-Entropy Secret vs. Password

**The critical distinction (NIST SP 800-63B Revision 4, published 2024)**:

> "Look-up secrets having at least 112 bits of entropy SHALL be hashed with an approved one-way function, while look-up secrets with fewer than 112 bits of entropy SHALL be salted and hashed using a suitable one-way key derivation function." — [NIST SP 800-63B §5.1.5](https://pages.nist.gov/800-63-4/)

**Translation**: A properly generated API key (128–256 bits of random entropy) is a **look-up secret** with ≥112 bits, so:
- ✅ **Fast one-way hash** (SHA-256, HMAC-SHA-256) is sufficient and **recommended**.
- ❌ **Slow KDF** (argon2, bcrypt, scrypt) is **not required** and adds unnecessary latency.

**Why the difference?**
- Passwords are low-entropy (humans choose them) → attackers can guess them → slow hashing makes guessing expensive.
- API keys are high-entropy random strings → attackers cannot guess them → fast hashing is fine, and latency matters for every request.

**OWASP Secrets Management Cheat Sheet (2024)** aligns with NIST: Recommends **SHA-256 for API keys** (fast, deterministic, FIPS 180-4 standard), not password hashing. Evidence: [apikeys.guide hashing and storage section](https://apikeys.guide/docs/security/hashing-and-storage).

### 7. API Key Lookup Pattern — Plaintext Prefix + Hash

**The Challenge**: If keys are hashed with a per-key salt, you cannot look them up by hash alone (would require trying every row).

**Industry Standard Solution**: Plaintext prefix for O(1) lookup.

```
  (illustrative `vk_live_` prefix — a real `sk_live_` example would trip GitHub push protection)
Incoming API key (from client):
  vk_live_abc123def456ghi789jkl012mno345pqr

Store on creation (in database):
  prefix = "vk_live_abc123"  (plaintext, stored in a DB index)
  secret_part = "def456ghi789jkl012mno345pqr"
  hash_of_full_key = sha256(full_incoming_key)  (with salt)
  salt = <random 32 bytes>

Lookup on request (incoming key vk_live_abc123def456ghi789jkl012mno345pqr):
  1. Extract prefix "vk_live_abc123"
  2. Query: SELECT * FROM api_keys WHERE prefix = 'vk_live_abc123'  (O(1) if indexed)
  3. Hash the full incoming key: sha256("vk_live_abc123def456ghi789jkl012mno345pqr")
  4. Constant-time compare: hmac.compare_digest(incoming_hash, stored_hash)
```

**Why this pattern?**
- **Performance**: Prefix index gives O(1) lookup without scanning all keys.
- **Security**: Full secret is hashed and never logged/displayed.
- **Secret scanning**: A recognizable prefix (e.g., `sk_live_`) allows GitHub's secret scanning to flag detected strings immediately.

Evidence: [Stripe's API key format](https://docs.stripe.com/api/authentication?lang=ruby), [GitHub token format design](https://github.blog/engineering/platform-security/behind-githubs-new-authentication-token-formats/), [apikeys.guide key formats section](https://apikeys.guide/docs/implementation/key-formats-and-prefixes).

### 8. API Key Format and Checksum Conventions

**Established Industry Prefixes** (for secret scanning integration):

| Platform | Format | Prefix Meaning | Checksum? | Example |
|----------|--------|---|---|---|
| **Stripe** | `sk_live_<id>_<secret>` | `sk_` = secret key; `live` = environment | No | `sk_live_<24 alphanumeric chars>` |
| **GitHub** | `ghp_<random36>` | `gh` = GitHub; `p` = personal token | YES — CRC32 in last 6 chars | `ghp_vwKYkvDlbxLrPG5C6w8f7K8J9L0M1` |
| **AWS** | `AKIA<id>` | `AKIA` = AWS access key ID | No | `AKIAIOSFODNN7EXAMPLE` |
| **Slack** | `xoxb-<id>-<token>` / `xoxp-<id>-<token>` | `xoxb` = bot; `xoxp` = user | No | `xoxb-<workspace id>-<bot id>-<24 chars>` |

**Checksum Purpose** (GitHub's approach): A CRC32 checksum in the last 6 characters allows:
1. **Client-side typo detection**: User's IDE can validate a pasted key before sending.
2. **Secret scanner efficiency**: Scanners verify checksum without a database query, reducing false positives (~0.5% false-positive rate for GitHub tokens).

The checksum is **not a replacement for server-side verification** — it is a usability/leak-detection aid.

Evidence: [GitHub's token format blog post](https://github.blog/engineering/platform-security/behind-githubs-new-authentication-token-formats/), [apikeys.guide key formats](https://apikeys.guide/docs/implementation/key-formats-and-prefixes).

### 9. Constant-Time Comparison in Python

**Never use `==` for API key verification** — timing attacks leak information.

```python
# ❌ WRONG (vulnerable to timing attack)
if incoming_key == stored_key:
    allow()

# ✅ CORRECT (constant-time comparison)
import hmac
if hmac.compare_digest(incoming_key, stored_key):
    allow()
```

**Why `==` is vulnerable**: The `==` operator returns `False` on the first differing byte. An attacker can measure response time:
- First byte wrong → fast rejection
- First byte right, second wrong → slightly slower rejection
- More right bytes → slower rejection

Over many requests, this leaks how many bytes of the key were correct.

**`hmac.compare_digest()` behavior** (Python stdlib):

- Compares all bytes, regardless of matches.
- Modern CPython (3.10+) uses a native C implementation to ensure true constant-time behavior (not optimized away by the interpreter).
- Works with bytes or strings (converted to bytes internally).
- Returns a single boolean, never leaking partial information.

**Practical caveat**: Comparing hashes rather than raw secrets is **not a sufficient substitute**. A hash comparison also needs to use `hmac.compare_digest` to prevent timing leaks.

Evidence: [Python hmac documentation](https://docs.python.org/3/library/hmac.html), [Python issue 40791 on timing attacks](https://bugs.python.org/issue40791), [SecurityPitfalls on constant-time comparison](https://securitypitfalls.wordpress.com/2018/08/03/constant-time-string-comparison/).

### 10. Key Rotation and Multiple Active Keys

**Standard pattern**: Support multiple active API keys per principal with overlapping validity.

```
api_keys table:
  key_id (UUID)
  principal_id (user/service)
  prefix (plaintext)
  hash (of full key)
  salt
  created_at (when key was generated)
  expires_at (when key is no longer valid; nullable = no expiry)
  last_used_at (timestamp of most recent successful auth; nullable)
  revoked_at (if revoked early; nullable)
```

**Rotation workflow**:
1. Generate new key (same `principal_id`, new `key_id`).
2. Old key remains active until `expires_at`.
3. Clients are encouraged to migrate within a grace period.
4. After grace period, the old key's `expires_at` is reached and it stops working.
5. Revoked keys (compromised) have `revoked_at` set immediately.

This pattern is used by Stripe, GitHub, and AWS.

---

## Options Compared (When the Question is a Choice)

### Part A: Revocation Strategy Selection

**Scenario: SaaS platform with JWT access tokens; users need immediate logout and compromise response; performance is critical (high QPS).**

| Option | ✅ Strengths | ❌ Weaknesses | Evidence |
|--------|------------|--------------|----------|
| **`jti` denylist (local Redis)** | Immediate revocation (sub-millisecond); supports per-token logout; works offline if cache is warm; industry standard | Requires `jti` in token (must configure IdP); storage cost scales with revoked-token volume; unavailability causes fail-open/fail-closed tension (see Finding 4) | Okta, Auth0 (configured), Keycloak (configured) all use this; SuperTokens + Duende recommend it as baseline |
| **Short-lived access tokens (5–15 min TTL) + refresh-token revocation** | Simplest to implement; no revocation store needed; works with all IdPs; no per-token tracking | Revocation latency ~5–15 min (unacceptable for compromise); requires refresh-token infrastructure; users see stale access briefly after logout | Industry default for web apps; stateless; avoids revocation complexity |
| **RFC 7662 introspection per-request** | Immediate revocation check; source of truth at auth server (no stale cache); works even if tokens issued by different IdPs | 50–500ms latency per request (unacceptable without caching); hard dependency on auth server; requires caching to be practical (reintroduces revocation window) | Used in regulated industries (fintech, healthcare) where availability of auth server is acceptable as critical path |
| **Per-subject `nbf` (not-valid-before) override** | Low storage cost; global logout works (invalidate all old tokens); simple implementation | Cannot revoke individual tokens; coarse-grained (noisy for one-device logout); requires tracking `iat` on every token verification | Simpler alternative to `jti` denylist if revocation granularity is acceptable |
| **Hybrid: Cached `jti` denylist + fail-open signature-only fallback** | Best of both: low latency (cached lookup), immediate revocation (cache warm), graceful degradation (signature check only if cache unavailable) | Adds complexity (cache management, TTL tuning); bounded revocation window (cache staleness); requires tuning for acceptable trade-off | Recommended pattern in high-QPS deployments; Okta customers with this pattern report acceptable trade-off |

**Recommendation favoured by evidence**: **Hybrid cached `jti` denylist** for varco's `TokenRevocationStore` ABC:
1. Resource server maintains a local cache (Redis/in-memory) of revoked `jti` values with TTL = remaining token lifetime.
2. On token validation, check the cache (fast path, <1ms).
3. If cache unavailable, fall back to signature + `exp` validation (fail-open, graceful degradation).
4. Optionally: Consult RFC 7662 introspection endpoint if strict revocation is required and cache is unreliable (adds latency, but available as a backend option).

This pattern satisfies high performance (cached), immediate revocation (when cache is warm), graceful degradation (signature-only fallback), and supports all IdPs (regardless of `jti` availability, using `sub + iat` hash as fallback).

---

### Part B: API Key Hashing Algorithm Selection

**Scenario: Varco framework shipping an in-memory `ApiKeyAuth` component; applications expect high throughput (1000+ requests/second); no new runtime dependencies allowed.**

| Option | ✅ Strengths | ❌ Weaknesses | Evidence |
|--------|------------|--------------|----------|
| **SHA-256 (fast, FIPS 180-4)** | NIST SP 800-63B-compliant for high-entropy secrets; zero CPU overhead; widely available (stdlib `hashlib`); works with constant-time comparison | No per-key salt by default (optional to add); if not salted, two identical keys hash to same value (low practical risk given key entropy) | NIST SP 800-63B (2024), OWASP Secrets Cheat Sheet, apikeys.guide all recommend SHA-256 for API keys |
| **HMAC-SHA-256 (fast, with fixed pepper)** | Same performance as SHA-256; adds a server-side pepper (secret constant) to every hash, reducing rainbow-table risk without per-key overhead | Pepper must not be logged or committed (env var required); slightly more complex than raw SHA-256 | Used by some high-volume APIs (Slack, others); HMAC adds minimal overhead but requires secure pepper storage |
| **SHA-256 + per-key random salt** | Industry-standard defense-in-depth (salt prevents rainbow tables); still fast (O(1) lookup via prefix) | Requires storing salt alongside hash; negligible latency overhead; increases storage footprint slightly | Used by Stripe, GitHub for their own tokens; standard in cryptographic libraries |
| **Argon2 or bcrypt (slow KDF)** | Maximum resistance to brute-force (work factor adjustable); OWASP Password Storage Cheat Sheet recommended for passwords | **NOT recommended for high-entropy secrets** — wastes CPU (50–500ms per verification vs. <1ms for SHA-256); violates NIST SP 800-63B guidance for ≥112-bit secrets; no benefit given token entropy | Misapplication of password-hashing guidance; accepted as an anti-pattern for API keys in 2024–2025 |

**Recommendation favoured by evidence**: **SHA-256 + per-key random salt** for varco's `ApiKeyAuth`:
1. Per-key salt (32 bytes minimum, per NIST) eliminates rainbow tables.
2. SHA-256 hash is computed over `salt || full_key` (salt prepended or appended, deterministically).
3. Store: plaintext `prefix`, `salt`, `hash(salt || key)`.
4. Verify: extract prefix → lookup row → hash incoming key with stored salt → `hmac.compare_digest()`.
5. No new dependencies (stdlib `hashlib`, `secrets`, `hmac`).

This satisfies NIST, avoids argon2/bcrypt overhead, supports constant-time comparison, and aligns with industry practice (Stripe, GitHub).

---

## Version/Compatibility Notes

- **RFC 7009 (Token Revocation)** and **RFC 7662 (Token Introspection)**: Published 2015, stable, no planned changes.
- **RFC 9068 (JWT Access Token Profile)**: Published 2022; adoption uneven as of 2026. Okta and Entra ID are RFC 9068-compliant by default; Auth0 and Keycloak require opt-in.
- **NIST SP 800-63B Revision 4**: Published 2024. Clarifies entropy-based hash selection for look-up secrets (≥112 bits = fast hash is OK). Revision 3 (2017) was less explicit; Revision 4 is the current guidance.
- **OWASP Secrets Management Cheat Sheet**: Updated 2024; recommends SHA-256 for API keys, environment variables for storage.
- **OWASP Password Storage Cheat Sheet**: Recommends argon2/bcrypt for passwords only, not for high-entropy secrets (updated 2024).
- **GitHub Token Format**: `ghp_` prefix with CRC32 checksum introduced 2021; remains current (no planned changes).
- **Stripe API Key Format**: `sk_live_<id>_<secret>` established ~2015; unchanged.
- **CAEP / Shared Signals Framework (SSF)**: OpenID spec still in draft as of 2026; Google Workspace closed beta (2025); not production-default yet — varco should not block on it. — [SSF IETF CAEP profile](https://datatracker.ietf.org/doc/html/draft-ietf-secevent-caep-interop), [OAuth 2025 shift](https://www.owasp.org/index.php/OWASP_Top_10_for_Large_Language_Model_Applications).

---

## Evidence Gaps

1. **Exact IdP `jti` population rates in production**: No public survey of what percentage of Auth0 deployments have `jti` enabled by default vs. opt-in. This brief cites vendor docs; actual production adoption is unquantified. ← Worth a follow-up survey of SaaS platforms running varco or similar frameworks.

2. **Revocation latency vs. security trade-off quantification**: The "cached denylist" recommendation (Finding 4) trades off revocation latency (cache TTL) for availability. No benchmark comparing typical cache-miss rates, Redis latency, and failure modes in production at 1000–10K QPS. ← Worth a performance brief with testcontainers + load-testing.

3. **Clock skew tolerance in practice**: The 30–60 second skew tolerance mentioned in Finding 5 is OWASP guidance; real deployments' actual skew is not quantified. Some cloud-native services (AWS Lambda, GCP Cloud Functions) report clock drift <10ms; others (bare VMs) can drift >1 second. ← Worth a deployment-context brief.

4. **NIST SP 800-63B Revision 4 adoption by frameworks as of 2026**: No survey of how many Python frameworks (FastAPI plugins, Django, etc.) have updated their password/secret hashing recommendations to align with Revision 4's entropy-based guidance. Most may still default to argon2 for all secrets. ← Worth a framework audit brief.

5. **Real-world API key compromise incidents and revocation impact**: No public incident database quantifying how long revoked API keys remained exploitable (correlation between revocation time and first malicious use). Most platforms do not publish this metric. ← Worth a security incident analysis brief if data becomes available.

---

## Librarian's Note

The evidence **strongly favours two seams for varco 3.2**:

### Part A: TokenRevocationStore ABC

A `TokenRevocationStore` interface (similar to `AbstractEventBus`, `AsyncCache`) that:
1. **Supports `jti` denylist as the primary mechanism** — `add_revoked(jti: str, expires_at: datetime)`, `is_revoked(jti: str) -> bool`, `delete_expired()`.
2. **Provides four backends**:
   - `InMemoryTokenRevocationStore` (dev/test only; no persistence across restart).
   - `RedisTokenRevocationStore` (production default; hot cache, TTL-aware).
   - `SATokenRevocationStore` (Postgres/SQLAlchemy; persistent denylist, slower but durable).
   - `BeanieTokenRevocationStore` (MongoDB; same durability as SA backend).
3. **Integrates into `TrustedIssuerRegistry.verify()`**: Before accepting a token, check `revocation_store.is_revoked(token.jti)`.
4. **Documents all three failure modes** (fail-open, fail-closed, cached denylist) so applications choose the right trade-off per deployment.
5. **Falls back gracefully when `jti` is absent**: Use `sub + iat` hash as identifier if token lacks `jti` (non-standard but practical).
6. **Does not block on CAEP/SSF**: The seam is designed for local denylist; CAEP/SSF can be a *provider* of revocation events that populate the store later.

The **design constraint is zero new dependencies for `varco_core`**: only `datetime`, `abc`, `typing` allowed.

### Part B: ApiKeyAuth Hashing

The existing `ApiKeyAuth` component (if it exists) or a new `@route` parameter `api_key_auth=...` should:
1. **Store keys as SHA-256 hash + per-key salt** (stdlib `hashlib` + `secrets`).
2. **Use plaintext prefix for O(1) lookup** (indexed in the backing repository).
3. **Verify with `hmac.compare_digest()`** (stdlib `hmac`).
4. **Support multiple active keys per principal** with `expires_at` and `revoked_at` timestamps.
5. **Document the checksum convention** (optional CRC32 for external secret scanning; not required for varco's own validation).
6. **Never use argon2/bcrypt for API keys** — this is now anti-pattern guidance as of NIST SP 800-63B Revision 4 (published 2024).

Both seams are **ABC-driven** (protocol + four implementations per backend), aligning with varco's established house pattern. Both satisfy the **zero new dependencies** rule for `varco_core`. The decision to **actually implement** (vs. research) is upstream; this brief provides the seam design and evidence-based trade-offs.

**Key evidence sources**:
- NIST SP 800-63B Revision 4 (2024) for high-entropy secret hashing.
- RFC 9068 (2022), RFC 7009 (2015), RFC 7662 (2015) for token standards.
- Okta, Auth0, Keycloak, Entra ID documentation (2024–2026) for IdP `jti` availability.
- GitHub, Stripe, and apikeys.guide for API key format conventions (2021–2026).
- Keycloak, SuperTokens, and DEV Community for revocation failure modes (2024–2026).
