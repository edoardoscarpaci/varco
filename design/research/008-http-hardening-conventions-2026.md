# Research 008 — HTTP Hardening Conventions for Framework Middleware (S7/S8/S10)

Date: 2026-09-05 · Freshness matters: **YES** — Security headers standards (W3C/OWASP/IETF) are active; RateLimit headers are in IETF draft (expires Nov 2026); framework defaults change per release.

## Question

What are the current recommended conventions for three HTTP hardening middleware layers in a JSON API framework (2026), specifically: (1) security response headers (S7), (2) request body size/complexity limits (S8), and (3) rate-limit headers and response codes (S10)? What are the recommended defaults for each, which headers apply to JSON APIs vs. HTML-rendering apps, and what are the actual defaults in comparable frameworks?

## Findings

### 1. Security Response Headers (S7) — Current Recommendations and Applicability to JSON APIs

**Core Principle:** A JSON API does not render HTML and does not fetch external resources, so a significant subset of browser security headers is redundant—but remains valuable as defense-in-depth to prevent misuse if the response is accidentally loaded as a document.

**OWASP Secure Headers Project Recommendations (2026):**

The OWASP Secure Headers Project maintains a current (2026) baseline recommended for production HTTPS sites:

| Header | Recommended Value for JSON API | Purpose | Browser-Only? |
|--------|------|---------|---------------|
| **Strict-Transport-Security** | `max-age=31536000; includeSubDomains` (1 year) | Force HTTPS, prevent downgrade attacks | ✅ Yes (browsers) |
| **Content-Security-Policy** | `default-src 'none'; frame-ancestors 'none'` (most restrictive for JSON APIs) | Block all resource loading; prevent XSS/injection misuse if accidentally rendered | ✅ Yes (browsers) |
| **X-Content-Type-Options** | `nosniff` | Prevent MIME-sniffing; block treating responses as different media types | ✅ Yes (browsers) |
| **X-Frame-Options** | `DENY` or `SAMEORIGIN` | Prevent clickjacking / embedding in frames | ✅ Yes (browsers) |
| **Referrer-Policy** | `strict-origin-when-cross-origin` | Control leaked referrer information in navigation | ✅ Yes (browsers) |
| **Permissions-Policy** | `geolocation=(), microphone=(), camera=()` | Disable unused browser APIs | ✅ Yes (browsers) |
| **Cross-Origin-Resource-Policy** | `same-origin` | Control cross-origin resource access | ✅ Yes (browsers) |
| **Cross-Origin-Opener-Policy** | `same-origin` | Isolate browsing context groups | ✅ Yes (browsers) |

**CSP for JSON APIs — Detailed Guidance:**

According to MDN Web Docs, `default-src 'none'` serves as a fallback for all fetch directives and, when set to `'none'`, blocks all resource types (images, scripts, styles, fonts, frames, workers). For a JSON API:
- **Most restrictive (recommended):** `Content-Security-Policy: default-src 'none'; frame-ancestors 'none'`
- **Why this works:** JSON endpoints don't render HTML, don't load external resources, and shouldn't execute scripts. Even if an attacker somehow causes the response to be parsed as a document, CSP blocks everything.

**Important caveat for FastAPI apps:** FastAPI ships automatic `/docs` (Swagger UI) and `/redoc` endpoints by default, which *do* load scripts and stylesheets from a CDN (jsDelivr, etc.). A framework-level CSP of `default-src 'none'` will **break** these documentation routes. Solutions:
- (a) Ship with a permissive CSP by default (`default-src 'self' https: 'unsafe-inline'`—not ideal, but common practice in Node.js Helmet defaults)
- (b) Override CSP per-route: strict for `/api/*` endpoints, permissive for `/docs` and `/redoc`
- (c) Ship CSP disabled by default (opt-in via settings)

**HSTS Considerations:**
- Recommended `max-age`: **31536000 seconds (1 year)** per OWASP.
- **Critical operational rule:** HSTS must be sent **over HTTPS only**; browsers ignore the header if sent over plaintext HTTP (prevents MITM from adding it).
- **Development/localhost caveat:** Browsers treat `localhost` as a special secure origin exempt from HSTS, so development over `http://localhost` is safe. **However, custom local domains (e.g., `myapp.local` via `/etc/hosts) ARE subject to HSTS**, so be careful not to lock yourself out.
- **Proxy deployments:** If the app is always behind a TLS-terminating proxy/ingress, the framework must only send HSTS when `X-Forwarded-Proto: https` is present (and the proxy is trusted). A frame that sends HSTS over plaintext HTTP would break development and non-HTTPS internal deployments.

**Deprecated/Harmful Headers to Explicitly NOT Send:**
- **`X-XSS-Protection`**: Deprecated. Helmet.js (Node.js) deliberately sets it to `0` to disable the browser's buggy XSS filter, which can worsen XSS attacks. — [Helmet.js FAQ](https://helmet.js.org/faq/you-might-not-need-helmet)
- **`X-Powered-By`**: Information disclosure; serves no security purpose; commonly removed.
- **`Server`**: Information disclosure; consider sending a generic value or none.

**Comparison of Framework Defaults (2026):**

| Framework | Default Preset | HSTS Enabled? | CSP Default | Customizable? |
|-----------|---|---|---|---|
| **secure.py (Python)** | BALANCED (8 headers) | Yes, 1 year | `default-src 'self'` (permissive) | ✅ Yes, granular |
| **Helmet.js (Node)** | Default (13 headers) | Yes, no age (browsers decide) | `default-src 'self'` (permissive) | ✅ Yes per-header |
| **Django SecurityMiddleware** | Mostly disabled | ⚠️ No (requires `SECURE_HSTS_SECONDS`) | Not included (use django-csp) | ✅ Via settings |
| **varco (current)** | None shipped | ❌ None | None | N/A |

**secure.py's BALANCED preset (the reference Python library):**
- Sends 8 headers by default: HSTS, X-Frame-Options, X-Content-Type-Options, Referrer-Policy, CSP (permissive), COOP, CORP, Permissions-Policy.
- Avoids unsafe script execution but allows `'self'` for app's own resources.
- Documented as "a reasonable starting point, not a substitute for application-specific review."

### 2. Request Body Size and Complexity Limits (S8)

**Current State of the Ecosystem:**

- **RFC 9110 (HTTP Semantics, 2022):** Defines HTTP body semantics but explicitly **does not mandate a maximum body size**—it is left to implementations. Servers MAY respond with `413 Content Too Large` (renamed from "Payload Too Large" in prior RFCs) if a request exceeds their willingness to process.
- **Uvicorn/Hypercorn defaults:** Neither enforces a global HTTP request body size limit by default. WebSocket message size is limited (`--ws-max-size` defaults to 16777216 bytes / 16 MB in Uvicorn), but HTTP request bodies have no framework-level limit. This is a significant DoS vector—any Starlette/FastAPI app is vulnerable to memory exhaustion without explicit middleware.
- **Starlette/FastAPI:** No built-in request body size limiting middleware shipped by default. Third-party ASGI middleware (e.g., `content-size-limit-asgi`) is recommended; several GitHub issues have requested this for years.

**Correct Implementation Approach:**

Body-size limiting **must be done in ASGI middleware**, not at the HTTP-parsing layer, because the goal is to reject *before* buffering. The correct technique:
1. Intercept the `receive()` method of the ASGI message stream.
2. Track cumulative bytes received via `http.request` messages (not trusting `Content-Length` alone, which can be spoofed or omitted under chunked transfer-encoding).
3. Raise an exception immediately when the limit is exceeded, **before** buffering completes.

**Recommended Limit Values:**

No IETF or OWASP standard specifies exact limits. Common production deployments:
- **Typical JSON API:** 1–10 MB (most endpoints don't need more).
- **File upload endpoints:** 100 MB–1 GB (depends on use case).
- **Upstream server defaults (reference):**
  - nginx: `client_max_body_size` defaults to 1 MB.
  - Apache: `LimitRequestBody` defaults to 0 (unlimited).
  - AWS API Gateway: 10 MB hard limit.
  - Google Cloud Endpoints: 32 MB default.

**Status Code and Response Handling:**

- **RFC 9110 §15.4.14** and **RFC 6585 §4** define `413 Content Too Large` (HTTP 413) as the response when a server refuses to process a request because the request entity exceeds limits.
- **Response behavior:**
  - If `Content-Length` exceeds the limit before the connection is opened: reject with 413 and optionally `Retry-After` header.
  - If the body stream exceeds the limit mid-flight (chunked encoding): close the stream with 413.
- Avoid 400 for this case; 413 is semantically correct.

**JSON Nesting and Complexity Limits:**

The ecosystem does not have standardized limits for JSON nesting depth or array/object size, unlike some higher-level frameworks. Recent evidence (CVE-2026-0994):
- A protobuf JSON parser had a recursion-depth bypass where deeply nested `Any` structures could exhaust Python's stack limit, causing `RecursionError`.
- **Lesson:** Complex JSON parsing (especially with nested schemas) is a DoS vector. Mitigation is application-specific; no framework middleware can safely enforce "max nesting depth" without understanding the schema.
- **Practical guidance:** Rely on raw byte limits (the easy part) and, if nesting attacks are a concern, add schema-level validation (max nesting depth) in the deserialization layer (Pydantic model validators, etc.).

### 3. Rate Limiting — Algorithms, Standards, and Multi-Tenant Scoping (S10)

**Standards Status:**

- **RFC 6585 (2011):** Defined HTTP 429 Too Many Requests status code (still current).
- **RFC 9110 (2022):** Re-affirmed 429 and introduced the `Retry-After` response header as the standard way to signal when a client should retry.
- **draft-ietf-httpapi-ratelimit-headers (IETF, Active):** Current version is **draft-11**, published **23 May 2026**, expires **24 November 2026**. This is **not yet an RFC**—it is a Standards Track Internet-Draft in active development by the HTTPAPI Working Group.

**RateLimit Header Specification (draft-11):**

The draft defines two header fields using RFC 9651 Structured Field syntax:

1. **RateLimit-Policy** (server → client, describes the quota policy):
   ```
   RateLimit-Policy: "burst";q=100;w=60,"daily";q=1000;w=86400
   ```
   - `q`: quota allocated (requests, bytes, or concurrent-requests depending on `qu` parameter).
   - `w`: policy window in seconds.
   - `qu`: quota unit (defaults to `requests`).

2. **RateLimit** (server → client, current state):
   ```
   RateLimit: "default";r=50;t=30
   ```
   - `r`: available quota remaining.
   - `t`: time window in seconds.
   - `qu`: quota unit.

**Legacy Conventions (De Facto, Not Standard Yet):**

In practice, many APIs still use the earlier de facto standard headers (before structured fields):
- `X-RateLimit-Limit`: Total quota.
- `X-RateLimit-Remaining`: Remaining quota.
- `X-RateLimit-Reset`: Unix timestamp when the quota resets.

Examples: GitHub, Stripe, Cloudflare use `X-RateLimit-*` headers. The IETF draft explicitly allows for backwards compatibility but eventually intends to standardize on the new structured-field format.

**HTTP Status Code and Retry-After:**

- **429 Too Many Requests** (RFC 6585/9110): Correct status when rate limit is exceeded.
- **Retry-After** header (RFC 9110 §10.2.3): Indicates when the client should retry. Two formats:
  - Delay in seconds: `Retry-After: 30`
  - Exact date/time: `Retry-After: Wed, 21 Oct 2026 07:28:00 GMT`

**Rate Limiting Algorithms:**

Three major algorithms compete in production:

| Algorithm | Burst Tolerance | Memory per Client | Accuracy | Recommended For | Trade-off |
|-----------|---|---|---|---|---|
| **Token Bucket** | ✅ High (natural bursts OK) | O(1) | ~95% (allows short-term overage) | User-facing APIs with bursty traffic | Simplest; slight overage at boundaries |
| **Sliding Window Log** | ❌ None (strict) | O(requests in window) | ✅ 100% (perfect accuracy) | APIs requiring strict limits; not distributed | High memory; impractical at scale |
| **Sliding Window Counter** | ✅ Medium | O(1) | ~99.7% (Cloudflare's implementation) | Distributed, high-throughput APIs | Most production-appropriate |

**Production Practice (2026):**
- **Single-instance apps:** Token bucket is the go-to (simple, tolerates natural bursts from page loads, etc.).
- **Distributed/multi-pod apps:** Sliding window counter backed by Redis (Cloudflare's 0.003% error rate over 400M requests shows the accuracy). Token bucket is also viable with Redis atomic operations (Lua script).
- **Default algorithm for varco:** Token bucket (simplest, HTTP-idiomatic for APIs).

**Multi-Tenant Rate Limiting — Scoping and Keying:**

OWASP API Top 10 2023 ranks "Unrestricted Resource Consumption" (A4) as a critical risk. Rate limiting must be implemented at multiple scopes:

1. **Per-Tenant Quota:** Each tenant has their own bucket. Prevents "noisy neighbor" abuse (one tenant consuming all quota).
2. **Per-Subject Quota (if authenticated):** Each user/API key within a tenant has a sub-quota.
3. **Per-IP Quota (for unauthenticated traffic):** Protects against distributed abuse from sources without identity.
4. **Layered enforcement:** Stricter per-IP limit for unauthenticated, medium per-subject, generous per-tenant.

**Example (multi-tenant SaaS):**
- Unauthenticated per-IP: 10 req/min (very strict, discourages enumeration).
- Authenticated per-user: 100 req/min.
- Per-tenant burst: 1000 req/min (aggregated across all users in that tenant).
- If any limit is exceeded, respond with 429 + `Retry-After`.

**IP-Based Limiting and Trusted Proxy Headers — Critical Security Caveat:**

RFC 7239 (Forwarded Header) and the de facto `X-Forwarded-For` header allow a proxy to communicate the original client IP. **This is a critical trust boundary:**

- **Vulnerable pattern:** Trust `X-Forwarded-For` blindly → attacker sends `X-Forwarded-For: 192.0.2.1` → limiter thinks requests are from different IPs, bypassing limits.
- **Correct pattern (per RFC 7239 and OWASP):**
  1. Identify trusted proxies (IP whitelist of your ingress/load balancer).
  2. Extract client IP from `Forwarded` (RFC 7239 structured header, preferred) or `X-Forwarded-For` (legacy).
  3. Take the **first N hops** from the header, where N = (number of trusted proxies between the internet and your app).
  4. If the header is absent or malformed, fall back to the TCP connection source (the last untrusted proxy).
  5. **Never follow redirects** in the header list; pin to the first resolved address (prevents DNS-rebinding attacks).

Example in a Kubernetes cluster:
- Trusted proxy: the ingress controller at 10.0.0.5.
- Client IP extraction: `Forwarded: for=203.0.113.42` (RFC 7239) or `X-Forwarded-For: 203.0.113.42, 10.0.0.5` (read the first entry before your ingress).

**Middleware Stack Ordering (Critical Tension):**

Rate-limiting middleware must balance two competing goals:
1. **Early rejection:** Limiting should occur before expensive operations (database queries, external API calls).
2. **Subject-aware keying:** Rate-limiting by subject/tenant requires authentication to have run first (to know who the caller is).

**Recommended order (from outermost to innermost):**
1. Trusted proxy header extraction (`X-Forwarded-For`/`Forwarded`).
2. **Rate limiting middleware (early, per-IP if unauthenticated).**
3. Authentication middleware (populate request context with user/tenant).
4. Per-subject rate limiting (dependency injection; limits per user/tenant within route handlers).
5. Authorization, business logic, database queries.

This allows unauthenticated requests to be rejected cheaply (per-IP), while authenticated requests get finer-grained per-subject limits.

### 4. Ecosystem Implementation Status (Reference Data)

**Python / FastAPI Ecosystem:**
- **secure.py v0.3.0 (Feb 2026):** Ships security headers; no built-in rate limiting.
- **SlowAPI (rate limiting):** Dedicated library for FastAPI/Starlette; integrates via middleware; supports in-memory and Redis backends.
- **Starlette:** No built-in security headers or rate limiting middleware; relies on third-party libraries or per-app implementation.

**Node.js / Express Ecosystem:**
- **Helmet.js v8.3.0 (July 2026):** Comprehensive security headers; no rate limiting (separate library, e.g., express-rate-limit).
- **express-rate-limit:** Mature, widely used; supports memory and Redis backends.

**Django:**
- **django-csp:** Third-party CSP implementation; not in core.
- **django-ratelimit:** Third-party rate limiting.
- **SecurityMiddleware (core):** Sets some headers but most are disabled by default and require configuration.

## Options Compared

### Option 1: Ship Security Headers Disabled (Opt-In)

**Pros:**
- Zero breaking changes; zero blast radius.
- Apps that don't need headers pay zero overhead.
- Allows downstream users to set headers however they want.

**Cons:**
- Framework's "secure by default" goal is not met.
- Most users won't opt in (secure.py and Helmet.js show that libraries ship defaults; users then customize, rather than starting from zero).
- Documentation burden: must explain why headers aren't sent and how to enable them.

**Evidence:** Django's SecurityMiddleware is mostly disabled by default; adoption is lower than Helmet.js (which ships defaults on).

---

### Option 2: Ship Security Headers with Sensible Defaults (Opt-Out)

**Pros:**
- New users are secure-by-default out of the box.
- Follows secure.py and Helmet.js precedent.
- Most headers (HSTS, X-Frame-Options, etc.) have no downside for JSON APIs.

**Cons:**
- CSP with `default-src 'none'` breaks FastAPI's `/docs` and `/redoc` endpoints (significant breakage for a framework).
- Some deployments (non-HTTPS dev, plain HTTP behind proxy) are broken by HSTS if not guarded.
- Requires careful defaults that are "permissive enough" to not break common use cases.

**Evidence:** secure.py (BALANCED preset) and Helmet.js both ship defaults; users can customize. But they both use a permissive CSP (`default-src 'self' ...`) to avoid breakage.

---

### Option 3: Ship Headers as Opt-In Middleware with Two Presets (Recommended)

**Pros:**
- Users who want "secure by default" can use the STRICT preset; those who want control use BALANCED or CUSTOM.
- Flexible: varco can ship middleware, not middleware instances—users compose it.
- Follows secure.py's philosophy: "reasonable starting point."

**Cons:**
- Requires shipping middleware + documentation on presets.
- Users who don't read docs still ship no headers.

**Evidence:** secure.py ships BALANCED (permissive) by default but offers a STRICT option for higher-security deployments.

---

### Rate Limiting Algorithm: Token Bucket vs. Sliding Window Counter

**Token Bucket:**
- Natural burst tolerance (matches HTTP semantics: multiple requests from a single page load).
- Simplest implementation (especially in-memory).
- Slight edge-case overage at bucket boundaries.

**Sliding Window Counter (Redis-backed):**
- 99.7% accuracy even at high throughput.
- Better for strict SLA enforcement.
- Requires Redis (not available in-memory).

**Recommendation for varco:** Ship **token bucket as the in-memory default** (simple, HTTPidiom), and **allow swapping for sliding-window via configuration** (for high-security, distributed deployments).

## Version/Compatibility Notes

- **OWASP Secure Headers Project:** Currently maintained (2026); no breaking changes expected to the core 8-header set (HSTS, CSP, X-Frame-Options, X-Content-Type-Options, Referrer-Policy, COOP, CORP, Permissions-Policy). OWASP Top 10 2025 (finalized Jan 2026) reaffirmed these as table-stakes.
- **RFC 9110 (HTTP Semantics):** Published 2022; status 413 "Content Too Large" (renamed from "Payload Too Large") is stable. Retry-After is stable.
- **RFC 9651 (Structured Field Values):** Published 2024; basis for the new RateLimit header syntax. Stable but recent.
- **draft-ietf-httpapi-ratelimit-headers-11:** Published 23 May 2026; **expires 24 November 2026**. Not yet an RFC. May still undergo significant changes before standardization. The older `X-RateLimit-*` de facto standard will likely coexist for years after the RFC is published.
- **Permissions-Policy (W3C):** Working Draft as of 18 June 2026 (this brief's publication date). Successor to Feature-Policy (deprecated ~2023). No RFC equivalent; W3C maintains it. Unlikely to change drastically before Recommendation status.
- **secure.py:** v0.3.0 (Feb 2026); Python 3.10+ only; actively maintained. Zero new runtime dependencies (uses only stdlib and Starlette, both already present).
- **Helmet.js:** v8.3.0 (July 2026); actively maintained; Node.js equivalent.
- **Django SecurityMiddleware:** Unchanged in Django 6.0 (latest); most settings are **disabled by default**, requiring explicit configuration.

---

## Evidence Gaps

1. **Authoritative guidance on CSP for JSON APIs (vs. HTML apps):** OWASP Secure Headers Project recommends `default-src 'none'` as the most secure, but no framework ships this by default because it breaks use cases like Swagger/ReDoc documentation routes. A dedicated brief on "CSP policy per-route in FastAPI" would clarify the correct pattern.

2. **Exact body-size limit recommendations from OWASP or IETF:** No RFC or OWASP guide specifies recommended body-size ceilings (1 MB, 10 MB, 100 MB). The current practice (nginx 1 MB, AWS 10 MB, etc.) is vendor-specific, not standardized. Worth a future brief.

3. **RateLimit header adoption timeline:** The draft-11 expires November 2026. It may become an RFC in 2026–2027 or cycle for another revision. The coexistence period with `X-RateLimit-*` headers is unpredictable. Revisit this in 2027 once the RFC status is known.

4. **Trusted proxy detection in practice:** RFC 7239 and RFC 9110 define the `Forwarded` header, but most Python/Node.js apps still use `X-Forwarded-For` (non-standard but de facto). The ecosystem's handling of multi-hop proxies, trusted-proxy configuration, and IP extraction strategies is not uniformly benchmarked. A future brief on "IP extraction in multi-proxy deployments" would be valuable.

5. **Performance trade-offs of rate limiting algorithms at scale:** Token bucket vs. sliding window counter — no published benchmark of memory usage, CPU cost, and accuracy vs. throughput in Python + Redis. SlowAPI (Python) and express-rate-limit (Node) both support both, but no comparative benchmark exists in the public domain.

6. **JSON nesting/complexity DoS prevention:** CVE-2026-0994 in protobuf shows the risk, but no framework ships configurable nesting-depth limits (nor a standard way to enforce them). This is currently an application-layer responsibility with no standardized solution.

---

## Librarian's Note

The evidence **strongly favours a three-part approach** for varco's S7/S8/S10 middleware:

1. **Security Headers (S7):** Ship as a **composable middleware with two presets:**
   - **BALANCED** (default, permissive): `default-src 'self'`-based CSP; HSTS enabled only over HTTPS (guarded by `X-Forwarded-Proto` if behind a proxy); includes all 8 headers from OWASP baseline.
   - **STRICT** (opt-in): `default-src 'none'`-based CSP; more aggressive. Users who enable this understand it breaks `/docs` and `/redoc` and take responsibility for overriding.
   - Allow per-route override via route parameters.
   - Foundation: Use secure.py's architecture as a reference (granular, composable, not a single "on/off" toggle).

2. **Body Size Limits (S8):** Ship as **ASGI middleware (opt-in, disabled by default to avoid breakage):**
   - Default limit: **1 MB** (conservative; matches nginx; configurable).
   - Implement correctly: intercept `receive()` messages, count bytes cumulatively, reject with 413 before buffer completes.
   - Add a secondary complexity guard: warn on nesting depth >10 levels (application-layer validation via Pydantic models, not middleware).

3. **Rate Limiting (S10):** Ship as **pluggable, not middleware:**
   - Implement `AbstractRateLimiter` ABC (token bucket + per-tenant support).
   - Provide in-memory `InMemoryRateLimiter` (default, single-process).
   - Provide `RedisRateLimiter` backend (for distributed).
   - Implement via dependency injection or per-route decorator (not global middleware) so subject/tenant can be extracted from auth context.
   - Emit both `X-RateLimit-*` (legacy, for now) and the IETF draft `RateLimit`/`RateLimit-Policy` headers (when the RFC is finalized, drop `X-RateLimit-*`).
   - Return 429 + `Retry-After` on limit exceeded.

**Why this structure:** (1) Headers are opt-in but secure-by-default; (2) body limits are opt-in to avoid breaking existing deployments; (3) rate limiting is dependency-injected so it can key by tenant/subject, not just IP.

**Timing note:** The IETF RateLimit draft expires November 2026. If varco ships rate-limiting headers before October 2026, emit both `X-RateLimit-*` and the draft `RateLimit` (not `RateLimit-Policy` yet, to avoid the risk of the draft changing). Revisit and trim `X-RateLimit-*` support in 3.3.0 (post-RFC).

The decision to **actually implement** is upstream; this brief provides the feature design constraints, the frame of reference from secure.py/Helmet.js/Django, and the current state of IETF standardization.

