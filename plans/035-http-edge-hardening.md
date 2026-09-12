# Plan 035 — HTTP edge hardening: error-leak fix (S3), security headers (S7), body limits (S8), rate limiting (S10)

Covers BACKLOG 3.2 rows **S3** (🔴 must, S — *close the error-response information leak*),
**S7** (🟡 should, S — *security headers middleware*), **S8** (🟡 should, S — *request body size
and complexity limits*) and **S10** (🟡 should, S–M — *HTTP rate-limit middleware, per tenant and
per subject*).

**Research brief backing this plan:**
`design/research/008-http-hardening-conventions-2026.md`, written for S7/S8/S10. Every
externally-grounded claim below cites it as `brief 008 §N`. Where this plan **deviates** from the
brief's Librarian's Note it says so explicitly and argues it (§D-S7-default, §D-S8-default,
§D-S10-headers).

## Scope and siblings

One of five plans in the 3.2 security release
([`plans/000-index-3-2-security-release.md`](000-index-3-2-security-release.md)). This slice is
**first in the build order** — lowest risk, three pure additions, no upstream dependency.

| Plan | Rows | Boundary with this plan |
|---|---|---|
| 033 | S6, S5, S16 | **Owns how `current_tenant()` gets SET.** Phase 5 treats `current_tenant()` as an *input contract* |
| 034 | S1, S2, S13, S14 | Both touch `varco_fastapi/auth/server_auth.py`; **this plan does not** — no overlap |
| 036 | S4, S9, S11 | **Owns the `SecurityPosture` preflight.** §D-seam defines and exports the introspection; 036 builds the harness. This plan must not build a preflight |
| 037 | S12, S15 | No overlap ✅ *planned* |

**Phase 1 (S3) is independently mergeable and must merge first**, per the index's build order and
the grouping decision at `plans/000-index-3-2-security-release.md:53-55`.

## Goal

A varco app gets, by default and with no code change: an error envelope that never echoes an
exception's string to the client, a baseline of security response headers on **every** response
including error responses, and a hard ceiling on request-body bytes. It gets, with one explicit
opt-in, per-tenant / per-subject / per-IP HTTP rate limiting assembled from the `RateLimiter` ABC
and backends varco already ships. And Plan 036 gets one pure, importable function that reports
what of that is actually wired.

## Non-goals

- **No new rate limiter.** `RateLimitConfig` / `RateLimiter` / `InMemoryRateLimiter`
  (`varco_core/varco_core/resilience/rate_limit.py:86, :169, :246`) and `RedisRateLimiter`
  (`varco_redis/varco_redis/rate_limit.py:169`) are used **as-is**. This plan adds the ASGI
  assembly and nothing else. No new algorithm, no token bucket (§D-S10-algorithm).
- **No change to the `RateLimiter` ABC.** Not one new abstract method — the same rule that keeps
  `BulkCache` a separate Protocol from `AsyncCache` (Plan 011 / D-11). The consequence is that
  remaining-quota headers cannot be emitted; that is parked, not worked around (§D-S10-headers).
- **No `SecurityPosture` preflight, and no startup refusal.** Plan 036 owns the harness. This plan
  ships a pure `inspect_http_edge()` read (§D-seam) and never raises from it.
- **No JSON nesting/complexity limit.** Brief 008 §2 is explicit that no framework middleware can
  enforce max nesting depth without knowing the schema, and that it belongs in the deserialization
  layer. Parked with a trigger (§Parked); "complexity limits" in S8's title resolves to **raw byte
  limits only**, and the row is renamed accordingly in BACKLOG.md.
- **No middleware reordering of the existing stack.** Phase 2 *pins and documents* the verified
  order and fixes three comments that misdescribe it; it does not move `MetricsMiddleware`,
  `TracingMiddleware` or `extra_middleware`. Two genuine ordering defects found while verifying are
  filed to BACKLOG.md instead (§D-order-bugs).
- **No `X-Powered-By` / `Server` header stripping.** `Server` is set by uvicorn at the ASGI-server
  layer, not by the app; a middleware cannot reliably remove it. Documented as a deployment
  concern (brief 008 §1's "Deprecated/Harmful Headers").
- **No per-route CSP override machinery.** Brief 008's Evidence Gap 1 records that no authoritative
  per-route-CSP-in-FastAPI pattern exists. `exclude_paths` covers the one real case (`/docs`,
  `/redoc`) without inventing a route-decorator API.
- **No new conformance module.** None of the four rows implements one of the five `varco_core` ABCs
  that `testkit/varco_conformance` covers, so **no `COVERAGE.md` row is owed** — stated so the
  absence is a decision, not an oversight.

---

## Design

### What already exists — verified against source while writing this plan

Every anchor below was opened and read; the two marked ⚠️ are **corrections to the scout report /
BACKLOG row wording**.

| Fact | Location | Consequence |
|---|---|---|
| The named S3 leak: `body = {"code": "INTERNAL_ERROR", "message": str(exc)}` | `varco_fastapi/varco_fastapi/middleware/error.py:283` | Real, and fixed in Phase 1 |
| ⚠️ **A second, identical leak the backlog does not name**: `body = {"code": "SERVICE_ERROR", "message": str(exc)}` | `varco_fastapi/varco_fastapi/exceptions.py:159` | Same class, same fix, same phase — fixing only `error.py` would leave the route-handler path leaking |
| ⚠️ **The named path is narrow; the *reachable* sibling is the happy path.** `error_code_for()` never raises (it returns `FastrestErrorCodes.INTERNAL_ERROR` for an unknown type, `http.py:237-238`) and `error_message_for()` swallows a raising `message_resolver` (`http.py:308-314`). So the `except Exception` fallback fires only when `exc.error_params()` raises, a `translator` raises, or `ErrorMessage` validation fails | `varco_core/varco_core/exception/http.py:237, :308-314` | The **broadly reachable** echo of `str(exc)` is `msg.detail`, populated unconditionally at `http.py:324-326` and copied into the body at `error.py:278-279` / `exceptions.py:142-143`. Both are addressed, on different blast-radius tracks (§D-S3) |
| `ErrorEnvelopeSettings` already exists with `env_prefix="VARCO_ERROR_"` and is already threaded into `error_message_for(envelope_settings=…)` | `varco_core/varco_core/exception/settings.py:44`, `http.py:246` | The `detail` knob has a home; no new settings class for S3 |
| `_internal_error_response()` is **already sanitized** — opaque message, `exc_info=True` log, `detail` only under `debug=True` | `error.py:306-328` | This is the shape Phase 1 copies. A `DBAPIError`/`OSError` reaches *this* path, not `_service_error_response`, so the BACKLOG row's examples only apply to a `ServiceException` that wraps one |
| Zero security headers anywhere in the repo | grep, all ten packages | S7 is a pure addition, confirmed |
| Only `MetricsMiddleware` reads `Content-Length`, to record a histogram | `varco_fastapi/varco_fastapi/middleware/metrics.py:396` | Reading is not enforcing, confirmed |
| `IdempotencyMiddleware` calls `await request.body()` — it buffers the whole body | `varco_fastapi/varco_fastapi/middleware/idempotency.py:342` | A body limit that sits *inside* it is useless; §D-order places it outside |
| `RateLimiter` ABC surface is exactly `acquire(key) -> bool` and `retry_after(key) -> float` | `varco_core/varco_core/resilience/rate_limit.py:196, :227` | No `remaining()`. Drives §D-S10-headers |
| `InMemoryRateLimiter` is a **sliding-window log** (`deque(maxlen=rate)` of timestamps), not a token bucket | `rate_limit.py:246-373` (docstring `:248`) | Drives §D-S10-algorithm |
| `InMemoryRateLimiter`'s own DESIGN block: *"with a very large number of distinct keys the lock dict grows unboundedly. Callers should use bounded key spaces"* | `rate_limit.py:265-267` | ⚠️ An IP-keyed limiter has an **attacker-controlled** key space. Drives §D-S10-keyspace |
| `RedisRateLimiter.acquire()` raises `redis.asyncio.RedisError` when Redis is unavailable — *"callers should decide whether to fail open or fail closed"* | `varco_redis/varco_redis/rate_limit.py:205-208` | The middleware must decide explicitly. §D-S10-failopen |
| The established "who is the caller" pair, already used by `IdempotencyMiddleware`: `get_auth_context_or_none()` + `current_tenant()`, `"-"` for anonymous | `varco_fastapi/varco_fastapi/context.py:290`, `varco_core/varco_core/service/tenant.py:151`, used at `idempotency.py:269-305` | Phase 5 reuses this verbatim — no new identity plumbing |
| `ProfilingSettings` is the precedent for a pydantic `BaseSettings` living beside its middleware | `varco_fastapi/varco_fastapi/middleware/profiling.py:94` | Three new settings classes follow it |
| `IdempotencyMiddleware` is exported from `varco_fastapi.middleware.__all__` but **not** from `varco_fastapi.__init__.__all__` | `middleware/__init__.py:169` vs. `varco_fastapi/__init__.py:271-278` | The precedent for an opt-in middleware's export site (§Steps, Phase 7) |

### §D-order — the verified middleware order, and where the three new entries go

**This is the plan's first-class design decision.** Starlette's `add_middleware` is
`self.user_middleware.insert(0, …)` (`.venv/.../starlette/applications.py:101`) and
`build_middleware_stack` constructs `reversed(middleware)` — therefore **`user_middleware[0]` is
outermost, and the last `add_middleware` call wins the outermost position.**

`create_varco_app` adds, in source order:
`ProfilingMiddleware` (`app.py:471`) → `LocalizationMiddleware` (`:486`) →
`RequestContextMiddleware` (`:494`) → `TracingMiddleware` (`:498`) → `MetricsMiddleware` (`:511`) →
`RequestLoggingMiddleware` (`:525`) → `ErrorMiddleware` (`:531`) → `extra_middleware` (`:538-544`) →
`install_cors` (`:548`).

**Verified current execution order (outermost → innermost):**

```
CORSMiddleware → extra_middleware… → ErrorMiddleware → RequestLoggingMiddleware →
MetricsMiddleware → TracingMiddleware → RequestContextMiddleware →
LocalizationMiddleware → ProfilingMiddleware → route handler
```

**Target order after this plan (new entries in bold):**

| # | Layer | Why exactly here |
|---|---|---|
| 1 | `CORSMiddleware` | Unchanged. Preflight `OPTIONS` must not reach anything else (`app.py:62-63`) |
| 2 | **`SecurityHeadersMiddleware`** | Inside CORS so it never rewrites a preflight response; **outside `ErrorMiddleware`** so headers attach to 4xx/5xx and to the 413/429 this plan introduces. This is the whole reason it is not innermost |
| 3 | `extra_middleware…` | Unchanged position (⚠️ note: **outside** `ErrorMiddleware`, contradicting `app.py:537`'s comment — §D-order-bugs) |
| 4 | `ErrorMiddleware` | Unchanged |
| 5 | **`BodyLimitMiddleware`** | **Inside `ErrorMiddleware`** so its 413 renders through the one error envelope with `correlation_id`; **outside** `IdempotencyMiddleware` (`idempotency.py:342` buffers the body) and outside the route handler, so the ceiling is enforced *before* anything buffers. Brief 008 §2: reject before buffering completes |
| 6 | `RequestLoggingMiddleware` | Unchanged |
| 7 | `MetricsMiddleware` | Unchanged |
| 8 | `TracingMiddleware` | Unchanged |
| 9 | **`RateLimitMiddleware(stage=PRE_AUTH)`** | Optional. Outside `RequestContextMiddleware` so an unauthenticated flood is rejected **before** JWT signature verification. Only `IP`/`GLOBAL` scopes are legal here (brief 008 §3's "recommended order", step 2) |
| 10 | `RequestContextMiddleware` | Unchanged — populates `AuthContext` and may enter `tenant_context()` |
| 11 | **`RateLimitMiddleware(stage=POST_AUTH)`** | Optional. Inside `RequestContextMiddleware` because `SUBJECT`/`TENANT` scopes read `get_auth_context_or_none()` / `current_tenant()`; inside `ErrorMiddleware` so the 429 renders through the envelope (brief 008 §3, step 4) |
| 12 | `LocalizationMiddleware` | Unchanged |
| 13 | `IdempotencyMiddleware` | Unchanged opt-in — inside Error, inside RequestContext (Plan 029) |
| 14 | `ProfilingMiddleware` | Unchanged innermost |
| 15 | route handler | |

**DESIGN: one `RateLimitMiddleware` class registered at two stack positions, not one position or two classes**

✅ Brief 008 §3 names the tension outright — *"early rejection"* vs. *"subject-aware keying"* — and
   its recommended order puts a per-IP limit **before** authentication and a per-subject limit
   **after**. A single position cannot satisfy both.
✅ Two positions, one class, one settings object: the rules are partitioned by scope, and a rule
   whose key needs auth (`SUBJECT`/`TENANT`) is **refused at construction** in `PRE_AUTH` with a
   `ValueError` naming the scope. Config errors fail loudly at startup, not silently at runtime as
   an unkeyed `"-"` bucket every caller shares.
✅ Putting the IP limit outside `RequestContextMiddleware` closes a real DoS: without it an
   attacker forces an unbounded number of JWT signature verifications (and, on a `kid` miss, JWKS
   fetches) before any limit applies.
❌ Two registrations of one class is harder to explain than one. Mitigated by `create_varco_app`
   doing the partition automatically and by the ordering table above being the documented contract.
  Rejected — **one position, after auth**: ❌ the signature-verification DoS above; ❌ contradicts
  brief 008 §3's explicit ordering.
  Rejected — **two classes (`IpRateLimitMiddleware` + `RateLimitMiddleware`)**: ❌ two APIs, two
  settings objects and two docs sections for one behaviour that differs only by which key function
  runs; ❌ the shared 429 rendering, `Retry-After` computation, exempt-path logic and fail-open
  policy would be duplicated or hoisted into a third module anyway.
  Rejected — **rely on `create_varco_app(extra_middleware=…)`**: ❌ verified wrong position —
  `extra_middleware` lands **outside** `ErrorMiddleware` (`app.py:538` runs after `:531`), so a 429
  raised there would not render through the error envelope, and it is outside
  `RequestContextMiddleware` too, so `SUBJECT`/`TENANT` keys would always be `"-"`. This is exactly
  why the three new entries get dedicated `create_varco_app` keywords instead.

### §D-order-bugs — three comments that misdescribe the stack; two possible defects, filed not fixed

Verifying the order above turned up three in-repo statements that contradict it. **None is fixed
by moving code** — this plan's rows do not include a stack reorder, and moving `MetricsMiddleware`
would change observable OTel behaviour for every existing app.

| Statement | Location | Reality |
|---|---|---|
| *"Extra middleware from caller (added before CORS = **inside** ErrorMiddleware)"* | `app.py:537` | It is **outside** `ErrorMiddleware` |
| *"Metrics — sits **INSIDE** TracingMiddleware so OTel context is already active"* | `app.py:500-504` | It is **outside** `TracingMiddleware`; the module docstring at `app.py:66-68` states the same reversed order (`Tracing → Metrics → Logging`) and also omits `LocalizationMiddleware`, `ProfilingMiddleware` and `extra_middleware` while listing a `SessionMiddleware` that `create_varco_app` never adds |
| Recommended order `3. Tracing / 4. Metrics / 5. Logging` | `middleware/__init__.py:16-25` | Same reversal relative to what `create_varco_app` actually builds |

| ID | Choice | Consequence |
|---|---|---|
| D-order-bugs | **Correct the three comments to state reality (Phase 2) and pin the real order in a characterization test.** File **two BACKLOG rows** — *"is `MetricsMiddleware` intended to be outside `TracingMiddleware`?"* and *"should `extra_middleware=` land inside `ErrorMiddleware`?"* — as questions with evidence, not as fixes | The plan's own ordering table becomes verifiable and stays true; two real questions get raised with a written record instead of being silently "fixed" inside an unrelated security plan |

✅ CLAUDE.md's rule for a discovered contract violation is a loud marker plus a BACKLOG row, never
   an in-place production fix inside an unrelated change (Test Conventions, conformance paragraph).
   The same discipline applies here.
✅ The characterization test (Step 6) is what makes Phases 3–5 safe: it fails the moment anyone
   perturbs the order, which is the actual risk of adding three entries to a nine-entry stack.
❌ The plan knowingly leaves a possible OTel-context defect unfixed. Accepted and written down —
   it is a metrics-correctness question with its own blast radius, not an HTTP-edge security row.

### §D-S3 — the error leak: flip the fallback in 3.2, warn-only on the `detail` echo

Two paths echo `str(exc)`. They have different blast radii and get different treatment, which is
exactly what the locked migration posture demands (`BACKLOG.md:54`).

| ID | Path | Choice | 3.2 behaviour |
|---|---|---|---|
| D-S3a | `error.py:283` + `exceptions.py:159` (the unmapped-exception fallback) | **Flip.** Return `{"code": …, "message": "An internal error occurred."}` + the existing `correlation_id`; log the type and the string server-side at ERROR with `exc_info=True`, mirroring `_internal_error_response()` (`error.py:306-328`) | Changed. Caller-side fix: look up the `correlation_id` in the log |
| D-S3b | `error.py:278-279` + `exceptions.py:142-143` (the `msg.detail` echo, reachable for **every** `ServiceException`) | **Warn-only.** New `ErrorEnvelopeSettings.include_detail: bool = True` — today's behaviour is the default. `inspect_http_edge()` reports it as a finding for Plan 036; scheduled to flip to `False` in 4.0 | Unchanged, byte-identical |

**DESIGN: flip the fallback, keep `detail` and report it**

✅ D-S3a's caller-side fix is *"read the log line you already have the id for"* — the cheapest
   possible, so the blast-radius rule flips it (`BACKLOG.md:54`). The response bodies that change
   are, by construction, only those already failing in an unmapped way.
✅ D-S3b's `detail` is **deliberately** present: `exceptions.py:135-141` records that `RouteGuard`
   denial messages are only actionable because `str(exc)` reaches the body. Suppressing that by
   default is application work for every consumer parsing it — the other half of the locked rule,
   so it warns in 3.2 and flips in 4.0.
✅ One knob, on the settings object that already governs the envelope
   (`exception/settings.py:44`), threaded through the parameter `error_message_for()` already
   accepts (`http.py:246`). No new settings class, no new call-site plumbing.
✅ Fixing **both** copies in the same commit is non-negotiable: `error.py` handles exceptions from
   middleware, `exceptions.py` handles them from route handlers. Fixing one leaves the other
   leaking on the more common path.
❌ A developer who relied on the fallback's `str(exc)` for debugging loses it from the response.
   Mitigated: `ErrorMiddleware(debug=True)` still exists (`error.py:320-321`), and the server-side
   log is strictly richer than what the body carried.
  Rejected — **suppress `detail` by default in 3.2**: ❌ silently breaks `RouteGuard` denial
  messaging, the one documented consumer, and it is application work, not a config change.
  Rejected — **redact `str(exc)` heuristically** (strip paths, SQL fragments): ❌ a denylist that
  is wrong once is a leak; the sanitized-message + correlation-id pattern is already the house
  answer three lines away at `error.py:315-318`.
  Rejected — **fix only `error.py:283` as the row literally says**: ❌ `exceptions.py:159` is the
  same bug and the same commit; a row's wording does not bound a security fix.

⚠️ **Honest note on reachability, recorded because the BACKLOG row overstates it.** The row cites
*"an unmapped `DBAPIError` or `OSError`"*. Neither reaches `_service_error_response()` — both are
caught by `except Exception` at `error.py:246` and rendered by the already-sanitized
`_internal_error_response()`. The fallback is reached only via a `ServiceException` (possibly
wrapping one of those) whose `error_params()`/`translator`/envelope construction raises. It is
still a leak, still fixed, and the row's title is still right; only its example is imprecise.
Step 3's test asserts the actual reachable trigger rather than a `DBAPIError`.

### §D-S7-default — security headers ON by default, minus CSP, minus CORP/COOP

| ID | Choice | Consequence |
|---|---|---|
| D-S7-default | `SecurityHeadersMiddleware` is **installed by default** by `create_varco_app` with the `BALANCED` preset. `BALANCED` sends **four** headers; `STRICT` is opt-in and adds CSP, COOP, CORP and `frame-ancestors` | Every varco app gains four headers on upgrade. One env var (`VARCO_SECURITY_HEADERS_ENABLED=false`) turns it off; any single header can be overridden or set to `None` to omit |

**`BALANCED` (default) — brief 008 §1's OWASP table, filtered for a JSON API:**

| Header | Value | Source |
|---|---|---|
| `X-Content-Type-Options` | `nosniff` | brief 008 §1 — no downside, any content type |
| `X-Frame-Options` | `DENY` | brief 008 §1 — clickjacking; a JSON API is never framed |
| `Referrer-Policy` | `strict-origin-when-cross-origin` | brief 008 §1 |
| `Strict-Transport-Security` | `max-age=31536000; includeSubDomains` — **sent only when the request is HTTPS** | brief 008 §1 (1 year per OWASP) + §1's operational rule |

**`STRICT` (opt-in) additionally sends:** `Content-Security-Policy: default-src 'none';
frame-ancestors 'none'`, `Cross-Origin-Opener-Policy: same-origin`,
`Cross-Origin-Resource-Policy: same-origin`, `Permissions-Policy: geolocation=(), microphone=(),
camera=()`.

**DESIGN: on by default, but a four-header BALANCED that deliberately omits CSP, COOP and CORP**

✅ Brief 008 §1's framework table shows secure.py and Helmet.js both ship defaults **on**, and
   Django's mostly-disabled `SecurityMiddleware` is named as the lower-adoption counter-example.
   Option 1 ("ship disabled") is rejected there for the reason that applies here: most users never
   opt in.
✅ The blast-radius rule permits the flip: the caller-side fix is one env var, and the failure mode
   of the four chosen headers on a JSON API is *nil* — none of them affects a `fetch()` of JSON.
✅ **CSP is omitted from BALANCED, deviating from secure.py, and this is the argued part.** Brief
   008 §1's "Important caveat for FastAPI apps" is that FastAPI ships `/docs` and `/redoc` which
   load scripts from a CDN, and a `default-src 'none'` CSP **breaks them**. secure.py's escape is a
   permissive `default-src 'self'` — which would *also* break `/docs` (jsDelivr is not `'self'`)
   while providing close to no protection for a JSON body. A header that is either broken or
   meaningless is worse than no header, so BALANCED sends none and `STRICT` sends the real one
   together with an `exclude_paths` default of `("/docs", "/redoc", "/openapi.json")`.
✅ **CORP is omitted from BALANCED, and this is the second argued deviation.**
   `Cross-Origin-Resource-Policy: same-origin` instructs the browser to block cross-origin reads of
   the response — which is precisely what a CORS-enabled JSON API exists to allow. varco ships CORS
   as a first-class, configurable feature (`install_cors`, `CORSConfig`); shipping CORP
   `same-origin` on by default would silently defeat a configured `allow_origins` for browser
   callers. COOP only meaningfully applies to top-level documents and is inert on JSON, so it rides
   with CORP into `STRICT` rather than adding a no-op header to every response.
✅ HSTS is **scheme-guarded**: emitted only when the request is HTTPS, determined from the ASGI
   `scope["scheme"]`, or from `X-Forwarded-Proto` **only when the peer is a configured trusted
   proxy** (the same trust rule as §D-S10-ip; brief 008 §1's proxy paragraph and §3's XFF caveat).
   Brief 008 §1: browsers ignore HSTS over plaintext anyway, and `localhost` is exempt — but a
   custom local domain is **not**, which is how a developer locks themselves out.
✅ Headers are set with **`setdefault` semantics** — a route or a downstream middleware that
   already set the header wins. Additive by construction.
❌ Four new headers appear on every response. That is an observable change; it gets an upgrade
   note, a CHANGELOG `### Added`, and one env var to revert.
❌ An app that legitimately frames its own HTML gets broken by `X-Frame-Options: DENY`. Loud,
   immediate, and fixed by `VARCO_SECURITY_HEADERS_FRAME_OPTIONS=SAMEORIGIN`. Called out in the
   upgrade note as the single most likely breakage.
  Rejected — **opt-in, disabled by default** (brief 008 Option 1): ❌ its own stated Con — *"most
  users won't opt in"* — and it is the cheapest row in a security release.
  Rejected — **secure.py's full 8-header BALANCED** (brief 008 §1): ❌ CORP breaks CORS, CSP
  `default-src 'self'` breaks `/docs` without protecting anything, COOP is inert. Shipping four
  headers that are all correct beats eight of which three are wrong for this shape of app.
  Rejected — **`STRICT` as the default**: ❌ breaks `/docs` and `/redoc` out of the box, which is a
  framework breaking its own default surface.

### §D-S8-default — a body ceiling ON by default at 10 MiB, deviating from the brief

| ID | Choice | Consequence |
|---|---|---|
| D-S8-default | `BodyLimitMiddleware` is **installed by default** at `max_bytes = 10 MiB`, enforcing both a `Content-Length` pre-check **and** a cumulative count over wrapped `receive()`. Over-limit → `RequestBodyTooLargeError` (a `ServiceException` with `http_status=413`) whose message names the ceiling and the env var | An app posting >10 MiB starts getting a self-describing 413 |

**DESIGN: on by default, against brief 008's Librarian's Note, at a precedented ceiling**

✅ Brief 008 §2 records the gap in the strongest terms available to it: neither uvicorn nor
   hypercorn enforces any HTTP body limit, Starlette/FastAPI ship none, and *"any Starlette/FastAPI
   app is vulnerable to memory exhaustion without explicit middleware."* Shipping the fix disabled
   leaves a documented DoS on by default.
✅ **The deviation is argued, not overlooked.** The brief recommends opt-in *"to avoid breakage"*.
   Breakage here is **loud, immediate and self-describing** — a 413 whose body names the byte
   ceiling and `VARCO_BODY_LIMIT_MAX_BYTES` — which is categorically different from a silent
   behaviour change. The blast-radius rule's test is "cheap caller-side fix", and one env var
   qualifies.
✅ 10 MiB is not invented: brief 008 §2's reference table gives AWS API Gateway a **10 MB hard
   limit**, which a very large share of production APIs already live under, and nginx's default is
   **1 MB** — an order of magnitude stricter than what is proposed. 10 MiB is the conservative end
   of "will not surprise anyone", not the aggressive end.
✅ Both checks are needed, per brief 008 §2: `Content-Length` alone *"can be spoofed or omitted
   under chunked transfer-encoding"*, so the declared length is a cheap early rejection and the
   cumulative count over `receive()` is the real enforcement. The cumulative check rejects **before
   buffering completes**, which the brief names as the point of doing this in ASGI middleware.
✅ `413` is the correct status per RFC 9110 §15.4.14 / RFC 6585 §4 (brief 008 §2), explicitly not
   `400`.
✅ Raising a `ServiceException` rather than returning a bare `Response` means the 413 gets the one
   error envelope, the `correlation_id`, and the `Content-Language` handling — for free, because
   `ErrorMiddleware` already unwraps a `BaseExceptionGroup` looking for a `ServiceException`
   (`error.py:188-189`). This is why §D-order puts it **inside** `ErrorMiddleware`.
❌ A file-upload endpoint above 10 MiB breaks on upgrade. Mitigated by `exempt_paths`
   (per-path-prefix exemption), one env var for the global ceiling, and a prominent upgrade note.
❌ `max_bytes` is global, not per-route. A per-route ceiling is a decorator API this plan does not
   need; `exempt_paths` covers the upload endpoint case. Parked.
  Rejected — **opt-in, off by default** (brief 008's Librarian's Note): ❌ leaves a documented
  memory-exhaustion DoS as the default posture of a security release.
  Rejected — **1 MiB, matching nginx**: ❌ breaks a large fraction of real JSON APIs (base64
  attachments, bulk imports) for a marginal security gain over 10 MiB.
  Rejected — **trust `Content-Length` only**: ❌ brief 008 §2 — spoofable and absent under chunked
  encoding.
  Rejected — **a `BaseHTTPMiddleware` implementation**: ❌ it cannot intercept `receive()`, so it
  could only check after Starlette has already buffered — the exact failure the brief warns about.
  `BodyLimitMiddleware` is **pure ASGI**.

### §D-S10-shape — assembly only, keyed by scope, two stages, explicit rules

New module `varco_fastapi/varco_fastapi/middleware/rate_limit.py`:

```python
class RateLimitScope(StrEnum):
    IP = "ip"           # PRE_AUTH-capable
    GLOBAL = "global"   # PRE_AUTH-capable
    SUBJECT = "subject" # POST_AUTH only — needs get_auth_context_or_none()
    TENANT = "tenant"   # POST_AUTH only — needs current_tenant()

class RateLimitStage(StrEnum):
    PRE_AUTH = "pre_auth"
    POST_AUTH = "post_auth"

@dataclass(frozen=True)
class RateLimitRule:
    scope: RateLimitScope
    limiter: RateLimiter          # varco_core.resilience.rate_limit — NOT re-implemented
    name: str = ""                # appears in RateLimit-Policy and in log lines

class RateLimitMiddleware:        # pure ASGI
    def __init__(self, app, *, rules, stage, settings=None,
                 acknowledge_unbounded_keyspace=False) -> None: ...
```

Key construction, deliberately mirroring `IdempotencyMiddleware._scoped_key`
(`idempotency.py:269-285`) rather than inventing a second convention:

| Scope | Key | Missing value |
|---|---|---|
| `GLOBAL` | `ratelimit:global:{name}` | n/a |
| `IP` | `ratelimit:ip:{name}:{client_ip}` | no resolvable peer → **skip the rule**, WARN once |
| `SUBJECT` | `ratelimit:subject:{name}:{auth.user_id}` | anonymous → **skip the rule** (an anonymous caller is covered by the `IP` rule, not by a shared `"-"` bucket) |
| `TENANT` | `ratelimit:tenant:{name}:{current_tenant()}` | no ambient tenant → **skip the rule**, WARN once |

**DESIGN: skip an unkeyable rule rather than share a placeholder bucket**

✅ A shared `"-"` bucket is worse than no limit: every anonymous or untenanted caller in the
   deployment contends for one budget, so a single client trivially denies service to all of them.
   That converts a protection into an amplification.
❌ It means an unkeyable request is unlimited by that rule. Mitigated by the `IP` rule, which is
   always keyable, and by `inspect_http_edge()` reporting a `POST_AUTH`-only configuration with no
   `IP`/`GLOBAL` rule as a finding.
  Rejected — **fail closed (429) when unkeyable**: ❌ every anonymous request to a public endpoint
  becomes a 429 the moment a `SUBJECT` rule exists. This is not the `tenancy_cache_key()` case —
  there, failing closed prevents a cross-tenant *leak*; here there is no leak to prevent.

### §D-S10-ip — the client IP is the TCP peer unless a trusted proxy says otherwise

| ID | Choice | Consequence |
|---|---|---|
| D-S10-ip | The client IP is `scope["client"][0]`. `X-Forwarded-For` / `Forwarded` are consulted **only** when the immediate peer matches `trusted_proxies` (a tuple of CIDRs, default **empty**), and then only `trusted_proxy_hops` entries are skipped from the right | With no configuration, the header is ignored entirely — an attacker cannot mint a fresh bucket per request by setting a header |

✅ Brief 008 §3 names this exactly: *"Vulnerable pattern: Trust `X-Forwarded-For` blindly →
   attacker sends `X-Forwarded-For: 192.0.2.1` → limiter thinks requests are from different IPs,
   bypassing limits"*, and the correct pattern is an explicit trusted-proxy list plus a known hop
   count.
✅ Default-empty is fail-closed in the direction that matters: behind an ingress with no
   configuration every request keys on the ingress IP, so the limit is *too strict*, is noticed
   immediately, and is fixed by configuration — the opposite of a silent bypass.
❌ An operator who forgets `VARCO_RATE_LIMIT_TRUSTED_PROXIES` behind an ingress rate-limits their
   whole cluster as one client. Loud; a Pitfalls row and a WARNING when an `IP` rule denies a
   request whose peer is an RFC 1918 address.
  Rejected — **trust `X-Forwarded-For`'s leftmost entry by default**: ❌ brief 008 §3's named
  bypass; this is a security release.

### §D-S10-keyspace — an IP-scoped `InMemoryRateLimiter` is refused unless acknowledged

`InMemoryRateLimiter`'s own DESIGN block (`rate_limit.py:265-267`) warns that its per-key lock and
window dicts *"grow unboundedly"* and that callers *"should use bounded key spaces"*. An IP-keyed
HTTP limiter has an **attacker-controlled** key space: each new source address permanently costs a
`deque(maxlen=rate)` plus an `asyncio.Lock`.

| ID | Choice | Consequence |
|---|---|---|
| D-S10-keyspace | `RateLimitMiddleware.__init__` raises `ValueError` for a `scope=IP` (or `SUBJECT`, whose keys are also caller-influenced) rule whose limiter is an `InMemoryRateLimiter`, unless `acknowledge_unbounded_keyspace=True`. `RedisRateLimiter` is unaffected — its sorted sets carry a TTL | The one configuration that turns a rate limiter into a memory-exhaustion primitive cannot be reached by accident |

✅ The same "explicit acknowledgement kwarg" shape varco already uses for a footgun it will not
   remove (`mount_tenant_admin(acknowledge_bundled_admin=True)`, CLAUDE.md's `mount_*` row).
✅ It is checked at construction, so it fails at startup in a test, not under load in production.
❌ A single-process app that genuinely wants in-memory IP limiting must pass a keyword. Accepted —
   that is the point, and the error message says exactly what to pass and why.
  Rejected — **add an LRU/`max_keys` bound to `InMemoryRateLimiter`**: it is the right long-term
  fix and is **parked with a trigger**, not done here — it changes a `varco_core` public class's
  memory semantics for every existing `@rate_limit` user, which is a different plan's blast radius.
  Rejected — **hash IPs into N fixed buckets**: ❌ bounded memory bought with false positives —
  unrelated clients share a budget by hash collision.

### §D-S10-failopen — fail **open** on limiter error, loudly, by default

| ID | Choice | Consequence |
|---|---|---|
| D-S10-failopen | A limiter exception (`RedisError`, timeout, anything) is caught, logged at ERROR once per rule per `error_log_interval`, and the request is **allowed**. `VARCO_RATE_LIMIT_FAIL_OPEN=false` inverts it to a 503 | A Redis outage degrades enforcement, it does not take the API down |

✅ `RedisRateLimiter`'s docstring hands the decision to the caller in exactly these terms
   (`varco_redis/varco_redis/rate_limit.py:205-208`); this plan is that caller and must decide
   rather than let an unhandled `RedisError` become a 500 on every request — which is what happens
   today if someone wires the limiter by hand.
✅ Availability: a rate limiter is a protection against abuse, not a correctness invariant. Losing
   it for the duration of a Redis outage is a smaller incident than a total outage. Contrast with
   `tenancy_cache_key()`, which fails closed because the failure mode there is a **data leak**.
✅ `inspect_http_edge()` reports `fail_open` so Plan 036 can flag it on a deployment that wants the
   opposite.
❌ An attacker who can degrade Redis gets an unlimited window. Documented as a Pitfall, with the
   `fail_open=False` inversion and the standing advice to pair `RedisRateLimiter` with
   `@circuit_breaker` (its own docstring's recommendation, `rate_limit.py:208`).

### §D-S10-algorithm — sliding-window log, not token bucket; not this plan's call

Brief 008 §3 recommends **token bucket** as varco's default. varco already ships a
**sliding-window log** in memory (`rate_limit.py:248`) and a Redis sorted-set sliding window.

| ID | Choice | Consequence |
|---|---|---|
| D-S10-algorithm | Use what exists. No algorithm work in this plan | The 100 %-accuracy / no-burst-tolerance row of brief 008 §3's table, which is the conservative choice at an HTTP edge |

✅ The row's own rationale is *"varco already owns every part, it only lacks the wiring"*
   (`BACKLOG.md:75`). Adding an algorithm makes it a different, larger row.
✅ Brief 008 §3's table marks sliding-window log as **100 % accurate, zero burst tolerance** — for
   a public HTTP edge, strictness is a defensible default, and the memory cost the brief flags
   (`O(requests in window)`) is bounded to `rate` entries by `deque(maxlen=rate)`.
❌ No burst tolerance: a browser page issuing 10 parallel requests consumes 10 of the budget at
   once. Documented; the mitigation is choosing `rate` accordingly, and token bucket is parked.

### §D-S10-headers — `Retry-After` always; the IETF draft headers behind one flag; remaining-quota parked

| ID | Choice | Consequence |
|---|---|---|
| D-S10-headers | On 429: status `429`, `Retry-After: <ceil(seconds)>` **always**. `RateLimit-Policy` (draft-11 structured field) only when `emit_draft_headers=True` (default `False`). `RateLimit` (needs `r=` remaining) and `X-RateLimit-*` are **not emitted at all** | Only stable standards ship on by default |

✅ `429` is RFC 6585 / RFC 9110 and `Retry-After` is RFC 9110 §10.2.3 — both stable (brief 008 §3,
   §Version notes). `retry_after` is directly available from the ABC (`rate_limit.py:227`).
✅ `RateLimit-Policy` is computable from `RateLimitConfig.rate`/`period` alone, so it is offered —
   but **behind a flag**, because brief 008's §Version notes say draft-11 *"expires 24 November
   2026"*, is *"not yet an RFC"*, and *"may still undergo significant changes"*. Shipping a
   soon-to-change header on by default in a framework is a wire commitment we cannot honour.
✅ **`RateLimit` and `X-RateLimit-Remaining` are not emitted because the ABC cannot produce
   `remaining`** — and adding an abstract method to `RateLimiter` would break every out-of-tree
   implementation, the same rule that kept `BulkCache` off `AsyncCache` (Plan 011 / D-11).
   The parked shape is written down: an optional `@runtime_checkable` `RateLimitIntrospection`
   Protocol with `remaining(key) -> int`, emitted only for limiters that satisfy it.
❌ This deviates from brief 008's Librarian's Note, which suggests emitting `X-RateLimit-*` **and**
   the draft `RateLimit`. Stated and argued: the note assumes a limiter that can report remaining
   quota; varco's cannot, and inventing an estimate would be a header that lies.
  Rejected — **emit `X-RateLimit-Limit`/`-Reset` without `-Remaining`**: ❌ a partial legacy trio is
  more confusing than none, and the legacy set is explicitly the thing the IETF draft intends to
  replace.

### §D-seam — what Plan 036 consumes, named precisely

Per the index (`plans/000-index-3-2-security-release.md:82-84`), every arrow into 036 is an
obligation on the source plan: **define and export the check; never build the preflight.**

**Module:** `varco_fastapi/varco_fastapi/middleware/introspect.py`
**Exported names** (added to `varco_fastapi.middleware.__all__` **and** `varco_fastapi.__all__`,
because 036 imports them across a plan boundary and needs a stable path):

```python
@dataclass(frozen=True)
class HttpEdgeFinding:
    check: str          # stable id — the contract 036 keys on
    severity: str       # "info" | "warn" | "high"
    detail: str         # what was observed
    remediation: str    # the exact env var / kwarg that fixes it

@dataclass(frozen=True)
class HttpEdgePosture:
    security_headers_installed: bool
    security_headers_preset: str | None      # "balanced" | "strict" | None
    hsts_enabled: bool
    csp_enabled: bool
    body_limit_installed: bool
    body_limit_max_bytes: int | None
    rate_limit_installed: bool
    rate_limit_scopes: tuple[str, ...]       # e.g. ("ip", "tenant")
    rate_limit_fail_open: bool
    rate_limit_distributed: bool             # limiter is not an InMemoryRateLimiter
    error_detail_exposed: bool               # ErrorEnvelopeSettings.include_detail
    error_debug_enabled: bool                # ErrorMiddleware(debug=True)
    findings: tuple[HttpEdgeFinding, ...]

def inspect_http_edge(app: Any) -> HttpEdgePosture: ...
```

**Stable `check` ids this plan commits to emitting** (036 may render, group or escalate them; it
must not redefine them):

| `check` | Severity | Emitted when |
|---|---|---|
| `http.security_headers.absent` | `warn` | No `SecurityHeadersMiddleware` in the stack |
| `http.security_headers.hsts_absent` | `info` | Installed, but `hsts_max_age == 0` |
| `http.body_limit.absent` | `warn` | No `BodyLimitMiddleware` in the stack |
| `http.rate_limit.absent` | `warn` | No `RateLimitMiddleware` in the stack |
| `http.rate_limit.no_pre_auth_rule` | `warn` | Installed, but every rule is `POST_AUTH` — an unauthenticated flood is unlimited |
| `http.rate_limit.in_memory_limiter` | `info` | A limiter is an `InMemoryRateLimiter` — per-process, so the effective limit multiplies by replica count |
| `http.rate_limit.fail_open` | `info` | `fail_open=True` (the default) |
| `http.error.detail_exposed` | `warn` | `ErrorEnvelopeSettings.include_detail` is `True` (D-S3b's 4.0 flip candidate) |
| `http.error.debug_enabled` | `high` | `ErrorMiddleware(debug=True)` — a stack-adjacent repr in the body |

**DESIGN: a pure read over `app.user_middleware`, returning a frozen dataclass**

✅ `Middleware` exposes `.cls` and `.kwargs` (`.venv/.../starlette/middleware/__init__.py:23-25`),
   and `user_middleware` survives startup — so the read needs no registry, no global state and no
   cooperation from the middleware instances.
✅ Frozen dataclass return + a pure function means 036 consumes it with no import from 036 into
   `varco_fastapi.middleware`, and no shared mutable state. Same shape as Plan 037's `RlsPosture`
   (`plans/037-data-layer-tenant-enforcement.md:304`).
✅ Stable string `check` ids, listed above, are the actual contract — 036 can be written against
   this table before a line of 035 is merged.
✅ `inspect_http_edge()` **never raises** and never touches the network: an unrecognised app object
   returns an all-`False` posture with `http.*.absent` findings, so 036 degrades gracefully — the
   index already calls this edge "soft" (`:77`).
❌ It reads a Starlette private-ish attribute. Accepted: it is the only structural source of truth,
   `add_middleware` already raises after startup so there is no supported alternative, and Step 30
   guards it with a test that fails loudly if the attribute shape changes.
  Rejected — **a module-global registry each middleware writes to on `__init__`**: ❌ process-global
  mutable state, wrong under multiple apps in one process, and it would make `inspect_http_edge`
  report a middleware from a *different* app.
  Rejected — **building the preflight here**: ⛔ 036 owns it; the index forbids it (`:82-84`).

### Alternatives considered (plan-level)

- **Adopt `secure.py` as a dependency for S7** — ❌ it is one more runtime dependency in
  `varco_fastapi` for four `response.headers.setdefault(...)` calls, its BALANCED preset is the one
  §D-S7-default argues is wrong for a JSON API (CORP/CSP), and CLAUDE.md's import-budget rule makes
  a new top-level import a measured cost, not a free one.
- **Adopt `SlowAPI` for S10** (brief 008 §4) — ❌ it duplicates the `RateLimiter` ABC and both
  backends varco already ships, and its keying is decorator/`Depends`-based, which cannot see
  `current_tenant()` at the right stack position.
- **Ship all four rows as one opt-in `install_edge_hardening(app)` call** — ❌ collapses four
  independent decisions into one on/off switch, and the whole point of §D-S7-default and
  §D-S8-default is that two of them are safe on by default while S10 is not.
- **Put the three settings classes in `varco_core`** — ❌ they are HTTP-specific and would put
  ASGI/browser concerns in the backend-agnostic core. `varco_core.resilience.rate_limit` (the
  algorithm) stays in core; the HTTP assembly stays in `varco_fastapi`. Same layer rule as
  `varco_core.tls` vs. `varco_fastapi.auth`.

---

## Steps

### Phase 1 — S3: close the error-response information leak (🔴 must, S) — **independently mergeable**

1. [x] `varco_core/tests/test_error_envelope_settings.py` (extend, **failing first**) —
       `ErrorEnvelopeSettings().include_detail is True` (byte-identical default);
       `VARCO_ERROR_INCLUDE_DETAIL=false` parses; `error_message_for(exc,
       envelope_settings=ErrorEnvelopeSettings(include_detail=False)).detail is None`; with the
       default, `detail == str(exc)` exactly as today (`http.py:324-326`).
2. [x] `varco_core/varco_core/exception/settings.py` — add `include_detail: bool = True` with a
       docstring `Attributes:` entry stating it is D-S3b's 4.0 flip candidate.
       `varco_core/varco_core/exception/http.py:324-326` — gate `detail` on
       `settings.include_detail`. **No other default moves.**
3. [x] `varco_fastapi/tests/test_error_leak_s3.py` (new, **failing first**) — define a
       `ServiceException` subclass whose `error_params()` raises (the *actually reachable* trigger,
       per §D-S3's honest note), route it through both paths, and assert for each:
       the body's `message` is the opaque constant, `str(exc)` appears **nowhere** in the serialized
       body, `correlation_id` is present, the status still comes from `_FALLBACK_STATUS`
       (`error.py:332-337` / `exceptions.py:171-175`), and `caplog` contains the exception type and
       `exc_info`. Add a parallel case asserting a *mapped* `ServiceNotFoundError` is
       **byte-identical to today** — the no-regression proof.
4. [x] `varco_fastapi/varco_fastapi/middleware/error.py:280-283` — replace the fallback body with
       `{"code": "INTERNAL_ERROR", "message": "An internal error occurred."}` and add
       `_logger.error(..., exc_info=True)` mirroring `_internal_error_response()`
       (`error.py:308-313`). Add a `DESIGN:` comment citing §D-S3a and `BACKLOG.md`'s S3 row.
5. [x] `varco_fastapi/varco_fastapi/exceptions.py:156-159` — the identical fix, keeping the
       `"SERVICE_ERROR"` code so no code string changes. Both sites reference §D-S3a.

⛔ **CHECKPOINT** — `uv run pytest varco_fastapi/tests/test_error_leak_s3.py
varco_fastapi/tests/test_exception_envelope.py varco_fastapi/tests/test_error_localization_rd3.py
varco_core/tests/test_error_envelope_settings.py`. **Phase 1 ships on its own.**

### Phase 2 — the ordering contract: pin it, then correct the comments (🟢 S)

6. [x] `varco_fastapi/tests/test_middleware_order.py` (new) — build an app with
       `create_varco_app(enable_tracing=True, enable_metrics=True, enable_profiling=True, i18n=…,
       extra_middleware=[_Marker])` and assert `[m.cls.__name__ for m in app.user_middleware]`
       equals the verified list in §D-order **exactly**, in order. A second case asserts the
       Starlette invariant this plan depends on (last `add_middleware` → `user_middleware[0]`) so a
       Starlette upgrade that inverts it fails here rather than silently reordering the stack.
7. [x] `varco_fastapi/varco_fastapi/app.py:537` — correct the comment: `extra_middleware` lands
       **outside** `ErrorMiddleware`, with a one-line consequence (*a `ServiceException` raised
       there is not rendered by the error envelope*) and a pointer to the dedicated
       `security_headers=`/`body_limit=`/`rate_limit=` keywords added in Phases 3–5.
8. [x] `varco_fastapi/varco_fastapi/app.py:60-68` and `:500-504`, and
       `varco_fastapi/varco_fastapi/middleware/__init__.py:6-51` — replace the three order
       descriptions with the single §D-order table (one home for the fact; the other two point at
       it). Do **not** move any `add_middleware` call.
9. [x] `BACKLOG.md` — two new rows in the live cycle's work table, filed as questions with the
       evidence from §D-order-bugs: *"`MetricsMiddleware` sits outside `TracingMiddleware` — is the
       OTel-context comment or the position wrong?"* and *"should `extra_middleware=` land inside
       `ErrorMiddleware`?"* Both 🟡, both explicitly **not** fixed by Plan 035.

⛔ **CHECKPOINT** — Step 6 green **before** any new middleware is added.

### Phase 3 — S7: security headers (🟡 should, S)

10. [x] `varco_fastapi/tests/test_security_headers_middleware.py` (new, **failing first**) —
        `BALANCED` sends exactly the four headers of §D-S7-default with exactly those values;
        `STRICT` additionally sends CSP/COOP/CORP/Permissions-Policy; **BALANCED sends no
        `Content-Security-Policy` and no `Cross-Origin-Resource-Policy`** (the two argued
        omissions, asserted so a future "completeness" edit cannot land silently); HSTS is
        **absent** over `http://` and present over `https://`; HSTS is absent over `http://` with
        `X-Forwarded-Proto: https` and **no** trusted proxy configured, and present when the peer
        is trusted; a header already set by the route is **not overwritten** (`setdefault`); headers
        are present on a 404, on a 500 and on a `ServiceException`-rendered 4xx (the §D-order
        reason it sits outside `ErrorMiddleware`); `exclude_paths` defaults exempt `/docs`,
        `/redoc`, `/openapi.json` under `STRICT`; a header field set to `None` omits it; `enabled=False`
        produces a byte-identical response to no middleware at all.
11. [x] `varco_fastapi/varco_fastapi/middleware/security_headers.py` (new) —
        `SecurityHeadersPreset` (`StrEnum`: `BALANCED`/`STRICT`), `SecurityHeadersSettings`
        (subclass `VarcoSettings`, `SettingsConfigDict(env_prefix="VARCO_SECURITY_HEADERS_",
        frozen=True)`), `SecurityHeadersMiddleware` (pure ASGI, wraps `send` to mutate
        `http.response.start` headers — never `BaseHTTPMiddleware`, so it costs no body buffering).
        Full docstrings with `Args`/`Returns`/`Edge cases`/`Thread safety`, a `DESIGN:` block per
        §D-S7-default citing brief 008 §1 for every value, and an explicit
        *"deliberately not sent: `X-XSS-Protection` (deprecated and harmful — brief 008 §1),
        `Server`/`X-Powered-By` (set below the app)"* note.
12. [x] `varco_fastapi/varco_fastapi/app.py` — `create_varco_app(security_headers:
        SecurityHeadersSettings | bool | None = None)`; `None` → `SecurityHeadersSettings()` (on,
        BALANCED); `False` → not registered. Registered at position 2 of §D-order (i.e. added
        **after** `ErrorMiddleware` and **before** `install_cors`).
13. [x] `varco_fastapi/tests/test_middleware_order.py` (extend) — the §D-order list now contains
        `SecurityHeadersMiddleware` at index 1, and `security_headers=False` removes it and
        restores the Phase-2 list exactly.

⛔ **CHECKPOINT** — `uv run pytest varco_fastapi/tests/test_security_headers_middleware.py
varco_fastapi/tests/test_middleware_order.py varco_fastapi/tests/test_cors_secure_default.py`

### Phase 4 — S8: request body size limits (🟡 should, S)

14. [x] `varco_core/tests/test_exception_taxonomy.py` (extend, **failing first**) —
        `RequestBodyTooLargeError` is a `ServiceException`, maps to HTTP **413** through
        `error_code_for`/`error_message_for`, and carries a `message_key` per
        `technical_docs/features/error-taxonomy-and-i18n.md`'s rule (`code` is the machine id,
        `message_key` is the i18n key).
15. [x] `varco_core/varco_core/exception/` — add `RequestBodyTooLargeError` plus its `ErrorCode`
        registration (413). It lives in `varco_core` because the taxonomy does; the middleware
        that raises it lives in `varco_fastapi`.
16. [x] `varco_fastapi/tests/test_body_limit_middleware.py` (new, **failing first**) — a
        `Content-Length` above the ceiling is rejected **before the route handler runs** (assert a
        handler-side sentinel was never set); a chunked/`Transfer-Encoding` body with **no**
        `Content-Length` that exceeds the ceiling is rejected mid-stream (brief 008 §2's real
        enforcement path); a *lying* `Content-Length` below the ceiling with a larger actual body is
        still rejected; the status is **413**, the body carries the envelope's `code`/`message`
        plus `correlation_id`, and the message names the byte ceiling and
        `VARCO_BODY_LIMIT_MAX_BYTES`; a body exactly at the ceiling passes; a GET with no body
        passes; `exempt_paths` bypasses; `enabled=False` is byte-identical to no middleware;
        `SecurityHeadersMiddleware`'s headers are present on the 413 (the §D-order proof).
17. [x] `varco_fastapi/varco_fastapi/middleware/body_limit.py` (new) — `BodyLimitSettings`
        (`VARCO_BODY_LIMIT_`, fields `enabled=True`, `max_bytes=10*1024*1024`, `exempt_paths=()`,
        `trust_content_length=True`) and `BodyLimitMiddleware` (**pure ASGI**: wraps `receive`,
        counts `http.request` `body` bytes cumulatively, raises `RequestBodyTooLargeError` on the
        first message that crosses the ceiling). `DESIGN:` block per §D-S8-default with brief 008
        §2 citations for the two-check approach and for 413.
18. [x] `varco_fastapi/varco_fastapi/app.py` — `create_varco_app(body_limit: BodyLimitSettings |
        bool | None = None)`; `None` → on at 10 MiB; registered at position 5 of §D-order (added
        **before** `ErrorMiddleware`, **after** `RequestLoggingMiddleware`).
19. [x] `varco_fastapi/tests/test_middleware_order.py` (extend) — `BodyLimitMiddleware` appears
        immediately inside `ErrorMiddleware`; `body_limit=False` removes it.
20. [x] `varco_fastapi/tests/test_body_limit_middleware.py` (extend) — an over-limit request
        carrying an `Idempotency-Key` is rejected **without** `IdempotencyMiddleware` ever calling
        `await request.body()` (`idempotency.py:342`), asserted with a store spy. This is the
        regression test for the §D-order placement decision.

⛔ **CHECKPOINT** — `uv run pytest varco_fastapi/tests/test_body_limit_middleware.py
varco_fastapi/tests/test_idempotency_middleware.py varco_fastapi/tests/test_middleware_order.py`

### Phase 5 — S10: HTTP rate-limit middleware (🟡 should, S–M)

21. [x] `varco_fastapi/tests/test_rate_limit_middleware.py` (new, **failing first**, unit, using
        `InMemoryRateLimiter` and `acknowledge_unbounded_keyspace=True` where required) —
        **construction:** a `SUBJECT`/`TENANT` rule in `stage=PRE_AUTH` raises `ValueError` naming
        the scope; an `IP`/`SUBJECT` rule with an `InMemoryRateLimiter` and no acknowledgement
        raises `ValueError` naming the kwarg (§D-S10-keyspace); the same with `RedisRateLimiter`
        does not.
        **keying:** `TENANT` keys on `current_tenant()`; `SUBJECT` keys on
        `get_auth_context_or_none().user_id`; two tenants get independent budgets; an anonymous
        request **skips** the `SUBJECT` rule rather than sharing a bucket (§D-S10-shape).
        **IP:** `X-Forwarded-For` is ignored with no trusted proxy; honoured when the peer matches
        `trusted_proxies` at the configured hop count (§D-S10-ip).
        **response:** 429 with `Retry-After` as an integer ≥ 1; the body is the standard envelope
        with `correlation_id`; `SecurityHeadersMiddleware`'s headers are present on it;
        `RateLimit-Policy` absent by default and present with `emit_draft_headers=True`; **no
        `RateLimit` and no `X-RateLimit-*` header is ever emitted** (§D-S10-headers, asserted so a
        future edit cannot add a lying `remaining`).
        **failure:** a limiter raising `RedisError` allows the request and logs ERROR
        (`fail_open=True`); with `fail_open=False` it returns 503.
        **inertness:** with no rules the middleware is a pass-through, byte-identical.
22. [x] `varco_fastapi/varco_fastapi/middleware/rate_limit.py` (new) — `RateLimitScope`,
        `RateLimitStage`, `RateLimitRule` (`@dataclass(frozen=True)`), `RateLimitSettings`
        (`VARCO_RATE_LIMIT_`: `enabled`, `fail_open=True`, `trusted_proxies=()`,
        `trusted_proxy_hops=0`, `emit_draft_headers=False`, `exempt_paths=()`,
        `error_log_interval=60.0`), `RateLimitMiddleware` (pure ASGI). It **imports
        `RateLimiter`/`RateLimitConfig` from `varco_core.resilience.rate_limit` and implements no
        limiting logic of its own**. Full docstrings; `DESIGN:` blocks per §D-S10-shape,
        §D-S10-ip, §D-S10-keyspace, §D-S10-failopen, §D-S10-algorithm, §D-S10-headers, each citing
        brief 008 §3.
        ⚠️ Any `asyncio.Lock` is created lazily inside a method, never in `__init__` (CLAUDE.md).
23. [x] `varco_fastapi/varco_fastapi/app.py` — `create_varco_app(rate_limit:
        RateLimitBundle | None = None)`, **default `None` = not registered** (§D-S10 is the one
        opt-in row). `RateLimitBundle` is a frozen dataclass of `rules` + `settings`;
        `create_varco_app` partitions `rules` by scope and registers **up to two**
        `RateLimitMiddleware` instances at positions 9 and 11 of §D-order.
24. [x] `varco_fastapi/tests/test_middleware_order.py` (extend) — a bundle with `IP` + `TENANT`
        rules yields two `RateLimitMiddleware` entries at exactly positions 9 and 11 (one outside
        and one inside `RequestContextMiddleware`); a bundle with only `TENANT` rules yields one,
        inside; `rate_limit=None` yields none.
25. [x] `varco_fastapi/tests/conftest.py` — a **module-local-style** session-scoped `redis_url`
        fixture honouring `VARCO_TEST_REDIS_URL` and otherwise starting a testcontainer, following
        the in-file precedent at `varco_fastapi/tests/test_app_migrations_integration.py:82-104`
        (which records why `varco_fastapi` has no shared fixture) and CLAUDE.md's
        `VARCO_TEST_<SERVICE>_URL` contract — bare `REDIS_URL` is **never** honoured.
26. [x] `varco_fastapi/tests/test_rate_limit_middleware_integration.py` (new,
        `@pytest.mark.integration`, `pytest.importorskip("varco_redis")`) — real Redis,
        `uuid4().hex[:8]`-namespaced key prefix per the shared-container rule: two concurrent
        clients under one `TENANT` rule share a budget across **two independently constructed
        middleware instances** (the property `InMemoryRateLimiter` cannot provide); the 429 carries
        a correct `Retry-After`; after `period` elapses the request succeeds again.
        ⚠️ `varco_redis` is imported **only inside this test module** — it must not appear in
        `varco_fastapi`'s `[project.dependencies]`, and `test_layer_boundaries.py`'s
        `FORBIDDEN_MODULES` is unchanged. **This step is the one droppable step in Phase 5**; the
        fallback is a fake limiter raising `RedisError`, which Step 21 already covers for fail-open.

⛔ **CHECKPOINT** — `uv run pytest varco_fastapi/tests/test_rate_limit_middleware.py
varco_fastapi/tests/test_middleware_order.py` and `uv run pytest varco_fastapi/tests/ -m integration`

### Phase 6 — the seam Plan 036 consumes (🟡 S)

27. [x] `varco_fastapi/tests/test_http_edge_introspect.py` (new, **failing first**) — a default
        `create_varco_app()` reports `security_headers_installed=True`, `body_limit_installed=True`,
        `rate_limit_installed=False`, and exactly the finding set
        `{http.rate_limit.absent, http.error.detail_exposed}`; an app with everything disabled
        reports all three `*.absent` findings; an app with only `POST_AUTH` rules reports
        `http.rate_limit.no_pre_auth_rule`; `ErrorMiddleware(debug=True)` reports
        `http.error.debug_enabled` at severity `high`; **`inspect_http_edge(object())` returns an
        all-absent posture and does not raise** (the graceful-degradation contract the index's soft
        edge depends on, `plans/000-index-3-2-security-release.md:77`); and the emitted `check`
        strings equal the §D-seam table **exactly** — asserted against a literal frozenset, so
        renaming one silently breaks this test rather than Plan 036.
28. [x] `varco_fastapi/varco_fastapi/middleware/introspect.py` (new) — `HttpEdgeFinding`,
        `HttpEdgePosture` (both `@dataclass(frozen=True)`), `inspect_http_edge()`. Pure; no I/O;
        never raises. Module docstring states: **this is a seam for Plan 036's `SecurityPosture`
        preflight and is not itself a preflight** — it reports, it never warns at startup and never
        refuses to boot.
29. [x] `varco_fastapi/varco_fastapi/middleware/__init__.py` + `varco_fastapi/varco_fastapi/__init__.py`
        — export `HttpEdgePosture`, `HttpEdgeFinding`, `inspect_http_edge` from **both** (the
        cross-plan contract justifies the top-level slot); export the three middlewares and their
        settings/enums from `varco_fastapi.middleware.__all__` only, following the
        `IdempotencyMiddleware` precedent (`middleware/__init__.py:169`).
30. [x] `varco_fastapi/tests/test_http_edge_introspect.py` (extend) — a guard on the Starlette
        attribute shape `inspect_http_edge` depends on: every entry of `app.user_middleware` has
        `.cls` and `.kwargs` (`.venv/.../starlette/middleware/__init__.py:23-25`). A Starlette
        upgrade that changes it fails here, loudly, instead of silently returning an empty posture.

⛔ **CHECKPOINT** — Plan 036 is now unblocked on this edge.

### Phase 7 — docs, README, CLAUDE.md, CHANGELOG, api-surface (🟡 S — **same commit as the code**)

31. [x] `technical_docs/features/http-edge-hardening.md` (new — one doc for the three additions,
        because they share the ordering contract and the `create_varco_app` keywords, and splitting
        would put that table in three places). Sections: the §D-order table as the normative
        ordering contract; the S7 header table with brief 008 §1 citations and the two argued
        omissions; the S8 two-check model, the 10 MiB choice and `exempt_paths`; the S10 scope/stage
        model, trusted-proxy configuration, fail-open, and the standards status (`429`/`Retry-After`
        stable, `RateLimit-Policy` draft-11 expiring 24 Nov 2026, remaining-quota parked).
        **Pitfalls table**, at minimum: `X-Frame-Options: DENY` breaks a self-framing HTML app ·
        HSTS silently absent behind a TLS-terminating proxy until `trusted_proxies` is set, and
        **locks you out of a custom local domain once it is set** (brief 008 §1) ·
        `STRICT` CSP breaks `/docs`/`/redoc` unless `exclude_paths` covers them · a 10 MiB ceiling
        silently caps an upload endpoint until `exempt_paths` is set · `InMemoryRateLimiter` is
        per-process so the effective limit is `rate × replicas` · forgetting `trusted_proxies`
        behind an ingress rate-limits the whole cluster as one client ·
        `fail_open=True` means a Redis outage disables enforcement · a `SUBJECT`-only rule set
        leaves anonymous traffic unlimited · registering any of the three via
        `extra_middleware=` puts it at the **wrong** position (verified, §D-order-bugs).
32. [x] `technical_docs/features/error-taxonomy-and-i18n.md` — a subsection for S3: the two fixed
        fallback sites, `include_detail`'s 4.0 flip candidacy, and a **Pitfalls** row: *the response
        no longer carries the exception string — correlate via `correlation_id` in the log*.
33. [x] `README.md` — a "Security headers", "Request body limits" and "HTTP rate limiting" section
        with runnable snippets (both the `create_varco_app(...)` form and the hand-registered form
        with its required position), plus three env-var reference tables:
        `VARCO_SECURITY_HEADERS_*`, `VARCO_BODY_LIMIT_*`, `VARCO_RATE_LIMIT_*`, and the new
        `VARCO_ERROR_INCLUDE_DETAIL` row in the existing `VARCO_ERROR_*` table.
34. [x] `CLAUDE.md` — pointer-only edits: (a) a one-line "HTTP edge hardening (Plan 035 / S3, S7,
        S8, S10)" entry under Key Abstractions pointing at the feature doc, carrying the two
        **Rules** that change agent behaviour — *never register a varco edge middleware via
        `extra_middleware=` (wrong position, verified)* and *never add an abstract method to
        `RateLimiter` to get remaining quota — use an optional Protocol* ; (b) a Decision-Tree
        branch: *browser security header? → `varco_fastapi.middleware.security_headers` · request
        too big? → `…middleware.body_limit` · HTTP rate limit? → `…middleware.rate_limit`, never a
        new limiter · "is my edge configured?" → `inspect_http_edge()`, and the preflight is Plan
        036's*.
35. [x] `CHANGELOG.md` `## [Unreleased]` — `### Security`: the S3 fallback leak, both sites, with
        the "Plan 035 / S3" tag. `### Added`: `SecurityHeadersMiddleware`, `BodyLimitMiddleware`,
        `RateLimitMiddleware`, `inspect_http_edge`, `ErrorEnvelopeSettings.include_detail`.
        `### Changed`: the unmapped-exception body no longer contains `str(exc)`; four security
        headers are now sent by default; a 10 MiB body ceiling is now enforced by default — each
        with its one-env-var revert. `BACKLOG.md`: mark S3/S7/S8/S10 `✅ planned → plans/035…`,
        rename S8 to *"Request body size limits"* per §Non-goals, and add the parked rows below.
36. [x] `uv run python scripts/api_surface.py` then `--check`; **commit both snapshot files in this
        commit** (CI gate on `make lint`'s no-`PKG` path). Expected `varco_fastapi` additions:
        `HttpEdgePosture`, `HttpEdgeFinding`, `inspect_http_edge`. Additions are reported as notes
        and never fail `--check` (CLAUDE.md), but the snapshot must still be regenerated.
37. [x] `uv run python scripts/import_budget.py --check --warn-only` — the three new modules are
        imported by `varco_fastapi/__init__.py` only via `middleware/introspect.py`; confirm no
        ceiling breach before committing, per CLAUDE.md's *"a new top-level import needs a budget
        check, not a hunch"*.

⛔ **CHECKPOINT** — `make lint`, `make type-check`, `make test`, `make integration-test PKG=varco_fastapi`.

---

## Migration and upgrade note (existing deployments — read before shipping)

**Three observable changes on upgrade to 3.2, each revertible with one environment variable.**

| Change | Who notices | Revert |
|---|---|---|
| An unmapped `ServiceException` no longer echoes `str(exc)` in `message` | Anyone parsing that body — it was already an error path. The information moved to the server log, keyed by the same `correlation_id` already in the body | None, deliberately. This is the security fix (§D-S3a) |
| Four security headers on every response | A browser client that **frames** the API's HTML (`X-Frame-Options: DENY`), or a proxy that already sets the same headers (varco `setdefault`s, so the existing value wins) | `VARCO_SECURITY_HEADERS_ENABLED=false`, or `VARCO_SECURITY_HEADERS_FRAME_OPTIONS=SAMEORIGIN` |
| Request bodies over 10 MiB get a 413 | An upload or bulk-import endpoint | `VARCO_BODY_LIMIT_MAX_BYTES=<bytes>`, `VARCO_BODY_LIMIT_EXEMPT_PATHS=/upload`, or `VARCO_BODY_LIMIT_ENABLED=false` |

**Nothing else changes.** Rate limiting is **not** installed unless `rate_limit=` is passed
(§D-S10). `ErrorEnvelopeSettings.include_detail` defaults to `True`, so the `detail` key on a
mapped `ServiceException` is byte-identical to 3.1 — it is reported by `inspect_http_edge()` as a
4.0 flip candidate and does not move in 3.2 (§D-S3b).

**Adopting rate limiting, in order:** (1) start with an `IP` rule only, `fail_open=True`, a rate
well above observed peak; (2) set `VARCO_RATE_LIMIT_TRUSTED_PROXIES` **before** deploying behind an
ingress, or every request keys on the ingress address (§D-S10-ip); (3) add `TENANT` and `SUBJECT`
rules once the IP rule has run clean for a full traffic cycle; (4) move to `RedisRateLimiter` before
scaling past one replica — `InMemoryRateLimiter` multiplies the effective limit by replica count.

**The 4.0 flip list this plan contributes to:** `ErrorEnvelopeSettings.include_detail` → `False`.
That is the only entry; S7 and S8 flip *now* because their caller-side fix is one env var and their
failure is loud.

---

## Edge cases

- **Preflight `OPTIONS`** → handled by `CORSMiddleware`, which is outermost; `SecurityHeadersMiddleware`
  sits inside it and never rewrites a preflight response. Asserted.
- **A response the route already gave a security header** → `setdefault`; the route wins. Asserted.
- **HSTS over plain `http://`** → not sent. Brief 008 §1: browsers ignore it anyway, and sending it
  would be a lie about the connection. Asserted for both the direct and the untrusted-proxy case.
- **`Content-Length` absent (chunked)** → the pre-check is skipped; the cumulative `receive()` count
  is the enforcement. Asserted (brief 008 §2).
- **`Content-Length` present and lying (declares 1 KB, sends 50 MiB)** → the cumulative count
  rejects it. Asserted; this is why `trust_content_length` is an optimisation, not the check.
- **A route that never reads the body** → the `receive()` wrapper never fires, so only the
  `Content-Length` pre-check applies. Documented; the body is never buffered either, so there is no
  exposure.
- **A streaming response** → untouched. `BodyLimitMiddleware` wraps `receive`, not `send`.
- **429 while `IdempotencyMiddleware` holds a reservation** → the rate limiter sits **outside**
  it (§D-order position 11 vs. 13), so no reservation is ever taken for a rate-limited request.
- **A rule whose key is unavailable** → the rule is skipped, not failed-closed (§D-S10-shape). An
  all-`POST_AUTH` configuration is reported as `http.rate_limit.no_pre_auth_rule`.
- **Multiple apps in one process** → `inspect_http_edge(app)` takes the app; there is no global
  registry, so each app reports its own stack (§D-seam).
- **`inspect_http_edge()` on a non-Starlette object** → all-absent posture, no raise.
- **`create_varco_app(enable_error_middleware=False)`** → `BodyLimitMiddleware`'s 413 and
  `RateLimitMiddleware`'s 429 have no envelope to render through. Both must then emit a plain JSON
  response themselves rather than raise. Asserted, and documented as a Pitfall.

## Verification

```bash
uv sync --all-packages --all-extras

# Phase 1 alone (it ships alone)
uv run pytest varco_fastapi/tests/test_error_leak_s3.py \
              varco_fastapi/tests/test_exception_envelope.py \
              varco_fastapi/tests/test_error_localization_rd3.py \
              varco_core/tests/test_error_envelope_settings.py -q

# Phases 2-6
uv run pytest varco_fastapi/tests/test_middleware_order.py \
              varco_fastapi/tests/test_security_headers_middleware.py \
              varco_fastapi/tests/test_body_limit_middleware.py \
              varco_fastapi/tests/test_rate_limit_middleware.py \
              varco_fastapi/tests/test_http_edge_introspect.py \
              varco_fastapi/tests/test_idempotency_middleware.py \
              varco_fastapi/tests/test_cors_secure_default.py -q

# integration — real Redis
uv run pytest varco_fastapi/tests/ -m integration -q
make integration-test PKG=varco_fastapi

uv run python scripts/api_surface.py --check         # MUST be clean before committing
uv run python scripts/import_budget.py --check --warn-only
make lint && make type-check && make test
```

**DoD:**
1. Step 3 proves `str(exc)` appears nowhere in either fallback body, **and** that a mapped
   exception's body is byte-identical to 3.1.
2. Step 6 pins the verified stack order *before* three entries are added to it, and Steps 13/19/24
   assert each new entry's exact index.
3. Steps 10/16/21 each contain an explicit `enabled=False` / no-rules byte-identical-to-today case.
4. Step 27 asserts the `check`-id set as a literal — Plan 036 can be written against §D-seam's
   table without reading this plan's code.
5. `api_surface.py --check` green with the regenerated snapshot committed.

## Parked

| Item | Why | Un-park trigger |
|---|---|---|
| `RateLimit` / `X-RateLimit-Remaining` headers | The `RateLimiter` ABC cannot report remaining quota, and adding an abstract method breaks out-of-tree implementations (Plan 011 / D-11 analogue) | The design is written: an optional `@runtime_checkable RateLimitIntrospection` Protocol with `remaining(key) -> int`. Un-park when `draft-ietf-httpapi-ratelimit-headers` becomes an RFC (brief 008 Evidence Gap 3) |
| A bounded / LRU key space on `InMemoryRateLimiter` | The right fix for §D-S10-keyspace, but it changes a public `varco_core` class's memory semantics for every existing `@rate_limit` caller | A consumer reports memory growth, or the IP-scope acknowledgement kwarg proves to be the common path rather than the exception |
| Token-bucket limiter (brief 008 §3's recommended default) | varco already ships two sliding-window implementations; a third algorithm is its own row | Burst intolerance is reported as a real problem by a consumer |
| JSON nesting / complexity limits | Brief 008 §2: no framework middleware can enforce nesting depth without the schema; it belongs in Pydantic validators. CVE-2026-0994 is the cited motivation, not a counter-argument | A standardized middleware-level approach appears, or varco grows a schema-aware deserialization layer |
| Per-route body ceilings and per-route CSP | `exempt_paths` covers the real cases without a decorator API; brief 008 Evidence Gap 1 records that no authoritative per-route-CSP-in-FastAPI pattern exists | Two consumers need genuinely different ceilings on two routes of one app |
| `Server` / `X-Powered-By` stripping | `Server` is emitted by uvicorn below the ASGI app; a middleware cannot reliably remove it | varco grows deployment-level server configuration guidance |
| `Permissions-Policy` / COOP / CORP in `BALANCED` | §D-S7-default: CORP actively defeats a configured CORS policy; COOP is inert on JSON | An app serves HTML from a varco route by design |

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| **`X-Frame-Options: DENY` breaks an app that frames its own HTML** | Medium — a real, if uncommon, breakage on upgrade | Loud and immediate (the browser refuses to render); one env var; the top row of the upgrade-note table and of the Pitfalls table |
| **The 10 MiB ceiling silently caps an upload endpoint** | Medium | The 413 body names the ceiling *and* the env var, so it is self-diagnosing; `exempt_paths`; the upgrade note. Deviating from brief 008's opt-in recommendation is argued in §D-S8-default, not assumed |
| ⚠️ **ASSUMPTION — no consumer parses the unmapped-exception `message` field.** Verified in-repo; out-of-repo is an assumption | Medium | It is an error path that was already returning an arbitrary string, so nothing could have parsed it reliably; `### Security` + `### Changed` CHANGELOG entries; `correlation_id` is unchanged and is the supported correlation key |
| ⚠️ **ASSUMPTION — `app.user_middleware` entries expose `.cls`/`.kwargs` and survive startup.** Verified against the pinned Starlette (`.venv/.../starlette/middleware/__init__.py:23-25`, `applications.py:101`) but it is not a documented public API | **High — §D-seam rests on it** | Step 30's explicit shape guard fails loudly on a Starlette upgrade; `inspect_http_edge()` never raises, so the worst runtime outcome is an all-absent posture, which 036 already treats as a soft edge |
| ⚠️ **ASSUMPTION — a `ServiceException` raised from inside a pure-ASGI middleware is caught by `ErrorMiddleware`.** `error.py:157-199` unwraps `BaseExceptionGroup` and dispatches `ServiceException`, so it should be; not yet demonstrated for a *pure ASGI* raiser rather than a route handler | **High — both the 413 and the 429 envelopes depend on it** | Verify empirically at Step 16 **before** building Phase 5 on it. If it does not hold, the fallback is for both middlewares to construct their own `JSONResponse` (the `enabled_error_middleware=False` path they need anyway, per §Edge cases) — not a redesign |
| **Rate limiting registered at the wrong stack position produces a silently useless limiter** (all keys `"-"`, or a 429 with no envelope) | High — it would look shipped and protect little | §D-order is normative and tested (Steps 13/19/24); `create_varco_app` does the placement; the Pitfalls table names `extra_middleware=` as the wrong path, with the verified `app.py:538` evidence |
| **`fail_open=True` means a Redis outage disables enforcement** | Medium — availability chosen over enforcement, deliberately | §D-S10-failopen; ERROR logging; `inspect_http_edge()` reports it; `fail_open=False` inverts it; the standing `@circuit_breaker` advice from `RedisRateLimiter`'s own docstring |
| **An IP-keyed `InMemoryRateLimiter` becomes a memory-exhaustion primitive** | High if reached | §D-S10-keyspace refuses it at construction unless acknowledged; the bounded-keyspace fix is parked with a trigger |
| ⚠️ **ASSUMPTION — `RateLimit-Policy`'s draft-11 syntax will not change before 3.2 ships.** Brief 008 §Version notes says it *"may still undergo significant changes"* and expires 24 Nov 2026 | Low | It is off by default (`emit_draft_headers=False`); nothing in varco parses it; a syntax change is a one-line edit |
| **Adding three entries to a nine-entry stack perturbs an existing one** | Medium | Phase 2's characterization test is written and green **before** any addition, and every subsequent phase re-asserts the full list |
| **Phase 2's two filed ordering questions are read as "Plan 035 broke metrics"** | Low | §D-order-bugs states plainly that the *comments* were wrong and the *positions* are untouched; the BACKLOG rows are phrased as questions with evidence |

## Open questions

1. **Should `SecurityHeadersMiddleware` sit outside or inside `CORSMiddleware`?** Outside would let
   it stamp preflight responses too; inside (chosen) keeps CORS's preflight short-circuit
   untouched, which `app.py:62-63` treats as load-bearing. Decide at Step 11 — lean inside, and
   assert the preflight response shape is unchanged either way.
2. **Does `RateLimitBundle` belong in `varco_fastapi.middleware.rate_limit` or beside
   `RateLimitConfig` in `varco_core.resilience`?** It carries `RateLimitRule`s, which carry a
   `RateLimitScope` that only means anything over HTTP. Decide at Step 23 — lean `varco_fastapi`,
   for the same layer reason the settings classes live there.
3. **Should `http.error.detail_exposed` be `warn` or `info`?** It is today's documented default and
   the string is application-authored, so `warn` may be noisy on every single app until 4.0.
   Decide with Plan 036's planner — this plan emits `warn` and 036 may downgrade the rendering,
   but the `check` id itself is fixed by §D-seam.
