# HTTP edge hardening — security headers, body limits, rate limiting

Plan 035 (BACKLOG 3.2, rows **S3** must/S, **S7** should/S, **S8** should/S, **S10** should/S–M).
Research brief backing S7/S8/S10: `design/research/008-http-hardening-conventions-2026.md`.

One doc for three additions because they share the ordering contract and the `create_varco_app`
keywords — splitting the ordering table into three places would make it drift the way three
already-drifted comments did (§D-order-bugs, below).

## The ordering contract (§D-order) — normative

Starlette's `add_middleware()` prepends (`user_middleware.insert(0, ...)`) — **the last call ends
up outermost**. The verified execution order (outermost → innermost) with every optional
middleware enabled, new Plan 035 entries in **bold**:

```
CORSMiddleware
**SecurityHeadersMiddleware**        (opt-out: security_headers=False)
extra_middleware=[...]               (caller-supplied — OUTSIDE ErrorMiddleware, see below)
ErrorMiddleware
**BodyLimitMiddleware**              (opt-out: body_limit=False)
RequestLoggingMiddleware
TracingMiddleware
MetricsMiddleware                    (INSIDE Tracing — Plan 041 / §D-S17-decision)
**RateLimitMiddleware(stage=PRE_AUTH)**   (opt-in via rate_limit=RateLimitBundle(...))
RequestContextMiddleware             (populates AuthContext / current_tenant())
**RateLimitMiddleware(stage=POST_AUTH)**  (opt-in, same bundle)
LocalizationMiddleware
IdempotencyMiddleware                (opt-in, unchanged — Plan 029)
ProfilingMiddleware
route handler
```

`varco_fastapi.middleware`'s module docstring is the single normative home for this table —
`varco_fastapi/app.py`'s comments and docstring point here instead of restating it, which is
exactly the drift `§D-order-bugs` found and corrected (below).

**Why each new entry sits where it does:**

| Middleware | Position | Why |
|---|---|---|
| `SecurityHeadersMiddleware` | Inside CORS, outside everything else | Never rewrites a CORS preflight response; headers attach to every error response — 404, 500, a mapped `ServiceException`, the new 413/429 — because it wraps `ErrorMiddleware` from outside |
| `BodyLimitMiddleware` | Inside `ErrorMiddleware`, outside `IdempotencyMiddleware` | Its 413 renders through the one error envelope with `correlation_id`; sits outside `IdempotencyMiddleware` (which buffers the whole body via `request.body()`) so an over-limit request is rejected before anything buffers it |
| `RateLimitMiddleware(PRE_AUTH)` | Outside `RequestContextMiddleware` | Rejects an unauthenticated flood **before** any JWT signature verification (and, on a `kid` miss, a JWKS fetch) — only `IP`/`GLOBAL` scopes are legal here |
| `RateLimitMiddleware(POST_AUTH)` | Inside `RequestContextMiddleware` | `SUBJECT`/`TENANT` scopes read `get_auth_context_or_none()`/`current_tenant()`, populated by `RequestContextMiddleware` |

**One `RateLimitMiddleware` class, registered at up to two stack positions** — not one position,
not two classes. A single position cannot satisfy both "reject early, before auth" and "key by
subject/tenant, which needs auth" (brief 008 §3's own named tension). `create_varco_app` partitions
a `RateLimitBundle`'s rules by scope automatically; `RateLimitMiddleware.__init__` itself refuses a
`SUBJECT`/`TENANT` rule at `stage=PRE_AUTH` with a `ValueError` naming the scope, so a
misconfiguration fails at startup, not silently at runtime as an unkeyed `"-"` bucket everyone
shares.

### §D-order-bugs — three comments were wrong; two questions filed, not fixed

Verifying the order above found three in-repo comments/docstrings that contradicted what
`create_varco_app` actually builds (`app.py:537`'s "extra_middleware lands inside ErrorMiddleware"
was backwards; `app.py:500-504` and `middleware/__init__.py:16-25` both claimed
`MetricsMiddleware` sits inside `TracingMiddleware`, also backwards). All three comments were
corrected at the time to state the then-verified reality. **The underlying positions were not
moved by Plan 035** — it was a security release, not a metrics-correctness fix, and moving
`MetricsMiddleware` would have changed observable OTel behaviour for every existing app. Two
BACKLOG rows (`S17`, `S18`) filed the resulting open questions ("is the comment wrong or the
position?", "should `extra_middleware=` land inside `ErrorMiddleware`?") as questions with
evidence, explicitly not resolved by Plan 035.

**`S17` resolved by Plan 041.** A **third** stale comment Plan 035 missed
(`varco_fastapi/varco_fastapi/middleware/metrics.py:262-270`'s "Recommended position" paragraph,
which still recommended `Tracing → MetricsMiddleware` and claimed the tracing context was active
at record time — describing exactly the order Plan 041 went on to create) was found while writing
Plan 041 and corrected. See "Metrics inside tracing (Plan 041 / S17)" below for the decision, its
argument, and the operator-facing consequences. `S18` (`extra_middleware=` position) remains open.

⛔ **Never register a varco edge middleware via `extra_middleware=`.** It is verified to land
**outside** `ErrorMiddleware` (`app.py:538` runs after `:531`) — a `ServiceException` raised there
never renders through the error envelope, and it is outside `RequestContextMiddleware` too, so
`SUBJECT`/`TENANT` keys would always be `"-"`. This is exactly why all three new middlewares get
dedicated `create_varco_app` keywords instead.

## S7 — Security headers (`varco_fastapi.middleware.security_headers`)

**On by default** (`security_headers=None`, the default) at the `BALANCED` preset — four headers,
safe for a JSON API, citing brief 008 §1 for every value:

| Header | Value | Note |
|---|---|---|
| `X-Content-Type-Options` | `nosniff` | No downside for any content type |
| `X-Frame-Options` | `DENY` | Clickjacking — a JSON API is never legitimately framed |
| `Referrer-Policy` | `strict-origin-when-cross-origin` | |
| `Strict-Transport-Security` | `max-age=31536000; includeSubDomains` | **HTTPS only** — scheme-guarded |

`STRICT` (opt-in, `SecurityHeadersSettings(preset=SecurityHeadersPreset.STRICT)`) adds:
`Content-Security-Policy: default-src 'none'; frame-ancestors 'none'`,
`Cross-Origin-Opener-Policy: same-origin`, `Cross-Origin-Resource-Policy: same-origin`,
`Permissions-Policy: geolocation=(), microphone=(), camera=()`.

**Two deliberate omissions from `BALANCED`, argued not overlooked:**

- **CSP.** FastAPI ships `/docs`/`/redoc` loading a script from a CDN — `default-src 'none'`
  breaks them, and the "safer" permissive `default-src 'self'` alternative (secure.py's own
  default) breaks them too (the CDN is not same-origin) while protecting close to nothing on a
  JSON body. A header that is either broken or meaningless is worse than none, so `BALANCED` sends
  none and `STRICT` sends the real one, alongside an `exclude_paths` default covering
  `/docs`/`/redoc`/`/openapi.json`.
- **CORP.** `Cross-Origin-Resource-Policy: same-origin` instructs the browser to block
  cross-origin *reads* of the response — exactly what a CORS-enabled API's `allow_origins` exists
  to permit. Shipping it in `BALANCED` would silently defeat a configured CORS policy for every
  browser caller. `COOP` is inert on JSON (it only matters for top-level documents), so it rides
  into `STRICT` alongside CORP rather than adding a no-op header everywhere.

HSTS is scheme-guarded: sent only when `scope["scheme"] == "https"`, or when
`X-Forwarded-Proto: https` arrives from a peer matching `trusted_proxies` (a tuple of CIDRs,
default empty — the same trust rule §D-S10-ip uses for the rate limiter's `IP` scope, implemented
once in `varco_fastapi.middleware._forwarded` and shared by both). With no configuration, the
header is ignored entirely.

Every header is set with **`setdefault` semantics** — a route or downstream middleware that
already set the header wins.

Deliberately **never sent**: `X-XSS-Protection` (deprecated and harmful — brief 008 §1),
`Server`/`X-Powered-By` (set by uvicorn below the ASGI application; a middleware running inside it
cannot reliably remove them).

## S8 — Request body size limits (`varco_fastapi.middleware.body_limit`)

**On by default** (`body_limit=None`) at **10 MiB**, deviating from brief 008's "ship opt-in to
avoid breakage" recommendation. The deviation is argued: brief 008 §2 states plainly that
*"any Starlette/FastAPI app is vulnerable to memory exhaustion without explicit middleware"* — no
uvicorn/hypercorn/Starlette/FastAPI layer enforces a body ceiling. Shipping the fix disabled would
leave a documented DoS on by default in a security release. The breakage this causes on upgrade is
loud, immediate, and self-describing (a 413 whose body names the ceiling and
`VARCO_BODY_LIMIT_MAX_BYTES`) — categorically different from a silent behaviour change, which is
what the blast-radius rule actually asks for a "cheap caller-side fix" to look like.

**10 MiB is not invented** — brief 008 §2's reference table gives AWS API Gateway a hard 10 MB
limit that a large share of production APIs already live under; nginx's own default is 1 MB, an
order of magnitude stricter than what varco proposes.

**Two checks, both needed** (brief 008 §2): a `Content-Length` pre-check is a cheap early
rejection but is spoofable or absent under chunked transfer-encoding; the real enforcement is a
cumulative count over every ASGI `receive()` message's body bytes, which rejects **before
buffering completes** — the entire reason this has to be ASGI middleware rather than
`BaseHTTPMiddleware` (which cannot intercept `receive()` at all; it can only inspect a body
Starlette has already fully buffered).

`RequestBodyTooLargeError` (`varco_core.exception`, HTTP 413 per RFC 9110 §15.4.14 / RFC 6585 §4)
is a `ServiceException` — raising it renders through the one error envelope with
`correlation_id`, which is why `BodyLimitMiddleware` sits **inside** `ErrorMiddleware` (§D-order).

`exempt_paths` (path-prefix match) and `VARCO_BODY_LIMIT_MAX_BYTES`/`VARCO_BODY_LIMIT_ENABLED`
cover the real upload-endpoint case.

## S10 — HTTP rate limiting (`varco_fastapi.middleware.rate_limit`)

**Off by default** — the one opt-in row (`rate_limit=None`). Pass a `RateLimitBundle` to
`create_varco_app` to turn it on:

```python
from varco_core.resilience.rate_limit import InMemoryRateLimiter, RateLimitConfig
from varco_fastapi.middleware.rate_limit import RateLimitBundle, RateLimitRule, RateLimitScope

bundle = RateLimitBundle(
    rules=(
        RateLimitRule(scope=RateLimitScope.IP, limiter=InMemoryRateLimiter(RateLimitConfig(rate=100, period=60.0)), name="ip"),
        RateLimitRule(scope=RateLimitScope.TENANT, limiter=InMemoryRateLimiter(RateLimitConfig(rate=1000, period=60.0)), name="tenant"),
    ),
)
app = create_varco_app(container, rate_limit=bundle, ...)
```

**No new rate limiter, no new algorithm.** `RateLimiter`/`RateLimitConfig`/`InMemoryRateLimiter`
(`varco_core.resilience.rate_limit`) and `RedisRateLimiter` (`varco_redis.rate_limit`) are used
as-is — this module is the ASGI assembly around them and nothing else.

### Scope/stage model

| Scope | Legal stage(s) | Key | Missing value |
|---|---|---|---|
| `GLOBAL` | Either | `ratelimit:global:{name}` | n/a |
| `IP` | Either | `ratelimit:ip:{name}:{client_ip}` | No resolvable peer → skip the rule, WARN (throttled) |
| `SUBJECT` | `POST_AUTH` only | `ratelimit:subject:{name}:{user_id}` | Anonymous → skip (covered by the `IP` rule, not a shared `"-"` bucket) |
| `TENANT` | `POST_AUTH` only | `ratelimit:tenant:{name}:{tenant_id}` | No ambient tenant → skip, WARN (throttled) |

A `SUBJECT`/`TENANT` rule at `stage=PRE_AUTH` raises `ValueError` at construction — auth has not
run yet, so any such key would be `"-"` for everyone. **An unkeyable rule is skipped, never
fail-closed** — a shared `"-"` bucket for every anonymous/untenanted caller is worse than no limit
at all (one client can exhaust it for everyone), unlike `tenancy_cache_key()`'s fail-closed rule,
where the failure mode is a cross-tenant *leak*, not present here.

### Trusted-proxy IP resolution (§D-S10-ip)

The client IP is `scope["client"][0]` unless the immediate peer matches `trusted_proxies` (a tuple
of CIDRs, default empty) — only then is `X-Forwarded-For` consulted, skipping
`trusted_proxy_hops` entries from the right. Brief 008 §3 names the bypass this closes directly:
*"attacker sends `X-Forwarded-For: 192.0.2.1` → limiter thinks requests are from different IPs,
bypassing limits"*. With no configuration, the header is ignored entirely.

### An IP/SUBJECT-scoped `InMemoryRateLimiter` is refused unless acknowledged (§D-S10-keyspace)

`InMemoryRateLimiter`'s own DESIGN block warns its per-key lock/window dicts "grow unboundedly"
with bounded key spaces recommended. `IP`/`SUBJECT` keys are attacker-controlled — each new
source address or subject permanently costs a `deque` + `asyncio.Lock`. Constructing such a rule
with an `InMemoryRateLimiter` raises `ValueError` unless
`acknowledge_unbounded_keyspace=True` is passed — the same "explicit acknowledgement kwarg" shape
`mount_tenant_admin(acknowledge_bundled_admin=True)` already uses for a footgun varco will not
remove. `RedisRateLimiter` is unaffected (its sorted sets carry a TTL). `TENANT`/`GLOBAL` scopes
are not gated — their key spaces are operator-controlled, not attacker-controlled.

This guard applies uniformly, whether `RateLimitMiddleware` is hand-registered
(`app.add_middleware(RateLimitMiddleware, ...)`) or assembled via
`create_varco_app(rate_limit=RateLimitBundle(...))`. `RateLimitBundle` itself carries an
`acknowledge_unbounded_keyspace: bool = False` field — the caller's own explicit opt-in, forwarded
verbatim to both `RateLimitMiddleware` instances `create_varco_app` builds from the bundle. An
`IP`/`SUBJECT` rule backed by a plain `InMemoryRateLimiter` raises `ValueError` unless
`RateLimitBundle(..., acknowledge_unbounded_keyspace=True)` is passed — exactly as it would for a
hand-registered instance.

⚠️ **Not at `create_varco_app()` call time.** Starlette's `add_middleware()` only stores a
deferred `Middleware(cls, **kwargs)` descriptor — the `RateLimitMiddleware` instance, and
therefore this guard, is only actually constructed on the first ASGI call
(`build_middleware_stack()`), for both the hand-registered and the `create_varco_app` form. A
misconfigured bundle will not raise from `create_varco_app()` itself; it raises on the first
request the app receives (including a `TestClient`/`AsyncClient` call in a test, or a load
balancer's first health-check hit in production — in practice close to, but not literally,
"startup"). See `varco_fastapi/tests/test_rate_limit_middleware.py`'s
`_first_request` helper for the mechanics.

### Fail-open, loudly, by default (§D-S10-failopen)

A limiter exception (`RedisError`, a timeout, anything) is caught, logged at ERROR (throttled to
once per `error_log_interval` per rule), and the request is **allowed**.
`RateLimitSettings(fail_open=False)` (or `VARCO_RATE_LIMIT_FAIL_OPEN=false`) inverts it to 503. A
rate limiter is a protection against abuse, not a correctness invariant — losing it for a Redis
outage's duration is a smaller incident than a total outage. Pair a `RedisRateLimiter` with
`@circuit_breaker`, per its own docstring's standing recommendation.

### Response shape and standards status

On denial: `429`, `Retry-After: <ceil(seconds)>` **always** (RFC 6585 / RFC 9110 §10.2.3 — stable).
`RateLimit-Policy` (the IETF draft-11 structured field, computable from `RateLimitConfig` alone)
is emitted **only** with `emit_draft_headers=True` (default `False`) — brief 008's §Version notes
record the draft *"expires 24 November 2026"* and *"may still undergo significant changes"*, too
unstable to ship on by default in a framework. **`RateLimit` and every `X-RateLimit-*` header are
never emitted, under any setting** — the `RateLimiter` ABC has no `remaining()` method, and adding
one would break every out-of-tree implementation (the same rule that kept `BulkCache` off
`AsyncCache`, Plan 011 / D-11). A header that lies about remaining quota is worse than an absent
one; the parked design (an optional `RateLimitIntrospection` Protocol) is recorded in the plan's
Parked table.

`RateLimitMiddleware` constructs its own 429/503 `JSONResponse` directly rather than raising
through `ErrorMiddleware` — the extra headers above (`Retry-After`, `RateLimit-Policy`) have
nowhere to hook into a generic exception-dispatch path, and this way the middleware behaves
identically whether or not `ErrorMiddleware` is present in the stack. `correlation_id` is still
populated (ambient if set, freshly generated otherwise — the same fallback `ErrorMiddleware` uses
for its own unmapped-exception path, §D-S3a).

## S3 — the error-response leak, and `include_detail`

Two paths echoed `str(exc)` into the response body: `ErrorMiddleware._service_error_response`'s
fallback (`error.py`) and `add_exception_handlers`'s fallback (`exceptions.py`) — reached only
when `error_message_for()` itself raises (e.g. `exc.error_params()` raises), **not** by an
unmapped `DBAPIError`/`OSError` as an earlier BACKLOG wording suggested (those are caught earlier
and already rendered by the sanitized `_internal_error_response()`). Both sites now return
`{"code": ..., "message": "An internal error occurred."}` plus `correlation_id`, and log the
exception type server-side at ERROR with `exc_info=True`.

`ErrorEnvelopeSettings.include_detail` (default `True`, byte-identical to pre-3.2) is a **separate,
warn-only** knob for a **different** echo — `ErrorMessage.detail`, populated unconditionally for
every `ServiceException` and deliberately kept because it is the only actionable channel for
`RouteGuard` denial messages. `inspect_http_edge()` reports it as a `warn`-severity
`http.error.detail_exposed` finding — 4.0's flip candidate, not 3.2's.

## Metrics inside tracing (Plan 041 / S17)

`MetricsMiddleware` now records `http.server.request.duration`/`http.server.active_requests`/
`http.server.request.body.size` **INSIDE** `TracingMiddleware` (§D-S17-decision) — every HTTP
server metric is recorded with a live, sampled span current in OTel context, which is what makes
an OTel exemplar on the duration histogram possible at all (the SDK's default
`TraceBasedExemplarFilter`, unconfigured and unchanged, attaches an exemplar only when a sampled
span is current). `varco_fastapi/varco_fastapi/router/metrics.py` has sold this capability in its
OpenMetrics-negotiation docstring since it shipped; before this plan varco could not deliver it.

**Before/after** (§D-S17-histogram):

| | Before (metrics outer) | After (metrics inner) |
|---|---|---|
| `http.server.request.duration` covers | `TracingMiddleware`'s own overhead **+** everything inner | everything inner only |
| Exemplars | Never attached — no span is current at record time | Attached when the request's span is sampled |
| Attribute set | `http.request.method`, `http.route`, `http.response.status_code` | Unchanged — not a cardinality or schema change |

**What this means for an operator upgrading to 3.2**: `http.server.request.duration` no longer
includes `TracingMiddleware`'s own overhead, so every latency series takes a small, one-time
**downward** step at upgrade (brief 012 §5 puts ASGI tracing instrumentation overhead at <2% of
request time, and varco's `TracingMiddleware` does strictly less than the reference
instrumentation it was measured against). Nothing about the attribute set changed, so no dashboard
query needs rewriting. HTTP metrics gaining exemplars is new capability, not a regression.

With `enable_tracing=False`, metrics still record exactly as before — no span, no exemplar, no
error, no warning. The dependency is one-directional and degrades to pre-3.2 behaviour.

**Pitfalls**

| Pitfall | Why it happens | Fix |
|---|---|---|
| A latency alert with an absolute threshold fires (or a trained anomaly detector flags a drop) right after upgrading to 3.2 | `http.server.request.duration` steps down once, permanently, because it no longer double-counts `TracingMiddleware`'s own overhead | Expected, one-time; re-baseline the alert/detector rather than treating it as a regression |

## The seam Plan 036 consumes — `inspect_http_edge()`

`varco_fastapi.middleware.introspect.inspect_http_edge(app) -> HttpEdgePosture` is a pure,
side-effect-free read over `app.user_middleware` — **not itself a preflight**. It never raises
(an unrecognised `app` object returns an all-absent posture) and never warns at startup. See the
module's own docstring for the full `check`-id table (`http.security_headers.absent`,
`http.rate_limit.no_pre_auth_rule`, `http.error.debug_enabled`, …) — Plan 036 may render, group,
or escalate these; it must not redefine them.

## Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| `X-Frame-Options: DENY` breaks an app that frames its own HTML | `BALANCED` sends `DENY` by default | `SecurityHeadersSettings(x_frame_options="SAMEORIGIN")` or `VARCO_SECURITY_HEADERS_X_FRAME_OPTIONS=SAMEORIGIN` |
| HSTS silently absent behind a TLS-terminating proxy | `X-Forwarded-Proto` is ignored until the peer is a configured trusted proxy | Set `trusted_proxies` — but this **also locks you out of a custom local domain** the moment you set it broadly (brief 008 §1) |
| `STRICT` CSP breaks `/docs`/`/redoc` | `default-src 'none'` blocks the Swagger UI CDN script | The `exclude_paths` default already covers this — do not remove those three paths |
| A 10 MiB ceiling silently caps an upload endpoint | `BodyLimitMiddleware` is on by default | `exempt_paths=("/upload",)` or raise `VARCO_BODY_LIMIT_MAX_BYTES` |
| `InMemoryRateLimiter` under-limits nothing but over-multiplies | Per-process — the effective limit is `rate × replica count` | Use `RedisRateLimiter` before scaling past one replica |
| Forgetting `trusted_proxies` behind an ingress | Every request keys on the ingress IP — the **whole cluster** shares one budget | Set `VARCO_RATE_LIMIT_TRUSTED_PROXIES` before deploying behind an ingress |
| `fail_open=True` masks a Redis outage | The default trades enforcement for availability | Pair `RedisRateLimiter` with `@circuit_breaker`; flip `fail_open=False` if enforcement must win |
| A `SUBJECT`-only rule set leaves anonymous traffic unlimited | `inspect_http_edge()` reports `http.rate_limit.no_pre_auth_rule` | Add an `IP`/`GLOBAL` rule at `stage=PRE_AUTH` |
| Registering any of the three via `extra_middleware=` | Verified wrong position (§D-order-bugs) — outside `ErrorMiddleware`/`RequestContextMiddleware` | Use the dedicated `security_headers=`/`body_limit=`/`rate_limit=` keywords |
| `enable_error_middleware=False` with `body_limit`/`rate_limit` on | Both middlewares are self-contained for this case — `RateLimitMiddleware` always sends its own 429/503 response directly; `BodyLimitMiddleware` self-renders a plain JSON 413 (via the same shared `send_json_error` helper) when constructed with `has_error_middleware=False`, which `create_varco_app` passes automatically from its own `enable_error_middleware` value | No action needed — both middlewares degrade gracefully with `ErrorMiddleware` absent; the 413/429 still carry `code`/`message`/`correlation_id` |
| `create_varco_app(rate_limit=RateLimitBundle(...))` raises `ValueError` for an unbounded `InMemoryRateLimiter` keyspace | §D-S10-keyspace's guard fires uniformly for hand-registered and bundle-assembled `RateLimitMiddleware` alike | Pass `RateLimitBundle(..., acknowledge_unbounded_keyspace=True)` to accept it for a single-process deployment, or use `RedisRateLimiter` for `IP`/`SUBJECT` scopes |

## See also

- README's "Security headers", "Request body limits", and "HTTP rate limiting" sections for
  runnable snippets and the full `VARCO_SECURITY_HEADERS_*`/`VARCO_BODY_LIMIT_*`/
  `VARCO_RATE_LIMIT_*` env-var reference tables.
- `technical_docs/features/error-taxonomy-and-i18n.md`'s S3 subsection for the `include_detail`
  knob.
- `design/research/008-http-hardening-conventions-2026.md` — the research brief backing S7/S8/S10.
- `plans/035-http-edge-hardening.md` — the design plan (§D-order, §D-S3, §D-S7-default,
  §D-S8-default, §D-S10-shape/ip/keyspace/failopen/algorithm/headers, §D-seam).
