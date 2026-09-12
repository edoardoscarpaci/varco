# Plan 041 — `MetricsMiddleware` ordering decision (S17) and a JWKS background refresher (S22)

Covers BACKLOG 3.2 extension rows **S17** (🟡 should, S — *decide `MetricsMiddleware`'s position
relative to `TracingMiddleware`*) and **S22** (🟡 should, S — *JWKS background refresh task for
`TrustedIssuerRegistry`*).

Grouped because both are **`varco_fastapi` application-assembly** concerns and nothing else: one
moves a single `add_middleware()` call in the middleware stack, the other adds a single lifespan
component. They share no code and are sequenced as two independently mergeable phases.

**Research brief backing S17:**
[`design/research/012-otel-exemplars-and-metrics-span-context.md`](../design/research/012-otel-exemplars-and-metrics-span-context.md),
written for this plan. Every externally-grounded claim below cites it as `brief 012 §N`. Where this
plan **corrects** the brief (its §4 describes a layout that is not varco's, and gets the direction
of the latency change backwards for us) it says so explicitly and argues it (§D-S17-histogram) —
the deviation style of `plans/035-http-edge-hardening.md` §D-S7-default.

**There is no brief for S22** and none is owed: it is entirely repo-internal, built on four shipped
lifecycle precedents and one shipped `varco_core` refresh primitive. Every claim below carries a
`file:line`.

## Scope and siblings

One of five plans covering the 3.2 extension rows (`S17`, `S19`–`S23`).

| Plan | Rows | Boundary with this plan |
|---|---|---|
| 038 | S19 — inbound webhook verification | ✅ *planned*. Chose a **route dependency, not a middleware**, partly so it would not need a position in this plan's table; made "zero diff in Plan 041's files" a DoD item (`plans/038-inbound-webhook-verification.md:721,747`). **Reciprocated**: this plan touches no file 038 touches, and adds no route dependency |
| 039 | S20 — retention & purge automation | ✅ *planned*. ⚠️ **Real adjacency** — it adds `RetentionLifecycle` and a `create_varco_app(retention=...)` keyword. It already recorded that ⛔ **`varco_fastapi/varco_fastapi/lifespan.py` is off-limits for restructuring by both 039 and 041** (`plans/039-retention-and-purge-automation.md:519-523`). **Honoured**: §D-S22-lifecycle adds a component satisfying the existing `AbstractLifecycle` Protocol (`varco_fastapi/varco_fastapi/lifespan.py:73-90`) and registers it through the existing path. `lifespan.py` is **not modified by this plan**. The only shared file is `app.py`'s keyword list, where 039 adds `retention=` and this plan adds `jwks_refresh=` — a two-line rebase, not a semantic collision |
| 040 | S21 — unified redaction seam | Planned in parallel; **not present in `plans/` at the time of writing**. It touches `varco_fastapi/varco_fastapi/middleware/logging.py` (redaction *inside* a middleware). ⚠️ Boundary stated and checked in §D-S17-blast: `RequestLoggingMiddleware`'s position is **unchanged** by this plan — it is outside both `MetricsMiddleware` and `TracingMiddleware` before and after (`varco_fastapi/varco_fastapi/middleware/__init__.py:26-28`), so what a logging middleware sees does not change. Only `Metrics` and `Tracing` swap with each other |
| 042 | S23 — conformance-guard recovery | testkit + CLAUDE.md only. No overlap |

**Phase 1 (S17) is independently mergeable and must merge first** — it is a two-block move with an
observability blast radius that deserves its own review, exactly as `plans/035` made its S3 phase
mergeable ahead of the rest (`plans/035-http-edge-hardening.md:27-28`).

## Goal

`MetricsMiddleware` executes **inside** `TracingMiddleware`, so every HTTP server metric varco
records is recorded with a live, sampled span in the current OTel Context — which is what makes an
exemplar possible at all, and what `varco_fastapi/varco_fastapi/router/metrics.py:20-25,45-51`
already promises operators. The position is a *decision* with a written argument, not an accident
with a corrected comment.

`TrustedIssuerRegistry` can refresh its JWKS caches on a timer without ever receiving a `verify()`
call, via `start_refresh()`/`stop_refresh()` on the registry itself plus a thin
`JwksRefreshLifecycle` wired by `create_varco_app(jwks_refresh=...)`. The CLAUDE.md ⚠️ that says no
such task exists is **deleted and replaced**, because it is no longer true.

## Non-goals

- **No merged tracing+metrics middleware.** The reference implementations do it in one middleware
  (brief 012 §5); varco will not (§D-S17-shape). `enable_tracing` and `enable_metrics` are separate
  `create_varco_app` keywords (`varco_fastapi/varco_fastapi/app.py:583,593`) and both classes are
  separately exported and separately registerable.
- **No other reordering.** `SecurityHeaders`, `extra_middleware`, `Error`, `BodyLimit`,
  `RequestLogging`, both `RateLimit` stages, `RequestContext`, `Localization`, `Idempotency` and
  `Profiling` keep their exact positions. Only `MetricsMiddleware` and `TracingMiddleware` swap.
- **No fix for the metrics/error status-code mismatch.** `MetricsMiddleware` records
  `http.response.status_code="500"` for any exception that reaches it
  (`varco_fastapi/varco_fastapi/middleware/metrics.py:368,386`) even when `ErrorMiddleware`
  — which is *outside* it, and stays outside it — renders a 404/409/422. Pre-existing, **unchanged
  by this plan**, filed as a BACKLOG row (§BACKLOG entries).
- **No new middleware, no route dependency, no `extra_middleware=` involvement.** CLAUDE.md's
  standing rule (never register `SecurityHeaders`/`BodyLimit`/`RateLimit` via `extra_middleware=`)
  is untouched: this plan adds no keyword to that family and moves nothing across
  `ErrorMiddleware`.
- **No new runtime dependency, and no dependency-floor bump.** `varco_core/pyproject.toml:40-42`
  declares `opentelemetry-sdk>=1.20`; exemplars landed in SDK 1.28.0 (brief 012 §2). The floor is
  **not** raised (§D-S17-sdkfloor) — the exemplar assertion is capability-guarded instead.
- **No `MeterProvider`/`ExemplarFilter` configuration change.**
  `varco_core/varco_core/observability/di.py:418` builds `MeterProvider(resource=…,
  metric_readers=…)` with no `exemplar_filter=`, so the SDK default `TraceBasedExemplarFilter`
  applies (brief 012 §2). That default is correct and stays.
- **No background refresher on by default.** `VARCO_JWKS_TTL_SECONDS` defaults to `0.0`
  (`varco_core/varco_core/authority/registry.py:227-231`) and continues to mean "off"
  (§D-S22-interval).
- **No third JWKS knob.** No `VARCO_JWKS_REFRESH_SECONDS`. The refresher's period is derived from
  the two knobs that already exist (§D-S22-interval).
- **No `lifespan.py` change.** Held to 039's undertaking (`plans/039-…:519-523`).
- **No new conformance module and no `COVERAGE.md` row owed.** Neither row implements one of the
  eight `varco_core` ABCs `testkit/varco_conformance` covers. Stated so the absence is a decision.
- **No `SecurityPosture` harness wiring.** Plan 036 owns the harness
  (`varco_fastapi/varco_fastapi/posture.py:258`). This plan exports one pure inspector and files a
  BACKLOG row for the wiring — the identical split 039 §D-S20-posture used.

---

## Design

### What already exists — verified against source while writing this plan

Both rows were audited the way 038 and 039 audited theirs. **S17's premise is correct and slightly
understated; S22's premise is correct and slightly overstated.** Details below.

| Fact | Location | Consequence |
|---|---|---|
| `TracingMiddleware` is added at `:584`, `MetricsMiddleware` at `:599`; `add_middleware()` prepends, so **metrics is outermost** | `varco_fastapi/varco_fastapi/app.py:582-604` | S17's premise confirmed |
| The normative table records the same: `MetricsMiddleware` on the line *above* `TracingMiddleware` | `varco_fastapi/varco_fastapi/middleware/__init__.py:27-28` | Correct today; Phase 1 swaps two lines |
| ⚠️ **A third stale comment Plan 035 did not correct** — `MetricsMiddleware`'s own class docstring still recommends `CORS → Error → Tracing → MetricsMiddleware → …` and claims *"the OTel tracing context is active … when metrics are recorded"* | `varco_fastapi/varco_fastapi/middleware/metrics.py:262-270` | **S17 is wider than the row says.** The row says Plan 035 "corrected the two comments"; there are three, and the uncorrected one describes exactly the order this plan is about to create. Phase 1 rewrites it as a pointer, not a restatement (one-home rule) |
| ⚠️ **varco advertises a capability it structurally cannot deliver** — the metrics router's docstring sells OpenMetrics negotiation because *"OpenMetrics exemplars link metrics to traces — Grafana can show the trace for any latency spike"* | `varco_fastapi/varco_fastapi/router/metrics.py:20-25,45-51` | The decisive repo-internal argument for S17, stronger than brief 012's generic one (§D-S17-decision) |
| varco configures **no** `ExemplarFilter`, so the spec default `TraceBasedExemplarFilter` is in force | `varco_core/varco_core/observability/di.py:418`; brief 012 §2 | Exemplars are *enabled*; they are simply unreachable. The scout's "verify whether varco configures exemplars at all" check resolves in favour of the move, not against it |
| `TracingMiddleware`'s span is still current inside `call_next` — the `with tracer.start_as_current_span(...)` block wraps the `await call_next(request)` at `:187` and closes at `:196` | `varco_fastapi/varco_fastapi/middleware/tracing.py:156-196` | Moving metrics inside tracing puts `record()` inside the span context. Verified, not assumed |
| ⚠️ **The refresh primitive S22 needs already exists** — `_refresh_all_sources()` gathers every source's `refresh()` concurrently with `return_exceptions=True`, commits only successes, silently keeps the last-good keyset per entry, and stamps both timestamps | `varco_core/varco_core/authority/registry.py:532-551` | **S22 is narrower than the row implies.** No refresh logic, no keep-last-good logic, and no per-issuer failure isolation needs writing. The missing piece is a timer and a start/stop pair — roughly 60 lines |
| `JwksUrlSource.refresh()` is itself rate-limited and returns the cached keyset without touching the network inside `min_refresh_interval` (default 10 s); on a fetch failure it returns stale cache and only raises when there is no cache at all | `varco_core/varco_core/authority/sources/jwks_url.py:162-193` | Decides the retry question (§D-S22-failure) with source evidence rather than preference |
| `TrustedIssuerRegistry` uses `__slots__` | `varco_core/varco_core/authority/registry.py:151-159` | New attributes must be added to `__slots__` or they raise `AttributeError` |
| `AbstractLifecycle` is a `runtime_checkable` Protocol (`start`/`stop`), and `register()` `isinstance`-checks against it | `varco_fastapi/varco_fastapi/lifespan.py:73-90,178-183` | Naming the registry's methods `start`/`stop` would silently make a `TrustedIssuerRegistry` satisfy it (§D-S22-seam) |
| `_collect_lifecycle_components()` resolves an **explicit allowlist** of types, not a Protocol scan | `varco_fastapi/varco_fastapi/app.py:861-884` | Honest correction to the obvious worry: adding `start`/`stop` to the registry would **not** cause auto-registration. §D-S22-seam's argument stands on other grounds |
| The canonical background-task shape in `varco_core`: `stop_event` + `create_task` in `start()`; `set()` → `cancel()` → `await` → swallow `CancelledError` in `stop()`, both idempotent | `varco_core/varco_core/watch/base.py:173-191` | Copied verbatim in shape by §D-S22-loop |
| `ReloadingTrustStore` owns its own `start()`/`stop()` in `varco_core`; `varco_fastapi` merely calls them | `varco_core/varco_core/tls/reload.py:138-155` | The prior that puts the loop in `varco_core` (§D-S22-seam) |
| The four shipped lifecycle shapes: `startup`/`shutdown` + `start`/`stop` aliases, container resolution with a `LookupError` that names the missing interface, appended (not prepended) in `create_varco_app` | `varco_fastapi/varco_fastapi/reliability.py:59-139`; `varco_fastapi/varco_fastapi/posture.py:258-397`; `app.py:367-378,394-395,411-422` | The house shape §D-S22-lifecycle follows |
| `inspect_revocation_posture()` — pure read, never raises, takes its objects as arguments, exported from its own subpackage and **not** from `varco_core`'s top level | `varco_core/varco_core/revocation/posture.py:1-17,31,72-99` | The exact template for §D-S22-posture |
| The locked SDK is 1.40.0; the declared floor is `>=1.20` | `uv.lock:1831-1832`; `varco_core/pyproject.toml:41` | CI has exemplars; a floor-compliant install might not (§D-S17-sdkfloor) |

---

### §D-S17-decision — **move `MetricsMiddleware` inside `TracingMiddleware`**

| ID | Choice | Consequence |
|---|---|---|
| D-S17-decision | Swap the two `add_middleware` blocks in `create_varco_app` so `TracingMiddleware` is registered **last of the two** and therefore ends up **outer**. Resulting order: `… RequestLogging → Tracing → Metrics → RateLimit(PRE_AUTH) → RequestContext → …` | HTTP metrics are recorded with a sampled span current; exemplars become possible. One block moves; nothing else does |

This follows brief 012's Librarian's Note. The plan still owes the table and the caveat, so:

✅ **The repo argument is stronger than the brief's.** `router/metrics.py:20-25,45-51` sells the
   OpenMetrics content-type negotiation *specifically* on exemplars ("Grafana can show the trace
   for any latency spike directly from a dashboard panel"). Today no HTTP-server metric varco
   records can ever carry one, because none is recorded inside a span. A framework that documents
   a benefit it structurally cannot produce is worse than one that never mentioned it.
✅ **The default filter is already the right one, and it is being wasted.**
   `observability/di.py:418` passes no `exemplar_filter=`, so `TraceBasedExemplarFilter` is in
   force (brief 012 §2). Under it, "no active sampled span" means **no exemplar at all** — the
   value is recorded, the linkage is dropped (brief 012 §3). Nothing needs configuring; the span
   just needs to exist.
✅ **Both reference implementations do it.** `opentelemetry-instrumentation-asgi` and
   `-fastapi` record the duration histogram in a `finally` block **with the span still current**
   (brief 012 §5). This is the strongest available prior art and there is no dissenting
   implementation in the brief.
✅ **The move is mechanically trivial and reversible**: two blocks swap in one function; the
   characterization test at `varco_fastapi/tests/test_middleware_order.py:27-37` is the decision's
   permanent record.
❌ **The duration histogram's measured span changes.** Argued in full at §D-S17-histogram; it is a
   dashboard concern, not a correctness one, and it is what the CHANGELOG owes an operator.
❌ **`MetricsMiddleware` now depends on `TracingMiddleware` for its most valuable property.** With
   `enable_tracing=False`, metrics still record perfectly and still carry no exemplar — the
   pre-plan behaviour. Accepted and documented: the dependency is one-directional and degrades to
   exactly today's behaviour.
❌ **A skipped path is now skipped inside the span.** `skip_paths` (`metrics.py:314,350-353`)
   short-circuits `/metrics` and `/health`; those requests were previously not even entered by
   metrics before tracing ran. After the move a `/health` request still creates a span (tracing is
   outer) and still records no metric — **unchanged in both dimensions**, because tracing has no
   skip list today and did not gain one.

**Rejected — keep it outside and record the reason as an Answered decision.** ❌ The only argument
for it is "separation of concerns" (brief 012's Options table), which is an implementation-
convenience argument; the brief's own note says the decision belongs with user value. ❌ It
requires either deleting the exemplar promise from `router/metrics.py` or leaving it false. ❌ It
leaves varco the only implementation in evidence that records HTTP metrics outside span context.
There is no operator benefit on this side of the table.

### §D-S17-histogram — what the histogram measures afterwards, and what the CHANGELOG owes

⚠️ **Brief 012 §4 gets the direction backwards for varco**, because it assumes a layout where the
span is created before metrics start timing. varco's layout is the opposite today: metrics is
**outer**, so `time.perf_counter()` (`metrics.py:365,376`) currently brackets *all* of
`TracingMiddleware` — W3C context extraction (`tracing.py:138-139`), tracer lookup, span creation,
seven `set_attribute` calls, the auth-context read, and span close.

| | Today (metrics outer) | After (metrics inner) |
|---|---|---|
| `http.server.request.duration` covers | tracing middleware overhead **+** everything inner | everything inner only |
| Change at upgrade | — | every latency series **steps down** by the per-request tracing overhead |

Magnitude: brief 012 §5's cited overhead study puts ASGI tracing instrumentation at <2% of request
time; varco's `TracingMiddleware` does strictly less than the reference one. So the step is small
and **downward**, not upward as the brief's §4 framing would suggest.

✅ It is also the *more* semantically defensible number: OTel's HTTP semconv says the metric value
   SHOULD equal the HTTP server span duration (brief 012 §4), and after the move the metric covers
   a strict subset of the span rather than a superset of it.
❌ Any alert with an absolute latency threshold, and any anomaly detector trained on the old
   series, sees a step change at upgrade.
❌ Percentile comparisons across the upgrade boundary are not apples-to-apples for one window.

**What the CHANGELOG owes an operator** (Phase 1, Step 6) — three sentences, not a footnote:
1. `http.server.request.duration` no longer includes `TracingMiddleware`'s own overhead; expect a
   small, one-time **downward** step in every latency series at upgrade.
2. Attribute sets are **unchanged** — `http.request.method`, `http.route`,
   `http.response.status_code` (`metrics.py:382-387`) — so this is **not** a cardinality or schema
   change, and no dashboard query needs rewriting.
3. HTTP metrics now carry OTel exemplars when a sampled span exists, which is new capability, not
   a regression.

### §D-S17-shape — two middlewares, not one; and no manual context attach

| Option | Verdict |
|---|---|
| **Move the existing `add_middleware` call** (chosen) | ✅ One block moves. ✅ Both classes stay separately exported (`middleware/__init__.py:71`) and separately toggled (`enable_tracing`/`enable_metrics`, `app.py:583,593`). ✅ Zero new API surface, so `scripts/api_surface.py --check` stays quiet for Phase 1 |
| **One merged `ObservabilityMiddleware`** (the reference-implementation shape, brief 012 §5) | ❌ Collapses two independent `create_varco_app` toggles into one; an app with `enable_tracing=False, enable_metrics=True` has no answer. ❌ Removing or repurposing `MetricsMiddleware`/`TracingMiddleware` is an `__all__` break and a hard `api-check` gate failure for an "S" row. ❌ Over-engineering: stack ordering already delivers the *only* property the merge would buy (a current span at `record()` time) |
| **Keep the order; have `MetricsMiddleware` attach the ambient OTel context itself** | ❌ Reimplements, with `context.attach()`/`detach()` it does not own, what one line of ordering gives free. ❌ It cannot work: outside `TracingMiddleware` there is no span to attach — it would have to *create* one, which is `TracingMiddleware`'s job |

### §D-S17-blast — what else depends on the current order (enumerated, with evidence)

| Question | Answer | Evidence |
|---|---|---|
| Are `ErrorMiddleware`-rendered errors counted? | Yes — but as `status_code=500` regardless of the rendered status, because `ErrorMiddleware` is **outside** metrics before *and* after | `app.py:636-642` (error added after metrics ⇒ outer); `metrics.py:368,375-388` |
| Does this plan change that? | **No.** Error/Metrics relative order is untouched. Filed, not fixed | §Non-goals; §BACKLOG entries |
| Are `BodyLimitMiddleware` 413s counted or traced? | **Neither**, before or after — body-limit sits at position 5, outside both | `app.py:617-634`; `middleware/__init__.py:25-28` |
| Does a rate-limit 429 get a span, and is it counted? | **Both**, before and after — `PRE_AUTH` (`app.py:561-580`) and `POST_AUTH` (`app.py:542-555`) are both inside tracing *and* inside metrics in both layouts | `middleware/__init__.py:29,32` |
| Does `RequestLoggingMiddleware`'s view change? (⚠️ Plan 040's boundary) | **No.** It is outside both middlewares before and after; only Metrics and Tracing swap with each other | `middleware/__init__.py:26-28`; `app.py:606-615` |
| Does `inspect_http_edge()` depend on indices? | **No** — it reads `app.user_middleware` by class identity, never by position | `varco_fastapi/varco_fastapi/middleware/introspect.py:216` |
| Does `_collect_lifecycle_components()` or any DI scan care? | **No** — middleware is never resolved from the container | `app.py:861-884` |
| What breaks in tests? | Exactly one list: `_PHASE_2_BASELINE_ORDER` at `varco_fastapi/tests/test_middleware_order.py:32-33`. Every Plan-035 test compares against it by slice, so swapping those two entries fixes all of them at once | `test_middleware_order.py:112-125,130+` |

### §D-S17-sdkfloor — capability-guard the exemplar assertion; do **not** raise the floor

| ID | Choice | Consequence |
|---|---|---|
| D-S17-sdkfloor | `varco_core/pyproject.toml:41`'s `opentelemetry-sdk>=1.20` is **unchanged**. The ordering assertion runs unconditionally; the exemplar assertion is skipped when the installed SDK cannot produce exemplars | CI (SDK 1.40.0, `uv.lock:1831-1832`) runs the real assertion; a floor-compliant 1.20 install still passes |

✅ Raising a runtime-dependency floor to make a test assertion unconditional is backwards — the
   floor exists for consumers, not for our test suite.
✅ The property the plan is actually buying (metrics recorded inside span context) is provable by
   the ordering test alone; the exemplar assertion is corroboration.
❌ A 1.20-era environment silently skips the strongest test. Accepted: `uv.lock` pins 1.40.0, so
   the skip cannot happen in CI, and the skip message names the reason.

### §D-S22-seam — `start_refresh()`/`stop_refresh()` on the registry, driven by a thin lifecycle

| ID | Choice | Consequence |
|---|---|---|
| D-S22-seam | The loop lives on `TrustedIssuerRegistry` in `varco_core`, named `start_refresh()`/`stop_refresh()` — **not** `start()`/`stop()`. `varco_fastapi` contributes only a thin `JwksRefreshLifecycle` that calls them | Portable and testable with no FastAPI; no accidental Protocol conformance |

✅ **The `ReloadingTrustStore` prior applies directly**: it owns its own `start()`/`stop()` in
   `varco_core` (`varco_core/varco_core/tls/reload.py:138-155`) and `varco_fastapi` merely calls
   them. Refreshing a keyset is portable behaviour — a `varco_core`-only app (a worker, a CLI, a
   consumer) needs it just as much as a FastAPI app, and putting the loop in `varco_fastapi` would
   deny it to them.
✅ **The primitive is already there.** `start_refresh` is a timer around
   `_refresh_all_sources()` (`registry.py:532-551`), which already does the concurrency, the
   per-source failure tolerance, and the keep-last-good commit. Nothing about *refreshing* is
   being written.
✅ **`start_refresh`/`stop_refresh` over `start`/`stop`**: bare `start`/`stop` on a
   `TrustedIssuerRegistry` would make it structurally satisfy `AbstractLifecycle`
   (`lifespan.py:73-90`), so `lifespan.register(registry)` would start a JWKS refresher under a
   method name that says nothing about refreshing. The registry is not a lifecycle object — it
   verifies tokens, checks revocation, and caches keys; "stop the registry" is ambiguous in a way
   "stop refreshing" is not. ⓘ **Honest correction to the obvious worry:**
   `_collect_lifecycle_components()` resolves an explicit type allowlist (`app.py:861-884`), *not*
   a Protocol scan, so bare names would **not** have caused silent auto-registration. The argument
   above stands on naming clarity alone.
✅ **⛔ No scanned `@Configuration`, no module-level task, nothing that starts on import.** This is
   the exact `varco_core.tls` rule CLAUDE.md states for a filesystem watcher —
   `container.scan("varco_core", recursive=True)` auto-activates scanned `@Configuration`s, and a
   background HTTPS-fetching task in every app that scans `varco_core` would be indefensible.
   `start_refresh()` is only ever reached by an explicit call.
❌ Two methods on a class that already has many. Accepted: they are named for what they do and are
   documented as a pair.

**Rejected — the loop entirely inside `JwksRefreshLifecycle` (`varco_fastapi`).** ❌ Denies the
feature to every non-FastAPI consumer. ❌ Forces the refresh cadence to be tested through an ASGI
lifespan instead of a bare `asyncio` test. ❌ Contradicts the `ReloadingTrustStore` prior.

**Rejected — `varco_core.reload.ReloadableResource` as the mechanism.** ❌ Over-fitting: it models
"one value, loaded from a path watcher, swapped under a lock with generation counting"
(`tls/reload.py:135-137`), whereas this is N independent keysets each committed in place by
`_refresh_all_sources()`. ❌ Its keep-last-good guarantee is already provided, twice — by
`registry.py:543-547` (skip failed sources) and by `jwks_url.py:188-193` (return stale cache on
fetch failure). Wrapping it would add a second, redundant last-good layer with different semantics.

### §D-S22-interval — derive the period from `ttl_seconds`; **no third knob**

| ID | Choice | Consequence |
|---|---|---|
| D-S22-interval | Effective period = `interval` if explicitly passed, else `ttl_seconds` (`registry.py:227-231`, env `VARCO_JWKS_TTL_SECONDS`, default `0.0`). `<= 0` ⇒ **no task is created**. A positive period below `min_refresh_interval` (`registry.py:220-224`) is **clamped up** to it, with one WARNING naming both values | No new env var; the refresher is **off by default**; the existing two knobs get *more* coherent, not less |

✅ **`ttl_seconds` is already defined as exactly this quantity** — "the age at which the cached
   keyset is considered stale" (`registry.py:195-201`). A background refresher that ticks at that
   period *delivers* the knob's documented intent instead of adding a competing one. The knob
   currently under-delivers (it only fires inside a `get_key()` call); after this plan it means the
   same thing in both the reactive and the background path.
✅ **No third overlapping knob**, which was the explicit hazard. An operator still reasons about
   exactly two numbers: "how stale may a keyset get" and "how often may I re-fetch at most".
✅ **Off by default falls out for free.** `ttl_seconds` defaults to `0.0`, so a
   `create_varco_app()` with no arguments starts no task, opens no socket, and behaves
   byte-identically to 3.1. This is the house pattern — `MigrationSettings.mode="off"`,
   `I18nSettings.enabled=False`, `TimezoneSettings.enabled=False`,
   `RetentionLifecycle(interval=0.0)` (`plans/039-…:524-525`).
✅ **The `min_refresh_interval` clamp is not paternalism, it is arithmetic.**
   `JwksUrlSource.refresh()` returns the cache without a network call inside its own
   `min_refresh_interval` (`jwks_url.py:182-184`), so a period below it produces ticks that
   provably do nothing. Clamping + one WARNING turns a silent no-op into a named misconfiguration.
❌ An app that wants proactive-on-`verify()` reload but **not** a background task cannot express
   that with `ttl_seconds` alone. Mitigated by the explicit `interval=` override on both
   `start_refresh(interval=…)` and `JwksRefreshLifecycle(interval=…)` — a constructor argument, no
   env var, so the ambient configuration story stays two knobs.
❌ Off by default means the row's hole stays open for a default deployment. Accepted, and
   deliberately: flipping it on would make **every** varco app begin periodic outbound HTTPS calls
   — a network-behaviour change, not a code change, and not one an upgrade note can undo for a
   locked-down environment. §D-S22-posture is the mitigation: the deployment can *see* that it is
   off.

**Rejected — a new `VARCO_JWKS_REFRESH_SECONDS`.** ❌ Three knobs whose interaction ("if TTL is 300
and refresh is 60 and min-refresh is 10, when does a fetch happen?") no operator should have to
derive. The task brief named this hazard; the evidence supports it.

### §D-S22-loop — one task for all issuers, `stop_event`-driven, copied from `watch/base.py`

| ID | Choice | Consequence |
|---|---|---|
| D-S22-loop | **One** task per registry, calling `_refresh_all_sources()` per tick. `start_refresh()` creates an `asyncio.Event` and the task; `stop_refresh()` sets it, cancels, awaits, and swallows `CancelledError`. Both idempotent. The sleep is `asyncio.wait_for(stop_event.wait(), timeout=period)` so shutdown is immediate, never up to one period late | Byte-for-byte the shape at `varco_core/varco_core/watch/base.py:173-191` and `watch/poll.py:97-102` |

✅ **Failure isolation already lives inside the tick, not between tasks.**
   `_refresh_all_sources()` gathers with `return_exceptions=True` and commits only non-exception
   results (`registry.py:538-547`) — one dead issuer cannot stop another from refreshing. A task
   per issuer would buy isolation that already exists, at N times the task count.
✅ **One task means one place to reason about lifetime**, and `stop_refresh()` has one thing to
   cancel — which is what makes "no orphaned task, no `Task was destroyed but it is pending`"
   testable as a single assertion.
✅ **Lazy by construction.** No `asyncio.Event`, no `Task`, no `Lock` is created in `__init__` or
   at module scope — `start_refresh()` is `async def` and therefore always has a running loop
   (the same rule `_get_lock()` already follows at `registry.py:240-256`).
❌ A pathologically slow issuer delays the next tick for all of them. Bounded in practice:
   `JwksUrlSource` carries its own `timeout` (default 10 s, `jwks_url.py:110`), so a tick's worst
   case is one HTTP timeout, not unbounded.
❌ `__slots__` must grow by two entries (`registry.py:151-159`) or the attributes raise
   `AttributeError`. Called out because it is the one non-obvious edit in the file.

### §D-S22-failure — no retry **inside** a tick; the loop *is* the cadence

| ID | Choice | Consequence |
|---|---|---|
| D-S22-failure | A failing tick is logged at WARNING and the loop continues. **No `RetryPolicy` is applied inside a tick.** The task body is wrapped in `try/except Exception` so no failure can ever kill the loop; `asyncio.CancelledError` is re-raised, never swallowed | A JWKS endpoint that is down slows nothing, crashes nothing, and self-heals on the next tick |

The standing rule is *never invent a second retry model* — so the alternative was considered
against source, not skipped:

✅ **An immediate retry would provably be a no-op.** `JwksUrlSource.refresh()` returns the cached
   keyset without a network call when `min_refresh_interval` has not elapsed
   (`jwks_url.py:179-184`). A `RetryPolicy` with any sane backoff fires its second attempt inside
   that window, so it would "retry" by re-reading the same cache and reporting success. That is
   worse than no retry: it manufactures a false green.
✅ **The periodic loop already is the retry cadence**, at a fixed interval, with the keep-last-good
   semantics the row asks for supplied twice over (`registry.py:543-547`,
   `jwks_url.py:188-193`).
✅ **Startup cannot be crashed by a down endpoint** because `JwksRefreshLifecycle.start()`
   deliberately does **not** call `load_all()` — which raises `KeyLoadError` when any source fails
   (`registry.py:361-363,388-393`). The initial load stays the app's own explicit
   `await registry.load_all()` (the documented startup step, `registry.py:349-354`), unchanged.
   §D-S22-lifecycle spells this out.
❌ No exponential backoff: a permanently dead issuer is polled at a constant rate forever.
   Accepted — the rate is the operator's own `ttl_seconds`, floored at `min_refresh_interval`, and
   a JWKS fetch is a single small GET.
❌ WARNING-per-tick could be noisy for a long outage. Mitigated: the tick logs **once per
   transition** into and out of the failed state, not once per tick — the same `_in_error` latch
   `StatPollWatcher` uses (`varco_core/varco_core/watch/poll.py:73`).

### §D-S22-lifecycle — `JwksRefreshLifecycle`, following the shipped pattern **as-is**

| ID | Choice | Consequence |
|---|---|---|
| D-S22-lifecycle | `varco_fastapi/varco_fastapi/jwks.py` — `JwksRefreshLifecycle(registry, *, interval=None)` with `startup()`/`shutdown()` **and** `start()`/`stop()` aliases. `create_varco_app(jwks_refresh=None)` **appends** it when non-`None` | One more component of an established shape; nothing new to learn |

✅ Byte-for-byte the `ReliabilityLifecycle` shape: aliases with the comment explaining why they
   exist (`varco_fastapi/varco_fastapi/reliability.py:114-123`); appended, not prepended
   (`app.py:411-422`) — like reliability, because the registry it drives is built by the app, not
   by an earlier lifecycle component.
✅ ⛔ **`varco_fastapi/varco_fastapi/lifespan.py` is not modified.** The component satisfies the
   existing `AbstractLifecycle` Protocol (`lifespan.py:73-90`) and rides the existing `register()`
   (`lifespan.py:162-183`). Held to `plans/039-…:519-523`, from both sides.
✅ `start()` calls **only** `registry.start_refresh(interval=self._interval)`. It never calls
   `load_all()` (§D-S22-failure) — a down issuer at boot must not fail startup.
✅ `stop()` is idempotent and always awaits the cancelled task, so `VarcoLifespan._stop_all()`
   (`lifespan.py:312-324`) reports a clean stop and no pending task survives the lifespan.
❌ A second way to start the refresher (the lifecycle, or calling `start_refresh()` yourself).
   Accepted: the lifecycle is a thin wrapper and the registry stays usable standalone, exactly as
   `OutboxRelay` is.

**Rejected — start the refresher automatically whenever a `TrustedIssuerRegistry` is bound in DI.**
❌ This is the `varco_core.tls` scanned-`@Configuration` failure mode restated: a DI binding whose
side effect is a background network loop. ❌ It would also make the behaviour depend on
*whether* an app used DI for its registry, which is optional (`registry.py:169-178` says DI is
opt-in by design). Same verdict 039 reached for `bind_retention_registry`
(`plans/039-…:528-530`).

### §D-S22-posture — **yes**, `inspect_jwks_posture()`; checked against 038's three arguments

Plan 038 argued against an inspector for its off-by-default path
(`plans/038-inbound-webhook-verification.md:409-433`); 039 checked those arguments rather than
assuming them (`plans/039-…:549-558`). The same check:

| 038's argument | Does it apply to JWKS refresh? |
|---|---|
| *"a per-route `Depends(...)` with no process-global registry to read — inspecting it would require **building** one"* | ❌ **No.** A `TrustedIssuerRegistry` is an explicit, app-constructed object with its own entries dict (`registry.py:213`). The inspector reads what exists, exactly like `inspect_revocation_posture(registry=…)` (`revocation/posture.py:72-76`) |
| *"an app with no such route would get a permanent, meaningless finding"* | ⚠️ **Partly** — resolved the same way revocation does: `registry=None` and "no remote sources registered" are reported as **facts**, never findings, so a PEM-only or registry-less app gets no noise |
| *"the question is already answered at construction, loudly, by a `ValueError`"* | ❌ **No.** Nothing raises. The whole point of the row is that the two knobs *silently* under-deliver; "is anything actually refreshing my JWKS?" has no other way to be answered |

| ID | Choice | Consequence |
|---|---|---|
| D-S22-posture | `varco_core/authority/posture.py`: `inspect_jwks_posture(registry=None) -> JwksPostureReport` — pure read, never raises, no globals. Fields: `registry_present`, `remote_source_count` (sources whose `source_id` is remote — JWKS/OIDC), `ttl_seconds`, `min_refresh_interval`, `refresher_running`, `effective_interval`, `keysets_loaded`. Exported from `varco_core.authority`'s `__all__` only — **not** `varco_core`'s top level, matching `inspect_revocation_posture` | Consistent with the house pattern; no new cross-plan seam; import budget untouched |

✅ `refresher_running=False` **with** `remote_source_count > 0` is the genuinely security-relevant
   finding: a key the issuer removed from its JWKS stays trusted in this process indefinitely,
   because nothing will ever re-fetch. That is the fact the row exists to make visible.
✅ Pure and dependency-free — reads attributes, no I/O, no `await`. Same contract as
   `revocation/posture.py:15-16,92`.
❌ Plan 036's `SecurityPosture` must eventually call it. **Out of scope** here; the pure function
   is the whole deliverable and a BACKLOG row records the wiring — identical split to
   `plans/039-…:567-568`.

---

## Phases

Each phase is independently verifiable and ends with a green `make lint`.

### Phase 1 — S17: move `MetricsMiddleware` inside `TracingMiddleware` (independently mergeable)

1. [x] `varco_fastapi/tests/test_metrics_span_context.py` — **new, red**. Build an app via
       `create_varco_app(enable_tracing=True, enable_metrics=True, …)` against an
       `InMemoryMetricReader` + a real `TracerProvider` with an always-on sampler (the fixture
       shape already used at `varco_fastapi/tests/milestone_g/test_metrics_middleware.py:30,54`
       and `examples/03-observability-metrics/tests/test_smoke.py:83-92`; note
       `examples/FINDINGS.md:576-594` — patch the internal getter, do not call
       `set_meter_provider()` twice). Two assertions:
       (a) unconditional — during a request, `opentelemetry.trace.get_current_span()` inside the
       metrics dispatch has a valid `SpanContext` (assert via a monkeypatched
       `_get_duration_histogram` recording the current span context);
       (b) capability-guarded (`pytest.skip` when the installed SDK has no exemplar support,
       §D-S17-sdkfloor) — the collected `http.server.request.duration` histogram data point has a
       non-empty `.exemplars` whose `trace_id` matches the request's span.
       **Must fail before Step 3.**
2. [x] `varco_fastapi/tests/test_middleware_order.py:32-33` — swap `"MetricsMiddleware"` and
       `"TracingMiddleware"` in `_PHASE_2_BASELINE_ORDER`. **This edit is the decision's permanent
       record.** Every Plan-035 test compares by slice against this list
       (`:118,124,130+`), so no other test list changes. **Must fail before Step 3.**
3. [x] `varco_fastapi/varco_fastapi/app.py:582-604` — move the `if enable_metrics:` block so it is
       registered **before** the `if enable_tracing:` block (`add_middleware` prepends, so the
       later call is outer → tracing outer, metrics inner). Replace the `:586-592` comment with a
       short §D-S17-decision rationale + a pointer to the normative table; delete the
       "BACKLOG.md's filed question" sentence — it is now answered.
4. [x] `varco_fastapi/varco_fastapi/middleware/metrics.py:262-270` — rewrite the stale
       "Recommended position" paragraph (⚠️ the **third** wrong comment, which Plan 035 missed) as
       a one-line pointer to `varco_fastapi.middleware`'s normative table, per the one-home rule.
       Add a `DESIGN:` block naming §D-S17-decision: why this middleware must sit inside
       `TracingMiddleware` (exemplars) and what it degrades to when `enable_tracing=False`.
5. [x] `varco_fastapi/varco_fastapi/middleware/__init__.py:27-28` — swap the two lines in the
       normative table; annotate `MetricsMiddleware` with `(INSIDE Tracing — Plan 041 / §D-S17-decision:
       exemplars need a current span)`.
6. [x] `technical_docs/features/http-edge-hardening.md:23-24` — swap the same two lines in the
       mirrored table; `:55-65` (§D-order-bugs) — mark `S17` **resolved by Plan 041**, state that a
       **third** stale comment (`middleware/metrics.py:262-270`) was found and corrected, and link
       the new feature doc section.
7. [x] `technical_docs/features/http-edge-hardening.md` — new section
       *"Metrics inside tracing (Plan 041 / S17)"*: the §D-S17-histogram before/after table, the
       three-sentence operator note, and a **Pitfalls** row (*"latency alerts with absolute
       thresholds see a one-time downward step"*).
8. [x] `CHANGELOG.md` — the three sentences from §D-S17-histogram verbatim, under Changed, plus
       the new exemplar capability under Added.
9. [x] `BACKLOG.md` — move `S17` from Live to *Shipped this cycle* with the one-line outcome
       (*"moved inside `TracingMiddleware`; exemplars now reachable"*); add the two new rows from
       §BACKLOG entries.
10. [ ] Verify:
        `uv run pytest varco_fastapi/tests/test_middleware_order.py varco_fastapi/tests/test_metrics_span_context.py varco_fastapi/tests/milestone_g/test_metrics_middleware.py varco_fastapi/tests/test_http_edge_introspect.py -v`
        → all green; then `uv run pytest varco_fastapi/tests/ -q` → no regression; then
        `make lint` and `make type-check`.
        ⓘ No `__all__` change in this phase, so `scripts/api_surface.py --check` (inside
        `make lint`'s no-`PKG` path) must pass **without** regeneration. If it does not, something
        unintended changed.

### Phase 2 — S22: `start_refresh()`/`stop_refresh()` + `JwksRefreshLifecycle`

11. [x] `varco_core/tests/test_jwks_refresh.py` — **new, red**. Against a fake `KeySource` whose
        `refresh()` counts calls:
        - `start_refresh()` with an effective period `<= 0` creates **no** task
          (`registry._refresh_task is None`) and is a silent no-op;
        - with a short period, `refresh()` is called ≥2 times within a generous margin (house
          rule: **increase the sleep margin, never `xfail`**, CLAUDE.md §Test Conventions);
        - `start_refresh()` twice is idempotent — one task;
        - `stop_refresh()` before `start_refresh()` is a no-op;
        - **clean shutdown**: after `stop_refresh()`, `registry._refresh_task is None`, and
          `len(asyncio.all_tasks())` is back to the pre-start baseline — the explicit
          "no orphaned task / no `Task was destroyed but it is pending`" assertion;
        - a source whose `refresh()` raises every time does **not** kill the loop: a second,
          healthy source keeps being refreshed across ≥2 ticks (§D-S22-failure);
        - a period below `min_refresh_interval` is clamped up and logs one WARNING naming both
          values (`caplog`).
12. [x] `varco_core/varco_core/authority/registry.py` — extend `__slots__` (`:151-159`) with
        `_refresh_task`, `_refresh_stop`, `_refresh_interval`, `_refresh_in_error`; initialise all
        four to `None`/`False` in `__init__` (**no `Event`, no `Task` constructed here** — same
        rule as `_get_lock()` at `:240-256`). Add:
        - `async def start_refresh(self, *, interval: float | None = None) -> None` — §D-S22-interval
          resolution + clamp + WARNING; `<= 0` ⇒ return; idempotent; creates the `Event` and the
          task (`watch/base.py:173-178` shape).
        - `async def stop_refresh(self) -> None` — set → cancel → await → swallow `CancelledError`;
          idempotent (`watch/base.py:180-191` shape).
        - `async def _refresh_loop(self) -> None` — `asyncio.wait_for(stop_event.wait(),
          timeout=period)`/`TimeoutError` tick (`watch/poll.py:97-102` shape), body wrapped in
          `try/except Exception` with the `_refresh_in_error` transition latch; `CancelledError`
          re-raised, never caught.
        - `@property def refresh_running(self) -> bool` and
          `@property def refresh_interval(self) -> float` (`0.0` = off) — the two facts
          §D-S22-posture reads.
        Full docstrings with **Args / Returns / Raises / Edge cases / Async safety**, plus a
        `DESIGN:` block per §D-S22-loop and §D-S22-failure with ✅/❌.
13. [x] `varco_core/tests/test_jwks_posture.py` — **new, red**: `inspect_jwks_posture()` with no
        argument, with a registry that has only PEM sources, with a remote source and no refresher
        (`refresher_running=False`, `remote_source_count=1` — the finding the row exists for), and
        with a running refresher. Assert it never raises for any input, including a registry with
        zero entries.
14. [x] `varco_core/varco_core/authority/posture.py` — **new**. `@dataclass(frozen=True)
        JwksPostureReport` + `inspect_jwks_posture(registry=None)`, modelled line-for-line on
        `varco_core/varco_core/revocation/posture.py:1-17,31-99` including its "reports facts,
        036 owns the judgement" module docstring. Add both names to
        `varco_core/varco_core/authority/__init__.py`'s `__all__`; **do not** add them to
        `varco_core/varco_core/__init__.py` (matches `inspect_revocation_posture`; keeps the PEP
        562 lazy `__init__` and the import budget untouched).
15. [x] `varco_fastapi/tests/test_jwks_refresh_lifecycle.py` — **new, red**: `start()` starts the
        refresher and `stop()` stops it; `start()` **never** calls `load_all()` (a registry whose
        only source always raises still starts cleanly — §D-S22-failure); `interval=None` with
        `ttl_seconds=0.0` starts nothing; a full `create_varco_app(jwks_refresh=…)` lifespan cycle
        via `TestClient` leaves no pending task.
16. [x] `varco_fastapi/varco_fastapi/jwks.py` — **new**. `JwksRefreshLifecycle(registry, *,
        interval=None)` with `startup()`/`shutdown()` and `start()`/`stop()` aliases, carrying the
        same explanatory comment as `varco_fastapi/varco_fastapi/reliability.py:114-118`.
        ⛔ `varco_fastapi/varco_fastapi/lifespan.py` is **not** touched.
17. [x] `varco_fastapi/varco_fastapi/app.py` — add `jwks_refresh: JwksRefreshLifecycle | None =
        None` to `create_varco_app`, documented in the `Args:` block; **append** it to
        `lifespan_components` when non-`None`, in the style of the reliability block at
        `:411-422`. Import inside the branch (`# noqa: PLC0415`), like every sibling.
18. [x] `varco_fastapi/varco_fastapi/__init__.py` — export `JwksRefreshLifecycle` in the
        `# ── Lifecycle ──` group beside `MigrationLifecycle` (`:189,351`) and add it to `__all__`.
19. [x] `uv run python scripts/api_surface.py` — regenerate
        `design/api-freeze-and-standards/measurements/api-surface.{json,md}` and commit both.
        **Hard requirement**: `__all__` grew in three packages' surfaces; `--check` fails the next
        `make lint`/CI run otherwise (CLAUDE.md §Public API surface snapshot).
20. [x] Docs, all in this change:
        - `technical_docs/features/jwt-claim-transformer.md:310-314` — ⛔ **delete the ⚠️ "There is
          no background refresher task" paragraph** and replace it with the refresher's usage,
          the §D-S22-interval derivation, the off-by-default statement, and a **Pitfalls** table
          (*"`ttl_seconds=0` means the refresher never starts"*; *"binding the registry in DI does
          not start it"*; *"a period below `min_refresh_interval` is clamped"*; *"`start()` does
          not `load_all()` — call it yourself"*). Retitle the section (drop "deferred").
        - `README.md:2869-2870` — add a row noting that `VARCO_JWKS_TTL_SECONDS` now also sets the
          background refresher's period, and a short "JWKS background refresh" snippet showing
          `create_varco_app(jwks_refresh=JwksRefreshLifecycle(registry))`.
        - `CLAUDE.md` §Authority/JWT — ⛔ **delete the ⚠️ sentence** *"There is no background
          refresher task…"* and replace it with the two-line rule: the refresher is off unless
          `VARCO_JWKS_TTL_SECONDS`/`interval=` is positive, and it must be started via
          `create_varco_app(jwks_refresh=…)` or an explicit `start_refresh()` — never by a scanned
          `@Configuration`. Add the S17 outcome as a one-line pointer in the HTTP-edge section.
        - `CHANGELOG.md` — Added: `start_refresh()`/`stop_refresh()`, `JwksRefreshLifecycle`,
          `inspect_jwks_posture()`; note the unchanged default.
        - `BACKLOG.md` — move `S22` to *Shipped this cycle*; add the §D-S22-posture wiring row.
21. [x] Verify:
        `uv run pytest varco_core/tests/test_jwks_refresh.py varco_core/tests/test_jwks_posture.py varco_fastapi/tests/test_jwks_refresh_lifecycle.py -v`
        → green with **zero** `Task was destroyed but it is pending` lines in captured output;
        then `uv run pytest varco_core/tests/ varco_fastapi/tests/ -q`; then `make lint`
        (including `api-check`) and `make type-check`.

---

## Edge cases

| Input / state | Expected behaviour |
|---|---|
| `enable_tracing=False, enable_metrics=True` | Metrics recorded exactly as today, no span, no exemplar. No error, no warning |
| `enable_metrics=True` with no `MeterProvider` set | OTel returns a no-op meter; unchanged (`metrics.py:286-287`) |
| OTel SDK < 1.28 installed | Ordering test passes; exemplar test skips with a message naming the version (§D-S17-sdkfloor) |
| A request to `/health` (a `skip_paths` prefix) | Span created (tracing is outer, no skip list), no metric recorded — identical before and after |
| An exception propagating to `ErrorMiddleware` | Metrics records `status_code="500"`, span status ERROR; the client sees the mapped status. **Unchanged**, filed not fixed |
| `start_refresh()` with `ttl_seconds=0.0` and no `interval=` | No task, no log noise beyond DEBUG, `refresh_running is False` |
| `start_refresh(interval=1.0)` with `min_refresh_interval=10.0` | Clamped to `10.0`, one WARNING naming both values |
| `start_refresh()` called twice | Second call is a no-op; exactly one task |
| `stop_refresh()` called before `start_refresh()`, or twice | No-op both times, no exception |
| Every JWKS source down for the whole process lifetime | Loop keeps ticking; one WARNING on the first failure and one INFO on recovery, not one per tick; last-good keysets stay served (`registry.py:543-547`) |
| A registry with **zero** entries | `_refresh_all_sources()` gathers nothing and stamps timestamps; the loop is harmless. `inspect_jwks_posture()` reports `remote_source_count=0` as a fact |
| `JwksRefreshLifecycle.start()` when the JWKS endpoint is down | Startup succeeds — `load_all()` is never called (§D-S22-failure) |
| Lifespan shutdown while a tick is mid-fetch | `stop_refresh()` cancels and awaits; the in-flight `asyncio.to_thread` fetch is abandoned by the awaiting task, `CancelledError` swallowed by `stop_refresh()` only |
| `inspect_jwks_posture(registry=None)` | A report with `registry_present=False`; never raises |

## Verification

```bash
# Phase 1 (S17) — must be green on its own commit
uv run pytest varco_fastapi/tests/test_middleware_order.py \
              varco_fastapi/tests/test_metrics_span_context.py \
              varco_fastapi/tests/milestone_g/test_metrics_middleware.py \
              varco_fastapi/tests/test_http_edge_introspect.py -v
uv run pytest varco_fastapi/tests/ -q

# Phase 2 (S22)
uv run pytest varco_core/tests/test_jwks_refresh.py \
              varco_core/tests/test_jwks_posture.py \
              varco_fastapi/tests/test_jwks_refresh_lifecycle.py -v
uv run pytest varco_core/tests/ varco_fastapi/tests/ -q

# Both phases
uv run python scripts/api_surface.py          # Phase 2 only — regenerate + commit
make lint                                     # ruff + format + api-check + asyncapi-check + import-budget
make type-check
make test                                     # all eleven suites
```

**Definition of done**

1. `_PHASE_2_BASELINE_ORDER` lists `TracingMiddleware` before `MetricsMiddleware`.
2. All four documents carrying the ordering table agree (`middleware/__init__.py`,
   `middleware/metrics.py`, `app.py`, `http-edge-hardening.md`) — the drift §D-order-bugs found is
   not re-created.
3. `rg -n "no background refresher task" .` returns **nothing**.
4. `rg -n "install_process_trust|@Configuration" varco_core/varco_core/authority/` shows no
   scanned configuration was added.
5. `varco_fastapi/varco_fastapi/lifespan.py` has **zero diff** (039's undertaking, reciprocated).
6. Every file Plan 038 listed as its own has **zero diff** (038's DoD item 6, reciprocated).
7. `make lint` passes including `api-check` with the regenerated snapshot committed.

## Risks

| Risk | Likelihood | Mitigation / invariant that must hold |
|---|---|---|
| ⚠️ **ASSUMPTION** — `opentelemetry-exporter-prometheus` may not translate SDK exemplars into OpenMetrics output, so the *end-to-end* Grafana story could still not work even after this move. Brief 012's Evidence Gap 2/3 do not cover the exporter | Medium | The plan's claim is deliberately scoped to *"exemplars become possible at the SDK layer"*, proven by an `InMemoryMetricReader` assertion (Step 1b) that is exporter-independent. A BACKLOG row records the exporter question. **Do not claim end-to-end Grafana exemplars in the CHANGELOG** |
| ⚠️ **ASSUMPTION** — brief 012 Evidence Gap 1: no evidence any varco consumer uses exemplars today | Medium | Accepted. The move is justified independently by `router/metrics.py:20-25,45-51` promising the capability, and costs one block move |
| Operators with absolute latency alerts see a step change | High | §D-S17-histogram; the three-sentence CHANGELOG note is a required step, not optional |
| A `TrustedIssuerRegistry` constructed before the loop exists then `start_refresh()`ed from a sync context | Low | `start_refresh` is `async def`; there is no sync entry point. Invariant: no `Event`/`Task`/`Lock` is created outside a running loop |
| A background refresher makes a compromised-key window *feel* closed when it is off | Medium | §D-S22-posture's `refresher_running=False` + `remote_source_count>0` is exactly this fact; the Pitfalls table names it |
| A rebase collision with 039 in `app.py`'s keyword list | Medium | Both plans append one keyword in the established style; §Scope names the collision so it is a two-line merge, not a semantic surprise |
| ⚠️ **ASSUMPTION** — Plan 040 (S21) is not yet written; its logging-middleware assumptions were checked against the *current* file, not against 040's plan | Medium | §D-S17-blast establishes the invariant 040 needs: `RequestLoggingMiddleware`'s position is unchanged. If 040 lands first and moves it, re-verify that row before merging Phase 1 |
| `__slots__` omission on `TrustedIssuerRegistry` | Low | Step 12 names it; a missing entry fails immediately with `AttributeError` in Step 11's tests |

## Open questions

1. Should `enable_metrics=True` with `enable_tracing=False` emit a one-time INFO noting that no
   exemplars will be produced? Leaning **no** (log noise for a legitimate configuration), but it is
   the cheapest possible discoverability fix if operators are surprised. Not implemented.
2. Should `JwksRefreshLifecycle` optionally call `load_all()` behind an explicit
   `initial_load=True` kwarg? Deliberately **not** in this plan (§D-S22-failure); revisit only if
   an app reports duplicating the call.
3. Whether `ttl_seconds > 0` should imply the refresher *without* a `jwks_refresh=` keyword, once
   the keyword exists. **No** for 3.2 (§D-S22-lifecycle's rejected alternative); a 4.0 flip is
   arguable and belongs on the 4.0 flip list, not here.

## BACKLOG entries

Rows to add to `BACKLOG.md`'s **Live** table as part of this plan:

| ID | Feature | Severity | Complexity | Rationale | Evidence |
|----|---------|----------|------------|-----------|----------|
| *(new)* | **`MetricsMiddleware` records `status_code="500"` for exceptions that `ErrorMiddleware` renders as 4xx** | 🟡 should | S | `ErrorMiddleware` is outside `MetricsMiddleware` and stays there, so any `ServiceException` reaching metrics is counted as a 500 while the client receives 404/409/422. Every error-rate dashboard over `http.server.request.duration` is wrong for mapped exceptions. Pre-existing; explicitly out of Plan 041's scope | `varco_fastapi/varco_fastapi/middleware/metrics.py:368,375-388`; `varco_fastapi/varco_fastapi/app.py:636-642` |
| *(new)* | **Verify `opentelemetry-exporter-prometheus` actually emits exemplars end-to-end** | 🟢 nice | S | Plan 041 makes exemplars reachable at the SDK layer and proves it with `InMemoryMetricReader`. Whether the Prometheus exporter path (`observability/di.py:406-418` → `router/metrics.py`) translates them into OpenMetrics output is unverified — brief 012 has no evidence either way. If it does not, `router/metrics.py:45-51`'s promise is still only half true | `design/research/012-otel-exemplars-and-metrics-span-context.md` Evidence Gaps 2/3 |
| *(new)* | **Wire `inspect_jwks_posture()` into `SecurityPosture`** | 🟡 should | S | Plan 041 exports the pure inspector; Plan 036 owns the harness (`varco_fastapi/varco_fastapi/posture.py:258`, collectors at `:400+`). Same split 039 §D-S20-posture used for `inspect_retention_posture()`; both should land in one harness pass rather than two | `varco_core/varco_core/authority/posture.py` (new); `plans/039-retention-and-purge-automation.md:567-568` |

Rows to **move to Shipped this cycle**: `S17` (Phase 1), `S22` (Phase 2).
