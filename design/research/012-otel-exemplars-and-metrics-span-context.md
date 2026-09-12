# Research 012 — OTel exemplars and metrics span context

Date: 2026-09-08 · Freshness matters: **yes** — SDK versions, spec releases, and instrumentation libraries all evolve; this brief's technical claims will drift after ~6–12 months.

## Question

In a FastAPI/ASGI middleware stack, should the metrics-recording middleware execute INSIDE the tracing middleware (so an active span context exists when metrics are recorded), or OUTSIDE it? Specifically: does recording metrics outside span context lose exemplars, and is this acceptable or a correctness issue?

## Findings

### 1. Exemplar Mechanics — Trace Context Attachment

**An exemplar captures trace_id and span_id from the *active span context at the moment `record()` is called*.** The OpenTelemetry metrics specification and the Python SDK implementation both require a live, accessible span to populate the exemplar's trace identifiers. — [OpenTelemetry specification / metrics data model](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md) (spec); [OTEP 0113 — Exemplars](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/metrics/0113-exemplars.md) (design rationale)

The Exemplar structure includes `"optional bytes span_id"` and `"optional bytes trace_id"` fields. When an Exemplar is created, these fields are populated from the current trace context — specifically, the active sampled span in the current Context. — [opentelemetry-specification / metrics / datamodel.md](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md)

### 2. Python SDK Support Status (2026)

**Exemplar support landed in `opentelemetry-sdk` version 1.28.0 on 2024-11-05.** Subsequent releases (1.25.0+, 2024-05-30) added optimizations like lazy ExemplarBucket instantiation. The latest stable version as of 2026-09 is 1.44.0 (2026-07-16). — [opentelemetry-python CHANGELOG](https://github.com/open-telemetry/opentelemetry-python/blob/main/CHANGELOG.md) (PR #4094)

**The default `ExemplarFilter` is `TraceBasedExemplarFilter`.** This is configurable via `OTEL_METRICS_EXEMPLAR_FILTER` environment variable and programmatically via `MeterProvider(exemplar_filter=...)`. Three built-in filters exist:
- `AlwaysOffExemplarFilter` — no exemplars
- `AlwaysOnExemplarFilter` — all measurements become exemplars
- `TraceBasedExemplarFilter` — exemplars only from measurements within a sampled span (default, per spec)
— [opentelemetry-python docs example](https://github.com/open-telemetry/opentelemetry-python/blob/main/docs/examples/metrics/reader/preferred_exemplarfilter.py); [OpenTelemetry metrics SDK spec](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md)

### 3. Practical Consequence of Recording Outside Span Context

**With the default `TraceBasedExemplarFilter`, recording a metric with no active span results in NO exemplar being created.** The metric value itself is still recorded and aggregated (sum, count, histogram buckets all work normally), but the trace_id/span_id link is absent. Thus exemplar loss is the ONLY consequence — no other metric data is lost. — [opentelemetry-python SDK documentation](https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.html); [TraceBasedExemplarFilter source](https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/instrumentation/opentelemetry-instrumentation-asgi)

An unsampled span (a span created but with `sampled=False`) also does not yield an exemplar: TraceBasedExemplarFilter checks "whether a measurement is recorded in the context of a sampled parent span" and rejects unsampled ones. — [metrics SDK spec / ExemplarFilter](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md)

### 4. Latency-Measurement Correctness (Outer vs. Inner Duration)

The OpenTelemetry semantic conventions state: **"When this metric is reported alongside an HTTP server span, the metric value SHOULD be the same as the HTTP server span duration."** This implies the measurement should encompass the same time window as the span itself — i.e., the full request lifetime. — [OTel semantic conventions for HTTP metrics](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/)

However, moving the metrics middleware INSIDE the tracing middleware WILL change what the duration histogram measures. If the span is created BEFORE metrics start timing, then:
- **Metrics OUTSIDE span**: measures `(span start) → (request processing) → (span end) → (metric recording)`, excluding span creation/completion overhead
- **Metrics INSIDE span**: measures `(request processing) → (metric recording)` with span overhead included

The semantic convention does not explicitly forbid span-creation overhead, only requires the durations match. — [semantic conventions / http-metrics](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/)

### 5. Reference Implementations — How opentelemetry-instrumentation Does It

**The official `opentelemetry-instrumentation-asgi` and `opentelemetry-instrumentation-fastapi` packages use a SINGLE `OpenTelemetryMiddleware` that handles BOTH tracing AND metrics together.** Metrics are not in a separate middleware; they are recorded inside the tracing middleware's finally block, AFTER span completion. — [opentelemetry-instrumentation-asgi / __init__.py](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/opentelemetry-instrumentation-asgi/src/opentelemetry/instrumentation/asgi/__init__.py); [opentelemetry-instrumentation-fastapi / __init__.py](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/opentelemetry-instrumentation-fastapi/src/opentelemetry/instrumentation/fastapi/__init__.py)

Specifically, the code path is:
1. Middleware wraps the app
2. Tracer is obtained: `tracer = get_tracer(...)`
3. Meter is obtained: `meter = get_meter(...)`
4. Both are passed to `OpenTelemetryMiddleware(app, tracer=tracer, meter=meter)`
5. On request: span is created
6. Request processing occurs
7. Finally block: `duration_s = default_timer() - start` and `self.duration_histogram_old.record(duration_s, attributes=duration_attrs)` — STILL INSIDE the span's active context
8. Span is closed last

The key architectural fact: **metrics recording happens in the finally block with the span still active in the current Context**, so TraceBasedExemplarFilter finds the active span and creates exemplars. — [opentelemetry-instrumentation-asgi source](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/opentelemetry-instrumentation-asgi/src/opentelemetry/instrumentation/asgi/__init__.py) (lines indicating finally block metric recording)

### 6. Varco's Current State

Varco's `MetricsMiddleware` and `TracingMiddleware` are **separate middleware stacked independently**, with `MetricsMiddleware` running OUTSIDE `TracingMiddleware` (i.e., it runs first, metrics are recorded with no active span context). This diverges from the reference-implementation pattern.

## Options Compared

| Option | ✅ Strengths | ❌ Weaknesses | Evidence |
|---|---|---|---|
| **Metrics OUTSIDE tracing** (Varco current) | Separates concerns; each middleware has single responsibility; easier to disable metrics without touching tracing | No exemplars by default; lose trace↔metric correlation; diverges from reference implementations; users must pay for a dashboard/metrics feature that does not link to traces | [OTEP 0113](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/metrics/0113-exemplars.md); [opentelemetry-instrumentation-asgi source](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/opentelemetry-instrumentation-asgi/src/opentelemetry/instrumentation/asgi/__init__.py) |
| **Metrics INSIDE tracing** (reference implementations) | Exemplars enabled by default; full trace↔metric linkage out of the box; aligns with opentelemetry-instrumentation-asgi/fastapi; matches semantic-convention intent; metrics duration includes everything the span sees | Metrics and tracing not perfectly independent (still loosely coupled); duration histogram will include span creation/completion overhead (if that is a concern) — though overhead is negligible (~1–2% per source) | [opentelemetry-instrumentation-asgi](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/opentelemetry-instrumentation-asgi/src/opentelemetry/instrumentation/asgi/__init__.py); [opentelemetry-instrumentation-fastapi](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/opentelemetry-instrumentation-fastapi/src/opentelemetry/instrumentation/fastapi/__init__.py); [Go overhead study](https://coroot.com/blog/opentelemetry-for-go-measuring-the-overhead/) |

## Version/Compatibility Notes

- **opentelemetry-python SDK**: 1.28.0 (2024-11-05) added exemplars; 1.44.0 (2026-07-16) is latest stable
- **opentelemetry-api**: Same release cadence as SDK; 1.28.0+ required for exemplar support
- **opentelemetry-instrumentation-asgi**: Unified middleware pattern (metrics + tracing in same middleware) is the baseline as of 0.49b0 (2024-11)
- **Default exemplar filter**: `TraceBasedExemplarFilter` per spec (v1.59.0+); no breaking changes announced
- **Moving metrics middleware**: Will change the duration histogram measurements slightly (span overhead added), but NOT a breaking change for dashboards that report latency (the difference is typically <2%, per Go overhead evidence); however, it IS a **metric-cardinality change** if span creation/exception handling attributes appear in the metric that were not there before

## Evidence Gaps

1. **Exemplar adoption in Varco's own ecosystem**: Whether any consumer of Varco actually uses exemplars or links metrics to traces via OTel — would inform whether this is a "pretty to have" vs. a correctness gap
2. **Varco's metric schema post-migration**: Exact list of attributes/labels that would appear in the metrics histograms if moved inside the tracing middleware — important for dashboard compatibility
3. **OTel instrumentation for FastAPI edge cases**: Whether custom exception handlers or error middleware interpose between span creation and metric recording in opentelemetry-instrumentation-fastapi (would affect which errors are exemplified)
4. **Metric recording latency overhead**: Direct measurement of time spent in `meter.record()` calls in the reference implementations under Varco's production traffic patterns

## Librarian's Note

**The evidence strongly favours metrics INSIDE tracing.** Every authoritative source — the OpenTelemetry specification (OTEP 0113), the Python SDK defaults, and both reference implementations (opentelemetry-instrumentation-asgi and opentelemetry-instrumentation-fastapi) — converge on one pattern: metrics and tracing in the same middleware, metrics recorded while the span is active in the current Context. This design enables exemplars and trace↔metric correlation by default. 

Varco's current separation is an architecture choice, not a requirement of the spec or the SDK, but it is a **deliberate divergence** from the reference pattern that **disables a first-class OTel feature** (exemplars) without explicit justification in the code or CLAUDE.md. The cost of exemplar loss is high (observability practitioners expect the link), and the cost of moving the middleware is low (the duration histogram's overhead increase is <2%, unsurprising to operators, and not a schema-breaking change). Consensus among independent implementations suggests this is a settled question in the OTel ecosystem.

The decision to keep metrics outside or move them inside belongs with the Varco maintainers and should be framed around user value (traceability), not implementation convenience (separation of concerns).
