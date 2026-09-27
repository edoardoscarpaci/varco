# 03 — Observability: OTel Metrics + Prometheus Scraping

Demonstrates the full varco observability / metrics pipeline:

1. `enable_metrics=True` in `create_varco_app` — adds `MetricsMiddleware` to
   the ASGI stack and mounts `GET /metrics` (Prometheus scrape endpoint).
2. `MetricsMiddleware` — records three OTel instruments per request following
   the [OTel HTTP semantic conventions](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/):
   - `http.server.request.duration` (Histogram, seconds)
   - `http.server.active_requests` (UpDownCounter)
   - `http.server.request.body.size` (Histogram, bytes — when `Content-Length` present)
3. `PrometheusMetricReader` — bridges OTel SDK metrics into
   `prometheus_client.REGISTRY` so `generate_latest()` at `GET /metrics`
   includes them.
4. `MeterProvider` — the OTel SDK provider set as the global provider before
   the first request so lazy instrument creation picks it up.

No database, no message broker, no Docker required.

## Prerequisites

```bash
# OTel SDK is already a varco_core dep — no extra needed
# For the real Prometheus exporter (used in production):
uv add opentelemetry-exporter-prometheus
```

Without `opentelemetry-exporter-prometheus`, `GET /metrics` returns **503**
with an install hint — the app still starts and all other endpoints work.

## Run locally

```bash
cd examples/03-observability-metrics
uv run uvicorn app:app --reload
```

## Try it

```bash
# Create a few posts so the middleware has something to measure
curl -X POST http://localhost:8000/posts \
  -H "Content-Type: application/json" \
  -d '{"title": "Hello", "body": "First post"}'

curl http://localhost:8000/posts

# Scrape the Prometheus endpoint
curl http://localhost:8000/metrics
```

After a few requests, `/metrics` output includes:

```
# HELP http_server_request_duration_seconds Duration of HTTP server requests
# TYPE http_server_request_duration_seconds histogram
http_server_request_duration_seconds_bucket{...} ...
...
# HELP http_server_active_requests Number of active in-flight HTTP server requests
# TYPE http_server_active_requests gauge
...
```

## Key files

| File | Purpose |
|---|---|
| `app.py` | `create_app()` — wires `MeterProvider`, calls `create_varco_app(modules=[*default_modules(), MetricsModule()])` |
| `models.py` | `Post(AuditedDomainModel)` — simple domain model for traffic generation |
| `tests/test_smoke.py` | Verifies `/metrics` is mounted, middleware records instruments, CRUD works |

## Architecture

```
create_varco_app(modules=[*default_modules(), MetricsModule()])
  │
  ├─ MetricsMiddleware (ASGI middleware)
  │    └─ Records OTel instruments per request
  │         └─ MeterProvider(metric_readers=[PrometheusMetricReader])
  │                └─ PrometheusMetricReader → prometheus_client.REGISTRY
  │
  └─ MetricsRouter mounts GET /metrics
       └─ generate_latest() → Prometheus text format
```

## How the OTel → Prometheus pipeline works

1. `_setup_prometheus_meter_provider()` runs at startup, creating
   `PrometheusMetricReader` and `MeterProvider`, then calling
   `otel_metrics.set_meter_provider(provider)`.
2. `MetricsMiddleware` creates instruments **lazily** on the first request via
   `otel_metrics.get_meter(meter_name)` — obtaining them from the provider set
   in step 1.
3. `PrometheusMetricReader` was auto-registered with `prometheus_client.REGISTRY`
   at construction time — no explicit registration needed.
4. Every `generate_latest()` call (triggered by each Prometheus scrape of
   `GET /metrics`) pulls data from the registry, which pulls from the reader,
   which collects from the `MeterProvider`.

## Testing without prometheus_client

The smoke tests use `InMemoryMetricReader` and `mock.patch` on
`opentelemetry.metrics._internal.get_meter_provider` so the middleware
instruments flow into an in-process reader without any optional extras.
This is the same pattern used in `varco_fastapi/tests/milestone_g/test_metrics_endpoint.py`.

## OTel HTTP instruments

| Instrument | Type | Attributes |
|---|---|---|
| `http.server.request.duration` | Histogram (seconds) | method, route template, status code |
| `http.server.active_requests` | UpDownCounter | method only (route unknown at increment time) |
| `http.server.request.body.size` | Histogram (bytes) | method, route template |

The `/metrics` and `/health` paths are excluded from measurement by default
(`MetricsMiddleware(skip_paths=frozenset({"/metrics", "/health"}))`) to avoid
Prometheus scrapes inflating request counts.
