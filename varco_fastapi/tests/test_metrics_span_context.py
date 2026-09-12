"""
Tests for Plan 041 / S17, §D-S17-decision — ``MetricsMiddleware`` must record
its ``http.server.request.duration`` histogram with a live, sampled OTel span
current in context, so exemplars become reachable.

Uses a real ``TracerProvider`` (always-on sampler, the SDK default) plus an
``InMemoryMetricReader``-backed ``MeterProvider``, wired via
``create_varco_app(enable_tracing=True, enable_metrics=True, ...)`` — the
production assembly path, not a hand-built middleware stack, so the test
actually exercises the ordering decision in ``app.py``.

Fixture shape reused from ``varco_fastapi/tests/milestone_g/test_metrics_middleware.py``
and ``examples/03-observability-metrics/tests/test_smoke.py`` — heed
``examples/FINDINGS.md`` F23: patch the internal getter
(``opentelemetry.metrics._internal.get_meter_provider``), never call
``set_meter_provider()`` twice (it is a one-way door once a non-default
provider is set).

RED before Phase 1 Step 3 (the ``app.py`` reorder): today ``MetricsMiddleware``
is OUTSIDE ``TracingMiddleware``, so at ``record()`` time there is no current
span — assertion (a) fails with an invalid ``SpanContext``.
"""

from __future__ import annotations

import importlib.metadata
import unittest.mock as mock

import pytest
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from varco_fastapi.app import create_varco_app
from varco_fastapi.middleware import metrics as metrics_module


def _otel_sdk_version() -> tuple[int, ...]:
    raw = importlib.metadata.version("opentelemetry-sdk")
    parts: list[int] = []
    for chunk in raw.split(".")[:3]:
        digits = "".join(ch for ch in chunk if ch.isdigit())
        parts.append(int(digits) if digits else 0)
    return tuple(parts)


# Exemplars landed in opentelemetry-sdk 1.28.0 (§D-S17-sdkfloor). A
# floor-compliant install (>=1.20, per varco_core/pyproject.toml) may predate
# that — the capability-guarded assertion skips rather than fails there.
_SDK_HAS_EXEMPLAR_SUPPORT = _otel_sdk_version() >= (1, 28, 0)


@pytest.fixture(autouse=True)
def _clear_instrument_cache():
    """Module-level instrument cache must not bleed provider state across tests."""
    metrics_module._instruments.clear()
    yield
    metrics_module._instruments.clear()


@pytest.fixture()
def metric_reader():
    """Fresh InMemoryMetricReader + MeterProvider, isolated per test (FINDINGS F23)."""
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    with mock.patch(
        "opentelemetry.metrics._internal.get_meter_provider",
        return_value=provider,
    ):
        yield reader


@pytest.fixture()
def tracer_provider():
    """Real TracerProvider, always-on sampler — every request gets a sampled span."""
    provider = TracerProvider(sampler=ALWAYS_ON)
    with mock.patch("opentelemetry.trace.get_tracer_provider", return_value=provider):
        yield provider


def _build_app() -> object:
    app = create_varco_app(
        enable_tracing=True,
        enable_metrics=True,
        enable_logging=False,
        enable_error_middleware=False,
        security_headers=False,
        body_limit=False,
        configure_jwt=False,
        validate=False,
    )

    @app.get("/ping")
    def ping() -> dict:
        return {"ok": True}

    return app


class TestMetricsRecordsInsideSpanContext:
    def test_current_span_at_record_time_has_valid_span_context(
        self, metric_reader: InMemoryMetricReader, tracer_provider: TracerProvider
    ) -> None:
        """
        Unconditional assertion (Step 1a): during a request, the span current
        at the point MetricsMiddleware records its duration histogram must be
        a real, valid span — proof that metrics recording happens INSIDE
        TracingMiddleware's span context, not outside it.
        """
        captured_span_contexts: list[trace.SpanContext] = []
        real_get_duration_histogram = metrics_module._get_duration_histogram

        def _spying_get_duration_histogram(meter_name: str):
            captured_span_contexts.append(trace.get_current_span().get_span_context())
            return real_get_duration_histogram(meter_name)

        app = _build_app()
        with mock.patch.object(
            metrics_module, "_get_duration_histogram", side_effect=_spying_get_duration_histogram
        ):
            with TestClient(app) as client:
                response = client.get("/ping")
                assert response.status_code == 200

        assert len(captured_span_contexts) == 1
        span_context = captured_span_contexts[0]
        assert span_context.is_valid, (
            "MetricsMiddleware recorded its duration histogram with no current "
            "span in OTel context — it must run INSIDE TracingMiddleware "
            "(Plan 041 / S17, §D-S17-decision)."
        )

    @pytest.mark.skipif(
        not _SDK_HAS_EXEMPLAR_SUPPORT,
        reason="opentelemetry-sdk < 1.28.0 installed — no exemplar support (§D-S17-sdkfloor)",
    )
    def test_duration_histogram_datapoint_carries_matching_exemplar(
        self, metric_reader: InMemoryMetricReader, tracer_provider: TracerProvider
    ) -> None:
        """
        Capability-guarded assertion (Step 1b): the collected
        http.server.request.duration data point must carry a non-empty
        exemplar whose trace_id matches the request's own span — proof that
        exemplars are reachable end-to-end at the SDK layer.
        """
        recorded_trace_ids: list[int] = []
        real_get_duration_histogram = metrics_module._get_duration_histogram

        def _spying_get_duration_histogram(meter_name: str):
            current_span = trace.get_current_span()
            recorded_trace_ids.append(current_span.get_span_context().trace_id)
            return real_get_duration_histogram(meter_name)

        app = _build_app()
        with mock.patch.object(
            metrics_module, "_get_duration_histogram", side_effect=_spying_get_duration_histogram
        ):
            with TestClient(app) as client:
                response = client.get("/ping")
                assert response.status_code == 200

        assert len(recorded_trace_ids) == 1
        request_trace_id = recorded_trace_ids[0]

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None

        duration_points = []
        for resource_metrics in metrics_data.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    if metric.name == "http.server.request.duration":
                        duration_points.extend(metric.data.data_points)

        assert len(duration_points) == 1
        data_point = duration_points[0]

        assert data_point.exemplars, (
            "http.server.request.duration data point carries no exemplars — "
            "expected one linking back to the request's sampled span."
        )
        exemplar_trace_ids = {ex.trace_id for ex in data_point.exemplars}
        assert request_trace_id in exemplar_trace_ids
