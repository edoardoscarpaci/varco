"""
varco_core.observability
=========================
OpenTelemetry tracing and metrics for varco services.

Public API
----------

Decorators::

    @span                          # wrap any function in an OTel span
    @span(SpanConfig(...))         # configured form

    @counter(CounterConfig(...))   # increment a counter on each successful call
    @histogram(HistogramConfig(...)) # record call duration as a histogram

Service mixin::

    class OrderService(
        TracingServiceMixin,        # auto-spans all CRUD methods
        AsyncService[Order, ...],
    ): ...

Config + DI::

    config = OtelConfig(
        service_name="orders-svc",
        otlp_endpoint="http://otel-collector:4317",
    )
    container.install(OtelConfiguration, config=config)
"""

from __future__ import annotations

from varco_core.observability.config import OtelConfig
from varco_core.observability.di import OtelConfiguration
from varco_core.observability.helpers import (
    create_counter,
    create_histogram,
    create_span,
)
from varco_core.observability.metrics import (
    CounterConfig,
    HistogramConfig,
    counter,
    histogram,
)
from varco_core.observability.metric import Metric, MetricKind, register_gauge
from varco_core.observability.mixin import TracingServiceMixin
from varco_core.observability.span import SpanConfig, span

__all__ = [
    # Config
    "OtelConfig",
    # DI
    "OtelConfiguration",
    # Tracing — decorator
    "span",
    "SpanConfig",
    # Tracing — context manager
    "create_span",
    # Metrics — decorators
    "counter",
    "CounterConfig",
    "histogram",
    "HistogramConfig",
    # Metrics — imperative helpers
    "create_counter",
    "create_histogram",
    # Service mixin
    "TracingServiceMixin",
    # Custom named metrics
    "Metric",
    "MetricKind",
    "register_gauge",
]
