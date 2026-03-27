"""
varco_core.observability.di
============================
Providify DI wiring for OpenTelemetry tracing and metrics.

``OtelConfiguration``
    ``@Configuration`` class that builds a ``TracerProvider`` and a
    ``MeterProvider`` from an ``OtelConfig``, registers them as the OTel
    global providers, and exposes them as injectable singletons.

Usage (most common — OTLP export to a collector)::

    import os
    from varco_core.observability import OtelConfig, OtelConfiguration

    container = DIContainer()
    container.install(
        OtelConfiguration,
        config=OtelConfig(
            service_name="orders-svc",
            service_version="1.0.0",
            otlp_endpoint="http://otel-collector:4317",
            extra_resource_attrs={
                "k8s.pod.name": os.environ.get("POD_NAME", "unknown"),
            },
        ),
    )

Usage (tests / local dev — no export, use InMemorySpanExporter)::

    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    # No need to install OtelConfiguration at all in this case.

Overriding the default config::

    container.provide(lambda: OtelConfig(service_name="my-svc"))
    container.install(OtelConfiguration)

    # OtelConfiguration reads OtelConfig from the container — the explicit
    # provide() call above takes precedence over the default provider.

DESIGN: OtelConfiguration is synchronous (uses container.install, not ainstall)
    OTel SDK setup — creating providers, attaching exporters — is entirely
    synchronous.  Using ainstall() would impose an unnecessary async
    constraint on callers and add confusion.
    ✅ ``container.install(OtelConfiguration)`` — simple, no await needed.
    ❌ Cannot export spans over a transport that requires an async connection
       at setup time (e.g. gRPC with async handshake).  In practice the OTLP
       gRPC exporter opens connections lazily on first export — not at SDK init.

DESIGN: set_tracer_provider / set_meter_provider (OTel global API)
    Both providers are registered as the OTel process-global provider so
    that all @span / @counter / @histogram decorators pick them up via
    trace.get_tracer() / metrics.get_meter() without needing a reference
    to the provider object.
    ✅ Decorators work anywhere in the process with zero configuration at
       the call site.
    ❌ Only one provider can be active per process — installing
       OtelConfiguration twice would silently replace the first provider.
       Guard against this in tests by installing it in a fixture with scope
       "session" rather than per-test.

DESIGN: OTel Resource — attaching service identity to every span/metric
    A Resource is built from ``OtelConfig`` fields and merged with
    ``Resource.create()`` so that OTel SDK auto-detected attributes
    (e.g. ``process.pid``, ``telemetry.sdk.version``) are also present.
    ✅ ``service.name`` and ``service.version`` appear on every span and
       metric automatically — no per-call boilerplate.
    ✅ ``extra_resource_attrs`` (e.g. pod name, node name) identify the
       replica that produced each span in autoscaling clusters.
    ❌ Resource is immutable — runtime changes (e.g. live migration) are
       not reflected.  In practice, pod identity doesn't change mid-lifecycle.

Thread safety:  ✅ Providify singletons are created once and cached.
Async safety:   ✅ All providers are synchronous — no async setup needed.
"""

from __future__ import annotations

import logging

from opentelemetry import metrics as otel_metrics
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from providify import Configuration, Inject, Provider

from varco_core.observability.config import OtelConfig

_logger = logging.getLogger(__name__)


# ── OtelConfiguration ─────────────────────────────────────────────────────────


@Configuration
class OtelConfiguration:
    """
    Providify ``@Configuration`` that wires OTel tracing and metrics.

    Provides:
        ``OtelConfig``      — default config pointing at localhost with no
                              export; override by providing your own before
                              installing this configuration.
        ``TracerProvider``  — SDK provider configured with the Resource and
                              (optionally) an OTLP span exporter.  Also
                              registered as the OTel global tracer provider.
        ``MeterProvider``   — SDK provider configured with the Resource and
                              (optionally) a periodic OTLP metric reader.
                              Also registered as the OTel global meter provider.

    Lifecycle:
        Both providers expose ``shutdown()`` — call them at process exit to
        flush pending spans and metrics.  The OTel SDK also registers an
        ``atexit`` handler automatically for graceful shutdown.

    Thread safety:  ✅ Providify singletons are created once and cached.
    Async safety:   ✅ Synchronous providers — no async lifecycle needed.

    Example::

        container = DIContainer()
        container.install(
            OtelConfiguration,
            config=OtelConfig(
                service_name="orders-svc",
                otlp_endpoint="http://otel-collector:4317",
            ),
        )
        tracer_provider = container.get(TracerProvider)
    """

    @Provider(singleton=True)
    def otel_config(self) -> OtelConfig:
        """
        Default ``OtelConfig`` with no OTLP export and service name ``"varco"``.

        Override before installing this configuration to supply a real
        service name, version, and collector endpoint::

            container.provide(lambda: OtelConfig(
                service_name="orders-svc",
                service_version="1.0.0",
                otlp_endpoint="http://otel-collector:4317",
            ))
            container.install(OtelConfiguration)

        Returns:
            A minimal ``OtelConfig`` suitable for local development — records
            spans and metrics in memory but does not export them.
        """
        # Sensible local-dev default: records spans but never exports them.
        # Callers override this by providing their own OtelConfig before install.
        return OtelConfig(service_name="varco")

    @Provider(singleton=True)
    def tracer_provider(self, config: Inject[OtelConfig]) -> TracerProvider:
        """
        Build, configure, and globally register the OTel ``TracerProvider``.

        Attaches a ``BatchSpanProcessor`` with an OTLP gRPC span exporter when
        ``config.otlp_endpoint`` is set.  If no endpoint is configured, spans
        are recorded in memory only — useful in tests and local dev.

        Calling ``trace.set_tracer_provider()`` makes this the process-global
        provider so all ``@span`` decorators pick it up via
        ``trace.get_tracer(name)`` without needing a direct reference.

        Args:
            config: The injectable ``OtelConfig`` resolved from the container.

        Returns:
            A started ``TracerProvider`` registered as the OTel global.

        Raises:
            ImportError:  If ``opentelemetry-exporter-otlp-proto-grpc`` is not
                          installed and ``config.otlp_endpoint`` is set.

        Edge cases:
            - ``config.otlp_endpoint=None`` → no exporter; provider records
              spans to a no-op exporter.  Use ``InMemorySpanExporter`` in tests.
            - Installing OtelConfiguration twice → second call replaces the
              global provider.  Use session-scoped fixtures in pytest to avoid.

        Thread safety:  ✅ Created once by the Providify singleton mechanism.
        """
        # Build an OTel Resource that stamps every span with service identity.
        # Resource.create() merges our attrs with SDK auto-detected attributes
        # (process.pid, telemetry.sdk.language, telemetry.sdk.version, etc.)
        resource = _build_resource(config)

        provider = TracerProvider(resource=resource)

        if config.otlp_endpoint:
            # Lazy import — the OTLP exporter is an optional extra package.
            # Fails with a clear ImportError if not installed.
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )

            exporter = OTLPSpanExporter(endpoint=config.otlp_endpoint)
            # BatchSpanProcessor buffers spans and exports in batches —
            # more efficient than SimpleSpanProcessor for production use.
            provider.add_span_processor(BatchSpanProcessor(exporter))
            _logger.info(
                "OTel tracing configured: service=%r endpoint=%r",
                config.service_name,
                config.otlp_endpoint,
            )
        else:
            _logger.info(
                "OTel tracing configured: service=%r (no export — no otlp_endpoint set)",
                config.service_name,
            )

        # Register as the process-global tracer provider so @span decorators
        # call trace.get_tracer() and get this provider automatically.
        trace.set_tracer_provider(provider)
        return provider

    @Provider(singleton=True)
    def meter_provider(self, config: Inject[OtelConfig]) -> MeterProvider:
        """
        Build, configure, and globally register the OTel ``MeterProvider``.

        Attaches a ``PeriodicExportingMetricReader`` with an OTLP gRPC metric
        exporter when ``config.otlp_endpoint`` is set.  The reader pushes
        metrics every ``config.export_interval_ms`` milliseconds — no scrape
        endpoint needed, suitable for ephemeral/autoscaling pods.

        Calling ``otel_metrics.set_meter_provider()`` makes this the process-
        global provider so all ``@counter`` and ``@histogram`` decorators pick
        it up via ``metrics.get_meter(name)``.

        Args:
            config: The injectable ``OtelConfig`` resolved from the container.

        Returns:
            A ``MeterProvider`` registered as the OTel global.

        Raises:
            ImportError:  If ``opentelemetry-exporter-otlp-proto-grpc`` is not
                          installed and ``config.otlp_endpoint`` is set.

        Edge cases:
            - ``config.otlp_endpoint=None`` → metrics are recorded locally but
              never exported.  Use ``InMemoryMetricReader`` in tests.
            - ``config.export_interval_ms`` too low → excessive network traffic.
              Default 60 000 ms matches the Prometheus scrape default.

        Thread safety:  ✅ Created once by the Providify singleton mechanism.
        """
        resource = _build_resource(config)

        if config.otlp_endpoint:
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                OTLPMetricExporter,
            )

            exporter = OTLPMetricExporter(endpoint=config.otlp_endpoint)
            # PeriodicExportingMetricReader pushes metrics on a background timer —
            # no scrape endpoint needed.  Compatible with ephemeral pods.
            reader = PeriodicExportingMetricReader(
                exporter,
                export_interval_millis=config.export_interval_ms,
            )
            provider = MeterProvider(resource=resource, metric_readers=[reader])
            _logger.info(
                "OTel metrics configured: service=%r endpoint=%r interval_ms=%d",
                config.service_name,
                config.otlp_endpoint,
                config.export_interval_ms,
            )
        else:
            provider = MeterProvider(resource=resource)
            _logger.info(
                "OTel metrics configured: service=%r (no export — no otlp_endpoint set)",
                config.service_name,
            )

        # Register as process-global so @counter / @histogram decorators get
        # this provider via metrics.get_meter() without a direct reference.
        otel_metrics.set_meter_provider(provider)
        return provider


# ── Helpers ───────────────────────────────────────────────────────────────────


def _build_resource(config: OtelConfig) -> Resource:
    """
    Build an OTel ``Resource`` from ``config``.

    Merges user-provided attributes with OTel SDK auto-detected attributes
    (process PID, SDK version, etc.) via ``Resource.create()``.

    Args:
        config: The ``OtelConfig`` providing service identity and extra attrs.

    Returns:
        An immutable ``Resource`` object stamped on every span and metric.
    """
    # OTel semantic convention keys for service identity.
    attrs: dict[str, str] = {
        "service.name": config.service_name,
        "service.version": config.service_version,
    }
    # Merge extra attrs last so callers can override service.name / service.version
    # if they need to (unusual but allowed).
    attrs.update(config.extra_resource_attrs)

    # Resource.create() merges with OTEL_RESOURCE_ATTRIBUTES env var and
    # SDK-detected attributes (process.pid, telemetry.sdk.*, etc.).
    return Resource.create(attrs)


__all__ = ["OtelConfiguration"]
