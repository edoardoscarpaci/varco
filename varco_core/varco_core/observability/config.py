"""
varco_core.observability.config
================================
``OtelConfig`` — immutable bootstrap configuration for OpenTelemetry.

This dataclass is the single injectable settings object for the entire
observability stack.  It follows the same pattern as ``SAConfig`` in
``varco_sa``: one frozen dataclass that doubles as both the DI settings object
and the bootstrap config, avoiding a parallel "settings" class.

DESIGN: frozen dataclass instead of pydantic BaseSettings
    ✅ Immutable — callers can't mutate config after construction.
    ✅ Hashable — safe to use as dict key if needed.
    ✅ Zero runtime dependency on pydantic-settings (already a dep of
       varco_core, but keeping this layer pure makes it easier to lift
       the observability package into its own package later).
    ❌ No automatic env-var reading — callers must read os.environ themselves
       or wrap in a pydantic BaseSettings if they want that convenience.
       Trade-off accepted: keeps this module framework-agnostic.

DESIGN: single config object instead of separate TracerConfig + MeterConfig
    ✅ One install() call wires both tracing and metrics consistently.
    ✅ ``service_name`` and ``service_version`` are shared — no risk of typo
       divergence between the tracer and meter resource attributes.
    ❌ Users who only want tracing still carry the metrics fields (and vice
       versa).  Acceptable — the fields have sensible defaults and the DI
       module only creates providers for what the user actually needs.

Stateless / autoscaling notes
------------------------------
Each replica configures its own OTel providers at startup from its own
``OtelConfig``.  There is no shared in-process state between replicas — each
is an independent OS process with its own SDK state.

``extra_resource_attrs`` is the hook for injecting replica identity:

    config = OtelConfig(
        service_name="orders-svc",
        extra_resource_attrs={
            "k8s.pod.name":  os.environ["POD_NAME"],
            "k8s.node.name": os.environ["NODE_NAME"],
        },
    )

This stamps every span and metric with the originating pod so distributed
traces and per-pod metrics work correctly in Grafana / Tempo / Prometheus.
"""

from __future__ import annotations

from dataclasses import dataclass, field


# ── OtelConfig ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class OtelConfig:
    """
    Immutable bootstrap configuration for OpenTelemetry tracing and metrics.

    Pass this to ``OtelConfiguration`` (the DI module) to wire both a
    ``TracerProvider`` and a ``MeterProvider`` into the OTel global API so
    that all ``@span``, ``@counter``, and ``@histogram`` decorators in the
    process pick them up automatically.

    Args:
        service_name:
            Human-readable name that identifies this service in traces and
            metrics (e.g. ``"orders-svc"``).  Becomes the ``service.name``
            OTel resource attribute — required by most backends.
        service_version:
            Semantic version of the running binary (e.g. ``"1.2.3"``).
            Useful for correlating regressions with deployments.
        otlp_endpoint:
            gRPC endpoint of the OpenTelemetry Collector
            (e.g. ``"http://otel-collector:4317"``).  Set to ``None`` to
            disable export — spans and metrics are still recorded in memory
            but never sent anywhere.  Useful for local development and tests.
        tracer_name:
            Default tracer name used by ``@span`` when ``SpanConfig.tracer_name``
            is not overridden.  Identifies the *instrumentation library* in
            the OTel data model — use a stable reverse-DNS-style string.
        meter_name:
            Default meter name used by ``@counter`` and ``@histogram`` when
            their config does not override it.  Same naming convention as
            ``tracer_name``.
        export_interval_ms:
            How often the periodic metric reader pushes metrics to the
            collector (milliseconds).  Lower values → more real-time
            dashboards but higher network overhead.  Default is 60 000 ms
            (60 s), which matches the Prometheus scrape default.
        extra_resource_attrs:
            Additional OTel resource attributes stamped on every span and
            metric produced by this process.  Use this to inject
            infrastructure-level identity that OTel cannot discover
            automatically::

                extra_resource_attrs={
                    "k8s.pod.name":       os.environ["POD_NAME"],
                    "k8s.node.name":      os.environ["NODE_NAME"],
                    "deployment.version": os.environ["APP_VERSION"],
                }

            In autoscaling clusters this is the primary way to distinguish
            metrics from different replicas of the same service.

    Edge cases:
        - ``service_name`` empty string → accepted but will look odd in most
          backends; callers are responsible for providing a meaningful name.
        - ``otlp_endpoint=None`` → no exporter attached; the SDK still
          records spans (useful with ``InMemorySpanExporter`` in tests).
        - ``export_interval_ms=0`` → not validated here; the OTel SDK will
          raise at provider construction time.
        - ``extra_resource_attrs`` values are always strings — OTel resource
          attributes must be strings per the semantic conventions spec.

    Thread safety:  ✅ Frozen dataclass — immutable after construction.
    Async safety:   ✅ Stateless value object — safe to share across tasks.

    Example::

        import os
        from varco_core.observability import OtelConfig

        config = OtelConfig(
            service_name="orders-svc",
            service_version="1.0.0",
            otlp_endpoint="http://otel-collector:4317",
            extra_resource_attrs={
                "k8s.pod.name": os.environ.get("POD_NAME", "unknown"),
            },
        )
    """

    # ── Required ──────────────────────────────────────────────────────────────

    service_name: str

    # ── Optional — sensible defaults for local development ────────────────────

    service_version: str = "0.0.0"

    # None means "don't export" — useful in tests and local dev where there is
    # no collector running.
    otlp_endpoint: str | None = None

    # Default tracer / meter names — can be overridden per-decorator via
    # SpanConfig.tracer_name / CounterConfig.meter_name.
    tracer_name: str = "varco"
    meter_name: str = "varco"

    # 60 000 ms matches the Prometheus default scrape interval — a sensible
    # starting point that balances freshness against collector load.
    export_interval_ms: int = 60_000

    # Pod/node identity and any other infrastructure attributes the OTel SDK
    # cannot discover automatically from the environment.
    extra_resource_attrs: dict[str, str] = field(default_factory=dict)


__all__ = ["OtelConfig"]
