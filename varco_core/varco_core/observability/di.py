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
    from providify import DIContainer, Provider
    from varco_core.observability import OtelConfig, OtelConfiguration

    # The override is a @Provider-decorated *module-level* function.
    # ⚠️ Not a lambda: DIContainer.provide() rejects undecorated callables.
    # ⚠️ Module level: under PEP 563 the return annotation is a lazy string
    #    that providify resolves against the function's __globals__, so a type
    #    imported inside a function body is invisible to it.
    @Provider(singleton=True)
    def otel_config() -> OtelConfig:
        return OtelConfig(
            service_name="orders-svc",
            service_version="1.0.0",
            otlp_endpoint="http://otel-collector:4317",
            extra_resource_attrs={
                "k8s.pod.name": os.environ.get("POD_NAME", "unknown"),
            },
        )

    container = DIContainer()
    container.provide(otel_config)      # any order — the default is @Fallback
    container.install(OtelConfiguration)

    tracer_provider = container.get(TracerProvider)

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

    @Provider(singleton=True)
    def otel_config() -> OtelConfig:
        return OtelConfig(service_name="my-svc")

    container.provide(otel_config)
    container.install(OtelConfiguration)

    # OtelConfiguration reads OtelConfig from the container — the explicit
    # provide() call above takes precedence over the module's own default.

``otel_config``'s default is declared ``@Fallback`` — register yours in any
order, before or after ``install(OtelConfiguration)``; a non-fallback
``OtelConfig`` binding always wins
(`technical_docs/features/dependency-injection.md`).

⚠️ ``DIContainer.install()`` takes the module class only — there is no
   ``install(OtelConfiguration, config=...)`` keyword.  Configuration is
   supplied by binding ``OtelConfig`` as shown above.

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
from typing import Any

from opentelemetry import metrics as otel_metrics
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from providify import Configuration, Disposes, Fallback, Inject, Provider

from varco_core.observability.attributes import (
    GlobalAttributes,
    configure_global_attributes,
    current_global_attributes,
    global_attributes,
    load_global_attributes_from_env,
    set_global_attributes,
)
from varco_core.observability.config import OtelConfig
from varco_core.observability.params import (
    set_capture_enabled,
    set_param_capture_defaults,
)

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

        @Provider(singleton=True)
        def otel_config() -> OtelConfig:
            return OtelConfig(
                service_name="orders-svc",
                otlp_endpoint="http://otel-collector:4317",
            )

        container = DIContainer()
        container.provide(otel_config)       # any order — @Fallback
        container.install(OtelConfiguration)
        tracer_provider = container.get(TracerProvider)
    """

    @Fallback
    @Provider(singleton=True)
    def otel_config(self) -> OtelConfig:
        """
        Default ``OtelConfig`` with no OTLP export and service name ``"varco"``.

        ``@Fallback`` — register yours in any order; it wins regardless of
        registration order (`technical_docs/features/dependency-injection.md`)::

            @Provider(singleton=True)
            def otel_config() -> OtelConfig:
                return OtelConfig(
                    service_name="orders-svc",
                    service_version="1.0.0",
                    otlp_endpoint="http://otel-collector:4317",
                )

            container.provide(otel_config)
            container.install(OtelConfiguration)

        The override must be a ``@Provider``-decorated module-level function
        (``provide()`` rejects undecorated callables, and a lazy PEP-563 return
        annotation only resolves against the function's own ``__globals__``).

        Returns:
            A minimal ``OtelConfig`` suitable for local development — records
            spans and metrics in memory but does not export them.
        """
        # Sensible local-dev default: records spans but never exports them.
        # Callers override this by providing their own OtelConfig binding, in any order.
        return OtelConfig(service_name="varco")

    @Provider(singleton=True)
    def observability_attributes(self, config: Inject[OtelConfig]) -> GlobalAttributes:
        """
        Seed the process-wide global attribute registry and parameter-capture
        defaults from ``config`` (Plan 004).

        ``tracer_provider`` and ``meter_provider`` both ``Inject`` this
        provider so DI resolution ordering is deterministic — the registry
        and capture defaults are always seeded before either provider is
        built, regardless of which one providify resolves first.

        Delegates to the module-level ``_apply_observability_config`` — the
        same synchronous entry point usable directly by no-DI callers/tests.

        Returns:
            The process-wide ``GlobalAttributes`` singleton.
        """
        return _apply_observability_config(config)

    @Provider(singleton=True)
    def tracer_provider(
        self, config: Inject[OtelConfig], _attrs: Inject[GlobalAttributes]
    ) -> TracerProvider:
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
            _attrs: Unused directly — ``Inject[GlobalAttributes]`` forces
                providify to resolve ``observability_attributes`` (which
                seeds the global attribute registry + capture defaults)
                before this provider runs, so ``promote_global_attrs_to_resource``
                sees a fully-seeded registry.

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
    def meter_provider(
        self, config: Inject[OtelConfig], _attrs: Inject[GlobalAttributes]
    ) -> MeterProvider:
        """
        Build, configure, and globally register the OTel ``MeterProvider``.

        Supports two export modes simultaneously — both can be active at once:

        **OTLP push** (``config.otlp_endpoint`` set):
            Attaches a ``PeriodicExportingMetricReader`` that pushes metrics to
            the OTLP collector every ``config.export_interval_ms`` ms.  No
            scrape endpoint needed — suitable for ephemeral/autoscaling pods.

        **Prometheus pull** (``config.prometheus_enabled=True``):
            Attaches a ``PrometheusMetricReader`` that registers with
            ``prometheus_client.REGISTRY`` at construction time.  Any code
            that calls ``prometheus_client.generate_latest()`` — including
            ``MetricsRouter``'s ``GET /metrics`` endpoint — will serve all
            OTel metrics as Prometheus text format.

        DESIGN: list-based readers instead of single reader
            ✅ OTLP push and Prometheus pull coexist — both readers active.
            ✅ Either reader can be None without affecting the other.
            ✅ MeterProvider accepts a list natively — no wrapping needed.
            ❌ Readers are immutable post-construction — cannot add
               PrometheusMetricReader retroactively without restart.
               This is an OTel SDK constraint, not a varco limitation.

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
            - ``config.otlp_endpoint=None`` and ``prometheus_enabled=False``
              → metrics are recorded in-memory only; use ``InMemoryMetricReader``
              in tests to inspect them.
            - ``config.prometheus_enabled=True`` but ``opentelemetry-exporter-
              prometheus`` not installed → logs an ERROR, skips the reader.
              The app starts normally; Prometheus metrics are silently lost.
            - Both ``otlp_endpoint`` and ``prometheus_enabled=True`` → both
              readers are attached; metrics flow to both destinations.
            - ``config.export_interval_ms`` too low → excessive OTLP traffic.
              Default 60 000 ms matches the Prometheus scrape default.

        Thread safety:  ✅ Created once by the Providify singleton mechanism.
        """
        resource = _build_resource(config)

        # Accumulate readers so OTLP push and Prometheus pull can coexist.
        # MeterProvider accepts an empty list — zero readers = in-memory only.
        readers: list[Any] = []

        if config.otlp_endpoint:
            # Lazy import — the OTLP exporter is an optional extra package.
            # Fails with a clear ImportError if not installed.
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                OTLPMetricExporter,
            )

            exporter = OTLPMetricExporter(endpoint=config.otlp_endpoint)
            # PeriodicExportingMetricReader pushes metrics on a background timer —
            # no scrape endpoint needed.  Compatible with ephemeral pods.
            readers.append(
                PeriodicExportingMetricReader(
                    exporter,
                    export_interval_millis=config.export_interval_ms,
                )
            )
            _logger.info(
                "OTel metrics OTLP configured: service=%r endpoint=%r interval_ms=%d",
                config.service_name,
                config.otlp_endpoint,
                config.export_interval_ms,
            )

        if config.prometheus_enabled:
            # PrometheusMetricReader auto-registers with prometheus_client.REGISTRY
            # at construction time — generate_latest() in MetricsRouter picks it up
            # automatically without needing a direct reference to this reader.
            try:
                from opentelemetry.exporter.prometheus import PrometheusMetricReader

                readers.append(PrometheusMetricReader())
                _logger.info(
                    "OTel metrics Prometheus configured: service=%r — /metrics enabled",
                    config.service_name,
                )
            except ImportError:
                # Missing optional dependency — degrade gracefully rather than
                # crashing the entire app.  The user can install the extra:
                #   pip install 'varco-fastapi[prometheus]'
                _logger.error(
                    "OtelConfiguration: prometheus_enabled=True but "
                    "'opentelemetry-exporter-prometheus' is not installed. "
                    "Prometheus /metrics will return no OTel data. "
                    "Fix: pip install 'varco-fastapi[prometheus]'"
                )

        provider = MeterProvider(resource=resource, metric_readers=readers)

        if not config.otlp_endpoint and not config.prometheus_enabled:
            _logger.info(
                "OTel metrics configured: service=%r (in-memory only — "
                "no otlp_endpoint and prometheus_enabled=False)",
                config.service_name,
            )

        # Register as process-global so @counter / @histogram decorators get
        # this provider via metrics.get_meter() without a direct reference.
        otel_metrics.set_meter_provider(provider)
        return provider

    @Disposes(TracerProvider)
    def shutdown_tracer_provider(self, provider: TracerProvider) -> None:
        """
        Flush and shut down the ``TracerProvider`` produced by
        ``tracer_provider`` on ``container.ashutdown()`` (Plan 050 /
        §D-disposes row 10).

        DESIGN: ``@Disposes`` over ``@PreDestroy`` (same as
        ``varco_redis/dlq.py``'s ``close_dlq``)
            ✅ providify never consults a ``@PreDestroy`` marker on a
               ``@Provider``-produced instance (Jakarta CDI producer rule) —
               ``@Disposes`` is the only reachable teardown hook.
            ✅ Without this, the last batch of spans buffered by the
               ``BatchSpanProcessor`` is silently dropped at shutdown — the
               bug this row closes.
            ❌ One more disposer to keep idempotent — ``shutdown()`` on the
               OTel SDK's own ``TracerProvider`` already is.

        Args:
            provider: The ``TracerProvider`` instance this configuration
                produced.

        Async safety: sync — the SDK's own ``shutdown()`` is sync.
        """
        provider.shutdown()

    @Disposes(MeterProvider)
    def shutdown_meter_provider(self, provider: MeterProvider) -> None:
        """
        Flush and shut down the ``MeterProvider`` produced by
        ``meter_provider`` on ``container.ashutdown()`` (Plan 050 /
        §D-disposes row 11) — same reasoning as ``shutdown_tracer_provider``.

        Args:
            provider: The ``MeterProvider`` instance this configuration
                produced.

        Async safety: sync — the SDK's own ``shutdown()`` is sync.
        """
        provider.shutdown()


# ── Helpers ───────────────────────────────────────────────────────────────────


def _apply_observability_config(config: OtelConfig) -> GlobalAttributes:
    """
    Apply the Plan 004 observability config to process-wide state.

    This is the shared, synchronous, DI-independent implementation behind
    ``OtelConfiguration.observability_attributes`` — a seam usable directly
    by no-DI callers and tests that want the same bootstrap behaviour
    without building a full container.

    Order (per Plan 004 — later steps win on key collision):
        1. ``load_global_attributes_from_env()`` — ambient
           ``VARCO_OTEL_GLOBAL_ATTRS*`` env vars first.
        2. ``set_global_attributes(config.global_attributes)`` — explicit
           config wins over ambient env.
        3. ``configure_global_attributes(apply_to_spans=..., apply_to_metrics=...)``
           from ``config.global_attributes_on_spans`` / ``_on_metrics``.
        4. Seed ``set_capture_enabled()`` / ``set_param_capture_defaults()``
           from ``config.capture_params`` / ``config.param_capture`` — only
           when explicitly set (``None`` leaves the process default/env-driven
           value untouched).

    Args:
        config: The ``OtelConfig`` to apply.

    Returns:
        The process-wide ``GlobalAttributes`` singleton.
    """
    load_global_attributes_from_env()
    if config.global_attributes:
        set_global_attributes(config.global_attributes)
    configure_global_attributes(
        apply_to_spans=config.global_attributes_on_spans,
        apply_to_metrics=config.global_attributes_on_metrics,
    )
    if config.capture_params is not None:
        set_capture_enabled(config.capture_params)
    if config.param_capture is not None:
        set_param_capture_defaults(config.param_capture)
    return global_attributes()


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

    # Plan 004 (B): opt-in promotion of the *global attribute registry*
    # (varco_core.observability.attributes) into the Resource — two
    # independent knobs, no silent double-labelling by default.
    if config.promote_global_attrs_to_resource:
        attrs.update(current_global_attributes())  # type: ignore[arg-type]

    # Resource.create() merges with OTEL_RESOURCE_ATTRIBUTES env var and
    # SDK-detected attributes (process.pid, telemetry.sdk.*, etc.).
    return Resource.create(attrs)


__all__ = ["OtelConfiguration"]
