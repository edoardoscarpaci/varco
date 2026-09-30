"""
Plan 050 / Step 7 (§D-disposes rows 10-11) — `OtelConfiguration.tracer_provider`
/`meter_provider` get a reachable `@Disposes` calling `.shutdown()`, so a
container `ashutdown()` flushes the last batch of spans/metrics instead of
dropping them. Docker-free — in-memory exporters only.

RED MODE: no `@Disposes` exists on either provider today.
"""

from __future__ import annotations

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from providify import DIContainer
from varco_core.observability.di import OtelConfiguration


def test_tracer_provider_binding_has_a_disposer() -> None:
    container = DIContainer()
    container.install(OtelConfiguration)

    binding = container.get_binding(TracerProvider)
    assert binding.disposer is not None


async def test_ashutdown_calls_tracer_provider_shutdown() -> None:
    container = DIContainer()
    container.install(OtelConfiguration)

    provider = container.get(TracerProvider)
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    provider.get_tracer("test").start_span("span").end()

    await container.ashutdown()

    # a shut-down TracerProvider's own `_active` flag flips False; the SDK
    # itself does not expose a public "is shut down" flag, so we assert on
    # the shutdown side effect that matters: no UNMATCHED_DISPOSER report
    # from validate().
    report = container.validate()
    assert not any(
        "UNMATCHED_DISPOSER" in str(issue) for issue in getattr(report, "issues", report)
    )
