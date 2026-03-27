"""
varco_core.observability.helpers
=================================
Low-level helpers for manual span and metric creation.

Use these when the ``@span`` / ``@counter`` / ``@histogram`` decorators are
not sufficient — for example when you need to span only *part* of a function
body, or when you need to create a custom metric instrument once and record
multiple observations at different times.

Public API
----------
``create_span(name, ...)``
    Async/sync context manager that opens a named OTel span for the duration
    of its block.  Supports dynamic attributes (set inside the block via the
    yielded span object) and automatic correlation ID bridging.

``create_counter(name, ...)``
    Create (or retrieve from the lazy cache) an OTel ``Counter`` instrument.
    Returns the raw instrument so the caller controls when and how to record.

``create_histogram(name, ...)``
    Create (or retrieve from the lazy cache) an OTel ``Histogram`` instrument.

Usage — span context manager::

    from varco_core.observability.helpers import create_span

    async def process_order(order: Order) -> None:
        # Span only the slow part, not the full function
        with create_span("order.validate") as span:
            result = await validate(order)
            span.set_attribute("validation.rules_checked", len(result.rules))

        await save(order)   # not spanned — fast path, no need

    # Or nest spans to build a trace tree:
    with create_span("order.process") as parent:
        with create_span("order.validate"):
            await validate(order)
        with create_span("order.save"):
            await save(order)

Usage — custom metric instrument::

    from varco_core.observability.helpers import create_counter, create_histogram

    # Create once (or let the module cache handle it)
    _active_connections = create_counter(
        "db.connections.active",
        description="Active database connections",
        unit="1",
    )
    _query_duration = create_histogram(
        "db.query.duration",
        description="Database query duration",
        unit="s",
    )

    async def execute_query(sql: str) -> Any:
        _active_connections.add(1)
        start = time.perf_counter()
        try:
            return await _db.execute(sql)
        finally:
            _query_duration.record(time.perf_counter() - start)
            _active_connections.add(-1)   # counter used as gauge (pattern)

DESIGN: separate helpers module instead of adding to span.py / metrics.py
    ✅ Clear separation: @span/@counter/@histogram are the decorator API;
       create_span/create_counter/create_histogram are the imperative API.
    ✅ Users who only need decorators don't import this module at all.
    ✅ create_counter / create_histogram reuse the same lazy cache as the
       decorator internals — creating the same instrument twice is safe and
       idempotent (returns the same cached instance).
    ❌ One extra import for users who want both APIs.  Acceptable — explicit
       imports are more readable than one giant module.

Thread safety:  ✅ create_span is a context manager — each ``with`` block
                   creates its own span via the OTel tracer; no shared state.
Async safety:   ✅ The context manager is a regular ``contextlib.contextmanager``
                   — safe inside async functions because the span is started and
                   finished within a single ``await``-free block.  If you need
                   to span across multiple ``await`` points, use ``@span`` or
                   open the span manually with ``tracer.start_as_current_span()``.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

from opentelemetry import metrics as otel_metrics
from opentelemetry import trace
from opentelemetry.trace import Span, StatusCode

from varco_core.tracing import current_correlation_id

# Reuse the lazy instrument cache from metrics.py — same key format.
from varco_core.observability.metrics import _instrument_cache


# ── create_span ───────────────────────────────────────────────────────────────


@contextmanager
def create_span(
    name: str,
    *,
    tracer_name: str = "varco",
    attributes: dict[str, str] | None = None,
    record_exception: bool = True,
    set_status_on_error: bool = True,
) -> Generator[Span, None, None]:
    """
    Context manager that opens a named OTel span for the duration of its block.

    Unlike the ``@span`` decorator (which spans an entire function call),
    ``create_span`` lets you instrument an arbitrary code block — including
    only part of a function body.  The yielded ``Span`` object accepts dynamic
    attribute calls so you can stamp runtime values (entity IDs, result counts,
    etc.) after they become available.

    Automatically bridges the active correlation ID to the span as a
    ``correlation_id`` attribute when one is set in the current async context.

    The span also becomes the *current* span in OTel context for its duration,
    so any child ``create_span`` or ``@span``-decorated call inside the block
    will be recorded as a child span — forming a trace tree automatically.

    Args:
        name:
            Span name as it appears in the trace UI
            (e.g. ``"order.validate"``).
        tracer_name:
            OTel instrumentation library name.  Defaults to ``"varco"``.
        attributes:
            Optional dict of static string attributes added at span creation
            time.  Dynamic attributes should be set on the yielded span via
            ``span.set_attribute(key, value)`` inside the ``with`` block.
        record_exception:
            When ``True`` (default), exceptions that propagate out of the
            ``with`` block are recorded on the span via
            ``span.record_exception(exc)``.  The exception is always
            re-raised — this context manager never swallows exceptions.
        set_status_on_error:
            When ``True`` (default), sets the span status to ``ERROR`` if
            an exception propagates.

    Yields:
        The active ``opentelemetry.trace.Span`` object for the duration of
        the block.  Use it to set dynamic attributes::

            with create_span("my.op") as s:
                result = do_work()
                s.set_attribute("result.count", str(len(result)))

    Raises:
        Any exception raised inside the ``with`` block — always re-raised
        after being optionally recorded on the span.

    Edge cases:
        - Nested ``create_span`` calls create parent-child spans automatically
          via OTel context propagation — no explicit parent reference needed.
        - If no ``TracerProvider`` is configured (e.g. in tests without DI),
          OTel returns a no-op tracer and the span is a no-op.
        - ``create_span`` is a synchronous context manager (``with``, not
          ``async with``).  It is safe inside ``async def`` functions as long
          as you do not hold the span across unrelated ``await`` points.

    Thread safety:  ✅ Each ``with`` block creates its own independent span.
    Async safety:   ⚠️ Safe within a single coroutine's synchronous block.
                       Do not yield from inside the ``with`` block to a
                       different task — OTel context is task-local.

    Example::

        from varco_core.observability.helpers import create_span

        async def ship_order(order_id: UUID) -> None:
            with create_span("order.ship", attributes={"queue": "shipping"}) as s:
                label = await _assign_carrier(order_id)
                s.set_attribute("carrier", label)   # dynamic — known only after await
    """
    tracer = trace.get_tracer(tracer_name)
    with tracer.start_as_current_span(name, record_exception=False) as span:
        # Set static attributes supplied at call time.
        for k, v in (attributes or {}).items():
            span.set_attribute(k, v)

        # Bridge correlation ID → span attribute.
        cid = current_correlation_id()
        if cid is not None:
            span.set_attribute("correlation_id", cid)

        try:
            yield span
        except Exception as exc:
            if record_exception:
                span.record_exception(exc)
            if set_status_on_error:
                span.set_status(StatusCode.ERROR, str(exc))
            raise


# ── create_counter ────────────────────────────────────────────────────────────


def create_counter(
    name: str,
    *,
    meter_name: str = "varco",
    description: str = "",
    unit: str = "1",
) -> Any:
    """
    Create (or retrieve) a named OTel ``Counter`` instrument.

    The instrument is created lazily on the first call and cached in
    ``_instrument_cache`` so subsequent calls with the same
    ``(meter_name, name)`` return the same instance.  This matches the
    behaviour of the ``@counter`` decorator.

    Use this when you need direct access to the instrument to call
    ``.add()`` with dynamic attributes or at arbitrary points in time::

        _orders_created = create_counter("orders.created")

        async def create_order(dto: CreateOrderDTO, tenant: str) -> Order:
            order = await _repo.create(dto)
            # Dynamic attribute — known only at call time
            _orders_created.add(1, attributes={"tenant": tenant})
            return order

    Args:
        name:
            Metric name following OTel naming conventions
            (e.g. ``"orders.created"``).
        meter_name:
            OTel meter / instrumentation library name.  Defaults to
            ``"varco"``.
        description:
            Human-readable description shown in metric explorer UIs.
        unit:
            OTel unit string.  ``"1"`` is correct for a dimensionless count.

    Returns:
        An ``opentelemetry.metrics.Counter`` instrument.

    Edge cases:
        - Calling with the same ``name`` and ``meter_name`` twice returns the
          same cached instrument — idempotent.
        - No active ``MeterProvider`` → OTel returns a no-op meter; the
          instrument is a no-op but the call succeeds.

    Thread safety:  ✅ Cache write is idempotent under GIL (CPython).
    Async safety:   ✅ Stateless after first-call creation.
    """
    key = (meter_name, name)
    instrument = _instrument_cache.get(key)
    if instrument is None:
        meter = otel_metrics.get_meter(meter_name)
        instrument = meter.create_counter(
            name=name,
            description=description,
            unit=unit,
        )
        _instrument_cache[key] = instrument
    return instrument


# ── create_histogram ──────────────────────────────────────────────────────────


def create_histogram(
    name: str,
    *,
    meter_name: str = "varco",
    description: str = "",
    unit: str = "s",
) -> Any:
    """
    Create (or retrieve) a named OTel ``Histogram`` instrument.

    Same lazy-cache semantics as ``create_counter``.  Use this when you need
    direct access to the instrument — for example to record values that are
    not simple call durations::

        _payload_bytes = create_histogram(
            "kafka.message.payload.bytes",
            description="Kafka message payload size",
            unit="By",  # bytes per OTel semantic conventions
        )

        async def publish(event: Event, channel: str) -> None:
            payload = serialize(event)
            await _bus.publish(event, channel=channel)
            _payload_bytes.record(len(payload), attributes={"channel": channel})

    Args:
        name:
            Metric name (e.g. ``"orders.list.duration"``).
        meter_name:
            OTel meter name.  Defaults to ``"varco"``.
        description:
            Human-readable description.
        unit:
            OTel unit.  ``"s"`` (seconds) is the default — correct for
            duration histograms per OTel semantic conventions.  Use ``"By"``
            for byte sizes, ``"ms"`` for milliseconds, etc.

    Returns:
        An ``opentelemetry.metrics.Histogram`` instrument.

    Edge cases:
        - Same idempotent caching as ``create_counter``.
        - No ``MeterProvider`` → no-op instrument.

    Thread safety:  ✅ Cache write is idempotent under GIL.
    Async safety:   ✅ Stateless after first-call creation.
    """
    key = (meter_name, name)
    instrument = _instrument_cache.get(key)
    if instrument is None:
        meter = otel_metrics.get_meter(meter_name)
        instrument = meter.create_histogram(
            name=name,
            description=description,
            unit=unit,
        )
        _instrument_cache[key] = instrument
    return instrument


__all__ = [
    "create_counter",
    "create_histogram",
    "create_span",
]
