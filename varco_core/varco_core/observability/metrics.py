"""
varco_core.observability.metrics
=================================
``@counter`` and ``@histogram`` — OpenTelemetry metrics decorators.

Wrap any sync or async callable to automatically record OTel metrics on
each call:

``@counter``
    Increments a named ``Counter`` instrument by 1 after each **successful**
    call.  Exceptions do NOT increment the counter — use a separate error
    counter if you need to track failure rates.

``@histogram``
    Records the wall-clock **duration** of each call (success or failure) as
    a ``Histogram`` observation in seconds.  Duration is measured with
    ``time.perf_counter()`` for monotonic, high-resolution timing.

Usage::

    from varco_core.observability import counter, histogram, CounterConfig, HistogramConfig

    @counter(CounterConfig(name="orders.created"))
    async def create_order(dto: CreateOrderDTO) -> OrderReadDTO: ...

    @histogram(HistogramConfig(name="orders.list.duration"))
    async def list_orders(params: QueryParams) -> list[OrderReadDTO]: ...

    # Stack freely — both can be combined on the same function:
    @counter(CounterConfig(name="payments.charged"))
    @histogram(HistogramConfig(name="payments.charge.duration"))
    async def charge_card(amount: float) -> TransactionId: ...

DESIGN: lazy instrument creation (first-call, not at decoration time)
    OTel metrics instruments must be obtained from the global ``MeterProvider``
    which is configured by ``OtelConfiguration`` at app startup.  Decorators
    are evaluated at *import* time, before the provider is set up.  Creating
    instruments eagerly would capture the no-op ``MeterProvider`` that OTel
    registers before any real provider is installed.

    Lazy creation (first call) guarantees the instrument is obtained from the
    real provider.
    ✅ Instruments are created from the live provider — data is actually exported.
    ✅ No ordering constraint between module import and DI bootstrap.
    ❌ A tiny extra dict lookup on every call to check if the instrument exists.
       In practice this is negligible — it is a single dict.get() call.

    Alternative considered: module-level ``get_meter(tracer_name)`` at
    decoration time — rejected because it always captures the no-op provider.

DESIGN: counter increments only on success
    ✅ ``orders.created`` counter means "orders actually created" — not
       "create_order called".  Failed calls pollute the semantic.
    ❌ No built-in error counter — callers add one explicitly if needed.
       This is intentional: the decorator cannot know which exceptions are
       "errors" vs expected control-flow (e.g. ``ServiceNotFoundError``).

Thread safety:  ⚠️ Conditional — ``_instrument_cache`` is a module-level dict.
                    Multiple threads writing the same key simultaneously is
                    benign (idempotent — both threads create the same instrument
                    from the same provider).  The GIL makes concurrent dict
                    writes safe in CPython.  In free-threaded Python (3.13+)
                    use a lock; for now CPython's GIL is the guard.
Async safety:   ✅ Coroutine wrappers never share mutable state across tasks.
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from opentelemetry import metrics as otel_metrics

_F = TypeVar("_F", bound=Callable[..., Any])

# ── Lazy instrument cache ──────────────────────────────────────────────────────

# Instruments are created on first call and reused forever.
# Key: (meter_name, instrument_name) — both strings, so hashable.
# Value: opentelemetry.metrics.Counter | opentelemetry.metrics.Histogram
#
# Module-level dict is safe here: in CPython the GIL prevents torn writes.
# Two concurrent "first calls" write the same key with equivalent objects —
# last writer wins, both objects are valid instruments from the same provider.
_instrument_cache: dict[tuple[str, str], Any] = {}


# ── CounterConfig ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CounterConfig:
    """
    Immutable configuration for the ``@counter`` decorator.

    Args:
        name:
            OTel metric name (e.g. ``"orders.created"``).  Follow the OTel
            semantic conventions naming style: ``"<namespace>.<noun>.<verb>"``
            or ``"<namespace>.<noun>"``.  Must be unique within a meter.
        meter_name:
            OTel instrumentation library name passed to
            ``opentelemetry.metrics.get_meter()``.  Defaults to ``"varco"``
            which groups all framework metrics under one scope.
        description:
            Human-readable description shown in metric explorer UIs.
        unit:
            OTel unit string.  ``"1"`` (dimensionless count) is correct for
            a counter.  See the OTel spec for the full unit vocabulary.
        attributes:
            Static metric attributes (labels) added to every observation.
            Values must be strings.  Dynamic attributes (e.g. ``tenant_id``)
            should be set on the instrument at record time — not supported
            directly by this decorator; add them inside the function body via
            ``opentelemetry.metrics.get_meter(...).create_counter(...).add(...)``.

    Thread safety:  ✅ Frozen dataclass — immutable, safe to share.
    """

    name: str
    meter_name: str = "varco"
    description: str = ""
    unit: str = "1"
    attributes: dict[str, str] = field(default_factory=dict)


# ── HistogramConfig ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class HistogramConfig:
    """
    Immutable configuration for the ``@histogram`` decorator.

    Args:
        name:
            OTel metric name (e.g. ``"orders.list.duration"``).
        meter_name:
            OTel instrumentation library name.  Defaults to ``"varco"``.
        description:
            Human-readable description.
        unit:
            OTel unit string.  ``"s"`` (seconds) is the correct unit for
            duration histograms per OTel semantic conventions.
        attributes:
            Static metric attributes.  See ``CounterConfig.attributes``.

    Thread safety:  ✅ Frozen dataclass — immutable, safe to share.
    """

    name: str
    meter_name: str = "varco"
    description: str = ""
    # Seconds — OTel semantic conventions recommend "s" for duration metrics.
    unit: str = "s"
    attributes: dict[str, str] = field(default_factory=dict)


# ── @counter decorator ────────────────────────────────────────────────────────


def counter(cfg: CounterConfig) -> Callable[[_F], _F]:
    """
    Increment an OTel ``Counter`` by 1 after each successful call.

    The counter is NOT incremented when the decorated function raises an
    exception — the semantics of ``orders.created`` should be "orders actually
    created", not "create_order was called".

    Args:
        cfg: ``CounterConfig`` controlling the metric name, meter name, and
             static attributes.

    Returns:
        A decorator that wraps the target function with counter instrumentation.
        The wrapper preserves the original function's signature via
        ``functools.wraps``.

    Raises:
        Any exception from the wrapped function — this decorator never
        swallows exceptions.

    Edge cases:
        - No active ``MeterProvider`` → OTel returns a no-op meter; the
          function still runs normally, metrics are just discarded.
        - First call creates the ``Counter`` instrument from the live provider
          and caches it for all subsequent calls.

    Thread safety:  ✅ Stateless after first-call instrument creation.
    Async safety:   ✅ Safe — each invocation is independent.

    Example::

        @counter(CounterConfig(name="orders.created", attributes={"service": "orders"}))
        async def create_order(dto: CreateOrderDTO) -> OrderReadDTO:
            ...
    """

    def decorator(func: _F) -> _F:
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                result = await func(*args, **kwargs)
                # Increment AFTER successful return — failures don't count.
                _get_counter(cfg).add(1, attributes=cfg.attributes or None)
                return result

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            _get_counter(cfg).add(1, attributes=cfg.attributes or None)
            return result

        return sync_wrapper  # type: ignore[return-value]

    return decorator


# ── @histogram decorator ──────────────────────────────────────────────────────


def histogram(cfg: HistogramConfig) -> Callable[[_F], _F]:
    """
    Record the wall-clock duration of each call as an OTel ``Histogram``.

    Duration is measured with ``time.perf_counter()`` (monotonic,
    sub-microsecond resolution) and recorded in **seconds** regardless of the
    configured ``unit`` — set ``unit="ms"`` only if your backend requires
    milliseconds (most modern backends prefer seconds).

    Unlike ``@counter``, the histogram records on **both success and failure**
    — latency outliers from failing calls are still valuable for diagnosis.

    Args:
        cfg: ``HistogramConfig`` controlling the metric name, unit, and
             static attributes.

    Returns:
        A decorator that wraps the target function with histogram instrumentation.

    Raises:
        Any exception from the wrapped function — re-raised after recording
        the duration.

    Edge cases:
        - Exceptions: duration is still recorded so latency outliers from
          errors appear in the distribution.
        - Nested ``@histogram`` decorators: each creates its own instrument
          with its own name — no double-counting.
        - No active ``MeterProvider``: no-op meter — function runs normally.

    Thread safety:  ✅ Stateless per-call timing via local variable.
    Async safety:   ✅ ``time.perf_counter()`` is safe inside coroutines;
                       ``await`` points do not affect monotonic clock reads.

    Example::

        @histogram(HistogramConfig(name="orders.list.duration", unit="s"))
        async def list_orders(params: QueryParams) -> list[OrderReadDTO]:
            ...
    """

    def decorator(func: _F) -> _F:
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.perf_counter()
                try:
                    return await func(*args, **kwargs)
                finally:
                    # finally runs on both success and exception — we always
                    # want to record the latency regardless of outcome.
                    elapsed = time.perf_counter() - start
                    _get_histogram(cfg).record(
                        elapsed, attributes=cfg.attributes or None
                    )

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start
                _get_histogram(cfg).record(elapsed, attributes=cfg.attributes or None)

        return sync_wrapper  # type: ignore[return-value]

    return decorator


# ── Lazy instrument helpers ───────────────────────────────────────────────────


def _get_counter(cfg: CounterConfig) -> Any:
    """
    Return (and lazily create) the ``Counter`` instrument for ``cfg``.

    The instrument is cached in ``_instrument_cache`` by
    ``(meter_name, metric_name)`` so that repeated calls share the same
    instrument object.  Creating the same instrument twice from the same
    provider is harmless but wasteful.

    Args:
        cfg: The ``CounterConfig`` identifying the instrument.

    Returns:
        An ``opentelemetry.metrics.Counter`` instance.
    """
    key = (cfg.meter_name, cfg.name)
    instrument = _instrument_cache.get(key)
    if instrument is None:
        meter = otel_metrics.get_meter(cfg.meter_name)
        instrument = meter.create_counter(
            name=cfg.name,
            description=cfg.description,
            unit=cfg.unit,
        )
        # Last-writer-wins under concurrent first calls — both objects are
        # valid; the cache avoids repeated meter.create_counter() calls.
        _instrument_cache[key] = instrument
    return instrument


def _get_histogram(cfg: HistogramConfig) -> Any:
    """
    Return (and lazily create) the ``Histogram`` instrument for ``cfg``.

    Same lazy-cache pattern as ``_get_counter``.

    Args:
        cfg: The ``HistogramConfig`` identifying the instrument.

    Returns:
        An ``opentelemetry.metrics.Histogram`` instance.
    """
    key = (cfg.meter_name, cfg.name)
    instrument = _instrument_cache.get(key)
    if instrument is None:
        meter = otel_metrics.get_meter(cfg.meter_name)
        instrument = meter.create_histogram(
            name=cfg.name,
            description=cfg.description,
            unit=cfg.unit,
        )
        _instrument_cache[key] = instrument
    return instrument


__all__ = [
    "CounterConfig",
    "HistogramConfig",
    "counter",
    "histogram",
]
