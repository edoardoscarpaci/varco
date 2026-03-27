"""
varco_core.observability.span
==============================
``@span`` — OpenTelemetry tracing decorator for sync and async callables.

Wraps any function or method in an OTel span, automatically naming it from
the function's ``__qualname__`` unless overridden via ``SpanConfig``.  The
active correlation ID (from ``varco_core.tracing``) is written as a span
attribute so that traces and structured logs can be joined by correlation ID
in any observability backend.

Usage — bare decorator (span name = function qualname)::

    from varco_core.observability import span

    @span
    async def place_order(order_id: UUID) -> Order:
        ...

Usage — configured::

    from varco_core.observability import span, SpanConfig

    @span(SpanConfig(name="order.place", attributes={"db": "postgresql"}))
    async def place_order(order_id: UUID) -> Order:
        ...

Composition with resilience decorators::

    @span(SpanConfig(name="payment.charge"))   # outermost — spans the full call
    @retry(RetryPolicy(max_attempts=3))
    @circuit_breaker(CircuitBreakerConfig(failure_threshold=5))
    async def charge_card(amount: float) -> TransactionId:
        ...

    # Execution order (outermost first):
    #   span → retry (all attempts) → breaker → actual call
    #
    # Keep @span outermost so the span duration includes retries and backoff.

DESIGN: decorator supports both bare (@span) and configured (@span(config)) forms
    ✅ Single name — no @span vs @span_with_config split.
    ✅ Consistent with Python stdlib patterns (e.g. functools.wraps).
    ✅ Type-checker sees the right return type in both call forms via overloads.
    ❌ Implementation must detect whether the first argument is a callable
       (bare form) or a SpanConfig (configured form) — slightly tricky.

DESIGN: correlation ID → span attribute bridge
    ✅ Joins traces with structured log lines that carry ``correlation_id``.
    ✅ Zero coupling — reads from ContextVar; no explicit parameter threading.
    ❌ If correlation context is not set the attribute is omitted (not an error).
       This is intentional — not every code path runs inside a request scope.

Thread safety:  ✅ SpanConfig is a frozen dataclass — safe to share across threads.
Async safety:   ✅ Async wrapper uses ``async with`` on the span context manager.
                   Each asyncio task inherits its own OTel context via ContextVar.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar, overload

from opentelemetry import trace
from opentelemetry.trace import StatusCode

from varco_core.tracing import current_correlation_id

_logger = logging.getLogger(__name__)

_F = TypeVar("_F", bound=Callable[..., Any])
_AsyncF = TypeVar("_AsyncF", bound=Callable[..., Awaitable[Any]])


# ── SpanConfig ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SpanConfig:
    """
    Immutable configuration for the ``@span`` decorator.

    Args:
        name:
            Span name shown in the trace UI.  If ``None``, defaults to the
            decorated function's ``__qualname__``
            (e.g. ``"OrderService.create"``).
        tracer_name:
            OTel instrumentation library name passed to
            ``opentelemetry.trace.get_tracer()``.  Use the default (``"varco"``)
            unless you need to distinguish spans by library origin.
        attributes:
            Static key-value pairs attached to the span at creation time.
            Values must be strings (OTel attribute type constraint).
            Dynamic attributes (e.g. ``order_id``) should be set inside the
            function body via ``opentelemetry.trace.get_current_span().set_attribute()``.
        record_exception:
            When ``True`` (default), any exception that propagates out of the
            decorated function is recorded on the span via
            ``span.record_exception(exc)``.  The exception is always
            re-raised — this decorator never swallows exceptions.
        set_status_on_error:
            When ``True`` (default), sets the span status to ``ERROR`` if an
            exception propagates.  Only meaningful when ``record_exception``
            is also ``True``.

    Edge cases:
        - ``name=None`` with a lambda → ``__qualname__`` is ``"<lambda>"`` which
          is not very useful; pass an explicit name for lambdas.
        - Static ``attributes`` keys must not clash with OTel semantic
          conventions reserved keys (e.g. ``"http.method"``) unless you
          intend to set them.

    Thread safety:  ✅ Frozen dataclass — immutable, safe to share.
    """

    # None means "use the function's __qualname__" — avoids repeating the name
    # when the function name is already descriptive.
    name: str | None = None

    # "varco" groups all framework-level spans under one instrumentation scope
    # in backends like Tempo / Jaeger.  Override for per-service scope names.
    tracer_name: str = "varco"

    # Static attributes added at span creation time.  Dynamic values (request
    # parameters, entity IDs, etc.) should be set inside the function body.
    attributes: dict[str, str] = field(default_factory=dict)

    # Always record exceptions — silent failures are hard to debug in prod.
    record_exception: bool = True
    set_status_on_error: bool = True


# ── @span decorator ───────────────────────────────────────────────────────────


# Overloads so the type checker knows the return type in both call forms:
#   @span          → F (same type as the decorated function)
#   @span(config)  → Callable[[F], F]
@overload
def span(func: _F) -> _F: ...


@overload
def span(config: SpanConfig) -> Callable[[_F], _F]: ...


def span(
    func: _F | SpanConfig | None = None,
    config: SpanConfig | None = None,
) -> _F | Callable[[_F], _F]:
    """
    Wrap a sync or async callable in an OpenTelemetry tracing span.

    Supports two call forms::

        @span                              # bare — name = function.__qualname__
        async def my_fn(): ...

        @span(SpanConfig(name="my.span")) # configured
        async def my_fn(): ...

    The active correlation ID (``varco_core.tracing.current_correlation_id()``)
    is automatically set as the ``correlation_id`` span attribute when present.
    This bridges structured log lines (which carry the correlation ID) with
    distributed traces so operators can pivot between the two in their
    observability backend.

    Args:
        func:   The function to decorate (bare form) or a ``SpanConfig``
                (configured form).  Do not pass this explicitly — it is
                filled in by Python's decorator machinery.
        config: Unused in the public API; reserved for future positional
                config support.

    Returns:
        The decorated function (bare form) or a decorator factory
        (configured form).  The wrapper preserves the original function's
        ``__name__``, ``__qualname__``, ``__doc__``, and ``__annotations__``
        via ``functools.wraps``.

    Raises:
        Any exception raised by the wrapped function — this decorator never
        swallows exceptions.  When ``SpanConfig.record_exception`` is True
        the exception is recorded on the span before re-raising.

    Edge cases:
        - No active ``TracerProvider`` (e.g. in unit tests that skip DI) →
          OTel returns a no-op tracer; the function still runs normally.
        - Nested ``@span`` decorators → each creates its own child span,
          forming a parent-child hierarchy automatically via OTel context
          propagation.
        - ``current_correlation_id()`` returns ``None`` → the attribute is
          simply not set on the span (no error).

    Thread safety:  ✅ Wrapper is stateless; ``SpanConfig`` is frozen.
    Async safety:   ✅ Async wrapper uses ``async with`` — span context is
                       propagated correctly across ``await`` points.
    """
    # ── Detect call form ──────────────────────────────────────────────────────

    if callable(func) and not isinstance(func, SpanConfig):
        # Bare form: @span — func is the actual function being decorated.
        return _make_wrapper(func, SpanConfig())

    # Configured form: @span(SpanConfig(...)) — func IS the SpanConfig.
    effective_config: SpanConfig = (
        func if isinstance(func, SpanConfig) else SpanConfig()
    )

    def decorator(fn: _F) -> _F:
        return _make_wrapper(fn, effective_config)

    return decorator  # type: ignore[return-value]


# ── Internal wrapper builder ───────────────────────────────────────────────────


def _make_wrapper(func: _F, cfg: SpanConfig) -> _F:
    """
    Build the actual wrapper function (sync or async) for ``func``.

    Separated from ``span()`` so the logic is not duplicated across the two
    call forms of the decorator.

    Args:
        func: The original callable to wrap.
        cfg:  The resolved ``SpanConfig`` to apply.

    Returns:
        A wrapped callable that creates an OTel span around each call.
    """
    # Span name: explicit > function qualname
    span_name = cfg.name or func.__qualname__

    if asyncio.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            tracer = trace.get_tracer(cfg.tracer_name)
            # Use record_exception=False so the SDK does NOT auto-record on
            # exception — we handle this ourselves based on SpanConfig flags.
            with tracer.start_as_current_span(
                span_name,
                record_exception=False,
            ) as current:
                # Set static attributes declared in SpanConfig.
                for k, v in cfg.attributes.items():
                    current.set_attribute(k, v)

                # Bridge: stamp the active correlation ID onto the span so
                # traces can be joined with structured log lines in the backend.
                cid = current_correlation_id()
                if cid is not None:
                    current.set_attribute("correlation_id", cid)

                try:
                    return await func(*args, **kwargs)
                except Exception as exc:
                    if cfg.record_exception:
                        current.record_exception(exc)
                    if cfg.set_status_on_error:
                        current.set_status(StatusCode.ERROR, str(exc))
                    raise

        return async_wrapper  # type: ignore[return-value]

    # Sync wrapper — identical logic, but no await.
    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        tracer = trace.get_tracer(cfg.tracer_name)
        with tracer.start_as_current_span(
            span_name,
            record_exception=False,
        ) as current:
            for k, v in cfg.attributes.items():
                current.set_attribute(k, v)

            cid = current_correlation_id()
            if cid is not None:
                current.set_attribute("correlation_id", cid)

            try:
                return func(*args, **kwargs)
            except Exception as exc:
                if cfg.record_exception:
                    current.record_exception(exc)
                if cfg.set_status_on_error:
                    current.set_status(StatusCode.ERROR, str(exc))
                raise

    return sync_wrapper  # type: ignore[return-value]


__all__ = ["SpanConfig", "span"]
