"""
varco_core.event.middleware
============================
Built-in ``EventMiddleware`` implementations.

All four classes implement the ``EventMiddleware`` ABC from ``event.base``
using the ASGI-style ``(event, channel, next)`` signature.

``LoggingMiddleware``
    Logs the event type, channel, and elapsed dispatch time at a configurable
    log level.  Logs a WARNING if a downstream handler raises.

``CorrelationMiddleware``
    Reads or generates a ``correlation_id`` string and stores it in a
    ``ContextVar`` for the duration of the dispatch chain.  Handlers can
    retrieve the current correlation ID via ``CorrelationMiddleware.current_id()``.

``RetryMiddleware``
    Wraps the ``next`` call in a retry loop with exponential back-off.
    Uses ``RetryPolicy`` from ``varco_core.resilience`` for configuration.

``TracingEventMiddleware``
    Wraps each event dispatch in an OTel span.  Span name is
    ``"event.{EventTypeName}"``; channel and correlation_id are set as
    span attributes.

Wiring example::

    from varco_core.event import InMemoryEventBus
    from varco_core.event.middleware import (
        CorrelationMiddleware,
        LoggingMiddleware,
        RetryMiddleware,
        TracingEventMiddleware,
    )
    from varco_core.resilience import RetryPolicy

    bus = InMemoryEventBus(middleware=[
        CorrelationMiddleware(),
        TracingEventMiddleware(),
        LoggingMiddleware(),
        RetryMiddleware(RetryPolicy(max_attempts=3, base_delay=0.1)),
    ])

    # Middleware executes outermost-first: CorrelationMiddleware → LoggingMiddleware
    # → RetryMiddleware → handler dispatch.

DESIGN: module-level ContextVar for CorrelationMiddleware
    ✅ Module-level placement makes the ContextVar unique and importable — no
       instance attribute needed.
    ✅ Each asyncio Task inherits the context at creation time; ``set()``/
       ``reset()`` are scoped to the current task.
    ✅ ``ContextVar`` is the correct async-safe mechanism — unlike threading.local,
       it is isolated per asyncio Task, so concurrent event dispatches never
       clobber each other's correlation IDs.
    ❌ Module-level variable requires accessing it via
       ``CorrelationMiddleware.current_id()`` (a static method) — a minor
       indirection but cleaner than exposing the raw ``ContextVar``.

DESIGN: RetryMiddleware inline loop over ``@retry`` decorator
    ✅ ``next`` is a one-shot coroutine instance — it cannot be called twice.
       The retry loop must re-call ``next(event, channel)`` on each attempt,
       which only works with the inline loop pattern.
    ✅ No wrapping overhead on the happy path — the coroutine is awaited once.
    ❌ Slightly more code than the decorator form — justified by correctness.

Thread safety:  ✅  ``ContextVar`` is thread-safe (and async-task-safe).
Async safety:   ✅  All ``__call__`` methods are ``async def``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Final
from uuid import uuid4

from varco_core.event.base import Event, EventMiddleware

if TYPE_CHECKING:
    pass

_logger = logging.getLogger(__name__)

# Module-level ContextVar — one per process; isolated per asyncio Task.
# Accessed exclusively via ``CorrelationMiddleware.current_id()`` so callers
# never need to import the raw ContextVar.
_CORRELATION_ID: Final[ContextVar[str | None]] = ContextVar(
    "varco_correlation_id", default=None
)


# ── LoggingMiddleware ──────────────────────────────────────────────────────────


class LoggingMiddleware(EventMiddleware):
    """
    Logs event dispatch — type, channel, elapsed time, and outcome.

    On success, emits one log line at ``level`` (default ``INFO``) with the
    event type name, channel, and elapsed milliseconds.
    On failure (a downstream handler raises), emits one ``WARNING`` line
    with the same fields, then re-raises the exception so callers see it.

    Args:
        logger: Python ``Logger`` to write to.  Defaults to a logger named
                ``"varco_core.event.middleware"`` — the same as this module.
        level:  Log level for successful dispatch lines.
                Defaults to ``logging.INFO``.  Failures always log at WARNING.

    DESIGN: single log line per dispatch over pre/post lines
        ✅ One line per event — cleaner log output in high-throughput systems.
        ✅ Elapsed time appears on the same line as the outcome.
        ❌ If ``next()`` hangs (deadlock), no "started" line appears until it
           returns.  Accept this trade-off — a separate pre-dispatch DEBUG line
           would double the log volume.

    Thread safety:  ✅ Stateless — safe to share across buses/tasks.
    Async safety:   ✅ ``__call__`` is ``async def``.

    Example::

        bus = InMemoryEventBus(middleware=[LoggingMiddleware()])
        # INFO  varco_core.event.middleware — OrderPlacedEvent → orders (1.3ms)
    """

    def __init__(
        self,
        logger: logging.Logger | None = None,
        level: int = logging.INFO,
    ) -> None:
        """
        Args:
            logger: Logger instance.  Defaults to the module logger.
            level:  Log level for successful lines.  Failures are always WARNING.
        """
        self._logger = logger if logger is not None else _logger
        self._level = level

    async def __call__(
        self,
        event: Event,
        channel: str,
        next: Callable[[Event, str], Awaitable[None]],
    ) -> None:
        """
        Log event type, channel, and elapsed time around ``next()``.

        Args:
            event:   The event being dispatched.
            channel: The target channel.
            next:    Continuation — awaited to dispatch to handlers.

        Raises:
            Any exception raised by ``next()`` — LoggingMiddleware never
            swallows errors, it only logs them before re-raising.
        """
        event_name = type(event).__name__
        t0 = time.perf_counter()
        try:
            await next(event, channel)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            self._logger.log(
                self._level,
                "%s → %s (%.1fms)",
                event_name,
                channel,
                elapsed_ms,
            )
        except Exception:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            self._logger.warning(
                "%s → %s FAILED (%.1fms)",
                event_name,
                channel,
                elapsed_ms,
            )
            raise

    def __repr__(self) -> str:
        return f"LoggingMiddleware(level={logging.getLevelName(self._level)!r})"


# ── CorrelationMiddleware ──────────────────────────────────────────────────────


class CorrelationMiddleware(EventMiddleware):
    """
    Propagates a ``correlation_id`` through the event dispatch chain.

    On each dispatch:

    1. Checks whether the event has a ``correlation_id`` attribute (e.g. a
       field declared on the event subclass).
    2. Falls back to the current ``ContextVar`` value if set by the caller.
    3. Generates a fresh UUID4 string if neither source provides a value.
    4. Stores the resolved ID in ``_CORRELATION_ID`` for the duration of the
       ``next()`` call.  Handlers and nested middleware can read it via
       ``CorrelationMiddleware.current_id()``.
    5. Restores the previous ``ContextVar`` value after ``next()`` returns
       (including on exception), so nested event dispatches are properly scoped.

    The event itself is NOT mutated — ``Event`` is a frozen Pydantic model.
    If correlation ID propagation into the event payload is needed, declare a
    ``correlation_id: str`` field on the event subclass before publishing.

    Thread safety:  ✅ ``ContextVar`` is isolated per asyncio Task.
    Async safety:   ✅ ``set()``/``reset()`` are atomic at the task level.

    Example::

        # From a handler or downstream middleware:
        cid = CorrelationMiddleware.current_id()  # "abc-123" or None
    """

    @staticmethod
    def current_id() -> str | None:
        """
        Return the correlation ID active in the current asyncio task.

        Returns:
            The correlation ID string, or ``None`` if no middleware is active
            or no ID was propagated to this task.
        """
        return _CORRELATION_ID.get()

    async def __call__(
        self,
        event: Event,
        channel: str,
        next: Callable[[Event, str], Awaitable[None]],
    ) -> None:
        """
        Resolve and inject the correlation ID, then pass to ``next()``.

        Resolution order:
        1. ``event.correlation_id`` — if the event has this attribute.
        2. ``_CORRELATION_ID.get()`` — already set by an outer context.
        3. ``str(uuid4())`` — generated fresh as a last resort.

        Args:
            event:   The event being dispatched.
            channel: The target channel.
            next:    Continuation — awaited with the (possibly enriched) event.

        Edge cases:
            - If ``event.correlation_id`` is an empty string, it is treated as
              falsy and a new UUID is generated.
            - The ContextVar is always restored after ``next()`` returns,
              even if an exception is raised — no state leaks between dispatches.
        """
        # Resolve: event attribute > current context > fresh UUID
        correlation_id: str = (
            getattr(event, "correlation_id", None)
            or _CORRELATION_ID.get()
            or str(uuid4())
        )
        token = _CORRELATION_ID.set(correlation_id)
        try:
            await next(event, channel)
        finally:
            _CORRELATION_ID.reset(token)

    def __repr__(self) -> str:
        return "CorrelationMiddleware()"


# ── RetryMiddleware ────────────────────────────────────────────────────────────


class RetryMiddleware(EventMiddleware):
    """
    Retries failed event dispatches with exponential back-off.

    On each attempt, calls ``next(event, channel)``.  If it raises, waits
    ``base_delay * exponential_base ** (attempt - 1)`` seconds (capped at
    ``max_delay``) and tries again.  After ``max_attempts`` failures the last
    exception is re-raised.

    Args:
        max_attempts:     Total attempts including the first.  Must be ≥ 1.
                          ``1`` means no retries.  Defaults to ``3``.
        base_delay:       Seconds to wait before the second attempt.
                          Defaults to ``1.0``.
        max_delay:        Upper bound on any single delay.  Defaults to ``60.0``.
        exponential_base: Multiplier applied per retry.  Defaults to ``2.0``
                          (doubles the delay on each retry).

    DESIGN: inline retry loop over ``@retry`` decorator
        ``next`` is a one-shot coroutine instance — calling it a second time
        would raise ``RuntimeError: coroutine already executed``.  The retry
        loop must therefore call ``next(event, channel)`` fresh on each attempt,
        which only works with an inline loop.  The ``@retry`` decorator wraps
        a callable once and calls it repeatedly — that pattern does not apply
        to coroutine instances.

    Thread safety:  ✅ Stateless — policy fields are set at construction.
    Async safety:   ✅ ``asyncio.sleep`` yields the event loop between retries.

    Edge cases:
        - ``max_attempts=1`` → no retries; first failure propagates immediately.
        - If ``next`` raises ``asyncio.CancelledError``, it propagates without
          retry — cancellation must not be swallowed.
        - The last exception (after all attempts) is re-raised with its original
          traceback intact.

    Example::

        bus = InMemoryEventBus(middleware=[
            RetryMiddleware(max_attempts=3, base_delay=0.5),
        ])
        # First attempt + up to 2 retries; delays: 0.5s, 1.0s
    """

    def __init__(
        self,
        *,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
    ) -> None:
        """
        Args:
            max_attempts:     Total call attempts including the first.
            base_delay:       Seconds before the second attempt.
            max_delay:        Cap on any single delay.
            exponential_base: Per-retry delay multiplier.

        Raises:
            ValueError: ``max_attempts`` < 1.
        """
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be ≥ 1, got {max_attempts}.")
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._exponential_base = exponential_base

    async def __call__(
        self,
        event: Event,
        channel: str,
        next: Callable[[Event, str], Awaitable[None]],
    ) -> None:
        """
        Attempt ``next(event, channel)`` up to ``max_attempts`` times.

        Args:
            event:   The event being dispatched.
            channel: The target channel.
            next:    Continuation — called fresh on each attempt.

        Raises:
            The last exception raised by ``next()`` after all attempts exhaust.
            ``asyncio.CancelledError`` propagates immediately (no retry).
        """
        last_exc: BaseException | None = None
        for attempt in range(self._max_attempts):
            try:
                await next(event, channel)
                return  # success — done
            except asyncio.CancelledError:
                raise  # never retry cancellation
            except Exception as exc:
                last_exc = exc
                if attempt + 1 < self._max_attempts:
                    # Exponential backoff: base_delay * base^attempt, capped at max_delay
                    delay = min(
                        self._base_delay * (self._exponential_base**attempt),
                        self._max_delay,
                    )
                    _logger.debug(
                        "RetryMiddleware: %s → %s failed (attempt %d/%d), "
                        "retrying in %.2fs: %s",
                        type(event).__name__,
                        channel,
                        attempt + 1,
                        self._max_attempts,
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)

        _logger.warning(
            "RetryMiddleware: %s → %s exhausted %d attempts.",
            type(event).__name__,
            channel,
            self._max_attempts,
        )
        raise last_exc  # type: ignore[misc]

    def __repr__(self) -> str:
        return (
            f"RetryMiddleware("
            f"max_attempts={self._max_attempts}, "
            f"base_delay={self._base_delay}, "
            f"max_delay={self._max_delay})"
        )


# ── TracingEventMiddleware ─────────────────────────────────────────────────────


class TracingEventMiddleware(EventMiddleware):
    """
    Wraps each event dispatch in an OpenTelemetry span.

    The span name is ``"event.{EventTypeName}"`` by default — e.g.
    ``"event.OrderPlacedEvent"``.  The channel and correlation_id are added
    as span attributes so traces can be filtered by topic and correlated
    across services.

    Args:
        tracer_name:      OTel tracer name used for ``get_tracer()``.
                          Defaults to ``"varco"``.
        record_exception: Whether to call ``span.record_exception(exc)`` on
                          failure.  Defaults to ``True``.
        set_status_on_error: Whether to set ``StatusCode.ERROR`` on failure.
                             Defaults to ``True``.
        attributes:       Static span attributes applied to every span
                          (e.g. ``{"messaging.system": "internal"}``).

    DESIGN: span name as ``"event.{EventTypeName}"``
        ✅ Consistent naming convention — easy to find in trace dashboards.
        ✅ No per-class configuration needed; the event class name is available
           at dispatch time.
        ✅ Prefix ``"event."`` distinguishes event spans from service/repo spans.
        ❌ Renaming an event class silently changes span names in dashboards —
           pin a static ``span_name`` kwarg if stability is required (not added
           here to keep the API simple).

    DESIGN: places correctly in the middleware chain
        ``TracingEventMiddleware`` should be placed after ``CorrelationMiddleware``
        in the middleware list so the correlation ID is already set in the
        ContextVar when the span is opened.  This lets the middleware read
        the ID from ``CorrelationMiddleware.current_id()`` and stamp it on
        the span.

    Thread safety:  ✅ Stateless — safe to share across buses and tasks.
    Async safety:   ✅ ``__call__`` is ``async def``.

    Example::

        bus = InMemoryEventBus(middleware=[
            CorrelationMiddleware(),   # sets correlation ID first
            TracingEventMiddleware(),  # reads it for the span
        ])
    """

    def __init__(
        self,
        *,
        tracer_name: str = "varco",
        record_exception: bool = True,
        set_status_on_error: bool = True,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        """
        Args:
            tracer_name:         OTel tracer name.
            record_exception:    Call ``span.record_exception(exc)`` on error.
            set_status_on_error: Set ``StatusCode.ERROR`` on error.
            attributes:          Static attributes added to every span.
        """
        self._tracer_name = tracer_name
        self._record_exception = record_exception
        self._set_status_on_error = set_status_on_error
        self._attributes: dict[str, Any] = attributes or {}

    async def __call__(
        self,
        event: Event,
        channel: str,
        next: Callable[[Event, str], Awaitable[None]],
    ) -> None:
        """
        Open a span, call ``next(event, channel)``, then close the span.

        Span attributes:
            ``messaging.channel``   — The target channel string.
            ``event.type``          — The event class name.
            ``correlation_id``      — From ``CorrelationMiddleware.current_id()``
                                      or ``event.correlation_id`` if present.
                                      Omitted when neither source has a value.

        Args:
            event:   The event being dispatched.
            channel: The target channel.
            next:    Continuation to the next middleware or handler(s).

        Raises:
            Any exception raised by ``next()`` — always re-raised after the
            span is closed (exception recorded when ``record_exception=True``).
        """
        try:
            from opentelemetry import trace  # noqa: PLC0415
            from opentelemetry.trace import StatusCode  # noqa: PLC0415
        except ImportError:
            # opentelemetry not installed — fall back to no-op dispatch
            await next(event, channel)
            return

        span_name = f"event.{type(event).__name__}"
        tracer = trace.get_tracer(self._tracer_name)

        with tracer.start_as_current_span(
            span_name, record_exception=False, set_status_on_exception=False
        ) as current_span:
            # Static attributes (e.g. {"messaging.system": "internal"})
            for k, v in self._attributes.items():
                current_span.set_attribute(k, v)

            # Dynamic attributes — channel and event type
            current_span.set_attribute("messaging.channel", channel)
            current_span.set_attribute("event.type", type(event).__name__)

            # Correlation ID: prefer CorrelationMiddleware's ContextVar, fall
            # back to the event attribute if present (e.g. domain events stamp it).
            cid: str | None = CorrelationMiddleware.current_id() or getattr(
                event, "correlation_id", None
            )
            if cid:
                current_span.set_attribute("correlation_id", cid)

            try:
                await next(event, channel)
            except Exception as exc:
                if self._record_exception:
                    current_span.record_exception(exc)
                if self._set_status_on_error:
                    current_span.set_status(StatusCode.ERROR, str(exc))
                raise

    def __repr__(self) -> str:
        return (
            f"TracingEventMiddleware("
            f"tracer_name={self._tracer_name!r}, "
            f"record_exception={self._record_exception}, "
            f"set_status_on_error={self._set_status_on_error})"
        )


__all__ = [
    "LoggingMiddleware",
    "CorrelationMiddleware",
    "RetryMiddleware",
    "TracingEventMiddleware",
]
