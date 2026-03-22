"""
varco_core.tracing
======================
Correlation ID utilities for request tracing and structured logging.

A correlation ID is a short opaque string (UUID4) generated at the HTTP
boundary and threaded through every log line produced by a single request.
It lets operators grep a single request's activity across multiple services.

Public API
----------
``generate_correlation_id()``
    Generate a fresh UUID4-based correlation ID string.

``correlation_context(correlation_id)``
    Async-safe context manager that sets the correlation ID for the duration
    of the ``async with`` block, then restores the previous value.

``current_correlation_id()``
    Read the correlation ID for the current async task (or ``None`` if unset).

``CorrelationIdFilter``
    ``logging.Filter`` subclass that injects ``correlation_id`` into every
    ``LogRecord`` so structured formatters can include it without modifying
    every log call site.

Usage with FastAPI::

    import logging
    import uvicorn
    from fastapi import FastAPI, Request
    from varco_core.tracing import (
        CorrelationIdFilter,
        correlation_context,
        generate_correlation_id,
    )

    app = FastAPI()

    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        cid = request.headers.get("X-Correlation-ID") or generate_correlation_id()
        async with correlation_context(cid):
            response = await call_next(request)
            response.headers["X-Correlation-ID"] = cid
            return response

    # Wire the filter once at startup
    logging.getLogger().addFilter(CorrelationIdFilter())

DESIGN: ContextVar over threading.local
    ✅ Each asyncio.Task inherits the parent's context at creation time but
       mutations don't bleed back — safe for concurrent request handling.
    ✅ No explicit lock needed — ContextVar.set() is atomic at the task level.
    ✅ Works with ``asyncio.TaskGroup`` and ``asyncio.gather()`` — child tasks
       see the correlation ID of the spawning task automatically.
    ❌ Not accessible from threads spawned with ``loop.run_in_executor()``
       unless the executor receives the context explicitly (use
       ``contextvars.copy_context().run(fn)`` in the executor callback).

DESIGN: context manager over set/reset manual calls
    ✅ Always restores the previous value — even if the body raises.
    ✅ Nesting-safe: ``correlation_context`` stacks cleanly because each
       ``ContextVar.set()`` returns a token that resets to the exact previous
       value, not just ``None``.
    ❌ Slightly more ceremony than a bare ``set()`` call; justified because
       bare ``set()`` without reset leaks state into sibling tasks.

Thread safety:  ✅ ``ContextVar`` is thread-safe — each OS thread has its
                own context copy when entered via ``asyncio.run()``.
Async safety:   ✅ Each Task has its own context — mutations are isolated.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncIterator
from uuid import uuid4

# ── Internal context variable ─────────────────────────────────────────────────

# Stores the active correlation ID for the current async task.
# Default is None — unset until correlation_context() is entered.
_correlation_id: ContextVar[str | None] = ContextVar(
    "varco_correlation_id", default=None
)


# ── Public helpers ─────────────────────────────────────────────────────────────


def generate_correlation_id() -> str:
    """
    Generate a fresh, globally unique correlation ID.

    Uses UUID4 (random) so IDs are unpredictable and require no central
    coordinator.  The string form (``str(uuid4())``) is 36 characters —
    short enough for log lines, unique enough for request tracing.

    Returns:
        A new UUID4 string, e.g. ``"550e8400-e29b-41d4-a716-446655440000"``.

    Edge cases:
        - Collision probability is astronomically low (~1 in 5×10³⁶ per call).
        - No seed is exposed — callers cannot reproduce an ID from a seed.
    """
    # uuid4() reads from os.urandom() — no shared state, no lock needed
    return str(uuid4())


def current_correlation_id() -> str | None:
    """
    Return the correlation ID for the current async task, or ``None``.

    Reads the task-local ``ContextVar`` — safe to call from any coroutine.
    Returns ``None`` when called outside a ``correlation_context`` block.

    Returns:
        The active correlation ID string, or ``None`` if none is set.

    Async safety:   ✅ Pure read — no mutation, no I/O.
    """
    return _correlation_id.get()


@asynccontextmanager
async def correlation_context(correlation_id: str) -> AsyncIterator[str]:
    """
    Async context manager that activates a correlation ID for the block.

    Sets ``_correlation_id`` to ``correlation_id`` on entry and resets it to
    the previous value on exit — even if the body raises an exception.
    Nesting-safe: ``token.reset()`` restores the *exact* previous value,
    so nested ``correlation_context`` calls stack and unstack correctly.

    Args:
        correlation_id: The ID to activate for the duration of the block.

    Yields:
        ``correlation_id`` — so callers can write::

            async with correlation_context(generate_correlation_id()) as cid:
                logger.info("handling request", extra={"cid": cid})

    Raises:
        Any exception raised inside the ``async with`` body is propagated
        unchanged after the context is restored.

    Edge cases:
        - Empty string is accepted — no validation is performed on the value.
        - Nesting works correctly because ``ContextVar.reset(token)`` restores
          the value *before the matching set()*, not just ``None``.

    Async safety:   ✅ Each Task has its own context copy — sibling tasks are
                    unaffected by this set/reset pair.
    """
    # token records the state before set() — used to revert on exit
    token = _correlation_id.set(correlation_id)
    try:
        yield correlation_id
    finally:
        # Always restore previous value — idempotent even if body raised
        _correlation_id.reset(token)


# ── Logging integration ───────────────────────────────────────────────────────


class CorrelationIdFilter(logging.Filter):
    """
    ``logging.Filter`` that stamps every ``LogRecord`` with the active
    correlation ID so structured formatters can include it without modifying
    every individual log call.

    Usage::

        # Wire once at application startup — affects all loggers
        logging.getLogger().addFilter(CorrelationIdFilter())

        # Then use the injected attribute in a formatter:
        formatter = logging.Formatter(
            "%(asctime)s %(correlation_id)s %(levelname)s %(message)s"
        )

    DESIGN: Filter over explicit ``extra={}`` at every log call
        ✅ Zero-change adoption — existing log calls gain the ID automatically.
        ✅ Works with third-party libraries' log calls too.
        ❌ The ``correlation_id`` attribute is added dynamically — static
           analysis tools won't know it exists on ``LogRecord``.

    Attributes:
        fallback: Value used when no correlation ID is active.
                  Defaults to ``"-"`` (a conventional "no value" sentinel in
                  log formats, distinguishable from an actual ID).

    Thread safety:  ✅ ``filter()`` only reads the ContextVar — no writes.
    Async safety:   ✅ Each Task has its own correlation ID — filter is safe
                    to share across concurrent tasks.

    Edge cases:
        - Called from a thread without a running event loop → ``fallback``
          is used because the ContextVar has no value in that thread's context.
        - Multiple filters on the same logger → each runs independently;
          adding ``CorrelationIdFilter`` twice stamps the attribute twice
          (idempotent, second write is a no-op because the value is the same).
    """

    def __init__(self, fallback: str = "-") -> None:
        """
        Initialise with an optional fallback value.

        Args:
            fallback: String to use when no correlation ID is active.
                      Common choices: ``"-"``, ``"n/a"``, ``"unknown"``.
        """
        super().__init__()
        # Stored so subclasses can override the fallback without re-implementing filter()
        self.fallback = fallback

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Stamp ``record.correlation_id`` and pass the record through.

        Always returns ``True`` — this filter never suppresses records, it
        only enriches them.

        Args:
            record: The log record to enrich.

        Returns:
            ``True`` — record is always passed through.
        """
        # Read task-local value; fall back to sentinel when unset
        record.correlation_id = _correlation_id.get() or self.fallback  # type: ignore[attr-defined]
        return True


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "CorrelationIdFilter",
    "correlation_context",
    "current_correlation_id",
    "generate_correlation_id",
]
