"""
varco_fastapi.middleware.profiling
====================================
ASGI middleware that profiles individual HTTP requests using
``varco_core.profiling``.

Opt-in and disabled by default — enable with the ``VARCO_PROFILING_ENABLED=true``
environment variable or by setting ``ProfilingSettings(enabled=True)``.

Usage via ``create_varco_app``::

    # Set env var VARCO_PROFILING_ENABLED=true before starting the server, or:
    app = create_varco_app(container, enable_profiling=True)

Usage via app settings::

    VARCO_PROFILING_ENABLED=true
    VARCO_PROFILING_SLOW_THRESHOLD_MS=100
    VARCO_PROFILING_ATTACH_HEADERS=true

DESIGN: serialised profiling via process-wide asyncio.Lock
    ``cProfile`` and ``tracemalloc`` are process-global — two concurrent sessions
    contaminate each other's numbers.  The middleware uses a lazy ``asyncio.Lock``
    (one per middleware instance):
    - At most one request is profiled at a time.
    - Concurrent requests that find the lock taken **pass through unprofiled**
      (never wait) so profiling never adds latency.
    ✅ Correct measurements on the profiled request.
    ✅ No throughput impact — unprofiled requests take the fast path.
    ❌ Only ~1 request profiled per session; load tests are not a good trigger.

DESIGN: threshold gating
    Reports are only logged when ``wall_time_ms >= slow_threshold_ms``
    (or when ``slow_threshold_ms == 0``, meaning log all profiled requests).
    This avoids log noise from trivially fast responses.

DESIGN: innermost placement
    Should be added after ``TracingMiddleware``/``MetricsMiddleware`` so profiling
    attributes the cost to the route handler, not to other middleware layers.
    The app factory places it innermost (see ``create_varco_app``).
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from pydantic_settings import SettingsConfigDict
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from varco_core.config import VarcoSettings

if TYPE_CHECKING:
    from collections.abc import Callable

_logger = logging.getLogger(__name__)

# ── ProfilingSettings ─────────────────────────────────────────────────────────


class ProfilingSettings(VarcoSettings):
    """Configuration for ``ProfilingMiddleware``, loaded from environment variables.

    All fields use the ``VARCO_PROFILING_`` prefix.

    Example env vars::

        VARCO_PROFILING_ENABLED=true
        VARCO_PROFILING_SLOW_THRESHOLD_MS=100
        VARCO_PROFILING_MEM_THRESHOLD_MB=5
        VARCO_PROFILING_ATTACH_HEADERS=true
        VARCO_PROFILING_TOP_N=10

    Attributes:
        enabled:             Whether to install the middleware at all (default ``False``).
        skip_paths:          Request paths to never profile (default: ``/metrics``,
                             ``/health``, ``/readyz``).
        slow_threshold_ms:   Only log a report when wall time exceeds this value in
                             milliseconds.  ``0.0`` means log every profiled request
                             (default ``0.0``).
        mem_threshold_mb:    Only log when memory delta exceeds this value in MiB.
                             ``0.0`` means log regardless of memory delta (default ``0.0``).
        attach_headers:      Attach ``X-Profile-Wall-Ms`` and ``X-Profile-Mem-Kb``
                             response headers (default ``False``).
        top_n:               Number of top functions / allocation sites in the report
                             (default ``15``).
        track_rss:           Include process RSS delta in the report (default ``True``).

    Thread safety:  ✅ Frozen pydantic model.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_PROFILING_", frozen=True)

    enabled: bool = False
    skip_paths: frozenset[str] = frozenset({"/metrics", "/health", "/readyz"})
    slow_threshold_ms: float = 0.0
    mem_threshold_mb: float = 0.0
    attach_headers: bool = False
    top_n: int = 15
    track_rss: bool = True


# ── ProfilingMiddleware ───────────────────────────────────────────────────────


class ProfilingMiddleware(BaseHTTPMiddleware):
    """ASGI middleware that profiles one HTTP request at a time.

    Concurrency: uses a process-wide ``asyncio.Lock`` (created lazily) so only
    one request is profiled at a time.  Concurrent requests pass through
    unprofiled — they are **never** delayed.

    Args:
        app:      The ASGI application.
        settings: ``ProfilingSettings`` instance.  Defaults to reading from env.

    Thread safety:  ⚠️ The asyncio lock is per-instance; use one middleware instance.
    Async safety:   ✅ Lock acquired/released in the event loop thread.
    """

    def __init__(self, app: Any, settings: ProfilingSettings | None = None) -> None:
        super().__init__(app)
        self._settings = settings or ProfilingSettings()
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        """Return the process-wide profiling lock, creating it lazily.

        Locks must be created inside a running event loop — never at import or
        ``__init__`` time.

        Returns:
            The singleton ``asyncio.Lock`` for this middleware instance.
        """
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Any],
    ) -> Response:
        """Profile the request if eligible; otherwise pass through unchanged.

        Args:
            request:   The incoming HTTP request.
            call_next: The next ASGI handler in the chain.

        Returns:
            The response, with optional profiling headers attached.
        """
        s = self._settings

        # Skip configured paths
        if request.url.path in s.skip_paths:
            response: Response = await call_next(request)
            return response

        lock = self._get_lock()

        # Non-blocking: if another request is already being profiled, pass through
        if lock.locked():
            response = await call_next(request)
            return response

        async with lock:
            return await self._profiled_dispatch(request, call_next, s)

    async def _profiled_dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Any],
        s: ProfilingSettings,
    ) -> Response:
        """Run the request inside a ``ProfileSession`` and attach optional headers.

        Args:
            request:   The incoming HTTP request.
            call_next: The next ASGI handler.
            s:         The profiling settings.

        Returns:
            The response, possibly with ``X-Profile-*`` headers.
        """
        try:
            from varco_core.profiling import (
                ProfileConfig,
                ProfileSession,
            )  # noqa: PLC0415
        except ImportError:
            _logger.warning(
                "ProfilingMiddleware: varco_core.profiling not available; "
                "request passed through unprofiled."
            )
            # Distinct local name from `response` below — the two are
            # separate control-flow paths (import missing vs. profiled) and
            # giving them one shared annotated name would force a single
            # declared type across both, which is what a `no-redef` mypy
            # error is warning about.
            fallback_response: Response = await call_next(request)
            return fallback_response

        name = f"{request.method} {request.url.path}"
        config = ProfileConfig(top_n=s.top_n, track_rss=s.track_rss)

        response: Response | None = None
        session = ProfileSession(name, config)
        try:
            async with session:
                response = await call_next(request)
        except Exception:
            # Re-raise application errors — profiling must not swallow them
            raise

        report = session.report

        if report is None:
            return response

        # Threshold gating — only log when above thresholds
        slow_enough = report.wall_time_ms >= s.slow_threshold_ms
        mem_threshold_bytes = s.mem_threshold_mb * 1024 * 1024
        mem_enough = abs(report.mem_delta_bytes) >= mem_threshold_bytes
        should_log = (s.slow_threshold_ms == 0.0 or slow_enough) and (
            s.mem_threshold_mb == 0.0 or mem_enough
        )

        if should_log:
            _logger.info(report.format())

        # Optional response headers
        if s.attach_headers and response is not None:
            response.headers["X-Profile-Wall-Ms"] = f"{report.wall_time_ms:.2f}"
            if report.mem_delta_bytes is not None:
                mem_kb = report.mem_delta_bytes / 1024
                response.headers["X-Profile-Mem-Kb"] = f"{mem_kb:.2f}"

        return response
