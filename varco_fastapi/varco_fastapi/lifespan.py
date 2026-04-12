"""
varco_fastapi.lifespan
======================
FastAPI lifespan handler for startup/shutdown of varco services.

``VarcoLifespan`` collects all registered lifecycle objects and starts/stops
them in the correct order during the FastAPI application lifespan.

Managed components (each optional):
- ``AbstractEventBus``   — start publishing/consuming events
- ``OutboxRelay``        — start polling outbox table
- ``AbstractJobRunner``  — start background job execution
- Any ``AbstractLifecycle`` custom component (``start()`` / ``stop()``)

Usage::

    from fastapi import FastAPI
    from varco_fastapi.lifespan import VarcoLifespan

    lifespan = VarcoLifespan()
    lifespan.register(event_bus)
    lifespan.register(outbox_relay)
    lifespan.register(job_runner)

    app = FastAPI(lifespan=lifespan)

DESIGN: explicit registration over DI container scanning
    ✅ No magic — exactly what you register is started and stopped
    ✅ Order is preserved — components start in registration order,
       stop in reverse (LIFO — dependents stop before dependencies)
    ✅ Compatible with any component that has start()/stop() methods
    ✅ Works as a FastAPI ``lifespan`` context manager
    ❌ Must be configured manually at app startup — no auto-discovery

Recommended startup order
-------------------------
Register components in dependency order.  Each component starts after the
things it depends on, and stops before them (LIFO shutdown is automatic)::

    lifespan.register(bus)           # 1. Bus first — no other component can
                                     #    subscribe before it starts its loop.
    lifespan.register(consumer)      # 2. Consumer calls register_to(bus) in
                                     #    start() — bus must already be running.
    lifespan.register(ws_bus)        # 3. WebSocketEventBus.start() calls
    lifespan.register(sse_bus)       #    bus.subscribe() — needs bus running.
    lifespan.register(outbox_relay)  # 4. Polls DB + publishes to bus — needs both.
    lifespan.register(job_runner)    # 5. Independent — can start last.

    # Shutdown (LIFO): job_runner → outbox_relay → sse_bus → ws_bus → consumer → bus

Rule of thumb: register a component *after* everything it depends on.

Thread safety:  ⚠️ Registration must happen before app startup (not thread-safe).
Async safety:   ✅ ``__call__`` is an async context manager (FastAPI lifespan).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Protocol, runtime_checkable

_logger = logging.getLogger(__name__)


# ── Lifecycle protocol ─────────────────────────────────────────────────────────


@runtime_checkable
class AbstractLifecycle(Protocol):
    """
    Protocol for components with an async start/stop lifecycle.

    Any object with ``start()`` and ``stop()`` async methods satisfies this
    protocol via structural subtyping — no explicit inheritance needed.

    Thread safety:  ✅ Protocol check is read-only.
    Async safety:   ✅ Both methods are ``async def``.
    """

    async def start(self) -> None:
        """Start the component.  Idempotent."""
        ...

    async def stop(self) -> None:
        """Stop the component cleanly.  Idempotent."""
        ...


# ── VarcoLifespan ─────────────────────────────────────────────────────────────


class VarcoLifespan:
    """
    FastAPI ``lifespan`` context manager that starts/stops varco components.

    Components are started in registration order (first registered, first started)
    and stopped in reverse order (last registered, first stopped).  This ensures
    that consumers of a service stop before the service itself.

    Usage::

        lifespan = VarcoLifespan()
        lifespan.register(event_bus)       # started first
        lifespan.register(outbox_relay)    # started second
        lifespan.register(job_runner)      # started third

        app = FastAPI(lifespan=lifespan)
        # On startup: event_bus → outbox_relay → job_runner
        # On shutdown: job_runner → outbox_relay → event_bus (LIFO)

    Args:
        *components: Components to register at construction time (in order).
                     Each must have ``start()`` and ``stop()`` async methods.

    Thread safety:  ⚠️ ``register()`` must be called before the app starts.
    Async safety:   ✅ ``__call__`` is an async context manager.
    """

    def __init__(
        self,
        *components: Any,
        setup: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._components: list[Any] = list(components)
        # Optional async hook called before any component is started.
        # Useful when the caller needs to perform async DI resolution
        # (e.g. await container.ainstall(...)) and then register the
        # resolved components via self.register() — all before start().
        # DESIGN: setup callback over subclassing
        #   ✅ Keeps VarcoLifespan a plain orchestrator — no DI knowledge
        #   ✅ Closure captures all outer-scope state naturally
        #   ❌ Caller must not forget to register components inside setup
        self._setup: Callable[[], Awaitable[None]] | None = setup

    def register(self, component: Any) -> None:
        """
        Register a lifecycle component for management.

        Args:
            component: Any object with ``start()`` and ``stop()`` async methods.
                       Checked via ``AbstractLifecycle`` protocol at registration.

        Raises:
            TypeError: If ``component`` does not have ``start`` and ``stop`` methods.

        Edge cases:
            - Registering the same component twice will start/stop it twice —
              components should be idempotent.
            - Registration must happen before the app starts (not thread-safe).
        """
        if not isinstance(component, AbstractLifecycle):
            raise TypeError(
                f"Component {component!r} does not implement AbstractLifecycle. "
                "It must have async start() and stop() methods."
            )
        self._components.append(component)

    @asynccontextmanager
    async def __call__(self, app: Any) -> AsyncIterator[None]:
        """
        FastAPI lifespan context manager.

        Called by FastAPI on app startup and shutdown.  Starts all components
        in registration order, yields control to FastAPI for request handling,
        then stops all components in reverse order.

        Args:
            app: The FastAPI application (passed by FastAPI; typically unused).

        Yields:
            Nothing — the lifespan block yields to FastAPI's event loop.

        Edge cases:
            - If a component's ``start()`` raises, subsequent components are NOT
              started and the app startup fails (FastAPI raises ``RuntimeError``).
            - If a component's ``stop()`` raises during shutdown, the error is
              logged and subsequent stops still run — a failing stop must not
              block other components from cleaning up.
        """
        # Run async setup first so it can register additional components
        # (e.g. DI resolution of bus/consumer/job_runner) before the start loop.
        if self._setup is not None:
            await self._setup()

        # Start in registration order
        started: list[Any] = []
        for component in self._components:
            try:
                await component.start()
                started.append(component)
                _logger.info("VarcoLifespan: started %s", type(component).__name__)
            except Exception as exc:
                _logger.error(
                    "VarcoLifespan: failed to start %s: %s",
                    type(component).__name__,
                    exc,
                    exc_info=True,
                )
                # Stop already-started components before re-raising
                await self._stop_all(started)
                raise

        try:
            yield  # FastAPI handles requests here
        finally:
            # Stop in reverse order (LIFO)
            await self._stop_all(list(reversed(started)))

    async def _stop_all(self, components: list[Any]) -> None:
        """Stop all components, logging errors but not raising."""
        for component in components:
            try:
                await component.stop()
                _logger.info("VarcoLifespan: stopped %s", type(component).__name__)
            except Exception as exc:  # noqa: BLE001
                _logger.error(
                    "VarcoLifespan: error stopping %s: %s",
                    type(component).__name__,
                    exc,
                    exc_info=True,
                )


__all__ = [
    "AbstractLifecycle",
    "VarcoLifespan",
]
