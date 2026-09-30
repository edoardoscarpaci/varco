"""
varco_fastapi.lifespan
======================
FastAPI lifespan handler for startup/shutdown of varco services.

``VarcoLifespan`` collects registered lifecycle objects and delegates the
actual start/stop orchestration to ``varco_core.di.lifecycle.LifecycleRunner``
(Plan 050 / §D-runner) — this class is now a thin ASGI adapter over that
Docker-free, FastAPI-free engine.

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

Phased ordering (Plan 050)
---------------------------
A modules-built app (``create_varco_app``) does not construct
``VarcoLifespan`` with a hand-picked registration order at all — its
components come from ``varco_core.di.lifecycle.phased_components()``,
sequenced by each ``VarcoModule``'s declared ``phase`` (and, within a
phase, ``depends_on``). The hand-wired form below (bare
``lifespan.register(component)`` calls) keeps its pre-050 contract exactly:
registration order in, exact reverse order out.

Recommended startup order (hand-wired form only)
--------------------------------------------------
Register components in dependency order. Each component starts after the
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

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from varco_core.di.lifecycle import AbstractLifecycle, LifecycleRunner

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
        setup:       Optional async hook awaited *before* any component starts.
        shutdown:    Optional async hook awaited *after* every component has
                     stopped.  ``create_varco_app()`` fills it with
                     ``lambda: container.ashutdown()`` (Plan 022 / RL-8a).

    Attributes:
        last_runner: The most recently entered ``LifecycleRunner`` — set at
            the start of every ``__call__`` cycle (Plan 050). Exposes
            ``.plan``/``.last_failures`` for 052's ``varco diagnose``.

    Thread safety:  ⚠️ ``register()`` must be called before the app starts.
    Async safety:   ✅ ``__call__`` is an async context manager.
    """

    def __init__(
        self,
        *components: Any,
        setup: Callable[[], Awaitable[None]] | None = None,
        shutdown: Callable[[], Awaitable[None]] | None = None,
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
        # Optional async teardown hook, awaited once after the runner's
        # stop pass. Plan 022 / §D-8a2(a) — the mirror image of setup=, and
        # the seam through which create_varco_app() reaches
        # container.ashutdown() so that @PreDestroy-bearing singletons which
        # are NOT registered lifecycle components are torn down at all.
        # DESIGN: a callable hook over a `container=` kwarg
        #   ✅ A callable is not DI knowledge — a container would be, and the
        #      setup= DESIGN block above explicitly refuses that.
        #   ✅ Additive: every existing VarcoLifespan(...) call site is unchanged,
        #      and omitting it is byte-identical to pre-3.0.0 behaviour.
        #   ✅ Testable with a bare coroutine factory — no providify needed.
        #   ❌ A second optional kwarg on a class that already had one.  Accepted:
        #      the symmetry with setup= is precisely the point.
        #   Rejected: a process-global-container lookup inside the runner — implicit
        #      global state, untestable, and silently wrong in multi-container tests.
        self._shutdown: Callable[[], Awaitable[None]] | None = shutdown
        self.last_runner: LifecycleRunner | None = None

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

        Called by FastAPI on app startup and shutdown. Delegates to a fresh
        ``LifecycleRunner`` built over this instance's registered
        components, in registration order (Plan 050 / §D-runner) — the
        runner owns start order, exact-reverse stop, rollback-on-partial-
        start, and the ``shutdown=`` hook.

        Args:
            app: The FastAPI application (passed by FastAPI; typically unused).

        Yields:
            Nothing — the lifespan block yields to FastAPI's event loop.

        Edge cases:
            - If a component's ``start()`` raises, subsequent components are NOT
              started, already-started ones are rolled back in exact reverse
              order, and the app startup fails (FastAPI raises the original
              exception).
            - If a component's ``stop()`` raises during shutdown, the error is
              logged and subsequent stops still run — a failing stop must not
              block other components from cleaning up.
            - ``VarcoLifespan`` calls the ``shutdown=`` hook, when one was
              supplied, **after** every stop — never instead of it. Registered
              components therefore still stop in documented reverse dependency
              order, and only then does the hook (in practice
              ``container.ashutdown()``) sweep whatever the DI container still
              holds. A component reachable by both paths has ``stop()`` called
              twice; all ten shipped ``@PreDestroy`` components were measured
              idempotent, and ``register()``'s docstring already required it.
            - With **no** ``shutdown=`` hook the behaviour is byte-identical to
              pre-3.0.0: a ``@PreDestroy`` hook on a singleton that is not also
              a registered lifecycle component is never run. Plan 022 / RL-8a
              measured six such orphans out of ten and adopted the hook in
              ``create_varco_app()``; see
              ``design/api-freeze-and-standards/measurements/predestroy-vs-lifespan.md``
              for the evidence and
              ``test_lifespan_shutdown_characterization.py`` for the locked
              shape of providify's aggregated ``ShutdownError``.
            - The hook runs even when ``setup()`` or a component's ``start()``
              raised — a half-built app is exactly the case where eagerly
              constructed singletons would otherwise leak.
            - The hook never raises out of the lifespan. Failures are logged at
              ERROR, one line per ``ShutdownFailure``, consistent with the
              runner's own "logs errors but does not raise" contract.
        """
        runner = LifecycleRunner(self._components, setup=self._setup, shutdown=self._shutdown)
        self.last_runner = runner
        async with runner:
            yield


__all__ = [
    "AbstractLifecycle",
    "VarcoLifespan",
]
