"""
varco_ws.di
===========
Providify DI integration for ``varco_ws``.

``WebSocketEventBus`` and ``SSEEventBus`` are decorated with ``@Singleton``
and ``Inject[AbstractEventBus]`` on their constructors.  They self-register
when ``container.scan("varco_ws", recursive=True)`` is called — no
``@Configuration`` class or ``install()`` call is needed.

An ``AbstractEventBus`` implementation must already be registered in the
container before scanning ``varco_ws`` — both adapters inject it.

Usage
-----
General-purpose DI (all-events, all-channels)::

    from varco_redis.di import bootstrap as redis_bootstrap
    from varco_ws.di import bootstrap as ws_bootstrap

    redis_bootstrap()               # registers AbstractEventBus
    ws_bootstrap()                  # scans varco_ws, finds both adapters

    ws_bus  = container.get(WebSocketEventBus)
    sse_bus = container.get(SSEEventBus)

    # Start adapters in the FastAPI lifespan handler
    @asynccontextmanager
    async def lifespan(app):
        await ws_bus.start()
        await sse_bus.start()
        yield
        await ws_bus.stop()
        await sse_bus.stop()

Or manually::

    container = DIContainer()
    container.scan("varco_redis", recursive=True)   # provides AbstractEventBus
    container.scan("varco_ws", recursive=True)       # finds WebSocket + SSE adapters

Per-channel adapters
--------------------
The scan-discovered singletons subscribe to all events on all channels.
For per-channel adapters, use ``bind_websocket_adapter()`` / ``bind_sse_adapter()``
instead of manual lifespan boilerplate::

    from varco_ws.di import bootstrap, bind_websocket_adapter, bind_sse_adapter
    from myapp.events import OrderEvent

    bootstrap(container)
    bind_websocket_adapter(container, event_type=OrderEvent, channel="orders")
    bind_sse_adapter(container, event_type=OrderEvent, channel="orders")

    orders_ws  = container.get(WebSocketEventBus)   # per-channel singleton
    orders_sse = container.get(SSEEventBus)          # per-channel singleton

Lifecycle
---------
Both adapters must be explicitly started and stopped via ``start()``/``stop()``
in the FastAPI lifespan handler.  They are **not** started by the container.

DESIGN: @Singleton on adapter classes over @Provider in @Configuration
    ✅ Scan discovers adapters automatically — no install() call needed.
    ✅ No @Configuration classes required at all.
    ❌ The scan-discovered singleton always uses event_type=Event, channel="*"
       (all events, all channels) — use bind_websocket_adapter()/bind_sse_adapter()
       when per-channel filtering is needed.

Thread safety:  ✅ Safe — DI registration is single-threaded at startup.
Async safety:   ✅ No I/O at scan time; I/O happens in start().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

# Module-level imports (Plan 050 / §D-asks P31, not inside the factory
# functions below) — required so bind_websocket_adapter/bind_sse_adapter's
# closures can declare `bus: Inject[AbstractEventBus]` as a parameter.
# Under PEP 563 (`from __future__ import annotations`), a parameter
# annotation is a lazy string providify evaluates against the *defining
# function's* module globals (`providify/_annotations.py:180-235`) — a name
# imported only inside the function body is invisible to that lookup. The
# previous DESIGN note here claimed this dependency was "closure-local" and
# therefore unreachable; that was simply wrong — AbstractEventBus is an
# ordinary, stable, module-importable type, so moving the import here (not
# a runtime-only class, which genuinely cannot be done this way — see
# design/upstream-gaps/providify-provider-inject-override.md) fixes it with
# no upstream change.
from providify import DIContainer, Inject, Provider
from varco_core.event.base import AbstractEventBus

if TYPE_CHECKING:
    pass

# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(container: DIContainer) -> DIContainer:
    """
    Bootstrap ``varco_ws`` into a ``DIContainer``.

    Calls ``container.scan("varco_ws", recursive=True)`` to discover
    ``WebSocketEventBus`` and ``SSEEventBus`` (both ``@Singleton``-decorated).

    An ``AbstractEventBus`` implementation **must** already be registered in
    the container before calling this function — both adapters inject it::

        from providify import DIContainer
        from varco_redis.di import bootstrap as redis_bootstrap
        from varco_ws.di import bootstrap as ws_bootstrap

        container = DIContainer()
        redis_bootstrap(container)   # scan varco_redis → registers AbstractEventBus
        ws_bootstrap(container)      # scan varco_ws → registers WebSocketEventBus + SSEEventBus

        ws_bus  = container.get(WebSocketEventBus)
        sse_bus = container.get(SSEEventBus)

        # Start both in the FastAPI lifespan handler — not here
        @asynccontextmanager
        async def lifespan(app):
            await ws_bus.start()
            await sse_bus.start()
            yield
            await ws_bus.stop()
            await sse_bus.stop()

    Args:
        container: The ``DIContainer`` to scan into. Required — this
            function never creates one and never reads
            the process-global container (Plan 046 §D-locator).

    Returns:
        The same ``container`` passed in, after scanning.

    Edge cases:
        - Calling twice is safe — scanning is idempotent.
        - The adapters are **not started** — call ``start()`` in the lifespan
          handler before serving clients.
        - ``AbstractEventBus`` must already be registered; otherwise
          resolution raises ``LookupError`` when an adapter is first resolved.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ Scanning is synchronous.  I/O happens in ``start()``.
    """
    # Scan discovers WebSocketEventBus and SSEEventBus via their @Singleton
    # decorators.  Both inject Inject[AbstractEventBus] from the container.
    container.scan("varco_ws", recursive=True)

    return container


# ── bind_websocket_adapter ────────────────────────────────────────────────────


def bind_websocket_adapter(
    container: DIContainer,
    *,
    qualifier: str | None = None,
    event_type: Any = None,
    channel: str = "*",
    max_queue_size: int = 100,
    backpressure_policy: Any = None,
) -> DIContainer:
    """
    Register a per-channel ``WebSocketEventBus`` singleton in a providify
    ``DIContainer``.

    After this call ``container.get(WebSocketEventBus)`` returns the adapter
    configured for the requested ``event_type`` and ``channel``.  This is the
    DI-native alternative to manually instantiating
    ``WebSocketEventBus(bus, event_type=..., channel=...)`` inside a FastAPI
    lifespan handler.

    Usage::

        from varco_ws.di import bootstrap, bind_websocket_adapter
        from myapp.events import OrderEvent

        bootstrap(container)                         # scan varco_ws
        bind_websocket_adapter(
            container,
            event_type=OrderEvent,
            channel="orders",
        )

        orders_ws = container.get(WebSocketEventBus)
        # Start in the FastAPI lifespan handler — not here:
        @asynccontextmanager
        async def lifespan(app):
            await orders_ws.start()
            yield
            await orders_ws.stop()

    Args:
        container:           A providify ``DIContainer`` instance.
        qualifier:           Optional qualifier so multiple per-channel
                             adapters can coexist (§D-bind) — e.g.
                             ``qualifier="orders"``. ``None`` registers the
                             unqualified default.
        event_type:          Event class to subscribe to.  ``None`` defaults to
                             ``Event`` (all events).
        channel:             Bus channel to subscribe to.  ``"*"`` means all channels.
        max_queue_size:      Per-client outbound queue depth.  0 = unbounded.
                             Default matches ``WebSocketEventBus`` default (100).
        backpressure_policy: Action when a client queue is full.  ``None``
                             defaults to ``BackpressurePolicy.DROP_OLDEST``.

    Returns:
        The same ``container`` passed in.

    Raises:
        AlreadyBoundError: A non-fallback ``WebSocketEventBus`` binding of
            the same ``qualifier`` already exists — qualify differently to
            register a second per-channel adapter.

    Edge cases:
        - The adapter is **not** started automatically; call ``start()`` in the
          FastAPI lifespan handler.
        - ``AbstractEventBus`` must already be registered before the factory is
          resolved (not before this call) — registration order does not matter.

    Thread safety:  ✅ Registration is intended at bootstrap (single-threaded).
    Async safety:   ✅ No I/O during registration.
    """
    from varco_core.di import is_user_bound  # noqa: PLC0415
    from varco_core.event.base import Event  # noqa: PLC0415

    from varco_ws.websocket import (  # noqa: PLC0415
        BackpressurePolicy,
        WebSocketEventBus,
    )

    if is_user_bound(container, WebSocketEventBus, qualifier=qualifier):
        from varco_core.di import AlreadyBoundError  # noqa: PLC0415

        raise AlreadyBoundError(
            f"A non-fallback WebSocketEventBus binding qualifier={qualifier!r} already "
            "exists — pass a different qualifier= to register another per-channel "
            "adapter, or use providify's di_overrides/ContainerOverrides in a test."
        )

    # Capture every arg into a local binding-time variable so that the closure
    # does NOT close over the parameter names (which would alias the last call's
    # values if bind_websocket_adapter is called in a loop for multiple channels).
    _event_type = event_type if event_type is not None else Event
    _channel = channel
    _max_queue_size = max_queue_size
    _backpressure_policy = (
        backpressure_policy if backpressure_policy is not None else BackpressurePolicy.DROP_OLDEST
    )

    def _ws_factory(bus: Inject[AbstractEventBus]) -> WebSocketEventBus:
        """Singleton WebSocketEventBus factory — built once at first injection.

        ``bus`` is a **declared** injection (Plan 050 / §D-asks P31) —
        resolvable because ``AbstractEventBus`` is imported at this
        module's top level (see the module docstring's DESIGN note), so
        the PEP-563 annotation string resolves against ``varco_ws.di``'s
        own globals. A missing ``AbstractEventBus`` binding is now a
        ``validate()`` ``MISSING_BINDING``, not a ``LookupError`` at first
        ``get(WebSocketEventBus)`` — the old closure-over-``container``
        shape could only fail at first resolution.

        Registered via ``container.provide(Provider(...)(...), returns=...)`` —
        the decoration-time ``returns=`` override on ``@Provider`` means the
        placeholder return annotation on this closure never needs patching.
        """
        return WebSocketEventBus(
            bus,
            event_type=_event_type,
            channel=_channel,
            max_queue_size=_max_queue_size,
            backpressure_policy=_backpressure_policy,
        )

    container.provide(
        Provider(singleton=True, qualifier=qualifier)(_ws_factory), returns=WebSocketEventBus
    )
    return container


# ── bind_sse_adapter ──────────────────────────────────────────────────────────


def bind_sse_adapter(
    container: DIContainer,
    *,
    qualifier: str | None = None,
    event_type: Any = None,
    channel: str = "*",
    max_queue_size: int = 100,
) -> DIContainer:
    """
    Register a per-channel ``SSEEventBus`` singleton in a providify
    ``DIContainer``.

    After this call ``container.get(SSEEventBus)`` returns the adapter
    configured for the requested ``event_type`` and ``channel``.  This is the
    DI-native alternative to manually instantiating
    ``SSEEventBus(bus, event_type=..., channel=...)`` inside a FastAPI
    lifespan handler.

    Usage::

        from varco_ws.di import bootstrap, bind_sse_adapter
        from myapp.events import OrderEvent

        bootstrap(container)                  # scan varco_ws
        bind_sse_adapter(
            container,
            event_type=OrderEvent,
            channel="orders",
        )

        orders_sse = container.get(SSEEventBus)
        # Start in the FastAPI lifespan handler — not here:
        @asynccontextmanager
        async def lifespan(app):
            await orders_sse.start()
            yield
            await orders_sse.stop()

    Args:
        container:      A providify ``DIContainer`` instance.
        qualifier:      Optional qualifier so multiple per-channel adapters
                        can coexist (§D-bind). ``None`` registers the
                        unqualified default.
        event_type:     Event class to subscribe to.  ``None`` defaults to
                        ``Event`` (all events).
        channel:        Bus channel to subscribe to.  ``"*"`` means all channels.
        max_queue_size: Per-subscriber queue depth.  0 = unbounded.
                        Default matches ``SSEEventBus`` default (100).

    Returns:
        The same ``container`` passed in.

    Raises:
        AlreadyBoundError: A non-fallback ``SSEEventBus`` binding of the
            same ``qualifier`` already exists.

    Edge cases:
        - The adapter is **not** started automatically; call ``start()`` in the
          FastAPI lifespan handler.
        - ``AbstractEventBus`` must already be registered before the factory is
          resolved (not before this call) — registration order does not matter.

    Thread safety:  ✅ Registration is intended at bootstrap (single-threaded).
    Async safety:   ✅ No I/O during registration.
    """
    from varco_core.di import is_user_bound  # noqa: PLC0415
    from varco_core.event.base import Event  # noqa: PLC0415

    from varco_ws.sse import SSEEventBus  # noqa: PLC0415

    if is_user_bound(container, SSEEventBus, qualifier=qualifier):
        from varco_core.di import AlreadyBoundError  # noqa: PLC0415

        raise AlreadyBoundError(
            f"A non-fallback SSEEventBus binding qualifier={qualifier!r} already exists — "
            "pass a different qualifier= to register another per-channel adapter, or use "
            "providify's di_overrides/ContainerOverrides in a test."
        )

    # Capture every arg at binding time — same closure-capture pattern as
    # bind_websocket_adapter and bind_mcp_adapter in varco_fastapi.
    _event_type = event_type if event_type is not None else Event
    _channel = channel
    _max_queue_size = max_queue_size

    def _sse_factory(bus: Inject[AbstractEventBus]) -> SSEEventBus:
        """Singleton SSEEventBus factory — built once at first injection.

        ``bus`` is a **declared** injection (Plan 050 / §D-asks P31) — see
        ``_ws_factory``'s matching docstring above and the module
        docstring's DESIGN note.

        Registered via ``container.provide(Provider(...)(...), returns=...)``
        — the decoration-time ``returns=`` override means this closure's
        placeholder return annotation never needs patching.
        """
        return SSEEventBus(
            bus,
            event_type=_event_type,
            channel=_channel,
            max_queue_size=_max_queue_size,
        )

    container.provide(
        Provider(singleton=True, qualifier=qualifier)(_sse_factory), returns=SSEEventBus
    )
    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "bootstrap",
    "bind_websocket_adapter",
    "bind_sse_adapter",
]
