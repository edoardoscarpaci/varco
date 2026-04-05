"""
varco_core.event.memory
=======================
In-memory ``AbstractEventBus`` implementations.

Two implementations are provided:

``InMemoryEventBus``
    Full-featured bus with subscription management, middleware, error policy,
    and configurable dispatch mode.  Suitable for development, testing, and
    single-process applications.

``NoopEventBus``
    Discards all published events and returns pre-cancelled subscriptions.
    Use in unit tests that don't care about events, or to disable the event
    system entirely without removing DI bindings.

Dispatch modes
--------------
``DispatchMode.SYNC`` (default)
    ``publish()`` awaits all handlers before returning.  The caller blocks
    until every handler finishes.  Simplest for tests — assert side-effects
    immediately after ``await bus.publish(...)``.

``DispatchMode.BACKGROUND``
    ``publish()`` schedules handlers as a single ``asyncio.Task`` and returns
    immediately.  The caller is never blocked by handler latency — correct
    for production use where events must not inflate HTTP response time.

    **Testing with BACKGROUND mode**: use ``await bus.drain()`` to wait for
    all pending background tasks before making assertions::

        await service.create(dto, ctx)
        await bus.drain()                # wait for background handlers
        event, _ = bus.emitted[0]
        assert isinstance(event, EntityCreatedEvent)

Background task safety
----------------------
Background tasks in asyncio must be strongly referenced while running —
Python's GC can collect weakly-referenced tasks mid-execution.  ``InMemoryEventBus``
keeps a ``_pending_tasks: set[Task]`` and uses ``task.add_done_callback`` to
discard tasks on completion.  This is the pattern recommended in the Python
asyncio docs.

``emitted`` list
----------------
Records every published ``(event, channel)`` pair for test inspection.
This list grows unbounded — intended for testing only.  In production, use
``InMemoryEventBus`` with a bounded ring-buffer or replace with a real bus.
Call ``clear_emitted()`` between test cases to avoid stale assertions.

Thread safety:  ❌  ``InMemoryEventBus`` is NOT thread-safe.  All access
                    must be from the same event loop.
Async safety:   ✅  ``publish`` is ``async def``.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

from providify import Instance, Singleton

from varco_core.event.base import (
    CHANNEL_ALL,
    CHANNEL_DEFAULT,
    AbstractEventBus,
    ChannelConfig,
    DispatchMode,
    ErrorPolicy,
    Event,
    EventMiddleware,
    Subscription,
    _SubscriptionEntry,
)

_logger = logging.getLogger(__name__)


# ── InMemoryEventBus ──────────────────────────────────────────────────────────


@Singleton(priority=-sys.maxsize - 1, qualifier="in_memory")
class InMemoryEventBus(AbstractEventBus):
    """
    Synchronous or background in-process event bus.

    Suitable for development, testing, and single-process applications.
    Replace with ``KafkaEventBus`` or ``RedisEventBus`` for cross-process
    delivery.

    Args:
        error_policy:   Controls handler error behaviour.
                        Defaults to ``ErrorPolicy.COLLECT_ALL``.
        dispatch_mode:  ``SYNC`` (default) — callers wait for all handlers.
                        ``BACKGROUND`` — handlers run as a background task;
                        callers return immediately.
        middleware:     Optional list of ``EventMiddleware`` instances.
                        Applied in list order (index 0 is outermost).

    Thread safety:  ❌  Not thread-safe — use from a single event loop only.
    Async safety:   ✅  ``publish`` is ``async def``.

    Example (production — non-blocking)::

        bus = InMemoryEventBus(dispatch_mode=DispatchMode.BACKGROUND)

    Example (tests — blocking, assert immediately)::

        bus = InMemoryEventBus()   # SYNC is the default
        await bus.publish(event, channel="orders")
        assert len(bus.emitted) == 1

    Example (tests — with BACKGROUND mode)::

        bus = InMemoryEventBus(dispatch_mode=DispatchMode.BACKGROUND)
        await bus.publish(event, channel="orders")
        await bus.drain()          # wait for background handlers
        assert len(bus.emitted) == 1
    """

    def __init__(
        self,
        *,
        error_policy: ErrorPolicy = ErrorPolicy.COLLECT_ALL,
        dispatch_mode: DispatchMode = DispatchMode.SYNC,
        middleware: Instance[EventMiddleware] | list[EventMiddleware] | None = None,
    ) -> None:
        """
        Args:
            error_policy:  Handler error policy.
            dispatch_mode: ``SYNC`` or ``BACKGROUND``.  See module docstring.
            middleware:    DI instance handle for ``EventMiddleware`` bindings.
                           All registered middlewares are resolved via
                           ``middleware.get_all()`` when provided.
        """
        self._subscriptions: list[_SubscriptionEntry] = []

        # Channel registry — maps logical channel name → optional ChannelConfig.
        # Auto-populated on every publish() and subscribe() call so that
        # channels are always visible even when not explicitly declared.
        # DESIGN: dict over set — stores config alongside the name so
        # declare_channel() metadata is preserved for introspection.
        self._channels: dict[str, ChannelConfig | None] = {}

        # All published (event, channel) pairs — appended before dispatch so
        # test assertions always see the event even if a handler raises.
        # DESIGN: unbounded list — intentionally for testing only.
        # Production deployments should not rely on this list.
        self.emitted: list[tuple[Event, str]] = []

        self._error_policy = error_policy
        self._dispatch_mode = dispatch_mode
        # Support both DI-injected Instance[EventMiddleware] and direct list
        # construction (used in tests and non-DI usage patterns).
        # DESIGN: isinstance guard over a single code path
        #   ✅ Backward-compatible — existing tests that pass list(...) unchanged.
        #   ✅ DI path resolves all registered EventMiddleware singletons via get_all().
        #   ❌ Two branches — acceptable given the stable public API contract.
        if middleware is None:
            self._middleware: list[EventMiddleware] = []
        elif isinstance(middleware, list):
            self._middleware = middleware
        else:
            # Instance[EventMiddleware] from DI container
            self._middleware = (
                list(middleware.get_all()) if middleware.resolvable() else []
            )

        # Strong references to pending background tasks — prevents GC from
        # collecting tasks before they complete.  Each task removes itself
        # via add_done_callback when it finishes.
        # DESIGN: set over list — O(1) discard vs O(n) remove.
        self._pending_tasks: set[asyncio.Task] = set()

        # Pre-build the middleware chain once — avoids rebuilding per publish.
        # Typed as Coroutine (not just Awaitable) so asyncio.create_task()
        # accepts the result in BACKGROUND mode — create_task requires a
        # coroutine object, not a generic Awaitable.
        self._chain: Callable[[Event, str], Coroutine[Any, Any, None]] = (
            self._build_chain()
        )

    # ── Subscription management ────────────────────────────────────────────────

    def subscribe(
        self,
        event_type: type[Event] | str,
        handler: Callable[[Event], Awaitable[None] | None],
        *,
        channel: str = CHANNEL_ALL,
        filter: Callable[[Event], bool] | None = None,  # noqa: A002
        priority: int = 0,
    ) -> Subscription:
        """
        Register a handler for matching events.

        Args:
            event_type: ``Event`` subclass (class dispatch, inheritance-aware)
                        or ``__event_type__`` string (exact match).
            handler:    Async or sync callable invoked with the matching event.
            channel:    Channel filter.  Defaults to ``CHANNEL_ALL`` (``"*"``).
            filter:     Optional predicate applied after event_type/channel match.
            priority:   Dispatch order within a single publish call.  Higher
                        values run first.  Equal priorities run in subscription
                        order (FIFO).  Defaults to ``0``.

        Returns:
            A ``Subscription`` handle.  Call ``.cancel()`` to deregister.
        """
        entry = _SubscriptionEntry(
            event_type=event_type,
            channel=channel,
            handler=handler,
            filter=filter,
            priority=priority,
        )
        self._subscriptions.append(entry)
        # Auto-record the channel so channel_exists() / list_channels() always
        # reflect channels that have been used, even without a declare_channel() call.
        # CHANNEL_ALL ("*") is a routing wildcard — not a real channel name.
        if channel != CHANNEL_ALL:
            self._channels.setdefault(channel, None)
        return Subscription(entry)

    # ── Publishing ─────────────────────────────────────────────────────────────

    async def publish(
        self,
        event: Event,
        *,
        channel: str = CHANNEL_DEFAULT,
    ) -> asyncio.Task[None] | None:
        """
        Publish ``event`` to ``channel``.

        Behaviour depends on ``dispatch_mode``:

        - ``SYNC``: awaits all handlers before returning.  Exceptions from
          handlers propagate to the caller according to ``error_policy``.
        - ``BACKGROUND``: schedules handlers as a background ``asyncio.Task``
          and returns immediately.  Exceptions are logged via ``_on_task_done``;
          they are never raised to the caller.

        Args:
            event:   The event to publish.
            channel: Target channel.  Defaults to ``CHANNEL_DEFAULT``.

        Raises:
            ExceptionGroup: (``SYNC`` + ``COLLECT_ALL``) One or more handlers raised.
            Exception:      (``SYNC`` + ``FAIL_FAST``) First handler error.

        Edge cases:
            - ``emitted`` is appended BEFORE dispatch — test assertions always
              see the event even when a handler raises under ``FAIL_FAST``.
            - Cancelled subscriptions are flushed at the start of each publish.
            - In ``BACKGROUND`` mode no exception is ever raised to the caller.
        """
        # Record before dispatch — test assertions always see the event
        self.emitted.append((event, channel))
        # Auto-register the channel so list_channels() reflects real usage.
        self._channels.setdefault(channel, None)

        # Flush cancelled entries — O(n) but runs once per publish, not per handler.
        # Safe to do here because we're about to iterate self._subscriptions.
        self._subscriptions = [s for s in self._subscriptions if not s.cancelled]

        if self._dispatch_mode is DispatchMode.BACKGROUND:
            # Schedule the full middleware + dispatch pipeline as a background task.
            # The caller returns immediately after task creation.
            task = asyncio.create_task(
                self._chain(event, channel),
                # Name makes debugging easier — visible in asyncio task dumps
                name=f"event-dispatch:{type(event).__name__}@{channel}",
            )
            # Strong reference — prevents GC from collecting the task mid-run.
            # discard() is called automatically when the task finishes.
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)
            task.add_done_callback(self._on_task_done)
            # Return the task so callers can optionally await it if they need
            # to synchronise on completion (e.g. end-to-end tests, saga steps).
            return task
        else:
            # SYNC: await the full pipeline before returning to the caller.
            await self._chain(event, channel)
            return None

    # ── Background task error handling ────────────────────────────────────────

    def _on_task_done(self, task: asyncio.Task) -> None:
        """
        Handle a completed background dispatch task.

        Called automatically by asyncio when a background task finishes
        (success, exception, or cancellation).  In ``BACKGROUND`` mode,
        exceptions can never be raised to the caller — they are logged here.

        Args:
            task: The completed background task.

        Edge cases:
            - Cancelled tasks are silently ignored — cancellation is intentional
              (e.g. application shutdown or ``drain()`` with a timeout).
            - Under ``COLLECT_ALL`` the task may raise ``ExceptionGroup`` —
              logged as a group.
            - Under ``FAIL_FAST`` the task raises the first handler exception —
              logged individually.
            - Under ``FIRE_FORGET`` the task never raises — this callback is a
              no-op beyond the discard already applied by the other callback.
        """
        if task.cancelled():
            # Intentional cancellation — no action needed
            return

        exc = task.exception()
        if exc is None:
            return

        # Log the error — cannot raise since the caller has already returned.
        _logger.error(
            "Background event dispatch raised an unhandled error "
            "(dispatch_mode=BACKGROUND, error_policy=%s): %s",
            self._error_policy.value,
            exc,
            exc_info=exc,
        )

    # ── Internal dispatch ──────────────────────────────────────────────────────

    async def _dispatch(self, event: Event, channel: str) -> None:
        """
        Dispatch ``event`` to all matching handlers with error policy applied.

        Called at the end of the middleware chain — either directly (SYNC)
        or inside a background task (BACKGROUND).

        Args:
            event:   The (possibly middleware-modified) event.
            channel: The (possibly middleware-modified) channel.

        Raises:
            ExceptionGroup: (``COLLECT_ALL``) Collected handler errors.
            Exception:      (``FAIL_FAST``) First handler error.

        Edge cases:
            - Snapshots the subscription list before iterating so handlers
              that cancel their own subscription mid-dispatch don't affect the
              current iteration.
            - Detects sync vs async handlers via ``asyncio.iscoroutine`` on
              the return value — works for both plain and ``functools.wraps``
              wrapped coroutines.
        """
        errors: list[BaseException] = []

        # Build a filtered, priority-sorted snapshot of matching entries.
        # Sorting is done here (just-in-time) rather than at subscribe() time
        # because:
        #   1. Subscriptions can be added after construction — maintaining a
        #      sorted list would require bisect inserts everywhere.
        #   2. Cancelled entries are already filtered out — no wasted iterations.
        #   3. Python's sort is stable — equal-priority handlers run in
        #      subscription order (FIFO), which is the documented guarantee.
        matching_entries = sorted(
            (
                e
                for e in self._subscriptions
                if not e.cancelled
                and self._matches_event_type(event, e.event_type)
                and self._matches_channel(channel, e.channel)
                and (e.filter is None or e.filter(event))
            ),
            key=lambda e: e.priority,
            reverse=True,  # Higher priority → runs first
        )

        for entry in matching_entries:
            try:
                result = entry.handler(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:  # noqa: BLE001
                if self._error_policy is ErrorPolicy.FAIL_FAST:
                    raise
                elif self._error_policy is ErrorPolicy.COLLECT_ALL:
                    errors.append(exc)
                elif self._error_policy is ErrorPolicy.FIRE_FORGET:
                    _logger.warning(
                        "Event handler %r raised and was ignored "
                        "(FIRE_FORGET policy): %s",
                        entry.handler,
                        exc,
                        exc_info=True,
                    )

        if errors:
            if len(errors) == 1:
                # Unwrap single error — cleaner traceback than a 1-item ExceptionGroup
                raise errors[0]
            raise ExceptionGroup(
                f"Event handlers raised {len(errors)} error(s) "
                f"for {type(event).__name__!r} on channel {channel!r}",
                errors,
            )

    # ── Middleware chain builder ───────────────────────────────────────────────

    def _build_chain(self) -> Callable[[Event, str], Coroutine[Any, Any, None]]:
        """
        Build the middleware chain once at construction time.

        Chain order: middleware[0] → middleware[1] → … → _dispatch.
        Built by wrapping from the end backwards so the first middleware
        in the list is the outermost wrapper.

        Returns:
            A coroutine function ``(event, channel) → None``.
        """

        async def core(event: Event, channel: str) -> None:
            await self._dispatch(event, channel)

        # All functions in the chain are async def — they return Coroutine,
        # not just Awaitable. The annotation is explicit so create_task() accepts
        # the result without a cast.
        chain: Callable[[Event, str], Coroutine[Any, Any, None]] = core

        for mw in reversed(self._middleware):
            # Capture mw and chain in a closure — avoids loop late-binding bug
            def _make_step(
                _mw: EventMiddleware,
                _next: Callable[[Event, str], Coroutine[Any, Any, None]],
            ) -> Callable[[Event, str], Coroutine[Any, Any, None]]:
                async def step(event: Event, channel: str) -> None:
                    await _mw(event, channel, _next)

                return step

            chain = _make_step(mw, chain)

        return chain

    # ── Routing helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _matches_event_type(
        event: Event,
        event_type: type[Event] | str,
    ) -> bool:
        """
        Return ``True`` if ``event`` matches the ``event_type`` filter.

        Class-based dispatch uses ``isinstance`` — supports full inheritance.
        String dispatch matches against ``__event_type__`` or class name.

        Edge cases:
            - ``isinstance`` handles diamond inheritance correctly.
            - String comparison is case-sensitive.
        """
        if isinstance(event_type, type):
            # Inheritance-aware: subscribing to EntityEvent receives all subclasses
            return isinstance(event, event_type)
        declared_name = getattr(type(event), "__event_type__", type(event).__name__)
        return event_type == declared_name

    @staticmethod
    def _matches_channel(publish_channel: str, subscribe_channel: str) -> bool:
        """
        Return ``True`` if ``subscribe_channel`` matches ``publish_channel``.

        ``CHANNEL_ALL`` (``"*"``) matches any publish channel.
        """
        return subscribe_channel == CHANNEL_ALL or subscribe_channel == publish_channel

    # ── Test utilities ─────────────────────────────────────────────────────────

    async def drain(self) -> None:
        """
        Wait for all pending background dispatch tasks to complete.

        Required in tests that use ``DispatchMode.BACKGROUND`` and need to
        assert on handler side-effects after publishing::

            bus = InMemoryEventBus(dispatch_mode=DispatchMode.BACKGROUND)
            await service.create(dto, ctx)
            await bus.drain()          # ← wait for background handlers
            assert handler.called      # ← safe to assert now

        In ``SYNC`` mode this is a no-op — all handlers have already
        completed by the time ``publish()`` returned.

        Edge cases:
            - Safe to call when no tasks are pending — returns immediately.
            - Exceptions from background tasks are NOT re-raised here; they
              were already logged by ``_on_task_done``.  Use ``SYNC`` mode
              if you need exceptions to propagate.
            - New tasks created during ``drain()`` (handlers that publish
              further events) are NOT awaited — call ``drain()`` again if
              needed.
        """
        if not self._pending_tasks:
            return

        # Snapshot — tasks completing during gather remove themselves from
        # the set via done_callback; iterating the live set is unsafe.
        # return_exceptions=True — errors were already logged by _on_task_done;
        # we don't want gather to raise here.
        await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)

    def clear_emitted(self) -> None:
        """
        Clear the ``emitted`` history.

        Use between test cases when the same bus instance is reused::

            bus.clear_emitted()
            await service.create(dto, ctx)
            assert len(bus.emitted) == 1

        Edge cases:
            - Does NOT cancel active subscriptions.
            - Does NOT cancel pending background tasks.
        """
        self.emitted.clear()

    # ── Channel management ──────────────────────────────────────────────────────

    async def declare_channel(
        self,
        channel: str,
        config: ChannelConfig | None = None,
    ) -> None:
        """
        Explicitly declare a channel, optionally with configuration metadata.

        Unlike ``subscribe()`` and ``publish()`` which auto-register channels
        implicitly, this method lets you pre-declare a channel BEFORE any
        events are published or subscribed — useful for validating that the
        channel set is correct at startup time.

        Args:
            channel: Logical channel name to declare.
            config:  Optional ``ChannelConfig`` (e.g. Kafka partitions,
                     retention).  Stored for introspection; ``InMemoryEventBus``
                     does not act on these values.

        Edge cases:
            - Calling twice with the same channel is idempotent — the config is
              updated only if ``config`` is not ``None`` on the second call.
              Passing ``None`` on a subsequent call leaves the existing config
              unchanged.
            - ``CHANNEL_ALL`` (``"*"``) may be declared but has no special
              meaning beyond being visible in ``list_channels()``.
        """
        # Preserve existing config if a second call passes None
        if channel not in self._channels or config is not None:
            self._channels[channel] = config

    async def channel_exists(self, channel: str) -> bool:
        """
        Return ``True`` if ``channel`` has been declared or used on this bus.

        A channel is "known" if:
        - It was passed to ``declare_channel()``, OR
        - It was used as the ``channel`` argument of ``publish()``, OR
        - It was used as the ``channel`` argument of ``subscribe()``
          (excluding ``CHANNEL_ALL``).

        Args:
            channel: Logical channel name to check.

        Returns:
            ``True`` if the channel is in the internal registry.

        Edge cases:
            - Returns ``False`` for channels that have never been touched,
              even if ``subscribe(channel=CHANNEL_ALL)`` has been called —
              the wildcard is not a concrete channel.
        """
        return channel in self._channels

    async def list_channels(self) -> list[str]:
        """
        Return all channels known to this bus, sorted alphabetically.

        Includes channels registered via ``declare_channel()``, ``publish()``,
        or ``subscribe()`` (excluding ``CHANNEL_ALL``).

        Returns:
            Sorted list of channel name strings.

        Edge cases:
            - Returns an empty list on a fresh bus with no subscriptions or
              published events.
            - Channels from cancelled subscriptions remain in the list.
        """
        return sorted(self._channels.keys())

    async def delete_channel(self, channel: str) -> None:
        """
        Remove a channel from this bus and cancel all its subscriptions.

        After deletion, ``channel_exists(channel)`` returns ``False`` and
        ``list_channels()`` no longer includes the channel.  Active
        subscriptions whose ``channel`` matches exactly (not via wildcard)
        are cancelled.

        Args:
            channel: Logical channel name to delete.

        Edge cases:
            - Deleting a non-existent channel is a no-op.
            - Subscriptions with ``channel=CHANNEL_ALL`` are NOT cancelled —
              they would still receive events published to this channel name
              if a new subscription is later created.
            - Pending background tasks are NOT cancelled.  Events that were
              already dispatched continue to their handlers.
        """
        self._channels.pop(channel, None)
        # Cancel all subscriptions on this exact channel
        for entry in self._subscriptions:
            if entry.channel == channel:
                entry.cancelled = True

    def __repr__(self) -> str:
        active = sum(1 for s in self._subscriptions if not s.cancelled)
        return (
            f"InMemoryEventBus("
            f"subscriptions={active}, "
            f"emitted={len(self.emitted)}, "
            f"pending_tasks={len(self._pending_tasks)}, "
            f"mode={self._dispatch_mode.value!r}, "
            f"policy={self._error_policy.value!r})"
        )


# ── NoopEventBus ──────────────────────────────────────────────────────────────


class NoopEventBus(AbstractEventBus):
    """
    ``AbstractEventBus`` that silently discards all events.

    Use when:

    - A unit test does not care about events at all and you want to avoid
      setting up an ``InMemoryEventBus``.
    - You want to disable the event system for a specific component without
      removing DI bindings.
    - Benchmarking code where event overhead should be excluded.

    ``subscribe()`` returns a pre-cancelled ``Subscription`` — the handler
    is never registered and will never be called.

    DESIGN: NoopEventBus vs NoopEventProducer
        ``NoopEventProducer`` is the correct default for *services* — it
        avoids the bus entirely at the producer level.
        ``NoopEventBus`` is useful when code depends directly on
        ``AbstractEventBus`` (e.g. ``EventConsumer.register_to(bus)`` still
        needs a bus instance to accept the call without error).

    Thread safety:  ✅  Stateless — no shared mutable state.
    Async safety:   ✅  ``publish`` returns immediately.
    """

    async def publish(
        self,
        event: Event,
        *,
        channel: str = CHANNEL_DEFAULT,
    ) -> asyncio.Task[None] | None:
        """Discard ``event`` silently.  Always returns ``None``."""
        # Intentional no-op — Null Object pattern.
        # Both params are unused by design; assign to _ to suppress IDE hints.
        _ = event, channel
        return None

    def subscribe(
        self,
        event_type: type[Event] | str,
        handler: Callable[[Event], Awaitable[None] | None],
        *,
        channel: str = CHANNEL_ALL,
        filter: Callable[[Event], bool] | None = None,  # noqa: A002
        priority: int = 0,
    ) -> Subscription:
        """
        Return a pre-cancelled ``Subscription`` — handler is never registered.

        Returns:
            A ``Subscription`` whose ``is_cancelled`` is already ``True``.

        Edge cases:
            - Calling ``.cancel()`` on the returned subscription is a no-op
              (already cancelled).
        """
        # Create an entry and immediately cancel it — the handler is never
        # added to any dispatch list, so it will never be called.
        # priority is accepted for API compatibility with AbstractEventBus but
        # has no effect here — suppress the unused-variable hint explicitly.
        _ = priority
        entry = _SubscriptionEntry(
            event_type=event_type,
            channel=channel,
            handler=handler,
            filter=filter,
        )
        entry.cancelled = True
        return Subscription(entry)

    def __repr__(self) -> str:
        return "NoopEventBus()"
