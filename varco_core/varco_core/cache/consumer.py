"""
varco_core.cache.consumer
=========================
``CacheInvalidationConsumer`` — event-driven cache invalidation via ``EventConsumer``.

Subscribes to a configurable bus channel and marks arriving ``CacheInvalidated``
key lists as invalidated in a shared ``ExplicitStrategy``.  The strategy is then
consulted at read-time by the ``CacheBackend`` so the stale entries are evicted
on the next ``get()`` call.

Architecture
------------
::

    CacheInvalidationConsumer (EventConsumer)
        register_to(bus) → bus.subscribe(CacheInvalidated, handler, channel=…)
            — channel resolved via callable: lambda self: self._channel

    handler → strategy.invalidate_many(event.keys)

    CacheBackend
        strategy = ExplicitStrategy()  ← same instance, shared by reference

Wiring example::

    from varco_core.cache import InMemoryCache, ExplicitStrategy
    from varco_core.cache.consumer import CacheInvalidationConsumer
    from varco_core.event import InMemoryEventBus, BusEventProducer

    # Shared strategy — must be the SAME object passed to both cache and consumer
    strategy = ExplicitStrategy()
    cache = InMemoryCache(strategy=strategy)

    bus = InMemoryEventBus()
    consumer = CacheInvalidationConsumer(strategy)
    consumer.register_to(bus)          # subscribe — now listens for CacheInvalidated

    # On the write side, CachedService receives an AbstractEventProducer:
    producer = BusEventProducer(bus)
    cached_svc = CachedService(svc, cache, namespace="user", producer=producer)

    async with bus, cache:
        user = await cached_svc.get(1)    # miss → cached
        await cached_svc.update(1, data)  # updates DB, producer publishes CacheInvalidated
        # consumer receives the event → strategy.invalidate_many(["user:get:1", …])
        # next get() evicts "user:get:1" → fresh DB read

Replacing ``EventDrivenStrategy``
----------------------------------
Before this module, ``EventDrivenStrategy`` was both an ``InvalidationStrategy``
(read-side) AND managed a bus subscription (infrastructure-side).  This module
separates those two responsibilities:

    EventDrivenStrategy (old) = CacheInvalidationConsumer + ExplicitStrategy (new)

    Old:  strategy = EventDrivenStrategy(bus, channel="…")
          async with InMemoryCache(strategy=strategy) as cache:
              await strategy.start()   # ← opens bus subscription inside strategy

    New:  strategy = ExplicitStrategy()
          cache = InMemoryCache(strategy=strategy)
          consumer = CacheInvalidationConsumer(strategy, channel="…")
          consumer.register_to(bus)   # ← EventConsumer is the only bus-touching class

Callable channel in ``@listen``
--------------------------------
``@listen`` stores metadata at class-definition time, so instance attributes
(like ``self._channel``) cannot be referenced directly.  Instead, a callable
``lambda self: self._channel`` is passed as the ``channel`` argument.  When
``register_to(bus)`` is called, it has ``self`` in scope and resolves the
callable to the actual channel string before calling ``bus.subscribe()``.

DESIGN: callable channel in ``@listen`` over overriding ``register_to``
    ✅ Keeps all subscription wiring inside ``register_to`` — no scattered overrides.
    ✅ ``@listen`` stays declarative; channel is visible at the method definition.
    ✅ Backward-compatible — existing string channels work unchanged.
    ✅ ``CacheInvalidationConsumer`` never needs to call ``bus.subscribe()`` itself —
       the bus-touching code stays in ``EventConsumer.register_to``.
    ❌ The lambda syntax is less obvious than a string literal — mitigated by
       the docstring and consistent use of the pattern across the codebase.

DESIGN: separate consumer + ExplicitStrategy over EventDrivenStrategy
    ✅ EventConsumer is the only type that calls bus.subscribe() — all bus
       access is localized to the intended abstraction layer.
    ✅ ExplicitStrategy remains pure — no bus dependency, easier to test.
    ✅ CacheInvalidationConsumer is testable without a running cache backend.
    ❌ Caller must wire the same ExplicitStrategy instance to both the cache
       backend and the consumer — the connection is now explicit, not implicit.

Thread safety:  ❌  Delegates to ExplicitStrategy — not thread-safe.
Async safety:   ✅  ``_on_invalidation_event`` is ``async def`` — awaited by the bus.

📚 Docs
- 🐍 ``EventConsumer.register_to()`` — base class subscription wiring
- 🐍 ``ExplicitStrategy`` — the key-tracking strategy this consumer writes to
- 🐍 ``CacheInvalidated`` — the domain event carrying the stale key list
- 🐍 ``@listen(channel=callable)`` — callable channel resolved at ``register_to`` time
"""

from __future__ import annotations

import logging
from typing import Final

from varco_core.cache.invalidation import ExplicitStrategy
from varco_core.cache.service import CacheInvalidated
from varco_core.event.consumer import EventConsumer, listen

_logger = logging.getLogger(__name__)

# Default channel — matches CacheServiceMixin._cache_bus_channel and CachedService
# defaults so out-of-the-box wiring requires no channel configuration at all.
_DEFAULT_INVALIDATION_CHANNEL: Final[str] = "varco.cache.invalidations"


class CacheInvalidationConsumer(EventConsumer):
    """
    ``EventConsumer`` that drives cache invalidation from bus events.

    Subscribes to a ``CacheInvalidated`` event channel and marks the arriving
    keys as invalidated in a shared ``ExplicitStrategy``.  The strategy is the
    same instance passed to the ``CacheBackend`` — so when the backend consults
    it on the next read, stale entries are evicted transparently.

    This class replaces the former ``EventDrivenStrategy``, splitting its two
    responsibilities:

    - **Bus interaction** → this class  (``EventConsumer`` is the only type
      that calls ``bus.subscribe()`` directly).
    - **Key tracking** → ``ExplicitStrategy``  (pure, no bus knowledge).

    The channel is configured per-instance at construction time.  Because
    ``@listen`` stores metadata at class-definition time (where ``self`` does
    not exist), a callable ``lambda self: self._channel`` is passed instead of
    a string literal.  ``register_to`` resolves it when ``self`` is available.

    Args:
        strategy: The ``ExplicitStrategy`` shared with the ``CacheBackend``.
                  Must be the *same instance* — a copy will not work because
                  key tracking is instance-local state.
        channel:  Bus channel to subscribe to.  Defaults to
                  ``"varco.cache.invalidations"`` which matches the default
                  used by ``CacheServiceMixin`` and ``CachedService``.

    Thread safety:  ❌  Delegates to ``ExplicitStrategy`` — not thread-safe.
    Async safety:   ✅  ``_on_invalidation_event`` is ``async def``.

    Edge cases:
        - If the bus has not been started before ``register_to()`` is called,
          events will not be received — always start the bus first.
        - Keys in the event that are not present in the cache are silently added
          to ``ExplicitStrategy``'s invalidated set and never evicted.  Call
          ``strategy.reset()`` periodically if memory growth is a concern.
        - Registering the same consumer to the same bus twice produces two
          independent subscriptions — events are processed twice, which is
          harmless but wasteful.  Avoid double registration.
        - ``CHANNEL_ALL`` (``"*"``) is a valid ``channel`` value and will
          receive ``CacheInvalidated`` events regardless of the publish channel.

    📚 Docs
    - 🐍 ``EventConsumer.register_to()`` — base class for subscription wiring
    - 🐍 ``ExplicitStrategy.invalidate_many()`` — key-marking target
    - 🐍 ``CacheInvalidated`` — event type this consumer subscribes to
    - 🐍 ``@listen(channel=lambda self: …)`` — callable channel pattern
    """

    def __init__(
        self,
        strategy: ExplicitStrategy,
        *,
        channel: str = _DEFAULT_INVALIDATION_CHANNEL,
    ) -> None:
        """
        Args:
            strategy: ``ExplicitStrategy`` instance shared with the cache
                      backend.  Must be the *same object* — not a copy.
            channel:  Channel to listen on for ``CacheInvalidated`` events.
                      Defaults to ``"varco.cache.invalidations"``.

        Edge cases:
            - ``channel`` must match exactly what the producer publishes to.
        """
        # Store strategy reference — the bus handler writes into it so the
        # cache backend sees eviction decisions on the next read.
        self._strategy = strategy
        # Channel is stored as an instance attribute because it cannot be a
        # class-level constant when different instances target different channels.
        # The @listen decorator below resolves it at register_to time via
        # `lambda self: self._channel`, not at class-definition time.
        self._channel = channel

    # ``@listen`` stores entries at class-definition time.  The channel is
    # NOT a compile-time constant here — it comes from self._channel which
    # is set in __init__.  The callable form ``lambda self: self._channel``
    # defers resolution to ``register_to``, when self is available.
    @listen(CacheInvalidated, channel=lambda self: self._channel)  # type: ignore[arg-type]
    async def _on_invalidation_event(self, event: CacheInvalidated) -> None:
        """
        Handle an incoming ``CacheInvalidated`` event.

        Marks all ``event.keys`` in the shared ``ExplicitStrategy`` for
        eviction.  The cache backend will evict them on the next ``get()`` call.

        Args:
            event: The ``CacheInvalidated`` event carrying the stale key list.

        Edge cases:
            - An empty ``event.keys`` list is a safe no-op — ``invalidate_many``
              iterates over an empty sequence without error.
            - Keys not present in the cache are silently added to the strategy's
              invalidated set.  They accumulate without error — call
              ``strategy.reset()`` to clear them if memory is a concern.
        """
        self._strategy.invalidate_many(event.keys)
        _logger.debug(
            "CacheInvalidationConsumer: marked %d key(s) for invalidation "
            "(namespace=%r, operation=%r).",
            len(event.keys),
            event.namespace,
            event.operation,
        )

    def __repr__(self) -> str:
        return (
            f"CacheInvalidationConsumer("
            f"channel={self._channel!r}, "
            f"strategy={self._strategy!r})"
        )


__all__ = ["CacheInvalidationConsumer"]
