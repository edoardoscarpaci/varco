"""
varco_redis.channel
====================
Redis-backed ``ChannelManager`` implementation.

``RedisChannelManager`` provides a ``ChannelManager`` interface for Redis Pub/Sub.
Redis Pub/Sub channels are ephemeral — they exist only while subscribers are
connected and cannot be "created" or "deleted" at the broker level.

For this reason ``RedisChannelManager`` maintains a **local registry** of declared
channels and mirrors the ``ChannelManager`` contract.  This is useful for:

- Bootstrap verification: ensuring expected channels are declared before services start.
- Introspection: listing channels known to a specific deployment.
- Consistency: providing the same API across all backends regardless of Redis's
  ephemeral channel model.

DESIGN: local registry over real broker queries
    ✅ Redis PUBSUB CHANNELS works but returns only ACTIVE channels (ones with
       live subscribers).  A declared-but-idle channel would not appear.
    ✅ No admin credentials needed — Redis Pub/Sub has no ACL for channel creation.
    ✅ Symmetric with KafkaChannelManager — same interface, different behaviour.
    ❌ Local state is lost on restart — ``declare_channel`` calls are not persisted.
       If persistence is needed, store them in a Redis SET (future enhancement).

Thread safety:  ❌  Not thread-safe.  Use from a single event loop.
Async safety:   ✅  All public methods are ``async def``.
"""

from __future__ import annotations

import logging
import sys

from providify import Inject, PostConstruct, PreDestroy, Singleton

from varco_core.event.base import ChannelConfig
from varco_core.event.channel import ChannelManager
from varco_redis.config import RedisEventBusSettings

_logger = logging.getLogger(__name__)


# ── RedisChannelManagerSettings ───────────────────────────────────────────────

# Redis Pub/Sub requires no admin credentials — the same connection settings
# as the bus are sufficient.  We reuse RedisEventBusSettings directly rather
# than introducing a new settings class that would be identical.
#
# DESIGN: alias over new class
#   ✅ No code duplication — same fields, same env prefix.
#   ✅ Users can pass the same settings object to both bus and manager.
#   ❌ The alias is less discoverable than a distinct class name.
#      Documented here so users know the manager and bus share settings.
RedisChannelManagerSettings = RedisEventBusSettings


# ── RedisChannelManager ───────────────────────────────────────────────────────


@Singleton(priority=-sys.maxsize, qualifier="redis")
class RedisChannelManager(ChannelManager):
    """
    Redis Pub/Sub channel management via a local registry.

    Implements ``ChannelManager`` for Redis.  Because Redis Pub/Sub channels
    are ephemeral, this class maintains a local in-memory registry of declared
    channels.  No admin credentials are required.

    Lifecycle:
        start / stop are no-ops for Redis (no persistent admin connection
        needed) but are implemented for ``ChannelManager`` protocol compliance::

            async with RedisChannelManager(settings) as manager:
                await manager.declare_channel("orders")
                channels = await manager.list_channels()

    Args:
        settings: Redis connection settings.  Defaults to
                  ``RedisEventBusSettings()`` (reads from ``VARCO_REDIS_*`` env).

    Thread safety:  ❌  Not thread-safe — the local registry is a plain dict.
    Async safety:   ✅  All public methods are ``async def``.

    Edge cases:
        - ``declare_channel`` does not create anything on the Redis broker —
          channels appear automatically when the first subscriber connects.
        - ``delete_channel`` removes the channel from the local registry only;
          it cannot force active subscribers to disconnect.
        - ``channel_exists`` and ``list_channels`` reflect the local registry,
          NOT the actual live Redis pub/sub state.
        - Local registry is not persisted — it resets on object construction.
    """

    def __init__(self, settings: Inject[RedisEventBusSettings]) -> None:
        """
        Args:
            settings: Redis connection settings injected from the container.
        """
        self._settings = settings

        # Local registry: logical channel name → optional ChannelConfig.
        # Redis Pub/Sub has no server-side channel registry; we track locally.
        self._registry: dict[str, ChannelConfig | None] = {}

        # start/stop are no-ops for Redis but we track started state for
        # consistent RuntimeError on pre-start calls (protocol compliance).
        self._started = False

    # ── ChannelManager implementation ─────────────────────────────────────────

    @PostConstruct
    async def start(self) -> None:
        """
        Mark the manager as started.

        Redis Pub/Sub requires no persistent admin connection — this method
        exists for ``ChannelManager`` protocol compliance and is otherwise
        a no-op.

        Raises:
            RuntimeError: If already started.
        """
        if self._started:
            raise RuntimeError(
                "RedisChannelManager.start() called on an already-started manager. "
                "Call stop() first."
            )
        self._started = True
        _logger.debug("RedisChannelManager started (url=%s)", self._settings.url)

    @PreDestroy
    async def stop(self) -> None:
        """
        Mark the manager as stopped.  Idempotent — safe to call multiple times.
        """
        self._started = False
        _logger.debug("RedisChannelManager stopped.")

    async def declare_channel(
        self,
        channel: str,
        config: ChannelConfig | None = None,
    ) -> None:
        """
        Register ``channel`` in the local registry.

        Redis Pub/Sub channels are ephemeral — no server-side creation occurs.
        This records the channel so ``list_channels()`` and ``channel_exists()``
        reflect it.

        Args:
            channel: Logical channel name (e.g. ``"orders"``).
            config:  Optional channel configuration.  Stored locally for
                     introspection; not applied to Redis itself.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Calling with a new ``config`` on an already-declared channel
              updates the stored config.
            - Calling with ``config=None`` on an existing channel leaves the
              existing config unchanged.
        """
        self._require_started()
        # Preserve existing config if re-declaring with config=None.
        if channel not in self._registry or config is not None:
            self._registry[channel] = config
        _logger.debug("Declared Redis channel %r (config=%r)", channel, config)

    async def delete_channel(self, channel: str) -> None:
        """
        Remove ``channel`` from the local registry.

        Does NOT disconnect active subscribers on Redis — they will continue
        to receive messages until they disconnect themselves.

        Args:
            channel: Logical channel name to remove.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - If ``channel`` was never declared, the call is a silent no-op.
        """
        self._require_started()
        self._registry.pop(channel, None)
        _logger.debug("Removed Redis channel %r from local registry.", channel)

    async def channel_exists(self, channel: str) -> bool:
        """
        Return ``True`` if ``channel`` is in the local registry.

        Does NOT query the Redis broker for active subscribers — reflects only
        what was declared via ``declare_channel()``.

        Args:
            channel: Logical channel name to check.

        Returns:
            ``True`` if the channel was declared.

        Raises:
            RuntimeError: If called before ``start()``.
        """
        self._require_started()
        return channel in self._registry

    async def list_channels(self) -> list[str]:
        """
        Return a sorted list of all declared channel names.

        Returns only channels registered via ``declare_channel()``.  Active but
        undeclared channels (subscribers connected without going through this
        manager) are NOT included.

        Returns:
            Sorted list of logical channel names.

        Raises:
            RuntimeError: If called before ``start()``.
        """
        self._require_started()
        return sorted(self._registry.keys())

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _require_started(self) -> None:
        """
        Raise ``RuntimeError`` if the manager has not been started.

        Raises:
            RuntimeError: If ``start()`` has not been called.
        """
        if not self._started:
            raise RuntimeError(
                f"{type(self).__name__} is not started. "
                f"Call 'await manager.start()' or use 'async with manager' first."
            )

    def __repr__(self) -> str:
        return (
            f"RedisChannelManager("
            f"url={self._settings.url!r}, "
            f"channels={len(self._registry)}, "
            f"started={self._started})"
        )


__all__ = [
    "RedisChannelManager",
    "RedisChannelManagerSettings",
]
