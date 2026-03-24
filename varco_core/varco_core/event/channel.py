"""
varco_core.event.channel
========================
Channel management protocol — separate from ``AbstractEventBus``.

``ChannelManager`` handles admin-level operations on event channels/topics:
creating, deleting, listing, and checking existence.  These operations are
intentionally separated from ``AbstractEventBus`` because:

1. **Privilege separation** — creating Kafka topics or Redis keyspaces requires
   admin/broker credentials.  Most application services only *produce* and
   *consume* events; they do not need admin access.

2. **Optional infrastructure** — not every deployment requires programmatic
   channel management.  Infrastructure-as-code (Terraform, Helm) may create
   topics externally.  Separating the concern means the bus can operate without
   any admin credentials at all.

3. **Simpler bus implementations** — ``AbstractEventBus`` can focus on publish/
   subscribe/lifecycle; admin operations move to a dedicated class with its own
   lifecycle.

Usage pattern::

    # Only wire this when admin credentials are available
    manager = KafkaChannelManager(KafkaChannelManagerSettings.from_env())
    async with manager:
        await manager.declare_channel("orders", ChannelConfig(num_partitions=6))

    # The bus itself needs no admin credentials
    bus = KafkaEventBus(KafkaEventBusSettings.from_env())
    async with bus:
        bus.subscribe(OrderEvent, handler, channel="orders")
        await bus.publish(event, channel="orders")

DESIGN: ABC over Protocol
    ✅ ``async def __aenter__`` / ``__aexit__`` default implementation lives in
       the ABC — no boilerplate in every concrete class.
    ✅ ``start()`` / ``stop()`` are abstract — implementations must manage their
       own admin client lifecycle.
    ❌ Requires inheritance rather than structural typing.  The alternative
       (Protocol) would not allow the default ``__aenter__``/``__aexit__``
       implementation.

Thread safety:  ⚠️  Implementations must document their own guarantees.
Async safety:   ✅  All public methods are ``async def``.

📚 Docs
- 🐍 https://docs.python.org/3/reference/datamodel.html#asynchronous-context-managers
  Async context managers — __aenter__ / __aexit__
- 🔍 https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.admin.AIOKafkaAdminClient
  Kafka admin client — topic management
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from varco_core.event.base import ChannelConfig


# ── ChannelManager ────────────────────────────────────────────────────────────


class ChannelManager(ABC):
    """
    Abstract base for channel/topic management operations.

    Provides create, delete, exists-check, and list operations on event
    channels.  Implementations hold their own admin client credentials
    separately from the ``AbstractEventBus``.

    Lifecycle:
        ``start()`` / ``stop()`` — connect/disconnect the admin client.
        ``async with manager`` — alternative that calls start/stop automatically.

    Thread safety:  ⚠️  Implementations must document their own guarantees.
    Async safety:   ✅  All public methods are ``async def``.

    Example (Kafka, manual lifecycle)::

        manager = KafkaChannelManager(settings)
        await manager.start()
        await manager.declare_channel("orders", ChannelConfig(num_partitions=6))
        await manager.stop()

    Example (context manager)::

        async with KafkaChannelManager(settings) as manager:
            await manager.declare_channel("events", ChannelConfig())
    """

    @abstractmethod
    async def start(self) -> None:
        """
        Connect the admin client to the backend.

        Must be called before any channel operation.  Calling ``declare_channel``,
        ``delete_channel``, ``channel_exists``, or ``list_channels`` before
        ``start()`` raises ``RuntimeError``.

        Raises:
            RuntimeError: If already started.

        Edge cases:
            - Idempotent implementations should raise on double-start to surface
              configuration mistakes early.
        """

    @abstractmethod
    async def stop(self) -> None:
        """
        Disconnect and release the admin client.

        Idempotent — safe to call multiple times.

        Edge cases:
            - Calling before ``start()`` is a no-op.
            - All pending admin operations must complete or be cancelled before
              ``stop()`` returns.
        """

    @abstractmethod
    async def declare_channel(
        self,
        channel: str,
        config: ChannelConfig | None = None,
    ) -> None:
        """
        Create the channel on the backend if it does not already exist.

        Idempotent — calling on an existing channel is a no-op.
        It does NOT modify an existing channel's configuration.

        Args:
            channel: Logical channel name (e.g. ``"orders"``).
            config:  Optional channel configuration.  ``None`` uses backend
                     defaults (Kafka: 1 partition, replication factor 1).

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Calling twice with the same channel is safe — idempotent.
            - To change an existing Kafka topic's config (partitions,
              replication), use the Kafka AdminClient directly.
        """

    @abstractmethod
    async def delete_channel(self, channel: str) -> None:
        """
        Delete the channel from the backend.

        Args:
            channel: Logical channel name to delete.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Deleting a non-existent channel is silently ignored.
            - Kafka: irreversible — all unread messages on the topic are lost.
            - Redis: Pub/Sub channels are ephemeral; this updates the local
              declaration registry only.
        """

    @abstractmethod
    async def channel_exists(self, channel: str) -> bool:
        """
        Return ``True`` if the channel exists on the backend.

        Args:
            channel: Logical channel name to check.

        Returns:
            ``True`` if the channel exists.

        Raises:
            RuntimeError: If called before ``start()``.
        """

    @abstractmethod
    async def list_channels(self) -> list[str]:
        """
        Return all channels known to the backend.

        Returns:
            Sorted list of logical channel name strings.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Kafka: returns ALL topics the admin client can see — this may
              include topics from other services.  Use ``channel_prefix``
              filtering in the implementation to narrow the result.
        """

    # ── Async context manager default implementation ──────────────────────────

    async def __aenter__(self) -> ChannelManager:
        """
        Start the admin client and return ``self``.

        Allows using the manager as an async context manager::

            async with KafkaChannelManager(settings) as manager:
                await manager.declare_channel("orders")
        """
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop the admin client on context manager exit."""
        await self.stop()

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"


__all__ = [
    "ChannelManager",
]
