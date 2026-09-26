"""
varco_nats.channel
==================
NATS JetStream-backed ``ChannelManager`` implementation.

``NatsStreamManager`` administers the JetStream **stream** that backs the event
bus — creating it, purging per-channel messages, checking which channels carry
data, and listing them.  It is intentionally separate from ``NatsEventBus``
because stream administration may require broader credentials than a service
that only publishes and consumes events.

NATS channel model
------------------
Unlike Kafka — where each channel is a distinct topic — NATS channels are
**subjects under a single stream's wildcard**.  One JetStream stream
(``stream_name``) captures ``{subject_prefix}.>`` so every channel lives in it.

Consequences for this manager:

- ``declare_channel`` ensures the *backing stream* exists **and** records the
  channel in a process-local declaration registry (Plan 019 / RT2-C-contract
  — see ``NatsStreamManager``'s DESIGN block).
- ``channel_exists`` reports ``True`` if the backing stream exists AND the
  channel is either declared (registry) or currently carries a message
  (broker evidence) — the declared-or-present contract
  ``varco_core.event.channel.ChannelManager`` documents.
- ``list_channels`` enumerates the union of declared channels and subjects
  that currently carry messages.
- ``delete_channel`` purges a channel's messages from the stream **and**
  discards it from the registry.
- ``channel_has_messages`` (NATS-only, not on the ABC) preserves the old
  "subject currently carries a message" predicate under an honest name, for
  operational introspection.

Configuration
-------------
``NatsChannelManagerSettings`` uses the ``VARCO_NATS_ADMIN_`` prefix so admin
settings stay separate from the bus's ``VARCO_NATS_`` namespace::

    bus_config = NatsEventBusSettings(
        servers="nats://nats.internal:4222",
        durable_name="my-service",
    )
    manager_config = NatsChannelManagerSettings(
        servers="nats://nats.internal:4222",
        stream_name="varco-events",   # must match the bus
    )

DESIGN: separate from NatsEventBus
    ✅ Admin credentials never bleed into the bus.
    ✅ Services that only produce/consume events have no admin dependency.
    ✅ Infrastructure / migration scripts can use NatsStreamManager standalone.
    ❌ Two objects instead of one — justified by the privilege separation.

Thread safety:  ❌  Not thread-safe.  Use from a single event loop.
Async safety:   ✅  All public methods are ``async def``.

📚 Docs
- 🔍 https://docs.nats.io/nats-concepts/jetstream/streams — JetStream streams
- 🔍 https://nats-io.github.io/nats.py/ — nats-py JetStreamManager API
"""

from __future__ import annotations

import logging
from typing import Any

from nats import connect
from nats.js.errors import NotFoundError
from providify import Fallback, Inject, PostConstruct, PreDestroy, Singleton
from pydantic import Field
from pydantic_settings import SettingsConfigDict
from varco_core.config import VarcoSettings
from varco_core.di import BACKEND_PRIORITY
from varco_core.event.base import ChannelConfig
from varco_core.event.channel import ChannelManager

_logger = logging.getLogger(__name__)


# ── NatsChannelManagerSettings ────────────────────────────────────────────────


class NatsChannelManagerSettings(VarcoSettings):
    """
    Configuration for ``NatsStreamManager``.

    Uses the ``VARCO_NATS_ADMIN_`` prefix to separate admin settings from the
    bus's ``VARCO_NATS_`` namespace.

    Attributes:
        servers:                  Comma-separated ``nats://host:port`` URLs the
                                  admin client connects to.
                                  Env var: ``VARCO_NATS_ADMIN_SERVERS``.
        stream_name:              Name of the JetStream stream to administer.
                                  **Must match** the bus's ``stream_name``.
                                  Env var: ``VARCO_NATS_ADMIN_STREAM_NAME``.
        subject_prefix:           Root subject token — **must match** the bus's
                                  ``subject_prefix``.
                                  Env var: ``VARCO_NATS_ADMIN_SUBJECT_PREFIX``.
        channel_prefix:           Channel-name prefix — **must match** the bus's
                                  ``channel_prefix``.
                                  Env var: ``VARCO_NATS_ADMIN_CHANNEL_PREFIX``.
        duplicate_window_seconds: Dedup window used when this manager creates
                                  the stream.
                                  Env var: ``VARCO_NATS_ADMIN_DUPLICATE_WINDOW_SECONDS``.
        connect_kwargs:           Extra kwargs forwarded to ``nats.connect()``.
                                  **Not env-readable** — use kwargs / ``from_dict()``.

    Thread safety:  ✅ Immutable — frozen=True.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``stream_name``, ``subject_prefix`` and ``channel_prefix`` must match
          the bus's values exactly, or the manager administers a different
          stream / subjects than the bus uses.
    """

    model_config = SettingsConfigDict(
        env_prefix="VARCO_NATS_ADMIN_",
        frozen=True,
    )

    servers: str = "nats://localhost:4222"
    """Admin client server URLs.  Env: ``VARCO_NATS_ADMIN_SERVERS``."""

    stream_name: str = "varco-events"
    """JetStream stream to administer.  Env: ``VARCO_NATS_ADMIN_STREAM_NAME``."""

    subject_prefix: str = "varco"
    """Root subject token.  Must match the bus.  Env: ``VARCO_NATS_ADMIN_SUBJECT_PREFIX``."""

    channel_prefix: str = ""
    """Channel-name prefix.  Must match the bus.  Env: ``VARCO_NATS_ADMIN_CHANNEL_PREFIX``."""

    duplicate_window_seconds: float = 120.0
    """Dedup window used at stream creation.  Env: ``VARCO_NATS_ADMIN_DUPLICATE_WINDOW_SECONDS``."""

    connect_kwargs: dict[str, Any] = Field(default_factory=dict)
    """Extra kwargs for ``nats.connect()``.  Not env-readable."""

    def to_servers_list(self) -> list[str]:
        """Return the configured NATS server URLs as a list (split on commas)."""
        return [part.strip() for part in self.servers.split(",") if part.strip()]

    def subject_name(self, channel: str) -> str:
        """
        Return the full JetStream subject for a logical channel.

        Args:
            channel: Logical event channel name.

        Returns:
            ``{subject_prefix}.{channel_prefix}{channel}``.
        """
        return f"{self.subject_prefix}.{self.channel_prefix}{channel}"

    def wildcard_subject(self) -> str:
        """Return ``{subject_prefix}.>`` — the subject the backing stream captures."""
        return f"{self.subject_prefix}.>"

    def channel_from_subject(self, subject: str) -> str:
        """Recover the logical channel name from a full subject (inverse of subject_name)."""
        return subject.removeprefix(f"{self.subject_prefix}.").removeprefix(self.channel_prefix)


# Plan 048 / DI-6, Step 10 — the `nats_channel_manager_settings` @Fallback
# @Provider that used to live here is gone. `NatsChannelManagerSettings`
# is now registered eagerly, fail-fast, by `bind_settings()` inside
# `varco_nats.di.bootstrap()` (before `scan()`).


# ── NatsStreamManager ─────────────────────────────────────────────────────────


@Fallback
@Singleton(priority=BACKEND_PRIORITY, qualifier="nats")
class NatsStreamManager(ChannelManager):
    """
    JetStream stream administration via the nats-py JetStream management API.

    Implements ``ChannelManager`` for NATS.  Because NATS channels are subjects
    under one stream's wildcard, the channel-keyed operations administer that
    single backing stream:

    - ``declare_channel`` → ensure the backing stream exists AND record the
      channel in the local declaration registry.
    - ``channel_exists``  → stream exists AND (declared OR carries a message).
    - ``list_channels``   → sorted union of declared channels and subjects
      that currently carry messages.
    - ``delete_channel``  → purge the channel's messages from the stream AND
      discard it from the registry.
    - ``channel_has_messages`` → NATS-only affordance preserving the original
      "subject currently carries a message" predicate.

    DESIGN: declaration registry + broker evidence (Plan 019 / RT2-C-contract)
        The ABC's ``channel_exists`` docstring requires the round-trip
        ``declare_channel(c)`` ⟹ ``channel_exists(c)`` is ``True`` until
        ``delete_channel(c)``. NATS has no per-channel broker object (every
        channel is a subject under one stream's wildcard), so — exactly like
        Redis's documented Pub/Sub registry (``varco_core.event.channel``'s
        own ``delete_channel`` Edge cases block already blesses this shape)
        — this manager tracks declared channels itself.
        ✅ Satisfies the ABC round-trip that a pure "has messages" predicate
           could not (the RT2-C bug this fixes).
        ✅ The ``OR carries messages`` half keeps today's operational value:
           a channel declared by *another* process becomes discoverable once
           it carries data, so ``list_channels()`` still reflects reality
           beyond this instance's own declarations.
        ❌ The registry is per-instance and process-local — a fresh manager
           in another pod reports ``False`` for a channel declared elsewhere
           that has never carried a message. Identical, and equally
           documented, limitation to Redis's ``ChannelManager``.
        ❌ Rejected — one JetStream stream per channel (a real broker object
           per channel, research 005 §E's suggested shape): a topology and
           wire-format change. ``stream_name`` is one shared stream with a
           ``{prefix}.>`` wildcard that the bus, the DLQ, and every existing
           deployment already share; splitting it per-channel changes
           retention/replica/dedup-window management from one object to N
           and breaks every existing stream. Rejected for this fix.

    Lifecycle:
        Must be started before any operation::

            manager = NatsStreamManager(settings)
            await manager.start()
            await manager.declare_channel("orders")
            await manager.stop()

        Or as an async context manager (preferred)::

            async with NatsStreamManager(settings) as manager:
                await manager.declare_channel("orders")

    Args:
        settings: Admin configuration injected from the container.

    Thread safety:  ❌  Not thread-safe.
    Async safety:   ✅  All public methods are ``async def``.

    Edge cases:
        - ``declare_channel`` is idempotent — an existing stream is left as-is,
          and re-declaring an already-declared channel is a no-op.
        - ``channel_exists`` returns ``True`` immediately after
          ``declare_channel``, even with zero messages published — the
          declared-or-present contract, not "carries data".
        - A channel this manager never declared, but that carries a message
          (published by another process, or before this manager started),
          still reports ``True`` — broker evidence supplements the registry.
        - ``stream_name`` / ``subject_prefix`` / ``channel_prefix`` MUST match
          the bus's values or the manager administers the wrong stream.
    """

    def __init__(self, settings: Inject[NatsChannelManagerSettings]) -> None:
        """
        Args:
            settings: Admin configuration injected from the container.
        """
        self._settings = settings
        # NATS client + JetStream context are created in start() — nats-py
        # objects must be created inside a running event loop.
        self._nc: Any | None = None
        self._js: Any | None = None
        # Process-local declaration registry (Plan 019 / RT2-C-contract) — the
        # value is unused today but kept as ChannelConfig | None so a future
        # need to recall "what config was this channel declared with" has
        # somewhere to live without a second dict.
        self._declared: dict[str, ChannelConfig | None] = {}

    # ── ChannelManager implementation ─────────────────────────────────────────

    @PostConstruct
    async def start(self) -> None:
        """
        Connect the admin NATS client and JetStream context.

        Raises:
            RuntimeError:   If already started.
            NoServersError: (nats-py) If the configured servers are unreachable.
        """
        if self._nc is not None:
            raise RuntimeError(
                "NatsStreamManager.start() called on an already-started manager. Call stop() first."
            )
        self._nc = await connect(
            servers=self._settings.to_servers_list(),
            **self._settings.connect_kwargs,
        )
        # jetstream() is synchronous — it wraps the client with the JetStream
        # management + publish API surface.
        self._js = self._nc.jetstream()
        _logger.info(
            "NatsStreamManager started (servers=%s, stream=%s)",
            self._settings.servers,
            self._settings.stream_name,
        )

    @PreDestroy
    async def stop(self) -> None:
        """Close the admin connection.  Idempotent — safe to call multiple times."""
        if self._nc is None:
            return
        await self._nc.close()
        self._nc = None
        self._js = None
        _logger.info("NatsStreamManager stopped.")

    async def declare_channel(
        self,
        channel: str,
        config: ChannelConfig | None = None,
    ) -> None:
        """
        Ensure the backing JetStream stream exists, and record ``channel`` in
        the local declaration registry.

        Because every channel is a subject under the stream's ``{prefix}.>``
        wildcard, declaring *any* channel ensures the *stream* — there is no
        per-channel broker object in NATS. This method additionally records
        ``channel`` in ``self._declared`` (Plan 019 / RT2-C-contract) so
        ``channel_exists``/``list_channels`` can satisfy the ABC's
        declared-or-present contract without a per-channel broker object.

        Args:
            channel: Logical channel name — recorded in the declaration
                     registry (no longer "logging only").
            config:  Optional channel configuration.  Only
                     ``replication_factor`` is honoured — it maps to the
                     JetStream stream's ``num_replicas``.  ``num_partitions``
                     has no NATS equivalent and is ignored.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Idempotent — if the stream already exists it is left untouched
              (its replica count is NOT changed to match ``config``), and
              re-declaring an already-declared channel is a no-op.
            - If another stream already captures the wildcard subject, the
              underlying ``add_stream`` raises — overlapping subjects are a
              configuration error and must surface.
        """
        self._require_started()
        cfg = config or ChannelConfig()
        stream = self._settings.stream_name

        # Probe first so an existing stream is left as-is (idempotent).
        try:
            await self._js.stream_info(stream)  # type: ignore[union-attr]
            _logger.debug(
                "JetStream stream %r already exists — declare_channel(%r) is a no-op.",
                stream,
                channel,
            )
            self._declared[channel] = config
            return
        except NotFoundError:
            # Stream is absent — create it below.
            pass

        await self._js.add_stream(  # type: ignore[union-attr]
            name=stream,
            subjects=[self._settings.wildcard_subject()],
            num_replicas=cfg.replication_factor,
            duplicate_window=self._settings.duplicate_window_seconds,
        )
        self._declared[channel] = config
        _logger.info(
            "Created JetStream stream %r (subjects=[%s], replicas=%d) while declaring channel %r",
            stream,
            self._settings.wildcard_subject(),
            cfg.replication_factor,
            channel,
        )

    async def delete_channel(self, channel: str) -> None:
        """
        Purge all messages for ``channel`` from the backing stream, and
        discard it from the declaration registry.

        NATS has no per-channel object to delete — instead this purges every
        message stored under the channel's subject.  The stream itself and
        other channels are unaffected.

        Args:
            channel: Logical channel name whose messages should be purged.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Irreversible — purged messages are permanently gone.
            - Purging a channel that never received a message is a no-op
              (``purge_stream`` simply removes zero messages).
            - Discarding a channel that was never declared by this manager
              instance is a no-op on the registry (``dict.pop(..., None)``).
            - To delete the whole stream, use the nats-py
              ``JetStreamManager.delete_stream`` API directly.
        """
        self._require_started()
        subject = self._settings.subject_name(channel)
        # purge_stream with a subject filter removes only that subject's messages.
        await self._js.purge_stream(  # type: ignore[union-attr]
            self._settings.stream_name,
            subject=subject,
        )
        self._declared.pop(channel, None)
        _logger.info(
            "Purged channel %r (subject=%r) from stream %r",
            channel,
            subject,
            self._settings.stream_name,
        )

    async def channel_exists(self, channel: str) -> bool:
        """
        Return ``True`` if the channel is declared-or-present: the backing
        stream exists AND (the channel was declared by this manager OR its
        subject currently carries at least one message).

        Satisfies the ABC's round-trip contract
        (``varco_core.event.channel.ChannelManager.channel_exists``):
        ``declare_channel(c)`` ⟹ ``channel_exists(c)`` is ``True`` until
        ``delete_channel(c)`` — even with zero messages published.

        Args:
            channel: Logical channel name to check.

        Returns:
            ``True`` if the stream exists and the channel is declared or
            carries a message.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Returns ``False`` (not an error) if the backing stream itself
              does not exist yet.
            - A channel declared by *another* process (not this manager
              instance) reports ``False`` unless it also carries a message —
              the registry is process-local. See the class DESIGN block.
            - For the old "carries a message" predicate alone, use
              ``channel_has_messages()``.
        """
        self._require_started()
        if channel in self._declared:
            # Still confirm the backing stream itself exists — a channel
            # declared before the stream was later deleted out-of-band must
            # not report True.
            try:
                await self._js.stream_info(self._settings.stream_name)  # type: ignore[union-attr]
            except NotFoundError:
                return False
            return True
        return await self.channel_has_messages(channel)

    async def channel_has_messages(self, channel: str) -> bool:
        """
        Return ``True`` if the channel's subject currently carries any
        message — the predicate ``channel_exists`` implemented before Plan
        019 / RT2-C-contract, preserved here under an honest name.

        NATS-only affordance, not part of the ``ChannelManager`` ABC —
        Kafka/Redis brokers cannot answer this question the same way.

        Args:
            channel: Logical channel name to check.

        Returns:
            ``True`` if the channel's subject has at least one message in the
            backing stream.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Returns ``False`` for a channel that has never received a
              message, or if the backing stream does not exist yet.
        """
        self._require_started()
        subject = self._settings.subject_name(channel)
        try:
            # subjects_filter makes JetStream populate state.subjects — a dict
            # of {subject: message_count} restricted to the filter.
            info = await self._js.stream_info(  # type: ignore[union-attr]
                self._settings.stream_name,
                subjects_filter=subject,
            )
        except NotFoundError:
            # No backing stream → no channel carries data.
            return False

        subjects = getattr(info.state, "subjects", None) or {}
        has_messages: bool = subjects.get(subject, 0) > 0
        return has_messages

    async def list_channels(self) -> list[str]:
        """
        Return the sorted union of declared channels and channels that
        currently carry messages.

        Combines the local declaration registry with JetStream's per-subject
        message counts — every declared channel is listed even with zero
        messages, and every channel carrying data is listed even if this
        manager instance never declared it (broker evidence).

        Returns:
            Sorted list of logical channel names, deduplicated.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Returns just the declared set (not an error) if the backing
              stream does not exist yet — ``channel_has_messages``'s
              stream-absent branch contributes an empty set in that case.
        """
        self._require_started()
        try:
            info = await self._js.stream_info(  # type: ignore[union-attr]
                self._settings.stream_name,
                subjects_filter=self._settings.wildcard_subject(),
            )
        except NotFoundError:
            return sorted(self._declared)

        subjects = getattr(info.state, "subjects", None) or {}
        # Map each subject carrying messages back to its logical channel name.
        carrying_messages = {
            self._settings.channel_from_subject(subject)
            for subject, count in subjects.items()
            if count > 0
        }
        return sorted(set(self._declared) | carrying_messages)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _require_started(self) -> None:
        """
        Raise ``RuntimeError`` if the admin client has not been started.

        Raises:
            RuntimeError: If ``start()`` has not been called.
        """
        if self._js is None:
            raise RuntimeError(
                f"{type(self).__name__} is not started. "
                f"Call 'await manager.start()' or use 'async with manager' first."
            )

    def __repr__(self) -> str:
        started = self._nc is not None
        return (
            f"NatsStreamManager("
            f"servers={self._settings.servers!r}, "
            f"stream={self._settings.stream_name!r}, "
            f"started={started})"
        )


__all__ = [
    "NatsChannelManagerSettings",
    "NatsStreamManager",
]
