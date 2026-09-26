"""
varco_nats.config
=================
Configuration for the NATS JetStream event bus backend.

``NatsEventBusSettings`` is the configuration object for ``NatsEventBus``.
It extends ``EventBusSettings`` so all NATS connection and routing settings
are read from environment variables automatically.

Environment variables (prefix ``VARCO_NATS_``)
----------------------------------------------
::

    VARCO_NATS_SERVERS=nats://nats.internal:4222
    VARCO_NATS_STREAM_NAME=orders-events
    VARCO_NATS_SUBJECT_PREFIX=orders
    VARCO_NATS_DURABLE_NAME=order-service
    VARCO_NATS_CHANNEL_PREFIX=prod.

Subject / stream model
----------------------
NATS JetStream stores messages in a **stream** that captures one or more
**subjects**.  varco channels map onto subjects under a common prefix::

    channel = "orders"
        → subject = "{subject_prefix}.{channel_prefix}{channel}"
        → e.g.   "varco.prod.orders"   (subject_prefix="varco", channel_prefix="prod.")

A single JetStream stream (``stream_name``) captures the wildcard subject
``{subject_prefix}.>`` so every channel lives in one stream.  Consumers are
durable, named after ``durable_name`` so restarts resume from the last
acknowledged message — the JetStream analogue of a Kafka consumer group.

DESIGN: Pydantic BaseSettings over frozen dataclass
    ✅ Env var reading is automatic — no ``os.environ`` boilerplate.
    ✅ Validates types at load time — invalid values fail at startup.
    ✅ Immutable after construction — prevents accidental mutation.
    ❌ ``connect_kwargs`` cannot be set from a plain env var (would need a JSON
       string).  Use keyword args or ``from_dict()``.

Thread safety:  ✅ Immutable after construction (frozen=True).
Async safety:   ✅ No mutable state.

📚 Docs
- 🔍 https://docs.nats.io/nats-concepts/jetstream — JetStream streams & consumers
- 🔍 https://docs.pydantic.dev/latest/concepts/pydantic_settings/
  Pydantic Settings — env_prefix, SettingsConfigDict
"""

from __future__ import annotations

import enum
from typing import Any

from pydantic import Field
from pydantic_settings import SettingsConfigDict
from varco_core.event.config import EventBusSettings

# ── NatsDeliverySemantics ─────────────────────────────────────────────────────


class NatsDeliverySemantics(enum.StrEnum):
    """
    Delivery guarantee level for ``NatsEventBus``.

    AT_MOST_ONCE  — The JetStream message is acknowledged **before** the local
                    handler dispatch runs.  A consumer crash after the ack but
                    before the handler runs loses the message.  Lowest
                    overhead; no duplicates, possible message loss.

    AT_LEAST_ONCE — The message is acknowledged **after** the local dispatch
                    chain completes (default).  A crash between dispatch and ack
                    causes JetStream to redeliver — handlers may see duplicates.
                    A handler that merely *raises* (no crash) also triggers
                    redelivery: the message is ``nak()``ed immediately, bounded
                    by ``max_deliver`` (Plan 019 / RT2-B — see
                    ``NatsEventBus._on_message``'s DESIGN block for the full
                    outcome table). This mirrors ``KafkaEventBus`` at-least-once
                    semantics: JetStream redelivery is the broker-level safety
                    net, while handler-level retries are the responsibility of
                    varco's ``@listen(retry_policy=..., dlq=...)`` machinery.

    EXACTLY_ONCE  — As AT_LEAST_ONCE for acknowledgement, plus producer-side
                    deduplication: every published message carries a
                    ``Nats-Msg-Id`` header set to the event's ``event_id``.
                    JetStream drops duplicates that arrive within the stream's
                    ``duplicate_window``.  This removes producer-retry
                    duplicates; consumer-crash redelivery is still bounded by
                    the dedup window.

    DESIGN: str + enum.Enum
        ✅ Values are plain strings — JSON-serialisable for config / logging.
        ✅ env var readable: ``VARCO_NATS_DELIVERY_SEMANTICS=exactly_once``.

    Trade-offs:
        AT_MOST_ONCE  → throughput ↑↑, correctness ↓
        AT_LEAST_ONCE → throughput ↑,  correctness ~ (default)
        EXACTLY_ONCE  → throughput ↓,  correctness ↑↑
    """

    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


# ── NatsEventBusSettings ──────────────────────────────────────────────────────


class NatsEventBusSettings(EventBusSettings):
    """
    Immutable configuration for ``NatsEventBus``.

    All fields are read from environment variables with the ``VARCO_NATS_``
    prefix.  Every field has a sensible default so a local NATS server with
    JetStream enabled can be used with no configuration::

        bus = NatsEventBus(NatsEventBusSettings())  # connects to localhost:4222

    Attributes:
        servers:                  Comma-separated ``nats://host:port`` URLs.
                                  Env var: ``VARCO_NATS_SERVERS``.
        stream_name:              Name of the JetStream stream that backs the
                                  bus.  All channels live in this one stream.
                                  Env var: ``VARCO_NATS_STREAM_NAME``.
        subject_prefix:           Root subject token for all channels.  The
                                  backing stream captures ``{prefix}.>``.
                                  Env var: ``VARCO_NATS_SUBJECT_PREFIX``.
        durable_name:             Base name for durable JetStream consumers.
                                  Instances sharing the same ``durable_name``
                                  resume from the same position after a restart
                                  — the JetStream analogue of a Kafka
                                  consumer group.
                                  Env var: ``VARCO_NATS_DURABLE_NAME``.
        delivery_semantics:       Delivery guarantee level.  Default
                                  ``AT_LEAST_ONCE``.
                                  Env var: ``VARCO_NATS_DELIVERY_SEMANTICS``.
        auto_create_stream:       When ``True`` (default) the bus ensures the
                                  backing stream exists on ``start()``.  Set
                                  ``False`` when the stream is provisioned by
                                  infrastructure-as-code.
                                  Env var: ``VARCO_NATS_AUTO_CREATE_STREAM``.
        ack_wait_seconds:         How long JetStream waits for a message ack
                                  before redelivering it.  Should exceed the
                                  worst-case handler dispatch time.
                                  Env var: ``VARCO_NATS_ACK_WAIT_SECONDS``.
        max_deliver:              Maximum number of delivery attempts
                                  JetStream makes for a single message before
                                  giving up.  Research 005 §B: JetStream's
                                  broker-side default is **unlimited**
                                  redelivery, so without this bound a
                                  permanently-failing handler that ``nak()``s
                                  on every attempt becomes an infinite
                                  redelivery loop (Plan 019 / RT2-B). Once
                                  ``msg.metadata.num_delivered`` reaches this
                                  value the message is ``term()``ed instead of
                                  ``nak()``ed and a WARNING is logged — the
                                  redelivery budget is exhausted, not the
                                  message dropped silently.
                                  Env var: ``VARCO_NATS_MAX_DELIVER``.
        duplicate_window_seconds: Stream dedup window for ``EXACTLY_ONCE``.
                                  Messages with a repeated ``Nats-Msg-Id``
                                  inside this window are dropped.
                                  Env var: ``VARCO_NATS_DUPLICATE_WINDOW_SECONDS``.
        connect_kwargs:           Extra kwargs forwarded to ``nats.connect()``
                                  (e.g. ``user_credentials``, ``tls``, ``name``).
                                  **Not env-readable** — use kwargs or
                                  ``from_dict()``.

    Thread safety:  ✅ Immutable — frozen=True.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``durable_name`` defaults to ``"varco-default"`` — always override in
          production to avoid cross-service interference on a shared NATS.
        - JetStream streams are NOT auto-created by the server; if
          ``auto_create_stream`` is ``False`` the stream must already exist or
          ``start()`` / ``publish()`` will fail.
        - ``channel_prefix`` (inherited) is applied *inside* ``subject_prefix``:
          the full subject is ``{subject_prefix}.{channel_prefix}{channel}``.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_NATS_", frozen=True)

    servers: str = "nats://localhost:4222"
    """Comma-separated ``nats://host:port`` URLs.  Env: ``VARCO_NATS_SERVERS``."""

    stream_name: str = "varco-events"
    """JetStream stream backing the bus.  Env: ``VARCO_NATS_STREAM_NAME``."""

    subject_prefix: str = "varco"
    """Root subject token for all channels.  Env: ``VARCO_NATS_SUBJECT_PREFIX``."""

    durable_name: str = "varco-default"
    """Base durable consumer name.  Env: ``VARCO_NATS_DURABLE_NAME``."""

    delivery_semantics: NatsDeliverySemantics = NatsDeliverySemantics.AT_LEAST_ONCE
    """Delivery guarantee level.  Env: ``VARCO_NATS_DELIVERY_SEMANTICS``."""

    auto_create_stream: bool = True
    """Ensure the backing stream exists on start().  Env: ``VARCO_NATS_AUTO_CREATE_STREAM``."""

    ack_wait_seconds: float = 30.0
    """JetStream ack-wait before redelivery.  Env: ``VARCO_NATS_ACK_WAIT_SECONDS``."""

    max_deliver: int = 5
    """Max JetStream delivery attempts before term(). Env: ``VARCO_NATS_MAX_DELIVER``."""

    duplicate_window_seconds: float = 120.0
    """Dedup window for EXACTLY_ONCE.  Env: ``VARCO_NATS_DUPLICATE_WINDOW_SECONDS``."""

    # Extra kwargs forwarded verbatim to nats.connect() — use for TLS, nkey,
    # JWT credentials, client name, etc.  Cannot be set from a plain env var.
    connect_kwargs: dict[str, Any] = Field(default_factory=dict)
    """Extra kwargs for ``nats.connect()``.  Not env-readable."""

    # ── Subject / consumer helpers ────────────────────────────────────────────

    def to_servers_list(self) -> list[str]:
        """
        Return the configured NATS server URLs as a list.

        Returns:
            A non-empty list of server URL strings — ``servers`` split on commas.

        Edge cases:
            - Whitespace around comma-separated entries is stripped.
        """
        return [part.strip() for part in self.servers.split(",") if part.strip()]

    def subject_name(self, channel: str) -> str:
        """
        Return the full JetStream subject for a logical event channel.

        The subject is ``{subject_prefix}.{channel_prefix}{channel}`` so every
        channel lives under the ``{subject_prefix}.>`` wildcard the backing
        stream captures.

        Args:
            channel: The logical event channel name (e.g. ``"orders"``).

        Returns:
            The full NATS subject (e.g. ``"varco.prod.orders"``).

        Edge cases:
            - Empty ``channel_prefix`` (default) → ``{subject_prefix}.{channel}``.
            - ``channel`` must not contain NATS wildcard characters
              (``*``, ``>``) — those are not validated here and would broaden
              the subject unexpectedly.
        """
        return f"{self.subject_prefix}.{self.channel_prefix}{channel}"

    def wildcard_subject(self) -> str:
        """
        Return the wildcard subject the backing stream captures.

        Returns:
            ``"{subject_prefix}.>"`` — matches every channel subject.
        """
        # ``>`` is the NATS multi-token wildcard — it matches one or more tokens
        # after the prefix, so every channel subject is covered.
        return f"{self.subject_prefix}.>"

    def channel_from_subject(self, subject: str) -> str:
        """
        Recover the logical channel name from a full JetStream subject.

        Inverse of ``subject_name()`` — strips ``subject_prefix`` and
        ``channel_prefix``.

        Args:
            subject: The NATS subject a message arrived on.

        Returns:
            The logical channel name.

        Edge cases:
            - A subject that does not start with ``{subject_prefix}.`` is
              returned unchanged after the prefix-strip attempts — callers
              should not normally see such subjects.
        """
        return subject.removeprefix(f"{self.subject_prefix}.").removeprefix(self.channel_prefix)

    def durable_for(self, channel: str) -> str:
        """
        Return the durable JetStream consumer name for a channel.

        Each channel-specific subscription needs its own durable consumer —
        JetStream durables are 1:1 with a consumer cursor.

        Args:
            channel: The logical event channel name.

        Returns:
            A durable name derived from ``durable_name`` and ``channel``.

        Edge cases:
            - ``.``, ``*``, ``>`` and spaces are not valid in NATS durable
              names; they are replaced with ``_`` so any channel name is safe.
        """
        # NATS durable/consumer names disallow the subject token separators.
        safe_channel = (
            channel.replace(".", "_").replace("*", "_").replace(">", "_").replace(" ", "_")
        )
        return f"{self.durable_name}-{safe_channel}"


# Plan 048 / DI-6, Step 10 — the `nats_event_bus_settings` @Fallback
# @Provider that used to live here is gone. `NatsEventBusSettings` is now
# registered eagerly, fail-fast, by `bind_settings()` inside
# `varco_nats.di.bootstrap()` (before `scan()`).


__all__ = [
    "NatsDeliverySemantics",
    "NatsEventBusSettings",
]
