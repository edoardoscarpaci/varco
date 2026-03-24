"""
varco_kafka.dlq
===============
Kafka-backed implementation of ``AbstractDeadLetterQueue``.

``KafkaDLQ`` publishes failed event entries to a dedicated Kafka DLQ topic and
supports replay/monitoring via a separate consumer group with manual offset commits.

Architecture
------------
::

    Handler exhausts retries
        ↓
    dlq.push(DeadLetterEntry)
        → AIOKafkaProducer.send_and_wait(topic="__dlq__", value=JSON bytes)

    DLQ relay / monitoring
        → AIOKafkaConsumer.getmany(timeout_ms=...) [group: __varco_dlq_relay__]
        → pop_batch() returns DeadLetterEntry list
        → relay processes each entry
        → ack(entry_id) commits offset for that entry's partition

DESIGN: dedicated DLQ topic over Kafka Streams / external DB
    ✅ Kafka retains messages with configurable retention — no data loss on relay restart.
    ✅ Multiple relay / monitoring services can consume from the DLQ topic independently
       by using different consumer group IDs.
    ✅ Reuses existing Kafka infrastructure — no extra system to operate.
    ✅ push() is fire-and-forget (send_and_wait returns after broker ack) — low overhead.
    ❌ count() cannot be implemented without a Kafka AdminClient — returns ``-1``
       (documented limitation; use Kafka consumer lag metrics for monitoring).
    ❌ ack() commits offsets up to the acked message — if entries are acked out-of-order,
       lower offsets are implicitly acked (standard Kafka behaviour).  For sequential
       relay processing this is always correct.

Topic naming
------------
Default DLQ topic: ``{topic_prefix}__dlq__``

Override via ``dlq_topic`` constructor argument or
``KafkaDLQSettings.dlq_topic``.

Offset commits
--------------
``pop_batch()`` reads messages with ``enable_auto_commit=False`` — no offsets are
committed until ``ack()`` is called.  On each ``ack()``, the offset+1 for the
message's partition is committed via the consumer.

IMPORTANT: ``ack()`` commits offsets up to and including the acked message in its
partition.  If entries from multiple partitions are interleaved in a batch, acking
entry B (partition 1, offset 5) does not affect entry A (partition 0, offset 2).
However, acking entry C (partition 1, offset 7) before entry B (partition 1,
offset 5) commits through offset 7, implicitly acking B.  This is standard Kafka
behaviour — design the relay to ack sequentially per batch.

Usage (push-only, most common)::

    dlq = KafkaDLQ(settings=KafkaEventBusSettings(bootstrap_servers="..."))
    await dlq.start()

    # Wire into @listen — called automatically on retry exhaustion:
    class OrderConsumer(EventConsumer):
        @listen(
            OrderPlacedEvent,
            retry_policy=RetryPolicy(max_attempts=3),
            dlq=dlq,
        )
        async def on_order_placed(self, event: OrderPlacedEvent) -> None:
            ...

    await dlq.stop()

Usage (with relay consumer)::

    async with KafkaDLQ(settings=...) as dlq:
        entries = await dlq.pop_batch(limit=10)
        for entry in entries:
            await alert_ops(entry)
            await dlq.ack(entry.entry_id)

DI integration via ``KafkaDLQConfiguration``::

    from varco_kafka.di import KafkaDLQConfiguration
    from varco_core.event.dlq import AbstractDeadLetterQueue

    container = DIContainer()
    await container.ainstall(KafkaDLQConfiguration)
    dlq = await container.aget(AbstractDeadLetterQueue)

Thread safety:  ❌ Not thread-safe.  Use from a single event loop.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🔍 https://aiokafka.readthedocs.io/en/stable/producer.html
  AIOKafkaProducer — send_and_wait usage
- 🔍 https://aiokafka.readthedocs.io/en/stable/consumer.html
  AIOKafkaConsumer — getmany, seek, commit usage
- 📐 https://www.confluent.io/blog/kafka-consumer-multi-threaded-messaging/
  Kafka consumer patterns — manual offset commit rationale
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

from providify import Configuration, Inject, Provider

from varco_core.event.dlq import AbstractDeadLetterQueue, DeadLetterEntry
from varco_core.event.serializer import JsonEventSerializer
from varco_kafka.config import KafkaEventBusSettings

_logger = logging.getLogger(__name__)

# Default consumer group for the DLQ relay.  Override via dlq_consumer_group arg.
# Double underscores mark this as an internal varco consumer group.
_DEFAULT_DLQ_GROUP = "__varco_dlq_relay__"

# Default DLQ topic suffix.  Full name = topic_prefix + this.
_DEFAULT_DLQ_TOPIC_SUFFIX = "__dlq__"

# How long to wait for Kafka messages in pop_batch() before returning an empty list.
# Short enough to be responsive; long enough not to hammer Kafka with empty polls.
_CONSUMER_POLL_TIMEOUT_MS = 500


# ── KafkaDLQ ──────────────────────────────────────────────────────────────────


class KafkaDLQ(AbstractDeadLetterQueue):
    """
    Kafka-backed ``AbstractDeadLetterQueue`` using a dedicated DLQ topic.

    **Push path** (primary use case): failed event entries are serialized to JSON
    and produced to ``{topic_prefix}__dlq__`` via ``AIOKafkaProducer``.

    **Pop/ack path** (relay use case): a dedicated consumer group
    (``__varco_dlq_relay__``) reads from the DLQ topic with manual offset commits.
    ``ack()`` commits the offset for each acknowledged entry.

    Args:
        settings:          Kafka connection settings.  Defaults to
                           ``KafkaEventBusSettings.from_env()``.
        dlq_topic:         Full DLQ topic name.  Defaults to
                           ``{topic_prefix}__dlq__``.
        dlq_consumer_group: Consumer group for pop/ack.  Defaults to
                            ``__varco_dlq_relay__``.  Use a custom group if
                            you want multiple independent relay consumers.

    Lifecycle:
        Call ``await dlq.start()`` before use.  Call ``await dlq.stop()``
        when done.  Or use as an async context manager.

    Thread safety:  ❌ Not thread-safe.  Use from a single event loop.
    Async safety:   ✅ All methods are ``async def``.

    Edge cases:
        - ``push()`` NEVER raises — all exceptions are swallowed and logged.
        - ``pop_batch()`` polls Kafka with a 500 ms timeout.  Returns an empty
          list if no messages arrive within the timeout.
        - ``ack()`` commits offsets per partition.  Acking entry C before entry B
          (same partition) implicitly acks B — design the relay to ack in batch order.
        - ``count()`` returns ``-1`` — Kafka consumer lag requires an AdminClient,
          which is not included in this class.  Use Kafka monitoring tools instead.
        - The DLQ topic must exist before ``pop_batch()`` is called.  Either
          create it manually or use ``KafkaChannelManager.declare_channel()``.
        - If the producer is started but the DLQ topic doesn't exist, Kafka
          auto-creates it (if auto.create.topics.enable=true on the broker)
          with default settings.  In production, pre-create the topic with
          appropriate retention settings (high retention for DLQ).

    Example::

        async with KafkaDLQ(settings=KafkaEventBusSettings()) as dlq:
            # Push (usually called automatically via @listen(dlq=dlq)):
            await dlq.push(DeadLetterEntry.from_failure(...))

            # Relay:
            entries = await dlq.pop_batch(limit=10)
            for entry in entries:
                print(f"DLQ: {entry.handler_name} failed — {entry.error_message}")
                await dlq.ack(entry.entry_id)
    """

    def __init__(
        self,
        settings: KafkaEventBusSettings | None = None,
        *,
        dlq_topic: str | None = None,
        dlq_consumer_group: str = _DEFAULT_DLQ_GROUP,
    ) -> None:
        """
        Args:
            settings:           Kafka connection settings.
            dlq_topic:          Override for the DLQ topic name.  If None,
                                defaults to ``{topic_prefix}__dlq__``.
            dlq_consumer_group: Consumer group ID for pop/ack operations.
                                Override when running multiple relay instances
                                that need independent positions in the DLQ.

        Edge cases:
            - If ``dlq_topic`` is provided, ``topic_prefix`` is NOT prepended —
              the topic name is used verbatim.
        """
        self._settings = settings or KafkaEventBusSettings.from_env()

        # DLQ topic name — default uses topic_prefix for namespace isolation.
        self._dlq_topic = (
            dlq_topic
            if dlq_topic is not None
            else f"{self._settings.channel_prefix}{_DEFAULT_DLQ_TOPIC_SUFFIX}"
        )
        self._dlq_consumer_group = dlq_consumer_group

        self._serializer = JsonEventSerializer()

        # Producer used by push() — created in start().
        self._producer: Any | None = None

        # Consumer used by pop_batch() / ack() — created lazily in
        # _ensure_consumer() so push-only users don't pay for a consumer.
        self._consumer: Any | None = None
        self._started = False

        # In-flight tracking: maps entry_id (str) → (TopicPartition, offset)
        # Populated by pop_batch(), consumed by ack().
        # Not persisted — if the process restarts, entries are re-read by
        # the consumer from the last committed offset (at-least-once delivery).
        self._in_flight: dict[str, tuple[TopicPartition, int]] = {}

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start the Kafka producer.  Idempotent.

        The consumer is started lazily on the first ``pop_batch()`` call to
        avoid opening an unnecessary consumer for push-only use cases.

        Raises:
            NoBrokersAvailable: (aiokafka) If the brokers are unreachable.
        """
        if self._started:
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.bootstrap_servers,
            **self._settings.producer_kwargs,
        )
        await self._producer.start()
        self._started = True
        _logger.info(
            "KafkaDLQ started (brokers=%s, dlq_topic=%r)",
            self._settings.bootstrap_servers,
            self._dlq_topic,
        )

    async def stop(self) -> None:
        """
        Stop the producer and consumer.  Idempotent.

        Edge cases:
            - Calling before ``start()`` is a no-op.
            - In-flight entries tracked in ``_in_flight`` are discarded — they
              will be re-delivered on the next ``pop_batch()`` after restart.
        """
        if not self._started:
            return

        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

        self._started = False
        self._in_flight.clear()
        _logger.info("KafkaDLQ stopped.")

    async def __aenter__(self) -> KafkaDLQ:
        """Support ``async with KafkaDLQ(...) as dlq:`` usage."""
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Stop producer and consumer on context manager exit."""
        await self.stop()

    # ── AbstractDeadLetterQueue interface ─────────────────────────────────────

    async def push(self, entry: DeadLetterEntry) -> None:
        """
        Serialize ``entry`` and produce it to the DLQ Kafka topic.

        Uses ``send_and_wait`` — blocks until the broker acknowledges the message.
        This ensures durability: a successful ``push()`` means the entry is in
        Kafka and will not be lost (within Kafka's retention window).

        Args:
            entry: The ``DeadLetterEntry`` to store.

        Edge cases:
            - ``push()`` NEVER raises — all exceptions are swallowed and logged.
              This is a hard contract: callers (retry wrapper) must not be
              interrupted by DLQ failures.
            - If the producer is not started (``start()`` not called), a warning
              is logged and the entry is dropped.
            - Kafka message key is set to ``str(entry.entry_id)`` — useful for
              log compaction if the DLQ topic uses ``cleanup.policy=compact``.
              For a plain retention topic, the key is just for debugging.

        Async safety: ✅ Awaits ``send_and_wait``.
        """
        try:
            if self._producer is None:
                _logger.warning(
                    "KafkaDLQ.push() called before start() — entry dropped "
                    "(entry_id=%s, handler=%r).",
                    entry.entry_id,
                    entry.handler_name,
                )
                return

            payload = self._serialize_entry(entry)
            # Key = entry_id string — enables log compaction and partition
            # stickiness for entries from the same handler.
            key = str(entry.entry_id).encode("utf-8")

            await self._producer.send_and_wait(
                self._dlq_topic,
                value=payload,
                key=key,
            )
            _logger.debug(
                "KafkaDLQ.push: sent entry_id=%s to topic=%r (handler=%r)",
                entry.entry_id,
                self._dlq_topic,
                entry.handler_name,
            )

        except Exception as exc:  # noqa: BLE001 — push MUST NOT propagate
            _logger.error(
                "KafkaDLQ.push() failed — entry dropped (entry_id=%s): %s",
                entry.entry_id,
                exc,
                exc_info=True,
            )

    async def pop_batch(self, *, limit: int = 10) -> list[DeadLetterEntry]:
        """
        Return up to ``limit`` unacknowledged entries from the DLQ topic.

        Uses ``AIOKafkaConsumer.getmany()`` with a 500 ms timeout.  Entries are
        NOT committed — they remain in the consumer's pending-ack state until
        ``ack()`` is called for each one.

        The consumer subscribes to the DLQ topic using ``__varco_dlq_relay__``
        (or the configured group).  Kafka delivers only the unconsumed portion
        of the topic — messages committed by previous ``ack()`` calls are not
        re-delivered.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of ``DeadLetterEntry`` objects.  Empty list if no messages
            are available within the poll timeout.

        Raises:
            ValueError:   If ``limit`` < 1.
            RuntimeError: If called before ``start()``.

        Edge cases:
            - May return fewer than ``limit`` entries even when the DLQ is not
              empty (Kafka returns available messages up to the limit; if fewer
              messages are in the buffer, fewer are returned).
            - Deserialization failures are logged and the message is skipped.
              The offset is still tracked — call ``ack()`` with the entry_id
              recovered from the raw payload to advance past bad messages.
              In practice, raw parsing errors leave the offset un-advanced
              (the entry_id cannot be recovered) and the message is re-delivered
              indefinitely.  This is a known limitation — monitor for
              deserialization errors.
            - Messages from multiple partitions may be interleaved in the
              returned list.  Ack in list order to avoid implicit double-acks
              within the same partition.

        Async safety: ✅ Awaits ``getmany()``.
        """
        if limit < 1:
            raise ValueError(f"pop_batch limit must be ≥ 1, got {limit}.")
        if not self._started:
            raise RuntimeError(
                "KafkaDLQ.pop_batch() called before start(). "
                "Call await dlq.start() or use 'async with dlq' first."
            )

        consumer = await self._ensure_consumer()

        # getmany() returns a dict of {TopicPartition: [ConsumerRecord, ...]}
        # max_records limits the total across all partitions.
        records_by_partition = await consumer.getmany(
            timeout_ms=_CONSUMER_POLL_TIMEOUT_MS,
            max_records=limit,
        )

        entries: list[DeadLetterEntry] = []
        for tp, records in records_by_partition.items():
            for record in records:
                try:
                    entry = self._deserialize_entry(record.value)
                except Exception as exc:  # noqa: BLE001
                    _logger.warning(
                        "KafkaDLQ.pop_batch: failed to deserialize message "
                        "from topic=%r partition=%d offset=%d: %s — skipping.",
                        tp.topic,
                        tp.partition,
                        record.offset,
                        exc,
                        exc_info=True,
                    )
                    continue

                # Track this record so ack() can commit its offset later.
                # Key = str(entry_id) so lookup is O(1) in ack().
                self._in_flight[str(entry.entry_id)] = (tp, record.offset)
                entries.append(entry)

        _logger.debug(
            "KafkaDLQ.pop_batch: returned %d entries (limit=%d)",
            len(entries),
            limit,
        )
        return entries

    async def ack(self, entry_id: UUID) -> None:
        """
        Commit the Kafka offset for the message associated with ``entry_id``.

        After this call, the message is marked as consumed and will not be
        re-delivered to this consumer group.

        IMPORTANT: Kafka offset commits are per-partition and advance-only.
        Committing offset 7 for a partition commits all messages ≤ 7 in that
        partition.  If you ack message B (offset 7) before message A (offset 5)
        in the same partition, message A is also implicitly acked.  Design the
        relay to process and ack messages in the order returned by ``pop_batch()``.

        Args:
            entry_id: The ``DeadLetterEntry.entry_id`` to acknowledge.

        Edge cases:
            - Calling with an unknown ``entry_id`` (not returned by a prior
              ``pop_batch()`` or already acked) is a silent no-op.
            - If the consumer was not yet started (first pop_batch not called),
              ack is a no-op.
            - If the process restarts between pop_batch() and ack(), the
              committed offset is not advanced — the message is re-delivered on
              the next pop_batch() (at-least-once semantics).

        Async safety: ✅ Awaits consumer.commit().
        """
        entry_id_str = str(entry_id)
        record = self._in_flight.pop(entry_id_str, None)

        if record is None:
            # Not in-flight — either already acked or not fetched in this session.
            _logger.debug("KafkaDLQ.ack: entry_id=%s not in-flight — noop.", entry_id)
            return

        tp, offset = record

        # Commit offset + 1 — Kafka convention: committed offset = next offset to fetch.
        # This marks the message at ``offset`` as fully processed.
        await self._consumer.commit({tp: offset + 1})

        _logger.debug(
            "KafkaDLQ.ack: committed offset=%d for partition=%d (entry_id=%s)",
            offset + 1,
            tp.partition,
            entry_id,
        )

    async def count(self) -> int:
        """
        Returns ``-1`` — Kafka consumer lag is not computable without an AdminClient.

        To monitor DLQ depth, use Kafka consumer lag metrics:
        - ``kafka-consumer-groups.sh --describe --group __varco_dlq_relay__``
        - Prometheus ``kafka_consumer_group_lag`` metric
        - Confluent Control Center / Kafka UI

        Returns:
            Always ``-1``.  This is documented behaviour — not an error.

        Edge cases:
            - An alternative: subclass ``KafkaDLQ`` and override ``count()``
              with an AdminClient call to get the consumer group lag.
        """
        # DESIGN: return -1 over NotImplementedError
        #   ✅ Callers can handle -1 gracefully (e.g. skip DLQ count in healthchecks)
        #   ✅ The AbstractDeadLetterQueue docstring explicitly says count() may be
        #      approximate for Kafka — -1 signals "not available"
        #   ❌ -1 is not the same as "empty" or "non-empty" — callers must handle it
        _logger.debug(
            "KafkaDLQ.count(): Kafka consumer lag not available without AdminClient "
            "— returning -1."
        )
        return -1

    # ── Consumer lifecycle helper ──────────────────────────────────────────────

    async def _ensure_consumer(self) -> AIOKafkaConsumer:
        """
        Create and start the DLQ consumer on first use.

        DESIGN: lazy consumer creation
            ✅ Push-only users (the common case) never pay for a consumer.
            ✅ Consumer is started inside a running event loop — no "attached
               to a different loop" errors.
            ❌ First pop_batch() has slightly higher latency (consumer start +
               initial partition assignment).

        Returns:
            The started ``AIOKafkaConsumer`` instance.

        Edge cases:
            - If the DLQ topic does not exist, aiokafka raises
              ``UnknownTopicOrPartitionError`` during assignment.  Pre-create
              the topic or ensure ``auto.create.topics.enable=true`` on the broker.

        Async safety: ✅ Idempotent — safe to call concurrently.
        """
        if self._consumer is not None:
            return self._consumer

        self._consumer = AIOKafkaConsumer(
            self._dlq_topic,
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=self._dlq_consumer_group,
            # auto_offset_reset="earliest" so the relay starts from the
            # beginning of the DLQ on first run — ensures no entries are missed.
            auto_offset_reset="earliest",
            # Manual commits — we commit only after successful relay processing.
            # This gives at-least-once semantics: a relay crash re-delivers entries.
            enable_auto_commit=False,
            **self._settings.consumer_kwargs,
        )
        await self._consumer.start()
        _logger.info(
            "KafkaDLQ consumer started (group=%r, topic=%r)",
            self._dlq_consumer_group,
            self._dlq_topic,
        )
        return self._consumer

    # ── Serialization helpers ──────────────────────────────────────────────────

    def _serialize_entry(self, entry: DeadLetterEntry) -> bytes:
        """
        Serialize ``entry`` to UTF-8 JSON bytes for Kafka storage.

        The nested ``Event`` is serialized using ``JsonEventSerializer`` so it
        can be deserialized back to a typed ``Event`` on pop.

        Args:
            entry: The ``DeadLetterEntry`` to serialize.

        Returns:
            UTF-8 encoded JSON bytes.

        Edge cases:
            - Datetimes are ISO-8601 strings (timezone-aware).
            - ``entry_id`` stored as UUID string for human readability and
              to serve as the Kafka message key.
        """
        event_bytes = self._serializer.serialize(entry.event)

        data = {
            "entry_id": str(entry.entry_id),
            "channel": entry.channel,
            "handler_name": entry.handler_name,
            "error_type": entry.error_type,
            "error_message": entry.error_message,
            "attempts": entry.attempts,
            "first_failed_at": entry.first_failed_at.isoformat(),
            "last_failed_at": entry.last_failed_at.isoformat(),
            # Event serialized with its own type-aware serializer — self-describing.
            "event_payload": event_bytes.decode("utf-8"),
        }
        return json.dumps(data).encode("utf-8")

    def _deserialize_entry(self, payload: bytes) -> DeadLetterEntry:
        """
        Deserialize a Kafka message payload back to a ``DeadLetterEntry``.

        Args:
            payload: Raw JSON bytes from the Kafka message value.

        Returns:
            A fully populated ``DeadLetterEntry``.

        Raises:
            KeyError:   Missing required JSON field.
            ValueError: Malformed field (bad UUID, bad datetime, etc.).

        Edge cases:
            - Datetimes without timezone info are treated as UTC.
        """
        data: dict = json.loads(payload.decode("utf-8"))

        event = self._serializer.deserialize(data["event_payload"].encode("utf-8"))

        def _parse_dt(value: str) -> datetime:
            """Parse ISO-8601 string, defaulting to UTC if tz info is absent."""
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        return DeadLetterEntry(
            entry_id=UUID(data["entry_id"]),
            event=event,
            channel=data["channel"],
            handler_name=data["handler_name"],
            error_type=data["error_type"],
            error_message=data["error_message"],
            attempts=data["attempts"],
            first_failed_at=_parse_dt(data["first_failed_at"]),
            last_failed_at=_parse_dt(data["last_failed_at"]),
        )

    def __repr__(self) -> str:
        return (
            f"KafkaDLQ("
            f"brokers={self._settings.bootstrap_servers!r}, "
            f"dlq_topic={self._dlq_topic!r}, "
            f"group={self._dlq_consumer_group!r}, "
            f"started={self._started})"
        )


# ── DI Configuration ──────────────────────────────────────────────────────────


@Configuration
class KafkaDLQConfiguration:
    """
    Providify ``@Configuration`` that wires ``KafkaDLQ`` into the container.

    Provides:
        ``AbstractDeadLetterQueue`` — started ``KafkaDLQ`` singleton.

    Reuses ``KafkaEventBusSettings`` if already registered by
    ``KafkaEventBusConfiguration``.  If not, falls back to
    ``KafkaEventBusSettings.from_env()``.

    DESIGN: separate @Configuration from KafkaEventBusConfiguration
        ✅ Services that only push to the DLQ (no relay) don't need to install
           the full bus configuration.
        ✅ Both bind to their respective interfaces — no ambiguity.
        ❌ Two installs instead of one for the "bus + DLQ" case.
           Acceptable — privilege separation is worth the extra verbosity.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def``.

    Example (bus + DLQ)::

        container = DIContainer()
        await container.ainstall(KafkaEventBusConfiguration)
        await container.ainstall(KafkaDLQConfiguration)

        dlq = await container.aget(AbstractDeadLetterQueue)

        class MyConsumer(EventConsumer):
            @listen(
                MyEvent,
                retry_policy=RetryPolicy(max_attempts=3),
                dlq=dlq,
            )
            async def on_event(self, event: MyEvent) -> None: ...

    Example (DLQ relay only)::

        container = DIContainer()
        await container.ainstall(KafkaDLQConfiguration)
        dlq = await container.aget(AbstractDeadLetterQueue)

        entries = await dlq.pop_batch(limit=10)
        for entry in entries:
            await handle_dlq_entry(entry)
            await dlq.ack(entry.entry_id)
    """

    @Provider(singleton=True)
    def kafka_dlq_settings(self) -> KafkaEventBusSettings:
        """
        Default ``KafkaEventBusSettings`` for the DLQ.

        If ``KafkaEventBusConfiguration`` was installed first, the container
        resolves the already-registered ``KafkaEventBusSettings`` singleton
        instead of this provider.

        Returns:
            ``KafkaEventBusSettings`` with development-friendly defaults.
        """
        # Reads from VARCO_KAFKA_* env vars if set.
        return KafkaEventBusSettings.from_env()

    @Provider(singleton=True)
    async def kafka_dlq(
        self,
        settings: Inject[KafkaEventBusSettings],
    ) -> AbstractDeadLetterQueue:
        """
        Create and start the ``KafkaDLQ`` singleton.

        Args:
            settings: ``KafkaEventBusSettings`` — injected from the container.

        Returns:
            A started ``KafkaDLQ`` bound to ``AbstractDeadLetterQueue``.

        Raises:
            NoBrokersAvailable: (aiokafka) If the configured brokers are
                                unreachable at startup time.
        """
        _logger.info(
            "KafkaDLQConfiguration: starting KafkaDLQ "
            "(brokers=%s, dlq_topic=%s__dlq__)",
            settings.bootstrap_servers,
            settings.channel_prefix,
        )
        dlq = KafkaDLQ(settings)
        await dlq.start()
        return dlq


# ── Public API ────────────────────────────────────────────────────────────────


__all__ = [
    "KafkaDLQ",
    "KafkaDLQConfiguration",
]
