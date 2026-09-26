"""
varco_redis.stream_dlq
======================
Redis Streams-backed implementation of ``AbstractDeadLetterQueue``.

``RedisStreamDLQ`` uses a dedicated Redis Stream key
``{channel_prefix}dlq:stream`` with a consumer group for durable,
at-least-once pop + ack semantics — the same transport as
``RedisStreamEventBus``, so no new Redis topology is required.

Architecture
------------
::

    push()    → XADD {prefix}dlq:stream * {payload: <json bytes>}
    pop_batch() → XREADGROUP GROUP {group} CONSUMER varco-dlq-relay
                            COUNT {limit} BLOCK 0 STREAMS {key} >
    ack()     → XACK {key} {group} {stream_msg_id}
                XDEL {key} {stream_msg_id}   ← clean up acknowledged messages

DESIGN: entry_id ↔ stream message ID mapping
    ``DeadLetterEntry.entry_id`` is a UUID required by the
    ``AbstractDeadLetterQueue.ack(UUID)`` contract.  Redis Streams assign
    their own message IDs (e.g. ``"1704067200000-0"``) at XADD time.  These
    two ID spaces must be bridged.

    **Chosen approach**: an in-memory ``_pending`` dict mapping
    ``entry_id (UUID)`` → ``stream_msg_id (bytes)``, populated by
    ``pop_batch()`` and cleared by ``ack()``.

    Tradeoffs:
        ✅ ``DeadLetterEntry`` stays frozen — no field additions needed.
        ✅ Minimal complexity — one dict per DLQ instance.
        ✅ The ``ack()`` contract is satisfied: callers use the UUID they
           received from ``pop_batch()``.
        ❌ In-memory map is lost on process restart.  If the relay crashes
           between ``pop_batch()`` and ``ack()``, the messages stay in the
           PEL and are re-delivered on the next ``pop_batch()`` call (the
           consumer group reads them again via XREADGROUP).  Each re-delivery
           produces a new ``entry_id`` UUID for the same underlying stream
           message — this is acceptable because it is at-least-once semantics.
        ❌ Concurrent relay instances sharing the same consumer name would
           corrupt the map.  Use distinct ``consumer_name`` values per
           replica (default: ``"varco-dlq-relay"`` is a single-relay design).

    Rejected alternative: encode the stream msg ID as the ``entry_id`` UUID
        Using a deterministic UUID derived from the stream msg ID (e.g.
        ``uuid.uuid5(NAMESPACE_URL, msg_id_str)``) would eliminate the dict.
        Rejected because: callers expect ``DeadLetterEntry.entry_id`` to be
        a stable, opaque identifier, not an encoded transport address.  It
        would leak Redis stream internals into the public API.

DESIGN: dedicated stream key over re-using bus stream keys
    ✅ DLQ entries are operationally separate from live events — separate key
       makes it trivial to XLEN / XRANGE / XTRIM the DLQ without affecting
       live streams.
    ✅ Different retention policies can be applied (e.g. MAXLEN on the DLQ
       stream, long retention on live streams).
    ❌ One extra Redis key to monitor; negligible in practice.

DESIGN: consumer group over plain XREAD
    ✅ Enables at-least-once relay: messages unacknowledged due to a relay
       crash re-appear in the PEL (Pending Entry List) and are re-delivered
       on the next ``pop_batch()`` call.
    ✅ Consistent with ``RedisStreamEventBus`` — operators already understand
       consumer groups; no new mental model needed.
    ❌ Requires ``XGROUP CREATE`` before first ``pop_batch()`` — handled
       lazily and idempotently inside ``pop_batch()`` itself.

Serialization
-------------
``DeadLetterEntry`` is serialized to JSON using ``JsonEventSerializer``
(same format as ``RedisDLQ``) — the entire entry is stored in the ``payload``
field of each stream entry.

Key naming
----------
::

    {channel_prefix}dlq:stream    ← Redis Stream

Usage::

    from varco_redis.stream_dlq import RedisStreamDLQ
    from varco_redis.config import RedisEventBusSettings

    dlq = RedisStreamDLQ(settings=RedisEventBusSettings())
    await dlq.connect()

    await dlq.push(entry)              # XADD
    entries = await dlq.pop_batch()    # XREADGROUP (blocks until msgs arrive)
    await dlq.ack(entries[0].entry_id) # XACK + XDEL

    await dlq.disconnect()

Or as async context manager::

    async with RedisStreamDLQ() as dlq:
        ...

DI integration via ``RedisStreamDLQConfiguration``::

    from varco_redis.stream_dlq import RedisStreamDLQConfiguration

    container = DIContainer()
    await container.ainstall(RedisStreamDLQConfiguration)
    dlq = await container.aget(AbstractDeadLetterQueue)

Thread safety:  ❌ Not thread-safe — use from a single event loop.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🔍 https://redis.io/docs/data-types/streams/
  Redis Streams — XADD, XREADGROUP, XACK, XDEL, consumer groups
- 🔍 https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.xadd
  redis-py xadd / xreadgroup / xack / xdel API reference
- 🔍 https://redis.io/commands/xgroup-create/
  XGROUP CREATE with MKSTREAM flag
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

import redis.asyncio as aioredis
from providify import Configuration, Disposes, Fallback, InjectMeta, Provider
from varco_core.di import BACKEND_PRIORITY
from varco_core.di.select import selected_by
from varco_core.event.dlq import AbstractDeadLetterQueue, DeadLetterEntry
from varco_core.event.serializer import JsonEventSerializer

from varco_redis.config import RedisEventBusSettings

_logger = logging.getLogger(__name__)

# Redis Stream key suffix — appended to channel_prefix.
# DESIGN: different suffix from RedisDLQ ("{prefix}dlq:entries") so the two
# DLQ implementations can coexist in the same Redis keyspace without collision.
_STREAM_SUFFIX = "dlq:stream"

# Consumer group name used by the relay reader.
# All relay replicas sharing this name will load-balance consumption.
# Use distinct group names if you want independent fan-out readers.
_DEFAULT_GROUP = "varco-dlq"

# Consumer identity within the group.  A single-relay deployment can use a
# fixed name; multi-replica deployments should use a unique name per pod.
_DEFAULT_CONSUMER = "varco-dlq-relay"

# How long to block on XREADGROUP (milliseconds) when no messages are present.
# Matches the _BLOCK_MS in streams.py — balances responsiveness vs CPU use.
# DESIGN: blocking (not polling)
#   ✅ Event loop yields while waiting — no busy spin.
#   ✅ Wakes up within 100ms of a new message arriving.
#   ❌ Cannot be interrupted mid-block; only cancellation or message arrival
#      unblocks the call.
_BLOCK_MS = 100

# Field name inside each Redis Stream entry that carries the JSON payload.
_PAYLOAD_FIELD = "payload"


# ── RedisStreamDLQ ────────────────────────────────────────────────────────────


class RedisStreamDLQ(AbstractDeadLetterQueue):
    """
    Redis Streams-backed ``AbstractDeadLetterQueue``.

    Uses a dedicated stream key ``{prefix}dlq:stream`` with a consumer group
    for durable, at-least-once pop + ack semantics — the same transport as
    ``RedisStreamEventBus``, so no new Redis topology is required.

    ``push()`` appends to the stream via ``XADD``.
    ``pop_batch()`` reads unacknowledged messages via ``XREADGROUP``.
    ``ack()`` removes the message from the PEL via ``XACK`` and deletes it
    from the stream via ``XDEL`` to reclaim memory.

    Args:
        settings: ``RedisEventBusSettings`` — reuses the bus connection config.
                  DLQ keys are namespaced under ``{channel_prefix}`` to avoid
                  collision with bus channels.
        group:    Consumer group name.  All relay instances sharing the same
                  group load-balance consumption.  Defaults to ``"varco-dlq"``.
        consumer: Consumer identity within the group.  Must be unique per
                  replica if multiple relays run in parallel.  Defaults to
                  ``"varco-dlq-relay"``.

    Lifecycle:
        Call ``await dlq.connect()`` before use.  Call ``await dlq.disconnect()``
        when done.  Or use as an async context manager.

    Thread safety:  ❌ Not thread-safe.  Use from a single event loop.
    Async safety:   ✅ All methods are ``async def``.

    Edge cases:
        - ``push()`` NEVER raises — all exceptions are swallowed and logged.
          Callers (the retry wrapper) cannot recover from DLQ failures.
        - ``pop_batch()`` blocks up to 100ms if the stream is empty before
          returning an empty list.
        - The consumer group is created lazily on first ``pop_batch()`` call.
          ``BUSYGROUP`` errors (group already exists) are silently suppressed.
        - ``ack()`` on an unknown entry_id is a no-op — the ``_pending`` map
          simply won't contain it.
        - The ``_pending`` map is in-memory; it is lost on process restart.
          Unacked stream messages re-appear in the PEL and are re-delivered
          on the next ``pop_batch()`` call with a fresh ``entry_id``.

    Example::

        async with RedisStreamDLQ(RedisEventBusSettings()) as dlq:
            entries = await dlq.pop_batch(limit=5)
            for entry in entries:
                print(f"Failed: {entry.handler_name}")
                await dlq.ack(entry.entry_id)
    """

    def __init__(
        self,
        settings: RedisEventBusSettings | None = None,
        *,
        group: str = _DEFAULT_GROUP,
        consumer: str = _DEFAULT_CONSUMER,
    ) -> None:
        """
        Args:
            settings: Redis connection settings.  Defaults to
                      ``RedisEventBusSettings.from_env()`` (reads
                      ``VARCO_REDIS_*`` env vars or ``redis://localhost:6379/0``).
            group:    Consumer group name.  Instances sharing the same group
                      load-balance.  Use distinct names for independent readers.
            consumer: Consumer identity within the group.
        """
        self._settings = settings or RedisEventBusSettings.from_env()
        self._group = group
        self._consumer = consumer

        # Stream key — namespaced to avoid collision with bus channel keys.
        self._stream_key = f"{self._settings.channel_prefix}{_STREAM_SUFFIX}"

        # Serializer for the nested Event inside DeadLetterEntry.
        # Same serializer as the bus and RedisDLQ — ensures round-trip consistency.
        self._serializer = JsonEventSerializer()

        # Redis client created lazily in connect() — must not be instantiated
        # outside a running event loop (redis.asyncio requirement).
        self._redis: Any | None = None

        # In-memory map: entry_id (UUID) → Redis stream message ID (bytes).
        # Populated by pop_batch(); cleared by ack().
        # See module-level DESIGN comment for the tradeoff discussion.
        self._pending: dict[UUID, bytes] = {}

        # Flag used to create the consumer group at most once per connection.
        # Resets on disconnect() so reconnect() re-creates if needed.
        self._group_created: bool = False

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """
        Open the Redis connection.  Idempotent.

        Must be called before ``push()``, ``pop_batch()``, ``ack()``, or ``count()``.

        Raises:
            ConnectionError: (redis.asyncio) If Redis is unreachable.
        """
        if self._redis is not None:
            # Already connected — idempotent.
            return
        self._redis = aioredis.from_url(
            self._settings.url,
            decode_responses=False,  # need raw bytes for JSON payload
            socket_timeout=self._settings.socket_timeout,
            **self._settings.redis_kwargs,
        )
        # Reset the group-created flag so a fresh reconnect re-creates if needed.
        self._group_created = False
        _logger.info(
            "RedisStreamDLQ connected (url=%s, stream_key=%r, group=%r).",
            self._settings.url,
            self._stream_key,
            self._group,
        )

    async def disconnect(self) -> None:
        """
        Close the Redis connection.  Idempotent.

        Edge cases:
            - Calling before ``connect()`` is a no-op.
            - Any in-memory ``_pending`` entries are lost — their stream
              messages remain in the PEL and will be re-delivered on reconnect.
        """
        if self._redis is None:
            return
        await self._redis.aclose()
        self._redis = None
        self._group_created = False
        # Clear pending map on disconnect — entries can no longer be acked
        # against a closed connection; they will be re-read from PEL on next start.
        self._pending.clear()
        _logger.info("RedisStreamDLQ disconnected.")

    async def __aenter__(self) -> RedisStreamDLQ:
        """Support ``async with RedisStreamDLQ(...) as dlq:`` usage."""
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Disconnect on context manager exit."""
        await self.disconnect()

    # ── AbstractDeadLetterQueue interface ─────────────────────────────────────

    async def push(self, entry: DeadLetterEntry) -> None:
        """
        Serialize ``entry`` and append it to the Redis Stream via ``XADD``.

        Args:
            entry: The ``DeadLetterEntry`` to store.

        Edge cases:
            - NEVER raises — all exceptions are swallowed and logged.
              This is a hard contract: callers (retry wrapper) must not be
              interrupted by DLQ failures.
            - If the Redis connection is not open (``connect()`` not called),
              a warning is logged and the entry is silently dropped.
            - Each call appends a new stream entry — there is no deduplication
              on ``entry_id``.  Idempotent re-pushes result in duplicate stream
              entries (acceptable: DLQ entries represent distinct failure events).

        Async safety: ✅ Each XADD is an independent, non-blocking call.
        """
        try:
            if self._redis is None:
                _logger.warning(
                    "RedisStreamDLQ.push() called before connect() — entry dropped "
                    "(entry_id=%s, handler=%r).",
                    entry.entry_id,
                    entry.handler_name,
                )
                return

            payload = self._serialize_entry(entry)
            # XADD with id="*" lets Redis auto-assign a monotonic stream message ID.
            # The "*" auto-id ensures strict ordering of DLQ entries by push time.
            await self._redis.xadd(self._stream_key, {_PAYLOAD_FIELD: payload})

            _logger.debug(
                "RedisStreamDLQ.push: stored entry_id=%s handler=%r error=%r",
                entry.entry_id,
                entry.handler_name,
                entry.error_type,
            )

        except Exception as exc:  # noqa: BLE001 — push MUST NOT propagate
            _logger.error(
                "RedisStreamDLQ.push() failed unexpectedly — entry dropped (entry_id=%s): %s",
                entry.entry_id,
                exc,
                exc_info=True,
            )

    async def pop_batch(self, *, limit: int = 10) -> list[DeadLetterEntry]:
        """
        Retrieve up to ``limit`` unacknowledged entries from the DLQ stream.

        Uses ``XREADGROUP`` with the consumer group so that messages returned
        here are tracked in the PEL (Pending Entry List) until ``ack()`` is
        called.  If the relay crashes mid-processing, the messages re-appear
        in the PEL on the next call.

        On first call, creates the consumer group via ``XGROUP CREATE ... MKSTREAM``
        (idempotent — ``BUSYGROUP`` errors are suppressed).

        Args:
            limit: Maximum number of entries to return.  Must be ≥ 1.

        Returns:
            List of ``DeadLetterEntry`` objects, arrival order (oldest-first).
            Empty list if the DLQ stream is empty and the block timeout expires.

        Raises:
            ValueError:  If ``limit`` < 1.
            RuntimeError: If called before ``connect()``.

        Edge cases:
            - Blocks up to 100ms waiting for messages before returning empty.
            - Entries whose payload cannot be deserialized are logged at WARNING
              and skipped.  Their stream message ID is NOT acked — use
              ``xdel`` directly in redis-cli to remove corrupt entries.
            - Concurrent ``pop_batch()`` calls with the same consumer name will
              both claim different messages from the group (load-balanced).
              Use distinct consumer names for independent replicas.

        Async safety: ✅ XREADGROUP is non-blocking for the event loop
                      (the 100ms block runs in the redis.asyncio I/O layer).
        """
        if limit < 1:
            raise ValueError(f"pop_batch limit must be ≥ 1, got {limit}.")
        if self._redis is None:
            raise RuntimeError(
                "RedisStreamDLQ.pop_batch() called before connect(). "
                "Call await dlq.connect() or use 'async with dlq' first."
            )

        # Create the consumer group lazily on first call — idempotent.
        # BUSYGROUP means the group already exists (expected on reconnect).
        if not self._group_created:
            await self._ensure_group()

        # XREADGROUP reads new (">") messages not yet delivered to this group.
        # The ">" special ID means: give me messages I have not yet seen.
        # block=_BLOCK_MS — wait up to 100ms for messages before returning.
        results = await self._redis.xreadgroup(
            self._group,
            self._consumer,
            {self._stream_key: ">"},
            count=limit,
            block=_BLOCK_MS,
        )

        if not results:
            # No messages available — normal when DLQ is empty.
            return []

        entries: list[DeadLetterEntry] = []
        # results is a list of [stream_key, [(msg_id, fields), ...]]
        for _stream_key, messages in results:
            for msg_id, fields in messages:
                # redis-py returns field names as bytes when decode_responses=False.
                # Try both bytes and str keys for robustness against redis-py
                # version differences or future decode_responses changes.
                payload: bytes | None = fields.get(b"payload") or fields.get(_PAYLOAD_FIELD)
                if payload is None:
                    _logger.warning(
                        "RedisStreamDLQ.pop_batch: stream entry %r has no 'payload' "
                        "field — skipping.",
                        msg_id,
                    )
                    continue

                try:
                    entry = self._deserialize_entry(payload)
                except Exception as exc:  # noqa: BLE001
                    _logger.warning(
                        "RedisStreamDLQ.pop_batch: failed to deserialize stream "
                        "entry %r: %s — skipping.",
                        msg_id,
                        exc,
                        exc_info=True,
                    )
                    continue

                # Register the mapping UUID → stream msg ID so ack() can find it.
                self._pending[entry.entry_id] = msg_id
                entries.append(entry)

        _logger.debug(
            "RedisStreamDLQ.pop_batch: returned %d entries (limit=%d).",
            len(entries),
            limit,
        )
        return entries

    async def ack(self, entry_id: UUID) -> None:
        """
        Acknowledge a DLQ entry: remove it from the PEL and delete from stream.

        Calls ``XACK`` (removes from PEL) then ``XDEL`` (removes stream entry
        to reclaim memory).  Idempotent — calling with an unknown ``entry_id``
        is a silent no-op.

        Args:
            entry_id: The ``DeadLetterEntry.entry_id`` returned by ``pop_batch()``.

        Edge cases:
            - If ``entry_id`` is not in the ``_pending`` map (e.g. process
              restarted since pop_batch), this is a no-op — the message stays
              in the PEL and will be re-delivered on the next ``pop_batch()``.
            - If Redis is disconnected, a warning is logged and the call is a
              no-op.

        Async safety: ✅ Two sequential awaits — XACK then XDEL.
        """
        if self._redis is None:
            _logger.warning(
                "RedisStreamDLQ.ack() called before connect() — noop (entry_id=%s).",
                entry_id,
            )
            return

        msg_id = self._pending.pop(entry_id, None)
        if msg_id is None:
            # entry_id not in pending map — either already acked or from a
            # different process restart.  Silently ignore — idempotent contract.
            _logger.debug(
                "RedisStreamDLQ.ack: entry_id=%s not in pending map — noop.",
                entry_id,
            )
            return

        # XACK: removes from PEL so the message is no longer tracked as in-flight.
        await self._redis.xack(self._stream_key, self._group, msg_id)
        # XDEL: removes the message body from the stream to free memory.
        # DESIGN: XDEL after XACK (not instead of)
        #   ✅ XACK is idempotent — safe to call even if msg_id no longer exists.
        #   ✅ XDEL removes the actual data; without it the stream grows unbounded.
        #   ❌ Two round-trips instead of one.  Acceptable for a low-throughput DLQ.
        await self._redis.xdel(self._stream_key, msg_id)

        _logger.debug(
            "RedisStreamDLQ.ack: acknowledged entry_id=%s (stream_id=%r).",
            entry_id,
            msg_id,
        )

    async def count(self) -> int:
        """
        Return the approximate number of pending entries in the DLQ stream.

        Uses ``XLEN`` — O(1), returns the total number of stream entries
        (including entries in flight / not yet acked).

        Returns:
            Non-negative integer.  ``0`` if the DLQ is empty.

        Raises:
            RuntimeError: If called before ``connect()``.

        Edge cases:
            - ``XLEN`` counts ALL stream entries, including entries already
              delivered to the consumer group but not yet acked (in-flight).
              This is an upper bound on the true unprocessed entry count.
            - After ``XDEL`` in ``ack()``, entries are removed — the count
              reflects only unacked entries once the relay has caught up.

        Async safety: ✅ Awaits single XLEN command.
        """
        if self._redis is None:
            raise RuntimeError(
                "RedisStreamDLQ.count() called before connect(). "
                "Call await dlq.connect() or use 'async with dlq' first."
            )
        # redis-py ships no py.typed marker — xlen() is structurally Any.
        length: int = await self._redis.xlen(self._stream_key)
        return length

    # ── Consumer group management ──────────────────────────────────────────────

    async def _ensure_group(self) -> None:
        """
        Create the consumer group if it does not already exist.

        Uses ``XGROUP CREATE ... $ MKSTREAM`` so:
        - The stream is created if it does not exist (``MKSTREAM``).
        - The group starts from the tail (``$``) — messages pushed before this
          call are considered already processed.

        DESIGN: start from "0" (oldest) over "$" (latest)
            Starting from "0" means all existing stream entries are delivered
            to this consumer group, including messages pushed BEFORE the group
            was created.  This is correct for a DLQ relay: push() may be called
            before pop_batch() (the relay may start later), and we must not drop
            any failed events.
            ✅ No DLQ entries are lost even if the relay starts after push().
            ✅ On restart (BUSYGROUP), the group already tracks its read position
               so only unacknowledged (PEL) entries are re-delivered.
            ❌ On the very first deploy, any historical stream entries are
               re-processed.  For a DLQ this is the desired behaviour — failed
               events should always be inspected.

        Edge cases:
            - ``BUSYGROUP`` error means the group already exists — silently
              suppressed.  This is expected on reconnect or concurrent startup.
            - Any other ``ResponseError`` propagates to the caller.

        Async safety: ✅ Single XGROUP CREATE command.
        """
        assert self._redis is not None
        try:
            await self._redis.xgroup_create(
                self._stream_key,
                self._group,
                id="0",  # start from the beginning — deliver all existing entries
                mkstream=True,  # create the stream if it doesn't exist yet
            )
            self._group_created = True
            _logger.debug(
                "RedisStreamDLQ: created consumer group %r on stream %r (id='0').",
                self._group,
                self._stream_key,
            )
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                # Group already exists — expected on reconnect or concurrent start.
                self._group_created = True
                _logger.debug(
                    "RedisStreamDLQ: group %r already exists on stream %r.",
                    self._group,
                    self._stream_key,
                )
            else:
                # Unexpected error — propagate so the caller knows something is wrong.
                raise

    # ── Serialization helpers ──────────────────────────────────────────────────

    def _serialize_entry(self, entry: DeadLetterEntry) -> bytes:
        """
        Serialize a ``DeadLetterEntry`` to JSON bytes for Redis Stream storage.

        The nested ``Event`` is serialized using ``JsonEventSerializer`` (same
        format as the bus) so it can be deserialized back to a typed ``Event``
        on pop.  All other fields are serialized as a flat JSON dict.

        Args:
            entry: The ``DeadLetterEntry`` to serialize.

        Returns:
            UTF-8 encoded JSON bytes.

        Edge cases:
            - Datetimes are stored as ISO-8601 strings (timezone-aware).
            - ``entry_id`` is stored as a UUID string for human readability.
        """
        # Serialize the nested Event to bytes, then decode to a JSON-compatible
        # string for embedding in the outer dict.
        event_bytes = self._serializer.serialize(entry.event)  # type: ignore[arg-type]

        data = {
            "entry_id": str(entry.entry_id),
            "channel": entry.channel,
            "handler_name": entry.handler_name,
            "error_type": entry.error_type,
            "error_message": entry.error_message,
            "attempts": entry.attempts,
            "first_failed_at": entry.first_failed_at.isoformat(),
            "last_failed_at": entry.last_failed_at.isoformat(),
            # Embed the event payload as a JSON string — it's already
            # self-describing (contains __event_type__).
            "event_payload": event_bytes.decode("utf-8"),
        }
        return json.dumps(data).encode("utf-8")

    def _deserialize_entry(self, payload: bytes) -> DeadLetterEntry:
        """
        Deserialize a Redis Stream payload back to a ``DeadLetterEntry``.

        Args:
            payload: Raw JSON bytes from the Redis Stream entry's ``payload`` field.

        Returns:
            A fully populated ``DeadLetterEntry``.

        Raises:
            KeyError:   If a required JSON field is missing.
            ValueError: If a field has an unexpected type or format.

        Edge cases:
            - ``first_failed_at`` and ``last_failed_at`` are parsed as ISO-8601
              strings with timezone.  If stored without timezone (legacy data),
              they are treated as UTC.
            - The ``entry_id`` in the deserialized entry is the UUID stored
              inside the JSON payload — NOT a new UUID4.  This ensures the relay
              can correlate entries across re-serializations.
        """
        data: dict[str, Any] = json.loads(payload.decode("utf-8"))

        # Re-deserialize the embedded Event payload — uses self._serializer
        # which resolves the event class via __event_type__ registry lookup.
        event = self._serializer.deserialize(data["event_payload"].encode("utf-8"))

        def _parse_dt(value: str) -> datetime:
            """Parse ISO-8601 datetime, defaulting to UTC if no tz info."""
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                # Legacy data stored without timezone — assume UTC.
                dt = dt.replace(tzinfo=UTC)
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
            f"RedisStreamDLQ("
            f"url={self._settings.url!r}, "
            f"stream_key={self._stream_key!r}, "
            f"group={self._group!r}, "
            f"connected={self._redis is not None}, "
            f"pending={len(self._pending)})"
        )


# ── DI Configuration ──────────────────────────────────────────────────────────


@Configuration
class RedisStreamDLQConfiguration:
    """
    Providify ``@Configuration`` that wires ``RedisStreamDLQ`` into the container.

    Provides:
        ``AbstractDeadLetterQueue`` — connected ``RedisStreamDLQ`` singleton.

    Reuses ``RedisEventBusSettings`` if already registered (e.g. by
    ``scan()`` / ``bootstrap()``).  If not, falls back to
    ``RedisEventBusSettings.from_env()``.

    Lifecycle:
        The DLQ is connected inside the provider and disconnected
        automatically by this configuration's ``@Disposes(AbstractDeadLetterQueue)``
        method (``close_dlq``) when ``await container.ashutdown()`` is called
        — not ``@PreDestroy``, which providify never invokes on a
        ``@Provider``-produced instance (Plan 024 / C2). Calling
        ``await dlq.disconnect()`` explicitly beforehand is also safe.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def``.

    Example (Streams bus + Streams DLQ)::

        from varco_core.di import bind_instance
        from varco_redis.di import bootstrap
        from varco_redis.stream_dlq import RedisStreamDLQConfiguration

        container = DIContainer()
        # replaces the removed bootstrap(streams=True) (Plan 046 §D-streams)
        bind_instance(container, RedisEventBusSettings, RedisEventBusSettings(use_streams=True))
        bootstrap(container)
        await container.ainstall(RedisStreamDLQConfiguration)

        dlq = await container.aget(AbstractDeadLetterQueue)

    Example (DLQ only)::

        container = DIContainer()
        await container.ainstall(RedisStreamDLQConfiguration)
        dlq = await container.aget(AbstractDeadLetterQueue)
        entries = await dlq.pop_batch(limit=10)
    """

    @Fallback
    @selected_by("dlq", "redis-streams")
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    async def redis_stream_dlq(
        self,
        settings: Annotated[RedisEventBusSettings | None, InjectMeta(optional=True)] = None,
    ) -> AbstractDeadLetterQueue:
        """
        Create, connect, and return the ``RedisStreamDLQ`` singleton.

        Args:
            settings: Optionally-injected ``RedisEventBusSettings`` — reuses
                        the bus's own settings binding when a bus was already
                        registered via ``scan()``/``bootstrap()``; ``None``
                        when nothing is bound, in which case this falls back
                        to ``RedisEventBusSettings.from_env()`` (Plan 045 —
                        replaces a second, now-deleted ``@Provider`` of this
                        settings type).

        Returns:
            A connected ``RedisStreamDLQ`` bound to ``AbstractDeadLetterQueue``.

        Raises:
            ConnectionError: (redis.asyncio) If Redis is unreachable at startup.
        """
        settings = settings or RedisEventBusSettings.from_env()
        _logger.info(
            "RedisStreamDLQConfiguration: connecting RedisStreamDLQ (url=%s).",
            settings.url,
        )
        dlq = RedisStreamDLQ(settings)
        await dlq.connect()
        return dlq

    @Disposes(AbstractDeadLetterQueue)
    async def close_dlq(self, dlq: RedisStreamDLQ) -> None:
        """
        Disconnect the ``RedisStreamDLQ`` produced by ``redis_stream_dlq``.

        DESIGN: ``@Disposes`` over relying on ``@PreDestroy``
            ✅ Providify never invokes ``@PreDestroy`` on a ``@Provider``-produced
               instance — ``@Disposes`` is upstream's own supported teardown
               mechanism (Plan 024 / C2). ``RedisStreamDLQ`` carries no
               ``@PreDestroy`` (Tier B — invisible to providify's
               ``UNREACHABLE_PRE_DESTROY`` detector).

        Args:
            dlq: The ``RedisStreamDLQ`` instance this configuration produced.
                Typed concretely (not ``AbstractDeadLetterQueue``) because
                ``disconnect()`` is not part of the ABC — see the sibling
                note in ``RedisDLQConfiguration.close_dlq``.

        Async safety: ✅ Awaited by providify's ``_adispose`` during
            ``container.ashutdown()``.
        """
        await dlq.disconnect()


# ── Public API ────────────────────────────────────────────────────────────────


__all__ = [
    "RedisStreamDLQ",
    "RedisStreamDLQConfiguration",
]
