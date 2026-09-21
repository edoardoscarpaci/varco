"""
varco_redis.dlq
===============
Redis-backed implementation of ``AbstractDeadLetterQueue``.

``RedisDLQ`` uses two Redis data structures for persistent, ordered storage:

- **Hash** ``{prefix}dlq:entries`` — maps ``entry_id → serialized JSON payload``.
  Stores the full ``DeadLetterEntry`` as JSON bytes.  O(1) lookup by id.

- **Sorted Set** ``{prefix}dlq:queue`` — maps ``entry_id → score`` where score is
  the Unix timestamp of ``DeadLetterEntry.last_failed_at``.  Provides FIFO ordering
  (oldest failures first) for ``pop_batch()`` and O(1) ``count()`` via ``ZCARD``.

DESIGN: Hash + Sorted Set over Redis Streams
    ✅ No consumer group setup — simpler to operate and test.
    ✅ ``pop_batch()`` does NOT remove entries — they stay until ``ack()`` is called.
       This gives at-least-once relay semantics: a crashed relay re-reads the same
       entries on restart.
    ✅ ``ack()`` removes from BOTH structures atomically via a pipeline.
    ✅ ``count()`` is O(1) via ``ZCARD`` — no full scan.
    ❌ ``pop_batch()`` always re-reads the same entries until acked — if the relay
       crashes between pop and ack, entries are reprocessed.  Handlers must be
       idempotent.
    ❌ Not a "once per consumer group" model — all consumers reading from the same
       DLQ see the same entries.  For fan-out DLQ monitoring, use Redis Streams.

Alternative considered: Redis List (LPUSH/RPOP)
    Rejected — RPOP removes entries immediately, so a relay crash loses entries
    permanently.  The Hash+ZSET approach retains entries until explicit ack.

Alternative considered: Redis Streams (XADD + XREADGROUP)
    Rejected — requires consumer group setup and a more complex ack protocol.
    The Hash+ZSET approach is simpler and sufficient for single-relay scenarios.

Serialization
-------------
``DeadLetterEntry`` is serialized to JSON using ``JsonEventSerializer`` for the
``event`` field, plus a JSON wrapper for the metadata fields (handler_name, error_type,
error_message, attempts, timestamps).  The event is stored using its own serializer
so it can be deserialized back to a typed ``Event`` on pop.

Key naming
----------
::

    {channel_prefix}dlq:entries    ← Redis Hash (entry_id → JSON bytes)
    {channel_prefix}dlq:queue      ← Redis Sorted Set (entry_id → timestamp score)

Usage::

    from varco_redis.dlq import RedisDLQ
    from varco_redis.config import RedisEventBusSettings

    dlq = RedisDLQ(settings=RedisEventBusSettings())
    await dlq.connect()

    # push (called automatically by @listen(dlq=dlq) when retries are exhausted)
    await dlq.push(
        DeadLetterEntry.from_failure(
            event=my_event,
            channel="orders",
            handler_name="OrderConsumer.on_order_placed",
            last_exc=ValueError("processing failed"),
            attempts=3,
            first_failed_at=datetime.now(UTC),
        )
    )

    # relay — pop, process, ack
    entries = await dlq.pop_batch(limit=10)
    for entry in entries:
        await replay_or_alert(entry)
        await dlq.ack(entry.entry_id)

    # cleanup
    await dlq.disconnect()

Or use as async context manager::

    async with RedisDLQ(settings=...) as dlq:
        ...

DI integration via ``RedisDLQConfiguration``::

    from varco_redis.di import RedisDLQConfiguration
    from varco_core.event.dlq import AbstractDeadLetterQueue

    container = DIContainer()
    await container.ainstall(RedisDLQConfiguration)

    dlq = await container.aget(AbstractDeadLetterQueue)

Thread safety:  ❌ Not thread-safe — use from a single event loop.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🔍 https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.zadd
  ZADD — Sorted Set add/score
- 🔍 https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.hset
  HSET — Hash field set
- 🔍 https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.pipeline.Pipeline
  Pipeline — atomic multi-command execution
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
from varco_core.event.base import Event
from varco_core.event.dlq import AbstractDeadLetterQueue, DeadLetterEntry
from varco_core.event.serializer import JsonEventSerializer
from varco_core.serialization import Serializer

from varco_redis.config import RedisEventBusSettings

_logger = logging.getLogger(__name__)

# Suffix for the Redis Hash that stores full entry payloads.
_ENTRIES_SUFFIX = "dlq:entries"
# Suffix for the Redis Sorted Set that provides ordering and membership.
_QUEUE_SUFFIX = "dlq:queue"


def _coerce_utc(dt: datetime | None) -> datetime | None:
    """Coerce a naive datetime to UTC (SQLite/caller parity — see varco_sa.dlq)."""
    if dt is None or dt.tzinfo is not None:
        return dt
    _logger.warning("RedisDLQ received a naive datetime filter — assuming UTC.")
    return dt.replace(tzinfo=UTC)


# ── RedisDLQ ──────────────────────────────────────────────────────────────────


class RedisDLQ(AbstractDeadLetterQueue):
    """
    Redis-backed ``AbstractDeadLetterQueue`` using Hash + Sorted Set.

    Each DLQ entry is stored in two structures:
    - A **Hash** for O(1) lookup by ``entry_id``.
    - A **Sorted Set** scored by ``last_failed_at`` timestamp for FIFO ordering.

    ``pop_batch()`` reads entries without removing them.  ``ack()`` removes
    the entry from both structures atomically via a Redis pipeline.

    Args:
        settings: ``RedisEventBusSettings`` — reuses the bus connection config.
                  DLQ keys are namespaced under ``{channel_prefix}`` to avoid
                  collision with bus channels.

    Lifecycle:
        Call ``await dlq.connect()`` before use.  Call ``await dlq.disconnect()``
        when done.  Or use as an async context manager.

    Thread safety:  ❌ Not thread-safe.  Use from a single event loop.
    Async safety:   ✅ All methods are ``async def``.

    Edge cases:
        - ``push()`` must NOT raise — it swallows all exceptions and logs.
          Callers (the retry wrapper) cannot recover from DLQ failures.
        - ``pop_batch()`` re-reads the same entries on every call until ``ack()``
          is called — implement the relay to ack each entry promptly.
        - Large payloads (> Redis max string size 512 MB) will fail at push time.
          In practice, event payloads should be well under 1 MB.
        - Redis Sorted Set scores are IEEE 754 doubles — timestamps in seconds
          since epoch are representable without precision loss up to year ~2255.

    Example::

        async with RedisDLQ(settings=RedisEventBusSettings()) as dlq:
            entries = await dlq.pop_batch(limit=5)
            for entry in entries:
                print(f"Failed: {entry.handler_name} — {entry.error_message}")
                await dlq.ack(entry.entry_id)
    """

    # RD-4 — Redis Hash+ZSET is addressable (unlike Kafka/NATS streams).
    supports_random_access = True

    def __init__(
        self,
        settings: RedisEventBusSettings | None = None,
        *,
        serializer: Serializer[Event] | None = None,
    ) -> None:
        """
        Args:
            settings:   Redis connection settings.  Defaults to
                        ``RedisEventBusSettings.from_env()`` (reads
                        ``VARCO_REDIS_*`` env vars or ``redis://localhost:6379/0``).
            serializer: Serializer for the ``Event`` nested inside each
                        ``DeadLetterEntry``.  Must match the bus's serializer or a
                        redrive republishes the wrong wire format.  Defaults to
                        ``JsonEventSerializer()``; ``RedisDLQConfiguration``
                        forwards the container-bound ``Serializer[Event]``.

        Edge cases:
            - ``serializer=None`` → ``JsonEventSerializer()``, byte-identical to
              the behaviour before this parameter existed.
        """
        self._settings = settings or RedisEventBusSettings.from_env()
        # Key names derived from channel_prefix — namespaced to avoid
        # collisions with event channel keys.
        self._entries_key = f"{self._settings.channel_prefix}{_ENTRIES_SUFFIX}"
        self._queue_key = f"{self._settings.channel_prefix}{_QUEUE_SUFFIX}"

        # DESIGN: the nested Event's serializer is injected, not hard-coded
        #   A dead letter stores the event as bytes (`event_payload`), and those
        #   bytes must be readable by whoever redrives them.  Constructing
        #   `JsonEventSerializer()` here meant a CloudEvents-configured app wrote
        #   CloudEvents envelopes onto the bus but plain varco JSON into the DLQ —
        #   two wire formats for the same event, and a redrive that republished
        #   the wrong one.
        #   ✅ The DLQ now round-trips in whatever format the bus speaks.
        #   ✅ `None` keeps the previous behaviour exactly, so nothing changes for
        #      an app that never binds a serializer.
        #   ❌ A DLQ populated before a serializer swap holds the old format; its
        #      backlog must be drained before the swap (documented in the feature
        #      doc's migration timeline).
        self._serializer: Serializer[Event] = serializer or JsonEventSerializer()

        # Redis client created lazily in connect() — must not be instantiated
        # outside a running event loop (redis.asyncio requirement).
        self._redis: Any | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """
        Open the Redis connection.  Idempotent.

        Must be called before ``push()``, ``pop_batch()``, ``ack()``, or ``count()``.

        Raises:
            ConnectionError: (redis.asyncio) If Redis is unreachable.
        """
        if self._redis is not None:
            return
        self._redis = aioredis.from_url(
            self._settings.url,
            decode_responses=False,  # We need raw bytes for JSON payload
            socket_timeout=self._settings.socket_timeout,
            **self._settings.redis_kwargs,
        )
        _logger.info(
            "RedisDLQ connected (url=%s, entries_key=%r, queue_key=%r)",
            self._settings.url,
            self._entries_key,
            self._queue_key,
        )

    async def disconnect(self) -> None:
        """
        Close the Redis connection.  Idempotent.

        Edge cases:
            - Calling before ``connect()`` is a no-op.
        """
        if self._redis is None:
            return
        await self._redis.aclose()
        self._redis = None
        _logger.info("RedisDLQ disconnected.")

    async def __aenter__(self) -> RedisDLQ:
        """Support ``async with RedisDLQ(...) as dlq:`` usage."""
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Disconnect on context manager exit."""
        await self.disconnect()

    # ── AbstractDeadLetterQueue interface ─────────────────────────────────────

    async def push(self, entry: DeadLetterEntry) -> None:
        """
        Serialize ``entry`` and store it in the Redis Hash + Sorted Set.

        Uses a Redis pipeline so both HSET and ZADD execute atomically —
        no partial writes where the entry is in the Hash but not the Sorted Set.

        Args:
            entry: The ``DeadLetterEntry`` to store.

        Edge cases:
            - ``push()`` NEVER raises — all exceptions are swallowed and logged.
              This is a hard contract: callers (retry wrapper) must not be
              interrupted by DLQ failures.
            - If the Redis connection is not open (``connect()`` not called),
              a warning is logged and the entry is silently dropped.
            - Duplicate ``entry_id`` calls overwrite the existing entry in the
              Hash and update the Sorted Set score — idempotent push.

        Async safety: ✅ Each push uses a pipeline — no interleaving.
        """
        try:
            if self._redis is None:
                _logger.warning(
                    "RedisDLQ.push() called before connect() — entry dropped "
                    "(entry_id=%s, handler=%r).",
                    entry.entry_id,
                    entry.handler_name,
                )
                return

            payload = self._serialize_entry(entry)
            # Score = Unix timestamp of last_failed_at — smallest score = oldest
            # failure = popped first (FIFO within DLQ).
            score = entry.last_failed_at.timestamp()

            # Pipeline executes HSET + ZADD atomically (no MULTI/EXEC needed
            # here since both commands touch different keys — pipeline reduces
            # round-trip latency vs two separate awaits).
            async with self._redis.pipeline(transaction=False) as pipe:
                await pipe.hset(self._entries_key, str(entry.entry_id), payload)
                await pipe.zadd(self._queue_key, {str(entry.entry_id): score})
                await pipe.execute()

            _logger.debug(
                "RedisDLQ.push: stored entry_id=%s handler=%r error=%r",
                entry.entry_id,
                entry.handler_name,
                entry.error_type,
            )

        except Exception as exc:  # noqa: BLE001 — push MUST NOT propagate
            _logger.error(
                "RedisDLQ.push() failed unexpectedly — entry dropped (entry_id=%s): %s",
                entry.entry_id,
                exc,
                exc_info=True,
            )

    async def pop_batch(self, *, limit: int = 10) -> list[DeadLetterEntry]:
        """
        Return up to ``limit`` unacknowledged entries from the DLQ, oldest-first.

        Entries are NOT removed — they remain in the Hash + Sorted Set until
        ``ack()`` is called.  This gives at-least-once relay semantics: the relay
        that crashes mid-processing will re-read the same entries on restart.

        Args:
            limit: Maximum number of entries to return.  Must be ≥ 1.

        Returns:
            List of ``DeadLetterEntry`` objects, oldest-first.
            Empty list if the DLQ is empty.

        Raises:
            ValueError: If ``limit`` < 1.
            RuntimeError: If called before ``connect()``.

        Edge cases:
            - Entries whose Hash payload is missing (corrupted Redis state) are
              logged at WARNING and skipped — the Sorted Set entry is NOT removed
              automatically.  Use ``ack(entry_id)`` to remove orphaned entries.
            - Deserialization failures are logged and the entry is skipped.
            - Concurrent ``pop_batch()`` calls will return the same entries —
              there is no "claim" mechanism.  Use a single relay instance per DLQ.

        Async safety: ✅ Separate ZRANGE + HMGET (two round-trips).
        """
        if limit < 1:
            raise ValueError(f"pop_batch limit must be ≥ 1, got {limit}.")
        if self._redis is None:
            raise RuntimeError(
                "RedisDLQ.pop_batch() called before connect(). "
                "Call await dlq.connect() or use 'async with dlq' first."
            )

        # ZRANGE with BYSCORE would give a time range; here we just want the
        # N oldest entries regardless of their score — ZRANGE 0 N-1 (rank-based).
        entry_id_bytes: list[bytes] = await self._redis.zrange(self._queue_key, 0, limit - 1)

        if not entry_id_bytes:
            # Queue is empty — common case in low-failure systems.
            return []

        entry_ids = [b.decode("utf-8") for b in entry_id_bytes]

        # Fetch payloads from Hash in a single round-trip (HMGET).
        payloads: list[bytes | None] = await self._redis.hmget(self._entries_key, *entry_ids)

        entries: list[DeadLetterEntry] = []
        for entry_id_str, payload in zip(entry_ids, payloads):
            if payload is None:
                # Entry is in Sorted Set but missing from Hash — corrupted state.
                # Log and skip; caller may ack the entry_id to clean up the
                # Sorted Set entry.
                _logger.warning(
                    "RedisDLQ.pop_batch: entry_id=%s is in queue but has no "
                    "Hash payload — skipping (corrupted state?). "
                    "Call ack('%s') to remove orphaned Sorted Set entry.",
                    entry_id_str,
                    entry_id_str,
                )
                continue

            try:
                entry = self._deserialize_entry(entry_id_str, payload)
                entries.append(entry)
            except Exception as exc:  # noqa: BLE001
                _logger.warning(
                    "RedisDLQ.pop_batch: failed to deserialize entry_id=%s: %s — skipping.",
                    entry_id_str,
                    exc,
                    exc_info=True,
                )

        _logger.debug(
            "RedisDLQ.pop_batch: returned %d entries (limit=%d)",
            len(entries),
            limit,
        )
        return entries

    async def ack(self, entry_id: UUID) -> None:
        """
        Remove ``entry_id`` from both the Hash and the Sorted Set atomically.

        After this call the entry will no longer be returned by ``pop_batch()``.
        Idempotent — calling with an unknown ``entry_id`` is a silent no-op.

        Args:
            entry_id: The ``DeadLetterEntry.entry_id`` to acknowledge.

        Edge cases:
            - If only one of the two structures contains the entry (corrupted
              state), the remove is applied to whichever structure has it —
              both HDEL and ZREM are attempted regardless.
            - ``ack()`` on an unknown ID returns without error — at-least-once
              delivery means a concurrent relay may have acked it already.

        Async safety: ✅ Uses a pipeline for atomic HDEL + ZREM.
        """
        if self._redis is None:
            _logger.warning(
                "RedisDLQ.ack() called before connect() — noop (entry_id=%s).",
                entry_id,
            )
            return

        entry_id_str = str(entry_id)

        # Use a pipeline to remove from both structures in one round-trip.
        async with self._redis.pipeline(transaction=False) as pipe:
            await pipe.hdel(self._entries_key, entry_id_str)
            await pipe.zrem(self._queue_key, entry_id_str)
            await pipe.execute()

        _logger.debug("RedisDLQ.ack: acknowledged entry_id=%s", entry_id)

    async def count(self) -> int:
        """
        Return the number of unacknowledged entries in the DLQ.

        Uses ``ZCARD`` — O(1), exact count.

        Returns:
            Non-negative integer.  ``0`` if the DLQ is empty.

        Raises:
            RuntimeError: If called before ``connect()``.

        Edge cases:
            - Count reflects the Sorted Set membership, which may differ from
              the Hash membership if the state is corrupted.  In normal
              operation the two are always in sync.

        Async safety: ✅ Awaits single ZCARD command.
        """
        if self._redis is None:
            raise RuntimeError(
                "RedisDLQ.count() called before connect(). "
                "Call await dlq.connect() or use 'async with dlq' first."
            )
        # redis-py ships no py.typed marker — zcard() is structurally Any.
        cardinality: int = await self._redis.zcard(self._queue_key)
        return cardinality

    # ── Serialization helpers ──────────────────────────────────────────────────

    def _serialize_entry(self, entry: DeadLetterEntry) -> bytes:
        """
        Serialize a ``DeadLetterEntry`` to JSON bytes for Redis storage.

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
            # entry_id stored separately for reconstruction in _deserialize_entry
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
            # Plan 009, Phase 6 (R4) — tenant scoping.
            "tenant_id": entry.tenant_id,
        }
        return json.dumps(data).encode("utf-8")

    def _deserialize_entry(self, entry_id_str: str, payload: bytes) -> DeadLetterEntry:
        """
        Deserialize a Redis payload back to a ``DeadLetterEntry``.

        Args:
            entry_id_str: The entry_id string from the Sorted Set member.
            payload:      Raw JSON bytes from the Redis Hash.

        Returns:
            A fully populated ``DeadLetterEntry``.

        Raises:
            KeyError:    If a required JSON field is missing.
            ValueError:  If a field has an unexpected type or format.

        Edge cases:
            - ``first_failed_at`` and ``last_failed_at`` are parsed as ISO-8601
              strings with timezone.  If stored without timezone (legacy data),
              they are treated as UTC.
        """
        data: dict[str, Any] = json.loads(payload.decode("utf-8"))

        # Re-deserialize the embedded Event payload — uses self._serializer
        # which resolves the event class via __event_type__ registry lookup.
        event = self._serializer.deserialize(data["event_payload"].encode("utf-8"))

        def _parse_dt(value: str) -> datetime:
            """Parse ISO-8601 datetime, defaulting to UTC if no tz info."""
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt

        return DeadLetterEntry(
            entry_id=UUID(entry_id_str),
            event=event,
            channel=data["channel"],
            handler_name=data["handler_name"],
            error_type=data["error_type"],
            error_message=data["error_message"],
            attempts=data["attempts"],
            first_failed_at=_parse_dt(data["first_failed_at"]),
            last_failed_at=_parse_dt(data["last_failed_at"]),
            tenant_id=data.get("tenant_id"),
        )

    # ── Plan 009 additions — retention (R3), redrive (R1), tenancy (R4) ────────

    async def get(self, entry_id: UUID) -> DeadLetterEntry | None:
        """Fetch one entry by id (Hash lookup) without removing it."""
        if self._redis is None:
            raise RuntimeError("RedisDLQ.get() called before connect().")
        results = await self._redis.hmget(self._entries_key, str(entry_id))
        payload = results[0] if results else None
        if payload is None:
            return None
        return self._deserialize_entry(str(entry_id), payload)

    async def list_entries(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        channel: str | None = None,
        source: Any | None = None,
        tenant_id: str | None = None,
        older_than: datetime | None = None,
        newer_than: datetime | None = None,
    ) -> list[DeadLetterEntry]:
        """
        Non-destructive, filtered, paginated read.

        Filtering is applied in Python after a full ``ZRANGE`` scan — Redis
        has no secondary index on channel/source/tenant here. Acceptable for
        an admin/CLI browse surface; not intended for hot-path use.
        """
        if self._redis is None:
            raise RuntimeError("RedisDLQ.list_entries() called before connect().")
        older_than = _coerce_utc(older_than)
        newer_than = _coerce_utc(newer_than)
        entry_id_bytes: list[bytes] = await self._redis.zrange(self._queue_key, 0, -1)
        entry_ids = [b.decode("utf-8") for b in entry_id_bytes]
        if not entry_ids:
            return []
        payloads = await self._redis.hmget(self._entries_key, *entry_ids)

        entries: list[DeadLetterEntry] = []
        for entry_id_str, payload in zip(entry_ids, payloads):
            if payload is None:
                continue
            try:
                entry = self._deserialize_entry(entry_id_str, payload)
            except Exception as exc:  # noqa: BLE001 — skip corrupted entries, matches pop_batch()
                _logger.warning(
                    "RedisDLQ.list_entries: failed to deserialize entry_id=%s: %s — skipping.",
                    entry_id_str,
                    exc,
                )
                continue
            if channel is not None and entry.channel != channel:
                continue
            if source is not None and entry.source != source:
                continue
            if tenant_id is not None and entry.tenant_id != tenant_id:
                continue
            if older_than is not None and entry.last_failed_at >= older_than:
                continue
            if newer_than is not None and entry.last_failed_at <= newer_than:
                continue
            entries.append(entry)

        return entries[offset : offset + limit]

    async def delete_where(
        self,
        *,
        older_than: datetime | None = None,
        source: Any | None = None,
        channel: str | None = None,
        tenant_id: str | None = None,
        limit: int | None = None,
    ) -> int:
        """Bulk delete matching entries — one predicate is required."""
        if older_than is None and source is None and channel is None and tenant_id is None:
            raise ValueError(
                "delete_where() requires at least one predicate "
                "(older_than/source/channel/tenant_id) — refusing to delete "
                "every entry."
            )
        matching = await self.list_entries(
            limit=limit or 1_000_000,
            channel=channel,
            source=source,
            tenant_id=tenant_id,
            older_than=older_than,
        )
        for entry in matching:
            await self.ack(entry.entry_id)
        return len(matching)

    async def count_by_channel(self) -> dict[str, int]:
        """Return ``{channel: count}`` via a full scan of current entries."""
        entries = await self.list_entries(limit=1_000_000)
        counts: dict[str, int] = {}
        for entry in entries:
            counts[entry.channel] = counts.get(entry.channel, 0) + 1
        return counts

    def __repr__(self) -> str:
        return (
            f"RedisDLQ("
            f"url={self._settings.url!r}, "
            f"queue_key={self._queue_key!r}, "
            f"connected={self._redis is not None})"
        )


# ── DI Configuration ──────────────────────────────────────────────────────────


@Configuration
class RedisDLQConfiguration:
    """
    Providify ``@Configuration`` that wires ``RedisDLQ`` into the container.

    Provides:
        ``AbstractDeadLetterQueue`` — connected ``RedisDLQ`` singleton.

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

    Example (bus + DLQ)::

        from varco_redis.di import bootstrap

        container = bootstrap(DIContainer())   # scans varco_redis bus singletons
        await container.ainstall(RedisDLQConfiguration)

        # Wire into an EventConsumer handler:
        class OrderConsumer(EventConsumer):
            @listen(
                OrderPlacedEvent,
                retry_policy=RetryPolicy(max_attempts=3),
                dlq=await container.aget(AbstractDeadLetterQueue),
            )
            async def on_order_placed(self, event: OrderPlacedEvent) -> None:
                ...

    Example (DLQ only, no bus)::

        container = DIContainer()
        await container.ainstall(RedisDLQConfiguration)
        dlq = await container.aget(AbstractDeadLetterQueue)
        entries = await dlq.pop_batch(limit=10)
    """

    @Fallback
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    async def redis_dlq(
        self,
        settings: Annotated[RedisEventBusSettings | None, InjectMeta(optional=True)] = None,
        serializer: Annotated[Serializer[Event] | None, InjectMeta(optional=True)] = None,
    ) -> AbstractDeadLetterQueue:
        """
        Create, connect, and return the ``RedisDLQ`` singleton.

        Args:
            settings: Optionally-injected ``RedisEventBusSettings`` — reuses
                        the bus's own settings binding when a bus was already
                        registered via ``scan()``/``bootstrap()``; ``None``
                        when nothing is bound, in which case this falls back
                        to ``RedisEventBusSettings.from_env()`` (Plan 045 —
                        replaces a second, now-deleted ``@Provider`` of this
                        settings type that used to tie with the bus's own
                        under ``@Fallback``).
            serializer: Optionally-injected ``Serializer[Event]``.  Forwarded so the
                        DLQ stores dead letters in the same wire format the bus
                        publishes; ``None`` when nothing is bound, in which case
                        the DLQ falls back to ``JsonEventSerializer()``.

        Returns:
            A connected ``RedisDLQ`` bound to ``AbstractDeadLetterQueue``.

        Raises:
            ConnectionError: (redis.asyncio) If Redis is unreachable at startup.
        """
        settings = settings or RedisEventBusSettings.from_env()
        _logger.info(
            "RedisDLQConfiguration: connecting RedisDLQ (url=%s)",
            settings.url,
        )
        dlq = RedisDLQ(settings, serializer=serializer)
        await dlq.connect()
        return dlq

    @Disposes(AbstractDeadLetterQueue)
    async def close_dlq(self, dlq: RedisDLQ) -> None:
        """
        Disconnect the ``RedisDLQ`` produced by ``redis_dlq`` on shutdown.

        DESIGN: ``@Disposes`` over relying on ``@PreDestroy``
            ✅ Providify never invokes ``@PreDestroy`` on a ``@Provider``-produced
               instance — ``@Disposes`` is upstream's own supported teardown
               mechanism (Plan 024 / C2). ``RedisDLQ`` carries no
               ``@PreDestroy`` at all (Tier B — invisible to providify's
               ``UNREACHABLE_PRE_DESTROY`` detector), so only this behavioural
               disposer test can catch a regression here.

        Args:
            dlq: The ``RedisDLQ`` instance this configuration produced.
                Typed concretely (not ``AbstractDeadLetterQueue``) because
                ``disconnect()`` is a `RedisDLQ`-specific method, not part of
                the ABC — ``@Disposes(AbstractDeadLetterQueue)`` above is the
                *dispatch key* providify matches against the bound interface;
                it is independent of this parameter's static type.

        Async safety: ✅ Awaited by providify's ``_adispose`` during
            ``container.ashutdown()``.
        """
        await dlq.disconnect()


# ── Public API ────────────────────────────────────────────────────────────────


__all__ = [
    "RedisDLQ",
    "RedisDLQConfiguration",
]
