"""
varco_nats.dlq
==============
NATS JetStream-backed implementation of ``AbstractDeadLetterQueue``.

``NatsDLQ`` publishes failed event entries to a dedicated JetStream DLQ stream
and supports replay/monitoring via a durable pull consumer.

Architecture
------------
::

    Handler exhausts retries
        ↓
    dlq.push(DeadLetterEntry)
        → JetStreamContext.publish(subject="varco.__dlq__", payload=JSON bytes)

    DLQ relay / monitoring
        → pull consumer .fetch(batch=...) [durable: __varco_dlq_relay__]
        → pop_batch() returns DeadLetterEntry list
        → relay processes each entry
        → ack(entry_id) acknowledges the JetStream message

DESIGN: dedicated DLQ stream with WorkQueue retention
    ✅ JetStream persists entries durably — no data loss on relay restart.
    ✅ WorkQueue retention deletes a message the moment it is acked, so
       ``count()`` returns the EXACT number of pending entries — unlike the
       Kafka DLQ which cannot compute lag without an AdminClient and returns -1.
    ✅ ``push()`` is fire-and-forget (publish returns after the JetStream ack).
    ❌ WorkQueue retention permits only ONE consumer per subject — a single
       relay is supported.  Running multiple independent relays would require
       LIMITS retention and is intentionally not supported here.

Subject / stream naming
-----------------------
Default DLQ subject: ``{subject_prefix}.{channel_prefix}__dlq__``
Default DLQ stream:  ``{stream_name}-dlq``

Both can be overridden via constructor arguments.

Acknowledgement
---------------
``pop_batch()`` fetches messages WITHOUT acking — they remain in the stream
until ``ack()`` is called.  A relay crash between ``pop_batch()`` and ``ack()``
redelivers the entry (at-least-once relay semantics).

Usage (push-only, most common)::

    dlq = NatsDLQ(settings=NatsEventBusSettings(servers="nats://localhost:4222"))
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

    async with NatsDLQ(settings=...) as dlq:
        entries = await dlq.pop_batch(limit=10)
        for entry in entries:
            await alert_ops(entry)
            await dlq.ack(entry.entry_id)

Thread safety:  ❌ Not thread-safe.  Use from a single event loop.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🔍 https://docs.nats.io/nats-concepts/jetstream/streams#retentionpolicy
  JetStream WorkQueue retention
- 🔍 https://nats-io.github.io/nats.py/ — nats-py pull_subscribe / fetch
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from nats import connect
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import RetentionPolicy
from nats.js.errors import NotFoundError
from providify import Configuration, Disposes, Fallback, InjectMeta, Provider
from varco_core.di import BACKEND_PRIORITY
from varco_core.di.select import selected_by
from varco_core.event.base import Event
from varco_core.event.dlq import (
    AbstractDeadLetterQueue,
    DeadLetterEntry,
    DeadLetterSource,
)
from varco_core.event.serializer import JsonEventSerializer
from varco_core.serialization import Serializer

from varco_nats.config import NatsEventBusSettings

_logger = logging.getLogger(__name__)

# Default durable name for the DLQ relay pull consumer.  Double underscores
# mark this as an internal varco consumer.
_DEFAULT_DLQ_DURABLE = "__varco_dlq_relay__"

# Default DLQ subject suffix.  Full subject = {subject_prefix}.{channel_prefix}__dlq__
_DEFAULT_DLQ_SUFFIX = "__dlq__"

# Default DLQ stream name suffix.  Full name = {stream_name}-dlq
_DEFAULT_DLQ_STREAM_SUFFIX = "-dlq"

# How long pop_batch() waits for messages before returning an empty list.
# Short enough to be responsive; long enough not to hammer NATS with polls.
_FETCH_TIMEOUT_SECONDS = 0.5

# How long ack() waits for the JetStream server to CONFIRM the ack.
# ack() must not return before the message is actually removed — see the
# DESIGN block on NatsDLQ.ack.
_ACK_TIMEOUT_SECONDS = 2.0


# ── NatsDLQ ───────────────────────────────────────────────────────────────────


class NatsDLQ(AbstractDeadLetterQueue):
    """
    NATS JetStream-backed ``AbstractDeadLetterQueue`` using a dedicated stream.

    **Push path** (primary use case): failed event entries are serialized to
    JSON and published to ``{subject_prefix}.{channel_prefix}__dlq__``.

    **Pop/ack path** (relay use case): a durable pull consumer fetches entries
    in batches; ``ack()`` acknowledges each processed message, which (under
    WorkQueue retention) deletes it from the stream.

    Args:
        settings:    NATS connection settings.  Defaults to
                     ``NatsEventBusSettings.from_env()``.
        dlq_subject: Full DLQ subject.  Defaults to
                     ``{subject_prefix}.{channel_prefix}__dlq__``.
        dlq_stream:  DLQ stream name.  Defaults to ``{stream_name}-dlq``.
        dlq_durable: Durable name for the relay pull consumer.  Defaults to
                     ``__varco_dlq_relay__``.

    Lifecycle:
        Call ``await dlq.start()`` before use, ``await dlq.stop()`` when done.
        Or use as an async context manager.

    Thread safety:  ❌ Not thread-safe.  Use from a single event loop.
    Async safety:   ✅ All methods are ``async def``.

    Edge cases:
        - ``push()`` NEVER raises — all exceptions are swallowed and logged.
        - ``pop_batch()`` waits up to 0.5 s for messages; an empty DLQ yields
          an empty list.
        - ``count()`` returns the EXACT pending-entry count (WorkQueue
          retention removes acked messages from the stream).
        - WorkQueue retention allows only one consumer — a single relay.

    Example::

        async with NatsDLQ(settings=NatsEventBusSettings()) as dlq:
            await dlq.push(DeadLetterEntry.from_failure(...))

            entries = await dlq.pop_batch(limit=10)
            for entry in entries:
                print(f"DLQ: {entry.handler_name} failed — {entry.error_message}")
                await dlq.ack(entry.entry_id)
    """

    def __init__(
        self,
        settings: NatsEventBusSettings | None = None,
        *,
        dlq_subject: str | None = None,
        dlq_stream: str | None = None,
        dlq_durable: str = _DEFAULT_DLQ_DURABLE,
        serializer: Serializer[Event] | None = None,
    ) -> None:
        """
        Args:
            settings:    NATS connection settings.  Defaults to
                         ``NatsEventBusSettings.from_env()``.
            dlq_subject: Override for the DLQ subject.  If ``None``, defaults to
                         ``{subject_prefix}.{channel_prefix}__dlq__``.
            dlq_stream:  Override for the DLQ stream name.  If ``None``, defaults
                         to ``{stream_name}-dlq``.
            serializer:  Serializer for the ``Event`` nested inside each
                         ``DeadLetterEntry``.  Must match the bus's serializer or
                         a redrive republishes the wrong wire format.  Defaults to
                         ``JsonEventSerializer()``; ``NatsDLQConfiguration``
                         forwards the container-bound ``Serializer[Event]``.
            dlq_durable: Durable name for the relay pull consumer.

        Edge cases:
            - When ``dlq_subject`` / ``dlq_stream`` are provided, the
              ``subject_prefix`` / ``stream_name`` defaults are NOT applied —
              the given values are used verbatim.
        """
        self._settings = settings or NatsEventBusSettings.from_env()

        # DLQ subject — defaults reuse the bus's subject_prefix / channel_prefix
        # for namespace isolation on a shared NATS cluster.
        self._dlq_subject = (
            dlq_subject
            if dlq_subject is not None
            else (
                f"{self._settings.subject_prefix}."
                f"{self._settings.channel_prefix}{_DEFAULT_DLQ_SUFFIX}"
            )
        )
        self._dlq_stream = (
            dlq_stream
            if dlq_stream is not None
            else f"{self._settings.stream_name}{_DEFAULT_DLQ_STREAM_SUFFIX}"
        )
        self._dlq_durable = dlq_durable

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

        # NATS client + JetStream context — created in start().
        self._nc: Any | None = None
        self._js: Any | None = None
        self._started = False

        # Pull consumer used by pop_batch() / ack() — created lazily in
        # _ensure_consumer() so push-only users don't pay for a consumer.
        self._consumer: Any | None = None

        # In-flight tracking: maps entry_id (str) → the JetStream Msg so ack()
        # can acknowledge it later.  Not persisted — on a restart entries are
        # re-fetched (at-least-once relay semantics).
        self._in_flight: dict[str, Any] = {}

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Connect to NATS and ensure the DLQ stream exists.  Idempotent.

        The pull consumer is created lazily on the first ``pop_batch()`` call to
        avoid opening a consumer for push-only use cases.

        Raises:
            NoServersError: (nats-py) If the configured servers are unreachable.
            APIError:       (nats-py) If JetStream is unavailable.
        """
        if self._started:
            return

        self._nc = await connect(
            servers=self._settings.to_servers_list(),
            **self._settings.connect_kwargs,
        )
        self._js = self._nc.jetstream()
        await self._ensure_dlq_stream()

        self._started = True
        _logger.info(
            "NatsDLQ started (servers=%s, dlq_stream=%r, dlq_subject=%r)",
            self._settings.servers,
            self._dlq_stream,
            self._dlq_subject,
        )

    async def stop(self) -> None:
        """
        Close the NATS connection.  Idempotent.

        Edge cases:
            - Calling before ``start()`` is a no-op.
            - In-flight entries tracked in ``_in_flight`` are discarded — they
              are re-fetched on the next ``pop_batch()`` after restart.
        """
        if not self._started:
            return

        if self._nc is not None:
            await self._nc.close()

        self._nc = None
        self._js = None
        self._consumer = None
        self._started = False
        self._in_flight.clear()
        _logger.info("NatsDLQ stopped.")

    async def __aenter__(self) -> NatsDLQ:
        """Support ``async with NatsDLQ(...) as dlq:`` usage."""
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Close the connection on context manager exit."""
        await self.stop()

    # ── AbstractDeadLetterQueue interface ─────────────────────────────────────

    async def push(self, entry: DeadLetterEntry) -> None:
        """
        Serialize ``entry`` and publish it to the JetStream DLQ subject.

        Uses ``JetStreamContext.publish`` — blocks until JetStream acknowledges
        the publish.  A successful ``push()`` means the entry is durably stored.

        Args:
            entry: The ``DeadLetterEntry`` to store.

        Edge cases:
            - ``push()`` NEVER raises — all exceptions are swallowed and logged.
              This is a hard contract: callers (the retry wrapper) must not be
              interrupted by DLQ failures.
            - If the DLQ is not started, a warning is logged and the entry is
              dropped.

        Async safety: ✅ Awaits ``js.publish``.
        """
        try:
            if self._js is None:
                _logger.warning(
                    "NatsDLQ.push() called before start() — entry dropped "
                    "(entry_id=%s, handler=%r).",
                    entry.entry_id,
                    entry.handler_name,
                )
                return

            payload = self._serialize_entry(entry)
            await self._js.publish(self._dlq_subject, payload)
            _logger.debug(
                "NatsDLQ.push: sent entry_id=%s to subject=%r (handler=%r)",
                entry.entry_id,
                self._dlq_subject,
                entry.handler_name,
            )

        except Exception as exc:  # noqa: BLE001 — push MUST NOT propagate
            _logger.error(
                "NatsDLQ.push() failed — entry dropped (entry_id=%s): %s",
                entry.entry_id,
                exc,
                exc_info=True,
            )

    async def pop_batch(self, *, limit: int = 10) -> list[DeadLetterEntry]:
        """
        Return up to ``limit`` unacknowledged entries from the DLQ stream.

        Uses a durable pull consumer's ``fetch()`` with a short timeout.
        Entries are NOT acknowledged — they remain in the stream until
        ``ack()`` is called for each one.

        Args:
            limit: Maximum number of entries to return.  Must be ≥ 1.

        Returns:
            List of ``DeadLetterEntry`` objects.  Empty list if no messages
            are available within the fetch timeout.

        Raises:
            ValueError:   If ``limit`` < 1.
            RuntimeError: If called before ``start()``.

        Edge cases:
            - May return fewer than ``limit`` entries even when the DLQ is not
              empty (``fetch`` returns whatever is immediately available).
            - Deserialization failures are logged and the bad message is
              terminated (``msg.term()``) so it is not redelivered forever.

        Async safety: ✅ Awaits ``consumer.fetch()``.
        """
        if limit < 1:
            raise ValueError(f"pop_batch limit must be ≥ 1, got {limit}.")
        if not self._started:
            raise RuntimeError(
                "NatsDLQ.pop_batch() called before start(). "
                "Call await dlq.start() or use 'async with dlq' first."
            )

        consumer = await self._ensure_consumer()

        try:
            msgs = await consumer.fetch(limit, timeout=_FETCH_TIMEOUT_SECONDS)
        except NatsTimeoutError:
            # No messages available within the timeout — an empty DLQ.
            return []

        entries: list[DeadLetterEntry] = []
        for msg in msgs:
            try:
                entry = self._deserialize_entry(msg.data)
            except Exception as exc:  # noqa: BLE001
                _logger.warning(
                    "NatsDLQ.pop_batch: failed to deserialize a DLQ message: %s — terminating it.",
                    exc,
                    exc_info=True,
                )
                # term() tells JetStream to drop this poison message — it is
                # unparseable, so redelivery would loop forever.
                await self._safe_term(msg)
                continue

            # Track the message so ack() can acknowledge it later.
            self._in_flight[str(entry.entry_id)] = msg
            entries.append(entry)

        _logger.debug(
            "NatsDLQ.pop_batch: returned %d entries (limit=%d)",
            len(entries),
            limit,
        )
        return entries

    async def ack(self, entry_id: UUID) -> None:
        """
        Acknowledge the JetStream message associated with ``entry_id``.

        Under WorkQueue retention, acknowledging a message deletes it from the
        DLQ stream — it will not be returned by a future ``pop_batch()``.

        DESIGN: ``ack_sync()``, never the fire-and-forget ``ack()``.
            nats-py's ``Msg.ack()`` only publishes to the reply subject and
            returns immediately, so the message can still be in the stream when
            this coroutine returns. That breaks
            ``AbstractDeadLetterQueue.ack``'s postcondition ("Removes the entry
            from the DLQ so it is not returned by future ``pop_batch`` calls")
            — a read-after-ack sees stale state — and, more seriously, lets a
            process exiting straight after ``ack()`` lose the ack entirely, so
            ``DlqRedriver``'s publish-then-ack policy redelivers the dead
            letter. ``ack_sync()`` does a request/reply and waits for the
            server to confirm.
            ✅ The postcondition holds on return; redrive is not duplicated.
            ❌ One network round trip per ack instead of zero — the correct
               trade for a durability primitive.

        Args:
            entry_id: The ``DeadLetterEntry.entry_id`` to acknowledge.

        **Never raises** — a failed acknowledgement is logged, never
        propagated, so it cannot abort a relay/redrive loop mid-batch.

        Edge cases:
            - Calling with an unknown ``entry_id`` (not returned by a prior
              ``pop_batch()``, or already acked) is a silent no-op.
            - If the server does not confirm within
              ``_ACK_TIMEOUT_SECONDS``, the entry is kept in ``_in_flight`` so
              a later ``ack()`` retries it. A duplicate ack is harmless to
              JetStream; a silently dropped entry is not.
            - If the process restarts between ``pop_batch()`` and ``ack()``, the
              message is not acked — it is re-fetched on the next ``pop_batch()``
              (at-least-once relay semantics).

        Async safety: ✅ Awaits ``msg.ack_sync()``.
        """
        entry_id_str = str(entry_id)
        msg = self._in_flight.pop(entry_id_str, None)

        if msg is None:
            # Not in-flight — already acked or not fetched in this session.
            _logger.debug("NatsDLQ.ack: entry_id=%s not in-flight — noop.", entry_id)
            return

        try:
            await msg.ack_sync(timeout=_ACK_TIMEOUT_SECONDS)
        except Exception:
            # Keep the entry acknowledgeable: we do not know whether the server
            # processed the ack, and a re-ack is harmless while a dropped entry
            # would be redelivered forever.
            self._in_flight[entry_id_str] = msg
            _logger.warning(
                "NatsDLQ.ack: server did not confirm ack for entry_id=%s — "
                "left in-flight for retry.",
                entry_id,
                exc_info=True,
            )
            return
        _logger.debug("NatsDLQ.ack: acknowledged entry_id=%s", entry_id)

    async def count(self) -> int:
        """
        Return the exact number of unacknowledged entries in the DLQ.

        WorkQueue retention deletes a message the moment it is acked, so the
        DLQ stream's message count IS the pending-entry count.

        Returns:
            Non-negative integer.  ``0`` if the DLQ is empty or the DLQ stream
            does not exist yet.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Entries fetched by ``pop_batch()`` but not yet acked are still
              counted — they remain in the stream until acknowledged.
        """
        if not self._started:
            raise RuntimeError(
                "NatsDLQ.count() called before start(). "
                "Call await dlq.start() or use 'async with dlq' first."
            )
        try:
            info = await self._js.stream_info(self._dlq_stream)  # type: ignore[union-attr]
        except NotFoundError:
            # No DLQ stream yet → nothing has ever been pushed.
            return 0
        return int(info.state.messages)

    async def delete_where(
        self,
        *,
        older_than: datetime | None = None,
        source: DeadLetterSource | Sequence[DeadLetterSource] | None = None,
        channel: str | None = None,
        tenant_id: str | None = None,
        limit: int | None = None,
    ) -> int:
        """
        Always raises — JetStream has no per-message delete by predicate.

        WHY the no-predicate check runs first: the ABC's contract
        (``AbstractDeadLetterQueue.delete_where``) requires refusing an
        unbounded "delete everything" call with ``ValueError`` *before* a
        backend gets to say whether it supports predicate-based deletion at
        all — a backend that skips straight to ``NotImplementedError``
        regardless of arguments silently masks the "no predicate given"
        footgun the ABC exists to catch. This mirrors the sibling fix in
        ``KafkaDLQ`` (KI-2) — same class of ABC-contract deviation.

        Raises:
            ValueError: no predicate at all was given — refuses to silently
                delete every entry (checked before the backend-support check,
                per the ABC contract).
            NotImplementedError: a predicate was given, naming ``MaxAge``
                (the stream-level retention setting) as the correct
                mechanism (RD-4) — JetStream streams are not randomly
                deletable.
        """
        if older_than is None and source is None and channel is None and tenant_id is None:
            raise ValueError(
                "delete_where() requires at least one predicate "
                "(older_than/source/channel/tenant_id) — refusing to delete "
                "every entry."
            )
        raise NotImplementedError(
            "NatsDLQ does not support delete_where() — JetStream streams are "
            "not randomly deletable. Configure the DLQ stream's MaxAge "
            "instead (JetStream's own retention mechanism)."
        )

    # ── Stream / consumer lifecycle helpers ───────────────────────────────────

    async def _ensure_dlq_stream(self) -> None:
        """
        Create the DLQ JetStream stream if it does not already exist.

        The stream uses WorkQueue retention so acked messages are removed —
        this is what makes ``count()`` an exact pending-entry count.

        Edge cases:
            - Idempotent — an existing stream is left untouched.
        """
        assert self._js is not None  # guaranteed by start() ordering
        try:
            await self._js.stream_info(self._dlq_stream)
            _logger.debug("DLQ stream %r already exists.", self._dlq_stream)
            return
        except NotFoundError:
            pass

        await self._js.add_stream(
            name=self._dlq_stream,
            subjects=[self._dlq_subject],
            # WorkQueue: a message is deleted once acked — pop+ack DLQ semantics.
            retention=RetentionPolicy.WORK_QUEUE,
        )
        _logger.info(
            "Created DLQ stream %r (subject=%r, retention=workqueue)",
            self._dlq_stream,
            self._dlq_subject,
        )

    async def _ensure_consumer(self) -> Any:
        """
        Create and return the durable pull consumer on first use.

        DESIGN: lazy pull-consumer creation
            ✅ Push-only users (the common case) never open a consumer.
            ✅ The consumer is created inside a running event loop.
            ❌ The first ``pop_batch()`` pays the consumer-setup latency.

        Returns:
            The nats-py ``PullSubscription`` for the DLQ stream.

        Async safety: ✅ Idempotent — returns the cached consumer if present.
        """
        if self._consumer is not None:
            return self._consumer

        # A durable pull subscription so the relay resumes from its last
        # position after a restart.
        self._consumer = await self._js.pull_subscribe(  # type: ignore[union-attr]
            self._dlq_subject,
            durable=self._dlq_durable,
            stream=self._dlq_stream,
        )
        _logger.info(
            "NatsDLQ pull consumer started (durable=%r, stream=%r)",
            self._dlq_durable,
            self._dlq_stream,
        )
        return self._consumer

    async def _safe_term(self, msg: Any) -> None:
        """
        Terminate a poison message without ever propagating an error.

        Args:
            msg: The nats-py ``Msg`` to terminate.
        """
        try:
            await msg.term()
        except Exception as exc:  # noqa: BLE001 — best-effort poison handling
            _logger.warning("NatsDLQ: failed to terminate a poison message: %s", exc)

    # ── Serialization helpers ──────────────────────────────────────────────────

    def _serialize_entry(self, entry: DeadLetterEntry) -> bytes:
        """
        Serialize ``entry`` to UTF-8 JSON bytes for JetStream storage.

        The nested ``Event`` is serialized using ``JsonEventSerializer`` so it
        round-trips back to a typed ``Event`` on pop.

        Args:
            entry: The ``DeadLetterEntry`` to serialize.

        Returns:
            UTF-8 encoded JSON bytes.

        Edge cases:
            - Datetimes are stored as ISO-8601 strings (timezone-aware).
        """
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
            # Event serialized with its own type-aware serializer — self-describing.
            "event_payload": event_bytes.decode("utf-8"),
        }
        return json.dumps(data).encode("utf-8")

    def _deserialize_entry(self, payload: bytes) -> DeadLetterEntry:
        """
        Deserialize a JetStream message payload back to a ``DeadLetterEntry``.

        Args:
            payload: Raw JSON bytes from the JetStream message data.

        Returns:
            A fully populated ``DeadLetterEntry``.

        Raises:
            KeyError:   Missing required JSON field.
            ValueError: Malformed field (bad UUID, bad datetime, etc.).

        Edge cases:
            - Datetimes without timezone info are treated as UTC.
        """
        data: dict[str, Any] = json.loads(payload.decode("utf-8"))

        event = self._serializer.deserialize(data["event_payload"].encode("utf-8"))

        def _parse_dt(value: str) -> datetime:
            """Parse ISO-8601 string, defaulting to UTC if tz info is absent."""
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
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
            f"NatsDLQ("
            f"servers={self._settings.servers!r}, "
            f"dlq_stream={self._dlq_stream!r}, "
            f"dlq_subject={self._dlq_subject!r}, "
            f"started={self._started})"
        )


# ── DI Configuration ──────────────────────────────────────────────────────────


@Configuration
class NatsDLQConfiguration:
    """
    Providify ``@Configuration`` that wires ``NatsDLQ`` into the container.

    Provides:
        ``AbstractDeadLetterQueue`` — started ``NatsDLQ`` singleton.

    Reuses ``NatsEventBusSettings`` if already registered (via the
    ``@Singleton`` decorator scanned from ``varco_nats``).  Otherwise falls
    back to ``NatsEventBusSettings.from_env()``.

    DESIGN: separate @Configuration over folding the DLQ into the bus scan
        ✅ Services that only push to the DLQ don't need the full bus.
        ✅ Binds explicitly to ``AbstractDeadLetterQueue`` — no ambiguity.
        ❌ One extra ``ainstall()`` for the "bus + DLQ" case — acceptable.

    Thread safety:  ✅ Providify singletons are created once and cached.
    Async safety:   ✅ The provider is ``async def``.

    Example::

        container = DIContainer()
        container.scan("varco_nats", recursive=True)   # registers the bus
        await container.ainstall(NatsDLQConfiguration)  # registers the DLQ

        dlq = await container.aget(AbstractDeadLetterQueue)
    """

    @Fallback
    @selected_by("dlq", "nats")
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    async def nats_dlq(
        self,
        settings: Annotated[NatsEventBusSettings | None, InjectMeta(optional=True)] = None,
        serializer: Annotated[Serializer[Event] | None, InjectMeta(optional=True)] = None,
    ) -> AbstractDeadLetterQueue:
        """
        Create and start the ``NatsDLQ`` singleton.

        Args:
            settings: Optionally-injected ``NatsEventBusSettings`` — reuses
                        the bus's own settings binding when a bus was already
                        registered via ``scan()``/``bootstrap()``; ``None``
                        when nothing is bound, in which case this falls back
                        to ``NatsEventBusSettings.from_env()`` (Plan 045 —
                        replaces a second, now-deleted ``@Provider`` of this
                        settings type that used to tie with the bus's own
                        under ``@Fallback``).
            serializer: Optionally-injected ``Serializer[Event]``.  Forwarded so the
                        DLQ stores dead letters in the same wire format the bus
                        publishes; ``None`` when nothing is bound, in which case
                        the DLQ falls back to ``JsonEventSerializer()``.

        Returns:
            A started ``NatsDLQ`` bound to ``AbstractDeadLetterQueue``.

        Raises:
            NoServersError: (nats-py) If the configured servers are unreachable.
        """
        settings = settings or NatsEventBusSettings.from_env()
        _logger.info(
            "NatsDLQConfiguration: starting NatsDLQ (servers=%s)",
            settings.servers,
        )
        dlq = NatsDLQ(settings, serializer=serializer)
        await dlq.start()
        return dlq

    @Disposes(AbstractDeadLetterQueue)
    async def close_dlq(self, dlq: NatsDLQ) -> None:
        """
        Stop the ``NatsDLQ`` produced by ``nats_dlq`` on shutdown.

        DESIGN: ``@Disposes`` over relying on ``@PreDestroy``
            ✅ Providify never invokes ``@PreDestroy`` on a ``@Provider``-produced
               instance — ``@Disposes`` is upstream's own supported teardown
               mechanism (Plan 024 / C2). Tier B — ``NatsDLQ`` carries no
               ``@PreDestroy``, so this leak was invisible to providify's
               ``UNREACHABLE_PRE_DESTROY`` detector before the fix.

        Args:
            dlq: The ``NatsDLQ`` instance this configuration produced. Typed
                concretely (not ``AbstractDeadLetterQueue``) because
                ``stop()`` is not part of the ABC — see
                ``RedisDLQConfiguration.close_dlq`` for the same note.

        Async safety: ✅ Awaited by providify's ``_adispose`` during
            ``container.ashutdown()``.
        """
        await dlq.stop()


__all__ = [
    "NatsDLQ",
    "NatsDLQConfiguration",
]
