"""
varco_redis.backplane
=======================
``RedisPubSubBackplane`` — the Redis Pub/Sub implementation of
``varco_core.cache.backplane.CacheBackplane`` (Plan 010 / C1, Decision D-1).

Same layer split as every other ``varco_redis`` primitive: the ABC
(``CacheBackplane``) lives in ``varco_core``; this module is the concrete,
``redis.asyncio``-backed implementation, discovered by
``container.scan("varco_redis", recursive=True)`` — no ``@Configuration``/
``ainstall()`` call required (unlike ``RedisCacheConfiguration``, which needs
imperative async setup to *start* a cache; a backplane's own lifecycle is
driven entirely by the hosting ``LayeredCache.start()``/``stop()``, per
CLAUDE.md's "instantiate InvalidationStrategy outside its lifecycle"
pitfall — the same rule applies here).

House style per ``rate_limit.py``/``bulkhead.py``/``lock.py``: shared
singleton, async client, ``connect()``-shaped lifecycle. **No Lua script is
required here** — unlike the rate limiter/bulkhead/lock, Pub/Sub ``PUBLISH``
is a single, already-atomic Redis command; there is no read-then-write race
to close with a script. This is a deliberate deviation from the Lua house
style, not an oversight.

DESIGN: no CLIENT TRACKING (Decision D-1)
    ✅ ``redis.asyncio`` has no client-side-caching support (redis-py issue
       #3916, open, no ETA) — the sync-client workaround would require a
       thread pool, defeating the point of an async framework (research
       brief 002 §1).
    ✅ Pub/Sub is production-proven for exactly this job (Redisson,
       FusionCache both ship Redis Pub/Sub backplanes — brief 002 §4).
    ❌ Fire-and-forget: a subscriber disconnected at publish time never
       receives the message (brief 002 §3). Mitigated by
       ``LayeredCache``'s mandatory ``promote_ttl`` when a backplane is
       wired, and by this class's own flush-on-reconnect behaviour below.

DESIGN: self-origin filtering happens INSIDE this class
    ✅ Real Redis Pub/Sub delivers a publisher's own message back to itself
       if it is also subscribed to the same channel — a genuine wire-level
       echo, unlike the ``InMemoryBackplane`` test double (which has no
       wire to echo over). ``self.origin`` — a UUID generated once per
       process/instance — is compared against every received message's
       ``origin`` before the local handler is ever called.

DESIGN: key-name exposure opt-outs — channel_for / hash_keys
    Every subscriber to the (default, single, plaintext) channel learns
    which key names are being touched — under
    ``TenantIsolation.SHARED`` this exposes nothing new (every pod already
    serves every tenant); under per-tenant-pod topologies it is new
    cross-tenant activity metadata (see the module docstring's citation of
    ``tenancy/cache_key.py``'s ``tenant:{id}:`` key format).
    - ``channel_for=`` derives the channel from the key itself, so a node
      subscribes only to the tenants it hosts.
    - ``hash_keys=True`` publishes ``sha256(key)[:16]`` instead of the raw
      key. A hashed prefix cannot be prefix-matched by a receiver, so
      ``kind="prefix"`` degrades to a local ``"clear"`` under this mode —
      stated here and in ``publish()``'s docstring, not silent.

Thread safety:  ❌ Not thread-safe — use from a single asyncio event loop.
Async safety:   ✅ All methods are ``async def`` except ``origin``
                   (trivial property) and ``subscribe()`` (synchronous
                   registration, matching the ABC).

📚 Docs
- 🔍 https://redis.io/docs/latest/develop/reference/client-side-caching/
  Client-side caching reference — the message-loss / reconnect-flush
  mitigations this module implements
- 🔍 https://redis.io/commands/publish/
  PUBLISH — Redis Pub/Sub broadcast semantics
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import redis.asyncio as aioredis
from providify import Fallback, Provider, Singleton
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from varco_core.cache.backplane import (
    CacheBackplane,
    InvalidationKind,
    InvalidationMessage,
)
from varco_core.observability.cache import (
    record_backplane_dropped,
    record_backplane_published,
    record_backplane_received,
)

_logger = logging.getLogger(__name__)

# How often the listener loop polls get_message() — mirrors RedisEventBus.
_POLL_INTERVAL = 0.05


# ── RedisBackplaneSettings ──────────────────────────────────────────────────


class RedisBackplaneSettings(BaseSettings):
    """
    Configuration for ``RedisPubSubBackplane``.

    Attributes:
        url:        Redis connection URL. Env: ``VARCO_REDIS_CACHE_BACKPLANE_URL``.
        channel:    Pub/Sub channel name every node publishes/subscribes to.
                    Env: ``VARCO_REDIS_CACHE_BACKPLANE_CHANNEL``.
        hash_keys:  Publish ``sha256(key)[:16]`` instead of the raw key name
                    (see the module DESIGN block's key-name exposure note).
                    Env: ``VARCO_REDIS_CACHE_BACKPLANE_HASH_KEYS``.

    Thread safety:  ✅ Immutable (pydantic ``BaseSettings``, not mutated
                       after construction by this module).
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_REDIS_CACHE_BACKPLANE_", frozen=True)

    url: str = "redis://localhost:6379/0"
    channel: str = "varco.cache.invalidate"
    hash_keys: bool = False
    redis_kwargs: dict[str, Any] = Field(default_factory=dict)


# NEVER @Singleton on a pydantic BaseSettings — providify's `**values`
# constructor pitfall (CLAUDE.md). Register via a module-level @Provider,
# same as every other varco_redis settings class that isn't shaped like
# RedisEventBusSettings's own custom __init__.
@Fallback
@Provider(singleton=True)
def _redis_backplane_settings() -> RedisBackplaneSettings:
    """Default ``RedisBackplaneSettings``, reading ``VARCO_REDIS_CACHE_BACKPLANE_*`` env vars."""
    return RedisBackplaneSettings()


# ── RedisPubSubBackplane ─────────────────────────────────────────────────────


@Singleton
class RedisPubSubBackplane(CacheBackplane):
    """
    Redis Pub/Sub ``CacheBackplane`` implementation.

    Args:
        settings:   ``RedisBackplaneSettings``. Defaults to
                    ``RedisBackplaneSettings()`` (reads env vars). Ignored
                    for the connection URL when ``client=`` is given
                    directly (tests).
        client:     An already-constructed ``redis.asyncio.Redis``-shaped
                    client to use instead of creating one from ``settings``
                    — the seam unit tests use to inject a fake client with
                    no real broker.
        channel:    Overrides ``settings.channel``.
        hash_keys:  Overrides ``settings.hash_keys``.
        channel_for: Optional ``Callable[[str], str]`` deriving the Redis
                    channel from the invalidation key/prefix — the
                    per-tenant-pod key-exposure opt-out (see module
                    DESIGN block). ``None`` (default) uses one fixed
                    channel for every message.

    Lifecycle:
        ``start()``/``stop()`` are driven exclusively by the hosting
        ``LayeredCache`` — never call these directly from application code
        (CLAUDE.md's InvalidationStrategy-lifecycle pitfall applies
        identically to a backplane).

    Thread safety:  ❌ Not thread-safe — one event loop only.
    Async safety:   ✅ All methods are ``async def`` (except ``origin``,
                       ``subscribe()``).
    """

    def __init__(
        self,
        settings: RedisBackplaneSettings | None = None,
        *,
        client: Any | None = None,
        channel: str | None = None,
        hash_keys: bool | None = None,
        channel_for: Callable[[str], str] | None = None,
    ) -> None:
        self._settings = settings or RedisBackplaneSettings()
        self._external_client = client
        self._channel = channel if channel is not None else self._settings.channel
        self._hash_keys = hash_keys if hash_keys is not None else self._settings.hash_keys
        self._channel_for = channel_for

        self._origin = uuid.uuid4().hex
        self._handler: Callable[[InvalidationMessage], Awaitable[None]] | None = None

        # An externally-injected client (tests) is usable for publish()
        # immediately, without requiring start() first — start() only needs
        # to additionally stand up the listener task/subscription.
        self._redis: Any | None = self._external_client
        self._pubsub: Any | None = None
        self._listener_task: asyncio.Task[None] | None = None
        self._started = False

    # ── CacheBackplane interface ────────────────────────────────────────────

    @property
    def origin(self) -> str:
        """This process's unique publisher identity, generated once at construction."""
        return self._origin

    async def start(self) -> None:
        """Connect (or reuse the injected test client) and start the listener task. Idempotent."""
        if self._started:
            return
        self._redis = self._external_client or aioredis.from_url(
            self._settings.url,
            decode_responses=False,
            **self._settings.redis_kwargs,
        )
        self._pubsub = self._redis.pubsub()
        await self._resubscribe()
        self._listener_task = asyncio.create_task(
            self._listen_loop(), name="redis-cache-backplane-listener"
        )
        self._started = True
        _logger.debug("RedisPubSubBackplane started (channel=%r).", self._channel)

    async def stop(self) -> None:
        """Cancel the listener task and close the connection. Idempotent."""
        if not self._started:
            return
        if self._listener_task is not None:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None
        if self._pubsub is not None:
            await self._pubsub.close()
        if self._redis is not None and self._external_client is None:
            await self._redis.aclose()
        self._started = False
        _logger.debug("RedisPubSubBackplane stopped.")

    async def publish(self, message: InvalidationMessage) -> None:
        """
        Publish ``message``. Never raises (design rule 1 — see
        ``CacheBackplane.publish()``'s docstring). A ``kind="prefix"``
        message degrades to ``kind="clear"`` when ``hash_keys=True`` — a
        hash cannot be prefix-matched by a receiver.
        """
        try:
            if self._redis is None:
                raise RuntimeError("RedisPubSubBackplane.publish() called before start().")
            kind: InvalidationKind = message.kind
            payload = message.payload
            if self._hash_keys and kind != "clear":
                if kind == "prefix":
                    kind, payload = "clear", ""
                else:
                    payload = hashlib.sha256(payload.encode()).hexdigest()[:16]
            data = json.dumps(
                {
                    "kind": kind,
                    "payload": payload,
                    "origin": message.origin,
                    "ts": message.ts,
                }
            ).encode()
            channel = self._channel_for(message.payload) if self._channel_for else self._channel
            await self._redis.publish(channel, data)
            record_backplane_published(kind=kind)
        except Exception as exc:  # noqa: BLE001 - publish() must never raise
            record_backplane_dropped(reason="publish_failed")
            _logger.debug("RedisPubSubBackplane: publish failed: %s", exc)

    def subscribe(self, handler: Callable[[InvalidationMessage], Awaitable[None]]) -> None:
        """Register the local receive handler — exactly one per instance."""
        self._handler = handler

    # ── Internal ─────────────────────────────────────────────────────────────

    async def _resubscribe(self) -> None:
        assert self._pubsub is not None
        channel = self._channel_for("") if self._channel_for else self._channel
        await self._pubsub.subscribe(channel)

    async def _listen_loop(self) -> None:
        """
        Background listener — polls ``get_message()`` and dispatches to the
        local handler, applying self-origin filtering (a real Redis Pub/Sub
        channel echoes a publisher's own message back to it).

        On any connection error, re-subscribes and — per brief 002 §2 — emits
        a synthetic ``kind="clear"`` to the local handler, since any
        invalidation published while disconnected was silently dropped
        (Pub/Sub has no queue, no replay).
        """
        assert self._pubsub is not None
        while True:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=_POLL_INTERVAL
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - connection hiccup, not fatal
                _logger.debug("RedisPubSubBackplane: get_message() failed, reconnecting: %s", exc)
                await asyncio.sleep(_POLL_INTERVAL)
                try:
                    await self._resubscribe()
                except Exception as resub_exc:  # noqa: BLE001 - keep polling regardless
                    _logger.debug(
                        "RedisPubSubBackplane: resubscribe failed, will retry: %s",
                        resub_exc,
                    )
                    continue
                await self._flush_on_reconnect()
                continue

            if message is None:
                await asyncio.sleep(_POLL_INTERVAL)
                continue
            if message.get("type") not in ("message", "pmessage"):
                continue
            await self._handle_raw(message.get("data"))

    async def _handle_raw(self, data: Any) -> None:
        try:
            decoded = json.loads(data)
            invalidation_message = InvalidationMessage(
                kind=decoded["kind"],
                payload=decoded["payload"],
                origin=decoded["origin"],
                ts=decoded["ts"],
            )
        except Exception as exc:  # noqa: BLE001 - malformed payload from the wire
            record_backplane_dropped(reason="decode_failed")
            _logger.debug("RedisPubSubBackplane: failed to decode message: %s", exc)
            return

        if invalidation_message.origin == self._origin:
            # Self-echo — Redis Pub/Sub delivers a publisher's own message
            # back to it when it is also subscribed. Filtered here (design
            # rule 4), NOT by the caller.
            return
        if self._handler is not None:
            record_backplane_received(kind=invalidation_message.kind)
            await self._handler(invalidation_message)

    async def _flush_on_reconnect(self) -> None:
        """Emit a synthetic local ``clear`` after a reconnect (brief 002 §2)."""
        if self._handler is None:
            return
        synthetic = InvalidationMessage(kind="clear", payload="", origin="__reconnect__", ts=0.0)
        await self._handler(synthetic)

    def __repr__(self) -> str:
        return (
            f"RedisPubSubBackplane(channel={self._channel!r}, "
            f"hash_keys={self._hash_keys}, started={self._started})"
        )


__all__ = ["RedisBackplaneSettings", "RedisPubSubBackplane"]
