"""
varco_redis.idempotency
========================
``RedisIdempotencyStore`` — Redis-backed ``AbstractIdempotencyStore``
(Plan 029 / D1b, Step 11).

Atomicity (§D-D1-atomic) comes from Redis's native ``SET key value NX PX``
— a single round trip that only succeeds if the key does not already
exist, with the TTL applied atomically in the same command. This is
exactly the primitive the ABC's docstring asks every implementation to use
instead of emulating one with ``EXISTS`` + ``SET``.

Storage shape
-------------
One Redis string key per idempotency key, holding a small JSON envelope::

    {"state": "reserved", "fingerprint": "..."}
    {"state": "completed", "fingerprint": "...", "status": 200,
     "body_b64": "...", "headers": {...}, "created_at": "..."}

``complete()`` overwrites the reservation's value with ``SET ... KEEPTTL``
(Redis 6.0+) — the record inherits whatever TTL remains from the original
``reserve()`` call rather than resetting the clock, since a record must
expire ``ttl`` seconds after the *request* was first seen, not after it
finished executing.

``delete_expired()`` is a no-op returning ``0`` — Redis's own ``PX``
already expires keys natively; there is nothing for this method to sweep.

Usage::

    from varco_redis.idempotency import RedisIdempotencyStore

    store = RedisIdempotencyStore(url="redis://localhost:6379/0")
    outcome = await store.reserve("order-42", fingerprint, ttl=86400.0)

Thread safety:  ❌ Not thread-safe across OS threads — use from a single
                event loop, same as every other ``varco_redis`` primitive.
Async safety:   ✅ All methods are ``async def``. The underlying
                ``redis.asyncio.Redis`` client is created lazily on first
                use (not in ``__init__``) since some client construction
                paths prefer a running event loop, matching the rest of
                this package's connect-on-first-use convention.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as aioredis
from varco_core.idempotency.base import AbstractIdempotencyStore, ReserveOutcome
from varco_core.idempotency.record import IdempotencyRecord

_KEY_PREFIX = "varco:idempotency:"


class RedisIdempotencyStore(AbstractIdempotencyStore):
    """
    Redis-backed ``AbstractIdempotencyStore`` using ``SET NX PX``.

    Args:
        url:         Redis connection URL.
        key_prefix:  Prefix applied to every Redis key this store touches —
                     namespacing so multiple varco apps can safely share one
                     Redis instance. Default ``"varco:idempotency:"``.
        redis_kwargs: Extra keyword arguments forwarded verbatim to
                     ``redis.asyncio.from_url()`` (SSL, auth, pool sizing).

    Thread safety:  ❌ Not thread-safe — use from a single event loop.
    Async safety:   ✅ All methods are ``async def``.
    """

    def __init__(
        self,
        *,
        url: str = "redis://localhost:6379/0",
        key_prefix: str = _KEY_PREFIX,
        **redis_kwargs: Any,
    ) -> None:
        self._url = url
        self._key_prefix = key_prefix
        self._redis_kwargs = redis_kwargs
        # Lazily created — see module docstring's Async safety note.
        self._redis: aioredis.Redis | None = None
        #: Flipped True by aclose() — the disposer-idempotency flag
        #: (Plan 050 / §D-disposes) tests read when no client was ever
        #: created (so `_redis is None` alone can't distinguish "never
        #: opened" from "closed").
        self._closed = False

    def _get_client(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.from_url(
                self._url,
                decode_responses=True,
                **self._redis_kwargs,
            )
        return self._redis

    def _redis_key(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    async def reserve(self, key: str, fingerprint: str, *, ttl: float) -> ReserveOutcome:
        """See ``AbstractIdempotencyStore.reserve()``."""
        if ttl <= 0:
            raise ValueError(f"reserve() ttl must be > 0, got {ttl!r}.")

        client = self._get_client()
        redis_key = self._redis_key(key)
        envelope = json.dumps({"state": "reserved", "fingerprint": fingerprint})

        acquired = await client.set(redis_key, envelope, nx=True, px=int(ttl * 1000))
        if acquired:
            return ReserveOutcome.ACQUIRED

        raw = await client.get(redis_key)
        if raw is None:
            # Expired between the failed SET NX and this GET — safe to
            # retry once; treat as if we lost a genuine race (IN_FLIGHT is
            # the conservative answer, and the caller's own retry will see
            # ACQUIRED once the winner completes or the key fully expires).
            return ReserveOutcome.IN_FLIGHT
        existing = json.loads(raw)
        if existing.get("state") == "completed":
            return ReserveOutcome.REPLAY
        return ReserveOutcome.IN_FLIGHT

    async def complete(self, key: str, record: IdempotencyRecord) -> None:
        """See ``AbstractIdempotencyStore.complete()``."""
        client = self._get_client()
        redis_key = self._redis_key(key)
        envelope = json.dumps(
            {
                "state": "completed",
                "fingerprint": record.fingerprint,
                "status": record.status,
                "body_b64": base64.b64encode(record.body).decode("ascii"),
                "headers": dict(record.headers),
                "created_at": record.created_at.isoformat(),
            }
        )
        # KEEPTTL: the record inherits the remaining TTL from reserve()
        # rather than resetting the clock — the retention window is
        # measured from first-seen, not from completion.
        await client.set(redis_key, envelope, keepttl=True)

    async def get(self, key: str) -> IdempotencyRecord | None:
        """See ``AbstractIdempotencyStore.get()``."""
        client = self._get_client()
        raw = await client.get(self._redis_key(key))
        if raw is None:
            return None
        envelope = json.loads(raw)
        if envelope.get("state") != "completed":
            return None
        return IdempotencyRecord(
            status=envelope["status"],
            body=base64.b64decode(envelope["body_b64"]),
            headers=envelope["headers"],
            fingerprint=envelope["fingerprint"],
            created_at=datetime.fromisoformat(envelope["created_at"]).astimezone(UTC),
        )

    async def release(self, key: str) -> None:
        """See ``AbstractIdempotencyStore.release()``."""
        client = self._get_client()
        await client.delete(self._redis_key(key))

    async def delete_expired(self) -> int:
        """See ``AbstractIdempotencyStore.delete_expired()`` — Redis's own
        ``PX`` handles expiry natively, so this is a no-op."""
        return 0

    async def aclose(self) -> None:
        """
        Close the lazily-created ``redis.asyncio.Redis`` client, if one was
        ever created (Plan 050 / §D-disposes row 13 — before this, a client
        created by ``RedisStoresConfiguration.idempotency_store`` leaked at
        every ``container.ashutdown()``).

        Idempotent — a second call, or a call before any client was ever
        created, is a no-op.

        Async safety: awaited by ``RedisStoresConfiguration``'s
            ``@Disposes(AbstractIdempotencyStore)``.
        """
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
        self._closed = True


__all__ = ["RedisIdempotencyStore"]
