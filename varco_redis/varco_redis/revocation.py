"""
varco_redis.revocation
========================

``RedisTokenRevocationStore`` — the production ``AbstractTokenRevocationStore``
backend (Plan 034 / S13b, Step 33).

Storage shape
-------------
One Redis string key per entry, ``{namespace}:{scope}:{key}``, holding the
JSON-serialized ``RevocationEntry``. A ``TOKEN`` entry is stored with a
native ``PX`` TTL derived from ``expires_at`` (brief 009 §5's TTL rule
comes for free from Redis's own expiry — no sweep job needed); a watermark
entry with ``expires_at=None`` (a kill switch) is stored with **no** TTL.

``is_revoked()`` issues one ``MGET`` over the four candidate keys
(``TOKEN``/``SUBJECT``/``TENANT``/``ISSUER``) — the one-round-trip property
§D-S13-shape requires, regardless of how many of the four claims are
present on the token being verified.

⚠️ **Durability caveat (§D-S13-backends)**: a Redis instance without
persistence (no AOF/RDB) loses every entry — including indefinite kill
switches — on restart, resurrecting revoked tokens until their own
``exp``. The `TOKEN`-scope TTL never exceeds `exp + skew`, so a resurrected
`TOKEN` entry's exposure window is bounded by the token's own lifetime;
a resurrected kill switch has no such bound and must be re-applied by the
operator after a Redis restart if persistence is not configured.

Thread safety:  ❌ Not thread-safe — use from a single event loop, same as
                every other ``varco_redis`` primitive.
Async safety:   ✅ All methods are ``async def``. The underlying
                ``redis.asyncio.Redis`` client is constructed eagerly in
                ``__init__`` (matches ``RedisIdempotencyStore``/
                ``RedisEventBus``'s convention — ``from_url()`` itself
                performs no I/O and needs no running event loop; only the
                first actual command does).
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import redis.asyncio as aioredis
from providify import Singleton
from varco_core.revocation.base import AbstractTokenRevocationStore
from varco_core.revocation.model import RevocationEntry, RevocationScope, RevocationVerdict
from varco_core.revocation.null import NullTokenRevocationStore

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["RedisTokenRevocationStore"]

_DEFAULT_NAMESPACE = "varco:revocation"

# Grace-period TTL (Redis PX) applied when an entry is revoked with
# expires_at already in the past — see revoke()'s docstring comment.
_EXPIRED_ENTRY_GRACE_MS = 60_000

# Scan order matches AbstractTokenRevocationStore.is_revoked()'s parameter
# order — kept as a module constant so the MGET index math in is_revoked()
# and the key-building helper never drift apart.
_ALL_SCOPES: tuple[RevocationScope, ...] = (
    RevocationScope.TOKEN,
    RevocationScope.SUBJECT,
    RevocationScope.TENANT,
    RevocationScope.ISSUER,
)


class RedisTokenRevocationStore(AbstractTokenRevocationStore):
    """
    Redis-backed ``AbstractTokenRevocationStore`` — the production backend.

    Args:
        url:       Redis connection URL.
        namespace: Key namespace prefix — every key this store touches is
                   ``{namespace}:{scope}:{key}``. Default ``"varco:revocation"``.
                   Use a unique namespace per test/tenant if sharing one
                   Redis instance (CLAUDE.md's per-test namespacing rule).
        redis_kwargs: Extra keyword arguments forwarded verbatim to
                   ``redis.asyncio.from_url()`` (SSL, auth, pool sizing).

    Thread safety:  ❌ Not thread-safe — use from a single event loop.
    Async safety:   ✅ All methods are ``async def``.
    """

    def __init__(
        self,
        *,
        url: str = "redis://localhost:6379/0",
        namespace: str = _DEFAULT_NAMESPACE,
        **redis_kwargs: Any,
    ) -> None:
        self._namespace = namespace
        self._client: aioredis.Redis = aioredis.from_url(url, decode_responses=True, **redis_kwargs)

    def _key(self, scope: RevocationScope, key: str) -> str:
        return f"{self._namespace}:{scope.value}:{key}"

    def _serialize(self, entry: RevocationEntry) -> str:
        return json.dumps(
            {
                "scope": entry.scope.value,
                "key": entry.key,
                "revoked_at": entry.revoked_at.isoformat(),
                "expires_at": entry.expires_at.isoformat() if entry.expires_at else None,
                "reason": entry.reason,
            }
        )

    def _deserialize(self, raw: str) -> RevocationEntry:
        data = json.loads(raw)
        return RevocationEntry(
            scope=RevocationScope(data["scope"]),
            key=data["key"],
            revoked_at=datetime.fromisoformat(data["revoked_at"]),
            expires_at=datetime.fromisoformat(data["expires_at"]) if data["expires_at"] else None,
            reason=data["reason"],
        )

    async def revoke(self, entry: RevocationEntry) -> None:
        """See ``AbstractTokenRevocationStore.revoke()``."""
        redis_key = self._key(entry.scope, entry.key)
        payload = self._serialize(entry)
        if entry.expires_at is None:
            # A kill switch (indefinite) — no TTL.
            await self._client.set(redis_key, payload)
            return
        ttl_ms = int((entry.expires_at - datetime.now(UTC)).total_seconds() * 1000)
        if ttl_ms <= 0:
            # Already-expired at write time ("garbage, not a revocation"
            # per the plan's Edge cases). Still WRITTEN, with a short
            # grace-period TTL rather than letting Redis expire it
            # near-instantly, so that (a) delete_expired()'s active sweep
            # below has a real window to find and count it (asserted by the
            # conformance suite) and (b) is_revoked() never has to race
            # Redis's own millisecond expiry: it re-checks the entry's own
            # `expires_at` after deserializing (see below), which is
            # authoritative regardless of whether the Redis key itself has
            # physically expired yet.
            ttl_ms = _EXPIRED_ENTRY_GRACE_MS
        await self._client.set(redis_key, payload, px=ttl_ms)

    async def unrevoke(self, scope: RevocationScope, key: str) -> bool:
        """See ``AbstractTokenRevocationStore.unrevoke()``."""
        deleted: int = await self._client.delete(self._key(scope, key))
        return deleted > 0

    async def is_revoked(
        self,
        *,
        jti: str | None,
        subject: str | None,
        issuer: str | None,
        tenant_id: str | None,
        issued_at: datetime | None,
    ) -> RevocationVerdict:
        """
        See ``AbstractTokenRevocationStore.is_revoked()``.

        One ``MGET`` over all four candidate keys (§D-S13-shape) — even
        when some of the four claims are absent, the round trip is still
        exactly one.
        """
        candidates = (
            (RevocationScope.TOKEN, jti),
            (RevocationScope.SUBJECT, subject),
            (RevocationScope.TENANT, tenant_id),
            (RevocationScope.ISSUER, issuer),
        )
        redis_keys = [self._key(scope, key) for scope, key in candidates if key is not None]
        if not redis_keys:
            return RevocationVerdict(revoked=False)

        raw_values = await self._client.mget(redis_keys)

        # Re-associate each returned value with its (scope, key) — mget()
        # preserves order, and we only queried keys for non-None candidates.
        queried = [(scope, key) for scope, key in candidates if key is not None]

        now = datetime.now(UTC)
        for (scope, candidate_key), raw in zip(queried, raw_values, strict=True):
            if raw is None:
                continue
            entry = self._deserialize(raw)
            if entry.expires_at is not None and entry.expires_at <= now:
                # Logically expired even though the Redis key may still
                # physically exist for a moment (grace-period TTL above,
                # or ordinary millisecond-scale propagation delay) — never
                # treated as a live revocation. Never a revocation, per the
                # plan's Edge cases ("garbage, not a revocation").
                continue
            if scope is RevocationScope.TOKEN:
                return RevocationVerdict(
                    revoked=True, scope=entry.scope, key=entry.key, reason=entry.reason
                )
            # Watermark scopes: §D-S13-noiat — a token with no `iat` is
            # treated as revoked by any matching non-TOKEN entry.
            if issued_at is None or issued_at < entry.revoked_at:
                return RevocationVerdict(
                    revoked=True, scope=entry.scope, key=entry.key, reason=entry.reason
                )

        return RevocationVerdict(revoked=False)

    async def list_entries(self, scope: RevocationScope | None = None) -> Sequence[RevocationEntry]:
        """See ``AbstractTokenRevocationStore.list_entries()``."""
        scopes = _ALL_SCOPES if scope is None else (scope,)
        entries: list[RevocationEntry] = []
        for s in scopes:
            pattern = f"{self._namespace}:{s.value}:*"
            async for redis_key in self._client.scan_iter(match=pattern):
                raw = await self._client.get(redis_key)
                if raw is not None:
                    entries.append(self._deserialize(raw))
        return tuple(entries)

    async def delete_expired(self) -> int:
        """
        See ``AbstractTokenRevocationStore.delete_expired()``.

        Mostly redundant with Redis's own ``PX`` TTL, which already expires
        the overwhelming majority of entries natively with no sweep needed.
        This method exists for the one case native TTL does not cover
        promptly: an entry revoked with ``expires_at`` already in the past
        is written with a short grace-period TTL (see ``revoke()``) rather
        than expiring near-instantly — an explicit sweep here reclaims it
        (and reports the count) without waiting out that grace period.

        Returns:
            The number of entries actively deleted by this sweep. Entries
            that expire via native Redis TTL without ever being swept here
            are not counted — this number is a lower bound on total
            cleanup, exactly like an idempotent, best-effort sweep should
            be.
        """
        now = datetime.now(UTC)
        removed = 0
        for s in _ALL_SCOPES:
            pattern = f"{self._namespace}:{s.value}:*"
            async for redis_key in self._client.scan_iter(match=pattern):
                raw = await self._client.get(redis_key)
                if raw is None:
                    continue
                entry = self._deserialize(raw)
                if entry.expires_at is not None and entry.expires_at <= now:
                    deleted: int = await self._client.delete(redis_key)
                    removed += deleted
        return removed


@Singleton(priority=-sys.maxsize - 1)
class RedisScanNullTokenRevocationStoreDefault(NullTokenRevocationStore):
    """
    A trivial ``NullTokenRevocationStore`` subclass, defined **in this
    module** so ``container.scan("varco_redis", recursive=True)`` alone
    (with no scan of ``varco_core`` at all) still finds a default binding
    for ``AbstractTokenRevocationStore``.

    DESIGN: a local subclass over relying on a cross-package scan
        Providify's scanner explicitly skips re-exported symbols — "only
        objects whose defining module matches the scanned module are
        registered" (``providify/scanner.py``'s own docstring). The real
        ``NullTokenRevocationStore`` is defined in ``varco_core.revocation.null``,
        so scanning ``varco_redis`` alone would never discover it, and an
        app that scans only ``varco_redis`` (a legitimate, standalone usage
        pattern — see this package's other ``@Singleton`` defaults) would
        see ``AbstractTokenRevocationStore`` unbound rather than defaulting
        to the always-off Null Object every other entry point gets.
        ✅ Zero behavioural difference from the real ``NullTokenRevocationStore``
           — every method is inherited unchanged.
        ✅ ``isinstance(x, NullTokenRevocationStore)`` still holds for this
           subclass, so callers checking "is revocation off" by type need
           no special case.
        ❌ Two classes exist for one concept. Accepted — the alternative
           (duplicating scan("varco_core.revocation") inside
           ``enable_redis_token_revocation``) would silently start
           registering unrelated ``varco_core.revocation`` bindings for an
           app that only asked for ``varco_redis``.
    """
