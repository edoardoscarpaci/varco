"""
varco_core.revocation.memory
==============================

``InMemoryTokenRevocationStore`` — a dev/test/single-process
``AbstractTokenRevocationStore`` implementation (Plan 034 / S13a, Step 18).

Thread safety:  ❌ Not safe across threads — a lazily-created ``asyncio.Lock``
                serialises concurrent coroutines on the same event loop
                only (CLAUDE.md: locks are always created lazily, never at
                ``__init__`` or module scope).
Async safety:   ✅ All mutating operations hold the lock; reads do not need
                to (dict reads are atomic under the GIL and staleness here
                is bounded by whatever concurrent ``revoke()`` is doing).
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING

from varco_core.revocation.base import AbstractTokenRevocationStore
from varco_core.revocation.model import RevocationEntry, RevocationScope, RevocationVerdict

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["InMemoryTokenRevocationStore"]


class InMemoryTokenRevocationStore(AbstractTokenRevocationStore):
    """
    In-memory ``AbstractTokenRevocationStore`` — dict-backed, single process.

    DESIGN: a lazily-created ``asyncio.Lock`` over a module/``__init__``-time one
        ✅ ``asyncio.Lock()`` requires a running event loop; creating it in
           ``__init__`` would break construction before a loop starts
           (CLAUDE.md's house rule, identical reasoning to
           ``TrustedIssuerRegistry._get_lock()``).
        ❌ One extra ``is None`` branch per call. Negligible.

    Not durable — restarting the process loses every entry, including kill
    switches. Suitable for development, tests, and single-process
    deployments; production should use ``varco_redis``'s
    ``RedisTokenRevocationStore``.
    """

    def __init__(self) -> None:
        # (scope, key) -> RevocationEntry
        self._entries: dict[tuple[RevocationScope, str], RevocationEntry] = {}
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        """Return the lock, creating it lazily on first use (see class DESIGN block)."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _is_live(self, entry: RevocationEntry, *, now: datetime) -> bool:
        """An entry is live iff it has no expiry, or its expiry is in the future."""
        return entry.expires_at is None or entry.expires_at > now

    async def revoke(self, entry: RevocationEntry) -> None:
        async with self._get_lock():
            self._entries[(entry.scope, entry.key)] = entry

    async def unrevoke(self, scope: RevocationScope, key: str) -> bool:
        async with self._get_lock():
            return self._entries.pop((scope, key), None) is not None

    async def is_revoked(
        self,
        *,
        jti: str | None,
        subject: str | None,
        issuer: str | None,
        tenant_id: str | None,
        issued_at: datetime | None,
    ) -> RevocationVerdict:
        from datetime import UTC

        now = datetime.now(UTC)

        # TOKEN scope: a live denylist entry is a match, unconditionally.
        if jti is not None:
            entry = self._entries.get((RevocationScope.TOKEN, jti))
            if entry is not None and self._is_live(entry, now=now):
                return RevocationVerdict(
                    revoked=True, scope=entry.scope, key=entry.key, reason=entry.reason
                )

        # Watermark scopes: SUBJECT, TENANT, ISSUER.
        for scope, candidate_key in (
            (RevocationScope.SUBJECT, subject),
            (RevocationScope.TENANT, tenant_id),
            (RevocationScope.ISSUER, issuer),
        ):
            if candidate_key is None:
                continue
            entry = self._entries.get((scope, candidate_key))
            if entry is None or not self._is_live(entry, now=now):
                continue
            # §D-S13-noiat: a token with no `iat` is treated as revoked by
            # any matching non-TOKEN entry — fail-closed on the ambiguous
            # case ("issued at an unknown time" cannot be shown to be after
            # the watermark).
            if issued_at is None or issued_at < entry.revoked_at:
                return RevocationVerdict(
                    revoked=True, scope=entry.scope, key=entry.key, reason=entry.reason
                )

        return RevocationVerdict(revoked=False)

    async def list_entries(self, scope: RevocationScope | None = None) -> Sequence[RevocationEntry]:
        async with self._get_lock():
            return tuple(
                entry
                for (entry_scope, _), entry in self._entries.items()
                if scope is None or entry_scope == scope
            )

    async def delete_expired(self) -> int:
        from datetime import UTC

        now = datetime.now(UTC)
        async with self._get_lock():
            expired_keys = [
                k for k, entry in self._entries.items() if not self._is_live(entry, now=now)
            ]
            for k in expired_keys:
                del self._entries[k]
            return len(expired_keys)
