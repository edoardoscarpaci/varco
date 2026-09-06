"""
varco_core.revocation.null
=============================

``NullTokenRevocationStore`` — the no-op ``AbstractTokenRevocationStore``
default (Plan 034 / S13a, Step 18, §D-S13-di). Bound automatically by
``container.scan("varco_core.revocation", recursive=True)`` so an
application that never opts into revocation sees zero behaviour change.

Registered at the lowest possible priority — the exact
``varco_core.flags.NullFeatureFlags`` precedent
(``varco_core/varco_core/flags/null.py``) — so
``enable_token_revocation()`` unconditionally wins once called.
"""

from __future__ import annotations

import sys
from datetime import datetime
from typing import TYPE_CHECKING

from providify import Singleton

from varco_core.revocation.base import AbstractTokenRevocationStore
from varco_core.revocation.model import RevocationScope, RevocationVerdict

if TYPE_CHECKING:
    from collections.abc import Sequence

    from varco_core.revocation.model import RevocationEntry

__all__ = ["NullTokenRevocationStore"]


@Singleton(priority=-sys.maxsize - 1)
class NullTokenRevocationStore(AbstractTokenRevocationStore):
    """
    No-op revocation store — never revokes anything, performs no I/O.

    ⚠️ This is a Null Object that **deliberately violates** the ABC's
    revoke→is_revoked contract: ``revoke()`` silently accepts and discards
    every entry, so a caller who binds this store and calls ``revoke()``
    gets no error and no effect. That is the point — a truthful "revocation
    is off" default requires that binding it costs nothing and changes
    nothing, not that it partially works. See
    ``testkit/varco_conformance/COVERAGE.md``'s stated-absence row for this
    class (it does not subclass ``TokenRevocationStoreConformance`` for
    exactly this reason — the conformance suite asserts revoke() has an
    effect, which this class contractually does not provide).

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ No I/O.
    """

    async def revoke(self, entry: RevocationEntry) -> None:
        return None

    async def unrevoke(self, scope: RevocationScope, key: str) -> bool:
        return False

    async def is_revoked(
        self,
        *,
        jti: str | None,
        subject: str | None,
        issuer: str | None,
        tenant_id: str | None,
        issued_at: datetime | None,
    ) -> RevocationVerdict:
        return RevocationVerdict(revoked=False)

    async def list_entries(self, scope: RevocationScope | None = None) -> Sequence[RevocationEntry]:
        return ()

    async def delete_expired(self) -> int:
        return 0
