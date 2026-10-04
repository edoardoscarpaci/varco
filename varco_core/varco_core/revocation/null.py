"""
varco_core.revocation.null
=============================

``NullTokenRevocationStore`` — the no-op ``AbstractTokenRevocationStore``
default (Plan 034 / S13a, Step 18, §D-S13-di). Bound automatically by
``container.scan("varco_core.revocation", recursive=True)`` so an
application that never opts into revocation sees zero behaviour change.

``@Fallback`` — the in-process default; any application binding of
``AbstractTokenRevocationStore`` (including ``enable_token_revocation()``'s
``InMemoryTokenRevocationStore``) wins regardless of registration order
(`technical_docs/features/dependency-injection.md`).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from providify import Fallback, Singleton

from varco_core.revocation.base import AbstractTokenRevocationStore
from varco_core.revocation.model import RevocationScope, RevocationVerdict

if TYPE_CHECKING:
    from collections.abc import Sequence

    from varco_core.revocation.model import RevocationEntry

__all__ = ["NullTokenRevocationStore", "is_null_revocation_store"]


@Fallback
@Singleton
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


def is_null_revocation_store(store: AbstractTokenRevocationStore | None) -> bool:
    """
    Report whether *store* is "Null" — absent, or a ``NullTokenRevocationStore``
    (Plan 053 §D-null). The one definition of "non-Null" shared by
    ``TrustedIssuerRegistry._check_revocation()``'s derived
    ``revocation_require_jti`` and ``inspect_revocation_posture()``.

    DESIGN: ``isinstance`` behind one helper
        ✅ An out-of-tree store is non-Null by construction (correct: it
           does something); a user *subclass* of the Null store is Null
           (correct: it inherits the no-op contract); zero ABC change.
        ❌ A user *wrapper* around a Null store (e.g. a logging decorator)
           is non-Null, so it derives ``require_jti=True`` and rejects
           ``jti``-less tokens — subclass the Null class instead, or set
           ``VARCO_JWT_REVOCATION_REQUIRE_JTI=false``.
        Rejected: an ``is_null`` ClassVar on the ABC (new surface every
        out-of-tree store inherits, a second definition of the same fact);
        ``type(store) is NullTokenRevocationStore`` (a Null subclass would
        start rejecting tokens).

    Args:
        store: The store to classify, or ``None``.

    Returns:
        ``True`` iff *store* is ``None`` or an instance of
        ``NullTokenRevocationStore`` (including subclasses).

    Example::

        is_null_revocation_store(NullTokenRevocationStore())       # True
        is_null_revocation_store(InMemoryTokenRevocationStore())   # False
    """
    return store is None or isinstance(store, NullTokenRevocationStore)
