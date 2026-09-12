"""
varco_core.revocation
========================

A seam for invalidating a JWT (or class of JWTs) before its natural
``exp`` — per token, per subject, per tenant, per issuer (Plan 034 / S13).

``NullTokenRevocationStore`` (this package, ``null.py``) is the scanned
``@Singleton`` default — importing/bootstrapping this package never checks
revocation for an app that has not opted in. ``enable_token_revocation()``
(``di.py``) opts into ``InMemoryTokenRevocationStore``;
``varco_redis.di.enable_redis_token_revocation()`` opts into the production
Redis backend.

⚠️ Binding a store in the container does NOT by itself enable checking —
``TrustedIssuerRegistry`` must also receive it
(``TrustedIssuerRegistry(revocation_store=...)``). See
``varco_core.revocation.di``'s docstring for the two-step wiring.

Usage::

    from varco_core.revocation import RevocationEntry, RevocationScope
    from varco_core.revocation.memory import InMemoryTokenRevocationStore

    store = InMemoryTokenRevocationStore()
    await store.revoke(RevocationEntry.for_token(jti, exp, skew=60))

    registry = TrustedIssuerRegistry(revocation_store=store)
"""

from __future__ import annotations

from varco_core.revocation.base import AbstractTokenRevocationStore
from varco_core.revocation.model import (
    RevocationEntry,
    RevocationFailureMode,
    RevocationScope,
    RevocationVerdict,
)
from varco_core.revocation.posture import RevocationPostureReport, inspect_revocation_posture

__all__ = [
    "AbstractTokenRevocationStore",
    "RevocationEntry",
    "RevocationFailureMode",
    "RevocationPostureReport",
    "RevocationScope",
    "RevocationVerdict",
    "inspect_revocation_posture",
]
