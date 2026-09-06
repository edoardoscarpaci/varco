"""
varco_core.revocation.base
============================

``AbstractTokenRevocationStore`` — the seam every revocation backend
implements (Plan 034 / S13a, §D-S13-shape).

This module is the contract document for the whole feature: the four
scopes and their lookup keys, the not-valid-before watermark rule, the
missing-``iat`` fail-closed rule, the TTL convention, and the three
documented failure modes (brief 009 §4 requires all three be documented,
even though only two ship — see ``RevocationFailureMode``).

``from __future__ import annotations``; imports limited to ``abc``,
``dataclasses`` (via ``model``), ``datetime``, ``typing`` — no new runtime
dependency for this plan.

Thread safety:  Documented per-implementation below.
Async safety:   Documented per-implementation below.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from varco_core.revocation.model import RevocationEntry, RevocationScope, RevocationVerdict


class AbstractTokenRevocationStore(abc.ABC):
    """
    A seam for invalidating a JWT (or class of JWTs) before its natural
    ``exp``, per token / subject / tenant / issuer.

    Contract
    --------
    Four independent scopes (``RevocationScope``), checked together by a
    single ``is_revoked()`` round trip:

    - ``TOKEN``   — exact-token denylist, keyed by ``jti``. Revoked iff the
                    presented ``jti`` has a live (non-expired) entry.
    - ``SUBJECT`` — not-valid-before watermark, keyed by ``f"{iss}|{sub}"``.
                    Revoked iff ``issued_at < revoked_at``.
    - ``TENANT``  — watermark, keyed by ``tenant_id``. Same rule as ``SUBJECT``.
    - ``ISSUER``  — watermark, keyed by ``iss``. Same rule as ``SUBJECT``.

    **Watermark rule (§D-S13-nvb)**: for the three non-``TOKEN`` scopes, a
    token is revoked iff its ``iat`` predates the entry's ``revoked_at`` —
    "compares iat against a stored timestamp… invalidates all old tokens at
    once" (brief 009 §1). A token minted *after* the kill switch was set is
    valid — this is what makes a tenant/issuer kill switch survivable
    rather than terminal.

    **Missing-``iat`` rule (§D-S13-noiat)**: a token with no ``iat`` claim
    is treated as revoked by ANY matching non-``TOKEN`` entry — fail-closed
    on the ambiguous case, because "issued at an unknown time" cannot be
    shown to be after the watermark.

    **TTL convention (brief 009 §5)**: a ``TOKEN`` entry's ``expires_at``
    SHOULD be ``token_exp + clock_skew_tolerance_seconds`` (see
    ``RevocationEntry.for_token()``), so the denylist entry outlives the
    window in which a lagging verifier would still accept the token.
    Non-``TOKEN`` (watermark) entries typically have ``expires_at=None``
    (indefinite) — a kill switch has no natural expiry.

    **Failure modes (brief 009 §4, all three documented here as required,
    only two implemented in ``TrustedIssuerRegistry`` — §D-S13-fail)**:

    - ``FAIL_CLOSED`` (varco's default): a store outage raises rather than
      silently admitting a possibly-revoked token. A 503, not a 401 — an
      outage is not a bad credential.
    - ``FAIL_OPEN``: a store outage is logged and verification proceeds.
      Brief 009 §4's own baseline recommendation, paired with short-lived
      tokens.
    - ``FAIL_OPEN_WITHIN_GRACE`` (cached denylist + bounded staleness): not
      implemented in 3.2 — brief 009's Evidence Gap 2 says the quantity
      that would size the grace window is unmeasured. Parked, not a store
      method.

    Thread safety:  Implementation-dependent — see each concrete class.
    Async safety:   Implementation-dependent — see each concrete class.
    """

    @abc.abstractmethod
    async def revoke(self, entry: RevocationEntry) -> None:
        """
        Persist a revocation entry.

        Args:
            entry: The entry to store. Revoking the same ``(scope, key)``
                   twice is idempotent — the second call's entry replaces
                   the first (last write wins).

        Returns:
            None.

        Edge cases:
            - Never raises "already revoked" — this is a pure upsert.
        """

    @abc.abstractmethod
    async def unrevoke(self, scope: RevocationScope, key: str) -> bool:
        """
        Remove a revocation entry.

        Args:
            scope: The scope of the entry to remove.
            key:   The entry's lookup key.

        Returns:
            ``True`` if an entry was removed, ``False`` if none matched
            ``(scope, key)``.
        """

    @abc.abstractmethod
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
        Check all four scopes in one round trip.

        Args:
            jti:       The token's ``jti`` claim, or ``None`` if absent.
            subject:   The ``SUBJECT``-scope lookup key
                       (``f"{iss}|{sub}"``, already assembled by the
                       caller), or ``None`` if not computable.
            issuer:    The token's ``iss`` claim, or ``None``.
            tenant_id: The token's ``tenant_id`` claim
                       (**never** ``current_tenant()`` — §D-S13-scope), or
                       ``None`` if absent.
            issued_at: The token's ``iat`` claim (aware UTC), or ``None``
                       if absent — see the missing-``iat`` rule above.

        Returns:
            A ``RevocationVerdict``. When multiple scopes match, an
            implementation MAY report any one of them — callers only need
            to know *that* the token is revoked, not an exhaustive list
            (``list_entries()`` exists for that).

        Edge cases:
            - Any of the four keys may be ``None`` (not present on the
              token) — that scope is simply skipped; the others still run.
            - Called for every verified token when a store is bound, so
              implementations SHOULD make this a single round trip
              (e.g. one Redis ``MGET`` over the four candidate keys).

        Thread safety / Async safety: see the implementing class.
        """

    @abc.abstractmethod
    async def list_entries(self, scope: RevocationScope | None = None) -> Sequence[RevocationEntry]:
        """
        List currently-stored entries, optionally filtered by scope.

        Args:
            scope: Restrict to one scope, or ``None`` for all scopes.

        Returns:
            All matching, non-expired entries. Order is unspecified.
        """

    @abc.abstractmethod
    async def delete_expired(self) -> int:
        """
        Remove every entry whose ``expires_at`` is in the past.

        Returns:
            The number of entries removed.

        Edge cases:
            - An entry with ``expires_at=None`` never expires and is never
              removed by this method.
        """
