"""
varco_core.revocation.model
=============================

``RevocationScope``, ``RevocationEntry``, ``RevocationVerdict``,
``RevocationFailureMode`` — the value objects behind the
``AbstractTokenRevocationStore`` seam (Plan 034 / S13a, §D-S13-shape).

``from __future__ import annotations``; imports limited to stdlib
(``dataclasses``, ``datetime``, ``enum``) per CLAUDE.md's "no new runtime
dependency anywhere" rule for this plan.

Thread safety:  ✅ All types here are frozen/immutable value objects.
Async safety:   ✅ No I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum


class RevocationScope(StrEnum):
    """
    The four independent axes a token can be revoked on.

    Brief 009 §2's critical finding shapes this: ``jti`` is not universally
    present (Auth0/Keycloak/Cognito do not emit it by default), so a store
    keyed only on ``jti`` would silently do nothing for those deployments.
    ``SUBJECT``/``TENANT``/``ISSUER`` need only the standard ``iat`` claim.

    Members:
        TOKEN:   key = ``jti``. A denylist entry — exact-token revocation.
        SUBJECT: key = ``f"{iss}|{sub}"``. A "not valid before" watermark —
                 global logout for one subject.
        TENANT:  key = ``tenant_id``. A watermark — a per-tenant kill switch.
        ISSUER:  key = ``iss``. A watermark — a per-issuer kill switch.
    """

    TOKEN = "token"
    SUBJECT = "subject"
    TENANT = "tenant"
    ISSUER = "issuer"


class RevocationFailureMode(StrEnum):
    """
    How ``TrustedIssuerRegistry.verify()`` behaves when a bound
    ``AbstractTokenRevocationStore`` raises during ``is_revoked()``.

    Brief 009 §4: "No industry consensus on a 'right' answer" — varco ships
    both documented modes and defaults to the one whose failure is loud and
    diagnosable (§D-S13-fail).

    Members:
        FAIL_CLOSED: A store outage raises ``RevocationStoreUnavailableError``
                     (mapped to HTTP 503 by ``JwtBearerAuth``). **Default.**
                     Nobody is affected on upgrade — the DI default
                     (``NullTokenRevocationStore``) performs no I/O and can
                     never trigger this path; it only ever applies to an app
                     that explicitly wired a real store.
        FAIL_OPEN:   A store outage is logged at ``error`` and the token is
                     treated as not revoked — verification proceeds. Brief
                     009 §4's own baseline recommendation is "fail-open with
                     short-lived tokens"; pick this only alongside short
                     token lifetimes.
    """

    FAIL_CLOSED = "fail_closed"
    FAIL_OPEN = "fail_open"


def _require_aware_utc(value: datetime | None, *, field_name: str) -> None:
    """
    Raise ``ValueError`` if ``value`` is a naive ``datetime``.

    Args:
        value:      The datetime to check. ``None`` is always accepted.
        field_name: Name reported in the error message.

    Raises:
        ValueError: ``value`` has no ``tzinfo`` (the house rule: aware UTC
                    only, never a naive datetime — CLAUDE.md's timezone
                    discipline applies to every varco datetime field).
    """
    if value is not None and value.tzinfo is None:
        raise ValueError(
            f"RevocationEntry.{field_name} must be an aware (UTC) datetime, "
            f"not a naive one — got {value!r}"
        )


@dataclass(frozen=True)
class RevocationEntry:
    """
    One revocation record — either an exact-token denylist entry (``TOKEN``
    scope) or a not-valid-before watermark (every other scope).

    Attributes:
        scope:      Which axis this entry revokes on.
        key:        The scope's lookup key (see ``RevocationScope``'s
                    per-member docstring for the exact key shape).
        revoked_at: When this entry was created. Aware UTC. For non-``TOKEN``
                    scopes this **is** the watermark itself — see
                    ``AbstractTokenRevocationStore``'s docstring for the
                    ``issued_at < revoked_at`` rule.
        expires_at: When this entry should stop matching. ``None`` means
                    indefinite (the shape a kill switch needs — a watermark
                    has no natural expiry). For a ``TOKEN`` entry this
                    SHOULD be set to the token's own ``exp`` plus a clock-skew
                    allowance (brief 009 §5) — see ``for_token()``.
        reason:     Optional operator note. **Never returned to a client** —
                    it reaches only logs and admin/audit surfaces
                    (§D-S13-error, the same discipline CLAUDE.md applies to
                    ``ServiceAuthorizationError`` excluding ``reason``).

    Raises:
        ValueError: ``revoked_at``/``expires_at`` is a naive datetime.

    Thread safety:  ✅ Frozen.
    Async safety:   ✅ No I/O.
    """

    scope: RevocationScope
    key: str
    revoked_at: datetime
    expires_at: datetime | None
    reason: str | None = None

    def __post_init__(self) -> None:
        _require_aware_utc(self.revoked_at, field_name="revoked_at")
        _require_aware_utc(self.expires_at, field_name="expires_at")

    @classmethod
    def for_token(
        cls,
        jti: str | None,
        exp: datetime,
        *,
        skew: float = 60.0,
        reason: str | None = None,
    ) -> RevocationEntry:
        """
        Build a ``TOKEN``-scope entry whose TTL follows brief 009 §5's rule.

        Args:
            jti:    The token's ``jti`` claim. Required — a ``TOKEN``-scope
                    revocation cannot be expressed for a token that never
                    had one (use ``SUBJECT``/``TENANT``/``ISSUER`` instead).
            exp:    The token's ``exp`` claim (aware UTC).
            skew:   Clock-skew tolerance in seconds, added on top of ``exp``
                    so the entry outlives the window a lagging verifier
                    would still accept the token in. Default ``60.0``
                    (brief 009 §5: OWASP/RFC 7519 recommend 30-60s).
            reason: Optional operator note (never reaches a client).

        Returns:
            A ``RevocationEntry`` with ``expires_at == exp + timedelta(seconds=skew)``.

        Raises:
            ValueError: ``jti`` is ``None``.

        Example::

            entry = RevocationEntry.for_token(token.jti, token.exp, skew=60)
            await store.revoke(entry)
        """
        if jti is None:
            raise ValueError(
                "RevocationEntry.for_token(): jti is required — a TOKEN-scope "
                "revocation cannot be expressed for a token with no jti. Use "
                "RevocationScope.SUBJECT/TENANT/ISSUER instead."
            )
        return cls(
            scope=RevocationScope.TOKEN,
            key=jti,
            revoked_at=exp,  # informational for TOKEN scope; the TTL is what matters
            expires_at=exp + timedelta(seconds=skew),
            reason=reason,
        )


@dataclass(frozen=True)
class RevocationVerdict:
    """
    The result of an ``is_revoked()`` check.

    Attributes:
        revoked: Whether the token is revoked under any matching entry.
        scope:   Which scope matched, or ``None`` when ``revoked`` is
                 ``False``.
        key:     The matching entry's key, or ``None``.
        reason:  The matching entry's operator note, or ``None``. **Never
                 returned to a client** — see ``RevocationEntry.reason``.

    Thread safety:  ✅ Frozen.
    """

    revoked: bool
    scope: RevocationScope | None = None
    key: str | None = None
    reason: str | None = None
