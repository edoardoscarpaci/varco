"""
fastrest_core.jwt.util
=======================

``JwtUtil`` — read-only predicate and inspection helper for a ``JsonWebToken``.

Also exports ``SYSTEM_ISSUER``, the canonical issuer string for internal
service-to-service tokens.

Thread safety:  ✅ All methods are read-only; the wrapped token is frozen.
Async safety:   ✅ Pure — no I/O, no shared mutable state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import ClassVar, Final

from fastrest_core.auth import AuthContext
from fastrest_core.jwt.model import JsonWebToken


# ── System issuer constant ────────────────────────────────────────────────────

# Canonical issuer string for internal service-to-service (system) tokens.
# Services that identify themselves as "the system" should use this as the
# ``iss`` claim so that JwtUtil.is_system() works without extra configuration.
# Override JwtUtil.SYSTEM_ISSUER at application startup to match your own value.
SYSTEM_ISSUER: Final[str] = "fastrest/system"


# ── JwtUtil ───────────────────────────────────────────────────────────────────


class JwtUtil:
    """
    Read-only predicate and inspection helper for a ``JsonWebToken``.

    Wraps a ``JsonWebToken`` and exposes boolean checks and typed accessors
    without re-encoding or mutating the token.  All methods are pure —
    they only read the wrapped token's fields and return a result.

    The class-level ``SYSTEM_ISSUER`` constant defines the canonical issuer
    for internal system tokens.  Override it once at application startup::

        JwtUtil.SYSTEM_ISSUER = "my-org/internal"

    Thread safety:  ✅ All methods are read-only; the wrapped token is frozen.
    Async safety:   ✅ Pure — no I/O, no shared mutable state.

    Example::

        util = JwtUtil(JwtParser.parse(raw_token, secret))

        if util.is_system():
            ...  # trust elevated system privileges
        elif util.has_auth_ctx():
            ctx = util.get_auth_ctx()
            if not ctx.can(Action.READ, "posts"):
                raise PermissionError("Not allowed")
    """

    # Canonical system issuer — override at startup to match your deployment
    SYSTEM_ISSUER: ClassVar[str] = SYSTEM_ISSUER

    # __slots__ prevents accidental attribute additions and marginally reduces
    # per-instance memory (JwtUtil may be created once per request)
    __slots__ = ("_token",)

    def __init__(self, token: JsonWebToken) -> None:
        """
        Wrap a decoded ``JsonWebToken``.

        Args:
            token: The token to inspect.  Obtain one from
                   ``JwtParser.parse()`` or ``JwtBuilder.build()``.
        """
        # The token is frozen — safe to store a direct reference;
        # callers who retain the original object cannot mutate through us.
        self._token = token

    # ── Token access ──────────────────────────────────────────────────────────

    @property
    def token(self) -> JsonWebToken:
        """The underlying ``JsonWebToken`` value object."""
        return self._token

    # ── Issuer checks ─────────────────────────────────────────────────────────

    def is_issuer(self, iss: str) -> bool:
        """
        Return ``True`` if the token was issued by ``iss``.

        Args:
            iss: Expected issuer string.  Case-sensitive exact match.

        Returns:
            ``True`` iff ``token.iss == iss``.

        Edge cases:
            - Returns ``False`` when ``token.iss`` is ``None``.
        """
        return self._token.iss == iss

    def is_system(self) -> bool:
        """
        Return ``True`` if this is an internal system (service-to-service) token.

        Compares the token's ``iss`` claim against ``JwtUtil.SYSTEM_ISSUER``.
        System tokens typically carry elevated or unconditional trust — always
        verify the issuer before granting system-level access.

        Returns:
            ``True`` iff ``token.iss == JwtUtil.SYSTEM_ISSUER``.

        Edge cases:
            - Returns ``False`` when ``token.iss`` is ``None``.
            - Override ``JwtUtil.SYSTEM_ISSUER`` at startup to customise what
              counts as a system issuer in your deployment.
        """
        return self._token.iss == self.SYSTEM_ISSUER

    # ── Temporal checks ───────────────────────────────────────────────────────

    def is_expired(self) -> bool:
        """
        Return ``True`` if the token is past its expiration time.

        Uses the current UTC time for comparison.

        Returns:
            ``True`` if ``exp`` is set and in the past; ``False`` if ``exp``
            is ``None`` (no expiry) or still in the future.

        Edge cases:
            - A token with no ``exp`` claim is treated as non-expiring —
              returns ``False``.  Apply your own policy for such tokens.
            - Exact equality (``now == exp``) is treated as expired — the
              token is invalid at the expiry instant, not after it.
        """
        if self._token.exp is None:
            # No expiry claim — treat as non-expiring; caller must decide policy
            return False
        return datetime.now(timezone.utc) >= self._token.exp

    def is_valid_now(self) -> bool:
        """
        Return ``True`` if the token is currently valid w.r.t. ``exp`` and ``nbf``.

        Performs two checks:
        1. ``exp`` is not in the past (or absent).
        2. ``nbf`` is not in the future (or absent).

        Returns:
            ``True`` if both temporal constraints are satisfied.

        Edge cases:
            - Missing ``exp`` → treated as non-expiring.
            - Missing ``nbf`` → treated as immediately valid.
            - Signature validity is NOT checked here — use ``JwtParser.parse()``
              for cryptographic verification.
        """
        now = datetime.now(timezone.utc)

        if self._token.exp is not None and now >= self._token.exp:
            return False  # token has expired

        if self._token.nbf is not None and now < self._token.nbf:
            return False  # token is not yet active

        return True

    # ── Token type checks ─────────────────────────────────────────────────────

    def is_type(self, token_type: str) -> bool:
        """
        Return ``True`` if the token's ``token_type`` claim matches.

        Args:
            token_type: Expected token type (e.g. ``"access"``, ``"refresh"``).

        Returns:
            ``True`` iff ``token.token_type == token_type``.

        Edge cases:
            - Returns ``False`` when ``token.token_type`` is ``None``.
        """
        return self._token.token_type == token_type

    def is_access_token(self) -> bool:
        """
        Return ``True`` if ``token_type == "access"``.

        Returns:
            ``True`` iff the token carries the ``"access"`` type discriminator.
        """
        return self.is_type("access")

    def is_refresh_token(self) -> bool:
        """
        Return ``True`` if ``token_type == "refresh"``.

        Returns:
            ``True`` iff the token carries the ``"refresh"`` type discriminator.
        """
        return self.is_type("refresh")

    # ── AuthContext helpers ───────────────────────────────────────────────────

    def has_auth_ctx(self) -> bool:
        """
        Return ``True`` if the token carries an embedded ``AuthContext``.

        Returns:
            ``True`` iff ``token.auth_ctx is not None``.
        """
        return self._token.auth_ctx is not None

    def get_auth_ctx(self) -> AuthContext:
        """
        Return the embedded ``AuthContext``.

        Returns:
            The ``AuthContext`` embedded in this token.

        Raises:
            ValueError: No ``AuthContext`` is present.  Call
                ``has_auth_ctx()`` first if presence is uncertain.

        Edge cases:
            - Always raises for system tokens that carry no identity claims
              unless those tokens were explicitly built with ``with_auth_ctx()``.

        Example::

            if util.has_auth_ctx():
                ctx = util.get_auth_ctx()
                ctx.can(Action.READ, "posts")
        """
        if self._token.auth_ctx is None:
            raise ValueError(
                "This token does not carry an AuthContext. "
                "Guard with has_auth_ctx() before calling get_auth_ctx(). "
                f"Token sub={self._token.sub!r}, iss={self._token.iss!r}"
            )
        return self._token.auth_ctx

    def is_anonymous(self) -> bool:
        """
        Return ``True`` if the token represents an unauthenticated caller.

        Delegates to ``AuthContext.is_anonymous()`` when ``auth_ctx`` is
        present; falls back to ``token.sub is None`` otherwise.

        Returns:
            ``True`` when no identity can be determined from the token.
        """
        if self._token.auth_ctx is not None:
            return self._token.auth_ctx.is_anonymous()
        # No auth context — fall back to the presence of the subject claim
        return self._token.sub is None

    # ── Audience check ────────────────────────────────────────────────────────

    def is_audience(self, aud: str) -> bool:
        """
        Return ``True`` if ``aud`` appears in the token's audience.

        Handles both the single-string and multi-audience (``frozenset``) forms
        transparently so callers don't need to branch on the type.

        Args:
            aud: Audience identifier to check (e.g. ``"my-service"``).

        Returns:
            ``True`` if ``aud`` matches the token's ``aud`` claim.

        Edge cases:
            - Returns ``False`` when ``token.aud`` is ``None``.
        """
        if self._token.aud is None:
            return False
        if isinstance(self._token.aud, frozenset):
            return aud in self._token.aud
        # Single-string audience — exact match
        return self._token.aud == aud

    def __repr__(self) -> str:
        return (
            f"JwtUtil("
            f"sub={self._token.sub!r}, "
            f"iss={self._token.iss!r}, "
            f"type={self._token.token_type!r}, "
            f"has_auth_ctx={self.has_auth_ctx()}, "
            f"is_expired={self.is_expired()})"
        )
