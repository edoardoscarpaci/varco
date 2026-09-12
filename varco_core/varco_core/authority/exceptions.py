"""
varco_core.authority.exceptions
====================================

Typed exceptions for the authority and trusted-issuer subsystem.

All exceptions inherit from ``AuthorityError`` so callers can catch the
entire family with a single ``except AuthorityError`` clause when they don't
care about the specific failure mode.

Thread safety:  ✅ Exception objects are immutable once constructed.
Async safety:   ✅ Pure value objects — no I/O.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # TYPE_CHECKING-only: varco_core.revocation does not import this module,
    # so no cycle exists — this guard documents the intentional layering
    # (exceptions.py is the lower layer that revocation-aware callers catch,
    # not the other way around) rather than working around a real cycle.
    from varco_core.revocation import RevocationScope

# ── Base exception ─────────────────────────────────────────────────────────────


class AuthorityError(Exception):
    """
    Base class for all authority / trusted-issuer errors.

    Catch this to handle the entire family without binding to a specific
    subclass.  Raise a subclass whenever the failure mode is known.
    """


# ── Key routing errors ────────────────────────────────────────────────────────


class UnknownKidError(AuthorityError):
    """
    Raised when a token's ``kid`` header claim cannot be matched to any
    registered key.

    This can mean:
    - The token was signed by a completely different service (wrong audience).
    - The key was retired before all tokens signed with it expired.
    - The token header is missing the ``kid`` claim entirely.
    - A remote JWKS endpoint has rotated keys and the local cache is stale.

    Attributes:
        kid: The ``kid`` value that could not be resolved, or ``None`` when
             the token header did not contain a ``kid`` claim at all.

    Args:
        message: Human-readable explanation.
        kid:     The unresolved kid value (``None`` when absent from header).

    Example::

        raise UnknownKidError(
            f"No key registered for kid={kid!r}. Known kids: {list(known)}.",
            kid=kid,
        )
    """

    def __init__(self, message: str, *, kid: str | None = None) -> None:
        # Store kid as a structured attribute so callers can inspect it
        # without parsing the message string.
        self.kid = kid
        super().__init__(message)


# ── Issuer registry errors ────────────────────────────────────────────────────


class IssuerNotFoundError(AuthorityError):
    """
    Raised when a registry operation references a label that is not registered.

    Attributes:
        label: The label that was not found in the registry.

    Args:
        message: Human-readable explanation.
        label:   The missing registry label.
    """

    def __init__(self, message: str, *, label: str) -> None:
        self.label = label
        super().__init__(message)


# ── Key loading errors ────────────────────────────────────────────────────────


class KeyLoadError(AuthorityError):
    """
    Raised when a key source fails to load or refresh its keyset.

    Wraps the underlying cause (network error, file not found, invalid PEM,
    malformed JWKS JSON, etc.) via exception chaining so the original
    exception is always visible in the traceback.  Pass a human-readable
    explanation of what failed and why as the message argument.

    Example::

        try:
            pem_bytes = path.read_bytes()
        except OSError as e:
            raise KeyLoadError(
                f"Cannot read PEM file at {path!r}: {e}"
            ) from e
    """


# ── Revocation errors (Plan 034 / S13, §D-S13-error) ─────────────────────────────


class TokenRevokedError(AuthorityError):
    """
    Raised by ``TrustedIssuerRegistry.verify()`` when a bound
    ``AbstractTokenRevocationStore`` reports the token as revoked.

    ``str(exc)`` is a **fixed, constant string** — ``scope``/``key``/
    ``reason`` are attributes only, never interpolated into the message.
    This is deliberate (§D-S13-error): ``JwtBearerAuth`` maps this
    exception straight to a 401 ``HTTPException``, and a client must learn
    only "revoked", never *why* or *at what scope* — the same discipline
    CLAUDE.md applies to ``error_params()`` and to
    ``ServiceAuthorizationError`` excluding ``reason``.

    Attributes:
        scope:  The ``RevocationScope`` that matched. Log/audit only.
        key:    The matching entry's lookup key. Log/audit only.
        reason: The matching entry's operator note, or ``None``. Log/audit
                only — this is the exact field CLAUDE.md warns is a "new
                exfiltration surface".

    Args:
        scope:  The scope that matched.
        key:    The matching key.
        reason: Optional operator note.

    Example::

        try:
            await registry.verify(token)
        except TokenRevokedError as exc:
            logger.warning("revoked: scope=%s key=%s reason=%s", exc.scope, exc.key, exc.reason)
            raise HTTPException(401, detail=str(exc)) from exc  # "Token has been revoked."
    """

    def __init__(
        self,
        *,
        scope: RevocationScope,
        key: str,
        reason: str | None = None,
    ) -> None:
        self.scope = scope
        self.key = key
        self.reason = reason
        # Constant message — see class docstring. Never build this from
        # scope/key/reason, no matter how tempting a richer message is.
        super().__init__("Token has been revoked.")


class RevocationStoreUnavailableError(AuthorityError):
    """
    Raised by ``TrustedIssuerRegistry.verify()`` when a bound
    ``AbstractTokenRevocationStore`` raises during ``is_revoked()`` and
    ``RevocationFailureMode.FAIL_CLOSED`` (the default) is in effect.

    ``JwtBearerAuth`` maps this to HTTP **503**, not 401 — an outage is not
    a bad credential, and reporting it as one would be a lie (§D-S13-error).

    The underlying store exception is available via ``__cause__``
    (standard exception chaining) for server-side logs; the message given
    here is deliberately generic to avoid leaking backend internals
    (connection strings, stack traces) to a caller who only needs to know
    "try again later".
    """
