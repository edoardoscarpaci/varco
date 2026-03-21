"""
fastrest_core.authority.exceptions
====================================

Typed exceptions for the authority and trusted-issuer subsystem.

All exceptions inherit from ``AuthorityError`` so callers can catch the
entire family with a single ``except AuthorityError`` clause when they don't
care about the specific failure mode.

Thread safety:  ✅ Exception objects are immutable once constructed.
Async safety:   ✅ Pure value objects — no I/O.
"""

from __future__ import annotations


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
    exception is always visible in the traceback.

    Args:
        message: Human-readable explanation of what failed and why.

    Example::

        try:
            pem_bytes = path.read_bytes()
        except OSError as e:
            raise KeyLoadError(
                f"Cannot read PEM file at {path!r}: {e}"
            ) from e
    """
