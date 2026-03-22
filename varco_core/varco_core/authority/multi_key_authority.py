"""
varco_core.authority.multi_key_authority
=============================================

``MultiKeyAuthority`` — wraps multiple ``JwtAuthority`` instances to support
key rotation without invalidating in-flight tokens.

Rotation model
--------------
One authority is "active" (signs new tokens); all others are "valid" (verify
old tokens until they expire).  The ``kid`` header in each signed JWT is the
routing key: ``verify()`` reads ``kid`` from the unverified header and
delegates to the matching authority.

    Phase 0: only key A active
        _authorities = {"svc:A": auth_A}
        _active_kid  = "svc:A"

    Phase 1: rotate → add key B, promote to active
        _authorities = {"svc:A": auth_A, "svc:B": auth_B}
        _active_kid  = "svc:B"
        ← old tokens (kid=svc:A) still verify; new tokens get kid=svc:B

    Phase 2: all A-signed tokens expired → retire key A
        _authorities = {"svc:B": auth_B}
        _active_kid  = "svc:B"

DESIGN: threading.Lock for rotation/retirement mutations
    ✅ Python's GIL protects dict reads — verify() (read-only) is safe
       without acquiring the lock.
    ✅ Lock is only held during rotate()/retire() — short critical sections.
    ❌ Not asyncio.Lock — MultiKeyAuthority is used synchronously in sign()
       and verify() which are pure CPU; no async I/O involved.

Thread safety:  ✅ Safe — dict reads are GIL-protected; writes use threading.Lock.
Async safety:   ✅ sign() and verify() are pure CPU — safe in async contexts.
                   rotate() and retire() are synchronous and brief.
"""

from __future__ import annotations

import threading

import jwt as _jwt

from varco_core.jwk.model import JsonWebKeySet
from varco_core.jwt.builder import JwtBuilder
from varco_core.jwt.model import JsonWebToken

from varco_core.authority.exceptions import UnknownKidError
from varco_core.authority.jwt_authority import JwtAuthority


# ── MultiKeyAuthority ─────────────────────────────────────────────────────────


class MultiKeyAuthority:
    """
    Multi-key JWT authority supporting zero-downtime key rotation.

    Maintains a registry of ``JwtAuthority`` instances keyed by ``kid``.
    Exactly one authority is "active" (used for signing new tokens); all
    others remain valid for verifying tokens signed before rotation.

    Thread safety:  ✅ Safe — see module docstring.
    Async safety:   ✅ sign() and verify() are pure CPU; rotate()/retire()
                       are brief synchronous mutations protected by a lock.

    Args:
        initial_authority: The first (and initially active) ``JwtAuthority``.

    Edge cases:
        - Calling ``retire(kid)`` on the active kid raises ``ValueError`` —
          you must rotate to a new key before retiring the old one.
        - ``verify()`` on a token with no ``kid`` header raises
          ``UnknownKidError`` — all tokens signed by this authority carry a
          kid (injected by ``JwtAuthority.sign()``).
        - An authority's ``kid`` must be unique — ``rotate()`` replaces an
          existing entry with the same kid without warning.

    Example::

        authority = MultiKeyAuthority(
            JwtAuthority.from_pem(old_pem, kid="svc:A", issuer="my-svc", algorithm="RS256")
        )

        # Later — rotate to a new key
        authority.rotate(
            JwtAuthority.from_pem(new_pem, kid="svc:B", issuer="my-svc", algorithm="RS256")
        )

        # After old tokens expire
        authority.retire("svc:A")
    """

    __slots__ = ("_authorities", "_active_kid", "_lock")

    def __init__(self, initial_authority: JwtAuthority) -> None:
        """
        Args:
            initial_authority: The starting authority.  Becomes the active
                               signing key immediately.
        """
        # dict[kid → JwtAuthority] — GIL protects reads; _lock protects writes
        self._authorities: dict[str, JwtAuthority] = {
            initial_authority.kid: initial_authority
        }
        self._active_kid: str = initial_authority.kid
        # Separate lock for rotate/retire — verify/sign do not acquire it
        self._lock: threading.Lock = threading.Lock()

    # ── Read-only properties ───────────────────────────────────────────────────

    @property
    def active_kid(self) -> str:
        """Kid of the currently active (signing) authority."""
        return self._active_kid

    @property
    def issuer(self) -> str:
        """
        Issuer string of the currently active signing authority.

        All ``JwtAuthority`` instances in a ``MultiKeyAuthority`` are expected
        to share the same issuer — rotating keys does not change the service
        identity.  This property delegates to the active authority to avoid
        storing a redundant copy.

        Returns:
            The ``iss`` claim value pre-seeded into tokens produced by ``token()``.

        Edge cases:
            - After ``rotate()``, the issuer is read from the new active
              authority.  If the new authority has a different issuer (a
              misconfiguration), this will silently return the new value.
              Keep all authorities within one ``MultiKeyAuthority`` on the
              same issuer to avoid surprising behaviour.
        """
        # GIL-protected read — _authorities dict is only mutated under _lock,
        # and _active_kid is written atomically inside the same lock section.
        # Reading _active_kid then indexing is safe without acquiring the lock.
        return self._authorities[self._active_kid].issuer

    @property
    def kids(self) -> frozenset[str]:
        """Frozenset of all registered kid values (active + valid-for-verify)."""
        # dict.keys() snapshot is safe to read without the lock (GIL)
        return frozenset(self._authorities)

    # ── Token operations ───────────────────────────────────────────────────────

    def token(self) -> JwtBuilder:
        """
        Return a ``JwtBuilder`` pre-seeded with the active authority's issuer.

        Returns:
            Fresh ``JwtBuilder`` with ``iss`` set to the active issuer.
        """
        return self._authorities[self._active_kid].token()

    def sign(self, builder: JwtBuilder) -> str:
        """
        Sign the token using the active authority.

        Args:
            builder: Configured ``JwtBuilder``.

        Returns:
            URL-safe JWT string with the active ``kid`` in the header.

        Raises:
            jwt.exceptions.InvalidKeyError: Key incompatible with algorithm.
        """
        # Always delegate to the active authority — rotate() changes which one
        return self._authorities[self._active_kid].sign(builder)

    def verify(
        self,
        token_str: str,
        *,
        audience: str | list[str] | None = None,
    ) -> JsonWebToken:
        """
        Verify a JWT string, routing to the correct authority by ``kid``.

        Reads the ``kid`` from the unverified JWT header (no crypto needed for
        this step), then delegates to the matching ``JwtAuthority.verify()``.

        Args:
            token_str: Raw JWT string.
            audience:  Expected ``aud`` value(s).  ``None`` skips audience check.

        Returns:
            ``JsonWebToken`` with all claims populated.

        Raises:
            UnknownKidError:            Token has no ``kid`` header, or ``kid``
                                        does not match any registered authority.
            jwt.ExpiredSignatureError:  Token has passed its ``exp`` time.
            jwt.InvalidSignatureError:  Signature verification failed.
            jwt.DecodeError:            Token is malformed.

        Edge cases:
            - A token with no ``kid`` header raises ``UnknownKidError`` because
              routing by kid is the only strategy — trying each key in turn
              would be a timing oracle vulnerability.
            - If a kid was retired before all tokens signed with it expired,
              verification will raise ``UnknownKidError`` for those tokens.
              Keep retired keys in ``_authorities`` until all tokens expire.
        """
        # Decode header only — no signature verification, no payload parsing.
        # get_unverified_header() is cheap: base64-decode of the first segment.
        header = _jwt.get_unverified_header(token_str)
        kid: str | None = header.get("kid")

        if kid is None:
            raise UnknownKidError(
                "Token header has no 'kid' claim — cannot route to the correct key. "
                "All tokens signed by JwtAuthority include a 'kid' in the header. "
                "If verifying a third-party token without kid, use JwtAuthority directly.",
                kid=None,
            )

        # GIL protects this dict read — no lock needed
        authority = self._authorities.get(kid)

        if authority is None:
            known = sorted(self._authorities)
            raise UnknownKidError(
                f"No authority registered for kid={kid!r}. "
                f"Known kids: {known}. "
                f"Either this token was signed by a different service, or the key "
                f"was retired before all tokens signed with it expired.",
                kid=kid,
            )

        return authority.verify(token_str, audience=audience)

    # ── Rotation lifecycle ─────────────────────────────────────────────────────

    def rotate(self, new_authority: JwtAuthority) -> None:
        """
        Add a new authority and promote it to the active signing key.

        The old active authority remains registered for verification of
        in-flight tokens until ``retire()`` is called.

        Args:
            new_authority: The new ``JwtAuthority`` to promote.  Its ``kid``
                           must be different from the current active kid.

        Edge cases:
            - If ``new_authority.kid`` already exists in the registry, the
              existing entry is silently replaced.  This allows re-using a kid
              after a brief retirement (e.g. in tests).
            - Calling ``rotate()`` with the same kid as the active authority
              replaces the key material but does NOT change ``_active_kid``.
        """
        with self._lock:
            self._authorities[new_authority.kid] = new_authority
            self._active_kid = new_authority.kid

    def retire(self, kid: str) -> None:
        """
        Remove a previously active authority from the registry.

        Call this only after all tokens signed with the given ``kid`` have
        expired (i.e., after at least one token TTL has passed since
        ``rotate()``).

        Args:
            kid: The kid to remove.  Must NOT be the currently active kid.

        Raises:
            ValueError: ``kid`` is the currently active signing key — rotate
                        to a new key first.
            KeyError:   ``kid`` is not registered.

        Edge cases:
            - Retiring a kid that has already been retired raises ``KeyError``.
        """
        with self._lock:
            if kid == self._active_kid:
                raise ValueError(
                    f"Cannot retire the active kid={kid!r}. "
                    f"Call rotate() with a new authority first, then retire the old kid. "
                    f"Retiring the active key would leave the authority unable to sign."
                )
            if kid not in self._authorities:
                raise KeyError(
                    f"No authority registered for kid={kid!r}. "
                    f"Known kids: {sorted(self._authorities)}. "
                    f"The key may have already been retired."
                )
            del self._authorities[kid]

    # ── JWKS exposure ──────────────────────────────────────────────────────────

    def jwks(self) -> JsonWebKeySet:
        """
        Return a ``JsonWebKeySet`` containing all registered public keys.

        Includes both the active key and any still-valid keys retained for
        verifying in-flight tokens.  External verifiers fetching this JWKS
        can verify tokens signed by any of the registered keys.

        Returns:
            ``JsonWebKeySet`` with one entry per registered authority.

        Edge cases:
            - Keys are returned in arbitrary dict iteration order (insertion
              order in CPython 3.7+, but callers must not depend on it).
        """
        # Snapshot keys under GIL — safe without the mutation lock
        all_keys = tuple(auth.jwk() for auth in self._authorities.values())
        return JsonWebKeySet(keys=all_keys)

    def __repr__(self) -> str:
        return (
            f"MultiKeyAuthority("
            f"active_kid={self._active_kid!r}, "
            f"kids={sorted(self._authorities)!r})"
        )
