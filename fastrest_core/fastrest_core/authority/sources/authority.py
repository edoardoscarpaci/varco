"""
fastrest_core.authority.sources.authority
==========================================

``AuthoritySource`` — an ``IssuerSource`` backed by a local ``JwtAuthority``
or ``MultiKeyAuthority``.

Unlike file/URL sources, this source reads key material directly from an
in-memory authority object.  There is no I/O — ``load()`` and ``refresh()``
always return the authority's current ``jwks()`` output synchronously.

Rotation awareness
------------------
When the underlying authority is a ``MultiKeyAuthority``, ``refresh()``
automatically reflects any keys added via ``rotate()`` since the last call.
``MultiKeyAuthority.rotate()`` mutates the internal key registry in-place;
``jwks()`` always snapshots the current state.  No config change needed after
key rotation — the registry picks it up on the next refresh cycle.

DESIGN: ``_SigningAuthority`` Protocol instead of importing the concrete classes
    ✅ Avoids circular imports — both ``JwtAuthority`` and ``MultiKeyAuthority``
       import from ``fastrest_core.authority.exceptions`` and ``fastrest_core.jwk``.
       Having ``sources/authority.py`` import those concrete classes would create
       a dependency cycle through ``authority/__init__.py``.
    ✅ Forward-compatible — any future authority with ``.issuer`` + ``.jwks()``
       works without changing this module.
    ✅ ``@runtime_checkable`` on the Protocol enables ``isinstance()`` checks in
       ``register_authority()`` for early, clear error messages.
    ❌ Less obvious what types are accepted — mitigated by ``__init__`` docstring
       listing ``JwtAuthority | MultiKeyAuthority`` explicitly.
    Alternative considered: ``TYPE_CHECKING``-only import of the concrete classes
    for annotations — rejected because it doesn't add much over the Protocol and
    still creates the circular risk when the type checker runs.

Thread safety:  ✅ ``AuthoritySource`` is immutable after construction.
                   ``load()`` / ``refresh()`` delegate to ``authority.jwks()``.
                   ``JwtAuthority.jwks()`` is pure CPU on immutable state (✅).
                   ``MultiKeyAuthority.jwks()`` does a GIL-protected dict snapshot
                   — safe to call without an explicit lock (✅).
Async safety:   ✅ No I/O, no blocking calls, no shared mutable state.
                   Both ``load()`` and ``refresh()`` are safe to ``await``
                   concurrently from multiple tasks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from fastrest_core.jwk.model import JsonWebKeySet

if TYPE_CHECKING:
    # Imported only for type-checker annotation — not at runtime.
    # Concrete classes live in the parent package which imports from sources/;
    # importing them here at runtime would create a circular dependency.
    from fastrest_core.authority.jwt_authority import JwtAuthority
    from fastrest_core.authority.multi_key_authority import MultiKeyAuthority


# ── _SigningAuthority Protocol ─────────────────────────────────────────────────


@runtime_checkable
class _SigningAuthority(Protocol):
    """
    Structural protocol for objects that can produce a ``JsonWebKeySet``.

    Both ``JwtAuthority`` and ``MultiKeyAuthority`` satisfy this protocol.
    ``@runtime_checkable`` allows ``isinstance()`` checks for clear validation
    error messages in ``TrustedIssuerRegistry.register_authority()``.

    Attributes:
        issuer: The ``iss`` claim value this authority signs tokens with.

    Methods:
        jwks(): Return the current public ``JsonWebKeySet``.
    """

    @property
    def issuer(self) -> str:
        """The ``iss`` claim value signed into every token."""
        ...

    def jwks(self) -> JsonWebKeySet:
        """Return the current public keyset for external verifiers."""
        ...


# ── AuthoritySource ────────────────────────────────────────────────────────────


class AuthoritySource:
    """
    ``IssuerSource`` backed by a local ``JwtAuthority`` or ``MultiKeyAuthority``.

    Adapts a signing authority into the ``IssuerSource`` protocol so it can be
    registered in ``TrustedIssuerRegistry`` alongside external sources (JWKS
    URLs, PEM files, OIDC endpoints).  This lets the registry verify tokens
    signed by the framework itself without any env var configuration.

    The canonical usage is through ``TrustedIssuerRegistry.register_authority()``
    which constructs the source automatically:

        authority = MultiKeyAuthority(
            JwtAuthority.from_pem(pem, kid="svc:A", issuer="my-svc", algorithm="RS256")
        )
        registry.register_authority(authority)       # ← builds AuthoritySource internally
        await registry.load_all()
        token = await registry.verify(raw_token_str) # ← now verifies my-svc tokens ✅

    Thread safety:  ✅ Immutable after construction — ``_authority`` reference never
                       changes.  ``jwks()`` calls on the wrapped authority are
                       GIL-safe (see module-level docstring for details).
    Async safety:   ✅ ``load()`` and ``refresh()`` are pure coroutines with no
                       blocking calls.  Safe to call concurrently from multiple tasks.

    Args:
        authority: ``JwtAuthority`` or ``MultiKeyAuthority`` to wrap.
        issuer:    The ``iss`` claim value for this authority's tokens.
                   Used to build ``source_id`` — should equal ``authority.issuer``.

    Edge cases:
        - After ``MultiKeyAuthority.rotate()``, the next ``refresh()`` call will
          include the new key and the old one — both are visible in ``jwks()``.
        - After ``MultiKeyAuthority.retire(kid)``, the retired kid disappears
          from ``jwks()`` output.  The next refresh propagates the removal to the
          registry.  Tokens signed with the retired kid will then fail verification
          — only call ``retire()`` after all such tokens have expired.
        - ``JwtAuthority`` has a single key — ``jwks()`` always returns exactly one
          key; rotation is handled externally (swap the ``JwtAuthority`` instance).
    """

    # __slots__ prevents accidental attribute addition and saves memory —
    # AuthoritySource is created once per signing authority.
    __slots__ = ("_authority", "_source_id")

    def __init__(
        self,
        authority: JwtAuthority | MultiKeyAuthority,
        *,
        issuer: str,
    ) -> None:
        """
        Args:
            authority: The signing authority to wrap.  Must implement the
                       ``_SigningAuthority`` protocol (i.e. have ``.issuer``
                       and ``.jwks()``).  Pass a ``JwtAuthority`` or
                       ``MultiKeyAuthority``.
            issuer:    The ``iss`` claim value for this authority's tokens.
                       Should equal ``authority.issuer`` — passed explicitly
                       to avoid calling ``authority.issuer`` at construction
                       time, which is a minor optimisation for hot paths.

        Raises:
            Nothing — authority is stored as-is; any protocol mismatch will
            only surface at ``load()``/``refresh()`` call time.

        Edge cases:
            - The ``authority`` reference is stored directly — not copied.
              Rotation via ``MultiKeyAuthority.rotate()`` is automatically
              reflected in subsequent ``load()``/``refresh()`` calls.
        """
        # Store a live reference — mutations to MultiKeyAuthority (rotate/retire)
        # are reflected automatically because we always call .jwks() on demand.
        self._authority = authority
        # Prefix "authority::" distinguishes in-memory sources from file/URL
        # sources in logs and __repr__ output.
        self._source_id: str = f"authority::{issuer}"

    @property
    def source_id(self) -> str:
        """Unique identifier for this source — used in logs and __repr__."""
        return self._source_id

    async def load(self) -> JsonWebKeySet:
        """
        Return the authority's current public keyset.

        For in-memory authorities there is nothing to "load" — this is a
        synchronous keyset snapshot wrapped in a coroutine to satisfy the
        ``IssuerSource`` protocol.

        Returns:
            Current ``JsonWebKeySet`` from the wrapped authority.  For
            ``MultiKeyAuthority`` this includes all registered keys (active
            and any keys retained for in-flight token verification).

        Edge cases:
            - Always succeeds — no I/O means no network failures.
            - Returns all keys currently registered in the authority, including
              ones retained after rotation but not yet retired.
        """
        # jwks() is pure CPU — no need for asyncio.to_thread().
        # The Protocol guarantees both JwtAuthority and MultiKeyAuthority
        # implement jwks() and return JsonWebKeySet.
        return self._authority.jwks()

    async def refresh(self) -> JsonWebKeySet:
        """
        Return the authority's current public keyset (always up-to-date).

        Unlike JWKS URL sources, there is no stale cache to invalidate —
        the in-memory authority is always current.  For ``MultiKeyAuthority``,
        calling ``refresh()`` after ``rotate()`` will include the newly added
        key in the returned keyset.

        Returns:
            Current ``JsonWebKeySet`` — reflects any rotations or retirements
            that have occurred since the last call.

        Edge cases:
            - Calling ``refresh()`` after ``retire(kid)`` returns the smaller
              keyset (without the retired kid).  The registry must propagate
              this change — tokens signed with the retired kid will fail
              verification on the next ``registry.verify()`` call.
        """
        # No caching needed — in-memory authorities always return current state.
        # This is the key advantage over URL sources: zero round-trip cost.
        return self._authority.jwks()

    def __repr__(self) -> str:
        return f"AuthoritySource(source_id={self._source_id!r})"
