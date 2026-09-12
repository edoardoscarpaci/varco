"""
varco_core.auth.api_key
========================

Offline, stdlib-only API-key hashing primitives — ``hash_api_key()`` /
``verify_api_key()`` — backing ``ApiKeyAuth``'s ``hashed_keys=`` path
(Plan 034 / S14).

These live in ``varco_core``, not ``varco_fastapi``, so an application can
pre-hash a key at issuance time (e.g. in a CLI, a migration script, an admin
job) without importing a web framework at all.

DESIGN: SHA-256 (optionally HMAC-SHA-256 under a process pepper), never a
password-hashing KDF (argon2/bcrypt/scrypt) (§D-S14-algo)
    ✅ Brief 009 §6 cites NIST SP 800-63B Rev 4 (2024): a "look-up secret"
       with >= 112 bits of entropy SHALL be hashed with an approved
       one-way function — only *low*-entropy secrets need a deliberately
       slow KDF. A varco-issued API key is a high-entropy random token, not
       a human-chosen password.
    ✅ Brief 009 §Part-B calls argon2/bcrypt for API keys "a misapplication
       of password-hashing guidance" — accepted in this codebase as an
       anti-pattern to avoid, not a stricter-is-always-better default.
    ❌ SHA-256 alone offers no defense against a precomputed rainbow table.
       Mitigated: a rainbow table over a >=112-bit random token is not a
       credible threat (see the pepper DESIGN block below) — the salt/KDF
       machinery that defeats rainbow tables exists for *low*-entropy
       secrets, which this is not.
    Rejected — argon2/bcrypt/scrypt: ❌ deliberately slow, which is a
      *liability* on the auth hot path (every request pays it), for a
      precomputation defense that a high-entropy secret does not need.

DESIGN: a single process-wide pepper (HMAC) over a per-key random salt
    Brief 009 §Part-B's own recommendation (SHA-256 + per-key salt) is aimed
    at a *repository-backed* key store that can also impose a lookup-index
    prefix convention (brief 009 §7). ``ApiKeyAuth`` is an in-memory map with
    no such convention, so:
    ✅ A per-key salt would force an O(n) linear scan over every configured
       key on every request (no way to know which salt to try without
       already knowing which key matched) — exactly the cost brief 009 §6
       says fast hashing exists to avoid.
    ✅ A deterministic HMAC-SHA-256 keyed by a single process pepper is
       still directly dict-indexable: O(1) candidate lookup, then
       ``hmac.compare_digest`` for the actual accept decision (§D-S14-compare).
       Brief 009 §Part-B lists this as its second option ("adds a
       server-side pepper... reducing rainbow-table risk without per-key
       overhead").
    ❌ The pepper must be identical everywhere ``hash_api_key()`` is called
       offline and wherever ``ApiKeyAuth`` verifies at runtime — a mismatch
       silently rejects every key. Pitfalls-table row
       (``technical_docs/features/credential-and-token-lifecycle.md``); the
       error message here never echoes the pepper or the key.
    ❌ Rotating the pepper invalidates every stored digest at once. Accepted:
       rotation means re-hashing from the plaintext source, exactly like any
       other credential rotation the application already owns.
    Rejected — per-key random salt + linear scan: ❌ O(n) per request in the
      auth hot path for a defense high-entropy secrets do not need.

Digests are stored prefixed with their scheme (``"sha256$<hex>"`` /
``"hmac-sha256$<hex>"``) so a future scheme is distinguishable at a glance
and a scheme mismatch is diagnosable rather than a silent false negative.

Thread safety:  Pure functions, no shared state — safe from any thread.
Async safety:   Synchronous, CPU-only (hashing a short string) — safe to
                call directly from an async context; never worth an
                executor hop.
"""

from __future__ import annotations

import hashlib
import hmac

_SHA256_SCHEME = "sha256"
_HMAC_SHA256_SCHEME = "hmac-sha256"


def hash_api_key(raw: str, *, pepper: bytes | str | None = None) -> str:
    """
    Hash a raw API key for at-rest storage / constant-time comparison.

    Args:
        raw:    The plaintext API key. Must be non-empty.
        pepper: Optional process-wide secret (bytes or str). When provided,
                the digest is ``HMAC-SHA-256(pepper, raw)`` instead of plain
                ``SHA-256(raw)``. Also settable via ``VARCO_API_KEY_PEPPER``
                by ``ApiKeyAuth`` itself — this function takes it explicitly
                so it can be used entirely offline (no env-var coupling).

    Returns:
        A scheme-prefixed hex digest: ``"sha256$<hex>"`` (no pepper) or
        ``"hmac-sha256$<hex>"`` (with a pepper).

    Raises:
        ValueError: ``raw`` is empty.

    Edge cases:
        - The same ``raw`` with and without a pepper produces different
          digests, by design (they're different schemes).

    Example::

        digest = hash_api_key("sk_live_abc123", pepper=os.environb[b"VARCO_API_KEY_PEPPER"])
        # store `digest`, discard the plaintext
    """
    if not raw:
        raise ValueError("hash_api_key(): raw API key must be a non-empty string")

    raw_bytes = raw.encode("utf-8")
    if pepper is None:
        digest = hashlib.sha256(raw_bytes).hexdigest()
        return f"{_SHA256_SCHEME}${digest}"

    pepper_bytes = pepper.encode("utf-8") if isinstance(pepper, str) else pepper
    digest = hmac.new(pepper_bytes, raw_bytes, hashlib.sha256).hexdigest()
    return f"{_HMAC_SHA256_SCHEME}${digest}"


def verify_api_key(raw: str, digest: str, *, pepper: bytes | str | None = None) -> bool:
    """
    Verify a raw API key against a stored, scheme-prefixed digest.

    Args:
        raw:    The plaintext API key presented by the caller.
        digest: A digest previously produced by ``hash_api_key()``
                (``"sha256$<hex>"`` or ``"hmac-sha256$<hex>"``).
        pepper: The same pepper (if any) used to produce ``digest``.

    Returns:
        ``True`` iff ``hash_api_key(raw, pepper=pepper)`` matches ``digest``
        under a constant-time comparison. ``False`` for any mismatch,
        including a mismatched pepper.

    Raises:
        ValueError: ``digest`` carries an unrecognized scheme prefix — this
                    is a configuration error (a corrupt or foreign digest),
                    never a verification outcome, so it is not conflated
                    with ``False``. The message names the offending scheme,
                    never the digest itself.

    Edge cases:
        - A one-character difference in ``raw`` yields ``False``, never a
          partial match — the whole point of hashing before comparing.

    DESIGN: ``hmac.compare_digest`` over ``==`` (§D-S14-compare)
        ✅ Brief 009 §9: ``==`` on a credential is a timing oracle — string
           comparison in CPython short-circuits on the first differing byte,
           leaking information about how many leading bytes matched.
           ``hmac.compare_digest`` runs in constant time relative to the
           length of the inputs.
        ❌ Both operands here are the same-length ASCII hex digest, so a
           length-leak (a distinct failure mode from a byte-leak) never
           arises to begin with; ``compare_digest`` is applied for defense
           in depth and because it is the documented, unambiguous primitive
           brief 009 cites, not because a length attack is otherwise live
           here.

    Example::

        if not verify_api_key(presented_key, stored_digest, pepper=pepper):
            raise HTTPException(401, "Invalid API key")
    """
    try:
        scheme, _, _ = digest.partition("$")
    except AttributeError:  # pragma: no cover - digest is always a str in practice
        raise ValueError("verify_api_key(): digest must be a string") from None

    if scheme not in (_SHA256_SCHEME, _HMAC_SHA256_SCHEME):
        raise ValueError(
            f"verify_api_key(): unrecognized digest scheme {scheme!r} "
            "(expected 'sha256' or 'hmac-sha256')"
        )

    try:
        expected = hash_api_key(raw, pepper=pepper)
    except ValueError:
        return False

    return hmac.compare_digest(expected, digest)
