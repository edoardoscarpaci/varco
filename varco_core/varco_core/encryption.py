"""
varco_core.encryption
=====================
Field-level at-rest encryption for domain model fields and arbitrary payloads.

Architecture
------------
Three layers, each usable independently:

1. ``FieldEncryptor`` — structural protocol any encryptor must satisfy.
   All implementations accept an optional ``context`` string (e.g. a tenant
   ID or key-ID hint) so that the mapper can pass domain-level metadata without
   changing the interface between simple and complex encryptors.

2. Concrete encryptors:

   - ``FernetFieldEncryptor``       — single-key AES-128-CBC + HMAC-SHA256.
   - ``MultiKeyEncryptorRegistry``  — multiple keys with O(1) decryption routing
     via an embedded ``kid`` header.  Designed for zero-downtime key rotation:
     register a new key → promote it to primary → retire the old one after
     re-encryption.
   - ``TenantAwareEncryptorRegistry`` — per-tenant keys.  Each tenant maps to
     any ``FieldEncryptor`` (including a ``MultiKeyEncryptorRegistry`` for that
     tenant).  Falls back to an optional default encryptor for unregistered
     tenants.

3. ``encrypt_payload`` / ``decrypt_payload`` — thin wrappers for encrypting
   arbitrary bytes outside the domain model context (events, cache values,
   JWT claims, etc.).

Wire format for ``MultiKeyEncryptorRegistry``
---------------------------------------------
To route decryption to the correct key without trying every registered
encryptor (O(N)), the kid is embedded in the stored ciphertext::

    | 4 bytes (big-endian uint32) | N bytes     | M bytes        |
    | kid_length                  | kid (UTF-8) | raw ciphertext |

This allows O(1) decryption: extract the kid → look up the encryptor → decrypt.
The format is self-describing, so decryption works even after the primary key
has been rotated or retired.

Usage example — single key::

    from cryptography.fernet import Fernet
    from varco_core.encryption import FernetFieldEncryptor

    encryptor = FernetFieldEncryptor(Fernet.generate_key())

Usage example — key rotation::

    from varco_core.encryption import MultiKeyEncryptorRegistry, FernetFieldEncryptor

    reg = MultiKeyEncryptorRegistry(
        primary_kid="v1",
        primary_encryptor=FernetFieldEncryptor(key_v1),
    )
    # Zero-downtime rotation:
    reg.register("v2", FernetFieldEncryptor(key_v2))
    reg.set_primary("v2")   # new writes use v2; old v1 ciphertext still decryptable
    reg.retire("v1")        # only after re-encrypting all rows with v1

Usage example — per-tenant keys::

    from varco_core.encryption import TenantAwareEncryptorRegistry

    tenant_reg = TenantAwareEncryptorRegistry(default=shared_fallback)
    tenant_reg.register("acme", FernetFieldEncryptor(acme_key))
    tenant_reg.register("globex", FernetFieldEncryptor(globex_key))

    # The mapper calls: encryptor.encrypt(plaintext, context="acme")
"""

from __future__ import annotations

import base64
import dataclasses
import struct
from datetime import datetime
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from varco_core.jwk.model import JsonWebKey


# ════════════════════════════════════════════════════════════════════════════════
# FieldEncryptor — structural protocol
# ════════════════════════════════════════════════════════════════════════════════


@runtime_checkable
class FieldEncryptor(Protocol):
    """
    Structural protocol for all field-level encryptors.

    The ``context`` keyword argument carries domain-level metadata (e.g. a
    tenant ID or a caller-supplied key hint).  Implementations that do not
    need context simply ignore it — making the simpler ``FernetFieldEncryptor``
    and the context-aware ``TenantAwareEncryptorRegistry`` satisfy the same
    protocol without an adapter.

    DESIGN: context as keyword-only str | None
      ✅ Optional — simple single-key encryptors need no changes
      ✅ Avoids a parallel "contextual" protocol — one protocol fits all
      ✅ The mapper passes context only when a ``tenant_id_field`` is configured
      ❌ Callers that pass the wrong context type get no static error (Protocol
         is structural, not parametric — callers are responsible for consistency)

    Thread safety:  ✅ All implementations must be safe to call concurrently.
                    ``FernetFieldEncryptor`` is stateless per-call.
                    ``MultiKeyEncryptorRegistry`` and ``TenantAwareEncryptorRegistry``
                    hold a mutable registry dict — safe to read concurrently
                    after startup writes; use external locking for live rotation.
    Async safety:   ✅ All implementations are synchronous CPU-only operations.
                    Safe to call from async context without ``run_in_executor``.
    """

    def encrypt(self, plaintext: bytes, *, context: str | None = None) -> bytes:
        """
        Encrypt ``plaintext`` and return ciphertext bytes.

        Args:
            plaintext: Raw bytes to encrypt.
            context:   Optional routing hint (e.g. tenant ID, caller label).
                       Ignored by implementations that use a single key.

        Returns:
            Opaque ciphertext bytes.  The format is implementation-defined.

        Raises:
            EncryptionError: Wraps any implementation-level failure.
        """
        ...

    def decrypt(self, ciphertext: bytes, *, context: str | None = None) -> bytes:
        """
        Decrypt ``ciphertext`` and return the original plaintext bytes.

        Args:
            ciphertext: Bytes previously produced by ``encrypt()``.
            context:    Same value that was passed to ``encrypt()`` for this
                        record.  Ignored by single-key implementations.

        Returns:
            Original plaintext bytes.

        Raises:
            EncryptionError: Wraps key-not-found, wrong key, or tampered data.
        """
        ...


# ════════════════════════════════════════════════════════════════════════════════
# Custom exception
# ════════════════════════════════════════════════════════════════════════════════


class EncryptionError(Exception):
    """
    Raised when an encrypt or decrypt operation fails.

    Wraps the underlying implementation exception (e.g.
    ``cryptography.fernet.InvalidToken``, ``KeyError`` for unknown kid/tenant)
    via exception chaining so callers can inspect the root cause.

    Attributes:
        operation: ``"encrypt"`` or ``"decrypt"`` — which direction failed.
        context:   The context value that was passed to the encryptor, if any.
    """

    def __init__(
        self,
        message: str,
        *,
        operation: str,
        context: str | None = None,
    ) -> None:
        self.operation = operation
        self.context = context
        super().__init__(message)


class KeyDestroyedError(EncryptionError):
    """
    Raised when decrypting ciphertext framed with a **destroyed** (crypto-
    shredded) kid — Phase 1, U-2.

    Distinguishable from the generic ``EncryptionError`` an unknown/retired
    kid raises: a caller can render "this data was erased" instead of
    "corrupt data" only if the two are told apart. A bare ``except
    EncryptionError`` still catches this (it is a subclass), but a caller
    checking the specific type can react differently.

    Attributes:
        kid:   The destroyed key ID found in the ciphertext header.
        scope: The scope the destroyed key belonged to, when known
               (forwarded from the ``context`` the decrypting registry used).

    DESIGN: destroy() vs retire() — two verbs, two contracts
      ✅ ``retire(kid)`` removes a key from primary rotation but keeps decrypt
         working — for the "re-encrypt everything, then drop the old key"
         rotation workflow.
      ✅ ``destroy(kid)`` records the kid as destroyed so decrypt raises this
         error — for the "this data subject's data must become unreadable"
         crypto-shredding workflow (U-2). The distinction is the whole point:
         conflating them would make rotation destructive or shredding
         reversible.
      ❌ Two verbs on the same registry is one more thing to document — worth
         it because they answer different questions ("can we still read old
         data?" vs "must this data become unreadable now?").
    """

    def __init__(
        self,
        message: str,
        *,
        operation: str,
        context: str | None = None,
        kid: str | None = None,
        scope: str | None = None,
    ) -> None:
        self.kid = kid
        self.scope = scope
        super().__init__(message, operation=operation, context=context)


@dataclasses.dataclass(frozen=True)
class DestroyReceipt:
    """
    Audit record returned by a successful crypto-shred (Phase 1, U-2).

    ⚠️ DEVIATION from the gap register's sketch: the register sketches this as
    a ``NamedTuple``. This repo's convention (CLAUDE.md: "Frozen
    ``@dataclass(frozen=True)`` for all value objects") wins over the filer's
    sketch — a frozen dataclass gets the same immutability with a proper
    ``to_dict()`` method and clearer field docs.

    Attributes:
        scope:        The scope that was destroyed.
        kids:         Every kid tombstoned by this call. Empty on a repeat
                       call for an already-destroyed scope (idempotent) or an
                       unknown scope (not an error).
        destroyed_at: UTC timestamp of the destroy operation.
        actor:        Optional identity that performed the destruction, for
                       audit trails. Not persisted by ``EncryptionKeyStore``
                       itself — callers that need it durable must record it
                       themselves (e.g. via the audit log, Plan 004).
    """

    scope: str
    kids: tuple[str, ...]
    destroyed_at: datetime
    actor: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialise to a JSON-compatible dict — e.g. for an audit log entry."""
        return {
            "scope": self.scope,
            "kids": list(self.kids),
            "destroyed_at": self.destroyed_at.isoformat(),
            "actor": self.actor,
        }


# ════════════════════════════════════════════════════════════════════════════════
# FernetFieldEncryptor — single-key AES-128-CBC + HMAC-SHA256
# ════════════════════════════════════════════════════════════════════════════════


class FernetFieldEncryptor:
    """
    Single-key encryptor using ``cryptography.fernet.Fernet``.

    Fernet provides AES-128-CBC encryption with HMAC-SHA256 authentication.
    It is a symmetric scheme — the same key is used for encrypt and decrypt.

    ``context`` is intentionally ignored.  This encryptor holds exactly one
    key and does not distinguish between tenants or key versions.

    Args:
        key: A 32-byte URL-safe base64-encoded Fernet key.
             Generate with ``Fernet.generate_key()``.

    DESIGN: lazy import of ``cryptography``
      ✅ Projects not using encryption pay zero import cost
      ✅ ``cryptography`` stays an optional dependency in pyproject.toml
      ❌ Import errors surface at construction time, not at module import time

    Thread safety:  ✅ Fernet is stateless per-call — safe to share across threads.
    Async safety:   ✅ CPU-only; safe to call from async context.

    Example::

        from cryptography.fernet import Fernet
        enc = FernetFieldEncryptor(Fernet.generate_key())
        ct = enc.encrypt(b"secret")
        pt = enc.decrypt(ct)
        assert pt == b"secret"

    Edge cases:
        - Decrypting with the wrong key raises ``EncryptionError`` wrapping
          ``cryptography.fernet.InvalidToken``.
        - Fernet tokens are time-stamped.  The timestamp is informational only —
          ``FernetFieldEncryptor`` does not enforce TTLs.  Use
          ``Fernet.decrypt(token, max_age=...)`` directly if you need TTLs.
    """

    def __init__(self, key: bytes) -> None:
        # Lazy import — cryptography is optional; surfaces error at construction
        # time so the user gets a clear ImportError pointing to the missing dep.
        try:
            from cryptography.fernet import Fernet as _Fernet
        except ImportError as exc:
            raise ImportError(
                "FernetFieldEncryptor requires the 'cryptography' package. "
                "Install it with: pip install 'varco-core[crypto]'  "
                "or: pip install cryptography"
            ) from exc

        self._fernet = _Fernet(key)

    def __repr__(self) -> str:
        return "FernetFieldEncryptor(<key hidden>)"

    def encrypt(self, plaintext: bytes, *, context: str | None = None) -> bytes:
        """
        Encrypt ``plaintext`` with Fernet.

        Args:
            plaintext: Bytes to encrypt.
            context:   Ignored — single-key encryptor has no routing.

        Returns:
            Fernet token as bytes.

        Raises:
            EncryptionError: If Fernet raises during encryption.
        """
        try:
            return self._fernet.encrypt(plaintext)
        except Exception as exc:
            raise EncryptionError(
                f"Fernet encryption failed: {exc}",
                operation="encrypt",
                context=context,
            ) from exc

    def decrypt(self, ciphertext: bytes, *, context: str | None = None) -> bytes:
        """
        Decrypt a Fernet token.

        Args:
            ciphertext: Fernet token produced by ``encrypt()``.
            context:    Ignored.

        Returns:
            Original plaintext bytes.

        Raises:
            EncryptionError: Wrong key, tampered token, or malformed token
                wrapping ``cryptography.fernet.InvalidToken``.
        """
        try:
            return self._fernet.decrypt(ciphertext)
        except Exception as exc:
            raise EncryptionError(
                "Fernet decryption failed — wrong key or tampered ciphertext.",
                operation="decrypt",
                context=context,
            ) from exc


# ════════════════════════════════════════════════════════════════════════════════
# PlaintextFieldEncryptor — the acknowledged dev/test identity encryptor (Plan 056)
# ════════════════════════════════════════════════════════════════════════════════


class PlaintextFieldEncryptor:
    """
    ⛔ DEV/TEST ONLY. Identity ``FieldEncryptor`` — ``encrypt()`` and
    ``decrypt()`` return their input unchanged.

    Exists so a durable ``WebhookSubscriptionRepository`` (whose
    ``encryptor`` is required since 4.0, Plan 056 / FLIP-13) can be
    constructed without a real key in a local run — through the hand path
    *or* the DI path (``bind_instance(container, FieldEncryptor,
    PlaintextFieldEncryptor(acknowledge_plaintext=True))``) — and so that
    choice is greppable (``rg -n PlaintextFieldEncryptor``) and reported by
    ``SecurityPosture`` as ``posture.webhook_secrets_plaintext`` (WARN —
    blocks a production start unless suppressed) — automatically in a default
    app, whose ``LocalPostureCollector`` resolves the bound
    ``WebhookSubscriptionRepository`` from the container.

    Args:
        acknowledge_plaintext: Must be literally ``True`` — an explicit,
            in-source statement that the values it "protects" are stored
            in plaintext.

    Raises:
        TypeError: ``acknowledge_plaintext`` is omitted, or anything other
            than the ``True`` singleton (``False``, ``1``, ``"yes"``).

    DESIGN: ``Literal[True]`` + ``TypeError`` — Plan 053's
        ``acknowledge_unverified`` shape
        ✅ A ``bool`` whose only legal value is ``True`` says so in the
           annotation (mypy rejects ``False``) and at runtime (identity
           check, so ``1`` is refused too).
        ✅ A real ``FieldEncryptor``: the repositories keep one constructor
           contract and the wire format stays the hex-of-bytes envelope every
           real encryptor uses, so switching to Fernet later needs only
           ``reencrypt_legacy_plaintext(legacy_hex=True)``.
        ❌ A class named ``…Encryptor`` that encrypts nothing — mitigated by
           the name, this ⛔ line, the shouting ``__repr__``, the
           acknowledgement, and the posture finding.
        Rejected: a silent ``NullFieldEncryptor`` (no acknowledgement — the
        three-word name is the point); placing it in ``varco_core.webhook``
        (it is a ``FieldEncryptor``; implementations live here).

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ No I/O.

    Example::

        repo = SAWebhookSubscriptionRepository(
            url="sqlite+aiosqlite:///dev.db",
            encryptor=PlaintextFieldEncryptor(acknowledge_plaintext=True),
        )
    """

    def __init__(self, *, acknowledge_plaintext: Literal[True]) -> None:
        if acknowledge_plaintext is not True:
            raise TypeError(
                "PlaintextFieldEncryptor: acknowledge_plaintext must be literally True — "
                "this encryptor stores plaintext (dev/test only; use FernetFieldEncryptor(key) "
                "in production)."
            )

    def __repr__(self) -> str:
        return "PlaintextFieldEncryptor(<PLAINTEXT — dev/test only>)"

    def encrypt(self, plaintext: bytes, *, context: str | None = None) -> bytes:
        """
        Return ``plaintext`` unchanged.

        Args:
            plaintext: Bytes to "encrypt".
            context:   Ignored.

        Returns:
            ``plaintext``.
        """
        return plaintext

    def decrypt(self, ciphertext: bytes, *, context: str | None = None) -> bytes:
        """
        Return ``ciphertext`` unchanged.

        Args:
            ciphertext: Bytes to "decrypt".
            context:    Ignored.

        Returns:
            ``ciphertext``.
        """
        return ciphertext


# ════════════════════════════════════════════════════════════════════════════════
# MultiKeyEncryptorRegistry — key rotation with O(1) decryption routing
# ════════════════════════════════════════════════════════════════════════════════

# Wire format constants — keep in sync with _pack_ciphertext / _unpack_ciphertext.
_KID_LEN_STRUCT = struct.Struct(">I")  # 4-byte big-endian uint32 for kid length


def _pack_ciphertext(kid: str, raw_ciphertext: bytes) -> bytes:
    """
    Pack a kid + raw ciphertext into the multi-key wire format.

    Format: [4 bytes: kid_len][kid_bytes: UTF-8][raw_ciphertext]

    Args:
        kid:            Key identifier string.
        raw_ciphertext: Ciphertext produced by the underlying encryptor.

    Returns:
        Framed bytes ready to store in the DB.
    """
    kid_bytes = kid.encode("utf-8")
    # struct.pack produces exactly 4 bytes for the length field
    return _KID_LEN_STRUCT.pack(len(kid_bytes)) + kid_bytes + raw_ciphertext


def _unpack_ciphertext(framed: bytes) -> tuple[str, bytes]:
    """
    Unpack the multi-key wire format back into (kid, raw_ciphertext).

    Args:
        framed: Bytes produced by ``_pack_ciphertext``.

    Returns:
        ``(kid, raw_ciphertext)``

    Raises:
        EncryptionError: Framed bytes are too short or malformed.
    """
    header_size = _KID_LEN_STRUCT.size  # always 4
    if len(framed) < header_size:
        raise EncryptionError(
            f"Ciphertext is too short to contain a kid header "
            f"(got {len(framed)} bytes, expected at least {header_size}).",
            operation="decrypt",
        )
    (kid_len,) = _KID_LEN_STRUCT.unpack(framed[:header_size])
    total_header = header_size + kid_len
    if len(framed) < total_header:
        raise EncryptionError(
            f"Ciphertext header declares kid_len={kid_len} but only "
            f"{len(framed) - header_size} bytes follow the length field.",
            operation="decrypt",
        )
    kid = framed[header_size:total_header].decode("utf-8")
    raw_ciphertext = framed[total_header:]
    return kid, raw_ciphertext


class MultiKeyEncryptorRegistry:
    """
    Registry of named ``FieldEncryptor`` instances for zero-downtime key rotation.

    Encryption always uses the *primary* key.  Decryption routes to the correct
    key by reading the ``kid`` embedded in the ciphertext header — O(1) regardless
    of how many keys are registered.

    Key rotation workflow
    ---------------------
    1. Generate a new key and register it::

           reg.register("v2", FernetFieldEncryptor(new_key))

    2. Promote it to primary so new writes use v2::

           reg.set_primary("v2")

    3. Re-encrypt all existing DB rows that carry the v1 header.
       (This step is application-level — varco provides no bulk re-encryptor.)

    4. Retire v1 once no rows reference it::

           reg.retire("v1")

    Args:
        primary_kid:       Key ID for the initial primary encryptor.
        primary_encryptor: The encryptor associated with ``primary_kid``.

    DESIGN: embedded kid header over MultiFernet try-each approach
      ✅ O(1) decryption regardless of registry size
      ✅ Auditable — every stored record carries its kid explicitly
      ✅ Compatible with any FieldEncryptor impl, not just Fernet
      ❌ Stored format differs from raw Fernet tokens — cannot swap
         MultiKeyEncryptorRegistry in/out without a data migration

    Thread safety:  ⚠️ ``register``, ``set_primary``, and ``retire`` are NOT
                    thread-safe — call them at startup before serving requests.
                    ``encrypt`` and ``decrypt`` are safe to call concurrently
                    after the registry is fully configured.
    Async safety:   ✅ ``encrypt`` / ``decrypt`` are CPU-only; safe from async.

    Edge cases:
        - Retiring the current primary raises ``ValueError`` — you must promote
          another key first.
        - Decrypting a record whose kid has been retired raises ``EncryptionError``
          — retire only after confirming no rows reference that kid.
        - Registering a kid that already exists silently replaces the encryptor
          (useful for reloading a rotated key from a vault without restarting).
    """

    def __init__(
        self,
        primary_kid: str,
        primary_encryptor: FieldEncryptor,
    ) -> None:
        # _encryptors maps kid → encryptor; plain dict is safe after startup
        self._encryptors: dict[str, FieldEncryptor] = {primary_kid: primary_encryptor}
        self._primary_kid: str = primary_kid
        # kids marked destroyed (crypto-shredded) — checked before dispatch in
        # decrypt(). Unlike retire(), destroy() does NOT remove the encryptor
        # from _encryptors — see KeyDestroyedError's DESIGN block.
        self._destroyed: set[str] = set()

    def __repr__(self) -> str:
        kids = list(self._encryptors.keys())
        return f"MultiKeyEncryptorRegistry(primary={self._primary_kid!r}, registered={kids})"

    def register(self, kid: str, encryptor: FieldEncryptor) -> None:
        """
        Add or replace a named encryptor.

        Silently replaces an existing entry — useful for hot-reloading a key
        from a vault without changing the kid.

        Args:
            kid:       Unique key identifier (e.g. ``"v2"``, ``"2025-01"``).
            encryptor: Any ``FieldEncryptor`` implementation.
        """
        self._encryptors[kid] = encryptor

    def set_primary(self, kid: str) -> None:
        """
        Promote ``kid`` to the active key used for all new encryptions.

        Args:
            kid: Must already be registered via ``register()``.

        Raises:
            KeyError: ``kid`` is not registered.
        """
        if kid not in self._encryptors:
            raise KeyError(
                f"Cannot promote {kid!r} to primary — it is not registered. "
                f"Registered kids: {list(self._encryptors.keys())}. "
                "Call register() first."
            )
        self._primary_kid = kid

    def retire(self, kid: str) -> None:
        """
        Remove a key from the registry.

        Only call this after all DB rows encrypted with ``kid`` have been
        re-encrypted with the current primary.  After retirement, any attempt
        to decrypt a row with that kid raises ``EncryptionError``.

        Args:
            kid: Key identifier to remove.

        Raises:
            ValueError: Attempting to retire the current primary.
            KeyError:   ``kid`` is not registered.
        """
        if kid == self._primary_kid:
            raise ValueError(
                f"Cannot retire {kid!r} — it is the current primary key. "
                "Promote another key with set_primary() before retiring this one."
            )
        if kid not in self._encryptors:
            raise KeyError(
                f"Cannot retire {kid!r} — it is not registered. "
                f"Registered kids: {list(self._encryptors.keys())}."
            )
        del self._encryptors[kid]

    def destroy(self, kid: str) -> None:
        """
        Mark ``kid`` as crypto-shredded (Phase 1, U-2) — distinct from
        ``retire()``.

        After this call, ``decrypt()`` on ciphertext framed with ``kid``
        raises ``KeyDestroyedError`` instead of succeeding or raising the
        generic "kid not registered" ``EncryptionError``. The encryptor
        mapping is left intact (unlike ``retire()``, which deletes it) — the
        point is not to save memory, it is to make the failure
        distinguishable.

        Args:
            kid: Key identifier to destroy. No-op (not an error) if ``kid``
                 was never registered — mirrors ``EncryptionKeyStore.
                 destroy_scope``'s "unknown scope is not an error" contract.

        Edge cases:
            - Calling ``destroy()`` twice for the same ``kid`` is idempotent.
            - A destroyed kid that is also the current primary can still be
              used to *encrypt* new data — ``destroy()`` only affects
              ``decrypt()``. Callers driving crypto-shredding should also
              call ``set_primary()`` to a different kid first if they want to
              stop new writes under the destroyed kid too.
        """
        self._destroyed.add(kid)

    def encrypt(self, plaintext: bytes, *, context: str | None = None) -> bytes:
        """
        Encrypt with the primary key and embed the kid in the result.

        Args:
            plaintext: Bytes to encrypt.
            context:   Ignored — the primary kid is always used for encryption.

        Returns:
            Framed ciphertext: ``[kid_len][kid][raw_ciphertext]``.

        Raises:
            EncryptionError: Underlying encryptor failed.
        """
        # Always encrypt with primary — kid is embedded so decrypt can route
        raw = self._encryptors[self._primary_kid].encrypt(plaintext, context=context)
        return _pack_ciphertext(self._primary_kid, raw)

    def decrypt(self, ciphertext: bytes, *, context: str | None = None) -> bytes:
        """
        Unpack the kid header, route to the correct encryptor, and decrypt.

        Args:
            ciphertext: Framed bytes produced by ``encrypt()``.
            context:    Forwarded to the resolved encryptor (usually ignored).

        Returns:
            Original plaintext bytes.

        Raises:
            KeyDestroyedError: ``kid`` was crypto-shredded via ``destroy()``
                — a subclass of ``EncryptionError``, so existing
                ``except EncryptionError`` call sites keep working.
            EncryptionError: Malformed header, retired/unknown kid, or wrong
                key. ``destroy()`` and an unknown kid are deliberately kept
                distinguishable (see ``KeyDestroyedError``) — do not merge
                these two branches.
        """
        kid, raw_ciphertext = _unpack_ciphertext(ciphertext)
        if kid in self._destroyed:
            raise KeyDestroyedError(
                f"Decryption failed — kid {kid!r} has been destroyed "
                f"(crypto-shredded). This ciphertext's data subject has been "
                f"erased; it is not recoverable.",
                operation="decrypt",
                context=context,
                kid=kid,
                scope=context,
            )
        encryptor = self._encryptors.get(kid)
        if encryptor is None:
            raise EncryptionError(
                f"Decryption failed — kid {kid!r} is not registered "
                f"(it may have been retired). "
                f"Registered kids: {list(self._encryptors.keys())}. "
                "Ensure all rows encrypted with this kid are re-encrypted "
                "before retiring it.",
                operation="decrypt",
                context=context,
            )
        return encryptor.decrypt(raw_ciphertext, context=context)


# ════════════════════════════════════════════════════════════════════════════════
# TenantAwareEncryptorRegistry — per-tenant key isolation
# ════════════════════════════════════════════════════════════════════════════════


class TenantAwareEncryptorRegistry:
    """
    Per-tenant encryption key isolation.

    Each tenant ID maps to its own ``FieldEncryptor`` — which may itself be a
    ``MultiKeyEncryptorRegistry`` for per-tenant key rotation.  An optional
    ``default`` encryptor handles tenants that are not explicitly registered.

    The mapper passes the tenant ID as ``context`` when calling
    ``encrypt``/``decrypt``.  Configure the mapper with ``tenant_id_field``
    to enable this::

        orm_cls, mapper = factory.build(
            Patient,
            encryptor=tenant_reg,
            tenant_id_field="tenant_id",
        )

    Args:
        default: Fallback encryptor for tenants without an explicit entry.
                 ``None`` means unregistered tenants raise ``EncryptionError``.

    DESIGN: context as tenant_id
      ✅ No change to FieldEncryptor interface — context is a plain str
      ✅ Each tenant's key is completely isolated — cross-tenant read impossible
      ✅ Composable with MultiKeyEncryptorRegistry for per-tenant key rotation
      ❌ The mapper must know which field holds the tenant_id (tenant_id_field)
         — this is a mapper-level concern, not a pure encryption concern

    Thread safety:  ⚠️ ``register`` is NOT thread-safe — call at startup.
                    ``encrypt`` / ``decrypt`` are safe to call concurrently.
    Async safety:   ✅ CPU-only; safe from async.

    Edge cases:
        - ``context=None`` (no tenant) uses the default encryptor if set;
          otherwise raises ``EncryptionError``.
        - An unregistered tenant_id uses the default encryptor if set;
          otherwise raises ``EncryptionError`` — never silently bypasses encryption.
        - Registering the same tenant_id twice silently replaces the encryptor.

    Example::

        from varco_core.encryption import (
            TenantAwareEncryptorRegistry,
            MultiKeyEncryptorRegistry,
            FernetFieldEncryptor,
        )

        reg = TenantAwareEncryptorRegistry()
        # Simple single-key per tenant
        reg.register("acme", FernetFieldEncryptor(acme_key))
        # Per-tenant key rotation
        acme_multi = MultiKeyEncryptorRegistry("v1", FernetFieldEncryptor(acme_v1_key))
        acme_multi.register("v2", FernetFieldEncryptor(acme_v2_key))
        acme_multi.set_primary("v2")
        reg.register("acme", acme_multi)
    """

    def __init__(self, default: FieldEncryptor | None = None) -> None:
        self._registry: dict[str, FieldEncryptor] = {}
        # default encryptor for tenants not explicitly registered
        self._default = default

    def __repr__(self) -> str:
        tenants = list(self._registry.keys())
        return (
            f"TenantAwareEncryptorRegistry("
            f"tenants={tenants}, "
            f"default={'set' if self._default else 'none'})"
        )

    def register(self, tenant_id: str, encryptor: FieldEncryptor) -> None:
        """
        Associate a ``FieldEncryptor`` with a tenant ID.

        Silently replaces any existing entry for ``tenant_id``.

        Args:
            tenant_id: The tenant identifier used as the ``context`` string.
            encryptor: Any ``FieldEncryptor`` implementation.
        """
        self._registry[tenant_id] = encryptor

    def _resolve(self, context: str | None) -> FieldEncryptor:
        """
        Look up the encryptor for ``context`` (tenant_id), falling back to default.

        Args:
            context: Tenant ID, or ``None``.

        Returns:
            The resolved ``FieldEncryptor``.

        Raises:
            EncryptionError: No encryptor found and no default is configured.
        """
        if context is not None:
            encryptor = self._registry.get(context)
            if encryptor is not None:
                return encryptor

        # Fall back to default — covers both context=None and unregistered tenants
        if self._default is not None:
            return self._default

        # No default and no match — refuse silently passing plaintext
        registered = list(self._registry.keys())
        raise EncryptionError(
            f"No encryptor registered for tenant {context!r} "
            f"and no default encryptor configured. "
            f"Registered tenants: {registered}. "
            "Call register() for this tenant or provide a default encryptor.",
            operation="encrypt" if context is None else "decrypt",
            context=context,
        )

    def encrypt(self, plaintext: bytes, *, context: str | None = None) -> bytes:
        """
        Encrypt using the encryptor registered for ``context`` (tenant_id).

        Args:
            plaintext: Bytes to encrypt.
            context:   Tenant ID.  ``None`` uses the default encryptor.

        Returns:
            Ciphertext bytes from the resolved encryptor.

        Raises:
            EncryptionError: Tenant not registered and no default encryptor.
        """
        return self._resolve(context).encrypt(plaintext, context=context)

    def decrypt(self, ciphertext: bytes, *, context: str | None = None) -> bytes:
        """
        Decrypt using the encryptor registered for ``context`` (tenant_id).

        Args:
            ciphertext: Bytes produced by ``encrypt()``.
            context:    Tenant ID.  Must match the value used during encryption.

        Returns:
            Original plaintext bytes.

        Raises:
            EncryptionError: Tenant not registered, no default, or wrong key.
        """
        return self._resolve(context).decrypt(ciphertext, context=context)


# ════════════════════════════════════════════════════════════════════════════════
# ScopedEncryptorRegistry — sibling of TenantAwareEncryptorRegistry (Phase 1, U-1)
# ════════════════════════════════════════════════════════════════════════════════


class ScopedEncryptorRegistry:
    """
    Per-**scope** encryption key isolation — sibling of
    ``TenantAwareEncryptorRegistry`` (Phase 1, U-1).

    Identical shape to ``TenantAwareEncryptorRegistry`` but keyed by the
    opaque ``scope`` dimension instead of ``tenant_id`` — a scope can be a
    tenant, but does not have to be (e.g. ``f"{tenant}:subject:{sid}"`` for
    per-data-subject keys). ``TenantAwareEncryptorRegistry`` is left
    untouched and keeps working exactly as before — this is a genuinely
    separate class, not a subclass, so a change here can never affect the
    tenant path.

    Built via ``EncryptionKeyManager.build_scoped_registry(scope)`` — callers
    should not normally construct this directly.

    Thread safety:  ⚠️ ``register`` is NOT thread-safe — populate at
                    construction (``build_scoped_registry`` does this once).
                    ``encrypt`` / ``decrypt`` are safe to call concurrently.
    Async safety:   ✅ CPU-only; safe from async.
    """

    def __init__(self, default: FieldEncryptor | None = None) -> None:
        self._registry: dict[str, FieldEncryptor] = {}
        self._default = default

    def __repr__(self) -> str:
        return (
            f"ScopedEncryptorRegistry("
            f"scopes={list(self._registry.keys())}, "
            f"default={'set' if self._default else 'none'})"
        )

    def register(self, scope: str, encryptor: FieldEncryptor) -> None:
        """Associate a ``FieldEncryptor`` with ``scope``. Replaces silently."""
        self._registry[scope] = encryptor

    def _resolve(self, context: str | None) -> FieldEncryptor:
        if context is not None:
            encryptor = self._registry.get(context)
            if encryptor is not None:
                return encryptor

        if self._default is not None:
            return self._default

        raise EncryptionError(
            f"No encryptor registered for scope {context!r} "
            f"and no default encryptor configured. "
            f"Registered scopes: {list(self._registry.keys())}. "
            "Call register() for this scope or provide a default encryptor.",
            operation="encrypt" if context is None else "decrypt",
            context=context,
        )

    def encrypt(self, plaintext: bytes, *, context: str | None = None) -> bytes:
        """Encrypt using the encryptor registered for ``context`` (scope)."""
        return self._resolve(context).encrypt(plaintext, context=context)

    def decrypt(self, ciphertext: bytes, *, context: str | None = None) -> bytes:
        """
        Decrypt using the encryptor registered for ``context`` (scope).

        Raises:
            KeyDestroyedError: The resolved encryptor's kid was destroyed —
                propagated unchanged from the underlying
                ``MultiKeyEncryptorRegistry``.
            EncryptionError: Scope not registered / no default, or the
                underlying encryptor rejected the ciphertext.
        """
        return self._resolve(context).decrypt(ciphertext, context=context)


# ════════════════════════════════════════════════════════════════════════════════
# Standalone payload utilities
# ════════════════════════════════════════════════════════════════════════════════


def encrypt_payload(
    payload: bytes,
    encryptor: FieldEncryptor,
    *,
    context: str | None = None,
) -> bytes:
    """
    Encrypt an arbitrary byte payload outside the domain model context.

    Thin wrapper over ``encryptor.encrypt`` — useful for encrypting event
    payloads, cache values, JWT claims, or any raw bytes.

    Args:
        payload:   Plaintext bytes to encrypt.
        encryptor: Any ``FieldEncryptor`` implementation.
        context:   Optional routing hint forwarded to the encryptor.

    Returns:
        Ciphertext bytes from the encryptor.

    Raises:
        EncryptionError: Encryptor failed.

    Example::

        import json
        from varco_core.encryption import encrypt_payload, FernetFieldEncryptor

        enc = FernetFieldEncryptor(key)
        token = encrypt_payload(json.dumps({"role": "admin"}).encode(), enc)
        claims = decrypt_payload(token, enc).decode()
    """
    return encryptor.encrypt(payload, context=context)


def decrypt_payload(
    ciphertext: bytes,
    encryptor: FieldEncryptor,
    *,
    context: str | None = None,
) -> bytes:
    """
    Decrypt a payload previously produced by ``encrypt_payload``.

    Args:
        ciphertext: Bytes produced by ``encrypt_payload``.
        encryptor:  The same ``FieldEncryptor`` instance (or compatible key)
                    used to encrypt.
        context:    Same value passed during encryption; forwarded to the
                    encryptor.

    Returns:
        Original plaintext bytes.

    Raises:
        EncryptionError: Wrong key, tampered data, or unknown kid/tenant.
    """
    return encryptor.decrypt(ciphertext, context=context)


# ════════════════════════════════════════════════════════════════════════════════
# JwkEncryptorBridge — convert between JsonWebKey and FieldEncryptor
# ════════════════════════════════════════════════════════════════════════════════


class JwkEncryptorBridge:
    """
    Converts between RFC 7517 ``JsonWebKey`` (``oct``, ``use="enc"``) objects
    and ``FernetFieldEncryptor`` instances.

    This bridges the interoperable JWK key format used for key storage and
    distribution with the ``FieldEncryptor`` interface used by mappers,
    key managers, and payload utilities.

    Supported key types
    -------------------
    Only ``kty="oct"`` (symmetric) keys are supported.  The ``k`` field must
    contain exactly 32 bytes (256 bits) of raw key material encoded as
    base64url (no padding) — the format produced by
    ``JwkBuilder.generate_oct_enc_key()``.

    Asymmetric keys (``RSA``, ``EC``) are *not* supported here because Fernet
    is a symmetric scheme.  For asymmetric key-wrapping (JWE), use the
    ``cryptography`` library's ``hazmat`` APIs directly.

    Wire format compatibility
    -------------------------
    JWK ``k`` field: base64url, **no padding** (RFC 7518 §6.4.1).
    Fernet key:      base64url, **with padding** (``cryptography.fernet`` expectation).

    The bridge handles this difference transparently — callers never see raw
    key bytes.

    DESIGN: classmethods over instance
      ✅ Stateless bridge — no conversion state to hold
      ✅ Mirrors JwkBuilder's classmethod style for consistency
      ❌ Cannot be injected as a Protocol — but it's not meant to be

    Thread safety:  ✅ Stateless — all methods are classmethods.
    Async safety:   ✅ CPU-only; safe to call from async context.

    Example::

        from varco_core.jwk import JwkBuilder
        from varco_core.encryption import JwkEncryptorBridge

        # Generate
        jwk = JwkBuilder.generate_oct_enc_key("tenant:acme:v1")
        enc = JwkEncryptorBridge.from_jwk(jwk)

        # Store jwk.to_dict() in DB / Redis.  On restart:
        enc = JwkEncryptorBridge.from_jwk(JsonWebKey.from_dict(stored_dict))
    """

    @classmethod
    def from_jwk(cls, jwk: JsonWebKey) -> FernetFieldEncryptor:
        """
        Build a ``FernetFieldEncryptor`` from an ``oct`` JWK.

        Args:
            jwk: A ``JsonWebKey`` with ``kty="oct"`` and a non-``None`` ``k``
                 field.  The ``k`` value must encode exactly 32 bytes.

        Returns:
            ``FernetFieldEncryptor`` backed by the key material in ``jwk.k``.

        Raises:
            ValueError:  ``jwk.kty`` is not ``"oct"``, ``jwk.k`` is ``None``,
                         or the decoded key is not 32 bytes.
            ImportError: ``cryptography`` is not installed.

        Edge cases:
            - ``jwk.use`` and ``jwk.alg`` are not validated — the bridge
              does not enforce that the key was intended for encryption.
              Callers should check ``jwk.use == "enc"`` if needed.
            - 16-byte (AES-128) keys raise ``ValueError`` — Fernet requires
              exactly 32 bytes.

        Example::

            jwk = JsonWebKey.from_dict(stored_dict)
            enc = JwkEncryptorBridge.from_jwk(jwk)
        """
        if jwk.kty != "oct":
            raise ValueError(
                f"JwkEncryptorBridge.from_jwk only supports kty='oct' (symmetric) keys, "
                f"got kty={jwk.kty!r}. "
                f"For RSA/EC key-wrapping use the cryptography JWE primitives directly."
            )
        if jwk.k is None:
            raise ValueError(
                "JwkEncryptorBridge.from_jwk: JsonWebKey has kty='oct' but k=None. "
                "Ensure the key was created with JwkBuilder.generate_oct_enc_key() "
                "or loaded from a JWKS that includes the symmetric key material."
            )

        # JWK k is base64url WITHOUT padding; add it back for urlsafe_b64decode
        raw = cls._decode_k(jwk.k)

        if len(raw) != 32:
            raise ValueError(
                f"JwkEncryptorBridge.from_jwk: Fernet requires a 32-byte key, "
                f"but the JWK k field decoded to {len(raw)} bytes. "
                f"Generate the key with JwkBuilder.generate_oct_enc_key(bits=256)."
            )

        # Fernet expects base64url WITH padding — re-encode the raw bytes
        fernet_key = base64.urlsafe_b64encode(raw)  # always 44 chars with padding
        return FernetFieldEncryptor(fernet_key)

    @classmethod
    def to_jwk(
        cls,
        raw_key_bytes: bytes,
        kid: str,
        *,
        alg: str = "A256GCM",
    ) -> JsonWebKey:
        """
        Wrap raw 32-byte symmetric key material as an ``oct`` JWK.

        Use this when you have raw key bytes (e.g. from a KMS or HSM) and need
        to produce a ``JsonWebKey`` for storage or distribution.

        Args:
            raw_key_bytes: Exactly 32 bytes of symmetric key material.
            kid:           Key ID to embed in the JWK.
            alg:           Intended algorithm.  Defaults to ``"A256GCM"``.

        Returns:
            ``JsonWebKey`` with ``kty="oct"``, ``use="enc"``, and the key
            material in ``k`` (base64url, no padding).

        Raises:
            ValueError: ``raw_key_bytes`` is not exactly 32 bytes.

        Edge cases:
            - The returned JWK includes private key material (``k``).  Never
              expose it via a public JWKS endpoint — ``JsonWebKeySet.public_set()``
              excludes ``oct`` keys automatically.

        Example::

            # Extract raw bytes from a wrapped Fernet key
            fernet_key = Fernet.generate_key()          # 44-char base64url bytes
            raw = base64.urlsafe_b64decode(fernet_key)  # 32 raw bytes
            jwk = JwkEncryptorBridge.to_jwk(raw, "my-key")
        """
        from varco_core.jwk.model import JsonWebKey as _JsonWebKey
        from varco_core.jwk.model import _b64url_encode

        if len(raw_key_bytes) != 32:
            raise ValueError(
                f"to_jwk requires exactly 32 bytes of key material, "
                f"got {len(raw_key_bytes)}. "
                f"Fernet keys decoded from base64url are always 32 bytes."
            )

        k = _b64url_encode(raw_key_bytes)
        return _JsonWebKey(kty="oct", kid=kid, use="enc", alg=alg, k=k)

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _decode_k(k: str) -> bytes:
        """
        Decode JWK ``k`` (base64url, no padding) to raw bytes.

        Args:
            k: The ``k`` string from a ``JsonWebKey``.

        Returns:
            Raw bytes.

        Edge cases:
            - Adds padding before decoding — RFC 7518 specifies no padding in
              the wire format, but ``urlsafe_b64decode`` requires it.
        """
        # Restore base64 padding: length mod 4 → number of '=' to add
        padding = 4 - len(k) % 4
        if padding != 4:
            k += "=" * padding
        return base64.urlsafe_b64decode(k)
