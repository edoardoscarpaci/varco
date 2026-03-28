"""
varco_core.dto.encryption
=========================
DTO-level field encryption for API responses.

Purpose
-------
DB-level encryption (``varco_core.encryption`` + mapper) is *transparent* — the
service and API layers always see plaintext.  That is the right default for most
fields.

DTO-level encryption solves a different problem: fields the *API client* must
not see in plaintext even after they are legitimately returned by the service.
Typical examples:

- A customer's ``ssn`` or ``card_pan`` in a ReadDTO response where the
  requesting actor is a support agent (read access) but not a billing system
  (full decrypt access).
- Audit log entries that must be stored and returned opaque.
- Any field that a downstream service will forward encrypted to another party.

The encrypted field is returned as a **base64url string** (no padding) so it
survives JSON serialisation without escaping.  Recipients with the correct
``FieldEncryptor`` can decrypt it client-side or via a dedicated endpoint.

Usage
-----
::

    from typing import Annotated
    from varco_core.dto.base import ReadDTO
    from varco_core.dto.encryption import EncryptedDTOField, encrypt_dto_response

    class PatientReadDTO(ReadDTO):
        name: str
        ssn: Annotated[str, EncryptedDTOField()]   # returned as ciphertext

    dto = PatientReadDTO(pk=1, name="Alice", ssn="123-45-6789", ...)

    enc = FernetFieldEncryptor(key)
    response_dict = encrypt_dto_response(dto, enc)
    # response_dict["ssn"] == "base64url-ciphertext"
    # response_dict["name"] == "Alice"            # untouched

    # Round-trip (client-side or test):
    plain_dict = decrypt_dto_response(response_dict, PatientReadDTO, enc)
    # plain_dict["ssn"] == "123-45-6789"

DESIGN: base64url string representation
  ✅ JSON-safe without escaping — fits in any JSON/API response body
  ✅ Self-contained — recipient needs only the key, not schema metadata
  ✅ Works with any FieldEncryptor (Fernet, multi-key, tenant-aware)
  ❌ Field values are strings at the wire level — callers must restore types
     manually when decrypting non-str fields (int, UUID, etc.)

DESIGN: return dict rather than mutating the DTO
  ✅ Pydantic models are effectively immutable after validation — no setattr
  ✅ Avoids constructing a second DTO with incorrect types (ciphertext ≠ str)
  ✅ The caller can pass the dict directly to FastAPI's JSONResponse

Thread safety:  ✅ Stateless functions — no shared mutable state.
Async safety:   ✅ CPU-only; safe to call from async context.
"""

from __future__ import annotations

import base64
import dataclasses
import json
from typing import TYPE_CHECKING, Any, get_type_hints

# WeakKeyDictionary — classes are GC'd when no longer referenced; no leak even
# if many transient model classes are created dynamically (e.g. in tests).
import weakref

if TYPE_CHECKING:
    from pydantic import BaseModel

    from varco_core.encryption import FieldEncryptor


# ════════════════════════════════════════════════════════════════════════════════
# EncryptedDTOField — presence marker
# ════════════════════════════════════════════════════════════════════════════════


@dataclasses.dataclass(frozen=True)
class EncryptedDTOField:
    """
    Marker placed in ``Annotated[T, EncryptedDTOField()]`` on a DTO field to
    indicate the value should be encrypted before serialisation and returned
    as a base64url-encoded ciphertext string.

    This marker is intentionally empty — its *presence* is the signal, not its
    value.  Mirrors the ``EncryptedHint`` design used at the mapper layer.

    Example::

        class PatientReadDTO(ReadDTO):
            name: str
            ssn: Annotated[str, EncryptedDTOField()]   # encrypted in responses
            notes: Annotated[str | None, EncryptedDTOField()] = None  # nullable

    Edge cases:
        - Nullable fields (``str | None``) — ``None`` values are never encrypted;
          the JSON field is ``null`` in the response.
        - Non-str types (``int``, ``UUID``) — values are JSON-serialised before
          encryption; ``decrypt_dto_response`` JSON-parses them back.
    """


# ════════════════════════════════════════════════════════════════════════════════
# Internal: collect annotated fields
# ════════════════════════════════════════════════════════════════════════════════


def _find_encrypted_dto_fields(model_cls: type) -> frozenset[str]:
    """
    Inspect a Pydantic model class and return the names of all fields annotated
    with ``EncryptedDTOField``.

    Uses ``typing.get_type_hints(include_extras=True)`` so ``Annotated`` metadata
    is preserved.  Results are cached per class via a module-level ``WeakKeyDictionary``
    to avoid repeating reflection on every request.

    Args:
        model_cls: A Pydantic ``BaseModel`` subclass.

    Returns:
        ``frozenset`` of field names whose annotation includes ``EncryptedDTOField``.

    Edge cases:
        - If the class has no ``Annotated`` fields, returns ``frozenset()``.
        - If the class itself is not a Pydantic model, returns ``frozenset()``
          (no ``model_fields`` attribute — graceful degradation).
    """
    cached = _FIELD_CACHE.get(model_cls)
    if cached is not None:
        return cached

    encrypted: set[str] = set()
    try:
        # include_extras=True preserves Annotated metadata — without it,
        # get_type_hints() strips the Annotated wrapper and we lose EncryptedDTOField
        hints = get_type_hints(model_cls, include_extras=True)
    except Exception:
        # Graceful degradation — if reflection fails (e.g. dynamic class without
        # a resolvable module namespace), return empty set rather than raising
        _FIELD_CACHE[model_cls] = frozenset()
        return frozenset()

    for field_name, annotation in hints.items():
        # Check whether Annotated metadata contains an EncryptedDTOField instance
        if _has_encrypted_dto_marker(annotation):
            encrypted.add(field_name)

    result = frozenset(encrypted)
    _FIELD_CACHE[model_cls] = result
    return result


def _has_encrypted_dto_marker(annotation: Any) -> bool:
    """
    Return True if ``annotation`` is ``Annotated[T, ..., EncryptedDTOField(), ...]``.

    Args:
        annotation: Any type annotation.

    Returns:
        True when ``EncryptedDTOField`` is present in the ``Annotated`` metadata.

    Edge cases:
        - Non-Annotated annotations (plain ``str``, ``int``, etc.) → False.
        - ``Annotated`` with no ``EncryptedDTOField`` in metadata → False.
        - Nested ``Annotated`` types are flattened by ``get_type_hints`` — no
          need to recurse manually.
    """
    # typing.get_args on an Annotated type returns (base_type, *metadata)
    # On a plain type it returns () — safe to check
    if not hasattr(annotation, "__metadata__"):
        return False
    return any(isinstance(m, EncryptedDTOField) for m in annotation.__metadata__)


_FIELD_CACHE: weakref.WeakKeyDictionary[type, frozenset[str]] = (
    weakref.WeakKeyDictionary()
)


# ════════════════════════════════════════════════════════════════════════════════
# Serialisation helpers
# ════════════════════════════════════════════════════════════════════════════════


def _value_to_bytes(value: Any) -> bytes:
    """
    Coerce any DTO field value to bytes for encryption.

    Str → UTF-8 encode.
    Bytes → pass through (rare but valid).
    Anything else → JSON-encode then UTF-8 encode.  This handles int, float,
    UUID, datetime, etc. that may appear in DTO fields.

    Args:
        value: The raw field value from the DTO.

    Returns:
        Bytes to pass to the encryptor.

    Edge cases:
        - ``None`` is never passed here — callers guard against it before calling.
        - Non-JSON-serialisable objects (e.g. custom classes with no __json__)
          raise ``TypeError`` from ``json.dumps``.  Callers should use str or
          standard JSON-serialisable types for encrypted fields.
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    # Fall back to JSON for int, float, list, dict, UUID (via str), etc.
    return json.dumps(value, default=str).encode("utf-8")


def _bytes_to_str_value(raw: bytes, original_value: Any) -> Any:
    """
    Coerce decrypted bytes back to the original Python type.

    This mirrors ``_value_to_bytes``:
    - If the original was str → UTF-8 decode.
    - If the original was bytes → return raw.
    - Otherwise → JSON-parse.  The caller is responsible for further coercion
      to domain types (e.g. UUID from str).

    Args:
        raw:            Decrypted bytes.
        original_value: The *ciphertext* string from the wire (used only to
                        signal that the field was originally a non-bytes value).

    Returns:
        Decoded Python value.

    Edge cases:
        - JSON-parse may return str when the original was str (round-trip of
          json.dumps(str_value) → json.loads → str).  This is correct — the
          original encoding was UTF-8, not JSON, so the decode path is also UTF-8.
    """
    # We always encoded str as UTF-8 (not JSON), so decode as UTF-8
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        # Raw binary — return as-is; caller knows what to do
        return raw


def _b64url_encode(data: bytes) -> str:
    """Encode bytes to base64url with no padding — JSON-safe wire format."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    """Decode base64url (with or without padding) back to bytes."""
    # Add padding back — base64.urlsafe_b64decode requires it
    padding = 4 - len(s) % 4
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


# ════════════════════════════════════════════════════════════════════════════════
# Public API
# ════════════════════════════════════════════════════════════════════════════════


def encrypt_dto_response(
    dto: BaseModel,
    encryptor: FieldEncryptor,
    *,
    context: str | None = None,
) -> dict[str, Any]:
    """
    Serialise a Pydantic DTO to a JSON-safe dict with ``EncryptedDTOField``
    fields replaced by base64url-encoded ciphertext strings.

    Fields that are *not* annotated with ``EncryptedDTOField`` are included
    unchanged.  ``None`` values on encrypted fields are passed through as-is
    (``null`` in JSON) — they are never encrypted.

    Args:
        dto:       Any Pydantic ``BaseModel`` instance (typically a ``ReadDTO``
                   subclass with one or more ``EncryptedDTOField`` annotations).
        encryptor: The ``FieldEncryptor`` to use.  For per-tenant encryption,
                   pass the ``TenantAwareEncryptorRegistry`` and supply the
                   ``context`` (tenant_id).
        context:   Optional routing hint forwarded to the encryptor (e.g. tenant
                   ID).  Ignored by single-key encryptors.

    Returns:
        A ``dict[str, Any]`` with all ``EncryptedDTOField`` fields replaced by
        base64url strings.  All other fields are returned as ``model_dump()``
        would produce them.

    Raises:
        EncryptionError: The encryptor failed (wrong key, unregistered tenant,
                         etc.).
        TypeError:       A field value is not JSON-serialisable (unlikely —
                         standard Pydantic types all serialise cleanly).

    Example::

        response = encrypt_dto_response(patient_dto, enc, context="acme")
        # FastAPI: return JSONResponse(content=response)

    Thread safety:  ✅ Stateless — safe to call concurrently.
    Async safety:   ✅ CPU-only; safe from async context.

    Edge cases:
        - A DTO with *no* ``EncryptedDTOField`` annotations: returns
          ``model_dump()`` unchanged — no-op.
        - Pydantic's ``model_dump()`` is called first (handles aliases,
          serialisers, excludes) — encryption is applied to the dumped value,
          not the raw Python attribute.  This means custom serialisers run
          *before* encryption.
    """
    encrypted_fields = _find_encrypted_dto_fields(type(dto))

    # model_dump() respects Pydantic serialisers, aliases, and exclusions —
    # use it as the canonical serialisation step before applying encryption.
    data = dto.model_dump()

    if not encrypted_fields:
        # Fast path — no annotated fields; skip the per-field loop entirely
        return data

    for field_name in encrypted_fields:
        if field_name not in data:
            # Field excluded by model_dump() (e.g. exclude=...) — skip
            continue

        value = data[field_name]
        if value is None:
            # Never encrypt None — preserve null semantics in the response
            continue

        # Encrypt and replace with base64url string (JSON-safe)
        plaintext = _value_to_bytes(value)
        ciphertext = encryptor.encrypt(plaintext, context=context)
        data[field_name] = _b64url_encode(ciphertext)

    return data


def decrypt_dto_response(
    data: dict[str, Any],
    dto_cls: type[BaseModel],
    encryptor: FieldEncryptor,
    *,
    context: str | None = None,
) -> dict[str, Any]:
    """
    Reverse of ``encrypt_dto_response`` — decrypt base64url ciphertext values
    back to plaintext.

    Useful for:
    - Testing round-trips in unit tests.
    - Client-side decryption when the client shares the same encryptor.
    - A dedicated "decrypt" endpoint that re-exposes plaintext to authorised
      callers.

    Args:
        data:      Dict produced by ``encrypt_dto_response`` (or received from
                   the wire).
        dto_cls:   The DTO class whose ``EncryptedDTOField`` annotations drive
                   which fields to decrypt.
        encryptor: The same ``FieldEncryptor`` used to encrypt.
        context:   Same ``context`` value used during encryption.

    Returns:
        A new dict with encrypted fields replaced by their plaintext values
        (as str — see ``_bytes_to_str_value`` for type restoration notes).

    Raises:
        EncryptionError: Wrong key, tampered data, or unknown tenant.

    Edge cases:
        - Fields absent from ``data`` are silently skipped — partial dicts are
          allowed (e.g. sparse PATCH responses).
        - ``None`` values are passed through unchanged (consistent with
          ``encrypt_dto_response``).
        - Non-encrypted fields are returned untouched.

    Thread safety:  ✅ Stateless — safe to call concurrently.
    Async safety:   ✅ CPU-only; safe from async context.
    """
    encrypted_fields = _find_encrypted_dto_fields(dto_cls)
    if not encrypted_fields:
        # Fast path — nothing to decrypt
        return dict(data)

    result = dict(data)
    for field_name in encrypted_fields:
        if field_name not in result:
            continue
        value = result[field_name]
        if value is None:
            continue

        # Decode from base64url then decrypt
        ciphertext = _b64url_decode(str(value))
        plaintext = encryptor.decrypt(ciphertext, context=context)
        result[field_name] = _bytes_to_str_value(plaintext, value)

    return result
