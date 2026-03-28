"""
Unit tests for varco_core.dto.encryption
==========================================
Covers ``EncryptedDTOField``, ``encrypt_dto_response``, and
``decrypt_dto_response``.

No external dependencies beyond cryptography (Fernet).  All tests use a
single in-process ``FernetFieldEncryptor``.

Sections
--------
- ``EncryptedDTOField``      — marker identity, frozen, equality
- ``_find_encrypted_fields`` — annotation inspection, caching, edge cases
- ``encrypt_dto_response``   — field replacement, passthrough, None handling
- ``decrypt_dto_response``   — round-trip, None, missing fields
- Context passing            — context forwarded to encryptor
"""

from __future__ import annotations

from typing import Annotated
from unittest.mock import MagicMock

import pytest
from cryptography.fernet import Fernet
from pydantic import BaseModel

from varco_core.dto.encryption import (
    EncryptedDTOField,
    _find_encrypted_dto_fields,
    decrypt_dto_response,
    encrypt_dto_response,
)
from varco_core.encryption import FernetFieldEncryptor


# ── Fixtures ───────────────────────────────────────────────────────────────────


def _enc() -> FernetFieldEncryptor:
    return FernetFieldEncryptor(Fernet.generate_key())


# ── Test models ────────────────────────────────────────────────────────────────


class SimpleDTO(BaseModel):
    name: str
    ssn: Annotated[str, EncryptedDTOField()]


class NullableDTO(BaseModel):
    name: str
    notes: Annotated[str | None, EncryptedDTOField()] = None


class NoEncryptedFieldsDTO(BaseModel):
    name: str
    age: int


class MultipleEncryptedDTO(BaseModel):
    name: str
    ssn: Annotated[str, EncryptedDTOField()]
    card_pan: Annotated[str, EncryptedDTOField()]
    email: str  # not encrypted


class IntFieldDTO(BaseModel):
    label: str
    secret_score: Annotated[int, EncryptedDTOField()]


# ════════════════════════════════════════════════════════════════════════════════
# EncryptedDTOField
# ════════════════════════════════════════════════════════════════════════════════


class TestEncryptedDTOField:
    def test_instances_are_equal(self) -> None:
        """Two EncryptedDTOField() instances are equal (frozen dataclass)."""
        assert EncryptedDTOField() == EncryptedDTOField()

    def test_frozen(self) -> None:
        m = EncryptedDTOField()
        with pytest.raises((AttributeError, TypeError)):
            m.x = 1  # type: ignore[attr-defined]

    def test_is_hashable(self) -> None:
        """Frozen dataclasses must be hashable — usable in sets/dict keys."""
        s = {EncryptedDTOField(), EncryptedDTOField()}
        assert len(s) == 1


# ════════════════════════════════════════════════════════════════════════════════
# _find_encrypted_dto_fields
# ════════════════════════════════════════════════════════════════════════════════


class TestFindEncryptedDtoFields:
    def test_simple_dto_detects_ssn(self) -> None:
        result = _find_encrypted_dto_fields(SimpleDTO)
        assert "ssn" in result
        assert "name" not in result

    def test_nullable_dto_detects_notes(self) -> None:
        result = _find_encrypted_dto_fields(NullableDTO)
        assert "notes" in result
        assert "name" not in result

    def test_no_encrypted_fields_returns_empty(self) -> None:
        result = _find_encrypted_dto_fields(NoEncryptedFieldsDTO)
        assert result == frozenset()

    def test_multiple_encrypted_fields(self) -> None:
        result = _find_encrypted_dto_fields(MultipleEncryptedDTO)
        assert result == frozenset({"ssn", "card_pan"})
        assert "email" not in result
        assert "name" not in result

    def test_caching_returns_same_frozenset(self) -> None:
        """Second call should hit the cache and return the identical object."""
        result1 = _find_encrypted_dto_fields(SimpleDTO)
        result2 = _find_encrypted_dto_fields(SimpleDTO)
        assert result1 is result2  # same object from WeakKeyDictionary cache


# ════════════════════════════════════════════════════════════════════════════════
# encrypt_dto_response
# ════════════════════════════════════════════════════════════════════════════════


class TestEncryptDtoResponse:
    def test_encrypted_field_replaced_with_string(self) -> None:
        enc = _enc()
        dto = SimpleDTO(name="Alice", ssn="123-45-6789")
        result = encrypt_dto_response(dto, enc)
        # ssn must be a non-empty string (base64url ciphertext)
        assert isinstance(result["ssn"], str)
        assert len(result["ssn"]) > 0

    def test_non_encrypted_field_unchanged(self) -> None:
        enc = _enc()
        dto = SimpleDTO(name="Alice", ssn="123-45-6789")
        result = encrypt_dto_response(dto, enc)
        assert result["name"] == "Alice"

    def test_no_encrypted_fields_returns_model_dump(self) -> None:
        enc = _enc()
        dto = NoEncryptedFieldsDTO(name="Bob", age=42)
        result = encrypt_dto_response(dto, enc)
        assert result == {"name": "Bob", "age": 42}

    def test_none_value_not_encrypted(self) -> None:
        enc = _enc()
        dto = NullableDTO(name="Carol", notes=None)
        result = encrypt_dto_response(dto, enc)
        assert result["notes"] is None

    def test_non_none_nullable_value_encrypted(self) -> None:
        enc = _enc()
        dto = NullableDTO(name="Dana", notes="some notes")
        result = encrypt_dto_response(dto, enc)
        assert isinstance(result["notes"], str)
        assert result["notes"] != "some notes"

    def test_multiple_fields_all_encrypted(self) -> None:
        enc = _enc()
        dto = MultipleEncryptedDTO(
            name="Eve", ssn="SSN", card_pan="4111111111111111", email="e@x.com"
        )
        result = encrypt_dto_response(dto, enc)
        # ssn and card_pan replaced; name and email unchanged
        assert result["ssn"] != "SSN"
        assert result["card_pan"] != "4111111111111111"
        assert result["name"] == "Eve"
        assert result["email"] == "e@x.com"

    def test_int_field_encrypted_as_string(self) -> None:
        enc = _enc()
        dto = IntFieldDTO(label="user", secret_score=99)
        result = encrypt_dto_response(dto, enc)
        # encrypted value is always a string
        assert isinstance(result["secret_score"], str)
        assert result["label"] == "user"

    def test_ciphertext_is_base64url_no_padding(self) -> None:
        enc = _enc()
        dto = SimpleDTO(name="Alice", ssn="123-45-6789")
        result = encrypt_dto_response(dto, enc)
        ct = result["ssn"]
        assert isinstance(ct, str)
        # base64url uses only A-Z a-z 0-9 - _ and no =
        assert "=" not in ct

    def test_context_forwarded_to_encryptor(self) -> None:
        """``context`` kwarg must be passed through to encryptor.encrypt()."""
        mock_enc = MagicMock()
        mock_enc.encrypt.return_value = b"fake-ct"
        dto = SimpleDTO(name="Alice", ssn="123")

        encrypt_dto_response(dto, mock_enc, context="acme")  # type: ignore[arg-type]
        # Every encrypt call must have been called with context="acme"
        for call in mock_enc.encrypt.call_args_list:
            assert call.kwargs.get("context") == "acme"

    def test_two_calls_produce_different_ciphertext(self) -> None:
        """Fernet uses a random IV — same plaintext gives different ciphertext."""
        enc = _enc()
        dto = SimpleDTO(name="Alice", ssn="123-45-6789")
        ct1 = encrypt_dto_response(dto, enc)["ssn"]
        ct2 = encrypt_dto_response(dto, enc)["ssn"]
        assert ct1 != ct2  # different IVs → different tokens


# ════════════════════════════════════════════════════════════════════════════════
# decrypt_dto_response
# ════════════════════════════════════════════════════════════════════════════════


class TestDecryptDtoResponse:
    def test_round_trip_str_field(self) -> None:
        enc = _enc()
        dto = SimpleDTO(name="Alice", ssn="123-45-6789")
        encrypted = encrypt_dto_response(dto, enc)
        decrypted = decrypt_dto_response(encrypted, SimpleDTO, enc)
        assert decrypted["ssn"] == "123-45-6789"
        assert decrypted["name"] == "Alice"

    def test_round_trip_nullable_field(self) -> None:
        enc = _enc()
        dto = NullableDTO(name="Bob", notes="my notes")
        encrypted = encrypt_dto_response(dto, enc)
        decrypted = decrypt_dto_response(encrypted, NullableDTO, enc)
        assert decrypted["notes"] == "my notes"

    def test_none_value_passes_through(self) -> None:
        enc = _enc()
        dto = NullableDTO(name="Carol", notes=None)
        encrypted = encrypt_dto_response(dto, enc)
        decrypted = decrypt_dto_response(encrypted, NullableDTO, enc)
        assert decrypted["notes"] is None

    def test_no_encrypted_fields_returns_copy(self) -> None:
        enc = _enc()
        data = {"name": "Dave", "age": 30}
        result = decrypt_dto_response(data, NoEncryptedFieldsDTO, enc)
        assert result == data
        # Must be a new dict, not the same object
        assert result is not data

    def test_multiple_fields_round_trip(self) -> None:
        enc = _enc()
        dto = MultipleEncryptedDTO(
            name="Eve", ssn="SSN", card_pan="4111111111111111", email="e@x.com"
        )
        encrypted = encrypt_dto_response(dto, enc)
        decrypted = decrypt_dto_response(encrypted, MultipleEncryptedDTO, enc)
        assert decrypted["ssn"] == "SSN"
        assert decrypted["card_pan"] == "4111111111111111"
        assert decrypted["name"] == "Eve"
        assert decrypted["email"] == "e@x.com"

    def test_int_field_round_trip(self) -> None:
        enc = _enc()
        dto = IntFieldDTO(label="user", secret_score=99)
        encrypted = encrypt_dto_response(dto, enc)
        decrypted = decrypt_dto_response(encrypted, IntFieldDTO, enc)
        # Value comes back as a string (see _bytes_to_str_value docstring)
        # The original value 99 was encoded as "99" bytes via JSON
        assert str(decrypted["secret_score"]) == "99"

    def test_partial_dict_skips_missing_fields(self) -> None:
        """Missing encrypted fields in ``data`` are silently skipped."""
        enc = _enc()
        # Only provide 'name', not 'ssn'
        partial = {"name": "Frank"}
        result = decrypt_dto_response(partial, SimpleDTO, enc)
        assert result == {"name": "Frank"}

    def test_wrong_key_raises_encryption_error(self) -> None:
        from varco_core.encryption import EncryptionError

        enc1 = _enc()
        enc2 = _enc()
        dto = SimpleDTO(name="Grace", ssn="secret")
        encrypted = encrypt_dto_response(dto, enc1)
        with pytest.raises(EncryptionError):
            decrypt_dto_response(encrypted, SimpleDTO, enc2)

    def test_context_forwarded_to_encryptor(self) -> None:
        mock_enc = MagicMock()
        mock_enc.decrypt.return_value = b"plaintext"
        data = {"ssn": "dGVzdA"}  # fake base64url
        decrypt_dto_response(data, SimpleDTO, mock_enc, context="acme")  # type: ignore[arg-type]
        for call in mock_enc.decrypt.call_args_list:
            assert call.kwargs.get("context") == "acme"
