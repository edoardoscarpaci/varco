"""
Plan 056 / §D-flip13-reencrypt — drift decision: under the default
``legacy_hex=None`` an entry that fails to decrypt **and** is ambiguous
(valid hex decoding to printable UTF-8) is never guessed. The batch planner
refuses, naming every ambiguous subscription id, before anything is
written; explicit ``legacy_hex=True``/``False`` keep their behaviour.

Docker-free: ``plan_legacy_reencryption`` is the shared, storage-agnostic
rule both durable repositories run before their first write.
"""

from __future__ import annotations

from typing import Any

import pytest
from varco_core.webhook.base import (
    AmbiguousLegacySecretError,
    plan_legacy_reencryption,
    reencrypt_legacy_secret,
)


def _fernet() -> Any:
    fernet = pytest.importorskip("cryptography.fernet")
    from varco_core.encryption import FernetFieldEncryptor

    return FernetFieldEncryptor(fernet.Fernet.generate_key())


def _plaintext() -> Any:
    from varco_core.encryption import PlaintextFieldEncryptor

    return PlaintextFieldEncryptor(acknowledge_plaintext=True)


def _decrypt(encryptor: Any, stored: str) -> str:
    return encryptor.decrypt(bytes.fromhex(stored)).decode("utf-8")


# ── single entry ────────────────────────────────────────────────────────────


def test_unambiguous_raw_secret_reencrypts_under_default() -> None:
    enc = _fernet()

    stored = reencrypt_legacy_secret("whsec_raw3x", enc)

    assert stored is not None and _decrypt(enc, stored) == "whsec_raw3x"


def test_ambiguous_entry_refuses_under_default() -> None:
    # hex("whsec_dev") — a PlaintextFieldEncryptor row, or a raw hex 3.x secret?
    with pytest.raises(AmbiguousLegacySecretError, match="legacy_hex=True"):
        reencrypt_legacy_secret(b"whsec_dev".hex(), _fernet())


def test_explicit_true_reads_hex() -> None:
    enc = _fernet()

    stored = reencrypt_legacy_secret(b"whsec_dev".hex(), enc, legacy_hex=True)

    assert stored is not None and _decrypt(enc, stored) == "whsec_dev"


def test_explicit_false_keeps_raw_hex_secret() -> None:
    enc = _fernet()

    stored = reencrypt_legacy_secret("41424344", enc, legacy_hex=False)

    assert stored is not None and _decrypt(enc, stored) == "41424344"


def test_already_ciphertext_is_left_alone() -> None:
    enc = _fernet()
    stored = enc.encrypt(b"whsec_x").hex()

    assert reencrypt_legacy_secret(stored, enc) is None


def test_wrong_key_fernet_token_refuses_under_default() -> None:
    # The stored hex of a Fernet token decodes to printable base64url — under
    # the old guess it was re-encrypted as the "secret". Now it refuses.
    written_by = _fernet()
    stored = written_by.encrypt(b"whsec_real").hex()

    with pytest.raises(AmbiguousLegacySecretError):
        reencrypt_legacy_secret(stored, _fernet())


# ── the batch planner both repositories call before writing ─────────────────


def test_plan_names_every_ambiguous_id_and_returns_nothing() -> None:
    enc = _fernet()
    rows = [
        ("sub-ok", ["whsec_raw"]),
        ("sub-a", [b"whsec_a".hex()]),
        ("sub-b", ["whsec_raw2", "41424344"]),
    ]

    with pytest.raises(AmbiguousLegacySecretError) as info:
        plan_legacy_reencryption(rows, enc)

    assert info.value.subscription_ids == ("sub-a", "sub-b")
    message = str(info.value)
    assert "sub-a" in message and "sub-b" in message
    assert "legacy_hex=True" in message and "legacy_hex=False" in message
    assert "Nothing was written" in message


def test_plan_unambiguous_batch_under_default() -> None:
    enc = _fernet()
    already = enc.encrypt(b"whsec_done").hex()
    rows = [("sub-1", ["whsec_one"]), ("sub-2", [already])]

    plan = plan_legacy_reencryption(rows, enc)

    assert [pk for pk, _ in plan] == ["sub-1"]
    assert _decrypt(enc, plan[0][1][0]) == "whsec_one"


@pytest.mark.parametrize(("legacy_hex", "expected"), [(True, "whsec_a"), (False, None)])
def test_plan_explicit_choice_resolves_ambiguity(legacy_hex: bool, expected: str | None) -> None:
    enc = _fernet()
    stored = b"whsec_a".hex()

    plan = plan_legacy_reencryption([("sub-a", [stored])], enc, legacy_hex=legacy_hex)

    assert _decrypt(enc, plan[0][1][0]) == (expected or stored)


def test_plaintext_encryptor_rows_need_explicit_true() -> None:
    # The Plaintext -> Fernet switch: every such row is hex of printable text.
    plain_row = _plaintext().encrypt(b"whsec_dev").hex()

    with pytest.raises(AmbiguousLegacySecretError):
        plan_legacy_reencryption([("sub-p", [plain_row])], _fernet())


def test_error_is_a_value_error() -> None:
    # Callers catching the pre-existing ValueError contract keep working.
    assert issubclass(AmbiguousLegacySecretError, ValueError)
