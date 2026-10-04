"""
Plan 056 / FLIP-13, Step 10 — ``PlaintextFieldEncryptor``: the greppable,
acknowledged dev/test hatch for the now-required webhook ``encryptor``
(red until Step 11).
"""

from __future__ import annotations

import inspect
from typing import Literal

import pytest


def _cls() -> type:
    from varco_core.encryption import PlaintextFieldEncryptor

    return PlaintextFieldEncryptor


def test_construction_without_acknowledgement_raises_type_error() -> None:
    with pytest.raises(TypeError, match="acknowledge_plaintext"):
        _cls()()


def test_acknowledge_false_raises_type_error_literally_true() -> None:
    # A bool whose only legal value is True: False must not silently construct.
    with pytest.raises(TypeError, match="literally True"):
        _cls()(acknowledge_plaintext=False)


def test_acknowledged_instance_is_a_field_encryptor() -> None:
    from varco_core.encryption import FieldEncryptor

    assert isinstance(_cls()(acknowledge_plaintext=True), FieldEncryptor)


def test_encrypt_and_decrypt_are_identity_and_ignore_context() -> None:
    enc = _cls()(acknowledge_plaintext=True)

    assert enc.encrypt(b"s") == b"s"
    assert enc.decrypt(b"s") == b"s"
    assert enc.encrypt(b"s", context="ctx") == b"s"
    assert enc.decrypt(b"s", context="ctx") == b"s"


def test_repr_shouts_plaintext() -> None:
    assert "PLAINTEXT" in repr(_cls()(acknowledge_plaintext=True))


def test_signature_guard() -> None:
    # Class __init__ is invisible to api_surface.py --check; pin it here.
    param = inspect.signature(_cls().__init__).parameters["acknowledge_plaintext"]

    assert param.kind is inspect.Parameter.KEYWORD_ONLY
    assert param.default is inspect.Parameter.empty
    assert param.annotation in (Literal[True], "Literal[True]")


def test_exported_from_varco_core_top_level() -> None:
    import varco_core

    assert varco_core.PlaintextFieldEncryptor is _cls()
