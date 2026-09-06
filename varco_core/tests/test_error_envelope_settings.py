"""
Red-mode tests for Plan 035 / Phase 1, Step 1 (S3, §D-S3b).

``ErrorEnvelopeSettings.include_detail`` is the warn-only knob for the
``msg.detail`` echo (``str(exc)``) reachable for every ``ServiceException``.
Default is ``True`` — today's behaviour, byte-identical — and is reported by
``inspect_http_edge()`` (Plan 035 Phase 6) as a 4.0 flip candidate.
"""

from __future__ import annotations

from varco_core.exception.http import error_message_for
from varco_core.exception.service import ServiceException
from varco_core.exception.settings import ErrorEnvelopeSettings


class _BoomException(ServiceException):
    def __init__(self) -> None:
        super().__init__("some internal detail that could leak")


def test_include_detail_defaults_to_true_byte_identical() -> None:
    # D-S3b: today's behaviour (detail always present) must not move by default.
    settings = ErrorEnvelopeSettings()
    assert settings.include_detail is True


def test_include_detail_env_var_parses_false(monkeypatch) -> None:
    monkeypatch.setenv("VARCO_ERROR_INCLUDE_DETAIL", "false")
    settings = ErrorEnvelopeSettings()
    assert settings.include_detail is False


def test_include_detail_env_var_parses_true_explicitly(monkeypatch) -> None:
    monkeypatch.setenv("VARCO_ERROR_INCLUDE_DETAIL", "true")
    settings = ErrorEnvelopeSettings()
    assert settings.include_detail is True


def test_error_message_for_gates_detail_on_include_detail_false() -> None:
    exc = _BoomException()
    settings = ErrorEnvelopeSettings(include_detail=False)
    msg = error_message_for(exc, envelope_settings=settings)
    assert msg.detail is None


def test_error_message_for_default_detail_equals_str_exc_exactly() -> None:
    # Today's behaviour (http.py:324-326), unchanged by the new default.
    exc = _BoomException()
    msg = error_message_for(exc)
    assert msg.detail == str(exc)
