"""
Red-mode tests for Plan 035 / Phase 1, Step 1 (S3, §D-S3b).

``ErrorEnvelopeSettings.include_detail`` gates the ``msg.detail`` echo
(``str(exc)``) reachable for every ``ServiceException``. Default is ``False``
since 4.0 (Plan 054 / FLIP-10); ``VARCO_ERROR_INCLUDE_DETAIL=true`` restores
the 3.x echo and is reported by ``inspect_http_edge()`` as
``http.error.detail_exposed``.
"""

from __future__ import annotations

from varco_core.exception.http import error_message_for
from varco_core.exception.service import ServiceException
from varco_core.exception.settings import ErrorEnvelopeSettings


class _BoomException(ServiceException):
    def __init__(self) -> None:
        super().__init__("some internal detail that could leak")


def test_include_detail_defaults_to_false(monkeypatch) -> None:
    # Plan 054 / FLIP-10: application-authored str(exc) never reaches an error body by default.
    monkeypatch.delenv("VARCO_ERROR_INCLUDE_DETAIL", raising=False)
    settings = ErrorEnvelopeSettings()
    assert settings.include_detail is False


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


def test_error_message_for_enabled_detail_equals_str_exc_exactly() -> None:
    # When opted back in (Plan 054 / FLIP-10's escape hatch), the echo is
    # exactly str(exc) — the 3.x byte-for-byte shape.
    exc = _BoomException()
    msg = error_message_for(exc, envelope_settings=ErrorEnvelopeSettings(include_detail=True))
    assert msg.detail == str(exc)


# ── Plan 040 / S21, Phase 2, Step 7 — redact_params (§D-S21-errparams) ────────


def test_redact_params_defaults_to_true() -> None:
    settings = ErrorEnvelopeSettings()
    assert settings.redact_params is True


def test_redact_params_env_var_parses_false(monkeypatch) -> None:
    monkeypatch.setenv("VARCO_ERROR_REDACT_PARAMS", "false")
    settings = ErrorEnvelopeSettings()
    assert settings.redact_params is False


def test_redact_params_env_var_parses_true_explicitly(monkeypatch) -> None:
    monkeypatch.setenv("VARCO_ERROR_REDACT_PARAMS", "true")
    settings = ErrorEnvelopeSettings()
    assert settings.redact_params is True


# ── Plan 054 / FLIP-10 (§D-flip10) — default omits detail; params carry the content ──


def test_env_true_restores_detail(monkeypatch) -> None:
    # The one escape hatch restores the 3.x echo byte-for-byte.
    monkeypatch.setenv("VARCO_ERROR_INCLUDE_DETAIL", "true")
    exc = _BoomException()
    assert error_message_for(exc).detail == str(exc)


def test_default_omits_detail_but_keeps_params(monkeypatch) -> None:
    from varco_core.exception.service import ServiceValidationError

    monkeypatch.delenv("VARCO_ERROR_INCLUDE_DETAIL", raising=False)
    msg = error_message_for(ServiceValidationError("x", field="f"))

    assert msg.detail is None
    assert msg.params == {"detail": "x", "field": "f"}


def test_route_guard_denial_keeps_operation_in_params(monkeypatch) -> None:
    # RouteGuard passes its denial text as `operation`; it must survive FLIP-10 in params.
    from varco_core.exception.service import ServiceAuthorizationError

    monkeypatch.delenv("VARCO_ERROR_INCLUDE_DETAIL", raising=False)
    msg = error_message_for(ServiceAuthorizationError("Missing required role(s): 'admin'", str))

    assert msg.detail is None
    assert msg.params["operation"] == "Missing required role(s): 'admin'"
