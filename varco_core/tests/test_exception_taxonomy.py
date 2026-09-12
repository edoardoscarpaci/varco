"""
Red-mode tests for Plan 035 / Phase 4, Step 14 (S8) — ``RequestBodyTooLargeError``.

Lives in ``varco_core`` because the taxonomy does (the middleware that
raises it, ``BodyLimitMiddleware``, lives in ``varco_fastapi``). Must map to
HTTP 413 through ``error_code_for``/``error_message_for`` and carry a
``message_key`` — ``code`` is the machine id, ``message_key`` is the i18n
key (technical_docs/features/error-taxonomy-and-i18n.md).

``RequestBodyTooLargeError`` does not exist yet — every test below must fail
with ``ImportError``, not a fixture typo.
"""

from __future__ import annotations

from varco_core.exception import RequestBodyTooLargeError  # type: ignore[attr-defined]
from varco_core.exception.http import error_code_for, error_message_for
from varco_core.exception.service import ServiceException


def test_request_body_too_large_error_is_a_service_exception() -> None:
    exc = RequestBodyTooLargeError(max_bytes=10 * 1024 * 1024)
    assert isinstance(exc, ServiceException)


def test_request_body_too_large_error_maps_to_http_413_via_error_code_for() -> None:
    exc = RequestBodyTooLargeError(max_bytes=10 * 1024 * 1024)
    code = error_code_for(exc)
    assert code.http_status == 413


def test_request_body_too_large_error_maps_to_http_413_via_error_message_for() -> None:
    exc = RequestBodyTooLargeError(max_bytes=10 * 1024 * 1024)
    msg = error_message_for(exc)
    assert msg.http_status == 413


def test_request_body_too_large_error_has_a_message_key() -> None:
    # code is the machine id, message_key is the i18n key — never conflated.
    assert RequestBodyTooLargeError.message_key is not None
    assert isinstance(RequestBodyTooLargeError.message_key, str)


def test_request_body_too_large_error_message_key_differs_from_code() -> None:
    exc = RequestBodyTooLargeError(max_bytes=10 * 1024 * 1024)
    code = error_code_for(exc)
    assert RequestBodyTooLargeError.message_key != code.code
