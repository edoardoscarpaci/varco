"""
tests.test_redaction_error_params
===================================
Plan 040 / S21, Phase 2, Steps 8-9 — ``error_params()`` made mechanically
safe against the two leak shapes CLAUDE.md warns about (§D-S21-errparams):

    (a) name-based:  a secret-named key  -> "[REDACTED]"
    (b) shape-based: a non-JSON value    -> "<TypeName>"
    (c) the documented residue: a secret VALUE under a benign key is still
        emitted verbatim — no mechanism catches it, and that limitation is
        itself a tested fact, not a hopeful sentence.

Plus the no-regression proof: every in-tree ``ServiceException`` subclass's
``error_params()`` emits byte-identically to before this plan, including
``ServiceAuthorizationError`` still excluding ``reason``.
"""

from __future__ import annotations

from typing import Any

from varco_core.exception.body_limit import RequestBodyTooLargeError
from varco_core.exception.http import error_message_for
from varco_core.exception.idempotency import (
    IdempotencyFingerprintMismatchError,
    IdempotencyKeyConflictError,
    IdempotencyKeyInvalidError,
)
from varco_core.exception.rate_limit import RateLimitExceededError
from varco_core.exception.service import (
    ServiceAuthorizationError,
    ServiceConflictError,
    ServiceException,
    ServiceNotFoundError,
    ServiceValidationError,
)
from varco_core.exception.settings import ErrorEnvelopeSettings

# ── D-S21-errparams-a: name-based redaction ────────────────────────────────


class _ApiKeyLeakingException(ServiceException):
    def __init__(self) -> None:
        super().__init__("boom")

    def error_params(self) -> dict[str, Any]:
        return {"api_key": "sk_live_x", "page": 2}


def test_secret_named_key_is_redacted_by_default() -> None:
    exc = _ApiKeyLeakingException()
    msg = error_message_for(exc)
    assert msg.params == {"api_key": "[REDACTED]", "page": 2}


def test_secret_named_key_survives_when_redact_params_false() -> None:
    exc = _ApiKeyLeakingException()
    settings = ErrorEnvelopeSettings(redact_params=False)
    msg = error_message_for(exc, envelope_settings=settings)
    assert msg.params == {"api_key": "sk_live_x", "page": 2}


# ── D-S21-errparams-b: shape-based safety ──────────────────────────────────


class _LiveObjectLeakingException(ServiceException):
    class _SessionLike:
        def __repr__(self) -> str:
            return "<AsyncSession id=0x1234>"

    def __init__(self) -> None:
        super().__init__("boom")

    def error_params(self) -> dict[str, Any]:
        return {"_session": self._SessionLike()}


def test_non_json_value_renders_as_type_name_placeholder() -> None:
    exc = _LiveObjectLeakingException()
    msg = error_message_for(exc)
    assert msg.params == {"_session": "<_SessionLike>"}


# ── D-S21-errparams-c: the documented, tested residue ──────────────────────


class _BenignKeySecretValueException(ServiceException):
    def __init__(self) -> None:
        super().__init__("boom")

    def error_params(self) -> dict[str, Any]:
        return {"internal_reason": "postgres://user:pw@host/db"}


def test_secret_value_under_benign_key_still_emitted_verbatim() -> None:
    # The residue: no mechanism catches a secret VALUE under a non-matching
    # key. This is deliberately not a bug to fix, but a fact to pin down.
    exc = _BenignKeySecretValueException()
    msg = error_message_for(exc)
    assert msg.params == {"internal_reason": "postgres://user:pw@host/db"}


# ── No-regression proof: every in-tree ServiceException, byte-identical ────


def test_service_not_found_error_params_unchanged() -> None:
    exc = ServiceNotFoundError("123", ServiceNotFoundError)
    msg = error_message_for(exc)
    assert msg.params == {"entity": "ServiceNotFoundError", "entity_id": "123"}


def test_service_authorization_error_params_unchanged_reason_still_excluded() -> None:
    exc = ServiceAuthorizationError("delete", ServiceNotFoundError, reason="internal detail")
    msg = error_message_for(exc)
    assert msg.params == {"operation": "delete", "entity": "ServiceNotFoundError"}
    assert "reason" not in msg.params


def test_service_conflict_error_params_unchanged() -> None:
    exc = ServiceConflictError("duplicate email")
    msg = error_message_for(exc)
    assert msg.params == {"detail": "duplicate email"}


def test_service_validation_error_params_unchanged() -> None:
    exc = ServiceValidationError("bad range", field="start_date")
    msg = error_message_for(exc)
    assert msg.params == {"detail": "bad range", "field": "start_date"}


def test_idempotency_key_conflict_error_params_unchanged() -> None:
    exc = IdempotencyKeyConflictError("idem-key-1")
    msg = error_message_for(exc)
    assert msg.params == {"key": "idem-key-1"}


def test_idempotency_fingerprint_mismatch_error_params_unchanged() -> None:
    exc = IdempotencyFingerprintMismatchError("idem-key-2")
    msg = error_message_for(exc)
    assert msg.params == {"key": "idem-key-2"}


def test_idempotency_key_invalid_error_params_unchanged() -> None:
    exc = IdempotencyKeyInvalidError("too long")
    msg = error_message_for(exc)
    assert msg.params == {"detail": "too long"}


def test_request_body_too_large_error_params_unchanged() -> None:
    exc = RequestBodyTooLargeError(max_bytes=1024)
    msg = error_message_for(exc)
    assert msg.params == {"max_bytes": 1024}


def test_rate_limit_exceeded_error_params_unchanged() -> None:
    exc = RateLimitExceededError(rule_name="global", retry_after_seconds=30)
    msg = error_message_for(exc)
    assert msg.params == {"rule_name": "global", "retry_after_seconds": 30}
