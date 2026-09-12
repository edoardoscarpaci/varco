"""
varco_core.exception.webhook
==============================
Exceptions raised by inbound webhook signature verification
(Plan 038 / S19, Step 14, §D-S19-errors).

Two subtypes, matching brief 013 §152's status assignment exactly:

    WebhookSignatureError → HTTP 401 (bad/missing/expired signature)
    WebhookReplayError    → HTTP 409 (a message id already seen —
                            ``varco_core.webhook.inbound.replay.WebhookReplayGuard``)

``WebhookSignatureError`` has no existing 401-mapped ``ServiceException``
parent (``exception/codes.py:163-184`` covers 403/409/422 only), so it
registers its own ``ErrorCode`` via ``register_error_code()`` at import
time — the exact precedent ``IdempotencyKeyInvalidError``
(``exception/idempotency.py:125-164``) already uses for the analogous
400 gap. ``WebhookReplayError`` subclasses ``ServiceConflictError`` and
gets 409 for free via the MRO walk — the same trick
``IdempotencyKeyConflictError`` uses.

⚠️ **Non-leak obligation (§D-S19-secret):** both exceptions' ``error_params()``
return ``{}`` — the same discipline CLAUDE.md records for
``ServiceAuthorizationError`` deliberately excluding ``reason``. Neither
constructor accepts (or stores) a secret or a signature value; only the
provider name is retained, purely for a server-side log line.
"""

from __future__ import annotations

from typing import Any

from varco_core.exception.codes import ErrorCode
from varco_core.exception.http import register_error_code
from varco_core.exception.service import ServiceConflictError, ServiceException

__all__ = ["WebhookSignatureError", "WebhookReplayError"]


class WebhookSignatureError(ServiceException):
    """
    Raised when an inbound webhook's signature (or timestamp) does not
    verify — ``VerificationResult.verified is False``.

    Maps to HTTP 401 Unauthorized via a registered ``ErrorCode`` — brief
    013 §152 assigns 401 for "you presented a credential and it did not
    verify", which is exactly this case.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Safe to raise in async contexts.
    """

    message_key = "varco.error.webhook_signature_invalid"

    def __init__(self, provider: str, *args: Any, **kwargs: Any) -> None:
        """
        Args:
            provider: The verifier's provider name (e.g. ``"stripe"``) —
                      retained only for a server-side log line, never
                      rendered into the response body's params
                      (§D-S19-secret).
            args:     Forwarded to ``Exception.__init__``.
            kwargs:   Forwarded to ``Exception.__init__``.
        """
        self.provider = provider
        super().__init__(
            f"Inbound webhook signature verification failed for provider {provider!r}.",
            *args,
            **kwargs,
        )

    def error_params(self) -> dict[str, Any]:
        # ⛔ Deliberately empty — §D-S19-secret's non-leak obligation.
        # Even `provider` is withheld from the wire response; it is safe
        # to log server-side (self.provider) but not to hand back to the
        # caller, who may be the attacker probing which provider a route
        # expects.
        return {}


class WebhookReplayError(ServiceConflictError):
    """
    Raised when ``WebhookReplayGuard.claim()`` observes a message id that
    is already in flight or already completed
    (``ReserveOutcome.IN_FLIGHT``/``REPLAY``).

    Maps to HTTP 409 Conflict (inherited from ``ServiceConflictError``) —
    brief 013 §152 assigns 409 to a replayed delivery.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Safe to raise in async contexts.
    """

    message_key = "varco.error.webhook_replay"

    def __init__(self, provider: str, *args: Any, **kwargs: Any) -> None:
        """
        Args:
            provider: The provider name the replayed delivery came from —
                      retained only for a server-side log line.
            args:     Forwarded to ``Exception.__init__``.
            kwargs:   Forwarded to ``Exception.__init__``.
        """
        self.provider = provider
        super().__init__(
            f"Inbound webhook delivery for provider {provider!r} has already been "
            "seen (in flight or already processed).",
            *args,
            **kwargs,
        )

    def error_params(self) -> dict[str, Any]:
        # ⛔ Deliberately empty — §D-S19-secret's non-leak obligation.
        return {}


# Register the 401 mapping for WebhookSignatureError at import time — before
# request handling begins (register_error_code's documented "call at
# startup only" contract, same as IdempotencyKeyInvalidError).
register_error_code(
    WebhookSignatureError,
    ErrorCode(
        code="VARCO_WEBHOOK_001",
        http_status=401,
        default_message="Inbound webhook signature verification failed.",
        message_key="varco.error.webhook_signature_invalid",
    ),
)
