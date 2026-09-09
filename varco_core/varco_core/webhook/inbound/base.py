"""
varco_core.webhook.inbound.base
=================================
``WebhookVerifier`` (ABC), ``VerificationResult``, ``VerificationFailure``,
``SecretEncoding`` — the core seam inbound webhook verification is built on
(Plan 038 / S19, Step 5).

§D-S19-shape — one ABC, a shared HMAC template, four adapters
    Adopted from brief 013's *Librarian's note*: a generic ``WebhookVerifier``
    ABC with provider-specific adapters, not a monolithic multi-provider
    verifier, because Stripe/GitHub/Slack/Standard-Webhooks genuinely differ
    in header names, signed-payload construction, and whether a timestamp
    exists at all (brief 013 §2).

DESIGN: ``ABC`` + sync ``def verify``, not ``Protocol`` + ``async def``
    ✅ Verification is pure CPU (HMAC) — ``signing.py``'s own module
       docstring already states this (`:22-23`); an ``async def`` here
       would be a lie about the cost and force ``await`` on a hot path.
    ✅ Matches ``WebhookSigner`` (``signing.py:45``) and gives the shared
       ``__init__`` secret validation for free.
    ❌ A caller wanting to run verification in a thread pool anyway (e.g. to
       avoid blocking the event loop under extreme load) must do so
       explicitly — accepted; HMAC-SHA256 over a webhook-sized body is
       microseconds, not a blocking-I/O-scale cost.

DESIGN: tolerance on the constructor, not per-call
    CLAUDE.md's standing webhook rule: a knob lives on ``WebhookSettings``
    and is threaded through; a per-call argument would make the env var a
    lie about what actually governs a given verifier instance.

DESIGN: returns a ``VerificationResult``; only the HTTP adapter raises
    ✅ A pure core seam that never raises composes cleanly with any
       transport (FastAPI, a Lambda handler, a CLI replay tool, a test).
       ``varco_fastapi.webhook.inbound.verify_webhook`` maps
       ``verified=False`` to ``WebhookSignatureError`` (401) — the same
       split as ``RateLimiter.acquire() -> bool`` vs. the middleware's 429.
    ❌ Callers that want an exception-first API must wrap this themselves.
       Accepted: raising is a transport-layer decision, not a core one.

DESIGN: ``VerificationFailure`` is a closed ``StrEnum``, not a free-form
``error_code: str``
    ✅ A typed, closed set is greppable and testable; free-form strings
       drift silently across the four provider adapters.

⛔ **Rule (enforced by ``varco_core/tests/test_webhook_inbound_no_di_side_effect.py``,
Step 7): no module-level ``@Singleton``/``@Provider``/``@Configuration`` in
``varco_core.webhook.inbound`` or any of its submodules.**
``container.scan("varco_core", recursive=True)`` is a documented, in-use
pattern that auto-activates both shapes — the same rule CLAUDE.md states for
``varco_core.event.cloudevents`` and ``varco_core.tls``. Wiring is the app's,
at route level, via ``varco_fastapi.webhook.inbound.verify_webhook``.

Thread safety:  ✅ ``VerificationResult``/``VerificationFailure``/
                ``SecretEncoding`` are immutable values. ``WebhookVerifier``
                subclasses are stateless beyond their (immutable) secret
                list — safe to share across requests, same contract as
                ``WebhookSigner``.
Async safety:   ✅ ``verify()`` is pure CPU, synchronous — no ``await``
                needed, no shared mutable state.
"""

from __future__ import annotations

import abc
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

__all__ = [
    "VerificationFailure",
    "VerificationResult",
    "SecretEncoding",
    "WebhookVerifier",
]

_DEFAULT_TOLERANCE_SECONDS = 300.0


class VerificationFailure(StrEnum):
    """
    The closed set of reasons ``WebhookVerifier.verify()`` can fail.

    Members:
        MISSING_HEADER:            A required header (signature, id, or
                                    timestamp) was absent.
        MALFORMED_HEADER:          A required header was present but could
                                    not be parsed (non-numeric timestamp,
                                    empty/whitespace signature, missing
                                    ``t=``/``v1=`` component, ...).
        TIMESTAMP_OUT_OF_TOLERANCE: The signed timestamp (past *or* future)
                                    fell outside the verifier's tolerance
                                    window.
        SIGNATURE_MISMATCH:        Every candidate signature failed
                                    ``hmac.compare_digest`` against every
                                    active secret.

    Thread safety:  ✅ ``StrEnum`` members are module-level singletons.
    Async safety:   ✅ Pure value — no I/O.
    """

    MISSING_HEADER = "missing_header"
    MALFORMED_HEADER = "malformed_header"
    TIMESTAMP_OUT_OF_TOLERANCE = "timestamp_out_of_tolerance"
    SIGNATURE_MISMATCH = "signature_mismatch"


class SecretEncoding(StrEnum):
    """
    §D-S19-secretbytes — per-provider secret-to-bytes policy.

    Members:
        STANDARD_WEBHOOKS_B64: Strip a conventional ``whsec_`` prefix and
                                base64-decode, falling back to raw UTF-8
                                bytes when the decode fails — delegates to
                                ``StandardWebhooksSigner._secret_bytes``
                                (§D-S19-gap), never a second implementation
                                of the same heuristic.
        RAW_UTF8:               The secret string's raw UTF-8 bytes,
                                verbatim — what Stripe/GitHub/Slack's own
                                client libraries key their HMAC on.

    Thread safety:  ✅ ``StrEnum`` members are module-level singletons.
    Async safety:   ✅ Pure value — no I/O.
    """

    STANDARD_WEBHOOKS_B64 = "standard_webhooks_b64"
    RAW_UTF8 = "raw_utf8"


@dataclass(frozen=True)
class VerificationResult:
    """
    The outcome of ``WebhookVerifier.verify()`` — deliberately minimal.

    Attributes:
        verified:          Whether the signature (and, where applicable,
                            the timestamp) checked out.
        provider:          The verifier's ``provider`` name (e.g.
                            ``"stripe"``) — useful for logging/metrics
                            without re-deriving it from the verifier
                            instance.
        message_id:        The provider's dedup key for this delivery
                            (``webhook-id``/``svix-id``/
                            ``X-GitHub-Delivery``/...), or ``None`` when the
                            provider ships none. Reported even on a failed
                            verification when it *could* be extracted, so an
                            app can log/dedupe on it regardless of outcome.
        timestamp_checked: Whether a tolerance-window check was performed at
                            all. ``False`` **structurally** for GitHub (it
                            ships no timestamp), never as a configuration
                            choice for any other provider.
        failure:           The ``VerificationFailure`` reason, or ``None``
                            when ``verified`` is ``True``.

    ⛔ Deliberately absent (§D-S19-secret, §D-S19-rotation): which secret
    matched, the signature value, any secret material, or an index/count of
    matching secrets. Nothing here is safe to omit from a non-leak audit —
    if a field would let an attacker learn *which* of N rotated secrets is
    still valid, or recover the secret/signature itself, it does not belong
    on this dataclass.

    Thread safety:  ✅ ``frozen=True`` — immutable after construction.
    Async safety:   ✅ Pure value object — no I/O.
    """

    verified: bool
    provider: str
    message_id: str | None
    timestamp_checked: bool
    failure: VerificationFailure | None = None


class WebhookVerifier(abc.ABC):
    """
    Common contract for an inbound webhook signature verifier
    (§D-S19-shape).

    Each provider adapter defines its own header names and signed-payload
    construction (genuinely different across providers — brief 013 §2) but
    all share this ABC's constructor contract: a list of active secrets
    (§D-S19-rotation — every one is tried) and a tolerance window in
    seconds, threaded from ``WebhookSettings`` rather than taken per call
    (§D-S19-config).

    Args:
        secrets:           Active secrets to verify against, in any order —
                            unlike the outbound signer, inbound verification
                            has no "newest last" convention because nothing
                            here *signs*; every secret is tried and the
                            first match wins (§D-S19-rotation).
        tolerance_seconds:  Seconds of clock skew to tolerate around a
                            signed timestamp. Default ``300.0`` (brief 013
                            §2 — the de-facto Stripe/Svix/Slack convention).

    Raises:
        ValueError: ``secrets`` is empty.

    Thread safety:  ✅ Stateless beyond the immutable secret list — safe to
                    share across requests/threads.
    Async safety:   ✅ ``verify()`` is pure CPU, synchronous.
    """

    def __init__(
        self, secrets: list[str], *, tolerance_seconds: float = _DEFAULT_TOLERANCE_SECONDS
    ) -> None:
        if not secrets:
            raise ValueError(f"{type(self).__name__} requires at least one secret.")
        self._secrets = list(secrets)
        self._tolerance_seconds = tolerance_seconds

    @property
    @abc.abstractmethod
    def provider(self) -> str:
        """The stable provider name (e.g. ``"stripe"``) — used in logs,
        replay keys, and ``VerificationResult.provider``."""
        raise NotImplementedError

    @abc.abstractmethod
    def verify(self, *, body: bytes, headers: Mapping[str, str]) -> VerificationResult:
        """
        Verify one inbound delivery.

        Args:
            body:    The exact raw request body bytes — never a re-encoded
                     ``str`` (§D-S19-gap, brief 013 §50.2: verifying
                     anything other than the exact bytes that were signed
                     breaks the signature for a non-UTF-8 or
                     re-serialization-sensitive body).
            headers: The request's headers. Lookup is case-insensitive
                     (HTTP headers are case-insensitive by spec; brief 013
                     §2's Svix-alias handling depends on this too) —
                     implementations normalise internally, callers pass
                     headers in whatever case they arrived.

        Returns:
            A ``VerificationResult``. Never raises for an ordinary
            verification failure — a missing/malformed header, an
            out-of-tolerance timestamp, and a signature mismatch are all
            reported via ``VerificationResult.failure``, not an exception.

        Edge cases:
            - An empty or whitespace-only signature header is
              ``MALFORMED_HEADER``, never attempted as a comparison.
            - A non-numeric timestamp is ``MALFORMED_HEADER``, never an
              unhandled ``ValueError``.
            - A future timestamp beyond tolerance is rejected exactly like
              a past one (``abs(now - ts) > tolerance``).

        Async safety: ✅ Synchronous, pure CPU — safe to call from any
            context without ``await``.
        """
        raise NotImplementedError
