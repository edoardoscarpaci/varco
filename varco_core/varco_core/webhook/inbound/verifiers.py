"""
varco_core.webhook.inbound.verifiers
======================================
``HmacWebhookVerifier`` (the shared HMAC template for Stripe/GitHub/Slack)
plus the four concrete adapters — Standard Webhooks/Svix, Stripe, GitHub,
Slack — and ``get_verifier()``, mirroring ``signing.get_signer``
(Plan 038 / S19, Steps 6, 11).

§D-S19-gap — the Standard Webhooks/Svix path delegates
    ``StandardWebhooksVerifier`` holds a ``StandardWebhooksSigner`` built
    from the same secrets/tolerance and delegates the signature *decision*
    to it, so inbound and outbound cannot drift for the one scheme varco
    already ships a correct HMAC implementation for (Step 4's round-trip
    test is the anti-drift proof). Stripe/GitHub/Slack have no shipped
    implementation to reuse — brief 013 §2 shows three genuinely different
    signed-basestring constructions — so ``HmacWebhookVerifier`` gives them
    a shared template (case-insensitive header lookup, a tolerance-window
    helper, a constant-time any-secret-matches helper) rather than three
    fully independent implementations.

§D-S19-secretbytes — per-provider secret encoding
    Standard Webhooks/Svix secrets are conventionally ``whsec_`` + base64
    (delegated to ``StandardWebhooksSigner._secret_bytes``). Stripe/GitHub/
    Slack key their HMAC on the secret string's raw UTF-8 bytes —
    ``HmacWebhookVerifier.secret_encoding`` is fixed to
    ``SecretEncoding.RAW_UTF8`` for all three; see the module docstring in
    ``plans/038-inbound-webhook-verification.md`` for the ``⚠️ ASSUMPTION``
    this pins for Stripe (no known-answer vector was available to settle
    it against a real delivery).

Thread safety:  ✅ Every verifier is stateless beyond its immutable secret
                list — safe to share across requests.
Async safety:   ✅ Pure CPU, synchronous — no ``await`` anywhere in this
                module.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING

from varco_core.webhook.inbound.base import (
    SecretEncoding,
    VerificationFailure,
    VerificationResult,
    WebhookVerifier,
)
from varco_core.webhook.signing import StandardWebhooksSigner

if TYPE_CHECKING:
    from varco_core.webhook.settings import WebhookSettings

__all__ = [
    "HmacWebhookVerifier",
    "StandardWebhooksVerifier",
    "SvixWebhookVerifier",
    "StripeWebhookVerifier",
    "GitHubWebhookVerifier",
    "SlackWebhookVerifier",
    "get_verifier",
]

_DEFAULT_TOLERANCE_SECONDS = 300.0


def _normalize_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Lower-case every header name once at entry (HTTP headers are
    case-insensitive by spec; brief 013 §2's Svix-alias handling and the
    plan's Edge cases section both depend on this)."""
    return {k.lower(): v for k, v in headers.items()}


def _first_present(normalized: dict[str, str], names: tuple[str, ...]) -> str | None:
    """Return the first header value present among ``names`` (alias
    resolution — e.g. Standard Webhooks' ``webhook-id`` vs. Svix's
    ``svix-id``), or ``None`` if none are present."""
    for name in names:
        if name in normalized:
            return normalized[name]
    return None


class HmacWebhookVerifier(WebhookVerifier):
    """
    Shared HMAC-SHA256 template for the three providers that key on the
    secret's raw UTF-8 bytes rather than delegating to
    ``StandardWebhooksSigner`` (§D-S19-secretbytes).

    Subclasses implement ``verify()`` themselves (the header names and
    signed-content construction differ too much to template — brief 013
    §2) but share ``_secret_bytes``/``_hexdigest``/``_match_any``/
    ``_check_tolerance`` so the constant-time-compare and
    tolerance-window logic is written exactly once.

    Thread safety:  ✅ Stateless beyond the immutable secret list.
    Async safety:   ✅ Pure CPU.
    """

    secret_encoding: SecretEncoding = SecretEncoding.RAW_UTF8

    def _secret_bytes(self, secret: str) -> bytes:
        # All three HmacWebhookVerifier subclasses use RAW_UTF8
        # (§D-S19-secretbytes) — no branch on self.secret_encoding needed
        # today, but the attribute stays so a future in-tree provider that
        # *does* need STANDARD_WEBHOOKS_B64 can flip it without touching
        # this method's signature.
        return secret.encode("utf-8")

    def _hexdigest(self, secret: str, signed_content: bytes) -> str:
        return hmac.new(self._secret_bytes(secret), signed_content, hashlib.sha256).hexdigest()

    def _match_any(self, provided: list[str], signed_content: bytes) -> bool:
        """
        Try every provided candidate against every active secret with
        ``hmac.compare_digest`` (§D-S19-rotation — identical shape to the
        shipped ``StandardWebhooksSigner.verify()`` loop, early-returning
        on the first match so nothing observable reveals which secret/
        candidate matched).
        """
        candidates = [self._hexdigest(secret, signed_content) for secret in self._secrets]
        for value in provided:
            for candidate in candidates:
                if hmac.compare_digest(value, candidate):
                    return True
        return False

    def _check_tolerance(self, timestamp: str) -> VerificationFailure | None:
        """
        Parse and bound-check a timestamp string.

        Returns:
            ``None`` if the timestamp parses and is within tolerance;
            ``VerificationFailure.MALFORMED_HEADER`` for a non-numeric
            value; ``VerificationFailure.TIMESTAMP_OUT_OF_TOLERANCE`` for a
            past *or* future timestamp beyond ``self._tolerance_seconds``.
        """
        try:
            ts = float(timestamp)
        except ValueError:
            return VerificationFailure.MALFORMED_HEADER
        if abs(time.time() - ts) > self._tolerance_seconds:
            return VerificationFailure.TIMESTAMP_OUT_OF_TOLERANCE
        return None


class StandardWebhooksVerifier(WebhookVerifier):
    """
    Standard Webhooks / Svix inbound verifier (§D-S19-gap).

    Delegates the signature *decision* to a held ``StandardWebhooksSigner``
    built from the same secrets/tolerance — the anti-drift guarantee: a
    delivery produced by ``StandardWebhooksSigner.sign()`` verifies through
    this class (proved by the round-trip test, Step 4). Performs its own
    header presence/parse/tolerance checks *first* so a caller's server log
    can distinguish ``TIMESTAMP_OUT_OF_TOLERANCE`` from
    ``SIGNATURE_MISMATCH`` — the shipped signer only returns ``bool``. The
    double timestamp check (here, then again inside the delegated
    ``verify()``) is deliberate and costs one ``float()`` + one ``abs()``.

    Accepts **both** ``webhook-*`` and ``svix-*`` header names regardless
    of which concrete class is used (brief 013 §2: Svix libraries accept
    either set) — ``SvixWebhookVerifier`` exists only so ``.provider``
    reads ``"svix"`` in logs and replay keys (Open question 1, resolved
    "yes, both accept both").

    Thread safety:  ✅ Stateless beyond the immutable secret list.
    Async safety:   ✅ Pure CPU.
    """

    _ID_HEADERS = ("webhook-id", "svix-id")
    _TIMESTAMP_HEADERS = ("webhook-timestamp", "svix-timestamp")
    _SIGNATURE_HEADERS = ("webhook-signature", "svix-signature")

    def __init__(
        self, secrets: list[str], *, tolerance_seconds: float = _DEFAULT_TOLERANCE_SECONDS
    ) -> None:
        super().__init__(secrets, tolerance_seconds=tolerance_seconds)
        self._signer = StandardWebhooksSigner(secrets, tolerance_seconds=tolerance_seconds)

    @property
    def provider(self) -> str:
        return "standard_webhooks"

    def verify(self, *, body: bytes, headers: Mapping[str, str]) -> VerificationResult:
        normalized = _normalize_headers(headers)
        msg_id = _first_present(normalized, self._ID_HEADERS)
        timestamp = _first_present(normalized, self._TIMESTAMP_HEADERS)
        signature_header = _first_present(normalized, self._SIGNATURE_HEADERS)

        if msg_id is None or timestamp is None or signature_header is None:
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=msg_id,
                timestamp_checked=False,
                failure=VerificationFailure.MISSING_HEADER,
            )

        try:
            ts = float(timestamp)
        except ValueError:
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=msg_id,
                timestamp_checked=False,
                failure=VerificationFailure.MALFORMED_HEADER,
            )

        if not signature_header.strip():
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=msg_id,
                timestamp_checked=False,
                failure=VerificationFailure.MALFORMED_HEADER,
            )

        if abs(time.time() - ts) > self._tolerance_seconds:
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=msg_id,
                timestamp_checked=True,
                failure=VerificationFailure.TIMESTAMP_OUT_OF_TOLERANCE,
            )

        verified = self._signer.verify(
            payload=body,
            headers={
                "webhook-id": msg_id,
                "webhook-timestamp": timestamp,
                "webhook-signature": signature_header,
            },
        )
        if not verified:
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=msg_id,
                timestamp_checked=True,
                failure=VerificationFailure.SIGNATURE_MISMATCH,
            )
        return VerificationResult(
            verified=True,
            provider=self.provider,
            message_id=msg_id,
            timestamp_checked=True,
            failure=None,
        )


class SvixWebhookVerifier(StandardWebhooksVerifier):
    """
    Identical to ``StandardWebhooksVerifier`` except ``.provider`` reads
    ``"svix"`` — exists purely so logs/replay keys/metrics can distinguish
    a Svix-labelled integration from a generic Standard Webhooks one, per
    Open question 1's resolution (both classes accept both header sets).
    """

    @property
    def provider(self) -> str:
        return "svix"


class StripeWebhookVerifier(HmacWebhookVerifier):
    """
    Stripe inbound verifier (brief 013 §2).

    Header: ``Stripe-Signature: t=<unix ts>,v1=<hex>[,v1=<hex>...]``
    (comma/space-tolerant; multiple ``v1=`` entries support secret
    rotation, §D-S19-rotation). A ``v0=``-only header (Stripe's legacy
    scheme) is rejected — only ``v1`` is accepted, per the brief.

    Signed content: ``f"{timestamp}."`` concatenated with the **raw body
    bytes** (never a ``str`` decode — §D-S19-gap's raw-body seam).

    ⚠️ ``⚠️ ASSUMPTION`` (§D-S19-secretbytes, carried to the plan's Risks):
    the secret is keyed as raw UTF-8 bytes, matching stripe-python's
    behaviour rather than the brief's "base64 secret" description. No
    known-answer vector was available to settle this against a real
    delivery; see the plan's Risks table.

    Thread safety:  ✅ Stateless beyond the immutable secret list.
    Async safety:   ✅ Pure CPU.
    """

    @property
    def provider(self) -> str:
        return "stripe"

    def verify(self, *, body: bytes, headers: Mapping[str, str]) -> VerificationResult:
        normalized = _normalize_headers(headers)
        header_value = normalized.get("stripe-signature")
        if header_value is None:
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=None,
                timestamp_checked=False,
                failure=VerificationFailure.MISSING_HEADER,
            )

        timestamp: str | None = None
        v1_signatures: list[str] = []
        for part in header_value.split(","):
            part = part.strip()
            if not part or "=" not in part:
                continue
            key, _, value = part.partition("=")
            key = key.strip()
            value = value.strip()
            if key == "t":
                timestamp = value
            elif key == "v1":
                v1_signatures.append(value)

        if timestamp is None or not v1_signatures:
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=None,
                timestamp_checked=False,
                failure=VerificationFailure.MALFORMED_HEADER,
            )

        failure = self._check_tolerance(timestamp)
        if failure is not None:
            timestamp_checked = failure is VerificationFailure.TIMESTAMP_OUT_OF_TOLERANCE
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=None,
                timestamp_checked=timestamp_checked,
                failure=failure,
            )

        signed_content = f"{timestamp}.".encode() + body
        if not self._match_any(v1_signatures, signed_content):
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=None,
                timestamp_checked=True,
                failure=VerificationFailure.SIGNATURE_MISMATCH,
            )
        return VerificationResult(
            verified=True,
            provider=self.provider,
            message_id=None,
            timestamp_checked=True,
            failure=None,
        )


class GitHubWebhookVerifier(HmacWebhookVerifier):
    """
    GitHub inbound verifier (brief 013 §2, §D-S19-github).

    Header: ``X-Hub-Signature-256: sha256=<hex>`` over the **raw body
    only** — GitHub ships no timestamp, so ``timestamp_checked`` is
    ``False`` structurally, never by configuration.
    ``X-GitHub-Delivery`` is the only dedup handle (surfaced as
    ``VerificationResult.message_id``).

    §D-S19-github: because the tolerance window is structurally
    unavailable for this provider, construction **refuses** to proceed
    without either ``replay_guard=`` or
    ``acknowledge_no_replay_protection=True`` — the one provider where a
    replay-unprotected receiver cannot be wired up by accident.

    Args:
        secrets:            Active secrets, any order (§D-S19-rotation).
        tolerance_seconds:   Accepted for constructor-shape symmetry with
                             every other verifier, but unused —
                             GitHub has no timestamp to bound.
        replay_guard:        A ``WebhookReplayGuard`` the caller intends to
                             use alongside this verifier. Not held or
                             called by this class itself (the FastAPI
                             dependency wires it) — its mere presence here
                             is what satisfies the construction-time check.
        acknowledge_no_replay_protection: Explicit escape hatch when no
                             replay guard is available. Default ``False``.

    Raises:
        ValueError: Neither ``replay_guard`` nor
            ``acknowledge_no_replay_protection=True`` was supplied.

    Thread safety:  ✅ Stateless beyond the immutable secret list.
    Async safety:   ✅ Pure CPU.
    """

    def __init__(
        self,
        secrets: list[str],
        *,
        tolerance_seconds: float = _DEFAULT_TOLERANCE_SECONDS,
        replay_guard: object | None = None,
        acknowledge_no_replay_protection: bool = False,
    ) -> None:
        if replay_guard is None and not acknowledge_no_replay_protection:
            raise ValueError(
                "GitHubWebhookVerifier ships no signed timestamp, so the "
                "usual tolerance window cannot bound a replay. Pass "
                "replay_guard=<WebhookReplayGuard> to dedupe deliveries, or "
                "acknowledge_no_replay_protection=True to construct anyway "
                "and accept the risk (§D-S19-github)."
            )
        super().__init__(secrets, tolerance_seconds=tolerance_seconds)
        self._replay_guard = replay_guard
        self._acknowledge_no_replay_protection = acknowledge_no_replay_protection

    @property
    def provider(self) -> str:
        return "github"

    def verify(self, *, body: bytes, headers: Mapping[str, str]) -> VerificationResult:
        normalized = _normalize_headers(headers)
        message_id = normalized.get("x-github-delivery")
        header_value = normalized.get("x-hub-signature-256")

        if header_value is None:
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=message_id,
                timestamp_checked=False,
                failure=VerificationFailure.MISSING_HEADER,
            )

        prefix = "sha256="
        if not header_value.startswith(prefix) or not header_value[len(prefix) :].strip():
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=message_id,
                timestamp_checked=False,
                failure=VerificationFailure.MALFORMED_HEADER,
            )
        provided = header_value[len(prefix) :]

        if not self._match_any([provided], body):
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=message_id,
                timestamp_checked=False,
                failure=VerificationFailure.SIGNATURE_MISMATCH,
            )
        return VerificationResult(
            verified=True,
            provider=self.provider,
            message_id=message_id,
            timestamp_checked=False,
            failure=None,
        )


class SlackWebhookVerifier(HmacWebhookVerifier):
    """
    Slack inbound verifier (brief 013 §2).

    Headers: ``X-Slack-Signature: v0=<hex>`` +
    ``X-Slack-Request-Timestamp: <unix ts>`` — both required. Signed
    content: ``f"v0:{timestamp}:"`` concatenated with the raw body bytes
    (the timestamp is a distinct signed field, not concatenated the way
    Standard Webhooks does it — tampering the body alone still fails
    verification even though the timestamp itself is untouched).

    Thread safety:  ✅ Stateless beyond the immutable secret list.
    Async safety:   ✅ Pure CPU.
    """

    @property
    def provider(self) -> str:
        return "slack"

    def verify(self, *, body: bytes, headers: Mapping[str, str]) -> VerificationResult:
        normalized = _normalize_headers(headers)
        signature = normalized.get("x-slack-signature")
        timestamp = normalized.get("x-slack-request-timestamp")

        if signature is None or timestamp is None:
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=None,
                timestamp_checked=False,
                failure=VerificationFailure.MISSING_HEADER,
            )

        prefix = "v0="
        if not signature.startswith(prefix) or not signature[len(prefix) :].strip():
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=None,
                timestamp_checked=False,
                failure=VerificationFailure.MALFORMED_HEADER,
            )
        provided = signature[len(prefix) :]

        failure = self._check_tolerance(timestamp)
        if failure is not None:
            timestamp_checked = failure is VerificationFailure.TIMESTAMP_OUT_OF_TOLERANCE
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=None,
                timestamp_checked=timestamp_checked,
                failure=failure,
            )

        basestring = f"v0:{timestamp}:".encode() + body
        if not self._match_any([provided], basestring):
            return VerificationResult(
                verified=False,
                provider=self.provider,
                message_id=None,
                timestamp_checked=True,
                failure=VerificationFailure.SIGNATURE_MISMATCH,
            )
        return VerificationResult(
            verified=True,
            provider=self.provider,
            message_id=None,
            timestamp_checked=True,
            failure=None,
        )


_PROVIDERS: dict[str, type[WebhookVerifier]] = {
    "standard_webhooks": StandardWebhooksVerifier,
    "svix": SvixWebhookVerifier,
    "stripe": StripeWebhookVerifier,
    "github": GitHubWebhookVerifier,
    "slack": SlackWebhookVerifier,
}


def get_verifier(
    provider: str,
    *,
    secrets: list[str],
    settings: WebhookSettings | None = None,
    **kwargs: object,
) -> WebhookVerifier:
    """
    Construct a ``WebhookVerifier`` by provider name — mirrors
    ``signing.get_signer`` (§D-S19-config).

    Args:
        provider: One of ``"standard_webhooks"``, ``"svix"``, ``"stripe"``,
                   ``"github"``, ``"slack"``.
        secrets:   Active secrets, any order (§D-S19-rotation).
        settings:  ``WebhookSettings`` to source ``inbound_tolerance_seconds``
                   from when ``tolerance_seconds`` is not explicitly passed
                   in ``**kwargs``. ``None`` (the default) constructs
                   ``WebhookSettings()`` from the environment.
        **kwargs:  Forwarded to the provider class's constructor (e.g.
                   ``replay_guard=``/``acknowledge_no_replay_protection=``
                   for ``"github"``, or an explicit ``tolerance_seconds=``
                   override — which always wins over ``settings``, the
                   standing per-instance-override rule).

    Raises:
        ValueError: Unknown ``provider`` name.

    Edge cases:
        - An explicit ``tolerance_seconds=`` keyword in ``**kwargs`` always
          wins over ``settings.inbound_tolerance_seconds`` — the
          constructor keyword is a per-instance override, never a
          duplicate source (CLAUDE.md's standing webhook-settings rule).
    """
    try:
        verifier_cls = _PROVIDERS[provider]
    except KeyError:
        raise ValueError(f"Unknown inbound webhook provider: {provider!r}") from None

    if "tolerance_seconds" not in kwargs:
        # Import locally to avoid a module-level import cycle risk between
        # verifiers.py and settings.py (neither currently imports the
        # other at module scope, but this keeps the dependency direction
        # obviously one-way: verifiers -> settings, never reversed).
        from varco_core.webhook.settings import WebhookSettings as _WebhookSettings

        resolved_settings = settings if settings is not None else _WebhookSettings()
        kwargs["tolerance_seconds"] = resolved_settings.inbound_tolerance_seconds

    return verifier_cls(secrets, **kwargs)  # type: ignore[arg-type]
