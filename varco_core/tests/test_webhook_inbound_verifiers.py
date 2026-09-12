"""
Plan 038 (S19) / Steps 4, 8, 9, 10 — red-mode tests for
``varco_core.webhook.inbound`` — ``WebhookVerifier`` ABC, ``VerificationResult``,
``VerificationFailure``, and the four HMAC adapters (Standard Webhooks/Svix,
Stripe, GitHub, Slack).

Nothing under ``varco_core/varco_core/webhook/inbound/`` exists yet — every
test here is expected to fail with ``ModuleNotFoundError`` on import.
"""

from __future__ import annotations

import hashlib
import hmac
import time

import pytest

# ── Standard Webhooks / Svix (Step 4) ────────────────────────────────────────


class TestStandardWebhooksVerifierRoundTrip:
    SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"

    def test_outbound_signer_output_verifies_inbound(self, monkeypatch) -> None:
        # The anti-drift proof of §D-S19-gap: a delivery produced by the
        # shipped outbound signer must verify through the new inbound path.
        from varco_core.webhook import signing
        from varco_core.webhook.inbound.verifiers import StandardWebhooksVerifier
        from varco_core.webhook.signing import StandardWebhooksSigner

        monkeypatch.setattr(signing.time, "time", lambda: 1700000000.0)

        signer = StandardWebhooksSigner(secrets=[self.SECRET])
        headers = signer.sign(msg_id="msg_1", timestamp="1700000000", payload="{}")

        verifier = StandardWebhooksVerifier(secrets=[self.SECRET])
        result = verifier.verify(body=b"{}", headers=headers)
        assert result.verified is True
        assert result.provider == "standard_webhooks"
        assert result.message_id == "msg_1"

    def test_svix_header_aliases_verify_identically(self, monkeypatch) -> None:
        from varco_core.webhook import signing
        from varco_core.webhook.inbound.verifiers import StandardWebhooksVerifier
        from varco_core.webhook.signing import StandardWebhooksSigner

        monkeypatch.setattr(signing.time, "time", lambda: 1700000000.0)

        signer = StandardWebhooksSigner(secrets=[self.SECRET])
        headers = signer.sign(msg_id="msg_1", timestamp="1700000000", payload="{}")
        svix_headers = {
            "svix-id": headers["webhook-id"],
            "svix-timestamp": headers["webhook-timestamp"],
            "svix-signature": headers["webhook-signature"],
        }

        verifier = StandardWebhooksVerifier(secrets=[self.SECRET])
        result = verifier.verify(body=b"{}", headers=svix_headers)
        assert result.verified is True

    def test_header_lookup_is_case_insensitive(self, monkeypatch) -> None:
        from varco_core.webhook import signing
        from varco_core.webhook.inbound.verifiers import StandardWebhooksVerifier
        from varco_core.webhook.signing import StandardWebhooksSigner

        monkeypatch.setattr(signing.time, "time", lambda: 1700000000.0)

        signer = StandardWebhooksSigner(secrets=[self.SECRET])
        headers = signer.sign(msg_id="msg_1", timestamp="1700000000", payload="{}")
        mixed_case_headers = {
            "Webhook-Id": headers["webhook-id"],
            "WEBHOOK-TIMESTAMP": headers["webhook-timestamp"],
            "Webhook-Signature": headers["webhook-signature"],
        }

        verifier = StandardWebhooksVerifier(secrets=[self.SECRET])
        result = verifier.verify(body=b"{}", headers=mixed_case_headers)
        assert result.verified is True

    def test_tampered_body_is_signature_mismatch(self, monkeypatch) -> None:
        from varco_core.webhook import signing
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import StandardWebhooksVerifier
        from varco_core.webhook.signing import StandardWebhooksSigner

        monkeypatch.setattr(signing.time, "time", lambda: 1700000000.0)

        signer = StandardWebhooksSigner(secrets=[self.SECRET])
        headers = signer.sign(msg_id="msg_1", timestamp="1700000000", payload="{}")

        verifier = StandardWebhooksVerifier(secrets=[self.SECRET])
        result = verifier.verify(body=b'{"tampered": true}', headers=headers)
        assert result.verified is False
        assert result.failure == VerificationFailure.SIGNATURE_MISMATCH

    def test_expired_timestamp_is_out_of_tolerance(self, monkeypatch) -> None:
        from varco_core.webhook import signing
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import StandardWebhooksVerifier
        from varco_core.webhook.signing import StandardWebhooksSigner

        monkeypatch.setattr(signing.time, "time", lambda: 1700000000.0)
        signer = StandardWebhooksSigner(secrets=[self.SECRET])
        old_ts = str(1700000000 - 301)
        headers = signer.sign(msg_id="msg_1", timestamp=old_ts, payload="{}")

        verifier = StandardWebhooksVerifier(secrets=[self.SECRET])
        result = verifier.verify(body=b"{}", headers=headers)
        assert result.verified is False
        assert result.failure == VerificationFailure.TIMESTAMP_OUT_OF_TOLERANCE

    def test_future_timestamp_beyond_tolerance_is_rejected(self, monkeypatch) -> None:
        from varco_core.webhook import signing
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import StandardWebhooksVerifier
        from varco_core.webhook.signing import StandardWebhooksSigner

        monkeypatch.setattr(signing.time, "time", lambda: 1700000000.0)
        signer = StandardWebhooksSigner(secrets=[self.SECRET])
        future_ts = str(1700000000 + 301)
        headers = signer.sign(msg_id="msg_1", timestamp=future_ts, payload="{}")

        verifier = StandardWebhooksVerifier(secrets=[self.SECRET])
        result = verifier.verify(body=b"{}", headers=headers)
        assert result.verified is False
        assert result.failure == VerificationFailure.TIMESTAMP_OUT_OF_TOLERANCE

    def test_wrong_secret_is_signature_mismatch(self, monkeypatch) -> None:
        from varco_core.webhook import signing
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import StandardWebhooksVerifier
        from varco_core.webhook.signing import StandardWebhooksSigner

        monkeypatch.setattr(signing.time, "time", lambda: 1700000000.0)
        signer = StandardWebhooksSigner(secrets=[self.SECRET])
        headers = signer.sign(msg_id="msg_1", timestamp="1700000000", payload="{}")

        verifier = StandardWebhooksVerifier(secrets=["whsec_someOtherSecretEntirely=="])
        result = verifier.verify(body=b"{}", headers=headers)
        assert result.verified is False
        assert result.failure == VerificationFailure.SIGNATURE_MISMATCH

    def test_missing_header_is_missing_header(self) -> None:
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import StandardWebhooksVerifier

        verifier = StandardWebhooksVerifier(secrets=[self.SECRET])
        result = verifier.verify(
            body=b"{}",
            headers={"webhook-id": "msg_1", "webhook-timestamp": "1700000000"},
        )
        assert result.verified is False
        assert result.failure == VerificationFailure.MISSING_HEADER

    def test_malformed_non_numeric_timestamp_is_malformed_header(self) -> None:
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import StandardWebhooksVerifier

        verifier = StandardWebhooksVerifier(secrets=[self.SECRET])
        result = verifier.verify(
            body=b"{}",
            headers={
                "webhook-id": "msg_1",
                "webhook-timestamp": "not-a-number",
                "webhook-signature": "v1,abc",
            },
        )
        assert result.verified is False
        assert result.failure == VerificationFailure.MALFORMED_HEADER

    def test_empty_signature_header_is_malformed_header(self, monkeypatch) -> None:
        from varco_core.webhook import signing
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import StandardWebhooksVerifier

        monkeypatch.setattr(signing.time, "time", lambda: 1700000000.0)
        verifier = StandardWebhooksVerifier(secrets=[self.SECRET])
        result = verifier.verify(
            body=b"{}",
            headers={
                "webhook-id": "msg_1",
                "webhook-timestamp": "1700000000",
                "webhook-signature": "   ",
            },
        )
        assert result.verified is False
        assert result.failure == VerificationFailure.MALFORMED_HEADER


# ── VerificationResult / VerificationFailure shape ───────────────────────────


def test_verification_result_is_frozen_dataclass() -> None:
    from varco_core.webhook.inbound.base import VerificationResult

    result = VerificationResult(
        verified=True,
        provider="standard_webhooks",
        message_id="msg_1",
        timestamp_checked=True,
    )
    with pytest.raises(Exception):
        result.verified = False  # type: ignore[misc]


def test_verification_result_never_carries_a_secret_field() -> None:
    from varco_core.webhook.inbound.base import VerificationResult

    field_names = {f.name for f in __import__("dataclasses").fields(VerificationResult)}
    assert "secret" not in field_names
    assert "matched_secret" not in field_names
    assert "signature" not in field_names


# ── Stripe (Step 8) ───────────────────────────────────────────────────────────


class TestStripeWebhookVerifier:
    SECRET = "whsec_stripe_test_secret"

    def _sign(self, secret: str, ts: str, body: bytes) -> str:
        signed = f"{ts}.{body.decode()}".encode()
        digest = hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
        return digest

    def test_valid_signature_verifies(self) -> None:
        from varco_core.webhook.inbound.verifiers import StripeWebhookVerifier

        ts = str(int(time.time()))
        body = b'{"id": "evt_1"}'
        sig = self._sign(self.SECRET, ts, body)
        verifier = StripeWebhookVerifier(secrets=[self.SECRET])
        result = verifier.verify(body=body, headers={"Stripe-Signature": f"t={ts},v1={sig}"})
        assert result.verified is True
        assert result.provider == "stripe"

    def test_v0_only_header_is_rejected(self) -> None:
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import StripeWebhookVerifier

        ts = str(int(time.time()))
        body = b'{"id": "evt_1"}'
        verifier = StripeWebhookVerifier(secrets=[self.SECRET])
        result = verifier.verify(body=body, headers={"Stripe-Signature": f"t={ts},v0=deadbeef"})
        assert result.verified is False
        assert result.failure in (
            VerificationFailure.MALFORMED_HEADER,
            VerificationFailure.SIGNATURE_MISMATCH,
        )

    def test_multiple_v1_entries_rotation_accepted(self) -> None:
        from varco_core.webhook.inbound.verifiers import StripeWebhookVerifier

        ts = str(int(time.time()))
        body = b'{"id": "evt_1"}'
        good_sig = self._sign(self.SECRET, ts, body)
        bad_sig = "0" * 64
        verifier = StripeWebhookVerifier(secrets=[self.SECRET])
        result = verifier.verify(
            body=body,
            headers={"Stripe-Signature": f"t={ts},v1={bad_sig},v1={good_sig}"},
        )
        assert result.verified is True

    def test_comma_space_tolerant_parsing(self) -> None:
        from varco_core.webhook.inbound.verifiers import StripeWebhookVerifier

        ts = str(int(time.time()))
        body = b'{"id": "evt_1"}'
        sig = self._sign(self.SECRET, ts, body)
        verifier = StripeWebhookVerifier(secrets=[self.SECRET])
        result = verifier.verify(body=body, headers={"Stripe-Signature": f"t={ts}, v1={sig}"})
        assert result.verified is True

    def test_missing_t_is_malformed_header(self) -> None:
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import StripeWebhookVerifier

        verifier = StripeWebhookVerifier(secrets=[self.SECRET])
        result = verifier.verify(body=b'{"id": "evt_1"}', headers={"Stripe-Signature": "v1=abcdef"})
        assert result.verified is False
        assert result.failure == VerificationFailure.MALFORMED_HEADER


# ── GitHub (Step 9) ───────────────────────────────────────────────────────────


class TestGitHubWebhookVerifier:
    SECRET = "github_test_secret"

    def _sign(self, secret: str, body: bytes) -> str:
        return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

    def test_construction_without_replay_guard_or_ack_raises(self) -> None:
        from varco_core.webhook.inbound.verifiers import GitHubWebhookVerifier

        with pytest.raises(ValueError) as exc:
            GitHubWebhookVerifier(secrets=[self.SECRET])
        message = str(exc.value)
        assert "replay_guard" in message
        assert "acknowledge_no_replay_protection" in message

    def test_construction_with_acknowledgement_succeeds(self) -> None:
        from varco_core.webhook.inbound.verifiers import GitHubWebhookVerifier

        verifier = GitHubWebhookVerifier(
            secrets=[self.SECRET], acknowledge_no_replay_protection=True
        )
        assert verifier.provider == "github"

    def test_valid_signature_over_raw_body(self) -> None:
        from varco_core.webhook.inbound.verifiers import GitHubWebhookVerifier

        body = b'{"action": "opened"}'
        sig = self._sign(self.SECRET, body)
        verifier = GitHubWebhookVerifier(
            secrets=[self.SECRET], acknowledge_no_replay_protection=True
        )
        result = verifier.verify(
            body=body,
            headers={
                "X-Hub-Signature-256": f"sha256={sig}",
                "X-GitHub-Delivery": "delivery-1",
            },
        )
        assert result.verified is True
        assert result.timestamp_checked is False
        assert result.message_id == "delivery-1"

    def test_sha1_header_is_rejected(self) -> None:
        from varco_core.webhook.inbound.verifiers import GitHubWebhookVerifier

        body = b'{"action": "opened"}'
        verifier = GitHubWebhookVerifier(
            secrets=[self.SECRET], acknowledge_no_replay_protection=True
        )
        result = verifier.verify(
            body=body,
            headers={
                "X-Hub-Signature-1": "sha1=deadbeef",
                "X-GitHub-Delivery": "delivery-1",
            },
        )
        assert result.verified is False


# ── Slack (Step 10) ───────────────────────────────────────────────────────────


class TestSlackWebhookVerifier:
    SECRET = "slack_test_secret"

    def _sign(self, secret: str, ts: str, body: bytes) -> str:
        basestring = f"v0:{ts}:{body.decode()}".encode()
        digest = hmac.new(secret.encode("utf-8"), basestring, hashlib.sha256).hexdigest()
        return f"v0={digest}"

    def test_valid_signature_verifies(self) -> None:
        from varco_core.webhook.inbound.verifiers import SlackWebhookVerifier

        ts = str(int(time.time()))
        body = b'{"type": "event_callback"}'
        sig = self._sign(self.SECRET, ts, body)
        verifier = SlackWebhookVerifier(secrets=[self.SECRET])
        result = verifier.verify(
            body=body,
            headers={"X-Slack-Signature": sig, "X-Slack-Request-Timestamp": ts},
        )
        assert result.verified is True
        assert result.provider == "slack"

    def test_missing_timestamp_header_is_missing_header(self) -> None:
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import SlackWebhookVerifier

        verifier = SlackWebhookVerifier(secrets=[self.SECRET])
        result = verifier.verify(
            body=b'{"type": "event_callback"}',
            headers={"X-Slack-Signature": "v0=deadbeef"},
        )
        assert result.verified is False
        assert result.failure == VerificationFailure.MISSING_HEADER

    def test_expired_timestamp_is_out_of_tolerance(self) -> None:
        from varco_core.webhook.inbound.base import VerificationFailure
        from varco_core.webhook.inbound.verifiers import SlackWebhookVerifier

        old_ts = str(int(time.time()) - 301)
        body = b'{"type": "event_callback"}'
        sig = self._sign(self.SECRET, old_ts, body)
        verifier = SlackWebhookVerifier(secrets=[self.SECRET])
        result = verifier.verify(
            body=body,
            headers={"X-Slack-Signature": sig, "X-Slack-Request-Timestamp": old_ts},
        )
        assert result.verified is False
        assert result.failure == VerificationFailure.TIMESTAMP_OUT_OF_TOLERANCE

    def test_body_tampering_alone_does_not_change_signed_timestamp_field(self) -> None:
        # The basestring is "v0:{ts}:{body}" -- a vector showing the
        # timestamp is a distinct signed field, not concatenated with body
        # the way Standard Webhooks does it.
        from varco_core.webhook.inbound.verifiers import SlackWebhookVerifier

        ts = str(int(time.time()))
        body = b'{"type": "event_callback"}'
        sig = self._sign(self.SECRET, ts, body)
        tampered_body = b'{"type": "tampered"}'
        verifier = SlackWebhookVerifier(secrets=[self.SECRET])
        result = verifier.verify(
            body=tampered_body,
            headers={"X-Slack-Signature": sig, "X-Slack-Request-Timestamp": ts},
        )
        assert result.verified is False


# ── get_verifier dispatch (Step 11) ──────────────────────────────────────────


@pytest.mark.parametrize(
    "provider",
    ["standard_webhooks", "svix", "stripe", "slack"],
)
def test_get_verifier_dispatches_by_provider_name(provider: str) -> None:
    from varco_core.webhook.inbound.verifiers import get_verifier

    verifier = get_verifier(provider, secrets=["s3cr3t"])
    assert verifier is not None


def test_get_verifier_github_requires_acknowledgement() -> None:
    from varco_core.webhook.inbound.verifiers import get_verifier

    verifier = get_verifier("github", secrets=["s3cr3t"], acknowledge_no_replay_protection=True)
    assert verifier is not None


def test_get_verifier_unknown_provider_raises_value_error() -> None:
    from varco_core.webhook.inbound.verifiers import get_verifier

    with pytest.raises(ValueError):
        get_verifier("not-a-real-provider", secrets=["s3cr3t"])
