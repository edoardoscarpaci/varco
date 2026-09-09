"""
varco_core.webhook.inbound
============================
Inbound webhook signature verification (Plan 038 / S19) — the receiver-side
counterpart to ``varco_core.webhook.signing``'s outbound signer.

See ``base.py``'s module docstring for the full §D-S19-shape ``DESIGN:``
block and the "no ``@Singleton``/``@Provider``/``@Configuration`` here"
rule this package must never violate.

Re-exports the public surface so a caller writes
``from varco_core.webhook.inbound import WebhookVerifier, get_verifier``
rather than reaching into ``base``/``verifiers``/``replay`` directly.

⚠️ ``varco_core/webhook/__init__.py`` carries ``__all__ = []`` and adds no
name for this package (verified by grep, Plan 038 §What already exists) —
``api_surface.py`` therefore records no new top-level surface for this
addition; see Step 27.
"""

from __future__ import annotations

from varco_core.webhook.inbound.base import (
    SecretEncoding,
    VerificationFailure,
    VerificationResult,
    WebhookVerifier,
)
from varco_core.webhook.inbound.replay import WebhookReplayGuard
from varco_core.webhook.inbound.verifiers import (
    GitHubWebhookVerifier,
    HmacWebhookVerifier,
    SlackWebhookVerifier,
    StandardWebhooksVerifier,
    StripeWebhookVerifier,
    SvixWebhookVerifier,
    get_verifier,
)

__all__ = [
    "SecretEncoding",
    "VerificationFailure",
    "VerificationResult",
    "WebhookVerifier",
    "WebhookReplayGuard",
    "HmacWebhookVerifier",
    "StandardWebhooksVerifier",
    "SvixWebhookVerifier",
    "StripeWebhookVerifier",
    "GitHubWebhookVerifier",
    "SlackWebhookVerifier",
    "get_verifier",
]
