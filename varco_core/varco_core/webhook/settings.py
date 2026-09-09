"""
varco_core.webhook.settings
=============================
``WebhookSettings`` — configuration for outbound webhook delivery
(Plan 031 / D4a, Step 1).

Not registered with a providify ``@Provider`` at import time (unlike
``IdempotencySettings``'s aspirational docstring) — ``varco_core`` ships no
DI wiring module of its own (CLAUDE.md: DI wiring lives in each backend's
``di.py``); a consuming app or backend package registers this the same way
it registers any other ``pydantic`` ``BaseSettings`` subclass, via its own
``@Provider``.
"""

from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings

__all__ = ["WebhookSettings"]


class WebhookSettings(VarcoSettings):
    """
    Configuration for outbound webhook delivery (§D-D4-delivery,
    §D-D4-ssrf, §D-D4-signing).

    Attributes:
        allow_insecure_http:    Deployment-wide opt-in for ``http://``
                                 targets (§D-D4-ssrf layer 1). Never settable
                                 per-subscription/per-tenant — a tenant must
                                 not be able to downgrade its own delivery to
                                 plaintext. Default ``False``.
        signature_tolerance_seconds: Replay-protection window used by
                                 ``StandardWebhooksSigner``/``Rfc9421Signer``.
                                 Default ``300`` — Stripe's observed
                                 convention (brief 005 §2); not normative.
        retry_max_attempts:     Total delivery attempts, including the
                                 first. Default ``8`` (Svix-shaped —
                                 §D-D4-delivery).
        retry_base_delay_seconds: Base delay for the exponential-backoff
                                 schedule. Documented production convention
                                 is Stripe/Svix's multi-hour tail; the
                                 numeric default here is deliberately small
                                 (see ``varco_core.webhook.dispatcher``'s
                                 module docstring for the full trade-off) —
                                 override for a production deployment.
        retry_max_delay_seconds: Ceiling the exponential backoff saturates
                                 at. Present so the whole default
                                 ``RetryPolicy`` is expressible from
                                 settings alone — without it a caller
                                 wanting a production-scale tail would have
                                 to hand-build a ``RetryPolicy`` just to
                                 raise the cap.
        request_timeout_seconds: Per-attempt HTTP timeout. Default ``10.0``
                                 (Stripe's convention, brief 005 §2).
        disable_after_failures: Consecutive failures (across distinct
                                 events) before a subscription is
                                 auto-disabled. Default ``20``.
        persist_all_deliveries: When ``True``, every delivery attempt is
                                 recorded as a ``WebhookDelivery``, not just
                                 failures (open question 2). Default
                                 ``False`` — failures-only, the cheaper
                                 write path.
        allow_list:              Optional exclusive allowlist of target
                                 hosts/CIDRs (§D-D4-ssrf layer 3). ``None``
                                 (default) — deny-list-only posture.
        extra_deny_ranges:       Additional CIDR ranges to block, beyond the
                                 built-in private/loopback/link-local set.
        inbound_tolerance_seconds: Clock-skew tolerance accepted from an
                                 *inbound* delivery's signed timestamp
                                 (Plan 038 / S19, §D-S19-config). Deliberately
                                 a **separate** field from
                                 ``signature_tolerance_seconds`` — that one
                                 governs what we ask receivers of *our own*
                                 outbound deliveries to tolerate; this one
                                 governs what we accept from a possibly
                                 clock-skewed upstream provider. Loosening
                                 one must never loosen the other. Default
                                 ``300.0`` — brief 013 §2's de-facto
                                 Stripe/Svix/Slack convention (identical to
                                 the outbound default, so nothing surprising
                                 out of the box).
        inbound_replay_ttl_seconds: Default TTL, in seconds, for
                                 ``varco_core.webhook.inbound.replay.WebhookReplayGuard``
                                 entries. Default ``600.0`` — brief 013 §46:
                                 "with a 5-minute tolerance window, a cache
                                 with 5-10 minute TTL covers it". ⚠️ GitHub
                                 (no timestamp, §D-S19-github) needs a much
                                 longer window — brief 013 §47 suggests
                                 24-48h — which is why a GitHub route passes
                                 ``ttl_seconds=`` to ``WebhookReplayGuard``
                                 explicitly rather than relying on this
                                 default (Open question 4).

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Pure value object — no I/O.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_WEBHOOK_")

    allow_insecure_http: bool = False
    signature_tolerance_seconds: float = 300.0
    retry_max_attempts: int = 8
    retry_base_delay_seconds: float = 0.01
    retry_max_delay_seconds: float = 0.2
    request_timeout_seconds: float = 10.0
    disable_after_failures: int = 20
    persist_all_deliveries: bool = False
    allow_list: tuple[str, ...] | None = None
    extra_deny_ranges: tuple[str, ...] = ()
    inbound_tolerance_seconds: float = 300.0
    inbound_replay_ttl_seconds: float = 600.0
