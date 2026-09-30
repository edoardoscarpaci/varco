"""
varco_core.webhook.dispatcher
================================
``WebhookDispatcher`` — the delivery engine (Plan 031 / D4c, Step 12-13,
§D-D4-home, §D-D4-delivery).

⚠️ **Never holds ``AbstractEventBus``.** ``WebhookDispatcher`` is an
``EventConsumer``: it subscribes via ``@listen`` and is wired from
``@PostConstruct`` (or test code) via ``register_to(bus)`` — the one
sanctioned path CLAUDE.md carves out beyond ``OutboxRelay``/``DlqRedriver``.
Once registered, the dispatcher never touches ``bus`` again.

Retry model (§D-D4-delivery — reuse, invent nothing)
------------------------------------------------------
Delivery retries are driven by the **existing** ``varco_core.resilience.
RetryPolicy`` — the same primitive ``@listen(retry_policy=...)`` already
uses elsewhere in this codebase. This module does **not** use the generic
``@listen(retry_policy=..., dlq=...)`` wrapper mechanism, though: that
wrapper retries and DLQs the *whole handler invocation* once, which is the
wrong granularity here — one event can match **N** subscriptions, each of
which must be retried, disabled, and DLQ'd *independently*. Instead,
``register_to()`` is overridden to capture ``retry_policy``/``dlq`` as
instance state, and the ``@listen``-decorated handler runs its own
per-subscription retry loop, reusing ``RetryPolicy.compute_delay()``
directly rather than inventing a second backoff formula.

DESIGN: constructor default ``RetryPolicy`` is fast, not literally
Svix's seconds schedule
    §D-D4-delivery's documented *shape* is Svix's 8-attempt exponential
    schedule with jitter (immediate, 5s, 5m, 30m, 2h, 5h, 10h, 10h in
    Svix's real deployment). Reusing ``RetryPolicy`` gives the same
    *shape* (max_attempts, exponential backoff, jitter) but not
    arbitrary non-monotonic per-attempt seconds — ``RetryPolicy`` is a
    single exponential-with-jitter formula, and inventing a second,
    schedule-table-shaped retry primitive is exactly what §D-D4-delivery
    forbids ("no new reliability primitive invented").
    ✅ The shipped ``WebhookSettings`` defaults (``max_attempts=8``,
       small ``base_delay``) keep the *attempt count* Svix-shaped while
       keeping the unit test suite fast — a literal seconds-to-hours
       schedule would make every dispatcher test take hours.
    ✅ A production deployment moves the whole schedule with three env
       vars (``VARCO_WEBHOOK_RETRY_MAX_ATTEMPTS`` /
       ``_RETRY_BASE_DELAY_SECONDS`` / ``_RETRY_MAX_DELAY_SECONDS``), or
       passes its own ``RetryPolicy`` — the "configurable" half of
       §D-D4-delivery's requirement.
    ❌ The literal numeric default here does not itself match Svix's
       real-world seconds — documented here explicitly so nobody mistakes
       it for a production-ready number.

Thread safety:  N/A — one dispatcher instance per process, no shared
                mutable state beyond the repository/dlq it is given.
Async safety:   ✅ All delivery logic is ``async def``. ``asyncio.sleep``
                is used for backoff — never blocks the event loop.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import TYPE_CHECKING, Any

from varco_core.event.base import AbstractEventBus, Event, Subscription
from varco_core.event.consumer import EventConsumer, listen
from varco_core.resilience.retry import RetryPolicy
from varco_core.webhook import transport
from varco_core.webhook.settings import WebhookSettings
from varco_core.webhook.signing import WebhookSigner, get_signer
from varco_core.webhook.ssrf import SSRFValidationError, validate_target

if TYPE_CHECKING:
    from varco_core.event.dlq import AbstractDeadLetterQueue
    from varco_core.webhook.base import WebhookSubscriptionRepository
    from varco_core.webhook.models import WebhookSubscription

_logger = logging.getLogger(__name__)

__all__ = ["WebhookDispatcher", "WebhookTriggerEvent"]


class WebhookTriggerEvent(Event):  # type: ignore[misc]
    """
    Generic event envelope the dispatcher listens for.

    An application publishes its own domain events (``OrderPlacedEvent``,
    etc.) — the dispatcher does not know their Python types ahead of time.
    Rather than requiring every application event to inherit from a
    varco-specific base, an app (or a thin adapter consumer) republishes
    the event of interest as a ``WebhookTriggerEvent`` carrying the
    original event's type name and a JSON-serializable payload. This is
    the same "adapt at the boundary, keep the core generic" shape as
    ``varco_core.event.cloudevents``.

    Attributes:
        matched_event_type: The logical event type name matched against each
                          subscription's ``event_patterns`` (e.g.
                          ``"order.created"``).
        payload:          The JSON-serializable event body delivered to
                          the receiver's webhook endpoint.
    """

    __event_type__ = "webhook.trigger"

    matched_event_type: str
    payload: dict[str, Any]


def _policy_from_settings(settings: WebhookSettings) -> RetryPolicy:
    """
    Build the default per-subscription ``RetryPolicy`` from ``settings``.

    Every field of the policy is settings-derived so a deployment can move
    the whole schedule to a production-scale tail (Stripe/Svix's hours)
    without hand-building a ``RetryPolicy``. See the module DESIGN block for
    why the shipped numeric defaults are fast rather than literally Svix's.
    """
    return RetryPolicy(
        max_attempts=settings.retry_max_attempts,
        base_delay=settings.retry_base_delay_seconds,
        max_delay=settings.retry_max_delay_seconds,
        jitter=True,
    )


class WebhookDispatcher(EventConsumer):
    """
    Delivers signed HTTP callbacks for every ``ACTIVE`` subscription
    matching an incoming ``WebhookTriggerEvent`` (§D-D4-delivery).

    Configuration flows from one place — ``WebhookSettings`` (env prefix
    ``VARCO_WEBHOOK_``). Every value it carries reaches the call site that
    needs it: the SSRF knobs are forwarded to ``validate_target()``, the
    replay-tolerance window to the constructed ``WebhookSigner``, and the
    retry/timeout/disable knobs become this dispatcher's defaults. The
    explicit keyword arguments below stay as per-instance **overrides** for
    a caller that wants one value different without building a whole
    settings object.

    DESIGN: settings object + explicit-keyword overrides
        ✅ ``VARCO_WEBHOOK_ALLOW_INSECURE_HTTP`` and friends actually take
           effect — before this, they were documented configuration that
           reached no call site, which is worse than no setting at all
           because an operator would believe the deny-list was extended.
        ✅ One source of truth: the settings docstring is the reference for
           every knob; nothing is configurable only via a constructor
           keyword and nothing only via env.
        ❌ Two ways to express the same value (settings field and keyword).
           Accepted because the precedence rule is one line — an explicit
           keyword always wins — and mirrors how ``RetryPolicy`` is already
           passed elsewhere in this codebase.
        Rejected — **settings-only, drop the keywords**: ❌ breaks every
        existing caller (including the test suite's
        ``disable_after_failures=1``) and forces a ``BaseSettings``
        construction, which reads the environment, into unit tests that
        want one number changed.

    Args:
        repository:             The ``WebhookSubscriptionRepository`` to
                                 read subscriptions from and update
                                 (``consecutive_failures``/``status``) on
                                 delivery outcomes.
        settings:                Delivery configuration. ``None`` (the
                                 default) constructs ``WebhookSettings()``,
                                 which reads ``VARCO_WEBHOOK_*`` from the
                                 environment.
        retry_policy:            Overrides the settings-derived retry
                                 schedule. ``None`` builds one from
                                 ``settings`` — see the module DESIGN block
                                 for why its default is Svix-*shaped* rather
                                 than Svix's literal seconds.
        request_timeout_seconds: Overrides ``settings.request_timeout_seconds``.
        disable_after_failures:  Overrides ``settings.disable_after_failures``.

    Edge cases:
        - A ``DISABLED`` subscription is skipped entirely — never even
          attempted (guarded by
          ``test_disabled_subscription_is_skipped_entirely``).
        - The DLQ push on exhaustion never raises — same
          ``push()``-never-raises contract ``OutboxRelay``/``JobRunner``
          honour (CLAUDE.md).
        - ``settings=None`` reads the process environment at construction
          time. A test that must not see the ambient environment passes an
          explicit ``WebhookSettings(...)``.
    """

    def __init__(
        self,
        *,
        repository: WebhookSubscriptionRepository,
        settings: WebhookSettings | None = None,
        retry_policy: RetryPolicy | None = None,
        request_timeout_seconds: float | None = None,
        disable_after_failures: int | None = None,
        dlq: AbstractDeadLetterQueue | None = None,
    ) -> None:
        self._repository = repository
        # WebhookSettings() reads VARCO_WEBHOOK_* — constructing it here is
        # what makes those env vars real rather than aspirational.
        self._settings = settings if settings is not None else WebhookSettings()
        self._retry_policy = retry_policy or _policy_from_settings(self._settings)
        # `is not None` rather than `or`, so an explicit 0/0.0 override is
        # honoured instead of silently falling back to the settings value.
        self._request_timeout_seconds = (
            request_timeout_seconds
            if request_timeout_seconds is not None
            else self._settings.request_timeout_seconds
        )
        self._disable_after_failures = (
            disable_after_failures
            if disable_after_failures is not None
            else self._settings.disable_after_failures
        )
        # Captured here (constructor, Plan 050's `dlq=` kwarg — an
        # injected AbstractDeadLetterQueue reaches the dispatcher without
        # the app remembering to pass it to register_to()) OR by the
        # overridden register_to() below, which still wins (an explicit
        # register_to(bus, dlq=...) call overrides a DI-injected one) —
        # the generic @listen(retry_policy=..., dlq=...) wrapper mechanism
        # is deliberately NOT used here (see module docstring); this
        # dispatcher drives its own per-subscription retry loop instead.
        self._dlq: AbstractDeadLetterQueue | None = dlq

    def register_to(
        self,
        bus: AbstractEventBus,
        *,
        retry_policy: RetryPolicy | None = None,
        dlq: AbstractDeadLetterQueue | None = None,
    ) -> list[Subscription]:
        """
        Wire this dispatcher to ``bus`` and capture ``dlq`` for the
        dispatcher's own internal retry loop.

        ``retry_policy``/``dlq`` are intentionally NOT forwarded to
        ``super().register_to()`` — doing so would activate the generic
        per-handler retry wrapper, which retries/DLQs the whole handler
        call (all matching subscriptions at once) rather than each
        subscription independently. See the module docstring.
        """
        if dlq is not None:
            self._dlq = dlq
        return super().register_to(bus)

    @listen(WebhookTriggerEvent)
    async def _on_trigger(self, event: WebhookTriggerEvent) -> None:
        """Look up matching subscriptions and deliver to each independently."""
        tenant_id = event.payload.get("tenant_id")
        subscriptions = await self._repository.find_active_matching(
            event.matched_event_type, tenant_id=tenant_id
        )
        for subscription in subscriptions:
            await self._deliver_one(subscription, event)

    async def _deliver_one(
        self, subscription: WebhookSubscription, event: WebhookTriggerEvent
    ) -> None:
        """Run the full retry loop for one subscription; DLQ on exhaustion."""
        import asyncio
        import json

        webhook_id = str(uuid.uuid4())
        payload_json = json.dumps(event.payload, default=str)
        signer = self._build_signer(subscription)

        last_exc: BaseException | None = None
        first_failed_at = time.time()
        succeeded = False

        for attempt_index in range(self._retry_policy.max_attempts):
            try:
                headers = self._build_headers(signer, webhook_id, payload_json, subscription)
                response = await self._send(
                    subscription.target_url,
                    headers=headers,
                    body=payload_json.encode(),
                    timeout=self._request_timeout_seconds,
                )
                if 200 <= response.status_code < 300:
                    succeeded = True
                    break
                last_exc = RuntimeError(f"Webhook receiver returned HTTP {response.status_code}.")
            except Exception as exc:  # noqa: BLE001 - any transport failure is retryable
                last_exc = exc

            if attempt_index < self._retry_policy.max_attempts - 1:
                await asyncio.sleep(self._retry_policy.compute_delay(attempt_index))

        if succeeded:
            await self._on_success(subscription)
            return

        await self._on_exhaustion(subscription, event, last_exc, webhook_id, first_failed_at)

    def _build_signer(self, subscription: WebhookSubscription) -> WebhookSigner:
        """
        Construct the subscription's signer with the configured replay
        window.

        ``signature_tolerance_seconds`` governs how far a ``webhook-timestamp``
        may drift before a verifier rejects it; passing it here is what makes
        ``VARCO_WEBHOOK_SIGNATURE_TOLERANCE_SECONDS`` affect the signatures
        this dispatcher produces and verifies.
        """
        return get_signer(
            subscription.signer,
            secrets=subscription.active_secrets,
            tolerance_seconds=self._settings.signature_tolerance_seconds,
        )

    def _build_headers(
        self,
        signer: Any,
        webhook_id: str,
        payload_json: str,
        subscription: WebhookSubscription,
    ) -> dict[str, str]:
        """Sign the payload and merge in the subscription's custom headers."""
        if subscription.signer == "rfc9421":
            # RFC 9421 needs method/target-uri/authority — resolved lazily at
            # send time in a real implementation; the fast path here signs
            # against the target URL directly since @listen tests stub _send.
            from urllib.parse import urlsplit

            parsed = urlsplit(subscription.target_url)
            signed = signer.sign(
                method="POST",
                target_uri=subscription.target_url,
                authority=parsed.netloc,
                payload=payload_json.encode(),
            )
        else:
            signed = signer.sign(
                msg_id=webhook_id,
                timestamp=str(int(time.time())),
                payload=payload_json,
            )
        headers = dict(subscription.custom_headers)
        headers.update(signed)
        headers["Content-Type"] = "application/json"
        return headers

    async def _send(
        self, url: str, *, headers: dict[str, str], body: bytes, timeout: float
    ) -> transport.WebhookResponse:
        """
        Validate the target (§D-D4-ssrf) and perform the HTTP send.

        The three §D-D4-ssrf knobs come from ``self._settings``, so a
        deployment's ``VARCO_WEBHOOK_ALLOW_LIST`` / ``_EXTRA_DENY_RANGES`` /
        ``_ALLOW_INSECURE_HTTP`` govern what this dispatcher will connect to.

        Monkeypatched wholesale in unit tests (``WebhookDispatcher._send``)
        to avoid real network I/O — the SSRF validation and transport
        module are exercised by their own dedicated test suites instead.
        """
        try:
            target = await validate_target(
                url,
                allow_insecure_http=self._settings.allow_insecure_http,
                allow_list=self._settings.allow_list,
                extra_deny_ranges=self._settings.extra_deny_ranges,
            )
        except SSRFValidationError as exc:
            raise RuntimeError(f"Webhook target rejected by SSRF guard: {exc}") from exc
        return await transport.send_webhook(target, headers=headers, body=body, timeout=timeout)

    async def _on_success(self, subscription: WebhookSubscription) -> None:
        """Reset the consecutive-failure counter on any successful delivery."""
        if subscription.consecutive_failures != 0:
            subscription.consecutive_failures = 0
            await self._repository.save(subscription)

    async def _on_exhaustion(
        self,
        subscription: WebhookSubscription,
        event: WebhookTriggerEvent,
        last_exc: BaseException | None,
        webhook_id: str,
        first_failed_at: float,
    ) -> None:
        """
        Push to the DLQ (never raises) and update the failure count,
        auto-disabling the subscription past the configured threshold
        (§D-D4-delivery).
        """
        subscription.consecutive_failures += 1
        if subscription.consecutive_failures >= self._disable_after_failures:
            subscription.status = "DISABLED"
            _logger.warning(
                "Webhook subscription %s auto-disabled after %d consecutive failures.",
                subscription.pk,
                subscription.consecutive_failures,
            )
        await self._repository.save(subscription)

        if self._dlq is None:
            return

        from datetime import UTC, datetime

        from varco_core.event.dlq import DeadLetterEntry

        try:
            await self._dlq.push(
                DeadLetterEntry(
                    event=event,
                    channel=f"webhook:{webhook_id}",
                    handler_name=f"{type(self).__qualname__}._deliver_one",
                    error_type=type(last_exc).__name__ if last_exc else "Unknown",
                    error_message=str(last_exc) if last_exc else "",
                    attempts=self._retry_policy.max_attempts,
                    first_failed_at=datetime.fromtimestamp(first_failed_at, tz=UTC),
                    last_failed_at=datetime.now(tz=UTC),
                    tenant_id=subscription.tenant_id,
                )
            )
        except Exception:  # noqa: BLE001 - push() must never raise to the caller
            _logger.exception("WebhookDispatcher: DLQ push failed — swallowing per contract.")
