"""
varco_fastapi.webhook.inbound
================================
``verify_webhook`` — the FastAPI route dependency wiring
``varco_core.webhook.inbound`` into an app (Plan 038 / S19, Step 19,
§D-S19-seam).

§D-S19-seam — a route dependency, not a middleware
    A middleware structurally cannot do this: the secret and provider are
    per-route facts (``/hooks/stripe`` and ``/hooks/github`` need different
    secrets *and* different algorithms), which a middleware would need a
    path→verifier map to handle — the routing table re-implemented one
    layer too early. ``varco_fastapi/varco_fastapi/middleware/__init__.py``
    and ``app.py`` are **untouched by this plan** — Plan 041 owns the
    ordering table.

The raw-body pitfall (brief 013 §32, §50.2) is closed by this dependency,
not worked around: ``await request.body()`` caches into ``Request._body``
(pinned Starlette: ``requests.py:254-260``) and ``stream()`` re-yields the
cached value (``:234-236``), so a downstream pydantic body model on the
*same* route still parses. The verified bytes are handed to the handler as
``VerifiedWebhook.body``, so it never needs to re-serialize anything to
recover byte fidelity.

``BodyLimitMiddleware`` (on by default at 10 MiB, outside every inner
layer) rejects an over-limit body with a 413 *before* this dependency ever
buffers anything — no coordination needed, it is simply outside this
dependency's reach.

DESIGN: reserve-before-yield, complete-after-clean-return,
release-in-except (§D-S19-replay)
    ✅ A dependency **with** ``yield`` is what makes the replay guard's
       retry story correct: a provider that retries a delivery our handler
       *raised* on gets ``release()`` — the retry is accepted. A clean
       return gets ``complete()`` — a subsequent identical delivery is a
       genuine replay.
    ❌ **Honest limitation, not fixed here:** a handler that *returns* a
       5xx ``Response`` object without raising is indistinguishable from a
       success at this layer — the delivery is marked ``complete()`` and
       is not replayable. If your handler wants to keep a delivery
       replayable on a failure path, raise rather than returning an error
       response, or call ``replay_guard.release(...)`` yourself.

⚠️ §D-S19-secret: nothing in this module logs the secret or the raw
signature header value — only ``provider``/``message_id``/``failure`` ever
reach a log line or the response body (via ``WebhookSignatureError``'s
empty ``error_params()``).

Thread safety:  ✅ ``VerifiedWebhook`` is a frozen dataclass — safe to
                share/read across the request's lifetime.
Async safety:   ✅ The dependency is ``async def`` (FastAPI's contract for
                a yield-dependency); ``verifier.verify()`` itself is
                synchronous pure CPU, called inline with no lock needed.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from fastapi import Request
from varco_core.exception.webhook import WebhookSignatureError

if TYPE_CHECKING:
    from varco_core.webhook.inbound.base import WebhookVerifier
    from varco_core.webhook.inbound.replay import WebhookReplayGuard

__all__ = ["VerifiedWebhook", "verify_webhook"]

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VerifiedWebhook:
    """
    The verified delivery handed to a route handler.

    Attributes:
        body:               The exact raw request body bytes that were
                             verified — never re-serialized (brief 013
                             §50.2's exact prohibition on verifying
                             ``request.json()`` output instead of the raw
                             bytes).
        provider:            The verifier's provider name (e.g. ``"stripe"``).
        message_id:          The provider's dedup key, or ``None`` when the
                             provider ships none.
        timestamp_checked:   Whether a tolerance-window check was actually
                             performed — ``False`` for GitHub, structurally.

    Thread safety:  ✅ ``frozen=True`` — immutable.
    Async safety:   ✅ Pure value object — no I/O.
    """

    body: bytes
    provider: str
    message_id: str | None
    timestamp_checked: bool


def verify_webhook(
    verifier: WebhookVerifier,
    *,
    replay_guard: WebhookReplayGuard | None = None,
) -> Callable[[Request], AsyncIterator[VerifiedWebhook]]:
    """
    Build a FastAPI dependency that verifies an inbound webhook's
    signature (and, optionally, guards against a replayed delivery).

    Args:
        verifier:     The ``WebhookVerifier`` to check the delivery against
                      (per-route — a Stripe route and a GitHub route each
                      construct their own with their own secrets).
        replay_guard: Optional ``WebhookReplayGuard`` — when supplied, a
                      message id already seen (in flight or completed)
                      raises ``WebhookReplayError`` (409) instead of being
                      reprocessed. **Mandatory in practice** for a
                      ``GitHubWebhookVerifier`` (it refuses construction
                      without one, or an explicit acknowledgement —
                      §D-S19-github); optional for every other provider.

    Returns:
        An ``async def`` dependency function suitable for
        ``Depends(verify_webhook(...))``, yielding a ``VerifiedWebhook``.

    Raises:
        WebhookSignatureError: The verifier reports ``verified=False`` —
            renders as HTTP 401 via ``ErrorMiddleware``.
        WebhookReplayError: ``replay_guard`` reports the message id as
            already in flight or already processed — HTTP 409.

    Edge cases:
        - **A handler that returns a 5xx ``Response`` without raising is
          treated as a success** — the delivery is marked ``complete()``
          and is *not* replayable. Raise instead, or call
          ``replay_guard.release(...)`` yourself, if that is not the
          behaviour you want.
        - A body over ``BodyLimitMiddleware``'s ceiling (10 MiB by default)
          never reaches this dependency at all — it is rejected 413 by the
          middleware, which sits outside every inner layer.
        - No ``message_id`` (a provider that ships none in its headers —
          Stripe's own dedup id lives in the JSON body, which this
          dependency deliberately never parses) does **not** skip
          ``replay_guard`` — the claim key falls back to a SHA-256 hash of
          the raw body, so a byte-identical retried delivery is still
          caught. ``VerifiedWebhook.message_id`` itself is unaffected by
          this fallback; it continues to report exactly what the verifier
          extracted (``None`` in this case).

    Async safety: ✅ Safe to use concurrently across requests — all
        per-request state lives in the closure-local dependency
        invocation, never on ``verifier``/``replay_guard`` themselves.
    """

    async def _dependency(request: Request) -> AsyncIterator[VerifiedWebhook]:
        # Buffering the body here (rather than in a middleware) is exactly
        # what makes this a route-level, per-provider seam: Starlette
        # caches the result on `Request._body`, so a downstream pydantic
        # body model on the *same* route still parses correctly afterwards
        # (§D-S19-seam's DESIGN block — pinned by Step 18's combined test).
        body = await request.body()
        result = verifier.verify(body=body, headers=request.headers)

        if not result.verified:
            # ⚠️ §D-S19-secret: log only provider/message_id/failure — never
            # the signature header value or the secret.
            _logger.warning(
                "Inbound webhook verification failed: provider=%s message_id=%s failure=%s",
                result.provider,
                result.message_id,
                result.failure,
            )
            raise WebhookSignatureError(result.provider)

        # A provider that ships no natural dedup id in its headers (Stripe's
        # own id lives in the JSON body, deliberately never parsed here —
        # verification only ever sees raw bytes + headers) still needs a
        # replay key: fall back to a content hash. This never affects
        # `VerifiedWebhook.message_id`, which continues to report exactly
        # what the verifier extracted.
        replay_key = result.message_id or hashlib.sha256(body).hexdigest()

        claimed = False
        if replay_guard is not None:
            await replay_guard.claim(result.provider, replay_key, body)
            claimed = True

        try:
            yield VerifiedWebhook(
                body=body,
                provider=result.provider,
                message_id=result.message_id,
                timestamp_checked=result.timestamp_checked,
            )
        except BaseException:
            if claimed:
                assert replay_guard is not None
                await replay_guard.release(result.provider, replay_key)
            raise
        else:
            if claimed:
                assert replay_guard is not None
                await replay_guard.complete(result.provider, replay_key, body)

    return _dependency
