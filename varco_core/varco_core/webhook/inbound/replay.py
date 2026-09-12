"""
varco_core.webhook.inbound.replay
====================================
``WebhookReplayGuard`` — a thin adapter reusing
``varco_core.idempotency.AbstractIdempotencyStore`` as an inbound-webhook
replay cache (Plan 038 / S19, Step 13, §D-S19-replay).

⛔ **Deviation from brief 013's ``AbstractWebhookReplayCache`` + three
backends sketch.** No new ABC, no new backend — ``reserve()`` is already
documented as *"the single atomic primitive… concurrent callers racing on
the same key receive exactly one ACQUIRED"*
(``idempotency/base.py:96-101``), which is verbatim what brief 013 §48
asks for. A new ABC would owe four new backends, four conformance
subclasses, and a ``COVERAGE.md`` row per implementation for semantics
identical to one that already ships (see the plan's §D-S19-replay
``DESIGN:`` block for the full argument).

``ReserveOutcome`` mapping (the load-bearing part of this adapter):
    ACQUIRED  -> proceed; the caller must eventually call ``complete()``
                 (clean return) or ``release()`` (failure).
    IN_FLIGHT -> a concurrent duplicate is still being processed ->
                 ``WebhookReplayError``.
    REPLAY    -> a completed record already exists -> ``WebhookReplayError``.

This is why a provider's retry of a delivery our own handler *failed* is
still accepted: ``release()`` removes the reservation entirely, so the next
``claim()`` for the same id sees no entry and returns ``ACQUIRED`` again. A
naive ``has_seen``/``mark_seen`` cache (the brief's own sketch) gets this
wrong — it would mark a delivery "seen" the moment it arrives, swallowing a
legitimate provider retry after our own failure and losing the event
permanently.

Plan 038's Open question 2 asked whether ``claim()`` should take the
already-computed ``VerificationResult`` instead of the three primitive
values below, to remove the caller's opportunity to pass a mismatched
``provider``. This module resolves it the other way: ``claim()``/
``complete()``/``release()`` take plain ``(provider, message_id, body)`` (or
``(provider, message_id)`` for ``release()``), matching ``AbstractIdempotencyStore``'s
own primitive-argument shape and keeping this module free of any import-time
dependency on ``varco_core.webhook.inbound.base``. The call site
(``varco_fastapi.webhook.inbound.verify_webhook``) always derives all three
from one already-verified ``VerificationResult``, so the "mismatched
provider" risk the open question raised does not arise in practice.

Thread safety:  ✅ Delegates all locking to the underlying
                ``AbstractIdempotencyStore`` implementation — this adapter
                holds no state of its own beyond the store reference and
                the immutable ``ttl_seconds``.
Async safety:   ✅ All methods are ``async def``, matching the store's own
                contract.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

from varco_core.exception.webhook import WebhookReplayError
from varco_core.idempotency.base import ReserveOutcome
from varco_core.idempotency.record import IdempotencyRecord

if TYPE_CHECKING:
    from varco_core.idempotency.base import AbstractIdempotencyStore

__all__ = ["WebhookReplayGuard"]


def _storage_key(provider: str, message_id: str) -> str:
    """
    Build the mandatory ``webhook:{provider}:{message_id}`` storage key.

    The ``webhook:`` prefix is what keeps this guard's key space disjoint
    from ``IdempotencyMiddleware``'s own keys when a store is shared
    between the two (§D-S19-replay's documented, accepted risk) —
    asserted by a test (Step 12).
    """
    return f"webhook:{provider}:{message_id}"


@dataclass(frozen=True)
class WebhookReplayGuard:
    """
    Adapts an ``AbstractIdempotencyStore`` into an inbound-webhook replay
    guard (§D-S19-replay).

    Attributes:
        store:       Any ``AbstractIdempotencyStore`` implementation — the
                     in-memory default for tests/single-process, or a
                     durable backend (Redis/SA/Beanie) for a multi-process
                     deployment.
        ttl_seconds: How long a claimed message id remains rejected as a
                     replay. Must be > 0. brief 013 §46's guidance is
                     "5–10 minutes covers a 5-minute tolerance window" for
                     timestamp-bearing providers; GitHub (no timestamp,
                     §D-S19-github) needs a much longer window — brief 013
                     §47 suggests 24–48h — passed explicitly per route,
                     never a second settings field (Open question 4).

    Raises:
        ValueError: ``ttl_seconds`` is not positive.

    Thread safety:  ✅ ``frozen=True`` — immutable after construction;
                    all mutable state lives in ``store``.
    Async safety:   ✅ Every method is ``async def``, safe to call
                    concurrently for the same ``(provider, message_id)`` —
                    the underlying store's ``reserve()`` is the atomic
                    primitive that makes this true.
    """

    store: AbstractIdempotencyStore
    ttl_seconds: float

    def __post_init__(self) -> None:
        if self.ttl_seconds <= 0:
            raise ValueError(f"ttl_seconds must be > 0, got {self.ttl_seconds!r}.")

    async def claim(self, provider: str, message_id: str, body: bytes) -> None:
        """
        Atomically claim ``(provider, message_id)``, or raise if it has
        already been seen.

        Args:
            provider:   The verifier's provider name (e.g. ``"stripe"``).
            message_id: The provider's dedup key for this delivery.
            body:       The raw delivery body — hashed into the
                        fingerprint stored alongside the reservation
                        (not compared here; the underlying store's
                        ``REPLAY``/``IN_FLIGHT`` outcomes are what this
                        method acts on).

        Raises:
            WebhookReplayError: The store reports ``IN_FLIGHT`` (a
                concurrent duplicate is still being processed) or
                ``REPLAY`` (a completed record already exists) —
                §D-S19-replay's documented "mild semantic stretch",
                argued in the module docstring above.

        Async safety: ✅ Safe to call concurrently for the same
            ``(provider, message_id)`` — of N concurrent callers, exactly
            one raises no exception (the store's atomic ``reserve()``
            guarantee).
        """
        key = _storage_key(provider, message_id)
        fingerprint = hashlib.sha256(body).hexdigest()
        outcome = await self.store.reserve(key, fingerprint, ttl=self.ttl_seconds)
        if outcome is not ReserveOutcome.ACQUIRED:
            raise WebhookReplayError(provider)

    async def complete(self, provider: str, message_id: str, body: bytes) -> None:
        """
        Mark ``(provider, message_id)`` as successfully processed.

        Call after the route handler returns cleanly — this is what makes
        a subsequent delivery of the same id a genuine replay (rejected)
        rather than an in-flight duplicate.

        Args:
            provider:   The verifier's provider name.
            message_id: The provider's dedup key for this delivery.
            body:       The raw delivery body — hashed into the completed
                        record's fingerprint for parity with ``claim()``.

        Async safety: ✅ Safe to call once per successful ``claim()``.
        """
        key = _storage_key(provider, message_id)
        fingerprint = hashlib.sha256(body).hexdigest()
        record = IdempotencyRecord(status=200, body=b"", headers={}, fingerprint=fingerprint)
        await self.store.complete(key, record)

    async def release(self, provider: str, message_id: str) -> None:
        """
        Release a claimed ``(provider, message_id)`` without completing
        it — the provider-retry-after-our-failure case §D-S19-replay
        names: a subsequent ``claim()`` for the same id is then accepted
        again (``ACQUIRED``), because ``release()`` removes the
        reservation entirely rather than leaving it ``IN_FLIGHT`` until
        the TTL elapses.

        Args:
            provider:   The verifier's provider name.
            message_id: The provider's dedup key for this delivery.

        Async safety: ✅ Idempotent — safe to call more than once
            (delegates to the store's own idempotent ``release()``).
        """
        key = _storage_key(provider, message_id)
        await self.store.release(key)
