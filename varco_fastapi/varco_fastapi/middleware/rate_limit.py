"""
varco_fastapi.middleware.rate_limit
====================================
``RateLimitMiddleware`` — Plan 035 / Phase 5, Step 22 (S10).

The ASGI assembly around ``varco_core.resilience.rate_limit.RateLimiter`` —
**no new algorithm, no new ABC method, no re-implementation of limiting
logic** (§Non-goals). This module imports ``RateLimiter``/
``RateLimitConfig`` from ``varco_core`` as-is and wires HTTP semantics
(keying by scope, ``Retry-After``, fail-open, the optional draft
``RateLimit-Policy`` header) on top.

One class, registered at up to **two** stack positions by
``create_varco_app(rate_limit=RateLimitBundle(...))`` — §D-order's DESIGN
block ("one RateLimitMiddleware class, two positions") explains why a
single position cannot satisfy both an early, IP-keyed rejection (before
auth) and a subject/tenant-keyed one (after auth, where
``get_auth_context_or_none()``/``current_tenant()`` are populated).

Key construction (§D-S10-shape) deliberately mirrors
``IdempotencyMiddleware._scoped_key`` rather than inventing a second
convention:

    GLOBAL  -> "ratelimit:global:{name}"
    IP      -> "ratelimit:ip:{name}:{client_ip}"        (unkeyable -> skip)
    SUBJECT -> "ratelimit:subject:{name}:{user_id}"      (anonymous -> skip)
    TENANT  -> "ratelimit:tenant:{name}:{tenant_id}"     (no tenant -> skip)

DESIGN: skip an unkeyable rule rather than share a placeholder bucket
    (§D-S10-shape)
    ✅ A shared ``"-"`` bucket for every anonymous/untenanted caller is
       worse than no limit at all — one client can trivially exhaust it for
       everyone else, turning a protection into an amplification.
    ❌ An unkeyable request is unlimited by *that* rule. Mitigated by the
       always-keyable ``IP``/``GLOBAL`` rule, and by
       ``inspect_http_edge()`` flagging an all-``POST_AUTH`` configuration.

DESIGN: the client IP is the TCP peer unless a trusted proxy says otherwise
    (§D-S10-ip)
    ✅ Brief 008 §3's named bypass — trusting ``X-Forwarded-For`` blindly
       lets an attacker mint a fresh bucket per request. Consulted only
       when the immediate peer matches ``trusted_proxies`` (default empty).

DESIGN: an IP/SUBJECT-scoped ``InMemoryRateLimiter`` is refused unless
    acknowledged (§D-S10-keyspace)
    ✅ ``InMemoryRateLimiter``'s own DESIGN block warns its per-key
       lock/window dicts "grow unboundedly" with "callers should use
       bounded key spaces" — an IP or subject key is attacker-controlled.
       Refusing at construction (unless
       ``acknowledge_unbounded_keyspace=True``) turns a production
       memory-exhaustion primitive into a startup-time ``ValueError``.
       ``RedisRateLimiter`` is unaffected — its sorted sets carry a TTL.

DESIGN: fail **open** on limiter error, loudly, by default (§D-S10-failopen)
    ✅ ``RedisRateLimiter``'s own docstring hands this decision to the
       caller — this middleware is that caller. A rate limiter is a
       protection against abuse, not a correctness invariant: losing it
       for a Redis outage's duration is a smaller incident than a total
       outage. ``fail_open=False`` inverts it to 503 for deployments that
       want the opposite; every failure is logged at ERROR either way.

DESIGN: ``Retry-After`` always; draft ``RateLimit-Policy`` behind a flag;
    no ``RateLimit``/``X-RateLimit-*``, ever (§D-S10-headers)
    ✅ ``429``/``Retry-After`` are RFC 6585 / RFC 9110 — stable.
       ``RateLimit-Policy`` (draft-11) is computable from
       ``RateLimitConfig`` alone but is offered only behind
       ``emit_draft_headers=True`` because brief 008's §Version notes say
       the draft "expires 24 November 2026" and "may still undergo
       significant changes" — a framework should not ship a
       soon-to-change header on by default.
    ✅ ``RateLimit``/``X-RateLimit-Remaining`` are never emitted, under any
       setting: the ``RateLimiter`` ABC has no ``remaining()`` method, and
       adding one would break every out-of-tree implementation (the same
       rule that kept ``BulkCache`` off ``AsyncCache``, Plan 011 / D-11). A
       header that lies about remaining quota is worse than an absent one.

DESIGN: constructs its own 429/503 response directly, never raises through
    ``ErrorMiddleware``
    ✅ A 429 needs ``Retry-After`` **and**, optionally, ``RateLimit-Policy``
       — headers no generic ``ServiceException`` dispatch path knows how
       to attach. Sending the response directly means this middleware
       behaves identically whether or not ``ErrorMiddleware`` is present
       in the stack (the same self-contained shape the
       ``enable_error_middleware=False`` edge case requires of both new
       middlewares) — one code path, not two.
    ✅ ``correlation_id`` is still populated (ambient if set, freshly
       generated otherwise — the same fallback ``ErrorMiddleware`` uses),
       so the response is no less correlatable for not going through the
       envelope machinery.

Thread safety:  ✅ Stateless per request — ``RateLimiter`` instances own
                their own concurrency safety; this middleware holds no
                mutable per-request state beyond a local variable.
Async safety:   ✅ Pure ``async def``; the only mutable *instance* state is
                a best-effort last-warned timestamp dict for log
                throttling, never awaited across.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic_settings import SettingsConfigDict
from varco_core.config import VarcoSettings
from varco_core.resilience.rate_limit import InMemoryRateLimiter, RateLimiter
from varco_core.service.tenant import current_tenant

from varco_fastapi.middleware._forwarded import peer_is_trusted_proxy, resolve_forwarded_for
from varco_fastapi.middleware._json_response import send_json_error

if TYPE_CHECKING:
    from collections.abc import Callable

    from starlette.types import Receive, Scope, Send

_logger = logging.getLogger(__name__)

__all__ = [
    "RateLimitBundle",
    "RateLimitMiddleware",
    "RateLimitRule",
    "RateLimitScope",
    "RateLimitSettings",
    "RateLimitStage",
]


class RateLimitScope(StrEnum):
    """Which identity a ``RateLimitRule``'s budget is keyed by."""

    #: Keyed by the resolved client IP. Legal in either stage.
    IP = "ip"
    #: A single, unkeyed, process/deployment-wide budget. Legal in either stage.
    GLOBAL = "global"
    #: Keyed by ``get_auth_context_or_none().user_id``. ``POST_AUTH`` only.
    SUBJECT = "subject"
    #: Keyed by ``current_tenant()``. ``POST_AUTH`` only.
    TENANT = "tenant"


class RateLimitStage(StrEnum):
    """Which position in §D-order a ``RateLimitMiddleware`` instance occupies."""

    #: Outside ``RequestContextMiddleware`` — before auth. Only IP/GLOBAL rules legal.
    PRE_AUTH = "pre_auth"
    #: Inside ``RequestContextMiddleware`` — after auth. SUBJECT/TENANT rules legal too.
    POST_AUTH = "post_auth"


#: Scopes that require an authenticated/tenant-scoped request context — refused
#: at PRE_AUTH construction time (§D-S10-shape).
_POST_AUTH_ONLY_SCOPES = frozenset({RateLimitScope.SUBJECT, RateLimitScope.TENANT})

#: Scopes whose key is caller-influenced — an attacker-controlled keyspace
#: makes an InMemoryRateLimiter a memory-exhaustion primitive (§D-S10-keyspace).
_UNBOUNDED_KEYSPACE_SCOPES = frozenset({RateLimitScope.IP, RateLimitScope.SUBJECT})


@dataclass(frozen=True)
class RateLimitRule:
    """
    One rate-limit rule: a scope, the limiter enforcing it, and a name.

    Attributes:
        scope:   Which identity the budget is keyed by.
        limiter: The ``RateLimiter`` backend enforcing this rule's budget —
                 imported from ``varco_core.resilience.rate_limit`` /
                 ``varco_redis.rate_limit`` as-is, never re-implemented.
        name:    Appears in the rule's rate-limit key and in
                 ``RateLimit-Policy`` (when emitted) and log lines.
                 Default ``""``.
    """

    scope: RateLimitScope
    limiter: RateLimiter
    name: str = ""


@dataclass(frozen=True)
class RateLimitBundle:
    """
    A set of ``RateLimitRule``s plus shared settings, passed to
    ``create_varco_app(rate_limit=...)``.

    ``create_varco_app`` partitions ``rules`` by scope — ``IP``/``GLOBAL``
    rules go into a ``RateLimitMiddleware(stage=PRE_AUTH)`` at §D-order
    position 9; ``SUBJECT``/``TENANT`` rules go into a second instance at
    position 11 — and registers only the instances a non-empty partition
    needs (§Open question 2: this type lives beside the middleware, not in
    ``varco_core.resilience``, because ``RateLimitScope`` only means
    anything over HTTP — the same layer reasoning as the settings classes).

    Attributes:
        rules:    Every rule across both stages — ``create_varco_app`` does
                  the PRE_AUTH/POST_AUTH partition automatically.
        settings: Shared ``RateLimitSettings``, or ``None`` to read from
                  the environment.
        acknowledge_unbounded_keyspace: Forwarded verbatim to both
                  ``RateLimitMiddleware`` registrations ``create_varco_app``
                  builds from this bundle. Default ``False`` — §D-S10-keyspace
                  requires the caller to opt in explicitly for an
                  ``IP``/``SUBJECT``-scoped rule backed by an
                  ``InMemoryRateLimiter``; ``create_varco_app`` must never
                  forge this on the caller's behalf, or the guard becomes
                  unreachable for the documented, recommended API.
    """

    rules: tuple[RateLimitRule, ...]
    settings: RateLimitSettings | None = None
    acknowledge_unbounded_keyspace: bool = False


class RateLimitSettings(VarcoSettings):
    """
    Configuration for ``RateLimitMiddleware``, loaded from environment
    variables under the ``VARCO_RATE_LIMIT_`` prefix.

    Attributes:
        enabled:            Whether the middleware enforces anything at all.
                            Default ``True`` — but note ``rate_limit=None``
                            is ``create_varco_app``'s actual opt-in gate
                            (§D-S10 is the one row that ships off by
                            default); this flag matters for a
                            hand-registered instance.
        fail_open:          On a limiter exception, allow the request
                            (``True``, default) or return 503
                            (``False``). §D-S10-failopen.
        trusted_proxies:    CIDRs whose ``X-Forwarded-For`` is trusted for
                            the ``IP`` scope. Default empty — the header is
                            ignored with no configuration (§D-S10-ip).
        trusted_proxy_hops: Number of trusted proxy hops to skip from the
                            right when resolving ``X-Forwarded-For``.
                            Default ``0``.
        emit_draft_headers: Emit the draft-11 ``RateLimit-Policy`` header.
                            Default ``False`` (§D-S10-headers).
        exempt_paths:       Request path prefixes exempt from every rule.
                            Default empty.
        error_log_interval: Minimum seconds between repeated
                            "limiter raised" ERROR log lines for the same
                            rule, and between repeated "unkeyable rule"
                            WARNING log lines. Default ``60.0``.

    Thread safety:  ✅ Frozen pydantic model.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_RATE_LIMIT_", frozen=True)

    enabled: bool = True
    fail_open: bool = True
    trusted_proxies: tuple[str, ...] = ()
    trusted_proxy_hops: int = 0
    emit_draft_headers: bool = False
    exempt_paths: tuple[str, ...] = ()
    error_log_interval: float = 60.0


class RateLimitMiddleware:
    """
    Pure-ASGI middleware enforcing one or more ``RateLimitRule``s.

    Args:
        app:      The wrapped ASGI application.
        rules:    Rules to enforce, checked in order — the first denial
                  short-circuits and sends a 429.
        stage:    ``PRE_AUTH`` or ``POST_AUTH`` — see ``RateLimitStage``.
                  Determines which scopes are legal (§D-S10-shape).
        settings: ``RateLimitSettings`` instance. Defaults to reading from
                  the environment.
        acknowledge_unbounded_keyspace: Required ``True`` to construct an
                  ``IP``/``SUBJECT``-scoped rule backed by an
                  ``InMemoryRateLimiter`` (§D-S10-keyspace).

    Raises:
        ValueError: A ``SUBJECT``/``TENANT`` rule is used with
            ``stage=PRE_AUTH`` (naming the offending scope), or an
            ``IP``/``SUBJECT`` rule uses an ``InMemoryRateLimiter`` without
            ``acknowledge_unbounded_keyspace=True`` (naming the kwarg).

    Thread safety:  ✅ Stateless across requests beyond the log-throttle dict.
    Async safety:   ✅ Pure ``async def __call__``.
    """

    def __init__(
        self,
        app: Any,
        *,
        rules: tuple[RateLimitRule, ...],
        stage: RateLimitStage,
        settings: RateLimitSettings | None = None,
        acknowledge_unbounded_keyspace: bool = False,
    ) -> None:
        for rule in rules:
            if stage is RateLimitStage.PRE_AUTH and rule.scope in _POST_AUTH_ONLY_SCOPES:
                raise ValueError(
                    f"RateLimitRule(scope={rule.scope.value.upper()!s}) requires "
                    f"stage=POST_AUTH — {rule.scope.name} needs an authenticated "
                    "request context that is not yet populated at PRE_AUTH "
                    "(before RequestContextMiddleware runs)."
                )
            if (
                rule.scope in _UNBOUNDED_KEYSPACE_SCOPES
                and isinstance(rule.limiter, InMemoryRateLimiter)
                and not acknowledge_unbounded_keyspace
            ):
                raise ValueError(
                    f"RateLimitRule(scope={rule.scope.value.upper()!s}) with an "
                    "InMemoryRateLimiter has an attacker-controlled key space — "
                    "each new key permanently costs a deque + asyncio.Lock "
                    "(rate_limit.py's own DESIGN block). Pass "
                    "acknowledge_unbounded_keyspace=True to accept this for a "
                    "single-process deployment, or use RedisRateLimiter (its "
                    "sorted sets carry a TTL)."
                )

        self.app = app
        self._rules = rules
        self._stage = stage
        self._settings = settings or RateLimitSettings()
        self._last_logged: dict[str, float] = {}

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        settings = self._settings
        if scope["type"] != "http" or not settings.enabled or not self._rules:
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if any(path.startswith(prefix) for prefix in settings.exempt_paths):
            await self.app(scope, receive, send)
            return

        for rule in self._rules:
            key = self._key_for(rule, scope)
            if key is None:
                continue

            try:
                allowed = await rule.limiter.acquire(key)
            except Exception as exc:  # noqa: BLE001 — a backend failure, not our bug
                if settings.fail_open:
                    self._log_throttled(
                        f"error:{rule.name}",
                        _logger.error,
                        "RateLimitMiddleware: limiter raised for rule %r (%s) — "
                        "failing open (allowing the request). Pair a remote "
                        "limiter with @circuit_breaker if this recurs.",
                        rule.name,
                        exc,
                    )
                    continue
                await send_json_error(
                    scope,
                    receive,
                    send,
                    status_code=503,
                    code="RATE_LIMITER_UNAVAILABLE",
                    message="The rate limiter is unavailable and fail_open=False.",
                )
                return

            if not allowed:
                retry_after_seconds = await rule.limiter.retry_after(key)
                await self._send_rate_limited(rule, retry_after_seconds, scope, receive, send)
                return

        await self.app(scope, receive, send)

    # ── Keying (§D-S10-shape) ────────────────────────────────────────────────

    def _key_for(self, rule: RateLimitRule, scope: Scope) -> str | None:
        """Resolve this rule's rate-limit key, or ``None`` to skip an unkeyable rule."""
        if rule.scope is RateLimitScope.GLOBAL:
            return f"ratelimit:global:{rule.name}"

        if rule.scope is RateLimitScope.IP:
            client_ip = self._client_ip(scope)
            if not client_ip:
                self._log_throttled(
                    f"ip-unkeyable:{rule.name}",
                    _logger.warning,
                    "RateLimitMiddleware: no resolvable client IP for IP-scoped "
                    "rule %r — skipping this rule for this request.",
                    rule.name,
                )
                return None
            return f"ratelimit:ip:{rule.name}:{client_ip}"

        if rule.scope is RateLimitScope.SUBJECT:
            # Anonymous is the overwhelmingly common case for a public
            # endpoint — deliberately NOT logged (§D-S10-shape: this is
            # covered by the IP rule, not a misconfiguration).
            from varco_fastapi.context import get_auth_context_or_none

            ctx = get_auth_context_or_none()
            if ctx is None or ctx.user_id is None:
                return None
            return f"ratelimit:subject:{rule.name}:{ctx.user_id}"

        if rule.scope is RateLimitScope.TENANT:
            tenant = current_tenant()
            if tenant is None:
                self._log_throttled(
                    f"tenant-unkeyable:{rule.name}",
                    _logger.warning,
                    "RateLimitMiddleware: no ambient tenant for TENANT-scoped "
                    "rule %r — skipping this rule for this request.",
                    rule.name,
                )
                return None
            return f"ratelimit:tenant:{rule.name}:{tenant}"

        raise AssertionError(f"unreachable RateLimitScope: {rule.scope!r}")  # pragma: no cover

    def _client_ip(self, scope: Scope) -> str | None:
        """Resolve the client IP per §D-S10-ip's trust rule (shared with security_headers)."""
        client = scope.get("client")
        peer = client[0] if client else None
        if peer and peer_is_trusted_proxy(peer, self._settings.trusted_proxies):
            headers = dict(scope.get("headers") or [])
            raw = headers.get(b"x-forwarded-for")
            if raw is not None:
                return resolve_forwarded_for(
                    raw.decode("latin-1"), self._settings.trusted_proxy_hops
                )
        return peer

    # ── Response construction (§D-S10-headers) ──────────────────────────────

    async def _send_rate_limited(
        self,
        rule: RateLimitRule,
        retry_after_seconds: float,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        retry_after_int = max(1, math.ceil(retry_after_seconds))
        headers = {"Retry-After": str(retry_after_int)}
        if self._settings.emit_draft_headers:
            policy = self._policy_header_value(rule)
            if policy is not None:
                headers["RateLimit-Policy"] = policy
        await send_json_error(
            scope,
            receive,
            send,
            status_code=429,
            code="RATE_LIMIT_EXCEEDED",
            message=(
                f"Rate limit exceeded for rule {rule.name or rule.scope.value!r}. "
                f"Retry after {retry_after_int} second(s)."
            ),
            headers=headers,
        )

    @staticmethod
    def _policy_header_value(rule: RateLimitRule) -> str | None:
        """
        Render the draft-11 ``RateLimit-Policy`` structured-field value.

        Computable purely from ``RateLimitConfig.rate``/``.period`` — no
        ``remaining()`` needed (§D-S10-headers). Returns ``None`` if the
        limiter exposes no ``.config`` (an out-of-tree ``RateLimiter``
        implementation is not required to).
        """
        config = getattr(rule.limiter, "config", None)
        if config is None:
            return None
        name = rule.name or rule.scope.value
        return f'"{name}"; q={config.rate}; w={int(config.period)}'

    def _log_throttled(
        self,
        throttle_key: str,
        log_fn: Callable[..., None],
        message: str,
        *args: Any,
    ) -> None:
        """Log at most once per ``error_log_interval`` seconds per ``throttle_key``."""
        now = time.monotonic()
        last = self._last_logged.get(throttle_key, 0.0)
        if now - last >= self._settings.error_log_interval:
            self._last_logged[throttle_key] = now
            log_fn(message, *args)
