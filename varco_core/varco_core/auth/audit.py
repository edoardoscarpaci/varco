"""
varco_core.auth.audit
========================
``AuditingAuthorizer`` — Plan 036 (S11)'s authorization-decision audit
(§D-S11-shape, §D-S11-payload, §D-S11-policy).

A decorator over whatever `AbstractAuthorizer` an app has bound — never an
HTTP middleware. `authorize()` is called from the service layer at eleven
verified call sites (`service/base.py`, `service/bulk.py`,
`service/soft_delete.py`) and never from HTTP middleware, so a middleware
would silently miss every authorization decision made by a job runner, an
event consumer, or a CLI verb. Wrapping the authorizer itself covers all of
them with one object.

§D-036-oq1 (this plan's Open Question 1, resolved at implementation time —
see the plan's "Open questions" section for the full argument): the
recorded decision is emitted as a **distinct** `Event` subclass,
`AuthorizationDecisionEvent`, carrying one generic `payload: dict[str, Any]`
field rather than reusing `AuditEntry`'s typed shape (`entity_type`,
`entity_id`, `diff`, ...) — an authorization decision has no `diff`, and
needs `decision`/`denial_type`/`is_collection`, none of which fit
`AuditEntry` without widening it for every mutation-audit consumer too. No
`AuditRepository`/`AuditConsumer` wiring is built here — the wrapper's
promise is exactly "produced via `AbstractEventProducer`"; persisting the
event onto durable storage is the same story every other `AbstractEventBus`
publish already tells (a consumer subscribes to the channel), and is left
to application wiring or a follow-up plan.

Thread safety:  ✅ Stateless — no instance state beyond the delegate/producer
                   references, both expected to be shared singletons.
Async safety:   ✅ `authorize()` is `async def`; `_record()` awaits the
                   producer directly, no fire-and-forget.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any

from varco_core.auth.base import AbstractAuthorizer
from varco_core.event.base import Event

if TYPE_CHECKING:
    from varco_core.auth.base import Action, AuthContext, Resource
    from varco_core.event.producer import AbstractEventProducer

__all__ = ["AuditDecisionPolicy", "AuthorizationDecisionEvent", "AuditingAuthorizer"]


class AuditDecisionPolicy(StrEnum):
    """
    §D-S11-policy: which decisions `AuditingAuthorizer` actually records.

    An authorization decision happens on every service call, several times
    per request — an unconditional synchronous audit write would be the
    single largest cost in the release. The default balances that against
    brief 006 §3's lesson: impersonation must be logged at issuance and at
    use, and silent actor tokens are a critical risk (CVE-2025-55241).
    """

    DENIALS = "denials"
    """Every deny, **plus** every allow whose `ctx` carries an actor
    (`ctx.metadata["actor"]`) — the default. A pure-DENIALS deployment
    cannot answer "who read this record" from the authz trail alone;
    `AuditLogMixin` is the answer for mutations, `ALL` for reads."""

    ALL = "all"
    """Every decision, allow or deny. For a regulated deployment that must
    evidence every access."""

    NONE = "none"
    """Nothing is recorded, but the wrapper stays installed — flipping the
    policy later needs no re-wiring of DI."""


class AuthorizationDecisionEvent(Event):  # type: ignore[misc]
    """
    The event `AuditingAuthorizer` produces (§D-036-oq1).

    A single `payload` field (rather than typed columns) keeps this class
    a thin, generic carrier — the field allowlist and its construction
    live entirely in `AuditingAuthorizer._build_payload`, in one place,
    per §D-S11-payload.

    Attributes:
        payload: Exactly the fields listed in §D-S11-payload's "Recorded"
            table — never `vars(exc)`, never a denial reason, never
            `resource.entity`, never `ctx.grants`/`ctx.roles`/the raw
            token, never `ctx.metadata` wholesale.
    """

    __event_type__ = "varco.authz.decision"

    payload: dict[str, Any]


class AuditingAuthorizer(AbstractAuthorizer):
    """
    Wrap `delegate`, call it, and record the outcome via `producer`
    (§D-S11-shape).

    Args:
        delegate: The real `AbstractAuthorizer` to enforce. Its behaviour
            is byte-identical through this wrapper — the original
            exception is re-raised unchanged on denial.
        producer: An `AbstractEventProducer` — **never**
            `AbstractEventBus` (the standing house rule, same shape
            `AuditLogMixin` already uses).
        policy: Which decisions to record (§D-S11-policy). Default
            `AuditDecisionPolicy.DENIALS`.
        channel: The channel `_produce()` is called with. Default
            `"varco.audit"` — the same convention `AuditEvent` uses, so an
            app that already routes that channel to a persistence consumer
            picks this event up for free.

    Edge cases:
        - Wrapping an already-wrapped `AuditingAuthorizer` would double
          record. `enable_authorization_audit()` (the DI-wiring entry
          point) is responsible for detecting this — this class itself
          does not self-inspect `delegate`'s type, to avoid an import
          cycle with its own module.
        - A collection-level `Resource` (`entity=None`) records
          `entity_pk=None`, `is_collection=True`.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ `authorize()` is `async def`.
    """

    def __init__(
        self,
        delegate: AbstractAuthorizer,
        producer: AbstractEventProducer,
        *,
        policy: AuditDecisionPolicy = AuditDecisionPolicy.DENIALS,
        channel: str = "varco.audit",
    ) -> None:
        self._delegate = delegate
        self._producer = producer
        self._policy = policy
        self._channel = channel

    async def authorize(self, ctx: AuthContext, action: Action, resource: Resource) -> None:
        """
        Delegate, then record — denial re-raised untouched either way.

        Raises:
            Exception: Whatever `delegate.authorize()` raises on denial,
                completely unmodified (the same instance, same traceback
                context) — recording never alters denial behaviour.
        """
        try:
            await self._delegate.authorize(ctx, action, resource)
        except Exception as exc:
            if self._policy is not AuditDecisionPolicy.NONE:
                await self._record(ctx, action, resource, allowed=False, exc=exc)
            raise
        if self._should_record_allow(ctx):
            await self._record(ctx, action, resource, allowed=True)

    def _should_record_allow(self, ctx: AuthContext) -> bool:
        if self._policy is AuditDecisionPolicy.ALL:
            return True
        if self._policy is AuditDecisionPolicy.NONE:
            return False
        # DENIALS: an allow is recorded only when delegated (an actor is
        # present) — the CVE-2025-55241 property (brief 006 §3).
        return _actor_id(ctx) is not None

    async def _record(
        self,
        ctx: AuthContext,
        action: Action,
        resource: Resource,
        *,
        allowed: bool,
        exc: Exception | None = None,
    ) -> None:
        payload = _build_payload(ctx, action, resource, allowed=allowed, exc=exc)
        event = AuthorizationDecisionEvent(payload=payload)
        await self._producer._produce(event, channel=self._channel)


def _actor_id(ctx: AuthContext) -> str | None:
    """The single `metadata["actor"]` key — never `ctx.metadata` wholesale
    (§D-S11-payload: an app-controlled dict of unknown contents)."""
    value = ctx.metadata.get("actor")
    return value if isinstance(value, str) else None


def _build_payload(
    ctx: AuthContext,
    action: Action,
    resource: Resource,
    *,
    allowed: bool,
    exc: Exception | None,
) -> dict[str, Any]:
    """
    Build the recorded payload strictly per §D-S11-payload's allowlist.

    **Never** includes: the denial reason / `str(exc)`, `resource.entity`
    itself, `ctx.grants`/`ctx.roles`/the raw token, `ctx.metadata`
    wholesale, or `vars(exc)` in any form — see the module docstring's
    Phase-4-test cross-reference (a field ALLOWLIST test, not a denylist
    one, so a future field addition must be deliberately argued in).
    """
    from varco_core.service.tenant import current_tenant
    from varco_core.tracing import current_correlation_id

    payload: dict[str, Any] = {
        "principal_id": ctx.user_id,
        "actor_id": _actor_id(ctx),
        "tenant_id": current_tenant(),
        "action": str(action),
        "entity_type": resource.entity_type.__name__,
        "entity_pk": (None if resource.is_collection else getattr(resource.entity, "pk", None)),
        "is_collection": resource.is_collection,
        "decision": "allow" if allowed else "deny",
        "correlation_id": current_correlation_id(),
    }
    if not allowed:
        payload["denial_type"] = type(exc).__name__ if exc is not None else None
    return payload
