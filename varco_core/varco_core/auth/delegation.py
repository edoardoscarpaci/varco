"""
varco_core.auth.delegation
============================
RFC 8693 ``act`` claim consumption — ``ActorContext``, ``DelegationPolicy``,
``AllowlistDelegationPolicy``, ``DelegationRecord`` (Plan 033 / S16,
§D-S16-shape — DROPPABLE per §D-S16-cut).

varco **consumes** an already-issued, already-verified delegation/actor
token — it never issues one (brief 006 §3: token exchange is the IdP's
job). ``act`` is already parsed into ``AuthContext.metadata["actor"]``
(``varco_core.jwt.parser``); this module adds policy and mandatory audit
on top of that already-parsed claim.

DESIGN: mandatory audit at the point of use, motivated by CVE-2025-55241
    ✅ brief 006 §3/§6: Entra's actor tokens were issued "with no logs; no
       audit trail of who asked to impersonate whom", and that was the
       escalation vector. "Impersonation must be logged at issuance AND
       use." varco does not issue, so **use** is the half varco owns, and
       it is not optional — every ``ActAsTenantSource`` decision (allow
       *and* deny) emits a ``DelegationRecord``, logged at INFO with both
       principal and actor.
    ✅ Delegation (``sub`` = principal, ``act`` = service) is preferred over
       impersonation (``sub`` replaced, no ``act``) per brief 006 §3 —
       ``ActorContext.from_metadata`` returns ``None`` when there is no
       ``act``, so a bare impersonation token can never take this path.
    ✅ ``AllowlistDelegationPolicy`` supports per-tenant scoping and an
       explicit ``"*"``, which must be *written* — brief 006 §3's
       "scope it".
    ❌ Logging at INFO on every delegated request is volume. Accepted — a
       delegated request is by construction rare, and a silent one is the
       CVE.
    Rejected — support impersonation (``sub`` swapped, no ``act``): ❌ brief
    006 §3 marks it "⚠️ Risky; rarely used for tenant scoping" and it is
    indistinguishable from the principal in an audit log — the CVE's shape.
    Rejected — an ``allow_all`` default on ``AllowlistDelegationPolicy``: ❌
    unrestricted delegation, explicitly forbidden by brief 006 §3.

Thread safety:  ✅ All value objects are frozen; ``AllowlistDelegationPolicy``
    holds only immutable configuration.
Async safety:   ✅ ``DelegationPolicy.allows()`` is ``async`` for
    ABC-compatibility with an out-of-tree, I/O-backed policy (e.g. a
    database-stored grant table); the shipped ``AllowlistDelegationPolicy``
    does no I/O.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

__all__ = [
    "ActorContext",
    "DelegationPolicy",
    "AllowlistDelegationPolicy",
    "DelegationRecord",
]


@dataclass(frozen=True)
class ActorContext:
    """
    The verified ``act`` claim, already parsed by ``varco_core.jwt.parser``
    into ``AuthContext.metadata["actor"]``.

    Args:
        subject: ``act.sub`` — WHO is acting on behalf of the token's
            principal.
        chain: Nested ``act.act.sub``... values, outermost first (RFC 8693
            §4.1). Empty when there is no further nesting.
    """

    subject: str
    chain: tuple[str, ...]

    @classmethod
    def from_metadata(cls, metadata: Mapping[str, Any]) -> ActorContext | None:
        """
        Build an ``ActorContext`` from ``AuthContext.metadata``.

        Args:
            metadata: The already-verified token's metadata mapping.

        Returns:
            An ``ActorContext``, or ``None`` when there is no ``act`` claim
            at all, or it is malformed (not a dict, or missing a string
            ``sub``) — **never raises**, so a bare impersonation token (no
            ``act``) or a garbled one can never take this path.
        """
        raw_actor = metadata.get("actor")
        if not isinstance(raw_actor, dict):
            return None
        sub = raw_actor.get("sub")
        if not isinstance(sub, str):
            return None

        chain_list = [sub]
        node = raw_actor.get("act")
        while isinstance(node, dict):
            nested_sub = node.get("sub")
            if not isinstance(nested_sub, str):
                break
            chain_list.append(nested_sub)
            node = node.get("act")

        # A single-element chain means "no further nesting" — represented
        # as an empty tuple, since `subject` alone already carries that case.
        chain = tuple(chain_list) if len(chain_list) > 1 else ()
        return cls(subject=sub, chain=chain)


class DelegationPolicy(ABC):
    """
    Decides whether ``actor`` may act as ``tenant_id`` on behalf of
    ``principal``.

    Edge cases:
        - No policy bound at all ⇒ no claim, ever (deny-by-default at the
          ``ActAsTenantSource`` level, not here — this ABC is only reached
          when a policy IS bound).
    """

    name: ClassVar[str]

    @abstractmethod
    async def allows(self, actor: ActorContext, principal: str | None, tenant_id: str) -> bool:
        """
        Args:
            actor: The delegating service/agent.
            principal: The token's own subject (``sub``), or ``None``.
            tenant_id: The tenant the caller is requesting to act as.

        Returns:
            ``True`` when delegation is permitted.
        """
        raise NotImplementedError


class AllowlistDelegationPolicy(DelegationPolicy):
    """
    Deny-by-default, explicit ``actor -> tenants`` allowlist.

    Args:
        grants: ``{actor_subject: frozenset(tenant_ids) | "*"}``. An actor
            absent from this mapping, or an empty mapping, denies
            everything. ``"*"`` must be written explicitly — never a
            default.
    """

    name: ClassVar[str] = "allowlist"

    def __init__(self, grants: Mapping[str, frozenset[str] | Literal["*"]]) -> None:
        self.grants = grants

    async def allows(self, actor: ActorContext, principal: str | None, tenant_id: str) -> bool:
        allowed = self.grants.get(actor.subject)
        if allowed is None:
            return False
        if allowed == "*":
            return True
        return tenant_id in allowed


@dataclass(frozen=True)
class DelegationRecord:
    """
    An audited delegation decision — attached to
    ``TenantProvenance.delegation`` and emitted at INFO regardless of
    ``allowed`` (§D-S16-shape: "unlogged-is-impossible").

    Args:
        actor: The delegating service/agent's subject.
        actor_chain: The full nested chain, outermost first.
        principal: The token's own subject, or ``None``.
        tenant_id: The tenant that was requested.
        allowed: Whether the policy permitted it.
        policy: The deciding ``DelegationPolicy.name``.
    """

    actor: str
    actor_chain: tuple[str, ...]
    principal: str | None
    tenant_id: str
    allowed: bool
    policy: str
