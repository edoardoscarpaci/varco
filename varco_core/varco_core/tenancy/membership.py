"""
varco_core.tenancy.membership
==============================
Tenant↔subject membership binding (Plan 033 / S5, §D-S5-claim) — a signed
claim list, no external lookup, fail-open in 3.2.

DESIGN: a Null Object DI default plus an ``enable_*`` opt-in, fail-open in 3.2
    ✅ Exactly the shape CLAUDE.md already records twice —
       ``NullFeatureFlags`` scanned + ``enable_feature_flags()``, and the
       ``enable_policy_authorizer`` rule that an opt-in must **not** be a
       scanned ``@Configuration`` (scan auto-activates those and would
       silently shadow an app's own binding).
    ✅ Fail-open-with-a-warning is the locked blast-radius rule applied
       honestly: adding a ``tenants`` claim to every token in a fleet is
       real application work at the IdP, so it warns in 3.2 and flips in
       4.0 (``BACKLOG.md:54``).
    ✅ The self-membership branch means a large class of existing
       single-tenant deployments become *correctly* enforced the moment
       they opt in, with no IdP change at all.
    ✅ Trust stays anchored in the signature — no lookup, no per-request
       query, per the locked membership decision (``BACKLOG.md:57``).
    ❌ ``ALLOW``-on-missing means an opted-in app whose IdP silently stops
       emitting ``tenants`` degrades to no enforcement. Mitigated: the
       WARNING is emitted once per process **and** the decision's ``reason``
       is ``"claim_absent"``, which lands in ``TenantProvenance.membership``
       and therefore in 036's S11 audit.
    Rejected — ``ClaimTenantMembership`` as the scanned default: ❌ a
    scanned default that denies is a fleet-wide 403 on upgrade; a scanned
    default that allows is indistinguishable from ``NullTenantMembership``
    while being harder to reason about.
    Rejected — ``on_missing_claim=DENY`` in 3.2: ❌ needs every token in a
    fleet reissued; textbook "real application work" under the locked rule.
    Rejected — a ``tenants`` field on ``AuthContext``: ❌ a frozen,
    api-surface-tracked dataclass with many out-of-tree constructors;
    ``metadata`` is the documented home for extra claims.

Thread safety:  ✅ Both shipped implementations are stateless except for a
    module-level "warned once" flag guarded by no lock — a benign race would
    at worst emit the warning twice, never corrupt state.
Async safety:   ✅ ``check()`` is ``async`` for ABC-compatibility with a
    future I/O-backed resolver (``BACKLOG.md:97``); the shipped
    implementations do no I/O.
"""

from __future__ import annotations

import logging
import os
import sys
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar

from providify import Singleton

from varco_core.exception import ServiceException

if TYPE_CHECKING:
    from varco_core.auth.base import AuthContext

logger = logging.getLogger(__name__)

__all__ = [
    "MembershipDecision",
    "AbstractTenantMembership",
    "NullTenantMembership",
    "ClaimTenantMembership",
    "MissingClaimPolicy",
    "TenantMembershipError",
    "TenantMembershipSettings",
]

_LEGAL_ON_MISSING = ("allow", "deny")

_DEFAULT_MEMBERSHIP_CLAIM_KEY = "tenants"

# Module-level "warned once per process" flag for the missing-claim WARNING
# (§D-S5-claim). Deliberately not per-instance: the point is one operator-
# visible signal per process lifetime, not once per ClaimTenantMembership().
_missing_claim_warned = False


class MissingClaimPolicy(StrEnum):
    """What ``ClaimTenantMembership`` does when the membership claim is absent."""

    ALLOW = "allow"  # 3.2 default — allow + one process-level WARNING
    DENY = "deny"  # 4.0 default


@dataclass(frozen=True)
class TenantMembershipSettings:
    """
    Env-driven configuration for ``enable_tenant_membership()`` (Plan 033 /
    S5, §D-S5-claim, Decision 4).

    DESIGN: a plain frozen dataclass with a hand-written ``from_env()``,
    living in this module rather than ``tenancy/settings.py``
    ✅ ``ClaimTenantMembership``/``MissingClaimPolicy`` — the only things
       this settings object configures — are already defined here;
       colocating avoids a new inter-module import (``settings.py`` would
       otherwise need to import ``MissingClaimPolicy`` from this module,
       or this module would need to import a type from ``settings.py``
       purely to hand it back to ``di.py``).
    ✅ ``varco_core.tenancy.di`` (the only caller) already imports from
       ``membership.py`` — this adds no new import there.
    ✅ Matches the house shape (`TenancySettings`, `TenantProvenanceSettings`)
       — plain frozen dataclass, hand-written ``from_env()``, no pydantic.

    Args:
        claim_key: The ``AuthContext.metadata`` key holding the membership
            list. Env: ``VARCO_TENANT_MEMBERSHIP_CLAIM``. Defaults to
            ``"tenants"``.
        on_missing_claim: ``MissingClaimPolicy``. Env:
            ``VARCO_TENANT_MEMBERSHIP_ON_MISSING``. Defaults to ``ALLOW``
            in 3.2 (flips to ``DENY`` in 4.0, per the 4.0 flip list).

    Edge cases:
        - ``VARCO_TENANT_MEMBERSHIP`` itself (the ``null``/``claim``
          selector in the README's env-var table) is deliberately **not**
          read here — that var decides *whether* an app calls
          ``enable_tenant_membership()`` at all, which is an application
          bootstrap decision, not something this settings object (reached
          only once that decision is already "yes") governs.
    """

    claim_key: str = "tenants"
    on_missing_claim: MissingClaimPolicy = MissingClaimPolicy.ALLOW

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> TenantMembershipSettings:
        """
        Build ``TenantMembershipSettings`` from environment variables.

        Args:
            env: Mapping to read from. ``None`` reads the real
                 ``os.environ``.

        Returns:
            A ``TenantMembershipSettings`` reflecting the given environment.

        Raises:
            ValueError: ``VARCO_TENANT_MEMBERSHIP_ON_MISSING`` set to a
                value outside the legal set (``allow``/``deny``).
        """
        source = env if env is not None else os.environ

        on_missing_raw = source.get("VARCO_TENANT_MEMBERSHIP_ON_MISSING", "allow")
        if on_missing_raw not in _LEGAL_ON_MISSING:
            raise ValueError(
                f"Invalid VARCO_TENANT_MEMBERSHIP_ON_MISSING={on_missing_raw!r}. "
                f"Legal values are: {', '.join(_LEGAL_ON_MISSING)}."
            )

        return cls(
            claim_key=source.get("VARCO_TENANT_MEMBERSHIP_CLAIM", "tenants"),
            on_missing_claim=MissingClaimPolicy(on_missing_raw),
        )


@dataclass(frozen=True)
class MembershipDecision:
    """
    The result of an ``AbstractTenantMembership.check()`` call.

    Args:
        allowed: Whether the subject may act as ``tenant_id``.
        reason: A stable machine token (e.g. ``"not_in_claim"``,
            ``"self_tenant"``, ``"claim_absent"``, ``"no_provider"``) —
            never a formatted string containing a tenant id.
        provider: The producing ``AbstractTenantMembership.name``.
        tenant_id: The tenant id that was checked.
        subject: The subject's ``user_id``, or ``None`` for an anonymous
            caller.
    """

    allowed: bool
    reason: str
    provider: str
    tenant_id: str
    subject: str | None


class TenantMembershipError(ServiceException):
    """
    Raised by an application that chooses to turn a denied membership check
    into an exception rather than reading ``MembershipDecision`` directly.

    Args:
        tenant_id: The tenant the subject was denied membership of. **Never**
            included in ``error_params()`` — same exfiltration rule as
            ``CrossTenantAccessError``.
        reason: The stable machine token from the denying ``MembershipDecision``.
    """

    message_key = "varco.error.tenant_membership_denied"

    def __init__(self, tenant_id: str, reason: str) -> None:
        self._tenant_id = tenant_id
        self.reason = reason
        super().__init__(f"Tenant membership check denied (reason={reason!r}).")

    def error_params(self) -> dict[str, Any]:
        """Return interpolation data — deliberately excludes the tenant id."""
        return {"reason": self.reason}


class AbstractTenantMembership(ABC):
    """
    Decides whether an authenticated subject may act as a given tenant.

    ``check()`` is ``async`` even though the shipped implementations do no
    I/O — the parked repository-backed resolver (``BACKLOG.md:97``) is an
    out-of-tree implementation of this exact ABC and needs it.

    Edge cases:
        - ``check()`` must never raise for any input, including a malformed
          claim shape.
    """

    name: ClassVar[str]

    @abstractmethod
    async def check(self, ctx: AuthContext, tenant_id: str) -> MembershipDecision:
        """
        Args:
            ctx: The authenticated (or anonymous) caller's ``AuthContext``.
            tenant_id: The tenant id membership is being checked against.

        Returns:
            A ``MembershipDecision``. Never raises.
        """
        raise NotImplementedError


@Singleton(priority=-sys.maxsize - 1)
class NullTenantMembership(AbstractTenantMembership):
    """
    The scanned DI default — always allows.

    This is the Null Object: an app that scans ``varco_core`` and does
    nothing else gets exactly today's behaviour, byte-identical.
    """

    name: ClassVar[str] = "null"

    async def check(self, ctx: AuthContext, tenant_id: str) -> MembershipDecision:
        return MembershipDecision(
            allowed=True,
            reason="no_provider",
            provider=self.name,
            tenant_id=tenant_id,
            subject=getattr(ctx, "user_id", None),
        )


class ClaimTenantMembership(AbstractTenantMembership):
    """
    Membership from a signed claim list — no external lookup.

    Allows when **any** of:
        - ``tenant_id`` is in the ``tenants`` metadata list (the normal
          multi-org case); or
        - the token's own ``metadata["tenant_id"]`` equals ``tenant_id`` (a
          single-tenant token is its own membership proof — ``"self_tenant"``); or
        - the membership claim is absent **and** ``on_missing_claim=ALLOW``.

    Everything else denies.

    Args:
        claim_key: The ``AuthContext.metadata`` key holding the membership
            list. Defaults to ``"tenants"``.
        on_missing_claim: ``MissingClaimPolicy`` — behaviour when the claim
            is entirely absent. Defaults to ``ALLOW`` in 3.2 (flips to
            ``DENY`` in 4.0, per the 4.0 flip list).
    """

    name: ClassVar[str] = "claim"

    def __init__(
        self,
        *,
        claim_key: str = _DEFAULT_MEMBERSHIP_CLAIM_KEY,
        on_missing_claim: MissingClaimPolicy = MissingClaimPolicy.ALLOW,
    ) -> None:
        self.claim_key = claim_key
        self.on_missing_claim = on_missing_claim

    async def check(self, ctx: AuthContext, tenant_id: str) -> MembershipDecision:
        subject = getattr(ctx, "user_id", None)
        metadata = getattr(ctx, "metadata", None) or {}

        raw_claim = metadata.get(self.claim_key)

        if raw_claim is None:
            # Self-tenant: a single-tenant token is its own membership proof.
            if metadata.get("tenant_id") == tenant_id:
                return self._decision(True, "self_tenant", tenant_id, subject)
            return self._missing_claim_decision(tenant_id, subject)

        if not isinstance(raw_claim, list):
            return self._decision(False, "not_in_claim", tenant_id, subject)

        try:
            in_claim = tenant_id in raw_claim
        except TypeError:  # pragma: no cover — defensive, `in` on a list never raises
            in_claim = False

        if in_claim:
            return self._decision(True, "in_claim", tenant_id, subject)

        # DESIGN: self-tenant is a fallback for an ABSENT tenants claim only
        # — deliberately NOT re-checked here.
        # ✅ An explicit tenants list is authoritative once present: a token
        #    carrying tenants=["beta"] plus tenant_id="acme" must not be
        #    treated as a member of "acme" just because tenant_id happens to
        #    match — that would make the tenants list decorative the moment
        #    an IdP also stamps its usual single tenant_id claim alongside
        #    it, silently defeating the multi-org case this claim exists for.
        # ❌ A narrower reading than the plan's literal "OR" phrasing.
        #    Accepted — the plan's own unit tests only exercise self-tenant
        #    with the tenants claim entirely absent; the FastAPI integration
        #    test (test_tenant_chain_middleware.py) pins the case where both
        #    are present and disagree, and denies.
        return self._decision(False, "not_in_claim", tenant_id, subject)

    def _missing_claim_decision(self, tenant_id: str, subject: str | None) -> MembershipDecision:
        global _missing_claim_warned  # noqa: PLW0603 — see module docstring
        if not _missing_claim_warned:
            logger.warning(
                "ClaimTenantMembership: no %r claim present on the caller's "
                "token; on_missing_claim=%s. This warning is emitted once "
                "per process.",
                self.claim_key,
                self.on_missing_claim.value,
            )
            _missing_claim_warned = True
        allowed = self.on_missing_claim is MissingClaimPolicy.ALLOW
        return self._decision(allowed, "claim_absent", tenant_id, subject)

    @staticmethod
    def _decision(
        allowed: bool, reason: str, tenant_id: str, subject: str | None
    ) -> MembershipDecision:
        return MembershipDecision(
            allowed=allowed,
            reason=reason,
            provider=ClaimTenantMembership.name,
            tenant_id=tenant_id,
            subject=subject,
        )
