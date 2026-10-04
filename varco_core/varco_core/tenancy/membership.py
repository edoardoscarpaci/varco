"""
varco_core.tenancy.membership
==============================
Tenant↔subject membership binding (Plan 033 / S5, §D-S5-claim) — a signed
claim list, no external lookup, fail-closed by default since 4.0 (Plan 055 /
FLIP-7).

DESIGN: a Null Object DI default plus an ``enable_*`` opt-in
    ✅ Exactly the shape CLAUDE.md already records twice —
       ``NullFeatureFlags`` scanned + ``enable_feature_flags()``, and the
       ``enable_policy_authorizer`` rule that an opt-in must **not** be a
       scanned ``@Configuration`` (scan auto-activates those and would
       silently shadow an app's own binding).
    ✅ The self-membership branch means a large class of existing
       single-tenant deployments become *correctly* enforced the moment
       they opt in, with no IdP change at all.
    ✅ Trust stays anchored in the signature — no lookup, no per-request
       query, per the locked membership decision (``BACKLOG.md:57``).
    Rejected — ``ClaimTenantMembership`` as the scanned default: ❌ a
    scanned default that denies is a fleet-wide 403 on upgrade; a scanned
    default that allows is indistinguishable from ``NullTenantMembership``
    while being harder to reason about.
    Rejected — a ``tenants`` field on ``AuthContext``: ❌ a frozen,
    api-surface-tracked dataclass with many out-of-tree constructors;
    ``metadata`` is the documented home for extra claims.

DESIGN: fail-closed by default since 4.0 (Plan 055 / FLIP-7)
    ``on_missing_claim`` defaults to ``DENY`` at all three sites (the enum
    comment, ``TenantMembershipSettings``'s field, ``ClaimTenantMembership``'s
    constructor). It was ``ALLOW`` + one WARNING in 3.2.
    ✅ Brief 006 §2 rule 5 — *fail closed on missing tenant*: an opted-in app
       whose IdP silently stops emitting ``tenants`` now denies instead of
       degrading to no enforcement.
    ✅ The self-tenant branch is untouched — a single-tenant token whose
       ``tenant_id`` equals the resolved tenant is still its own proof, so
       a single-tenant fleet keeps working with no IdP change.
    ✅ One greppable opt-out (``VARCO_TENANT_MEMBERSHIP_ON_MISSING=allow``),
       reported as ``tenant.membership_missing_claim_allows`` (HIGH) — so
       the opt-out needs its acknowledgement (``VARCO_SECURITY_SUPPRESS``).
    ❌ A multi-org fleet whose IdP has not yet added the ``tenants`` claim
       403s every cross-tenant request at upgrade. Mitigated: the decision's
       ``reason`` is ``"claim_absent"`` and one WARNING per process names
       the policy, so an operator can find out why.
    Rejected — flipping only the env default and keeping the constructor
    default ``ALLOW``: ❌ two defaults for one field; a code caller
    constructing ``ClaimTenantMembership()`` would stay fail-open silently.
    Rejected — removing ``MissingClaimPolicy.ALLOW``: ❌ removes a 3.2
    public enum member with no deprecation window, and a suppressed,
    time-boxed ``allow`` during an IdP rollout is a legitimate shape.

Thread safety:  ✅ Both shipped implementations are stateless except for a
    module-level "warned once" flag guarded by no lock — a benign race would
    at worst emit the warning twice, never corrupt state.
Async safety:   ✅ ``check()`` is ``async`` for ABC-compatibility with a
    future I/O-backed resolver (``BACKLOG.md:97``); the shipped
    implementations do no I/O.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar

from providify import Fallback, Singleton
from pydantic import AliasChoices, Field
from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings
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


_DEFAULT_MEMBERSHIP_CLAIM_KEY = "tenants"

# Module-level "warned once per process" flag for the missing-claim WARNING
# (§D-S5-claim). Deliberately not per-instance: the point is one operator-
# visible signal per process lifetime, not once per ClaimTenantMembership().
_missing_claim_warned = False


class MissingClaimPolicy(StrEnum):
    """What ``ClaimTenantMembership`` does when the membership claim is absent."""

    # The 3.2 default — an explicit opt-out since 4.0 (Plan 055 / FLIP-7);
    # reported as `tenant.membership_missing_claim_allows` (HIGH).
    ALLOW = "allow"
    # The default since 4.0 (Plan 055 / FLIP-7).
    DENY = "deny"


class TenantMembershipSettings(VarcoSettings):
    """
    Env-driven configuration for ``enable_tenant_membership()`` (Plan 033 /
    S5, §D-S5-claim, Decision 4).

    DESIGN: ``VarcoSettings`` (pydantic), living in this module rather than
    ``tenancy/settings.py``
    ✅ ``ClaimTenantMembership``/``MissingClaimPolicy`` — the only things
       this settings object configures — are already defined here;
       colocating avoids a new inter-module import.
    ✅ ``varco_core.tenancy.di`` (the only caller) already imports from
       ``membership.py`` — this adds no new import there.
    **Plan 048 / DI-6: converted from a frozen dataclass with a
    hand-written ``from_env(env: Mapping | None)`` to a pydantic
    ``VarcoSettings``** — same field names, same default literals; the
    ``env=`` parameter is gone (tests use ``monkeypatch.setenv``).

    Attributes:
        claim_key: The ``AuthContext.metadata`` key holding the membership
            list. Env: ``VARCO_TENANT_MEMBERSHIP_CLAIM``. Defaults to
            ``"tenants"``.
        on_missing_claim: ``MissingClaimPolicy``. Env:
            ``VARCO_TENANT_MEMBERSHIP_ON_MISSING``. Defaults to ``DENY``
            since 4.0 (Plan 055 / FLIP-7); ``allow`` is the explicit,
            HIGH-reported opt-out.

    Edge cases:
        - ``VARCO_TENANT_MEMBERSHIP`` itself (the ``null``/``claim``
          selector in the README's env-var table) is deliberately **not**
          read here — that var decides *whether* an app calls
          ``enable_tenant_membership()`` at all, which is an application
          bootstrap decision, not something this settings object (reached
          only once that decision is already "yes") governs.
    """

    model_config = SettingsConfigDict(
        env_prefix="VARCO_TENANT_MEMBERSHIP_", frozen=True, populate_by_name=True
    )

    # ⚠️ The env var is VARCO_TENANT_MEMBERSHIP_CLAIM, not
    # VARCO_TENANT_MEMBERSHIP_CLAIM_KEY (the field's own name).
    claim_key: str = Field(
        default="tenants", validation_alias=AliasChoices("VARCO_TENANT_MEMBERSHIP_CLAIM")
    )
    # ⚠️ The env var is VARCO_TENANT_MEMBERSHIP_ON_MISSING, not
    # VARCO_TENANT_MEMBERSHIP_ON_MISSING_CLAIM (the field's own name) —
    # a validation_alias is required to keep the incumbent name working.
    on_missing_claim: MissingClaimPolicy = Field(
        default=MissingClaimPolicy.DENY,
        validation_alias=AliasChoices("VARCO_TENANT_MEMBERSHIP_ON_MISSING"),
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


@Fallback
@Singleton
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
            is entirely absent. Defaults to ``DENY`` since 4.0 (Plan 055 /
            FLIP-7); pass ``MissingClaimPolicy.ALLOW`` to restore the 3.2
            fail-open behaviour (reported as a HIGH posture finding).
    """

    name: ClassVar[str] = "claim"

    def __init__(
        self,
        *,
        claim_key: str = _DEFAULT_MEMBERSHIP_CLAIM_KEY,
        on_missing_claim: MissingClaimPolicy = MissingClaimPolicy.DENY,
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
            # The one log line is how an operator learns why a fleet 403s
            # after the 4.0 flip — so it names the policy and the outcome.
            outcome = (
                "allowed (explicit opt-out)"
                if self.on_missing_claim is MissingClaimPolicy.ALLOW
                else "denied (reason=claim_absent)"
            )
            logger.warning(
                "ClaimTenantMembership: on_missing_claim=%s — callers without a %r "
                "claim are %s. This warning is emitted once per process.",
                self.on_missing_claim.value,
                self.claim_key,
                outcome,
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
