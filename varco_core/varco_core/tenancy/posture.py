"""
varco_core.tenancy.posture
============================
``inspect_tenant_provenance()`` — the introspection seam Plan 036 / S9's
preflight reports on (§D-036-seams). This plan defines and exports it;
036 owns the preflight surface and the HTTP mapping.

DESIGN: a pure inspector returning a frozen record, mirroring 037's ``inspect_rls_posture``
    ✅ Identical precedent one plan over (Plan 037 §D-S12-posture) — a frozen
       dataclass keeps 036's consumption a pure read, with no import from
       ``varco_fastapi`` into ``varco_core``.
    ✅ Pure and ambient-free means 036 can call it at startup, before any
       request exists.
    ✅ Stable finding tokens mean 036 formats strings without string-matching
       this plan's prose.
    ❌ Two new public types in the api-surface snapshot. Accepted.
    Rejected — 036 introspects the chain object itself: ❌ it would depend
    on private attribute shapes across a plan boundary and break the moment
    a source gains a field.

Thread safety:  ✅ Frozen dataclass; pure function.
Async safety:   ✅ No I/O, no ambient reads.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from varco_core.tenancy.source import CrossCheckMode, TenantSourceChain, TenantTrust

if TYPE_CHECKING:
    from varco_core.auth.delegation import DelegationPolicy
    from varco_core.tenancy.membership import AbstractTenantMembership

__all__ = ["TenantProvenancePosture", "inspect_tenant_provenance"]


@dataclass(frozen=True)
class TenantProvenancePosture:
    """
    A point-in-time snapshot of a deployment's tenant-provenance configuration.

    Args:
        chain_configured: ``False`` means header-only — today's behaviour.
        source_names: Chain order, e.g. ``("jwt", "subdomain")``.
        highest_trust: The highest ``TenantTrust`` among configured sources,
            or ``None`` when no chain is configured.
        legacy_source_active: A ``LegacyTenantSource`` is somewhere in the
            (possibly implicit) chain.
        legacy_source_implicit: ``True`` when ``chain=None`` fell back to
            the header internally, rather than an operator naming
            ``LegacyTenantSource`` explicitly.
        cross_check_mode: The configured ``CrossCheckMode``.
        subdomain_base_domains: Base domains configured on any
            ``SubdomainTenantSource`` in the chain.
        subdomain_trusts_forwarded_host: Any ``SubdomainTenantSource`` in the
            chain has ``trust_forwarded_host=True``.
        membership_provider: ``None`` when no membership provider is bound
            at all.
        membership_on_missing_claim: ``ClaimTenantMembership.on_missing_claim``
            value, or ``None`` when no claim-based provider is bound.
        delegation_policy: Phase 6 only; ``None`` when S16 is cut or no
            policy is bound.
        unchained_claim_tenant_setter: ``RequestContextMiddleware`` still
            sets the tenant alone (Correction 1).
        findings: Stable tokens — see the module for the pinned set.
    """

    chain_configured: bool
    source_names: tuple[str, ...]
    highest_trust: TenantTrust | None
    legacy_source_active: bool
    legacy_source_implicit: bool
    cross_check_mode: CrossCheckMode
    subdomain_base_domains: tuple[str, ...]
    subdomain_trusts_forwarded_host: bool
    membership_provider: str | None
    membership_on_missing_claim: str | None
    delegation_policy: str | None
    unchained_claim_tenant_setter: bool
    findings: tuple[str, ...]


def inspect_tenant_provenance(
    chain: TenantSourceChain | None,
    *,
    membership: AbstractTenantMembership | None = None,
    delegation: DelegationPolicy | None = None,
    request_context_sets_tenant: bool = True,
) -> TenantProvenancePosture:
    """
    Inspect a (possibly absent) tenant-provenance configuration.

    Pure: no I/O, no ambient reads, no logging. Safe to call at startup or
    in a test.

    Args:
        chain: The configured ``TenantSourceChain``, or ``None`` when the
            deployment has not adopted one (today's header-only behaviour).
        membership: The bound ``AbstractTenantMembership``, or ``None``.
        delegation: The bound delegation policy (Phase 6 / S16), or ``None``.
        request_context_sets_tenant: Whether
            ``RequestContextMiddleware.enable_tenant_context`` is ``True``
            in this deployment.

    Returns:
        A ``TenantProvenancePosture`` snapshot.
    """
    # Deferred import — varco_core.tenancy.sources imports this module's
    # sibling `source.py` only, never posture.py, so this is not a cycle;
    # deferred purely to keep this module's own import graph shallow.
    from varco_core.tenancy.sources import LegacyTenantSource, SubdomainTenantSource

    findings: list[str] = []

    if chain is None:
        findings.append("tenant.no_chain")
        findings.append("tenant.legacy_source_implicit")
        posture = TenantProvenancePosture(
            chain_configured=False,
            source_names=("legacy",),
            highest_trust=TenantTrust.LOW,
            legacy_source_active=True,
            legacy_source_implicit=True,
            cross_check_mode=CrossCheckMode.LENIENT,
            subdomain_base_domains=(),
            subdomain_trusts_forwarded_host=False,
            membership_provider=_membership_provider_name(membership),
            membership_on_missing_claim=_membership_on_missing(membership),
            delegation_policy=_delegation_policy_name(delegation),
            unchained_claim_tenant_setter=request_context_sets_tenant,
            findings=(),  # filled below, after the membership/delegation findings
        )
    else:
        source_names = tuple(getattr(s, "name", s.__class__.__name__) for s in chain.sources)
        legacy_sources = [s for s in chain.sources if isinstance(s, LegacyTenantSource)]
        subdomain_sources = [s for s in chain.sources if isinstance(s, SubdomainTenantSource)]

        if legacy_sources:
            findings.append("tenant.legacy_source_explicit")
        if len(chain.sources) == 1:
            findings.append("tenant.single_source")
        if chain.mode is CrossCheckMode.LENIENT:
            findings.append("tenant.cross_check_lenient")
        if any(s.trust_forwarded_host for s in subdomain_sources):
            findings.append("tenant.subdomain_trusts_forwarded_host")

        highest_trust = max((s.trust for s in chain.sources), default=None)
        base_domains: tuple[str, ...] = ()
        trusts_forwarded = False
        for s in subdomain_sources:
            base_domains = tuple(s.base_domains)
            trusts_forwarded = trusts_forwarded or s.trust_forwarded_host

        posture = TenantProvenancePosture(
            chain_configured=True,
            source_names=source_names,
            highest_trust=highest_trust,
            legacy_source_active=bool(legacy_sources),
            legacy_source_implicit=False,
            cross_check_mode=chain.mode,
            subdomain_base_domains=base_domains,
            subdomain_trusts_forwarded_host=trusts_forwarded,
            membership_provider=_membership_provider_name(membership),
            membership_on_missing_claim=_membership_on_missing(membership),
            delegation_policy=_delegation_policy_name(delegation),
            unchained_claim_tenant_setter=request_context_sets_tenant,
            findings=(),  # filled below
        )

    if membership is None:
        findings.append("tenant.no_membership_provider")
    else:
        on_missing = _membership_on_missing(membership)
        if on_missing == "allow":
            findings.append("tenant.membership_missing_claim_allows")

    if request_context_sets_tenant:
        findings.append("tenant.unchained_claim_tenant_setter")

    if delegation is None:
        findings.append("tenant.delegation_unbound")

    from dataclasses import replace

    return replace(posture, findings=tuple(findings))


def _membership_provider_name(membership: AbstractTenantMembership | None) -> str | None:
    if membership is None:
        return None
    return getattr(membership, "name", membership.__class__.__name__)


def _membership_on_missing(membership: AbstractTenantMembership | None) -> str | None:
    on_missing = getattr(membership, "on_missing_claim", None)
    if on_missing is None:
        return None
    return getattr(on_missing, "value", str(on_missing))


def _delegation_policy_name(delegation: DelegationPolicy | None) -> str | None:
    if delegation is None:
        return None
    return delegation.name
