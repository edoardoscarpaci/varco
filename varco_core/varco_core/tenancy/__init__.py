"""
varco_core.tenancy
===================
Backend-agnostic multitenancy contracts (Plan 007) — isolation strategy
selection, the tenant catalog, the bounded per-tenant resource pool, the
dynamic UoW provider, and the tenant provisioner ABC.

Zero third-party dependencies. Backend packages (``varco_sa``,
``varco_beanie``) and ``varco_fastapi`` build on these contracts; this
package never imports ``sqlalchemy``, ``pymongo``, or ``beanie``.

See ``plans/007-multitenancy-isolation-strategies.md`` for the full design.
"""

from __future__ import annotations

from varco_core.tenancy.cache_key import tenancy_cache_key
from varco_core.tenancy.catalog import (
    AbstractTenantCatalog,
    StaticTenantCatalog,
    TenantDescriptor,
    TenantIsolationError,
    TenantNotFoundError,
)
from varco_core.tenancy.di import enable_tenant_membership
from varco_core.tenancy.global_scope import (
    GlobalScopeReadOnlyError,
    GlobalUoWProvider,
    is_global_entity,
)
from varco_core.tenancy.membership import (
    AbstractTenantMembership,
    ClaimTenantMembership,
    MembershipDecision,
    MissingClaimPolicy,
    NullTenantMembership,
    TenantMembershipError,
    TenantMembershipSettings,
)
from varco_core.tenancy.pool import TenantResourcePool
from varco_core.tenancy.posture import TenantProvenancePosture, inspect_tenant_provenance
from varco_core.tenancy.provenance import (
    CrossTenantAccessError,
    assert_tenant_matches,
    current_tenant_provenance,
    provenance_context,
)
from varco_core.tenancy.provider import DynamicTenantUoWProvider
from varco_core.tenancy.provisioner import (
    AbstractTenantProvisioner,
    DestructiveOperationRefused,
    ExternalTenantProvisioner,
)
from varco_core.tenancy.scope_guard import validate_service_scope
from varco_core.tenancy.settings import (
    TenancySettings,
    TenantIsolation,
    TenantProvenanceSettings,
    TenantScope,
    TenantStatus,
    build_tenant_source_chain,
)
from varco_core.tenancy.source import (
    CrossCheckMode,
    TenantClaim,
    TenantProvenance,
    TenantRequest,
    TenantSource,
    TenantSourceChain,
    TenantTrust,
)
from varco_core.tenancy.sources import (
    JwtClaimTenantSource,
    LegacyTenantSource,
    SubdomainTenantSource,
)

__all__ = [
    "TenantIsolation",
    "TenantScope",
    "TenantStatus",
    "TenancySettings",
    "TenantDescriptor",
    "AbstractTenantCatalog",
    "StaticTenantCatalog",
    "TenantNotFoundError",
    "TenantIsolationError",
    "TenantResourcePool",
    "DynamicTenantUoWProvider",
    "AbstractTenantProvisioner",
    "ExternalTenantProvisioner",
    "DestructiveOperationRefused",
    "GlobalUoWProvider",
    "GlobalScopeReadOnlyError",
    "is_global_entity",
    "validate_service_scope",
    "tenancy_cache_key",
    # ── Plan 033 / S6 — tenant identity provenance ──────────────────────
    "TenantTrust",
    "TenantRequest",
    "TenantClaim",
    "TenantSource",
    "TenantSourceChain",
    "CrossCheckMode",
    "TenantProvenance",
    "JwtClaimTenantSource",
    "SubdomainTenantSource",
    "LegacyTenantSource",
    "TenantProvenanceSettings",
    "build_tenant_source_chain",
    "current_tenant_provenance",
    "provenance_context",
    "assert_tenant_matches",
    "CrossTenantAccessError",
    # ── Plan 033 / S5 — tenant<->subject membership binding ─────────────
    "MembershipDecision",
    "AbstractTenantMembership",
    "NullTenantMembership",
    "ClaimTenantMembership",
    "MissingClaimPolicy",
    "TenantMembershipError",
    "TenantMembershipSettings",
    "enable_tenant_membership",
    # ── Plan 033 / Plan 036 seams ────────────────────────────────────────
    "TenantProvenancePosture",
    "inspect_tenant_provenance",
]
