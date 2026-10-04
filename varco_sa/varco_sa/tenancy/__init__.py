"""
varco_sa.tenancy
==================
SQLAlchemy backend for the multitenancy contracts declared in
``varco_core.tenancy`` (Plan 007).

Landed so far: Phase 2's global-scope SQLSTATE translation and Phase 3's
schema-per-tenant routing/provisioning/RLS-assertion. See
``plans/007-multitenancy-isolation-strategies.md`` for the full surface —
``SAEngineRegistry``, ``SATenantCatalog``, the ``admin/`` package (Phase 4/6)
are not yet implemented.
"""

from __future__ import annotations

from varco_sa.tenancy.catalog import SATenantCatalog
from varco_sa.tenancy.engine_registry import SAEngineRegistry
from varco_sa.tenancy.global_dsn import resolve_global_engine
from varco_sa.tenancy.global_scope import (
    install_global_readonly_translation,
    install_tenant_passthrough,
    maybe_install_global_readonly_translation,
)
from varco_sa.tenancy.guard import guard_fanout_configuration
from varco_sa.tenancy.models import tenants_metadata, tenants_table
from varco_sa.tenancy.provisioner import SASchemaProvisioner
from varco_sa.tenancy.rls_check import assert_rls_enabled
from varco_sa.tenancy.rls_posture import RlsPostureCollector, rls_findings
from varco_sa.tenancy.rls_session import hook_installed
from varco_sa.tenancy.router import SYMBOLIC_SCHEMA_TOKEN, SASchemaRouter

__all__ = [
    "install_global_readonly_translation",
    "install_tenant_passthrough",
    "maybe_install_global_readonly_translation",
    "SASchemaRouter",
    "SYMBOLIC_SCHEMA_TOKEN",
    "SASchemaProvisioner",
    "assert_rls_enabled",
    "RlsPostureCollector",
    "rls_findings",
    "hook_installed",
    "SATenantCatalog",
    "tenants_table",
    "tenants_metadata",
    "SAEngineRegistry",
    "resolve_global_engine",
    "guard_fanout_configuration",
]
