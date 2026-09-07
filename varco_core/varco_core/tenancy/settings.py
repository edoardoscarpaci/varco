"""
varco_core.tenancy.settings
============================
``TenantIsolation`` / ``TenantScope`` / ``TenantStatus`` enums and the
env-driven ``TenancySettings`` (Plan 007, Phase 1, step 1-2) — **and**, since
Plan 033 / S6 (§D-S6-settings, open question 2), the env-driven
``TenantProvenanceSettings`` + ``build_tenant_source_chain()``.

§D-S6-oq2 — module placement: ``TenantProvenanceSettings`` lives in this
module rather than a new one. ``settings.py`` was already one cohesive
dataclass-plus-enums module for tenancy configuration, and a second,
unrelated settings object here reads no differently than the first — the
same reasoning Plan 037 recorded for its own, near-identical open question
about ``rls_check.py``. ⚠️ **No field on ``TenancySettings`` moves or is
added** — the two dataclasses are independent; only the module is shared.

⚠️ **``TenantProvenanceSettings`` is not the RD-9 case.** RD-9 forbids a
``VARCO_TENANCY_MOUNT_ADMIN`` env var *forever* because a bare environment
variable would expose a privileged HTTP surface. ``VARCO_TENANT_*`` does
the opposite: it *restricts* where a tenant identity may come from, and
mounts nothing. The one thing that stays code-only, per RD-9's actual
reasoning, is anything that mounts a surface — this module mounts nothing.

DESIGN: three enum values, not six — RLS is additive
    ✅ ``TenantIsolation`` names *how strongly* tenants are isolated; RLS is
       a hardening flag (``enforce_rls: bool``) on ``SHARED``, not a fourth
       enum value — keeps the enum backend-neutral (avoids doubling again
       for ``TenantScope``).
    ❌ Two independent boolean-ish axes are visible only through the enum
       *and* a flag rather than one flat value. Accepted — see the plan's
       "Alternatives considered" section for the rejected six-value form.

DESIGN: frozen dataclass with ``from_env()``, mirroring ``MigrationSettings``
    ✅ Matches the established shape for injectable settings objects in this
       repo (``SAConfig``, ``BeanieSettings``, ``MigrationSettings``).
    ✅ Avoids the ``@Singleton``-on-pydantic-``BaseSettings`` pitfall — this
       is a plain frozen dataclass, safe to register via ``@Provider``.
    ✅ ``env=`` is injectable so tests never mutate ``os.environ``.
    ❌ No automatic env-var validation/coercion helpers pydantic gives for
       free — ``from_env()`` does its own parsing, same trade-off as
       ``MigrationSettings``.

RD-9: **no** ``VARCO_TENANCY_MOUNT_ADMIN`` env var exists or is ever read.
    The privileged admin surface can only be mounted via an explicit,
    acknowledged code call (``mount_tenant_admin(..., acknowledge_bundled_
    admin=True)``, Phase 5) — never by environment alone. This module does
    not recognise any such key, by design; asserted directly in
    ``test_tenancy_settings.py::test_from_env_ignores_mount_admin_env_var_entirely``.

Thread safety:  ✅ Frozen — safe to share across coroutines/threads.
Async safety:   ✅ No I/O.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from varco_core.auth.delegation import DelegationPolicy
    from varco_core.tenancy.source import CrossCheckMode, TenantSourceChain, TenantTrust

_LEGAL_ISOLATION = ("shared", "schema", "database")


class TenantIsolation(StrEnum):
    """How strongly tenants are isolated at the storage layer."""

    SHARED = "shared"  # one schema/db/collection + discriminator
    SCHEMA = "schema"  # one Postgres schema per tenant   (Postgres only)
    DATABASE = "database"  # one logical database per tenant  (Postgres + Mongo)


class TenantScope(StrEnum):
    """Orthogonal axis: whether an entity is per-tenant or globally shared."""

    TENANT = "tenant"  # default — routed per tenant under SCHEMA/DATABASE
    GLOBAL = "global"  # one shared copy; every tenant reads it


class TenantStatus(StrEnum):
    """Lifecycle status of a tenant in the catalog (Plan 007, Phase 4)."""

    PENDING = "pending"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DEPROVISIONING = "deprovisioning"
    DELETED = "deleted"


@dataclass(frozen=True)
class TenancySettings:
    """
    Env-driven multitenancy configuration.

    With every default, the deployment is byte-identical to today's
    behaviour: no pool, no extra engine/client, no symbolic schema, no
    control-plane surface constructed.

    Args:
        isolation: ``TenantIsolation`` — storage isolation strategy.
                   Env: ``VARCO_TENANCY_ISOLATION``.
        enforce_rls: Hardening flag on ``SHARED`` — assert Postgres RLS is
                   enabled on every routed table. Env:
                   ``VARCO_TENANCY_ENFORCE_RLS``.
        schema_template: ``{tenant_id}``-templated schema name for
                   ``SCHEMA``. Env: ``VARCO_TENANCY_SCHEMA_TEMPLATE``.
        db_template: ``{tenant_id}``-templated database name for
                   ``DATABASE``. Env: ``VARCO_TENANCY_DB_TEMPLATE``.
        max_entries: Soft cap on the bounded per-tenant resource pool.
                   Env: ``VARCO_TENANCY_MAX_ENTRIES``.
        idle_ttl_s: Sweeper idle threshold, seconds. Env:
                   ``VARCO_TENANCY_IDLE_TTL``.
        catalog_ttl_s: ``CachedTenantCatalog`` TTL backstop, seconds. Env:
                   ``VARCO_TENANCY_CATALOG_TTL``.
        fanout_framework_tables: Enable ``TenantFanoutSupervisor`` (RD-8).
                   Env: ``VARCO_TENANCY_FANOUT_FRAMEWORK_TABLES``.
        global_dsn: Optional DSN for the global/shared database (RD-10).
                   Falls back to the app's own DSN when unset. Env:
                   ``VARCO_TENANCY_GLOBAL_DSN``.
        global_writable: Opt-in to a writable global credential (RD-10).
                   Env: ``VARCO_TENANCY_GLOBAL_WRITABLE``.
        rls_set_tenant: Install the ``after_begin`` GUC-setter hook
                   (``varco_sa.tenancy.rls_session.install_rls_tenant_hook``)
                   automatically at provider construction (Plan 037 / S12c,
                   §D-S12-hook). Env: ``VARCO_TENANCY_RLS_SET_TENANT``.
        rls_require_tenant: When ``rls_set_tenant`` is on, raise instead of
                   clearing the GUC when no tenant is ambient (Plan 037,
                   §D-S12-hook's fail-closed-is-opt-in note). Env:
                   ``VARCO_TENANCY_RLS_REQUIRE_TENANT``.
        assert_tenant_filter: Opt-in, **development-time only** AST
                   tenant-filter guard (Plan 037 / S15,
                   ``varco_core.query.applicator.tenant_guard``) — **not a
                   security control**; Postgres RLS (``enforce_rls``/
                   ``rls_set_tenant`` above) is. Env:
                   ``VARCO_TENANCY_ASSERT_TENANT_FILTER``.

    Edge cases:
        - No key corresponding to "mount the admin surface" is recognised
          anywhere in this module (RD-9) — asserted by a dedicated test.

    DESIGN: two new flags, ``enforce_rls`` untouched (Plan 037 / §D-S12-hook)
        ✅ ``enforce_rls`` means one thing today — "assert Postgres RLS is
           enabled on every routed table". Widening it to also mean "and set
           the GUC for me" would change runtime behaviour for every existing
           ``enforce_rls=True`` deployment on upgrade — precisely the
           silent-change class this cycle exists to stop.
        ✅ Independent knobs match reality: an app can want the assertion
           without the hook, or the hook without the assertion.
        ❌ Three RLS-related flags on one settings object. Accepted; the
           alternative is one flag that means three things.
    """

    isolation: TenantIsolation = TenantIsolation.SHARED
    enforce_rls: bool = False
    schema_template: str = "t_{tenant_id}"
    db_template: str = "db_{tenant_id}"
    max_entries: int = 50
    idle_ttl_s: float = 300.0
    catalog_ttl_s: float = 60.0
    fanout_framework_tables: bool = False
    global_dsn: str | None = None
    global_writable: bool = False
    rls_set_tenant: bool = False
    rls_require_tenant: bool = False
    assert_tenant_filter: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> TenancySettings:
        """
        Build ``TenancySettings`` from environment variables.

        Args:
            env: Mapping to read from. ``None`` reads the real
                 ``os.environ``. Tests pass a scoped mapping instead of
                 mutating the process environ.

        Returns:
            A ``TenancySettings`` reflecting the given environment, with
            documented defaults for anything unset.

        Raises:
            ValueError: ``VARCO_TENANCY_ISOLATION`` set to a value outside
                the legal set.
        """
        source = env if env is not None else os.environ

        isolation = source.get("VARCO_TENANCY_ISOLATION", TenantIsolation.SHARED.value)
        if isolation not in _LEGAL_ISOLATION:
            raise ValueError(
                f"Invalid VARCO_TENANCY_ISOLATION={isolation!r}. "
                f"Legal values are: {', '.join(_LEGAL_ISOLATION)}."
            )

        def _bool(key: str, default: bool) -> bool:
            raw = source.get(key)
            if raw is None:
                return default
            return raw.strip().lower() in ("1", "true", "yes", "on")

        defaults = cls()

        return cls(
            isolation=TenantIsolation(isolation),
            enforce_rls=_bool("VARCO_TENANCY_ENFORCE_RLS", defaults.enforce_rls),
            schema_template=source.get("VARCO_TENANCY_SCHEMA_TEMPLATE", defaults.schema_template),
            db_template=source.get("VARCO_TENANCY_DB_TEMPLATE", defaults.db_template),
            max_entries=int(source.get("VARCO_TENANCY_MAX_ENTRIES", defaults.max_entries)),
            idle_ttl_s=float(source.get("VARCO_TENANCY_IDLE_TTL", defaults.idle_ttl_s)),
            catalog_ttl_s=float(source.get("VARCO_TENANCY_CATALOG_TTL", defaults.catalog_ttl_s)),
            fanout_framework_tables=_bool(
                "VARCO_TENANCY_FANOUT_FRAMEWORK_TABLES",
                defaults.fanout_framework_tables,
            ),
            global_dsn=source.get("VARCO_TENANCY_GLOBAL_DSN", defaults.global_dsn),
            global_writable=_bool("VARCO_TENANCY_GLOBAL_WRITABLE", defaults.global_writable),
            rls_set_tenant=_bool("VARCO_TENANCY_RLS_SET_TENANT", defaults.rls_set_tenant),
            rls_require_tenant=_bool(
                "VARCO_TENANCY_RLS_REQUIRE_TENANT", defaults.rls_require_tenant
            ),
            assert_tenant_filter=_bool(
                "VARCO_TENANCY_ASSERT_TENANT_FILTER", defaults.assert_tenant_filter
            ),
        )


_LEGAL_TENANT_SOURCES = ("jwt", "subdomain", "legacy", "act_as")
_LEGAL_CROSS_CHECK = ("lenient", "strict")
_LEGAL_MIN_TRUST = ("low", "medium", "high", "highest")
_DEFAULT_RESERVED_LABELS_STR = "www,api,app,admin,static,cdn"


@dataclass(frozen=True)
class TenantProvenanceSettings:
    """
    Env-driven tenant-provenance chain configuration (Plan 033 / S6,
    §D-S6-settings).

    Every field is unset/off by default; an unset ``VARCO_TENANT_SOURCES``
    means ``build_tenant_source_chain()`` returns ``None`` — nothing
    changes for an app that configures none of this.

    Args:
        sources: Ordered source names, from ``VARCO_TENANT_SOURCES``
            (comma-separated: ``jwt``, ``subdomain``, ``legacy``, ``act_as``).
            Empty ⇒ no chain.
        cross_check: ``CrossCheckMode``. Env: ``VARCO_TENANT_CROSS_CHECK``.
        min_trust: ``TenantTrust`` floor. Env: ``VARCO_TENANT_MIN_TRUST``.
        claim_metadata_key: ``AuthContext.metadata`` key the JWT source
            reads. Env: ``VARCO_TENANT_CLAIM_METADATA_KEY``.
        base_domains: Required when ``subdomain`` is in ``sources``. Env:
            ``VARCO_TENANT_BASE_DOMAINS`` (comma-separated).
        trust_forwarded_host: Env: ``VARCO_TENANT_TRUST_FORWARDED_HOST``.
        forwarded_host_header: Env: ``VARCO_TENANT_FORWARDED_HOST_HEADER``.
        reserved_labels: Env: ``VARCO_TENANT_RESERVED_LABELS``
            (comma-separated).
        legacy_header: Env: ``VARCO_TENANT_LEGACY_HEADER``.

    Raises:
        ValueError: An unknown source name, or ``subdomain`` requested
            without ``VARCO_TENANT_BASE_DOMAINS``.
    """

    sources: tuple[str, ...] = ()
    cross_check: CrossCheckMode = None  # type: ignore[assignment]  # set in __post_init__ default below
    min_trust: TenantTrust = None  # type: ignore[assignment]
    claim_metadata_key: str = "tenant_id"
    base_domains: tuple[str, ...] = ()
    trust_forwarded_host: bool = False
    forwarded_host_header: str = "X-Forwarded-Host"
    reserved_labels: frozenset[str] = frozenset()
    legacy_header: str = "X-Tenant-Id"

    def __post_init__(self) -> None:
        # Defaults that reference other modules (CrossCheckMode/TenantTrust)
        # are applied here rather than as literal dataclass field defaults,
        # to avoid importing varco_core.tenancy.source before it (and this
        # module) both finish defining their own top-level names.
        from varco_core.tenancy.source import CrossCheckMode, TenantTrust

        if self.cross_check is None:
            object.__setattr__(self, "cross_check", CrossCheckMode.LENIENT)
        if self.min_trust is None:
            object.__setattr__(self, "min_trust", TenantTrust.LOW)
        if not self.reserved_labels:
            object.__setattr__(
                self, "reserved_labels", frozenset(_DEFAULT_RESERVED_LABELS_STR.split(","))
            )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> TenantProvenanceSettings:
        """
        Build ``TenantProvenanceSettings`` from environment variables.

        Args:
            env: Mapping to read from. ``None`` reads the real
                 ``os.environ``.

        Returns:
            A ``TenantProvenanceSettings`` reflecting the given environment.

        Raises:
            ValueError: An unknown source name in ``VARCO_TENANT_SOURCES``,
                an illegal ``VARCO_TENANT_CROSS_CHECK``/``VARCO_TENANT_MIN_TRUST``
                value, or ``subdomain`` requested without
                ``VARCO_TENANT_BASE_DOMAINS``.
        """
        from varco_core.tenancy.source import CrossCheckMode, TenantTrust

        source = env if env is not None else os.environ

        raw_sources = source.get("VARCO_TENANT_SOURCES", "")
        sources = tuple(s.strip() for s in raw_sources.split(",") if s.strip())
        unknown = [s for s in sources if s not in _LEGAL_TENANT_SOURCES]
        if unknown:
            raise ValueError(
                f"Invalid VARCO_TENANT_SOURCES entry(ies): {', '.join(unknown)}. "
                f"Legal values are: {', '.join(_LEGAL_TENANT_SOURCES)}."
            )

        cross_check_raw = source.get("VARCO_TENANT_CROSS_CHECK", "lenient")
        if cross_check_raw not in _LEGAL_CROSS_CHECK:
            raise ValueError(
                f"Invalid VARCO_TENANT_CROSS_CHECK={cross_check_raw!r}. "
                f"Legal values are: {', '.join(_LEGAL_CROSS_CHECK)}."
            )

        min_trust_raw = source.get("VARCO_TENANT_MIN_TRUST", "low")
        if min_trust_raw not in _LEGAL_MIN_TRUST:
            raise ValueError(
                f"Invalid VARCO_TENANT_MIN_TRUST={min_trust_raw!r}. "
                f"Legal values are: {', '.join(_LEGAL_MIN_TRUST)}."
            )

        base_domains_raw = source.get("VARCO_TENANT_BASE_DOMAINS", "")
        base_domains = tuple(d.strip() for d in base_domains_raw.split(",") if d.strip())
        if "subdomain" in sources and not base_domains:
            raise ValueError(
                "VARCO_TENANT_SOURCES includes 'subdomain' but VARCO_TENANT_BASE_DOMAINS "
                "is unset. A subdomain source requires at least one explicit base domain "
                "(§D-S6-oq3) — set VARCO_TENANT_BASE_DOMAINS."
            )

        def _bool(key: str, default: bool) -> bool:
            raw = source.get(key)
            if raw is None:
                return default
            return raw.strip().lower() in ("1", "true", "yes", "on")

        reserved_raw = source.get("VARCO_TENANT_RESERVED_LABELS", _DEFAULT_RESERVED_LABELS_STR)
        reserved_labels = frozenset(r.strip() for r in reserved_raw.split(",") if r.strip())

        return cls(
            sources=sources,
            cross_check=CrossCheckMode(cross_check_raw),
            min_trust=TenantTrust[min_trust_raw.upper()],
            claim_metadata_key=source.get("VARCO_TENANT_CLAIM_METADATA_KEY", "tenant_id"),
            base_domains=base_domains,
            trust_forwarded_host=_bool("VARCO_TENANT_TRUST_FORWARDED_HOST", False),
            forwarded_host_header=source.get(
                "VARCO_TENANT_FORWARDED_HOST_HEADER", "X-Forwarded-Host"
            ),
            reserved_labels=reserved_labels,
            legacy_header=source.get("VARCO_TENANT_LEGACY_HEADER", "X-Tenant-Id"),
        )


def build_tenant_source_chain(
    settings: TenantProvenanceSettings | None = None,
    *,
    delegation_policy: DelegationPolicy | None = None,
) -> TenantSourceChain | None:
    """
    Build a ``TenantSourceChain`` from ``TenantProvenanceSettings``, or
    ``None`` when nothing is configured.

    Args:
        settings: The settings to build from. ``None`` reads the real
            process environment via ``TenantProvenanceSettings.from_env()``.
        delegation_policy: Phase 6 only — a ``DelegationPolicy`` to bind to
            an ``act_as`` source. Required if ``"act_as"`` is in
            ``settings.sources``.

    Returns:
        A ``TenantSourceChain``, or ``None`` when ``VARCO_TENANT_SOURCES``
        (or ``settings.sources``) is empty — nothing changes.

    Raises:
        ValueError: ``"act_as"`` is requested without a ``delegation_policy``.
    """
    from varco_core.tenancy.source import TenantSourceChain
    from varco_core.tenancy.sources import (
        JwtClaimTenantSource,
        LegacyTenantSource,
        SubdomainTenantSource,
    )

    resolved = settings if settings is not None else TenantProvenanceSettings.from_env()

    if not resolved.sources:
        return None

    built: list[Any] = []
    for name in resolved.sources:
        if name == "jwt":
            built.append(JwtClaimTenantSource(metadata_key=resolved.claim_metadata_key))
        elif name == "subdomain":
            built.append(
                SubdomainTenantSource(
                    base_domains=resolved.base_domains,
                    trust_forwarded_host=resolved.trust_forwarded_host,
                    forwarded_host_header=resolved.forwarded_host_header,
                    reserved_labels=resolved.reserved_labels,
                )
            )
        elif name == "legacy":
            built.append(LegacyTenantSource(header=resolved.legacy_header))
        elif name == "act_as":
            if delegation_policy is None:
                raise ValueError(
                    "VARCO_TENANT_SOURCES includes 'act_as' but no delegation_policy was "
                    "given to build_tenant_source_chain() — act_as requires a bound "
                    "DelegationPolicy (Plan 033 / S16)."
                )
            from varco_core.tenancy.sources import ActAsTenantSource

            built.append(ActAsTenantSource(delegation_policy))

    return TenantSourceChain(
        sources=tuple(built), mode=resolved.cross_check, min_trust=resolved.min_trust
    )
