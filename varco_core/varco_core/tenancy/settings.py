"""
varco_core.tenancy.settings
============================
``TenantIsolation`` / ``TenantScope`` / ``TenantStatus`` enums and the
env-driven ``TenancySettings`` (Plan 007, Phase 1, step 1-2) — **and**, since
Plan 033 / S6 (§D-S6-settings, open question 2), the env-driven
``TenantProvenanceSettings`` + ``build_tenant_source_chain()``.

§D-S6-oq2 — module placement: ``TenantProvenanceSettings`` lives in this
module rather than a new one. ``settings.py`` was already one cohesive
settings module for tenancy configuration, and a second, unrelated settings
object here reads no differently than the first. ⚠️ **No field on
``TenancySettings`` moves or is added except the two Plan 048 / §D-loose
foldings below** — the two classes are otherwise independent; only the
module is shared.

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

DESIGN: ``VarcoSettings`` (pydantic), not a hand-rolled ``from_env()``
    **Plan 048 / DI-6: converted from frozen dataclasses with hand-rolled
    ``from_env(env: Mapping | None)`` to pydantic ``VarcoSettings``.** Same
    class names, same field names, every default literal byte-identical.
    The ``env: Mapping | None`` constructor parameter is gone — tests now
    use ``monkeypatch.setenv``. Two loose reads outside any class
    (``VARCO_TENANCY_NODE_ID``/``VARCO_TENANCY_STORE_ID``,
    ``tenancy/control/service.py``) are folded into ``TenancySettings`` as
    ``node_id``/``store_id`` fields (§D-loose) — not a rename, a new field
    with the same env-var name.
    ✅ Registered via ``bind_settings()`` — one recipe, fail-fast, provenance.
    ✅ ``Literal``/``StrEnum`` fields validate natively — the old
       ``_LEGAL_ISOLATION``/``_LEGAL_TENANT_SOURCES``/``_LEGAL_CROSS_CHECK``/
       ``_LEGAL_MIN_TRUST`` manual-membership checks are gone; pydantic's own
       message still names the legal values.
    ❌ None — pure shape conversion; no behavioural change to defaults.

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

from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings
from varco_core.settings.types import CommaSeparated
from varco_core.tenancy.source import CrossCheckMode, TenantTrust

if TYPE_CHECKING:
    from collections.abc import Iterable

    from varco_core.auth.delegation import DelegationPolicy
    from varco_core.tenancy.source import TenantSource, TenantSourceChain

_DEFAULT_RESERVED_LABELS = ("www", "api", "app", "admin", "static", "cdn")

#: Plan 051 / DI-5, §D-tenant: the four names ``build_tenant_source_chain``
#: constructs itself from ``TenantProvenanceSettings`` fields. A contributed
#: ``TenantSource`` (the collection-seam pool) may never reuse one of these
#: names — see ``build_tenant_source_chain``'s collision check.
_BUILTIN_SOURCE_NAMES = ("jwt", "subdomain", "legacy", "act_as")


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


class TenancySettings(VarcoSettings):
    """
    Env-driven multitenancy configuration.

    Every ``TenancySettings()`` default except ``enforce_rls`` is
    byte-identical to pre-Plan-007 behaviour (no pool, no extra
    engine/client, no symbolic schema, no control-plane surface
    constructed); ``enforce_rls=True`` (4.0, Plan 056 / FLIP-12) has an
    effect only where ``SAModule`` finds ``TENANT``-scoped tables carrying a
    tenant column, on Postgres, under ``SHARED``.

    Attributes:
        isolation: ``TenantIsolation`` — storage isolation strategy.
                   Env: ``VARCO_TENANCY_ISOLATION``.
        enforce_rls: Hardening flag on ``SHARED``. Default ``True`` since
                   4.0 (Plan 056 / FLIP-12): ``SAModule``'s ``sa.rls_posture``
                   lifecycle component (after migrations) reads the RLS
                   posture of every ``TENANT``-scoped table with a tenant
                   column on Postgres under ``SHARED`` isolation;
                   ``SecurityPosture`` reports it (``data.rls_*``).
                   ``false`` skips the read and is itself reported
                   (``data.rls_not_enforced``, WARN). Never emits DDL. Env:
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
        node_id: Plan 048 / §D-loose fold of the loose
                   ``VARCO_TENANCY_NODE_ID`` read
                   (``tenancy/control/service.py``). Env:
                   ``VARCO_TENANCY_NODE_ID``.
        store_id: Plan 048 / §D-loose fold of the loose
                   ``VARCO_TENANCY_STORE_ID`` read. Env:
                   ``VARCO_TENANCY_STORE_ID``.

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
        4.0: the default flipped; the meaning did not — it still never sets
        the GUC (Plan 056 §D-flip12-guc).
    """

    model_config = SettingsConfigDict(
        env_prefix="VARCO_TENANCY_", frozen=True, populate_by_name=True
    )

    isolation: TenantIsolation = TenantIsolation.SHARED
    enforce_rls: bool = True
    schema_template: str = "t_{tenant_id}"
    db_template: str = "db_{tenant_id}"
    max_entries: int = 50
    idle_ttl_s: float = Field(
        default=300.0, validation_alias=AliasChoices("VARCO_TENANCY_IDLE_TTL")
    )
    catalog_ttl_s: float = Field(
        default=60.0, validation_alias=AliasChoices("VARCO_TENANCY_CATALOG_TTL")
    )
    fanout_framework_tables: bool = False
    global_dsn: str | None = None
    global_writable: bool = False
    rls_set_tenant: bool = False
    rls_require_tenant: bool = False
    assert_tenant_filter: bool = False
    node_id: str | None = None
    store_id: str | None = None


class TenantProvenanceSettings(VarcoSettings):
    """
    Env-driven tenant-provenance chain configuration (Plan 033 / S6,
    §D-S6-settings).

    Every field is unset/off by default; an unset ``VARCO_TENANT_SOURCES``
    means ``build_tenant_source_chain()`` returns ``None`` — nothing
    changes for an app that configures none of this.

    Attributes:
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
        ValidationError: An unknown source name, or ``subdomain`` requested
            without ``VARCO_TENANT_BASE_DOMAINS`` (raised from a
            ``model_validator(mode="after")``).
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_TENANT_", frozen=True)

    sources: CommaSeparated = ()
    cross_check: CrossCheckMode = CrossCheckMode.LENIENT
    min_trust: TenantTrust = TenantTrust.LOW
    claim_metadata_key: str = "tenant_id"
    base_domains: CommaSeparated = ()
    trust_forwarded_host: bool = False
    forwarded_host_header: str = "X-Forwarded-Host"
    reserved_labels: CommaSeparated = _DEFAULT_RESERVED_LABELS
    legacy_header: str = "X-Tenant-Id"

    @field_validator("min_trust", mode="before")
    @classmethod
    def _min_trust_by_name(cls, value: object) -> object:
        """
        Accept the enum's *name* (``"low"``/``"medium"``/``"high"``/``"highest"``,
        case-insensitive), matching the old ``from_env()``'s
        ``TenantTrust[min_trust_raw.upper()]`` lookup — ``TenantTrust`` is an
        ``IntEnum``, so pydantic's native coercion expects the *value*
        (``10``/``20``/…), not the name, and would otherwise reject
        ``VARCO_TENANT_MIN_TRUST=medium``.
        """
        if isinstance(value, str) and not value.isdigit():
            try:
                return TenantTrust[value.upper()]
            except KeyError as exc:
                legal = ", ".join(t.name.lower() for t in TenantTrust)
                raise ValueError(
                    f"Invalid VARCO_TENANT_MIN_TRUST={value!r}. Legal values are: {legal}."
                ) from exc
        return value

    # Plan 051 / DI-5, §D-tenant: the unknown-source-name guard moved to
    # build_tenant_source_chain() — only the builder can see the contributed
    # TenantSource pool, so only it can say whether an unrecognised name is
    # truly unknown or is a legitimately bound pool source. Constructing
    # TenantProvenanceSettings(sources=("partner",)) no longer raises here.

    @model_validator(mode="after")
    def _check_subdomain_requires_base_domains(self) -> TenantProvenanceSettings:
        """Mirror the old ``from_env()``'s ``subdomain`` + no-base-domains guard."""
        if "subdomain" in self.sources and not self.base_domains:
            raise ValueError(
                "VARCO_TENANT_SOURCES includes 'subdomain' but VARCO_TENANT_BASE_DOMAINS "
                "is unset. A subdomain source requires at least one explicit base domain "
                "(§D-S6-oq3) — set VARCO_TENANT_BASE_DOMAINS."
            )
        return self


def build_tenant_source_chain(
    settings: TenantProvenanceSettings | None = None,
    *,
    delegation_policy: DelegationPolicy | None = None,
    sources: Iterable[TenantSource] = (),
) -> TenantSourceChain | None:
    """
    Build a ``TenantSourceChain`` from ``TenantProvenanceSettings``, or
    ``None`` when nothing is configured.

    Plan 051 / DI-5, §D-tenant: ``sources=`` is the contributed
    ``TenantSource`` pool (typically ``collect(container, TenantSource)``,
    see ``resolve_tenant_source_chain``) — a plugin's out-of-tree source
    joins the chain by being *named* in ``settings.sources``
    (``VARCO_TENANT_SOURCES``); being bound in the pool alone is inert. The
    allowlist and the order both stay the operator's: chain membership and
    order = ``settings.sources``, name by name — a built-in name
    (``jwt``/``subdomain``/``legacy``/``act_as``) always constructs the
    built-in exactly as before; any other name must match exactly one pool
    source's ``name``.

    Args:
        settings: The settings to build from. ``None`` reads the real
            process environment via ``TenantProvenanceSettings.from_env()``.
        delegation_policy: Phase 6 only — a ``DelegationPolicy`` to bind to
            an ``act_as`` source. Required if ``"act_as"`` is in
            ``settings.sources``.
        sources: The contributed ``TenantSource`` pool. A pool source not
            named in ``settings.sources`` is inert (never an error) — bound
            ≠ active.

    Returns:
        A ``TenantSourceChain``, or ``None`` when ``VARCO_TENANT_SOURCES``
        (or ``settings.sources``) is empty — nothing changes, regardless of
        the pool.

    Raises:
        ValueError: ``"act_as"`` is requested without a ``delegation_policy``;
            a pool source's ``name`` collides with a built-in name (checked
            even if that built-in is not itself active); two pool sources
            share one ``name``; or a name in ``settings.sources`` matches
            neither a built-in nor a pool source.
    """
    from varco_core.tenancy.source import TenantSourceChain
    from varco_core.tenancy.sources import (
        JwtClaimTenantSource,
        LegacyTenantSource,
        SubdomainTenantSource,
    )

    resolved = settings if settings is not None else TenantProvenanceSettings.from_env()

    pool_by_name: dict[str, TenantSource] = {}
    for source in sources:
        if source.name in pool_by_name:
            raise ValueError(
                f"Two contributed TenantSource instances share the name {source.name!r} — "
                "pool source names must be unique."
            )
        pool_by_name[source.name] = source

    colliding = pool_by_name.keys() & set(_BUILTIN_SOURCE_NAMES)
    if colliding:
        name = sorted(colliding)[0]
        raise ValueError(
            f"A contributed TenantSource is named {name!r}, which collides with a "
            f"built-in source name ({', '.join(_BUILTIN_SOURCE_NAMES)}) — shadowing a "
            "built-in source name is not allowed; pick another name and omit the "
            "built-in from VARCO_TENANT_SOURCES."
        )

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
                    reserved_labels=frozenset(resolved.reserved_labels),
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
        elif name in pool_by_name:
            built.append(pool_by_name[name])
        else:
            bound = ", ".join(sorted(pool_by_name)) or "(none)"
            raise ValueError(
                f"unknown tenant source {name!r}; built-ins: "
                f"{', '.join(_BUILTIN_SOURCE_NAMES)}; bound: {bound}"
            )

    return TenantSourceChain(
        sources=tuple(built), mode=resolved.cross_check, min_trust=resolved.min_trust
    )
