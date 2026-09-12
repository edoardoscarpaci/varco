"""
varco_core.tenancy.di
=======================
Providify DI integration for tenant membership (Plan 033 / S5).

Mirrors ``varco_core.flags.di``'s ``enable_feature_flags`` precedent exactly:
``NullTenantMembership`` is bound by default (a scanned ``@Singleton`` at
the lowest priority — see ``membership.py``), and
``enable_tenant_membership(container)`` is the only way to swap in
``ClaimTenantMembership``. This is deliberately **not** a scanned
``@Configuration`` — ``scan`` auto-activates those, which would silently
bind a membership provider (and start enforcing it) in every app that
scans ``varco_core`` recursively.

Usage::

    from providify import DIContainer
    from varco_core.tenancy.di import enable_tenant_membership
    from varco_core.tenancy.membership import AbstractTenantMembership

    container = DIContainer()
    container.scan("varco_core", recursive=True)  # NullTenantMembership bound
    enable_tenant_membership(container)             # opt-in: ClaimTenantMembership

    membership = await container.aget(AbstractTenantMembership)  # ClaimTenantMembership

Custom configuration — either construct ``TenantMembershipSettings``
directly, or set ``VARCO_TENANT_MEMBERSHIP_CLAIM``/
``VARCO_TENANT_MEMBERSHIP_ON_MISSING`` and let ``settings=None`` (the
default) read them via ``TenantMembershipSettings.from_env()``::

    from varco_core.tenancy.membership import MissingClaimPolicy, TenantMembershipSettings

    enable_tenant_membership(
        container,
        settings=TenantMembershipSettings(
            claim_key="organizations", on_missing_claim=MissingClaimPolicy.DENY
        ),
    )
"""

from __future__ import annotations

from typing import Any

from providify import Provider

from varco_core.tenancy.membership import (
    AbstractTenantMembership,
    ClaimTenantMembership,
    TenantMembershipSettings,
)

__all__ = ["enable_tenant_membership"]


def _build_claim_membership_provider(settings: TenantMembershipSettings) -> Any:
    """Build a module-level `@Provider` function closing over the given settings."""

    @Provider(singleton=True)
    def _provide_claim_tenant_membership() -> AbstractTenantMembership:
        """
        Module-level provider binding ``ClaimTenantMembership`` as the app
        ``AbstractTenantMembership``.

        Module-level so ``scan`` does NOT auto-register it — it activates
        only when ``enable_tenant_membership`` passes it to
        ``container.provide``.
        """
        return ClaimTenantMembership(
            claim_key=settings.claim_key, on_missing_claim=settings.on_missing_claim
        )

    return _provide_claim_tenant_membership


def enable_tenant_membership(
    container: Any,
    settings: TenantMembershipSettings | None = None,
) -> Any:
    """
    Opt in to ``ClaimTenantMembership`` as the application's
    ``AbstractTenantMembership``, shadowing the always-allow
    ``NullTenantMembership`` default.

    Args:
        container: The ``DIContainer`` already scanned via
            ``container.scan("varco_core", recursive=True)``.
        settings: A ``TenantMembershipSettings``, or ``None`` (default) to
            build one via ``TenantMembershipSettings.from_env()`` —
            ``VARCO_TENANT_MEMBERSHIP_CLAIM`` (default ``"tenants"``) and
            ``VARCO_TENANT_MEMBERSHIP_ON_MISSING`` (default ``"allow"``).

    Returns:
        The same container, for chaining.

    Example::

        container = DIContainer()
        container.scan("varco_core", recursive=True)
        enable_tenant_membership(container)
    """
    resolved = settings if settings is not None else TenantMembershipSettings.from_env()
    container.provide(_build_claim_membership_provider(resolved))
    return container
