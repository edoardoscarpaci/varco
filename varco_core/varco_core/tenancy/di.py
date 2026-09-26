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

from typing import TYPE_CHECKING, Any

from providify import Fallback, Provider

from varco_core.di import BACKEND_PRIORITY
from varco_core.di.features import feature_default
from varco_core.tenancy.membership import (
    AbstractTenantMembership,
    ClaimTenantMembership,
    TenantMembershipSettings,
)

if TYPE_CHECKING:
    from providify import DIContainer

__all__ = ["enable_tenant_membership"]


@Fallback
@feature_default("tenant_membership")
@Provider(singleton=True, priority=BACKEND_PRIORITY)
def _tenant_membership_when_enabled() -> AbstractTenantMembership:
    """
    Plan 047 §D-features — the env twin of ``enable_tenant_membership()``.

    **Repair round, Decision 4:** a **private**, hand-registered
    ``@Provider`` function — mirrors ``varco_core.revocation.di``'s
    ``_in_memory_revocation_store_when_selected`` shape (NOT the scanned
    shape this docstring used to point at; corrected by Decision 4).
    ``varco_core.di.bootstrap()`` registers it explicitly via
    ``container.provide()`` (guarded by the same per-container idempotency
    set the revocation twin uses) rather than relying on ``container.
    scan()`` to auto-discover a public module-level provider — all three
    feature twins in this repo now share one registration shape.

    Reads ``TenantMembershipSettings.from_env()`` directly (the same
    shape ``varco_core.flags.di._feature_flags_when_enabled`` uses) rather
    than injecting ``Inject[TenantMembershipSettings]`` — the plan's own
    text floats DI-injecting the settings, but doing so would require a
    new `@Fallback` `TenantMembershipSettings` default binding, which
    every already-landed per-package `expected_defaults` conformance
    list (Plan 046, unmodified by this plan) does not include; adding one
    here would break every one of those lists for no behavioural gain,
    since `enable_tenant_membership()` itself already reads the same
    `from_env()` default when no explicit `settings=` is given.

    Gated by ``@feature_default("tenant_membership")`` — inert unless
    ``VARCO_TENANT_MEMBERSHIP_ENABLED`` is truthy.

    Returns:
        A ``ClaimTenantMembership`` built from ``TenantMembershipSettings.from_env()``.
    """
    settings = TenantMembershipSettings.from_env()
    return ClaimTenantMembership(
        claim_key=settings.claim_key, on_missing_claim=settings.on_missing_claim
    )


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
    container: DIContainer,
    settings: TenantMembershipSettings | None = None,
) -> DIContainer:
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

    Raises:
        AlreadyBoundError: A non-fallback ``AbstractTenantMembership``
            binding already exists — either the application's own, or a
            previous call to this function (§D-enable).

    Example::

        container = DIContainer()
        container.scan("varco_core", recursive=True)
        enable_tenant_membership(container)
    """
    from varco_core.di import AlreadyBoundError, is_user_bound  # noqa: PLC0415

    if is_user_bound(container, AbstractTenantMembership):
        raise AlreadyBoundError(
            "A non-fallback AbstractTenantMembership binding already exists — "
            "enable_tenant_membership() shadows the @Fallback NullTenantMembership "
            "default and cannot register a second competing binding."
        )
    resolved = settings if settings is not None else TenantMembershipSettings.from_env()
    container.provide(_build_claim_membership_provider(resolved))
    return container
