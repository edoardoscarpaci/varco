"""
Tests for TenantUoWProvider with SQLAlchemy backends.

These tests verify that TenantUoWProvider correctly routes make_uow()
calls to the appropriate SQLAlchemyRepositoryProvider based on the
active tenant context.

The SQLAlchemyRepositoryProvider instances are mocked so no real SA
session or SQLite database is needed.  The SA provider's own behaviour
(register, get_repository, make_uow internals) is already covered in
test_sa_provider.py — this file focuses exclusively on the routing layer.

Coverage:
- Static two-tenant setup routes to the correct SA provider
- Runtime registration of a new SA tenant
- After registration, make_uow() immediately delegates to the new provider
- Only one SA provider is called per request (no cross-contamination)

Thread safety:  N/A (unit tests)
Async safety:   N/A (TenantUoWProvider.make_uow and register are synchronous)
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from varco_core.service.tenant import TenantUoWProvider, tenant_context
from varco_sa.provider import SQLAlchemyRepositoryProvider


# ── Helpers ───────────────────────────────────────────────────────────────────


def _mock_sa_provider() -> MagicMock:
    """
    Return a MagicMock with the SQLAlchemyRepositoryProvider spec.

    MagicMock is sufficient because TenantUoWProvider only calls make_uow()
    on the backing provider — no SA model generation happens here.

    Returns:
        A MagicMock configured with the SQLAlchemyRepositoryProvider spec.
    """
    # Using spec= ensures that attribute access on the mock is validated
    # against the real class — accidental typos in call sites are caught.
    return MagicMock(spec=SQLAlchemyRepositoryProvider)


# ── Static setup ──────────────────────────────────────────────────────────────


def test_routes_to_acme_sa_provider_when_acme_tenant_is_active() -> None:
    """
    make_uow() must delegate to the acme SA provider when tenant is 'acme'.

    Verifies the happy path for static two-tenant setup.
    """
    acme_provider = _mock_sa_provider()
    globex_provider = _mock_sa_provider()

    p = TenantUoWProvider({"acme": acme_provider, "globex": globex_provider})

    with tenant_context("acme"):
        p.make_uow()

    acme_provider.make_uow.assert_called_once()
    # globex provider must not be touched — isolation between tenants.
    globex_provider.make_uow.assert_not_called()


def test_routes_to_globex_sa_provider_when_globex_tenant_is_active() -> None:
    """make_uow() must delegate to the globex SA provider when tenant is 'globex'."""
    acme_provider = _mock_sa_provider()
    globex_provider = _mock_sa_provider()

    p = TenantUoWProvider({"acme": acme_provider, "globex": globex_provider})

    with tenant_context("globex"):
        p.make_uow()

    globex_provider.make_uow.assert_called_once()
    acme_provider.make_uow.assert_not_called()


def test_sequential_requests_for_different_tenants_each_hit_own_sa_provider() -> None:
    """
    Sequential requests for 'acme' and 'globex' must each call only their
    own SA provider — no cross-tenant contamination between requests.
    """
    acme_provider = _mock_sa_provider()
    globex_provider = _mock_sa_provider()

    p = TenantUoWProvider({"acme": acme_provider, "globex": globex_provider})

    with tenant_context("acme"):
        p.make_uow()
    with tenant_context("globex"):
        p.make_uow()

    # Each called exactly once — no double-dispatch or cross-call.
    acme_provider.make_uow.assert_called_once()
    globex_provider.make_uow.assert_called_once()


# ── Runtime registration ──────────────────────────────────────────────────────


def test_runtime_registered_sa_provider_is_immediately_routable() -> None:
    """
    A provider registered via register() after construction must be
    immediately reachable via make_uow() without restarting the server.

    This is the dynamic provisioning contract — new tenants can be onboarded
    while the application is running.
    """
    p = TenantUoWProvider()  # start empty

    new_tenant_provider = _mock_sa_provider()
    p.register("startup_tenant", new_tenant_provider)

    with tenant_context("startup_tenant"):
        p.make_uow()

    new_tenant_provider.make_uow.assert_called_once()


def test_replacing_sa_provider_at_runtime_routes_to_new_provider() -> None:
    """
    Calling register() with an existing tenant ID replaces the provider.
    The new provider receives subsequent make_uow() calls; the old one does not.
    """
    old_provider = _mock_sa_provider()
    new_provider = _mock_sa_provider()

    p = TenantUoWProvider({"acme": old_provider})
    p.register("acme", new_provider)  # hot-swap

    with tenant_context("acme"):
        p.make_uow()

    new_provider.make_uow.assert_called_once()
    old_provider.make_uow.assert_not_called()


def test_make_uow_raises_for_sa_tenant_not_yet_registered() -> None:
    """
    make_uow() must raise KeyError when the active tenant has no SA provider.

    This can happen during startup before register() has been called, or
    if the tenant ID in the token is invalid.
    """
    p = TenantUoWProvider({"acme": _mock_sa_provider()})

    with tenant_context("unknown_sa_tenant"):
        with pytest.raises(KeyError, match="unknown_sa_tenant"):
            p.make_uow()
