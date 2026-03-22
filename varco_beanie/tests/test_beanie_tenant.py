"""
Tests for TenantUoWProvider with Beanie backends.

These tests verify that TenantUoWProvider correctly routes make_uow()
calls to the appropriate BeanieRepositoryProvider based on the active
tenant context.

The BeanieRepositoryProvider instances are mocked so no real Motor client
or MongoDB connection is needed.  The Beanie provider's own behaviour
(register, init, make_uow internals) is already covered in
test_beanie_provider.py — this file focuses exclusively on the routing layer.

Coverage:
- Static two-tenant setup routes to the correct Beanie provider
- Runtime registration of a new Beanie tenant (e.g. after init() completes)
- After registration, make_uow() immediately delegates to the new provider
- Only one Beanie provider is called per request (no cross-contamination)
- Mixed SA + Beanie backends can coexist in the same TenantUoWProvider

Thread safety:  N/A (unit tests)
Async safety:   N/A (TenantUoWProvider.make_uow and register are synchronous)
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from varco_core.providers import RepositoryProvider
from varco_core.service.tenant import TenantUoWProvider, tenant_context
from varco_beanie.provider import BeanieRepositoryProvider


# ── Helpers ───────────────────────────────────────────────────────────────────


def _mock_beanie_provider() -> MagicMock:
    """
    Return a MagicMock with the BeanieRepositoryProvider spec.

    MagicMock is sufficient because TenantUoWProvider only calls make_uow()
    on the backing provider — no Beanie Document generation or Motor connection
    is exercised in these routing tests.

    Returns:
        A MagicMock configured with the BeanieRepositoryProvider spec.
    """
    return MagicMock(spec=BeanieRepositoryProvider)


# ── Static setup ──────────────────────────────────────────────────────────────


def test_routes_to_acme_beanie_provider_when_acme_tenant_is_active() -> None:
    """
    make_uow() must delegate to the acme Beanie provider when tenant is 'acme'.

    Verifies the happy path for a static two-tenant Beanie setup.
    """
    acme_provider = _mock_beanie_provider()
    globex_provider = _mock_beanie_provider()

    p = TenantUoWProvider({"acme": acme_provider, "globex": globex_provider})

    with tenant_context("acme"):
        p.make_uow()

    acme_provider.make_uow.assert_called_once()
    # globex provider must not be touched — tenant isolation.
    globex_provider.make_uow.assert_not_called()


def test_routes_to_globex_beanie_provider_when_globex_tenant_is_active() -> None:
    """make_uow() must delegate to the globex Beanie provider when tenant is 'globex'."""
    acme_provider = _mock_beanie_provider()
    globex_provider = _mock_beanie_provider()

    p = TenantUoWProvider({"acme": acme_provider, "globex": globex_provider})

    with tenant_context("globex"):
        p.make_uow()

    globex_provider.make_uow.assert_called_once()
    acme_provider.make_uow.assert_not_called()


def test_sequential_beanie_requests_for_different_tenants_are_isolated() -> None:
    """
    Sequential requests for 'acme' and 'globex' must each call only their
    own Beanie provider.
    """
    acme_provider = _mock_beanie_provider()
    globex_provider = _mock_beanie_provider()

    p = TenantUoWProvider({"acme": acme_provider, "globex": globex_provider})

    with tenant_context("acme"):
        p.make_uow()
    with tenant_context("globex"):
        p.make_uow()

    acme_provider.make_uow.assert_called_once()
    globex_provider.make_uow.assert_called_once()


# ── Runtime registration ──────────────────────────────────────────────────────


def test_runtime_registered_beanie_provider_is_immediately_routable() -> None:
    """
    A Beanie provider registered after construction (e.g. after init() completes)
    must be immediately reachable via make_uow().

    This matches the real workflow: create TenantUoWProvider early, then call
    provider.init() asynchronously, then register() once init() has completed.
    """
    p = TenantUoWProvider()  # start empty

    new_provider = _mock_beanie_provider()
    p.register("new_db_tenant", new_provider)

    with tenant_context("new_db_tenant"):
        p.make_uow()

    new_provider.make_uow.assert_called_once()


def test_replacing_beanie_provider_at_runtime_routes_to_new_provider() -> None:
    """
    Calling register() with an existing tenant ID replaces the Beanie provider.

    This is useful when a tenant's MongoDB database is migrated to a new cluster.
    """
    old_provider = _mock_beanie_provider()
    new_provider = _mock_beanie_provider()

    p = TenantUoWProvider({"acme": old_provider})
    p.register("acme", new_provider)  # hot-swap

    with tenant_context("acme"):
        p.make_uow()

    new_provider.make_uow.assert_called_once()
    old_provider.make_uow.assert_not_called()


def test_make_uow_raises_for_beanie_tenant_not_yet_registered() -> None:
    """
    make_uow() must raise KeyError when the active tenant has no Beanie provider.

    This can happen if the tenant's init() hasn't completed yet, or if the
    tenant ID in the JWT claim is invalid.
    """
    p = TenantUoWProvider({"acme": _mock_beanie_provider()})

    with tenant_context("unregistered_mongo_tenant"):
        with pytest.raises(KeyError, match="unregistered_mongo_tenant"):
            p.make_uow()


# ── Mixed SA + Beanie backends ────────────────────────────────────────────────


def test_mixed_sa_and_beanie_backends_coexist_in_same_provider() -> None:
    """
    TenantUoWProvider is backend-agnostic — an SA provider for one tenant
    and a Beanie provider for another can coexist in the same routing table.

    Each make_uow() call hits only the correct backend for the active tenant.
    """
    # Use a generic RepositoryProvider spec to represent an SA provider
    # without importing SQLAlchemy in this Beanie test file.
    sa_provider = MagicMock(spec=RepositoryProvider)
    beanie_provider = _mock_beanie_provider()

    p = TenantUoWProvider({"sql_tenant": sa_provider, "mongo_tenant": beanie_provider})

    with tenant_context("sql_tenant"):
        p.make_uow()
    with tenant_context("mongo_tenant"):
        p.make_uow()

    sa_provider.make_uow.assert_called_once()
    beanie_provider.make_uow.assert_called_once()
