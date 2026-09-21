"""
Unit tests for varco_casbin.di
==============================
Covers DI wiring: the engine binds to both PolicyEngine and PolicyManagement,
settings resolve via the provider, and the authorizer is truly opt-in
(scan/bootstrap must NOT bind it; enable_policy_authorizer must).
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_casbin.config import CasbinSettings
from varco_casbin.di import bootstrap, enable_policy_authorizer
from varco_casbin.engine import CasbinPolicyEngine
from varco_core.auth import AbstractAuthorizer, PolicyEngine, PolicyManagement
from varco_core.auth.authorizer import BaseAuthorizer
from varco_core.di import AlreadyBoundError


async def _fresh() -> DIContainer:
    """A container with varco_casbin bootstrapped and the BaseAuthorizer fallback."""
    container = bootstrap(DIContainer())
    # Register the permissive fallback so AbstractAuthorizer resolves pre-opt-in.
    container.scan("varco_core.auth.authorizer")
    return container


async def test_engine_binds_to_both_interfaces() -> None:
    """One CasbinPolicyEngine instance serves PolicyEngine and PolicyManagement."""
    c = await _fresh()
    engine = await c.aget(PolicyEngine)
    mgmt = await c.aget(PolicyManagement)
    assert isinstance(engine, CasbinPolicyEngine)
    assert engine is mgmt
    await c.ashutdown()


async def test_settings_resolve_via_provider() -> None:
    """CasbinSettings resolves through the bootstrap-registered provider."""
    c = await _fresh()
    settings = await c.aget(CasbinSettings)
    assert isinstance(settings, CasbinSettings)
    await c.ashutdown()


async def test_authorizer_is_opt_in() -> None:
    """Bootstrap alone must not shadow the fallback; enable_* flips it."""
    c = await _fresh()

    # Pre-opt-in: the permissive fallback wins.
    before = await c.aget(AbstractAuthorizer)
    assert isinstance(before, BaseAuthorizer)

    # Opt in: the policy authorizer now shadows the fallback.
    enable_policy_authorizer(c)
    after = await c.aget(AbstractAuthorizer)
    assert type(after).__name__ == "PolicyEngineAuthorizer"
    await c.ashutdown()


async def test_bootstrap_twice_registers_one_settings_binding() -> None:
    """Plan 046 §D-contract — a second bootstrap() must not re-register
    _provide_casbin_settings (the one direct provide() inside bootstrap())."""
    c = DIContainer()
    bootstrap(c)
    before = len(c.get_all_bindings(CasbinSettings))

    bootstrap(c)

    assert len(c.get_all_bindings(CasbinSettings)) == before
    await c.ashutdown()


async def test_enable_policy_authorizer_twice_raises() -> None:
    c = await _fresh()
    enable_policy_authorizer(c)

    with pytest.raises(AlreadyBoundError):
        enable_policy_authorizer(c)

    await c.ashutdown()


async def test_enable_policy_authorizer_after_app_binding_raises() -> None:
    """§D-enable — the app's own non-fallback AbstractAuthorizer also blocks it."""
    from providify import Provider

    c = await _fresh()

    @Provider(singleton=True, priority=1000)
    def _app_authorizer() -> AbstractAuthorizer:
        return BaseAuthorizer()

    c.provide(_app_authorizer)

    with pytest.raises(AlreadyBoundError):
        enable_policy_authorizer(c)

    await c.ashutdown()


async def test_post_construct_starts_engine() -> None:
    """The engine resolved via DI is already started (enforce works)."""
    from varco_core.auth.policy import EnforcementRequest as ER

    c = await _fresh()
    engine = await c.aget(PolicyEngine)
    await engine.add_policy("admin", "*", "*")
    await engine.add_role_for_user("alice", "admin")
    assert await engine.enforce(ER("alice", "posts", "read")) is True
    await c.ashutdown()
