"""
Unit tests for varco_core.flags.di (Plan 032 / D7).

Mirrors varco_casbin.di's enable_policy_authorizer precedent: NullFeatureFlags
is bound by default (nothing changes for an app that never opts in), and
enable_feature_flags(container) is the only way to swap in a real backend.
"""

from __future__ import annotations

from providify import DIContainer
from varco_core.flags import AbstractFeatureFlags, InMemoryFeatureFlags, NullFeatureFlags
from varco_core.flags.di import enable_feature_flags


async def test_null_feature_flags_bound_by_default() -> None:
    container = DIContainer()
    container.scan("varco_core.flags", recursive=True)
    flags = await container.aget(AbstractFeatureFlags)
    assert isinstance(flags, NullFeatureFlags)
    await container.ashutdown()


async def test_enable_feature_flags_swaps_the_default() -> None:
    container = DIContainer()
    container.scan("varco_core.flags", recursive=True)

    # Pre-opt-in: the no-op default resolves.
    before = await container.aget(AbstractFeatureFlags)
    assert isinstance(before, NullFeatureFlags)

    enable_feature_flags(container)
    after = await container.aget(AbstractFeatureFlags)
    assert isinstance(after, InMemoryFeatureFlags)
    await container.ashutdown()


async def test_feature_flags_env_enabled_activates_the_twin(monkeypatch) -> None:
    # Plan 047 §D-features — VARCO_FEATURE_FLAGS_ENABLED=true, no enable_*
    # call, gated via feature_default("feature_flags"). Repair round,
    # Decision 4: the twin is hand-registered by bootstrap() (mirroring
    # the revocation twin), not scan-discovered — use bootstrap(), not a
    # bare scan().
    from varco_core.di import bootstrap

    monkeypatch.setenv("VARCO_FEATURE_FLAGS_ENABLED", "true")
    container = DIContainer()
    bootstrap(container)

    flags = await container.aget(AbstractFeatureFlags)
    assert isinstance(flags, InMemoryFeatureFlags)
    await container.ashutdown()


async def test_feature_flags_env_disabled_keeps_null_default(monkeypatch) -> None:
    monkeypatch.setenv("VARCO_FEATURE_FLAGS_ENABLED", "false")
    container = DIContainer()
    container.scan("varco_core", recursive=True)

    flags = await container.aget(AbstractFeatureFlags)
    assert isinstance(flags, NullFeatureFlags)
    await container.ashutdown()
