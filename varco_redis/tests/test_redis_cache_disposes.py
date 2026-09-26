"""
Plan 024 / Phase 1, Steps 13 + 16 — `@Disposes` teardown for the two Redis
cache configurations (`RedisCacheConfiguration`, `RedisLayeredCacheConfiguration`).

Plan 045 / Step 6: the P24-DISPOSES-FIRSTMATCH guard is CLOSED —
providify 2.1.0 scopes `@Disposes` wiring to the bindings the same
`install()`/`ainstall()` registered (`providify/CHANGELOG.md:14-24`), so
installing both configurations no longer overwrites the first module's
disposer. This module now asserts three passing contracts: both
provider-produced `CacheBackend` instances are stopped on `ashutdown()`;
`validate()` reports zero `UNMATCHED_DISPOSER`/`DISPOSER_OVERWRITTEN`
issues; and `DIContainer.provide()` returns the `ProviderBinding` it
registered (`providify/CHANGELOG.md:27-29`).

RED MODE (before the pin bump, still resolving providify 2.0.1): the
`validate()` assertions fail with `AttributeError` because
`IssueKind.UNMATCHED_DISPOSER`/`DISPOSER_OVERWRITTEN` do not exist yet;
`test_provide_returns_the_provider_binding` fails because `provide()`
still returns `None`.

No Docker required — Redis I/O is monkeypatched to no-ops.
"""

from __future__ import annotations

from typing import Any

import pytest
from providify import DIContainer, IssueKind
from varco_core.cache.base import CacheBackend
from varco_core.settings import bind_settings
from varco_redis.cache import (
    RedisCache,
    RedisCacheConfiguration,
    RedisCacheSettings,
)


async def _noop_start(self: Any) -> None:
    # No real Redis connection — proves the disposer, not connectivity.
    self._redis = object()


def _make_recorder() -> tuple[list[str], Any]:
    calls: list[str] = []

    async def _record_stop(self: Any) -> None:
        calls.append("stopped")
        self._redis = None

    return calls, _record_stop


async def test_redis_cache_configuration_reports_zero_unreachable_pre_destroy_issues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Tier-A mechanism-level assertion (§D-C2-test) — providify's own
    # detector must see a disposer wired for RedisCache's @PreDestroy stop().
    monkeypatch.setattr(RedisCache, "start", _noop_start)
    calls, record_stop = _make_recorder()
    monkeypatch.setattr(RedisCache, "stop", record_stop)

    container = DIContainer()
    bind_settings(container, RedisCacheSettings)
    await container.ainstall(RedisCacheConfiguration)
    await container.aget(CacheBackend)

    report = container.validate(raise_on_error=False)
    # Force the attribute lookup unconditionally — a list-comprehension
    # guard (`if i.kind == IssueKind.UNREACHABLE_PRE_DESTROY`) would never
    # even evaluate the RHS when report.issues is empty, silently turning
    # this into a vacuous pass on a pre-2.0.1 providify that has never heard
    # of the kind at all.
    target_kind = IssueKind.UNREACHABLE_PRE_DESTROY
    unreachable = [i for i in report.issues if i.kind == target_kind]
    assert unreachable == []

    await container.ashutdown()


async def test_redis_cache_stop_runs_on_ashutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    # Behavioural round-trip — the actual defect: stop() must fire.
    monkeypatch.setattr(RedisCache, "start", _noop_start)
    calls, record_stop = _make_recorder()
    monkeypatch.setattr(RedisCache, "stop", record_stop)

    container = DIContainer()
    bind_settings(container, RedisCacheSettings)
    await container.ainstall(RedisCacheConfiguration)
    await container.aget(CacheBackend)
    await container.ashutdown()

    assert calls == ["stopped"]


def test_provide_returns_the_provider_binding() -> None:
    # providify 2.1.0: DIContainer.provide() now returns the ProviderBinding
    # it registered (was None) — providify/CHANGELOG.md:27-29. NOTE:
    # `ProviderBinding` is not re-exported from the `providify` top-level
    # package (`providify/__init__.py` __all__ has no such name) — the
    # installed 2.1.0 shape requires importing it from `providify.binding`
    # directly. Corrected here from the test-writer's `from providify import
    # ProviderBinding`, which does not match the shipped contract.
    from providify.binding import ProviderBinding

    def factory() -> CacheBackend:
        return RedisCache.__new__(RedisCache)  # never started; binding shape only

    container = DIContainer()
    # `provide()` requires either an `@Provider`-decorated factory or an
    # explicit `returns=` override (`ProviderBindingNotDecoratedError`
    # otherwise, per `providify/binding.py`'s undecorated-factory guard) —
    # `returns=` is the documented spelling for registering a plain,
    # undecorated factory someone else wrote.
    binding = container.provide(factory, returns=CacheBackend)

    assert isinstance(binding, ProviderBinding)


async def test_flipping_cache_backend_env_after_resolution_does_not_evict_singleton(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Plan 047 §D-tests: "resolve under redis, flip the variable to layered,
    # resolve again (lazy predicate; the first singleton is NOT evicted,
    # scope.py:1048-1053) -> both instantiated -> ashutdown() stops both —
    # the same P24 contract."
    monkeypatch.setattr(RedisCache, "start", _noop_start)
    stopped: list[str] = []

    async def _record_redis_stop(self: Any) -> None:
        stopped.append("redis")
        self._redis = None

    monkeypatch.setattr(RedisCache, "stop", _record_redis_stop)

    from varco_core.cache.layered import LayeredCache

    async def _layered_start(self: Any) -> None:
        self._l1_started = True
        self._l2_started = True

    async def _record_layered_stop(self: Any) -> None:
        stopped.append("layered")

    monkeypatch.setattr(LayeredCache, "start", _layered_start)
    monkeypatch.setattr(LayeredCache, "stop", _record_layered_stop)

    from varco_core.cache.base import CacheBackend
    from varco_core.di import bootstrap as core_bootstrap
    from varco_redis.di import bootstrap as redis_bootstrap

    container = DIContainer()
    core_bootstrap(container)
    monkeypatch.setenv("VARCO_CACHE_BACKEND", "redis")
    redis_bootstrap(container)

    first = await container.aget(CacheBackend)
    assert isinstance(first, RedisCache)

    monkeypatch.setenv("VARCO_CACHE_BACKEND", "layered")
    second = await container.aget(CacheBackend)

    # The first singleton is not evicted by the env flip (lazy @Requires
    # predicate re-evaluated on the NEXT get(), not a cache invalidation).
    assert isinstance(second, LayeredCache)

    await container.ashutdown()

    assert "redis" in stopped
    assert "layered" in stopped
