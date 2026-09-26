"""
Plan 024 / Phase 1, Step 15 — `@Disposes` teardown for `MemcachedCacheConfiguration`.

RED MODE: `MemcachedCacheConfiguration` has no `@Disposes(CacheBackend)` method
yet. `MemcachedCache.stop()` is only reachable via `@PreDestroy`, which
providify never calls on a `@Provider`-produced instance. Expected failures:

- The `validate()` assertion fails with `AttributeError` because the pinned
  providify (Step 12 not yet applied) has no `IssueKind.UNREACHABLE_PRE_DESTROY`.
- The round-trip assertion fails because `ashutdown()` never calls
  `MemcachedCache.stop()` for a provider-produced instance.

No Docker required — Memcached I/O is monkeypatched to no-ops.
"""

from __future__ import annotations

from typing import Any

import pytest
from providify import DIContainer, IssueKind
from varco_core.cache.base import CacheBackend
from varco_memcached.cache import MemcachedCache, MemcachedCacheConfiguration


async def _noop_start(self: Any) -> None:
    self._client = object()


async def test_memcached_cache_configuration_reports_zero_unreachable_pre_destroy_issues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(MemcachedCache, "start", _noop_start)

    async def _record_stop(self: Any) -> None:
        self._client = None

    monkeypatch.setattr(MemcachedCache, "stop", _record_stop)

    container = DIContainer()
    from varco_memcached.di import bootstrap

    bootstrap(container)
    await container.ainstall(MemcachedCacheConfiguration)
    await container.aget(CacheBackend)

    report = container.validate(raise_on_error=False)
    # Force the attribute lookup unconditionally — see the redis-cache sibling
    # test for why a comprehension guard would vacuously pass here.
    target_kind = IssueKind.UNREACHABLE_PRE_DESTROY
    unreachable = [i for i in report.issues if i.kind == target_kind]
    assert unreachable == []

    await container.ashutdown()


async def test_memcached_cache_stop_runs_on_ashutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(MemcachedCache, "start", _noop_start)
    calls: list[str] = []

    async def _record_stop(self: Any) -> None:
        calls.append("stopped")
        self._client = None

    monkeypatch.setattr(MemcachedCache, "stop", _record_stop)

    container = DIContainer()
    from varco_memcached.di import bootstrap

    bootstrap(container)
    await container.ainstall(MemcachedCacheConfiguration)
    await container.aget(CacheBackend)
    await container.ashutdown()

    assert calls == ["stopped"]
