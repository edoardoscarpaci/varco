"""
Tests for Plan 041 / S22, §D-S22-loop/§D-S22-failure/§D-S22-interval —
``TrustedIssuerRegistry.start_refresh()`` / ``stop_refresh()``: a background
task that periodically calls ``_refresh_all_sources()`` without ever needing
a ``verify()`` call.

RED until Phase 2 Step 12 adds ``start_refresh``/``stop_refresh``/
``_refresh_loop``/``refresh_running``/``refresh_interval`` to
``TrustedIssuerRegistry``.

House rule (CLAUDE.md §Test Conventions): increase the sleep margin rather
than mark a timing-sensitive test xfail.
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from varco_core.authority.registry import TrustedIssuerRegistry
from varco_core.jwk.model import JsonWebKeySet


class _CountingKeySource:
    """A fake IssuerSource whose refresh() counts calls and never fails."""

    def __init__(self) -> None:
        self.refresh_calls = 0
        self.load_calls = 0

    @property
    def source_id(self) -> str:
        return "fake::counting"

    async def load(self) -> JsonWebKeySet:
        self.load_calls += 1
        return JsonWebKeySet(keys=())

    async def refresh(self) -> JsonWebKeySet:
        self.refresh_calls += 1
        return JsonWebKeySet(keys=())


class _AlwaysFailingKeySource:
    """A fake IssuerSource whose refresh() always raises."""

    def __init__(self) -> None:
        self.refresh_calls = 0

    @property
    def source_id(self) -> str:
        return "fake::always-failing"

    async def load(self) -> JsonWebKeySet:
        return JsonWebKeySet(keys=())

    async def refresh(self) -> JsonWebKeySet:
        self.refresh_calls += 1
        raise RuntimeError("simulated JWKS endpoint outage")


async def _wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.01) -> None:
    """Poll ``predicate`` with a generous margin instead of a fixed sleep."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    assert predicate(), f"condition not met within {timeout}s"


class TestStartRefreshNoTask:
    async def test_effective_period_zero_creates_no_task(self) -> None:
        registry = TrustedIssuerRegistry(ttl_seconds=0.0)
        source = _CountingKeySource()
        registry.register("FAKE", "fake-iss", source)  # type: ignore[arg-type]

        await registry.start_refresh()

        assert registry._refresh_task is None
        assert registry.refresh_running is False

        # No task means no ticking at all — give it a beat to prove nothing fires.
        await asyncio.sleep(0.05)
        assert source.refresh_calls == 0

    async def test_negative_period_creates_no_task(self) -> None:
        registry = TrustedIssuerRegistry(ttl_seconds=0.0)
        registry.register("FAKE", "fake-iss", _CountingKeySource())  # type: ignore[arg-type]

        await registry.start_refresh(interval=-5.0)

        assert registry._refresh_task is None
        assert registry.refresh_running is False


class TestStartRefreshTicks:
    async def test_refresh_called_at_least_twice_within_margin(self) -> None:
        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        source = _CountingKeySource()
        registry.register("FAKE", "fake-iss", source)  # type: ignore[arg-type]

        try:
            await registry.start_refresh(interval=0.02)
            await _wait_until(lambda: source.refresh_calls >= 2, timeout=3.0)
        finally:
            await registry.stop_refresh()

        assert source.refresh_calls >= 2


class TestStartRefreshIdempotent:
    async def test_calling_start_refresh_twice_creates_one_task(self) -> None:
        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        registry.register("FAKE", "fake-iss", _CountingKeySource())  # type: ignore[arg-type]

        try:
            await registry.start_refresh(interval=0.05)
            first_task = registry._refresh_task
            await registry.start_refresh(interval=0.05)
            second_task = registry._refresh_task

            assert first_task is second_task
        finally:
            await registry.stop_refresh()


class TestStopRefreshNoOp:
    async def test_stop_before_start_is_a_noop(self) -> None:
        registry = TrustedIssuerRegistry(ttl_seconds=0.0)

        await registry.stop_refresh()

        assert registry._refresh_task is None
        assert registry.refresh_running is False

    async def test_stop_twice_is_a_noop(self) -> None:
        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        registry.register("FAKE", "fake-iss", _CountingKeySource())  # type: ignore[arg-type]

        await registry.start_refresh(interval=0.05)
        await registry.stop_refresh()
        await registry.stop_refresh()  # second call must not raise

        assert registry._refresh_task is None


class TestStopRefreshCleanShutdown:
    async def test_no_orphaned_task_after_stop(self) -> None:
        baseline_task_count = len(asyncio.all_tasks())

        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        registry.register("FAKE", "fake-iss", _CountingKeySource())  # type: ignore[arg-type]

        await registry.start_refresh(interval=0.02)
        await asyncio.sleep(0.05)
        await registry.stop_refresh()

        assert registry._refresh_task is None
        assert len(asyncio.all_tasks()) == baseline_task_count


class TestFailureIsolation:
    async def test_failing_source_does_not_kill_the_loop_and_sibling_keeps_refreshing(
        self,
    ) -> None:
        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        healthy = _CountingKeySource()
        failing = _AlwaysFailingKeySource()
        registry.register("HEALTHY", "healthy-iss", healthy)  # type: ignore[arg-type]
        registry.register("FAILING", "failing-iss", failing)  # type: ignore[arg-type]

        try:
            await registry.start_refresh(interval=0.02)
            await _wait_until(lambda: healthy.refresh_calls >= 2, timeout=3.0)
        finally:
            await registry.stop_refresh()

        assert healthy.refresh_calls >= 2
        assert failing.refresh_calls >= 1
        # The loop must still be considered alive throughout — proven by the
        # healthy source continuing to tick despite the failing sibling.


class TestClampToMinRefreshInterval:
    async def test_period_below_min_refresh_interval_is_clamped_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=5.0)
        registry.register("FAKE", "fake-iss", _CountingKeySource())  # type: ignore[arg-type]

        try:
            with caplog.at_level(logging.WARNING):
                await registry.start_refresh(interval=0.5)

            assert registry.refresh_interval == 5.0

            warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
            assert len(warning_records) == 1
            message = warning_records[0].getMessage()
            assert "0.5" in message
            assert "5.0" in message
        finally:
            await registry.stop_refresh()


class TestRefreshProperties:
    async def test_refresh_running_and_refresh_interval_reflect_state(self) -> None:
        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        registry.register("FAKE", "fake-iss", _CountingKeySource())  # type: ignore[arg-type]

        assert registry.refresh_running is False
        assert registry.refresh_interval == 0.0

        try:
            await registry.start_refresh(interval=0.05)
            assert registry.refresh_running is True
            assert registry.refresh_interval == 0.05
        finally:
            await registry.stop_refresh()

        assert registry.refresh_running is False
