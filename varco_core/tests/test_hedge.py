"""
Tests for varco_core.resilience.hedge
=======================================
Covers ``HedgeConfig`` and the ``@hedge`` decorator.

Key test strategy:
- Use ``asyncio.Event`` / ``asyncio.Barrier`` to control when calls return.
- Use call counters to verify that hedged (duplicate) calls are actually fired.
- Verify that losing tasks are properly cancelled and do not leak.

All tests are async (asyncio_mode = "auto" is set project-wide).
"""

from __future__ import annotations

import asyncio

import pytest

from varco_core.resilience.hedge import HedgeConfig, _hedged_call, hedge


# ── HedgeConfig ───────────────────────────────────────────────────────────────


class TestHedgeConfig:
    def test_defaults(self) -> None:
        cfg = HedgeConfig(delay=0.1)
        assert cfg.delay == pytest.approx(0.1)
        assert cfg.max_hedges == 1

    def test_explicit_max_hedges(self) -> None:
        cfg = HedgeConfig(delay=0.05, max_hedges=3)
        assert cfg.max_hedges == 3

    def test_frozen(self) -> None:
        cfg = HedgeConfig(delay=0.1)
        with pytest.raises((AttributeError, TypeError)):
            cfg.delay = 0.5  # type: ignore[misc]

    def test_delay_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="delay must be > 0"):
            HedgeConfig(delay=0.0)

    def test_negative_delay_rejected(self) -> None:
        with pytest.raises(ValueError, match="delay must be > 0"):
            HedgeConfig(delay=-0.1)

    def test_max_hedges_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_hedges must be ≥ 1"):
            HedgeConfig(delay=0.1, max_hedges=0)

    def test_repr(self) -> None:
        cfg = HedgeConfig(delay=0.1, max_hedges=2)
        assert "0.1" in repr(cfg)
        assert "2" in repr(cfg)


# ── @hedge decorator ──────────────────────────────────────────────────────────


class TestHedgeDecorator:
    def test_rejects_sync_function(self) -> None:
        with pytest.raises(TypeError, match="async"):

            @hedge(HedgeConfig(delay=0.1))
            def sync_fn() -> None:
                pass

    async def test_functools_wraps_preserves_name(self) -> None:
        @hedge(HedgeConfig(delay=0.1))
        async def my_func(x: int) -> int:
            return x

        assert my_func.__name__ == "my_func"

    async def test_fast_call_completes_without_hedge(self) -> None:
        """
        If the first call returns before the delay, no hedge is issued.
        The call counter should be exactly 1.
        """
        call_count = 0

        @hedge(HedgeConfig(delay=1.0))  # long delay — hedge should never fire
        async def fast_call() -> str:
            nonlocal call_count
            call_count += 1
            return "fast"

        result = await fast_call()
        assert result == "fast"
        # Delay is 1 s, call is instant — only one invocation.
        assert call_count == 1

    async def test_slow_first_call_triggers_hedge(self) -> None:
        """
        If the first call is slow (blocked on an event), the hedge should
        fire after the delay.  The second call (hedge) should complete first.
        """
        call_count = 0
        first_call_barrier = asyncio.Event()

        @hedge(HedgeConfig(delay=0.01))
        async def slow_then_fast() -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call blocks indefinitely — simulates a slow replica.
                await first_call_barrier.wait()
            # Second call (hedge) returns immediately.
            return "hedge-result"

        result = await slow_then_fast()
        assert result == "hedge-result"
        # Both calls were initiated (first + 1 hedge).
        assert call_count == 2

        # Allow the blocked first call to complete (cleanup).
        first_call_barrier.set()

    async def test_result_is_from_first_completing_call(self) -> None:
        """
        Whichever call returns first wins; the other is cancelled.
        """
        order: list[int] = []
        release = asyncio.Event()

        @hedge(HedgeConfig(delay=0.01))
        async def ordered_call() -> str:
            call_num = len(order) + 1
            order.append(call_num)
            if call_num == 1:
                await release.wait()  # first call is slow
            return f"call-{call_num}"

        result = await ordered_call()
        # The hedge (call 2) completed first.
        assert result == "call-2"

        # Clean up the blocked first call.
        release.set()

    async def test_max_hedges_limits_concurrent_calls(self) -> None:
        """
        With max_hedges=2, at most 3 concurrent calls are ever in flight.
        """
        call_count = 0
        max_concurrent = 0
        active = 0
        release = asyncio.Event()

        @hedge(HedgeConfig(delay=0.01, max_hedges=2))
        async def counter_call() -> str:
            nonlocal call_count, max_concurrent, active
            call_count += 1
            active += 1
            max_concurrent = max(max_concurrent, active)
            await release.wait()
            active -= 1
            return "done"

        # Fire, wait a bit for all hedges, then release.
        task = asyncio.create_task(counter_call())
        await asyncio.sleep(0.05)  # enough for both hedges to fire
        release.set()
        result = await task

        assert result == "done"
        # max_hedges=2 → at most 3 calls (original + 2 hedges).
        assert call_count <= 3
        assert max_concurrent <= 3


# ── _hedged_call (internal) ───────────────────────────────────────────────────


class TestHedgedCallInternal:
    async def test_cancelled_tasks_do_not_leak(self) -> None:
        """
        After the winning call returns, all losing tasks must be cancelled
        and awaited — no leaked background tasks.
        """
        tasks_created: list[asyncio.Task] = []
        release_all = asyncio.Event()

        async def trackable_call() -> str:
            tasks_created.append(asyncio.current_task())  # type: ignore[arg-type]
            await release_all.wait()
            return "done"

        # Short delay so hedge fires before the release event.
        config = HedgeConfig(delay=0.01)
        task = asyncio.create_task(_hedged_call(config, trackable_call, (), {}))

        # Wait for hedge to fire.
        await asyncio.sleep(0.05)
        release_all.set()
        await task

        # All tasks that were created should now be done (not still running).
        for t in tasks_created:
            assert t.done(), f"Task {t} is still running — leaked!"

    async def test_exception_from_winning_task_propagates(self) -> None:
        """
        If the winning (first-completing) task raised, the exception
        should propagate to the caller.
        """
        config = HedgeConfig(delay=1.0)  # hedge never fires — instant failure

        async def always_fails() -> str:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await _hedged_call(config, always_fails, (), {})

    async def test_first_call_wins_when_hedge_is_slower(self) -> None:
        """
        If the original call returns before the hedge fires (delay > call time),
        the original result is returned.
        """
        config = HedgeConfig(delay=1.0)  # hedge won't fire in time
        call_count = 0

        async def fast() -> str:
            nonlocal call_count
            call_count += 1
            return f"result-{call_count}"

        result = await _hedged_call(config, fast, (), {})
        assert result == "result-1"
        assert call_count == 1
