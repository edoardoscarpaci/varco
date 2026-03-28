"""
Tests for varco_core.resilience.bulkhead
=========================================
Covers ``BulkheadConfig``, ``Bulkhead``, ``BulkheadFullError``,
and the ``@bulkhead`` decorator.

All tests are async (asyncio_mode = "auto" is set project-wide).
"""

from __future__ import annotations

import asyncio

import pytest

from varco_core.resilience.bulkhead import (
    Bulkhead,
    BulkheadConfig,
    BulkheadFullError,
    bulkhead,
)


# ── BulkheadConfig ────────────────────────────────────────────────────────────


class TestBulkheadConfig:
    def test_defaults(self) -> None:
        cfg = BulkheadConfig(max_concurrent=5)
        assert cfg.max_concurrent == 5
        assert cfg.max_wait == 0.0

    def test_explicit_max_wait(self) -> None:
        cfg = BulkheadConfig(max_concurrent=5, max_wait=0.5)
        assert cfg.max_wait == pytest.approx(0.5)

    def test_frozen(self) -> None:
        cfg = BulkheadConfig(max_concurrent=5)
        with pytest.raises((AttributeError, TypeError)):
            cfg.max_concurrent = 10  # type: ignore[misc]

    def test_max_concurrent_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_concurrent must be ≥ 1"):
            BulkheadConfig(max_concurrent=0)

    def test_max_wait_must_be_non_negative(self) -> None:
        with pytest.raises(ValueError, match="max_wait must be ≥ 0"):
            BulkheadConfig(max_concurrent=5, max_wait=-0.1)

    def test_repr(self) -> None:
        cfg = BulkheadConfig(max_concurrent=3, max_wait=1.0)
        assert "3" in repr(cfg)
        assert "1.0" in repr(cfg)


# ── BulkheadFullError ─────────────────────────────────────────────────────────


class TestBulkheadFullError:
    def test_attributes(self) -> None:
        err = BulkheadFullError("payments", 5, 0.2)
        assert err.bulkhead_name == "payments"
        assert err.max_concurrent == 5
        assert err.max_wait == pytest.approx(0.2)

    def test_message_contains_name(self) -> None:
        err = BulkheadFullError("payments", 5, 0.2)
        assert "payments" in str(err)
        assert "5" in str(err)


# ── Bulkhead ──────────────────────────────────────────────────────────────────


class TestBulkheadCallBasic:
    async def test_call_passes_through_result(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=2))

        async def double(x: int) -> int:
            return x * 2

        assert await bh.call(double, 4) == 8

    async def test_call_propagates_exceptions(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=2))

        async def boom() -> None:
            raise ValueError("inner error")

        with pytest.raises(ValueError, match="inner error"):
            await bh.call(boom)

    async def test_slot_released_after_exception(self) -> None:
        """Exception inside the call must release the slot."""
        bh = Bulkhead(BulkheadConfig(max_concurrent=1))

        async def boom() -> None:
            raise RuntimeError("oops")

        with pytest.raises(RuntimeError):
            await bh.call(boom)

        # Slot must be free now — the next call should succeed.
        assert (
            await bh.call(
                lambda: asyncio.sleep(0),
            )
            is None
        )  # type: ignore[arg-type]
        # More explicit check via available_slots.
        assert bh.available_slots == 1

    async def test_available_slots_decrements_during_call(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=3))
        # Before any calls, all slots are free.
        assert bh.available_slots == 3

        barrier = asyncio.Event()

        async def blocked() -> None:
            await barrier.wait()

        # Start 2 concurrent blocked calls.
        t1 = asyncio.create_task(bh.call(blocked))
        t2 = asyncio.create_task(bh.call(blocked))

        # Give the event loop a chance to start both tasks.
        await asyncio.sleep(0)

        # 2 slots occupied.
        assert bh.available_slots == 1

        # Release both.
        barrier.set()
        await t1
        await t2
        assert bh.available_slots == 3


class TestBulkheadFailFast:
    async def test_fail_fast_when_all_slots_busy(self) -> None:
        """max_wait=0.0 (default) → raise BulkheadFullError immediately."""
        bh = Bulkhead(BulkheadConfig(max_concurrent=1))
        barrier = asyncio.Event()

        async def slow() -> None:
            await barrier.wait()

        # Occupy the single slot.
        running_task = asyncio.create_task(bh.call(slow))
        await asyncio.sleep(0)  # let the task start

        # Second call should be rejected immediately.
        with pytest.raises(BulkheadFullError) as exc_info:
            await bh.call(slow)

        assert exc_info.value.bulkhead_name == "unnamed"
        assert exc_info.value.max_concurrent == 1

        # Clean up.
        barrier.set()
        await running_task


class TestBulkheadBoundedWait:
    async def test_bounded_wait_acquires_freed_slot(self) -> None:
        """
        If a slot frees within max_wait, the waiting call should proceed.
        """
        bh = Bulkhead(BulkheadConfig(max_concurrent=1, max_wait=1.0))
        barrier = asyncio.Event()

        async def slow() -> str:
            await barrier.wait()
            return "done"

        # Occupy the slot.
        t1 = asyncio.create_task(bh.call(slow))
        await asyncio.sleep(0)

        # Second call waits — free the slot almost immediately.
        async def free_after_delay() -> None:
            await asyncio.sleep(0.01)
            barrier.set()

        asyncio.create_task(free_after_delay())
        result = await bh.call(slow)
        assert result == "done"
        await t1

    async def test_bounded_wait_times_out(self) -> None:
        """If no slot frees within max_wait, BulkheadFullError is raised."""
        bh = Bulkhead(BulkheadConfig(max_concurrent=1, max_wait=0.05))
        barrier = asyncio.Event()

        async def slow() -> None:
            await barrier.wait()

        t1 = asyncio.create_task(bh.call(slow))
        await asyncio.sleep(0)

        with pytest.raises(BulkheadFullError):
            await bh.call(slow)

        barrier.set()
        await t1


class TestBulkheadProtect:
    async def test_protect_wraps_async_function(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=2), name="db")

        @bh.protect
        async def fetch() -> str:
            return "data"

        assert await fetch() == "data"

    def test_protect_rejects_sync_function(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=2))
        with pytest.raises(TypeError, match="async functions"):

            @bh.protect
            def sync_fn() -> None:
                pass

    async def test_protect_preserves_function_name(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=2))

        @bh.protect
        async def my_func() -> None:
            pass

        assert my_func.__name__ == "my_func"


class TestBulkheadRepr:
    def test_repr_contains_name_and_config(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=5), name="payments")
        r = repr(bh)
        assert "payments" in r
        assert "5" in r


# ── @bulkhead decorator ───────────────────────────────────────────────────────


class TestBulkheadDecorator:
    async def test_decorator_creates_per_function_bulkhead(self) -> None:
        @bulkhead(BulkheadConfig(max_concurrent=2))
        async def handler(x: int) -> int:
            return x + 1

        assert await handler(9) == 10

    def test_decorator_rejects_sync_function(self) -> None:
        with pytest.raises(TypeError, match="async functions"):

            @bulkhead(BulkheadConfig(max_concurrent=2))
            def sync_fn() -> None:
                pass

    async def test_decorator_uses_function_qualname_as_name(self) -> None:
        """BulkheadFullError should contain the function's qualname as the name."""
        barrier = asyncio.Event()

        @bulkhead(BulkheadConfig(max_concurrent=1))
        async def my_handler() -> None:
            await barrier.wait()

        t1 = asyncio.create_task(my_handler())
        await asyncio.sleep(0)

        with pytest.raises(BulkheadFullError) as exc_info:
            await my_handler()

        # The bulkhead was named after the function's qualname.
        assert "my_handler" in exc_info.value.bulkhead_name

        barrier.set()
        await t1

    async def test_custom_name_passed_to_error(self) -> None:
        barrier = asyncio.Event()

        @bulkhead(BulkheadConfig(max_concurrent=1), name="custom-name")
        async def handler() -> None:
            await barrier.wait()

        t1 = asyncio.create_task(handler())
        await asyncio.sleep(0)

        with pytest.raises(BulkheadFullError) as exc_info:
            await handler()

        assert exc_info.value.bulkhead_name == "custom-name"

        barrier.set()
        await t1
