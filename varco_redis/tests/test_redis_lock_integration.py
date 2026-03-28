"""
Integration tests for varco_redis.lock
=======================================
These tests spin up a real Redis instance via testcontainers and verify the
end-to-end behaviour of ``RedisLock``.

DISABLED BY DEFAULT — requires Docker.  Run with::

    pytest -m integration tests/test_redis_lock_integration.py

Or set the ``VARCO_RUN_INTEGRATION`` env var::

    VARCO_RUN_INTEGRATION=1 pytest tests/test_redis_lock_integration.py

Prerequisites:
    - Docker daemon running
    - testcontainers[redis] installed (see pyproject.toml dev dependencies)
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from varco_core.lock import LockNotAcquiredError

pytestmark = pytest.mark.integration

if not os.environ.get("VARCO_RUN_INTEGRATION"):
    pytest.skip(
        "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration",
        allow_module_level=True,
    )


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def redis_container():
    """Start a Redis instance for the integration test module."""
    from testcontainers.redis import RedisContainer

    with RedisContainer() as redis:
        yield redis


@pytest.fixture
async def lock(redis_container):
    """Connected ``RedisLock`` backed by the testcontainers Redis instance."""
    from varco_redis.config import RedisEventBusSettings
    from varco_redis.lock import RedisLock

    # Unique channel prefix per test run — prevents cross-test key collisions.
    prefix = f"test:{uuid.uuid4().hex[:8]}:"
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    url = f"redis://{host}:{port}/0"

    settings = RedisEventBusSettings(url=url, channel_prefix=prefix)
    async with RedisLock(settings=settings) as lock:
        yield lock


# ── Tests ──────────────────────────────────────────────────────────────────────


class TestRedisLockIntegration:
    async def test_try_acquire_returns_handle_when_free(self, lock) -> None:
        """try_acquire on a free key returns a LockHandle."""
        handle = await lock.try_acquire("integration:free", ttl=10)
        assert handle is not None
        await handle.release()

    async def test_try_acquire_returns_none_when_held(self, lock) -> None:
        """try_acquire on a held key returns None."""
        held = await lock.acquire("integration:held", ttl=10)
        try:
            second = await lock.try_acquire("integration:held", ttl=10)
            assert second is None
        finally:
            await held.release()

    async def test_acquire_context_manager_releases_on_exit(self, lock) -> None:
        """The LockHandle context manager releases the lock on exit."""
        async with await lock.acquire("integration:ctx", ttl=10):
            # Lock held inside
            contender = await lock.try_acquire("integration:ctx", ttl=10)
            assert contender is None

        # Lock released after exiting
        free = await lock.try_acquire("integration:ctx", ttl=10)
        assert free is not None
        await free.release()

    async def test_release_wrong_token_does_not_release_lock(self, lock) -> None:
        """
        Releasing with a wrong token (phantom release) must be a no-op —
        the real holder's lock remains intact.
        """
        real_handle = await lock.acquire("integration:token_guard", ttl=30)
        try:
            # Fake holder tries to release with a different token.
            fake_token = uuid.uuid4()
            await lock.release("integration:token_guard", fake_token)

            # Real lock must still be held.
            contender = await lock.try_acquire("integration:token_guard", ttl=10)
            assert (
                contender is None
            ), "Lock should still be held after wrong-token release"
        finally:
            await real_handle.release()

    async def test_acquire_raises_lock_not_acquired_on_timeout(self, lock) -> None:
        """acquire() raises LockNotAcquiredError when contended and timeout exceeded."""
        held = await lock.acquire("integration:timeout", ttl=30)
        try:
            with pytest.raises(LockNotAcquiredError) as exc_info:
                await lock.acquire("integration:timeout", ttl=10, timeout=0.2)
            assert exc_info.value.key == "integration:timeout"
        finally:
            await held.release()

    async def test_acquire_succeeds_after_holder_releases(self, lock) -> None:
        """
        A blocking acquire must succeed once the current holder releases,
        verifying that the polling loop works correctly against a real Redis.
        """
        held = await lock.acquire("integration:blocking_release", ttl=30)

        async def _release_after(delay: float) -> None:
            await asyncio.sleep(delay)
            await held.release()

        asyncio.create_task(_release_after(0.1))

        handle = await lock.acquire(
            "integration:blocking_release",
            ttl=10,
            timeout=3.0,
            retry_interval=0.05,
        )
        assert handle is not None
        await handle.release()

    async def test_different_keys_are_independent(self, lock) -> None:
        """Locks on distinct keys never block each other."""
        ha = await lock.acquire("integration:key_a", ttl=10)
        hb = await lock.try_acquire("integration:key_b", ttl=10)
        assert hb is not None, "key_b should be acquirable while key_a is held"
        await ha.release()
        await hb.release()

    async def test_ttl_expiry_allows_re_acquire(self, lock) -> None:
        """
        After the TTL expires (server-side), the same key must be acquirable
        again by a new caller without explicitly calling release().
        """
        # Acquire with a very short TTL — 1 s.
        _handle = await lock.acquire("integration:ttl_expiry", ttl=1)

        # Wait for the TTL to expire on Redis.
        await asyncio.sleep(1.5)

        # Now a new caller must be able to acquire without release.
        new_handle = await lock.try_acquire("integration:ttl_expiry", ttl=10)
        assert (
            new_handle is not None
        ), "Lock should be acquirable after TTL expiry — Redis key should have been deleted"
        await new_handle.release()
