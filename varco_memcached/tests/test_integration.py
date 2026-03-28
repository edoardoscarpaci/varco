"""
Integration tests for varco_memcached.cache
=============================================
These tests require a real Memcached server.  They use ``testcontainers``
to spin up a Memcached Docker container automatically.

Run with::

    uv run pytest varco_memcached/tests/test_integration.py -m integration

All tests in this file are tagged ``@pytest.mark.integration`` and are
**skipped by default**.  CI pipelines that have Docker available should
run them explicitly.

Sections
--------
- Round-trip  — set/get survive a real Memcached network call
- TTL expiry  — entries expire after the configured TTL (uses a short 1s TTL)
- Delete      — entry disappears after delete
- Clear       — all tracked entries removed via the in-process registry
- Exists      — presence check against real server
"""

from __future__ import annotations

import asyncio

import pytest

# testcontainers ships MemcachedContainer which uses a TCP socket probe to wait
# for readiness — more reliable than log-based waiting since memcached:alpine
# logs nothing on startup.
from testcontainers.memcached import MemcachedContainer

from varco_memcached.cache import MemcachedCache, MemcachedCacheSettings


# ── Fixture: real Memcached via Docker ──────────────────────────────────────────


@pytest.fixture(scope="module")
def memcached_container():
    """
    Start a Memcached Docker container for the duration of this test module.

    Uses ``testcontainers.memcached.MemcachedContainer`` which waits for
    readiness via a TCP socket probe — memcached:alpine emits no startup logs,
    so log-based waiting would always time out.

    Yields:
        The running ``MemcachedContainer`` instance.
    """
    with MemcachedContainer() as container:
        yield container


@pytest.fixture
def memcached_settings(
    memcached_container: MemcachedContainer,
) -> MemcachedCacheSettings:
    """
    ``MemcachedCacheSettings`` pointing at the Docker-managed Memcached container.

    Args:
        memcached_container: The running container fixture.

    Returns:
        Settings with the dynamic host/port assigned by Docker.
    """
    host = memcached_container.get_container_host_ip()
    port = int(memcached_container.get_exposed_port(11211))
    return MemcachedCacheSettings(
        host=host,
        port=port,
        key_prefix="test:",
    )


# ── Tests ────────────────────────────────────────────────────────────────────────


@pytest.mark.integration
class TestMemcachedCacheIntegration:
    async def test_set_and_get_roundtrip(
        self, memcached_settings: MemcachedCacheSettings
    ) -> None:
        """Values written to Memcached are returned intact on get."""
        async with MemcachedCache(memcached_settings) as cache:
            await cache.set("hello", {"world": 42})
            result = await cache.get("hello")
        assert result == {"world": 42}

    async def test_get_missing_returns_none(
        self, memcached_settings: MemcachedCacheSettings
    ) -> None:
        """Missing key returns None without raising."""
        async with MemcachedCache(memcached_settings) as cache:
            result = await cache.get("absolutely-not-set")
        assert result is None

    async def test_set_with_ttl_expires(
        self, memcached_settings: MemcachedCacheSettings
    ) -> None:
        """An entry with ttl=1 is gone after 2 seconds."""
        async with MemcachedCache(memcached_settings) as cache:
            await cache.set("ttl-key", "soon-gone", ttl=1)
            # Confirm it exists right after set.
            assert await cache.get("ttl-key") == "soon-gone"
            # Wait for Memcached to expire it.
            await asyncio.sleep(2)
            result = await cache.get("ttl-key")
        assert result is None

    async def test_delete_removes_entry(
        self, memcached_settings: MemcachedCacheSettings
    ) -> None:
        """delete() makes a previously-set key return None."""
        async with MemcachedCache(memcached_settings) as cache:
            await cache.set("to-delete", "value")
            await cache.delete("to-delete")
            result = await cache.get("to-delete")
        assert result is None

    async def test_delete_nonexistent_noop(
        self, memcached_settings: MemcachedCacheSettings
    ) -> None:
        """delete() on a missing key must not raise."""
        async with MemcachedCache(memcached_settings) as cache:
            await cache.delete("nonexistent-key")

    async def test_exists_true(
        self, memcached_settings: MemcachedCacheSettings
    ) -> None:
        """exists() returns True for a key that is present."""
        async with MemcachedCache(memcached_settings) as cache:
            await cache.set("exists-key", "yes")
            assert await cache.exists("exists-key") is True

    async def test_exists_false_for_missing(
        self, memcached_settings: MemcachedCacheSettings
    ) -> None:
        """exists() returns False for a key that was never set."""
        async with MemcachedCache(memcached_settings) as cache:
            assert await cache.exists("not-there") is False

    async def test_clear_removes_tracked_keys(
        self, memcached_settings: MemcachedCacheSettings
    ) -> None:
        """clear() deletes all keys written via this cache instance."""
        async with MemcachedCache(memcached_settings) as cache:
            await cache.set("clear-a", 1)
            await cache.set("clear-b", 2)
            await cache.clear()
            assert await cache.get("clear-a") is None
            assert await cache.get("clear-b") is None
