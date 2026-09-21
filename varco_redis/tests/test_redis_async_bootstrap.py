"""
tests.test_redis_async_bootstrap
===================================
Plan 046 §D-contract — ``async_bootstrap(container) -> DIContainer`` no
longer accepts ``setup_cache=``/``streams=`` kwargs: it is always
``bootstrap(container)`` followed by
``await container.ainstall(RedisCacheConfiguration)``. Call plain
``bootstrap()`` for a scan-only container.

Supersedes the pre-046 characterization (Plan 014 / audit F7) which pinned
the *opposite* default (``setup_cache`` defaulting to ``False``) — that
asymmetry with ``varco_memcached`` is exactly what DI-2 removed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from varco_redis.di import async_bootstrap


def _make_mock_container() -> MagicMock:
    """Build a ``MagicMock`` standing in for a ``DIContainer``."""
    mock = MagicMock()
    mock.ainstall = AsyncMock(return_value=None)
    return mock


class TestAsyncBootstrapAlwaysInstallsCache:
    async def test_always_calls_ainstall(self) -> None:
        """``await async_bootstrap(container)`` always installs the cache."""
        from varco_redis.cache import RedisCacheConfiguration  # noqa: PLC0415

        container = _make_mock_container()

        with patch("varco_redis.di.bootstrap", return_value=container):
            result = await async_bootstrap(container)

        container.ainstall.assert_awaited_once_with(RedisCacheConfiguration)
        assert result is container
