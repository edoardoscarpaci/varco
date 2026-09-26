"""
Unit tests for varco_memcached.di
===================================
Tests for ``bootstrap()`` and ``async_bootstrap()``.

No real Memcached instance is required — ``MemcachedCacheConfiguration``
is mocked via ``unittest.mock`` to verify the DI wiring without opening
any network connections.

Sections
--------
- ``bootstrap``       — returns container, requires an explicit container (Plan 046)
- ``async_bootstrap`` — delegates to bootstrap, calls ainstall
- ``__init__`` re-export — bootstrap and async_bootstrap importable from package root
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from varco_memcached.di import async_bootstrap, bootstrap

# ── helpers ───────────────────────────────────────────────────────────────────


def _make_mock_container() -> MagicMock:
    """
    Build a ``MagicMock`` that stands in for a ``DIContainer``.

    ``ainstall`` must be an ``AsyncMock`` so ``await container.ainstall(...)``
    doesn't raise a ``TypeError`` inside the async path.
    """
    mock = MagicMock()
    # ainstall is called with ``await`` — it must be coroutine-compatible
    mock.ainstall = AsyncMock(return_value=None)
    return mock


# ── bootstrap ─────────────────────────────────────────────────────────────────


class TestBootstrap:
    """Tests for the synchronous ``bootstrap()`` helper."""

    def test_returns_provided_container(self) -> None:
        """
        bootstrap(container) must return the same container object.

        Ensures that callers who chain calls can fluently pass the result
        to the next bootstrap step.
        """
        container = _make_mock_container()
        result = bootstrap(container)
        assert result is container, (
            "bootstrap() must return the container it was given so callers "
            "can chain: container = bootstrap(container)."
        )

    def test_scans_varco_memcached_package(self) -> None:
        """
        bootstrap() must call ``container.scan("varco_memcached", recursive=True)``.

        The scan is what auto-discovers @Singleton classes
        (MemcachedCacheSettings, MemcachedHealthCheck).
        """
        container = _make_mock_container()
        bootstrap(container)
        container.scan.assert_called_once_with("varco_memcached", recursive=True)

    def test_bootstrap_requires_a_container(self) -> None:
        """
        Plan 046 §D-contract — ``bootstrap()`` no longer defaults ``container``
        to ``None``/``DIContainer.current()``; calling it with no argument is
        a ``TypeError`` (missing positional argument), the intended loud
        failure.
        """
        import pytest  # noqa: PLC0415

        with pytest.raises(TypeError):
            bootstrap()  # type: ignore[call-arg]


# ── async_bootstrap ───────────────────────────────────────────────────────────


class TestAsyncBootstrap:
    """Tests for the asynchronous ``async_bootstrap()`` helper."""

    async def test_returns_container(self) -> None:
        """
        async_bootstrap() must return the container so callers can chain awaits.
        """
        container = _make_mock_container()

        # MemcachedCacheConfiguration is imported locally inside async_bootstrap
        # (not at module level), so we must patch it at its source module.
        # Patching "varco_memcached.di.MemcachedCacheConfiguration" would fail
        # because the name doesn't exist in the di module's namespace.
        with (
            patch("varco_memcached.di.bootstrap", return_value=container),
            patch("varco_memcached.cache.MemcachedCacheConfiguration"),
        ):
            result = await async_bootstrap(container)

        assert result is container

    async def test_calls_bootstrap_first(self) -> None:
        """
        async_bootstrap() must delegate the sync scan to bootstrap() before
        doing any async work.

        This ensures the @Singleton classes are registered before ainstall()
        tries to resolve them.
        """
        container = _make_mock_container()

        with (
            patch("varco_memcached.di.bootstrap", return_value=container) as mock_bs,
            patch("varco_memcached.cache.MemcachedCacheConfiguration"),
        ):
            await async_bootstrap(container)

        mock_bs.assert_called_once_with(container)

    async def test_calls_ainstall_with_configuration(self) -> None:
        """
        async_bootstrap() must call ``await container.ainstall(MemcachedCacheConfiguration)``.

        ``ainstall`` is what triggers ``MemcachedCacheConfiguration.setup()``,
        which constructs and starts the cache.  Without this call the
        CacheBackend binding is never registered.

        The local import inside ``async_bootstrap()`` means we verify
        ``container.ainstall`` was called with the *real*
        ``MemcachedCacheConfiguration`` class, not a mock.
        """
        from varco_memcached.cache import MemcachedCacheConfiguration  # noqa: PLC0415

        container = _make_mock_container()

        with patch("varco_memcached.di.bootstrap", return_value=container):
            await async_bootstrap(container)

        # ainstall must be awaited exactly once with the real configuration class
        container.ainstall.assert_awaited_once_with(MemcachedCacheConfiguration)

    async def test_no_kwargs_call_awaits_ainstall_exactly_once(self) -> None:
        """
        ``await async_bootstrap(container)`` with **no kwargs** (Plan 046
        removed ``setup_cache=`` entirely) must await
        ``container.ainstall(MemcachedCacheConfiguration)`` exactly once —
        it is unconditional now.
        """
        from varco_memcached.cache import MemcachedCacheConfiguration  # noqa: PLC0415

        container = _make_mock_container()

        with patch("varco_memcached.di.bootstrap", return_value=container):
            await async_bootstrap(container)

        container.ainstall.assert_awaited_once_with(MemcachedCacheConfiguration)

    async def test_async_bootstrap_requires_a_container(self) -> None:
        """Plan 046 §D-contract — same required-container contract as ``bootstrap()``."""
        import pytest  # noqa: PLC0415

        with pytest.raises(TypeError):
            await async_bootstrap()  # type: ignore[call-arg]


# ── package-level re-export ───────────────────────────────────────────────────


class TestPackageReexport:
    """
    Verify that bootstrap and async_bootstrap are importable from
    the package root (``varco_memcached``).

    This matters because downstream code does::

        from varco_memcached import bootstrap, async_bootstrap

    and ``__init__.py`` must export them in ``__all__``.
    """

    def test_bootstrap_importable_from_package(self) -> None:
        """bootstrap is accessible from the top-level package namespace."""
        import varco_memcached  # noqa: PLC0415

        assert hasattr(varco_memcached, "bootstrap"), (
            "'bootstrap' not found in varco_memcached namespace — "
            "add it to __init__.py imports and __all__."
        )
        assert callable(varco_memcached.bootstrap)

    def test_async_bootstrap_importable_from_package(self) -> None:
        """async_bootstrap is accessible from the top-level package namespace."""
        import varco_memcached  # noqa: PLC0415

        assert hasattr(varco_memcached, "async_bootstrap"), (
            "'async_bootstrap' not found in varco_memcached namespace — "
            "add it to __init__.py imports and __all__."
        )
        assert callable(varco_memcached.async_bootstrap)

    def test_both_in_dunder_all(self) -> None:
        """Both helpers must appear in ``__all__`` for star-import clarity."""
        import varco_memcached  # noqa: PLC0415

        assert "bootstrap" in varco_memcached.__all__, (
            "'bootstrap' missing from varco_memcached.__all__"
        )
        assert "async_bootstrap" in varco_memcached.__all__, (
            "'async_bootstrap' missing from varco_memcached.__all__"
        )


class TestSettingsBoundOnceAndFailFast:
    """Plan 048 / Step 11 — MemcachedCacheSettings goes through bind_settings()."""

    def test_settings_bound_once_after_two_bootstrap_calls(self) -> None:
        from providify import DIContainer
        from varco_memcached.cache import MemcachedCacheSettings
        from varco_memcached.di import bootstrap

        c = DIContainer()
        bootstrap(c)
        bootstrap(c)

        assert len(c.get_all_bindings(MemcachedCacheSettings)) == 1

    def test_bad_env_fails_bootstrap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from providify import DIContainer
        from varco_core.settings import SettingsValidationError
        from varco_memcached.di import bootstrap

        monkeypatch.setenv("VARCO_MEMCACHED_CACHE_PORT", "notanint")

        with pytest.raises(SettingsValidationError) as exc:
            bootstrap(DIContainer())

        assert "VARCO_MEMCACHED_CACHE_PORT" in str(exc.value)
