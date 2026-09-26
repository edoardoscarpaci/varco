"""
DI-16 worked example — two named ``RedisCacheSettings`` instances under
different qualifiers/env prefixes (Plan 048 / Step 7).

Unit-only: no Redis container is started. Depends on
``varco_core.settings.bind_settings``, which does not exist yet — every test
fails with ``ImportError`` until Step 2 (and Step 8's redis rewiring) land.
"""

from __future__ import annotations

from typing import Annotated

import pytest
from providify import DIContainer, Inject, InjectMeta, Singleton
from varco_core.settings import bind_settings, provenance_of
from varco_redis.cache import RedisCacheSettings


@pytest.fixture
def redis_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VARCO_REDIS_CACHE_URL", "redis://default:6379/0")
    monkeypatch.setenv("VARCO_REDIS_SESSIONS_CACHE_URL", "redis://sessions:6379/1")


class TestNamedRedisCacheSettings:
    def test_default_and_qualified_bindings_resolve_distinct_instances(
        self, di_container: DIContainer, redis_settings_env: None
    ) -> None:
        default = bind_settings(di_container, RedisCacheSettings)
        sessions = bind_settings(
            di_container,
            RedisCacheSettings,
            qualifier="sessions",
            env_prefix="VARCO_REDIS_SESSIONS_CACHE_",
        )

        assert default.url == "redis://default:6379/0"
        assert sessions.url == "redis://sessions:6379/1"
        assert default is not sessions

    def test_two_caches_singleton_resolves_both_via_injectmeta(
        self, di_container: DIContainer, redis_settings_env: None
    ) -> None:
        bind_settings(di_container, RedisCacheSettings)
        bind_settings(
            di_container,
            RedisCacheSettings,
            qualifier="sessions",
            env_prefix="VARCO_REDIS_SESSIONS_CACHE_",
        )

        @Singleton
        class _TwoCaches:
            def __init__(
                self,
                default: Inject[RedisCacheSettings],
                sessions: Annotated[RedisCacheSettings, InjectMeta(qualifier="sessions")],
            ) -> None:
                self.default = default
                self.sessions = sessions

        # `register()`, not `provide()` — `_TwoCaches` is a `@Singleton`-
        # decorated class, and `provide()` is providify's *factory-function*
        # registration API (it would call `_TwoCaches` as if it were a
        # zero-metadata callable, which resolves no injected parameters at
        # all). `register()` is the class-registration counterpart that
        # actually reads `@Singleton`'s DI metadata off `__init__`.
        di_container.register(_TwoCaches)
        instance = di_container.get(_TwoCaches)

        assert instance.default.url == "redis://default:6379/0"
        assert instance.sessions.url == "redis://sessions:6379/1"

    def test_validate_is_clean_with_two_qualified_bindings(
        self, di_container: DIContainer, redis_settings_env: None
    ) -> None:
        bind_settings(di_container, RedisCacheSettings)
        bind_settings(
            di_container,
            RedisCacheSettings,
            qualifier="sessions",
            env_prefix="VARCO_REDIS_SESSIONS_CACHE_",
        )

        report = di_container.validate(raise_on_error=False)

        assert report.errors == ()

    def test_provenance_reports_the_overridden_env_prefix(
        self, di_container: DIContainer, redis_settings_env: None
    ) -> None:
        bind_settings(
            di_container,
            RedisCacheSettings,
            qualifier="sessions",
            env_prefix="VARCO_REDIS_SESSIONS_CACHE_",
        )

        prov = provenance_of(di_container, RedisCacheSettings, qualifier="sessions")

        assert prov.env_prefix == "VARCO_REDIS_SESSIONS_CACHE_"
