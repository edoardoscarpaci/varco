"""
Where the example binds ``RedisEventBusSettings(use_streams=True)`` (Plan 058).

``bind_settings(..., instance=...)`` raises ``SettingsAlreadyBoundError``
against **any** prior binding of the class, ``@Fallback`` included — so a
settings override must be bound before ``RedisModule``'s ``bootstrap()``
records its env-path fallback. ``create_app()`` therefore binds it ahead of
``create_varco_app()``; these tests pin both halves of that contract.

Docker-free: ``create_app()`` connects to nothing at construction
(``create_async_engine`` is lazy; Redis/WS connect in ``async_init``/``start``).
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_core.settings import SettingsAlreadyBoundError, bind_settings, provenance_of
from varco_redis.config import RedisEventBusSettings

from example.app import create_app


def test_create_app_binds_streams_settings_from_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The resolved settings come from the code instance, not the env fallback."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.delenv("VARCO_REDIS_USE_STREAMS", raising=False)

    app = create_app()
    container = app.state.varco_container

    settings = container.get(RedisEventBusSettings)
    assert settings.use_streams is True
    assert provenance_of(container, RedisEventBusSettings).source == "instance"


def test_binding_after_a_backend_bootstrap_still_raises() -> None:
    """Binding the override after the fallback is recorded fails closed."""
    from varco_redis.di import bootstrap

    container = bootstrap(DIContainer())

    with pytest.raises(SettingsAlreadyBoundError):
        bind_settings(
            container, RedisEventBusSettings, instance=RedisEventBusSettings(use_streams=True)
        )
