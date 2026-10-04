"""
Plan 056 / FLIP-13 repair — ``posture.webhook_secrets_plaintext`` is
assessed in a **stock** ``create_varco_app()``: the default
``LocalPostureCollector`` resolves the DI-bound
``WebhookSubscriptionRepository`` itself, so a bound
``PlaintextFieldEncryptor`` behind the SA provider refuses a production
start unless suppressed.

Opts out of the package-wide ``_posture_warn_only`` fixture (054
§D-flip2-tests). Assertions look only at the webhook id: unrelated default
findings (e.g. ``posture.authorizer_unbound``) may also block a bare app.
"""

from __future__ import annotations

from typing import Any

import pytest
from providify import DIContainer
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase

_ID = "posture.webhook_secrets_plaintext"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "VARCO_SECURITY_ENFORCE",
        "VARCO_SECURITY_ENV",
        "VARCO_SECURITY_SUPPRESS",
        "VARCO_WEBHOOK_BACKEND",
    ):
        monkeypatch.delenv(name, raising=False)


def _sa_module(tmp_path: Any) -> Any:
    from varco_sa.config import SAConfig
    from varco_sa.modules import SAModule

    class _Base(DeclarativeBase):
        pass

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'wh.db'}")
    return SAModule(SAConfig(engine=engine, base=_Base, entity_classes=()))


async def _refusal_message(container: DIContainer, *modules: Any) -> str:
    """Start the app; return the posture refusal message ('' when it starts)."""
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.modules.defaults import default_modules

    app = create_varco_app(
        container, modules=[*default_modules(), *modules], routers=[], validate=False
    )
    try:
        async with app.router.lifespan_context(app):
            return ""
    except RuntimeError as exc:
        return str(exc)


def _plaintext_bound_container() -> DIContainer:
    from varco_core.di import bind_instance
    from varco_core.encryption import FieldEncryptor, PlaintextFieldEncryptor

    container = DIContainer()
    bind_instance(container, FieldEncryptor, PlaintextFieldEncryptor(acknowledge_plaintext=True))
    return container


async def test_di_bound_plaintext_encryptor_refuses_production_start(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setenv("VARCO_WEBHOOK_BACKEND", "sa")

    message = await _refusal_message(_plaintext_bound_container(), _sa_module(tmp_path))

    assert _ID in message


async def test_suppressed_plaintext_encryptor_does_not_block(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setenv("VARCO_WEBHOOK_BACKEND", "sa")
    monkeypatch.setenv("VARCO_SECURITY_SUPPRESS", _ID)

    message = await _refusal_message(_plaintext_bound_container(), _sa_module(tmp_path))

    assert _ID not in message


async def test_stock_app_with_in_memory_repository_is_not_a_finding() -> None:
    # The @Fallback in-memory repository holds secrets in memory only — never reported.
    message = await _refusal_message(DIContainer())

    assert _ID not in message


def test_unconstructible_repository_is_not_assessed_never_raises() -> None:
    from varco_core.webhook.base import WebhookSubscriptionRepository
    from varco_fastapi.posture import LocalPostureCollector

    class _Boom:
        """Stands in for a container whose webhook provider raises on construction."""

        def is_resolvable(self, interface: type) -> bool:
            return interface is WebhookSubscriptionRepository

        def get(self, interface: type) -> Any:
            if interface is WebhookSubscriptionRepository:
                raise RuntimeError("provider exploded")
            raise LookupError(interface)

    checks = {f.check for f in LocalPostureCollector(container=_Boom()).collect()}

    assert "posture.not_assessed.webhook_secrets_plaintext" in checks
    assert _ID not in checks
