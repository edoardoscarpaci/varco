"""
Plan 050 / Step 11 REPAIR (§D-wire rows 4-5) — ``SAConfiguration.webhook_repository``:
live only when ``VARCO_WEBHOOK_BACKEND=sa``, injects the app's own
``SAConfig.engine`` and a **required** ``FieldEncryptor`` (no plaintext
default through the DI path — that stays a hand-construction-only escape
hatch, closed for good by Plan 056/FLIP-13's constructor refusal).

Docker-free — sqlite in-memory.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase


def _sa_config() -> object:
    from varco_sa.config import SAConfig

    class _Base(DeclarativeBase):
        pass

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    return SAConfig(engine=engine, base=_Base, entity_classes=())


async def test_sa_webhook_repository_reports_missing_binding_without_encryptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    ``VARCO_WEBHOOK_BACKEND=sa`` selects the SA provider; with no bound
    ``FieldEncryptor`` a full ``validate()`` names it as ``MISSING_BINDING``
    on the ``webhook_repository`` provider — never a silently-constructed
    plaintext repository.
    """
    from providify import IssueKind
    from varco_core.di import bind_instance
    from varco_core.encryption import FieldEncryptor  # noqa: F401 - documents the requested type
    from varco_sa.config import SAConfig
    from varco_sa.di import bootstrap

    monkeypatch.setenv("VARCO_WEBHOOK_BACKEND", "sa")

    container = DIContainer()
    bootstrap(container)
    bind_instance(container, SAConfig, _sa_config())

    report = container.validate(raise_on_error=False)
    missing = [
        issue
        for issue in report.errors
        if issue.kind is IssueKind.MISSING_BINDING and issue.requested == "FieldEncryptor"
    ]
    assert missing, [i.message for i in report.errors]


async def test_sa_webhook_repository_resolves_with_encryptor_and_reuses_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cryptography.fernet import Fernet
    from varco_core.di import bind_instance
    from varco_core.encryption import FernetFieldEncryptor, FieldEncryptor
    from varco_core.webhook.base import WebhookSubscriptionRepository
    from varco_sa.config import SAConfig
    from varco_sa.di import bootstrap
    from varco_sa.webhook import SAWebhookSubscriptionRepository

    monkeypatch.setenv("VARCO_WEBHOOK_BACKEND", "sa")

    container = DIContainer()
    bootstrap(container)
    config = _sa_config()
    bind_instance(container, SAConfig, config)
    encryptor = FernetFieldEncryptor(Fernet.generate_key())
    bind_instance(container, FieldEncryptor, encryptor)

    repo = container.get(WebhookSubscriptionRepository)
    assert isinstance(repo, SAWebhookSubscriptionRepository)
    assert repo._encryptor is encryptor  # type: ignore[attr-defined]
    assert repo._engine is config.engine  # type: ignore[attr-defined]
