"""
Plan 050 / Step 11 REPAIR (§D-wire rows 4-5) — ``BeanieConfiguration.webhook_repository``:
live only when ``VARCO_WEBHOOK_BACKEND=beanie``, injects the app's own
``BeanieSettings.mongo_client``/``db_name`` and a **required**
``FieldEncryptor`` (no plaintext default through the DI path).

Docker-free — ``AsyncMongoClient`` connects lazily (an unroutable address
with a 1ms timeout is enough, same trick ``test_beanie_di_wiring.py`` uses;
``container.validate()``/``container.get()`` never open a real connection).
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from pymongo import AsyncMongoClient


def _beanie_settings() -> object:
    from varco_beanie.config import BeanieSettings

    return BeanieSettings(
        mongo_client=AsyncMongoClient(
            "mongodb://127.0.0.1:1/?connectTimeoutMS=1&serverSelectionTimeoutMS=1"
        ),
        db_name="x",
    )


async def test_beanie_webhook_repository_reports_missing_binding_without_encryptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from providify import IssueKind
    from varco_beanie.config import BeanieSettings
    from varco_beanie.di import bootstrap
    from varco_core.di import bind_instance

    monkeypatch.setenv("VARCO_WEBHOOK_BACKEND", "beanie")

    container = DIContainer()
    bootstrap(container)
    bind_instance(container, BeanieSettings, _beanie_settings())

    report = container.validate(raise_on_error=False)
    missing = [
        issue
        for issue in report.errors
        if issue.kind is IssueKind.MISSING_BINDING and issue.requested == "FieldEncryptor"
    ]
    assert missing, [i.message for i in report.errors]


async def test_beanie_webhook_repository_resolves_with_encryptor_and_reuses_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cryptography.fernet import Fernet
    from varco_beanie.config import BeanieSettings
    from varco_beanie.di import bootstrap
    from varco_beanie.webhook import BeanieWebhookSubscriptionRepository
    from varco_core.di import bind_instance
    from varco_core.encryption import FernetFieldEncryptor, FieldEncryptor
    from varco_core.webhook.base import WebhookSubscriptionRepository

    monkeypatch.setenv("VARCO_WEBHOOK_BACKEND", "beanie")

    container = DIContainer()
    bootstrap(container)
    settings = _beanie_settings()
    bind_instance(container, BeanieSettings, settings)
    encryptor = FernetFieldEncryptor(Fernet.generate_key())
    bind_instance(container, FieldEncryptor, encryptor)

    repo = container.get(WebhookSubscriptionRepository)
    assert isinstance(repo, BeanieWebhookSubscriptionRepository)
    assert repo._encryptor is encryptor  # type: ignore[attr-defined]
    assert repo._client is settings.mongo_client  # type: ignore[attr-defined]
    assert repo._owns_client is False  # type: ignore[attr-defined]


async def test_bound_plaintext_encryptor_is_the_di_path_hatch_and_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Plan 056 / FLIP-13, Step 13 — the DI-path dev hatch end to end (Beanie
    twin of the ``varco_sa`` case).
    """
    posture = pytest.importorskip("varco_fastapi.posture")
    from varco_beanie.config import BeanieSettings
    from varco_beanie.di import bootstrap
    from varco_beanie.webhook import BeanieWebhookSubscriptionRepository
    from varco_core.di import bind_instance
    from varco_core.encryption import FieldEncryptor, PlaintextFieldEncryptor
    from varco_core.webhook.base import WebhookSubscriptionRepository

    monkeypatch.setenv("VARCO_WEBHOOK_BACKEND", "beanie")

    container = DIContainer()
    bootstrap(container)
    bind_instance(container, BeanieSettings, _beanie_settings())
    bind_instance(container, FieldEncryptor, PlaintextFieldEncryptor(acknowledge_plaintext=True))

    repo = container.get(WebhookSubscriptionRepository)
    findings = posture.LocalPostureCollector(webhook_repository=repo).collect()

    assert isinstance(repo, BeanieWebhookSubscriptionRepository)
    assert "posture.webhook_secrets_plaintext" in {f.check for f in findings}
