"""
Plan 056 / FLIP-13, Steps 10 & 14 — ``SAWebhookSubscriptionRepository``
requires a ``FieldEncryptor``; plaintext secrets are never stored silently;
``reencrypt_legacy_plaintext()`` upgrades 3.x rows (red until Step 11).

sqlite (file-backed under ``tmp_path`` so a second engine sees the same
rows for raw SELECT/INSERT), Docker-free.
"""

from __future__ import annotations

import inspect
import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from varco_core.webhook.models import WebhookSubscription
from varco_sa.webhook import SAWebhookSubscriptionRepository


@pytest.fixture
def url(tmp_path: Path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'wh.db'}"


@pytest.fixture
async def raw(url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(url)
    yield eng
    await eng.dispose()


def _plaintext() -> Any:
    from varco_core.encryption import PlaintextFieldEncryptor

    return PlaintextFieldEncryptor(acknowledge_plaintext=True)


def _fernet() -> Any:
    fernet = pytest.importorskip("cryptography.fernet")
    from varco_core.encryption import FernetFieldEncryptor

    return FernetFieldEncryptor(fernet.Fernet.generate_key())


def _sub(secret: str) -> WebhookSubscription:
    return WebhookSubscription(
        tenant_id=f"t-{uuid.uuid4().hex[:8]}",
        target_url="https://example.com/hook",
        event_patterns=["order.created"],
        active_secrets=[secret],
    )


async def _stored_secrets(raw: AsyncEngine, pk: object) -> list[str]:
    async with raw.connect() as conn:
        value = await conn.scalar(
            sa.text("SELECT active_secrets FROM webhook_subscriptions WHERE pk = :pk"),
            {"pk": str(pk)},
        )
    return json.loads(value) if isinstance(value, str) else list(value)


async def _insert_legacy_row(raw: AsyncEngine, secret: str) -> uuid.UUID:
    """Insert a row the 3.x way: active_secrets in plaintext, no hex, no cipher."""
    pk = uuid.uuid4()
    now = datetime.now(UTC)
    async with raw.begin() as conn:
        await conn.execute(
            sa.text(
                "INSERT INTO webhook_subscriptions (pk, tenant_id, target_url, event_patterns, active_secrets, "
                "status, consecutive_failures, signer, custom_headers, created_at, updated_at) VALUES "
                "(:pk, 't-legacy', 'https://example.com/hook', '[]', :secrets, 'ACTIVE', 0, "
                "'standard_webhooks', '{}', :now, :now)"
            ),
            {"pk": str(pk), "secrets": json.dumps([secret]), "now": now},
        )
    return pk


# ── constructor contract ──────────────────────────────────────────────────────


def test_omitting_encryptor_is_a_missing_keyword_type_error(url: str) -> None:
    with pytest.raises(TypeError, match="missing 1 required keyword-only argument: 'encryptor'"):
        SAWebhookSubscriptionRepository(url=url)  # type: ignore[call-arg]


def test_encryptor_none_raises_type_error_naming_every_fix(url: str) -> None:
    with pytest.raises(TypeError) as exc:
        SAWebhookSubscriptionRepository(url=url, encryptor=None)  # type: ignore[arg-type]

    msg = str(exc.value)
    assert "SAWebhookSubscriptionRepository" in msg
    assert "NoneType" in msg
    assert "FernetFieldEncryptor(key)" in msg
    assert "bind_instance" in msg
    assert "PlaintextFieldEncryptor(acknowledge_plaintext=True)" in msg
    assert "reencrypt_legacy_plaintext" in msg


def test_passing_the_key_bytes_instead_of_an_encryptor_raises_type_error(url: str) -> None:
    # The second real mistake: handing over the Fernet key itself.
    with pytest.raises(TypeError, match="got bytes"):
        SAWebhookSubscriptionRepository(url=url, encryptor=b"not-an-encryptor")  # type: ignore[arg-type]


def test_signature_guard() -> None:
    # Class __init__ is invisible to api_surface.py --check.
    param = inspect.signature(SAWebhookSubscriptionRepository.__init__).parameters["encryptor"]

    assert param.kind is inspect.Parameter.KEYWORD_ONLY
    assert param.default is inspect.Parameter.empty
    assert "FieldEncryptor" in str(param.annotation)
    assert "None" not in str(param.annotation)


def test_beanie_signature_guard() -> None:
    beanie_webhook = pytest.importorskip("varco_beanie.webhook")
    param = inspect.signature(
        beanie_webhook.BeanieWebhookSubscriptionRepository.__init__
    ).parameters["encryptor"]

    assert param.kind is inspect.Parameter.KEYWORD_ONLY
    assert param.default is inspect.Parameter.empty
    assert "FieldEncryptor" in str(param.annotation)
    assert "None" not in str(param.annotation)


# ── storage behaviour ─────────────────────────────────────────────────────────


async def test_plaintext_encryptor_round_trip_stores_hex_of_secret(
    url: str, raw: AsyncEngine
) -> None:
    repo = SAWebhookSubscriptionRepository(url=url, encryptor=_plaintext())
    await repo.start()
    try:
        saved = await repo.save(_sub("whsec_plain"))
        found = await repo.find_by_id(saved.pk)
        stored = await _stored_secrets(raw, saved.pk)
    finally:
        await repo.stop()

    assert found is not None and found.active_secrets == ["whsec_plain"]
    assert bytes.fromhex(stored[0]).decode() == "whsec_plain"


async def test_fernet_round_trip_never_stores_the_secret(url: str, raw: AsyncEngine) -> None:
    repo = SAWebhookSubscriptionRepository(url=url, encryptor=_fernet())
    await repo.start()
    try:
        saved = await repo.save(_sub("whsec_secret"))
        found = await repo.find_by_id(saved.pk)
        stored = await _stored_secrets(raw, saved.pk)
    finally:
        await repo.stop()

    assert found is not None and found.active_secrets == ["whsec_secret"]
    assert "whsec_secret" not in stored[0]
    assert stored[0] != b"whsec_secret".hex()


async def test_reencrypt_legacy_plaintext_upgrades_3x_rows_idempotently(
    url: str, raw: AsyncEngine
) -> None:
    repo = SAWebhookSubscriptionRepository(url=url, encryptor=_fernet())
    await repo.start()
    try:
        pk = await _insert_legacy_row(raw, "whsec_legacy")
        with pytest.raises(Exception):  # noqa: B017 - non-hex/invalid token before the upgrade
            await repo.find_by_id(pk)

        first = await repo.reencrypt_legacy_plaintext()
        found = await repo.find_by_id(pk)
        second = await repo.reencrypt_legacy_plaintext()
    finally:
        await repo.stop()

    assert first == 1
    assert found is not None and found.active_secrets == ["whsec_legacy"]
    assert second == 0


async def test_reencrypt_rewrites_rows_written_under_plaintext_encryptor(
    url: str, raw: AsyncEngine
) -> None:
    # The Plaintext -> Fernet switch: run the method with the Fernet repository.
    plain = SAWebhookSubscriptionRepository(url=url, encryptor=_plaintext())
    await plain.start()
    try:
        saved = await plain.save(_sub("whsec_dev"))
    finally:
        await plain.stop()

    repo = SAWebhookSubscriptionRepository(url=url, encryptor=_fernet())
    await repo.start()
    try:
        # A PlaintextFieldEncryptor row is hex of printable text — ambiguous,
        # so the operator states the reading (drift decision on Plan 056).
        rewritten = await repo.reencrypt_legacy_plaintext(legacy_hex=True)
        found = await repo.find_by_id(saved.pk)
        stored = await _stored_secrets(raw, saved.pk)
    finally:
        await repo.stop()

    assert rewritten == 1
    assert found is not None and found.active_secrets == ["whsec_dev"]
    assert stored[0] != b"whsec_dev".hex()


# ── drift decision: ambiguous entries refuse under legacy_hex=None ─────────────


async def test_ambiguous_default_refuses_and_writes_nothing(url: str, raw: AsyncEngine) -> None:
    from varco_core.webhook.base import AmbiguousLegacySecretError

    repo = SAWebhookSubscriptionRepository(url=url, encryptor=_fernet())
    await repo.start()
    try:
        # One unambiguous row that WOULD be rewritten, one ambiguous row.
        clean_pk = await _insert_legacy_row(raw, "whsec_clean")
        ambiguous_pk = await _insert_legacy_row(raw, "41424344")
        with pytest.raises(AmbiguousLegacySecretError) as info:
            await repo.reencrypt_legacy_plaintext()
        clean_after = await _stored_secrets(raw, clean_pk)
        ambiguous_after = await _stored_secrets(raw, ambiguous_pk)
    finally:
        await repo.stop()

    assert info.value.subscription_ids == (str(ambiguous_pk),)
    assert str(ambiguous_pk) in str(info.value)
    # Checked before writing: neither row changed, not even the clean one.
    assert clean_after == ["whsec_clean"]
    assert ambiguous_after == ["41424344"]


async def test_explicit_false_keeps_hex_3x_secret_verbatim(url: str, raw: AsyncEngine) -> None:
    repo = SAWebhookSubscriptionRepository(url=url, encryptor=_fernet())
    await repo.start()
    try:
        pk = await _insert_legacy_row(raw, "41424344")
        rewritten = await repo.reencrypt_legacy_plaintext(legacy_hex=False)
        found = await repo.find_by_id(pk)
    finally:
        await repo.stop()

    assert rewritten == 1
    assert found is not None and found.active_secrets == ["41424344"]


async def test_explicit_true_reads_hex(url: str, raw: AsyncEngine) -> None:
    repo = SAWebhookSubscriptionRepository(url=url, encryptor=_fernet())
    await repo.start()
    try:
        pk = await _insert_legacy_row(raw, "41424344")
        rewritten = await repo.reencrypt_legacy_plaintext(legacy_hex=True)
        found = await repo.find_by_id(pk)
    finally:
        await repo.stop()

    assert rewritten == 1
    assert found is not None and found.active_secrets == ["ABCD"]


async def test_wrong_key_ciphertext_refuses_under_default(url: str, raw: AsyncEngine) -> None:
    from varco_core.webhook.base import AmbiguousLegacySecretError

    writer = SAWebhookSubscriptionRepository(url=url, encryptor=_fernet())
    await writer.start()
    try:
        saved = await writer.save(_sub("whsec_real"))
        before = await _stored_secrets(raw, saved.pk)
    finally:
        await writer.stop()

    other_key = SAWebhookSubscriptionRepository(url=url, encryptor=_fernet())
    await other_key.start()
    try:
        with pytest.raises(AmbiguousLegacySecretError):
            await other_key.reencrypt_legacy_plaintext()
        after = await _stored_secrets(raw, saved.pk)
    finally:
        await other_key.stop()

    assert after == before
