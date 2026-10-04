"""
Plan 056 / FLIP-13, Step 10 — Beanie twin of
``varco_sa/tests/test_webhook_repository_encryptor_required.py`` (red until
Step 11).

Constructor tests are Docker-free (no ``start()``); the round-trip and
``reencrypt_legacy_plaintext()`` legs are ``@pytest.mark.integration`` on the
shared ``mongo_url`` container with a per-test ``db_name``.
"""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from varco_beanie.webhook import BeanieWebhookSubscriptionRepository
from varco_core.webhook.models import WebhookSubscription

_URL = "mongodb://127.0.0.1:1"  # never connected — constructor tests do not start()


def _plaintext():
    from varco_core.encryption import PlaintextFieldEncryptor

    return PlaintextFieldEncryptor(acknowledge_plaintext=True)


def _fernet():
    fernet = pytest.importorskip("cryptography.fernet")
    from varco_core.encryption import FernetFieldEncryptor

    return FernetFieldEncryptor(fernet.Fernet.generate_key())


def _db() -> str:
    return f"wh_{uuid4().hex[:8]}"


def _sub(secret: str) -> WebhookSubscription:
    return WebhookSubscription(
        tenant_id=f"t-{uuid4().hex[:8]}",
        target_url="https://example.com/hook",
        event_patterns=["order.created"],
        active_secrets=[secret],
    )


# ── constructor contract (Docker-free) ───────────────────────────────────────


def test_omitting_encryptor_is_a_missing_keyword_type_error() -> None:
    with pytest.raises(TypeError, match="missing 1 required keyword-only argument: 'encryptor'"):
        BeanieWebhookSubscriptionRepository(url=_URL, db_name="x")  # type: ignore[call-arg]


def test_encryptor_none_raises_type_error_naming_every_fix() -> None:
    with pytest.raises(TypeError) as exc:
        BeanieWebhookSubscriptionRepository(url=_URL, db_name="x", encryptor=None)  # type: ignore[arg-type]

    msg = str(exc.value)
    assert "BeanieWebhookSubscriptionRepository" in msg
    assert "NoneType" in msg
    assert "FernetFieldEncryptor(key)" in msg
    assert "bind_instance" in msg
    assert "PlaintextFieldEncryptor(acknowledge_plaintext=True)" in msg
    assert "reencrypt_legacy_plaintext" in msg


def test_passing_key_bytes_raises_type_error() -> None:
    with pytest.raises(TypeError, match="got bytes"):
        BeanieWebhookSubscriptionRepository(url=_URL, db_name="x", encryptor=b"key")  # type: ignore[arg-type]


def test_messages_are_shared_with_the_sa_backend() -> None:
    # One constant in varco_core.webhook.base so the two texts cannot drift.
    from varco_core.webhook.base import _ENCRYPTOR_REQUIRED

    with pytest.raises(TypeError) as exc:
        BeanieWebhookSubscriptionRepository(url=_URL, db_name="x", encryptor=None)  # type: ignore[arg-type]

    assert str(exc.value) == _ENCRYPTOR_REQUIRED.format(
        cls="BeanieWebhookSubscriptionRepository", got="NoneType"
    )


def test_plaintext_encryptor_constructs_without_connecting() -> None:
    repo = BeanieWebhookSubscriptionRepository(url=_URL, db_name="x", encryptor=_plaintext())

    assert repo is not None


def test_signature_guard() -> None:
    param = inspect.signature(BeanieWebhookSubscriptionRepository.__init__).parameters["encryptor"]

    assert param.kind is inspect.Parameter.KEYWORD_ONLY
    assert param.default is inspect.Parameter.empty
    assert "FieldEncryptor" in str(param.annotation)
    assert "None" not in str(param.annotation)


# ── storage behaviour (real MongoDB) ─────────────────────────────────────────


async def _stored(pk: object) -> list[str]:
    from varco_beanie.webhook import WebhookSubscriptionDocument

    doc = await WebhookSubscriptionDocument.get(pk)
    assert doc is not None
    return list(doc.active_secrets)


async def _insert_legacy(secret: str):
    from varco_beanie.webhook import WebhookSubscriptionDocument

    now = datetime.now(UTC)
    doc = WebhookSubscriptionDocument(
        tenant_id="t-legacy",
        target_url="https://example.com/hook",
        active_secrets=[secret],  # the 3.x plaintext shape
        created_at=now,
        updated_at=now,
    )
    await doc.insert()
    return doc.id


@pytest.mark.integration
async def test_plaintext_round_trip_stores_hex(mongo_url: str) -> None:
    repo = BeanieWebhookSubscriptionRepository(url=mongo_url, db_name=_db(), encryptor=_plaintext())
    await repo.start()
    try:
        saved = await repo.save(_sub("whsec_plain"))
        found = await repo.find_by_id(saved.pk)
        stored = await _stored(saved.pk)
    finally:
        await repo.stop()

    assert found is not None and found.active_secrets == ["whsec_plain"]
    assert bytes.fromhex(stored[0]).decode() == "whsec_plain"


@pytest.mark.integration
async def test_fernet_round_trip_never_stores_the_secret(mongo_url: str) -> None:
    repo = BeanieWebhookSubscriptionRepository(url=mongo_url, db_name=_db(), encryptor=_fernet())
    await repo.start()
    try:
        saved = await repo.save(_sub("whsec_secret"))
        found = await repo.find_by_id(saved.pk)
        stored = await _stored(saved.pk)
    finally:
        await repo.stop()

    assert found is not None and found.active_secrets == ["whsec_secret"]
    assert "whsec_secret" not in stored[0]


@pytest.mark.integration
async def test_reencrypt_legacy_plaintext_is_idempotent(mongo_url: str) -> None:
    repo = BeanieWebhookSubscriptionRepository(url=mongo_url, db_name=_db(), encryptor=_fernet())
    await repo.start()
    try:
        pk = await _insert_legacy("whsec_legacy")
        with pytest.raises(Exception):  # noqa: B017 - non-hex before the upgrade
            await repo.find_by_id(pk)

        first = await repo.reencrypt_legacy_plaintext()
        found = await repo.find_by_id(pk)
        second = await repo.reencrypt_legacy_plaintext()
    finally:
        await repo.stop()

    assert first == 1
    assert found is not None and found.active_secrets == ["whsec_legacy"]
    assert second == 0


@pytest.mark.integration
async def test_reencrypt_rewrites_rows_written_under_plaintext_encryptor(mongo_url: str) -> None:
    db = _db()
    plain = BeanieWebhookSubscriptionRepository(url=mongo_url, db_name=db, encryptor=_plaintext())
    await plain.start()
    try:
        saved = await plain.save(_sub("whsec_dev"))
    finally:
        await plain.stop()

    repo = BeanieWebhookSubscriptionRepository(url=mongo_url, db_name=db, encryptor=_fernet())
    await repo.start()
    try:
        # PlaintextFieldEncryptor rows are hex of printable text — ambiguous
        # under the default, so the reading is stated (drift decision, 056).
        rewritten = await repo.reencrypt_legacy_plaintext(legacy_hex=True)
        found = await repo.find_by_id(saved.pk)
    finally:
        await repo.stop()

    assert rewritten == 1
    assert found is not None and found.active_secrets == ["whsec_dev"]


@pytest.mark.integration
async def test_ambiguous_default_refuses_and_writes_nothing(mongo_url: str) -> None:
    from varco_core.webhook.base import AmbiguousLegacySecretError

    repo = BeanieWebhookSubscriptionRepository(url=mongo_url, db_name=_db(), encryptor=_fernet())
    await repo.start()
    try:
        clean_pk = await _insert_legacy("whsec_clean")
        ambiguous_pk = await _insert_legacy("41424344")
        with pytest.raises(AmbiguousLegacySecretError) as info:
            await repo.reencrypt_legacy_plaintext()
        clean_after = await _stored(clean_pk)
        ambiguous_after = await _stored(ambiguous_pk)
    finally:
        await repo.stop()

    assert info.value.subscription_ids == (str(ambiguous_pk),)
    assert clean_after == ["whsec_clean"]
    assert ambiguous_after == ["41424344"]


@pytest.mark.integration
@pytest.mark.parametrize(("legacy_hex", "expected"), [(True, "ABCD"), (False, "41424344")])
async def test_explicit_choice_resolves_ambiguity(
    mongo_url: str, legacy_hex: bool, expected: str
) -> None:
    repo = BeanieWebhookSubscriptionRepository(url=mongo_url, db_name=_db(), encryptor=_fernet())
    await repo.start()
    try:
        pk = await _insert_legacy("41424344")
        rewritten = await repo.reencrypt_legacy_plaintext(legacy_hex=legacy_hex)
        found = await repo.find_by_id(pk)
    finally:
        await repo.stop()

    assert rewritten == 1
    assert found is not None and found.active_secrets == [expected]
