"""
varco_beanie.webhook
======================
``BeanieWebhookSubscriptionRepository`` — MongoDB/Beanie
``WebhookSubscriptionRepository`` (Plan 031 / D4a, Step 3).

DESIGN: self-managed ``AsyncIOMotorClient`` + ``init_beanie`` over the usual
    "app calls ``init_beanie()`` once at startup" convention every other
    ``varco_beanie`` document uses (``DeduplicationDocument``,
    ``AuditDocument``, …)
    ✅ Mirrors ``SAWebhookSubscriptionRepository``'s ``url=`` +
       ``start()``/``stop()`` shape — a caller wiring D4 across backends
       constructs both repositories identically, and a webhook
       subscription registry is a small, standalone, framework-owned
       resource (like the idempotency store) rather than an application
       document sharing the app's primary ``init_beanie()`` call.
    ❌ Diverges from every other ``varco_beanie`` module's convention.
       Accepted for this one resource — see
       ``varco_core.webhook.base``'s module docstring for the same
       reasoning applied on the SQLAlchemy side.

Secrets encryption
------------------
Same convention as ``varco_sa.webhook``: ``active_secrets`` are encrypted
via a ``FieldEncryptor``, **required** since 4.0 (Plan 056 / FLIP-13) —
``None`` or a non-``FieldEncryptor`` is a ``TypeError``. Dev/test:
``PlaintextFieldEncryptor(acknowledge_plaintext=True)`` (reported as
``posture.webhook_secrets_plaintext``). A 3.x deployment with rows written
by 3.x without an encryptor runs
``await repo.reencrypt_legacy_plaintext()`` once after upgrading.

Usage::

    repo = BeanieWebhookSubscriptionRepository(
        url="mongodb://...", db_name="app", encryptor=FernetFieldEncryptor(key)
    )

Thread safety:  ⚠️ One repository instance owns one Motor client — do not
                share across event loops.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from beanie import Document, init_beanie
from pydantic import Field
from pymongo import ASCENDING, IndexModel
from varco_core.webhook.base import (
    WebhookSubscriptionRepository,
    plan_legacy_reencryption,
    require_field_encryptor,
)
from varco_core.webhook.models import WebhookSubscription

if TYPE_CHECKING:
    from varco_core.encryption import FieldEncryptor

__all__ = ["BeanieWebhookSubscriptionRepository", "WebhookSubscriptionDocument"]


class WebhookSubscriptionDocument(Document):
    """
    Beanie document backing ``BeanieWebhookSubscriptionRepository``.

    Register it in your ``init_beanie()`` call if sharing the app's
    connection instead of using this repository's self-managed
    ``start()``/``stop()`` lifecycle::

        await init_beanie(database=db, document_models=[..., WebhookSubscriptionDocument])
    """

    # Beanie's own default `id` type is `PydanticObjectId` — overridden to a
    # plain UUID so it matches `DomainModel.pk`'s `UUID_AUTO` strategy, same
    # convention as every other UUID-keyed document in this package
    # (AuditDocument, DeduplicationDocument, InboxDocument, ...).
    id: UUID = Field(default_factory=uuid4)  # type: ignore[assignment]

    tenant_id: str
    target_url: str
    event_patterns: list[str] = Field(default_factory=list)
    # Hex of the repository encryptor's output (see module docstring).
    active_secrets: list[str] = Field(default_factory=list)
    status: str = "ACTIVE"
    consecutive_failures: int = 0
    signer: str = "standard_webhooks"
    custom_headers: dict[str, str] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime

    class Settings:
        name = "webhook_subscriptions"
        indexes = [
            IndexModel([("tenant_id", ASCENDING)]),
        ]


class BeanieWebhookSubscriptionRepository(WebhookSubscriptionRepository):
    """
    MongoDB/Beanie ``WebhookSubscriptionRepository``.

    Args:
        url:        Mongo connection URL. Mutually exclusive with ``client``.
                    When given, ``start()`` opens a private Motor client this
                    repository owns and closes on ``stop()``.
        client:     An already-constructed Mongo client to reuse (Plan 050 /
                    §D-wire row 4-5 — ``BeanieConfiguration.webhook_repository()``
                    passes the app's own ``BeanieSettings.mongo_client``).
                    Mutually exclusive with ``url``. ``stop()`` never closes
                    a client it did not create — same ``_owns_client``
                    convention ``SAWebhookSubscriptionRepository`` uses for
                    an injected engine.
        db_name:    Database name to initialize Beanie against.
        encryptor:  **Required.** The ``FieldEncryptor`` for
                    ``active_secrets`` (Plan 056 / FLIP-13; see module
                    docstring). Dev/test:
                    ``PlaintextFieldEncryptor(acknowledge_plaintext=True)``.

    Raises:
        TypeError: ``encryptor`` is omitted, ``None``, or not a
            ``FieldEncryptor`` — the message names every fix.
        ValueError: Neither or both of ``url``/``client`` were given.

    Edge cases:
        - Call ``await repo.start()`` before any other method — it opens
          the Motor client (unless one was given) and calls ``init_beanie()``
          scoped to ``WebhookSubscriptionDocument`` only.
        - Call ``await repo.stop()`` to close the client (only if this
          repository created it).
    """

    def __init__(
        self,
        *,
        url: str | None = None,
        client: Any = None,
        db_name: str,
        encryptor: FieldEncryptor,
    ) -> None:
        # Guard first: a plaintext store must never get as far as a URL check.
        encryptor = require_field_encryptor(encryptor, cls_name=type(self).__name__)
        if (url is None) == (client is None):
            raise ValueError(
                "BeanieWebhookSubscriptionRepository requires exactly one of "
                f"url= or client= — got url={url!r}, client={client!r}."
            )
        self._url = url
        self._db_name = db_name
        self._encryptor = encryptor
        self._client: Any = client
        # Tracks whether THIS repository opened the client (and must
        # therefore close it in stop()) — an app-supplied client (client=)
        # is never closed here, even though self._client is populated.
        self._owns_client = client is None

    async def start(self) -> None:
        """Open the Motor client (unless one was given) and initialize Beanie for this document."""
        if self._client is None:
            from motor.motor_asyncio import AsyncIOMotorClient

            self._client = AsyncIOMotorClient(self._url)
        await init_beanie(
            database=self._client[self._db_name],
            document_models=[WebhookSubscriptionDocument],
        )

    async def stop(self) -> None:
        """Close the Motor client — only if this repository created it."""
        if self._client is not None and self._owns_client:
            self._client.close()
            self._client = None

    def _encrypt_secret(self, secret: str) -> str:
        return self._encryptor.encrypt(secret.encode("utf-8")).hex()

    def _decrypt_secret(self, stored: str) -> str:
        return self._encryptor.decrypt(bytes.fromhex(stored)).decode("utf-8")

    async def reencrypt_legacy_plaintext(self, *, legacy_hex: bool | None = None) -> int:
        """
        Rewrite every legacy plaintext ``active_secrets`` entry as ciphertext
        under this repository's encryptor — the one-shot 3.x → 4.0 upgrade
        step (Plan 056 / §D-flip13-reencrypt). Not called automatically.

        Same rule as ``SAWebhookSubscriptionRepository.reencrypt_legacy_plaintext``
        (``varco_core.webhook.base.reencrypt_legacy_secret``): only a decrypt
        failure marks an entry as plaintext. Plaintext → Fernet switch: run
        this on the **Fernet** repository with ``legacy_hex=True``.

        Args:
            legacy_hex: How to read a legacy entry — see
                ``varco_core.webhook.base.reencrypt_legacy_secret``. ``None``
                (default) upgrades only unambiguous entries and refuses the
                batch if any entry is ambiguous; ``True`` — hex of the secret
                (``PlaintextFieldEncryptor`` rows); ``False`` — raw 3.x
                secrets.

        Returns:
            The number of documents rewritten. ``0`` on a second run.

        Raises:
            beanie.exceptions.CollectionWasNotInitialized: Called before
                ``start()`` (``init_beanie`` has not run).
            AmbiguousLegacySecretError: ``legacy_hex=None`` and some entry
                is valid hex of printable UTF-8 — names every such
                subscription id; **nothing is written** (every document is
                planned before the first save).

        Edge cases:
            - Call after ``start()``; documents are loaded and planned first,
              then each changed document is saved on its own, so an
              interrupted write phase is finished by a re-run.
            - ⚠️ Run it with the encryptor that wrote any existing
              ciphertext: under ``legacy_hex=None`` a wrong key (or a
              destroyed ``kid``) refuses, but under an explicit
              ``True``/``False`` real ciphertext is re-encrypted as the
              "secret" — see ``reencrypt_legacy_secret``'s Edge cases.

        Async safety:   ⚠️ Run once, from a maintenance script, not
                        concurrently with writers.
        """
        docs = await WebhookSubscriptionDocument.find_all().to_list()
        by_id = {doc.id: doc for doc in docs}
        # Plan first (raises AmbiguousLegacySecretError before any save).
        plan = plan_legacy_reencryption(
            ((doc.id, doc.active_secrets) for doc in docs),
            self._encryptor,
            legacy_hex=legacy_hex,
        )
        for doc_id, new_secrets in plan:
            doc = by_id[doc_id]
            doc.active_secrets = new_secrets
            doc.updated_at = datetime.now(UTC)
            await doc.save()
        return len(plan)

    def _doc_to_entity(self, doc: WebhookSubscriptionDocument) -> WebhookSubscription:
        entity = WebhookSubscription(
            tenant_id=doc.tenant_id,
            target_url=doc.target_url,
            event_patterns=list(doc.event_patterns),
            active_secrets=[self._decrypt_secret(s) for s in doc.active_secrets],
            status=doc.status,
            consecutive_failures=doc.consecutive_failures,
            signer=doc.signer,
            custom_headers=dict(doc.custom_headers),
            created_at=doc.created_at,
            updated_at=doc.updated_at,
        )
        entity.pk = doc.id if isinstance(doc.id, UUID) else UUID(str(doc.id))
        entity._raw_orm = doc
        return entity

    async def save(self, subscription: WebhookSubscription) -> WebhookSubscription:
        """See ``WebhookSubscriptionRepository.save()``."""
        now = datetime.now(UTC)
        encrypted_secrets = [self._encrypt_secret(s) for s in subscription.active_secrets]

        if subscription.pk is None:
            subscription.pk = uuid4()
            doc = WebhookSubscriptionDocument(
                id=subscription.pk,
                tenant_id=subscription.tenant_id,
                target_url=subscription.target_url,
                event_patterns=list(subscription.event_patterns),
                active_secrets=encrypted_secrets,
                status=subscription.status,
                consecutive_failures=subscription.consecutive_failures,
                signer=subscription.signer,
                custom_headers=dict(subscription.custom_headers),
                created_at=now,
                updated_at=now,
            )
            await doc.insert()
        else:
            doc = await WebhookSubscriptionDocument.get(subscription.pk)
            if doc is None:
                raise ValueError(f"WebhookSubscription {subscription.pk} not found.")
            doc.tenant_id = subscription.tenant_id
            doc.target_url = subscription.target_url
            doc.event_patterns = list(subscription.event_patterns)
            doc.active_secrets = encrypted_secrets
            doc.status = subscription.status
            doc.consecutive_failures = subscription.consecutive_failures
            doc.signer = subscription.signer
            doc.custom_headers = dict(subscription.custom_headers)
            doc.updated_at = now
            await doc.save()

        return self._doc_to_entity(doc)

    async def find_by_id(self, pk: object) -> WebhookSubscription | None:
        """See ``WebhookSubscriptionRepository.find_by_id()``."""
        doc = await WebhookSubscriptionDocument.get(pk)
        return self._doc_to_entity(doc) if doc is not None else None

    async def find_by_tenant(self, tenant_id: str) -> list[WebhookSubscription]:
        """See ``WebhookSubscriptionRepository.find_by_tenant()``."""
        docs = await WebhookSubscriptionDocument.find(
            WebhookSubscriptionDocument.tenant_id == tenant_id
        ).to_list()
        return [self._doc_to_entity(d) for d in docs]

    async def find_active_matching(
        self, event_type: str, *, tenant_id: str | None = None
    ) -> list[WebhookSubscription]:
        """See ``WebhookSubscriptionRepository.find_active_matching()``."""
        import fnmatch

        query = WebhookSubscriptionDocument.find(WebhookSubscriptionDocument.status == "ACTIVE")
        if tenant_id is not None:
            query = query.find(WebhookSubscriptionDocument.tenant_id == tenant_id)
        docs = await query.to_list()
        return [
            self._doc_to_entity(d)
            for d in docs
            if any(fnmatch.fnmatch(event_type, p) for p in d.event_patterns)
        ]

    async def delete(self, pk: object) -> None:
        """See ``WebhookSubscriptionRepository.delete()``."""
        doc = await WebhookSubscriptionDocument.get(pk)
        if doc is not None:
            await doc.delete()
