"""
varco_sa.webhook
==================
``SAWebhookSubscriptionRepository`` — SQLAlchemy async
``WebhookSubscriptionRepository`` (Plan 031 / D4a, Step 3).

Same framework-table shape as ``varco_sa.idempotency``/``varco_sa.dlq``: own
``Table``, own ``MetaData``, ``register_framework_metadata()``, a manual
dataclass↔row mapping — never the ``@register`` ORM generator (see
``varco_core.webhook.base``'s module docstring for why).

Secrets encryption
------------------
``active_secrets`` are encrypted at rest via the existing ``FieldEncryptor``
(§D-D4-signing: "no new crypto path"). ``encryptor`` is required since 4.0
(Plan 056 / FLIP-13) — ``None`` or a non-``FieldEncryptor`` is a
``TypeError``. Dev/test: ``PlaintextFieldEncryptor(acknowledge_plaintext=True)``
(reported as ``posture.webhook_secrets_plaintext``). A 3.x deployment with
rows written by 3.x without an encryptor runs
``await repo.reencrypt_legacy_plaintext()`` once after upgrading.

Usage::

    from varco_core.encryption import FernetFieldEncryptor
    from varco_sa.webhook import SAWebhookSubscriptionRepository

    repo = SAWebhookSubscriptionRepository(
        url="postgresql+asyncpg://...", encryptor=FernetFieldEncryptor(key)
    )
    await repo.start()
    ...
    await repo.stop()

Thread safety:  ✅ ``AsyncEngine`` connection pool is coroutine-safe.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import JSON, Column, DateTime, Integer, MetaData, String, Table
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from varco_core.webhook.base import (
    WebhookSubscriptionRepository,
    plan_legacy_reencryption,
    require_field_encryptor,
)
from varco_core.webhook.models import WebhookSubscription

from varco_sa.metadata import register_framework_metadata as _register_fw_metadata

if TYPE_CHECKING:
    from varco_core.encryption import FieldEncryptor

__all__ = ["SAWebhookSubscriptionRepository", "webhook_metadata"]

# Separate MetaData — never pollutes the application's Base.metadata, same
# convention as every other framework table in this package.
webhook_metadata = MetaData()

_subscriptions_table = Table(
    "webhook_subscriptions",
    webhook_metadata,
    Column("pk", PGUUID(as_uuid=True).with_variant(String(36), "sqlite"), primary_key=True),
    Column("tenant_id", String(255), nullable=False, index=True),
    Column("target_url", String(2048), nullable=False),
    Column("event_patterns", JSON, nullable=False),
    # Stored as a JSON list of strings — each entry is the hex of the
    # encryptor's output (see module docstring).
    Column("active_secrets", JSON, nullable=False),
    Column("status", String(16), nullable=False),
    Column("consecutive_failures", Integer, nullable=False, default=0),
    Column("signer", String(32), nullable=False),
    Column("custom_headers", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

_register_fw_metadata("varco_sa.webhook", webhook_metadata)


def _ensure_tz(dt: datetime) -> datetime:
    """Coerce naive datetimes (SQLite) to UTC — same helper as ``varco_sa.dlq``."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


class SAWebhookSubscriptionRepository(WebhookSubscriptionRepository):
    """
    SQLAlchemy async ``WebhookSubscriptionRepository`` backed by
    ``webhook_subscriptions``.

    Args:
        url:           Async SQLAlchemy connection URL. Mutually exclusive
                       with ``engine``. When given, ``start()`` builds a
                       private engine this repository owns and disposes on
                       ``stop()``.
        engine:        An already-constructed ``AsyncEngine`` to reuse (Plan
                       050 / §D-wire row 4-5 — ``SAConfiguration.webhook_repository()``
                       passes the app's own pool). Mutually exclusive with
                       ``url``. ``stop()`` never disposes an engine it did
                       not create — same ``_owns_engine`` convention as
                       ``SAIdempotencyStore``.
        encryptor:     **Required.** The ``FieldEncryptor`` applied to each
                       entry of ``active_secrets`` before it is persisted, and
                       to decrypt on read (Plan 056 / FLIP-13). Dev/test:
                       ``PlaintextFieldEncryptor(acknowledge_plaintext=True)``.
        engine_kwargs: Extra kwargs forwarded to ``create_async_engine()`` —
                       only meaningful with ``url=``.

    Raises:
        TypeError: ``encryptor`` is omitted, ``None``, or not a
            ``FieldEncryptor`` (e.g. the Fernet key ``bytes`` itself) — the
            message names the Fernet one-liner, the DI path, and the dev hatch.
        ValueError: Neither or both of ``url``/``engine`` were given.

    Edge cases:
        - Call ``await repo.start()`` before any other method.
        - Call ``await repo.stop()`` to dispose the engine's pool (only if
          this repository created it).
    """

    def __init__(
        self,
        *,
        url: str | None = None,
        engine: AsyncEngine | None = None,
        encryptor: FieldEncryptor,
        **engine_kwargs: Any,
    ) -> None:
        # Guard first: a plaintext store must never get as far as a URL check.
        encryptor = require_field_encryptor(encryptor, cls_name=type(self).__name__)
        if (url is None) == (engine is None):
            raise ValueError(
                "SAWebhookSubscriptionRepository requires exactly one of url= "
                f"or engine= — got url={url!r}, engine={engine!r}."
            )
        self._url = url
        self._encryptor = encryptor
        self._engine_kwargs = engine_kwargs
        self._engine: AsyncEngine | None = engine
        # Tracks whether THIS repository created the engine (and must
        # therefore dispose it in stop()) — an app-supplied engine
        # (engine=) is never disposed here, even though self._engine is
        # already populated (mirrors SAIdempotencyStore._owns_engine).
        self._owns_engine = engine is None

    def _require_engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError(
                "SAWebhookSubscriptionRepository method called before start(). "
                "Call `await repo.start()` first."
            )
        return self._engine

    async def start(self) -> None:
        """Create the engine (unless one was given) and ensure ``webhook_subscriptions`` exists."""
        if self._engine is None:
            assert self._url is not None  # noqa: S101 - invariant, not user input
            self._engine = create_async_engine(self._url, **self._engine_kwargs)
        async with self._engine.begin() as conn:
            await conn.run_sync(webhook_metadata.create_all, checkfirst=True)

    async def stop(self) -> None:
        """Dispose the engine's connection pool — only if this repository created it."""
        if self._engine is not None and self._owns_engine:
            await self._engine.dispose()
            self._engine = None

    def _encrypt_secret(self, secret: str) -> str:
        return self._encryptor.encrypt(secret.encode("utf-8")).hex()

    def _decrypt_secret(self, stored: str) -> str:
        return self._encryptor.decrypt(bytes.fromhex(stored)).decode("utf-8")

    async def reencrypt_legacy_plaintext(self, *, legacy_hex: bool | None = None) -> int:
        """
        Rewrite every legacy plaintext ``active_secrets`` entry as ciphertext
        under this repository's encryptor — the one-shot 3.x → 4.0 upgrade
        step (Plan 056 / §D-flip13-reencrypt). Not called automatically: a
        data rewrite belongs to the operator's deploy, not to ``start()``.

        An entry that decrypts cleanly is left alone; only a decrypt failure
        marks an entry as plaintext (``reencrypt_legacy_secret``'s rule).
        Plaintext → Fernet switch: run this on the **Fernet** repository
        with ``legacy_hex=True`` — the ``PlaintextFieldEncryptor`` rows fail
        Fernet decryption and are hex of the secret.

        Args:
            legacy_hex: How to read a legacy entry — see
                ``varco_core.webhook.base.reencrypt_legacy_secret``. ``None``
                (default) upgrades only unambiguous entries and refuses the
                batch if any entry is ambiguous; ``True`` — hex of the secret
                (``PlaintextFieldEncryptor`` rows); ``False`` — raw 3.x
                secrets.

        Returns:
            The number of rows rewritten. ``0`` on a second run (idempotent).

        Raises:
            RuntimeError: Called before ``start()``.
            AmbiguousLegacySecretError: ``legacy_hex=None`` and some entry
                is valid hex of printable UTF-8 — names every such
                subscription id; **nothing is written** (the whole table is
                planned before the first update).

        Edge cases:
            - Rows are read and planned in one pass; each changed row is then
              updated in its own transaction, so a crash mid-write leaves a
              mix of upgraded and legacy rows, which a re-run (same
              ``legacy_hex``) finishes.
            - ⚠️ Run it with the encryptor that wrote any existing
              ciphertext: under ``legacy_hex=None`` a wrong key (or a
              destroyed ``kid``) refuses, but under an explicit
              ``True``/``False`` real ciphertext is re-encrypted as the
              "secret" — see ``reencrypt_legacy_secret``'s Edge cases.

        Async safety:   ⚠️ Run once, from a maintenance script
                        (``asyncio.run(repo.reencrypt_legacy_plaintext())``),
                        not concurrently with writers.
        """
        engine = self._require_engine()
        async with engine.connect() as conn:
            rows = (
                await conn.execute(
                    sa.select(_subscriptions_table.c.pk, _subscriptions_table.c.active_secrets)
                )
            ).fetchall()

        # Plan first (raises AmbiguousLegacySecretError before any write).
        plan = plan_legacy_reencryption(
            ((row.pk, row.active_secrets) for row in rows),
            self._encryptor,
            legacy_hex=legacy_hex,
        )
        for pk, new_secrets in plan:
            async with engine.begin() as conn:
                await conn.execute(
                    sa.update(_subscriptions_table)
                    .where(_subscriptions_table.c.pk == pk)
                    .values(active_secrets=new_secrets, updated_at=datetime.now(UTC))
                )
        return len(plan)

    def _row_to_entity(self, row: Any) -> WebhookSubscription:
        entity = WebhookSubscription(
            tenant_id=row.tenant_id,
            target_url=row.target_url,
            event_patterns=list(row.event_patterns),
            active_secrets=[self._decrypt_secret(s) for s in row.active_secrets],
            status=row.status,
            consecutive_failures=row.consecutive_failures,
            signer=row.signer,
            custom_headers=dict(row.custom_headers),
            created_at=_ensure_tz(row.created_at),
            updated_at=_ensure_tz(row.updated_at),
        )
        entity.pk = row.pk if isinstance(row.pk, UUID) else UUID(str(row.pk))
        entity._raw_orm = row
        return entity

    async def save(self, subscription: WebhookSubscription) -> WebhookSubscription:
        """See ``WebhookSubscriptionRepository.save()``."""
        engine = self._require_engine()
        now = datetime.now(UTC)
        encrypted_secrets = [self._encrypt_secret(s) for s in subscription.active_secrets]

        if subscription.pk is None:
            subscription.pk = uuid4()
            async with engine.begin() as conn:
                await conn.execute(
                    sa.insert(_subscriptions_table).values(
                        pk=str(subscription.pk),
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
                )
        else:
            async with engine.begin() as conn:
                await conn.execute(
                    sa.update(_subscriptions_table)
                    .where(_subscriptions_table.c.pk == str(subscription.pk))
                    .values(
                        tenant_id=subscription.tenant_id,
                        target_url=subscription.target_url,
                        event_patterns=list(subscription.event_patterns),
                        active_secrets=encrypted_secrets,
                        status=subscription.status,
                        consecutive_failures=subscription.consecutive_failures,
                        signer=subscription.signer,
                        custom_headers=dict(subscription.custom_headers),
                        updated_at=now,
                    )
                )
        subscription._raw_orm = object()
        found = await self.find_by_id(subscription.pk)
        assert found is not None  # we just wrote it
        return found

    async def find_by_id(self, pk: object) -> WebhookSubscription | None:
        """See ``WebhookSubscriptionRepository.find_by_id()``."""
        engine = self._require_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                sa.select(_subscriptions_table).where(_subscriptions_table.c.pk == str(pk))
            )
            row = result.fetchone()
        return self._row_to_entity(row) if row is not None else None

    async def find_by_tenant(self, tenant_id: str) -> list[WebhookSubscription]:
        """See ``WebhookSubscriptionRepository.find_by_tenant()``."""
        engine = self._require_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                sa.select(_subscriptions_table).where(_subscriptions_table.c.tenant_id == tenant_id)
            )
            rows = result.fetchall()
        return [self._row_to_entity(r) for r in rows]

    async def find_active_matching(
        self, event_type: str, *, tenant_id: str | None = None
    ) -> list[WebhookSubscription]:
        """See ``WebhookSubscriptionRepository.find_active_matching()``."""
        import fnmatch

        engine = self._require_engine()
        query = sa.select(_subscriptions_table).where(_subscriptions_table.c.status == "ACTIVE")
        if tenant_id is not None:
            query = query.where(_subscriptions_table.c.tenant_id == tenant_id)
        async with engine.connect() as conn:
            result = await conn.execute(query)
            rows = result.fetchall()
        return [
            self._row_to_entity(r)
            for r in rows
            if any(fnmatch.fnmatch(event_type, p) for p in r.event_patterns)
        ]

    async def delete(self, pk: object) -> None:
        """See ``WebhookSubscriptionRepository.delete()``."""
        engine = self._require_engine()
        async with engine.begin() as conn:
            await conn.execute(
                sa.delete(_subscriptions_table).where(_subscriptions_table.c.pk == str(pk))
            )
