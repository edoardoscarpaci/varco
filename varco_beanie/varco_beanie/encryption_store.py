"""
varco_beanie.encryption_store
==============================
Beanie (pymongo / MongoDB) implementation of ``EncryptionKeyStore``.

Persistence model
-----------------
Each ``EncryptionKeyEntry`` is stored as a MongoDB document in the
``varco_encryption_keys`` collection::

    {
        "_id":           "<kid>",          # MongoDB _id = kid
        "algorithm":     "fernet",
        "key_material":  "<base64url>",    # possibly wrapped with master KEK
        "created_at":    ISODate(...),
        "tenant_id":     "<str> | null",
        "is_primary":    true | false,
        "wrapped":       true | false
    }

An index on ``tenant_id`` makes ``load_for_tenant`` efficient without a
full collection scan.

DESIGN: kid as MongoDB _id
  ✅ O(1) single-document lookup via the clustered ``_id`` index
  ✅ Upsert via ``save()`` (Beanie upserts documents with pre-set ``id``)
  ✅ Delete via ``delete()`` on the retrieved document
  ❌ MongoDB ``_id`` is immutable once inserted — renaming a kid requires
     a delete + re-insert.  In practice, kids are UUIDs and never renamed.

DESIGN: list_tenants via Python deduplication
  ✅ No MongoDB aggregate required — key count is always small
  ✅ Works on any MongoDB deployment (standalone, replica set, Atlas)
  ❌ Slightly more data transferred than a ``distinct`` call for large stores.
     At key-management scale (tens to hundreds of entries) this is negligible.

Registration
-----------
``EncryptionKeyDocument`` must be included in your ``init_beanie()``
(or ``BeanieRepositoryProvider.register()``) call::

    from varco_beanie.encryption_store import EncryptionKeyDocument

    # Option A — init_beanie directly
    await init_beanie(database=db, document_models=[..., EncryptionKeyDocument])

    # Option B — BeanieRepositoryProvider
    provider.register(EncryptionKeyDocument)
    await provider.init()

Usage::

    from pymongo import AsyncMongoClient
    from beanie import init_beanie
    from varco_beanie.encryption_store import BeanieEncryptionKeyStore, EncryptionKeyDocument

    client = AsyncMongoClient("mongodb://localhost:27017")
    await init_beanie(database=client["mydb"], document_models=[EncryptionKeyDocument])

    store = BeanieEncryptionKeyStore()
    manager = EncryptionKeyManager(store, master_encryptor=kek)
    registry = await manager.build_tenant_registry()

Thread safety:  ✅ pymongo AsyncMongoClient connection pool is thread-safe.
                   BeanieEncryptionKeyStore itself is stateless after construction.
Async safety:   ✅ All methods are async and await Beanie Document operations.
"""

from __future__ import annotations

from datetime import UTC, datetime

from beanie import Document
from pydantic import Field

from varco_core.encryption_store import EncryptionKeyEntry


# ── EncryptionKeyDocument — Beanie Document ───────────────────────────────────


class EncryptionKeyDocument(Document):
    """
    Beanie document for the ``varco_encryption_keys`` MongoDB collection.

    Maps 1:1 to ``EncryptionKeyEntry``.  The ``id`` field (MongoDB ``_id``)
    is set to the ``kid`` so lookups by kid are O(1) via the primary index.

    Attributes:
        id:           Key ID — used as MongoDB ``_id``.
        algorithm:    Encryption algorithm (currently only ``"fernet"``).
        key_material: Base64url-encoded key bytes, possibly wrapped with KEK.
        created_at:   UTC creation timestamp; used for rotation ordering.
        tenant_id:    Tenant this key belongs to.  ``None`` = global key.
        is_primary:   Whether this is the active key for its tenant.
        wrapped:      Whether ``key_material`` was encrypted with a master KEK.

    Thread safety:  ✅ Stateless document class.
    Async safety:   ✅ All Beanie operations are async.

    Edge cases:
        - ``id`` is set to ``kid`` at creation time — never auto-generated.
        - MongoDB ``_id`` is immutable; to "rename" a kid, delete and re-insert.
        - Beanie's ``save()`` upserts when ``id`` is already set — safe for
          idempotent key rotation writes.
    """

    # kid → MongoDB _id; using str keeps ObjectId coercion out of our model
    id: str = Field(alias="_id")  # type: ignore[assignment]

    # Encryption algorithm hint — "fernet" only for now
    algorithm: str

    # base64url key material (possibly wrapped with KEK)
    key_material: str

    # UTC creation time — stored as timezone-aware datetime
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # None = global / app-level key
    tenant_id: str | None = None

    # True = this is the active key for its tenant
    is_primary: bool = True

    # True = key_material encrypted with master KEK
    wrapped: bool = False

    class Settings:
        # Collection name — matches the SA table name for cross-backend consistency
        name = "varco_encryption_keys"

        # Index on tenant_id — makes load_for_tenant O(N_tenant) not O(N_total)
        # Beanie 2.x manages indexes via init_beanie() at startup
        indexes = ["tenant_id"]

    def __repr__(self) -> str:
        return (
            f"EncryptionKeyDocument("
            f"kid={self.id!r}, "
            f"tenant_id={self.tenant_id!r}, "
            f"is_primary={self.is_primary}, "
            f"wrapped={self.wrapped})"
        )


# ── BeanieEncryptionKeyStore ──────────────────────────────────────────────────


class BeanieEncryptionKeyStore:
    """
    Beanie/MongoDB async ``EncryptionKeyStore``.

    Satisfies the ``EncryptionKeyStore`` structural protocol from
    ``varco_core.encryption_store`` without inheriting from it.

    Prerequisites
    -------------
    ``EncryptionKeyDocument`` must be registered with ``init_beanie()``
    *before* any method on this store is called — Beanie requires document
    models to be initialised before queries can execute.

    DESIGN: stateless after construction
      ✅ No connection management in the store — the pymongo client is
         managed externally (by ``init_beanie`` or ``BeanieRepositoryProvider``).
      ✅ Safe to create multiple instances — all point to the same collection
         via Beanie's global document registry.
      ❌ Requires init_beanie() to be called before any operation — calling
         before initialisation raises Beanie's ``CollectionWasNotInitialized``.

    Thread safety:  ✅ pymongo connection pool is thread-safe; store is stateless.
    Async safety:   ✅ All methods are async and await Beanie Document operations.

    Edge cases:
        - ``save()`` uses Beanie's ``save()`` which upserts by ``_id`` — safe
          to call multiple times with the same kid (rotation / demote workflow).
        - ``load_for_tenant(None)`` uses ``tenant_id == None`` which compiles to
          ``{"tenant_id": null}`` in MongoDB — only matches explicitly-null
          tenant_id (not missing fields).  New documents always set
          ``tenant_id`` explicitly so this is consistent.
        - ``list_tenants()`` loads all non-null tenant_id values and deduplicates
          in Python — efficient since key counts are always small.
        - ``delete()`` is a no-op if the kid does not exist.

    Example::

        from pymongo import AsyncMongoClient
        from beanie import init_beanie
        from varco_beanie.encryption_store import BeanieEncryptionKeyStore

        client = AsyncMongoClient("mongodb://localhost:27017")
        await init_beanie(database=client["mydb"], document_models=[EncryptionKeyDocument])

        store = BeanieEncryptionKeyStore()
    """

    def __repr__(self) -> str:
        return "BeanieEncryptionKeyStore()"

    # ── EncryptionKeyStore protocol ───────────────────────────────────────────

    async def save(self, entry: EncryptionKeyEntry) -> None:
        """
        Upsert an ``EncryptionKeyEntry`` by kid.

        Uses Beanie's ``save()`` which performs an upsert when the document
        has a pre-set ``id`` — if a document with this ``kid`` exists it is
        replaced, otherwise a new document is inserted.

        Args:
            entry: Entry to persist.

        Raises:
            beanie.exceptions.CollectionWasNotInitialized: ``init_beanie()``
                was not called before this method.
            pymongo.errors.PyMongoError: Connection or write failure.

        Thread safety:  ✅ Each call is independent; no shared state.
        Async safety:   ✅ Awaits Beanie's async save().
        """
        # Convert created_at to UTC-aware datetime — MongoDB stores datetime
        # without timezone info internally but Beanie/pydantic may receive
        # a timezone-aware object; we normalise to UTC to be safe.
        created_at = entry.created_at
        if created_at.tzinfo is None:
            # Assume UTC if somehow a naive datetime slipped through
            created_at = created_at.replace(tzinfo=UTC)

        doc = EncryptionKeyDocument(
            id=entry.kid,  # kid → MongoDB _id
            algorithm=entry.algorithm,
            key_material=entry.key_material,
            created_at=created_at,
            tenant_id=entry.tenant_id,
            is_primary=entry.is_primary,
            wrapped=entry.wrapped,
        )

        # save() upserts by _id — safe to call for new and existing kids
        await doc.save()

    async def load(self, kid: str) -> EncryptionKeyEntry | None:
        """
        Load a single entry by kid.

        Args:
            kid: Key ID to retrieve (MongoDB _id).

        Returns:
            ``EncryptionKeyEntry`` or ``None`` when not found.

        Raises:
            pymongo.errors.PyMongoError: Connection failure.

        Thread safety:  ✅ Read-only.
        Async safety:   ✅ Awaits Beanie's async get().
        """
        doc = await EncryptionKeyDocument.get(kid)
        if doc is None:
            return None
        return _doc_to_entry(doc)

    async def load_for_tenant(self, tenant_id: str | None) -> list[EncryptionKeyEntry]:
        """
        Load all entries for ``tenant_id``, ordered by ``created_at`` ascending.

        Args:
            tenant_id: Tenant ID or ``None`` for global keys.

        Returns:
            List of entries sorted by ``created_at`` ascending.

        Thread safety:  ✅ Read-only.
        Async safety:   ✅ Awaits Beanie find + to_list().

        Edge cases:
            - ``tenant_id=None`` matches documents where ``tenant_id`` is
              explicitly ``null`` — not missing fields.  All documents created
              by this store always set the field explicitly, so this is safe.
        """
        # Use dict-based queries so unit tests can patch EncryptionKeyDocument.find
        # without needing init_beanie() to set up field expression descriptors.
        # "+created_at" is Beanie's string sort syntax (ascending).
        if tenant_id is None:
            # None → match explicit null in MongoDB
            docs = (
                await EncryptionKeyDocument.find({"tenant_id": None})
                .sort("+created_at")
                .to_list()
            )
        else:
            docs = (
                await EncryptionKeyDocument.find({"tenant_id": tenant_id})
                .sort("+created_at")
                .to_list()
            )

        return [_doc_to_entry(doc) for doc in docs]

    async def list_tenants(self) -> list[str]:
        """
        Return sorted list of distinct non-None tenant IDs.

        Loads all documents with a non-null ``tenant_id`` and deduplicates
        in Python — efficient at key-management scale (tens to hundreds of entries).

        Returns:
            Sorted list of tenant ID strings.

        Thread safety:  ✅ Read-only.
        Async safety:   ✅ Awaits Beanie find + to_list().

        Edge cases:
            - An empty store returns ``[]``.
            - Global keys (``tenant_id=None``) are excluded.
        """
        # Dict-based query avoids needing init_beanie() for field descriptors.
        # {"tenant_id": {"$ne": None}} excludes documents with null tenant_id.
        docs = await EncryptionKeyDocument.find({"tenant_id": {"$ne": None}}).to_list()

        # Deduplicate in Python — small dataset; avoids MongoDB aggregate
        seen: set[str] = set()
        for doc in docs:
            if doc.tenant_id is not None:
                seen.add(doc.tenant_id)

        return sorted(seen)

    async def delete(self, kid: str) -> None:
        """
        Remove an entry by kid.  No-op if not found.

        Args:
            kid: Key ID to remove.

        Thread safety:  ✅ Stateless write.
        Async safety:   ✅ Awaits Beanie get() + delete().
        """
        doc = await EncryptionKeyDocument.get(kid)
        if doc is None:
            return  # idempotent — already gone
        await doc.delete()


# ── Serialisation helper ──────────────────────────────────────────────────────


def _doc_to_entry(doc: EncryptionKeyDocument) -> EncryptionKeyEntry:
    """
    Convert a ``EncryptionKeyDocument`` to an ``EncryptionKeyEntry``.

    Args:
        doc: Beanie document loaded from MongoDB.

    Returns:
        Populated ``EncryptionKeyEntry``.

    Edge cases:
        - ``created_at`` may be timezone-naive (MongoDB stores UTC but may not
          carry tzinfo through pymongo).  We normalise to UTC-aware.
    """
    created_at = doc.created_at
    # Normalise to UTC-aware — pymongo may return naive datetime for UTC values
    if hasattr(created_at, "tzinfo") and created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)

    return EncryptionKeyEntry(
        kid=doc.id,  # MongoDB _id → kid
        algorithm=doc.algorithm,
        key_material=doc.key_material,
        created_at=created_at,
        tenant_id=doc.tenant_id,
        is_primary=doc.is_primary,
        wrapped=doc.wrapped,
    )
