"""
varco_redis.encryption_store
=============================
Redis-backed ``EncryptionKeyStore`` implementation.

Persistence model
-----------------
Each ``EncryptionKeyEntry`` is stored as a Redis Hash at the key::

    {prefix}:key:{kid}

Fields of the hash map 1:1 to ``EncryptionKeyEntry`` attributes.

Tenant indexing is maintained via Redis Sets::

    {prefix}:tenant:{tenant_id}   →  Set{kid1, kid2, ...}

This allows ``load_for_tenant(tenant_id)`` in O(|tenants|) hashes instead of
scanning all keys.  The Set is kept in sync on every ``save`` and ``delete``.

DESIGN: Redis Hash + Set indices over a single flat key per entry
  ✅ Hash allows partial updates (not needed here, but ergonomic)
  ✅ Set index makes ``load_for_tenant`` efficient without SCAN
  ✅ TTL can be set on the Hash for automatic expiry (not default, but possible)
  ❌ Two Redis calls per write (Hash + Set update) — not atomic without Lua.
     For key management (startup / rotation) this is acceptable; it is not a
     hot path.

Thread safety:  ✅ ``redis.asyncio`` client is safe to share across coroutines.
Async safety:   ✅ All methods are async.

Usage::

    import redis.asyncio as aioredis
    from varco_redis.encryption_store import RedisEncryptionKeyStore

    redis_client = aioredis.from_url("redis://localhost:6379")
    store = RedisEncryptionKeyStore(redis_client)

    manager = EncryptionKeyManager(store, master_encryptor=kek)
    registry = await manager.build_tenant_registry()
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.encryption_store import EncryptionKeyEntry

if TYPE_CHECKING:
    import redis.asyncio as aioredis


# ── RedisEncryptionKeyStore ────────────────────────────────────────────────────


class RedisEncryptionKeyStore:
    """
    Redis-backed async ``EncryptionKeyStore``.

    Args:
        client: An ``aioredis.Redis`` client (from ``redis.asyncio.from_url()``
                or ``StrictRedis()``).  The client should be pre-configured with
                the correct host, port, and auth credentials.
        prefix: Namespace prefix for all keys.  Defaults to ``"varco:enc"``.
                Override when sharing a Redis instance between multiple apps or
                environments (e.g. ``"myapp:prod:enc"``).

    DESIGN: json.dumps for hash field values
      ✅ Human-readable in Redis CLI / RedisInsight for debugging
      ✅ No external serialiser dependency
      ❌ Slightly more overhead than msgpack — acceptable for key management

    Thread safety:  ✅ aioredis.Redis client is coroutine-safe.
    Async safety:   ✅ All methods are async.

    Edge cases:
        - ``save()`` is NOT atomic across hash write + set update.  A crashed
          pod between the two writes leaves the Set inconsistent.  The
          ``_repair_tenant_index()`` helper can be called at startup to rebuild
          the Set from all existing Hash keys.
        - ``delete()`` removes the entry from both the Hash key and the tenant
          Set.  If the tenant Set becomes empty it is also deleted (Redis
          does this automatically when the last member is removed).
        - ``prefix`` must not contain ``:`` at the end — the store appends it
          itself.

    Example::

        import redis.asyncio as aioredis

        client = aioredis.from_url("redis://localhost:6379/0")
        store = RedisEncryptionKeyStore(client, prefix="myapp:enc")
    """

    def __init__(
        self,
        client: aioredis.Redis,
        *,
        prefix: str = "varco:enc",
    ) -> None:
        self._client = client
        # Normalise prefix — strip trailing colon if provided by mistake
        self._prefix = prefix.rstrip(":")

    def __repr__(self) -> str:
        return f"RedisEncryptionKeyStore(prefix={self._prefix!r})"

    # ── Key-name helpers ──────────────────────────────────────────────────────

    def _hash_key(self, kid: str) -> str:
        """Redis Hash key for a single entry: ``{prefix}:key:{kid}``."""
        return f"{self._prefix}:key:{kid}"

    def _tenant_set_key(self, tenant_id: str) -> str:
        """Redis Set key for tenant index: ``{prefix}:tenant:{tenant_id}``."""
        return f"{self._prefix}:tenant:{tenant_id}"

    def _all_tenants_set_key(self) -> str:
        """Redis Set tracking all distinct tenant IDs: ``{prefix}:tenants``."""
        return f"{self._prefix}:tenants"

    # ── EncryptionKeyStore protocol ───────────────────────────────────────────

    async def save(self, entry: EncryptionKeyEntry) -> None:
        """
        Persist an ``EncryptionKeyEntry``.

        Two Redis operations:
        1. ``HSET {prefix}:key:{kid}`` — store/update all entry fields.
        2. If ``entry.tenant_id`` is not None:
           ``SADD {prefix}:tenant:{tenant_id} {kid}`` — update tenant index.
           ``SADD {prefix}:tenants {tenant_id}`` — update global tenant set.

        Not atomic — see class docstring for edge cases.

        Args:
            entry: Entry to persist.

        Raises:
            redis.RedisError: Connection or command failure.

        Thread safety:  ✅ Each call is independent.
        Async safety:   ✅ Awaits aioredis commands.
        """
        hash_key = self._hash_key(entry.kid)
        # Serialise all fields as strings — Redis Hash values are always bytes/str
        mapping = _entry_to_redis_mapping(entry)
        await self._client.hset(hash_key, mapping=mapping)  # type: ignore[arg-type]

        # Update tenant index — O(1) SADD per tenant
        if entry.tenant_id is not None:
            await self._client.sadd(self._tenant_set_key(entry.tenant_id), entry.kid)
            await self._client.sadd(self._all_tenants_set_key(), entry.tenant_id)
        else:
            # Global key — add to dedicated global Set so load_for_tenant(None) works
            await self._client.sadd(f"{self._prefix}:global", entry.kid)

    async def load(self, kid: str) -> EncryptionKeyEntry | None:
        """
        Load a single entry by ``kid``.

        Args:
            kid: Key ID to retrieve.

        Returns:
            ``EncryptionKeyEntry`` or ``None`` when not found.

        Raises:
            redis.RedisError: Connection failure.

        Thread safety:  ✅ Read-only.
        Async safety:   ✅ Awaits aioredis HGETALL.
        """
        hash_key = self._hash_key(kid)
        # HGETALL returns {} when the key does not exist — not None
        raw = await self._client.hgetall(hash_key)
        if not raw:
            return None
        return _redis_mapping_to_entry(raw)

    async def load_for_tenant(self, tenant_id: str | None) -> list[EncryptionKeyEntry]:
        """
        Load all entries for ``tenant_id``.

        Uses the tenant Set index to avoid a full SCAN of all hash keys.

        Args:
            tenant_id: Tenant ID or ``None`` for global keys.

        Returns:
            Entries sorted by ``created_at`` ascending.

        Thread safety:  ✅ Read-only.
        Async safety:   ✅ Awaits aioredis commands.

        Edge cases:
            - Global keys (``tenant_id=None``) have no Set index.  They are
              stored with the field ``tenant_id`` as the literal string
              ``"__global__"``.  ``load_for_tenant(None)`` uses a dedicated
              global set ``{prefix}:global`` for indexing.
            - An inconsistent tenant Set (crashed mid-write) may return kids
              that no longer have a corresponding Hash.  These are silently
              skipped.
        """
        if tenant_id is None:
            # Global keys use a dedicated set key
            kids_raw = await self._client.smembers(f"{self._prefix}:global")
        else:
            kids_raw = await self._client.smembers(self._tenant_set_key(tenant_id))

        if not kids_raw:
            return []

        # Load each entry by kid; skip tombstoned entries (Set/Hash inconsistency)
        entries: list[EncryptionKeyEntry] = []
        for kid_bytes in kids_raw:
            kid = (
                kid_bytes.decode("utf-8") if isinstance(kid_bytes, bytes) else kid_bytes
            )
            entry = await self.load(kid)
            if entry is not None:
                entries.append(entry)

        return sorted(entries, key=lambda e: e.created_at)

    async def list_tenants(self) -> list[str]:
        """
        Return sorted list of distinct non-None tenant IDs.

        Uses the global ``{prefix}:tenants`` Set — O(N) where N is number of
        distinct tenants (not total entries).

        Returns:
            Sorted list of tenant ID strings.

        Thread safety:  ✅ Read-only.
        Async safety:   ✅ Awaits aioredis SMEMBERS.
        """
        raw = await self._client.smembers(self._all_tenants_set_key())
        tenants = [(t.decode("utf-8") if isinstance(t, bytes) else t) for t in raw]
        return sorted(tenants)

    async def delete(self, kid: str) -> None:
        """
        Remove an entry by ``kid``.  No-op if not found.

        Removes:
        1. The Hash at ``{prefix}:key:{kid}``.
        2. The kid from the tenant Set (if applicable).
        3. Removes ``tenant_id`` from the global tenants Set if the tenant Set
           is now empty.

        Args:
            kid: Key ID to remove.

        Thread safety:  ✅ Write operations.
        Async safety:   ✅ Awaits aioredis commands.
        """
        hash_key = self._hash_key(kid)

        # Load first to get tenant_id before deleting the Hash
        raw = await self._client.hgetall(hash_key)
        if not raw:
            return  # already gone — no-op

        entry = _redis_mapping_to_entry(raw)

        # Delete the Hash
        await self._client.delete(hash_key)

        # Remove from tenant index
        if entry.tenant_id is not None:
            tenant_set_key = self._tenant_set_key(entry.tenant_id)
            await self._client.srem(tenant_set_key, kid)

            # Remove tenant from global set if no more kids for this tenant
            remaining = await self._client.scard(tenant_set_key)
            if remaining == 0:
                await self._client.srem(self._all_tenants_set_key(), entry.tenant_id)
        else:
            # Global key — remove from global Set
            await self._client.srem(f"{self._prefix}:global", kid)

    # ── Maintenance helpers ────────────────────────────────────────────────────

    async def _repair_tenant_index(self) -> None:
        """
        Rebuild the tenant Set indices by scanning all Hash keys.

        Call this at startup if a previous run crashed mid-write and left the
        Sets inconsistent.  This is O(N) where N = number of stored keys —
        acceptable for startup, not for request handling.

        Thread safety:  ⚠️ Mutates Sets; not safe to run concurrently with save/delete.
        Async safety:   ✅ Awaits aioredis SCAN + HGETALL + SADD.
        """
        scan_pattern = f"{self._prefix}:key:*"
        cursor: int = 0

        # SCAN iterates without blocking the Redis event loop (vs KEYS)
        while True:
            cursor, keys = await self._client.scan(
                cursor, match=scan_pattern, count=100
            )
            for key in keys:
                raw = await self._client.hgetall(key)
                if not raw:
                    continue
                try:
                    entry = _redis_mapping_to_entry(raw)
                except Exception:
                    # Malformed entry — skip
                    continue

                if entry.tenant_id is not None:
                    await self._client.sadd(
                        self._tenant_set_key(entry.tenant_id), entry.kid
                    )
                    await self._client.sadd(
                        self._all_tenants_set_key(), entry.tenant_id
                    )
                else:
                    await self._client.sadd(f"{self._prefix}:global", entry.kid)

            if cursor == 0:
                break


# ── Serialisation helpers ──────────────────────────────────────────────────────


def _entry_to_redis_mapping(entry: EncryptionKeyEntry) -> dict[str, str]:
    """
    Serialise an ``EncryptionKeyEntry`` to a flat ``{str: str}`` mapping
    suitable for ``HSET``.

    Args:
        entry: Entry to serialise.

    Returns:
        Dict mapping Redis Hash field names to string values.

    Edge cases:
        - ``tenant_id=None`` is serialised as the literal ``"__global__"``
          to avoid storing an empty/null string in Redis (which could be
          confused with a tenant named ``""``).
        - ``is_primary`` and ``wrapped`` are stored as ``"1"`` / ``"0"``
          for compactness.
    """
    return {
        "kid": entry.kid,
        "algorithm": entry.algorithm,
        "key_material": entry.key_material,
        "created_at": entry.created_at.isoformat(),
        # None serialised as sentinel string — Redis cannot store Python None
        "tenant_id": entry.tenant_id if entry.tenant_id is not None else "__global__",
        "is_primary": "1" if entry.is_primary else "0",
        "wrapped": "1" if entry.wrapped else "0",
    }


def _redis_mapping_to_entry(
    raw: dict[bytes | str, bytes | str],
) -> EncryptionKeyEntry:
    """
    Deserialise a Redis Hash mapping (from ``HGETALL``) to an ``EncryptionKeyEntry``.

    Args:
        raw: Dict returned by ``client.hgetall()`` — keys and values may be
             ``bytes`` or ``str`` depending on ``decode_responses`` setting.

    Returns:
        Populated ``EncryptionKeyEntry``.

    Raises:
        KeyError: A required field is missing from ``raw``.

    Edge cases:
        - Both bytes and str keys/values are handled — ``decode_responses=True``
          gives str; ``decode_responses=False`` (default) gives bytes.
        - ``"__global__"`` tenant_id sentinel is mapped back to ``None``.
    """

    def _str(v: bytes | str) -> str:
        return v.decode("utf-8") if isinstance(v, bytes) else v

    # Normalise all keys and values to str
    data = {_str(k): _str(v) for k, v in raw.items()}

    # Restore the None sentinel for global keys
    tenant_raw = data.get("tenant_id")
    tenant_id: str | None = None if tenant_raw in (None, "__global__") else tenant_raw

    return EncryptionKeyEntry.from_dict(
        {
            "kid": data["kid"],
            "algorithm": data["algorithm"],
            "key_material": data["key_material"],
            "created_at": data["created_at"],
            "tenant_id": tenant_id,
            "is_primary": data.get("is_primary", "1") == "1",
            "wrapped": data.get("wrapped", "0") == "1",
        }
    )
