"""
varco_redis.job_store
=====================
Redis-backed implementation of ``AbstractJobStore``.

Persistence model
-----------------
Each ``Job`` is stored as a JSON string at the key::

    {key_prefix}{job_id}

Example: ``varco:job:550e8400-e29b-41d4-a716-446655440000``

Status index
------------
To enable efficient ``list_by_status()``, a Redis Sorted Set is maintained
per status::

    {key_prefix}status:{status}   →  SortedSet { job_id: created_at_timestamp }

Entries are added/removed on every ``save()`` call.  This makes
``list_by_status()`` O(log N + K) (ZRANGEBYSCORE) instead of O(N) (SCAN).

Claim key (for try_claim atomicity)
-------------------------------------
``try_claim()`` uses a claim guard key::

    {key_prefix}claim:{job_id}   →  "1"  (SET NX EX {claim_ttl_seconds})

Only the first caller that successfully sets the claim key proceeds to
transition the job to RUNNING.  All concurrent callers get ``None`` (NX
fails for them).  The EX TTL ensures the claim key expires automatically if
the runner crashes between claiming and updating the job JSON.

DESIGN: JSON string per job over Redis Hash
    ✅ Single GET / SET per read/write — one round-trip.
    ✅ No schema evolution issues — JSON is self-describing.
    ✅ Simple to inspect in redis-cli (GETDEL / GET).
    ❌ Updates replace the entire value — no partial field patching.
       For background jobs (infrequent saves, small payloads) this is fine.

DESIGN: Sorted Set index per status
    ✅ list_by_status() is O(log N + K) — no full SCAN needed.
    ✅ Oldest-first ordering via created_at timestamp as ZSET score.
    ❌ Index and job value can diverge if a write crashes between the two ops.
       This is a known tradeoff — eventual consistency is acceptable for a
       job store (the poller re-examines jobs on the next tick).

Thread safety:  ✅ redis.asyncio.Redis is coroutine-safe across tasks.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🐍 https://redis-py.readthedocs.io/en/stable/commands.html
  redis-py async command reference
- 📐 https://redis.io/docs/latest/commands/set/ (NX, EX options)
  SET NX EX — atomic conditional set with TTL
- 📐 https://redis.io/docs/latest/commands/zadd/
  ZADD — sorted set upsert
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING
from uuid import UUID

from varco_core.job.base import AbstractJobStore, Job, JobStatus
from varco_core.job.task import TaskPayload

if TYPE_CHECKING:
    import redis.asyncio as aioredis

_logger = logging.getLogger(__name__)

# Default TTL in seconds for the claim guard key.
# A runner that crashes between SET NX and the RUNNING update releases the
# claim after this many seconds — preventing permanent lock-out.
_DEFAULT_CLAIM_TTL: int = 30


# ── Serialization helpers ─────────────────────────────────────────────────────


def _dt_to_str(dt: datetime | None) -> str | None:
    """Serialize datetime to ISO-8601 string."""
    return dt.isoformat() if dt is not None else None


def _str_to_dt(s: str | None) -> datetime | None:
    """Deserialize ISO-8601 string to a timezone-aware UTC datetime."""
    if s is None:
        return None
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _job_to_json(job: Job) -> str:
    """Serialize a ``Job`` to a JSON string for Redis storage."""
    return json.dumps(
        {
            "job_id": str(job.job_id),
            "status": job.status.value,
            "created_at": _dt_to_str(job.created_at),
            "started_at": _dt_to_str(job.started_at),
            "completed_at": _dt_to_str(job.completed_at),
            # bytes → hex string; None → None
            "result": job.result.hex() if job.result is not None else None,
            "error": job.error,
            "callback_url": job.callback_url,
            "auth_snapshot": job.auth_snapshot,
            "request_token": job.request_token,
            "metadata": job.metadata,
            "task_payload": (
                job.task_payload.to_dict() if job.task_payload is not None else None
            ),
        }
    )


def _json_to_job(raw: str | bytes) -> Job:
    """Deserialize a JSON string from Redis storage back to a ``Job``."""
    data: dict[str, Any] = json.loads(raw)

    task_payload: TaskPayload | None = None
    if data.get("task_payload") is not None:
        task_payload = TaskPayload.from_dict(data["task_payload"])

    result: bytes | None = None
    if data.get("result") is not None:
        result = bytes.fromhex(data["result"])

    return Job(
        job_id=UUID(data["job_id"]),
        status=JobStatus(data["status"]),
        created_at=_str_to_dt(data["created_at"]) or datetime.now(timezone.utc),
        started_at=_str_to_dt(data.get("started_at")),
        completed_at=_str_to_dt(data.get("completed_at")),
        result=result,
        error=data.get("error"),
        callback_url=data.get("callback_url"),
        auth_snapshot=data.get("auth_snapshot"),
        request_token=data.get("request_token"),
        metadata=data.get("metadata") or {},
        task_payload=task_payload,
    )


# ── RedisJobStore ─────────────────────────────────────────────────────────────


class RedisJobStore(AbstractJobStore):
    """
    Redis-backed implementation of ``AbstractJobStore``.

    Stores each job as a JSON string and maintains a Sorted Set index per
    status for efficient ``list_by_status()`` queries.  Uses ``SET NX EX``
    for atomic ``try_claim()`` without requiring Lua scripts or WATCH/MULTI.

    Thread safety:  ✅ ``redis.asyncio.Redis`` is coroutine-safe.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        client:         An ``aioredis.Redis`` client instance.
        key_prefix:     Namespace prefix for all Redis keys.
                        Default: ``"varco:job:"``.
        claim_ttl:      TTL in seconds for the claim guard key.
                        Default: ``30``.  Shorter values increase the risk of
                        a slow runner losing its claim; longer values increase
                        the lock-out window on crash.

    Edge cases:
        - ``save()`` is not atomic across the JSON SET and the ZSET updates.
          A crash mid-write may leave the status index stale.  The correctness
          impact is minor — the job value is always authoritative; the index
          is a secondary view for ``list_by_status()``.
        - ``try_claim()`` is atomic via the NX claim key.  Two concurrent
          callers for the same ``job_id`` — only one succeeds; the other gets
          ``None``.  The EX TTL bounds the claim window on crashes.
        - ``delete()`` also removes the job from all status index keys.

    Example::

        import redis.asyncio as aioredis
        from varco_redis.job_store import RedisJobStore

        client = aioredis.from_url("redis://localhost:6379/0")
        store = RedisJobStore(client)

        job = Job()
        await store.save(job)
        claimed = await store.try_claim(job.job_id)
    """

    def __init__(
        self,
        client: aioredis.Redis,
        *,
        key_prefix: str = "varco:job:",
        claim_ttl: int = _DEFAULT_CLAIM_TTL,
    ) -> None:
        """
        Args:
            client:     Async Redis client — shared across all operations.
            key_prefix: Prefix for all Redis keys managed by this store.
            claim_ttl:  TTL in seconds for the claim guard key used in
                        ``try_claim()``.
        """
        self._client = client
        self._prefix = key_prefix
        self._claim_ttl = claim_ttl

    # ── Key helpers ───────────────────────────────────────────────────────────

    def _job_key(self, job_id: UUID) -> str:
        """Redis key for a single job's JSON value."""
        return f"{self._prefix}{job_id}"

    def _status_key(self, status: JobStatus) -> str:
        """Redis Sorted Set key for a given status index."""
        return f"{self._prefix}status:{status.value}"

    def _claim_key(self, job_id: UUID) -> str:
        """Redis key for the claim guard (SET NX EX)."""
        return f"{self._prefix}claim:{job_id}"

    # ── AbstractJobStore implementation ───────────────────────────────────────

    async def save(self, job: Job) -> None:
        """
        Persist or update a job (upsert semantics).

        Writes the serialized job JSON and updates the status Sorted Set index.
        If the job's status changed, the old status key is also cleaned up.

        DESIGN: GET old status + SET new + ZADD/ZREM (3 commands, not atomic)
            ✅ No Lua script required — simple and debuggable.
            ✅ Correct under non-concurrent saves (the job runner guarantees
               that only one task holds a job at a time).
            ❌ A crash mid-write leaves the index stale — acceptable tradeoff.

        Args:
            job: The ``Job`` to persist.

        Async safety: ✅ All awaits are independent — no shared lock needed.
        """
        job_key = self._job_key(job.job_id)

        # Read the existing value to detect status changes (for index cleanup).
        existing_raw = await self._client.get(job_key)
        old_status: JobStatus | None = None
        if existing_raw is not None:
            try:
                old_job = _json_to_job(existing_raw)
                old_status = old_job.status
            except Exception:
                pass  # Corrupted value — treat as non-existent

        # Write the updated job JSON.
        await self._client.set(job_key, _job_to_json(job))

        # Update status index: remove from old status set, add to new status set.
        if old_status is not None and old_status != job.status:
            await self._client.zrem(self._status_key(old_status), str(job.job_id))

        # ZADD uses created_at as the score for oldest-first ordering.
        score = job.created_at.timestamp()
        await self._client.zadd(self._status_key(job.status), {str(job.job_id): score})

        _logger.debug("RedisJobStore.save: job_id=%s status=%s", job.job_id, job.status)

    async def get(self, job_id: UUID) -> Job | None:
        """
        Retrieve a ``Job`` by its ``job_id``.

        Args:
            job_id: The unique job identifier.

        Returns:
            The ``Job`` if found, or ``None`` if not in Redis.

        Async safety: ✅ Single GET command.
        """
        raw = await self._client.get(self._job_key(job_id))
        if raw is None:
            return None
        try:
            return _json_to_job(raw)
        except Exception as exc:
            _logger.error(
                "RedisJobStore.get: failed to deserialize job_id=%s: %s", job_id, exc
            )
            return None

    async def list_by_status(
        self,
        status: JobStatus,
        *,
        limit: int = 100,
    ) -> list[Job]:
        """
        Return up to ``limit`` jobs matching ``status``, ordered oldest first.

        Uses the Sorted Set index (score = created_at timestamp) so the query
        is O(log N + K) — no full scan needed.

        Args:
            status: Filter by this lifecycle state.
            limit:  Maximum number of results.

        Returns:
            List of matching ``Job`` objects, ordered by ``created_at ASC``.

        Edge cases:
            - If the index is stale (crash mid-write), the job value is still
              returned if the JSON key exists.  Stale index entries pointing to
              non-existent keys return ``None`` from GET and are skipped.

        Async safety: ✅ ZRANGE + N GETs (pipeline for efficiency).
        """
        status_key = self._status_key(status)
        # ZRANGE with BYSCORE ascending returns oldest IDs first.
        job_id_strs: list[bytes] = await self._client.zrange(status_key, 0, limit - 1)
        if not job_id_strs:
            return []

        jobs: list[Job] = []
        for jid_bytes in job_id_strs:
            raw = await self._client.get(self._job_key(UUID(jid_bytes.decode())))
            if raw is None:
                # Stale index entry — skip.
                continue
            try:
                jobs.append(_json_to_job(raw))
            except Exception as exc:
                _logger.warning(
                    "RedisJobStore.list_by_status: failed to deserialize job %s: %s",
                    jid_bytes,
                    exc,
                )

        _logger.debug(
            "RedisJobStore.list_by_status: status=%s returned %d jobs",
            status,
            len(jobs),
        )
        return jobs

    async def delete(self, job_id: UUID) -> None:
        """
        Remove a ``Job`` from the store and all status indexes.

        Silent no-op if ``job_id`` is not in Redis.

        Args:
            job_id: The unique job identifier.

        Async safety: ✅ GET + DEL + ZREM across all status keys.
        """
        job_key = self._job_key(job_id)
        raw = await self._client.get(job_key)

        # Remove from whichever status index the job currently belongs to.
        if raw is not None:
            try:
                job = _json_to_job(raw)
                await self._client.zrem(self._status_key(job.status), str(job_id))
            except Exception:
                # Corrupted value — remove from all status indexes defensively.
                for s in JobStatus:
                    await self._client.zrem(self._status_key(s), str(job_id))

        await self._client.delete(job_key)
        _logger.debug("RedisJobStore.delete: job_id=%s", job_id)

    async def try_claim(self, job_id: UUID) -> Job | None:
        """
        Atomically claim a PENDING job and transition it to RUNNING.

        Uses a claim guard key with ``SET NX EX`` to ensure that only one
        caller succeeds even under concurrent invocations.

        Steps:
        1. ``SET claim_key "1" NX EX {claim_ttl}`` — atomic claim acquisition.
        2. Read the job JSON (``GET job_key``).
        3. If the job is PENDING, update it to RUNNING and save.
        4. Return the running ``Job``; release claim key on failure.

        DESIGN: claim key (SET NX EX) over WATCH/MULTI/EXEC
            ✅ Simpler than a Redis transaction — one extra key, one extra command.
            ✅ EX TTL auto-expires if the runner crashes after claiming but
               before updating the job — prevents indefinite lock-out.
            ✅ No Lua script required.
            ❌ The claim key and the job JSON are two separate keys — not atomically
               linked.  A crash after NX-SET but before job JSON update leaves the
               job PENDING until the claim TTL expires.  After expiry, another runner
               can re-claim.

        Args:
            job_id: The UUID of the PENDING job to claim.

        Returns:
            The claimed ``Job`` in RUNNING state, or ``None`` if:
            - The job does not exist.
            - The job is not in PENDING state.
            - The claim key was already held by another caller.

        Async safety: ✅ SET NX is atomic on the Redis server.
        """
        claim_key = self._claim_key(job_id)

        # Attempt to acquire the claim guard atomically.
        # SET NX returns True if set (acquired), None/False if already exists.
        acquired = await self._client.set(claim_key, "1", nx=True, ex=self._claim_ttl)
        if not acquired:
            # Another runner already holds the claim for this job.
            _logger.debug(
                "RedisJobStore.try_claim: claim key already held for job_id=%s",
                job_id,
            )
            return None

        try:
            raw = await self._client.get(self._job_key(job_id))
            if raw is None:
                # Job was deleted between claim and read — release and bail.
                return None

            job = _json_to_job(raw)
            if job.status != JobStatus.PENDING:
                # Already running or terminal — another path got here first.
                return None

            # Transition PENDING → RUNNING and persist.
            running_job = job.as_running()
            await self.save(running_job)

            _logger.debug(
                "RedisJobStore.try_claim: claimed job_id=%s → RUNNING", job_id
            )
            return running_job

        except Exception:
            # On any error, release the claim key so another runner can try.
            await self._client.delete(claim_key)
            raise

    def __repr__(self) -> str:
        return f"RedisJobStore(prefix={self._prefix!r}, claim_ttl={self._claim_ttl})"


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "RedisJobStore",
]
