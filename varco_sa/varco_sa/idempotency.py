"""
varco_sa.idempotency
=====================
``SAIdempotencyStore`` — SQLAlchemy async ``AbstractIdempotencyStore``
(Plan 029 / D1b, Step 11).

Atomicity (§D-D1-atomic) comes from a ``UNIQUE(key)`` primary key plus
catching ``IntegrityError`` on a losing concurrent ``INSERT`` — the same
pattern ``SADeduplicator``'s upsert uses, adapted here to distinguish
"lost the race, someone else is still running" (``IN_FLIGHT``) from "lost
the race, they already finished" (``REPLAY``).

Table
-----
``varco_idempotency`` — one row per key::

    from varco_sa.idempotency import idempotency_metadata
    target_metadata = [Base.metadata, idempotency_metadata]

Columns: ``key`` (PK), ``fingerprint``, ``state`` (``reserved``/
``completed``), ``status``, ``body``, ``headers`` (JSON), ``created_at``,
``expires_at``.

DESIGN: owns its own engine (``url=`` + ``start()``/``stop()``) rather than
    accepting a pre-built ``AsyncEngine`` like ``SAJobStore``/
    ``SADeadLetterQueue``
    ✅ Matches ``RedisIdempotencyStore``'s constructor shape
       (``url=``-first) — a caller wiring D1 across backends configures
       every store the same way.
    ❌ Diverges from this package's usual "caller owns the engine"
       convention (``SAJobStore(engine)``, ``SADeadLetterQueue(engine)``).
       Accepted: idempotency stores are typically a single, dedicated,
       small-footprint resource (unlike the job store / DLQ, which often
       share the application's primary engine) — owning a small private
       engine sized for point lookups is a reasonable, explicit trade-off,
       and ``start()``/``stop()`` make the lifecycle obvious rather than
       implicit.

Usage::

    from varco_sa.idempotency import SAIdempotencyStore

    store = SAIdempotencyStore(url="postgresql+asyncpg://...")
    await store.start()          # creates the engine + ensures the table
    outcome = await store.reserve("order-42", fingerprint, ttl=86400.0)
    await store.stop()           # disposes the engine

Thread safety:  ✅ ``AsyncEngine`` connection pool is coroutine-safe.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Column, DateTime, Integer, LargeBinary, MetaData, String, Table
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from varco_core.idempotency.base import AbstractIdempotencyStore, ReserveOutcome
from varco_core.idempotency.record import IdempotencyRecord

from varco_sa.metadata import register_framework_metadata as _register_fw_metadata

# ── Table schema ──────────────────────────────────────────────────────────────

# Separate MetaData so varco_idempotency never pollutes the application's
# Base.metadata — same pattern as every other framework table in this
# package (outbox, jobs, dlq, dedup log, ...).
idempotency_metadata = MetaData()

_idempotency_table = Table(
    "varco_idempotency",
    idempotency_metadata,
    Column("key", String(255), primary_key=True),
    Column("fingerprint", String(64), nullable=False),
    # "reserved" | "completed" — see AbstractIdempotencyStore.ReserveOutcome.
    Column("state", String(16), nullable=False),
    Column("status", Integer, nullable=True),
    Column("body", LargeBinary, nullable=True),
    Column("headers", sa.JSON, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False, index=True),
)

_register_fw_metadata("varco_sa.idempotency", idempotency_metadata)


def _ensure_tz(dt: datetime) -> datetime:
    """Coerce naive datetimes (SQLite) to UTC — same helper as ``varco_sa.dlq``."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


class SAIdempotencyStore(AbstractIdempotencyStore):
    """
    SQLAlchemy async ``AbstractIdempotencyStore`` backed by
    ``varco_idempotency``.

    Args:
        url:          Async SQLAlchemy connection URL, e.g.
                      ``"postgresql+asyncpg://..."``.
        engine_kwargs: Extra keyword arguments forwarded to
                      ``create_async_engine()``.

    Edge cases:
        - Call ``await store.start()`` before any other method — it builds
          the engine and calls ``ensure_table()``. Calling any other method
          first raises ``RuntimeError``.
        - Call ``await store.stop()`` to dispose the engine's connection
          pool when done (e.g. app shutdown).

    Thread safety:  ✅ ``AsyncEngine`` connection pool is coroutine-safe.
    Async safety:   ✅ All methods are ``async def``.
    """

    def __init__(
        self,
        *,
        url: str | None = None,
        engine: AsyncEngine | None = None,
        **engine_kwargs: Any,
    ) -> None:
        """
        Args:
            url: Async SQLAlchemy connection URL. Mutually exclusive with
                ``engine``. When given, ``start()`` builds a private engine
                this store owns and disposes on ``stop()``.
            engine: An already-constructed ``AsyncEngine`` to reuse (Plan
                047 N2 — ``SAConfiguration.idempotency_store()`` passes the app's
                own pool). Mutually exclusive with ``url``. ``stop()``
                never disposes an engine it did not create.
            **engine_kwargs: Forwarded to ``create_async_engine()`` — only
                meaningful with ``url=``.

        Raises:
            ValueError: Neither or both of ``url``/``engine`` were given.

        DESIGN: additive `engine=` kwarg, `url=` stays first-keyword-optional
            ✅ Reuses the app's own connection pool when DI-constructed
               (N2) — a second, small-footprint engine per store is no
               longer forced.
            ✅ `stop()` disposing only a self-created engine means an
               app-supplied engine survives this store's teardown, matching
               `SAJobStore`/`SADeadLetterQueue`'s "caller owns the engine"
               convention for the `engine=` path specifically.
            ❌ One more constructor branch/guard than a single-source-of-
               truth constructor. Accepted — every existing `url=` caller
               is unaffected (additive).
        """
        if (url is None) == (engine is None):
            raise ValueError(
                "SAIdempotencyStore requires exactly one of url= or engine= — "
                f"got url={url!r}, engine={engine!r}."
            )
        self._url = url
        self._engine_kwargs = engine_kwargs
        self._engine: AsyncEngine | None = engine
        # Tracks whether THIS store created the engine (and must therefore
        # dispose it in stop()) — an app-supplied engine (engine=) is never
        # disposed here, even though self._engine is already populated.
        self._owns_engine = engine is None

    def _require_engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError(
                "SAIdempotencyStore method called before start(). Call `await store.start()` first."
            )
        return self._engine

    async def start(self) -> None:
        """Create the engine (unless one was given) and ensure ``varco_idempotency`` exists (idempotent)."""
        if self._engine is None:
            # __init__'s exactly-one-of guard guarantees self._url is set
            # whenever self._engine was not given.
            assert self._url is not None  # noqa: S101 - invariant, not user input
            self._engine = create_async_engine(self._url, **self._engine_kwargs)
        async with self._engine.begin() as conn:
            await conn.run_sync(idempotency_metadata.create_all, checkfirst=True)

    async def stop(self) -> None:
        """Dispose the engine's connection pool — only if this store created it."""
        if self._engine is not None and self._owns_engine:
            await self._engine.dispose()
            self._engine = None

    async def reserve(self, key: str, fingerprint: str, *, ttl: float) -> ReserveOutcome:
        """See ``AbstractIdempotencyStore.reserve()``."""
        if ttl <= 0:
            raise ValueError(f"reserve() ttl must be > 0, got {ttl!r}.")
        engine = self._require_engine()
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=ttl)

        acquired = await self._try_insert(engine, key, fingerprint, now, expires_at)
        if acquired:
            return ReserveOutcome.ACQUIRED

        async with engine.connect() as conn:
            result = await conn.execute(
                sa.select(_idempotency_table).where(_idempotency_table.c.key == key)
            )
            row = result.fetchone()

        if row is None:
            # Deleted between our failed INSERT and this SELECT (e.g. an
            # expiry sweep) — retry once; the unique constraint still makes
            # this race-safe overall.
            acquired = await self._try_insert(engine, key, fingerprint, now, expires_at)
            return ReserveOutcome.ACQUIRED if acquired else ReserveOutcome.IN_FLIGHT

        if _ensure_tz(row.expires_at) <= now:
            # TTL elapsed — replace the stale row and treat as fresh.
            async with engine.begin() as conn:
                await conn.execute(
                    sa.delete(_idempotency_table).where(_idempotency_table.c.key == key)
                )
            acquired = await self._try_insert(engine, key, fingerprint, now, expires_at)
            return ReserveOutcome.ACQUIRED if acquired else ReserveOutcome.IN_FLIGHT

        if row.state == "completed":
            return ReserveOutcome.REPLAY
        return ReserveOutcome.IN_FLIGHT

    async def _try_insert(
        self,
        engine: AsyncEngine,
        key: str,
        fingerprint: str,
        now: datetime,
        expires_at: datetime,
    ) -> bool:
        """Attempt the atomic INSERT; return ``True`` iff it won the race."""
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    sa.insert(_idempotency_table).values(
                        key=key,
                        fingerprint=fingerprint,
                        state="reserved",
                        status=None,
                        body=None,
                        headers=None,
                        created_at=now,
                        expires_at=expires_at,
                    )
                )
            return True
        except IntegrityError:
            return False

    async def complete(self, key: str, record: IdempotencyRecord) -> None:
        """See ``AbstractIdempotencyStore.complete()``."""
        engine = self._require_engine()
        async with engine.begin() as conn:
            await conn.execute(
                sa.update(_idempotency_table)
                .where(_idempotency_table.c.key == key)
                .values(
                    state="completed",
                    fingerprint=record.fingerprint,
                    status=record.status,
                    body=record.body,
                    headers=dict(record.headers),
                )
            )

    async def get(self, key: str) -> IdempotencyRecord | None:
        """See ``AbstractIdempotencyStore.get()``."""
        engine = self._require_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                sa.select(_idempotency_table).where(_idempotency_table.c.key == key)
            )
            row = result.fetchone()
        if row is None or row.state != "completed":
            return None
        if _ensure_tz(row.expires_at) <= datetime.now(UTC):
            return None
        return IdempotencyRecord(
            status=row.status,
            body=row.body or b"",
            headers=row.headers or {},
            fingerprint=row.fingerprint,
            created_at=_ensure_tz(row.created_at),
        )

    async def release(self, key: str) -> None:
        """See ``AbstractIdempotencyStore.release()``."""
        engine = self._require_engine()
        async with engine.begin() as conn:
            await conn.execute(sa.delete(_idempotency_table).where(_idempotency_table.c.key == key))

    async def delete_expired(self) -> int:
        """See ``AbstractIdempotencyStore.delete_expired()`` — a real sweep,
        since SQL has no native per-row TTL."""
        engine = self._require_engine()
        now = datetime.now(UTC)
        async with engine.begin() as conn:
            result = await conn.execute(
                sa.delete(_idempotency_table).where(_idempotency_table.c.expires_at < now)
            )
        return result.rowcount or 0


__all__ = ["SAIdempotencyStore", "idempotency_metadata"]
