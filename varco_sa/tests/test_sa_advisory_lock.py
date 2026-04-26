"""
Unit tests for varco_sa.advisory_lock
========================================
Covers ``SAAdvisoryLock`` and ``_key_to_int64`` using a mocked AsyncEngine.

No real PostgreSQL connection is required — all ``pg_try_advisory_lock`` and
``pg_advisory_unlock`` SQL calls are mocked at the ``AsyncConnection.execute``
level.

DESIGN: Mock-based testing over SQLite
    ``pg_try_advisory_lock`` is PostgreSQL-specific and not available on SQLite.
    Integration tests (requiring a real PostgreSQL) are marked with
    ``@pytest.mark.integration`` and are excluded from the default test run.

Sections
--------
- ``_key_to_int64``           — deterministic hash, range, empty key
- ``SAAdvisoryLock`` construction
- ``try_acquire``             — acquired; contended; connection error; empty key
- ``release``                 — success; token mismatch; double release
- ``LockHandle`` context mgr  — auto-release on exit
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from varco_core.lock import LockHandle
from varco_sa.advisory_lock import SAAdvisoryLock, _key_to_int64


# ── _key_to_int64 ─────────────────────────────────────────────────────────────


class TestKeyToInt64:
    def test_deterministic(self) -> None:
        """Same key always produces the same int64."""
        assert _key_to_int64("inventory:42") == _key_to_int64("inventory:42")

    def test_different_keys_produce_different_ints(self) -> None:
        assert _key_to_int64("key-A") != _key_to_int64("key-B")

    def test_result_in_signed_int64_range(self) -> None:
        """Result must fit in a signed int64 (PostgreSQL bigint)."""
        val = _key_to_int64("some-lock-key")
        assert 0 <= val <= 0x7FFFFFFFFFFFFFFF

    def test_empty_string_does_not_raise(self) -> None:
        """Empty key produces a valid hash (validation is done upstream)."""
        val = _key_to_int64("")
        assert isinstance(val, int)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_mock_engine(acquired: bool = True, unlock_result: bool = True):
    """
    Build a MagicMock AsyncEngine whose ``connect()`` returns a context-manager
    that yields a mock connection.

    The mock connection's ``execute()`` returns ``acquired`` for
    ``pg_try_advisory_lock`` and ``unlock_result`` for ``pg_advisory_unlock``.
    """
    # Result mock — .scalar() returns the boolean value.
    result_mock = MagicMock()
    result_mock.scalar.return_value = acquired

    unlock_result_mock = MagicMock()
    unlock_result_mock.scalar.return_value = unlock_result

    conn_mock = AsyncMock()
    conn_mock.execute = AsyncMock(side_effect=[result_mock, unlock_result_mock])
    conn_mock.close = AsyncMock()

    engine_mock = MagicMock()
    engine_mock.connect = AsyncMock(return_value=conn_mock)

    return engine_mock, conn_mock


# ── Construction ──────────────────────────────────────────────────────────────


class TestSAAdvisoryLockConstruction:
    def test_repr_contains_class_name(self) -> None:
        engine = MagicMock()
        lock = SAAdvisoryLock(engine)
        assert "SAAdvisoryLock" in repr(lock)

    def test_initial_held_count_is_zero(self) -> None:
        engine = MagicMock()
        lock = SAAdvisoryLock(engine)
        assert len(lock._held) == 0


# ── try_acquire ───────────────────────────────────────────────────────────────


class TestSAAdvisoryLockTryAcquire:
    async def test_try_acquire_returns_handle_when_acquired(self) -> None:
        """pg_try_advisory_lock returns True → LockHandle returned."""
        engine, conn = _make_mock_engine(acquired=True)
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("my-key", ttl=30)

        assert handle is not None
        assert isinstance(handle, LockHandle)
        assert handle.key == "my-key"
        conn.close.assert_not_called()  # Connection kept open while held

    async def test_try_acquire_returns_none_when_contended(self) -> None:
        """pg_try_advisory_lock returns False → None returned, connection closed."""
        engine, conn = _make_mock_engine(acquired=False)
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("my-key", ttl=30)

        assert handle is None
        conn.close.assert_called_once()

    async def test_try_acquire_empty_key_raises(self) -> None:
        engine = MagicMock()
        lock = SAAdvisoryLock(engine)
        with pytest.raises(ValueError, match="non-empty"):
            await lock.try_acquire("", ttl=30)

    async def test_try_acquire_connection_error_returns_none(self) -> None:
        """Connection failure → None returned (treated as contention)."""
        engine = MagicMock()
        engine.connect = AsyncMock(side_effect=Exception("DB unreachable"))
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("my-key", ttl=30)
        assert handle is None

    async def test_try_acquire_stores_token_in_held_dict(self) -> None:
        engine, _ = _make_mock_engine(acquired=True)
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("my-key", ttl=30)

        assert handle is not None
        assert handle.token in lock._held

    async def test_try_acquire_calls_pg_advisory_lock_sql(self) -> None:
        """SQL call contains 'pg_try_advisory_lock'."""
        engine, conn = _make_mock_engine(acquired=True)
        lock = SAAdvisoryLock(engine)

        await lock.try_acquire("my-key", ttl=10)

        sql_call = conn.execute.call_args_list[0]
        sql_text = str(sql_call.args[0])
        assert "pg_try_advisory_lock" in sql_text


# ── release ───────────────────────────────────────────────────────────────────


class TestSAAdvisoryLockRelease:
    async def test_release_removes_token_from_held_dict(self) -> None:
        engine, conn = _make_mock_engine(acquired=True, unlock_result=True)
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("my-key", ttl=30)
        assert handle is not None
        assert handle.token in lock._held

        await lock.release("my-key", handle.token)
        assert handle.token not in lock._held

    async def test_release_closes_connection(self) -> None:
        engine, conn = _make_mock_engine(acquired=True, unlock_result=True)
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("my-key", ttl=30)
        assert handle is not None

        conn.close.assert_not_called()
        await lock.release("my-key", handle.token)
        conn.close.assert_called_once()

    async def test_release_with_unknown_token_is_noop(self) -> None:
        """Releasing with an unknown token → silent no-op, no error."""
        engine = MagicMock()
        lock = SAAdvisoryLock(engine)

        await lock.release("my-key", uuid4())  # must not raise
        assert len(lock._held) == 0

    async def test_release_with_pg_unlock_false_does_not_raise(self) -> None:
        """pg_advisory_unlock returning false → logged warning, no raise."""
        engine, conn = _make_mock_engine(acquired=True, unlock_result=False)
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("my-key", ttl=30)
        assert handle is not None

        await lock.release("my-key", handle.token)  # must not raise

    async def test_double_release_is_noop(self) -> None:
        """Releasing the same token twice → second release is a silent no-op."""
        engine, conn = _make_mock_engine(acquired=True, unlock_result=True)
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("my-key", ttl=30)
        assert handle is not None

        await lock.release("my-key", handle.token)
        await lock.release("my-key", handle.token)  # must not raise


# ── LockHandle context manager ────────────────────────────────────────────────


class TestSAAdvisoryLockHandle:
    async def test_context_manager_releases_on_exit(self) -> None:
        """``async with handle:`` auto-releases on exit."""
        engine, conn = _make_mock_engine(acquired=True, unlock_result=True)
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("my-key", ttl=30)
        assert handle is not None

        async with handle:
            pass  # do some critical work

        assert handle.token not in lock._held
        conn.close.assert_called_once()

    async def test_context_manager_releases_on_exception(self) -> None:
        """``async with handle:`` releases even when the body raises."""
        engine, conn = _make_mock_engine(acquired=True, unlock_result=True)
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("my-key", ttl=30)
        assert handle is not None

        with pytest.raises(RuntimeError):
            async with handle:
                raise RuntimeError("work failed")

        assert handle.token not in lock._held
        conn.close.assert_called_once()
