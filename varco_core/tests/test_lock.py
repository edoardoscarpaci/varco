"""
tests.test_lock
===============
Unit tests for varco_core.lock — distributed locking abstractions.

Covers:
    LockHandle          — context manager, idempotent release
    InMemoryLock        — acquire, release, token-guarded release,
                          contention (try_acquire returns None),
                          blocking acquire (acquire raises LockNotAcquiredError)
    AbstractDistributedLock.acquire  — poll-based blocking acquire

All tests use InMemoryLock — no external broker required.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from varco_core.lock import (
    InMemoryLock,
    LockHandle,
    LockNotAcquiredError,
)


# ── LockHandle tests ───────────────────────────────────────────────────────────


async def test_lock_handle_release_via_context_manager() -> None:
    """
    A LockHandle used as an async context manager must release the lock
    automatically on exit — even if no exception occurs.
    """
    lock = InMemoryLock()
    handle = await lock.acquire("test:cm", ttl=10)

    async with handle:
        # Lock is held inside the block
        assert await lock.try_acquire("test:cm", ttl=10) is None

    # Lock is released after exiting the block
    second = await lock.try_acquire("test:cm", ttl=10)
    assert second is not None
    await second.release()


async def test_lock_handle_release_idempotent() -> None:
    """
    Calling LockHandle.release() more than once must be safe — the second call
    is a silent no-op.
    """
    lock = InMemoryLock()
    handle = await lock.acquire("test:idempotent", ttl=10)

    await handle.release()
    # Second release must not raise
    await handle.release()


async def test_lock_handle_repr() -> None:
    """LockHandle.__repr__ must return a non-empty string for debugging."""
    lock = InMemoryLock()
    handle = await lock.acquire("test:repr", ttl=10)
    assert "test:repr" in repr(handle)
    await handle.release()


# ── InMemoryLock.try_acquire ───────────────────────────────────────────────────


async def test_try_acquire_returns_handle_when_free() -> None:
    """
    try_acquire must return a LockHandle when no other holder has the key.
    """
    lock = InMemoryLock()
    handle = await lock.try_acquire("key:free", ttl=10)
    assert isinstance(handle, LockHandle)
    await handle.release()


async def test_try_acquire_returns_none_when_held() -> None:
    """
    try_acquire must return None when the same key is already held.
    """
    lock = InMemoryLock()
    handle = await lock.acquire("key:held", ttl=10)
    try:
        second = await lock.try_acquire("key:held", ttl=10)
        assert second is None
    finally:
        await handle.release()


async def test_try_acquire_different_keys_independent() -> None:
    """
    Locks on different keys must be independent — holding key A must not
    prevent acquiring key B.
    """
    lock = InMemoryLock()
    handle_a = await lock.acquire("key:A", ttl=10)
    handle_b = await lock.try_acquire("key:B", ttl=10)

    assert handle_b is not None, "key:B should be free while key:A is held"

    await handle_a.release()
    await handle_b.release()


async def test_try_acquire_after_release_succeeds() -> None:
    """
    After releasing a lock, try_acquire on the same key must succeed.
    """
    lock = InMemoryLock()
    handle = await lock.acquire("key:cycle", ttl=10)
    await handle.release()

    handle2 = await lock.try_acquire("key:cycle", ttl=10)
    assert handle2 is not None
    await handle2.release()


async def test_try_acquire_raises_on_empty_key() -> None:
    """
    try_acquire must raise ValueError if the key is an empty string.
    """
    lock = InMemoryLock()
    with pytest.raises(ValueError, match="non-empty"):
        await lock.try_acquire("", ttl=10)


async def test_try_acquire_raises_on_non_positive_ttl() -> None:
    """
    try_acquire must raise ValueError if ttl is zero or negative.
    """
    lock = InMemoryLock()
    with pytest.raises(ValueError, match="positive"):
        await lock.try_acquire("key:badttl", ttl=0)


# ── InMemoryLock.release token guard ──────────────────────────────────────────


async def test_release_wrong_token_is_noop(caplog: pytest.LogCaptureFixture) -> None:
    """
    release() with a mismatched token must be a silent no-op — it must NOT
    release the lock.

    This guards against the phantom-release bug: a holder whose TTL expired
    (in InMemoryLock TTLs are not enforced, so we simulate with a fake token)
    must not accidentally release a new holder's lock.
    """
    import logging

    lock = InMemoryLock()
    real_handle = await lock.acquire("key:token_guard", ttl=10)

    # Simulate a stale holder trying to release with an old token.
    fake_token = uuid4()
    with caplog.at_level(logging.WARNING):
        await lock.release("key:token_guard", fake_token)

    # The real lock must still be held — a second try_acquire must fail.
    contender = await lock.try_acquire("key:token_guard", ttl=10)
    assert contender is None, "Lock should still be held after wrong-token release"

    await real_handle.release()


async def test_release_unknown_key_is_noop() -> None:
    """
    release() for a key that was never acquired must be a silent no-op.
    """
    lock = InMemoryLock()
    # Should not raise
    await lock.release("key:unknown", uuid4())


# ── AbstractDistributedLock.acquire (blocking) ────────────────────────────────


async def test_acquire_returns_handle_when_free() -> None:
    """
    acquire() must return a LockHandle when the lock is immediately available.
    """
    lock = InMemoryLock()
    handle = await lock.acquire("key:blocking_free", ttl=10, timeout=1.0)
    assert isinstance(handle, LockHandle)
    await handle.release()


async def test_acquire_raises_when_timeout_exceeded() -> None:
    """
    acquire() must raise LockNotAcquiredError when the lock is held and the
    timeout is exceeded.
    """
    lock = InMemoryLock()
    held = await lock.acquire("key:blocking_timeout", ttl=30)
    try:
        with pytest.raises(LockNotAcquiredError) as exc_info:
            # timeout=0.1 — should exhaust quickly while key:blocking_timeout is held
            await lock.acquire("key:blocking_timeout", ttl=10, timeout=0.1)
        assert exc_info.value.key == "key:blocking_timeout"
        assert exc_info.value.timeout == pytest.approx(0.1)
    finally:
        await held.release()


async def test_acquire_succeeds_after_holder_releases() -> None:
    """
    acquire() must succeed once the current holder releases — even if there was
    contention during the polling window.
    """
    lock = InMemoryLock()
    held = await lock.acquire("key:blocking_release", ttl=30)

    async def _release_after(delay: float) -> None:
        await asyncio.sleep(delay)
        await held.release()

    # Release after a short delay — acquire polls until it sees the key free.
    asyncio.create_task(_release_after(0.05))

    handle = await lock.acquire(
        "key:blocking_release",
        ttl=10,
        timeout=2.0,
        retry_interval=0.02,
    )
    assert isinstance(handle, LockHandle)
    await handle.release()


async def test_acquire_raises_on_empty_key() -> None:
    """acquire() must propagate the ValueError from try_acquire for empty keys."""
    lock = InMemoryLock()
    with pytest.raises(ValueError, match="non-empty"):
        await lock.acquire("", ttl=10, timeout=1.0)


async def test_acquire_raises_on_non_positive_ttl() -> None:
    """acquire() must raise ValueError when ttl is not positive."""
    lock = InMemoryLock()
    with pytest.raises(ValueError, match="positive"):
        await lock.acquire("key:bad", ttl=0)


# ── InMemoryLock repr ─────────────────────────────────────────────────────────


async def test_in_memory_lock_repr() -> None:
    """InMemoryLock.__repr__ must include key/held counts for debuggability."""
    lock = InMemoryLock()
    handle = await lock.acquire("key:repr", ttl=10)
    r = repr(lock)
    assert "InMemoryLock" in r
    assert "held=1" in r
    await handle.release()


# ── LockNotAcquiredError ──────────────────────────────────────────────────────


def test_lock_not_acquired_error_attributes() -> None:
    """
    LockNotAcquiredError must expose key and timeout as attributes so callers
    can programmatically inspect the failure reason.
    """
    err = LockNotAcquiredError("order:42", 5.0)
    assert err.key == "order:42"
    assert err.timeout == 5.0
    assert "order:42" in str(err)
    assert "5.0" in str(err)
