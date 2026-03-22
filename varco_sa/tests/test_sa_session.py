"""
Unit tests for varco_sa.sqlalchemy_session — SessionContext and with_session.

Tests verify:
- SessionContext sets / resets the current_session ContextVar
- SessionContext commits on clean exit
- SessionContext rolls back on exception
- with_session decorator uses the context session when available
- with_session creates a new session when none is in context
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from varco_sa.sqlalchemy_session import SessionContext, current_session, with_session


# ── SessionContext ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_session_context_sets_current_session():
    """current_session ContextVar must hold the session inside the context."""
    mock_session = AsyncMock(spec=AsyncSession)

    async with SessionContext(mock_session) as sess:
        assert sess is mock_session
        # ContextVar must be set inside the block
        assert current_session.get() is mock_session


@pytest.mark.asyncio
async def test_session_context_resets_after_exit():
    """ContextVar must be reset when the context manager exits."""
    mock_session = AsyncMock(spec=AsyncSession)

    async with SessionContext(mock_session):
        pass

    # After exit, getting the ContextVar should raise LookupError
    with pytest.raises(LookupError):
        current_session.get()


@pytest.mark.asyncio
async def test_session_context_commits_on_clean_exit():
    mock_session = AsyncMock(spec=AsyncSession)

    async with SessionContext(mock_session):
        pass

    mock_session.commit.assert_awaited_once()
    mock_session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_context_rolls_back_on_exception():
    mock_session = AsyncMock(spec=AsyncSession)

    with pytest.raises(ValueError):
        async with SessionContext(mock_session):
            raise ValueError("simulated error")

    mock_session.rollback.assert_awaited_once()
    mock_session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_context_closes_on_clean_exit():
    mock_session = AsyncMock(spec=AsyncSession)

    async with SessionContext(mock_session):
        pass

    mock_session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_session_context_closes_on_exception():
    mock_session = AsyncMock(spec=AsyncSession)

    with pytest.raises(RuntimeError):
        async with SessionContext(mock_session):
            raise RuntimeError("boom")

    mock_session.close.assert_awaited_once()


# ── with_session decorator ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_with_session_uses_explicit_session():
    """When session is passed explicitly, the decorator must use it directly."""
    mock_session = AsyncMock(spec=AsyncSession)
    received = {}

    mock_factory = MagicMock()

    class _Repo:
        @with_session(mock_factory)
        async def do_work(self, session: AsyncSession | None = None):
            received["session"] = session

    repo = _Repo()
    await repo.do_work(session=mock_session)
    assert received["session"] is mock_session


@pytest.mark.asyncio
async def test_with_session_uses_context_session_when_no_explicit():
    """When no session is passed, the decorator should use the ContextVar."""
    mock_session = AsyncMock(spec=AsyncSession)
    received = {}

    mock_factory = MagicMock()

    class _Repo:
        @with_session(mock_factory)
        async def do_work(self, session: AsyncSession | None = None):
            received["session"] = session

    repo = _Repo()
    # Set the session in the ContextVar before calling
    token = current_session.set(mock_session)
    try:
        await repo.do_work()
    finally:
        current_session.reset(token)

    assert received["session"] is mock_session


@pytest.mark.asyncio
async def test_with_session_creates_new_session_when_no_context():
    """When no session is in context, the decorator creates one via the factory."""
    mock_session = AsyncMock(spec=AsyncSession)
    # Make the factory's async context manager yield mock_session
    mock_factory = MagicMock()
    mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)

    received = {}

    class _Repo:
        @with_session(mock_factory)
        async def do_work(self, session: AsyncSession | None = None):
            received["session"] = session

    repo = _Repo()
    # Ensure no session in context
    # (fresh ContextVar has no value — accessing raises LookupError)
    await repo.do_work()

    mock_factory.assert_called_once()
    assert received["session"] is mock_session
