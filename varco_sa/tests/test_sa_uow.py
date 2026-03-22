"""
Tests for SQLAlchemyUnitOfWork.

Uses AsyncMock for the AsyncSession so no real database connection is needed.

Coverage:
- _begin:    creates session via factory, wires repo attributes
- commit:    calls session.commit()
- rollback:  calls session.rollback() + close(), clears session reference
- session:   property returns session when open, raises RuntimeError otherwise
- __repr__:  shows open/closed
- context manager: clean exit → commit, exception → rollback + propagation

Thread safety:  N/A (unit tests)
Async safety:   ✅ Uses AsyncMock throughout
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from varco_sa.uow import SQLAlchemyUnitOfWork


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_mock_session() -> AsyncMock:
    """
    Build a mock AsyncSession with async commit / rollback / close.

    Returns:
        An AsyncMock that quacks like AsyncSession for UoW testing.
    """
    session = AsyncMock(spec=AsyncSession)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    return session


def _make_uow(
    *,
    repo_factories: dict | None = None,
    session: AsyncMock | None = None,
) -> tuple[SQLAlchemyUnitOfWork, AsyncMock]:
    """
    Build a SQLAlchemyUnitOfWork with a mock session factory.

    Args:
        repo_factories: Dict of {attr_name: factory(session) → repo}.
        session:        The session object the factory will return.

    Returns:
        (uow, mock_session) — the session returned by the factory.
    """
    mock_session = session or _make_mock_session()
    session_factory = MagicMock(return_value=mock_session)

    uow = SQLAlchemyUnitOfWork(
        session_factory=session_factory,
        repo_factories=repo_factories or {},
    )
    return uow, mock_session


# ── _begin ────────────────────────────────────────────────────────────────────


async def test_begin_calls_session_factory() -> None:
    """_begin() calls session_factory() to create a new session."""
    mock_session = _make_mock_session()
    factory = MagicMock(return_value=mock_session)
    uow = SQLAlchemyUnitOfWork(session_factory=factory, repo_factories={})

    await uow._begin()

    factory.assert_called_once()


async def test_begin_stores_session() -> None:
    """After _begin(), the session is available as uow._session."""
    uow, mock_session = _make_uow()
    await uow._begin()

    assert uow._session is mock_session


async def test_begin_wires_repo_factories_as_attributes() -> None:
    """_begin() calls each repo factory with the session and sets the attr."""
    mock_repo = MagicMock()
    factories = {"users": MagicMock(return_value=mock_repo)}

    uow, mock_session = _make_uow(repo_factories=factories)
    await uow._begin()

    factories["users"].assert_called_once_with(mock_session)
    assert uow.users is mock_repo


async def test_begin_does_not_create_second_session_if_already_open() -> None:
    """
    Calling _begin() twice reuses the existing session.

    Edge case: the condition `if self._session is None` prevents double-open.
    """
    factory = MagicMock(return_value=_make_mock_session())
    uow = SQLAlchemyUnitOfWork(session_factory=factory, repo_factories={})

    await uow._begin()
    await uow._begin()

    assert factory.call_count == 1


# ── commit ────────────────────────────────────────────────────────────────────


async def test_commit_calls_session_commit() -> None:
    """commit() delegates to session.commit()."""
    uow, mock_session = _make_uow()
    await uow._begin()
    await uow.commit()

    mock_session.commit.assert_called_once()


async def test_commit_no_op_when_session_is_none() -> None:
    """
    commit() without a prior _begin() is a silent no-op.

    Edge case: guards in test teardown may call commit() even if _begin()
    was never called — should not raise AttributeError.
    """
    uow, mock_session = _make_uow()
    await uow.commit()  # no session open

    mock_session.commit.assert_not_called()


async def test_commit_does_not_close_session() -> None:
    """
    commit() leaves the session open (only rollback closes + clears it).

    DESIGN: SA commit keeps the session alive so the caller can continue
    using it after a partial commit.  rollback() clears the session.
    """
    uow, mock_session = _make_uow()
    await uow._begin()
    await uow.commit()

    mock_session.close.assert_not_called()
    assert uow._session is not None


# ── rollback ──────────────────────────────────────────────────────────────────


async def test_rollback_calls_session_rollback_and_close() -> None:
    """rollback() calls session.rollback() then session.close()."""
    uow, mock_session = _make_uow()
    await uow._begin()
    await uow.rollback()

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


async def test_rollback_clears_session_reference() -> None:
    """After rollback(), uow._session is None."""
    uow, _ = _make_uow()
    await uow._begin()
    await uow.rollback()

    assert uow._session is None


async def test_rollback_no_op_when_session_is_none() -> None:
    """rollback() without a prior _begin() is a silent no-op."""
    uow, mock_session = _make_uow()
    await uow.rollback()

    mock_session.rollback.assert_not_called()
    mock_session.close.assert_not_called()


async def test_rollback_calls_rollback_before_close() -> None:
    """
    rollback() must call rollback() before close() — order matters.

    Calling close() before rollback() can leave the transaction dangling
    in some SA backends.
    """
    uow, mock_session = _make_uow()
    await uow._begin()

    call_order: list[str] = []
    mock_session.rollback = AsyncMock(side_effect=lambda: call_order.append("rollback"))
    mock_session.close = AsyncMock(side_effect=lambda: call_order.append("close"))

    await uow.rollback()

    assert call_order == ["rollback", "close"]


# ── session property ──────────────────────────────────────────────────────────


async def test_session_property_returns_session_when_open() -> None:
    """session property returns the AsyncSession inside an open UoW."""
    uow, mock_session = _make_uow()
    await uow._begin()

    assert uow.session is mock_session


def test_session_property_raises_runtime_error_when_closed() -> None:
    """
    Accessing session before _begin() raises RuntimeError.

    This prevents silent None bugs when callers access the escape hatch
    outside the async with block.
    """
    uow, _ = _make_uow()

    with pytest.raises(RuntimeError, match="async with uow"):
        _ = uow.session


# ── __repr__ ──────────────────────────────────────────────────────────────────


def test_repr_shows_closed_before_begin() -> None:
    """__repr__ shows 'closed' when no session is open."""
    uow, _ = _make_uow()
    assert "closed" in repr(uow)


async def test_repr_shows_open_after_begin() -> None:
    """__repr__ shows 'open' when a session is active."""
    uow, _ = _make_uow()
    await uow._begin()
    assert "open" in repr(uow)


# ── Context manager ───────────────────────────────────────────────────────────


async def test_context_manager_enters_via_begin() -> None:
    """__aenter__ calls _begin() and returns the UoW instance."""
    uow, mock_session = _make_uow()

    async with uow as ctx:
        assert ctx is uow
        assert uow._session is mock_session


async def test_context_manager_commits_on_clean_exit() -> None:
    """A clean async with block triggers commit() on exit."""
    uow, mock_session = _make_uow()

    async with uow:
        pass

    mock_session.commit.assert_called_once()


async def test_context_manager_rollback_on_exception() -> None:
    """An exception inside the block triggers rollback()."""
    uow, mock_session = _make_uow()

    with pytest.raises(ValueError):
        async with uow:
            raise ValueError("test")

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


async def test_context_manager_exception_propagates() -> None:
    """The original exception is not swallowed — it propagates to the caller."""
    uow, _ = _make_uow()

    with pytest.raises(RuntimeError, match="propagate me"):
        async with uow:
            raise RuntimeError("propagate me")


async def test_context_manager_commit_not_called_on_exception() -> None:
    """commit() is NOT called when an exception triggers rollback."""
    uow, mock_session = _make_uow()

    with pytest.raises(Exception):
        async with uow:
            raise Exception("boom")

    mock_session.commit.assert_not_called()
