"""
Tests for BeanieUnitOfWork.

Motor client and session are mocked — no MongoDB required.

Coverage:
- _begin:  creates session, wires repo attributes, starts transaction (opt-in)
- commit:  ends session (with and without transaction)
- rollback: aborts transaction (if active), ends session
- __repr__: shows open/closed and tx mode
- context manager: clean exit → commit, exception → rollback

Thread safety:  N/A (unit tests)
Async safety:   ✅ Uses AsyncMock throughout
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from varco_beanie.uow import BeanieUnitOfWork


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_motor_client(session: MagicMock | None = None) -> MagicMock:
    """
    Build a mock AsyncIOMotorClient.

    start_session() is an AsyncMock returning the given (or a fresh) session.

    Args:
        session: The session object to return from start_session().

    Returns:
        A mock Motor client ready for injection into BeanieUnitOfWork.
    """
    client = MagicMock()
    mock_session = session or MagicMock()
    client.start_session = AsyncMock(return_value=mock_session)
    return client


def _make_uow(
    *,
    repo_factories: dict | None = None,
    transactional: bool = False,
    session: MagicMock | None = None,
) -> tuple[BeanieUnitOfWork, MagicMock]:
    """
    Build a BeanieUnitOfWork with mocked Motor client.

    Returns:
        (uow, mock_session) — the session is the one returned by start_session().
    """
    mock_session = session or MagicMock()
    mock_session.commit_transaction = AsyncMock()
    mock_session.abort_transaction = AsyncMock()
    mock_session.end_session = AsyncMock()

    client = _make_motor_client(session=mock_session)
    uow = BeanieUnitOfWork(
        motor_client=client,
        repo_factories=repo_factories or {},
        transactional=transactional,
    )
    return uow, mock_session


# ── _begin ────────────────────────────────────────────────────────────────────


async def test_begin_creates_motor_session() -> None:
    """_begin() calls motor_client.start_session() to open a new session."""
    client = _make_motor_client()
    uow = BeanieUnitOfWork(motor_client=client, repo_factories={})
    await uow._begin()

    client.start_session.assert_called_once()


async def test_begin_wires_repo_factories_as_attributes() -> None:
    """_begin() calls each repo factory with the session and sets it as an attribute."""
    mock_repo = MagicMock()
    # factory receives the session and returns the repo
    factories = {"users": MagicMock(return_value=mock_repo)}

    uow, mock_session = _make_uow(repo_factories=factories)
    await uow._begin()

    factories["users"].assert_called_once_with(mock_session)
    assert uow.users is mock_repo


async def test_begin_starts_transaction_when_transactional() -> None:
    """
    _begin() calls session.start_transaction() when transactional=True.

    Note: start_transaction() is a sync call on Motor's session, not awaited.
    """
    uow, mock_session = _make_uow(transactional=True)
    await uow._begin()

    mock_session.start_transaction.assert_called_once()


async def test_begin_skips_transaction_when_not_transactional() -> None:
    """_begin() does NOT call start_transaction() when transactional=False."""
    uow, mock_session = _make_uow(transactional=False)
    await uow._begin()

    mock_session.start_transaction.assert_not_called()


async def test_begin_idempotent_if_session_already_open() -> None:
    """
    A second call to _begin() reuses the existing session.

    Edge case: calling _begin() twice should not open a second session.
    The condition `if self._session is None` prevents double-open.
    """
    client = _make_motor_client()
    uow = BeanieUnitOfWork(motor_client=client, repo_factories={})
    await uow._begin()
    await uow._begin()

    # start_session called only once — second _begin reuses the open session
    assert client.start_session.call_count == 1


# ── commit ────────────────────────────────────────────────────────────────────


async def test_commit_ends_session_without_transaction() -> None:
    """commit() ends the session when transactional=False."""
    uow, mock_session = _make_uow(transactional=False)
    await uow._begin()
    await uow.commit()

    mock_session.end_session.assert_called_once()


async def test_commit_commits_transaction_and_ends_session() -> None:
    """commit() commits the transaction then ends the session when transactional=True."""
    uow, mock_session = _make_uow(transactional=True)
    await uow._begin()
    await uow.commit()

    mock_session.commit_transaction.assert_called_once()
    mock_session.end_session.assert_called_once()


async def test_commit_clears_session_reference() -> None:
    """
    After commit(), the internal session reference is set to None.

    Important for is-closed state detection and idempotency.
    """
    uow, _ = _make_uow()
    await uow._begin()
    assert uow._session is not None
    await uow.commit()
    assert uow._session is None


async def test_commit_no_op_when_no_session() -> None:
    """
    commit() without a prior _begin() is a silent no-op.

    Edge case: caller might commit inside a guard that runs before
    _begin() in error recovery scenarios — should not raise.
    """
    uow, mock_session = _make_uow()
    await uow.commit()  # no session open yet
    mock_session.end_session.assert_not_called()


# ── rollback ──────────────────────────────────────────────────────────────────


async def test_rollback_ends_session_without_transaction() -> None:
    """rollback() ends the session when transactional=False."""
    uow, mock_session = _make_uow(transactional=False)
    await uow._begin()
    await uow.rollback()

    mock_session.end_session.assert_called_once()


async def test_rollback_aborts_transaction_and_ends_session() -> None:
    """rollback() aborts the transaction then ends the session when transactional=True."""
    uow, mock_session = _make_uow(transactional=True)
    await uow._begin()
    await uow.rollback()

    mock_session.abort_transaction.assert_called_once()
    mock_session.end_session.assert_called_once()


async def test_rollback_clears_session_reference() -> None:
    """After rollback(), the session reference is set to None."""
    uow, _ = _make_uow()
    await uow._begin()
    await uow.rollback()
    assert uow._session is None


async def test_rollback_no_op_when_no_session() -> None:
    """rollback() without a prior _begin() is a silent no-op."""
    uow, mock_session = _make_uow()
    await uow.rollback()
    mock_session.end_session.assert_not_called()


# ── __repr__ ──────────────────────────────────────────────────────────────────


async def test_repr_shows_closed_before_begin() -> None:
    """__repr__ shows 'closed' before _begin() is called."""
    uow, _ = _make_uow()
    assert "closed" in repr(uow)


async def test_repr_shows_open_after_begin() -> None:
    """__repr__ shows 'open' after _begin() is called."""
    uow, _ = _make_uow()
    await uow._begin()
    assert "open" in repr(uow)


def test_repr_shows_no_tx_for_non_transactional() -> None:
    """__repr__ shows 'no-tx' when transactional=False."""
    uow, _ = _make_uow(transactional=False)
    assert "no-tx" in repr(uow)


def test_repr_shows_transactional_flag() -> None:
    """__repr__ shows 'transactional' when transactional=True."""
    uow, _ = _make_uow(transactional=True)
    assert "transactional" in repr(uow)


# ── Context manager ───────────────────────────────────────────────────────────


async def test_context_manager_commits_on_clean_exit() -> None:
    """async with uow: calls _begin() on enter, commit() on clean exit."""
    uow, mock_session = _make_uow()

    async with uow:
        pass  # no exception

    # commit path: end_session called, no rollback
    mock_session.end_session.assert_called_once()
    mock_session.abort_transaction.assert_not_called()


async def test_context_manager_rollback_on_exception() -> None:
    """async with uow: calls rollback() when an exception escapes the block."""
    uow, mock_session = _make_uow()

    with pytest.raises(RuntimeError):
        async with uow:
            raise RuntimeError("boom")

    # rollback path: end_session called
    mock_session.end_session.assert_called_once()


async def test_context_manager_exception_propagates() -> None:
    """The original exception propagates out of the async with block."""
    uow, _ = _make_uow()

    with pytest.raises(ValueError, match="test error"):
        async with uow:
            raise ValueError("test error")


async def test_context_manager_wires_repos_inside_block() -> None:
    """Repo factories are resolved inside the async with block."""
    mock_repo = MagicMock()
    factories = {"items": MagicMock(return_value=mock_repo)}
    uow, _ = _make_uow(repo_factories=factories)

    async with uow:
        assert uow.items is mock_repo
