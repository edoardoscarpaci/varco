"""
fastrest_sa.uow
================
SQLAlchemy async Unit of Work.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from fastrest_core.uow import AsyncUnitOfWork


class SQLAlchemyUnitOfWork(AsyncUnitOfWork):
    """
    Unit of Work backed by a SQLAlchemy ``AsyncSession``.

    Repositories are injected via ``repo_factories`` and become attributes
    (``uow.users``, ``uow.posts``, …) inside the ``async with`` block.

    Args:
        session_factory: Callable returning a fresh ``AsyncSession``.
        repo_factories:  ``{attr_name: factory(session) → repo}`` mapping.

    Example::

        async with uow:
            user = await uow.users.save(User(name="Edo", email="..."))

    Thread safety:  ❌ One UoW per request / task.
    Async safety:   ✅ Uses ``AsyncSession`` throughout.
    """

    def __init__(
        self,
        session_factory: Callable[[], AsyncSession],
        repo_factories: dict[str, Callable[[AsyncSession], Any]],
    ) -> None:
        self._session_factory = session_factory
        self._repo_factories = repo_factories
        self._session: AsyncSession | None = None

    async def _begin(self) -> None:
        if self._session is None:
            self._session = self._session_factory()
        for attr, factory in self._repo_factories.items():
            setattr(self, attr, factory(self._session))

    async def commit(self) -> None:
        if self._session:
            await self._session.commit()

    async def rollback(self) -> None:
        if self._session:
            await self._session.rollback()
            await self._session.close()
            self._session = None

    @property
    def session(self) -> AsyncSession:
        """Raw ``AsyncSession`` — escape hatch for SA-specific features."""
        if self._session is None:
            raise RuntimeError("Accessed outside 'async with uow'.")
        return self._session

    def __repr__(self) -> str:
        return f"SQLAlchemyUnitOfWork({'open' if self._session else 'closed'})"
