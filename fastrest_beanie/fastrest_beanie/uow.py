"""
fastrest_beanie.uow
====================
Beanie / Motor Unit of Work.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession

from fastrest_core.uow import AsyncUnitOfWork


class BeanieUnitOfWork(AsyncUnitOfWork):
    """
    Unit of Work backed by a Motor ``AsyncIOMotorClientSession``.

    Set ``transactional=True`` only when connected to a MongoDB replica set
    or sharded cluster — standalone instances do not support multi-document
    transactions.

    Args:
        motor_client:    Connected Motor client.
        repo_factories:  ``{attr_name: factory(session | None) → repo}``.
        transactional:   Wrap operations in a MongoDB transaction.

    Thread safety:  ❌ One UoW per request / task.
    Async safety:   ✅ Uses Motor async session throughout.
    """

    def __init__(
        self,
        motor_client: AsyncIOMotorClient,
        repo_factories: dict[str, Callable[[AsyncIOMotorClientSession | None], Any]],
        *,
        transactional: bool = False,
    ) -> None:
        self._client = motor_client
        self._repo_factories = repo_factories
        self._transactional = transactional
        self._session: AsyncIOMotorClientSession | None = None

    async def _begin(self) -> None:
        if self._session is None:
            self._session = await self._client.start_session()
        if self._transactional:
            self._session.start_transaction()
        for attr, factory in self._repo_factories.items():
            setattr(self, attr, factory(self._session))

    async def commit(self) -> None:
        if self._session:
            if self._transactional:
                await self._session.commit_transaction()
            await self._session.end_session()
            self._session = None

    async def rollback(self) -> None:
        if self._session:
            if self._transactional:
                await self._session.abort_transaction()
            await self._session.end_session()
            self._session = None

    def __repr__(self) -> str:
        tx = "transactional" if self._transactional else "no-tx"
        return f"BeanieUnitOfWork({'open' if self._session else 'closed'}, {tx})"
