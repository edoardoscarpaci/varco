"""
orm_abstraction.uow
===================
Async Unit of Work — transactional boundary abstraction.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType


class AsyncUnitOfWork(ABC):
    """
    Abstract async context manager that wraps a single database transaction.

    Usage::

        async with uow:
            user = await uow.users.save(User(name="Edo", email="..."))
            await uow.posts.save(Post(title="...", author_id=user.pk))
            # committed on clean exit, rolled back on exception

    Thread safety:  ❌ One UoW per coroutine.
    Async safety:   ✅ Designed for async/await throughout.

    Edge cases:
        - ``rollback()`` is called automatically on any exception inside the
          ``async with`` block — the exception still propagates.
        - Explicit ``commit()`` before ``__aexit__`` is valid for partial
          commits; the final ``__aexit__`` then commits nothing new.
    """

    async def __aenter__(self) -> AsyncUnitOfWork:
        await self._begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()

    @abstractmethod
    async def _begin(self) -> None:
        """Open the underlying transaction.  Called by ``__aenter__``."""

    @abstractmethod
    async def commit(self) -> None:
        """Flush and commit the current transaction."""

    @abstractmethod
    async def rollback(self) -> None:
        """Discard all pending changes and roll back."""
