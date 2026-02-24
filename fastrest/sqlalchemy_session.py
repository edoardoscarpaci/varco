from sqlalchemy.ext.asyncio import async_sessionmaker,AsyncSession
from functools import wraps
from contextvars import ContextVar,Token
from contextlib import AbstractAsyncContextManager
from typing import Optional

# Global session context
current_session: ContextVar[AsyncSession] = ContextVar("current_session")

class SessionContext(AbstractAsyncContextManager):
    """Context manager for multi-operation transactions."""
    def __init__(self, session: AsyncSession):
        self.session = session
        self.token : Optional[Token] = None

    async def __aenter__(self):
        self.token = current_session.set(self.session)
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        current_session.reset(self.token) # type: ignore
        if exc_type:
            await self.session.rollback()
        else:
            await self.session.commit()
        await self.session.close()

def with_session(session_factory: async_sessionmaker):
    """
    Decorator to automatically provide a session to repository methods.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, session: AsyncSession | None = None, **kwargs):
            if session is None:
                try:
                    # Try to get session from contextvar
                    session = current_session.get()
                except LookupError:
                    # Create a new session if not in context
                    async with session_factory() as s:
                        return await func(self, *args, session=s, **kwargs)
            return await func(self, *args, session=session, **kwargs)
        return wrapper
    return decorator

