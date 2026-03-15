"""
Shared fixtures for fastrest_sa tests.

Uses an in-memory SQLite database (aiosqlite) so no external service is
required.  A fresh database and session are created for every test function.

DESIGN: fresh DeclarativeBase per test
    DeclarativeBase.metadata is a class-level object that persists for the
    lifetime of the class.  If the same module-level Base is shared across
    tests, the second test that calls SAModelFactory.build() for an already-
    registered table name will get an SA InvalidRequestError ("Table X is
    already defined for this MetaData instance").

    Creating a new DeclarativeBase subclass inside the fixture gives each test
    an isolated metadata namespace.  Python class creation is cheap (~µs) so
    there is no meaningful overhead.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from fastrest_sa.factory import SAModelFactory


@pytest.fixture
def base() -> type[DeclarativeBase]:
    """
    Return a fresh ``DeclarativeBase`` subclass per test.

    Each call produces a new class with an empty ``MetaData`` — no cross-test
    table-name collisions possible.

    Returns:
        A newly created ``DeclarativeBase`` subclass.
    """

    # Define the class inside the fixture so each call gets a brand-new class
    # object with its own MetaData instance — no shared state between tests.
    class _FreshBase(DeclarativeBase):
        pass

    return _FreshBase


@pytest.fixture
def factory(base: type[DeclarativeBase]) -> SAModelFactory:
    """
    Return an ``SAModelFactory`` bound to the per-test fresh ``Base``.

    Args:
        base: Fresh ``DeclarativeBase`` from the ``base`` fixture.

    Returns:
        A new ``SAModelFactory`` instance with an empty internal cache.
    """
    return SAModelFactory(base)


@pytest_asyncio.fixture
async def session(base: type[DeclarativeBase]) -> AsyncSession:
    """
    Fresh in-memory SQLite session per test.

    Creates all tables declared in ``base.metadata`` before yielding,
    then drops them and disposes the engine on teardown.

    Args:
        base: Fresh ``DeclarativeBase`` from the ``base`` fixture.

    Yields:
        An open ``AsyncSession`` backed by a new in-memory SQLite DB.
    """
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(base.metadata.create_all)
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    async with async_session() as s:
        yield s
    async with engine.begin() as conn:
        await conn.run_sync(base.metadata.drop_all)
    await engine.dispose()
