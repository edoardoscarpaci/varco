"""
conftest.py — pytest-asyncio configuration for the example integration tests.

Declares a session-scoped asyncio event loop so that session-scoped async
fixtures (e.g. ``running_server`` that starts uvicorn) and function-scoped
test coroutines share the same loop.

Without this, pytest-asyncio creates separate loops for fixture scope vs test
scope — uvicorn's background asyncio.Task runs in the fixture's loop while
httpx.AsyncClient runs in the test's loop, causing ReadTimeout on every
request.

DESIGN: conftest.py over pyproject.toml asyncio_default_test_loop_scope
    The ini option only takes effect when pytest rootdir resolves to
    example/pyproject.toml.  When tests are run from the workspace root
    (``uv run pytest example/example/tests/``), rootdir may be the workspace
    root whose pyproject.toml has no pytest section.  A conftest.py is always
    discovered regardless of rootdir.
"""

from __future__ import annotations

import asyncio

import pytest


@pytest.fixture(scope="session")
def event_loop():
    """
    Provide a single asyncio event loop for the entire test session.

    Both session-scoped fixtures and function-scoped tests run in this loop,
    ensuring uvicorn's background task and httpx clients share the same
    event loop.

    Yields:
        A running asyncio event loop that lives for the whole test session.

    Edge cases:
        - pytest-asyncio 0.21+ deprecates the custom ``event_loop`` fixture
          in favour of ``asyncio_default_fixture_loop_scope`` / ``asyncio_default_test_loop_scope``
          ini options.  The deprecation warning can be silenced by adding
          ``filterwarnings = ["ignore::pytest_asyncio.plugin.PytestUnraisableExceptionWarning"]``
          to pytest ini options.  The fixture remains the most portable
          approach across pytest-asyncio versions.
    """
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
