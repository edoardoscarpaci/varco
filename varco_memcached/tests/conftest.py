"""
Shared fixtures for varco_memcached integration tests (Plan 012 / RT1, Steps 6-8).

``memcached_host_port`` starts a single Memcached container **once per test
session**, replacing the per-file ``scope="module"`` ``MemcachedContainer``
fixture previously declared in ``test_integration.py``/``test_health.py``.

Per-test namespacing rule: because the server is shared for the whole
session, every test MUST use a unique key prefix (e.g.
``f"test:{uuid4().hex[:8]}:"``) — Memcached has no keyspaces/databases to
isolate tests the way Redis DB indices or Mongo database names do. A test
that needs a pristine server (``flush_all``) must declare its own
function-scoped ``memcached_container_fresh`` fixture instead.

``VARCO_TEST_MEMCACHED_URL`` overrides the container entirely (Open Question
1) — expected shape is ``host:port``; when set, no container is started and
the value is reported via ``request.config.stash``.
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def memcached_host_port(request: pytest.FixtureRequest) -> tuple[str, int]:
    """
    Session-scoped ``(host, port)`` tuple for the shared Memcached server.

    See the module docstring for the per-test namespacing rule and the
    ``VARCO_TEST_MEMCACHED_URL`` override contract.

    Yields:
        A ``(host, port)`` tuple.
    """
    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip(
            "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration"
        )

    override = os.environ.get("VARCO_TEST_MEMCACHED_URL")
    if override:
        request.config.stash.setdefault("varco_test_overrides", []).append(("memcached", override))
        host, _, port = override.partition(":")
        yield host, int(port)
        return

    from testcontainers.memcached import MemcachedContainer  # noqa: PLC0415

    with MemcachedContainer() as container:
        yield (
            container.get_container_host_ip(),
            int(container.get_exposed_port(11211)),
        )


@pytest.fixture(autouse=True)
def _select_memcached_backends(monkeypatch: pytest.MonkeyPatch) -> None:
    """Plan 047 §D-tests — see varco_kafka's twin fixture docstring."""
    monkeypatch.setenv("VARCO_CACHE_BACKEND", "memcached")
