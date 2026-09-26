"""
Shared fixtures for varco_redis integration tests (Plan 012 / RT1, Steps 6-8).

``redis_url`` starts a single Redis container **once per test session** and
is shared by every integration test in this package, replacing the ~9
per-file ``scope="module"`` container fixtures that previously each started
their own broker.

Per-test namespacing rule: because the container is shared across the whole
session, every test MUST confine itself to a key/stream/channel prefix it
owns exclusively (e.g. ``f"test:{uuid4().hex[:8]}:..."``). A test that needs
a genuinely pristine server (e.g. asserting on ``DBSIZE`` or issuing
``FLUSHALL``) must declare its own function-scoped ``redis_container_fresh``
fixture instead of relying on this shared one.

``VARCO_TEST_REDIS_URL`` overrides the container entirely (Open Question 1
in plans/012-r3-reliability-and-regression-proofing.md) — when set, no
container is started; the value is used as-is and reported via
``request.config.stash`` rather than silently falling back to a container
on a dead endpoint.
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def redis_url(request: pytest.FixtureRequest) -> str:
    """
    Session-scoped Redis connection URL — real container or override.

    See the module docstring for the per-test namespacing rule and the
    ``VARCO_TEST_REDIS_URL`` override contract.

    Yields:
        A ``redis://`` URL for the shared broker.
    """
    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip(
            "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration"
        )

    override = os.environ.get("VARCO_TEST_REDIS_URL")
    if override:
        request.config.stash.setdefault("varco_test_overrides", []).append(("redis", override))
        yield override
        return

    from testcontainers.redis import RedisContainer  # noqa: PLC0415

    with RedisContainer() as container:
        yield f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0"


@pytest.fixture
def redis_container_fresh():
    """
    Function-scoped, pristine Redis container.

    Use only when a test genuinely needs a clean server (``FLUSHALL``,
    ``DBSIZE`` assertions) — pays a full container-boot cost per test, which
    is exactly the friction the session-scoped ``redis_url`` fixture exists
    to avoid, so reach for this deliberately and rarely.

    Yields:
        A started ``testcontainers.redis.RedisContainer``.
    """
    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip(
            "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration"
        )

    from testcontainers.redis import RedisContainer  # noqa: PLC0415

    with RedisContainer() as container:
        yield container


@pytest.fixture(autouse=True)
def _select_redis_backends(monkeypatch: pytest.MonkeyPatch) -> None:
    """Plan 047 §D-tests — see varco_kafka's twin fixture docstring."""
    monkeypatch.setenv("VARCO_EVENT_BUS_BACKEND", "redis")
    monkeypatch.setenv("VARCO_DLQ_BACKEND", "redis")
    monkeypatch.setenv("VARCO_CACHE_BACKEND", "redis")
    monkeypatch.setenv("VARCO_IDEMPOTENCY_BACKEND", "redis")
    monkeypatch.setenv("VARCO_JOB_STORE_BACKEND", "redis")
    monkeypatch.setenv("VARCO_REVOCATION_BACKEND", "redis")
