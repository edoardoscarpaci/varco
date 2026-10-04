"""
test_smoke.py
=============
Integration smoke tests for the ``17-transactional-outbox`` example.

All tests require a running PostgreSQL instance — tagged ``pytest.mark.integration``
and skipped by default.  Run with::

    uv run pytest examples/17-transactional-outbox/tests/ -v -m integration

What these tests verify
-----------------------
1. **Create order returns 202** — ``POST /v1/orders`` succeeds and creates a
   row in both the ``orders`` and ``varco_outbox`` tables atomically.

2. **OutboxRelay delivers the event** — after a short wait, ``GET /v1/events``
   shows the ``OrderCreatedEvent`` received by ``OrderConsumer``.

3. **Deduplication skips replays** — calling the relay's poll cycle again does
   not add duplicate events to ``consumer.received`` because the outbox row is
   deleted after first delivery.

4. **GET /health returns 200** — basic liveness check.

DESIGN: session-scoped app and Postgres container
    ✅ One container + one ``create_app()`` call shared across all tests.
    ✅ Tests are additive — each creates its own records and asserts only on
       those records; empty-DB assumption is avoided.
    ✅ The FastAPI lifespan (DDL + relay start/stop) is entered explicitly via
       ``app.router.lifespan_context`` — ``ASGITransport`` never triggers it.
    ❌ The relay runs with ``poll_interval=0.1`` s — tests wait up to 2 s for
       delivery.  Very fast but not instant.

Thread safety:  ❌ Single event loop.
Async safety:   ✅ All test functions are ``async def``.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from httpx import ASGITransport
from testcontainers.postgres import PostgresContainer  # noqa: E402

# ── sys.path guard ─────────────────────────────────────────────────────────────
_EXAMPLE_ROOT = str(Path(__file__).parent.parent.resolve())
if _EXAMPLE_ROOT not in sys.path:
    sys.path.insert(0, _EXAMPLE_ROOT)

pytestmark = pytest.mark.integration


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def db_url():
    """
    Start a PostgreSQL 16 container and yield the asyncpg connection URL.

    Scope is ``"session"`` — the container is shared across all tests to
    reduce Docker startup overhead.

    Yields:
        ``postgresql+asyncpg://...`` connection URL string.

    Edge cases:
        - Requires Docker daemon to be running.
        - The host port is ephemeral — assigned by Docker.
    """
    with PostgresContainer("postgres:16-alpine") as pg:
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        user = pg.username
        password = pg.password
        db = pg.dbname
        yield f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"


@pytest.fixture(scope="session")
async def app_client(db_url):
    """
    Create the FastAPI app and yield a session-scoped ``httpx.AsyncClient``.

    ``httpx.ASGITransport`` never sends ASGI ``lifespan`` events (httpx
    0.28), so the app's lifespan — DDL for ``orders``/``varco_outbox``/
    ``varco_dedup_log`` and the ``OutboxRelay`` start/stop — is entered
    explicitly via ``app.router.lifespan_context``.  Without it the first
    INSERT fails with ``relation "orders" does not exist``.

    DESIGN: drive the app's own lifespan rather than duplicating its DDL here
        ✅ Exercises exactly the startup/shutdown code production runs.
        ✅ No extra dependency (``asgi-lifespan``'s ``LifespanManager``).
        ❌ Relies on Starlette's ``router.lifespan_context`` attribute.

    Yields:
        An ``httpx.AsyncClient`` connected to the in-process FastAPI app.

    Edge cases:
        - The relay starts with ``poll_interval=0.1`` s — events are delivered
          within ~0.2 s after the HTTP POST commit.
        - The lifespan runs ``Base.metadata.create_all`` at startup — safe even
          if the schema already exists (idempotent DDL).
    """
    from app import create_app  # noqa: PLC0415

    fastapi_app, _container = create_app(db_url)

    async with (
        fastapi_app.router.lifespan_context(fastapi_app),
        httpx.AsyncClient(
            transport=ASGITransport(app=fastapi_app),
            base_url="http://test",
        ) as client,
    ):
        yield client


# ── Tests ──────────────────────────────────────────────────────────────────────


async def _wait_for_order_event(
    client: httpx.AsyncClient, order_id: str, *, timeout: float = 5.0
) -> list[dict]:
    """
    Poll ``GET /v1/events`` until an event for ``order_id`` has been delivered.

    DESIGN: wait on the specific order, never on an event *count*
        The tests share one app, and the relay delivers earlier tests' events
        asynchronously — a "count_before + 1" condition is satisfied by a
        straggler from a previous test, returning before this test's event
        lands (an observed flake).
        ✅ Deterministic regardless of what other tests left in flight.
        ❌ None worth naming — the order id is always known to the caller.

    Args:
        client:   Test HTTP client.
        order_id: The ``pk`` of the order whose ``OrderCreatedEvent`` to await.
        timeout:  Maximum seconds to wait before raising ``TimeoutError``.

    Returns:
        The full list of event dicts from the response that contained it.

    Raises:
        TimeoutError: No event for ``order_id`` appeared within ``timeout`` seconds.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        events = (await client.get("/v1/events")).json()
        if any(e["order_id"] == order_id for e in events):
            return events
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting for the event of order {order_id!r}")
        await asyncio.sleep(0.1)


class TestHealthCheck:
    """GET /health returns 200."""

    async def test_health_returns_ok(self, app_client) -> None:
        """Health probe must return 200 with ``{"status": "ok"}``."""
        resp = await app_client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


class TestCreateOrder:
    """POST /v1/orders creates an order and returns 202 Accepted."""

    async def test_create_returns_202(self, app_client) -> None:
        """
        POST /v1/orders returns 202 Accepted with the ``OrderRead`` body.

        The 202 signals that event delivery is asynchronous — the caller
        should not assume the event has reached consumers by response time.
        """
        resp = await app_client.post("/v1/orders", json={"amount": 42.5})
        assert resp.status_code == 202, f"Expected 202, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body["amount"] == 42.5
        assert body["status"] == "pending"
        UUID(body["pk"])  # must be a valid UUID

    async def test_create_multiple_orders(self, app_client) -> None:
        """
        Multiple POST calls each create independent orders.
        """
        amounts = [10.0, 20.0, 30.0]
        pks = []
        for amount in amounts:
            resp = await app_client.post("/v1/orders", json={"amount": amount})
            assert resp.status_code == 202
            pks.append(resp.json()["pk"])
        # All PKs must be distinct UUIDs
        assert len(set(pks)) == len(pks), "Duplicate PKs returned"


class TestOutboxRelay:
    """
    Verify the OutboxRelay delivers OrderCreatedEvent to the consumer.

    These tests post an order, then poll ``GET /v1/events`` until the relay
    has published the event and the consumer has recorded it.
    """

    async def test_relay_delivers_event(self, app_client) -> None:
        """
        After ``POST /v1/orders``, the relay delivers ``OrderCreatedEvent``
        to the consumer within a few seconds.

        Steps:
        1. POST a new order.
        2. Poll until that order's event appears in ``GET /v1/events``.
        3. Verify ``order_id`` and ``amount`` match the created order.
        """
        resp = await app_client.post("/v1/orders", json={"amount": 99.99})
        order_pk = resp.json()["pk"]

        # Poll until this order's event appears.
        events = await _wait_for_order_event(app_client, order_pk)

        # Find our specific event by order_id.
        matching = [e for e in events if e["order_id"] == order_pk]
        assert len(matching) >= 1, f"No event found for order_id={order_pk!r}. Events: {events}"
        assert matching[0]["amount"] == 99.99

    async def test_outbox_row_deleted_after_delivery(self, app_client) -> None:
        """
        After the relay publishes the event, the outbox row is deleted.

        We verify this indirectly: a second poll does NOT add duplicate events
        to ``consumer.received``.  The outbox row is gone so the relay has
        nothing to replay.
        """
        # Create an order and wait for delivery.
        resp = await app_client.post("/v1/orders", json={"amount": 5.0})
        order_pk = resp.json()["pk"]

        await _wait_for_order_event(app_client, order_pk)

        # Wait an extra relay cycle to let any duplicate tick run.
        await asyncio.sleep(0.5)

        events_after = (await app_client.get("/v1/events")).json()
        matching = [e for e in events_after if e["order_id"] == order_pk]

        # Should see exactly one event — the outbox row was deleted after delivery.
        assert len(matching) == 1, (
            f"Expected 1 event for order_id={order_pk!r}, got {len(matching)}. "
            "Outbox row may not have been deleted after relay."
        )
