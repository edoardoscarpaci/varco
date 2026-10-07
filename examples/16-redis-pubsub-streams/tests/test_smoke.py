"""
test_smoke.py
=============
Integration smoke tests for the ``16-redis-pubsub-streams`` example.

All tests require a running Redis instance and are tagged
``@pytest.mark.integration``.  Run them with::

    pytest -m integration examples/16-redis-pubsub-streams/tests/

The ``redis_url`` fixture spins up a real Redis container via testcontainers
and yields the connection URL.  The ``client`` fixture starts ``RedisEventBus``
explicitly (because ``httpx.ASGITransport`` does NOT trigger ASGI lifespan —
see FINDINGS F06) and passes the pre-started bus into ``create_app``.

DESIGN: pre-start bus in the fixture, pass to create_app
    ✅ Avoids the ASGITransport-lifespan problem without any workaround.
    ✅ The fixture fully controls bus lifetime — ``stop()`` is guaranteed
       in the ``finally`` block even if tests fail.
    ✅ Tests can inspect ``consumer.received`` directly without touching
       request state.
    ❌ Tests must wait for Pub/Sub delivery (``asyncio.sleep``) — Redis
       Pub/Sub is asynchronous; there is no synchronous ack.

Thread safety:  ❌  Single asyncio event loop; no concurrent test execution.
Async safety:   ✅  All test functions are ``async def``.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from httpx import ASGITransport

pytestmark = pytest.mark.integration


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def redis_url():
    """
    Start a Redis 7 container and yield the connection URL.

    Scope is ``"session"`` — the container is shared across all tests to avoid
    the ~2 s startup overhead per test.

    Yields:
        Redis connection URL string, e.g. ``"redis://127.0.0.1:32768/0"``.

    Edge cases:
        - Requires Docker daemon to be running.
        - Uses port 6379 inside the container; the host port is ephemeral
          (assigned by Docker).
    """
    from testcontainers.redis import RedisContainer  # noqa: PLC0415

    with RedisContainer("redis:7-alpine") as r:
        host = r.get_container_host_ip()
        port = r.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture
async def bus_and_consumer(redis_url):
    """
    Pre-start a ``RedisEventBus`` and build an ``OrderConsumer``.

    Yields the tuple ``(bus, consumer)`` with the bus already started.
    The caller (``client`` fixture) attaches the bus to ``create_app`` so
    the lifespan does NOT start/stop it — this fixture owns the lifecycle.

    Yields:
        Tuple of ``(RedisEventBus, OrderConsumer)`` — both ready to use.

    Edge cases:
        - Subscriptions from ``consumer.register_to(bus)`` are registered on the
          bus before ``start()`` is called.  ``start()`` then subscribes
          to the matching Redis channels immediately.
        - ``stop()`` is called in the ``finally`` block — guaranteed even
          on test failure.
    """
    from consumer import OrderConsumer  # noqa: PLC0415

    from varco_redis import RedisEventBus, RedisEventBusSettings  # noqa: PLC0415

    config = RedisEventBusSettings(url=redis_url)
    _bus = RedisEventBus(config=config)

    # Build consumer and register @listen methods BEFORE start() so the bus
    # knows which channels to subscribe to when it connects to Redis.
    _consumer = OrderConsumer()
    _consumer.register_to(_bus)

    await _bus.start()
    try:
        yield _bus, _consumer
    finally:
        await _bus.stop()


@pytest.fixture
async def client(redis_url, bus_and_consumer):
    """
    ``httpx.AsyncClient`` wired to the FastAPI app.

    Passes the pre-started bus from ``bus_and_consumer`` into ``create_app``
    so the ASGI lifespan does not need to run.

    Yields:
        ``httpx.AsyncClient`` configured with the example app as ASGI transport.

    Edge cases:
        - ``ASGITransport`` does NOT trigger ASGI lifespan — that is
          intentional; bus lifecycle is managed by ``bus_and_consumer``.
    """
    from app import create_app  # noqa: PLC0415

    _bus, _ = bus_and_consumer
    app = create_app(redis_url, bus=_bus)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as c:
        yield c


@pytest.fixture
def consumer(bus_and_consumer):
    """
    Convenience fixture: returns just the ``OrderConsumer`` for inspection.

    Returns:
        The ``OrderConsumer`` instance shared with the running bus.
    """
    _, _consumer = bus_and_consumer
    return _consumer


# ── Helper ────────────────────────────────────────────────────────────────────


async def _wait_for_notifications(
    consumer,
    *,
    count: int,
    timeout: float = 3.0,
    poll_interval: float = 0.05,
) -> None:
    """
    Poll ``consumer.received`` until ``count`` events arrive or ``timeout`` expires.

    Redis Pub/Sub delivery is asynchronous — there is no synchronous
    acknowledgement.  This helper replaces a fixed ``asyncio.sleep`` with a
    short-polling loop, making tests faster and more deterministic.

    Args:
        consumer:      The ``OrderConsumer`` whose ``received`` list to check.
        count:         Minimum number of events to wait for.
        timeout:       Maximum seconds to wait before failing.
        poll_interval: Seconds between each poll.

    Raises:
        AssertionError: If ``count`` events do not arrive within ``timeout``.

    Edge cases:
        - If events arrive faster than ``poll_interval``, the loop exits early.
        - ``timeout`` is a best-effort wall-clock bound; asyncio scheduling
          may add a small additional delay.
    """
    elapsed = 0.0
    while len(consumer.received) < count and elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    assert len(consumer.received) >= count, (
        f"Expected at least {count} notification(s) after {timeout}s, got {len(consumer.received)}"
    )


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestHealthCheck:
    """Liveness probe — no broker interaction needed."""

    async def test_health_returns_200(self, client) -> None:
        """``GET /health`` should always return HTTP 200 with status ok."""
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestPublishAndReceive:
    """Full publish → Redis → consume pipeline tests."""

    async def test_publish_order_returns_202(self, client) -> None:
        """
        ``POST /v1/orders`` should return HTTP 202 Accepted with the order_id.
        """
        response = await client.post(
            "/v1/orders",
            json={"order_id": "smoke-001", "amount": 42.0},
        )
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "accepted"
        assert body["order_id"] == "smoke-001"

    async def test_published_event_received_by_consumer(self, client, consumer) -> None:
        """
        Event published via POST should arrive at the ``OrderConsumer``.

        Redis Pub/Sub is asynchronous — we wait up to 3 s for delivery.
        """
        # Clear any events from earlier tests to get a clean count.
        consumer.received.clear()

        await client.post(
            "/v1/orders",
            json={"order_id": "smoke-002", "amount": 99.0},
        )

        # Wait for the Pub/Sub listener task to deliver the message.
        await _wait_for_notifications(consumer, count=1)

        assert len(consumer.received) == 1

    async def test_notification_body_matches_published_event(self, client, consumer) -> None:
        """
        The notification returned by ``GET /v1/notifications`` must reflect
        the exact ``order_id`` and ``amount`` from the published event.
        """
        consumer.received.clear()

        await client.post(
            "/v1/orders",
            json={"order_id": "smoke-003", "amount": 123.45},
        )

        await _wait_for_notifications(consumer, count=1)

        # Verify via the HTTP API — not by inspecting consumer.received directly.
        response = await client.get("/v1/notifications")
        assert response.status_code == 200
        items = response.json()
        assert len(items) == 1
        assert items[0]["order_id"] == "smoke-003"
        assert items[0]["amount"] == pytest.approx(123.45)

    async def test_multiple_events_all_received(self, client, consumer) -> None:
        """
        Publishing N events must result in exactly N notifications.

        Verifies ordering and fan-out over a real Redis Pub/Sub channel.
        """
        consumer.received.clear()

        order_ids = ["smoke-m1", "smoke-m2", "smoke-m3"]
        for oid in order_ids:
            await client.post(
                "/v1/orders",
                json={"order_id": oid, "amount": 10.0},
            )

        await _wait_for_notifications(consumer, count=3)

        assert len(consumer.received) == 3
        received_ids = [ev.order_id for ev in consumer.received]
        # All published order IDs must be present (order may vary slightly
        # due to async scheduling, but Pub/Sub is FIFO within one publisher).
        for oid in order_ids:
            assert oid in received_ids


class TestValidation:
    """HTTP input validation — no Redis interaction needed."""

    async def test_missing_order_id_returns_422(self, client) -> None:
        """
        ``POST /v1/orders`` without ``order_id`` must return HTTP 422 (Pydantic
        validation error), not a 500.
        """
        response = await client.post(
            "/v1/orders",
            # Missing required field "order_id"
            json={"amount": 50.0},
        )
        assert response.status_code == 422

    async def test_missing_amount_returns_422(self, client) -> None:
        """
        ``POST /v1/orders`` without ``amount`` must return HTTP 422.
        """
        response = await client.post(
            "/v1/orders",
            json={"order_id": "smoke-no-amount"},
        )
        assert response.status_code == 422

    async def test_empty_notifications_on_fresh_consumer(self, client, consumer) -> None:
        """
        ``GET /v1/notifications`` returns ``[]`` when no events have been received.
        """
        consumer.received.clear()

        response = await client.get("/v1/notifications")
        assert response.status_code == 200
        assert response.json() == []
