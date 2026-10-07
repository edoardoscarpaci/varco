"""
test_smoke.py
=============
Integration smoke tests for the ``15-nats-jetstream-events`` example.

All tests require a running NATS server with JetStream enabled and are tagged
``@pytest.mark.integration``.  Run them with::

    pytest -m integration examples/15-nats-jetstream-events/tests/

The ``nats_url`` fixture spins up a real NATS container via testcontainers
with JetStream enabled (``-js`` flag) and yields the connection URL.

The ``bus_and_consumer`` fixture starts ``NatsEventBus`` explicitly (because
``httpx.ASGITransport`` does NOT trigger ASGI lifespan) and passes the
pre-started bus into ``create_app``.

DESIGN: pre-start bus in the fixture, pass to create_app
    ✅ Avoids the ASGITransport-lifespan problem without any workaround.
    ✅ The fixture fully controls bus lifetime — ``stop()`` is guaranteed
       in the ``finally`` block even if tests fail.
    ✅ Tests can inspect ``consumer.received`` directly without touching
       request state.
    ❌ Tests must wait for JetStream delivery (polling loop) — NATS JetStream
       is asynchronous; there is no synchronous ack from the consumer side.

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
def nats_url():
    """
    Start a NATS 2.10 container with JetStream enabled and yield the URL.

    Scope is ``"session"`` — the container is shared across all tests to avoid
    the ~2 s startup overhead per test.

    The ``-js`` flag passed to the NATS server command enables JetStream;
    without it, ``NatsEventBus.start()`` would fail when trying to access the
    JetStream context.

    Yields:
        NATS connection URL string, e.g. ``"nats://127.0.0.1:32768"``.

    Edge cases:
        - Requires Docker daemon to be running.
        - Uses port 4222 inside the container; the host port is ephemeral
          (assigned by Docker).
        - ``wait_for_logs`` polls until the "Server is ready" log line
          appears — fails if NATS does not start within 30 s.
    """
    from testcontainers.core.container import DockerContainer  # noqa: PLC0415
    from testcontainers.core.waiting_utils import wait_for_logs  # noqa: PLC0415

    container = (
        DockerContainer("nats:2.10-alpine")
        .with_command("-js")  # enable JetStream — required for NatsEventBus
        .with_exposed_ports(4222)
    )
    container.start()
    try:
        # The NATS server logs this line once it is accepting connections.
        wait_for_logs(container, "Server is ready", timeout=30)
        host = container.get_container_host_ip()
        port = container.get_exposed_port(4222)
        yield f"nats://{host}:{port}"
    finally:
        container.stop()


@pytest.fixture
async def bus_and_consumer(nats_url):
    """
    Pre-start a ``NatsEventBus`` and build an ``OrderConsumer``.

    Yields the tuple ``(bus, consumer)`` with the bus already started.
    The caller (``client`` fixture) attaches the bus to ``create_app`` so
    the lifespan does NOT start/stop it — this fixture owns the lifecycle.

    Each test gets a **fresh consumer** (``received`` list starts empty) via
    function scope, but reuses the same NATS container (session scope).
    A unique ``durable_name`` per fixture invocation prevents JetStream
    consumer state from leaking between tests.

    Yields:
        Tuple of ``(NatsEventBus, OrderConsumer)`` — both ready to use.

    Edge cases:
        - Subscriptions from ``consumer.register_to(bus)`` are registered on the
          bus before ``start()`` is called.  ``start()`` then creates the
          durable JetStream consumer and begins receiving messages.
        - ``stop()`` is called in the ``finally`` block — guaranteed even
          on test failure.
        - Each invocation gets a unique ``durable_name`` to avoid the
          JetStream "consumer already exists with different config" error
          when tests create consumers for the same channel with different
          settings.
    """
    import uuid  # noqa: PLC0415

    from consumer import OrderConsumer  # noqa: PLC0415
    from varco_nats.config import NatsEventBusSettings  # noqa: PLC0415

    from varco_nats import NatsEventBus  # noqa: PLC0415

    # Unique durable name per fixture invocation — prevents JetStream consumer
    # conflicts between tests sharing the same NATS server and stream.
    durable_name = f"test-consumer-{uuid.uuid4().hex[:8]}"

    config = NatsEventBusSettings(
        servers=nats_url,
        stream_name="varco-events",
        durable_name=durable_name,
        subject_prefix="varco",
    )
    _bus = NatsEventBus(config=config)

    # Build consumer and register @listen methods BEFORE start() so the bus
    # records which channels to create JetStream consumers for on connect.
    _consumer = OrderConsumer()
    _consumer.register_to(_bus)

    await _bus.start()
    try:
        yield _bus, _consumer
    finally:
        await _bus.stop()


@pytest.fixture
async def client(nats_url, bus_and_consumer):
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
    app = create_app(nats_url, bus=_bus)

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
    timeout: float = 5.0,
    poll_interval: float = 0.1,
) -> None:
    """
    Poll ``consumer.received`` until ``count`` events arrive or ``timeout`` expires.

    NATS JetStream delivery is asynchronous — there is no synchronous
    acknowledgement from the consumer side.  This helper replaces a fixed
    ``asyncio.sleep`` with a short-polling loop, making tests faster and
    more deterministic.

    JetStream ack round-trips are slightly slower than Redis Pub/Sub, so the
    default timeout and poll interval are more generous than the Redis example.

    Args:
        consumer:      The ``OrderConsumer`` whose ``received`` list to check.
        count:         Minimum number of events to wait for.
        timeout:       Maximum seconds to wait before failing (default 5 s).
        poll_interval: Seconds between each poll (default 0.1 s).

    Raises:
        AssertionError: If ``count`` events do not arrive within ``timeout``.

    Edge cases:
        - If events arrive faster than ``poll_interval``, the loop exits early.
        - ``timeout`` is a best-effort wall-clock bound; asyncio scheduling
          may add a small additional delay.
        - JetStream at-least-once may redeliver messages — ``received`` could
          exceed ``count``; this helper only checks for a minimum.
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
    """Full publish → NATS JetStream → consume pipeline tests."""

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

        NATS JetStream is asynchronous — we poll up to 5 s for delivery.
        JetStream at-least-once guarantees the event will eventually arrive
        as long as the consumer is running and connected.
        """
        # Start from a clean slate for this test.
        consumer.received.clear()

        await client.post(
            "/v1/orders",
            json={"order_id": "smoke-002", "amount": 99.0},
        )

        # Poll until the JetStream push consumer delivers the message.
        await _wait_for_notifications(consumer, count=1)

        assert len(consumer.received) >= 1

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
        assert len(items) >= 1
        # Find the matching item — at-least-once may deliver earlier events too.
        matching = [i for i in items if i["order_id"] == "smoke-003"]
        assert len(matching) == 1
        assert matching[0]["amount"] == pytest.approx(123.45)

    async def test_multiple_events_all_received(self, client, consumer) -> None:
        """
        Publishing N events must result in at least N notifications.

        Verifies that every published order ID appears in the consumer's
        received list after JetStream delivers all messages.
        """
        consumer.received.clear()

        order_ids = ["smoke-m1", "smoke-m2", "smoke-m3"]
        for oid in order_ids:
            await client.post(
                "/v1/orders",
                json={"order_id": oid, "amount": 10.0},
            )

        # Wait for all three to arrive.
        await _wait_for_notifications(consumer, count=3)

        received_ids = [ev.order_id for ev in consumer.received]
        # All published order IDs must be present (order may vary slightly
        # due to async JetStream scheduling, but within-stream ordering is
        # FIFO for a single publisher).
        for oid in order_ids:
            assert oid in received_ids

    async def test_empty_notifications_on_fresh_consumer(self, client, consumer) -> None:
        """
        ``GET /v1/notifications`` returns ``[]`` when no events have been received.
        """
        consumer.received.clear()

        response = await client.get("/v1/notifications")
        assert response.status_code == 200
        assert response.json() == []


class TestValidation:
    """HTTP input validation — no NATS interaction needed."""

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
