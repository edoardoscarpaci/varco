"""
test_smoke.py
=============
Integration smoke tests for the ``14-kafka-order-events`` example.

All tests require a running Kafka broker and are tagged
``@pytest.mark.integration``.  Run them with::

    pytest -m integration examples/14-kafka-order-events/tests/

The ``kafka_servers`` fixture spins up a real Kafka container via
testcontainers and yields the bootstrap servers string.  The ``bus_and_consumer``
fixture starts ``KafkaEventBus`` explicitly (because ``httpx.ASGITransport``
does NOT trigger ASGI lifespan — see FINDINGS F06) and passes the pre-started
bus into ``create_app``.

DESIGN: pre-start bus in the fixture, pass to create_app
    ✅ Avoids the ASGITransport-lifespan problem without any workaround.
    ✅ The fixture fully controls bus lifetime — ``stop()`` is guaranteed
       in the ``finally`` block even if tests fail.
    ✅ Tests can inspect ``consumer.received`` directly without touching
       request state.
    ❌ Tests must wait for Kafka message delivery — Kafka consumer group
       coordination is asynchronous.  We use a short polling loop instead
       of a fixed sleep to keep tests fast.

DESIGN: unique consumer group ID per fixture invocation
    ✅ Prevents offset interference between tests — each test's consumer
       starts fresh, not from where the previous test left off.
    ✅ ``auto_offset_reset="earliest"`` ensures messages published before
       the consumer fully joined are still received.
    ❌ Orphaned consumer groups accumulate in the broker during the test run.
       For production Kafka, clean up groups; for testcontainers this is fine
       since the container is destroyed after the session.

Thread safety:  ❌  Single asyncio event loop; no concurrent test execution.
Async safety:   ✅  All test functions are ``async def``.
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest
from httpx import ASGITransport

pytestmark = pytest.mark.integration


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def kafka_servers():
    """
    Start a Kafka broker container and yield the bootstrap servers string.

    Scope is ``"session"`` — the container is shared across all tests to avoid
    the ~10 s startup overhead per test.  The ``confluentinc/cp-kafka`` image
    is used because it starts reliably without requiring ZooKeeper in KRaft mode.

    Yields:
        Bootstrap servers string, e.g. ``"127.0.0.1:32768"``.

    Edge cases:
        - Requires Docker daemon to be running.
        - First run downloads the image (~500 MB); subsequent runs use cache.
        - The host port is ephemeral (assigned by Docker).
    """
    from testcontainers.kafka import KafkaContainer  # noqa: PLC0415

    with KafkaContainer("confluentinc/cp-kafka:7.4.0") as kafka:
        yield kafka.get_bootstrap_server()


@pytest.fixture
async def bus_and_consumer(kafka_servers):
    """
    Pre-start a ``KafkaEventBus`` and build an ``OrderConsumer``.

    Creates a fresh consumer group per invocation (unique UUID suffix) so
    tests don't share Kafka offsets.  ``auto_offset_reset="earliest"`` ensures
    messages published just before the consumer joined are still received.

    Yields the tuple ``(bus, consumer)`` with the bus already started.
    The caller (``client`` fixture) attaches the bus to ``create_app`` so
    the lifespan does NOT start/stop it — this fixture owns the lifecycle.

    Yields:
        Tuple of ``(KafkaEventBus, OrderConsumer)`` — both ready to use.

    Edge cases:
        - ``consumer.register_to(bus)`` is called before ``bus.start()`` — subscriptions
          are registered on the bus object, and ``start()`` creates the Kafka
          consumer for those topics.
        - ``stop()`` is called in the ``finally`` block — guaranteed even
          on test failure.
        - Kafka consumer group formation takes a few hundred milliseconds
          after ``start()``.  Use ``_wait_for_notifications()`` rather than
          fixed sleeps.
    """
    from consumer import OrderConsumer  # noqa: PLC0415
    from varco_kafka.config import KafkaEventBusSettings  # noqa: PLC0415

    from varco_kafka import KafkaEventBus  # noqa: PLC0415

    # Unique group ID per test run — prevents offset interference between tests.
    group_id = f"test-smoke-{uuid.uuid4().hex[:8]}"

    config = KafkaEventBusSettings(
        bootstrap_servers=kafka_servers,
        group_id=group_id,
        # earliest — so consumers see messages published before they fully joined.
        auto_offset_reset="earliest",
    )
    _bus = KafkaEventBus(config=config)

    # Build consumer and register @listen methods BEFORE start() so the bus
    # knows which topics to subscribe to when it connects to Kafka.
    _consumer = OrderConsumer()
    _consumer.register_to(_bus)

    await _bus.start()
    try:
        yield _bus, _consumer
    finally:
        await _bus.stop()


@pytest.fixture
async def client(kafka_servers, bus_and_consumer):
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
    app = create_app(kafka_servers, bus=_bus)

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

    Kafka delivery is asynchronous — consumer group formation and message
    dispatch add latency vs Redis Pub/Sub.  This helper replaces a fixed
    ``asyncio.sleep`` with a short-polling loop, making tests faster and more
    deterministic.

    Args:
        consumer:      The ``OrderConsumer`` whose ``received`` list to check.
        count:         Minimum number of events to wait for.
        timeout:       Maximum seconds to wait before failing.  Default 5 s
                       because Kafka consumer group coordination can take up
                       to ~1–2 s after ``start()``.
        poll_interval: Seconds between each poll.

    Raises:
        AssertionError: If ``count`` events do not arrive within ``timeout``.

    Edge cases:
        - If events arrive faster than ``poll_interval``, the loop exits early.
        - ``timeout`` is a best-effort wall-clock bound; asyncio scheduling
          may add a small additional delay.
        - Kafka AT_LEAST_ONCE may deliver duplicates; this helper only checks
          that at least ``count`` events arrived.
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
    """Full publish → Kafka → consume pipeline tests."""

    async def test_publish_order_returns_202(self, client) -> None:
        """
        ``POST /v1/orders`` should return HTTP 202 Accepted with the order_id.
        """
        response = await client.post(
            "/v1/orders",
            json={"order_id": "kafka-001", "amount": 42.0},
        )
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "accepted"
        assert body["order_id"] == "kafka-001"

    async def test_published_event_received_by_consumer(self, client, consumer) -> None:
        """
        Event published via POST should arrive at the ``OrderConsumer``.

        Kafka delivery is asynchronous — we poll up to 5 s for the consumer
        group to receive and dispatch the message.
        """
        # Clear any events from earlier tests to get a clean count.
        consumer.received.clear()

        await client.post(
            "/v1/orders",
            json={"order_id": "kafka-002", "amount": 99.0},
        )

        # Wait for the Kafka consumer loop to dispatch the message.
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
            json={"order_id": "kafka-003", "amount": 123.45},
        )

        await _wait_for_notifications(consumer, count=1)

        # Verify via the HTTP API — not by inspecting consumer.received directly.
        response = await client.get("/v1/notifications")
        assert response.status_code == 200
        items = response.json()
        assert len(items) >= 1
        # Find our specific event — AT_LEAST_ONCE may include others.
        our_items = [i for i in items if i["order_id"] == "kafka-003"]
        assert len(our_items) >= 1
        assert our_items[0]["amount"] == pytest.approx(123.45)

    async def test_multiple_events_all_received(self, client, consumer) -> None:
        """
        Publishing N events must result in at least N notifications.

        Verifies that all published order IDs are received over the Kafka
        topic.  AT_LEAST_ONCE delivery means the list may contain more events
        than published (from previous tests), so we only assert presence.
        """
        consumer.received.clear()

        order_ids = ["kafka-m1", "kafka-m2", "kafka-m3"]
        for oid in order_ids:
            await client.post(
                "/v1/orders",
                json={"order_id": oid, "amount": 10.0},
            )

        await _wait_for_notifications(consumer, count=3)

        received_ids = [ev.order_id for ev in consumer.received]
        # All published order IDs must be present.
        for oid in order_ids:
            assert oid in received_ids


class TestValidation:
    """HTTP input validation — no Kafka interaction needed."""

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
            json={"order_id": "kafka-no-amount"},
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
