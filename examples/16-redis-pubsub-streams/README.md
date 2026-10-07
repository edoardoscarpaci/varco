# Example 16 — Redis Pub/Sub & Streams Event Bus

**Skill taught**: `varco_redis` eventing — `RedisEventBus` (Pub/Sub, at-most-once) and
the `RedisStreamEventBus` (Redis Streams, at-least-once), combined with `EventConsumer +
@listen` and `AbstractEventProducer`.

---

## What you'll learn

| Concept | Where |
|---|---|
| `RedisEventBus` — Pub/Sub, at-most-once | `app.py`, `consumer.py` |
| `RedisStreamEventBus` — Streams, at-least-once | `varco_redis.streams` (discussed below) |
| `EventConsumer + @listen` — declarative subscriptions | `consumer.py` |
| `AbstractEventProducer` — publish without holding the bus | `router.py` |
| Domain event deserialization (`JsonEventSerializer`) | `events.py` |
| Pre-start bus pattern for tests (ASGITransport + lifespan) | `tests/conftest.py`, `tests/test_smoke.py` |

---

## Domain scenario — Notification Hub

```
POST /v1/orders        — publish OrderPlacedEvent {order_id, amount}
GET  /v1/notifications — list events received by the consumer
GET  /health           — liveness probe
```

An `OrderConsumer` listens on the `"orders"` channel and appends every
`OrderPlacedEvent` to an in-memory list.  The HTTP layer exposes that list.

---

## File layout

```
16-redis-pubsub-streams/
├── app.py       # create_app(redis_url, *, bus=None) — wires bus, consumer, producer
├── events.py    # OrderPlacedEvent (DomainEvent subclass)
├── consumer.py  # OrderConsumer(EventConsumer) with @listen
├── router.py    # POST /v1/orders, GET /v1/notifications, GET /health
└── tests/
    ├── __init__.py
    ├── conftest.py   # sys.path setup
    └── test_smoke.py # 8 integration tests (@pytest.mark.integration)
```

---

## Key patterns

### 1. `@listen` is declarative — `register_to` is imperative

```python
class OrderConsumer(EventConsumer):
    def __init__(self) -> None:
        self.received = []  # no bus reference — only register_to() touches it

    @listen(OrderPlacedEvent, channel="orders")
    async def on_order(self, event: OrderPlacedEvent) -> None:
        self.received.append(event)


consumer = OrderConsumer()
consumer.register_to(bus)  # subscriptions created HERE
```

`@listen` stores metadata on the method at class-definition time.  No subscription
exists until `register_to(bus)` is called.  This keeps the consumer bus-agnostic and
testable without a real broker.  This example is not DI-wired, so `create_app` calls
`register_to(bus)` itself; in a DI-wired app the consumer is a `@Singleton` bound in
the container and `CoreModule.start()` registers it after the bus starts (Plan 051 /
DI-5) — no hand-written registration at all.

### 2. Publish via `AbstractEventProducer`, not the bus

```python
# router.py — the HTTP handler never sees AbstractEventBus
await producer._produce(event, channel="orders")
```

`BusEventProducer` wraps the bus and exposes only `_produce()`.  Services and HTTP
handlers depend on the producer interface — swapping Kafka for Redis requires only a
DI rebinding, zero service changes.

### 3. Pre-start bus in tests (ASGITransport + lifespan workaround)

`httpx.ASGITransport` does **not** trigger the ASGI lifespan.  Solution: start the bus
in the fixture and pass the live instance into `create_app`:

```python
@pytest.fixture
async def bus_and_consumer(redis_url):
    config = RedisEventBusSettings(url=redis_url)
    _bus = RedisEventBus(config=config)
    _consumer = OrderConsumer()
    _consumer.register_to(_bus)  # register @listen before start()
    await _bus.start()
    try:
        yield _bus, _consumer
    finally:
        await _bus.stop()

@pytest.fixture
async def client(redis_url, bus_and_consumer):
    _bus, _ = bus_and_consumer
    app = create_app(redis_url, bus=_bus)   # lifespan skips start/stop
    async with httpx.AsyncClient(transport=ASGITransport(app=app), ...) as c:
        yield c
```

### 4. Pub/Sub vs Streams — when to choose each

| | `RedisEventBus` (Pub/Sub) | `RedisStreamEventBus` (Streams) |
|---|---|---|
| Delivery | At-most-once | At-least-once |
| Persistence | No (offline = lost) | Yes (retained until ACK) |
| Consumer groups | No | Yes (load-balanced fan-out) |
| Complexity | Low | Medium |
| Use when | Metrics, presence, cache invalidation | Orders, payments, audit events |

Switch to Streams by setting `VARCO_REDIS_USE_STREAMS=true` (env var) — the
`RedisEventBusSelectorConfiguration` in `varco_redis.bus` handles the selection.

---

## Running the tests

```bash
# From the workspace root — requires Docker
uv run pytest examples/16-redis-pubsub-streams/tests/ -v -m integration
```

---

## Running the app

```bash
VARCO_REDIS_URL=redis://localhost:6379/0 \
  uv run uvicorn examples.16-redis-pubsub-streams.app:app --reload
```

Or using the factory directly:

```python
from examples.16_redis_pubsub_streams.app import create_app
app = create_app("redis://localhost:6379/0")
```
