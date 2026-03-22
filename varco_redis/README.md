# varco-redis

Redis Pub/Sub event bus backend for [varco](https://github.com/edoardoscarpaci/varco).

`RedisEventBus` implements `AbstractEventBus` from `varco_core` using
[redis.asyncio](https://redis-py.readthedocs.io/en/stable/examples/asyncio_examples.html).
Published events are serialized to JSON (via `JsonEventSerializer`) and sent to
Redis Pub/Sub channels.  A background listener task polls those channels and
dispatches messages to locally registered handlers.

---

## Installation

```bash
pip install varco-redis
# or with uv:
uv add varco-redis
```

---

## Quick start

```python
from varco_redis import RedisEventBus, RedisConfig
from varco_core.event import BusEventProducer, EventConsumer, listen, Event

# Define your events
class OrderPlacedEvent(Event):
    __event_type__ = "order.placed"
    order_id: str
    total: float

# Configure the bus
config = RedisConfig(url="redis://localhost:6379/0")

async def main():
    async with RedisEventBus(config) as bus:
        # --- Consumer side ---
        class OrderConsumer(EventConsumer):
            @listen(OrderPlacedEvent, channel="orders")
            async def on_placed(self, event: OrderPlacedEvent) -> None:
                print(f"Order placed: {event.order_id}")

        OrderConsumer().register_to(bus)

        # Give the Pub/Sub subscription time to establish
        import asyncio
        await asyncio.sleep(0.1)

        # --- Producer side ---
        producer = BusEventProducer(bus)
        await producer._produce(
            OrderPlacedEvent(order_id="abc", total=99.0),
            channel="orders",
        )
```

---

## Configuration

```python
from varco_redis import RedisConfig

config = RedisConfig(
    url="redis://redis.internal:6379/0",   # Redis connection URL
    channel_prefix="prod:",               # optional — "orders" → "prod:orders"
    socket_timeout=5.0,                   # seconds, None = no timeout
)
```

| Field | Default | Description |
|---|---|---|
| `url` | `"redis://localhost:6379/0"` | Redis connection URL |
| `channel_prefix` | `""` | Prepended to every channel name |
| `decode_responses` | `False` | Must be `False` — bus expects raw bytes |
| `socket_timeout` | `None` | Socket operation timeout in seconds |
| `redis_kwargs` | `{}` | Extra kwargs for `redis.asyncio.from_url()` |

---

## Lifecycle

```python
# Explicit lifecycle
bus = RedisEventBus(config)
await bus.start()    # connects to Redis, starts listener task
# ... use bus ...
await bus.stop()     # cancels listener, closes connection

# Context manager (recommended)
async with RedisEventBus(config) as bus:
    ...
```

---

## Wildcard subscriptions

Subscribing with `channel=CHANNEL_ALL` (the default) uses Redis
`PSUBSCRIBE "*"` — the handler receives events from **every** channel
published to this Redis instance.  Use `channel_prefix` to scope channels
to your service and avoid cross-service interference:

```python
# All events with prefix "svc-a:" on this Redis
config = RedisConfig(channel_prefix="svc-a:")
bus.subscribe(MyEvent, handler)  # receives from all "svc-a:*" channels
```

---

## Running tests

```bash
# Unit tests (no Redis required)
uv sync
uv run pytest

# Integration tests (requires Docker)
VARCO_RUN_INTEGRATION=1 uv run pytest -m integration
```

---

## Delivery semantics

Redis Pub/Sub provides **at-most-once** delivery — messages published while
no subscriber is connected are **silently dropped**.  If you need
at-least-once or exactly-once delivery, use Redis Streams (planned as
`varco_redis.streams` in a future release) or switch to `varco_kafka`.
