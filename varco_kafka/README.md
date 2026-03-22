# varco-kafka

Apache Kafka event bus backend for [varco](https://github.com/edoardoscarpaci/varco).

`KafkaEventBus` implements `AbstractEventBus` from `varco_core` using
[aiokafka](https://aiokafka.readthedocs.io/).  Published events are serialized
to JSON (via `JsonEventSerializer`) and sent to Kafka topics.  A background
consumer task reads from those topics and dispatches messages to locally
registered handlers.

---

## Installation

```bash
pip install varco-kafka
# or with uv:
uv add varco-kafka
```

---

## Quick start

```python
from varco_kafka import KafkaEventBus, KafkaConfig
from varco_core.event import BusEventProducer, EventConsumer, listen, Event

# Define your events
class OrderPlacedEvent(Event):
    __event_type__ = "order.placed"
    order_id: str
    total: float

# Configure the bus
config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="order-service",
)

async def main():
    async with KafkaEventBus(config) as bus:
        # --- Consumer side ---
        class OrderConsumer(EventConsumer):
            @listen(OrderPlacedEvent, channel="orders")
            async def on_placed(self, event: OrderPlacedEvent) -> None:
                print(f"Order placed: {event.order_id}")

        OrderConsumer().register_to(bus)

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
from varco_kafka import KafkaConfig

config = KafkaConfig(
    bootstrap_servers="kafka.internal:9092",   # broker address(es)
    group_id="my-service",                     # consumer group ID
    topic_prefix="prod.",                      # optional — "orders" → "prod.orders"
    auto_offset_reset="latest",                # "latest" or "earliest"
    enable_auto_commit=True,                   # at-least-once delivery
)
```

| Field | Default | Description |
|---|---|---|
| `bootstrap_servers` | `"localhost:9092"` | Kafka broker address(es) |
| `group_id` | `"varco-default"` | Consumer group ID |
| `topic_prefix` | `""` | Prepended to every topic name |
| `auto_offset_reset` | `"latest"` | Offset policy for new consumer groups |
| `enable_auto_commit` | `True` | Auto-commit consumer offsets |
| `producer_kwargs` | `{}` | Extra kwargs for `AIOKafkaProducer` |
| `consumer_kwargs` | `{}` | Extra kwargs for `AIOKafkaConsumer` |

---

## Lifecycle

```python
# Explicit lifecycle
bus = KafkaEventBus(config)
await bus.start()     # connects producer, starts consumer task
# ... use bus ...
await bus.stop()      # flushes producer, cancels consumer task

# Context manager (recommended)
async with KafkaEventBus(config) as bus:
    ...
```

---

## Running tests

```bash
# Unit tests (no Kafka required)
uv sync
uv run pytest

# Integration tests (requires Docker)
VARCO_RUN_INTEGRATION=1 uv run pytest -m integration
```

---

## Delivery semantics

`KafkaEventBus` provides **at-least-once** delivery with `enable_auto_commit=True`.
Consumer errors are logged and do not stop the consumer loop.

For **exactly-once** semantics, configure Kafka transactions in
`producer_kwargs` / `consumer_kwargs` — this is out of scope for the bus itself.
