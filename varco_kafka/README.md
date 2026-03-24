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
from varco_kafka import KafkaEventBus, KafkaEventBusSettings
from varco_core.event import BusEventProducer, EventConsumer, listen, Event

# Define your events
class OrderPlacedEvent(Event):
    __event_type__ = "order.placed"
    order_id: str
    total: float

# Configure the bus
settings = KafkaEventBusSettings(
    bootstrap_servers="localhost:9092",
    group_id="order-service",
)

async def main():
    async with KafkaEventBus(settings) as bus:
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

### Event bus

```python
from varco_kafka import KafkaEventBusSettings

settings = KafkaEventBusSettings(
    bootstrap_servers="kafka.internal:9092",   # broker address(es)
    group_id="my-service",                     # consumer group ID
    channel_prefix="prod.",                    # optional — "orders" → "prod.orders"
    auto_offset_reset="latest",                # "latest" or "earliest"
    enable_auto_commit=True,                   # at-least-once delivery
)
```

| Field | Default | Env var | Description |
|---|---|---|---|
| `bootstrap_servers` | `"localhost:9092"` | `VARCO_KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address(es) |
| `group_id` | `"varco-default"` | `VARCO_KAFKA_GROUP_ID` | Consumer group ID |
| `channel_prefix` | `""` | `VARCO_KAFKA_CHANNEL_PREFIX` | Prepended to every topic name |
| `auto_offset_reset` | `"latest"` | `VARCO_KAFKA_AUTO_OFFSET_RESET` | Offset policy for new consumer groups |
| `enable_auto_commit` | `True` | `VARCO_KAFKA_ENABLE_AUTO_COMMIT` | Auto-commit consumer offsets |
| `producer_kwargs` | `{}` | — | Extra kwargs for `AIOKafkaProducer` |
| `consumer_kwargs` | `{}` | — | Extra kwargs for `AIOKafkaConsumer` |

### Channel manager (topic administration)

`KafkaChannelManager` handles Kafka topic creation and deletion.  It uses a
separate settings class so admin credentials never bleed into the bus:

```python
from varco_kafka import KafkaChannelManager, KafkaChannelManagerSettings

admin_settings = KafkaChannelManagerSettings(
    bootstrap_servers="kafka.internal:9092",
    # env prefix: VARCO_KAFKA_ADMIN_
)

async with KafkaChannelManager(admin_settings) as manager:
    await manager.declare_channel("orders")      # create topic if absent
    await manager.delete_channel("orders")       # delete topic
```

| Field | Default | Env var | Description |
|---|---|---|---|
| `bootstrap_servers` | `"localhost:9092"` | `VARCO_KAFKA_ADMIN_BOOTSTRAP_SERVERS` | Kafka broker address(es) |
| `admin_kwargs` | `{}` | — | Extra kwargs for `AIOKafkaAdminClient` |

---

## Lifecycle

```python
# Explicit lifecycle
bus = KafkaEventBus(settings)
await bus.start()     # connects producer, starts consumer task
# ... use bus ...
await bus.stop()      # flushes producer, cancels consumer task

# Context manager (recommended)
async with KafkaEventBus(settings) as bus:
    ...
```

---

## DI integration

`varco_kafka` ships two `@Configuration` classes so you can install only what
each service needs:

```python
from providify import DIContainer
from varco_core.event import AbstractEventBus, ChannelManager
from varco_kafka import KafkaEventBusConfiguration, KafkaChannelManagerConfiguration

# Services that only publish/consume events
container = DIContainer()
await container.ainstall(KafkaEventBusConfiguration)
bus = await container.aget(AbstractEventBus)

# Admin services that also manage topics
await container.ainstall(KafkaChannelManagerConfiguration)
manager = await container.aget(ChannelManager)

await container.ashutdown()
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
