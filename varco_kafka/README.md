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

## Connection settings

`KafkaConnectionSettings` is a structured, env-var loadable config object
that produces kwargs for `AIOKafkaProducer` and `AIOKafkaConsumer`.

### Plain connection (PLAINTEXT)

```python
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from varco_kafka.connection import KafkaConnectionSettings

conn = KafkaConnectionSettings(
    bootstrap_servers="broker1:9092,broker2:9092",
    group_id="order-service",
)

producer = AIOKafkaProducer(**conn.to_aiokafka_kwargs())
consumer = AIOKafkaConsumer("orders", **conn.to_aiokafka_kwargs())

await producer.start()
await consumer.start()
```

### From environment variables

```bash
KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092
KAFKA_GROUP_ID=order-service
```

```python
conn = KafkaConnectionSettings.from_env()
producer = AIOKafkaProducer(**conn.to_aiokafka_kwargs())
```

You can also configure a single broker via `KAFKA_HOST` + `KAFKA_PORT` without
setting `KAFKA_BOOTSTRAP_SERVERS` — the settings synthesise it automatically:

```bash
KAFKA_HOST=my-broker
KAFKA_PORT=9093
# bootstrap_servers → "my-broker:9093"
```

### With TLS / SSL (no auth)

```python
from varco_core.connection import SSLConfig
from pathlib import Path

ssl = SSLConfig(ca_cert=Path("/etc/ssl/kafka-ca.pem"), verify=True)
conn = KafkaConnectionSettings.with_ssl(
    ssl,
    bootstrap_servers="broker:9093",
    group_id="my-service",
)
# security_protocol → "SSL"
producer = AIOKafkaProducer(**conn.to_aiokafka_kwargs())
```

Or from env:

```bash
KAFKA_BOOTSTRAP_SERVERS=broker:9093
KAFKA_SSL__CA_CERT=/etc/ssl/kafka-ca.pem
KAFKA_SSL__VERIFY=true
```

### With SASL authentication (SASL_PLAINTEXT)

```python
from varco_core.connection import SaslConfig

conn = KafkaConnectionSettings(
    bootstrap_servers="broker:9092",
    group_id="my-service",
    auth=SaslConfig(
        mechanism="SCRAM-SHA-256",
        username="alice",
        password="secret",
    ),
)
# security_protocol → "SASL_PLAINTEXT"
producer = AIOKafkaProducer(**conn.to_aiokafka_kwargs())
```

Or from env:

```bash
KAFKA_AUTH__TYPE=sasl
KAFKA_AUTH__MECHANISM=SCRAM-SHA-256
KAFKA_AUTH__USERNAME=alice
KAFKA_AUTH__PASSWORD=secret
```

### With SASL + TLS (SASL_SSL)

```python
ssl = SSLConfig(ca_cert=Path("/etc/ssl/ca.pem"), verify=True)
conn = KafkaConnectionSettings.with_ssl(
    ssl,
    bootstrap_servers="broker:9093",
    group_id="my-service",
    auth=SaslConfig(mechanism="SCRAM-SHA-256", username="alice", password="secret"),
)
# security_protocol → "SASL_SSL"
```

### SASL PLAIN via BasicAuthConfig

`BasicAuthConfig` (type `"basic"`) is automatically mapped to SASL PLAIN:

```python
from varco_core.connection import BasicAuthConfig

conn = KafkaConnectionSettings(
    bootstrap_servers="broker:9092",
    auth=BasicAuthConfig(username="alice", password="secret"),
)
# sasl_mechanism → "PLAIN"
```

Or from env:

```bash
KAFKA_AUTH__TYPE=basic
KAFKA_AUTH__USERNAME=alice
KAFKA_AUTH__PASSWORD=secret
```

### `security_protocol` matrix

| `ssl` | `auth` | `security_protocol` |
|---|---|---|
| not set | not set | `PLAINTEXT` |
| set | not set | `SSL` |
| not set | set | `SASL_PLAINTEXT` |
| set | set | `SASL_SSL` |

### Connection settings reference

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated broker addresses |
| `KAFKA_HOST` | `localhost` | Single broker hostname (synthesises `bootstrap_servers`) |
| `KAFKA_PORT` | `9092` | Single broker port (synthesises `bootstrap_servers`) |
| `KAFKA_GROUP_ID` | `varco-default` | Consumer group ID |
| `KAFKA_SSL__CA_CERT` | — | Path to CA certificate |
| `KAFKA_SSL__CLIENT_CERT` | — | Path to client certificate (mTLS) |
| `KAFKA_SSL__CLIENT_KEY` | — | Path to client private key (mTLS) |
| `KAFKA_SSL__VERIFY` | `true` | TLS peer verification |
| `KAFKA_AUTH__TYPE` | — | `sasl` or `basic` |
| `KAFKA_AUTH__MECHANISM` | `PLAIN` | SASL mechanism (`PLAIN`, `SCRAM-SHA-256`, etc.) |
| `KAFKA_AUTH__USERNAME` | — | SASL username |
| `KAFKA_AUTH__PASSWORD` | — | SASL password |

> **Note:** `KafkaConnectionSettings` is a general-purpose connection config.
> `KafkaEventBusSettings` (used by `KafkaEventBus`) is a separate, independent
> class — existing event bus code is unaffected.

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
