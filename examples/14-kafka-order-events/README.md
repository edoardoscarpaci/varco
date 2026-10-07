# 14 — Kafka Order Events

Demonstrates the **`varco_kafka`** event bus backend: publishing `OrderPlacedEvent`
via HTTP, consuming it with `EventConsumer` + `@listen`, and handling transient
failures with a `RetryPolicy`.

## What this example shows

| Feature | Details |
|---|---|
| `KafkaEventBus` | AT_LEAST_ONCE delivery over Kafka topics |
| `BusEventProducer` | Publish side — services never touch the bus directly |
| `@listen` + `RetryPolicy` | Handler retried up to 3× on failure (base delay 0.5 s) |
| `OrderConsumer(EventConsumer)` | Declarative `@listen`; `create_app` calls `register_to(bus)` (non-DI example — a container-bound `@Singleton` consumer is auto-registered by `CoreModule.start()`) |
| In-memory notification list | Exposed via `GET /v1/notifications` |

## Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/orders` | Publish `OrderPlacedEvent` → 202 Accepted |
| `GET` | `/v1/notifications` | List all received events |
| `GET` | `/health` | Liveness probe → `{"status": "ok"}` |

## Key difference from the Redis example (16)

- **AT_LEAST_ONCE delivery** — messages are persisted to Kafka topics and
  replayed after consumer restarts (Redis Pub/Sub is at-most-once).
- **`RetryPolicy`** on `@listen` — the handler is retried up to 3 times on
  failure before the bus moves to the next message.
- **`KafkaEventBusSettings(bootstrap_servers=..., group_id=...)`** instead of
  `RedisEventBusSettings(url=...)`.

## Run locally

```bash
# Start Kafka (KRaft mode — no ZooKeeper needed)
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_CLUSTER_ID=$(docker run --rm confluentinc/cp-kafka:7.4.0 kafka-storage random-uuid) \
  confluentinc/cp-kafka:7.4.0

# Run the app
cd examples/14-kafka-order-events
uv run uvicorn app:app --reload

# Publish an order
curl -X POST http://localhost:8000/v1/orders \
     -H 'Content-Type: application/json' \
     -d '{"order_id": "order-1", "amount": 49.99}'

# Check notifications
curl http://localhost:8000/v1/notifications
```

## Run integration tests

```bash
# Requires Docker daemon running
uv run pytest -m integration examples/14-kafka-order-events/tests/
```
