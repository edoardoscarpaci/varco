# Example 17 — Transactional Outbox

Demonstrates the **transactional outbox pattern** for guaranteed at-least-once
event delivery, using `varco_sa` (SQLAlchemy/PostgreSQL) and `SADeduplicator`.

## What it shows

| Concept | Implementation |
|---|---|
| Atomic order + event write | `OrderService.create()` saves `Order` + `OutboxEntry` in one SA transaction |
| Background relay | `OutboxRelay` polls `varco_outbox` table and publishes to `InMemoryEventBus` |
| At-least-once delivery | Relay deletes the outbox row only after `bus.publish()` succeeds |
| Deduplication | `SADeduplicator` (backed by `varco_dedup_log` table) in `OrderConsumer` |

## Infrastructure

| Service | Purpose |
|---|---|
| PostgreSQL | Orders, `varco_outbox`, and `varco_dedup_log` tables |

## Run locally

```bash
export DATABASE_URL="postgresql+asyncpg://user:pw@localhost:5432/mydb"
cd examples/17-transactional-outbox
VARCO_SECURITY_ENV=development uv run uvicorn app:app --reload
```

```bash
# Create an order (event goes to outbox atomically)
curl -X POST http://localhost:8000/v1/orders \
     -H "Content-Type: application/json" \
     -d '{"amount": 42.5}'

# Poll until relay delivers the event
curl http://localhost:8000/v1/events
```

## Run integration tests

```bash
VARCO_SECURITY_ENV=development uv run pytest examples/17-transactional-outbox/tests/ -v -m integration
```

## Key design decisions

- **`InMemoryEventBus`** is used as the event bus so this example needs only
  Postgres (no Redis/Kafka container). In production, swap for `KafkaEventBus`
  or `RedisEventBus` — only `create_app` changes.
- **`poll_interval=0.1`** seconds in the relay — fast for demo purposes.
  Set to `1.0`+ in production to reduce DB load.
- **`SADeduplicator`** guards against relay crash-before-delete replays.
  The `event_id` UUID is the idempotency key.
