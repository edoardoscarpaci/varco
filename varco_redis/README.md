# varco-redis

Redis backend for [varco](https://github.com/edoardoscarpaci/varco).

Provides two independent subsystems backed by Redis:

- **`RedisEventBus`** — Pub/Sub event bus implementing `AbstractEventBus` from `varco_core`
- **`RedisCache`** — async cache backend implementing `CacheBackend` from `varco_core`

---

## Installation

```bash
pip install varco-redis
# or with uv:
uv add varco-redis
```

---

## Event bus quick start

```python
from varco_redis import RedisEventBus, RedisEventBusSettings
from varco_core.event import BusEventProducer, EventConsumer, listen, Event

# Define your events
class OrderPlacedEvent(Event):
    __event_type__ = "order.placed"
    order_id: str
    total: float

# Configure the bus
settings = RedisEventBusSettings(url="redis://localhost:6379/0")

async def main():
    async with RedisEventBus(settings) as bus:
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

## Event bus configuration

```python
from varco_redis import RedisEventBusSettings

settings = RedisEventBusSettings(
    url="redis://redis.internal:6379/0",   # Redis connection URL
    channel_prefix="prod:",               # optional — "orders" → "prod:orders"
    socket_timeout=5.0,                   # seconds, None = no timeout
)
```

| Field | Default | Env var | Description |
|---|---|---|---|
| `url` | `"redis://localhost:6379/0"` | `VARCO_REDIS_URL` | Redis connection URL |
| `channel_prefix` | `""` | `VARCO_REDIS_CHANNEL_PREFIX` | Prepended to every channel name |
| `decode_responses` | `False` | — | Must be `False` — bus expects raw bytes |
| `socket_timeout` | `None` | `VARCO_REDIS_SOCKET_TIMEOUT` | Socket operation timeout in seconds |
| `redis_kwargs` | `{}` | — | Extra kwargs for `redis.asyncio.from_url()` |

---

## Event bus lifecycle

```python
# Explicit lifecycle
bus = RedisEventBus(settings)
await bus.start()    # connects to Redis, starts listener task
# ... use bus ...
await bus.stop()     # cancels listener, closes connection

# Context manager (recommended)
async with RedisEventBus(settings) as bus:
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
settings = RedisEventBusSettings(channel_prefix="svc-a:")
bus.subscribe(MyEvent, handler)  # receives from all "svc-a:*" channels
```

---

## Cache quick start

```python
from varco_redis.cache import RedisCache, RedisCacheSettings

cache_settings = RedisCacheSettings(
    url="redis://localhost:6379/0",
    key_prefix="myapp:",   # all keys stored as "myapp:<key>"
    default_ttl=300,       # seconds; None = no expiry
)

async with RedisCache(cache_settings) as cache:
    await cache.set("user:42", {"name": "Alice"})
    user = await cache.get("user:42")    # returns dict or None
    await cache.delete("user:42")
    await cache.clear()                  # removes all "myapp:*" keys
```

---

## Cache configuration

```python
from varco_redis.cache import RedisCacheSettings

settings = RedisCacheSettings(
    url="redis://localhost:6379/0",
    key_prefix="prod:",
    default_ttl=600,
    socket_timeout=2.0,
)
```

| Field | Default | Env var | Description |
|---|---|---|---|
| `url` | `"redis://localhost:6379/0"` | `VARCO_REDIS_CACHE_URL` | Redis connection URL |
| `key_prefix` | `""` | `VARCO_REDIS_CACHE_KEY_PREFIX` | Prepended to every stored key |
| `default_ttl` | `None` | `VARCO_REDIS_CACHE_DEFAULT_TTL` | Default TTL in seconds; `None` = no expiry |
| `decode_responses` | `False` | — | Must be `False` — cache stores raw bytes |
| `socket_timeout` | `None` | `VARCO_REDIS_CACHE_SOCKET_TIMEOUT` | Socket operation timeout in seconds |
| `redis_kwargs` | `{}` | — | Extra kwargs for `redis.asyncio.from_url()` |

---

## Layered cache (L1 memory + L2 Redis)

```python
from varco_core.cache import InMemoryCache, LayeredCache, TTLStrategy
from varco_redis.cache import RedisCache, RedisCacheSettings

l1 = InMemoryCache(strategy=TTLStrategy(60))       # fast in-process layer
l2 = RedisCache(RedisCacheSettings(key_prefix="app:"))  # shared Redis layer

async with LayeredCache(l1, l2, promote_ttl=60) as cache:
    await cache.set("product:1", product, ttl=300)
    # First read: L1 miss → L2 hit → promote to L1
    result = await cache.get("product:1")
    # Second read: served from L1 (no network round-trip)
```

---

## Cache + `CachedService` with cross-process invalidation

```python
from varco_core.cache import CachedService, LayeredCache, TTLStrategy, InMemoryCache
from varco_redis.cache import RedisCache, RedisCacheSettings
from varco_redis import RedisEventBus, RedisEventBusSettings

bus_settings = RedisEventBusSettings(url="redis://localhost:6379/0")
cache_settings = RedisCacheSettings(url="redis://localhost:6379/0", key_prefix="posts:")

async with (
    RedisEventBus(bus_settings) as bus,
    LayeredCache(
        InMemoryCache(strategy=TTLStrategy(60)),
        RedisCache(cache_settings),
        promote_ttl=60,
    ) as cache,
):
    cached = CachedService(
        post_service,
        cache,
        namespace="posts",
        default_ttl=300,
        bus=bus,                           # publish invalidation events
        bus_channel="posts.invalidations", # other nodes subscribe here
    )

    post = await cached.get(42)           # cached
    posts = await cached.list()           # cached
    await cached.update(42, {"title": "New"})  # invalidates + publishes event
```

---

## DI integration

```python
from providify import DIContainer
from varco_core.cache import CacheBackend
from varco_redis.cache import RedisCacheConfiguration

container = DIContainer()
await container.ainstall(RedisCacheConfiguration)

cache = await container.aget(CacheBackend)  # RedisCache singleton
await container.ashutdown()
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

## Connection settings

`RedisConnectionSettings` is a structured, env-var loadable config object
that produces a URL and kwargs for `redis.asyncio`.

### Plain connection

```python
import redis.asyncio
from varco_redis.connection import RedisConnectionSettings

conn = RedisConnectionSettings(host="my-redis", port=6379, db=0)

client = redis.asyncio.from_url(conn.to_url(), **conn.to_redis_kwargs())
# to_url()  → "redis://my-redis:6379/0"
```

### From environment variables

```bash
REDIS_HOST=my-redis
REDIS_PORT=6379
REDIS_DB=1
```

```python
conn = RedisConnectionSettings.from_env()
client = redis.asyncio.from_url(conn.to_url(), **conn.to_redis_kwargs())
```

### With password (AUTH)

```python
conn = RedisConnectionSettings(host="my-redis", password="s3cret")
# to_url() → "redis://:s3cret@my-redis:6379/0"
client = redis.asyncio.from_url(conn.to_url(), **conn.to_redis_kwargs())
```

### With ACL username + password (Redis 6+)

```python
conn = RedisConnectionSettings(
    host="my-redis",
    username="alice",
    password="s3cret",
)
# to_url() → "redis://alice:s3cret@my-redis:6379/0"
```

### With TLS / SSL

```python
from varco_core.connection import SSLConfig
from pathlib import Path

ssl = SSLConfig(ca_cert=Path("/etc/ssl/redis-ca.pem"), verify=True)
conn = RedisConnectionSettings.with_ssl(ssl, host="prod-redis")
# to_url()           → "rediss://prod-redis:6379/0"
# to_redis_kwargs()  → {"decode_responses": False, "ssl": <SSLContext>}

client = redis.asyncio.from_url(conn.to_url(), **conn.to_redis_kwargs())
```

Or from env:

```bash
REDIS_HOST=prod-redis
REDIS_SSL__CA_CERT=/etc/ssl/redis-ca.pem
REDIS_SSL__VERIFY=true
```

### With mTLS (client certificates)

```python
ssl = SSLConfig(
    ca_cert=Path("/etc/ssl/ca.pem"),
    client_cert=Path("/etc/ssl/client.crt"),
    client_key=Path("/etc/ssl/client.key"),
)
conn = RedisConnectionSettings.with_ssl(ssl, host="prod-redis")
```

### Connection settings reference

| Env var | Default | Description |
|---|---|---|
| `REDIS_HOST` | `localhost` | Redis server hostname |
| `REDIS_PORT` | `6379` | Redis server port |
| `REDIS_DB` | `0` | Database index (0–15) |
| `REDIS_PASSWORD` | — | AUTH password |
| `REDIS_USERNAME` | — | ACL username (Redis 6+) |
| `REDIS_DECODE_RESPONSES` | `false` | Return strings instead of bytes |
| `REDIS_SOCKET_TIMEOUT` | — | Socket timeout in seconds |
| `REDIS_SSL__CA_CERT` | — | Path to CA certificate |
| `REDIS_SSL__CLIENT_CERT` | — | Path to client certificate (mTLS) |
| `REDIS_SSL__CLIENT_KEY` | — | Path to client private key (mTLS) |
| `REDIS_SSL__VERIFY` | `true` | TLS peer verification |

> **Note:** `RedisConnectionSettings` is a general-purpose connection config.
> `RedisEventBusSettings` (used by `RedisEventBus`) is a separate, independent
> class — existing code that uses the event bus is unaffected.

---

## Delivery semantics

Redis Pub/Sub provides **at-most-once** delivery — messages published while
no subscriber is connected are **silently dropped**.  If you need
at-least-once or exactly-once delivery, use Redis Streams (planned as
`varco_redis.streams` in a future release) or switch to `varco_kafka`.
