# varco-core

[![PyPI version](https://img.shields.io/pypi/v/varco-core)](https://pypi.org/project/varco-core/)
[![Python](https://img.shields.io/pypi/pyversions/varco-core)](https://pypi.org/project/varco-core/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/edoardoscarpaci/varco/blob/main/LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-edoardoscarpaci%2Fvarco-blue?logo=github)](https://github.com/edoardoscarpaci/varco)

Backend-agnostic domain model and service layer for **varco**.

Provides the pure-Python building blocks that all backend packages depend on — no ORM imports at the core layer. Pair it with [`varco-sa`](https://pypi.org/project/varco-sa/) (SQLAlchemy) or [`varco-beanie`](https://pypi.org/project/varco-beanie/) (MongoDB) for a concrete backend.

---

## Install

```bash
pip install varco-core
```

---

## Features

- **Domain model** — `DomainModel`, `AuditedDomainModel`, `VersionedDomainModel`, soft-delete and multi-tenant variants
- **Generic async service** — `AsyncService` with built-in create / read / update / delete / list, pagination, soft-delete, and multi-tenancy mixins
- **Authorization** — `AbstractAuthorizer`, `GrantBasedAuthorizer`, `RoleBasedAuthorizer`, `OwnershipAuthorizer`
- **DTO layer** — `CreateDTO`, `ReadDTO`, `UpdateDTO`, `generate_dtos()` factory, cursor-based pagination
- **Fluent query builder** — `QueryBuilder` → AST → `QueryParser` (string → AST via Lark grammar); backend-independent
- **JWT / JWK** — `JwtBuilder`, `JwtParser`, `JwkBuilder`, `MultiKeyAuthority`, OIDC + PEM sources
- **Event system** — `AbstractEventBus`, `EventConsumer`, `BusEventProducer`, `listen` decorator; backend-agnostic pub/sub
- **Dead Letter Queue** — `AbstractDeadLetterQueue`, `InMemoryDeadLetterQueue`; failed events routed after retry exhaustion
- **Transactional Outbox** — `OutboxEntry`, `OutboxRepository`, `OutboxRelay`; at-least-once event delivery via DB-backed outbox
- **Resilience** — `@retry` with exponential back-off, `CircuitBreaker` (CLOSED / OPEN / HALF_OPEN), `@timeout`; composable on any sync/async callable
- **Async cache** — `InMemoryCache`, `LayeredCache`, `CachedService`; pluggable invalidation strategies (TTL, Explicit, Tagged, EventDriven, Composite)
- **Validation** — `Validator` protocol, `DomainModelValidator`, `CompositeValidator`, `ValidationResult`; collect-all business-invariant validation
- **Serialization** — `Serializer` protocol, `JsonSerializer` (Pydantic-backed), `NoOpSerializer`; pluggable ser/deser for bus and cache backends
- **Tracing** — `correlation_context`, `current_correlation_id`, `CorrelationIdFilter`
- **DI-ready** — all service classes are designed for constructor injection via [`providify`](https://pypi.org/project/providify/)

---

## What's in the package

| Module | Purpose |
|---|---|
| `model.py` | `DomainModel`, `AuditedDomainModel`, `VersionedDomainModel`, `SoftDeleteMixin`, `TenantMixin` and derived classes |
| `meta.py` | `FieldHint`, `ForeignKey`, `PrimaryKey`, `PKStrategy`, constraints, `pk_field()` |
| `mapper.py` | `AbstractMapper` — bidirectional ORM ↔ domain translation |
| `repository.py` | `AsyncRepository` ABC — CRUD + `exists()` + `stream_by_query()` |
| `uow.py` | `AsyncUnitOfWork` ABC |
| `registry.py` | `DomainModelRegistry` + `@register` decorator |
| `providers.py` | `RepositoryProvider` ABC |
| `assembler.py` | `AbstractDTOAssembler[D, C, R, U]` |
| `service/base.py` | `AsyncService`, `IUoWProvider` |
| `service/tenant.py` | `TenantAwareService`, `TenantUoWProvider`, `tenant_context` |
| `service/soft_delete.py` | `SoftDeleteService` |
| `service/types.py` | `Assembler` alias, `ServiceProtocol` |
| `auth/` | `AbstractAuthorizer`, `Action`, `AuthContext`, `ResourceGrant` |
| `auth/helpers.py` | `GrantBasedAuthorizer`, `OwnershipAuthorizer`, `RoleBasedAuthorizer` |
| `exception/` | `FastrestErrorCodes`, `ErrorCode`, `ErrorMessage`, HTTP error mapping |
| `tracing.py` | `correlation_context`, `current_correlation_id`, `CorrelationIdFilter` |
| `query/` | `QueryBuilder`, `QueryParams`, `QueryParser`, AST visitors |
| `jwt/` | `JwtBuilder`, `JwtParser`, `JwtUtil`, `JsonWebToken` |
| `jwk/` | `JwkBuilder`, `JsonWebKey`, `JsonWebKeySet` |
| `authority/` | `JwtAuthority`, `MultiKeyAuthority`, `TrustedIssuerRegistry`, OIDC/PEM sources |
| `event/` | `AbstractEventBus`, `EventConsumer`, `BusEventProducer`, `listen`, `ChannelManager` |
| `event/dlq.py` | `AbstractDeadLetterQueue`, `InMemoryDeadLetterQueue`, `DeadLetterEntry` |
| `service/outbox.py` | `OutboxEntry`, `OutboxRepository`, `OutboxRelay` |
| `service/validation.py` | `ValidatorServiceMixin` — DI-injected validator hook into `AsyncService` |
| `resilience/` | `retry`, `RetryPolicy`, `CircuitBreaker`, `CircuitBreakerConfig`, `timeout`, `CallTimeoutError`, `RetryExhaustedError`, `CircuitOpenError` |
| `cache/` | `AsyncCache`, `CacheBackend`, `InMemoryCache`, `NoOpCache`, `LayeredCache`, `CachedService`, all invalidation strategies |
| `validation.py` | `Validator`, `DomainModelValidator`, `CompositeValidator`, `ValidationResult`, `ValidationError` |
| `serialization.py` | `Serializer`, `JsonSerializer`, `NoOpSerializer` |

---

## Quick start

### Define a domain model

```python
from __future__ import annotations
from typing import Annotated
from varco_core import AuditedDomainModel
from varco_core.meta import FieldHint, PrimaryKey, PKStrategy, pk_field

class Post(AuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    title: Annotated[str, FieldHint(max_length=200)]
    body: str
    published: bool = False
```

### Wire a service with DI

```python
from varco_core import AsyncService, IUoWProvider
from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth import AbstractAuthorizer
from providify import Inject, Singleton

@Singleton
class PostService(AsyncService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO]):
    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer:   Inject[AbstractAuthorizer],
        assembler:    Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]],
    ) -> None:
        super().__init__(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

    def _get_repo(self, uow): return uow.posts
```

### Build and run a query

```python
from varco_core import QueryBuilder, QueryParams

params = QueryParams(
    node=QueryBuilder().eq("published", True).and_().gt("pk", 10).build(),
    limit=20,
    offset=0,
)

# Parse from a query string (e.g. from a URL parameter)
from varco_core import QueryParser
params = QueryParser().parse('published == true AND pk > 10', limit=20)
```

### JWT — build and verify tokens

```python
from varco_core import JwtBuilder, JwtParser

token = JwtBuilder(secret="s3cr3t").subject("user-42").expires_in(3600).build()
payload = JwtParser(secret="s3cr3t").parse(token)
```

### Cache — in-memory with TTL

```python
from varco_core.cache import InMemoryCache, TTLStrategy

async with InMemoryCache(strategy=TTLStrategy(default_ttl=300)) as cache:
    await cache.set("user:42", {"name": "Alice"})
    result = await cache.get("user:42")   # returns dict; None after 300 s
    await cache.delete("user:42")
```

### Cache — layered L1 (memory) + L2 (Redis)

```python
from varco_core.cache import InMemoryCache, LayeredCache, TTLStrategy
from varco_redis.cache import RedisCache, RedisCacheSettings

l1 = InMemoryCache(strategy=TTLStrategy(60))
l2 = RedisCache(RedisCacheSettings(url="redis://localhost:6379/0"))

async with LayeredCache(l1, l2, promote_ttl=60) as cache:
    await cache.set("product:1", product_dict, ttl=300)
    # First read fetches from L2 Redis and promotes to L1
    result = await cache.get("product:1")
    # Subsequent reads are served from L1 (in-process, zero network)
```

### Cache — `@cached` decorator (any async function or method)

```python
from varco_core.cache import cached, InMemoryCache, LayeredCache, TTLStrategy
from varco_redis.cache import RedisCache, RedisCacheSettings

# Declare a module-level cache (started at app startup)
_cache = LayeredCache(
    InMemoryCache(strategy=TTLStrategy(60)),
    RedisCache(RedisCacheSettings(key_prefix="posts:")),
    promote_ttl=60,
)

@cached(_cache, ttl=300, namespace="posts")
async def get_post(post_id: int) -> dict:
    return await db.fetch_post(post_id)  # only called on a cache miss

post = await get_post(42)     # miss → hits DB → cached
post = await get_post(42)     # hit  → served from cache

# Evict a specific entry:
await get_post.invalidate(42)

# Evict all entries in this cache:
await get_post.invalidate_all()
```

For instance methods, pass a factory callable:

```python
class PostRepository:
    def __init__(self, cache: CacheBackend) -> None:
        self._cache = cache

    @cached(lambda self: self._cache, ttl=120, namespace="posts")
    async def find_by_id(self, post_id: int) -> dict | None:
        ...
```

### Cache — `CacheServiceMixin` (look-aside built into the service)

Compose caching into your `AsyncService` via MRO — no wrapper needed,
all methods remain visible and IDE-discoverable.

`_cache` and `_cache_bus` are declared as `ClassVar[Inject[T]]` so the
**providify container injects them automatically** — no extra `__init__`
parameters required:

```python
from varco_core import AsyncService, CacheServiceMixin
from providify import Inject, Singleton

@Singleton
class PostService(
    CacheServiceMixin[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO],
    AsyncService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO],
):
    _cache_namespace = "post"   # unique key prefix
    _cache_ttl = 300             # seconds

    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer:   Inject[AbstractAuthorizer],
        assembler:    Inject[AbstractDTOAssembler[Post, ...]],
        # No cache parameter — resolved via ClassVar[Inject[CacheBackend]]
    ) -> None:
        super().__init__(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

    def _get_repo(self, uow): return uow.posts

# Register the cache backend once — all CacheServiceMixin subclasses share it:
from varco_redis.cache import RedisCacheConfiguration
container = DIContainer()
await container.ainstall(RedisCacheConfiguration)  # binds CacheBackend

# Usage is identical to a plain PostService — caching is transparent:
post = await post_service.get(42, ctx)            # look-aside (ReadDTO cached)
posts = await post_service.list(params, ctx)      # cached by params hash
await post_service.update(42, dto, ctx)           # evicts get key + list
```

Override the qualifier or injection strategy on specific services:

```python
from typing import Annotated, ClassVar
from providify import InjectMeta, LiveMeta

class PostService(CacheServiceMixin, AsyncService[Post, ...]):
    _cache_namespace = "post"
    # Use a specific qualified backend (e.g. "layered"):
    _cache: ClassVar[Annotated[CacheBackend, InjectMeta(qualifier="layered")]]

class SessionService(CacheServiceMixin, AsyncService[Session, ...]):
    _cache_namespace = "session"
    # Re-resolve on every call for a request-scoped cache:
    _cache: ClassVar[Annotated[CacheBackend, LiveMeta()]]
```

Combine with `SoftDeleteService` or `TenantAwareService` via MRO —
caching wraps all cross-cutting checks transparently:

```python
class PostService(
    CacheServiceMixin,
    TenantAwareService,
    SoftDeleteService,
    AsyncService[Post, ...],
):
    _cache_namespace = "post"
    _cache_ttl = 60
    ...
```

### Cache — cross-process invalidation via event bus

```python
from varco_core.cache import InMemoryCache, LayeredCache, TTLStrategy
from varco_redis.cache import RedisCache, RedisCacheSettings
from varco_redis import RedisEventBus, RedisEventBusSettings

bus_settings = RedisEventBusSettings(url="redis://localhost:6379/0")
cache_settings = RedisCacheSettings(url="redis://localhost:6379/0")

async with (
    RedisEventBus(bus_settings) as bus,
    LayeredCache(
        InMemoryCache(strategy=TTLStrategy(60)),
        RedisCache(cache_settings),
        promote_ttl=60,
    ) as cache,
):
    class PostService(CacheServiceMixin, AsyncService[Post, ...]):
        _cache_namespace = "post"
        _cache_bus_channel = "posts.cache.invalidations"

        def __init__(self, ..., cache, bus):
            super().__init__(...)
            self._cache = cache
            self._cache_bus = bus  # publishes CacheInvalidated on every mutation

    # All instances sharing this bus evict their L1 caches automatically.
    await post_service.update(42, dto, ctx)
```

### Cache — composable invalidation strategies

```python
from varco_core.cache import (
    InMemoryCache, CompositeStrategy, TTLStrategy, ExplicitStrategy, TaggedStrategy
)

strategy = CompositeStrategy(
    TTLStrategy(300),         # evict after 5 minutes
    ExplicitStrategy(),       # evict on demand
    TaggedStrategy(),         # evict by tag
)

async with InMemoryCache(strategy=strategy) as cache:
    await cache.set("user:1", user, tags={"users", "tenant:acme"})

    # Evict a single key
    strategy.strategies[1].invalidate("user:1")

    # Evict everything tagged "tenant:acme"
    strategy.strategies[2].invalidate_tag("tenant:acme")
```

### Resilience — retry, circuit breaker, timeout

Composable decorators for any sync or async callable:

```python
from varco_core.resilience import retry, RetryPolicy, CircuitBreaker, CircuitBreakerConfig, timeout

# Retry with exponential back-off + jitter
@retry(RetryPolicy(max_attempts=3, base_delay=0.5, retryable_on=(ConnectionError,)))
async def call_payment_api(payload: dict) -> Receipt:
    return await http_client.post("/charge", json=payload)

# Timeout (async only)
@timeout(10.0)
async def fetch_user(user_id: int) -> User:
    return await db.get(User, user_id)

# Circuit breaker — share ONE instance per external dependency
payment_breaker = CircuitBreaker(
    CircuitBreakerConfig(failure_threshold=5, recovery_timeout=30.0),
    name="payments",
)

@payment_breaker.protect
async def charge(amount: float) -> str:
    return await _charge(amount)

# Compose all three (bottom-to-top execution order):
@timeout(10.0)
@retry(RetryPolicy(max_attempts=3, base_delay=0.5))
@circuit_breaker(CircuitBreakerConfig(failure_threshold=5))
async def resilient_call() -> Response: ...
```

### Dead Letter Queue (DLQ)

Route events that exhausted all retry attempts to a DLQ for inspection or replay:

```python
from varco_core.event import EventConsumer, listen
from varco_core.event.dlq import InMemoryDeadLetterQueue, DeadLetterEntry
from varco_core.resilience import RetryPolicy

dlq = InMemoryDeadLetterQueue(max_size=10_000)  # swap for KafkaDLQ / RedisDLQ in production

class OrderConsumer(EventConsumer):
    @PostConstruct
    def _setup(self) -> None:
        self.register_to(self._bus)

    @listen(
        OrderPlacedEvent,
        channel="orders",
        retry_policy=RetryPolicy(max_attempts=3, base_delay=1.0),
        dlq=dlq,  # routed here after 3 failures
    )
    async def on_order(self, event: OrderPlacedEvent) -> None:
        await self._fulfillment.process(event)

# Replay or inspect later
entries: list[DeadLetterEntry] = await dlq.pop_batch(limit=50)
for entry in entries:
    print(entry.handler_name, entry.error_type, entry.attempts)
    await dlq.ack(entry.entry_id)
```

### Transactional Outbox — guaranteed at-least-once delivery

Persist events in the same DB transaction as your domain entity, then relay asynchronously:

```python
from varco_core.service.outbox import OutboxEntry, OutboxRelay

# 1. In your service — save event in the same transaction as the entity
async with uow:
    order = await repo.save(Order(...))
    await outbox_repo.save(OutboxEntry.from_event(
        OrderPlacedEvent(order_id=order.pk), channel="orders"
    ))

# 2. At app startup — start the relay background task
relay = OutboxRelay(outbox=outbox_repo, bus=bus, poll_interval=1.0)
await relay.start()

# 3. At shutdown
await relay.stop()
```

`OutboxRelay` polls the outbox, publishes each entry to the bus, and deletes it on success.
If the broker is down, entries accumulate until it recovers — no events are lost.

### Validation — collect-all business invariants

```python
from dataclasses import dataclass
from varco_core.model import DomainModel
from varco_core.validation import DomainModelValidator, CompositeValidator, ValidationResult

@dataclass(kw_only=True)
class Order(DomainModel):
    quantity: int
    total: float

class OrderValidator(DomainModelValidator[Order]):
    def validate(self, value: Order) -> ValidationResult:
        result = ValidationResult.ok()
        result += self._rule(value.quantity <= 0, "quantity must be positive", field="quantity")
        result += self._rule(value.total < 0, "total must be non-negative", field="total")
        return result

# All errors collected before raising — user sees the full list at once
validator = CompositeValidator(OrderValidator(), AnotherValidator())
result = validator.validate(order)
result.raise_if_invalid()  # raises ServiceValidationError with all errors joined

# Inject into a service via ValidatorServiceMixin:
from varco_core.service.validation import ValidatorServiceMixin

class OrderService(
    ValidatorServiceMixin[Order, int, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
    AsyncService[Order, int, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
):
    def _get_repo(self, uow): return uow.orders
```

### Serialization — pluggable ser/deser

```python
from varco_core.serialization import JsonSerializer, NoOpSerializer

s = JsonSerializer()
data: bytes = s.serialize({"key": "value"})                  # → UTF-8 JSON bytes
back: dict   = s.deserialize(data)                            # → raw Python dict
model        = s.deserialize(data, type_hint=MyPydanticModel) # → validated model

# Pass-through for pre-serialized bytes
noop = NoOpSerializer()
assert noop.serialize(b"raw") is b"raw"
```

---

## Related packages

| Package | Description |
|---|---|
| [`varco-sa`](https://pypi.org/project/varco-sa/) | SQLAlchemy async backend |
| [`varco-beanie`](https://pypi.org/project/varco-beanie/) | Beanie / Motor MongoDB backend |

---

## Links

- **Repository**: https://github.com/edoardoscarpaci/varco
- **Full docs**: https://github.com/edoardoscarpaci/varco#readme
- **Issue tracker**: https://github.com/edoardoscarpaci/varco/issues
