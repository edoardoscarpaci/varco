# Architecture Reference — varco

Complete technical map of all packages, modules, classes, and design patterns. Use this to navigate the codebase efficiently.

---

## Package Overview

```
varco_core/              — Domain model, service layer, event system, resilience, DI contracts
  ├── event/             — AbstractEventBus, AbstractEventProducer, EventConsumer, @listen
  ├── service/           — AsyncService[D, PK, C, R, U], mixins (validator, tenant, soft-delete)
  ├── cache/             — AsyncCache protocol, CacheBackend ABC, invalidation strategies
  ├── query/             — QueryParser → AST → QueryTransformer → backend applicator
  ├── resilience/        — @timeout, @retry, @circuit_breaker decorators
  ├── authority/         — JwtAuthority, TrustedIssuerRegistry, key rotation
  ├── auth/              — AbstractAuthorizer, user/role/permission models
  ├── repository.py      — AsyncRepository[D, PK] protocol
  ├── uow.py             — AsyncUnitOfWork, IUoWProvider protocols
  ├── model.py           — DomainModel ABC
  ├── dto/               — DTOBase, DTOFactory, pagination
  ├── mapper.py          — Type mapping utilities
  ├── providers.py       — DIContainer, DI wiring helpers
  └── exception/         — Domain, service, query, HTTP exception hierarchy

varco_kafka/             — Kafka event bus backend (aiokafka)
  ├── bus.py             — KafkaEventBus(AbstractEventBus)
  ├── channel.py         — KafkaChannel (topic routing)
  ├── dlq.py             — KafkaDLQ (dead letter queue)
  ├── config.py          — KafkaConfig (frozen dataclass)
  └── di.py              — KafkaEventBusConfiguration (@Configuration)

varco_redis/             — Redis Pub/Sub event bus + cache backend (redis.asyncio)
  ├── bus.py             — RedisEventBus(AbstractEventBus)
  ├── cache.py           — RedisCache(CacheBackend[K, V])
  ├── streams.py         — Redis streams utilities (for channels)
  ├── channel.py         — RedisChannel (pubsub or stream routing)
  ├── dlq.py             — RedisDLQ (dead letter queue)
  ├── config.py          — RedisConfig, CacheConfig (frozen dataclasses)
  └── di.py              — RedisEventBusConfiguration, RedisCacheConfiguration

varco_sa/                — SQLAlchemy async ORM backend
  ├── __init__.py        — SAConfig, SAModelFactory, bind_repositories()
  ├── outbox.py          — OutboxRepository impl for SQLAlchemy
  ├── di.py              — SAConfiguration (@Configuration)
  └── (auto-generated)   — ORM models created from DomainModel subclasses at import time

varco_beanie/            — Beanie/MongoDB async ODM backend
  ├── __init__.py        — BeanieConfig, BeanieModelFactory
  ├── outbox.py          — OutboxRepository impl for Beanie
  └── di.py              — BeanieConfiguration (@Configuration)
```

---

## Type Hierarchy & Protocols

### Event System

```
AbstractEventBus (ABC)
  ├── InMemoryEventBus        (tests)
  ├── KafkaEventBus           (varco_kafka)
  └── RedisEventBus           (varco_redis)

AbstractEventProducer (ABC)
  └── EventProducer(producer: AbstractEventBus)

EventConsumer (ABC)
  └── @listen decorator + register_to(bus) pattern
  └── Composable with: EventDrivenStrategy (cache invalidation)

AbstractDeadLetterQueue (ABC)
  ├── InMemoryDeadLetterQueue (tests)
  ├── KafkaDLQ                (varco_kafka)
  └── RedisDLQ                (varco_redis)
```

### Service Layer

```
AsyncService[D, PK, C, R, U] (ABC)
  ├── Abstract: _get_repo(uow) → AsyncRepository[D, PK]
  ├── Hooks (chainable via super()):
  │   ├── _scoped_params(user, ...) → dict  (tenant, authorization)
  │   ├── _check_entity(entity)             (authorization checks)
  │   └── _prepare_for_create(data)         (normalization)
  │
  └── Methods:
      ├── create(data: C) → R
      ├── read(pk: PK, **scope) → R
      ├── update(pk: PK, data: U, **scope) → R
      ├── delete(pk: PK, **scope) → None
      └── list(query: QueryParams, **scope) → Page[R]

Mixins (MRO-composable):
  ├── ValidatorServiceMixin         — calls @validate at layer boundary
  ├── TenantAwareService            — injects tenant_id into scope
  ├── SoftDeleteService             — filters deleted entities by default
  ├── CacheServiceMixin             — caches read/list results
  └── EventConsumer                 — listens to events, composes via register_to()
```

### Cache System

```
AsyncCache[K, V] (Protocol, runtime_checkable)
  └── Methods: get, set, delete, clear, has, get_many, set_many

CacheBackend[K, V] (ABC, extends AsyncCache)
  ├── Abstract: _get(key), _set(key, value), _delete(key), _clear()
  ├── Concrete: InMemoryCache, NoOpCache, RedisCache (varco_redis), LayeredCache
  └── Lifecycle: __aenter__, __aexit__, start(), stop()

InvalidationStrategy (ABC)
  ├── Concrete: TTLStrategy, ExplicitStrategy, TaggedStrategy
  │             EventDrivenStrategy, CompositeStrategy
  └── Lifecycle: start(), stop() called by hosting backend
  └── Rule: Never instantiate outside backend lifecycle — may hold subscriptions
```

### Repository & UnitOfWork

```
AsyncRepository[D, PK] (Protocol, runtime_checkable)
  └── Methods: save, get, delete, filter, find_one, find_many, count, exists

AsyncUnitOfWork (Protocol, runtime_checkable)
  └── Methods: begin, commit, rollback
  └── Pattern: async with uow: ... (context manager)

IUoWProvider (Protocol)
  └── get_uow() → AsyncUnitOfWork
```

### Query System

```
QueryParams (dataclass)
  ├── filters: list[FilterSpec]        (string-encoded like "age__gte=18")
  ├── sort: list[SortSpec]             ("+name", "-created_at")
  ├── limit: int, offset: int

QueryParser → FilterNode AST
  ├── ComparisonNode(field, op, value)
  ├── AndNode(left, right)
  ├── OrNode(left, right)
  └── NotNode(operand)
  └── All frozen dataclasses (immutable, hashable, cacheable)

ASTVisitor (ABC, visitor pattern)
  └── visit(node: FilterNode) → backend-specific (WHERE clause, etc.)

Concrete visitors:
  ├── SQLAlchemyFilterVisitor      (varco_core.query.applicator.sqlalchemy)
  ├── QueryOptimizer               (constant-folding, dead-branch elimination)
  └── TypeCoercionVisitor          (coerce string scalars to field types)

QueryTransformer (wiring)
  └── parse(params) → visit(ast) → apply(backend_query)
```

### Authority / JWT

```
JwtAuthority
  ├── from_pem(pem_bytes, kid, issuer, algorithm)
  ├── sign(claims: JwtBuilder) → str
  └── verify(token: str) → JwtPayload

MultiKeyAuthority (rotation)
  ├── rotate(new_authority: JwtAuthority)
  ├── retire(kid: str)
  └── sign/verify delegate to active authority

TrustedIssuerRegistry (multi-issuer verification)
  ├── from_env()
  ├── await load_all()
  └── await verify(raw_token) → JwtPayload

KeySource (ABC)
  ├── PemFile(path)
  ├── PemFolder(path)
  ├── JwksUrl(url)
  └── OidcDiscovery(issuer_url)
```

### Resilience

```
@timeout(seconds: float)                        — async only, raises asyncio.TimeoutError
@retry(policy: RetryPolicy)                     — sync or async, exponential backoff
@circuit_breaker(config: CircuitBreakerConfig)  — sync or async, failure threshold + half-open state

CircuitBreaker (shared instance pattern)
  └── Rule: one per external dependency, not per-call — must accumulate failures
  └── Methods: protect(fn), state property (CLOSED/OPEN/HALF_OPEN)

Built into @listen:
  └── @listen(..., retry_policy=..., dlq=...) → wrapper built at register_to() time
```

### Outbox Pattern

```
OutboxEntry (frozen dataclass)
  ├── event_type: str
  ├── event_id: str
  ├── aggregate_id: str
  ├── serialized: bytes
  └── created_at: datetime

OutboxRepository (ABC)
  ├── save_outbox(entry: OutboxEntry)
  ├── get_pending(limit: int) → list[OutboxEntry]
  └── delete(entry_id: str)

OutboxRelay (background task)
  ├── poll loop: get_pending() → publish() → delete()
  └── Rule: only place allowed to call AbstractEventBus directly (besides register_to)
  └── Contract: push() to DLQ must never raise — logs errors and swallows
```

---

## Design Patterns by Module

### Event Wiring (The "Register-to" Pattern)

```python
# ❌ WRONG: Subscribe at __init__ time
class OrderConsumer(EventConsumer):
    def __init__(self, bus: AbstractEventBus):
        self._bus = bus
        self._bus.subscribe(OrderPlacedEvent, self.on_order)  # ← too early!

    async def on_order(self, event: OrderPlacedEvent): ...

# ✅ CORRECT: Metadata at class-definition time, subscribe at @PostConstruct
class OrderConsumer(EventConsumer):
    def __init__(self, bus: AbstractEventBus):
        self._bus = bus

    @PostConstruct
    def _setup(self) -> None:
        self.register_to(self._bus)  # ← called once, after DI wiring

    @listen(OrderPlacedEvent, channel="orders")
    async def on_order(self, event: OrderPlacedEvent) -> None: ...
```

**Why**: `@listen` stores metadata on the function object at class-definition time. `register_to()` reads that metadata and creates the subscription. This split makes the consumer testable (can mock the bus) and bus-agnostic (same consumer works with in-memory, Kafka, Redis).

### Service Layer Composition (MRO Chains)

```python
class UserService(
    CacheServiceMixin,
    TenantAwareService,
    ValidatorServiceMixin,
    AsyncService[User, UUID, UserCreateDTO, UserReadDTO, UserUpdateDTO],
):
    def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[User, UUID]:
        return uow.get_repository(User)

# Method resolution order (MRO):
# UserService → CacheServiceMixin → TenantAwareService → ValidatorServiceMixin
#   → AsyncService → ...

# Calling create(data):
# 1. CacheServiceMixin.__create__  (if caching enabled)
# 2. TenantAwareService._scoped_params (inject tenant_id)
# 3. ValidatorServiceMixin._check_entity (call @validate decorators)
# 4. AsyncService.create (core logic)
```

**Rule**: Each hook must call `super()` so the next mixin in the chain runs. Order matters (left-to-right in class inheritance).

### Cache Invalidation Strategies

```python
# TTL: evict after 60 seconds
cache = await InMemoryCache(
    invalidation_strategy=TTLStrategy(ttl_seconds=60)
).__aenter__()

# Tag-based: invalidate by tag
cache = await InMemoryCache(
    invalidation_strategy=TaggedStrategy()
).__aenter__()
# cache.set(key, value, tags=["user:123", "order:456"])
# cache.invalidate_by_tag("user:123")  # evicts all entries with that tag

# Event-driven: invalidate on domain events
cache = await InMemoryCache(
    invalidation_strategy=EventDrivenStrategy(
        bus=event_bus,
        mappings={UserCreatedEvent: ["user:*"]},  # wildcard patterns
    )
).__aenter__()

# Composite: combine multiple strategies
cache = await InMemoryCache(
    invalidation_strategy=CompositeStrategy([
        TTLStrategy(ttl_seconds=300),
        TaggedStrategy(),
        EventDrivenStrategy(bus, mappings),
    ])
).__aenter__()
```

### DI Wiring

```python
# Container setup
container = DIContainer()

# Install backend configurations
await container.ainstall(KafkaEventBusConfiguration)  # async setup
container.install(SAConfiguration)                    # sync setup
container.install(RedisCacheConfiguration)

# Bind repositories (auto-derived from DomainModel fields)
bind_repositories(container, User, Order, Product)

# Resolve: DI knows concrete types, app code injects protocols
user_service: AsyncService[User, UUID, ...] = container.resolve(UserService)
event_bus: AbstractEventBus = container.resolve(AbstractEventBus)
```

---

## File Organization

### varco_core Submodules

| Module | Purpose | Key Classes |
|--------|---------|------------|
| `event/` | Event bus, producer, consumer, serialization | `AbstractEventBus`, `AbstractEventProducer`, `EventConsumer`, `@listen` |
| `service/` | Domain service layer, mixins, outbox | `AsyncService`, `ValidatorServiceMixin`, `CacheServiceMixin`, `OutboxRelay` |
| `cache/` | Cache abstraction, backends, invalidation | `AsyncCache`, `CacheBackend`, `InvalidationStrategy`, `@cached` decorator |
| `query/` | Query AST, parser, visitors, transformers | `QueryParams`, `FilterNode`, `ASTVisitor`, `QueryTransformer` |
| `resilience/` | Retry, timeout, circuit breaker | `@retry`, `@timeout`, `@circuit_breaker` |
| `authority/` | JWT signing, verification, key rotation | `JwtAuthority`, `TrustedIssuerRegistry`, `MultiKeyAuthority` |
| `auth/` | User/role/permission abstractions | `AbstractAuthorizer`, permission models |
| `repository.py` | Repository protocol | `AsyncRepository[D, PK]` |
| `uow.py` | Unit of work protocol | `AsyncUnitOfWork`, `IUoWProvider` |
| `model.py` | Domain model base | `DomainModel` |
| `dto/` | Data transfer objects | `DTOBase`, `DTOFactory`, `Page` |
| `meta.py` | Field metadata decorators | `FieldHint`, `ForeignKey`, `PrimaryKey` |
| `exception/` | Exception hierarchy | `RepositoryException`, `ServiceException`, `QueryException` |
| `providers.py` | DI container | `DIContainer` |

---

## Common Workflows

### Adding a New Service with Full Stack

1. **Define domain model** (`varco_core.model.DomainModel`)
   - Annotate fields with types, use `@FieldHint`, `@PrimaryKey`, `@ForeignKey` for metadata

2. **Define DTOs** (`varco_core.dto.DTOBase`)
   - `CreateDTO`, `ReadDTO`, `UpdateDTO` subclasses

3. **Implement service** (`varco_core.service.AsyncService`)
   - Implement `_get_repo()` method
   - Mix in `CacheServiceMixin`, `TenantAwareService`, `ValidatorServiceMixin` as needed

4. **Implement event consumer** (`varco_core.event.EventConsumer`)
   - Decorate handlers with `@listen(EventType, channel="name")`
   - Call `register_to(bus)` in `@PostConstruct` method

5. **Bind in DI** (in your app's DI setup)
   - `bind_repositories(container, DomainModel)`
   - `container.resolve(YourService)`

### Publishing Events Safely

```python
# ❌ WRONG: publishes after DB commit, broker failure silently drops event
async with uow:
    user = await repo.save(User(...))
# ← DB committed here
await producer.produce(UserCreatedEvent(user.id))  # ← can fail!

# ✅ CORRECT: persists event in same DB transaction
async with uow:
    user = await repo.save(User(...))
    await outbox_repo.save_outbox(OutboxEntry.from_event(
        UserCreatedEvent(user.id)
    ))
# ← DB committed with both user and outbox entry
# OutboxRelay polls and publishes asynchronously
```

### Using the Query System

```python
from varco_core.query import QueryParams, QueryTransformer

# Client sends filter/sort as strings (e.g., HTTP query params)
params = QueryParams(
    filters=["age__gte=18", "status__eq=active"],
    sort=["+created_at"],
    limit=20,
    offset=0,
)

# Transform into backend query
transformer = QueryTransformer()
filtered_query = transformer.transform(base_query, params, User)
# ← base_query is now: base_query.where(...).order_by(...).limit(...).offset(...)
```

---

## Integration Points & Backend-Specific Implementations

### varco_sa (SQLAlchemy)

- **ORM generation**: `SAModelFactory` reads `DomainModel.fields` and creates SQLAlchemy models at import
- **Repository impl**: Standard async SQLAlchemy queries
- **Outbox impl**: SQL table-based `OutboxRepository`
- **Query applicator**: `SQLAlchemyFilterVisitor` converts AST → WHERE clause
- **DI**: `SAConfiguration` with engine, declarative base, entity classes

### varco_kafka (Kafka)

- **Bus impl**: `KafkaEventBus` — uses `aiokafka.AIOKafkaProducer` / `AIOKafkaConsumer`
- **Channel routing**: Topic names from `@listen(event_type, channel="orders")` → Kafka topic
- **DLQ impl**: Dedicated Kafka topic for dead letters
- **Config**: `KafkaConfig` with broker addresses, consumer group, etc.

### varco_redis (Redis)

- **Bus impl**: `RedisEventBus` — uses Redis Pub/Sub or Streams
- **Cache impl**: `RedisCache` — async redis.asyncio, lazy connection pooling
- **Channel routing**: Redis pubsub channels or streams
- **DLQ impl**: Dedicated Redis stream for dead letters
- **Invalidation**: `EventDrivenStrategy` can subscribe to events and invalidate cache keys
- **Config**: `RedisConfig` with host/port; `CacheConfig` with TTL, strategy

---

## Anti-Patterns to Avoid

| Anti-Pattern | Why It's Wrong | Fix |
|---|---|---|
| Service calls `AbstractEventBus` directly | Only producer/consumer/outbox should touch the bus | Inject `AbstractEventProducer` |
| Publishes events after DB commit | Broker failure silently loses events | Use `OutboxRepository` + `OutboxRelay` |
| Instantiates `InvalidationStrategy` outside cache lifecycle | May hold subscriptions/background tasks | Let `CacheBackend` manage it via `start()`/`stop()` |
| Per-call `CircuitBreaker` instances | Never accumulates failures, so circuit never opens | Use shared instance per external dependency |
| Subscribes to events in `__init__` | Blocks service instantiation, makes testing hard | Defer to `@PostConstruct` + `register_to()` |
| Mixin hook doesn't call `super()` | Breaks the MRO chain, later mixins never run | Always chain with `return await super()._hook_name(...)` |

