# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**Quick reference**: See [ARCHITECTURE.md](ARCHITECTURE.md) for a complete technical map of all packages, modules, classes, type hierarchies, and design patterns. Use it to navigate the codebase efficiently without reading files one-by-one.

---

## Commands

All commands run from the **workspace root** (`/home/edoardo/projects/varco`) using a single shared virtual environment managed by `uv`.

```bash
# Install everything (all workspace members + dev deps)
uv sync

# Run all tests for one package
uv run pytest varco_core/tests/
uv run pytest varco_kafka/tests/
uv run pytest varco_redis/tests/
uv run pytest varco_sa/tests/

# Run a single test file
uv run pytest varco_core/tests/test_event.py

# Run a single test by name
uv run pytest varco_core/tests/test_event.py::TestInMemoryEventBus::test_subscribe

# Run integration tests (require Docker — Kafka, Redis, or MongoDB broker)
uv run pytest varco_kafka/tests/ -m integration
uv run pytest varco_redis/tests/ -m integration

# Import any workspace package directly (no install step needed)
uv run python -c "from varco_core.event import AbstractEventBus"
```

There is no lint command configured. There is no type-check command configured. `pytest-asyncio` is installed with `asyncio_mode = "auto"` in every package — all `async def test_*` functions run automatically without `@pytest.mark.asyncio`.

---

## Architecture

Varco is a **uv workspace monorepo** of five packages. Each package is independently installable from PyPI. `varco_core` has no sibling dependencies; all other packages depend on it.

```
varco_core        — domain model, service layer, event system, resilience, DI contracts
varco_kafka       — Kafka event bus backend (aiokafka)
varco_redis       — Redis Pub/Sub event bus + cache backend (redis.asyncio)
varco_sa          — SQLAlchemy async ORM backend
varco_beanie      — Beanie/MongoDB async ODM backend
```

### Dependency graph

```
varco_kafka  ──┐
varco_redis  ──┤─→ varco_core
varco_sa     ──┤
varco_beanie ──┘
```

`varco_core` is the only package without a `[tool.uv.sources]` sibling reference. All backend packages resolve it from the workspace rather than PyPI during development.

---

## Key Abstractions and Layer Rules

### Event system (varco_core.event)

Three concentric layers — never skip a layer:

```
User code (services, handlers)
  ↓ may use
AbstractEventProducer   — publish side; services depend on THIS, not the bus
EventConsumer + @listen — consume side; methods decorated at class-definition time
  ↓ both delegate to
AbstractEventBus        — low-level interface; only producers/consumers touch it
  ↓ implemented by
InMemoryEventBus        — for tests
KafkaEventBus           — varco_kafka
RedisEventBus           — varco_redis
```

**Rule**: services must never hold or call `AbstractEventBus` directly. They inject `AbstractEventProducer` and call `_produce()` / `_produce_many()`. The only accepted exceptions are `OutboxRelay` (infrastructure) and `EventConsumer.register_to()` (wiring-time only).

**`@listen` is declarative / `register_to` is imperative.** The decorator stores metadata on the function object at class-definition time. No subscription is created until `consumer.register_to(bus)` is called (typically in a `@PostConstruct` method). This separation makes the consumer bus-agnostic and testable.

```python
# Correct wiring pattern
class OrderConsumer(EventConsumer):
    @PostConstruct
    def _setup(self) -> None:
        self.register_to(self._bus)   # wiring happens here, not in @listen

    @listen(OrderPlacedEvent, channel="orders")
    async def on_order(self, event: OrderPlacedEvent) -> None: ...
```

### Service layer (varco_core.service)

`AsyncService[D, PK, C, R, U]` is generic over five type parameters:
- `D` — DomainModel subclass
- `PK` — primary key type
- `C` / `R` / `U` — Create / Read / Update DTO subclasses

Concrete subclasses implement exactly one abstract method:
```python
def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[D, PK]: ...
```

Three optional hooks chain via `super()` for mixin composition: `_scoped_params`, `_check_entity`, `_prepare_for_create`. Authorization is enforced at the service layer (not HTTP), via the injected `AbstractAuthorizer`.

**Mixin composition pattern** — `ValidatorServiceMixin`, `TenantAwareService`, `SoftDeleteService`, `EventConsumer` all compose via MRO. Chain hooks with `super()` so every mixin in the chain runs.

### DI wiring (providify)

Every backend package ships a `di.py` with a `@Configuration` class. Wire backends into a `DIContainer` using `container.install()` or `await container.ainstall()`. The DI module is the only place that knows concrete types — application code always injects interfaces (`AbstractEventBus`, `AsyncRepository[D]`, `IUoWProvider`).

```python
# Typical app bootstrap
container = DIContainer()
await container.ainstall(KafkaEventBusConfiguration)
container.install(SAModule)
bind_repositories(container, User, Post)
```

### Resilience (varco_core.resilience)

Three standalone decorators composable with any callable:

```python
@timeout(10.0)                                         # async only
@retry(RetryPolicy(max_attempts=3, base_delay=0.5))   # sync or async
@circuit_breaker(CircuitBreakerConfig(failure_threshold=5))
async def call_external() -> Response: ...
```

**`CircuitBreaker` must be a shared instance per external dependency** — a per-call instance will never accumulate enough failures to open. Use `@circuit_breaker(config)` for per-function breakers, or `breaker.protect(fn)` for a shared breaker across multiple functions.

`@retry` is also integrated into `@listen` via `retry_policy=` and `dlq=` parameters. The wrapper is built at `register_to()` time (not decoration time) so the resolved channel string and bound `self` are available.

### Dead Letter Queue (varco_core.event.dlq)

`AbstractDeadLetterQueue` is the interface. `InMemoryDeadLetterQueue` is for tests. Backend implementations (`KafkaDLQ`, `RedisDLQ` in their respective packages) push to a dedicated topic/channel.

**Contract**: `push()` must never raise — the retry wrapper in `_make_retry_wrapper` cannot recover from DLQ failures. Implementations must log errors and swallow them.

```python
# Handler that retries 3x then routes to DLQ
@listen(
    OrderPlacedEvent,
    channel="orders",
    retry_policy=RetryPolicy(max_attempts=3, base_delay=1.0),
    dlq=my_dlq,
)
async def on_order(self, event: OrderPlacedEvent) -> None: ...
```

### SQLAlchemy backend (varco_sa)

`varco_sa` generates SQLAlchemy ORM models at import time from `DomainModel` subclasses via `SAModelFactory`. Models are never declared manually — they are derived from the domain model's field annotations and `FieldHint` / `ForeignKey` / `PrimaryKey` metadata decorators in `varco_core.meta`.

The `SAConfig` object (engine + declarative base + entity classes) is the single injectable configuration object — it doubles as the DI settings object, avoiding a parallel `SASettings` class.

### Cache system (varco_core.cache)

`AsyncCache[K, V]` is a `runtime_checkable` Protocol; `CacheBackend[K, V]` is the ABC backends subclass (adds `start()`/`stop()` lifecycle). Hierarchy:

```
AsyncCache (Protocol)  ←  structural checks, type hints
  ↑
CacheBackend (ABC)     ←  inherit start/stop + async context manager
  ↑
InMemoryCache  NoOpCache  RedisCache (varco_redis)  LayeredCache
```

`InvalidationStrategy` is a separate ABC — every strategy (TTL, tag-based, event-driven, composite) implements `start()`/`stop()` called by the hosting backend. Use `CompositeStrategy` to combine strategies. The `@cached` decorator (`varco_core.cache.decorator`) wraps any async callable; `CacheServiceMixin` adds caching to service layer methods.

**Rule**: never instantiate `InvalidationStrategy` outside its backend's `start()`/`stop()` lifecycle — it may hold subscriptions or background tasks.

### Query system (varco_core.query)

The query system builds a typed AST over filter/sort/pagination parameters and applies it to backends:

```
QueryParams (HTTP layer input)
  ↓ parsed by
QueryParser          → FilterNode AST (ComparisonNode / AndNode / OrNode / NotNode)
  ↓ visited by
ASTVisitor           → e.g. SQLAlchemyFilterVisitor → WHERE clause
QueryOptimizer       → constant-folding / dead-branch elimination
TypeCoercionVisitor  → coerce string scalars to annotated field types
  ↓ applied by
QueryApplicator      → attaches filter + sort + pagination to a backend query
```

All AST nodes are `@dataclass(frozen=True)` — immutable, hashable, safe to cache. `QueryTransformer` wires the full pipeline in one call. The SQLAlchemy applicator lives in `varco_core.query.applicator.sqlalchemy` (not in `varco_sa`) so the query system stays backend-agnostic.

### Transactional Outbox (varco_core.service.outbox)

Services must **not** publish events directly after a DB commit — a broker failure will silently drop the event. Use the outbox pattern instead:

1. Within the DB transaction, save the event as an `OutboxEntry` via `OutboxRepository`.
2. A background `OutboxRelay` polls pending entries, publishes to the bus, and deletes on success.

```python
# Correct: event is persisted in the same transaction as the domain entity
async with uow:
    await repo.save(entity)
    await outbox_repo.save_outbox(OutboxEntry.from_event(event))
# OutboxRelay publishes asynchronously — delivery is guaranteed even on broker restart
```

`OutboxRepository` is an ABC; `varco_sa` and `varco_beanie` each ship a concrete implementation. `OutboxRelay` is the only place allowed to call `AbstractEventBus` directly (besides `EventConsumer.register_to()`).

### Authority / JWT system (varco_core.authority)

`JwtAuthority` signs tokens with a private key; `TrustedIssuerRegistry` verifies tokens from multiple trusted issuers. Key rotation is zero-downtime via `MultiKeyAuthority`:

```python
# Signing
authority = JwtAuthority.from_pem(pem_bytes, kid="svc:A", issuer="my-svc", algorithm="RS256")
token = authority.sign(authority.token().subject("usr_1").expires_in(timedelta(hours=1)))

# Rotation
multi = MultiKeyAuthority(authority)
multi.rotate(JwtAuthority.from_pem(new_pem, kid="svc:B", ...))
multi.retire("svc:A")   # only after all tokens signed with svc:A have expired

# Verification (multi-issuer)
registry = TrustedIssuerRegistry.from_env()
await registry.load_all()
payload = await registry.verify(raw_token)
```

Key sources (`varco_core.authority.sources`): `PemFile`, `PemFolder`, `JwksUrl`, `OidcDiscovery`. `TrustedIssuerRegistry.from_env()` reads issuer config from environment variables.

---

## Planning & Development Workflows

### Before Adding a Feature

1. **Check ARCHITECTURE.md type hierarchies** — Find existing abstractions that apply. For example:
   - Adding authentication? → Look at `authority/jwt_authority.py` and `TrustedIssuerRegistry`
   - Adding caching? → Extend `CacheBackend` and pick an `InvalidationStrategy`
   - Adding event handling? → Extend `EventConsumer`, use `@listen`, wire with `register_to()`

2. **Check if a backend implementation already exists** — Don't implement the same interface twice:
   - Event bus? → Kafka and Redis backends exist; add a new one only if truly needed
   - Cache? → In-memory, Redis, and layered exist
   - ORM? → SQLAlchemy and Beanie exist
   - Query filtering? → AST + visitor pattern handles this; extend `ASTVisitor` if needed

3. **Identify the layer boundary** — Where does this feature live?
   - Protocol/ABC in `varco_core`? → Used by app code
   - Concrete impl in a backend (`varco_kafka`, `varco_redis`, `varco_sa`)? → Backend-specific
   - Service mixin? → If it composes via MRO with other mixins

### Common Scenarios

#### Scenario: Add a new event type and handler

```python
# 1. Define the event in varco_core (domain layer)
class OrderShippedEvent(DomainEvent):
    order_id: UUID
    shipped_at: datetime

# 2. Emit from service (via producer, not bus directly)
class OrderService(AsyncService[Order, UUID, ...]):
    async def ship_order(self, order_id: UUID) -> None:
        async with self._uow_provider.get_uow() as uow:
            order = await repo.get(order_id)
            order.status = "shipped"
            await repo.save(order)
            await self._producer.produce(OrderShippedEvent(
                order_id=order_id,
                shipped_at=datetime.now(UTC),
            ))

# 3. Handle in a consumer (EventConsumer subclass)
class NotificationConsumer(EventConsumer):
    def __init__(self, bus: AbstractEventBus, mailer: Mailer):
        self._bus = bus
        self._mailer = mailer

    @PostConstruct
    def _setup(self) -> None:
        self.register_to(self._bus)

    @listen(OrderShippedEvent, channel="orders")
    async def on_order_shipped(self, event: OrderShippedEvent) -> None:
        await self._mailer.send(f"Order {event.order_id} shipped!")

# 4. Wire in DI
container = DIContainer()
await container.ainstall(KafkaEventBusConfiguration)
container.install(NotificationConsumerModule)
```

**Caveat**: If the handler can fail (e.g., email send timeout), add `retry_policy` and `dlq`:

```python
@listen(
    OrderShippedEvent,
    channel="orders",
    retry_policy=RetryPolicy(max_attempts=3, base_delay=1.0),
    dlq=my_dlq,  # routes to DLQ after 3 failures
)
async def on_order_shipped(self, event: OrderShippedEvent) -> None: ...
```

#### Scenario: Add caching to a service method

```python
# 1. Choose invalidation strategy
from varco_core.cache import TTLStrategy, TaggedStrategy, CompositeStrategy

# 2. Mix in CacheServiceMixin (order matters in MRO!)
class UserService(
    CacheServiceMixin,          # ← LEFT side (runs first)
    TenantAwareService,
    AsyncService[User, UUID, UserCreateDTO, UserReadDTO, UserUpdateDTO],
):
    _cache_config = CacheConfig(
        backend=RedisCache(...),
        invalidation_strategy=CompositeStrategy([
            TTLStrategy(ttl_seconds=300),
            TaggedStrategy(),
        ]),
    )

    def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[User, UUID]:
        return uow.get_repository(User)

# 3. Use @cached on methods (works with any async callable)
from varco_core.cache import cached

@cached(key_fn=lambda self, user_id: f"user:{user_id}")
async def get_user_profile(self, user_id: UUID) -> UserProfile:
    # This is cached; invalidation strategy handles eviction
    return await self.read(user_id)

# 4. Invalidate explicitly when needed
@listen(UserUpdatedEvent, channel="users")
async def on_user_updated(self, event: UserUpdatedEvent) -> None:
    await self._cache.invalidate_by_tag(f"user:{event.user_id}")
```

**Caveat**: Cache invalidation is hard. Order strategies by most-to-least-aggressive:
- `EventDrivenStrategy` (immediate, requires event emission)
- `TaggedStrategy` (explicit, requires you to call invalidate)
- `TTLStrategy` (eventual, stale reads until expiry)

#### Scenario: Add filtering to a list endpoint

```python
# 1. HTTP layer receives filter strings (e.g., ?age__gte=18&status__eq=active)
from varco_core.query import QueryParams

params = QueryParams(
    filters=request.query_params.getlist("filter"),  # ["age__gte=18", "status__eq=active"]
    sort=request.query_params.getlist("sort"),       # ["+created_at"]
    limit=int(request.query_params.get("limit", 50)),
    offset=int(request.query_params.get("offset", 0)),
)

# 2. Pass to service (uses QueryTransformer internally)
page = await user_service.list(params, tenant_id=current_user.tenant_id)

# 3. Service.list() handles:
#    - Parse filters → AST (ComparisonNode, AndNode, OrNode, NotNode)
#    - Type coercion (string "18" → int 18)
#    - Optimize (constant folding, dead branches)
#    - Apply to backend query (SQLAlchemy: WHERE clause)
```

**Caveat**: Filter operators are backend-agnostic AST. If adding a new comparison operator:
- Update `QueryParser` to recognize it (e.g., `"__between"`)
- Extend `ASTVisitor` subclasses to handle it (e.g., `SQLAlchemyFilterVisitor`)
- Test on every backend (SQLAlchemy, Beanie, etc.)

#### Scenario: Integrate a new external API (with resilience)

```python
from varco_core.resilience import retry, timeout, circuit_breaker, RetryPolicy, CircuitBreakerConfig

class PaymentService:
    def __init__(self, http_client: httpx.AsyncClient):
        self._client = http_client
        # Shared breaker per external service (NOT per-call)
        self._breaker = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=5, recovery_timeout=60.0)
        )

    @timeout(10.0)  # Fail fast if API hangs
    @retry(RetryPolicy(max_attempts=3, base_delay=0.5, max_delay=5.0))
    @circuit_breaker(config=...)  # Or use self._breaker.protect(fn)
    async def charge_card(self, amount: float, card_token: str) -> TransactionId:
        response = await self._client.post(
            "https://payment-api.example.com/charge",
            json={"amount": amount, "token": card_token},
        )
        if response.status_code >= 500:
            raise ExternalServiceError("Payment API down")
        return TransactionId(response.json()["id"])

# Decorator order matters (bottom-to-top execution):
# 1. circuit_breaker checks state
# 2. retry wraps and retries on failure
# 3. timeout cancels if > 10s
```

**Caveat**: `CircuitBreaker` must be **shared** per external service. A per-call instance never accumulates failures:

```python
# ❌ WRONG: New breaker each call
def charge(self):
    breaker = CircuitBreaker(config)  # Fresh instance!
    return breaker.protect(self._call_payment_api)()

# ✅ CORRECT: Shared breaker
self._breaker = CircuitBreaker(config)  # Once at __init__

async def charge(self):
    return await self._breaker.protect(self._call_payment_api)()
```

---

## Coding Standards

All code in this repo follows the **coding-practice** skill. Key non-obvious rules specific to this codebase:

- `from __future__ import annotations` at the top of every file.
- `asyncio.Lock` is always created **lazily** (never at module level or `__init__`) — locks must be created inside a running event loop.
- Frozen `@dataclass(frozen=True)` for all value objects and config. Mutable dataclasses are a red flag.
- `TYPE_CHECKING` guards for cross-package type hints that would create circular imports at runtime (e.g. `consumer.py` importing from `dlq.py`).
- Every design decision gets a `DESIGN:` block with `✅` benefits and `❌` drawbacks.
- Docstrings include `Args:`, `Returns:`, `Raises:`, `Edge cases:`, `Thread safety:` / `Async safety:` where relevant.

---

## Test Conventions

- All tests are `async def` — no `@pytest.mark.asyncio` needed (auto mode).
- Integration tests require a real broker via Docker and are tagged `@pytest.mark.integration`. They are skipped by default; run with `-m integration`.
- `InMemoryEventBus` is the standard bus for unit tests. Use `bus.drain()` after publishes when `DispatchMode.BACKGROUND` is active.
- `InMemoryDeadLetterQueue` is the standard DLQ for unit tests.
- Two pre-existing test failures exist and are unrelated to any current work: `test_cache.py::TestTTLStrategy::test_cache_evicts_expired_on_read` (timing flake) and `test_event.py::TestJsonEventSerializer::test_serialize_produces_bytes` (API mismatch).

---

## Common Pitfalls & How to Avoid Them

| Pitfall | Symptom | Root Cause | Fix |
|---------|---------|-----------|-----|
| **Direct bus access in service** | Service holds `AbstractEventBus` and calls `bus.publish()` | Violates layer rule; bus is infra, not for app logic | Always inject `AbstractEventProducer`; it abstracts the bus |
| **Events published after commit** | Events silently lost when broker is unavailable | Post-commit publish has no rollback path | Use `OutboxRepository` + `OutboxRelay` within same transaction |
| **Subscription in `__init__`** | Service won't instantiate if broker is down; hard to test | Coupling service creation to bus state | Defer to `@PostConstruct` + `register_to()` |
| **Per-call CircuitBreaker** | Circuit never opens, all requests fail after threshold | Instance never accumulates enough failures | Use shared `CircuitBreaker` per external dependency |
| **Mixin hook doesn't chain** | Later mixins in MRO never run | Hook returns value without calling `super()` | Always `return await super()._hook_name(...)` |
| **Instantiate InvalidationStrategy outside lifecycle** | Consumer crashes on `start()` or subscriptions leak | Strategy may spawn background tasks or hold subscriptions | Let `CacheBackend` create it; call `backend.start()` / `stop()` |
| **Cache key collision** | Wrong data returned to different users | Key function doesn't include scope (tenant_id, user_id) | Use namespaced keys: `f"user:{tenant_id}:{user_id}"` |
| **Forgot `@PostConstruct` on consumer** | Events never delivered | `register_to()` never called; subscription created at wrong time | Add `@PostConstruct` method that calls `self.register_to(self._bus)` |
| **Async lock at module level** | `RuntimeError: no running event loop` | Locks created before event loop starts | Create locks lazily inside methods: `self._lock = self._lock or asyncio.Lock()` |
| **Missing `await` on async call** | Coroutine leaked, cleanup never runs | Easy to miss on unfamiliar APIs | IDE linting catches this; always `await` calls to `async def` |

---

## Decision Tree: What to Implement Where?

```
Am I adding a new capability?
├─ Event system feature (new event type, new consumer pattern)?
│  └─ → varco_core.event (protocol) + varco_kafka/redis (backend)
│
├─ Cache feature (new invalidation strategy, new backend)?
│  └─ → varco_core.cache (ABC) + varco_redis/sa (impl)
│
├─ Query filtering (new comparison operator, new visitor)?
│  └─ → varco_core.query (parser + visitor) + varco_core.query.applicator.sqlalchemy
│
├─ Resilience pattern (new retry/timeout/breaker variant)?
│  └─ → varco_core.resilience (decorator + config)
│
├─ Authentication/JWT feature?
│  └─ → varco_core.authority (protocol) + varco_core.authority.sources (key sources)
│
├─ Service layer feature (mixin, hook, outbox)?
│  └─ → varco_core.service (ABC + mixin) + varco_sa/beanie (repository impl)
│
└─ ORM/database feature?
   └─ → varco_sa (SQLAlchemy) and/or varco_beanie (MongoDB)
        ↳ Models auto-generated from varco_core.model.DomainModel
        ↳ Implement backend-specific Repository, OutboxRepository

---

Should I create a new backend implementation?
├─ Only if you're supporting a genuinely different transport/storage:
│  ├─ New event bus (e.g., RabbitMQ, AWS SNS)? → new package varco_[backend]
│  ├─ New cache backend (e.g., Memcached)? → add to varco_redis or new package
│  ├─ New ORM (e.g., Tortoise)? → new package varco_[backend]
│  └─ New DLQ (e.g., S3-based dead letters)? → new package or existing
│
└─ Do NOT create a new backend just for:
   ├─ A different config (use the existing backend with new settings)
   ├─ A new feature (extend the existing backend's interface)
   └─ Convenience (keep it simple; fewer backends = fewer bugs)

---

Should I add it to varco_core or a backend?
├─ varco_core if:
│  ├─ ✅ It's a protocol/ABC that backends implement
│  ├─ ✅ It's used by application code (services, handlers)
│  ├─ ✅ It's transport/storage agnostic (event types, domain model)
│  └─ ✅ All backends need it (caching, query, resilience)
│
└─ Backend (varco_kafka/redis/sa/beanie) if:
   ├─ ✅ It's a concrete implementation of a varco_core interface
   ├─ ✅ It depends on third-party libraries specific to that backend (aiokafka, redis, sqlalchemy)
   └─ ✅ It only makes sense for one transport/storage system
```

---

## Pre-Implementation Checklist

Before writing code, ask yourself:

- [ ] **Is this already implemented elsewhere?** → Search ARCHITECTURE.md type hierarchies, check `varco_*/` for similar patterns
- [ ] **Does this belong in varco_core or a backend?** → Use decision tree above
- [ ] **Am I respecting layer boundaries?** → Services inject protocols, not concrete implementations; only DI knows concrete types
- [ ] **Will this compose via MRO if it's a mixin?** → Does it call `super()` on every hook?
- [ ] **Is my event consumer testable?** → Decorated with `@listen`, wired in `@PostConstruct`, no bus reference in `__init__`?
- [ ] **If I'm publishing events, am I using the outbox pattern?** → Events saved in same DB transaction, relayed asynchronously?
- [ ] **If I'm caching, is my key namespaced?** → Includes tenant_id, user_id, or other scope identifier?
- [ ] **If I'm using external APIs, do I have resilience?** → Timeout + retry + circuit breaker, with shared breaker instance?
- [ ] **Are my dataclasses frozen?** → `@dataclass(frozen=True)` for value objects, configs, AST nodes?
- [ ] **Am I creating locks lazily?** → Never at module level or `__init__`, always inside methods?
- [ ] **Did I add docstrings with Args/Returns/Raises/Edge cases?** → Especially for new abstractions and non-obvious code
- [ ] **Did I test with the right bus?** → `InMemoryEventBus` for unit tests, real broker (Docker) for integration tests?
