# Changelog

All notable changes to the varco framework are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Varco packages use [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] — 2026-04-07

First public alpha release of the varco framework. All eight packages are published
to PyPI simultaneously. This release establishes the public API surface — expect
breaking changes between alpha versions while the API stabilises.

### New packages

| Package | Version | Description |
|---------|---------|-------------|
| `varco-core` | 0.1.0 | Domain model, service layer, event system, resilience, query AST, JWT authority |
| `varco-kafka` | 0.1.0 | Apache Kafka event bus backend (aiokafka) |
| `varco-redis` | 0.1.0 | Redis Pub/Sub event bus, cache, DLQ, rate limiter (redis.asyncio) |
| `varco-sa` | 0.1.0 | SQLAlchemy async ORM backend with auto-generated models |
| `varco-beanie` | 0.1.0 | Beanie/MongoDB async ODM backend |
| `varco-ws` | 0.1.0 | WebSocket and SSE event bus implementations |
| `varco-fastapi` | 0.1.0 | FastAPI adapter — routing mixins, auth middleware, typed HTTP client, DI wiring |
| `varco-memcached` | 0.1.0 | Memcached cache backend |

---

### varco-core

#### Added
- **`AsyncService`** — generic service base class over five type parameters
  (`DomainModel`, primary key, Create/Read/Update DTOs). Implements full CRUD
  with `_get_repo()` as the single required hook.
- **`AbstractEventBus` / `AbstractEventProducer` / `EventConsumer`** — layered
  event system. `@listen` stores metadata declaratively; `register_to()` wires
  subscriptions imperatively at startup.
- **`InMemoryEventBus`** — zero-dependency event bus for unit tests.
- **`AbstractDeadLetterQueue` / `InMemoryDeadLetterQueue`** — DLQ interface and
  in-memory implementation for test-time failure inspection.
- **`@retry` / `@timeout` / `@circuit_breaker`** — composable resilience
  decorators. `CircuitBreaker` and `Bulkhead` are shared-instance patterns.
- **`@rate_limit` / `InMemoryRateLimiter`** — per-process rate limiting. Use
  `varco-redis` `RedisRateLimiter` for multi-pod deployments.
- **`@hedge`** — hedged request decorator for tail-latency reduction on
  idempotent reads.
- **`QueryParser` / `ASTVisitor` / `QueryOptimizer` / `TypeCoercionVisitor`** —
  typed query AST pipeline. Filter strings (`age__gte=18`) are parsed into
  `FilterNode` trees, optimised, and applied to backends via visitor pattern.
- **`JwtAuthority` / `MultiKeyAuthority` / `TrustedIssuerRegistry`** — JWT
  signing and verification with zero-downtime key rotation.
- **Key sources** (`PemFile`, `PemFolder`, `JwksUrl`, `OidcDiscovery`) — pluggable
  key loading for `TrustedIssuerRegistry`.
- **`OutboxRepository` / `OutboxRelay`** — transactional outbox pattern.
  Events are saved in the same DB transaction as domain entities; `OutboxRelay`
  publishes them asynchronously.
- **`AsyncCache` / `CacheBackend` / `LayeredCache`** — cache protocol hierarchy.
  `InMemoryCache` and `NoOpCache` ship in core.
- **`InvalidationStrategy`** — pluggable cache invalidation.
  `TTLStrategy`, `TaggedStrategy`, `EventDrivenStrategy`, `CompositeStrategy`
  all ship in core.
- **`@cached` / `CacheServiceMixin`** — decorator and mixin for caching service
  methods.
- **`TenantAwareService` / `SoftDeleteService` / `ValidatorServiceMixin` /
  `AsyncValidatorServiceMixin`** — composable service mixins via MRO.
- **`@span` / `@counter` / `@histogram`** — OpenTelemetry tracing and metrics
  decorators. `OtelConfiguration` wires `TracerProvider` / `MeterProvider` via DI.
- **`VarcoSettings`** — base pydantic-settings class for all backend
  configuration objects.

---

### varco-kafka

#### Added
- **`KafkaEventBus`** — `AbstractEventBus` implementation backed by aiokafka.
  Supports topic-per-channel routing, configurable consumer groups, and
  backpressure via `DispatchMode`.
- **`KafkaDLQ`** — Dead letter queue that routes failed events to a dedicated
  Kafka topic after exhausting retry attempts.
- **`KafkaChannelManager`** — manages topic creation and partition assignment.
- **`KafkaEventBusConfiguration` / `KafkaChannelManagerConfiguration`** —
  `@Configuration` classes for DI wiring.
- **`KafkaHealthCheck`** — liveness / readiness probe for Kafka connectivity.

---

### varco-redis

#### Added
- **`RedisEventBus`** — `AbstractEventBus` implementation backed by Redis
  Pub/Sub.
- **`RedisStreamEventBus`** — alternative implementation backed by Redis Streams
  for durable, consumer-group-aware delivery.
- **`RedisDLQ`** — Dead letter queue using a Redis Hash + Sorted Set backend.
  `push()` never raises — failures are logged and swallowed per the DLQ contract.
- **`RedisCache`** — `CacheBackend` implementation backed by Redis.
- **`RedisRateLimiter`** — distributed rate limiter using Redis atomic counters.
  Use this instead of `InMemoryRateLimiter` in multi-pod deployments.
- **`RedisEncryptionKeyStore`** — encrypted key storage backed by Redis.
- **`RedisLock`** — distributed lock backed by Redis `SET NX EX`.
- **`RedisEventBusConfiguration` / `RedisCacheConfiguration` /
  `RedisStreamConfiguration` / `RedisDLQConfiguration`** — `@Configuration`
  classes for DI wiring.
- **`RedisHealthCheck`** — liveness / readiness probe for Redis connectivity.

---

### varco-sa

#### Added
- **`SAModelFactory`** — generates SQLAlchemy ORM models at import time from
  `DomainModel` subclasses. Models are never declared manually.
- **`SARepository`** — `AsyncRepository` implementation for SQLAlchemy.
- **`SAUnitOfWork` / `SAUoWProvider`** — unit-of-work pattern over SQLAlchemy
  async sessions.
- **`SAOutboxRepository`** — `OutboxRepository` implementation for SQLAlchemy.
- **`SAEncryptionKeyStore`** — encrypted key storage backed by SQLAlchemy.
- **`SQLAlchemyFilterVisitor` / `SQLAlchemyQueryApplicator`** — applies the
  `varco-core` query AST to SQLAlchemy `Select` statements.
- **`SAModule`** / **`bind_repositories()`** — DI wiring helpers.
- **`SAHealthCheck`** — liveness / readiness probe for database connectivity.

---

### varco-beanie

#### Added
- **`BeanieModelFactory`** — generates Beanie `Document` models from
  `DomainModel` subclasses.
- **`BeanieRepository`** — `AsyncRepository` implementation for Beanie/MongoDB.
- **`BeanieUnitOfWork` / `BeanieUoWProvider`** — unit-of-work pattern over
  Beanie sessions.
- **`BeanieOutboxRepository`** — `OutboxRepository` implementation for Beanie.
- **`BeanieModule`** / **`bind_repositories()`** — DI wiring helpers.
- **`BeanieHealthCheck`** — liveness / readiness probe for MongoDB connectivity.

---

### varco-ws

#### Added
- **`WebSocketEventBus` / `WebSocketConnection`** — `AbstractEventBus`
  implementation that delivers events over WebSocket connections.
- **`SSEEventBus` / `SSEConnection`** — `AbstractEventBus` implementation that
  delivers events as Server-Sent Events streams.

---

### varco-fastapi

#### Added
- **`VarcoRouter`** — base `APIRouter` subclass with built-in DI resolution.
- **CRUD mixins** — `CreateMixin`, `ReadMixin`, `UpdateMixin`, `DeleteMixin`,
  `ListMixin`, `StreamMixin` — compose standard HTTP endpoints without boilerplate.
- **`AuthMiddleware`** — validates JWT bearer tokens on every request using
  `TrustedIssuerRegistry`.
- **`CORSMiddleware`** — env-var-driven CORS configuration.
- **`AsyncVarcoClient` / `SyncVarcoClient`** — typed HTTP clients with
  automatic JWT injection, retry, and circuit breaker.
- **`SkillAdapter`** — mounts Google A2A (Agent-to-Agent) skill endpoints from
  a `SkillDefinition`. Install the `[a2a]` extra for the A2A SDK types.
- **`MCPAdapter`** — mounts Model Context Protocol (MCP) tool endpoints.
  Install the `[mcp]` extra (`mcp>=1.0`) for full support.
- **`VarcoFastAPIModule`** / **`bind_clients()`** — DI wiring for FastAPI.
- **Background job runner** — `AsyncJobRunner` backed by `asyncio.TaskGroup`
  for lifecycle-managed background tasks.

---

### varco-memcached

#### Added
- **`MemcachedCache`** — `CacheBackend` implementation backed by aiomcache.
- **`MemcachedCacheConfiguration`** — `@Configuration` class for DI wiring.

---

## [Unreleased]

_Changes planned for the next release will appear here._

---

<!-- Links -->
[0.1.0]: https://github.com/edoardoscarpaci/varco/releases/tag/v0.1.0
[Unreleased]: https://github.com/edoardoscarpaci/varco/compare/v0.1.0...HEAD
