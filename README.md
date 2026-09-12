# varco

A modular Python framework for building expressive, backend-agnostic REST APIs on top of SQLAlchemy and MongoDB (Beanie/Motor). It provides a clean domain model layer, a generic service layer with built-in authorization, a fluent query builder with AST-based filtering, automatic ORM class generation, and a pluggable type coercion system.

[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/edoardoscarpaci/varco/badge)](https://securityscorecards.dev/viewer?uri=github.com/edoardoscarpaci/varco)
[![CI](https://github.com/edoardoscarpaci/varco/actions/workflows/test.yml/badge.svg)](https://github.com/edoardoscarpaci/varco/actions/workflows/test.yml)

**CI**: every push and pull request to `main` runs a live GitHub Actions gate — `ruff` +
`mypy` + the full unit suite across Python 3.12 and 3.13
(`.github/workflows/test.yml`) — plus a nightly + push-to-`main` integration run against real
brokers via testcontainers (`.github/workflows/integration.yml`). See
[CLAUDE.md](CLAUDE.md#ci) for the job breakdown.

**Contributing**: see [CONTRIBUTING.md](CONTRIBUTING.md) for dev setup and the versioning/
deprecation policy, [SECURITY.md](SECURITY.md) to report a vulnerability privately, and
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community expectations.

**Common pitfalls**: [technical_docs/common-pitfalls.md](technical_docs/common-pitfalls.md)
collects the cross-cutting traps that have actually bitten someone using varco — per-call
`CircuitBreaker`/`Bulkhead`/`Singleflight` instances that never do anything, cache keys missing
a tenant namespace, DI binding annotations that silently drop every injection, and more. Worth a
read before writing code against any of those primitives. Feature-specific operational pitfalls
live in each feature's own page under [technical_docs/features/](technical_docs/features/).

---

## Packages

| Package | Description |
|---|---|
| `varco_core` | Backend-agnostic domain model, service layer, authorization, assembler, query AST, builder, parser, DTOs, event system, cache framework, resilience patterns, JWT authority, observability, health checks |
| `varco_fastapi` | FastAPI integration — CRUD router, mixins, JWT auth middleware, job runner, HTTP client utilities |
| `varco_sa` | SQLAlchemy async backend (ORM generation, repository, schema guard, Alembic helpers) |
| `varco_beanie` | Beanie (Motor/MongoDB) async backend |
| `varco_kafka` | Apache Kafka event bus backend (`KafkaEventBus` via aiokafka) |
| `varco_nats` | NATS JetStream event bus backend (`NatsEventBus` via nats-py) |
| `varco_redis` | Redis Pub/Sub event bus + cache backend (`RedisEventBus`, `RedisCache` via redis.asyncio) |
| `varco_ws` | WebSocket and SSE event bus adapters |
| `varco_memcached` | Memcached cache backend |
| `varco_casbin` | Casbin policy-engine authorization backend (ACL/RBAC/ABAC + REST admin router) |

All ten packages are released **in lockstep** — they always share one version number, and a
breaking change in any one bumps all ten. Install the exact version you need with any package
manager's usual pin; siblings resolve automatically via a compatible-release pin
(`varco-core~=3.0`), so `varco-kafka==3.1.0` works with `varco-core` anywhere in the `3.x` line.
See `CONTRIBUTING.md`'s versioning and deprecation policy for the full contract.

---

## Quickstart — Example App

The [`examples/00-full-stack-post-api/`](examples/00-full-stack-post-api/) directory contains a
**complete, runnable Post API** that wires the full varco stack together:

- FastAPI + `VarcoCRUDRouter` with 6 CRUD mixins
- JWT auth (RSA-2048, role-based)
- Redis Streams event bus + Redis cache
- SQLAlchemy async ORM (PostgreSQL via asyncpg)
- WebSocket + SSE real-time push
- Async job runner (background task offload)
- Providify DI container

**Start in 3 commands:**

```bash
git clone https://github.com/edoardoscarpaci/varco && cd varco
uv sync --all-packages --all-extras
cd examples/00-full-stack-post-api && docker compose up -d
# app at http://localhost:8000/docs
```

Full setup, API reference, architecture diagram, and extension guide:
**[examples/00-full-stack-post-api/README.md](examples/00-full-stack-post-api/README.md)**

The [`examples/`](examples/) directory has 20+ additional focused examples (JWT rotation, Casbin
policy engine, field encryption, observability, profiling, …) — see
**[examples/README.md](examples/README.md)** for the full package → example map.

---

## Table of Contents

- [Quickstart — Example App](#quickstart--example-app)
- [Domain Model](#domain-model)
  - [Soft Delete](#soft-delete)
  - [Multi-tenancy models](#multi-tenancy-models)
  - [Schema versioning](#schema-versioning--migration)
- [Metadata & Constraints](#metadata--constraints)
- [Repository & Unit of Work](#repository--unit-of-work)
  - [exists() and stream()](#exists-and-stream)
- [DTOs](#dtos)
- [DTO Assembler](#dto-assembler)
- [Service Layer](#service-layer)
  - [Composable mixins](#composable-mixins)
  - [TenantAwareService](#tenantawareservice)
  - [SoftDeleteService](#softdeleteservice)
  - [Combining mixins](#combining-mixins)
  - [paged_list()](#paged_list)
  - [exists() and stream()](#exists-and-stream-1)
  - [ServiceProtocol](#serviceprotocol)
  - [Authorization order](#authorization-order)
  - [DI wiring](#di-wiring)
- [Authorization](#authorization)
  - [Action](#action)
  - [ResourceGrant](#resourcegrant)
  - [AuthContext](#authcontext)
  - [AbstractAuthorizer](#abstractauthorizer)
  - [Built-in authorizers](#built-in-authorizers)
  - [BaseAuthorizer](#baseauthorizer)
- [Error codes & HTTP mapping](#error-codes--http-mapping)
- [Internationalization, Timezones, and Bulk Cache Ops](#internationalization-timezones-and-bulk-cache-ops)
- [Correlation ID / Tracing](#correlation-id--tracing)
- [Multi-tenancy (DB-level)](#multi-tenancy-db-level)
- [Tenant identity provenance](#tenant-identity-provenance)
- [Query System](#query-system)
- [Event System](#event-system)
  - [Event base class](#event-base-class)
  - [AbstractEventBus](#abstracteventbus)
  - [InMemoryEventBus](#inmemoryeventbus)
  - [Producer — AbstractEventProducer](#producer--abstracteventproducer)
  - [Consumer — EventConsumer + @listen](#consumer--eventconsumer--listen)
  - [Priority](#priority)
  - [ErrorPolicy and DispatchMode](#errorpolicy-and-dispatchmode)
  - [Middleware](#event-middleware)
  - [Domain events](#domain-events)
  - [JsonEventSerializer](#jsoneventserializer)
  - [Kafka backend (varco_kafka)](#kafka-backend-varco_kafka)
  - [Redis backend (varco_redis)](#redis-backend-varco_redis)
- [Transactional Outbox](#transactional-outbox)
- [SQLAlchemy Backend](#sqlalchemy-backend)
  - [Bootstrap (one-liner setup)](#bootstrap-one-liner-setup)
  - [Alembic helpers](#alembic-helpers)
  - [Schema Guard](#schema-guard)
- [Schema Migrations](#schema-migrations)
  - [The varco CLI](#the-varco-cli)
- [Beanie Backend](#beanie-backend)
  - [Bootstrap](#bootstrap-beanie)
- [Exception Hierarchy](#exception-hierarchy)
- [Running tests](#running-tests)
- [Cache System](#cache-system)
  - [AsyncCache and CacheBackend](#asynccache-and-cachebackend)
  - [InMemoryCache](#inmemorycache)
  - [LayeredCache](#layeredcache)
  - [Invalidation strategies](#invalidation-strategies)
  - [CacheServiceMixin](#cacheservicemixin)
  - [@cached decorator](#cached-decorator)
  - [CachedService wrapper](#cachedservice-wrapper)
- [Resilience](#resilience)
  - [retry](#retry)
  - [circuit_breaker](#circuit_breaker)
  - [timeout](#timeout)
  - [rate_limit](#rate_limit)
  - [bulkhead](#bulkhead)
  - [hedge](#hedge)
  - [Composing patterns](#composing-patterns)
- [JWT / Authority System](#jwt--authority-system)
  - [JwtAuthority — signing](#jwtauthority--signing)
  - [MultiKeyAuthority — key rotation](#multikeyauthority--key-rotation)
  - [TrustedIssuerRegistry — verification](#trustedissuerregistry--verification)
  - [JWKS background refresh](#jwks-background-refresh-plan-041--s22)
  - [Key sources](#key-sources)
  - [Verification hardening (VARCO_JWT_*)](#verification-hardening-varco_jwt_)
- [Connection Settings](#connection-settings)
  - [SSLConfig](#sslconfig)
  - [TLS trust store](#tls-trust-store)
  - [RedisConnectionSettings](#redisconnectionsettings)
  - [HttpConnectionSettings](#httpconnectionsettings)
- [FastAPI Integration](#fastapi-integration)
  - [VarcoRouter and VarcoCRUDRouter](#varcorouter-and-varcocrudrouter)
  - [CRUD mixins](#crud-mixins)
  - [Service-free routers — GenericRouter](#service-free-routers--genericrouter)
  - [JWT authentication middleware](#jwt-authentication-middleware)
  - [Request context](#request-context)
  - [Middleware stack](#middleware-stack)
  - [Job runner — async mode](#job-runner--async-mode)
  - [Bootstrap helpers](#bootstrap-helpers)
  - [MCP — exposing a VarcoRouter as an MCP server](#mcp--exposing-a-varcorouter-as-an-mcp-server)
  - [A2A — exposing a non-router subject](#a2a--exposing-a-non-router-subject)
  - [Calling other varco services — client_for](#calling-other-varco-services--client_for)
- [Observability](#observability)
  - [@span — distributed tracing](#span--distributed-tracing)
  - [@counter and @histogram](#counter-and-histogram)
  - [TracingServiceMixin](#tracingservicemixin)
  - [OtelConfig and DI wiring](#otelconfig-and-di-wiring)
- [Redaction](#redaction)
- [Profiling](#profiling)
- [Background Jobs](#background-jobs)
- [Database Auditing](#database-auditing)
- [Dead Letter Queue](#dead-letter-queue)
- [Idempotency-Key middleware](#idempotency-key-middleware)
- [Token revocation](#token-revocation)
- [API-key hashing](#api-key-hashing)
- [CloudEvents envelope](#cloudevents-envelope)
- [AsyncAPI export](#asyncapi-export)
- [Outbound webhooks](#outbound-webhooks)
- [Inbound webhook verification](#inbound-webhook-verification)
- [Feature flags](#feature-flags)
- [Recurring schedules](#recurring-schedules)
- [Retention & purge automation](#retention--purge-automation)
- [File watching and hot reload](#file-watching-and-hot-reload)
- [Security headers](#security-headers)
- [Request body limits](#request-body-limits)
- [HTTP rate limiting](#http-rate-limiting)
- [Security posture preflight](#security-posture-preflight)
- [Authorization-decision audit](#authorization-decision-audit)
- [Composite Deployment](#composite-deployment)
- [Durability preset (one-line opt-in)](#durability-preset-one-line-opt-in)
- [Changelog summary](#changelog-summary)
- [Health Checks](#health-checks)

---

## Domain Model

All domain entities inherit from one of three base classes:

```python
from varco_core import DomainModel, AuditedDomainModel, VersionedDomainModel
```

| Class | Extra fields |
|---|---|
| `DomainModel` | `pk` only |
| `AuditedDomainModel` | `pk`, `created_at`, `updated_at` |
| `VersionedDomainModel` | `pk`, `created_at`, `updated_at`, `definition_version`, `row_version` |

```python
from __future__ import annotations
from typing import Annotated
from varco_core import AuditedDomainModel
from varco_core.meta import FieldHint, PrimaryKey, PKStrategy, pk_field


class User(AuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    name: Annotated[str, FieldHint(max_length=100)]
    email: Annotated[str, FieldHint(unique=True, max_length=255)]
    active: bool = True
```

### Soft Delete

Inherit from one of the soft-delete bases to get a `deleted_at: datetime | None` field:

```python
from varco_core import SoftDeleteDomainModel, SoftDeleteAuditedDomainModel


# Simple — pk + deleted_at
class ArchivedPost(SoftDeleteDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    title: str


# Audited — pk + created_at + updated_at + deleted_at
class Post(SoftDeleteAuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    title: str
    body: str
```

Or mix in `SoftDeleteMixin` yourself onto any existing hierarchy:

```python
from varco_core import SoftDeleteMixin, VersionedDomainModel


class Document(SoftDeleteMixin, VersionedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    content: str
```

The `SoftDeleteService` mixin (see [below](#softdeleteservice)) automatically excludes soft-deleted records from all queries and replaces physical deletion with a timestamp stamp.

### Multi-tenancy models

```python
from varco_core import TenantDomainModel, TenantAuditedDomainModel


class Post(TenantAuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    tenant_id: Annotated[str, FieldHint(index=True, nullable=False)]
    title: str
```

Or add `TenantMixin` to any base:

```python
from varco_core import TenantMixin, SoftDeleteAuditedDomainModel


class Post(TenantMixin, SoftDeleteAuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    tenant_id: Annotated[str, FieldHint(index=True)]
    title: str
```

### Schema versioning & migration

```python
from varco_core import DomainMigrator


class UserMigrator(DomainMigrator):
    steps = [
        lambda data: {**data, "active": True},  # v0 → v1
        lambda data: {**data, "email": data["email"].lower()},  # v1 → v2
    ]
```

---

## Metadata & Constraints

```python
from varco_core.meta import (
    FieldHint,
    PrimaryKey,
    PKStrategy,
    ForeignKey,
    UniqueConstraint,
    CheckConstraint,
    pk_field,
)
```

### Field-level hints

```python
class Post(AuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    title: Annotated[str, FieldHint(max_length=200, nullable=False)]
    slug: Annotated[str, FieldHint(unique=True, index=True, max_length=200)]
    author_id: Annotated[int, ForeignKey("users.pk", on_delete="CASCADE")]
    views: int = 0
```

### PK strategies

| Strategy | Behaviour |
|---|---|
| `INT_AUTO` | Auto-increment integer |
| `UUID_AUTO` | Auto-generated UUID4 |
| `STR_ASSIGNED` | Caller must supply the value |
| `CUSTOM` | User-provided generation logic |

### Table-level constraints

```python
from varco_core.meta import UniqueConstraint, CheckConstraint


class Subscription(AuditedDomainModel):
    __constraints__ = [
        UniqueConstraint("user_id", "plan_id", name="uq_user_plan"),
        CheckConstraint("price >= 0", name="chk_price_positive"),
    ]

    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    user_id: int
    plan_id: int
    price: float
```

---

## Repository & Unit of Work

```python
from varco_core import AsyncRepository, AsyncUnitOfWork
```

### `AsyncRepository[D, PK]` interface

```python
# All methods are async
await repo.find_by_id(pk)  # D | None
await repo.find_all()  # list[D]
await repo.save(entity)  # D  (INSERT or UPDATE)
await repo.delete(entity)
await repo.find_by_query(params)  # list[D]
await repo.count(params)  # int
await repo.exists(pk)  # bool  — lightweight, no ORM hydration
async for entity in repo.stream_by_query(params):  # AsyncIterator[D]
    process(entity)
```

### Custom repository

```python
from varco_core import AsyncRepository


class UserRepository(AsyncRepository[User, int]):
    async def find_active(self) -> list[User]:
        params = QueryParams(node=QueryBuilder().eq("active", True).build())
        return await self.find_by_query(params)
```

### `exists()` and `stream()`

`exists()` is a lightweight PK probe that avoids loading the full entity:

```python
async with provider.make_uow() as uow:
    if await uow.posts.exists(post_id):
        print("post exists in the backing store")
```

`stream_by_query()` yields entities one at a time — useful when result sets are too large to hold in memory:

```python
params = QueryParams(node=QueryBuilder().eq("active", True).build())

async with provider.make_uow() as uow:
    async for post in uow.posts.stream_by_query(params):
        await send_email(post.author_email)
        # Only one Post is in memory at a time — session stays open for the loop
```

---

## DTOs

Pydantic-based request/response contracts:

```python
from varco_core import CreateDTO, ReadDTO, UpdateDTO, UpdateOperation
```

```python
class UserCreate(CreateDTO):
    name: str
    email: str


class UserRead(ReadDTO):
    name: str
    email: str
    active: bool


class UserUpdate(UpdateDTO):
    name: str | None = None
    email: str | None = None
```

### Update operations

```python
class TagUpdate(UpdateDTO):
    tags: list[str] | None = None


patch = TagUpdate(tags=["python"], op=UpdateOperation.EXTEND)  # append
patch = TagUpdate(tags=["old"], op=UpdateOperation.REMOVE)  # remove
patch = TagUpdate(tags=["new"], op=UpdateOperation.REPLACE)  # overwrite (default)
```

---

## DTO Assembler

`AbstractDTOAssembler[D, C, R, U]` is the only layer responsible for translating between domain entities and DTOs:

```python
from varco_core.assembler import AbstractDTOAssembler
from dataclasses import replace


class PostAssembler(AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]):
    def to_domain(self, dto: CreatePostDTO) -> Post:
        return Post(title=dto.title, body=dto.body)

    def to_read_dto(self, entity: Post) -> PostReadDTO:
        return PostReadDTO(
            id=entity.pk,
            title=entity.title,
            body=entity.body,
            created_at=entity.created_at,
        )

    def apply_update(self, entity: Post, dto: UpdatePostDTO) -> Post:
        # dataclasses.replace — copies _raw_orm so repo treats it as UPDATE
        return replace(
            entity,
            title=dto.title if dto.title is not None else entity.title,
            body=dto.body if dto.body is not None else entity.body,
        )
```

The shorthand `Assembler` alias saves typing in service `__init__` signatures:

```python
from varco_core import Assembler  # TypeAlias for AbstractDTOAssembler


def __init__(
    self, assembler: Inject[Assembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]]
): ...
```

---

## Service Layer

The service layer is the **only** layer that enforces authorization, orchestrates transactions, and raises typed `ServiceException` subclasses.

### Composable mixins

`AsyncService` exposes four protected hook methods that mixins override to inject cross-cutting behaviour without duplicating CRUD logic:

| Hook | When called | Purpose |
|---|---|---|
| `_pre_check(ctx)` | Before opening the UoW | Fast stateless gate (e.g. tenant ID present) |
| `_scoped_params(params, ctx)` | Before `list` / `count` queries | Inject extra filter nodes |
| `_check_entity(entity, ctx)` | After `find_by_id` in `get` / `update` / `delete` | Validate entity visibility |
| `_prepare_for_create(entity, ctx)` | After `to_domain()` in `create` | Stamp cross-cutting fields |

Every hook calls `super()` at the end — this chains through Python's MRO so multiple mixins compose without any CRUD method duplication.

### TenantAwareService

Enforces row-level tenant isolation via the four hooks. No CRUD methods are overridden:

```python
from varco_core import TenantAwareService, IUoWProvider
from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth import AbstractAuthorizer
from providify import Inject, Singleton


@Singleton
class PostService(TenantAwareService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO]):
    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer: Inject[AbstractAuthorizer],
        assembler: Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]],
    ) -> None:
        super().__init__(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

    def _get_repo(self, uow):
        return uow.posts
```

What the hooks inject:

- `_pre_check` — raises `ServiceAuthorizationError` if `ctx.metadata["tenant_id"]` is absent, before any DB access
- `_scoped_params` — prepends `tenant_id = <tid>` to every query
- `_check_entity` — raises `ServiceNotFoundError` (404, not 403) for cross-tenant entities
- `_prepare_for_create` — stamps `tenant_id` from the JWT onto every new entity

By default the field name is `"tenant_id"`. Override `_tenant_field` to use a different name:

```python
class PostService(TenantAwareService[Post, ...]):
    _tenant_field = "org_id"  # uses Post.org_id instead of Post.tenant_id

    def _get_repo(self, uow):
        return uow.posts
```

### SoftDeleteService

Replaces physical deletion with a `deleted_at` timestamp and excludes soft-deleted records from all queries:

```python
from varco_core import SoftDeleteService


@Singleton
class PostService(SoftDeleteService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO]):
    def __init__(self, uow_provider, authorizer, assembler): ...
    def _get_repo(self, uow):
        return uow.posts
```

Extra methods beyond the standard CRUD:

```python
# Physical delete is gone — this stamps deleted_at = now() instead
await svc.delete(post_id, ctx)

# Restore a soft-deleted entity — clears deleted_at
restored = await svc.restore(post_id, ctx)
```

What the hooks inject:

- `_scoped_params` — prepends `deleted_at IS NULL` to every `list` / `count` query
- `_check_entity` — raises `ServiceNotFoundError` if the entity has `deleted_at` set
- `_prepare_for_create` — resets `deleted_at = None` on new entities

### Combining mixins

Both mixins use cooperative `super()` calls, so they compose automatically via Python's MRO. Put `TenantAwareService` first so the tenant check runs before the soft-delete check:

```python
@Singleton
class PostService(
    TenantAwareService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO],
    SoftDeleteService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO],
    AsyncService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO],
):
    def __init__(self, uow_provider, authorizer, assembler): ...
    def _get_repo(self, uow):
        return uow.posts
```

MRO: `PostService → TenantAwareService → SoftDeleteService → AsyncService`

Hook execution on a `list()` call:

```
_scoped_params:
  TenantAwareService  → injects "tenant_id = acme"
  SoftDeleteService   → injects "deleted_at IS NULL"
  AsyncService        → returns (base no-op)

Final filter: tenant_id = 'acme' AND deleted_at IS NULL AND <caller's filter>
```

If you need this combination in many services, define a shared abstract base once:

```python
class TenantSoftDeleteService(
    TenantAwareService[D, PK, C, R, U],
    SoftDeleteService[D, PK, C, R, U],
    AsyncService[D, PK, C, R, U],
    ABC,
    Generic[D, PK, C, R, U],
):
    """Pre-composed tenant-aware + soft-delete base."""
```

### `paged_list()`

Returns a pagination envelope by running `list()` and `count()` concurrently:

```python
page = await svc.paged_list(
    QueryParams(limit=20, offset=0),
    ctx,
    raw_query=request.query_params.get("q"),
)
# page.items       → list[PostReadDTO] for the current page
# page.total_count → int  (full matching set — ignores limit/offset)
# page.next        → PageCursor | None (None on the last page)
```

`TenantAwareService` and `SoftDeleteService` both compose with `paged_list()` automatically — `list()` and `count()` already call `_scoped_params()` so filters are applied in both sub-calls.

### `exists()` and `stream()`

`exists()` is a lightweight PK check without fetching entity data:

```python
# Returns True/False — same Action.READ auth as get()
if await svc.exists(post_id, ctx):
    print("post is visible in the backing store")
```

Note: `exists()` does **not** apply `_check_entity` hooks (soft-delete, tenant boundary). It reports raw backing-store presence. Use `get()` and catch `ServiceNotFoundError` if you need service-layer visibility semantics.

`stream()` is the service-layer counterpart of `stream_by_query()` — same authorization and `_scoped_params` as `list()`, but yields one `ReadDTO` at a time:

```python
from contextlib import aclosing

# Iterate over potentially millions of rows without loading them all into memory
async with aclosing(svc.stream(QueryParams(), ctx)) as it:
    async for post_dto in it:
        await publish_to_queue(post_dto)
```

The UoW (and DB cursor) stays open for the entire iteration. Wrap in `contextlib.aclosing()` when early exit is possible — it guarantees `aclose()` is called and the session is released cleanly.

### ServiceProtocol

Use `ServiceProtocol` to type-hint HTTP handlers or adapters without coupling to `AsyncService`:

```python
from varco_core import ServiceProtocol


async def list_handler(
    service: ServiceProtocol[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO],
    params: QueryParams,
    ctx: AuthContext,
) -> list[PostReadDTO]:
    return await service.list(params, ctx)
```

`ServiceProtocol` declares all public methods: `get`, `list`, `count`, `paged_list`, `create`, `update`, `delete`, `exists`, `stream`.

### Authorization order

| Operation | Order |
|---|---|
| `create`, `list`, `count`, `paged_list` | Auth first → then open DB |
| `get`, `update`, `delete` | Fetch first → then auth (so ownership checks can inspect the entity) |
| `exists` | Auth first (collection-level READ) → then DB |
| `stream` | Auth first (collection-level LIST) → then stream |

> The fetch-first pattern for `get`/`update`/`delete` prevents an **existence oracle**: a 403 would reveal the entity exists even when the caller lacks permission. A missing entity always returns 404 regardless of auth.

### DI wiring

```python
from providify import Singleton, DIContainer


@Singleton
class PostAssembler(AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]): ...


@Singleton
class PostService(AsyncService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO]):
    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer: Inject[AbstractAuthorizer],
        assembler: Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]],
    ) -> None:
        super().__init__(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

    def _get_repo(self, uow):
        return uow.posts


container = DIContainer()
container.register(PostAssembler)
container.register(PostService)
```

---

## Authorization

### Action

`Action` is a `StrEnum` — every value is also a plain `str` at runtime:

```python
from varco_core.auth import Action

Action.CREATE  # "create"
Action.READ  # "read"
Action.UPDATE  # "update"
Action.DELETE  # "delete"
Action.LIST  # "list"

Action.READ == "read"  # True
```

### ResourceGrant

```python
from varco_core.auth import ResourceGrant, Action

ResourceGrant("posts", frozenset({Action.LIST, Action.CREATE, Action.READ}))
ResourceGrant("posts:abc123", frozenset({Action.UPDATE, Action.DELETE}))
ResourceGrant("*", frozenset(Action))  # wildcard — admin
```

### AuthContext

```python
from varco_core.auth import AuthContext, ResourceGrant, Action

ctx = AuthContext(
    user_id="usr_123",
    roles=frozenset({"editor"}),
    grants=(
        ResourceGrant("posts", frozenset({Action.LIST, Action.READ})),
        ResourceGrant("posts:abc123", frozenset({Action.UPDATE, Action.DELETE})),
    ),
    metadata={"tenant_id": "acme"},  # arbitrary bag — used by TenantAwareService
)

ctx.is_anonymous()  # False
ctx.has_role("editor")  # True
ctx.can(Action.READ, "posts")  # True  — type-level grant
ctx.can(Action.UPDATE, "posts")  # False — no type-level UPDATE
ctx.can(Action.UPDATE, "posts:abc123")  # True  — instance-level grant
```

Anonymous (unauthenticated) caller:

```python
ctx = AuthContext()  # user_id=None, no grants
ctx.is_anonymous()  # True
ctx.can(Action.READ, "posts")  # False
```

### AbstractAuthorizer

```python
from varco_core.auth import AbstractAuthorizer, Action, AuthContext, Resource
from varco_core.exception.service import ServiceAuthorizationError


class AppAuthorizer(AbstractAuthorizer):
    async def authorize(self, ctx: AuthContext, action: Action, resource: Resource) -> None:
        meta = getattr(resource.entity_type, "Meta", None)
        table = getattr(meta, "table", resource.entity_type.__name__.lower())

        if not resource.is_collection:
            if ctx.can(action, f"{table}:{resource.entity.pk}"):
                return

        if ctx.can(action, table):
            return

        raise ServiceAuthorizationError(str(action), resource.entity_type)
```

### Built-in authorizers

Three ready-to-use `AbstractAuthorizer` implementations for common patterns:

#### `GrantBasedAuthorizer`

Checks `ctx.can(action, resource_key)`. The resource key is derived as `"posts"` (collection) or `"posts:42"` (instance) by default — override `_resource_key()` to customise:

```python
from varco_core import GrantBasedAuthorizer


@Singleton
class AppAuthorizer(GrantBasedAuthorizer):
    def _resource_key(self, entity_type, entity=None) -> str:
        table = entity_type.__name__.lower() + "s"
        if entity is not None:
            return f"{table}:{entity.pk}"
        return table
```

#### `OwnershipAuthorizer`

Grants collection ops (LIST, CREATE) to everyone; instance ops (GET, UPDATE, DELETE) only when `entity.<owner_field> == ctx.user_id`:

```python
from varco_core import OwnershipAuthorizer


@Singleton
class AppAuthorizer(OwnershipAuthorizer):
    _owner_field = "author_id"  # default is "owner_id"

    # Override to customise collection-level behaviour (default: allow all)
    async def _check_collection(self, ctx, action, resource):
        if action == Action.CREATE and ctx.is_anonymous():
            raise ServiceAuthorizationError("create", resource.entity_type)
```

#### `RoleBasedAuthorizer`

Grants actions based on `ctx.roles` and a static permission table:

```python
from varco_core import RoleBasedAuthorizer
from varco_core.auth import Action


@Singleton
class AppAuthorizer(RoleBasedAuthorizer):
    role_permissions = {
        "admin": frozenset(Action),  # all actions
        "editor": frozenset({Action.READ, Action.LIST, Action.UPDATE}),
        "viewer": frozenset({Action.READ, Action.LIST}),
    }
```

### BaseAuthorizer

Permissive fallback — allows every operation. Registered at the lowest priority so any application authorizer automatically takes precedence:

```python
from varco_core.base_authorizer import BaseAuthorizer

# Development / testing only — never ship this to production without shadowing it
container.scan("varco_core.base_authorizer")

# Production guard
assert not isinstance(container.get(AbstractAuthorizer), BaseAuthorizer), (
    "No real authorizer registered — refusing to start"
)
```

---

## Error codes & HTTP mapping

`FastrestErrorCodes` is a Python `Enum` where each member's `.value` is a frozen `ErrorCode` dataclass. Stable code strings (e.g. `"FASTREST_001"`) serve as i18n translation catalog keys:

```python
from varco_core import FastrestErrorCodes, ErrorCode

FastrestErrorCodes.NOT_FOUND.code  # "FASTREST_001"
FastrestErrorCodes.NOT_FOUND.http_status  # 404
FastrestErrorCodes.NOT_FOUND.default_message  # "The requested resource was not found."

list(FastrestErrorCodes)  # all built-in codes — iterable because it's an Enum
```

| Member | Code | HTTP |
|---|---|---|
| `NOT_FOUND` | `FASTREST_001` | 404 |
| `UNAUTHORIZED` | `FASTREST_002` | 403 |
| `CONFLICT` | `FASTREST_003` | 409 |
| `VALIDATION_ERROR` | `FASTREST_004` | 422 |
| `INTERNAL_ERROR` | `FASTREST_500` | 500 |

`ErrorEnvelopeSettings` (env prefix `VARCO_ERROR_`) governs what else the envelope carries:

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_ERROR_INCLUDE_MESSAGE_KEY` | `true` | Include the i18n `message_key` member |
| `VARCO_ERROR_INCLUDE_PARAMS` | `true` | Include the structured `params` member |
| `VARCO_ERROR_PROBLEM_DETAILS` | `false` | Emit an RFC 9457 Problem Details body instead |
| `VARCO_ERROR_INCLUDE_DETAIL` | `true` | Include the `detail` member (`str(exc)`) on a mapped `ServiceException`. Byte-identical to pre-3.2 (Plan 035 / §D-S3b) — a **warn-only**, 4.0-flip-candidate knob, reported by `inspect_http_edge()` as `http.error.detail_exposed`. Set `false` to omit it now |

See `technical_docs/features/error-taxonomy-and-i18n.md` for the full envelope shape and the S3
fallback-leak fix this knob is adjacent to (unconditional, no toggle).

### FastAPI exception handler

```python
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from varco_core.exception.service import ServiceException
from varco_core.exception.http import error_message_for

app = FastAPI()


@app.exception_handler(ServiceException)
async def service_error_handler(request: Request, exc: ServiceException):
    msg = error_message_for(exc)
    # msg.model_dump(exclude_none=True) → {"code": "FASTREST_001", "http_status": 404,
    #                      "message": "The requested resource was not found.",
    #                      "detail": "Post with pk=42 not found.",
    #                      "message_key": "varco.error.not_found",
    #                      "params": {"entity": "Post", "entity_id": "42"}}
    # message_key/params are new (a built-in exception only — see
    # technical_docs/features/error-taxonomy-and-i18n.md); an out-of-tree
    # ServiceException with no message_key gets a byte-identical body.
    return JSONResponse(status_code=msg.http_status, content=msg.model_dump(exclude_none=True))
```

### i18n support

Pass a `translator` callable that receives the stable code string and returns the localised message:

```python
@app.exception_handler(ServiceException)
async def service_error_handler(request: Request, exc: ServiceException):
    msg = error_message_for(exc, translator=request.state.translate)
    # request.state.translate("FASTREST_001") → "Kaynak bulunamadı."  (Turkish)
    return JSONResponse(status_code=msg.http_status, content=msg.model_dump())
```

### Custom application error codes

Register app-specific codes at startup. They take precedence over built-in codes:

```python
from varco_core import ErrorCode, register_error_code
from varco_core.exception.service import ServiceException


class QuotaExceededError(ServiceException): ...


register_error_code(
    QuotaExceededError,
    ErrorCode("APP_001", 429, "Request quota exceeded."),
)
```

---

## Internationalization, Timezones, and Bulk Cache Ops

Three off-by-default features (nothing below changes behaviour until you opt
in) — each linked doc has the full design, decision rationale, and known
limitations:

- **Internationalization** — `MessageCatalog` (stdlib `gettext`-backed by
  default, zero new runtime dependency), an `Accept-Language`/`?lang=`
  negotiation chain, and request-scoped locale via `varco_core.context`.
  `create_varco_app(i18n=I18nSettings(enabled=True, supported_locales=("en",
  "fr")))`. See
  [`technical_docs/features/i18n-and-localization.md`](technical_docs/features/i18n-and-localization.md).
- **Timezones & DST-safe scheduling** — per-request timezone resolution
  (`?tz=`/`X-Timezone`), a documented DST gap/overlap policy for one-shot
  scheduled jobs (`Job.run_at_wall`/`run_at_tz`), and a declared (not
  accidental) timezone contract for query-layer datetime filters
  (`DatetimeCoercionPolicy`). `pip install "varco-core[tz]"` on slim/
  distroless images. See
  [`technical_docs/features/timezone-handling.md`](technical_docs/features/timezone-handling.md).
- **Bulk cache operations** — `BulkCache` (`get_many`/`set_many`/
  `delete_many`), a separate Protocol from `AsyncCache` so out-of-tree cache
  implementations never silently stop satisfying `isinstance()`, with
  portable loop-based defaults and native `MGET`/`get_multi` overrides in
  `varco_redis`/`varco_memcached`. See
  [`technical_docs/features/cache-hardening.md`](technical_docs/features/cache-hardening.md)'s
  "Bulk operations" section.

---

## Correlation ID / Tracing

Attach a correlation ID to every log record in the current async task:

```python
from varco_core import (
    generate_correlation_id,  # → str (UUID4)
    current_correlation_id,  # → str | None
    correlation_context,  # async context manager
    CorrelationIdFilter,  # logging.Filter
)
import logging

# Wire the filter once at startup — stamps record.correlation_id on every log line
logging.getLogger().addFilter(CorrelationIdFilter())


# In the HTTP middleware — activate a fresh ID per request
@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    cid = request.headers.get("X-Correlation-ID") or generate_correlation_id()
    async with correlation_context(cid):
        response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid
    return response


# Anywhere in the service layer
cid = current_correlation_id()  # "f47ac10b-58cc-4372-a567-0e02b2c3d479"
```

The correlation ID is stored in a `ContextVar` — each asyncio task gets its own isolated copy. Tasks spawned inside `correlation_context()` inherit the ID automatically.

---

## Multi-tenancy (DB-level)

`TenantUoWProvider` routes `make_uow()` to a per-tenant backend (separate DB or schema):

```python
from varco_core import TenantUoWProvider, tenant_context, current_tenant
from varco_sa import SQLAlchemyRepositoryProvider

provider = TenantUoWProvider(
    {
        "acme": SQLAlchemyRepositoryProvider(engine_acme, sessions_acme),
        "globex": SQLAlchemyRepositoryProvider(engine_globex, sessions_globex),
    }
)


# In the HTTP adapter — activate the tenant once per request
@app.middleware("http")
async def tenant_middleware(request: Request, call_next):
    tid = request.headers["X-Tenant-ID"]
    with tenant_context(tid):
        return await call_next(request)
```

Add tenants at runtime (no restart needed):

```python
new_provider = SQLAlchemyRepositoryProvider(new_engine, new_sessions)
await new_provider.create_all()
provider.register("new_tenant", new_provider)
```

```python
provider.has_tenant("acme")  # True
provider.registered_tenants()  # ["acme", "globex"]
current_tenant()  # "acme" (inside a tenant_context block)
```

`TenantUoWProvider` (above) is the static, hand-registered form. For a
**selectable isolation strategy** (shared schema, optionally RLS-asserted;
Postgres schema-per-tenant; Postgres/Mongo database-per-tenant), a **dynamic
tenant control plane** (REST or event-driven onboarding, backed by a
durable catalog), and **global/shared scope** for reference data every
tenant reads, see `varco_core.tenancy` and
[`technical_docs/features/multitenancy.md`](technical_docs/features/multitenancy.md).
Every default there is byte-identical to the `TenantUoWProvider` shape above
— nothing changes unless you opt in:

```python
from varco_core.tenancy import TenancySettings, TenantIsolation

settings = TenancySettings(isolation=TenantIsolation.SCHEMA)  # opt-in
```

**Worked example — turn on schema-per-tenant isolation and onboard a tenant:**

```python
from varco_core.tenancy import TenancySettings, TenantIsolation
from varco_sa.tenancy.router import SASchemaRouter
from varco_sa.tenancy.provisioner import SASchemaProvisioner
from varco_sa.tenancy.catalog import SATenantCatalog
from varco_core.tenancy.control.service import TenantControlService

# 1. Opt in — everything else stays SHARED-shaped until this flips
settings = TenancySettings(isolation=TenantIsolation.SCHEMA)

# 2. Wire the router (schema_translate_map, not search_path — see
#    technical_docs/features/postgres-rls.md §3) and the provisioner
router = SASchemaRouter(schema_template=settings.schema_template)
provisioner = SASchemaProvisioner(engine=engine)

# 3. The control service ties catalog + provisioner + event emission together
control_service = TenantControlService(
    catalog=SATenantCatalog(session=admin_session),
    provisioner=provisioner,
    producer=producer,  # AbstractEventProducer — emits TenantCatalogChanged
)

# 4. Onboard — idempotent; a second call on an already-active tenant is a no-op
await control_service.provision("acme")

# 5. Per-request: TenantResolutionMiddleware checks catalog status BEFORE
#    pool.ensure(), then wraps the handler in tenant_context("acme") —
#    session_factory_for(engine, "acme") resolves every routed table to
#    schema "t_acme"; global + framework tables stay in the default schema.
```

**Caveat**: models generated under `SAModelFactory.build(..., isolation="schema")` carry a
symbolic `"tenant"` schema token only for `TenantScope.TENANT` models — a `GLOBAL` model or
one of the ten framework tables never does, and under `TenantIsolation.SHARED` (the
default) `__table__.schema` stays `None`, byte-identical to today.

**Mounting the admin/provisioning surface** — always opt-in and gated:

```python
from varco_fastapi.tenancy.mount import mount_tenant_admin

app = create_varco_app(container, routers=[...])  # tenant traffic — no admin privilege
mount_tenant_admin(  # ← privileged surface, opt-in
    app,
    control_service,
    acknowledge_bundled_admin=True,  # required; ValueError without it
    server_auth=auth,
    admin_role="tenant-admin",
    prefix="/tenancy",
)
```

### Database-enforced isolation (Postgres RLS)

Strategy 2 (shared schema + RLS asserted, above) is opt-in and generates nothing by default. To
get a full, reviewed policy set for every `TenantScope.TENANT` table without hand-writing one
per table, plus the GUC-setter every policy needs at query time:

```python
from varco_sa.rls_autogen import plan_tenant_rls, tenant_rls_upgrade, tenant_rls_downgrade

# In code review, before any DDL exists:
plans = plan_tenant_rls([User, Order, Invoice], base=Base)
print(plans)  # resolve every skipped_reason and nullable-tenant-column choice first

# In your own reviewed Alembic revision — varco never ships one for you:
def upgrade() -> None:
    tenant_rls_upgrade(op, plans=plans)

def downgrade() -> None:
    tenant_rls_downgrade(op, plans=plans)
```

```python
from sqlalchemy.ext.asyncio import AsyncSession
from varco_sa.tenancy.rls_session import install_rls_tenant_hook

# Sets rls.tenant_id from current_tenant() at the start of every transaction —
# covers SQLAlchemyUnitOfWork, get_repository(), and app code holding the
# session factory directly. Opt-in; nothing installs unless you call this or
# set VARCO_TENANCY_RLS_SET_TENANT=true.
install_rls_tenant_hook(AsyncSession, require_tenant=False)
```

```python
from varco_sa.tenancy.rls_check import inspect_rls_posture

# Is my deployment actually protected, or just carrying a policy nobody's role obeys?
posture = await inspect_rls_posture(conn, tables=["orders", "invoices"])
```

Two new `TenancySettings` fields, both `False` by default:

| Field | Env | Meaning |
|---|---|---|
| `rls_set_tenant` | `VARCO_TENANCY_RLS_SET_TENANT` | Install `install_rls_tenant_hook` automatically at DI wiring time |
| `rls_require_tenant` | `VARCO_TENANCY_RLS_REQUIRE_TENANT` | The hook raises instead of clearing the GUC when no tenant is ambient |

Full guide (ordering rule, nullable-tenant-column decision, pooler survival table, locking and
rollback, the `BYPASSRLS`/owner footgun): [Postgres RLS](technical_docs/features/postgres-rls.md).

### The tenant-filter guard (development-time)

`assert_tenant_predicate()` is a development-time assertion that a tenant-scoped query was built
with a tenant filter; **it is not a security control — Postgres RLS (above) is.** It walks
varco's query AST before any backend compiles it and raises `TenantFilterError` unless an
equality comparison on the tenant field sits on the top-level `AND` spine:

```python
from varco_core.tenancy.settings import TenancySettings

# VARCO_TENANCY_ASSERT_TENANT_FILTER=true — off by default, on both backends.
TenancySettings(assert_tenant_filter=True)
```

It has exactly one false-negative class, and it is documented rather than claimed: a query that
never builds a `QueryParams` AST — raw `session.execute(text(...))`, a hand-written `Select`,
`get(pk)`, a Mongo aggregation pipeline — passes unguarded. Use it to catch a forgotten filter
while writing a query; use RLS to stop one reaching the database.

---

## Tenant identity provenance

`TenantSourceChain` (`varco_core.tenancy.source`) answers a question the section above assumes
is already settled: **which of a request's signals — a header, a subdomain, a JWT claim — is
allowed to name the tenant, and what happens when two disagree?** Nothing flips by default —
`TenantResolutionMiddleware(chain=None)` keeps reading `X-Tenant-Id`, byte-identically. Full
design (trust ranking, the two cross-check modes, the subdomain algorithm, membership binding,
act-as delegation, the 4.0 flip list, a Pitfalls table):
`technical_docs/features/tenant-provenance.md`.

```python
from varco_core.tenancy.settings import TenantProvenanceSettings, build_tenant_source_chain
from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware

# Env-driven — see the table below. VARCO_TENANT_SOURCES unset -> build_tenant_source_chain()
# returns None -> chain=None below -> byte-identical to pre-3.2 header-only behaviour.
chain = build_tenant_source_chain(TenantProvenanceSettings.from_env())

app.add_middleware(
    TenantResolutionMiddleware,
    catalog=catalog,
    pool=pool,
    chain=chain,               # None (default) or a TenantSourceChain
    server_auth=server_auth,   # needed for a JWT-claim source to see a verified AuthContext
    reject_status=403,
)
```

Or build a chain explicitly, in code:

```python
from varco_core.tenancy.source import CrossCheckMode, TenantSourceChain
from varco_core.tenancy.sources import JwtClaimTenantSource, SubdomainTenantSource

chain = TenantSourceChain(
    sources=(
        JwtClaimTenantSource(),
        SubdomainTenantSource(base_domains=("example.com",)),
    ),
    mode=CrossCheckMode.LENIENT,  # STRICT rejects a lone claim too — see the feature doc
)
```

⛔ `SubdomainTenantSource` is not by itself a security control; deploy it behind
`TrustedHostMiddleware` (`allowed_hosts=[...]`, Starlette's own) or an edge that rejects unknown
`Host` values.

**Membership binding** — is the authenticated subject allowed to act as the tenant the chain
resolved? Opt in via the same `enable_*` shape as `varco_casbin.enable_policy_authorizer`:

```python
from varco_core.tenancy.di import enable_tenant_membership

container = bootstrap(DIContainer())
enable_tenant_membership(container)  # opt-in: ClaimTenantMembership over the scanned Null default
```

**Act-as / RFC 8693 delegation** — varco **consumes** an already-issued, already-verified
delegation token; it never issues one (token exchange is the IdP's job). `ActAsTenantSource`
emits a claim only when the token carries an `act` claim *and* a bound `DelegationPolicy` allows
the requested tenant:

```python
from varco_core.auth.delegation import AllowlistDelegationPolicy
from varco_core.tenancy.sources import ActAsTenantSource

policy = AllowlistDelegationPolicy(grants={"svc-billing": frozenset({"acme"})})
act_as_source = ActAsTenantSource(policy)  # None => deny-by-default, no claim ever
```

If your IdP has no RFC 8693 token-exchange endpoint, build the internal issuer yourself — no
`JwtBuilder` change is needed:

```python
token = authority.sign(
    authority.token().subject("acme-user").claim("act", {"sub": "svc-billing"})
)
```

### Env vars (`VARCO_TENANT_*`)

All read by `TenantProvenanceSettings.from_env()`. Every one is unset by default; an unset
`VARCO_TENANT_SOURCES` means `build_tenant_source_chain()` returns `None` — nothing changes.

| Var | Default | Meaning |
|---|---|---|
| `VARCO_TENANT_SOURCES` | *(unset)* | Ordered, comma-separated: `jwt`, `subdomain`, `legacy`, `act_as`. Unset ⇒ no chain |
| `VARCO_TENANT_CROSS_CHECK` | `lenient` | `lenient` \| `strict` |
| `VARCO_TENANT_MIN_TRUST` | `low` | `low` \| `medium` \| `high` \| `highest` — claims below the floor are discarded |
| `VARCO_TENANT_CLAIM_METADATA_KEY` | `tenant_id` | Key in `AuthContext.metadata` the JWT source reads |
| `VARCO_TENANT_BASE_DOMAINS` | *(unset)* | Comma-separated. **Required** when `subdomain` is in the chain |
| `VARCO_TENANT_TRUST_FORWARDED_HOST` | `false` | Read `X-Forwarded-Host`; the claim drops to `MEDIUM` trust |
| `VARCO_TENANT_FORWARDED_HOST_HEADER` | `X-Forwarded-Host` | |
| `VARCO_TENANT_RESERVED_LABELS` | `www,api,app,admin,static,cdn` | Subdomain labels that are never a tenant |
| `VARCO_TENANT_LEGACY_HEADER` | `X-Tenant-Id` | Header the legacy source reads |
| `VARCO_TENANT_MEMBERSHIP` | *(unset)* | `null` \| `claim`. Unset ⇒ `NullTenantMembership` |
| `VARCO_TENANT_MEMBERSHIP_CLAIM` | `tenants` | `AuthContext.metadata` key holding the membership list |
| `VARCO_TENANT_MEMBERSHIP_ON_MISSING` | `allow` | `allow` \| `deny`. **Flips to `deny` in 4.0** |
| `VARCO_TENANT_ACT_AS_HEADER` | `X-Act-As-Tenant` | Act-as / RFC 8693 delegation only |
| `VARCO_JWT_TRANSFORM_TENANTS_FIELD` | *(unset)* | Foreign name for the membership-list claim; the per-issuer `VARCO_JWT_TRANSFORM__<LABEL>__TENANTS_FIELD` form also exists |

---

## Query System

### QueryBuilder

Fluent, immutable builder — every method returns a new instance:

```python
from varco_core import QueryBuilder, QueryParams, SortField, SortOrder

# Simple equality filter
params = QueryParams(node=QueryBuilder().eq("active", True).build(), limit=10)

# Compound filter
node = QueryBuilder().eq("active", True).gte("age", 18).like("name", "Alice%").build()

# OR / NOT / IN / NULL
adult_or_admin = QueryBuilder().gte("age", 18).or_(QueryBuilder().eq("role", "admin")).build()
not_banned = QueryBuilder().eq("banned", True).not_().build()
status_filter = QueryBuilder().in_("status", ["active", "trial"]).build()
unverified = QueryBuilder().is_null("verified_at").build()
```

| Method | SQL equivalent |
|---|---|
| `.eq(field, value)` | `field = value` |
| `.ne(field, value)` | `field != value` |
| `.gt / .gte / .lt / .lte` | `> / >= / < / <=` |
| `.like(field, pattern)` | `field LIKE pattern` |
| `.in_(field, values)` | `field IN (values)` |
| `.is_null(field)` | `field IS NULL` |
| `.is_not_null(field)` | `field IS NOT NULL` |
| `.and_(other)` | `... AND ...` |
| `.or_(other)` | `... OR ...` |
| `.not_()` | `NOT (...)` |

### QueryParams

```python
params = QueryParams(
    node=QueryBuilder().eq("published", True).build(),
    sort=[
        SortField("created_at", SortOrder.DESC),
        SortField("title", SortOrder.ASC),
    ],
    limit=20,
    offset=40,  # page 3 of 20
)
```

**Worked example — add filtering to a list endpoint end-to-end:**

```python
# 1. HTTP layer receives filter strings (e.g., ?age__gte=18&status__eq=active)
from varco_core.query import QueryParams

params = QueryParams(
    filters=request.query_params.getlist("filter"),  # ["age__gte=18", "status__eq=active"]
    sort=request.query_params.getlist("sort"),  # ["+created_at"]
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

### QueryParser

```python
from varco_core import QueryParser

parser = QueryParser()
node = parser.parse('status = "active" AND age >= 18')
params = QueryParams(node=node, limit=50)
```

Grammar supports: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `IN`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`.

### Type Coercion

```python
from varco_core.query.visitor.type_coercion import (
    coerce_int,
    coerce_float,
    coerce_boolean,
    coerce_datetime,
    coerce_list,
    TypeCoercionRegistry,
    ASTTypeCoercion,
    register_default_coercer,
)

coerce_int("42")  # 42
coerce_boolean("yes")  # True
coerce_datetime("2024-01-15T10:30:00Z")  # datetime(...)
coerce_list('["a","b"]')  # ['a', 'b']
coerce_list("a,b,c")  # ['a', 'b', 'c']

registry = TypeCoercionRegistry()
registry.register("age", int, coerce_int)
registry.register("created_at", datetime, coerce_datetime)
coerced_ast = ASTTypeCoercion(registry).visit(parsed_ast)
```

---

## Event System

varco includes a general-purpose async event system.  `varco_core` provides
the in-process `InMemoryEventBus`; `varco_kafka` and `varco_redis` provide
distributed backends.

### Layer map

```
User code (services, handlers)
  ↓ depends on
AbstractEventProducer  /  EventConsumer + @listen
  ↓ delegates to
AbstractEventBus
  ↓ implemented by
InMemoryEventBus   (varco_core)
KafkaEventBus      (varco_kafka)
RedisEventBus      (varco_redis)
```

---

### Event base class

All events inherit from `Event` — a Pydantic frozen model.  Auto-generated
`event_id` (UUID4) and `timestamp` (UTC) are injected at construction time.

Declare an optional `__event_type__` class variable for stable serialization
across deployments (otherwise the class name is used):

```python
from varco_core.event import Event


class OrderPlacedEvent(Event):
    __event_type__ = "order.placed"  # stable cross-process identifier
    order_id: str
    total: float
```

Every `Event` subclass is automatically registered in `Event._registry`
(via `__init_subclass__`) — no manual registration required.

---

### AbstractEventBus

The low-level interface.  User code should **not** depend on it directly —
use `AbstractEventProducer` for publishing and `EventConsumer` for consuming.

```python
class AbstractEventBus(ABC):
    @abstractmethod
    async def publish(self, event: Event, *, channel: str = "default") -> asyncio.Task | None: ...

    @abstractmethod
    def subscribe(
        self,
        event_type: type[Event] | str,
        handler: Callable,
        *,
        channel: str = "*",
        filter: Callable | None = None,
        priority: int = 0,
    ) -> Subscription: ...
```

**Subscription dispatch rules** — a handler is called when ALL match:

1. `event_type` — `isinstance` check (supports inheritance) or `__event_type__` string.
2. `channel` — subscriber's channel is `"*"` (wildcard) or equals the publish channel.
3. `filter` — the optional predicate returns `True`.

---

### InMemoryEventBus

Full-featured in-process bus.  Suitable for dev, test, and single-process apps.

```python
from varco_core.event import InMemoryEventBus, DispatchMode, ErrorPolicy

# Test mode — callers block until all handlers complete
bus = InMemoryEventBus()

# Production mode — publish() returns immediately; handlers run in background
bus = InMemoryEventBus(dispatch_mode=DispatchMode.BACKGROUND)

# Middleware + custom error policy
bus = InMemoryEventBus(
    error_policy=ErrorPolicy.FIRE_FORGET,
    middleware=[LoggingMiddleware()],
)
```

**Test utilities:**

```python
# emitted records every (event, channel) pair for assertion
event, channel = bus.emitted[0]
bus.clear_emitted()

# drain() waits for all background tasks (BACKGROUND mode only)
await bus.drain()
```

**NoopEventBus** — discards all events silently.  Useful in tests that don't
care about events and don't want to configure a real bus:

```python
from varco_core.event import NoopEventBus

bus = NoopEventBus()
```

---

### Producer — AbstractEventProducer

Services inject `AbstractEventProducer` and call `_produce()`.  The bus is
a complete implementation detail — services never touch it directly.

```python
from varco_core.event import AbstractEventProducer, BusEventProducer, NoopEventProducer

# Production — wraps a real bus
producer = BusEventProducer(bus)
await producer._produce(OrderPlacedEvent(...), channel="orders")
await producer._produce_many([(e1, "orders"), (e2, "payments")])

# Null Object — silently discards (default in AsyncService when no producer is injected)
producer = NoopEventProducer()
```

`AsyncService` accepts an optional producer via DI:

```python
from varco_core import AsyncService
from varco_core.event import BusEventProducer

class OrderService(AsyncService[...]):
    def __init__(self, ..., producer: Annotated[AbstractEventProducer, InjectMeta(optional=True)] = None):
        super().__init__(..., producer=producer)
```

Domain events (`EntityCreatedEvent`, `EntityUpdatedEvent`, `EntityDeletedEvent`)
are published **automatically** by `AsyncService` after each mutating operation
commits — no additional code required.

---

### Consumer — EventConsumer + @listen

`EventConsumer` is a base class (or mixin).  Decorate methods with `@listen`
and call `register_to(bus)` to subscribe:

```python
from varco_core.event import EventConsumer, listen


class NotificationConsumer(EventConsumer):
    @listen(OrderPlacedEvent, channel="orders")
    async def on_order_placed(self, event: OrderPlacedEvent) -> None:
        await self._send_email(event)

    @listen(OrderPlacedEvent, filter=lambda e: e.total > 1000, channel="orders")
    async def on_large_order(self, event: OrderPlacedEvent) -> None:
        await self._alert_team(event)


consumer = NotificationConsumer()
consumer.register_to(bus)
# or: bus.register_consumer(consumer)
```

**Stacking** — same method, multiple channels:

```python
@listen(OrderPlacedEvent, channel="orders")
@listen(OrderPlacedEvent, channel="audit")
async def on_placed(self, event: OrderPlacedEvent) -> None: ...
```

**Multiple event types** — one handler, multiple types:

```python
@listen(OrderPlacedEvent, OrderUpdatedEvent)
async def on_any_order(self, event: Event) -> None: ...
```

**Worked example — add a new event type and handler end-to-end:**

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
            await self._producer.produce(
                OrderShippedEvent(
                    order_id=order_id,
                    shipped_at=datetime.now(UTC),
                )
            )


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
container.scan("varco_kafka", recursive=True)  # discovers the Kafka bus @Singletons
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

---

### Priority

Higher `priority` values run first.  Equal priorities run in subscription
order (FIFO):

```python
bus.subscribe(OrderPlacedEvent, handler_a, priority=10)  # runs first
bus.subscribe(OrderPlacedEvent, handler_b, priority=0)  # runs second


# Same with @listen
@listen(OrderPlacedEvent, priority=10)
async def high_priority_handler(self, event): ...
```

---

### ErrorPolicy and DispatchMode

| `ErrorPolicy` | Behaviour |
|---|---|
| `COLLECT_ALL` (default) | All handlers run; errors collected and re-raised as `ExceptionGroup` |
| `FAIL_FAST` | First error stops dispatch immediately |
| `FIRE_FORGET` | Errors logged at WARNING, never propagated |

| `DispatchMode` | Behaviour |
|---|---|
| `SYNC` (default) | `publish()` blocks until all handlers complete; returns `None` |
| `BACKGROUND` | `publish()` returns `asyncio.Task` immediately; handlers run in background |

```python
# BACKGROUND — caller can optionally await the returned task
task = await bus.publish(event, channel="orders")
if task:
    await task  # optional: wait for all handlers to finish
```

---

### Event Middleware

ASGI-style middleware wraps the full dispatch pipeline:

```python
from varco_core.event import EventMiddleware


class LoggingMiddleware(EventMiddleware):
    async def __call__(self, event, channel, next):
        logger.info("→ %s on %s", type(event).__name__, channel)
        await next(event, channel)
        logger.info("✓ %s dispatched", type(event).__name__)


bus = InMemoryEventBus(middleware=[LoggingMiddleware()])
```

Middleware can also modify the event/channel or suppress dispatch (by not calling `next`).

---

### Domain events

`AsyncService` automatically emits entity lifecycle events after each
mutating operation.  Channel is derived from the entity class name
(lowercase): `Post` → `"post"`, `Order` → `"order"`.

```python
from varco_core.event import EntityEvent, EntityCreatedEvent, EntityUpdatedEvent, EntityDeletedEvent

# Subscribe to all Post events
bus.subscribe(EntityEvent, handler, channel="post")

# Subscribe to all creates across all entities
bus.subscribe(EntityCreatedEvent, handler)

# Subscribe to Post creates only
bus.subscribe(EntityCreatedEvent, handler, channel="post")
```

`EntityCreatedEvent` and `EntityUpdatedEvent` carry a `payload` dict — the
serialized ReadDTO at the time of the operation.  `EntityDeletedEvent` carries
only the `pk` (the entity no longer exists).

All entity events carry an optional `correlation_id` threaded from the active
`correlation_context()` automatically.

---

### JsonEventSerializer

Serialize any `Event` subclass to/from UTF-8 JSON bytes.  Used internally by
`KafkaEventBus` and `RedisEventBus`:

```python
from varco_core.event import JsonEventSerializer

data = JsonEventSerializer.serialize(event)
# → b'{"__event_type__": "order.placed", "event_id": "...", ...}'

restored = JsonEventSerializer.deserialize(data)
assert isinstance(restored, OrderPlacedEvent)
```

Deserialization looks up the event class in `Event._registry` — populated
automatically when the event module is imported.

---

### Kafka backend (varco_kafka)

```bash
pip install varco-kafka
```

```python
from varco_kafka import KafkaEventBus, KafkaConfig
from varco_core.event import BusEventProducer, EventConsumer, listen

config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="my-service",
    topic_prefix="prod.",  # optional — "prod.orders", "prod.payments"
    auto_offset_reset="latest",
)

async with KafkaEventBus(config) as bus:
    # Publisher
    producer = BusEventProducer(bus)
    await producer._produce(OrderPlacedEvent(order_id="1"), channel="orders")

    # Consumer
    class OrderConsumer(EventConsumer):
        @listen(OrderPlacedEvent, channel="orders")
        async def on_placed(self, event: OrderPlacedEvent) -> None: ...

    OrderConsumer().register_to(bus)
```

`KafkaEventBus.publish()` always returns `None` — Kafka delivery is inherently
asynchronous (broker-side).  The consumer loop runs as a background task
and dispatches received messages to local handlers using the same priority /
filter logic as `InMemoryEventBus`.

**Topic naming:** channel `"orders"` → topic `"prod.orders"` (with prefix `"prod."`).

---

### Redis backend (varco_redis)

```bash
pip install varco-redis
```

```python
from varco_redis import RedisEventBus, RedisConfig
from varco_core.event import BusEventProducer, EventConsumer, listen

config = RedisConfig(
    url="redis://localhost:6379/0",
    channel_prefix="prod:",  # optional — "prod:orders", "prod:payments"
)

async with RedisEventBus(config) as bus:
    # Publisher
    producer = BusEventProducer(bus)
    await producer._produce(OrderPlacedEvent(order_id="1"), channel="orders")

    # Consumer
    class OrderConsumer(EventConsumer):
        @listen(OrderPlacedEvent, channel="orders")
        async def on_placed(self, event: OrderPlacedEvent) -> None: ...

    OrderConsumer().register_to(bus)
```

`RedisEventBus` uses Redis Pub/Sub — **at-most-once** delivery.  Messages
published while no subscribers are connected are silently dropped.

Wildcard subscriptions (`channel="*"`) use `PSUBSCRIBE "*"` under the hood,
so a single CHANNEL_ALL handler receives events from all channels on that Redis instance.
Use `channel_prefix` to scope channels to your service.

---

## Transactional Outbox

Publishing events directly after a DB commit risks silent loss when the broker is unavailable. The outbox pattern solves this by persisting the event inside the **same DB transaction** as the domain entity, then relaying asynchronously.

```
DB transaction
  service.create(entity) ──► repo.save(entity)
                          ──► outbox_repo.save(OutboxEntry.from_event(event))

Background relay (OutboxRelay)
  get_pending() → bus.publish() → delete(entry)  ✓
```

### OutboxEntry

```python
from varco_core.service.outbox import OutboxEntry

entry = OutboxEntry.from_event(OrderPlacedEvent(order_id="1"), channel="orders")
# entry.entry_id    — UUID4
# entry.event_type  — "order.placed"
# entry.payload     — JSON bytes
# entry.channel     — "orders"
# entry.created_at  — UTC timestamp
```

### OutboxRepository (ABC)

```python
from varco_core.service.outbox import OutboxRepository


class SAOutboxRepository(OutboxRepository):
    async def save(self, entry: OutboxEntry) -> None: ...
    async def get_pending(self, limit: int = 100) -> list[OutboxEntry]: ...
    async def delete(self, entry_id: UUID) -> None: ...
```

`varco_sa` and `varco_beanie` each ship a concrete `SAOutboxRepository` / `BeanieOutboxRepository` — use those and skip the manual implementation.

### OutboxRelay

```python
from varco_core.service.outbox import OutboxRelay

relay = OutboxRelay(
    outbox_repo=sa_outbox_repo,
    bus=event_bus,
    poll_interval=5.0,  # seconds between polls
)

# Start the background loop (typically in the app lifespan)
await relay.start()
# ...
await relay.stop()
```

`OutboxRelay` is the **only** place allowed to call `AbstractEventBus` directly — all other application code must go through `AbstractEventProducer`.

---

## SQLAlchemy Backend

### Installation

```
pip install varco-sa
```

### Bootstrap (one-liner setup)

`SAFastrestApp` wires engine, ORM generation, and UoW provider in one place:

```python
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from varco_sa import SAConfig, SAFastrestApp


class Base(DeclarativeBase):
    pass


engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")

app = SAFastrestApp(
    SAConfig(
        engine=engine,
        base=Base,
        entity_classes=(User, Post, Subscription),
    )
)

await app.create_all()  # CREATE TABLE IF NOT EXISTS ...
uow_provider = app.uow_provider  # inject this into services
```

Manual setup (if you need more control):

```python
from varco_sa import SQLAlchemyRepositoryProvider

provider = SQLAlchemyRepositoryProvider(engine=engine, session_factory=session_factory)
provider.register(User, Post, Subscription)
await provider.create_all()

async with provider.make_uow() as uow:
    user = await uow.users.save(User(name="Alice", email="alice@example.com"))
    posts = await uow.posts.find_by_query(QueryParams(limit=20))
```

### Alembic helpers

Use varco-generated metadata in your Alembic `env.py` without duplicating table definitions:

```python
# alembic/env.py
from varco_sa import get_target_metadata
from myapp.models import User, Post, Subscription

target_metadata = get_target_metadata(User, Post, Subscription)
```

Preview the DDL before running a migration:

```python
from varco_sa import print_create_ddl

ddl = print_create_ddl(User, Post, dialect="postgresql")
print(ddl)
# CREATE TABLE users (
#     id SERIAL PRIMARY KEY,
#     name VARCHAR(100) NOT NULL,
#     ...
# );
```

### Schema Guard

Detect drift between the generated ORM metadata and the actual live database:

```python
from varco_sa import SchemaGuard

guard = SchemaGuard(engine, provider.metadata)
report = await guard.check()

if report.has_drift:
    for drift in report.drifts:
        print(drift)  # "Column 'users.phone' missing from database"
```

### Repository interface (SQLAlchemy)

```python
async with provider.make_uow() as uow:
    # Existence check — SELECT COUNT(*) via identity map cache
    if await uow.posts.exists(post_id):
        ...

    # Stream with server-side cursor — constant memory regardless of size
    params = QueryParams(node=QueryBuilder().eq("active", True).build())
    async for post in uow.posts.stream_by_query(params):
        await process(post)
```

---

## Schema Migrations

One backend-agnostic contract (`varco_core.migration.AbstractMigrator`), two engines
(Alembic for Postgres, a versioned runner for MongoDB), an opt-in ASGI lifespan
component, and a `varco` CLI.

```bash
pip install "varco-sa[migrations]"
```

```python
from varco_sa.migration import AlembicMigrator
from varco_fastapi import create_varco_app

migrator = AlembicMigrator(engine, script_location="alembic")
app = create_varco_app(container, routers=[...], migrations=migrator)
```

```bash
VARCO_MIGRATE_MODE=check uvicorn myapp:app     # refuse to serve against a stale schema
```

`VARCO_MIGRATE_MODE` has three values, and it defaults to `off` — with `migrations=None`
nothing is registered and nothing changes:

| mode | At startup | Use it for |
|---|---|---|
| `off` (default) | nothing | migrations run out-of-band already |
| `check` | fail startup if the schema is behind; never writes DDL | **the recommended production posture** |
| `upgrade` | lock → apply pending revisions → release | single-instance, dev, PaaS without a pre-deploy hook |

Multi-pod exclusion is a Postgres advisory lock held open in its own transaction across
Alembic's — released by `COMMIT` and by process death, so there is no TTL to size.
`varco_sa` ships its own Alembic branch inside the wheel, so `pip install -U varco-sa`
brings framework schema changes with it (hence `upgrade heads`, plural). MongoDB index
reconciliation defaults to report-only even in `upgrade` mode.

### The `varco` CLI

`varco_core` now installs a `varco` console script; backends contribute verbs through the
`varco.commands` entry-point group.

```bash
varco migrate pending   -t myapp.db:migrator     # exit 1 if behind → CI gate
varco migrate upgrade   -t myapp.db:migrator     # the pre-deploy-job path
varco migrate adopt     -t myapp.db:migrator     # one-time ensure_table() bridge
varco migrate index     -t myapp.db:migrator --create    # MongoDB, opt-in

# Plan 009 — reliability & service integration
varco dlq list           -t module:factory [--channel C] [--source S] [--limit N]
varco dlq redrive        -t module:factory -b module:factory \
                          (--entry-id UUID | --batch [--limit N]) [--dry-run]
varco dlq purge          -t module:factory --before ISO8601 [--limit N]
varco retention prune    --type {dlq,audit} --before ISO8601 [--limit N] [--chunk N] [--dry-run] \
                          -t module:factory   # or VARCO_RETENTION_TARGET
varco export-contract    myapp.routers:OrderRouter [-o order.contract.json] [--strict]
varco gen-client         -c order.contract.json -o order_client.py [--class-name OrderClient]
varco gen-client-stubs   (myapp.routers:OrderRouter | -c order.contract.json) -o client.pyi [--check]
```

Full guides: [`technical_docs/features/schema-migrations.md`](technical_docs/features/schema-migrations.md),
[`technical_docs/features/dead-letter-queues.md`](technical_docs/features/dead-letter-queues.md),
[`technical_docs/features/portable-contracts.md`](technical_docs/features/portable-contracts.md).

---

## Beanie Backend

### Installation

```
pip install varco-beanie
```

### Bootstrap (Beanie)

```python
from motor.motor_asyncio import AsyncIOMotorClient
from varco_beanie import BeanieFastrestApp, BeanieSettings

client = AsyncIOMotorClient("mongodb://localhost:27017")

app = BeanieFastrestApp(
    BeanieSettings(
        motor_client=client,
        db_name="mydb",
        entity_classes=(User, Post),
    )
)

await app.init()  # calls beanie.init_beanie() internally
uow_provider = app.uow_provider  # inject into services
```

Manual setup:

```python
from varco_beanie import BeanieRepositoryProvider

provider = BeanieRepositoryProvider(motor_client=client, db_name="mydb")
provider.register(User, Post)
await provider.init()

async with provider.make_uow() as uow:
    user = await uow.users.save(User(name="Bob", email="bob@example.com"))
    recent = await uow.posts.find_by_query(
        QueryParams(
            node=QueryBuilder().eq("published", True).build(),
            sort=[SortField("created_at", SortOrder.DESC)],
            limit=10,
        )
    )
```

### Repository interface (Beanie)

```python
async with provider.make_uow() as uow:
    # Lightweight existence check — uses .count(), no document load
    if await uow.posts.exists(post_id):
        ...

    # Stream — yields documents from the Motor cursor in internal batches
    async for post in uow.posts.stream_by_query(QueryParams()):
        await process(post)
```

### DI integration (Providify)

```python
from varco_beanie import BeanieModule, BeanieSettings, bind_repositories
from providify import DIContainer, Provider

container = DIContainer()


@Provider(singleton=True)
def settings() -> BeanieSettings:
    return BeanieSettings(motor_client=client, db_name="mydb", entity_classes=(User, Post))


container.provide(settings)
container.install(BeanieModule)
bind_repositories(container, User, Post)

user_repo = await container.aget(AsyncRepository[User])
```

---

## Exception Hierarchy

### Service exceptions

```python
from varco_core.exception.service import (
    ServiceException,
    ServiceNotFoundError,
    ServiceAuthorizationError,
    ServiceConflictError,
    ServiceValidationError,
)

try:
    result = await svc.get(pk, ctx)
except ServiceNotFoundError:
    ...  # HTTP 404
except ServiceAuthorizationError:
    ...  # HTTP 403
except ServiceConflictError:
    ...  # HTTP 409
except ServiceValidationError:
    ...  # HTTP 422
```

| Exception | HTTP | When raised |
|---|---|---|
| `ServiceNotFoundError` | 404 | Entity with requested pk does not exist |
| `ServiceAuthorizationError` | 403 | Caller lacks permission |
| `ServiceConflictError` | 409 | Uniqueness or business-rule violation |
| `ServiceValidationError` | 422 | Domain invariant violated by DTO |

### Query exceptions

| Exception | When raised |
|---|---|
| `OperationNotFound` | Unknown operator in a query string |
| `OperationNotSupported` | Dotted path fields or unsupported op |
| `CoercionError` | Type coercion failure |

### Repository exceptions

| Exception | When raised |
|---|---|
| `FieldNotFound` | Column not found during query compilation |
| `StaleEntityError` | Optimistic lock violation on `VersionedDomainModel` |

---

## Running tests

```bash
# All packages from the root
uv run pytest

# One package at a time
uv run pytest varco_core/tests/
uv run pytest varco_sa/tests/
uv run pytest varco_beanie/tests/
uv run pytest varco_kafka/tests/
uv run pytest varco_redis/tests/

# Integration tests (require Docker — Kafka, Redis, or MongoDB)
uv run pytest varco_kafka/tests/ -m integration
uv run pytest varco_redis/tests/ -m integration
```

`make test` (`scripts/unit_tests.sh`) runs the same per-package suites as CI's `unit` job —
all ten packages plus the `examples/00-full-stack-post-api` suite, one accumulated summary
rather than aborting on the first red package. `make lint` / `make type-check` use the same
`uv run ruff` / `uv run mypy` commands CI's `lint` job runs — never `uvx ruff` (see
[technical_docs/common-pitfalls.md](technical_docs/common-pitfalls.md)).

`make chaos-test` / `make chaos-test-clean` run the **chaos** suite — tests that kill, pause, or
restart a real container mid-test to assert guarantees `make integration-test` cannot: outbox
entries surviving a broker/database restart, a `CircuitBreaker` opening and recovering around a
black-holed dependency, and a job lease correctly fencing out a crashed worker. Excluded from
`make integration-test` by default (see CLAUDE.md's Test Conventions "Chaos tests" paragraph for
the full design).

### providify's pytest fixtures (providify ≥ 2.0.0)

Installing `providify` activates its own `pytest11` plugin — four function-scoped, yield-based
fixtures, available in every test with no import and no conftest change:

```python
def test_something(di_container):
    # di_container: a fresh, empty DIContainer, one per test
    di_container.scan("varco_core", recursive=True)
    ...


async def test_something_async(di_acontainer):
    # di_acontainer: the async counterpart — usable directly under
    # asyncio_mode = "auto"; container.ashutdown() is awaited at teardown
    ...


def test_with_override(di_container, di_overrides):
    # di_overrides: a ContainerOverrides bound to di_container — any
    # override made through it is undone automatically at teardown
    di_overrides.instance(SomeInterface, a_test_double)
    ...


def test_with_global(di_container, di_global):
    # di_global: makes DIContainer.current() return di_container for the
    # duration of this test, then restores the previous current()
    ...
```

varco deliberately does not re-export or wrap any of these four fixtures (Plan 016 / RL-3d) —
use providify's own names directly. A project's own `conftest.py` can redefine `di_container` (or
any of the other three) and that definition wins over the plugin default, same as any other
pytest fixture override. A test that requests none of them sees no behavioural difference from
providify's plugin not being installed at all.

---

## Cache System

`varco_core.cache` provides a backend-agnostic async cache framework with pluggable invalidation strategies. `varco_redis` ships a Redis-backed implementation. Cache hardening (`CachePolicy`) adds stampede protection / singleflight, stale-while-revalidate, TTL jitter, opt-in negative caching, a `LayeredCache` cross-node L1 coherence backplane, and a hit/miss/eviction observability pack — every default reproduces today's behaviour byte-for-byte; see [`technical_docs/features/cache-hardening.md`](technical_docs/features/cache-hardening.md).

### AsyncCache and CacheBackend

```
AsyncCache (Protocol)  ←  structural checks, type hints
  ↑
CacheBackend (ABC)     ←  inherit start/stop + async context manager
  ↑
InMemoryCache   NoOpCache   RedisCache (varco_redis)   LayeredCache
```

```python
from varco_core.cache import AsyncCache, CacheBackend, InMemoryCache, NoOpCache
```

### InMemoryCache

```python
from varco_core.cache import InMemoryCache, TTLStrategy

async with InMemoryCache(strategy=TTLStrategy(300)) as cache:
    await cache.set("user:42", {"name": "Alice"})
    user = await cache.get("user:42")  # None after 300 s
    await cache.delete("user:42")
    await cache.clear()
```

`NoOpCache` discards all writes silently — useful in tests:

```python
from varco_core.cache import NoOpCache

cache = NoOpCache()
```

### LayeredCache

Promotes L2 hits to L1 on read — reduces network round-trips:

```python
from varco_core.cache import InMemoryCache, LayeredCache, TTLStrategy
from varco_redis.cache import RedisCache, RedisCacheSettings

l1 = InMemoryCache(strategy=TTLStrategy(60))
l2 = RedisCache(RedisCacheSettings(url="redis://localhost:6379/0", key_prefix="app:"))

async with LayeredCache(l1, l2, promote_ttl=60) as cache:
    await cache.set("product:1", product, ttl=300)
    result = await cache.get("product:1")  # L2 hit → promoted to L1
    result = await cache.get("product:1")  # L1 hit (no Redis round-trip)
```

In a multi-pod deployment, pass `backplane=RedisPubSubBackplane()` (`varco_redis.backplane`) so a
write on one pod invalidates every other pod's L1 (`promote_ttl` becomes required when a backplane
is wired). See [`technical_docs/features/cache-hardening.md`](technical_docs/features/cache-hardening.md)
for the full design.

### Invalidation strategies

```python
from varco_core.cache import (
    TTLStrategy,  # time-based expiry
    ExplicitStrategy,  # manual invalidation via cache.delete()
    TaggedStrategy,  # bulk invalidation by tag
    EventDrivenStrategy,  # bus-event-triggered invalidation
    CompositeStrategy,  # logical OR of multiple strategies
)
```

**TTLStrategy** — expire entries after a fixed TTL:

```python
strategy = TTLStrategy(ttl_seconds=300)
```

**TaggedStrategy** — invalidate all keys sharing a tag:

```python
from varco_core.cache import TaggedStrategy

strategy = TaggedStrategy()
async with InMemoryCache(strategy=strategy) as cache:
    await cache.set("user:42", user, tags=["user:42", "tenant:acme"])
    await cache.invalidate_by_tag("tenant:acme")  # evicts every "tenant:acme" key
```

**EventDrivenStrategy** — listen on an event bus channel and evict on receipt:

```python
from varco_core.cache import EventDrivenStrategy

strategy = EventDrivenStrategy(bus, channel="cache-invalidations")
```

**CompositeStrategy** — apply multiple strategies; a key is evicted when any strategy fires:

```python
from varco_core.cache import CompositeStrategy, TTLStrategy, EventDrivenStrategy

strategy = CompositeStrategy(
    TTLStrategy(300),
    EventDrivenStrategy(bus, channel="cache-invalidations"),
)
async with InMemoryCache(strategy=strategy) as cache:
    ...
```

> **Rule**: never instantiate `InvalidationStrategy` outside its backend's `start()`/`stop()` lifecycle — strategies may hold subscriptions or background tasks.

### CacheServiceMixin

Add transparent look-aside caching to any `AsyncService` subclass:

```python
from varco_core.cache import CacheServiceMixin


@Singleton
class PostService(
    CacheServiceMixin,  # ← LEFT side so caching wraps all CRUD
    AsyncService[Post, UUID, PostCreate, PostRead, PostUpdate],
):
    _cache_backend = Inject[CacheBackend]  # injected from DI
    _cache_namespace = "posts"
    _cache_ttl = 300

    def _get_repo(self, uow):
        return uow.posts
```

`get()` results are cached automatically; `update()` and `delete()` evict the entry.

**Worked example — mix in caching + explicit invalidation on an event:**

```python
# 1. Choose invalidation strategy
from varco_core.cache import TTLStrategy, TaggedStrategy, CompositeStrategy


# 2. Mix in CacheServiceMixin (order matters in MRO!)
class UserService(
    CacheServiceMixin,  # ← LEFT side (runs first)
    TenantAwareService,
    AsyncService[User, UUID, UserCreateDTO, UserReadDTO, UserUpdateDTO],
):
    _cache_config = CacheConfig(
        backend=RedisCache(...),
        invalidation_strategy=CompositeStrategy(
            [
                TTLStrategy(ttl_seconds=300),
                TaggedStrategy(),
            ]
        ),
    )

    def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[User, UUID]:
        return uow.users


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

### @cached decorator

Cache any async callable independently of the service layer:

```python
from varco_core.cache import cached, InMemoryCache, TTLStrategy

cache = InMemoryCache(strategy=TTLStrategy(300))


@cached(cache=cache, key_fn=lambda self, user_id: f"profile:{user_id}", ttl=60)
async def get_user_profile(self, user_id: UUID) -> UserProfile:
    return await self._repo.find_by_id(user_id)
```

Pass `policy=CachePolicy(ttl=..., singleflight=True)` (or `singleflight=True` directly) for
stampede protection — N concurrent misses on the same key collapse into one recompute per process:

```python
from varco_core.cache import CachePolicy, cached


@cached(cache, policy=CachePolicy(ttl=300.0), singleflight=True, namespace="users")
async def get_user(user_id: int) -> dict:
    return await db.fetch_one("SELECT * FROM users WHERE id = $1", user_id)
```

### CachedService wrapper

Wrap any service in a cache layer without inheritance:

```python
from varco_core.cache import CachedService

cached_svc = CachedService(
    post_service,
    cache,
    namespace="posts",
    default_ttl=300,
    bus=event_bus,  # publish cross-process invalidation events
    bus_channel="posts.invalidations",
)

post = await cached_svc.get(post_id)  # cache miss → fetched, stored
posts = await cached_svc.list()  # cached list
await cached_svc.update(post_id, dto)  # evicts + publishes invalidation event
```

---

## Resilience

`varco_core.resilience` provides six composable resilience decorators for both sync and async callables.

### retry

Retries a failing function with exponential back-off and optional jitter:

```python
from varco_core.resilience import retry, RetryPolicy, RetryExhaustedError


@retry(
    RetryPolicy(
        max_attempts=3,
        base_delay=0.5,
        max_delay=10.0,
        jitter=True,
        retryable=(httpx.HTTPError, TimeoutError),
    )
)
async def call_api() -> Response: ...
```

| `RetryPolicy` field | Default | Description |
|---|---|---|
| `max_attempts` | `3` | Max total attempts (including first) |
| `base_delay` | `1.0` | Initial back-off in seconds |
| `max_delay` | `60.0` | Cap on back-off |
| `jitter` | `True` | Add random ±20 % jitter to delay |
| `retryable` | `(Exception,)` | Exception types that trigger a retry |

### circuit_breaker

Prevents cascading failures by stopping calls to a broken dependency:

```python
from varco_core.resilience import CircuitBreaker, CircuitBreakerConfig, circuit_breaker


# Decorator form — one breaker per decorated function
@circuit_breaker(
    CircuitBreakerConfig(
        failure_threshold=5,
        recovery_timeout=60.0,
        success_threshold=2,
    )
)
async def call_payment_api() -> None: ...


# Shared instance form — one breaker protecting multiple functions
_breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=5))


async def charge(amount: float) -> None:
    await _breaker.protect(call_payment_api)(amount)
```

> **Important**: `CircuitBreaker` **must** be a shared instance per external dependency — a per-call instance never accumulates enough failures to open.

States: `CLOSED` (normal) → `OPEN` (failing fast) → `HALF_OPEN` (probing recovery).

### timeout

Cancels an async call if it exceeds a time limit (async-only):

```python
from varco_core.resilience import timeout, CallTimeoutError


@timeout(10.0)  # seconds
async def fetch_data() -> bytes: ...


try:
    data = await fetch_data()
except CallTimeoutError:
    # call was cancelled after 10 s
    ...
```

### rate_limit

Caps calls per rolling time window (async-only):

```python
from varco_core.resilience import rate_limit, InMemoryRateLimiter, RateLimitConfig

# Shared limiter — one per external service
_limiter = InMemoryRateLimiter(RateLimitConfig(rate=100, period=1.0))


@rate_limit(limiter=_limiter)
async def send_notification(user_id: str) -> None: ...
```

> **Multi-pod**: `InMemoryRateLimiter` is per-process. Use `varco_redis.RedisRateLimiter` for distributed enforcement.

### bulkhead

Caps maximum concurrent in-flight calls (async-only):

```python
from varco_core.resilience import Bulkhead, BulkheadConfig, bulkhead, BulkheadFullError

# Shared bulkhead — one per external dependency
_db_bh = Bulkhead(BulkheadConfig(max_concurrent=10, max_wait=0.5))


@_db_bh.protect
async def heavy_db_query() -> list[Row]: ...


# Or as a decorator with a new shared instance:
@bulkhead(BulkheadConfig(max_concurrent=5))
async def call_slow_api() -> None: ...
```

> **Important**: same rule as `CircuitBreaker` — `Bulkhead` must be a **shared** instance, not per-call.

### hedge

Issues a speculative duplicate call after a delay to cut tail latency (async-only):

```python
from varco_core.resilience import hedge, HedgeConfig


@hedge(HedgeConfig(delay=0.1))  # fire second attempt after 100 ms
async def read_product(product_id: int) -> Product: ...
```

> **Warning**: Only use `@hedge` for **idempotent reads/upserts** — hedging a write causes duplicate side-effects (double charge, double email, etc.).

### Composing patterns

Decorators compose bottom-to-top (innermost executes first):

```python
from varco_core.resilience import timeout, retry, circuit_breaker, RetryPolicy, CircuitBreakerConfig


# Execution order: circuit_breaker → retry loop → timeout → actual call
@timeout(10.0)
@retry(RetryPolicy(max_attempts=3, base_delay=0.5))
@circuit_breaker(CircuitBreakerConfig(failure_threshold=5, recovery_timeout=60.0))
async def call_external_api(payload: dict) -> Response: ...
```

**Worked example — integrate a new external API with resilience:**

```python
from varco_core.resilience import (
    retry,
    timeout,
    circuit_breaker,
    rate_limit,
    bulkhead,
    RetryPolicy,
    CircuitBreakerConfig,
    RateLimitConfig,
    BulkheadConfig,
    InMemoryRateLimiter,
)

# Shared instances — one per external dependency (NOT per-call)
_payment_limiter = InMemoryRateLimiter(RateLimitConfig(rate=100, period=1.0))
_payment_bulkhead = Bulkhead(BulkheadConfig(max_concurrent=10, max_wait=0.5))


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

**Rate limiting**: Use `@rate_limit` to cap calls per second. `InMemoryRateLimiter` is per-process; use `varco_redis.RedisRateLimiter` in multi-pod deployments.

**Bulkhead**: Use `Bulkhead` to cap concurrent in-flight calls to one dependency. Must be a **shared** instance (same rule as `CircuitBreaker`). `Bulkhead` is per-process, same limitation as `InMemoryRateLimiter` — use `varco_redis.RedisBulkhead` for fleet-wide concurrency limiting in multi-pod deployments.

**Hedged requests**: Use `@hedge` only for idempotent reads to cut tail latency — never for writes.

---

## JWT / Authority System

`varco_core.authority` provides JWT signing, rotation, and multi-issuer verification.

### JwtAuthority — signing

```python
from varco_core.authority import JwtAuthority
from datetime import timedelta

# Load from a PEM private key
authority = JwtAuthority.from_pem(
    pem_bytes,
    kid="svc:auth-v1",
    issuer="my-service",
    algorithm="RS256",  # RS256, ES256, or HS256
)

# Build and sign a token
token_str = authority.sign(
    authority.token()
    .subject("usr_42")
    .expires_in(timedelta(hours=1))
    .claim("roles", ["editor"])
    .claim("tenant_id", "acme")
)

# Verify and decode (same authority — single-issuer setup)
payload = authority.verify(token_str)
# payload.sub, payload.iss, payload.exp, payload["roles"], ...
```

### MultiKeyAuthority — key rotation

Zero-downtime key rotation: new tokens use the new key; old tokens remain valid until expiry.

```python
from varco_core.authority import JwtAuthority, MultiKeyAuthority

# Initial key
authority_v1 = JwtAuthority.from_pem(pem_v1, kid="svc:v1", issuer="my-svc", algorithm="RS256")
multi = MultiKeyAuthority(authority_v1)

# Rotate — start signing with the new key
authority_v2 = JwtAuthority.from_pem(pem_v2, kid="svc:v2", issuer="my-svc", algorithm="RS256")
multi.rotate(authority_v2)

# Retire old key only after all tokens signed with svc:v1 have expired
multi.retire("svc:v1")
```

### TrustedIssuerRegistry — verification

Verify tokens from multiple trusted issuers (e.g. your own service + an external IdP):

```python
from varco_core.authority import TrustedIssuerRegistry, AuthorizationConfig

# Load from environment variables
registry = TrustedIssuerRegistry.from_env()
await registry.load_all()  # fetches JWKS / PEM files for each issuer

# Verify any token — registry finds the right issuer by `iss` claim
payload = await registry.verify(raw_token_string)
# raises IssuerNotFoundError or AuthorityError on failure

# From explicit config
config = AuthorizationConfig(
    issuers=[
        IssuerConfig(issuer="my-svc", source="pem_file", path="/etc/keys/svc.pub"),
        IssuerConfig(issuer="google", source="oidc", discovery_url="https://accounts.google.com"),
        IssuerConfig(
            issuer="corporate",
            source="jwks_url",
            jwks_url="https://auth.corp.internal/.well-known/jwks.json",
        ),
    ]
)
registry = config.to_registry()
await registry.load_all()
```

### JWKS background refresh (Plan 041 / S22)

`TrustedIssuerRegistry` can refresh its JWKS caches on a timer, without ever
waiting for a `verify()` call:

```python
from varco_core.authority import TrustedIssuerRegistry
from varco_fastapi import JwksRefreshLifecycle, create_varco_app

registry = TrustedIssuerRegistry.from_env()
await registry.load_all()  # unchanged — the documented startup step

app = create_varco_app(
    ...,
    jwks_refresh=JwksRefreshLifecycle(registry),  # None (default) = off
)
```

Off by default — `VARCO_JWKS_TTL_SECONDS` (default `0.0`) also sets the
background refresher's period; `0.0` means the refresher never starts. See
`technical_docs/features/jwt-claim-transformer.md`'s "JWKS caching knobs, and
the background refresher" section for the full design, and
`varco_core.authority.inspect_jwks_posture()` to check whether one is
actually running.

### Key sources

| Source class | Import | Description |
|---|---|---|
| `PemFileSource` | `varco_core.authority.sources` | Load public key from a `.pem` file on disk |
| `PemFolderSource` | `varco_core.authority.sources` | Watch a folder; auto-reload on new files |
| `JwksUrlSource` | `varco_core.authority.sources` | Fetch JWKS JSON from a URL |
| `OidcDiscoverySource` | `varco_core.authority.sources` | OIDC discovery endpoint → JWKS |

### Environment variable config

`TrustedIssuerRegistry.from_env()` reads a JSON array from `VARCO_TRUSTED_ISSUERS`:

```bash
VARCO_TRUSTED_ISSUERS='[
  {"issuer": "my-svc",  "source": "pem_file",  "path": "/etc/keys/svc.pub"},
  {"issuer": "google",  "source": "oidc",       "discovery_url": "https://accounts.google.com"}
]'
```

### Verification hardening (VARCO_JWT_\*)

`varco_core.jwt.config.JwtVerificationSettings` — env-var reference:

| Env var | Default | Effect |
|---|---|---|
| `VARCO_JWT_LEEWAY_SECONDS` | `0.0` | clock-skew leeway for `exp`/`nbf` checks — fixes intermittent cross-host 401s |
| `VARCO_JWT_AUDIENCE` | `None` | this service's expected `aud` — **required** unless `VARCO_JWT_ALLOW_ANY_AUDIENCE=true` (Plan 005 Phase 2 / U-13, **BREAKING security default**: `JwtBearerAuth` now *refuses to construct* with `ValueError` when omitted, instead of logging a warning and proceeding) |
| `VARCO_JWT_ALLOW_ANY_AUDIENCE` | `false` | named escape hatch restoring the pre-Phase-2 "warn once and proceed with no audience enforcement" behaviour |
| `VARCO_JWT_ENFORCE_ISS` | `true` | whether `TrustedIssuerRegistry.verify()` checks the token's `iss` claim against the resolved issuer's registered value (Plan 005 Phase 2 / U-13, **BREAKING security default**: previously never enforced here) |
| `VARCO_JWT_TRANSFORM_*` | — | claim-transform mapping (global) — see the claim-transformer feature doc |
| `VARCO_JWT_TRANSFORM__<LABEL>__*` | — | per-issuer claim-transform override, keyed by `iss` |
| `VARCO_JWT_PROFILE__<NAME>__*` | — | named token profile declaration |
| `VARCO_JWKS_MIN_REFRESH_SECONDS` | `10.0` | rate limit between kid-miss JWKS refreshes; also the floor a background refresher's period is clamped up to (Plan 041 / S22) |
| `VARCO_JWKS_TTL_SECONDS` | `0.0` | proactive JWKS reload age threshold (`0` = disabled); also the background refresher's tick period when `interval=` is not passed explicitly (Plan 041 / S22) — `0` means the refresher never starts |

**Common pitfalls:**

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **Token from another service accepted** | Since Plan 005 Phase 2: `JwtBearerAuth()` **fails to start** (`ValueError`) unless `audience=`/`VARCO_JWT_AUDIENCE`/`allow_any_audience=True` is set — this used to be a silent accept | `aud` was never enforced by default — now fails closed instead of warning | Set `VARCO_JWT_AUDIENCE` (or `JwtBearerAuth(audience=...)`), or explicitly opt out with `allow_any_audience=True` / `VARCO_JWT_ALLOW_ANY_AUDIENCE=true` |
| **Forged/misrouted `iss` claim accepted** | A token signed by issuer A's key but claiming `iss` of issuer B used to verify successfully | `TrustedIssuerRegistry.verify()` never checked `iss` against the resolving issuer | Since Plan 005 Phase 2 this is enforced by default (`VARCO_JWT_ENFORCE_ISS=true`); opt out per-call with `verify(enforce_issuer=False)` only if you have a specific reason |
| **Intermittent 401 across hosts** | Same token, same secret, fails verification only on some hosts/some requests | Clock skew between hosts — `exp`/`nbf` checked with zero tolerance by default | Set `VARCO_JWT_LEEWAY_SECONDS=30` (or `leeway=` on `parse()`/`verify()`) |

**Worked example — consume a foreign-shaped JWT (Keycloak/Cognito):**

An IdP-minted token rarely uses varco's canonical claim names. Set env vars — **no code
changes** — and every JWT entry point (`JwtParser.parse()`, `TrustedIssuerRegistry.verify()`,
`JwtBearerAuth`, `PassthroughAuth`) picks up the mapping for free:

```bash
# Keycloak: roles live under realm_access.roles, Spring-style "ROLE_" prefix
export VARCO_JWT_TRANSFORM_ROLES_FIELD="realm_access.roles"
export VARCO_JWT_TRANSFORM_ROLES_STRIP_PREFIX="ROLE_"
export VARCO_JWT_TRANSFORM_TOKEN_TYPE_FIELD="typ"

# Cognito: groups + token_use instead of roles/token_type
export VARCO_JWT_TRANSFORM_ROLES_FIELD="cognito:groups"
export VARCO_JWT_TRANSFORM_TOKEN_TYPE_FIELD="token_use"
```

```python
from varco_core.jwt import JwtParser

token = JwtParser.parse(raw_token, secret, algorithms=["HS256"])  # algorithms= required (Plan 034 / S1)
token.auth_ctx.roles  # populated from the foreign claim
token.extra_claims["realm_access"]  # original claim still visible (non-destructive)
```

For per-issuer overrides (mixed fleets, gateway forwarding tokens from several IdPs), a code
escape hatch (`ClaimMapping` / a custom `ClaimTransformer`), and the full env-var table, see
`technical_docs/features/jwt-claim-transformer.md`.

**Worked example — gate a route on a named token profile (replacing `SYSTEM_ISSUER`):**

Instead of a single `JwtUtil.SYSTEM_ISSUER` value, declare one or more named profiles and
gate routes on the resolved profile:

```bash
export VARCO_JWT_PROFILE__INTERNAL__ISS="mesh-signer"
export VARCO_JWT_PROFILE__INTERNAL__TOKEN_TYPE="system"
export VARCO_JWT_PROFILE__INTERNAL__ROLES="internal"
```

```python
from varco_fastapi.router.presets import GenericRouter
from varco_fastapi.router.endpoint import route
from varco_fastapi.auth import JwtBearerAuth
from varco_fastapi.auth.guard import require_token_profile


class MeshRouter(GenericRouter):
    _prefix = "/mesh"
    _auth = JwtBearerAuth(registry)

    @route("GET", "/internal-only", requires=require_token_profile("internal"))
    async def internal_only(self, ctx) -> dict:
        return {"ok": True}
```

A bare `sub`+`iss` service-mesh token with no roles/scopes/grants still gets a fully
authorized `AuthContext` when it matches a profile declaring `implied_roles`/`implied_scopes`
(⚠️ intentional materialisation — see `technical_docs/features/token-profiles.md`).
`JwtUtil.SYSTEM_ISSUER`/`is_system()` keep working unchanged for callers not ready to migrate.

---

## Connection Settings

`varco_core.connection` provides structured, env-var-loadable config objects for every backend. `SSLConfig` is shared across all of them.

### SSLConfig

```python
from varco_core.connection import SSLConfig
from pathlib import Path

# TLS with custom CA
ssl = SSLConfig(ca_cert=Path("/etc/ssl/ca.pem"), verify=True)

# mTLS (client certificates)
ssl = SSLConfig(
    ca_cert=Path("/etc/ssl/ca.pem"),
    client_cert=Path("/etc/ssl/client.crt"),
    client_key=Path("/etc/ssl/client.key"),
)

# Disable verification (dev / testing only)
ssl = SSLConfig(verify=False, check_hostname=False)
```

### TLS trust store

`varco_core.tls.TrustStore` (Plan 026) is a strict superset of `SSLConfig` and unifies TLS
trust configuration across every backend — recursive, multi-folder CA search, `include_system_cas`,
in-memory `bytes` CAs, and mTLS, in one type reachable from `varco_kafka`/`varco_redis`/`varco_sa`
as well as `varco_fastapi`. Full design (mutate-vs-swap reload strategy, the deprecation shim,
env-var reference): `technical_docs/features/tls-trust-and-hot-reload.md`.

```python
from pathlib import Path
from varco_core.tls import TrustStore

# Private CA, recursive folder scan (the default on this type)
store = TrustStore(ca_folders=Path("/etc/ssl/private-ca"))
ctx = store.build_ssl_context()

# From environment variables — VARCO_TRUST_STORE_DIR / VARCO_CA_CERT / VARCO_CLIENT_CERT /
# VARCO_CLIENT_KEY, plus SSL_CERT_FILE / SSL_CERT_DIR (additive — see the table below)
store = TrustStore.from_env()
```

**Bridge with `SSLConfig`** (lossless — carries both `verify` and `check_hostname`, unlike
`HttpConnectionSettings.to_trust_store()`, which keeps returning the deprecated
`varco_fastapi.auth.TrustStore` and drops `verify=False`):

```python
from varco_core.connection.ssl import SSLConfig

store = SSLConfig(verify=False, check_hostname=False).to_trust_store()
cfg = store.to_ssl_config()  # lossy in the other direction — see the feature doc
```

**Hot reload** — `ReloadingTrustStore` composes `varco_core.watch`/`varco_core.reload` (see
["File watching and hot reload"](#file-watching-and-hot-reload)) to pick up certificate rotation
without a process restart:

```python
from varco_core.tls import ReloadingTrustStore, bind_trust_store
from varco_fastapi.lifespan import VarcoLifespan

store = ReloadingTrustStore(TrustStore(ca_folders=Path("/etc/ssl/private-ca")))

lifespan = VarcoLifespan()
lifespan.register(store)  # starts/stops the watcher for you

# DI: makes an already-constructed, already-owned store resolvable — no lifecycle side effect
bind_trust_store(container, store)
```

**`ssl_context=` on the JWT issuer sources** (Plan 026 / T5) — point `JwksUrlSource`/
`OidcDiscoverySource` at an internal PKI:

```python
from varco_core.authority.sources import JwksUrlSource

ctx = TrustStore(ca_folders=Path("/etc/ssl/internal-pki")).build_ssl_context()
jwks = JwksUrlSource("https://idp.internal/.well-known/jwks.json", ssl_context=ctx)
```

⚠️ `ssl_context=` fixes certificate *verification*, not proxy handling — `HTTP_PROXY`/
`HTTPS_PROXY` still apply exactly as before. This does not make "JWKS through a proxy" work; it
was already unaffected either way.

| Env var | Effect | Additive? |
|---|---|---|
| `VARCO_TRUST_STORE_DIR` | entry in `ca_folders` | ✅ |
| `VARCO_CA_CERT` | `ca_cert` | ✅ |
| `VARCO_CLIENT_CERT` | `client_cert` | ✅ |
| `VARCO_CLIENT_KEY` | `client_key` | ✅ |
| `SSL_CERT_FILE` | `ca_cert` (only if `VARCO_CA_CERT` left it unset) | ✅ — **diverges from OpenSSL**, which replaces the default trust store entirely |
| `SSL_CERT_DIR` | entry in `ca_folders` | ✅ — **diverges from OpenSSL** the same way |

⚠️ **`SSL_CERT_FILE`/`SSL_CERT_DIR` are additive in varco, not replace-semantics like OpenSSL/
`uv`/`requests`.** Both are merged on top of `include_system_cas`'s default of `True` — varco
never silently drops the system trust store because a sidecar exported one of these names. If
you need the OpenSSL behaviour, set `include_system_cas=False` explicitly.

#### mTLS — encrypted private keys and PKCS#12 (Plan 027 / T6)

```python
from pathlib import Path
from varco_core.tls import TrustStore

# Encrypted PEM client key — str/bytes, or a callable (recommended: fetched lazily, never
# held in memory until build_ssl_context() actually needs it)
store = TrustStore(
    client_cert=Path("/etc/ssl/client.crt"),
    client_key=Path("/etc/ssl/client-encrypted.key"),
    key_password=lambda: vault_client.read_secret("client-key-password"),
)

# PKCS#12 / .pfx bundle — zero new dependencies, built on cryptography (already a hard
# varco_core dependency). pkcs12_trust_ca defaults to False: the bundle's own CAs are not
# automatically trusted for server verification.
store = TrustStore(
    pkcs12_file=Path("/etc/ssl/client-identity.p12"),
    pkcs12_password="s3cret",
    pkcs12_trust_ca=False,
)
ctx = store.build_ssl_context()
```

`pkcs12_file` is mutually exclusive with `client_cert`/`client_key`. Neither `key_password` nor
`pkcs12_file`/`pkcs12_password` are `SSLConfig` settings fields — a secret does not belong in a
pydantic settings model populated from env vars. Full design (the temp-file window,
`/dev/shm` preference, the `password=b""` OpenSSL-prompt-avoidance deviation):
`technical_docs/features/tls-trust-and-hot-reload.md`.

#### Client injection — httpx, aiohttp, urllib3, requests (Plan 027 / T4)

Four adapters convert a `TrustStore`/`ReloadingTrustStore` into the shape each mainstream
HTTP client wants, with **no new hard dependency** on any of them (function-body imports only):

```python
from pathlib import Path
from varco_core.tls import TrustStore

store = TrustStore(ca_folders=Path("/etc/ssl/private-ca"))

import httpx
client = httpx.Client(verify=store.to_httpx_verify())  # httpx 0.28+

import aiohttp
# must be built inside the event loop that will use it
connector = await store.to_aiohttp_connector()  # aiohttp 3.14+
async with aiohttp.ClientSession(connector=connector) as session:
    ...

import urllib3
pool = store.to_urllib3_poolmanager()  # urllib3 v2.x

import requests
session = requests.Session()
session.mount("https://", store.to_requests_adapter())  # requests 2.32.3+
```

A missing library raises `MissingClientDependencyError` naming the `pip install` package. All
four also exist as module-level functions in `varco_core.tls.clients`
(`to_httpx_verify(store)`, etc.) — the methods above are thin delegations for discoverability.
Every adapter reads the store's context **at call time** — under `ReloadStrategy.SWAP`, a
client built before a rotation keeps the old context until rebuilt via `store.subscribe(cb)`;
under `MUTATE` it picks up the rotation with no action.

#### `install_process_trust()` — process-wide trust, explicit and never automatic (Plan 027 / T4)

```python
from pathlib import Path
from varco_core.tls import TrustStore, install_process_trust

store = TrustStore(ca_folders=Path("/etc/ssl/private-ca"))

# ⚠️ Call this ONCE, at your application's entry point, before constructing/importing any
# HTTP client. It patches ssl._create_default_https_context process-wide — every stdlib-ssl-
# backed client built AFTER this call (via a create_default_context()-consuming path) picks
# up `store`'s trust. varco itself never calls this on your behalf; it is an application-level
# decision only, and omitting acknowledge_global_mutation=True raises ValueError with no
# mutation at all.
handle = install_process_trust(store, acknowledge_global_mutation=True)
...
handle.restore()  # or use install_process_trust(...) as a context manager
```

### RedisConnectionSettings

```python
from varco_redis.connection import RedisConnectionSettings
import redis.asyncio

# Plain
conn = RedisConnectionSettings(host="redis.internal", port=6379, db=0)
client = redis.asyncio.from_url(conn.to_url(), **conn.to_redis_kwargs())

# With password
conn = RedisConnectionSettings(host="redis.internal", password="s3cret")

# With TLS — from env: REDIS_HOST, REDIS_SSL__CA_CERT, etc.
conn = RedisConnectionSettings.from_env()
```

| Env var | Default | Description |
|---|---|---|
| `REDIS_HOST` | `localhost` | Hostname |
| `REDIS_PORT` | `6379` | Port |
| `REDIS_PASSWORD` | — | AUTH password |
| `REDIS_USERNAME` | — | ACL username (Redis 6+) |
| `REDIS_SSL__CA_CERT` | — | Path to CA certificate |
| `REDIS_SSL__VERIFY` | `true` | TLS peer verification |

### HttpConnectionSettings

Produces kwargs for `httpx.AsyncClient`. Multiple instances are supported via per-client prefixes.

```python
from varco_fastapi.connection import HttpConnectionSettings
import httpx

# Plain
conn = HttpConnectionSettings(base_url="https://api.example.com/v1", timeout=10.0)

async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
    response = await client.get("/users")

# With Basic auth
from varco_core.connection import BasicAuthConfig

conn = HttpConnectionSettings(
    base_url="https://api.example.com",
    auth=BasicAuthConfig(username="svc-user", password="secret"),
)

# With TLS
from varco_core.connection import SSLConfig

conn = HttpConnectionSettings.with_ssl(
    SSLConfig(ca_cert=Path("/etc/ssl/ca.pem")),
    base_url="https://secure-api.example.com",
)

# From env — multi-client via distinct prefixes
conn = HttpConnectionSettings.from_env(prefix="PAYMENT_API_")
conn = HttpConnectionSettings.from_env(prefix="NOTIF_API_")
```

| Env var | Default | Description |
|---|---|---|
| `{PREFIX}BASE_URL` | _(empty)_ | Full base URL |
| `{PREFIX}HOST` | `localhost` | Hostname (used when BASE_URL is empty) |
| `{PREFIX}PORT` | `443` | Port (used when BASE_URL is empty) |
| `{PREFIX}TIMEOUT` | `30.0` | Request timeout in seconds |
| `{PREFIX}AUTH__TYPE` | — | `basic` or `oauth2` |
| `{PREFIX}SSL__CA_CERT` | — | CA certificate path |
| `{PREFIX}SSL__VERIFY` | `true` | TLS peer verification |

---

## FastAPI Integration

`varco_fastapi` provides a batteries-included FastAPI integration layer.

### VarcoRouter and VarcoCRUDRouter

`VarcoRouter[D, PK, C, R, U]` is the generic base class. Subclasses compose CRUD mixins via MRO and declare ClassVars for prefix, tags, and auth strategy. `build_router()` materializes all routes into a FastAPI `APIRouter`.

`VarcoCRUDRouter` extends `VarcoRouter` with service injection, CRUD handler dispatch, and named-task auto-registration for recoverable async mode.

```python
from varco_fastapi.router.crud import VarcoCRUDRouter
from varco_fastapi.router.mixins import (
    CreateMixin,
    ReadMixin,
    UpdateMixin,
    PatchMixin,
    DeleteMixin,
    ListMixin,
)
from providify import Singleton


@Singleton
class OrderRouter(
    CreateMixin,
    ReadMixin,
    UpdateMixin,
    PatchMixin,
    DeleteMixin,
    ListMixin,
    VarcoCRUDRouter[Order, UUID, OrderCreate, OrderRead, OrderUpdate],
):
    _prefix = "/orders"
    _tags = ["orders"]
    _version = "v1"  # adds /v1/orders prefix


router = OrderRouter().build_router()
app.include_router(router)
```

**Custom routes** — use `@route` for non-CRUD endpoints on the same router:

```python
from varco_fastapi.router.endpoint import route


@Singleton
class OrderRouter(ReadMixin, ListMixin, VarcoCRUDRouter[...]):
    _prefix = "/orders"

    @route("GET", "/{order_id}/summary")
    async def get_summary(self, order_id: UUID) -> dict:
        ctx = get_request_context().auth
        order = await self._service.get(order_id, ctx)
        return {"pk": str(order.pk), "status": order.status}
```

**Exposing custom service methods, typed** — `VarcoCRUDRouter` (and the `CRUDRouter`/
`ReadOnlyRouter`/`WriteRouter`/`NoDeleteRouter` presets) accept an optional, defaulted 6th
type parameter `S` — the concrete `AsyncService` subclass. Add it as the 6th type arg to get
`self._service` typed `S | None` and the `self.service` property typed non-Optional `S`, with
zero per-subclass boilerplate (no cast, no hand-rolled `@property` override):

```python
from varco_fastapi.router.presets import CRUDRouter
from varco_fastapi.router.endpoint import route


class OrderService(AsyncService[Order, UUID, OrderCreate, OrderRead, OrderUpdate]):
    async def cancel_order(self, order_id: UUID) -> None: ...


class OrderRouter(CRUDRouter[Order, UUID, OrderCreate, OrderRead, OrderUpdate, OrderService]):
    _prefix = "/orders"

    @route("POST", "/{order_id}/cancel")
    async def cancel(self, order_id: UUID) -> None:
        # self.service is typed OrderService (not the erased AsyncService base) —
        # .cancel_order is visible to the type checker with no cast.
        await self.service.cancel_order(order_id)
```

- 5-arg subscription (`CRUDRouter[Order, UUID, OrderCreate, OrderRead, OrderUpdate]`) still
  works unchanged — `S` defaults to `AsyncService[Any, ...]` via PEP 696
  (`typing_extensions.TypeVar`, since `requires-python = ">=3.12"` predates the native syntax).
- `self.service` raises `RuntimeError` if `_service` was never injected/set — prefer it over
  `self._service` at call sites that invoke custom methods, so you don't repeat an
  `is None` guard. The 501-Not-Implemented CRUD fallback path is unaffected — it still reads
  `getattr(self, "_service", None)` directly, not the property.
- Fallback for anyone staying on 5 type args: declare a subclass `@property` that casts, e.g.
  `@property\n    def service(self) -> OrderService:\n        return cast(OrderService, self._service)`.

See `technical_docs/features/custom-routes.md` for the full custom-route parameter-injection
reference.

### CRUD mixins

Each mixin contributes exactly one route. All support per-mixin OpenAPI customization via ClassVars.

| Mixin | Route | Method | Auth default |
|---|---|---|---|
| `CreateMixin` | `POST /` | `service.create()` | Requires auth |
| `ReadMixin` | `GET /{pk}` | `service.get()` | Public |
| `UpdateMixin` | `PUT /{pk}` | `service.update()` | Requires auth |
| `PatchMixin` | `PATCH /{pk}` | `service.patch()` | Requires auth |
| `DeleteMixin` | `DELETE /{pk}` | `service.delete()` | Requires auth |
| `ListMixin` | `GET /` | `service.list()` | Public |
| `SummaryMixin` | `GET /{pk}/summary` | lightweight projection | Public |

```python
@Singleton
class OrderRouter(CreateMixin, ReadMixin, ListMixin, VarcoCRUDRouter[...]):
    _prefix = "/orders"
    _create_summary = "Place a new order"
    _create_status_code = 201
    _list_max_limit = 200
    _create_async_capable = True  # allow ?with_async=true
```

### Service-free routers — `GenericRouter`

Use `GenericRouter` (alias for `VarcoRouter` with no type args) when the server has no
`AsyncService` or repository — e.g. a data-transformation pipeline, an API gateway, or
computed analytics routes. All cross-cutting features (middleware, telemetry, auth,
`RouteGuard` authorization) work identically to a service-backed router.

```python
from varco_fastapi.router.presets import GenericRouter
from varco_fastapi.router.endpoint import route
from varco_fastapi.auth import JwtBearerAuth
from varco_fastapi.auth.guard import require_scopes, require_roles


class ReportRouter(GenericRouter):
    _prefix = "/reports"
    _auth = JwtBearerAuth(...)  # authentication stays in middleware

    @route("GET", "/summary", requires=require_scopes("reports:read"))
    async def get_summary(self, ctx: AuthContext) -> dict:
        return {"total": compute_total(ctx)}

    @route("DELETE", "/cache", requires=require_roles("admin"))
    async def purge_cache(self, ctx: AuthContext) -> None:
        invalidate_cache()


# Wire exactly like a normal router
app = create_varco_app(routers=[ReportRouter])
```

**Key facts**:
- `validate_router_class` does NOT warn about missing generic type args (`D/PK/C/R/U`) for a `GenericRouter` — they are legitimately absent.
- `requires=` kwarg on `@route` takes a `RouteGuard` (built with `require_scopes`, `require_roles`, `require_grant`, `require_predicate`, or `allow_anonymous`).
- Guard is checked **before** the handler runs; denial raises `ServiceAuthorizationError` → HTTP 403.
- If `ctx` is declared in the handler, `_auth` must be set (or it will not be populated).
- For truly public endpoints (no auth needed), use `requires=allow_anonymous()` or omit `requires=` altogether and don't declare `ctx`.
- **Custom `@route` handlers get full FastAPI parameter injection** — declare `Query(...)`, `Body(...)` (Pydantic models), `Depends(...)`, `Request`/`Response`/`BackgroundTasks`, and **type-coerced** path params, exactly like a hand-written FastAPI endpoint; the return annotation drives the OpenAPI response model. `build_router()` synthesizes a wrapper whose `__signature__` mirrors the method so FastAPI parses everything natively (see `_make_custom_handler` / `_synthesize_custom_signature` in `router/base.py`). `ctx`/`auth`/`context` and the `RouteGuard`/async-offload behavior are unchanged. **Exception:** on an `async_capable` route with a job runner wired, `response_model` inference is suppressed (the route may return a `JobAcceptedResponse` when `?with_async=true`).

See `technical_docs/features/generic-router.md` for the full design.

### JWT authentication middleware

`JwtBearerAuth` validates `Authorization: Bearer <token>` headers and populates the `AuthContext` for each request:

```python
from varco_fastapi.auth.server_auth import JwtBearerAuth
from varco_core.authority import TrustedIssuerRegistry

registry = TrustedIssuerRegistry.from_env()
await registry.load_all()

auth = JwtBearerAuth(registry)


# Apply to the whole router
@Singleton
class OrderRouter(CreateMixin, VarcoCRUDRouter[...]):
    _prefix = "/orders"
    _auth = auth  # ClassVar — all routes use this auth strategy
```

Or inject via DI (installed automatically by `VarcoFastAPIModule`):

```python
from varco_fastapi.di import VarcoFastAPIModule

container.install(VarcoFastAPIModule)
# AbstractServerAuth → JwtBearerAuth registered automatically
```

### Request context

`RequestContext` is a per-request `ContextVar` populated by `RequestContextMiddleware`. Access it anywhere in the call stack:

```python
from varco_fastapi.context import get_request_context

ctx = get_request_context()
auth = ctx.auth  # AuthContext (user_id, roles, grants)
jwt = ctx.jwt  # raw JWT payload dict
request = ctx.request  # FastAPI Request object
```

### Middleware stack

Install the full middleware stack in one call:

```python
from varco_fastapi.middleware import setup_middleware
from varco_fastapi.middleware.cors import CORSConfig
from varco_fastapi.middleware.error import ErrorMiddleware

app = FastAPI()
setup_middleware(app, cors=CORSConfig.from_env())
# Installs (outermost → innermost):
#   ErrorMiddleware         — JSON error responses for ServiceException
#   CORSMiddleware          — CORS headers
#   RequestContextMiddleware — AuthContext ContextVar per request
#   LoggingMiddleware        — structured request/response logging
```

### Job runner — async mode

Append `?with_async=true` to any CRUD endpoint (when `_create_async_capable = True` etc.) to receive `202 Accepted` with a `job_id`:

```bash
POST /orders?with_async=true
# → 202 {"job_id": "...", "status": "pending"}

GET /jobs/{job_id}
# → {"job_id": "...", "status": "completed", "result": {...}}
```

`VarcoCRUDRouter` auto-registers CRUD closures in `TaskRegistry` at `build_router()` time — named `"ClassName.create"`, `"ClassName.update"`, etc. — so `JobRunner.recover()` can re-submit `PENDING` jobs after a process restart.

```python
from varco_core.job import AbstractJobRunner, JobRunner
from varco_core.job.store import InMemoryJobStore

runner = JobRunner(InMemoryJobStore())
await runner.start()

# After restart — re-submit any jobs that were PENDING when the process died
await runner.recover()
```

### Bootstrap helpers

One-liner setup for each backend (also registered by `container.install(VarcoFastAPIModule)`):

```python
from varco_fastapi.app import (
    sa_bootstrap,  # SQLAlchemy repo provider + UoW
    redis_bootstrap,  # Redis event bus
    ws_bootstrap,  # WebSocket + SSE adapters
    fastapi_bootstrap,  # FastAPI defaults (auth, CORS, job runner, producer)
    redis_async_bootstrap,  # Redis cache (async — call inside lifespan)
)
from varco_fastapi.lifespan import VarcoLifespan

container = DIContainer()
sa_bootstrap(container)
redis_bootstrap(container, streams=True)  # use Redis Streams (at-least-once)
ws_bootstrap(container)
fastapi_bootstrap(container, setup_producer=True)

app = FastAPI(lifespan=VarcoLifespan(container))
```

### MCP — exposing a `VarcoRouter` as an MCP server

`MCPAdapter` (`varco_fastapi.router.mcp`) converts any `VarcoRouter` into a Model Context
Protocol server, built against **MCP Python SDK v2.x** (`pip install "varco-fastapi[mcp]"` — the
package now requires `mcp>=2,<3`, since `pip install mcp` already resolves to the v2 line and
v1.x is maintenance-only; see CHANGELOG's **BREAKING (optional extra)** entry, Plan 029 / N1, if
upgrading from an app pinned to varco-fastapi's old `mcp<2` requirement).

```python
from varco_fastapi.router.mcp import MCPAdapter
from myapp.routers import OrderRouter
from myapp.clients import OrderClient

adapter = MCPAdapter(
    OrderRouter,
    client=OrderClient(base_url="http://localhost:8080"),
    ttl_ms=60_000,  # tools/list client-side cache TTL (2026-07-28 wire requirement)
)

# Mount as an HTTP+SSE endpoint on an existing FastAPI app
adapter.mount(app)   # adds GET {path}/sse + POST {path}/messages/

# Or run as a standalone stdio MCP server (for local LLMs)
server = adapter.to_mcp_server()   # an mcp.server.Server (v2)
from mcp.server.stdio import stdio_server
async with stdio_server() as (read, write):
    await server.run(read, write, server.create_initialization_options())
```

Every route flagged `mcp_enabled=True` (via `_create_mcp = True` etc. on the router, or
`@route(mcp_enabled=True)`) becomes one MCP tool; execution is delegated to the already-injected
`AsyncVarcoClient`, so no handler logic is duplicated. Full migration rationale (why v1/v2 dual
support was rejected, the `ListToolsResult`/`ttl_ms`/`cache_scope` requirement, and the
HTTP+SSE-vs-Streamable-HTTP follow-up): `design/research/003-mcp-python-sdk-v2-migration.md`.

### A2A — exposing a non-router subject

Use `source=` instead of `router_cls` when the thing you want other agents to call is not a
`VarcoRouter` — a data pipeline, a wrapper around a third-party API, anything with no CRUD
routes to introspect:

```python
from varco_fastapi.router.a2a.source import SkillDefinition, AgentMetadata
from varco_fastapi.router.skill import SkillAdapter


class ReportSkillSource:
    def skills(self) -> list[SkillDefinition]:
        return [
            SkillDefinition(
                id="generate_report",
                name="Generate Report",
                description="Builds a PDF summary for the given date range",
                input_modes=("application/json",),
                output_modes=("application/json",),
                route=None,  # no VarcoRouter route backs this skill
            )
        ]

    def agent_metadata(self) -> AgentMetadata:
        return AgentMetadata(name="ReportAgent", description="Generates PDF reports")

    async def invoke(self, skill_id: str, payload: dict, *, ctx=None) -> dict:
        # ctx is the verified caller's AuthContext (U-3) — use it for the audit trail,
        # e.g. record which end user / agent / platform requested the report.
        return {"report_url": await build_report(payload, requested_by=ctx)}


adapter = SkillAdapter(
    None,  # router_cls omitted
    source=ReportSkillSource(),
    agent_name="ReportAgent",
    agent_description="Generates PDF reports",
    client=None,  # not needed — invoke() does its own work
)
adapter.mount(app)  # same v1.0.0 + legacy A2A surface as a router-backed adapter
```

`adapter.router_class` is `None` for a non-router source — that is the documented contract,
not a bug. `router_cls` and `source=` are mutually exclusive; passing both or neither raises
`ValueError` at construction. Full design: `technical_docs/features/a2a-surface.md`.

### Calling other varco services — `client_for`

`client_for()` is the documented way to call another varco service — no
class to subclass, no manual httpx wiring, returns a ready-to-call instance:

```python
from varco_fastapi.client import client_for
from orders_service.routers import OrderRouter  # importable peer router

client = client_for(OrderRouter, "https://orders.internal")
order = await client.read(order_id)
await client.cancel(order_id, reason="oos")
```

For a peer whose Python package isn't importable (a different team/repo),
export a portable contract and generate a typed client, or use the runtime
one-liner:

```bash
varco export-contract myapp.routers:OrderRouter -o order.contract.json
varco gen-client -c order.contract.json -o order_client.py --class-name OrderClient
```

```python
from varco_fastapi.contract.runtime import contract_client

client = contract_client("order.contract.json", "https://orders.internal")
```

`PeerRegistry` bundles "one env var per peer" plus resilience (retry,
timeout, a shared circuit breaker, auth forwarding) pre-wired:

```bash
export VARCO_PEER_ORDERS_URL="https://orders.internal"
```

```python
from varco_fastapi.client.peer import PeerRegistry

registry = PeerRegistry.from_env()
client = registry.client("orders", OrderRouter)
```

`make_client`, `GenericClient`, `OpenAPIClient`, `ClientConfigurator`, and
`generate_client` still exist for their original use cases (no-router
services, third-party OpenAPI docs) — import them from
`varco_fastapi.client.advanced`. Full guides: [`docs/client.md`](docs/client.md),
[`docs/client-code-generation.md`](docs/client-code-generation.md),
[`docs/peer-service-integration.md`](docs/peer-service-integration.md),
[`technical_docs/features/portable-contracts.md`](technical_docs/features/portable-contracts.md).

⚠️ **Current gap**: `client_for()`'s custom `@route` methods are not yet
built through the same typed mechanism as `gen-client`/`contract_client()` —
they still accept `**kwargs: Any`. See the "important" note in
[`docs/client-code-generation.md`](docs/client-code-generation.md) before
assuming the two paths behave identically for a given router.

---

## Observability

`varco_core.observability` provides OpenTelemetry tracing and metrics.

### @span — distributed tracing

```python
from varco_core.observability import span, SpanConfig


@span  # auto-named from function name
async def process_order(order_id: UUID) -> None: ...


@span(SpanConfig(name="orders.process", attributes={"service": "orders"}))
async def process_order(order_id: UUID) -> None: ...


# Context manager form
from varco_core.observability import create_span


async def process_order(order_id: UUID) -> None:
    async with create_span("orders.validate") as s:
        s.set_attribute("order.id", str(order_id))
        await validate(order_id)
```

### @counter and @histogram

```python
from varco_core.observability import counter, histogram, CounterConfig, HistogramConfig


@counter(CounterConfig(name="orders.created", description="Total orders created"))
async def create_order(dto: OrderCreate) -> Order: ...


@histogram(HistogramConfig(name="orders.processing_ms", unit="ms"))
async def process_order(order_id: UUID) -> None: ...
```

Imperative helpers for more control:

```python
from varco_core.observability import create_counter, create_histogram

_orders_counter = create_counter("orders.created", "Total orders created")
_proc_histogram = create_histogram("orders.processing_ms", unit="ms")

_orders_counter.add(1, {"status": "success"})
_proc_histogram.record(42.5, {"region": "eu-west-1"})
```

### TracingServiceMixin

Auto-spans every CRUD method on an `AsyncService` with zero boilerplate:

```python
from varco_core.observability import TracingServiceMixin


@Singleton
class OrderService(
    TracingServiceMixin,  # wraps get/list/create/update/delete in OTel spans
    AsyncService[Order, UUID, OrderCreate, OrderRead, OrderUpdate],
):
    def _get_repo(self, uow):
        return uow.orders
```

### OtelConfig and DI wiring

```python
from varco_core.observability import OtelConfig, OtelConfiguration
from providify import DIContainer, Provider


# Module-level and @Provider-decorated: `install()` has no `config=` keyword,
# `provide()` rejects undecorated callables, and under PEP 563 the return
# annotation is resolved against this function's own module globals.
@Provider(singleton=True)
def otel_config() -> OtelConfig:
    return OtelConfig(
        service_name="orders-svc",
        otlp_endpoint="http://otel-collector:4317",
        service_version="1.2.0",
    )


container = DIContainer()
container.provide(otel_config)  # ⚠️ before install() — equal-priority
container.install(OtelConfiguration)  #    bindings resolve first-registered

tracer_provider = container.get(TracerProvider)
```

### Automatic parameter capture

Every `@span` (bare or configured) records its decorated function's call arguments
as `param.<name>` span attributes by default — redacted (name-based, e.g.
`password`/`token`/`secret`), truncated, and scalar-only (opaque objects render as
`"<TypeName>"`, never their contents):

```python
from varco_core.observability import span


@span
async def place_order(order_id: UUID, password: str = "") -> Order: ...


# span attributes: param.order_id="<uuid>", param.password="[REDACTED]"
```

Per-decorator or process-wide kill switches:

```python
from varco_core.observability import set_capture_enabled, SpanConfig, span


@span(SpanConfig(capture_params=False))  # off for this function only
async def charge_card(card_token: str) -> None: ...


set_capture_enabled(False)  # off process-wide (or VARCO_OTEL_CAPTURE_PARAMS=false)
```

`TracingServiceMixin`/`TracingRepositoryMixin` spans do **not** auto-capture
`pk`/`dto`/`params` — only `@span`-decorated functions and
`create_span(..., params=...)` do. See
[the full guide](technical_docs/features/observability-attributes.md) for the PII
guidance before enabling this in production.

### Global attributes

A process-wide registry stamps entries on **every** span AND **every** metric
measurement (counter / up-down counter / histogram / observable gauge):

```python
from varco_core.observability import set_global_attributes, register_global_attribute_provider

set_global_attributes(**{"deployment.colour": "blue"})

register_global_attribute_provider(
    lambda: {"k8s.pod.name": os.environ.get("POD_NAME", "unknown")},
    name="pod-identity",
    cache_ttl=None,  # evaluate once — never poll/do I/O in a provider
)
```

> ⚠️ **Cardinality warning**: a global attribute becomes a label on **every metric
> series**. Static process identity (pod name, deployment environment) belongs in
> `OtelConfig.extra_resource_attrs` (free, no series multiplication) — reach for
> the registry only for values not known at bootstrap or that you must `group by`
> as a metric label. See the
> [full decision table](technical_docs/features/observability-attributes.md).

`VARCO_OTEL_*` env vars (no code required):

| Env var | Default | Effect |
|---|---|---|
| `VARCO_OTEL_CAPTURE_PARAMS` | `true` | Process-wide `@span` parameter-capture kill switch. |
| `VARCO_OTEL_CAPTURE_PARAMS_EXCLUDE` | *(empty)* | Comma-separated parameter names always excluded from capture. |
| `VARCO_OTEL_GLOBAL_ATTRS` | *(empty)* | Literal `key=value` pairs, comma-separated. |
| `VARCO_OTEL_GLOBAL_ATTR_ENV` | *(empty)* | `key=ENV_VAR_NAME` pairs — value read lazily from another env var (Kubernetes Downward API friendly). |
| `VARCO_OTEL_GLOBAL_ATTRS_SPANS` | `true` | Apply the registry to spans. |
| `VARCO_OTEL_GLOBAL_ATTRS_METRICS` | `true` | Apply the registry to metrics — the runtime rollback for a cardinality incident. |

### Auditing

`varco_core.service.audit` provides an event-driven, append-only audit trail
(`AuditLogMixin` + `AuditConsumer` + `AuditRepository`) for `varco_sa` and
`varco_beanie`. See the
[Database Auditing guide](technical_docs/features/database-auditing.md) for
wiring, the Alembic/Beanie setup, and the per-backend idempotency behaviour.

---

## Redaction

`varco_core.redaction` is one small, key-name-based redaction seam shared by span capture,
`error_params()`, the audit trail, and request logging — `Redactor` (a one-method Protocol),
`PolicyRedactor` (the default), `RedactionPolicy`, and `redact_mapping()`/`redact_query_string()`/
`json_safe()`. Span capture's byte-identical 15-pattern list moved here; nothing else it does
changed.

```python
from varco_core.redaction import PolicyRedactor
from varco_core.service.audit import AuditLogMixin

# Opt in per service — closes the one live leak this seam exists to fix
# (a secret-shaped field on a read DTO stored verbatim in the audit table):
class OrderService(AuditLogMixin, AsyncService[...]):
    _audit_redactor = PolicyRedactor()
```

`error_params()` is redacted on the error envelope by default
(`ErrorEnvelopeSettings.redact_params=True`) — a secret-named key becomes `"[REDACTED]"` and a
non-JSON value becomes `"<TypeName>"`. `RequestLoggingMiddleware(redactor=...)` gives a
body/header/URL-logging subclass something to call; the default (`redactor=None`) is
byte-identical to pre-3.2 logging.

| Env var | Default | Effect |
|---|---|---|
| `VARCO_ERROR_REDACT_PARAMS` | `true` | Route `error_params()` through `redact_mapping()` + `json_safe()` before emitting it on the error envelope. |

Full design (the pattern-constant table, the substring matcher's documented false positives, the
hash-chain write-path-only rule, a Pitfalls table): `technical_docs/features/redaction.md`.

---

## Profiling

Diagnostic CPU + memory profiler. Complements the aggregate OTel observability layer
(spans/metrics answer "how slow on average"; the profiler answers "which function is hot"
and "what allocated this memory"). Off by default — zero overhead when disabled
(`VARCO_PROFILING_ENABLED=false`).

```python
from varco_core.profiling import profile, profiled, ProfileConfig, set_profiling_enabled

# 1. Enable globally (or VARCO_PROFILING_ENABLED=true in env)
set_profiling_enabled(True)


# 2a. Decorator form — wraps every call
@profile(ProfileConfig(top_n=10))
async def slow_query() -> list[Row]:
    return await db.execute("SELECT ...")


# 2b. Context manager form — gives access to the report object
async with profiled("batch_export") as session:
    rows = await db.fetch_all()
    await write_csv(rows)
print(session.report.format())  # human-readable table to stderr/logs

# 3. FastAPI: enable via env var or create_varco_app flag
#    VARCO_PROFILER_ENABLED=true VARCO_PROFILER_ATTACH_HEADERS=true
app = create_varco_app(container, enable_profiling=True)
# → X-Profile-Wall-Ms + X-Profile-Mem-Kb headers on each response
```

**Adding a custom backend (memray, pyinstrument, py-spy):**

```python
from varco_core.profiling import CpuProfilerBackend, CpuProfileResult, register_cpu_backend


class PyinstrumentBackend:
    name = "pyinstrument"

    def start(self) -> None: ...
    def collect(self, top_n: int, sort_by: str) -> CpuProfileResult: ...


register_cpu_backend("pyinstrument", PyinstrumentBackend)
cfg = ProfileConfig(cpu_backend="pyinstrument")
```

```python
from varco_core.profiling import MemoryProfilerBackend, MemoryProfileResult, register_memory_backend


class MemrayBackend:
    name = "memray"

    def start(self) -> None: ...
    def collect(self, top_n: int) -> MemoryProfileResult: ...


register_memory_backend("memray", MemrayBackend)
cfg = ProfileConfig(memory_backend="memray")
```

**Caveats:**

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **Profiling left always-on** | 20–100% overhead in production | `cProfile`/`tracemalloc` are expensive deterministic tools | Default is off (`VARCO_PROFILING_ENABLED=false`); activate only to diagnose a hotspot |
| **Two profiling sessions concurrent** | Contaminated reports (each session records the other's frames) | `cProfile`/`tracemalloc` are process-global | The middleware serialises with a `Lock`; for manual use, never profile two operations simultaneously |
| **`cProfile` across `await` on a busy loop** | Report includes frames from other coroutines | `cProfile` captures the whole event loop thread | Use a sampling backend (e.g. pyinstrument) for concurrent async code; cProfile is best for CPU-bound or isolated coroutines |
| **tracemalloc state not restored** | App's own tracemalloc usage broken after a profiling session | Session left tracemalloc on when it found it off (or vice versa) | `TracemallocMemoryBackend.collect()` always restores the prior tracing state; if writing a custom memory backend, do the same |

---

## Background Jobs

`AbstractJobStore`/`AbstractJobRunner` (`varco_core.job.base`) support a time dimension
(`Job.run_at`, `AbstractJobRunner.enqueue(run_at=, delay=)`), bounded retry
(`Job.attempt`/`max_attempts`, `JobRunner(retry_policy=, dlq=)`), and a fenced lease
(`try_claim(owner_id=, lease_ttl=)`, `renew()`, `reap_expired_leases()`,
`save(expected_epoch=)` → `StaleLeaseError` on a stale write).

```python
claimed = await store.try_claim(job_id, owner_id="worker-7", lease_ttl=30.0)
renewed = await store.renew(job_id, owner_id="worker-7", epoch=claimed.lease_epoch, lease_ttl=30.0)
await store.save(claimed.as_completed(result), expected_epoch=claimed.lease_epoch)
# StaleLeaseError if a stalled worker resumes after being fenced out by a reap
```

`JobPoller(lease_aware=True)` (the default) detects death via `store.reap_expired_leases()`
instead of a wall-clock `stale_threshold`. Retention primitive:
`store.delete_where(..., limit=1000)` looped until it returns `0` (a bounded chunked sweep —
never one unbounded `delete_where()` call, which can starve the connection pool).

Zoned, DST-safe one-shot scheduling:

```python
await job_runner.enqueue(job, coro, run_at_wall=wall_dt, tz="America/New_York")
# _prepare_zoned_job() resolves DST gaps/overlaps and materializes run_at (UTC) before save()
```

Design rationale (lease/fencing model, zoned-schedule materialization, retry-binding
decisions) lives in `technical_docs/features/job-scheduling-and-leases.md`.

---

## Database Auditing

An append-only audit trail for `create`/`update`/`delete` mutations, event-driven like the
outbox pattern but persisted by a dedicated consumer rather than a relay: `AuditLogMixin`
(service mixin, composes to the LEFT of `AsyncService`) emits an `AuditEvent` on the
`"varco.audit"` channel via the service's existing `AbstractEventProducer` — `AuditConsumer`
subscribes and persists each event as an `AuditEntry` via an injected `AuditRepository`
(`SAAuditRepository` in `varco_sa`, `BeanieAuditRepository` in `varco_beanie`).

```python
class OrderService(
    AuditLogMixin,  # ← left of AsyncService
    AsyncService[Order, UUID, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
):
    def _get_repo(self, uow):
        return uow.orders

    def _get_audit_actor(self, ctx):
        return ctx.sub  # override — base returns None


# Wire the consumer from @PostConstruct, same rule as any other EventConsumer
class AuditWiring:
    def __init__(self, bus: Inject[AbstractEventBus], audit_repo: Inject[SAAuditRepository]):
        self._bus = bus
        self._consumer = AuditConsumer(audit_repo=audit_repo)

    @PostConstruct
    def _setup(self) -> None:
        self._consumer.register_to(self._bus)
```

Idempotency is backend-specific: `SAAuditRepository.save` uses Postgres
`INSERT ... ON CONFLICT (entry_id) DO NOTHING` (falling back to a plain `IntegrityError`-raising
insert on non-Postgres dialects); `BeanieAuditRepository.save` is a plain `doc.insert()` with no
conflict handling. See `technical_docs/features/database-auditing.md` for the full wiring guide
(Alembic/Beanie setup, `list_for_entity()`, retention, tamper evidence via `hash_chain=True`).

---

## Dead Letter Queue

`AbstractDeadLetterQueue` is the interface. `InMemoryDeadLetterQueue` is for tests. Backend
implementations (`KafkaDLQ`, `RedisDLQ`, `SADeadLetterQueue` in their respective packages) push
to a dedicated topic/channel/table. **Contract**: `push()` must never raise.

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

Redrive, retention, tenancy, a Beanie backend, and a bundled REST admin surface
(`mount_reliability_admin()`) are covered in `technical_docs/features/dead-letter-queues.md`.
`mount_reliability_admin(..., cross_tenant_role="cross-tenant-admin")` (Plan 036 / S4) is required
to omit `tenant_id` and reach every tenant, or to pass a `tenant_id` other than the caller's own —
an omitted `tenant_id` now scopes to the caller's resolved tenant, not "every tenant". See
`technical_docs/features/admin-surface-tenancy.md`.

---

## Idempotency-Key middleware

Plan 029 / D1. `IdempotencyMiddleware` (`varco_fastapi.middleware.idempotency`) makes a retried
`POST`/`PATCH` carrying a repeated `Idempotency-Key` header replay the first response instead of
executing twice. The storage contract, the fingerprint function, and the in-memory default
implementation live in `varco_core.idempotency` (framework-agnostic — no FastAPI dependency);
only the HTTP adapter lives in `varco_fastapi`.

⚠️ **Opt-in, off by default.** Not added by `create_varco_app()` — register it explicitly, and it
**must** sit inside `ErrorMiddleware` (so its 409/422/400 render through the normal error path)
and inside `RequestContextMiddleware` (so `current_tenant()`/the auth subject are populated
before its scoping logic reads them):

```python
from varco_core.idempotency.memory import InMemoryIdempotencyStore
from varco_fastapi.middleware import (
    ErrorMiddleware,
    IdempotencyMiddleware,
    RequestContextMiddleware,
    install_middleware_stack,
)

install_middleware_stack(app, [
    ErrorMiddleware,
    (RequestContextMiddleware, {"server_auth": my_auth}),
    (IdempotencyMiddleware, {"store": InMemoryIdempotencyStore()}),
])
```

```python
import httpx

headers = {"Idempotency-Key": "order-42-retry-1"}
first = httpx.post("http://api/orders", json={"sku": "abc"}, headers=headers)
second = httpx.post("http://api/orders", json={"sku": "abc"}, headers=headers)
# second.status_code == first.status_code
# second.headers["Idempotency-Replayed"] == "true"
# the handler executed exactly once
```

Outcomes: no header on a `POST`/`PATCH` → executes normally (unless `require_key=True`, then
400); a fresh key → executes, stores, returns the real response; a completed key with a matching
fingerprint (method + path + query + body hash) → replays the stored response; a completed key
with a **different** fingerprint → **422**; a key whose first request is still running → **409**
(with a `Retry-After` header); an empty or over-length key → **400**.

Four `AbstractIdempotencyStore` implementations ship: `InMemoryIdempotencyStore` (`varco_core` —
⚠️ single-process only, same warning as `InMemoryRateLimiter`), `RedisIdempotencyStore`
(`varco_redis`, `SET NX PX`), `SAIdempotencyStore` (`varco_sa`, a unique index +
`IntegrityError`), `BeanieIdempotencyStore` (`varco_beanie`, a unique index +
`DuplicateKeyError`). Each uses its own backend's atomic primitive for the load-bearing
`reserve()` call — never emulated with a separate `exists()` + `set()`.

⚠️ This implements the expired IETF draft (`draft-ietf-httpapi-idempotency-key-header-07`) plus
Stripe's de-facto conventions (24h TTL, 1 MiB body cap, streaming responses never captured) —
not an RFC. Full design (fingerprint construction, header replay allowlist, tenant/subject
scoping and its fail-closed rule, streaming/over-ceiling handling, a Pitfalls table):
`technical_docs/features/idempotency-key.md`.

---

## Token revocation

Plan 034 / S13. `varco_core.revocation.AbstractTokenRevocationStore` lets you invalidate a JWT
before its natural `exp` — per token, per subject, per tenant, or per issuer — through one ABC
with three in-tree implementations. **Off by default**: `TrustedIssuerRegistry(revocation_store=None)`
(the default) performs no check at all — zero-config behaviour is byte-identical to before this
feature existed.

```python
from datetime import UTC, datetime

from varco_core.authority import TrustedIssuerRegistry
from varco_core.revocation import RevocationEntry, RevocationScope
from varco_core.revocation.memory import InMemoryTokenRevocationStore

store = InMemoryTokenRevocationStore()
registry = TrustedIssuerRegistry(revocation_store=store)  # the required second wiring step
await registry.load_all()

# Deny a single compromised token by jti (denylist):
await store.revoke(RevocationEntry.for_token(jti="jti-123", exp=some_token_exp, skew=60))

# Global logout for one subject — every token issued before "now" stops verifying:
await store.revoke(RevocationEntry(
    scope=RevocationScope.SUBJECT,
    key="my-issuer|usr_1",
    revoked_at=datetime.now(UTC),
    expires_at=None,   # a watermark has no natural expiry
))

# ... later, a revoked token raises TokenRevokedError -> JwtBearerAuth maps it to a 401
# ... a store outage under the default FAIL_CLOSED mode raises RevocationStoreUnavailableError -> 503
```

Production wiring uses the Redis backend instead of the in-memory one:

```python
from varco_redis.di import enable_redis_token_revocation
from varco_core.revocation import AbstractTokenRevocationStore

container.scan("varco_redis", recursive=True)
enable_redis_token_revocation(container, url="redis://localhost:6379/0")

store = await container.aget(AbstractTokenRevocationStore)
registry = TrustedIssuerRegistry(revocation_store=store)  # still required — binding alone is not enough
```

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_JWT_REVOCATION_ENABLED` | `true` | Master kill-switch — consult the bound store *if one is wired at all* |
| `VARCO_JWT_REVOCATION_FAILURE_MODE` | `fail_closed` | `fail_closed` (503 on a store outage) or `fail_open` (log + proceed) |
| `VARCO_JWT_REVOCATION_REQUIRE_JTI` | `false` | `true` fails closed on a `jti`-less token at `TOKEN` scope — leave `false` for Auth0/Keycloak/Cognito issuers |
| `VARCO_JWT_REVOCATION_SKEW_SECONDS` | `60.0` | Clock-skew tolerance added to a `TOKEN`-scope entry's TTL beyond the token's own `exp` |

Full design (the four-scope table, the not-valid-before watermark rule, the two-step DI footgun,
a Pitfalls table): `technical_docs/features/credential-and-token-lifecycle.md`.

---

## API-key hashing

Plan 034 / S14. `varco_core.auth.api_key.hash_api_key()`/`verify_api_key()` are stdlib-only
(`hashlib`, `hmac`) primitives so an API key never has to sit in memory as plaintext, and
comparison is always `hmac.compare_digest` — never a raw `==`.

`ApiKeyAuth` uses them automatically — `keys=` (plaintext) is hashed immediately at construction;
`hashed_keys=` (the production path) accepts pre-hashed digests directly, so the plaintext key
never enters the process at all:

```python
from varco_core.auth.api_key import hash_api_key
from varco_fastapi.auth import ApiKeyAuth

# Pre-hash offline (e.g. in a migration script or admin job):
digest = hash_api_key("sk_live_abc123", pepper=os.environb.get(b"VARCO_API_KEY_PEPPER"))
# store `digest` in your config/DB — never the plaintext key

auth = ApiKeyAuth(hashed_keys={digest: AuthContext(user_id="svc_orders")})
```

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_API_KEY_PEPPER` | unset (plain SHA-256) | Process-wide pepper `ApiKeyAuth` reads when no explicit `pepper=` is given; must be identical wherever `hash_api_key()` is called offline and wherever `ApiKeyAuth` verifies at runtime, or every key silently 401s |

⚠️ **The `?api_key=` query-parameter fallback is off by default** (`param=None`) — a credential in
a URL is already in the access log, the proxy log, and the `Referer` header by the time anything
could warn about it. Name `param="api_key"` explicitly to opt back in.

Full design (SHA-256 vs. HMAC-SHA-256 with a pepper, why per-key salt does not apply here, a
Pitfalls table): `technical_docs/features/credential-and-token-lifecycle.md`.

---

## CloudEvents envelope

Plan 030 / N2. `CloudEventsJsonSerializer` (`varco_core.event.cloudevents`) is a **second**
`Serializer[Event]` implementation that wraps every published event in a
[CloudEvents v1.0.2](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
*structured-mode* JSON envelope, so a partner platform, an Eventarc/EventBridge consumer or a
Knative sink can read varco's events without knowing varco.

⚠️ **Opt-in, never auto-active.** `Event` does not change, no bus changes, and nothing happens
until you bind it:

```python
from providify import DIContainer
from varco_core.event.cloudevents import CloudEventsSettings, bind_cloudevents_serializer

container = DIContainer()
bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/orders"))
```

or, without DI:

```python
from varco_core.event.cloudevents import CloudEventsJsonSerializer, CloudEventsSettings

serializer = CloudEventsJsonSerializer(CloudEventsSettings(source="/svc/orders"))
bus = KafkaEventBus(settings, serializer=serializer)
```

On the wire:

```json
{
  "specversion": "1.0",
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "source": "/svc/orders",
  "type": "order.placed",
  "time": "2026-01-01T00:00:00+00:00",
  "datacontenttype": "application/json",
  "correlationid": "corr-1",
  "tenantid": "acme",
  "data": {"order_id": "o-1", "total": 9.5}
}
```

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_CLOUDEVENTS_SOURCE` | *(none — required)* | Producer identity. There is no correct default for "who am I"; construction raises `ValidationError` without it |
| `VARCO_CLOUDEVENTS_DATACONTENTTYPE` | `application/json` | Must end in `json`/`+json` — varco emits `data`, never `data_base64` |

⚠️ Four things to know before adopting:

- `tenantid` comes from `current_tenant()` and is **best-effort** — absent on an
  `OutboxRelay`-driven publish;
- Kafka's binding also wants a `content-type: application/cloudevents+json` **header**, which
  varco cannot set today (`publish()` never gains `headers=`);
- a CloudEvents-serialized and a native-serialized event cannot share a channel mid-migration —
  roll out in three phases, and **drain any existing DLQ backlog first**, because nothing converts
  rows that were stored in the old format;
- the binding reaches every bus (Kafka, NATS, both Redis shapes) and every `@Configuration`-wired
  DLQ automatically. **`SADeadLetterQueue` and `OutboxRelay` are constructed by hand** — pass
  `serializer=` explicitly there or they keep the `JsonEventSerializer` default.

Full design, the named Redis Streams `ce`-field convention, the three-phase dual-emit rollout and a
Pitfalls table: `technical_docs/features/cloudevents-envelope.md`.

---

## AsyncAPI export

Plan 030 / N3. `varco_core.asyncapi.generate_asyncapi()` describes every wired `EventConsumer` as
an **AsyncAPI 3.1.0** document — channels, `action: receive` operations, and message payloads from
Pydantic's `model_json_schema()`.

```python
from varco_core.asyncapi import generate_asyncapi

doc = generate_asyncapi(
    container,                       # or a list of live EventConsumer instances
    title="Orders", version="1.0.0",
    protocol="kafka", group_id="orders-workers",
)
```

or from the CLI:

```bash
varco export-asyncapi --title Orders --version 1.0.0 \
    --consumer myapp.consumers:OrderConsumer \
    --protocol kafka --group-id orders-workers \
    -o asyncapi.json

varco export-asyncapi ... -o asyncapi.json --check   # fail on drift, for CI
```

⚠️ **Generation is runtime, from *registered* consumers.** A `@listen` channel may be a callable
resolved at `register_to()` time against a bound `self`, so a static scan would be silently wrong —
and a consumer that was never wired is deliberately absent from the document. Bindings: Kafka
(`topic`, `groupId`), NATS only when a queue group is configured, Redis never — the generated
document's own `info.description` explains why. No `servers` block is emitted by default.

Full detail, including the local `npx @asyncapi/cli validate` invocation and the snapshot gate:
`technical_docs/features/asyncapi-export.md`.

---

## Outbound webhooks

Plan 031 / D4. A subscription registry, signing, SSRF-hardened delivery, retry into the existing
DLQ, and an admin surface — assembled from parts varco already ships (`RetryPolicy`,
`AbstractDeadLetterQueue`, `DlqRedriver`, `FieldEncryptor`), no new reliability primitive and no
new crypto path. Everything portable lives in `varco_core.webhook`; `varco_sa`/`varco_beanie`
hold repositories; `varco_fastapi.webhook` holds only the admin mount.

```python
from varco_core.webhook.base import InMemoryWebhookSubscriptionRepository
from varco_core.webhook.dispatcher import WebhookDispatcher
from varco_core.webhook.models import WebhookSubscription

repo = InMemoryWebhookSubscriptionRepository()
await repo.save(
    WebhookSubscription(
        tenant_id="acme",
        target_url="https://acme.example.com/hooks",
        event_patterns=["order.*"],
        active_secrets=["whsec_..."],
        status="ACTIVE",
        consecutive_failures=0,
        signer="standard_webhooks",
        custom_headers={},
    )
)

dispatcher = WebhookDispatcher(repository=repo)
dispatcher.register_to(bus, dlq=dlq)  # bus: AbstractEventBus, dlq: AbstractDeadLetterQueue
```

Signing defaults to **Standard Webhooks** (`webhook-id`/`webhook-timestamp`/
`webhook-signature`, HMAC-SHA256, space-delimited multi-signature for zero-downtime secret
rotation) — the scheme an off-the-shelf verification snippet actually understands. RFC 9421
("HTTP Message Signatures" + RFC 9530 `Content-Digest`) is available opt-in via
`WebhookSubscription.signer = "rfc9421"`.

Every delivery target goes through `varco_core.webhook.ssrf.validate_target()` — resolve, then
validate every resolved address against the private/loopback/link-local/reserved ranges
(including the IPv4-mapped bypass form), then **pin the connection to the first resolved
address**, never a later re-resolution (this is what defeats DNS rebinding). `https` only unless
`VARCO_WEBHOOK_ALLOW_INSECURE_HTTP=true` — a deployment-wide switch, never per-tenant.

`WebhookSettings` (env prefix `VARCO_WEBHOOK_`) is the single configuration source and
`WebhookDispatcher` consumes it — omit `settings=` and it constructs one from the environment;
pass an explicit instance to pin the values. The SSRF knobs (`allow_insecure_http`, `allow_list`,
`extra_deny_ranges`) reach `validate_target()`, `signature_tolerance_seconds` reaches the signer,
and the retry/timeout/disable knobs become the dispatcher's defaults. An explicit
`retry_policy=`/`request_timeout_seconds=`/`disable_after_failures=` keyword overrides the
corresponding settings field.

Delivery reuses `varco_core.resilience.RetryPolicy` per-subscription, times out at 10s per
attempt, and pushes to the existing DLQ on exhaustion (`push()` never raises). A subscription
auto-disables after `disable_after_failures` consecutive failures across distinct events;
re-enabling is an explicit admin action.

```python
from varco_fastapi.webhook import mount_webhook_admin

mount_webhook_admin(
    app,
    repository=repo,
    redriver=my_dlq_redriver,       # enables the replay-through-DLQ route
    acknowledge_bundled_admin=True,  # required — RD-9, same posture as mount_reliability_admin
    server_auth=my_auth,
    admin_role="webhook-admin",
    cross_tenant_role="cross-tenant-admin",  # Plan 036 / S4 — required to address another
                                              # tenant's subscription; ctx=None (unauthenticated)
                                              # can never hold it
)
```

Secrets (`WebhookSubscription.active_secrets`) are encrypted at rest via the existing
`FieldEncryptor` when a repository is constructed with `encryptor=` — ⚠️ `encryptor=None` (the
default) stores plaintext, a dev/test-only escape hatch. Because delivery is at-least-once,
point a varco-app receiver at plan 029's `Idempotency-Key` middleware for dedup on the stable
`webhook-id`.

Full design (the signing-scheme decision, the five-layer SSRF model, the retry-schedule
convention, a Pitfalls table): `technical_docs/features/outbound-webhooks.md`.

---

## Inbound webhook verification

Plan 038 / S19 — the receiving half of the pair above: verify a webhook sent *to* a varco app
(Stripe, GitHub, Slack, Svix, or any Standard Webhooks-conformant sender) with one route
dependency instead of hand-rolling `hmac`. `varco_core.webhook.inbound` (`WebhookVerifier` ABC +
four adapters + `get_verifier`) is fully portable; `varco_fastapi.webhook.verify_webhook` is the
only FastAPI-specific piece.

```python
from fastapi import Depends, FastAPI
from varco_core.webhook.inbound import get_verifier
from varco_fastapi.webhook import VerifiedWebhook, verify_webhook

app = FastAPI()
verifier = get_verifier("stripe", secrets=["whsec_..."])
verify_stripe = verify_webhook(verifier)


@app.post("/hooks/stripe")
async def receive_stripe(webhook: VerifiedWebhook = Depends(verify_stripe)) -> dict:
    # webhook.body is the exact raw bytes that were verified -- never re-parsed.
    return {"received": True, "provider": webhook.provider}
```

A route dependency, not a middleware — the secret and the algorithm are per-route facts
(`/hooks/stripe` and `/hooks/github` need different secrets *and* different schemes), which a
middleware would need a path→verifier map to handle. `await request.body()` caches into
Starlette's `Request._body`, so a downstream pydantic body model on the *same* route still parses
correctly. `BodyLimitMiddleware` (on by default at 10 MiB) rejects an over-limit body with a 413
*before* this dependency ever buffers anything.

Add a replay guard — reused from the existing `AbstractIdempotencyStore` seam, no new ABC — to
reject a message id that arrives twice (mandatory, or an explicit acknowledgement, for GitHub,
which ships no timestamp at all):

```python
from varco_core.webhook.inbound import WebhookReplayGuard
from varco_core.idempotency.memory import InMemoryIdempotencyStore  # or a Redis/SA/Beanie backend

guard = WebhookReplayGuard(store=InMemoryIdempotencyStore(), ttl_seconds=600.0)
github_verifier = get_verifier(
    "github", secrets=["..."], replay_guard=guard
)  # or acknowledge_no_replay_protection=True to accept the risk
verify_github = verify_webhook(github_verifier, replay_guard=guard)
```

A tampered/expired signature raises `WebhookSignatureError` (401); a replayed message id raises
`WebhookReplayError` (409). Both render through the standard error envelope with a
`correlation_id`, and neither ever includes the secret or the signature value.

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_WEBHOOK_INBOUND_TOLERANCE_SECONDS` | `300.0` | Clock-skew tolerance accepted on an inbound delivery's timestamp — separate from the outbound `signature_tolerance_seconds` |
| `VARCO_WEBHOOK_INBOUND_REPLAY_TTL_SECONDS` | `600.0` | Default TTL for `WebhookReplayGuard` entries — pass an explicit, longer `ttl_seconds=` for GitHub's multi-hour retry window |

Full design (the provider divergence table, why Standard Webhooks/Svix delegates to the shipped
signer, the replay model, secret sourcing, the raw-body/body-limit story, a Pitfalls table):
`technical_docs/features/inbound-webhooks.md`.

---

## Feature flags

Plan 032 / D7. `varco_core.flags` is a varco-shaped `AbstractFeatureFlags` seam — **not** a
transcription of OpenFeature's `AbstractProvider`. The OpenFeature Python SDK sits at 0.10.0 and
the spec itself at 0.9.0 (verified 2026-09-04); both pre-1.0, and 0.10.0 itself shipped a
breaking change inside a minor release. So the seam ships now and an `OpenFeatureFlags` adapter
is deferred until the SDK — not the spec — reaches 1.0. Full version evidence and the un-park
trigger: `technical_docs/features/feature-flags.md`.

Four typed resolutions (bool/string/numeric/object), each degrading to the caller's own
`default` on any "flag not found" condition — never an exception:

```python
from varco_core.flags import AbstractFeatureFlags, FlagEvaluationContext, InMemoryFeatureFlags

flags = InMemoryFeatureFlags(
    flags={"new_checkout": False},
    tenant_overrides={"acme": {"new_checkout": True}},
)

ctx = FlagEvaluationContext.current()  # tenant_id from current_tenant(), attributes from RequestContext
resolution = await flags.resolve_bool("new_checkout", default=False, context=ctx)
resolution.value    # True inside tenant "acme"'s ambient context, False elsewhere
resolution.reason   # "TENANT_OVERRIDE" / "STATIC" / "DEFAULT"
```

`NullFeatureFlags` (a scanned `@Singleton`, lowest priority) is the DI default — importing or
scanning `varco_core.flags` never changes behaviour for an app that has not opted in. Opt in
explicitly:

```python
from varco_core.flags.di import enable_feature_flags

container.scan("varco_core.flags", recursive=True)  # NullFeatureFlags bound
enable_feature_flags(container)                       # opt-in: InMemoryFeatureFlags
```

`FlagEvaluationContext.tenant_id` comes from `current_tenant()` — never `RequestContext`, same
single-source-of-truth rule the rest of the codebase follows for tenancy.

---

## Recurring schedules

Plan 032 / D6. `varco_core.schedule` closes the loop the job subsystem's zoned fields
(`Job.run_at_wall`/`run_at_tz`/`run_at_fold`) were built for: a `Schedule` entity describes a
recurring cron expression, and `ScheduleMaterializer` turns its next-due occurrence(s) into
ordinary `Job` rows. **No new execution path** — the existing `AbstractJobRunner` runs the
produced jobs exactly as it always has.

```python
from varco_core.schedule.entity import CatchUpPolicy, Schedule
from varco_core.schedule.materializer import ScheduleMaterializer
from varco_core.tz.schedule import GapPolicy, OverlapPolicy

schedule = Schedule(
    cron_expr="0 9 * * 1-5",           # 09:00, weekdays
    timezone="America/New_York",
    gap_policy=GapPolicy.NEXT_VALID,    # spring-forward
    overlap_policy=OverlapPolicy.FIRST, # fall-back
    catchup_policy=CatchUpPolicy.SKIP,  # default — see the Pitfalls table
    payload={"report": "daily-digest"},
)

materializer = ScheduleMaterializer(job_store=job_store)
jobs = await materializer.materialize(schedule)  # [] if not yet due or already materialized
```

Cron parsing is a hand-rolled, zero-dependency 5-field parser (`varco_core.schedule.cron`) —
RRULE/RFC 5545 is explicitly out of scope (a complete implementation means a `dateutil` runtime
dependency for a 🟢 backlog row). Three catch-up policies govern what a materializer that missed
occurrences does about it (`SKIP` default, `FIRE_ONCE`, `BACKFILL_ALL` bounded by
`max_backfill`) — see the Pitfalls table in `technical_docs/features/recurring-schedules.md` for
the surprising cases.

`AbstractScheduleRepository` (`varco_core.schedule.repository`) is the storage contract —
`InMemoryScheduleRepository` for tests, `SAScheduleRepository`/`BeanieScheduleRepository` for
Postgres/Mongo, both with a `UNIQUE(schedule_id)` constraint.

⚠️ Cross-process double-materialization safety does **not** reuse the job store's fenced-lease
primitives — see `technical_docs/features/recurring-schedules.md` for what the materializer
actually does instead (a deterministic occurrence id + a per-schedule in-process lock) and why.

---

## Retention & purge automation

Plan 039 (S20). A `RetentionPolicy` registry materialized onto the cron→`Job` path above —
declare *"prune dead letters older than 30 days at 03:00 Europe/Rome"* once, and varco schedules
and runs it with the same DST-safe cron semantics and `AbstractJobRunner`. **Nothing is scheduled
and nothing is deleted by default.**

```python
from datetime import timedelta

from varco_core.retention.di import bind_retention_registry
from varco_core.retention.policy import RetentionPolicy, RetentionRegistry
from varco_core.retention.targets import AuditRetentionTarget, DlqRetentionTarget
from varco_fastapi.app import create_varco_app
from varco_fastapi.retention import RetentionLifecycle

registry = RetentionRegistry()
registry.register(
    RetentionPolicy(
        name="nightly-dlq",
        target=DlqRetentionTarget(dlq=my_dlq, acknowledge_dead_letter_deletion=True),
        cron_expr="0 3 * * *",
        timezone="Europe/Rome",
        dry_run=True,                  # preview first — would_delete, deletes nothing
        older_than=timedelta(days=30),
    )
)
registry.register(
    RetentionPolicy(
        name="audit-1y",
        target=AuditRetentionTarget(repo=my_audit_repo),
        cron_expr="0 4 * * *",
        timezone="UTC",
        dry_run=False,
        older_than=timedelta(days=365),
    )
)

bind_retention_registry(container, registry)
app = create_varco_app(
    container,
    retention=RetentionLifecycle(registry, container=container, interval=300.0),
)
```

| Target | Wraps | Cutoff? | Preview? |
|---|---|---|---|
| `DlqRetentionTarget` | `AbstractDeadLetterQueue.delete_where` | ✅ | ✅ |
| `AuditRetentionTarget` | `AuditRepository.delete_where` | ✅ | ✅ |
| `IdempotencyRetentionTarget` | `AbstractIdempotencyStore.delete_expired` | ❌ | ❌ (skipped under `dry_run=True`) |
| `RevocationRetentionTarget` | `AbstractTokenRevocationStore.delete_expired` | ❌ | ❌ (skipped under `dry_run=True`) |
| `JobRetentionTarget` | `AbstractJobStore.delete_where` | ✅ | ✅ |
| `CallableRetentionTarget` | any `async (older_than, limit, dry_run) -> int` | declared by caller | declared by caller |

⚠️ **`dry_run` has no default — every policy must say it.** ⚠️ A DLQ policy needs
`acknowledge_dead_letter_deletion=True` — dead letters are the last remaining copy of a failed
event. ⚠️ `older_than < 24h` needs `acknowledge_short_retention=True`. ⚠️ The outbox, encryption
keys, and webhook deliveries are **not** retention targets — see
`technical_docs/features/retention-and-purge.md`'s Pitfalls table for why.

CLI, one policy, one run, no scheduler process needed (e.g. a Kubernetes `CronJob`):

```bash
varco retention prune --policy nightly-dlq --target myapp.wiring:build_retention_registry
varco retention list --target myapp.wiring:build_retention_registry
```

`inspect_retention_posture(registry=registry)` reports `destructive_count`, `dry_run_count`,
`platform_wide_policies` (policies with `tenant_ids=None` — a cross-tenant purge), `dlq_policies`,
and `short_retention_policies` — a pure, never-raising read, same shape as
`inspect_revocation_posture()`.

Full design (the eight safety guards, the three Plan-032 driver gaps this closed, multi-process
convergence, tenancy scoping): `technical_docs/features/retention-and-purge.md`.

---

## File watching and hot reload

`varco_core.watch` (`AbstractPathWatcher`) watches a set of directories for changes and calls
subscribers back — correct under atomic rename and under the Kubernetes `..data` symlink swap
kubelet uses to deliver a rotated Secret/ConfigMap. `varco_core.reload.ReloadableResource[T]`
loads a value from that watcher (or from any manual trigger) and swaps it under a lock, with
**keep-last-good** semantics on any post-startup load failure.

```python
from pathlib import Path

from varco_core.reload import ReloadableResource
from varco_core.watch import WatchTarget, default_watcher

watcher = default_watcher([WatchTarget(root=Path("/etc/certs"))])


def load_bundle() -> bytes:
    return Path("/etc/certs/ca.pem").read_bytes()


bundle = ReloadableResource(loader=load_bundle, watcher=watcher, name="ca-bundle")
await bundle.start()  # first load is fail-fast — propagates if it fails
bundle.current  # -> bytes, updated automatically on every settled rotation
```

**Watcher implementations**:
- `StatPollWatcher` (default, stdlib-only) — polls a stat fingerprint
  (`st_mtime_ns`, `st_size`, `st_ino`) of every matching file, `interval=5.0` by default.
  Correct on NFS and Docker bind mounts, where inotify never fires. `default_watcher(...)`
  always returns one of these — prefer it when you just want "the default", not a specific
  implementation.
- `WatchfilesWatcher` (opt-in, `pip install "varco-core[watch]"`) — lower-latency local
  dev-loop watching backed by the Rust `notify` crate (the same library uvicorn's
  `--reload` uses). Still re-derives events from the same stat-fingerprint diff, never from
  watchfiles' own event kind, so both implementations emit identical event streams.

Both debounce: after any detected change, a watcher waits for the target to be stable for one
`quiet_period` (default `0.25s`) before notifying, coalescing an entire rotation into **one**
callback.

**`ReloadableResource[T]` keep-last-good contract**: the *first* `start()` load is fail-fast — a
service that cannot load its initial CA bundle refuses to start. Every load *after* that keeps
serving `current`/`generation` unchanged on failure and returns
`ReloadOutcome(changed=False, error=exc)` — a truncated or half-written file during rotation
never takes the service down.

**SIGHUP composition** — `reload()` is always independently callable, with or without a
watcher:

```python
import signal

loop = asyncio.get_running_loop()
loop.add_signal_handler(signal.SIGHUP, lambda: asyncio.create_task(bundle.reload()))
```

**`VarcoLifespan` wiring** — both `AbstractPathWatcher` and `ReloadableResource` structurally
satisfy `varco_fastapi.lifespan.AbstractLifecycle` (a `runtime_checkable` Protocol), so they
register with zero import from `varco_core` into `varco_fastapi`:

```python
from varco_fastapi.lifespan import VarcoLifespan

lifespan = VarcoLifespan()
lifespan.register(bundle)  # starts the watcher too — ReloadableResource.start()/stop() own it
```

---

## Security headers

`SecurityHeadersMiddleware` (Plan 035 / S7) sends a baseline of security response headers on
**every** response, including error responses. **On by default** — `create_varco_app()` installs
it at the `BALANCED` preset with no code change:

```python
app = create_varco_app(container)
# GET /anything now carries:
#   X-Content-Type-Options: nosniff
#   X-Frame-Options: DENY
#   Referrer-Policy: strict-origin-when-cross-origin
#   Strict-Transport-Security: max-age=31536000; includeSubDomains   (HTTPS only)
```

Opt into `STRICT` for CSP/COOP/CORP/Permissions-Policy (breaks `/docs`/`/redoc` unless
`exclude_paths` covers them — the default does):

```python
from varco_fastapi.middleware.security_headers import SecurityHeadersPreset, SecurityHeadersSettings

app = create_varco_app(container, security_headers=SecurityHeadersSettings(preset=SecurityHeadersPreset.STRICT))
```

Hand-registered form (position matters — see `technical_docs/features/http-edge-hardening.md`'s
ordering contract: inside `CORSMiddleware`, outside everything else):

```python
from varco_fastapi.middleware.security_headers import SecurityHeadersMiddleware

app.add_middleware(ErrorMiddleware)
app.add_middleware(SecurityHeadersMiddleware)  # added AFTER ErrorMiddleware → ends up outside it
install_cors(app, CORSConfig.from_env())
```

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_SECURITY_HEADERS_ENABLED` | `true` | Master on/off switch |
| `VARCO_SECURITY_HEADERS_PRESET` | `balanced` | `balanced` \| `strict` |
| `VARCO_SECURITY_HEADERS_X_CONTENT_TYPE_OPTIONS` | `nosniff` | Set empty/unset to omit |
| `VARCO_SECURITY_HEADERS_X_FRAME_OPTIONS` | `DENY` | `SAMEORIGIN` for a self-framing app |
| `VARCO_SECURITY_HEADERS_REFERRER_POLICY` | `strict-origin-when-cross-origin` | |
| `VARCO_SECURITY_HEADERS_HSTS_MAX_AGE` | `31536000` | `0` disables HSTS |
| `VARCO_SECURITY_HEADERS_HSTS_INCLUDE_SUBDOMAINS` | `true` | |
| `VARCO_SECURITY_HEADERS_TRUSTED_PROXIES` | `()` | CIDRs trusted for `X-Forwarded-Proto` (HSTS behind a TLS-terminating proxy) |
| `VARCO_SECURITY_HEADERS_EXCLUDE_PATHS` | `("/docs", "/redoc", "/openapi.json")` | Path prefixes exempt from every header |

## Request body limits

`BodyLimitMiddleware` (Plan 035 / S8) enforces a hard ceiling on request-body bytes — a
`Content-Length` pre-check plus a cumulative count over the ASGI `receive()` stream (rejects
before Starlette finishes buffering). **On by default at 10 MiB**:

```python
app = create_varco_app(container)
# POST with a body > 10 MiB now gets a 413 naming the ceiling and VARCO_BODY_LIMIT_MAX_BYTES
```

```python
from varco_fastapi.middleware.body_limit import BodyLimitSettings

app = create_varco_app(
    container,
    body_limit=BodyLimitSettings(max_bytes=50 * 1024 * 1024, exempt_paths=("/upload",)),
)
```

Hand-registered form — must sit inside `ErrorMiddleware` (so the 413 renders through the error
envelope) and outside `IdempotencyMiddleware` (so an over-limit request is rejected before
anything buffers it):

```python
from varco_fastapi.middleware.body_limit import BodyLimitMiddleware

app.add_middleware(BodyLimitMiddleware)
app.add_middleware(ErrorMiddleware)  # added AFTER → ends up outside BodyLimitMiddleware
```

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_BODY_LIMIT_ENABLED` | `true` | Master on/off switch |
| `VARCO_BODY_LIMIT_MAX_BYTES` | `10485760` (10 MiB) | The byte ceiling |
| `VARCO_BODY_LIMIT_EXEMPT_PATHS` | `()` | Path prefixes exempt entirely |
| `VARCO_BODY_LIMIT_TRUST_CONTENT_LENGTH` | `true` | Whether to also do the cheap pre-check (the cumulative count always runs) |

## HTTP rate limiting

`RateLimitMiddleware` (Plan 035 / S10) is the ASGI assembly around the existing
`RateLimiter`/`RateLimitConfig`/`InMemoryRateLimiter` (`varco_core.resilience.rate_limit`) and
`RedisRateLimiter` (`varco_redis.rate_limit`) — no new algorithm. **Off by default** — the one
opt-in row:

```python
from varco_core.resilience.rate_limit import InMemoryRateLimiter, RateLimitConfig
from varco_fastapi.middleware.rate_limit import RateLimitBundle, RateLimitRule, RateLimitScope

bundle = RateLimitBundle(
    rules=(
        RateLimitRule(
            scope=RateLimitScope.IP,
            limiter=InMemoryRateLimiter(RateLimitConfig(rate=100, period=60.0)),
            name="ip",
        ),
        RateLimitRule(
            scope=RateLimitScope.TENANT,
            limiter=InMemoryRateLimiter(RateLimitConfig(rate=1000, period=60.0)),
            name="tenant",
        ),
    ),
)
app = create_varco_app(container, rate_limit=bundle)
# create_varco_app partitions rules by scope automatically: IP/GLOBAL -> stage=PRE_AUTH
# (before auth), SUBJECT/TENANT -> stage=POST_AUTH (after RequestContextMiddleware).
```

Hand-registered form — `IP`/`GLOBAL` rules go **outside** `RequestContextMiddleware`;
`SUBJECT`/`TENANT` rules require `stage=POST_AUTH` and go **inside** it:

```python
from varco_fastapi.middleware.rate_limit import RateLimitMiddleware, RateLimitStage

app.add_middleware(RequestContextMiddleware, server_auth=my_auth)
app.add_middleware(
    RateLimitMiddleware,
    rules=(tenant_rule,),
    stage=RateLimitStage.POST_AUTH,
)  # added AFTER RequestContextMiddleware → ends up inside it
```

An `IP`/`SUBJECT`-scoped rule backed by `InMemoryRateLimiter` raises `ValueError` unless you pass
`acknowledge_unbounded_keyspace=True` — its key space is attacker-controlled (a new source
IP/subject permanently costs a `deque` + `asyncio.Lock`). This applies whether the middleware is
hand-registered or assembled via `create_varco_app(rate_limit=RateLimitBundle(...))`:
`RateLimitBundle` carries its own `acknowledge_unbounded_keyspace` field (default `False`),
forwarded verbatim to both `RateLimitMiddleware` instances `create_varco_app` builds from it — so
`RateLimitBundle(rules=(...), acknowledge_unbounded_keyspace=True)` is required for the bundle form
too. Use `RedisRateLimiter` (TTL-bounded) for any multi-process deployment, and see
`technical_docs/features/http-edge-hardening.md`'s §D-S10-keyspace subsection for the full
reasoning.

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_RATE_LIMIT_ENABLED` | `true` | Master on/off switch for a constructed instance |
| `VARCO_RATE_LIMIT_FAIL_OPEN` | `true` | A limiter exception allows the request (`false` → 503) |
| `VARCO_RATE_LIMIT_TRUSTED_PROXIES` | `()` | CIDRs trusted for `X-Forwarded-For` (the `IP` scope) |
| `VARCO_RATE_LIMIT_TRUSTED_PROXY_HOPS` | `0` | Trusted proxy hops to skip from the right |
| `VARCO_RATE_LIMIT_EMIT_DRAFT_HEADERS` | `false` | Emit the IETF draft-11 `RateLimit-Policy` header |
| `VARCO_RATE_LIMIT_EXEMPT_PATHS` | `()` | Path prefixes exempt from every rule |
| `VARCO_RATE_LIMIT_ERROR_LOG_INTERVAL` | `60.0` | Minimum seconds between repeated "limiter raised"/"unkeyable rule" log lines |

`Retry-After` is always sent on a 429 (RFC 9110 §10.2.3). `RateLimit`/`X-RateLimit-*` are never
emitted — the `RateLimiter` ABC cannot report remaining quota. Full design (scope/stage model,
trusted-proxy trust rule, fail-open reasoning, standards status): see
`technical_docs/features/http-edge-hardening.md`.

**"Is my edge configured?"** — `inspect_http_edge(app)` (also exported from `varco_fastapi`
directly) returns a frozen `HttpEdgePosture` reporting what of the above is actually wired, with
zero startup refusal — it is a pure read, not a preflight (that is Plan 036's `SecurityPosture`).

```python
from varco_fastapi import inspect_http_edge

posture = inspect_http_edge(app)
posture.security_headers_installed   # True
posture.rate_limit_installed         # False, unless you passed rate_limit=
[f.check for f in posture.findings]  # e.g. ["http.rate_limit.absent", "http.error.detail_exposed"]
```

---

## Security posture preflight

`SecurityPostureLifecycle` (Plan 036 / S9) is a startup preflight that aggregates the security
checks the rest of this README documents piecemeal — tenant provenance, auth hardening, HTTP
edge, RLS, and a handful of admin-mount facts — into one report an operator reads at startup.
**Opt-in in 3.2** — an app that never wires it gets no report and no log lines:

```python
from varco_fastapi import (
    SecurityPostureLifecycle,
    SecurityPostureSettings,
    create_varco_app,
)
from varco_fastapi.posture import _collect_http, _collect_local
from functools import partial

lifecycle = SecurityPostureLifecycle(
    collectors=[
        partial(_collect_http, app=app),
        partial(_collect_local, container=container, webhook_repository=webhook_repo),
        # ... _collect_tenant / _collect_auth / _collect_data, each importing its
        # sibling's inspector inside the function body — see the feature doc
    ],
    settings=SecurityPostureSettings(),  # reads VARCO_SECURITY_*
)
app = create_varco_app(container, extra_lifespan_components=[lifecycle])
```

`lifecycle.report` (a frozen `SecurityPosture`) is available once `start()` has run —
`.summary()` renders one line, always separating `NOT_ASSESSED` ("could not check") from the
other three severities so it is never mistaken for a clean report.

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_SECURITY_ENV` | `production` | `"production"` \| `"development"` — **presentation only**; every check still runs in both modes, `WARN` findings are demoted to `INFO` in `development` |
| `VARCO_SECURITY_ENFORCE` | `warn` | `"warn"` \| `"refuse"` — `"refuse"` raises at `start()` on any unsuppressed `HIGH` (or, in `production`, `WARN`) finding; never on `NOT_ASSESSED` alone |
| `VARCO_SECURITY_SUPPRESS` | `""` | Comma-separated `check` ids to suppress — the finding is still produced, demoted to `INFO`, and flagged `suppressed=True` |

Full design (the collector table, the consolidated 4.0 flip list gathering every warn-only
default across the 3.2 cycle, a Pitfalls table): `technical_docs/features/security-posture.md`.

---

## Authorization-decision audit

`AuditingAuthorizer` (Plan 036 / S11) wraps whatever `AbstractAuthorizer` an app has bound and
records the outcome of every `authorize()` call it delegates — a decorator, not a middleware,
because `authorize()` is called from the service layer and never from HTTP middleware (a
middleware would miss jobs, consumers, and the CLI):

```python
from varco_core import enable_authorization_audit

container = DIContainer()
container.scan("myapp", recursive=True)     # binds the app's real AbstractAuthorizer
enable_authorization_audit(container)        # call LAST — see the feature doc's ordering note
```

By default (`AuditDecisionPolicy.DENIALS`) it records every denial plus every allow whose
`AuthContext` carries a delegated `actor` — the exact "who is really acting" property whose
absence was the CVE-2025-55241 (Entra actor token) attack vector. `ALL` records every decision;
`NONE` disables recording while keeping the wrapper installed. Recording is via
`AbstractEventProducer` onto the `"varco.audit"` channel — the same channel `AuditEvent` uses —
never the bus directly.

Full design (the recorded/excluded field allowlist, the policy trade-offs, ordering pitfalls):
`technical_docs/features/authorization-audit.md`.

---

## Composite Deployment

Use `create_composite_app` (`varco_fastapi.composite`) to run several **already-built** varco
services in a single ASGI process. Each service keeps its own container, database,
environment, middleware, and `/docs` — they are mounted as ASGI sub-apps under prefixes.

```python
from varco_fastapi import create_composite_app, ServiceMount

from orders_service.app import app as orders_app  # its own create_varco_app()
from billing_service.app import app as billing_app  # its own container + DB + env

composite = create_composite_app(
    [
        ServiceMount("/orders", orders_app),
        ServiceMount("/billing", billing_app),
    ]
)
# uvicorn composite:composite
```

**Key facts**:
- Mounting is purely additive — existing service code is untouched. Each sub-app serves
  its own `{prefix}/docs` + `{prefix}/openapi.json`; there is no merged OpenAPI schema.
- `create_composite_app` installs a `CompositeLifespan` that drives **each sub-app's own
  lifespan**. This is required: Starlette's `Router.lifespan` does NOT descend into
  mounted sub-apps, so without it every service's DB pool / `AbstractEventBus` /
  `OutboxRelay` would silently never start.
- Startup is **fail-fast** — one service failing to start aborts the whole process
  (no half-broken deployment). Shutdown is LIFO.
- `aggregate_health=True` (default) exposes a root `GET /health` that probes each
  service's own health in-process (via `httpx.ASGITransport`) and returns 503 if any is
  unhealthy — one readiness signal for the whole deployment.
- **Env isolation**: all services share one `os.environ`. Runtime isolation is automatic
  (each app holds its own container/engine objects). The only hazard is *build-time*
  env-name collision — two services reading bare `os.environ["DATABASE_URL"]` see the
  same value. Fix by namespacing env vars per service, or use
  `build_service(prefix, factory, env={...})` which overlays a scoped environment only
  while that one service is built, then restores it.
- No composite-level middleware by default — each sub-app owns its full middleware stack
  and runs it exactly as standalone (avoids double-processing, e.g. tracing wrapped twice).

Design detail: `technical_docs/features/composite-deployment.md`.

---

## Durability preset (one-line opt-in)

```python
from varco_core.reliability import ReliabilityPreset
from varco_fastapi import create_varco_app
from varco_sa.dlq import SADeadLetterQueue

dlq = SADeadLetterQueue(engine)
app = create_varco_app(container, routers=[...], reliability=ReliabilityPreset.durable(dlq=dlq))
```

Turns on `RetryPolicy.durable_delivery()` + the DLQ for every bare `@listen(...)` handler (via
`set_default_reliability_preset()`'s resolution at `register_to()` time), starts an
`OutboxRelay`, wires an `AuditConsumer`, and installs the reliability metrics pack — all from
one preset object. `reliability=None` (the default) registers nothing — byte-identical to not
using this feature. See `technical_docs/features/reliability-preset.md`.

---

## Changelog summary

Full migration notes live under `technical_docs/migrations/`.

**Plan 009 — Reliability & Service Integration**
([full note](technical_docs/migrations/009-reliability-and-integration.md)):
DLQ redrive (`DlqRedriver`, `varco dlq`) and retention/pruning
(`delete_where()`, `varco retention prune`) across SA/Redis/Beanie (Kafka/NATS
raise naming their own retention mechanism); tenant-scoped DLQ + audit
entries with optional Postgres RLS; a reliability metrics pack
(`varco.dlq.*`/`varco.outbox.*`/`varco.audit.*`); a one-call
`ReliabilityPreset` for "opt into durability once"; a bundled
`mount_reliability_admin()` REST surface; opt-in audit hash-chaining for
tamper evidence; a collapsed client front door (`client_for`) plus typed
custom-route codegen for cross-repo consumers (`varco export-contract` /
`varco gen-client`); and `PeerRegistry` for "one env var, one inject" peer
service calls. ⚠️ Breaking: `AuditRepository.list_for_entity()` gains a
keyword-only `tenant_id=`; `make_client`/`GenericClient`/`OpenAPIClient`/
`ClientConfigurator`/`generate_client` moved to `varco_fastapi.client.advanced`.
See the migration note for the full breaking-change table and a known gap
(`client_for()`'s custom-route methods are not yet typed via the new
contract machinery).

---

## Health Checks

`varco_core.health` provides liveness and readiness probe abstractions. Every backend package ships a concrete `HealthCheck` subclass.

```python
from varco_core.health import HealthCheck, HealthResult, HealthStatus, CompositeHealthCheck
```

### HealthCheck (ABC)

```python
from varco_core.health import HealthCheck, HealthResult, HealthStatus


class RedisHealthCheck(HealthCheck):
    name = "redis"

    async def check(self) -> HealthResult:
        try:
            await self._client.ping()
            return HealthResult(status=HealthStatus.HEALTHY, component=self.name)
        except Exception as exc:
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                component=self.name,
                detail=str(exc),
            )
```

> **Rule**: `check()` must **never raise** — return `UNHEALTHY` instead. A probe that raises crashes the health endpoint.

### CompositeHealthCheck

Runs all probes concurrently and reduces to the worst-case status:

```python
from varco_core.health import CompositeHealthCheck

composite = CompositeHealthCheck(
    [
        redis_health,
        postgres_health,
        kafka_health,
    ]
)

result = await composite.check()
# result.status → HealthStatus.UNHEALTHY if any probe is unhealthy
```

| `HealthStatus` | Meaning |
|---|---|
| `HEALTHY` | Component is fully operational |
| `DEGRADED` | Component is operational but with reduced capacity |
| `UNHEALTHY` | Component is unavailable |

### FastAPI health endpoint

```python
from varco_fastapi.router.health import health_router

app.include_router(health_router)
# GET /health → {"status": "healthy", "components": [...]}
```
