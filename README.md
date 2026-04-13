# varco

A modular Python framework for building expressive, backend-agnostic REST APIs on top of SQLAlchemy and MongoDB (Beanie/Motor). It provides a clean domain model layer, a generic service layer with built-in authorization, a fluent query builder with AST-based filtering, automatic ORM class generation, and a pluggable type coercion system.

---

## Packages

| Package | Description |
|---|---|
| `varco_core` | Backend-agnostic domain model, service layer, authorization, assembler, query AST, builder, parser, DTOs, event system, cache framework, resilience patterns, JWT authority, observability, health checks |
| `varco_fastapi` | FastAPI integration — CRUD router, mixins, JWT auth middleware, job runner, HTTP client utilities |
| `varco_sa` | SQLAlchemy async backend (ORM generation, repository, schema guard, Alembic helpers) |
| `varco_beanie` | Beanie (Motor/MongoDB) async backend |
| `varco_kafka` | Apache Kafka event bus backend (`KafkaEventBus` via aiokafka) |
| `varco_redis` | Redis Pub/Sub event bus + cache backend (`RedisEventBus`, `RedisCache` via redis.asyncio) |
| `varco_ws` | WebSocket and SSE event bus adapters |
| `varco_memcached` | Memcached cache backend |

---

---

## Quickstart — Example App

The [`example/`](example/) directory contains a **complete, runnable Post API** that wires the full varco stack together:

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
uv sync
cd example && docker compose up -d
# app at http://localhost:8000/docs
```

Full setup, API reference, architecture diagram, and extension guide: **[example/README.md](example/README.md)**

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
- [Correlation ID / Tracing](#correlation-id--tracing)
- [Multi-tenancy (DB-level)](#multi-tenancy-db-level)
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
- [Beanie Backend](#beanie-backend)
  - [Bootstrap](#bootstrap-beanie)
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
  - [Key sources](#key-sources)
- [Connection Settings](#connection-settings)
  - [SSLConfig](#sslconfig)
  - [RedisConnectionSettings](#redisconnectionsettings)
  - [HttpConnectionSettings](#httpconnectionsettings)
- [FastAPI Integration](#fastapi-integration)
  - [VarcoRouter and VarcoCRUDRouter](#varcorouter-and-varcoCRUDRouter)
  - [CRUD mixins](#crud-mixins)
  - [JWT authentication middleware](#jwt-authentication-middleware)
  - [Request context](#request-context)
  - [Middleware stack](#middleware-stack)
  - [Job runner — async mode](#job-runner--async-mode)
  - [Bootstrap helpers](#bootstrap-helpers)
- [Observability](#observability)
  - [@span — distributed tracing](#span--distributed-tracing)
  - [@counter and @histogram](#counter-and-histogram)
  - [TracingServiceMixin](#tracingservicemixin)
  - [OtelConfig and DI wiring](#otelconfig-and-di-wiring)
- [Health Checks](#health-checks)
- [Exception Hierarchy](#exception-hierarchy)

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
        lambda data: {**data, "active": True},                       # v0 → v1
        lambda data: {**data, "email": data["email"].lower()},       # v1 → v2
    ]
```

---

## Metadata & Constraints

```python
from varco_core.meta import (
    FieldHint, PrimaryKey, PKStrategy, ForeignKey,
    UniqueConstraint, CheckConstraint, pk_field,
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
await repo.find_by_id(pk)            # D | None
await repo.find_all()                # list[D]
await repo.save(entity)              # D  (INSERT or UPDATE)
await repo.delete(entity)
await repo.find_by_query(params)     # list[D]
await repo.count(params)             # int
await repo.exists(pk)                # bool  — lightweight, no ORM hydration
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
patch = TagUpdate(tags=["old"],    op=UpdateOperation.REMOVE)  # remove
patch = TagUpdate(tags=["new"],    op=UpdateOperation.REPLACE) # overwrite (default)
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
            body=dto.body   if dto.body  is not None else entity.body,
        )
```

The shorthand `Assembler` alias saves typing in service `__init__` signatures:

```python
from varco_core import Assembler   # TypeAlias for AbstractDTOAssembler

def __init__(self, assembler: Inject[Assembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]]):
    ...
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
class PostService(
    TenantAwareService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO]
):
    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer:   Inject[AbstractAuthorizer],
        assembler:    Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]],
    ) -> None:
        super().__init__(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

    def _get_repo(self, uow): return uow.posts
```

What the hooks inject:

- `_pre_check` — raises `ServiceAuthorizationError` if `ctx.metadata["tenant_id"]` is absent, before any DB access
- `_scoped_params` — prepends `tenant_id = <tid>` to every query
- `_check_entity` — raises `ServiceNotFoundError` (404, not 403) for cross-tenant entities
- `_prepare_for_create` — stamps `tenant_id` from the JWT onto every new entity

By default the field name is `"tenant_id"`. Override `_tenant_field` to use a different name:

```python
class PostService(TenantAwareService[Post, ...]):
    _tenant_field = "org_id"   # uses Post.org_id instead of Post.tenant_id
    def _get_repo(self, uow): return uow.posts
```

### SoftDeleteService

Replaces physical deletion with a `deleted_at` timestamp and excludes soft-deleted records from all queries:

```python
from varco_core import SoftDeleteService

@Singleton
class PostService(
    SoftDeleteService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO]
):
    def __init__(self, uow_provider, authorizer, assembler): ...
    def _get_repo(self, uow): return uow.posts
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
    def _get_repo(self, uow): return uow.posts
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
class PostAssembler(AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]):
    ...

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
Action.READ    # "read"
Action.UPDATE  # "update"
Action.DELETE  # "delete"
Action.LIST    # "list"

Action.READ == "read"  # True
```

### ResourceGrant

```python
from varco_core.auth import ResourceGrant, Action

ResourceGrant("posts",        frozenset({Action.LIST, Action.CREATE, Action.READ}))
ResourceGrant("posts:abc123", frozenset({Action.UPDATE, Action.DELETE}))
ResourceGrant("*",            frozenset(Action))   # wildcard — admin
```

### AuthContext

```python
from varco_core.auth import AuthContext, ResourceGrant, Action

ctx = AuthContext(
    user_id="usr_123",
    roles=frozenset({"editor"}),
    grants=(
        ResourceGrant("posts",        frozenset({Action.LIST, Action.READ})),
        ResourceGrant("posts:abc123", frozenset({Action.UPDATE, Action.DELETE})),
    ),
    metadata={"tenant_id": "acme"},   # arbitrary bag — used by TenantAwareService
)

ctx.is_anonymous()                    # False
ctx.has_role("editor")                # True
ctx.can(Action.READ,   "posts")       # True  — type-level grant
ctx.can(Action.UPDATE, "posts")       # False — no type-level UPDATE
ctx.can(Action.UPDATE, "posts:abc123") # True  — instance-level grant
```

Anonymous (unauthenticated) caller:

```python
ctx = AuthContext()           # user_id=None, no grants
ctx.is_anonymous()            # True
ctx.can(Action.READ, "posts") # False
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
    _owner_field = "author_id"   # default is "owner_id"

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
        "admin":  frozenset(Action),                                      # all actions
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
assert not isinstance(container.get(AbstractAuthorizer), BaseAuthorizer), \
    "No real authorizer registered — refusing to start"
```

---

## Error codes & HTTP mapping

`FastrestErrorCodes` is a Python `Enum` where each member's `.value` is a frozen `ErrorCode` dataclass. Stable code strings (e.g. `"FASTREST_001"`) serve as i18n translation catalog keys:

```python
from varco_core import FastrestErrorCodes, ErrorCode

FastrestErrorCodes.NOT_FOUND.code           # "FASTREST_001"
FastrestErrorCodes.NOT_FOUND.http_status    # 404
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
    # msg.model_dump() → {"code": "FASTREST_001", "http_status": 404,
    #                      "message": "The requested resource was not found.",
    #                      "detail": "Post with pk=42 not found."}
    return JSONResponse(status_code=msg.http_status, content=msg.model_dump())
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

## Correlation ID / Tracing

Attach a correlation ID to every log record in the current async task:

```python
from varco_core import (
    generate_correlation_id,   # → str (UUID4)
    current_correlation_id,    # → str | None
    correlation_context,       # async context manager
    CorrelationIdFilter,       # logging.Filter
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
cid = current_correlation_id()   # "f47ac10b-58cc-4372-a567-0e02b2c3d479"
```

The correlation ID is stored in a `ContextVar` — each asyncio task gets its own isolated copy. Tasks spawned inside `correlation_context()` inherit the ID automatically.

---

## Multi-tenancy (DB-level)

`TenantUoWProvider` routes `make_uow()` to a per-tenant backend (separate DB or schema):

```python
from varco_core import TenantUoWProvider, tenant_context, current_tenant
from varco_sa import SQLAlchemyRepositoryProvider

provider = TenantUoWProvider({
    "acme":   SQLAlchemyRepositoryProvider(engine_acme, sessions_acme),
    "globex": SQLAlchemyRepositoryProvider(engine_globex, sessions_globex),
})

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
provider.has_tenant("acme")         # True
provider.registered_tenants()       # ["acme", "globex"]
current_tenant()                    # "acme" (inside a tenant_context block)
```

---

## Query System

### QueryBuilder

Fluent, immutable builder — every method returns a new instance:

```python
from varco_core import QueryBuilder, QueryParams, SortField, SortOrder

# Simple equality filter
params = QueryParams(node=QueryBuilder().eq("active", True).build(), limit=10)

# Compound filter
node = (
    QueryBuilder()
    .eq("active", True)
    .gte("age", 18)
    .like("name", "Alice%")
    .build()
)

# OR / NOT / IN / NULL
adult_or_admin = QueryBuilder().gte("age", 18).or_(QueryBuilder().eq("role", "admin")).build()
not_banned     = QueryBuilder().eq("banned", True).not_().build()
status_filter  = QueryBuilder().in_("status", ["active", "trial"]).build()
unverified     = QueryBuilder().is_null("verified_at").build()
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
    offset=40,   # page 3 of 20
)
```

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
    coerce_int, coerce_float, coerce_boolean, coerce_datetime, coerce_list,
    TypeCoercionRegistry, ASTTypeCoercion, register_default_coercer,
)

coerce_int("42")                        # 42
coerce_boolean("yes")                   # True
coerce_datetime("2024-01-15T10:30:00Z") # datetime(...)
coerce_list('["a","b"]')                # ['a', 'b']
coerce_list('a,b,c')                    # ['a', 'b', 'c']

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
    __event_type__ = "order.placed"   # stable cross-process identifier
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

---

### Priority

Higher `priority` values run first.  Equal priorities run in subscription
order (FIFO):

```python
bus.subscribe(OrderPlacedEvent, handler_a, priority=10)   # runs first
bus.subscribe(OrderPlacedEvent, handler_b, priority=0)    # runs second

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
    await task   # optional: wait for all handlers to finish
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
    topic_prefix="prod.",          # optional — "prod.orders", "prod.payments"
    auto_offset_reset="latest",
)

async with KafkaEventBus(config) as bus:
    # Publisher
    producer = BusEventProducer(bus)
    await producer._produce(OrderPlacedEvent(order_id="1"), channel="orders")

    # Consumer
    class OrderConsumer(EventConsumer):
        @listen(OrderPlacedEvent, channel="orders")
        async def on_placed(self, event: OrderPlacedEvent) -> None:
            ...

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
    channel_prefix="prod:",         # optional — "prod:orders", "prod:payments"
)

async with RedisEventBus(config) as bus:
    # Publisher
    producer = BusEventProducer(bus)
    await producer._produce(OrderPlacedEvent(order_id="1"), channel="orders")

    # Consumer
    class OrderConsumer(EventConsumer):
        @listen(OrderPlacedEvent, channel="orders")
        async def on_placed(self, event: OrderPlacedEvent) -> None:
            ...

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
    poll_interval=5.0,   # seconds between polls
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

class Base(DeclarativeBase): pass

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")

app = SAFastrestApp(SAConfig(
    engine=engine,
    base=Base,
    entity_classes=(User, Post, Subscription),
))

await app.create_all()                     # CREATE TABLE IF NOT EXISTS ...
uow_provider = app.uow_provider            # inject this into services
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
        print(drift)   # "Column 'users.phone' missing from database"
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

## Beanie Backend

### Installation

```
pip install varco-beanie
```

### Bootstrap (Beanie)

```python
from motor.motor_asyncio import AsyncIOMotorClient
from varco_beanie import BeanieConfig, BeanieFastrestApp

client = AsyncIOMotorClient("mongodb://localhost:27017")

app = BeanieFastrestApp(BeanieConfig(
    motor_client=client,
    db_name="mydb",
    entity_classes=(User, Post),
))

await app.init()                    # calls beanie.init_beanie() internally
uow_provider = app.uow_provider     # inject into services
```

Manual setup:

```python
from varco_beanie import BeanieRepositoryProvider

provider = BeanieRepositoryProvider(motor_client=client, db_name="mydb")
provider.register(User, Post)
await provider.init()

async with provider.make_uow() as uow:
    user = await uow.users.save(User(name="Bob", email="bob@example.com"))
    recent = await uow.posts.find_by_query(QueryParams(
        node=QueryBuilder().eq("published", True).build(),
        sort=[SortField("created_at", SortOrder.DESC)],
        limit=10,
    ))
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

---

## Cache System

`varco_core.cache` provides a backend-agnostic async cache framework with pluggable invalidation strategies. `varco_redis` ships a Redis-backed implementation.

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
    user = await cache.get("user:42")   # None after 300 s
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

### Invalidation strategies

```python
from varco_core.cache import (
    TTLStrategy,          # time-based expiry
    ExplicitStrategy,     # manual invalidation via cache.delete()
    TaggedStrategy,       # bulk invalidation by tag
    EventDrivenStrategy,  # bus-event-triggered invalidation
    CompositeStrategy,    # logical OR of multiple strategies
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
    CacheServiceMixin,   # ← LEFT side so caching wraps all CRUD
    AsyncService[Post, UUID, PostCreate, PostRead, PostUpdate],
):
    _cache_backend = Inject[CacheBackend]   # injected from DI
    _cache_namespace = "posts"
    _cache_ttl = 300

    def _get_repo(self, uow): return uow.posts
```

`get()` results are cached automatically; `update()` and `delete()` evict the entry.

### @cached decorator

Cache any async callable independently of the service layer:

```python
from varco_core.cache import cached, InMemoryCache, TTLStrategy

cache = InMemoryCache(strategy=TTLStrategy(300))

@cached(cache=cache, key_fn=lambda self, user_id: f"profile:{user_id}", ttl=60)
async def get_user_profile(self, user_id: UUID) -> UserProfile:
    return await self._repo.find_by_id(user_id)
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
    bus=event_bus,                   # publish cross-process invalidation events
    bus_channel="posts.invalidations",
)

post = await cached_svc.get(post_id)      # cache miss → fetched, stored
posts = await cached_svc.list()           # cached list
await cached_svc.update(post_id, dto)     # evicts + publishes invalidation event
```

---

## Resilience

`varco_core.resilience` provides six composable resilience decorators for both sync and async callables.

### retry

Retries a failing function with exponential back-off and optional jitter:

```python
from varco_core.resilience import retry, RetryPolicy, RetryExhaustedError

@retry(RetryPolicy(
    max_attempts=3,
    base_delay=0.5,
    max_delay=10.0,
    jitter=True,
    retryable=(httpx.HTTPError, TimeoutError),
))
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
@circuit_breaker(CircuitBreakerConfig(
    failure_threshold=5,
    recovery_timeout=60.0,
    success_threshold=2,
))
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

@timeout(10.0)   # seconds
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

@hedge(HedgeConfig(delay=0.1))   # fire second attempt after 100 ms
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
    algorithm="RS256",   # RS256, ES256, or HS256
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
config = AuthorizationConfig(issuers=[
    IssuerConfig(issuer="my-svc",    source="pem_file", path="/etc/keys/svc.pub"),
    IssuerConfig(issuer="google",    source="oidc",     discovery_url="https://accounts.google.com"),
    IssuerConfig(issuer="corporate", source="jwks_url", jwks_url="https://auth.corp.internal/.well-known/jwks.json"),
])
registry = config.to_registry()
await registry.load_all()
```

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
    CreateMixin, ReadMixin, UpdateMixin, PatchMixin, DeleteMixin, ListMixin,
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
    _version = "v1"           # adds /v1/orders prefix

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
    _create_async_capable = True    # allow ?with_async=true
```

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
    _auth = auth   # ClassVar — all routes use this auth strategy
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
auth = ctx.auth           # AuthContext (user_id, roles, grants)
jwt  = ctx.jwt            # raw JWT payload dict
request = ctx.request     # FastAPI Request object
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
    sa_bootstrap,       # SQLAlchemy repo provider + UoW
    redis_bootstrap,    # Redis event bus
    ws_bootstrap,       # WebSocket + SSE adapters
    fastapi_bootstrap,  # FastAPI defaults (auth, CORS, job runner, producer)
    redis_async_bootstrap,  # Redis cache (async — call inside lifespan)
)
from varco_fastapi.lifespan import VarcoLifespan

container = DIContainer()
sa_bootstrap(container)
redis_bootstrap(container, streams=True)    # use Redis Streams (at-least-once)
ws_bootstrap(container)
fastapi_bootstrap(container, setup_producer=True)

app = FastAPI(lifespan=VarcoLifespan(container))
```

---

## Observability

`varco_core.observability` provides OpenTelemetry tracing and metrics.

### @span — distributed tracing

```python
from varco_core.observability import span, SpanConfig

@span                        # auto-named from function name
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
    TracingServiceMixin,     # wraps get/list/create/update/delete in OTel spans
    AsyncService[Order, UUID, OrderCreate, OrderRead, OrderUpdate],
):
    def _get_repo(self, uow): return uow.orders
```

### OtelConfig and DI wiring

```python
from varco_core.observability import OtelConfig, OtelConfiguration
from providify import DIContainer

config = OtelConfig(
    service_name="orders-svc",
    otlp_endpoint="http://otel-collector:4317",
    service_version="1.2.0",
)

container = DIContainer()
container.install(OtelConfiguration, config=config)
```

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

composite = CompositeHealthCheck([
    redis_health,
    postgres_health,
    kafka_health,
])

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
