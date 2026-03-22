# fastrest

A modular Python framework for building expressive, backend-agnostic REST APIs on top of SQLAlchemy and MongoDB (Beanie/Motor). It provides a clean domain model layer, a generic service layer with built-in authorization, a fluent query builder with AST-based filtering, automatic ORM class generation, and a pluggable type coercion system.

---

## Packages

| Package | Description |
|---|---|
| `fastrest_core` | Backend-agnostic domain model, service layer, authorization, assembler, query AST, builder, parser, DTOs |
| `fastrest_sa` | SQLAlchemy async backend (ORM generation, repository, schema guard, Alembic helpers) |
| `fastrest_beanie` | Beanie (Motor/MongoDB) async backend |

---

## Table of Contents

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
- [SQLAlchemy Backend](#sqlalchemy-backend)
  - [Bootstrap (one-liner setup)](#bootstrap-one-liner-setup)
  - [Alembic helpers](#alembic-helpers)
  - [Schema Guard](#schema-guard)
- [Beanie Backend](#beanie-backend)
  - [Bootstrap](#bootstrap-beanie)
- [Exception Hierarchy](#exception-hierarchy)

---

## Domain Model

All domain entities inherit from one of three base classes:

```python
from fastrest_core import DomainModel, AuditedDomainModel, VersionedDomainModel
```

| Class | Extra fields |
|---|---|
| `DomainModel` | `pk` only |
| `AuditedDomainModel` | `pk`, `created_at`, `updated_at` |
| `VersionedDomainModel` | `pk`, `created_at`, `updated_at`, `definition_version`, `row_version` |

```python
from __future__ import annotations
from typing import Annotated
from fastrest_core import AuditedDomainModel
from fastrest_core.meta import FieldHint, PrimaryKey, PKStrategy, pk_field

class User(AuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    name: Annotated[str, FieldHint(max_length=100)]
    email: Annotated[str, FieldHint(unique=True, max_length=255)]
    active: bool = True
```

### Soft Delete

Inherit from one of the soft-delete bases to get a `deleted_at: datetime | None` field:

```python
from fastrest_core import SoftDeleteDomainModel, SoftDeleteAuditedDomainModel

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
from fastrest_core import SoftDeleteMixin, VersionedDomainModel

class Document(SoftDeleteMixin, VersionedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    content: str
```

The `SoftDeleteService` mixin (see [below](#softdeleteservice)) automatically excludes soft-deleted records from all queries and replaces physical deletion with a timestamp stamp.

### Multi-tenancy models

```python
from fastrest_core import TenantDomainModel, TenantAuditedDomainModel

class Post(TenantAuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    tenant_id: Annotated[str, FieldHint(index=True, nullable=False)]
    title: str
```

Or add `TenantMixin` to any base:

```python
from fastrest_core import TenantMixin, SoftDeleteAuditedDomainModel

class Post(TenantMixin, SoftDeleteAuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    tenant_id: Annotated[str, FieldHint(index=True)]
    title: str
```

### Schema versioning & migration

```python
from fastrest_core import DomainMigrator

class UserMigrator(DomainMigrator):
    steps = [
        lambda data: {**data, "active": True},                       # v0 → v1
        lambda data: {**data, "email": data["email"].lower()},       # v1 → v2
    ]
```

---

## Metadata & Constraints

```python
from fastrest_core.meta import (
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
from fastrest_core.meta import UniqueConstraint, CheckConstraint

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
from fastrest_core import AsyncRepository, AsyncUnitOfWork
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
from fastrest_core import AsyncRepository

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
from fastrest_core import CreateDTO, ReadDTO, UpdateDTO, UpdateOperation
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
from fastrest_core.assembler import AbstractDTOAssembler
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
from fastrest_core import Assembler   # TypeAlias for AbstractDTOAssembler

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
from fastrest_core import TenantAwareService, IUoWProvider
from fastrest_core.assembler import AbstractDTOAssembler
from fastrest_core.auth import AbstractAuthorizer
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
from fastrest_core import SoftDeleteService

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
from fastrest_core import ServiceProtocol

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
from fastrest_core.auth import Action

Action.CREATE  # "create"
Action.READ    # "read"
Action.UPDATE  # "update"
Action.DELETE  # "delete"
Action.LIST    # "list"

Action.READ == "read"  # True
```

### ResourceGrant

```python
from fastrest_core.auth import ResourceGrant, Action

ResourceGrant("posts",        frozenset({Action.LIST, Action.CREATE, Action.READ}))
ResourceGrant("posts:abc123", frozenset({Action.UPDATE, Action.DELETE}))
ResourceGrant("*",            frozenset(Action))   # wildcard — admin
```

### AuthContext

```python
from fastrest_core.auth import AuthContext, ResourceGrant, Action

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
from fastrest_core.auth import AbstractAuthorizer, Action, AuthContext, Resource
from fastrest_core.exception.service import ServiceAuthorizationError

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
from fastrest_core import GrantBasedAuthorizer

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
from fastrest_core import OwnershipAuthorizer

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
from fastrest_core import RoleBasedAuthorizer
from fastrest_core.auth import Action

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
from fastrest_core.base_authorizer import BaseAuthorizer

# Development / testing only — never ship this to production without shadowing it
container.scan("fastrest_core.base_authorizer")

# Production guard
assert not isinstance(container.get(AbstractAuthorizer), BaseAuthorizer), \
    "No real authorizer registered — refusing to start"
```

---

## Error codes & HTTP mapping

`FastrestErrorCodes` is a Python `Enum` where each member's `.value` is a frozen `ErrorCode` dataclass. Stable code strings (e.g. `"FASTREST_001"`) serve as i18n translation catalog keys:

```python
from fastrest_core import FastrestErrorCodes, ErrorCode

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
from fastrest_core.exception.service import ServiceException
from fastrest_core.exception.http import error_message_for

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
from fastrest_core import ErrorCode, register_error_code
from fastrest_core.exception.service import ServiceException

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
from fastrest_core import (
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
from fastrest_core import TenantUoWProvider, tenant_context, current_tenant
from fastrest_sa import SQLAlchemyRepositoryProvider

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
from fastrest_core import QueryBuilder, QueryParams, SortField, SortOrder

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
from fastrest_core import QueryParser

parser = QueryParser()
node = parser.parse('status = "active" AND age >= 18')
params = QueryParams(node=node, limit=50)
```

Grammar supports: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `IN`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`.

### Type Coercion

```python
from fastrest_core.query.visitor.type_coercion import (
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

## SQLAlchemy Backend

### Installation

```
pip install fastrest-sa
```

### Bootstrap (one-liner setup)

`SAFastrestApp` wires engine, ORM generation, and UoW provider in one place:

```python
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from fastrest_sa import SAConfig, SAFastrestApp

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
from fastrest_sa import SQLAlchemyRepositoryProvider

provider = SQLAlchemyRepositoryProvider(engine=engine, session_factory=session_factory)
provider.register(User, Post, Subscription)
await provider.create_all()

async with provider.make_uow() as uow:
    user = await uow.users.save(User(name="Alice", email="alice@example.com"))
    posts = await uow.posts.find_by_query(QueryParams(limit=20))
```

### Alembic helpers

Use fastrest-generated metadata in your Alembic `env.py` without duplicating table definitions:

```python
# alembic/env.py
from fastrest_sa import get_target_metadata
from myapp.models import User, Post, Subscription

target_metadata = get_target_metadata(User, Post, Subscription)
```

Preview the DDL before running a migration:

```python
from fastrest_sa import print_create_ddl

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
from fastrest_sa import SchemaGuard

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
pip install fastrest-beanie
```

### Bootstrap (Beanie)

```python
from motor.motor_asyncio import AsyncIOMotorClient
from fastrest_beanie import BeanieConfig, BeanieFastrestApp

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
from fastrest_beanie import BeanieRepositoryProvider

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
from fastrest_beanie import BeanieModule, BeanieSettings, bind_repositories
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
from fastrest_core.exception.service import (
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
python -m pytest

# One package at a time
python -m pytest fastrest_core/
python -m pytest fastrest_sa/
python -m pytest fastrest_beanie/
```
