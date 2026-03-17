# fastrest

A modular Python framework for building expressive, backend-agnostic REST APIs on top of SQLAlchemy and MongoDB (Beanie/Motor). It provides a clean domain model layer, a generic service layer with built-in authorization, a fluent query builder with AST-based filtering, automatic ORM class generation, and a pluggable type coercion system.

---

## Packages

| Package | Description |
|---|---|
| `fastrest_core` | Backend-agnostic domain model, service layer, authorization, assembler, query AST, builder, parser, DTOs |
| `fastrest_sa` | SQLAlchemy async backend (ORM generation, repository, schema guard) |
| `fastrest_beanie` | Beanie (Motor/MongoDB) async backend |

---

## Table of Contents

- [Domain Model](#domain-model)
- [Metadata & Constraints](#metadata--constraints)
- [Repository & Unit of Work](#repository--unit-of-work)
- [DTOs](#dtos)
- [DTO Assembler](#dto-assembler)
- [Service Layer](#service-layer)
  - [IUoWProvider](#iuowprovider)
  - [AsyncService](#asyncservice)
  - [Authorization order](#authorization-order)
  - [DI wiring](#di-wiring)
- [Authorization](#authorization)
  - [Action](#action)
  - [ResourceGrant](#resourcegrant)
  - [AuthContext](#authcontext)
  - [AbstractAuthorizer](#abstractauthorizer)
  - [BaseAuthorizer](#baseauthorizer)
- [Query System](#query-system)
  - [QueryBuilder](#querybuilder)
  - [QueryParams](#queryparams)
  - [QueryParser](#queryparser)
  - [Type Coercion](#type-coercion)
- [SQLAlchemy Backend](#sqlalchemy-backend)
  - [Provider setup](#provider-setup)
  - [Schema Guard](#schema-guard)
- [Beanie Backend](#beanie-backend)
  - [Provider setup](#beanie-provider-setup)
  - [DI integration](#di-integration-providify)
- [Exception Hierarchy](#exception-hierarchy)

---

## Domain Model

All domain entities inherit from one of three base classes:

```python
from fastrest_core import DomainModel, AuditedDomainModel, VersionedDomainModel
```

| Class | Extra fields |
|---|---|
| `DomainModel` | `id` only |
| `AuditedDomainModel` | `id`, `created_at`, `updated_at` |
| `VersionedDomainModel` | `id`, `created_at`, `updated_at`, `definition_version`, `row_version` |

```python
from __future__ import annotations
from typing import Annotated
from fastrest_core import AuditedDomainModel
from fastrest_core.meta import FieldHint, PrimaryKey, PKStrategy, pk_field

class User(AuditedDomainModel):
    id: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    name: Annotated[str, FieldHint(max_length=100)]
    email: Annotated[str, FieldHint(unique=True, max_length=255)]
    active: bool = True
```

### Accessing the underlying ORM object

```python
# After saving through a repository the entity is "persisted"
assert user.is_persisted()

# Access the raw SA / Beanie object (raises RuntimeError if not persisted)
raw_orm = user.raw()

# Type-safe cast
from fastrest_core import cast_raw
from fastrest_sa import SAModelRegistry

OrmUser = SAModelRegistry.get(User)
orm_obj = cast_raw(user, OrmUser)
```

### Schema versioning & migration

```python
from fastrest_core import DomainMigrator

class UserMigrator(DomainMigrator):
    steps = [
        lambda data: {**data, "active": True},           # v0 → v1
        lambda data: {**data, "email": data["email"].lower()},  # v1 → v2
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
    id: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    title: Annotated[str, FieldHint(max_length=200, nullable=False)]
    slug: Annotated[str, FieldHint(unique=True, index=True, max_length=200)]
    author_id: Annotated[int, ForeignKey("users.id", on_delete="CASCADE")]
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

    id: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
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
await repo.find_by_id(pk)           # D | None
await repo.find_all()               # list[D]
await repo.save(entity)             # D  (INSERT or UPDATE)
await repo.delete(entity)
await repo.find_by_query(params)    # list[D]
await repo.count(params)            # int
```

### Custom repository

```python
from fastrest_core import AsyncRepository

class UserRepository(AsyncRepository[User, int]):
    async def find_active(self) -> list[User]:
        params = QueryParams(node=QueryBuilder().eq("active", True).build())
        return await self.find_by_query(params)
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

# op controls how the field is applied:
patch = TagUpdate(tags=["python"], op=UpdateOperation.EXTEND)  # append
patch = TagUpdate(tags=["old"],    op=UpdateOperation.REMOVE)  # remove
patch = TagUpdate(tags=["new"],    op=UpdateOperation.REPLACE) # overwrite (default)
```

---

## DTO Assembler

`AbstractDTOAssembler[D, C, R, U]` is the only layer responsible for translating between domain entities and DTOs. It keeps mapping logic out of the service and makes it independently testable and swappable.

```python
from fastrest_core.assembler import AbstractDTOAssembler
from dataclasses import replace

class PostAssembler(AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]):

    def to_domain(self, dto: CreatePostDTO) -> Post:
        # Return an unpersisted entity — repo.save() assigns pk and timestamps.
        # Do NOT set pk, created_at, updated_at here.
        return Post(title=dto.title, body=dto.body, is_published=False)

    def to_read_dto(self, entity: Post) -> PostReadDTO:
        # entity is always persisted here (pk is never None)
        return PostReadDTO(
            id=str(entity.pk),
            title=entity.title,
            body=entity.body,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
        )

    def apply_update(self, entity: Post, dto: UpdatePostDTO) -> Post:
        # Use dataclasses.replace — copies _raw_orm so repo treats it as UPDATE
        # None in dto means "no change" — preserve the existing value
        return replace(
            entity,
            title=dto.title if dto.title is not None else entity.title,
            body=dto.body   if dto.body  is not None else entity.body,
        )
```

| Method | Called for | Must return |
|---|---|---|
| `to_domain(dto)` | `create()` | New unpersisted entity (`pk=None`) |
| `to_read_dto(entity)` | `get()`, `list()`, `create()`, `update()` | Populated `ReadDTO` |
| `apply_update(entity, dto)` | `update()` | New entity (via `dataclasses.replace`) — never mutate in place |

---

## Service Layer

The service layer is the **only** layer that enforces authorization, orchestrates transactions, and raises typed `ServiceException` subclasses.

```
HTTP adapter
    ↓  AuthContext (from JWT)
AsyncService
    ↓  authorization (AbstractAuthorizer)
    ↓  UoW + repository
    ↓  DTO assembly (AbstractDTOAssembler)
HTTP adapter  ←  ReadDTO
```

### IUoWProvider

Minimal interface for anything that can produce a fresh `AsyncUnitOfWork`. `RepositoryProvider` already satisfies it — just bind it in the DI container:

```python
from fastrest_core.service import IUoWProvider

@Provider(singleton=True)
def uow_provider(repo_provider: SQLAlchemyRepositoryProvider) -> IUoWProvider:
    # RepositoryProvider.make_uow() already satisfies the interface
    return repo_provider
```

### AsyncService

Subclass `AsyncService[D, PK, C, R, U]` and implement a single method:

```python
from fastrest_core.service import AsyncService, IUoWProvider
from fastrest_core.assembler import AbstractDTOAssembler
from fastrest_core.auth import AbstractAuthorizer
from providify import Inject, Singleton
from uuid import UUID

@Singleton
class PostService(AsyncService[Post, UUID, CreatePostDTO, PostReadDTO, UpdatePostDTO]):

    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer:   Inject[AbstractAuthorizer],
        assembler:    Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]],
    ) -> None:
        super().__init__(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

    def _get_repo(self, uow):
        # Return the repository for Post from the open UoW
        return uow.posts  # type: ignore[attr-defined]
```

All five CRUD methods are provided by the base class:

```python
# All methods take an AuthContext — built from the decoded JWT in the HTTP adapter
ctx = AuthContext(user_id="usr_123", grants=(...))

post_dto  = await svc.create(CreatePostDTO(title="Hello"), ctx)   # → PostReadDTO
post_dto  = await svc.get("post-1", ctx)                          # → PostReadDTO
posts     = await svc.list(QueryParams(), ctx)                     # → list[PostReadDTO]
post_dto  = await svc.update("post-1", UpdatePostDTO(title="Hi"), ctx)  # → PostReadDTO
await svc.delete("post-1", ctx)                                   # → None
```

### Authorization order

The order in which auth is checked vs. the DB is opened is intentional and security-relevant:

| Operation | Order |
|---|---|
| `create`, `list` | **Auth first**, then open DB — denied callers never touch the database |
| `get`, `update`, `delete` | **Fetch first**, then auth — the authorizer can inspect entity fields (e.g. ownership); a missing entity always raises `ServiceNotFoundError` regardless of auth |

> The fetch-first pattern for `get`/`update`/`delete` prevents an **existence oracle**: if auth ran first, a 403 would reveal that the entity exists even when the caller has no permission to see it.

### DI wiring

With providify, wire the assembler and service with `@Singleton` on the classes:

```python
from providify import Singleton, DIContainer

@Singleton
class PostAssembler(AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]):
    ...  # implement the three methods

@Singleton
class PostService(AsyncService[Post, UUID, CreatePostDTO, PostReadDTO, UpdatePostDTO]):
    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer:   Inject[AbstractAuthorizer],
        assembler:    Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]],
    ) -> None:
        super().__init__(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

    def _get_repo(self, uow): return uow.posts

container = DIContainer()
container.register(PostAssembler)   # binds under AbstractDTOAssembler[Post, ...]
container.register(PostService)     # auto-wired via Inject[...] annotations
```

---

## Authorization

Authorization primitives live in `fastrest_core.auth` and `fastrest_core.base_authorizer`.

### Action

`Action` is a `StrEnum` — every value is also a plain `str` at runtime, so JWT round-trips are transparent:

```python
from fastrest_core.auth import Action

Action.CREATE  # "create"
Action.READ    # "read"
Action.UPDATE  # "update"
Action.DELETE  # "delete"
Action.LIST    # "list"

Action.READ == "read"  # True — StrEnum compares equal to its string value
```

### ResourceGrant

Immutable value object pairing a resource key with an allowed set of actions:

```python
from fastrest_core.auth import ResourceGrant, Action

# Type-level grant — applies to every instance of the resource
ResourceGrant("posts", frozenset({Action.LIST, Action.CREATE, Action.READ}))

# Instance-level grant — applies to one specific entity only
ResourceGrant("posts:abc123", frozenset({Action.UPDATE, Action.DELETE}))

# Wildcard — grants everything on every resource (admin)
ResourceGrant("*", frozenset(Action))
```

Resource key conventions:
- **Type-level**: lowercase table name (e.g. `"posts"`, `"users"`) — matches `Meta.table`
- **Instance-level**: `"{table}:{pk}"` (e.g. `"posts:abc123"`)
- **Wildcard**: `"*"`

### AuthContext

Immutable identity + permission snapshot, built once per request from the decoded JWT:

```python
from fastrest_core.auth import AuthContext, ResourceGrant, Action

ctx = AuthContext(
    user_id="usr_123",
    roles=frozenset({"editor"}),
    grants=(
        ResourceGrant("posts",        frozenset({Action.LIST, Action.READ})),
        ResourceGrant("posts:abc123", frozenset({Action.UPDATE, Action.DELETE})),
    ),
)

ctx.is_anonymous()              # False
ctx.has_role("editor")          # True
ctx.can(Action.READ,   "posts") # True  — type-level grant
ctx.can(Action.UPDATE, "posts") # False — no type-level UPDATE grant
ctx.can(Action.UPDATE, "posts:abc123")  # True  — instance-level grant
ctx.grants_for("posts")         # frozenset({Action.LIST, Action.READ})
```

An anonymous (unauthenticated) caller:

```python
ctx = AuthContext()          # user_id=None, no grants
ctx.is_anonymous()           # True
ctx.can(Action.READ, "posts") # False
```

### AbstractAuthorizer

Implement one class that handles all entity types. The service injects it as `Inject[AbstractAuthorizer]`:

```python
from fastrest_core.auth import AbstractAuthorizer, Action, AuthContext, Resource
from fastrest_core.exception.service import ServiceAuthorizationError

class AppAuthorizer(AbstractAuthorizer):

    async def authorize(self, ctx: AuthContext, action: Action, resource: Resource) -> None:
        # Derive the resource key using Meta.table (or lowercase class name)
        meta = getattr(resource.entity_type, "Meta", None)
        table = getattr(meta, "table", resource.entity_type.__name__.lower())

        # Instance-level check (more specific)
        if not resource.is_collection:
            if ctx.can(action, f"{table}:{resource.entity.pk}"):
                return

        # Type-level check
        if ctx.can(action, table):
            return

        raise ServiceAuthorizationError(str(action), resource.entity_type)
```

For entity-specific rules, dispatch on `resource.entity_type`:

```python
class AppAuthorizer(AbstractAuthorizer):
    async def authorize(self, ctx, action, resource):
        if resource.entity_type is Post:
            await _check_post(ctx, action, resource)
        elif resource.entity_type is User:
            await _check_user(ctx, action, resource)
        else:
            raise ServiceAuthorizationError(str(action), resource.entity_type)
```

### BaseAuthorizer

`BaseAuthorizer` is a permissive fallback — it allows every operation unconditionally. It is registered at priority `-(2**31)` so any application-specific authorizer at the default priority (0) automatically shadows it:

```python
from fastrest_core.base_authorizer import BaseAuthorizer
from providify import Singleton

# No setup needed — BaseAuthorizer is registered automatically via scan:
container.scan("fastrest_core.base_authorizer")

# Your own authorizer at default priority 0 shadows it:
@Singleton
class AppAuthorizer(AbstractAuthorizer):
    async def authorize(self, ctx, action, resource): ...

container.register(AppAuthorizer)  # takes priority over BaseAuthorizer
```

Use `BaseAuthorizer` for:
- Prototypes and internal tools with no access-control requirements
- Integration tests focused on business logic, not permissions
- Early development before authorization rules are defined

> For production, add a startup guard to prevent accidentally shipping a permissive app:
> ```python
> from fastrest_core.base_authorizer import BaseAuthorizer
> assert not isinstance(container.get(AbstractAuthorizer), BaseAuthorizer), \
>     "No real authorizer registered — refusing to start in production"
> ```

---

## Query System

### QueryBuilder

Fluent, immutable builder — every method returns a new instance:

```python
from fastrest_core import QueryBuilder, QueryParams, SortField, SortOrder

# Simple equality filter
params = QueryParams(node=QueryBuilder().eq("active", True).build(), limit=10)

# Compound filter with sorting
node = (
    QueryBuilder()
    .eq("active", True)
    .gte("age", 18)
    .like("name", "Alice%")
    .build()
)

# OR logic
adult_or_admin = (
    QueryBuilder()
    .gte("age", 18)
    .or_(QueryBuilder().eq("role", "admin"))
    .build()
)

# NOT
not_banned = QueryBuilder().eq("banned", True).not_().build()

# IN
status_filter = QueryBuilder().in_("status", ["active", "trial"]).build()

# NULL checks
unverified = QueryBuilder().is_null("verified_at").build()
```

#### All builder methods

| Method | SQL equivalent |
|---|---|
| `.eq(field, value)` | `field = value` |
| `.ne(field, value)` | `field != value` |
| `.gt(field, value)` | `field > value` |
| `.gte(field, value)` | `field >= value` |
| `.lt(field, value)` | `field < value` |
| `.lte(field, value)` | `field <= value` |
| `.like(field, pattern)` | `field LIKE pattern` |
| `.in_(field, values)` | `field IN (values)` |
| `.is_null(field)` | `field IS NULL` |
| `.is_not_null(field)` | `field IS NOT NULL` |
| `.and_(other)` | `... AND ...` |
| `.or_(other)` | `... OR ...` |
| `.not_()` | `NOT (...)` |

### QueryParams

Immutable value object bundling filter, sort, and pagination:

```python
from fastrest_core import QueryParams, SortField, SortOrder

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

Parse query strings into AST nodes (useful for URL query parameters):

```python
from fastrest_core import QueryParser

parser = QueryParser()

# Parse a filter expression from a string
node = parser.parse('status = "active" AND age >= 18')
params = QueryParams(node=node, limit=50)
```

Grammar supports: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `IN`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`.

### Type Coercion

The coercion system normalises string values from query parameters into the correct Python types before they reach the database.

#### Individual coercers

```python
from fastrest_core.query.visitor.type_coercion import (
    coerce_int, coerce_float, coerce_boolean, coerce_datetime, coerce_list,
)

coerce_int("42")           # → 42
coerce_boolean("yes")      # → True
coerce_datetime("2024-01-15T10:30:00Z")  # → datetime(...)

# coerce_list handles many formats
coerce_list('["a","b"]')   # JSON array  → ['a', 'b']
coerce_list('[a,b]')       # bracketed   → ['a', 'b']
coerce_list('a,b,c')       # CSV         → ['a', 'b', 'c']
coerce_list('single')      # single val  → ['single']
coerce_list(['x', 'y'])    # passthrough → ['x', 'y']
```

#### Registry & visitor

```python
from fastrest_core.query.visitor.type_coercion import (
    TypeCoercionRegistry, ASTTypeCoercion, register_default_coercer, coerce_datetime,
)
from datetime import datetime

# Register a global default for a Python type
register_default_coercer(datetime, coerce_datetime)

# Build a registry manually
registry = TypeCoercionRegistry()
registry.register("age", int, coerce_int)
registry.register("created_at", datetime, coerce_datetime)

# Walk an AST and coerce all comparison values
coercer = ASTTypeCoercion(registry)
coerced_ast = coercer.visit(parsed_ast)
```

#### Column-level coercers (SQLAlchemy)

Colocate coercion logic with the model via `Column.info`:

```python
from sqlalchemy import Column, String, DateTime
from fastrest_core.query.visitor.type_coercion import coerce_datetime

class MyModel(Base):
    __tablename__ = "my_table"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, info={"coercer": coerce_datetime})
    tags = Column(String, info={"coercer": lambda s: s.split(",")})
```

Build a registry from a model (respects `Column.info['coercer']`):

```python
from fastrest_sa import registry_from_sa_model

registry = registry_from_sa_model(MyModel, field_coercions={
    "special_field": lambda x: custom_coerce(x),
})
coercer = ASTTypeCoercion(registry)
```

**Resolution order:** `Column.info['coercer']` → `field_coercions[name]` → global type default.

---

## SQLAlchemy Backend

### Installation

```
pip install fastrest-sa
```

### Provider setup

The recommended entry point. It auto-generates ORM classes from your domain models at runtime:

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from fastrest_sa import SQLAlchemyRepositoryProvider

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
session_factory = async_sessionmaker(engine, expire_on_commit=False)

provider = SQLAlchemyRepositoryProvider(engine=engine, session_factory=session_factory)
provider.register(User, Post, Subscription)

# Create all tables
await provider.create_all()

# Use the UoW
async with provider.make_uow() as uow:
    # Save a new entity
    user = await uow.users.save(User(name="Alice", email="alice@example.com"))

    # Query with filters, sorting, pagination
    from fastrest_core import QueryBuilder, QueryParams, SortField, SortOrder

    params = QueryParams(
        node=QueryBuilder().eq("active", True).gte("age", 18).build(),
        sort=[SortField("created_at", SortOrder.DESC)],
        limit=20,
    )
    active_users = await uow.users.find_by_query(params)
    total = await uow.users.count(params)

    # Update
    user.name = "Alice Smith"
    user = await uow.users.save(user)

    # Delete
    await uow.users.delete(user)
```

### Optimistic locking

Use `VersionedDomainModel` to get automatic optimistic concurrency control:

```python
class Document(VersionedDomainModel):
    id: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    content: str

# If two callers load the same document and both try to save, the second raises:
from fastrest_core import StaleEntityError

try:
    await uow.documents.save(stale_document)
except StaleEntityError:
    # Reload and retry
    ...
```

### Schema Guard

Detect drift between the generated ORM metadata and the actual database schema:

```python
from fastrest_sa import SchemaGuard

guard = SchemaGuard(engine, provider.metadata)
report = await guard.check()

if report.has_drift:
    for drift in report.drifts:
        print(drift)
```

---

## Beanie Backend

### Installation

```
pip install fastrest-beanie
```

### Beanie provider setup

```python
from motor.motor_asyncio import AsyncIOMotorClient
from fastrest_beanie import BeanieRepositoryProvider
from fastrest_core import QueryBuilder, QueryParams, SortField, SortOrder

client = AsyncIOMotorClient("mongodb://localhost:27017")

provider = BeanieRepositoryProvider(motor_client=client, db_name="mydb")
provider.register(User, Post)
await provider.init()

async with provider.make_uow() as uow:
    # Save
    user = await uow.users.save(User(name="Bob", email="bob@example.com"))

    # Query
    params = QueryParams(
        node=QueryBuilder().eq("published", True).build(),
        sort=[SortField("created_at", SortOrder.DESC)],
        limit=10,
    )
    recent_posts = await uow.posts.find_by_query(params)
```

### DI integration (Providify)

```python
from fastrest_beanie import BeanieModule, BeanieSettings, bind_repositories
from providify import DIContainer, Provider

container = DIContainer()

@Provider(singleton=True)
def settings() -> BeanieSettings:
    return BeanieSettings(
        motor_client=client,
        db_name="mydb",
        entity_classes=(User, Post),
    )

container.provide(settings)
container.install(BeanieModule)
bind_repositories(container, User, Post)

# Resolve the repository anywhere in the app
user_repo = await container.aget(AsyncRepository[User])
```

---

## Exception Hierarchy

### Service exceptions (`fastrest_core.exception.service`)

All service exceptions inherit from `ServiceException` — HTTP adapters catch the whole family with one clause and dispatch on subtype:

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
except ServiceNotFoundError as e:
    # e.entity_id (str), e.entity_cls — map to HTTP 404
    ...
except ServiceAuthorizationError as e:
    # e.operation, e.entity_cls, e.reason (internal, never surface to client) — HTTP 403
    ...
except ServiceConflictError as e:
    # e.detail — HTTP 409
    ...
except ServiceValidationError as e:
    # e.detail, e.field (optional) — HTTP 422
    ...
```

| Exception | HTTP | When raised |
|---|---|---|
| `ServiceException` | — | Base class for all service exceptions |
| `ServiceNotFoundError` | 404 | Entity with the requested pk does not exist |
| `ServiceAuthorizationError` | 403 | Caller lacks permission for the operation |
| `ServiceConflictError` | 409 | Business-rule or uniqueness constraint violated |
| `ServiceValidationError` | 422 | DTO passes Pydantic but fails a domain invariant |

> `ServiceNotFoundError` is raised **before** the authorization check on `get`/`update`/`delete` — a missing entity always returns 404 regardless of the caller's identity, preventing an existence oracle.

> `ServiceAuthorizationError.reason` is stored on the exception but intentionally excluded from `str(exc)` — log it server-side at DEBUG level, never surface it to API clients.

### Query exceptions (`fastrest_core.exception.query`)

| Exception | When raised |
|---|---|
| `QueryException` | Base class |
| `OperationNotFound` | Unknown operator in a query string |
| `OperationNotSupported` | Dotted path fields or unsupported op |
| `WrongNodeVisited` | Visitor type mismatch |
| `CoercionError` | Type coercion failure |

### Repository exceptions (`fastrest_core.exception.repository`)

| Exception | When raised |
|---|---|
| `RepositoryException` | Base class |
| `RepositoryClassCreationFailed` | ORM class generation error |
| `FieldNotFound` | Column not found during query compilation |
| `EntityNotFound` | `find_by_id` returns nothing |
| `StaleEntityError` | Optimistic lock violation on `VersionedDomainModel` |

---

## Running tests

```bash
# All packages
python -m pytest

# One package
python -m pytest fastrest_core/
python -m pytest fastrest_sa/
python -m pytest fastrest_beanie/
```
