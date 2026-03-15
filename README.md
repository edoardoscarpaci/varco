# fastrest

A modular Python framework for building expressive, backend-agnostic query APIs on top of SQLAlchemy and MongoDB (Beanie/Motor). It provides a clean domain model layer, a fluent query builder with AST-based filtering, automatic ORM class generation, and a pluggable type coercion system.

---

## Packages

| Package | Description |
|---|---|
| `fastrest_core` | Backend-agnostic domain model, query AST, builder, parser, DTOs |
| `fastrest_sa` | SQLAlchemy async backend (ORM generation, repository, schema guard) |
| `fastrest_beanie` | Beanie (Motor/MongoDB) async backend |

---

## Table of Contents

- [Domain Model](#domain-model)
- [Metadata & Constraints](#metadata--constraints)
- [Repository & Unit of Work](#repository--unit-of-work)
- [DTOs](#dtos)
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
