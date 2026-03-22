# varco-sa

SQLAlchemy async backend for **varco**.

Generates SQLAlchemy ORM classes at runtime from your `DomainModel` subclasses — no hand-written ORM models needed.

## What lives here

| Module | Purpose |
|---|---|
| `factory.py` | `SAModelFactory` — generates `DeclarativeBase` subclasses at runtime; `SAModelRegistry` — escape hatch |
| `repository.py` | `AsyncSQLAlchemyRepository` — `AsyncSession`-backed CRUD + `exists()` + `stream_by_query()` |
| `uow.py` | `SQLAlchemyUnitOfWork` — session lifecycle + atomic commits |
| `provider.py` | `SQLAlchemyRepositoryProvider` — wires factory + repos + UoW |
| `bootstrap.py` | `SAConfig`, `SAFastrestApp` — one-liner app setup |
| `alembic_helpers.py` | `get_target_metadata`, `print_create_ddl` — Alembic integration |
| `schema_guard.py` | `SchemaGuard` — drift detection between ORM metadata and live DB |

## Install

```bash
pip install varco-sa
# With PostgreSQL driver:
pip install "varco-sa[postgresql]"
# With SQLite (for tests / dev):
pip install "varco-sa[sqlite]"
```

## Quick start (bootstrap)

```python
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_sa import SAConfig, SAFastrestApp

class Base(DeclarativeBase): pass

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")

app = SAFastrestApp(SAConfig(
    engine=engine,
    base=Base,
    entity_classes=(User, Post),
))

await app.create_all()              # CREATE TABLE IF NOT EXISTS ...
uow_provider = app.uow_provider     # Inject[IUoWProvider]
```

## Manual setup

```python
from sqlalchemy.ext.asyncio import async_sessionmaker
from varco_sa import SQLAlchemyRepositoryProvider

sessions = async_sessionmaker(engine, expire_on_commit=False)
provider = SQLAlchemyRepositoryProvider(engine=engine, session_factory=sessions)
provider.register(User, Post)
await provider.create_all()

async with provider.make_uow() as uow:
    user = await uow.users.save(User(name="Edo", email="edo@example.com"))
    print(user.pk)
```

## Alembic integration

```python
# alembic/env.py
from varco_sa import get_target_metadata
from myapp.models import User, Post

target_metadata = get_target_metadata(User, Post)
```

Preview DDL before running a migration:

```python
from varco_sa import print_create_ddl
print(print_create_ddl(User, Post, dialect="postgresql"))
```

## `exists()` and `stream_by_query()`

```python
async with provider.make_uow() as uow:
    # Lightweight — uses SA identity-map cache, no full ORM load when cached
    if await uow.posts.exists(post_id):
        ...

    # Stream with a server-side cursor — constant memory
    params = QueryParams(node=QueryBuilder().eq("active", True).build())
    async for post in uow.posts.stream_by_query(params):
        await process(post)
```

## Escape hatch — SA-specific features

```python
from varco_sa import SAModelRegistry
from varco_core import cast_raw

UserORM = SAModelRegistry.get(User)

from sqlalchemy.orm import relationship
UserORM.posts = relationship("PostORM", back_populates="author")

async with provider.make_uow() as uow:
    user = await uow.users.find_by_id(some_uuid)
    orm_user = cast_raw(user, UserORM)
    await uow.session.refresh(orm_user, ["posts"])
```

See the [root README](../README.md) for full documentation.
