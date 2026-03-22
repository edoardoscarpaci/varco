# varco-sa

[![PyPI version](https://img.shields.io/pypi/v/varco-sa)](https://pypi.org/project/varco-sa/)
[![Python](https://img.shields.io/pypi/pyversions/varco-sa)](https://pypi.org/project/varco-sa/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/edoardoscarpaci/varco/blob/main/LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-edoardoscarpaci%2Fvarco-blue?logo=github)](https://github.com/edoardoscarpaci/varco)

SQLAlchemy async backend for **varco**.

Generates SQLAlchemy ORM classes at runtime from your `DomainModel` subclasses — no hand-written ORM models needed. Requires [`varco-core`](https://pypi.org/project/varco-core/).

---

## Install

```bash
pip install varco-sa

# With PostgreSQL async driver:
pip install "varco-sa[postgresql]"

# With SQLite (tests / local dev):
pip install "varco-sa[sqlite]"
```

---

## Features

- **Zero-boilerplate ORM** — `SAModelFactory` generates `DeclarativeBase` subclasses at runtime; no duplication between domain and ORM layers
- **Full async repository** — `AsyncSQLAlchemyRepository` implements `AsyncRepository` (CRUD, `exists()`, `stream_by_query()` with server-side cursor)
- **Unit of Work** — `SQLAlchemyUnitOfWork` manages `AsyncSession` lifecycle and atomic commits
- **One-liner bootstrap** — `SAFastrestApp` + `SAConfig` wire engine, session factory, ORM generation, and table creation
- **Alembic integration** — `get_target_metadata()` and `print_create_ddl()` helpers for migration scripts
- **Schema Guard** — `SchemaGuard` detects drift between ORM metadata and the live database schema
- **Query integration** — accepts `varco-core` `QueryParams` / `QueryBuilder` AST natively; translates to SQLAlchemy `where()` clauses

---

## What's in the package

| Module | Purpose |
|---|---|
| `factory.py` | `SAModelFactory` — generates `DeclarativeBase` subclasses at runtime; `SAModelRegistry` — escape hatch |
| `repository.py` | `AsyncSQLAlchemyRepository` — `AsyncSession`-backed CRUD + `exists()` + `stream_by_query()` |
| `uow.py` | `SQLAlchemyUnitOfWork` — session lifecycle + atomic commits |
| `provider.py` | `SQLAlchemyRepositoryProvider` — wires factory + repos + UoW |
| `bootstrap.py` | `SAConfig`, `SAFastrestApp` — one-liner app setup |
| `alembic_helpers.py` | `get_target_metadata`, `print_create_ddl` — Alembic integration |
| `schema_guard.py` | `SchemaGuard` — drift detection between ORM metadata and live DB |

---

## Quick start

### Bootstrap (one-liner)

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
uow_provider = app.uow_provider     # ready to inject as IUoWProvider
```

### Manual setup

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

### Query integration

```python
from varco_core import QueryBuilder, QueryParams

async with provider.make_uow() as uow:
    # exists() — uses SA identity-map cache, no full ORM load when cached
    if await uow.posts.exists(post_id):
        ...

    # stream_by_query() — server-side cursor, constant memory regardless of result size
    params = QueryParams(node=QueryBuilder().eq("active", True).build())
    async for post in uow.posts.stream_by_query(params):
        await process(post)
```

### Alembic integration

```python
# alembic/env.py
from varco_sa import get_target_metadata
from myapp.models import User, Post

target_metadata = get_target_metadata(User, Post)
```

Preview the DDL before running a migration:

```python
from varco_sa import print_create_ddl

print(print_create_ddl(User, Post, dialect="postgresql"))
```

### Schema Guard — detect drift

```python
from varco_sa import SchemaGuard

guard = SchemaGuard(engine, User, Post)
differences = await guard.check()
if differences:
    print("Schema drift detected:", differences)
```

### Access the generated SA model (escape hatch)

```python
from varco_sa import SAModelRegistry
from sqlalchemy.orm import relationship

UserORM = SAModelRegistry.get(User)
UserORM.posts = relationship("PostORM", back_populates="author")
```

---

## Related packages

| Package | Description |
|---|---|
| [`varco-core`](https://pypi.org/project/varco-core/) | Domain model, service layer, query AST, JWT — required dependency |
| [`varco-beanie`](https://pypi.org/project/varco-beanie/) | Beanie / Motor MongoDB backend (alternative to this package) |

---

## Links

- **Repository**: https://github.com/edoardoscarpaci/varco
- **Full docs**: https://github.com/edoardoscarpaci/varco#sqlalchemy-backend
- **Issue tracker**: https://github.com/edoardoscarpaci/varco/issues
