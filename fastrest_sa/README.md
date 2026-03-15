# fastrest-sa

SQLAlchemy async backend for **fastrest**.

Generates SQLAlchemy ORM classes at runtime from your `DomainModel` subclasses — no hand-written ORM models needed.

## What lives here

| Module | Purpose |
|---|---|
| `factory.py` | `SAModelFactory` — generates `DeclarativeBase` subclasses at runtime; `SAModelRegistry` — escape hatch to retrieve generated classes |
| `repository.py` | `AsyncSQLAlchemyRepository` — `AsyncSession`-backed CRUD |
| `uow.py` | `SQLAlchemyUnitOfWork` — session lifecycle + atomic commits |
| `provider.py` | `SQLAlchemyRepositoryProvider` — wires factory + repos + UoW |

## Install

```bash
pip install fastrest-sa
# With PostgreSQL driver:
pip install "fastrest-sa[postgresql]"
# With SQLite (for tests / dev):
pip install "fastrest-sa[sqlite]"
```

## Quick start

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from fastrest_sa.provider import SQLAlchemyRepositoryProvider

class Base(DeclarativeBase): pass

engine   = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
sessions = async_sessionmaker(engine, expire_on_commit=False)

provider = SQLAlchemyRepositoryProvider(base=Base, session_factory=sessions)
provider.autodiscover("myapp.models")   # picks up all @register classes

# Create tables
async with engine.begin() as conn:
    await conn.run_sync(Base.metadata.create_all)

# Use in a service
async with provider.make_uow() as uow:
    user = await uow.users.save(User(name="Edo", email="edo@example.com"))
    print(user.pk)  # UUID, populated after save()
```

## Escape hatch — SA-specific features

```python
from fastrest_sa.factory import SAModelRegistry
from fastrest_core import cast_raw

UserORM = SAModelRegistry.get(User)

# Add a relationship post-build
from sqlalchemy.orm import relationship
UserORM.posts = relationship("PostORM", back_populates="author")

# Access via cast_raw inside a UoW
async with provider.make_uow() as uow:
    user = await uow.users.find_by_id(some_uuid)
    orm_user = cast_raw(user, UserORM)
    await uow.session.refresh(orm_user, ["posts"])
```
