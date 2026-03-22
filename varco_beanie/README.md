# varco-beanie

[![PyPI version](https://img.shields.io/pypi/v/varco-beanie)](https://pypi.org/project/varco-beanie/)
[![Python](https://img.shields.io/pypi/pyversions/varco-beanie)](https://pypi.org/project/varco-beanie/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/edoardoscarpaci/varco/blob/main/LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-edoardoscarpaci%2Fvarco-blue?logo=github)](https://github.com/edoardoscarpaci/varco)

Beanie (Motor / MongoDB) async backend for **varco**.

Generates Beanie `Document` classes at runtime from your `DomainModel` subclasses — no hand-written Document models needed. Requires [`varco-core`](https://pypi.org/project/varco-core/).

---

## Install

```bash
pip install varco-beanie
```

### Requirements

- Python ≥ 3.12
- MongoDB ≥ 4.0
- For multi-document transactions: a MongoDB replica set or sharded cluster

---

## Features

- **Zero-boilerplate ODM** — `BeanieModelFactory` generates `Document` subclasses at runtime from your `DomainModel` classes; no duplication
- **Full repository** — `AsyncBeanieRepository` implements `AsyncRepository` (CRUD, `exists()`, `stream_by_query()`)
- **Unit of Work** — `BeanieUnitOfWork` wraps Motor session lifecycle with optional transactions
- **One-liner bootstrap** — `BeanieRepositoryProvider` + `BeanieFastrestApp` wire everything including `init_beanie()`
- **Query integration** — accepts `varco-core` `QueryParams` / `QueryBuilder` AST natively

---

## What's in the package

| Module | Purpose |
|---|---|
| `factory.py` | `BeanieModelFactory` — generates `Document` subclasses; `BeanieDocRegistry` — escape hatch to access the generated Document |
| `repository.py` | `AsyncBeanieRepository` — Motor-backed CRUD + `exists()` + `stream_by_query()` |
| `uow.py` | `BeanieUnitOfWork` — Motor session lifecycle (optional transactions) |
| `provider.py` | `BeanieRepositoryProvider` — wires factory + repos + UoW + `init_beanie()` |
| `bootstrap.py` | `BeanieConfig`, `BeanieFastrestApp` — one-liner app setup |

---

## Quick start

### Bootstrap (one-liner)

```python
from motor.motor_asyncio import AsyncIOMotorClient
from varco_beanie import BeanieConfig, BeanieFastrestApp

client = AsyncIOMotorClient("mongodb://localhost:27017")

app = BeanieFastrestApp(BeanieConfig(
    motor_client=client,
    db_name="myapp",
    entity_classes=(User, Post),
))

await app.init()                    # calls beanie.init_beanie() internally
uow_provider = app.uow_provider     # ready to inject as IUoWProvider
```

### Manual setup

```python
from varco_beanie import BeanieRepositoryProvider

provider = BeanieRepositoryProvider(motor_client=client, db_name="myapp")
provider.register(User, Post)
await provider.init()

async with provider.make_uow() as uow:
    user = await uow.users.save(User(name="Edo", email="edo@example.com"))
    print(user.pk)
```

### Transactions (replica set required)

```python
provider = BeanieRepositoryProvider(
    motor_client=client,
    db_name="myapp",
    transactional=True,   # wraps each UoW in a Motor session transaction
)
```

### Query integration

```python
from varco_core import QueryBuilder, QueryParams

async with provider.make_uow() as uow:
    # exists() — uses .count(), no document load
    if await uow.posts.exists(post_id):
        ...

    # stream_by_query() — Motor batches internally, bounded memory
    params = QueryParams(node=QueryBuilder().eq("published", True).build())
    async for post in uow.posts.stream_by_query(params):
        await process(post)
```

### Access the generated Beanie Document (escape hatch)

```python
from varco_beanie import BeanieDocRegistry

PostDoc = BeanieDocRegistry.get(Post)
# use PostDoc for Beanie-specific operations not exposed by the repository
```

---

## Notes

- `CheckConstraint` entries in `varco-core` metadata are silently ignored — MongoDB has no SQL CHECK constraints. Use Pydantic validators instead.
- Foreign key hints are metadata only — MongoDB has no FK enforcement.
- Composite PKs are emulated via compound unique indexes; `find_by_id(pk_tuple)` issues a `find_one` with a composite filter.

---

## Related packages

| Package | Description |
|---|---|
| [`varco-core`](https://pypi.org/project/varco-core/) | Domain model, service layer, query AST, JWT — required dependency |
| [`varco-sa`](https://pypi.org/project/varco-sa/) | SQLAlchemy async backend (alternative to this package) |

---

## Links

- **Repository**: https://github.com/edoardoscarpaci/varco
- **Full docs**: https://github.com/edoardoscarpaci/varco#beanie-backend
- **Issue tracker**: https://github.com/edoardoscarpaci/varco/issues
