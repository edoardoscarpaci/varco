# fastrest-beanie

Beanie (Motor / MongoDB) async backend for **fastrest**.

Generates Beanie `Document` classes at runtime from your `DomainModel` subclasses — no hand-written Document models needed.

## What lives here

| Module | Purpose |
|---|---|
| `factory.py` | `BeanieModelFactory` — generates `Document` subclasses; `BeanieDocRegistry` — escape hatch |
| `repository.py` | `AsyncBeanieRepository` — Motor-backed CRUD + `exists()` + `stream_by_query()` |
| `uow.py` | `BeanieUnitOfWork` — Motor session lifecycle (optional transactions) |
| `provider.py` | `BeanieRepositoryProvider` — wires factory + repos + UoW + `init_beanie()` |
| `bootstrap.py` | `BeanieConfig`, `BeanieFastrestApp` — one-liner app setup |

## Install

```bash
pip install fastrest-beanie
```

## Requirements

- MongoDB ≥ 4.0
- For multi-document transactions: replica set or sharded cluster

## Quick start (bootstrap)

```python
from motor.motor_asyncio import AsyncIOMotorClient
from fastrest_beanie import BeanieConfig, BeanieFastrestApp

client = AsyncIOMotorClient("mongodb://localhost:27017")

app = BeanieFastrestApp(BeanieConfig(
    motor_client=client,
    db_name="myapp",
    entity_classes=(User, Post),
))

await app.init()                    # calls beanie.init_beanie() internally
uow_provider = app.uow_provider     # Inject[IUoWProvider]
```

## Manual setup

```python
from fastrest_beanie import BeanieRepositoryProvider

provider = BeanieRepositoryProvider(motor_client=client, db_name="myapp")
provider.register(User, Post)
await provider.init()

async with provider.make_uow() as uow:
    user = await uow.users.save(User(name="Edo", email="edo@example.com"))
    print(user.pk)
```

## `exists()` and `stream_by_query()`

```python
async with provider.make_uow() as uow:
    # Uses .count() — no document load
    if await uow.posts.exists(post_id):
        ...

    # Motor batches internally — bounded memory regardless of collection size
    params = QueryParams(node=QueryBuilder().eq("published", True).build())
    async for post in uow.posts.stream_by_query(params):
        await process(post)
```

## Transactions

```python
# Requires a MongoDB replica set
provider = BeanieRepositoryProvider(
    motor_client=client,
    db_name="myapp",
    transactional=True,   # wraps each UoW in a Motor session transaction
)
```

## Notes

- `CheckConstraint` entries are silently ignored — MongoDB has no SQL CHECK constraints. Use Pydantic validators instead.
- Foreign key hints are metadata only — MongoDB has no FK enforcement.
- Composite PKs are emulated via compound unique indexes; `find_by_id(pk_tuple)` issues a `find_one` with a composite filter.

See the [root README](../README.md) for full documentation.
