# fastrest-beanie

Beanie (Motor / MongoDB) async backend for **fastrest**.

Generates Beanie `Document` classes at runtime from your `DomainModel` subclasses — no hand-written Document models needed.

## What lives here

| Module | Purpose |
|---|---|
| `factory.py` | `BeanieModelFactory` — generates `Document` subclasses; `BeanieDocRegistry` — escape hatch |
| `repository.py` | `AsyncBeanieRepository` — Motor-backed CRUD |
| `uow.py` | `BeanieUnitOfWork` — Motor session lifecycle (optional transactions) |
| `provider.py` | `BeanieRepositoryProvider` — wires factory + repos + UoW + `init_beanie()` |

## Install

```bash
pip install fastrest-beanie
```

## Requirements

- MongoDB ≥ 4.0
- For multi-document transactions: replica set or sharded cluster

## Quick start

```python
from motor.motor_asyncio import AsyncIOMotorClient
from fastrest_beanie.provider import BeanieRepositoryProvider

client   = AsyncIOMotorClient("mongodb://localhost:27017")
provider = BeanieRepositoryProvider(motor_client=client, db_name="myapp")
provider.autodiscover("myapp.models")

# Must be called once at startup — initialises Motor indexes
await provider.init()

async with provider.make_uow() as uow:
    user = await uow.users.save(User(name="Edo", email="edo@example.com"))
    print(user.pk)  # UUID or ObjectId depending on PKStrategy
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

- `CheckConstraint` entries are silently ignored with a warning — MongoDB has no SQL CHECK constraints. Use Pydantic validators instead.
- Foreign key hints are stored in metadata for documentation purposes only — MongoDB has no native FK enforcement.
- Composite PKs are emulated via compound unique indexes; `find_by_id(pk_tuple)` issues a `find_one` query on the composite fields.
