# fastrest-core

Backend-agnostic domain model layer for **fastrest**.

Provides the pure-Python building blocks that all backend packages depend on — no ORM imports, no runtime dependencies.

## What lives here

| Module | Purpose |
|---|---|
| `model.py` | `DomainModel` base dataclass + `cast_raw()` escape hatch |
| `meta.py` | `FieldHint`, `ForeignKey`, `PrimaryKey`, `PKStrategy`, `UniqueConstraint`, `CheckConstraint`, `pk_field()` |
| `mapper.py` | `AbstractMapper` — bidirectional ORM ↔ domain translation |
| `repository.py` | `AsyncRepository` ABC |
| `uow.py` | `AsyncUnitOfWork` ABC |
| `registry.py` | `DomainModelRegistry` + `@register` decorator |
| `providers.py` | `RepositoryProvider` ABC + `autodiscover()` |

## Install

```bash
pip install fastrest-core
# or, for development:
pip install -e .
```

## Quick start

```python
from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from fastrest_core import DomainModel, register
from fastrest_core.meta import FieldHint, PrimaryKey, PKStrategy, pk_field

@register
@dataclass
class User(DomainModel):
    pk:    Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
    name:  Annotated[str,  FieldHint(max_length=120)]
    email: Annotated[str,  FieldHint(unique=True, nullable=False)]

    class Meta:
        table = "users"
```

Then install `fastrest-sa` or `fastrest-beanie` for a concrete backend.
