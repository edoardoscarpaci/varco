# fastrest-core

Backend-agnostic domain model and service layer for **fastrest**.

Provides the pure-Python building blocks that all backend packages depend on — no ORM imports at the core layer.

## What lives here

| Module | Purpose |
|---|---|
| `model.py` | `DomainModel`, `AuditedDomainModel`, `VersionedDomainModel`, `SoftDeleteMixin`, `TenantMixin` and derived classes |
| `meta.py` | `FieldHint`, `ForeignKey`, `PrimaryKey`, `PKStrategy`, constraints, `pk_field()` |
| `mapper.py` | `AbstractMapper` — bidirectional ORM ↔ domain translation |
| `repository.py` | `AsyncRepository` ABC — CRUD + `exists()` + `stream_by_query()` |
| `uow.py` | `AsyncUnitOfWork` ABC |
| `registry.py` | `DomainModelRegistry` + `@register` decorator |
| `providers.py` | `RepositoryProvider` ABC |
| `assembler.py` | `AbstractDTOAssembler[D, C, R, U]` |
| `service/base.py` | `AsyncService`, `IUoWProvider` |
| `service/tenant.py` | `TenantAwareService`, `TenantUoWProvider`, `tenant_context` |
| `service/soft_delete.py` | `SoftDeleteService` |
| `service/types.py` | `Assembler` alias, `ServiceProtocol` |
| `auth/` | `AbstractAuthorizer`, `Action`, `AuthContext`, `ResourceGrant` |
| `auth/helpers.py` | `GrantBasedAuthorizer`, `OwnershipAuthorizer`, `RoleBasedAuthorizer` |
| `exception/codes.py` | `FastrestErrorCodes` enum, `ErrorCode` |
| `exception/http.py` | `ErrorMessage`, `error_message_for`, `register_error_code` |
| `tracing.py` | `correlation_context`, `current_correlation_id`, `CorrelationIdFilter` |
| `query/` | `QueryBuilder`, `QueryParams`, `QueryParser`, AST visitors |

## Install

```bash
pip install fastrest-core
```

## Quick start

```python
from __future__ import annotations
from typing import Annotated
from fastrest_core import AuditedDomainModel
from fastrest_core.meta import FieldHint, PrimaryKey, PKStrategy, pk_field

class Post(AuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    title: Annotated[str, FieldHint(max_length=200)]
    body: str
    published: bool = False
```

Install `fastrest-sa` or `fastrest-beanie` for a concrete backend, then wire a service:

```python
from fastrest_core import AsyncService, IUoWProvider
from fastrest_core.assembler import AbstractDTOAssembler
from fastrest_core.auth import AbstractAuthorizer
from providify import Inject, Singleton

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
```

See the [root README](../README.md) for the full documentation.
