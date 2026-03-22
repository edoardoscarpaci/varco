# varco-core

[![PyPI version](https://img.shields.io/pypi/v/varco-core)](https://pypi.org/project/varco-core/)
[![Python](https://img.shields.io/pypi/pyversions/varco-core)](https://pypi.org/project/varco-core/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/edoardoscarpaci/varco/blob/main/LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-edoardoscarpaci%2Fvarco-blue?logo=github)](https://github.com/edoardoscarpaci/varco)

Backend-agnostic domain model and service layer for **varco**.

Provides the pure-Python building blocks that all backend packages depend on — no ORM imports at the core layer. Pair it with [`varco-sa`](https://pypi.org/project/varco-sa/) (SQLAlchemy) or [`varco-beanie`](https://pypi.org/project/varco-beanie/) (MongoDB) for a concrete backend.

---

## Install

```bash
pip install varco-core
```

---

## Features

- **Domain model** — `DomainModel`, `AuditedDomainModel`, `VersionedDomainModel`, soft-delete and multi-tenant variants
- **Generic async service** — `AsyncService` with built-in create / read / update / delete / list, pagination, soft-delete, and multi-tenancy mixins
- **Authorization** — `AbstractAuthorizer`, `GrantBasedAuthorizer`, `RoleBasedAuthorizer`, `OwnershipAuthorizer`
- **DTO layer** — `CreateDTO`, `ReadDTO`, `UpdateDTO`, `generate_dtos()` factory, cursor-based pagination
- **Fluent query builder** — `QueryBuilder` → AST → `QueryParser` (string → AST via Lark grammar); backend-independent
- **JWT / JWK** — `JwtBuilder`, `JwtParser`, `JwkBuilder`, `MultiKeyAuthority`, OIDC + PEM sources
- **Tracing** — `correlation_context`, `current_correlation_id`, `CorrelationIdFilter`
- **DI-ready** — all service classes are designed for constructor injection via [`providify`](https://pypi.org/project/providify/)

---

## What's in the package

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
| `exception/` | `FastrestErrorCodes`, `ErrorCode`, `ErrorMessage`, HTTP error mapping |
| `tracing.py` | `correlation_context`, `current_correlation_id`, `CorrelationIdFilter` |
| `query/` | `QueryBuilder`, `QueryParams`, `QueryParser`, AST visitors |
| `jwt/` | `JwtBuilder`, `JwtParser`, `JwtUtil`, `JsonWebToken` |
| `jwk/` | `JwkBuilder`, `JsonWebKey`, `JsonWebKeySet` |
| `authority/` | `JwtAuthority`, `MultiKeyAuthority`, `TrustedIssuerRegistry`, OIDC/PEM sources |

---

## Quick start

### Define a domain model

```python
from __future__ import annotations
from typing import Annotated
from varco_core import AuditedDomainModel
from varco_core.meta import FieldHint, PrimaryKey, PKStrategy, pk_field

class Post(AuditedDomainModel):
    pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
    title: Annotated[str, FieldHint(max_length=200)]
    body: str
    published: bool = False
```

### Wire a service with DI

```python
from varco_core import AsyncService, IUoWProvider
from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth import AbstractAuthorizer
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

### Build and run a query

```python
from varco_core import QueryBuilder, QueryParams

params = QueryParams(
    node=QueryBuilder().eq("published", True).and_().gt("pk", 10).build(),
    limit=20,
    offset=0,
)

# Parse from a query string (e.g. from a URL parameter)
from varco_core import QueryParser
params = QueryParser().parse('published == true AND pk > 10', limit=20)
```

### JWT — build and verify tokens

```python
from varco_core import JwtBuilder, JwtParser

token = JwtBuilder(secret="s3cr3t").subject("user-42").expires_in(3600).build()
payload = JwtParser(secret="s3cr3t").parse(token)
```

---

## Related packages

| Package | Description |
|---|---|
| [`varco-sa`](https://pypi.org/project/varco-sa/) | SQLAlchemy async backend |
| [`varco-beanie`](https://pypi.org/project/varco-beanie/) | Beanie / Motor MongoDB backend |

---

## Links

- **Repository**: https://github.com/edoardoscarpaci/varco
- **Full docs**: https://github.com/edoardoscarpaci/varco#readme
- **Issue tracker**: https://github.com/edoardoscarpaci/varco/issues
