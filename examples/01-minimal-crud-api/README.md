# 01 — Minimal CRUD API

The smallest complete Varco CRUD service: a `Product` catalog with five endpoints,
an in-memory repository (no database, no Docker), and full DI wiring via providify.
Use this as the entry point when learning the framework — every layer is present but
nothing is hidden behind a higher-level abstraction.

## What this example shows

| Feature | Details |
|---|---|
| `AsyncService` subclass | `ProductService` implements only `_get_repo()` — the minimum required method |
| `VarcoCRUDRouter` + CRUD mixins | `ProductRouter` composes `CreateMixin`, `ReadMixin`, `UpdateMixin`, `DeleteMixin`, `ListMixin` — one endpoint each |
| `Page` response envelope | `GET /v1/products` returns a paginated `Page[ProductRead]` with `total`, `items`, `offset`, and `limit` |
| `AuditedDomainModel` | `Product` inherits `created_at` / `updated_at` — no manual timestamp management |
| Pydantic DTOs | `ProductCreate` / `ProductRead` / `ProductUpdate` match the `AsyncService[D, PK, C, R, U]` type parameters |
| `@Singleton` DI wiring | `ProductService` and `ProductRouter` are registered via `@Singleton` + `container.scan()` |
| In-memory repository | `InMemoryProductRepository` + `InMemoryUoWProvider` — all state lives in a Python dict |

> **Note**: the in-memory repository is for demo purposes only — it has no transaction
> isolation and data is lost when the process restarts.  For a production PostgreSQL
> backend see **example 09 — SQLAlchemy CRUD API**.

## Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/products` | Create a product → 201 with `ProductRead` |
| `GET` | `/v1/products/{id}` | Fetch a product by UUID → 200 or 404 |
| `PUT` | `/v1/products/{id}` | Full update → 200 or 404 |
| `DELETE` | `/v1/products/{id}` | Delete a product → 204 or 404 |
| `GET` | `/v1/products` | List all products (paginated) → 200 `Page[ProductRead]` |
| `GET` | `/health` | Liveness probe → `{"status": "ok"}` |

## Run locally

No Docker required — the in-memory repository keeps everything in process.

```bash
cd examples/01-minimal-crud-api
VARCO_SECURITY_ENV=development uv run uvicorn app:app --reload
```

Then open http://localhost:8000/docs for the interactive Swagger UI.

```bash
# Create a product
curl -X POST http://localhost:8000/v1/products \
     -H 'Content-Type: application/json' \
     -d '{"name": "Widget", "price": 9.99}'

# List products
curl http://localhost:8000/v1/products

# Fetch by UUID (replace <id> with the pk from the create response)
curl http://localhost:8000/v1/products/<id>

# Update
curl -X PUT http://localhost:8000/v1/products/<id> \
     -H 'Content-Type: application/json' \
     -d '{"price": 7.49}'

# Delete
curl -X DELETE http://localhost:8000/v1/products/<id>
```

## Run tests

All tests use the in-memory state — no external services needed.

```bash
VARCO_SECURITY_ENV=development uv run pytest examples/01-minimal-crud-api/tests/
```

## How it works

- **Domain model** — `Product` extends `AuditedDomainModel`. The `pk` field is `init=False`
  with `PKStrategy.UUID_AUTO`; the repository assigns a UUID on first `save()`.

- **Repository** — `InMemoryProductRepository` stores products in `{UUID: Product}`.
  `InMemoryUoWProvider` holds a single shared dict so data persists across requests
  within the process.

- **Service** — `ProductService` extends `AsyncService` and implements `_get_repo()` to
  return `uow.products`. No caching, no events — deliberately minimal.

- **Router** — `ProductRouter` inherits all five CRUD mixins from `VarcoCRUDRouter`.
  Adding or removing a mixin is a one-line change; the service layer is unaffected.

- **DI wiring** — `app.py` scans `assembler` and `service` modules so providify
  registers the concrete classes under their generic base types. `ProductService` is
  resolved via `container.get(AsyncService[Product, UUID, ...])` and passed directly
  to `ProductRouter` to work around a `get_type_hints()` limitation with
  `from __future__ import annotations` in `VarcoCRUDRouter.__init__`.
