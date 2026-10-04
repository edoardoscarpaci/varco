# 22 — Multi-Tenant Soft Delete

Demonstrates MRO-based mixin composition for a production-grade service: tenant
isolation via `TenantAwareService`, soft deletion via `SoftDeleteService`, and title
validation via `ValidatorServiceMixin` — all stacked on a single `AsyncService`
subclass backed by PostgreSQL through `varco_sa`.

## What this example shows

| Feature | Details |
|---|---|
| `TenantAwareService` | Row-level tenant isolation — `_scoped_params` injects `tenant_id = <tid>` on every query; `_check_entity` raises 404 for cross-tenant access |
| `SoftDeleteService` | `delete()` stamps `deleted_at` instead of issuing `DELETE`; `list()` automatically excludes soft-deleted rows via `deleted_at IS NULL` |
| `ValidatorServiceMixin` | Validates business invariants (`title` must not be blank) before write — runs via `_validate_entity` hook |
| MRO hook chain | Three mixins compose cooperatively via `super()` — no manual chaining needed in `NoteService` |
| `domain_replace()` vs `dataclasses.replace()` | `apply_update` in the assembler uses `domain_replace()` to preserve `init=False` fields (`pk`, `_raw_orm`, timestamps); plain `dataclasses.replace()` resets them to defaults on Python ≤ 3.12, causing silent re-INSERT |
| `is_not_null()` filter | `list_deleted()` uses `QueryBuilder().is_not_null("deleted_at")` to expose a tenant's soft-deleted notes |
| `varco_sa` backend | Async PostgreSQL via `asyncpg`; ORM model auto-generated from `Note(SoftDeleteAuditedDomainModel)` |
| Header-based tenant identity | `X-Tenant-Id` header extracted per request and placed in `AuthContext.metadata` — same service pattern as a JWT-based app |

> **Finding F22**: always use `domain_replace()` (from `varco_core.model`) instead of
> `dataclasses.replace()` when updating domain entities.  `dataclasses.replace()` skips
> `init=False` fields — `pk` becomes `None` and the repository silently inserts a
> duplicate row.  `domain_replace()` copies all fields including `init=False` ones.

## Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/notes` | Create a note for the caller's tenant → 201 `NoteRead` |
| `GET` | `/v1/notes` | List active (non-soft-deleted) notes for the tenant → 200 |
| `GET` | `/v1/notes/{id}` | Fetch a single note → 200 or 404 |
| `DELETE` | `/v1/notes/{id}` | Soft-delete a note → 204 or 404 |
| `GET` | `/health` | Liveness probe → `{"status": "ok"}` |

All endpoints require the `X-Tenant-Id` header. Notes are scoped to the tenant that
created them — a different tenant receives 404, not 403, to prevent existence-oracle leaks.

## Run locally

```bash
# Start PostgreSQL
docker run -d --name pg \
  -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 \
  postgres:16-alpine

# Run the app
cd examples/22-multi-tenant-soft-delete
DATABASE_URL=postgresql+asyncpg://postgres:secret@localhost:5432/postgres \
  VARCO_SECURITY_ENV=development uv run uvicorn app:app --reload
```

```bash
# Create notes for two tenants
curl -X POST http://localhost:8000/v1/notes \
     -H 'X-Tenant-Id: tenant-A' \
     -H 'Content-Type: application/json' \
     -d '{"title": "Hello from A", "content": "First note"}'

curl -X POST http://localhost:8000/v1/notes \
     -H 'X-Tenant-Id: tenant-B' \
     -H 'Content-Type: application/json' \
     -d '{"title": "Hello from B"}'

# List tenant-A notes (tenant-B notes are invisible)
curl -H 'X-Tenant-Id: tenant-A' http://localhost:8000/v1/notes

# Soft-delete a note (replace <id> with the pk from the create response)
curl -X DELETE -H 'X-Tenant-Id: tenant-A' http://localhost:8000/v1/notes/<id>

# Confirm it is gone from the active list
curl -H 'X-Tenant-Id: tenant-A' http://localhost:8000/v1/notes
```

## Run integration tests

Integration tests spin up a PostgreSQL container automatically via `testcontainers`.

```bash
# Requires Docker daemon running
VARCO_SECURITY_ENV=development uv run pytest -m integration examples/22-multi-tenant-soft-delete/tests/
```

## How it works

- **MRO composition** — `NoteService` inherits `ValidatorServiceMixin`,
  `TenantAwareService`, `SoftDeleteService`, and `AsyncService` left-to-right.
  Each mixin extends exactly one hook (`_scoped_params`, `_check_entity`,
  `_prepare_for_create`, or `_validate_entity`) and chains via `super()` so every
  mixin in the chain runs in declaration order.

- **Tenant scoping** — `TenantAwareService._scoped_params` injects
  `tenant_id = <tid>` into every `list()` / `count()` query.
  `_prepare_for_create` stamps the tenant on the entity before the first `save()`.
  The `NoteCreate` DTO intentionally omits `tenant_id` — the HTTP layer cannot
  inject a foreign tenant ID.

- **Soft delete** — `SoftDeleteService.delete()` uses `domain_replace()` to produce
  a new entity with `deleted_at` set to the current UTC time, then calls `repo.save()`.
  `_scoped_params` appends `deleted_at IS NULL` after the tenant filter so only active
  rows appear in normal list queries.

- **`domain_replace()`** — the assembler's `apply_update()` calls `domain_replace()`
  (not `dataclasses.replace()`) to copy `init=False` fields (`pk`, `_raw_orm`,
  `created_at`, `updated_at`) from the original entity.  Without this, Python ≤ 3.12
  would reset `pk` to `None` and the repository would INSERT a duplicate row instead
  of performing an UPDATE.

- **`list_deleted()`** — a custom service method that bypasses `_scoped_params` and
  builds an explicit `QueryBuilder().eq("tenant_id", tid).and_(is_not_null("deleted_at"))`
  query, exposing a tenant's own soft-deleted notes without making them visible in
  normal list responses.
