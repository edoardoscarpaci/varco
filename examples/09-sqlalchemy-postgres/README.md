# 09 — SQLAlchemy / PostgreSQL

A complete blog-post CRUD API backed by a real **PostgreSQL** database using the `varco_sa` package.

## What this example demonstrates

| Feature | Where |
|---|---|
| `DomainModel` + `FieldHint` / `PrimaryKey` annotations | `models.py` |
| `SAModelFactory` auto-generates ORM model (no manual `Column()`) | wired in `app.py` via `SAConfig` |
| `SAConfig` — single injectable object (engine + base + entities) | `app.py` `_build_container()` |
| `AsyncRepository[Post]` / `SAUoWProvider` | injected into `PostService` |
| Query filtering: `?filter=author__eq=alice` → SQL `WHERE` | `GET /v1/posts` |
| Timestamp hooks: `_prepare_for_create` + `apply_update` | `service.py`, `assembler.py` |
| `AbstractDTOAssembler` / `domain_replace` for safe UPDATE | `assembler.py` |

## Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/posts` | Create a post |
| `GET` | `/v1/posts/{id}` | Read one post |
| `PUT` | `/v1/posts/{id}` | Replace a post (full update) |
| `DELETE` | `/v1/posts/{id}` | Delete a post |
| `GET` | `/v1/posts` | List all posts (supports `?filter=`) |

### Filter syntax

The list endpoint accepts `?filter=field__op=value`:

```
?filter=author__eq=alice
?filter=title__contains=hello
```

Supported operators: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `contains`, `startswith`, `endswith`.

## Run locally

Requires a running PostgreSQL instance:

```bash
export DATABASE_URL="postgresql+asyncpg://user:pw@localhost:5432/mydb"
cd examples/09-sqlalchemy-postgres
VARCO_SECURITY_ENV=development uv run uvicorn app:app --reload
```

Try it:

```bash
# Create
curl -X POST http://localhost:8000/v1/posts \
     -H "Content-Type: application/json" \
     -d '{"title": "Hello", "body": "World", "author": "alice"}'

# Filter
curl "http://localhost:8000/v1/posts?filter=author__eq=alice"
```

## Run tests

Tests require Docker (PostgreSQL is managed by `testcontainers`):

```bash
# From the workspace root
VARCO_SECURITY_ENV=development uv run pytest examples/09-sqlalchemy-postgres/tests/ -v -m integration
```

## Key design decisions

- **`Base` is module-level** — `SAModelFactory.build(Post)` registers into the same `Base.metadata`, so `create_all()` creates all tables in one call.  Call `create_app()` only once per process.
- **Timestamps stamped in Python** — `PostService._prepare_for_create()` sets `created_at`/`updated_at` before `save()`, so values are available immediately without a SELECT round-trip.
- **`domain_replace()` not `dataclasses.replace()`** — `apply_update` preserves `init=False` fields (`pk`, `_raw_orm`) so the repository performs UPDATE, not INSERT.
- **No auth** — intentional; this example focuses on the SA backend patterns.  Add `_auth = JwtBearerAuth(...)` to `PostRouter` for production use.
