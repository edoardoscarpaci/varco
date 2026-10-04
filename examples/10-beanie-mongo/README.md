# Example 10 — Beanie/MongoDB

Demonstrates the `varco_beanie` Beanie ODM backend: auto-generated Beanie Documents
from `DomainModel` annotations, `BeanieRepositoryProvider`, `BeanieUnitOfWork`,
and full CRUD via `CRUDRouter`.

## What it shows

| Concept | Implementation |
|---|---|
| ODM model generation | `BeanieModelFactory` auto-generates a Beanie `Document` from `Post(AuditedDomainModel)` |
| Repository provider | `BeanieRepositoryProvider` — resolves `AsyncRepository[Post]` |
| Unit of work | `BeanieUnitOfWork` — wraps Motor session for CRUD |
| CRUD API | `CRUDRouter[Post, UUID, PostCreate, PostRead, PostUpdate]` |

## Infrastructure

| Service | Purpose |
|---|---|
| MongoDB 7 | Document storage for blog posts |

## Run locally

```bash
export MONGODB_URL="mongodb://localhost:27017"
cd examples/10-beanie-mongo
VARCO_SECURITY_ENV=development uv run uvicorn app:app --reload
```

```bash
# Create a post
curl -X POST http://localhost:8000/v1/posts \
     -H "Content-Type: application/json" \
     -d '{"title": "Hello", "content": "World", "author": "alice"}'

# List posts
curl http://localhost:8000/v1/posts
```

## Run integration tests

```bash
VARCO_SECURITY_ENV=development uv run pytest examples/10-beanie-mongo/tests/ -v -m integration
```

## Key design notes

- `BeanieRepositoryProvider.init()` (which calls `init_beanie()`) must run
  inside a live asyncio event loop — it is called in the FastAPI startup hook
  in production and explicitly in the test fixture (since `ASGITransport`
  does not trigger startup hooks).
- `Post.pk` uses `PKStrategy.UUID_AUTO` — a UUID is auto-generated before
  the first `save()`, so no MongoDB round-trip is needed to retrieve the ID.
