# Varco Example — Post API

End-to-end reference implementation of the **varco** framework stack.  
Demonstrates JWT auth, RBAC authorization, Redis caching, Redis Streams event bus,
SQLAlchemy async ORM, async job runner, CRUD routing, WebSocket push, and SSE streaming
— wired together with the providify DI container in ~350 lines of application code.

---

## Quick start

```bash
# 1. Clone and install workspace dependencies
git clone <repo>
cd varco
uv sync

# 2. Start infrastructure
cd example
docker compose up -d          # PostgreSQL + Redis

# 3. Run the app
DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost/example" \
VARCO_REDIS_URL="redis://localhost:6379" \
VARCO_REDIS_CACHE_URL="redis://localhost:6379" \
uv run uvicorn example.app:create_app --factory --reload

# 4. Open the interactive docs
open http://localhost:8000/docs
```

The `docker-compose.yml` ships with all required env vars pre-configured — just `docker compose up` is enough for a full stack.

---

## API reference

### Authentication

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/auth/login` | Exchange username + password for a JWT Bearer token |
| `GET`  | `/auth/me`    | Inspect the current caller's identity and roles |

**Demo users**

| Username | Password  | Role   | Can do |
|----------|-----------|--------|--------|
| `alice`  | `alice123`| admin  | Full CRUD on all posts |
| `bob`    | `bob123`  | editor | Create + read any post; update/delete **own** posts only |
| *(none)* | —         | anonymous | Read-only (list + get) |

**Login flow**

```bash
# 1. Obtain a token
TOKEN=$(curl -s -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "alice", "password": "alice123"}' \
  | jq -r .access_token)

# 2. Use the token on any endpoint
curl http://localhost:8000/v1/posts \
  -H "Authorization: Bearer $TOKEN"
```

---

### Posts (`/v1/posts`)

| Method   | Path                     | Auth required | Description |
|----------|--------------------------|---------------|-------------|
| `POST`   | `/v1/posts`              | editor / admin | Create a post |
| `GET`    | `/v1/posts`              | none (public)  | List all posts (paginated) |
| `GET`    | `/v1/posts/{id}`         | none (public)  | Fetch one post by UUID |
| `GET`    | `/v1/posts/{id}/summary` | none (public)  | Lightweight summary (pk + title) |
| `PUT`    | `/v1/posts/{id}`         | owner / admin  | Full replace |
| `PATCH`  | `/v1/posts/{id}`         | owner / admin  | Partial update |
| `DELETE` | `/v1/posts/{id}`         | owner / admin  | Delete |

**Create a post (alice)**

```bash
curl -X POST http://localhost:8000/v1/posts \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title": "Hello World", "body": "First post!"}'
# → 201 {"pk": "...", "title": "Hello World", "author_id": "...", ...}
```

**Async mode** — append `?with_async=true` to `POST /v1/posts` or `DELETE /v1/posts/{id}` to receive `202 Accepted` with a `job_id` that can be polled via `GET /jobs/{job_id}`:

```bash
curl -X POST "http://localhost:8000/v1/posts?with_async=true" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title": "Async post", "body": "Queued"}'
# → 202 {"job_id": "...", "status": "pending"}
```

---

### Real-time streams

| Protocol | Path | Description |
|----------|------|-------------|
| WebSocket | `ws://localhost:8000/ws/posts` | Push all post events to connected client |
| SSE      | `GET /events/posts`            | Server-Sent Events stream of post events |

**WebSocket**

```javascript
const ws = new WebSocket("ws://localhost:8000/ws/posts");
ws.onmessage = (e) => console.log(JSON.parse(e.data));
// {"event_type": "posts.post.created", "event_id": "...", "data": {...}}
```

**SSE (curl)**

```bash
curl -N http://localhost:8000/events/posts
# data: {"event_type": "posts.post.created", ...}
```

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│  HTTP client (browser, curl, httpx)                                   │
└───────────────────┬───────────────────────────────────────────────────┘
                    │ HTTP / WebSocket
┌───────────────────▼───────────────────────────────────────────────────┐
│  FastAPI  (uvicorn ASGI)                                              │
│                                                                       │
│  Middleware stack (outermost → innermost)                             │
│    ErrorMiddleware        — JSON error responses                      │
│    CORSMiddleware         — CORS headers                              │
│    RequestContextMiddleware — AuthContext ContextVar per request      │
│                                                                       │
│  Routers                                                              │
│    PostRouter   /v1/posts  (VarcoCRUDRouter + 6 mixins)               │
│    AuthRouter   /auth                                                 │
│    StreamsRouter /ws/posts, /events/posts                             │
└───────┬───────────────────────────────────────────────────────────────┘
        │ injects via providify DI
┌───────▼───────────────────────────────────────────────────────────────┐
│  PostService                                                          │
│    ├─ CacheServiceMixin    — Redis look-aside cache                   │
│    ├─ AsyncService         — CRUD + event publishing                  │
│    ├─ PostAuthorizer       — RBAC + ownership rules                   │
│    └─ PostAssembler        — domain ↔ DTO conversion                  │
└───────┬──────────────────┬────────────────────────────────────────────┘
        │                  │
┌───────▼──────┐   ┌───────▼────────────────────────────────────────────┐
│  SQLAlchemy  │   │  Redis                                             │
│  (asyncpg)   │   │    RedisStreamEventBus  — at-least-once events     │
│  PostgreSQL  │   │    RedisCache           — look-aside key/value     │
└──────────────┘   │    WebSocketEventBus    — fan-out to WS clients    │
                   │    SSEEventBus          — fan-out to SSE clients   │
                   └────────────────────────────────────────────────────┘
```

---

## Component breakdown

### `example/app.py` — bootstrap

`create_app()` is a synchronous factory that:

1. Builds a `JwtAuthority` (RSA-2048 key from env, or ephemeral in dev mode).
2. Creates a `DIContainer` and provides an `SAConfig` via `make_sa_provider(Base, Post)`.
3. Scans all backend packages in one pass:
   - `sa_bootstrap(container)` — SQLAlchemy repo provider + UoW binding
   - `redis_bootstrap(container, streams=True)` — Redis Streams event bus
   - `ws_bootstrap(container)` — WebSocket + SSE adapters
   - `fastapi_bootstrap(container, setup_producer=True)` — FastAPI defaults + event producer
   - `container.scan("example")` — app-level singletons (service, assembler, authorizer, consumer)
4. Wires the `VarcoLifespan` with an async `_bootstrap` hook that runs at startup:
   - `create_tables(container)` — creates DB schema
   - `redis_async_bootstrap(container, setup_cache=True)` — starts Redis cache
   - Resolves and registers lifecycle components (bus → consumer → ws_bus → sse_bus → job_runner)
   - Resolves `PostService`, builds routers, and calls `app.include_router()`

### `example/models.py` — domain model

```python
class Post(DomainModel):
    pk:         UUID   # auto-assigned by SQLAlchemy
    title:      str
    body:       str
    author_id:  UUID | None
    created_at: datetime
    updated_at: datetime
```

`DomainModel` is a Python dataclass.  `varco_sa` auto-generates the SQLAlchemy ORM mapping at import time via `SAModelFactory` — no `Column()` declarations needed.

### `example/service.py` — business logic

```python
class PostService(
    CacheServiceMixin,          # transparent Redis caching on get()
    AsyncService[Post, UUID, PostCreate, PostRead, PostUpdate],
):
```

- `create()` stamps `author_id` from `AuthContext.user_id`, saves the entity, and publishes `PostCreatedEvent`.
- `update()` / `delete()` run `PostAuthorizer.authorize()` before touching the DB.
- `get()` results are cached in Redis with a 5-minute TTL; `update()` and `delete()` evict the cache entry.

### `example/authorizer.py` — RBAC

| Role | list | read | create | update | delete |
|------|------|------|--------|--------|--------|
| admin | ✅ | ✅ | ✅ | ✅ any | ✅ any |
| editor | ✅ | ✅ | ✅ | ✅ own | ✅ own |
| anonymous | ✅ | ✅ | ❌ | ❌ | ❌ |

Ownership is checked by comparing `post.author_id` with `ctx.user_id` (the JWT `sub` claim).

### `example/consumer.py` — event consumer

`PostEventConsumer` listens for `PostCreatedEvent` and `PostDeletedEvent` on the `"posts"` channel.  Handlers log the events and (in a real app) would notify subscribers, update read models, etc.

### `example/streams.py` — real-time push

`StreamsRouter` (`@Singleton`) injects `WebSocketEventBus` and `SSEEventBus` from the DI container.  Both adapters subscribe to all events on all channels when `start()` is called — every post event is fanned out to all connected WebSocket and SSE clients.

---

## DI wiring overview

```
DIContainer
│
├── SAConfig          ← make_sa_provider(Base, Post)  reads DATABASE_URL
├── RepositoryProvider← sa_bootstrap()   [auto-discovered @Singleton]
├── IUoWProvider      ← sa_bootstrap()   [re-export of RepositoryProvider]
├── AsyncRepository[Post] ← bind_repositories(container, Post)
│
├── AbstractEventBus  ← redis_bootstrap(streams=True) [RedisStreamEventBus]
│
├── WebSocketEventBus ← ws_bootstrap()  [auto-discovered @Singleton]
├── SSEEventBus       ← ws_bootstrap()  [auto-discovered @Singleton]
│
├── AbstractEventProducer ← fastapi_bootstrap(setup_producer=True)
├── TrustStore, CORSConfig, TaskRegistry ← fastapi_bootstrap()
│
├── PostAssembler     ← container.scan("example")  [@Singleton]
├── PostAuthorizer    ← container.scan("example")  [@Singleton, priority=0]
├── PostService       ← container.scan("example")  [@Singleton]
├── PostEventConsumer ← container.scan("example")  [@Singleton]
└── StreamsRouter     ← container.scan("example")  [@Singleton]
```

---

## Running the tests

```bash
# Unit tests (no Docker required)
uv run pytest example/example/tests/test_post_service.py -v

# Integration tests (requires Docker)
uv run pytest example/example/tests/ -m integration -v

# Run a specific integration test file
uv run pytest example/example/tests/test_api_auth.py -m integration -v
uv run pytest example/example/tests/test_e2e_integration.py -m integration -v
```

### Test structure

| File | Type | Needs Docker | Coverage |
|------|------|-------------|----------|
| `test_post_service.py` | Unit | No | PostService + EventConsumer logic |
| `test_e2e_integration.py` | Integration | Yes | Full CRUD via HTTP, caching, 404 |
| `test_api_auth.py` | Integration | Yes | Auth login, `/auth/me`, RBAC (admin/editor/anonymous) |

---

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | asyncpg PostgreSQL DSN |
| `VARCO_REDIS_URL` | Yes | — | Redis URL for the event bus |
| `VARCO_REDIS_CACHE_URL` | Yes | — | Redis URL for the cache backend |
| `VARCO_JWT_PRIVATE_KEY_PEM` | No | auto-generated | RSA/EC PEM private key for JWT signing |
| `VARCO_REDIS_USE_STREAMS` | No | `false` | Set `true` to use Redis Streams (at-least-once delivery) |
| `VARCO_CORS_ORIGINS` | No | `*` | Comma-separated allowed CORS origins |

---

## Extending the example

### Add a new entity (e.g. `Comment`)

```python
# 1. Domain model (models.py)
class Comment(DomainModel):
    pk: UUID
    post_id: UUID
    body: str
    author_id: UUID | None

# 2. DTOs (dtos.py)
class CommentCreate(CreateDTO): body: str
class CommentRead(ReadDTO):     pk: UUID; body: str; author_id: UUID | None
class CommentUpdate(UpdateDTO): body: str | None = None

# 3. Service (service.py)
@Singleton
class CommentService(AsyncService[Comment, UUID, CommentCreate, CommentRead, CommentUpdate]):
    def _get_repo(self, uow): return uow.get_repository(Comment)

# 4. Router (router.py)
@Singleton
class CommentRouter(CreateMixin, ReadMixin, ListMixin, VarcoCRUDRouter[...]):
    _prefix = "/comments"

# 5. Wire in app.py
container.provide(make_sa_provider(Base, Post, Comment))   # add Comment
bind_repositories(container, Post, Comment)
```

### Use Redis Streams (at-least-once delivery)

```bash
VARCO_REDIS_USE_STREAMS=true uv run uvicorn example.app:create_app --factory
```

Or in code:

```python
redis_bootstrap(container, streams=True)
```

### Add a custom route to PostRouter

```python
@route("GET", "/{post_id}/stats")
async def get_stats(self, post_id: UUID) -> dict:
    ctx = get_request_context().auth
    post = await self._service.get(post_id, ctx)
    return {"pk": str(post.pk), "title_length": len(post.title)}
```
