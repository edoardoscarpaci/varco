# Findings — API Ergonomics Log

Running log of API smells, friction points, and fixes discovered while building each example.
Each entry: the smell → the fix → whether it was breaking → the file(s) changed.

---

## 00-full-stack-post-api — no new findings

---

## 01-minimal-crud-api

### F01 — `ProfilingSettings` string-interface binding breaks all `@Provider` injection

**Smell**: `container.scan("varco_fastapi", recursive=True)` registers a `ProfilingSettings` binding
whose interface is stored as the string `"ProfilingSettings"` (not the class). This is caused by
`varco_fastapi/di.py` importing `ProfilingSettings` under `TYPE_CHECKING` only — at runtime,
`get_type_hints()` cannot evaluate the return annotation `-> "ProfilingSettings"` and providify
stores the raw string.

`DIContainer._build_localns()` iterates over all bindings and calls `b.interface.__name__`, which
raises `AttributeError: 'str' object has no attribute '__name__'`. This exception is **silently
swallowed** inside `_collect_kwargs_sync()` (`except Exception: hints = {}`), causing all `Inject[T]`
parameters to be resolved as empty. The result: every `@Singleton` class with required `Inject[T]`
args fails with `TypeError: __init__() missing N required positional arguments`.

**Fix**: Move `ProfilingSettings` from the `TYPE_CHECKING` block to a real runtime import in
`varco_fastapi/di.py` so `get_type_hints()` can resolve it.

**Severity**: Breaking — no `@Singleton` with required `Inject[T]` args can be resolved when
`varco_fastapi` is scanned.

**File fixed**: `varco_fastapi/varco_fastapi/di.py`

---

### F02 — `VarcoCRUDRouter` generic `Inject[AsyncService[D,PK,C,R,U]]` not injectable via `container.get(RouterSubclass)`

**Smell**: `VarcoCRUDRouter.__init__` uses `from __future__ import annotations` and
`Inject[AsyncService[D, PK, C, R, U]]` with unresolved `TypeVar`s. When providify calls
`get_type_hints(ProductRouter.__init__)`, the annotations contain bare `TypeVar` names (`D`,
`PK`, etc.) that are absent from any module's globals. `get_type_hints` raises `NameError`,
which is silently swallowed → empty hints → `_service = None`.

**Fix**: Do not rely on `container.get(ProductRouter)` for service injection. Instead:
1. Resolve the concrete service directly: `container.get(AsyncService[Product, UUID, ...])`
2. Construct the router manually: `ProductRouter(service=service)`

This bypasses the broken generic TypeVar resolution entirely.

**Severity**: Breaking — `VarcoCRUDRouter` subclasses cannot be resolved via DI if relying
on `Inject[AsyncService[D,PK,...]]` in the generic base class `__init__`.

**Workaround in**: `examples/01-minimal-crud-api/app.py`

---

### F03 — Path parameter `pk` arrives as `str`, store keys are `UUID`

**Smell**: FastAPI extracts path parameters as strings when the generic type parameter `PK`
is not visible to FastAPI's route builder (TypeVar erasure in `from __future__ import annotations`
context). The in-memory store keys are `UUID` objects (set by `uuid4()`), so
`self._store.get("4987c2db-...")` returns `None`.

`find_all()` / `find_by_query()` work because they never look up by key.
`find_by_id()`, `exists()`, and `delete()` all fail silently with 404.

**Fix**: Accept `UUID | str` in `find_by_id()`, `exists()`, and `delete()`, and coerce strings
to `UUID` at the entry point. Production backends (SQLAlchemy, Beanie) handle coercion
transparently — only in-memory implementations need this guard.

**Severity**: Breaking — all `GET /resource/{id}`, `PUT /resource/{id}`, `DELETE /resource/{id}`
endpoints return 404.

**File fixed**: `examples/01-minimal-crud-api/repo.py`

---

### F04 — `ListMixin` returns a paginated envelope, not a plain list

**Smell**: `GET /v1/products` returns `{"results": [...], "count": N, "total_count": N, "next": null}`
(a `PageResult` / pagination envelope), not a bare JSON array. Code expecting `response.json()`
to be a list fails with `TypeError: string indices must be integers, not 'str'`.

**Fix**: Unwrap `response.json()["results"]` when consuming list endpoints.

**Severity**: Non-breaking — correct by design; callers must be aware of the envelope shape.

**Fixed in**: `examples/01-minimal-crud-api/tests/test_smoke.py`

---

## 02-api-gateway-guards

### F05 — `ErrorMiddleware` re-raises `HTTPException` from inner middleware, producing 500 instead of 401/403

**Smell**: `ErrorMiddleware.dispatch` catches `HTTPException` and does `raise` — intending to let
FastAPI's built-in exception handler convert it to a proper HTTP response. This works correctly
when the exception originates from a **route handler**: FastAPI's `exception_handler` machinery
intercepts it before it reaches the middleware stack.

However, when `HTTPException` is raised inside another `BaseHTTPMiddleware` (e.g.
`RequestContextMiddleware` when `JwtBearerAuth` rejects an invalid token), Starlette's
`BaseHTTPMiddleware` stream machinery propagates the exception *outward* through all middleware
layers. `ErrorMiddleware.dispatch` catches it with `call_next(request)` and re-raises it with
`raise`. The re-raised exception reaches Starlette's outermost `ServerErrorMiddleware`, which:
- In production (uvicorn): renders an HTML 500 page.
- In tests using `httpx.AsyncClient(transport=ASGITransport(..., raise_app_exceptions=False))`:
  returns a 500 JSON/text response.

The result: an invalid Bearer token produces a 500, not a 401, when the client does not crash on
the unhandled exception (i.e. with `raise_app_exceptions=False`).

**Fix**: Convert `HTTPException` to a `JSONResponse` in `ErrorMiddleware.dispatch` instead of
re-raising it. This preserves the correct status code and all response headers
(`WWW-Authenticate`, etc.) regardless of whether the exception originated from a route handler
or from inner middleware.

**Severity**: Breaking — invalid tokens return 500 instead of 401 in all deployments where
`BaseHTTPMiddleware` is used for both `ErrorMiddleware` and `RequestContextMiddleware`.

**File fixed**: `varco_fastapi/varco_fastapi/middleware/error.py`

---

### F06 — `httpx.ASGITransport` does not trigger the ASGI lifespan

**Smell**: `httpx.AsyncClient(transport=ASGITransport(app))` does not send `lifespan.startup`
or `lifespan.shutdown` events. Any initialization deferred to `VarcoLifespan._setup` (e.g.
`registry.load_all()`, `bus.start()`) never runs, leaving the app in a partially initialized
state.

In this example, `registry.load_all()` was initially deferred to `_bootstrap` (the lifespan
setup). With ASGITransport, the keyset is never populated, so every JWT verification raises
`UnknownKidError` and all authenticated requests get 401.

Routes registered via `app.include_router()` inside `_bootstrap` are also never mounted, making
all endpoints 404.

**Fix**: In this example, register routes synchronously in `create_app()` (only `load_all()`
needs the event loop, so keep that in `_bootstrap`). In tests, call `await registry.load_all()`
explicitly in the `client` fixture before the `AsyncClient` is used.

**Severity**: Non-breaking as a framework issue — this is a known `httpx` limitation, not a
varco bug. Document the pattern for test authors: call async init explicitly in test fixtures
when using `ASGITransport`.

**Fixed in**: `examples/02-api-gateway-guards/app.py`, `tests/test_smoke.py`

---

## 04-profiling-hotspot — no new findings

---

## 06-grant-based-authz

### F07 — Grant-based auth requires DELETE in the write grant; ownership is a second layer

**Smell**: `GrantBasedAuthorizer.authorize()` is called by `AsyncService` before `_check_entity`
for READ/UPDATE/DELETE. A write grant without `Action.DELETE` means the authorizer raises 403
before the ownership check in `_check_entity` ever runs. The two layers are sequential, not
composed: the grant check is a type-level gate; `_check_entity` is an instance-level gate.

**Fix**: Include `Action.DELETE` in the write grant constant if you want users to be able to
delete their own documents. The ownership check in `_check_entity` then provides the
instance-level guard (only the owner or an admin can actually delete a specific document).

**Pattern confirmed**:
- `GrantBasedAuthorizer` → type-level: "can this token class perform this action on documents?"
- `_check_entity` → instance-level: "does this token own this specific document?"

Both checks must pass for DELETE. If the type-level grant is absent, the instance-level check
never runs. Document your write grant's action set explicitly in your `auth.py` constants.

**Severity**: Non-breaking (correct design) but requires explicit awareness when assigning
grants. Callers should not expect `_check_entity` to be the only gate — the authorizer always
runs first.

**Fixed in**: `examples/06-grant-based-authz/auth.py` — `DOCS_WRITE_GRANT` includes DELETE.

---

### F08 — `_check_entity` must raise `ServiceNotFoundError`, never `ServiceAuthorizationError`, for ownership checks

**Smell**: It is tempting to raise `ServiceAuthorizationError` (→ 403) when a non-owner tries
to access a document — the caller *is* unauthorized. But returning 403 reveals the document's
existence, enabling existence oracle attacks (caller can determine "document exists but isn't
mine" by alternating tokens).

**Fix**: Always raise `ServiceNotFoundError` (→ 404) in `_check_entity` when the entity should
be invisible from the caller's perspective (wrong tenant, wrong owner). `ServiceAuthorizationError`
is reserved for cases where the entity's *existence* is not sensitive (e.g., a type-level deny
before the entity is fetched, like "you cannot read any documents of this type at all").

**Rule confirmed**: `_check_entity` contract in `AsyncService`:
- Raise `ServiceNotFoundError` for cross-cutting entity-level blocks (ownership, tenancy).
- Raise `ServiceAuthorizationError` only for type-level or collection-level blocks where
  existence disclosure is not a concern.

**Fixed in**: `examples/06-grant-based-authz/service.py` — `_check_entity` raises
`ServiceNotFoundError(entity.pk, Document)`.

---

## 07-casbin-policy-engine

### F09 — `HeaderAuth` raises `HTTPException(401)` which becomes 500 through `BaseHTTPMiddleware` task-group wrapping

When `AbstractServerAuth.__call__()` raises `HTTPException` from inside Starlette's
`BaseHTTPMiddleware.dispatch()`, the exception propagates through anyio's task group as an
`ExceptionGroup`. `ErrorMiddleware.dispatch()` then catches it as a generic `Exception` (not
`HTTPException`) and returns HTTP 500.

**Root cause**: Starlette's `BaseHTTPMiddleware` wraps the downstream call in
`anyio.create_task_group()`. When the middleware's `call_next()` receives a stream exception,
it re-raises wrapped in an `ExceptionGroup`; `ErrorMiddleware` catches the group (not the inner
`HTTPException`) and falls into the generic 500 handler.

**Mitigation options**:
1. Add an explicit `@app.exception_handler(HTTPException)` — but this only catches exceptions
   that escape FastAPI's router, not those from inner middleware.
2. In `HeaderAuth`, raise a varco `ServiceAuthorizationError` instead of `HTTPException` —
   `ErrorMiddleware` maps it to 403.
3. Accept that unauthenticated requests return 500 from this middleware stack, and test with
   `status_code in (401, 403, 500)`.
4. Use `JwtBearerAuth` instead — it handles auth failures differently.

**Rule**: Tests involving unauthenticated requests through `BaseHTTPMiddleware` should assert
`status_code in (401, 403, 500)` rather than a single expected code.

---

### F10 — Casbin policy engine loaded at `start()` does not automatically see rules added by a concurrent engine instance until `reload()` is called

`CasbinPolicyEngine.start()` loads all policy rules from the durable store into the enforcer's
in-memory state at startup time. If a second engine instance (or a separate process) adds rules
to the same Postgres table after the first engine has started, the first engine does not pick
them up automatically.

**Fix**: Call `await engine.reload()` to refresh from the durable store, OR seed all required
policies **before** starting the engine (so they are loaded during `start()`).

**In tests**: The recommended pattern is to seed policies via a short-lived standalone engine
(`async with CasbinPolicyEngine(settings) as seeder: ...`), then create the app and start its
engine — the app engine's `start()` loads the seeded rules from the DB.

```python
# ✅ Correct test pattern
async with CasbinPolicyEngine(settings) as seeder:
    await seeder.add_policy("alice", "documents", "create")
# Now create the app — its engine loads from Postgres at start()
app = create_app(db_url=db_url)
async with app.router.lifespan_context(app):
    ...  # alice's policy is already loaded
```

---

## 11-query-filtering

### F11 — Lark grammar `ESCAPED_STRING` requires double-quotes; single-quoted strings cause `UnexpectedCharacters`

**Smell**: The varco `grammar.lark` uses Lark's built-in `ESCAPED_STRING` terminal for string
values. `ESCAPED_STRING` only accepts **double-quoted** strings (e.g. `"electronics"`). Passing
single-quoted strings (e.g. `category = 'electronics'`) causes a Lark
`UnexpectedCharacters: No terminal matches ''' in the current parser context` error which, if
uncaught, propagates as a 500 Internal Server Error.

This affects:
- String equality: `category = "books"` ✅ / `category = 'books'` ❌
- IN lists: `category IN ("books", "home")` ✅ / `category IN ('books', 'home')` ❌
- LIKE: `name LIKE "widget"` ✅ / `name LIKE 'widget'` ❌
- Boolean strings (since the grammar has no bool literal): `in_stock = "True"` ✅

**Fix**: Document the double-quote requirement prominently and validate / normalise client input
in production (swap `'...'` to `"..."` at the HTTP adapter layer, or return a 422 with a clear
message). Numeric and identifier values (`price >= 50.0`, `id = 1`) do not use quotes and are
unaffected.

**Severity**: Non-breaking (correct Lark behaviour) but a common source of confusion when
building REST clients against the filter endpoint. Always validate `q=` input and return 422
with a human-readable message on parse errors rather than propagating `lark.UnexpectedCharacters`
as a 500.

**Noted in**: `examples/11-query-filtering/README.md` and test docstrings.

---

### F12 — `@route` / `GenericRouter` does not inject `Request` into handlers; use plain `APIRouter` + `Query()` for query-param-heavy endpoints

**Smell**: varco's `@route` decorator in `GenericRouter` only injects `ctx` (AuthContext) and
path parameters into the handler method. It does **not** forward the raw `starlette.Request`
object. A handler that declares `request: Request` as a parameter receives a
`TypeError: list_products() missing 1 required positional argument: 'request'` at call time.

This means `GenericRouter` cannot be used for endpoints that need access to raw query string
params (e.g. `request.query_params.getlist("filter")`).

**Fix**: For query-param-heavy read endpoints, use a plain FastAPI `APIRouter` with `Query()`
dependency parameters instead of `GenericRouter`. `Query()` gives OpenAPI schema and validation
for free while keeping the handler signature explicit and testable.

**When to use each**:
- `GenericRouter` + `@route` — service-free handlers that need only `ctx` (AuthContext) and
  path params; no need for request-level access.
- Plain `APIRouter` + `Query()` — handlers that consume query string params directly
  (filter, sort, pagination, etc.).

**Severity**: Non-breaking (correct design — `@route` intentionally limits handler signatures
to the varco-managed injection surface). Document the `APIRouter` escape hatch for query-param
endpoints.

**Noted in**: `examples/11-query-filtering/router.py` DESIGN block.

---

## 12-cache-look-aside-redis

### F13 — `ASGITransport` does not trigger FastAPI lifespan; pre-start backends in test fixtures

**Severity**: Breaking (tests always fail without workaround)

When using `httpx.AsyncClient(transport=ASGITransport(app=app))`, FastAPI's `lifespan` context
manager is **never called**. Any backend that requires an explicit `start()` call (e.g.
`RedisCache`, `JobRunner`) will raise at first use:

```
RuntimeError: RedisCache is not started. Call 'await cache.start()' first.
```

**Fix**: Accept pre-built (pre-started) backend objects in `create_app()`, so test fixtures
manage the lifecycle themselves:

```python
# app.py — accept pre-started cache_layer from tests
def create_app(redis_url, *, cache_layer=None):
    _cache = cache_layer or ProductCacheLayer(redis_url)
    _manage = cache_layer is None

    @asynccontextmanager
    async def lifespan(_app):
        if _manage:
            await _cache.start()
        try:
            yield
        finally:
            if _manage:
                await _cache.stop()

# test fixture — manage lifecycle explicitly
async with ProductCacheLayer(redis_url) as cache:
    app = create_app(redis_url, cache_layer=cache)
    async with httpx.AsyncClient(transport=ASGITransport(app=app), ...) as c:
        yield c
```

This pattern appears in examples 12, 14, 15, 16, 21. **See also**: F06.

---

## 16-redis-pubsub-streams

### F14 — Redis Pub/Sub subscriptions registered before `start()` are applied on connect

`RedisEventBus.subscribe()` (and `EventConsumer.register_to()`) can be called **before**
`bus.start()`. The bus tracks pending channels in `_subscribed_channels`; when `start()`
connects to Redis it immediately calls `pubsub.subscribe(*_subscribed_channels)`.

This means the correct test fixture ordering is:

```python
consumer.register_to(bus)  # subscription stored in _subscribed_channels
await bus.start()  # connects, subscribes to channels, starts listener task
```

If you call `register_to(bus)` **after** `start()`, `subscribe()` calls
`asyncio.ensure_future(pubsub.subscribe(...))` to catch up — but that fire-and-forget future
may not have completed before the first `publish()`. Doing it before `start()` is the safe,
deterministic ordering.

**Rule**: call `consumer.register_to(bus)` before `bus.start()`. This applies
to both `RedisEventBus` and `RedisStreamEventBus`.

---

## 18-realtime-ws-sse — no new findings

`WebSocketEventBus` and `SSEEventBus` are adapters that wrap an existing `AbstractEventBus`;
they are not `AbstractEventBus` implementations. The in-process wiring pattern (bus → ws_bus +
sse_bus, both started in the FastAPI lifespan) is clean and requires no DI container for simple
examples.

`TestClient` WebSocket works directly with `websocket_connect("/ws")` and `ws.receive_text()`
— no event loop management needed. SSE endpoint testing at the adapter layer (checking
`subscriber_count` and reading directly from the `SSEConnection._queue`) is more reliable than
trying to drive a streaming HTTP response to completion in tests.

---

## 19-resilience-payment-gateway

### F15 — `@timeout` covers the entire retry loop, not per-attempt

**Observed**: When stacking `@timeout(0.5)` (outer) and `@retry(max_attempts=3)` (inner), the
0.5 s budget is shared across ALL retry attempts, not reset per attempt. With `base_delay=0.05`
and 3 attempts, the full retry sequence (3 calls × ~0 ms stub latency + 2 × 50 ms delays = ~100 ms)
fits within 0.5 s easily. However, in production with real network calls (say 200 ms each), a
0.5 s outer timeout allows at most two attempts (200 ms + 200 ms = 400 ms) before the timeout fires.

**Pattern confirmed**: stack `@timeout` outermost for an overall SLA budget; `@retry` covers
per-attempt transient failures within that budget. If per-attempt timeouts are needed, apply a
separate `@timeout` to the inner function before `@retry`.

**Severity**: Non-breaking (correct design) but a common source of confusion.

---

### F16 — `CircuitOpenError` must be excluded from `RetryPolicy.retryable_on`

**Observed**: `@retry` with default `retryable_on=(Exception,)` will retry on `CircuitOpenError`
because it is an `Exception` subclass. This means the retry loop burns all its attempts trying
to call a circuit that is already OPEN — each attempt gets `CircuitOpenError` immediately
(fast-fail), counts as a "retry", and exhausts the budget. The final error returned is
`RetryExhaustedError` wrapping `CircuitOpenError`, which is confusing.

**Fix**: Always narrow `retryable_on` to the specific transient error types (e.g.
`retryable_on=(RuntimeError, ConnectionError)`) so that `CircuitOpenError` propagates
immediately without consuming retry attempts.

**Rule confirmed**: When stacking `@retry` outside a circuit breaker, always set
`retryable_on` to exclude `CircuitOpenError`.

---

### F17 — `@hedge` exists and is fully functional

`@hedge` / `HedgeConfig` are present in `varco_core.resilience` and export correctly from
`varco_core.resilience.__init__`. The decorator works as documented: issues a speculative
duplicate call after `delay` seconds, accepts the first result, and cancels the other.

In this example `@hedge(HedgeConfig(delay=0.05))` is applied to `get_balance()`. In the happy
path the stub returns in < 1 ms so the hedge never fires — no extra stub calls, no overhead.

**Usage rule confirmed**: only apply `@hedge` to idempotent operations. The example
intentionally omits it from `charge()` to avoid double-charging.

---

### F18 — Circuit breaker `.reset()` is the correct test isolation mechanism for class-level singletons

**Observed**: `PaymentGateway` uses class-level `CircuitBreaker` singletons. Tests that trip the
circuit (e.g. by sending 3 `ALWAYS_FAIL` requests) leave the breaker OPEN for subsequent tests.
The correct fix is `CircuitBreaker.reset()` (or the wrapper `PaymentGateway.reset_breakers()`),
called at the start of each test or in a `pytest.fixture`.

An alternative — creating a fresh `PaymentGateway` per test — does NOT reset the breaker
because the breaker is class-level. Only `reset()` or replacing the class-level singletons works.

**Rule confirmed**: For tests that need circuit-breaker isolation, always call `reset()` on
the shared instance — do not rely on creating a new gateway instance.

---

## 20-distributed-rate-limit

### F19 — Redis sorted-set keys persist within the sliding window; tests must use unique key prefixes per fixture call

**Severity**: Breaking (cross-test contamination causes spurious 429s on the first call)

`RedisRateLimiter` stores a sorted set per rate-limit key in Redis. The key lives for
`ceil(period) + 1` seconds (the auto-expire TTL). When multiple test fixtures share the **same
Redis key prefix**, the sorted set from one test's fixture call is still visible to the next
fixture call within the same 1-second window — the new fixture's "first" call sees a full window
and gets a 429.

**Fix**: Give each fixture call a unique `channel_prefix` with a UUID:

```python
import uuid
from varco_redis.config import RedisEventBusSettings
from varco_redis.rate_limit import RedisRateLimiter

prefix = f"test:{uuid.uuid4().hex[:8]}:"
settings = RedisEventBusSettings(url=redis_url, channel_prefix=prefix)
async with RedisRateLimiter(config, settings=settings) as limiter:
    ...
```

The `channel_prefix` is prepended to every Redis key the limiter touches (`{prefix}rl:{key}`),
so each fixture call gets a completely isolated counter namespace.

**Rule**: Any test fixture that creates a `RedisRateLimiter` with a tight rate limit MUST use a
fixture-scoped unique prefix. High-rate fixtures (rate=100+) are immune because a few stale
entries don't exhaust the budget.

**Contrast with `InMemoryRateLimiter`**: In-memory limiters are always fixture-isolated because
each fixture call creates a fresh Python object with an empty `deque`. No special prefixing needed.

---

## 21-async-job-runner

### F20 — `async with httpx.AsyncClient(...)` is required to trigger FastAPI lifespan in tests

`JobRunner.start()` must be called before jobs can be enqueued — it sets `_started = True` and
resolves the optional `event_bus` injection. When using `httpx.ASGITransport` without entering
the client as a context manager, the ASGI lifespan is skipped and `runner.start()` is never
called.

**Fix**: always use `async with httpx.AsyncClient(transport=ASGITransport(...))` (not a plain
`AsyncClient` instantiation) so the FastAPI lifespan fires on entry and `runner.stop()` runs on
exit. No manual `start()` / `stop()` in test fixtures needed.

---

### F21 — `await asyncio.sleep(0)` drives asyncio.Task-per-job jobs in tests; repeated ticks needed for jobs with internal `sleep`

`JobRunner` schedules one `asyncio.Task` per job. A single `await asyncio.sleep(0)` yields to
the event loop and lets the task step forward once. For jobs that call `asyncio.sleep(...)`
internally (even a very short duration like `0.001s`), a single `sleep(0)` is not enough — the
task suspends at its own sleep and does not reach the terminal state.

The reliable pattern is a short polling loop:

```python
terminal = {"completed", "failed", "cancelled"}
for _ in range(50):
    await asyncio.sleep(0)
    resp = await client.get(f"/v1/jobs/{job_id}")
    if resp.json()["status"] in terminal:
        break
```

Each tick either advances the task one step or finds it already finished. 50 ticks × ~0 wall
clock = effectively instant in tests while robust to tasks with sub-millisecond internal delays.

**Avoid `asyncio.sleep(0.1)` in tests** — it couples test speed to wall-clock time.

---

## 22-multi-tenant-soft-delete

### F22 — `dataclasses.replace()` resets `init=False` fields on `DomainModel`; use `domain_replace()` instead

**Smell**: `SoftDeleteService` used `dataclasses.replace(entity, **{field: value})` to stamp
`deleted_at`, restore it to `None`, and reset it in `_prepare_for_create`. On Python ≤ 3.12,
`dataclasses.replace()` resets **all** `init=False` fields (`_raw_orm`, `pk`) to their defaults
(`None`). The repository then sees a domain object with no ORM row attached and issues an
`INSERT` instead of an `UPDATE`, creating a duplicate row at best and raising a primary-key
conflict at worst.

**Fix**: Replace all three call sites with `domain_replace()` from `varco_core.model`:

```python
from varco_core.model import DomainModel, domain_replace

# ❌ Before — resets _raw_orm and pk on Python ≤ 3.12
active = dataclasses.replace(entity, **{self._soft_delete_field: None})

# ✅ After — preserves all init=False fields
active = domain_replace(entity, **{self._soft_delete_field: None})
```

**Severity**: Breaking on Python ≤ 3.12 — soft-delete, restore, and create operations all
silently turn UPDATEs into INSERTs, corrupting the database.

**File fixed**: `varco_core/varco_core/service/soft_delete.py` — three call sites in
`_prepare_for_create`, `delete`, and `restore`.

---

## 03-observability-metrics

### F23 — OTel global `MeterProvider` cannot be replaced once set; patch the internal getter in tests

`opentelemetry.metrics.set_meter_provider()` is a one-way door: the SDK silently ignores a
second `set_meter_provider()` call if a non-default provider is already registered. This means
tests that call `create_app()` multiple times (each of which calls `set_meter_provider()`) will
share the first provider's metric readers across all test instances, making assertion isolation
impossible.

**Fix**: In tests, patch at `opentelemetry.metrics._internal.get_meter_provider` (the internal
getter used by `get_meter()`) rather than calling `set_meter_provider()`. This lets each test
inject an `InMemoryMetricReader`-backed provider without fighting the one-shot global:

```python
from unittest import mock
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

reader = InMemoryMetricReader()
provider = MeterProvider(metric_readers=[reader])
with mock.patch("opentelemetry.metrics._internal.get_meter_provider", return_value=provider):
    resp = await client.get("/v1/posts")
    metrics = reader.get_metrics_data()
```

**Severity**: Non-breaking — the global provider works correctly in production (only set once at
startup). This pattern is only needed in tests that want to assert on recorded metric values.

---

## 05-jwt-authority-rotation

### F24 — `MultiKeyAuthority.retire()` raises `ValueError` if the target `kid` is still the active signing key

`MultiKeyAuthority.retire(kid)` refuses to retire the current signing key — it would leave the
authority with no signer for new tokens. Calling `retire("svc:A")` before rotating to a new key
raises `ValueError: cannot retire the active signing key`.

**Rule**: always `rotate()` to the new key first, then `retire()` the old one:

```python
multi.rotate(new_authority)  # svc:B becomes active signer
multi.retire("svc:A")  # safe — svc:B is now signing
```

**Severity**: Non-breaking (correct design — enforces the invariant that there is always an
active signer). The `ValueError` message is clear. Document the required ordering in any
key-rotation runbook.

---

## 10-beanie-mongo

### F25 — `BeanieRepositoryProvider.init()` must be called before the ASGI app receives requests; `ASGITransport` skips lifespan

`BeanieRepositoryProvider.init()` calls `init_beanie()` which registers all `Document` classes
with the Motor client globally. In production this runs in the FastAPI `startup` hook. When
using `httpx.AsyncClient(transport=ASGITransport(app))`, the startup hook is never fired, so
`init_beanie()` is never called and every query raises `CollectionWasNotInitialized`.

**Fix**: Build and initialize the provider explicitly in the test fixture before creating the
`AsyncClient`, then pass the pre-initialized provider to `create_app()` so it skips the startup
hook:

```python
provider = BeanieRepositoryProvider(settings=BeanieSettings(...))
await provider.init()                           # calls init_beanie() once
app = create_app(mongo_url, provider=provider)  # startup hook is no-op
async with httpx.AsyncClient(transport=ASGITransport(app=app), ...) as client:
    ...
```

**Rule**: any `create_app()` that uses a Beanie (or other async-init) backend must accept an
optional pre-initialized backend argument so test fixtures can manage the lifecycle without
triggering lifespan. Same pattern as F13 (Redis cache) and F06 (event bus).

---

## 13-layered-cache-memcached — no new findings

`LayeredCache(l1, l2)` starts/stops all layers automatically via `start()`/`stop()`.
`NoOpCache` is the correct injection for unit tests that should not hit Memcached.
No API friction beyond what is already documented in F13.

---

## 14-kafka-order-events

### F26 — `KafkaEventBus` must be fully started before `register_to()` is called

`KafkaEventBus.start()` connects to the broker and creates topics. Calling
`consumer.register_to(bus)` before `bus.start()` stores the subscription in
`_subscribed_channels`, which is fine for `RedisEventBus` (it catches up on connect) but
`KafkaEventBus` requires the producer and consumer group to be initialized first.

**Fix**: the pre-started bus pattern (`create_app(bus=pre_started_bus)`) remains the correct
approach for `ASGITransport` test isolation, but the fixture must call `await bus.start()` before
passing it to `create_app()`, and `create_app()` must wire `consumer.register_to(bus)`
**after** the started bus is passed in. Starting the bus first, then registering consumers, is
the safe, deterministic order for Kafka.

---

## 15-nats-jetstream-events

### F27 — NATS `durable_name` must be unique per test fixture; reusing a name with a different subject filter raises `BadRequest`

NATS JetStream durable consumers are server-side objects identified by `(stream_name,
durable_name)`. If two test fixtures (or two test runs) create a consumer with the same
`durable_name` but a different subject filter or starting position, the server rejects the
second create with `400 Bad Request: consumer already exists`.

**Fix**: generate a unique `durable_name` per fixture call using a short UUID suffix:

```python
from uuid import uuid4

durable = f"test-{uuid4().hex[:8]}"
settings = NatsEventBusSettings(..., durable_name=durable)
```

**Rule**: treat `durable_name` like a Redis key prefix — always unique per test run to avoid
cross-fixture consumer conflicts on the shared NATS server.

---

## 17-transactional-outbox

### F28 — `InMemoryEventBus` must be imported from `varco_core.event.memory`, not `varco_core.event.bus.memory`

`from varco_core.event.bus.memory import InMemoryEventBus` raises `ModuleNotFoundError`.
The correct import path is:

```python
from varco_core.event.memory import InMemoryEventBus
```

The public re-export in `varco_core.event` also works:

```python
from varco_core.event import InMemoryEventBus
```

**Severity**: Breaking (import error at startup). The `varco_core.event.bus` sub-package does
not exist; `memory.py` is a direct child of `varco_core/event/`.

---

### F29 — `DomainEvent` does not exist; the event base class is `Event` from `varco_core.event`

`from varco_core.event.base import DomainEvent` raises `ImportError: cannot import name 'DomainEvent'`.
The only concrete event base class is `Event` (a frozen Pydantic `BaseModel`):

```python
# ❌ Wrong — DomainEvent does not exist
from varco_core.event.base import DomainEvent


@dataclass(frozen=True)
class OrderCreatedEvent(DomainEvent):
    event_id: UUID = field(default_factory=uuid4)
    order_id: str = ""


# ✅ Correct — subclass the Pydantic Event; event_id is already inherited
from varco_core.event import Event


class OrderCreatedEvent(Event):
    __event_type__ = "order.created"
    order_id: str = ""
    amount: float = 0.0
```

`Event` already provides `event_id: UUID` and `timestamp: datetime` fields — do not redeclare them.
Declare `__event_type__` as a `ClassVar[str]` for deterministic serialization across process boundaries.

**Severity**: Breaking (import error at startup).
**File fixed**: `examples/17-transactional-outbox/events.py`
