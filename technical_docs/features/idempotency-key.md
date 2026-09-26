# Idempotency-Key middleware — `varco_core.idempotency` + `varco_fastapi.middleware.idempotency`

Plan 029 (BACKLOG 3.1, row **D1** must/S). Closes: "a retried `POST`/`PATCH` executes twice
because the caller could not tell whether its first attempt actually reached the server."

## The seam: contract in `varco_core`, HTTP adapter in `varco_fastapi`

Same rule CLAUDE.md already states three times over (`varco_fastapi.tenancy` → only
`varco_core.tenancy`; migrations → only `varco_core.migration`; TLS trust → only
`varco_core.tls`, never the reverse): `varco_core.idempotency` holds the ABC
(`AbstractIdempotencyStore`), the value object (`IdempotencyRecord`), the fingerprint function
(`compute_fingerprint`), the settings (`IdempotencySettings`), and the single-process default
(`InMemoryIdempotencyStore`) — all framework-agnostic, zero FastAPI/Starlette import.
`IdempotencyMiddleware` (the only thing that knows about HTTP headers/status codes/streaming
responses) lives in `varco_fastapi.middleware.idempotency`.

⚠️ **Not the same thing as `varco_core.service.inbox`.** The inbox pattern persists an *incoming
event* before a handler runs so a poller can re-deliver it after a crash (bus → handler gap), and
deletes the entry once processed. This ABC persists an **outgoing response** to suppress
re-execution of a retried HTTP request, and the record must *survive* processing for its full
TTL — retention is the entire point, the opposite of the inbox's delete-when-done lifecycle.

## Why `reserve()` is the one atomic primitive

`AsyncCache` (`varco_core.cache.base`) exposes `get`/`set`/`delete`/`exists`/`clear`/
`delete_prefix` and **no atomic set-if-absent**. `exists()` then `set()` is a race: two
concurrent retries can both observe "absent" and both execute. CLAUDE.md's decision tree
forecloses the obvious fix — adding an `add()` to `AsyncCache` is the exact shape of change the
`BulkCache`-over-`AsyncCache` rule (Plan 011 D-11) already forbids, because it would break
`isinstance()` for every out-of-tree cache backend.

So atomicity is pushed **up** into a new ABC instead:

```python
class AbstractIdempotencyStore(ABC):
    async def reserve(self, key: str, fingerprint: str, *, ttl: float) -> ReserveOutcome: ...
    async def complete(self, key: str, record: IdempotencyRecord) -> None: ...
    async def get(self, key: str) -> IdempotencyRecord | None: ...
    async def release(self, key: str) -> None: ...
    async def delete_expired(self) -> int: ...
```

`reserve()` returns one of three outcomes:

| Outcome | Meaning | HTTP result |
|---|---|---|
| `ACQUIRED` | First caller for this key | Execute, then `complete()` or `release()` |
| `IN_FLIGHT` | A reservation exists, not yet completed | **409 Conflict** (`Retry-After: 1`) |
| `REPLAY` | A completed record exists | Fingerprint matches → replay; differs → **422** |

Every implementation uses its backend's own native atomic primitive — never `exists()` + `set()`:

| Implementation | Package | Atomic primitive |
|---|---|---|
| `InMemoryIdempotencyStore` | `varco_core` | A lazily-created `asyncio.Lock` (never at module scope/`__init__`) |
| `RedisIdempotencyStore` | `varco_redis` | `SET key value NX PX ttl` |
| `SAIdempotencyStore` | `varco_sa` | `INSERT` against a `UNIQUE(key)` primary key, catching `IntegrityError` |
| `BeanieIdempotencyStore` | `varco_beanie` | A unique index + catching `DuplicateKeyError` |

A shared conformance suite (`testkit/varco_conformance/idempotency_store.py`,
`IdempotencyStoreConformance`) runs the identical behavioural contract — including a genuine
`asyncio.gather()` concurrency race asserting exactly one `ACQUIRED` — against all four.

## Fingerprint — what is bound, and what a mismatch means

```
fingerprint = sha256(method + "\n" + raw_path + "\n" + sorted_query + "\n" + sha256(body))
```

The raw body bytes are hashed directly, never parsed and re-serialized, so JSON key ordering or
incidental whitespace cannot produce a spurious mismatch in either direction. Query parameters
are sorted before hashing, so reordering them never triggers a false mismatch either.

A completed record with a **different** fingerprint for the same key returns
**422 Unprocessable Content** (`IdempotencyFingerprintMismatchError`) — Stripe's behaviour for a
reused idempotency key with a different payload, which the (expired) IETF draft defers to.

## Response replay — what is stored, what is never replayed

Stored: status code, raw body bytes (byte-for-byte, never re-serialized), and a filtered header
subset. Always replayed: `Content-Type`, `Location`, `Content-Language`, plus anything in the
configurable `replay_header_allowlist`. **Never replayed, hard-coded**: `Date`, `Set-Cookie`,
`Cache-Control`, `Age`, `Expires`, `ETag`, `Server`, `Content-Length` (recomputed), and every
varco-generated correlation header (`X-Request-ID`, `X-Correlation-ID`) — replaying a correlation
id would make two distinct requests indistinguishable in the trace backend, which is worse than
losing the header.

**Streaming responses are never captured.** The (expired) draft is silent on this, and buffering
an arbitrary `StreamingResponse` to store it is an unbounded-memory hazard. A streaming response
passes through untouched and the reservation is released, so a retry re-executes. The same
release-and-pass-through path runs for any response over `max_stored_body_bytes` (default 1 MiB).
Both cases are detected via `Content-Length` header presence/value — see the middleware's own
DESIGN block for why that, rather than a response-type check, is the correct signal.

## Storage key scoping — tenant/subject, and failing closed

```
idempotency:{tenant}:{subject}:{key}
```

`tenant` comes from `current_tenant()` (never `RequestContext` — CLAUDE.md's rule, the single
source of truth). `subject` comes from the ambient `AuthContext.user_id` when one exists. When
tenancy is disabled *and* there is no ambient subject (the common case — no multitenancy, no auth
middleware), the key is the **bare, unprefixed** value — no point namespacing by a literal `"-"`
placeholder that carries no information.

⚠️ **Fails closed when tenancy is enabled.** If `tenancy_enabled=True` and `current_tenant()` is
unset, the middleware raises `RuntimeError` rather than falling back to an unscoped key — the
same rule and reasoning as `tenancy_cache_key()`/`localization_cache_key()`. A cross-tenant
idempotency collision would replay one tenant's response to another tenant, which is a data leak,
not a cache miss.

## Middleware placement is a correctness requirement

```python
install_middleware_stack(app, [
    ErrorMiddleware,                                  # outermost
    (RequestContextMiddleware, {"server_auth": auth}),
    (IdempotencyMiddleware, {"store": store}),         # innermost of these three
])
```

It must sit **inside** `ErrorMiddleware` so its 409/422/400 render through the normal RFC-9457
error path, and **inside** `RequestContextMiddleware` so `current_tenant()`/the auth subject are
populated before the scoping logic above reads them. This is asserted by
`varco_fastapi/tests/test_idempotency_middleware.py::test_middleware_sits_inside_error_and_request_context_middlewares`.

## Selecting an idempotency store via env (Plan 047)

`VARCO_IDEMPOTENCY_BACKEND=<redis|sa|beanie>` selects the live `AbstractIdempotencyStore` —
all three are *new* DI bindings as of Plan 047 (`RedisIdempotencyStore`/`SAIdempotencyStore`/
`BeanieIdempotencyStore` previously carried no binding at all; the middleware always needed an
explicit `store=`). Unset means `InMemoryIdempotencyStore` (now resolvable by default, `qualifier=
"in_memory"`). Pass `container.get(AbstractIdempotencyStore)` to `IdempotencyMiddleware(store=…)`.
See `technical_docs/features/dependency-injection.md`'s "Selecting a backend" section.

## Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| `InMemoryIdempotencyStore` used behind a load balancer | Each process/pod sees its own reservations — a retry routed to a different instance never sees the first instance's in-flight reservation | Use `RedisIdempotencyStore`/`SAIdempotencyStore`/`BeanieIdempotencyStore` for any multi-process deployment |
| A large streaming download seems to "skip" idempotency protection | Deliberate — buffering an arbitrary stream to capture it is an unbounded-memory hazard (§D-D1-replay) | Documented, not a bug — a retry of a streaming route always re-executes |
| Claiming RFC conformance in docs/marketing | `draft-ietf-httpapi-idempotency-key-header-07` **expired 2026-04-18** and was never published as an RFC | Describe this feature as implementing the draft plus Stripe's de-facto practice, never "RFC 9xxx-compliant" |
| Registering `IdempotencyMiddleware` outside `ErrorMiddleware`/`RequestContextMiddleware` | 409/422/400 leak as raw 500s (uncaught `ServiceException`), or `current_tenant()`/auth subject are unavailable when the key is scoped | Always install via `install_middleware_stack` in the order shown above |
| Expecting a `Retry-After` computed from the real remaining TTL | `AbstractIdempotencyStore.reserve()` returns only a `ReserveOutcome` enum member — no TTL round-trips back to the caller (§D-D1-atomic keeps the return type minimal) | `Retry-After` is a fixed, conservative `1` second — treat it as a hint, not a guarantee |

## See also

- README's ["Idempotency-Key middleware"](https://github.com/edoardoscarpaci/varco/blob/main/README.md#idempotency-key-middleware) section for
  a runnable usage snippet.
- `design/research/005-idempotency-webhooks-and-cloudevents.md` §1 — the research brief this
  feature implements.
- `plans/029-idempotency-key-and-mcp-v2.md` — the design plan (§D-D1-home, §D-D1-atomic,
  §D-D1-fingerprint, §D-D1-replay, §D-D1-scope, §D-D1-optin, §D-D1-ttl).
