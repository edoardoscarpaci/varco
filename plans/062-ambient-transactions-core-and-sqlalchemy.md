# Plan 062 — Ambient transactions: one `@transactional` boundary (propagation, isolation, whole-boundary retry, post-commit hooks) — `varco_core` + `varco_sa` + the `varco_fastapi` request boundary (ACID-1, slice 1 of 2)

Covers BACKLOG 4.0.0 row **`ACID-1`** (`BACKLOG.md:59` — *"Ambient transactions — `@transactional`
boundary (per request / use case) that service calls, outbox and audit writes join;
`REQUIRED`/`REQUIRES_NEW` propagation; per-boundary isolation level; whole-transaction retry on
Postgres 40001/40P01 and Mongo `TransientTransactionError`"*) under the locked decision
`BACKLOG.md:33` (*"One `@transactional(isolation=…, retry=…)` boundary, designed once — retry must
re-run the whole transaction"*) and the release-vehicle decision `BACKLOG.md:31` (ships inside
4.0.0; breaking allowed, with a `technical_docs/migrations/3-to-4.md` section).

**This plan designs the whole public API once** and implements it for `varco_core` (API, ambient
context, backend seam, in-memory resource, service-layer semantics, conformance suite), `varco_sa`
(the SQLAlchemy/Postgres backend) and `varco_fastapi` (the opt-in request boundary). **Slice 063**
implements the same seam for `varco_beanie`/Mongo (PyMongo `AsyncMongoClient` sessions;
`BeanieUnitOfWork` is `transactional=False` today, `varco_beanie/uow.py:49-90`) — **no Mongo code
here**; §D-seam is written so 063 only implements it.

**Plan 061** (optimistic concurrency, `plans/061-optimistic-concurrency-and-conditional-requests.md`)
defines `varco_core.exception.OPTIMISTIC_LOCK_ERRORS = (StaleEntityError, ConcurrencyConflictError)`
(061 §D-exceptions, `:190-221`), adds `expected_version=` to `AsyncService.update/patch/delete` and
`SoftDeleteService.delete` (061 §D-api, `:160-182`), and adds `_NEVER_STORE_STATUSES` to
`IdempotencyMiddleware` (061 Step `:463`). This plan **imports `OPTIMISTIC_LOCK_ERRORS` by name and
never redefines it** — *an exception in it is never retried and never classified transient*
(§D-retry); 061's keyword, its `_check_expected_version()` call and its `StaleEntityError`
translation are preserved untouched by every edit here (§D-service); 503 is *added to* 061's
`_NEVER_STORE_STATUSES` rather than a second capture rule (Step 20). **061 lands first** (Step
0(d) is a hard gate).

**Plan 067** (streaming reads, `plans/067-streaming-reads-and-keyset-pagination.md`) builds NDJSON
export over `AsyncService.stream()`: the handler *primes* the iterator (first `__anext__` inside the
handler, 067 `:228-231`), returns an `NdjsonResponse`, and the remaining rows are sent **after the
handler returns**, under `aclosing()` (067 `:234-236,393-396`); its §D-neighbours hands this plan
the constraint *"a request-scoped transaction boundary that ends at handler return must not be
joined by `stream()`"* (067 `:436-441`). This plan honours it: **`stream()` never joins an ambient
boundary — it always runs on its own detached UoW** (§D-stream). 067 also fixes
`varco_sa/repository.py:392` (`async with` on the coroutine `stream_scalars()`, 067 `:33-35`) — this
plan plans around nothing at that line, and the streaming sub-tests (Steps 13(j), 16) require 067
landed (Step 0(i)). **069** (job orchestration) may later run job handlers inside a boundary — no
work here.

**Sources of record read (2026-10-07 → 2026-10-10):** `CLAUDE.md` (layer rules, DI rules, test
conventions, snapshot gates); `BACKLOG.md:27-63,184,190-192`; `plans/058` (house format);
`plans/061` (`:14`, `:58-65`, `:160-221`, `:458-467`); `plans/067` (`:13`, `:33-35`, `:95`, `:110-113`,
`:228-241`, `:386-404`, `:430-441`); `varco_core/varco_core/uow.py:13-60`; `service/base.py:209-256`
(`IUoWProvider`), `:291-365` (`AsyncService.__init__`), `:369-402` (`_emit`), `:530-640` (`_after_*`
hooks + their DESIGN blocks), `:740-1294` (every CRUD method), `:1319-1345`
(`_publish_domain_event`); `service/soft_delete.py:209-318`; `service/bulk.py:116-272` (grep);
`service/audit.py:586-693`; `service/outbox.py:1-120`; `cache/mixin.py:326-526`;
`event/producer.py` (whole); `context/ambient.py` (whole); `resilience/retry.py:70-315`;
`exception/service.py:26-236`; `exception/idempotency.py` (the `register_error_code` precedent);
`tenancy/provider.py`, `tenancy/global_scope.py` (whole); `providers.py:39-148`;
`varco_sa/provider.py:80-221`; `varco_sa/uow.py` (whole); `varco_sa/sqlalchemy_session.py` (whole);
`varco_sa/outbox.py:1-110,228-317`; `varco_sa/tenancy/rls_session.py` (grep: `after_begin` at
`:336`); `varco_fastapi/middleware/slots.py:33-100`; `modules/base.py:38-60`; `app.py:20-41,470-494`;
`router/endpoint.py:44-163`; `router/base.py:526-810`; `router/introspection.py:43-117`;
`middleware/error.py:322-373`; `middleware/idempotency.py:371-430`;
`technical_docs/migrations/3-to-4.md` (section list). Research:
[021 §A](../design/research/021-transactions-conditional-requests-streaming.md) (lines 32-63, 151-194),
[018 §A](../design/research/018-transactionally-correct-endpoints-and-gdpr-frameworks.md) (lines 11-44).

> ⚠️ **Eleven premises checked against source before anything was decided.**
> (1) **The outbox is not wired into `AsyncService`.** Services publish through the producer
> *after* commit (`service/base.py:1116-1131`); an outbox row is written by hand —
> `SAOutboxRepository(uow.session)` inside the caller's UoW (`varco_sa/outbox.py:40-51,259-270`).
> "Outbox rows join the transaction" therefore means *the joined UoW's `.session` is the
> boundary's session* — no automatic outbox producer exists, and this plan does not add one
> (§D-outbox-audit). (2) **`varco_sa/sqlalchemy_session.py` is an orphaned, half-built ambient
> session** (`current_session` ContextVar, `SessionContext`, `with_session`) imported by no package
> code — only `varco_sa/tests/test_sa_session.py`. Two ambient-session mechanisms would be one too
> many: it is deprecated here (Step 14). (3) **`paged_list` runs `list()` and `count()` under
> `asyncio.gather`** (`service/base.py:941`) — two tasks on one `AsyncSession` once both join a
> boundary. (4) **`CacheServiceMixin` invalidates right after `super().create()` returns and
> populates on a miss** (`cache/mixin.py:470-478,374-375`) — inside a boundary that is
> invalidate-before-commit and cache-uncommitted-data. (5) **`SQLAlchemyUnitOfWork.commit()` never
> closes its session** (`varco_sa/uow.py:51-53`; only `rollback()` closes) — the joined path must
> own closing explicitly. (6) **`varco_sa/repository.py` translates no DB error into a transient
> class** (061 adds only `StaleDataError` → `StaleEntityError`) — SQLSTATE classification is new
> code. (7) **Exactly 12 `make_uow()` call sites in the service layer**:
> `service/base.py:769,825,887,995,1065,1101,1166,1262` (get, list, count, exists, stream, create,
> update, delete), `soft_delete.py:248,301`, `bulk.py:169,238`; `TenantAwareService`, the
> validators, `AuditLogMixin` and `CacheServiceMixin` call none (line numbers pre-061/067; Step 0(a)
> re-measures). (8) **`IdempotencyMiddleware` captures every status code**
> (`middleware/idempotency.py:384-430` — the only skips are streaming and over-ceiling; 061 adds
> 412/428 to a never-store set) — an exhausted-retry response would otherwise be replayed for its
> key forever. (9) **`ErrorMiddleware` already duck-types `retry_after_seconds`** into a
> `Retry-After` header (`middleware/error.py:327`) — a new exception gets the header for free by
> carrying that attribute. (10) **The RLS tenant GUC is set by an `after_begin` listener reading
> `current_tenant()` at BEGIN** (`varco_sa/tenancy/rls_session.py:5-70,336`) — a joined transaction
> keeps the *first* tenant's GUC for its whole life, so cross-tenant joining must be impossible,
> not merely discouraged. (11) **`stream()`'s UoW outlives the code that starts it.** It is an async
> generator whose `authorize()`, `_scoped_params()` and `make_uow()` all run on the first
> `__anext__` (067 F1, `:110`); 067's export handler primes that first `__anext__` *inside* the
> handler and Starlette sends the rest after the handler returns — and Starlette's pre-2.4-spec
> `StreamingResponse` iterates the body in a task-group child task, not the handler's task (067
> `:234-236`). A joined `stream()` would therefore be primed inside the boundary, then read rows on a
> session the boundary already committed and closed, from a task §D-tasks forbids.

## Goal

After this plan: **`varco_core.transaction`** ships one ambient transaction boundary in three
shapes — `@transactional(...)` (decorator, retryable), `async with transaction(...)` (context
manager, never retried), `await run_in_transaction(options, fn, ...)` (functional, retryable) —
with `REQUIRED`/`REQUIRES_NEW`/`NESTED`/`MANDATORY` propagation, a per-boundary `IsolationLevel`,
a `read_only` hint, opt-in **whole-boundary retry** on backend-classified transient failures
(Postgres 40001/40P01 here; Mongo's labels in 063) reusing `RetryPolicy` and excluding 061's
`OPTIMISTIC_LOCK_ERRORS`, and `on_commit()` hooks that run only after the **outermost** commit.
Every `AsyncService` CRUD call (and `SoftDeleteService`, `BulkServiceMixin`) **joins** an ambient
boundary when one is active and behaves byte-identically to today when none is — **except
`stream()`, which always runs on its own detached UoW** so 067's NDJSON export keeps working under a
request boundary; domain events, `_emit()` and every `_after_*` hook are deferred to after the
outermost commit; `CacheServiceMixin` never caches uncommitted data. **`varco_sa`**'s
`SQLAlchemyRepositoryProvider` implements the backend seam (lazy enlistment, isolation applied
before BEGIN, `SAVEPOINT` for `NESTED`, SQLSTATE classification), proven on real Postgres by a
serialization-failure retry test, a two-service atomic-rollback test and the 061 retry-then-412
handoff test. **`varco_fastapi`** offers the per-request boundary as a route/router option plus an
app-wide `TransactionModule` (Django `ATOMIC_REQUESTS`-like), never a `create_varco_app` keyword
and never a middleware. A shared conformance suite (`testkit/varco_conformance/transaction.py`)
pins the seam so 063 subclasses it. Docs: new `technical_docs/features/transactions.md`, README,
ARCHITECTURE, CLAUDE.md, CHANGELOG, a `3-to-4.md` section; every snapshot gate green.

## Non-goals

- **Any Mongo/Beanie implementation** — 063. The seam (§D-seam) carries the Mongo-specific notes
  063 needs (commit-only retry on `UnknownTransactionCommitResult`, no savepoints).
- **Two-phase commit / multi-resource atomicity.** One boundary = one transactional resource; a
  second distinct resource raises (§D-tenancy). No XA, no "best-effort" ordered double commit.
- **An outbox-backed producer** (`OutboxEventProducer` routing `_produce()` into the ambient
  transaction's outbox) — the thing that would make domain + audit events *atomic* with data.
  Parked with a trigger (§Parked); this plan makes audit/events *post-commit* instead.
- **Anything 061 owns**: `expected_version`, `OPTIMISTIC_LOCK_ERRORS`, `StaleDataError` →
  `StaleEntityError` translation, ETag/`If-Match`, 412/428 never-store. Referenced, never redefined.
- **Anything 067 owns**: `NdjsonResponse`, priming, `aclosing()`, `yield_per`/batch sizes, the
  `stream_scalars` fix at `varco_sa/repository.py:392`, export ceilings. This plan only fixes how
  `stream()` relates to an ambient boundary (§D-stream).
- **A snapshot-consistent export across several statements** (e.g. export + count in one
  REPEATABLE READ snapshot). A single streamed SELECT already has a statement snapshot under READ
  COMMITTED (067 `:440`); a multi-statement snapshot export would need a boundary that lives as
  long as the response body, which §D-stream rejects. Parked.
- **Retrying `23505` (unique_violation) under `SERIALIZABLE`** — research 021:55 notes Postgres
  advises it, but classifying it transient would retry genuine duplicates N times before surfacing
  them. Parked.
- **`SUPPORTS`/`NOT_SUPPORTED`/`NEVER` propagation** — rejected (§D-propagation); the one real
  "run outside" need is served per-UoW by `open_detached_uow()`.
- **An ASGI middleware boundary / an `EdgeSlot`** — rejected (§D-http).
- **Job handlers inside a boundary** (069/`ORCH-1`).
- **Translating `IntegrityError` → `ServiceConflictError`** in `varco_sa` — a separate gap.
- **Fixing `SQLAlchemyUnitOfWork.commit()`'s missing `close()`** on the *standalone* path
  (premise 5) — noted in §Risks; the joined path closes explicitly.
- **Editing `BACKLOG.md`** — the coordinator rolls it up.
- **Settings/env vars.** No `TransactionSettings`, no `VARCO_TX_*` variable: a decorator evaluates
  at import time, before any container exists, and a request-boundary default is a
  `TransactionModule(...)` argument (code-configured, greppable).

## Design

### §D-api — the public API (what 063 and every app codes against)

```python
# varco_core.transaction (names marked * are also re-exported from top-level varco_core)
class Propagation(StrEnum):            # *
    REQUIRED      # join the ambient boundary, or open one (default)
    REQUIRES_NEW  # suspend the ambient boundary, open an independent one, restore it after
    NESTED        # SAVEPOINT inside the ambient boundary; behaves as REQUIRED when there is none
    MANDATORY     # join; TransactionConfigurationError when there is no ambient boundary

class IsolationLevel(StrEnum):         # *  values are the SQL spellings SQLAlchemy accepts
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"

@dataclass(frozen=True)
class TransactionOptions:              # *
    propagation: Propagation = Propagation.REQUIRED
    isolation: IsolationLevel | None = None   # None = backend default (root) / inherit (join)
    read_only: bool = False                   # a hint the backend enforces where it can
    retry: RetryPolicy | None = None          # honoured only by a boundary that OWNS a transaction

TRANSACTION_RETRY: Final[RetryPolicy]  # * max_attempts=3, base_delay=0.05, max_delay=1.0,
                                       #   jitter=True, retryable_on=(TransientTransactionError,)

def transaction(*, propagation=..., isolation=None, read_only=False
                ) -> AbstractAsyncContextManager[TransactionContext]        # * (no retry=)
def transactional(fn=None, /, *, propagation=..., isolation=None, read_only=False,
                  retry: RetryPolicy | None = None)                          # * decorator
async def run_in_transaction(options: TransactionOptions, fn, /, *args, **kwargs)  # *
def current_transaction() -> TransactionContext | None                     # *
async def on_commit(callback: Callable[[], Awaitable[None] | None]) -> None  # *
def open_detached_uow(provider: IUoWProvider) -> AsyncUnitOfWork          # never joins (§D-stream)

class TransactionContext:              # * read-only view of the current frame
    options: TransactionOptions        # effective (root's isolation/read_only when joined)
    kind: FrameKind                    # ROOT | JOINED | NESTED
    attempt: int                       # 0-based attempt of the owning root
    depth: int
    tenant_id: str | None              # fixed at first enlistment
    is_rollback_only: bool
    state: MutableMapping[object, Any] # root-scoped transaction-local storage
    def set_rollback_only(self) -> None
    def on_commit(self, callback) -> None

# exceptions — varco_core.exception.transaction (all four re-exported top-level)
class TransientTransactionError(ServiceException)   # 503 + Retry-After, VARCO_TX_001;
                                                    # NOT a ServiceConflictError, NOT in OPTIMISTIC_LOCK_ERRORS
class TransactionConfigurationError(RuntimeError)   # misuse: names the rule and the remedy
class TransactionRolledBackError(RuntimeError)      # clean body, but the unit was marked rollback-only
class TransactionClosedError(RuntimeError)          # a joined UoW used after its boundary ended

# backend seam — varco_core.transaction only (not top-level; backend-author API, §D-seam)
TransactionalResource, ResourceTransaction, Savepoint, TransactionCapabilities,
JoinedUnitOfWork, AmbientMode, FrameKind, classify_chain
# test double — varco_core.transaction.memory
InMemoryTransactionalResource
```

`varco_sa`: `SQLAlchemyRepositoryProvider` *is* a `TransactionalResource` (no new public name a
user must import). `varco_fastapi`: `TransactionModule`, `RequestTransactionPolicy`,
`@route(..., transaction=...)`, `VarcoRouter._transaction`. **No service signature changes** —
061's `expected_version=` keyword on `update/patch/delete` is independent and untouched, and
`stream()`'s signature is unchanged.

```text
DESIGN: three shapes — decorator, context manager, functional — with retry only where re-runnable
  ✅ A `with` block cannot re-execute its own body, so `transaction()` simply has no `retry=`
     parameter: retrying a context-manager boundary is unrepresentable, not a runtime error.
  ✅ `@transactional` matches Spring's `@Transactional` muscle memory (018 §A:14) and is the
     natural shape for a service/use-case method; `run_in_transaction(options, fn, ...)` is what
     the FastAPI wrapper and any dynamic caller use (options first, positional-only — never
     collides with the callee's own keyword arguments, e.g. 061's `expected_version=`).
  ✅ One options value (`TransactionOptions`, frozen) flows through all three and the route option.
  ❌ Three entry points to document instead of one. Mitigated: one table in transactions.md, and
     the decorator/functional forms are thin wrappers over the same owner loop (§D-model).
```

### §D-model — lazy enlistment: frames, one root, one resource

```text
 transaction()/@transactional ──► Frame (ROOT | JOINED | NESTED) pushed on AmbientVar
                                    │
 provider.make_uow() (sync) ────────┤ current_transaction() is not None?
   ├─ no  → standalone UoW (today's behaviour, byte-identical)
   └─ yes → JoinedUnitOfWork(frame, resource)
               │ __aenter__ (async)
               ▼
        root._enlist(resource):  task check → tenant check → resource-key check
           first time: rtx = await resource.open_transaction(root options)   ← BEGIN (isolation
                       then create every pending NESTED savepoint, outermost first   set before it)
           later:      reuse rtx
               │ __aexit__: clean → rtx.flush()   exception → maybe mark unit rollback-only (§D-failure)
               ▼
 ROOT exit (owner loop, per attempt):
   body raised     → rtx.rollback(); hooks discarded; transient = rtx.classify(exc)
                     → retry (sleep policy.compute_delay(i)) or re-raise (transient from exc)
   body clean      → rollback-only? rtx.rollback(); raise TransactionRolledBackError
                     else close still-open joined UoWs (§D-stream); rtx.commit()
                          commit raised → classify → retry or re-raise (as above)
   finally         → rtx.close(); AmbientVar reset (outer frame restored, or None)
   after success   → run on_commit hooks in order, with NO ambient transaction (§D-postcommit)

 open_detached_uow(provider) ─► make_uow() called with the ambient var cleared (sync) →
                                standalone UoW marked DETACHED: own session, own transaction,
                                no task/tenant/key checks — exactly today's UoW
```

`_CURRENT = AmbientVar[TransactionContext]("varco.transaction")` at module scope —
`context/ambient.py`'s own DESIGN block is why module-scope `ContextVar` construction is correct.
A boundary that never enlists a resource opens **no connection** (Django's per-request overhead
caveat, 018 §A:21, does not apply to requests that never touch the DB) and still runs its
`on_commit` hooks at exit.

```text
DESIGN: lazy enlistment inside the provider's make_uow(), not an eager "begin on entry"
  ✅ The boundary needs no knowledge of which backend it will touch — the decorator works on a
     use-case method and on a FastAPI route without naming a provider or engine.
  ✅ Every make_uow() caller joins automatically — services, a hand-written
     `SAOutboxRepository(uow.session)`, user code — because the join happens where the UoW is made.
  ✅ Wrappers (TenantUoWProvider, DynamicTenantUoWProvider) need no change: they delegate to a
     provider that joins.
  ✅ Isolation still lands before BEGIN: open_transaction() applies it on the first enlistment.
  ❌ Every resource-owning provider must implement the seam; one that does not would silently run
     standalone inside a boundary. Guarded loudly at the service layer (§D-service) and documented
     for direct make_uow() callers.
  Rejected: eager begin with `transactional(provider=...)` — couples every use-case method and
  route to a DI-known provider; opens a connection for requests that never touch the DB.
```

### §D-propagation — the set: `REQUIRED`, `REQUIRES_NEW`, `NESTED`, `MANDATORY`

| Propagation | Ambient boundary present | None present |
|---|---|---|
| `REQUIRED` | JOINED frame (same resource transaction) | new ROOT |
| `REQUIRES_NEW` | ambient suspended; new ROOT (own connection); outer restored after | new ROOT |
| `NESTED` | NESTED frame: `SAVEPOINT` at (lazy) enlistment; exception → `ROLLBACK TO SAVEPOINT`, hooks registered in the frame discarded, exception propagates; clean → `RELEASE`, hooks merged into the parent | new ROOT |
| `MANDATORY` | JOINED frame | `TransactionConfigurationError` |

```text
DESIGN: four values, NESTED included now
  ✅ REQUIRED/REQUIRES_NEW are the row's floor; MANDATORY costs three lines and lets a GDPR-erase
     step or a "must run inside the request transaction" helper assert its precondition.
  ✅ NESTED is the only way to recover from a failed statement inside a Postgres transaction
     (`current transaction is aborted` otherwise) — get-or-create on IntegrityError is impossible
     inside a request boundary without it. SQLAlchemy's `begin_nested()` is async-compatible
     (021 §A:47, :151); designing it now keeps 063's contract complete (Mongo declares
     `savepoints=False` → NESTED raises TransactionConfigurationError at enlistment).
  ❌ Lazy savepoint creation (a NESTED frame may be entered before the first enlistment) is the most
     intricate part of the core; covered by dedicated unit + Postgres tests.
  Rejected: SUPPORTS (indistinguishable from REQUIRED for a lazily-enlisting design),
  NOT_SUPPORTED (its one real use — run one UoW outside — is `open_detached_uow()`, used by
  `stream()` and GlobalUoWProvider), NEVER (no varco caller needs to assert absence). Each is purely
  additive later.
```

**Join rules** (all raise `TransactionConfigurationError` naming the rule and both values):
a JOINED/NESTED frame with an explicit `isolation` different from the root's effective isolation
(running a "serializable" method at read committed is a silent correctness bug — Spring's
`validateExistingTransaction`, made the default); `NESTED` on a resource without savepoints.
`read_only` on a joined frame is not validated (a write inside a read-only root fails in the
database — Postgres SQLSTATE 25006 — which is the honest error). `retry=` on a non-ROOT frame is
ignored and logged once at DEBUG — the owning root's policy governs.

**A boundary must not span a `yield`.** `transaction()` is a class-based async context manager whose
`__aenter__` inspects its caller frame (`sys._getframe(1).f_code.co_flags & inspect.CO_ASYNC_GENERATOR`)
and raises `TransactionConfigurationError` when entered directly inside an async-generator body:
an async generator has no context of its own, so a frame set before a `yield` would leak into the
consumer's code between iterations (the consumer's service calls would silently join it), and the
reset after the last `yield` may run in asyncio's finalizer context (`ValueError: token created in a
different Context`). `@transactional` on an async-generator function is already a `TypeError`.
Best-effort guard (a helper one frame deeper evades it) + a Pitfalls row.

### §D-isolation — per-boundary isolation level

`IsolationLevel | None` on the ROOT; `None` = whatever the engine/connection default is (Postgres:
READ COMMITTED). `READ_UNCOMMITTED` is deliberately absent (Postgres maps it to READ COMMITTED; no
varco use). The backend applies it **before BEGIN** on the connection that will run the
transaction (021 §A:36 — per-connection `execution_options(isolation_level=...)`, set before BEGIN;
SQLAlchemy resets it when the connection returns to the pool — ⚠️ ASSUMPTION, verified by Step
13(d)). A level the backend's `TransactionCapabilities.isolation_levels` does not contain → raise at
`open_transaction()`, never a silent downgrade.

### §D-retry — whole-boundary retry, and side effects inside it

Retry happens **only in a ROOT's owner loop** and re-runs **the whole body** (Postgres docs via 018
§A:38 and 021 §A:53 — never replay just the failed statement, never retry a savepoint). Decision per
attempt: `transient = rtx.classify(exc)` (backend-specific, §D-seam) → retried iff
`transient is not None and attempt + 1 < policy.max_attempts and policy.is_retryable(transient)
and not isinstance(exc, OPTIMISTIC_LOCK_ERRORS)` — 061's tuple, imported from
`varco_core.exception`, never copied; the delay is `policy.compute_delay(attempt)`. On exhaustion
the last `TransientTransactionError` (with `attempts=N`) is raised `from` the backend exception.
`asyncio.CancelledError`/`KeyboardInterrupt` are `BaseException` — rolled back, never classified,
never retried.

```text
DESIGN: retry is opt-in (`retry=None` default; TRANSACTION_RETRY is the named preset)
  ✅ The body is re-executed in full: a non-idempotent external call inside it (charge a card, send
     an email, POST to a partner) happens twice. Spring's own guidance is "retries are not safe
     without idempotency" (018 §A:15-16) — so the author of the body must say "this is pure DB".
  ✅ Without in-process retry the default path is still correct: the transient failure surfaces as
     TransientTransactionError → 503 + Retry-After, and a client retrying with an Idempotency-Key
     retries safely end-to-end.
  ✅ Post-commit work is per attempt: hooks registered by a rolled-back attempt are discarded, so
     deferring side effects to on_commit()/the outbox is the safe pattern and the docs say so.
  ✅ Reuses RetryPolicy wholesale (max_attempts/backoff/jitter/is_retryable) — no second retry model
     (same rule as Plan 005's job retry).
  ❌ READ COMMITTED (Postgres default) never raises 40001, but deadlocks (40P01) can happen at any
     level and are not retried unless the author opts in. Accepted; Open Question 2.
  ❌ `isolation=SERIALIZABLE` without `retry=` is legal and will surface 40001s to clients —
     documented as the first row of the Pitfalls table.

DESIGN: OPTIMISTIC_LOCK_ERRORS is never retried (Plan 061's tuple, referenced by name)
  ✅ A lost update must reach the client as 409/412 (061's ConcurrencyConflictError /
     PreconditionFailedError), never be "fixed" by re-running the body against data the client
     never saw. One `isinstance(exc, OPTIMISTIC_LOCK_ERRORS)` covers both the repository-level
     StaleEntityError (direct repo use inside @transactional) and the service-level translation
     (061 :218-219).
  ✅ Referencing the tuple (never a copy) means a member 061 adds later is excluded automatically.
  ✅ Classification order in every backend (§D-seam): an OPTIMISTIC_LOCK_ERRORS instance anywhere
     in the cause chain → None, checked before any SQLSTATE/label inspection — so the exclusion
     holds even if a backend wraps the conflict in a driver error.
  ✅ The 061 handoff case (061 :14) is correct by construction: under REPEATABLE READ/SERIALIZABLE
     Postgres reports a concurrent UPDATE as 40001 *before* the version predicate sees zero rows —
     that 40001 IS retried; the re-run reloads the row, re-evaluates `expected_version`, raises
     PreconditionFailedError (412), which is NOT retried. Step 13(k) proves it.
  ✅ TransientTransactionError is neither a ServiceConflictError nor in OPTIMISTIC_LOCK_ERRORS
     (asserted by a test) — its client action is "resubmit unchanged", not "re-GET".
  ❌ A user who wants optimistic-conflict retry must write it explicitly. Intended.
```

### §D-postcommit — where post-commit work runs

`on_commit(cb)`: no ambient boundary → `cb` runs now (awaited if it returns an awaitable — Django
semantics outside `atomic`); inside → appended to the current frame's list (NESTED frames merge
into the parent on `RELEASE`, discard on `ROLLBACK TO SAVEPOINT`; JOINED frames append to the
root). After the **outermost** (ROOT) commit, the owner runs the hooks **in registration order,
with the ambient var set to `None`**. Every hook runs even if an earlier one raised; failures are
logged at ERROR with traceback; after all ran, the **first** failure is re-raised unchanged with
the others attached via `add_note()`. Hooks never trigger a retry (the commit is durable).

```text
DESIGN: hooks run after the outermost commit, outside any boundary, all-run-then-raise-first
  ✅ "After the OUTERMOST commit" is Django on_commit (018 §A:20): an inner REQUIRED call's event
     can never announce data a later failure rolls back.
  ✅ REQUIRES_NEW roots run their own hooks after their own commit — the inner data is durable, so
     its events go out even if the outer later rolls back (Spring afterCommit semantics).
  ✅ No ambient transaction while hooks run: a hook calling a service opens a standalone UoW, and
     `_emit()` inside a REQUIRES_NEW root's hook publishes now instead of being re-deferred onto the
     suspended outer (which could roll back and lose an event for committed data).
  ✅ Re-raising the first failure unchanged keeps the single-service case byte-identical to today
     (an `_after_create` exception propagates after commit, `service/base.py:554-556`); running the
     rest first means one failing notification never suppresses the audit event.
  ❌ The caller gets an exception for a committed write — exactly as today; documented.
  ❌ Hooks run inline in the request: a slow hook delays the response. Same as today.
```

### §D-service — default behaviour: services always join when a boundary is present (except `stream()`)

`AsyncService` gains `_open_uow()` and all 12 call sites (premise 7) use it:

```python
def _open_uow(self, *, detached: bool = False) -> AsyncUnitOfWork:
    if detached:                                  # only stream() passes True (§D-stream)
        return open_detached_uow(self._uow_provider)
    uow = self._uow_provider.make_uow()
    if current_transaction() is not None and uow.ambient_mode is AmbientMode.STANDALONE:
        raise TransactionConfigurationError(...)  # "provider X cannot join an ambient
                                                  #  transaction — implement TransactionalResource"
    return uow
```

`AsyncUnitOfWork` (`varco_core/uow.py`) gains a plain class attribute
`ambient_mode: AmbientMode = AmbientMode.STANDALONE` (every existing UoW, including every test
fake, inherits it); `JoinedUnitOfWork` sets `JOINED`; `open_detached_uow()` (used by `stream()` and
`GlobalUoWProvider`) returns a `DETACHED` one. Mutations build their event **eagerly** (so
`correlation_id`/`tenant_id` are captured at call time) and defer only the publish + hook:

```python
# create(), after the `async with self._open_uow() as uow:` block
event = EntityCreatedEvent(..., correlation_id=current_correlation_id(), ...)
async def _post() -> None:
    await self._publish_domain_event(event)
    await self._after_create(saved, read_dto, ctx)
await on_commit(_post)          # runs now outside a boundary — byte-identical order
```

Same shape for `update`, `delete`, `BulkServiceMixin.create_many/delete_many` (one hook per
entity, preserving today's per-entity order), and `_emit()` (defers inside a boundary).
`paged_list` runs `list()` then `count()` **sequentially** when `current_transaction()` is not
None (gather otherwise, unchanged). `SoftDeleteService.delete/restore` only switch to
`_open_uow()`. `stream()` switches to `_open_uow(detached=True)` (§D-stream) — with no boundary
that is byte-identical to today. Direct `self._producer._produce(...)` calls are not intercepted —
documented ("use `_emit()`").

**061 coexistence (hard rule for Steps 6–7):** the edits touch only (a) the `make_uow()` call
expression, (b) the code *after* the UoW block (event build + `on_commit`), and (c) `paged_list`.
061's keyword-only `expected_version=` on `update/patch/delete` (and `SoftDeleteService.delete`),
its `_check_expected_version()` call inside the UoW, its `StaleEntityError` →
`ConcurrencyConflictError`/`PreconditionFailedError` translation, and `CacheServiceMixin`'s
forwarding of the keyword stay byte-identical. A 061 precondition failure raised inside the UoW
propagates before `on_commit` is reached — no event, no hook (061's own "no event published"
assertion keeps holding inside and outside a boundary). **067 coexistence:** in `stream()` only the
UoW-opening expression changes; 067's `yield_per`/`close()`/keyset logic inside the repositories and
`stream()`'s authorize/scope ordering are untouched.

```text
DESIGN: join-when-present, today's behaviour when absent; loud for providers that cannot join
  ✅ Zero behaviour change for any app that never opens a boundary — every existing test stays
     green unmodified (the Phase A regression test asserts the exact event/hook order outside one).
  ✅ "A two-service endpoint is two commits" (BACKLOG.md:59) is fixed by adding one decorator.
  ✅ A provider that cannot join fails at the first service call inside a boundary instead of
     silently committing per call — the failure mode this row exists to remove.
  ✅ No service signature changes — nothing collides with 061's `expected_version=`.
  ❌ `create()` returns BEFORE its events/hooks run when inside a boundary — a semantic change for
     code that reads its own side effects synchronously. Documented in 3-to-4.md and the
     `_after_*` docstrings ("after the outermost commit").
  ❌ Direct make_uow() calls on a non-seam provider inside a boundary are not guarded (no
     service-layer choke point). Documented.
```

### §D-failure — exceptions crossing a joined UoW, rollback-only, `TransactionRolledBackError`

A joined UoW exiting with an exception marks the **unit** (nearest NESTED frame, else root)
rollback-only **unless** the exception is a `ServiceException` **that is not in
`OPTIMISTIC_LOCK_ERRORS`**. `NotFound`/`Authorization`/`Validation`/business `Conflict` are domain
outcomes raised around healthy statements — a caller catching `ServiceNotFoundError` for
get-or-create must not doom the request. Everything else dooms the unit: driver errors (on
Postgres the transaction is aborted anyway), bugs, and every `OPTIMISTIC_LOCK_ERRORS` member —
`StaleEntityError` comes from a failed SA flush, after which the session must be rolled back
(SA `PendingRollbackError` otherwise), and 061 translates it into `ConcurrencyConflictError`
*inside* the UoW, so the translated form must doom too (conservative for a pre-write
`PreconditionFailedError`: a caller that swallows it and continues gets
`TransactionRolledBackError`, documented). `uow.commit()` on a joined UoW = flush only;
`uow.rollback()` = mark rollback-only. A ROOT whose body exits cleanly while rollback-only rolls
back and raises `TransactionRolledBackError` (the cause recorded on the mark is `__cause__`) —
Spring's `UnexpectedRollbackException`. A NESTED frame exiting with an exception rolls back to its
savepoint and clears its own mark (a stale write inside `NESTED` is recoverable).

### §D-tasks — a boundary belongs to the task that opened it

The ROOT records `asyncio.current_task()`. A joined-UoW `__aenter__`, an enlistment, or a
`TransactionContext.on_commit()` from **any other task** raises `TransactionConfigurationError`
("…a spawned task inherits the context copy but must not share the session; open its own boundary
or `await` sequentially"). This covers `asyncio.gather`/`TaskGroup` over service calls and a
background `create_task(service.create(...))` that outlives the request. (`stream()` is unaffected:
it never joins, so 067's body iteration in a Starlette child task is fine — §D-stream.)

```text
DESIGN: forbid cross-task joining (loud) over sharing (lock) or ignoring (silent)
  ✅ AsyncSession is not safe for concurrent use; a per-transaction lock would deadlock a
     legitimate same-task interleaving (a repository-level `stream_by_query` in a joined UoW with a
     `get()` inside the loop) unless made task-reentrant, and would still serialise silently.
  ✅ Treating a child task as "outside" would silently commit its work independently — partial
     atomicity, the exact bug the row fixes.
  ✅ ContextVar copy-on-spawn (context/ambient.py:37-41) means the child *sees* the frame; the
     owner check is what turns that into a clear error.
  ❌ `paged_list` must special-case the boundary (§D-service); user code using gather inside a
     boundary must be rewritten. Documented as a Pitfall.
```

### §D-tenancy — one tenant, one resource, GLOBAL detached

- **Cross-tenant:** the ROOT fixes `tenant_id = current_tenant()` at first enlistment; any later
  joined-UoW enter or enlistment under a different `current_tenant()` raises (premise 10: the RLS
  GUC, the `SCHEMA` `schema_translate_map`, and the `DATABASE` engine are all bound at BEGIN). Holds
  for all three `TenantIsolation` strategies.
- **One resource:** enlisting a second distinct `transaction_key` in one ROOT raises ("no two-phase
  commit: two resources cannot commit atomically; run the second in `REQUIRES_NEW` or outside the
  boundary"). Two providers on the *same* sessionmaker share one session (the key is the
  sessionmaker, §D-sa).
- **`TenantScope.GLOBAL`:** `GlobalUoWProvider.make_uow()` returns `open_detached_uow(delegate)`.
  Global reads/writes are **never** part of a tenant boundary — uniformly under SHARED, SCHEMA and
  DATABASE.
- **Detached UoWs and tenancy** (`stream()`, GLOBAL): a detached UoW BEGINs on its own connection,
  so the RLS `after_begin` listener reads `current_tenant()` at *its* BEGIN — for `stream()` that is
  the first `__anext__`, which 067 primes inside the handler, under the request's tenant context.

```text
DESIGN: GLOBAL never enlists (uniform) over enlist-if-same-resource (deployment-dependent)
  ✅ Code that works under SHARED behaves identically under SCHEMA/DATABASE, where the global
     store is a different engine and enlisting would raise the one-resource error on every request
     that reads a reference table.
  ✅ The app-facing global credential is read-only by default (RD-10, global_scope.py:13-16), so
     the common in-request global access is a read, for which atomicity is irrelevant.
  ❌ A global write inside a boundary (global_writable=True) is not atomic with the tenant write,
     and the request holds a second pooled connection. Both documented (Pitfalls + the
     connection-budget worksheet in multitenancy.md gets one line).
```

### §D-stream — `stream()` never joins: it always runs on its own detached UoW

**Chosen:** `AsyncService.stream()` opens its UoW with `_open_uow(detached=True)` — the provider's
`make_uow()` is called with the ambient var cleared and the result is a standalone UoW (own
session, own connection, own transaction, held for the iterator's lifetime and released by 067's
`aclosing()` on completion/break/disconnect, 067 `:393-396`). Inside or outside a boundary it
behaves exactly as `stream()` does today — no task binding, no tenant/resource-key checks, no
truncation. Inside a boundary it does **not** see the boundary's uncommitted writes and is not
rolled back with it (it only reads). One DEBUG log line per call when a boundary is active.

The escape hatch for "stream my own uncommitted rows, fully consumed before I return" is
repository-level: `async with self._open_uow() as uow: async for e in
self._get_repo(uow).stream_by_query(...)` — a *joined* UoW, task-bound, and if still open when the
ROOT exits it is closed with the session: further iteration raises `TransactionClosedError`
(the loud failure for misuse of the joined path).

```text
DESIGN: stream() gets its own UoW, always (067 §D-neighbours :436-441), over the alternatives
  ✅ 067's export primes stream() inside the handler and sends the rest after the handler returns
     (067 :228-236); a per-request boundary commits and closes its session at handler return
     (§D-http). A joined stream would read rows on a closed session. A detached stream lives exactly
     as long as the response body needs it — the constraint 067 hands to this plan, honoured.
  ✅ Starlette may iterate the body in a task-group child task (067 :234-236); a joined UoW would
     trip §D-tasks there. A detached UoW has no task binding.
  ✅ stream() cannot know who consumes it (export response, job, script), so the rule is uniform:
     one predictable behaviour instead of "joins unless an NdjsonResponse is involved".
  ✅ Consistency is not lost: a single streamed SELECT has its own statement snapshot under READ
     COMMITTED (067 :440); Mongo cursors are not snapshot-isolated either way (067's Pitfall).
  ✅ No ContextVar leakage: no boundary is ever opened inside the generator body (§D-propagation's
     "a boundary must not span a yield").
  ✅ Zero change outside a boundary; zero change to 067's code beyond the UoW-opening expression.
  ❌ Inside a boundary, stream() does not see the boundary's uncommitted writes and holds a second
     pooled connection while the body streams (Pitfalls + multitenancy.md worksheet).
  ❌ The boundary's isolation/read_only do not apply to the stream; a multi-statement snapshot
     export is parked (§Non-goals).
  Rejected — join while iterated inside the boundary, fail loudly (TransactionClosedError) once the
     owning boundary has closed: breaks 067's export under every route that opts into a boundary
     (`_transaction = True` on a router, a module policy covering GET), turning a valid
     configuration into a mid-body failure after `200` is already on the wire.
  Rejected — the request-boundary opt-in excludes streaming routes: a route cannot be classified
     reliably at build time (a custom @route may return an NdjsonResponse conditionally), and the
     non-HTTP callers (065's job export, scripts) would still hit the joined-then-closed problem.
  Rejected — detach only when used by an NdjsonResponse: needs a side channel from the response
     class into the service (an ambient flag or a new stream() keyword), i.e. the HTTP layer
     reaching into service transaction semantics; and other long-lived consumers stay broken.
  Rejected — extend the request boundary to response completion: commits after `http.response.start`
     (a client could see 200 for a rolled-back write — §D-http) and holds the write transaction for
     the whole download.
```

### §D-cache — `CacheServiceMixin` inside a boundary

Writes (`create/update/delete`) record their namespace in `tx.state[_CACHE_DIRTY]` and defer every
invalidation and `_publish_invalidated` through `on_commit()`. Reads (`get/list`) **bypass the
cache entirely** (no read, no populate) for a namespace dirtied in the current ROOT; other
namespaces behave as today. Outside a boundary: byte-identical. 061's `expected_version` forwarding
in `update/delete` is untouched.

```text
DESIGN: per-transaction dirty-namespace bypass over "bypass the cache inside any boundary"
  ✅ Read-your-own-writes inside the boundary; uncommitted data never enters a shared cache;
     invalidation can no longer run before the commit it describes.
  ✅ A per-request boundary (the TransactionModule path) does not disable caching for every
     read-only request.
  ❌ Populating a clean namespace inside a REPEATABLE_READ root may cache snapshot-old data — the
     same read-then-set race that exists today at READ COMMITTED, not a new one.
```

### §D-outbox-audit — what joins, what is post-commit

- **Outbox rows join.** `SAOutboxRepository(uow.session)` inside a joined UoW writes on the
  boundary's session → atomic with the entity, rolled back with it (Step 13(h) proves it).
- **Audit is post-commit, eventually consistent, and now commit-exact.** `AuditLogMixin`'s
  `_after_*` hooks are deferred with the rest, so an `AuditEvent` is produced only for mutations
  that committed (never for a rolled-back attempt or a request that failed later), and
  `AuditConsumer` persists it as today. *Atomic* audit (audit row in the same transaction) needs
  the parked `OutboxEventProducer` — Open Question 3.
- **Framework stores never join** — `SAIdempotencyStore`, `SADeadLetterQueue`, the audit/job/webhook
  repositories build sessions from their own sessionmaker, not through `make_uow()`. Intended: an
  idempotency reservation and a dead letter must persist even when the boundary rolls back.

### §D-seam — the backend contract (what 063 implements)

```python
@dataclass(frozen=True)
class TransactionCapabilities:
    isolation_levels: frozenset[IsolationLevel]
    savepoints: bool
    read_only: bool                     # False → the hint is accepted and ignored

class TransactionalResource(ABC):       # implemented by a RepositoryProvider
    @property
    @abstractmethod
    def transaction_key(self) -> Hashable: ...            # identity of the transactional resource
    @property
    @abstractmethod
    def transaction_capabilities(self) -> TransactionCapabilities: ...
    @abstractmethod
    async def open_transaction(self, options: TransactionOptions) -> ResourceTransaction: ...
        # BEGIN with isolation/read_only applied before the first statement; unsupported option
        # → TransactionConfigurationError (never a silent downgrade)

class ResourceTransaction(ABC):
    @property
    @abstractmethod
    def handle(self) -> Any: ...                          # AsyncSession / ClientSession
    async def flush(self) -> None: ...
    async def commit(self) -> None: ...   # may retry a commit-ONLY failure internally (Mongo)
    async def rollback(self) -> None: ...
    async def close(self) -> None: ...    # idempotent
    async def savepoint(self) -> Savepoint: ...           # only when capabilities.savepoints
    def classify(self, exc: BaseException) -> TransientTransactionError | None: ...
        # pure; implemented as classify_chain(exc, <backend matcher>)

class Savepoint(ABC):
    async def release(self) -> None: ...
    async def rollback(self) -> None: ...

class JoinedUnitOfWork(AsyncUnitOfWork):  # core implements _begin/commit/rollback/__aexit__
    ambient_mode = AmbientMode.JOINED
    def __init__(self, tx: TransactionContext, resource: TransactionalResource) -> None: ...
    @abstractmethod
    def _bind(self, rtx: ResourceTransaction) -> None: ...  # attach repos over rtx.handle

def classify_chain(exc, match: Callable[[BaseException], TransientTransactionError | None])
    # cycle-safe walk over __cause__/__context__; an OPTIMISTIC_LOCK_ERRORS instance anywhere in
    # the chain → None (checked first); otherwise the first non-None `match(e)`
```

A provider's `make_uow()` becomes: `tx = current_transaction(); return
<Backend>JoinedUnitOfWork(tx, self, ...) if tx is not None else <today's UoW>`. Detached UoWs need
nothing from the backend — `open_detached_uow()` just calls `make_uow()` with the ambient cleared.

**Notes for 063 (guidance, not decisions):** `transaction_key` = the `AsyncMongoClient`;
`capabilities(savepoints=False, ...)`; `classify()` maps the `TransientTransactionError` error
label → `TransientTransactionError(reason="transient")` via `classify_chain`; `commit()` retries
`UnknownTransactionCommitResult` **itself** (commit-only, per the driver's `with_transaction`
contract) and never surfaces it to the whole-body loop; a Beanie filtered `replace_one` stale write
is 061's `StaleEntityError` and therefore never transient; `stream()` on Beanie is detached like on
SA (no change needed — the core decides); how Mongo read/write concerns map onto `IsolationLevel`
is 063's call — an unmapped level must raise. Beanie operations need the session passed per call —
063's `_bind()` problem.

```text
DESIGN: the seam is "open / flush / commit / rollback / savepoint / classify" on an object the
provider returns, plus a capabilities record
  ✅ Exactly the operations the owner loop needs; nothing SQL- or Mongo-shaped leaks into core.
  ✅ classify() lives with the backend because only it can read SQLSTATEs or error labels; core
     only sees TransientTransactionError — so RetryPolicy.retryable_on works unchanged.
  ✅ The 061 exclusion lives in one shared helper (classify_chain), not re-implemented per backend.
  ✅ Capabilities make NESTED/isolation refusal uniform and testable by the conformance suite.
  ❌ A provider author implements three ABCs (resource, transaction, joined UoW). The in-memory
     implementation (~150 lines) is the worked example.
```

### §D-sa — the SQLAlchemy implementation (`varco_sa/transaction.py`)

- `transaction_key` = the provider's `async_sessionmaker` (one per `SAConfig` provider singleton;
  per-tenant providers under SCHEMA/DATABASE each have their own).
- `open_transaction(options)`: `session = self._session_factory()`; `await
  session.connection(execution_options={...})` with `isolation_level=options.isolation` and, on the
  Postgres dialects, `postgresql_readonly=options.read_only` — `connection(execution_options=)` is
  applied when the connection is first procured, i.e. before BEGIN (⚠️ ASSUMPTION, Step 0(c)).
  Acquiring the connection eagerly is also what fires the RLS `after_begin` listener with the
  boundary's tenant (rls_session.py:45-61). SA `ArgumentError` for an unsupported level →
  `TransactionConfigurationError`.
- `SQLAlchemyResourceTransaction`: `flush/commit/rollback` → the session's; `close()` →
  `session.close()`; `savepoint()` → `await session.begin_nested()` (flushes unconditionally,
  021 §A:47) wrapped in `SASavepoint`.
- `classify()` = `classify_chain(exc, _sqlstate_matcher)`: first `sqlalchemy.exc.DBAPIError` →
  `code = getattr(e.orig, "sqlstate", None) or getattr(e.orig, "pgcode", None)` (asyncpg and
  psycopg 3 both expose `sqlstate`, 021 §A:51,168 — ⚠️ ASSUMPTION for SA's asyncpg adapter, Step
  0(b)) → `"40001"` → `reason="serialization_failure"`, `"40P01"` → `reason="deadlock"`; else
  `None`. A `StaleDataError` is never inspected — 061 translates it to `StaleEntityError` first,
  and `classify_chain` short-circuits on it. Constant `TRANSIENT_SQLSTATES: Final[Mapping[str, str]]`.
- `SQLAlchemyJoinedUnitOfWork(JoinedUnitOfWork)`: `_bind()` builds the provider's
  `repo_factories` over `rtx.handle` and exposes `.session` (so `SAOutboxRepository(uow.session)`
  keeps working).
- `capabilities`: all three levels, `savepoints=True`, `read_only=True`. (aiosqlite accepts only
  `SERIALIZABLE`; the others surface as `TransactionConfigurationError` from `open_transaction` —
  asserted in the sqlite unit test, not pre-declared per dialect.)
- No change to `varco_sa/repository.py` (067 owns its streaming fix; 061 its `StaleDataError`
  translation).

### §D-http — the FastAPI request boundary: a route option + `TransactionModule`, never middleware

Precedence (first non-`None` wins): `@route(..., transaction=...)` → `VarcoRouter._transaction`
(ClassVar; applies to generated CRUD routes and custom routes) → the app-wide
`RequestTransactionPolicy` bound by `TransactionModule` (default `methods={"POST","PUT","PATCH",
"DELETE"}`, `options=TransactionOptions()`) → no boundary. The value is `TransactionOptions`,
`True` (= `TransactionOptions()`) or `False` (= never, overrides the module). `_register_route`
wraps the endpoint closure **returned by** `_make_http_handler` (061 adds `if_match`/`response`
parameters inside it — the wrapper preserves `__signature__`, so they keep working) so that
FastAPI calls `run_in_transaction(options, handler, **kwargs)`; retry re-invokes the handler with
the same resolved arguments — including 061's parsed `If-Match`, so a retried attempt re-evaluates
the same precondition against freshly loaded data. `create_varco_app`'s `_mount_router` passes the
bound policy to `build_router(request_transaction=policy)`. **Streaming routes need no special
case**: 067's export handler (`GET`, so outside the default policy anyway) works under any boundary
because `stream()` is detached (§D-stream); a retried handler re-primes a fresh stream.

```text
DESIGN: wrap the endpoint (Django ATOMIC_REQUESTS wraps the view), not an ASGI middleware slot
  ✅ The commit happens when the handler returns, BEFORE FastAPI serialises and sends the response:
     a commit failure becomes an error response. An ASGI middleware would commit after
     `http.response.start` — a client could see 200 for a rolled-back write.
  ✅ Retry can re-run the handler; a middleware cannot replay a consumed request body.
  ✅ Starlette's BaseHTTPMiddleware runs call_next in another task — §D-tasks would reject joining
     from the endpoint task. The wrapper enters and uses the boundary in the same task.
  ✅ It needs no EdgeSlot, and is by construction INSIDE every slot — tenant resolution
     (TENANT_RESOLUTION=95, so current_tenant() is set before enlistment), REQUEST_CONTEXT,
     IDEMPOTENCY (whose reservation must stay outside the transaction) — EdgeSlot rules hold
     without a new slot.
  ✅ A VarcoModule (not a create_varco_app keyword — CLAUDE.md "a switchable subsystem is a
     VarcoModule") for the app-wide default; listing it is the opt-in.
  ❌ FastAPI dependencies (incl. `yield` dependencies) resolve before the handler and run outside
     the boundary; a streaming body runs after the commit on stream()'s own detached UoW;
     `?with_async=true` jobs run outside. All three documented.
```

`TransientTransactionError` → **503** with `Retry-After` (its `retry_after_seconds=1` reaches the
header through `ErrorMiddleware`'s existing duck-typing, premise 9), code `VARCO_TX_001`,
`message_key="varco.error.transaction_transient"`, `error_params() == {"reason": reason}` (never SQL
text). `IdempotencyMiddleware` must not store a 503: Step 20 adds `503` to 061's
`_NEVER_STORE_STATUSES` (one member, one DESIGN line — premise 8: otherwise "retry after 1 s" would
be replayed for that key forever). Both are Open Questions 1/4 with these as the defaults.

### §D-migration — the `3-to-4.md` section (Step 9 writes it; 15/21 append)

"Ambient transactions (Plan 062)": (1) nothing changes unless you open a boundary; (2) inside one,
services join, and `_after_*` hooks, domain events and `_emit()` run after the **outermost**
commit — `create()` returns before them; (3) `stream()` never joins: inside a boundary it reads
committed data on its own connection; (4) a custom `IUoWProvider` must implement
`TransactionalResource` to be used by services inside a boundary (`TransactionConfigurationError`
otherwise); (5) never `asyncio.gather` service calls inside a boundary, never open one across a
`yield`; (6) `varco_sa.sqlalchemy_session` is deprecated (→ `transaction()`), removal 5.0.0;
(7) opt into per-request boundaries with `TransactionModule(...)` or `@route(transaction=...)`;
(8) a transient DB failure is now a 503 `VARCO_TX_001` when it escapes a boundary; optimistic-lock
conflicts (`OPTIMISTIC_LOCK_ERRORS`, Plan 061) are never retried by it. Placed directly after
061's section.

### Alternatives considered

- **Eager boundary bound to a provider** (`transactional(provider=...)`): rejected —
  ❌ couples use-case methods/routes to a DI-known provider; ❌ opens a connection for requests that
  never touch the DB; ✅ would avoid the lazy-savepoint logic. (§D-model)
- **Service-level join only** (`AsyncService` asks the boundary, providers untouched): rejected —
  ❌ hand-written `make_uow()` callers (the documented outbox pattern) would not join; ✅ no
  provider change. (§D-model)
- **Revive `varco_sa.sqlalchemy_session`** (`current_session` ContextVar): rejected — ❌ SA-only, so
  063 would need a parallel mechanism; ❌ no propagation, isolation, hooks or retry; ❌ commits
  per `SessionContext`, not per outermost boundary. Deprecated instead.
- **ASGI middleware at a new `EdgeSlot.TRANSACTION`**: rejected — ❌ commit after response start,
  ❌ no retry (body not replayable), ❌ BaseHTTPMiddleware task hop; ✅ would also cover plain
  FastAPI routes not built by `VarcoRouter` (they can use `@transactional` on the handler). (§D-http)
- **`stream()` joins, fails loudly after the boundary closes / opt-in excludes streaming routes /
  detach only under `NdjsonResponse` / boundary extends to response completion**: all rejected in
  §D-stream's DESIGN block (067 §D-neighbours).
- **Retry on by default**: rejected for now — ❌ silently duplicates external side effects in the
  body; ✅ would absorb deadlocks transparently. Open Question 2.
- **Exclude every `ServiceConflictError` from retry** (broader than 061's tuple): rejected —
  ❌ the coordinated contract is *exactly* `OPTIMISTIC_LOCK_ERRORS`; ❌ redundant anyway, since
  `classify()` only ever returns non-`None` for backend transient codes, so a business conflict
  raised by user code is never classified transient in the first place.
- **`TransientTransactionError` as a 409 `ServiceConflictError` subclass**: rejected — ❌ 409 tells
  the client to change the request, a serialization failure needs only a resubmit; ❌ blurs it with
  061's 409/412 family that clients handle by re-GETting; ✅ fewer 5xx on dashboards. Open Question 1.
- **Lock-serialised shared session across tasks**: rejected (§D-tasks).
- **`ExceptionGroup` for post-commit failures**: rejected — ❌ changes the single-hook exception
  type every existing caller sees; first-failure-plus-notes keeps it. (§D-postcommit)

## Steps

All commands from `/home/edoardo/projects/varco`. **Three phases, each independently mergeable**
(each ends green with its own docs, CHANGELOG lines and regenerated snapshots, per CLAUDE.md's
same-commit docs rule): after **A**, the API works with `InMemoryTransactionalResource`, and SA/
Beanie providers inside a boundary raise `TransactionConfigurationError` at the first service call
(loud, never silently per-call commits; outside a boundary nothing changed; `stream()` detached
everywhere); after **B**, SA joins; after **C**, routes can opt in. Every new module: `from
__future__ import annotations`, DESIGN blocks with ✅/❌, docstrings with Args/Returns/Raises/Edge
cases/Async safety (CLAUDE.md Coding Standards); locks, if any, created lazily.

### Phase 0 — Preflight

0. [ ] **Record the tree** in `/tmp/claude-1000/-home-edoardo-projects-varco/<session>/scratchpad/062-preflight.txt`:
       (a) `rg -n 'make_uow\(\)' varco_core/varco_core/service varco_core/varco_core/cache` → the 12
       sites of premise 7 (re-number after 061/067); `rg -n 'asyncio\.gather|create_task|TaskGroup'
       varco_core/varco_core/service varco_core/varco_core/cache` → only `paged_list` (+ docstrings).
       (b) `uv run python -c "import sqlalchemy; print(sqlalchemy.__version__)"` and
       `rg -n 'sqlstate|pgcode' "$(uv run python -c 'import sqlalchemy.dialects.postgresql.asyncpg as m; print(m.__file__)')"`
       → confirm the adapted DBAPI error carries `sqlstate`/`pgcode` and `raise ... from error`
       (resolves §Risks ⚠️ A1). (c) `uv run python -c "import inspect, sqlalchemy.ext.asyncio as a; print(inspect.signature(a.AsyncSession.connection))"`
       → `execution_options` present; `rg -n 'postgresql_readonly' <asyncpg.py>` (⚠️ A2/A3).
       (d) **Hard gate — 061 landed:** `uv run python -c "from varco_core.exception import OPTIMISTIC_LOCK_ERRORS, StaleEntityError, ConcurrencyConflictError; print(OPTIMISTIC_LOCK_ERRORS)"`
       succeeds; `rg -n 'expected_version' varco_core/varco_core/service/base.py varco_core/varco_core/service/soft_delete.py varco_core/varco_core/cache/mixin.py`
       non-empty; `rg -n '_NEVER_STORE_STATUSES' varco_fastapi/varco_fastapi/middleware/idempotency.py`
       non-empty. **Any red → stop: build 061 first.** (e)
       `rg -n '_LAZY|TYPE_CHECKING' varco_core/varco_core/__init__.py | head` — the two blocks a new
       top-level name must enter. (f) `rg -n 'retry_after_seconds' varco_fastapi/varco_fastapi` —
       is `exceptions.add_exception_handlers` also duck-typing it, or only `middleware/error.py:327`?
       (g) `rg -n '^## ' technical_docs/migrations/3-to-4.md` — where 061's section sits.
       (h) `make lint && make test PKG=varco_core && make test PKG=varco_sa && make test PKG=varco_fastapi`
       green baseline. (i) **Gate for Steps 13(j)/16(export) — 067 landed:** `rg -n
       'type: ignore\[attr-defined\]' varco_sa/varco_sa/repository.py` shows no `stream_scalars`
       ignore (067's own verify line, 067 `:729`) and `uv run python -c "import varco_fastapi.streaming"`
       succeeds. If 067 has not landed, Phases A–C may still merge, but those two sub-tests are written
       in the commit that follows 067 — never skipped or xfailed. **Verify**: (a)–(i) recorded; any
       red in (a)–(h) → stop.

### Phase A — `varco_core`: the API, the seam, service semantics (mergeable alone, after 061)

1. [ ] `varco_core/tests/test_transaction_boundary.py` (**new, failing first**) — against
       `InMemoryTransactionalResource` (Step 3): (i) commit on clean exit; writes invisible to a
       standalone reader until the ROOT commits; (ii) exception → rollback, nothing visible;
       (iii) REQUIRED inside REQUIRED shares one `ResourceTransaction` (one `open_transaction` call);
       (iv) inner non-`ServiceException` caught by the outer body → `TransactionRolledBackError` at
       outer exit, `__cause__` is the inner exception; (v) inner `ServiceNotFoundError` caught → outer
       commits; inner `StaleEntityError` / `ConcurrencyConflictError` caught → outer raises
       `TransactionRolledBackError` (§D-failure); (vi) REQUIRES_NEW commits although the outer rolls
       back; ambient restored after; (vii) NESTED: inner exception → outer writes survive, inner writes
       gone, inner hooks discarded; NESTED entered *before* first enlistment still gets its savepoint;
       NESTED with no boundary = ROOT; NESTED on a `savepoints=False` resource →
       `TransactionConfigurationError`; a `ConcurrencyConflictError` inside NESTED, caught → outer
       commits (savepoint cleared the mark); (viii) MANDATORY without boundary →
       `TransactionConfigurationError`; (ix) isolation mismatch on join →
       `TransactionConfigurationError`; unsupported isolation → same, at enlistment; (x) retry:
       resource told to fail commit #1 with a transient → body runs twice, hooks of attempt 0 never
       run, `attempt` is 0 then 1; exhaustion → `TransientTransactionError.attempts == 3`,
       `__cause__` the backend error; **each member of `OPTIMISTIC_LOCK_ERRORS`** (iterate the
       imported tuple — never a literal list) raised inside a retried body, *and* a transient whose
       `__cause__` is one of them → raised after **one** attempt; `CancelledError` → rollback, no
       retry; `retry=` on a JOINED frame ignored; (xi) `on_commit`: outside a boundary runs
       immediately (sync and async callbacks); inside, after the outermost commit only, in order,
       with `current_transaction() is None` during the hook; all hooks run when #1 raises, #1
       re-raised with a note naming #2's failure; no hooks after rollback; (xii) a boundary that never
       enlists opens nothing and still runs hooks; (xiii) second distinct `transaction_key` →
       `TransactionConfigurationError`; (xiv) `tenant_context("a")` enlist then a joined UoW under
       `tenant_context("b")` → `TransactionConfigurationError`; (xv) joined UoW entered from
       `asyncio.create_task` / inside `asyncio.gather` → `TransactionConfigurationError`;
       (xvi) a **joined** UoW left open (repository-level stream, simulated `break`) is closed at ROOT
       exit; using it after → `TransactionClosedError`; (xvii) `open_detached_uow()` inside a
       boundary → DETACHED, own handle, commits independently, usable from another task and after the
       boundary exited; (xviii) `async with transaction():` entered directly inside an async-generator
       body → `TransactionConfigurationError`; `@transactional` on a sync function / async generator
       → `TypeError` at decoration; `@transactional` and `@transactional(...)` both work and preserve
       `__wrapped__`/name. **Verify**: `uv run pytest varco_core/tests/test_transaction_boundary.py -q`
       → collection error (module missing) — red.

2. [ ] `varco_core/varco_core/exception/transaction.py` (**new**) + `varco_core/tests/test_transaction_exceptions.py`
       (**new, failing first**) — the four exceptions of §D-api;
       `TransientTransactionError(reason: str, *, attempts: int = 1)` with `retry_after_seconds: int
       = 1`, `message_key = "varco.error.transaction_transient"`, `error_params() -> {"reason": ...}`;
       `register_error_code(TransientTransactionError, ErrorCode(code="VARCO_TX_001",
       http_status=503, default_message="The operation conflicted with concurrent work. Retry the
       request.", message_key=...))` at import (the `exception/idempotency.py:153-164` precedent);
       re-export from `varco_core/varco_core/exception/__init__.py` beside 061's names. Tests:
       `not issubclass(TransientTransactionError, ServiceConflictError)`;
       `not issubclass(TransientTransactionError, OPTIMISTIC_LOCK_ERRORS)`; `error_message_for()` →
       503/`VARCO_TX_001`; `error_params()` never contains the driver message; the three
       `RuntimeError`s are not `ServiceException`s (→ 500). **Verify**: the test file green.

3. [ ] `varco_core/varco_core/transaction/` (**new package**) + `varco_core/varco_core/uow.py` —
       `options.py` (`Propagation`, `IsolationLevel`, `TransactionOptions`, `TRANSACTION_RETRY`),
       `context.py` (`FrameKind`, `TransactionContext` + the internal root/frame state, `_CURRENT`
       AmbientVar, `current_transaction()`, `on_commit()`, internal `_suspend_ambient()`), `resource.py`
       (`TransactionCapabilities`, `TransactionalResource`, `ResourceTransaction`, `Savepoint`,
       `JoinedUnitOfWork`, `classify_chain()` importing `OPTIMISTIC_LOCK_ERRORS` from
       `varco_core.exception`, `open_detached_uow()`), `boundary.py` (class-based `transaction()` with
       the async-generator caller guard, `transactional()`, `run_in_transaction()`, the owner loop of
       §D-model, the retry predicate of §D-retry), `memory.py` (`InMemoryTransactionalResource`:
       `make_uow()` → standalone or joined UoW exposing `.tables` and repos from `repo_factories`
       (mirrors SA's), copy-on-begin/swap-on-commit, savepoint = snapshot stack, nominally supports all
       three isolation levels and records the requested one, `read_only` raises on write, test hooks
       `fail_next_commit(exc)` / `fail_next_flush(exc)`, and `classify()` recognising a module-local
       `SimulatedTransientError` through `classify_chain`), `__init__.py` (`__all__`). `uow.py`: define
       `AmbientMode` there (so `resource.py` imports `uow.py`, never the reverse — no cycle) and add
       `ambient_mode` to `AsyncUnitOfWork`; `transaction/__init__.py` re-exports `AmbientMode`.
       **Verify**: Step 1 + Step 2 green; `uv run mypy varco_core/varco_core/transaction
       varco_core/varco_core/exception/transaction.py` clean.

4. [ ] `varco_core/varco_core/__init__.py` — add the starred names of §D-api to `_LAZY` **and** the
       `TYPE_CHECKING` block **and** `__all__` (all three, Step 0(e)). **Verify**:
       `uv run python -c "import varco_core as v; v.transactional; v.TransientTransactionError"`;
       `uv run python scripts/import_budget.py --check --warn-only` shows no `varco_core` regression.

5. [ ] `varco_core/tests/test_transaction_service.py` (**new, failing first**) — `AsyncService`
       subclasses over `InMemoryTransactionalResource` (two entity types, one versioned per 061, an
       `InMemoryEventBus`-backed `BusEventProducer`, a recording `_after_*` mixin): (i) **regression,
       no boundary**: create/update/delete publish their event and run `_after_*` before returning,
       same order as today; (ii) **two-service atomic rollback**: `@transactional` use case creates A
       then B, B's validator raises → neither persisted, no event published, no hook ran; (iii) inside
       a boundary `create()` returns with the event **not yet** on the bus; after the boundary, events
       in call order; (iv) `_emit()` deferred inside, immediate outside; (v) `AuditLogMixin` emits
       `AuditEvent`s only after commit, none on rollback; (vi) a `REQUIRES_NEW` inner create's event
       is published at the inner commit even though the outer rolls back; (vii) `paged_list` inside a
       boundary does not trip §D-tasks (sequential); (viii) a 3.x-style `FakeUoWProvider` (STANDALONE
       UoW) inside a boundary → `TransactionConfigurationError` naming the provider class;
       (ix) `GlobalUoWProvider` inside a boundary → DETACHED, independent commit, no error;
       (x) `BulkServiceMixin.create_many/delete_many` defer per entity in order;
       (xi) `SoftDeleteService.delete/restore` join; (xii) **`stream()` is detached (§D-stream, 067
       handoff)**: inside a boundary that has created row X, a primed `stream()` (first `__anext__`
       inside the boundary) does **not** yield X; the boundary then exits (commit, joined session
       closed) and the *same* iterator continues to completion from a **different task**
       (`asyncio.create_task(drain(it))`, mimicking Starlette's child-task body) without
       `TransactionClosedError`/`TransactionConfigurationError`; `current_transaction()` observed from
       inside the repository during iteration is `None`; outside a boundary `stream()` behaves exactly
       as before; (xiii) **061 coexistence**: `update(pk, dto, ctx, expected_version=<wrong>)` inside a
       `@transactional(retry=TRANSACTION_RETRY)` boundary → `PreconditionFailedError` after exactly
       one body run, no event, no hook; `expected_version=<right>` → saved, event after commit.
       **Verify**: red (service code unchanged).

6. [ ] **Service layer.** `varco_core/varco_core/service/base.py` — `_open_uow(*, detached=False)`
       (§D-service); the 8 sites (`stream()` uses `detached=True`, the other 7 the default); eager
       event build + `on_commit` in `create`/`update`/`delete`; `_emit()` via `on_commit`;
       `paged_list` sequential branch; update `IUoWProvider`, `_after_*`, `_emit`, `stream`
       ("never joins an ambient boundary — runs on its own detached UoW; see transactions.md
       §Streaming"), `paged_list` docstrings ("after the outermost commit when inside a boundary";
       Edge cases). `service/bulk.py` (2 sites + per-entity deferral), `service/soft_delete.py`
       (2 sites), `tenancy/global_scope.py` (`open_detached_uow(self._delegate)`). **Respect the 061
       and 067 coexistence rules of §D-service** — `git diff` of `update`/`patch`/`delete`/soft-delete
       must show no change to signatures, `_check_expected_version()` calls or `StaleEntityError`
       handling, and in `stream()` only the UoW-opening expression and docstring change. **Verify**:
       Step 5 green; `rg -n 'self\._uow_provider\.make_uow\(\)' varco_core/varco_core/service` → 0;
       `uv run pytest varco_core/tests -q` green (every existing test — including 061's and 067's —
       unmodified).

7. [ ] `varco_core/tests/test_transaction_cache.py` (**new, failing first**) then
       `varco_core/varco_core/cache/mixin.py` — §D-cache: inside a boundary, update(pk) then get(pk)
       returns the new value and never calls `cache.set`; invalidation happens only after commit and
       not at all on rollback; a namespace not written in the boundary still hits the cache; outside
       a boundary byte-identical (existing cache tests, incl. 061's `expected_version` forwarding
       spy, unmodified). **Verify**: the new file + `uv run pytest varco_core/tests -k cache -q` green.

8. [ ] `testkit/varco_conformance/transaction.py` (**new**, base class `TransactionalResourceConformance`,
       not `Test*`) — abstract fixtures `resource`, `write(uow, key, value)`, `read_committed(key)`
       (fresh standalone UoW), `make_transient_error() -> BaseException`, `make_non_transient_error()`;
       tests: commit visibility, rollback discard, one resource transaction across two `make_uow()`s,
       REQUIRES_NEW independence (two concurrent handles), NESTED per `capabilities.savepoints` (works,
       or raises), each declared isolation level opens (others raise), `classify()` positive/negative,
       a transient chained under each `OPTIMISTIC_LOCK_ERRORS` member → `None`, `close()` idempotent,
       `open_detached_uow()` inside a boundary gets a distinct handle and commits independently.
       Subclass in `varco_core/tests/test_conformance_inmemory.py` for
       `InMemoryTransactionalResource`; `testkit/varco_conformance/COVERAGE.md` — new suite + row (SA →
       Phase B, Beanie → 063, custom providers → "implement or raise"). **Verify**: `uv run pytest
       varco_core/tests/test_conformance_inmemory.py -q -k Transaction` green.

9. [ ] **Docs (Phase A slice).** `technical_docs/features/transactions.md` (**new**): API table
       (three shapes), propagation table, isolation, retry + the side-effects warning + the
       `OPTIMISTIC_LOCK_ERRORS` exclusion (link 061's feature page), post-commit semantics, service join
       rules, failure/rollback-only, tasks, tenancy/GLOBAL, **Streaming** (stream() is detached — why,
       citing the export path of 067's feature page; the repository-level joined escape hatch), cache,
       outbox/audit, "Implementing a backend" (§D-seam verbatim incl. the 063 notes), Pitfalls table
       (SERIALIZABLE without retry; gather inside a boundary; a boundary across a `yield`;
       `_producer._produce` bypasses deferral; `stream()` inside a boundary does not see its
       uncommitted writes and holds a second connection; GLOBAL write not atomic + second connection;
       REQUIRES_NEW holds two connections; a joined repository-level stream used after exit; direct
       `make_uow()` on a non-seam provider runs standalone; swallowing a 061 conflict inside a joined
       call dooms the boundary). `mkdocs.yml` nav entry. `README.md` — "Transactions" section
       (decorator, CM, `on_commit`) + TOC. `ARCHITECTURE.md` — package map row + "Transactions" type
       hierarchy. `CLAUDE.md` — a short "Ambient transactions (varco_core.transaction, Plan 062)" rule
       block (services join, `stream()` never joins, events/hooks after outermost commit, providers
       implement `TransactionalResource`, never gather inside a boundary, never open one across a
       `yield`, `OPTIMISTIC_LOCK_ERRORS` never retried — one line, pointing at 061's rule rather than
       restating it) + one decision-tree line ("multi-call atomicity / isolation / transient retry? →
       `varco_core.transaction`, never a hand-opened shared UoW") + the conformance-module count
       ("**nine**" → the correct number; re-count after 061/067). `technical_docs/common-pitfalls.md` —
       one cross-cutting row (gather of service calls inside a boundary). `CHANGELOG.md` `[Unreleased]`
       — "Added — ambient transactions (Plan 062)" + "Changed — inside a boundary, events/hooks run
       after the outermost commit (Plan 062)". `technical_docs/migrations/3-to-4.md` — new section
       directly after 061's (Step 0(g)) with §D-migration items 1–5, 8; renumber the following
       sections and fix in-file refs (`rg -n '§[0-9]+\b' technical_docs/migrations/3-to-4.md`).
       **Verify**: `rg -n '\*\*nine\*\*' CLAUDE.md` → 0 (or the correct count); `make docs` green.

10. [ ] **Gates (Phase A).** `uv run python scripts/api_surface.py` (commit the regenerated
        `api-surface.json/.md`), `make entry-points` / `make di-catalogue` / `make settings-reference`
        (expect **no** diff — assert it), `make lint`, `make type-check`, `make test PKG=varco_core`,
        `make test`. **Verify**: `git status` shows only intended files; all `--check`s green.

### Phase B — `varco_sa`: the SQLAlchemy backend (mergeable after A)

11. [ ] `varco_sa/tests/test_sa_transaction.py` (**new, failing first**; aiosqlite, no Docker) —
        two `make_uow()` inside one boundary share one `AsyncSession` (`uow1.session is uow2.session`);
        rollback discards both; standalone path unchanged outside; `SAOutboxRepository(uow.session)`
        inside a joined UoW commits atomically with the entity and rolls back with it;
        `classify()` on synthetic `DBAPIError`s whose `orig.sqlstate` is `40001`/`40P01`/`23505`/
        missing (and one with only `pgcode`), and on a 40001 chained under a `StaleEntityError` (→
        `None`); `isolation=REPEATABLE_READ` on sqlite → `TransactionConfigurationError`; session closed
        after the ROOT (`session.close` called once); a `DynamicTenantUoWProvider` over two per-tenant
        providers joins per tenant and the cross-tenant rule fires; a versioned entity's stale write
        inside a joined UoW (061's `StaleEntityError`) dooms the boundary → `TransactionRolledBackError`
        if swallowed; `open_detached_uow(provider)` inside a boundary returns a UoW whose `.session` is
        **not** the boundary's. **Verify**: red.

12. [ ] `varco_sa/varco_sa/transaction.py` (**new**) + `varco_sa/varco_sa/provider.py` —
        §D-sa: `SQLAlchemyResourceTransaction`, `SASavepoint`, `SQLAlchemyJoinedUnitOfWork`,
        `classify_sqlalchemy_error()` (via `classify_chain`), `TRANSIENT_SQLSTATES`;
        `SQLAlchemyRepositoryProvider` implements `TransactionalResource` and `make_uow()`
        (`provider.py:183`) returns the joined UoW when `current_transaction()` is set. Do not touch
        `varco_sa/repository.py` (061's `StaleDataError` translation and 067's `stream_scalars` fix live
        there). **Verify**: Step 11 green; `make test PKG=varco_sa` green (all existing SA tests
        unmodified).

13. [ ] `varco_sa/tests/test_sa_transaction_integration.py` (**new**, `pytestmark =
        pytest.mark.integration`, shared `postgres_url`, tables namespaced by a `uuid4().hex[:8]` run
        id) + `TestSATransactionalResourceConformance` in the same file:
        (a) **two-service atomic rollback** — `PostService.create` + `CommentService.create` in one
        `@transactional`, the second raises → `SELECT count(*)` = 0 in both tables, no event;
        (b) **real serialization failure + retry** — classic write-skew under `SERIALIZABLE`: two
        boundaries each read both "on-call" rows then update a different one, synchronised with two
        `asyncio.Event` barriers so both read before either writes; without `retry=` exactly one raises
        `TransientTransactionError(reason="serialization_failure")` whose `__cause__` is a `DBAPIError`
        with SQLSTATE 40001 (at statement or COMMIT time — assert either); with
        `retry=TRANSACTION_RETRY` (jitter off, `base_delay=0`) both complete, the loser's body ran
        exactly twice, and the invariant (≥1 on call) holds; (c) **deadlock** — two boundaries update
        rows A,B in opposite order behind barriers → one `TransientTransactionError(reason="deadlock")`;
        (d) **isolation applied and reset** — `SHOW transaction_isolation` inside a `REPEATABLE_READ`
        boundary = `repeatable read`; a following standalone UoW on a 1-connection pool reports
        `read committed`; (e) `read_only=True` + INSERT → `DBAPIError` SQLSTATE 25006, classified
        `None`, not retried; (f) **NESTED** — IntegrityError inside a NESTED frame is caught, outer
        insert commits (get-or-create); (g) REQUIRES_NEW row committed although the outer rolls back;
        (h) outbox row absent after rollback, present after commit; (i) with
        `install_rls_tenant_hook`, `current_setting('<guc>', true)` inside the boundary = the
        enlisting tenant; joined UoW under another tenant → `TransactionConfigurationError`;
        (j) **067 handoff (requires 067, Step 0(i))** — on a pool of size 2: inside a boundary that
        inserted row X, prime `service.stream()` (first `__anext__`), exit the boundary (commit), then
        drain the iterator from a child task: it completes with no error, never yields X's
        *uncommitted* state but the committed rows only as of its own statement snapshot, its own
        connection is returned to the pool after `aclose()` (pool `checkedout() == 0`), and with RLS
        installed the stream's own connection carries the request tenant's GUC;
        (k) **061 handoff (061 `:14`)** — versioned entity at v1; boundary B
        (`REPEATABLE_READ`, `retry=TRANSACTION_RETRY`) loads it with `expected_version=1`, waits on a
        barrier while A updates+commits (→ v2), then B's UPDATE raises 40001 → retried; attempt 2
        reloads v2, `expected_version=1` mismatches → `PreconditionFailedError`; assert B's body ran
        exactly **twice** (not three times) and the row is v2. Timing: if (b)/(c)/(k) flake, widen
        barrier timeouts (CLAUDE.md: never xfail a flaky timing test). **Verify**:
        `uv run pytest varco_sa/tests/test_sa_transaction_integration.py -m integration -q` green twice
        in a row.

14. [ ] **Deprecate `varco_sa/varco_sa/sqlalchemy_session.py`** — `warn_deprecated("varco_sa.sqlalchemy_session.SessionContext"
        / ".with_session", since="4.0.0", removed_in="5.0.0", replacement="varco_core.transaction
        (transaction() / @transactional) — technical_docs/features/transactions.md")` in
        `SessionContext.__init__` and `with_session()`; module docstring says so;
        `varco_sa/tests/test_sa_session.py` wraps calls in `pytest.warns(DeprecationWarning)`.
        **Verify**: `uv run pytest varco_core/tests/test_deprecation_floor.py varco_sa/tests/test_sa_session.py -q`
        green (since = pending 4.0.0 is allowed by the floor test's rule).

15. [ ] **Docs + gates (Phase B).** `transactions.md` §SQLAlchemy (key = sessionmaker, isolation
        before BEGIN, savepoints, SQLSTATE table, RLS interaction incl. detached streams, aiosqlite
        limits, the 40001 → retry → 412 sequence with 061); README SA snippet (two services, one
        boundary, outbox in the same transaction); `ARCHITECTURE.md` varco_sa entry; `COVERAGE.md` SA
        row → "subclassed (integration)"; `technical_docs/features/multitenancy.md` connection-budget
        worksheet: +1 line (REQUIRES_NEW, GLOBAL and `stream()` inside a boundary each hold a second
        connection); `CHANGELOG.md` Added (SA backend) + Deprecated (`sqlalchemy_session`,
        since/removed_in/floor); `3-to-4.md` item 6 + the deprecations list. Regenerate
        `api_surface.py`; `make lint`, `make type-check`, `make test PKG=varco_sa`. **Verify**: all
        `--check`s green.

### Phase C — `varco_fastapi`: the request boundary (mergeable after A; B for a real DB)

16. [ ] `varco_fastapi/tests/test_request_transaction.py` (**new, failing first**; in-memory
        resource provider wired into a `VarcoCRUDRouter` + one custom `@route`): a route with
        `transaction=True` makes two service calls atomic (second raises → error status and neither
        row exists); retry: an injected transient on commit #1 with `TransactionOptions(retry=...)` →
        handler invoked twice, 200; precedence route > `_transaction` > `TransactionModule` policy;
        `transaction=False` beats the module; the module's default policy wraps POST but not GET;
        a FastAPI dependency observes `current_transaction() is None`; exhausted transient → **503**,
        `Retry-After: 1`, body `code == "VARCO_TX_001"`; with `IdempotencyMiddleware` installed, the 503
        is **not** replayed for the same key (second request reaches the handler); a PUT with a stale
        `If-Match` (061) inside a retried route boundary → 412 after one handler run (the wrapper
        preserved 061's `if_match` parameter); **067 export under a boundary (requires 067, Step
        0(i))**: a router with `_transaction = True` and 067's export route — the full NDJSON body
        arrives (every row, no `$error` trailer) although the boundary committed at handler return, and
        a client disconnect mid-body still closes the stream's UoW. **Verify**: red.

17. [ ] `varco_fastapi/varco_fastapi/transaction.py` (**new**) — `RequestTransactionPolicy`
        (frozen: `methods: frozenset[str]`, `options: TransactionOptions`), `resolve_route_transaction()`
        (§D-http precedence), `wrap_endpoint(handler, options)` (async-only; preserves
        `__signature__`/`__name__`; calls `run_in_transaction`). **Verify**: the
        `resolve_route_transaction` cases of Step 16 green.

18. [ ] **Router plumbing.** `router/endpoint.py` — `_RouteEntry.transaction: TransactionOptions |
        bool | None = None`, `route(..., transaction=...)` + docstring; `router/introspection.py` —
        `ResolvedRoute.transaction` (`hash=False, compare=False`, like `requires`, `:117`);
        `router/base.py` — `_transaction: ClassVar[TransactionOptions | bool | None] = None`,
        `build_router(self, *, request_transaction: RequestTransactionPolicy | None = None)`,
        `_register_route` wraps the handler returned by `_make_http_handler` when the resolved value
        is not `None`/`False` (do not edit `_make_http_handler` — 061's `if_match` plumbing and 067's
        export factory live in that neighbourhood); `router/crud.py` `build_router` override gains the
        same keyword. **Verify**: `make test PKG=varco_fastapi` green (every existing router test,
        incl. 061's and 067's, unmodified).

19. [ ] `varco_fastapi/varco_fastapi/modules/transaction.py` (**new**) — `TransactionModule(VarcoModule)`
        (declares `phase` per Plan 050's rule; no lifecycle components; `_register` binds the
        `RequestTransactionPolicy` instance); `varco_fastapi/varco_fastapi/app.py` `_mount_router`
        resolves the policy when bound and passes it to `build_router(request_transaction=...)`;
        export from `varco_fastapi/modules/__init__.py` and `varco_fastapi/__init__.py` `__all__`
        (`TransactionModule`, `RequestTransactionPolicy`). Not in `default_modules()` (opt-in).
        **Verify**: Step 16 precedence tests green.

20. [ ] `varco_fastapi/varco_fastapi/middleware/idempotency.py` — add `503` to 061's
        `_NEVER_STORE_STATUSES` (extend its DESIGN block by one line: a 503 is transient by
        definition; replaying it contradicts its own `Retry-After`) — no second capture branch;
        `varco_fastapi/varco_fastapi/exceptions.py` — if Step 0(f) found `add_exception_handlers`
        does not duck-type `retry_after_seconds`, add it there too (parity with
        `middleware/error.py:327`). **Verify**: Step 16 fully green; `uv run pytest
        varco_fastapi/tests -k idempotency -q` green.

21. [ ] **Docs + gates (Phase C).** `transactions.md` §HTTP (precedence, what runs outside —
        dependencies, streaming bodies (detached `stream()`), `with_async` jobs —, 503 + Idempotency-Key
        interaction, `If-Match` re-evaluated per attempt, why not middleware); README "Per-request
        transactions" snippet (`modules=[*default_modules(), TransactionModule(RequestTransactionPolicy())]`
        and `@route(..., transaction=True)`); `ARCHITECTURE.md` varco_fastapi entry; `CLAUDE.md`
        decision tree: "Per-request transaction (ATOMIC_REQUESTS)? → `@route(transaction=)` /
        `VarcoRouter._transaction` / `TransactionModule`, never a middleware or a `create_varco_app`
        kwarg"; `technical_docs/features/idempotency-key.md` — 503 joins 061's never-store row; 067's
        streaming feature page — one line linking transactions.md §Streaming (no behaviour change for
        it); `CHANGELOG.md` Added (request boundary) + Changed (idempotency never stores 503);
        `3-to-4.md` items 7–8. Regenerate: `uv run python scripts/api_surface.py`, `make entry-points`
        (TransactionModule appears in `technical_docs/entry-points.md` + ARCHITECTURE's marked block),
        `make di-catalogue` (expect no diff — module not default), `make settings-reference` (no diff).
        Then `make lint`, `make type-check`, `make test`. **Verify**: every snapshot `--check` green;
        `git diff --stat` limited to intended files.

## Edge cases

- No boundary anywhere → byte-identical behaviour: events and `_after_*` run before `create()`
  returns; `paged_list` gathers; `stream()` unchanged; cache unchanged; 061's `expected_version`
  semantics unchanged.
- REQUIRED inside REQUIRED, inner raises a driver error, outer catches and returns → outer raises
  `TransactionRolledBackError` (`__cause__` = inner error); nothing committed.
- Inner raises `ServiceNotFoundError`, outer catches → outer commits normally.
- Inner raises any `OPTIMISTIC_LOCK_ERRORS` member, outer catches → `TransactionRolledBackError`
  (inside `NESTED` → recoverable, outer commits).
- `uow.commit()` called explicitly on a joined UoW → flush only; `uow.rollback()` → rollback-only.
- `MANDATORY` with no boundary → `TransactionConfigurationError`; `NESTED` with none → ROOT.
- `NESTED` on Mongo (063) / any `savepoints=False` resource → `TransactionConfigurationError` at
  enlistment.
- Inner explicit isolation ≠ root's → `TransactionConfigurationError`; inner `isolation=None` joins
  anything.
- `retry=` on a JOINED frame → ignored (DEBUG log); the root's policy governs.
- Transient raised inside a NESTED frame → savepoint rolled back, exception propagates, ROOT retries
  the **whole** body (a savepoint retry cannot fix a serialization failure — 021 §A:194).
- Serialization failure at **COMMIT** → classified and retried like a statement failure.
- `OPTIMISTIC_LOCK_ERRORS` member raised, or chained as the cause of a transient-looking error →
  never retried, never classified transient.
- 40001 from a concurrent UPDATE on a versioned entity under REPEATABLE READ with retry → retried
  once; the re-run raises `PreconditionFailedError` (with `expected_version`) or succeeds against the
  new version (without it) — never a third attempt for the conflict itself.
- Retries exhausted → `TransientTransactionError(attempts=N)` from the last backend error → 503.
- `CancelledError` during body or commit → rollback attempted, re-raised, never retried.
- Post-commit hook raises → remaining hooks run; first exception re-raised with notes; data stays
  committed; no retry.
- Hook registers another `on_commit` → runs immediately (no ambient during hooks).
- Boundary with zero enlistments → no connection; hooks still run.
- Second distinct resource → `TransactionConfigurationError`; GLOBAL provider → DETACHED,
  independent commit, second connection.
- Enlistment under tenant A then UoW under tenant B → `TransactionConfigurationError`.
- `asyncio.gather`/`TaskGroup`/`create_task` making (non-stream) service calls inside a boundary →
  `TransactionConfigurationError`.
- `service.stream()` inside a boundary → detached: does not see the boundary's uncommitted writes;
  keeps working after the boundary commits, from any task, until exhausted or `aclose()`d (067's
  export under a request boundary streams every row).
- Retried route handler that primed a stream on attempt 0 → attempt 0's iterator is discarded with
  the failed attempt (closed by GC/`aclose()` of the discarded response — never sent); attempt 1
  primes a fresh one.
- Repository-level `stream_by_query` in a *joined* UoW, `break`, boundary exit → commit; iterating
  again → `TransactionClosedError`.
- `async with transaction():` directly inside an async-generator body → `TransactionConfigurationError`.
- `@transactional` on a sync function or async generator → `TypeError` at decoration.
- `read_only=True` + write on Postgres → `DBAPIError` (25006), not transient, not retried.
- Request boundary + `?with_async=true` → the job runs outside any boundary.
- Exhausted transient behind `IdempotencyMiddleware` → 503 not stored, a retry with the same key
  re-executes.

## Verification

```bash
# Prerequisites (061 landed; 067 landed for 13(j)/16(export))
uv run python -c "from varco_core.exception import OPTIMISTIC_LOCK_ERRORS; print(OPTIMISTIC_LOCK_ERRORS)"
uv run python -c "import varco_fastapi.streaming"
# Phase A
uv run pytest varco_core/tests/test_transaction_boundary.py varco_core/tests/test_transaction_exceptions.py \
  varco_core/tests/test_transaction_service.py varco_core/tests/test_transaction_cache.py -q
uv run pytest varco_core/tests/test_conformance_inmemory.py -q
rg -n 'self\._uow_provider\.make_uow\(\)' varco_core/varco_core/service     # → 0
rg -n 'OPTIMISTIC_LOCK_ERRORS\s*[:=]' varco_*/varco_*                        # → exactly 1 hit (061's definition)
rg -n '_open_uow\(detached=True\)' varco_core/varco_core/service/base.py     # → 1 (stream)
make test PKG=varco_core
# Phase B
uv run pytest varco_sa/tests/test_sa_transaction.py varco_sa/tests/test_sa_session.py -q
uv run pytest varco_sa/tests/test_sa_transaction_integration.py -m integration -q   # twice
make test PKG=varco_sa
# Phase C
uv run pytest varco_fastapi/tests/test_request_transaction.py -q
make test PKG=varco_fastapi
# Every phase
make lint            # ruff check + format --check + api-check + asyncapi-check + import-budget(warn)
                     # + settings-check + di-catalogue-check + entry-points-check
make type-check
make test
uv run python scripts/api_surface.py --check
uv run python scripts/import_budget.py --check --warn-only
rg -n 'sqlalchemy_session' varco_*/varco_* | rg -v 'varco_sa/varco_sa/sqlalchemy_session.py'   # → 0
```

## Risks

- ⚠️ **ASSUMPTION A1** — SQLAlchemy 2.0.48's asyncpg adapter exposes the SQLSTATE on
  `DBAPIError.orig` as `.sqlstate` (and `.pgcode`) and chains the asyncpg error via `raise … from`.
  Research 021 §A:51,168 says asyncpg 0.31 has both, and lists "which is canonical" as an evidence
  gap (:181). **Verified in Step 0(b)** against installed source; `classify()` reads `sqlstate`,
  falls back to `pgcode`, then to `orig.__cause__.sqlstate`. Invariant: a 40001 must never be
  classified `None` — Step 13(b) is the end-to-end proof.
- ⚠️ **ASSUMPTION A2** — `AsyncSession.connection(execution_options={"isolation_level": …})`
  applies before BEGIN and SQLAlchemy resets the level when the connection returns to the pool
  (021 §A:36 says the level "remains in effect until the connection is returned to the pool").
  Step 0(c) + Step 13(d) verify; if the reset does not happen, `close()` must reset explicitly.
- ⚠️ **ASSUMPTION A3** — the asyncpg dialect honours `postgresql_readonly`. Step 0(c); if absent,
  `read_only` becomes a capability `False` on that dialect (hint ignored), not a raise.
- ⚠️ **ASSUMPTION A4** — Postgres reliably produces 40001 for the barrier-synchronised write-skew
  test and the 061 handoff test, and 40P01 for the opposite-order updates. Textbook behaviour (018
  §A:36-38; 061 `:14` for the RR concurrent-UPDATE case); the barriers make ordering deterministic.
  If flaky, widen timeouts — never `xfail` (CLAUDE.md test conventions).
- ⚠️ **ASSUMPTION A5** — the async-generator caller guard (`sys._getframe(1)` +
  `inspect.CO_ASYNC_GENERATOR`) sees the user's frame because `transaction()` is a class-based CM
  (no `@asynccontextmanager` frame in between). Best-effort only — a helper function one frame
  deeper evades it; the Pitfalls row is the real defence. Step 1(xviii) pins the direct case.
- **Build order with 061 and 067.** Phase A imports `OPTIMISTIC_LOCK_ERRORS` and edits the same
  methods 061 edits (`update/patch/delete`, `SoftDeleteService.delete`, `CacheServiceMixin`,
  `_make_http_handler` neighbourhood, `idempotency.py`) and the one line of `stream()` 067 depends
  on. Invariant: 062 lands strictly after 061 (Step 0(d) gate); 067's streaming sub-tests are added
  only once 067 has landed (Step 0(i)); Step 6/7/12/18/20 diffs touch none of 061's or 067's lines
  (reviewed via `git diff`).
- **Silent non-atomicity** if a resource-owning provider bypasses the seam. Invariant: every
  `AsyncService` path goes through `_open_uow()` (the `rg` in Verification = 0); custom providers get
  a loud error at the service layer.
- **Behaviour change inside a boundary** (deferred hooks/events): code reading its own side effects
  right after `create()` breaks only once it opts into a boundary. Mitigated by the 3-to-4 section and
  the regression test (Step 5(i)) proving zero change outside one.
- **`stream()` does not see a boundary's own writes.** A use case that creates rows and then streams
  them back in the same boundary gets the pre-boundary state. Documented with the repository-level
  joined escape hatch; accepted as the price of 067's export working under every boundary.
- **Dooming on swallowed `OPTIMISTIC_LOCK_ERRORS`** is conservative for 061's pre-write
  `PreconditionFailedError` (no DB failure happened). Accepted: correct for the post-flush
  `StaleEntityError` case, and a precondition failure is normally propagated, not swallowed.
  Open Question 6.
- **Connection budget**: REQUIRES_NEW, GLOBAL access and `stream()` inside a boundary each hold a
  second pooled connection per request; a per-request boundary holds its connection for the whole
  handler. Documented in transactions.md + multitenancy.md's worksheet.
- **`IdempotencyMiddleware` change** (never store 503) affects any app that returns 503 for its own
  reasons — they also stop being replayed, which is the correct semantics for a transient status.
  Open Question 4.
- **Standalone `SQLAlchemyUnitOfWork.commit()` leaks its session until GC** (premise 5) — not
  changed here; flagged for the coordinator as a candidate BACKLOG row.
- **Import budget**: `service/base.py` now imports `varco_core.transaction.context` (stdlib +
  `context/ambient.py` only). Measured in Step 4/10; never "fixed" by raising a ceiling.

## Open questions (for the coordinator / user — defaults chosen, so the plan is buildable as is)

1. **HTTP status of an escaped `TransientTransactionError`** — default **503 + `Retry-After: 1`**
   (`VARCO_TX_001`). Alternative 409 (fewer 5xx on dashboards; but tells the client to change the
   request and blurs it with 061's 409/412 family).
2. **Retry default** — default **off** (`retry=None`; `TRANSACTION_RETRY` opt-in). Alternative: on by
   default for `@transactional` (absorbs deadlocks transparently; duplicates external side effects in
   the body).
3. **Atomic audit/events via an `OutboxEventProducer`** — parked here (audit is post-commit and
   commit-exact). Pull into 063, a 062c, or a new BACKLOG row? It is the only way to make "audit
   writes join" literally true.
4. **`IdempotencyMiddleware` never stores 503** — default **yes, in Phase C (Step 20)**, as one
   member of 061's `_NEVER_STORE_STATUSES`; or split it out as its own row.
5. **GLOBAL never enlists** — default **yes** (uniform across SHARED/SCHEMA/DATABASE); alternative
   "enlist when the resource key matches, raise otherwise".
6. **Joined-UoW exception policy** — default: a `ServiceException` outside `OPTIMISTIC_LOCK_ERRORS`
   does **not** doom the transaction, anything else does. Alternative (Spring-like): every exception
   crossing a joined UoW dooms it.
7. **`stream()` always detached** — default **yes** (§D-stream). Is a multi-statement
   snapshot-consistent export (export + count in one REPEATABLE READ snapshot) a 4.0 requirement for
   067/065? If so it needs a response-lifetime read-only boundary — a separate design, parked here.
8. **`23505` under `SERIALIZABLE`** — parked (not transient). Revisit if a user hits it.

## Parked / out of scope (with triggers)

- `OutboxEventProducer` — trigger: Open Question 3 answered "yes" or a user needs atomic audit.
- Snapshot-consistent multi-statement export (a response-lifetime read-only boundary) — trigger:
  Open Question 7 answered "yes".
- `SUPPORTS`/`NOT_SUPPORTED`/`NEVER` propagation — trigger: a concrete caller.
- Retrying `23505` under `SERIALIZABLE` — trigger: a real report.
- `IntegrityError` → `ServiceConflictError` translation in `varco_sa` — separate gap.
- `SQLAlchemyUnitOfWork.commit()` not closing its session — separate gap (premise 5).
- Request boundary for non-`VarcoRouter` FastAPI routes — use `@transactional` on the handler.

## Done — this plan

- `varco_core.transaction` exports the §D-api surface; top-level `varco_core` re-exports the starred
  names; `api-surface.json` regenerated.
- All 12 service-layer UoW sites go through `_open_uow()`; events/`_emit`/`_after_*` deferred inside a
  boundary, unchanged outside (regression test green); 061's `expected_version` paths unchanged;
  `stream()` detached, proven to outlive a boundary (Steps 5(xii), 13(j), 16).
- `OPTIMISTIC_LOCK_ERRORS` is referenced (never redefined) by the retry predicate and
  `classify_chain`; Step 13(k) proves the 40001 → retry → 412 handoff.
- `CacheServiceMixin` passes the dirty-namespace tests.
- `testkit/varco_conformance/transaction.py` exists; in-memory and SA subclass it; `COVERAGE.md`
  updated; CLAUDE.md's conformance-module count correct.
- `varco_sa` passes the Postgres integration file, including a real 40001 retry and the two-service
  atomic rollback.
- `varco_fastapi` route/router/module opt-in works; 067's export streams in full under a boundary;
  503 + `Retry-After`; idempotency never stores 503; `TransactionModule` appears in
  `technical_docs/entry-points.md`.
- `varco_sa.sqlalchemy_session` deprecated (since 4.0.0, removed in 5.0.0).
- `transactions.md`, README, ARCHITECTURE, CLAUDE.md, CHANGELOG, `3-to-4.md`, `common-pitfalls.md`,
  `multitenancy.md`, `idempotency-key.md` updated; `make lint`, `make type-check`, `make test` green.
