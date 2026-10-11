# Plan 063 — Ambient transactions on MongoDB: the `varco_beanie` backend of `varco_core.transaction` (ACID-1, slice 2 of 2)

Covers the Mongo half of BACKLOG 4.0.0 row **`ACID-1`** (`BACKLOG.md:59` — *"… whole-transaction
retry on Postgres 40001/40P01 **and Mongo `TransientTransactionError`**"*) under the locked decisions
`BACKLOG.md:33` (*one `@transactional(isolation=…, retry=…)` boundary, designed once — retry must
re-run the whole transaction*) and `BACKLOG.md:31` (ships inside 4.0.0; breaking allowed, each break
with a `technical_docs/migrations/3-to-4.md` section).

**This plan implements, and never redesigns, Plan 062's API.** Everything public —
`transaction()`/`@transactional`/`run_in_transaction`, `Propagation`, `IsolationLevel`,
`TransactionOptions`, `TRANSACTION_RETRY`, `current_transaction()`, `on_commit()`,
`open_detached_uow()`, `TransactionContext`, the four exceptions — and the backend seam
(`TransactionalResource`, `ResourceTransaction`, `TransactionCapabilities`, `JoinedUnitOfWork._bind()`,
`classify_chain`) is 062's (`plans/062-ambient-transactions-core-and-sqlalchemy.md` §D-api `:156-232`,
§D-seam `:627-699`, its notes for this plan `:679-687`). This plan writes the Mongo implementation of
that seam, the Mongo-specific behaviour around it (replica-set requirement, isolation mapping, error
labels, commit-only retry, 120 s window), the session threading Beanie needs, the fate of
`BeanieSettings.transactional`, a replica-set test fixture, and docs.

**Neighbours.** **061** (`plans/061-optimistic-concurrency-and-conditional-requests.md`) adds
`row_version`-filtered `replace_one`/`delete_one` calls to `AsyncBeanieRepository` with a
`# 063: pass session= here` comment at each (061 `:15`, `:134-151`, Step 6 `:427-431`), defines
`OPTIMISTIC_LOCK_ERRORS` (never retried — 062 §D-retry), and documents Beanie `delete_many`'s *partial*
deletion on a stale batch "until 063" (061 `:420-422`, `:539`, `:581`). This plan threads the session
into exactly those calls and proves `delete_many` atomic **inside a boundary** (Step 7(g)). **067**
(`plans/067-streaming-reads-and-keyset-pagination.md`) fixes Beanie's never-closed cursor (067
`:45-46`, `:487-490`) and needs `stream()` detached (062 §D-stream) — nothing changes for it here:
a detached Beanie UoW passes no session (Step 7(j)). **069a** keeps `JobQueue.enqueue()`
non-joining (069a `:41-42`, R6 `:654`); `BeanieJobStore` therefore never enlists (062
§D-outbox-audit "framework stores never join", `:623-625`).

**Sources of record read (2026-10-10):** `CLAUDE.md`; `BACKLOG.md:27-37,59`; `plans/062` (whole);
`plans/061` `:1-40,128-157,415-431`; `plans/067` `:40-53,474-493,640-647`; `plans/069a` (grep:
`:20,41-42,64-69,654`); `varco_beanie/varco_beanie/uow.py` (whole); `provider.py` (whole);
`repository.py` (whole); `config.py` (whole); `modules.py` (whole); `outbox.py:1-80,234-440` (grep);
`di.py:190-302`; `health.py:1-60`; `bootstrap.py` (grep); `tenancy/pool.py` (whole);
`tenancy/binding.py:60-130`; `varco_beanie/tests/conftest.py` (whole); `varco_beanie/pyproject.toml`
(grep); `varco_beanie/tests/test_beanie_config_alias.py:1-95`, `test_beanie_bootstrap.py:40-80`, the
`transactional` grep over `varco_beanie/tests/`; `varco_core/varco_core/posture.py:60-147`;
`varco_core/varco_core/di/report.py:140-200,753-957`; `varco_core/varco_core/tenancy/provider.py:60-98`;
`varco_sa/varco_sa/modules.py:40-255`; `varco_sa/varco_sa/provider.py:176-201`;
`technical_docs/features/security-posture.md` (grep: severity table `:286-346`);
`technical_docs/features/multitenancy.md:225-269`; `testkit/varco_conformance/COVERAGE.md` (grep);
`Makefile:333-391` (grep); `scripts/integration_tests.sh:20-33,81-88`; `uv.lock` (grep). Installed
third-party source (the U-8 discipline — source, not docs): **PyMongo 4.16.0**
`pymongo/asynchronous/client_session.py:440-560,690-890,1017-1028`, `pymongo/errors.py:34-52,100-107,183-193`,
`pymongo/asynchronous/mongo_client.py:1274-1283,1390-1392`; **Beanie 2.0.1**
`beanie/odm/documents.py` (grep `session`), `odm/interfaces/find.py` (grep), `odm/queries/find.py:108-152,936-975`,
`odm/interfaces/session.py:11-19`; **testcontainers 4.14.2** `testcontainers/mongodb/__init__.py` (whole).
Research: [021 §B](../design/research/021-transactions-conditional-requests-streaming.md) (`:65-81`), options
table `:155-157`, version notes `:169-170`, evidence gaps `:178-186`.

> ⚠️ **Seven premises checked against source before anything was decided.**
> (1) **`BeanieSettings(transactional=True)` has never put a single write inside a transaction.**
> `BeanieRepositoryProvider.make_uow()` builds every repository with
> `lambda _s, m=mapper: AsyncBeanieRepository(mapper=m)` — the session argument is discarded
> (`provider.py:119-122`) — and `AsyncBeanieRepository` takes no session at all ("No session
> injection", `repository.py:38`; no `session=` on any call, `:127-508`). `BeanieUnitOfWork` starts a
> session and a transaction (`uow.py:78-95`) that no operation ever uses. (2) **On a standalone server
> it never failed either.** PyMongo's `start_transaction()` is client-side only (state `STARTING`,
> `client_session.py:751-789`), and `commit_transaction()` on a transaction that sent no command
> returns without contacting the server (`STARTING`/`COMMITTED_EMPTY` → no command, `:800-803`). So
> 3.x users with `transactional=True` on a standalone node saw no error and got no atomicity — fixing
> the flag in place would *start* failing them. (3) **The documented outbox recipe does not run.**
> `varco_beanie/outbox.py:44-55` writes `BeanieOutboxRepository(session=uow.session)`, but
> `BeanieUnitOfWork` exposes only `_session` (`uow.py:76`) — `AttributeError`. (4) **The shared
> `mongo_url` fixture is a standalone node.** `MongoDbContainer("mongo:7")` (`conftest.py:70-73`) sets
> root credentials and no `--replSet` (`testcontainers/mongodb/__init__.py:50-78`); multi-document
> transactions are refused there. (5) **`varco_beanie` already uses PyMongo's native
> `AsyncMongoClient`, not Motor** (`uow.py:6-31,39`; `pyproject.toml:24-30`; brief 021 `:69` "Beanie
> 2.0.1 now uses PyMongo's native `AsyncMongoClient`"). `motor` 3.7.1 is in the lock only through
> `varco-casbin`'s `beanie` extra (`uv.lock:2866-2870,2896`) — no driver migration is needed here.
> (6) **`AsyncMongoClient` compares and hashes by connection properties, not identity**
> (`mongo_client.py:1274-1283`, `eq_props()`): two separately constructed clients for the same URL
> are `==` and hash equal, yet a session may be used only with the client that started it
> (`:1390-1392`). 062's suggestion "`transaction_key` = the `AsyncMongoClient`" (`:679`) must therefore
> be an **identity** key, or two distinct clients would share one session. (7) **Beanie 2.0.1 accepts
> `session=` on every call the repository makes** — `Document.get/insert/insert_many/replace/save/delete`
> (`documents.py:248-251,325-329,435-438,469-472,552-554,874-876`), `find/find_one/find_all`
> (`interfaces/find.py:59-63,212-219,286-292`), `FindMany.delete` and `FindOne.replace_one`
> (`queries/find.py:112-130,939-963`) — and `set_session(None)` keeps an already-set session
> (`interfaces/session.py:11-19`), so `find(f, session=s).count()/.delete()/.update()` inherit it.
> Brief 021 §B does not cover `session=` threading; this is from source.

## Goal

After this plan, `BeanieRepositoryProvider` **is a `TransactionalResource`**. Inside a
`transaction()`/`@transactional`/`run_in_transaction` boundary every Beanie service call, every
`provider.make_uow()` and every store handed `uow.session` (outbox, inbox, saga, conversation)
**joins one MongoDB transaction** on a manually driven `AsyncClientSession`. 062's owner loop
retries the whole body on `TransientTransactionError`-labelled failures. `UnknownTransactionCommitResult`
is retried by commit alone, inside the resource, and never re-runs the body. Both retries are
bounded by the driver spec's 120 s window. `IsolationLevel` maps onto read/write concerns:
`READ_COMMITTED` becomes majority/majority, `REPEATABLE_READ` becomes snapshot/majority, and
`SERIALIZABLE` is refused. `NESTED` is refused (no savepoints). A standalone server is refused loudly
at the boundary's first enlistment and reported at startup by a non-blocking posture finding.
Outside a boundary, Beanie behaves exactly as it does in 3.x. `BeanieSettings.transactional=True`
now raises instead of silently doing nothing. A single-node replica-set test container proves all
of this against a real server. `transactions.md` gains a Mongo section and Pitfalls table, and every
snapshot gate is green.

## Non-goals

- **Redesigning anything in 062** — no new core exception, propagation value, `TransactionOptions`
  field, or seam method. Where the core's contract is ambiguous, Step 0 reads the landed code and
  this plan adapts (⚠️ A6/A7).
- **A Motor → PyMongo driver migration** — already done for `varco_beanie` (premise 5).
  `varco_casbin`'s Motor-based Beanie adapter is untouched (Open Question 7).
- **Per-UoW Mongo transactions outside a boundary** (making `transactional=True` actually work as
  SA's always-transactional standalone UoW) — rejected (§D-default); parked.
- **Beanie `use_revision`/`RevisionIdWasChanged`** (brief 021 `:79-81`) — 061 rejected it for
  `row_version` (061 `:156`); not revisited.
- **Framework stores enlisting** — `BeanieJobStore`, `BeanieIdempotencyStore`,
  `BeanieDeadLetterQueue`, the audit/webhook repositories build no UoW and never join (062
  §D-outbox-audit). Enqueue-iff-commit is `on_commit(...)` or the outbox (069a R6).
- **A Beanie `IUoWProvider` for `TenantIsolation.DATABASE`** — none ships (Recipe 4 hands the app
  Document clones, `multitenancy.md:232-245`; `BeanieTenantPool.peek()` returns a binding, not a
  provider, `tenancy/pool.py:139-141`). Cross-tenant joining is refused by 062's tenant rule either
  way (§D-tenancy).
- **Making a directly injected `AsyncRepository[D]` (`bind_repositories()`) join** — on Beanie it is
  session-less (`di.py:289-294`), on SA it opens its own session (`varco_sa/provider.py:176-181`);
  062 left both non-joining. Documented as a Pitfall; cross-backend follow-up is Open Question 4.
- **Enforcing `read_only` on Mongo** — MongoDB has no read-only transaction mode;
  `capabilities.read_only=False` ("hint accepted and ignored", 062 `:634`).
- **A lifetime-limit integration test** (`setParameter transactionLifetimeLimitSeconds`) — it would
  mutate the shared container's server state; unit-tested by classification instead (⚠️ A3).
- **Editing `BACKLOG.md`** — the coordinator rolls it up.

## Design

### §D-driver — manual sessions, not `with_transaction()`

`open_transaction(options)` does `session = client.start_session()` (sync — **never awaited**, the bug
recorded at `uow.py:13-31`) then `await session.start_transaction(read_concern=…, write_concern=…,
read_preference=ReadPreference.PRIMARY)`. Commit/abort/end are `commit_transaction()`/
`abort_transaction()`/`end_session()`, all awaited.

```text
DESIGN: manual start_transaction/commit_transaction over session.with_transaction(callback)
  ✅ Structural fit: 062 enlists lazily, on the first make_uow() INSIDE the body (§D-model). When
     open_transaction() runs there is no callable that "is the transaction body" to hand to
     with_transaction() — wrapping it would need 062's rejected eager-begin design.
  ✅ No double retry: with_transaction() re-runs its callback on TransientTransactionError for up to
     120 s on its own (client_session.py:705-749; brief 021 :71-77). Under 062's owner loop that would
     multiply (retry=TRANSACTION_RETRY → up to 3 × 120 s), and with 062's default retry=None the
     driver would STILL re-run the body — silently breaking 062's "retry is opt-in" DESIGN (:352-366),
     which exists because a body may hold non-idempotent side effects.
  ✅ The core owns attempts: on_commit hooks are discarded per attempt and TransactionContext.attempt
     counts attempts (062 :250-257). Driver-internal re-runs would be invisible to both.
  ✅ Backoff: PyMongo 4.16's with_transaction loop has no sleep at all (:705-749). 062's RetryPolicy
     has exponential backoff with jitter.
  ✅ What with_transaction gets right is kept, as two explicit rules: commit-only retry on
     UnknownTransactionCommitResult (excluding MaxTimeMSExpired), and the 120 s window (§D-classify).
  ❌ varco re-implements ~20 lines of the Convenient Transactions API spec. Pinned by unit tests
     against the spec's own cases.
  Rejected: with_transaction() per boundary with the core's retry disabled for Mongo — needs eager
  begin and a backend-owned body. That is a redesign of 062, not an implementation of it.
```

### §D-seam-impl — `varco_beanie/varco_beanie/transaction.py` (new)

```python
MONGO_RETRY_TIME_LIMIT_SECONDS: Final[float] = 120.0   # transactions-convenient-api spec;
                                                       # client_session.py:468-472
MONGO_ISOLATION_CONCERNS: Final[Mapping[IsolationLevel, tuple[ReadConcern, WriteConcern]]]  # §D-isolation
MONGO_TRANSIENT_REASONS: Final[Mapping[int, str]] = {112: "write_conflict", 251: "transaction_aborted"}

class _ClientKey:            # frozen, identity semantics: __eq__ is `is` on the client,
                             # __hash__ = id(client) — premise 6
class BeanieResourceTransaction(ResourceTransaction):
    handle -> AsyncClientSession
    flush()      # no-op: Beanie/PyMongo send writes immediately, nothing is buffered
    commit()     # commit-only retry loop (§D-classify); never surfaces UnknownTransactionCommitResult
                 # to the body loop while inside the window
    rollback()   # abort_transaction() iff session.in_transaction; idempotent (PyMongo swallows abort
                 # errors per the spec, client_session.py:853-857)
    close()      # idempotent end_session() — aborts an in-progress transaction (:521-538)
    savepoint()  # raises TransactionConfigurationError (unreachable: capabilities.savepoints=False)
    classify(exc) = classify_chain(exc, self._match)        # §D-classify
class BeanieJoinedUnitOfWork(JoinedUnitOfWork):
    _bind(rtx)   # repos = {attr: factory(rtx.handle)}; exposes `.session` = rtx.handle
def classify_mongo_error(exc, *, age_seconds: float) -> TransientTransactionError | None  # the matcher
```

`BeanieRepositoryProvider(RepositoryProvider, TransactionalResource)` (`provider.py`):
`transaction_key` → `_ClientKey(self._client)`; `transaction_capabilities` →
`TransactionCapabilities(isolation_levels=frozenset({READ_COMMITTED, REPEATABLE_READ}),
savepoints=False, read_only=False)`; `open_transaction(options)` → refuse an unmapped isolation
(`SERIALIZABLE`, Mongo-specific message) → `await self._ensure_transactions_supported()` (§D-standalone)
→ session + `start_transaction` (`end_session()` in `finally` if `start_transaction` raises) →
`BeanieResourceTransaction(session, started_at=time.monotonic())`. `make_uow()`:
`current_transaction()` set → `BeanieJoinedUnitOfWork(tx, self, repo_factories=…)`, else
`BeanieUnitOfWork(repo_factories)` (§D-default). The repo factories become
`lambda s, m=mapper: AsyncBeanieRepository(mapper=m, session=s)`. The `_s` discard of premise 1 is
gone.

No new top-level `varco_beanie` name: like `SQLAlchemyRepositoryProvider` (062 `:215-216`), the
provider *is* the resource. The new classes live in `varco_beanie.transaction` and
`varco_beanie.topology` for backend authors and hand-wired apps.

### §D-session — the session travels by repository constructor, every call passes it

`AsyncBeanieRepository.__init__(mapper, *, session: AsyncClientSession | None = None,
assert_tenant_filter=False, tenant_field="tenant_id")`. **Every** driver call passes
`session=self._session`: `find_one`/`get` (`find_by_id`, `:140-146`), `find_all` (`:159`), `insert`
and the update write (`save`, `:176-182`, including 061's conditional `find_one(…).replace_one(…)`),
`delete` and the re-fetch `get` (`:199-208`, including 061's conditional delete), `find` in
`find_by_query`/`count`/`exists`/`delete_many`/`update_many_by_query`/`stream_by_query`
(`:239,277,305,308,401,451,491`), and `insert_many`/`save` in `save_many` (`:354,361`).
`session=None` is Beanie's own default, so the standalone path stays byte-identical.

```text
DESIGN: constructor-injected session over an ambient lookup inside the repository
  ✅ Explicit and testable: one behavioural test (Step 1b) asserts session= on every driver call.
     The failure it guards — one forgotten kwarg silently writing outside the transaction — is
     exactly ACID-1's bug class.
  ✅ Same shape every Beanie store already has (BeanieOutboxRepository(session=...), outbox.py:234-246).
  ✅ Detached UoWs stay detached: stream() (062 §D-stream) builds its repos with session=None. An
     ambient lookup would pull the boundary's session into a stream that outlives the boundary.
  ✅ Joining stays where 062 put it, at make_uow() (062 §D-model). A repository never decides on
     its own to join.
  ❌ Every new repository method must remember the kwarg. Guarded by the test and a CLAUDE.md rule line.
  Rejected: AsyncBeanieRepository reading current_transaction() itself — a repository built outside
  a boundary (bind_repositories) would join silently inside one, and stream() would join.
```

### §D-isolation — `IsolationLevel` onto read/write concern; no SERIALIZABLE

| `IsolationLevel` | Mongo transaction options | Why |
|---|---|---|
| `None` | inherit the client/session defaults (nothing passed) | 062: `None` = backend default (`:174`, `:331`) |
| `READ_COMMITTED` | `readConcern("majority")`, `writeConcern("majority")` | reads see only majority-committed data, never another transaction's uncommitted write; per-shard snapshots on a sharded cluster, which READ COMMITTED permits |
| `REPEATABLE_READ` | `readConcern("snapshot")`, `writeConcern("majority")` | one consistent snapshot for the whole transaction, across shards. Snapshot isolation is what Postgres's REPEATABLE READ also is. `snapshot` guarantees majority-committed data only when paired with w:majority |
| `SERIALIZABLE` | **`TransactionConfigurationError`** at `open_transaction()` | MongoDB has no serializable isolation: write skew is possible under snapshot |

Read preference is always `PRIMARY` inside a transaction (reads in a multi-document transaction
must target the primary; inheriting a client-level `secondaryPreferred` would fail every read).

```text
DESIGN: map onto concerns, refuse SERIALIZABLE, never downgrade
  ✅ Honest against 062's rule "unsupported level → raise at open_transaction(), never a silent
     downgrade" (:336-337). Each mapping is at least as strong as the ANSI level it names. Postgres
     itself upgrades READ UNCOMMITTED.
  ✅ READ_COMMITTED and REPEATABLE_READ keep the same meaning on both backends (REPEATABLE READ is
     snapshot isolation on Postgres too), so a use case written for SA keeps its guarantees on Mongo.
  ✅ Mongo has NO SERIALIZABLE. Snapshot isolation lets two transactions each read A,B and write
     disjoint documents (write skew). The refusal message gives the two remedies: write a guard
     document both transactions touch (turns the skew into a WriteConflict, which is retried), or
     rely on 061's row_version predicate.
  ❌ `None` inherits the client's read concern (usually "local"). Reads can then include writes not
     yet majority-replicated, which a failover can roll back. Documented: pass READ_COMMITTED or
     REPEATABLE_READ for durable reads (Open Question 3).
  ❌ The concern semantics are MongoDB-documented, not in brief 021 (⚠️ A8). Step 7(e) pins
     REPEATABLE_READ's snapshot behaviour end to end; READ_COMMITTED's cross-read behaviour is not
     asserted.
```

### §D-classify — labels, codes, commit-only retry, the 120 s window

`classify()` = `classify_chain(exc, matcher)` — 062's helper. It returns `None` first if any
`OPTIMISTIC_LOCK_ERRORS` member is in the chain, so 061's `StaleEntityError`/`ConcurrencyConflictError`
are never retried, even when wrapped. The matcher is evaluated per chain link; first match wins:

| Link | Result |
|---|---|
| `PyMongoError` with label **`UnknownTransactionCommitResult`** (with or without `TransientTransactionError`) | `None` — the commit may have applied; re-running the body could apply it twice. `commit()` already retried the commit itself |
| label **`TransientTransactionError`**, attempt age ≥ `MONGO_RETRY_TIME_LIMIT_SECONDS` | `None` (+ `add_note()` naming the window) |
| label `TransientTransactionError`, `code == 112` (WriteConflict) | `TransientTransactionError(reason="write_conflict")` |
| label `TransientTransactionError`, `code == 251` (NoSuchTransaction — failover, or the server's `transactionLifetimeLimitSeconds`, default 60 s, expired) | `TransientTransactionError(reason="transaction_aborted")` |
| label `TransientTransactionError`, other | `TransientTransactionError(reason="transient")` |
| anything else — `DuplicateKeyError` (11000), `IllegalOperation` (20, "transactions only on a replica set"), unlabelled `OperationFailure`, `InvalidOperation` | `None` |

**Commit-only retry** (`BeanieResourceTransaction.commit()`): loop `await
session.commit_transaction()`. On a `PyMongoError` labelled `UnknownTransactionCommitResult` whose
`code != 50` (MaxTimeMSExpired), while `time.monotonic() - started_at < 120`, call it again. PyMongo
supports re-commit: state `COMMITTED` → `IN_PROGRESS`, `client_session.py:806-809`, and later
attempts are upgraded to w:majority, `:887-889`. Outside the window or for code 50, re-raise unchanged
with `add_note("commit outcome unknown after <n> attempts; the transaction may have committed")`.
A `TransientTransactionError` label on commit is re-raised as is: 062's owner loop classifies it and
re-runs the whole body. That is the `break` branch of `with_transaction`, `:741-745`.

```text
DESIGN: the 120 s window is per attempt (from start_transaction), not cumulative across attempts
  ✅ The driver spec's 120 s exists to stop with_transaction's otherwise-unbounded loops. 062's
     whole-body loop is already bounded by RetryPolicy.max_attempts, so the window's remaining job is
     (a) bounding the commit-only loop, which has no count, and (b) not re-running an attempt that
     itself outlived it. Twice the server's 60 s default lifetime means a re-run cannot succeed under
     default settings.
  ✅ Needs nothing from 062 beyond the seam: started_at is the resource transaction's own state.
     Cumulative timing would need a chain start that survives attempts, which TransactionContext does
     not promise.
  ❌ classify() is no longer a pure function of exc: it also reads the attempt's age. The age is fixed
     at failure time to within the owner loop's synchronous step. Documented in its docstring.
  ❌ Worst case with TRANSACTION_RETRY: 3 attempts × ≤120 s. Pitfall: keep boundaries short.
```

`UnknownTransactionCommitResult` exhausted → a raw `PyMongoError` escapes the boundary. It is not a
`ServiceException`, so the response is 500. No `on_commit` hook runs (062 discards hooks when commit
raises), so no event is published for possibly committed data. Use the outbox for events that must
follow data. Open Question 5.

### §D-standalone — refuse at first enlistment, report at startup (posture INFO)

**Detection** (`varco_beanie/varco_beanie/topology.py`, new): `MongoTopology` (`STANDALONE` |
`REPLICA_SET` | `SHARDED`) and `async probe_topology(client)` = one `await
client.admin.command("hello")`. It returns `REPLICA_SET` if `"setName" in reply` (any replica-set
member, including a `directConnection=true` connection to a single-node set) and `SHARDED` if
`reply.get("msg") == "isdbgrid"` (mongos; also what a load-balanced deployment answers, ⚠️ A1).
Anything else is `STANDALONE`. `supports_transactions` is `topology is not STANDALONE`.

**Refusal:** `BeanieRepositoryProvider._ensure_transactions_supported()` probes once per provider and
caches the result, positive or negative. A probe that *raises* (connection failure) is not cached and
propagates unchanged. No lock: two concurrent first probes are benign duplicates, which keeps
CLAUDE.md's lazy-lock rule moot. On `STANDALONE`, `open_transaction()` raises
`TransactionConfigurationError`: *"MongoDB at <hosts> is a standalone server; multi-document
transactions need a replica set or sharded cluster. A single-node replica set is enough
(`mongod --replSet rs0` + `rs.initiate()`; connect with `?directConnection=true`) — see
transactions.md §MongoDB."* Because enlistment is lazy, this fires at the first Beanie call inside a
boundary. A standalone app that never opens a boundary is unaffected.

**Startup report:** `MongoTransactionPostureCollector(PostureCollector)`, `name =
"beanie.transactions"`, contributed by `BeanieModule._register()` via `contribute()` (never a scanned
`@Singleton`). An async lifecycle component `beanie.transactions_posture` refreshes it at
`TRANSACTION_POSTURE_PHASE = Phase.INFRA + 50`. This mirrors `SAModule`'s `RlsPostureCollector` and
`sa.rls_posture` exactly (`varco_sa/modules.py:48-92,146-180,182-210,225-233`). `collect()` returns
one finding, check id **`mongo.transactions_unsupported`**: `INFO` when the probe saw a standalone
server, `NOT_ASSESSED` when the probe raised or never ran (fixed detail text, never the exception
string — `posture.py:85-86`), and `[]` on a replica set/sharded cluster or when no `BeanieSettings`
is resolvable (no Mongo, nothing to say). It never blocks, in any environment.

```text
DESIGN: fail at first enlistment + a non-blocking posture finding
  ✅ The error appears where the misuse is (the first Beanie call in a boundary) and names the fix.
     Standalone deployments that never open a boundary — common in dev and small prod — keep working.
  ✅ The probe precedes any write. Relying on the server's own refusal (code 20) would surface only at
     the first operation, because start_transaction() sends nothing (premise 2). Classification maps
     code 20 to None anyway (unlabelled).
  ✅ Posture, not diagnose: build_wiring_report() is a pure read over the container with no backend
     contribution seam (report.py:753-815). The only module hook, VarcoModule.validate(), is
     synchronous, has no I/O, and is reported as WARNING (:792-802), which strict_validation would
     turn into a startup refusal for EVERY standalone app, including those that never open a boundary.
     Posture already has the "module contributes a sync collector, an async component refreshes it"
     pattern (SAModule) and a non-blocking INFO severity.
  ✅ A new `mongo.*` family owned by BeanieModule, not `data.*`: CLAUDE.md fixes "SAModule owns
     data.*" (Plan 056), and one-owner-per-family keeps that rule true.
  ❌ "Security posture" now reports a data-integrity capability. Precedent: data.rls_enabled_without_policy
     is a "silent outage" check (security-posture.md:346).
  ❌ One extra `hello` per process (posture + provider caches are independent). Accepted over
     coupling the collector to the provider's resolution.
  Rejected: refusing at provider init/startup (breaks every standalone app); a WARNING wiring finding
  (blocks strict startups for apps that never use transactions); a HealthCheck status (a deployment
  fact, not liveness — it would flap dashboards).
```

### §D-default — `BeanieUnitOfWork`, `transactional`, and the default

- **`BeanieUnitOfWork` (standalone) never opens a session or a transaction.** 3.x opened a session no
  repository used (premise 1), so dropping it is byte-identical on the data path. New signature
  `BeanieUnitOfWork(repo_factories)`; repos are built with `session=None`; `commit()`/`rollback()`
  are no-ops; it gains a read-only `session` property returning `None`. Inside a boundary,
  `make_uow()` returns `BeanieJoinedUnitOfWork`, whose `session` is the boundary's. The documented
  outbox recipe `BeanieOutboxRepository(session=uow.session)` now works on both paths: immediate
  insert standalone, joined insert inside a boundary (fixes premise 3).
- **`BeanieSettings.transactional` stays as a field (default `False`, no effect); `True` raises**
  `TransactionConfigurationError` in `__post_init__`: *"BeanieSettings.transactional=True never
  wrapped repository writes in a transaction in 3.x (the session was not passed to Beanie). In 4.0
  open a boundary instead: transaction() / @transactional — see 3-to-4.md §Ambient transactions."*
  The field is documented deprecated, **removal in 5.0.0, not before 2027-10 (12-month floor from
  4.0.0)**.
- **The default stays "no transaction"** outside a boundary — exactly what 3.x actually did.

```text
DESIGN: keep the field (False inert, True raises) over the alternatives
  ✅ Ends the lie at upgrade time: an app that believed it had atomic UoWs learns it never did, gets
     the remedy, and has a real mechanism (with retry and isolation) to move to.
  ✅ Keeps every BeanieConfig/BeanieSettings(…, transactional=False) call site constructing. That is
     AB-4's promise for the deprecated BeanieConfig alias (test_beanie_config_alias.py:79-91), whose
     removal floor is 2027-08-31. Deleting the field would break alias call sites before the floor.
  ✅ One mechanism for a Mongo transaction: the ambient boundary.
  ❌ Breaks startup for configs with transactional=True — including two repo tests that set it
     casually (test_beanie_bootstrap.py:50-70). Allowed (BACKLOG.md:31) and documented in 3-to-4.md.
  ❌ A field whose only live value raises, kept for one major. Accepted for the alias floor.
  Rejected — honour it (standalone transactional UoW, SA parity): a second transaction path with no
     retry; standalone users who set it start failing; and since 062's open_detached_uow() calls
     make_uow(), stream()'s detached UoW would become a transaction, killing every 067 export longer
     than the server's 60 s transaction lifetime.
  Rejected — deprecate and ignore (warn): keeps a flag named "transactional" that delivers no
     transaction for another year, and Python hides DeprecationWarning by default outside __main__.
     Open Question 1.
  Rejected — delete the field now: breaks BeanieConfig alias call sites before their floor.
  Rejected — flip the default to True: every Beanie app would need a replica set.
```

### §D-tenancy — key = client identity; one tenant per boundary

- `transaction_key = _ClientKey(client)` (identity, premise 6). Two providers on **one** client share
  one session, even with different `db_name`s: a replica-set transaction may span databases (⚠️ A5,
  proven by Step 7(i)). Two **distinct** clients, even to the same URL, are distinct resources →
  062's one-resource rule raises.
- **`DATABASE` isolation** (`BeanieTenantPool`): with a shared client the key is equal across tenants
  but 062's tenant rule (tenant fixed at first enlistment, 062 `:519-522`) refuses the second tenant
  first. With `client_per_tenant=True` the keys differ and both rules refuse. A boundary can never
  span tenants. No Beanie per-tenant `IUoWProvider` ships (Non-goals); one an app builds from
  `BeanieRepositoryProvider` inherits the seam.
- **`TenantScope.GLOBAL`**: `GlobalUoWProvider` returns `open_detached_uow()` (062 `:527-529`) → a
  standalone `BeanieUnitOfWork` with no session → immediate writes, outside the boundary.
- **`stream()`** is detached (062 §D-stream) → `session=None` → its cursor runs outside any
  transaction. That is required: a cursor inside a transaction dies with it, and the server's 60 s
  lifetime would cut 067 exports. 067's cursor-close fix is untouched. A *repository-level*
  `stream_by_query` on a **joined** UoW does carry the session. Iterating it after the boundary
  ended raises `InvalidOperation("Cannot use ended session")`: `_apply_to()` → `_check_ended()`,
  `client_session.py:1017-1028,546-548`. It is loud and never a silent session-less read.

### Alternatives considered

- **`with_transaction()` callback API:** rejected in §D-driver.
  ❌ structurally impossible with lazy enlistment; ❌ double retry and retry without opt-in;
  ❌ no backoff; ✅ spec-complete for free.
- **Ambient session lookup inside `AsyncBeanieRepository`:** rejected in §D-session.
  ❌ silently joins repositories built outside a boundary; ❌ pulls `stream()` into the boundary;
  ✅ no constructor change.
- **Per-UoW transactions honouring `transactional=True`:** rejected in §D-default.
  ❌ second path without retry; ❌ breaks long exports via the detached path; ✅ SA parity.
- **Standalone → diagnose WARNING via `BeanieModule.validate()`:** rejected in §D-standalone.
  ❌ `strict_validation` refuses every standalone app; ❌ `validate()` cannot do I/O; ✅ shows in
  `varco diagnose`.
- **Map `SERIALIZABLE` → snapshot (best available):** rejected.
  ❌ a silent downgrade, against 062's rule; ❌ write skew would surprise an author who asked for
  serializability; ✅ portable use cases would "just run".
- **`transaction_key` = the client object as is:** rejected.
  ❌ value equality merges distinct clients (premise 6); ✅ one less class.
- **Replace the shared `mongo_url` fixture with a replica set:** rejected.
  ❌ every Beanie integration test would depend on replica-set initiation; ❌ the standalone-behaviour
  test needs a standalone anyway; ✅ one container per session instead of two.
- **Function-scoped `*_container_fresh` replica set per transaction test:** rejected.
  ❌ ~4 s boot × ~12 tests; ❌ CLAUDE.md reserves fresh containers for tests needing a *pristine*
  server, which these do not (they namespace by database); ✅ isolation from other tests.

## Steps

All commands from `/home/edoardo/projects/varco`. One mergeable unit, after 062 Phase A (seam and
conformance suite) and 061. Every new module gets `from __future__ import annotations`, DESIGN blocks
with ✅/❌, and docstrings with Args/Returns/Raises/Edge cases/Async safety (CLAUDE.md Coding
Standards). No lock is created anywhere.

0. [ ] **Preflight** — record in `/tmp/claude-1000/-home-edoardo-projects-varco/<session>/scratchpad/063-preflight.txt`:
       (a) **Hard gate — 062 Phase A landed:**
       `uv run python -c "from varco_core.transaction import TransactionalResource, ResourceTransaction, JoinedUnitOfWork, TransactionCapabilities, classify_chain, open_detached_uow, current_transaction; from varco_conformance.transaction import TransactionalResourceConformance"`
       (run with `PYTHONPATH=testkit`). Then read the landed signatures: `JoinedUnitOfWork.__init__`
       and its abstract methods; whether the core validates `options.isolation` against
       `capabilities` *before* `open_transaction()` (avoid a double check); how a joined UoW used
       after its ROOT ends raises `TransactionClosedError` and whether a backend hook is needed
       (⚠️ A6); and the conformance suite's abstract fixtures plus its detached-handle assertion
       (⚠️ A7). If 062 Phase B landed, read `varco_sa/varco_sa/transaction.py` and mirror its names.
       (b) **Hard gate — 061 landed:** `rg -n '# 063' varco_beanie/varco_beanie/repository.py`
       non-empty; `uv run python -c "from varco_core.exception import OPTIMISTIC_LOCK_ERRORS"`.
       (c) **Soft gate — 067** (needed only for Step 7(j)):
       `rg -n 'batch_size' varco_beanie/varco_beanie/repository.py` non-empty. If 067 has not
       landed, write 7(j) in the commit after it — never skip or xfail it.
       (d) Re-verify premises 1–7 at their cited lines, plus `rg -n 'transactional' varco_beanie examples testkit`
       (the call-site list Step 4 updates). (e) `rg -n '^## |^### ' technical_docs/features/transactions.md technical_docs/migrations/3-to-4.md`
       to find the sections 062 wrote. (f) Baseline:
       `make lint && make test PKG=varco_beanie && uv run pytest varco_beanie/tests -m integration -q`.
       **Verify:** (a)–(f) recorded; any red in (a), (b) or (f) → stop.

1. [ ] **Unit tests, failing first (Docker-free).**
       (a) `varco_beanie/tests/test_beanie_transaction.py` (**new**), using fake
       `AsyncMongoClient`/`AsyncClientSession` doubles (`MagicMock` + `AsyncMock`; `start_session`
       a plain `MagicMock`, sync). Cases:
       - Outside a boundary, `make_uow()` returns `BeanieUnitOfWork`, `uow.session is None`,
         `start_session` is never called, and repos hold `session=None`.
       - Inside `transaction()` with the probe stubbed to `REPLICA_SET`, two `make_uow()` calls
         share one session, with one `start_session` and one awaited `start_transaction(...)`.
       - Isolation mapping: `None` → no concerns passed and `read_preference=PRIMARY`;
         `READ_COMMITTED` → majority/majority; `REPEATABLE_READ` → snapshot/majority;
         `SERIALIZABLE` → `TransactionConfigurationError` whose message names "snapshot isolation"
         and "SERIALIZABLE", with no session left open.
       - `NESTED` inside a boundary → `TransactionConfigurationError` at enlistment.
       - Probe: a `hello` reply without `setName`/`msg` → `TransactionConfigurationError`
         containing "replica set"; `setName` → ok; `msg="isdbgrid"` → ok; `hello` is awaited once
         across two boundaries; a probe raising `ConnectionFailure` propagates and is not cached
         (the next boundary probes again).
       - **Classify matrix** (labelled `OperationFailure`s built with
         `details={"errorLabels": [...]}`, `pymongo/errors.py:183-193`): 112 → `write_conflict`;
         251 → `transaction_aborted`; any other labelled code → `transient`;
         `UnknownTransactionCommitResult`, alone or together with the transient label → `None`;
         unlabelled 11000 and 20 → `None`; a labelled error after `time.monotonic` is patched past
         120 s → `None`, with a note; a labelled error chained under `StaleEntityError` → `None`;
         `error_params()` never contains the driver message.
       - `commit()`: two `UnknownTransactionCommitResult` failures then success → three
         `commit_transaction` awaits and no raise; the same failure with code 50 → re-raised after
         one await; the window exceeded → re-raised with the note; a transient-labelled commit
         failure → re-raised unchanged after one await.
       - `rollback()` and `close()` are idempotent; `close()` ends the session once; `rollback()`
         after a successful commit is a no-op.
       - `transaction_key`: two providers over one client compare equal; two providers over two
         `==`-equal clients do not (premise 6).
       - `BeanieSettings(transactional=True)` and `BeanieConfig(…, transactional=True)` →
         `TransactionConfigurationError`; `BeanieConfig(…, transactional=False)` still constructs.
       (b) `varco_beanie/tests/test_beanie_repository_session.py` (**new**): a recording fake
       Document class patched onto a mapper. For a repository built with `session=S`, every
       driver call made by `find_by_id` (scalar and composite PK), `find_all`, `save` (insert,
       unversioned update, and 061's versioned `find_one(...).replace_one(...)`), `delete` (all
       three branches), `find_by_query`, `count`, `exists`, `save_many`, `delete_many`
       (unversioned and 061's versioned `$or`), `update_many_by_query` and `stream_by_query`
       carries `session=S`, or inherits it from `find(..., session=S)`. With `session=None`,
       every call matches today's exactly.
       **Verify:** `uv run pytest varco_beanie/tests/test_beanie_transaction.py varco_beanie/tests/test_beanie_repository_session.py -q`
       → red (import/attribute errors).

2. [ ] `varco_beanie/varco_beanie/repository.py` — add the `session=` keyword (§D-session) and thread
       it into every call listed there, replacing each 061 `# 063: pass session= here` comment. Rewrite
       the class docstring's "No session injection" paragraph (`:38-39`) to say a session is passed by
       a joined UoW and that `None` keeps Beanie's sessionless default. Leave 061's
       conditional-write logic and 067's `batch_size`/cursor-close untouched; the diff on those lines
       is the added kwarg only. **Verify:** Step 1(b) green; `rg -n '# 063' varco_beanie/varco_beanie/repository.py`
       → 0; `uv run pytest varco_beanie/tests -q` green, with every existing test unmodified.

3. [ ] `varco_beanie/varco_beanie/topology.py` (**new**: `MongoTopology`, `probe_topology()`) and
       `varco_beanie/varco_beanie/transaction.py` (**new**: §D-seam-impl constants, `_ClientKey`,
       `BeanieResourceTransaction`, `BeanieJoinedUnitOfWork`, `classify_mongo_error()`), each with
       DESIGN blocks carrying §D-driver, §D-isolation and §D-classify. **Verify:** the classify,
       commit, rollback/close and isolation-table cases of Step 1(a) green, exercising
       `BeanieResourceTransaction` directly; `uv run mypy varco_beanie/varco_beanie/transaction.py varco_beanie/varco_beanie/topology.py`
       clean.

4. [ ] **Provider, UoW and settings.** `varco_beanie/varco_beanie/provider.py`: the provider
       subclasses `TransactionalResource` and gains `transaction_key`,
       `transaction_capabilities`, `open_transaction()` and the cached
       `_ensure_transactions_supported()`; `make_uow()` returns the joined or standalone UoW;
       the session-taking repo factories. Update the class docstring ("Connection details …
       and transaction flag" → the boundary). `varco_beanie/varco_beanie/uow.py`: new
       `BeanieUnitOfWork(repo_factories)` per §D-default, keeping the motor→pymongo DESIGN
       table (`:6-31`) as history for `BeanieResourceTransaction`. `varco_beanie/varco_beanie/config.py`:
       `transactional` deprecated, `True` raises in `__post_init__`; docstring `:64-72` rewritten.
       Update the existing tests that assert the old semantics. Rewrite them rather than delete
       them, and give each a one-line docstring naming 063:
       - `test_beanie_uow.py:108-253`: the standalone UoW opens no session; `session` is `None`.
       - `test_beanie_provider.py:175-182`: "forwards the transactional flag" becomes "make_uow is
         standalone outside a boundary".
       - `test_beanie_di.py:84-103`: `True` raises.
       - `test_beanie_bootstrap.py:50-70`: drop `transactional=True` from the KI-10 construction tests.
       - `test_beanie_config_alias.py:55`: the default is still `False`.

       **Verify:** Step 1(a) fully green; `make test PKG=varco_beanie` green;
       `uv run python scripts/import_budget.py --check --warn-only` shows no `varco_beanie` regression.

5. [ ] **Posture.** Write `varco_beanie/tests/test_beanie_transaction_posture.py` (**new**,
       failing first, fake client). Cases:
       - Before `refresh()`, `collect()` returns one `NOT_ASSESSED` finding,
         `mongo.transactions_unsupported`.
       - Standalone `hello` → one `INFO` finding with a remediation naming `--replSet`.
       - Replica set or mongos → `[]`.
       - A probe raising `ConnectionFailure` → `NOT_ASSESSED` whose detail contains no exception
         text.
       - No `BeanieSettings` resolvable → `[]`.
       - `collect()` never raises.
       - `BeanieModule(settings=…)` registered on a container: `collect(container, PostureCollector)`
         contains a collector named `beanie.transactions`, and `components(container)` returns
         `beanie` then `beanie.transactions_posture` at `Phase.INFRA + 50`.

       Then implement `MongoTransactionPostureCollector` in `varco_beanie/varco_beanie/topology.py`,
       and in `varco_beanie/varco_beanie/modules.py` the `TRANSACTION_POSTURE_PHASE` constant,
       `_contribute_transaction_collector()` called from `_register()`, and `components()`.
       Follow §D-standalone, mirroring `varco_sa/modules.py:146-233`, and carry the DESIGN block
       into `BeanieModule`'s docstring. **Verify:** the new file green;
       `uv run pytest varco_beanie/tests -k "module or posture" -q` green.

6. [ ] **Replica-set test fixture.** `varco_beanie/tests/conftest.py`: add a session-scoped
       `mongo_replica_set_url`, the `mongo_url` twin. It skips unless `VARCO_RUN_INTEGRATION`
       is set, and honours an override `VARCO_TEST_MONGO_RS_URL`, stashed like `mongo_url`
       (`:64-68`). Otherwise it runs
       `DockerContainer("mongo:7").with_command("--replSet rs0 --bind_ip_all").with_exposed_ports(27017)`
       (no auth — a keyed replica set is out of scope) and waits on
       `wait_for_logs(..., "waiting for connections")` (the regex `MongoDbContainer` uses). It
       then calls `replSetInitiate` with `{"_id": "rs0", "members": [{"_id": 0, "host": "localhost:27017"}]}`
       through a sync `pymongo.MongoClient(url)`, and polls `hello.isWritablePrimary` for up to
       30 s. It yields `mongodb://<host>:<mapped port>/?directConnection=true`: the member
       advertises `localhost:27017`, which the host cannot reach, so the driver must not
       discover it. Extend the module comment (`:30-45`) with the per-test namespacing rule and
       "standalone vs replica set: use `mongo_url` to assert standalone behaviour".
       `Makefile:355-360,386-391` adds `-u VARCO_TEST_MONGO_RS_URL` to all four `env -u` lists.
       `scripts/integration_tests.sh:29-33` adds the name to the documented override list. Its
       report loop (`:88`) already matches `VARCO_TEST_*_URL`. **Verify:**
       `uv run pytest varco_beanie/tests -m integration -q` still green (the new fixture is unused
       until Step 7, so existing integration tests must be unaffected);
       `rg -c 'VARCO_TEST_MONGO_RS_URL' Makefile` → 4. The fixture itself is exercised by Step 7(m).

7. [ ] `varco_beanie/tests/test_beanie_transaction_integration.py` (**new**,
       `pytestmark = pytest.mark.integration`). Fixtures: `mongo_replica_set_url`, plus a
       per-test database `f"tx_{uuid4().hex[:8]}"`, `init_beanie` on it, and every collection
       pre-created before the test (⚠️ A4). Use `InMemoryEventBus` + `BusEventProducer`.
       Barriers are `asyncio.Event`s with explicit timeouts. If a race test flakes, widen the
       timeouts; never xfail it (CLAUDE.md).
       (a) **Two-service atomic rollback:** one `@transactional` use case calls
           `OrderService.create`, writes `BeanieOutboxRepository(session=uow.session).save(...)`,
           then `StockService.create` raises. Afterwards both collections and `varco_outbox` are
           empty and no event was published. The clean variant commits all three, and the events
           go out after commit, in order.
       (b) **A real `TransientTransactionError` retry, write conflict between two sessions:**
           - Setup: one counter document at 0. Boundary A writes it, sets `a_wrote`, registers
             `on_commit(a_committed.set)`, and awaits `b_conflicted`.
           - Boundary B, on attempt 0, awaits `a_wrote`, then increments the counter, catching
             `PyMongoError` only to set `b_conflicted` and re-raise. On attempt ≥1 it awaits
             `a_committed` first.
           - Without `retry=`, B raises `TransientTransactionError(reason="write_conflict",
             attempts=1)` whose `__cause__` is an `OperationFailure` with `code == 112` and
             `has_error_label("TransientTransactionError")` (⚠️ A2). The counter is 1.
           - With `retry=TRANSACTION_RETRY` (jitter off, `base_delay=0`), both commit, B's body
             runs exactly twice, and the counter is 2.
       (c) **`NESTED` rejection:** inside `@transactional`, a `transaction(propagation=NESTED)` +
           service call → `TransactionConfigurationError` naming savepoints. The outer boundary
           rolls back and nothing is written. Repeat with `NESTED` entered before the first
           enlistment.
       (d) **Standalone server** (the existing `mongo_url`, verified standalone): a boundary +
           `service.create` → `TransactionConfigurationError` containing "replica set", and the
           collection stays empty; the same call outside a boundary succeeds;
           `MongoTransactionPostureCollector.refresh()` then `collect()` → exactly one INFO
           `mongo.transactions_unsupported`; the same collector against `mongo_replica_set_url`
           → `[]`.
       (e) **Isolation:**
           - A `REPEATABLE_READ` boundary reads X (0). Another client sets X=1 and commits. The
             boundary re-reads X and still gets 0 (snapshot, ⚠️ A8).
           - `SERIALIZABLE` → `TransactionConfigurationError` at first enlistment, nothing written.
           - `read_only=True` + a write → the write commits (ignored hint, documented).
       (f) **061 handoff on Mongo:**
           - Setup: a versioned entity at v1; boundary B is
             `@transactional(isolation=REPEATABLE_READ, retry=TRANSACTION_RETRY)`.
           - B `get`s v1, then waits until A has updated and committed v2.
           - B's `update(..., expected_version=1)` hits a write conflict and is retried. The
             second run reloads v2 and raises `PreconditionFailedError`. Assert the body ran
             exactly twice and the document is v2.
           - If the server instead reports zero matches (⚠️ A2b), 061's `StaleEntityError` path
             gives one run and a 409. Assert the invariant that holds in both cases: ≤2 runs, a
             061 error, the document at v2. Record the observed path in transactions.md.
       (g) **`delete_many` atomic inside a boundary** (061's "until 063", `:420-422`): three
           versioned docs, one bumped concurrently. `delete_many` inside a boundary raises
           `StaleEntityError`, the boundary rolls back, and all three remain.
       (h) A `REQUIRES_NEW` inner create stays committed although the outer boundary rolls back.
       (i) **One client, two databases:** two `BeanieRepositoryProvider`s on one client with
           different `db_name`s share `transaction_key` and roll back together (⚠️ A5). A second
           client to the same URL is a distinct key, so the core's one-resource rule raises
           `TransactionConfigurationError`.
       (j) **`stream()` detached** (requires 067, Step 0(c)): inside a boundary that created X,
           prime `service.stream()`; it does not yield X. Exit the boundary, then drain the
           iterator from `asyncio.create_task`; it completes without error, and `aclose()` closes
           the cursor (067's assertion).
       (k) A repository taken from a joined UoW, used after its boundary ended, raises
           `InvalidOperation` or `TransactionClosedError` (⚠️ A6), and the collection is
           unchanged — never a silent sessionless write.
       (l) `class TestBeanieTransactionalResourceConformance(TransactionalResourceConformance)`
           with the suite's fixtures: `resource` = provider; `make_transient_error()` = a labelled
           112 `OperationFailure`; `make_non_transient_error()` = `DuplicateKeyError`. If the
           suite's detached-handle assertion assumes a non-`None` handle, override that one test
           with a docstring (CLAUDE.md "legitimate backend capability divergence", ⚠️ A7).
       (m) Fixture smoke: `hello["setName"] == "rs0"` on `mongo_replica_set_url`; `mongo_url` has
           no `setName`.
       **Verify:** `uv run pytest varco_beanie/tests/test_beanie_transaction_integration.py -m integration -q`
       green **twice in a row**; `make integration-test-clean PKG=varco_beanie` green.

8. [ ] **Store docstrings (no behaviour change).** Rewrite every "replica set, `transactional=True`"
       recipe to "inside a `transaction()` boundary, pass `uow.session`":
       - `varco_beanie/varco_beanie/outbox.py:6-27,44-64,201-212`
       - `inbox.py:44-50,197-207`
       - `saga.py:137-179`
       - `conversation.py:200-236`
       - `deduplication.py:217-253`
       - `audit.py:47,323,350`

       Add to each: "framework stores never join on their own (062 §D-outbox-audit)".
       **Verify:** `rg -n 'transactional=True' varco_beanie/varco_beanie` → only the
       `config.py` refusal and its test reference.

9. [ ] **Docs.** One commit with the code (CLAUDE.md same-commit rule).
       (a) `technical_docs/features/transactions.md` (created by 062): new section
           **"MongoDB / Beanie (Plan 063)"**, covering:
           - the capabilities table;
           - the §D-isolation table and its DESIGN summary, including **no SERIALIZABLE** and the
             write-skew remedies;
           - the replica-set requirement, with the local-dev recipe
             (`docker run mongo:7 --replSet rs0` + `rs.initiate()` + `?directConnection=true`)
             and the posture finding;
           - the §D-classify table, commit-only retry, and the 120 s/60 s limits;
           - why manual sessions, not `with_transaction`;
           - session propagation (`uow.session`, which stores join and which never do);
           - tenancy (key = client identity, DATABASE, GLOBAL) and streaming;
           - the observed 7(f) path;
           - **a Mongo Pitfalls table.** Rows:
             - standalone server;
             - `SERIALIZABLE` refused, and write skew under `REPEATABLE_READ`;
             - a boundary open longer than `transactionLifetimeLimitSeconds` (60 s);
             - `NESTED` refused;
             - a direct Beanie call or a store built with `session=None` inside a boundary runs
               outside it;
             - an `AsyncRepository[D]` from `bind_repositories()` never joins (both backends);
             - unknown commit outcome → 500, no hooks;
             - `read_only` ignored;
             - first write to a missing collection inside a snapshot transaction (⚠️ A4);
             - hot-document write conflicts without `retry=` → 503s;
             - `BeanieSettings(transactional=True)` raises;
             - `enqueue()`/job/idempotency/DLQ stores never join → `on_commit()` or the outbox.

           Extend the "Implementing a backend" section with the `_ClientKey` identity lesson.
       (b) `technical_docs/features/security-posture.md`: a severity-table row for
           `mongo.transactions_unsupported` (INFO / NOT_ASSESSED, 063, never blocks, remedy:
           replica set) and a collector-table row for `beanie.transactions` (contributed by
           `BeanieModule`).
       (c) `README.md` "Transactions" (062's section): a Beanie snippet — two services and an outbox
           write in one `@transactional(retry=TRANSACTION_RETRY)` on a replica set — plus a
           one-line replica-set note.
       (d) `ARCHITECTURE.md`: the varco_beanie entry lists `transaction.py`/`topology.py`;
           `BeanieRepositoryProvider` appears under 062's "Transactions" hierarchy as a
           `TransactionalResource`.
       (e) `CHANGELOG.md` `[Unreleased]`:
           - **Added:** the Beanie/MongoDB backend for ambient transactions; the
             `mongo.transactions_unsupported` posture finding; `BeanieUnitOfWork.session`.
           - **Changed:** `BeanieUnitOfWork(repo_factories)` no longer opens a session.
           - **Deprecated:** `BeanieSettings.transactional`, removal 5.0.0.
           - **Breaking:** `transactional=True` raises.
           - **Fixed:** sessions now reach Beanie, and the `uow.session` outbox recipe works.
       (f) `technical_docs/migrations/3-to-4.md`: append a **"MongoDB / Beanie"** subsection to
           062's "Ambient transactions" section, with these items:
           1. `transactional=True` now raises — why, and what replaces it;
           2. a boundary needs a replica set; standalone → error at the first call plus the
              INFO finding;
           3. `SERIALIZABLE` and `NESTED` are refused on Mongo;
           4. `BeanieUnitOfWork`'s constructor changed, and `uow.session` exists;
           5. pass `uow.session` to outbox/inbox/saga stores.

           Fix in-file `§` references (`rg -n '§[0-9]+\b' technical_docs/migrations/3-to-4.md`).
       (g) `testkit/varco_conformance/COVERAGE.md`: the `transaction` row's Beanie entry →
           "subclassed — `varco_beanie/tests/test_beanie_transaction_integration.py` (replica-set
           container)", plus any 7(l) override recorded as a capability divergence.
       (h) `technical_docs/features/multitenancy.md`: one line in Recipe 4 (`:232-249`) saying a
           boundary never spans tenants and no Beanie per-tenant provider ships.
       (i) `technical_docs/common-pitfalls.md`: a row "Beanie driver call without `session=`
           inside a boundary → silently non-transactional", fixed by threading
           `session=self._session` and guarded by `test_beanie_repository_session.py`.
       (j) `CLAUDE.md`:
           - Commands/chaos paragraph: "six-`VARCO_TEST_*_URL`" → "seven".
           - Test Conventions: add `mongo_replica_set_url` to the session-fixture list and
             `VARCO_TEST_MONGO_RS_URL` to the override examples.
           - Under 062's ambient-transactions block, one rule line: "Rule (Plan 063): every
             `AsyncBeanieRepository` driver call passes `session=self._session`; a Mongo boundary
             needs a replica set (standalone → `TransactionConfigurationError` at first
             enlistment); `NESTED`/`SERIALIZABLE` refused — see transactions.md §MongoDB."

       **Verify:** `make docs` green; `rg -n 'six-.VARCO_TEST' CLAUDE.md` → 0.

10. [ ] **Gates.** Run `uv run python scripts/api_surface.py`: expect no diff (no top-level name
        added; class signatures are not recorded). If one appears, inspect it and commit the
        snapshot. `make entry-points`: expect a diff only if `BeanieModule`'s components or
        DESIGN text are rendered; commit it. `make di-catalogue`: if a `TransactionalResource`
        row appears for the Beanie provider, it must match 062's SA precedent from Step 0(a);
        commit it. `make settings-reference`: expect no diff (`BeanieSettings` is a dataclass,
        not a `bind_settings()` class); assert it. Then `make lint`, `make type-check`,
        `make test`, `make integration-test-clean PKG=varco_beanie`. **Verify:** every `--check`
        green; `git diff --stat` limited to the files named in Steps 1–9 plus regenerated
        snapshots.

## Edge cases

- No boundary anywhere → byte-identical to 3.x on the data path: no session, sessionless driver
  calls, immediate writes, and `stream()` unchanged.
- First Beanie call inside a boundary against a standalone server → `TransactionConfigurationError`
  naming the replica-set fix; nothing written; the boundary rolls back (an empty rollback).
- A boundary that never touches Beanie → no probe, no session (062's lazy enlistment).
- `hello` probe fails (network) → the error propagates unchanged and is not cached; the next boundary
  probes again. The posture finding is `NOT_ASSESSED`.
- `isolation=SERIALIZABLE` → refused before any session opens. `READ_COMMITTED`/`REPEATABLE_READ`
  map onto majority/majority and snapshot/majority. `None` inherits the client's defaults.
- `NESTED` with an ambient boundary → `TransactionConfigurationError` at enlistment. `NESTED` with no
  boundary → ROOT (062), allowed.
- Write conflict (112) → `write_conflict`, retried iff the root opted in. Otherwise →
  `TransientTransactionError` → 503 + `Retry-After` (062).
- Transaction aborted by failover or the lifetime limit (251) → `transaction_aborted`, retried while
  the attempt is under 120 s old. An older attempt is never retried.
- `UnknownTransactionCommitResult` on commit → the commit alone is retried for up to 120 s (not on
  code 50). If it is still unknown, the raw error → 500, no hooks run, and the body is never
  re-run.
- `TransientTransactionError` label on commit → the whole body is re-run by 062's loop.
- `StaleEntityError` from 061's conditional write inside a boundary → never transient, so the
  boundary dooms. `delete_many` with a stale entity inside a boundary → nothing deleted.
- A WriteConflict caught and swallowed by user code inside the body → the server already aborted;
  062's doom mark makes the root raise `TransactionRolledBackError`.
- `read_only=True` → accepted and ignored; writes succeed.
- Two providers on one client with different databases → one transaction. Two clients to the same
  URL → two resources → refused.
- Tenant A then tenant B in one boundary → refused by 062's tenant rule (shared or per-tenant client).
- GLOBAL provider inside a boundary → detached, sessionless, immediate.
- `stream()` inside a boundary → detached, sessionless; it survives the boundary's exit and is
  drained from another task.
- A repository from a joined UoW used after the boundary → `InvalidOperation`/`TransactionClosedError`,
  never a sessionless write.
- `CancelledError` mid-boundary → 062 rolls back → `abort_transaction()`; `close()` ends the session.
- `BeanieSettings(transactional=True)`/`BeanieConfig(..., transactional=True)` → raises at
  construction. `False` → inert.
- `BeanieOutboxRepository(session=uow.session)` → `None` outside a boundary (immediate insert);
  inside one, the boundary's session (atomic).
- `JobQueue.enqueue()` inside a boundary → not rolled back with it. Use `on_commit(...)` or the
  outbox.

## Verification

```bash
# Prerequisites (062 Phase A + 061 landed; 067 for 7(j))
PYTHONPATH=testkit uv run python -c "from varco_core.transaction import TransactionalResource; from varco_conformance.transaction import TransactionalResourceConformance"
rg -n '# 063' varco_beanie/varco_beanie/repository.py      # before Step 2: non-empty; after: 0
# Unit (Docker-free)
uv run pytest varco_beanie/tests/test_beanie_transaction.py varco_beanie/tests/test_beanie_repository_session.py \
  varco_beanie/tests/test_beanie_transaction_posture.py -q
make test PKG=varco_beanie
# Integration (replica-set container + the standalone mongo_url container) — twice
uv run pytest varco_beanie/tests/test_beanie_transaction_integration.py -m integration -q
uv run pytest varco_beanie/tests/test_beanie_transaction_integration.py -m integration -q
make integration-test-clean PKG=varco_beanie
# Structural
rg -n 'transactional=True' varco_beanie/varco_beanie        # → config.py refusal only
rg -n 'lambda _s' varco_beanie/varco_beanie/provider.py     # → 0 (premise 1 gone)
rg -c 'VARCO_TEST_MONGO_RS_URL' Makefile                    # → 4
rg -n 'OPTIMISTIC_LOCK_ERRORS\s*[:=]' varco_*/varco_*       # → exactly 1 (061's definition)
# Gates
make lint            # ruff check + format --check + api-check + asyncapi-check + import-budget(warn)
                     # + settings-check + di-catalogue-check + entry-points-check
make type-check
make test
uv run python scripts/api_surface.py --check
uv run python scripts/import_budget.py --check --warn-only
```

## Risks

- ⚠️ **ASSUMPTION A1:** `hello`'s `setName` (replica-set member, including `directConnection`) and
  `msg == "isdbgrid"` (mongos) identify every transaction-capable topology, including a
  load-balanced deployment, which answers as mongos. Verified for replica set and standalone by
  7(d)/7(m); mongos and load balancers are not containerised here. Invariant: a false
  "standalone" verdict on a capable server must be impossible for replica sets. If a load
  balancer answers otherwise, the remedy is one `elif`.
- ⚠️ **ASSUMPTION A2:** a second transaction writing a document another open transaction has
  modified fails **immediately** with `OperationFailure` code 112 labelled
  `TransientTransactionError`, rather than waiting on a lock. **Verified by 7(b).**
  **A2b:** the same applies to a write against a document modified *after* the writer's snapshot
  (7(f)); if the server instead reports zero matches, 061's `StaleEntityError` path applies and
  7(f) asserts the invariant common to both.
- ⚠️ **ASSUMPTION A3:** a transaction aborted by `transactionLifetimeLimitSeconds` surfaces as code
  251 (NoSuchTransaction) labelled `TransientTransactionError` at the next operation or commit. This
  is not integration-tested, because changing the server parameter would affect the shared
  container. Unit-tested by classification; the 120 s window bounds re-runs whatever the code.
- ⚠️ **ASSUMPTION A4:** the first insert into a collection that does not exist yet, inside a
  `snapshot` transaction, may be refused. Tests pre-create collections; a Pitfalls row tells apps to
  create collections at startup (Beanie indexes / `BeanieIndexGuard`). Unverified either way.
- ⚠️ **ASSUMPTION A5:** a replica-set transaction can span databases on one client (two
  `db_name`s). **Verified by 7(i)**; if refused, `transaction_key` must include `db_name`, and
  the 062 one-resource rule then refuses the mix loudly.
- ⚠️ **ASSUMPTION A6:** 062's `JoinedUnitOfWork` needs no backend hook to make a used-after-exit
  joined UoW fail. Step 0(a) reads the landed code. Whatever it does, Beanie's floor is PyMongo's
  ended-session `InvalidOperation` (`client_session.py:546-548,1028`), so a sessionless write is
  impossible.
- ⚠️ **ASSUMPTION A7:** 062's conformance suite's "detached handle is distinct" test tolerates a
  `None` handle for a detached Beanie UoW. If not, one subclass override with a docstring is
  allowed; the shared assertion is never weakened.
- ⚠️ **ASSUMPTION A8:** the read/write-concern semantics in §D-isolation come from MongoDB's
  documentation, not from brief 021, whose §B covers only `with_transaction`, labels and revisions
  (`:65-81`). REPEATABLE_READ's snapshot stability is verified by 7(e). READ_COMMITTED's mapping is
  argued as "at least as strong" and not asserted.
- **Session/client mismatch enforcement:** PyMongo documents "a session may only be used with the
  client that started it" (`mongo_client.py:1391-1392`), but grep finds no enforcement string in
  the async client. A mismatched session might therefore *not* fail loudly. That makes
  `_ClientKey`'s identity semantics load-bearing. Invariant: two distinct clients never share a
  `ResourceTransaction`, pinned by 1(a) and 7(i).
- **Breaking `transactional=True`.** Apps with it set fail at construction. Mitigated by the precise
  message, 3-to-4.md and CHANGELOG. Open Question 1 records the softer alternative.
- **Missed `session=`** in a future repository method silently writes outside the transaction.
  Invariant: `test_beanie_repository_session.py` covers every driver call; the CLAUDE.md rule line
  and the common-pitfalls row make it the documented convention.
- **Commit-only retry blocking up to 120 s** in a request during a network partition. Bounded and
  spec-mandated, and each attempt is itself bounded by PyMongo's server selection timeout.
  Documented.
- **A second container per Beanie integration session** (about 4 s). Acceptable; the standalone
  container stays for the standalone test and every existing test.
- **Build order.** 063 lands after 062 Phase A (hard gate 0(a)) and 061 (hard gate 0(b)); 7(j) after
  067. Step 2's diff touches only kwargs on 061's and 067's lines (reviewed via `git diff`).

## Open questions (defaults chosen — the plan is buildable as is)

1. **`BeanieSettings(transactional=True)`** — default **raise at construction** (§D-default).
   Alternative: warn and ignore until 5.0 (non-breaking, but the flag keeps promising a transaction
   it never delivers).
2. **Standalone signal** — default **posture `mongo.transactions_unsupported` at INFO** plus refusal
   at first enlistment. Alternatives: a diagnose WARNING (blocks strict startups of every standalone
   app), or no startup signal at all.
3. **`isolation=None` on Mongo** — default **inherit the client's read/write concern** (062's
   "backend default"). Alternative: treat `None` as snapshot/majority for durable reads by default.
   That would deviate from 062's definition.
4. **Directly injected repositories (`bind_repositories()` → `AsyncRepository[D]`) never join**, on
   either backend. Default: **document as a Pitfall**. Alternatively, a 062 follow-up row making
   `get_repository()` boundary-aware for SA and Beanie together.
5. **Unknown commit outcome after the window** — default: **the raw `PyMongoError` escapes (500)**.
   Alternative: a core `TransactionOutcomeUnknownError` with its own code. That is a 062 API
   addition, so the coordinator decides.
6. **One client, several databases in one boundary** — default **allowed** (one key per client
   identity). Alternative: key on `(client, db_name)` and refuse the mix.
7. **`motor` in `varco_casbin`'s `beanie` extra** (`uv.lock:2866-2870`) — out of scope here. Should
   it get its own BACKLOG row, given Motor's EOL (brief 021 `:69`)?

## Parked / out of scope (with triggers)

- Per-UoW Mongo transactions outside a boundary (honouring `transactional=True`). Trigger: a user
  needs SA-parity single-call atomicity on Mongo *and* accepts the export/lifetime trade-off.
- Removing `BeanieSettings.transactional`. Trigger: 5.0.0, not before 2027-10 (12-month floor
  from 4.0.0).
- A lifetime-limit integration test on a function-scoped replica set with
  `transactionLifetimeLimitSeconds=1`. Trigger: a real report of a mis-classified lifetime abort.
- A Beanie `IUoWProvider` for `TenantIsolation.DATABASE`. Trigger: a user wiring services over
  `BeanieTenantPool`.
- `BeanieJobStore` (or any framework store) enlisting. Trigger: Open Question 3 of 062 (an
  outbox-backed producer) or a request for enqueue-iff-commit without the outbox.
- Read-only enforcement on Mongo (rejecting writes in a joined UoW). Trigger: a user relying on
  `read_only=True` as a guard.

## Done — this plan

- `BeanieRepositoryProvider` implements `TransactionalResource`: identity `transaction_key`;
  capabilities `{READ_COMMITTED, REPEATABLE_READ}` with no savepoints and no read-only mode; manual
  sessions; `PRIMARY` read preference. `make_uow()` returns `BeanieJoinedUnitOfWork` inside a
  boundary and a sessionless `BeanieUnitOfWork` outside one.
- `AsyncBeanieRepository` passes `session=` on every driver call (Step 1(b) green); every
  `# 063` marker is gone.
- `classify()` maps the `TransientTransactionError` label (112/251/other) to transient, and
  `UnknownTransactionCommitResult` and the 120 s window to `None`. `commit()` retries the commit
  alone within the window. `OPTIMISTIC_LOCK_ERRORS` is never retried.
- Standalone: refused at first enlistment; `mongo.transactions_unsupported` INFO from
  `BeanieModule`'s collector.
- `BeanieSettings.transactional=True` raises; `False` is inert; deprecated for 5.0.0.
- `mongo_replica_set_url` fixture and `VARCO_TEST_MONGO_RS_URL` override exist and are wired into the
  clean-room Makefile targets.
- The integration file is green twice: two-service atomic rollback, a real write-conflict retry,
  NESTED rejection, standalone behaviour, isolation, the 061 handoff, atomic `delete_many`,
  REQUIRES_NEW, cross-database, detached `stream()`, used-after-exit, and the conformance subclass.
- Updated: transactions.md (Mongo section + Pitfalls), security-posture.md, README, ARCHITECTURE,
  CHANGELOG, 3-to-4.md, COVERAGE.md, multitenancy.md, common-pitfalls.md, CLAUDE.md, and the
  store docstrings.
- `make lint`, `make type-check`, `make test` and `make integration-test-clean PKG=varco_beanie`
  green; snapshots regenerated where they moved.
