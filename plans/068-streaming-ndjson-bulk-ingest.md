# Plan 068 — Streaming bulk ingest: chunked NDJSON upload, per-line validation, batched `save_many`, per-line failure report, body-limit exemption for the ingest route (STREAM-2)

Covers BACKLOG 4.0.0-feature-extension row **`STREAM-2`** (`BACKLOG.md:62` — *"Streaming bulk ingest —
chunked NDJSON upload, per-line validation, batched `save_many`, per-line failure report, body-limit
exemption for the route"*). Ships inside **4.0.0** (`BACKLOG.md:31`: breaking changes allowed, each
with a `3-to-4.md` row). Arrow/Parquet stays parked (`BACKLOG.md:35,111`). The per-route body-ceiling
park (`BACKLOG.md:140`) is **not** un-parked: this plan adds a *private, ingest-only* exemption, not a
public per-route ceiling API (§D-bodylimit).

**This plan REUSES Plan 067's codec and HTTP adapter verbatim** — `varco_core.ndjson`
(`NDJSON_MEDIA_TYPE`, `NDJSON_MEDIA_TYPES`, `is_ndjson_media_type`, `encode_record`, `encode_error`,
`ControlKind`, `NdjsonLine`, `aiter_ndjson`, `DEFAULT_MAX_LINE_BYTES`, `NdjsonStreamError`) and
`varco_fastapi.streaming` (`NdjsonResponse`, `negotiate_ndjson`, `StreamingSettings`) plus 067's
`error_envelope(exc, request)` helper in `varco_fastapi/middleware/error.py`. **Two explicit
extensions of 067's API, nothing redesigned:** (E1) `ControlKind.SUMMARY = "$summary"` + `encode_summary()`
(067 §D-codec: "068 may add a `ControlKind`"); (E2) `ingest_*` fields on `StreamingSettings`.
Neighbours: **061** (creates-only; §D-scope), **062** (ingest route excluded from the request boundary;
§D-atomicity), **064–066** (no input echo in the report; §D-report), **069** (job-backed ingest parked).

**Sources of record read for this plan (2026-10-10):** `BACKLOG.md:1-64,109-140`; `plans/067` (whole);
`plans/062` §D-api (`:131-205`), §D-postcommit (`:346-370`), §D-service (`:372-429`), §D-stream
(`:497-515`), §D-outbox-audit (`:535-546`), §D-http (`:648-687`); `plans/061` (`:14,17,58-60,129-130,
160-206,298`); research `design/research/021-transactions-conditional-requests-streaming.md` §D
(`:97-133,159-172`); `design/research/019-streaming-orchestration-and-test-performance-2026.md` §A
(`:15-35`); `varco_core/repository.py:185-219`; `varco_core/service/bulk.py` (whole);
`varco_core/service/base.py:1069-1132,1319-1343`; `varco_core/cache/mixin.py:456-479`;
`varco_core/exception/http.py:91-179,242-366`; `exception/body_limit.py`; `exception/idempotency.py:42,89,125,159`;
`exception/service.py:193-262`; `varco_sa/repository.py:398-466`; `varco_beanie/repository.py:313-360`;
`varco_beanie/factory.py:254-279`; `varco_fastapi/middleware/body_limit.py` (whole);
`middleware/idempotency.py:36-56,215-240,307-369`; `middleware/slots.py:33-61`; `router/base.py:684-935,1185-1192`;
`router/introspection.py:303-414`; `router/mixins.py:39-110`; `tests/test_body_limit_middleware.py:24-78`;
installed sources: **Starlette 1.3.1** (`requests.py:234-252`, `responses.py:222-283`,
`middleware/base.py:20-245`, `routing.py:238-254,359-416`, `applications.py:87`), **uvicorn 0.42.0**
(`protocols/http/h11_impl.py:205,255-274,528-550`, `protocols/http/httptools_impl.py:227,305-317,460,520-577`),
**httpcore** (`_async/http11.py:65-106`), **Beanie 2.0.1** (`odm/documents.py:435-464`).

> ⚠️ **Premises corrected from source before anything was decided.**
> (1) **Brief 021's "the request body tap closes on `http.response.start`" (`021:125`, `:172`) is false for
> uvicorn 0.42.0.** Both protocol implementations return `http.disconnect` from `receive()` only when
> `self.disconnected or self.response_complete` (`h11_impl.py:536-542`, `httptools_impl.py:568-574`) —
> after the response *completes*, not when it starts. The ASGI-spec wording itself was not re-read
> (⚠️ ASSUMPTION, Risks). **The design does not depend on it**: full-duplex is ruled out by three other
> verified facts (§D-duplex), so the central decision — *consume the whole body, then respond* — holds
> whichever reading of the spec is right.
> (2) **A bulk create already exists**: `BulkServiceMixin.create_many()` (`varco_core/service/bulk.py:116-194`,
> exported from `varco_core.service`). It is all-or-nothing per call and raises on the first bad item —
> unusable for a per-line report as-is; this plan adds a sibling, `create_each()` (§D-service).
> (3) **`create_many`'s docstring promises `ServiceConflictError` on a uniqueness violation
> (`bulk.py:146-147`) but nothing translates one**: SA `save_many` flushes with no `except`
> (`varco_sa/repository.py:440-447`), Beanie calls `insert_many` with no `except`
> (`varco_beanie/repository.py:350-355`) — callers get raw `IntegrityError` / `BulkWriteError` (500).
> (4) **`CacheServiceMixin` invalidates list caches only in `create()`** (`cache/mixin.py:456-479`); a
> `create_many()` today leaves every cached list stale. Ingest would inherit the bug (§D-service fixes both).
> (5) **`IdempotencyMiddleware` buffers the whole body** (`await request.body()`, `idempotency.py:342`)
> whenever an `Idempotency-Key` is present. On an ingest route exempt from the body limit this is an
> unbounded-memory DoS (§D-idempotency).
> (6) **Beanie `insert_many` converts documents to dicts** (`documents.py:454-463`), so PyMongo's `_id`
> assignment never reaches the `Document`; varco's Beanie pks are client-assigned for every strategy but
> `INT_AUTO` (`factory.py:254-279`), which is why a failed batch can be attributed per index (§D-persist).

## Goal

After this plan: a router composing the new opt-in **`IngestMixin`** exposes `POST {prefix}/ingest`,
which accepts an NDJSON body (`application/jsonl` or `application/x-ndjson`, chunked or with
`Content-Length`) of `CreateDTO` lines **without buffering it**, validates each line, persists valid
lines in **batches through `BulkServiceMixin.create_each()` → `save_many()`** (one UoW per batch, every
per-entity hook/authorization/tenancy/event/audit guarantee of `create()` preserved), and answers with
an **NDJSON report** (`$error` per failed line — or one result line per input line with
`?report=all` — and a final `$summary` carrying a `last_line` resume point); the route enforces its own
byte/line/time/failure ceilings and is **automatically exempt** from `BodyLimitMiddleware`'s default
10 MiB ceiling; `Idempotency-Key` on it is refused before any body byte is read; duplicate keys are
reported per line on SQLAlchemy and Beanie; the client gains `ingest()`; memory stays bounded on
Postgres and MongoDB for a 200 000-line upload (integration-tested); docs, CHANGELOG, a `3-to-4.md`
row, and every snapshot gate are green.

## Non-goals

- **Updates / upserts / deletes through ingest** — creates only (§D-scope); parked with a trigger.
- **All-or-nothing ingest** (one transaction for the whole upload) — rejected (§D-atomicity); parked.
- **Streaming the report while the body is still being read** (full duplex) — rejected (§D-duplex).
- **A public per-route body-ceiling API** — the parked feature (`BACKLOG.md:140`); the exemption here
  is private to `IngestMixin` (§D-bodylimit).
- **`Idempotency-Key` support on ingest** — refused (§D-idempotency); idempotency comes from natural keys.
- **Compressed request bodies** (`Content-Encoding: gzip`) — 415; parked.
- **Job-backed ingest** (upload to object storage, process as a 069 job, report as an artifact) — parked.
- **Ingest through MCP / A2A / skills / `ServiceContract` / `varco gen-client`** — skipped (asserted).
- **Per-batch transient-error retry** (062's `TRANSACTION_RETRY` around each batch) — a transient
  failure aborts with a resumable summary; parked.
- **Translating `IntegrityError` in SA `save()`** (single `create()` 500 → 409) — Open Question 3; this
  plan translates only in `save_many`, the path `create_many`'s docstring already promised.
- **Arrow / Parquet / CSV ingest** — parked (`BACKLOG.md:35`).
- **`BACKLOG.md` edits** — coordinator's; the handback lists premises (1)–(5).

## Design

```
 client ── POST /orders/ingest?report=errors   Content-Type: application/jsonl   (chunked or Content-Length)
   │
 BodyLimitMiddleware ── over 10 MiB? → route_streams_body(scope)? → endpoint marked → pass through (§D-bodylimit)
 IdempotencyMiddleware ─ Idempotency-Key + marked route → 400 before any body byte (§D-idempotency)
   │
 ingest handler ── PRE-FLIGHT (before the first receive(); real HTTP statuses via ErrorMiddleware):
   │                 negotiate_ndjson(Accept) 406 · Content-Type/Encoding 415 · Content-Length > max 413
   │                 · service.authorize_create(ctx) 401/403/TENANT_REQUIRED
   │               INGEST PHASE (always ends in 200 + report):
   │                 request.stream() ─► _counted(max_bytes) ─► aiter_ndjson(max_line_bytes) ─► _deadline(max_seconds)
   │                   per line: malformed/control → line failure · C.model_validate → line failure | batch
   │                   every batch_size lines: service.create_each(batch, ctx) ─► BatchOutcome
   │                       (one UoW: prepare/validate per entity → save_many → commit → per-entity event + _after_create)
   │                   window outcomes (sorted by line) ─► IngestReportWriter (SpooledTemporaryFile)
   │                 body fully consumed (or a ceiling / fatal error) ─► $error? + $summary
   ▼
 NdjsonResponse(writer.replay())   ← headers sent only now (§D-duplex)
   {"$error": {"code": "...", "status": 422, "line": 3, ...}}
   {"line": 4, "pk": "…"}                      (only with ?report=all)
   {"$summary": {"records": …, "created": …, "failed": …, "last_line": …, "complete": true, …}}
```

### §D-duplex — consume the whole body, then respond (the central decision)

Brief 021 §D (`:125`) frames this as an ASGI constraint; premise (1) shows uvicorn would in fact let an
app keep reading after `http.response.start`. Full duplex is nevertheless unsafe, for three verified
reasons:

1. **Starlette steals body chunks from a concurrent response.** `StreamingResponse.__call__` with ASGI
   `spec_version < 2.4` runs `listen_for_disconnect(receive)` in a task group beside the body
   (`responses.py:265-280`); that loop calls `receive()` and discards every `http.request` message
   (`:242-246`). uvicorn 0.42.0 reports `spec_version: "2.3"` (`h11_impl.py:205`,
   `httptools_impl.py:227`). A report streamed while `request.stream()` is still reading would race it
   for `receive()` and silently lose upload chunks. 067's `NdjsonResponse` overrides only
   `stream_response`, so it inherits this.
2. **varco's own HTTP client is half-duplex.** httpcore's HTTP/1.1 connection sends request headers and
   the **entire** body before reading response headers (`_async/http11.py:86-106`). A server writing a
   report while reading would, once socket buffers fill, block in `send` (uvicorn awaits
   `flow.drain()` when writing is paused, `httptools_impl.py:460`) while the client blocks writing the
   body → deadlock on exactly the large uploads this feature exists for.
3. ⚠️ ASSUMPTION (not verified here): other ASGI servers and reverse proxies (nginx's default request
   buffering) may buffer or close differently; a half-duplex design is correct on all of them.

DESIGN: read the full body inside the handler, persist batch-by-batch while reading, send headers only
after the body is consumed
  ✅ Correct on every server and client regardless of the spec reading; no `receive()` race (the
     report's `listen_for_disconnect` starts only after `request.stream()` is exhausted)
  ✅ **Every pre-flight error is a real HTTP status**, and nothing is in flight when it is raised;
     uvicorn sends `100 Continue` only on the app's first `receive()` (`h11_impl.py:529-534`,
     `httptools_impl.py:564-566`), so a client using `Expect: 100-continue` uploads **nothing** when
     refused (pre-flight runs before the first `receive()`)
  ✅ Natural backpressure: while a batch is being persisted the handler does not call `receive()`;
     uvicorn pauses reading past `HIGH_WATER_LIMIT` (`h11_impl.py:264-266`, `httptools_impl.py:308-310`)
     → TCP backpressure to the client; server memory is bounded by one batch + one line + the spool
  ✅ `MetricsMiddleware`/tracing measure the whole ingest (the handler runs for the upload's duration)
  ❌ The report cannot be read until the upload finishes; a client cannot react to line 3's failure
     mid-upload. Mitigated by `ingest_max_failed_lines` (abort early) and the resume point
  ❌ The report must be held until the end → spooled (§D-report), not streamed from memory
  ❌ An aborted ingest (ceiling hit) still makes the client finish uploading: uvicorn drains and discards
     the unread remainder after the response completes (`h11_impl.py:261-273`,
     `httptools_impl.py:305-307`). Closing the connection instead risks a TCP RST that destroys the
     already-sent report. Documented Pitfall: honour the ceilings client-side

### §D-phases — pre-flight vs ingest phase: which failures are HTTP statuses

- **Pre-flight** (before the first `receive()`): each raises through `ErrorMiddleware` as a normal JSON
  error. Order: `negotiate_ndjson(Accept)` (406, 067); `Content-Type` not NDJSON per
  `is_ndjson_media_type` or a `charset` other than `utf-8` → 415; `Content-Encoding` present and not
  `identity` → 415 (both via `HTTPException`, 067's precedent for 406); declared `Content-Length` >
  effective `max_bytes` → `IngestLimitExceededError` (413); `service.authorize_create(ctx)` →
  401/403/`TENANT_REQUIRED`. Authentication already ran as the route's `Depends(server_auth)`
  (`router/base.py:914-923`), which never reads the body.
- **Ingest phase** (from the first `receive()`): the response is **always 200 + report**. Per-line
  failures are `$error` lines with `line`; a terminal failure (ceiling, too many failures, a batch
  re-authorization denial, a database outage) is one `$error` **without** `line`, immediately before
  `$summary` (`complete: false`). Rationale: once any batch may have committed, a 4xx/5xx JSON body would
  hide what was persisted.
- **Client disconnect** (`ClientDisconnect` from `request.stream()`, `requests.py:249-251`): stop
  reading, **do not** persist the pending window, log WARNING (correlation id, `last_line`, counts),
  return nothing (no one to send to). `CancelledError` always propagates.

### §D-report — the report format (extends 067's codec by one control kind)

Built only from 067's codec plus **(E1)** `ControlKind.SUMMARY = "$summary"` and
`encode_summary(payload: Mapping[str, Any]) -> bytes` in `varco_core/varco_core/ndjson.py`.

- **Per-line failure:** `encode_error({**envelope, "status": status, "line": n})` where `(status,
  envelope)` = 067's `error_envelope(exc, request)` — the exact body `ErrorMiddleware` would render for
  that exception (code, message, `message_key`, redacted `params`, `correlation_id`). **Rule (written into
  `streaming.md`): an `$error` payload carrying an integer `line` is a non-terminal per-line result of an
  ingest report; without `line` it is terminal** — the distinction 067 anticipated (`067:165-166,446`).
- **Per-line success** (`?report=all` only): a data line `{"line": n, "pk": <ReadDTO.pk>}`
  (`encode_record`; composite pk → JSON array). Default `?report=errors` omits success lines.
- **Summary** (always the last line): `{"$summary": IngestSummary}` with `records` (non-blank lines
  resolved), `created`, `failed`, `last_line`, `complete`, `unresolved: [first, last] | null`,
  `post_commit_failures`, `batches`, `elapsed_ms`.
- **Resolution invariant (the resume contract):** every physical line `≤ last_line` has **exactly one**
  outcome in the report (blank lines have none and are not counted in `records`); **no line `> last_line`
  appears**; a client resumes by re-sending the lines after `last_line`. `unresolved` names the range
  whose persistence outcome is unknown after a fatal error (§D-persist) — re-sending it is safe only with
  natural unique keys (duplicates come back as per-line 409 `CONFLICT`).
- **Ordering:** outcomes are written per *window* (the lines read since the last flush), sorted by line
  number, after the window's batch settles — so a window's early validation failures are dropped if the
  window ends unresolved, keeping the invariant.
- **No input echo.** Pydantic failures become `IngestLineInvalidError` whose `error_params()` is
  `{"errors": [{"field": "a.b", "type": "int_parsing", "message": "…"}…≤20], "truncated": bool}` built
  with `ValidationError.errors(include_url=False, include_input=False, include_context=False)`. Malformed
  lines carry `aiter_ndjson`'s decoder reason, never the line bytes. Nothing logs line content (GDPR
  064–066 neighbours; Plan 054 S3 rule).
- **Spooling:** `IngestReportWriter` writes encoded lines into a `tempfile.SpooledTemporaryFile(
  max_size=ingest_report_spool_bytes)` — one write per window via `anyio.to_thread.run_sync` (anyio is
  Starlette's own dependency); `replay()` yields lines read in 64 KiB blocks the same way and closes the
  spool in `finally` (it runs under `NdjsonResponse`'s `aclosing`). The summary is kept in memory, so a
  spool failure (read-only `TMPDIR` at rollover) degrades to a report of `$error` + `$summary` only.

DESIGN: errors-only by default, one-line-per-input opt-in, spooled
  ✅ Bounded server memory in both modes; the default report of a clean 1 M-line ingest is one line
  ✅ `?report=all` gives server-assigned pks per line for clients that need them
  ✅ One writer path for both modes
  ❌ Deviates from the coordinator's "one result line per input line" default → Open Question 1
  ❌ `?report=all` past the spool threshold needs a writable `TMPDIR` (Pitfall)

### §D-atomicity — per-batch commit; the route never runs inside a request transaction

Each batch is one `create_each()` call = one UoW = one commit (a plain UoW today; under 062 a standalone
ROOT, because no ambient boundary exists in the handler). Lines are committed in order; `last_line`
marks a prefix boundary.

DESIGN: per-batch commit over all-or-nothing
  ✅ Bounded resources: one connection and one short transaction per batch, no transaction held for the
     upload's duration (VACUUM horizon, unique-index locks, pool starvation), SA identity map dropped per batch
  ✅ Works on Beanie today: `BeanieUnitOfWork` is non-transactional (062 premise, `varco_beanie/uow.py:49-90`),
     so all-or-nothing is not even expressible there; Mongo transactions also have a lifetime limit
     (⚠️ ASSUMPTION: 60 s default `transactionLifetimeLimitSeconds`, not re-verified)
  ✅ Resumable: a failure at line 999 999 loses at most one batch, not the file
  ❌ A file is not atomic; the client must read `$summary` → documented resume recipe
  Rejected: a single 062 `transaction()` around the whole ingest (all of the above, plus 062's
  whole-boundary retry would re-invoke the handler with a consumed body).

**062 obligation (whichever of 062/068 builds second implements it):** the `"ingest"` CRUD route is
registered with **`transaction=False`** — the CRUD-route equivalent of `@route(..., transaction=False)`
in 062 §D-http's precedence chain — so neither `VarcoRouter._transaction` nor `TransactionModule`'s
default `methods={"POST", …}` policy can wrap it. A `REQUIRED` request boundary would silently turn
per-batch commits into one upload-long transaction; its retry would replay a consumed body. Step 7
pins it with a test guarded by `pytest.importorskip("varco_core.transaction")` (it starts running the
moment 062 lands). Programmatic `create_each()` inside a caller's own `@transactional` joins it
(`REQUIRED`, 062 §D-service) — documented; the router never does that.

### §D-scope — creates only (061 interaction)

Every line is a `CreateDTO` (`C`); every entity takes the INSERT path of `save_many`.

DESIGN: creates-only over upsert/update lines
  ✅ An UPDATE line needs a pk lookup, per-entity `Action.UPDATE` authorization against the loaded
     entity (`bulk.py:50-57`'s ownership argument), 061's per-line `expected_version` and its
     `StaleEntityError` → 409/412 translation — N reads per batch and a second report vocabulary
  ✅ SA `save_many` returns "inserts first, then updates" (`varco_sa/repository.py:414-415`) — mixed
     batches break the index→line mapping; creates-only keeps input order (pinned by test)
  ✅ No stale line can occur, so 061 needs nothing from this plan (061 `:17` hands the decision here)
  ❌ "Load or refresh a dataset" needs a delete + re-ingest or an app route → parked with a trigger
A `version`/`row_version` member on an ingest line is whatever the `CreateDTO` does with an unknown
field (ignored or a per-line 422 under `extra="forbid"`) — never a precondition.

### §D-service — `BulkServiceMixin.authorize_create()` + `create_each()` (`varco_core/service/bulk.py`)

```python
@dataclass(frozen=True)
class BatchOutcome(Generic[R]):
    created: Mapping[int, R]                 # input index → ReadDTO
    failed: Mapping[int, ServiceException]   # input index → attributable domain failure
    unresolved: tuple[int, ...] = ()         # fate unknown (only when fatal is set)
    fatal: Exception | None = None           # the unattributable failure that stopped the call
    post_commit_failures: int = 0
    # invariant: created ∪ failed ∪ unresolved == range(len(dtos)), pairwise disjoint

async def authorize_create(self, ctx: AuthContext = _ANON_CTX) -> None   # _pre_check + Action.CREATE (type-level)
async def create_each(self, dtos: Sequence[C], ctx: AuthContext = _ANON_CTX) -> BatchOutcome[R]
```

`create_each` algorithm (parity with `create()` `base.py:1088-1132` and `create_many()` `bulk.py:156-194`):
1. Empty → empty outcome, no I/O. `await self.authorize_create(ctx)` — **raises** (nothing written yet).
2. **Batch UoW:** per item `to_domain → _prepare_for_create → _validate_entity → _validate_entity_async`
   (same order, same position inside the UoW as `create_many`); a `ServiceException` →
   `failed[i]`, the item is skipped. `repo.save_many(survivors)` → `to_read_dto` each → `created`.
3. `save_many` raised **`BatchWriteError`** (§D-persist, Beanie): its `persisted` → `created`, its
   `failed` → `failed`; if `failed` is empty the error is unattributable → `fatal`, rest `unresolved`;
   otherwise loop step 3 on the remaining (never-attempted) survivors in a new UoW.
4. `save_many` raised a **`ServiceException`** (SA's translated conflict; the UoW rolled back) →
   **singles fallback**: each survivor in its own UoW via `save_many([entity])` (entities reused, hooks
   not re-run); `ServiceException`/attributed `BatchWriteError` → `failed[i]`; anything else → `fatal`,
   that item and the rest `unresolved`.
5. Any other `Exception` → `fatal`; everything not yet in `created`/`failed` is `unresolved`.
6. **Post-commit, per created item in index order:** `_publish_domain_event(EntityCreatedEvent(...))`
   then `_after_create(saved, read_dto, ctx)` — each wrapped: an exception is logged at ERROR (entity
   type, pk, correlation id, `exc_info`) and counted in `post_commit_failures`, never raised (the data is
   durable; raising would lose every other line's outcome). Then, if anything was created, once:
   `_after_bulk_create(read_dtos, ctx)` (same wrapping). `CancelledError` is never caught.

New non-abstract hook on `AsyncService` (additive — out-of-tree services unaffected):
`async def _after_bulk_create(self, read_dtos: Sequence[R], ctx: AuthContext) -> None: return None`.
`create_many` calls it too; **`CacheServiceMixin` overrides it** to `_invalidate_list(tenant_id=…)` +
`_publish_invalidated([], operation="create")` once per call and chains `super()` (fixes premise 4 for
`create_many` as well). `create_many` additionally unwraps a `BatchWriteError` to its first attributed
`ServiceException` (`raise … from bwe`) so its documented contract holds on both backends.

**If 062 has landed when this is built:** `create_each` uses `self._open_uow()` and defers step 6 per
item through `on_commit()` exactly as 062 §D-service does for `create_many` — the `try/except` lives
*inside* the deferred callback, so a hook failure is still logged-and-counted, never re-raised by the
ROOT. Outside a boundary (the ingest route's case) `on_commit` runs immediately (062 §D-postcommit), so
`post_commit_failures` is exact.

DESIGN: a partial-success sibling on `BulkServiceMixin` over growing `AsyncService` or reusing `create_many`
  ✅ Keeps every per-entity guarantee by running the identical hook chain (`super()`-composed mixins —
     `TenantAwareService` stamps the tenant, `SoftDeleteService`, `ValidatorServiceMixin`,
     `AuditLogMixin._after_create` all run per entity)
  ✅ Authorization once per batch (type-level, as `create()`/`create_many()`), re-checked every batch
  ✅ Opt-in mixin — 067 rejected growing `AsyncService`'s surface for the same reason (`067:460-462`)
  ✅ Events stay per entity through `_publish_domain_event` (the scout's `_produce_many` would bypass
     any override of `_publish_domain_event` and diverge from `create_many`)
  ❌ `IngestMixin` requires the service to compose `BulkServiceMixin` (`TypeError` naming the fix)
  ❌ The singles fallback costs one transaction per survivor of a failed batch — re-ingesting a file of
     duplicates is slow (correct); a natural-key pre-check is parked

### §D-persist — attributing a failed batch: `BatchWriteError` and the SA translation

- **`varco_core/varco_core/exception/batch.py` (new):** `BatchWriteError(Exception)` — **not** a
  `ServiceException` (repository-level, never rendered; like 061's `StaleEntityError`). Fields:
  `persisted: Mapping[int, Any]` (input index → persisted domain entity, durably written despite the
  failure — only a non-transactional backend sets it), `failed: Mapping[int, ServiceException]`.
  Indexes in neither were not attempted. Raised `from` the driver error.
- **SA `save_many`:** wrap the insert flush (`repository.py:446`) in `except IntegrityError as exc:
  raise ServiceConflictError(f"{entity_type} violates a uniqueness or integrity constraint") from exc` —
  **never** `str(exc)` in the message (Postgres embeds the duplicate value: personal data). The
  transactional UoW rolls the batch back; `create_each` falls back to singles. Coexists with 061's
  `StaleDataError` catch on the same flush (one `try`, two `except`s).
- **Beanie `save_many`:** pass `ordered=True` explicitly to `insert_many`; `except BulkWriteError as exc`:
  with ordered inserts the documents before the first `writeErrors[0]["index"]` were written, that index
  failed, the rest were not attempted. Code `11000` → `failed={i: ServiceConflictError(...)}`; any other
  code → `failed={}` (unattributable). `persisted` = `from_orm` of the written prefix. ⚠️ Verify the
  `details` keys (`writeErrors[].index`, `.code`, `nInserted`) in the installed PyMongo before writing.

### §D-bodylimit — an automatic, private, fail-closed exemption

`varco_fastapi/varco_fastapi/middleware/_streaming_route.py` (new, private):
- `mark_streaming_body(endpoint) -> endpoint` sets `endpoint.__varco_streaming_body__ = True`; called
  **only** by `_make_ingest_handler`.
- `route_streams_body(scope) -> bool`: walks `scope["app"].router.routes` (`scope["app"]` is set by
  `Starlette.__call__`, `applications.py:87`), calling Starlette's own pure `route.matches(scope)`
  (`routing.py:238-254`) and taking the **first `Match.FULL`** (Router's own rule); recurses into a
  matching `Mount` with `{**scope, **child_scope}` over `mount.routes` (`routing.py:384-416`; covers
  `composite.py:320`'s sub-apps), depth ≤ 4; returns the endpoint's marker. No app, no match, a
  `PARTIAL` match, or any exception → `False` (**fail closed**: the 10 MiB ceiling applies).

`BodyLimitMiddleware.__call__` consults it **lazily, only when it would otherwise reject**: (a) declared
`Content-Length > max_bytes` → if marked, pass through unwrapped; (b) the cumulative counter crosses
`max_bytes` → look up once (memoized in the closure) → if marked, stop counting, else raise as today.
Under-limit requests never pay for a lookup. The ingest route enforces **its own** ceiling
(`ingest_max_bytes`, both the `Content-Length` pre-check and a counter over `request.stream()`).

DESIGN: route marker resolved lazily in the middleware, over `exempt_paths` or a scope flag
  ✅ Zero configuration — mounting `IngestMixin` is the opt-in; the exemption is exactly that route and
     method, independent of mount prefix
  ✅ The ceiling is **replaced** by the ingest's own (byte/line/time), never removed
  ✅ Fail closed: an unresolvable route keeps the 10 MiB ceiling (a loud 413, never a silent unbounded read)
  ✅ Not the parked per-route-ceiling feature: private helper, boolean, ingest-only (`BACKLOG.md:140`
     trigger untouched)
  ❌ The middleware duplicates routing (via Starlette's own `matches`) on the over-limit path only
  Rejected — `VARCO_BODY_LIMIT_EXEMPT_PATHS`: manual (a forgotten env var = every large ingest 413s),
  prefix-broad (`/orders/ingest` also exempts `/orders/ingest-x`), mount-prefix dependent, removes the
  ceiling entirely. Rejected — a scope flag set by the handler before reading: the `Content-Length`
  pre-check runs before routing, and deferring it breaks the pinned contract
  `test_content_length_above_ceiling_rejected_before_handler_runs` (`test_body_limit_middleware.py:49-54`).

### §D-idempotency — `Idempotency-Key` on an ingest route is refused, before any body byte

In `IdempotencyMiddleware.dispatch`, after `_applies_to(request)` and **before** `request.body()`
(`idempotency.py:342`): if `route_streams_body(request.scope)` — a key present → raise
`IdempotencyKeyInvalidError("Idempotency-Key is not supported on a streaming-ingest route; make lines
idempotent with natural unique keys")` (400 `VARCO_IDEMPOTENCY_001`, existing class); no key →
`call_next` (`require_key` does not apply to these routes). The lookup runs only when a key is present
or `require_key=True`.

DESIGN: refuse over ignore or support
  ✅ Closes premise (5)'s unbounded buffer on the exempt route
  ✅ A client that thinks its retries are deduplicated learns otherwise immediately, not after a duplicate load
  ✅ Natural-key idempotency already works: a re-sent line comes back as a per-line 409
  ❌ SDKs that blanket-send `Idempotency-Key` on every POST must drop it for ingest → Open Question 2
  Rejected — support: the store's `reserve()` needs the full-body fingerprint *before* execution, and
  replay would need the whole report stored.

### §D-limits — `StreamingSettings` extension (E2) and router ClassVars

Fields added to 067's `StreamingSettings` (`VARCO_STREAMING_`, already bound by `bind_settings()` and
injected into routers, `067 §D-settings`):

| Field | Default | Meaning |
|---|---|---|
| `ingest_max_bytes` | `1_073_741_824` (`ge=1`) | hard per-request body ceiling for ingest (replaces the 10 MiB default) |
| `ingest_max_lines` | `1_000_000` (`ge=1`) | non-blank lines per request |
| `ingest_max_line_bytes` | `1_048_576` (`ge=1024`) | passed to `aiter_ndjson(max_line_bytes=…)` |
| `ingest_max_failed_lines` | `1_000` (`ge=0`; `0` = abort on the first failure) | abort threshold |
| `ingest_max_seconds` | `600.0` (`gt=0`) | deadline on **body reads** (slow-loris guard) |
| `ingest_batch_size` | `500` (`ge=1`, `le=10_000`) | lines per `create_each` call |
| `ingest_report_spool_bytes` | `4_194_304` (`ge=65_536`) | report spool in-memory threshold |

`IngestMixin` ClassVars: `_ingest_max_bytes`, `_ingest_max_lines`, `_ingest_max_failed_lines`,
`_ingest_max_seconds` (`None` default; may only **lower** — `min()` with settings) and
`_ingest_batch_size` (free within 1..10 000). A ceiling hit → flush the current window, then terminal
`$error` (`IngestLimitExceededError`, `params={"limit": "bytes"|"lines"|"seconds", "value": …}`, or
`IngestTooManyFailuresError`) + `$summary(complete=false)`. The deadline wraps only `__anext__` of the
line iterator (`asyncio.timeout_at`), never a batch flush (cancelling a flush mid-write would make its
outcome unknown); it is also checked after each flush.

DESIGN: extend `StreamingSettings` over a new `IngestSettings`
  ✅ No second binding, no second router injection kwarg; one `VARCO_STREAMING_` prefix for the feature
  ❌ Edits 067's class (E2) — additive fields only

### Exceptions (`varco_core/varco_core/exception/ingest.py`, new; registered like `body_limit.py:68-82`)

| Class | Base | HTTP / code | `message_key` | `error_params()` |
|---|---|---|---|---|
| `IngestLimitExceededError` | `ServiceException` | 413 `VARCO_INGEST_001` | `varco.error.ingest_limit_exceeded` | `limit`, `value` |
| `IngestTooManyFailuresError` | `ServiceException` | 422 `VARCO_INGEST_002` | `varco.error.ingest_too_many_failures` | `max_failed_lines` |
| `IngestLineMalformedError` | `ServiceException` | 400 `VARCO_INGEST_003` | `varco.error.ingest_line_malformed` | `reason` (decoder text / `"control record"`) |
| `IngestLineInvalidError` | `ServiceValidationError` | 422 `VARCO_INGEST_004` | `varco.error.ingest_line_invalid` | `errors` (≤20), `truncated` |

### §D-route — `IngestMixin`: `POST {prefix}/ingest`

`_CRUD_ACTION = "ingest"`; `_CRUD_ROUTES["ingest"] = ("POST", "/ingest", 200, False)` (not
async-offloadable: a body cannot be handed to a job); `_ingest_path` via the same `_{action}_path`
lookup 067 Step 7 adds for `_export_path`; the OpenAPI/metadata ClassVar set mirrors `ExportMixin`
with an `_ingest_` prefix. Introspection: `request_model = C`, `response_model = None`.
Handler signature: `(request: Request, auth = Depends(server_auth), report: Literal["errors", "all"] =
Query("errors"))` — **no body parameter**, so FastAPI never reads the body. Registration:
`response_model=None`, `response_class=NdjsonResponse`, `openapi_extra={"requestBody": {"required":
True, "content": {"application/jsonl": {"itemSchema": C}, "application/x-ndjson": {"itemSchema": C}}}}`
built with the **same schema-registration mechanism 067 Step 7 uses for `R`'s response `itemSchema`**.
Response headers: negotiated `Content-Type`, `Cache-Control: no-store`, `X-Accel-Buffering: no`.
`_make_ingest_handler` raises `TypeError` at build time when a concrete `_service` is not a
`BulkServiceMixin` (when `_service` is a lazy DI proxy, the same check runs in pre-flight).
Completion log: one INFO line (`entity`, `records`, `created`, `failed`, `complete`, `elapsed_ms`,
correlation id) — never line content.

### Alternatives considered

- **Full-duplex report streaming** — rejected (§D-duplex): Starlette `<2.4` disconnect listener steals
  body chunks; httpcore is half-duplex → deadlock. ✅ earlier feedback ❌ unsafe on the stack we ship.
- **JSON-array body (`list[C]`) on a normal POST** — rejected: FastAPI buffers and validates the whole
  array; the 10 MiB limit; no per-line report. ✅ zero new code ❌ the problem this row exists to solve.
- **All-or-nothing transaction** — rejected (§D-atomicity). Parked with a trigger.
- **`exempt_paths` / public per-route ceiling / handler-set scope flag** — rejected (§D-bodylimit).
- **Support `Idempotency-Key`** — rejected (§D-idempotency).
- **Creates + updates** — rejected (§D-scope).
- **`create_each` on `AsyncService`** — rejected (§D-service): every out-of-tree service inherits it.
- **Bisection instead of singles fallback** — rejected: log₂N rounds, each a rollback, for a rarely-hit
  path; ✅ fewer transactions on huge failed batches ❌ more code, same worst case for all-duplicate files.
- **Report entirely in memory** — rejected: `?report=all` on 1 M lines is tens of MB per request.
- **Separate `IngestSettings`** — rejected (§D-limits).
- **Job-backed ingest (069)** — parked: needs object storage and a report artifact; not trivial.

## Steps

Each step is test-first: the **(a)** tests are written and fail before **(b)**. **Preflight (part of
Step 1):** confirm 067 Steps 3/5/7/8 have landed and record the actual names of `error_envelope`,
`negotiate_ndjson`, the `_{action}_path` lookup and the `itemSchema` mechanism — if any differ from this
plan, use 067's. Note whether 061 (`save_many` `StaleDataError` catch) and 062 (`_open_uow`, `on_commit`,
route `transaction=`) have landed; Steps 2, 3 and 7 say what changes in each case.

1. [ ] **Codec extension (E1)** — `varco_core/varco_core/ndjson.py`: `ControlKind.SUMMARY = "$summary"`,
   `encode_summary(payload) -> bytes` (requires `records`, `created`, `failed`, `last_line`, `complete`);
   module docstring gains the ingest-report rule (`$error` + `line` = non-terminal).
   (a) `varco_core/tests/test_ndjson.py` (extend 067's): `encode_summary` round-trips through
   `aiter_ndjson` with `control is ControlKind.SUMMARY`; missing required key → `ValueError`;
   `encode_record` refuses a `{"$summary": …}`-shaped value; an `$error` with `line` decodes with the
   payload intact.
   (b) Implement.

2. [ ] **Batch-failure contract in the repositories** — new `varco_core/varco_core/exception/batch.py`
   (`BatchWriteError`, exported from `varco_core.exception`); `varco_sa/varco_sa/repository.py:440-447`
   (IntegrityError → `ServiceConflictError`, beside 061's catch if present); `varco_beanie/varco_beanie/repository.py:350-355`
   (`ordered=True`, `BulkWriteError` → `BatchWriteError`); `varco_core/varco_core/repository.py:187-219`
   (docstring: `Raises: ServiceConflictError, BatchWriteError`; creates-only order guarantee);
   `varco_core/varco_core/service/bulk.py` `create_many` (unwrap `BatchWriteError`).
   (a) `varco_sa/tests/test_save_many_conflict.py` (aiosqlite, `make test`): a unique column, a batch
   with a duplicate → `ServiceConflictError`, message contains neither the value nor driver text,
   `__cause__` is `IntegrityError`, the session rolled back (no row); a clean batch returns entities in
   input order. `varco_beanie/tests/test_save_many_bulk_error_unit.py` (monkeypatched `insert_many`
   raising a real `pymongo.errors.BulkWriteError` with ordered `details`): dup at index 2 of 5 →
   `persisted` = {0, 1} (pks set), `failed` = {2: `ServiceConflictError`}, 3–4 absent; code 121 →
   `failed == {}`; `ordered=True` forwarded. `varco_core/tests/test_bulk_service.py` (extend):
   `create_many` raises `ServiceConflictError` for both shapes.
   (b) Implement (verify PyMongo `details` keys first).

3. [ ] **`create_each` + `authorize_create` + `_after_bulk_create`** — `varco_core/varco_core/service/bulk.py`
   (`BatchOutcome`, methods, DESIGN blocks, `__all__`), `varco_core/varco_core/service/__init__.py`
   (export `BatchOutcome`), `varco_core/varco_core/service/base.py` (no-op `_after_bulk_create` with
   docstring), `varco_core/varco_core/cache/mixin.py` (override; under 062 defer like `create`).
   (a) `varco_core/tests/test_bulk_create_each.py` with an in-memory UoW/repo whose `save_many` can be
   scripted to raise `ServiceConflictError` (SA shape) or `BatchWriteError` (Beanie shape): per-item
   `_validate_entity` failure isolated, others created; outcome invariant (disjoint, complete) for every
   scenario; `authorize_create` denial raises before any UoW; authorization called once per call;
   `TenantAwareService` stamps the ambient tenant on every entity and a DTO-supplied tenant is
   overwritten; conflict → singles fallback attributes the duplicate and creates the rest; Beanie shape →
   prefix created, dup failed, remainder retried in a new UoW; infra `RuntimeError` mid-batch → `fatal`
   set, survivors `unresolved`, validation failures stay `failed`; singles fallback infra failure after
   two commits → those two `created`; one `EntityCreatedEvent` + one `_after_create` per created item in
   index order, none for failed; an `_after_create` that raises on item 2 → items 3+ still get events,
   `post_commit_failures == 1`, nothing raised; `CancelledError` propagates; MRO with `AuditLogMixin`
   (one audit event per created entity), `SoftDeleteService`, `ValidatorServiceMixin`;
   `CacheServiceMixin` invalidates the list once per `create_each` and once per `create_many`
   (regression for premise 4); empty input → no I/O.
   (b) Implement.

4. [ ] **Ingest exceptions + settings (E2)** — new `varco_core/varco_core/exception/ingest.py` (table
   above; lazy re-export from `varco_core.exception` and top-level `varco_core.__all__` like 067's cursor
   errors); `varco_fastapi/varco_fastapi/streaming.py` (`StreamingSettings` `ingest_*` fields,
   docstring); i18n default messages wherever `varco.error.*` keys are catalogued.
   (a) `varco_core/tests/test_ingest_exceptions.py`: `error_code_for` → 413/422/400/422 with the four
   codes; `error_params()` shapes; `IngestLineInvalidError.from_pydantic(err)` caps at 20, sets
   `truncated`, and contains no `input`/`url`/`ctx` (assert a sentinel input string is absent);
   `isinstance(..., ServiceValidationError)`. `varco_fastapi/tests/test_streaming_response.py` (extend
   067's): env parsing of every `VARCO_STREAMING_INGEST_*`; bounds validation; DI-resolved from
   `bootstrap()`.
   (b) Implement.

5. [ ] **Ingest engine + report writer** — new `varco_fastapi/varco_fastapi/ingest.py`: `IngestLimits`
   (frozen), `IngestSummary` (pydantic; the `$summary` payload), `IngestReportWriter` (spool,
   `write_window()`, `write_terminal()`, `replay()`), `run_ingest(chunks, *, create_each, dto_cls,
   limits, writer, envelope) -> IngestSummary` (the loop of the Design diagram: `_counted`,
   `aiter_ndjson`, `_deadline`, per-line classification, windows, `BatchOutcome` → line mapping,
   `last_line`/`unresolved` computation, terminal handling, `ClientDisconnect` handling). Imported lazily
   by the route factory (no import-budget impact).
   (a) `varco_fastapi/tests/test_ingest_engine.py` (pure, scripted `create_each`, async byte generators):
   line-level errors (invalid JSON, invalid UTF-8, over-long line with a 2 KiB cap → one failure and
   resync, `{"$next": "x"}` control line, a JSON array/number/null → `IngestLineInvalidError`) each at
   its exact physical line; blank lines and `\r\n` and a final line without `\n`; **batch boundaries**
   (batch 1, 3, exactly N, > total; a failure on the first and the last line of a batch); window ordering
   (errors sorted by line, report lines ≤ `last_line` only); **partial-failure report** (`report=errors`
   lists only failures; `report=all` one line per non-blank line in order with pks); fatal in batch 2 →
   batch-1 outcomes, terminal `$error` without `line` and without `str(exc)`, `unresolved` = batch-2
   range, `last_line` = end of batch 1, `complete=false`; ceilings: bytes (chunked), lines,
   failed-lines (`0` aborts on the first), seconds (a generator that stalls; deadline never cancels a
   flush); `ClientDisconnect` → pending window not flushed, WARNING logged, earlier batches counted;
   spool rollover with a tiny threshold replays identical bytes; spool write failure → `$error` +
   `$summary` only; tracemalloc: peak for 50 000 vs 5 000 lines differs by < 2× (engine only).
   (b) Implement with full docstrings (`Async safety`, `Edge cases`).

6. [ ] **Middleware exemptions** — new `varco_fastapi/varco_fastapi/middleware/_streaming_route.py`;
   `varco_fastapi/varco_fastapi/middleware/body_limit.py:200-242` (lazy lookup on both rejection paths,
   module DESIGN block); `varco_fastapi/varco_fastapi/middleware/idempotency.py:327-342` (refusal before
   `request.body()`).
   (a) `varco_fastapi/tests/test_streaming_route_exemption.py`: `route_streams_body` true for a marked
   endpoint, false for unmarked/unknown path/`PARTIAL` (wrong method), recurses into a `Mount`ed
   sub-app, survives `include_router` and a `functools.wraps` wrapper (062's endpoint wrapper shape),
   returns false when `scope["app"]` is absent; **BodyLimit**: marked route + `Content-Length` 12 MiB →
   reaches the handler; marked route + chunked 12 MiB → reaches it; unmarked → 413 as before (every
   existing `test_body_limit_middleware.py` test unchanged and green); a spy proves no lookup for an
   under-limit request; **Idempotency**: marked route + key → 400 `VARCO_IDEMPOTENCY_001` and the
   request body generator was never iterated; marked route without key and `require_key=True` → passes;
   unmarked routes unchanged.
   (b) Implement.

7. [ ] **`IngestMixin` + `POST /ingest`** — `varco_fastapi/varco_fastapi/router/mixins.py` (`IngestMixin`,
   ClassVars, `__all__`), `router/introspection.py:306-313,379-392` (`"ingest"` row; `request_model =
   C`), `router/base.py` (`_make_ingest_handler` + pre-flight; registry at `:1185-1192`; `_register_route`
   `:759-774`: `response_class`, `openapi_extra`; `mark_streaming_body`), `router/mcp.py`,
   `router/skill.py`, `router/a2a/router_source.py` (skip `ingest`), top-level `varco_fastapi.__all__`
   (`IngestMixin`). **062 obligation** (§D-atomicity): set the route's `transaction=False` if 062 has
   landed; otherwise leave a comment at the registry naming the obligation for 062's builder.
   (a) `varco_fastapi/tests/test_ingest_route.py` (in-process ASGI, in-memory `BulkServiceMixin`
   service): 1 250 lines / batch 500 → 3 batches, `$summary` exact, rows, events and audit per entity;
   `?report=all`; all Step 5 line-level cases end-to-end, asserting a raw sentinel value from a failing
   line never appears in the body; **auth**: CREATE denied → 403 JSON envelope and the body generator was
   never iterated; authorizer flips to deny after batch 1 → 200, batch 1 created, terminal `$error`
   `status: 403`; no `server_auth` → anonymous ctx like `create`; **tenancy**: two tenants ingest
   concurrently, each sees only its rows, a line carrying the other tenant's id is stamped with the
   caller's; missing tenant → `TENANT_REQUIRED` status pre-flight; **pre-flight**: `application/json` →
   415, `application/x-ndjson` and `; charset=utf-8` accepted, `charset=utf-16` → 415,
   `Content-Encoding: gzip` → 415, `Accept: application/json` → 406, `Content-Length` > `ingest_max_bytes`
   → 413 JSON; a 12 MiB chunked ingest succeeds under the default `BodyLimitModule`; `Idempotency-Key` →
   400; headers (`Content-Type` per `Accept`, `Cache-Control: no-store`, `X-Accel-Buffering: no`,
   status 200); ClassVar lowering applies, ClassVar raising is clamped; OpenAPI: POST `/ingest`, no JSON
   body param, `requestBody` media types with `itemSchema` → `C`, `_ingest_path` rename; a router without
   `IngestMixin` has no `/ingest`; a non-`BulkServiceMixin` service → `TypeError`; MCP/skill/A2A tool
   lists contain no ingest action; `?with_async=true` ignored; **062 guard**
   (`pytest.importorskip("varco_core.transaction")`): with `TransactionModule` installed and
   `_transaction = True` on the router, `current_transaction() is None` inside each `create_each` call.
   (b) Implement.

8. [ ] **Client `ingest()`** — new `varco_fastapi/varco_fastapi/client/ingest.py` (`IngestResult`:
   `summary`, `failures: list[...]`, `created: list[...]` (report=all), `fatal: Mapping | None`,
   `raise_for_fatal()` → `NdjsonStreamError`); the `"ingest"` branch in `client/base.py`'s CRUD method
   builder beside 067's `export`. Upload: an async generator over `AsyncIterable | Iterable` of
   `C | Mapping`, `encode_record` per item, coalesced to 64 KiB chunks → httpx `content=` (chunked); the
   response parsed with `aiter_ndjson`. Same httpx client/auth headers as 067's `ExportStream`.
   (a) `varco_fastapi/tests/test_client_ingest.py` against the Step 7 app: a 10 000-item async iterator
   ingests without materializing a list (generator consumed lazily — instrumented); failures carry
   `line`; `report="all"` fills `created`; a terminal `$error` sets `fatal` and `raise_for_fatal()`
   raises; a pre-flight 403 raises the client's existing HTTP error type.
   (b) Implement.

9. [ ] **SQLAlchemy integration (Postgres)** — new `varco_sa/tests/test_ingest_integration.py`
   (`pytestmark = pytest.mark.integration`, session `postgres_url`, `uuid4().hex[:8]`-namespaced table
   and tenants per CLAUDE.md §Test Conventions). Serve the app with an **in-process uvicorn server on an
   ephemeral port** (real flow control, real `Expect: 100-continue`), client = httpx streaming upload.
   (i) **Bounded memory, large upload:** 200 000 lines (~30 MB) vs 20 000 lines through the client
   generator — `tracemalloc` peak ratio ≤ 2 (a buffered implementation is ~10×), every row present;
   (ii) a unique natural-key column: a file with 37 duplicates scattered across batch boundaries → exactly
   37 per-line 409 `CONFLICT`s at the right lines, everything else committed, no driver text in the body;
   (iii) re-ingesting the same file → every line 409, zero new rows; (iv) a `max_lines` abort mid-file,
   then resuming from `last_line + 1` → the exact set, no duplicates; (v) `Expect: 100-continue` with a
   denied caller → 403 and zero body bytes received by the server (byte-counting ASGI wrapper);
   (vi) `engine.pool.checkedout() == 0` after the ingest (one connection per batch, released);
   (vii) `report=all` pks equal the DB's.

10. [ ] **Beanie integration (MongoDB)** — new `varco_beanie/tests/test_ingest_integration.py` (session
    `mongo_url`, namespaced database, unique index on a natural-key field): (i) of Step 9; a duplicate at
    index 250 of a 500-line batch → lines 1–249 and 251–500 of that batch created, line 250 409 (the
    `BatchWriteError` path, proven against a real server); a file of all-duplicates → every line 409, no
    new documents; resume after an abort; `report=all` pks equal the stored `_id`s.

11. [ ] **Docs** — `technical_docs/features/streaming.md` (067's file) gains an **"Ingest"** section:
    wire format, the report contract (`$error`+`line` vs terminal, `$summary` fields, the resolution
    invariant and resume recipe), pre-flight vs ingest-phase statuses, §D-duplex with the verified source
    citations, per-batch atomicity, creates-only, the ceilings table, the body-limit exemption and its
    fail-closed rule, the `Idempotency-Key` refusal and natural-key idempotency, the 062 `transaction=False`
    rule; **Pitfalls rows**: nginx/ingress request buffering and body-size limits in front of the route,
    an aborted ingest still uploads the remainder, `?report=all` needs a writable `TMPDIR` past the
    spool threshold, gzip bodies → 415, rate limiting counts one request per ingest, re-uploading an
    `/export` file (its `$next` line is rejected), duplicates without natural keys on retry, Beanie
    partial batches and `unresolved`, a slow client hits `ingest_max_seconds`, `create_each` inside an
    app's own `@transactional` joins it. **README** — a "Streaming bulk ingest (NDJSON)" subsection after
    067's export subsection (router + `BulkServiceMixin` service, `curl -H 'Content-Type:
    application/jsonl' -H 'Transfer-Encoding: chunked' --data-binary @orders.jsonl`, client `ingest()`),
    TOC. **ARCHITECTURE.md** — package-map lines for `varco_fastapi.ingest`, `client/ingest.py`,
    `middleware/_streaming_route.py`, `varco_core.exception.ingest`/`.batch`; the Service Layer type
    hierarchy gains `create_each`/`BatchOutcome`. **CLAUDE.md** — one decision-tree line under
    067's streaming line: `↳ Bulk NDJSON ingest? → IngestMixin + BulkServiceMixin.create_each
    (varco_fastapi.ingest) — never a buffered JSON-array POST, never VARCO_BODY_LIMIT_EXEMPT_PATHS for
    it`. **CHANGELOG.md `[4.0.0]`** — *Added* (`IngestMixin`, `create_each`/`authorize_create`/
    `BatchOutcome`, `_after_bulk_create`, `BatchWriteError`, ingest exceptions, `ControlKind.SUMMARY`/
    `encode_summary`, `StreamingSettings.ingest_*`, client `ingest()`), *Changed* (SA `save_many` raises
    `ServiceConflictError` on integrity violations; Beanie `save_many` raises `BatchWriteError`;
    `BodyLimitMiddleware`/`IdempotencyMiddleware` ingest-route behaviour), *Fixed* (`create_many` honours
    its documented `ServiceConflictError`; `CacheServiceMixin` invalidates lists after `create_many`).
    **`technical_docs/migrations/3-to-4.md`** — one row: code catching `IntegrityError` /
    `BulkWriteError` around `save_many`/`create_many` now catches `ServiceConflictError` /
    `BatchWriteError`.

12. [ ] **Snapshot gates + full verification** — regenerate and commit: `uv run python
    scripts/api_surface.py` (new `__all__` names), `make settings-reference` (`StreamingSettings`
    fields); run `make di-catalogue-check`, `make entry-points-check`, `make asyncapi-check` and **assert
    no diff** (no new binding, wiring verb or consumer); `uv run python scripts/import_budget.py --check
    --warn-only` (expect no breach — `varco_fastapi.ingest` is imported lazily); then §Verification.
    Regenerated artifacts land in the same change as the code (CLAUDE.md: hard requirement for `api-check`).

## Edge cases

- Empty body / only blank lines → 200, `$summary{records: 0, created: 0, complete: true, last_line: N}`.
- Final line without `\n` → processed; `\r\n` → accepted; a UTF-8 BOM → stripped (067).
- Over-long line → one `IngestLineMalformedError` at that line, memory capped, decoding resyncs.
- A control-shaped line (`$next`/`$error`/`$summary`/unknown `$x`) → `IngestLineMalformedError("control record")`.
- A JSON value that is not an object → `IngestLineInvalidError` (pydantic `dict_type`).
- Duplicate natural key inside one batch → SA: singles fallback, first created, second 409; Beanie:
  ordered prefix created, duplicate 409, remainder retried.
- Duplicate of a pre-existing row → per-line 409 on both backends; message never contains the value.
- Database outage in batch k → batches `< k` committed and reported; terminal `$error` (status 500/503,
  no `str(exc)`); `unresolved` = batch k's range; `last_line` before it.
- Authorization revoked mid-ingest → terminal `$error` with `status: 403` after the committed batches.
- `ingest_max_failed_lines = 0` → aborts on the first failed line after flushing the window.
- Ceiling hit mid-line (bytes) → the partial line is discarded; `last_line` = the last complete line.
- `Content-Length` lies (shorter than the body) → the route's counter still enforces `ingest_max_bytes`.
- Client disconnects mid-upload → no response; pending window not committed; earlier batches stay.
- `CancelledError` (shutdown) → propagates; the in-flight UoW rolls back; post-commit work of a batch
  committed in the same instant may not run (same as `create()`).
- An `_after_create` hook raises → the line is still reported created; `post_commit_failures` counts it; ERROR log.
- A cached service (`CacheServiceMixin`) → list cache invalidated once per batch.
- `TenantAwareService` without an ambient tenant → `TENANT_REQUIRED` status pre-flight, nothing read.
- `Idempotency-Key` present → 400 before any body byte; absent with `require_key=True` → allowed.
- Ingest route mounted inside a `Mount`ed sub-app → exemption still resolves (recursion); deeper than 4
  levels or unresolvable → 10 MiB ceiling applies (fail closed).
- `?with_async=true` → ignored (not async-capable).
- An entity pk literally `"ingest"` → irrelevant (no `POST /{id}` route exists).
- A `CreateDTO` with `extra="forbid"` and a `version` member on a line → per-line 422 (§D-scope).
- `PKStrategy.INT_AUTO` on Beanie → `report=all` pk may be `None` (pre-existing Beanie limitation,
  premise 6); documented.

## Verification

```bash
uv sync --all-packages --all-extras
uv run pytest varco_core/tests/test_ndjson.py varco_core/tests/test_bulk_create_each.py \
              varco_core/tests/test_bulk_service.py varco_core/tests/test_ingest_exceptions.py -q
uv run pytest varco_sa/tests/test_save_many_conflict.py varco_beanie/tests/test_save_many_bulk_error_unit.py -q
uv run pytest varco_fastapi/tests/test_ingest_engine.py varco_fastapi/tests/test_streaming_route_exemption.py \
              varco_fastapi/tests/test_ingest_route.py varco_fastapi/tests/test_client_ingest.py \
              varco_fastapi/tests/test_body_limit_middleware.py varco_fastapi/tests/test_streaming_response.py -q
make test                                     # all eleven suites
make integration-test-clean PKG=varco_sa      # Step 9 (Docker)
make integration-test-clean PKG=varco_beanie  # Step 10 (Docker)
make lint                                     # ruff + format --check + api/asyncapi/settings/di/entry-points gates
make type-check                               # mypy strict, ten dirs
rg -n 'exempt_paths' varco_fastapi/varco_fastapi/router/   # the ingest route never relies on exempt_paths (no hits)
rg -n '__varco_streaming_body__' varco_*/varco_*            # set only in _streaming_route.py, read only there
```

## Risks

- **Silent body truncation** if anything ever streams the report while the body is still being read —
  invariant: `run_ingest` returns before `NdjsonResponse` is constructed; Step 7 asserts the handler has
  exhausted `request.stream()` before returning.
- **Unbounded buffering on the exempt route** — invariant: no code on the ingest path calls
  `request.body()`; `IdempotencyMiddleware` refuses before buffering (Step 6 asserts the generator was
  never iterated).
- **Exemption scope creep** — invariant: `__varco_streaming_body__` is set only by `_make_ingest_handler`
  (Verification `rg`); a request for a public marker is the `BACKLOG.md:140` un-park trigger.
- **PII in the report or logs** — invariant: no `str(driver_exc)` in a `ServiceConflictError` message,
  no pydantic `input`, no line content in logs; Steps 2/4/7 assert sentinels are absent. ⚠️ App-authored
  `ServiceValidationError` messages may still echo values (redaction is key-name based, Plan 040).
- ⚠️ ASSUMPTION — **ASGI spec wording** on `receive()` after `http.response.start` not re-read; uvicorn
  0.42.0 behaviour verified (premise 1). Irrelevant by construction (§D-duplex).
- ⚠️ ASSUMPTION — **other ASGI servers / proxies** (hypercorn, granian, nginx buffering) — the
  half-duplex design is safe on all; proxy body limits are an operator Pitfall.
- ⚠️ ASSUMPTION — **PyMongo `BulkWriteError.details`** keys and the ordered-prefix semantics; Step 2
  verifies in source before coding, Step 10 proves against a real server.
- ⚠️ ASSUMPTION — **httpx `ASGITransport` streams a generator request body lazily** chunk by chunk (unit
  tests in Steps 6–8 rely on it to assert "never iterated"); if it pre-reads, those assertions move to
  Step 9's uvicorn harness.
- ⚠️ ASSUMPTION — **FastAPI never reads the body for a handler without a body parameter**; Step 9(i)'s
  memory ratio and Step 7's generator assertion are the proof.
- ⚠️ ASSUMPTION — **062's endpoint wrapper uses `functools.wraps`** (copies `__dict__`, so the marker
  survives); Step 6 tests a `wraps` wrapper; if 062 builds a fresh function, it must copy the marker.
- ⚠️ ASSUMPTION — **Mongo transaction lifetime default (60 s)** — used only as a rejection argument.
- **SA `IntegrityError` at commit (deferred constraints)** escapes `save_many`'s translation → the batch
  becomes `fatal`/`unresolved` (correct, just coarse). Documented.
- **`_after_bulk_create` on `AsyncService`** — a new non-abstract hook; invariant: default no-op, every
  override chains `super()` (MRO rule).
- **Brief 021 §D claim (premise 1)** — the brief is not edited by this plan; the handback asks the
  coordinator to add an in-tree "corrected" banner, as was done for research 002.

## Out of scope / Parked (for the coordinator to record; this plan does not edit BACKLOG.md)

- **Upsert/update ingest** (per-line `expected_version`, 061) · ⏵ a consumer needs to refresh an existing
  dataset by natural key and a delete + re-ingest is unacceptable.
- **All-or-nothing ingest** (spooled body + one 062 boundary, SA only) · ⏵ a consumer needs atomic load of
  a file above the JSON-body limit on Postgres.
- **Job-backed ingest** (object storage + 069 job + report artifact) · ⏵ ingests routinely exceed
  `ingest_max_seconds`, or must survive a pod restart.
- **Compressed request bodies** (streaming `zlib.decompressobj`, ratio cap) · ⏵ a consumer's upload
  bandwidth is the bottleneck.
- **Per-batch transient retry** (`TRANSACTION_RETRY` per `create_each`, post-062) · ⏵ measured
  40001/40P01 aborts on concurrent ingests.
- **Natural-key pre-check to avoid the singles fallback** · ⏵ re-ingest of large mostly-duplicate files
  is a measured bottleneck.
- **`Idempotency-Key` on ingest** · ⏵ two consumers whose SDKs cannot omit the header.
- **Tracing span per batch** (`TracingServiceMixin` covers `create` only) · ⏵ an operator needs per-batch
  latency in traces.
- **Ingest in `ServiceContract` / `gen-client` / MCP / A2A** · ⏵ a cross-repo consumer needs a generated `ingest()`.

## Done — this plan

- `POST {prefix}/ingest` streams an NDJSON body batch-by-batch through `create_each` → `save_many` with
  every per-entity guarantee of `create()`, and answers with a `$error`/`$summary` report honouring the
  resolution invariant (Steps 3, 5, 7).
- Duplicate keys are per-line 409s on SA and Beanie, with no driver text (Steps 2, 9, 10).
- The route is exempt from the 10 MiB default automatically and fail-closed, enforces its own ceilings,
  and refuses `Idempotency-Key` before buffering (Step 6).
- Bounded memory for a 200 000-line upload proven on Postgres and MongoDB (Steps 9, 10).
- 062's `transaction=False` obligation pinned by a guarded test (Step 7).
- Client `ingest()`; docs, README, ARCHITECTURE, CLAUDE.md line, CHANGELOG `[4.0.0]`, `3-to-4.md` row;
  `make lint`, `make type-check`, `make test`, both integration runs green; snapshots committed.
