# Plan 067 — Streaming reads: NDJSON export over `AsyncService.stream()`, opaque keyset-cursor pagination + RFC 8288 `Link`, server-side cursors verified end-to-end (STREAM-1)

Covers BACKLOG 4.0.0-feature-extension row **`STREAM-1`** (`BACKLOG.md:61` — *"Streaming reads —
NDJSON export routes over `AsyncService.stream()`, opaque keyset-cursor pagination + RFC 8288
`Link`, server-side cursor (`yield_per`) verified end-to-end"*). Ships inside **4.0.0**
(`BACKLOG.md:31`: breaking changes allowed, each with a `3-to-4.md` section). Arrow/Parquet stays
parked (`BACKLOG.md:35`, `:111`).

**This plan OWNS the shared streaming line codec** (`varco_core.ndjson`: media type, line
encoder/decoder, control-record convention) and the HTTP adapter over it
(`varco_fastapi.streaming.NdjsonResponse`). **Plan 068** (STREAM-2, NDJSON bulk ingest) reuses both
verbatim; **Plan 065** (GDPR export) may reuse the encoder. Neighbours: **061** (version/ETag),
**062** (ambient transactions) — interactions in §D-neighbours, designed there, not here.

**Sources of record read for this plan (2026-10-07):** `BACKLOG.md:19-63,111,140,184`;
`plans/058` (house format); research `design/research/021-transactions-conditional-requests-streaming.md`
§A.5, §D, Options table, Evidence gaps; `design/research/019-streaming-orchestration-and-test-performance-2026.md`
§A + Options + Version notes; `varco_core/service/base.py:789-1067`; `varco_core/dto/pagination.py:1-60,100-245`;
`varco_core/query/params.py`, `query/type.py:20-41,80-85,207-224`; `varco_core/exception/service.py:20-44,239-278`;
`varco_sa/repository.py:320-394,576-590`; `varco_sa/factory.py:296-335`; `varco_sa/query/compiler.py:299-343`;
`varco_beanie/repository.py:457-508`; `varco_beanie/factory.py:57,174,228-232`; `varco_beanie/query/compiler.py:15-36,125-200`;
`varco_fastapi/router/base.py:85-116,205-331,566-607,626-883,1093-1192`; `router/pagination.py`;
`router/mixins.py:304-473`; `router/introspection.py:303-414`; `router/mcp.py:250-268,704-711`;
`router/skill.py:603-609`; `router/a2a/router_source.py:167-173`; `client/base.py:565-603,800-850`;
`middleware/error.py:79-150`; `middleware/idempotency.py:30-89`; `middleware/cors.py:131`;
`varco_fastapi/di.py:483-512`; installed sources in `.venv`: **FastAPI 0.135.2**
(`fastapi/routing.py:440-675,846-975`, `fastapi/openapi/utils.py:357-392`), **Starlette 1.3.1**
(`starlette/responses.py:222-283`), **SQLAlchemy 2.0.48** (`ext/asyncio/session.py:80,639-743`,
`ext/asyncio/result.py:44-63`, `ext/asyncio/base.py:110-174`, `dialects/postgresql/asyncpg.py:620-669,1027`),
**Beanie 2.0.1** (`odm/queries/cursor.py`, `odm/queries/find.py:223-262,663-689`), PyMongo 4.16.0, asyncpg 0.31.0.

> ⚠️ **Four premises corrected from source before anything was decided.**
> (1) **`AsyncService.stream()` has never worked on SQLAlchemy.** `varco_sa/repository.py:392` is
> `async with self._session.stream_scalars(stmt) as stream:  # type: ignore[attr-defined]`, but in
> SA 2.0.48 `AsyncSession.stream_scalars` is a plain `async def` returning `AsyncScalarResult`
> (`ext/asyncio/session.py:713-743`, no `@asyncstartablecontext`), and `AsyncCommon`/`AsyncScalarResult`
> define no `__aenter__` (`ext/asyncio/result.py:44-63,490`). `async with <coroutine>` raises
> `TypeError` on first iteration; the `type: ignore` hid exactly that. No test in `varco_sa/tests/` or
> `varco_beanie/tests/` calls `stream_by_query` (only `varco_core` fakes override it). **BACKLOG's
> "Repository streaming … already exist (verified 2026-10-07)" row (`BACKLOG.md:184`) is wrong for SA**
> — this plan fixes the code (Step 1); the row is the coordinator's to correct, not this plan's.
> (2) **No `yield_per` anywhere.** `AsyncSession.stream()` always sets `stream_results=True`
> (`session.py:80,674-679`) so asyncpg's `AsyncAdapt_asyncpg_ss_cursor` (`server_side = True`,
> `asyncpg.py:620-647`) *would* be used, fetching 50 rows per round trip; `yield_per` is never set.
> Beanie never closes its PyMongo cursor on early exit (`BaseCursorQuery.cursor` is created lazily
> at `cursor.py:40-43` and dropped).
> (3) **`pk` is not a queryable field on either backend**, so a pk tie-breaker cannot be expressed
> today: SA maps a single PK to ORM attribute `id` (`varco_sa/factory.py:311-315`) and resolves sort
> and filter fields by `getattr(orm_cls, name)` (`repository.py:590`, `query/compiler.py:337` →
> `FieldNotFound`); Beanie maps it to document `id`/`_id` (`varco_beanie/factory.py:174,228-232`) and
> its compiler emits the raw field name (`query/compiler.py:15-36`) — `pk` silently matches nothing.
> (4) **The offset `Link` header reverses ascending sorts.** `router/pagination.py:118-121` tests
> `s.order == "asc"` but `SortOrder` values are `"ASC"`/`"DESC"` (`query/type.py:40-41`), so every
> field renders as `-field`. Fixed in Step 6 with a regression test.

> **Brief claims re-checked (U-8 discipline).** Brief 019 §A / 021 §D say FastAPI streams JSON
> Lines natively "since 0.134.0". **Behaviour verified in the installed 0.135.2 source**: a generator
> endpoint with the default `response_class` sets `is_json_stream` (`routing.py:966-975`), is wrapped
> in a `StreamingResponse(media_type="application/jsonl")` (`routing.py:620-652`), validates each item
> through `stream_item_field` (`routing.py:471-494`), and OpenAPI emits `itemSchema` under
> `application/jsonl` (`openapi/utils.py:357-372`). **The version number 0.134.0 is not verifiable
> from an installed wheel** (no changelog shipped) → ⚠️ ASSUMPTION, harmless: this plan does not use
> the native path (§D-native). Brief 021 §D's media-type registration status (`application/json-seq`
> IANA-registered via RFC 7464; `application/x-ndjson` and `application/jsonl` unregistered) is taken
> as-is → ⚠️ ASSUMPTION (Risks).

## Goal

After this plan: **`AsyncService.stream()` works on SQLAlchemy and Beanie** with a real server-side
cursor (SA `yield_per`, PyMongo `batch_size`) that is closed deterministically on completion, error,
or client disconnect — proven on Postgres and MongoDB by memory-bounded integration tests;
**`varco_core.ndjson`** is the one line codec (media type `application/jsonl`, `application/x-ndjson`
accepted; `$error`/`$next` single-key control records; a bounded-memory async line decoder);
**`varco_core.query.keyset`** builds a backend-agnostic keyset predicate with a pk tie-breaker and
encodes/decodes opaque base64url cursors (strictly typed scalars, optional HMAC-SHA256 with
rotation); **`pk` is a queryable logical field** on both backends; **`ListMixin` gains an opt-in
cursor mode** (`_list_pagination = "cursor"`) with RFC 8288 `Link: <…>; rel="next"` and
`PagedReadDTO.next.cursor`; **a new opt-in `ExportMixin`** adds `GET /export` streaming NDJSON over
`AsyncService.stream()` with prime-before-headers error mapping, a row/time ceiling that emits a
resumable `$next` cursor, and a `$error` trailer for mid-stream failures; the generated client gains
`list(cursor=…)` and `export()`; docs, CHANGELOG, a short `3-to-4.md` note, and every snapshot gate
are green.

## Non-goals

- **Streaming ingest** (chunked NDJSON upload, per-line validation, body-limit exemption) — Plan 068.
  This plan only *provides* the decoder 068 consumes.
- **Arrow / Parquet / CSV export** — Arrow/Parquet parked (`BACKLOG.md:35,111`); CSV not asked.
- **Flipping `ListMixin`'s default to cursor pagination** — rejected (§D-mode); parked with a trigger.
- **Null-aware keyset ordering** (nullable sort keys in cursor mode) — refused with 422 (§D-sortkeys);
  parked with a trigger.
- **`rel="prev"`/`"last"`/`"first"` links**, backward keyset traversal — `next` only.
- **ETag / `If-None-Match` on list or export responses, or per-line ETags** — Plan 061 owns ETags and
  scopes them to single resources (§D-neighbours).
- **Designing how `stream()` composes with ambient transactions** — Plan 062 (§D-neighbours states the
  constraint only).
- **Exposing export through MCP / A2A skills / `ServiceContract`/`varco gen-client` codegen** — an
  NDJSON stream is not a tool result; the CRUD `export` action is skipped by those adapters (asserted).
- **A new `Action.EXPORT` permission or an export audit event** — export authorizes as `Action.LIST`
  (§D-auth); an "export happened" audit record is parked.
- **New `AsyncService` methods.** The HTTP layer composes `list()`/`stream()`/`count()` with
  `QueryParams`; no service signature changes.
- **GZip/compression middleware** — varco ships none; a docs Pitfall covers app-installed ones.
- **`BACKLOG.md` edits** — owned by the cycle rollup; the handback reports premise (1).

## Context — verified facts this plan builds on

| # | Fact | Source |
|---|---|---|
| F1 | `stream()` is an async generator: LIST `authorize()` (`:1047-1051`), `_scoped_params()` (`:1055`) and the UoW (`:1065-1067`) all run on the **first `__anext__`**, not at call time | `varco_core/service/base.py:998-1067` |
| F2 | `list()` = LIST authorize + `_scoped_params` + `find_by_query`; `count()` same gate; `paged_list()` gathers both per page | `base.py:789-945` |
| F3 | SA stream bug — premise (1) | `varco_sa/repository.py:387-394` |
| F4 | `AsyncSession.stream_scalars(stmt, execution_options=…)` accepts `execution_options`; result has async `close()` | `session.py:713-743`; `result.py:50-53` |
| F5 | Beanie `find(filter, batch_size=N)` forwards to PyMongo (`**pymongo_kwargs`); `FindMany.cursor` holds the live PyMongo cursor after first `__anext__` | `find.py:223-262,682-689`; `cursor.py:37-47` |
| F6 | `QueryParams` / `SortField` frozen; `Scalar = int\|float\|str\|bool` (datetime/UUID/Decimal only at runtime) | `params.py:36-71`; `type.py:24-25,210-224` |
| F7 | Offset envelope: `PageCursor(limit, offset: int (required, ge=0), sort, query)`; list handler calls `count()` on **every** page | `dto/pagination.py:119-153`; `router/base.py:1131-1152` |
| F8 | CRUD routes: `_CRUD_ROUTES` table + `_CRUD_HANDLER_FACTORIES` registry; literal paths sort before parameterized | `introspection.py:286-313`; `router/base.py:1185-1192` |
| F9 | Router DI pattern: `VarcoRouter.__init__(job_runner: Instance[...] \| None)`; `VarcoCRUDRouter` forwards | `router/base.py:566-607`; `router/crud.py` |
| F10 | `ErrorMiddleware` is a `BaseHTTPMiddleware` — cannot change status after `http.response.start` | `middleware/error.py:79-150` |
| F11 | Starlette 1.3.1 `StreamingResponse`: ASGI ≥2.4 → `OSError` on `send` re-raised as `ClientDisconnect`, **body iterator not closed**; <2.4 → task-group cancel | `starlette/responses.py:248-283` |
| F12 | `IdempotencyMiddleware` treats a response without `Content-Length` as streaming and releases | `middleware/idempotency.py:36-56` |
| F13 | CORS already exposes `Link`, `X-Total-Count` | `middleware/cors.py:131` |
| F14 | MCP / skill / A2A list adapters always send `offset=body.get("offset", 0)`; client `list()` takes `q/sort/limit/offset` | `mcp.py:250-268,704-711`; `skill.py:603-609`; `a2a/router_source.py:167-173`; `client/base.py:572-603` |
| F15 | Settings are bound by `bind_settings()` in `varco_fastapi.di.bootstrap()`'s tuple | `varco_fastapi/di.py:496-505` |
| F16 | `ServiceValidationError` → HTTP 422; class→code map in `exception/http.py` | `exception/service.py:239-278`; `exception/http.py:~192` |

## Design

```
 GET /orders?q=…&sort=-created_at&cursor=…        GET /orders/export?q=…&sort=…&cursor=…&limit=…
        │  ListMixin (_list_pagination="cursor")           │  ExportMixin (opt-in)
        ▼                                                  ▼
 varco_core.query.keyset ── with_tiebreaker(sort, tiebreaker_fields(D))  → (…, pk)
                         ── decode_cursor(token, secrets, expected fingerprint)  (strict scalars, HMAC opt.)
                         ── apply_keyset(params, cursor, sort, limit+1)  → node AND predicate
        │                                                  │
 service.list(params, ctx)   (+ count() on first page)     service.stream(params, ctx)  ── primed: first
        │                                                  │   __anext__ runs auth/scoping/SQL BEFORE headers
        ▼                                                  ▼
 PagedReadDTO{results, next{cursor}} + Link rel="next"   NdjsonResponse (aclosing, 64 KiB coalescing)
                                                          lines: ReadDTO JSON … [$next | $error]
                                                          ▲
 varco_core.ndjson  (encode_record / encode_error / encode_next / aiter_ndjson / media types) ── reused by 068, 065
 Repositories: SA stream_scalars(yield_per) + close();  Beanie find(batch_size) + cursor.close();  `pk` resolvable
```

### §D-codec — the shared line codec (`varco_core/varco_core/ndjson.py`) — **068/065 reuse this**

- **Media type.** Emit `application/jsonl` (`NDJSON_MEDIA_TYPE`); accept `application/jsonl` and
  `application/x-ndjson` (`NDJSON_MEDIA_TYPES`) in `Accept` (export) and `Content-Type` (068 ingest).
  When a client's `Accept` names only `application/x-ndjson`, echo it — same bytes.
- **Line format.** One UTF-8 JSON value per line, terminated by `\n` (never `\r\n` emitted; `\r\n`
  accepted on decode). No BOM emitted (a leading UTF-8 BOM is stripped on decode). Encoders are
  compact (no raw newline can appear: JSON escapes control characters). **Decoders split on `b"\n"`
  only** — never `str.splitlines()`, which also splits on U+2028/U+2029/`\x1c-\x1e`/`\x85` that may
  legally appear raw inside JSON strings.
- **Data lines are unchanged DTO JSON** — exactly `ReadDTO.model_dump_json(by_alias=True)`, the same
  bytes a `GET /{id}` body or a `results[]` item carries, so `jq -c`, `pandas.read_json(lines=True)`,
  DuckDB `read_json_auto` work on the raw body.
- **Control records** are JSON objects with **exactly one key from a reserved `$`-prefixed set**
  (`ControlKind`): `{"$error": {…}}` and `{"$next": "<cursor>"}`. A pydantic field cannot be named
  `$error` without an explicit alias, so a data line can only collide by deliberate aliasing;
  `encode_record()` refuses (`ValueError`) a value that would serialize to a control shape.
  - `$error` payload = the **same envelope `ErrorMiddleware` would have rendered** (`code`, `message`,
    `correlation_id`, … via the shared helper — Step 5) plus `status` (the HTTP status it would have
    carried) and, for 068's per-line report, `line`. Required keys: `code`, `message`.
  - `$next` = an opaque keyset cursor to resume from (§D-limits).
  - **Completion rule:** a stream that ends cleanly with neither `$error` nor `$next` is complete. A
    transport-level truncation (pod kill, network drop) is an incomplete chunked body, which every
    HTTP client surfaces as a protocol error. Application failure → `$error`; transport failure → HTTP.
  - New kinds are added by extending `ControlKind` (068 may add one). A decoder that meets an
    unknown `$`-prefixed single-key object sets `NdjsonLine.control_raw` to that key (and `control`
    to `None`), so an older reader can stop safely instead of mis-reading it as a data line.
- **API** (all pure, no DI, stdlib-only — `json`, `codecs`; no import-budget impact):

```python
NDJSON_MEDIA_TYPE: Final = "application/jsonl"
NDJSON_MEDIA_TYPES: Final[frozenset[str]] = frozenset({"application/jsonl", "application/x-ndjson"})
DEFAULT_MAX_LINE_BYTES: Final = 1_048_576

class ControlKind(StrEnum):
    ERROR = "$error"
    NEXT = "$next"

def is_ndjson_media_type(content_type: str | None) -> bool          # ignores parameters (charset=…)
def encode_record(value: BaseModel | Mapping[str, Any]) -> bytes     # one line incl. b"\n"
def encode_error(payload: Mapping[str, Any]) -> bytes                # requires "code","message"
def encode_next(cursor: str) -> bytes

@dataclass(frozen=True)
class NdjsonLine:
    number: int                        # 1-based physical line number (blank lines counted, skipped)
    value: Any = None                  # parsed JSON; None when error is set
    error: str | None = None           # invalid UTF-8 / invalid JSON / over max_line_bytes
    control_raw: str | None = None     # "$error"/"$next"/unknown "$x" for a control-shaped object
    @property
    def control(self) -> ControlKind | None: ...
    @property
    def payload(self) -> Any: ...      # the control record's inner value

async def aiter_ndjson(
    chunks: AsyncIterable[bytes], *, max_line_bytes: int = DEFAULT_MAX_LINE_BYTES,
) -> AsyncIterator[NdjsonLine]          # never raises on a bad line: yields NdjsonLine(error=…);
                                        # an over-long line is discarded up to the next b"\n"
                                        # (memory bounded by max_line_bytes), reported, and decoding resyncs
class NdjsonStreamError(Exception):     # raised by readers that treat $error as fatal (the client)
    payload: Mapping[str, Any]
```

`encode_record` for a `Mapping` uses `json.dumps(separators=(",", ":"), ensure_ascii=False,
allow_nan=False, default=_json_default)` with `_json_default` handling `datetime`/`date` (ISO 8601),
`UUID`/`Decimal` (str), `Enum` (`.value`), `BaseModel` (`model_dump(mode="json", by_alias=True)`).

DESIGN: `application/jsonl` + `$`-keyed control records over RFC 7464 `application/json-seq` or a
per-line discriminated union
  ✅ `application/jsonl` is what FastAPI 0.135.2 itself emits and documents in OpenAPI
     (`routing.py:649`, `openapi/utils.py:372`) — one media type across an app's native and varco routes
  ✅ Data lines stay plain DTO JSON — line tools and dataframe loaders work on the raw body
  ✅ One error convention for export, 068's per-line failures, and 065's job output — the `$error`
     payload is the existing error envelope, so client error handling is shared with non-streamed responses
  ❌ Neither `jsonl` nor `x-ndjson` is IANA-registered (brief 021 §D); `json-seq` is, but its `0x1E`
     record prefix breaks every line tool and no mainstream client library speaks it
  ❌ A reader that ignores `$error`/`$next` mis-counts by one line — documented; the varco client and
     `aiter_ndjson` classify them

### §D-native — own `NdjsonResponse`, not FastAPI's native generator endpoints

DESIGN: `varco_fastapi.streaming.NdjsonResponse(StreamingResponse)` over FastAPI's yield-based JSONL
  ✅ **Errors before headers become real statuses**: the handler *primes* the iterator (§D-prime)
     before returning a response; a native generator endpoint's body does not run until Starlette
     iterates it — i.e. after `200` is on the wire (F1, F10)
  ✅ Mid-stream errors become a `$error` trailer + clean end; the native path (`routing.py:629-634`)
     just propagates, aborting the connection with no application signal
  ✅ Deterministic resource release: `aclosing()` around the record iterator — Starlette's ≥2.4-spec
     path never closes `body_iterator` on disconnect (F11), which would leave the DB cursor + UoW open
     until GC
  ✅ No double work: `ReadDTO`s are already validated; native re-validates every item
     (`routing.py:471-491`)
  ✅ Codec is framework-free (`varco_core`) — usable by 065's job-based export writing to storage
  ❌ OpenAPI `itemSchema` is not automatic — Step 7 attaches it (`responses=`), mirroring FastAPI's
     own shape (`openapi/utils.py:357-372`), pinned by a test
  ❌ Media type and coalescing are ours to maintain

### §D-prime — pre-data errors keep their HTTP status

The export handler calls `first = await anext(records, _END)` **before** constructing the response.
That first `__anext__` runs LIST authorization, `_scoped_params`, query compilation (`FieldNotFound`,
`OperationNotSupported`), and the first DB fetch — so a 403 / 404-oracle / 422 / `TENANT_REQUIRED`
still flows through `ErrorMiddleware` as a normal JSON error. Cursor decoding and sort-key checks run
even earlier (pure, before any service call). Only failures *after* the first row (connection loss, a
row that fails `to_read_dto`) become `$error`. If anything raises between priming and returning the
response, the handler `aclose()`s the iterator. The list route needs no priming: `list()` is a
coroutine that raises before the handler returns.

### §D-mode — keyset cursors **alongside** offset, opt-in per router; default unchanged

`ListMixin._list_pagination: ClassVar[Literal["offset", "cursor"]] = "offset"`. Cursor mode uses a
separate FastAPI dependency (`q`, `sort`, `limit`, `cursor`) so an offset-mode route's OpenAPI and
behaviour are byte-identical to 3.x.

DESIGN: opt-in cursor mode over flipping the 4.0 default (BACKLOG allows the break)
  ✅ **The break would not land on the upgrader but on the app's own API consumers** (mobile apps,
     partners, browser front-ends sending `?offset=40`), whom an app developer cannot fix by
     upgrading code — unlike every other 4.0 break, which is wiring inside the app
  ✅ Cursor mode refuses nullable sort keys (§D-sortkeys); a default flip would turn a working
     `?sort=-published_at` into a 422 the first time such a request arrives
  ✅ Offset remains the right tool for "page 7 of 35" UIs — it is not deprecated
  ❌ New routers do not get keyset by default — README recommends `_list_pagination = "cursor"` for
     large or append-heavy collections; "flip the default" is parked with a trigger (§Parked)
  ❌ Two modes to document and test

In cursor mode: `?offset=0` is tolerated (identity; third-party clients and old MCP adapters send it),
`?offset>0` → 422 `OFFSET_NOT_SUPPORTED` naming the cursor alternative; in offset mode `?cursor=` is
not a declared parameter (ignored by FastAPI, as any unknown query param).

`PageCursor` gains `cursor: str | None = None`; `offset` becomes `int | None = None` (`None` only on
cursor-mode routes). Offset-mode responses are value-identical; the OpenAPI schema of `PageCursor`
changes (`offset` nullable, new `cursor`) — **Changed**, plus one `3-to-4.md` row for typed consumers
(`page.next.offset` is now `int | None` to mypy).

### §D-cursor-integrity — what the cursor carries, and why injection is impossible

- **The cursor never carries a filter or a sort.** `q` and `sort` always come from the request's own
  query string (the `Link` URL preserves them, `pagination.py:114`) and go through `QueryParser` +
  `_scoped_params` exactly as today. The cursor carries only (a) a **fingerprint** —
  `sha256(json([raw_q or "", [[field, order], …]]))[:32]` hex — and (b) the boundary row's **typed
  scalar** sort-key values. A cursor presented with a different `q`/`sort` → 422 `CURSOR_MISMATCH`.
- **The predicate's shape is computed server-side from the request's sort**; cursor bytes can only
  supply values. Values are decoded from an allowlisted tag set (`["i",int]`, `["f",float]`,
  `["s",str]`, `["b",bool]`, `["dt",iso8601]`, `["d",iso-date]`, `["u",uuid]`, `["dec",str]`) and
  type-checked against the tag; **any object/array value is rejected** — this is what closes the
  Mongo operator-injection path (`EQUAL` compiles to `{field: value}`, so a `{"$ne": null}` value
  would be an operator; `varco_beanie/query/compiler.py:15-16`).
- **Encoding:** payload `{"v":1,"f":"<fp>","k":[[tag,val],…]}` → compact JSON → base64url, no padding.
  **Signed when `StreamingSettings.cursor_secrets` is non-empty**:
  `<payload>.<base64url(HMAC-SHA256(secrets[0], b"varco-cursor-v1." + payload))>`; verification tries
  every secret with `hmac.compare_digest` (rotation: prepend the new secret, drop the old one after the
  longest-lived cursor's useful life). With secrets configured an unsigned token is rejected (no
  downgrade); without secrets a signed token is rejected (cannot verify).
- **Size cap:** `MAX_CURSOR_LENGTH = 2048` characters, checked before any decoding (also the
  `Query(max_length=…)` on the parameter).

DESIGN: unsigned-by-default + strict typing, HMAC opt-in — over always-signed (random per-process
key) or always-unsigned
  ✅ Safe by construction without a key: authorization and `_scoped_params` re-run on every request,
     so a hand-crafted cursor can only move the caller's start point **within the set they may
     already list**; the tag allowlist removes injection
  ✅ Works on N replicas behind a round-robin balancer with zero config — a per-process random key
     would 422 every page that lands on another pod
  ✅ Operators who want integrity (detect tampering/corruption, stop clients from synthesizing
     positions) set one env var; rotation is a list
  ❌ Unsigned cursors are readable, not confidential: they contain the boundary row's sort-key
     values and pk — sorting by a personal-data field puts it in URLs and access logs (Risks; docs
     Pitfall; GDPR-1 may later refuse cursor sorts on classified fields)
  ❌ No posture finding for "unsigned" — deliberately (Plan 054: a WARN blocks production start, and
     unsigned is not unsafe)

### §D-sortkeys — supported key types, null rule, tie-breaker, predicate form

- **Tie-breaker.** `with_tiebreaker(sort, tiebreaker_fields(D))` appends the pk unless already
  present: `("pk",)` for a single-PK model, `meta.composite_pk_fields` for a composite PK
  (`MetaReader.read(D)`, `varco_core/meta.py`). Direction = the direction of the last explicit sort
  field (ASC if none) so a composite index `(created_at DESC, id DESC)` serves the whole ORDER BY via
  one backward scan. No `sort` → `pk ASC`.
- **`pk` becomes queryable** (premise 3, Step 2): SA adds `pk = synonym("id")` to every generated
  single-PK ORM class; Beanie's compiler and sort builder alias `pk → _id`. Side effect: `?sort=pk`
  and `?q=pk = "…"` start working on both backends (CHANGELOG **Fixed**).
- **Allowed key types** (up-front check against the `ReadDTO`'s annotation, before any SQL): `int`,
  `float`, `str`, `bool`, `datetime`, `date`, `UUID`, `Decimal`, `Enum` of those, or `Any` (the
  `ReadDTO.pk` annotation, `dto/base.py:124`). The field must exist on the `ReadDTO` (boundary values
  are read from the last DTO by attribute name); a missing, `Optional`, or unsupported field → 422
  `CURSOR_SORT_KEY_UNSUPPORTED` with the field name. At encode time a `None`/NaN/inf value (DB disagrees
  with the DTO annotation) raises the same error (list: 422 before the body; export: `$error`).
- **Nullable keys are refused, not ordered.** Backends disagree on NULL placement (Postgres NULLS LAST
  for ASC, Mongo/SQLite nulls lowest) and `x > NULL` is unknown in SQL — a null boundary silently
  drops rows. Null-aware keyset is parked (trigger in §Parked).
- **Predicate** for sort `s1..sn` (incl. tie-breaker), values `v1..vn`, `op_i = > (ASC) | < (DESC)`:
  `P(i) = (s_i op_i v_i) OR (s_i = v_i AND P(i+1))`, `P(n) = s_n op_n v_n`; emitted as
  `(s1 op_1= v1) AND P(1)` when `n ≥ 2` — the redundant leading `>=`/`<=` bound gives Postgres an index
  range condition the bare OR-chain does not. Built only from existing `ComparisonNode`/`AndNode`/
  `OrNode` (`query/type.py:112-185`), so SA, Beanie and any out-of-tree compiler need no change.
  `ComparisonNode.value` is typed `Scalar = int|float|str|bool` (F6) but already carries datetimes at
  runtime after `ASTTypeCoercion`; keyset values are `cast()` at the one construction site — the
  `Scalar` alias is **not** widened (no mypy ripple).

DESIGN: expanded OR-chain over SQL row-value comparison `(a, b) > (x, y)`
  ✅ Backend-agnostic (Mongo has no row values) and correct for mixed ASC/DESC directions
  ✅ Zero compiler change; the leading bound recovers most index benefit
  ❌ Postgres plans a uniform-direction row-value comparison better — a backend-specific fast path is
     parked (needs an AST node or an SA-only hook)

### §D-count — total count: first page only

In cursor mode with `_list_include_total = True` (unchanged default), `count()` runs **only on the
request without `cursor`**, against the user filter only (never the keyset predicate); later pages
return `total_count: null` and no `X-Total-Count`. "Has next" is decided by fetching `limit + 1` rows
— no COUNT needed. Offset mode is unchanged (count on every page).

DESIGN: count on the first page only
  ✅ One `COUNT(*)` per traversal instead of per page — keeps "342 results" UIs, removes the per-page
     cost keyset exists to avoid
  ❌ `total_count` is null on page 2+ (documented; the client already has it from page 1)

### §D-export-route — `ExportMixin`: `GET {prefix}/export`

New `_CRUD_ACTION = "export"` mixin; `_CRUD_ROUTES["export"] = ("GET", "/export", 200, False)` (not
async-offloadable — the stream *is* the long operation). Query params: `q`, `sort`, `cursor`,
`limit` (rows for this response, capped by the ceiling). `Accept`: absent, `*/*`, `application/*`,
`application/jsonl` → `application/jsonl`; only `application/x-ndjson` → echoed; anything else → 406.
Response headers: `Content-Type`, `Cache-Control: no-store` (exports are bulk personal data),
`X-Accel-Buffering: no` (nginx; the same header FastAPI's SSE path sets, `routing.py:618`). ClassVars
mirror `ListMixin`'s OpenAPI/metadata set with an `_export_` prefix plus `_export_max_rows: int | None`
and `_export_max_seconds: float | None` (both may only **lower** the `StreamingSettings` ceiling) and
`_export_path: str = "/export"`. ⚠️ Do **not** add `_export_rate_limiter` unless Step 7's grep shows
`_list_rate_limiter` is actually consumed for CRUD routes (`rg _list_rate_limiter varco_fastapi/`
hits only `mixins.py:358` today); rate-limit exports through `RateLimitModule` path rules instead.

DESIGN: a separate opt-in route over `Accept`-negotiation on `GET /`
  ✅ **A bulk-exfiltration surface must be opt-in** — negotiation would silently give every
     `ListMixin` router an unbounded export the moment 4.0 is installed
  ✅ Own OpenAPI operation (`application/jsonl` + `itemSchema`), own limits (list `max_limit=100` vs
     export `max_rows=1e6`), own RouteGuard/`dependencies`, no `Vary: Accept` cache hazard
  ❌ `/export` shadows `GET /{id}` for an entity whose STR-assigned pk is literally `"export"`
     (literal routes register first, `introspection.py:286-300`) — documented; `_export_path` renames it

### §D-limits — ceilings, disconnect, coalescing

- **Row ceiling** `min(?limit, _export_max_rows, settings.export_max_rows)`; the query fetches
  `ceiling + 1`. Reaching the ceiling with a further row available → emit `encode_next(cursor of the
  last emitted row)` and end cleanly. **Time ceiling** `export_max_seconds` (monotonic, checked after
  each row) → same `$next`. The client resumes with `?cursor=<$next>` and identical `q`/`sort`. This
  also bounds how long one export holds a DB transaction/snapshot (Risks: VACUUM horizon).
- **Disconnect / cancellation**: `NdjsonResponse.stream_response` runs the line iterator inside
  `contextlib.aclosing()`; `ClientDisconnect`/`CancelledError` unwind it → `service.stream()`'s
  `async with uow` exits → SA `result.close()` / PyMongo `cursor.close()` in the repositories'
  `finally` (Step 1). Never swallow `CancelledError`.
- **Mid-stream exception** (`Exception`, not `BaseException`): log once at ERROR with the
  correlation id, write `encode_error(envelope)` (never `str(exc)` for unmapped exceptions — Plan 035
  S3 / Plan 054 rule), end cleanly.
- **Coalescing**: lines are buffered up to `settings.chunk_bytes` (64 KiB) per ASGI `send`; control
  records flush immediately. One send per row would cost one ASGI message per row.
- **Repository batch sizes** are class attributes, not settings: `stream_batch_size: ClassVar[int] =
  500` on `AsyncSQLAlchemyRepository` (SA `yield_per`) and the Beanie repository (PyMongo
  `batch_size`) — overridable per repository subclass without settings plumbing into UoW providers.

### §D-settings — `StreamingSettings` (`VARCO_STREAMING_`), bound in `varco_fastapi.di.bootstrap()`

| Field | Default | Meaning |
|---|---|---|
| `cursor_secrets` | `[]` (list of `SecretStr`, comma-separated env) | empty → unsigned cursors; first signs, all verify |
| `export_max_rows` | `1_000_000` (`ge=1`) | hard per-response row ceiling for `ExportMixin` |
| `export_max_seconds` | `300.0` (`gt=0`) | hard per-response duration ceiling |
| `chunk_bytes` | `65_536` (`ge=1024`) | NDJSON send coalescing size |

Registered via `bind_settings()` (CLAUDE.md "Settings" rule — never `@Singleton`), injected into
routers the same way as `job_runner`: `VarcoRouter.__init__(*, job_runner=…, streaming:
Instance[StreamingSettings] | None = None)` (and `VarcoCRUDRouter`). DI-less fallback:
`StreamingSettings()` constructed lazily at first use (same precedent as `WebhookDispatcher`
constructing `WebhookSettings`). No `bind_*`/`enable_*` verb → `entry-points-check` unchanged.

### §D-auth — tenancy and authorization are unchanged

Export calls `service.stream()`, list calls `service.list()`/`count()`; both apply `Action.LIST`,
`_pre_check`, and `_scoped_params` (tenant, soft-delete) exactly as today (F1, F2). No new `Action`:
anything export returns, a LIST-authorized caller can already page through. Repository
`_guard_tenant_filter` still runs (`varco_sa/repository.py:369`, `varco_beanie/repository.py:483`)
with the keyset predicate AND-ed **inside** the user node, so the tenant conjunct added by
`_scoped_params` stays top-level. A test pins tenant isolation for both list-cursor and export.

### §D-neighbours — 061, 062, 065, 068

- **061 (version/ETag):** each export line is `ReadDTO.model_dump_json(by_alias=True)` — if 061 adds
  `version` to `ReadDTO`, it streams verbatim with no change here. **No per-line ETag; no ETag on list
  or export responses.** Step 7 pins "every `ReadDTO` field appears on the line" so 061's field
  arrives untouched.
- **062 (ambient transactions):** `stream()` owns its UoW for the iterator's lifetime and the
  iterator outlives the route handler (the body is sent after the handler returns). Constraint handed
  to 062: *a request-scoped transaction boundary that ends at handler return must not be joined by
  `stream()`* — either `stream()` keeps opening its own (read-only) UoW, or 062's boundary extends to
  response completion. Postgres gives the single streamed SELECT a consistent snapshot under READ
  COMMITTED; Mongo cursors are not snapshot-isolated (docs Pitfall).
- **065 (GDPR export):** reuse `encode_record`/`encode_error` to write NDJSON to a file/object;
  `NDJSON_MEDIA_TYPE` for the artifact's content type.
- **068 (ingest):** reuse `is_ndjson_media_type` (request `Content-Type`), `aiter_ndjson(request.stream(),
  max_line_bytes=…)` (bounded memory, per-line errors, resync), `encode_error({..., "line": n})` for the
  per-line failure report, and `NdjsonResponse` to stream that report. 068 may add a `ControlKind`.

### Alternatives considered

- **FastAPI native `yield` JSONL endpoints** — rejected (§D-native): no pre-header error mapping, no
  error trailer, no deterministic cursor close, per-item re-validation, codec unusable outside HTTP.
- **`application/json-seq` (RFC 7464)** — rejected (§D-codec): the only registered option, but the
  `0x1E` prefix breaks line tools; ✅ standard ❌ ecosystem.
- **Flip `ListMixin` to cursor by default** — rejected (§D-mode): breaks apps' external API clients.
- **`Accept` negotiation on `GET /` instead of `/export`** — rejected (§D-export-route): silent
  exfiltration surface, conflicting limits, cache `Vary` hazard.
- **Per-process random HMAC key, always signed** — rejected (§D-cursor-integrity): breaks multi-replica.
- **Cursor embeds `q`/`sort`** (`?cursor=` alone suffices) — rejected: puts a filter on a
  client-controlled path; a fingerprint gives the same consistency check with no injection surface.
- **Push keyset into the service (`AsyncService.keyset_page()`)** — rejected: grows the service API
  every out-of-tree service inherits; ✅ sort values read from entities ❌ API growth for what is a
  `QueryParams` transformation; reading boundary values from the `ReadDTO` (422 if absent) suffices.
- **Rename `pk` → ORM name inside every repository query method** (an AST field-rename visitor) —
  rejected for SA: one `synonym` covers filter, sort, aggregation and core's SA applicator in one
  place; ✅ no ORM change ❌ N call sites to keep in sync. (Beanie has no synonym equivalent, so its
  alias lives in the compiler + one sort helper.)
- **Trailer `{"$end": {...}}` on every stream** — rejected: pollutes every successful body for a
  signal HTTP chunked framing already gives.

## Steps

Each step is test-first: the **(a)** tests are written and fail (or pin the bug) before **(b)**.

1. [ ] **Repository streaming actually works and closes** — `varco_sa/varco_sa/repository.py:336-394`,
   `varco_beanie/varco_beanie/repository.py:457-508`, `varco_core/varco_core/service/base.py:998-1067`
   (docstring only).
   (a) `varco_sa/tests/test_stream_by_query.py` (aiosqlite, runs in `make test`): full iteration
   yields every row in sort order; `limit`/`offset` honoured; **early `break` + `aclose()` closes the
   result** (spy on `AsyncScalarResult.close`); an exception inside the consumer still closes;
   `AsyncService.stream()` end-to-end over an SA UoW. The first test fails today with `TypeError`
   (premise 1). `varco_beanie/tests/test_stream_by_query_unit.py`: monkeypatch `_orm_cls.find` with a
   fake `FindMany` exposing `.cursor`; assert `batch_size=500` forwarded and `cursor.close()` awaited
   on early exit and on error.
   (b) SA: `result = await self._session.stream_scalars(stmt, execution_options={"yield_per":
   self.stream_batch_size})`; iterate in `try/finally: await result.close()`; delete the
   `type: ignore[attr-defined]`; add `stream_batch_size: ClassVar[int] = 500` with a DESIGN block
   (asyncpg's default is 50 rows/round trip, `asyncpg.py:634`). Beanie: `find(mongo_filter,
   batch_size=self.stream_batch_size)`; `try/finally` closes `find_query.cursor` when not `None`
   (⚠️ verify `AsyncCursor.close()` is a coroutine in PyMongo 4.16 before writing `await`). Rewrite both
   repositories' "Edge cases" (Beanie's "abandoning … does not leak" is false). Update `stream()`'s
   docstring: auth/scoping run on first `__anext__` (F1) — callers needing errors before side effects
   must prime.

2. [ ] **`pk` is a queryable logical field** — `varco_sa/varco_sa/factory.py:311-315`,
   `varco_beanie/varco_beanie/query/compiler.py` (`__init__` + comparison visit),
   `varco_beanie/varco_beanie/repository.py` (one `_sort_spec()` helper used by `find_by_query` and
   `stream_by_query`; one `_compiler()` helper used by every compiler construction site).
   (a) `varco_sa/tests/test_pk_logical_field.py`: `sort=[SortField("pk", DESC)]` and
   `ComparisonNode("pk", GREATER_THAN, x)` work through `find_by_query`, `count`, `stream_by_query`;
   `from_orm`/`to_orm` round-trip unchanged; `sa_inspect(orm_cls).columns` unchanged (a synonym is not
   a column) and the Alembic autogenerate comparison used by `varco_sa/tests` reports no diff; a
   composite-PK model gets no synonym. `varco_beanie/tests/test_compiler_pk_alias.py` (pure): `pk` →
   `_id` in filters and sort specs for single PK; composite PK untouched; `allowed_fields` checks the
   logical name.
   (b) SA: `all_cols = {"id": pk_col, "pk": synonym("id"), **non_pk_cols}` (single PK only), DESIGN
   block. Beanie: `BeanieQueryCompiler(..., field_aliases: Mapping[str, str] | None = None)`;
   the repository passes `{"pk": "_id"}` when `self._mapper._pk_orm_attrs == ["id"]`.

3. [ ] **The line codec** — new `varco_core/varco_core/ndjson.py` (§D-codec API), module docstring
   carrying the DESIGN blocks; if `varco_core/__init__.py` keeps a lazy submodule listing, add
   `ndjson` there (names stay in the submodule, **not** top-level `__all__`).
   (a) `varco_core/tests/test_ndjson.py`: BaseModel line equals `model_dump_json(by_alias=True)` +
   `\n`; Mapping encoding of datetime/UUID/Decimal/Enum; NaN refused; a string containing `\n`,
   U+2028, `\x1e` stays on one line and round-trips; `encode_record` refuses a `{"$error": …}`-shaped
   value; `encode_error` requires `code`/`message`; decoder: chunk boundaries inside a multi-byte UTF-8
   char and inside `\r\n`; final line without `\n`; blank lines counted; BOM stripped; invalid JSON →
   `NdjsonLine(error=…)` and decoding continues; over-long line → one error line, buffered bytes never
   exceed `max_line_bytes` (10 MiB single-line input), resync on the next line; unknown `$x` single-key
   object → `control_raw="$x"`, `control is None`; `is_ndjson_media_type` with parameters and casing.
   (b) Implement; `Thread safety`/`Async safety`/`Edge cases` docstrings throughout.

4. [ ] **Keyset core + cursor errors** — new `varco_core/varco_core/query/keyset.py`;
   `varco_core/varco_core/exception/query.py` (or `service.py`, next to `ServiceValidationError`):
   `InvalidCursorError`, `CursorMismatchError`, `CursorSortKeyError`, `OffsetNotSupportedError` — all
   `ServiceValidationError` subclasses (→ 422) with `message_key` `varco.cursor.invalid` /
   `.mismatch` / `.sort_key_unsupported` / `varco.pagination.offset_not_supported`; codes added to the
   class→code map at `varco_core/exception/http.py:~192` (check whether the lookup walks the MRO — if
   exact-type, the entries are mandatory); export the four exceptions from `varco_core.__all__` (lazy)
   and the keyset names from `varco_core.query.__all__`.
   API: `KeysetCursor` (frozen: `values: tuple[CursorValue, ...]`, `fingerprint: str`),
   `MAX_CURSOR_LENGTH`, `tiebreaker_fields(domain_cls) -> tuple[str, ...]`,
   `with_tiebreaker(sort, tiebreaker) -> tuple[SortField, ...]`,
   `query_fingerprint(raw_query, sort) -> str`, `check_sort_keys(sort, read_dto_cls) -> None`,
   `keyset_predicate(sort, values) -> TransformerNode`,
   `apply_keyset(params, cursor, *, sort, limit) -> QueryParams` (node AND predicate, `offset=None`),
   `boundary_cursor(row, *, sort, fingerprint) -> KeysetCursor`,
   `encode_cursor(cursor, *, secrets: Sequence[bytes] = ()) -> str`,
   `decode_cursor(token, *, secrets=(), sort, fingerprint) -> KeysetCursor`.
   (a) `varco_core/tests/test_keyset.py`: predicate shape for n=1,2,3 and mixed directions (incl. the
   leading bound); a small test-local AST evaluator (promoted to a shared test helper in Step 6) proves
   a full traversal of a 1 000-row list with heavy ties visits every row exactly once for every
   direction combination, including composite tie-breakers; tag round-trips (aware/naive datetime,
   date, UUID, Decimal, bool-before-int, Enum); **injection**: object/array/unknown-tag/wrong-type
   values → `InvalidCursorError`; length cap checked before decode; signed/unsigned matrix (unsigned
   with secrets → reject; signed without → reject; rotation: old secret verifies, new signs; tampered
   byte → reject; `compare_digest` used); fingerprint mismatch on changed `q` or `sort` →
   `CursorMismatchError`; `check_sort_keys` refuses missing/`Optional`/unsupported fields and accepts
   `Any` pk; `boundary_cursor` raises on `None`/NaN; tie-breaker direction follows the last sort field;
   `apply_keyset` keeps the user node's tenant conjunct reachable to `_guard_tenant_filter`.
   (b) Implement; `cast()` keyset values into `ComparisonNode` (do not widen `Scalar`, §D-sortkeys).

5. [ ] **HTTP streaming adapter + settings** — new `varco_fastapi/varco_fastapi/streaming.py`:
   `StreamingSettings` (§D-settings), `negotiate_ndjson(accept: str | None) -> str` (406 via
   `HTTPException`), `NdjsonResponse(StreamingResponse)` (`media_type = NDJSON_MEDIA_TYPE` class
   attribute; takes an `AsyncIterator[bytes]` of encoded lines; overrides `stream_response` with
   `aclosing` + coalescing + `$error` trailer), `prime(aiter) -> tuple[first, aiter]` helper.
   `varco_fastapi/varco_fastapi/middleware/error.py`: extract the status/code/message/correlation-id
   envelope computation into a pure `error_envelope(exc, request) -> tuple[int, dict]` that
   `ErrorMiddleware` and `NdjsonResponse` both call (no behaviour change for `ErrorMiddleware`; its
   tests stay green). `varco_fastapi/varco_fastapi/di.py:497-505`: add `StreamingSettings` to the
   `bind_settings` tuple. `varco_fastapi/varco_fastapi/router/base.py:566-607` + `router/crud.py`:
   `streaming: Instance[StreamingSettings] | None = None` kwarg, resolved like `job_runner`.
   Top-level `varco_fastapi.__all__`: `NdjsonResponse`, `StreamingSettings`.
   (a) `varco_fastapi/tests/test_streaming_response.py`: lines coalesced to ≤ `chunk_bytes`; control
   lines flush; exception after 3 rows → 3 data lines + one `$error` whose payload equals what
   `ErrorMiddleware` renders for the same exception (status included) and the body ends cleanly; an
   unmapped exception never leaks `str(exc)`; **client disconnect** (ASGI `send` raising `OSError`, and
   the <2.4 `http.disconnect` path) → the source iterator's `finally` ran; `CancelledError` propagates;
   `negotiate_ndjson` table (absent, `*/*`, `application/*`, q-values, x-ndjson echo, `application/json`
   → 406); `StreamingSettings` env parsing incl. comma-separated secrets; DI resolves it from
   `bootstrap()`; a DI-less router falls back lazily.
   (b) Implement.

6. [ ] **Cursor-mode list + RFC 8288 `Link`** — `varco_fastapi/varco_fastapi/router/mixins.py:307-372`
   (`_list_pagination` ClassVar + docstring), `router/base.py:205-331` (`HttpQueryParams.cursor`; new
   `_http_cursor_params` dependency with `cursor: str | None = Query(None, max_length=MAX_CURSOR_LENGTH)`),
   `router/base.py:1093-1181` (`_make_list_handler` cursor branch: tie-breaker, `check_sort_keys(sort,
   R)`, `offset>0` → `OffsetNotSupportedError`, decode, `apply_keyset(limit+1)`, `service.list`,
   `has_more`, first-page-only `count()` on the unkeyed node, `PageCursor(cursor=…, offset=None)`),
   `varco_core/varco_core/dto/pagination.py:119-153` (`cursor` field, `offset: int | None = None`,
   docstrings + module DESIGN block), `varco_fastapi/varco_fastapi/router/pagination.py:55-134`
   (`_build_next_url`: cursor mode sets `cursor`, drops `offset`; **fix the `"asc"` comparison**;
   docstrings cite RFC 8288, not RFC 5988).
   (a) `varco_fastapi/tests/test_list_cursor_pagination.py` (a fake in-memory service evaluating the AST
   with Step 4's shared evaluator): following `Link rel="next"` from page 1 visits every row once with
   ties and mixed directions; body `next.cursor` equals the `Link` URL's `cursor`; last page has no
   `Link`/`next`; total on page 1 only; `?offset=0` tolerated, `?offset=5` → 422
   `OFFSET_NOT_SUPPORTED`; mismatched `q`/`sort` → 422 `CURSOR_MISMATCH`; nullable sort → 422 before
   any service call; signed cursors when settings carry secrets; an offset-mode route's OpenAPI
   operation is byte-identical to before (snapshot the operation dict); a tenant-scoped service never
   returns another tenant's rows across pages. In the existing pagination-header test module:
   **regression — an ASC sort survives into the next `Link`** (fails today, premise 4).
   (b) Implement.

7. [ ] **`ExportMixin` + `GET /export`** — `varco_fastapi/varco_fastapi/router/mixins.py` (new
   `ExportMixin`, `__all__`), `router/introspection.py:306-313` (`"export"` row; extend the
   `action in ("read", "list")` model branch at `:386`), `router/base.py` (`_make_export_handler`,
   registry entry at `:1185-1192`, `_register_route` at `:744-774`: `response_model=None`,
   `response_class=NdjsonResponse`, `responses={200: {"content": {"application/jsonl": {"itemSchema":
   …R…}}}}` mirroring `fastapi/openapi/utils.py:357-372`), `router/mcp.py`, `router/skill.py`,
   `router/a2a/router_source.py` (skip `export`), top-level `varco_fastapi.__all__` (`ExportMixin`).
   (a) `varco_fastapi/tests/test_export_mixin.py`: lines are exact `ReadDTO` JSON and **every
   `ReadDTO` field appears** (061 pin); LIST denial → 403 JSON (not 200 + `$error`) — priming proven;
   bad `q` / unknown field / nullable sort / bad cursor → 4xx before headers; `limit` ceiling → `$next`
   and resuming with it yields the remainder exactly once; `_export_max_rows` cannot raise the settings
   ceiling; time ceiling via a monkeypatched monotonic clock → `$next`; mid-stream failure → `$error`;
   headers `Cache-Control: no-store`, `X-Accel-Buffering: no`, `Content-Type` per `Accept`; 406;
   `/export` registered before `/{id}`; a router without `ExportMixin` has no `/export`; OpenAPI
   `itemSchema` resolves to R; tenant scoping; MCP/skill/A2A tool lists contain no export action.
   (b) Implement.

8. [ ] **Client + agent adapters learn cursors** — `varco_fastapi/varco_fastapi/client/base.py:572-603`
   (`list(..., cursor: str | None = None)`, send only non-`None` params), new
   `varco_fastapi/varco_fastapi/client/export.py` (`ExportStream[R]`: async-iterable of `R`, raises
   `NdjsonStreamError` on `$error`, `follow_next: bool = True` re-requests with the `$next` cursor,
   exposes `.next_cursor` when `follow_next=False`; streams via the same httpx client/auth headers the
   SSE path uses, `client/handle.py:215-300`), the `"export"` branch in `client/base.py`'s CRUD method
   builder; `router/mcp.py:250-268,704-711`, `router/skill.py:603-609`, `router/a2a/router_source.py:167-173`:
   default `offset` to `None` (not `0`), pass `cursor`, advertise `cursor` (not `offset`) in the MCP
   input schema when the router's `_list_pagination == "cursor"`.
   (a) `varco_fastapi/tests/test_client_export.py` against an in-process ASGI app: full export across
   two `$next` resumptions with `follow_next=True`; `follow_next=False` stops with `.next_cursor` set;
   `$error` → `NdjsonStreamError` carrying the envelope; a truncated body → httpx protocol error
   surfaced. Extend the existing MCP/skill/A2A list tests: a cursor-mode router receives no `offset`,
   paging via `cursor` works.
   (b) Implement.

9. [ ] **SQLAlchemy integration (Postgres)** — new `varco_sa/tests/test_streaming_integration.py`
   (`pytestmark = pytest.mark.integration`, session `postgres_url`, `uuid4().hex[:8]`-namespaced
   table/tenant per CLAUDE.md §Test Conventions):
   (i) **the server-side cursor is real**: during iteration
   `result._real_result.context.cursor.server_side is True` (`asyncpg.py:620-621`), and `pg_cursors`
   queried on the same connection lists the portal (⚠️ if asyncpg forbids the interleaved query, keep
   the first assertion and drop this one, with a comment saying why); (ii) **memory bounded**:
   `tracemalloc` peak while streaming 5 000 vs 50 000 rows through `AsyncService.stream()` — assert
   `peak(50k) ≤ 2 × peak(5k)` (a buffered implementation is ~10×); (iii) early exit after 10 rows
   returns the connection to the pool (`engine.pool.checkedout() == 0` after `aclose()`); (iv) keyset
   full traversal over HTTP (ASGI transport, cursor-mode router) with 20 000 rows, 50 distinct
   `created_at` values, `-created_at` and `+status,-created_at` — every row exactly once; a concurrent
   insert between pages causes no duplicate; (v) `GET /export` of 20 000 rows with
   `export_max_rows=7 000` resumes twice to the exact set; (vi) `?sort=pk` works.

10. [ ] **Beanie integration (MongoDB)** — new `varco_beanie/tests/test_streaming_integration.py`
   (session `mongo_url`, namespaced database): (ii)/(iv)/(v)/(vi) of Step 9 on Mongo; early exit closes
   the server cursor (`find_query.cursor.alive is False` after `aclose()`, and `serverStatus`
   `metrics.cursor.open.total` does not grow across 20 aborted streams); UUID `_id` tie-breaker order is
   stable; a crafted unsigned cursor carrying `{"$ne": null}` is a 422, never a query.

11. [ ] **Docs** — new `technical_docs/features/streaming.md` (codec spec incl. the completion rule and
   control records; keyset design: fingerprint, tags, signing/rotation, tie-breaker, predicate, null
   rule; the export route; limits/`$next` resumption; §D-neighbours; **Pitfalls table**: nullable sort
   key, cursor reused with another `q`, PII in unsigned cursors/URLs, an app-installed GZip middleware
   delaying lines, proxy buffering (`X-Accel-Buffering`), `MetricsMiddleware`/tracing durations
   measuring time-to-headers on streams (⚠️ verify, document only), long exports holding a Postgres
   snapshot (VACUUM horizon), Mongo cursors not snapshot-isolated, float/`real` sort keys, `/export` vs
   a pk literally `"export"`, reading NDJSON with `splitlines()`); `mkdocs.yml` nav entry beside the
   other features (`:103-147`); **README** — under "Query System" add "Pagination: offset vs cursor" and
   "Streaming export (NDJSON)" subsections with runnable snippets (router ClassVars, `curl -N … | jq
   -c`, client `export()`), update the TOC; **ARCHITECTURE.md** — package-map lines for
   `varco_core.ndjson`, `varco_core.query.keyset`, `varco_fastapi.streaming`, `client/export.py`; the
   Query-System pipeline gains the keyset node; **CLAUDE.md** — one decision-tree line under "Query
   filtering": `↳ Large collection / NDJSON export / keyset pagination? → varco_core.ndjson (codec) +
   varco_core.query.keyset + varco_fastapi.streaming (ExportMixin, NdjsonResponse) — never FastAPI's
   native yield endpoint for a service-backed export, never offset for export`; **CHANGELOG.md**
   `[Unreleased]` — *Added* (codec, keyset, cursor mode, `ExportMixin`, `NdjsonResponse`,
   `StreamingSettings`, client `export()`/`list(cursor=)`), *Changed* (`PageCursor.offset` optional +
   `cursor`; repository `stream_batch_size`), *Fixed* (SA `stream_by_query` `TypeError`; Beanie cursor
   leak on early exit; `pk` queryable; ASC sort reversed in the offset `Link`);
   **`technical_docs/migrations/3-to-4.md`** §5 — one table row: `PagedReadDTO.next.offset` is
   `int | None` (typed consumers), plus one line pointing at `streaming.md` for opting into cursor
   pagination (no action required).

12. [ ] **Snapshot gates + full verification** — regenerate and commit:
   `uv run python scripts/api_surface.py` (new `varco_core`/`varco_fastapi` `__all__` names),
   `make settings-reference` (`StreamingSettings`), `make di-catalogue` (the new binding); run
   `make entry-points-check` and `make asyncapi-check` and **assert no diff** (no new wiring verb, no
   consumer change); `uv run python scripts/import_budget.py --check --warn-only` (stdlib-only new
   modules — expect no breach); then the §Verification block. Commit regenerated artifacts in the same
   change as the code (CLAUDE.md: a hard requirement for `api-check`).

## Edge cases

- `stream()` caller breaks after 1 row → repositories' `finally` closes the cursor; UoW exits; no
  connection checked out (Steps 1, 9(iii)).
- Client disconnects mid-export (either ASGI spec path) → `aclosing` runs; DB cursor closed; no
  `$error` written (nothing to write to); `CancelledError` not swallowed.
- Exception before the first row (auth, tenant missing, bad field, bad cursor) → normal JSON error
  with its status (primed).
- Exception after N rows → N data lines + `$error` (status field = the would-be status), clean end.
- Empty result: list → `results: []`, no `next`, no `Link`; export → empty 200 body, no control line.
- Row count exactly equals the ceiling → no `$next` (the `+1` probe found nothing).
- Time ceiling hit exactly at the last row → `$next` emitted; resuming returns an empty body.
- `?limit=0` on export → 422 (`ge=1`); on a cursor list → the existing clamp semantics, `next` null.
- `?cursor=` on an offset-mode router → undeclared param, ignored (offset semantics).
- `?offset=0` on a cursor router → tolerated; `?offset=3` → 422 `OFFSET_NOT_SUPPORTED`.
- A cursor from a different router/collection with the same `q`/`sort` → same fingerprint, values
  applied to this collection — harmless (authorization/scoping re-run); signed cursors do not bind the
  route (documented).
- `sort` duplicates a field (`+a,-a`) → first occurrence wins in `with_tiebreaker`; pk given explicitly
  → not appended again.
- Composite-PK model → the tie-breaker is the composite fields; each must exist on the `ReadDTO`
  (else 422).
- Naive datetimes (SQLite) round-trip as naive; aware as aware with offset — never silently converted.
- Mongo datetimes are millisecond-truncated at storage, but boundary values are read back from storage,
  so equality holds (CLAUDE.md's BSON-truncation pitfall does not apply — values are never client-made).
- `ReadDTO` with an aliased field → export lines use the alias (`by_alias=True`), sort uses the domain
  name; a sort key whose DTO attribute differs from the domain field → 422.
- A data value that serializes to a single-key `$`-object → `encode_record` raises → `$error` (export)
  — impossible for a plain `ReadDTO` without deliberate aliasing.
- Over-long line during decode (068/client) → one `NdjsonLine(error=…)`, memory capped, resync.
- `Accept: application/json` on `/export` → 406; `Accept` absent → `application/jsonl`.
- Entity with STR pk `"export"` → `GET /export` hits export; documented, `_export_path` renames.
- An unsigned cursor arrives after secrets are configured (rotation start) → 422; clients restart paging.

## Verification

```bash
uv sync --all-packages --all-extras
uv run pytest varco_core/tests/test_ndjson.py varco_core/tests/test_keyset.py -q
uv run pytest varco_sa/tests/test_stream_by_query.py varco_sa/tests/test_pk_logical_field.py -q
uv run pytest varco_beanie/tests/test_stream_by_query_unit.py varco_beanie/tests/test_compiler_pk_alias.py -q
uv run pytest varco_fastapi/tests/test_streaming_response.py varco_fastapi/tests/test_list_cursor_pagination.py \
              varco_fastapi/tests/test_export_mixin.py varco_fastapi/tests/test_client_export.py -q
make test                                     # all eleven suites
make integration-test-clean PKG=varco_sa      # Step 9 (Docker)
make integration-test-clean PKG=varco_beanie  # Step 10 (Docker)
make lint                                     # ruff check + format --check + api/asyncapi/settings/di/entry-points gates
make type-check                               # mypy strict, ten dirs
rg -n 'type: ignore\[attr-defined\]' varco_sa/varco_sa/repository.py   # the stream_scalars ignore is gone
rg -n '"asc"' varco_fastapi/varco_fastapi/router/pagination.py          # no lowercase SortOrder compare left
```

## Risks

- **Mongo operator injection via cursor values** — invariant: `decode_cursor` returns only allowlisted
  scalar types; Step 4 + Step 10 tests pin it. Never add an object/array tag.
- **Long-lived streams hold DB resources**: one connection + an open transaction/snapshot per export;
  a 300 s export holds Postgres's xmin horizon (VACUUM) for 300 s. Invariant: `export_max_seconds` is a
  hard ceiling routers can only lower; size the pool for concurrent exports (docs).
- **SA ORM `yield_per`** — ⚠️ ASSUMPTION: generated ORM classes have no joined-eager *collection*
  relationships (incompatible with `yield_per`), and the weak-referencing identity map lets streamed
  instances be collected once converted (Step 9(ii) proves bounded memory, not the mechanism).
- **`pk = synonym("id")`** — ⚠️ ASSUMPTION: `_SAAutoMapper` and Alembic autogenerate ignore synonyms;
  Step 2 tests both. Invariant: synonym only for single-PK models.
- **PyMongo 4.16 `AsyncCursor.close()` is a coroutine** — ⚠️ ASSUMPTION, verify in Step 1 before
  writing `await`.
- **`pg_cursors` interleaved query** — ⚠️ ASSUMPTION that asyncpg allows a query on the connection
  while a portal is open inside the transaction; Step 9 has a fallback assertion.
- **Media-type registration status** (brief 021 §D: `jsonl`/`x-ndjson` unregistered, `json-seq`
  registered) — ⚠️ ASSUMPTION, not re-verified against the IANA registry; if one is registered later,
  `NDJSON_MEDIA_TYPES` gains it (additive).
- **FastAPI "JSON Lines since 0.134.0"** — ⚠️ ASSUMPTION (version only; 0.135.2 behaviour verified).
  Irrelevant to the design (native path unused) but cited by both briefs.
- **Metrics/tracing record time-to-headers, not stream duration** for `BaseHTTPMiddleware`-based
  middleware — ⚠️ ASSUMPTION, documented only (fixing it is RO-1-adjacent, out of scope).
- **Uvicorn's reported ASGI spec version** decides which Starlette disconnect path runs (F11) —
  irrelevant by construction (`aclosing` covers both); Step 5 tests both.
- **PII in unsigned cursors** (boundary values in URLs/logs) — documented Pitfall; GDPR-1 may refuse
  cursor sorts on `personal_data` fields later.
- **Mongo export consistency** — a Mongo cursor is not snapshot-isolated; concurrent updates to a sort
  key can move a document across the boundary (duplicate/miss). Postgres' single statement is
  consistent. Documented, not fixed.
- **`ErrorMiddleware` refactor** (Step 5 extraction) — invariant: its response bodies and statuses are
  byte-identical; its existing tests are the guard.

## Out of scope / Parked (for the coordinator to record; this plan does not edit BACKLOG.md)

- **Flip `ListMixin` default to cursor** · ⏵ trigger: two consumer apps report offset drift/duplicate
  bugs, *and* a major whose migration guide can warn apps' external clients in advance.
- **Null-aware keyset** (explicit `NULLS FIRST/LAST` on SA, IS NULL disjuncts) · ⏵ a consumer needs
  cursor pagination over a nullable sort key that cannot be made non-null.
- **Row-value fast path for uniform-direction keyset on Postgres** · ⏵ a measured plan regression
  attributable to the OR-chain on a real dataset.
- **Export audit event / `Action.EXPORT`** · ⏵ a compliance requirement distinguishing bulk export
  from paging (likely surfaced by GDPR-2).
- **Export in `ServiceContract`/`varco gen-client`, MCP/A2A** · ⏵ a consumer of a generated client
  needs `export()` across repos.
- **`rel="prev"`/backward traversal** · ⏵ a UI needing "previous page" over cursor mode.

## Done — this plan

- `AsyncService.stream()` iterates on SA and Beanie; cursors close on completion, error, break, and
  disconnect; server-side cursor + bounded memory proven on Postgres and Mongo (Steps 1, 9, 10).
- `varco_core.ndjson` exists with the §D-codec API, documented as the shared codec for 065/068.
- `varco_core.query.keyset` + cursor exceptions; `pk` queryable on both backends.
- `ListMixin(_list_pagination="cursor")` paginates with opaque cursors, `Link rel="next"`, first-page
  total; offset mode byte-identical (OpenAPI snapshot) except the fixed ASC `Link` bug.
- `ExportMixin` streams NDJSON with primed errors, `$next` resumption, `$error` trailer, headers.
- Client `list(cursor=)`, `export()`; MCP/skill/A2A send no default `offset=0`.
- `streaming.md`, README, ARCHITECTURE, CLAUDE.md line, CHANGELOG, `3-to-4.md` row landed;
  `make lint`, `make type-check`, `make test`, both integration runs green; regenerated snapshots
  committed.
