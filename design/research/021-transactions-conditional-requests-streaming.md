# Research 021 — SQL transactions, MongoDB transactions, HTTP conditional requests, streaming

Date: 2026-10-07 · Freshness matters: **yes** — isolation level defaults, Motor deprecation, idempotency-key RFC progression, NDJSON media type evolution

> ⚠️ **Corrected (2026-10-10, plans/068 source check):** the "Critical ASGI constraint" below (§D, and
> the Starlette 1.3.1 compatibility line) is **wrong for uvicorn 0.42.0**. uvicorn keeps serving the
> request body until the response *completes*, not when `http.response.start` is sent
> (`uvicorn/protocols/http/h11_impl.py:536-542`, `httptools_impl.py:568-574`). The real hazards for
> streaming a response while still reading the request are different: Starlette's pre-ASGI-2.4
> disconnect listener consumes `receive()` (uvicorn reports spec 2.3), and httpcore clients send the
> whole request before reading the response. See plans/068-streaming-ndjson-bulk-ingest.md for the
> verified analysis. Do not cite the original claim.

## Question

A. Transactions (SQLAlchemy 2.0.48 async + Postgres + asyncpg):
 1. Per-transaction isolation level: where to set it (`execution_options()` on connection vs `sessionmaker` vs `session.connection(execution_options=...)`); constraints (must be set before BEGIN).
 2. SAVEPOINT via `begin_nested()` in async; rollback behaviour on nested transaction.
 3. Exception mapping: Postgres SQLSTATE 40001 (serialization_failure) and 40P01 (deadlock_detected) through asyncpg → SQLAlchemy; exact attribute to read on asyncpg exception for SQLSTATE (`.sqlstate` or `.pgcode`?); guidance on retrying serialization failures and 40001 under REPEATABLE READ.
 4. `version_id_col` optimistic locking: when `StaleDataError` is raised (update succeeds? zero rows?); interaction with bulk/Core UPDATE; async ORM behaviour.
 5. Server-side cursors: `AsyncSession.stream()` + `yield_per` with asyncpg; open transaction requirement; memory and consumer-delay behaviour.

B. MongoDB transactions (PyMongo 4.16 via Beanie 2.0.1):
 1. Motor deprecation status; which Beanie 2.0.1 uses (Motor or PyMongo `AsyncMongoClient`)?
 2. `with_transaction()` callback API; built-in retry of TransientTransactionError / UnknownTransactionCommitResult labels; 120 s limit.
 3. Beanie revision_id / `use_revision` API; what exception is raised on stale revision (exact exception type).

C. HTTP conditional requests (RFC 9110):
 1. ETag format (strong vs weak `W/`); If-Match semantics (strong comparison, `*`); 412 Precondition Failed response.
 2. RFC 6585 §3 428 Precondition Required; ETag on PUT/PATCH responses (required?); If-None-Match / 304 on GET; CORS `Access-Control-Expose-Headers` for ETag.

D. HTTP streaming (FastAPI 0.135.2 / Starlette 1.3.1):
 1. NDJSON vs JSON Lines media types: `application/x-ndjson`, `application/jsonl`, `application/json-seq` (RFC 7464) — which is IANA-registered as of 2026?
 2. FastAPI 0.135 native support: exact release note for streaming JSON Lines (when was it added?).
 3. Starlette `Request.stream()` for chunked uploads; ASGI middleware wrapping `receive`; when does the request body tap close?
 4. Error signalling mid-stream: trailer records / last-line error object conventions; opaque cursor encoding (base64url of sort key + signature?); RFC 8288 Link header format for rel="next".
 5. ietf-httpapi-idempotency-key-header draft status in October 2026 (one line).

## Findings

### A. SQL Transactions (SQLAlchemy 2.0 async + Postgres/asyncpg)

**1. Per-transaction isolation level setting**

Isolation level is set via `execution_options(isolation_level=...)` on an `AsyncConnection` or `AsyncSession` — [SQLAlchemy 2.0 asyncio documentation](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html). The setting must be applied **before BEGIN** is emitted; SQLAlchemy sends `isolation_level` in a `SET TRANSACTION` statement that precedes the actual `BEGIN` for the transaction. A connection's isolation level remains in effect until the connection is returned to the pool — setting it is not transactionally scoped. Apply at the connection object returned from `AsyncSession.connection()` or `AsyncEngine.begin()` to affect that single transaction only.

```python
async with session.connection() as conn:
    await conn.execution_options(isolation_level="SERIALIZABLE").execute(...)
```

Setting on `sessionmaker()` or `AsyncSession()` constructor does not apply to future transactions; it is a snapshot default consulted only if no per-connection override exists. Per-transaction override always wins.

**2. SAVEPOINT via `begin_nested()` in async**

`AsyncSession.begin_nested()` establishes a SAVEPOINT and returns a `SessionTransaction` context manager. On `.commit()`, the session emits `RELEASE SAVEPOINT`; on `.rollback()`, it emits `ROLLBACK TO SAVEPOINT`. The session **unconditionally flushes pending state** to the database when `begin_nested()` is called, regardless of `Session.autoflush` — [SQLAlchemy 2.0 transactions documentation](https://docs.sqlalchemy.org/en/20/orm/session_transaction.html). Rolling back a nested transaction expires only in-memory object state modified since the savepoint was created; unmodified state is retained. Async behaviour is identical to sync — the context manager is awaitable.

**3. Exception mapping: SQLSTATE to asyncpg to SQLAlchemy**

asyncpg raises database-specific exceptions mapped from Postgres error codes. SQLAlchemy wraps the asyncpg exception in `DBAPIError` (or a subclass like `IntegrityError`), exposing the original via `.orig` attribute — e.g. `exc.orig.sqlstate` returns the SQLSTATE string (`"40001"`) — [asyncpg exception handling](https://tessl.io/registry/tessl/pypi-asyncpg/0.30.0/files/docs/exception-handling.md).

SQLSTATE 40001 (serialization_failure) arises under REPEATABLE READ or SERIALIZABLE isolation levels when Postgres detects a conflict violating snapshot consistency; 40P01 (deadlock_detected) indicates a circular wait. **Retry strategy**: catch the exception, rollback the entire transaction (not just nested savepoints), and retry from the start. Retrying only part of a transaction will not resolve a serialization conflict — [PostgreSQL 18 MVCC serialization-failure-handling documentation](https://www.postgresql.org/docs/18/mvcc-serialization-failure-handling.html). READ COMMITTED is the Postgres default and does not raise 40001 for ordinary write conflicts; it re-evaluates against the latest committed row instead.

SQLSTATE 23505 (unique_violation) can also arise under SERIALIZABLE when concurrent inserts collide on a unique key; the server cannot detect this as a serialization problem because the conflict is checked at constraint-violation time, not at snapshot-consistency time. PostgreSQL documentation advises retrying 23505 errors as well when using SERIALIZABLE — [Postgres buglist and discussion](https://www.postgresql.org/message-id/attachment/32407/Untitled.pages.pdf).

**4. `version_id_col` optimistic locking**

Optimistic locking: a version column (typically an integer, configured via `version_id_col=` in mapper) is incremented on every ORM flush. When the ORM emits an UPDATE, it includes a WHERE clause on both PK and current version: `UPDATE user SET version_id=version_id+1, … WHERE id=? AND version_id=?`. If zero rows are updated (because another transaction already incremented the version), SQLAlchemy raises `StaleDataError` — [SQLAlchemy 2.0 versioning documentation](https://docs.sqlalchemy.org/en/20/orm/versioning.html). Async ORM raises the exception identically to sync. **Bulk UPDATE via `Query.update()` or `update()` Core statement bypasses versioning entirely** — the ORM never sees those changes and cannot increment the version or detect staleness. Versioning applies only to `Session.flush()`, not bulk operations.

**5. Server-side cursors with `AsyncSession.stream()`**

`AsyncSession.stream()` and `.stream_scalars()` return async iterables that fetch rows from the server on demand using `yield_per` (rows buffered at once) — [SQLAlchemy 2.0 asyncio documentation](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html). **An open transaction is required**; the cursor cannot outlive the session's transaction. If the consumer pauses, the transaction remains open and holds resources (connection, locks, memory on the server). No lazy loading occurs — rows already sent to the client are not re-fetched. Memory behaviour: `yield_per=1000` fetches 1000 rows at once; very large `yield_per` values can saturate client memory if the consumer is slow.

### B. MongoDB Transactions (PyMongo 4.16 + Beanie 2.0.1)

**Motor deprecation & Beanie 2.0 integration**

Motor, the async wrapper for PyMongo, was deprecated on May 14, 2025, in favour of PyMongo 4.9+'s native `AsyncMongoClient`. Motor reaches end-of-life on May 14, 2026 (critical fixes until May 2027) — [MongoDB Python driver migration guide](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/reference/migration) and [Sammai blog on PyMongo's native async](https://sammai.bearblog.dev/pymongos-native-async-driver-moving-beyond-motor/). Beanie 2.0.1 now uses PyMongo's native `AsyncMongoClient` directly, not Motor.

**`with_transaction()` callback API & built-in retry**

PyMongo's `session.with_transaction(callback, read_preference=None, write_concern=None, read_concern=None)` executes the callback within a transaction. The callback receives the session as a parameter: `await session.with_transaction(lambda sess: collection.insert_one({...}, session=sess))`. PyMongo **automatically retries transient errors**:
- `TransientTransactionError` (e.g. network hiccup mid-transaction)
- `UnknownTransactionCommitResult` (commit sent but response lost)

Each retryable error is retried up to 120 seconds of cumulative elapsed time — [MongoDB PyMongo transactions documentation](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/transactions). Beyond 120 s, the exception propagates. Read and write concern can be overridden per-transaction via keyword arguments.

**Beanie revision_id / `use_revision` optimistic locking**

Enable with `class Settings: use_revision = True` on a Beanie Document model. Each document stores a `revision_id` field; any updating operation (via ORM, not bulk) increments it. On flush, if the local `revision_id` no longer matches the database version, Beanie raises `RevisionIdWasChanged` exception — [Beanie revision tutorial](https://beanie-odm.dev/tutorial/revision/). ⚠️ Bulk operations may not respect the revision check — the feature is designed for ORM-level updates.

### C. HTTP Conditional Requests (RFC 9110)

**ETag format and If-Match semantics**

An ETag is an opaque string in quotes (RFC 9110 §8.8.3). A **strong ETag** is a simple quoted string: `ETag: "abc123"`; a **weak ETag** is prefixed with `W/`: `ETag: W/"abc123"`. Strong ETags match byte-for-byte content; weak ETags match semantically equivalent representations (e.g., after GZIP).

If-Match: `If-Match: "abc123"` or `If-Match: "abc", "def"` or `If-Match: *` requires the resource's current strong ETag to match one of the listed values (strong comparison only — weak ETags never match). If no match, the server returns **412 Precondition Failed**. The `*` wildcard matches if the resource exists.

**RFC 6585 §3 (428 Precondition Required) and ETag on responses**

428 Precondition Required signals that the server requires a conditional header (If-Match, If-Unmodified-Since) to proceed. It is optional — servers may not implement it. ETag on PUT/PATCH responses is **not mandated by RFC 9110** but is a best practice for clients to obtain the new version without a second request.

If-None-Match + GET returns 304 Not Modified if the client's ETag matches the current resource ETag (weak or strong comparison), avoiding body transmission. CORS: the `ETag` response header must be listed in `Access-Control-Expose-Headers` for browsers to access it.

### D. HTTP Streaming (FastAPI 0.135.2 / Starlette 1.3.1)

**NDJSON vs JSON Lines media types**

Three related media types exist for line-delimited JSON:
- **`application/x-ndjson`** — widely used, `x-` prefix indicates private use (not IANA-registered). 
- **`application/jsonl`** — sometimes used, non-standard.
- **`application/json-seq`** (RFC 7464) — IANA-registered, uses `0x1E` (ASCII Record Separator) prefix before each JSON value — [Speakeasy docs on JSON Lines](https://www.speakeasy.com/docs/sdks/customize/runtime/jsonl-events.md).

As of October 2026, `application/x-ndjson` remains unregistered with IANA, despite ongoing discussion. `application/json-seq` is the only RFC-standardized variant.

**FastAPI 0.135.2 streaming JSON Lines support**

Streaming JSON Lines was added in FastAPI 0.134.0 (so 0.135 has it) — [FastAPI streaming tutorial](https://fastapi.tiangolo.com/tutorial/stream-json-lines/). Use `yield` in an async endpoint instead of `return`:

```python
@app.get("/stream")
async def stream_items():
    for item in items:
        yield item
```

FastAPI automatically wraps yields in the OpenAPI response model and sends each item as a newline-delimited JSON line. No explicit `StreamingResponse` required.

**Starlette `Request.stream()` for chunked uploads**

`Request.stream()` provides async iteration over incoming request body chunks without buffering the entire body. Calling `.body()`, `.form()`, or `.json()` after `.stream()` raises an error — the body is consumed once.

**Critical ASGI constraint**: When the application sends `http.response.start`, the ASGI server transitions from request-reading to response-sending phase. Any subsequent calls to `receive()` return `http.disconnect` or block indefinitely — the request body tap closes. If middleware wraps `receive()` and consumes the request stream, it must buffer chunks before passing control to the application, or the application cannot read the stream — [Starlette requests documentation](https://www.starlette.dev/requests/).

**Error signalling mid-stream**

No standard convention exists for signalling errors mid-NDJSON stream. Common patterns:
1. **Trailer record**: emit a final JSON object with an error key, e.g. `{"error": "connection lost"}`.
2. **Last line as error object**: if the stream ends unexpectedly, parse the last incomplete line as the error signal (fragile).
3. **Union type model**: emit items of type `union[SuccessItem, ErrorItem]` where ErrorItem carries error details — clients parse each line and handle both.

**Opaque cursor encoding for keyset pagination**

Encode the sort key (or composite key tuple) and metadata (optional HMAC/signature, TTL timestamp) as JSON, then base64url-encode it into an opaque token the client treats as an opaque string. Example: `cursor = base64url_encode(json.dumps({"last_pk": 42, "last_sort": 1.5, "hmac": "…"}))`. On the next page request, decode the cursor, verify the HMAC if present, extract the sort key, and use it as the WHERE clause boundary: `WHERE (sort_key, pk) > (cursor.last_sort, cursor.last_pk)` — [pypaginate cursor encoding](https://pypaginate.readthedocs.io/en/latest/concepts/cursor-encoding.html). The HMAC prevents tampering; it is optional but recommended for untrusted clients. Without HMAC, clients can craft cursors to arbitrary positions.

**RFC 8288 Link header for pagination**

Format: `Link: <https://api.example.com/items?cursor=abc>; rel="next", <https://api.example.com/items?cursor=xyz>; rel="last"` — [RFC 8288 Web Linking](https://datatracker.ietf.org/doc/html/rfc8288). URL must be in angle brackets; `rel` parameter must be quoted. Supports rel values like `next`, `prev`, `first`, `last`, and custom URIs for extension relations.

**ietf-httpapi-idempotency-key-header draft status**

As of October 2026, draft-ietf-httpapi-idempotency-key-header-07 expired April 18, 2026. Status: no RFC yet; working group discussion continues — [IETF datatracker](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/).

## Options compared

| Topic | Recommended | Notes |
|---|---|---|
| **Isolation level setting** | Per-connection `execution_options()` before BEGIN | Must be before transaction starts; per-sessionmaker is a snapshot only |
| **Savepoint pattern** | `AsyncSession.begin_nested()` as context mgr | Async-compatible; flushes unconditionally |
| **Serialization error handling** | Catch DBAPIError, retry entire txn | READ COMMITTED avoids retries; REPEATABLE READ/SERIALIZABLE require retry logic |
| **Optimistic locking** | `version_id_col` on ORM model | Raises `StaleDataError` on zero-row update; skip for bulk operations |
| **Streaming & backpressure** | `stream()` + `yield_per` | Requires open txn; consumer delay holds resources; no lazy loading |
| **MongoDB async** | PyMongo `AsyncMongoClient` (Motor deprecated) | Beanie 2.0.1 uses AsyncMongoClient natively |
| **MongoDB transactions** | `session.with_transaction(callback)` | Auto-retries transient errors for 120 s |
| **Beanie optimistic locking** | `use_revision = True` in Settings | Raises `RevisionIdWasChanged` on stale revision |
| **Conditional requests** | Strong ETag + If-Match | RFC 9110 standard; weak ETags never match on If-Match |
| **Streaming media type** | `application/json-seq` (RFC 7464) | Only RFC-standardized; `application/x-ndjson` unregistered but widely used |
| **FastAPI streaming** | `yield` in async endpoint | Built-in since 0.134.0; no explicit StreamingResponse needed |
| **Error in stream** | Union type model (SuccessItem | ErrorItem) | Most explicit; trailer record second choice |
| **Pagination cursor** | base64url(json(sort_key + hmac)) | Opaque to client; HMAC prevents tampering (optional but recommended) |
| **Link header for next** | RFC 8288 format: `<url>; rel="next"` | Registered IANA standard for link relations |

## Version/compatibility notes

- **SQLAlchemy 2.0.48**: `begin_nested()` async-compatible; `version_id_col` works identically to 1.4.
- **asyncpg 0.31.0**: Exception `.sqlstate` attribute gives SQLSTATE string; `.pgcode` property also available.
- **PyMongo 4.16.0**: `AsyncMongoClient` (native async); Motor (deprecated 2025-05-14, EOL 2026-05-14).
- **Beanie 2.0.1**: Uses PyMongo AsyncMongoClient; `use_revision` setting; `RevisionIdWasChanged` exception.
- **FastAPI 0.135.2**: Streaming JSON Lines added 0.134.0; yields auto-wrapped.
- **Starlette 1.3.1**: `Request.stream()` closes on `http.response.start` per ASGI spec.
- **RFC 9110**: Published June 2022; obsoletes RFC 7231/7232/7233.
- **RFC 8288**: Published October 2017; Web Linking standard for rel parameters.
- **RFC 7464**: Published February 2015; `application/json-seq` standard (IANA-registered).
- **draft-ietf-httpapi-idempotency-key-header-07**: Expired 2026-04-18; no RFC yet.

## Evidence gaps

1. **SQLAlchemy `stream()` exact transaction requirement** — docs confirm open txn required but don't state if nested savepoint suffices or only top-level txn required.
2. **asyncpg `pgcode` vs `sqlstate` attribute semantics** — both reportedly exist; which is canonical (string vs int?).
3. **Beanie `use_revision` interaction with `BulkWriter`** — docs note it "may work incorrectly" but do not specify the failure mode.
4. **Motor end-of-life critical-fixes scope** — unclear if all bugs or only security fixes are addressed after May 2027.
5. **NDJSON IANA registration progress** — search results show May 2026 IETF discussion but no outcome; current status unknown.
6. **FastAPI streaming error recovery** — no official guidance on half-sent responses or client reconnect behavior.
7. **Idempotency-Key draft renewal** — expired 2026-04-18; unclear if renewed or submitted to RFC process.

## Librarian's note

**What the sources indicate**:

1. **Isolation levels** — SQLAlchemy's `execution_options()` per-connection is the only stable knob; transactional isolation must be set before BEGIN via asyncpg's SET TRANSACTION, not via sessionmaker snapshot defaults.

2. **Serialization conflicts** — REPEATABLE READ and SERIALIZABLE raise 40001 (and occasionally 23505 on unique-key collisions); READ COMMITTED never raises 40001 and never needs retry logic. The retry must be whole-transaction retry, not nested-savepoint retry.

3. **Motor→PyMongo migration** — Motor is formally deprecated as of 2025 and reaches EOL in May 2026 (two months from today); any async MongoDB workload should plan migration to PyMongo `AsyncMongoClient` now.

4. **Conditional requests** — RFC 9110 is the authoritative standard; strong ETag + If-Match is the only semantically guaranteed model for optimistic concurrency at the HTTP layer (weak ETags are permitted by the spec but never match If-Match).

5. **Streaming media type** — `application/json-seq` (RFC 7464) is the only IANA-registered standard; `application/x-ndjson` (unregistered) is pragmatic and widely used. Choice depends on interop requirements; neither is wrong, but RFC 7464 is authoritative.

6. **FastAPI streaming** — available since 0.134.0 (four minor versions before 0.135); explicit `StreamingResponse` not needed, `yield`-based endpoints handle it automatically.

7. **Idempotency-Key standardization** — still in draft (expired 2026-04-18); not yet an RFC. Implementers should assume informal convention (`Idempotency-Key: <uuid>`) until draft resolves or a new RFC emerges.

Sources cited:
- [SQLAlchemy 2.0 Asyncio Documentation](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html)
- [SQLAlchemy 2.0 Transactions & Connection Management](https://docs.sqlalchemy.org/en/20/orm/session_transaction.html)
- [SQLAlchemy 2.0 Versioning (Optimistic Locking)](https://docs.sqlalchemy.org/en/20/orm/versioning.html)
- [asyncpg Exception Handling](https://tessl.io/registry/tessl/pypi-asyncpg/0.30.0/files/docs/exception-handling.md)
- [PostgreSQL 18 MVCC Serialization Failure Handling](https://www.postgresql.org/docs/18/mvcc-serialization-failure-handling.html)
- [MongoDB PyMongo Transactions Documentation](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/transactions)
- [MongoDB Python Driver Migration Guide](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/reference/migration)
- [Beanie Revision Tutorial](https://beanie-odm.dev/tutorial/revision/)
- [RFC 9110: HTTP Semantics](https://datatracker.ietf.org/doc/html/rfc9110)
- [RFC 8288: Web Linking](https://datatracker.ietf.org/doc/html/rfc8288)
- [RFC 7464: JSON Text Sequences](https://datatracker.ietf.org/doc/html/rfc7464)
- [FastAPI Streaming JSON Lines Tutorial](https://fastapi.tiangolo.com/tutorial/stream-json-lines/)
- [Starlette Requests Documentation](https://www.starlette.dev/requests/)
- [IETF Idempotency-Key Header Draft](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/)
- [Speakeasy JSON Lines Events Documentation](https://www.speakeasy.com/docs/sdks/customize/runtime/jsonl-events.md)
- [pypaginate Cursor Encoding Concepts](https://pypaginate.readthedocs.io/en/latest/concepts/cursor-encoding.html)
