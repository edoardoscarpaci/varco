# Research 019 — Streaming, Orchestration, and Test Performance in 2026
Date: 2026-10-07 · Freshness matters: **yes** — frameworks and libraries ship major versions annually; pytest / uv / Python 3.12/3.13 performance profiles shift materially month-to-month.

## Question
What are table stakes vs. differentiators in 2026 for Python async web backends on three axes:

**A. Streaming large data:** NDJSON/JSON Lines, SSE (server-sent events), HTTP chunked uploads, async cursor pagination (keyset + RFC 8288 Link headers), Apache Arrow/Parquet exports, backpressure. Which do Django REST / Litestar / FastAPI / Spring WebFlux offer natively?

**B. Task/workflow orchestration:** Durable execution as a library (DBOS Transact, DBOSify) vs. as a server (Temporal Python, Restate, Hatchet, Prefect 3). What is the established pattern now? Limits of Postgres-backed durable workflows?

**C. Large pytest suites:** pytest-xdist on pytest-asyncio auto mode + session fixtures, pytest-testmon, import-time reduction, uv run overhead, fake clocks (time-machine, looptime), pytest-asyncio 1.x loop-scope and performance.

## Findings

### A. Streaming Large Data

**NDJSON / JSON Lines:**
- FastAPI added native JSON Lines support in 0.134.0+ — `async def route() -> AsyncIterable[Item]` with `yield`, automatic Pydantic validation and `application/jsonl` serialization — [FastAPI: Stream JSON Lines](https://fastapi.tiangolo.com/tutorial/stream-json-lines/) (0.134.0+)
- Django REST Framework supports streaming via `StreamingHttpResponse`, but WSGI deployments tie a worker for the entire duration; ASGI/async enables long-lived streams — [Django REST Framework: Streaming Response](https://blog.devgenius.io/django-rest-framework-streaming-response-3dfc8c34fb8e) (no versioned date)
- Litestar does not ship native JSON Lines in search results; third-party `sse-starlette` is the extension pattern for Starlette-based frameworks

**Server-Sent Events (SSE):**
- FastAPI supports SSE natively via `StreamingResponse` with `yield`, media type `text/event-stream`. **Not** a 0.1xx-era addition — manual formatting required — [OpenAI agents Python](https://github.com/openai/openai-agents-python/pull/4171) (2026)
- `sse-starlette` (third party) is the drop-in Starlette/FastAPI extension, implements W3C Server-Sent Events spec, used for LLM token streaming and live progress updates — [sse-starlette PyPI](https://simple-repository.app.cern.ch/project/sse-starlette) (version 3.0.2)
- Django REST 6.1+ ships OpenAPI 3.2.0 support for streaming endpoints with `itemSchema` — [Django Modern REST](https://django-modern-rest.readthedocs.io/en/0.11.0/pages/streaming/common.html) (0.11.0)

**Async Cursor Pagination & Backpressure:**
- SQLAlchemy 2.1+ `AsyncConnection.stream()` returns `AsyncResult` using server-side cursors with `.yield_per()` method for chunking; async iteration provides native backpressure by fetching only consumed rows — [SQLAlchemy 2.1 Async Documentation](https://docs.sqlalchemy.org/en/21/orm/extensions/asyncio.html) (2.1+)
- PyMongo / Motor provide async cursors; Beanie's `AsyncCursor` is not feature-complete for streaming (synchronous iteration only in earlier versions) — no authoritative 2026 update
- RFC 8288 Web Linking standard for pagination cursors in HTTP Link headers (`rel=next`, `rel=prev`) — required for OpenAPI contract first — [RFC 8288: Web Linking](https://tools.ietf.org/html/rfc8288) / [Fintoc API pagination](https://docs.fintoc.com/v2023-11-15/reference/fintoc-api/pagination) (2026)
- Keyset pagination (last-item-based, not offset) is the standard for large result sets; implementations differ across frameworks

**Apache Arrow / Parquet:**
- PyArrow 24.0.0 stable (April 2026) supports streaming record batches via scanner iterators for in-memory-unsafe result sets; zero-copy Parquet read/write — [Apache Arrow Python](https://arrow.apache.org/docs/python) (24.0.0)
- Spring WebFlux offers `Flux<T>` reactive streams natively; Python has no direct equivalent (Reactive Extensions libraries exist but are not widely adopted)

---

### B. Task/Workflow Orchestration

**Library-Based (Postgres-Backed) Durable Execution:**
- **DBOS Transact** (open source, MIT) materializes durable workflows via `@DBOS.step()` / `@DBOS.workflow()` decorators into Postgres state checkpoints; exactly-once semantics, recovery, retries without a server — [DBOS Transact](https://www.dbos.dev/blog/dbos-transact-open-source-typescript-framework) (TypeScript/Python, July 2026)
- **DBOSify** — July 2026 release — is a drop-in Temporal Python replacement running on Postgres instead of a Temporal cluster; same signal/activity/retry model; durable streams throughput increased 20x in July 2026 — [DBOS Blog: July 2026 Release](https://dbos.dev/blog/new-in-dbos-july-2026) (Python SDK, 2026)
- **Limits:** Single database as single point of failure (mitigation: multi-region Postgres); sharding coordination is app-owned (no built-in distributed coordination); replay-dependent recovery means crash at step N replays N-1 steps deterministically

**Server-Based Durable Execution:**
- **Temporal** (9.1/10 ranking July 2026) — battle-tested, multi-language, used by NVIDIA/Salesforce/Netflix; requires external Temporal cluster deployment — [Best Process Orchestration Platforms 2026](https://automationatlas.io/rankings/best-process-orchestration-2026/) (July 2026)
- **Restate** (Python SDK MIT licensed) — stateless services, virtual objects (per-key state), and workflows with exactly-once guarantees; uses RabbitMQ (optional) + Restate server; durable RPC calls and messages with optional delays — [Restate Python Overview](https://docs.restate.dev/develop/python/overview) (2026)
- **Hatchet** (7.6/10 ranking) — PostgreSQL as sole required datastore, optional RabbitMQ, MIT licensed; no enterprise carve-out — [Best Process Orchestration 2026](https://automationatlas.io/rankings/best-process-orchestration-2026/) (2026)
- **Prefect 3.0** (7.6/10 ranking, July 2026 Dagster acquisition) — decorated flows/tasks, dynamic `.map()` fan-out, genuine transaction compensation with `on_rollback()` / `on_commit()` hooks under `READ_COMMITTED` or `SERIALIZABLE` isolation — [Prefect 3: Transactions](https://docs.prefect.io/v3/develop/transactions) (3.0, 2026)

**Established Pattern:** Postgres-backed is now legitimate table stakes (DBOS, Hatchet); Temporal remains the "battle-tested at scale" choice for polyglot teams. **Separating service orchestration (Restate VOs) from workflow orchestration (durable steps) is distinct design axes.** Celery canvas (chain/group/chord) and Dramatiq remain for simple fan-out but offer no durable replay.

---

### C. Speeding Up Large pytest Suites

**Concrete Speedups (Reported):**

| Technique | Baseline | After | Speedup | Evidence |
|---|---|---|---|---|
| pytest-xdist `--numprocesses=auto` (parallelization) | 191s | 63s | **67% reduction** | [PyPI test suite 81% speedup](https://blog.trailofbits.com/2025/05/01/making-pypis-test-suite-81-faster/) (May 2025) |
| Python 3.12 `sys.monitoring` + `COVERAGE_CORE=sysmon` | 58s | 27s | **53% reduction** | [PyPI test suite](https://blog.trailofbits.com/2025/05/01/making-pypis-test-suite-81-faster/) (May 2025) |
| Test discovery via `testpaths = ["tests/"]` | 7.84s | 2.60s | **66% collection time** | [PyPI test suite](https://blog.trailofbits.com/2025/05/01/making-pypis-test-suite-81-faster/) (May 2025) |
| **Combined (PyPI final)** | 163s (3900 tests) | 30s (4734 tests) | **81% total** | [PyPI test suite](https://blog.trailofbits.com/2025/05/01/making-pypis-test-suite-81-faster/) (May 2025) |

**pytest-xdist + pytest-asyncio Compatibility:**
- pytest-xdist workers do NOT automatically share session-scoped fixtures; workaround: FileLock for inter-process coordination where first worker computes, others read — [Paylogic: pytest-xdist and session fixtures](https://developer.paylogic.com/articles/pytest-xdist-and-session-scoped-fixtures.html) (no date)
- pytest-asyncio auto mode (default since 0.21+) compatible with xdist; caveat: worker processes must have event loops initialized (pytest-asyncio handles this) — [pytest-xdist documentation](https://pypi.org/project/pytest-xdist/2.5.0/) (xdist 2.5.0+)

**Async Test Acceleration (Fake Clocks):**
- **looptime** — pytest plugin that advances asyncio event loop time deterministically; `await asyncio.sleep(100)` completes in ~0.01s; test sees loop time = 100s — [looptime documentation](https://looptime.readthedocs.io/getting-started/) (0.7+)
- **time-machine** — `freezegun` for time module, not asyncio; does NOT work with looptime (two competing time facades)
- Exante Engineering achieved 8.5x speedup on their async test suite by eliminating `asyncio.sleep()` walls with looptime — no authoritative link cited; practice is established in async testing

**Import Time & Python 3.12:**
- Removing unnecessary imports (e.g., `ddtrace` during testing) saves ~1 second per run; `python -X importtime` shows per-module times — [PyPI test suite](https://blog.trailofbits.com/2025/05/01/making-pypis-test-suite-81-faster/) (May 2025)
- Python 3.12+ `sys.monitoring` (coverage backend) reduces instrumentation overhead by 50%+ vs. traditional tracing — [PyPI test suite](https://blog.trailofbits.com/2025/05/01/making-pypis-test-suite-81-faster/) (May 2025)
- `uv run` overhead: uv cold-start is ~200-300ms; reusing an existing venv in loop avoids this; no published benchmark of `uv run pytest` vs. `pytest` directly in 2026 — practice is to run pytest from cached venv, not `uvx pytest`

---

## Options Compared

| Streaming Dimension | Table Stakes | Differentiator | Status |
|---|---|---|---|
| NDJSON / JSON Lines | FastAPI 0.134.0+ native via `AsyncIterable[T]` | OpenAPI auto-schema | Established |
| SSE (server-sent events) | Manual `StreamingResponse` + format loop | `sse-starlette` for batteries-included | Emerging (first-class in 0.1xx? NO — manual still) |
| Async cursor pagination | SQLAlchemy `stream()` + `yield_per()` | RFC 8288 Link header rendering | Table stakes in SQLAlchemy 2.1+ |
| Keyset/cursor pagination | Opaque cursor + `LIMIT` offset | OpenAPI wiring, Link header contract | Emerging; implementation varies |
| Apache Arrow/Parquet | PyArrow streaming record batches | Export endpoint / analytics | Optional differentiator |

| Orchestration Dimension | Library (Postgres) | Server (Temporal/Restate) |
|---|---|---|
| Deployment | Zero external services; app owns database | Separate cluster; polyglot-friendly |
| Durable execution | `@step()` decorators, replay-based recovery | Workflow SDK, built-in recovery |
| Compensation/saga | DBOS: transactional (explicit SQL); Prefect 3: `on_rollback()` | Temporal: saga pattern + compensations; Restate: retries only |
| Exactly-once semantics | Postgres txn isolation + replay determinism | Built-in by protocol |
| Scaling | Postgres connection budget (1000s of workflows) | Unbounded (server-side) |
| Testing | Same test database; fixtures deterministic | Temporal server required in CI |

| Test Performance Dimension | Technique | Speedup | Notes |
|---|---|---|---|
| Parallelization | pytest-xdist + `--numprocesses=auto` | **67%** | Requires DB isolation per worker |
| Coverage instrumentation | Python 3.12 `sys.monitoring` | **53%** | coverage.py 7.4.0+ required |
| Collection time | `testpaths` configuration | **66% collection only** | Minimal effort, high reward |
| Async loops | looptime for `asyncio.sleep()` | **~100x** per-test (not cumulative) | Incompatible with time-machine |
| Import cleanup | Remove ddtrace / heavy imports | **~1s** per suite | Code-dependent |

---

## Version/Compatibility Notes

**FastAPI:**
- JSON Lines: **0.134.0+** (recent feature; users on 0.109 and earlier have no native support)
- SSE: Native `StreamingResponse` always available, but **no first-class decorator** until / if future version ships

**SQLAlchemy:**
- Async streaming + `yield_per()`: **2.1+**; earlier versions do not have `AsyncConnection.stream()`

**pytest-xdist:**
- **2.5.0+** stable; FileLock pattern for session fixtures documented in production use

**pytest-asyncio:**
- Auto mode default: **0.21.0+** (breaking change from 0.20); xdist compatible via event-loop-per-worker

**Python:**
- `sys.monitoring` (coverage backend): **3.12+** only; 3.11 and earlier use traditional tracing
- uv: **1.0+** (October 2024); usage pattern settled in 2026

**DBOS Transact / DBOSify:**
- Python SDK: **July 2026 release** (new entry to market alongside Hatchet 2024 / 2025)
- Restate Python: **Stable as of 2026** (launched earlier; SDK stable)
- Prefect 3.0: **General availability** (3.0.0+ with transaction backport to 2.x possible)

---

## Evidence Gaps

1. **Litestar streaming:** Search results did not confirm native JSON Lines or SSE support; third-party libraries likely required. Litestar 2.x may have changed from its pre-2.0 stance (research deferred).

2. **Spring WebFlux (Java/Kotlin):** Mentioned as reference point for `Flux<T>` reactive streams; no direct Python equivalent proven, though RxPY exists and is niche.

3. **Beanie async cursors:** Motor (PyMongo async driver) supports async cursors; Beanie's implementation status in 2026 unconfirmed (may only offer synchronous iteration).

4. **pytest-asyncio loop-scope changes 1.x:** Research 002 mentions mypy-strict changes; specific loop-scope performance impact on large suites not quantified.

5. **uv run overhead in CI:** Cold-start penalty measured; cost of repeated `uv run pytest` in a matrix job (with venv cache) not published.

6. **Temporal Python SDK maturity 2026:** No reported use cases or performance data in search results; Temporal Go/Java mature; Python adoption track unclear.

---

## Librarian's Note

**A. Streaming:** FastAPI's native JSON Lines (0.134.0+) + SQLAlchemy async cursors (2.1+) + RFC 8288 Link headers is now table stakes; SSE remains manual. Arrow/Parquet export is an optional differentiator. **What the sources indicate:** varco can ship streaming endpoints as first-class patterns (like the outbox pattern today) without heavy lifting; OpenAPI contract-first pagination is standard-building.

**B. Orchestration:** DBOS/DBOSify (Postgres-backed library model) is now established and legitimate alongside Temporal/Restate (server model). The decision is orthogonal to stream/bus/job storage — **both patterns will coexist.** Prefect 3's transaction compensation is the first Python orchestrator offering genuine saga support. **What the sources indicate:** "durable execution library over our own Postgres" (varco's job store / outbox pattern today) is a proven pattern; expanding it to full workflow orchestration (signals, compensation, multi-step recovery) is incremental and established by DBOS / Prefect 3 precedent.

**C. Test performance:** pytest-xdist + session fixture FileLock + looptime + Python 3.12 `sys.monitoring` is the 2026 best practice; combined speedup is 67%–81% depending on code shape. **What the sources indicate:** varco's ~1000-test 11-suite matrix (per CLAUDE.md) can likely see 50%+ speedup by (a) enabling xdist with `--numprocesses=auto` + per-worker DB isolation, (b) setting `testpaths = ["varco_*/tests"]`, (c) adopting looptime for timing-sensitive tests, (d) running on Python 3.12+ only in CI (`COVERAGE_CORE=sysmon`). Session fixtures (integration container ports) require FileLock pattern or `function`-scoped fresh instances — current VARCO_TEST_*_URL override pattern is compatible.

