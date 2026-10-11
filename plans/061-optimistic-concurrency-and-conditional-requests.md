# Plan 061 — Optimistic concurrency and HTTP conditional requests (ACID-2)

Covers BACKLOG 4.0.0 row **`ACID-2`** (`BACKLOG.md:57` — *"Optimistic concurrency + HTTP
conditionals — version column on `DomainModel`, ETag on GET, `If-Match` on PUT/PATCH/DELETE → 412
on mismatch, opt-in 428 when absent"*). Ships inside **4.0.0** (locked decision `BACKLOG.md:31`:
breaking changes allowed outright, each with a `technical_docs/migrations/3-to-4.md` section). One
of eleven plans (059–069), indexed by `plans/000-index-4-0-feature-extension.md` (written by the
index task, not this plan).

**Neighbours and what each takes from this plan:**

| Plan | Row | Handoff |
|---|---|---|
| 062 | `ACID-1` ambient `@transactional` + whole-transaction retry on 40001/40P01 | Imports `varco_core.exception.OPTIMISTIC_LOCK_ERRORS` (§D-exceptions) and **never retries** an exception in it. Under REPEATABLE READ/SERIALIZABLE, Postgres reports a concurrent UPDATE as 40001 *before* our version predicate sees zero rows; 062's retry re-runs the whole use case, which re-loads the row and re-evaluates `expected_version` → 412. That is correct and is 062's test to write, not this plan's |
| 063 | Mongo transactions | The Beanie conditional `replace_one`/`delete_one` calls added in Step 6 take no `session=` today (the repository has none, `varco_beanie/repository.py:38`); 063 threads its session into exactly those calls |
| 067 | `STREAM-1` NDJSON reads | Nothing to do: every NDJSON line is a `ReadDTO`, so a versioned entity's line already carries `row_version` (the per-item validator, §D-dto) |
| 068 | `STREAM-2` NDJSON ingest | `save_many()` on a versioned entity performs the atomic check per row (Steps 4/6) and raises `StaleEntityError`; 068 decides whether ingest lines may carry `row_version` and how a stale line is reported. This plan adds no ingest-specific behaviour |
| 065/066 | `GDPR-1/2` | Erasure/anonymisation through the service or repository bumps `row_version` → outstanding ETags 412 afterwards (desired). A long-running erasure job that holds a loaded entity across awaits must reload on `StaleEntityError` |

**Sources of record read for this plan (2026-10-07):** `BACKLOG.md:27-37,57`;
`plans/058-di-guide-migration-and-4-0-release.md` (house format); research briefs
`design/research/021-transactions-conditional-requests-streaming.md` §A.4 (`:57-59`), §B (`:79-81`),
§C (`:83-95`), Evidence gaps (`:178-186`) and `design/research/018-transactionally-correct-endpoints-and-gdpr-frameworks.md`
§A (`:24-30` Rails `lock_version`/EF tokens, `:47-49` RFC 9110/6585); source files cited inline
below.

> ⚠️ **Three premises corrected from source before anything was decided.**
> (1) **"No version column anywhere in the tree" is false.** `VersionedDomainModel`
> (`varco_core/varco_core/model.py:306-354`) already declares `row_version: int` (`init=False`,
> default `0`), and `TenantVersionedDomainModel` (`:521-563`) inherits it. The mapper sets it to `1`
> on INSERT and `old + 1` on UPDATE (`varco_core/varco_core/mapper.py:379,510-511`), the SA factory
> already emits the column (`varco_sa/tests/test_sa_factory.py:211`), and `generate_dtos()`
> already puts it in the ReadDTO (`varco_core/tests/test_dto_factory.py:433`).
> (2) **The existing check does not prevent lost updates.** `sync_to_orm()` compares
> `orm_obj.row_version` with `domain.row_version` *in Python* (`mapper.py:483-493`), and both SA
> (`varco_sa/varco_sa/repository.py:196-209`) and Beanie (`varco_beanie/varco_beanie/repository.py:180-183`)
> then write with a predicate on the primary key only. Two requests that each load version 1 in
> their own session both pass the check and both write version 2 — the second silently overwrites
> the first. The Python check only fires when a stale domain object meets a fresher ORM object in
> the *same* identity map (`varco_sa/tests/test_sa_repository.py:233-250` is exactly that shape).
> So the BACKLOG rationale ("lost updates are possible today") is right; its "no column" evidence is
> not.
> (3) **`StaleEntityError` reaches clients as a 500 today.** It is a `RepositoryException`
> (`varco_core/varco_core/exception/repository.py:70-100`), not a `ServiceException`; nothing in
> `AsyncService.update()` (`varco_core/varco_core/service/base.py:1134-1211`) catches it, despite
> the docstrings claiming it is "wrapped at the service boundary" as `ServiceConflictError`
> (`varco_core/varco_core/exception/service.py:197-198,212-214`). `ErrorMiddleware` renders
> non-`ServiceException`s through `_internal_error_response()` (`varco_fastapi/varco_fastapi/middleware/error.py:246-247`).
> No `ETag`/`If-Match` handling exists in any router (the only `etag` in `varco_fastapi` is the
> idempotency never-replay list, `varco_fastapi/varco_fastapi/middleware/idempotency.py:101`).

## Goal

After this plan: an entity that carries `row_version` (via the new `RowVersionMixin` or the
existing `VersionedDomainModel`) is protected against lost updates **atomically** on both backends
— SQLAlchemy through `version_id_col` (the UPDATE/DELETE carries `AND row_version = :loaded`), Beanie
through a `row_version`-filtered `replace_one`/`delete_one` — and every stale write surfaces as
`StaleEntityError` from the repository and as `ConcurrencyConflictError` (409) or
`PreconditionFailedError` (412) from `AsyncService`. `AsyncService.update/patch/delete` accept
`expected_version=`; the CRUD router sends a strong `ETag: "<row_version>"` on GET/POST/PUT/PATCH,
evaluates `If-Match` on PUT/PATCH/DELETE (412 on mismatch), and answers 428 when
`VARCO_CONDITIONAL_REQUIRE_IF_MATCH=true` (or a per-router override) and the header is absent.
The generated client can send `If-Match`, CORS exposes `ETag` and allows `If-Match`, a 412/428 is
never stored by `IdempotencyMiddleware`, and `OPTIMISTIC_LOCK_ERRORS` exists for Plan 062 to
exclude from retry. Docs, migration guide, CHANGELOG, and snapshot gates are updated.

## Non-goals

- **Versioning every `DomainModel` by default** — rejected in §D-optin; varco does not own app
  tables and cannot ship the `ALTER TABLE` every app would need.
- **`If-None-Match` / `304 Not Modified` on GET** — parked (§D-etag): a version-derived ETag
  validates *resource state*, not the byte representation, and representations here vary by
  `Accept-Language`, caller role (field redaction), and timezone rendering. Trigger: a measured
  bandwidth need plus a representation-scoped validator design.
- **`If-Unmodified-Since` / `Last-Modified`** — `updated_at` has wall-clock resolution and is not
  monotonic across hosts; `If-Match` is the one precondition implemented.
- **`If-Match` on `POST` (create)** — the target is the collection, which has no validator; the
  create handler ignores the header (recorded deviation from RFC 9110 §13.1.1's "MUST evaluate";
  a client conditioning a create on a collection ETag is not a shape anyone has asked for).
- **Preconditions on job-submitted CRUD tasks** (`_task_update`/`_task_patch`/`_task_delete`,
  `varco_fastapi/varco_fastapi/router/crud.py:508-570`) — those payloads carry no precondition;
  unchanged.
- **Replaying `ETag` on an idempotent replay** — Plan 029 put `etag` in `_NEVER_REPLAY_HEADERS`
  (`idempotency.py:94-107`); not reopened. A replayed body still carries `row_version`.
- **A posture/diagnose finding for unversioned CRUD routes** — opt-in is the design; documented in
  the feature page instead.
- **Pessimistic locking (`SELECT … FOR UPDATE`)** and **isolation levels** — Plan 062's boundary.
- **Changing `ServiceContract`'s schema** — CRUD methods are regenerated from `crud_action`
  (`varco_fastapi/varco_fastapi/contract/model.py:90-91`), so `contract_client`/`gen-client` pick
  up the new client keyword automatically.
- **Mapping a raw `StaleEntityError` in `ErrorMiddleware`** — repository exceptions stay internal;
  only `AsyncService` translates (custom services must translate their own, §Edge cases).

## Design

### §D-optin — opt-in per model, keyed on the `row_version` field; no new column for existing versioned users

**Chosen:** an entity is *versioned* iff its dataclass declares a field named `row_version` with
`init=False` — the exact condition the mapper already uses (`"row_version" in self._post_init_fields`,
`mapper.py:484`). New `RowVersionMixin` (one field, same declaration as `VersionedDomainModel`'s)
lets any base opt in without `definition_version`/audit timestamps:

```python
@dataclass(kw_only=True)
class Post(RowVersionMixin, AuditedDomainModel): ...   # locking only
class Doc(VersionedDomainModel): ...                    # locking + schema migration, unchanged
```

`is_versioned(domain_cls) -> bool` (in `varco_core/model.py`) is the single predicate the SA
factory, Beanie repository, service, and router all call. `VersionedDomainModel` and
`TenantVersionedDomainModel` are **left byte-identical** (no MRO refactor → no field/column/DTO
order change for existing users); the mixin duplicates one field declaration, accepted.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: opt-in via `row_version` (mixin or existing base)** | Industry norm — Rails enables locking by `lock_version` column presence, EF Core per property, both opt-in (brief 018 §A `:24-30`); existing `VersionedDomainModel` apps get atomic locking with **zero schema change** (column already exists); unversioned apps unaffected | Lost updates remain possible on entities nobody opted in — mitigated by the feature page's first paragraph, README, and the migration guide's recommendation |
| (2) `row_version` on `DomainModel` itself (on for all) | No one can forget | Every app table needs an `ALTER TABLE … ADD COLUMN row_version INTEGER NOT NULL DEFAULT 1` before 4.0 boots — varco does not own app tables and cannot ship that revision (same rule as RLS, `plans/037` §D-S12-oq4); every SELECT fails until the app migrates; composite-PK and read-only models pay for nothing. Rejected |
| (3) `Meta.optimistic_lock = True` flag + field injected by the factory | No field on the dataclass | The domain object would not carry its own version — the service could not evaluate `expected_version` without reaching into `_raw_orm`; a second mechanism beside the field the mapper already keys on. Rejected |

### §D-atomic — the stale-write guarantee lives in the write statement, on both backends

**SQLAlchemy (chosen):** for a versioned class, `SAModelFactory.build()` adds
`"__mapper_args__": {"version_id_col": <the row_version Column>, "version_id_generator": False}` to
`orm_attrs` (`varco_sa/varco_sa/factory.py:320-324`). SA then emits `UPDATE … WHERE id = :id AND
row_version = :loaded` and raises `StaleDataError` on a zero-row UPDATE (brief 021 §A.4 `:59`).
`version_id_generator=False` keeps the **mapper the single writer** of the new value
(`mapper.py:510-511` already writes `old + 1`), while SA uses the *loaded* value in the predicate
(⚠️ ASSUMPTION A1, Step 0). DELETE gets the same predicate (⚠️ A2). `SARepository.save()`,
`delete()`, `save_many()` catch `sqlalchemy.orm.exc.StaleDataError` around `flush()` and raise
`StaleEntityError(entity_cls, expected_version=<loaded>, actual_version=None) from exc`. The
Python pre-check in `sync_to_orm()` stays (cheap; still catches the same-identity-map shape) but is
documented as *not* the guarantee.

**Beanie (chosen):** for a versioned class, `BeanieRepository.save()`'s UPDATE path captures
`loaded = entity.row_version` *before* `sync_to_orm()`, then writes with
`orm_cls.find_one({"_id": raw.id, "row_version": loaded}).replace_one(raw)` (⚠️ A5) instead of
`raw.save()`. `matched_count == 0` → re-read by id: absent → `LookupError` (same as SA's
not-found UPDATE, `repository.py:202-206`); present → `StaleEntityError(actual_version=doc.row_version)`.
`delete()` uses `find_one({"_id": …, "row_version": loaded}).delete()`; `deleted_count == 0` and the
document still exists → `StaleEntityError`; absent → no-op (today's semantics). Unversioned
classes keep `raw.save()`/`raw.delete()` byte-identical.

**Bulk paths** (brief 021 `:59`: Core UPDATE "bypasses versioning entirely"):
`update_many_by_query()` on a versioned class adds `row_version = row_version + 1` (SA
`sa_update(...).values(row_version=col + 1, **update)`; Beanie `{"$set": update, "$inc":
{"row_version": 1}}`) so a bulk update invalidates outstanding ETags; an `update` dict naming
`row_version` → `ValueError` ("managed field"). `delete_many()` on a versioned class predicates
each row on its loaded version (SA `or_(and_(pk == p, row_version == v), …)`; Beanie `{"$or":
[{"_id": p, "row_version": v}, …]}`) and raises `StaleEntityError` when fewer rows are deleted than
given — for versioned entities an already-deleted row is indistinguishable from a stale one and
is reported as stale (documented edge case).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: SA `version_id_col` + Beanie `row_version`-filtered writes, one token** | One token (`row_version`) feeds the ETag, the service check, and both backends' predicates; SA's mechanism is the documented one (brief 021 §A.4); works inside the existing ORM flush (identity map intact) | Beanie needs hand-rolled conditional calls (two methods) and a re-read to tell "stale" from "gone" |
| (2) Beanie `Settings.use_revision = True` → `RevisionIdWasChanged` (brief 021 §B `:81`) | Beanie-native | A **second** token (`revision_id`, a UUID) persisted in every document beside `row_version` — the ETag and the check could disagree; does not cover `delete()`; brief 021's own Evidence gap 3 (`:182`): bulk behaviour unspecified. Rejected |
| (3) Hand-rolled Core `UPDATE … WHERE row_version = ?` + rowcount on SA | Explicit | Bypasses the ORM flush — identity map and dirty tracking diverge from the DB, `from_orm(raw)` would return stale state. Rejected |
| (4) `SELECT … FOR UPDATE` | Familiar | Pessimistic; holds row locks across authorization/validation hooks; not what the row asks for; isolation is 062's. Rejected |

### §D-api — `expected_version=` is an explicit keyword on the service, evaluated after authorization

**Chosen:** `AsyncService.update(pk, dto, ctx, *, expected_version: int | AbstractSet[int] | None =
None)`, same keyword on `patch()` (forwarded to `update()`) and `delete()`. Semantics inside the
existing UoW, in this order (RFC 9110 §13.2.1: preconditions are evaluated only when the request
would otherwise succeed, so 404/403 win; brief 021 §C `:89`):

1. `find_by_id` → `ServiceNotFoundError` (404) — unchanged.
2. `_check_entity` / `_async_check_entity` / `authorize()` (403) — unchanged.
3. **New** `self._check_expected_version(entity, expected_version)`: `None` → no-op. Otherwise
   normalise to a `frozenset[int]`; if the entity is not versioned **or** `entity.row_version` is
   not in the set → `PreconditionFailedError` (412). An empty set never matches (the router's
   encoding of "only weak/garbage tags were sent").
4. `apply_update` → validate → `repo.save()` / `repo.delete()`. A `StaleEntityError` from the
   repository (the race between step 1 and the write) is translated: `expected_version` given →
   `PreconditionFailedError`; not given → `ConcurrencyConflictError` (409).

`SoftDeleteService.delete()` (`varco_core/varco_core/service/soft_delete.py:209-266`) gets the
same keyword and the same two insertions (it writes through `save()`); `restore()` gets the
`StaleEntityError` → `ConcurrencyConflictError` translation only (no HTTP route). The three in-tree
overriders forward it: `CacheServiceMixin.update/delete` (`varco_core/varco_core/cache/mixin.py:481,507`),
`TracingServiceMixin.update/delete` (`varco_core/varco_core/observability/mixin.py:141,145`), and
`ServiceProtocol` (`varco_core/varco_core/service/types.py:191,210`) gains the keyword.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: explicit keyword-only argument** | Visible at every call site; usable from non-HTTP code (`await svc.update(pk, dto, ctx, expected_version=3)`); a nested service call made *during* the request can never accidentally inherit the outer request's precondition | **BREAKING** for an out-of-tree mixin that overrides `update`/`patch`/`delete` without `**kwargs`: it raises `TypeError` the first time a caller passes the keyword. The router passes it only when `If-Match` was sent, so un-migrated overrides keep working for unconditional traffic; migration guide entry |
| (2) An `AmbientVar` set by the router, read by `AsyncService` | No signature churn | A precondition is about *one target resource*; ambient scope leaks it into every service call in the request (service A's update calling service B's update would test B against A's ETag). Keying it by `(entity_type, pk)` fixes that at the cost of magic nobody can see at the call site. Rejected |
| (3) Carry it in `AuthContext.metadata` | No signature change | `AuthContext` is identity, not request shape; frozen and shared. Rejected |

### §D-exceptions — three service exceptions, one tuple for Plan 062

New module `varco_core/varco_core/exception/concurrency.py` (same shape as `rate_limit.py:60-136`:
classes + `register_error_code()` at import time):

```
ServiceConflictError (409)
└── ConcurrencyConflictError     → 409  VARCO_CONCURRENCY_001  message_key varco.error.concurrency_conflict
    └── PreconditionFailedError   → 412  VARCO_PRECONDITION_001 message_key varco.error.precondition_failed
ServiceException
└── PreconditionRequiredError     → 428  VARCO_PRECONDITION_002 message_key varco.error.precondition_required

OPTIMISTIC_LOCK_ERRORS: tuple[type[Exception], ...] = (StaleEntityError, ConcurrencyConflictError)
```

`error_code_for()` walks the MRO checking the custom map first (`varco_core/varco_core/exception/http.py:232-236`),
so `PreconditionFailedError` resolves to 412 although its parent is registered 409 — and
`add_exception_handlers()`'s `ServiceConflictError` handler (`varco_fastapi/varco_fastapi/exceptions.py:259-261`)
renders both through `error_message_for()` with the right status. `PreconditionRequiredError` gets
its own `@app.exception_handler` in `add_exception_handlers()` so it does not fall into the
generic `ServiceException` handler that logs at ERROR (`exceptions.py:267-275`).

`error_params()` = `{"entity": entity_cls.__name__}` only — never the expected/actual versions,
never the current ETag (§D-status). `str(exc)` carries the versions for server logs only (it is
not emitted: `include_detail` is `False` since Plan 054, `http.py:336-337`). `StaleEntityError`'s
`actual_version` becomes `int | None = None` (SA cannot know it without a re-read); its message
omits "found N" when `None`.

✅ 062 excludes one `isinstance(exc, OPTIMISTIC_LOCK_ERRORS)` and covers both the repository-level
(direct repo use inside `@transactional`) and service-level shapes. ✅ `except ServiceConflictError`
in user code still catches every concurrency failure. ❌ Two new codes in the error taxonomy;
accepted — each is a distinct client action (re-GET vs. re-submit vs. add a header).

### §D-status — 412 iff the caller supplied a precondition; 409 otherwise; never echo the current ETag

| Situation | Status | Why |
|---|---|---|
| `If-Match` present, current version not in it | **412** | RFC 9110 §13.1.1 (brief 021 §C `:89`) |
| `If-Match` present and matched, then the write raced (step 4) | **412** | From the client's view its validator is no longer current; same client action (re-GET) |
| No `If-Match`, write raced | **409** | No condition was given, so 412 ("a condition evaluated to false") would be wrong; it is a state conflict |
| `If-Match: *`, entity exists | 2xx | `*` matches any current representation |
| Entity missing | **404** | RFC 9110 §13.2.1 — a 404 without the precondition wins |
| Caller not authorized | **403** | Same rule; also avoids a version oracle for unauthorized callers |
| `If-Match` on an **unversioned** entity (not `*`) | **412** | The resource has no strong validator, so no tag can match — the RFC-correct and safe answer; silently ignoring would let a client believe it wrote conditionally |
| Header absent, `require_if_match` on, entity versioned | **428** | RFC 6585 §3 (brief 018 `:49`, brief 021 `:93`) |

The 412/409 body never carries the current ETag or version: a client that could copy it from the
error into a retry would blind-overwrite the very change the 412 protects — it must re-GET.

### §D-etag — strong, `"<row_version>"`, resource-state validator

`ETag: "7"` — the quoted decimal `row_version`, **strong** (no `W/`), set on GET-by-id 200,
POST 201, PUT/PATCH 200 for versioned entities (ETag on write responses is "best practice, not
mandated", brief 021 `:93`). DELETE 204 carries none.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: `"<row_version>"`** | The client can build `If-Match` from the body's `row_version` — essential for list items (no per-item headers) and for replayed idempotent responses (no `ETag`, §Non-goals); trivially parsed back to an int; strong, as `If-Match` requires (weak tags never match, brief 021 `:89,198`) | Reveals the modification count — but `row_version` is already in the ReadDTO body (`test_dto_factory.py:433`), so nothing new leaks; a tag copied between two resources at the same version matches (a client bug, never an authorization bypass — authz and tenancy run first) |
| (2) `"<hash(type,pk,tenant)>-<version>"` | Tags never collide across resources | The client can no longer derive `If-Match` from `row_version`; list items would need an `etag` body field; tenant-in-tag breaks `GLOBAL`-scoped entities read from two tenants. Rejected |
| (3) Weak `W/"<version>"` | Honest about representation variance | Can never satisfy `If-Match` (strong comparison only) — defeats the row. Rejected |
| (4) Hash of the serialized body | Byte-accurate | Varies with locale/redaction/timezone per caller; recomputed on every response; cannot be compared without re-rendering. Rejected |

The validator describes **stored state**, not bytes: two callers whose representations differ (role
redaction, `Accept-Language`) see the same ETag. That is fine for `If-Match` (lost-update
prevention is about state) and is exactly why `If-None-Match`/304 is parked.

### §D-ifmatch — parsing (`varco_fastapi/varco_fastapi/router/conditional.py`)

`parse_if_match(raw: str | None) -> IfMatch` returns one of: `ABSENT`, `ANY` (`*`), or a
`frozenset[int]`. Rules: comma-separated entity-tags; a strong tag whose opaque part is a
non-negative decimal → that int; a weak tag (`W/"…"`), a non-decimal strong tag, or an unquoted
token contributes **nothing** (never matches — RFC strong comparison); `*` anywhere → `ANY`. A
header that yields an empty set is **not** "absent": the service receives `frozenset()` → 412.
`format_etag(version: int) -> str` is the one formatter. Both are pure functions with docstrings
pinning the grammar.

### §D-428 — `ConditionalRequestSettings` + a per-router override, versioned routes only

`ConditionalRequestSettings(BaseSettings)` (env prefix `VARCO_CONDITIONAL_`, `frozen=True`) with one
field, `require_if_match: bool = False`; bound by `bind_settings()` in `varco_fastapi`'s
`bootstrap()` tuple (`varco_fastapi/varco_fastapi/di.py:497-505`). `VarcoCRUDRouter.__init__`
gains `conditional: Instance[ConditionalRequestSettings] | None = None` (the `job_runner`
`is_resolvable()` pattern, `crud.py:605-614`); unresolvable → `ConditionalRequestSettings()`
(the `ErrorEnvelopeSettings()` precedent, `http.py:299`). A `ClassVar[bool | None]
_require_if_match = None` on the router overrides it per router (`None` = follow settings). The
requirement applies only where `is_versioned(service._entity_type())` (`service/base.py:1359`) — an
unversioned route has no ETag to send, so it can never 428.

✅ App-wide switch via env, per-router opt-out/in, no new verb. ❌ A second knob location
(ClassVar); accepted — the `_list_max_limit` ClassVars (`router/base.py:1121-1123`) are the
established per-router config shape. No posture finding: off is the documented default.

### §D-dto — the ETag comes from the ReadDTO's `row_version`

Handlers read `row_version` from the returned DTO (`getattr(dto, "row_version", None)`, must be
`int`) and set `response.headers["ETag"]`. Generated DTOs already carry it; hand-written ones add
`row_version: int`. List responses carry `row_version` per item and **no** `ETag` header — a client
sends `If-Match: "<item.row_version>"`. Build-time check in each CRUD handler factory: entity
versioned but response model lacks `row_version` → one `WARNING` (ETags never emitted); same and
`require_if_match` in effect → `TypeError` at router build (every write would 428 with no way to
obtain a validator — a guaranteed-broken route fails loudly).

### §D-idempotency — a 412/428 is never stored for replay

`IdempotencyMiddleware`'s fingerprint is method/path/query/body only (`idempotency.py:343`).
Today a 412 rendered by the in-app exception handler is a normal response to it and is stored
(`_execute_and_capture`, `:384-423`); the client that re-GETs and retries **with the same key and
body** but a fresh `If-Match` would get the stored 412 replayed for the key's TTL (24 h default,
`:217`). **Chosen:** a module constant `_NEVER_STORE_STATUSES = frozenset({412, 428})`; such a
response releases the reservation (`self._store.release`) and is returned unstored — exactly the
streaming/over-ceiling path (`:386-402`). A precondition failure happens before any side effect,
so re-execution is always safe.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: never store 412/428** | The corrected retry executes; same-key network retries of the failing request just re-fail (no side effect); Stripe-like "validation failures are not saved" | Narrows Plan 029's capture rule by two statuses |
| (2) Add `If-Match` to the fingerprint | Honest "different request" | The corrected retry gets 422 `IdempotencyFingerprintMismatchError` — the client must mint a new key to fix a precondition; changes the fingerprint of every existing record. Rejected |

⚠️ The 409 race-without-precondition is still stored (409 is shared with deterministic business
conflicts and cannot be told apart by status) — Open question Q2.

### §D-cors-client — the edges that need to know about the headers

- `CORSConfig` defaults (`varco_fastapi/varco_fastapi/middleware/cors.py:112-131`): `allow_headers`
  gains `If-Match` (not CORS-safelisted → the preflight must allow it); `expose_headers` gains
  `ETag` (brief 021 `:95`). Defaults move → `settings-reference` regenerates.
- Generated client (`varco_fastapi/varco_fastapi/client/base.py:521-570`): `update(pk, body, *,
  if_match: int | None = None)`, same on `patch`/`delete`; `_request()` (`:850`) gains `headers:
  Mapping[str, str] | None = None` merged into the `PreparedRequest`. The ETag header is not
  surfaced — the returned DTO carries `row_version`. `client/protocol.py:78-104` gains the keyword.

## Steps

All commands from `/home/edoardo/projects/varco`. Step 0 first; then core (1–2), SA (3–4), Beanie
(5–6) and FastAPI (7–8) — 3–8 each need 1–2; docs and gates (9–11) last. Tests precede
implementation in every pair.

0. [ ] **Preflight — resolve every ⚠️ ASSUMPTION from installed source; record in
       `/tmp/claude-1000/…/scratchpad/061-preflight.txt`.**
       (a) A1/A2: read `uv run python -c "import sqlalchemy.orm.persistence as p; print(p.__file__)"`
       → `_collect_update_commands` (how `version_id_generator=False` builds the WHERE value) and
       `_emit_delete_statements` (StaleDataError vs. warning on rowcount mismatch for versioned
       mappers); cite `file:line`.
       (b) A3: `sqlalchemy.dialects.sqlite` `supports_sane_rowcount` (the unit tests run on aiosqlite).
       (c) A4: that a `type()`-built declarative class accepts `__mapper_args__["version_id_col"]`
       pointing at the Column object in the same dict (5-line scratch script, aiosqlite).
       (d) A5: in installed `beanie`, `FindOne.replace_one(...)` and `FindOne.delete()` exist and
       return pymongo `UpdateResult`/`DeleteResult` (or `None`); if `replace_one` is absent, the
       fallback is `orm_cls.get_pymongo_collection().replace_one(filter, <the encoder Beanie's own
       Document.replace() uses>)` — record which.
       (e) `rg -n 'If-Match|ETag|etag' varco_*/varco_*` → only `idempotency.py:101`;
       `rg -n 'except StaleEntityError' varco_*/varco_*` → 0 (premise (3)).
       (f) `CHANGELOG.md`: is the top section `## [Unreleased]` or `## [4.0.0] — …` un-tagged
       (`git tag -l v4.0.0`)? Decides where Step 9's block goes.
       (g) `ls plans/062* plans/063* plans/067* plans/068*` — re-read each one's mention of this
       plan's names (`OPTIMISTIC_LOCK_ERRORS`, `expected_version`, `StaleEntityError`); rename
       here, never there, if they disagree.
       (h) Baseline gates green: `make lint`, `uv run pytest varco_core/tests varco_sa/tests
       varco_beanie/tests varco_fastapi/tests -q`.
       **Verify:** every row of §Risks marked ⚠️ has a measured answer in the file.

1. [ ] **Core tests, failing first.** `varco_core/tests/test_optimistic_concurrency.py` (new), with a
       test-local versioned in-memory repository/UoW in the `test_service.py:211-399` style whose
       `save()` raises `StaleEntityError` on demand:
       (a) `is_versioned`: `RowVersionMixin` subclass, `VersionedDomainModel`, `TenantVersionedDomainModel`
       → `True`; `DomainModel`, `AuditedDomainModel` → `False`; a class declaring `row_version` with
       `init=True` → `False`.
       (b) `error_code_for()`: `ConcurrencyConflictError` → 409, `PreconditionFailedError` → 412,
       `PreconditionRequiredError` → 428; `error_params()` == `{"entity": …}` (no versions);
       `OPTIMISTIC_LOCK_ERRORS == (StaleEntityError, ConcurrencyConflictError)`;
       `isinstance(PreconditionFailedError(...), ServiceConflictError)`.
       (c) `AsyncService.update`: `expected_version=1` on v1 → saved; `=2` on v1 →
       `PreconditionFailedError` and `repo.save` **not called**; `=frozenset({1, 5})` → saved;
       `=frozenset()` → 412; missing pk with `expected_version=1` → `ServiceNotFoundError` (404
       wins); denying authorizer → `ServiceAuthorizationError` (403 wins); unversioned entity +
       `expected_version=1` → 412; repo raises `StaleEntityError` → `ConcurrencyConflictError`
       without `expected_version`, `PreconditionFailedError` with it; no event published and no
       `_after_update` on either failure.
       (d) `patch` forwards to `update` with the keyword; `delete` mirrors (c).
       (e) `SoftDeleteService.delete(expected_version=…)` mirrors (c); `restore()` translates.
       (f) `CacheServiceMixin` and `TracingServiceMixin` forward `expected_version` (spy on `super()`).
       (g) `StaleEntityError(cls, 1)` constructs (`actual_version` optional); its `str()` omits "found".
       **Verify:** `uv run pytest varco_core/tests/test_optimistic_concurrency.py -q` → red (import errors).

2. [ ] **Core implementation.** (i) `varco_core/varco_core/model.py` — `RowVersionMixin`
       (`row_version: int = field(default=0, init=False, repr=False, compare=False)`, full
       docstring + DESIGN pointing at §D-optin) and `is_versioned()`; `VersionedDomainModel` untouched.
       (ii) `varco_core/varco_core/exception/concurrency.py` (new, §D-exceptions) with the three
       `register_error_code()` calls; `exception/repository.py` `actual_version: int | None = None`;
       fix the stale "wrapped at the service boundary" docstrings in `exception/service.py:197-214`
       to name `ConcurrencyConflictError`; tree + `__all__` in `exception/__init__.py:1-80`.
       (iii) `service/base.py` — keyword on `update`/`patch`/`delete`, `_check_expected_version()`
       (docstring: order 404 → 403 → 412, empty set never matches), `StaleEntityError` translation
       around the `save()`/`delete()` calls; Raises sections updated. (iv) `service/soft_delete.py`,
       `cache/mixin.py`, `observability/mixin.py`, `service/types.py` per §D-api. (v)
       `varco_core/varco_core/__init__.py` — the PEP 562 lazy map (`:731` pattern), the
       `TYPE_CHECKING` block (`:251` pattern) and `__all__` (`:966` pattern) gain `RowVersionMixin`,
       `is_versioned`, the three exceptions, `OPTIMISTIC_LOCK_ERRORS`. (vi) the `AsyncRepository.save()`
       /`delete()` docstrings (`varco_core/varco_core/repository.py`) state the versioned contract.
       **Verify:** Step 1 green; `uv run pytest varco_core/tests -q` green (incl. `test_mapper.py`,
       `test_model.py`); `uv run python scripts/import_budget.py --check --warn-only` no new breach.

3. [ ] **SA tests, failing first.** `varco_sa/tests/test_sa_optimistic_lock.py` (new, aiosqlite, two
       independent `AsyncSession`s on one file-backed or shared-cache engine so two transactions
       really interleave): (a) factory: versioned ORM class's mapper has `version_id_col` = the
       `row_version` column and `version_id_generator is False`; unversioned has none. (b) **the
       lost-update race** — session A and B each `find_by_id` v1; A saves + commits (v2); B saves →
       `StaleEntityError` (red today: B silently writes v2). (c) delete race: A updates + commits, B
       deletes its v1 copy → `StaleEntityError`, row survives. (d) `save_many` with one stale row →
       `StaleEntityError`. (e) `update_many_by_query` bumps `row_version` by 1 on every matched row;
       an `update` dict naming `row_version` → `ValueError`. (f) `delete_many` with one stale entity
       → `StaleEntityError`, transaction rolled back, all rows survive. (g) unversioned entity: (b)'s
       shape still last-writer-wins (behaviour unchanged). (h) the existing
       `test_sa_repository.py:233-250` same-session case still raises. Plus
       `varco_sa/tests/test_sa_optimistic_lock_integration.py` (`pytestmark =
       pytest.mark.integration`, `postgres_url` session fixture, `uuid4().hex[:8]` table namespace):
       (b), (c), (f) against Postgres under READ COMMITTED. **Verify:** unit file red at (a)(b)(c)(e)(f).

4. [ ] **SA implementation.** `varco_sa/varco_sa/factory.py:320-326` — `__mapper_args__` for
       `is_versioned(domain_cls)` (DESIGN block: §D-atomic, `version_id_generator=False` reason);
       `varco_sa/varco_sa/repository.py` — `StaleDataError` → `StaleEntityError` translation in
       `save()` (`:207-208`), `delete()` (`:225-228`), `save_many()` (`:462-463`); versioned
       `update_many_by_query()` (`:561`) and `delete_many()` (`:507-513`) per §D-atomic, docstrings'
       Edge cases updated (bulk now versioned-aware). **Verify:** `uv run pytest
       varco_sa/tests/test_sa_optimistic_lock.py varco_sa/tests -q` green; `uv run pytest
       varco_sa/tests/test_sa_optimistic_lock_integration.py -m integration -q` green.

5. [ ] **Beanie tests, failing first.** `varco_beanie/tests/test_beanie_optimistic_lock_integration.py`
       (new, `pytestmark = pytest.mark.integration`, `mongo_url`, per-test database name — no Mongo
       mock exists in this repo): two `find_by_id` calls yield two independent `Document` copies →
       the same (b)–(g) matrix as Step 3 (Beanie has no transaction yet, so (f) asserts the
       *documented* partial outcome: matching rows deleted, `StaleEntityError` raised — 063 makes it
       atomic). Plus one Docker-free unit test in `varco_beanie/tests/test_beanie_factory.py` style
       asserting the versioned/unversioned branch selection (`is_versioned`) with the write calls
       patched. **Verify:** `uv run pytest varco_beanie/tests/test_beanie_optimistic_lock_integration.py
       -m integration -q` → red at (b)(c)(e)(f).

6. [ ] **Beanie implementation.** `varco_beanie/varco_beanie/repository.py` — `save()` (`:179-183`),
       `delete()` (`:185-208`), `save_many()` (`:355-362`), `delete_many()` (`:366-401`),
       `update_many_by_query()` (`:403-451`) per §D-atomic, using Step 0(d)'s recorded API; a
       `# 063: pass session= here` comment at each conditional call. **Verify:** Step 5 green;
       `uv run pytest varco_beanie/tests -q` green; `make integration-test PKG=varco_beanie` green.

7. [ ] **FastAPI tests, failing first.** `varco_fastapi/tests/test_conditional_requests.py` (new;
       `httpx.AsyncClient` over a `create_varco_app(...)` with a versioned in-memory service, the
       `in_memory_modules()`/`di_overrides` recipe; `monkeypatch.setenv` per test, never autouse):
       (a) `parse_if_match`/`format_etag` table: `"3"`, `"3", "5"`, `*`, `W/"3"` (→ empty),
       `"abc"` (→ empty), `3` unquoted (→ empty), `None` (→ ABSENT). (b) GET → 200 + `ETag: "1"`;
       POST → 201 + `ETag: "1"`; PUT `If-Match: "1"` → 200 + `ETag: "2"`; PUT `If-Match: "1"` again
       → 412 with `code == "VARCO_PRECONDITION_001"` and no version/ETag anywhere in body or
       headers; PUT no header → 200 (default off); PATCH and DELETE mirror; DELETE 204 has no ETag.
       (c) `VARCO_CONDITIONAL_REQUIRE_IF_MATCH=true`: PUT/PATCH/DELETE without header → 428
       `VARCO_PRECONDITION_002`; `If-Match: *` → 200; router `_require_if_match = False` → 200;
       unversioned router → never 428, no ETag, `If-Match: "1"` → 412. (d) `W/"1"` → 412. (e) list
       items carry `row_version`, response has no `ETag`. (f) build-time: versioned entity + ReadDTO
       without `row_version` → one WARNING (`caplog`); with require on → `TypeError`. (g) OpenAPI:
       PUT/PATCH/DELETE declare an `If-Match` header parameter. (h) `IdempotencyMiddleware`: PUT with
       key K + stale `If-Match` → 412; same K + same body + fresh `If-Match` → 200 and
       `Idempotency-Replayed` absent. (i) `CORSConfig()` defaults contain `If-Match` / `ETag`.
       (j) client: `await client.update(pk, body, if_match=2)` sends `If-Match: "2"` (recording
       transport); without it sends none. (k) a 409 race (service stubbed to raise
       `ConcurrencyConflictError`) renders 409 `VARCO_CONCURRENCY_001` through both
       `ErrorMiddleware` and `add_exception_handlers`. **Verify:** red.

8. [ ] **FastAPI implementation.** (i) `varco_fastapi/varco_fastapi/router/conditional.py` (new:
       `ConditionalRequestSettings`, `IfMatch`, `parse_if_match`, `format_etag`,
       `resolve_precondition(raw, *, required) -> int-set | None` raising `PreconditionRequiredError`;
       module DESIGN block = §D-etag/§D-ifmatch). (ii) `router/base.py:898-1090` — create/read/
       update/patch/delete factories: `response: Response` + `if_match: str | None =
       Header(default=None, alias="If-Match")` on update/patch/delete, ETag stamping, build-time
       check (§D-dto), both `auth_dep` branches. (iii) `router/crud.py:252-321` — `conditional=`
       constructor kwarg + `_require_if_match` ClassVar, passed through `_make_http_handler`
       (`:625-632`) and the base-class path (`router/base.py:827-828`). (iv) `exceptions.py` — a
       `PreconditionRequiredError` handler. (v) `middleware/idempotency.py` — `_NEVER_STORE_STATUSES`
       (§D-idempotency, DESIGN block). (vi) `middleware/cors.py` defaults. (vii) `client/base.py`
       + `client/protocol.py` (§D-cors-client). (viii) `di.py:497-505` — `ConditionalRequestSettings`
       in the `bind_settings` tuple. (ix) `varco_fastapi/__init__.py` exports
       `ConditionalRequestSettings`. **Verify:** Step 7 green; `uv run pytest varco_fastapi/tests
       examples/00-full-stack-post-api -q` green.

9. [ ] **Docs (same change, never a follow-up).** (a) `technical_docs/features/optimistic-concurrency.md`
       (new): what is versioned and how to opt in; the atomic guarantee per backend; the §D-status
       table; ETag format and why strong; `If-Match` grammar; 428 knob; list/streaming/ingest
       behaviour (067/068); idempotency interplay; 062 handoff (never retried; 40001 under RR/
       SERIALIZABLE); app-owned Alembic recipe for adopting `RowVersionMixin` on an existing table
       (`op.add_column(..., sa.Column("row_version", sa.Integer(), nullable=False,
       server_default="1"))`; Mongo: documents missing the field — see Edge cases); **Pitfalls**
       table (nginx-weakened ETags ⚠️, hand-written ReadDTO without `row_version`, custom service
       not translating `StaleEntityError`, bulk ops before 4.0 did not bump, replay has no ETag,
       client copying an ETag across resources). Add to `mkdocs.yml` nav. (b) `README.md` — an
       "Optimistic concurrency & conditional requests" section (opt-in snippet, curl
       GET/PUT/412/428, client `if_match=`), each snippet naming the test that runs it; TOC entry.
       (c) `ARCHITECTURE.md` — exception hierarchy (+3, `OPTIMISTIC_LOCK_ERRORS`), domain-model
       hierarchy (+`RowVersionMixin`), Service Layer note on `expected_version=`. (d) `CLAUDE.md` —
       one rule line under "Service layer" (*"`StaleEntityError` is translated by `AsyncService`;
       an optimistic-lock conflict (`OPTIMISTIC_LOCK_ERRORS`) is never auto-retried —
       `technical_docs/features/optimistic-concurrency.md`"*) and one decision-tree branch
       (*"Prevent lost updates / conditional writes? → `RowVersionMixin` + `expected_version=` /
       `If-Match`, never a hand-rolled version check"*). (e) `CHANGELOG.md` (heading per Step 0(f)) —
       `### BREAKING — optimistic concurrency and conditional requests (Plan 061, ACID-2)` +
       Added/Changed/Fixed bullets (Fixed: `StaleEntityError` no longer a 500; lost update on
       `VersionedDomainModel`). (f) `technical_docs/migrations/3-to-4.md` — a section: (1)
       `VersionedDomainModel` users: concurrent writes that used to silently overwrite now 409; (2)
       mixins overriding `update`/`patch`/`delete` must accept `expected_version` (or `**kwargs`);
       (3) `StaleEntityError(…, actual_version)` is optional — `except` sites unaffected; (4)
       `update_many_by_query`/`delete_many` on versioned entities now bump/check versions; (5) CORS
       defaults changed — an app overriding `allow_headers`/`expose_headers` must add `If-Match`/
       `ETag` itself; (6) to opt in: mixin + app-owned migration. **Verify:** `rg -n
       'wrapped at the service boundary' varco_core/` → 0; `rg -n 'optimistic-concurrency.md'
       CLAUDE.md README.md ARCHITECTURE.md mkdocs.yml technical_docs/migrations/3-to-4.md` → ≥ 5.

10. [ ] **Snapshot gates — regenerate and commit, in this order.** `uv run python scripts/api_surface.py`
        (new names in `varco_core`/`varco_fastapi` `__all__`; the client/handler changes are
        methods/closures, not recorded); `make settings-reference` (`ConditionalRequestSettings`,
        `CORSConfig` defaults); `make di-catalogue` (the new `bind_settings` binding); `make
        entry-points-check` (expected unchanged — no new `bind_*/enable_*/mount_*/install_*`;
        regenerate only if it moves and say why in the commit); `make asyncapi-check` (expected
        unchanged). **Verify:** `make lint` (no `PKG`) green — every `--check` passes on the
        committed snapshots.

11. [ ] **Full verification.** `make lint`, `make type-check` (strict — new code carries no
        `type: ignore` beyond the established `row_version` attr pattern in `mapper.py`; record the
        M1 delta), `make test`, `make integration-test PKG=varco_sa`, `make integration-test
        PKG=varco_beanie`, `make integration-test PKG=varco_fastapi`. Tick this plan's boxes; do
        **not** edit `BACKLOG.md` (the 4.0.0 rollup owns it).

## Edge cases

- `If-Match: "3", W/"4"` on v4 → 412 (the weak tag never matches; the strong one does not match).
- `If-Match: "3", *` → treated as `*` → passes.
- `If-Match: ""` / `If-Match: ,` → empty set → 412.
- `If-Match` on a missing pk → 404 (not 412); unauthorized → 403 (not 412).
- PATCH with an empty body and a matching `If-Match` → save runs → `row_version` +1 (every save
  bumps; no "no-op detection") → new ETag. Documented.
- `If-Match` on an unversioned entity → 412; `*` on an unversioned entity → 2xx.
- `require_if_match` on + unversioned router → never 428.
- Soft-deleted entity: `If-Match` on PUT → 404 (`_check_entity` runs before the precondition).
- `expected_version=1` from non-HTTP code on a versioned entity at v1 that is concurrently written
  between load and flush → `PreconditionFailedError` (412) — the caller asked for a precondition.
- A **custom service method** calling `repo.save()` on a versioned entity gets the raw
  `StaleEntityError` (a `RepositoryException`) → 500 unless it translates; feature-page pitfall.
- Mongo documents written before the mixin was adopted have no `row_version` field: the Beanie
  model's default (`0`) is read, and the conditional filter `{"row_version": 0}` does **not** match
  a missing field — first write after adoption raises `StaleEntityError`. The feature page gives
  the one-line backfill (`updateMany({row_version: {$exists: false}}, {$set: {row_version: 1}})`),
  and the migration guide says "backfill before enabling". ⚠️ Step 5 adds a test pinning this so the
  docs cannot drift.
- SA table adopted with `server_default="1"`: existing rows read as 1, ETag `"1"` — consistent.
- `delete_many` on versioned entities where one row was already deleted by someone else →
  `StaleEntityError` (indistinguishable from stale); SA rolls back the batch, Beanie (no
  transaction until 063) has deleted the matching rows.
- Idempotent replay of a successful PUT → body has the then-current `row_version`, no `ETag` header.
- `CacheServiceMixin.get` serving a cached DTO → its `row_version` (and ETag) are those of the
  cached state; the cache is evicted on every update (`cache/mixin.py:495-497`), so a stale cached
  ETag can only exist inside the existing cache-staleness window, and an `If-Match` built from it
  fails safe (412).

## Verification

```bash
uv run pytest varco_core/tests/test_optimistic_concurrency.py -q
uv run pytest varco_sa/tests/test_sa_optimistic_lock.py -q
uv run pytest varco_sa/tests/test_sa_optimistic_lock_integration.py -m integration -q
uv run pytest varco_beanie/tests/test_beanie_optimistic_lock_integration.py -m integration -q
uv run pytest varco_fastapi/tests/test_conditional_requests.py -q
make test
make integration-test PKG=varco_sa && make integration-test PKG=varco_beanie && make integration-test PKG=varco_fastapi
make lint          # ruff + format + api-check + settings-check + di-catalogue-check + entry-points-check + asyncapi-check + import-budget (warn-only)
make type-check
rg -n 'If-Match' varco_fastapi/varco_fastapi/router/   # handlers wired
rg -n 'OPTIMISTIC_LOCK_ERRORS' varco_core/varco_core/  # exported for 062
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION A1 — with `version_id_generator=False`, SA's UPDATE predicate uses the **loaded** `row_version` while writing the mapper-assigned new value | The predicate compares against what this transaction read | Step 0(a) reads `sqlalchemy/orm/persistence.py`; Step 3(b) is the behavioural proof; fallback: default generator + drop the mapper's `+1` for SA-versioned classes |
| ⚠️ ASSUMPTION A2 — a versioned DELETE with zero matched rows raises `StaleDataError` (not just a warning) | Stale DELETE never succeeds silently | Step 0(a) + Step 3(c); fallback: explicit Core `DELETE … WHERE id AND row_version` + rowcount check inside `delete()` |
| ⚠️ ASSUMPTION A3 — aiosqlite reports sane UPDATE/DELETE rowcounts | The Docker-free SA tests prove the race | Step 0(b); if not, (b)/(c) move to the Postgres integration file only |
| ⚠️ ASSUMPTION A4 — `type()`-built declarative classes accept `version_id_col` pointing at a Column in the same dict | Factory builds versioned mappers | Step 0(c) scratch script; fallback: assign `__mapper_args__` as a `declared_attr`/lambda returning `cls.__table__.c.row_version` |
| ⚠️ ASSUMPTION A5 — installed Beanie offers `FindOne.replace_one()`/`.delete()` returning pymongo results with `matched_count`/`deleted_count` (brief 021 does not cover it) | Beanie stale writes are detected atomically | Step 0(d) reads the source; fallback recorded there (collection-level `replace_one` with Beanie's own encoder) |
| ⚠️ ASSUMPTION — nginx's gzip module weakens strong ETags (`W/`), so browser clients behind it would echo weak tags and always get 412 (not in either brief; unverified) | Strong comparison stays RFC-correct (brief 021 `:89`) | Feature-page pitfall: build `If-Match` from the body's `row_version`, or disable ETag weakening at the proxy; no server-side leniency |
| ⚠️ ASSUMPTION — RFC 9110 §13.2.1's "ignore preconditions when the unconditional response would not be 2xx/412" is quoted from memory, not from brief 021 | 404/403 precede 412 | The ordering is also independently required by varco's own no-existence-oracle rule (`exception/service.py:84-87`); Step 0 reads RFC 9110 §13.2.1 and pastes the sentence into the feature page |
| **BREAKING: `VersionedDomainModel` apps that relied (unknowingly) on last-writer-wins now see 409/`StaleEntityError` under concurrency** | Lost updates are impossible on versioned entities | Migration guide (1); CHANGELOG BREAKING block; the 409 is the row's intended behaviour |
| **BREAKING: out-of-tree mixins overriding `update`/`patch`/`delete` without `expected_version`** | Unconditional traffic keeps working | Router passes the keyword only when `If-Match` was sent; migration guide (2) |
| Plan 062 retries a conflict anyway (e.g. retries on `ServiceConflictError` broadly) | No optimistic-lock conflict is ever auto-retried | `OPTIMISTIC_LOCK_ERRORS` exported and named in 062's handoff; Step 0(g) cross-reads 062 |
| Plan 062's RR/SERIALIZABLE boundary turns our races into 40001 | Retry re-runs the whole use case incl. the precondition | Stated in the neighbours table; 062 owns the test |
| `IdempotencyMiddleware` change narrows Plan 029's capture rule | A precondition failure has no side effect, so not storing it is always safe | Step 7(h); DESIGN block cites §D-D1-replay |
| Hand-written ReadDTOs lacking `row_version` silently emit no ETag | A versioned route either emits ETags or says loudly why not | Build-time WARNING; `TypeError` when 428 is required (Step 7(f)) |
| Bulk `update_many_by_query` now changes `row_version` | Every write path that changes a versioned row bumps its version | Step 3(e)/5; migration guide (4) |
| Beanie `delete_many` partial deletion on a stale batch until 063 | Documented, never silent (`StaleEntityError` raised) | Edge cases; 063 handoff |
| Snapshot gates drift (settings/di-catalogue/api) | Every `--check` green on the committed tree | Step 10 in order, `make lint` no-`PKG` |
| M1 (`type: ignore` count) rises | Directional only | Step 11 records the delta |

## Open questions

- **Q1 — `If-Match` on `POST` (create).** Ignored here (§Non-goals). Should the create handler
  instead 412 on any non-`*` `If-Match` to honour RFC 9110's "MUST evaluate"?
- **Q2 — the 409 race-without-precondition is still stored by `IdempotencyMiddleware`.** A same-key
  retry replays the 409 for the TTL. Fix options: a private marker header set by the error renderer
  for `ConcurrencyConflictError` and stripped before send, or accept and document. Not decided here.
- **Q3 — `RowVersionMixin` naming.** Chosen to match the existing `row_version` field; the user
  may prefer `OptimisticLockMixin` (Rails/EF vocabulary).
- **Q4 — should `require_if_match` default to `True` in 4.0** (a security-ish flip like Plan
  053–057's)? Chosen `False`: it breaks every existing client of every versioned route at once, and
  there is no posture precedent for it.
