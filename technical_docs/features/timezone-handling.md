# Timezones — resolution, DST-safe scheduling, and the query-layer contract

Plan 011 (T1, T2, T3, D-9). Closes: "no per-request timezone, no
documented DST policy for scheduled jobs, and an accidental (not declared)
timezone assumption in the query layer's datetime coercion."

**Off by default.** `TimezoneSettings.enabled=False` — no resolution, no
`X-Timezone`/`?tz=` read, `current_timezone()` is `None`,
`datetime.now(timezone.utc)` everywhere is unchanged. T2's three `Job`
columns are `None`/`0`-defaulted — `run_at_tz IS NULL` is byte-identical to
pre-Plan-011 in every respect. T3's `DatetimeCoercionPolicy(assume="naive")`
default reproduces `coerce_datetime()`'s exact pre-Plan-011 output.

## The rule that never changes: store UTC, render local

varco does **not** change what it stores. Every timestamp is still written
as aware-UTC (`datetime.now(timezone.utc)`). T1 is a *rendering and
interpretation* layer only — it never touches persistence.

```python
from varco_core.tz.resolve import to_user_tz, now_local

to_user_tz(
    order.created_at
)  # UTC-aware in -> ambient-timezone-aware out (identity if none resolved)
now_local()  # datetime.now() in the ambient timezone, else aware-UTC
```

## T1 — per-request timezone resolution (the five-source chain)

```
?tz= (query_param) -> X-Timezone (header) -> user_profile -> tenant_default -> fallback
```

```python
resolved = await resolve_timezone(
    query_param=request.query_params.get("tz"),
    header=request.headers.get("X-Timezone"),
    user_profile_zoneinfo=auth_ctx_zoneinfo_claim,
    tenant_id=current_tenant(),
    tenant_defaults_provider=provider,
    default_timezone="UTC",
)
```

Every candidate is validated with `validate_iana_zone()` *before* entering
the candidate list — a garbage zone name logs one WARNING and falls through
to the next source instead of raising. `X-Timezone` (not a made-up header
name) is brief 004 §B3's exact recommendation.

`TimezoneSettings.default_timezone` is validated **at startup**
(`model_validator`) — a missing tzdata database on a slim image
(`python:*-slim`, distroless, Alpine) raises a legible `ValueError` naming
`pip install tzdata` there, never as a per-request surprise.

### The `tz` optional extra

`zoneinfo` (stdlib since 3.9) needs a tzdata *database*, which the CPython
standard library does not ship on every platform — glibc-based Linux images
usually have `/usr/share/zoneinfo`; `slim`/distroless/Alpine images
frequently do not.

```bash
pip install "varco-core[tz]"   # pulls in the `tzdata` PyPI package (pure-Python IANA data)
```

This is the **only** new dependency this plan introduces anywhere, and it
is optional — `varco_core.tz`'s contracts import only stdlib `zoneinfo`;
the extra just makes sure a zone actually resolves in a minimal container.

## T2 — DST-safe one-shot scheduling

### Storage: three additive `Job` columns, `run_at` is materialized, not replaced

```python
run_at_wall: datetime | None = None  # naive local wall-clock, no tzinfo — the INTENT
run_at_tz: str | None = None  # IANA zone name, e.g. "America/New_York"
run_at_fold: int = 0  # PEP 495 fold — disambiguates an overlap
```

`run_at: datetime | None` — the pre-existing field — keeps its **exact
current semantics**: the materialized UTC instant the claim predicate
compares against the store's `now()`. Nothing about `claim_next`,
`try_claim`, `JobPoller`, or any store's SQL changes.

> **`run_at` is materialized, not replaced.** `(run_at_wall, run_at_tz,
> run_at_fold)` is the *intent*; `run_at` is the *materialization* of that
> intent under whatever tzdata was current when it was computed. A row
> with `run_at_tz IS NULL` is byte-identical to today in every respect —
> every existing row already satisfies this.

Consequences: in-flight jobs are unaffected; both rolling-deploy directions
are safe (an old pod reading a new pod's zoned row still claims it at the
right instant, simply without the ability to re-materialize — unlike Plan
010's cache envelope, T2 has no two-step-deploy requirement); no new index
(`ix_varco_jobs_claim` is unchanged — `SAJobStore.list_pending_zoned()`
rides the existing `run_at` column with a targeted `WHERE run_at_tz IS NOT
NULL` predicate instead).

`SAJobStore`, `BeanieJobStore`, and the in-memory store shipped in
`varco_fastapi.job.store` all set `supports_zoned_schedules = True` and
persist all three columns/fields.

### Gap and overlap policy (D-8)

```python
from varco_core.tz.schedule import GapPolicy, OverlapPolicy, resolve_zoned

instant = resolve_zoned(
    wall,
    zone,
    fold=0,
    gap=GapPolicy.NEXT_VALID,  # default
    overlap=OverlapPolicy.FIRST,  # default
).astimezone(timezone.utc)
```

No `dateutil` dependency — `datetime_exists()`/`datetime_ambiguous()` are
~8 lines each over stdlib `zoneinfo`: a time is *ambiguous* iff
`utcoffset(fold=0) != utcoffset(fold=1)`; a time is *nonexistent* iff
round-tripping it through UTC and back does not reproduce it.

| Situation | Policy | Default | Behaviour |
|---|---|---|---|
| Fall-back overlap (a wall time occurs twice) | `OverlapPolicy.FIRST` / `.LAST` | `FIRST` (`fold=0`) | Run **once**, at the first occurrence. Contrast Quartz, which fires both — firing a one-shot job twice is a duplicate side effect. |
| Spring-forward gap (a wall time doesn't exist) | `GapPolicy.NEXT_VALID` / `.PREVIOUS_VALID` / `.SKIP` / `.ERROR` | **`NEXT_VALID`** | Rolls forward to the first valid instant after the gap, logs one WARNING naming the job/zone/both times. |

**⚠️ Deliberate deviation from brief 004.** Brief 004's Librarian's note
recommends `"skip"` as the *gap* default, matching Quartz/Kubernetes
CronJob. That recommendation is correct for a **recurring** occurrence —
skipping one 02:30 firing out of 365 is a rounding error, the job runs
again tomorrow. For T2's **one-shot** `Job` (D-7 — T2 covers one-shot only,
recurring/RRULE is a Non-goal), "skip" means the job is *never* executed
and *never fails*: it sits PENDING forever or vanishes — silent data loss,
the exact class of defect `OutboxRelay(max_attempts=...)` refuses to
construct without a `dlq=` specifically to avoid. So varco defaults to
`NEXT_VALID` instead. `GapPolicy.SKIP` is available and, when chosen,
transitions the job to a **terminal** state with a named
`ScheduleGapError` rather than leaving it pending — skipping is allowed,
never silently. `GapPolicy.ERROR` refuses at enqueue time.

### `ScheduleRematerializer` — opt-in recompute-on-read

`run_at` is written **once**, at enqueue, under whichever pod's tzdata was
current then. Because tzdata itself changes (a government moves a DST
transition date years after the fact), the recommended discipline (brief
004 §A3) is to recompute on read:

```python
from varco_core.job.reschedule import ScheduleRematerializer

rematerializer = ScheduleRematerializer(store, interval=300.0, horizon=timedelta(hours=48))
await rematerializer.start()  # spawns a background asyncio.Task
```

Sweeps pending zoned jobs inside `horizon` of now, recomputes `run_at` from
`(run_at_wall, run_at_tz, run_at_fold)` under **current** tzdata, and writes
back **only when the value actually changed**, fenced with
`store.save(expected_epoch=...)` (Plan 005 Phase 4's lease fencing) — a job
claimed mid-sweep raises `StaleLeaseError`, which the sweeper logs at DEBUG
and skips rather than raising, so a worker executing the job right now
never has its schedule rewritten underneath it.

**Default `interval=0.0` = not started** — `start()` spawns no task at all,
byte-identical to not using this component (RD-1). Operator note: pin
`tzdata` in any image running the sweeper so a rematerialization decision
is reproducible across pods rather than depending on whichever OS tzdata
package happens to be installed.

### RD-5 — a store must declare support; enqueue refuses otherwise

```python
class AbstractJobStore(ABC):
    supports_zoned_schedules: ClassVar[bool] = False  # opt-in per store
```

`AbstractJobRunner._prepare_zoned_job(job, store, run_at_wall=..., tz=...,
fold=..., gap=..., overlap=...)` is a concrete, reusable **static helper**
on the ABC — it validates the `run_at=` / `run_at_wall=`+`tz=` mutual
exclusion, checks `store.supports_zoned_schedules`, calls `resolve_zoned()`,
and returns the job with `run_at`/`run_at_wall`/`run_at_tz`/`run_at_fold`
populated via `dataclasses.replace()`. It raises `ValueError` naming the
store class when `tz=` is supplied to a store that hasn't opted in.

This mirrors the exact failure mode Plan 005 Phase 4 hit with
`try_claim(owner_id=, lease_ttl=)`: an out-of-tree store that maps `Job`
fields onto fixed columns explicitly would otherwise **silently drop**
`run_at_wall`/`run_at_tz` — the job still fires at the right instant
(`run_at` was materialized at enqueue) but re-materialization silently
no-ops, so the DST safety the caller asked for quietly isn't there. Failing
closed turns that into a named, loud error instead.

**Implementation status — `_prepare_zoned_job()` is now called by the
shipped `varco_fastapi.job.runner.JobRunner`** (Plan 011 drift-fix pass).
`JobRunner.enqueue(self, job: Job, coro, *, run_at=, delay=, run_at_wall=,
tz=, fold=, gap=, overlap=)` calls `AbstractJobRunner._prepare_zoned_job()`
before `self._store.save(job)`, so the RD-5 guard is enforced on the
standard submission path — a zoned schedule targeting a store that hasn't
opted into `supports_zoned_schedules` raises `ValueError` naming the store
class, and `coro` is closed first so no coroutine is leaked. Constructing a
`Job` with `run_at_wall`/`run_at_tz`/`run_at_fold` set directly and calling
`store.save(job)` still works (and still bypasses the guard, by
construction — nothing requires going through `enqueue()`), but is no
longer the only way to reach T2 through the shipped runner. Any service
adding its own zoned-`enqueue()` convenience method should still call
`AbstractJobRunner._prepare_zoned_job()` itself rather than reimplementing
the gap/overlap/RD-5 logic.

## T3 — the query layer's datetime coercion contract (D-10)

`DatetimeCoercionPolicy` (`varco_core.query.policy`):

```python
assume: Literal["naive", "utc", "context"] = "naive"
log_naive: bool = True
```

⚠️ **Correction to the backlog.** The backlog claimed
`?created_at__gte=2026-01-01` "silently means UTC today." It does not:
`coerce_datetime()` returns `datetime.fromisoformat(value)` **unchanged**
for a naive input — a naive string produces a naive `datetime`, with no
`tzinfo` at all. The UTC-ish behavior users observe is one layer lower —
whatever the *database session* decides (Postgres coerces a naive literal
against a `TIMESTAMPTZ` column using the session `TimeZone`, usually but not
necessarily UTC).

| `assume=` | Behaviour | When |
|---|---|---|
| `"naive"` (**default**) | Returned exactly as `fromisoformat` produced it — byte-identical to pre-Plan-011 | Always safe; the only choice that can't break an existing naive-column query |
| `"utc"` (**recommended**) | Attaches `tzinfo=UTC` | Turn this on if your timestamp columns are `TIMESTAMPTZ` |
| `"context"` (opt-in) | Reads `current_timezone()`; falls back to `"utc"` + one DEBUG log if no ambient timezone is resolved | Only if you deliberately want naive filter bounds interpreted in the caller's own timezone — brief 004 §B1: *"Varco would be pioneering this if implemented"*, no mainstream framework does this by default |

**Why the default isn't the recommendation.** Attaching `tzinfo=UTC` is
*not* a no-op: `asyncpg` **rejects** an aware `datetime` against a
`TIMESTAMP WITHOUT TIME ZONE` column. Making `"utc"` the default would turn
a working query into a runtime error on upgrade for every app with a naive
timestamp column — exactly the silent-upgrade breakage this repo's
default-off convention exists to prevent. `"naive"` stays default; `"utc"`
is what every doc, including this one, tells you to turn on.

### Two invariants that hold under every policy

1. **An explicit offset always wins.** `2026-01-01T00:00:00Z` or
   `...-05:00` is used verbatim; no policy is ever applied to an
   already-aware value. This is the portable, always-correct client
   behaviour — recommend it above everything else in this section.
2. **Convert the bound, never the column.** varco emits `WHERE created_at >
   <utc bound>`; it never generates `WHERE created_at AT TIME ZONE 'UTC' >
   ...` — the latter defeats the index (brief 004 §B2). Nothing in
   `coerce_datetime()`/`ASTTypeCoercion` ever touches a column reference; a
   future contributor "fixing" a timezone bug by moving the conversion into
   SQL would be violating this invariant, not restoring correct behaviour.

### The date-only trap

`2026-01-01` is midnight at the start of that day (under whichever policy
is active) — `__lte=2026-01-01` therefore excludes almost all of January
1st. The single most common datetime-filter bug in any API; state it
explicitly to callers building filter UIs.

⚠️ **Implementation status — `policy=` is only wired to the free function,
not the AST visitor.** `coerce_datetime(value, *, policy=...)` honours
`DatetimeCoercionPolicy` correctly and is fully tested. `ASTTypeCoercion`
(`varco_core.query.visitor.type_coercion`) — the visitor
`QueryTransformer`/`registry_from_sa_model` actually drive — has **no**
`policy=` constructor parameter, and its internal `_coerce_value()` calls
`field_info.coercer(value)` with no `policy=` argument at all. Concretely:
registering `datetime: coerce_datetime` (the module's own
`default_field_coercions` default) means every datetime field coerced
through the AST path is called as `coerce_datetime(value)` — always
`policy=None`, i.e. always `"naive"` behaviour — **regardless of any
`DatetimeCoercionPolicy` you construct**. To apply `"utc"`/`"context"`
today, call `coerce_datetime(value, policy=my_policy)` directly outside the
AST pipeline, or register a field-specific coercer
(`functools.partial(coerce_datetime, policy=my_policy)`) via
`TypeCoercionRegistry.register_field()`. Wiring `policy=` through
`ASTTypeCoercion`/`QueryTransformer` end-to-end is unimplemented.

## D-9 — RFC 9557 is an output format only

```python
from varco_core.tz.format import format_rfc9557

format_rfc9557(instant, zone)  # "2026-03-08T09:00:00-05:00[America/New_York]"
```

No production-ready Python RFC 9557/IXDTF *parser* exists (brief 004 §A4 +
Evidence Gap 1 — `whenever` shows the shape in examples without documenting
compliance; neither `dateutil.isoparse` nor stdlib `datetime` support the
bracket suffix). So: storage is exclusively the three `Job` columns above,
full stop; `format_rfc9557()` is a ~10-line f-string over `isoformat()` for
API responses and logs; and `coerce_datetime()` **rejects** an input ending
in a bracketed zone suffix with a `ValueError` naming the two supported
inputs (an RFC 3339 offset, or a separate `tz=` field) rather than
attempting to parse it. When a production-ready parser lands, it becomes an
additive branch inside the coercer — never a storage-model change, which is
the entire point of keeping storage independent of wire format.

## Wiring: `LocalizationMiddleware` and its ordering hazard (RD-3)

I2 and T1 share **one** middleware, `varco_fastapi.middleware.localization.
LocalizationMiddleware`, resolving locale and/or timezone in a single ASGI
pass with two independent enable flags — not two middlewares with two
`ContextVar` tokens whose reset order must be right.

### Actual request order, verified against `create_varco_app`

`create_varco_app`'s `add_middleware()` calls execute in **reverse** — the
*last* call becomes the *outermost* layer, dispatching first. Reading the
call sequence in `varco_fastapi/varco_fastapi/app.py` and reversing it, the
real request-flow order is:

```
CORS → [extra_middleware=, e.g. an app-supplied TenantResolutionMiddleware]
     → ErrorMiddleware → RequestLoggingMiddleware → MetricsMiddleware
     → TracingMiddleware → RequestContextMiddleware
     → LocalizationMiddleware → route handler
```

`LocalizationMiddleware` is the **innermost** built-in layer, closest to
the handler — added earliest (first `add_middleware()` call among the
built-ins), so it ends up dispatching last. `ErrorMiddleware` sits
**outside** it, several layers away — not immediately adjacent.

`TenantResolutionMiddleware` has no dedicated slot in `create_varco_app`;
an app wires it via `extra_middleware=`, which is added just inside CORS —
still **outside** `ErrorMiddleware`/`RequestLoggingMiddleware`/
`MetricsMiddleware`/`TracingMiddleware`/`RequestContextMiddleware`, and
therefore still outside (dispatches before) `LocalizationMiddleware`. The
ordering guarantee RD-3 depends on — "the tenant-default precedence step
sees `current_tenant()` already populated" — holds, but the two middlewares
are **not adjacent** in the stack the way a simplified
"`... → TenantResolution → Localization → handler`" diagram might suggest;
several other built-in layers sit between them.

### The `request.state` mirror — and its actual reach

`LocalizationMiddleware.dispatch()` stashes the resolved `RequestContext`
on `request.state.varco_request_context` in addition to the `ContextVar`,
specifically because a `ContextVar` set by an inner middleware becomes
invisible to any outer middleware the instant the inner one's `finally`
resets the token — which happens as the exception unwinds *through*
`LocalizationMiddleware` on its way out to `ErrorMiddleware`.

**This mirror is now read by both shipped error-rendering paths**
(Plan 011 drift-fix pass). Both `varco_fastapi.middleware.error.
ErrorMiddleware.dispatch()` (via `_service_error_response()`) and
`varco_fastapi.exceptions._make_error_response()` (used by
`add_exception_handlers()`) read `request.state.varco_request_context` and,
when constructed with `message_catalog=`, pass a `message_resolver=`
derived from the active locale into `error_message_for()` and set
`Content-Language` on the response themselves. `ErrorEnvelopeModule`
(`varco_fastapi.modules`, part of `default_modules()`) wires both automatically from its
resolved `MessageCatalog` (`message_catalog=`) and `I18nSettings.set_content_language`. Since
`request.state` — unlike the
`ContextVar` — is not reset by `LocalizationMiddleware`'s `finally`, it
survives being several layers further out from `ErrorMiddleware` exactly as
designed: the ordering hazard this mirror exists to solve. **The net
effect: an error response is now localized and carries `Content-Language`
whenever I18n is enabled and a catalog is bound**, the same as a
success-path response, whose `Content-Language` header was already set
correctly on the non-exception return path.

With no `message_catalog=` supplied (i18n disabled, the default), both
paths are byte-identical to before this fix — no localization, no header.
Writing your own custom exception handler that reads
`request.state.varco_request_context` and calls `error_message_for(exc,
message_resolver=catalog.format_message)` directly still works, and is the
right approach for a bespoke error-rendering path outside `create_varco_app`.

## See also

- `technical_docs/features/i18n-and-localization.md` — I2, the sibling X1
  consumer, `MessageCatalog`.
- `technical_docs/features/error-taxonomy-and-i18n.md` — `message_key`/
  `params`, the `message_resolver` seam.
- `technical_docs/features/job-scheduling-and-leases.md` — the "Zoned
  schedules" section for the full T2 wiring recipe.

## Pitfalls

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **`assume="utc"` breaks a working datetime filter** | `asyncpg` raises on a query that worked before the policy was changed | asyncpg rejects an **aware** datetime against a `TIMESTAMP WITHOUT TIME ZONE` column — which is exactly why `"naive"` (today's behaviour) is the default and `"utc"` is only the *recommendation* (Plan 011 / D-10) | Migrate the column to `TIMESTAMPTZ`, or leave the policy at `"naive"` and have clients send an explicit offset (`2026-01-01T00:00:00Z`) — an explicit offset wins under every policy |
| **`DatetimeCoercionPolicy(assume="utc")` has no effect on a `?field__gte=` filter** | The naive-string bound is still returned naive despite a policy being configured | `ASTTypeCoercion` (the visitor `QueryTransformer` drives) has no `policy=` parameter — only the free function `coerce_datetime(value, policy=...)` honours it | Call `coerce_datetime(value, policy=my_policy)` directly, or register a field-specific coercer via `TypeCoercionRegistry.register_field()` with `functools.partial(coerce_datetime, policy=my_policy)` |
| **tzdata absent in a slim image** | `ValueError` at `TimezoneSettings`/`I18nSettings`-adjacent startup naming a zone that "could not be resolved" | `python:*-slim`/distroless/Alpine images often ship no `/usr/share/zoneinfo` | `pip install tzdata` or `pip install "varco-core[tz]"` |
