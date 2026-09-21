# Plan 054 — HTTP-edge and admin-surface default flips (FLIP-2, FLIP-3, FLIP-4, FLIP-10) + the posture severity table

Covers BACKLOG 4.0 rows **`FLIP-2`** (`BACKLOG.md:68` — *"`SecurityPostureSettings.enforce`
default `"warn"` → `"refuse"` in `production`"*), **`FLIP-3`** (`:82` —
*"`mount_reliability_admin(server_auth=None)` refuses instead of warn-and-mount"*), **`FLIP-4`**
(`:83` — *"`build_webhook_router(server_auth=None)` refuses"*) and **`FLIP-10`** (`:71` —
*"`ErrorEnvelopeSettings.include_detail` → `False`"*), plus the adoption of Plan 047's
`di.in_process_backend` finding into one **posture severity table** that states, for every shipped
`check` id, whether it blocks a production start under the 4.0 default. Flip track, slice row 054
(`plans/000-index-4-0-di-standardization.md:33`): independently mergeable, no plan dependency —
but it *targets* the shapes 047/049/051/052/053 land (a `PostureCollector` subclass, a
`SecurityPostureModule` that is on by default, a `WiringReport` that prints `enforce`), and every
step names its pre-/post- form.

**Neighbours (do not absorb):** Plan 053 owns `FLIP-5` and its `posture.authorizer_unbound` (HIGH)
— this plan never touches the authorizer block; it only makes the refusal 053 designed for
("under 054's `refuse`") the default. Plan 049 owns `SecurityPostureModule`/FLIP-1 and its
`VARCO_SECURITY_POSTURE_ENABLED` escape hatch — this plan never reads or changes `enabled`. Plan
051 owns the `PostureCollector` ABC — this plan's collector edits are bodies, never the seam. Plan
052 owns `WiringReport`/`run_wiring_preflight()` — this plan specifies one additive value
(`enforce_source="derived"`) and never changes the report's refusal rule. Plan 047 owns
`di.in_process_backend`'s *emission*; this plan owns only its *consequence*. Plan 058 assembles
this plan's §Migration into the one 3.x → 4.0 guide.

**Design of record:** `plans/036-authorization-surface-and-posture.md` §D-S9-oq1 (`:241-277` —
one variable, presentation only, `development` is the only lenient word), §D-S9-suppress
(`:390-401`), §D-S9-enforce (`:403-418` — *"In `refuse`, `start()` raises after logging if any
finding is `HIGH` (and, in `production`, `WARN`) and is not suppressed"*), §D-S9-flip rows 2/3/4/10
(`:551-553,559`), rows 8/11 (`:557,560` — **deliberately not flips**);
`plans/035-http-edge-hardening.md` §D-S3 (`:186-214` — D-S3a flipped in 3.2, D-S3b warn-only with
the 4.0 flip scheduled; open question 3 `:980-983` lets 036's planner re-tier
`http.error.detail_exposed`'s *rendering*); `design/research/010-default-implementation-di-conventions.md`
§3 (`:43-55` — *"the default should refuse to start if no authorizer is configured (fail-closed
rule)"*; Spring CVE-2025-41248/41249 as the fail-open incident); `design/research/008-http-hardening-conventions-2026.md`
(the S3/S7/S8/S10 conventions 035 built on); `BACKLOG.md:221` (*"Admin mount acknowledgement —
three independent barriers per mount; the `server_auth=None` gap is `FLIP-3`/`FLIP-4`"*).

> ⚠️ **Five premises corrected from source while writing this plan; each reshapes a §D block.**
> (1) `enforce="refuse"` does **not** block "all findings not in the suppress list" and does not
> block "HIGH only" — it blocks `HIGH` in every environment and `WARN` only when
> `settings.environment == "production"` *exactly* (`varco_fastapi/varco_fastapi/posture.py:378-386`);
> `NOT_ASSESSED`/`INFO` never block. This plan keeps that rule byte-identical and flips only the
> default — so under the 4.0 default **every `WARN` id is a production start-blocker**, which is why
> §D-severity-table exists and why exactly one id is re-tiered (§D-ratelimit). (2) The scout's path
> for `include_detail` was wrong: the field is `varco_core/varco_core/exception/settings.py:72`
> (env `VARCO_ERROR_INCLUDE_DETAIL`), read in `varco_core/varco_core/exception/http.py:296,331-332`
> (`error_message_for()`), and the `detail` key is emitted by **two** response builders —
> `varco_fastapi/varco_fastapi/exceptions.py:142-143` (route-handler path) and
> `varco_fastapi/varco_fastapi/middleware/error.py:278-279` (middleware path). The middleware path
> emits **neither `message_key` nor `params`** (`error.py:271-279`), so once `detail` goes it
> carries nothing actionable — §D-flip10-parity. (3) The reliability admin routers never enforce
> `admin_role` themselves — `build_dlq_router`'s own docstring says enforcement is *"left to the
> caller's own `dependencies=`"* (`admin/dlq_router.py:65-70`), and `server_auth` is resolved on only
> **3 of 8** DLQ routes and **3 of 6** audit routes (`dlq_router.py:100,141,191`,
> `audit_router.py:84,145,193`) purely to answer the cross-tenant question. Requiring `server_auth`
> without calling it on every route would be a lie — §D-flip3-enforce. (4) The in-repo precedent
> for FLIP-3/4 is not one of the four `acknowledge_*` sites but the **third admin surface itself**:
> `build_tenant_router(server_auth=None)` → `ValueError` *"a guard that can never be satisfied is a
> startup error, not a per-request one"* (`varco_fastapi/varco_fastapi/tenancy/router.py:113-117`),
> re-raised by `mount_tenant_admin` whose module docstring lists *"`server_auth` is mandatory (no
> default) -> `TypeError`/`ValueError`"* as barrier 2 of 3 (`tenancy/mount.py:19-23`). (5) A
> `RouteGuard` denial is a `ServiceAuthorizationError(<guard message>, str)` (`auth/guard.py:173-235`),
> so its `error_params()` is `{"operation": "<guard message>", "entity": "str"}`
> (`varco_core/varco_core/exception/service.py:183-190`) — the actionable text **survives FLIP-10
> in `params`** on the route-handler path. 035's fear that suppressing `detail` "silently breaks
> `RouteGuard` denial messages" (`:214`) was true in 3.2 only because `params` did not yet exist
> on that path when the DEVIATION comment at `exceptions.py:135-141` was written.

## Goal

After this plan: a varco app whose `VARCO_SECURITY_ENV` is not exactly `development` runs
`SecurityPosture` with `enforce="refuse"` unless `VARCO_SECURITY_ENFORCE=warn` (or a code
`SecurityPostureSettings(enforce="warn")`) says otherwise — one `HIGH` finding anywhere, or one
unsuppressed `WARN` in `production`, aborts startup with a `RuntimeError` naming every blocking id,
the derivation, and the three fixes; `mount_reliability_admin()`, `mount_webhook_admin()`,
`build_dlq_router()`, `build_audit_router()` and `build_webhook_router()` all require `server_auth`
(a required keyword; `None` → `ValueError`) and every one of their routes resolves it and enforces
`admin_role`; every mapped `ServiceException` body omits `detail` unless
`VARCO_ERROR_INCLUDE_DETAIL=true`, and the middleware path carries `message_key`/`params` like the
handler path always did; `technical_docs/features/security-posture.md` carries one table listing
every shipped `check` id with its severity and its 4.0 production consequence, including
`di.in_process_backend` (WARN → blocks) and `http.rate_limit.absent` (re-tiered to INFO — a
recorded non-flip must not become a start-blocker by the back door). Each flip has exactly one
greppable escape hatch, a posture id, a §Migration row, and tests for default, escape hatch,
posture, and 052 composition.

## Non-goals

- **Changing what `refuse` means** (which severities block, the `NOT_ASSESSED` exemption, the
  `environment == "production"` comparison for `WARN`) — 036 §D-S9-enforce's rule is shipped and
  an operator who set `VARCO_SECURITY_ENFORCE=refuse` in 3.2 must see no semantic change.
- **`SecurityPostureSettings.enabled` / FLIP-1** — 049. **`posture.authorizer_unbound` /
  `posture.base_authorizer_bound`** — 053. **`posture.webhook_secrets_plaintext`'s flip
  (FLIP-13)** — 055/`varco_sa`. **`data.*` (FLIP-12)** — 056. **`redaction.*` (FLIP-040-*)** —
  057. **`tenant.*` (FLIP-6/7/9)** — 055. This plan lists their ids in the severity table with the
  severities *those* plans assign; it changes none of them.
- **A `Literal["production","development"]` type on `environment`** — 048's recipe (053 parked
  it there too). This plan adds an INFO finding for an unrecognised value instead (§D-flip2-env).
- **Emitting `message_key`/`params` for `QueryException`/`HTTPException` bodies on the
  middleware path** (`error.py:190-197,215-220`) — not `ServiceException`s, not `include_detail`'s
  scope. `QueryException`'s `f"Invalid query: {exc}"` echo is named in §Parked.
- **`ErrorMiddleware(debug=True)`'s 500 `detail`** (`http.error.debug_enabled`, HIGH) — unchanged.
- **Removing the `posture.admin_mount_unauthenticated` id** — ids are never removed (036
  §D-S9-suppress); §D-flip3-posture says what it means now.
- **`mount_tenant_admin`** — already refuses (premise (4)); untouched except its docstring
  gaining "same rule as the other two mounts since 4.0".
- **A second environment variable** for any flip here. `VARCO_SECURITY_ENFORCE`,
  `VARCO_SECURITY_ENV`, `VARCO_SECURITY_SUPPRESS`, `VARCO_ERROR_INCLUDE_DETAIL` already exist and
  are the only knobs this plan reads.

## Context (verified from source, 2026-09-17)

| Fact | Where | Why it matters |
|---|---|---|
| `SecurityPostureSettings`: `environment: str = Field(default="production", validation_alias=AliasChoices("VARCO_SECURITY_ENV"))`, `enforce: str = "warn"`, `suppress: str = ""`; `env_prefix="VARCO_SECURITY_"`, `frozen=True`, `populate_by_name=True` | `varco_fastapi/varco_fastapi/posture.py:154-196` (fields `:187-192`) | The flip site. `frozen=True` rules out a `model_validator(mode="after")` that assigns `enforce`; the derivation is a property (§D-flip2-default) |
| Demotion: `WARN → INFO` iff `settings.environment == "development"`; suppression → `INFO` + `suppressed=True`; unknown suppression id → `posture.unknown_suppression` INFO | `posture.py:326-355` (`:351` the only `"development"` read) | `HIGH` is never demoted; an unrecognised environment value demotes nothing |
| Refuse rule: `if settings.enforce == "refuse": blocking = [f … if not f.suppressed and (f.severity == HIGH or (f.severity == WARN and settings.environment == "production"))]`; `RuntimeError("SecurityPostureLifecycle(enforce='refuse'): refusing to start — unsuppressed blocking findings: …")` | `posture.py:378-393` | Premise (1). Reads the raw field — must read `effective_enforce`. The message gains the derivation and the fixes (§D-flip2-message) |
| `SecurityPosture(environment, findings, not_assessed)` frozen dataclass; `summary()` prints the four counts + `(N suppressed)`; `start()` logs `SecurityPosture: <summary>` once, then one line per WARN/HIGH | `posture.py:89-151`, `:360-376` | Gains an appended, defaulted `enforce: str` so the one summary line says which mode judged it (§D-flip2-report) |
| `_collect_local(...)` — `posture.admin_mount_unauthenticated` (WARN) fires from two caller-supplied booleans, `webhook_mounted_unauthenticated`/`reliability_mounted_unauthenticated`, default `False` | `posture.py:671-702` (post-051 `LocalPostureCollector.collect()`, same body) | Nobody in-tree sets them (051's `configure_app` cannot know a mount happened — mounts are never module kwargs, RD-9); after FLIP-3/4 the state is unreachable through varco's own mounts (§D-flip3-posture) |
| `_collect_http` re-emits 035's nine `http.*` ids and severities unchanged | `posture.py:585-600`; the module comment `:407-409` | Re-tiering `http.rate_limit.absent` therefore happens at 035's seam, `introspect.py` (§D-ratelimit) |
| `mount_reliability_admin(app, *, audit_repo=None, dlq=None, redriver=None, acknowledge_bundled_admin=False, server_auth: Any \| None = None, admin_role="reliability-admin", cross_tenant_role="cross-tenant-admin", prefix="/reliability", dependencies=None)`; `server_auth is None` → one `_logger.warning(... "mounting UNAUTHENTICATED")` and mounts | `varco_fastapi/varco_fastapi/admin/mount.py:46-58`, warn `:118-123`; `acknowledge_bundled_admin` `ValueError` `:103-110`; double-mount `ValueError` `:112-116` | FLIP-3's site. The `ValueError` text shape at `:104-110` is the one this plan copies |
| `build_dlq_router(dlq, *, redriver=None, server_auth=None, admin_role=…, cross_tenant_role=…, prefix="/dlq")` — 8 routes; `resolve_ctx(server_auth, request)` on `list_entries`/`delete_where`/`redrive_batch` only; docstring: *"`admin_role` enforcement is still left to the caller's own `dependencies=`"* | `varco_fastapi/varco_fastapi/admin/dlq_router.py:47-87`, routes `:89-191`, `resolve_ctx` calls `:100,141,191` | Premise (3): `GET /entries/{id}`, `DELETE /entries/{id}`, `GET /stats`, `POST /entries/{id}/redrive` never touch `server_auth` at all |
| `build_audit_router(repo, *, server_auth=None, admin_role="reliability-admin", …)` — 6 routes; `resolve_ctx` on 3 | `varco_fastapi/varco_fastapi/admin/audit_router.py:46-47`, routes `:70-193` | Same gap |
| `admin/_tenant_scope.py`: `resolve_ctx()` (returns `None` when `server_auth is None`), `allow_cross_tenant()` (`ctx is None ⇒ False`), `resolve_tenant_scope()`; `__all__ = []` (internal) | `varco_fastapi/varco_fastapi/admin/_tenant_scope.py:28-94` | The internal home for the new router-level guard (§D-flip3-enforce); `resolve_ctx` learns to read `request.state` first |
| `mount_webhook_admin(app, *, repository, redriver=None, acknowledge_bundled_admin=False, server_auth=None, admin_role="webhook-admin", cross_tenant_role=…, prefix="/webhooks")`; `None` → warning and mounts | `varco_fastapi/varco_fastapi/webhook/mount.py:37-108` (warn `:89-94`) | FLIP-4's mount |
| `build_webhook_router(repository, *, redriver=None, server_auth=None, admin_role=…, cross_tenant_role=…, prefix=…)`; `_enforce(request)` on **every** route resolves ctx and, `if server_auth is not None`, `_require_role(ctx, admin_role)` (403 `f"{admin_role!r} role required."`); `_resolve_ctx` `:67-90`; `_require_role` `:93-103` | `varco_fastapi/varco_fastapi/webhook/router.py:147-200`, routes `:202-299` | FLIP-4's builder — already the right shape minus the `None` branch; the model for the reliability routers |
| `build_tenant_router(control_service, *, server_auth: AbstractServerAuth \| None, …)` (no default) → `ValueError` on `None`; `mount_tenant_admin` docstring barrier 2 | `varco_fastapi/varco_fastapi/tenancy/router.py:55,113-117`; `tenancy/mount.py:19-23,61,77,118-122` | Premise (4): the precedent this plan makes uniform across the three mounts |
| `varco_fastapi/tests/test_tenant_router.py::test_build_tenant_router_refuses_without_server_auth` | `:91-95` | The test shape to copy |
| `ErrorEnvelopeSettings.include_detail: bool = True`; docstring already says *"4.0 flip candidate — the default is planned to become `False`"*; `env_prefix="VARCO_ERROR_"` | `varco_core/varco_core/exception/settings.py:42-50,66,72` | FLIP-10's site |
| `error_message_for()`: `settings = envelope_settings or ErrorEnvelopeSettings()`; `detail = raw_detail if (raw_detail and settings.include_detail) else None`; the explanatory comment cites 035 §D-S3b and `exceptions.py:135-141` | `varco_core/varco_core/exception/http.py:242-362` (`:296`, `:325-332`) | Unchanged logic; the comment is rewritten |
| Route-handler path `_make_error_response()`: `ErrorEnvelopeSettings()` read fresh per call; body = `code`, `message`, `detail` (if any), `message_key`, `params`, RFC 9457 members; the Plan 002 DEVIATION comment | `varco_fastapi/varco_fastapi/exceptions.py:119-155` (`:135-143`) | Comment rewritten (premise (5)); no logic change |
| Middleware path `ErrorMiddleware._service_error_response()`: body = `code`, `message`, `detail` only; `+ correlation_id` | `varco_fastapi/varco_fastapi/middleware/error.py:249-300` (`:268-279`) | Premise (2): gains `message_key`/`params` (§D-flip10-parity) |
| `create_varco_app` calls `add_exception_handlers()` — `ServiceAuthorizationError`/`NotFound`/`Conflict`/`Validation` from routes/dependencies take the handler path; the middleware path is for exceptions raised inside middleware | `varco_fastapi/varco_fastapi/app.py:311,529`; `exceptions.py:255-267` | Why FLIP-10's blast radius on `RouteGuard` is the handler path, where `params` exists |
| `ServiceValidationError.error_params()` → `{"detail", "field"?}`; `ServiceConflictError` → `{"detail"}`; `ServiceNotFoundError` → `{"entity","entity_id"}`; `ServiceAuthorizationError` → `{"operation","entity"?}` (never `reason`) | `varco_core/varco_core/exception/service.py:123-124,183-190,235-236,287-291` | Every built-in exception's actionable content is already structured; `detail` was a duplicate of it |
| `inspect_http_edge()` — `http.error.detail_exposed` `"warn"`, remediation *"VARCO_ERROR_INCLUDE_DETAIL=false (4.0 flip candidate)"*; `http.rate_limit.absent` `"warn"`; the docstring id table | `varco_fastapi/varco_fastapi/middleware/introspect.py:13-24` (table), `:355-365` (detail), rate-limit block (locate: `rg -n 'http.rate_limit.absent' introspect.py`) | Both texts change (§D-flip10-posture, §D-ratelimit) |
| `test_http_edge_introspect.py::test_default_app_reports_exactly_rate_limit_absent_and_detail_exposed_findings` asserts the default app's ids are `{"http.rate_limit.absent", "http.error.detail_exposed"}` | `varco_fastapi/tests/test_http_edge_introspect.py:51-55` | Becomes `{"http.rate_limit.absent"}` (at INFO) |
| `varco_core/tests/test_error_envelope_settings.py::test_include_detail_defaults_to_true_byte_identical` | `:22-25` | Flips to `False` |
| Tests reading a `ServiceException` body's `detail`: `auth/test_token_profile_guard.py:84` (`str(body.get("detail", body))` — falls back to the whole body, so `params.operation` satisfies it); `milestone_a/test_middleware.py:73` (`debug=True` 500 — `_internal_error_response`, not `include_detail`); `milestone_f/test_mcp_auth_middleware.py:143-196` and `test_middleware.py:120` (`HTTPException` `{"detail": …}` bodies — not the envelope) | grep `"detail"` in `varco_fastapi/tests` | Only the first needs a tightened assertion; the rest are unaffected and must stay green unmodified (the proof that FLIP-10 touches only the envelope) |
| `examples/00-full-stack-post-api/example/tests/test_api_auth.py:120` — `"detail" in resp.json()` on a 401 | that file | An `HTTPException` body; unaffected |
| In-repo sites constructing the five admin functions: 34 in 7 files — `test_mount_reliability_admin.py` (11), `test_admin_tenant_guard.py` (7), `test_dlq_router.py` (5), `test_audit_router.py` (5), `test_mount_webhook_admin.py` (4), `test_mount_guard_weakset.py` (1), `test_webhook_bola_guard.py` (1); `test_admin_tenant_guard.py:23-34` already has a `_CtxAuth(roles)` stand-in | `rg -c 'build_dlq_router\(\|build_audit_router\(\|build_webhook_router\(\|mount_reliability_admin\(\|mount_webhook_admin\(' varco_*/tests examples/` | Sweep list for Step 8 |
| In-repo sites running a posture lifecycle with default settings: `test_posture_core.py:61-173`, `test_posture_collectors.py:62`; post-049 every `create_varco_app()` test that enters lifespan (the `SecurityPostureModule` is default-on) | grep above | FLIP-2's blast radius — §D-flip2-tests |
| 047: `di.in_process_backend` WARN, one per in-process `SeamSelection`; *"Whether production **refuses** on it is 054's call"*; 052's `InMemoryProfile` binds `SecurityPostureSettings(environment="development", enforce="warn")` | `plans/047-…md:537-546,839-840`; `plans/052-…md:503` | The profile is unaffected by this plan; the default app is (§D-severity-table) |
| 052: `WiringReport.enforce: str; enforce_source: Literal["code","env","default"]`; header renders `enforce=<value> (<source>)`; `varco diagnose` never starts a module; `run_wiring_preflight()` refuses on providify `ERROR` before any `start()`; `WiringFinding` is *"never a posture finding"* | `plans/052-…md:218-228,237-238,381,403-411,210` | §D-compose: the two refusals have disjoint inputs and disjoint times by construction; `"derived"` is the one additive value |
| 053: `posture.authorizer_unbound` HIGH *"blocks under 054's `refuse` in every environment"*; `posture.base_authorizer_bound` WARN in production only via an explicit instance, *"needs `VARCO_SECURITY_SUPPRESS=…`"* | `plans/053-…md:199-202` | Already designed against this plan's default; the severity table records it |

## Design

### §D-flip2-default — `enforce: str | None = None`; `None` derives `"refuse"` unless `VARCO_SECURITY_ENV` is exactly `development`

**Chosen: a sentinel-derived default, computed by a property; the refuse rule itself is untouched.**

```python
class SecurityPostureSettings(VarcoSettings):
    environment: str = Field(default="production", validation_alias=AliasChoices("VARCO_SECURITY_ENV"))
    enforce: str | None = None          # explicit "warn" | "refuse" wins; None → derived
    suppress: str = ""

    @property
    def effective_enforce(self) -> str:
        if self.enforce is not None:
            return self.enforce
        return "warn" if self.environment == "development" else "refuse"

    @property
    def enforce_is_derived(self) -> bool:
        return self.enforce is None
```

`SecurityPostureLifecycle.start()` reads `settings.effective_enforce` at `:378`; the blocking
predicate at `:379-386` is byte-identical. Resulting 4.0 matrix (default `enforce`):

| `VARCO_SECURITY_ENV` | effective `enforce` | `HIGH` | unsuppressed `WARN` | `WARN` presentation | `NOT_ASSESSED`/`INFO` |
|---|---|---|---|---|---|
| unset / `production` | `refuse` (derived) | blocks | **blocks** (`:385`) | `WARN` | never block |
| `development` | `warn` (derived) | logged | logged | demoted to `INFO` | — |
| anything else (`staging`, `prod`, `Development`, `""`) | `refuse` (derived) | blocks | logged (not `production`) | `WARN`, not demoted | — ; plus `posture.unrecognised_environment` INFO (§D-flip2-env) |
| any, `VARCO_SECURITY_ENFORCE=warn` | `warn` (env) | logged | logged | per environment | — |
| any, `VARCO_SECURITY_ENFORCE=refuse` | `refuse` (env) — the 3.2 opt-in, unchanged | blocks | blocks in `production` only | per environment | — |

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: `None` sentinel + `effective_enforce` property, derivation keyed on `environment != "development"`** | The BACKLOG row verbatim ("`refuse` in `production`") *and* 036 §D-S9-oq1's rule that `development` is the only lenient word — the same literal, the same comparison direction 053 chose for the authorizer gate (*"any other value is strict for both readers"*); 048's precedence holds per field (`instance=`/code `enforce="warn"` > env `VARCO_SECURITY_ENFORCE=warn` > derived) with no validator trick; `frozen=True` untouched; the 3.2 explicit values keep their exact meaning; 052 can render provenance honestly (`derived`) because the field itself still says `None`; the same shape 053 used for `revocation_require_jti: bool \| None` | `settings.enforce` is no longer the value the lifecycle uses — every reader must go through `effective_enforce` (three in-tree readers: the lifecycle, 052's report, this plan's tests; guarded by a test that greps for `.enforce ==` outside the property). A `SecurityPostureSettings(environment="development")` *in code* derives `warn` — consistent with 053's "the code form is presentation only" only in the sense that the env var is what production reads; documented |
| (2) literal `enforce: str = "refuse"` | One line | `development` would refuse on `HIGH` — `http.error.debug_enabled` (HIGH, `ErrorMiddleware(debug=True)`) and 053's `posture.authorizer_unbound` (HIGH, never demoted) would make the documented dev setup (`debug=True`, `VARCO_SECURITY_ENV=development`) unstartable unless every developer also exports `VARCO_SECURITY_ENFORCE=warn`. Not "refuse in production"; rejected |
| (3) pydantic `default_factory` reading `os.environ["VARCO_SECURITY_ENV"]` | The field carries the value | A second, hand-rolled read of a variable pydantic already parses (alias, `populate_by_name`), which disagrees with a code-supplied `environment="development"`; and 048's provenance would record it as `default` while the value is not the class default — the "silently derived" shape 052 exists to expose. Rejected |
| (4) keyed on `environment == "production"` | The row's literal word | `staging`/`prod`/`Production` would silently get **`warn`** — an unrecognised value would be *more* lenient than the default, the exact inversion 036 §D-S9-oq1 rejected ("must default to strict or nobody sets it"). Rejected — the strict direction is `!= development`, and §D-flip2-env reports the unrecognised value |
| (5) block `HIGH` only under the derived default; keep `HIGH+WARN` for an explicit `refuse` | Smaller blast radius | Two meanings for one word depending on how it was set — an operator reading `enforce=refuse` in the report cannot tell which rule applies; 036 committed to one rule. Rejected — the blast radius is handled by the severity table (§D-severity-table) and by suppression by id, which is the designed answer |

**Escape hatch (one greppable line):** `VARCO_SECURITY_ENFORCE=warn` (env) or
`SecurityPostureSettings(enforce="warn")` / `bind_settings(container, SecurityPostureSettings,
instance=SecurityPostureSettings(enforce="warn"))` (code, 048's recipe). Per-finding:
`VARCO_SECURITY_SUPPRESS=<id>[,<id>…]` — the answer for a knowingly-accepted `WARN` (036
§D-S9-suppress), unchanged.

### §D-flip2-env — an unrecognised `VARCO_SECURITY_ENV` (or `VARCO_SECURITY_ENFORCE`) value is reported, never normalised

`SecurityPostureLifecycle.start()` appends one `PostureFinding(check="posture.unrecognised_environment",
severity=INFO, detail="VARCO_SECURITY_ENV is '<value>' — not 'production' or 'development'.
Treated as strict for enforcement (refuse on HIGH) but WARN findings neither block nor demote.",
remediation="Set VARCO_SECURITY_ENV to exactly 'production' or 'development'.")` when
`settings.environment not in ("production", "development")`. The sibling
`posture.unrecognised_enforce` (INFO) fires when `settings.effective_enforce not in ("warn",
"refuse")` — the value is then treated as `warn` (today's `== "refuse"` test at `:378` would
silently do the same; the finding makes it visible). INFO never blocks; neither is demoted; both
are suppressible like any id.

- ✅ Closes the one silent seam premise (1)'s matrix has: `prod` is strict-but-not-quite, and
  without a finding nobody would know; `Refuse`/`strict` would silently mean `warn`. ✅ Same
  "report the typo" shape as `posture.unknown_suppression` (`posture.py:326-343`). ✅ Never
  normalises — 053's rule (`@Requires` compares the exact string; `SecurityPostureSettings` never
  did either).
- ❌ Two more ids. Rejected — raising on an unknown value: 048 owns the `Literal` type, and a
  startup exception for a presentation knob is disproportionate; the strict derivation already
  protects the consequence side. Both findings are deletable the day 048's `Literal` lands.

### §D-flip2-message — the `RuntimeError` names the derivation and the three fixes

```
SecurityPostureLifecycle(enforce='refuse', derived from VARCO_SECURITY_ENV='production'):
refusing to start — unsuppressed blocking findings: di.in_process_backend, posture.authorizer_unbound.
Fix each finding (see the SecurityPosture[...] lines above), suppress a knowingly-accepted id with
VARCO_SECURITY_SUPPRESS=<id>, or set VARCO_SECURITY_ENFORCE=warn to restore the 3.x warn-only preflight.
```

The first parenthesis reads `enforce='refuse', from VARCO_SECURITY_ENFORCE` / `from code` when
explicit (source per 048's `provenance_of()` when the settings instance came through
`bind_settings`, else `"env"` iff `"VARCO_SECURITY_ENFORCE" in os.environ`, else `"code"`). The
`refusing to start — unsuppressed blocking findings:` fragment is **kept verbatim** — 049's
`SecurityPostureModule` test and any operator alert rule may match on it.

### §D-flip2-report — `SecurityPosture.enforce` (appended, defaulted) and the summary line

`SecurityPosture` gains `enforce: str = "warn"` as a trailing field (frozen dataclass; positional
construction in tests keeps working). `summary()` ends with `· enforce=<value>` after the
`(N suppressed)` group; the shape stays fixed across reports. `start()` sets it from
`settings.effective_enforce`. ✅ The one log line an operator greps says which rule judged the
report. ❌ One more field on a public frozen dataclass — additive; `api_surface.py --check`
cannot see dataclass fields (class signatures are not recorded) and reports nothing.

### §D-flip2-tests — one autouse `VARCO_SECURITY_ENFORCE=warn` per test package; the FLIP-2 tests `delenv` it

**Chosen: `varco_fastapi/tests/conftest.py` and `examples/00-full-stack-post-api/example/tests/conftest.py`
each gain a function-scoped autouse fixture `_posture_warn_only(monkeypatch)` that sets
`VARCO_SECURITY_ENFORCE=warn`, with a docstring naming FLIP-2.** The tests *about* FLIP-2
(Step 2) call `monkeypatch.delenv("VARCO_SECURITY_ENFORCE", raising=False)` first, and a code
`SecurityPostureSettings(enforce=…)` instance beats the env anyway (048 precedence).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: autouse `ENFORCE=warn`, FLIP-2 tests opt out explicitly** | Post-049 *every* `create_varco_app()` test that enters lifespan runs the posture module; post-047 every such test with in-memory seams emits `di.in_process_backend` (WARN); with `VARCO_SECURITY_ENV` unset (CI) the derived default is `refuse` → **hundreds** of existing tests would refuse to start for a reason unrelated to what they test. `ENFORCE` changes *consequence only* — presentation (demotion, ids, severities) is untouched, so every posture assertion in the suite still sees the real report; one line, greppable (`rg -n '_posture_warn_only' varco_*/tests examples/`) | Hides the flip from the suite by default — mitigated by Step 2's explicit `delenv` tests, which are the proof, and by `test_settings_default_enforce_is_none_and_derived` which runs with the variable deleted |
| (2) sweep every test to `VARCO_SECURITY_ENV=development` (053's per-test discipline) | Greppable per test | 053 chose per-test `setenv` for `ENV` because an autouse `ENV=development` would *change presentation* (WARN → INFO) under every posture test — the reason does not transfer to `ENFORCE`, and the sweep here is not four files but every lifespan-entering test in the repo. Rejected |
| (3) use 052's `InMemoryProfile` everywhere | The designed test shape | Rewriting hundreds of pre-052 tests is 058-scale work, not a flip plan's. Rejected; new tests written by this plan **do** use the profile where it exists |

### §D-severity-table — every shipped `check` id, its severity, and its 4.0 production consequence

The table below is **the** deliverable 047 asked for ("adopt `di.in_process_backend` into the
severity table") and is written into `technical_docs/features/security-posture.md` as a new
section replacing the prose-only collector table's severity hints. Column 4 is mechanical:
`HIGH` → blocks everywhere; `WARN` → blocks in `production` (unless suppressed); `INFO`/
`NOT_ASSESSED` → never. Severities are those the owning plan assigns; **this plan moves exactly
one** (§D-ratelimit) and adds exactly two INFO ids (§D-flip2-env).

| `check` | Severity | Owner | Blocks a `production` start under the 4.0 default? | Escape |
|---|---|---|---|---|
| `posture.authorizer_unbound` | HIGH | 053 | **yes, every environment** | bind an `AbstractAuthorizer`; `VARCO_SECURITY_ENV=development` |
| `posture.base_authorizer_bound` | WARN | 036/053 | yes (only reachable via an explicit instance binding in production) | `VARCO_SECURITY_SUPPRESS=posture.base_authorizer_bound` |
| `posture.admin_mount_unauthenticated` | WARN | 036/**054** | yes — but unreachable through varco's mounts since FLIP-3/4 (§D-flip3-posture) | pass `server_auth=` |
| `posture.tenant_admin_mounted` | INFO | 036 | no | — |
| `posture.webhook_secrets_plaintext` | WARN | 036 (flip: 055/`varco_sa`) | yes | `encryptor=`; suppress |
| `posture.webhook_transport_unverified` | INFO | 036 | no | — |
| `posture.unknown_suppression` | INFO | 036 | no | fix the typo |
| `posture.unrecognised_environment` | INFO | **054** (new) | no | set `production`/`development` |
| `posture.unrecognised_enforce` | INFO | **054** (new) | no — the value is treated as `warn` | set `VARCO_SECURITY_ENFORCE` to exactly `warn`/`refuse`, or unset it |
| `posture.not_assessed.*` | NOT_ASSESSED | 036 | never (§D-S9-enforce) | install the sibling package |
| `di.in_process_backend` | WARN | 047 (consequence: **054**) | **yes** — an in-memory event bus / cache / DLQ / idempotency store / job store in production is lost on restart and single-process by construction; INFO in `development` by the standard demotion | `VARCO_<SEAM>_BACKEND=<real backend>`; a deliberate single-process deployment: `VARCO_SECURITY_SUPPRESS=di.in_process_backend` (one id covers every seam; the detail names each) |
| `auth.api_key_query_fallback` | WARN | 034/036 | yes | drop `param=`; suppress |
| `auth.api_key_plaintext` | WARN | 034/036 (053 deprecates the path) | yes | `hashed_keys=` |
| `auth.passthrough_bound` | WARN | 034/036 | yes | suppress if the gateway deployment is deliberate (053) |
| `auth.revocation_registry_unwired` | WARN | 034/036 | yes — the "bound but not wired" bug class, exactly what should never reach production silently | `TrustedIssuerRegistry(revocation_store=…)` |
| `auth.revocation_unbound` | INFO | 034/036 | no | — |
| `auth.revocation_fail_open` | HIGH | 034/036 | yes, every environment | `revocation_failure_mode=fail_closed` |
| `auth.revocation_jti_not_required` | INFO | 053 | no | — |
| `auth.jwks_refresh_off` | WARN | 052 | yes | `JwksRefreshModule`/`VARCO_JWT_JWKS_TTL_SECONDS` |
| `auth.jwks_keysets_unloaded` | INFO | 052 | no | — |
| `tenant.no_chain`, `tenant.legacy_source_implicit`, `tenant.no_membership_provider`, `tenant.unchained_claim_tenant_setter` | WARN | 033/036 (flips: 055) | yes | per `tenant-provenance.md` |
| `tenant.membership_missing_claim_allows` | HIGH | 033/036 | yes, every environment | `on_missing_claim=DENY` |
| `tenant.cross_check_lenient`, `tenant.delegation_unbound` | INFO | 033/036 (recorded non-flip) | no | — |
| `http.security_headers.absent`, `http.body_limit.absent` | WARN | 035 | yes — both are on by default; absent means an app turned them off | re-enable or suppress |
| `http.security_headers.hsts_absent` | INFO | 035 | no | — |
| `http.rate_limit.absent` | **INFO** (was WARN — §D-ratelimit) | 035 → **054** | **no** — rate limiting is a recorded non-flip (036 row 11) | — |
| `http.rate_limit.no_pre_auth_rule` | WARN | 035 | yes — an *installed* limiter with no pre-auth rule is a gap in an adopted feature | add a `PRE_AUTH` rule |
| `http.rate_limit.in_memory_limiter`, `http.rate_limit.fail_open` | INFO | 035 | no | — |
| `http.error.detail_exposed` | WARN | 035 (flip: **054**) | yes — after FLIP-10 it fires only on an explicit `VARCO_ERROR_INCLUDE_DETAIL=true` | unset it; suppress if deliberate |
| `http.error.debug_enabled` | HIGH | 035 | yes, every environment | `debug=False` |
| `data.rls_disabled`, `data.role_bypasses_rls` | HIGH | 037 (flip: 056) | yes, every environment | per `postgres-rls.md` |
| `data.rls_not_forced` | WARN | 037 | yes | `FORCE ROW LEVEL SECURITY` |
| `data.rls_not_checked` | NOT_ASSESSED | 037 | never | pass `rls_posture=` |
| `redaction.audit.disabled`, `redaction.error_params.disabled` | WARN | 040/057 | yes (057 argued exactly this: "an explicit opt-out … should need an explicit suppression") | per `redaction.md` |
| `redaction.default.custom`, `redaction.patterns.substring_mode` | INFO | 040/057 | no | — |
| `retention.platform_wide` | WARN | 052 | yes | scope the policy |
| `retention.dry_run_only`, `retention.short_retention` | INFO | 052 | no | — |

⚠️ The table is authoritative for *consequence*, not for *emission* — each owning plan's doc
still says when its id fires. A new id added by a later plan must add a row here (a `technical_docs`
rule stated in the doc's own header, mirroring `COVERAGE.md`'s "argue against a written record").
Suppression is an operator statement for **any** tier — a suppressed HIGH is demoted to INFO and
does not block (`:382`), as in 3.2.

**What a stock 4.0 app sees in production with zero configuration** (all sibling flips landed):
`di.in_process_backend` for every seam left on its in-process default → **refuses**. That is the
row's purpose and brief 010 §3's rule applied to the seams whose in-process default is a
data-loss shape (DLQ, idempotency, job store, outbox-adjacent bus); the fix is one
`VARCO_<SEAM>_BACKEND=` per seam, or one suppression for a deliberate single-process deployment.
The §Migration table says so in its first row.

### §D-ratelimit — `http.rate_limit.absent` WARN → INFO, because a recorded non-flip cannot become a start-blocker

036 §D-S9-flip row 11 (`:560`): *"Rate limiting is opt-in — remains opt-in; **not** a flip"*, with
the explicit warning at `:565-566` that a 4.0 planner must not "complete the list" by flipping it.
Under FLIP-2's default, a `WARN` id **is** a production start-blocker — so leaving
`http.rate_limit.absent` at WARN would flip row 11 by the back door: every 4.0 production app
without a `RateLimitBundle` would refuse to start. The id moves to `"info"` at 035's own seam
(`introspect.py`, the `http.rate_limit.absent` block), with its detail text unchanged and its
remediation gaining "(opt-in; INFO since 4.0 — Plan 054 §D-ratelimit)".

- ✅ Honours both recorded decisions at once (row 11 *and* FLIP-2). ✅ The id, detail, and the
  `HttpEdgePosture.rate_limit_installed` fact are unchanged — a dashboard keyed on the id still
  works. ✅ `http.rate_limit.no_pre_auth_rule` stays WARN: an *installed* limiter with every rule
  `POST_AUTH` is a hole in an adopted feature, not a non-adoption.
- ❌ An operator who used `VARCO_SECURITY_ENFORCE=refuse` in 3.2 *to* force rate-limit adoption
  loses that lever. Accepted: that operator can add the id to their own gate (`SecurityPosture`
  is data), and the 3.2 CHANGELOG never promised WARN for it. ❌ A severity moves — 052's
  non-goal says "no severity assigned in `varco_fastapi/posture.py` moves"; this one is assigned
  in `introspect.py` (035's seam, `posture.py:407-409` re-emits unchanged), and the move is a
  4.0 CHANGELOG line, not a silent drift.
- Rejected — **keep WARN, document "suppress it"**: makes the *documented default* of a stock app
  "refuses to start until you suppress a non-flip", which is the definition of a back-door flip.
- Rejected — **a `NON_BLOCKING_WARN` allowlist in the lifecycle**: a special-case list beside the
  severity ladder is exactly the kind of table that rots; severity *is* the tier.

### §D-flip3 — `server_auth` becomes a required keyword on both mounts and all three builders; `None` → `ValueError`; no acknowledgement kwarg

**Chosen (i): the `build_tenant_router`/`mount_tenant_admin` precedent, made uniform.**

```python
def mount_reliability_admin(app, *, server_auth: Any, audit_repo=None, dlq=None, ...):   # no default
    if not acknowledge_bundled_admin: raise ValueError(...)                               # unchanged, first
    if server_auth is None:
        raise ValueError(
            "mount_reliability_admin() requires server_auth — an admin surface that can replay "
            "messages onto the bus and delete audit records must never mount unauthenticated "
            "(4.0, Plan 054 / FLIP-3). Behind a gateway that already verified the caller, pass "
            "PassthroughAuth(acknowledge_unverified=True)."
        )
```

Same shape in `mount_webhook_admin` (FLIP-4, "create/delete subscriptions and rotate secrets"),
`build_webhook_router`, `build_dlq_router`, `build_audit_router` (each builder refuses on its own,
so a caller who bypasses the mount gets the same refusal — exactly as `build_tenant_router` does
at `tenancy/router.py:113-117`). Omitting the keyword entirely is Python's own
`TypeError: … missing 1 required keyword-only argument: 'server_auth'` — visible to mypy under
`strict`, the barrier-2 shape `tenancy/mount.py:21` already documents (*"no default ->
`TypeError`/`ValueError`"*). The `_logger.warning(... UNAUTHENTICATED)` branches
(`admin/mount.py:118-123`, `webhook/mount.py:89-94`) are deleted — there is nothing left to warn
about.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(i) chosen: required keyword; `None` → `ValueError`; no escape hatch** | Makes the three admin mounts *identical* on barrier 2 — the BACKLOG "Verified as already sound" row (`:221`) claims "three independent barriers per mount" and names this gap as the only exception; `ValueError` matches the sibling's exact class and the two other `ValueError`s already in each mount (`acknowledge_bundled_admin`, double mount); the message is the one place the *legitimate* "I have a gateway" shape is answered, and that answer is 053's `PassthroughAuth(acknowledge_unverified=True)` — a real strategy, reported by `auth.passthrough_bound`, not a hole; a stub `server_auth` for tests is three lines (`test_admin_tenant_guard.py:23-34` already has one) | 34 in-repo call sites gain a kwarg (Step 8); the `ctx is None` fail-closed branch in `allow_cross_tenant()` is no longer reachable through a mount and is unit-tested directly instead (it stays: a custom `server_auth` may legitimately return `None` for an anonymous caller) |
| (ii) `acknowledge_unauthenticated: bool = False` → `ValueError` (the `acknowledge_bundled_admin`/`acknowledge_global_mutation` shape) or `Literal[True]` → `TypeError` (053's `acknowledge_unverified`) | Consistent with the four acknowledgement kwargs | Every existing `acknowledge_*` gates a shape that is *sometimes right* — bundling an admin surface into the app pod, mutating process trust, trusting a verifying gateway. An unauthenticated DLQ-replay/audit-delete/secret-rotation surface is never right, and the one deployment that looks like it (a gateway) already has its acknowledgement kwarg on `PassthroughAuth`. A second acknowledgement for the same fact is two knobs for one axis (036 §D-S9-oq1's reasoning). Rejected |
| (iii) keep `= None`, refuse only at the mounts, leave the builders | Fewer signatures move | `build_tenant_router` refuses at the builder; a caller who hand-includes `build_dlq_router(dlq)` with no auth would still get an unauthenticated surface. Rejected |

**Escape hatch:** none, by design (the row's point). **Posture id:** §D-flip3-posture.

### §D-flip3-enforce — the reliability routers enforce `admin_role` on every route, like the webhook router already does

Premise (3): today `mount_reliability_admin(server_auth=X)` calls `X` on 6 of 14 routes and
enforces `admin_role` on none. Requiring `server_auth` without honouring it on every route would
make FLIP-3 a signature change with no security effect. `admin/_tenant_scope.py` (the shared
internal module both routers already import) gains:

```python
def admin_guard(server_auth: Any, admin_role: str) -> Callable[[Request], Awaitable[Any]]:
    """Router-level dependency: resolve ``server_auth``, enforce ``admin_role`` (403), stash ctx."""
    async def _guard(request: Request) -> Any:
        ctx = await resolve_ctx(server_auth, request)
        if admin_role not in getattr(ctx, "roles", []):
            raise HTTPException(status_code=403, detail=f"{admin_role!r} role required.")
        request.state.varco_admin_ctx = ctx
        return ctx
    return _guard
```

`build_dlq_router`/`build_audit_router` construct `APIRouter(prefix=…, tags=…,
dependencies=[Depends(admin_guard(server_auth, admin_role))])`; `resolve_ctx()` returns
`request.state.varco_admin_ctx` when present (so the three existing per-route calls do not invoke
`server_auth` twice); the docstrings' *"enforcement is left to the caller's own `dependencies=`"*
sentences are replaced. `mount_reliability_admin(dependencies=…)` stays **additive** (an IP
allowlist, an mTLS check) — its docstring no longer implies it is the *only* place role
enforcement can happen. The webhook router's `_enforce` (`webhook/router.py:193-200`) loses its
`if server_auth is not None:` branch (always true now) and is otherwise unchanged.

- ✅ The three admin surfaces now share one contract: authenticated on every route, `admin_role`
  enforced on every route, `cross_tenant_role` for the cross-tenant step (§D-S4-role unchanged).
  ✅ 403 text identical to the webhook router's (`:103`). ✅ No new public API; one internal
  factory. ❌ A router-level dependency runs before per-route parameter validation, so a
  malformed request from an unauthenticated caller is now 401/403 rather than 422 — correct
  (never validate for a caller you have not authenticated). ❌ `test_dlq_router.py`/
  `test_audit_router.py` fixtures gain a `_CtxAuth(["reliability-admin"])`.
- Rejected — **leave enforcement to `dependencies=`, document harder**: the docstring already
  said it and nobody in-tree (README `:4020`, `dead-letter-queues.md:449-468`,
  `database-auditing.md:329-343`) passes one; a required `server_auth` that six routes never call
  is precisely the "silently unauthenticated" state the row exists to end.

### §D-flip3-posture — `posture.admin_mount_unauthenticated` stays, documented as unreachable through varco's mounts

The id and the two collector booleans stay (ids are never removed; an out-of-tree caller who
builds their own router around varco's handlers can still feed them). `LocalPostureCollector`'s
docstring and the severity table say: *"since 4.0 no varco mount or builder can produce this state
(`ValueError`); the id remains for hand-wired surfaces."* Severity stays WARN. No new id for
FLIP-3/4 — the refusal is the finding.

### §D-flip10 — `include_detail` defaults `False`; `VARCO_ERROR_INCLUDE_DETAIL=true` is the one escape hatch

`varco_core/varco_core/exception/settings.py:72` → `include_detail: bool = False`. `error_message_for()`'s
logic (`http.py:331-332`) is unchanged; its comment (`:326-330`) is rewritten to state the 4.0
default and why the `RouteGuard` argument no longer applies (premise (5)). The route-handler path
(`exceptions.py:142-143`) and middleware path (`error.py:278-279`) still emit `detail` *when
present* — with the default it is `None` for every `ServiceException`, so the key is absent.

What a client sees, by exception, on the route-handler path (`add_exception_handlers`, the
`create_varco_app` default):

| Exception | 3.2 body (`detail` present) | 4.0 body | Actionable content lives in |
|---|---|---|---|
| `ServiceValidationError("start before end", field="start")` | `detail: "Validation error (field: 'start'): start before end"` | no `detail` | `params.detail`, `params.field` (`service.py:287-291`) |
| `ServiceConflictError("email taken")` | `detail: "email taken"` | no `detail` | `params.detail` |
| `ServiceNotFoundError(User, "u1")` | `detail: "…User… u1"` | no `detail` | `params.entity`, `params.entity_id` |
| `ServiceAuthorizationError("delete", Post)` | `detail: "Permission denied: 'delete' on Post."` | no `detail` | `params.operation`, `params.entity`; `reason` was never emitted |
| `RouteGuard` denial (`require_roles("admin")`) | `detail: "Permission denied: 'Missing required role(s): 'admin'' on str."` | no `detail` | `params.operation == "Missing required role(s): 'admin'"` (premise (5)) |
| An out-of-tree `ServiceException` with no `error_params()` override | `detail: str(exc)` | no `detail`, `params` absent (D-4) | the server log via `correlation_id` — the 035 §D-S3a answer; or override `error_params()` |

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: flip the default; nothing else about the envelope moves** | The row verbatim; 035 §D-S3b scheduled exactly this; brief 008's convention (application-authored strings never reach an error body by default); every built-in exception's actionable content is already structured in `params` (D-4, `include_params=True` default), so the *only* loss is for an out-of-tree exception that never implemented `error_params()` — which is the leak shape 035 named; one env var restores 3.2 byte-for-byte | Clients that parsed `detail` (a free-form string) must move to `params` (structured) or `message_key`; `RouteGuard`'s `params.entity == "str"` artefact (it passes `str` as `entity_cls`) becomes more visible — cosmetic, pre-existing, §Parked |
| (2) keep `True`, escalate `http.error.detail_exposed` to HIGH | No wire change | Every app blocks in every environment until it sets the variable — a flip by another name, with a worse default (the leak stays until someone acts). Rejected |
| (3) keep `detail` for `ServiceAuthorizationError` only | Preserves `RouteGuard` text verbatim | A per-class exception to a security default, and unnecessary — `params.operation` already carries it. Rejected |

**Posture (§D-flip10-posture):** `http.error.detail_exposed` stays WARN (035 OQ3 — now that the
knob is opt-in, WARN is right: an explicit opt-out of a security default needs an explicit
suppression, the tier 057 chose for `redaction.audit.disabled`). Detail text →
*"`VARCO_ERROR_INCLUDE_DETAIL=true` — the application-authored `str(exc)` is echoed in every error
body (off by default since 4.0)."*, remediation → *"Unset `VARCO_ERROR_INCLUDE_DETAIL`; if the
echo is deliberate, `VARCO_SECURITY_SUPPRESS=http.error.detail_exposed`."* Under the 4.0 default a
production app that opts back in refuses to start unless it also suppresses — two visible,
operator-owned statements, the 053 `posture.base_authorizer_bound` pattern.

### §D-flip10-parity — `ErrorMiddleware._service_error_response` emits `message_key`/`params` like the handler path

`error.py:271-279` becomes the four-key shape `exceptions.py:131-147` has had since Plan 011:
`code`, `message`, `detail` (if any), `message_key` (if any), `params` (if any). The RFC 9457
members are **not** added here (the middleware has no `settings.problem_details` read today and
adding one is 011's D-3 scope, not this plan's).

- ✅ Without this, FLIP-10 leaves the middleware path with `{code, message, correlation_id}` and
  nothing actionable — a regression for the (rare) `ServiceException` raised inside a middleware.
  ✅ Additive keys on the wire; `error_message_for()` already computes both. ✅ Three lines.
- ❌ A middleware-path body gains up to two keys — under 4.0 breaking-allowed and named in
  §Migration. Rejected — leaving the asymmetry: the parity is what makes "move from `detail` to
  `params`" a true statement for every path.

### §D-compose — 052's startup preflight and this plan's posture refusal never double-report

Disjoint by construction, pinned by tests (Step 4):

| Refusal | Runs | Input | Raises | Reports |
|---|---|---|---|---|
| 052 `run_wiring_preflight()` | after `async_init`, **before any `start()`** | providify `validate()` issues + `WiringFinding`s (*"never a posture finding"*, 052 `:210`) | `ConfigurationError` on `ERROR` | `WiringReport` (header: `enforce=<effective> (<source>)`) |
| this plan, `SecurityPostureModule.start()` (last in `default_modules()`, 049) | last `start()` | `PostureFinding`s | `RuntimeError` on `HIGH` / production `WARN` | `SecurityPosture` (one `SecurityPosture:` log line) |

- If the preflight refuses, the posture module never starts (`module.report` raises *"has not
  run yet"*) — one error, one log block. If the wiring is clean and a posture finding blocks, the
  `WiringReport` carries no finding with that id and the `RuntimeError` is raised once.
- `varco diagnose --check` never starts a module (052 `:381`) — posture findings never affect its
  exit code; its header shows `enforce=refuse (derived)` so an operator reading the report before
  deploy knows the consequence they are about to get.
- **052's one additive change (specified here, implemented wherever 052's report lives):**
  `WiringReport.enforce_source: Literal["code", "env", "default", "derived"]`; the report sets
  `enforce = settings.effective_enforce` and `enforce_source = "derived"` when
  `settings.enforce_is_derived`, else 048's `provenance_of(container, SecurityPostureSettings).fields["enforce"]`
  (which is `"code"`/`"env"`; `"default"` is now unreachable for this field but stays in the
  Literal for the schema). Renderer: `enforce=refuse (derived from VARCO_SECURITY_ENV=production)`.
  If 052 has not landed at implementation time, Step 4 leaves the note in `plans/052-…md`
  §D-report and this plan's Done criterion carries it.

### §D-snapshots — what must and must not be regenerated

| Artefact | Effect | Action |
|---|---|---|
| `design/api-freeze-and-standards/measurements/api-surface.{json,md}` | `mount_reliability_admin`, `mount_webhook_admin`, `build_webhook_router`, `build_dlq_router`, `build_audit_router` are **functions** — `--check` records their `inspect.signature()`, so `server_auth` losing its default **fails `make lint`** until the snapshot is regenerated. `SecurityPostureSettings`/`SecurityPosture`/`ErrorEnvelopeSettings` are classes — invisible to `--check` (CLAUDE.md "Known limitation"); they get `inspect.signature()`/field-default regression tests instead | Regenerate + commit in Step 9; `--check` clean at Step 14 |
| `design/api-freeze-and-standards/measurements/asyncapi-example.json` | none | not regenerated; `make asyncapi-check` unchanged |
| `design/async-performance-patterns/measurements/import-budget.json` | none (no new top-level import) | `--check --warn-only` no breach |
| 048's settings-reference snapshot (if landed) | `enforce` default `warn` → `None (derived)`; `include_detail` `True` → `False` | regenerate in Steps 3/11 |
| 047's DI catalogue snapshot | none | untouched |

## Steps

All commands run from `/home/edoardo/projects/varco`. Phase A (FLIP-2 + table) → Phase B
(FLIP-3/4) → Phase C (FLIP-10) → Docs → Verification. Phases are independent of each other; A
first because it changes what every other phase's posture test observes.

### Phase A — FLIP-2, the severity table, `http.rate_limit.absent`

1. [ ] **Preflight.** Record in `/tmp/claude-1000/-home-edoardo-projects-varco/625af1a2-0cf8-4199-b924-0b3f986861e6/scratchpad/054-preflight.txt`:
       every `file:line` in §Context re-read (`posture.py:154-196,326-393,671-702`, `admin/mount.py:46-123`,
       `dlq_router.py:47-191`, `audit_router.py:46-193`, `_tenant_scope.py:28-94`,
       `webhook/mount.py:37-108`, `webhook/router.py:67-103,147-200`, `tenancy/router.py:113-117`,
       `tenancy/mount.py:19-23`, `exception/settings.py:72`, `exception/http.py:296,331-332`,
       `exceptions.py:119-147`, `middleware/error.py:249-300`, `introspect.py:13-24,355-365`);
       which of 047 (`di.in_process_backend` emitter class name), 048 (`bind_settings`,
       `provenance_of`, the settings snapshot script), 049 (`SecurityPostureModule`,
       `default_modules`), 051 (`LocalPostureCollector`, `HttpEdgeCollector`,
       `varco_core.posture`), 052 (`WiringReport`, `build_wiring_report`, `run_wiring_preflight`,
       `InMemoryProfile`) and 053 (`posture.authorizer_unbound`, `PassthroughAuth(acknowledge_unverified=)`)
       are in the tree — each later step names its pre-/post- form. Measure the blast radius:
       `VARCO_SECURITY_ENV= VARCO_SECURITY_ENFORCE= uv run pytest varco_fastapi/tests -q -x -k "posture or app or lifespan"`
       with a **temporary** local `enforce="refuse"` default (revert immediately) → record which
       tests refuse and why (expected: `di.in_process_backend` on every lifespan-entering test
       post-047/049). Confirm pydantic-settings' `model_fields_set` behaviour for env-populated
       fields with a two-line check (needed by §D-flip2-message's source detection when 048's
       `provenance_of` is absent). **Verify**: the file exists with ≥ 16 confirmations and the
       measured refusal list.

2. [ ] `varco_fastapi/tests/test_posture_core.py` (red) — `test_settings_default_enforce_is_warn`
       (`:30-36`) → **replace** with `test_settings_default_enforce_is_none_and_derived`:
       `delenv` both variables → `settings.enforce is None`, `settings.effective_enforce == "refuse"`,
       `settings.enforce_is_derived is True`; **add** `test_development_derives_warn`
       (`setenv VARCO_SECURITY_ENV=development` → `"warn"`, derived), `test_explicit_env_enforce_wins`
       (`VARCO_SECURITY_ENFORCE=warn` + production → `"warn"`, not derived),
       `test_code_enforce_beats_env` (`SecurityPostureSettings(enforce="refuse", environment="development")`
       → `"refuse"`), `test_unrecognised_environment_derives_refuse` (`staging` → `"refuse"`),
       `test_enforce_field_default_is_none_signature_guard` (pydantic `model_fields["enforce"].default is None`
       — the class-signature guard `api_surface --check` cannot provide). Then the behavioural
       set, each with both variables `delenv`ed first (§D-flip2-tests) and one synthetic WARN
       collector: `test_warn_finding_refuses_in_production_by_default` (`pytest.raises(RuntimeError, match=r"enforce='refuse', derived from VARCO_SECURITY_ENV='production'")`,
       message also matches `test.some_warn` and `VARCO_SECURITY_ENFORCE=warn`);
       `test_same_warn_finding_starts_in_development` (`setenv development` → starts; the finding
       is present at `INFO`; `report.enforce == "warn"`); `test_env_warn_escape_hatch_starts`
       (`setenv VARCO_SECURITY_ENFORCE=warn` → starts, finding at `WARN`, `report.enforce == "warn"`);
       `test_suppressed_warn_starts` (`suppress="test.some_warn"` → starts);
       `test_high_refuses_in_every_environment` (parametrised `{production, development, staging}`
       with a HIGH collector → `RuntimeError` in production/staging; **development → starts**
       because derived `warn`); `test_staging_warn_does_not_block_but_is_not_demoted_and_is_reported`
       (`staging` + WARN → starts; finding stays `WARN`; `posture.unrecognised_environment` INFO
       present with `'staging'` in detail; `production`/`development` → that id absent);
       `test_unrecognised_enforce_value_is_reported_and_treated_as_warn` (`VARCO_SECURITY_ENFORCE=Refuse`
       + HIGH → starts; `posture.unrecognised_enforce` INFO present);
       `test_not_assessed_never_blocks_under_derived_refuse` (`:152-153`'s existing test re-run
       with the default instead of an explicit `refuse`); `test_summary_line_names_enforce`
       (`report.summary().endswith("enforce=refuse")`); `test_refuse_message_keeps_3_2_fragment`
       (`"refusing to start — unsuppressed blocking findings:"` in the message). Existing tests
       `:92-115` (suppression / unknown-suppression with WARN findings) gain
       `enforce="warn"` on their settings instance so they keep testing what they test.
       **Verify**: `uv run pytest varco_fastapi/tests/test_posture_core.py -q` → red on every new
       test except `test_code_enforce_beats_env` and the two existing `refuse` tests.

3. [ ] `varco_fastapi/varco_fastapi/posture.py` — §D-flip2-default's field + two properties (with
       `Attributes`/`Returns` docstrings and a *"DESIGN: sentinel-derived default, keyed on
       `!= "development"` (Plan 054 / FLIP-2)"* block carrying the ✅/❌ and the rejected literal
       default); `:378` reads `effective_enforce`; §D-flip2-env's two findings appended after the
       unknown-suppression block (`:343`); §D-flip2-message's `RuntimeError` text (source
       detection: 048's `provenance_of` when the instance is container-bound, else the
       `os.environ` check from Step 1); §D-flip2-report's `SecurityPosture.enforce` field +
       `summary()` suffix; module docstring `:15-18` (*"Never fails startup by default"*) rewritten
       to the 4.0 statement; `SecurityPostureSettings` docstring `:165-168`. Add the reader guard
       `varco_fastapi/tests/test_posture_enforce_readers.py::test_only_the_property_reads_the_raw_field`
       (`rg`-style scan of `varco_*/varco_*` for `\.enforce\b` outside `posture.py`'s property
       bodies and 052's provenance read → the allowed set is enumerated in the test). Then
       **`varco_fastapi/tests/conftest.py`** and **`examples/00-full-stack-post-api/example/tests/conftest.py`**
       gain `_posture_warn_only` (§D-flip2-tests, autouse, `monkeypatch.setenv("VARCO_SECURITY_ENFORCE", "warn")`,
       docstring naming FLIP-2 and the opt-out rule). If 048's settings snapshot exists,
       regenerate. **Verify**: Step 2 green; `make test PKG=varco_fastapi`; `make test PKG=examples`
       green **with** the fixture and the Step 1 refusal list empty.

4. [ ] **052 composition tests** — `varco_fastapi/tests/test_posture_wiring_compose.py` (new;
       skipped with a reason if 052's `run_wiring_preflight`/`build_wiring_report` are absent —
       recorded in Step 1): `test_wiring_error_refuses_before_posture_runs` — a container with a
       `@Singleton` whose `Inject[...]` is unbound + `SecurityPostureModule(collectors=[<HIGH collector>])`
       → entering the app's lifespan raises 052's `ConfigurationError`; the module's `.report`
       raises *"has not run yet"*; `caplog` has **no** `SecurityPosture:` line;
       `test_clean_wiring_then_posture_refuses_once` — clean container, same HIGH collector,
       variables `delenv`ed → `RuntimeError` naming the id exactly once; `caplog` has exactly one
       `SecurityPosture:` summary line; `app.state.varco_wiring_report.findings` contains no
       entry with that id; `test_wiring_report_shows_effective_enforce_and_provenance` —
       parametrised: (unset, unset) → `("refuse", "derived")`; (`production`, `VARCO_SECURITY_ENFORCE=warn`)
       → `("warn", "env")`; code instance via `bind_settings(... instance=SecurityPostureSettings(enforce="warn"))`
       → `("warn", "code")`; `development` → `("warn", "derived")`; the rendered header contains
       `enforce=refuse (derived from VARCO_SECURITY_ENV=production)`;
       `test_diagnose_check_ignores_posture_findings` — `varco diagnose --check` (via 052's
       `_run`) on the HIGH-collector app exits `0` (it never starts modules). Then the one 052
       edit (§D-compose): `WiringReport.enforce_source` Literal gains `"derived"`, the builder
       reads `effective_enforce`/`enforce_is_derived`, the renderer prints the derivation; 052's
       JSON schema doc (if any) gains the value. If 052 is absent, append the §D-compose
       paragraph to `plans/052-…md` §D-report as a one-line note. **Verify**: green (or skipped
       with the recorded reason).

5. [ ] **`di.in_process_backend` consequence + `http.rate_limit.absent` re-tier** (red first) —
       `varco_fastapi/tests/test_posture_severity_table.py` (new): `test_in_process_backend_blocks_production_start`
       — 047's collector (name from Step 1) over a container whose event-bus seam resolves to the
       in-memory default, variables `delenv`ed → `SecurityPostureLifecycle(collectors=[it]).start()`
       raises naming `di.in_process_backend`; `test_in_process_backend_is_info_in_development` —
       `setenv development` → starts, finding at `INFO`; `test_in_process_backend_suppressed_starts`;
       `test_rate_limit_absent_is_info` — `inspect_http_edge(FastAPI())` → the finding's
       `severity == "info"`; `test_default_app_edge_findings` — `test_http_edge_introspect.py:51-55`
       updated: ids `{"http.rate_limit.absent"}` only (detail_exposed gone after Step 11 — order
       Step 11 before running this, or mark it `xfail(strict=True)` until Phase C lands in the
       same PR); **`test_every_shipped_check_id_has_a_severity_table_row`** — collects every
       string literal matching `check="…"` across `varco_*/varco_*` (`rg -o 'check="[a-z_.]+"'`)
       plus the `http.*` ids from `introspect.py`'s docstring table, and asserts each appears in
       `technical_docs/features/security-posture.md`'s severity table (the doc is the register;
       `posture.not_assessed.*` matched by prefix) — the mechanical form of the table's "add a
       row" rule. Then `varco_fastapi/varco_fastapi/middleware/introspect.py`: the
       `http.rate_limit.absent` block's `severity="warn"` → `"info"`, remediation suffix, the
       docstring table `:19`; `http-edge-hardening.md` §S10 gains one sentence. **Verify**: green;
       `uv run pytest varco_fastapi/tests/test_http_edge_introspect.py -q`.

### Phase B — FLIP-3 / FLIP-4

6. [ ] `varco_fastapi/tests/test_mount_reliability_admin.py`, `test_mount_webhook_admin.py`,
       `test_dlq_router.py`, `test_audit_router.py`, `test_admin_tenant_guard.py` (red) — **add**:
       `test_mount_reliability_admin_refuses_server_auth_none` (`pytest.raises(ValueError, match="requires server_auth")`,
       message mentions `PassthroughAuth(acknowledge_unverified=True)`; **nothing mounted**:
       `app.routes` unchanged and a second call with auth succeeds — the WeakSet was not
       poisoned); `test_mount_reliability_admin_requires_server_auth_keyword` (`TypeError` when
       omitted; `inspect.signature(...).parameters["server_auth"].default is inspect.Parameter.empty`
       and `KEYWORD_ONLY`); the same pair for `mount_webhook_admin`, `build_webhook_router`,
       `build_dlq_router`, `build_audit_router`; `test_mount_reliability_admin_does_not_log_unauthenticated_warning`
       (`caplog`: no `UNAUTHENTICATED` record on a successful mount — the branch is gone);
       `test_acknowledge_check_still_runs_first` (`acknowledge_bundled_admin=False, server_auth=None`
       → the `acknowledge_bundled_admin` message, not the `server_auth` one — order preserved).
       §D-flip3-enforce tests in `test_dlq_router.py`/`test_audit_router.py`: parametrised over
       **every** route of each router (build the path list from `router.routes`, so a new route
       cannot dodge the guard): no credentials → the `server_auth` stub's own 401 (a stub raising
       `HTTPException(401)` when a marker header is absent); credentials without the role → 403
       `"'reliability-admin' role required."`; with the role → not 401/403;
       `test_server_auth_resolved_once_per_request` (a counting stub; `GET /entries` with the role
       → count 1 — `resolve_ctx` read `request.state`); `test_mount_dependencies_are_additive`
       (a `dependencies=[Depends(deny_all)]` still applies on top). `test_admin_tenant_guard.py:118-131`
       (the `ctx is None` cross-tenant test that mounted with `server_auth=None`) → rewrite
       against a stub `server_auth` returning `None` for the request, asserting
       `allow_cross_tenant(None, role) is False` still governs; add a direct unit test of
       `allow_cross_tenant(None, "x") is False`. **Verify**: red on the new refusal/guard tests.

7. [ ] Implementation — `varco_fastapi/varco_fastapi/admin/_tenant_scope.py`: `admin_guard()`
       (§D-flip3-enforce, docstring with `Args`/`Returns`/`Raises`/`Edge cases`: "a `server_auth`
       that returns `None` is an authenticated-but-anonymous caller — it fails the role check
       (403) and `allow_cross_tenant` stays `False`"); `resolve_ctx()` reads
       `request.state.varco_admin_ctx` first (docstring updated); module docstring's "shared
       cross-tenant resolution helper" → "shared auth + cross-tenant helpers". `admin/dlq_router.py`
       and `admin/audit_router.py`: `server_auth: Any` required keyword-only, `None` →
       `ValueError` (message names the builder), `APIRouter(..., dependencies=[Depends(admin_guard(server_auth, admin_role))])`,
       docstrings (`dlq_router.py:65-70` and the audit twin) rewritten: *"Resolved and enforced on
       every route (4.0, Plan 054 / FLIP-3); `admin_role` is required on every route; the caller's
       `dependencies=` are additive."* `admin/mount.py`: signature per §D-flip3 (keep the
       `acknowledge_bundled_admin` check first, then double-mount, then the new `server_auth`
       check — **before** any `include_router`, so a refusal never half-mounts and never poisons
       `_MOUNTED_APPS`); delete `:118-123`; docstring `Args: server_auth` → *"**Required.** …"*,
       `Raises` gains the `None` case, `Edge cases` first bullet replaced, a *"DESIGN: required
       `server_auth`, no acknowledgement kwarg (Plan 054 / FLIP-3)"* block with §D-flip3's ✅/❌
       and the rejected (ii). `webhook/mount.py` and `webhook/router.py`: the same for FLIP-4
       (`_enforce` drops the `if server_auth is not None:` branch; `_resolve_ctx`'s `None` branch
       `:79-80` becomes unreachable and is deleted); `webhook/router.py:20` module docstring
       sentence about `ctx is None` updated. `tenancy/mount.py:19-23` docstring: barrier 2 gains
       *"(since 4.0 the same rule holds for `mount_reliability_admin`/`mount_webhook_admin`)"*.
       **Verify**: Step 6 green.

8. [ ] In-repo sweep — every site in §Context's 34-site list gains `server_auth=<stub>`; the
       stub is one shared helper `varco_fastapi/tests/_admin_auth.py::ctx_auth(roles: list[str])`
       (moving `test_admin_tenant_guard.py:23-34`'s `_CtxAuth` there; a `Request`-taking variant
       that raises `HTTPException(401)` without an `X-Test-Auth` header for the per-route tests).
       Any doc snippet calling these functions (README `:4020-4021,4346`, `dead-letter-queues.md:449-468`,
       `database-auditing.md:329-343`, `outbound-webhooks.md:124`, `admin-surface-tenancy.md:50-51`,
       ARCHITECTURE.md `:1231`) gains `server_auth=my_auth` — listed again in Step 12.
       **Verify**: `make test PKG=varco_fastapi` green; `rg -n 'server_auth=None' varco_*/tests`
       → only the explicit `ValueError` tests.

9. [ ] `uv run python scripts/api_surface.py` — five function signatures change (`server_auth`
       loses its default; keyword-only order may reorder in the rendering). Commit the snapshot.
       **Verify**: `uv run python scripts/api_surface.py --check` clean; the `.md` diff shows
       exactly the five functions.

### Phase C — FLIP-10

10. [ ] `varco_core/tests/test_error_envelope_settings.py` (red) — `:22-25` → `test_include_detail_defaults_to_false`
        (`delenv VARCO_ERROR_INCLUDE_DETAIL`); **add** `test_env_true_restores_detail`
        (`setenv true` → `error_message_for(exc).detail == str(exc)`),
        `test_default_omits_detail_but_keeps_params` (a `ServiceValidationError("x", field="f")`
        → `detail is None`, `params == {"detail": "x", "field": "f"}`),
        `test_route_guard_denial_keeps_operation_in_params` (a `ServiceAuthorizationError("Missing required role(s): 'admin'", str)`
        → `params["operation"]` carries the text). `varco_fastapi/tests/test_error_envelope_http.py`
        (new; both paths, `delenv` first): handler path (`add_exception_handlers`) — a
        `ServiceValidationError` route → body has no `detail`, has `params.detail`; with
        `setenv true` → has `detail`; middleware path (`ErrorMiddleware` standalone, exception
        raised inside a tiny inner middleware) — body has `code`, `message`, `message_key`,
        `params`, `correlation_id`, **no** `detail` (§D-flip10-parity); with `setenv true` →
        `detail` present too; `test_http_exception_bodies_untouched` (`HTTPException(401, detail="x")`
        → `{"detail": "x"}` on both paths — proves the flip touches only the envelope);
        `test_debug_500_detail_untouched` (`ErrorMiddleware(debug=True)` → `detail` present on a
        500, unchanged). `auth/test_token_profile_guard.py:84` → tighten to
        `body["params"]["operation"]` (and assert `"detail" not in body`). **Verify**: red on the
        default/parity tests; `test_http_exception_bodies_untouched` and `test_debug_500_detail_untouched`
        pass today (controls).

11. [ ] Implementation — `varco_core/varco_core/exception/settings.py:72` → `False`; docstring
        `:42-50` → *"Default `False` (4.0, Plan 054 / FLIP-10). `true` restores the 3.x echo —
        reported by `inspect_http_edge()` as `http.error.detail_exposed` (WARN; blocks a production
        start under the default `enforce` unless suppressed). Every built-in `ServiceException`
        carries its actionable content in `params` (D-4); an out-of-tree subclass should override
        `error_params()` rather than re-enable this."* `exception/http.py:326-330` comment
        rewritten (premise (5)); `:277-280` `Edge cases` bullet updated. `varco_fastapi/varco_fastapi/exceptions.py:135-141`
        DEVIATION comment → a two-line note ("`detail` is emitted when present — `None` by default
        since 4.0; `params` carries the structured content, including `RouteGuard`'s
        `operation`"). `middleware/error.py:271-279` → §D-flip10-parity's four-key body (comment
        cites this plan). `middleware/introspect.py:355-365` → §D-flip10-posture's texts;
        docstring table `:23`. If 048's settings snapshot exists, regenerate. **Verify**: Step 10
        green; `make test PKG=varco_core`; `make test PKG=varco_fastapi` (the four unaffected
        `"detail"` tests in §Context green **unmodified**); `make test PKG=examples`.

### Docs (same commit as the code — feedback memory)

12. [ ] `technical_docs/features/security-posture.md` — env table `:111-115`: `VARCO_SECURITY_ENFORCE`
        default → *"unset → derived: `refuse` unless `VARCO_SECURITY_ENV=development`"*, meaning
        rewritten; §"`enforce="refuse"`" `:137-145` → *"`refuse` by default in production (4.0,
        Plan 054 / FLIP-2)"* with the matrix from §D-flip2-default, the message shape, and the
        three fixes; §"`VARCO_SECURITY_ENV` — presentation only" gains *"and the one derivation:
        `enforce` when unset"* (053 already appended "and the one binding it gates" — keep both);
        **new section "The severity table — every `check` id and its 4.0 consequence"** =
        §D-severity-table verbatim, with the header rule *"a new id must add a row here;
        `test_every_shipped_check_id_has_a_severity_table_row` enforces it"*; the collector table
        `:56-62` loses its severity hints (now one home); flip list `:152-167` rows 2/3/4/10 →
        *"**landed, Plan 054**"*, row 11 gains *"(`http.rate_limit.absent` re-tiered to INFO so the
        FLIP-2 default cannot flip it by the back door — §D-ratelimit)"*; §Pitfalls: replace the
        "opt-in in 3.2" row with *"Production app refuses to start with `di.in_process_backend`
        → every in-process seam is a finding under the 4.0 default → `VARCO_<SEAM>_BACKEND=` per
        seam, or suppress for a deliberate single-process deployment"*; *"`VARCO_SECURITY_ENV=staging`
        → strict enforcement but no WARN blocking and no demotion; `posture.unrecognised_environment`
        says so → use exactly `production`/`development`"*; *"`VARCO_SECURITY_ENFORCE=warn` in a
        3.2 `.env` you forgot about → the 4.0 refusal never fires → `varco diagnose` header shows
        `enforce=warn (env)`"*; *"`SecurityPostureSettings(environment='development')` in code →
        derives `warn` for this process only; the env var is what your deployment reads"*.
      - `technical_docs/features/http-edge-hardening.md` — §S3 `:258-273`: `include_detail`
        default `False` (4.0), the exception-by-exception table from §D-flip10, the parity
        change, `VARCO_ERROR_INCLUDE_DETAIL=true` as the one hatch; §S10 one sentence on
        `http.rate_limit.absent` → INFO; §Pitfalls `:317`: *"Client parsed `detail` → key absent
        since 4.0 → read `params` (`ServiceValidationError.detail`/`field`,
        `ServiceAuthorizationError.operation`) or `message_key`; or `VARCO_ERROR_INCLUDE_DETAIL=true`
        and expect `http.error.detail_exposed`"*; *"Out-of-tree `ServiceException` lost its text →
        override `error_params()`"*.
      - `technical_docs/features/error-taxonomy-and-i18n.md` `:21` (default), `:205`, `:230`
        (the "unlike `include_detail`, which is warn-only" clause → "which flipped in 4.0").
      - `technical_docs/features/admin-surface-tenancy.md` §D-S4-role `:45-62` (*"`ctx is None`
        ⇒ `False`"* stays, with *"reachable only through a `server_auth` that returns `None` —
        no mount can be unauthenticated since 4.0"*); §D-S4-scope `:87` gains the every-route
        `admin_role` enforcement; §Pitfalls `:104+`: replace the "mounting unauthenticated and
        expecting cross-tenant" row with *"`mount_*(server_auth=None)` → `ValueError` (4.0, FLIP-3/4)
        → pass a strategy; behind a verifying gateway, `PassthroughAuth(acknowledge_unverified=True)`"*
        and *"`admin_role` is enforced on every reliability route since 4.0 — a `dependencies=`
        that used to be the only gate is now additive"*.
      - `technical_docs/features/dead-letter-queues.md` `:449-493`, `database-auditing.md`
        `:329-343,443`, `outbound-webhooks.md` `:124-153`: snippets gain `server_auth=`, the
        "`server_auth=None` mounts unauthenticated and logs" sentences → "refuses (`ValueError`)".
      - `README.md` — `:924` env row (`false`, meaning rewritten); `:4020-4021` and `:4346`
        snippets; `:4849-4870` posture section (`SecurityPostureSettings()` comment → "reads
        `VARCO_SECURITY_*`; `enforce` derives `refuse` in production since 4.0"), `:4868` env row;
        a "What a stock production app must set" sentence under the posture section pointing at
        the severity table.
      - `ARCHITECTURE.md` `:1231` (`mount_webhook_admin(app, *, repository=, server_auth=, …)`)
        and the reliability twin (locate: `rg -n 'mount_reliability_admin' ARCHITECTURE.md`).
      - `CLAUDE.md` — §"Admin surface tenancy…" Rules: **add** *"`mount_reliability_admin`,
        `mount_webhook_admin` and their three builders require `server_auth` — `None` is a
        `ValueError` with no escape hatch (Plan 054 / FLIP-3/4); never add an
        `acknowledge_unauthenticated` kwarg — the gateway case is `PassthroughAuth(acknowledge_unverified=True)`"*;
        §"HTTP edge hardening" Rules: **add** *"`ErrorEnvelopeSettings.include_detail` is `False`
        (Plan 054 / FLIP-10); actionable content belongs in `error_params()`, never in `str(exc)`"*;
        §"Security posture…": **add** *"`enforce` derives `refuse` unless `VARCO_SECURITY_ENV=development`
        (Plan 054 / FLIP-2); a WARN id is a production start-blocker — check the severity table
        in `security-posture.md` before assigning WARN to a new finding, and add the row"*. One
        line each — link, don't inline.
      **Verify**: `rg -n 'warn-only|4\.0 flip candidate|mounts unauthenticated|UNAUTHENTICATED' README.md technical_docs/ varco_*/varco_*`
      → 0 hits outside CHANGELOG history; `rg -n 'server_auth=None' README.md technical_docs/ examples/`
      → 0.

13. [ ] `CHANGELOG.md` — under `## [Unreleased]` (`:10`), above the existing blocks:
        `### BREAKING — HTTP-edge and admin-surface default flips (Plan 054, FLIP-2 / FLIP-3 / FLIP-4 / FLIP-10)`
        with bullets: FLIP-2 (default-before `warn`, default-after derived `refuse` unless
        `development`; the exact `RuntimeError` fragment; `VARCO_SECURITY_ENFORCE=warn`;
        `posture.unrecognised_environment`/`posture.unrecognised_enforce` INFO added;
        `SecurityPosture.enforce` field added; **`http.rate_limit.absent` WARN → INFO** with the
        §D-ratelimit reason; the severity table's location); FLIP-3/4 (`server_auth` required on
        five functions, `ValueError` on `None`, `TypeError` when omitted; `admin_role` now
        enforced on every reliability route; the deleted warning;
        `PassthroughAuth(acknowledge_unverified=True)` for the gateway case); FLIP-10
        (`include_detail=False`; `VARCO_ERROR_INCLUDE_DETAIL=true`; the middleware path gains
        `message_key`/`params`; `http.error.detail_exposed` text). End with *"Migration: this
        plan's §Migration (assembled into the 4.0 guide by Plan 058)."* `BACKLOG.md:68,71,82,83`
        — **only if the launching thread has not already done it**: status → `done (Plan 054)`;
        `:221` → *"three barriers per mount, uniformly, since Plan 054"*. **Verify**:
        `rg -n 'Plan 054' CHANGELOG.md BACKLOG.md` → ≥ 1 / ≥ 4.

### Verification

14. [ ] `make test` (all eleven suites — the autouse fixture keeps the default app booting);
        `make lint` (no-`PKG` path: `api-check` clean **with the regenerated snapshot**,
        `asyncapi-check` unchanged, `import-budget --warn-only` no breach); `make type-check`
        clean (no new `type: ignore`); `rg -n '_posture_warn_only' varco_*/tests examples/` → the
        two conftests only; `rg -n 'VARCO_SECURITY_ENFORCE' varco_*/tests` → the two fixtures plus
        the explicit `delenv`/`setenv` sites in `test_posture_core.py`/`test_posture_wiring_compose.py`/
        `test_posture_severity_table.py`; `git diff --stat -- design/api-freeze-and-standards/measurements/asyncapi-example.json`
        → empty; the Step 1 refusal list re-run **without** the fixture reproduces the
        `di.in_process_backend` refusals (the proof the flip is real), then green with it.

## Edge cases

- **Both variables unset in CI** → derived `refuse`, production rules; every lifespan-entering
  test relies on the autouse fixture — a test that deletes it must set its own expectations.
- **`VARCO_SECURITY_ENFORCE=refuse` explicitly + `development`** → `refuse`, `HIGH` blocks, `WARN`
  does not (not `production`) and is demoted — identical to 3.2's opt-in in development; pinned.
- **`VARCO_SECURITY_ENFORCE=Refuse` / `REFUSE` / `strict`** → `effective_enforce` returns it
  verbatim, `:378`'s `== "refuse"` is `False` → treated as `warn`, and
  `posture.unrecognised_enforce` (INFO) names the value (§D-flip2-env). Never normalised.
- **A `SecurityPostureSettings` instance constructed in 3.2 code with `enforce="warn"`** →
  identical behaviour; `enforce_is_derived is False`; report says `(code)`.
- **`staging` + a HIGH finding** → refuses (derived `refuse`); the message says
  `derived from VARCO_SECURITY_ENV='staging'`; `posture.unrecognised_environment` is in the same
  report.
- **Suppressing a HIGH id in production** → suppression demotes to INFO and clears it from the
  blocking predicate (`:382`) — a suppressed HIGH does not block, as in 3.2. Documented in the
  severity table's header: suppression is an operator statement, for any tier.
- **`mount_reliability_admin(app, server_auth=None, dlq=…, acknowledge_bundled_admin=False)`** →
  the acknowledgement `ValueError`, not the `server_auth` one (order pinned).
- **`mount_reliability_admin(app, server_auth=X)` with neither `audit_repo` nor `dlq`** → mounts
  nothing, does not poison the WeakSet (the pre-existing "mount nothing" case, unchanged, still
  requires a non-`None` `server_auth` — a guard that can never be satisfied is refused even when
  it would guard nothing).
- **A `server_auth` that returns `None`** (anonymous-allowing strategy) → `admin_guard` 403s
  (no roles), `allow_cross_tenant` stays `False`; the cross-tenant fail-closed property of
  §D-S4-role is preserved through the only remaining path.
- **A `server_auth` that raises `HTTPException(401)`** → 401 from the router-level dependency
  before parameter validation — a malformed request from an unauthenticated caller is 401, not
  422 (§D-flip3-enforce ✅/❌).
- **`ErrorEnvelopeSettings(include_detail=True)` passed explicitly to `error_message_for()`** →
  `detail` present, no env read; unchanged.
- **`VARCO_ERROR_INCLUDE_DETAIL=true` in a 3.2 `.env` loaded by the app** → the echo is on,
  `http.error.detail_exposed` WARN fires, and in production under the derived `refuse` the app
  **refuses to start** — the escape hatch of one flip trips the other; the §Migration row and
  the Pitfalls row say so, and the fix is one suppression or unsetting the variable.
- **A `ServiceException` raised inside `RequestContextMiddleware`** (middleware path) → the body
  now carries `message_key`/`params` (§D-flip10-parity); `correlation_id` unchanged.
- **`HTTPException` bodies, `QueryException` bodies, `debug=True` 500 bodies** → untouched (three
  controls in Step 10).
- **An operator alert keyed on the 3.2 `RuntimeError` text** → the `refusing to start —
  unsuppressed blocking findings:` fragment is preserved verbatim (pinned).

## Migration (3.x → 4.0; Plan 058 assembles this section verbatim)

| Before (3.2) | After (4.0) | Action if you relied on the old behaviour |
|---|---|---|
| `SecurityPosture` warned and started (`enforce="warn"` default) | With `VARCO_SECURITY_ENV` unset or `production`: `enforce` derives `refuse` — one `HIGH` finding anywhere, or one unsuppressed `WARN`, aborts startup: `RuntimeError: SecurityPostureLifecycle(enforce='refuse', derived from VARCO_SECURITY_ENV='production'): refusing to start — unsuppressed blocking findings: <ids>`. **A stock app on in-process defaults refuses with `di.in_process_backend`.** `development` derives `warn`; any other value derives `refuse` and is reported as `posture.unrecognised_environment` | Fix each id (the `SecurityPosture[...]` log lines name the remediation; the severity table in `security-posture.md` lists every id and whether it blocks). Knowingly accepted: `VARCO_SECURITY_SUPPRESS=<id>,<id>`. Restore 3.2 wholesale: `VARCO_SECURITY_ENFORCE=warn` (or `SecurityPostureSettings(enforce="warn")` in code). Dev/tests: `VARCO_SECURITY_ENV=development` |
| `http.rate_limit.absent` was WARN | INFO — rate limiting stays opt-in (036 row 11) and the new default must not turn it into a start-blocker | Nothing. If you gated on it via `enforce=refuse`, gate on the report yourself (`SecurityPosture` is data) |
| `mount_reliability_admin(app, dlq=…, acknowledge_bundled_admin=True)` mounted **unauthenticated** with one warning; `admin_role` was documentation unless you passed `dependencies=` | `server_auth` is a required keyword on `mount_reliability_admin`, `mount_webhook_admin`, `build_dlq_router`, `build_audit_router`, `build_webhook_router`: omitted → `TypeError`; `None` → `ValueError: … requires server_auth …`. Every reliability route now resolves `server_auth` and enforces `admin_role` (403 `'reliability-admin' role required.`) | Pass `server_auth=<your AbstractServerAuth / callable>`; grant `admin_role` (and `cross_tenant_role` where needed) on the admin token. Behind a gateway that already verified the caller: `server_auth=PassthroughAuth(acknowledge_unverified=True)` (expect `auth.passthrough_bound`, suppress if deliberate). A `dependencies=` you used as the only gate keeps working, additively |
| `posture.admin_mount_unauthenticated` could fire | Unreachable through varco's mounts (they refuse); the id remains for hand-wired surfaces | Nothing |
| Every mapped `ServiceException` body carried `detail: str(exc)` | No `detail` key. The structured content is where it already was: `params` (`ServiceValidationError` → `detail`/`field`; `ServiceConflictError` → `detail`; `ServiceNotFoundError` → `entity`/`entity_id`; `ServiceAuthorizationError` and every `RouteGuard` denial → `operation`), plus `message_key`. The `ErrorMiddleware` path now emits `message_key`/`params` too | Read `params`/`message_key`. Out-of-tree exception: override `error_params()`. Restore 3.2 byte-for-byte: `VARCO_ERROR_INCLUDE_DETAIL=true` — reported as `http.error.detail_exposed` (WARN), which **blocks a production start** under the new default unless `VARCO_SECURITY_SUPPRESS=http.error.detail_exposed` |
| `SecurityPosture(environment, findings, not_assessed)` | Gains a trailing `enforce: str` field; `summary()` ends with `· enforce=<value>` | Nothing (additive); a summary-line parser matching the exact tail must accept the suffix |

## Docs

Listed in Step 12 (feature docs, README, ARCHITECTURE, CLAUDE.md) and Step 13 (CHANGELOG,
BACKLOG). No new `technical_docs/features/*.md` — the severity table is a new **section** of
`security-posture.md`, the doc that already owns the ids. Plan 058 folds §Migration into the
consolidated guide.

## Verification

```bash
# Preflight (Step 1) — the blast radius, measured once with a temporary local refuse default
VARCO_SECURITY_ENV= VARCO_SECURITY_ENFORCE= uv run pytest varco_fastapi/tests -q -x -k "posture or app or lifespan"

# Phase gates
uv run pytest varco_fastapi/tests/test_posture_core.py varco_fastapi/tests/test_posture_wiring_compose.py \
              varco_fastapi/tests/test_posture_severity_table.py varco_fastapi/tests/test_http_edge_introspect.py -q   # A
uv run pytest varco_fastapi/tests/test_mount_reliability_admin.py varco_fastapi/tests/test_mount_webhook_admin.py \
              varco_fastapi/tests/test_dlq_router.py varco_fastapi/tests/test_audit_router.py \
              varco_fastapi/tests/test_admin_tenant_guard.py varco_fastapi/tests/test_tenant_router.py -q            # B
uv run pytest varco_core/tests/test_error_envelope_settings.py varco_fastapi/tests/test_error_envelope_http.py \
              varco_fastapi/tests/auth/test_token_profile_guard.py varco_fastapi/tests/milestone_a/test_middleware.py \
              varco_fastapi/tests/milestone_f/test_mcp_auth_middleware.py -q                                          # C

# Package + gates
make test
uv run python scripts/api_surface.py            # regenerate: five function signatures (server_auth required)
make lint                                        # api-check clean / asyncapi-check unchanged / import-budget no breach
make type-check

# Boundary checks
rg -n '_posture_warn_only' varco_*/tests examples/                       # exactly two conftests
rg -n 'server_auth=None' varco_*/varco_* README.md technical_docs/ examples/   # 0 in prod code/docs
rg -n 'UNAUTHENTICATED' varco_*/varco_*                                  # 0
rg -n 'include_detail: bool = ' varco_core/varco_core/exception/settings.py   # = False
rg -n '\.enforce\b' varco_*/varco_* | rg -v 'effective_enforce|enforce_is_derived|posture.py'   # only 052's provenance read
git diff --stat -- design/api-freeze-and-standards/measurements/asyncapi-example.json   # empty
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| **A 4.0 production deployment on in-process defaults stops booting** — the row's purpose and the widest behaviour break in this plan | The refusal is loud, at startup, names every id and all three fixes; `VARCO_SECURITY_ENFORCE=warn` restores 3.2 in one line | §Migration row 1 first; CHANGELOG; the severity table; `varco diagnose` shows `enforce=refuse (derived …)` before deploy |
| **A recorded non-flip becomes a start-blocker via WARN** | Every WARN id in the severity table is a hole or a misconfiguration, never a non-adoption of an opt-in feature | §D-ratelimit re-tiers the one such id; the table's column 4 is reviewed row by row at Step 12 |
| ⚠️ ASSUMPTION — 047's collector emits `di.in_process_backend` for *explicitly selected* `memory` as well as for the unset default | The severity table's "escape" cell is right | Step 1 reads 047's emitter; if explicit `memory` is silent, the cell says "select `memory` explicitly, or suppress" |
| ⚠️ ASSUMPTION — post-049 every lifespan-entering test runs `SecurityPostureModule` and post-047 emits `di.in_process_backend` | The autouse fixture is necessary and sufficient | Step 1 measures the refusal list with a temporary local default; Step 14 re-runs it without the fixture |
| ⚠️ ASSUMPTION — 052's `enforce_source` is a plain `Literal` the report builder sets from 048's `provenance_of()` | Adding `"derived"` is one value + one branch | Step 4 reads 052's implementation; if provenance is computed differently, the branch lives wherever `enforce` is read and the test still asserts the four tuples |
| ⚠️ ASSUMPTION — pydantic-settings puts env-populated fields into `model_fields_set` (so `"env"` vs `"code"` cannot be told apart from it alone) | §D-flip2-message's source detection is correct without 048 | Step 1's two-line check; the fallback is the `os.environ` comparison, and 048's `provenance_of` is preferred when the instance is container-bound |
| `admin_role` enforcement on every reliability route breaks an integration that relied on `dependencies=`-only gating with a role-less token | 403 is the correct answer for a caller without the role on a privileged surface | §Migration row 3; the additive `dependencies=` keeps their extra gate |
| `api_surface.py --check` red after Step 7 | Snapshot regenerated with exactly five signature changes | Step 9; Step 14's `make lint` |
| Two flips' escape hatches interact (`VARCO_ERROR_INCLUDE_DETAIL=true` trips FLIP-2's refusal) | Each is independently visible: the WARN names the variable, the refusal names the WARN | Edge cases; Pitfalls rows in two docs |
| The `RuntimeError` text changes break an operator's log alert | The 3.2 fragment survives verbatim | `test_refuse_message_keeps_3_2_fragment` |
| Docs drift — README/feature docs still show `server_auth=None` semantics or "warn-only" for `include_detail` | No surviving 3.2 claim | Step 12's two `rg` checks |

## Done

- [ ] `SecurityPostureSettings.enforce: str | None = None`; `effective_enforce`/`enforce_is_derived`
      properties; the lifecycle reads the property; `RuntimeError` names derivation + fixes and
      keeps the 3.2 fragment; `SecurityPosture.enforce`; `posture.unrecognised_environment` and
      `posture.unrecognised_enforce` (INFO); all Step 2 tests green.
- [ ] Two autouse `_posture_warn_only` fixtures; the FLIP-2 tests opt out explicitly; the Step 1
      refusal list reproduces without the fixture and is empty with it.
- [ ] 052 composition tests green (or skipped with the recorded reason); `WiringReport.enforce_source`
      accepts `"derived"` and the header renders the derivation.
- [ ] `http.rate_limit.absent` is INFO; the severity table exists in `security-posture.md` with
      every shipped id; `test_every_shipped_check_id_has_a_severity_table_row` green.
- [ ] `server_auth` required on the two mounts and three builders; `None` → `ValueError`; every
      reliability route resolves it and enforces `admin_role`; the unauthenticated warnings are
      gone; 34 sites swept; api-surface snapshot regenerated and `--check` clean.
- [ ] `include_detail=False`; middleware path emits `message_key`/`params`; the three
      "untouched body" controls green unmodified; `http.error.detail_exposed` texts updated.
- [ ] Docs per Step 12; CHANGELOG per Step 13; BACKLOG rows `FLIP-2`, `FLIP-3`, `FLIP-4`,
      `FLIP-10` → `done (Plan 054)`.
- [ ] `make test`, `make lint`, `make type-check` green; AsyncAPI snapshot untouched.

## Out of scope / Parked

- **`Literal` types for `SecurityPostureSettings.environment`/`enforce`** — 048's recipe; this
  plan's two INFO findings are the interim safety net and can be deleted when the `Literal`
  lands (an invalid value then fails at construction).
- **`QueryException`'s `f"Invalid query: {exc}"` echo on the middleware path** (`error.py:190-197,238-245`)
  — a parser message about the caller's own query string, not `include_detail`'s scope; revisit if
  a parser message ever carries server-side data.
- **RFC 9457 members on the middleware path** — 011's D-3; the parity change here is
  `message_key`/`params` only.
- **`RouteGuard` passing `str` as `entity_cls`** (`auth/guard.py:175,187,199,214,223,234`) so
  `params.entity == "str"` — cosmetic, pre-existing; a `ServiceAuthorizationError(operation=…,
  entity_cls=None)` call would fix it; not a flip.
- **A "you have not adopted X" tier below WARN** — INFO already is that tier; no new severity.
- **An `acknowledge_unauthenticated` kwarg** — rejected in §D-flip3 (ii), recorded so it is not
  re-proposed.
- **Escalating `posture.admin_mount_unauthenticated` to HIGH** — unreachable through varco's
  APIs; escalating an unreachable id is noise.
