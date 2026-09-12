# Plan 040 — Unified redaction seam (S21)

Covers BACKLOG 3.2-extension row **S21** (🟡 should, M — *one `Redactor` behind spans, logs, audit
payloads, and `error_params()`*), `BACKLOG.md:82`.

**No research brief backs this row.** The incumbent implementation is the evidence. Every claim
below about `varco_core/observability/params.py`, `varco_core/service/audit.py`,
`varco_core/exception/**` or `varco_fastapi/middleware/logging.py` carries a `file:line` citation
that was opened and read while writing this plan. Anything not verified from source is marked
`⚠️ ASSUMPTION` in §Risks and nowhere else.

⚠️ **Read §D-S21-exists first.** The row's premise is *narrower than its sentence*: of the three
"undefended" surfaces it names, exactly **one** has a live, unredacted user-data leak today (the
audit trail's before/after JSON, which additionally documents a redaction hook that **does not
exist**). `error_params()` is safe for every in-tree exception and advisory only for out-of-tree
ones; the request-logging middleware emits **no user data at all** today. The plan is shaped around
those findings, not around the row's sentence.

## Scope and siblings

One of five plans covering the 3.2 extension rows `S17`, `S19`–`S23` (`BACKLOG.md:77-82`).

| Plan | Rows | Boundary with this plan |
|---|---|---|
| 038 | S19 — inbound webhook verification | ✅ **Already planned.** It states at `plans/038-inbound-webhook-verification.md:22` that **040 owns `varco_core.redaction` and the redaction mechanism**, that 038 designs *no* redaction, and that its only obligation is to not introduce a leak (its §D-S19-secret, `:344`, `:361`, `:588`). **This plan honours that exactly**: it touches **no** file under `varco_core/webhook/` or `varco_fastapi/webhook/`, adds no verifier, and its answer to the adjacency is one word in a pattern tuple — `"signature"` lands in the **opt-in** `EXTENDED_REDACT_PATTERNS`, never in a default (§D-S21-patterns). ⛔ No value-prefix matching (`whsec_`, `sk_live_`) — §D-S21-shape rejects value scanning outright |
| 039 | S20 — retention & purge automation | ⚠️ **Real adjacency, one shared file.** 039 states at `plans/039-retention-and-purge-automation.md:24` and `:947` that it **adds nothing** to `varco_core/varco_core/service/audit.py` — it only calls the shipped `delete_where` — and that *"040 owns what goes in, 039 owns what goes out (by age)."* **Stated from this side too: this plan changes only the write path** (`AuditLogMixin._after_create`/`_after_update`, `audit.py:512-606`) and the stale docstring at `:527-529`. It does **not** touch `AuditRepository.delete_where` (`:339-382`), `list`/`list_for_entity` (`:267-337`), `verify_chain` (`:384-444`), or anything under `varco_core/retention/`. `git diff` on this plan's branch must show **zero** lines changed inside `audit.py:267-444` |
| 041 | S17 + S22 — `MetricsMiddleware` ordering, JWKS background refresh | ⚠️ 041 owns middleware **ordering** in `varco_fastapi/varco_fastapi/app.py` and the normative ordering table in `varco_fastapi/varco_fastapi/middleware/__init__.py:14-36`. This plan adds **one constructor keyword** to `RequestLoggingMiddleware` and **nothing else** in that package: ⛔ **zero diff in `app.py`**, ⛔ **zero diff in `middleware/__init__.py`'s ordering table**, no new `create_varco_app` keyword, no `add_middleware` call moved. This is a **DoD item** (§Verification, DoD 6), the precedent set by 038 |
| 042 | S23 — conformance-guard recovery | Touches `testkit/` + CLAUDE.md only. This plan adds a new public seam (`Redactor`, a Protocol) and therefore owes `testkit/varco_conformance/COVERAGE.md` a **note row** explaining why it gets no suite (§D-S21-conformance). **042 does not write it — this plan does**, the precedent 039 set at its `:26`. ⚠️ Both plans edit `COVERAGE.md`; see Risks |

**Position in the build order:** independent of all four siblings. Its files are
`varco_core/varco_core/redaction/**` (new), `varco_core/varco_core/observability/params.py`,
`varco_core/varco_core/exception/{settings.py,http.py,service.py}`,
`varco_core/varco_core/service/audit.py` (write path only),
`varco_fastapi/varco_fastapi/middleware/logging.py`, `benchmarks/bench_redaction.py`, docs.
The only file any sibling also edits is `COVERAGE.md` (039, 042).

---

## Goal

`varco_core.redaction` exists as one small public seam — a one-method `Redactor` Protocol, a frozen
`RedactionPolicy`, a `PolicyRedactor` default carrying the *unchanged* incumbent pattern list, and a
depth-and-cycle-safe `redact_mapping()` walk. Span parameter capture keeps behaving **byte-for-byte
as it does today** while sourcing its rule from that seam instead of owning it. `error_params()`
becomes **mechanically** safe against the two leak shapes CLAUDE.md can only warn about today — a
secret-named key, and a `vars(exc)` dump of live objects. The audit trail gains the redaction hook
its own docstring has been promising to a method that does not exist. And the request-logging
middleware gains the one thing it lacks: something for a body-logging subclass to call.

## Non-goals

- **No new runtime dependency, in `varco_core` or anywhere.** Stdlib only — `dataclasses`,
  `typing`, `logging`, `functools`, and `urllib.parse` (imported inside the one function that needs
  it). ⛔ No `regex`, no `presidio`, no `scrubadub`.
- **⛔ No value scanning, no content classification, no regex over payload strings.** Redaction is
  **key-name-based only** in 3.2 (§D-S21-shape). No credit-card Luhn check, no JWT-shape detector,
  no `whsec_`/`sk_live_` prefix match. Parked with a trigger.
- **No change to what span capture redacts.** `DEFAULT_REDACT_PATTERNS` keeps its exact 15 entries
  (`params.py:92-108`), its exact substring semantics (`params.py:307-309`), and its exact
  `"[REDACTED]"` placeholder (`params.py:110`). Phase 1 is a pure extraction (§D-S21-compat).
- **No change to `sanitize_value`.** It is *value rendering*, not redaction — a different contract
  with one consumer. Moving it would be churn with no beneficiary (§D-S21-compat).
- **No audit-payload redaction by default.** Opt-in in 3.2, for a reason that is itself a finding
  (§D-S21-audit, §D-S21-falsepos). ⛔ **And never on the read path** (§D-S21-hashchain).
- **No encryption, no tokenization, no reversible masking.** Redaction destroys. varco already
  ships the reversible answer — `FieldEncryptor` / `MultiKeyEncryptorRegistry` / crypto-shredding
  (CLAUDE.md §Field-level encryption). A `RedactionPolicy` that could be un-applied would be an
  encryption key management problem wearing a redaction costume.
- **No DI verb** (`bind_redactor`, `enable_redaction`) in 3.2 — no varco component *injects* a
  `Redactor`, so a binding would be a name nobody resolves (§D-S21-di). ⛔ And under no
  circumstances a module-level `@Singleton`/`@Provider`/`@Configuration` in `varco_core.redaction`
  — the standing `varco_core.tls` / `cloudevents` scan rule.
- **No wiring into Plan 036's `SecurityPosture` harness.** This plan defines and exports a pure
  `inspect_redaction_posture()`; it does **not** add a collector to
  `varco_fastapi/varco_fastapi/posture.py:485-573` (§D-S21-posture).
- **No DLQ / outbox / webhook-delivery payload redaction.** `AbstractDeadLetterQueue` stores the
  serialized `Event` and `WebhookDelivery` stores request bodies; both are real fifth and sixth
  surfaces and both are **parked with triggers**, not silently included.
- **No fix for the incumbent pattern list's false positives.** `"pin"` matches `shipping`; `"auth"`
  matches `author` (§D-S21-falsepos). Found, evidenced, and **filed to BACKLOG as a question**, not
  fixed inside an unrelated plan — the discipline `plans/035-http-edge-hardening.md:162-184`
  established.

---

## Design

### §D-S21-exists — what already exists, verified against source while writing this plan

Every anchor below was opened and read. The rows marked ⚠️ are **corrections to the BACKLOG row's
wording**; the row marked 🔴 is the one live leak.

| Fact | Location | Consequence |
|---|---|---|
| `DEFAULT_REDACT_PATTERNS` — exactly **15** case-insensitive **substring** patterns, matched against the **parameter name only**, never the value | `varco_core/varco_core/observability/params.py:92-108` | The incumbent. Phase 1 moves this tuple and changes not one character of it |
| The matcher is three lines: `lname = name.lower(); any(p.lower() in lname for p in config.redact_patterns)` | `params.py:307-309` | Substring, not word, not regex. Drives §D-S21-falsepos |
| Placeholder is `"[REDACTED]"`, a module private | `params.py:110`, used at `:186`, `:344` | Preserved exactly |
| Redaction is **fail-closed over `include=`** — a redacted name wins even when explicitly allow-listed | `params.py:90-91` (comment), `:343-344`; test `varco_core/tests/test_observability_params.py:227-240` | The seam must preserve "redaction is not an opt-out you can override" |
| Redaction and value-rendering are already **separate**: `_is_redacted` decides, `sanitize_value` renders, `_render_captured` composes them | `params.py:307-309`, `:211-265`, `:320-354` | The extraction is clean because the incumbent already split the two concerns. Only the *decision* half moves |
| `sanitize_value` **never raises** — a broken `__repr__` yields `"<unrepresentable>"` | `params.py:262-265` | The house fail-safe convention this plan inherits (§D-S21-failsafe) |
| `render_captured_params` swallows any failure and returns `{}` — *drop data, never emit it* | `params.py:381-388` | Same |
| ⚠️ `DEFAULT_REDACT_PATTERNS` and `sanitize_value` are exported from **`params.py`'s** `__all__` (`:684-697`) but **NOT** from `varco_core.observability.__all__` (which re-exports only `ParamCaptureConfig` and `set_param_capture_defaults`, `observability/__init__.py:129`, `:131`) and **NOT** from `varco_core.__all__` (absent from `design/api-freeze-and-standards/measurements/api-surface.md`) | grep, three files | The surface to preserve is the **module path** `varco_core.observability.params`, and the `api_surface.py` gate is blind to this move. Preserving it is a correctness obligation, not a gate-driven one (§D-S21-compat) |
| **Redaction exists in exactly one place** — repo-wide `rg 'redact\|REDACTED' varco_*/varco_*/**.py` hits only `observability/params.py` and one docstring line in `observability/__init__.py:44` | grep, all ten packages | The row's core premise is **confirmed** |
| 🔴 **The one live leak.** `AuditLogMixin._after_create` writes `diff=read_dto.model_dump()` — the **entire** read DTO, unredacted; `_after_update` writes `diff={"before": …, "after": …}`, two full dumps | `varco_core/varco_core/service/audit.py:538`, `:572-575` | A `password_hash`, `api_key` or `totp_secret` on a read DTO is persisted verbatim into the audit table, forever, by default. Phase 3's target |
| 🔴 **The audit docstring documents a hook that does not exist.** *"Redact sensitive fields by overriding `_get_audit_diff_create()` if needed."* | `audit.py:527-529`; `rg _get_audit_diff` returns **one** hit — that docstring | A user following the shipped documentation writes a method varco never calls. This is the strongest single justification in the whole row, and it is fixed in Phase 3 |
| `AuditEntry.entry_hash()` hashes `diff` among ten fields; `prev_hash`/`seq` are set by the repository's `save()`, never by the consumer; `verify_chain()` is a pure `@staticmethod` recomputation | `audit.py:153-185`, `:142-151`, `:384-444` | The hash-chain interaction, resolved in §D-S21-hashchain |
| ⚠️ **`error_params()` is safe in-tree.** The base returns `{}` with an advisory docstring — *"A params dict is exactly the kind of thing someone later fills with `vars(exc)`; don't."* Every in-tree override returns a small hand-picked scalar dict | `varco_core/varco_core/exception/service.py:45-53`, `:106-107`, `:166-173`, `:218-219`, `:270`; `exception/{idempotency,body_limit,rate_limit}.py` | The gap is **out-of-tree overrides**, and it is a rule with no mechanism — exactly as CLAUDE.md says. §D-S21-errparams makes two thirds of it mechanical and says plainly which third stays advisory |
| `ServiceAuthorizationError.error_params()` deliberately excludes `reason`, with the reasoning inline | `exception/service.py:166-173`, `:154-156` | CLAUDE.md's own worked example. Phase 2 must keep it byte-identical, and asserts so |
| `error_message_for()` calls `exc.error_params()` at one site and emits it gated on `settings.include_params` | `exception/http.py:302`, `:335` | **One funnel** — the same property that made claim transformation zero-code-change (CLAUDE.md's `_from_raw_claims` rule). One edit covers every caller |
| `ErrorEnvelopeSettings` already exists with `env_prefix="VARCO_ERROR_"` and already carries `include_params` / `include_message_key` / `include_detail` | `varco_core/varco_core/exception/settings.py:29-56` | The new knob has a home. **No new settings class** |
| ⚠️ **The request-logging middleware emits no user data today.** The log entry is exactly `{method, path, status, duration_ms}` plus `request_id`/`user_id`/`tenant_id`; `request.url.path` **excludes the query string**; no headers, no body | `varco_fastapi/varco_fastapi/middleware/logging.py:122-136`, `:92` | So a `?api_key=…` is **not** in varco's own access log today. The row's "no shared defence" is true; "a live leak" is not. Phase 4 is a **prophylactic**, sized accordingly |
| The logging middleware's own docstring already names the risk it declines to handle: *"No request/response body logging (PII risk) — add a subclass if needed"* | `logging.py:24` | The subclass it invites has nothing to call. That is precisely the seam's job |
| `param_capture_defaults()` / `set_param_capture_defaults()` / `reset_param_capture_state()` — a module-level process default with getter, setter and a test-reset helper, justified by an inline DESIGN block on GIL-atomic single-reference assignment | `params.py:576-627` | The **exact precedent** for `default_redactor()`/`set_default_redactor()`/`reset_redaction_state()` (§D-S21-di) |
| `varco_core/__init__.py` is PEP 562 lazy: `_LAZY` maps each `__all__` name to its module, with a parallel `TYPE_CHECKING` eager block, and the comment warns *"a name added to `_LAZY` but not to…"* is the footgun | `varco_core/varco_core/__init__.py:55-105`, `:492-494`, `:778-831` | Three places must move together for every new exported name (Step 26) |
| `varco_core` import budget: `measured_ms: 6.6`, `ceiling_ms: 25.0` | `design/async-performance-patterns/measurements/import-budget.json:2-6` | 18 ms of headroom, and the new package is stdlib-only and lazily reachable. Still measured, never assumed (Step 27) |
| `varco_fastapi/varco_fastapi/posture.py`'s collectors are **hard-wired** to the four sibling inspectors | `posture.py:5-7`, `:485-573` | A fifth collector means editing a Plan-036 file. Drives §D-S21-posture |

**Honest verdict on the row.** Of the four surfaces it names: one is the incumbent and already
solved (spans); one has a live, unredacted, permanently-stored full-payload leak *and* a documented
phantom hook (audit); one is safe in-tree and unmechanised out-of-tree (`error_params()`); one
carries no user data at all today (logging). The row is right that redaction is single-site and
right that the seam is earned — but it earns it on **one** live leak plus **two** prophylactics,
not on three live leaks. The plan is sized to that.

### §D-S21-shape — a one-method Protocol, a frozen policy, and a free-function walk

```python
# varco_core/redaction/redactor.py
@runtime_checkable
class Redactor(Protocol):
    def redact(self, key: str, value: Any) -> Any:
        """Return `value`, or a placeholder if `key` names something sensitive."""


@dataclass(frozen=True)
class PolicyRedactor:                      # the default implementation
    policy: RedactionPolicy = RedactionPolicy()
    def redact(self, key: str, value: Any) -> Any: ...


def redact_mapping(                        # the traversal, NOT on the Protocol
    data: Mapping[str, Any],
    redactor: Redactor | None = None,
    *,
    policy: RedactionPolicy | None = None,
) -> dict[str, Any]: ...


def is_sensitive_key(key: str, policy: RedactionPolicy) -> bool: ...   # the leaf predicate
def redact_query_string(query: str, redactor: Redactor | None = None) -> str: ...
def json_safe(value: Any) -> Any: ...      # non-JSON objects → "<TypeName>"
```

| ID | Choice | Consequence |
|---|---|---|
| D-S21-shape | **`Redactor` is a `runtime_checkable` Protocol with exactly one method, `redact(key, value)`.** Traversal (`redact_mapping`), the leaf predicate (`is_sensitive_key`), URL handling (`redact_query_string`) and value rendering (`json_safe`) are **module-level free functions**, not Protocol methods | An out-of-tree redactor is ~4 lines and structurally cannot break when varco adds a traversal feature. Traversal policy — depth, cycles, item caps — lives in exactly one place |

✅ **One method is the whole point.** Four consumers is not many, and three of them need only the
   *leaf decision*: span capture already owns its own rendering and `max_params` truncation
   (`params.py:320-354`) and must keep it byte-identical; `error_params()` is a flat mapping; a log
   entry is a flat mapping. Only the audit diff needs a nested walk. Putting the walk on the
   Protocol would force every out-of-tree implementer to write traversal, depth limiting and cycle
   detection to satisfy a seam they wanted one predicate from.
✅ **Protocol, not ABC** — an app substituting a policy object must not be forced to inherit from
   `varco_core`. Same reasoning as `AsyncCache` being a `runtime_checkable` Protocol while
   `CacheBackend` (a thing *backends* implement) is the ABC. A redactor is a policy object, not a
   backend.
✅ **`runtime_checkable`** so `isinstance(x, Redactor)` works for the posture inspector and for a
   defensive check at a boundary — and, per CLAUDE.md's `BulkCache`-off-`AsyncCache` rule (Plan 011
   / D-11), the one-method surface is precisely what makes a future capability additive: a
   `RedactorIntrospection` Protocol, never a second abstract method.
✅ **`redact_mapping()` takes `redactor=None`** and falls back to `default_redactor()`, so the
   common call is `redact_mapping(diff)`.
❌ A caller can pass a `Redactor` and a `policy=` that disagree. Resolved by documenting
   `redactor=` as winning and `policy=` as a shorthand for `PolicyRedactor(policy)`; `ValueError`
   when both are given.
  Rejected — **a three-method Protocol (`redact` / `redact_mapping` / `redact_text`)**: ❌
  `redact_text` is value scanning, rejected outright below; ❌ `redact_mapping` on the Protocol
  duplicates traversal in every implementation and makes depth/cycle semantics unenforceable.
  Rejected — **a frozen dataclass with no Protocol**: ❌ an app wanting a Vault-backed or
  classifier-backed redactor would have to subclass our dataclass, inheriting a `patterns` field it
  does not use.
  Rejected — **an ABC**: ❌ forces inheritance from `varco_core` on a pure policy object; varco's
  ABCs are for backend implementations (`AbstractEventBus`, `AbstractDeadLetterQueue`), and the
  house Protocol precedent (`AsyncCache`, `FieldEncryptor`, `WebhookSigner`'s neighbours) fits
  better.
  Rejected — **`redact_text(str)` / value scanning** (regex over payload strings for card numbers,
  JWTs, `whsec_`/`sk_live_` prefixes): ❌ a denylist that is wrong once is a leak, and
  `plans/035-http-edge-hardening.md:216-218` already rejected exactly this shape for `str(exc)`;
  ❌ false positives are unbounded — a `description` field containing the word "password" would be
  destroyed, and in an *audit trail* that is data loss, not safety; ❌ it is per-string regex on the
  audit and logging hot paths, the opposite of §D-S21-perf. **Parked with a trigger.**

### §D-S21-patterns — the default tuple does not change; two additive constants, both opt-in

| ID | Choice | Consequence |
|---|---|---|
| D-S21-patterns | `varco_core/redaction/patterns.py` becomes the **canonical home** of `DEFAULT_REDACT_PATTERNS`, **byte-identical to today's 15 entries**, and `params.py` imports and re-exports the *same object*. Two new constants ship beside it — `EXTENDED_REDACT_PATTERNS` and `PII_REDACT_PATTERNS` — and **neither is in any default** | Span capture is byte-identical; `RedactionPolicy()` is byte-identical to span capture; an operator opts into more with one tuple concatenation |

**`EXTENDED_REDACT_PATTERNS` (opt-in)** — names that matter to the *new* surfaces and are genuinely
not already covered by the 15 (which cover `access_token`/`refresh_token` via `"token"`,
`client_secret`/`webhook_secret` via `"secret"`, `authorization` via `"auth"`):

`("signature", "passphrase", "bearer", "jwt", "salt")`

`"signature"` is this plan's entire answer to the 038 adjacency: it substring-matches
`webhook-signature`, `Stripe-Signature`, `X-Hub-Signature-256` and `signature` alike, as a **key**
name. ⛔ `whsec_` and `sk_live_` are **value** prefixes and are out of scope by §D-S21-shape; 038's
own undertaking is to never log the secret or the header in the first place
(`plans/038-inbound-webhook-verification.md:588`), and this plan does not weaken that into "log it,
we'll scrub it".

**`PII_REDACT_PATTERNS` (opt-in, and deliberately never a default)** —
`("email", "phone", "address", "iban", "card_number", "cvv", "birth", "national_id", "tax_id")`.

**Patterns explicitly rejected**, recorded so a later "completeness" edit has to argue with this
list: `"key"` (matches `primary_key`, `cache_key`, `idempotency_key`, `partition_key` — it would
redact half of every audit diff), `"name"`, `"id"`, `"user"`, `"account"`.

**DESIGN: the default does not move, and PII is not in it**

✅ **Byte-identical is the safest possible extraction**, and the locked "split by blast radius"
   rule does not even engage: there is no caller-side fix needed because there is no change. A
   pattern added to `DEFAULT_REDACT_PATTERNS` would silently blank an existing span attribute
   someone graphs on — a change with **no error, no log line and no loud failure**, which is the
   exact shape the locked decision reserves for a 4.0 flip, not a point release.
✅ **PII stays out of the default because an audit trail exists to record it.** An audit row whose
   `before`/`after` cannot show that an email address changed is not a safer audit trail, it is a
   broken one — and GDPR Art. 30 processing records are one of the reasons the feature exists
   (`technical_docs/features/database-auditing.md`). A **secret** in an audit row is always wrong;
   **PII** in an audit row is frequently the point. Different polarity, different default.
✅ varco already ships the right answer for PII-at-rest and it is not redaction: field-level
   encryption and crypto-shredding (`varco_core.encryption`, CLAUDE.md's own section). Redaction
   destroys; crypto-shredding defers destruction to a key. Offering `PII_REDACT_PATTERNS` as a
   named, documented, opt-in tuple points at that choice instead of pre-empting it.
✅ Concatenation is the whole API:
   `RedactionPolicy(patterns=DEFAULT_REDACT_PATTERNS + EXTENDED_REDACT_PATTERNS)`. No merge
   semantics, no precedence rules, no subtraction — a tuple.
❌ Three constants is more surface than one. Accepted: the alternative is either a silent default
   change or a magic `level="strict"` enum that hides which strings are actually matched, and a
   redaction rule you cannot read is a redaction rule you cannot audit.
  Rejected — **extend `DEFAULT_REDACT_PATTERNS` in place**: ❌ silently changes span capture for
  every existing deployment in a way that produces no error; ❌ breaks the parametrized incumbent
  test at `varco_core/tests/test_observability_params.py:204-225`, which is a *feature* of that
  test, not an obstacle to route around.
  Rejected — **a `RedactionLevel` enum (`OFF`/`SECRETS`/`STRICT`)**: ❌ hides the actual strings
  behind a name; ❌ every real deployment ends up needing one extra pattern anyway, at which point
  the enum is dead weight beside a tuple.

### §D-S21-falsepos — the incumbent matcher has real false positives; found, filed, **not fixed here**

Substring matching over a 15-entry list, applied to *developer-chosen parameter names*, is fine.
Applied to *domain-chosen payload keys* — which is what audit redaction would do — it is not:

| Pattern | Matches, wrongly | Verified by |
|---|---|---|
| `"pin"` | `shipping_address`, `mapping`, `typing`, `grouping`, `stripping` | `"pin" in "shipping"` → `True` (`shi-ppin-g`); matcher at `params.py:307-309` |
| `"auth"` | `author`, `authored_at`, `authority`, `authorship` | same matcher |
| `"otp"` / `"ssn"` | low collision rate, listed for completeness | — |

| ID | Choice | Consequence |
|---|---|---|
| D-S21-falsepos | **`RedactionPolicy.match_mode` ships with two values — `"substring"` (the default, byte-identical to the incumbent) and `"word"` (matches on `_`/`-`/camelCase token boundaries, so `pin` matches `pin`/`user_pin`/`userPin` but not `shipping`).** The word matcher is **documented and recommended for audit payloads** and is **not** any surface's default. A BACKLOG row asks the real question — *should `DEFAULT_REDACT_PATTERNS` move to word matching in 4.0?* | Nobody's behaviour changes; the sharp edge is named, measured and offered; the default-change question is raised with evidence instead of being answered inside an unrelated plan |

✅ CLAUDE.md's standing discipline for a contract defect discovered mid-plan is *a loud marker plus
   a BACKLOG row, never an in-place production fix* (Test Conventions), and
   `plans/035-http-edge-hardening.md:162-184` applied it to three misleading ordering comments.
   Same shape, same restraint.
✅ **This finding is load-bearing for §D-S21-audit.** "Audit redaction is on by default" and
   "`shipping_address` silently becomes `[REDACTED]` in every audit row" cannot both be shipped.
   The false positive is *the* reason the audit default stays off in 3.2 — a plan that hid the
   finding would have had no honest way to argue that default.
❌ Two match modes is a knob. Mitigated: the default is the incumbent, so nobody must learn it, and
   the Pitfalls table carries the `shipping`/`author` examples verbatim so the choice is concrete.
  Rejected — **make `"word"` the default for the new seam only**: ❌ two surfaces silently matching
  differently is the confusion 035 spent a whole §D block avoiding, and it makes "what does varco
  redact?" un-answerable without knowing which caller you are.
  Rejected — **fix `DEFAULT_REDACT_PATTERNS` to word matching now**: ❌ it changes span redaction
  for every existing deployment in the *unsafe* direction (a name that was redacted stops being
  redacted — e.g. a parameter genuinely named `shipping_pin`), which is a security-relevant default
  change that deserves its own row and its own blast-radius argument, not a paragraph inside this
  one.

### §D-S21-compat — extraction with no behaviour change: a re-export, not an alias, not a subclass

The prompt's test, correctly applied: CLAUDE.md's `TrustStore` precedent is that **an alias was not
available because the two names did not denote the same behaviour** — the new `varco_core.tls.TrustStore`
is a superset with different semantics, so `varco_fastapi.auth.TrustStore` had to be a *deprecation
subclass*. The `SchemaMigrationError` precedent is a *rename to resolve a collision*, so the old
name survives as a deprecated **alias**.

**Neither applies here.**

| ID | Choice | Consequence |
|---|---|---|
| D-S21-compat | `varco_core/observability/params.py` does `from varco_core.redaction.patterns import DEFAULT_REDACT_PATTERNS` and keeps the name in its own `__all__` (`params.py:685`). `_is_redacted` is reduced to a call to `is_sensitive_key`. **No alias, no subclass, no `DeprecationWarning`, no removal in 4.0** | `varco_core.observability.params.DEFAULT_REDACT_PATTERNS` keeps working, and keeps being *the identical object* (`is` identity, asserted). `ParamCaptureConfig` is untouched |

✅ The two names denote **the same tuple object with the same semantics** — not a rename, not a
   behaviour change. A `DeprecationWarning` on an import path that still works, still means the
   same thing, and is not scheduled for removal would be noise that trains people to filter
   warnings.
✅ `params.py`'s module docstring already promises *"this module imports **only stdlib**, never any
   other `varco_core.observability` module"* (`params.py:13-16`). `varco_core.redaction` is not an
   `observability` module and is itself stdlib-only, so the promise survives — but the docstring
   must be amended to say so precisely (Step 5), or it becomes the next stale comment.
✅ The `api_surface.py --check` gate is **blind to this move** (neither name is in any distribution
   package's top-level `__all__` — §D-S21-exists). That makes preserving the module path a
   correctness obligation with a test behind it (Step 3), not something the gate will catch.
❌ `varco_core.observability.params` now depends on `varco_core.redaction`. Accepted: it is a
   one-way, stdlib-only, leaf-package dependency in the direction core→core, and the reverse
   (`redaction` importing `observability`) is forbidden and asserted (Step 3).
  Rejected — **leave `DEFAULT_REDACT_PATTERNS` in `params.py` and have `redaction` import it**: ❌
  inverts the dependency, so the generic seam would import the OTel-adjacent module; ❌ leaves the
  canonical security constant in a module whose name says "span parameters".
  Rejected — **duplicate the tuple in both modules**: ❌ two homes for one fact, and the copy that
  drifts is the one nobody edits (CLAUDE.md's "One home per fact").
  Rejected — **move `sanitize_value` too**: ❌ it is value *rendering*, not redaction; it has one
  consumer (`_render_captured`, `params.py:346`); moving it is churn with no beneficiary, and
  `json_safe` (a different contract — JSON-shape preservation, no truncation) serves the new
  surfaces instead.

### §D-S21-errparams — the advisory rule made **two-thirds** mechanical, on by default

CLAUDE.md: *"treat `error_params()` as a **new exfiltration surface**… any override must apply the
same scrutiny, never `vars(exc)`."* Today that is a sentence in a markdown file. There are exactly
two leak shapes it is warning about, and they are mechanisable to different degrees.

| ID | Choice | 3.2 behaviour |
|---|---|---|
| D-S21-errparams-a | **Name-based:** `error_message_for()` passes `params` through `redact_mapping()` before emitting. New `ErrorEnvelopeSettings.redact_params: bool = True` | **Changed, on by default.** An out-of-tree exception returning `{"api_key": …}` now emits `"[REDACTED]"`. **Byte-identical for every in-tree exception** — asserted per-class in Step 9 |
| D-S21-errparams-b | **Shape-based:** every surviving leaf goes through `json_safe()` — JSON-native scalars and containers pass through **untouched and untruncated**; anything else renders as `"<TypeName>"` | **Changed, on by default.** A `vars(exc)` dump emitting `{"_session": <AsyncSession …>}` becomes `{"_session": "<AsyncSession>"}`. Byte-identical for every in-tree exception (all params are `str`/`int`) |
| D-S21-errparams-c | **The residue, stated plainly:** a secret under a *non-matching* key and a *JSON-scalar* value — `{"internal_reason": "postgres://user:pw@host/db"}` — is **still emitted**. No mechanism catches it | Unchanged. Documented in `error_params()`'s docstring, in the feature doc, and in the Pitfalls table |

**DESIGN: flip both in 3.2, and refuse to claim the third**

✅ **The blast-radius rule flips it.** The caller-side fix is one env var
   (`VARCO_ERROR_REDACT_PARAMS=false`), the pre-existing `VARCO_ERROR_INCLUDE_PARAMS=false`, or
   renaming a params key. And the *only* bodies that change are those already emitting something
   matching a secret pattern — which is, by construction, the bug being fixed.
✅ **One funnel, one edit.** `error_message_for()` is the single site that calls `error_params()`
   (`http.py:302`) and the single site that emits it (`:335`) — the same property that made JWT
   claim transformation zero-code-change (CLAUDE.md's `_from_raw_claims` rule). Every caller,
   in-tree and out, is covered by two lines.
✅ **`json_safe` truncates nothing.** It is deliberately *not* `sanitize_value`, whose 256-char
   ceiling (`params.py:180`, `:194-198`) would silently clip `ServiceConflictError.detail`
   (`exception/service.py:218-219`). Shape safety without length surprise.
✅ **Saying the third case is unsolved is the honest deliverable.** A seam that claimed to make
   `error_params()` safe would replace an advisory rule people follow with a mechanism people
   trust — strictly worse. The docstring at `exception/service.py:45-53` is amended to say exactly
   what is now enforced and exactly what is not.
❌ An out-of-tree exception whose params key happens to contain `"auth"` (say, `{"author": …}`)
   starts emitting `"[REDACTED]"` — §D-S21-falsepos, in the response body. Loud enough to notice
   (the client sees the placeholder), one env var to revert, and the word-matching mode is the
   documented answer. Named in the upgrade note.
❌ Two dict walks per error response. Bounded: `params` is a flat, hand-built dict of a handful of
   keys, and §D-S21-perf's cached predicate makes the name check a dict lookup after first use.
  Rejected — **warn-only in 3.2, flip in 4.0**: ❌ there is nowhere to warn *from* —
  `error_message_for()` is a pure function in `varco_core` with no preflight and no app object, so
  "warn-only" would mean a log line per error response, which is worse than the flip; ❌ the
  caller-side fix is one env var, which is precisely the locked decision's flip test.
  Rejected — **validating `error_params()` return values at class-definition time** (reject an
  override that returns non-scalars): ❌ impossible — the return value is only knowable at call
  time; ❌ it would turn a leak into a crash on the error path, i.e. an exception raised while
  rendering an exception.
  Rejected — **a `@safe_error_params` decorator authors must apply**: ❌ opt-in discipline is what
  the row is trying to replace; an override that forgets it is exactly the case that leaks.

### §D-S21-audit — the phantom hook made real; opt-in in 3.2, with the reason stated

| ID | Choice | 3.2 behaviour |
|---|---|---|
| D-S21-audit | `AuditLogMixin` gains a **class attribute** `_audit_redactor: Redactor \| None = None` and **one** overridable hook, `_audit_diff(action: str, diff: dict[str, Any]) -> dict[str, Any]`, called by all three `_after_*` hooks before the `AuditEvent` is produced. The default implementation returns `diff` unchanged when `_audit_redactor is None`, and `redact_mapping(diff, self._audit_redactor)` otherwise | **Unchanged, byte-identical**, unless a subclass sets `_audit_redactor` or overrides `_audit_diff`. Opt-in |

✅ **This is the docstring's promise, finally kept.** `audit.py:527-529` has been telling users to
   override `_get_audit_diff_create()` since Plan 009; that method exists nowhere. One hook covering
   all three actions is better than the three the docstring implies — `_after_delete` writes `{}`
   (`audit.py:601`) and `_after_update` writes a two-key nesting (`:572-575`), so three hooks would
   be three near-identical bodies. The stale docstring is **corrected to name the real hook**
   (Step 13), which is half the row's value on its own.
✅ **A class attribute, not a constructor parameter.** `AuditLogMixin` has no `__init__`
   (`audit.py:450-510`) and adding one would break MRO composition with `ValidatorServiceMixin` /
   `TenantAwareService` / `SoftDeleteService` — CLAUDE.md's mixin-composition rule. A class
   attribute is inherited, overridable per service, and costs nothing.
✅ **Opt-in is argued, not defaulted-to-safety-by-reflex.** Three independent reasons: (1)
   §D-S21-falsepos — substring matching would blank `shipping_address` in every order audit, and a
   default that mangles domain data is not a security win; (2) it changes what is **persisted**,
   which is irreversible for rows already written and cannot be undone by an env var, so it fails
   the locked decision's "cheap caller-side fix" test; (3) it interacts with the hash chain, and a
   default that silently changes what is hashed deserves the deliberateness of an explicit opt-in
   (§D-S21-hashchain).
✅ The 4.0 flip is **conditional on the word-matching question**, and the BACKLOG rows are written
   to say so: flip the audit default only if `match_mode="word"` becomes the default first.
❌ The live leak (`password_hash` in an audit row) is not closed by default in 3.2. Stated as the
   plan's most significant limitation, carried in the Risks table, the upgrade note, the Pitfalls
   table, and the reported posture (§D-S21-posture). One line — `_audit_redactor = PolicyRedactor()`
   — closes it per service.
  Rejected — **on by default with `DEFAULT_REDACT_PATTERNS`**: ❌ `shipping_address`; ❌ irreversible
  data change with no revert for rows already written.
  Rejected — **redact in `AuditConsumer.on_audit_event`** (`audit.py:745-778`): ❌ the unredacted
  payload has already crossed the event bus and may already sit in a broker, a DLQ, and an outbox
  row — redacting at the consumer secures the *table* while leaving the secret on the wire.
  Redacting in the **mixin, before `_produce()`**, is the only point where nothing has seen it yet.
  Rejected — **redact in the repository's `save()`**: ❌ same wire exposure as above, ❌ every
  backend would have to implement it identically, ❌ and it is one step from the read-path variant
  that §D-S21-hashchain forbids outright.
  Rejected — **honour the docstring literally with `_get_audit_diff_create/update/delete()`**: ❌
  three hooks, three bodies, three things to override; the docstring named a shape that was never
  built, so correcting the docstring is cheaper than building a shape chosen by an unimplemented
  comment.

### §D-S21-hashchain — redaction is a **write-path, pre-emission** transform; never a read filter

This is the correctness trap the row's slice flags, and it resolves cleanly **because of where the
hook sits**.

```
  service mutation
        │
        ▼
  AuditLogMixin._after_update
        │   ← ✅ THE ONLY LEGAL REDACTION POINT (before _produce)
        ▼
  AuditEvent(diff=…)  ──▶  bus  ──▶  AuditConsumer.on_audit_event
                                              │
                                              ▼
                                     AuditEntry.from_event(diff=…)   audit.py:204-215
                                              │
                                              ▼
                              repository.save()  → assigns seq, prev_hash    audit.py:142-151
                                              │
                                              ▼
                                     entry_hash() over the STORED diff       audit.py:170-185
                                              │
                                              ▼
                                     verify_chain() recomputes               audit.py:384-444
```

| ID | Choice | Consequence |
|---|---|---|
| D-S21-hashchain | Redaction happens **before the event is produced**, therefore before `AuditEntry.from_event()`, therefore before `seq`/`prev_hash`/`entry_hash()` exist. The chain hashes exactly what is stored, always. ⛔ **Redacting on the read path — inside `list()`, `list_for_entity()`, or an admin router — is forbidden**, because `verify_chain()` recomputes `entry_hash()` from the returned fields (`audit.py:442`) and would report a `HashMismatch` on **every** row | Chain integrity is unconditional. Rows written before and after a redactor is enabled are all verifiable, together, in one `verify_chain()` call |

✅ **Mixed-mode is safe by construction.** A row written in June with an unredacted `diff` hashes
   that unredacted `diff`; a row written in July with a redacted one hashes the redacted one. Each
   `entry_hash()` is a pure function of the entry's own stored fields (`audit.py:153-185`), so
   `verify_chain()` over a range spanning the flip returns `True`. Asserted directly (Step 12).
✅ Enabling a redactor is **not** a `ChainGap` and **not** a `HashMismatch` — neither finding type
   can be produced by a change in payload content, only by a missing `seq` or a mismatched
   `prev_hash` (`audit.py:426-439`).
✅ It composes with 039: `delete_where(..., allow_chain_break=True)` (`audit.py:360-366`) breaks a
   chain by deleting rows; redaction never deletes a row and never renumbers one. Orthogonal, as
   both plans state.
❌ **Redaction is irreversible and the chain proves it was always that way.** Once a redacted row is
   hashed, there is no forensic path back to the original value — and the chain will happily attest
   to the redacted version. That is the correct behaviour for a tamper-evidence mechanism (it
   attests to what was stored, not to what happened), and it is a Pitfalls row so nobody discovers
   it during an incident.
❌ A user who enables a redactor to satisfy a deletion request for *past* rows gets nothing —
   redaction is not retroactive. The right tool is 039's retention sweep or crypto-shredding.
   Pitfalls row.

### §D-S21-logging — a constructor keyword and a URL helper; ⛔ zero diff in Plan 041's files

| ID | Choice | Consequence |
|---|---|---|
| D-S21-logging | `RequestLoggingMiddleware.__init__` gains `redactor: Redactor \| None = None`. When set, the assembled `log_entry` (`logging.py:122-133`) goes through `redact_mapping()` before `self._log.log(...)`. `redact_query_string()` ships beside it for subclasses that log a full URL. **`create_varco_app` gains no keyword; `app.py` and `middleware/__init__.py` are not touched** | Today's log line is byte-identical (default `None`). A body-logging or header-logging subclass — the one the docstring at `logging.py:24` invites — finally has something to call |

✅ **Sized to the finding.** The middleware emits no user data today (`logging.py:122-136`), so
   anything more than a hook would be solving an absent problem. `request.url.path` excludes the
   query string, so varco's own access log does not carry a `?api_key=` — but a subclass logging
   `str(request.url)` would, and 034 made "a credential in a URL is already in the access log"
   a first-class concern. `redact_query_string()` is the narrow, key-based, stdlib-only
   (`urllib.parse.parse_qsl` / `urlencode`) answer.
✅ **`redactor=None` by default, and it does not fall back to `default_redactor()`.** A
   per-request dict walk that can never find anything is pure hot-path cost. Explicit opt-in.
✅ **⛔ Zero diff in `app.py` and in `middleware/__init__.py`'s ordering table** — Plan 041 owns
   both, and 038 set the precedent of making that a checked DoD item, not a promise. No
   `add_middleware` call is added, moved or reordered; the middleware count and order are unchanged,
   so 035's characterization test at `varco_fastapi/tests/test_middleware_order.py` must stay green
   untouched (Step 16).
❌ An operator who wants log redaction must construct the middleware themselves rather than pass a
   `create_varco_app` keyword. Accepted for one release: a keyword is an `app.py` diff, and the
   sibling boundary is worth more than one line of convenience. Filed as a BACKLOG row for whoever
   owns `app.py` next.
  Rejected — **route the log entry through `default_redactor()` unconditionally**: ❌ hot-path cost
  for zero benefit given what is logged today; ❌ it would also redact `user_id`/`tenant_id` if a
  future pattern ever matched them, silently breaking log correlation.
  Rejected — **a `create_varco_app(log_redactor=…)` keyword**: ❌ `app.py` is 041's neighbourhood
  this cycle.

### §D-S21-di — a module-level process default, no DI verb, no scanned decorator

| ID | Choice | Consequence |
|---|---|---|
| D-S21-di | `default_redactor()` / `set_default_redactor(r)` / `reset_redaction_state()` — a module-level default with a getter, a setter and a test-reset helper, **copied in shape from `param_capture_defaults()` / `set_param_capture_defaults()` / `reset_param_capture_state()`** (`params.py:598-627`). ⛔ **No `@Singleton`, no `@Provider`, no `@Configuration` anywhere in `varco_core.redaction`.** No `bind_redactor` / `enable_redaction` verb in 3.2 | `error_message_for()` — a pure function with no container — can reach a redactor. `container.scan("varco_core", recursive=True)` gains nothing to auto-activate |

✅ The scanned-decorator prohibition is the standing rule that already governs `varco_core.tls` and
   `varco_core.event.cloudevents` in CLAUDE.md, for the identical reason: `scan("varco_core",
   recursive=True)` is a documented, in-use pattern, and a decorator here would change behaviour in
   every app that scans core.
✅ The module-level default is precedented **inside the very module being extracted from**, with its
   own DESIGN block on GIL-atomic single-reference assignment (`params.py:576-583`). Same
   thread-safety note, same free-threading caveat, same test-reset helper — one convention, not two.
✅ **Argued against the verb taxonomy, as required.** A DI verb here would be `bind_*` ("registers a
   caller-constructed object, no lifecycle side effect", CLAUDE.md's table) — the
   `varco_core.tls.bind_trust_store` shape. It is **not** `enable_*` (nothing is being un-shadowed),
   not `install_*` (no process-global mutation beyond the default cell, and no app/session object),
   not `mount_*`. But no varco component *injects* `Redactor` — span capture reads its config,
   `error_message_for()` reads the process default, the audit mixin takes a class attribute, the
   logging middleware takes a constructor keyword — so `bind_redactor` would register a token
   nothing resolves. **Parked with a precise trigger**: the first varco component that injects
   `Redactor`, at which point `bind_redactor(container, redactor)` is the correct shape.
❌ Process-global mutable state, wrong if two apps in one process want different policies. Accepted:
   identical to the incumbent capture defaults, and every per-surface consumer accepts an explicit
   redactor that wins over the default.

### §D-S21-failsafe — a redactor that raises **redacts**; a walk that fails redacts everything

| ID | Choice | Consequence |
|---|---|---|
| D-S21-failsafe | **Leaf:** `redactor.redact(key, value)` raising → that leaf becomes the placeholder, never the original value; logged once at DEBUG. **Walk:** any failure inside `redact_mapping()` → return `{k: placeholder for k in data}` — top-level keys preserved, **every value redacted** — logged at ERROR with `exc_info=True`. `redact_mapping` **never raises** | A broken custom redactor degrades to maximum redaction, never to pass-through |

✅ **Data can be dropped safely; it cannot be un-emitted.** The incumbent already chose this
   polarity twice — `sanitize_value` returns `"<unrepresentable>"` rather than propagating
   (`params.py:262-265`) and `render_captured_params` returns `{}` rather than emitting
   half-processed values (`params.py:381-388`). This plan keeps the polarity and makes it stricter
   where the surface is more sensitive.
✅ Preserving top-level keys rather than returning `{}` keeps the *shape* — an audit row still
   records that fields changed, an error body still carries its params keys — while carrying no
   values. Structure is not the secret.
✅ Instrumentation must never break the application: the same rule `params.py:256-258` states as
   the reason `sanitize_value` is a safety boundary. A redaction failure that raised would take
   down an error-response render or an audit emission, converting a leak into an outage.
❌ A silently-broken custom redactor produces an all-redacted audit trail, which looks like a
   configuration mistake. Mitigated by the ERROR log with `exc_info=True` and by the posture
   inspector reporting the configured redactor's type.
  Rejected — **propagate the exception**: ❌ a raising redactor would break `error_message_for()`,
  i.e. crash while rendering an error — CLAUDE.md's own `message_resolver` precedent swallows
  exactly this (`exception/http.py:305-314`).
  Rejected — **return the input unchanged on failure**: ⛔ this is the one behaviour the slice
  forbids by name, and rightly: the failure mode of "redactor is broken" would become "nothing is
  redacted", silently, at exactly the moment you most need it.

### §D-S21-nesting — depth, cycles and item caps on the one nested surface

Only the audit diff is nested (`{"before": {...}, "after": {...}}` of two `model_dump()`s,
`audit.py:572-575`), and a `model_dump()` can nest arbitrarily deep.

| Guard | Default | Behaviour on breach |
|---|---|---|
| `max_depth` | `6` | The over-deep subtree is replaced by the string `"<max-depth>"` |
| `max_items` | `1000` per container | Extra entries dropped; a `"<truncated>"` marker entry is appended, mirroring `param._truncated` (`params.py:352-353`) |
| Cycles | always on | An already-visited container (`id()`-keyed `seen` set, scoped to the walk) renders as `"<cycle>"` |
| Non-JSON leaf | `render_non_json=True` | `json_safe()` → `"<TypeName>"` |

✅ A `model_dump()` should not contain a cycle, but a **hand-built** diff from an overridden
   `_audit_diff` can, and an unguarded recursive walk on the audit write path would hang the
   service. `id()`-keyed detection is stdlib, O(1), and scoped per call so nothing leaks between
   requests.
✅ Every breach produces a **visible marker**, never a silent drop — the same convention as
   `param._truncated`.
❌ A legitimately 8-deep document is clipped at 6. The knob is on the policy; the marker says which
   subtree; and the Pitfalls table carries it.

### §D-S21-perf — precomputed lowercase, a bounded decision cache, one benchmark, no gate

Audit diffs and log records are hot in a way span attributes are not: a span captures ≤ 32
developer-named parameters (`params.py:181`); an audit diff walks every field of two DTO dumps on
every write.

| ID | Choice | Consequence |
|---|---|---|
| D-S21-perf | `is_sensitive_key()` is backed by a module-level `functools.lru_cache(maxsize=4096)` over `(key_lower, patterns, match_mode)`. `RedactionPolicy.__post_init__` normalises `patterns` to lowercase once via `object.__setattr__` (the frozen-dataclass idiom). `redact_mapping` short-circuits on an empty mapping and never copies a value it does not change | The repeated work — the *same* DTO field names, every request — is a dict lookup after first sight. Pattern tuples are hashable, so the cache is keyed on the policy, not on a mutable default |

✅ The cache key includes the pattern tuple and the match mode, so `set_default_redactor()` or a
   per-service policy **cannot** read a stale decision — a different policy is a different key.
   `maxsize=4096` bounds it against an attacker-influenced key space (the `InMemoryRateLimiter`
   lesson from `plans/035-http-edge-hardening.md:404-424`: an unbounded cache keyed on
   caller-supplied names is a memory-exhaustion primitive; audit keys are schema-derived, but a
   hand-built diff need not be).
✅ **`benchmarks/bench_redaction.py`** (Plan 028's harness): `redact_mapping()` over a
   representative nested audit diff, and `is_sensitive_key()` cold vs. cached. ⛔ It asserts
   **nothing about time**, imports **no** backend that needs a container, and is collected only by
   `benchmarks/pytest.ini` — never by `scripts/unit_tests.sh`. ⛔ `bench` is never a required check
   and must never appear in `all-green`'s `needs:`.
❌ A process-wide cache is another piece of module state to reset in tests. Covered by
   `reset_redaction_state()`, which clears it — the same helper shape as
   `reset_param_capture_state()`.

### §D-S21-posture — a pure inspector, exported; ⛔ **not** wired into Plan 036's harness

| ID | Choice | Consequence |
|---|---|---|
| D-S21-posture | `varco_core/redaction/posture.py` exports `RedactionFinding` / `RedactionPosture` / `inspect_redaction_posture(*, service_classes=(), envelope_settings=None)` — a pure, never-raising read, with a **fixed `check`-id table** (below). It is **not** added as a collector in `varco_fastapi/varco_fastapi/posture.py` | The 4.0 audit-default flip has a place to live and a stable contract to be written against; Plan 036's file is untouched this cycle |

**Stable `check` ids this plan commits to emitting:**

| `check` | Severity | Emitted when |
|---|---|---|
| `redaction.audit.disabled` | `warn` | An inspected `AuditLogMixin` subclass has `_audit_redactor is None` and has not overridden `_audit_diff` |
| `redaction.error_params.disabled` | `warn` | `ErrorEnvelopeSettings.redact_params is False` |
| `redaction.default.custom` | `info` | `default_redactor()` is not a `PolicyRedactor` — an out-of-tree redactor is in force |
| `redaction.patterns.substring_mode` | `info` | The effective policy uses `match_mode="substring"` — §D-S21-falsepos's known false positives apply |

✅ **Consistent with 038 and 039, and the difference is stated.** 038 argued *against* an inspector
   because its whole feature is off by default and a finding would fire on every app. Here the
   picture differs per surface: spans are always on (nothing to report), `error_params` redaction is
   on by default (so `redaction.error_params.disabled` fires only for someone who turned it *off* —
   an actionable signal, not noise), and the audit default is off **by a decision this plan
   defends**, which is exactly the "loud warn-only in 3.2, flip in 4.0" shape the cycle's locked
   migration posture demands.
✅ **Defined-and-exported without building the preflight** is the obligation shape 035 §D-seam
   established and 036 consumed. This plan honours the first half; the second half is a BACKLOG row
   for whoever flips the audit default.
✅ `inspect_redaction_posture()` lives in `varco_core`, takes explicit arguments, touches no app
   object and no network, and never raises — so it is usable from a test, a CLI, or a future
   collector without any of them importing `varco_fastapi`.
❌ It ships with no in-tree caller, which is unusual. Accepted and named: the alternative is
   editing `varco_fastapi/varco_fastapi/posture.py:485-573` — a Plan-036 file that no extension
   plan owns — inside a plan whose sibling boundaries are otherwise clean. See §Open questions 1.
  Rejected — **wire a fifth collector into `posture.py` now**: ❌ crosses into 036's harness, and
  `redaction.audit.disabled` would fire `warn` on **every existing app** on upgrade, which is the
  noise concern `plans/035-http-edge-hardening.md:980-983` raised about `http.error.detail_exposed`
  and left to 036's owner to render.
  Rejected — **no inspector at all**: ❌ then the 4.0 flip has no warn-only stage, contradicting the
  cycle's locked migration posture for a default that needs real application work.

### §D-S21-conformance — one `COVERAGE.md` note row, written by this plan

`Redactor` is a new public Protocol, not an implementation of one of the eight ABCs
`testkit/varco_conformance` covers, so **no suite is owed** — but the absence must be argued in
writing, not rediscovered. This plan appends the note row itself (039's precedent, `:26`), naming
the un-park trigger: **a second in-tree `Redactor` implementation**. With exactly one
(`PolicyRedactor`), a conformance suite would test that implementation against itself.

### Alternatives considered (plan-level)

- **Do nothing; strengthen CLAUDE.md's wording instead** — ❌ the row exists because the rule
  already exists and has no mechanism; a louder sentence is the same non-mechanism. And it would
  leave `audit.py:527-529` pointing at a method that does not exist.
- **Put the seam in a new `varco_security` package** — ❌ an eleventh distribution for ~300 lines of
  stdlib, with the packaging, PyPI environment, trusted-publisher and Scorecard cost that CLAUDE.md
  documents for each; and `varco_core` is the correct home by the decision tree (a Protocol used by
  application code, transport-agnostic, needed by every backend).
- **Adopt a dependency (`scrubadub`, `presidio-analyzer`)** — ❌ a hard `varco_core` runtime
  dependency, breaking the plan's first non-goal and the import budget; ❌ both are *content*
  classifiers, which §D-S21-shape rejects on its own terms.
- **Redact at the serializer boundary** (`JsonEventSerializer` / `CloudEventsJsonSerializer`) so
  every event is scrubbed on the wire — ❌ it would change the wire bytes of every app, ❌ it
  redacts domain events that legitimately carry the field, and ❌ CLAUDE.md forbids the shape most
  likely to be reached for (a module-level decorator in `cloudevents.py`).
- **One `redact_everything(obj)` entry point that consumers call blindly** — ❌ the four surfaces
  have genuinely different needs (span capture must keep its own rendering and truncation; audit
  needs a nested walk; `error_params` needs shape safety without truncation); one signature would
  either lose span byte-identity or force the other three through OTel-shaped rendering.

---

## Steps

### Phase 1 — extract the seam with **zero** behaviour change (🟢 S) — **independently mergeable, must merge first**

1. [x] `varco_core/tests/test_redaction.py` (new, **failing first**) — the seam's own contract:
       `RedactionPolicy()` is frozen and its `patterns` **are** `DEFAULT_REDACT_PATTERNS`;
       `is_sensitive_key` is case-insensitive substring by default; `PolicyRedactor` satisfies
       `isinstance(x, Redactor)`; `redact_mapping` handles nesting, a cycle (`"<cycle>"`), depth
       (`"<max-depth>"`), item cap (`"<truncated>"`); **fail-safe**: a redactor whose `redact()`
       raises yields the placeholder for that leaf, and a walk that fails yields
       `{k: "[REDACTED]"}` for every top-level key and **never** the input (§D-S21-failsafe);
       `default_redactor()`/`set_default_redactor()`/`reset_redaction_state()` round-trip and the
       reset clears the `lru_cache`; `redact_query_string("api_key=abc&page=2")` →
       `"api_key=%5BREDACTED%5D&page=2"` with `page` intact; `json_safe` passes JSON scalars and
       containers through **untruncated** and renders an arbitrary object as `"<TypeName>"`;
       `match_mode="word"` does **not** match `shipping`/`author` while `"substring"` does
       (§D-S21-falsepos, asserted as a literal so the finding cannot be lost).
2. [x] `varco_core/varco_core/redaction/{__init__,patterns,policy,redactor,posture}.py` (new) —
       stdlib only, `from __future__ import annotations` in each, frozen dataclasses,
       `DESIGN:` blocks citing §D-S21-shape / §D-S21-patterns / §D-S21-failsafe / §D-S21-perf,
       docstrings with `Args:`/`Returns:`/`Raises:`/`Edge cases:`/`Thread safety:`/`Async safety:`.
       `patterns.py` holds the **verbatim 15-entry** `DEFAULT_REDACT_PATTERNS` moved from
       `params.py:92-108` (comment included), plus `EXTENDED_REDACT_PATTERNS` and
       `PII_REDACT_PATTERNS`. ⛔ No `@Singleton`/`@Provider`/`@Configuration` anywhere in the
       package (§D-S21-di).
3. [x] `varco_core/tests/test_redaction_extraction.py` (new, **the characterization test**) —
       `varco_core.observability.params.DEFAULT_REDACT_PATTERNS is
       varco_core.redaction.DEFAULT_REDACT_PATTERNS` (object identity, not equality); the tuple
       equals the 15 literals **written out in the test**; `"DEFAULT_REDACT_PATTERNS" in
       params.__all__`; `ParamCaptureConfig().redact_patterns is DEFAULT_REDACT_PATTERNS`; and the
       import-direction guard — `varco_core.redaction` and its submodules import **nothing** from
       `varco_core.observability` (assert over `ast`-parsed imports, the shape
       `varco_core/tests/test_tls_no_hard_client_deps.py` uses).
4. [x] `varco_core/varco_core/observability/params.py` — replace the literal tuple at `:92-108`
       with the import from `varco_core.redaction.patterns`, keeping the name in `__all__`
       (`:685`); reduce `_is_redacted` (`:307-309`) to a call to `is_sensitive_key`. **`sanitize_value`,
       `ParamCaptureConfig`, `_render_captured`, `CapturePlan` are not touched.**
5. [x] `varco_core/varco_core/observability/params.py:13-16` — amend the *"imports only stdlib"*
       docstring promise to state precisely what is now true: *only stdlib and
       `varco_core.redaction` (itself stdlib-only); never another `varco_core.observability`
       module.* A stale promise here becomes the next §D-order-bugs.
6. [x] Run the **untouched** incumbent suites and require zero diff in expectations:
       `varco_core/tests/test_observability_params.py` (the parametrized substring/case-insensitive
       cases at `:204-240`) and `varco_core/tests/test_observability.py::…test_password_kwarg_is_redacted`
       (`:1347-1355`).

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_redaction.py
varco_core/tests/test_redaction_extraction.py varco_core/tests/test_observability_params.py
varco_core/tests/test_observability.py -q`. **Phase 1 ships on its own and changes no behaviour.**

### Phase 2 — `error_params()` made mechanical (🟡 M)

7. [x] `varco_core/tests/test_error_envelope_settings.py` (extend, **failing first**) —
       `ErrorEnvelopeSettings().redact_params is True`; `VARCO_ERROR_REDACT_PARAMS=false` parses.
8. [x] `varco_core/tests/test_redaction_error_params.py` (new, **failing first**) — an out-of-tree
       `ServiceException` whose `error_params()` returns `{"api_key": "sk_live_x", "page": 2}`
       emits `{"api_key": "[REDACTED]", "page": 2}` through `error_message_for()`
       (D-S21-errparams-a); one returning `{"_session": <object with a live repr>}` emits
       `"<TypeName>"` (D-S21-errparams-b); one returning `{"internal_reason": "postgres://u:p@h/db"}`
       **still emits it verbatim** — the documented residue, asserted so the limitation is a
       tested fact and not a hopeful sentence (D-S21-errparams-c); `redact_params=False` restores
       the pre-3.2 body byte-for-byte.
9. [x] `varco_core/tests/test_redaction_error_params.py` (same file) — **the no-regression proof**:
       for **every** in-tree `ServiceException` subclass that overrides `error_params()`
       (`exception/service.py:106`, `:166`, `:218`, `:270`; `exception/idempotency.py:85`, `:121`,
       `:149`; `exception/body_limit.py:64`; `exception/rate_limit.py:122`), the emitted `params`
       is byte-identical to the pre-change value. Include the explicit
       `ServiceAuthorizationError` case: `reason` is still absent and `operation`/`entity` are
       untouched (`exception/service.py:166-173`).
10. [x] `varco_core/varco_core/exception/settings.py` — add `redact_params: bool = True` with an
        `Attributes:` entry citing §D-S21-errparams. `varco_core/varco_core/exception/http.py:302`
        — route `params` through `redact_mapping()` + `json_safe()` when
        `settings.redact_params`, with a `DESIGN:` comment. **No other default moves.**
11. [x] `varco_core/varco_core/exception/service.py:45-53` — rewrite the base `error_params()`
        docstring: state exactly what is now enforced (secret-named keys, non-JSON values) and
        exactly what is **not** (a secret under a benign key with a scalar value), and point at the
        feature doc. The `"never `vars(exc)`"` sentence stays — it is now backed by a mechanism for
        two of its three shapes, and the third is named.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_redaction_error_params.py
varco_core/tests/test_error_envelope_settings.py varco_core/tests/test_exception*.py
varco_fastapi/tests/test_error_leak_s3.py varco_fastapi/tests/test_exception_envelope.py -q`.

### Phase 3 — the audit hook the docstring already promised (🟡 M)

12. [x] `varco_core/tests/test_audit_redaction.py` (new, **failing first**) — default
        `_audit_redactor = None` → `diff` is byte-identical to today for create/update/delete
        (`audit.py:538`, `:572-575`, `:601`); with `_audit_redactor = PolicyRedactor()`, a
        `password` field in `before`/`after` is `"[REDACTED]"` while sibling fields survive; a
        subclass overriding `_audit_diff` wins; the hook is called **once per emission** and
        **before** `_produce` (assert on the produced `AuditEvent`, not on a stored row).
        ⚠️ **Hash-chain cases (§D-S21-hashchain), the correctness core**: a chain built from three
        entries — one written unredacted, then the redactor enabled, then two more —
        `AuditRepository.verify_chain()` returns `True` over all three together; and each entry's
        `entry_hash()` is a function of its **stored** `diff` (recomputing after mutating a
        returned `AuditEntry.diff` produces a `HashMismatch`, proving why read-path redaction is
        forbidden).
13. [x] `varco_core/varco_core/service/audit.py` — add the class attribute `_audit_redactor:
        Redactor | None = None` and the `_audit_diff(action, diff)` hook to `AuditLogMixin`; call
        it from `_after_create` (`:538`), `_after_update` (`:572-575`) and `_after_delete`
        (`:601`). **Fix the stale docstring at `:527-529`** — it names `_get_audit_diff_create()`,
        which exists nowhere in the repo — to name `_audit_diff`/`_audit_redactor`, and add the
        equivalent `Edge cases:` note to `_after_update`. `Redactor` is imported under
        `TYPE_CHECKING` beside the existing block (`audit.py:77-81`).
        ⛔ **Nothing inside `audit.py:267-444` is touched** — 039's half (§Scope and siblings).
14. [x] `varco_core/varco_core/service/audit.py` — a `DESIGN:` block on `_audit_diff` citing
        §D-S21-audit (why one hook, why a class attribute, why opt-in) and §D-S21-hashchain (why
        pre-emission is the only legal point, and the ⛔ read-path prohibition).

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_audit_redaction.py
varco_core/tests/test_audit.py varco_core/tests/test_audit_chain.py -q`, plus
`git diff --stat varco_core/varco_core/service/audit.py` reviewed line-by-line against §Scope.

### Phase 4 — the logging hook (🟢 S) — ⛔ zero diff in Plan 041's files

15. [x] `varco_fastapi/tests/test_logging_redaction.py` (new, **failing first**) — with
        `redactor=None` (the default) the emitted log entry is byte-identical to today
        (`logging.py:122-133`); with a `PolicyRedactor`, a subclass that adds
        `{"authorization": …}` to the entry logs `"[REDACTED]"` while `method`/`path`/`status`/
        `duration_ms`/`request_id`/`user_id`/`tenant_id` survive; `skip_paths` behaviour
        (`logging.py:91-93`) is unchanged.
16. [x] `varco_fastapi/varco_fastapi/middleware/logging.py` — add the `redactor` keyword, apply it
        to `log_entry` before `self._log.log(...)` (`:136`), and update the class docstring plus
        the module `DESIGN:` note at `:24` to point at the seam. ⛔ **No other file in
        `varco_fastapi` is edited in this phase**; `varco_fastapi/tests/test_middleware_order.py`
        must pass **unmodified** (Plan 041's boundary, §D-S21-logging).

⛔ **CHECKPOINT** — `uv run pytest varco_fastapi/tests/test_logging_redaction.py
varco_fastapi/tests/test_middleware_order.py -q`, plus `git diff --exit-code
varco_fastapi/varco_fastapi/app.py varco_fastapi/varco_fastapi/middleware/__init__.py`.

### Phase 5 — posture inspector (🟢 S)

17. [x] `varco_core/tests/test_redaction_posture.py` (new, **failing first**) — the `check`-id set
        is asserted as a **literal** against §D-S21-posture's table, so a future consumer can be
        written against the table without reading the code (035 DoD 4's precedent); a service class
        with no redactor emits `redaction.audit.disabled`; one with a redactor does not;
        `redact_params=False` emits `redaction.error_params.disabled`; a custom `default_redactor()`
        emits `redaction.default.custom`; the function **never raises** on a nonsense input and
        returns an all-`False` posture instead.
18. [x] `varco_core/varco_core/redaction/posture.py` — `RedactionFinding` / `RedactionPosture`
        (frozen) / `inspect_redaction_posture()`, pure, never raising, no I/O.
        ⛔ **`varco_fastapi/varco_fastapi/posture.py` is not touched** (§D-S21-posture).

### Phase 6 — docs, benchmark, snapshots, BACKLOG (🟢 S)

19. [x] `benchmarks/bench_redaction.py` (new) — `redact_mapping()` over a representative nested
        audit diff, and `is_sensitive_key()` cold vs. cached. ⛔ No time assertion, no
        container-backed import, no `pytest.ini` change (`benchmarks/pytest.ini` already collects
        `bench_*.py`).
20. [x] `technical_docs/features/redaction.md` (new) — the full design: what redaction is and is
        not (vs. crypto-shredding), the seam's four names, per-surface posture and why each,
        the pattern-constant table with **every** rejected pattern listed, the §D-S21-falsepos
        evidence, the hash-chain rule, and a **Pitfalls** table with at least these rows:
        *substring `"pin"` redacts `shipping_address` — use `match_mode="word"` for payloads* ·
        *`"auth"` redacts `author`* · *redaction is irreversible and the hash chain attests to the
        redacted value* · ⛔ *never redact on the audit read path — `verify_chain()` fails on every
        row* · *audit redaction is opt-in; a `password_hash` on a read DTO is stored verbatim until
        you set `_audit_redactor`* · *`error_params()` still leaks a secret under a benign key* ·
        *a broken custom redactor redacts everything, by design* · *`PII_REDACT_PATTERNS` in an
        audit trail may destroy the record you are keeping*.
21. [x] `README.md` — a "Redaction" section between the observability and auditing sections: the
        four-line opt-in for audit, the `error_params` default, the `redactor=` logging keyword,
        and a `VARCO_ERROR_REDACT_PARAMS` row in the existing `VARCO_ERROR_*` table.
22. [x] `ARCHITECTURE.md` — a "Redaction" type hierarchy (`Redactor` Protocol → `PolicyRedactor`;
        `RedactionPolicy`; the free functions) and the `varco_core.redaction` module listing in the
        package map.
23. [x] `CLAUDE.md` — **pointer-only**: (a) a one-line "Redaction (Plan 040 / S21)" entry under Key
        Abstractions pointing at the feature doc and carrying the rules that change agent
        behaviour — *redaction is key-name-based only, never value scanning* · *never redact on the
        audit read path* · *never add a scanned decorator to `varco_core.redaction`* · *a redactor
        that raises redacts*; (b) a Decision-Tree branch: *hiding a secret from a span/log/audit
        payload/error body? → `varco_core.redaction`, never a second pattern list · need it back
        later? → that is `varco_core.encryption` (crypto-shredding), not redaction*; (c) amend the
        existing ⚠️ on `error_params()` in §Error taxonomy to say what is now **mechanical** and
        what remains advisory.
24. [x] `testkit/varco_conformance/COVERAGE.md` — the note row for `Redactor` (§D-S21-conformance),
        with the un-park trigger. ⚠️ Coordinate with Plans 039 and 042, which also append rows;
        whichever lands last rebases.
25. [x] `CHANGELOG.md` `## [Unreleased]` — `### Added`: `varco_core.redaction`
        (`Redactor`/`PolicyRedactor`/`RedactionPolicy`/`redact_mapping`/`redact_query_string`/
        `json_safe`/`inspect_redaction_posture`), `AuditLogMixin._audit_diff`/`_audit_redactor`,
        `RequestLoggingMiddleware(redactor=…)`, `ErrorEnvelopeSettings.redact_params`.
        `### Security`: `error_params()` is now redacted and shape-guarded by default.
        `### Fixed`: `audit.py`'s docstring named a `_get_audit_diff_create()` hook that never
        existed. `BACKLOG.md`: mark `S21` `✅ planned → plans/040…` and add the rows below.
26. [x] `varco_core/varco_core/__init__.py` — export the new public names by editing **all three**
        places together (`__all__`, `_LAZY` at `:494`, and the `TYPE_CHECKING` block at `:105`) —
        the footgun the file's own comment at `:74` names. ⛔ No eager import is added.
27. [x] `uv run python scripts/api_surface.py` then `--check`; **commit both snapshot files in this
        commit** (CI gate on `make lint`'s no-`PKG` path). Then `uv run python
        scripts/import_budget.py --check --warn-only` — `varco_core` is at `6.6 ms` against a
        `25.0 ms` ceiling (`import-budget.json:2-6`); the new package is stdlib-only and reachable
        only through `_LAZY`, so a breach means an eager import slipped in. CLAUDE.md: *a new
        top-level import needs a budget check, not a hunch.*

⛔ **CHECKPOINT** — `make lint`, `make type-check`, `make test`, `make bench`.

---

## Migration and upgrade note (existing deployments — read before shipping)

**One observable change on upgrade to 3.2, revertible with one environment variable.**

| Change | Who notices | Revert |
|---|---|---|
| An error body's `params` now redacts secret-named keys and renders non-JSON values as `"<TypeName>"` | Only an app with a **custom** `ServiceException` whose `error_params()` returns a key matching one of the 15 patterns, or a non-JSON value. Every in-tree exception is byte-identical (asserted, Step 9) | `VARCO_ERROR_REDACT_PARAMS=false`, or the pre-existing `VARCO_ERROR_INCLUDE_PARAMS=false` |

**Nothing else changes.** Span capture is byte-identical (Phase 1 is a pure extraction). Audit
redaction is **opt-in** — one line per service, `_audit_redactor = PolicyRedactor()`. Log redaction
is **opt-in** — `RequestLoggingMiddleware(redactor=…)`.

**⚠️ The one thing to act on, even though nothing broke:** if any of your read DTOs carries a
secret-shaped field (`password_hash`, `api_key`, `totp_secret`), it is **already** stored verbatim
in your audit table and has been since you enabled auditing (`audit.py:538`, `:572-575`). Setting
`_audit_redactor` stops new rows; it does **not** clean old ones — that is 039's retention sweep or
crypto-shredding.

**Adopting audit redaction, in order:** (1) set `_audit_redactor = PolicyRedactor()` on **one**
service and inspect the resulting rows; (2) check for §D-S21-falsepos casualties — a
`shipping_address` field will be redacted under the default `match_mode="substring"`; (3) switch to
`RedactionPolicy(match_mode="word")` if any fire; (4) roll out per service, never globally in one
change — the transformation is irreversible for every row written after it.

**The 4.0 flip list this plan contributes to:** `AuditLogMixin._audit_redactor` defaulting to
`PolicyRedactor()` — **conditional** on `match_mode="word"` becoming the default first
(§D-S21-falsepos). Both are BACKLOG rows, both are questions, neither is decided here.

## Edge cases

- **Empty mapping** → `redact_mapping({})` returns `{}` without touching the redactor.
- **A non-`str` key** (`{1: "x"}` from a hand-built diff) → `str(key)` for the predicate; the
  original key is preserved in the output.
- **A cyclic diff** → `"<cycle>"` at the revisit point; the walk terminates.
- **A 20-deep `model_dump()`** → clipped at `max_depth=6` with `"<max-depth>"`; the shape above the
  cut is intact.
- **A 50 000-key dict** → first `max_items` kept plus a `"<truncated>"` marker.
- **A redactor that returns the value unchanged** → legal; that is its decision. A redactor that
  **raises** is not trusted (§D-S21-failsafe).
- **`redact_mapping(data, redactor, policy=…)` with both** → `ValueError` at the call, not a silent
  winner.
- **`set_default_redactor()` mid-process** → subsequent calls use it; the `lru_cache` is keyed on
  the pattern tuple and match mode, so no stale decision survives.
- **A span parameter named `signature`** → **not** redacted, because `"signature"` is in the opt-in
  `EXTENDED_REDACT_PATTERNS`, not in `DEFAULT_REDACT_PATTERNS`. Deliberate (§D-S21-patterns) and
  asserted, so a future "completeness" edit cannot land silently.
- **An audit chain spanning the redactor being enabled** → `verify_chain()` returns `True`
  (§D-S21-hashchain, asserted at Step 12).
- **`_audit_diff` overridden to return a *different* dict on every call** → the chain still
  verifies (it hashes what was stored), but the audit trail is non-deterministic. Documented as a
  Pitfall, not defended against.
- **`error_params()` returning a nested dict** → walked with the same depth/cycle guards.
- **A query string with a repeated key** (`?token=a&token=b`) → both values redacted;
  `parse_qsl`/`urlencode` round-trip preserves order and count.
- **A query string that is not valid URL encoding** → returned unchanged, logged at DEBUG; a
  logging helper must never raise (§D-S21-failsafe).

## Verification

```bash
uv sync --all-packages --all-extras

# Phase 1 alone (it ships alone, and changes nothing)
uv run pytest varco_core/tests/test_redaction.py \
              varco_core/tests/test_redaction_extraction.py \
              varco_core/tests/test_observability_params.py \
              varco_core/tests/test_observability.py -q

# Phases 2-5
uv run pytest varco_core/tests/test_redaction_error_params.py \
              varco_core/tests/test_error_envelope_settings.py \
              varco_core/tests/test_audit_redaction.py \
              varco_core/tests/test_audit.py \
              varco_core/tests/test_audit_chain.py \
              varco_core/tests/test_redaction_posture.py \
              varco_fastapi/tests/test_logging_redaction.py \
              varco_fastapi/tests/test_middleware_order.py -q

# sibling-boundary proofs (must both be empty)
git diff --exit-code varco_fastapi/varco_fastapi/app.py \
                     varco_fastapi/varco_fastapi/middleware/__init__.py \
                     varco_fastapi/varco_fastapi/posture.py
git diff -U0 varco_core/varco_core/service/audit.py   # review: no hunk inside :267-444

uv run python scripts/api_surface.py --check          # MUST be clean before committing
uv run python scripts/import_budget.py --check --warn-only
make bench
make lint && make type-check && make test
```

**DoD:**
1. Step 3 proves object identity between the two `DEFAULT_REDACT_PATTERNS` paths and that
   `varco_core.redaction` imports nothing from `varco_core.observability`. Phase 1 changes no
   behaviour and merges alone.
2. Step 6 shows the two incumbent span suites pass **unmodified**.
3. Step 9 proves every in-tree `ServiceException`'s `params` is byte-identical, including
   `ServiceAuthorizationError` still excluding `reason`.
4. Step 12 proves a hash chain spanning the redactor's enablement verifies as `True`.
5. Step 17 asserts the `check`-id set as a literal, so a future collector can be written against
   §D-S21-posture's table without reading this plan's code.
6. ⛔ **Zero diff** in `varco_fastapi/varco_fastapi/app.py`, `middleware/__init__.py` (Plan 041) and
   `posture.py` (Plan 036); **zero diff** inside `audit.py:267-444` (Plan 039). Checked by the two
   `git diff` commands above, not by assertion.
7. `api_surface.py --check` green with the regenerated snapshot committed; `import_budget.py`
   reports no `varco_core` breach.

## Parked

| Item | Why | Un-park trigger |
|---|---|---|
| Value scanning / content classification (`redact_text`, card/JWT/`whsec_` detection) | §D-S21-shape: a denylist that is wrong once is a leak, false positives destroy audit data, and per-string regex on the audit hot path contradicts §D-S21-perf | A consumer reports a real leak that key-name matching structurally cannot catch, **and** brings a false-positive budget |
| `bind_redactor(container, redactor)` | §D-S21-di: no varco component injects `Redactor`, so the binding would resolve for nobody | The first varco component that injects `Redactor` — the `varco_core.tls.bind_trust_store` shape applies unchanged |
| Wiring `inspect_redaction_posture()` into Plan 036's `SecurityPosture` | §D-S21-posture: `redaction.audit.disabled` would fire `warn` on every existing app, and `varco_fastapi/posture.py` is a 036 file | The audit-redaction default flip (4.0), when "opted out" rather than "not opted in" is the reportable state |
| DLQ payload redaction (`AbstractDeadLetterQueue` stores the serialized `Event`) | A real fifth surface, but it is the *event* payload — redacting it destroys the redrive path `varco_core.event.redrive` exists to serve | A consumer needs DLQ retention beyond a secret's rotation window |
| `WebhookDelivery` request-body redaction | A real sixth surface, owned by Plan 031's module, and delivery bodies are the replay evidence | Webhook delivery records outlive their payload's sensitivity in a real deployment |
| `PII_REDACT_PATTERNS` as any surface's default | §D-S21-patterns: PII in an audit trail is frequently the point, and crypto-shredding is varco's shipped answer | A regulator-driven requirement that no PII be readable in the audit store, with crypto-shredding ruled out |
| Retroactive redaction of already-stored audit rows | §D-S21-hashchain: rewriting a stored `diff` invalidates that row's `entry_hash()` and every subsequent `prev_hash` | Never, as stated — the supported paths are 039's retention sweep and crypto-shredding |

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| **The live leak is not closed by default** — a `password_hash` on a read DTO keeps landing in the audit table until an operator opts in | **High — it is the row's only live leak** | Argued, not overlooked (§D-S21-audit): the substring matcher's `shipping_address` false positive makes an on-by-default flip irresponsible. Carried in the upgrade note's ⚠️, the Pitfalls table, `redaction.audit.disabled`, and a BACKLOG flip row conditioned on word matching |
| **Extraction silently changes span redaction** | High if it happened | Phase 1 is identity-asserted (Step 3), runs the incumbent suites unmodified (Step 6), merges alone, and touches `sanitize_value`/`ParamCaptureConfig`/`_render_captured` not at all |
| **`error_params` redaction breaks an out-of-tree exception's contract** (a client parsing `params["author"]` gets `"[REDACTED]"`) | Medium | §D-S21-falsepos is the named cause; the placeholder is visible in the body rather than silent; `VARCO_ERROR_REDACT_PARAMS=false` reverts; `match_mode="word"` fixes it properly; the upgrade note names it |
| **A future edit "completes" `DEFAULT_REDACT_PATTERNS`** and silently changes span capture | Medium | Step 1 asserts `"signature"` is **absent** from the default and present in the opt-in tuple; Step 3 asserts the 15 literals in the test body; §Non-goals says it in words |
| **Audit redaction is mistakenly applied on the read path**, breaking `verify_chain()` on every row | **High — a data-correctness incident that looks like tampering** | §D-S21-hashchain forbids it in a `DESIGN:` block at the hook, in CLAUDE.md's rules, and in the Pitfalls table; Step 12 asserts the mechanism (recomputing over a mutated `diff` yields `HashMismatch`) so the failure mode is a documented, tested fact |
| **`COVERAGE.md` edit collides with Plans 039 and 042** | Low | One appended row; whichever lands last rebases. Named so it is expected — the identical note 039 carries |
| **Scope creep into Plan 041's middleware files** | Low | §D-S21-logging forbids `app.py` and `middleware/__init__.py`; DoD 6 makes the absent diff a checked condition, not a promise |
| **Scope creep into Plan 039's `audit.py` half** | Low | This plan touches only `audit.py:512-606` + the docstring at `:527-529`; DoD 6's `git diff -U0` review covers it |
| **The `lru_cache` grows on attacker-influenced keys** | Low | `maxsize=4096`; audit keys are schema-derived; cleared by `reset_redaction_state()`. The `InMemoryRateLimiter` unbounded-keyspace lesson applied pre-emptively |
| ⚠️ **ASSUMPTION — no in-tree `error_params()` key matches any of the 15 patterns.** Read from source for all nine overrides (`exception/service.py:106`, `:166`, `:218`, `:270`; `idempotency.py:85`, `:121`, `:149`; `body_limit.py:64`; `rate_limit.py:122`) but *not* executed | Medium | Step 9 turns it into an executed, per-class assertion **before** Step 10 changes anything |
| ⚠️ **ASSUMPTION — no out-of-tree consumer imports `DEFAULT_REDACT_PATTERNS` from a path other than `varco_core.observability.params`.** Verified in-repo (the only importer is `params.py` itself); out-of-repo is an assumption | Low | The name is not in `varco_core.__all__` nor in `varco_core.observability.__all__` (§D-S21-exists), so the only documented path is preserved by identity |
| ⚠️ **ASSUMPTION — `AuditEntry.diff` is always JSON-serializable in practice** (it is a `model_dump()`, `audit.py:538`), so `json_safe` never fires on the audit path | Low | If it does fire, the result is `"<TypeName>"` in the diff instead of a serialization failure at `save()` — strictly better than today, and the hash is over whatever is stored either way |
| ⚠️ **ASSUMPTION — `object.__setattr__` pattern normalisation in a frozen `__post_init__` passes `mypy --strict`.** Standard idiom, not verified against the pin (`mypy==2.3.1`) | Low | If it does not, drop normalisation and lowercase inside the cached predicate — a one-line fallback with the same measured cost after the first call |

## Open questions

1. **Should `inspect_redaction_posture()` ship with no in-tree caller?** §D-S21-posture argues yes
   (the 4.0 flip needs a warn-only stage and a stable contract; wiring it now means editing a Plan
   036 file and emitting `warn` on every app). Decide at Step 18 with 036's owner — lean ship it;
   the fallback is to drop Phase 5 entirely and file the whole inspector as a BACKLOG row, which
   costs this plan nothing else.
2. **Should `match_mode="word"` be the recommendation or the default for `_audit_redactor`?** The
   plan recommends it in docs and defaults to `"substring"` for one-convention consistency
   (§D-S21-falsepos). A defensible alternative is `PolicyRedactor.for_payloads()` — a named
   constructor that returns the word-matching policy — so the recommendation is one call rather
   than a paragraph. Decide at Step 13; lean the named constructor if it reads well.
3. **RESOLVED (repair round).** Does `EXTENDED_REDACT_PATTERNS` need `"cookie"`-adjacent HTTP names
   (`set-cookie`, `x-api-key`) for the logging surface? `"cookie"` and `"api_key"`/`"apikey"` are
   already in the 15, and `"x-api-key"` substring-matches `"api_key"`… **only if the key uses an
   underscore**. Verified: `"api_key" in "x-api-key"` is `False` — the hyphenated header name was
   NOT caught by DEFAULT or EXTENDED. Fix: `"api-key"` added to `EXTENDED_REDACT_PATTERNS`
   (`varco_core/varco_core/redaction/patterns.py`), never to `DEFAULT_REDACT_PATTERNS`. Regression
   tests: `test_hyphenated_api_key_header_not_caught_by_default_alone` and
   `test_hyphenated_api_key_header_caught_with_extended_patterns` in
   `varco_core/tests/test_redaction.py`.

## BACKLOG entries this plan files

Added to `BACKLOG.md`'s live work table by Step 25, all as **questions with evidence**, none fixed
here:

| ID | Row | Evidence |
|---|---|---|
| new | **Should `DEFAULT_REDACT_PATTERNS` move to word-boundary matching in 4.0?** `"pin"` matches `shipping_address`/`mapping`/`typing`; `"auth"` matches `author`/`authority`. Harmless on developer-named span parameters, wrong on domain-named payload keys | §D-S21-falsepos; `params.py:307-309` |
| new | **Should `AuditLogMixin._audit_redactor` default to `PolicyRedactor()` in 4.0?** ⚠️ **Conditional on the row above** — flipping it under substring matching would blank `shipping_address` in every order audit | §D-S21-audit; `audit.py:538`, `:572-575` |
| new | **Should `create_varco_app` gain a `log_redactor=` keyword?** Deliberately not added here to keep a zero diff in `app.py` while Plan 041 owns middleware ordering | §D-S21-logging |
| new | **Should `inspect_redaction_posture()` become a `SecurityPosture` collector?** Un-park when the audit default flips, so the finding means "opted out" rather than "not opted in" | §D-S21-posture; `varco_fastapi/posture.py:485-573` |
| new | **DLQ and `WebhookDelivery` payload redaction** — the fifth and sixth surfaces, both deliberately out of S21's scope | §Parked |
