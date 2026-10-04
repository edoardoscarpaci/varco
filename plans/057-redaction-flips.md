# Plan 057 — Redaction flips: word-boundary default, audit redaction on by default, posture collector (FLIP-040-1, FLIP-040-2, RO-5)

Covers BACKLOG 4.0 rows **`FLIP-040-1`** (`BACKLOG.md:87` — *"`DEFAULT_REDACT_PATTERNS` moves to
word-boundary matching (`RedactionPolicy.match_mode="word"` default)"*), **`FLIP-040-2`**
(`BACKLOG.md:88` — *"`AuditLogMixin._audit_redactor` defaults to `PolicyRedactor()` — conditional
on `FLIP-040-1`"*), and **`RO-5`** (`BACKLOG.md:113` — *"`inspect_redaction_posture()` becomes a
`SecurityPosture` collector — un-parked by `FLIP-040-2`; the finding then means 'opted out'"*).
All three are in the **flip track** of the 4.0 slice map (`scratchpad/slice-map.md`, row 057):
independently mergeable, no DI dependency, no dependency on Plan 045's providify pin bump.

**Neighbours (do not absorb):** Plan 049 owns `RO-4` (`log_redactor=` as a module/setting); Plan
051 migrates the posture-collector list to providify multi-binding — this plan adds the collector
**the 3.2 way** (a private `_collect_*` function + a `partial(...)` in the wiring snippet) and 051
moves it; Plan 058 assembles this plan's §Migration into the consolidated 3.x→4.0 guide. `RO-8`
(DLQ / `WebhookDelivery` payload redaction) stays live in BACKLOG — out of scope.

**Design of record:** `plans/040-unified-redaction-seam.md` — §D-S21-falsepos (`:250-282`),
§D-S21-audit (`:368-404`), §D-S21-posture (`:589-614`), §Parked (`:964-974`), the BACKLOG rows it
filed (`:1016-1028`). Feature docs: `technical_docs/features/redaction.md`,
`technical_docs/features/security-posture.md`, `technical_docs/features/database-auditing.md`.
Rules: CLAUDE.md §Redaction (`CLAUDE.md:732-755`).

> ⚠️ **Two premises corrected from source while writing this plan; each reshapes a §D block.**
> (1) 3.2's `match_mode="word"` **does not tokenise the pattern** — `policy.py:130-131` is
> `tokens = _tokenize(key); return any(pattern in tokens for pattern in patterns)`, so the three
> multi-token defaults `"api_key"`, `"private_key"`, `"session_id"` (`patterns.py:44,47,49`) can
> **never** match in word mode (`_tokenize("api_key") == ("api", "key")`, and `"api_key" in
> ("api", "key")` is `False`). Flipping the default onto the shipped matcher would silently
> *un-redact* `api_key`/`apiKey`/`x-api-key` everywhere — the exact "unsafe direction" 040
> §D-S21-falsepos `:278-282` warned about. §D-word redefines the matcher first; Step 1 records the
> failing repro before any line moves. (2) `redact_mapping()` applies `json_safe()` to **every
> surviving leaf** by default (`redactor.py:244-245`; `RedactionPolicy.render_non_json = True`,
> `policy.py:93`), and `AuditLogMixin` feeds it `read_dto.model_dump()` (`audit.py:616, 657-658`),
> which is pydantic's *python* mode — `datetime`/`UUID`/`Decimal` objects, not strings. A default
> `_audit_redactor = PolicyRedactor()` would therefore rewrite every `created_at` in every audit
> row to the literal `"<datetime>"`. Plan 040 filed this as an unexecuted ⚠️ ASSUMPTION
> (`plans/040:991`); §D-audit-policy resolves it with `render_non_json=False` on the audit default.

## Goal

After this plan: `RedactionPolicy()` and `PolicyRedactor()` match on **word-boundary tokens**
(`shipping_address` is not redacted by `"pin"`, `author` is not redacted by `"auth"`, and
`api_key`/`apiKey`/`x-api-key`/`Authorization` **are** redacted), with `match_mode="substring"` as
the explicit escape hatch; every `AuditLogMixin` subclass redacts its create/update diff **by
default** through a named, audit-specific policy that leaves non-sensitive `datetime`/`UUID`/
`Decimal` leaves byte-identical, with `_audit_redactor = None` as the explicit opt-out;
`inspect_redaction_posture()` runs inside `SecurityPosture` as a sixth collector, where
`redaction.audit.disabled` now means "opted out". The fifteen `DEFAULT_REDACT_PATTERNS` literals
are unchanged — the *matching* changes, not the list.

## Non-goals

- **Value scanning** of any kind (card/JWT/`whsec_` detection) — CLAUDE.md rule; still parked.
- **Redacting `list`-valued leaves inside a mapping** (`items: [{"password": …}]`) — `_walk()`
  (`redactor.py:219`) recurses into `Mapping` only; unchanged. Named in §Risks, not fixed.
- **Retroactive redaction of stored audit rows** — 040 §Parked: never.
- **`RO-4` `log_redactor=`** (Plan 049), **`RO-8`** (stays live), multi-binding of collectors
  (Plan 051), the consolidated migration guide (Plan 058).
- **Adding, removing, or reordering any `DEFAULT_REDACT_PATTERNS` literal.** The "verbatim
  fifteen" test stays. Every un-redaction this flip causes is answered by the escape hatch or by
  the caller adding a pattern, never by growing the default list (§D-word-defaults).
- **A `VARCO_OTEL_CAPTURE_PARAMS_MATCH_MODE` env var** for span capture — Plan 048 owns the
  settings recipe; this plan adds a code-level `ParamCaptureConfig.redact_match_mode` field only.
- **Renaming any posture `check` id.** 040 §D-S21-posture committed to four stable ids
  (`posture.py:80-91`); this plan changes their *meaning* and *wording*, never their spelling.
- **Touching `varco_fastapi/varco_fastapi/app.py` or `middleware/__init__.py`** — Plan 041's
  ordering boundary (040 §D-S21-logging) still holds; this plan's `varco_fastapi` diff is
  `posture.py` and its tests only.

## Context (verified from source, 2026-09-13)

| Fact | Where | Why it matters |
|---|---|---|
| `DEFAULT_REDACT_PATTERNS` — 15 literals, three multi-token (`api_key`, `private_key`, `session_id`) | `varco_core/varco_core/redaction/patterns.py:37-53`; ⛔ "verbatim, do not edit" comment `:32-36` | The list stays; the comment's "**substring**" wording (`:29`) must change |
| `EXTENDED_REDACT_PATTERNS` carries `"api-key"` **only** because `"api_key" in "x-api-key"` is `False` under substring | `patterns.py:64-77`; 040 Open Question 3 (`plans/040:1006-1014`) | Under word mode `api_key` tokenises to `("api","key")` and matches `x-api-key` — the entry becomes redundant but is pinned by `test_redaction.py:94-106`; keep it, re-document it |
| `RedactionPolicy.match_mode: Literal["substring","word"] = "substring"` | `policy.py:90` | The flip site |
| Word tokenizer `_WORD_TOKEN_RE = r"[A-Z]+(?=[A-Z][a-z0-9])\|[A-Z]?[a-z0-9]+\|[A-Z0-9]+"` — digits glued to the preceding run | `policy.py:58`, `_tokenize` `:106-121` | `password2` → `("password2",)` ≠ `"password"`; §D-word makes digit runs their own token |
| Word predicate: `any(pattern in tokens …)` — pattern **not** tokenised | `policy.py:126-131` | The defect in premise (1) |
| `lru_cache(maxsize=4096)` keyed on `(key, patterns, match_mode)`; cleared by `reset_redaction_state()` | `policy.py:124-125`; `redactor.py:384-398` | A changed default is a different key; no stale decisions |
| `patterns` lowercased once in `__post_init__` via `object.__setattr__` | `policy.py:95-103` | The same idiom validates word-mode patterns (§D-word) |
| `redact_mapping()` → `_walk()` applies `json_safe()` to each non-redacted leaf when `render_non_json` | `redactor.py:244-245`, `:300-310`; `json_safe` `:145-173` — `datetime`/`UUID`/`Decimal` → `"<TypeName>"` | Premise (2) |
| `AuditLogMixin._audit_redactor: Redactor \| None = None`; `_audit_diff()` is the only caller | `audit.py:513`, `:562-564`; opt-in rationale comment `:500-512` | The flip site; the comment block is rewritten |
| Diffs are `read_dto.model_dump()` (python mode) | `audit.py:616`, `:657-658` | Contains `datetime`/`UUID`/`Decimal` in practice |
| `entry_hash()` canonicalises with `json.dumps(..., default=str)` over the **stored** diff | `audit.py:155-186` | Redact-at-write is chain-safe; the read-path rule is unchanged |
| `inspect_redaction_posture()` — four ids; `:169` tests `match_mode == "substring"` | `varco_core/varco_core/redaction/posture.py:58-179` | Wording of the `substring_mode` and `audit.disabled` findings changes to "opted out" |
| Span capture: `_is_redacted()` builds `RedactionPolicy(patterns=config.redact_patterns)` with the **default** `match_mode` | `varco_core/varco_core/observability/params.py:299-307`; `ParamCaptureConfig.redact_patterns` docstring says "substrings" `:149-153`, field `:177` | Span capture flips with the default; it needs its own escape hatch field |
| `error_message_for()` → `redact_mapping(output_params)` with the process default | `varco_core/varco_core/exception/http.py:338-351` | Flips with the default; an out-of-tree `{"author": …}` stops being `"[REDACTED]"` (the §D-S21-errparams ❌ is resolved) |
| `RequestLoggingMiddleware(redactor=None)` never falls back to the default | `varco_fastapi/varco_fastapi/middleware/logging.py:65`, `:157` | Unaffected unless a caller passed `PolicyRedactor()` |
| `SecurityPostureLifecycle(collectors=[…])` — a caller-supplied list; five private `_collect_*` functions, each taking optional resolved objects as kwargs; `_wrap_collector` turns `ImportError` into one `NOT_ASSESSED` finding | `varco_fastapi/varco_fastapi/posture.py:199-255`, `:258-292`, `:400-425`, `:485-771`; module docstring `:4-9` says "four sibling … plus one local" | The 3.2 way to add a collector; 051 migrates it |
| `_collect_http` re-emits a sibling's severities via `PostureSeverity(f.severity)` | `posture.py:585-601` | The shape `_collect_redaction` copies |
| Tests pinning the current defaults | `varco_core/tests/test_redaction.py:55-58` (`match_mode == "substring"`), `:109-118` (hyphenated header NOT caught by default), `:161-167`; `test_observability_params.py:196-225` (parametrised names incl. `X_Authorization`, `api_key`, `secret_token`); `test_audit_redaction.py:101-142` (default is `None`, `password` stored verbatim); `test_redaction_posture.py:31-40` (`class _NoRedactorService(AuditLogMixin): pass` emits `audit.disabled`) | Each is rewritten in the step that flips the behaviour it pins |
| No `AuditLogMixin` subclass in `examples/`, `varco_sa`, `varco_beanie`, `varco_fastapi` source or tests | `rg AuditLogMixin` → only `varco_core/tests/{test_audit,test_audit_redaction,test_redaction_posture}.py` | The audit flip's in-repo blast radius is three test files; `test_audit.py`'s diffs (`{"total": 99.0}`, `{"status": …}`, `{"field": "value"}`) carry no sensitive name and must pass **unmodified** |

## Design

### §D-word — "word" matching, defined precisely

**Chosen: a key and every pattern are both tokenised by the same tokenizer; a pattern matches when
its token sequence occurs as a contiguous subsequence of the key's tokens, where the pattern's
last token may also match its simple plural (`token + "s"`).** `match_mode="word"` becomes the
`RedactionPolicy` default.

**Tokenizer** (`_tokenize`, `policy.py`): applied to the **original-case** string, then each
token lowercased. Boundaries, in this order of precedence:

| Boundary | Example → tokens |
|---|---|
| Any non-alphanumeric run (`_`, `-`, `.`, space, `/`, `:`) | `x-api-key` → `x api key`; `shipping_address` → `shipping address` |
| lower→Upper camelCase transition | `apiKey` → `api key`; `userPin` → `user pin` |
| Acronym followed by a capitalised word | `HTTPAuthorization` → `http authorization` |
| **Digit runs are their own token** (new — 3.2 glued them to the preceding run) | `password2` → `password 2`; `sha256` → `sha 256`; `X-Hub-Signature-256` → `x hub signature 256` |

Regex: `[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+`.

**Predicate** (`_is_sensitive_key_cached`, word branch): for each pattern `p`,
`ptoks = _tokenize(p)`; match iff there is an index `i` such that `ktoks[i+j] == ptoks[j]` for
all `j < len(ptoks)-1` and `ktoks[i+len(ptoks)-1] in (last, last + "s")`. A pattern whose
tokenisation is empty (e.g. `"-"`) is rejected at `RedactionPolicy` construction with
`ValueError` when `match_mode == "word"` (fail loud on a config error; substring mode keeps
accepting it). Pattern tokenisation is memoised per `patterns` tuple with a small
`lru_cache` so a cache miss on the key does not re-tokenise all fifteen patterns.

**The contract table — every row becomes a test** (`test_redaction_word_mode.py`, Step 2),
against `RedactionPolicy()` (i.e. the fifteen defaults, word mode):

| Key | Redacted? | Why |
|---|---|---|
| `password`, `PASSWORD`, `password_hash`, `passwordHash`, `new_password`, `passwords`, `password2` | ✅ | token `password` / plural / digit token |
| `passwd`, `db_passwd` | ✅ | token `passwd` |
| `secret`, `client_secret`, `clientSecret`, `SECRET_KEY`, `secrets` | ✅ | token `secret` |
| `token`, `access_token`, `refreshToken`, `x-auth-token`, `id_token`, `tokens` | ✅ | token `token` |
| `Authorization`, `authorization`, `X-Authorization`, `HTTPAuthorization` | ✅ | token `authorization` |
| `auth`, `auth_token`, `basic_auth`, `authKey` | ✅ | token `auth` |
| `api_key`, `apiKey`, `API_KEY`, `x-api-key`, `X-Api-Key`, `api_keys` | ✅ | pattern `api_key` → `("api","key")` contiguous subsequence |
| `apikey`, `APIKEY` | ✅ | token `apikey` |
| `credential`, `credentials`, `aws_credentials` | ✅ | token `credential` + plural |
| `private_key`, `privateKey`, `PRIVATE_KEY` | ✅ | pattern tokens `("private","key")` |
| `cookie`, `cookies`, `set-cookie` | ✅ | token `cookie` |
| `session_id`, `sessionId` | ✅ | pattern tokens `("session","id")` |
| `otp`, `otp_code`, `pin`, `user_pin`, `userPin`, `pin_code`, `pins`, `ssn` | ✅ | single tokens |
| `shipping_address`, `shipping`, `mapping`, `typing`, `grouping`, `stripping` | ❌ | `pin` is not a token of any of them (040's headline false positive) |
| `author`, `authored_at`, `authority`, `authorship` | ❌ | `auth` ≠ `author` |
| `page`, `harmless_field`, `primary_key`, `cache_key`, `idempotency_key`, `partition_key` | ❌ | `key` alone was never a pattern (`patterns.py:18-21`) and `("api","key")` needs `api` |
| `oauth`, `oauth_client` | ❌ | token `oauth` ≠ `auth` (**narrower than substring** — documented) |
| `csrftoken`, `csrfmiddlewaretoken`, `secretkey`, `totp_code`, `sessionid` | ❌ | no separator, so one token; substring caught the first four (**narrower** — documented in §Migration; `sessionid` was never caught by either mode) |
| `oauth_token`, `totp_secret` | ✅ | via `token` / `secret` |

And `RedactionPolicy(match_mode="substring")` is **byte-identical to 3.2** for every row above
(the substring branch at `policy.py:132-133` is untouched) — asserted for the four
false-positive rows and the five "narrower" rows so the escape hatch is proven, not promised.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) tokenised pattern, contiguous subsequence, last-token plural** (chosen) | Every multi-token default works in both spellings and both cases; hyphenated headers are caught by DEFAULT alone; `author`/`shipping` cannot match; one explicit plural rule closes the most common un-redaction (`credentials`, `secrets`, `tokens`, `cookies`, `passwords`) without growing the tuple; stdlib `re` only | Suffix-glued names (`csrftoken`) are missed — accepted and listed; the plural rule is one more sentence to learn |
| (2) 3.2's matcher as-is (pattern as one token) | Zero matcher change | Un-redacts `api_key`/`apiKey`/`private_key`/`session_id` — premise (1). Rejected outright |
| (3) prefix-token matching (`token.startswith(pattern)`) | Catches `passwords`, `csrftoken` (no — that is a suffix), `pinned` | `author.startswith("auth")` — reintroduces the headline false positive. Rejected |
| (4) add plural/suffix literals to `DEFAULT_REDACT_PATTERNS` (`credentials`, `secrets`, …) | No matcher rule | Endless (`api_keys`, `private_keys`, `session_ids`, …); breaks the verbatim-fifteen pin; two homes for one fact (a literal and the rule it stands in for). Rejected |
| (5) regex patterns (`RedactionPolicy(match_mode="regex")`) | Maximum expressiveness | A third mode is a knob nobody asked for; an out-of-tree `Redactor` already covers it (`redactor.py:85-111`). Rejected; not parked — the Protocol is the extension point |

### §D-word-defaults — the fifteen literals do not change; `"api-key"` in EXTENDED stays, re-documented

The whole migration story is "same list, different matching". Every un-redaction §D-word's
"narrower" rows introduce is answered by (a) the escape hatch, or (b) the caller adding the exact
key as a pattern (`RedactionPolicy(patterns=DEFAULT_REDACT_PATTERNS + ("csrftoken",))`) — never
by this plan growing the tuple. `patterns.py:29-36`'s ⛔ comment is reworded from "substring" to
"word-boundary tokens (see `policy.py`)" and keeps its "do not add/remove/reorder" instruction.
`EXTENDED_REDACT_PATTERNS`' `"api-key"` entry (`patterns.py:64-77`) is now redundant under word
mode (`api_key` and `api-key` tokenise identically) but is **kept**: the tuple is pinned by
`test_redaction.py:94-106`, and it still matters to a `match_mode="substring"` caller. Its
DESIGN comment says exactly that.

### §D-span — span capture flips with the default and gets its own escape hatch

`_is_redacted()` (`params.py:299-307`) constructs `RedactionPolicy(patterns=config.redact_patterns)`
and therefore inherits the new default — `@span`-captured parameters named `shipping_address`
stop being redacted, `api_key`/`apiKey` keep being redacted. A caller whose custom
`redact_patterns` relied on substring semantics (`("pwd",)` to catch `dbpwd`) needs a way to
say so **at the config they already hold**: `ParamCaptureConfig` gains
`redact_match_mode: Literal["word", "substring"] = "word"`, threaded into the `RedactionPolicy`
`_is_redacted` builds. ✅ One additive frozen-dataclass field, same object the caller already
edits; ❌ a second place the mode is spelled (accepted — `ParamCaptureConfig` already duplicates
`redact_patterns` for the same reason, `params.py:177`). No env var (Plan 048's recipe).

### §D-audit-policy — the audit default is a **named, audit-specific policy**, not `PolicyRedactor()`

**Chosen: `AUDIT_REDACTION_POLICY = RedactionPolicy(render_non_json=False)` exported from
`varco_core.redaction`, and `AuditLogMixin._audit_redactor: Redactor | None =
PolicyRedactor(AUDIT_REDACTION_POLICY)`.** `match_mode` is inherited from the new default (word)
— there is deliberately **no** explicit `match_mode="word"` in the constant, so the constant can
never silently diverge from the seam's default; Step 7's precondition test pins
`AUDIT_REDACTION_POLICY.match_mode == "word"` and is the mechanical form of BACKLOG's
"conditional on `FLIP-040-1`".

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) `PolicyRedactor(AUDIT_REDACTION_POLICY)` with `render_non_json=False`** (chosen) | Non-sensitive leaves are **byte-identical** to 3.2 — `datetime`/`UUID`/`Decimal` reach the bus/JSON column exactly as before, and the downstream serializer (`JsonEventSerializer`, SA's `JSON` column) keeps doing what it did; only secret-named keys change; a user extends it with `dataclasses.replace(AUDIT_REDACTION_POLICY, patterns=…)` | One more exported constant (`__all__` addition → snapshot regen) |
| (2) plain `PolicyRedactor()` (the BACKLOG row's literal wording) | Zero new names | Premise (2): every `created_at` becomes `"<datetime>"`, every `pk: UUID` becomes `"<UUID>"`, every `Decimal` amount becomes `"<Decimal>"` — a silent, irreversible, hash-attested corruption of every audit row. Rejected; the row's *intent* ("redact by default") is met by (1) |
| (3) teach `json_safe()` to render `datetime`/`UUID`/`Decimal` as strings | Fixes (2) for audit *and* `error_params()` | Changes `error_params()` output shape for every in-tree exception that Plan 040 Step 9 asserted byte-identical, and makes an *audit* value a string where 3.2 stored a native type — still not byte-identical. Rejected; `render_non_json` exists precisely to switch this off per surface |
| (4) keep opt-in; only document `AUDIT_REDACTION_POLICY` | No persisted-data change | 040 §D-S21-audit's three reasons for opt-in were: (1) substring false positives — closed by §D-word; (2) irreversible persisted change — true, and *that is the point*: the live `password_hash` leak (`plans/040:980`, "High — the row's only live leak") is closed by default; (3) hash-chain interaction — redact-at-write is chain-safe (`audit.py:540-545`, `test_audit_redaction.py:265-274`). With all three answered, opt-in is the less safe default. Rejected |

**Opt-out:** `_audit_redactor = None` on the subclass — the same class attribute, the same
`_audit_diff()` short-circuit at `audit.py:562-563`, byte-identical to 3.2. It is what
`redaction.audit.disabled` now detects and calls "opted out" (§D-posture). Overriding
`_audit_diff()` still wins outright (`audit.py:528-531`).

**The read-path rule is unchanged and re-asserted.** Step 7 adds a test that drives the mixin's
`_after_create`/`_after_update` with the **default** redactor, builds `AuditEntry` rows from the
produced events via `AuditEntry.from_event()` in sequence, and asserts `verify_chain()` is `True`
— redacted-at-write rows hash what they store. The existing mutated-read-path test
(`test_audit_redaction.py:277-295`) stays verbatim.

### §D-posture — `_collect_redaction`, the 3.2 way; ids stable, meaning "opted out"

**Chosen: `_collect_redaction(*, service_classes=(), envelope_settings=None) ->
list[PostureFinding]` in `varco_fastapi/varco_fastapi/posture.py`, the same thin-adapter shape as
`_collect_http` (`:585-601`)** — import `varco_core.redaction.posture` inside the function body
(so an absent module degrades to `NOT_ASSESSED` through `_wrap_collector`, `:199-255`), call
`inspect_redaction_posture(service_classes=…, envelope_settings=…)`, re-emit each finding with
`check` unchanged, `severity=PostureSeverity(f.severity)`, `detail=f.detail`, and a remediation
from a four-row table keyed by `check`:

| `check` (unchanged spelling) | Severity (from the inspector) | Meaning **after this plan** | Remediation text |
|---|---|---|---|
| `redaction.audit.disabled` | `warn` | An `AuditLogMixin` subclass **opted out** (`_audit_redactor = None`, no `_audit_diff` override) — audit diffs are written unredacted | "Remove `_audit_redactor = None` (the 4.0 default redacts), or override `_audit_diff()`; suppress with `VARCO_SECURITY_SUPPRESS=redaction.audit.disabled` if the opt-out is deliberate." |
| `redaction.error_params.disabled` | `warn` | `VARCO_ERROR_REDACT_PARAMS=false` | "Unset `VARCO_ERROR_REDACT_PARAMS` (default `true`)." |
| `redaction.default.custom` | `info` | An out-of-tree `Redactor` is the process default | "No action required; confirm the custom redactor covers `DEFAULT_REDACT_PATTERNS`." |
| `redaction.patterns.substring_mode` | `info` | The process default **opted out** of word matching (`set_default_redactor(PolicyRedactor(RedactionPolicy(match_mode="substring")))`) — 040's false positives apply again | "Drop `match_mode="substring"`, or add the exact keys you relied on as patterns under word mode." |

`warn` is the right tier for `audit.disabled`: under `enforce="refuse"` in `production`, a
`WARN` refuses startup (`posture.py:384-385`) — an explicit opt-out of a security default in
production *should* need an explicit suppression, exactly like `auth.revocation_unbound`.
`substring_mode` stays `info`: opting into substring redacts *more*, not less — a data-quality
concern, not a security one.

`inspect_redaction_posture()`'s **own** detail strings (`redaction/posture.py:132-136`,
`:174-177`) are reworded to "opted out" so a caller reading the core inspector without the
FastAPI collector sees the same meaning. The ids, the `Severity` literals, the signature, and
the never-raises contract are untouched.

`service_classes` remains **explicit caller input** — 036's rule for every collector
(`posture.py:686-689`, *"never re-derived by walking … or poking a sibling module's private
state"*).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) explicit `service_classes=` via `partial(...)`** (chosen) | 036-consistent; zero-arg callable when nothing is wired; Plan 051 replaces the `partial` with a multi-bound list without touching this function's body | An app must list its audited services once in the wiring snippet |
| (2) discover via `AuditLogMixin.__subclasses__()` recursively | Zero config | Picks up test doubles and abstract intermediates; a walk over global interpreter state is the "poking" 036 forbids; 051 makes it moot. Rejected |
| (3) new ids `redaction.audit.opted_out` | Reads better | 040 committed to the four ids as *stable* (`posture.py:40-41`, "never renamed once shipped"); an operator's `VARCO_SECURITY_SUPPRESS` list would silently stop matching. Rejected |
| (4) resolve `ErrorEnvelopeSettings` from the container inside the collector | Fewer kwargs | Not how any sibling collector works; `ErrorEnvelopeSettings()` is env-driven and the inspector already defaults to a fresh one (`redaction/posture.py:146`). Rejected |

### §D-order — FLIP-040-2 is gated on FLIP-040-1 by a test, not by discipline

Steps 2-6 (word default) must be green before Step 7 begins, and Step 7's first test asserts
`AUDIT_REDACTION_POLICY.match_mode == "word"` **and** that the default `_audit_redactor` leaves
`shipping_address`/`author` untouched — a reviewer who cherry-picks the audit flip without the
matcher gets a red test naming §D-S21-falsepos, not a corrupted audit table.

## Steps

All commands run from `/home/edoardo/projects/varco`. Phase A (Steps 1-6) → Phase B (7-9) →
Phase C (10-11) → Docs (12-13) → Verification (14). Steps 10-11 depend only on Phase B's
meaning change and could land in the same PR or the next; do not land them before Phase B.

### Phase A — FLIP-040-1: word-boundary default

1. [x] **Preflight — record the two premises as executed facts.** Run and paste output into
       `/tmp/claude-1000/-home-edoardo-projects-varco/625af1a2-0cf8-4199-b924-0b3f986861e6/scratchpad/057-preflight.txt`:
       `uv run python -c "from varco_core.redaction import RedactionPolicy, is_sensitive_key as k; p=RedactionPolicy(match_mode='word'); print([k(x,p) for x in ('api_key','apiKey','x-api-key','private_key','session_id','password2','credentials')])"`
       → expected today `[False, False, False, False, False, False, False]` (premise 1);
       `uv run python -c "from datetime import datetime; from uuid import uuid4; from varco_core.redaction import redact_mapping, PolicyRedactor; print(redact_mapping({'created_at': datetime.now(), 'pk': uuid4(), 'name': 'x'}, PolicyRedactor()))"`
       → expected today `{'created_at': '<datetime>', 'pk': '<UUID>', 'name': 'x'}` (premise 2).
       Re-read and confirm every `file:line` in §Context. If either output differs, stop and
       revise §D-word / §D-audit-policy before continuing.
       **Verify**: the scratchpad file exists with both outputs.

2. [x] `varco_core/tests/test_redaction_word_mode.py` (**new**, red) — the §D-word contract:
       (a) `TestTokenizer` — one parametrised test over the tokenizer table (`x-api-key`,
       `apiKey`, `HTTPAuthorization`, `password2`, `sha256`, `X-Hub-Signature-256`,
       `shipping_address`, `Authorization`, `__dunder__` → `("dunder",)`, `""` → `()`);
       (b) `TestDefaultWordContract` — one parametrised test per row of the ✅/❌ table against
       `RedactionPolicy()` (every key listed, expected bool), docstring citing 040
       §D-S21-falsepos; (c) `TestSubstringEscapeHatch` — the four false-positive rows are
       `True` and the five "narrower" rows are `True` under `RedactionPolicy(match_mode="substring")`
       (byte-identical 3.2 semantics); (d) `test_empty_token_pattern_rejected_in_word_mode`:
       `RedactionPolicy(patterns=("-",))` → `ValueError`; `RedactionPolicy(patterns=("-",),
       match_mode="substring")` constructs; (e) `test_plural_rule_applies_to_last_token_only`:
       `("api","keys")` matches `api_key`, `("apis","key")` does not; (f)
       `test_cache_key_includes_match_mode`: same key, both modes, different answers after
       `reset_redaction_state()`. All sync `def` (pure functions) — this module has no async.
       **Verify**: `uv run pytest varco_core/tests/test_redaction_word_mode.py -q` → red on
       (b)/(d)/(e) and on the `api_key` tokenizer rows; (c) already passes.

3. [x] `varco_core/tests/test_redaction.py` — flip the pinned defaults: `:55-58`
       `test_default_match_mode_is_substring` → `test_default_match_mode_is_word` asserting
       `"word"`; `:109-118` `test_hyphenated_api_key_header_not_caught_by_default_alone` →
       `test_hyphenated_api_key_header_caught_by_default_under_word_mode` (`True` for
       `x-api-key`/`X-Api-Key` with `RedactionPolicy()`), plus a sibling
       `test_hyphenated_api_key_header_not_caught_by_default_under_substring_mode` keeping the
       3.2 assertion under `match_mode="substring"` (the 040 Open Question 3 record survives);
       `:121-132` unchanged (EXTENDED still catches it in either mode — add a `match_mode=
       "substring"` variant); `:161` rename to `test_case_insensitive_word_match_by_default`
       (the three assertions hold under word mode). `:169-193` unchanged.
       `varco_core/tests/test_observability_params.py:196-225` — rename
       `test_redact_patterns_match_by_substring_case_insensitive` → `…_by_word_…`, fix the NOTE
       comment (`:199-201`), keep the five names (all match under word mode); add
       `test_shipping_address_param_not_redacted_by_default` (a `shipping_address=` parameter is
       captured verbatim) and `test_redact_match_mode_substring_escape_hatch`
       (`ParamCaptureConfig(redact_patterns=("pwd",), redact_match_mode="substring")` redacts
       `dbpwd`; the default does not).
       **Verify**: both files red on the renamed/added tests only.

4. [x] `varco_core/varco_core/redaction/policy.py` — implement §D-word: `_WORD_TOKEN_RE` →
       `r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+"`; `match_mode` default `"word"`;
       `__post_init__` additionally raises `ValueError` naming the offending pattern when
       `match_mode == "word"` and `_tokenize(p) == ()`; a memoised `_pattern_tokens(patterns:
       tuple[str, ...]) -> tuple[tuple[str, ...], ...]` (`lru_cache(maxsize=64)`); the word
       branch of `_is_sensitive_key_cached` implements contiguous-subsequence + last-token
       plural. Rewrite the module DESIGN block (`:8-24`, which currently argues *for* substring
       as default) into "DESIGN: word-boundary default (Plan 057 / FLIP-040-1)" with ✅ (no
       false positives on domain keys; multi-token defaults work in every spelling; hyphenated
       headers caught by DEFAULT) / ❌ (suffix-glued names like `csrftoken` are missed —
       escape hatch or explicit pattern; the plural rule is one more rule to know); keep the
       §D-S21-perf block. Update the `RedactionPolicy` and `is_sensitive_key` docstrings'
       `Args`/`Edge cases` (the "substring: incumbent semantics" line becomes "substring: the 3.2
       semantics, byte-identical, the escape hatch"). Add `reset_redaction_state()` clearing of
       `_pattern_tokens` (`redactor.py:384-398`).
       `varco_core/varco_core/redaction/patterns.py` — `:29-36` comment: "substring" → "word-
       boundary token match (semantics: `policy.py`); still fail-closed over `include=`"; keep
       the ⛔ do-not-edit instruction verbatim. `:64-70` (`"api-key"` DESIGN): append "Plan 057:
       redundant under the 4.0 word default (`api_key`/`api-key` tokenise identically); kept
       because the tuple is pinned and a `match_mode="substring"` caller still needs it".
       **Verify**: Step 2 + Step 3 green; `uv run pytest varco_core/tests/test_redaction*.py
       varco_core/tests/test_observability_params.py -q`; the Step 1 premise-1 command now prints
       all `True`.

5. [x] `varco_core/varco_core/observability/params.py` — §D-span: `ParamCaptureConfig` gains
       `redact_match_mode: Literal["word", "substring"] = "word"` (after `redact_patterns`,
       `:177`), documented in `Args` (`:149-153` — reword "Case-insensitive substrings" to
       "patterns matched on word-boundary tokens by default; `redact_match_mode="substring"`
       restores the 3.x substring semantics"); `_is_redacted` (`:299-307`) passes
       `match_mode=config.redact_match_mode` and its comment drops "byte-identical … substring".
       **Verify**: Step 3's two new `test_observability_params.py` tests green; the rest of that
       file unmodified and green.

6. [x] `varco_core/varco_core/redaction/posture.py` — reword only: `:170-177` detail →
       "The process default redactor opted out of word-boundary matching
       (`match_mode='substring'`) — 040's known false positives apply (e.g. 'pin' redacts
       'shipping_address')."; module docstring `:7-11` ⛔ paragraph → "Wired into
       `varco_fastapi.posture.SecurityPosture` by Plan 057 (`_collect_redaction`); the four ids
       are stable — see the table in `technical_docs/features/redaction.md`."
       `varco_core/tests/test_redaction_posture.py` — add
       `test_default_process_redactor_emits_no_substring_finding` (after
       `reset_redaction_state()`, no `redaction.patterns.substring_mode`) and
       `test_substring_default_redactor_emits_opted_out_finding` (`set_default_redactor(
       PolicyRedactor(RedactionPolicy(match_mode="substring")))` → the id fires, `"opted out"`
       in `detail`; `finally: reset_redaction_state()`).
       **Verify**: `make test PKG=varco_core` green. **Phase A gate** — do not start Step 7
       until this is green.

### Phase B — FLIP-040-2: audit redaction on by default (after Phase A)

7. [x] `varco_core/tests/test_audit_redaction.py` (red) — rewrite the "Default" section
       (`:98-142`): (a) **first**, `test_audit_default_precondition_word_mode` — imports
       `AUDIT_REDACTION_POLICY` from `varco_core.redaction`, asserts `.match_mode == "word"`,
       `.render_non_json is False`, `.patterns == DEFAULT_REDACT_PATTERNS`, and that
       `_FakeMixinService(producer)._audit_redactor == PolicyRedactor(AUDIT_REDACTION_POLICY)`;
       docstring: "§D-order — FLIP-040-2 is conditional on FLIP-040-1; this test is the gate";
       (b) `test_create_diff_redacted_by_default` — `{"password": "hunter2", "field": "value"}`
       → `{"password": "[REDACTED]", "field": "value"}`; (c)
       `test_update_diff_redacted_in_both_halves_by_default`; (d)
       `test_default_does_not_redact_domain_keys` — `{"shipping_address": "1 Main St",
       "author": "alice", "pin": "1234"}` → only `pin` redacted; (e)
       `test_default_leaves_non_json_leaves_byte_identical` — `datetime`, `UUID`, `Decimal`,
       `date` values come out **as the same objects** (`is`), never `"<datetime>"` (premise 2);
       (f) `test_delete_diff_is_empty_by_default` (`{}` passes through the fast path,
       `redactor.py:297-298`); (g) `test_opt_out_with_none_is_byte_identical` — a subclass with
       `_audit_redactor = None` reproduces the three 3.2 assertions verbatim (`password` stored
       as `"hunter2"`); (h) `test_chain_over_default_redacted_rows_verifies_true` — drive
       `_after_create` then `_after_update` on the default service, build entries via
       `AuditEntry.from_event(event, seq=…, prev_hash=…)` (read its signature at
       `audit.py` before writing), assert `AuditRepository.verify_chain([...]) is True` and that
       `entries[0].diff["password"] == "[REDACTED]"`. Keep `:145-295` (opt-in override, hook
       ordering, the two chain tests) verbatim. Module docstring updated.
       `varco_core/tests/test_redaction_posture.py:31-40` — rewrite: `class
       _NoRedactorService(AuditLogMixin): pass` now asserts `redaction.audit.disabled` is
       **absent** (renamed `test_default_service_class_emits_no_disabled_finding`), and a new
       `class _OptedOutService(AuditLogMixin): _audit_redactor = None` asserts it is **present**
       with `"opted out"` in `detail`.
       **Verify**: red on (a)-(e), (h), and the posture rewrite; `test_audit.py` untouched and
       green (its diffs have no sensitive names and no non-JSON leaves).

8. [x] Implementation: `varco_core/varco_core/redaction/policy.py` — add
       `AUDIT_REDACTION_POLICY: RedactionPolicy = RedactionPolicy(render_non_json=False)` with
       a DESIGN block (§D-audit-policy's ✅/❌: why `render_non_json=False`, why no explicit
       `match_mode`, `dataclasses.replace(AUDIT_REDACTION_POLICY, patterns=…)` as the extension
       idiom); add to `__all__` (`:54`). `varco_core/varco_core/redaction/__init__.py` — import +
       `__all__` (`:52`, `:64-78`). `varco_core/varco_core/__init__.py` — add
       `AUDIT_REDACTION_POLICY` to the lazy map beside `RedactionPolicy` (`:372`, `:657`,
       `:1178`). `varco_core/varco_core/service/audit.py` — `:513` →
       `_audit_redactor: Redactor | None = PolicyRedactor(AUDIT_REDACTION_POLICY)` (import both
       from `varco_core.redaction` beside `redact_mapping`, `:74`); rewrite the `#:` block
       `:500-512` — "on by default as of 4.0 (Plan 057 / FLIP-040-2): the three 3.2 reasons for
       opt-in are each answered — (1) word-boundary matching (FLIP-040-1) removes the
       `shipping_address`/`author` false positives; (2) the persisted change is the point: it
       closes the `password_hash`-on-a-read-DTO leak that was 3.2's only live leak; (3)
       redact-at-write is hash-chain-safe (`_audit_diff` DESIGN below). Opt out with
       `_audit_redactor = None`; posture reports it as `redaction.audit.disabled`."; `_audit_diff`
       docstring `Returns` (`:557-560`) and the `_after_create`/`_after_update` `Edge cases`
       (`:600-606`, `:643-646`) reworded from "unless a redactor is set / byte-identical to
       pre-3.2" to "redacted through `AUDIT_REDACTION_POLICY` unless `_audit_redactor = None`".
       Then `uv run python scripts/api_surface.py` (one **addition** to `varco_core.__all__` —
       reported as a note, but the snapshot is regenerated and committed in this step).
       **Verify**: Step 7 green; `make test PKG=varco_core`; `uv run python
       scripts/api_surface.py --check` clean; `uv run python scripts/import_budget.py --check
       --warn-only` reports no `varco_core` breach (no new eager import — `varco_core.__init__`
       is PEP 562 lazy and `audit.py` already imported `varco_core.redaction`).

9. [x] **Phase B gate** — `uv run pytest varco_core/tests/test_audit.py
       varco_core/tests/test_audit_redaction.py varco_core/tests/test_redaction_posture.py -q`
       green with `test_audit.py` showing **zero diff** in `git diff --stat`.

### Phase C — RO-5: the `SecurityPosture` collector

10. [x] `varco_fastapi/tests/test_posture_redaction_collector.py` (**new**, red) — (a)
        `test_zero_arg_collector_is_clean_on_a_default_process` — `reset_redaction_state()`;
        `_collect_redaction()` → `[]` (post-flip a default app has **no** redaction finding —
        the "opted out, not not-opted-in" property RO-5 was parked on); (b)
        `test_opted_out_service_class_is_warn_with_remediation` — `class _OptedOut(AuditLogMixin):
        _audit_redactor = None`; `_collect_redaction(service_classes=(_OptedOut,))` → one
        finding, `check == "redaction.audit.disabled"`, `severity is PostureSeverity.WARN`,
        `remediation` mentions `_audit_redactor = None` and `VARCO_SECURITY_SUPPRESS`; (c)
        `test_redact_params_false_is_warn` via `envelope_settings=ErrorEnvelopeSettings(
        redact_params=False)`; (d) `test_substring_process_default_is_info` — `set_default_redactor(
        PolicyRedactor(RedactionPolicy(match_mode="substring")))`, assert `INFO` + id, `finally
        reset_redaction_state()`; (e) `test_absent_core_module_degrades_to_not_assessed` — reuse
        `_block_import` from `test_posture_collectors.py:24-35` on `"varco_core.redaction"`, run
        `SecurityPostureLifecycle(collectors=[_collect_redaction])`, assert exactly one
        `NOT_ASSESSED` whose `check == "posture.not_assessed._collect_redaction"`; (f)
        `test_refuse_in_production_refuses_an_opted_out_service` —
        `SecurityPostureSettings(enforce="refuse", environment="production")` (read the field
        names at `posture.py:154-197` first) + `partial(_collect_redaction, service_classes=
        (_OptedOut,))` → `await lifecycle.start()` raises `RuntimeError`; (g)
        `test_ids_are_the_four_stable_literals` — the collector's remediation table keys ==
        `{"redaction.audit.disabled", "redaction.error_params.disabled", "redaction.default.custom",
        "redaction.patterns.substring_mode"}` (import the table, or assert over findings from a
        worst-case configuration exercising all four). All `async def`.
        **Verify**: red with `ImportError` on `_collect_redaction`.

11. [x] `varco_fastapi/varco_fastapi/posture.py` — add `_collect_redaction(*, service_classes:
        tuple[type, ...] = (), envelope_settings: Any | None = None) -> list[PostureFinding]`
        after `_collect_data` (`:604-668`), §D-posture's shape: body-local `from
        varco_core.redaction.posture import inspect_redaction_posture`, a module-level
        `_REDACTION_REMEDIATION: dict[str, str]` with the four rows, `PostureFinding(check=f.check,
        severity=PostureSeverity(f.severity), detail=f.detail, remediation=_REDACTION_REMEDIATION
        .get(f.check, ""))`. Docstring: "§D-posture (Plan 057 / RO-5): 040's
        `inspect_redaction_posture()`; ids and severities re-emitted unchanged, remediation
        assigned here; `service_classes` is explicit caller input (036's rule) — Plan 051 replaces
        the `partial` with a multi-bound list." Module docstring `:4-9` → "five sibling
        introspection seams (…, 040's `inspect_redaction_posture()`) plus one local collector";
        the `:400-402` section comment "the five collectors" → "the six collectors". No `__all__`
        change (private).
        **Verify**: Step 10 green; `make test PKG=varco_fastapi`; `test_posture_collectors.py`
        and `test_posture_core.py` unmodified and green.

### Docs

12. [x] Feature docs, same commit as the code (feedback memory):
        - `technical_docs/features/redaction.md` — §"The substring matcher's real false
          positives" (`:111-136`) → "Word-boundary matching (the 4.0 default) and the substring
          escape hatch": the tokenizer table, the §D-word ✅/❌ contract table **verbatim** (it is
          the same table the test asserts), the plural rule, the "narrower" list, and
          `RedactionPolicy(match_mode="substring")` / `ParamCaptureConfig(redact_match_mode=
          "substring")` as the escape hatches; §"The audit hook — opt-in, and why" (`:153-181`)
          → "The audit hook — on by default (4.0), `AUDIT_REDACTION_POLICY`, and opting out":
          why `render_non_json=False`, the `dataclasses.replace(...)` extension idiom,
          `_audit_redactor = None`; §Posture (`:210-224`) → the four ids with their **new**
          meanings and "wired into `SecurityPosture` as `_collect_redaction` (Plan 057)";
          §"Adopting audit redaction" (`:226-237`) → "Upgrading to 4.0's audit default" (rows
          written after upgrade are redacted; rows before are not; opt out per service; the
          posture WARN); §Pitfalls (`:247-258`) — replace rows 1, 2, 5 with: "A custom pattern
          that relied on substring semantics (`"pwd"` for `dbpwd`) silently stops matching in
          4.0 → `match_mode="substring"` or list the exact token"; "`csrftoken`/`totp_code`/
          `oauth` are no longer matched (no separator) → add the literal as a pattern"; "Audit
          redaction is on by default — a service that must store secret-named fields verbatim
          sets `_audit_redactor = None` and will see `redaction.audit.disabled` at WARN";
          "A plain `PolicyRedactor()` as `_audit_redactor` turns `datetime`/`UUID` leaves into
          `"<TypeName>"` → use `PolicyRedactor(dataclasses.replace(AUDIT_REDACTION_POLICY,
          …))`"; §Parked (`:270-279`) — delete the "Wiring `inspect_redaction_posture()`" row;
          §See also — add `plans/057-redaction-flips.md`.
        - `technical_docs/features/security-posture.md` — collector table (`:56-62`) gains
          `| _collect_redaction | 040 inspect_redaction_posture() | redaction.audit.disabled
          (warn — opted out), redaction.error_params.disabled (warn), redaction.default.custom
          (info), redaction.patterns.substring_mode (info — opted out) |`; the wiring snippet
          (`:74-95`) gains `partial(_collect_redaction, service_classes=(OrderService,))`; `:51`
          "Four collectors are thin adapters" → "Five".
        - `technical_docs/features/database-auditing.md` — new subsection under "Step 1 —
          compose `AuditLogMixin`" (`:44`): "Redaction (on by default since 4.0)" — three
          sentences + a pointer to `redaction.md`; Pitfalls (`:431`) gains the opt-out row.
        - `README.md` §Redaction (`:3822-3851`): the snippet (`:3830-3838`) becomes the
          **opt-out** and **extend** forms (`_audit_redactor = None`; `PolicyRedactor(replace(
          AUDIT_REDACTION_POLICY, patterns=DEFAULT_REDACT_PATTERNS + ("csrftoken",)))`), one
          sentence "matching is word-boundary since 4.0; `match_mode="substring"` is the escape
          hatch", `:3850` "the substring matcher's documented false positives" → "the word-
          boundary contract table". §Auditing (`:3812-3818`) — one sentence: diffs are redacted
          by default.
        - `ARCHITECTURE.md` `:740-775` — `RedactionPolicy` line gains `match_mode default
          "word"`; add `AUDIT_REDACTION_POLICY`; `:767-769` inspector ids annotated "(opted out)";
          `:775` `_audit_redactor … = None (opt-in)` → `= PolicyRedactor(AUDIT_REDACTION_POLICY)
          (opt-out: None)`; `:1689` add `AUDIT_REDACTION_POLICY`; `:1690` "not wired into
          `SecurityPosture`" → "wired via `varco_fastapi.posture._collect_redaction` (Plan 057)".
        - `CLAUDE.md` §Redaction (`:732-755`) — keep it a rulebook: bullet 5 (`:753-755`) →
          "Audit redaction is **on by default** (4.0, Plan 057) through `AUDIT_REDACTION_POLICY`
          (`render_non_json=False` — never a plain `PolicyRedactor()` for a persisted surface);
          `_audit_redactor = None` is the opt-out and is what `redaction.audit.disabled` reports."
          Add one bullet: "Matching is **word-boundary** by default; `match_mode="substring"` is
          the explicit escape hatch — never 'fix' a missed key by loosening the tokenizer; add the
          exact token as a pattern." The line in §Error taxonomy mentioning "redaction in 3.2 is
          key-name-based only" stays true and stays.
        **Verify**: `rg -n 'opt-in' technical_docs/features/redaction.md README.md CLAUDE.md
        ARCHITECTURE.md` shows no remaining claim that audit redaction is opt-in;
        `rg -n 'substring' technical_docs/features/redaction.md` hits only the escape-hatch and
        pitfall lines.

13. [x] `CHANGELOG.md` — under `## [Unreleased]` (`:10`), **above** Plan 043's block, add
        `### BREAKING — word-boundary redaction, audit redaction on by default, redaction posture
        collector (Plan 057, FLIP-040-1 / FLIP-040-2 / RO-5)` with four bullets: the matcher
        change (same fifteen literals; `api_key`/`apiKey`/`x-api-key` now caught by DEFAULT;
        `shipping_address`/`author` no longer; `csrftoken`-class names no longer; escape hatches
        `RedactionPolicy(match_mode="substring")` / `ParamCaptureConfig(redact_match_mode=
        "substring")`); the audit default (`AUDIT_REDACTION_POLICY`, `render_non_json=False`,
        opt-out `_audit_redactor = None`, rows written after upgrade are redacted); the
        collector (`_collect_redaction`, `redaction.audit.disabled` at WARN means opted out —
        refuses startup under `enforce="refuse"` in production unless suppressed); the
        `ParamCaptureConfig.redact_match_mode` addition. End with "Migration: see this plan's
        §Migration (assembled into the 4.0 guide by Plan 058)."
        `BACKLOG.md:87, :88, :113` — **only if the launching thread has not already done it**:
        status cells → `done (Plan 057)`.
        **Verify**: `rg -n 'Plan 057' CHANGELOG.md BACKLOG.md` → ≥ 1 / ≥ 3 hits.

### Verification

14. [x] `make test PKG=varco_core`; `make test PKG=varco_fastapi`; `make lint` (no-`PKG` path:
        `api-check` **clean with the regenerated snapshot**, `asyncapi-check` unchanged,
        `import-budget --warn-only` no breach); `make type-check` clean (`Literal` defaults and
        the new field type-check under `strict`; no new `type: ignore`); `git diff --stat` shows
        **no change** under `varco_fastapi/varco_fastapi/app.py`,
        `varco_fastapi/varco_fastapi/middleware/`, `varco_core/tests/test_audit.py`,
        `varco_sa/`, `varco_beanie/`; the two Step 1 commands now print all-`True` and a dict
        whose `created_at` is a `datetime` (with `PolicyRedactor(AUDIT_REDACTION_POLICY)`).

## Edge cases

- **A key with no alphanumerics (`"---"`) or an empty key** → `_tokenize` returns `()`; word
  mode never matches it; substring mode behaves as 3.2. Never raises (`is_sensitive_key`'s
  contract, `policy.py:150-152`).
- **A pattern that tokenises to nothing in word mode** → `ValueError` at `RedactionPolicy`
  construction; in substring mode accepted (3.2 behaviour). `PolicyRedactor()` /
  `AUDIT_REDACTION_POLICY` never hit this (fifteen non-empty literals).
- **A pattern given in camelCase (`"apiKey"`)** → tokenised like a key → `("api","key")` — the
  same as `"api_key"`. Documented; not deduplicated.
- **Plural rule on a multi-token pattern** → last token only: `api_keys` ✅, `apis_key` ❌.
- **Non-`str` key (`1`)** → stringified before tokenising (`policy.py:166`), preserved as-is
  in `redact_mapping()` output — unchanged.
- **`match_mode="substring"` in `AUDIT_REDACTION_POLICY` via `replace(...)`** → allowed; the
  process-default posture finding does not fire (it inspects `default_redactor()` only) — an
  audited service on substring is the caller's explicit choice and is not reported. Documented.
- **`_audit_redactor` set on the instance in `__init__`** (as `test_audit_redaction.py:148-151`
  does) → instance attribute shadows the class default — still works; the posture inspector
  reads the *class* attribute (`redaction/posture.py:125`) and cannot see an instance-level
  opt-out. Pre-existing limitation, now documented in the inspector's docstring.
- **A subclass overriding `_audit_diff()`** → wins outright, default redactor ignored, no
  posture finding (`_has_own_audit_diff_override`, `redaction/posture.py:114-121`) — unchanged.
- **`redact_mapping()` raising anywhere under the default audit redactor** → the whole diff
  becomes `{k: "[REDACTED]"}` and an `ERROR` log line (`redactor.py:314-320`) — the fail-safe
  rule now applies to every audit row by default; named in §Risks.
- **An app that already set `_audit_redactor = PolicyRedactor()` in 3.2** → keeps working but
  now also gets word matching **and** still gets `json_safe()` on non-JSON leaves (it opted
  into `render_non_json=True` by construction). The README/Pitfalls row tells them to switch to
  `AUDIT_REDACTION_POLICY`.
- **`SecurityPostureLifecycle` without `_collect_redaction` in its list** → no redaction
  findings; nothing forces the collector in (051/049 own auto-registration).

## Migration (3.x → 4.0; Plan 058 assembles this section verbatim)

| Before (3.2) | After (4.0) | Action if you relied on the old behaviour |
|---|---|---|
| `RedactionPolicy().match_mode == "substring"`; `"pin"` redacted `shipping_address`, `"auth"` redacted `author` on spans, `error_params()`, logs, opt-in audit | `"word"`; those keys pass through; `api_key`/`apiKey`/`x-api-key`/`Authorization`/`credentials` still redacted | Nothing — this is the fix. A custom pattern that only worked as a substring (`"pwd"` → `dbpwd`, `"key"` → anything): `RedactionPolicy(patterns=…, match_mode="substring")`, or list the exact token (`"dbpwd"`) |
| `@span` capture with custom `ParamCaptureConfig(redact_patterns=…)` used substring | Word-boundary | `ParamCaptureConfig(redact_match_mode="substring")` |
| Suffix-glued names (`csrftoken`, `totp_code`, `secretkey`, `oauth`) were redacted by substring | Not matched (one token) | Add the literal: `patterns=DEFAULT_REDACT_PATTERNS + ("csrftoken",)`; or the substring escape hatch |
| `EXTENDED_REDACT_PATTERNS`' `"api-key"` was needed to catch `x-api-key` | DEFAULT catches it under word mode; the entry remains (harmless, still needed under substring) | Nothing |
| `AuditLogMixin._audit_redactor = None` — audit diffs stored verbatim unless you opted in | `PolicyRedactor(AUDIT_REDACTION_POLICY)` — secret-named keys `"[REDACTED]"` in every row written after upgrade; `datetime`/`UUID`/`Decimal` leaves untouched; rows written before upgrade untouched | To keep storing secret-named fields verbatim: `_audit_redactor = None` on that service (and expect `redaction.audit.disabled` at WARN). To extend: `PolicyRedactor(dataclasses.replace(AUDIT_REDACTION_POLICY, patterns=…))` |
| You set `_audit_redactor = PolicyRedactor()` yourself in 3.2 | Still works; you are on `render_non_json=True`, so non-JSON leaves render as `"<TypeName>"` (as in 3.2) | Switch to `PolicyRedactor(AUDIT_REDACTION_POLICY)` (or simply delete the line) to get byte-identical non-sensitive leaves |
| `inspect_redaction_posture()` was not in `SecurityPosture`; `redaction.audit.disabled` meant "not opted in" | `_collect_redaction` is a collector; `redaction.audit.disabled` (WARN) means "opted out"; under `VARCO_SECURITY_ENFORCE=refuse` + `production` it refuses startup | Add `partial(_collect_redaction, service_classes=(…,))` to your collector list (051 automates this); suppress a deliberate opt-out with `VARCO_SECURITY_SUPPRESS=redaction.audit.disabled` |
| `redaction.patterns.substring_mode` (INFO) fired on every default app | Fires only when the process default was explicitly set to substring | Nothing |

## Verification

```bash
# Preflight (Step 1) — premise 1 must print all False today, all True after Step 4
uv run python -c "from varco_core.redaction import RedactionPolicy, is_sensitive_key as k; p=RedactionPolicy(match_mode='word'); print([k(x,p) for x in ('api_key','apiKey','x-api-key','private_key','session_id','password2','credentials')])"
# premise 2 — '<datetime>' today with PolicyRedactor(); a real datetime after Step 8 with AUDIT_REDACTION_POLICY
uv run python -c "from datetime import datetime; from varco_core.redaction import redact_mapping, PolicyRedactor; print(redact_mapping({'created_at': datetime.now(), 'name': 'x'}, PolicyRedactor()))"

# Phase gates
uv run pytest varco_core/tests/test_redaction_word_mode.py varco_core/tests/test_redaction.py varco_core/tests/test_observability_params.py varco_core/tests/test_redaction_posture.py -q   # Phase A
uv run pytest varco_core/tests/test_audit.py varco_core/tests/test_audit_redaction.py varco_core/tests/test_redaction_posture.py -q                                                       # Phase B
uv run pytest varco_fastapi/tests/test_posture_redaction_collector.py varco_fastapi/tests/test_posture_collectors.py varco_fastapi/tests/test_posture_core.py -q                        # Phase C

# Package + gates
make test PKG=varco_core
make test PKG=varco_fastapi
uv run python scripts/api_surface.py            # regenerate (one addition: AUDIT_REDACTION_POLICY)
make lint                                        # api-check / asyncapi-check / import-budget
make type-check

# Boundary checks
git diff --stat -- varco_fastapi/varco_fastapi/app.py varco_fastapi/varco_fastapi/middleware/ varco_core/tests/test_audit.py varco_sa varco_beanie   # empty
rg -n 'opt-in' technical_docs/features/redaction.md README.md CLAUDE.md ARCHITECTURE.md | rg -i 'audit'   # empty
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| **Un-redaction in the unsafe direction** — a key redacted under substring stops being redacted under word (040 §D-S21-falsepos `:278-282`'s warning) | Every one of the fifteen defaults matches in every common spelling (§D-word ✅ rows); the "narrower" set is enumerated, not discovered later | Step 2's table is the test; §Migration lists every narrower case with its one-line fix; the substring escape hatch is byte-identical and tested |
| **Audit data corruption via `json_safe()`** (premise 2) | Under the default audit redactor, a non-sensitive leaf is the **same object** in and out | Step 7(e) asserts identity for `datetime`/`UUID`/`Decimal`/`date`; §D-audit-policy rejects plain `PolicyRedactor()`; CLAUDE.md rule "never a plain `PolicyRedactor()` for a persisted surface" |
| **Irreversible persisted change on upgrade** — rows written after 4.0 have `"[REDACTED]"` where 3.2 stored the value | Only secret-named keys change; the hash chain attests to what is stored (`audit.py:155-186`) | §Migration row; README opt-out snippet; `test_chain_over_default_redacted_rows_verifies_true` |
| **Fail-safe polarity now applies to every audit row** — a bug in `redact_mapping()` blanks whole diffs by default | `redact_mapping()` never raises and logs `ERROR` on degradation (`redactor.py:314-320`) | Unchanged mechanism; the `ERROR` line is the operator signal; Pitfalls row |
| **`enforce="refuse"` + `production` now refuses on an opted-out service** | A deliberate opt-out is one `VARCO_SECURITY_SUPPRESS` entry away | Step 10(f) asserts the refusal; remediation text names the suppression; CHANGELOG bullet |
| ⚠️ ASSUMPTION — `AuditEntry.from_event()` exists with a `(event, *, seq, prev_hash)`-shaped signature usable from a test | Step 7(h) can build a chain from produced events | Read `audit.py` around `AuditEntry` before writing 7(h); if the shape differs, build entries with the `_entry()` helper at `test_audit_redaction.py:252-262` from `event.diff` instead — the assertion (chain over redacted-at-write diffs verifies) is unchanged |
| ⚠️ ASSUMPTION — `SecurityPostureSettings` exposes `enforce` and `environment` as constructor kwargs (`posture.py:154-197`, `~:165` per the scout) | Step 10(f) can construct a refusing lifecycle without env vars | Read the class first; fall back to `monkeypatch.setenv("VARCO_SECURITY_ENFORCE", "refuse")` + `VARCO_SECURITY_ENV=production` |
| ⚠️ ASSUMPTION — no out-of-tree consumer constructs `RedactionPolicy(patterns=("-",))` or another no-token pattern | The new `ValueError` is a config-time break only for a pattern that could never have been intended under word mode | CHANGELOG names it; substring mode still accepts it |
| ⚠️ ASSUMPTION — `_tokenize` regex change (digits as own tokens) has no consumer other than `_is_sensitive_key_cached` | 3.2 word-mode behaviour only changes in the *safer* direction (`password2` now matched) | `rg -n '_tokenize\|_WORD_TOKEN_RE' varco_*/varco_*` at Step 4 → only `policy.py` |
| `lru_cache` key unchanged `(key, patterns, match_mode)`; a pattern-token memo is added | `reset_redaction_state()` clears both caches | Step 4 extends `reset_redaction_state()`; Step 2(f) covers the mode key |
| `api_surface.py --check` red after Step 8 | One `__all__` addition (a note, never a failure) but the committed snapshot must match the tree | Step 8 regenerates and commits the snapshot; Step 14 runs `make lint` |
| Docs drift — three docs, README, ARCHITECTURE, CLAUDE all say "opt-in" today | No surviving "opt-in" claim about audit redaction | Step 12's `rg` verification |
| Plan 051 rewrites the collector list while this lands | `_collect_redaction`'s body is list-agnostic; only the `partial` in the snippet moves | Stated in §D-posture and the function docstring |

## Out of scope / Parked

- **`RO-8`** — DLQ / `WebhookDelivery` payload redaction: stays live in BACKLOG, untouched.
- **`RO-4`** — `create_varco_app(log_redactor=)`: Plan 049.
- **Auto-registering `_collect_redaction` / discovering `service_classes`**: Plan 051 (multi-binding)
  and Plan 049 (`SecurityPostureLifecycle` as a default-on module).
- **Redacting inside `list`-valued leaves** (`_walk()` recurses into `Mapping` only): a real gap
  for `items: [{"password": …}]`-shaped DTO dumps; not opened here — it changes `redact_mapping()`'s
  documented output shape (040 §D-S21-nesting) and deserves its own row with a
  `max_items`-per-list argument. File as a BACKLOG question in Step 13 if the launching thread
  agrees; otherwise leave for 058's rollup.
- **A `VARCO_OTEL_CAPTURE_PARAMS_MATCH_MODE` env var**: Plan 048's settings recipe.
- **`match_mode="regex"`**: rejected in §D-word; the `Redactor` Protocol is the extension point.
- **`AUDIT_REDACTION_POLICY` as a providify binding (`bind_redactor`)**: 040 §Parked's trigger
  (a varco component that *injects* `Redactor`) has not fired — the mixin reads a class
  attribute.
