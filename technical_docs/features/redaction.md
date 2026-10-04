# Unified redaction seam — `varco_core.redaction`

Plan 040 (BACKLOG 3.2-extension row **S21**, 🟡 should, M). Closes: "one `Redactor` behind spans,
logs, audit payloads, and `error_params()`" — narrower than that sentence in practice (see
[§What already existed](#what-already-existed-before-this-plan) below), but with one genuine live
leak fixed and two prophylactics shipped.

**4.0 (Plan 057 — FLIP-040-1 / FLIP-040-2 / RO-5):** matching is **word-boundary** by default
(`match_mode="substring"` is the escape hatch), audit redaction is **on by default** through
`AUDIT_REDACTION_POLICY`, and `inspect_redaction_posture()` is a default `SecurityPosture`
collector (`RedactionPostureCollector`) whose findings now mean "opted out". The fifteen
`DEFAULT_REDACT_PATTERNS` literals did not change — the *matching* did.

## What redaction is, and is not

`varco_core.redaction` is a **key-name-based** redaction seam: it decides whether to replace a
*value* based on whether its *key* looks sensitive (`"password"`, `"api_key"`, `"secret"`, …).

⛔ **It is not value scanning.** There is no credit-card Luhn check, no JWT-shape detector, no
`whsec_`/`sk_live_` prefix match, and no plan to add one in 3.2 — a denylist over payload
*strings* is wrong-once-is-a-leak territory, and false positives in that shape destroy audit data
rather than merely mis-labelling a span attribute. See [Parked](#parked) below.

❌ **It is not reversible.** Redaction destroys — `"[REDACTED]"` cannot be un-redacted. If you need
to store sensitive data and get it back later (or crypto-shred it on demand), that is
`varco_core.encryption` (`FieldEncryptor` / `MultiKeyEncryptorRegistry` / `TenantAwareEncryptorRegistry`
/ crypto-shredding) — a completely different tool for a completely different problem.

## What already existed before this plan

Span parameter capture (`varco_core.observability.params`) already redacted 15 hard-coded
patterns, case-insensitive substring (word-boundary since 4.0), on the parameter *name* only. This plan does not change that
behaviour — it **extracts** the pattern tuple and the matching decision into
`varco_core.redaction`, byte-identical, and re-points three other surfaces at the same seam:

| Surface | Before this plan | After this plan |
|---|---|---|
| Span capture (`@span`, `create_span(params=...)`) | Redacted, on by default | **Unchanged** — same 15 patterns, same object identity |
| `error_params()` (error response body) | Advisory rule only (CLAUDE.md sentence, no mechanism) | Mechanical for two of three leak shapes, on by default |
| Audit trail (`AuditLogMixin`) | 🔴 **No redaction at all** — a docstring promised a hook (`_get_audit_diff_create()`) that existed nowhere in the repo | Real hook (`_audit_diff`/`_audit_redactor`) — **on by default since 4.0** (a per-service choice in 3.2) (Plan 057) |
| Request logging (`RequestLoggingMiddleware`) | No user data logged at all (`request.url.path` excludes the query string, no headers, no body) | A `redactor=` keyword for a body/header/URL-logging subclass |

The audit trail was the one live leak: `AuditLogMixin._after_create`/`_after_update` wrote the
**entire** `read_dto.model_dump()` verbatim into the audit table. If any of your `ReadDTO`s carry
a secret-shaped field (`password_hash`, `api_key`, `totp_secret`), it has been stored unredacted
since you enabled auditing. Since 4.0 every row written **after** the upgrade is redacted by
default; rows written before are not — see
[Upgrading to 4.0's audit default](#upgrading-to-40s-audit-default) below.

## The seam's four names

```python
from varco_core.redaction import (
    Redactor,                  # runtime_checkable Protocol — ONE method: redact(key, value)
    PolicyRedactor,             # the default implementation, backed by a RedactionPolicy
    RedactionPolicy,            # frozen dataclass: patterns, match_mode ("word" default), max_depth, max_items
    AUDIT_REDACTION_POLICY,     # the AuditLogMixin default — RedactionPolicy(render_non_json=False)
    is_sensitive_key,           # the leaf predicate every Redactor answers
    redact_mapping,             # the nested-walk traversal (depth/cycle/item-safe)
    redact_query_string,        # a URL-query-string-shaped helper
    json_safe,                  # untruncated JSON-shape rendering
    default_redactor,           # process-wide default (getter)
    set_default_redactor,       # process-wide default (setter)
    reset_redaction_state,      # test helper
    DEFAULT_REDACT_PATTERNS,    # the 15 patterns (unchanged; word-boundary matched since 4.0)
    EXTENDED_REDACT_PATTERNS,   # opt-in — "signature", "passphrase", "bearer", "jwt", "salt", "api-key"
    PII_REDACT_PATTERNS,        # opt-in, NEVER a default
)
```

`Redactor` is deliberately a **one-method** Protocol — `redact(key, value) -> value`. Traversal
(`redact_mapping`), the leaf predicate (`is_sensitive_key`), URL handling (`redact_query_string`)
and value rendering (`json_safe`) are free functions, not Protocol methods. Three of the four
consumers (span capture, `error_params()`, a log entry) only ever need the leaf decision on a flat
mapping; only the audit diff needs the nested walk. Putting traversal on the Protocol would force
every out-of-tree implementer to reimplement depth limiting and cycle detection just to answer one
predicate.

`PolicyRedactor` is the default:

```python
from varco_core.redaction import PolicyRedactor, RedactionPolicy

redactor = PolicyRedactor()                       # DEFAULT_REDACT_PATTERNS, match_mode="word"
redactor.redact("password", "hunter2")             # "[REDACTED]"
redactor.redact("page", 2)                         # 2 (unchanged)
```

## The pattern constants

| Constant | Contents | Default anywhere? |
|---|---|---|
| `DEFAULT_REDACT_PATTERNS` | `password`, `passwd`, `secret`, `token`, `authorization`, `auth`, `api_key`, `apikey`, `credential`, `private_key`, `cookie`, `session_id`, `otp`, `pin`, `ssn` | Yes — span capture, and `PolicyRedactor()`'s default |
| `EXTENDED_REDACT_PATTERNS` | `signature`, `passphrase`, `bearer`, `jwt`, `salt`, `api-key` | No — opt-in only |
| `PII_REDACT_PATTERNS` | `email`, `phone`, `address`, `iban`, `card_number`, `cvv`, `birth`, `national_id`, `tax_id` | No, and never will be by default |

**Rejected patterns**, recorded so a later "completeness" edit has to argue with this list:
`"key"` (matches `primary_key`/`cache_key`/`idempotency_key`/`partition_key` — it would redact
half of every audit diff), `"name"`, `"id"`, `"user"`, `"account"`.

`EXTENDED_REDACT_PATTERNS`'s `"signature"` is this plan's entire answer to Plan 038's inbound
webhook verification: it matches `webhook-signature`, `Stripe-Signature`,
`X-Hub-Signature-256` as a **key** name, opt-in only. It is not, and cannot be, an answer to
"scrub `whsec_...`/`sk_live_...` out of a value" — that is value scanning, which this seam
structurally does not do. Plan 038's own undertaking is to never log the secret/header in the
first place; this does not weaken that into "log it, we'll scrub it".

`EXTENDED_REDACT_PATTERNS`'s `"api-key"` (Open Question 3) exists because HTTP header names are
hyphenated (`X-Api-Key`) while `DEFAULT_REDACT_PATTERNS`'s `"api_key"` is underscore-shaped —
under `match_mode="substring"`, `"api_key" in "x-api-key"` is `False`, so a hyphenated header name
is **not** caught by DEFAULT alone. Under the 4.0 word default the entry is redundant (`api_key`
and `api-key` tokenise identically, so DEFAULT alone catches `x-api-key`); it is **kept** because
the tuple is pinned and a substring-mode caller still needs it.

`PII_REDACT_PATTERNS` is never a default because an audit trail exists to record PII — an audit
row that cannot show an email address changed is not a safer audit trail, it is a broken one
(GDPR Art. 30 processing records are one of the reasons `varco_core.service.audit` exists). If you
need PII to be unreadable at rest, use `varco_core.encryption` (crypto-shredding), not redaction.

## Word-boundary matching (the 4.0 default) and the substring escape hatch

3.2's default, `match_mode="substring"`, was fine on *developer-chosen* parameter names but had
real false positives on *domain-chosen* payload keys — `"pin"` matched `shipping_address`
(`shi-**ppin**-g`), `"auth"` matched `author` — which is why audit redaction could not be a 3.2
default. Since 4.0 (Plan 057 / FLIP-040-1) `RedactionPolicy().match_mode == "word"`.

**Tokenizer.** A key **and** every pattern are split by the same tokenizer, each token
lowercased. Boundaries:

| Boundary | Example → tokens |
|---|---|
| Any non-alphanumeric run (`_`, `-`, `.`, space, `/`, `:`) | `x-api-key` → `x api key`; `shipping_address` → `shipping address` |
| lower→Upper camelCase transition | `apiKey` → `api key`; `userPin` → `user pin` |
| Acronym followed by a capitalised word | `HTTPAuthorization` → `http authorization` |
| Digit runs are their own token | `password2` → `password 2`; `sha256` → `sha 256`; `X-Hub-Signature-256` → `x hub signature 256` |

**Predicate.** A pattern matches when its token sequence occurs as a **contiguous subsequence** of
the key's tokens; the pattern's **last** token also matches its simple plural (`token + "s"`):
`api_keys` matches `api_key`, `apis_key` does not. A pattern that tokenises to nothing (`"-"`) is
a `ValueError` at `RedactionPolicy` construction in word mode (it could never match); substring
mode still accepts it. A pattern is tokenised exactly like a key, from its original case —
`"apiKey"`, `"APIKey"` and `"api_key"` all mean `("api","key")`. In word mode a pattern with an
uppercase letter is stored in that canonical form, so `RedactionPolicy(patterns=("apiKey",)).patterns
== ("api_key",)`; an all-lowercase pattern is stored as given. Substring mode only lowercases
(`"apikey"`).

**The contract table** (against `RedactionPolicy()` — every row is a test in
`varco_core/tests/test_redaction_word_mode.py`):

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
| `page`, `harmless_field`, `primary_key`, `cache_key`, `idempotency_key`, `partition_key` | ❌ | `key` alone was never a pattern and `("api","key")` needs `api` |
| `oauth`, `oauth_client` | ❌ | token `oauth` ≠ `auth` (**narrower than substring**) |
| `csrftoken`, `csrfmiddlewaretoken`, `secretkey`, `totp_code`, `sessionid` | ❌ | no separator, so one token; substring caught the first four (**narrower**; `sessionid` was never caught by either mode) |
| `oauth_token`, `totp_secret` | ✅ | via `token` / `secret` |

**The "narrower" set** — names 3.2's substring matcher redacted that word mode does not:
`oauth`, `oauth_client`, `csrftoken`, `csrfmiddlewaretoken`, `secretkey`, `totp_code`. Each is
answered by one of two escape hatches, never by growing `DEFAULT_REDACT_PATTERNS`:

```python
from varco_core.observability.params import ParamCaptureConfig
from varco_core.redaction import DEFAULT_REDACT_PATTERNS, RedactionPolicy

RedactionPolicy(patterns=DEFAULT_REDACT_PATTERNS + ("csrftoken",))  # list the exact token
RedactionPolicy(match_mode="substring")                              # the 3.2 matcher, byte-identical
ParamCaptureConfig(redact_match_mode="substring")                    # the same, for @span capture
```

## `error_params()` — two thirds mechanical

`ErrorEnvelopeSettings.redact_params` (default `True`) routes `error_params()`'s return value
through `redact_mapping()` before it is emitted on the error envelope:

| Leak shape | Mechanised? | Behaviour |
|---|---|---|
| A secret-*named* key (`{"api_key": "sk_live_x"}`) | ✅ Yes | `"[REDACTED]"` |
| A non-JSON *value* (a live object from a `vars(exc)` dump) | ✅ Yes | `"<TypeName>"` |
| A secret *value* under a *benign* key (`{"internal_reason": "postgres://u:p@h/db"}`) | ❌ No | Emitted verbatim — key-name matching structurally cannot catch this |

Every in-tree `ServiceException` is byte-identical — none of their params match a redaction
pattern or carry a non-JSON value. `VARCO_ERROR_REDACT_PARAMS=false` restores the pre-3.2 body
byte-for-byte.

## The audit hook — on by default (4.0), `AUDIT_REDACTION_POLICY`, and opting out

```python
from dataclasses import replace

from varco_core.redaction import AUDIT_REDACTION_POLICY, DEFAULT_REDACT_PATTERNS, PolicyRedactor
from varco_core.service.audit import AuditLogMixin

class OrderService(AuditLogMixin, AsyncService[...]):
    pass   # 4.0 default: _audit_redactor = PolicyRedactor(AUDIT_REDACTION_POLICY)

class LegacyService(AuditLogMixin, AsyncService[...]):
    _audit_redactor = None   # opt out — diffs stored verbatim; posture: redaction.audit.disabled (WARN)

class ExtendedService(AuditLogMixin, AsyncService[...]):
    _audit_redactor = PolicyRedactor(
        replace(AUDIT_REDACTION_POLICY, patterns=DEFAULT_REDACT_PATTERNS + ("csrftoken",))
    )

    # OR, for full control:
    def _audit_diff(self, action: str, diff: dict[str, Any]) -> dict[str, Any]:
        ...  # called before the AuditEvent is produced; overriding wins outright
```

`_audit_redactor` is a **class attribute**, not a constructor parameter — `AuditLogMixin` has no
`__init__`, and adding one would break MRO composition with `ValidatorServiceMixin`/
`TenantAwareService`/`SoftDeleteService`. `_audit_diff(action, diff)` is the one hook, called from
all three of `_after_create`/`_after_update`/`_after_delete`, **before** the `AuditEvent` is
produced — the only point where the diff has not yet crossed the event bus and may not yet sit in
a broker/DLQ/outbox row.

**Why `AUDIT_REDACTION_POLICY` and not `PolicyRedactor()`.** The diff is `read_dto.model_dump()` —
pydantic's *python* mode, so `datetime`/`UUID`/`Decimal` objects. A plain `RedactionPolicy()` has
`render_non_json=True`, which renders every surviving non-JSON leaf through `json_safe()` — every
`created_at` would become the literal `"<datetime>"` in every audit row, hash-attested and
irreversible. `AUDIT_REDACTION_POLICY = RedactionPolicy(render_non_json=False)` leaves every
non-sensitive leaf as the **same object**; only secret-named keys change. It deliberately sets no
`match_mode`, so it always inherits the seam's (word) default. Extend it with
`dataclasses.replace(AUDIT_REDACTION_POLICY, patterns=…)` so `render_non_json=False` survives.

**Why on by default now.** 3.2 gave three reasons for opt-in; each is answered: (1) the substring
false positives are gone under word matching; (2) the persisted change is the point — it closes
the `password_hash`-on-a-read-DTO leak that was 3.2's only live leak; (3) redact-at-write is
hash-chain-safe (below).

### The hash chain rule: write-path only, never read-path

Redaction happens **before** `AuditEntry.from_event()`, therefore before `seq`/`prev_hash`/
`entry_hash()` exist. The chain hashes exactly what is stored — a chain spanning the redactor
being enabled, part unredacted and part redacted, still verifies as `True`, because each entry's
hash is a pure function of that entry's own stored fields.

⛔ **Never redact on the read path** — `list()`/`list_for_entity()`/an admin router. Mutating a
*returned* `AuditEntry.diff` before calling `verify_chain()` makes that entry's recomputed hash
disagree with the *next* entry's recorded `prev_hash`, reporting a `HashMismatch` that looks
exactly like tampering, on every row from that point forward.

## Request logging

```python
from varco_core.redaction import PolicyRedactor
from varco_fastapi.middleware.logging import RequestLoggingMiddleware

app.add_middleware(RequestLoggingMiddleware, redactor=PolicyRedactor())
```

`redactor=None` (the default) is byte-identical to pre-3.2 — and today's log entry
(`method`/`path`/`status`/`duration_ms`/`request_id`/`user_id`/`tenant_id`) carries no user data
at all: `request.url.path` excludes the query string, and there is no header/body logging. This
keyword exists for the subclass the middleware's own docstring already invites — one that adds
body/header/full-URL logging finally has something to call. `redact_query_string()` is the
companion helper for a subclass that logs `str(request.url)` rather than just the path.

## Posture

`varco_core.redaction.posture.inspect_redaction_posture(*, service_classes=(), envelope_settings=None)`
is a pure, never-raising read with four stable `check` ids. Since 4.0 a default app emits **none** —
every finding means "opted out" of a default:

| `check` | Severity | Fires when (4.0 meaning) |
|---|---|---|
| `redaction.audit.disabled` | `warn` | An inspected `AuditLogMixin` subclass **opted out** — `_audit_redactor = None` and no `_audit_diff` override |
| `redaction.error_params.disabled` | `warn` | `ErrorEnvelopeSettings.redact_params is False` (`VARCO_ERROR_REDACT_PARAMS=false`) |
| `redaction.default.custom` | `info` | `default_redactor()` is not a `PolicyRedactor` |
| `redaction.patterns.substring_mode` | `info` | The process default **opted out** of word matching (`set_default_redactor(PolicyRedactor(RedactionPolicy(match_mode="substring")))`) |

Wired into `SecurityPosture` as `varco_fastapi.posture.RedactionPostureCollector` (Plan 057 /
RO-5, made a class by Plan 051 §D-posture) — severities re-emitted unchanged, remediation
assigned there. It is one of `SecurityPostureModule`'s **default** collectors, so every app runs
it with no wiring; a stock app emits no finding (`redaction.error_params.disabled` fires only if
the app set `VARCO_ERROR_REDACT_PARAMS=false`). The audited services are explicit input (036's
rule — never discovered by walking `__subclasses__()`): list them once with
`SecurityPostureModule(audited_services=(OrderService,))` (see `security-posture.md`); an
unlisted service is not inspected. Under `enforce="refuse"` in `production`, `redaction.audit.disabled`
(WARN) refuses startup — suppress a deliberate opt-out with
`VARCO_SECURITY_SUPPRESS=redaction.audit.disabled`. The inspector reads the **class** attribute:
an opt-out made on the instance in `__init__` is invisible to it.

## Upgrading to 4.0's audit default

1. Every `AuditLogMixin` service redacts secret-named keys in rows written **after** the upgrade;
   rows written before are untouched (and must stay so — rewriting a stored `diff` breaks the
   hash chain).
2. A service that must keep storing a secret-named field verbatim sets `_audit_redactor = None`
   and will see `redaction.audit.disabled` at WARN if it is listed in
   `SecurityPostureModule(audited_services=...)` — suppress it by id if the opt-out is deliberate.
3. If you set `_audit_redactor = PolicyRedactor()` yourself in 3.2, you are on
   `render_non_json=True` (non-JSON leaves render as `"<TypeName>"`, as before) — switch to
   `PolicyRedactor(AUDIT_REDACTION_POLICY)` or simply delete the line.

Redaction only stops **new** rows from being unredacted. It does not clean rows already written —
that is `varco_core.retention`'s sweep (Plan 039) or crypto-shredding, not redaction.

## Fail-safe behaviour

A redactor that raises is never trusted with a partial result: any failure during
`redact_mapping()`'s walk — including one leaf's `redact()` raising — degrades the **whole**
result to `{k: "[REDACTED]" for k in data}`, never the original input and never a
partially-redacted pass-through that could mask which leaves were actually inspected. Data can be
dropped safely; it cannot be un-emitted.

## Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| A custom pattern that relied on substring semantics (`"pwd"` for `dbpwd`) silently stops matching in 4.0 | Word mode matches whole tokens; `dbpwd` is one token | `match_mode="substring"` (or `ParamCaptureConfig(redact_match_mode="substring")`), or list the exact token (`"dbpwd"`) |
| `csrftoken`/`totp_code`/`oauth` are no longer matched | No separator — one token, which is not a pattern | Add the literal as a pattern: `DEFAULT_REDACT_PATTERNS + ("csrftoken",)` |
| Redaction is irreversible and the hash chain attests to the redacted value | Redaction destroys, and `entry_hash()` hashes exactly what is stored | Use crypto-shredding (`varco_core.encryption`) if you need the value back later |
| ⛔ Never redact on the audit read path | `verify_chain()` recomputes hashes from returned fields; mutating a returned entry's `diff` breaks every subsequent `prev_hash` check | Redact only in `_audit_diff`, before `_produce` — never in `list()`/an admin router |
| Audit redaction is on by default — a service that must store secret-named fields verbatim loses them | 4.0 default (Plan 057 / FLIP-040-2) | Set `_audit_redactor = None` on that service; expect `redaction.audit.disabled` at WARN |
| A plain `PolicyRedactor()` as `_audit_redactor` turns `datetime`/`UUID` leaves into `"<TypeName>"` | `RedactionPolicy()` has `render_non_json=True` | Use `PolicyRedactor(dataclasses.replace(AUDIT_REDACTION_POLICY, …))` |
| `error_params()` still leaks a secret under a benign key | Redaction in 3.2 is key-name-based only — no value scanning | Rename the key to match a pattern, or scrub the value before returning it from `error_params()` |
| A broken custom redactor redacts everything, by design | Fail-safe polarity: a raising redactor cannot be trusted for a partial result either | Fix the redactor; check the `ERROR`-level log line `redact_mapping()` emits on failure |
| `PII_REDACT_PATTERNS` in an audit trail may destroy the record you are keeping | An audit trail's job is often to record that PII *changed* | Use crypto-shredding for PII-at-rest instead of redacting it out of the audit trail |

## See also

- README's "Redaction" section for a runnable usage snippet and the `VARCO_ERROR_REDACT_PARAMS`
  env-var row.
- ARCHITECTURE.md's "Redaction" type hierarchy.
- `plans/040-unified-redaction-seam.md` — the design plan (§D-S21-shape, §D-S21-patterns,
  §D-S21-falsepos, §D-S21-compat, §D-S21-errparams, §D-S21-audit, §D-S21-hashchain,
  §D-S21-failsafe, §D-S21-nesting, §D-S21-perf, §D-S21-posture, §D-S21-di,
  §D-S21-conformance, §D-S21-logging).
- `plans/057-redaction-flips.md` — the 4.0 flips (§D-word, §D-span, §D-audit-policy, §D-posture,
  §Migration).

## Parked

| Item | Why | Un-park trigger |
|---|---|---|
| Value scanning / content classification (card/JWT/`whsec_` detection) | A denylist that is wrong once is a leak; false positives destroy audit data; per-string regex on the audit hot path is too slow | A consumer reports a real leak key-name matching structurally cannot catch, **and** brings a false-positive budget |
| `bind_redactor(container, redactor)` DI verb | No varco component currently injects `Redactor` | The first varco component that injects `Redactor` |
| DLQ payload redaction | Redacting `AbstractDeadLetterQueue`'s stored `Event` destroys the redrive path | A consumer needs DLQ retention beyond a secret's rotation window |
| `WebhookDelivery` request-body redaction | Delivery bodies are the replay evidence Plan 031 exists to preserve | Webhook delivery records outlive their payload's sensitivity in a real deployment |
| Retroactive redaction of already-stored audit rows | Rewriting a stored `diff` invalidates that row's `entry_hash()` and every subsequent `prev_hash` | Never — use 039's retention sweep or crypto-shredding instead |
