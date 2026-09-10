# Unified redaction seam — `varco_core.redaction`

Plan 040 (BACKLOG 3.2-extension row **S21**, 🟡 should, M). Closes: "one `Redactor` behind spans,
logs, audit payloads, and `error_params()`" — narrower than that sentence in practice (see
[§What already existed](#what-already-existed-before-this-plan) below), but with one genuine live
leak fixed and two prophylactics shipped.

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
patterns, case-insensitive substring, on the parameter *name* only. This plan does not change that
behaviour — it **extracts** the pattern tuple and the matching decision into
`varco_core.redaction`, byte-identical, and re-points three other surfaces at the same seam:

| Surface | Before this plan | After this plan |
|---|---|---|
| Span capture (`@span`, `create_span(params=...)`) | Redacted, on by default | **Unchanged** — same 15 patterns, same object identity |
| `error_params()` (error response body) | Advisory rule only (CLAUDE.md sentence, no mechanism) | Mechanical for two of three leak shapes, on by default |
| Audit trail (`AuditLogMixin`) | 🔴 **No redaction at all** — a docstring promised a hook (`_get_audit_diff_create()`) that existed nowhere in the repo | Real hook (`_audit_diff`/`_audit_redactor`), **opt-in** |
| Request logging (`RequestLoggingMiddleware`) | No user data logged at all (`request.url.path` excludes the query string, no headers, no body) | A `redactor=` keyword for a body/header/URL-logging subclass |

The audit trail was the one live leak: `AuditLogMixin._after_create`/`_after_update` wrote the
**entire** `read_dto.model_dump()` verbatim into the audit table. If any of your `ReadDTO`s carry
a secret-shaped field (`password_hash`, `api_key`, `totp_secret`), it has been stored unredacted
since you enabled auditing. This is not silently fixed by upgrading — see
[Adopting audit redaction](#adopting-audit-redaction) below.

## The seam's four names

```python
from varco_core.redaction import (
    Redactor,                  # runtime_checkable Protocol — ONE method: redact(key, value)
    PolicyRedactor,             # the default implementation, backed by a RedactionPolicy
    RedactionPolicy,            # frozen dataclass: patterns, match_mode, max_depth, max_items
    is_sensitive_key,           # the leaf predicate every Redactor answers
    redact_mapping,             # the nested-walk traversal (depth/cycle/item-safe)
    redact_query_string,        # a URL-query-string-shaped helper
    json_safe,                  # untruncated JSON-shape rendering
    default_redactor,           # process-wide default (getter)
    set_default_redactor,       # process-wide default (setter)
    reset_redaction_state,      # test helper
    DEFAULT_REDACT_PATTERNS,    # the incumbent 15 patterns, byte-identical
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

redactor = PolicyRedactor()                       # DEFAULT_REDACT_PATTERNS, match_mode="substring"
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
webhook verification: it substring-matches `webhook-signature`, `Stripe-Signature`,
`X-Hub-Signature-256` as a **key** name, opt-in only. It is not, and cannot be, an answer to
"scrub `whsec_...`/`sk_live_...` out of a value" — that is value scanning, which this seam
structurally does not do. Plan 038's own undertaking is to never log the secret/header in the
first place; this does not weaken that into "log it, we'll scrub it".

`EXTENDED_REDACT_PATTERNS`'s `"api-key"` (Open Question 3) exists because HTTP header names are
hyphenated (`X-Api-Key`) while `DEFAULT_REDACT_PATTERNS`'s `"api_key"` is underscore-shaped —
`"api_key" in "x-api-key"` is `False`, so a hyphenated header name is **not** caught by DEFAULT
alone. Opt-in only, for the same reason as every other entry: it changes matching behaviour and
must never be a silent default change.

`PII_REDACT_PATTERNS` is never a default because an audit trail exists to record PII — an audit
row that cannot show an email address changed is not a safer audit trail, it is a broken one
(GDPR Art. 30 processing records are one of the reasons `varco_core.service.audit` exists). If you
need PII to be unreadable at rest, use `varco_core.encryption` (crypto-shredding), not redaction.

## The substring matcher's real false positives

`match_mode="substring"` (the default, byte-identical to span capture's incumbent behaviour) is
fine applied to *developer-chosen* parameter names. Applied to *domain-chosen* payload keys — an
audit diff — it has real false positives:

| Pattern | Matches, wrongly |
|---|---|
| `"pin"` | `shipping_address` (`shi-**ppin**-g`), `mapping`, `typing`, `grouping`, `stripping` |
| `"auth"` | `author`, `authored_at`, `authority`, `authorship` |

`match_mode="word"` tokenises on `_`/`-`/camelCase boundaries instead, so `"pin"` matches
`pin`/`user_pin`/`userPin` but not `shipping`:

```python
from varco_core.redaction import RedactionPolicy, is_sensitive_key

is_sensitive_key("shipping_address", RedactionPolicy(match_mode="substring"))  # True  (false positive)
is_sensitive_key("shipping_address", RedactionPolicy(match_mode="word"))       # False
```

This is **documented and recommended for audit payloads**, but it is not any surface's default —
flipping the incumbent's default match mode is a security-relevant behaviour change with its own
blast radius, out of scope for this plan. It is *the* reason audit redaction stays opt-in rather
than on-by-default in 3.2: "audit redaction is on by default" and "`shipping_address` silently
becomes `[REDACTED]` in every audit row" cannot both be shipped honestly.

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

## The audit hook — opt-in, and why

```python
from varco_core.redaction import PolicyRedactor
from varco_core.service.audit import AuditLogMixin

class OrderService(AuditLogMixin, AsyncService[...]):
    _audit_redactor = PolicyRedactor()   # one line — every create/update diff is now redacted

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

**Why opt-in, not on by default**, three independent reasons:

1. The substring false-positive problem above — a default that mangles domain data
   (`shipping_address` → `[REDACTED]`) is not a security win.
2. It changes what is **persisted** — irreversible for rows already written, so it fails the "one
   env var reverts it" test a safe 3.2 default is held to.
3. It interacts with the hash chain (below) and deserves an explicit decision, not a silent flip.

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
is a pure, never-raising read with four stable `check` ids:

| `check` | Severity | Fires when |
|---|---|---|
| `redaction.audit.disabled` | `warn` | An inspected `AuditLogMixin` subclass has `_audit_redactor is None` and no `_audit_diff` override |
| `redaction.error_params.disabled` | `warn` | `ErrorEnvelopeSettings.redact_params is False` |
| `redaction.default.custom` | `info` | `default_redactor()` is not a `PolicyRedactor` |
| `redaction.patterns.substring_mode` | `info` | The effective policy uses `match_mode="substring"` |

⛔ **Not** wired into `varco_fastapi.posture.SecurityPosture` (Plan 036's harness) — `default`
audit-disabled would fire `warn` on every existing app on upgrade, and this is deliberately a
"defined and exported, not preflighted" deliverable for the 4.0 audit-default flip to build on.

## Adopting audit redaction

1. Set `_audit_redactor = PolicyRedactor()` on **one** service and inspect the resulting rows.
2. Check for false-positive casualties — a `shipping_address` field will be redacted under the
   default `match_mode="substring"`.
3. Switch to `RedactionPolicy(match_mode="word")` if any fire.
4. Roll out per service, never globally in one change — the transformation is irreversible for
   every row written after it.

Setting `_audit_redactor` only stops **new** rows from being unredacted. It does not clean rows
already written — that is `varco_core.retention`'s sweep (Plan 039) or crypto-shredding, not
redaction.

## Fail-safe behaviour

A redactor that raises is never trusted with a partial result: any failure during
`redact_mapping()`'s walk — including one leaf's `redact()` raising — degrades the **whole**
result to `{k: "[REDACTED]" for k in data}`, never the original input and never a
partially-redacted pass-through that could mask which leaves were actually inspected. Data can be
dropped safely; it cannot be un-emitted.

## Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| Substring `"pin"` redacts `shipping_address` | `"pin" in "shipping"` — false positive of `match_mode="substring"` (the default) | Use `RedactionPolicy(match_mode="word")` for audit payloads |
| `"auth"` redacts `author` | Same substring false positive | Same fix |
| Redaction is irreversible and the hash chain attests to the redacted value | Redaction destroys, and `entry_hash()` hashes exactly what is stored | Use crypto-shredding (`varco_core.encryption`) if you need the value back later |
| ⛔ Never redact on the audit read path | `verify_chain()` recomputes hashes from returned fields; mutating a returned entry's `diff` breaks every subsequent `prev_hash` check | Redact only in `_audit_diff`, before `_produce` — never in `list()`/an admin router |
| Audit redaction is opt-in — a `password_hash` on a read DTO is stored verbatim until you set `_audit_redactor` | Deliberate default (§D-S21-audit) — the substring matcher's false positives make an on-by-default flip irresponsible | Set `_audit_redactor = PolicyRedactor()` per service, after checking for false positives |
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

## Parked

| Item | Why | Un-park trigger |
|---|---|---|
| Value scanning / content classification (card/JWT/`whsec_` detection) | A denylist that is wrong once is a leak; false positives destroy audit data; per-string regex on the audit hot path is too slow | A consumer reports a real leak key-name matching structurally cannot catch, **and** brings a false-positive budget |
| `bind_redactor(container, redactor)` DI verb | No varco component currently injects `Redactor` | The first varco component that injects `Redactor` |
| Wiring `inspect_redaction_posture()` into `SecurityPosture` | `redaction.audit.disabled` would fire `warn` on every existing app today | The audit-redaction default flip (4.0) |
| DLQ payload redaction | Redacting `AbstractDeadLetterQueue`'s stored `Event` destroys the redrive path | A consumer needs DLQ retention beyond a secret's rotation window |
| `WebhookDelivery` request-body redaction | Delivery bodies are the replay evidence Plan 031 exists to preserve | Webhook delivery records outlive their payload's sensitivity in a real deployment |
| Retroactive redaction of already-stored audit rows | Rewriting a stored `diff` invalidates that row's `entry_hash()` and every subsequent `prev_hash` | Never — use 039's retention sweep or crypto-shredding instead |
