# Plan 038 — Inbound webhook signature verification (S19)

Covers BACKLOG 3.2-extension row **S19** (🟡 should, S — *inbound webhook signature verification,
the missing half of the outbound signer*), `BACKLOG.md:78`.

**Research brief backing this plan:**
[`design/research/013-inbound-webhook-signature-verification.md`](../design/research/013-inbound-webhook-signature-verification.md),
written for this row. Every externally-grounded claim below cites it as `brief 013 §<section>`
(section = the finding heading it lives under). Where this plan **deviates** from the brief's
*Design Recommendation for varco* it says so explicitly and argues it — §D-S19-replay (no new
replay-cache ABC), §D-S19-seam (route dependency, not middleware), §D-S19-posture (no inspector),
§D-S19-gap (delegate to the shipped signer instead of a new `StandardWebhooksVerifier` HMAC path).
That deviation style follows `plans/035-http-edge-hardening.md` §D-S7-default.

## Scope and siblings

One of five plans covering the 3.2 extension rows `S17`, `S19`–`S23` (`BACKLOG.md:77-82`).

| Plan | Rows | Boundary with this plan |
|---|---|---|
| 039 | S20 — retention & purge automation | **Owns the `RetentionPolicy` registry.** ⚠️ Real adjacency: this plan's replay guard writes expiring entries into an `AbstractIdempotencyStore`, whose `delete_expired()` (`varco_core/varco_core/idempotency/base.py:201-220`) is a *candidate* retention target. This plan **must not build a registry, a schedule, or a sweeper** — one non-goal line naming the future adapter, and nothing more |
| 040 | S21 — unified redaction seam | **Owns `varco_core.redaction` and the redaction mechanism.** This plan designs **no** redaction. Its only obligation is to *not introduce a leak*: an inbound secret or a raw signature header must never reach a log line, an exception message, or `error_params()` (§D-S19-secret) |
| 041 | S17 + S22 — `MetricsMiddleware` ordering, JWKS background refresh | ⚠️ **Owns the normative middleware-ordering table** (`varco_fastapi/varco_fastapi/middleware/__init__.py:6-36`) and the ordering decision in `app.py`. This plan **adds no middleware and edits no ordering table** — that is the single strongest argument for §D-S19-seam's route dependency |
| 042 | S23 — conformance-guard recovery | Touches `testkit/` + CLAUDE.md only. This plan adds a new ABC (`WebhookVerifier`) and therefore owes `testkit/varco_conformance/COVERAGE.md` a **note row explaining why it gets no suite** (§D-S19-conformance) — 042 does **not** write it, this plan does. ⚠️ Both plans edit `COVERAGE.md`; see Risks |

**Position in the build order:** independent of all four siblings. It touches
`varco_core/webhook/`, `varco_core/exception/`, `varco_fastapi/webhook/` and no file any sibling
owns, except the one `COVERAGE.md` row.

## Goal

A varco app that *receives* a webhook from Stripe, GitHub, Slack, Svix, or any Standard
Webhooks-conformant sender verifies it with one route dependency — constant-time signature compare,
timestamp tolerance, multi-secret rotation, and optional message-id replay rejection — instead of
hand-rolling `hmac` and getting one of brief 013 §50's six named pitfalls wrong. Nothing is enabled
by default and no shipped behaviour changes.

## Non-goals

- **No new HMAC implementation for Standard Webhooks.** `StandardWebhooksSigner.verify()` already
  ships a correct one (`varco_core/varco_core/webhook/signing.py:145-178`) — constant-time compare
  at `:176`, multi-secret rotation loop at `:171-178`, timestamp checked *before* the signature is
  computed (`:163-168` precedes `:170`, which is brief 013 §50 pitfall 4 already closed). The
  Standard Webhooks/Svix verifier **delegates** to it (§D-S19-gap).
- **No new runtime dependency, anywhere.** Stdlib `hmac`/`hashlib`/`base64`/`time` only; brief 013
  §Dependencies confirms this is sufficient, and `hmac.compare_digest` is the constant-time
  primitive brief 013 §50.1 requires. The `standardwebhooks` PyPI package (brief 013 §11) is
  **not** adopted — see §Alternatives.
- **No new replay-cache ABC and no new backend.** ⛔ Deviation from brief 013's
  `AbstractWebhookReplayCache` sketch. `AbstractIdempotencyStore.reserve()`
  (`varco_core/varco_core/idempotency/base.py:92-133`) is a shipped, exactly-shaped atomic
  set-if-absent seam with four backends (§D-S19-replay).
- **No retention registry, no scheduled sweep.** Plan 039 owns it. The future adapter is one
  sentence in the feature doc: *the replay store's `delete_expired()` is a candidate `RetentionPolicy`
  target once 039 ships.* Nothing more.
- **No middleware, no ordering-table edit, no `create_varco_app` keyword.** Plan 041 owns the table
  (§D-S19-seam).
- **No `SecurityPosture` inspector.** Argued, not overlooked (§D-S19-posture).
- **No inbound subscription entity, repository, or admin surface.** Inbound secrets are deployment
  configuration, not tenant-owned rows (§D-S19-secret). No new table, no migration, no
  `FieldEncryptor` path.
- **No asymmetric verification.** Standard Webhooks' Ed25519 `whsk_`/`whpk_` variant (brief 013 §11)
  needs a crypto library varco_core does not depend on. Parked with a trigger.
- **No RFC 9421 inbound verifier.** `Rfc9421Signer.verify()` already exists
  (`signing.py:297-351`) and takes `payload: bytes` plus the request line — it is *already*
  inbound-shaped. Wiring it behind the new ABC is additive and parked (§Parked); no provider in
  brief 013 §2's table uses it.
- **No binary-mode/transport-header handling, no CloudEvents interaction.** Out of scope.

---

## Design

### What already exists — verified against source while writing this plan

Every anchor was opened and read.

| Fact | Location | Consequence |
|---|---|---|
| **`WebhookSigner` already declares an abstract `verify()`** | `varco_core/varco_core/webhook/signing.py:73-76` | Verification is already in the ABC's surface. This plan adds the *receiver-side* layer around it, not a second crypto path |
| `StandardWebhooksSigner.verify(payload, headers) -> bool` — full working verification | `signing.py:145-178` | Reused by delegation (§D-S19-gap) |
| It checks the timestamp (`:163-168`) **before** computing any signature (`:170`) | `signing.py:163-170` | brief 013 §50 pitfall 4 ("trusting a signature header before checking the timestamp") is already closed in the shipped code |
| It uses `hmac.compare_digest` (`:176`) and loops every provided × every active secret (`:171-178`) | `signing.py:171-178` | brief 013 §50.1 and §11's rotation requirement already satisfied |
| `_secret_bytes()` strips `whsec_` then base64-decodes, **falling back to raw UTF-8 when the decode raises** | `signing.py:109-118` | Matches brief 013 §11's secret encoding for Standard Webhooks/Svix. ⚠️ The fallback is a heuristic — see §D-S19-secretbytes and Risks |
| ⚠️ `verify()`'s `payload` parameter is typed **`str`**, and the signed content is built as `f"{msg_id}.{timestamp}.{payload}".encode()` | `signing.py:145`, `:170` | **Gap (c).** An inbound receiver holds raw `bytes`; a `bytes → str → bytes` round trip raises on a non-UTF-8 body. §D-S19-gap widens it to `str \| bytes` |
| Header lookup is exact-cased dict access on `webhook-id`/`webhook-timestamp`/`webhook-signature` | `signing.py:157-159` | **Gap (a).** HTTP headers are case-insensitive and Svix ships `svix-*` aliases (brief 013 §2) |
| `_DEFAULT_TOLERANCE_SECONDS = 300.0` | `signing.py:42` | Matches brief 013 §2's de-facto 5 minutes for Stripe/Svix/Slack |
| `get_signer(scheme_name, *, secrets, **kwargs)` — the shipped name→class dispatch | `signing.py:371-390` | The exact shape `get_verifier()` mirrors |
| `WebhookSettings` (`env_prefix="VARCO_WEBHOOK_"`), `signature_tolerance_seconds: float = 300.0` | `varco_core/varco_core/webhook/settings.py:76`, `:79` | The single configuration source; new inbound knobs go here (CLAUDE.md's standing rule) |
| `varco_core/webhook/__init__.py` has `__all__: list[str] = []`; `varco_core/__init__.py` exports **no** webhook name (grep: zero matches for `webhook`) | `varco_core/varco_core/webhook/__init__.py:17` | The webhook surface is imported from its submodules. Adding `varco_core.webhook.inbound` therefore changes **no** top-level `__all__` → `api_surface.py --check` stays clean (Step 24 proves it) |
| `AbstractIdempotencyStore.reserve(key, fingerprint, *, ttl) -> ReserveOutcome`, documented as the **one atomic primitive**, with `ACQUIRED`/`IN_FLIGHT`/`REPLAY` | `varco_core/varco_core/idempotency/base.py:92-133`, `:34-64` | The replay guard (§D-S19-replay) |
| `complete(key, record)` / `release(key)` / `delete_expired()` | `idempotency/base.py:135-160`, `:179-199`, `:201-220` | Success/failure/retention halves of the same guard |
| `IdempotencyRecord(status, body, headers, fingerprint, created_at)` — frozen | `varco_core/varco_core/idempotency/record.py:19-56` | The minimal record `complete()` needs |
| Starlette caches the body: `body()` sets `self._body` (`:254-260`) and `stream()` re-yields it (`:234-236`) | `.venv/lib/python3.12/site-packages/starlette/requests.py:234-236`, `:254-260` | **A route dependency may `await request.body()` and downstream pydantic body parsing still works.** This is what makes §D-S19-seam viable without any middleware |
| `IdempotencyMiddleware` already does `body = await request.body()` in production | `varco_fastapi/varco_fastapi/middleware/idempotency.py:342` | In-repo precedent for buffering the body in the HTTP layer |
| `BodyLimitMiddleware` sits **outside** every inner layer and rejects an over-limit body before it is buffered | `varco_fastapi/varco_fastapi/middleware/__init__.py:25` (Plan 035 §D-S8-default) | brief 013 §32's body-limit interaction is already resolved *in our favour*: a route dependency never sees an over-limit body |
| `IdempotencyKeyInvalidError` registers its own 400 `ErrorCode` at import time because no built-in maps to 400 | `varco_core/varco_core/exception/idempotency.py:125-164` | The precedent for a new 401-mapped exception. 403/409/422 are built in (`exception/codes.py:163-184`); **401 is not** |
| `mount_webhook_admin(app, *, repository, …)` is the whole of `varco_fastapi.webhook` today | `varco_fastapi/varco_fastapi/webhook/mount.py:37-47`, `webhook/__init__.py:14` | The package the new dependency joins |
| `WebhookSubscription.active_secrets: list[str]`, `.tenant_id: str`, `.signer: str` | `varco_core/varco_core/webhook/models.py:88-94` | These are **our** secrets for **our** outbound deliveries — the wrong direction and wrong cardinality for inbound (§D-S19-secret) |
| **Nothing inbound exists.** `rg 'X-Hub-Signature\|Stripe-Signature\|X-Slack-Signature\|svix-id'` over all `*.py`: **zero hits** | verified absence, all ten packages | The BACKLOG row's claim is confirmed |

### §D-S19-gap — what is actually missing (the framing decision)

The row says *"the missing half of the outbound signer"*. Read literally that suggests writing a
`verify()`. **That would be wrong** — one already exists. Precisely enumerated:

| # | Candidate gap | Verdict | Evidence |
|---|---|---|---|
| (a) | **Provider header parsing / normalisation** | ⛔ **Missing** | `signing.py:157-159` does exact-cased `dict` lookups on three fixed names; HTTP headers are case-insensitive, Svix uses `svix-id`/`svix-timestamp`/`svix-signature` aliases, Stripe packs `t=`/`v1=` into one comma-delimited `Stripe-Signature`, GitHub uses `sha256=`-prefixed `X-Hub-Signature-256`, Slack splits across two headers (brief 013 §2) |
| (b) | **Replay window (message-id dedup)** | ⛔ **Missing** | The shipped signer bounds only the *timestamp* (`signing.py:167`). brief 013 §42: *"Timestamp window alone is insufficient — a webhook delivered within the tolerance window can be replayed indefinitely within that window"* |
| (c) | **Raw-body seam** | ⛔ **Missing** | `verify()` takes `payload: str` (`signing.py:145`) and re-encodes at `:170`. brief 013 §50.2 requires verifying the exact raw bytes |
| (d) | **FastAPI wiring** | ⛔ **Missing** | Verified absence — zero hits for any provider header name in any `*.py` |
| (e) | **Non-Standard-Webhooks schemes** (Stripe `t=,v1=`; GitHub raw-body-only; Slack `v0:ts:body`) | ⛔ **Missing** | brief 013 §2's divergence table — three genuinely different signed-basestring constructions |
| — | HMAC-SHA256 computation, constant-time compare, multi-secret rotation, tolerance check, timestamp-before-signature ordering | ✅ **Shipped** | `signing.py:120-122`, `:163-178` |

**DESIGN: the Standard Webhooks / Svix verifier delegates its signature check to
`StandardWebhooksSigner`; only (a)–(e) are new code**

✅ Satisfies CLAUDE.md's reuse rule literally: no second HMAC path for the scheme varco already
   implements, so inbound and outbound cannot drift (a varco app can verify its own outbound
   delivery — asserted by a round-trip test, Step 4).
✅ Composition, not inheritance: `StandardWebhooksVerifier` *holds* a `StandardWebhooksSigner`
   built from the same `secrets` + `tolerance_seconds`. No private attribute is touched
   (`_secret_bytes` is used only *through* `verify()`).
✅ The one shipped-code change is **additive and non-breaking**: widen
   `StandardWebhooksSigner.verify(payload: str)` → `payload: str | bytes`, decoding is skipped for
   `bytes` and `f"{id}.{ts}.".encode() + payload` is concatenated instead (byte-identical for valid
   UTF-8 — proved by Step 3's differential test). `api_surface.py` records **function** signatures
   only, not methods (CLAUDE.md's known limitation), and widening a parameter type is not a break
   in any case.
❌ Standard Webhooks verification then reports only `bool`, losing the "why". Mitigated: the
   verifier performs its own timestamp check **first** (cheap: one `float()` + one `abs()`), so it
   can distinguish `TIMESTAMP_OUT_OF_TOLERANCE` from `SIGNATURE_MISMATCH` in the *server log*, then
   delegates the signature decision. The double timestamp check is deliberate and costs nothing.
❌ Three provider adapters (Stripe/GitHub/Slack) are genuinely new HMAC code. Unavoidable — brief
   013 §2 shows three different basestrings; there is nothing in-tree to reuse for them.
  Rejected — **write a fresh `StandardWebhooksVerifier` with its own `hmac.new`**: ❌ two
  implementations of one spec in one package, guaranteed to drift, and the explicit thing
  CLAUDE.md's reuse rule forbids.
  Rejected — **make `WebhookSigner` itself the inbound interface** (add `verify_request()` to the
  ABC): ❌ adding an abstract method to a shipped ABC breaks every out-of-tree implementation — the
  same rule that keeps `BulkCache` off `AsyncCache` (Plan 011 / D-11) and `remaining()` off
  `RateLimiter` (Plan 035 §D-S10-headers). A signer signs; a verifier is a separate role with a
  different lifecycle (per-provider secrets, replay state).

### §D-S19-shape — one ABC, a shared HMAC template, four adapters

⛔ **Not** one configurable verifier. Adopted from brief 013's *Librarian's note*, which is explicit:
*"a generic `WebhookVerifier` ABC with provider-specific adapters, not a monolithic multi-provider
verifier"* — because the four providers differ in header names, signed-payload construction, and
whether a timestamp exists at all (brief 013 §2).

```
varco_core/webhook/inbound/
├── __init__.py     # re-exports; no @Singleton/@Provider/@Configuration (see below)
├── base.py         # WebhookVerifier (ABC), VerificationResult, VerificationFailure, SecretEncoding
├── verifiers.py    # HmacWebhookVerifier + StandardWebhooks/Svix, Stripe, GitHub, Slack
├── replay.py       # WebhookReplayGuard — a thin adapter over AbstractIdempotencyStore
└── (get_verifier lives in verifiers.py, mirroring get_signer at signing.py:371)
```

```python
class VerificationFailure(StrEnum):
    MISSING_HEADER = "missing_header"
    MALFORMED_HEADER = "malformed_header"
    TIMESTAMP_OUT_OF_TOLERANCE = "timestamp_out_of_tolerance"
    SIGNATURE_MISMATCH = "signature_mismatch"

@dataclass(frozen=True)
class VerificationResult:
    verified: bool
    provider: str
    message_id: str | None          # the dedup key; None when the provider ships none
    timestamp_checked: bool         # False for GitHub — structurally, not by configuration
    failure: VerificationFailure | None = None
    # ⛔ deliberately absent: which secret matched, the signature, any secret material (§D-S19-secret)

class WebhookVerifier(abc.ABC):
    def __init__(self, secrets: list[str], *, tolerance_seconds: float = 300.0) -> None: ...
    @property
    @abc.abstractmethod
    def provider(self) -> str: ...
    @abc.abstractmethod
    def verify(self, *, body: bytes, headers: Mapping[str, str]) -> VerificationResult: ...
```

Deviations from the brief's sketch, each argued:

| Brief 013 sketch | This plan | Why |
|---|---|---|
| `Protocol`, `runtime_checkable`, `async def verify` | **ABC**, `def verify` (sync) | Verification is pure CPU — `signing.py`'s own module docstring says so at `:22-23`. An `async def` would be a lie about the cost and force `await` on a hot path. The ABC matches `WebhookSigner` (`signing.py:45`) and gives the shared `__init__` secret validation for free |
| `verify(..., timestamp_tolerance_seconds: int = 300)` per call | tolerance on the **constructor**, from `WebhookSettings` | CLAUDE.md's standing webhook rule: a knob lives on `WebhookSettings` and is threaded through; a per-call argument makes the env var a lie |
| `raises WebhookVerificationError` **and** returns a result | **returns** a `VerificationResult`; only the HTTP adapter raises | A pure core seam that never raises composes; the FastAPI dependency maps `verified=False` → `WebhookSignatureError` (401). Same split as `RateLimiter.acquire() -> bool` vs. the middleware's 429 |
| `error_code: str` free-form | `VerificationFailure` StrEnum | A typed, closed set is greppable and testable; free-form strings drift |

**Rule (enforced by a test, Step 5): no module-level `@Singleton`/`@Provider`/`@Configuration` in
`varco_core.webhook.inbound`.** `container.scan("varco_core", recursive=True)` is a documented,
in-use pattern that auto-activates both shapes — the same rule CLAUDE.md states for
`varco_core.event.cloudevents` and `varco_core.tls`. Wiring is the app's, at route level.

**GitHub has no timestamp — decided, not papered over.** brief 013 §2: GitHub ships
`X-Hub-Signature-256` over the raw body only, with *"no built-in protection"* and
`X-GitHub-Delivery` as the only dedup handle.

| ID | Choice | Consequence |
|---|---|---|
| D-S19-github | `GitHubWebhookVerifier` sets `timestamp_checked=False` and **refuses to construct** without either a `replay_guard=` or an explicit `acknowledge_no_replay_protection=True` (`ValueError` naming both) | The one provider where the tolerance window is structurally unavailable cannot be deployed replay-unprotected by accident |

✅ Same "explicit acknowledgement kwarg for a footgun we will not remove" shape varco already uses
   (`mount_*(acknowledge_bundled_admin=True)`; `RateLimitMiddleware(acknowledge_unbounded_keyspace=True)`,
   Plan 035 §D-S10-keyspace).
✅ It fails at **construction**, i.e. at startup in a test, not under a replay in production.
✅ It is honest: `timestamp_checked=False` is reported in the result rather than a fake `True`.
❌ A GitHub receiver cannot be wired in three lines without a store or a keyword. That is the point;
   the error message names both escapes.
  Rejected — **synthesise a timestamp from `Date`/arrival time**: ❌ unsigned, therefore
  attacker-controllable; a tolerance check over an unsigned value is security theatre.
  Rejected — **silently skip the timestamp check for GitHub**: ❌ the caller believes they have
  replay protection they do not have — precisely the class of hole this whole cycle exists to close
  (`BACKLOG.md:53-55`).

### §D-S19-secretbytes — per-provider secret encoding, because the providers disagree

`StandardWebhooksSigner._secret_bytes` (`signing.py:109-118`) strips `whsec_` and base64-decodes,
falling back to UTF-8. That is correct for Standard Webhooks/Svix (brief 013 §11) and **wrong for
Stripe, GitHub and Slack**, whose libraries key HMAC on the secret string's raw bytes.

| ID | Choice | Consequence |
|---|---|---|
| D-S19-secretbytes | A `SecretEncoding` StrEnum (`STANDARD_WEBHOOKS_B64` / `RAW_UTF8`) on the base verifier; SW/Svix delegate to the shipped function, Stripe/GitHub/Slack use `secret.encode("utf-8")` verbatim | One documented policy per provider instead of one heuristic applied to all four |

⚠️ **`⚠️ ASSUMPTION` (carried to Risks):** brief 013 §2 describes Stripe as *"keyed on `whsec_`
base64 secret"*, which conflicts with stripe-python's use of the whole secret string as the key.
The brief does not settle it and no known-answer vector is supplied. Step 8 pins `RAW_UTF8` and
records the assumption in the test docstring; if a real Stripe delivery disproves it, the fix is one
enum member on one class.

⚠️ **Inherited footgun, documented not fixed:** `_secret_bytes`' base64 fallback silently keys on
*decoded garbage* for a non-`whsec_` secret that happens to be base64-decodable (e.g. an 8-character
ASCII secret). It is shipped behaviour on the outbound path; changing it would change the wire bytes
of every existing deployment. → Pitfalls row: *use the provider's real `whsec_`-prefixed secret
verbatim; never a hand-typed short string.*

### §D-S19-seam — a route dependency, not a middleware

| ID | Choice | Consequence |
|---|---|---|
| D-S19-seam | `varco_fastapi.webhook.inbound.verify_webhook(verifier, *, replay_guard=None)` — a **dependency factory** returning a FastAPI dependency **with `yield`**, injected per route. No middleware, no `create_varco_app` keyword, no ordering-table edit | Plan 041's ordering table is untouched; each route names its own provider and secret |

✅ **A middleware structurally cannot do this.** The secret and the provider are per-route facts
   (`/hooks/stripe` and `/hooks/github` need different secrets *and* different algorithms); a
   middleware would need a path→verifier map, which is the routing table re-implemented one layer
   too early.
✅ Plan 041 owns `varco_fastapi/varco_fastapi/middleware/__init__.py:6-36` and `app.py`'s ordering.
   Adding nothing there removes the entire collision.
✅ **The raw-body pitfall (brief 013 §32, §50.2) is closed by the dependency, not worked around.**
   `await request.body()` caches into `Request._body`
   (`.venv/.../starlette/requests.py:254-260`) and `stream()` re-yields the cached value
   (`:234-236`), so a downstream pydantic body model still parses the same request. The verified
   bytes are handed to the route as `VerifiedWebhook.body`, so a handler that wants byte fidelity
   never re-serialises (brief 013 §50.2's exact prohibition).
✅ **The `BodyLimitMiddleware` interaction resolves in our favour and needs no coordination.** It is
   on by default at 10 MiB and sits outside every inner layer
   (`varco_fastapi/varco_fastapi/middleware/__init__.py:25`, Plan 035 §D-S8-default), so an
   over-limit body is a 413 *before* the dependency ever buffers — exactly brief 013 §39's stated
   order, and it also answers brief 013's Evidence gap 4 (unbounded buffering) for a varco app.
   ⚠️ Documented consequence: a provider sending >10 MiB is rejected before verification;
   `VARCO_BODY_LIMIT_EXEMPT_PATHS` is the fix, and exempting the path means accepting an unbounded
   buffer on it — a Pitfalls row.
✅ A dependency **with `yield`** is what makes the replay guard correct (§D-S19-replay): reserve
   before `yield`, `complete()` after a clean return, `release()` in `except`.
❌ Only routes that declare it are protected — no blanket coverage. Accepted: a blanket inbound
   webhook verifier is meaningless anyway (every other route has no signature to check).
❌ FastAPI-specific. Accepted and correct: portable logic stays in `varco_core.webhook.inbound`,
   the HTTP adapter lives in `varco_fastapi` — the same seam rule as `AbstractEventBus` /
   `varco_core.tls`.
  Rejected — **`WebhookSignatureMiddleware` buffering into `scope["webhook.raw_body"]`** (brief
  013's §Seam sketch): ❌ cannot select a secret per route; ❌ needs a position in a table Plan 041
  owns; ❌ Starlette already caches the body, so the buffering half of it is redundant for a
  dependency-based reader.
  Rejected — **extend `IdempotencyMiddleware` to accept `webhook-id` as its key header**: ❌ it is
  keyed on `Idempotency-Key` (`idempotency.py:84`) and is a *response-replay* cache with a
  fingerprint-mismatch 422 contract, which is a different feature; ❌ it does no signature checking,
  which is the actual row.

### §D-S19-replay — reuse `AbstractIdempotencyStore`; no new store, no new backend

⛔ **Deviation from brief 013's `AbstractWebhookReplayCache` + three backends sketch.**

| ID | Choice | Consequence |
|---|---|---|
| D-S19-replay | `WebhookReplayGuard(store: AbstractIdempotencyStore, *, ttl_seconds)` — a ~40-line adapter. `reserve(key=f"webhook:{provider}:{message_id}", fingerprint=sha256(body), ttl=…)`: `ACQUIRED` → proceed; `IN_FLIGHT` → a concurrent duplicate; `REPLAY` → an already-handled duplicate. Both non-`ACQUIRED` outcomes → `WebhookReplayError` (409) | Four production backends (in-memory, Redis, SQLAlchemy, Beanie) on day one, zero new ABC |

✅ The seam is exactly shaped for this: `reserve()` is documented as *"the **single atomic
   primitive**… concurrent callers racing on the same `key` receive exactly one `ACQUIRED`"*
   (`idempotency/base.py:96-101`), which is verbatim what brief 013 §48 asks for
   (*"async cache with `reserve(key, ttl)` atomic primitive — similar to varco's `IdempotencyStore`
   pattern"* — the brief itself points here).
✅ A new ABC would owe four new backends, four conformance subclasses, four sets of migrations, and
   a `COVERAGE.md` row per implementation — for a semantics identical to one that ships.
✅ **`complete()`/`release()` make the retry story correct**, which a naive `has_seen`/`mark_seen`
   cache (the brief's sketch) gets wrong: a provider legitimately retries a delivery our handler
   *failed*. `mark_seen`-on-arrival would swallow that retry and lose the event permanently. The
   `yield`-dependency reserves, `complete()`s on a clean return, and `release()`s on an exception —
   so a failed delivery is retryable and a successful one is not.
✅ TTL: `max(2 × tolerance, 600)` seconds by default (`inbound_replay_ttl_seconds`), i.e. 600 s at
   the 300 s default — brief 013 §46: *"with a 5-minute tolerance window, a cache with 5–10 minute
   TTL covers it"*. For GitHub (no timestamp) the guard is **required** (§D-S19-github) and the
   documented TTL guidance is 24–48 h (brief 013 §47) — surfaced as a doc/Pitfalls row and a
   settings default of `None` meaning "use `inbound_replay_ttl_seconds`", never a silent 600 s.
✅ Off unless a store is supplied — except GitHub. Adopts brief 013 §146's *"enabled by default if a
   cache is registered; skipped if none exists"*.
❌ Using `IN_FLIGHT` to mean "a concurrent duplicate" is a mild semantic stretch of a store designed
   for HTTP idempotency. Accepted, and it is the *useful* stretch: it lets the 409 distinguish
   "still processing" from "already processed" at no cost.
❌ A store shared with `IdempotencyMiddleware` shares a key space. Mitigated by the mandatory
   `webhook:{provider}:` key prefix, asserted by a test (Step 12).
  Rejected — **a new `AbstractWebhookReplayCache` ABC + 3 backends** (the brief's sketch): ❌ a
  duplicate of a shipped seam, four backends of new surface for a 🟡/S row, and CLAUDE.md's decision
  tree explicitly routes "atomic set-if-absent" to this ABC.
  Rejected — **an in-process `set()` with a background cleanup task**: ❌ wrong across replicas
  (every pod would accept the same replay), and a background task in `varco_core` violates the
  no-side-effect-on-scan rule.
  Rejected — **`AsyncCache` with `exists()` + `set()`**: ⛔ non-atomic; Plan 011 / D-11 forbids
  adding a set-if-absent to `AsyncCache`, which is the exact reason `AbstractIdempotencyStore`
  exists (`idempotency/base.py:7-15`).

### §D-S19-secret — inbound secrets are deployment configuration, never subscription rows

| ID | Choice | Consequence |
|---|---|---|
| D-S19-secret | Secrets are passed to the verifier's constructor as `list[str]`, sourced by the app from env / a secret manager. **No** repository, **no** new entity, **no** `FieldEncryptor` path, **no** migration | Zero new persistence surface, and no accidental coupling between "who we send to" and "who sends to us" |

✅ `WebhookSubscription.active_secrets` (`models.py:91`) is scoped by `tenant_id` (`:88`) and paired
   with a `target_url` (`:89`) — it models *a subscriber we deliver to*. An inbound secret is issued
   **by Stripe/GitHub to us**: no target URL, no tenant ownership, cardinality one-per-upstream, and
   a rotation lifecycle the upstream controls. Storing it in that table would be a category error.
✅ The `FieldEncryptor` path exists precisely because outbound secrets are *rows*. Deployment
   configuration is protected by the deployment's secret manager, which is where a receiver's shared
   secret already lives.
✅ **Non-leak obligations (Plan 040 owns the mechanism; this plan owns not creating the leak):**
   (1) `VerificationResult` carries no secret, no signature, and no matching-secret index;
   (2) `WebhookSignatureError.error_params()` returns `{}` — the same discipline CLAUDE.md records
   for `ServiceAuthorizationError` deliberately excluding `reason`; (3) log lines carry
   `provider`, `message_id`, `failure` and nothing else (brief 013 §153); (4) a test greps the
   rendered error body and the captured log for the secret and the signature (Step 19).
❌ An app receiving webhooks from N tenants' *own* Stripe accounts must map tenant → secret itself.
   Accepted — that mapping is application domain, and the verifier is cheap to construct per
   request from a resolved secret list.
  Rejected — **reuse `WebhookSubscriptionRepository` with a `direction` field**: ❌ overloads a
  shipped entity, forces a migration on every existing deployment, and puts an untenanted global
  secret in a tenant-scoped table.

### §D-S19-rotation — accept any active secret; never report which one matched

brief 013 §11/§18: Standard Webhooks ships a **space-delimited list** of signatures for rotation and
receivers *"try to verify each signature until one matches"*.

| ID | Choice | Consequence |
|---|---|---|
| D-S19-rotation | Every provided signature is tried against every active secret with `hmac.compare_digest`, returning `True` on the first match — identical to the shipped loop (`signing.py:171-178`). **`VerificationResult` exposes no index, no keyid, and no count**, and neither does any log line or response body | Rotation works; nothing observable says which secret matched |

✅ `hmac.compare_digest` makes each individual comparison constant-time (brief 013 §50.1) — the
   protection that matters, because it is the one that leaks *secret content*.
✅ Keeping the early return means the SW path stays byte-identical to the shipped signer, so the two
   cannot diverge (§D-S19-gap).
✅ The requirement "must not leak which matched" is met where it is actually observable — in the
   **result, the response, and the log** — not by micro-managing loop timing.
❌ The number of `compare_digest` calls varies with the match index, a coarse timing signal.
   Accepted and argued: it reveals at most *which of N secrets an attacker's already-valid signature
   matched*, which requires possessing a valid signature first, and brief 013's Evidence gap 5
   records that network-level timing exploitation is unquantified even for the content case.
  Rejected — **non-short-circuiting `matched |= compare_digest(...)`**: ❌ desynchronises from the
  shipped signer for no gain against any threat model in brief 013 §50.

### §D-S19-config — new knobs on `WebhookSettings`, prefixed `inbound_`

CLAUDE.md's standing webhook rule: *a new knob goes on `WebhookSettings` and is threaded through;
never a constructor-keyword-only option, or the env var becomes a lie.* Explicit constructor
keywords remain per-instance overrides that win.

| Field | Env | Default | Why |
|---|---|---|---|
| `inbound_tolerance_seconds: float` | `VARCO_WEBHOOK_INBOUND_TOLERANCE_SECONDS` | `300.0` | brief 013 §2 — Stripe/Svix/Slack all use 5 minutes |
| `inbound_replay_ttl_seconds: float` | `VARCO_WEBHOOK_INBOUND_REPLAY_TTL_SECONDS` | `600.0` | brief 013 §46 — 5–10 min covers a 5 min tolerance |

✅ A **separate** field from the outbound `signature_tolerance_seconds` (`settings.py:79`): outbound
   tolerance is what we ask receivers to honour; inbound tolerance is what we accept from a possibly
   clock-skewed provider. Loosening one must not loosen the other. Defaults are identical, so
   nothing is surprising out of the box.
❌ Two similar-looking knobs. Mitigated by the `inbound_` prefix and an env-var table row each.

### §D-S19-errors — a new 401 `ErrorCode`, a reused 409

| Exception | Base | Status | Mechanism |
|---|---|---|---|
| `WebhookSignatureError` | `ServiceException` | **401** | `register_error_code()` at import time — no built-in maps to 401 (`exception/codes.py:163-184` covers 403/409/422 only). Exact precedent: `IdempotencyKeyInvalidError` (`exception/idempotency.py:125-164`) |
| `WebhookReplayError` | `ServiceConflictError` | 409 | Free via the MRO walk — same trick `IdempotencyKeyConflictError` uses (`exception/idempotency.py:42-47`) |

Home: `varco_core/varco_core/exception/webhook.py`, mirroring `exception/idempotency.py`.
`message_key`s: `varco.error.webhook_signature_invalid`, `varco.error.webhook_replay`.
Both `error_params()` return `{}` (§D-S19-secret).

✅ 401 is right for "you presented a credential and it did not verify"; brief 013 §152 assigns
   401 for signature/timestamp failure and 409 for replay, which this matches exactly.
❌ A new registered `ErrorCode` is process-global state set at import time. Same, already-documented
   contract as `IdempotencyKeyInvalidError` — registration happens at import, before requests.

### §D-S19-posture — **no** `inspect_inbound_webhooks()`; argued, not overlooked

| ID | Choice | Consequence |
|---|---|---|
| D-S19-posture | This plan exports **no** posture inspector and Plan 036's `SecurityPosture` gains nothing | No new cross-plan seam, no registry, no global state |

✅ Every shipped inspector reads structure that exists **whether or not the feature was opted into**:
   `inspect_http_edge(app)` walks `app.user_middleware`, `inspect_rls_posture()` reads the database,
   `inspect_revocation_posture()` reads DI bindings. An inbound verifier is a per-route
   `Depends(...)` object with **no process-global registry to read** — inspecting it would require
   *building* one, i.e. module-global mutable state, which §D-seam of Plan 035 explicitly rejected
   for the same reason ("wrong under multiple apps in one process").
✅ The question an inspector would answer ("is replay protection on?") is already answered **at
   construction, loudly**, by §D-S19-github's `ValueError`. A startup `ValueError` strictly dominates
   a posture warning.
✅ An app with no inbound webhook route would get a permanent, meaningless finding — noise that
   degrades every other posture check.
❌ A misconfigured *non-GitHub* receiver (tolerance widened to a day, no replay guard) is not
   surfaced by 036. Accepted; it is visible in the route's own source, and the Pitfalls table names
   it.
  Rejected — **`inspect_inbound_webhooks(verifiers)` taking an explicit list**: ❌ the caller would
  have to maintain the list by hand, so the check verifies the list, not the app — a posture check
  that cannot be wrong is not a check.
  Rejected — **a module-global verifier registry to enable inspection**: ⛔ process-global mutable
  state, wrong under multiple apps per process (Plan 035 §D-seam's rejected alternative, verbatim).

### §D-S19-conformance — no testkit suite; a `COVERAGE.md` note row instead

`WebhookVerifier` is a new ABC. `testkit/varco_conformance` covers eight `varco_core` ABCs; a ninth
suite would be justified by *out-of-tree implementations needing a contract check*.

| ID | Choice | Consequence |
|---|---|---|
| D-S19-conformance | **No** new `testkit/varco_conformance` module. Instead: one parametrized table in `varco_core/tests/test_webhook_inbound_verifiers.py` running the same contract assertions against all four in-tree verifiers, plus a **note row in `COVERAGE.md`** stating why no suite exists and the un-park trigger | The house rule ("a future absence must be argued against a written record") is satisfied without adding never-packaged surface for four in-tree classes |

✅ All four implementations are in-tree, pure-CPU, and Docker-free — the parametrized table gives
   identical coverage in the fast loop.
✅ `testkit` is never packaged, so a suite cannot reach an out-of-tree implementer anyway (the same
   argument Plan 016 §RL-3d used to decline re-exporting providify's fixtures).
❌ A future out-of-tree verifier gets no shared contract test. Un-park trigger recorded: **the first
   out-of-tree `WebhookVerifier`, or a fifth in-tree provider.**

### Alternatives considered (plan-level)

- **Depend on the `standardwebhooks` PyPI package** (brief 013 §19) — ❌ a new `varco_core` runtime
  dependency for code varco already ships (`signing.py:145-178`); ❌ CLAUDE.md's import-budget rule
  makes a new top-level import a measured cost; ❌ the brief's own Evidence gap 1 says that
  library's version/changelog/test coverage was not reviewed.
- **Ship only the Standard Webhooks verifier** — ❌ the row's motivating sentence names *"a
  Stripe/GitHub/Svix webhook"*; a receiver library that handles one of three is the hand-rolling it
  set out to prevent. The marginal cost is ~20 lines per adapter over a shared template.
- **Put the verifier in `varco_fastapi`** — ❌ inverted seam. Verification is transport-agnostic
  (a Lambda handler, a CLI replay tool, a test) — same rule as `varco_core.tls` vs.
  `varco_fastapi.auth`.
- **Ship it as a `RouteGuard`** — ❌ guards decide authorization from an `AuthContext`; this needs
  the raw body, which a guard does not receive.

---

## Steps

### Phase 1 — the raw-bytes seam on the shipped signer (🟢 S)

1. [x] `varco_core/tests/test_webhook_signing.py` (extend, **failing first**) — `verify()` accepts
       `payload=b"..."`; for a valid-UTF-8 body, `verify(payload=s)` and `verify(payload=s.encode())`
       return the **same** value for the same headers; a body with a lone `\xff` byte verifies
       correctly via the `bytes` path and would have raised via the `str` path.
2. [x] `varco_core/varco_core/webhook/signing.py:124-178` — widen `sign(payload=)` and
       `verify(payload=)` to `str | bytes`; build the signed content as
       `f"{msg_id}.{timestamp}.".encode() + (payload if isinstance(payload, bytes) else payload.encode())`
       at both `:137` and `:170`. `DESIGN:` comment citing §D-S19-gap and brief 013 §50.2.
       **No behaviour change for a `str` caller** — that is what Step 1 pins.
3. [x] `varco_core/tests/test_webhook_signing.py` (extend) — a **round-trip** test:
       `StandardWebhooksSigner.sign()` output verifies through `StandardWebhooksSigner.verify()` with
       `bytes`, byte-identically to the `str` path, for a body containing non-ASCII UTF-8.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_webhook_signing.py -q`

### Phase 2 — the core seam: ABC, result, shared template, SW/Svix adapter (🟡 S)

4. [x] `varco_core/tests/test_webhook_inbound_verifiers.py` (new, **failing first**) — for
       `StandardWebhooksVerifier`: a delivery produced by `StandardWebhooksSigner.sign()` verifies
       (**the outbound↔inbound round trip — the anti-drift proof of §D-S19-gap**); `svix-id`/
       `svix-timestamp`/`svix-signature` aliases verify identically (brief 013 §2); header lookup is
       **case-insensitive** (`Webhook-Id`); and the negative set: tampered body, expired timestamp,
       **future** timestamp beyond tolerance, wrong secret, missing header, malformed
       (non-numeric) timestamp, empty signature header — each asserting the exact
       `VerificationFailure` member.
5. [x] `varco_core/varco_core/webhook/inbound/base.py` (new) — `VerificationFailure` (StrEnum),
       `VerificationResult` (`@dataclass(frozen=True)`), `SecretEncoding` (StrEnum),
       `WebhookVerifier` (ABC). Full docstrings with `Args:`/`Returns:`/`Raises:`/`Edge cases:`/
       `Thread safety:`/`Async safety:`. Module docstring carries the §D-S19-shape `DESIGN:` block
       **and** the "no `@Singleton`/`@Provider`/`@Configuration` here" rule.
6. [x] `varco_core/varco_core/webhook/inbound/verifiers.py` (new) — `HmacWebhookVerifier` (shared
       template: case-insensitive header map, timestamp-first ordering, candidate generation,
       `compare_digest`, result construction) + `StandardWebhooksVerifier` (delegates the signature
       decision to a held `StandardWebhooksSigner`, per §D-S19-gap) + `SvixWebhookVerifier` (a
       header-alias subclass, brief 013 §2).
7. [x] `varco_core/tests/test_webhook_inbound_no_di_side_effect.py` (new) — `import
       varco_core.webhook.inbound` registers nothing: assert the module tree contains no
       `@Singleton`/`@Provider`/`@Configuration` marker attribute, and that a
       `container.scan("varco_core", recursive=True)` gains no binding for any inbound name (same
       shape as the cloudevents/tls guards).

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_webhook_inbound_verifiers.py varco_core/tests/test_webhook_inbound_no_di_side_effect.py -q`

### Phase 3 — the three divergent providers (🟡 S)

8. [x] `varco_core/tests/test_webhook_inbound_verifiers.py` (extend, **failing first**) — Stripe:
       `Stripe-Signature: t=<ts>,v1=<hex>` over `f"{ts}.{body}"` (brief 013 §2); a `v0=`-only header
       is **rejected** (`v1` only, per the brief); multiple `v1=` entries accepted (rotation);
       comma/space-tolerant parsing; a missing `t=` is `MALFORMED_HEADER`. Test docstring records
       the `⚠️ ASSUMPTION` on `RAW_UTF8` secret encoding (§D-S19-secretbytes).
9. [x] `varco_core/tests/test_webhook_inbound_verifiers.py` (extend, **failing first**) — GitHub:
       `X-Hub-Signature-256: sha256=<hex>` over the raw body (brief 013 §2); `timestamp_checked is
       False`; `message_id` comes from `X-GitHub-Delivery`; **construction raises `ValueError`**
       without `replay_guard=` or `acknowledge_no_replay_protection=True`, with both names in the
       message (§D-S19-github); a `sha1=` header is rejected.
10. [x] `varco_core/tests/test_webhook_inbound_verifiers.py` (extend, **failing first**) — Slack:
        `X-Slack-Signature: v0=<hex>` over `f"v0:{ts}:{body}"` with `X-Slack-Request-Timestamp`
        (brief 013 §2); both headers required; 5-minute tolerance; the timestamp is **not** part of
        the signed value the way SW's is — asserted by a vector.
11. [x] `varco_core/varco_core/webhook/inbound/verifiers.py` (extend) — `StripeWebhookVerifier`,
        `GitHubWebhookVerifier`, `SlackWebhookVerifier`, plus
        `get_verifier(provider, *, secrets, **kwargs) -> WebhookVerifier` mirroring `get_signer`
        (`signing.py:371-390`), raising `ValueError` on an unknown provider name.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_webhook_inbound_verifiers.py -q`

### Phase 4 — the replay guard over `AbstractIdempotencyStore` (🟡 S)

12. [x] `varco_core/tests/test_webhook_replay_guard.py` (new, **failing first**) — against
        `InMemoryIdempotencyStore`: first delivery `ACQUIRED` → allowed; an immediate identical
        replay → `WebhookReplayError`; after `complete()`, a replay still rejected within TTL; after
        `release()` (the failure path) the **same delivery id is accepted again** — the
        provider-retry case §D-S19-replay names; concurrent `asyncio.gather` of N identical
        deliveries yields exactly **one** acceptance; the storage key starts with
        `webhook:{provider}:`; a `ttl<=0` raises.
13. [x] `varco_core/varco_core/webhook/inbound/replay.py` (new) — `WebhookReplayGuard`
        (`@dataclass(frozen=True)` over the store + ttl), `async def claim(provider, message_id,
        body) -> None` (raises `WebhookReplayError`), `async def complete(...)`, `async def
        release(...)`. Docstring records that `IN_FLIGHT` vs `REPLAY` map to "concurrent duplicate"
        vs "already handled", and cites `idempotency/base.py:96-101`.
14. [x] `varco_core/varco_core/exception/webhook.py` (new) — `WebhookSignatureError` (+ its
        `register_error_code(..., http_status=401)`), `WebhookReplayError(ServiceConflictError)`.
        Both `error_params() -> {}` (§D-S19-secret). Module docstring mirrors
        `exception/idempotency.py:1-21`.
15. [x] `varco_core/tests/test_webhook_inbound_errors.py` (new) — `error_code_for` /
        `error_message_for` map `WebhookSignatureError` → 401 and `WebhookReplayError` → 409;
        `error_params()` is `{}` for both; neither `str(exc)` nor `error_params()` contains a secret
        or a signature.

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_webhook_replay_guard.py varco_core/tests/test_webhook_inbound_errors.py -q`

### Phase 5 — settings threading (🟢 S)

16. [x] `varco_core/tests/test_webhook_settings_wiring.py` (extend, **failing first**) —
        `WebhookSettings().inbound_tolerance_seconds == 300.0` and
        `inbound_replay_ttl_seconds == 600.0`; `VARCO_WEBHOOK_INBOUND_TOLERANCE_SECONDS=60` parses;
        `get_verifier("stripe", secrets=[...], settings=WebhookSettings(inbound_tolerance_seconds=60))`
        produces a verifier that rejects a 90 s-old delivery; an explicit
        `tolerance_seconds=` constructor keyword **wins** over the settings value (the standing
        override rule); `settings=None` constructs `WebhookSettings()` from the environment.
17. [x] `varco_core/varco_core/webhook/settings.py` — add the two `inbound_*` fields with full
        `Attributes:` docstring entries citing brief 013 §2 / §46. Thread `settings=` through
        `get_verifier()` and every verifier constructor (§D-S19-config).

⛔ **CHECKPOINT** — `uv run pytest varco_core/tests/test_webhook_settings_wiring.py -q`

### Phase 6 — the FastAPI route dependency (🟡 S)

18. [x] `varco_fastapi/tests/test_inbound_webhook_dependency.py` (new, **failing first**) — a route
        with `Depends(verify_webhook(verifier))`: a correctly signed POST returns 200 and the
        handler receives the **exact raw bytes**; a downstream pydantic body model on the *same*
        route still parses (the `Request._body` caching contract,
        `.venv/.../starlette/requests.py:254-260`); a tampered body → **401** with the
        `WebhookSignatureError` code, rendered through the error envelope with a `correlation_id`;
        a replayed delivery → **409**; a handler that **raises** leaves the delivery replayable
        (`release()` ran) while a clean return does not (`complete()` ran); the response body and
        `caplog` contain **neither the secret nor the signature header value** (§D-S19-secret).
19. [x] `varco_fastapi/varco_fastapi/webhook/inbound.py` (new) — `VerifiedWebhook`
        (`@dataclass(frozen=True)`: `body: bytes`, `provider: str`, `message_id: str | None`,
        `timestamp_checked: bool`) and `verify_webhook(verifier, *, replay_guard=None)` returning an
        `async def` dependency **with `yield`**: `await request.body()` → `verifier.verify()` →
        raise `WebhookSignatureError` on failure → `replay_guard.claim()` → `yield VerifiedWebhook`
        → `complete()` on clean exit, `release()` inside `except`. Docstring `Edge cases:` records
        the honest limit: **a handler that *returns* a 5xx without raising is treated as a success**
        and the delivery is not replayable — raise, or call `release()` yourself.
20. [x] `varco_fastapi/varco_fastapi/webhook/__init__.py:14` — export `verify_webhook`,
        `VerifiedWebhook` alongside `mount_webhook_admin`/`build_webhook_router`. **No** change to
        `varco_fastapi/__init__.py` (which today exports no webhook name — verified by grep), and
        **no** edit to `varco_fastapi/varco_fastapi/middleware/__init__.py` (Plan 041 owns it).
21. [x] `varco_fastapi/tests/test_inbound_webhook_dependency.py` (extend) — the `BodyLimitMiddleware`
        interaction: a >10 MiB signed body is rejected **413 before** the dependency runs (assert the
        verifier was never called), and an `exempt_paths`-exempted route reaches the verifier —
        brief 013 §39 and Evidence gap 4, pinned rather than assumed.

⛔ **CHECKPOINT** — `uv run pytest varco_fastapi/tests/test_inbound_webhook_dependency.py varco_fastapi/tests/test_middleware_order.py -q`

### Phase 7 — docs, README, CLAUDE.md, CHANGELOG, gates (🟡 S — **same commit as the code**)

22. [x] `technical_docs/features/inbound-webhooks.md` (new — **a separate file**, argued: CLAUDE.md's
        "one home per fact"; `outbound-webhooks.md` is organised around subscriptions/SSRF/delivery/
        admin, none of which has an inbound analogue, and the two Pitfalls tables are disjoint. A
        "See also" line is added at the top of each, both directions). Sections: module map; the
        provider divergence table (brief 013 §2, cited); why the SW verifier delegates to the
        shipped signer (§D-S19-gap); the replay model over `AbstractIdempotencyStore` and why no new
        ABC (§D-S19-replay); secret sourcing (§D-S19-secret); the route-dependency seam and the
        raw-body/body-limit story (§D-S19-seam). **Pitfalls table**, at minimum: GitHub has **no
        timestamp** so the tolerance window does not apply and a replay guard is mandatory ·
        verifying `await request.json()` output instead of `VerifiedWebhook.body` breaks the
        signature (brief 013 §50.2) · a >10 MiB delivery is 413'd before verification, and exempting
        the path accepts an unbounded buffer · a hand-typed short secret can be silently base64-decoded
        by `_secret_bytes` (§D-S19-secretbytes) · a handler that returns 5xx without raising marks
        the delivery consumed · a replay store shared with `IdempotencyMiddleware` is safe **only**
        because of the `webhook:` key prefix · widening `inbound_tolerance_seconds` widens the replay
        window and the guard TTL should follow · never log the signature header or the secret (Plan
        040 owns the general mechanism) · GitHub's dedup TTL should cover its retry window (24–48 h,
        brief 013 §47), not the 600 s default.
23. [x] `README.md` — an "Inbound webhook verification" section: a runnable
        `@app.post("/hooks/stripe")` snippet with `Depends(verify_webhook(...))`, a Redis-backed
        replay-guard snippet, and a `VARCO_WEBHOOK_INBOUND_*` env-var table (two rows) folded into
        the existing `VARCO_WEBHOOK_` table. `ARCHITECTURE.md` — `WebhookVerifier` /
        `HmacWebhookVerifier` / the four adapters / `VerificationResult` / `WebhookReplayGuard` in
        the webhook type hierarchy.
24. [x] `CLAUDE.md` — pointer-only: (a) extend the *Outbound webhooks* section title/pointer to
        name the inbound half and add **one Rule** that changes agent behaviour — *inbound webhook
        verification reuses `StandardWebhooksSigner.verify()` and `AbstractIdempotencyStore`; never
        a second HMAC path and never a new replay-cache ABC*; (b) a Decision-Tree branch under the
        webhook node: *receiving a signed webhook? → `varco_core.webhook.inbound` +
        `varco_fastapi.webhook.verify_webhook`, never a middleware (per-route secret) and never a
        hand-rolled `hmac`*.
25. [x] `testkit/varco_conformance/COVERAGE.md` — the §D-S19-conformance note row for
        `WebhookVerifier` (why no suite; the un-park trigger). ⚠️ Coordinate with Plan 042, which
        also edits this file.
26. [x] `CHANGELOG.md` `## [Unreleased]` `### Added`: `varco_core.webhook.inbound` (ABC + four
        verifiers + `get_verifier`), `WebhookReplayGuard`, `WebhookSignatureError`/
        `WebhookReplayError`, `varco_fastapi.webhook.verify_webhook`, two `WebhookSettings` fields.
        `### Changed`: `StandardWebhooksSigner.sign()/verify()` accept `bytes` (additive).
        `BACKLOG.md` — mark `S19` `✅ planned → plans/038-inbound-webhook-verification.md` and add
        the parked rows below.
27. [x] `uv run python scripts/api_surface.py` then `--check`. **Expected: no diff** — the inbound
        surface is reached via `varco_core.webhook.inbound` / `varco_fastapi.webhook`, and neither
        `varco_core/__init__.py` (zero webhook names, verified by grep) nor
        `varco_fastapi/__init__.py` (idem) carries a webhook name today. Commit the snapshot files
        if the script rewrites anything; a clean `--check` is itself the deliverable.
28. [x] `uv run python scripts/import_budget.py --check --warn-only` — confirm no ceiling breach.
        `varco_core.webhook.inbound` is **not** imported from `varco_core/__init__.py` (which is PEP
        562 lazy and carries no webhook name), so no change is expected; per CLAUDE.md this is
        checked, not assumed.

⛔ **CHECKPOINT** — `make lint`, `make type-check`, `make test`.

---

## Edge cases

- **Body is not valid UTF-8** → verified via the `bytes` path; never decoded (Step 1). brief 013 §50.2.
- **Header names differ in case** (`Webhook-Id`, `WEBHOOK-TIMESTAMP`) → normalised to lower-case once
  at entry; asserted (Step 4).
- **Svix `svix-*` aliases** → accepted by `SvixWebhookVerifier` and, per brief 013 §2's "either set
  accepted", also by `StandardWebhooksVerifier`. Asserted.
- **Timestamp in the future beyond tolerance** → rejected. The shipped check is `abs(now - ts) >
  tolerance` (`signing.py:167`), which already covers it; asserted explicitly (Step 4).
- **Non-numeric timestamp** → `MALFORMED_HEADER`, never an unhandled `ValueError`.
- **Empty / whitespace-only signature header** → `MALFORMED_HEADER`, no comparison attempted.
- **Multiple space-delimited signatures, one valid** → accepted (rotation, brief 013 §18); nothing
  reports which (§D-S19-rotation).
- **GitHub delivery with no replay guard and no acknowledgement** → `ValueError` at construction, at
  startup (§D-S19-github).
- **Provider retries a delivery our handler raised on** → `release()` ran, so the retry is accepted
  and processed (§D-S19-replay).
- **Handler returns a 5xx `Response` without raising** → treated as success; the delivery is *not*
  replayable. Documented limitation, in the dependency docstring and the Pitfalls table.
- **Two identical deliveries land concurrently** → exactly one `ACQUIRED`; the other gets 409
  (`IN_FLIGHT`). Asserted with `asyncio.gather` (Step 12).
- **No replay guard supplied (non-GitHub)** → signature + tolerance only; `message_id` is still
  reported so the app can dedupe itself. brief 013 §146.
- **Body over the 10 MiB ceiling** → 413 before verification (§D-S19-seam); asserted (Step 21).
- **`container.scan("varco_core", recursive=True)`** → no binding, no side effect (Step 7).

## Verification

```bash
uv sync --all-packages --all-extras

uv run pytest varco_core/tests/test_webhook_signing.py \
              varco_core/tests/test_webhook_inbound_verifiers.py \
              varco_core/tests/test_webhook_inbound_no_di_side_effect.py \
              varco_core/tests/test_webhook_replay_guard.py \
              varco_core/tests/test_webhook_inbound_errors.py \
              varco_core/tests/test_webhook_settings_wiring.py \
              varco_core/tests/test_webhook_dispatcher.py -q

uv run pytest varco_fastapi/tests/test_inbound_webhook_dependency.py \
              varco_fastapi/tests/test_middleware_order.py \
              varco_fastapi/tests/test_mount_webhook_admin.py -q

uv run python scripts/api_surface.py --check          # MUST be clean
uv run python scripts/import_budget.py --check --warn-only
make lint && make type-check && make test
```

**DoD:**
1. Step 4's round trip proves `StandardWebhooksSigner.sign()` output verifies through
   `StandardWebhooksVerifier` — inbound and outbound cannot drift.
2. Every negative case from the mandate (tampered body, expired timestamp, future timestamp, wrong
   secret, replayed id, missing header, malformed header) has a named test asserting a specific
   `VerificationFailure`.
3. Step 12 proves the provider-retry-after-failure path is *not* swallowed by the replay guard.
4. Step 18 proves neither the secret nor the signature reaches the response body or the log.
5. Step 27's `api_surface.py --check` is clean; Step 28's budget check shows no breach.
6. `varco_fastapi/varco_fastapi/middleware/__init__.py` and `app.py` are **untouched** (Plan 041's
   files) — verifiable with `git diff --stat`.

## Parked

| Item | Why | Un-park trigger |
|---|---|---|
| Ed25519 / asymmetric Standard Webhooks (`whsk_`/`whpk_`, brief 013 §11) | Needs a crypto dependency `varco_core` does not have | A consumer receives from an asymmetric-mode sender, or varco_core gains a crypto dependency for another reason |
| An inbound `Rfc9421Verifier` | `Rfc9421Signer.verify()` (`signing.py:297-351`) already takes `bytes` + the request line and is inbound-shaped; wiring it behind the ABC is additive. No provider in brief 013 §2 uses it | A consumer receives RFC 9421-signed webhooks |
| A `RetentionPolicy` adapter for the replay store's `delete_expired()` | Plan 039 owns the registry | Plan 039 ships |
| A `testkit/varco_conformance/webhook_verifier.py` suite | §D-S19-conformance | The first out-of-tree `WebhookVerifier`, or a fifth in-tree provider |
| More providers (Shopify, Twilio, PayPal, Discord) | Four covers brief 013 §2's table and the row's named cases | A consumer asks; each is ~20 lines on the shared template |
| A posture inspector for inbound receivers | §D-S19-posture | A registry of verifiers becomes structurally readable without global mutable state |

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| ⚠️ **ASSUMPTION — Stripe keys its HMAC on the secret string's raw UTF-8 bytes, not a base64 decode of it.** brief 013 §2 describes it as a *"`whsec_` base64 secret"*, which conflicts with stripe-python's behaviour; the brief supplies no known-answer vector (its own Evidence gap 1 is adjacent) | **High — a wrong encoding means every Stripe delivery fails verification** | §D-S19-secretbytes makes the policy an explicit `SecretEncoding` enum member, so the fix is one line; Step 8's test docstring records the assumption; the feature doc states it. A real Stripe delivery in a consumer's staging environment settles it |
| ⚠️ **ASSUMPTION — no authoritative known-answer vectors were retrieved for Stripe/GitHub/Slack.** The brief gives formats, not vectors | Medium | Tests are round-trip (we construct the header with the documented algorithm, then verify), which pins the *implementation against the documented formula* but not against the provider. Flagged in the feature doc; the un-risk is a consumer replaying a captured real delivery |
| ⚠️ **ASSUMPTION — `Request._body` caching makes a `Depends` raw-body read safe for downstream pydantic parsing.** Verified against the pinned Starlette (`.venv/.../starlette/requests.py:234-236`, `:254-260`) but `_body` is a private attribute of a third-party class | **High — §D-S19-seam rests on it** | Step 18 asserts the combined case (dependency + body model on one route) explicitly, so a Starlette upgrade that changes it fails loudly here. Fallback if it ever breaks: hand the raw bytes to the handler and document that the handler must not also declare a body model — a doc change, not a redesign |
| **Using `IN_FLIGHT` to mean "seen" stretches `AbstractIdempotencyStore`'s intent** | Medium | Argued in §D-S19-replay with the `complete()`/`release()` mapping that makes it exact; Step 12 pins all four transitions; the mandatory `webhook:` key prefix keeps it out of `IdempotencyMiddleware`'s namespace |
| **A replay store shared with `IdempotencyMiddleware` collides** | Medium | Mandatory `webhook:{provider}:` prefix, asserted (Step 12); documented as a Pitfall |
| ⚠️ **Widening `StandardWebhooksSigner.verify()`'s `payload` to `str \| bytes` touches shipped, wire-affecting code** | Medium | Additive only; Steps 1 and 3 pin byte-identical behaviour for existing `str` callers *before* the change lands; the existing `test_webhook_signing.py`/`test_webhook_dispatcher.py` suites run unchanged in the same checkpoint |
| **The inherited `_secret_bytes` base64 heuristic silently mis-keys a short non-`whsec_` secret** | Medium | Not changed (it is shipped outbound behaviour); documented as a Pitfalls row with the fix (use the provider's real secret verbatim) |
| **`COVERAGE.md` edit collides with Plan 042** | Low | One appended row; whichever lands second rebases it. Named here so it is expected |
| **Scope creep into 041's ordering table** | Low | §D-S19-seam adds no middleware; DoD item 6 makes the absence of a diff in 041's files a checked condition |
| **GitHub's `acknowledge_no_replay_protection` becomes the common path** | Medium — it would ship replay-unprotected receivers with a clear conscience | The `ValueError` message names the store option first and the acknowledgement second; the feature doc's first Pitfalls row is GitHub's missing timestamp. Un-park trigger for revisiting: the keyword shows up in consumer code |

## Open questions

1. **Should `StandardWebhooksVerifier` also accept the `svix-*` aliases, or only
   `SvixWebhookVerifier`?** brief 013 §2 says Svix libraries accept either set. Lean **yes, both
   accept both**, with `SvixWebhookVerifier` existing only so `provider` reads `"svix"` in logs and
   replay keys. Decide at Step 6.
2. **Should `WebhookReplayGuard.claim()` take the already-computed `VerificationResult` instead of
   `(provider, message_id, body)`?** It would remove an opportunity to pass a mismatched provider.
   Lean yes; decide at Step 13.
3. **Should the FastAPI dependency emit a metric/span on a verification failure?** A 401 rate per
   provider is genuinely useful, but `varco_core.observability` wiring here would be the first
   observability call in the webhook package. Lean **no in this plan** — the 401 is already counted
   by `MetricsMiddleware` — and revisit if an operator asks for per-provider granularity.
4. **Does `inbound_replay_ttl_seconds` need a per-provider override for GitHub's 24–48 h retry
   window (brief 013 §47)?** Lean: no new field — pass `ttl_seconds=` to `WebhookReplayGuard`
   explicitly for the GitHub route, and document it. Decide at Step 13.

## BACKLOG entries this plan creates

| ID | Row | Where |
|---|---|---|
| — | `S19` → `✅ planned → plans/038-inbound-webhook-verification.md` | `BACKLOG.md:78` |
| new | *Ed25519 (asymmetric) Standard Webhooks verification* — parked, trigger recorded | Parked table |
| new | *Inbound `Rfc9421Verifier`* — parked, `Rfc9421Signer.verify()` is already inbound-shaped | Parked table |
| new | *Confirm Stripe's HMAC secret encoding against a real delivery* — the §D-S19-secretbytes assumption, filed as a question with evidence, not a fix | Live table, 🟡 |
