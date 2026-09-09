# Inbound webhook verification

See also: [outbound-webhooks.md](outbound-webhooks.md) (the delivery half of this pair).

Plan 038 (S19) — a varco app that *receives* a webhook from Stripe, GitHub, Slack, Svix, or any
Standard Webhooks-conformant sender verifies it with one route dependency instead of hand-rolling
`hmac` and getting one of brief 013's six named pitfalls wrong. Nothing is enabled by default and
no shipped behaviour changes.

## Module map

| Concern | Module |
|---|---|
| ABC + result/failure types | `varco_core.webhook.inbound.base` (`WebhookVerifier`, `VerificationResult`, `VerificationFailure`, `SecretEncoding`) |
| Shared HMAC template + four adapters + dispatch | `varco_core.webhook.inbound.verifiers` (`HmacWebhookVerifier`, `StandardWebhooksVerifier`, `SvixWebhookVerifier`, `StripeWebhookVerifier`, `GitHubWebhookVerifier`, `SlackWebhookVerifier`, `get_verifier`) |
| Replay guard | `varco_core.webhook.inbound.replay.WebhookReplayGuard` |
| Errors | `varco_core.exception.webhook` (`WebhookSignatureError` 401, `WebhookReplayError` 409) |
| Settings | `varco_core.webhook.settings.WebhookSettings` (`inbound_tolerance_seconds`, `inbound_replay_ttl_seconds`) |
| FastAPI route dependency | `varco_fastapi.webhook.inbound.verify_webhook` / `VerifiedWebhook` |

## Provider divergence (brief 013 §2)

| Provider | Header(s) | Signed content | Timestamp? | Dedup id |
|---|---|---|---|---|
| Standard Webhooks / Svix | `webhook-id`/`webhook-timestamp`/`webhook-signature` (or `svix-*` aliases — both accepted by either class) | `{id}.{timestamp}.{body}` | Yes, 300s default | `webhook-id`/`svix-id` |
| Stripe | `Stripe-Signature: t=<ts>,v1=<hex>[,v1=<hex>...]` | `{timestamp}.{body}` | Yes, 300s default | none in headers (in the JSON body — not parsed here) |
| GitHub | `X-Hub-Signature-256: sha256=<hex>` | raw body only | **No** — structurally unavailable | `X-GitHub-Delivery` |
| Slack | `X-Slack-Signature: v0=<hex>` + `X-Slack-Request-Timestamp` | `v0:{timestamp}:{body}` | Yes, 300s default | none |

## Why the Standard Webhooks/Svix verifier delegates (§D-S19-gap)

`StandardWebhooksSigner.verify()` already ships a correct, constant-time, rotation-aware HMAC
implementation (`varco_core/varco_core/webhook/signing.py:145-178`). `StandardWebhooksVerifier`
holds one, built from the same secrets/tolerance, and delegates the signature *decision* to it —
so inbound and outbound cannot drift for the one scheme varco already implements. A round-trip
test (`test_webhook_inbound_verifiers.py::TestStandardWebhooksVerifierRoundTrip`) proves a
delivery produced by `StandardWebhooksSigner.sign()` verifies through this class. Stripe/GitHub/
Slack have no shipped implementation to reuse — three genuinely different signed-basestring
constructions — so they use the shared `HmacWebhookVerifier` template instead (case-insensitive
header lookup, a tolerance-window helper, a constant-time any-secret-matches loop), each writing
its own header parsing and signed-content construction.

`StandardWebhooksSigner.sign()`/`verify()` were additively widened to accept `payload: str |
bytes` for this plan (§D-S19-gap gap (c) — the raw-body seam) — a `str` caller sees byte-identical
behaviour; a `bytes` caller can verify a body that is not valid UTF-8, which a `str`-only API
cannot.

## The replay model — no new ABC (§D-S19-replay)

`WebhookReplayGuard` is a ~40-line adapter over the shipped `AbstractIdempotencyStore`
(`reserve`/`complete`/`release`), not a new ABC + backends. `reserve()` is already documented as
*"the single atomic primitive… concurrent callers racing on the same key receive exactly one
ACQUIRED"* — exactly what a replay guard needs, on four production backends (in-memory, Redis,
SQLAlchemy, Beanie) on day one.

Outcome mapping:

- `ACQUIRED` → proceed; call `complete()` on a clean return or `release()` on failure.
- `IN_FLIGHT`/`REPLAY` → `WebhookReplayError` (409).

The `complete()`/`release()` split is what makes a provider's retry-after-our-failure case
correct: `release()` removes the reservation entirely, so the next `claim()` for the same id sees
`ACQUIRED` again. A naive `has_seen`/`mark_seen` cache would swallow that retry and lose the event
permanently.

The mandatory `webhook:{provider}:{message_id}` key prefix keeps this guard's key space disjoint
from `IdempotencyMiddleware`'s own keys when a store instance is shared between the two.

## Secret sourcing (§D-S19-secret)

Inbound secrets are **deployment configuration**, not tenant-owned rows — passed to a verifier's
constructor as `list[str]`, sourced from env or a secret manager. There is no repository, no new
entity, no `FieldEncryptor` path, and no migration: `WebhookSubscription.active_secrets` models
*a subscriber we deliver to* (tenant-scoped, paired with a `target_url`), which is the wrong shape
for a secret **issued to us** by an upstream with no tenant ownership and a cardinality of one per
upstream integration.

Per-provider secret encoding (§D-S19-secretbytes): Standard Webhooks/Svix strip a conventional
`whsec_` prefix and base64-decode (delegated to `StandardWebhooksSigner._secret_bytes`);
Stripe/GitHub/Slack key on the secret string's raw UTF-8 bytes.

⚠️ **Assumption, not a known-answer vector**: brief 013 §2 describes Stripe's secret as a
`whsec_`-prefixed base64 value, which conflicts with stripe-python's own behaviour (raw UTF-8
bytes). This plan pins `RAW_UTF8` for Stripe. If a real Stripe delivery disproves it, the fix is
one `SecretEncoding` value on one class.

## The route-dependency seam (§D-S19-seam)

`varco_fastapi.webhook.inbound.verify_webhook(verifier, *, replay_guard=None)` is a **dependency
factory**, injected per route — never a middleware, never a `create_varco_app()` keyword, and no
edit to the ordering table (`varco_fastapi/varco_fastapi/middleware/__init__.py` — owned by a
sibling plan). A middleware structurally cannot do this: the secret and the provider are per-route
facts (`/hooks/stripe` and `/hooks/github` need different secrets *and* different algorithms).

```python
from fastapi import Depends, FastAPI
from varco_core.webhook.inbound import get_verifier
from varco_fastapi.webhook import VerifiedWebhook, verify_webhook

app = FastAPI()
verifier = get_verifier("stripe", secrets=["whsec_..."])
dependency = verify_webhook(verifier)


@app.post("/hooks/stripe")
async def receive_stripe(webhook: VerifiedWebhook = Depends(dependency)) -> dict:
    # webhook.body is the exact raw bytes that were verified.
    return {"received": True}
```

With a replay guard (recommended for GitHub — mandatory unless you pass
`acknowledge_no_replay_protection=True`):

```python
from varco_core.webhook.inbound import WebhookReplayGuard
from varco_redis.idempotency import RedisIdempotencyStore  # any AbstractIdempotencyStore backend

guard = WebhookReplayGuard(store=RedisIdempotencyStore(...), ttl_seconds=600.0)
github_verifier = get_verifier("github", secrets=["..."], replay_guard=guard)
github_dependency = verify_webhook(github_verifier, replay_guard=guard)
```

### The raw-body / body-limit story

`await request.body()` caches into Starlette's `Request._body`, and `Request.stream()` re-yields
the cached value afterwards — so a downstream pydantic body model declared on the *same* route
still parses correctly even though this dependency already consumed the body. The verified bytes
are handed to the handler as `VerifiedWebhook.body`, so a handler that wants byte fidelity never
re-serializes anything.

`BodyLimitMiddleware` (on by default at 10 MiB, outside every inner layer) rejects an over-limit
body with a 413 *before* this dependency ever buffers anything — no coordination is needed. A
provider sending more than 10 MiB is rejected before verification; exempting the path via
`exempt_paths`/`VARCO_BODY_LIMIT_EXEMPT_PATHS` means accepting an unbounded buffer on it.

## Settings

| Field | Env | Default |
|---|---|---|
| `inbound_tolerance_seconds` | `VARCO_WEBHOOK_INBOUND_TOLERANCE_SECONDS` | `300.0` |
| `inbound_replay_ttl_seconds` | `VARCO_WEBHOOK_INBOUND_REPLAY_TTL_SECONDS` | `600.0` |

Deliberately separate from the outbound `signature_tolerance_seconds` — outbound tolerance is
what we ask receivers of *our own* deliveries to honour; inbound tolerance is what we accept from
a possibly clock-skewed upstream provider. An explicit `tolerance_seconds=` constructor keyword on
any verifier always wins over the settings value.

## What this plan does not do

- No new HMAC implementation for Standard Webhooks (delegates to the shipped signer).
- No `inspect_inbound_webhooks()` posture inspector — a per-route `Depends(...)` object has no
  process-global registry to read; building one would be the exact module-global mutable state
  the framework's own posture design rejects elsewhere. A misconfigured non-GitHub receiver
  (widened tolerance, no replay guard) is visible in the route's own source instead.
- No retention registry/scheduled sweep for the replay store's `delete_expired()` — a future
  `RetentionPolicy` adapter, once the sibling retention plan ships.
- No asymmetric (Ed25519) Standard Webhooks verification, no inbound RFC 9421 verifier — both
  parked with an un-park trigger (see the plan's Parked table).

## Pitfalls

| Pitfall | Why | Fix |
|---|---|---|
| Constructing `GitHubWebhookVerifier` with no `replay_guard=` and no acknowledgement | GitHub ships no timestamp — the tolerance window structurally cannot bound a replay | Pass `replay_guard=<WebhookReplayGuard>`, or `acknowledge_no_replay_protection=True` if you accept the risk |
| Verifying `await request.json()` output instead of `VerifiedWebhook.body` | Re-serializing a parsed body changes its exact bytes — the signature was computed over the original bytes | Always verify/forward the raw bytes; never re-encode a parsed model before hashing |
| A >10 MiB delivery never reaching the verifier | `BodyLimitMiddleware` rejects it 413 first, by design | Raise `VARCO_BODY_LIMIT_MAX_BYTES`, or add the path to `exempt_paths` (accepting an unbounded buffer on it) |
| A hand-typed short secret silently base64-decoded into the wrong key bytes | `StandardWebhooksSigner._secret_bytes`'s base64 fallback decodes anything that *looks* base64-encoded, even an unrelated short ASCII string | Use the provider's real `whsec_`-prefixed secret verbatim; never a hand-typed short string |
| A handler that returns a 5xx `Response` without raising | Treated as success at this layer — the delivery is marked `complete()` and is not replayable | Raise instead of returning an error response, or call `replay_guard.release(...)` yourself |
| Sharing a replay store with `IdempotencyMiddleware` | Safe **only** because of the mandatory `webhook:{provider}:` key prefix | Do not change or bypass the prefix when adapting `WebhookReplayGuard` |
| Widening `inbound_tolerance_seconds` without widening the replay guard's TTL | A wider tolerance window widens the practical replay window too | Keep `inbound_replay_ttl_seconds` (or an explicit `ttl_seconds=`) at least as large as `2 × inbound_tolerance_seconds` |
| Logging the raw signature header or the secret for debugging | Both are sensitive — never appear in `VerificationResult`, `WebhookSignatureError.error_params()`, or this feature's own log lines (only `provider`/`message_id`/`failure` do) | Log only `provider`/`message_id`/`failure`; never the header value or the secret |
| Using the 600s replay-guard default for a GitHub receiver | GitHub's retry window is measured in hours, not minutes (brief 013 §47: 24-48h), far longer than the 300s tolerance window other providers use | Pass an explicit, longer `ttl_seconds=` to `WebhookReplayGuard` for a GitHub route |
