# Research 013 — Inbound Webhook Signature Verification

Date: 2026-09-08 · Freshness matters: **yes** — provider implementations, header formats, and timestamp tolerances are stable in the major platforms as of 2026; ASGI patterns remain current.

## Question

What are the receiver-side signature verification requirements for inbound webhooks from Stripe, GitHub, Svix, Slack, and the Standard Webhooks specification, and what is a framework-level design for verifying them in varco without hand-rolling HMAC compare logic, timestamp windows, or replay cache?

## Findings

### Standard Webhooks specification (receiver requirements)

- **Headers**: Three mandatory headers; `webhook-id` (unique attempt ID), `webhook-timestamp` (Unix seconds), `webhook-signature` (space-delimited list of signatures) — [Standard Webhooks spec](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md)
- **Signed payload construction**: `msg_id.timestamp.payload` (three components, periods as delimiters); the payload must be the exact raw request bytes (no re-serialization) — [spec](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md)
- **Secret encoding**: Base64-encoded, with `whsec_` prefix for symmetric (HMAC-SHA256) secrets, `whsk_` prefix for private keys (Ed25519 asymmetric), `whpk_` for public keys. Receiver strips the prefix and base64-decodes before use — [spec](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md); [whsec_ handling](https://www.svix.com/resources/glossary/webhook-secret/)
- **Timestamp tolerance**: Spec text says "verify the `webhook-timestamp` header has a timestamp that is within some allowable tolerance of the current timestamp to prevent replay attacks" but does *not* mandate a specific value — [spec](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md)
- **Constant-time comparison**: "Use a constant time comparison function to compare the calculated with the expected signature" to prevent timing-attack recovery of the HMAC — [spec](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md)
- **Multiple signatures**: Space-delimited list in the `webhook-signature` header; "try to verify each signature until one matches" during key rotation windows — [spec](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md)
- **Reference implementation**: Python library available at [`standardwebhooks` PyPI](https://pypi.org/project/standardwebhooks/) ([source](https://github.com/standard-webhooks/standard-webhooks/tree/main/libraries/python)); implements `Webhook(base64_secret).verify(payload, headers)` — **note: this is an external dependency, but varco's outbound `StandardWebhooksSigner` already uses the same spec, so inbound and outbound are byte-compatible**
- **Spec status**: Normative (v1.0, adopted by OpenAI, Anthropic, Google Gemini, Kong, Svix, Supabase, Vanta, Drata, Etsy, PagerDuty, Twilio, TaskRabbit, and others) — [adoption tracking](https://github.com/standard-webhooks/standard-webhooks?tab=readme-ov-file)

### Provider-specific signature formats and divergences

| Provider | Header(s) | Signed basestring | Digest/encoding | Timestamp | Replay window | Notes |
|---|---|---|---|---|---|---|
| **Standard Webhooks** | `webhook-id`, `webhook-timestamp`, `webhook-signature` | `id.timestamp.payload` | HMAC-SHA256, hex, space-delimited (rotation) | Unix seconds | Yes, via tolerance window + message ID cache | Spec does not mandate tolerance value; see Svix (5 min) and Stripe (5 min) for de facto standard |
| **Stripe** | `Stripe-Signature` (comma-delimited KV) | `timestamp.payload` | HMAC-SHA256, hex, keyed on `whsec_` base64 secret; `t=<timestamp>,v1=<sig>` format | Included in signed value | Yes; default ±5 minutes (`300` sec), via `constructEvent(tolerance=)` | `v1=` is current; `v0=` legacy — verify `v1=` only unless handling legacy; documented replay guidance: "Since the timestamp is part of the signed payload, it's verified by the signature, so an attacker can't change the timestamp without invalidating the signature" — [Stripe docs](https://docs.stripe.com/webhooks), [webhook signature ref](https://docs.stripe.com/webhooks/signature) |
| **GitHub** | `X-Hub-Signature-256` | Raw body only | HMAC-SHA256, hex, prefixed `sha256=` | **None** | **No built-in protection** — no timestamp in header or payload; relies on `X-GitHub-Delivery` header (unique per attempt) for deduplication if receiver maintains a cache | Use `X-GitHub-Delivery` header (UUID per delivery attempt) as message ID + optional app-level nonce cache for replay protection; no timestamp tolerance window available — [GitHub docs](https://docs.github.com/webhooks-and-events/webhooks/securing-your-webhooks) |
| **Svix** | `svix-id`, `svix-timestamp`, `svix-signature` (aliases for Standard Webhooks headers) | `id.timestamp.payload` | HMAC-SHA256, hex, same as Standard Webhooks | Unix seconds | Yes, 5 minutes default (hardcoded in Svix libraries); documented tolerance = "webhooks with a timestamp that are more than five minutes away from the current time" are rejected — [Svix docs](https://docs.svix.com/receiving/verifying-payloads/why), [announcement](https://www.svix.com/blog/standard-webhooks/) | Byte-identical to Standard Webhooks; Svix headers are branded aliases; either set of header names accepted by Svix libraries |
| **Slack** | `X-Slack-Signature`, `X-Slack-Request-Timestamp` | `v0:timestamp:raw_body` | HMAC-SHA256, hex, prefixed `v0=`; both headers required | Unix seconds (in `X-Slack-Request-Timestamp`) | Yes, 5 minutes default; "reject webhooks where the timestamp differs from your server time by more than 5 minutes" — [Slack docs](https://docs.slack.dev/authentication/verifying-requests-from-slack/) | Timestamp is *not* part of signed value (unlike Standard Webhooks / Stripe); instead, it is hashed as-is into the basestring; constant-time comparison required |

### The raw-body problem in ASGI/FastAPI

- **Core issue**: Starlette/FastAPI parse and may consume the body; signature verification requires the *exact raw bytes* before any deserialization or re-serialization.
- **Middleware solution (current best practice, 2025–2026)**: 
  - **ASGI-level middleware**: Create a custom `receive()` wrapper that buffers the entire body via `receive` loop, accumulating chunks while checking the `more_body` flag, then replays the buffered body downstream via a modified `receive_wrapper` — [Starlette docs](https://starlette.dev/requests/), [middleware discussion](https://github.com/encode/starlette/discussions/1729)
  - **BaseHTTPMiddleware approach** (if using Starlette's `BaseHTTPMiddleware`): The framework caches request body automatically when `request.body()` is called, making it available to both middleware and downstream handlers; **caveat**: this can neutralize streaming bodies, so not suitable for very large uploads — [Starlette middleware](https://starlette.dev/middleware/)
  - **Route dependency alternative** (single route): Inject `Request` and call `await request.body()` to get raw bytes before FastAPI's JSON dependency tries to parse. **Simpler than middleware but only works for one route.**
- **Interaction with body-limit middleware** (varco's `BodyLimitMiddleware` from Plan 035): The body-limit check happens *before* signature verification; if a webhook body exceeds the limit, it is rejected with `413` before the signature is even checked. This is safe — the signature verifier never sees an over-limit body. Order: `ErrorMiddleware` → `RequestContextMiddleware` → `BodyLimitMiddleware` → `SecurityHeadersMiddleware` → `RateLimitMiddleware` → app. — [varco CLAUDE.md, §HTTP edge hardening](https://github.com/edoardoscarpaci/varco/blob/main/CLAUDE.md)
- **Body consumption pitfall**: Calling `request.stream()` *without* buffering to memory makes all subsequent `.body()`, `.form()`, `.json()` calls raise an error. Always buffer the full body before any downstream parsing if you need both the raw bytes and the parsed form. — [Starlette docs](https://starlette.dev/requests/)

### Replay protection: best practices

- **Timestamp window alone is insufficient** — it only protects against replays *after* the window closes. A webhook delivered within the tolerance window can be replayed indefinitely within that window.
- **Message ID (or delivery ID) deduplication is required** — cache recently seen IDs and reject duplicates, even if the timestamp is valid. — [webhooks.fyi](https://webhooks.fyi/security/replay-prevention), [OWASP guidance](https://github.com/OWASP/CheatSheetSeries/blob/master/cheatsheets_draft/Webhook_Security_Guidelines_Cheat_Sheet.md)
- **Cache TTL guidance** — With a 5-minute tolerance window, a cache with 5–10 minute TTL covers it; the ID only needs to be remembered for the length of the tolerance window. Standard Webhooks spec does not mandate this, but industry practice is uniform. — [OWASP cheat sheet](https://github.com/OWASP/CheatSheetSeries/blob/master/cheatsheets_draft/Webhook_Security_Guidelines_Cheat_Sheet.md), [Svix resources](https://www.svix.com/resources/glossary/replay-attack/)
- **For providers without timestamps (GitHub)** — Use the delivery ID (`X-GitHub-Delivery`) as the dedup key and extend the cache TTL to cover the provider's documented retry window (GitHub documents no specific retry window, but industry norm is 24–48 hours). Alternatively, maintain a per-webhook durable record of seen deliveries. — [GitHub webhook security](https://docs.github.com/webhooks-and-events/webhooks/securing-your-webhooks)
- **Implementation**: Async cache (Redis, memcached, SQL) with `reserve(key, ttl)` atomic primitive — similar to varco's `IdempotencyStore` pattern (Plan 029 / D1) — or an in-process `set[tuple[provider, id]]` with background cleanup for dev/test.

### Security pitfalls a verifier must not commit

1. **Non-constant-time string comparison** — Using `==` or `.equals()` for signature comparison is vulnerable to timing attacks; an attacker measures response time to deduce the HMAC byte-by-byte. **Must use `hmac.compare_digest()` (Python stdlib)** or equivalent constant-time function. — [OWASP cheat sheet](https://github.com/OWASP/CheatSheetSeries/blob/master/cheatsheets_draft/Webhook_Security_Guidelines_Cheat_Sheet.md), [Snyk advisory](https://security.snyk.io/vuln/SNYK-GOLANG-GITHUBCOMADNANHWEBHOOKHOOK-537826)

2. **Verifying the parsed or re-serialized body instead of raw bytes** — If middleware or a handler has already parsed the JSON, re-serializing it may produce different byte sequences (whitespace, key order, unicode escaping), invalidating the signature. **Always verify against the raw request body before any parsing.** — [GitHub docs](https://docs.github.com/webhooks-and-events/webhooks/securing-your-webhooks), [Stripe docs](https://docs.stripe.com/webhooks/signature)

3. **Accepting an unbounded or missing timestamp** — A receiver that does not check the timestamp allows arbitrarily old webhooks (even from before the secret was issued). **Always validate `webhook-timestamp` is within the tolerance window** (e.g., ±5 minutes of server time). — [Standard Webhooks spec](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md)

4. **Trusting a signature header before checking the timestamp** — If the timestamp is checked *after* the signature, an attacker can forge a signature for an out-of-window timestamp and exploit a code path that uses the signature before validating recency. **Always check timestamp first, then signature.** (Stripe's libraries do this; Standard Webhooks spec implies it.)

5. **Logging or exposing the signing secret** — A secret in a log file or error message can be used to forge webhooks indefinitely. **Never log webhook secrets; treat them like API keys or passwords.** — [Standard Webhooks security](https://github.com/standard-webhooks/standard-webhooks), [OWASP cheat sheet](https://github.com/OWASP/CheatSheetSeries/blob/master/cheatsheets_draft/Webhook_Security_Guidelines_Cheat_Sheet.md)

6. **No idempotent deduplication** — Accepting a webhook twice silently causes duplicate side effects. **Always combine timestamp + message ID deduplication, even if the provider claims retries are rare.** — [Svix guidance](https://www.svix.com/resources/glossary/replay-attack/), [webhooks.fyi](https://webhooks.fyi/security/replay-prevention)

## Design Recommendation for varco

### Architecture shape

**Generic verifier ABC + per-provider adapters**, not one "mega-verifier" with if/else branches:

```python
# varco_core.webhook.inbound
@runtime_checkable
class WebhookVerifier(Protocol):
    """Verify inbound webhook authenticity (provider-agnostic interface)."""
    
    async def verify(
        self,
        body: bytes,
        headers: dict[str, str],
        timestamp_tolerance_seconds: int = 300,
    ) -> VerificationResult:
        """
        Verify webhook signature and replay (if applicable).
        
        Args:
            body: Raw request body (pre-JSON-parsing).
            headers: HTTP headers (case-insensitive key access recommended).
            timestamp_tolerance_seconds: Max age of webhook (default 5 min).
        
        Returns:
            VerificationResult with verified=True/False, message_id (if applicable).
            
        Raises:
            WebhookVerificationError: If verification fails (wrong signature, expired).
            WebhookReplayError: If message ID already processed (replay detected).
        """

@dataclass(frozen=True)
class VerificationResult:
    verified: bool
    message_id: str | None  # For deduplication
    provider: str
    error_code: str | None = None  # e.g., "signature_mismatch", "timestamp_expired"
```

**Provider implementations** (one per major provider):
- `StandardWebhooksVerifier` — Uses HMAC-SHA256 on `id.timestamp.payload`
- `StripeWebhookVerifier` — Uses HMAC-SHA256 on `timestamp.payload`, handles `v1=`/`v0=` variants
- `GitHubWebhookVerifier` — Uses HMAC-SHA256 on raw body, relies on `X-GitHub-Delivery` for dedup key
- `SlackWebhookVerifier` — Uses HMAC-SHA256 on `v0:timestamp:body`
- `SvixWebhookVerifier` — Subclass or alias of `StandardWebhooksVerifier` (protocol-compatible)

**Replay cache ABC** (optional but recommended):

```python
# varco_core.webhook.inbound.replay
class AbstractWebhookReplayCache(ABC):
    """Deduplicate webhook deliveries by message ID."""
    
    async def has_seen(self, provider: str, message_id: str) -> bool:
        """Check if message ID was seen before."""
    
    async def mark_seen(self, provider: str, message_id: str, ttl_seconds: int) -> None:
        """Mark message ID as seen for TTL seconds."""
```

Implementations:
- `InMemoryWebhookReplayCache` — `dict[tuple[provider, id], expires_at]` + asyncio.Task cleanup (dev/test)
- `RedisWebhookReplayCache` — Uses Redis `SET NX EX` (production)
- `SAWebhookReplayCache` — SQLAlchemy with a `webhook_idempotency` table + TTL-based sweep (when DB is the source of truth)

### Seam and wiring

- **Middleware** (for raw body capture):
  - `WebhookSignatureMiddleware` — Wraps `receive()` to buffer body and attach raw bytes to `scope["webhook.raw_body"]` for downstream access; **opt-in via `install_middleware_stack(..., signature_middleware=True)` in varco_fastapi**, positioned *inside* `ErrorMiddleware` and *after* `BodyLimitMiddleware` so body-too-large is caught before buffering
  - Alternative: Route-level via dependency `async def webhook_body(request: Request) -> bytes`
  
- **Route dependency** (for verification):
  - `@route.post("/webhook/...")` can inject `WebhookVerifier` (DI-bound) and `WebhookReplayCache` (optional), call them with raw body + headers, and raise `401 Unauthorized` if verification fails or `409 Conflict` if replay detected
  
- **DI wiring**:
  - `bootstrap()` in a new `varco_core.webhook.inbound.di` module binds default implementations
  - `enable_webhook_verification(container, verifier_cls=StandardWebhooksVerifier, replay_cache_cls=...)` to opt-in specific providers
  - App-level: `container = bootstrap(); enable_webhook_verification(container, StripeWebhookVerifier)`

### Safe defaults

- **Timestamp tolerance**: `300` seconds (5 minutes) — matches Stripe, Svix, Slack de facto standard
- **Replay cache**: Enabled by default if a cache is registered; skipped if none exists (receiver must handle duplicates)
- **Signature algorithm**: HMAC-SHA256 only (no alternatives for now)
- **Message ID field**: Provider-specific mapping (`webhook-id` → Standard, `X-GitHub-Delivery` → GitHub, `X-Slack-Request-Timestamp` → Slack as dedup key, etc.)
- **Error responses**: `401` for signature mismatch or timestamp out of window; `409` for replay (message ID seen before within cache TTL)
- **Logging**: Never log the raw signature or secret; log only `verified=true/false`, `provider`, `message_id`, `error_code`

### Dependencies

- **varco_core**: Uses only `stdlib` `hmac`, `hashlib`, `base64`, `time` — zero new runtime dependency
- **Replay cache backends** (per-backend):
  - `InMemoryWebhookReplayCache` — stdlib only
  - `RedisWebhookReplayCache` — varco_redis (already a sibling, no new dep)
  - `SAWebhookReplayCache` — varco_sa (already a sibling, no new dep)
- **Provider integrations**: No external provider SDKs needed; all verifiers are hand-written using `hmac`

## Version/compatibility notes

- **Spec versions**: Standard Webhooks v1.0 (normative, 2024–2026); Stripe webhook signatures stable since ~2018 (current: `t=` + `v1=` scheme); GitHub `X-Hub-Signature-256` stable since 2022; Slack `v0:` scheme stable since ~2018; Svix aligned with Standard Webhooks since co-creation (2023–2024).
- **Provider API stability**: All three headers (`webhook-id`, `webhook-timestamp`, `webhook-signature`) and the signed-payload format are considered stable by their respective providers and will not change without major notice.
- **Python HMAC**: `hmac.compare_digest()` available in Python 3.3+; varco targets 3.12+ so this is safe.
- **ASGI / Starlette**: Raw body buffering via `receive()` wrapper is stable; Starlette's ASGI contract has not changed since v0.15 (2020).

## Evidence gaps

1. **Standard Webhooks reference implementation testing** — The official `standardwebhooks` PyPI package (https://pypi.org/project/standardwebhooks/) is available but its exact version, changelog, and test coverage are not reviewed here; a production implementation should audit that library separately or compare against the spec text directly.

2. **GitHub delivery retry window** — GitHub's documentation does not explicitly state how long it retries a failed webhook delivery (only that it uses exponential backoff). The cache TTL for `X-GitHub-Delivery` dedup should cover this, but the optimal TTL is not documented; industry norm is 24–48 hours.

3. **Slack request-timestamp clock skew tolerance** — Slack states "5 minutes" but does not document how it handles fractional seconds or leap seconds in the Unix timestamp; no explicit guidance on clock-skew edge cases at subsecond precision.

4. **ASGI body buffering memory ceiling** — The `receive()` wrapper pattern has no built-in size limit; a receiver accepting large webhook bodies (video uploads, file attachments) could exhaust memory if not paired with the body-limit middleware. varco's design pairs them (BodyLimitMiddleware before signature verification), but the interaction is not formally specified in any Starlette/ASGI RFC.

5. **Timing-attack exploitation difficulty** — While the OWASP cheat sheet and CVE examples confirm timing attacks are real, the practical difficulty of exploiting them across a network (vs. locally on a server) is not quantified here; the recommendation to use `hmac.compare_digest()` is precautionary and widely adopted, but no concrete attack scenario is provided.

6. **Provider-specific secret rotation guidance** — None of the providers document a recommended cadence or mechanism for rotating signing secrets without downtime. The Standard Webhooks spec supports multiple space-delimited signatures (key rotation), but Stripe's documented rotation pattern is minimal. Worth a separate brief: "Webhook secret rotation strategies."

## Librarian's note

The evidence favours **a generic `WebhookVerifier` ABC with provider-specific adapters**, not a monolithic multi-provider verifier. Standard Webhooks, Stripe, Slack, and Svix all use HMAC-SHA256 but differ in signed-payload construction, timestamp handling, and replay ID location — a single if/else verifier would be fragile and hard to test. Middleware for raw-body capture + route-level dependency for verification is the standard 2025–2026 pattern in Starlette/FastAPI; ASGI-level `receive()` wrapping is more complex than `BaseHTTPMiddleware` but is the recommended approach when middleware must not modify the body. Replay protection via message ID cache + 5-minute timestamp tolerance is uniform across all three major platforms (Stripe, Svix, Slack), so safe defaults are well-established. Zero new runtime dependencies are required (stdlib `hmac`/`hashlib`/`base64` suffice), aligning with varco's policy.

---

### Sources
- [Standard Webhooks specification](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md)
- [Stripe webhook documentation](https://docs.stripe.com/webhooks)
- [Stripe webhook signature reference](https://docs.stripe.com/webhooks/signature)
- [GitHub webhook security guide](https://docs.github.com/webhooks-and-events/webhooks/securing-your-webhooks)
- [Svix webhook verification](https://docs.svix.com/receiving/verifying-payloads/why)
- [Svix Standard Webhooks announcement](https://www.svix.com/blog/standard-webhooks/)
- [Slack webhook verification docs](https://docs.slack.dev/authentication/verifying-requests-from-slack/)
- [OWASP Webhook Security Guidelines Cheat Sheet](https://github.com/OWASP/CheatSheetSeries/blob/master/cheatsheets_draft/Webhook_Security_Guidelines_Cheat_Sheet.md)
- [Starlette ASGI request handling](https://starlette.dev/requests/)
- [Starlette middleware documentation](https://starlette.dev/middleware/)
- [Starlette middleware discussion](https://github.com/encode/starlette/discussions/1729)
- [StandardWebhooks Python library](https://github.com/standard-webhooks/standard-webhooks/tree/main/libraries/python)
- [webhooks.fyi replay prevention](https://webhooks.fyi/security/replay-prevention)
- [Svix replay attack glossary](https://www.svix.com/resources/glossary/replay-attack/)
- [Svix webhook secret glossary](https://www.svix.com/resources/glossary/webhook-secret/)
- [Snyk webhook timing attack CVE](https://security.snyk.io/vuln/SNYK-GOLANG-GITHUBCOMADNANHWEBHOOKHOOK-537826)
- [Standard Webhooks adoption list](https://github.com/standard-webhooks/standard-webhooks?tab=readme-ov-file)
