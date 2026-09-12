# Outbound webhooks

See also: [inbound-webhooks.md](inbound-webhooks.md) (the receiving half — signature
verification of webhooks sent *to* a varco app).

Plan 031 (D4) — a subscription registry, signing, SSRF-hardened delivery, retry into the
existing DLQ, and an admin surface for replay/rotation/disablement. Assembled entirely from
parts varco already ships (`RetryPolicy`, `AbstractDeadLetterQueue`, `DlqRedriver`,
`FieldEncryptor`) — no new reliability primitive, no new crypto path.

## Module map

| Concern | Module |
|---|---|
| Entity + repository ABC + in-memory default | `varco_core.webhook.models` / `.base` |
| Settings | `varco_core.webhook.settings.WebhookSettings` (`VARCO_WEBHOOK_` prefix) |
| Signing | `varco_core.webhook.signing` |
| SSRF guard | `varco_core.webhook.ssrf` |
| HTTP send path | `varco_core.webhook.transport` |
| Dispatcher | `varco_core.webhook.dispatcher.WebhookDispatcher` |
| SQLAlchemy repository | `varco_sa.webhook.SAWebhookSubscriptionRepository` |
| Beanie repository | `varco_beanie.webhook.BeanieWebhookSubscriptionRepository` |
| Admin mount | `varco_fastapi.webhook.mount_webhook_admin` |

## Signing: Standard Webhooks by default, RFC 9421 opt-in

A signature is only worth what the *receiver* can verify. Standard Webhooks (`webhook-id`/
`webhook-timestamp`/`webhook-signature`, HMAC-SHA256 over `{id}.{timestamp}.{payload}`) is
what off-the-shelf verification snippets in the wild actually understand, and its
space-delimited multi-signature header gives zero-downtime secret rotation natively — every
active secret signs, and a receiver accepts any. RFC 9421 ("HTTP Message Signatures", plus RFC
9530 `Content-Digest`) is available as `Rfc9421Signer` for consumers who require a
standards-track scheme, but is not the default: shipping it alone would leave a naive receiver
unable to verify at all.

Both signers reject a timestamp outside a **300 second** tolerance window by default —
Stripe's observed convention, not a value either spec normalizes. `WebhookDispatcher` threads
`WebhookSettings.signature_tolerance_seconds` (env `VARCO_WEBHOOK_SIGNATURE_TOLERANCE_SECONDS`)
into every signer it constructs, so widening or narrowing the window is a configuration change,
not a code change. Verification
always goes through `hmac.compare_digest` — never `==` — to avoid a timing side-channel.

`WebhookSubscription.active_secrets` supports multiple entries (newest last) so a secret can be
rotated without a delivery gap: add the new secret, wait for consumers to switch, then prune the
old one via the admin surface's rotate-secret route.

## SSRF model — resolve, validate, pin

A webhook target is user-supplied input the server then fetches by design — every delivery goes
through `varco_core.webhook.ssrf.validate_target()`, which is layered and fails closed:

1. **Scheme allowlist** — `https` only, unless `WebhookSettings.allow_insecure_http` is `True`
   (env `VARCO_WEBHOOK_ALLOW_INSECURE_HTTP`), which `WebhookDispatcher` forwards to
   `validate_target()`. This is deployment-wide by construction — never make it
   per-tenant/per-subscription, or a tenant can downgrade its own delivery to plaintext.
2. **Resolve → validate → pin** — the hostname is resolved once; every resolved address is
   checked; the connection uses the *first* resolved address, never a later re-resolution. This
   is what defeats DNS rebinding — validating one address and letting the HTTP client
   re-resolve to another (a different address) at connect time is exactly the bug this closes.
3. **Blocked by default** — private/loopback/link-local/multicast/unspecified/reserved ranges,
   both IPv4 and IPv6, including the IPv4-mapped bypass form (`::ffff:169.254.169.254`). An
   optional exclusive allowlist (`WebhookSettings.allow_list`, forwarded to
   `validate_target(allow_list=...)`) is available for deployments that can enumerate their
   consumers, alongside `WebhookSettings.extra_deny_ranges` for additional blocked CIDRs.
4. **No redirect following** — a 3xx is a delivery failure, not a hop; following it would
   re-open every check above at a URL validation never saw.
5. IPv6 equivalents are covered explicitly (`::1`, `fc00::/7`, `fe80::/10`).

⚠️ **ASSUMPTION**: connecting to the pinned IP while preserving the original `Host` header and
TLS SNI is implemented via httpx's `extensions={"sni_hostname": ...}` request extension. This
has not been verified against a real TLS-terminating receiver in this plan's test suite (only
the SSRF-guard and the no-hard-dependency structural tests are automated) — if a deployment
finds this insufficient, the documented fallback is a custom `httpx.AsyncHTTPTransport`, never
dropping the pin.

## Delivery and retry

`WebhookDispatcher` is an `EventConsumer` — it never holds `AbstractEventBus` directly. It
listens for `WebhookTriggerEvent` (a generic envelope an application republishes its own domain
events as — see the class docstring) and, for every `ACTIVE` subscription whose
`event_patterns` match, runs its own per-subscription retry loop using the existing
`varco_core.resilience.RetryPolicy` (exponential backoff + jitter — the same primitive `@listen`
itself uses elsewhere).

The generic `@listen(retry_policy=..., dlq=...)` wrapper is deliberately **not** used here: it
retries/DLQs one handler invocation as a whole, and one event can match many subscriptions, each
of which must retry, DLQ, and auto-disable independently. `WebhookDispatcher.register_to()` is
overridden to capture `dlq=` as instance state instead, without forwarding it to the generic
wrapper.

- **Timeout**: 10 seconds per attempt, passed as `WebhookDispatcher(request_timeout_seconds=...)`
  — the default matches `WebhookSettings.request_timeout_seconds`'s documented default (Stripe's
  convention) but, like the SSRF knobs above, nothing shipped today reads a `WebhookSettings`
  instance to populate this constructor argument automatically; a caller wires the value itself.
- **Exhaustion → DLQ**: pushed via the existing `AbstractDeadLetterQueue` — `push()` never
  raises, same contract `OutboxRelay`/`JobRunner` honour. This is what makes replay free:
  `DlqRedriver` already exists and is wired into the admin surface's replay route.
- **Auto-disable**: after `disable_after_failures` (default 20) *consecutive* failures across
  distinct events, the subscription flips to `DISABLED` and is skipped entirely on future
  triggers. Re-enable is an explicit admin action (never automatic).
- **At-least-once, documented.** Every delivery carries a stable `webhook-id` so the receiver
  can deduplicate. If the receiver is itself a varco app, point it at plan 029's
  `Idempotency-Key` middleware (`varco_core.idempotency` + `varco_fastapi.middleware.idempotency`)
  as the recommended dedup mechanism — this is advice, not an import; the two features do not
  depend on each other.

⚠️ The constructor's *default* `RetryPolicy` (`max_attempts=8`, small `base_delay`) matches
Svix's documented 8-attempt *shape* but not its real seconds-scale schedule (immediate, 5s, 5m,
30m, 2h, 5h, 10h, 10h) — a literal hours-long default would make every unit test glacial.
Production deployments should construct `WebhookDispatcher` with an explicit `retry_policy=`
sized in real seconds.

## Secrets at rest

`active_secrets` are encrypted via the existing `FieldEncryptor`/`EncryptionKeyManager` when a
repository is constructed with `encryptor=` — no new crypto path. ⚠️ `encryptor=None` (the
default on both `SAWebhookSubscriptionRepository` and `BeanieWebhookSubscriptionRepository`)
stores secrets in plaintext — a documented dev/test-only escape hatch, never the intended
production configuration. The admin surface never returns `active_secrets` on a read — only the
create and rotate-secret responses reveal a secret, and only once.

## Admin surface

`varco_fastapi.webhook.mount_webhook_admin(app, *, repository=, redriver=None,
acknowledge_bundled_admin=True, server_auth=..., admin_role="webhook-admin",
prefix="/webhooks")` — same gated shape as `mount_reliability_admin`/`mount_tenant_admin`
(RD-9): raises `ValueError` unless `acknowledge_bundled_admin=True`, and there is deliberately
**no** env var and **no** `create_varco_app()` kwarg for it.

Routes: CRUD on `{prefix}/subscriptions`, `PATCH .../disable` / `.../enable`, `POST
.../rotate-secret`, and — only when `redriver=` is given — `POST
{prefix}/deliveries/{entry_id}/replay` through the existing `DlqRedriver`. Listing is scoped by
the caller's `X-Tenant-Id` header; a request with no tenant header returns an empty list rather
than leaking every tenant's subscriptions.

## Consumer-side idempotency

Because delivery is at-least-once, a receiver should deduplicate on `webhook-id`. If the
receiver is a varco app, plan 029's `Idempotency-Key` middleware is the recommended mechanism —
see `technical_docs/features/idempotency-key.md`. This is guidance, not a dependency between the
two features.

## Pitfalls

| Pitfall | Why | Fix |
|---|---|---|
| Constructing `WebhookDispatcher()` on the shipped retry defaults in production | The defaults are fast-but-Svix-*shaped* (8 attempts, sub-second delays), not Svix's real seconds schedule — see "Delivery and retry" above | Raise `VARCO_WEBHOOK_RETRY_BASE_DELAY_SECONDS`/`_RETRY_MAX_DELAY_SECONDS` to real seconds, or pass an explicit `RetryPolicy` |
| Expecting `active_secrets` to be encrypted with no extra setup | `encryptor=None` is the repository default | Pass a real `FieldEncryptor` to `SAWebhookSubscriptionRepository`/`BeanieWebhookSubscriptionRepository` |
| Passing both `settings=` and an explicit `retry_policy=`/`request_timeout_seconds=`/`disable_after_failures=` and expecting the settings value | An explicit constructor keyword is an **override** — it always wins over the settings field | Set the value in one place: the settings object for a deployment-wide value, the keyword for a one-off instance |
| Constructing `WebhookDispatcher` in a test and being surprised by ambient config | `settings=None` constructs `WebhookSettings()`, which reads `VARCO_WEBHOOK_*` from the process environment | Pass an explicit `WebhookSettings(...)` in any test that must not see the ambient environment |
| Assuming a per-subscription `allow_insecure_http` override exists | Deployment-wide only, by design — a tenant must never downgrade its own delivery to plaintext | Set `VARCO_WEBHOOK_ALLOW_INSECURE_HTTP` (or the settings field) for the whole deployment, never per-subscription |
| Expecting a redirected webhook target to be followed | §D-D4-ssrf layer 4 treats any 3xx as a delivery failure | Point the subscription at the final URL directly |
| Calling `mount_webhook_admin()` from `create_varco_app()` or via an env var | Neither exists, deliberately (RD-9) | Call `mount_webhook_admin()` explicitly after app construction |
