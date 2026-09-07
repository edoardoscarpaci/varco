# Authorization-decision audit — `AuditingAuthorizer` (S11)

Plan 036 (3.2 security release, BACKLOG row **S11**, 🟡 should). Research brief backing this
feature: `design/research/006-multi-tenant-identity-and-hardening.md` §3 — CVE-2025-55241
(Microsoft Entra actor tokens): impersonation being *unlogged*, not merely being permitted, was
the incident's turning point.

## Why a decorator, not a middleware

`AbstractAuthorizer.authorize(ctx, action, resource)` is called from the **service layer**, at
eleven verified sites (`varco_core/varco_core/service/base.py:782,815,877,989,1047,1095,1176,1272`,
`service/bulk.py:163,252`, `service/soft_delete.py:256,309`) — **never** from HTTP middleware. An
HTTP middleware wrapping authorization would silently miss every authorization decision made by a
job runner, an event consumer, or a CLI verb. `AuditingAuthorizer` wraps whatever
`AbstractAuthorizer` an app has bound instead — one object, all eleven call sites, every non-HTTP
caller included, with no `AbstractAuthorizer` signature change (so no out-of-tree authorizer
breaks).

## Wiring — `enable_authorization_audit()`, called last

```python
from varco_core import enable_authorization_audit

container = DIContainer()
container.scan("myapp", recursive=True)     # binds the app's real AbstractAuthorizer
enable_authorization_audit(container)        # call LAST
```

`enable_authorization_audit(container, *, policy=AuditDecisionPolicy.DENIALS)` lives in
`varco_core.auth.di` (§D-036-oq2 — resolved at implementation time: it wraps `AbstractAuthorizer`
and injects `AbstractEventProducer`, both `varco_core` interfaces with zero HTTP dependency, so it
follows the same precedent as `varco_casbin.di.enable_policy_authorizer` — the `enable_*` verb
lives in the package that owns the implementation being wired). It resolves the **currently
bound** `AbstractAuthorizer`, wraps it in an `AuditingAuthorizer`, and re-binds the wrapper —
⚠️ **order-sensitive**: calling it before the app registers its own authorizer wraps
`BaseAuthorizer` and is then shadowed by the app's later, real binding, so the wrapper never runs.
Calling it twice raises `RuntimeError` — it detects an already-`AuditingAuthorizer` delegate and
refuses rather than silently double-wrapping (which would double-record every decision).

## What is recorded, and what is deliberately excluded (§D-S11-payload)

The audit payload is written to durable storage a broad operator role can read — the same class
of surface CLAUDE.md's `error_params()` rule governs (`ServiceAuthorizationError` deliberately
excludes `reason`; an override must apply the same scrutiny, never `vars(exc)`).

**Recorded:**

| Field | Source | Why it is safe |
|---|---|---|
| `principal_id` | `ctx.user_id` | The subject already identified in every other audit row |
| `actor_id` | `ctx.metadata["actor"]` | brief 006 §3: every `act` delegation must be logged with both principal and actor |
| `tenant_id` | `current_tenant()` | The existing audit trail's own scoping |
| `action` | `str(action)` | An enum-shaped verb, no payload |
| `entity_type` | `resource.entity_type.__name__` | A class name, not an instance |
| `entity_pk` | `resource.entity.pk` when not a collection, else `None` | An identifier the caller already supplied |
| `is_collection` | `resource.is_collection` | Structural |
| `decision` | `"allow"` / `"deny"` | The point of the row |
| `denial_type` | `type(exc).__name__` on denial | A class name |
| `correlation_id` | ambient, as every other audit row | |

**Deliberately excluded, each for a stated reason:**

- **The denial `reason` / `str(exc)`** — the exact field `ServiceAuthorizationError` already
  excludes. Recording it here would reintroduce, into durable storage, the leak
  `ServiceAuthorizationError` and 035's S3 both close.
- **`resource.entity` itself** — may carry encrypted or personal fields; the audit trail is not
  the place to duplicate them, and `AuditEntry` already has a modelled place for entity diffs
  this event deliberately does not reuse.
- **`ctx.grants`, `ctx.roles`, and the raw token** — a full permission dump per decision, in
  storage, is a map of the authorization system for anyone who can read the audit table.
- **`ctx.metadata` wholesale** — an app-controlled dict of unknown contents; only the single
  `actor` key is read, by name.
- **⛔ `vars(exc)`, in any form.**

The recorded shape is `AuthorizationDecisionEvent(payload=...)` (`varco_core.auth.audit`) — a
distinct `Event` subclass on channel `"varco.audit"` (the same channel `AuditEvent` uses), carrying
one generic `payload: dict[str, Any]` field rather than reusing `AuditEntry`'s typed shape
(§D-036-oq1). `AuditEntry`'s columns are shaped around a *mutation* — `diff`, hash-chain fields —
an authorization decision has no diff and needs `decision`/`denial_type`/`is_collection`, none of
which fit without widening `AuditEntry` for every existing mutation-audit consumer too. No new
`AuditRepository`/`AuditConsumer` ABC method was added; persisting the event is application wiring
— subscribe your own consumer to `"varco.audit"`, exactly as for any other event.

## `AuditDecisionPolicy` — denials always; allows only when delegated, or when asked (§D-S11-policy)

An authorization decision happens on every service call, several times per request — an
unconditional synchronous audit write would be the single largest cost in the release.

| Policy | Records | When to use |
|---|---|---|
| `DENIALS` (default) | Every deny, **plus** every allow whose `ctx` carries an actor (`ctx.metadata["actor"]`) | The default — captures the security signal (every denial, every delegated/impersonated allow — the exact CVE-2025-55241 property) at a fraction of `ALL`'s write volume |
| `ALL` | Every decision, allow or deny | A regulated deployment that must evidence every access |
| `NONE` | Nothing, but the wrapper stays installed | Flip the policy later with no DI re-wiring |

**Why `DENIALS` plus delegated allows, not all-or-nothing**: the delegated-allow carve-out is what
makes `DENIALS` safe as a default. Brief 006 §3's lesson is not "log everything" — it is that
impersonation must be logged at issuance *and* at use, and silent actor tokens are the critical
risk. An `act`-carrying token is rare, so recording 100% of those costs almost nothing and
preserves exactly the property the CVE turned on. A pure-`DENIALS` deployment cannot answer "who
read this record" from the authz trail alone — that gap is accepted and documented: `AuditLogMixin`
is the answer for mutations, `ALL` is the answer for reads.

## No conformance suite (§D-S11-conformance)

`AuditingAuthorizer` implements `AbstractAuthorizer`, which is not one of the eight ABCs
`testkit/varco_conformance` covers — a single-method, app-supplied policy hook whose contract
("raise on denial, deny by default") is a business-logic property, not an I/O-adapter property.
No new suite was added; the contract is asserted directly in `varco_core/tests/` against both
`BaseAuthorizer` and `AuditingAuthorizer`. See `testkit/varco_conformance/COVERAGE.md`'s "No
conformance suite (Plan 036, §D-S11-conformance)" row for the full written verdict.

## Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| Calling `enable_authorization_audit()` before the app's own authorizer is bound | It resolves the **currently bound** `AbstractAuthorizer` — an earlier call wraps `BaseAuthorizer` and is then shadowed by the app's real, later binding, so the wrapper silently never runs | Call it **last**, after every other DI wiring that could register an `AbstractAuthorizer` |
| Assuming every allow/deny lands durably | `AuditingAuthorizer` produces onto `AbstractEventProducer` — it inherits the bus's own delivery semantics; a dropped event is a missing audit row | Use the outbox pattern (`varco_core.service.outbox`) for a deployment that cannot tolerate a missing row, the same documented answer as every other event-shaped audit trail — this is a Pitfalls row, not a new mechanism |
| Assuming `AuditDecisionPolicy.NONE` removes the wrapper | `NONE` still wraps — `_record()` is simply never called | If the goal is truly zero overhead, don't call `enable_authorization_audit()` at all; `NONE` exists so a *bound* policy can be flipped without re-wiring DI |
