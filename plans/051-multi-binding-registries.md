# Plan 051 — Multi-binding for "all implementations of X" (DI-5)

Covers BACKLOG 4.0 row **`DI-5`** (`BACKLOG.md:95`): *"Multi-binding for 'all implementations of
X' — providify `Multibound`/`InjectInstances` (immutable, ordered) for `RetentionTarget`s, posture
inspectors, `TenantSource`s, `WebhookVerifier`s, and `EventConsumer` auto-`register_to()`; the five
hand-rolled registries go"*. Evidence: scout §4 (*"every multi-implementation case is a manual list
or `add()` call; a plugin cannot contribute without editing framework code"*), research brief 014
§1 (Quarkus `@All`, Spring `List<T>`+`@Order`, .NET `IEnumerable<T>`) and §Librarian #2
(immutable collections).

Slice map row 051 (`plans/000-index-4-0-di-standardization.md`). Depends on **045** (`@Fallback`
on every framework default; `get_all()` excludes a shadowed fallback — the property every
collection here relies on; the explicit decision that `HealthCheck` contributions are plain
`@Singleton`, *never* `@Fallback`, "or they vanish from `get_all()`"), **046** (`bind_instance`,
the container-required `bootstrap()` contract, the `di_wiring` conformance suite this plan
extends), **049** (`VarcoModule`/`ModuleRegistry`; `CoreModule.start()` owns the bus;
`SecurityPostureModule._default_collectors()` is "the seam 051 removes"; `RetentionModule(registry,
interval)`), **057** (adds `_collect_redaction` to the collector list "the 3.2 way" — this plan
migrates it). Neighbours: **050** owns phased lifecycle and cross-module start/stop ordering (bus
before consumers, consumers stop before bus) — this plan owns only *which* consumers, discovered
through multi-binding, inside `CoreModule`'s own body; **052** (`varco diagnose`) reads
`collection_seams()` and reports what is multi-bound; **058** assembles §Migration into the guide.
All four predecessors are treated as landed and referenced by number.

**providify source of record**: `/home/edoardo/projects/providify` at **2.1.0** (`pyproject.toml`
`version = "2.1.0"`, CHANGELOG `[2.1.0] — 2026-09-13`, `CHANGELOG.md:12`). Multibinding itself
shipped in **2.0.0** (`CHANGELOG.md:620-648`, "F7"), so nothing here waits on upstream. Every
`providify/…` citation below was read in that tree; Step 1 re-confirms each against the installed
wheel (045 Step 1(c) moved the pin).

> ⚠️ **Eight premises in the task brief / BACKLOG row are corrected here from source, and each
> reshapes a §D block.**
> (1) providify orders a collection **ascending** by priority, not descending — `get_all()`
> `sorted(candidates, key=lambda b: b.priority)` at `container.py:1704-1707`, `_collect_sync`
> `:1793-1796`, `_collect_async` `:1815-1818`, `aget_all` `:1897-1900`; candidates come from
> `_filter()` iterating `self._bindings` (`:2129-2131`), which is a plain list appended in
> registration order (`:868`, `:1213`, `:1226`, `:1248`, `:1361`); Python's sort is stable, so
> **ties keep registration order**. Because `priority` also picks the *single* `get()` winner
> (highest, `_get_best_candidate` `max()`), the same number means "last in the list" for
> `list[T]` and "the winner" for `get(T)` — so varco never uses providify `priority` as a
> collection order key (§D-order). (2) **`InjectInstances[T]` raises `LookupError` on zero
> contributions** (`type.py:200` expands to `InjectMeta(all=True)`; `container.py:4063-4069` →
> `self.get_all(inner)` → `:1689-1690` raises). Only a **declared collection point** —
> `@Multibound` on the type or `container.multibind(T)` — with a bare `list[T]` hint resolves to
> `[]` (`:1626-1631` → `_collect_sync` `:1752-1796`; providify `CHANGELOG.md:635-639` calls this
> difference deliberate). varco standardises on `@Multibound` + `list[T]`, never `InjectInstances`
> (§D-shape). `validate()` never reports a missing collection either way (`:6549-6557`), so "an
> empty required collection" is varco's own check (§D-empty). (3) The delivered collection is a
> **fresh mutable `list`** per resolution (a list comprehension, `:1793-1796`), never a tuple —
> immutability (brief 014 §Librarian #2) is varco's to add, at the consumer (§D-immutable).
> (4) `TenantSourceChain` **order is not the trust ranking**. `TenantTrust` is a `ClassVar` on each
> source (`tenancy/source.py:139-140`); the winner is `max(claims, key=trust)` (`:275`); chain order
> only breaks equal-trust ties and fixes which conflicting pair is reported (`:190-191`,
> `:252-254`). The security property is the **allowlist** — `VARCO_TENANT_SOURCES` names which
> sources feed identity at all (`settings.py:230`, `:317-322`) — not the order (§D-tenant).
> (5) BACKLOG says "`RetentionTarget`s", but the registry holds **`RetentionPolicy`** objects
> (`retention/policy.py:244-297`); a target alone has no cron, no `dry_run`, no name. The
> contribution unit is the policy (§D-retention). (6) `WebhookVerifier` instances are **per-route
> objects constructed with that route's secrets** (`get_verifier(provider, *, secrets, …)`,
> `webhook/inbound/verifiers.py:544-593`) — a name→class dispatch, not "all implementations of X";
> multi-binding is the wrong shape there (§D-verifier ❌). (7) 046's `bind_instance()` raises
> `AlreadyBoundError` on a second non-fallback binding of the same `(interface, qualifier)`
> (046 §D-bind) — by design, so it **cannot** be the contribution verb; this plan adds
> `contribute()` (§D-verbs). (8) There is a **sixth** hand-rolled registry the row does not list:
> `varco_core/asyncapi/generator.py:247-257` walks `get_all_bindings(EventConsumer)` by hand and
> reads the private `_registered_buses` (`:260-274`) — folded into §D-consumer.

## Goal

Every "all implementations of X" seam in varco is a `@Multibound` type with one documented
convention — contributors are plain, non-`@Fallback` bindings of the seam; consumers read a
`tuple` via `varco_core.di.collect()`/`acollect()` (or a `list[T]` constructor hint); providify
orders ascending-priority-then-registration and varco never uses `priority` as a collection order
key. Five hand-rolled registries are gone (`RetentionRegistry.register()` + `bind_retention_registry`;
`SecurityPostureModule._default_collectors()` and 057's `partial(...)` list; the
`_LEGAL_TENANT_SOURCES` closed allowlist; the asyncapi generator's hand walk; the
`@PostConstruct`-wired `EventConsumer` convention). A third-party package contributes a retention
policy, a tenant source, a posture collector, or an event consumer with `container.scan()` + a
plain `@Singleton`/`@Provider` and zero framework edits — proven by an in-tree example package and
its tests. `WebhookVerifier` gets an explicit registration verb instead (the profiling-backend
precedent), with the ✅/❌ recorded.

## Non-goals

- **Any change to providify.** 2.1.0 is consumed as shipped; no new upstream ask is expected
  (§D-shape's zero-tolerant terminal exists). If Step 1 finds a gap it is filed per CLAUDE.md's
  rule, never worked around.
- **Cross-module start/stop phases** (bus before consumers across modules, consumers stop before
  bus, `@Disposes` reachability) — Plan 050. This plan sequences only *inside* `CoreModule.start()`
  / `stop()` (bus, then discovered consumers; reverse on stop), which 049 already made
  `CoreModule`'s own body.
- **DI-wiring `WebhookDispatcher`/`OutboxRelay`/`SADeadLetterQueue` etc.** (DI-10) — Plan 050.
  Once 050 binds `WebhookDispatcher` as a `@Singleton`, it is discovered by §D-consumer with no
  further change; 050 decides whether it opts out (`auto_register = False`).
- **`varco diagnose` rendering** of collection seams — Plan 052 reads `collection_seams()` and
  `collect()`; this plan gives it the API, no CLI.
- **Making the tenant chain the middleware default / `chain=None` → `TypeError`** (FLIP-6) —
  Plan 055. This plan ships `resolve_tenant_source_chain(container)`; 055 makes the middleware use
  it.
- **RO-2** (wiring `inspect_jwks_posture()`/`inspect_retention_posture()` as posture collectors) —
  Plan 052 per the index; after this plan each is a ~15-line `PostureCollector` subclass
  contributed by `JwksRefreshModule`/`RetentionModule` (handoff noted in Step 20).
- **A live RLS posture collector from `varco_sa`** — Plan 056's call; §D-posture makes it
  possible with no `varco_fastapi` edit (that is the in-tree proof of the plugin story).
- **`get_signer()`** (`webhook/signing.py`) — same shape as `get_verifier()`, not in DI-5's row;
  §D-verifier's verb is written so 058 can mirror it if asked.
- **Renaming any posture `check` id, `TenantTrust` value, or `RetentionPolicy` field.**

## Context

### What providify 2.1.0 ships for multi-binding (verified in the 2.1.0 tree)

| Primitive | Where | Semantic this plan relies on |
|---|---|---|
| `@Multibound` | `providify/decorator/multibinding.py:26-58`; exported `providify/__init__.py:121,152` | Stamps `__di_multibound__ = MultiboundMarker()` on the class (`:57`); "the marker travels with the type across modules, and works for types discovered by `container.scan()`" (`:10-12`). `_is_multibound` uses `getattr`, so **subclasses inherit the marker** (`:68-72`) — `list[JwtClaimTenantSource]` is also a collection point; harmless |
| `container.multibind(cls)` | `container.py:1364-1405` | The imperative twin; idempotent `set.add`; `qualifier=` is reserved and **not consulted** (`:1387-1391`, `:1402`) |
| `list[T]` resolution | `container.py:1620-1631` (`get`), `:1847-1854` (`aget`), `_multibound_inner` `:1727-1750` | A bare `list[T]` hint collects iff `T` is a declared collection point **and no literal `list[T]` binding exists** (rule 1 wins, `:1620-1627`); `validate_bindings()` warns when both exist (`:5902-5926`) |
| `_collect_sync` / `_collect_async` | `container.py:1752-1796` / `:1798-1818` | The zero-tolerant terminal: `[]` on no contributions (`:1756-1763`); async providers awaited by the async form only (`:1782-1788` raises for the sync one — the reason `acollect()` exists) |
| Ordering | `:1793-1796`, `:1815-1818`, `:1704-1707`; `_filter` `:2129-2151`; `_bindings` list `:868` | Ascending priority, stable → ties in registration order (premise 1) |
| Shadowed `@Fallback` excluded | `_filter` post-step `:2152-2170` (`:2168-2169`); docstrings `:1683-1686`, `:1882-1885` | One change site covers `get_all`/`_collect_*`/`get_all_bindings`/`validate()` — a `@Fallback` contribution vanishes the moment a non-fallback sibling exists (045 Class C rule) |
| `@Requires`/`@Profile`/`@Alternative` inactive excluded | `_filter` `:2150` → `_binding_is_active` `:1904-…`; `@Requires` `scope.py:818-919` | A 047 `@Requires`-gated contribution is simply absent from the collection while its predicate is false — no varco code needed. ⚠️ `@Requires(condition=)` is a **zero-arg** predicate (`:820`, `:842`): it can read `os.environ`, never the container — so it cannot gate on a *code-bound* settings instance (§D-tenant rejects it) |
| Qualifier narrowing | `_filter` `:2147` | `get(list[T], qualifier=q)` / `container.get(list[T])`: an **unqualified** request matches qualified and unqualified contributions alike; a qualified request only its own |
| `exact_only` self-bindings excluded from sweeps | `:1225-1226`, `:2139-2146` | `bind(ABC, Impl)` adds a hidden `Impl→Impl` self-binding that **never** duplicates in `list[ABC]` |
| Scanned `@Singleton class Impl(ABC)` | `register()` `:1228-1248` → `ClassBinding(cls, cls)`; `_binding_serves` via `issubclass` `:2138` | A plugin's plain `@Singleton` subclass is collected under the ABC with no `bind(ABC, …)` call — the whole plugin story (§D-plugin) |
| `get_all_bindings()` | `container.py:7464-7496` | Pure read, registration order, `[]` on none; each binding exposes `.scope` (`binding.py:218`, `:741-745`), `.fallback` (`:207`, `:723`), `.priority`, `.qualifier` — the `di_wiring` suite's tools (§D-tests) |
| `validate()` and collection hints | `container.py:6548-6557`; `validation.py:463-477` | "Missing is never reported (`[]` is a legal answer); every candidate becomes a cycle edge" — for `InjectInstances[T]` **and** a `list[T]` collection point alike |
| `Instance[T].aget_all()` | `type.py:1056-1080` | Swallows `LookupError` → `[]`; the pre-existing `HealthRouter` shape (`router/health.py:108-112`), left as-is (§D-health) |
| `InjectInstances[T]` | `type.py:171-213`, `:246`; `container.py:4063-4069`, `:4211-4217` | `Annotated[list[T], InjectMeta(all=True)]` → `get_all()` → **raises on zero**. Not used by varco after this plan (§D-shape) |

### The six hand-rolled registries (measured 2026-09-17)

| # | Registry | Where | Shape today | Who can contribute today |
|---|---|---|---|---|
| R1 | `RetentionRegistry` + `bind_retention_registry` | `varco_core/varco_core/retention/policy.py:244-297` (`register()` with three guards: `older_than`↔`supports_older_than`, duplicate `name`), `retention/di.py:59-102` (the `_bind_call_count` priority hack 046 already deletes), consumers `retention/scheduler.py:321-324,356`, `retention/posture.py:82-117`, `cli/retention.py:120-154,218-234`, `varco_fastapi/retention.py:63-134` | App constructs the registry, calls `register()` N times, binds it | The app only — a plugin package has no hook |
| R2 | Posture collectors | `varco_fastapi/varco_fastapi/posture.py:199-255` (`_wrap_collector(name=)`), `:284-292` (`SecurityPostureLifecycle(collectors=Sequence[Callable[[], list[PostureFinding]]])`), the five `_collect_*` functions `:485-771`; 049 `SecurityPostureModule._default_collectors(app, container)` (049 §D-posture — "the seam 051 removes"); 057 adds `_collect_redaction` via `partial` | A hand list of zero-arg callables built in `varco_fastapi` | `varco_fastapi` only — a `varco_sa` RLS collector would have to import `varco_fastapi` (wrong direction) |
| R3 | `TenantSourceChain` sources | `varco_core/varco_core/tenancy/settings.py:230` (`_LEGAL_TENANT_SOURCES`), `:317-322` (unknown name → `ValueError`), `:371-434` (`build_tenant_source_chain`'s if/elif over four built-ins); `source.py:179-204` (chain: `sources: tuple`, `mode`, `min_trust`) | A closed name allowlist + a four-way if/elif | Nobody — an out-of-tree `TenantSource` cannot be named in `VARCO_TENANT_SOURCES` at all |
| R4 | `WebhookVerifier` providers | `varco_core/varco_core/webhook/inbound/verifiers.py:535-541` (`_PROVIDERS` dict), `:544-593` (`get_verifier`) | Module-level name→class dict | Nobody |
| R5 | `EventConsumer` wiring | `varco_core/varco_core/event/consumer.py:686-711` (the `_bus` convention), `:700-704` (`@PostConstruct` path), `:992-1043` (`start()` reads `self._bus`), `:825-844` (double-registration guard, `RuntimeWarning`); CLAUDE.md §Event system ("typically in a `@PostConstruct` method"), §Database auditing ("must be wired from `@PostConstruct`"), the Pre-Implementation checklist; 049 deleted `_collect_lifecycle_components` and made `CoreModule.start()` start the bus (049 §D-core) and the example's `PostAppModule.start()` call `consumer.start()` (049 §D-example) | Each consumer wires itself, at construction time (`@PostConstruct`, possibly before the bus starts) or via an app-module `start()` | The app, per consumer, by hand |
| R6 | asyncapi consumer discovery | `varco_core/varco_core/asyncapi/generator.py:242-257` (hand walk over `get_all_bindings(EventConsumer)` + `binding.fn` peek), `:260-274` (`_is_registered` reads private `_registered_buses`) | A hand-rolled `get_all` | n/a (a reader, not a registry — but it duplicates R5's discovery) |

**In-tree precedent**: `HealthCheck` (`varco_core/varco_core/health.py:152`) is already multi-bound
by convention — seven backend probes are plain `@Singleton(qualifier=…)` (045 Class C, floors
removed), `HealthRouter` reads `Instance[HealthCheck].aget_all()` (`router/health.py:89`, `:108-112`).
045 pinned "not `@Fallback`, all present in `get_all_bindings(HealthCheck)` after a user check is
bound". This plan stamps `@Multibound` on it for convention uniformity and leaves its reader alone
(§D-health).

**In-tree `EventConsumer` subclasses** (`rg`): `CacheInvalidationConsumer`, `WebhookDispatcher`,
`AuditConsumer`, `TenantReadinessCoordinator`, `TenantProvisionConsumer` — **none is a scanned
`@Singleton`**; `AuditConsumer` is hand-constructed and `register_to()`-ed by `varco_sa/audit.py:34,254`
/ `varco_beanie/audit.py:38,338`. So §D-consumer's auto-registration touches only *app* consumers
bound in the container (e.g. the example's `@Singleton PostEventConsumer`,
`examples/00-full-stack-post-api/example/consumer.py:86-129`) until 050 binds `WebhookDispatcher`.

## Design decisions

### §D-shape — one convention: `@Multibound` seam, plain contributors, `tuple` at the consumer

> **A collection seam is an ABC (or a frozen value type) stamped `@Multibound`. A contribution is
> any ordinary non-`@Fallback` binding of that type — a scanned `@Singleton` subclass, a
> `@Provider` returning it, or `contribute(container, Seam, obj)`. A consumer reads it as a bare
> `list[Seam]` constructor hint or via `varco_core.di.collect(container, Seam)` /
> `acollect()`, which return an immutable `tuple`. Zero contributions is a legal, empty tuple.
> providify orders ascending priority, ties in registration order; varco never sets `priority` on a
> contribution and never reads the order as meaning — a seam that needs order carries its own
> domain key (`TenantTrust`).**

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: `@Multibound` on the seam + bare `list[T]` / `collect()`** | The only zero-tolerant terminal (`container.py:1756-1763`; premise 2) — a posture module with no collectors, a retention module with no policies, a bus with no consumers all resolve to `()` and are reported by varco's own `validate()` (§D-empty), never by a `LookupError` inside a constructor. The marker travels with the type (`multibinding.py:10-12`), so a plugin's container that scanned its own package first still collects. `validate()` treats the hint correctly (`:6549-6557`). Type checkers see `list[T]` with no alias tricks | providify import in five seam modules (`event/consumer.py`, `retention/policy.py`, `tenancy/source.py`, `posture.py`, `health.py`) — `varco_core` already imports providify in 25+ modules (045 §Context E3 footnote), and `@Multibound` is a `setattr` with no registration (`scanner.py:151-162` reads `@Singleton`/`@Provider`/`@Configuration` metadata only — ⚠️ Step 1 confirms `container.scan("varco_core")` gains **no** binding from the marker, the standing `varco_core.tls`/`cloudevents`/`retention` scan rule) |
| (2) `InjectInstances[T]` (the BACKLOG wording) | providify's named form; `Annotated[list[T], InjectMeta(all=True)]` | Raises `LookupError` on zero (`:1689-1690` via `:4069`) — every consumer would need a try/except or a dummy contribution; `validate()` and runtime disagree about "empty" (premise 2). Rejected |
| (3) `Instance[T].aget_all()` everywhere (the `HealthRouter` shape) | Lazy, zero-tolerant (`type.py:1076-1080`) | A proxy object held by every consumer and a `.aget_all()` call at first use — resolution timing becomes "whenever the consumer first looks", which is the hidden-discovery shape 049 just deleted. Kept only where it already exists (§D-health) |
| (4) `container.multibind(T)` in each `bootstrap()` instead of the decorator | No providify import in seam modules | An app that scans a plugin package before calling varco's `bootstrap()` — or a test with a bare container — silently gets `_UNRESOLVED`/`LookupError` for `list[T]`; the marker on the type has no such ordering hazard. Rejected |

**Contributors are never `@Fallback`** — 045 §D-rule's collection corollary (`_filter` `:2168-2169`:
a fallback contribution disappears the moment any non-fallback sibling exists). This is now a
suite-enforced rule (§D-tests, `test_collection_seams_have_no_fallback_contributors`), not prose.
A *default set* (varco's six posture collectors) is therefore contributed as ordinary bindings and
replaced by *construction* (`SecurityPostureModule(collectors=…)`), never by shadowing.

### §D-order — providify `priority` is never a collection order key

`priority` means "the `get(T)` winner" (highest, first-registered on ties) **and** "sort key,
ascending" for `list[T]` — opposite directions for one number (premise 1). A contributor who writes
`@Singleton(priority=100)` to "run first" runs *last* and simultaneously becomes what `get(T)`
returns. Rule: **no varco contribution carries `priority=`** (the `di_wiring` suite asserts every
collection-seam binding is at providify's default `0`), and every consumer treats the tuple as a
*set with a stable, documented-as-incidental order*: "registration order — the order your
`bootstrap()`/`scan()` calls ran". The one seam whose order is semantic — the tenant chain — sorts
by its own `TenantTrust` and by the operator's `VARCO_TENANT_SOURCES` list, never by providify
(§D-tenant).

Rejected — **an `order: ClassVar[int]` on every seam ABC, Spring `@Order` style** (brief 014 §1):
✅ explicit; ❌ four of five seams have no ordering semantics at all (findings are a set; policies
are keyed by name; consumers are independent; verifiers are keyed by provider), so the attribute
would be a lie on four ABCs to serve one, which already has `trust`. Not built; 052/058 may
revisit if a second ordered seam appears.

### §D-immutable — the tuple is made at the consumer, once

providify hands back a fresh `list` per resolution (`:1793-1796`). `collect()`/`acollect()` return
`tuple(...)`; every consumer stores the tuple (`TenantSourceChain.sources` is already a `tuple`;
`RetentionRegistry` is constructed from an iterable and has no `register()`; `SecurityPostureLifecycle`
keeps `tuple(collectors)`; `CoreModule` keeps `tuple[EventConsumer, ...]`). A `list[T]`
*constructor hint* (the providify-native form, allowed for app code) delivers a list the owner should
`tuple()` in `__init__` — stated once in the guide. Dedup: providify has none (brief 014 §1: Spring,
.NET, Guice none either); `contribute()` refuses the **same object twice** (identity, §D-verbs) and
each seam's consumer enforces its own **name** uniqueness at startup (`RetentionRegistry` duplicate
policy name; the tenant chain builder's duplicate/colliding `name`; posture collectors' duplicate
`name` → one `posture.duplicate_collector` finding, never a crash) — "dedup at registration, not
resolution" (brief 014 §Librarian #2) in the only place varco controls, since providify registration
is open.

### §D-verbs — `contribute()`, `collect()`, `acollect()`, `collection_seams()` in `varco_core.di`

```python
# varco_core/varco_core/di/collect.py   (049 made varco_core.di a package; Step 1 confirms the layout)
def contribute(container, seam: type[T], instance: T, *, name: str | None = None) -> ProviderBinding:
    """Register an already-constructed contribution to a @Multibound seam. Non-fallback, singleton.
    Raises AlreadyBoundError if this exact object is already contributed to `seam` (identity)."""
def collect(container, seam: type[T]) -> tuple[T, ...]:        # tuple(container.get(list[seam]))
async def acollect(container, seam: type[T]) -> tuple[T, ...]: # tuple(await container.aget(list[seam]))
def collection_seams() -> tuple[type, ...]:                     # lazy imports inside the body — import budget
    # (EventConsumer, RetentionPolicy, TenantSource, PostureCollector, HealthCheck)
```

- `contribute()` ✅ says "one of many" where `bind_instance()` says "the one" (046 raises
  `AlreadyBoundError` on a second `(interface, qualifier)` — the correct rule for a singleton seam
  and the wrong one for a collection); same body shape as `bind_instance` (`Provider(singleton=True)`
  closure, `returns=seam`, `__qualname__` stamped `varco.di:contribute[Seam:name]` so `describe()`
  and 052 can name it); the closure carries `__varco_contributed__ = instance` so the identity check
  is a public `get_all_bindings()` read. ❌ One more verb beside `bind_instance`/`bootstrap`.
  Accepted — the two verbs have opposite double-call semantics and must not share a name.
- `collect()`/`acollect()` ✅ the only sanctioned read; `tuple` by construction; `acollect` is
  what a module uses in `async_init()`/`start()` so an `async def @Provider` contribution
  (a policy whose target awaits a connection) works — `_collect_sync` refuses async providers
  (`:1782-1788`). ❌ Two helpers over a one-line providify call. Accepted: the immutability rule
  lives in exactly one function each.
- `collection_seams()` ✅ the list 052 iterates and the `di_wiring` suite asserts against; a
  function, not a constant, so `varco_core.di` imports no seam module at import time (the
  `varco_core/__init__.py` PEP 562 budget — Step 21 runs `import_budget.py`).
- Rejected — **`bind_instance(container, seam, obj, qualifier=<unique>)` as the contribution
  verb**: ✅ zero new API; ❌ abuses `qualifier` (a filter key, `:2147`) as a uniqueness token, and
  `get(seam, qualifier="x")` would then resolve a lone contribution as if it were *the*
  implementation.

### §D-empty — an empty required collection is varco's finding, never providify's

providify's `validate()` says "missing is never reported" for every collection hint
(`:6549-6557`). So each module that *requires* at least one contribution reports it from its
049 `validate(container) -> Sequence[str]` slot (052 renders; 049 sequences it after
`async_init()`), by a **pure read** — `container.get_all_bindings(Seam)` — so `validate()`
instantiates nothing:

| Module | Empty means | `validate()` message |
|---|---|---|
| `RetentionModule` (enabled) | nothing will ever be purged — the module is on for nothing | `"RetentionModule is enabled but no RetentionPolicy is contributed — contribute(container, RetentionPolicy, …) or remove the module"` |
| `SecurityPostureModule` (enabled) | a preflight that checks nothing | `"SecurityPostureModule is enabled but no PostureCollector is contributed"` (cannot happen with the default set; it can with `collectors=()`) |
| `CoreModule` | no consumers — **legal**, silent | — (an API-only app has none) |
| tenant chain | `VARCO_TENANT_SOURCES` empty → no chain, unchanged (`settings.py:403-404`) | — ; a *named* source with no built-in and no bound contribution → `ValueError` at build (fail closed, §D-tenant) |

### §D-retention — `RetentionPolicy` is the seam; the registry becomes a derived, frozen object

**Chosen: `@Multibound` on `RetentionPolicy` (the frozen dataclass, `policy.py:82-83`; a class
`setattr`, legal on a frozen dataclass). Apps and plugins contribute policies; `RetentionModule()`
takes no registry and builds one in `async_init()` from `await acollect(container, RetentionPolicy)`.
`RetentionRegistry(policies: Iterable[RetentionPolicy])` runs the three `register()` guards over
the whole set at construction and is immutable afterwards; `register()` is removed;
`bind_retention_registry` and `retention/di.py` are deleted.**

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: policies are the contributions; registry derived** | A plugin contributes one `@Provider def audit_policy(repo: Inject[AuditRepository]) -> RetentionPolicy` in its own `@Configuration` — its target's repository is *injected*, which is exactly the DI-10 direction 050 wants for hand-constructed infra; the three guards still run before any sweep (in `async_init()`, i.e. startup — a `ValueError` aborts the lifespan, same timing as today's `register()` at wiring); `RetentionScheduler(registry, …)`, `inspect_retention_posture(registry)` and the CLI keep their `RetentionRegistry` argument unchanged; the `uuid5` `schedule_id_for()` convergence is untouched; **`RetentionPolicy.dry_run` stays required with no default** — a contribution that does not state its blast radius still cannot be constructed | The registry is built at `async_init()`, so `RetentionModule.validate()` can only *count* bindings (a pure read) — a duplicate-name or capability error surfaces as the `async_init()` exception, listed by 052 after the fact rather than predicted. Accepted: data-loss feature, hard failure wanted |
| (2) `@Multibound` on `RetentionTarget` (the BACKLOG wording) | Matches the row | A target has no schedule, no `dry_run`, no name — a bound target is not a purge intent; policies would still be hand-listed. Rejected (premise 5) |
| (3) keep `RetentionRegistry.register()` and make the registry itself the contribution (`list[RetentionRegistry]` merged) | No dataclass marker | Two registries with one name each cannot be cross-checked for duplicate names until merge; the registry is exactly the "manual `add()`" DI-5 names. Rejected |

CLI: `varco retention list/--policy --target mod:callable` (`cli/retention.py:77`) — the callable
may now return a `RetentionRegistry` **or** an iterable of `RetentionPolicy` (wrapped into a
registry); `varco_core.retention.__init__` docstring's import list drops `di`. `retention/__init__.py`'s
⛔ scan rule and `test_retention_no_di_side_effect.py` stay true (the marker is not a registration —
Step 1 measures; the test's `container.get(RetentionRegistry)` assertion is kept and a
`collect(container, RetentionPolicy) == ()` assertion added).

### §D-tenant — the pool is multi-bound; the allowlist and the order stay the operator's

**Chosen: `@Multibound` on `TenantSource`. `build_tenant_source_chain(settings, *,
delegation_policy=None, sources: Iterable[TenantSource] = ())` gains the pool argument and owns
name validation; `_LEGAL_TENANT_SOURCES` is deleted from `settings.py` (after 048 the class is a
pydantic `VarcoSettings` — ⚠️ ASSUMPTION its `sources` validator is the moved check; Step 1 reads
048's shape). Chain membership and order = `settings.sources` (env `VARCO_TENANT_SOURCES`), name by
name: a built-in name (`jwt`/`subdomain`/`legacy`/`act_as`) constructs the built-in exactly as
today; any other name must match exactly one pool source's `name`. `varco_core.tenancy.di.
resolve_tenant_source_chain(container) -> TenantSourceChain | None` reads the 048-bound settings,
`collect(container, TenantSource)`, and an optional `DelegationPolicy`, and calls the builder.**

Fail-closed rules, all `ValueError` at build time (startup, never per request):
- a name in `settings.sources` matching **neither** a built-in nor a pool source → `"unknown tenant
  source 'x'; built-ins: …; bound: …"` (today's `:317-322` check, moved and widened);
- a pool source whose `name` **collides with a built-in** → refused ("shadowing a built-in source
  name is not allowed; pick another name and omit the built-in from `VARCO_TENANT_SOURCES`");
- two pool sources with one `name` → refused.
A pool source **not named** in `settings.sources` is inert: bound ≠ active. `resolve_tenant_source_chain`
logs the inert names at INFO; 052 reports them. `inspect_tenant_provenance()` gains one token,
`third_party_source_high_trust`, emitted at `INFO` when a non-built-in source in the active chain
declares `trust >= TenantTrust.HIGH` — the one way a plugin can *elevate* itself (by claiming
trust), surfaced to the operator, never blocked (it may be legitimate: a mTLS-derived source).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: built-ins constructed from settings as today; pool adds named extras; allowlist stays** | Zero behaviour change for every env-only deployment; the security property (which origins may speak, brief 006) remains an explicit operator list — scanning a package that happens to ship a `TenantSource` **cannot** add a tenant origin; order stays the operator's (tie-breaks only); no new scanned `@Configuration` in `varco_core.tenancy` | Two construction paths (settings factory for built-ins, container for extras) — documented in `tenant-provenance.md` |
| (2) built-ins as `@Provider` contributions in a scanned `TenantSourcesConfiguration`, filtered by name | One path | Constructs all four on every app (`SubdomainTenantSource` with no base domains, `ActAsTenantSource` needing a `DelegationPolicy`), needs `@Requires` to gate on settings — impossible, the predicate is zero-arg (`scope.py:820`) and cannot see a code-bound settings instance; a scanned `@Configuration` in `varco_core.tenancy` is one more thing the scan rule must audit. Rejected |
| (3) drop the allowlist — every bound `TenantSource` is active, ordered by `trust` | "Plug and play" | A dependency's stray `@Singleton` source silently becomes an identity origin — the exact failure brief 006's trust ranking exists to prevent. Rejected outright |

### §D-posture — `PostureCollector` moves to `varco_core`; the six built-ins are contributed by the module

**Chosen: a new `varco_core/varco_core/posture.py` holding `PostureSeverity`, `PostureFinding`
(moved verbatim from `varco_fastapi/posture.py:51-88`) and the new `@Multibound` ABC
`PostureCollector` (`name: ClassVar[str]`; `collect(self) -> list[PostureFinding]`, sync).
`varco_fastapi.posture` imports the two moved names (so `from varco_fastapi.posture import
PostureFinding` and `varco_fastapi.__all__` `:355-356` keep working — `api_surface --check` reports a
*module move* as a note, never a failure). The five `_collect_*` functions and 057's
`_collect_redaction` become six public classes in `varco_fastapi/posture.py` — `HttpEdgeCollector(app)`,
`LocalPostureCollector(container, …)`, `AuthPostureCollector(auth, registry, store)`,
`TenantPostureCollector(chain, membership, delegation)`, `DataPostureCollector(rls_posture=None)`,
`RedactionPostureCollector(service_classes, envelope_settings)` — each body unchanged (057 §D-posture
promised "without touching this function's body"; the function becomes the method).
`SecurityPostureModule.configure_app(app, container)` contributes the six via `contribute()`
(resolving each optional input with `container.is_resolvable(...)`, 049's rule) — or exactly
`self._collectors` when `collectors=` was given. `_default_collectors()` is deleted.
`SecurityPostureModule.start()` runs `await acollect(container, PostureCollector)`;
`SecurityPostureLifecycle(collectors=…)` accepts `PostureCollector` instances **or** the old
zero-arg callables (tests and hand wiring keep working; `_wrap_collector` takes `name` from
`.name` or `__name__`).**

- ✅ Layering: `varco_sa` (056's live RLS check), `varco_core` (052's jwks/retention collectors),
  and any plugin subclass `varco_core.posture.PostureCollector` — the same seam rule as
  `varco_core.migration` vs `varco_fastapi.migrate`; today a non-`varco_fastapi` collector is
  impossible (R2).
- ✅ `SecurityPostureModule(audited_services: Sequence[type] = ())` is the one new kwarg —
  057's "an app must list its audited services once" lands as a constructor argument on the module
  that owns the collector, exactly like 049's `LoggingModule(redactor=)`.
- ✅ Contributed from `configure_app()` (runs only when the module is enabled and has both `app`
  and the container), so a disabled posture module leaves no collector bindings behind.
- ❌ Six small classes where six functions were; the report's `check` ids, severities, wording
  are untouched (057 §Non-goals' stable-id rule). ❌ `PostureFinding` has two import paths for
  one 4.0 cycle; 058 may deprecate the `varco_fastapi` one later, not this plan.
- Rejected — **scanned `@Singleton` collectors in `varco_fastapi/posture.py` with optional
  injections**: ✅ no `contribute()` calls; ❌ `HttpEdgeCollector` needs the `app` and
  `RedactionPostureCollector` needs `audited_services` — two of six would still be module-contributed,
  i.e. two mechanisms for one seam. Rejected — **keep the callable shape, multi-bind
  `Callable[[], list[PostureFinding]]`**: not a class, cannot be `@Multibound`.

### §D-consumer — `EventConsumer` is `@Multibound`; `CoreModule` auto-registers; `@PostConstruct` stops being the rule

**Chosen: `@Multibound` on `EventConsumer`. Three additions to the base class: `auto_register:
ClassVar[bool] = True` (opt-out), `is_registered_to(bus) -> bool` (public read of the
`_registered_buses` guard set, `consumer.py:825-844`), and `start()` becomes **idempotent per
bus** (`if self.is_registered_to(self._bus): return` before `register_to`). `CoreModule.start(container)`
(049 §D-core body) becomes: start the bus; `consumers = await acollect(container, EventConsumer)`;
for each with `auto_register` and not `is_registered_to(bus)`: `register_to(bus)`; keep the
tuple. `CoreModule.stop()`: `for c in reversed(consumers): await c.stop()`; then stop the bus.
`CoreModule.validate()`: every `EventConsumer` binding must be `Scope.SINGLETON`
(`get_all_bindings(EventConsumer)`, `.scope`) — a non-singleton consumer is listed, and
`start()` raises `RuntimeError` naming it (a fresh instance per resolution would leak
subscriptions and be unreachable by `stop()`). The bus is the container's unqualified
`AbstractEventBus` winner — a consumer wanting a specific qualified bus sets `auto_register = False`
and wires itself.**

**CLAUDE.md rule change (decided: replace, keep the escape hatch)**: *"a `@Singleton` `EventConsumer`
bound in the container is registered to the app's `AbstractEventBus` by `CoreModule.start()`, after
the bus has started — do not call `register_to()` from `@PostConstruct` any more (it runs at
construction, possibly before the bus starts; the auto path skips an already-registered consumer, so
the old shape still works, but it is no longer the documented way). `register_to()` by hand is for
non-DI consumers (tests, scripts, the hand-constructed `AuditConsumer`), `auto_register = False`
consumers, and fan-out to a second bus."* The `_bus` attribute convention (`consumer.py:686-711`)
becomes optional — needed only by a consumer that is *also* passed to `LifecycleModule`/
`VarcoLifespan.register()` by hand; its docstring says so. The §Database auditing sentence "the
consumer must be wired from `@PostConstruct`" is rewritten to "hand-constructed and `register_to()`-ed
by `varco_sa`/`varco_beanie`'s audit wiring — not DI-bound, so not auto-registered".

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: discovery in `CoreModule.start()`, idempotent, singleton-only, opt-out ClassVar** | Bus-before-consumers inside the one module that owns the bus (049 handed exactly this to 051; 050 then phases it across modules); the double-registration `RuntimeWarning` (`:833-843`) never fires from the auto path; the example app's `PostAppModule.start()` line goes away; the asyncapi generator's "is it registered" read becomes public | A consumer is constructed at `start()` even if nothing else needs it — it is a singleton and would be constructed at first event anyway; a `@RequestScoped` consumer is refused rather than silently instantiated outside a scope |
| (2) keep `@PostConstruct` as the documented way, add discovery only for consumers that did not self-register | No rule change | Two documented ways; `@PostConstruct` runs at *construction* (`register_to` before the bus starts — `cache/consumer.py:149` already warns about that ordering); the asyncapi generator's "unregistered consumer is deliberately absent" rule then depends on construction order. Rejected |
| (3) discovery in every backend module (`KafkaModule.start()` registers consumers to *its* bus) | Multi-bus apps get per-bus registration | Every consumer registered to every bus; 050's phases would have to special-case it. Rejected — opt out and wire by hand for the rare second bus |

`varco_core/asyncapi/generator.py:242-257` keeps its tolerant loop (skips an unresolvable binding —
a partially describable app is the documented intent, `:238-240`) but reads
`consumer.is_registered_to(...)`/a new `registered_bus_count` property instead of the private set,
and dedups exactly as before. `make asyncapi-check` output is unchanged (Step 21 proves it).

### §D-verifier — `WebhookVerifier` is not a collection; an explicit registration verb instead

**Chosen ❌ multi-binding. ✅ `register_webhook_verifier(name: str, cls: type[WebhookVerifier], *,
replace: bool = False) -> None` in `verifiers.py`, the exact shape of `varco_core.profiling`'s
`register_cpu_backend()`/`register_memory_backend()` (CLAUDE.md's Decision Tree: "add pyinstrument
by implementing `CpuProfilerBackend` + `register_cpu_backend()`"): a process-global **class**
registry (stateless classes keyed by name, no instances, no secrets, no lifecycle), raising
`ValueError` on a taken name unless `replace=True`; `get_verifier()`'s unknown-name error lists the
registered names; `_PROVIDERS` stays private behind it.**

- ✅ Verifiers are constructed **per route with that route's secrets** (`:544-593`, `varco_fastapi.
  webhook.verify_webhook` is a route dependency, CLAUDE.md §Inbound webhooks) — there is no
  instance to multi-bind; ✅ a plugin registers `("shopify", ShopifyWebhookVerifier)` at import of
  its own module and every route may then name `provider="shopify"`; ✅ mirrors an existing,
  documented varco pattern instead of inventing a `type[WebhookVerifier]` binding.
- ❌ A module-global mutable dict (the pattern `retention/policy.py:28-29` rejects for *policies*).
  Accepted for the same reason profiling accepts it: the registry holds classes, is written at
  import time, and is read per request — the process-global state is the class table, which is
  process-global anyway. ❌ `replace=True` can shadow a built-in; documented, greppable.
- Rejected — **qualifier-keyed factory bindings** (`container.get(WebhookVerifierFactory,
  qualifier="stripe")`): ✅ container-visible; ❌ `verify_webhook` is a route dependency with no
  container in hand, so every route would need `Instance[...]` plumbing for a lookup that is a
  dict read; a `Protocol` + five factory classes for five constructors. Over-engineering for a
  name→class table.

### §D-health — `HealthCheck` gets the marker, keeps its reader

`@Multibound` on `HealthCheck` (`health.py:152`) so `collection_seams()` and the `di_wiring` rule
cover it (045 already proved its contributions are non-fallback). `HealthRouter`'s
`Instance[HealthCheck].aget_all()` is **unchanged** — it is lazy on purpose (the router is built
before the container may be complete) and already zero-tolerant. One test asserts
`collect(container, HealthCheck)` equals the router's resolved set for a core+kafka container
(`get_all_bindings`, no broker). ✅ one convention, zero behaviour change; ❌ two read shapes for
one seam — recorded in the guide's table as the deliberate exception.

### §D-plugin — the third-party story, proven in-tree

A package that never imports `varco_fastapi` contributes to every core seam with **scan + a plain
binding** — no `bind_*`, no varco verb:

```python
# testkit/varco_plugin_example/__init__.py  (never packaged; on every package's pythonpath already)
@Singleton
class PartnerHeaderTenantSource(TenantSource):         # name = "partner", trust = TenantTrust.MEDIUM
    def resolve(self, request): ...
@Singleton
class PluginPostureCollector(PostureCollector):        # name = "plugin"
    def collect(self): return [PostureFinding(check="plugin.example", severity=PostureSeverity.INFO, ...)]
@Singleton
class PluginConsumer(EventConsumer):
    @listen(SomeEvent, channel="plugin") async def on(self, e): ...
@Configuration
class PluginRetention:
    @Provider(singleton=True)
    def policy(self, dlq: Inject[AbstractDeadLetterQueue]) -> RetentionPolicy:
        return RetentionPolicy(name="plugin-dlq", target=DlqRetentionTarget(dlq=dlq), cron_expr="0 3 * * *",
                               timezone="UTC", dry_run=True, older_than=timedelta(days=30))
```

`varco_core.di.bootstrap(c); c.scan("varco_plugin_example")` → `collect(c, TenantSource)` contains
the partner source; with `VARCO_TENANT_SOURCES=jwt,partner` the chain has both, in that order;
`acollect(c, RetentionPolicy)` contains `plugin-dlq` (the DLQ injected, in-memory default);
`collect(c, PostureCollector)` contains the plugin collector; `acollect(c, EventConsumer)` contains
the consumer, and after `CoreModule.start()` it `is_registered_to(bus)`. Nothing in `varco_*`
changed to admit it. The same package is the README's "Contributing from your own package" snippet
(§D-docs). ✅ one example, four seams, every test Docker-free; ❌ a testkit package that looks like
a product — its module docstring says "example plugin, never packaged, referenced by
`test_plugin_contributions.py` and README".

### §D-tests — pins, per-seam tests, and the `di_wiring` suite extension

| Test module | Asserts | Docker |
|---|---|---|
| `varco_core/tests/test_providify_contracts.py` (045's upgrade checklist; **extend**) | New pins, providify-only: `list[T]` on a `@Multibound` T with zero bindings → `[]` and `InjectInstances[T]` → `LookupError` (premise 2); three contributions at priorities `0,0,5` registered `B,A,C` → `[B, A, C]` (ascending, ties registration order — premise 1); the returned list is a fresh object per call; a shadowed `@Fallback` contribution is absent and a lone one present; a `@Requires(env=…)` contribution appears/disappears with `monkeypatch.setenv`; `get(list[T], qualifier="x")` narrows; a scanned `@Singleton` subclass is collected under the ABC; `bind(ABC, Impl)`'s `exact_only` self-binding does not duplicate; `container.scan()` of a module holding only a `@Multibound` class registers nothing | no |
| `varco_core/tests/test_di_collect.py` (new) | `contribute()` → non-fallback singleton `ProviderBinding`, `returns` is the seam, `__qualname__` names it; same object twice → `AlreadyBoundError`, a second *equal-but-distinct* object → two entries; `collect()` returns a `tuple`, `()` on empty, in registration order; `acollect()` awaits an `async def @Provider` contribution; `collection_seams()` returns the five types and importing `varco_core.di` leaves no seam module in `sys.modules` (budget guard) | no |
| `varco_core/tests/test_retention_policy.py`, `test_retention_di.py` (**rewritten**), `test_retention_scheduler.py`, `test_retention_posture.py`, `test_retention_cli.py`, `test_retention_no_di_side_effect.py` | `RetentionRegistry(policies)` runs the three guards over the set (duplicate name / both capability mismatches → `ValueError` naming the policy); no `register` attribute; `contribute(c, RetentionPolicy, p)` + a `@Provider -> RetentionPolicy` both collected; `bind_retention_registry` gone (`ImportError`); CLI `--target` accepting an iterable; scan of `varco_core` → `collect(c, RetentionPolicy) == ()` | no |
| `varco_fastapi/tests/test_retention_lifecycle.py` (**rewritten**) + `test_lifecycle_modules.py` (049's; extend) | `RetentionModule(interval=…)` builds the registry in `async_init()` from contributions; two policies with one name → `async_init()` raises `ValueError`; zero policies + enabled → `validate()` message; `RetentionLifecycle(registry, …)` unchanged | no |
| `varco_core/tests/test_tenant_source_chain_build.py` (new) + `test_tenant_provenance*.py` (extend) | `build_tenant_source_chain(settings, sources=[…])`: unknown name → `ValueError` listing built-ins and bound names; built-in collision → `ValueError`; duplicate pool names → `ValueError`; order = `settings.sources` order; inert pool source absent; `resolve_tenant_source_chain(container)` reads settings + pool + optional `DelegationPolicy`; `inspect_tenant_provenance()` emits `third_party_source_high_trust` for a `HIGH`-trust plugin source and not for `MEDIUM` | no |
| `varco_core/tests/test_posture_core_types.py` (new) + `varco_fastapi/tests/test_posture_collectors.py`, `test_posture_core.py`, `test_lifecycle_modules.py` (**migrated**) | `PostureCollector` is `@Multibound`; the six classes produce byte-identical findings to the old functions (parametrised over the existing fixtures — the old function tests become class tests); `SecurityPostureLifecycle` accepts a mix of collectors and callables; `SecurityPostureModule.configure_app` contributes six (`collectors=` → exactly those); a plugin collector joins the report; duplicate collector `name` → `posture.duplicate_collector` `WARN`; `varco_fastapi.posture.PostureFinding is varco_core.posture.PostureFinding` | no |
| `varco_core/tests/test_event_consumer_autoregister.py` (new) + `varco_fastapi/tests/test_core_module.py` (049's; extend) | `is_registered_to()`; `start()` idempotent per bus (no `RuntimeWarning` on a second `start()`, `stop()` then `start()` re-registers); `CoreModule.start()` registers every `@Singleton` consumer once, skips `auto_register = False`, skips one already registered by `@PostConstruct` **without** a warning, raises on a `@Component`-scoped consumer that `validate()` also listed; `stop()` cancels subscriptions before the bus stops (order pinned with a recording bus); the asyncapi generator's output for the example app is unchanged (`make asyncapi-check`) | no |
| `varco_core/tests/test_webhook_verifier_registry.py` (new) | `register_webhook_verifier("x", Cls)` → `get_verifier("x", secrets=[…])` constructs `Cls`; taken name → `ValueError`; `replace=True` swaps; unknown-name error lists names; the five built-ins unchanged | no |
| `varco_core/tests/test_plugin_contributions.py` (new; §D-plugin) + one posture leg in `varco_fastapi/tests/test_plugin_posture_contribution.py` | The four-seam scenario above, from `varco_plugin_example`, with **no** varco edit | no |
| `testkit/varco_conformance/di_wiring.py` (046's suite; **extend**) — `test_collection_seams_have_no_fallback_contributors`: for each seam in `collection_seams()`, every `get_all_bindings(seam)` entry has `fallback is False` and `priority == 0` (§D-order); `test_event_consumer_contributions_are_singletons`: every `EventConsumer` binding is `Scope.SINGLETON`; `test_health_checks_collect`: `collect(c, HealthCheck)` names equal the router's resolved set | All ten package subclasses inherit both tests unchanged; `COVERAGE.md`'s `di_wiring` row gains the two names | no |

## Steps

All commands from `/home/edoardo/projects/varco`. Steps 1-3 first, in order; 4-9 are independent
of each other; 10-12 need 4-9; 13-16 after; 17-21 last. `make test` is expected red between
Step 5 and Step 12 — Steps 4-12 land as one commit.

### Preflight and the primitives

1. [x] **Preflight — measure every ⚠️ ASSUMPTION before writing code.** Record in
       `/tmp/claude-1000/…/scratchpad/051-preflight.txt`:
       (a) `uv run python -c "import importlib.metadata as m; print(m.version('providify'))"` → `2.1.0`;
       re-read every `file:line` in §Context on `.venv/lib/python3.12/site-packages/providify/`
       (`container.py:1620-1631,1662-1707,1727-1818,1862-1900,2129-2170,6548-6557,7464-7496`;
       `multibinding.py:26-72`; `type.py:171-213,1021-1080`; `scanner.py:142-162`;
       `binding.py:196-222,723-745`) — fix any drifted citation in this file first.
       (b) Layout after 046/049: `ls varco_core/varco_core/di*` — a package `di/` with `module.py`
       (049) or still `di.py` (046)? §D-verbs' file becomes `di/collect.py` or a section of
       `di.py` accordingly; the public names are identical either way.
       (c) Execute the marker-is-not-a-registration claim: a throwaway module with only a
       `@Multibound class X` → `container.scan(mod)` → `len(container.describe().bindings) == 0`.
       (d) Execute: `container.get(list[T])` for a T that is **not** a collection point and has no
       binding → record the exception type (`LookupError` expected; §D-verbs' `collect()` lets it
       propagate unwrapped).
       (e) Execute: `@Component`-scoped `EventConsumer` subclass + `aget(list[EventConsumer])` →
       record whether it instantiates (expected yes) — §D-consumer's `validate()` gate is why we
       never rely on this.
       (f) Read 048's landed `TenantProvenanceSettings`: where does the `sources` name validation
       live now (`rg -n '_LEGAL_TENANT_SOURCES|jwt.*subdomain.*legacy' varco_core/varco_core/tenancy/`)?
       Record the exact site Step 7 moves.
       (g) Read 049's landed `SecurityPostureModule`/`_default_collectors` and 057's
       `_collect_redaction` signature (`rg -n '_default_collectors|_collect_redaction' varco_fastapi/`),
       and `CoreModule.start()`'s body (`rg -n 'class CoreModule' -A 40 varco_core/varco_core/di/`).
       (h) `uv run python scripts/import_budget.py --check --warn-only` baseline; `make asyncapi-check`
       green baseline.
       **Verify**: the preflight file lists ≥ 25 confirmed citations and the six measurements.

2. [x] `varco_core/varco_core/di/collect.py` (**new**; re-exported from `varco_core/di/__init__.py`)
       — `contribute()`, `collect()`, `acollect()`, `collection_seams()` per §D-verbs, with a module
       docstring that *is* the convention (§D-shape rule verbatim, the §D-order warning, the
       §D-immutable rule, the `bind_instance`-vs-`contribute` double-call table). `contribute()`
       reuses 046's `AlreadyBoundError`. `collection_seams()` imports the five seams **inside the
       body**. No providify marker anywhere in the module.
       Test first: `varco_core/tests/test_di_collect.py` (§D-tests row 2) — red, then green.
       **Verify**: `uv run pytest varco_core/tests/test_di_collect.py -q`; `import_budget --check
       --warn-only` unchanged from 1(h).

3. [x] `varco_core/tests/test_providify_contracts.py` — add the multibinding pins (§D-tests row 1)
       under a `# ── Plan 051: multi-binding ──` header; extend the module docstring's checklist
       ("`list[T]` on a `@Multibound` seam returns `[]`; ascending priority, stable; shadowed
       fallback absent; `InjectInstances` raises on zero — if any of these move, §D-shape/§D-order
       in `dependency-injection.md` are wrong"). **Verify**: all pass at 2.1.0; `-rxX` shows `0
       xfailed` added.

### The seams (independent of each other)

4. [x] **Posture types + ABC** — `varco_core/varco_core/posture.py` (**new**): `PostureSeverity`,
       `PostureFinding` (moved verbatim from `varco_fastapi/posture.py:51-88`, docstrings kept),
       `@Multibound class PostureCollector(ABC)` (`name: ClassVar[str]`, `collect()`; docstring:
       "sync, must not raise — the lifecycle wraps it; a raising collector yields one
       `NOT_ASSESSED`; `name` must be unique per app — a duplicate is reported, not fatal"), `__all__`.
       Added to `varco_core/__init__.py`'s PEP 562 lazy map (`"PostureCollector": "varco_core.posture"`,
       plus the two moved names) — budget-neutral (dataclasses + `enum` only; Step 21 measures).
       `varco_fastapi/varco_fastapi/posture.py`: delete the two definitions, `from varco_core.posture
       import PostureFinding, PostureSeverity, PostureCollector`; `_wrap_collector` accepts a
       `PostureCollector | Callable`; `SecurityPostureLifecycle.__init__` normalises to
       `tuple` of `(name, fn)` pairs and emits `posture.duplicate_collector` (`WARN`) per repeated
       name. Test first: `varco_core/tests/test_posture_core_types.py` + the identity assertion in
       `varco_fastapi/tests/test_posture_core.py`.

5. [x] **Posture collectors as classes + the module contributes them** — `varco_fastapi/varco_fastapi/posture.py`:
       `HttpEdgeCollector`, `LocalPostureCollector`, `AuthPostureCollector`, `TenantPostureCollector`,
       `DataPostureCollector`, `RedactionPostureCollector` — each `__init__` takes exactly the old
       function's kwargs and stores them; `collect()` is the old body (**diff must show the body
       moved, not edited** — 057's promise); `name` = the old `_wrap_collector(name=…)` string so
       `posture.not_assessed.<name>` ids are unchanged. Old functions deleted. `__all__` gains the
       six. `varco_fastapi/varco_fastapi/modules/lifecycle.py` (049): `SecurityPostureModule(settings=None,
       *, enabled=None, collectors=None, audited_services=())`; `configure_app()` contributes per
       §D-posture; `start()` → `acollect`; `validate()` → §D-empty message; `_default_collectors`
       deleted. Tests: `varco_fastapi/tests/test_posture_collectors.py` migrated function→class
       (same fixtures, same expected ids — the byte-identical proof), `test_lifecycle_modules.py`
       extended (§D-tests row 6). **Verify**: `uv run pytest varco_fastapi/tests/test_posture*.py
       varco_fastapi/tests/test_lifecycle_modules.py -q`; `rg -n '_default_collectors|_collect_[a-z]+\(' varco_fastapi/varco_fastapi` → 0.

6. [x] **Retention** — `varco_core/varco_core/retention/policy.py`: `@Multibound` on `RetentionPolicy`
       (marker only; the dataclass is otherwise untouched — `dry_run` stays required);
       `RetentionRegistry.__init__(self, policies: Iterable[RetentionPolicy] = ())` runs the three
       guards (`register()`'s body becomes private `_add()`, called only from `__init__`); public
       `register()` **deleted**; class docstring's "pass it to `bind_retention_registry`" → "built by
       `RetentionModule` from contributed policies; construct one by hand only for the CLI/tests".
       `git rm varco_core/varco_core/retention/di.py`; `retention/__init__.py` docstring import list
       updated (`from varco_core.di import contribute` replaces the `di` line). `cli/retention.py`
       `_resolve()`: wrap a non-registry iterable in `RetentionRegistry(...)`; `list` help text
       updated. `varco_fastapi/varco_fastapi/retention.py` (`RetentionLifecycle`) **unchanged**;
       `modules/lifecycle.py` `RetentionModule(*, interval=0.0)` — `async_init()` → `self._registry =
       RetentionRegistry(await acollect(container, RetentionPolicy))`; `validate()` → §D-empty;
       `start()` builds `RetentionLifecycle(self._registry, container=…, interval=…)`. Tests per
       §D-tests rows 3-4 (write the red tests first; `test_retention_di.py` is rewritten to test
       `contribute`+`collect` for policies). **Verify**: `uv run pytest varco_core/tests/test_retention*.py
       varco_fastapi/tests/test_retention_lifecycle.py -q`; `rg -n 'bind_retention_registry|registry\.register\(' varco_* examples README.md CLAUDE.md ARCHITECTURE.md technical_docs` → 0.

7. [x] **Tenant sources** — `varco_core/varco_core/tenancy/source.py`: `@Multibound` on `TenantSource`;
       class docstring gains the contribution paragraph (§D-tenant: bound ≠ active; name uniqueness;
       built-in names reserved). `tenancy/settings.py`: `build_tenant_source_chain(settings=None, *,
       delegation_policy=None, sources: Iterable[TenantSource] = ())` — `_BUILTIN_SOURCE_NAMES =
       ("jwt", "subdomain", "legacy", "act_as")`; the three `ValueError`s of §D-tenant; the name
       validation moved from the site Step 1(f) found (delete `_LEGAL_TENANT_SOURCES` and its
       `from_env`/validator check — an unknown name is now a *build*-time error because only the
       builder can see the pool). `tenancy/di.py`: `resolve_tenant_source_chain(container) ->
       TenantSourceChain | None` (settings via `container.get(TenantProvenanceSettings)` — 048's
       binding; pool via `collect()`; `DelegationPolicy` via `is_resolvable`; INFO log of inert
       names); exported from `varco_core.tenancy`. `tenancy/posture.py`: the
       `third_party_source_high_trust` token (INFO) — `_tenant_finding_for_token` in
       `varco_fastapi/posture.py` gains its mapping. Tests per §D-tests row 5. **Verify**:
       `uv run pytest varco_core/tests/test_tenant* -q`; `rg -n '_LEGAL_TENANT_SOURCES' varco_*` → 0.

8. [x] **EventConsumer** — `varco_core/varco_core/event/consumer.py`: `@Multibound` on `EventConsumer`;
       `auto_register: ClassVar[bool] = True`; `is_registered_to(bus)`; `registered_bus_count`
       property; `start()` idempotence guard; class docstring: the "REQUIRED PATTERN — `_bus`"
       section (`:686-711`) rewritten to "DI-bound consumers are auto-registered by `CoreModule`;
       `_bus` + `start()` remain for hand-passed lifecycle components"; the `@PostConstruct` example
       (`:700-704`) removed. `varco_core/varco_core/di/module.py` (049's `CoreModule`): `start()`/
       `stop()`/`validate()` per §D-consumer (the `RuntimeError` message names the class and says
       "make it `@Singleton` or set `auto_register = False`"). `varco_core/varco_core/asyncapi/generator.py:260-274`
       → `consumer.registered_bus_count > 0`. `examples/00-full-stack-post-api/example/module.py`
       (049's `PostAppModule`): delete the `consumer.start()` line and its `stop()` twin;
       `example/consumer.py:99-129` docstring/`_bus` comment trimmed (the `_bus` assignment may
       stay — harmless — but the comment no longer claims it is required). Tests per §D-tests row 7.
       **Verify**: `uv run pytest varco_core/tests/test_event_consumer_autoregister.py
       varco_fastapi/tests/test_core_module.py varco_core/tests/test_asyncapi*.py -q`;
       `make asyncapi-check` green (snapshot unchanged — the consumer class and its channels did not
       move).

9. [x] **WebhookVerifier registry verb** — `varco_core/varco_core/webhook/inbound/verifiers.py`:
       `register_webhook_verifier(name, cls, *, replace=False)`; `get_verifier()` error message
       lists `sorted(_PROVIDERS)`; a module DESIGN block recording §D-verifier's ✅/❌ and the
       profiling precedent; exported from `varco_core.webhook.inbound` and the `varco_core` lazy
       map. Test first: `varco_core/tests/test_webhook_verifier_registry.py`. Also `health.py:152`:
       `@Multibound` on `HealthCheck` (marker only) + the `collect(HealthCheck)` test in
       `varco_kafka/tests/test_kafka_di_wiring.py`'s package subclass (via the suite, Step 10).

       **Follow-up note**: the "exported from ... the `varco_core` lazy map" half of this step
       had not actually landed in the working tree — closed by a small follow-up commit adding
       `register_webhook_verifier`/`registered_webhook_verifiers` to `varco_core/__init__.py`'s
       `_LAZY` map, `__all__` (287 → 289), and `TYPE_CHECKING` block, plus
       `varco_core/tests/test_plan050_051_public_reexports.py`'s identity assertions.

### Suite, plugin proof, wiring-level tests

10. [x] `testkit/varco_conformance/di_wiring.py` — the three tests of §D-tests' last row;
        `testkit/varco_conformance/COVERAGE.md` `di_wiring` row lists them. **Verify**: `for p in
        varco_core varco_kafka varco_nats varco_redis varco_memcached varco_ws varco_casbin varco_sa
        varco_beanie varco_fastapi; do uv run pytest $p/tests/test_*di_wiring*.py -q; done` — all
        green with no subclass edit.

11. [x] `testkit/varco_plugin_example/__init__.py` (**new**, §D-plugin) + `varco_core/tests/test_plugin_contributions.py`
        + `varco_fastapi/tests/test_plugin_posture_contribution.py`. **Verify**: both pass;
        `git diff --stat varco_*/varco_*` for this step is empty (the proof that no framework edit
        was needed — paste the empty diff into the preflight file).

12. [x] `varco_fastapi/tests/test_fastapi_di_wiring.py` (046's subclass) — add the `varco_fastapi`
        leg of `test_collection_seams_have_no_fallback_contributors` running **after**
        `SecurityPostureModule().configure_app(app, c)` so the six contributed collectors are
        checked too. `make test` green from here.

### Docs, changelog, gates

13. [x] `technical_docs/features/dependency-injection.md` (045's document) — new **§Multi-binding**:
        the §D-shape rule verbatim; the seam table (seam · module · who contributes varco's defaults ·
        how an app/plugin contributes · reader · ordering semantics · empty-means) for the five seams
        + `HealthCheck`'s reader exception + `WebhookVerifier`'s ❌ row; the §D-order warning with the
        two-directions example; the §D-immutable rule; `contribute` vs `bind_instance` table; the
        §D-empty table; the plugin snippet; a **Pitfalls** table (`priority=` on a contribution;
        `InjectInstances[T]` raising on zero; a `@Fallback` contribution vanishing; a literal
        `list[T]` binding winning over the collection point, `container.py:1620-1627`; a
        `@Component`-scoped consumer; `@PostConstruct` `register_to` before the bus started; a
        `TenantSource` bound but not named in `VARCO_TENANT_SOURCES`; `replace=True` shadowing a
        built-in verifier).
14. [x] `CLAUDE.md` — (a) §Event system: the `@listen`/`register_to` paragraph's "(typically in a
        `@PostConstruct` method)" → the §D-consumer rule (three lines + pointer); (b) §Database
        auditing: the `@PostConstruct` sentence rewritten per §D-consumer; (c) §Retention rules:
        "`bind_retention_registry`" → "`contribute(container, RetentionPolicy, …)` +
        `RetentionModule()`"; (d) §Tenant identity provenance: one line — "an out-of-tree
        `TenantSource` is a scanned `@Singleton` named in `VARCO_TENANT_SOURCES`; bound ≠ active";
        (e) §Admin surface/posture: one line — "a posture collector is a `varco_core.posture.PostureCollector`
        contribution, never an edit to `varco_fastapi/posture.py`"; (f) Decision tree: a new branch
        **"All implementations of X (a plugin/app must be able to add one)?"** → `@Multibound` seam +
        `contribute()`/scan; never a hand list, never `InjectInstances`; the retention branch's
        `bind_retention_registry` text and the Pre-Implementation checklist's "wired in
        `@PostConstruct`" bullet updated; the `install_*`/`bind_*` taxonomy row for
        `bind_retention_registry` (046 already reshaped the table — remove the name). Pointer to the
        guide's §Multi-binding. Nothing else — CLAUDE.md is a routing table.
15. [x] `README.md` — "Consumer — EventConsumer + @listen": drop the `@PostConstruct` block, add the
        two-line "bound = registered" note; "Retention & purge automation": `registry.register()` +
        `bind_retention_registry` → `contribute()` + `RetentionModule()`; "Security posture
        preflight": a `PostureCollector` subclass snippet; "Tenant identity provenance": the
        `VARCO_TENANT_SOURCES=jwt,partner` example; a new short "Contributing from your own package"
        subsection under "DI wiring" showing `varco_plugin_example`'s shape; "Receiving a signed
        webhook": `register_webhook_verifier`.
16. [x] `ARCHITECTURE.md` (the retention/posture/tenancy module listings — `retention/di.py` gone,
        `varco_core/posture.py` new), `technical_docs/features/retention-and-purge.md`,
        `security-posture.md` (the "Wiring a collector" snippet → subclass + contribute; the six
        class names), `tenant-provenance.md` (§D-tenant's rules + the new token),
        `inbound-webhooks.md` (the verb), `asyncapi-export.md` (discovery now reads the public
        accessor), `common-pitfalls.md` (two rows: `priority=` on a contribution; `InjectInstances`
        raising on zero).
17. [x] `CHANGELOG.md` `[Unreleased]`: `### BREAKING — multi-binding for collection seams (Plan 051,
        DI-5)` = §Migration's rows; `### Added` (`contribute`/`collect`/`acollect`/`collection_seams`,
        `PostureCollector`, `resolve_tenant_source_chain`, `register_webhook_verifier`,
        `EventConsumer.auto_register`/`is_registered_to`, the plugin example). `BACKLOG.md:95` →
        `done (Plan 051)`; RO-2's rationale gains "one `PostureCollector` subclass each after 051".
18. [x] `uv run python scripts/api_surface.py` — regenerate; the diff must be exactly: removals
        (`bind_retention_registry`, `RetentionRegistry.register` is a method — not tracked),
        additions (the names in Step 17), and the `PostureFinding`/`PostureSeverity` module-move
        notes. Anything else is another branch's dirt — stop and look.
19. [x] `make lint` (incl. `api-check`, `asyncapi-check`, `import-budget --warn-only`), `make type-check`,
        `make test`, `make docs`. Record `import_budget` delta vs 1(h) in the CHANGELOG entry if
        non-zero.
20. [x] **Handoffs** (in this plan's Done section and the index): **050** — `WebhookDispatcher`
        once `@Singleton`-bound is auto-registered; set `auto_register = False` if it must not be;
        cross-module phases must keep `CoreModule.start()`'s internal order (bus → consumers) and
        `stop()`'s reverse. **052** — `collection_seams()` + `collect()` are the read; the
        §D-empty messages come through `VarcoModule.validate()`; RO-2 = two `PostureCollector`
        subclasses contributed by `JwksRefreshModule`/`RetentionModule`. **055** — the middleware's
        chain default is `resolve_tenant_source_chain(container)`. **056** — a live RLS collector is
        a `PostureCollector` in `varco_sa`, contributed by `SAModule`. **058** — §Migration below.
21. [x] Final measurements pasted into the preflight file: `import_budget --check --warn-only`
        (no breach), `rg -c '@Multibound' varco_*/varco_*` → 5, `rg -n 'priority=' varco_*/varco_* |
        rg -v 'BACKEND_PRIORITY|@listen|subscribe|tests/'` → only 045-sanctioned sites.

## Edge cases

- **Zero contributions on any seam** → `collect()` returns `()`; `CoreModule` silent;
  `RetentionModule`/`SecurityPostureModule` report via `validate()` (§D-empty); providify's
  `validate()` says nothing (`:6549-6557`).
- **A contribution marked `@Fallback`** → absent the moment any sibling exists (`:2168-2169`); the
  `di_wiring` suite fails naming the binding. A *lone* `@Fallback` contribution is present — still
  refused by the suite (the rule is structural, not situational).
- **`priority=` on a contribution** → suite failure (§D-order); if an app does it anyway, providify
  sorts it last-for-highest and `get(Seam)` returns it as "the winner" — documented pitfall.
- **Same object contributed twice** → `AlreadyBoundError` from `contribute()`; two distinct
  equal-valued `RetentionPolicy`s → both collected, `RetentionRegistry` raises on the duplicate
  `name` at `async_init()`.
- **A literal `list[Seam]` binding exists** (an app did `provide(fn, returns=list[TenantSource])`)
  → providify's rule 1: the literal wins and the collection is ignored (`:1620-1627`),
  `validate_bindings()` warns (`:5902-5926`). Pitfall row; never done by varco.
- **`@Requires`-gated contribution whose env flag is off** → simply absent; flips on the next
  `collect()` (lazy, `scope.py:849-854`); a *singleton already resolved* stays cached and
  already-registered (`scope.py:872-876`) — `CoreModule.start()` runs once, so a consumer that
  appears after start is not registered until restart. Documented.
- **`@PostConstruct` consumer under auto-registration** → `is_registered_to(bus)` is `True` → skipped
  silently; no `RuntimeWarning`.
- **A consumer registered by hand to a qualified second bus** → `auto_register = False`, or leave it
  `True` and it is *also* registered to the default bus (fan-out, `consumer.py:731-732`). Documented.
- **`EventConsumer.stop()` then `start()`** → `stop()` clears `_registered_buses` (`:1087-1088`), so
  `start()` re-registers — unchanged.
- **Tenant: `VARCO_TENANT_SOURCES=jwt,partner` and no `partner` bound** → `ValueError` at
  `resolve_tenant_source_chain()` (startup), never a silent header-only fallback.
- **Tenant: a bound source named `jwt`** → `ValueError` (built-in collision) even if `jwt` is not
  in `settings.sources`.
- **Tenant: `settings.sources == ()`** → `None` chain regardless of the pool — unchanged.
- **Posture: two collectors named `local`** → one `posture.duplicate_collector` `WARN` finding, both
  run (findings are a set; ids may duplicate — the report already tolerates it).
- **Posture: `SecurityPostureModule(collectors=())`** → zero built-ins contributed; plugin
  collectors still join; `validate()` message only if the total is zero.
- **Retention: an `async def @Provider` policy** → `acollect()` awaits it; `collect()` would raise
  providify's `RuntimeError` (`:1782-1788`) — `RetentionModule` uses `acollect()` for this reason.
- **`scan("varco_core", recursive=True)`** → still registers no retention/tenancy/posture binding
  (the marker is not a registration — Step 1(c)); `collect(c, RetentionPolicy) == ()`.
- **`register_webhook_verifier` at import of a plugin module imported twice** → the second call
  raises `ValueError` (name taken) unless `replace=True` — a plugin guards with
  `if "x" not in registered_webhook_verifiers()` (a `frozenset` accessor shipped beside the verb).

## Verification

```bash
# Contracts + primitives
uv run pytest varco_core/tests/test_providify_contracts.py varco_core/tests/test_di_collect.py -q -rxX
# Seams
uv run pytest varco_core/tests/test_retention*.py varco_fastapi/tests/test_retention_lifecycle.py -q
uv run pytest varco_core/tests/test_tenant*.py -q
uv run pytest varco_core/tests/test_posture_core_types.py varco_fastapi/tests/test_posture*.py varco_fastapi/tests/test_lifecycle_modules.py -q
uv run pytest varco_core/tests/test_event_consumer_autoregister.py varco_fastapi/tests/test_core_module.py varco_core/tests/test_asyncapi*.py -q
uv run pytest varco_core/tests/test_webhook_verifier_registry.py -q
# Plugin proof + suite
uv run pytest varco_core/tests/test_plugin_contributions.py varco_fastapi/tests/test_plugin_posture_contribution.py -q
for p in varco_core varco_kafka varco_nats varco_redis varco_memcached varco_ws varco_casbin varco_sa varco_beanie varco_fastapi; do uv run pytest $p/tests/test_*di_wiring*.py -q || break; done
# Registries gone
rg -n 'bind_retention_registry|registry\.register\(|_LEGAL_TENANT_SOURCES|_default_collectors|_registered_buses' varco_*/varco_* examples README.md CLAUDE.md ARCHITECTURE.md technical_docs   # only consumer.py's own guard set
rg -n 'InjectInstances' varco_*/varco_*                        # 0
rg -c '@Multibound' varco_*/varco_*                            # 5 files
rg -n 'PostConstruct' CLAUDE.md README.md | rg -i 'consumer|register_to'   # 0
# Gates
make lint          # api-check regenerated (Step 18), asyncapi-check unchanged, import-budget no breach
make type-check
make test
make docs
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — the installed 2.1.0 wheel matches the tree line-for-line for every citation | §Context is true of what runs | Step 1(a) re-reads |
| ⚠️ ASSUMPTION — `@Multibound`'s `setattr` on a class registers nothing under `container.scan()` (`scanner.py:151-162` reads Singleton/Provider/Configuration metadata only) | `scan("varco_core")` gains no retention/tenancy/posture binding; `test_retention_no_di_side_effect.py` stays green | Step 1(c) executes it |
| ⚠️ ASSUMPTION — 049 landed `varco_core.di` as a package with `module.py` and `CoreModule.start()` starting the bus; 057 landed `_collect_redaction(*, service_classes=(), envelope_settings=None)` | Steps 5 and 8 edit the right files | Step 1(b), 1(g) read the tree |
| ⚠️ ASSUMPTION — 048 moved `TenantProvenanceSettings`'s name validation into a pydantic validator | Step 7 deletes exactly one check | Step 1(f) locates it |
| ⚠️ ASSUMPTION — `aget(list[EventConsumer])` instantiates a `@Component`-scoped consumer outside any scope (rather than raising) | `CoreModule.validate()`'s singleton gate is the only guard needed | Step 1(e); if providify raises instead, the gate stays (the error message is better) |
| ⚠️ ASSUMPTION — constructing the six posture collectors at `configure_app()` time is free (they store references; `collect()` runs at `start()`) | No I/O before `start()` | Their `__init__` bodies are assignments only — Step 5 reviews |
| The `RetentionRegistry` is built at `async_init()`, so a duplicate-name error is a startup exception, not a `validate()` prediction | The scheduler never runs a policy set that failed the guards | `async_init()` precedes `start()` in 049's sequence; the `ValueError` aborts the lifespan |
| A dependency ships a scanned `@Singleton` `TenantSource` | It is inert unless named in `VARCO_TENANT_SOURCES` | §D-tenant's allowlist; `resolve_tenant_source_chain` logs it; 052 reports it |
| A consumer that self-registered in `@PostConstruct` **before the bus started** still does so | Unchanged behaviour (the `cache/consumer.py:149` caveat) until the app removes the `@PostConstruct` | §Migration tells them to; the auto path never double-registers |
| `PostureFinding`/`PostureSeverity` move modules | `varco_fastapi.posture` re-exports; identity is preserved (`is`) | Test in Step 4; `api_surface` records a move note |
| `make asyncapi-check` drifts | The example consumer class and channels do not move; only the "is registered" read changes | Step 8's verify |
| Import budget: `varco_core.di` gains `collect.py`; `varco_core/__init__.py` lazy map gains three names | No new eager import; `collection_seams()` imports lazily | Steps 2, 4, 21 measure |
| M1 (`type: ignore` count) moves | Directional only | Step 21 records the delta |

## Migration (3.x → 4.0, DI-5 entry — Plan 058 assembles)

**BREAKING — collection seams are multi-bound; five registries are gone.**

| 3.x | 4.0 |
|---|---|
| `registry = RetentionRegistry(); registry.register(policy); bind_retention_registry(container, registry)`; `create_varco_app(retention=RetentionLifecycle(registry, container=c))` / 049's `RetentionModule(registry)` | `contribute(container, RetentionPolicy, policy)` (or a `@Provider … -> RetentionPolicy` in your `@Configuration`); `RetentionModule(interval=…)` with no registry argument. `RetentionRegistry.register()` no longer exists — `RetentionRegistry(policies)` if you construct one by hand (CLI targets, tests) |
| `varco retention … --target mod:callable` returning a `RetentionRegistry` | unchanged; the callable may also return an iterable of `RetentionPolicy` |
| `from varco_core.retention.di import bind_retention_registry` | removed — `from varco_core.di import contribute` |
| `SecurityPostureLifecycle(collectors=[partial(_collect_http, app=app), …])`, `SecurityPostureModule(collectors=[…callables…])` | The six built-ins are contributed by `SecurityPostureModule` automatically (⚠️ superseded — final ownership is 058 R8: four from `SecurityPostureModule`, tenant from `TenancyModule` (055), data from `SAModule` (056)); `collectors=` still accepted (replaces the built-ins; callables still allowed). Add your own with `@Singleton class MyCollector(varco_core.posture.PostureCollector)` or `contribute(container, PostureCollector, obj)`. `_collect_*` functions are gone — the bodies live in `HttpEdgeCollector`/`LocalPostureCollector`/`AuthPostureCollector`/`TenantPostureCollector`/`DataPostureCollector`/`RedactionPostureCollector` |
| `from varco_fastapi.posture import PostureFinding, PostureSeverity` | still works; canonical home is `varco_core.posture` |
| 057's `partial(_collect_redaction, service_classes=(PostService,))` | `SecurityPostureModule(audited_services=(PostService,))` |
| `VARCO_TENANT_SOURCES` limited to `jwt,subdomain,legacy,act_as`; an unknown name rejected at `TenantProvenanceSettings` construction | any bound `TenantSource.name` is legal; unknown/colliding names are rejected by `build_tenant_source_chain()` / `resolve_tenant_source_chain(container)` at build time. Bound but unnamed sources are inert. New posture token `third_party_source_high_trust` (INFO) |
| `build_tenant_source_chain(settings, delegation_policy=…)` | same, plus `sources=` (the pool); `resolve_tenant_source_chain(container)` does the reading for you |
| `class MyConsumer(EventConsumer)` with `@PostConstruct def _setup(self): self.register_to(self._bus)` | delete the `@PostConstruct` method; a `@Singleton` consumer is registered by `CoreModule.start()` after the bus starts. Set `auto_register = False` to opt out. `EventConsumer.start()` is now idempotent per bus; a `@Component`-scoped consumer is refused at startup |
| `PostAppModule.start(): await consumer.start()` (049's example) | nothing — remove the line |
| A custom inbound webhook provider required editing `_PROVIDERS` | `register_webhook_verifier("name", Cls)` at import; `get_verifier("name", …)` |
| `container.get_all(HealthCheck)`-style reads in app code | still fine; `collect(container, HealthCheck)` returns the same set as a tuple |

**Not changed**: `RetentionPolicy` fields (`dry_run` required); every posture `check` id and
severity; `TenantTrust` values and the chain's winner rule; `HealthRouter`; `get_verifier()`'s
signature; `register_to()`'s double-call `RuntimeWarning` (only `start()` and the auto path skip).

## Docs / backlog updates

- `technical_docs/features/dependency-injection.md` §Multi-binding — Step 13.
- `CLAUDE.md` — Step 14 (six touch points, all pointers).
- `README.md`, `ARCHITECTURE.md`, five feature docs, `common-pitfalls.md` — Steps 15-16.
- `CHANGELOG.md`, `BACKLOG.md:95` (+ `:110` RO-2 note) — Step 17.
- `testkit/varco_conformance/COVERAGE.md` — Step 10.
- `design/api-freeze-and-standards/measurements/api-surface.*` — Step 18.

## Done criteria

- [x] `rg -c '@Multibound' varco_*/varco_*` → exactly `event/consumer.py`, `retention/policy.py`,
      `tenancy/source.py`, `posture.py`, `health.py`; `collection_seams()` returns those five.
- [x] `bind_retention_registry`, `retention/di.py`, `RetentionRegistry.register()`,
      `_LEGAL_TENANT_SOURCES`, `_default_collectors`, the six `_collect_*` functions, the asyncapi
      generator's private `_registered_buses` read, and the `@PostConstruct` consumer rule in
      CLAUDE.md/README are gone.
- [x] `contribute`/`collect`/`acollect`/`collection_seams`, `PostureCollector`,
      `resolve_tenant_source_chain`, `register_webhook_verifier`, `EventConsumer.auto_register`/
      `is_registered_to`/`registered_bus_count` exist, are exported, and are in the regenerated
      `api-surface` snapshot.
- [x] The providify pins (Step 3) pass; the `di_wiring` suite's three new tests pass in all ten
      packages with no subclass edit.
- [x] `varco_plugin_example` contributes to four seams with an empty `git diff varco_*/varco_*`
      for Step 11.
- [x] `RetentionModule`/`SecurityPostureModule` report an empty collection through `validate()`;
      the tenant builder raises on unknown/colliding/duplicate names; `CoreModule` refuses a
      non-singleton consumer and never double-registers.
- [x] `make lint`, `make type-check`, `make test`, `make docs` green; `make asyncapi-check` unchanged;
      `import_budget --check --warn-only` reports no breach; no file under
      `/home/edoardo/projects/providify` modified.
- [x] CHANGELOG BREAKING block = §Migration; `BACKLOG.md:95` reads `done (Plan 051)`.
