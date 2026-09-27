# Internationalization — `MessageCatalog`, negotiation, and ambient locale

Plan 011 (I2, plus the `varco_core.context` primitive it builds on — X1).
Closes: "no request-scoped locale, no way to render a localized message
server-side, no negotiation over `Accept-Language`."

**Off by default** (`I18nSettings.enabled=False`) — no catalog constructed,
no middleware added, no `.mo` file read, no `Content-Language` header,
`current_locale()` returns `None` everywhere.

## `varco_core.context` — the ambient primitive I2 and T1 share (X1)

Before I2 itself: both I2 (locale) and T1 (timezone) need "read the
request, resolve a value, make it available ambiently for the rest of the
request, unset it after". `varco_core.context` ships this once:

- `AmbientVar[T]` (`context/ambient.py`) — a ~70-line generic wrapping
  `contextvars.ContextVar[T | None]`, with `.get()`, `.scope(value)` (sync
  `@contextmanager`), `.ascope(value)` (`@asynccontextmanager`), always
  token-reset in `finally`. This is the *generalization* of
  `tenant_context()`/`correlation_context()`, which are **not** rewritten
  onto it — they remain the precedent it documents.
- `RequestContext` (`context/request.py`) — one `@dataclass(frozen=True)`
  with `locale: str | None`, `timezone: ZoneInfo | None`, `extras: Mapping[str,
  str]`, held in exactly **one** `AmbientVar[RequestContext]`.
  `request_context(locale=..., timezone=...)` **merges** onto the enclosing
  context — setting a locale never blanks an already-resolved timezone.
- `resolve_precedence(candidates: Sequence[tuple[str, T | None]]) ->
  Resolved[T] | None` (`context/precedence.py`) — pure, synchronous, no I/O.
  Explicit `(source, value)` pairs (not an `or`-chain, which would skip a
  legitimate falsy value like `""`/`0`) and returns *which* source won —
  `Resolved.source` turns "why did this user get German?" into one DEBUG
  log line instead of a debugging session. Both I2's `resolve_locale()` and
  T1's `resolve_timezone()` are thin consumers of this one function.

**Tenant is deliberately absent from `RequestContext`.**
`varco_core.service.tenant.current_tenant()` stays the single source of
truth — two places to ask "who is the tenant" is how they diverge, and
`tenant_context()` is already load-bearing across `TenantAwareService`,
`tenancy_cache_key()`, RLS, the DLQ tenant stamp, and the audit trail.
Composition with the tenant is by *ordering*
(`LocalizationMiddleware` runs after `TenantResolutionMiddleware`), never by
containment.

## `MessageCatalog` — the ABC and its three implementations

```python
class MessageCatalog(abc.ABC):
    def get_message(self, key: str, locale: str) -> str | None: ...  # abstract
    def format_message(self, key, locale, params=None) -> str | None: ...  # default: str.format_map
    def available_locales(self) -> frozenset[str]: ...
    async def start(self) -> None: ...  # blocking I/O lives here, never lazily
    async def stop(self) -> None: ...
```

| Implementation | Use case | I/O |
|---|---|---|
| `NullMessageCatalog` | The DI default — `get_message` always `None` | Zero |
| `DictMessageCatalog` | Tests, small apps — `{locale: {key: template}}` | Zero |
| `GettextMessageCatalog` | Production default | Blocking `.mo` load in `start()` |

`format_message()`'s default uses a `__missing__`-tolerant mapping — a
missing interpolation parameter leaves the literal `{name}` placeholder
visible instead of raising `KeyError`, because this code runs inside the
exception-rendering path, where raising would turn a 404 into a 500.

### Why stdlib `gettext`, not Babel/PyICU/MessageFormat 2.0/Fluent

Settled by design brief 002 (`design/i18n-tz-framework/research/002-*.md`):

- **Fluent** (`fluent.runtime` 0.4.0, Python 3.6–3.9 only) — incompatible
  with varco's `>=3.12` floor. Rejected outright.
- **MessageFormat 2.0** — the spec is stable, but the only Python
  implementation is 0.1.x and no major framework has adopted it. Rejected as
  default; a documented ABC implementation for the day it matures.
- **PyICU** — viable (wheels exist for 3.12/3.13), the only route to
  gender/ordinal selectors, but drags `libicu` into every install for a
  capability most services never use. Rejected as default; documented as
  the first extension point to implement when you need it.
- **Babel + gettext** is the ecosystem's actual answer, but only half of it
  is a *runtime* concern — Babel's contribution is `pybabel
  extract/init/compile`, a **build-time** tool. `GettextMessageCatalog`
  therefore reads `.mo` files with pure stdlib `gettext`, adding **zero
  runtime dependencies to `varco_core`**, while the docs below point at
  `pybabel` for authoring.

### The authoring recipe (documented, not vendored — RD-7)

varco ships the machinery and zero `.po`/`.mo` files. `pybabel` is a `pip
install babel` away, used only at build time:

```bash
pip install babel   # dev-time only — never a varco_core runtime dependency

# 1. Extract msgids from your source into a .pot template
pybabel extract -o messages.pot --input-dirs=.

# 2. Initialize a new locale
pybabel init -i messages.pot -d locale -l fr

# 3. ... translate locale/fr/LC_MESSAGES/messages.po by hand or with a TMS ...

# 4. Compile to the .mo GettextMessageCatalog reads
pybabel compile -d locale
```

```python
from varco_core.i18n.gettext_catalog import GettextMessageCatalog

catalog = GettextMessageCatalog("locale", domain="messages", locales=("en", "fr"))
await catalog.start()  # blocking .mo load — call once at app startup, never per-request
```

A locale with no `.mo` file is skipped with one WARNING; `start()` never
raises for a missing locale.

### Thread/async safety — no process-global `activate()`

Flask-Babel's `force_locale` leaking across requests (issue #117) is a
documented hazard of a process-global "active locale". varco has **no**
global `activate()` call anywhere: the locale lives only in X1's
request-scoped `ContextVar`, and a `GettextMessageCatalog` is immutable
(post-`start()`) — every lookup takes `locale` as an explicit argument, so
concurrent requests for different locales never interfere.

### Plurals

`GettextMessageCatalog.format_message(key, locale, params={"count": n, ...})`
routes through `translation.ngettext(key, key, count)` when `params["count"]`
is an `int` (and not a `bool`) — CLDR plural forms for free when the `.mo`'s
`msgid`/`msgid_plural` pair was compiled from `key`. Every other
implementation's default is the simple `str.format_map` path.

## The precedence chain (I2's five sources)

```
?lang= (query_param) -> user_profile -> tenant_default -> Accept-Language -> fallback
```

```python
resolved = await resolve_locale(
    query_param=request.query_params.get("lang"),
    user_profile_locale=auth_ctx_locale_claim,
    tenant_id=current_tenant(),
    tenant_defaults_provider=provider,  # awaited only if tenant_id is set
    accept_language_header=request.headers.get("accept-language"),
    supported_locales=("en", "fr"),
    default_locale="en",
)
# resolved.value == "fr", resolved.source == "accept_language"
```

Only locales in `supported_locales` are ever returned — an unsupported
explicit `?lang=de` falls through to the next source rather than 400ing.

**Deliberate deviation from brief 002's ordering.** Brief 002's Librarian
lists a *stored* preference before `?lang=`. varco puts `?lang=` first:
brief 001's own "Precedence hierarchy" section groups explicit user choice
first, and an explicit per-request override must not be silently overruled
by a stale stored profile.

### `Accept-Language` — hand-rolled RFC 4647 §3.4 Lookup

`varco_core.i18n.negotiation` implements Lookup (not Basic Filtering, RFC
4647 §3.3.1, which is what WebOb implements) by hand — no standard Python
library does Lookup; `language_tags` only validates BCP 47 syntax. For each
candidate tag (highest `q` first), it progressively truncates at `-`
boundaries (`fr-CA-x-foo` → `fr-CA` → `fr`), skipping a truncation that
would leave a single-character subtag, and returns the first truncation
present in `supported`. `q=0` entries are excluded per RFC 9110 §12.5.4.

## RD-2 — per-tenant defaults, without a `varco_tenants` schema change

`TenantDescriptor` gains **no** locale/timezone fields — that would be an
Alembic revision + a Beanie document change + a migration obligation for
every deployment, for a value most tenants never set. Instead:

```python
from varco_core.context.defaults import TenantDefaultsProvider, TenantLocalizationDefaults


class MyTenantDefaults:
    async def defaults_for(self, tenant_id: str) -> TenantLocalizationDefaults:
        row = await my_tenant_settings_repo.get(tenant_id)
        return TenantLocalizationDefaults(locale=row.locale, timezone=row.timezone)
```

Ships with `NullTenantDefaults()` (the DI default, zero I/O, returns
`(None, None)` for every tenant) and `StaticTenantDefaults({...})` for
tests/small deployments. varco does **not** cache the result implicitly —
wrap the call in your own cache if you need one; an implicit per-tenant
cache with no invalidation path is a support ticket waiting to happen.

## `LocalizationMiddleware` — see the dedicated ordering section

`varco_fastapi.middleware.localization.LocalizationMiddleware` resolves
locale *and/or* timezone in one ASGI pass — see
`technical_docs/features/timezone-handling.md`'s "Wiring: LocalizationMiddleware
and its ordering hazard" section for the full request-order diagram and the
`request.state` mirror caveat (they are symmetric for I2/T1, documented
once there to avoid duplication).

`LocalizationModule(i18n=I18nSettings(enabled=True, ...))` (part of `default_modules()` —
replace the default instance via `default_modules(exclude=(LocalizationModule,))`) resolves the
DI-bound `MessageCatalog` (default: `NullMessageCatalog`, lowest DI
priority so any app-provided catalog wins) and, only when both `enabled`
and a non-`None` catalog are found, its `start()` calls
`catalog.start()`/`catalog.stop()` around the ASGI lifespan (`I18nLifecycle`, unchanged).

⚠️ **`i18n=`/`timezone=` are typed `Any | None` on `LocalizationModule`, not
`I18nSettings | None`/`TimezoneSettings | None`.** The implementation
accepts anything and falls back to a fresh default settings object with
`isinstance()`-shaped resolution (`self._i18n if self._i18n is not None else
I18nSettings()`) rather than a type-checked keyword. Pass an actual
`I18nSettings`/`TimezoneSettings` instance — passing anything else silently
behaves as if you passed nothing.

## RD-6 — locale is never an implicit cache-key component

```python
from varco_core.i18n.cache_key import localization_cache_key

key = localization_cache_key("product:42", locale=True)
# "product:42:locale:fr" — raises RuntimeError if no ambient locale is resolved
```

Caching an `fr`-rendered body under a key that doesn't mention `fr`, then
serving it to an `en` client, is the i18n analogue of a cross-tenant cache
leak — and easier to hit, because localization happens at render time, far
from the cache call. varco never silently namespaces a key by locale (that
would cold-start every cache); `localization_cache_key()` fails closed
instead, mirroring `tenancy_cache_key()`'s shape exactly. **Prefer caching
the unlocalized representation and localizing at render time** wherever
possible; reach for this function only when the cached artifact is itself
already localized.

## Extension points not shipped (D-1)

`MessageCatalog` is the seam for PyICU (gender/ordinal selectors), MF2
(`messageformat2`, once it's past 0.1.x), or Fluent (once it supports
3.12+) — implement the ABC, do not add a runtime dependency to
`varco_core`.

## RD-7 — framework responsibility line

varco owns the `MessageCatalog` ABC + a default implementation, content
negotiation, and request-scoped locale context. varco does **not** own
message authoring, catalog/translation management, or translatable-entity
content design (per-locale product descriptions, etc.) — those are app-side
concerns, explicitly parked in the backlog.

## See also

- `technical_docs/features/error-taxonomy-and-i18n.md` — `message_key`,
  the catalogue, `message_resolver`'s current wiring status.
- `technical_docs/features/timezone-handling.md` — T1, the sibling X1
  consumer, and `LocalizationMiddleware`'s ordering hazard in full.

## Pitfalls

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **`tenant_id` expected in `RequestContext`** | `AttributeError`, or two disagreeing answers to "who is the tenant" | `RequestContext` deliberately holds only `locale`/`timezone`/`extras` (Plan 011 / D-6) — `TenantAwareService`, RLS, `tenancy_cache_key()`, the DLQ stamp and the audit trail all read `current_tenant()`, and a second source of truth is how they diverge | Call `current_tenant()`; compose by *ordering* (`LocalizationMiddleware` is the innermost built-in layer, so any app-supplied `TenantResolutionMiddleware` via `extra_middleware=` always dispatches first), never by containment |
| **Localized response cached and served to the wrong locale** | A `fr` body is returned to an `en` client | The cache key did not mention the locale — the i18n analogue of the cross-tenant cache leak, and easier to hit because localization is applied at render time, far from the cache call | Cache the **unlocalized** representation and localize at render time; where the cached artifact is itself localized, build the key with `localization_cache_key(base, locale=True)`, which fails closed (`RuntimeError`) with no ambient locale, exactly like `tenancy_cache_key()` |
| **`?lang=xx` silently ignored** | No 400, the response comes back in the fallback locale | `xx` is not in `I18nSettings.supported_locales` — by design, an unsupported explicit override falls through to the next precedence source rather than erroring | Add the locale to `supported_locales`, or expect the fallthrough — this is deliberate, not a bug |
| **`Content-Language` header missing** | I18n appears to do nothing on an otherwise-working response | `I18nSettings.enabled=False` (the default), or `set_content_language=False` | Set `VARCO_I18N_ENABLED=true` (and check `set_content_language`) |
