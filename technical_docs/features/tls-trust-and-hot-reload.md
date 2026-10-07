# TLS trust and hot reload — `varco_core.tls`

Plan 026 (BACKLOG 3.1, rows **T3** must/L, **T7** nice/S, **T5** should/S). Closes: "there are
two overlapping TLS trust models in different layers, neither is a superset of the other, and
neither can be reloaded without a process restart."

## The two-model history, and why they merged

Before this plan, "what TLS should I trust?" had two independent answers:

- `varco_core.connection.ssl.SSLConfig` — a pydantic `BaseModel`, embedded as a nested field in
  every `ConnectionSettings` subclass, loaded from env vars via `env_nested_delimiter`. Had the
  `verify=False` escape hatch and `check_hostname`, but no `include_system_cas` toggle and no
  `bytes`-CA support.
- `varco_fastapi.auth.trust_store.TrustStore` — a frozen dataclass, a runtime capability object
  used by the auth layer. Had `include_system_cas` and in-memory `bytes` CAs, but always
  verified (no `verify=False`) and lived in `varco_fastapi`, unreachable from a broker backend
  (`varco_kafka`/`varco_redis`/`varco_sa`) that has no `varco_fastapi` dependency.

The one bridge between them, `HttpConnectionSettings.to_trust_store()`
(`varco_fastapi/varco_fastapi/connection.py:310-340`), was one-directional and **silently
dropped `verify=False`** — a documented caveat, never fixed, because fixing it required a type
that could carry every field either predecessor had.

`varco_core.tls.TrustStore` is that type: a strict superset (§D-T3-model in
`plans/026-tls-unification-and-reloading-truststore.md`), living in `varco_core` so any backend
can reach it, with a lossless bridge in both directions:

```python
from varco_core.connection.ssl import SSLConfig
from varco_core.tls import TrustStore

# SSLConfig -> TrustStore, carrying BOTH verify and check_hostname (lossless)
store = SSLConfig(verify=False, check_hostname=False).to_trust_store()

# TrustStore -> SSLConfig (lossy in two directions — see the docstring: bytes ca_cert and
# include_system_cas=False are not representable in SSLConfig)
cfg = store.to_ssl_config()
```

`varco_fastapi.auth.trust_store.TrustStore` still exists — as a deprecated subclass of
`varco_core.tls.TrustStore` that pins its exact 3.0 semantics (non-recursive scan,
`("*.pem", "*.crt")` patterns, deferred mTLS-pairing check). See "The deprecation shim" below.

## The field table (§D-T3-model)

`varco_core.tls.TrustStore` (`varco_core/varco_core/tls/store.py`), `@dataclass(frozen=True)`:

| Field | Type | Default | Notes |
|---|---|---|---|
| `ca_cert` | `Path \| bytes \| None` | `None` | `bytes` loads via `cadata=`; `Path` via `cafile=` |
| `ca_folders` | `Path \| Sequence[Path] \| None` | `None` | one path or many; normalised to a tuple (or `None`) in `__post_init__` |
| `cert_patterns` | `tuple[str, ...]` | `CERT_FILE_PATTERNS` (`("*.pem", "*.crt", "*.cer")`) | see "The cert-glob helper" below |
| `recursive` | `bool` | **`True`** | recursive folder scan — new default, **on this type only** |
| `client_cert` / `client_key` | `Path \| None` | `None` | mTLS client identity; both or neither |
| `include_system_cas` | `bool` | `True` | whether to layer on top of `ssl.create_default_context()` |
| `verify` | `bool` | `True` | `False` → `CERT_NONE`, the escape hatch `SSLConfig` had and the old `TrustStore` did not |
| `check_hostname` | `bool` | `True` | requires `verify=True` |

Validation is **eager**, at construction — `check_hostname=True` with `verify=False` raises
`ValueError`, and exactly one of `client_cert`/`client_key` set raises `ValueError`. This is
*stricter* than the pre-3.1 `varco_fastapi.auth.TrustStore`, which deferred the mTLS check to
`build_ssl_context()` — the deprecation subclass keeps deferring it (frozen behaviour).

```python
from pathlib import Path
from varco_core.tls import TrustStore

# Private CA + mTLS, recursive folder scan (this type's default)
store = TrustStore(
    ca_folders=Path("/etc/ssl/private-ca"),
    client_cert=Path("/etc/ssl/client.crt"),
    client_key=Path("/etc/ssl/client.key"),
)
ctx = store.build_ssl_context()
```

## The cert-glob helper (§D-T7)

Before this plan, four call sites each answered "what is a certificate file in this folder?"
differently and silently: `SSLConfig`/the old `TrustStore` globbed `*.pem` + `*.crt`;
`PemFolderSource` globbed `*.pem` only. A `.cer` file dropped into a CA folder was silently
ignored — no error, no log.

`varco_core.tls.discovery.iter_cert_files(root, *, patterns, recursive=False)` is now the one
implementation all four sites call, and it fixes the **named** defect (silence) without
widening what any of the three pre-existing sites trusts or loads: each site keeps its own
`patterns` default (`SSLConfig` and the legacy `varco_fastapi` shim: `("*.pem", "*.crt")`;
`PemFolderSource`: `("*.pem",)`). A file matching the wider `CERT_FILE_PATTERNS` set but not a
given site's own patterns is skipped **and logged once at WARNING** per `(root, patterns)` per
process, naming the skipped file(s).

`varco_core.tls.TrustStore` is the one type that defaults to the wider `CERT_FILE_PATTERNS` —
safe because it has no existing deployments to widen. Opt an existing `SSLConfig` into the
wider set explicitly:

```python
from varco_core.connection.ssl import SSLConfig
from varco_core.tls import CERT_FILE_PATTERNS

cfg = SSLConfig(ca_folder=Path("/etc/ssl/ca"), cert_patterns=CERT_FILE_PATTERNS, recursive=True)
```

`PemFolderSource` was **not** widened by default for a sharper reason than "consistency": that
folder holds JWT *signing keys*, one file per `kid`. Feeding it an X.509 certificate would at
best mint a surprise `kid`, at worst raise `KeyLoadError` and take issuer loading down — so the
one call site where the narrow default looks most like a bug is the one where "fixing" it by
widening is most dangerous.

## Reload — mutate vs. swap, chosen per event (§D-T3-reload)

`ReloadingTrustStore` (`varco_core/varco_core/tls/reload.py`) composes Plan 025's
`ReloadableResource[ssl.SSLContext]` (keep-last-good swap semantics) and `AbstractPathWatcher`
(filesystem change detection) — it does not inherit from either.

```python
from pathlib import Path
from varco_core.tls import TrustStore, ReloadingTrustStore

spec = TrustStore(ca_folders=Path("/etc/ssl/private-ca"))
store = ReloadingTrustStore(spec)

async with store:
    ctx = store.context  # ssl.SSLContext, never None after start()
```

`ReloadStrategy` (`AUTO` default / `MUTATE` / `SWAP`) picks the branch from the watcher's own
per-file diff:

- **A settled batch is additions only** → `MUTATE`: `load_verify_locations()` is called on the
  *live* `ssl.SSLContext` object. `ssl.SSLContext` has **no unload API** — brief
  001 §2 documents that "already-established TLS connections see no change — only NEW
  handshakes use the updated cert" — so mutation can only ever *add* trust, never revoke it.
  Cheap: no rebuild, no swap, every client already holding a reference to `store.context` picks
  up the new CA with zero coordination. This is the 6-day-cert-renewal common path.
- **Anything was removed or replaced** → `SWAP`: a fresh context is built from scratch and the
  reference is swapped, `generation` bumps by one, and `subscribe()` fires so pooled clients can
  rebuild.

⚠️ **Established connections keep whatever context they negotiated with either way** — `SWAP`
cannot revoke trust for a connection already open on the *old* context object. This is a
consequence of how TLS handshakes work, not something varco can fix from a library.

```python
def rebuild_pool(new_ctx: ssl.SSLContext) -> None:
    ...  # tell your connection pool to use new_ctx for new connections


unsubscribe = store.subscribe(rebuild_pool)  # fires only on SWAP, never on MUTATE
```

`AUTO` is a heuristic: a CA replaced by a file *rename* (same bytes, new name) is seen by the
watcher's diff as ADDED+REMOVED in one batch, which lands on the (safe, expensive) `SWAP`
branch rather than the cheap one it might logically deserve. Force a strategy explicitly if you
need one behaviour unconditionally: `ReloadingTrustStore(spec, strategy=ReloadStrategy.SWAP)`.

## `SSLConfig` does not gain reload (§D-T3-oq2)

`SSLConfig` stays a frozen pydantic settings fragment — it does not grow `start()`/`stop()`.
`SSLConfig` is constructed by pydantic-settings, potentially many times, at import/DI time;
making every settings object a background-task owner would be a lifecycle leak by design.
`ReloadingTrustStore` is the **only** reloadable path. A broker backend holding an `SSLConfig`
converts: `ReloadingTrustStore(cfg.to_trust_store())`.

## Owning the reload task (§D-T3-oq3)

`ReloadingTrustStore` owns its own background task via `start()`/`stop()` and is an `async`
context manager — there is **no scanned `@Configuration` in `varco_core`**. A
`container.scan("varco_core", recursive=True)` (a documented, in-use pattern — see the DI
section below) auto-activates every scanned `@Configuration`, so one in `varco_core.tls` would
start a filesystem watcher in *every* app that scans `varco_core` — the opposite of the locked
"Auto-injection: explicit, opt-in, never implicit" decision.

`start()`/`stop()` structurally satisfy `varco_fastapi.lifespan.AbstractLifecycle` (a
`runtime_checkable` Protocol), so a FastAPI app registers an already-constructed, already-owned
store with zero import from `varco_core` into `varco_fastapi`:

```python
from varco_fastapi.lifespan import VarcoLifespan
from varco_core.tls import TrustStore, ReloadingTrustStore

store = ReloadingTrustStore(TrustStore(ca_folders=Path("/etc/ssl/private-ca")))

lifespan = VarcoLifespan()
lifespan.register(store)  # starts/stops the watcher for you
```

Non-FastAPI consumers use `async with store:` or call `start()`/`stop()` directly. An unstarted
store raises `ResourceNotLoadedError` on `.context` access rather than silently serving nothing.

### DI wiring

`varco_core.tls.bind_trust_store(container, store)` is the only DI-wiring surface this package
exposes — it registers an **already-constructed** `ReloadingTrustStore` (and its `TrustStore`
spec) as DI singletons and returns the same `container` (Plan 046 §D-bind — both bindings are
now one-liners over `varco_core.di.bind_instance()`). It has **no lifecycle side effect** — the
caller still owns `start()`/`stop()`. `bind_trust_store` requires an explicit `container`
argument (never a process-global default) and raises `varco_core.di.AlreadyBoundError` if a
non-fallback `ReloadingTrustStore`/`TrustStore` binding already exists — call it once, or use
providify's `di_overrides`/`ContainerOverrides` in a test to replace it:

```python
from varco_core.tls import TrustStore, ReloadingTrustStore, bind_trust_store

spec = TrustStore(ca_folders=Path("/etc/ssl/private-ca"))
store = ReloadingTrustStore(spec)
bind_trust_store(container, store)
await store.start()  # the caller's responsibility, not bind_trust_store's
```

**`varco_core.tls.TrustStoreModule` (Plan 050 / DI-10, re-exported from `varco_core.tls.module`
as of the Plan 050/051 follow-up — both paths resolve to the identical class)** is the lifecycle
*owner* built on top of `bind_trust_store` — it still calls `bind_trust_store` under the hood
(unchanged, no lifecycle side effect of its own), but also runs the store's `start()`/`stop()` at
the `INFRA` phase, so a hand-rolled `lifespan.register(store)` call is no longer needed in a
`create_varco_app`-built app:

```python
from varco_core.tls import TrustStore, TrustStoreModule
from varco_fastapi import default_modules

app = create_varco_app(
    container,
    modules=[*default_modules(), TrustStoreModule(spec=TrustStore(ca_folders=Path("/etc/ssl/private-ca")))],
)
```

`TrustStoreModule(store=already_built_store)` is the other shape — mutually exclusive with
`spec=`. It is `module_owned` in `varco_core`'s `di_wiring` conformance subclass (the disposer
check reads `module_owned`, never expecting a `@Disposes(ReloadingTrustStore)`). Same "never a
scanned `@Configuration`" rule as `bind_trust_store` itself — see its own module docstring.

## `SSL_CERT_FILE` / `SSL_CERT_DIR` — additive, not OpenSSL-compatible (§D-T3-env)

`TrustStore.from_env()` reads the existing `VARCO_*` names **and** the two OpenSSL-standard
names, **additively on top of system CAs**:

⚠️ **This diverges from OpenSSL/`uv`/`requests` semantics on purpose.** OpenSSL, `uv`, and
`requests` treat a non-empty `SSL_CERT_FILE`/`SSL_CERT_DIR` as *replacing* the default trust
store entirely — only the specified certs are trusted. varco does not: both are merged
additively, alongside `include_system_cas`'s default of `True`, because silently dropping the
system store just because a sidecar exported one env var is a production-outage shape in a
framework whose every other CA mechanism is additive. If you need the OpenSSL replace-semantics,
set `include_system_cas=False` explicitly.

## An SSL context for the two URL-based issuer sources (§D-T5)

`JwksUrlSource` and `OidcDiscoverySource` used to fetch with a bare
`urllib.request.urlopen(url, timeout=...)` and no `context=` — unverifiable against an internal
PKI or an intercepting corporate proxy without process-wide env vars. Both now take a
keyword-only `ssl_context: ssl.SSLContext | None = None`:

```python
from varco_core.authority.sources import JwksUrlSource, OidcDiscoverySource
from varco_core.tls import TrustStore

ctx = TrustStore(ca_folders=Path("/etc/ssl/internal-pki")).build_ssl_context()

jwks = JwksUrlSource("https://idp.internal/.well-known/jwks.json", ssl_context=ctx)
oidc = OidcDiscoverySource("https://idp.internal", ssl_context=ctx)
```

`None` (the default) means "stdlib default context" — byte-identical to pre-Plan-026 behaviour.
`IssuerSourceFactory.from_string(..., ssl_context=)` and `AuthorizationConfig.to_registry(
ssl_context=)` forward it; `OidcDiscoverySource` threads the *same* context to both its own
discovery-document fetch and the delegate `JwksUrlSource` it constructs, because an internal PKI
issuer needs the same trust for its `.well-known` document and its JWKS endpoint.

`TrustedIssuerRegistry.from_env()` builds a context from `TrustStore.from_env()` **only when at
least one of the CA-triggering env vars is set** (`VARCO_TLS_TRUST_STORE_DIR`, `VARCO_TLS_CA_CERT`,
`VARCO_TLS_CLIENT_CERT`, `VARCO_TLS_CLIENT_KEY` — renamed in 4.0 from the legacy, non-namespaced
`VARCO_TRUST_STORE_DIR`/`VARCO_CA_CERT`/`VARCO_CLIENT_CERT`/`VARCO_CLIENT_KEY`, Plan 048 / DI-6 —
plus `SSL_CERT_FILE`, `SSL_CERT_DIR`) — with none of them
set, `ssl_context` stays `None`, so `from_env()`'s default behaviour is unchanged.

⚠️ **Out of scope: this fixes verification, not proxy handling.** `ssl_context=` has no bearing
on `HTTP_PROXY`/`HTTPS_PROXY` — a corporate proxy configured that way still applies exactly as
before. Do not read this as "JWKS through a proxy now works."

## The deprecation shim (§D-T3-oq1)

`varco_fastapi.auth.trust_store.TrustStore` is now
`@dataclass(frozen=True) class TrustStore(varco_core.tls.TrustStore)` — a subclass, not a plain
re-export alias, because the old and new names do **not** denote the same behaviour: the base
type is recursive by default and globs a wider cert set. Aliasing would silently give every
existing `varco_fastapi.auth.TrustStore` construction recursive, wider cert discovery on
upgrade to 3.1 — exactly what the "Cert search" locked decision exists to prevent. The subclass
instead pins the exact 3.0 defaults (`recursive=False`, `cert_patterns=("*.pem", "*.crt")`, and
a deferred — not eager — mTLS-pairing check) and emits a `DeprecationWarning` at construction
(not at import — `from varco_fastapi import TrustStore` alone does not warn).

```python
import warnings
from varco_fastapi.auth import TrustStore  # no warning yet

with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    ts = TrustStore(ca_cert=Path("/etc/ssl/ca.pem"))  # DeprecationWarning fires here
```

`isinstance(legacy_instance, varco_core.tls.TrustStore)` is `True` — every new API accepts an
old object. The reverse is not: `isinstance(varco_core.tls.TrustStore(), varco_fastapi.auth.
TrustStore)` is `False`. This asymmetry is inherent to a subclass shim, not a bug — see the
Pitfalls table below. `HttpConnectionSettings.to_trust_store()` (`varco_fastapi.connection`)
deliberately keeps returning the legacy type — see `SSLConfig.to_trust_store()` above for the
lossless replacement.

Removed in 5.0.0 (deprecated since 3.1.0; floor: 2027-09-05).

## Encrypted private keys (Plan 027 / T6a, §D-T6-password)

Before Plan 027, both predecessor implementations called
`ssl.SSLContext.load_cert_chain(certfile=, keyfile=)` with no `password=` argument
(`varco_core/varco_core/connection/ssl.py:271-274` pre-027,
`varco_fastapi/varco_fastapi/auth/trust_store.py:244-247`) — an encrypted client key
(`-----BEGIN ENCRYPTED PRIVATE KEY-----`) made `build_ssl_context()` raise, or on some
platforms prompt on a TTY.

`TrustStore.key_password: str | bytes | Callable[[], str | bytes] | None` closes this — it is
passed straight through to `ssl.SSLContext.load_cert_chain(..., password=...)`; varco never
decrypts the key itself:

```python
from pathlib import Path
from varco_core.tls import TrustStore

# str/bytes — held in memory for the store's lifetime
store = TrustStore(
    client_cert=Path("/etc/ssl/client.crt"),
    client_key=Path("/etc/ssl/client-encrypted.key"),
    key_password="s3cret",
)

# Callable — recommended: nothing is held in memory until build_ssl_context() actually needs it
store = TrustStore(
    client_cert=Path("/etc/ssl/client.crt"),
    client_key=Path("/etc/ssl/client-encrypted.key"),
    key_password=lambda: vault_client.read_secret("client-key-password"),
)
ctx = store.build_ssl_context()  # the callable is invoked here, once, not at construction
```

`key_password` (and `pkcs12_password` below) are declared `field(repr=False)` — neither prints
through the dataclass's default `repr()`, so a `repr(store)` in a log line or a DI debug dump
never leaks the secret. `key_password` set without `client_key` raises `ValueError` at
construction — a password with nothing to decrypt is always a misconfiguration.

⚠️ **Implementation deviation from a literal `password=key_password`**: `build_ssl_context()`
never passes `password=None` to `load_cert_chain`, even when `key_password` is unset. OpenSSL's
own behaviour for an *encrypted* key with no password callback is to fall back to an
interactive console prompt — exactly the "or worse, prompt on a TTY" failure mode this field
exists to prevent. Instead, an unset `key_password` is substituted with `password=b""`: an
*unencrypted* key still loads correctly (OpenSSL ignores an unused password), and an
*encrypted* key with no `key_password` now fails deterministically with `ssl.SSLError` ("PEM
lib") instead of ever touching a console. This is a documented behaviour choice, not what a
naive reading of §D-T6-password's design table would produce.

**Not a settings field.** `SSLConfig` (the pydantic settings fragment populated from env vars)
deliberately does **not** gain `key_password` — a secret field on a `BaseModel` lands in
`model_dump()`, DI debug logging, and any settings echo. Convert via `SSLConfig.to_trust_store()`
and set `key_password` in code instead.

## PKCS#12 / `.pfx` bundles (Plan 027 / T6b, §D-T6-pkcs12)

`TrustStore.pkcs12_file` + `pkcs12_password` ingest a PKCS#12 client-identity bundle (leaf cert
+ private key + optional CA chain) as an alternative to `client_cert`/`client_key`, using
`cryptography` — already a hard `varco_core` dependency — with **no new dependency** and no
`httpx-pkcs12`/`requests-pkcs12` shim:

```python
from pathlib import Path
from varco_core.tls import TrustStore

store = TrustStore(
    pkcs12_file=Path("/etc/ssl/client-identity.p12"),
    pkcs12_password="s3cret",       # or bytes, or a zero-arg callable, same shape as key_password
    pkcs12_trust_ca=False,          # default — see below
)
ctx = store.build_ssl_context()
```

`pkcs12_file` is **mutually exclusive** with `client_cert`/`client_key` — `ValueError` at
construction if both are set (pick exactly one way to supply the mTLS client identity).

**The bundle's own CAs are not trust anchors by default.** A PKCS#12 client-identity bundle
often carries the issuing CA(s) alongside the leaf cert so the whole chain can be sent to the
peer, but those CAs are trusted for *server* verification only if `pkcs12_trust_ca=True` is
passed explicitly — the default `False` means "load the identity, don't widen what I trust".

**The temp-file window.** `ssl.SSLContext.load_cert_chain` only accepts filenames — there is no
in-memory equivalent — so `varco_core.tls.pkcs12.materialize_chain()` decodes the bundle in
memory (`load_pkcs12_identity()`, PEM `bytes` only, no filesystem touch), then briefly writes
the private key + leaf cert PEM chain to disk before `load_cert_chain` reads it back:

- **`/dev/shm` (tmpfs) is preferred** when present and writable — legitimate because this
  cycle is Linux-only; falls back to the system temp dir (`tempfile.mkdtemp()`'s default)
  otherwise, e.g. `/dev/shm` full or read-only. Never fails the handshake over a tmpfs problem.
- The containing directory is a fresh `tempfile.mkdtemp()` (mode `0700`); the chain file itself
  is opened with `os.open(..., O_CREAT | O_EXCL | O_WRONLY, 0o600)` — `O_EXCL` refuses to
  follow or overwrite an existing path (no TOCTOU symlink race), and the explicit `0600` mode
  guards against a permissive umask widening it inside the `0700` directory.
- Both the file and its directory are removed in a `finally` — **on both the success path and
  every failure path**, including a wrong-password `Pkcs12LoadError` or a malformed-cert
  `ssl.SSLError` from `load_cert_chain` itself. The plaintext key exists on a filesystem for
  exactly the duration of one `materialize_chain()` block — never longer, and never logged.

A wrong password or a corrupt bundle raises `Pkcs12LoadError` (a `ValueError` subclass), not a
raw `cryptography` traceback. A bundle with no private key (trust-only) also raises
`Pkcs12LoadError`, naming `ca_cert`/`ca_folders` as the right tool instead.

## Client injection — httpx, aiohttp, urllib3, requests (Plan 027 / T4b, §D-T4-adapters)

Four thin, zero-hard-dependency adapters convert a `TrustStore`/`ReloadingTrustStore` into the
shape each mainstream Python HTTP client wants. Every import is inside the function body that
needs it (`varco_core/varco_core/tls/clients.py`) — importing `varco_core.tls` never imports
httpx/aiohttp/urllib3/requests, mechanically enforced by
`varco_core/tests/test_tls_no_hard_client_deps.py` (an `ast` walk plus a subprocess
`sys.modules` check).

| Adapter | Target API | Version floor and why |
|---|---|---|
| `to_httpx_verify()` → `ssl.SSLContext` | `httpx.Client(verify=ctx)` / `AsyncClient(verify=ctx)` | httpx 0.28+ — `verify=` accepts an `ssl.SSLContext` directly; `cert=` is **deprecated** as of 0.28 in favour of building the context yourself |
| `to_aiohttp_connector(**kw)` → `aiohttp.TCPConnector` | `ClientSession(connector=...)` | aiohttp 3.14+ — `TCPConnector(ssl=ctx)`; mutually exclusive with the older `verify_ssl=`/`fingerprint=` kwargs, never set here |
| `to_urllib3_poolmanager(**kw)` → `urllib3.PoolManager` | `PoolManager(ssl_context=ctx)` | urllib3 v2.x only — v1.26 is EOL and has a different `ssl_context=` story |
| `to_requests_adapter()` → `requests.adapters.HTTPAdapter` subclass | `session.mount("https://", adapter)` | requests 2.32.3+ — the release that fixed a custom `ssl.SSLContext` passed through a custom `HTTPAdapter` subclass being silently ignored by urllib3's pool-manager plumbing |

All four are module-level functions in `varco_core.tls.clients`, and are also exposed as
delegating methods directly on `TrustStore` and `ReloadingTrustStore` for discoverability:

```python
from pathlib import Path
from varco_core.tls import TrustStore

store = TrustStore(ca_folders=Path("/etc/ssl/private-ca"))

import httpx
client = httpx.Client(verify=store.to_httpx_verify())

import aiohttp
connector = await store.to_aiohttp_connector()   # must be built inside a running event loop
async with aiohttp.ClientSession(connector=connector) as session:
    ...

pool = store.to_urllib3_poolmanager()

import requests
session = requests.Session()
session.mount("https://", store.to_requests_adapter())
```

A missing library raises `MissingClientDependencyError` (an `ImportError` subclass) naming the
`pip install` package — the same shape as `varco_core.watch`'s `MissingWatchDependencyError`.

**Reload interaction — read at call time, always.** Every adapter reads the store's context at
the moment it runs, never caching it:

- Under `ReloadStrategy.MUTATE`, a client built from these adapters keeps working across a
  rotation with **no action** — the `ssl.SSLContext` object it holds is the exact object
  `ReloadingTrustStore` mutates in place.
- Under `SWAP`, an already-built client/connector/adapter holds the **old** context forever —
  rebuilding is the caller's job. `ReloadingTrustStore.subscribe(cb)` is the hook.
- Either way, an **already-established** TLS connection never sees a rotation — only a new
  handshake does (brief 001 §2).

## `install_process_trust()` — the acknowledged, explicit, never-automatic global (Plan 027 / T4c, §D-T4-install)

`varco_core.tls.install_process_trust(store, *, acknowledge_global_mutation)` patches the
private stdlib hook `ssl._create_default_https_context` so that any HTTP client built **after**
the call, via a `ssl.create_default_context()`-consuming path, picks up `store`'s trust
configuration process-wide — the "inject into everything at once" capability, made explicit and
reversible instead of automatic:

```python
from pathlib import Path
from varco_core.tls import TrustStore, install_process_trust

store = TrustStore(ca_folders=Path("/etc/ssl/private-ca"))

# Call ONCE, at process startup, before constructing any HTTP client:
handle = install_process_trust(store, acknowledge_global_mutation=True)
...
handle.restore()  # or: use install_process_trust(...) as a context manager
```

⚠️ **varco itself never calls this function** — `install_process_trust` is an *application*-level
decision only (brief 001 §3: libraries must not inject a trust store on their own; only the
application may). `acknowledge_global_mutation=True` is not a convenience default: omitting it
raises `ValueError` with no mutation, so the danger is always legible in a diff. It only affects
contexts created **after** the call and only via `create_default_context()`-style paths — a
client that already built its context, or one that constructs `ssl.SSLContext(...)` directly, is
unaffected either way, and there is no way to detect that from inside the function.
`ssl._create_default_https_context` is a private CPython name; `install_process_trust` asserts
it exists before touching anything and raises `RuntimeError` naming the running interpreter if
not, rather than silently no-op'ing.

## Pitfalls

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **`SSL_CERT_FILE`/`SSL_CERT_DIR` don't behave like OpenSSL** | A sidecar exports `SSL_CERT_DIR` expecting it to *replace* the trust store, but the system CAs are still trusted too | varco's `TrustStore.from_env()` is deliberately additive, diverging from OpenSSL/`uv`/`requests` replace-semantics (§D-T3-env) | If replace-semantics are actually wanted, set `include_system_cas=False` explicitly — do not rely on the env var alone |
| **A pooled HTTP/broker client never picks up a rotated CA** | `ReloadingTrustStore.generation` bumps and `subscribe()` fires, but an existing connection pool keeps using the old context | `SWAP` publishes a *new* `ssl.SSLContext` object — pooled clients that cached the old object by reference are never told unless they subscribe | Register a `store.subscribe(...)` callback that tells the pool to rebuild for new connections; existing established connections keep the old context regardless (brief 001 §2 — no library can fix this) |
| **A revoked CA is still trusted after a config change** | A CA file is removed from the watched folder, but existing connections (and, briefly, in-flight requests on the old context) still trust it | `MUTATE` can only ever *add* trust — `ssl.SSLContext` has no unload API. The watcher's diff correctly routes a removal through `SWAP`, but `SWAP` still can't revoke *already-established* connections | This is inherent to how `ssl.SSLContext` works, not a bug — plan for it: bound-lifetime connections/short pool max-age reduce the exposure window |
| **A private-CA `TrustStore()` is non-recursive when you expected the default** | `store = varco_fastapi.auth.TrustStore(...)` doesn't pick up a cert in a subdirectory of `ca_folder` | You're on the **legacy** shim, which pins `recursive=False` — only `varco_core.tls.TrustStore` defaults to `recursive=True` | Migrate to `varco_core.tls.TrustStore`, or pass `recursive=True` explicitly if staying on the legacy type during the deprecation window |
| **A `.cer` file in a `ca_folder` silently isn't trusted** | A cert file is present but never appears in `ctx.get_ca_certs()` | The call site's `cert_patterns` default doesn't include `*.cer` (`SSLConfig`/the legacy shim keep 3.0's `("*.pem", "*.crt")`) — this is now *warned about*, not silent, but still not loaded by default | Opt in explicitly with `cert_patterns=varco_core.tls.CERT_FILE_PATTERNS`, or use `varco_core.tls.TrustStore` (which defaults to the wider set) |
| **A `to_httpx_verify()`/`to_requests_adapter()`/etc. client keeps the old CA after a `SWAP` rotation** | A rotated CA is trusted by new `ReloadingTrustStore` reads, but a client built from an adapter *before* the rotation still rejects/accepts based on the old chain | Every adapter reads `.context` once, at call time — `SWAP` publishes a brand-new `ssl.SSLContext` object, and an already-constructed client/connector/adapter keeps whatever object it was handed | Register a `store.subscribe(cb)` callback and rebuild the client in it; under `MUTATE` no action is needed at all (Plan 027 / §D-T4-adapters) |
| **A rotated CA never reaches an already-open connection, adapters or not** | Same symptom as above, but even a *rebuilt* client's already-established sockets are unaffected | TLS handshakes negotiate trust once; neither `MUTATE` nor `SWAP` nor any adapter can retroactively change a connection already in flight (brief 001 §2) | Plan for it: bound connection lifetimes / short pool max-age reduce the exposure window — there is no library-level fix |
| **`install_process_trust()` "didn't do anything"** | An HTTP client built *before* the call, or one that constructs `ssl.SSLContext(...)` directly rather than via `ssl.create_default_context()`, ignores the installed trust store | The patched hook (`ssl._create_default_https_context`) is only consulted by `create_default_context()`-style paths, and only for contexts built **after** the call | Call `install_process_trust()` at the very top of the entry point, before constructing/importing any HTTP client; for a client that builds its context directly, use the per-client adapters (`varco_core.tls.clients`) instead |
| **A PKCS#12 identity's CA silently isn't trusted (or, the reverse: unexpectedly IS trusted)** | The server's chain fails to verify despite a CA being bundled in the `.p12`, or a bundled CA is trusted when the caller didn't expect it to be | `TrustStore.pkcs12_trust_ca` defaults to `False` — a client identity bundle's CAs are never automatically trust anchors | Pass `pkcs12_trust_ca=True` explicitly if the bundle's CA chain should also be trusted for server verification; otherwise supply that CA separately via `ca_cert`/`ca_folders` |
| **A `key_password` leaks through a config file or settings echo** | A plaintext decryption password for an mTLS client key ends up committed, logged, or shipped in a config dump | `key_password`/`pkcs12_password` are deliberately **not** `SSLConfig` fields (§D-T6-password ❌) specifically to keep a secret out of a pydantic settings model's `model_dump()` — but nothing stops a contributor from hand-rolling a YAML/JSON config file that stores the password anyway | Prefer the callable form (`key_password=lambda: vault_client.read(...)`) so the secret is fetched lazily and never held in a config object at all; a `str`/`bytes` password in *any* config file — varco's or a hand-rolled one — is a secret in a config file and must be treated with the same handling as any other credential |
