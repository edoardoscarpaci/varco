# Peer service integration — `PeerRegistry`

Plan 009, Phase 11 (C4). Consuming another varco service should be "one env
var + one inject", with resilience (retry, timeout, a shared circuit
breaker, auth forwarding, correlation IDs) pre-wired by default. `PeerRegistry`
(`varco_fastapi.client.peer`) is that piece — it sits on top of `client_for()`
(`docs/client.md`) and `contract_client()` (`docs/client-code-generation.md`),
picking whichever topology a given peer is configured for.

## One env var per peer

```bash
export VARCO_PEER_ORDERS_URL="https://orders.internal"
export VARCO_PEER_ORDERS_TIMEOUT="10"                # optional, default 30.0
export VARCO_PEER_ORDERS_TOKEN_REF="ORDERS_SVC_TOKEN"  # optional — a REFERENCE, not a secret
export ORDERS_SVC_TOKEN="eyJ..."                       # the actual token, read by the default resolver
```

```python
from varco_fastapi.client.peer import PeerRegistry

registry = PeerRegistry.from_env()
client = registry.client("orders", OrderRouter)  # importable-router topology
order = await client.read(order_id)
```

| Env var | Required | Meaning |
|---|---|---|
| `VARCO_PEER_<NAME>_URL` | ✅ | Peer base URL |
| `VARCO_PEER_<NAME>_TIMEOUT` | — | Seconds, default `30.0` |
| `VARCO_PEER_<NAME>_VERIFY` | — | `true`/`false`, or a CA bundle path |
| `VARCO_PEER_<NAME>_PROFILE` | — | Named `ClientProfile` (see "Custom profiles" below) |
| `VARCO_PEER_<NAME>_CONTRACT` | — | Path to a `.contract.json` — enables the cross-repo topology (below) |
| `VARCO_PEER_<NAME>_TOKEN_REF` | — | A **reference** (env var name / secret name), resolved by `SecretResolver` — never the literal token |

Peer names are upper-snake in env, lower-snake in code:
`VARCO_PEER_ORDERS_URL` → `registry.client("orders", ...)`.

## `_TOKEN_REF` is a reference, never a secret (RD-5)

`PeerConfig.token_ref` must name a place to look the secret up, not the
secret itself. `PeerRegistry` refuses to construct if a `token_ref` looks
like a literal credential (starts with `ey`, has a JWT-shaped 3-segment
`.`-delimited layout, or is over 200 chars):

```python
PeerRegistry({"orders": PeerConfig(name="orders", url="...", token_ref="eyJhbGciOi...")})
# ValueError naming RD-5 — pass allow_literal_secret=True only for tests/bootstrap
```

The default `SecretResolver` reads `os.environ[ref]` — so
`VARCO_PEER_ORDERS_TOKEN_REF=ORDERS_SVC_TOKEN` plus `ORDERS_SVC_TOKEN=<token>`
is zero extra wiring. Bring your own resolver (a secret-manager client, a
vault lookup) by implementing the one-method `SecretResolver` protocol and
passing `secret_resolver=` to `PeerRegistry(...)`/`from_env(...)`.

## Two topologies, one call

```python
# Monorepo / importable peer:
client = registry.client("orders", OrderRouter)

# Cross-repo peer — set VARCO_PEER_ORDERS_CONTRACT to a checked-in
# order.contract.json, no router import needed:
client = registry.client("orders")  # router_cls omitted
```

`registry.client(name, router_cls=None)` picks `client_for()` when
`router_cls` is given, `contract_client()` (via `PeerConfig.contract_path`)
otherwise. Neither given → `ValueError` naming both options. The generated
client is cached per `(name, router_cls)` pair.

## Resilience pre-wired by default

Every peer gets this middleware stack unless you supply your own
`ClientProfile` (via `profiles=` + `VARCO_PEER_<NAME>_PROFILE`):

```
AuthForwardMiddleware → CorrelationIdMiddleware → OTelClientMiddleware
  → RetryMiddleware(RetryPolicy(max_attempts=3, base_delay=0.2))
  → TimeoutMiddleware(peer.timeout)
```

...plus one **shared** `CircuitBreaker` per peer **name** — held on the
registry, created once, reused across every `client()` call for that peer
(never per-call — see CLAUDE.md's "per-call CircuitBreaker" pitfall).

```python
client = registry.client("orders", OrderRouter)
client._circuit_breaker  # the same CircuitBreaker instance every call returns
```

## Custom profiles

```python
from varco_fastapi.client.peer import PeerRegistry
from varco_fastapi.client.base import ClientProfile

registry = PeerRegistry.from_env(
    profiles={
        "aggressive-retry": ClientProfile(middleware=(...), timeout=5.0),
    }
)
```

Set `VARCO_PEER_ORDERS_PROFILE=aggressive-retry` to opt one peer into it;
peers without a matching profile name fall back to the built-in default
above.

## DI wiring

```python
from varco_fastapi.client.peer import bind_peers

bind_peers(container, {"orders": OrderRouter, "billing": BillingRouter})
# container.get(VarcoClient[OrderRouter]) resolves
```

`bind_peers` builds (or accepts) a `PeerRegistry`, resolves each named
mapping's client, and registers it via `bind_clients()` — the registry
itself should be constructed once and shared; `bind_peers` does this for you
when you don't pass `registry=` explicitly. It requires an explicit `container`
argument, returns that same container (Plan 046 §D-bind — it delegates to
`bind_clients`), and raises `varco_core.di.AlreadyBoundError` if a peer's client
class is already bound — call it once per process, or use providify's
`di_overrides`/`ContainerOverrides` in a test to replace a binding.

## Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| `VARCO_PEER_ORDERS_TIMEOUT` set but `_URL` missing | `ValueError` at `from_env()` | A half-configured peer is treated as a deploy bug — set the URL or remove the other vars |
| `token_ref` looks like a literal JWT | `ValueError` naming RD-5 | Use a reference name, not the token; `allow_literal_secret=True` only for tests |
| `registry.client("orders")` with no `router_cls` and no `_CONTRACT` set | `ValueError` naming both options | Pass `router_cls=` or set `VARCO_PEER_ORDERS_CONTRACT` |
| Building a fresh `PeerRegistry` per request | Circuit breaker never accumulates failures per peer | Construct once (module scope / DI singleton) and reuse — `bind_peers` does this for you |
| Custom `@route` methods via a peer client behaving like `**kwargs` | Registry defaults to `client_for()`'s in-process methods for an importable peer | See `docs/client-code-generation.md`'s "important" note — set `VARCO_PEER_<NAME>_CONTRACT` for the typed cross-repo path if strict typing matters for that peer today |
