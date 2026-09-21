# OPA Backend — Design Note / ADR (deferred)

**Status:** Proposed — not implemented this release. This document validates
that the `varco_core.auth.policy` seam is engine-neutral by mapping it onto
Open Policy Agent (OPA), so a future `varco_opa` package drops in without
breaking changes.

## Context

`varco_casbin` is the first `PolicyEngine` implementation (in-process, Casbin).
OPA is the natural second backend for organisations that standardise on a
central policy service and the **Rego** language. The risk this ADR retires:
that the `PolicyEngine` / `EnforcementRequest` interface is accidentally
Casbin-shaped and would need to change to support OPA.

## Decision

Ship the seam now; defer the OPA implementation. The seam is sufficient: an OPA
backend implements the same `PolicyEngine.enforce` and maps `EnforcementRequest`
1-to-1 onto an OPA `input` document.

## Mapping `EnforcementRequest` → OPA `input`

```python
# EnforcementRequest(subject, object, action, subject_attrs, object_attrs, domain)
input = {
    "subject": {"id": req.subject, **req.subject_attrs},
    "object": {"id": req.object, **req.object_attrs},
    "action": req.action,
    "domain": req.domain,
}
```

A Rego policy then decides:

```rego
package varco.authz
default allow = false
allow { input.object.owner_id == input.subject.id }     # ABAC ownership
allow { "admin" in input.subject.roles }                 # RBAC override
```

The attribute bags (`subject_attrs` / `object_attrs`) and `domain` — already on
`EnforcementRequest` for Casbin ABAC and domain RBAC — are exactly what Rego
needs. **No interface change required.**

## Proposed shape

```python
# varco_opa/engine.py (sketch — NOT implemented)
@Fallback
@Singleton(priority=BACKEND_PRIORITY, qualifier="opa")
class OpaPolicyEngine(PolicyEngine):
    def __init__(self, settings: Inject[OpaSettings], client: httpx.AsyncClient): ...

    @timeout(2.0)  # fail fast — OPA is on the hot path
    @retry(RetryPolicy(max_attempts=2))  # idempotent decision query
    @circuit_breaker(CircuitBreakerConfig(...))  # shared breaker per OPA endpoint
    async def enforce(self, request: EnforcementRequest) -> bool:
        resp = await self._client.post(
            f"{self._settings.base_url}/v1/data/{self._settings.package}/allow",
            json={"input": self._to_input(request)},
        )
        return bool(resp.json().get("result", False))
```

Resilience decorators are **mandatory** here (unlike in-process Casbin): OPA is a
network dependency on the authorization hot path. Use **shared** `CircuitBreaker`
/ `Bulkhead` instances per OPA endpoint (see the resilience rules in CLAUDE.md).

## What does NOT map cleanly

`PolicyManagement` (add/remove/list rules, role assignments) has **no direct OPA
equivalent** — OPA policy is authored as Rego bundles, distributed via the bundle
API or a registry, not mutated rule-by-rule. Therefore:

- `OpaPolicyEngine` would implement **`PolicyEngine` only**, not `PolicyManagement`.
- The `build_policy_router` admin API (dynamic CRUD) is **Casbin-specific** and
  would not be offered for OPA; OPA policy administration is bundle-push, out of
  scope for the REST router.

This is already accommodated: `build_policy_router` requires a `PolicyManagement`
backend and only exposes `/check` when the backend also implements `PolicyEngine`.

## Open questions (for the future implementation)

- Decision caching: OPA round-trips add latency to every authorize() — cache
  decisions with a short TTL keyed on the `input` hash? (Reuse `varco_core.cache`.)
- Partial evaluation / data documents: push frequently-used data into OPA vs.
  sending it in `input` each call.
- Bundle lifecycle and signing: out of scope for `PolicyEngine`; an operational
  concern for the OPA deployment.

## Consequences

- ✅ `varco_core.auth.policy` ships unchanged when `varco_opa` lands.
- ✅ Apps can switch Casbin ↔ OPA by swapping the DI binding for `PolicyEngine`.
- ❌ OPA users get enforcement but not the dynamic-CRUD admin API (by nature of
  Rego). Acceptable and documented.
