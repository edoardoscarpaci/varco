# Token Profiles — Technical Reference

`TokenProfile` + `TokenProfileRegistry` (`varco_core.jwt.profile`) replace the single
`JwtUtil.SYSTEM_ISSUER` class variable with a **registry of named profiles**, so a
deployment can recognise many kinds of special/internal tokens (`system`, `internal`,
`partner`, `service-mesh`, …) — not just one undifferentiated "system" bucket — and
authorize on the resolved profile at the route layer.

---

## Core files

| File | Role |
|---|---|
| `varco_core/jwt/profile.py` | `TokenProfile`, `TokenProfileRegistry`, `PROFILE_METADATA_KEY`, `resolve_token_profile()` |
| `varco_core/jwt/util.py` | `JwtUtil.matches_profile/profile_name/assert_profile`, rewritten `is_system()` |
| `varco_core/jwt/builder.py` | `JwtBuilder.as_profile(profile)` |
| `varco_core/jwt/parser.py` | `JwtParser._from_raw_claims` calls `resolve_token_profile()` after building the token |
| `varco_fastapi/auth/guard.py` | `RouteGuard.token_profiles` field + `require_token_profile(*names)` |

---

## Why not just `SYSTEM_ISSUER`?

`JwtUtil.SYSTEM_ISSUER` is a single `ClassVar` string — one issuer value can be
"the system". It cannot express "internal tokens must also carry `token_type=system`
and `aud=orders`", cannot be named (so you can't distinguish `internal` from
`partner` from `service-mesh`), and cannot grant implied roles. `TokenProfile`
generalises this into a named, declarative condition set with optional implied
grants:

```python
@dataclass(frozen=True)
class TokenProfile:
    name: str
    issuers: frozenset[str] = frozenset()  # any-of; empty = any issuer
    token_type: str | None = None  # exact match when set
    audiences: frozenset[str] = frozenset()  # any-of against token.aud
    required_claims: frozenset[str] = frozenset()  # canonical or raw claim names
    implied_roles: frozenset[str] = frozenset()  # merged into AuthContext.roles
    implied_scopes: frozenset[str] = frozenset()
```

`matches(token)` returns `True` iff every declared condition holds (empty
`issuers`/`audiences`/`required_claims` are vacuously satisfied). `explain(token)`
returns a human-readable description of the **first** failing condition (issuer →
token_type → audience → required_claims, in that order) — useful when diagnosing
"why didn't my token match this profile" during setup.

---

## `TokenProfileRegistry`

```python
class TokenProfileRegistry:
    def register(self, profile: TokenProfile) -> None: ...
    def get(self, name: str) -> TokenProfile: ...  # raises TokenProfileError
    def names(self) -> tuple[str, ...]: ...
    def resolve(self, token: JsonWebToken) -> TokenProfile | None: ...  # first match wins
    def matches(self, name: str, token: JsonWebToken) -> bool: ...
    @classmethod
    def from_env(cls) -> "TokenProfileRegistry": ...
```

`resolve()` returns the **first** registered profile that matches, in registration
order — overlapping profiles are first-wins, documented behaviour rather than an
error.

---

## Environment variables

`VARCO_JWT_PROFILE__<NAME>__{ISS,TOKEN_TYPE,AUD,REQUIRED_CLAIMS,ROLES,SCOPES}` — all
comma-separated lists except `TOKEN_TYPE` (a single string). `<NAME>` is lowercased to
form the profile name (`INTERNAL` → `"internal"`):

```bash
export VARCO_JWT_PROFILE__INTERNAL__ISS="mesh-signer"
export VARCO_JWT_PROFILE__INTERNAL__TOKEN_TYPE="system"
export VARCO_JWT_PROFILE__INTERNAL__ROLES="internal"     # implied_roles
```

⚠️ **A label with no condition at all is rejected.** At least one of
`ISS`/`TOKEN_TYPE`/`AUD`/`REQUIRED_CLAIMS` must be set — a condition-less profile
would silently match every token and grant its `implied_roles`/`implied_scopes` to
everyone. `TokenProfileRegistry.from_env()` raises `TokenProfileError` at load time
naming the offending label, rather than letting this footgun through.

---

## Integration — how a matched profile changes `AuthContext`

`JwtParser._from_raw_claims` calls `resolve_token_profile(token)` after building the
base token — the same funnel both SEAM 1 (`JwtParser.parse()`) and SEAM 2
(`TrustedIssuerRegistry.verify()`, and therefore `JwtBearerAuth`) go through:

- The matched profile's name is stored under
  `AuthContext.metadata["token_profile"]` (`PROFILE_METADATA_KEY`).
- `implied_roles` / `implied_scopes` are merged into `AuthContext.roles` /
  `AuthContext.scopes`.
- ⚠️ **Materialisation**: if the matched profile declares implied roles/scopes but
  the token carries **no** auth claims at all (`auth_ctx is None` up to this point),
  a fresh `AuthContext(user_id=token.sub, roles=implied_roles, ...)` is
  **materialised**. This is the "system token with elevated trust" case — a bare
  `sub`+`iss` service-mesh token becomes a fully authorized context because it
  matched a profile that grants roles.
- A token that matches a profile with **no** implied roles/scopes, and had no
  `auth_ctx` to begin with, still gets `auth_ctx is None` — today's behaviour is
  unchanged unless the profile actually grants something.

```python
tok = JwtParser.parse(raw_token, secret, algorithms=["HS256"])
tok.auth_ctx.metadata["token_profile"]  # "internal" (if matched)
tok.auth_ctx.roles  # includes any implied_roles merged in
```

### `JwtUtil` helpers

```python
util = JwtUtil(tok)
util.matches_profile("internal")  # bool
util.profile_name()  # first matching profile's name, or None
util.assert_profile("internal")  # raises TokenProfileError if no match
```

### `JwtBuilder.as_profile()`

For minting tokens that should match a given profile (tests, internal signing):

```python
token = (
    JwtBuilder()
    .subject("svc_1")
    .as_profile(internal_profile)  # sets iss (first of profile.issuers), token_type, aud
    .encode(secret)
)
```

`as_profile()` does **not** inject `implied_roles`/`implied_scopes` into the signed
token — those are derived at *parse* time by `resolve_token_profile()`, so a
profile's implied grants always reflect whatever registry is active when the token
is verified, not what was active when it was minted.

---

## Route-level authorization — `require_token_profile`

```python
from varco_fastapi.router.presets import GenericRouter
from varco_fastapi.router.endpoint import route
from varco_fastapi.auth import JwtBearerAuth
from varco_fastapi.auth.guard import require_token_profile


class MeshRouter(GenericRouter):
    _prefix = "/mesh"
    _auth = JwtBearerAuth(registry)

    @route("GET", "/internal-only", requires=require_token_profile("internal"))
    async def internal_only(self, ctx) -> dict:
        return {"ok": True}
```

`require_token_profile(*names)` builds a `RouteGuard` with the `token_profiles`
field set — an any-of match against `ctx.metadata.get("token_profile")`, checked
between the role check and the grant check inside `RouteGuard.check()`. See
`technical_docs/features/route-guard.md` for the full evaluation order.

---

## Back-compat: `SYSTEM_ISSUER` is not removed

`JwtUtil.SYSTEM_ISSUER` and `is_system()` keep working — this is a documentation-only
deprecation with **no runtime `DeprecationWarning`** and **no scheduled removal**
(intercepting a `ClassVar` read would require a metaclass descriptor; the churn isn't
worth it for a class variable that already works).

`is_system()`'s resolution order:

1. If a profile named `"system"` is registered (env var `VARCO_JWT_PROFILE__SYSTEM__*`
   or an explicit `configure_token_profiles()` registry) → delegate to
   `registry.matches("system", token)`.
2. Otherwise → compare `token.iss` against the **live** `JwtUtil.SYSTEM_ISSUER`
   `ClassVar` (never snapshotted) — so `monkeypatch.setattr(JwtUtil, "SYSTEM_ISSUER",
   ...)` in existing tests still works exactly as before.

A registered `"system"` profile always takes precedence over `SYSTEM_ISSUER` — even
if `SYSTEM_ISSUER` was also monkeypatched to a matching value, the profile decides.

**Migrating**: replace

```python
JwtUtil.SYSTEM_ISSUER = "my-org/internal"
```

with

```bash
export VARCO_JWT_PROFILE__SYSTEM__ISS="my-org/internal"
```

(or an explicit `TokenProfile(name="system", issuers=frozenset({"my-org/internal"}))`
registered via `configure_token_profiles()`), for a profile that composes with
`token_type`/`aud`/`required_claims` and can coexist with other named profiles.

---

## See also

- `technical_docs/features/jwt-claim-transformer.md` — the claim-mapping layer that
  runs in the same funnel, one step before profile resolution.
- `technical_docs/features/route-guard.md` — `RouteGuard` evaluation order.

## Pitfalls

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **`is_system()` false for my internal token** | A token minted by your own internal issuer is not recognised as "system" | Only one static `SYSTEM_ISSUER` was configured, and this token's issuer doesn't match it | Define `VARCO_JWT_PROFILE__SYSTEM__ISS` (or any named `TokenProfile`) instead — see `technical_docs/features/token-profiles.md` |
