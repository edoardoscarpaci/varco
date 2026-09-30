# Upstream gaps — ledger

**This file is an index, not a record.** Every gap's actual content — reproduction,
evidence, the ask, the interim workaround — lives in its own file under
[`design/upstream-gaps/`](design/upstream-gaps/). This file only points at them.

> ## ⚠️ This ledger is disposable, and its absence is expected
>
> It is **cleared from time to time** — wiped wholesale once a batch of gaps is
> resolved upstream, so the index does not accumulate closed rows forever.
>
> **If this file is missing, nothing is wrong.** It was cleared. Recreate it from
> this template and add your row. Do not go looking for what happened to it, and
> do not treat its absence as a sign that gap-filing has been abandoned.
>
> **Nothing is lost when it is cleared**, because it never held the only copy of
> anything: the per-gap files under `design/upstream-gaps/` are the durable
> artifacts and are never deleted. That is the whole point of the split. This
> ledger can always be rebuilt by listing that directory:
>
> ```bash
> ls design/upstream-gaps/*.md
> ```
>
> A previous incarnation of this file inlined every entry's full body. It was
> deleted in `cae7f33` and took all of them with it — which is precisely the
> failure mode this structure exists to prevent.

---

## How to file a gap

Full workflow, including when *not* to file: CLAUDE.md's
*"When you hit a `providify` limitation or bug"*.

1. **Verify the claim in the upstream library's own source**, citing `file:line`.
   Never file off a `CLAUDE.md`/`README.md`/docstring claim — the register's
   standing U-8 lesson is that entries filed off documentation did not survive
   contact with source. A docstring that *contradicts* the source is itself
   evidence, but quote both.
2. **Write `design/upstream-gaps/<library>-<short-slug>.md`.** This is the
   durable artifact. Cover: what upstream does today (with `file:line`), why it
   is a gap, a minimal reproduction, the ask (with ✅/❌ per candidate fix), and
   any interim workaround. If the gap is *partly ours*, say so plainly and in its
   own section — a report that blames upstream for something we can fix today is
   worse than no report.
3. **Guard it with a `strict=True` xfail** so the fix cannot land unnoticed and
   untested. Prefer a fast, dependency-free reproduction that runs in
   `make test` over one gated behind Docker and `-m integration`.
4. **Add a row below.** If this file does not exist, create it from this
   template — that is the normal path, not an error.

---

## Open gaps

| ID | Library | Gap | Report | Guard |
|---|---|---|---|---|
| **P28-FALLBACK-SHADOWED-SCANNED-CLASS** | `providify` 2.1.0 (`3cb207f`) | `FALLBACK_SHADOWED` is computed per-binding for its own "natural request" `(b.interface, b.qualifier, None)`; a scanned `@Singleton` class registers with `interface = cls` itself (`container.py:1248`), never the ABC it implements, so the INFO never fires for a scanned-class default shadowed at its ABC — even though `get(ABC)` correctly returns the override. Affects 22 of varco's own default sites; gates Plan 052's `varco diagnose` | [providify-fallback-shadowed-scanned-class.md](design/upstream-gaps/providify-fallback-shadowed-scanned-class.md) | `varco_core/tests/test_fallback_defaults.py` (strict xfail, `raises=AssertionError`, no Docker, every `make test`) |
| **P29-DESCRIPTOR-FALLBACK** | `providify` 2.1.0 | `BindingDescriptor` carries no `fallback` field (`descriptor.py:49-54`); `describe()` drops it and every other public read filters shadowed fallbacks, so enumerating a container's fallback bindings needs a private `container._bindings` read. Blocks Plan 046's `di_wiring` conformance suite's completeness check | [providify-descriptor-fallback-field.md](design/upstream-gaps/providify-descriptor-fallback-field.md) | `varco_core/tests/test_providify_contracts.py::TestP29DescriptorFallbackField::test_binding_descriptor_carries_fallback` (strict xfail) |
| **P30-PROVIDE-DISPOSER** | `providify` 2.1.0 | `provide(fn, *, returns=)` (`container.py:1250`) takes no `disposer=` kwarg; `ProviderBinding.disposer` (`binding.py:749`) is only ever assigned by the `@Configuration` install loop (`container.py:7379`), never reachable for a `bind_instance`-registered, caller-constructed object. Blocks Plan 050's disposer wiring for `enable_redis_token_revocation()`'s hand-constructed store | [providify-provide-disposer.md](design/upstream-gaps/providify-provide-disposer.md) | `varco_core/tests/test_providify_contracts.py::TestP050UpstreamAsks::test_provide_accepts_disposer_kwarg` (strict xfail, `raises=TypeError`) |
| **P31-PROVIDER-INJECT-OVERRIDE** | `providify` 2.1.0 | No `inject={...}` parameter-injection override on `provide()`/`@Provider` — a PEP-563 annotation only resolves against the defining function's own `__globals__` (`_annotations.py:180-235`), so a factory parameter whose type is a runtime-only value (chosen per application, e.g. `MCPAdapter`/`SkillAdapter`'s `client_cls`/`runner_cls`/`store_cls`/`conv_store_cls`) can never be declared as `Inject[...]` | [providify-provider-inject-override.md](design/upstream-gaps/providify-provider-inject-override.md) | `varco_core/tests/test_providify_contracts.py::TestP050UpstreamAsks::test_provider_accepts_inject_override` (strict xfail, `raises=TypeError`) |

## Recently closed

*(Kept only until the next clearing — a closed row's evidence lives in its report
file and in the CHANGELOG, not here.)*

- **P22-PROVIDER-PREDESTROY** — resolved 2026-09-02 (Plan 024 / C2). providify
  2.0.1 declared the leaked-teardown behaviour intentional (Jakarta CDI
  producer-method rule) and shipped only a `WARNING`-severity detector
  (`IssueKind.UNREACHABLE_PRE_DESTROY`); varco adopted `@Disposes` — upstream's
  own supported teardown mechanism — at all nine affected sites. See
  [providify-provider-predestroy.md](design/upstream-gaps/providify-provider-predestroy.md)
  §8 for the full resolution.
- **P24-DISPOSES-FIRSTMATCH** — resolved 2026-09-14 (providify 2.1.0, Plan 045
  / DI-15 close-out). Disposer wiring is now scoped to the bindings the same
  `install()`/`ainstall()` call registered; converted guard:
  `varco_redis/tests/test_redis_cache_disposes.py::test_both_cache_configurations_installed_together_both_get_stopped`.
  See [providify-disposes-first-match.md](design/upstream-gaps/providify-disposes-first-match.md) §7.
- **P25-CONDITIONAL-REGISTRATION** — resolved 2026-09-14 (providify 2.1.0,
  Plan 045 / DI-15 close-out). `@Requires(condition=..., env=..., value=...)`
  shipped as the third activation conjunct; converted guard:
  `varco_core/tests/test_providify_contracts.py::TestP25ConditionalRegistration::test_requires_decorator_is_exported`.
  See [providify-conditional-registration.md](design/upstream-gaps/providify-conditional-registration.md) §9.
- **P26-OPEN-GENERIC-BINDING** — resolved 2026-09-14 (providify 2.1.0, Plan
  045 / DI-15 close-out). `returns=Repo[T]` now registers an open binding
  served by any closed request; converted guard:
  `varco_core/tests/test_providify_contracts.py::TestP26OpenGenericBinding::test_open_alias_registration_resolves_closed_request`.
  See [providify-open-generic-binding.md](design/upstream-gaps/providify-open-generic-binding.md) §8.
- **P27-FALLBACK-BINDING** — resolved 2026-09-14 (providify 2.1.0, Plan 045 /
  DI-15 close-out). `@Fallback`/`FallbackMarker`/`IssueKind.FALLBACK_SHADOWED`
  shipped exactly under the recommended name and semantics; converted guards:
  `varco_core/tests/test_providify_contracts.py::TestP27FallbackBinding::test_fallback_decorator_is_exported`
  + `::test_validate_reports_shadowed_fallback`.
  See [providify-fallback-binding.md](design/upstream-gaps/providify-fallback-binding.md) §9.

---

**Last updated:** 2026-09-27 · Plan 050 filed P30, P31 (providify 2.1.0). Plan 045 closed P24, P25, P26, P27 (providify 2.1.0).
