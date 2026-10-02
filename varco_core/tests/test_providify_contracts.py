"""
Plan 045 (DI-15 close-out) — providify contract pins.

Renamed from ``test_providify_upstream_gaps.py`` (Plan 043/044). The five
gaps this module used to guard as ``strict xfail`` — P25-CONDITIONAL-
REGISTRATION, P26-OPEN-GENERIC-BINDING (two tests), P27-FALLBACK-BINDING
(two tests) — all shipped in providify 2.1.0 (``CHANGELOG.md:12-129`` in the
providify source tree). This module is now the **upgrade checklist**: every
test here is a passing contract pin of a providify 2.1.0 semantic varco's
own default-binding rule (`technical_docs/features/dependency-injection.md`,
§D-rule) depends on. Re-run this module at every future providify bump — a
failure here means a semantic varco relies on moved out from under it.

Gap ID -> report -> guard class map (history — closed by providify 2.1.0,
Plan 045):

- **P26-OPEN-GENERIC-BINDING** -> `design/upstream-gaps/providify-open-generic-binding.md`
  -> `TestP26OpenGenericBinding`.
- **P25-CONDITIONAL-REGISTRATION** -> `design/upstream-gaps/providify-conditional-registration.md`
  -> `TestP25ConditionalRegistration`.
- **P27-FALLBACK-BINDING** -> `design/upstream-gaps/providify-fallback-binding.md`
  -> `TestP27FallbackBinding`.

**P24-DISPOSES-FIRSTMATCH is NOT re-guarded here** — its guard lives in
`varco_redis/tests/test_redis_cache_disposes.py`.

**Plan 051 (DI-5) update**: `TestP051Multibinding` pins the multi-binding primitives
`varco_core.di.collect` builds on — `list[T]` on a `@Multibound` seam returns `[]` on zero
contributions (never `LookupError`); `InjectInstances[T]` raises on zero (the reason varco never
uses it); collection order is ascending priority, ties in registration order; the returned list
is a fresh object per call; a shadowed `@Fallback` contribution is absent (a lone one is
present); `get(list[T], qualifier=...)` narrows; a scanned `@Singleton` subclass is collected
under its ABC; a `bind(ABC, Impl)` self-binding does not duplicate; scanning a module holding
only a `@Multibound` class registers nothing. If any of these move, §D-shape/§D-order in
`technical_docs/features/dependency-injection.md` are wrong.

No Docker, no backend — pure providify, runs in `make test PKG=varco_core`.

⚠️ RED-MODE NOTE (Plan 045, written before the pin bump): this repo still
resolves providify 2.0.1. `from providify import Fallback` / `Requires`, the
`returns=Repo[T]` open-generic wildcard, and `IssueKind.FALLBACK_SHADOWED` /
`IssueKind.CONDITION_INACTIVE` do not exist yet at 2.0.1 — every test below
is expected to fail (ImportError/AttributeError/LookupError) until Step 1
bumps the pin to `providify>=2.1.0`.
"""

from __future__ import annotations

from typing import Generic, TypeVar

import pytest
from providify import DIContainer, Inject, IssueKind, Profile, Requires, Singleton
from providify.exceptions import ScopeViolationDetectedError

T = TypeVar("T")


class Repo(Generic[T]):
    """Local minimal generic — deliberately NOT `varco_core.repository.AsyncRepository`."""

    def __init__(self, entity: type[object] | None = None) -> None:
        self.entity = entity


class User:
    """A concrete closing type — stands in for a domain entity."""


class Order:
    """A second concrete closing type — used to prove per-closed-alias caching."""


class TestP26OpenGenericBinding:
    """P26-OPEN-GENERIC-BINDING — closed by providify 2.1.0's open-generic providers.

    `providify/CHANGELOG.md:69-98` (2.1.0): `provide(factory, returns=Repo[T])`
    now registers an OPEN binding served by any closed request sharing its
    origin, rule (1): binding-side TypeVar is a wildcard.
    """

    def test_open_alias_registration_resolves_closed_request(self) -> None:
        """Registering `returns=Repo[T]` then resolving `Repo[User]` now works (rule 1)."""

        def factory() -> Repo[T]:
            return Repo()

        container = DIContainer()
        container.provide(factory, returns=Repo[T])

        result: Repo[User] = container.get(Repo[User])

        assert isinstance(result, Repo)

    def test_open_alias_factory_receives_closed_type_argument(self) -> None:
        """A factory taking `entity: type[T]` receives the closed `User` type (rule 3)."""

        def factory(entity: type[T]) -> Repo[T]:
            return Repo(entity=entity)

        container = DIContainer()
        container.provide(factory, returns=Repo[T])

        result: Repo[User] = container.get(Repo[User])

        assert result.entity is User

    def test_closed_alias_still_matches_exactly(self) -> None:
        """Control: a closed binding (`Repo[User]`) matches a closed request."""

        def factory() -> Repo[User]:
            return Repo(entity=User)

        container = DIContainer()
        container.provide(factory, returns=Repo[User])

        result: Repo[User] = container.get(Repo[User])

        assert isinstance(result, Repo)
        assert result.entity is User

    def test_bare_open_alias_request_raises_lookup_error(self) -> None:
        """New behaviour change: `get(Repo)` against only an open binding raises
        `LookupError` — `providify/CHANGELOG.md:119-125` (rule 2/the bare-request
        narrowing)."""

        def factory() -> Repo[T]:
            return Repo()

        container = DIContainer()
        container.provide(factory, returns=Repo[T])

        with pytest.raises(LookupError):
            container.get(Repo)


class TestP25ConditionalRegistration:
    """P25-CONDITIONAL-REGISTRATION — closed by providify 2.1.0's `@Requires`.

    `providify/CHANGELOG.md:32-47`: `@Requires(condition=..., env=..., value=...)`
    gates a class or `@Provider` on a predicate evaluated lazily at resolve
    time, the third conjunct of binding activation beside `@Profile`/
    `@Alternative`. varco itself adopts `@Requires` in Plan 047, not here —
    this module only pins the shipped contract.
    """

    def test_requires_decorator_is_exported(self) -> None:
        """`providify.Requires` gates resolution on a live predicate.

        `bind()` requires `@Singleton` metadata on the implementation
        (`providify/container.py:1244-1245`; the P27 report §1) — so each
        `@Requires`-gated implementation is also `Singleton(...)`.
        """
        from providify import Requires

        flag = {"on": True}

        class Base:
            pass

        @Requires(condition=lambda: flag["on"])
        class OnImpl(Base):
            pass

        @Requires(condition=lambda: not flag["on"])
        class OffImpl(Base):
            pass

        Singleton(OnImpl)
        Singleton(OffImpl)

        container = DIContainer()
        container.bind(Base, OnImpl)
        container.bind(Base, OffImpl)

        assert isinstance(container.get(Base), OnImpl)

        flag["on"] = False

        assert isinstance(container.get(Base), OffImpl)

    def test_profile_is_the_only_declarative_switch_today(self) -> None:
        """Control: `@Profile` is a shipped declarative activation switch, unchanged."""

        class Base:
            pass

        @Profile("x")
        @Singleton
        class GatedImpl(Base):
            pass

        container = DIContainer(profiles=())
        container.bind(Base, GatedImpl)

        with pytest.raises(LookupError):
            container.get(Base)

        container.activate_profile("x")

        assert isinstance(container.get(Base), GatedImpl)


class Base:
    """Local minimal base type — providify-only, shared by the P27/@Fallback tests."""


class TestP27FallbackBinding:
    """P27-FALLBACK-BINDING — closed by providify 2.1.0's `@Fallback`.

    `providify/CHANGELOG.md:48-68`: `@Fallback` marks a class or `@Provider`
    function/method as a default binding — a candidate only when no active
    non-fallback binding matches the same request. This module pins the
    contract varco's default-binding rule (§D-rule) relies on.
    """

    def test_fallback_decorator_is_exported(self) -> None:
        """`@Fallback` yields to a non-fallback sibling at equal priority.

        `A` (`@Fallback @Singleton`) and `B` (plain `@Singleton`), both at
        priority 0, `bind(Base, A)` registered first, `bind(Base, B)`
        second -> `get(Base)` returns `B` (the fallback loses even though it
        was registered first). `get_all(Base)` excludes the shadowed
        fallback -> `[B]` only. A lone, unshadowed fallback still resolves.
        """
        from providify import Fallback

        @Fallback
        @Singleton
        class A(Base):
            pass

        @Singleton
        class B(Base):
            pass

        container = DIContainer()
        container.bind(Base, A)
        container.bind(Base, B)

        assert isinstance(container.get(Base), B)
        results: list[Base] = container.get_all(Base)
        assert [type(x) for x in results] == [B]

        lone_container = DIContainer()
        lone_container.bind(Base, A)

        assert isinstance(lone_container.get(Base), A)

    def test_validate_reports_shadowed_fallback(self) -> None:
        """`IssueKind.FALLBACK_SHADOWED` (`INFO`) names the shadowing binding."""
        shadowed_kind = IssueKind.FALLBACK_SHADOWED

        from providify import Fallback

        @Fallback
        @Singleton
        class A(Base):
            pass

        @Singleton
        class B(Base):
            pass

        container = DIContainer()
        container.bind(Base, A)
        container.bind(Base, B)

        report = container.validate(raise_on_error=False)
        shadow_issues = [issue for issue in report.issues if issue.kind == shadowed_kind]

        assert len(shadow_issues) == 1
        assert "B" in str(shadow_issues[0])

    def test_qualified_fallback_wins_its_own_qualified_request(self) -> None:
        """§D-qualifier: `get(Base, qualifier="in_memory")` narrows to the
        qualified fallback alone before the fallback drop
        (`providify/container.py:2096-2101`, E3/E4) — it still wins even
        with an unqualified non-fallback `B` also present."""
        from providify import Fallback

        @Fallback
        @Singleton(qualifier="in_memory")
        class A(Base):
            pass

        @Singleton
        class B(Base):
            pass

        container = DIContainer()
        container.bind(Base, A)
        container.bind(Base, B)

        assert isinstance(container.get(Base), B)
        assert isinstance(container.get(Base, qualifier="in_memory"), A)

    def test_priority_narrowing_returns_shadowed_fallback(self) -> None:
        """E14 (`providify/container.py:2108-2112`): `get(Base, priority=p)`
        narrows to that priority BEFORE the fallback drop, so an explicit
        priority-scoped request can still return a shadowed fallback."""
        from providify import Fallback

        @Fallback
        @Singleton(priority=0)
        class A(Base):
            pass

        @Singleton(priority=100)
        class B(Base):
            pass

        container = DIContainer()
        container.bind(Base, A)
        container.bind(Base, B)

        assert isinstance(container.get(Base), B)
        assert isinstance(container.get(Base, priority=0), A)

    def test_subclass_of_fallback_is_not_a_fallback(self) -> None:
        """`FallbackMarker`/`_is_fallback` never walks the MRO
        (`providify/metadata.py:858-861`) — a subclass of a `@Fallback`
        class is not itself a fallback."""
        from providify import Fallback

        @Fallback
        @Singleton
        class A(Base):
            pass

        @Singleton
        class ASubclass(A):
            pass

        container = DIContainer()
        container.bind(Base, ASubclass)

        report = container.validate(raise_on_error=False)
        shadow_issues = [i for i in report.issues if i.kind == IssueKind.FALLBACK_SHADOWED]
        assert shadow_issues == []

    def test_two_fallbacks_tie_by_priority_then_order(self) -> None:
        """Two fallbacks, no non-fallback candidate: neither is dropped;
        `max()` by priority wins, `100` beating `0`
        (`providify/scope.py:1042-1047`)."""
        from providify import Fallback

        @Fallback
        @Singleton(priority=0)
        class A(Base):
            pass

        @Fallback
        @Singleton(priority=100)
        class B(Base):
            pass

        container = DIContainer()
        container.bind(Base, A)
        container.bind(Base, B)

        assert isinstance(container.get(Base), B)

    def test_get_all_of_an_all_fallback_set_returns_both(self) -> None:
        """`get_all()` of an all-fallback set (no non-fallback sibling)
        returns every fallback, ascending by priority — nothing is
        excluded because nothing shadows anything here."""
        from providify import Fallback

        @Fallback
        @Singleton(priority=0)
        class A(Base):
            pass

        @Fallback
        @Singleton(priority=100)
        class B(Base):
            pass

        container = DIContainer()
        container.bind(Base, A)
        container.bind(Base, B)

        results: list[Base] = container.get_all(Base)
        assert {type(x) for x in results} == {A, B}

    def test_equal_priority_tie_breaks_to_first_registered_today(self) -> None:
        """Control: at equal priority with no fallback involved, `bind()`
        order decides the winner — unchanged at 2.1.0."""

        class A(Base):
            pass

        class B(Base):
            pass

        Singleton(A)
        Singleton(B)

        container = DIContainer()
        container.bind(Base, A)
        container.bind(Base, B)

        assert isinstance(container.get(Base), A)

    def test_higher_priority_already_wins_regardless_of_order_today(self) -> None:
        """Control: a strictly higher priority already wins today, order aside."""

        class A(Base):
            pass

        class B(Base):
            pass

        Singleton(A)
        Singleton(priority=1)(B)

        container = DIContainer()
        container.bind(Base, A)
        container.bind(Base, B)

        assert isinstance(container.get(Base), B)

    def test_inactive_sibling_is_not_a_candidate_today(self) -> None:
        """Control: an inactive `@Profile`-gated sibling never shadows anything."""

        class A(Base):
            pass

        @Profile("x")
        class B(Base):
            pass

        Singleton(A)
        Singleton(B)

        container = DIContainer(profiles=())
        container.bind(Base, A)
        container.bind(Base, B)

        first_pass: list[Base] = container.get_all(Base)
        assert [type(x) for x in first_pass] == [A]

        container.activate_profile("x")

        second_pass: list[Base] = container.get_all(Base)
        assert {type(x) for x in second_pass} == {A, B}

    def test_get_all_excludes_shadowed_fallback(self) -> None:
        """Named pin for Plan 051's multi-binding registries (§Context row
        `get_all()`/`aget_all()` exclusion): a shadowed fallback never
        appears in `get_all()`."""
        from providify import Fallback

        @Fallback
        @Singleton
        class A(Base):
            pass

        @Singleton
        class B(Base):
            pass

        container = DIContainer()
        container.bind(Base, A)
        container.bind(Base, B)

        results: list[Base] = container.get_all(Base)
        assert A not in [type(x) for x in results]


class TestP29DescriptorFallbackField:
    """P29-DESCRIPTOR-FALLBACK (Plan 046 / \u00a7D-suite) \u2014 `BindingDescriptor`
    carries no `fallback` field (`providify/descriptor.py:49-54`), so a
    public `describe()` read cannot answer "is this binding a fallback"
    without the private `container._bindings` shim
    (`testkit/varco_conformance/di_wiring.py::fallback_bindings`).

    Guard for `design/upstream-gaps/providify-descriptor-fallback-field.md`.
    """

    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason=(
            "GAP: P29-DESCRIPTOR-FALLBACK -- BindingDescriptor carries no "
            "`fallback` field (providify/descriptor.py:49-54) -- see "
            "design/upstream-gaps/providify-descriptor-fallback-field.md"
        ),
    )
    def test_binding_descriptor_carries_fallback(self) -> None:
        from providify import Fallback

        class Base:
            pass

        @Fallback
        @Singleton
        class F(Base):
            pass

        container = DIContainer()
        container.bind(Base, F)

        descriptor = container.describe().bindings[0]

        assert getattr(descriptor, "fallback", None) is True


# ── Plan 051: multi-binding ──────────────────────────────────────────────────
#
# Contract pins for providify 2.1.0's multi-binding primitives
# (`@Multibound`, `list[T]` resolution, `_collect_sync`/`_collect_async`,
# `InjectInstances[T]`) — the foundation `varco_core.di.collect` builds on
# (§D-shape, §D-order). If any pin here moves, §D-shape/§D-order in
# `technical_docs/features/dependency-injection.md` are wrong and need a
# fresh look at the providify source before touching varco code.


class TestP051Multibinding:
    def test_list_hint_on_multibound_seam_with_zero_contributions_is_empty(self) -> None:
        from providify import DIContainer, Multibound

        @Multibound
        class Seam:
            pass

        container = DIContainer()
        assert container.get(list[Seam]) == []

    def test_inject_instances_raises_on_zero_contributions(self) -> None:
        from providify import DIContainer, InjectInstances

        class Seam:
            pass

        container = DIContainer()
        with pytest.raises(LookupError):
            container.get(InjectInstances[Seam])

    def test_collection_order_is_ascending_priority_then_registration(self) -> None:
        from providify import DIContainer, Multibound, Provider

        @Multibound
        class Seam:
            pass

        class A(Seam):
            pass

        class B(Seam):
            pass

        class C(Seam):
            pass

        b, a, c = B(), A(), C()

        @Provider(singleton=True, returns=Seam, priority=0)
        def _b():
            return b

        @Provider(singleton=True, returns=Seam, priority=0)
        def _a():
            return a

        @Provider(singleton=True, returns=Seam, priority=5)
        def _c():
            return c

        container = DIContainer()
        container.provide(_b)
        container.provide(_a)
        container.provide(_c)

        assert container.get(list[Seam]) == [b, a, c]

    def test_collection_result_is_a_fresh_list_per_call(self) -> None:
        from providify import DIContainer, Multibound, Singleton

        @Multibound
        class Seam:
            pass

        @Singleton
        class Impl(Seam):
            pass

        container = DIContainer()
        container.register(Impl)

        first = container.get(list[Seam])
        second = container.get(list[Seam])
        assert first == second
        assert first is not second

    def test_shadowed_fallback_contribution_is_absent_lone_fallback_present(self) -> None:
        from providify import DIContainer, Fallback, Multibound, Singleton

        @Multibound
        class Seam:
            pass

        @Fallback
        @Singleton
        class Default(Seam):
            pass

        @Singleton
        class Real(Seam):
            pass

        # A lone @Fallback contribution is present.
        container = DIContainer()
        container.register(Default)
        assert len(container.get(list[Seam])) == 1

        # Once a non-fallback sibling exists, the fallback contribution vanishes.
        container2 = DIContainer()
        container2.register(Default)
        container2.register(Real)
        result = container2.get(list[Seam])
        assert len(result) == 1
        assert isinstance(result[0], Real)

    def test_qualified_get_narrows_collection(self) -> None:
        from providify import DIContainer, Multibound, Provider

        @Multibound
        class Seam:
            pass

        class A(Seam):
            pass

        class B(Seam):
            pass

        a, b = A(), B()

        @Provider(singleton=True, returns=Seam, qualifier="x")
        def _a():
            return a

        @Provider(singleton=True, returns=Seam)
        def _b():
            return b

        container = DIContainer()
        container.provide(_a)
        container.provide(_b)

        qualified = container.get(list[Seam], qualifier="x")
        assert qualified == [a]

    def test_scanned_singleton_subclass_is_collected_under_the_abc(self) -> None:
        from providify import DIContainer, Multibound, Singleton

        @Multibound
        class Seam:
            pass

        @Singleton
        class Impl(Seam):
            pass

        container = DIContainer()
        container.register(Impl)

        result = container.get(list[Seam])
        assert len(result) == 1
        assert isinstance(result[0], Impl)

    def test_bind_self_binding_does_not_duplicate_in_collection(self) -> None:
        from providify import DIContainer, Multibound, Singleton

        @Multibound
        class Seam:
            pass

        @Singleton
        class Impl(Seam):
            pass

        container = DIContainer()
        container.bind(Seam, Impl)

        result = container.get(list[Seam])
        assert len(result) == 1

    def test_scanning_a_module_with_only_a_multibound_class_registers_nothing(self) -> None:
        import sys
        import types

        from providify import DIContainer, Multibound

        module = types.ModuleType("_p051_scan_probe")

        @Multibound
        class Seam:
            pass

        Seam.__module__ = module.__name__
        module.Seam = Seam
        sys.modules[module.__name__] = module
        try:
            container = DIContainer()
            before = len(container.describe().bindings)
            container.scan(module.__name__)
            after = len(container.describe().bindings)
            assert after == before
        finally:
            del sys.modules[module.__name__]


class TestP050UpstreamAsks:
    """
    Plan 050 / §D-asks — two upstream gaps filed against providify 2.1.0,
    guarded here so a future providify version silently fixing either is
    noticed (XPASS on a strict xfail) rather than assumed.

    - **P-DISP** — `provide(fn, *, returns=, disposer=)`. Today `provide()`
      takes no `disposer=` kwarg (`container.py:1250`); `ProviderBinding.disposer`
      is a plain attribute providify's own install loop assigns
      (`container.py:7379`, `binding.py:749`), with no public setter API.
    - **P-INJ** — `provide(fn, inject={...})` / a parameter-injection
      override for a type only known at call time. Today annotations are
      evaluated against `fn.__globals__` + a `localns` of registered
      interfaces (`_annotations.py:180-235`); a closure-local runtime type
      cannot be named this way, and `provide()`/`@Provider` have no
      parameter-injection-override kwarg to route around it.

    Report files (per CLAUDE.md's "When you hit a providify limitation"):
    `design/upstream-gaps/providify-provide-disposer.md` (P-DISP),
    `design/upstream-gaps/providify-provider-inject-override.md` (P-INJ).
    """

    @pytest.mark.xfail(
        strict=True,
        raises=TypeError,
        reason="GAP: P-DISP — provide() accepts no disposer= kwarg on providify 2.1.0 "
        "(container.py:1250); see design/upstream-gaps/providify-provide-disposer.md",
    )
    def test_provide_accepts_disposer_kwarg(self) -> None:
        from providify import DIContainer

        container = DIContainer()
        calls: list[str] = []

        def factory() -> str:
            return "value"

        # Not yet a real parameter on provide() — expected TypeError.
        container.provide(factory, returns=str, disposer=lambda v: calls.append(v))  # type: ignore[call-arg]

    @pytest.mark.xfail(
        strict=True,
        raises=TypeError,
        reason="GAP: P-INJ — provide()/@Provider accept no inject= parameter-override "
        "kwarg on providify 2.1.0 (decorator/scope.py has no parameter twin to returns=); "
        "see design/upstream-gaps/providify-provider-inject-override.md",
    )
    def test_provider_accepts_inject_override(self) -> None:
        from providify import DIContainer

        container = DIContainer()

        class _RuntimeOnlyType:
            pass

        def factory(client: object) -> str:
            return "value"

        # `inject=` is not a real provide() kwarg today — expected TypeError.
        container.provide(factory, returns=str, inject={"client": _RuntimeOnlyType})  # type: ignore[call-arg]


class TestP32CopyCollectionPoints:
    """
    Plan 052 / Step 11 — P32, filed against providify 2.1.0.

    `DIContainer.copy()` bypasses `__init__` via `__new__` and never sets
    `_collection_points` (set at `container.py:875`) — so `get()`/`aget()`
    on the copy raises `AttributeError` the moment the container has any
    `@Multibound` type registered, since `_is_collection_point()`
    (`container.py:1725`) reads it unconditionally.

    Report: `design/upstream-gaps/providify-copy-collection-points.md`.
    """

    @pytest.mark.xfail(
        strict=True,
        raises=AttributeError,
        reason="GAP: P32 — DIContainer.copy() never copies _collection_points on "
        "providify 2.1.0 (container.py:7800+ vs. __init__'s container.py:875); "
        "see design/upstream-gaps/providify-copy-collection-points.md",
    )
    def test_copy_preserves_collection_points(self) -> None:
        from providify import DIContainer, Multibound, Singleton

        @Multibound
        class _Seam:
            pass

        @Singleton
        class _Contribution(_Seam):
            pass

        base = DIContainer()
        base.register(_Contribution)
        base.multibind(_Seam)
        clone = base.copy()

        # Expected to raise AttributeError on providify 2.1.0 — see report.
        # (`_multibound_inner()` only consults `_collection_points` for a
        # `list[T]`-shaped request — see the report's §2 for the exact path.)
        clone.get(list[_Seam])


class TestP33OverrideInstanceScopeLeak:
    """
    Plan 052 / Step 11 — P33, filed against providify 2.1.0.

    `ContainerOverrides.instance()` always binds `Scope.DEPENDENT`
    (`testing.py:128-130`); `_check_scope_violation()` then reports a
    false-positive scope leak for any `SINGLETON` consumer holding the
    overridden interface via plain `Inject[T]`.

    Report: `design/upstream-gaps/providify-override-instance-scope-leak.md`.
    """

    @pytest.mark.xfail(
        strict=True,
        raises=ScopeViolationDetectedError,
        reason="GAP: P33 — ContainerOverrides.instance() always binds Scope.DEPENDENT "
        "(providify 2.1.0 testing.py:128-130), tripping _check_scope_violation() for a "
        "SINGLETON consumer using plain Inject[T]; see "
        "design/upstream-gaps/providify-override-instance-scope-leak.md",
    )
    async def test_instance_override_trips_scope_leak_on_singleton_consumer(self) -> None:
        from providify.testing import ContainerOverrides

        @Singleton
        class _Dep:
            pass

        @Singleton
        class _Consumer:
            def __init__(self, d: Inject[_Dep]) -> None:
                self.d = d

        container = DIContainer()
        container.register(_Dep)
        container.register(_Consumer)

        overrides = ContainerOverrides(container)
        overrides.instance(_Dep, _Dep())

        # Expected to raise ScopeViolationDetectedError on providify 2.1.0 — see report.
        await container.aget(_Consumer)


# P34 fixtures live at module scope: under `from __future__ import annotations`
# a function-local class in an `Inject[...]` annotation cannot be resolved by
# validate(), which would fail the guard with UNRESOLVED_ANNOTATION — the wrong
# reason.
class _P34Unbound:
    """Deliberately never bound in any container."""


class _P34Port:
    """The interface the gated implementation is bound under."""


@Singleton
@Requires(env="VARCO_TEST_P34_GATE", value="on")
class _P34Gated(_P34Port):
    """Active only when ``VARCO_TEST_P34_GATE=on`` — never set by the suite."""

    def __init__(self, dep: Inject[_P34Unbound]) -> None:
        self.dep = dep


class TestP34ValidateInactiveOwnerDeps:
    """
    Plan 052 (amendment 4) — P34, filed against providify 2.1.0, **resolved in
    providify 2.1.1** (inactive owners now skip the graph tier). Converted from a
    strict xfail into a regular regression guard.

    `validate()`'s graph tier walks the *own* injection points of every
    registered binding (`container.py:6442`, pass 2 at `:6529+`) without asking
    `_binding_is_active()` about the owner — only the candidates are filtered
    (`:2150`). So a `@Requires`-inactive binding, which `get()` can never
    construct, still reports its unbound dependencies as `MISSING_BINDING`
    ERRORs. varco hits it through `SAConfiguration.webhook_repository`'s
    required `FieldEncryptor` (`varco diagnose --check` exits 1).

    Report: `design/upstream-gaps/providify-validate-inactive-owner-deps.md`.
    """

    def test_inactive_owner_dependencies_are_not_missing_binding_errors(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The gate env var is unset, so the condition is OFF for this test.
        # (@Requires reads it at evaluation time, never at decoration time.)
        monkeypatch.delenv("VARCO_TEST_P34_GATE", raising=False)

        container = DIContainer()
        container.bind(_P34Port, _P34Gated)
        report = container.validate(raise_on_error=False)

        # Precondition (holds today): providify itself knows _Gated is inactive.
        assert not container.is_resolvable(_P34Port)
        assert any(i.kind is IssueKind.CONDITION_INACTIVE for i in report.infos)
        # Fixed in providify 2.1.1: an inactive owner's own dependencies are no
        # longer checked (2.1.0 reported two MISSING_BINDING ERRORs here).
        assert report.errors == (), [(i.kind, i.owner) for i in report.errors]
