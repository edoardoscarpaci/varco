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
from providify import DIContainer, IssueKind, Profile, Singleton

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
