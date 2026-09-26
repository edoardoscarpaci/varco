"""
DIWiringConformance — shared contract tests for every package's
``bootstrap(container: DIContainer) -> DIContainer`` (Plan 046 / DI-13,
§D-suite).

The ninth conformance module. Same shape as every other module in this
package (see ``__init__.py``'s docstring): a base class **not** named
``Test*`` (never collected standalone), one abstract fixture
(``bootstrap``) raising ``NotImplementedError`` by default, subclassed per
package with two class attributes replacing hand-written assertions::

    from varco_conformance.di_wiring import DIWiringConformance

    class TestRedisDIWiringConformance(DIWiringConformance):
        expected_defaults = (
            (AbstractEventBus, None),
            (AbstractEventBus, "redis"),
            ...
        )

        @pytest.fixture
        def bootstrap(self):
            def _bootstrap(container):
                varco_core.di.bootstrap(container)
                varco_redis.di.bootstrap(container)
                return container
            return _bootstrap

``fallback_bindings(container)`` is the one private read in this package —
``container._bindings`` — centralised here because
``providify.descriptor.BindingDescriptor`` carries no ``fallback`` field
(``descriptor.py:49-54``) and every public read (``describe()``,
``get_binding()``, ``get_all_bindings()``) drops shadowed fallbacks once a
user binding exists (``container.py:2159-2169``). This is a shim intended
for deletion once P29 ships (``design/upstream-gaps/
providify-descriptor-fallback-field.md``); guarded by
``varco_core/tests/test_providify_contracts.py::
test_binding_descriptor_carries_fallback`` (a ``strict=True`` xfail).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple

import pytest
from providify import DIContainer, IssueKind

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


def fallback_bindings(container: DIContainer) -> list[Any]:
    """
    Return every binding registered on ``container`` whose ``.fallback`` is
    ``True`` — the one private read in this package (see module docstring).

    Args:
        container: The container to inspect.

    Returns:
        A list of raw binding objects (``ClassBinding``/``ProviderBinding``)
        with ``.fallback is True``.

    Edge cases:
        Reads ``container._bindings`` directly — a providify-internal
        attribute. Delete this shim once P29 lands a public
        ``fallback``-aware read.
    """
    return [b for b in container._bindings if getattr(b, "fallback", False) is True]


class Selectable(NamedTuple):
    """One backend binding this package should have gated on
    `VARCO_<SEAM>_BACKEND` (Plan 047 §D-suite)."""

    seam: str
    value: str
    interface: Any
    qualifier: str | None
    owner_substr: str


class DIWiringConformance:
    """Shared behavioural contract for every package's ``bootstrap()``."""

    #: (interface, qualifier) pairs this package's bootstrap() registers as
    #: @Fallback defaults. Must be overridden by every subclass.
    expected_defaults: ClassVar[tuple[tuple[Any, str | None], ...]] = ()

    #: (seam, value, interface, qualifier, owner_substr) tuples this package's
    #: backend bindings should be gated on (Plan 047 §D-suite). Empty tuple
    #: (the default) skips the three `selectable`-driven tests below.
    selectable: ClassVar[tuple[Selectable, ...]] = ()

    #: (IssueKind, owner) pairs tolerated by test_validate_is_clean /
    #: test_every_provider_singleton_has_a_reachable_disposer — each entry
    #: must be commented, in the subclass, with the plan number that owns
    #: fixing it (e.g. "# 050 — UNREACHABLE_PRE_DESTROY on X").
    known_warnings: ClassVar[frozenset[tuple[Any, str]]] = frozenset()

    #: Settings classes this package's bootstrap() binds via
    #: varco_core.settings.bind_settings() (Plan 048 / Step 18). Empty tuple
    #: (the default) skips the settings-uniqueness test below.
    settings_classes: ClassVar[tuple[type, ...]] = ()

    @pytest.fixture
    def bootstrap(self) -> Callable[[DIContainer], DIContainer]:
        """Abstract — must be overridden by every subclass.

        Returns a callable ``(container) -> container`` composing this
        package's ``bootstrap(container)`` (and any siblings it needs, e.g.
        ``varco_core.di.bootstrap`` first) with any settings a live
        ``validate()`` requires (an ``SAConfig``, a ``BeanieSettings``).
        """
        raise NotImplementedError(
            "DIWiringConformance subclasses must override the `bootstrap` fixture."
        )

    @pytest.fixture
    async def container(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> Iterable[DIContainer]:
        c = DIContainer()
        yield bootstrap(c)
        await c.ashutdown()

    async def test_bootstrap_returns_the_same_container(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> None:
        c = DIContainer()
        assert bootstrap(c) is c

    async def test_bootstrap_is_idempotent(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> None:
        c = DIContainer()
        bootstrap(c)
        before = len(c.describe().bindings)
        report_before = c.validate(raise_on_error=False)
        kinds_before = {i.kind for i in report_before.errors}

        bootstrap(c)

        assert len(c.describe().bindings) == before
        report_after = c.validate(raise_on_error=False)
        kinds_after = {i.kind for i in report_after.errors}
        assert kinds_after == kinds_before
        assert IssueKind.AMBIGUOUS_BINDING not in kinds_after

    async def test_bootstrap_never_touches_the_global(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> None:
        sentinel = DIContainer()
        with DIContainer.scoped(sentinel):
            bootstrap(DIContainer())

        assert len(sentinel.describe().bindings) == 0

    async def test_validate_is_clean(self, container: DIContainer) -> None:
        report = container.validate(raise_on_error=False)

        assert report.errors == ()

        observed_warnings = {(i.kind, getattr(i, "owner", "")) for i in report.warnings}
        assert observed_warnings - self.known_warnings == set()

        observed_infos = {i.kind for i in report.infos}
        assert observed_infos <= {IssueKind.FALLBACK_SHADOWED, IssueKind.CONDITION_INACTIVE}

    async def test_every_provider_singleton_has_a_reachable_disposer(
        self, container: DIContainer
    ) -> None:
        report = container.validate(raise_on_error=False)
        disposer_kinds = {
            IssueKind.UNREACHABLE_PRE_DESTROY,
            IssueKind.UNMATCHED_DISPOSER,
            IssueKind.DISPOSER_OVERWRITTEN,
        }
        observed = {
            (i.kind, getattr(i, "owner", "")) for i in report.warnings if i.kind in disposer_kinds
        }
        assert observed - self.known_warnings == set()

    async def test_settings_bound_once_after_two_bootstrap_calls(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> None:
        """Plan 048 / Step 18 — every settings class this package binds
        via bind_settings() must have exactly one binding after bootstrap()
        is called twice (the same idempotency contract as any other
        singleton).
        """
        if not self.settings_classes:
            pytest.skip("no settings_classes declared")

        c = DIContainer()
        bootstrap(c)
        bootstrap(c)

        for cls in self.settings_classes:
            assert len(c.get_all_bindings(cls)) == 1, f"{cls!r} bound more than once"

    async def test_declared_defaults_are_fallbacks(self, container: DIContainer) -> None:
        for interface, qualifier in self.expected_defaults:
            binding = container.get_binding(interface, qualifier=qualifier)
            assert binding.fallback is True, f"{interface!r}@{qualifier!r} is not a fallback"

    async def test_every_default_is_overridable_after_bootstrap(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> None:
        from unittest.mock import MagicMock

        from varco_core.di import bind_instance

        for interface, qualifier in self.expected_defaults:
            c = DIContainer()
            bootstrap(c)
            bind_instance(c, interface, MagicMock(spec=interface), qualifier=qualifier)

            binding = c.get_binding(interface, qualifier=qualifier)
            assert binding.fallback is False
            for b in c.get_all_bindings(interface, qualifier=qualifier):
                assert b.fallback is False

    async def test_declared_defaults_are_complete(self, container: DIContainer) -> None:
        observed = {
            (b.interface, b.qualifier)
            for b in fallback_bindings(container)
            if not getattr(b, "exact_only", False)
        }
        assert observed == set(self.expected_defaults)

    async def test_unselected_backend_is_inactive_and_default_serves(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> None:
        if not self.selectable:
            pytest.skip("no selectable backends declared")

        import os

        from varco_core.di.seams import seam_by_name

        for entry in self.selectable:
            seam = seam_by_name(entry.seam)
            old = os.environ.pop(seam.env, None)
            try:
                c = DIContainer()
                bootstrap(c)

                # DESIGN (repair, §D-vocab's own documented Edge case:
                # "get(T, qualifier='kafka') while unselected -> LookupError")
                #   A backend whose own qualifier equals its selected value
                #   name (e.g. KafkaEventBus, qualifier="kafka") is invisible
                #   to EVERY lookup while inactive, including one that asks
                #   for that exact qualifier — providify's qualified-request
                #   narrowing never falls back to an unqualified/differently
                #   -qualified candidate (container.py:2096-2101). So "the
                #   default serves" must be observed via the UNQUALIFIED
                #   request (what an ordinary Inject[Seam] call site actually
                #   resolves), never via entry.qualifier, which names the
                #   *backend's* qualifier, not the seam's default one. Using
                #   entry.qualifier here was unreachable-by-construction for
                #   every entry whose qualifier is backend-specific (kafka/
                #   nats/redis/layered/...) and only ever passed for entries
                #   qualified None (dlq, revocation) — fixed once, centrally,
                #   for every subclass.
                binding = c.get_binding(entry.interface)
                assert binding.fallback is True
                assert binding.priority == 0

                report = c.validate(raise_on_error=False)
                assert any(
                    i.kind == IssueKind.CONDITION_INACTIVE and "is unset" in str(i)
                    for i in report.infos
                )
                assert not any(i.kind == IssueKind.AMBIGUOUS_BINDING for i in report.errors)
            finally:
                if old is not None:
                    os.environ[seam.env] = old

    async def test_selected_backend_is_a_fallback_at_backend_priority(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> None:
        if not self.selectable:
            pytest.skip("no selectable backends declared")

        import os

        from varco_core.di import BACKEND_PRIORITY
        from varco_core.di.seams import seam_by_name

        for entry in self.selectable:
            seam = seam_by_name(entry.seam)
            old = os.environ.get(seam.env)
            os.environ[seam.env] = entry.value
            try:
                c = DIContainer()
                bootstrap(c)

                binding = c.get_binding(entry.interface, qualifier=entry.qualifier)
                assert binding.fallback is True
                assert binding.priority == BACKEND_PRIORITY
                assert entry.owner_substr in str(getattr(binding, "owner", binding))

                report = c.validate(raise_on_error=False)
                assert not any(
                    i.kind == IssueKind.CONDITION_INACTIVE
                    and entry.owner_substr in str(getattr(i, "owner", ""))
                    for i in report.infos
                )
                assert not any(i.kind == IssueKind.AMBIGUOUS_BINDING for i in report.errors)
            finally:
                if old is None:
                    os.environ.pop(seam.env, None)
                else:
                    os.environ[seam.env] = old

    async def test_code_binding_beats_env_selection(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> None:
        if not self.selectable:
            pytest.skip("no selectable backends declared")

        import os
        from unittest.mock import MagicMock

        from varco_core.di import bind_instance
        from varco_core.di.seams import seam_by_name

        for entry in self.selectable:
            seam = seam_by_name(entry.seam)
            old = os.environ.get(seam.env)
            os.environ[seam.env] = entry.value
            try:
                c = DIContainer()
                bootstrap(c)
                bind_instance(
                    c, entry.interface, MagicMock(spec=entry.interface), qualifier=entry.qualifier
                )

                binding = c.get_binding(entry.interface, qualifier=entry.qualifier)
                assert binding.fallback is False
            finally:
                if old is None:
                    os.environ.pop(seam.env, None)
                else:
                    os.environ[seam.env] = old

    async def test_unknown_value_fails_check(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> None:
        if not self.selectable:
            pytest.skip("no selectable backends declared")

        import os

        from varco_core.di import check_backend_selection
        from varco_core.di.errors import BackendSelectionError
        from varco_core.di.seams import seam_by_name

        entry = self.selectable[0]
        seam = seam_by_name(entry.seam)
        old = os.environ.get(seam.env)
        os.environ[seam.env] = "nope"
        try:
            c = DIContainer()
            bootstrap(c)
            with pytest.raises(BackendSelectionError) as exc:
                check_backend_selection(c)
            assert "nope" in str(exc.value)
        finally:
            if old is None:
                os.environ.pop(seam.env, None)
            else:
                os.environ[seam.env] = old

    async def test_catalogue_classifies_every_default(self, container: DIContainer) -> None:
        from varco_core.di import catalogue

        result = catalogue(container)
        classified = {
            (row.interface, row.qualifier) for row in getattr(result, "all_fallbacks", ())
        }
        for interface, qualifier in self.expected_defaults:
            assert (interface, qualifier) in classified or classified == set(), (
                "catalogue() must classify every declared default"
            )
