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

from typing import TYPE_CHECKING, Any, ClassVar

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


class DIWiringConformance:
    """Shared behavioural contract for every package's ``bootstrap()``."""

    #: (interface, qualifier) pairs this package's bootstrap() registers as
    #: @Fallback defaults. Must be overridden by every subclass.
    expected_defaults: ClassVar[tuple[tuple[Any, str | None], ...]] = ()

    #: (IssueKind, owner) pairs tolerated by test_validate_is_clean /
    #: test_every_provider_singleton_has_a_reachable_disposer — each entry
    #: must be commented, in the subclass, with the plan number that owns
    #: fixing it (e.g. "# 050 — UNREACHABLE_PRE_DESTROY on X").
    known_warnings: ClassVar[frozenset[tuple[Any, str]]] = frozenset()

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
