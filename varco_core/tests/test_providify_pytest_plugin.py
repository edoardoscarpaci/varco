"""
Assert providify 2.0.0's ``pytest11`` plugin (``providify/pytest_plugin.py``)
is active with **NO** conftest.py change anywhere in this repo (Plan 016 /
RL-3d, Design §RL-3d, Step 31).

The plugin ships four function-scoped, yield-based, non-autouse fixtures —
``di_container``, ``di_acontainer``, ``di_overrides``, ``di_global`` — and
its own module docstring states the invariant this file locks in: *"a
project that never asks for [any of them] must see zero behavioural
difference from providify not being installed at all."*

varco deliberately does NOT re-export or wrap these fixtures from its own
testkit (Design §RL-3d) — this file exercises the plugin's fixtures
directly, requested by ordinary test functions, with no conftest.py in
this directory contributing anything.

Two of the fixture-interaction assertions (``di_overrides`` undone at
teardown, ``di_global`` restored at teardown) are proven by driving the
fixture generator functions directly via their ``__wrapped__`` attribute
(the plain Python generator function pytest wraps) rather than through a
full nested pytest run — this observes the exact same code path a real
test's fixture teardown executes, without the overhead/fragility of an
in-process pytest subprocess.

Thread safety:  N/A (unit tests)
Async safety:   ✅ ``di_acontainer``-related tests are ``async def`` — this
                   repo's ``asyncio_mode = "auto"`` picks them up with no
                   ``@pytest.mark.asyncio`` marker needed.
"""

from __future__ import annotations

import providify.pytest_plugin as _plugin
from providify import DIContainer, Singleton
from providify.testing import ContainerOverrides

# ── Fixtures under test: no import needed for pytest to discover them —
# they arrive automatically via the pytest11 entry point. Requesting them
# by name in a test signature below is the entire point of this file: no
# conftest.py in this directory (or any parent) defines di_container,
# di_acontainer, di_overrides, or di_global.


# NOTE: `di_global` restores the previous global at teardown, so this holds in
# any order — provided no OTHER test leaks a global via a bare
# `DIContainer.current()` (Plan 059 / Step 3 fixed test_retention_di.py's).
def test_requesting_no_providify_fixture_observes_no_container_related_global_state() -> None:
    """
    A test that requests none of the four fixtures must see the same
    absence of global DI state as if providify's pytest plugin were not
    installed at all — the module's own stated invariant.
    """
    assert DIContainer._global is None


def test_di_container_fixture_yields_a_fresh_di_container(
    di_container: DIContainer,
) -> None:
    """The `di_container` fixture hands back a real, empty DIContainer."""
    assert isinstance(di_container, DIContainer)


# Module-level accumulator shared by the next two tests to prove function
# scope produces a DIFFERENT container object per test. Plan 059 / Step 3:
# the two tests are symmetric, so whichever pytest-randomly runs second does
# the comparison — no ordering assumption. The containers themselves are
# kept (not their id()s) so a garbage-collected first container can never
# have its id reused by the second.
_seen_containers: list[DIContainer] = []


def _record_and_assert_fresh(di_container: DIContainer) -> None:
    """Assert ``di_container`` is not one an earlier test of the pair received.

    Args:
        di_container: The fixture value this test received.
    """
    assert all(seen is not di_container for seen in _seen_containers)
    _seen_containers.append(di_container)


def test_di_container_fixture_first_observation(di_container: DIContainer) -> None:
    """One of a symmetric pair of tests recording the container's identity."""
    _record_and_assert_fresh(di_container)


def test_di_container_fixture_is_function_scoped(di_container: DIContainer) -> None:
    """
    A second test requesting `di_container` must get a DIFFERENT object —
    function scope, not session/module scope.
    """
    _record_and_assert_fresh(di_container)


def test_di_overrides_fixture_is_container_overrides_bound_to_the_container(
    di_container: DIContainer, di_overrides: ContainerOverrides
) -> None:
    """`di_overrides` is a real ContainerOverrides wrapping this test's di_container."""
    assert isinstance(di_overrides, ContainerOverrides)
    assert di_overrides._container is di_container


def test_di_overrides_are_undone_at_fixture_teardown() -> None:
    """
    Drives `di_container` + `di_overrides`' generator bodies directly (via
    `.__wrapped__`) to observe the exact teardown code path: an override
    made mid-test must be reverted once the fixture generator is advanced
    past its `yield` (i.e. at real pytest teardown time).
    """

    @Singleton
    class _Real:
        pass

    @Singleton
    class _Fake(_Real):
        pass

    container_gen = _plugin.di_container.__wrapped__()
    container = next(container_gen)

    overrides_gen = _plugin.di_overrides.__wrapped__(container)
    overrides = next(overrides_gen)

    container.bind(_Real, _Real)
    overrides.bind(_Real, _Fake)
    assert type(container.get(_Real)) is _Fake

    # Advance past the fixture's `yield` — this is di_overrides' teardown.
    try:
        next(overrides_gen)
    except StopIteration:
        pass

    assert type(container.get(_Real)) is _Real, (
        "di_overrides must undo its override once its fixture body resumes "
        "past yield (real pytest teardown)"
    )

    # Advance past di_container's `yield` too, mirroring real teardown order.
    try:
        next(container_gen)
    except StopIteration:
        pass


def test_di_global_fixture_makes_current_return_it(
    di_container: DIContainer, di_global: DIContainer
) -> None:
    """`di_global` installs di_container as DIContainer.current() for the test."""
    assert di_global is di_container
    assert DIContainer.current() is di_container


def test_di_global_fixture_restores_previous_global_at_teardown() -> None:
    """
    Drives `di_container` + `di_global`'s generator bodies directly to
    observe that DIContainer.current() reverts once di_global's fixture
    body resumes past its `yield` (real pytest teardown).
    """
    previous_global = DIContainer._global

    container_gen = _plugin.di_container.__wrapped__()
    container = next(container_gen)

    global_gen = _plugin.di_global.__wrapped__(container)
    installed = next(global_gen)

    assert installed is container
    assert DIContainer.current() is container

    try:
        next(global_gen)
    except StopIteration:
        pass

    assert DIContainer._global is previous_global, (
        "di_global must restore the previous global container at teardown"
    )

    try:
        next(container_gen)
    except StopIteration:
        pass


async def test_di_acontainer_fixture_works_under_asyncio_mode_auto(
    di_acontainer: DIContainer,
) -> None:
    """
    `di_acontainer` is usable in an `async def` test with no
    `@pytest.mark.asyncio` marker — this repo's `asyncio_mode = "auto"`
    (set in pyproject.toml, not a conftest.py) is sufficient on its own.
    """
    assert isinstance(di_acontainer, DIContainer)


async def test_di_acontainer_ashutdown_is_awaited_at_teardown() -> None:
    """
    Drives `di_acontainer`'s async generator body directly to confirm its
    teardown calls `await container.ashutdown()` (not the sync
    `shutdown()`) — verified by using an async `@PreDestroy` hook, which
    only `ashutdown()` can await without raising.
    """
    from providify import PreDestroy

    hook_called = False

    @Singleton
    class _AsyncOnly:
        @PreDestroy
        async def close(self) -> None:
            nonlocal hook_called
            hook_called = True

    agen = _plugin.di_acontainer.__wrapped__()
    container = await agen.__anext__()
    container.bind(_AsyncOnly, _AsyncOnly)
    container.get(_AsyncOnly)

    try:
        await agen.__anext__()
    except StopAsyncIteration:
        pass

    assert hook_called, "di_acontainer's teardown must await ashutdown()"
