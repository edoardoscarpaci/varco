"""
Plan 050 / Step 2 — the `LifecycleRunner` contract (§D-runner, §D-failures),
Docker-free, no FastAPI.

RED MODE: `varco_core.di.lifecycle` does not exist yet (Step 3 adds it) —
every test below is expected to fail with `ImportError`/`ModuleNotFoundError`.
"""

from __future__ import annotations

import logging

import pytest

# This import is expected to fail today (ImportError) — the module does not
# exist until Plan 050 / Step 3 lands.
from varco_core.di.lifecycle import (  # type: ignore[import-not-found]
    AbstractLifecycle,
    LifecycleComponent,
    LifecycleFailure,
    LifecycleRunner,
)


def _component(
    name: str, log: list[str], *, fail_start: bool = False, fail_stop: bool = False
) -> LifecycleComponent:
    async def start() -> None:
        log.append(f"start{name}")
        if fail_start:
            raise RuntimeError(f"start failed: {name}")

    async def stop() -> None:
        if fail_stop:
            log.append(f"stop{name}(raise)")
            raise RuntimeError(f"stop failed: {name}")
        log.append(f"stop{name}")

    return LifecycleComponent(name=name, phase=0, start=start, stop=stop)


async def test_start_order_is_given_order_and_stop_is_exact_reverse() -> None:
    # Confirms the happy path preserves the plan's ordering contract literally.
    log: list[str] = []
    components = [_component(str(i), log) for i in range(1, 6)]
    async with LifecycleRunner(components):
        pass
    assert log == [
        "start1",
        "start2",
        "start3",
        "start4",
        "start5",
        "stop5",
        "stop4",
        "stop3",
        "stop2",
        "stop1",
    ]


async def test_partial_start_rolls_back_in_reverse_and_never_touches_later_components() -> None:
    # Pins the exact `:253` regression this plan fixes: rollback must be
    # reverse order, not forward order.
    log: list[str] = []
    components = [
        _component("1", log),
        _component("2", log),
        _component("3", log, fail_start=True),
        _component("4", log),
        _component("5", log),
    ]
    with pytest.raises(RuntimeError, match="start failed: 3") as exc_info:
        async with LifecycleRunner(components):
            pass
    assert log == ["start1", "start2", "start3", "stop2", "stop1"]
    assert "start4" not in log
    assert "start5" not in log
    # identity of the original exception is preserved
    assert exc_info.value.args[0] == "start failed: 3"


async def test_partial_start_rollback_failure_is_attached_as_a_note() -> None:
    # A rollback stop() that itself raises must not replace or swallow the
    # original exception — it is recorded via add_note, and rollback continues.
    log: list[str] = []
    components = [
        _component("1", log),
        _component("2", log, fail_stop=True),
        _component("3", log, fail_start=True),
    ]
    with pytest.raises(RuntimeError, match="start failed: 3") as exc_info:
        async with LifecycleRunner(components):
            pass
    assert log == ["start1", "start2", "start3", "stop2(raise)", "stop1"]
    notes = getattr(exc_info.value, "__notes__", [])
    assert any("2" in note and "stop" in note for note in notes), notes


async def test_stop_failures_are_collected_not_raised_and_all_components_still_stop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Every component must still be stopped even when some stop() calls
    # raise, and the runner must never raise out of normal shutdown.
    log: list[str] = []
    components = [
        _component("1", log),
        _component("2", log, fail_stop=True),
        _component("3", log),
        _component("4", log, fail_stop=True),
        _component("5", log),
    ]
    caplog.set_level(logging.ERROR)
    runner = LifecycleRunner(components)
    async with runner:
        pass
    assert log == [
        "start1",
        "start2",
        "start3",
        "start4",
        "start5",
        "stop5",
        "stop4(raise)",
        "stop3",
        "stop2(raise)",
        "stop1",
    ]
    assert len(runner.last_failures) == 2
    names_failed = [f.name for f in runner.last_failures]
    assert names_failed == ["4", "2"]
    assert any(isinstance(f, LifecycleFailure) for f in runner.last_failures)
    # one aggregate summary line
    summary_lines = [
        r
        for r in caplog.records
        if "2 of 5" in r.message or "components failed to stop" in r.message
    ]
    assert summary_lines


async def test_shutdown_hook_runs_after_last_stop_even_when_setup_raised() -> None:
    log: list[str] = []
    shutdown_calls: list[str] = []

    async def setup() -> None:
        raise ValueError("setup exploded")

    async def shutdown() -> None:
        shutdown_calls.append("shutdown")

    components = [_component("1", log)]
    with pytest.raises(ValueError, match="setup exploded"):
        async with LifecycleRunner(components, setup=setup, shutdown=shutdown):
            pass
    assert "start1" not in log
    assert shutdown_calls == ["shutdown"]


async def test_shutdown_hook_shutdown_error_is_contained_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from providify import ShutdownError, ShutdownFailure  # type: ignore[import-not-found]

    log: list[str] = []

    async def shutdown() -> None:
        # NOTE (Plan 050 repair): providify's ShutdownFailure field is named
        # `owner`, not `name` (providify/exceptions.py) — fixed here per the
        # implementation-agent contract ("follow the plan, fix the test").
        raise ShutdownError(failures=[ShutdownFailure(owner="x", exception=ValueError("boom"))])

    caplog.set_level(logging.ERROR)
    async with LifecycleRunner([_component("1", log)], shutdown=shutdown):
        pass
    # nothing raised, one ERROR line per failure
    assert any("boom" in r.message or "boom" in str(r.exc_info) for r in caplog.records)


async def test_shutdown_hook_arbitrary_exception_is_contained() -> None:
    log: list[str] = []

    async def shutdown() -> None:
        raise ValueError("unexpected shutdown failure")

    # must not raise out of the context manager
    async with LifecycleRunner([_component("1", log)], shutdown=shutdown):
        pass


async def test_plan_reflects_realised_start_order() -> None:
    log: list[str] = []
    components = [_component(str(i), log) for i in range(1, 4)]
    runner = LifecycleRunner(components)
    async with runner:
        assert runner.plan == ("1", "2", "3")


async def test_plan_is_empty_after_a_setup_failure() -> None:
    async def setup() -> None:
        raise ValueError("nope")

    runner = LifecycleRunner([_component("1", [])], setup=setup)
    with pytest.raises(ValueError, match="nope"):
        async with runner:
            pass
    assert runner.plan == ()


def test_abstract_lifecycle_is_the_same_object_from_both_import_paths() -> None:
    from varco_fastapi.lifespan import (  # type: ignore[import-not-found]
        AbstractLifecycle as FastAPIAbstractLifecycle,
    )

    assert FastAPIAbstractLifecycle is AbstractLifecycle
