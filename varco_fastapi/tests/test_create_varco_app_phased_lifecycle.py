"""
Plan 050 / Step 5 — `create_varco_app` sequences lifecycle by
`phased_components()` (phase, then module order), not by raw registration
order (§D-runner / §D-phase). Docker-free.

RED MODE: `varco_core.di.lifecycle` (`Phase`, `phased_components`) does not
exist yet, and `create_varco_app` has not been switched to it — these tests
are expected to fail with `ImportError`/assertion mismatches.
"""

from __future__ import annotations

from providify import DIContainer
from varco_core.di.lifecycle import Phase  # type: ignore[import-not-found]
from varco_core.di.module import VarcoModule
from varco_fastapi.app import create_varco_app


def _phased_spy(name: str, phase: int, calls: list[str], **kwargs: object) -> VarcoModule:
    # NOTE (Plan 050 repair): `_register` is an `@abstractmethod` on
    # `VarcoModule` — ABCMeta freezes `__abstractmethods__` at class-body
    # evaluation time, so assigning `_Spy._register = ...` *after*
    # `class _Spy(VarcoModule): pass` (the plan's original sketch) leaves
    # the class permanently abstract and uninstantiable. Defining
    # `_register`/`start`/`stop` in the class body itself (via a `type()`
    # namespace dict, since the closures need `name`/`calls`) fixes it.
    def _register(self: VarcoModule, container: DIContainer) -> None:
        calls.append(f"{name}.register")

    async def start(self: VarcoModule, container: DIContainer) -> None:
        calls.append(f"{name}.start")

    async def stop(self: VarcoModule, container: DIContainer) -> None:
        calls.append(f"{name}.stop")

    namespace: dict[str, object] = {
        "name": name,
        "phase": phase,
        "_register": _register,
        "start": start,
        "stop": stop,
        **kwargs,
    }
    _Spy = type(f"Spy{name}", (VarcoModule,), namespace)
    return _Spy()


async def test_a_module_listed_after_another_still_starts_first_if_its_phase_is_earlier() -> None:
    # Plan's example: "Migrations(INFRA) listed after Core still starts
    # before core.bus and stops after it" — here modelled directly with the
    # Phase enum rather than the real CoreModule/MigrationsModule pair, to
    # isolate the ordering primitive from other steps' unfinished work.
    calls: list[str] = []
    later_but_earlier_phase = _phased_spy("Migrations", Phase.INFRA, calls)
    earlier_but_later_phase = _phased_spy("Bus", Phase.BUS, calls)

    container = DIContainer()
    app = create_varco_app(
        container,
        modules=[earlier_but_later_phase, later_but_earlier_phase],
        validate=False,
    )

    async with app.router.lifespan_context(app):
        pass

    start_calls = [c for c in calls if c.endswith(".start")]
    stop_calls = [c for c in calls if c.endswith(".stop")]

    assert start_calls == ["Migrations.start", "Bus.start"]
    assert stop_calls == ["Bus.stop", "Migrations.stop"]
