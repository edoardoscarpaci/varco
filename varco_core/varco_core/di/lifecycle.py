"""
varco_core.di.lifecycle
========================
Phased startup/shutdown with a reverse-order guarantee (Plan 050 / DI-9).

``Phase`` names five coarse, anonymous start-order buckets a ``VarcoModule``
declares itself into; ``VarcoModule.depends_on`` (``varco_core.di.module``)
remains the fine, *named* ordering within/across phases — both, as Spring's
``SmartLifecycle`` does (``design/research/014-multi-binding-qualifiers-and-conditional-di.md``
§5). ``resolve_lifecycle_order`` combines 049's ``resolve_module_order`` DFS
with a stable sort by phase, raising ``ModulePhaseError`` the moment a
``depends_on`` edge would require starting *after* a module scheduled to
start later — a wiring mistake caught at ``create_varco_app()`` time, never
silently rescheduled (§D-phase alternative (2), rejected).

``LifecycleRunner`` is the actual start/stop engine, extracted from
``varco_fastapi.lifespan.VarcoLifespan`` so it is usable Docker-free,
FastAPI-free (a future worker host) and unit-testable without any ASGI
machinery (§D-runner). It fixes the pre-050 bug where a partial-start
rollback stopped already-started components in **forward** order
(``lifespan.py:253`` — the exact regression ``test_lifecycle_runner.py``
pins by name) and adds the three invariants "phase is coarse/depends_on is
fine, both at once" needs:

- start in the given order; stop the **exact reverse of what actually
  started** — both on the happy path and when rolling back a partial start.
- a rollback ``stop()`` failure never replaces or swallows the *original*
  start failure — it is attached via ``BaseException.add_note`` (3.11+; this
  repo's floor is 3.12) and rollback continues.
- normal-shutdown ``stop()`` failures are collected, never raised (an
  ``asynccontextmanager``'s ``finally`` is the wrong place to raise — it
  would mask the real cause during ASGI shutdown), and are exposed via
  ``last_failures`` plus one aggregate log line.

Thread safety:  ⚠️ Single-threaded startup/shutdown only (same contract as
                    ``VarcoModule``'s own slots).
Async safety:   ✅ Every hook (``start``/``stop``/``setup``/``shutdown``) is
                    ``async def``.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from providify import DIContainer

    from varco_core.di.module import VarcoModule

_logger = logging.getLogger(__name__)


class Phase(IntEnum):
    """
    The five coarse, anonymous start-order buckets (§D-phase). Lower starts
    first; shutdown is the exact reverse of what actually started — never a
    second, independent "shutdown phase" ordering (Spring's model, brief 014
    §5).

    Room is left between values (steps of 100) so an app may insert itself
    at ``Phase.BUS + 50`` etc., the same escape hatch Spring's integer phase
    offers.
    """

    #: Migrations, tenancy pools, trust stores, catalogs, stores
    #: (DLQ/idempotency/job/revocation), repositories, caches.
    INFRA = 100
    #: The event bus.
    BUS = 200
    #: EventConsumers, OutboxRelay, WebhookDispatcher, WS/SSE adapters,
    #: AuditConsumer — the default phase for app modules.
    CONSUMERS = 300
    #: AbstractJobRunner, RetentionScheduler, JWKS background refresh.
    SCHEDULERS = 400
    #: SecurityPosture preflight — observes the fully-started world.
    EDGE = 500


@runtime_checkable
class AbstractLifecycle(Protocol):
    """
    Protocol for components with an async start/stop lifecycle.

    Moved here from ``varco_fastapi.lifespan`` (§D-runner) so a
    Docker-free, FastAPI-free worker host can depend on
    ``varco_core.di.lifecycle`` alone. Re-exported, identity-preserved, from
    ``varco_fastapi.lifespan`` — every ``isinstance(x, AbstractLifecycle)``
    call site (e.g. ``test_tls_reload_lifecycle``) keeps passing unchanged.

    Thread safety:  ✅ Protocol check is read-only.
    Async safety:   ✅ Both methods are ``async def``.
    """

    async def start(self) -> None:
        """Start the component. Idempotent."""
        ...

    async def stop(self) -> None:
        """Stop the component cleanly. Idempotent."""
        ...


@dataclass(frozen=True)
class LifecycleComponent:
    """
    One phased, named start/stop unit contributed by a ``VarcoModule``
    (§D-components).

    A module contributes exactly one of these by default (its own
    ``start``/``stop`` slots, wrapped); ``CoreModule``/``WebhookModule``
    override ``components()`` to contribute several, spanning phases.

    Args:
        name: A greppable identifier (e.g. ``"core.bus"``) — 052's
            ``varco diagnose`` prints this.
        phase: The ``Phase`` (or plain ``int``) this component starts in.
        start: A zero-argument async callable. Idempotent.
        stop: A zero-argument async callable. Idempotent.
    """

    name: str
    phase: int
    start: Callable[[], Awaitable[None]]
    stop: Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class LifecycleFailure:
    """
    One component's ``stop()`` failure, recorded rather than raised
    (§D-failures).

    Args:
        name: The failing component's ``LifecycleComponent.name``.
        exception: The exception ``stop()`` raised.
    """

    name: str
    exception: BaseException


class ModulePhaseError(ValueError):
    """
    Raised by ``resolve_lifecycle_order`` when a module's ``depends_on``
    names a module scheduled to start in a **later** phase — the module
    would have to start after something it depends on, which no ordering
    can satisfy (§D-phase).

    Args:
        module: The ``VarcoModule`` (instance or class) whose ``depends_on``
            pointed forward.
        dependency: The ``VarcoModule`` (instance or class) in the later
            phase.
    """

    def __init__(self, module: Any, dependency: Any) -> None:
        self.module = module
        self.dependency = dependency
        module_name = getattr(module, "name", None) or type(module).__name__
        dep_name = getattr(dependency, "name", None) or type(dependency).__name__
        super().__init__(
            f"{module_name!r} (phase={getattr(module, 'phase', '?')!r}) depends_on "
            f"{dep_name!r}, which is scheduled for a LATER phase "
            f"(phase={getattr(dependency, 'phase', '?')!r}) — a module cannot start "
            f"after something it depends on. Move the dependency to an earlier or "
            f"equal phase, or drop the depends_on edge."
        )


def resolve_lifecycle_order(modules: Sequence[VarcoModule]) -> list[VarcoModule]:
    """
    Order *modules* for lifecycle purposes: 049's ``resolve_module_order``
    DFS-post-order (dependencies first), then a **stable** sort by
    ``phase`` (§D-phase).

    Args:
        modules: The modules to order.

    Returns:
        A new list, dependency order preserved within a phase, earlier
        phases first across modules with no dependency relationship.

    Raises:
        ModulePhaseError: A module's ``depends_on`` names a module in a
            strictly later phase.
        ValueError: (from ``resolve_module_order``) duplicate module class.
        ModuleDependencyError: (from ``resolve_module_order``) an unresolved
            ``depends_on`` target.
        ModuleCycleError: (from ``resolve_module_order``) a dependency cycle.
    """
    from varco_core.di.module import resolve_module_order  # noqa: PLC0415

    for module in modules:
        module_phase = getattr(module, "phase", Phase.CONSUMERS)
        for dep_cls in getattr(module, "depends_on", ()):
            dep = next((m for m in modules if type(m) is dep_cls), None)
            if dep is None:
                continue  # resolve_module_order raises ModuleDependencyError for this
            dep_phase = getattr(dep, "phase", Phase.CONSUMERS)
            if dep_phase > module_phase:
                raise ModulePhaseError(module, dep)

    ordered = resolve_module_order(modules)
    return sorted(ordered, key=lambda m: getattr(m, "phase", Phase.CONSUMERS))


def phased_components(
    modules: Sequence[VarcoModule], container: DIContainer
) -> list[LifecycleComponent]:
    """
    Flatten every module's ``components(container)`` and stable-sort the
    result by ``phase`` (§D-components) — a single module's own components
    may span several phases (``CoreModule``: stores/INFRA, bus/BUS,
    consumers/CONSUMERS, job_runner/SCHEDULERS).

    Args:
        modules: Modules, in the order ``resolve_lifecycle_order`` returned
            (dependency order is the tiebreak within one phase).
        container: The ``DIContainer`` passed through to each module's
            ``components()``.

    Returns:
        Every contributed ``LifecycleComponent``, ascending by phase, module
        order as the tiebreak.
    """
    flat: list[LifecycleComponent] = []
    for module in modules:
        flat.extend(module.components(container))
    return sorted(flat, key=lambda c: c.phase)


class LifecycleRunner:
    """
    Starts a sequence of ``LifecycleComponent``/``AbstractLifecycle``
    objects in order and stops **exactly the reverse of what actually
    started** (§D-runner), whether that is a clean exit or a rollback after
    a partial-start failure.

    Args:
        components: The components to run, already ordered (typically
            ``phased_components()``'s output).
        setup: Optional async hook awaited before any component starts
            (e.g. ``await container.ainstall(SomeConfiguration)``). No
            component starts if this raises.
        shutdown: Optional async hook awaited **after** the last ``stop()``
            call — even when ``setup`` raised. Never raises out of this
            runner; failures are logged and contained (§D-failures) —
          typically ``container.ashutdown()``.

    Attributes:
        last_failures: Every ``stop()`` failure from the most recent normal
            shutdown, in stop order. Empty after a rollback (the rollback
            path attaches failures to the propagating exception instead —
            see ``__aexit__``).
        plan: Component names in realised start order. Empty before
            ``__aenter__`` runs and after a ``setup`` failure.

    Thread safety:  ⚠️ Single-threaded startup/shutdown only.
    Async safety:   ✅ Every hook is ``async def``.
    """

    def __init__(
        self,
        components: Sequence[LifecycleComponent | AbstractLifecycle],
        *,
        setup: Callable[[], Awaitable[None]] | None = None,
        shutdown: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._components: list[LifecycleComponent] = [
            _as_component(c, i) for i, c in enumerate(components)
        ]
        self._setup = setup
        self._shutdown = shutdown
        self._started: list[LifecycleComponent] = []
        self.last_failures: tuple[LifecycleFailure, ...] = ()
        self.plan: tuple[str, ...] = ()

    async def __aenter__(self) -> LifecycleRunner:
        """
        Run ``setup``, then start every component in order, recording each
        as it succeeds. On a start failure, roll back the already-started
        components in **exact reverse** order (the ``:253`` fix), attach any
        rollback-stop failure to the original exception via ``add_note``,
        and re-raise the original exception unchanged.

        Returns:
            ``self``.

        Raises:
            Exception: Whatever ``setup`` or a component's ``start()``
                raised, propagated with identity preserved.
        """
        self._started = []
        try:
            if self._setup is not None:
                await self._setup()

            for component in self._components:
                await component.start()
                self._started.append(component)
                _logger.info("LifecycleRunner: started %s", component.name)
        except Exception as exc:
            await self._rollback(exc)
            # __aexit__ is never called by `async with` when __aenter__
            # itself raises — so the shutdown hook (in practice
            # container.ashutdown()) must run here, not there, to keep its
            # documented "runs even when setup()/start() raised" contract.
            await self._run_shutdown_hook()
            raise

        self.plan = tuple(c.name for c in self._started)
        return self

    async def _rollback(self, original: BaseException) -> None:
        """
        Stop every already-started component in exact reverse order,
        attaching any rollback failure as a note on *original* rather than
        replacing it (§D-failures).

        Args:
            original: The exception a ``start()`` call raised.
        """
        for component in reversed(self._started):
            try:
                await component.stop()
                _logger.info("LifecycleRunner: rolled back %s", component.name)
            except Exception as rollback_exc:  # noqa: BLE001 — recorded, never replaces original
                _logger.error(
                    "LifecycleRunner: rollback stop() of %s raised: %r",
                    component.name,
                    rollback_exc,
                    exc_info=True,
                )
                original.add_note(
                    f"while rolling back, stop() of {component.name} raised: {rollback_exc!r}"
                )
        self._started = []

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        """
        Stop every started component in exact reverse order, collecting
        (never raising) stop failures, then await the ``shutdown`` hook
        (§D-failures). Always runs, including when the body under
        ``async with`` raised.
        """
        try:
            failures = await self._stop_all(list(reversed(self._started)))
            self.last_failures = tuple(failures)
            if failures:
                _logger.error(
                    "LifecycleRunner: %d of %d components failed to stop: %s",
                    len(failures),
                    len(self._started),
                    [f.name for f in failures],
                )
            self._started = []
        finally:
            await self._run_shutdown_hook()

    async def _stop_all(self, components: list[LifecycleComponent]) -> list[LifecycleFailure]:
        """
        Stop every component in *components* (already in the desired stop
        order), logging but never raising a failure.

        Args:
            components: Components to stop, in stop order.

        Returns:
            One ``LifecycleFailure`` per component whose ``stop()`` raised,
            in stop order.
        """
        failures: list[LifecycleFailure] = []
        for component in components:
            try:
                await component.stop()
                _logger.info("LifecycleRunner: stopped %s", component.name)
            except Exception as exc:  # noqa: BLE001 — collected, never raised
                _logger.error(
                    "LifecycleRunner: error stopping %s: %s",
                    component.name,
                    exc,
                    exc_info=True,
                )
                failures.append(LifecycleFailure(name=component.name, exception=exc))
        return failures

    async def _run_shutdown_hook(self) -> None:
        """
        Await the optional ``shutdown`` hook, containing every failure —
        moved verbatim from ``VarcoLifespan._run_shutdown_hook`` (§D-runner).
        """
        if self._shutdown is None:
            return
        from providify.exceptions import ShutdownError  # noqa: PLC0415

        try:
            await self._shutdown()
        except ShutdownError as exc:
            for failure in exc.failures:
                _logger.error(
                    "LifecycleRunner: container teardown failed for %s: %s",
                    failure.owner,
                    failure.exception,
                    exc_info=failure.exception,
                )
        except Exception as exc:  # noqa: BLE001 — contained, same contract as VarcoLifespan's
            _logger.error(
                "LifecycleRunner: container shutdown hook failed: %s",
                exc,
                exc_info=True,
            )

    @asynccontextmanager
    async def run(self) -> AsyncIterator[LifecycleRunner]:
        """An ``asynccontextmanager`` form of ``async with self: ...``."""
        async with self as runner:
            yield runner


def _as_component(obj: LifecycleComponent | AbstractLifecycle, index: int) -> LifecycleComponent:
    """
    Coerce a plain ``AbstractLifecycle``-shaped object (bare ``start()``/
    ``stop()``) into a ``LifecycleComponent`` — the hand-wired
    ``VarcoLifespan(bus, consumer)`` shape kept working unchanged.

    Args:
        obj: Either an already-built ``LifecycleComponent`` or any object
            with ``start()``/``stop()``.
        index: Position in the input sequence — used to synthesize a name
            for a bare ``AbstractLifecycle`` object.

    Returns:
        *obj* unchanged if it is already a ``LifecycleComponent``; otherwise
        a wrapping ``LifecycleComponent`` named after its type.
    """
    if isinstance(obj, LifecycleComponent):
        return obj
    name = f"{type(obj).__name__}#{index}"
    return LifecycleComponent(name=name, phase=Phase.CONSUMERS, start=obj.start, stop=obj.stop)


__all__ = [
    "AbstractLifecycle",
    "LifecycleComponent",
    "LifecycleFailure",
    "LifecycleRunner",
    "ModulePhaseError",
    "Phase",
    "phased_components",
    "resolve_lifecycle_order",
]
