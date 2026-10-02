"""
varco_core.di.module
=======================
``VarcoModule`` — the one unit of wiring every varco package ships exactly
one of (Plan 049 / DI-1). Answers Open Question 1 (§D-shape in
``plans/049-varco-module-and-composable-app.md``): a plain varco ABC,
**never** a providify ``@Configuration`` subclass.

DESIGN — a varco ABC, never a providify ``@Configuration`` (§D-shape)
-----------------------------------------------------------------------
providify's ``@Configuration``/``depends_on=`` gives correct installation
ordering (``providify/modules.py:52-141``) but the scanner auto-installs
**every** public ``@Configuration`` it meets during ``container.scan(...)``
(``providify/scanner.py:142-162``). ``container.scan("varco_core",
recursive=True)`` is a documented, in-use pattern (it *is*
``varco_core.di.bootstrap()``) — a ``@Configuration``-shaped module would
therefore be silently activated by any package's own bootstrap, which is
exactly what the T3 decision (``BACKLOG.md:249-252``) forbids. A plain
class carries no providify marker at all, so ``container.scan()`` never
sees it (``scanner.py:151-162``) — this is enforced mechanically by
``test_modules_never_scanned.py`` (§D-scan).

✅ Scan-proof by construction — a plain class is invisible to the scanner.
✅ User-constructed with arguments (``SAModule(config=...)``,
   ``RetentionModule(registry)``) — providify's ``install()`` only ever
   constructs a ``@Configuration`` itself via DI (``container.py:7221``),
   which cannot accept a hand-built, already-configured object.
✅ ``start()``/``stop()`` are varco's own words — 050 sequences them without
   fighting providify's ``@PostConstruct``/``@PreDestroy`` vocabulary, which
   fires at the wrong lifecycle moments for this purpose
   (``container.py:5380-5402``).
❌ A second, ~40-line topological sort (``resolve_module_order`` below)
   mirroring ``providify.modules.resolve_install_order`` — cannot be reused
   because it raises ``TypeError`` for any non-``@Configuration`` node
   (``modules.py:120-125``). ``depends_on`` is named identically to
   providify's own attribute on purpose — the concept maps 1:1; this
   docstring is the one place that says which one a reader is looking at.

Slots, and when each runs
--------------------------
- ``register(container)`` — **``@final``**. A template method: fetches (or,
  on the first call for any module, creates) a ``ModuleRegistry`` singleton
  on ``container``; if ``type(self)`` is already recorded, returns
  immediately (idempotent). Otherwise calls ``self._register(container)``
  (the package's own ``bootstrap()`` + any hand-built bindings), then
  records ``(self, enabled=self.is_enabled(container))`` in the registry.
  Runs synchronously, at ``create_varco_app()``/composition-root time —
  even for a module whose ``is_enabled()`` will resolve ``False``: a
  disabled module must still be *visible* (052's ``varco diagnose``) and
  its ``@Fallback`` defaults must still be bound.
- ``_register(container)`` — abstract. The package's own ``bootstrap()``
  call plus any constructor-argument bindings (``bind_instance(container,
  SAConfig, config)`` when a config was passed).
- ``async_init(container)`` — awaited by the composition root's lifespan
  ``setup=`` hook, **before any module's** ``start()``. The seam for
  ``await container.ainstall(<a real @Configuration>)`` (e.g.
  ``RedisCacheConfiguration``) and any other awaitable setup. No-op by
  default.
- ``validate(container)`` — returns a sequence of human-readable problem
  strings. Defined here; **nothing calls it** until Plan 052 folds it into
  the lifespan. No-op (``()``) by default.
- ``start(container)`` / ``stop(container)`` — the module's own
  ``AbstractLifecycle``-shaped body. No-op by default. Only run for a
  module whose ``is_enabled(container)`` was ``True`` at registration time
  (§D-enabled) — a later env change does not retroactively flip it (same
  not-evicted caveat as Plan 045).
- ``is_enabled(container)`` — the Plan 047 tri-state, moved here verbatim:
  ``enabled=None`` (the constructor default) defers to
  ``self._enabled_from_settings(container)``; ``enabled=False``/``True``
  (code) always wins over whatever the settings hook would answer.

``depends_on`` here is varco's own — it mirrors
``providify.modules.resolve_install_order``'s DFS-post-order shape (deps
first, in declaration order, each module once) but is walked by
``resolve_module_order`` below, a small, independent implementation. It is
**not** providify's ``depends_on=`` and confers no providify behaviour.

Thread safety:  ⚠️ Every slot is intended for single-threaded startup only
                    (the same contract every ``bootstrap()`` already carries).
Async safety:   ✅ ``async_init``/``start``/``stop`` are ``async def``;
                    ``register``/``_register``/``is_enabled`` do no I/O.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from functools import partial
from typing import TYPE_CHECKING, Any, final

from varco_core.di.lifecycle import Phase

if TYPE_CHECKING:
    from collections.abc import Sequence

    from providify import DIContainer

    from varco_core.di.lifecycle import LifecycleComponent


class ModuleCycleError(RuntimeError):
    """
    Raised by ``resolve_module_order`` when a ``depends_on`` cycle is
    detected. ``.cycle`` names every ``VarcoModule`` subclass on the cycle,
    in the order they were revisited.

    A varco-owned exception (not providify's ``ModuleCycleError``) because
    this is raised from varco's own walk, over varco's own ``depends_on``.
    """

    def __init__(self, cycle: tuple[type[VarcoModule], ...]) -> None:
        self.cycle = cycle
        names = " -> ".join(c.__name__ for c in cycle)
        super().__init__(f"VarcoModule depends_on cycle detected: {names}")


class ModuleDependencyError(LookupError):
    """
    Raised by ``resolve_module_order`` when a module's ``depends_on`` names
    a class with no corresponding instance in the module list passed to
    ``create_varco_app``/``resolve_module_order``.

    Explicit over implicit (brief 015 §Librarian #1): the message names the
    missing class and suggests ``default_modules()`` as the usual fix.
    """


class ModuleRegistry:
    """
    The composition root's own record of every ``VarcoModule`` that has
    called ``register()`` on this container — what 052's ``varco diagnose``
    reads (name · enabled · class), and the mechanism ``register()``'s
    idempotency is built on.

    DESIGN — a bound singleton, not a ``WeakKeyDictionary`` keyed by
    container (§D-registry)
        ✅ No process-global state; ``bind_instance`` is the sanctioned
           "already-constructed object" verb (Plan 046), so the registry is
           then visible to ``container.describe()`` like any other binding.
        ❌ One non-fallback binding of a varco-owned type in every app
           container — accepted, it is the composition root's own record.

    Thread safety:  ⚠️ Mutated only during single-threaded startup
                        (``register()`` calls), same as every ``bootstrap()``.
    Async safety:   ✅ No I/O; plain in-memory bookkeeping.
    """

    def __init__(self) -> None:
        self._modules: list[VarcoModule] = []
        self._enabled: dict[type[VarcoModule], bool] = {}
        self._seen: set[type[VarcoModule]] = set()

    @property
    def modules(self) -> tuple[VarcoModule, ...]:
        """An ordered, frozen view of every module recorded so far."""
        return tuple(self._modules)

    def enabled(self, module: VarcoModule) -> bool:
        """
        Whether ``module`` was enabled at the point it was recorded.

        Args:
            module: A previously-recorded ``VarcoModule`` instance.

        Returns:
            The ``bool`` recorded for ``type(module)`` at ``record()`` time.

        Raises:
            KeyError: ``module``'s class was never recorded.
        """
        return self._enabled[type(module)]

    def record(self, module: VarcoModule, *, enabled: bool) -> None:
        """
        Record ``module`` as registered, or no-op if its class already was.

        Args:
            module: The module instance to record.
            enabled: The tri-state resolution to remember for this class.

        Raises:
            ValueError: A **different** instance of the same class is
                already recorded — two distinct objects of one
                ``VarcoModule`` subclass in one app is a copy-paste error,
                never a legitimate configuration (§D-signature's duplicate
                rule, enforced once, here).

        Edge cases:
            Calling this twice with the *same* instance is a silent no-op
            — this is what makes ``VarcoModule.register()`` idempotent.
        """
        cls = type(module)
        if cls in self._seen:
            existing = next(m for m in self._modules if type(m) is cls)
            if existing is not module:
                raise ValueError(
                    f"A different instance of {cls.__name__} is already registered "
                    f"on this container — two instances of one module class is "
                    f"almost always a copy-paste error."
                )
            return
        self._seen.add(cls)
        self._modules.append(module)
        self._enabled[cls] = enabled


def _get_or_create_registry(container: DIContainer) -> ModuleRegistry:
    """
    Return the container's ``ModuleRegistry``, creating and binding one via
    ``bind_instance`` on first use.

    Args:
        container: The container ``register()`` is being called on.

    Returns:
        The (possibly newly created) ``ModuleRegistry`` singleton.
    """
    if container.is_resolvable(ModuleRegistry):
        registry: ModuleRegistry = container.get(ModuleRegistry)
        assert isinstance(registry, ModuleRegistry)  # noqa: S101 — internal invariant
        return registry

    from varco_core.di import bind_instance  # noqa: PLC0415 — import-budget discipline

    registry = ModuleRegistry()
    bind_instance(container, ModuleRegistry, registry)
    return registry


class VarcoModule(ABC):
    """
    The one unit of wiring every varco package ships exactly one of
    (Plan 049 / DI-1). See the module docstring for the full shape,
    rationale, and slot-by-slot timing.

    Args:
        enabled: The Plan 047 tri-state. ``None`` (default) defers to
            ``self._enabled_from_settings(container)``; ``True``/``False``
            (a code decision) always wins over the settings hook.

    Example::

        class MyAppModule(VarcoModule):
            name = "my_app"
            depends_on = (CoreModule,)

            def _register(self, container: DIContainer) -> None:
                container.scan("myapp", recursive=True)

        app = create_varco_app(container, modules=[*default_modules(), MyAppModule()])
    """

    # DESIGN: plain class attributes, NOT ClassVar (§D-adapters)
    #   ✅ `LifecycleModule` legitimately shadows both with an *instance*
    #      attribute of the same name (a per-instance `name=`/`depends_on=`
    #      constructor kwarg) — `ClassVar` forbids exactly that assignment
    #      (mypy's own `[misc]` "Cannot assign to class variable via
    #      instance"), so the annotation must not claim it.
    #   ❌ A subclass *could* (accidentally) shadow `name`/`depends_on` via
    #      `self.name = ...` without meaning to — accepted; the ordering/
    #      identity contract only cares what `getattr(instance, name)`
    #      returns, never which level defined it.

    #: Unique, greppable identifier — e.g. ``"core"``, ``"sa"``,
    #: ``"security_posture"``. Subclasses MUST override this.
    name: str = ""

    #: Other ``VarcoModule`` subclasses this one must be started after (and
    #: stopped before). Walked by ``resolve_module_order`` — varco's own
    #: attribute, NOT providify's ``depends_on=`` (see module docstring).
    #: May be shadowed by an instance attribute of the same name (the
    #: ``LifecycleModule``/``MiddlewareModule`` adapter shape).
    depends_on: tuple[type[VarcoModule], ...] = ()

    #: The coarse, anonymous start-order bucket (Plan 050 / §D-phase) — see
    #: ``varco_core.di.lifecycle.Phase``. Defaults to ``CONSUMERS``, the
    #: right default for a typical app module (a consumer, a relay); a
    #: module that opens a store/pool other modules depend on must declare
    #: ``INFRA`` explicitly. Plain class attribute, not ``ClassVar`` — same
    #: reason as ``depends_on`` above (``LifecycleModule`` shadows it with an
    #: instance attribute).
    phase: int = Phase.CONSUMERS

    def __init__(self, *, enabled: bool | None = None) -> None:
        self._enabled_kwarg = enabled

    def __init_subclass__(cls, **kwargs: object) -> None:
        """
        Enforce ``register()``'s ``@final``-ness at the one place Python
        can actually stop it: class-definition time.

        ``typing.final`` is a static-only marker (mypy enforces it, nothing
        at runtime does) — a subclass that defines its own ``register()``
        would silently skip the ``ModuleRegistry`` idempotency template.
        Raising here, from ``__init_subclass__``, makes the violation a
        ``TypeError`` at the class statement itself, not a stray bug
        discovered later at registration time.

        Raises:
            TypeError: ``cls`` defines its own ``register`` in its class
                body (i.e. ``"register" in cls.__dict__``).
        """
        super().__init_subclass__(**kwargs)
        if "register" in cls.__dict__:
            raise TypeError(
                f"{cls.__name__} must not override register() — it is @final. "
                f"Override _register() instead; register() is the ModuleRegistry "
                f"idempotency template every VarcoModule shares."
            )

    @final
    def register(self, container: DIContainer) -> None:
        """
        Idempotently register this module on *container* (§D-registry).

        A template method: fetches/creates the container's
        ``ModuleRegistry``; if this module's class was already recorded,
        returns immediately (no-op — a second ``register()`` call, e.g.
        from an app that both lists a module explicitly and gets it via
        ``default_modules()``, is otherwise harmless). Otherwise calls
        ``self._register(container)`` and records
        ``(self, enabled=self.is_enabled(container))``.

        Args:
            container: The ``DIContainer`` to register into.

        Raises:
            ValueError: A **different** instance of this module's class was
                already recorded on *container* (``ModuleRegistry.record``).
            Exception: Whatever ``self._register(container)`` raises,
                propagated unchanged — nothing is recorded for a module
                whose ``_register`` failed (a retry with a fresh container
                is clean).

        Edge cases:
            Runs even for a module whose ``is_enabled()`` resolves
            ``False`` — a disabled module must still bind its settings/
            ``@Fallback`` defaults and be visible to 052's ``varco
            diagnose`` as "disabled", not silently absent.

        Async safety: Synchronous — no I/O; the package ``bootstrap()``
            calls inside ``_register`` are themselves synchronous
            (Plan 046).
        """
        registry = _get_or_create_registry(container)
        if type(self) in registry._seen:  # noqa: SLF001 — same module, own registry
            return
        self._register(container)
        registry.record(self, enabled=self.is_enabled(container))

    @abstractmethod
    def _register(self, container: DIContainer) -> None:
        """
        The package's own ``bootstrap(container)`` call, plus any
        constructor-argument bindings (e.g. ``bind_instance(container,
        SAConfig, config)`` when a config instance was passed to this
        module).

        Args:
            container: The ``DIContainer`` to register into.

        Raises:
            Exception: Whatever the underlying ``bootstrap()``/
                ``bind_instance`` call raises — propagates to
                ``register()``'s caller unchanged.
        """

    async def async_init(self, container: DIContainer) -> None:
        """
        Awaitable setup, run once per module before any module's
        ``start()`` — the seam for ``await container.ainstall(<a real
        @Configuration>)`` (e.g. ``RedisCacheConfiguration``).

        Args:
            container: The ``DIContainer`` this module was registered on.

        Edge cases:
            No-op by default. Only awaited for a module recorded
            ``enabled=True`` (§D-enabled).

        Async safety: May perform I/O (e.g. opening a connection pool).
        """

    def validate(self, container: DIContainer) -> Sequence[str]:
        """
        Return a sequence of human-readable problem descriptions for this
        module's current configuration.

        Args:
            container: The ``DIContainer`` this module was registered on.

        Returns:
            An empty sequence by default. Plan 052 defines the caller that
            folds this into the lifespan — 049 defines the slot and calls
            it from nothing.
        """
        return ()

    async def start(self, container: DIContainer) -> None:
        """
        Start this module's runtime behaviour (e.g. an event bus, a
        background refresher).

        Args:
            container: The ``DIContainer`` this module was registered on.

        Edge cases:
            No-op by default. Only called for a module recorded
            ``enabled=True`` (§D-enabled) — a later env change does not
            retroactively flip a module already recorded disabled.

        Async safety: May perform I/O.
        """

    async def stop(self, container: DIContainer) -> None:
        """
        Stop whatever ``start()`` started. Must be idempotent and must not
        raise (mirrors ``VarcoLifespan._stop_all``'s log-and-swallow
        contract at the orchestrator level — an individual module's
        ``stop()`` is still allowed to raise; the orchestrator, not this
        method, is what tolerates it).

        Args:
            container: The ``DIContainer`` this module was registered on.

        Edge cases:
            No-op by default.

        Async safety: May perform I/O.
        """

    def components(self, container: DIContainer) -> Sequence[LifecycleComponent]:
        """
        The phased lifecycle units this module contributes (Plan 050 /
        §D-components). Default: one component named/phased like the
        module itself, whose ``start``/``stop`` delegate to
        ``self.start(container)``/``self.stop(container)``.

        Override when one module's lifecycle spans several phases —
        ``CoreModule`` is the in-tree example (stores/INFRA, bus/BUS,
        consumers/CONSUMERS, job_runner/SCHEDULERS).

        Args:
            container: The ``DIContainer`` this module was registered on.

        Returns:
            A sequence of ``LifecycleComponent``. May be empty for a
            module with no lifecycle (e.g. a middleware-only module) — it
            is still visible to 052's ``varco diagnose`` via
            ``ModuleRegistry``, just contributes nothing to the runner.
        """
        from varco_core.di.lifecycle import LifecycleComponent  # noqa: PLC0415

        return [
            LifecycleComponent(
                name=self.name,
                phase=self.phase,
                start=partial(self.start, container),
                stop=partial(self.stop, container),
            )
        ]

    def is_enabled(self, container: DIContainer) -> bool:
        """
        Resolve the Plan 047 tri-state for this module.

        Args:
            container: The ``DIContainer`` this module was registered on
                (or is about to be — called from within ``register()``
                after ``_register`` has run, so any settings binding
                ``_register`` performed is already visible).

        Returns:
            ``self._enabled_kwarg`` when it is not ``None`` (a code
            decision always wins); otherwise
            ``self._enabled_from_settings(container)``.
        """
        if self._enabled_kwarg is not None:
            return self._enabled_kwarg
        return self._enabled_from_settings(container)

    def _enabled_from_settings(self, container: DIContainer) -> bool:
        """
        Hook: resolve "enabled" from this module's own settings/env when
        the constructor received ``enabled=None``.

        Args:
            container: The ``DIContainer`` this module was registered on.

        Returns:
            ``True`` by default — a module with no settings-driven
            enable/disable (most backend modules) is simply on whenever it
            is listed. Override for a module backed by a settings class
            carrying its own ``.enabled``/mode field (e.g.
            ``SecurityPostureModule``, ``MigrationsModule``).
        """
        return True


def resolve_module_order(modules: Sequence[VarcoModule]) -> list[VarcoModule]:
    """
    Order *modules* by ``depends_on`` — a DFS post-order walk, visiting
    roots in list order, each dependency's own dependencies resolved first,
    in declaration order, each module appearing exactly once (§D-order).

    The same shape as ``providify.modules.resolve_install_order`` so a
    reader who knows one knows both — but a small, independent
    implementation (providify's raises ``TypeError`` for any non-
    ``@Configuration`` node, so it cannot be reused here).

    Args:
        modules: The modules to order. Unconstrained modules (empty
            ``depends_on``) keep their relative list order (a stable
            walk).

    Returns:
        A new list, one entry per input module, dependencies before
        dependents.

    Raises:
        ValueError: Two distinct instances of the same ``VarcoModule``
            subclass appear in *modules*.
        ModuleDependencyError: A module's ``depends_on`` names a class with
            no corresponding instance in *modules*.
        ModuleCycleError: A ``depends_on`` cycle is detected.

    Edge cases:
        An instance-level ``depends_on`` attribute (set in ``__init__``,
        e.g. by ``LifecycleModule``) shadows the class attribute and is
        honoured — this function reads ``getattr(m, "depends_on")``, never
        ``type(m).depends_on`` directly.
    """
    by_class: dict[type[VarcoModule], VarcoModule] = {}
    for m in modules:
        cls = type(m)
        if cls in by_class:
            raise ValueError(
                f"Duplicate VarcoModule instance for class {cls.__name__!r} — "
                f"two instances of one module class in one list is almost "
                f"always a copy-paste error. Qualify/replace one, or use "
                f"default_modules(exclude=(...,)) to drop the default and add "
                f"your own."
            )
        by_class[cls] = m

    result: list[VarcoModule] = []
    visited: set[type[VarcoModule]] = set()
    visiting: list[type[VarcoModule]] = []

    def visit(m: VarcoModule) -> None:
        cls = type(m)
        if cls in visited:
            return
        if cls in visiting:
            cycle_start = visiting.index(cls)
            raise ModuleCycleError(tuple((*visiting[cycle_start:], cls)))
        visiting.append(cls)
        for dep_cls in getattr(m, "depends_on", ()):
            dep = by_class.get(dep_cls)
            if dep is None:
                visiting.pop()
                raise ModuleDependencyError(
                    f"{cls.__name__} depends on {dep_cls.__name__}, but no instance "
                    f"of {dep_cls.__name__} was found in the module list. Add one, "
                    f"or start from default_modules() and add your own modules to it."
                )
            visit(dep)
        visiting.pop()
        visited.add(cls)
        result.append(m)

    for m in modules:
        visit(m)

    return result


class CoreModule(VarcoModule):
    """
    ``varco_core``'s one ``VarcoModule`` — ``bootstrap(container)`` +
    ``bind_core_settings(container)``, plus starting/stopping
    ``AbstractEventBus``/``AbstractJobRunner`` if the app resolves one
    (§D-core).

    This absorbs what ``_collect_lifecycle_components``/
    ``_try_resolve_component`` used to do for these two core seams — minus
    the discovery logging (``VARCO_LIFECYCLE_DISCOVERY_WARN``): with
    explicit modules there is no "did you forget bootstrap()?" to warn
    about, since the module *is* the bootstrap.

    Every other varco module ``depends_on = (CoreModule,)`` — its own
    ``bootstrap()`` typically scans ``varco_core`` subpackages and needs
    ``varco_core``'s own defaults registered first (Plan 045 / E2).
    """

    name = "core"
    #: For the record — ``components()`` below is what the runner actually
    #: sequences (its four components span INFRA/BUS/CONSUMERS/SCHEDULERS
    #: individually); this class-level value only matters for a caller that
    #: reads ``CoreModule.phase`` directly (e.g. ``depends_on`` ordering
    #: against a module that has no lifecycle of its own).
    phase = Phase.INFRA

    def __init__(self, *, enabled: bool | None = None, job_runner: bool = True) -> None:
        """
        Args:
            enabled: The usual Plan 047 tri-state.
            job_runner: Whether ``components()`` plans the ``SCHEDULERS``-phase
                ``core.job_runner`` component (Plan 052 / §D-testprofile —
                ``varco_core.testing.in_memory_modules()`` passes ``False`` so
                an in-memory test container plans no background poller). The
                store itself stays bound either way — only the poller is
                omitted.
        """
        super().__init__(enabled=enabled)
        self._consumers: tuple[Any, ...] = ()
        self._job_runner = job_runner

    def _register(self, container: DIContainer) -> None:
        from varco_core.di import bootstrap  # noqa: PLC0415
        from varco_core.settings.core import bind_core_settings  # noqa: PLC0415

        bootstrap(container)
        bind_core_settings(container)
        # §D-RO2: guarded by is_enabled() (evaluated now — the same
        # registration-time value `register()` will shortly record) — a
        # disabled CoreModule contributes nothing, mirroring "a disabled
        # module registers nothing" elsewhere in this class.
        if self.is_enabled(container):
            self._register_ro2_collectors(container)

    def _register_ro2_collectors(self, container: DIContainer) -> None:
        """
        Contribute RO-2's two posture collectors (Plan 052 / §D-RO2).

        DESIGN: contributed by ``CoreModule``, not by ``JwksRefreshModule``/
        ``RetentionModule`` (051 Step 20's handoff, corrected) — the
        decisive ``auth.jwks_refresh_off`` finding fires precisely when
        ``JwksRefreshModule`` is *absent*; a collector contributed by that
        module would never run for the app it exists to warn. Suppliers
        are lazy (``lambda: ...``) so a registry built after this call —
        the common case — is still seen correctly at ``collect()`` time.
        """
        from varco_core.authority.posture import JwksPostureCollector  # noqa: PLC0415
        from varco_core.authority.registry import TrustedIssuerRegistry  # noqa: PLC0415
        from varco_core.di.collect import contribute  # noqa: PLC0415
        from varco_core.posture import PostureCollector  # noqa: PLC0415
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.posture import RetentionPostureCollector  # noqa: PLC0415

        def _jwks_registry() -> Any:
            return (
                container.get(TrustedIssuerRegistry)
                if container.is_resolvable(TrustedIssuerRegistry)
                else None
            )

        def _retention_registry() -> Any:
            return (
                container.get(RetentionRegistry)
                if container.is_resolvable(RetentionRegistry)
                else None
            )

        contribute(container, PostureCollector, JwksPostureCollector(_jwks_registry))  # type: ignore[type-abstract]
        contribute(container, PostureCollector, RetentionPostureCollector(_retention_registry))  # type: ignore[type-abstract]

    def validate(self, container: DIContainer) -> Sequence[str]:
        """
        Flag any bound ``EventConsumer`` that is not ``Scope.SINGLETON``
        (§D-consumer) — a non-singleton consumer would leak a fresh,
        never-``stop()``-able subscription set every time it is resolved.
        """
        from providify import Scope  # noqa: PLC0415

        from varco_core.event.consumer import EventConsumer  # noqa: PLC0415

        problems: list[str] = []
        for binding in container.get_all_bindings(EventConsumer):
            interface = getattr(binding, "interface", None)
            scope = getattr(binding, "scope", None)
            if scope is not None and scope is not Scope.SINGLETON:
                name = getattr(interface, "__name__", repr(interface))
                problems.append(
                    f"{name} is an EventConsumer bound at scope {scope!r}, not singleton — "
                    f"make it @Singleton or set auto_register = False."
                )
        return problems

    async def start(self, container: DIContainer) -> None:
        """
        Kept for a hand-wired caller that invokes ``CoreModule`` directly
        (never through ``components()``/``LifecycleRunner``) — runs every
        phase's body in the pre-050 single-call order: stores, bus +
        consumers, job runner.

        Prefer ``components()`` (used by ``create_varco_app``/
        ``phased_components``), which sequences these four bodies at their
        own ``INFRA``/``BUS``/``CONSUMERS``/``SCHEDULERS`` phases instead.
        """
        await self._start_stores(container)
        await self._start_bus(container)
        await self._start_consumers(container)
        if self._job_runner:
            await self._start_job_runner(container)

    async def stop(self, container: DIContainer) -> None:
        """The exact reverse of ``start()`` — see its docstring."""
        if self._job_runner:
            await self._stop_job_runner(container)
        await self._stop_consumers(container)
        await self._stop_bus(container)
        await self._stop_stores(container)

    def components(self, container: DIContainer) -> Sequence[LifecycleComponent]:
        """
        Four phased components (§D-components): ``core.stores`` (``INFRA``),
        ``core.bus`` (``BUS``), ``core.consumers`` (``CONSUMERS``),
        ``core.job_runner`` (``SCHEDULERS``) — replacing the single-call
        ``start()``/``stop()`` body ``LifecycleRunner`` actually sequences.
        """
        from varco_core.di.lifecycle import LifecycleComponent  # noqa: PLC0415

        components = [
            LifecycleComponent(
                name="core.stores",
                phase=Phase.INFRA,
                start=partial(self._start_stores, container),
                stop=partial(self._stop_stores, container),
            ),
            LifecycleComponent(
                name="core.bus",
                phase=Phase.BUS,
                start=partial(self._start_bus, container),
                stop=partial(self._stop_bus, container),
            ),
            LifecycleComponent(
                name="core.consumers",
                phase=Phase.CONSUMERS,
                start=partial(self._start_consumers, container),
                stop=partial(self._stop_consumers, container),
            ),
        ]
        if self._job_runner:
            components.append(
                LifecycleComponent(
                    name="core.job_runner",
                    phase=Phase.SCHEDULERS,
                    start=partial(self._start_job_runner, container),
                    stop=partial(self._stop_job_runner, container),
                )
            )
        return components

    async def _start_stores(self, container: DIContainer) -> None:
        """
        Start every resolvable core store/cache seam that exposes
        ``start()`` — the generalised "seam's owner starts what resolves"
        rule 049 §D-core stated for the bus, at ``INFRA`` (§D-components).

        Duck-typed: most backends (e.g. ``InMemoryIdempotencyStore``) have
        no ``start()`` at all and are silently skipped.
        """
        from varco_core.cache.base import CacheBackend  # noqa: PLC0415
        from varco_core.event.dlq import AbstractDeadLetterQueue  # noqa: PLC0415
        from varco_core.idempotency.base import AbstractIdempotencyStore  # noqa: PLC0415
        from varco_core.job.base import AbstractJobStore  # noqa: PLC0415
        from varco_core.revocation.base import AbstractTokenRevocationStore  # noqa: PLC0415

        for seam in (
            AbstractIdempotencyStore,
            AbstractDeadLetterQueue,
            AbstractJobStore,
            AbstractTokenRevocationStore,
            CacheBackend,
        ):
            if not container.is_resolvable(seam):
                continue
            obj: Any = await container.aget(seam)
            start = getattr(obj, "start", None)
            if callable(start):
                await start()

    async def _stop_stores(self, container: DIContainer) -> None:
        """The exact reverse of ``_start_stores`` — see its docstring."""
        from varco_core.cache.base import CacheBackend  # noqa: PLC0415
        from varco_core.event.dlq import AbstractDeadLetterQueue  # noqa: PLC0415
        from varco_core.idempotency.base import AbstractIdempotencyStore  # noqa: PLC0415
        from varco_core.job.base import AbstractJobStore  # noqa: PLC0415
        from varco_core.revocation.base import AbstractTokenRevocationStore  # noqa: PLC0415

        for seam in (
            CacheBackend,
            AbstractTokenRevocationStore,
            AbstractJobStore,
            AbstractDeadLetterQueue,
            AbstractIdempotencyStore,
        ):
            if not container.is_resolvable(seam):
                continue
            obj: Any = await container.aget(seam)
            stop = getattr(obj, "stop", None)
            if callable(stop):
                await stop()

    async def _start_bus(self, container: DIContainer) -> None:
        """
        Start the resolved ``AbstractEventBus``, if any (§D-bus — the
        ``BUS`` phase is now the one starter for every bus, not just
        Redis's).

        Edge cases:
            ``AbstractEventBus`` itself declares no ``start``/``stop`` —
            only some backends (Kafka, Redis, the two ``varco_ws`` push
            adapters) add connection-management methods;
            ``InMemoryEventBus`` (the ``@Fallback`` default every bare
            container resolves to) has neither. Duck-typed via
            ``getattr(..., "start", None)`` so a default app's bus is a
            silent no-op instead of an ``AttributeError``.
        """
        from varco_core.event.base import AbstractEventBus  # noqa: PLC0415

        if container.is_resolvable(AbstractEventBus):
            bus: AbstractEventBus = await container.aget(AbstractEventBus)
            start = getattr(bus, "start", None)
            if callable(start):
                await start()

    async def _stop_bus(self, container: DIContainer) -> None:
        """The exact reverse of ``_start_bus`` — see its docstring."""
        from varco_core.event.base import AbstractEventBus  # noqa: PLC0415

        if container.is_resolvable(AbstractEventBus):
            bus: AbstractEventBus = await container.aget(AbstractEventBus)
            stop = getattr(bus, "stop", None)
            if callable(stop):
                await stop()

    async def _start_consumers(self, container: DIContainer) -> None:
        """
        Auto-register every ``@Singleton``/``@Multibound`` ``EventConsumer``
        contribution to the (already-started, by phase ordering) bus
        (§D-consumer, 051's auto-``register_to`` loop, moved verbatim).
        """
        from varco_core.di.collect import acollect  # noqa: PLC0415
        from varco_core.event.base import AbstractEventBus  # noqa: PLC0415
        from varco_core.event.consumer import EventConsumer  # noqa: PLC0415

        if not container.is_resolvable(AbstractEventBus):
            return
        bus: AbstractEventBus = await container.aget(AbstractEventBus)
        consumers = await acollect(container, EventConsumer)
        self._consumers = consumers
        for consumer in consumers:
            if consumer.auto_register and not consumer.is_registered_to(bus):
                consumer.register_to(bus)

    async def _stop_consumers(self, container: DIContainer) -> None:
        """The exact reverse of ``_start_consumers`` — see its docstring."""
        for consumer in reversed(self._consumers):
            await consumer.stop()
        self._consumers = ()

    async def _start_job_runner(self, container: DIContainer) -> None:
        """Start the resolved ``AbstractJobRunner``, if any."""
        from varco_core.job.base import AbstractJobRunner  # noqa: PLC0415

        if container.is_resolvable(AbstractJobRunner):
            runner: AbstractJobRunner = await container.aget(AbstractJobRunner)
            await runner.start()

    async def _stop_job_runner(self, container: DIContainer) -> None:
        """The exact reverse of ``_start_job_runner`` — see its docstring."""
        from varco_core.job.base import AbstractJobRunner  # noqa: PLC0415

        if container.is_resolvable(AbstractJobRunner):
            runner: AbstractJobRunner = await container.aget(AbstractJobRunner)
            await runner.stop()


class ObservabilityModule(VarcoModule):
    """
    Plan 004's ``global_attributes=``/``capture_params=`` passthrough,
    moved from ``create_varco_app`` keywords to a module (§D-core-list).

    Args:
        global_attributes: ``None`` (default, no-op) or a mapping merged
            into the process-wide global attribute registry via
            ``set_global_attributes()``.
        capture_params: ``None`` (default, no-op) or an explicit
            ``True``/``False`` forwarded to
            ``varco_core.observability.params.set_capture_enabled()``.
    """

    name = "observability"
    phase = Phase.INFRA

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        global_attributes: dict[str, str] | None = None,
        capture_params: bool | None = None,
    ) -> None:
        super().__init__(enabled=enabled)
        self._global_attributes = global_attributes
        self._capture_params = capture_params

    def _register(self, container: DIContainer) -> None:
        if self._global_attributes is not None:
            from varco_core.observability import set_global_attributes  # noqa: PLC0415

            set_global_attributes(self._global_attributes)
        if self._capture_params is not None:
            from varco_core.observability import set_capture_enabled  # noqa: PLC0415

            set_capture_enabled(self._capture_params)


class LifecycleModule(VarcoModule):
    """
    Wraps any plain ``start()``/``stop()`` object (the pre-049
    ``extra_lifespan_components=`` shape) as a ``VarcoModule`` — the
    core-side half of §D-adapters (no HTTP shape, so it lives in
    ``varco_core.di.module`` rather than ``varco_fastapi.modules``).

    Args:
        component: Any object with async ``start()``/``stop()`` methods.
        name: Defaults to ``type(component).__name__``.
        depends_on: Per-instance dependency override — sets an *instance*
            attribute shadowing the (empty) class-level ``depends_on``;
            ``resolve_module_order`` reads ``getattr(m, "depends_on")``, so
            this is honoured exactly like a class-level declaration.
        enabled: The usual Plan 047 tri-state.

    Edge cases:
        Two ``LifecycleModule`` instances wrapping two *different*
        components still collide at registration — ``ModuleRegistry``
        keys on class, not on the wrapped component or ``name=``. Wrap
        both in one composite component, or subclass ``VarcoModule``
        directly for a second lifecycle object.
    """

    def __init__(
        self,
        component: object,
        *,
        name: str | None = None,
        depends_on: tuple[type[VarcoModule], ...] = (),
        enabled: bool | None = None,
        phase: int = Phase.CONSUMERS,
    ) -> None:
        super().__init__(enabled=enabled)
        self._component = component
        self.name = name or type(component).__name__
        self.depends_on = depends_on
        self.phase = phase

    def _register(self, container: DIContainer) -> None:
        pass

    async def start(self, container: DIContainer) -> None:
        await self._component.start()  # type: ignore[attr-defined]

    async def stop(self, container: DIContainer) -> None:
        await self._component.stop()  # type: ignore[attr-defined]


__all__ = [
    "CoreModule",
    "LifecycleModule",
    "ModuleCycleError",
    "ModuleDependencyError",
    "ModuleRegistry",
    "ObservabilityModule",
    "VarcoModule",
    "resolve_module_order",
]
