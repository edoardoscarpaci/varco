"""
varco_core.migrator
=======================
Step-by-step data migrator for ``VersionedDomainModel`` subclasses.

Usage — simple (no parameters)
--------------------------------
Define ``steps`` as a class-level list.  ``Meta.migrator`` can be either
the class itself or an instance — the mapper normalises both::

    from varco_core.migrator import DomainMigrator

    def normalise_email(data: dict) -> dict:
        data["email"] = data["email"].lower()
        return data

    class UserMigrator(DomainMigrator):
        steps = [normalise_email]   # current_version = 2

    class Meta:
        migrator = UserMigrator     # class accepted — mapper calls UserMigrator()

Usage — parametrised (steps need runtime values)
-------------------------------------------------
Override ``__init__``, assign ``self.steps`` (shadows the ClassVar), and
use bound methods as steps so they can access ``self``::

    class UserMigrator(DomainMigrator):
        def __init__(self, default_country: str = "US"):
            self._default_country = default_country
            self.steps = [self._add_country]   # bound method → has self

        def _add_country(self, data: dict) -> dict:
            data.setdefault("country", self._default_country)
            return data

    class Meta:
        migrator = UserMigrator(default_country="IT")   # instance with params

Both forms call ``migrator.current_version()`` and ``migrator.migrate(data, v)``
as regular instance methods — the same interface regardless of how the
migrator was created.

Usage — rollback support via ``StepSpec``
-----------------------------------------
Wrap each step in a ``StepSpec`` to attach an optional reverse (down) function.
``DomainMigrator.rollback()`` chains the ``down`` callables in reverse order::

    from varco_core.migrator import DomainMigrator, StepSpec

    def add_slug(data: dict) -> dict:
        data["slug"] = data["name"].lower().replace(" ", "-")
        return data

    def remove_slug(data: dict) -> dict:
        data.pop("slug", None)
        return data

    class UserMigrator(DomainMigrator):
        steps = [StepSpec(up=add_slug, down=remove_slug, name="add_slug")]

    m = UserMigrator()
    data = m.migrate({"name": "Hello World"}, from_version=1)
    original = m.rollback(data, from_version=2)   # undoes add_slug

Usage — dry-run mode
---------------------
Call ``dry_run(from_version)`` to inspect the migration plan without executing
any steps.  Returns a ``MigrationPlan`` describing which steps would run::

    plan = UserMigrator().dry_run(from_version=1)
    print(plan.step_names)         # ["add_slug"]
    print(plan.can_rollback)       # True (all steps have down)

``MigrationError``
------------------
Raised when ``from_version`` is out of range (e.g. the DB somehow contains
a version higher than ``current_version`` — indicates a deployment mismatch),
or when ``rollback()`` is called on a step that has no ``down`` function.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, ClassVar


class MigrationError(Exception):
    """Raised when a migration step is missing, out of range, or has no rollback."""


# ── StepSpec ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class StepSpec:
    """
    Wrapper that associates a forward migration (``up``) with an optional
    rollback (``down``) function.

    ``StepSpec`` is callable — calling the instance invokes ``up(data)``.
    This means ``StepSpec`` instances are drop-in replacements for plain
    callable steps in ``DomainMigrator.steps`` without requiring any changes
    to the ``migrate()`` hot path.

    Args:
        up:   Forward migration function ``(dict) -> dict``.  Required.
        down: Reverse migration function ``(dict) -> dict``.  Optional.
              If ``None``, ``rollback()`` raises ``MigrationError`` when this
              step would need to be undone.
        name: Human-readable step name used in ``dry_run()`` output and
              error messages.  Defaults to ``up.__name__`` when not provided.

    DESIGN: callable dataclass over a base class
        ✅ Drop-in for plain callables in ``steps`` — no migration of existing
           migrators required.
        ✅ Frozen (immutable) — safe to share and cache.
        ✅ ``name`` field avoids relying on ``__name__`` for bound methods,
           which may have less descriptive names (e.g. ``_add_slug``).
        ❌ ``down`` must be provided at step-definition time; can't be added later
           without replacing the ``StepSpec`` instance.

    Thread safety:  ✅ Frozen — immutable once constructed.
    Async safety:   ✅ Synchronous; no I/O.
    """

    up: Callable[[dict[str, Any]], dict[str, Any]]
    """Forward migration function — runs during ``migrate()``."""

    down: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    """Reverse migration function — runs during ``rollback()``.  May be ``None``."""

    name: str | None = None
    """Human-readable name for the step.  Falls back to ``up.__name__`` if not set."""

    def __call__(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Invoke the forward migration (``up``) function.

        Allows ``StepSpec`` to be used directly as a callable wherever a plain
        migration function is expected — e.g. in the ``steps`` list and inside
        ``DomainMigrator.migrate()``.

        Args:
            data: The field dict to transform.

        Returns:
            Transformed field dict (from ``self.up``).
        """
        return self.up(data)

    @property
    def display_name(self) -> str:
        """Human-readable step name (``name`` field or ``up.__name__``)."""
        return self.name or getattr(self.up, "__name__", repr(self.up))

    def __repr__(self) -> str:
        has_down = self.down is not None
        return f"StepSpec(name={self.display_name!r}, " f"has_rollback={has_down})"


# ── MigrationPlan ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class MigrationPlan:
    """
    Immutable description of what ``DomainMigrator.migrate()`` would do.

    Returned by ``DomainMigrator.dry_run()``.  Use this to inspect or log
    the migration plan before executing it, or to generate human-readable
    documentation.

    Attributes:
        from_version: The starting schema version.
        to_version:   The target schema version (always ``current_version()``).
        steps:        Ordered list of ``(display_name, has_rollback)`` tuples
                      for each step that would run.

    Thread safety:  ✅ Frozen — immutable once constructed.
    """

    from_version: int
    """The data version before migration."""

    to_version: int
    """The data version after migration (equal to ``current_version()``)."""

    steps: list[tuple[str, bool]] = field(default_factory=list)
    """Ordered list of ``(step_name, has_rollback)`` tuples for pending steps."""

    @property
    def step_names(self) -> list[str]:
        """Names of the steps that would run, in order."""
        return [name for name, _ in self.steps]

    @property
    def can_rollback(self) -> bool:
        """
        ``True`` if every pending step has a rollback (``down``) function.

        When ``False``, calling ``rollback()`` for any version range that
        includes a step without a ``down`` will raise ``MigrationError``.
        """
        return all(has_rollback for _, has_rollback in self.steps)

    @property
    def is_noop(self) -> bool:
        """``True`` when no steps would run (``from_version == to_version``)."""
        return len(self.steps) == 0

    def __repr__(self) -> str:
        return (
            f"MigrationPlan("
            f"v{self.from_version}→v{self.to_version}, "
            f"steps={self.step_names}, "
            f"can_rollback={self.can_rollback})"
        )


# ── Internal helpers ──────────────────────────────────────────────────────────


def _step_display_name(step: Any) -> str:
    """
    Return a human-readable name for a migration step.

    - ``StepSpec`` → ``step.display_name``
    - Plain callable → ``fn.__name__``
    - Other → ``repr(step)``
    """
    if isinstance(step, StepSpec):
        return step.display_name
    return getattr(step, "__name__", repr(step))


class DomainMigrator:
    """
    Step-by-step data migrator for ``VersionedDomainModel`` subclasses.

    Class variable
    --------------
    steps
        List of callables ``(dict) -> dict``.  ``steps[0]`` migrates v1 → v2,
        ``steps[1]`` migrates v2 → v3, etc.  An empty list means the model is
        at version 1 with no migrations needed yet.

        For **parametrised** migrators, override ``__init__`` and assign
        ``self.steps = [self._some_bound_method]`` — the instance attribute
        shadows this ClassVar automatically via normal Python attribute lookup.

    Instance methods
    ----------------
    current_version()
        Returns ``len(self.steps) + 1``.  Works for both ClassVar steps and
        instance-level steps.

    migrate(data, from_version)
        Chains ``self.steps[from_version - 1:]``.  Updates
        ``definition_version`` in the returned dict automatically.

    Mapper normalisation
    --------------------
    ``Meta.migrator`` accepts both a class and an instance.  The mapper
    converts a bare class to an instance by calling ``migrator()`` so both
    forms are interchangeable::

        migrator = UserMigrator         # → mapper calls UserMigrator()
        migrator = UserMigrator("IT")   # → used as-is

    Thread safety:  ✅ Stateless when steps are pure functions.
                    ⚠️  Not safe if steps mutate shared state.
    Async safety:   ✅ Synchronous, allocation-only.
    """

    steps: ClassVar[list[Callable[[dict[str, Any]], dict[str, Any]]]] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Ensure every subclass owns an independent copy of ``steps``.

        Without this hook, all ``DomainMigrator`` subclasses that do NOT
        explicitly declare ``steps`` inherit — and share — the *same*
        empty list from the base class.  If any subclass then does
        ``self.steps.append(fn)`` instead of a clean reassignment
        (``self.steps = [fn]``), the mutation would bleed into every
        sibling migrator.

        The ``"steps" not in cls.__dict__`` guard checks the subclass's
        own namespace only (no MRO walk), so a subclass that already
        declared ``steps = [normalise_email]`` is left untouched.

        Edge cases:
            - Parametrised migrators that assign ``self.steps = [...]``
              in ``__init__`` shadow the ClassVar with an instance
              attribute — this hook does not interfere.
            - Subclasses of subclasses: each level gets its own copy
              because ``__init_subclass__`` fires once per new class.
        """
        super().__init_subclass__(**kwargs)
        # Shallow-copy the inherited list so future append/extend
        # on one migrator subclass cannot affect another.
        if "steps" not in cls.__dict__:
            cls.steps = list(cls.steps)

    def current_version(self) -> int:
        """Current schema version: ``len(self.steps) + 1``."""
        return len(self.steps) + 1

    def migrate(self, data: dict[str, Any], from_version: int) -> dict[str, Any]:
        """
        Run the migration chain from ``from_version`` to ``current_version``.

        Slices ``self.steps[from_version - 1:]`` so only the required steps
        run.  Updates ``definition_version`` in the returned dict.

        Both plain callables and ``StepSpec`` instances are accepted in
        ``self.steps`` — ``StepSpec`` delegates to its ``up`` callable, so
        both work identically in this method.

        Args:
            data:         Raw field dict representing the entity at
                          ``from_version``.  Each step receives and returns
                          the dict; steps may mutate it in-place.
            from_version: Version stored in the database row.

        Returns:
            Migrated dict with ``definition_version`` set to
            ``current_version()``.

        Raises:
            MigrationError: ``from_version`` is out of range — either less
                            than 1 or greater than ``current_version()``.

        Edge cases:
            - ``from_version == current_version()`` → no steps run, only
              ``definition_version`` is (re)set.  Safe no-op.
            - Extra keys added by a step that don't exist on the domain class
              are silently ignored by the mapper.
        """
        cv = self.current_version()
        if from_version < 1 or from_version > cv:
            raise MigrationError(
                f"{type(self).__name__}: cannot migrate from version "
                f"{from_version!r} (current_version={cv}). "
                "This usually means the DB contains a row written by a newer "
                "version of the application."
            )

        for step in self.steps[from_version - 1 :]:
            data = step(data)

        data["definition_version"] = cv
        return data

    def dry_run(self, from_version: int) -> MigrationPlan:
        """
        Return the migration plan without executing any steps.

        Produces a ``MigrationPlan`` describing which steps would run if
        ``migrate(data, from_version)`` were called.  No data is touched.

        Use this to:
        - Inspect pending steps before applying them (e.g. in CI).
        - Log the plan for audit purposes.
        - Verify that all steps have rollback functions before running.

        Args:
            from_version: The starting schema version to plan from.

        Returns:
            A ``MigrationPlan`` with ``from_version``, ``to_version``, and
            the ordered list of ``(step_name, has_rollback)`` tuples.

        Raises:
            MigrationError: ``from_version`` is out of range.

        Edge cases:
            - ``from_version == current_version()`` → ``plan.is_noop`` is
              ``True`` (empty ``steps`` list).

        Async safety:   ✅ Pure — no I/O, no side effects.
        Thread safety:  ✅ Returns a new immutable ``MigrationPlan`` each call.
        """
        cv = self.current_version()
        if from_version < 1 or from_version > cv:
            raise MigrationError(
                f"{type(self).__name__}: cannot plan migration from version "
                f"{from_version!r} (current_version={cv})."
            )

        pending_steps = self.steps[from_version - 1 :]
        step_info: list[tuple[str, bool]] = []
        for step in pending_steps:
            name = _step_display_name(step)
            has_rollback = isinstance(step, StepSpec) and step.down is not None
            step_info.append((name, has_rollback))

        return MigrationPlan(
            from_version=from_version,
            to_version=cv,
            steps=step_info,
        )

    def rollback(
        self,
        data: dict[str, Any],
        from_version: int,
        *,
        to_version: int = 1,
    ) -> dict[str, Any]:
        """
        Reverse the migration chain from ``from_version`` down to ``to_version``.

        Chains the ``down`` callables of ``StepSpec`` steps in reverse order.
        Steps that are plain callables (not ``StepSpec``) or that have
        ``down=None`` raise ``MigrationError`` when they would need to be undone.

        Args:
            data:         The field dict at ``from_version`` to roll back.
            from_version: The current data version (starting point).
            to_version:   The target version after rollback.  Default: ``1``
                          (full rollback to the original schema).

        Returns:
            Rolled-back dict with ``definition_version`` set to ``to_version``.

        Raises:
            MigrationError:
                - ``from_version`` or ``to_version`` is out of range.
                - ``to_version >= from_version`` (nothing to undo).
                - A step in the rollback range has no ``down`` function.

        Edge cases:
            - Rollback is applied to steps ``steps[to_version - 1 : from_version - 1]``
              in reverse order (i.e. the most-recently-applied step is undone first).
            - If any step in the range lacks a ``down`` function, the entire
              rollback aborts with ``MigrationError`` before mutating ``data``.

        DESIGN: validate all steps before running any
            ✅ Fails fast — avoids partial rollback that leaves data in an
               inconsistent intermediate state.
            ❌ Two passes over the step list; acceptable for typical schema sizes.

        Thread safety:  ✅ Step functions must be pure; no shared state.
        Async safety:   ✅ Synchronous.
        """
        cv = self.current_version()

        # ── Validate version range ────────────────────────────────────────────
        if from_version < 1 or from_version > cv:
            raise MigrationError(
                f"{type(self).__name__}: cannot rollback from version "
                f"{from_version!r} (current_version={cv})."
            )
        if to_version < 1 or to_version > cv:
            raise MigrationError(
                f"{type(self).__name__}: cannot rollback to version "
                f"{to_version!r} (current_version={cv})."
            )
        if to_version >= from_version:
            raise MigrationError(
                f"{type(self).__name__}: rollback to_version ({to_version}) "
                f"must be less than from_version ({from_version})."
            )

        # Identify the steps that were applied during the forward migration for
        # this range.  These are the steps we need to undo, in reverse order.
        steps_to_undo = self.steps[to_version - 1 : from_version - 1]

        # ── Pre-flight: verify all steps have down before mutating data ───────
        missing: list[str] = []
        for step in steps_to_undo:
            if not (isinstance(step, StepSpec) and step.down is not None):
                missing.append(_step_display_name(step))
        if missing:
            raise MigrationError(
                f"{type(self).__name__}: cannot rollback — the following steps "
                f"have no 'down' function: {missing}. "
                "Wrap each step in StepSpec(up=..., down=...) to enable rollback."
            )

        # ── Execute rollback ──────────────────────────────────────────────────
        for step in reversed(steps_to_undo):
            # Pre-flight guarantees step is a StepSpec with a non-None down
            data = step.down(data)  # type: ignore[union-attr]

        data["definition_version"] = to_version
        return data


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "DomainMigrator",
    "MigrationError",
    "MigrationPlan",
    "StepSpec",
]
