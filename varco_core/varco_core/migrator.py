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

``MigrationError``
------------------
Raised when ``from_version`` is out of range (e.g. the DB somehow contains
a version higher than ``current_version`` — indicates a deployment mismatch).
"""

from __future__ import annotations

from typing import Any, Callable, ClassVar


class MigrationError(Exception):
    """Raised when a migration step is missing or the version is out of range."""


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
