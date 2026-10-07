"""
varco_core.migration
=====================
Backend-agnostic migration contracts — ``AbstractMigrator``,
``SchemaMigrationPlan``, ``MigrationReport``, ``MigrationSettings``, the
migration exception hierarchy rooted at ``SchemaMigrationError``, and
``InMemoryMigrator`` (the standard unit-test double).

Nothing above the storage layer imports ``alembic`` or ``pymongo`` — those
live in ``varco_sa.migration`` and ``varco_beanie.migration`` respectively,
which both implement ``AbstractMigrator`` defined here.

**Renamed in 3.0.0 (Plan 022 / AB-2).** This package's ``MigrationError`` and
``MigrationPlan`` are now ``SchemaMigrationError`` and ``SchemaMigrationPlan``.
The old names still resolve here, to the *identical* objects, and emit a
``DeprecationWarning``; they are removed in 5.0.0 (floor: 2027-08-31 — the
12-month window put 4.0.0 out of reach). The rename exists because
the unrelated, older ``varco_core.migrator`` (domain data/field migration)
already owned both bare names at the ``varco_core`` top level, which forced
two deliberate re-export holes. Renaming the *newer, narrower* pair closes
them, so both concepts are now importable from ``varco_core`` directly and an
import site says which one it means.

See ``technical_docs/features/schema-migrations.md`` for the full guide.
"""

from __future__ import annotations

from varco_core.deprecation import deprecated_alias
from varco_core.migration.base import (
    AbstractMigrator,
    MigrationReport,
    Revision,
    SchemaMigrationPlan,
)
from varco_core.migration.errors import (
    IrreversibleMigrationError,
    MigrationBackendUnavailable,
    MigrationLockTimeout,
    PendingMigrationsError,
    SchemaMigrationError,
)
from varco_core.migration.inmemory import InMemoryMigrator
from varco_core.migration.settings import MigrationSettings

__all__ = [
    "AbstractMigrator",
    "InMemoryMigrator",
    "IrreversibleMigrationError",
    "MigrationBackendUnavailable",
    "MigrationLockTimeout",
    "MigrationReport",
    "MigrationSettings",
    "PendingMigrationsError",
    "Revision",
    "SchemaMigrationError",
    "SchemaMigrationPlan",
]

# AB-2's back-compat seam. Chained innermost-first so one module __getattr__
# serves both renamed names; anything else still raises AttributeError, which
# is what keeps `hasattr(varco_core.migration, ...)` honest.
__getattr__ = deprecated_alias(
    "MigrationPlan",
    SchemaMigrationPlan,
    since="3.0.0",
    removed_in="5.0.0",
    fallback=deprecated_alias(
        "MigrationError",
        SchemaMigrationError,
        since="3.0.0",
        removed_in="5.0.0",
    ),
)
