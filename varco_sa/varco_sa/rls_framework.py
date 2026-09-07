"""
varco_sa.rls_framework
=========================
``framework_rls_upgrade`` / ``framework_rls_downgrade`` — one-call RLS
helpers for the two framework tables (``varco_audit_log``,
``varco_dead_letters``) (Plan 009, Phase 6 / R4).

These wrap ``varco_sa.migration.ops.rls_upgrade`` (itself a thin Alembic
wrapper over ``varco_sa.rls.render_rls_ddl``) so the correct
``(SELECT current_setting(..., true))`` InitPlan form is always used — the
documented, non-negotiable performance regression this codebase guards
against everywhere RLS is touched.

**Nothing calls these automatically.** Paste them into a reviewed app
revision, per `technical_docs/features/postgres-rls.md`'s "RLS enabled by a
startup hook" pitfall — the same rule as every other RLS helper in this
codebase.

Usage (inside an Alembic revision)::

    from varco_sa.rls_framework import framework_rls_upgrade, framework_rls_downgrade

    def upgrade() -> None:
        framework_rls_upgrade(op)

    def downgrade() -> None:
        framework_rls_downgrade(op)

Thread safety:  N/A — one-shot DDL emission at migration time.
Async safety:   N/A — Alembic's ``op`` proxy is synchronous.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

# Plan 037 / Step 4 — §"What already exists" table: this table set was a
# hand-listed constant of exactly two names and had gone stale — at least
# varco_schedules, varco_webhook_subscriptions and the encryption-key-store
# table also carry a tenant_id column (schedule.py:67, webhook.py:61,
# encryption_store.py:100) and were silently missing. framework_rls_tables()
# below re-derives the set every call by walking framework_metadata() for
# tables carrying the tenant column, so a fourteenth framework table with a
# tenant_id column cannot be silently forgotten again — Step 5's completeness
# walk is the permanent regression test for that guarantee.
#
# varco_tenants is hard-excluded (never a candidate, §D-S12-autogen): its
# tenant_id is the table's PRIMARY KEY, not a filterable tenant column — an
# RLS policy there would show every connection exactly one row and break
# provisioning/fan-out and assert_rls_enabled() itself.
_HARD_EXCLUDED_TABLES: frozenset[str] = frozenset({"varco_tenants"})


def framework_rls_tables(*, tenant_column: str = "tenant_id") -> tuple[str, ...]:
    """
    Derive the framework tables carrying a tenant column, for RLS purposes.

    Walks ``varco_sa.metadata.framework_metadata()`` and selects every table
    that has a ``tenant_column`` column, excluding ``varco_tenants`` (its
    tenant id is a primary key, not a filterable column — see the module-level
    ``_HARD_EXCLUDED_TABLES`` comment).

    DESIGN: derive, never hand-list (Plan 037 / §D-S12-autogen)
        ✅ A hand-listed constant silently drifted for years — see the
           comment above this function. Re-deriving on every call means a
           newly registered framework table is picked up automatically.
        ✅ Cheap — ``framework_metadata()`` is already an in-memory
           ``MetaData`` walk; no I/O.
        ❌ A framework table that names its tenant column something other
           than ``tenant_id`` would be missed silently. Filed as an
           ASSUMPTION in the plan's Risks table; mitigated by
           ``test_framework_rls.py``'s completeness walk, which fails loudly
           the day a fourteenth table appears unaccounted for.

    Args:
        tenant_column: Column name identifying a tenant-carrying table.
                       Default: ``"tenant_id"``.

    Returns:
        A sorted tuple of table names (deterministic — Alembic revisions and
        tests should not depend on ``dict`` iteration order).
    """
    from varco_sa.metadata import framework_metadata

    metadata = framework_metadata()
    return tuple(
        sorted(
            table.name
            for table in metadata.tables.values()
            if tenant_column in table.columns and table.name not in _HARD_EXCLUDED_TABLES
        )
    )


#: Back-compat: computed from framework_rls_tables() so existing imports of
#: the constant keep working — Step 4's "keep the old constant as a module
#: attribute computed from it so no import breaks".
FRAMEWORK_RLS_TABLES: tuple[str, ...] = framework_rls_tables()


def framework_rls_upgrade(
    op: Any,
    *,
    tables: Sequence[str] = FRAMEWORK_RLS_TABLES,
    tenant_column: str = "tenant_id",
    cast_type: str = "text",
) -> None:
    """
    Enable RLS on each of ``tables`` (default: both framework tables).

    Args:
        op:            The Alembic ``op`` module (or any object exposing
                       ``execute()``, matching Alembic's ``Operations`` proxy).
        tables:        Table names to enable RLS on. Defaults to
                       ``FRAMEWORK_RLS_TABLES``.
        tenant_column: Column name carrying the tenant id. Defaults to
                       ``"tenant_id"`` (matches both framework tables' schema
                       from Phase 6).
        cast_type:     Postgres type the ``rls.tenant_id`` GUC is cast to.
                       Defaults to ``"text"`` — NOT ``render_rls_ddl``'s
                       ``"uuid"`` default — because both framework tables
                       declare ``tenant_id`` as ``String(255)``
                       (``DeadLetterEntry.tenant_id``/``AuditEntry.tenant_id``
                       are ``str | None``, never ``UUID``). A ``uuid`` cast
                       here aborts the revision with ``operator does not
                       exist: character varying = uuid``.

    Never raises directly — DDL errors surface from ``op.execute`` at
    migration-apply time.

    Edge cases:
        - Re-running against a table that already has the policy fails with
          Postgres' "policy already exists"; pair with
          ``framework_rls_downgrade`` for an idempotent revision.
    """
    # DESIGN: call render_rls_ddl() directly (not migration.ops.rls_upgrade)
    #   ✅ rls_upgrade()'s non-Postgres no-op guard needs op.get_bind() — a
    #      real Alembic Operations proxy, not the minimal execute()-only
    #      shape this module documents accepting. Calling render_rls_ddl()
    #      directly keeps framework_rls_upgrade usable with any op-like
    #      object that can execute() a string, matching the docstring's own
    #      "any object exposing execute()" contract.
    #   ❌ The non-Postgres skip-with-warning behaviour is NOT inherited here
    #      — callers targeting a non-Postgres dialect must guard themselves
    #      (this module is Postgres-only by construction: RLS doesn't exist
    #      anywhere else).
    from varco_sa.rls import render_rls_ddl

    for table in tables:
        for stmt in render_rls_ddl(table, tenant_column=tenant_column, cast_type=cast_type):
            op.execute(stmt)


def framework_rls_downgrade(op: Any, *, tables: Sequence[str] = FRAMEWORK_RLS_TABLES) -> None:
    """Reverse ``framework_rls_upgrade`` — drop policies, disable RLS."""
    for table in tables:
        name = f"{table}_tenant_isolation"
        op.execute(f"DROP POLICY IF EXISTS {name} ON {table}")
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")


__all__ = [
    "FRAMEWORK_RLS_TABLES",
    "framework_rls_downgrade",
    "framework_rls_tables",
    "framework_rls_upgrade",
]
