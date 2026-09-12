"""
varco_sa.tenancy.rls_check
=============================
This module has two roles, both about Postgres RLS **posture** — never DDL:

1. ``assert_rls_enabled()`` — the RD-6 assertion: **never emits DDL**, only
   reads ``pg_class``/``pg_policies`` and reports (or raises naming) tables
   missing row-level security.
2. ``inspect_rls_posture()`` (Plan 037 / S12d, §D-S12-posture) — a report,
   never a raise: surfaces the ``BYPASSRLS``/superuser/owner footgun
   ``assert_rls_enabled()`` cannot see (it only checks
   ``pg_class.relrowsecurity`` — a table can pass that check and still be
   fully unprotected if the connecting role bypasses RLS unconditionally, or
   owns the table without ``FORCE``).

§D-S12-oq2 — **module placement, decided**: ``inspect_rls_posture()`` lives
beside ``assert_rls_enabled()`` rather than in its own module.
``rls_check.py`` was already one cohesive module for "read Postgres RLS
catalog state"; a second, related reader here reads no differently than the
first, and the module docstring above now names both roles explicitly so
the RD-6 "assert-only" framing is not misread as covering
``inspect_rls_posture()`` too (it performs no assertion — see its own
docstring).

DESIGN: assert-only, and the failure must teach (RD-6)
    "assert-only and maybe add a guide or an error that point to the
    documentation that explain how to tenable" — the resolved user answer.
    ``TenantIsolationError`` names the table, the concrete remediation
    (``varco_sa.migration.ops.rls_upgrade(op, "<table>")`` in a reviewed
    revision), and the doc path
    (``technical_docs/features/postgres-rls.md``).

``GLOBAL``-scoped tables and the ten framework tables are **skipped, not
flagged** — the RD-6 trap this module exists to close: without the skip, a
shared reference table (which legitimately carries no RLS policy — it has
no ``tenant_id`` to filter on) would be reported as "missing a policy" and
the assertion would be unusable in any deployment with global tables.

DESIGN: report-and-warn in 3.2; raising is a 4.0 decision (§D-S12-posture)
    ✅ The locked blast-radius rule (BACKLOG.md:45): the fix for a missing
       ``FORCE`` is an ``ALTER TABLE ... FORCE ROW LEVEL SECURITY`` in a
       reviewed revision — and applying it can take an app that never sets
       the GUC to zero rows. That is "real application work", so it is
       warn-only now; ``assert_rls_enabled()``'s raise condition is not
       touched by this plan.
    ✅ A frozen dataclass return (``RlsPosture``) keeps a caller's
       consumption a pure read — no shared mutable state, no import from
       ``varco_fastapi`` into ``varco_sa`` (Plan 036 owns the preflight that
       reports this; this module only produces the data).
    ❌ A deployment that only reads the raise, not a WARNING log line, stays
       unprotected. Mitigated by Plan 036's preflight and the docs' Pitfalls
       tables.
  Rejected — make ``assert_rls_enabled`` raise on a missing ``FORCE`` or a
  bypassing role: ❌ an upgrade-time behaviour change for existing
  ``enforce_rls=True`` deployments, and the "role bypasses" branch would
  fail every local/CI Postgres container, whose default role *is* a
  superuser.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

from varco_core.tenancy.catalog import TenantIsolationError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection

logger = logging.getLogger(__name__)

_REMEDIATION_DOC = "technical_docs/features/postgres-rls.md"

# Tables with a policy AND relrowsecurity=true — pg_policies already implies
# a policy exists; relrowsecurity confirms RLS is actually turned on for the
# table (a table can have a stale policy while RLS itself is disabled).
_RLS_ENABLED_QUERY = """
SELECT DISTINCT c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_policies p ON p.schemaname = n.nspname AND p.tablename = c.relname
WHERE c.relrowsecurity = true
  AND c.relname = ANY(:table_names)
"""


async def assert_rls_enabled(
    conn: AsyncConnection,
    *,
    tables: Iterable[str],
    global_tables: set[str],
    framework_tables: set[str],
    enforce: bool,
) -> list[str]:
    """
    Return (and, if ``enforce``, raise on) tables missing Postgres RLS.

    Args:
        conn:             An open async connection.
        tables:           Every routed (``TENANT``-scoped) table name to
                          check.
        global_tables:    Table names to **skip** — ``GLOBAL``-scoped
                          entities legitimately carry no ``tenant_id`` and
                          no RLS policy (the RD-6 trap).
        framework_tables: The ten framework tables — also skipped (they are
                          forced ``GLOBAL``, see Phase 4).
        enforce:          When ``True`` and any non-skipped table is
                          missing a policy, raises. When ``False``, this
                          function still queries and returns the missing
                          list — never emits DDL either way (RD-6:
                          assert-only).

    Returns:
        Sorted list of table names missing RLS. Always ``[]`` on a
        non-Postgres dialect (skipped with one WARNING — mirrors
        ``SAAuditRepository``'s dialect fallback), regardless of
        ``enforce``.

    Raises:
        TenantIsolationError: ``enforce=True`` and one or more non-skipped
            tables are missing a policy. The message names every missing
            table, the literal remediation
            (``varco_sa.migration.ops.rls_upgrade(op, "<table>")``), and
            the doc path — asserted on the text by
            ``test_rls_assertion.py``.

    Edge cases:
        - ``enforce=False`` never emits DDL — the only forbidden
          statement is a write/DDL one; reads are always allowed.
    """
    dialect_name = getattr(conn.dialect, "name", None)
    if dialect_name != "postgresql":
        logger.warning(
            "assert_rls_enabled(): dialect %r is not postgresql — RLS is a "
            "Postgres-only feature. Skipping the check entirely (no rows "
            "read, nothing raised).",
            dialect_name,
        )
        return []

    candidates = sorted(t for t in tables if t not in global_tables and t not in framework_tables)
    if not candidates:
        return []

    import sqlalchemy as sa

    result = await conn.execute(sa.text(_RLS_ENABLED_QUERY), {"table_names": candidates})
    enabled: set[str] = set(result.scalars().all())

    missing = sorted(t for t in candidates if t not in enabled)

    if missing and enforce:
        table_list = ", ".join(missing)
        remediations = "\n".join(
            f'  varco_sa.migration.ops.rls_upgrade(op, "{t}")' for t in missing
        )
        raise TenantIsolationError(
            f"Row-Level Security is not enabled for table(s): {table_list}. "
            f"Add a reviewed migration revision calling:\n{remediations}\n"
            f"See {_REMEDIATION_DOC} for the full guide. "
            "(GLOBAL-scoped and framework tables are never flagged here — "
            "only TenancySettings.enforce_rls=True routed tables are.)"
        )

    return missing


@dataclasses.dataclass(frozen=True)
class TablePosture:
    """
    Per-table RLS state, as reported by ``inspect_rls_posture()``.

    Attributes:
        rls_enabled: ``pg_class.relrowsecurity`` — RLS is turned on.
        rls_forced:  ``pg_class.relforcerowsecurity`` — RLS also applies to
                     the table owner. Missing ``FORCE`` is a common
                     production footgun (owner-run migrations bypass every
                     policy silently) — brief 007 §1.
        has_policy:  At least one row in ``pg_policies`` for this table.
                     ``rls_enabled=True`` with ``has_policy=False`` is the
                     default-deny state §D-S12-order exists to avoid ever
                     leaving a table in.
    """

    rls_enabled: bool
    rls_forced: bool
    has_policy: bool


@dataclasses.dataclass(frozen=True)
class RlsPosture:
    """
    The connecting role's RLS posture, plus per-table state
    (§D-S12-posture).

    Attributes:
        current_role:   The connecting role's name (``current_user``).
        is_superuser:   ``pg_roles.rolsuper`` for ``current_role`` — a
                        superuser bypasses RLS **unconditionally**, on every
                        table, regardless of policies or ``FORCE``.
        rolbypassrls:   ``pg_roles.rolbypassrls`` for ``current_role`` — same
                        unconditional bypass as a superuser, without being
                        one (brief 007 §1: "common when migrations run as
                        the app user" via a dedicated bypass role).
        owned_tables:   Names, among ``tables``, that ``current_role`` owns.
                        An owner bypasses RLS unless the table's ``FORCE``
                        bit is also set.
        tables:         ``{table_name: TablePosture}`` for every table in
                        the requested set — always present, even for a table
                        with no RLS/policy at all (``rls_enabled=False``,
                        ``rls_forced=False``, ``has_policy=False``).
    """

    current_role: str
    is_superuser: bool
    rolbypassrls: bool
    owned_tables: frozenset[str]
    tables: Mapping[str, TablePosture]


_POSTURE_ROLE_QUERY = """
SELECT current_user AS role_name, rolsuper, rolbypassrls
FROM pg_roles
WHERE rolname = current_user
"""

_POSTURE_OWNED_TABLES_QUERY = """
SELECT c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_roles r ON r.oid = c.relowner
WHERE r.rolname = current_user
  AND c.relname = ANY(:table_names)
"""

_POSTURE_TABLE_STATE_QUERY = """
SELECT
    c.relname,
    c.relrowsecurity,
    c.relforcerowsecurity,
    EXISTS (
        SELECT 1 FROM pg_policies p
        WHERE p.schemaname = n.nspname AND p.tablename = c.relname
    ) AS has_policy
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = ANY(:table_names)
"""


async def inspect_rls_posture(
    conn: AsyncConnection,
    *,
    tables: Iterable[str],
) -> RlsPosture:
    """
    Report the connecting role's RLS posture and per-table RLS state.

    **Never raises on an unprotected finding — a report, not an assertion.**
    ``assert_rls_enabled()`` remains the only function in this module that
    raises, and its raise condition is unchanged by this function's
    existence (§D-S12-posture).

    Args:
        conn:   An open async connection — the report reflects exactly the
                role that connection authenticated as.
        tables: Table names to report per-table state for.

    Returns:
        A frozen ``RlsPosture``. On a non-Postgres dialect, returns a
        degenerate posture (``current_role=""``, both bypass flags
        ``False``, no owned tables, every requested table reported as
        ``TablePosture(False, False, False)``) with one WARNING — the same
        skip-with-one-WARNING contract ``assert_rls_enabled()`` and
        ``varco_sa.migration.ops`` already use for a non-Postgres dialect.

    Edge cases:
        - ``tables=[]`` still queries the role posture (current_role,
          is_superuser, rolbypassrls) — only the per-table walk is skipped.
    """
    import sqlalchemy as sa

    table_names = list(tables)

    dialect_name = getattr(conn.dialect, "name", None)
    if dialect_name != "postgresql":
        logger.warning(
            "inspect_rls_posture(): dialect %r is not postgresql — RLS is a "
            "Postgres-only feature. Returning a degenerate (all-False) "
            "posture; nothing was read.",
            dialect_name,
        )
        return RlsPosture(
            current_role="",
            is_superuser=False,
            rolbypassrls=False,
            owned_tables=frozenset(),
            tables={name: TablePosture(False, False, False) for name in table_names},
        )

    role_result = await conn.execute(sa.text(_POSTURE_ROLE_QUERY))
    role_row = role_result.one()
    current_role = role_row.role_name
    is_superuser = bool(role_row.rolsuper)
    rolbypassrls = bool(role_row.rolbypassrls)

    if is_superuser:
        logger.warning(
            "inspect_rls_posture(): current_role %r is a Postgres SUPERUSER — "
            "it bypasses RLS unconditionally, on every table, regardless of "
            "policies or FORCE. Any RLS policy is a no-op for this "
            "connection. See technical_docs/features/postgres-rls.md.",
            current_role,
        )
    elif rolbypassrls:
        logger.warning(
            "inspect_rls_posture(): current_role %r has BYPASSRLS — it "
            "bypasses RLS unconditionally, the same as a superuser, on "
            "every table. See technical_docs/features/postgres-rls.md.",
            current_role,
        )

    owned_tables: frozenset[str] = frozenset()
    table_states: dict[str, TablePosture] = {
        name: TablePosture(False, False, False) for name in table_names
    }

    if table_names:
        owned_result = await conn.execute(
            sa.text(_POSTURE_OWNED_TABLES_QUERY), {"table_names": table_names}
        )
        owned_tables = frozenset(owned_result.scalars().all())
        if owned_tables:
            logger.warning(
                "inspect_rls_posture(): current_role %r OWNS table(s) %s — "
                "an owner bypasses RLS unless FORCE ROW LEVEL SECURITY is "
                "also set. See technical_docs/features/postgres-rls.md.",
                current_role,
                sorted(owned_tables),
            )

        state_result = await conn.execute(
            sa.text(_POSTURE_TABLE_STATE_QUERY), {"table_names": table_names}
        )
        for row in state_result:
            rls_enabled = bool(row.relrowsecurity)
            rls_forced = bool(row.relforcerowsecurity)
            has_policy = bool(row.has_policy)
            table_states[row.relname] = TablePosture(rls_enabled, rls_forced, has_policy)
            if rls_enabled and not rls_forced and row.relname in owned_tables:
                logger.warning(
                    "inspect_rls_posture(): table %r has RLS ENABLED but not "
                    "FORCED, and current_role %r owns it — every query from "
                    "this connection bypasses the policy. See "
                    "technical_docs/features/postgres-rls.md.",
                    row.relname,
                    current_role,
                )

    return RlsPosture(
        current_role=current_role,
        is_superuser=is_superuser,
        rolbypassrls=rolbypassrls,
        owned_tables=owned_tables,
        tables=table_states,
    )


__all__ = ["RlsPosture", "TablePosture", "assert_rls_enabled", "inspect_rls_posture"]
