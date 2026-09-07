"""
varco_sa.rls_autogen
======================
The generated-for-you RLS DDL path (Plan 037 / S12b, §D-S12-autogen).

``render_rls_ddl()`` (``varco_sa.rls``) is a per-table, per-call generator —
correct, but every table's ``cast_type``/nullability has to be worked out by
hand. This module is the "one call, all TENANT tables" layer on top of it:
``plan_tenant_rls()`` walks a set of domain classes, derives each one's
tenant column and Postgres cast type from its generated SQLAlchemy
``Table``, and returns a pure, inspectable, printable plan — no I/O, no DDL.
``render_tenant_rls_ddl()``/``tenant_rls_upgrade()``/``tenant_rls_downgrade()``
turn that plan into strings and (only at the caller's own Alembic revision)
apply them.

DESIGN: plan / render / apply split (§D-S12-autogen)
    ✅ ``plan_tenant_rls()`` is pure — an operator can
       ``print(plan_tenant_rls(...))`` in code review before any DDL exists,
       and every skipped table is visible in the output rather than silently
       omitted.
    ✅ Every string still comes from ``varco_sa.rls.render_rls_ddl()`` — the
       ``(SELECT NULLIF(current_setting(...), '')::cast)`` InitPlan form is
       never re-derived here, the same single-source rule
       ``varco_sa/varco_sa/migration/ops.py`` already states for
       ``rls_upgrade()``.
    ❌ Two ways to get RLS DDL now (this module's metadata-based path, and
       ``render_rls_ddl()``'s name-based escape hatch for tables varco did
       not generate). Accepted and documented — see the module they both
       live beside.

DESIGN: cast_type is DERIVED per column, never defaulted (§D-S12-autogen)
    ✅ Scout-verified and already bitten once: several framework tables are
       ``String(255)``, and a hardcoded ``::uuid`` "made
       ``framework_rls_upgrade()`` abort every migration that used it"
       (``varco_sa/varco_sa/rls.py``'s own DESIGN comment). A generator that
       repeats that mistake at ten tables' scale is worse.
    ✅ Brief 007 §2: "Always cast ``current_setting()`` to the target column
       type" — the cast is correctness, not style.
    ❌ An unmappable column type is a ``skipped_reason``, never a guess —
       visible in the plan, never a silent wrong policy.

DESIGN: a nullable tenant column is REFUSED, not silently hidden
    (§D-S12-nullable)
    ✅ ``tenant_id = (SELECT NULLIF(current_setting(...), '')::t)`` is
       ``NULL`` — never ``TRUE`` — for a row whose ``tenant_id`` IS ``NULL``.
       So enabling RLS on a table with a nullable tenant column makes every
       untenanted row invisible to every connection, permanently, with no
       error. ``plan_tenant_rls()`` raises ``ValueError`` naming the table,
       the column, and both remedies unless the caller opts explicitly into
       ``NullTenantPolicy.VISIBLE`` (fail-open: ``OR col IS NULL``) or
       ``NullTenantPolicy.HIDDEN`` (today's default ``render_rls_ddl()``
       behaviour, spelled out explicitly rather than silently inherited).
    ❌ Two extra enum values on every call site that has a nullable tenant
       column. Accepted — the alternative is a silent, permanent data loss
       bug.

Thread safety:  ✅ Every function here is a pure, synchronous computation —
                   no shared state, no I/O (``tenant_rls_upgrade``/
                   ``tenant_rls_downgrade`` call ``op.execute()``, which is
                   the caller-supplied Alembic ``Operations`` proxy — this
                   module never opens a connection itself).
Async safety:   ✅ Alembic's ``op`` proxy is synchronous by convention (same
                   as ``varco_sa.rls_framework``); nothing here is a
                   coroutine.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Callable, Sequence
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from varco_core.tenancy.settings import TenantScope

from varco_sa.rls import render_rls_ddl

if TYPE_CHECKING:
    import sqlalchemy as sa

# §D-S12-autogen — varco_tenants' tenant_id is its PRIMARY KEY, not a
# filterable tenant column. An RLS policy on it would show every connection
# exactly one row and break provisioning, fan-out, and assert_rls_enabled
# itself. Never a candidate, regardless of what plan_tenant_rls is asked for.
_HARD_EXCLUDED_TABLES: frozenset[str] = frozenset({"varco_tenants"})


class NullTenantPolicy(StrEnum):
    """
    What to do about a nullable tenant column (§D-S12-nullable).

    - ``REFUSE`` (the default everywhere a caller doesn't say otherwise):
      ``plan_tenant_rls()`` raises ``ValueError`` naming the table, the
      column, and both remedies below. Never silently hidden.
    - ``HIDDEN``: today's ``render_rls_ddl()`` behaviour, spelled out
      explicitly — a ``NULL`` tenant column never matches any tenant's GUC,
      so those rows become invisible to every connection. Fine for a column
      whose ``NULL`` rows are genuinely orphaned/never queried.
    - ``VISIBLE``: fail-**open** — adds ``OR {col} IS NULL`` to both
      ``USING`` and ``WITH CHECK``, so every tenant can see (and, via
      ``WITH CHECK``, write) untenanted rows. Must be spelled out explicitly
      at the call site; never a default.
    """

    REFUSE = "refuse"
    HIDDEN = "hidden"
    VISIBLE = "visible"


@dataclasses.dataclass(frozen=True)
class RlsTablePlan:
    """
    One table's RLS plan — inspectable and printable before any DDL exists.

    Attributes:
        table:          The table name RLS would be applied to.
        tenant_column:  Column on ``table`` holding the tenant identifier.
        cast_type:      Postgres cast derived from the column's SQLAlchemy
                        type (``"uuid"``/``"text"``/``"bigint"``), or an
                        arbitrary placeholder when ``skipped_reason`` is set
                        (never trusted for DDL in that case).
        nullable:       Whether the tenant column allows ``NULL``.
        skipped_reason: When set, no DDL is emitted for this table (a
                        ``TenantScope.GLOBAL`` model is never even
                        represented here — this field is for a ``TENANT``
                        table plan_tenant_rls could not safely act on: no
                        tenant column, an unregistered domain class, or an
                        unmappable column type).
    """

    table: str
    tenant_column: str
    cast_type: str
    nullable: bool
    skipped_reason: str | None = None


def _cast_type_for(column: sa.Column[Any]) -> str | None:
    """
    Derive the Postgres cast Postgres needs for ``current_setting()``
    (always ``text``) to compare against ``column``.

    Returns:
        ``"uuid"``/``"text"``/``"bigint"``, or ``None`` when the column's
        type is not one of the mapped families — the caller turns ``None``
        into a ``skipped_reason``, never a guessed cast (§D-S12-autogen).
    """
    import sqlalchemy as sa

    col_type = column.type
    # Uuid columns — both SQLAlchemy 2.x's generic sa.Uuid and Postgres'
    # own dialect-specific UUID type (a plain domain class registered via
    # SAModelFactory always produces sa.Uuid; a hand-built Table, as in the
    # unit tests and an app's own escape-hatch table, may use either).
    if isinstance(col_type, sa.Uuid):
        return "uuid"
    try:
        from sqlalchemy.dialects.postgresql import UUID as PGUUID  # noqa: N811, PLC0415

        if isinstance(col_type, PGUUID):
            return "uuid"
    except ImportError:  # pragma: no cover - postgres dialect always available in this repo
        pass
    if isinstance(col_type, (sa.String, sa.Text, sa.Unicode)):
        return "text"
    if isinstance(col_type, sa.Integer):  # sa.BigInteger is an Integer subclass
        return "bigint"
    return None


def _read_tenant_scope(domain_cls: type) -> TenantScope:
    """
    Read ``domain_cls.Meta.tenant_scope`` the same way
    ``varco_core.meta.MetaReader.read()`` does (its Step 8), without
    requiring the class to be a full dataclass-backed ``DomainModel`` — a
    stand-in class exposing only ``Meta.tenant_scope`` (this module's own
    tests, and any operator's ad-hoc review script) resolves the same way a
    real registered domain class does.
    """
    meta_cls = getattr(domain_cls, "Meta", None)
    raw = getattr(meta_cls, "tenant_scope", TenantScope.TENANT)
    return TenantScope(raw)


def _default_table_lookup(domain_cls: type) -> sa.Table:
    """
    Resolve ``domain_cls`` to its generated ``Table`` via
    ``SAModelRegistry`` — the provider's own record of which ORM class
    ``SAModelFactory`` built for each domain class.

    Raises:
        KeyError: ``domain_cls`` was never built by ``SAModelFactory`` —
            caught by ``plan_tenant_rls()`` and turned into a
            ``skipped_reason``, never propagated.
    """
    from varco_sa.factory import SAModelRegistry  # noqa: PLC0415

    orm_cls = SAModelRegistry.get(domain_cls)
    return orm_cls.__table__  # type: ignore[attr-defined, no-any-return]


def plan_tenant_rls(
    domain_classes: Sequence[type],
    *,
    base: type,
    tenant_column: str = "tenant_id",
    table_lookup: Callable[[type], sa.Table] | None = None,
    null_tenant: NullTenantPolicy = NullTenantPolicy.REFUSE,
) -> list[RlsTablePlan]:
    """
    Build an inspectable RLS plan for every ``TenantScope.TENANT`` class.

    Args:
        domain_classes: Domain classes to consider. A ``TenantScope.GLOBAL``
                        class is silently absent from the result (never a
                        skip entry — GLOBAL tables are not RLS candidates at
                        all, matching ``assert_rls_enabled()``'s own skip).
        base:           The shared ``DeclarativeBase`` the generated tables
                        live under. Unused when ``table_lookup`` is given
                        explicitly; kept as a required, named parameter so a
                        future default lookup can walk ``base.metadata``
                        directly without a signature break.
        tenant_column:  Column name to look for on each resolved table.
                        Default ``"tenant_id"``.
        table_lookup:   ``domain_cls -> Table`` resolver. Defaults to
                        ``SAModelRegistry.get(domain_cls).__table__`` — the
                        provider's own record of the generated table.
        null_tenant:    What to do about a nullable ``tenant_column``
                        (§D-S12-nullable). Default ``REFUSE`` — raises.

    Returns:
        One ``RlsTablePlan`` per ``TENANT``-scoped class in
        ``domain_classes``, in the same order, each independently either
        ready to render or ``skipped_reason``-ed. ``varco_tenants`` is
        always excluded entirely (never even a skip entry — see the module
        docstring).

    Raises:
        ValueError: ``null_tenant=REFUSE`` (the default) and a resolved
            table's tenant column is nullable — names the table, the
            column, and both remedies (``NullTenantPolicy.VISIBLE``/
            ``NullTenantPolicy.HIDDEN``).

    Edge cases:
        - A domain class never registered with ``SAModelFactory`` (the
          default lookup raises ``KeyError``) is present with a
          ``skipped_reason`` — never a ``KeyError`` propagated to the
          caller.
        - A ``TENANT``-scoped class whose table has no ``tenant_column`` is
          present with a ``skipped_reason`` and, downstream,
          ``render_tenant_rls_ddl()`` emits no DDL for it.
        - An unmappable column type is a ``skipped_reason``, never a
          guessed cast.
    """
    lookup = table_lookup or _default_table_lookup
    plans: list[RlsTablePlan] = []

    for domain_cls in domain_classes:
        if _read_tenant_scope(domain_cls) is not TenantScope.TENANT:
            continue  # GLOBAL: not a candidate at all, not even a skip entry.

        try:
            table = lookup(domain_cls)
        except KeyError:
            plans.append(
                RlsTablePlan(
                    table=getattr(domain_cls, "__name__", str(domain_cls)),
                    tenant_column=tenant_column,
                    cast_type="",
                    nullable=False,
                    skipped_reason=(
                        f"{domain_cls!r} has no generated table registered "
                        "(SAModelRegistry.get() raised KeyError) — call "
                        "provider.register(...) first, or pass table_lookup="
                        "explicitly."
                    ),
                )
            )
            continue

        if table.name in _HARD_EXCLUDED_TABLES:
            # Hard-excluded, never a candidate — not even represented as a
            # skip entry, matching the GLOBAL-scope behaviour above.
            continue

        if tenant_column not in table.columns:
            plans.append(
                RlsTablePlan(
                    table=table.name,
                    tenant_column=tenant_column,
                    cast_type="",
                    nullable=False,
                    skipped_reason=(
                        f"Table {table.name!r} has no column {tenant_column!r} "
                        "— cannot generate an RLS policy without a tenant "
                        "column to filter on."
                    ),
                )
            )
            continue

        column = table.columns[tenant_column]
        cast_type = _cast_type_for(column)
        if cast_type is None:
            plans.append(
                RlsTablePlan(
                    table=table.name,
                    tenant_column=tenant_column,
                    cast_type="",
                    nullable=bool(column.nullable),
                    skipped_reason=(
                        f"Column {table.name}.{tenant_column} has type "
                        f"{column.type!r}, which has no known Postgres RLS "
                        "cast (uuid/text/bigint) — never guessed. Pass a "
                        "table whose tenant column is one of those families."
                    ),
                )
            )
            continue

        if column.nullable and null_tenant is NullTenantPolicy.REFUSE:
            raise ValueError(
                f"Table {table.name!r}, column {tenant_column!r} is nullable. "
                "Enabling RLS on it makes every row whose tenant column is "
                "NULL invisible to every connection, permanently, with no "
                "error (NULLIF(..., '') never equals NULL). Choose one: "
                f"NullTenantPolicy.VISIBLE (fail-open — adds `OR {tenant_column} "
                "IS NULL` so every tenant can see and write untenanted rows) "
                "or NullTenantPolicy.HIDDEN (today's default render_rls_ddl() "
                "behaviour, spelled out explicitly — untenanted rows stay "
                "invisible to everyone)."
            )

        plans.append(
            RlsTablePlan(
                table=table.name,
                tenant_column=tenant_column,
                cast_type=cast_type,
                nullable=bool(column.nullable),
            )
        )

    return plans


# Matches a render_rls_ddl()-produced CREATE POLICY statement — its USING and
# WITH CHECK clauses are always the identical tenant_filter string (rls.py's
# own construction). Used only to append `OR col IS NULL` for
# NullTenantPolicy.VISIBLE — never to re-derive the InitPlan form itself.
_CREATE_POLICY_RE = re.compile(
    r"^(CREATE POLICY .+ USING )\((.*)\)( WITH CHECK )\((.*)\)$",
)


def render_tenant_rls_ddl(
    plans: Sequence[RlsTablePlan],
    *,
    setting: str = "rls.tenant_id",
    null_tenant: NullTenantPolicy = NullTenantPolicy.REFUSE,
) -> list[str]:
    """
    Turn a plan (from ``plan_tenant_rls()``) into ordered DDL statements.

    Every statement is produced by ``varco_sa.rls.render_rls_ddl()`` — the
    InitPlan form is never re-derived here (single-source rule, see the
    module docstring). Statements from every non-skipped plan are
    concatenated in order.

    Args:
        plans:       Output of ``plan_tenant_rls()``.
        setting:     The Postgres GUC name, forwarded to ``render_rls_ddl()``.
        null_tenant: ``NullTenantPolicy.VISIBLE`` appends
                     ``OR {tenant_column} IS NULL`` to the ``USING``/
                     ``WITH CHECK`` clauses of any plan whose ``nullable`` is
                     ``True``. Any other value (including the default
                     ``REFUSE``) leaves the clause unchanged — by the time a
                     plan reaches this function, ``plan_tenant_rls()`` has
                     already either raised (``REFUSE`` at plan time) or been
                     told ``HIDDEN``/``VISIBLE`` explicitly, so ``render_*``
                     never needs to re-decide REFUSE's raise — only whether
                     to render the ``OR IS NULL`` branch.

    Returns:
        A flat list of DDL strings, in ``CREATE POLICY``/``ENABLE``/
        ``FORCE`` order per plan (§D-S12-order), skipping any plan whose
        ``skipped_reason`` is set (no DDL is ever emitted for those).
    """
    statements: list[str] = []
    for plan in plans:
        if plan.skipped_reason is not None:
            continue

        ddl = render_rls_ddl(
            plan.table,
            tenant_column=plan.tenant_column,
            setting=setting,
            cast_type=plan.cast_type,
        )

        if plan.nullable and null_tenant is NullTenantPolicy.VISIBLE:
            ddl = [_add_null_tenant_clause(stmt, plan.tenant_column) for stmt in ddl]

        statements.extend(ddl)

    return statements


def _add_null_tenant_clause(stmt: str, tenant_column: str) -> str:
    """Append ``OR {tenant_column} IS NULL`` to a CREATE POLICY statement's
    USING/WITH CHECK clauses. Non-CREATE-POLICY statements pass through
    unchanged (ENABLE/FORCE carry no filter clause to extend)."""
    match = _CREATE_POLICY_RE.match(stmt)
    if match is None:
        return stmt
    using_prefix, filter_expr, with_check_prefix, filter_expr_repeat = match.groups()
    extended = f"{filter_expr} OR {tenant_column} IS NULL"
    extended_repeat = f"{filter_expr_repeat} OR {tenant_column} IS NULL"
    return f"{using_prefix}({extended}){with_check_prefix}({extended_repeat})"


def tenant_rls_upgrade(
    op: Any,
    *,
    plans: Sequence[RlsTablePlan],
    setting: str = "rls.tenant_id",
    null_tenant: NullTenantPolicy = NullTenantPolicy.REFUSE,
) -> None:
    """
    Apply ``render_tenant_rls_ddl(plans, ...)`` via ``op.execute()``.

    Args:
        op:    The Alembic ``op`` module (or any object exposing
               ``execute()``), from the application's own reviewed revision
               — this module never applies DDL on its own (§D-S12-oq4: no
               revision ships in ``varco_sa``).
        plans: Output of ``plan_tenant_rls()``, reviewed by the operator.
        setting/null_tenant: Forwarded to ``render_tenant_rls_ddl()`` —
               must match whatever was passed to ``plan_tenant_rls()`` for
               ``null_tenant`` to render the same ``OR IS NULL`` decision.

    Never raises directly — DDL errors surface from ``op.execute()`` at
    migration-apply time, the same trust model as
    ``varco_sa.rls_framework.framework_rls_upgrade``.
    """
    for stmt in render_tenant_rls_ddl(plans, setting=setting, null_tenant=null_tenant):
        op.execute(stmt)


def tenant_rls_downgrade(
    op: Any,
    *,
    plans: Sequence[RlsTablePlan],
) -> None:
    """
    Reverse ``tenant_rls_upgrade`` — drop each plan's policy, disable RLS.

    Skips any plan whose ``skipped_reason`` is set (no DDL was ever emitted
    for it, so there is nothing to reverse).
    """
    for plan in plans:
        if plan.skipped_reason is not None:
            continue
        name = f"{plan.table.replace('.', '_')}_tenant_isolation"
        op.execute(f"DROP POLICY IF EXISTS {name} ON {plan.table}")
        op.execute(f"ALTER TABLE {plan.table} DISABLE ROW LEVEL SECURITY")


__all__ = [
    "NullTenantPolicy",
    "RlsTablePlan",
    "plan_tenant_rls",
    "render_tenant_rls_ddl",
    "tenant_rls_downgrade",
    "tenant_rls_upgrade",
]
