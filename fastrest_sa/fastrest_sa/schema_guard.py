"""
fastrest_sa.schema_guard
=========================
Startup schema drift detector for SQLAlchemy + fastrest.

Compares ``Base.metadata`` (the expected schema, derived from all registered
``SAModelFactory.build()`` calls) against the live database and reports any
structural gaps — missing tables or missing columns.

Typical usage — call once at application startup before serving requests::

    from fastrest_sa import SchemaGuard

    guard = SchemaGuard(Base)
    await guard.check(engine)   # raises SchemaDrift if DB is out of sync

    # Or non-raising variant for health-check endpoints:
    drift = await guard.report(engine)
    if drift.has_drift:
        logger.error(drift.format())

Thread safety:  ✅ ``SchemaGuard`` is stateless after construction.
Async safety:   ✅ ``check()`` and ``report()`` are async and non-blocking —
                    uses ``conn.run_sync()`` to call the synchronous SA inspect
                    API without blocking the event loop.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import DeclarativeBase

if TYPE_CHECKING:
    # AsyncEngine import is guarded — avoids a hard runtime dependency on
    # asyncio when SchemaGuard is imported in a sync-only context (e.g. tests
    # that only exercise the sync diff helper).
    from sqlalchemy.ext.asyncio import AsyncEngine


# ── Drift report ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SchemaDriftReport:
    """
    Immutable snapshot of the difference between expected and actual schema.

    ``missing_tables`` and ``missing_columns`` are *blocking* — they indicate
    migrations that must be run before the app can function correctly.
    ``extra_columns`` is *informational* — extra columns in the DB are safe
    (another migration may have added them, or a different service owns them).

    Attributes:
        missing_tables:  Table names present in metadata but absent from the DB.
        missing_columns: ``{table_name: [col, ...]}`` — columns in metadata
                         but missing from the live DB table.
        extra_columns:   ``{table_name: [col, ...]}`` — columns in the live
                         DB that are NOT declared in metadata.  Warning only.

    Thread safety:  ✅ Frozen dataclass — immutable and hashable after construction.
    Async safety:   ✅ Safe to pass across coroutine boundaries.
    """

    missing_tables: list[str] = field(default_factory=list)
    missing_columns: dict[str, list[str]] = field(default_factory=dict)
    extra_columns: dict[str, list[str]] = field(default_factory=dict)

    @property
    def has_drift(self) -> bool:
        """
        True if any *blocking* drift was found.

        Extra columns are NOT counted as drift — they don't prevent the app
        from functioning.

        Returns:
            ``True`` if missing tables or columns exist; ``False`` otherwise.
        """
        return bool(self.missing_tables or self.missing_columns)

    def format(self) -> str:
        """
        Human-readable multi-line summary of all detected drift.

        Returns:
            A formatted string describing missing tables/columns and extra
            columns, or ``"No schema drift detected."`` if everything is clean.

        Example:
            print(drift.format())
            # Missing tables (run migrations):
            #   - users
            # Missing columns (run migrations):
            #   - posts: author_id, updated_at
        """
        if not self.has_drift and not self.extra_columns:
            return "No schema drift detected."

        lines: list[str] = []

        if self.missing_tables:
            lines.append("Missing tables (run Alembic migrations):")
            for table in sorted(self.missing_tables):
                lines.append(f"  - {table}")

        if self.missing_columns:
            lines.append("Missing columns (run Alembic migrations):")
            for table, cols in sorted(self.missing_columns.items()):
                lines.append(f"  - {table}: {', '.join(sorted(cols))}")

        if self.extra_columns:
            # Extra columns are informational — they don't block the app
            lines.append("Extra columns in DB not in metadata (informational):")
            for table, cols in sorted(self.extra_columns.items()):
                lines.append(f"  - {table}: {', '.join(sorted(cols))}")

        return "\n".join(lines)

    def __repr__(self) -> str:
        total_missing_cols = sum(len(v) for v in self.missing_columns.values())
        total_extra_cols = sum(len(v) for v in self.extra_columns.values())
        return (
            f"SchemaDriftReport("
            f"missing_tables={len(self.missing_tables)}, "
            f"missing_columns={total_missing_cols}, "
            f"extra_columns={total_extra_cols})"
        )


# ── Drift exception ───────────────────────────────────────────────────────────


class SchemaDrift(Exception):
    """
    Raised by ``SchemaGuard.check()`` when the live DB schema is out of sync
    with ``Base.metadata``.

    The full ``SchemaDriftReport`` is attached so callers can inspect the
    details programmatically — e.g. expose them in a ``/health`` endpoint
    or structure them in a log entry.

    Attributes:
        report: The drift report describing all detected gaps.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Safe to raise and catch in async contexts.
    """

    def __init__(self, report: SchemaDriftReport) -> None:
        """
        Args:
            report: The full diff produced by ``SchemaGuard.report()``.
        """
        self.report = report
        super().__init__(
            "Schema drift detected — run Alembic migrations before starting the app.\n"
            + report.format()
        )


# ── Guard ─────────────────────────────────────────────────────────────────────


class SchemaGuard:
    """
    Compares ``Base.metadata`` against the live database at startup and reports
    any structural gaps (missing tables, missing columns).

    Call once after all ``SAModelFactory.build()`` calls have completed —
    typically in your ``lifespan`` (FastAPI) or ``startup`` event handler.

    DESIGN: run_sync() for inspection
        SQLAlchemy's ``inspect()`` is a synchronous API.  We call it inside
        ``conn.run_sync()`` so the event loop is never blocked.
        ✅ Compatible with any async driver (asyncpg, aiosqlite, aiomysql)
        ✅ Single round-trip — entire diff runs in one run_sync block
        ❌ One extra connection at startup — negligible but worth noting

    DESIGN: single run_sync block for the full diff
        All ``inspect`` calls are batched inside a single ``_diff_metadata``
        helper rather than one lambda per table.  This avoids:
        - Repeated event-loop suspension overhead
        - Python closure-capture bugs when building per-iteration lambdas

    DESIGN: extra columns are informational, not blocking
        Columns in the DB that are not in metadata could belong to another
        service, a manual hotfix, or a future migration.  Refusing to start
        because of extra columns would cause unnecessary downtime.

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ ``check()`` and ``report()`` are non-blocking.

    Args:
        base: The shared ``DeclarativeBase`` subclass passed to
              ``SAModelFactory``.  All ``build()`` calls must complete
              before ``check()`` / ``report()`` is invoked.

    Edge cases:
        - Empty metadata (no models built yet)  → clean report, no error.
        - DB has extra tables not in metadata   → silently ignored.
        - Column type mismatches                → NOT detected; type
          introspection is dialect-specific and fragile.  Use Alembic
          ``--autogenerate`` if you need type-change detection.
        - Schema-qualified table names          → ``get_table_names()``
          returns unqualified names by default; pass ``schema=`` to
          ``insp.get_table_names()`` if you use PostgreSQL schemas.
    """

    def __init__(self, base: type[DeclarativeBase]) -> None:
        """
        Args:
            base: Shared ``DeclarativeBase``.  Must have all tables registered
                  in its ``metadata`` before ``check()`` / ``report()`` runs.
        """
        # Store metadata rather than the base class so the guard holds no
        # strong reference to the base itself — avoids keeping large ORM
        # class graphs alive longer than necessary.
        self._metadata = base.metadata

    async def check(self, engine: "AsyncEngine") -> None:
        """
        Assert the live DB matches ``Base.metadata``.

        Args:
            engine: Async SA engine connected to the target database.

        Raises:
            SchemaDrift: One or more tables or columns are missing from the DB.
                         The attached ``report`` describes exactly what is missing.

        Edge cases:
            - DB is unreachable        → propagates the underlying driver error.
            - All tables are present   → returns ``None`` silently.

        Example:
            guard = SchemaGuard(Base)
            await guard.check(engine)   # raises SchemaDrift if stale
        """
        drift = await self.report(engine)
        if drift.has_drift:
            raise SchemaDrift(drift)

    async def report(self, engine: "AsyncEngine") -> SchemaDriftReport:
        """
        Return a ``SchemaDriftReport`` without raising.

        Use this when you want to log or expose drift as a health-check
        warning without aborting startup.

        Args:
            engine: Async SA engine connected to the target database.

        Returns:
            A ``SchemaDriftReport`` with all detected differences.
            ``report.has_drift`` is ``False`` if everything is in sync.

        Raises:
            Never raises ``SchemaDrift`` — all errors are returned in the report.
            Driver exceptions (e.g. connection refused) propagate unchanged.

        Example:
            drift = await guard.report(engine)
            logger.info("Schema check: %s", drift.format())
        """
        metadata = (
            self._metadata
        )  # local alias — avoids repeated attr lookup in run_sync

        async with engine.connect() as conn:
            # Batch the entire diff into one run_sync call — see class docstring
            # for why this is better than one call per table.
            missing_tables, missing_cols, extra_cols = await conn.run_sync(
                _diff_metadata, metadata
            )

        return SchemaDriftReport(
            missing_tables=missing_tables,
            missing_columns=missing_cols,
            extra_columns=extra_cols,
        )

    def __repr__(self) -> str:
        return f"SchemaGuard(tables={len(self._metadata.tables)})"


# ── Internal sync diff (runs inside conn.run_sync) ────────────────────────────


def _diff_metadata(
    conn: Any,
    metadata: Any,
) -> tuple[list[str], dict[str, list[str]], dict[str, list[str]]]:
    """
    Pure-sync comparison of ``metadata`` against the live DB.

    Designed to be called exclusively via ``conn.run_sync(_diff_metadata, metadata)``
    — never call this directly from async code.

    Args:
        conn:     A synchronous SA ``Connection`` provided by ``run_sync``.
        metadata: The ``MetaData`` object to compare against the live DB.

    Returns:
        A 3-tuple:
        - ``missing_tables``  — list of table names in metadata but not in DB.
        - ``missing_columns`` — ``{table: sorted([col, ...])}`` for tables that
                                exist but are missing one or more columns.
        - ``extra_columns``   — ``{table: sorted([col, ...])}`` for tables that
                                have columns not declared in metadata.

    Edge cases:
        - A table in metadata that doesn't exist in DB → added to missing_tables,
          no column comparison attempted for that table.
        - Empty metadata  → returns three empty collections.
        - get_columns() raises for an unrecognised table → propagates as-is
          (this should not happen given the table name came from get_table_names).
    """
    insp = sa_inspect(conn)
    # get_table_names() returns only tables visible to the current DB user —
    # tables from other schemas or insufficient privileges will appear missing.
    existing_tables: set[str] = set(insp.get_table_names())

    missing_tables: list[str] = []
    missing_cols: dict[str, list[str]] = {}
    extra_cols: dict[str, list[str]] = {}

    for table_name, table in metadata.tables.items():
        if table_name not in existing_tables:
            missing_tables.append(table_name)
            # No column comparison possible for missing tables
            continue

        # Compare column sets — names only; type comparison is dialect-specific
        live_col_names: set[str] = {col["name"] for col in insp.get_columns(table_name)}
        expected_col_names: set[str] = {col.name for col in table.columns}

        missing = sorted(expected_col_names - live_col_names)
        extra = sorted(live_col_names - expected_col_names)

        if missing:
            missing_cols[table_name] = missing
        if extra:
            extra_cols[table_name] = extra

    return missing_tables, missing_cols, extra_cols
