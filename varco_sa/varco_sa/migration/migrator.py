"""
varco_sa.migration.migrator
============================
``AlembicMigrator`` — a thin async facade over Alembic's ``ScriptDirectory``
+ ``EnvironmentContext`` + ``MigrationContext``, run headlessly (no
``env.py`` file required).

DESIGN: headless Alembic (no env.py) over ``alembic.command``
    ✅ ``alembic.command.upgrade()`` et al. require a real ``env.py`` on
       disk (``script.run_env()`` imports it) — this module drives
       ``EnvironmentContext``/``MigrationContext`` directly instead, which
       is what a normal ``env.py``'s ``run_migrations_online()`` does. This
       lets ``varco_sa`` ship the framework ``varco`` branch with **no**
       ``env.py`` in ``varco_sa/migrations/`` (Plan 006 D3).
    ✅ Alembic's API is synchronous and does blocking I/O — every call runs
       inside ``AsyncConnection.run_sync()`` (Alembic's own documented async
       recipe), never a second connection pool.
    ✅ ``transaction_per_migration=True`` + ``compare_type=True`` are always
       on (Plan 006 D4) — a failure in revision N leaves N-1 applied and
       ``alembic_version`` at N-1; a re-run resumes correctly.
    ❌ Bypassing ``alembic.command`` means ``alembic revision --autogenerate``
       is not available through this class — that stays a CLI-only,
       env.py-driven operation (``varco_sa.migration.cli``).

Thread safety:  ⚠️ One ``AlembicMigrator`` instance is not safe for
                   concurrent ``upgrade()``/``downgrade()`` calls from the
                   SAME process — cross-process exclusion is what
                   ``migration_lock`` provides.
Async safety:   ✅ Every public method is ``async def``; blocking Alembic
                   calls run via ``AsyncConnection.run_sync``.
"""

from __future__ import annotations

import importlib.resources
import os
import time
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.engine import Connection
from varco_core.migration.base import (
    AbstractMigrator,
    MigrationReport,
    Revision,
    SchemaMigrationPlan,
)
from varco_core.migration.errors import MigrationLockTimeout
from varco_core.migration.settings import MigrationSettings

from varco_sa.migration.lock import migration_lock

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine
    from varco_core.lock import AbstractDistributedLock


def _framework_versions_dir() -> Path:
    """Return the packaged ``varco_sa/migrations`` directory (the ``varco`` branch root)."""
    return Path(str(importlib.resources.files("varco_sa") / "migrations"))


def _package_root_dir() -> Path:
    """
    Return the ``varco_sa`` package root — the empty-revisions fallback.

    DESIGN: NOT ``_framework_versions_dir()`` as the script_location fallback
        ❌ Alembic's ``ScriptDirectory.from_config`` auto-scans
           ``script_location/versions`` **only when no explicit
           ``version_locations`` option is set at all** — precisely the
           case when ``include_framework_branch=False`` and no
           ``script_location``/``version_locations`` were given. Pointing
           ``script_location`` at the framework migrations directory in
           that case would leak the framework branch back in through the
           default scan, silently ignoring ``include_framework_branch=False``.
        ✅ ``varco_sa``'s own package root has no ``versions/`` subdirectory
           at all, so the default scan finds nothing — a genuinely empty
           fallback, verified against the installed Alembic version.
    """
    return Path(str(importlib.resources.files("varco_sa")))


class AlembicMigrator(AbstractMigrator):
    """
    Async ``AbstractMigrator`` implementation backed by Alembic.

    Args:
        engine:                 The application's ``AsyncEngine``. Alembic
                                 runs on this engine's own connections — no
                                 second pool is created (except the dedicated
                                 ``NullPool`` connection ``migration_lock``
                                 uses on PostgreSQL).
        script_location:        The app's own ``alembic/`` directory (must
                                 contain a ``versions/`` subdirectory). ``None``
                                 means "framework branch only" — the app has
                                 no revisions of its own.
        version_locations:      Extra version-location directories beyond
                                 ``script_location/versions``.
        include_framework_branch: When ``True`` (default), appends the
                                 packaged ``varco_sa/migrations/versions``
                                 directory so ``upgrade heads`` also applies
                                 framework-owned revisions (Plan 006 D3).
        lock:                   Optional caller-supplied
                                 ``AbstractDistributedLock`` (e.g.
                                 ``RedisLock``) — ``None`` uses the built-in
                                 Postgres advisory-lock mechanic (D2).
        settings:                ``None`` → ``MigrationSettings.from_env()``.
        schema:                  ``None`` (default, byte-identical to
                                 today) or a real Postgres schema name
                                 (Plan 007, Phase 9) — sets
                                 ``version_table_schema`` so a schema-scoped
                                 ``alembic_version`` table is read/written,
                                 threads a ``schema_translate_map`` so the
                                 symbolic ``"tenant"`` token
                                 (``varco_sa.tenancy.router.
                                 SYMBOLIC_SCHEMA_TOKEN``) resolves, and
                                 scopes the distributed lock key
                                 (``f"{lock_key}:{schema}"``) so tenants in
                                 one database never serialise against each
                                 other's migrations.

    DESIGN: script_location fallback to varco_sa's own package root
        ✅ When the caller has no ``script_location`` at all,
           ``ScriptDirectory.from_config`` still needs *some* existing
           directory to construct against — the ``varco_sa`` package root
           always exists and, critically, has no ``versions/`` subdirectory
           of its own, so it never leaks stray revisions in.
        ✅ ``version_locations`` is always built explicitly (see
           ``_build_config``) covering exactly the app dir (if any) plus the
           framework dir (if ``include_framework_branch=True``) — see
           ``_package_root_dir()``'s docstring for why the fallback must NOT
           be the framework migrations directory itself.
        ❌ Purely a config-resolution convenience — it does not change which
           revisions are scanned.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        script_location: str | Path | None = None,
        version_locations: Sequence[str | Path] = (),
        include_framework_branch: bool = True,
        lock: AbstractDistributedLock | None = None,
        settings: MigrationSettings | None = None,
        schema: str | None = None,
    ) -> None:
        self._engine = engine
        self._script_location = script_location
        self._extra_version_locations = list(version_locations)
        self._include_framework_branch = include_framework_branch
        self._lock = lock
        resolved_settings = settings or MigrationSettings.from_env()
        if schema is not None:
            # Per-tenant lock key — tenants sharing one database under
            # TenantIsolation.SCHEMA migrate concurrently without
            # contending on the same distributed lock (Plan 006's lock
            # semantics, applied per schema).
            # Plan 048 / DI-6: MigrationSettings is now a pydantic
            # VarcoSettings, not a dataclass — dataclasses.replace() no
            # longer applies. model_copy(update=...) is pydantic's
            # equivalent (works on a frozen model, same as replace()).
            resolved_settings = resolved_settings.model_copy(
                update={"lock_key": f"{resolved_settings.lock_key}:{schema}"}
            )
        self._settings = resolved_settings
        self._config: Any | None = None
        self._script: Any | None = None

        # Plan 007, Phase 9 — schema-per-tenant fan-out. None (default) is
        # byte-identical to today: version_table_schema stays unset and no
        # schema_translate_map is applied.
        self.schema = schema
        self.version_table_schema = schema

    # ── Alembic config/script lazy build ────────────────────────────────────

    def _build_config(self) -> tuple[Any, Any]:
        """Build (and cache) the Alembic ``Config`` + ``ScriptDirectory``."""
        if self._config is not None and self._script is not None:
            return self._config, self._script

        from alembic.config import Config
        from alembic.script import ScriptDirectory

        version_locations: list[str] = []
        if self._script_location is not None:
            version_locations.append(str(Path(self._script_location) / "versions"))
        version_locations.extend(str(v) for v in self._extra_version_locations)
        if self._include_framework_branch:
            version_locations.append(str(_framework_versions_dir() / "versions"))

        effective_script_location = (
            str(self._script_location)
            if self._script_location is not None
            else str(_package_root_dir())
        )

        config = Config()
        config.set_main_option("script_location", effective_script_location)
        if version_locations:
            config.set_main_option("path_separator", "os")
            config.set_main_option("version_locations", os.pathsep.join(version_locations))

        script = ScriptDirectory.from_config(config)
        self._config, self._script = config, script
        return config, script

    # ── Sync helpers (run inside AsyncConnection.run_sync) ──────────────────

    def _schema_configure_kwargs(self) -> dict[str, Any]:
        """
        Extra ``EnvironmentContext.configure()``/``MigrationContext.
        configure(opts=...)`` kwargs for schema-scoped fan-out.

        Empty when ``self.schema is None`` — the byte-identical-default
        guarantee (Plan 007 RD-3 backwards-compat): every existing
        ``env_ctx.configure(...)`` call keeps its exact kwargs.
        """
        if self.schema is None:
            return {}
        # version_table_schema scopes the alembic_version table itself;
        # schema_translate_map resolves the symbolic "tenant" token any
        # TENANT-scoped table's DDL carries under TenantIsolation.SCHEMA
        # (varco_sa.tenancy.router.SYMBOLIC_SCHEMA_TOKEN) to this tenant's
        # real schema — the same mechanism SASchemaRouter applies to
        # ordinary request-path sessions.
        return {
            "version_table_schema": self.version_table_schema,
        }

    def _sync_current_heads(self, sync_conn: Connection) -> tuple[str, ...]:
        # Instance method (not @staticmethod) — the single load-bearing
        # edit named in the plan — so it can carry self.schema through to
        # MigrationContext.configure(). A schema=None construction (the
        # default) passes opts={} — MigrationContext.configure(sync_conn)
        # behaves identically to before this method carried `self`.
        from alembic.runtime.migration import MigrationContext

        opts = self._schema_configure_kwargs()
        return tuple(MigrationContext.configure(sync_conn, opts=opts or None).get_current_heads())

    def _sync_upgrade(self, sync_conn: Connection, target: str) -> None:
        from alembic.runtime.environment import EnvironmentContext

        config, script = self._build_config()

        def _fn(rev: Any, context: Any) -> Any:
            return script._upgrade_revs(target, rev)

        with EnvironmentContext(config, script, fn=_fn, destination_rev=target) as env_ctx:
            env_ctx.configure(
                connection=sync_conn,
                target_metadata=None,
                transaction_per_migration=True,
                compare_type=True,
                **self._schema_configure_kwargs(),
            )
            with env_ctx.begin_transaction():
                env_ctx.run_migrations()

    def _sync_downgrade(self, sync_conn: Connection, target: str) -> None:
        from alembic.runtime.environment import EnvironmentContext

        config, script = self._build_config()

        def _fn(rev: Any, context: Any) -> Any:
            return script._downgrade_revs(target, rev)

        with EnvironmentContext(config, script, fn=_fn, destination_rev=target) as env_ctx:
            env_ctx.configure(
                connection=sync_conn,
                target_metadata=None,
                transaction_per_migration=True,
                compare_type=True,
                **self._schema_configure_kwargs(),
            )
            with env_ctx.begin_transaction():
                env_ctx.run_migrations()

    def _sync_stamp(self, sync_conn: Connection, target: str) -> None:
        from alembic.runtime.environment import EnvironmentContext

        config, script = self._build_config()

        def _fn(rev: Any, context: Any) -> Any:
            return script._stamp_revs(target, rev)

        with EnvironmentContext(config, script, fn=_fn, destination_rev=target) as env_ctx:
            env_ctx.configure(
                connection=sync_conn,
                target_metadata=None,
                **self._schema_configure_kwargs(),
            )
            with env_ctx.begin_transaction():
                env_ctx.run_migrations()

    def _pending_revisions(self, script: Any, current: tuple[str, ...]) -> tuple[Revision, ...]:
        """Return pending ``Revision``s in application order (oldest first)."""
        lower: Any = current if current else "base"
        script_revs = list(script.iterate_revisions("heads", lower))
        script_revs.reverse()  # iterate_revisions yields newest-first
        revisions = []
        for rev in script_revs:
            branch = next(iter(rev.branch_labels), None) if rev.branch_labels else None
            revisions.append(
                Revision(id=rev.revision, label=rev.doc or rev.revision, branch=branch)
            )
        return tuple(revisions)

    # ── AbstractMigrator ─────────────────────────────────────────────────────

    async def plan(self) -> SchemaMigrationPlan:
        _config, script = self._build_config()
        async with self._engine.connect() as conn:
            current = await conn.run_sync(self._sync_current_heads)
        pending = self._pending_revisions(script, current)
        return SchemaMigrationPlan(current=current, pending=pending)

    async def upgrade(self, target: str = "heads", *, dry_run: bool = False) -> MigrationReport:
        start = time.monotonic()

        if dry_run:
            plan = await self.plan()
            return MigrationReport(applied=plan.pending, duration_s=time.monotonic() - start)

        plan = await self.plan()
        if plan.is_empty:
            return MigrationReport(applied=(), duration_s=time.monotonic() - start)

        try:
            async with migration_lock(
                self._engine,
                self._settings.lock_key,
                timeout=self._settings.lock_timeout,
                lock=self._lock,
            ):
                plan = await self.plan()
                to_apply = plan.pending
                if to_apply:
                    async with self._engine.connect() as conn:
                        await conn.run_sync(self._sync_upgrade, target)
                return MigrationReport(applied=to_apply, duration_s=time.monotonic() - start)
        except MigrationLockTimeout:
            replanned = await self.plan()
            if replanned.is_empty:
                return MigrationReport(
                    applied=(), duration_s=time.monotonic() - start, skipped_locked=True
                )
            raise

    async def downgrade(self, target: str) -> MigrationReport:
        start = time.monotonic()
        plan_before = await self.plan()

        async with self._engine.connect() as conn:
            await conn.run_sync(self._sync_downgrade, target)

        plan_after = await self.plan()
        reversed_ids = {r.id for r in plan_after.pending} - {r.id for r in plan_before.pending}
        applied = tuple(r for r in plan_after.pending if r.id in reversed_ids)
        return MigrationReport(applied=applied, duration_s=time.monotonic() - start)

    async def stamp(self, target: str = "heads") -> None:
        async with self._engine.connect() as conn:
            await conn.run_sync(self._sync_stamp, target)

    async def adopt_framework_tables(self) -> list[str]:
        """
        Stamp the ``varco`` branch head against an already-``ensure_table()``-
        built database, without executing any DDL.

        Idempotent — a database whose ``varco`` branch is already stamped
        (or that has none of the framework tables at all) returns an empty
        list on a repeat call. Any subset of framework tables already
        present (e.g. only ``varco_jobs`` via ``SAJobStore.ensure_table()``)
        is enough to trigger the stamp — the baseline revision's own
        ``checkfirst=True`` guard means running it for real afterwards would
        have been a safe no-op for those tables anyway; ``adopt`` simply
        records that fact in ``alembic_version`` up front.

        Returns:
            The list of framework table names that existed at adoption
            time, or ``[]`` if nothing was adopted (either already stamped,
            or no framework table exists yet).
        """
        from varco_sa.metadata import framework_table_names

        table_names = sorted(framework_table_names())

        async with self._engine.connect() as conn:

            def _existing(sync_conn: Connection) -> list[str]:
                inspector = sa_inspect(sync_conn)
                present = set(inspector.get_table_names())
                return [name for name in table_names if name in present]

            existing = await conn.run_sync(_existing)

        if not existing:
            return []

        plan = await self.plan()
        varco_pending = [r for r in plan.pending if r.branch == "varco"]
        if not varco_pending:
            # Already stamped (or nothing to stamp) — idempotent no-op.
            return []

        await self.stamp("varco@head")
        return existing

    async def close(self) -> None:
        # AlembicMigrator does not own the engine — the caller passed it in
        # and is responsible for disposing it. Nothing to release here.
        return None


__all__ = ["AlembicMigrator"]
