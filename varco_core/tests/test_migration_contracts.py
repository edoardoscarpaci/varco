"""
Failing tests for varco_core.migration contracts + settings (Plan 006, Phase 1,
step 11).

Contracts only — ``AbstractMigrator``, ``SchemaMigrationPlan``, ``MigrationReport``,
``MigrationSettings``. Nothing here touches alembic or pymongo.
"""

from __future__ import annotations

import pytest

# ── MigrationSettings.from_env() ────────────────────────────────────────────


async def test_from_env_with_empty_environ_yields_documented_defaults() -> None:
    from varco_core.migration.settings import MigrationSettings

    settings = MigrationSettings.from_env()

    assert settings.mode == "off"
    assert settings.on_failure == "fail"
    assert settings.lock_timeout == 30.0
    assert settings.timeout == 300.0
    assert settings.lock_key == "varco:migrate"


async def test_from_env_parses_mode_on_failure_and_lock_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from varco_core.migration.settings import MigrationSettings

    monkeypatch.setenv("VARCO_MIGRATE_MODE", "upgrade")
    monkeypatch.setenv("VARCO_MIGRATE_ON_FAILURE", "warn")
    monkeypatch.setenv("VARCO_MIGRATE_LOCK_TIMEOUT", "5")

    settings = MigrationSettings.from_env()

    assert settings.mode == "upgrade"
    assert settings.on_failure == "warn"
    assert settings.lock_timeout == 5.0


async def test_from_env_unknown_mode_raises_valueerror_naming_legal_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic import ValidationError
    from varco_core.migration.settings import MigrationSettings

    monkeypatch.setenv("VARCO_MIGRATE_MODE", "nonsense")

    with pytest.raises(ValidationError) as exc:
        MigrationSettings.from_env()

    # The three legal values must all be named in the error, per step 11.
    message = str(exc.value)
    assert "off" in message
    assert "check" in message
    assert "upgrade" in message


# ── SchemaMigrationPlan ────────────────────────────────────────────────────────────


async def test_migration_plan_is_empty_true_for_no_pending_revisions() -> None:
    from varco_core.migration.base import SchemaMigrationPlan

    plan = SchemaMigrationPlan(current=(), pending=())

    assert plan.is_empty is True


async def test_migration_plan_is_empty_false_when_revisions_pending() -> None:
    from varco_core.migration.base import Revision, SchemaMigrationPlan

    plan = SchemaMigrationPlan(current=(), pending=(Revision(id="0001", label="init"),))

    assert plan.is_empty is False


# ── MigrationReport ──────────────────────────────────────────────────────────


async def test_migration_report_format_renders_applied_revisions_and_duration() -> None:
    from varco_core.migration.base import MigrationReport, Revision

    report = MigrationReport(
        applied=(Revision(id="0001", label="create orders"),),
        duration_s=1.234,
    )

    formatted = report.format()

    assert "0001" in formatted
    assert "create orders" in formatted
    assert "1.2" in formatted or "1.23" in formatted


# ── AbstractMigrator ─────────────────────────────────────────────────────────


async def test_abstract_migrator_cannot_be_instantiated_directly() -> None:
    from varco_core.migration.base import AbstractMigrator

    with pytest.raises(TypeError):
        AbstractMigrator()  # type: ignore[abstract]


async def test_abstract_migrator_check_is_concrete_and_raises_when_pending() -> None:
    from varco_core.migration.base import AbstractMigrator, Revision, SchemaMigrationPlan
    from varco_core.migration.errors import PendingMigrationsError

    class _StubMigrator(AbstractMigrator):
        async def plan(self) -> SchemaMigrationPlan:
            return SchemaMigrationPlan(current=(), pending=(Revision(id="0001", label="init"),))

        async def upgrade(self, target: str = "heads", *, dry_run: bool = False):
            raise NotImplementedError

        async def downgrade(self, target: str):
            raise NotImplementedError

        async def stamp(self, target: str = "heads") -> None:
            raise NotImplementedError

    migrator = _StubMigrator()

    with pytest.raises(PendingMigrationsError):
        await migrator.check()


async def test_abstract_migrator_check_returns_plan_when_nothing_pending() -> None:
    from varco_core.migration.base import AbstractMigrator, SchemaMigrationPlan

    class _StubMigrator(AbstractMigrator):
        async def plan(self) -> SchemaMigrationPlan:
            return SchemaMigrationPlan(current=("0001",), pending=())

        async def upgrade(self, target: str = "heads", *, dry_run: bool = False):
            raise NotImplementedError

        async def downgrade(self, target: str):
            raise NotImplementedError

        async def stamp(self, target: str = "heads") -> None:
            raise NotImplementedError

    migrator = _StubMigrator()

    result = await migrator.check()

    assert result.is_empty is True


async def test_abstract_migrator_close_is_concrete_noop_by_default() -> None:
    from varco_core.migration.base import AbstractMigrator, SchemaMigrationPlan

    class _StubMigrator(AbstractMigrator):
        async def plan(self) -> SchemaMigrationPlan:
            return SchemaMigrationPlan(current=(), pending=())

        async def upgrade(self, target: str = "heads", *, dry_run: bool = False):
            raise NotImplementedError

        async def downgrade(self, target: str):
            raise NotImplementedError

        async def stamp(self, target: str = "heads") -> None:
            raise NotImplementedError

    migrator = _StubMigrator()

    # Must not raise — no override needed for a migrator with no engine.
    await migrator.close()


# ── InMemoryMigrator ─────────────────────────────────────────────────────────


async def test_inmemory_migrator_upgrade_applies_pending_and_records_call() -> None:
    from varco_core.migration.base import Revision
    from varco_core.migration.inmemory import InMemoryMigrator

    migrator = InMemoryMigrator(
        revisions=[
            Revision(id="0001", label="init"),
            Revision(id="0002", label="add col"),
        ]
    )

    plan_before = await migrator.plan()
    assert len(plan_before.pending) == 2

    report = await migrator.upgrade()

    assert len(report.applied) == 2
    plan_after = await migrator.plan()
    assert plan_after.is_empty is True


async def test_inmemory_migrator_records_calls_for_assertions() -> None:
    from varco_core.migration.inmemory import InMemoryMigrator

    migrator = InMemoryMigrator(revisions=[])

    await migrator.plan()
    await migrator.upgrade()

    assert migrator.calls == ["plan", "upgrade"]


async def test_inmemory_migrator_raises_on_configured_failing_call() -> None:
    from varco_core.migration.base import Revision
    from varco_core.migration.inmemory import InMemoryMigrator

    migrator = InMemoryMigrator(
        revisions=[Revision(id="0001", label="init")],
        fail_on_upgrade_call=1,
    )

    with pytest.raises(Exception):  # noqa: B017 — exact type is migrator-defined
        await migrator.upgrade()
