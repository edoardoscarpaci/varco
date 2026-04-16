"""
tests.test_migrator_v2
========================
Tests for ``DomainMigrator`` v2 features:
  - ``StepSpec``     — callable wrapper with optional ``down`` function
  - ``MigrationPlan`` — immutable dry-run result
  - ``dry_run()``    — inspect plan without executing
  - ``rollback()``   — reverse migration chain

All tests are pure unit tests — no I/O, no external dependencies.
"""

from __future__ import annotations

import pytest

from varco_core.migrator import DomainMigrator, MigrationError, MigrationPlan, StepSpec


# ── Shared step functions ──────────────────────────────────────────────────────


def add_slug(data: dict) -> dict:
    data["slug"] = data["name"].lower().replace(" ", "-")
    return data


def remove_slug(data: dict) -> dict:
    data.pop("slug", None)
    return data


def add_bio(data: dict) -> dict:
    data.setdefault("bio", "")
    return data


def remove_bio(data: dict) -> dict:
    data.pop("bio", None)
    return data


def add_rank(data: dict) -> dict:
    data.setdefault("rank", 0)
    return data


def remove_rank(data: dict) -> dict:
    data.pop("rank", None)
    return data


# ── StepSpec — construction and callable ──────────────────────────────────────


def test_step_spec_callable_invokes_up():
    """StepSpec instance is callable and delegates to up."""
    spec = StepSpec(up=add_slug)
    result = spec({"name": "Hello World"})
    assert result["slug"] == "hello-world"


def test_step_spec_up_only():
    spec = StepSpec(up=add_slug)
    assert spec.up is add_slug
    assert spec.down is None


def test_step_spec_up_and_down():
    spec = StepSpec(up=add_slug, down=remove_slug)
    assert spec.down is remove_slug


def test_step_spec_explicit_name():
    spec = StepSpec(up=add_slug, name="add-user-slug")
    assert spec.display_name == "add-user-slug"


def test_step_spec_name_falls_back_to_up_name():
    """When no name is provided, display_name returns up.__name__."""
    spec = StepSpec(up=add_slug)
    assert spec.display_name == "add_slug"


def test_step_spec_is_frozen():
    """StepSpec must be immutable (frozen dataclass)."""
    spec = StepSpec(up=add_slug)
    with pytest.raises((AttributeError, TypeError)):
        spec.up = remove_slug  # type: ignore[misc]


def test_step_spec_repr():
    spec = StepSpec(up=add_slug, down=remove_slug)
    r = repr(spec)
    assert "add_slug" in r
    assert "has_rollback=True" in r


def test_step_spec_no_down_repr():
    spec = StepSpec(up=add_slug)
    r = repr(spec)
    assert "has_rollback=False" in r


# ── StepSpec works inside migrate() ───────────────────────────────────────────


class SlugBioMigratorSpec(DomainMigrator):
    """Two StepSpec steps: v1→v2 (add_slug), v2→v3 (add_bio)."""

    steps = [
        StepSpec(up=add_slug, down=remove_slug, name="add_slug"),
        StepSpec(up=add_bio, down=remove_bio, name="add_bio"),
    ]


def test_migrate_with_step_specs_full_chain():
    result = SlugBioMigratorSpec().migrate({"name": "Hello World"}, from_version=1)
    assert result["slug"] == "hello-world"
    assert result["bio"] == ""
    assert result["definition_version"] == 3


def test_migrate_with_step_specs_partial_chain():
    result = SlugBioMigratorSpec().migrate(
        {"name": "Test", "slug": "test"}, from_version=2
    )
    assert result["slug"] == "test"  # step 1 skipped
    assert result["bio"] == ""
    assert result["definition_version"] == 3


def test_migrate_mixes_plain_and_step_spec():
    """StepSpec and plain callables can coexist in the same steps list."""

    class MixedMigrator(DomainMigrator):
        steps = [
            add_slug,  # plain callable
            StepSpec(up=add_bio, down=remove_bio),  # StepSpec
        ]

    result = MixedMigrator().migrate({"name": "Hi There"}, from_version=1)
    assert result["slug"] == "hi-there"
    assert result["bio"] == ""
    assert result["definition_version"] == 3


# ── MigrationPlan ─────────────────────────────────────────────────────────────


def test_migration_plan_step_names():
    plan = MigrationPlan(
        from_version=1,
        to_version=3,
        steps=[("add_slug", True), ("add_bio", False)],
    )
    assert plan.step_names == ["add_slug", "add_bio"]


def test_migration_plan_can_rollback_all_have_down():
    plan = MigrationPlan(
        from_version=1,
        to_version=3,
        steps=[("s1", True), ("s2", True)],
    )
    assert plan.can_rollback is True


def test_migration_plan_can_rollback_false_when_any_missing():
    plan = MigrationPlan(
        from_version=1,
        to_version=3,
        steps=[("s1", True), ("s2", False)],
    )
    assert plan.can_rollback is False


def test_migration_plan_is_noop_when_no_steps():
    plan = MigrationPlan(from_version=3, to_version=3, steps=[])
    assert plan.is_noop is True


def test_migration_plan_is_not_noop_with_steps():
    plan = MigrationPlan(
        from_version=1,
        to_version=2,
        steps=[("add_slug", True)],
    )
    assert plan.is_noop is False


def test_migration_plan_is_frozen():
    plan = MigrationPlan(from_version=1, to_version=2, steps=[])
    with pytest.raises((AttributeError, TypeError)):
        plan.from_version = 2  # type: ignore[misc]


def test_migration_plan_repr():
    plan = MigrationPlan(
        from_version=1,
        to_version=3,
        steps=[("add_slug", True), ("add_bio", False)],
    )
    r = repr(plan)
    assert "v1" in r
    assert "v3" in r


# ── dry_run() ─────────────────────────────────────────────────────────────────


def test_dry_run_returns_migration_plan():
    plan = SlugBioMigratorSpec().dry_run(from_version=1)
    assert isinstance(plan, MigrationPlan)
    assert plan.from_version == 1
    assert plan.to_version == 3


def test_dry_run_lists_correct_step_names():
    plan = SlugBioMigratorSpec().dry_run(from_version=1)
    assert plan.step_names == ["add_slug", "add_bio"]


def test_dry_run_partial_plan():
    """Starting at v2 should only show the second step."""
    plan = SlugBioMigratorSpec().dry_run(from_version=2)
    assert plan.step_names == ["add_bio"]
    assert plan.from_version == 2
    assert plan.to_version == 3


def test_dry_run_at_current_version_is_noop():
    plan = SlugBioMigratorSpec().dry_run(from_version=3)
    assert plan.is_noop is True
    assert plan.step_names == []


def test_dry_run_reports_rollback_availability():
    plan = SlugBioMigratorSpec().dry_run(from_version=1)
    assert plan.can_rollback is True
    # Both steps have down= provided
    assert all(has_down for _, has_down in plan.steps)


def test_dry_run_reports_missing_rollback():
    """A plain callable step has no down — plan should mark it as False."""

    class NoDryMigrator(DomainMigrator):
        steps = [add_slug]  # plain callable, no down

    plan = NoDryMigrator().dry_run(from_version=1)
    assert plan.can_rollback is False


def test_dry_run_does_not_modify_data():
    """dry_run must not execute any steps or change any data."""
    data_before = {"name": "Test", "definition_version": 1}
    SlugBioMigratorSpec().dry_run(from_version=1)
    # Data should be completely unchanged
    assert data_before == {"name": "Test", "definition_version": 1}


def test_dry_run_invalid_version_raises():
    with pytest.raises(MigrationError, match="cannot plan"):
        SlugBioMigratorSpec().dry_run(from_version=99)


def test_dry_run_zero_version_raises():
    with pytest.raises(MigrationError):
        SlugBioMigratorSpec().dry_run(from_version=0)


# ── rollback() ────────────────────────────────────────────────────────────────


class ThreeStepMigrator(DomainMigrator):
    """v1→v2 (add_slug), v2→v3 (add_bio), v3→v4 (add_rank). All reversible."""

    steps = [
        StepSpec(up=add_slug, down=remove_slug),
        StepSpec(up=add_bio, down=remove_bio),
        StepSpec(up=add_rank, down=remove_rank),
    ]


def test_rollback_full_chain():
    """Roll back from v4 all the way to v1."""
    m = ThreeStepMigrator()
    data = {
        "name": "Test",
        "slug": "test",
        "bio": "",
        "rank": 0,
        "definition_version": 4,
    }
    result = m.rollback(data, from_version=4)
    assert "slug" not in result
    assert "bio" not in result
    assert "rank" not in result
    assert result["definition_version"] == 1


def test_rollback_partial_chain():
    """Roll back only the most recent step: v4 → v3."""
    m = ThreeStepMigrator()
    data = {
        "name": "Test",
        "slug": "test",
        "bio": "",
        "rank": 0,
        "definition_version": 4,
    }
    result = m.rollback(data, from_version=4, to_version=3)
    assert result["slug"] == "test"  # unchanged — not rolled back
    assert result["bio"] == ""  # unchanged
    assert "rank" not in result  # rolled back
    assert result["definition_version"] == 3


def test_rollback_to_v2():
    """Roll back from v4 to v2 (undo add_bio and add_rank in reverse order)."""
    m = ThreeStepMigrator()
    data = {"name": "Test", "slug": "test", "bio": "", "rank": 0}
    result = m.rollback(data, from_version=4, to_version=2)
    assert result["slug"] == "test"  # v1→v2 step: NOT rolled back
    assert "bio" not in result
    assert "rank" not in result
    assert result["definition_version"] == 2


def test_rollback_updates_definition_version():
    m = ThreeStepMigrator()
    data = {"name": "x", "slug": "x", "bio": "", "rank": 0}
    result = m.rollback(data, from_version=4, to_version=2)
    assert result["definition_version"] == 2


def test_rollback_requires_step_spec_with_down():
    """Plain callable in steps → rollback raises MigrationError."""

    class NoRollbackMigrator(DomainMigrator):
        steps = [add_slug]  # no down function

    m = NoRollbackMigrator()
    data = {"name": "Test", "slug": "test"}
    with pytest.raises(MigrationError, match="no 'down' function"):
        m.rollback(data, from_version=2)


def test_rollback_step_spec_without_down_raises():
    """StepSpec with down=None → rollback raises MigrationError."""

    class PartialSpecMigrator(DomainMigrator):
        steps = [
            StepSpec(up=add_slug, down=remove_slug),
            StepSpec(up=add_bio, down=None),  # no rollback
        ]

    m = PartialSpecMigrator()
    data = {"name": "Test", "slug": "test", "bio": ""}
    with pytest.raises(MigrationError, match="no 'down' function"):
        m.rollback(data, from_version=3)


def test_rollback_aborts_before_any_mutation_if_missing_down():
    """Pre-flight check: if any step lacks down, NO data should be changed."""

    class PartialMigrator(DomainMigrator):
        steps = [
            StepSpec(up=add_slug, down=None),  # missing down
            StepSpec(up=add_bio, down=remove_bio),
        ]

    m = PartialMigrator()
    original = {"name": "Test", "slug": "test", "bio": ""}
    import copy

    data = copy.deepcopy(original)

    with pytest.raises(MigrationError):
        m.rollback(data, from_version=3)

    # Data must be completely unchanged
    assert data == original


def test_rollback_to_version_must_be_less_than_from_version():
    m = ThreeStepMigrator()
    with pytest.raises(MigrationError, match="must be less than from_version"):
        m.rollback({}, from_version=2, to_version=2)


def test_rollback_to_version_above_from_version_raises():
    m = ThreeStepMigrator()
    with pytest.raises(MigrationError, match="must be less than from_version"):
        m.rollback({}, from_version=2, to_version=3)


def test_rollback_from_version_zero_raises():
    m = ThreeStepMigrator()
    with pytest.raises(MigrationError, match="cannot rollback from version"):
        m.rollback({}, from_version=0)


def test_rollback_from_version_above_current_raises():
    m = ThreeStepMigrator()
    with pytest.raises(MigrationError, match="cannot rollback from version"):
        m.rollback({}, from_version=99)


def test_rollback_to_version_zero_raises():
    m = ThreeStepMigrator()
    with pytest.raises(MigrationError, match="cannot rollback to version"):
        m.rollback({}, from_version=4, to_version=0)


# ── round-trip: migrate then rollback ─────────────────────────────────────────


def test_migrate_then_rollback_is_identity():
    """
    migrate(v1→vN) then rollback(vN→v1) should yield the original data,
    minus the ``definition_version`` key (which is added by migrate).
    """
    m = ThreeStepMigrator()
    original = {"name": "Round Trip"}
    migrated = m.migrate(dict(original), from_version=1)
    assert migrated["definition_version"] == 4

    rolled_back = m.rollback(migrated, from_version=4, to_version=1)
    # original keys should be present; extra keys from migration should be gone
    assert rolled_back["name"] == "Round Trip"
    assert "slug" not in rolled_back
    assert "bio" not in rolled_back
    assert "rank" not in rolled_back
    assert rolled_back["definition_version"] == 1


# ── StepSpec subclass independence ────────────────────────────────────────────


def test_step_spec_subclasses_do_not_share_steps():
    """Subclasses with StepSpec steps must have independent lists."""

    class ParentMigrator(DomainMigrator):
        steps = [StepSpec(up=add_slug, down=remove_slug)]

    class ChildMigrator(ParentMigrator):
        pass  # inherits steps

    class SiblingMigrator(ParentMigrator):
        steps = [StepSpec(up=add_bio, down=remove_bio)]

    assert ParentMigrator().current_version() == 2
    assert ChildMigrator().current_version() == 2
    assert SiblingMigrator().current_version() == 2
    # Adding a step to child must not affect parent
    ChildMigrator.steps.append(StepSpec(up=add_bio, down=remove_bio))
    assert ParentMigrator().current_version() == 2
    assert ChildMigrator().current_version() == 3
