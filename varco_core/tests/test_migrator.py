"""
Unit tests for varco_core.migrator
"""

import pytest
from varco_core.migrator import DomainMigrator, MigrationError


# ── Fixtures / helpers ────────────────────────────────────────────────────────


def add_slug(data: dict) -> dict:
    data["slug"] = data["name"].lower().replace(" ", "-")
    return data


def add_bio(data: dict) -> dict:
    data.setdefault("bio", "")
    return data


def upper_email(data: dict) -> dict:
    data["email"] = data["email"].upper()
    return data


class TwoStepMigrator(DomainMigrator):
    """v1 → v2 (add_slug), v2 → v3 (add_bio). current_version = 3."""

    steps = [add_slug, add_bio]


class OneStepMigrator(DomainMigrator):
    steps = [add_slug]


class NoStepMigrator(DomainMigrator):
    """Represents a model at v1 with no migrations yet."""

    steps = []


# ── current_version ───────────────────────────────────────────────────────────


def test_current_version_two_steps():
    assert TwoStepMigrator().current_version() == 3


def test_current_version_one_step():
    assert OneStepMigrator().current_version() == 2


def test_current_version_no_steps():
    assert NoStepMigrator().current_version() == 1


# ── migrate: full chain ───────────────────────────────────────────────────────


def test_migrate_from_v1_runs_all_steps():
    data = {"name": "Hello World", "email": "x@x.com"}
    result = TwoStepMigrator().migrate(data, from_version=1)
    assert result["slug"] == "hello-world"
    assert result["bio"] == ""
    assert result["definition_version"] == 3


def test_migrate_from_v2_skips_first_step():
    """Starting at v2 should only run add_bio, not add_slug."""
    data = {"name": "Hello World", "slug": "already-set", "email": "x@x.com"}
    result = TwoStepMigrator().migrate(data, from_version=2)
    assert result["slug"] == "already-set"  # untouched
    assert result["bio"] == ""
    assert result["definition_version"] == 3


def test_migrate_at_current_version_is_noop():
    """from_version == current_version → no steps run, definition_version stamped."""
    data = {"name": "Hi", "slug": "hi", "bio": "existing", "email": "x@x.com"}
    result = TwoStepMigrator().migrate(data, from_version=3)
    assert result["bio"] == "existing"
    assert result["definition_version"] == 3


def test_migrate_updates_definition_version_key():
    data = {"name": "Test", "email": "a@b.com"}
    result = OneStepMigrator().migrate(data, from_version=1)
    assert result["definition_version"] == 2


# ── migrate: error cases ──────────────────────────────────────────────────────


def test_migrate_from_version_zero_raises():
    with pytest.raises(MigrationError, match="cannot migrate from version 0"):
        TwoStepMigrator().migrate({}, from_version=0)


def test_migrate_from_version_above_current_raises():
    """DB row from a future app version — deployment mismatch."""
    with pytest.raises(MigrationError, match="cannot migrate from version 99"):
        TwoStepMigrator().migrate({}, from_version=99)


# ── parametrised migrator ─────────────────────────────────────────────────────


class CountryMigrator(DomainMigrator):
    """Parametrised: injects a configurable default country."""

    def __init__(self, default_country: str = "US"):
        self._default = default_country
        self.steps = [self._add_country]

    def _add_country(self, data: dict) -> dict:
        data.setdefault("country", self._default)
        return data


def test_parametrised_migrator_uses_constructor_value():
    m = CountryMigrator(default_country="IT")
    result = m.migrate({"name": "Edo"}, from_version=1)
    assert result["country"] == "IT"
    assert result["definition_version"] == 2


def test_parametrised_migrator_default_value():
    m = CountryMigrator()
    result = m.migrate({"name": "Edo"}, from_version=1)
    assert result["country"] == "US"


def test_parametrised_migrator_existing_key_not_overwritten():
    m = CountryMigrator(default_country="IT")
    result = m.migrate({"name": "Edo", "country": "DE"}, from_version=1)
    assert result["country"] == "DE"  # setdefault preserves existing


# ── factory callable ─────────────────────────────────────────────────────────


def test_migrator_accepted_as_factory_callable():
    """A zero-arg callable (e.g. lambda) should be called by the mapper."""
    factory = lambda: CountryMigrator("FR")  # noqa: E731
    # Simulate mapper normalisation
    migrator = factory() if not isinstance(factory, DomainMigrator) else factory
    result = migrator.migrate({"name": "Test"}, from_version=1)
    assert result["country"] == "FR"
