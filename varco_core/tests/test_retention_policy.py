"""
tests.test_retention_policy
=============================
Plan 039 (S20) / Step 3 — the eight §D-S20-safety guards for
``RetentionPolicy``/``RetentionRegistry`` (``varco_core.retention.policy``).

RED until ``varco_core/varco_core/retention/policy.py`` (Step 4) lands.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import timedelta

import pytest


class _FakeTarget:
    """A minimal RetentionTarget stand-in for policy-level (not target-level)
    tests — no purge() call is ever exercised here."""

    supports_older_than = True
    supports_dry_run = True

    @property
    def kind(self) -> str:
        return "fake"

    async def purge(self, *, older_than, limit, dry_run):  # pragma: no cover - unused here
        raise NotImplementedError


class _FakeTargetNoOlderThan(_FakeTarget):
    supports_older_than = False


def _policy_cls():
    from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415

    return RetentionPolicy


def _registry_cls():
    from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415

    return RetentionRegistry


class TestRetentionPolicyDryRunRequired:
    def test_missing_dry_run_raises_type_error(self) -> None:
        """Guard 1 — ``dry_run`` is a required field with no default."""
        RetentionPolicy = _policy_cls()
        with pytest.raises(TypeError):
            RetentionPolicy(  # type: ignore[call-arg]
                name="p",
                target=_FakeTarget(),
                cron_expr="0 3 * * *",
                timezone="UTC",
                older_than=timedelta(days=30),
            )


class TestRetentionPolicyOlderThanFloor:
    def test_zero_older_than_raises_value_error(self) -> None:
        """Guard 2 — ``older_than <= 0`` refuses, no acknowledgement escape."""
        RetentionPolicy = _policy_cls()
        with pytest.raises(ValueError):
            RetentionPolicy(
                name="p",
                target=_FakeTarget(),
                cron_expr="0 3 * * *",
                timezone="UTC",
                dry_run=True,
                older_than=timedelta(0),
            )

    def test_negative_older_than_raises_value_error(self) -> None:
        """Guard 2 — a negative window is never valid, ever."""
        RetentionPolicy = _policy_cls()
        with pytest.raises(ValueError):
            RetentionPolicy(
                name="p",
                target=_FakeTarget(),
                cron_expr="0 3 * * *",
                timezone="UTC",
                dry_run=True,
                older_than=timedelta(days=-1),
            )


class TestRetentionPolicyShortRetentionFloor:
    def test_sub_24h_without_acknowledgement_raises_value_error(self) -> None:
        """Guard 3 — under 24h needs acknowledge_short_retention=True."""
        RetentionPolicy = _policy_cls()
        with pytest.raises(ValueError) as exc:
            RetentionPolicy(
                name="p",
                target=_FakeTarget(),
                cron_expr="0 3 * * *",
                timezone="UTC",
                dry_run=True,
                older_than=timedelta(hours=1),
            )
        assert "acknowledge_short_retention" in str(exc.value)

    def test_sub_24h_with_acknowledgement_is_accepted(self) -> None:
        """Guard 3 — the escape hatch works when explicitly set."""
        RetentionPolicy = _policy_cls()
        policy = RetentionPolicy(
            name="p",
            target=_FakeTarget(),
            cron_expr="0 3 * * *",
            timezone="UTC",
            dry_run=True,
            older_than=timedelta(hours=1),
            acknowledge_short_retention=True,
        )
        assert policy.older_than == timedelta(hours=1)


class TestRetentionRegistryOlderThanCapability:
    def test_older_than_on_unsupported_target_raises_at_register(self) -> None:
        """Guard 4 — older_than set on a supports_older_than=False target is
        a wiring-time ValueError, never a silently ignored field."""
        RetentionPolicy = _policy_cls()
        RetentionRegistry = _registry_cls()
        policy = RetentionPolicy(
            name="p",
            target=_FakeTargetNoOlderThan(),
            cron_expr="0 3 * * *",
            timezone="UTC",
            dry_run=True,
            older_than=timedelta(days=30),
        )
        registry = RetentionRegistry()
        with pytest.raises(ValueError):
            registry.register(policy)

    def test_missing_older_than_on_supporting_target_raises_at_register(self) -> None:
        """Guard 4 — older_than absent on a supports_older_than=True target
        is also a wiring-time ValueError."""
        RetentionPolicy = _policy_cls()
        RetentionRegistry = _registry_cls()
        policy = RetentionPolicy(
            name="p",
            target=_FakeTarget(),
            cron_expr="0 3 * * *",
            timezone="UTC",
            dry_run=True,
            older_than=None,
        )
        registry = RetentionRegistry()
        with pytest.raises(ValueError):
            registry.register(policy)


class TestRetentionPolicyTenantIds:
    def test_empty_tuple_tenant_ids_raises_value_error(self) -> None:
        """An empty tenant_ids tuple is always a typo — never 'no tenants'."""
        RetentionPolicy = _policy_cls()
        with pytest.raises(ValueError):
            RetentionPolicy(
                name="p",
                target=_FakeTarget(),
                cron_expr="0 3 * * *",
                timezone="UTC",
                dry_run=True,
                older_than=timedelta(days=30),
                tenant_ids=(),
            )


class TestRetentionRegistryDuplicateName:
    def test_duplicate_name_in_one_registry_raises_value_error(self) -> None:
        RetentionPolicy = _policy_cls()
        RetentionRegistry = _registry_cls()
        policy1 = RetentionPolicy(
            name="dup",
            target=_FakeTarget(),
            cron_expr="0 3 * * *",
            timezone="UTC",
            dry_run=True,
            older_than=timedelta(days=30),
        )
        policy2 = RetentionPolicy(
            name="dup",
            target=_FakeTarget(),
            cron_expr="0 4 * * *",
            timezone="UTC",
            dry_run=True,
            older_than=timedelta(days=60),
        )
        registry = RetentionRegistry()
        registry.register(policy1)
        with pytest.raises(ValueError):
            registry.register(policy2)


class TestRetentionPolicyFrozen:
    def test_policy_is_frozen(self) -> None:
        RetentionPolicy = _policy_cls()
        policy = RetentionPolicy(
            name="p",
            target=_FakeTarget(),
            cron_expr="0 3 * * *",
            timezone="UTC",
            dry_run=True,
            older_than=timedelta(days=30),
        )
        with pytest.raises(FrozenInstanceError):
            policy.dry_run = False  # type: ignore[misc]
