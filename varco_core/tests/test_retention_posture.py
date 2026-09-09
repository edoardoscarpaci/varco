"""
tests.test_retention_posture
===============================
Plan 039 (S20) / Step 18 — ``inspect_retention_posture()``
(``varco_core.retention.posture``), mirroring
``revocation/posture.py:72-126``'s never-raises, pure-read shape.

RED until ``varco_core/varco_core/retention/posture.py`` (Step 19) lands.
"""

from __future__ import annotations

from datetime import timedelta


def _policy(name, *, target, dry_run, older_than=timedelta(days=30), tenant_ids=None):
    from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415

    return RetentionPolicy(
        name=name,
        target=target,
        cron_expr="0 3 * * *",
        timezone="UTC",
        dry_run=dry_run,
        older_than=older_than,
        tenant_ids=tenant_ids,
    )


class _FakeTarget:
    supports_older_than = True
    supports_dry_run = True
    kind = "dlq"

    async def purge(self, *, older_than, limit, dry_run):  # pragma: no cover - unused here
        raise NotImplementedError


class TestInspectRetentionPostureNoArgument:
    def test_no_argument_returns_configured_false(self) -> None:
        from varco_core.retention.posture import inspect_retention_posture  # noqa: PLC0415

        report = inspect_retention_posture()
        assert report.configured is False

    def test_never_raises_on_a_malformed_argument(self) -> None:
        from varco_core.retention.posture import inspect_retention_posture  # noqa: PLC0415

        # Must never raise, even given a nonsensical registry-shaped object.
        report = inspect_retention_posture(registry=None)
        assert report is not None


class TestInspectRetentionPostureWithRegistry:
    def test_reports_destructive_and_dry_run_counts(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.posture import inspect_retention_posture  # noqa: PLC0415

        registry = RetentionRegistry()
        registry.register(_policy("destructive", target=_FakeTarget(), dry_run=False))
        registry.register(_policy("preview", target=_FakeTarget(), dry_run=True))

        report = inspect_retention_posture(registry=registry)
        assert report.configured is True
        assert report.policy_count == 2
        assert report.destructive_count == 1
        assert report.dry_run_count == 1

    def test_platform_wide_policies_only_tenant_ids_none(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.posture import inspect_retention_posture  # noqa: PLC0415

        registry = RetentionRegistry()
        registry.register(_policy("global", target=_FakeTarget(), dry_run=True, tenant_ids=None))
        registry.register(_policy("scoped", target=_FakeTarget(), dry_run=True, tenant_ids=("t1",)))

        report = inspect_retention_posture(registry=registry)
        assert "global" in report.platform_wide_policies
        assert "scoped" not in report.platform_wide_policies

    def test_reports_dlq_policies_and_short_retention_policies(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.posture import inspect_retention_posture  # noqa: PLC0415

        class _DlqTarget(_FakeTarget):
            kind = "dlq"

        # short retention needs the acknowledgement to construct at all.
        from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415

        short_policy = RetentionPolicy(
            name="short-ack",
            target=_FakeTarget(),
            cron_expr="0 3 * * *",
            timezone="UTC",
            dry_run=True,
            older_than=timedelta(hours=2),
            acknowledge_short_retention=True,
        )
        registry2 = RetentionRegistry()
        registry2.register(_policy("dlq-nightly", target=_DlqTarget(), dry_run=True))
        registry2.register(short_policy)

        report = inspect_retention_posture(registry=registry2)
        assert "dlq-nightly" in report.dlq_policies
        assert "short-ack" in report.short_retention_policies

    def test_reports_kinds_set(self) -> None:
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415
        from varco_core.retention.posture import inspect_retention_posture  # noqa: PLC0415

        class _DlqTarget(_FakeTarget):
            kind = "dlq"

        registry = RetentionRegistry()
        registry.register(_policy("p1", target=_DlqTarget(), dry_run=True))
        report = inspect_retention_posture(registry=registry)
        assert "dlq" in report.kinds
