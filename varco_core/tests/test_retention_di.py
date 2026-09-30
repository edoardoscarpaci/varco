"""
tests.test_retention_di
==========================
Plan 051 (DI-5) / §D-retention — rewritten. ``varco_core.retention.di`` and
``bind_retention_registry`` are gone; ``RetentionPolicy`` is a
``@Multibound`` seam contributed via ``varco_core.di.collect.contribute()``
(or a scanned ``@Singleton``/``@Provider``) and read via ``collect()``/
``acollect()``.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from providify import DIContainer


def _policy(name: str = "p1"):
    from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415
    from varco_core.retention.targets import DlqRetentionTarget  # noqa: PLC0415

    class _FakeDlq:
        supports_delete_where = True

        async def delete_where(self, **kwargs):  # pragma: no cover - not exercised
            return 0

    return RetentionPolicy(
        name=name,
        target=DlqRetentionTarget(dlq=_FakeDlq(), acknowledge_dead_letter_deletion=True),
        cron_expr="0 3 * * *",
        timezone="UTC",
        dry_run=True,
        older_than=timedelta(days=30),
    )


class TestRetentionPolicyContribution:
    def test_bind_retention_registry_module_removed(self) -> None:
        with pytest.raises(ImportError):
            from varco_core.retention.di import bind_retention_registry  # noqa: F401,PLC0415

    def test_contribute_makes_policy_collectable(self) -> None:
        from varco_core.di.collect import collect, contribute  # noqa: PLC0415
        from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415

        container = DIContainer()
        policy = _policy()
        contribute(container, RetentionPolicy, policy)

        result = collect(container, RetentionPolicy)
        assert result == (policy,)

    def test_contributing_same_policy_object_twice_raises(self) -> None:
        from varco_core.di import AlreadyBoundError  # noqa: PLC0415
        from varco_core.di.collect import contribute  # noqa: PLC0415
        from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415

        container = DIContainer()
        policy = _policy()
        contribute(container, RetentionPolicy, policy)

        with pytest.raises(AlreadyBoundError):
            contribute(container, RetentionPolicy, policy)

    async def test_contributing_nothing_starts_no_task(self) -> None:
        """Binding must have no lifecycle side effect — no background task
        spawned just by contributing a policy."""
        import asyncio

        tasks_before = {t for t in asyncio.all_tasks() if not t.done()}
        container = DIContainer()
        from varco_core.di.collect import contribute  # noqa: PLC0415
        from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415

        contribute(container, RetentionPolicy, _policy())
        tasks_after = {t for t in asyncio.all_tasks() if not t.done()}
        assert tasks_after == tasks_before

    def test_never_touches_di_container_current(self) -> None:
        from varco_core.di.collect import contribute  # noqa: PLC0415
        from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415

        before = None
        try:
            before = DIContainer.current()
        except Exception:
            before = None

        container = DIContainer()
        contribute(container, RetentionPolicy, _policy())

        after = None
        try:
            after = DIContainer.current()
        except Exception:
            after = None

        assert before is after
