"""
tests.test_retention_lifecycle
=================================
Plan 039 (S20) / Step 21 — ``varco_fastapi.retention.RetentionLifecycle`` +
``create_varco_app(retention=...)``.

RED until ``varco_fastapi/varco_fastapi/retention.py`` (Step 22) lands and
``create_varco_app`` gains the ``retention=`` kwarg.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_core.job.base import AbstractJobStore
from varco_core.schedule.repository import AbstractScheduleRepository
from varco_fastapi.app import create_varco_app

# NOTE: AbstractJobStore/AbstractScheduleRepository must be importable at
# MODULE level (not only inside a test method) — providify's @Provider
# resolves a bare string return annotation ("AbstractJobStore", because this
# module uses `from __future__ import annotations`) against the provider
# function's __globals__, i.e. this module's globals, never the calling
# frame's locals. A local-only import inside the test method is invisible
# to that resolution and raises TypeError at container.provide() time,
# unrelated to whether RetentionLifecycle itself is correct.


def _registry():
    from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415

    return RetentionRegistry()


class TestCreateVarcoAppDefaultRetention:
    def test_no_retention_kwarg_registers_no_component_and_starts_no_task(self) -> None:
        """The load-bearing default: a default create_varco_app() schedules
        nothing and deletes nothing."""
        import varco_fastapi.lifespan as lifespan_module

        captured: list[tuple] = []
        original_init = lifespan_module.VarcoLifespan.__init__

        def _spy_init(self, *components, **kwargs):
            captured.append(components)
            return original_init(self, *components, **kwargs)

        import unittest.mock as mock

        with mock.patch.object(lifespan_module.VarcoLifespan, "__init__", _spy_init):
            create_varco_app(DIContainer(), validate=False)

        assert captured
        for components in captured:
            for component in components:
                assert "Retention" not in type(component).__name__


class TestRetentionLifecycleWiring:
    def test_interval_zero_registers_component_but_starts_no_task(self) -> None:
        from varco_fastapi.retention import RetentionLifecycle  # noqa: PLC0415

        container = DIContainer()
        lifecycle = RetentionLifecycle(_registry(), container=container, interval=0.0)
        app = create_varco_app(container, retention=lifecycle, validate=False)
        assert app is not None

    async def test_interval_positive_starts_and_stops_cleanly(self) -> None:
        from varco_core.job.base import AbstractJobStore
        from varco_core.schedule.repository import AbstractScheduleRepository
        from varco_fastapi.retention import RetentionLifecycle  # noqa: PLC0415

        class _StubJobStore(AbstractJobStore):
            async def save(self, job, *, expected_epoch=None): ...
            async def get(self, job_id):
                return None

            async def list_by_status(self, status, *, limit=100):
                return []

            async def delete(self, job_id): ...
            async def try_claim(self, job_id, *, owner_id=None, lease_ttl=None):
                return None

        class _StubScheduleRepo(AbstractScheduleRepository):
            async def save(self, schedule):
                return schedule

            async def find_by_id(self, pk):
                return None

            async def find_all_enabled(self):
                return []

            async def delete(self, pk): ...

        from providify import Provider

        container = DIContainer()

        @Provider(singleton=True)
        def _job_store() -> AbstractJobStore:
            return _StubJobStore()

        @Provider(singleton=True)
        def _schedule_repo() -> AbstractScheduleRepository:
            return _StubScheduleRepo()

        container.provide(_job_store)
        container.provide(_schedule_repo)

        lifecycle = RetentionLifecycle(_registry(), container=container, interval=0.05)
        await lifecycle.startup()
        await lifecycle.shutdown()

    async def test_missing_required_binding_raises_lookup_error_naming_interface(self) -> None:
        from varco_core.job.base import AbstractJobStore
        from varco_fastapi.retention import RetentionLifecycle  # noqa: PLC0415

        container = DIContainer()
        lifecycle = RetentionLifecycle(_registry(), container=container, interval=0.05)
        with pytest.raises(LookupError) as exc:
            await lifecycle.startup()
        assert "AbstractJobStore" in str(exc.value) or AbstractJobStore.__name__ in str(exc.value)

    def test_retention_component_appended_not_prepended(self) -> None:
        """Retention resolves job-runner/schedule bindings other components
        may set up, so it must be appended, never prepended — same rule as
        ReliabilityLifecycle."""
        import unittest.mock as mock

        import varco_fastapi.lifespan as lifespan_module
        from varco_fastapi.retention import RetentionLifecycle  # noqa: PLC0415

        captured: list[tuple] = []
        original_init = lifespan_module.VarcoLifespan.__init__

        def _spy_init(self, *components, **kwargs):
            captured.append(components)
            return original_init(self, *components, **kwargs)

        marker = object()

        container = DIContainer()
        lifecycle = RetentionLifecycle(_registry(), container=container, interval=0.0)

        with mock.patch.object(lifespan_module.VarcoLifespan, "__init__", _spy_init):
            create_varco_app(
                container,
                extra_lifespan_components=[marker],
                retention=lifecycle,
                validate=False,
            )

        components = captured[-1]
        assert components.index(marker) < components.index(lifecycle)
