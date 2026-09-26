"""
Plan 047 / Step 4 — `InMemoryJobStore` moves to `varco_core.job.memory`.

`varco_fastapi/varco_fastapi/job/store.py:39-40` moves to
`varco_core/varco_core/job/memory.py`, gains `qualifier="in_memory"`
(§D-vocab's `job_store` row), and `varco_fastapi.job.store` becomes a
re-export module so `from varco_fastapi.job.store import InMemoryJobStore`
keeps working (Migration table).

RED MODE: `varco_core/varco_core/job/memory.py` does not exist yet — every
test here is expected to fail with `ModuleNotFoundError`.
"""

from __future__ import annotations

from providify import DIContainer


def test_in_memory_job_store_lives_in_varco_core_job_memory() -> None:
    from varco_core.job.memory import InMemoryJobStore

    assert InMemoryJobStore is not None


def test_in_memory_job_store_is_resolvable_by_default_with_in_memory_qualifier() -> None:
    from varco_core.job.base import AbstractJobStore
    from varco_core.job.memory import InMemoryJobStore

    container = DIContainer()
    container.scan("varco_core", recursive=True)

    resolved = container.get(AbstractJobStore, qualifier="in_memory")
    assert isinstance(resolved, InMemoryJobStore)


def test_in_memory_job_store_binding_is_a_fallback() -> None:
    from varco_core.job.base import AbstractJobStore

    container = DIContainer()
    container.scan("varco_core", recursive=True)

    binding = container.get_binding(AbstractJobStore, qualifier="in_memory")
    assert binding.fallback is True


def test_varco_fastapi_job_store_re_exports_in_memory_job_store() -> None:
    # Migration table: "still works; canonical home is varco_core.job.memory"
    from varco_core.job.memory import InMemoryJobStore as CoreInMemoryJobStore
    from varco_fastapi.job.store import InMemoryJobStore as ReExportedJobStore

    assert ReExportedJobStore is CoreInMemoryJobStore
