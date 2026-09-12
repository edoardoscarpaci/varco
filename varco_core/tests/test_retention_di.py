"""
tests.test_retention_di
==========================
Plan 039 (S20) / Step 16 — ``bind_retention_registry(container, registry)``
(``varco_core.retention.di``), following the ``bind_trust_store`` precedent
(§D-S20-verb) — an already-constructed, already-owned object, no lifecycle
side effect.

RED until ``varco_core/varco_core/retention/di.py`` (Step 17) lands.
"""

from __future__ import annotations

from providify import DIContainer


def _registry():
    from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415

    return RetentionRegistry()


class TestBindRetentionRegistry:
    def test_makes_registry_resolvable(self) -> None:
        from varco_core.retention.di import bind_retention_registry  # noqa: PLC0415
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415

        container = DIContainer()
        registry = _registry()
        bind_retention_registry(container, registry)

        resolved = container.get(RetentionRegistry)
        assert resolved is registry

    async def test_starts_nothing(self) -> None:
        """Binding must have no lifecycle side effect — no background task
        spawned just by binding a registry."""
        import asyncio

        from varco_core.retention.di import bind_retention_registry  # noqa: PLC0415

        tasks_before = {t for t in asyncio.all_tasks() if not t.done()}
        container = DIContainer()
        bind_retention_registry(container, _registry())
        tasks_after = {t for t in asyncio.all_tasks() if not t.done()}
        assert tasks_after == tasks_before

    def test_calling_twice_replaces_the_binding(self) -> None:
        from varco_core.retention.di import bind_retention_registry  # noqa: PLC0415
        from varco_core.retention.policy import RetentionRegistry  # noqa: PLC0415

        container = DIContainer()
        first = _registry()
        second = _registry()
        bind_retention_registry(container, first)
        bind_retention_registry(container, second)

        resolved = container.get(RetentionRegistry)
        assert resolved is second

    def test_never_touches_di_container_current(self) -> None:
        from varco_core.retention.di import bind_retention_registry  # noqa: PLC0415

        before = None
        try:
            before = DIContainer.current()
        except Exception:
            before = None

        container = DIContainer()
        bind_retention_registry(container, _registry())

        after = None
        try:
            after = DIContainer.current()
        except Exception:
            after = None

        assert before is after
