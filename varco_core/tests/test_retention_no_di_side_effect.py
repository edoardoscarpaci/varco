"""
tests.test_retention_no_di_side_effect
=========================================
Plan 039 (S20) / Step 6 — ``varco_core.retention`` must register no
providify ``@Singleton``/``@Provider``/``@Configuration`` at import time,
and ``container.scan("varco_core", recursive=True)`` must gain no binding
for any retention name and start no task.

Mirrors the equivalent cloudevents/tls/webhook-inbound guards (CLAUDE.md's
standing rule) — here the blast radius of getting this wrong is data loss,
not wire bytes.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest


def _iter_retention_submodules():
    package = importlib.import_module("varco_core.retention")
    yield package
    for module_info in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
        yield importlib.import_module(module_info.name)


def test_no_singleton_provider_or_configuration_markers() -> None:
    for module in _iter_retention_submodules():
        for name in dir(module):
            obj = getattr(module, name)
            for marker in (
                "__providify_singleton__",
                "__providify_provider__",
                "__providify_configuration__",
            ):
                assert not getattr(obj, marker, False), (
                    f"{module.__name__}.{name} carries {marker} — a scanned "
                    "@Configuration in varco_core.retention would start a "
                    "deletion loop in every app that scans varco_core"
                )


async def test_scanning_varco_core_gains_no_retention_binding() -> None:
    from providify import DIContainer

    container = DIContainer()
    container.scan("varco_core", recursive=True)

    from varco_core.retention.policy import RetentionRegistry

    with pytest.raises(Exception):
        container.get(RetentionRegistry)


async def test_scanning_varco_core_starts_no_retention_task() -> None:
    """Importing/scanning varco_core alone must never spawn a background
    sweep task — only an explicit RetentionScheduler(interval>0).start()
    (or RetentionLifecycle) may do that."""
    import asyncio

    from providify import DIContainer

    tasks_before = {t for t in asyncio.all_tasks() if not t.done()}

    container = DIContainer()
    container.scan("varco_core", recursive=True)

    tasks_after = {t for t in asyncio.all_tasks() if not t.done()}
    new_tasks = tasks_after - tasks_before
    retention_tasks = [t for t in new_tasks if "retention" in (t.get_name() or "").lower()]
    assert retention_tasks == []
