"""
Plan 038 (S19) / Step 7 — ``varco_core.webhook.inbound`` must register no
providify ``@Singleton``/``@Provider``/``@Configuration`` at import time.

Mirrors the equivalent cloudevents/tls guards (CLAUDE.md's standing rule).
Fails with ``ModuleNotFoundError`` until Phase 2 lands.
"""

from __future__ import annotations

import importlib
import pkgutil


def _iter_inbound_submodules():
    package = importlib.import_module("varco_core.webhook.inbound")
    yield package
    for module_info in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
        yield importlib.import_module(module_info.name)


def test_no_singleton_provider_or_configuration_markers() -> None:
    for module in _iter_inbound_submodules():
        for name in dir(module):
            obj = getattr(module, name)
            for marker in (
                "__providify_singleton__",
                "__providify_provider__",
                "__providify_configuration__",
            ):
                assert not getattr(obj, marker, False), (
                    f"{module.__name__}.{name} carries {marker} — inbound webhook "
                    "verification must never auto-register via container.scan()"
                )


async def test_scanning_varco_core_gains_no_inbound_binding() -> None:
    from providify import DIContainer

    container = DIContainer()
    container.scan("varco_core", recursive=True)

    from varco_core.webhook.inbound.base import WebhookVerifier

    with __import__("pytest").raises(Exception):
        container.get(WebhookVerifier)
