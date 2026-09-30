"""
varco_core.webhook
====================
Outbound webhooks (Plan 031 / D4, §D-D4-home) — subscription registry,
signing, SSRF-hardened delivery, and the dispatcher. Everything portable
lives here; ``varco_sa``/``varco_beanie`` hold repositories,
``varco_fastapi.webhook`` holds only the admin mount.

⚠️ This module (and its submodules) must stay import-cheap and free of any
hard HTTP client dependency at module scope — see
``varco_core.webhook.transport`` and
``varco_core/tests/test_webhook_no_hard_client_deps.py``.

``WebhookModule`` (Plan 050 / §D-webhook) is re-exported here via a PEP 562
``__getattr__`` rather than a plain module-scope import: ``webhook/module.py``
imports ``providify`` and ``varco_core.di`` eagerly (its ``@Provider``
closures need those names in their own module globals — see its docstring),
and this package must stay cheap to import for callers who only want
``ssrf``/``signing``/the dispatcher's non-DI surface. ``from varco_core.webhook
import WebhookModule`` and ``from varco_core.webhook.module import
WebhookModule`` resolve to the identical class either way; only the former
defers the providify import to first access.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from varco_core.webhook.module import WebhookModule

__all__ = ["WebhookModule"]


def __getattr__(name: str) -> Any:
    """Resolve ``WebhookModule`` lazily (PEP 562) — see the module docstring.

    Args:
        name: The attribute being looked up on ``varco_core.webhook``.

    Returns:
        ``varco_core.webhook.module.WebhookModule`` on first access; cached
        as a normal module global thereafter.

    Raises:
        AttributeError: If ``name`` is not ``"WebhookModule"``.
    """
    if name == "WebhookModule":
        from varco_core.webhook.module import WebhookModule

        globals()["WebhookModule"] = WebhookModule
        return WebhookModule
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
