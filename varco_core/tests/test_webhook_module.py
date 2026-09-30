"""
Plan 050 / Step 11 (§D-webhook, §D-wire rows 4-5) — `WebhookModule`, the
explicit home for `WebhookDispatcher`, DI-wired against a
`WebhookSubscriptionRepository`/`WebhookSettings`/optional `AbstractDeadLetterQueue`.
Docker-free.

RED MODE: `varco_core.webhook.module` does not exist yet.
"""

from __future__ import annotations

from providify import DIContainer
from varco_core.di.module import CoreModule

# Expected to fail today: no such module.
from varco_core.webhook.module import WebhookModule  # type: ignore[import-not-found]


async def test_webhook_module_registers_fallback_repository_and_resolves_dispatcher() -> None:
    from varco_core.webhook.base import WebhookSubscriptionRepository
    from varco_core.webhook.dispatcher import WebhookDispatcher
    from varco_core.webhook.settings import WebhookSettings

    container = DIContainer()
    core = CoreModule()
    webhook = WebhookModule()
    core.register(container)
    webhook.register(container)

    dispatcher = container.get(WebhookDispatcher)
    assert dispatcher._repository is container.get(WebhookSubscriptionRepository)  # type: ignore[attr-defined]
    assert dispatcher._settings is container.get(WebhookSettings)  # type: ignore[attr-defined]


async def test_webhook_module_dlq_binding_reaches_the_dispatcher() -> None:
    from varco_core.di import bind_instance
    from varco_core.event.dlq import AbstractDeadLetterQueue
    from varco_core.webhook.dispatcher import WebhookDispatcher

    class _FakeDLQ(AbstractDeadLetterQueue):
        # NOTE (Plan 050 repair): AbstractDeadLetterQueue's real abstract
        # surface is push/pop_batch/ack/count (varco_core/event/dlq.py) —
        # the plan's sketch named list_entries/delete, which do not exist
        # on the ABC. Fixed here per the implementation-agent contract
        # ("follow the plan, fix the test").
        async def push(self, entry: object) -> None:
            return None

        async def pop_batch(self, *, limit: int = 10) -> list[object]:
            return []

        async def ack(self, entry_id: object) -> None:
            return None

        async def count(self) -> int:
            return 0

    container = DIContainer()
    CoreModule().register(container)
    dlq = _FakeDLQ()
    bind_instance(container, AbstractDeadLetterQueue, dlq)
    WebhookModule().register(container)

    dispatcher = container.get(WebhookDispatcher)
    assert dispatcher._dlq is dlq  # type: ignore[attr-defined]


def test_webhook_module_has_infra_and_consumers_components() -> None:
    from varco_core.di.lifecycle import Phase  # type: ignore[import-not-found]

    container = DIContainer()
    module = WebhookModule()
    module.register(container)
    components = module.components(container)  # type: ignore[attr-defined]
    names_phases = {c.name: c.phase for c in components}
    assert names_phases.get("webhook.repository") == Phase.INFRA
    assert names_phases.get("webhook.dispatcher") == Phase.CONSUMERS
