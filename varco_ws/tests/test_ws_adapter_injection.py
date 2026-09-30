"""
Plan 050 / Step 13 (§D-wire row 7-8) — ``bind_websocket_adapter``/
``bind_sse_adapter``'s ``bus: Inject[AbstractEventBus]`` declared injection.

Before the module-level ``Inject``/``AbstractEventBus`` imports landed
(§D-asks P31 — see ``varco_ws/di.py``'s module docstring DESIGN note), a
missing ``AbstractEventBus`` binding surfaced only as a ``LookupError`` at
first ``container.get(WebSocketEventBus)`` — invisible to
``container.validate()``. Docker-free — no real bus construction needed
(``InMemoryEventBus`` suffices).
"""

from __future__ import annotations

from providify import DIContainer, IssueKind


async def test_missing_bus_is_a_validate_missing_binding_not_a_lookup_error() -> None:
    from varco_ws.di import bind_websocket_adapter

    container = DIContainer()
    bind_websocket_adapter(container)

    report = container.validate(raise_on_error=False)
    assert any(
        issue.kind is IssueKind.MISSING_BINDING and issue.requested == "AbstractEventBus"
        for issue in report.errors
    ), [i.message for i in report.errors]


async def test_websocket_adapter_resolves_the_bound_bus() -> None:
    from varco_core.di.module import CoreModule
    from varco_core.event.base import AbstractEventBus
    from varco_ws.di import bind_websocket_adapter
    from varco_ws.websocket import WebSocketEventBus

    container = DIContainer()
    CoreModule().register(container)
    bind_websocket_adapter(container)

    adapter = container.get(WebSocketEventBus)
    assert adapter._bus is container.get(AbstractEventBus)  # type: ignore[attr-defined]


async def test_sse_adapter_resolves_the_bound_bus() -> None:
    from varco_core.di.module import CoreModule
    from varco_core.event.base import AbstractEventBus
    from varco_ws.di import bind_sse_adapter
    from varco_ws.sse import SSEEventBus

    container = DIContainer()
    CoreModule().register(container)
    bind_sse_adapter(container)

    adapter = container.get(SSEEventBus)
    assert adapter._bus is container.get(AbstractEventBus)  # type: ignore[attr-defined]
