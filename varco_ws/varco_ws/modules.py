"""
varco_ws.modules
===================
``WsModule`` — ``varco_ws``'s one ``VarcoModule`` (Plan 049 / DI-1, Step 8).

``start()``/``stop()`` do for ``WebSocketEventBus``/``SSEEventBus`` exactly
what ``CoreModule`` does for ``AbstractEventBus``/``AbstractJobRunner``
(§D-core) — absorbs the two ``_try_resolve_component(..., warn_if_missing=
False)`` calls ``_collect_lifecycle_components`` used to make for the
``varco_ws`` push adapters.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di.module import CoreModule, VarcoModule

if TYPE_CHECKING:
    from providify import DIContainer


class WsModule(VarcoModule):
    """Registers ``varco_ws``'s ``@Fallback`` defaults and starts/stops the
    two push adapters (``WebSocketEventBus``/``SSEEventBus``) if resolvable."""

    name = "ws"
    depends_on = (CoreModule,)

    def _register(self, container: DIContainer) -> None:
        from varco_ws.di import bootstrap  # noqa: PLC0415

        bootstrap(container)

    async def start(self, container: DIContainer) -> None:
        from varco_ws.sse import SSEEventBus  # noqa: PLC0415
        from varco_ws.websocket import WebSocketEventBus  # noqa: PLC0415

        if container.is_resolvable(WebSocketEventBus):
            ws_bus: WebSocketEventBus = await container.aget(WebSocketEventBus)
            await ws_bus.start()
        if container.is_resolvable(SSEEventBus):
            sse_bus: SSEEventBus = await container.aget(SSEEventBus)
            await sse_bus.start()

    async def stop(self, container: DIContainer) -> None:
        from varco_ws.sse import SSEEventBus  # noqa: PLC0415
        from varco_ws.websocket import WebSocketEventBus  # noqa: PLC0415

        if container.is_resolvable(SSEEventBus):
            sse_bus: SSEEventBus = await container.aget(SSEEventBus)
            await sse_bus.stop()
        if container.is_resolvable(WebSocketEventBus):
            ws_bus: WebSocketEventBus = await container.aget(WebSocketEventBus)
            await ws_bus.stop()


__all__ = ["WsModule"]
