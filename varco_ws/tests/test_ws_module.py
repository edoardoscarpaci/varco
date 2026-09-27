"""
Plan 049 / Step 8 — `varco_ws.modules.WsModule`, Docker-free.

RED MODE: `varco_ws.modules` does not exist yet — every test below is
expected to fail with `ImportError`.
"""

from __future__ import annotations

from providify import DIContainer
from varco_core.di.module import CoreModule
from varco_ws.modules import WsModule


class TestWsModule:
    async def test_register_does_not_raise_on_bare_container(self) -> None:
        container = DIContainer()
        m = WsModule()
        m.register(container)

    async def test_start_starts_websocket_and_sse_adapters_if_resolvable(self) -> None:
        # BUGFIX (test, not plan): WebSocketEventBus/SSEEventBus both inject
        # AbstractEventBus — `is_resolvable(WebSocketEventBus)` is True as
        # soon as varco_ws's own bootstrap() registers the class, but
        # actually constructing one needs AbstractEventBus already bound,
        # which is CoreModule's job (WsModule.depends_on names it).
        # Registering CoreModule first is what "if resolvable" actually
        # means at the seam WsModule.start() resolves against.

        container = DIContainer()
        CoreModule().register(container)
        m = WsModule()
        m.register(container)

        # §D-core: WsModule.start() does the same thing CoreModule.start()
        # does for the two core seams — starts the resolved
        # WebSocketEventBus/SSEEventBus if resolvable, no-op otherwise.
        await m.start(container)  # must not raise even if unresolvable
        await m.stop(container)

    async def test_depends_on_core_module(self) -> None:
        from varco_core.di.module import CoreModule

        assert CoreModule in WsModule.depends_on

    async def test_name_is_ws(self) -> None:
        assert WsModule.name == "ws"
