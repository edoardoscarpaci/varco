"""
varco_fastapi.jwks
====================
``JwksRefreshLifecycle`` — the FastAPI-side startup/shutdown wiring for
``TrustedIssuerRegistry.start_refresh()``/``stop_refresh()`` (Plan 041 / S22,
§D-S22-lifecycle).

Imports only ``varco_core.authority`` (a core seam) — never a backend, same
rule as every other ``varco_fastapi`` lifecycle component
(``ReliabilityLifecycle`` imports only ``varco_core.reliability``).

DESIGN: a thin wrapper, no loop logic of its own (§D-S22-seam)
    The refresh loop lives entirely on ``TrustedIssuerRegistry`` in
    ``varco_core`` — a ``varco_core``-only app (a worker, a CLI, a consumer)
    needs the refresher just as much as a FastAPI app, and putting the loop
    here would deny it to them. This class only calls
    ``registry.start_refresh()``/``stop_refresh()`` at the right point in the
    ASGI lifespan.
    ✅ Byte-for-byte the ``ReliabilityLifecycle`` shape: ``startup``/
       ``shutdown`` plus ``start``/``stop`` aliases (``VarcoLifespan`` drives
       every lifecycle component through ``AbstractLifecycle``'s
       ``start``/``stop`` names).
    ✅ ``start()`` never calls ``registry.load_all()`` — a permanently-down
       JWKS endpoint at boot must not fail startup (§D-S22-failure). The
       initial load remains the app's own explicit
       ``await registry.load_all()``.
    ❌ A second way to start the refresher exists (this lifecycle, or calling
       ``registry.start_refresh()`` directly). Accepted: the registry stays
       usable standalone, exactly as ``OutboxRelay`` is.

Thread safety:  ⚠️ Not thread-safe — construct/use from the app's own
                    startup/shutdown lifespan hooks, single-threaded.
Async safety:   ✅ ``startup()``/``shutdown()`` are ``async def``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_core.authority.registry import TrustedIssuerRegistry

__all__ = ["JwksRefreshLifecycle"]


class JwksRefreshLifecycle:
    """
    Drives a ``TrustedIssuerRegistry``'s background JWKS refresh task.

    Args:
        registry: The ``TrustedIssuerRegistry`` whose ``start_refresh()``/
                   ``stop_refresh()`` this lifecycle calls.
        interval: Forwarded verbatim to ``registry.start_refresh(interval=...)``.
                   ``None`` (default) resolves to the registry's own
                   ``ttl_seconds`` (§D-S22-interval) — a period ``<= 0``
                   (the ``ttl_seconds=0.0`` default) starts no task at all.

    Edge cases:
        - ``interval=None`` with the registry's ``ttl_seconds=0.0`` →
          ``startup()`` is a no-op; ``registry.refresh_running`` stays
          ``False``.
        - The registry's JWKS endpoint is permanently down → ``startup()``
          still succeeds; only ticks fail (logged, never raised).
    """

    def __init__(self, registry: TrustedIssuerRegistry, *, interval: float | None = None) -> None:
        self._registry = registry
        self._interval = interval

    async def startup(self) -> None:
        """
        Start the registry's background refresher.

        Deliberately does **not** call ``registry.load_all()`` — a
        permanently-down source must not prevent startup (§D-S22-failure).
        """
        await self._registry.start_refresh(interval=self._interval)

    async def shutdown(self) -> None:
        """Stop the registry's background refresher, if running. Idempotent."""
        await self._registry.stop_refresh()

    # ``start``/``stop`` aliases — same rationale as
    # ``ReliabilityLifecycle.start``/``.stop``: ``VarcoLifespan`` drives every
    # lifecycle component through ``AbstractLifecycle``'s ``start``/``stop``
    # names, while ``startup``/``shutdown`` is this class's own documented API.
    async def start(self) -> None:
        await self.startup()

    async def stop(self) -> None:
        await self.shutdown()
