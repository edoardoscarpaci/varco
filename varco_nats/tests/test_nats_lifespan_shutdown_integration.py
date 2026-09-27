"""
Plan 022 / Phase 4 (RL-8a), Step 25 — the orphan is *actually* torn down.

Steps 22–23 proved with unit tests that ``VarcoLifespan`` awaits its new
``shutdown=`` hook.  That is not the claim RL-8a makes.  The claim is that a
``@PreDestroy``-bearing singleton which is **not** a registered lifecycle
component — and which therefore leaked before this plan — now really releases
its resources.  Asserting "a hook was called" would restate Step 22; this file
asserts the *effect*, against a real NATS container.

``NatsStreamManager`` (``varco_nats/channel.py:198``, ``@Singleton`` bound to
``ChannelManager``) is orphan #4 of the six measured in
``design/api-freeze-and-standards/measurements/predestroy-vs-lifespan.md``:
``create_varco_app()`` resolves only ``AbstractEventBus`` / ``AbstractJobRunner``
/ the two ``varco_ws`` buses as lifecycle components, never a ``ChannelManager``.
It is the strongest available proof because its ``@PostConstruct start()`` opens
a **real NATS connection** and its ``@PreDestroy stop()`` closes it — so
"released" is observable on the client socket, not merely on a private flag.

⚠️ **Why not ``RedisCache``, the measurement's "worst case".**  It is bound by
``RedisCacheConfiguration.redis_cache()``, a ``@Provider`` — and providify's
teardown runs ``@Disposes`` disposers for a ``ProviderBinding`` and only reaches
``@PreDestroy`` for a ``ClassBinding`` (``providify/container.py:4567-4576``).
So the ``RedisCache``/``MemcachedCache`` orphans are **not** fixed by this
adoption.  That is an upstream gap, filed as UPSTREAM-GAPS.md U-21 and pinned by
``varco_redis/tests/test_redis_cache_lifespan_shutdown_integration.py``'s strict
xfail — it is deliberately not worked around in varco code.

⚠️ Cross-package direction.  ``varco_nats`` does not (and must not) depend on
``varco_fastapi``; the import below is guarded by ``importorskip`` and the test
lives here only because the session-scoped ``nats_url`` fixture and the real
container do (CLAUDE.md's Test Conventions).  Nothing in ``varco_nats``'s
shipped code is involved in that import.

Per-test namespacing: the shared session container means this test confines
itself to a ``uuid4().hex[:8]`` stream name it owns exclusively.

Thread safety:  N/A (integration test)
Async safety:   ✅ the lifespan is driven as an async context manager.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration


async def test_container_shutdown_closes_orphaned_stream_manager_connection(
    nats_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A started-but-unregistered ``NatsStreamManager`` has its socket closed.

    Edge cases covered here on purpose:
        - The manager is an *orphan* by construction: the ``VarcoLifespan``
          built below is given no positional components at all (only the
          ``shutdown=`` hook), so the test cannot silently degrade into "a
          registered component was stopped". (Plan 049 / DI-1 deleted
          ``_collect_lifecycle_components`` — see
          ``test_redis_cache_lifespan_shutdown_integration.py``'s identical
          note for the full reasoning.)
        - The assertion is on the live ``nats.aio.client.Client``, not on
          ``NatsStreamManager._nc`` alone: a hook that nulled the attribute but
          leaked the connection would still pass the weaker check.
    """
    providify = pytest.importorskip("providify")
    pytest.importorskip("varco_fastapi")

    from varco_core.event.channel import ChannelManager
    from varco_fastapi.lifespan import VarcoLifespan

    run_id = uuid4().hex[:8]
    monkeypatch.setenv("VARCO_NATS_ADMIN_SERVERS", nats_url)
    monkeypatch.setenv("VARCO_NATS_ADMIN_STREAM_NAME", f"test_{run_id}")

    container = providify.DIContainer()
    container.scan("varco_nats", recursive=True)

    manager: Any = await container.aget(ChannelManager)
    client = manager._nc
    assert client is not None and client.is_connected

    # Pre-condition: this manager is exactly the orphan RL-8a is about —
    # see the docstring's Edge cases note above.
    async def _shutdown() -> None:
        await container.ashutdown()

    async with VarcoLifespan(shutdown=_shutdown)(object()):
        pass

    assert manager._nc is None
    assert not client.is_connected
