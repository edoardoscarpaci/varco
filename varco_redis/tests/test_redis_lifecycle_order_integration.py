"""
Plan 050 / Step 14 REPAIR — end-to-end phased-lifecycle ordering over a real
Redis bus: ``[CoreModule(), RedisModule()]`` + one auto-registered
``EventConsumer`` singleton, driven through the real ``create_varco_app``
lifespan. Asserts the DI-9 invariant this plan exists for — consumers stop
before the bus — against an actual ``RedisEventBus``, not a spy, and that
``@Disposes(AbstractEventBus)``'s backstop second ``stop()`` call (run by
``VarcoLifespan``'s own ``container.ashutdown()`` shutdown hook, which always
runs last — see ``varco_core.di.lifecycle``) is a true no-op: no reconnect,
the underlying client's connection pool is disconnected exactly once.

Requires a real Redis broker (session-scoped ``redis_url`` fixture). No
chaos test — nothing here provokes container failure; every invariant is an
in-process ordering property.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from providify import DIContainer, Singleton
from varco_core.di.module import CoreModule
from varco_core.event.base import AbstractEventBus, Event
from varco_core.event.consumer import EventConsumer, listen
from varco_fastapi.app import create_varco_app
from varco_redis.modules import RedisModule

pytestmark = pytest.mark.integration


class _PingEvent(Event):
    pass


async def test_consumer_stops_before_the_bus_and_the_disposer_backstop_is_a_noop(
    monkeypatch: pytest.MonkeyPatch, redis_url: str
) -> None:
    channel = f"test-{uuid4().hex[:8]}"
    monkeypatch.setenv("VARCO_REDIS_URL", redis_url)
    # This test only needs the event bus — the package's autouse
    # `_select_redis_backends` fixture also selects redis for
    # cache/dlq/idempotency/job_store/revocation, which would make
    # `CoreModule`'s generic "core.stores" component try to start a
    # second Redis cache/store this test never asked for (and never
    # bound connection settings for). Undone here so only the bus is live.
    monkeypatch.delenv("VARCO_CACHE_BACKEND", raising=False)
    monkeypatch.delenv("VARCO_DLQ_BACKEND", raising=False)
    monkeypatch.delenv("VARCO_IDEMPOTENCY_BACKEND", raising=False)
    monkeypatch.delenv("VARCO_JOB_STORE_BACKEND", raising=False)
    monkeypatch.delenv("VARCO_REVOCATION_BACKEND", raising=False)

    calls: list[str] = []

    @Singleton
    class _SpyConsumer(EventConsumer):
        @listen(_PingEvent, channel=channel)
        async def on_ping(self, event: _PingEvent) -> None:
            return None

    container = DIContainer()
    container.register(_SpyConsumer)

    app = create_varco_app(
        container,
        modules=[CoreModule(), RedisModule()],
        validate=False,
    )

    async with app.router.lifespan_context(app):
        bus = container.get(AbstractEventBus)
        consumer = container.get(_SpyConsumer)
        assert consumer.is_registered_to(bus) is True

        real_bus_stop = type(bus).stop
        real_consumer_stop = type(consumer).stop

        async def _wrapped_bus_stop(self: object) -> None:
            calls.append("bus.stop")
            await real_bus_stop(self)

        async def _wrapped_consumer_stop(self: object, *args: object, **kwargs: object) -> None:
            calls.append("consumer.stop")
            await real_consumer_stop(self, *args, **kwargs)

        monkeypatch.setattr(type(bus), "stop", _wrapped_bus_stop)
        monkeypatch.setattr(type(consumer), "stop", _wrapped_consumer_stop)

        # Spy on the underlying client's connection-pool teardown so we can
        # prove it runs exactly once even though `bus.stop()` itself is
        # called twice (the module's own stop, then `ashutdown()`'s
        # `@Disposes(AbstractEventBus)` backstop) — both happen while this
        # `async with` block unwinds, since `VarcoLifespan`'s shutdown hook
        # (`container.ashutdown()`) always runs last, inside the same exit.
        real_redis = bus._redis  # type: ignore[attr-defined]
        disconnect_calls = {"count": 0}
        if real_redis is not None:
            real_disconnect = real_redis.connection_pool.disconnect

            async def _counting_disconnect(*args: object, **kwargs: object) -> None:
                disconnect_calls["count"] += 1
                await real_disconnect(*args, **kwargs)

            monkeypatch.setattr(real_redis.connection_pool, "disconnect", _counting_disconnect)

    # Exact reverse of what started — the consumer (CONSUMERS phase) stops
    # before the bus (BUS phase); the bus then appears a SECOND time because
    # `ashutdown()`'s `@Disposes(AbstractEventBus)` backstop runs after every
    # module stop, unconditionally, as documented in `varco_core.di.lifecycle`.
    assert calls == ["consumer.stop", "bus.stop", "bus.stop"]

    # `RedisEventBus.stop()` guards on `self._started` — the second call is a
    # true no-op, so the connection pool is disconnected exactly once.
    assert disconnect_calls["count"] == 1
