"""
Plan 024 / Phase 1, Step 18 — Tier B `@Disposes` teardown for
`RedisStreamDLQConfiguration` (binds `AbstractDeadLetterQueue`).

RED MODE: Tier B — `RedisStreamDLQ` carries no `@PreDestroy`, so providify's
`UNREACHABLE_PRE_DESTROY` detector cannot see this leak. Expected failure:
`ashutdown()` never calls `RedisStreamDLQ.disconnect()` for a
provider-produced instance, because no `@Disposes(AbstractDeadLetterQueue)`
exists on `RedisStreamDLQConfiguration` yet.

No Docker required — Redis I/O is monkeypatched to no-ops.
"""

from __future__ import annotations

from typing import Any

from providify import DIContainer
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_redis.stream_dlq import RedisStreamDLQ, RedisStreamDLQConfiguration


async def _noop_connect(self: Any) -> None:
    self._redis = object()


async def test_redis_stream_dlq_disconnect_runs_on_ashutdown(monkeypatch) -> None:
    # Plan 047 §D-tests: "redis-streams" is a secondary DLQ value — the
    # package conftest's autouse fixture selects "redis" as the primary.
    monkeypatch.setenv("VARCO_DLQ_BACKEND", "redis-streams")
    monkeypatch.setattr(RedisStreamDLQ, "connect", _noop_connect)
    calls: list[str] = []

    async def _record_disconnect(self: Any) -> None:
        calls.append("disconnected")
        self._redis = None

    monkeypatch.setattr(RedisStreamDLQ, "disconnect", _record_disconnect)

    container = DIContainer()
    await container.ainstall(RedisStreamDLQConfiguration)
    await container.aget(AbstractDeadLetterQueue)
    await container.ashutdown()

    assert calls == ["disconnected"]
