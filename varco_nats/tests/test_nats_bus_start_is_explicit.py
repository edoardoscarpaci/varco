"""
Plan 050 / Step 6 — `NatsEventBus.start()` is no longer `@PostConstruct`;
publish() before start() raises a clear RuntimeError (§D-bus). Docker-free.

RED MODE: the `@PostConstruct` marker is still present today — this test is
expected to fail.
"""

from __future__ import annotations

import pytest
from providify.decorator.lifecycle import _find_post_construct
from varco_nats.bus import NatsEventBus
from varco_nats.config import NatsEventBusSettings


def _bus() -> NatsEventBus:
    return NatsEventBus(NatsEventBusSettings(servers="nats://localhost:1"))


def test_start_has_no_postconstruct_marker() -> None:
    # §D-bus: the BUS phase is now the one starter, not first-resolution.
    assert _find_post_construct(NatsEventBus) is None


async def test_publish_before_start_raises_runtime_error_mentioning_start() -> None:
    bus = _bus()
    with pytest.raises(RuntimeError, match="start"):
        await bus.publish(object())  # type: ignore[arg-type]
