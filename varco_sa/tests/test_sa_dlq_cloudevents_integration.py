"""
Plan 050 / Step 14 REPAIR — end-to-end CloudEvents wiring over a real
Postgres DLQ: ``bind_cloudevents_serializer()`` → ``SAConfiguration.dead_letter_queue``
→ ``push()`` writes a CloudEvents envelope → ``DlqRedriver`` republishes the
original ``Event``.

Requires a real Postgres broker (session-scoped ``postgres_container``
fixture). Every test gets its own fresh, throwaway database
(``create_isolated_database_url`` with a ``uuid4().hex[:8]`` name) — the DLQ
table is a fixed, shared name (``varco_dead_letters``) with no per-row
namespace column this test could filter on, so the CLAUDE.md per-test
namespacing rule is satisfied by isolating the *database* rather than a row
key (the same pattern ``varco_sa``'s own migration-integration tests use —
see ``create_isolated_database_url``'s own docstring).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from varco_core.event.base import AbstractEventBus, Event
from varco_core.event.cloudevents import CloudEventsSettings, bind_cloudevents_serializer
from varco_core.event.dlq import AbstractDeadLetterQueue, DeadLetterEntry
from varco_core.event.memory import InMemoryEventBus
from varco_core.event.redrive import DlqRedriver

pytestmark = pytest.mark.integration


class _OrderPlaced(Event):
    order_id: str


async def test_sa_dlq_stores_and_redrives_a_cloudevents_envelope(
    postgres_container: object,
) -> None:
    from providify import DIContainer
    from varco_core.di import bind_instance
    from varco_sa.config import SAConfig
    from varco_sa.di import bootstrap
    from varco_sa.dlq import SADeadLetterQueue

    from tests.conftest import create_isolated_database_url

    db_name = f"varco_test_dlq_ce_{uuid4().hex[:8]}"
    url = await create_isolated_database_url(postgres_container, db_name)

    engine = create_async_engine(url)
    try:
        from sqlalchemy.orm import DeclarativeBase

        class _Base(DeclarativeBase):
            pass

        container = DIContainer()
        bootstrap(container)
        bind_instance(container, SAConfig, SAConfig(engine=engine, base=_Base, entity_classes=()))
        bind_cloudevents_serializer(container, CloudEventsSettings(source="/varco/tests"))

        dlq = container.get(AbstractDeadLetterQueue)
        assert isinstance(dlq, SADeadLetterQueue)
        await dlq.ensure_table()

        event = _OrderPlaced(order_id="ord-1")
        entry = DeadLetterEntry(
            event=event,
            channel="orders",
            handler_name="OrderConsumer.on_order_placed",
            error_type="ValueError",
            error_message="boom",
            attempts=3,
            first_failed_at=datetime.now(UTC),
            last_failed_at=datetime.now(UTC),
        )
        await dlq.push(entry)

        assert await dlq.count() == 1

        # Raw read — assert the envelope on the wire is CloudEvents, not
        # varco's plain JSON (the whole point of the wiring this guards).
        async with engine.connect() as conn:
            result = await conn.execute(sa.text("SELECT payload FROM varco_dead_letters LIMIT 1"))
            raw = result.scalar_one()
        payload = json.loads(raw if isinstance(raw, str) else bytes(raw).decode("utf-8"))
        assert payload["specversion"] == "1.0"

        # Redrive: republish onto an InMemoryEventBus using the SAME bound
        # serializer — the handler must receive the original Event back.
        bus = InMemoryEventBus()
        bind_instance(container, AbstractEventBus, bus)
        received: list[_OrderPlaced] = []

        async def _handler(evt: _OrderPlaced) -> None:
            received.append(evt)

        bus.subscribe(_OrderPlaced, _handler, channel="orders")

        entries = await dlq.list_entries(limit=10)
        assert len(entries) == 1
        redriver = DlqRedriver(dlq, bus)
        outcome = await redriver.redrive(entries[0].entry_id)
        assert outcome.published is True
        await bus.drain()

        assert len(received) == 1
        assert received[0].order_id == "ord-1"
    finally:
        await engine.dispose()
