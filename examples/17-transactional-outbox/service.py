"""
service.py
==========
Business logic for the order entity with transactional outbox integration.

``OrderService`` overrides ``create()`` to write both the ``Order`` row and
an ``OutboxEntry`` within a single DB transaction.  The ``OutboxRelay``
background task reads pending outbox entries and publishes them to the event
bus — decoupling event delivery from the HTTP request lifecycle.

DESIGN: override create() for outbox write — not a service hook
    ``AsyncService._prepare_for_create()`` runs before ``repo.save()``, but
    the outbox entry needs the ``pk`` from the saved entity.  Overriding the
    full ``create()`` method and calling ``repo.save()`` first, then writing
    the outbox entry in the same session, is the cleanest pattern.

    ✅ Both rows share the same ``AsyncSession`` — commit is atomic.
    ✅ If the broker is down, the outbox entry stays in the DB; the relay
       retries on next poll.
    ✅ ``OutboxRelay`` is the ONLY consumer of ``AbstractEventBus`` outside
       of ``EventConsumer.register_to()`` — the service never holds the bus.
    ❌ Manual ``create()`` override duplicates some ``AsyncService`` boilerplate.
       In production, extract a ``TransactionalOutboxMixin``.

DESIGN: SAOutboxRepository injected from the service
    The ``SAOutboxRepository`` is session-scoped — it must use the SAME
    ``AsyncSession`` as the ``SQLAlchemyUnitOfWork`` to participate in the
    same transaction.  The session is accessed via ``uow.session``.

    ✅ Single DB roundtrip: order + outbox entry committed together.
    ✅ No second UoW needed for the outbox — same session.
    ❌ Couples the service to the SA implementation detail (``uow.session``).
       In production, expose an ``OutboxRepository`` from the UoW directly.

Thread safety:  ⚠️ Singleton service — each call opens its own UoW.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from dtos import OrderCreate, OrderRead, OrderUpdate
from events import OrderCreatedEvent
from models import Order
from providify import Inject, Singleton
from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth.base import AbstractAuthorizer
from varco_core.service.base import _ANON_CTX, AsyncService, IUoWProvider
from varco_core.service.outbox import OutboxEntry

if TYPE_CHECKING:
    from varco_core.auth import AuthContext
    from varco_core.uow import AsyncUnitOfWork


@Singleton
class OrderService(AsyncService[Order, UUID, OrderCreate, OrderRead, OrderUpdate]):
    """
    CRUD service for ``Order`` entities with transactional outbox integration.

    Overrides ``create()`` to save an ``OutboxEntry`` alongside the ``Order``
    row in the same DB transaction.  All other CRUD operations (read, update,
    delete, list) delegate to the inherited ``AsyncService`` implementation.

    Thread safety:  ⚠️ Singleton — each call opens its own UoW; no shared state.
    Async safety:   ✅ All public methods are ``async def``.
    """

    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer: Inject[AbstractAuthorizer],
        assembler: Inject[AbstractDTOAssembler[Order, OrderCreate, OrderRead, OrderUpdate]],
    ) -> None:
        """
        Args:
            uow_provider: Injected ``IUoWProvider`` — provided by ``SAConfiguration``.
            authorizer:   Injected ``AbstractAuthorizer`` — ``BaseAuthorizer``
                          (permissive) for this example.
            assembler:    Injected ``OrderAssembler`` — DTO ↔ Order mapping.
        """
        super().__init__(
            uow_provider=uow_provider,
            authorizer=authorizer,
            assembler=assembler,
        )

    def _get_repo(self, uow: AsyncUnitOfWork):
        """
        Return the ``AsyncRepository[Order]`` from the open unit-of-work.

        Args:
            uow: The open ``AsyncUnitOfWork`` for this request.

        Returns:
            ``AsyncRepository[Order]`` backed by the current SA session.
        """
        return uow.orders  # type: ignore[attr-defined]

    async def create(self, dto: OrderCreate, ctx: AuthContext = _ANON_CTX) -> OrderRead:
        """
        Create a new order and save an ``OutboxEntry`` in the same DB transaction.

        Overrides the base ``create()`` to write the outbox entry atomically
        with the domain entity.  The ``OutboxRelay`` background task picks up
        the entry and publishes ``OrderCreatedEvent`` to the bus.

        Steps:
        1. Open a ``SQLAlchemyUnitOfWork``.
        2. Assemble ``Order`` from ``dto`` via the injected assembler.
        3. Save ``Order`` (generates ``pk`` on INSERT).
        4. Build ``OutboxEntry.from_event(OrderCreatedEvent(...))`` using the
           saved ``pk``.
        5. Save the outbox entry via ``SAOutboxRepository`` using the SAME
           ``AsyncSession`` — both rows share the same transaction.
        6. The UoW context manager auto-commits on clean exit.

        Args:
            dto: ``OrderCreate`` payload from the HTTP layer.
            ctx: Caller's identity and grants.

        Returns:
            ``OrderRead`` DTO of the persisted order.

        Raises:
            ServiceAuthorizationError: Authorization check fails (unlikely —
                                       ``BaseAuthorizer`` is permissive).

        Edge cases:
            - If ``repo.save()`` raises (DB error), the UoW rolls back and
              NO outbox entry is written — both rows are atomic.
            - If the relay publishes the event but crashes before deleting the
              outbox entry, the entry will be replayed on the next relay tick.
              Consumers should use ``SADeduplicator`` to handle duplicates.
        """
        from varco_sa.outbox import SAOutboxRepository  # noqa: PLC0415

        async with self._uow_provider.make_uow() as uow:
            entity = self._assembler.to_domain(dto)
            saved = await self._get_repo(uow).save(entity)

            # Build the outbox entry using the assigned pk from the saved entity.
            event = OrderCreatedEvent(order_id=str(saved.pk), amount=saved.amount)
            entry = OutboxEntry.from_event(event, channel="orders")

            # Write the outbox entry using the SAME session as the order row.
            # uow.session is the live AsyncSession; SAOutboxRepository uses it
            # directly so both writes share the same transaction boundary.
            outbox_repo = SAOutboxRepository(uow.session)  # type: ignore[arg-type]
            await outbox_repo.save(entry)

            return self._assembler.to_read_dto(saved)


__all__ = ["OrderService"]
