"""
varco_core.event.producer
=========================
Event producer abstractions for user-facing code.

``AbstractEventProducer`` is the only event-system type that services and
other domain classes should depend on.  It completely hides the bus —
callers never see ``AbstractEventBus`` and never interact with channels
or subscriptions directly.

Two concrete implementations ship in this module:

``BusEventProducer``
    Wraps an injected ``AbstractEventBus``.  This is the standard
    implementation registered in the DI container.  Can be extended to add
    batching, retry logic, or correlation-ID stamping without touching any
    service code.

``NoopEventProducer``
    Silently discards all events.  Used as the default when no bus is
    configured (optional DI binding), and in unit tests where event
    side-effects are irrelevant.

Usage in services::

    class OrderService(AsyncService[Order, UUID, ...]):
        def __init__(
            self,
            uow_provider: Inject[IUoWProvider],
            authorizer:   Inject[AbstractAuthorizer],
            assembler:    Inject[AbstractDTOAssembler[...]],
            producer:     Annotated[AbstractEventProducer, InjectMeta(optional=True)] = None,
        ) -> None:
            super().__init__(
                uow_provider=uow_provider,
                authorizer=authorizer,
                assembler=assembler,
                producer=producer,
            )

        async def create(self, dto, ctx):
            ...
            await self._produce(OrderCreatedEvent(...), channel="orders")

DESIGN: AbstractEventProducer over AbstractEventBus in service code
    ✅ Services never know a "bus" exists — full decoupling.
    ✅ The producer can add cross-cutting concerns (retry, batching) without
       touching the bus or any service.
    ✅ Swapping Kafka for Redis only requires rebinding the DI container —
       zero service changes.
    ✅ ``NoopEventProducer`` makes testing trivial — no fake bus needed.
    ❌ One extra indirection layer vs. injecting the bus directly —
       justified by the decoupling benefits above.

Thread safety:  ✅ ``AbstractEventProducer`` implementations are stateless
                    singletons — safe to share across concurrent requests.
Async safety:   ✅ Both ``produce`` and ``produce_many`` are ``async def``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from providify import Inject

from varco_core.event.base import CHANNEL_DEFAULT, AbstractEventBus, Event

if TYPE_CHECKING:
    pass


# ── AbstractEventProducer ─────────────────────────────────────────────────────


class AbstractEventProducer(ABC):
    """
    Abstract base for event producers.

    The only event-system interface that domain classes (services, etc.)
    should depend on.  Subclasses call ``_produce`` / ``_produce_many``
    (protected) from their business logic.

    DESIGN: protected ``_produce`` over public ``produce``
        ✅ Signals that production is an internal behaviour of the class,
           not a public API callable by external code.
        ✅ Consistent with ``AsyncService``'s protected hook pattern
           (``_scoped_params``, ``_prepare_for_create``, etc.).
        ❌ Slightly unconventional for a method on an ABC — justified by
           the intent that only subclass methods call it.

    Thread safety:  ✅ Stateless — implementations must not hold mutable state.
    Async safety:   ✅ ``_produce`` and ``_produce_many`` are ``async def``.
    """

    @abstractmethod
    async def _produce(
        self,
        event: Event,
        *,
        channel: str = CHANNEL_DEFAULT,
    ) -> None:
        """
        Publish a single event to the given channel.

        Args:
            event:   The event to publish.
            channel: Target channel.  Defaults to ``CHANNEL_DEFAULT``.

        Raises:
            Any exception propagated from the underlying bus implementation.

        Edge cases:
            - ``NoopEventProducer`` silently discards the call — no error.
            - The channel must be a single string; multi-channel routing is
              not supported by design (see architecture docs).
        """

    @abstractmethod
    async def _produce_many(
        self,
        events: list[tuple[Event, str]],
    ) -> None:
        """
        Publish multiple ``(event, channel)`` pairs.

        The default bus implementation is sequential; Kafka/Redis overrides
        may batch for efficiency.

        Args:
            events: List of ``(event, channel)`` tuples, published in order.

        Raises:
            Any exception propagated from the underlying bus implementation.

        Edge cases:
            - An empty list is a no-op.
            - ``NoopEventProducer`` discards all events silently.
        """


# ── BusEventProducer ──────────────────────────────────────────────────────────


class BusEventProducer(AbstractEventProducer):
    """
    Standard ``AbstractEventProducer`` that delegates to an ``AbstractEventBus``.

    This is the concrete implementation registered in the DI container for
    production use.  Inject it as ``AbstractEventProducer``::

        container.bind(AbstractEventProducer, BusEventProducer)

    The bus itself is injected into ``BusEventProducer`` — user code never
    sees the bus directly.

    DESIGN: thin wrapper over the bus
        This class is intentionally thin.  Cross-cutting concerns
        (correlation-ID stamping, batching, retry) belong here — NOT in
        the bus or in individual services.  Future enhancements should
        override or extend this class, not the bus.

    Thread safety:  ✅ Stateless — ``_bus`` reference is set once at construction.
    Async safety:   ✅ Delegates to ``AbstractEventBus.publish`` which is async.

    Attributes:
        _bus: The underlying ``AbstractEventBus``.  Not part of the public API.
    """

    def __init__(self, bus: Inject[AbstractEventBus]) -> None:
        """
        Args:
            bus: The ``AbstractEventBus`` to delegate to.  Injected by the
                 DI container — user code does not supply this directly.
        """
        # Stored as a private reference — deliberately hidden from subclasses
        # that should depend on _produce/_produce_many, not the bus directly.
        self._bus = bus

    async def _produce(
        self,
        event: Event,
        *,
        channel: str = CHANNEL_DEFAULT,
    ) -> None:
        """
        Delegate to ``AbstractEventBus.publish``.

        Args:
            event:   The event to publish.
            channel: Target channel.

        Raises:
            Any exception propagated from the bus (e.g. ``ExceptionGroup``
            when ``ErrorPolicy.COLLECT_ALL`` is active and handlers failed).
        """
        await self._bus.publish(event, channel=channel)

    async def _produce_many(
        self,
        events: list[tuple[Event, str]],
    ) -> None:
        """
        Delegate to ``AbstractEventBus.publish_many``.

        Args:
            events: List of ``(event, channel)`` tuples.
        """
        await self._bus.publish_many(events)

    def __repr__(self) -> str:
        return f"BusEventProducer(bus={self._bus!r})"


# ── NoopEventProducer ─────────────────────────────────────────────────────────


class NoopEventProducer(AbstractEventProducer):
    """
    ``AbstractEventProducer`` that silently discards all events.

    Used in two scenarios:

    1. **Default when no bus is configured** — ``AsyncService`` uses this
       when the optional ``producer`` DI binding is absent, so services
       work correctly without any event infrastructure wired.

    2. **Unit tests** — inject ``NoopEventProducer()`` when the test does
       not care about events, avoiding the need to set up an
       ``InMemoryEventBus`` or assert on it.

    DESIGN: explicit Noop over ``producer: ... | None`` guards in service code
        ✅ Service code calls ``await self._produce(...)`` unconditionally —
           no ``if self._producer is not None:`` guard scattered everywhere.
        ✅ Adding events to a new operation later requires no guard changes.
        ❌ Slightly less obvious than a ``None`` check — the Null Object
           pattern is the trade-off.

    Thread safety:  ✅ Stateless — no shared mutable state.
    Async safety:   ✅ Returns immediately — no I/O.
    """

    async def _produce(
        self,
        event: Event,
        *,
        channel: str = CHANNEL_DEFAULT,
    ) -> None:
        """Discard ``event`` silently."""
        # Intentional no-op — Null Object pattern.
        # Do NOT add logging here; this runs in tight loops during testing.

    async def _produce_many(
        self,
        events: list[tuple[Event, str]],
    ) -> None:
        """Discard all ``events`` silently."""

    def __repr__(self) -> str:
        return "NoopEventProducer()"
