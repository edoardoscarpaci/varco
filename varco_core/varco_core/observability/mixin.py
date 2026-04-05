"""
varco_core.observability.mixin
===============================
``TracingServiceMixin`` — automatic OTel span instrumentation for
``AsyncService`` subclasses.

Like ``TenantAwareService``, ``SoftDeleteService``, and ``CacheServiceMixin``,
this mixin is composed via Python's MRO — it does NOT wrap the service, it
*is* the service.  It overrides each of the five public CRUD methods, starts
an OTel span, and delegates to ``super()`` so the rest of the MRO chain
executes inside the span.

Usage::

    from varco_core.observability import TracingServiceMixin, SpanConfig

    class OrderService(
        TracingServiceMixin,          # ← outermost — spans wrap all other hooks
        TenantAwareService,
        SoftDeleteService,
        AsyncService[Order, UUID, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
    ):
        _tracing_config = SpanConfig(tracer_name="orders-svc")

        def _get_repo(self, uow): return uow.orders

MRO execution order for ``create()``::

    TracingServiceMixin.create()    ← opens OTel span
      TenantAwareService.create()  ← injects tenant scope
        SoftDeleteService.create() ← resets deleted_at
          AsyncService.create()    ← actual DB write
    span closed ← ─────────────────────────────────

The span captures the full duration of the operation including all mixin
hooks.

Customising the span config
---------------------------
Override ``_tracing_config`` on the subclass to change the tracer name,
add static attributes, or disable exception recording::

    class PostService(TracingServiceMixin, AsyncService[Post, ...]):
        _tracing_config = SpanConfig(
            tracer_name="posts-svc",
            attributes={"db": "postgresql"},
        )

DESIGN: ClassVar[SpanConfig] override instead of __init__ parameter
    Matches the existing ``CacheServiceMixin._cache_namespace`` pattern.
    ✅ Zero extra ``__init__`` parameters — no change to existing service
       constructors when adding this mixin.
    ✅ Per-service-class config is a class body declaration — obvious,
       static, inspectable without running the code.
    ❌ Config is shared across all instances of the same class (fine for a
       singleton service — not for services instantiated per-request, but
       ``AsyncService`` subclasses are always singletons in practice).

DESIGN: span names as ``{ClassName}.{operation}``
    ✅ Unique per service class — ``OrderService.create`` vs
       ``PostService.create`` in the trace UI.
    ✅ No configuration needed for a useful default name.
    ❌ Refactoring the class name silently changes the span name in your
       dashboards.  Pin the name in ``_tracing_config`` if stability matters.

Thread safety:  ✅ Mixin is stateless; ``SpanConfig`` is frozen.
Async safety:   ✅ Span context manager is entered/exited around ``await super()``.
                   OTel propagates context correctly across ``await`` points.
"""

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from opentelemetry import trace
from opentelemetry.trace import StatusCode

from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO
from varco_core.model import DomainModel
from varco_core.observability.span import SpanConfig
from varco_core.service.base import AsyncService, _ANON_CTX
from varco_core.tracing import current_correlation_id

if TYPE_CHECKING:
    from varco_core.auth import AuthContext
    from varco_core.query.params import QueryParams

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")
C = TypeVar("C", bound=CreateDTO)
R = TypeVar("R", bound=ReadDTO)
U = TypeVar("U", bound=UpdateDTO)


# ── TracingServiceMixin ────────────────────────────────────────────────────────


class TracingServiceMixin(AsyncService[D, PK, C, R, U], ABC, Generic[D, PK, C, R, U]):
    """
    MRO mixin that wraps each ``AsyncService`` CRUD operation in an OTel span.

    Compose **leftmost** so spans wrap all other mixin hooks and the actual
    service implementation::

        class OrderService(
            TracingServiceMixin,      # ← first (outermost)
            TenantAwareService,
            AsyncService[Order, UUID, ...],
        ): ...

    The span name is ``{ClassName}.{operation}`` by default
    (e.g. ``"OrderService.create"``).  Override ``_tracing_config`` to
    customise the tracer name, add static attributes, or change exception
    handling behaviour.

    Class attributes:
        _tracing_config:
            ``SpanConfig`` applied to all CRUD spans.  Override at the
            subclass level to customise per-service.  Defaults to
            ``SpanConfig()`` (tracer name ``"varco"``, no static attributes).

    Thread safety:  ✅ Mixin adds no mutable instance state.
    Async safety:   ✅ Each CRUD method opens a span, awaits super(), closes span.
    """

    # Subclasses override this at the class body level — no __init__ change needed.
    _tracing_config: ClassVar[SpanConfig] = SpanConfig()

    # ── CRUD overrides ────────────────────────────────────────────────────────

    async def create(self, dto: C, ctx: AuthContext = _ANON_CTX) -> R:
        """Create an entity, wrapped in an OTel span."""
        return await self._run_in_span("create", super().create, dto, ctx)

    async def read(self, pk: PK, ctx: AuthContext = _ANON_CTX) -> R:
        """Read an entity by primary key, wrapped in an OTel span."""
        return await self._run_in_span("read", super().read, pk, ctx)

    async def update(self, pk: PK, dto: U, ctx: AuthContext = _ANON_CTX) -> R:
        """Update an entity, wrapped in an OTel span."""
        return await self._run_in_span("update", super().update, pk, dto, ctx)

    async def delete(self, pk: PK, ctx: AuthContext = _ANON_CTX) -> None:
        """Delete an entity, wrapped in an OTel span."""
        await self._run_in_span("delete", super().delete, pk, ctx)

    async def list(self, params: QueryParams, ctx: AuthContext = _ANON_CTX) -> list[R]:
        """List entities matching query params, wrapped in an OTel span."""
        return await self._run_in_span("list", super().list, params, ctx)

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _run_in_span(self, operation: str, coro_fn, *args):
        """
        Open a span named ``{ClassName}.{operation}``, run ``coro_fn(*args)``
        inside it, close the span on completion or exception.

        This helper exists to avoid duplicating the span open/close/exception
        logic in each of the five CRUD overrides.

        Args:
            operation: Short operation name (``"create"``, ``"read"``, etc.).
                       Combined with the class name to form the span name.
            coro_fn:   Coroutine function to call (always a ``super().method``
                       call from one of the CRUD overrides above).
            *args:     Positional arguments forwarded to ``coro_fn``.

        Returns:
            The return value of ``coro_fn(*args)``.

        Raises:
            Any exception raised by ``coro_fn`` — always re-raised after
            being recorded on the span.

        Edge cases:
            - ``coro_fn`` raising a ``CancelledError`` → recorded and re-raised.
              The span is marked ERROR which may be surprising but is correct —
              a cancelled task failed to complete its work.

        Async safety: ✅ Each invocation is fully isolated — no shared state.
        """
        cfg = self._tracing_config
        # Span name: "OrderService.create", "PostService.list", etc.
        span_name = f"{type(self).__name__}.{operation}"

        tracer = trace.get_tracer(cfg.tracer_name)
        # Disable SDK auto-recording so SpanConfig flags fully control behaviour.
        with tracer.start_as_current_span(
            span_name,
            record_exception=False,
        ) as current_span:
            # Stamp static attributes from SpanConfig.
            for k, v in cfg.attributes.items():
                current_span.set_attribute(k, v)

            # Bridge correlation ID → span attribute so traces and log lines
            # can be joined in the observability backend.
            cid = current_correlation_id()
            if cid is not None:
                current_span.set_attribute("correlation_id", cid)

            try:
                return await coro_fn(*args)
            except Exception as exc:
                if cfg.record_exception:
                    current_span.record_exception(exc)
                if cfg.set_status_on_error:
                    current_span.set_status(StatusCode.ERROR, str(exc))
                raise


__all__ = ["TracingServiceMixin"]
