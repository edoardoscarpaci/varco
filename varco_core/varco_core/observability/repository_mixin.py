"""
varco_core.observability.repository_mixin
==========================================
``TracingRepositoryMixin`` — automatic OTel span instrumentation for
``AsyncRepository`` subclasses.

Like ``TracingServiceMixin`` for the service layer, this mixin is composed
via Python's MRO.  It overrides each repository method, opens an OTel span,
and delegates to ``super()`` so the concrete backend implementation runs
inside the span.

Usage::

    from varco_core.observability import TracingRepositoryMixin, SpanConfig

    class TracedUserRepository(
        TracingRepositoryMixin,           # ← outermost — spans wrap backend impl
        AsyncSQLAlchemyRepository[User, UUID],
    ):
        _tracing_config = SpanConfig(
            tracer_name="orders-svc",
            attributes={"db.system": "postgresql"},
        )

MRO execution order for ``save()``::

    TracingRepositoryMixin.save()     ← opens OTel span
      AsyncSQLAlchemyRepository.save() ← actual DB write
    span closed ← ────────────────────────────────────────

span_by_query spans cover the full lazy iteration::

    TracingRepositoryMixin.stream_by_query()
      span opened ←─────────────────────────
        AsyncSQLAlchemyRepository.stream_by_query()  ← DB cursor open
          yield entity, yield entity, ...
        cursor closed
      span closed ←─────────────────────────

Customising the span config
---------------------------
Override ``_tracing_config`` on the subclass::

    class UserRepository(TracingRepositoryMixin, AsyncBeanieRepository[User, UUID]):
        _tracing_config = SpanConfig(
            tracer_name="user-svc",
            attributes={"db.system": "mongodb"},
        )

DESIGN: ClassVar[SpanConfig] override
    Matches the ``TracingServiceMixin._tracing_config`` and
    ``CacheServiceMixin._cache_namespace`` patterns.
    ✅ Zero extra ``__init__`` parameters — no change to existing constructors.
    ✅ Per-repository-class config via a class body declaration.
    ❌ Config is shared across instances — fine for singleton repositories.

DESIGN: span name as ``{ClassName}.{operation}``
    ✅ Unique per repository class — ``UserRepository.save`` vs
       ``PostRepository.find_by_query`` in the trace UI.
    ✅ No extra config needed for a useful default name.
    ❌ Renaming the class silently changes span names in dashboards.
       Pin the name in ``_tracing_config`` if stability matters.

DESIGN: stream_by_query as async generator with spanning
    The span for ``stream_by_query`` covers the full iteration — from the
    first yielded entity to the last (or to an exception).  This accurately
    reflects the DB cursor lifetime and gives operators a span duration that
    includes all network/deserialisation overhead.
    ✅ Accurate span duration — cursor is open for the full iteration.
    ✅ Exceptions inside the generator body are recorded on the span.
    ❌ The span is not closed until the caller finishes iteration.  Long-lived
       streams produce very long spans — expected and correct.

Thread safety:  ✅ Mixin adds no mutable instance state.
Async safety:   ✅ All overrides are async def; span context propagates correctly.
"""

from __future__ import annotations

import logging
from abc import ABC
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    ClassVar,
    Generic,
    Sequence,
    TypeVar,
)

from opentelemetry import trace
from opentelemetry.trace import StatusCode

from varco_core.model import DomainModel
from varco_core.observability.span import SpanConfig
from varco_core.repository import AsyncRepository
from varco_core.tracing import current_correlation_id

if TYPE_CHECKING:
    from varco_core.query.params import QueryParams

_logger = logging.getLogger(__name__)

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")


# ── TracingRepositoryMixin ────────────────────────────────────────────────────


class TracingRepositoryMixin(AsyncRepository[D, PK], ABC, Generic[D, PK]):
    """
    MRO mixin that wraps each ``AsyncRepository`` operation in an OTel span.

    Compose **leftmost** so spans wrap the entire concrete backend implementation::

        class UserRepository(
            TracingRepositoryMixin,       # ← first (outermost)
            AsyncSQLAlchemyRepository[User, UUID],
        ): ...

    The span name is ``{ClassName}.{operation}`` by default
    (e.g. ``"UserRepository.save"``).  Override ``_tracing_config`` to
    customise the tracer name, add static attributes (e.g. ``"db.system"``),
    or disable exception recording.

    Class attributes:
        _tracing_config:
            ``SpanConfig`` applied to all repository spans.  Override at the
            subclass level to customise per-repository.  Defaults to
            ``SpanConfig()`` (tracer name ``"varco"``, no static attributes).

    Thread safety:  ✅ Mixin adds no mutable instance state.
    Async safety:   ✅ Each method opens a span, awaits super(), closes span.
    """

    _tracing_config: ClassVar[SpanConfig] = SpanConfig()

    # ── CRUD overrides ────────────────────────────────────────────────────────

    async def find_by_id(self, pk: PK) -> D | None:
        """Find entity by primary key, wrapped in an OTel span."""
        return await self._run_in_span("find_by_id", super().find_by_id, pk)

    async def find_all(self) -> list[D]:
        """Retrieve all entities, wrapped in an OTel span."""
        return await self._run_in_span("find_all", super().find_all)

    async def save(self, entity: D) -> D:
        """Persist entity (INSERT or UPDATE), wrapped in an OTel span."""
        return await self._run_in_span("save", super().save, entity)

    async def delete(self, entity: D) -> None:
        """Delete entity, wrapped in an OTel span."""
        await self._run_in_span("delete", super().delete, entity)

    async def find_by_query(self, params: QueryParams) -> list[D]:
        """Filtered/paginated query, wrapped in an OTel span."""
        return await self._run_in_span("find_by_query", super().find_by_query, params)

    async def count(self, params: QueryParams | None = None) -> int:
        """Count matching entities, wrapped in an OTel span."""
        return await self._run_in_span("count", super().count, params)

    async def exists(self, pk: PK) -> bool:
        """Check entity existence by primary key, wrapped in an OTel span."""
        return await self._run_in_span("exists", super().exists, pk)

    async def save_many(self, entities: Sequence[D]) -> list[D]:
        """Bulk INSERT/UPDATE, wrapped in an OTel span."""
        return await self._run_in_span("save_many", super().save_many, entities)

    async def delete_many(self, entities: Sequence[D]) -> None:
        """Bulk DELETE, wrapped in an OTel span."""
        await self._run_in_span("delete_many", super().delete_many, entities)

    async def update_many_by_query(
        self,
        params: QueryParams,
        update: dict[str, Any],
    ) -> int:
        """Bulk UPDATE by query, wrapped in an OTel span."""
        return await self._run_in_span(
            "update_many_by_query", super().update_many_by_query, params, update
        )

    def stream_by_query(self, params: QueryParams) -> AsyncIterator[D]:  # type: ignore[override]
        """
        Async generator that wraps the backing ``stream_by_query`` in an OTel span.

        The span covers the full iteration — from the first yielded entity to
        the last or to an exception.  This accurately reflects the DB cursor
        lifetime.

        Args:
            params: ``QueryParams`` with filter, sort, and optional pagination.

        Returns:
            An ``AsyncIterator[D]`` whose lifetime is wrapped in a single span.
        """
        return self._stream_with_span(params)

    async def _stream_with_span(self, params: QueryParams) -> AsyncIterator[D]:  # type: ignore[misc, return]
        """Async generator implementation for ``stream_by_query`` tracing."""
        cfg = self._tracing_config
        span_name = f"{type(self).__name__}.stream_by_query"

        tracer = trace.get_tracer(cfg.tracer_name)
        # Disable SDK auto-handling — we apply SpanConfig flags ourselves.
        with tracer.start_as_current_span(
            span_name, record_exception=False, set_status_on_exception=False
        ) as current_span:
            for k, v in cfg.attributes.items():
                current_span.set_attribute(k, v)

            cid = current_correlation_id()
            if cid is not None:
                current_span.set_attribute("correlation_id", cid)

            try:
                async for entity in super().stream_by_query(params):
                    yield entity
            except Exception as exc:
                if cfg.record_exception:
                    current_span.record_exception(exc)
                if cfg.set_status_on_error:
                    current_span.set_status(StatusCode.ERROR, str(exc))
                raise

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _run_in_span(self, operation: str, coro_fn, *args: Any) -> Any:
        """
        Open a span named ``{ClassName}.{operation}``, run ``coro_fn(*args)``
        inside it, and close on completion or exception.

        Args:
            operation: Short name (``"save"``, ``"find_by_id"``, etc.).
            coro_fn:   Coroutine function to call (always ``super().method``).
            *args:     Positional arguments forwarded to ``coro_fn``.

        Returns:
            The return value of ``coro_fn(*args)``.

        Raises:
            Any exception raised by ``coro_fn`` — always re-raised after
            being recorded on the span when ``_tracing_config.record_exception``
            is True.

        Async safety: ✅ Each invocation is fully isolated — no shared state.
        """
        cfg = self._tracing_config
        span_name = f"{type(self).__name__}.{operation}"

        tracer = trace.get_tracer(cfg.tracer_name)
        # Disable SDK auto-handling — we apply SpanConfig flags ourselves.
        with tracer.start_as_current_span(
            span_name, record_exception=False, set_status_on_exception=False
        ) as current_span:
            for k, v in cfg.attributes.items():
                current_span.set_attribute(k, v)

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


__all__ = ["TracingRepositoryMixin"]
