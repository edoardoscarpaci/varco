"""
varco_core.service.bulk
========================
``BulkServiceMixin`` — atomic multi-entity operations for ``AsyncService``.

Adds ``create_many()`` and ``delete_many()`` to any ``AsyncService`` subclass
via MRO composition.  Both methods run inside a **single ``UoW``** so the
batch is fully atomic: all entities are persisted/deleted together or the
entire operation rolls back.

Usage::

    from varco_core.service.bulk import BulkServiceMixin

    class OrderService(
        BulkServiceMixin,             # ← add before AsyncService in MRO
        AsyncService[Order, UUID, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
    ):
        def _get_repo(self, uow): return uow.orders

    # Now available:
    read_dtos = await svc.create_many([dto1, dto2, dto3], ctx)
    await svc.delete_many([pk1, pk2, pk3], ctx)

Hook composition
----------------
Both methods call the same hooks as their single-entity counterparts:

``create_many`` calls (per entity, inside UoW):
    ``_prepare_for_create``   — stamp tenant_id, owner_id, etc.
    ``_validate_entity``      — sync business invariants
    ``_validate_entity_async``— async I/O-bound invariants
    Then:  ``repo.save_many`` — one bulk INSERT round-trip
    After: ``_after_create``  — per-entity; called after UoW commits

``delete_many`` calls (per entity, inside UoW):
    ``_check_entity``         — cross-concern gate (soft-delete, tenant, etc.)
    ``authorizer.authorize``  — per-entity ownership / permission check
    Then:  ``repo.delete_many``— one bulk DELETE round-trip
    After: ``_after_delete``  — per-entity; called after UoW commits

DESIGN: one UoW for all entities
    ✅ Atomicity — all-or-nothing semantics match the caller's intent.
    ✅ A single DB round-trip for INSERT/DELETE via ``save_many``/``delete_many``.
    ✅ Hook composition works without change — mixins in the MRO see every
       entity individually through the per-entity hook calls.
    ❌ One failing entity blocks the entire batch.  Callers must pre-validate
       their DTOs or accept that a partial failure means a full rollback.

DESIGN: create_many authorizes once (type-level), delete_many per entity
    CREATE authorization is type-level (no existing entity to check ownership).
    This matches ``AsyncService.create()`` — one ``authorizer.authorize`` call.
    DELETE authorization is per-entity because each entity may have a different
    owner and ``Resource(entity=entity)`` is what ownership policies check.
    ✅ Consistent with the semantics of single-entity operations.
    ❌ ``delete_many(N_pks)`` makes N+1 DB calls (N×find_by_id + 1 delete_many).
       For very large batches, consider a query-based bulk delete instead.

Thread safety:  ✅ Mixin adds no mutable instance state.
Async safety:   ✅ All methods are async def; UoW and hook calls are awaited.
"""

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Generic, Sequence, TypeVar

from varco_core.auth import Action, AuthContext, Resource
from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO
from varco_core.event.domain import EntityCreatedEvent, EntityDeletedEvent
from varco_core.exception.service import ServiceNotFoundError
from varco_core.model import DomainModel
from varco_core.service.base import AsyncService, _ANON_CTX
from varco_core.service.mixin import ServiceMixin
from varco_core.tracing import current_correlation_id

if TYPE_CHECKING:
    pass

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")
C = TypeVar("C", bound=CreateDTO)
R = TypeVar("R", bound=ReadDTO)
U = TypeVar("U", bound=UpdateDTO)


# ── BulkServiceMixin ──────────────────────────────────────────────────────────


class BulkServiceMixin(
    ServiceMixin,
    AsyncService[D, PK, C, R, U],
    ABC,
    Generic[D, PK, C, R, U],
):
    """
    MRO mixin that adds atomic bulk create and delete to ``AsyncService``.

    Compose **before** ``AsyncService`` in the class hierarchy::

        class OrderService(
            BulkServiceMixin,     # ← left of AsyncService
            TenantAwareService,
            AsyncService[Order, UUID, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
        ):
            def _get_repo(self, uow): return uow.orders

    Both ``create_many`` and ``delete_many`` run within a **single** ``UoW``
    and share all hook composition points with their single-entity counterparts.

    Thread safety:  ✅ Mixin adds no mutable instance state.
    Async safety:   ✅ All methods are async def.
    """

    async def create_many(
        self,
        dtos: Sequence[C],
        ctx: AuthContext = _ANON_CTX,
    ) -> list[R]:
        """
        Create multiple entities in a single atomic transaction.

        All entities are prepared, validated, and persisted in one ``UoW``.
        If any validation or DB error occurs the entire batch rolls back.

        Authorization is checked once at the type level (no existing entity
        to check ownership against) — matching the semantics of single-entity
        ``create()``.

        Post-commit hooks (``_after_create``) and domain events
        (``EntityCreatedEvent``) are dispatched per entity, in input order.

        Args:
            dtos: Sequence of ``CreateDTO`` payloads.  Empty sequence → no-op,
                  returns ``[]``.
            ctx:  Caller's identity and grants.

        Returns:
            A list of ``ReadDTO`` objects in the same order as ``dtos``.

        Raises:
            ServiceAuthorizationError: Caller is not allowed to create this
                                       entity type.
            ServiceValidationError:    Any entity fails a business invariant.
            ServiceConflictError:      Any entity violates a DB uniqueness
                                       constraint (propagated from UoW commit).

        Edge cases:
            - Empty ``dtos`` returns ``[]`` immediately without touching the DB.
            - ``save_many`` delegates to a single bulk INSERT when supported by
              the backend — this is why all entities are batched into one call.

        Async safety: ✅ Opens one UoW; all operations inside are awaited.
        """
        if not dtos:
            return []

        # Fast stateless pre-flight before the UoW is opened.
        self._pre_check(ctx)

        # Type-level CREATE authorization — no existing entity to check ownership.
        await self._authorizer.authorize(
            ctx,
            Action.CREATE,
            Resource(entity_type=self._entity_type()),
        )

        async with self._uow_provider.make_uow() as uow:
            entities: list[D] = []
            for dto in dtos:
                entity = self._assembler.to_domain(dto)
                entity = self._prepare_for_create(entity, ctx)
                self._validate_entity(entity, ctx)
                await self._validate_entity_async(entity, ctx)
                entities.append(entity)

            saved_entities = await self._get_repo(uow).save_many(entities)
            read_dtos = [self._assembler.to_read_dto(e) for e in saved_entities]

        # ← UoW committed here.  Publish and run hooks after commit.
        for saved, read_dto in zip(saved_entities, read_dtos):
            await self._publish_domain_event(
                EntityCreatedEvent(
                    entity_type=self._entity_type().__name__,
                    pk=saved.pk,
                    tenant_id=ctx.metadata.get("tenant_id") if ctx else None,
                    correlation_id=current_correlation_id(),
                    payload=read_dto.model_dump(),
                )
            )
            await self._after_create(saved, read_dto, ctx)

        return read_dtos

    async def delete_many(
        self,
        pks: Sequence[PK],
        ctx: AuthContext = _ANON_CTX,
    ) -> None:
        """
        Delete multiple entities by primary key in a single atomic transaction.

        Each entity is fetched individually so that ``_check_entity`` and per-entity
        authorization can run.  All deletes are issued in one ``repo.delete_many``
        call at the end — typically a single ``DELETE WHERE pk IN (…)`` round-trip.

        If **any** entity is missing or fails an authorization check the entire
        batch rolls back — no partial deletes.

        Args:
            pks: Sequence of primary keys to delete.  Empty sequence → no-op.
            ctx: Caller's identity and grants.

        Returns:
            ``None``.

        Raises:
            ServiceNotFoundError:      Any ``pk`` in ``pks`` has no matching entity.
            ServiceAuthorizationError: Caller is not allowed to delete any one of
                                       the fetched entities.

        Edge cases:
            - Empty ``pks`` returns immediately without touching the DB.
            - Each entity is fetched with a separate ``find_by_id`` call —
              N+1 round-trips for N PKs.  Prefer ``update_many_by_query`` /
              a custom service method for very large batches.

        Async safety: ✅ Opens one UoW; all find_by_id calls and the final
                          delete_many are awaited sequentially.
        """
        if not pks:
            return

        # Fast stateless pre-flight before the UoW is opened.
        self._pre_check(ctx)

        async with self._uow_provider.make_uow() as uow:
            repo = self._get_repo(uow)
            entities: list[D] = []

            for pk in pks:
                entity = await repo.find_by_id(pk)
                if entity is None:
                    raise ServiceNotFoundError(pk, self._entity_type())

                # Cross-concern gate (soft-delete, tenant filter, etc.) before auth.
                self._check_entity(entity, ctx)
                await self._async_check_entity(entity, ctx)

                # Per-entity DELETE authorization — ownership may differ per entity.
                await self._authorizer.authorize(
                    ctx,
                    Action.DELETE,
                    Resource(entity_type=self._entity_type(), entity=entity),
                )
                entities.append(entity)

            deleted_pks = [e.pk for e in entities]
            await repo.delete_many(entities)

        # ← UoW committed here.  Publish and run hooks after commit.
        for pk in deleted_pks:
            await self._publish_domain_event(
                EntityDeletedEvent(
                    entity_type=self._entity_type().__name__,
                    pk=pk,
                    tenant_id=ctx.metadata.get("tenant_id") if ctx else None,
                    correlation_id=current_correlation_id(),
                )
            )
            await self._after_delete(pk, ctx)


__all__ = ["BulkServiceMixin"]
