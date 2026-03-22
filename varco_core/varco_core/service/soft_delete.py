"""
varco_core.service.soft_delete
===================================
Service mixin that replaces physical deletion with soft deletion.

Soft deletion marks an entity as logically deleted by setting a
``deleted_at`` timestamp instead of removing the row.  Active queries
automatically exclude soft-deleted records.

``SoftDeleteService`` overrides the three ``AsyncService`` composable hooks
so it can be combined with other mixins (e.g. ``TenantAwareService``) via
Python's MRO without duplicating CRUD method bodies::

    # Tenant + soft-delete together
    class PostService(TenantAwareService, SoftDeleteService, AsyncService[Post, ...]):
        def _get_repo(self, uow): return uow.posts

MRO co-operation
----------------
Each ``_scoped_params`` and ``_check_entity`` implementation ends with
``super(...)`` so the chain continues through the MRO.  In the example above:

    PostService._scoped_params → TenantAwareService._scoped_params
        → SoftDeleteService._scoped_params → AsyncService._scoped_params

Both tenant and soft-delete filters are injected in a single call.

``delete()`` is overridden because it changes behaviour fundamentally:
physical DELETE → UPDATE (set ``deleted_at``).  All other CRUD methods are
covered by the three hooks.

DESIGN: hook-based over CRUD override
    ✅ No code duplication with base ``AsyncService`` CRUD methods.
    ✅ Composable via MRO — works with ``TenantAwareService`` simultaneously.
    ✅ ``restore()`` is a clean extension point (not possible with pure CRUD
       override because base ``delete()`` physical-removes the row).
    ❌ Restore exposes soft-deleted entities via ``find_by_id()`` — it must
       bypass ``_check_entity`` to fetch them.  This is intentional and
       documented.

DESIGN: ``deleted_at`` stamped via ``dataclasses.replace()`` (not ORM)
    ✅ Keeps service layer ORM-agnostic.
    ✅ ``_raw_orm`` is preserved — the repository sees an UPDATE, not INSERT.
    ❌ Requires ``deleted_at`` to be declared with ``init=True`` on the model
       (see ``SoftDeleteMixin``).

Thread safety:  ⚠️ Inherits ``AsyncService`` singleton contract.
Async safety:   ✅ All public methods are ``async def``.
"""

from __future__ import annotations

import dataclasses
from abc import ABC
from datetime import UTC, datetime
from typing import ClassVar, Generic, TypeVar

from varco_core.auth import Action, AuthContext, Resource
from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO
from varco_core.exception.service import ServiceNotFoundError
from varco_core.model import DomainModel
from varco_core.query.builder import QueryBuilder
from varco_core.query.params import QueryParams
from varco_core.service.base import AsyncService

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")
C = TypeVar("C", bound=CreateDTO)
R = TypeVar("R", bound=ReadDTO)
U = TypeVar("U", bound=UpdateDTO)


class SoftDeleteService(AsyncService[D, PK, C, R, U], ABC, Generic[D, PK, C, R, U]):
    """
    Abstract ``AsyncService`` mixin that replaces physical deletion with
    soft deletion.

    Implements the three ``AsyncService`` composable hooks:

    - ``_scoped_params`` — injects ``<_soft_delete_field> IS NULL`` into
      every ``list`` and ``count`` query so soft-deleted records are excluded.
    - ``_check_entity``  — raises ``ServiceNotFoundError`` when a fetched entity
      is already soft-deleted (prevents accessing deleted records via ``get``
      / ``update``).
    - ``_prepare_for_create`` — resets ``deleted_at`` to ``None`` so a
      newly created entity is always active regardless of the constructor
      default.

    Additionally overrides ``delete()`` to set ``deleted_at`` instead of
    physically removing the row, and provides ``restore()`` to undo a
    soft deletion.

    Class attributes:
        _soft_delete_field: Name of the ``datetime | None`` field on the
                            domain model used for soft deletion.
                            Defaults to ``"deleted_at"``.

    Subclass contract:
        1. Domain model must declare ``_soft_delete_field`` with ``init=True``
           (e.g. inherit from ``SoftDeleteMixin``).
        2. Implement ``_get_repo(uow)`` — the only required override.

    Thread safety:  ⚠️ Inherits ``AsyncService`` singleton contract.
    Async safety:   ✅ All public methods are ``async def``.

    Edge cases:
        - ``_soft_delete_field`` not present on the domain model → ``AttributeError``
          from ``getattr`` at ``_check_entity`` time (fail-fast).
        - ``restore()`` bypasses ``_check_entity`` — intentional, as the entity
          is expected to be soft-deleted.
        - Combining with ``TenantAwareService``: put ``TenantAwareService``
          first in the MRO so tenant filter is injected before soft-delete
          filter — both are applied via the ``super()`` chain.

    Example::

        @Singleton
        class PostService(
            SoftDeleteService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO]
        ):
            def __init__(
                self,
                uow_provider: Inject[IUoWProvider],
                authorizer:   Inject[AbstractAuthorizer],
                assembler:    Inject[AbstractDTOAssembler[Post, ...]],
            ) -> None:
                super().__init__(
                    uow_provider=uow_provider,
                    authorizer=authorizer,
                    assembler=assembler,
                )

            def _get_repo(self, uow): return uow.posts
    """

    # Override to match a different field name on the domain model.
    _soft_delete_field: ClassVar[str] = "deleted_at"

    # ── Composable hooks ──────────────────────────────────────────────────────

    def _scoped_params(self, params: QueryParams, ctx: AuthContext) -> QueryParams:
        """
        Inject ``<_soft_delete_field> IS NULL`` into the query filter.

        Ensures that soft-deleted entities are never returned by ``list``
        or counted by ``count``.  Chains via ``super()`` so other mixins
        in the MRO (e.g. ``TenantAwareService``) also inject their filters.

        Args:
            params: Incoming query parameters.
            ctx:    Caller's identity (not inspected by this hook).

        Returns:
            New ``QueryParams`` with the soft-delete IS NULL filter prepended.
        """
        # IS NULL filter: only return active (non-deleted) entities
        null_node = QueryBuilder().is_null(self._soft_delete_field).build()
        scoped_node = QueryBuilder(null_node).and_(QueryBuilder(params.node)).build()
        scoped = dataclasses.replace(params, node=scoped_node)
        # Chain to super so additional mixins (e.g. tenant filter) also apply.
        return super()._scoped_params(scoped, ctx)

    def _check_entity(self, entity: D, ctx: AuthContext) -> None:
        """
        Raise ``ServiceNotFoundError`` if the entity is soft-deleted.

        Uses 404 (not 403) — from the caller's perspective, soft-deleted
        entities do not exist.  Chains via ``super()`` for MRO composability.

        Args:
            entity: Fetched domain entity.
            ctx:    Caller's identity (not inspected by this hook).

        Raises:
            ServiceNotFoundError: ``entity.deleted_at`` is not ``None``.
        """
        if getattr(entity, self._soft_delete_field) is not None:
            # Return 404 — soft-deleted entities are logically non-existent.
            raise ServiceNotFoundError(entity.pk, type(entity))
        # Chain to super so other checks (e.g. tenant) also run.
        super()._check_entity(entity, ctx)

    def _prepare_for_create(self, entity: D, ctx: AuthContext) -> D:
        """
        Reset ``deleted_at`` to ``None`` on newly created entities.

        Ensures a freshly created entity is always active, even if the
        domain model's constructor default were accidentally set to a
        non-None value.  Chains via ``super()`` for MRO composability.

        Args:
            entity: Freshly assembled, unsaved domain entity.
            ctx:    Caller's identity.

        Returns:
            New entity with ``_soft_delete_field`` set to ``None``.
        """
        # Always reset — guarantees new entities start active regardless of
        # constructor defaults or mixin ordering in the MRO.
        # type: ignore[assignment] — dataclasses.replace loses the TypeVar D
        # through the **kwargs unpacking; the result is still D at runtime.
        active: D = dataclasses.replace(entity, **{self._soft_delete_field: None})  # type: ignore[assignment]
        return super()._prepare_for_create(active, ctx)

    # ── Overridden CRUD ───────────────────────────────────────────────────────

    async def delete(self, pk: PK, ctx: AuthContext) -> None:
        """
        Soft-delete an entity by setting ``deleted_at`` to the current UTC time.

        No physical row is removed.  Subsequent ``get`` / ``list`` / ``count``
        calls will treat the entity as non-existent.  Use ``restore()`` to
        undo the soft deletion.

        Authorization order:
        1. Fetch the entity (raises ``ServiceNotFoundError`` if missing).
        2. ``_check_entity`` — raises 404 if already soft-deleted.
        3. Authorize ``Action.DELETE`` on the current entity state.
        4. Set ``deleted_at = now(UTC)`` via ``dataclasses.replace`` and save.

        Args:
            pk:  Primary key of the entity to soft-delete.
            ctx: Caller's identity and grants.

        Returns:
            ``None`` on success.

        Raises:
            ServiceNotFoundError:      No active entity with ``pk`` exists.
            ServiceAuthorizationError: Caller is not allowed to delete it.

        Edge cases:
            - Soft-deleting an already soft-deleted entity → 404 (from
              ``_check_entity``).  Idempotency requires explicit ``restore()``
              first.
            - The ORM row is updated (not deleted) — ``_raw_orm`` must be
              preserved via ``dataclasses.replace()``.
            - ``dataclasses.replace`` requires ``_soft_delete_field`` to have
              ``init=True`` on the domain model (see ``SoftDeleteMixin``).

        Thread safety:  ⚠️ Singleton service — each call opens its own UoW.
        Async safety:   ✅ ``async def`` — safe to call concurrently.
        """
        async with self._uow_provider.make_uow() as uow:
            entity = await self._get_repo(uow).find_by_id(pk)
            if entity is None:
                raise ServiceNotFoundError(pk, self._entity_type())

            # Check before authorizer — 404 prevents existence oracle.
            self._check_entity(entity, ctx)

            await self._authorizer.authorize(
                ctx,
                Action.DELETE,
                Resource(entity_type=self._entity_type(), entity=entity),
            )

            # Stamp deleted_at — dataclasses.replace preserves _raw_orm so
            # the repository issues an UPDATE (not INSERT).
            now = datetime.now(UTC)
            soft_deleted = dataclasses.replace(entity, **{self._soft_delete_field: now})
            await self._get_repo(uow).save(soft_deleted)

    async def restore(self, pk: PK, ctx: AuthContext) -> R:
        """
        Restore a soft-deleted entity by clearing ``deleted_at``.

        Bypasses ``_check_entity`` intentionally — the entity is expected to
        be soft-deleted (``_check_entity`` would raise 404 for such entities).

        Authorization order:
        1. Fetch the entity (raises ``ServiceNotFoundError`` if not found at all).
        2. Authorize ``Action.UPDATE`` on the current entity state.
        3. Clear ``deleted_at`` and save.

        Args:
            pk:  Primary key of the soft-deleted entity to restore.
            ctx: Caller's identity and grants.

        Returns:
            The ``ReadDTO`` of the restored (now active) entity.

        Raises:
            ServiceNotFoundError:      No entity (active OR soft-deleted) with
                ``pk`` exists — the row is completely absent.
            ServiceAuthorizationError: Caller is not allowed to update it.

        Edge cases:
            - Restoring an already active entity clears ``deleted_at`` to
              ``None`` — idempotent.
            - Does NOT validate whether the entity was soft-deleted before
              restoring — callers may restore active entities without error.

        Thread safety:  ⚠️ Singleton service — each call opens its own UoW.
        Async safety:   ✅ ``async def``.
        """
        async with self._uow_provider.make_uow() as uow:
            entity = await self._get_repo(uow).find_by_id(pk)
            if entity is None:
                # Entity doesn't exist at all — not just soft-deleted.
                raise ServiceNotFoundError(pk, self._entity_type())

            # INTENTIONALLY skips _check_entity — entity may be soft-deleted.
            # We authorize using UPDATE because restore is a state change.
            await self._authorizer.authorize(
                ctx,
                Action.UPDATE,
                Resource(entity_type=self._entity_type(), entity=entity),
            )

            # Clear deleted_at — entity becomes active again.
            restored = dataclasses.replace(entity, **{self._soft_delete_field: None})
            saved = await self._get_repo(uow).save(restored)
            return self._assembler.to_read_dto(saved)
