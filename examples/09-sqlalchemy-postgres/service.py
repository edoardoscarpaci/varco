"""
service.py
==========
Business logic for the blog post entity backed by PostgreSQL.

``PostService`` extends ``AsyncService[Post, UUID, PostCreate, PostRead, PostUpdate]``
and overrides ``_get_repo()`` (the only required abstract method) to return the
``post``-keyed repository from the active unit-of-work.

The service also overrides ``_prepare_for_create()`` to stamp ``created_at``
and ``updated_at`` timestamps before the entity is saved.  Without this, both
fields would be ``None`` in the database — the SA column type is nullable
(``FieldHint(nullable=True)``), but the ``PostRead`` DTO expects real datetimes.

DESIGN: timestamp stamping in the service layer, not the assembler
    ✅ The assembler stays pure (no ``datetime.now()`` calls, no UTC imports).
    ✅ Timestamps are available immediately after ``save()`` returns — no
       extra SELECT needed to read server-side ``DEFAULT NOW()``.
    ✅ ``_prepare_for_create`` is called by ``AsyncService.create()`` inside
       the UoW, so timestamps are consistent with the commit time.
    ❌ Timestamps are set in Python, not the DB — millisecond precision
       matches Python's ``datetime.now(UTC)`` resolution (adequate here).

Thread safety:  ⚠️ Singleton — methods must be stateless; each call opens
                   its own unit-of-work via ``self._uow_provider.make_uow()``.
Async safety:   ✅ All public methods are ``async def``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from dtos import PostCreate, PostRead, PostUpdate
from models import Post
from providify import Inject, Singleton
from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth.base import AbstractAuthorizer
from varco_core.service.base import AsyncService, IUoWProvider

if TYPE_CHECKING:
    from varco_core.auth import AuthContext
    from varco_core.uow import AsyncUnitOfWork


@Singleton
class PostService(AsyncService[Post, UUID, PostCreate, PostRead, PostUpdate]):
    """
    CRUD service for ``Post`` entities backed by PostgreSQL.

    Inherits the full ``AsyncService`` contract:
    - Authorization via injected ``AbstractAuthorizer`` (permissive, development only).
    - DTO ↔ domain translation via injected ``PostAssembler``.
    - Unit-of-work management via ``IUoWProvider`` (provided by SAConfiguration).
    - No event publishing in this example — keeps it focused on SA patterns.

    Thread safety:  ⚠️ Singleton — must be stateless; each request gets its own UoW.
    Async safety:   ✅ All public methods are ``async def``.
    """

    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer: Inject[AbstractAuthorizer],
        # Concrete generic alias so providify resolves the correct PostAssembler
        # binding (registered under AbstractDTOAssembler[Post, PostCreate, PostRead, PostUpdate]).
        assembler: Inject[AbstractDTOAssembler[Post, PostCreate, PostRead, PostUpdate]],
    ) -> None:
        """
        Args:
            uow_provider: Injected ``IUoWProvider`` — provided by ``SAConfiguration``.
            authorizer:   Injected ``AbstractAuthorizer`` — ``BaseAuthorizer``
                          (permissive), bound in ``app.py``. Since 4.0 that
                          class-keyed bind is gated: run with
                          ``VARCO_SECURITY_ENV=development`` (Plan 053 / FLIP-5).
            assembler:    Injected ``PostAssembler`` — handles DTO ↔ Post mapping.
        """
        # No producer injection — this example does not publish domain events.
        super().__init__(
            uow_provider=uow_provider,
            authorizer=authorizer,
            assembler=assembler,
        )

    def _get_repo(self, uow: AsyncUnitOfWork):
        """
        Return the ``AsyncRepository[Post]`` from the open unit-of-work.

        Repositories are exposed as entity-derived attributes — ``Post`` →
        ``uow.posts`` — the sanctioned accessor documented by
        ``RepositoryProvider.make_uow()`` and produced by
        ``SQLAlchemyUnitOfWork._begin()``.  No ``AsyncUnitOfWork`` defines a
        ``get_repository()`` method; that name belongs to ``RepositoryProvider``
        itself, not to a unit of work.

        Args:
            uow: The open ``AsyncUnitOfWork`` for this request.

        Returns:
            ``AsyncRepository[Post]`` backed by the current session.
        """
        return uow.posts  # type: ignore[attr-defined]

    def _prepare_for_create(self, entity: Post, ctx: AuthContext) -> Post:
        """
        Stamp ``created_at`` and ``updated_at`` before the first INSERT.

        Called by ``AsyncService.create()`` after ``assembler.to_domain()``
        and BEFORE ``repo.save()``.  The entity is assembled but not yet
        persisted when this hook fires.

        Args:
            entity: Freshly assembled ``Post`` (timestamps are ``None``).
            ctx:    Caller's auth context — not used in this example (no auth).

        Returns:
            A new ``Post`` with ``created_at`` and ``updated_at`` both set to now.

        Edge cases:
            - Always calls ``super()._prepare_for_create(entity, ctx)`` so that
              other mixins in the MRO (e.g. ``TenantAwareService``) can also
              stamp their fields after this hook runs.
            - The returned entity still has ``_raw_orm is None`` — it is
              unpersisted and the repository will INSERT it.
        """
        entity = super()._prepare_for_create(entity, ctx)

        # Stamp both timestamps to the same instant for consistency.
        # Using UTC explicitly so the stored datetime is timezone-aware
        # and readable without ambiguity across deployments in different zones.
        now = datetime.now(UTC)
        from dataclasses import replace as _replace

        return _replace(entity, created_at=now, updated_at=now)


__all__ = ["PostService"]
