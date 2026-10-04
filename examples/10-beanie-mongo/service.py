"""
service.py
==========
Business logic for the blog post entity backed by MongoDB.

``PostService`` extends ``AsyncService[Post, UUID, PostCreate, PostRead, PostUpdate]``
and overrides ``_get_repo()`` (the only required abstract method) to return the
``posts``-keyed repository from the active unit-of-work.

``created_at`` / ``updated_at`` are NOT stamped here.  ``Post`` inherits them
from ``AuditedDomainModel`` as ``init=False`` fields, and the varco mapper owns
them exclusively: ``to_orm`` stamps both on INSERT, ``sync_to_orm`` refreshes
``updated_at`` on every UPDATE.

DESIGN: let the mapper own the audit timestamps (no ``_prepare_for_create``)
    ✅ One authoritative source — no service/mapper double stamping.
    ✅ ``init=False`` fields cannot be passed to ``dataclasses.replace()`` /
       ``domain_replace()`` at all (``ValueError``), so a service-side stamp
       would need ``object.__setattr__`` — exactly what the mapper already does.
    ❌ The timestamps are only visible on the entity ``save()`` returns, not on
       the freshly assembled one — fine, the router reads the saved entity.

Thread safety:  ⚠️ Singleton — methods must be stateless; each call opens
                   its own unit-of-work via ``self._uow_provider.make_uow()``.
Async safety:   ✅ All public methods are ``async def``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from dtos import PostCreate, PostRead, PostUpdate
from models import Post
from providify import Inject, Singleton
from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth.base import AbstractAuthorizer
from varco_core.service.base import AsyncService, IUoWProvider

if TYPE_CHECKING:
    from varco_core.uow import AsyncUnitOfWork


@Singleton
class PostService(AsyncService[Post, UUID, PostCreate, PostRead, PostUpdate]):
    """
    CRUD service for ``Post`` entities backed by MongoDB/Beanie.

    Inherits the full ``AsyncService`` contract:
    - Authorization via injected ``AbstractAuthorizer`` (defaults to permissive).
    - DTO ↔ domain translation via injected ``PostAssembler``.
    - Unit-of-work management via ``IUoWProvider`` (provided by BeanieModule).
    - No event publishing in this example — keeps it focused on Beanie patterns.

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
            uow_provider: Injected ``IUoWProvider`` — provided by ``BeanieModule``.
            authorizer:   Injected ``AbstractAuthorizer`` — ``BaseAuthorizer``
                          (permissive) is bound explicitly in ``app.py``.
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
        ``BeanieRepositoryProvider.make_uow()`` and produced by
        ``BeanieUnitOfWork._begin()``.  No ``AsyncUnitOfWork`` defines a
        ``get_repository()`` method; that name belongs to ``RepositoryProvider``
        itself, not to a unit of work.

        Args:
            uow: The open ``AsyncUnitOfWork`` for this request.

        Returns:
            ``AsyncRepository[Post]`` backed by Beanie/Motor.
        """
        return uow.posts  # type: ignore[attr-defined]


__all__ = ["PostService"]
