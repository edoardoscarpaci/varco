"""
service
=======
Business logic for the ``Product`` entity.

``ProductService`` is a minimal ``AsyncService`` subclass — no caching,
no event publishing, no soft-delete.  It implements the single required
abstract method ``_get_repo()`` to wire the service to the
``InMemoryUoW.products`` attribute.

DESIGN: minimal service over full-featured service
    This example demonstrates the minimum viable ``AsyncService`` subclass.
    For a production-grade service with caching and events see the
    ``00-full-stack-post-api`` example.

    ✅ Easy to follow — one class, one method.
    ✅ No extra dependencies (Redis, Kafka) required.
    ❌ No caching or event publishing — add ``CacheServiceMixin`` and
       ``AbstractEventProducer`` for those features.

Thread safety:  ⚠️ Singleton — all methods must be stateless;
                   each call opens its own UoW via ``_uow_provider.make_uow()``.
Async safety:   ✅ All public methods are ``async def``.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from dtos import ProductCreate, ProductRead, ProductUpdate
from models import Product
from providify import Inject, InjectMeta, Singleton
from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth.base import AbstractAuthorizer
from varco_core.event import AbstractEventProducer
from varco_core.repository import AsyncRepository
from varco_core.service.base import AsyncService, IUoWProvider
from varco_core.uow import AsyncUnitOfWork


@Singleton
class ProductService(
    AsyncService[Product, UUID, ProductCreate, ProductRead, ProductUpdate],
):
    """
    CRUD service for ``Product`` entities.

    Provides create, read, update, delete, and list operations backed by
    the injected ``IUoWProvider`` (resolved to ``InMemoryUoWProvider`` via DI).

    Authorization uses the default ``BaseAuthorizer`` (permissive) — no auth
    is required for any operation in this minimal example. Since 4.0 it binds
    only under ``VARCO_SECURITY_ENV=development`` (Plan 053 / FLIP-5).

    Class injection (via providify):
        - ``uow_provider``  → ``IUoWProvider`` (``InMemoryUoWProvider``)
        - ``authorizer``    → ``AbstractAuthorizer`` (``BaseAuthorizer``)
        - ``assembler``     → ``AbstractDTOAssembler[Product, ...]`` (``ProductAssembler``)

    Thread safety:  ⚠️ Singleton — must be stateless.
    Async safety:   ✅ All public methods are ``async def``.
    """

    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer: Inject[AbstractAuthorizer],
        # Full generic alias required so providify resolves the correct
        # ProductAssembler binding (registered under the generic alias).
        assembler: Inject[AbstractDTOAssembler[Product, ProductCreate, ProductRead, ProductUpdate]],
        # Optional event producer — no-op when no bus is wired (this example).
        # Declared here so a bus can be added later without changing __init__.
        producer: Annotated[AbstractEventProducer, InjectMeta(optional=True)] = None,
    ) -> None:
        """
        Args:
            uow_provider: Injected ``IUoWProvider`` — provided by ``ProductModule``.
            authorizer:   Injected ``AbstractAuthorizer`` — ``BaseAuthorizer``
                          (permissive) by default; no custom authorizer is wired.
            assembler:    Injected ``ProductAssembler`` — handles DTO ↔ Product mapping.
            producer:     Optional ``AbstractEventProducer``.  Resolves to ``None``
                          in this example (no bus wired) — ``AsyncService`` falls
                          back to ``NoopEventProducer`` automatically.
        """
        super().__init__(
            uow_provider=uow_provider,
            authorizer=authorizer,
            assembler=assembler,
            producer=producer,
        )

    def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[Product, UUID]:
        """
        Return the ``AsyncRepository[Product]`` from the open unit-of-work.

        ``InMemoryUoW`` exposes the repository as ``uow.products``, mirroring
        the SQLAlchemy UoW attribute naming convention so services are portable
        between backends without code changes.

        Args:
            uow: The open ``AsyncUnitOfWork`` for this request.

        Returns:
            ``InMemoryProductRepository`` bound to the current UoW.
        """
        return uow.products  # type: ignore[attr-defined]


__all__ = ["ProductService"]
