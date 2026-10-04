"""
service
=======
Document service wired with Casbin RBAC authorization.

``DocumentService`` extends ``AsyncService`` with a thin override of
``_get_repo()`` — all RBAC enforcement is delegated to the injected
``AbstractAuthorizer``, which resolves to ``PolicyEngineAuthorizer``
(bridging ``AsyncService`` → ``CasbinPolicyEngine``) when
``enable_policy_authorizer`` is called during bootstrap.

DESIGN: service is thin — no per-method authorization code
    The Casbin integration lives entirely in ``PolicyEngineAuthorizer`` +
    ``RequestMapper``.  ``DocumentService`` adds no custom ``_check_entity``
    or ``_prepare_for_create`` hooks; the RBAC rules in the engine are the
    sole authorization gate.

    ✅ Authz rules are data (policy store), not code — change them at
       runtime via the REST admin router or ``PolicyManagement`` API.
    ✅ ``DocumentService`` stays domain-focused; no auth logic scattered
       through methods.
    ❌ Fine-grained per-document ownership requires ABAC rules in the
       engine model — beyond the scope of this RBAC example.

Thread safety:  ⚠️ Singleton — all methods must be stateless.
Async safety:   ✅ All public methods are ``async def``.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from dtos import DocumentCreate, DocumentRead, DocumentUpdate
from models import Document
from providify import Inject, InjectMeta, Singleton
from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth.base import AbstractAuthorizer
from varco_core.event import AbstractEventProducer
from varco_core.repository import AsyncRepository
from varco_core.service.base import AsyncService, IUoWProvider
from varco_core.uow import AsyncUnitOfWork


@Singleton
class DocumentService(
    AsyncService[Document, UUID, DocumentCreate, DocumentRead, DocumentUpdate],
):
    """
    CRUD service for ``Document`` entities under Casbin RBAC authorization.

    All access-control decisions are delegated to the injected
    ``AbstractAuthorizer``.  When wired with ``PolicyEngineAuthorizer``,
    every ``create``, ``read``, ``update``, and ``delete`` call is
    authorised against the active Casbin policy before touching the store.

    Class injection (via providify):
        - ``uow_provider``  → ``IUoWProvider`` (``InMemoryUoWProvider``)
        - ``authorizer``    → ``AbstractAuthorizer`` (``PolicyEngineAuthorizer``
                              when ``enable_policy_authorizer`` was called;
                              ``BaseAuthorizer`` permissive fallback otherwise —
                              under ``VARCO_SECURITY_ENV=development`` only)
        - ``assembler``     → ``AbstractDTOAssembler[Document, ...]``
                             (``DocumentAssembler``)

    Thread safety:  ⚠️ Singleton — methods must not mutate instance state.
    Async safety:   ✅ All public methods are ``async def``.
    """

    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer: Inject[AbstractAuthorizer],
        assembler: Inject[
            AbstractDTOAssembler[Document, DocumentCreate, DocumentRead, DocumentUpdate]
        ],
        # Optional event producer — no-op when no bus is wired (this example).
        producer: Annotated[AbstractEventProducer, InjectMeta(optional=True)] = None,
    ) -> None:
        """
        Args:
            uow_provider: Injected ``IUoWProvider`` — provided by ``DocumentModule``.
            authorizer:   Injected ``AbstractAuthorizer``.  Resolves to
                          ``PolicyEngineAuthorizer`` when DI is wired with
                          ``enable_policy_authorizer(container)``; otherwise
                          falls back to the permissive ``BaseAuthorizer``
                          (a binding only under
                          ``VARCO_SECURITY_ENV=development`` since 4.0).
            assembler:    Injected ``DocumentAssembler``.
            producer:     Optional ``AbstractEventProducer``.  Resolves to
                          ``None`` in this example (no bus wired) — ``AsyncService``
                          falls back to ``NoopEventProducer`` automatically.
        """
        super().__init__(
            uow_provider=uow_provider,
            authorizer=authorizer,
            assembler=assembler,
            producer=producer,
        )

    def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[Document, UUID]:
        """
        Return the ``AsyncRepository[Document]`` from the open unit-of-work.

        ``InMemoryUoW`` exposes the repository as ``uow.documents``.

        Args:
            uow: The open ``AsyncUnitOfWork`` for this request.

        Returns:
            ``InMemoryDocumentRepository`` bound to the current UoW.
        """
        # The in-memory UoW exposes the repo as a named attribute —
        # no generic lookup needed (no SA registry in this example).
        return uow.documents  # type: ignore[attr-defined]


__all__ = ["DocumentService"]
