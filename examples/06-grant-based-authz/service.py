"""
service
=======
Document service with grant-based and ownership-based authorization.

This module shows authorization enforced **at the service layer** using:

1. ``GrantBasedAuthorizer`` — checks JWT-embedded ``ResourceGrant``s for
   CREATE/LIST operations (type-level key ``"documents"``).
2. ``OwnershipAuthorizer`` — checks ``entity.owner_id == ctx.user_id`` for
   DELETE operations (instance-level ownership).
3. Service hooks:
   - ``_prepare_for_create`` — stamps ``owner_id`` from ``ctx.user_id``.
   - ``_check_entity`` — blocks READ/DELETE for non-owners without admin role.

DESIGN: combined authorizer (GrantBased + Ownership) via subclassing
    We compose both checks in a single ``DocumentAuthorizer`` class rather
    than chaining two separate DI bindings:

    ✅ Explicit dispatch — easy to read which rule applies to which action.
    ✅ Single DI binding — no need to register two authorizers and hope
       they compose.
    ✅ Admin override is inline — ``ctx.has_role("admin")`` checked before
       ownership, so admins can always act regardless of ownership.
    ❌ Less reusable than mixing in pre-built authorizers — acceptable here
       because the composition logic is the point of the example.

DESIGN: ownership check in ``_check_entity`` rather than ``authorize``
    The authorizer's ``authorize()`` is called by the service AFTER
    ``_check_entity``.  For DELETE, we raise ``ServiceNotFoundError`` (not
    ``ServiceAuthorizationError``) in ``_check_entity`` when the caller is
    neither the owner nor an admin.  This prevents existence oracles — a
    non-owner gets a 404 (entity doesn't exist from their perspective), not
    a 403 (entity exists but you're not allowed).

    ✅ Prevents existence-oracle attacks — attacker cannot distinguish
       "not found" from "found but not yours".
    ✅ Consistent with varco's ``_check_entity`` contract (raise
       ``ServiceNotFoundError``, never ``ServiceAuthorizationError``).
    ❌ Callers may be confused by 404 when the document exists but isn't
       theirs — a deliberate security trade-off.

Thread safety:  ⚠️ Singleton — must be stateless.
Async safety:   ✅ All public methods are ``async def``.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from dtos import DocumentCreate, DocumentRead, DocumentUpdate
from models import Document
from providify import Inject, InjectMeta, Singleton
from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth.base import (
    AbstractAuthorizer,
    AuthContext,
)
from varco_core.auth.helpers import GrantBasedAuthorizer
from varco_core.event import AbstractEventProducer
from varco_core.exception.service import ServiceNotFoundError
from varco_core.repository import AsyncRepository
from varco_core.service.base import AsyncService, IUoWProvider
from varco_core.uow import AsyncUnitOfWork

# ── DocumentAuthorizer ────────────────────────────────────────────────────────


@Singleton
class DocumentAuthorizer(GrantBasedAuthorizer):
    """
    Combined grant-based and ownership authorizer for ``Document`` entities.

    Authorization rules:
        CREATE: caller must have ``docs:write`` grant in their JWT.
        LIST:   caller must have ``docs:read`` grant in their JWT.
        READ:   caller must have ``docs:read`` grant (type-level is enough).
        DELETE: owner OR admin role (checked via ``_check_entity`` in the
                service, not here — see module docstring for rationale).

    Registered as a ``@Singleton`` (non-fallback), which always wins over the
    permissive ``@Fallback`` ``BaseAuthorizer`` — itself a binding only under
    ``VARCO_SECURITY_ENV=development`` since 4.0.

    DESIGN: inherits ``GrantBasedAuthorizer`` for the core grant check
        ``GrantBasedAuthorizer.authorize()`` calls ``ctx.can(action, key)``
        where the key is derived from the entity type + optional instance pk.
        We override ``_resource_key`` to use ``"documents"`` (type-level)
        for all operations — instance-level ACL is handled by ownership in
        ``_check_entity``.

        ✅ Reuses the battle-tested ``GrantBasedAuthorizer`` logic.
        ✅ Admin bypass: a ``ResourceGrant("*", frozenset(Action))`` in the
           JWT grants everything, including CREATE — no special-casing needed.
        ❌ No instance-level grant support (e.g. ``"documents:abc123"`` for
           READ) — this example uses ownership instead for simplicity.

    Thread safety:  ✅ Stateless after construction — no mutable state.
    Async safety:   ✅ ``authorize`` reads only the frozen ``AuthContext``.
    """

    def _resource_key(self, entity_type: type, entity: object | None) -> str:
        """
        Return a type-level resource key for all document operations.

        We always use ``"documents"`` (not ``"documents:<pk>"``), so a single
        JWT grant ``ResourceGrant("documents", ...)`` covers all instances.
        Fine-grained instance-level access is managed via ownership in the
        service's ``_check_entity`` hook instead.

        Args:
            entity_type: The domain class (always ``Document`` in this service).
            entity:      The entity instance (ignored — type-level key only).

        Returns:
            ``"documents"`` — the canonical type-level resource key.

        Edge cases:
            - Ignoring the entity means a grant for ``"documents"`` is enough
              to READ any document.  The ownership check in ``_check_entity``
              provides the instance-level guard for DELETE.
        """
        # Type-level key only — instance-level ACLs would require per-pk grants
        # which produce large JWTs.  Ownership is the preferred instance guard.
        return "documents"


# ── DocumentService ───────────────────────────────────────────────────────────


@Singleton
class DocumentService(
    AsyncService[Document, UUID, DocumentCreate, DocumentRead, DocumentUpdate],
):
    """
    CRUD service for ``Document`` entities.

    Authorization is enforced here, not at the HTTP layer:
        - ``_prepare_for_create`` stamps ``owner_id`` from the JWT subject.
        - ``_check_entity``  blocks non-owners from READ and DELETE.
        - ``DocumentAuthorizer.authorize()`` checks JWT grants for all ops.

    Class injection (via providify):
        - ``uow_provider``  → ``IUoWProvider`` (``InMemoryUoWProvider``)
        - ``authorizer``    → ``AbstractAuthorizer`` (``DocumentAuthorizer``)
        - ``assembler``     → ``AbstractDTOAssembler[Document, ...]``
                             (``DocumentAssembler``)

    Thread safety:  ⚠️ Singleton — all methods must be stateless;
                       each call opens its own UoW via ``_uow_provider.make_uow()``.
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
        # Declared here so a bus can be added later without changing __init__.
        producer: Annotated[AbstractEventProducer, InjectMeta(optional=True)] = None,
    ) -> None:
        """
        Args:
            uow_provider: Injected ``IUoWProvider`` — provided by ``DocumentModule``.
            authorizer:   Injected ``AbstractAuthorizer`` — resolves to
                          ``DocumentAuthorizer`` (a non-fallback ``@Singleton``),
                          shadowing the ``@Fallback`` ``BaseAuthorizer``.
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
        return uow.documents  # type: ignore[attr-defined]

    def _prepare_for_create(self, entity: Document, ctx: AuthContext) -> Document:
        """
        Stamp ``owner_id`` from the JWT subject before the entity is saved.

        Called by ``AsyncService.create()`` after the assembler's ``to_domain()``
        and after authorization has passed.  The assembler leaves ``owner_id``
        as ``None`` — this hook fills it from the authenticated caller's subject.

        Args:
            entity: Freshly assembled, unpersisted ``Document``.
            ctx:    Caller's identity — ``ctx.user_id`` is the JWT ``sub`` claim.

        Returns:
            A new ``Document`` with ``owner_id`` set to ``ctx.user_id``.

        Edge cases:
            - Anonymous callers (``ctx.user_id is None``) should be rejected
              by the authorizer before this hook runs.  If they somehow reach
              this point, ``owner_id`` will be ``None`` — the ownership
              authorizer will deny subsequent DELETE attempts.

        Async safety: ✅ Synchronous hook — no I/O.
        """
        # Stamp owner_id from the JWT subject — must happen in the service,
        # not in the assembler (assembler doesn't have access to the JWT).
        object.__setattr__(entity, "owner_id", ctx.user_id)
        # Chain to parent so other mixins in the MRO can also stamp fields.
        return super()._prepare_for_create(entity, ctx)

    def _check_entity(self, entity: Document, ctx: AuthContext) -> None:
        """
        Block non-owners from READ and DELETE access.

        Called by ``AsyncService`` immediately after ``find_by_id()`` for GET,
        UPDATE, and DELETE operations, before the authorizer runs.  We raise
        ``ServiceNotFoundError`` (not ``ServiceAuthorizationError``) to prevent
        existence oracles — a non-owner gets a 404, not a 403.

        Admin override: callers with the ``"admin"`` role bypass ownership.

        Args:
            entity: The document fetched from the repository.
            ctx:    Caller's identity.

        Raises:
            ServiceNotFoundError: Caller is neither the owner nor an admin.

        Edge cases:
            - Anonymous callers (``ctx.user_id is None``) are never the owner
              (``None != owner_id`` for any non-None ``owner_id``).
            - If ``entity.owner_id`` is ``None`` (data integrity issue — should
              not happen after ``_prepare_for_create`` runs), the check raises
              ``ServiceNotFoundError`` for all non-admins as a safe default.
            - Always chain via ``super()`` so other mixins in the MRO run too.

        Async safety: ✅ Synchronous hook — no I/O.
        """
        # Admins bypass the ownership check — they can access any document.
        # DESIGN: role check before ownership
        #   ✅ Admin tokens never need instance-level grants.
        #   ✅ Consistent with the convention in OwnershipAuthorizer docs.
        if ctx.has_role("admin"):
            super()._check_entity(entity, ctx)
            return

        # Non-admins must be the owner — raise 404 (not 403!) to avoid leaking
        # information about the document's existence to unauthorised callers.
        if entity.owner_id != ctx.user_id:
            # ServiceNotFoundError(entity_id, entity_cls) — note argument order.
            # This message is server-side only — never surfaces in the 404
            # response body, which just says "not found".
            raise ServiceNotFoundError(entity.pk, Document)

        # Chain to parent so any further mixin checks can run.
        super()._check_entity(entity, ctx)


__all__ = ["DocumentAuthorizer", "DocumentService"]
