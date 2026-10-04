"""
service.py
==========
Business logic for the patient records entity backed by PostgreSQL.

``PatientService`` extends ``AsyncService[Patient, UUID, PatientCreate,
PatientRead, PatientUpdate]`` and overrides ``_get_repo()`` (the only required
abstract method) to return the ``patient``-keyed repository from the active
unit-of-work.

The service also overrides ``_prepare_for_create()`` to stamp ``created_at``
and ``updated_at`` timestamps before the entity is saved.

DESIGN: timestamp stamping in the service layer, not the assembler
    ✅ The assembler stays pure (no ``datetime.now()`` calls, no UTC imports).
    ✅ Timestamps are available immediately after ``save()`` returns — no
       extra SELECT needed to read server-side ``DEFAULT NOW()``.
    ✅ ``_prepare_for_create`` is called by ``AsyncService.create()`` inside
       the UoW, so timestamps are consistent with the commit time.
    ❌ Timestamps are set in Python, not the DB — millisecond precision
       matches Python's ``datetime.now(UTC)`` resolution (adequate here).

Note on field-level encryption
    Encryption is completely transparent to this service.  The service always
    works with plaintext ``Patient`` objects — the ORM mapper (built by
    ``SAModelFactory.build(Patient, encryptor=...)`` in ``app.py``) handles
    all encrypt-on-write / decrypt-on-read transparently.

Thread safety:  ⚠️ Singleton — methods must be stateless; each call opens
                   its own unit-of-work via ``self._uow_provider.make_uow()``.
Async safety:   ✅ All public methods are ``async def``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from dtos import PatientCreate, PatientRead, PatientUpdate
from models import Patient
from providify import Inject, Singleton
from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth.base import AbstractAuthorizer
from varco_core.service.base import AsyncService, IUoWProvider

if TYPE_CHECKING:
    from varco_core.uow import AsyncUnitOfWork


@Singleton
class PatientService(AsyncService[Patient, UUID, PatientCreate, PatientRead, PatientUpdate]):
    """
    CRUD service for ``Patient`` entities backed by PostgreSQL.

    Inherits the full ``AsyncService`` contract:
    - Authorization via injected ``AbstractAuthorizer`` (permissive, development only).
    - DTO ↔ domain translation via injected ``PatientAssembler``.
    - Unit-of-work management via ``IUoWProvider`` (provided by SAConfiguration).
    - No event publishing in this example — keeps it focused on encryption.

    Thread safety:  ⚠️ Singleton — must be stateless; each request gets its own UoW.
    Async safety:   ✅ All public methods are ``async def``.
    """

    def __init__(
        self,
        uow_provider: Inject[IUoWProvider],
        authorizer: Inject[AbstractAuthorizer],
        # Concrete generic alias so providify resolves the correct PatientAssembler
        # binding (registered under AbstractDTOAssembler[Patient, PatientCreate,
        # PatientRead, PatientUpdate]).
        assembler: Inject[AbstractDTOAssembler[Patient, PatientCreate, PatientRead, PatientUpdate]],
    ) -> None:
        """
        Args:
            uow_provider: Injected ``IUoWProvider`` — provided by ``SAConfiguration``.
            authorizer:   Injected ``AbstractAuthorizer`` — ``BaseAuthorizer``
                          (permissive), bound in ``app.py``. Since 4.0 that
                          class-keyed bind is gated: run with
                          ``VARCO_SECURITY_ENV=development`` (Plan 053 / FLIP-5).
            assembler:    Injected ``PatientAssembler`` — handles DTO ↔ Patient mapping.
        """
        # No producer injection — this example does not publish domain events.
        super().__init__(
            uow_provider=uow_provider,
            authorizer=authorizer,
            assembler=assembler,
        )

    def _get_repo(self, uow: AsyncUnitOfWork):
        """
        Return the ``AsyncRepository[Patient]`` from the open unit-of-work.

        ``SQLAlchemyUnitOfWork`` exposes repositories as attributes named after
        the entity class: ``Patient → uow.patients`` (lowercased + "s").
        The attribute is set by ``SQLAlchemyUnitOfWork._begin()`` from the
        ``repo_factories`` dict built by ``RepositoryProvider.make_uow()``.

        Args:
            uow: The open ``AsyncUnitOfWork`` for this request.

        Returns:
            ``AsyncRepository[Patient]`` backed by the current session.
        """
        return uow.patients  # type: ignore[attr-defined]


__all__ = ["PatientService"]
