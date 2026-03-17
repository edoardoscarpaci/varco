"""
fastrest_core.assembler
========================
Abstract DTO assembler — translates between domain entities and DTOs.

The assembler is the **only** layer responsible for:

- Mapping a ``CreateDTO`` → fresh ``DomainModel`` (INSERT path).
- Mapping a persisted ``DomainModel`` → ``ReadDTO`` (response path).
- Applying an ``UpdateDTO`` to an existing entity → updated entity (UPDATE path).

DESIGN: one combined assembler over three separate classes
    ✅ All three mappings concern the same (D, C, R, U) quartet — they are
       inherently cohesive and always deployed together for a given entity.
    ✅ One DI binding per entity type instead of three — simpler container.
    ✅ Consistent with the existing ``AbstractMapper[D, O]`` pattern in
       this codebase (one class handles both translation directions).
    ❌ ``to_read_dto`` cannot be extracted for reuse in a read-only context
       (e.g. a search service) without referencing the full assembler.
       If that use case arises, extract a separate ``AbstractReadAssembler``
       at that point — don't pre-optimise now.

DESIGN: assembler is a separate injectable class, not methods on the service
    ✅ Service stays focused on orchestration and authorization — no field-
       level mapping code inside the service.
    ✅ Assembler can be injected via ``Inject[AbstractDTOAssembler]`` and
       swapped in tests without touching the service.
    ✅ Same assembler can be reused across multiple consumers (REST handler,
       CLI, background job) without duplicating mapping logic.
    ❌ One extra class per entity — justified by the clear SRP benefit.

Type parameters::

    D — DomainModel subclass (e.g. ``Post``)
    C — CreateDTO subclass  (e.g. ``CreatePostDTO``)
    R — ReadDTO subclass    (e.g. ``PostReadDTO``)
    U — UpdateDTO subclass  (e.g. ``UpdatePostDTO``)

Usage::

    class PostAssembler(AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]):

        def to_domain(self, dto: CreatePostDTO) -> Post:
            return Post(title=dto.title, body=dto.body)

        def to_read_dto(self, entity: Post) -> PostReadDTO:
            return PostReadDTO(
                id=str(entity.pk),
                title=entity.title,
                body=entity.body,
                created_at=entity.created_at,
                updated_at=entity.updated_at,
            )

        def apply_update(self, entity: Post, dto: UpdatePostDTO) -> Post:
            from dataclasses import replace
            return replace(
                entity,
                title=dto.title if dto.title is not None else entity.title,
                body=dto.body   if dto.body  is not None else entity.body,
            )

Thread safety:  ✅ Implementations must be stateless — safe to share as singleton.
Async safety:   ✅ All methods are synchronous — no I/O or awaiting.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from fastrest_core.dto import CreateDTO, ReadDTO, UpdateDTO
from fastrest_core.model import DomainModel

D = TypeVar("D", bound=DomainModel)
C = TypeVar("C", bound=CreateDTO)
R = TypeVar("R", bound=ReadDTO)
U = TypeVar("U", bound=UpdateDTO)


class AbstractDTOAssembler(ABC, Generic[D, C, R, U]):
    """
    Abstract DTO assembler for a single entity/DTO quartet (D, C, R, U).

    Concrete subclasses implement three translation methods that cover the
    full CRUD lifecycle for one entity type:

    - ``to_domain``    — ``CreateDTO`` → fresh ``DomainModel`` (INSERT)
    - ``to_read_dto``  — persisted ``DomainModel`` → ``ReadDTO`` (response)
    - ``apply_update`` — ``UpdateDTO`` applied to an existing entity (UPDATE)

    Thread safety:  ✅ Implementations must be stateless — safe to share as
                       a singleton across requests and coroutines.
    Async safety:   ✅ All methods are synchronous (pure transformations,
                       no I/O).

    Edge cases:
        - ``to_domain`` must return an *unpersisted* entity
          (``entity._raw_orm is None``) — the repository INSERT path
          detects ``_raw_orm is None`` to distinguish INSERT from UPDATE.
        - ``apply_update`` must return a *new* entity — never mutate the
          input.  ``dataclasses.replace(entity, ...)`` copies ``_raw_orm``
          automatically (it is a dataclass field) so the repository still
          treats the result as an UPDATE.
        - ``to_read_dto`` is only called on *persisted* entities — ``pk``,
          ``created_at``, and ``updated_at`` are always populated.
    """

    @abstractmethod
    def to_domain(self, dto: C) -> D:
        """
        Map a ``CreateDTO`` to a fresh, unpersisted domain entity.

        The returned entity must have ``_raw_orm is None`` so that
        ``repo.save()`` performs an INSERT rather than an UPDATE.

        Args:
            dto: Validated ``CreateDTO`` payload from the HTTP adapter.

        Returns:
            A new, unpersisted ``DomainModel`` instance ready for
            ``repo.save()``.

        Raises:
            ServiceValidationError: Business rule violated by ``dto`` that
                                    Pydantic could not catch (e.g. a value
                                    that conflicts with a domain invariant).

        Edge cases:
            - Do NOT call ``repo.save()`` here — the service handles persistence.
            - Set entity default values here (e.g. ``is_active=True``),
              not in the DTO — keeps the DTO focused on the API contract.
            - Auto-generated fields (``pk``, ``created_at``, ``updated_at``)
              must NOT be set here — the mapper / repository manages them.

        Example::

            def to_domain(self, dto: CreatePostDTO) -> Post:
                return Post(title=dto.title, body=dto.body, is_published=False)
        """

    @abstractmethod
    def to_read_dto(self, entity: D) -> R:
        """
        Map a persisted domain entity to a ``ReadDTO``.

        Called after every ``save()``, ``find_by_id()``, or
        ``find_by_query()`` to produce the value returned to the caller.

        Args:
            entity: A persisted ``DomainModel`` (``entity.is_persisted()``
                    is ``True`` — ``pk``, ``created_at``, ``updated_at``
                    are all populated).

        Returns:
            A fully populated ``ReadDTO`` instance.

        Edge cases:
            - ``entity.pk`` may be any type (``UUID``, ``int``, ``str``).
              Always cast to ``str`` for ``ReadDTO.id``.
            - ``entity.created_at`` / ``entity.updated_at`` are ``None``
              on entities that do not extend ``AuditedDomainModel`` — handle
              gracefully if your ``ReadDTO`` declares these as required fields.

        Example::

            def to_read_dto(self, entity: Post) -> PostReadDTO:
                return PostReadDTO(
                    id=str(entity.pk),
                    title=entity.title,
                    body=entity.body,
                    created_at=entity.created_at,
                    updated_at=entity.updated_at,
                )
        """

    @abstractmethod
    def apply_update(self, entity: D, dto: U) -> D:
        """
        Apply ``dto`` to ``entity`` and return the updated entity.

        Must return a **new** entity — never mutate ``entity`` in place.
        Use ``dataclasses.replace(entity, field=value)`` to produce a copy
        that preserves ``_raw_orm`` (inherited automatically from ``entity``
        because it is a dataclass field) so the repository performs an
        UPDATE rather than an INSERT.

        Args:
            entity: The current, persisted state of the entity before the
                    update is applied.
            dto:    The ``UpdateDTO`` describing what fields to change.

        Returns:
            A new ``DomainModel`` instance with the update applied.
            ``_raw_orm`` must be inherited from ``entity``.

        Raises:
            ServiceValidationError: Business rule violated by the combination
                                    of the current entity state and the dto
                                    values (e.g. invalid state transition).
            ServiceConflictError:   The requested change is not allowed in
                                    the current entity state.

        Edge cases:
            - Fields set to ``None`` in ``dto`` conventionally mean
              "no change" — check each field before replacing.
            - Honour ``dto.op`` (REPLACE / EXTEND / REMOVE / MERGE) for
              collection and dict fields where partial updates are supported.

        Example::

            def apply_update(self, entity: Post, dto: UpdatePostDTO) -> Post:
                from dataclasses import replace
                return replace(
                    entity,
                    title=dto.title if dto.title is not None else entity.title,
                    body=dto.body   if dto.body  is not None else entity.body,
                )
        """
