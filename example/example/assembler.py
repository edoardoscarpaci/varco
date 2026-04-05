"""
example.assembler
=================
DTO ↔ domain model translations for the ``Post`` entity.

``PostAssembler`` is a stateless singleton — all three mapping methods are
pure functions of their inputs.  It is the **only** place where
field-to-field mapping logic lives; services and repositories never touch
DTO fields directly.

DESIGN: ``author_id`` is NOT set in ``to_domain``
    The ``PostCreate`` DTO intentionally omits ``author_id`` so clients
    cannot forge authorship.  The service stamps it from ``AuthContext.subject``
    via ``PostService._prepare_for_create()`` AFTER ``to_domain`` returns.
    Setting a sentinel value (``UUID(int=0)``) here is safe — it is always
    overwritten before ``save()`` is called.

    ✅ Author identity comes from the JWT, not user input.
    ✅ Assembler stays pure (no ctx dependency) — testable without auth setup.
    ❌ sentinel value looks odd in isolation — mitigated by this comment.

DESIGN: ``created_at`` NOT set in ``to_domain``
    The repository timestamps INSERT operations via the ORM column default
    (``server_default=func.now()`` or similar).  Setting ``created_at`` in
    the assembler would be overridden by the DB anyway and would require
    mocking ``datetime.now`` in tests.

    ✅ Single source of truth for timestamps: the database clock.
    ❌ ``Post.created_at`` is ``None`` until after ``save()`` — callers must
       not read ``created_at`` before persistence.

DESIGN: ``apply_update`` uses ``domain_replace()`` not ``dataclasses.replace()``
    ``domain_replace()`` (``varco_core.model``) is a drop-in for
    ``dataclasses.replace()`` that also copies ``init=False`` fields (``pk``,
    ``_raw_orm``, timestamps) from the original.  On Python ≤ 3.12,
    plain ``dataclasses.replace()`` would reset ``pk`` to ``None``,
    causing the repository to treat the updated entity as a new INSERT.

    ✅ No manual ``object.__setattr__`` in assembler implementations.
    ✅ Future ``init=False`` fields on the domain model are handled automatically.
    ❌ ``domain_replace()`` is specific to ``varco_core`` — external assemblers
       must import it rather than using stdlib directly.

Thread safety:  ✅ Stateless — all methods are pure transformations.
Async safety:   ✅ No I/O — all methods are synchronous.
"""

from __future__ import annotations

from providify import Singleton

from varco_core.assembler import AbstractDTOAssembler
from varco_core.model import domain_replace

from example.dtos import PostCreate, PostRead, PostUpdate
from example.models import Post


@Singleton
class PostAssembler(AbstractDTOAssembler[Post, PostCreate, PostRead, PostUpdate]):
    """
    Assembler for the ``Post`` entity.

    Registered as a ``@Singleton`` so the DI container injects a single shared
    instance into every ``PostService`` instance (which is itself a singleton).

    Thread safety:  ✅ Stateless — safe to share across all concurrent requests.
    Async safety:   ✅ All methods are synchronous.
    """

    def to_domain(self, dto: PostCreate) -> Post:
        """
        Map ``PostCreate`` → fresh, unpersisted ``Post``.

        ``pk`` is left unset (``pk_field(init=False)``) — the repository
        generates the UUID on INSERT via ``PKStrategy.UUID_AUTO``.

        ``author_id`` is left as the sentinel ``UUID(int=0)`` — the service
        MUST call ``_prepare_for_create()`` to stamp the real value from
        ``AuthContext.subject`` before ``save()``.

        ``created_at`` is left as ``None`` — the repository / DB sets it.

        Args:
            dto: Validated ``PostCreate`` payload from the HTTP layer.

        Returns:
            An unpersisted ``Post`` with ``title`` and ``body`` set.
            ``pk``, ``author_id``, and ``created_at`` are placeholders.

        Edge cases:
            - The returned entity has ``_raw_orm is None`` — the repository
              treats this as a new INSERT, not an UPDATE.
            - ``author_id`` MUST be overwritten before ``save()`` is called.
        """
        # Only business fields from the DTO are set here.
        # Cross-cutting fields (author_id) and server-generated fields
        # (pk, created_at) are set by the service hook or the repository.
        return Post(title=dto.title, body=dto.body)

    def to_read_dto(self, entity: Post) -> PostRead:
        """
        Map a persisted ``Post`` → ``PostRead`` response.

        Called after every ``save()``, ``find_by_id()``, and
        ``find_by_query()`` to produce the value returned to the caller.

        Args:
            entity: A persisted ``Post`` with all fields populated.

        Returns:
            A ``PostRead`` with all fields from the entity.

        Edge cases:
            - ``entity.created_at`` is never ``None`` for a persisted entity.
              If somehow it is, the Pydantic validator in ``PostRead`` will
              raise a ``ValidationError`` — surfacing a real data integrity bug.
        """
        return PostRead(
            pk=entity.pk,
            title=entity.title,
            body=entity.body,
            author_id=entity.author_id,
            created_at=entity.created_at,
            # updated_at mirrors created_at when no updates have been applied.
            # The SA repository sets updated_at via an ORM column default
            # (server_onupdate=func.now()); the stub uses created_at as a fallback.
            updated_at=(
                entity.updated_at
                if entity.updated_at is not None
                else entity.created_at
            ),
        )

    def apply_update(self, entity: Post, dto: PostUpdate) -> Post:
        """
        Apply ``PostUpdate`` fields onto ``entity`` and return a new ``Post``.

        Only fields present in ``dto`` (not ``None``) are changed.  Fields
        absent from the PATCH body retain their existing values.

        Uses ``domain_replace()`` (not plain ``dataclasses.replace()``) to
        produce a copy that preserves all ``init=False`` fields (``pk``,
        ``_raw_orm``, ``created_at``, ``updated_at``) on Python ≤ 3.12.
        ``_raw_orm`` being preserved tells the repository this is an UPDATE,
        not an INSERT.

        Args:
            entity: Current persisted state of the post.
            dto:    ``PostUpdate`` payload — ``None`` fields mean "no change".

        Returns:
            A new ``Post`` instance with the update applied and all
            framework-managed fields (``pk``, ``_raw_orm``, timestamps)
            copied from ``entity``.

        Edge cases:
            - Sending ``{}`` (all-None ``PostUpdate``) produces an identical
              copy — a no-op UPDATE query will be issued by the repository.
            - ``author_id`` and ``created_at`` are intentionally NOT updateable
              via this method — they are write-once fields.
        """
        return domain_replace(
            entity,
            # Only overwrite if the client explicitly sent a new value.
            # None means "leave as is" — this is the standard partial-update
            # convention documented in UpdateDTO.
            title=dto.title if dto.title is not None else entity.title,
            body=dto.body if dto.body is not None else entity.body,
        )


__all__ = ["PostAssembler"]
