"""
varco_core.service.async_validation
=====================================
``AsyncValidatorServiceMixin`` — DI-injected async validator for
``AsyncService`` subclasses.

Mirrors ``ValidatorServiceMixin`` exactly, but overrides
``_validate_entity_async`` (not ``_validate_entity``) so it integrates with
validators that need I/O — database uniqueness checks, remote API calls, etc.

DI integration
--------------
``_async_validator_entity`` is declared as a ``ClassVar[Inject[T]]`` so the
providify container sets it automatically — subclasses require **zero extra
``__init__`` parameters** beyond what ``AsyncService`` already needs::

    @Singleton
    class PostService(
        AsyncValidatorServiceMixin[Post, UUID, CreatePostDTO, PostReadDTO, UpdatePostDTO],
        AsyncService[Post, UUID, CreatePostDTO, PostReadDTO, UpdatePostDTO],
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

Register the validator once and it is injected automatically::

    from varco_core.validation import AsyncDomainModelValidator, ValidationResult

    @Singleton
    class UniqueSlugValidator(AsyncDomainModelValidator[Post]):
        def __init__(self, repo: PostRepository) -> None:
            self._repo = repo

        async def validate(self, value: Post) -> ValidationResult:
            if await self._repo.slug_exists(value.slug):
                return ValidationResult.error("slug already in use", field="slug")
            return ValidationResult.ok()

    container.bind(AsyncValidator[Post], UniqueSlugValidator)
    # PostService._async_validator_entity is now automatically UniqueSlugValidator.

Combining sync and async validators
-------------------------------------
Both mixins can be composed — order matters for MRO::

    class PostService(
        AsyncValidatorServiceMixin,   # ← async hook (runs second)
        ValidatorServiceMixin,        # ← sync hook  (runs first)
        AsyncService[...],
    ):
        ...

    # AsyncValidatorServiceMixin.super()._validate_entity_async → base no-op
    # ValidatorServiceMixin.super()._validate_entity        → base no-op

MRO composition
---------------
This mixin MUST appear left of ``AsyncService`` in the class hierarchy.
When combined with ``ValidatorServiceMixin``, order them left-to-right as
desired — they override different hooks and do not interfere.

Thread safety:  ⚠️ Same contract as ``AsyncService`` singleton.
Async safety:   ✅ ``_validate_entity_async`` is a coroutine.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Generic, TypeVar

from providify import InjectMeta

from varco_core.dto import CreateDTO, ReadDTO, UpdateDTO
from varco_core.model import DomainModel
from varco_core.service.base import AsyncService

if TYPE_CHECKING:
    from varco_core.auth import AuthContext

_logger = logging.getLogger(__name__)

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")
C = TypeVar("C", bound=CreateDTO)
R = TypeVar("R", bound=ReadDTO)
U = TypeVar("U", bound=UpdateDTO)


class AsyncValidatorServiceMixin(AsyncService[D, PK, C, R, U], Generic[D, PK, C, R, U]):
    """
    ``AsyncService`` mixin that runs a DI-injected ``AsyncValidator[D]``
    before every ``create`` and ``update`` persist call.

    Integration point: overrides ``_validate_entity_async`` — the async hook
    in ``AsyncService`` for I/O-bound validation.  The sync
    ``_validate_entity`` hook is NOT touched — both can coexist.

    **DI wiring** — ``_async_validator_entity`` is declared as
    ``ClassVar[Annotated[AsyncValidator[D], InjectMeta(optional=True)]]`` so
    providify resolves the dependency automatically at the class level.
    Subclasses need no extra ``__init__`` parameters.

    Class attribute:

    - ``_async_validator_entity`` — resolved by the DI container to the
      registered ``AsyncValidator[D]`` singleton (or ``None`` if none is
      registered).

    DESIGN: ``_async_validator_entity`` name (not ``_async_validator``)
        ✅ Parallel naming to ``_validator_entity`` on ``ValidatorServiceMixin``.
        ✅ The ``_entity`` suffix scopes the name to the domain entity validator.
        ✅ No collision with user-defined names or test-library internals.

    DESIGN: optional injection via ``InjectMeta(optional=True)``
        ✅ Services work without any async validator registered — safe default.
        ✅ Consistent with ``ValidatorServiceMixin`` and ``AsyncService._producer``.
        ❌ Missing registration is silent — use required injection if you want
           a container-startup error when no validator is bound.

    Thread safety:  ⚠️ Singleton — ``_async_validator_entity`` is class-level.
                       The validator itself must be stateless or internally
                       thread-safe.
    Async safety:   ✅ ``_validate_entity_async`` is a coroutine.
    """

    # ── DI-injected class attribute ────────────────────────────────────────────
    # Resolved by the providify container at the class level — all instances of
    # a subclass share the same injected singleton without any __init__ change.
    #
    # ``optional=True`` → resolves to ``None`` when no AsyncValidator[D] is bound.
    # Mirrors how ``AsyncService._producer`` defaults to None.
    #
    # To require the validator (raise at container startup if not registered):
    #   _async_validator_entity: ClassVar[Inject[AsyncValidator[D]]]  # no optional
    #
    # To use a specific named binding:
    #   _async_validator_entity: ClassVar[
    #       Annotated[AsyncValidator[D], InjectMeta(qualifier="db")]
    #   ]

    #: DI-injected ``AsyncValidator[D]`` singleton.
    #: ``None`` when no ``AsyncValidator[D]`` is registered in the container.
    _async_validator_entity: ClassVar[  # type: ignore[misc]
        Annotated[Any, InjectMeta(optional=True)]
    ] = None

    # ── Extension hook override ────────────────────────────────────────────────

    async def _validate_entity_async(self, entity: D, ctx: AuthContext) -> None:
        """
        Delegate to the injected ``AsyncValidator[D]`` (if any), then chain.

        Called by ``AsyncService.create()`` and ``AsyncService.update()``
        after ``_validate_entity`` (the sync hook) while the UoW is still
        open.  Async validators can safely query the same session.

        When ``_async_validator_entity`` is ``None`` (no validator registered),
        validation is silently skipped — consistent with the Null Object pattern
        used throughout ``varco_core`` (``NoopEventProducer``, ``NoopEventBus``).

        Args:
            entity: The fully-assembled and stamped domain entity.
            ctx:    Caller's identity (threaded through for MRO chaining; the
                    validator itself receives only the entity).

        Raises:
            ServiceValidationError: The injected validator found one or more
                                    violations.

        Edge cases:
            - No async validator registered → method is a no-op.
            - ``entity._raw_orm`` is ``None`` during ``create`` — async
              validators must not call ``entity.raw()``.
            - ``super()._validate_entity_async(entity, ctx)`` is always called
              so future mixins in the MRO compose correctly.

        Async safety:   ✅ Awaited inside the open UoW.
        """
        validator = self._async_validator_entity  # read ClassVar once

        if validator is not None:
            _logger.debug(
                "AsyncValidatorServiceMixin[%s]: validating with %r.",
                type(entity).__name__,
                validator,
            )
            result = await validator.validate(entity)
            result.raise_if_invalid()

        # Always chain — other mixins in the MRO may override this hook.
        await super()._validate_entity_async(entity, ctx)  # type: ignore[misc]


__all__ = ["AsyncValidatorServiceMixin"]
