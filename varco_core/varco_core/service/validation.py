"""
varco_core.service.validation
==============================
``ValidatorServiceMixin`` — automatic business-invariant validation for
``AsyncService`` subclasses via a DI-injected ``Validator[D]``.

Like ``CacheServiceMixin``, this mixin is composed via Python's MRO — it does
NOT wrap the service, it *is* the service.  Override ``_validate_entity`` is
the integration point: the mixin intercepts the call added to ``AsyncService``
and delegates to the injected ``Validator[D]``.

DI integration
--------------
``_validator_entity`` is declared as a ``ClassVar[Inject[T]]`` so the
providify container sets it automatically — subclasses require **zero extra
``__init__`` parameters** beyond what ``AsyncService`` already needs::

    @Singleton
    class PostService(
        ValidatorServiceMixin[Post, UUID, CreatePostDTO, PostReadDTO, UpdatePostDTO],
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

    from varco_core.validation import DomainModelValidator, ValidationResult

    @Singleton
    class PostValidator(DomainModelValidator[Post]):
        def validate(self, value: Post) -> ValidationResult:
            result = ValidationResult.ok()
            if not value.title.strip():
                result += ValidationResult.error("title must not be blank", field="title")
            return result

    container.bind(Validator[Post], PostValidator)
    # PostService._validator_entity is now automatically PostValidator.

Qualifier-based selection
--------------------------
To use a specific named validator binding for a particular service, redeclare
``_validator_entity`` with an ``InjectMeta`` qualifier on the subclass::

    from typing import Annotated, ClassVar
    from providify import Inject, InjectMeta

    class PostService(ValidatorServiceMixin, AsyncService[...]):
        _validator_entity: ClassVar[
            Annotated[Validator[Post], InjectMeta(qualifier="strict")]
        ]

Optional injection (no validator registered)
---------------------------------------------
``_validator_entity`` defaults to ``None`` via ``InjectMeta(optional=True)``.
When no ``Validator[D]`` is registered in the DI container, validation is
silently skipped — no ``if validator is not None`` guards in user code::

    # Container with no Validator[Post] binding → PostService.create() works
    # normally without any validation step.  Inject a validator later without
    # changing PostService at all.

MRO composition
---------------
This mixin MUST appear first in the MRO so ``super()._validate_entity()``
threads through to ``AsyncService._validate_entity()`` (the no-op base)::

    class PostService(ValidatorServiceMixin, SoftDeleteService, AsyncService[...]): ...

``ValidatorServiceMixin._validate_entity()`` calls
``super()._validate_entity()`` which threads through any other mixin that
overrides it (e.g. a future ``AuditValidatorMixin``) and eventually reaches
the base no-op.

Thread safety:  ⚠️ Same contract as ``AsyncService`` singleton.
Async safety:   ✅ ``_validate_entity`` is synchronous — ``Validator[D].validate()``
                   must not block the event loop.  For async validation (DB
                   uniqueness checks, etc.) override ``_validate_entity`` directly
                   and ``await`` your async validator there.
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


class ValidatorServiceMixin(AsyncService[D, PK, C, R, U], Generic[D, PK, C, R, U]):
    """
    ``AsyncService`` mixin that runs a DI-injected ``Validator[D]`` before
    every ``create`` and ``update`` persist call.

    **DI wiring** — ``_validator_entity`` is declared as
    ``ClassVar[Annotated[Validator[D], InjectMeta(optional=True)]]`` so
    providify resolves the dependency automatically at the class level.
    Subclasses need no extra ``__init__`` parameters.

    Class attribute:

    - ``_validator_entity`` — resolved by the DI container to the registered
      ``Validator[D]`` singleton (or ``None`` if none is registered).

    Override ``_validator_entity`` on the subclass to change the qualifier or
    make the injection required (see module docstring for examples).

    DESIGN: ``_validator_entity`` name (not ``_validator``)
        ``_validator`` conflicts with ``unittest.mock.patch`` internal
        attributes and with common user-defined names.  The ``_entity``
        suffix scopes the name to the domain entity validator — consistent
        with ``_cache_namespace`` and ``_cache_ttl`` on ``CacheServiceMixin``.
        ✅ No naming collision with user code.
        ✅ Clear intent: this is the domain entity validator, not a generic one.

    DESIGN: optional injection via ``InjectMeta(optional=True)``
        ✅ Services work without a validator registered — safe default.
        ✅ Validators can be added later without changing service code.
        ✅ Consistent with how ``_producer`` is optional in ``AsyncService``.
        ❌ A missing validator registration is silent — use ``_require_validator``
           = ``True`` if you want an error at container startup.

    Thread safety:  ⚠️ Singleton — ``_validator_entity`` is a class-level ref
                       shared across all concurrent requests.  The validator
                       itself must be stateless.
    Async safety:   ✅ ``_validate_entity`` is synchronous.  For async
                       validation, override ``_validate_entity`` with
                       ``async def`` is not possible here — override the whole
                       method and call your async validator with ``await``.
    """

    # ── DI-injected class attribute ────────────────────────────────────────────
    # The providify container sets this at the class level — all instances of a
    # subclass see the same injected singleton without any __init__ change.
    #
    # ``optional=True`` → resolves to ``None`` when no Validator[D] is bound.
    # This mirrors how ``AsyncService._producer`` defaults to None.
    #
    # To require the validator (raise at container startup if not registered):
    #   _validator_entity: ClassVar[Inject[Validator[D]]]  # no optional
    #
    # To use a specific named binding:
    #   _validator_entity: ClassVar[Annotated[Validator[D], InjectMeta(qualifier="strict")]]

    #: Resolved by the DI container — the registered ``Validator[D]`` singleton.
    #: ``None`` when no ``Validator[D]`` is registered in the container.
    _validator_entity: ClassVar[  # type: ignore[misc]
        Annotated[Any, InjectMeta(optional=True)]
    ] = None

    # ── Extension hook override ────────────────────────────────────────────────

    def _validate_entity(self, entity: D, ctx: AuthContext) -> None:
        """
        Delegate to the injected ``Validator[D]`` (if any), then chain to super.

        Called by ``AsyncService.create()`` after ``_prepare_for_create`` and
        by ``AsyncService.update()`` after ``assembler.apply_update()``.  The
        entity is fully populated (stamped with cross-cutting fields) at this
        point.

        When ``_validator_entity`` is ``None`` (no validator registered),
        validation is silently skipped — consistent with the Null Object pattern
        used throughout ``varco_core`` (``NoopEventProducer``, ``NoopEventBus``).

        Args:
            entity: The fully-assembled domain entity to validate.
            ctx:    Caller's identity (passed through for chaining; the
                    validator itself does not receive ``ctx`` — it validates
                    the entity state only).

        Raises:
            ServiceValidationError: The injected validator found one or more
                                    violations.  The error message includes all
                                    collected violations (collect-all semantics).

        Edge cases:
            - No validator registered → method is a no-op.
            - Validator returns ``ValidationResult.ok()`` → no exception raised.
            - Validator raises an unexpected exception (bug in validator) → it
              propagates up and the UoW transaction is rolled back automatically.
            - ``super()._validate_entity(entity, ctx)`` is always called so
              other mixins in the MRO (e.g. a future ``AuditValidatorMixin``)
              compose correctly via MRO co-operative inheritance.

        Thread safety:  ✅ ``_validator_entity`` is a class-level singleton —
                           no instance mutation.
        Async safety:   ✅ Synchronous — does not yield the event loop.
        """
        validator = self._validator_entity  # read ClassVar once — no per-call overhead

        if validator is not None:
            _logger.debug(
                "ValidatorServiceMixin[%s]: validating with %r.",
                type(entity).__name__,
                validator,
            )
            # validate() collects all errors; raise_if_invalid() raises
            # ServiceValidationError with the full set of violations at once.
            validator.validate(entity).raise_if_invalid()

        # Always chain — other mixins in the MRO may override _validate_entity.
        super()._validate_entity(entity, ctx)  # type: ignore[misc]


__all__ = ["ValidatorServiceMixin"]
