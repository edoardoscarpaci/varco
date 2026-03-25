"""
varco_core.validation
======================
Pluggable, composable business-invariant validation for domain models.

This module is intentionally separate from Pydantic's structural validation
(type checking, field presence, format) and from ``ServiceValidationError``'s
raise-immediately contract.  The layer here **collects all errors** across
multiple rules before raising so the caller gets a complete picture of what
went wrong in a single response.

Relationship to the other validation layers::

    ┌──────────────────────────────────────┐
    │  HTTP adapter / API layer            │  ← Pydantic: type, format, presence
    │  CreateDTO / UpdateDTO validators    │
    └──────────────────────────────────────┘
                       ↓
    ┌──────────────────────────────────────┐
    │  Service layer (this module)         │  ← Validator[D]: business invariants
    │  DomainModelValidator / mixin        │    cross-field rules, domain logic
    └──────────────────────────────────────┘
                       ↓
    ┌──────────────────────────────────────┐
    │  Persistence / ORM layer             │  ← FieldHint: DB constraints (not
    │  FieldHint(nullable=False, ...)      │    enforced at Python runtime)
    └──────────────────────────────────────┘

Typical usage — stateless domain invariants::

    from varco_core.validation import DomainModelValidator, ValidationResult

    class OrderValidator(DomainModelValidator[Order]):
        def validate(self, value: Order) -> ValidationResult:
            result = ValidationResult.ok()
            if value.quantity <= 0:
                result += ValidationResult.error(
                    "quantity must be positive", field="quantity"
                )
            if value.end_date <= value.start_date:
                result += ValidationResult.error(
                    "end_date must be after start_date", field="end_date"
                )
            return result

Composing validators::

    from varco_core.validation import CompositeValidator

    validator = CompositeValidator(OrderValidator(), DiscountValidator())
    result = validator.validate(order)
    result.raise_if_invalid()

DI injection via ValidatorServiceMixin::

    @Singleton
    class OrderService(
        ValidatorServiceMixin[Order, UUID, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
        AsyncService[Order, UUID, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
    ):
        _validator_entity: ClassVar[Inject[Validator[Order]]] = OrderValidator()

Thread safety:  ✅ ``ValidationResult`` and ``ValidationError`` are frozen dataclasses
                — safe to share across threads and async tasks.
Async safety:   ✅ All objects here are stateless and immutable after construction.

📚 Docs:
    🐍 https://docs.python.org/3/library/dataclasses.html — frozen=True for immutability
    🐍 https://docs.python.org/3/library/typing.html#typing.Protocol — structural subtyping
    🐍 https://docs.python.org/3/library/typing.html#typing.runtime_checkable — isinstance() support
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Generic, Protocol, TypeVar, runtime_checkable

from varco_core.model import DomainModel

if TYPE_CHECKING:
    pass

T = TypeVar("T")
D = TypeVar("D", bound=DomainModel)


# ── ValidationError ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ValidationError:
    """
    A single field-level or cross-field validation failure.

    Immutable value object — safe to cache, hash, and share.

    Attributes:
        message: Human-readable description of what rule was violated and why.
                 Should be actionable: "quantity must be positive (got -5)".
        field:   Optional field name that caused the violation.
                 ``None`` when the rule spans multiple fields (cross-field rules)
                 or when it applies to the entity as a whole.

    Thread safety:  ✅ Frozen dataclass — immutable after construction.
    Async safety:   ✅ Safe to share across tasks.

    Edge cases:
        - ``field`` is intentionally not validated against the actual model
          fields — the validator owns that knowledge.
        - ``message`` is user-facing; do not include internal identifiers or
          stack traces — those belong in server-side logs.
    """

    message: str
    field: str | None = None


# ── ValidationResult ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ValidationResult:
    """
    An immutable collection of zero or more ``ValidationError``\\s.

    Designed for **collect-all** semantics: run all rules, accumulate errors,
    then call ``raise_if_invalid()`` once.  This gives the caller a complete
    picture of all violations rather than stopping at the first.

    ``ValidationResult.ok()`` — the canonical empty result — is a module-level
    singleton (``VALID``).  Use it as the starting accumulator in a validator::

        result = ValidationResult.ok()
        if condition_a:
            result += ValidationResult.error("rule A violated", field="field_a")
        if condition_b:
            result += ValidationResult.error("rule B violated", field="field_b")
        return result

    DESIGN: ``tuple[ValidationError, ...]`` over ``list[ValidationError]``
        ✅ Frozen dataclass requires hashable fields — tuple is hashable.
        ✅ Immutable — ``__add__`` always produces a new result, never mutates.
        ❌ Slightly more allocation than in-place list append.
        Alternative considered: ``list`` + ``copy()`` — rejected because
        frozen dataclasses prohibit mutable fields and the allocation cost
        is negligible for typical validator sizes (< 20 rules).

    Thread safety:  ✅ Frozen dataclass — immutable and thread-safe.
    Async safety:   ✅ Safe to share across tasks and combine without locking.

    Edge cases:
        - ``errors`` is a ``tuple``, never ``None`` — safe to iterate directly.
        - ``__add__`` on two ``ok()`` results returns ``ok()`` — no allocation.
        - ``raise_if_invalid()`` on ``ok()`` is a no-op — safe to call always.
        - Multiple errors: ``ServiceValidationError`` message joins them all,
          separated by ``"; "``.
    """

    # DESIGN: tuple instead of list — required by frozen=True (must be hashable)
    errors: tuple[ValidationError, ...] = ()

    @property
    def is_valid(self) -> bool:
        """``True`` when no validation errors were collected."""
        return not self.errors

    def __add__(self, other: ValidationResult) -> ValidationResult:
        """
        Combine two results, collecting all errors from both.

        Used to accumulate errors across multiple rules::

            result = ValidationResult.ok()
            result += validator_a.validate(entity)
            result += validator_b.validate(entity)

        Args:
            other: Another ``ValidationResult`` to merge into this one.

        Returns:
            A new ``ValidationResult`` containing all errors from both.

        Edge cases:
            - Adding two ``ok()`` results returns a new empty result — not
              the ``VALID`` singleton — but ``is_valid`` is still ``True``.
        """
        # Short-circuit when one side is empty — avoids tuple concatenation
        if not self.errors:
            return other
        if not other.errors:
            return self
        return ValidationResult(errors=self.errors + other.errors)

    def raise_if_invalid(self) -> None:
        """
        Raise ``ServiceValidationError`` when any errors were collected.

        For a single error, the ``ServiceValidationError`` carries the original
        ``field`` attribute so HTTP adapters can map it to a field-level response.
        For multiple errors, they are joined with ``"; "`` into one message and
        ``field`` is set to ``None`` (the violation spans multiple fields).

        Returns:
            ``None`` — this is a no-op when ``is_valid`` is ``True``.

        Raises:
            ServiceValidationError: One or more validation rules were violated.

        Edge cases:
            - Calling on an ``ok()`` result is safe and returns immediately.
            - Import of ``ServiceValidationError`` is deferred to avoid a
              circular import between ``validation`` and ``exception.service``.
        """
        if self.is_valid:
            return

        # Deferred import — avoids circular dependency between validation.py
        # and exception/service.py which may eventually import from validation.
        from varco_core.exception.service import ServiceValidationError  # noqa: PLC0415

        if len(self.errors) == 1:
            err = self.errors[0]
            raise ServiceValidationError(err.message, err.field)

        # Multiple errors — join into one message; no single field to blame
        joined = "; ".join(
            f"{e.field}: {e.message}" if e.field else e.message for e in self.errors
        )
        raise ServiceValidationError(f"Multiple validation errors: {joined}")

    @staticmethod
    def ok() -> ValidationResult:
        """
        Return the canonical empty (passing) result.

        Returns the module-level ``VALID`` singleton — no allocation on success
        paths.

        Returns:
            The shared ``VALID`` singleton with an empty error tuple.

        Example::

            def validate(self, value: MyModel) -> ValidationResult:
                if some_condition:
                    return ValidationResult.error("rule violated")
                return ValidationResult.ok()  # ← no allocation on happy path
        """
        # Return the singleton — avoids allocating a new empty tuple on every
        # successful validation call (which is the common case).
        return VALID

    @staticmethod
    def error(message: str, *, field: str | None = None) -> ValidationResult:
        """
        Convenience constructor for a single-error result.

        Equivalent to ``ValidationResult(errors=(ValidationError(msg, field),))``,
        but shorter and more readable in validator implementations.

        Args:
            message: Human-readable description of the violated rule.
            field:   Optional field name that triggered the violation.
                     ``None`` for cross-field or entity-level violations.

        Returns:
            A ``ValidationResult`` containing exactly one ``ValidationError``.

        Example::

            if value.quantity <= 0:
                return ValidationResult.error(
                    f"quantity must be positive (got {value.quantity})",
                    field="quantity",
                )
        """
        return ValidationResult(errors=(ValidationError(message=message, field=field),))

    def __repr__(self) -> str:
        if self.is_valid:
            return "ValidationResult(ok)"
        error_strs = [
            f"{e.field}: {e.message}" if e.field else e.message for e in self.errors
        ]
        return f"ValidationResult(errors=[{', '.join(error_strs)!r}])"


# Module-level singleton for the empty "all-pass" result.
# Returned by ValidationResult.ok() so successful validation paths never
# allocate a new object.
VALID: Final[ValidationResult] = ValidationResult()


# ── Validator[T] ──────────────────────────────────────────────────────────────


@runtime_checkable
class Validator(Protocol[T]):
    """
    Structural protocol for anything that can validate a value of type ``T``.

    Any class with a ``validate(value: T) -> ValidationResult`` method satisfies
    this protocol without inheriting from it — consistent with ``Serializer[T]``
    in ``varco_core.serialization``.

    ``@runtime_checkable`` enables ``isinstance(obj, Validator)`` checks at
    runtime (e.g. in ``ValidatorServiceMixin`` guard code).

    DESIGN: Protocol over ABC
        ✅ No inheritance required — third-party validators integrate without
           depending on ``varco_core``.
        ✅ Consistent with ``Serializer[T]`` in ``varco_core.serialization``.
        ✅ ``isinstance(obj, Validator)`` works via ``__runtime_checkable__``.
        ❌ No abstract-method enforcement — a class with the wrong signature
           satisfies the Protocol structurally until called.
        Alternative considered: ABC — rejected because it forces inheritance,
        which breaks the plug-in / zero-dependency use case.

    Thread safety:  ✅ Protocol has no state — implementations are responsible
                    for their own thread safety.
    Async safety:   ✅ No async contract here — see ``AsyncValidator`` if needed.

    Example::

        class AgeValidator:
            def validate(self, value: User) -> ValidationResult:
                if value.age < 0:
                    return ValidationResult.error("age cannot be negative", field="age")
                return ValidationResult.ok()

        isinstance(AgeValidator(), Validator)  # True
    """

    def validate(self, value: T) -> ValidationResult:
        """
        Validate ``value`` and return a result with zero or more errors.

        Args:
            value: The value to validate.

        Returns:
            ``ValidationResult.ok()`` when all rules pass.
            A ``ValidationResult`` with one or more errors otherwise.

        Raises:
            Nothing — validators collect errors rather than raising immediately.
            Callers raise by calling ``result.raise_if_invalid()``.
        """
        ...


# ── CompositeValidator[T] ─────────────────────────────────────────────────────


class CompositeValidator(Generic[T]):
    """
    Fan-out validator that delegates to multiple child validators and collects
    all their errors.

    All child validators always run — there is no short-circuit on the first
    failure.  This ensures the caller receives the full set of violations in a
    single call.

    DESIGN: collect-all over fail-fast
        ✅ One round-trip gives the full picture — e.g. an API response can
           highlight all invalid fields at once rather than one at a time.
        ✅ Each child validator is independent — order does not matter.
        ❌ Slightly more CPU cost than fail-fast — negligible for the typical
           validator count (< 10) and entity sizes in use here.
        Alternative considered: fail-fast with ``any()`` — rejected because
        partial error messages produce a worse user experience.

    Thread safety:  ✅ ``_validators`` is a tuple (immutable after construction).
    Async safety:   ✅ Each ``validate()`` call is synchronous and stateless.

    Example::

        validator = CompositeValidator(
            RangeValidator(),
            UniqueNameValidator(),
            SlugFormatValidator(),
        )
        result = validator.validate(entity)
        result.raise_if_invalid()
    """

    def __init__(self, *validators: Validator[T]) -> None:
        """
        Args:
            validators: One or more ``Validator[T]`` instances to fan-out to.
                        At least one is recommended; an empty composite always
                        returns ``ValidationResult.ok()``.

        Edge cases:
            - Zero validators → ``validate()`` always returns ``ok()``; valid
              as a placeholder when validators are added later via subclassing.
        """
        # Stored as a tuple — immutable after construction so the composite
        # is safe to share across threads and async tasks.
        self._validators: tuple[Validator[T], ...] = validators

    def validate(self, value: T) -> ValidationResult:
        """
        Run all child validators and combine their results.

        Args:
            value: The value to validate.

        Returns:
            Combined ``ValidationResult`` with all errors from all children.
            Returns ``ValidationResult.ok()`` if all children pass or if
            ``_validators`` is empty.

        Edge cases:
            - An empty composite returns ``ok()`` — safe sentinel behaviour.
            - If a child validator raises an unexpected exception, it propagates
              upward — no silent swallowing.  This is intentional: an unexpected
              exception indicates a bug in the validator, not a validation failure.
        """
        # Start with the singleton — no allocation on the common all-pass path.
        result = VALID
        for validator in self._validators:
            # __add__ short-circuits when one side is empty — efficient.
            result = result + validator.validate(value)
        return result

    def __repr__(self) -> str:
        names = ", ".join(type(v).__name__ for v in self._validators)
        return f"CompositeValidator({names})"


# ── DomainModelValidator[D] ───────────────────────────────────────────────────


class DomainModelValidator(ABC, Generic[D]):
    """
    Abstract base class for domain-model business-invariant validators.

    Subclass this to define the business rules for a specific ``DomainModel``
    subclass.  The ``ValidatorServiceMixin`` injects an instance via
    ``ClassVar[Inject[Validator[D]]]`` so no extra ``__init__`` parameters
    are needed on the service.

    This is the **user-facing** base class — it provides a concrete
    ``__repr__``, documents the contract, and satisfies ``Validator[D]``
    structurally (the ``validate`` method).

    DESIGN: ABC over Protocol for the user-facing base
        ✅ ``@abstractmethod`` enforces implementation — easy to catch
           incomplete validators at class-definition time.
        ✅ Provides a concrete ``__repr__`` and optional helper methods
           (``_rule``) that reduce boilerplate in subclasses.
        ✅ Users can subclass ``DomainModelValidator`` and register it in the
           DI container as ``Validator[MyModel]`` — one binding, reused everywhere.
        ❌ Requires inheriting from ``varco_core`` — third-party validators
           should use the ``Validator[T]`` protocol directly.

    Thread safety:  ✅ No shared mutable state in the base class.
                       Subclasses are responsible for their own thread safety.
    Async safety:   ✅ ``validate()`` is synchronous — for async invariants
                       (e.g. "email not already taken") use a stateful validator
                       injected by the service layer and call it explicitly.

    Example::

        from varco_core.validation import DomainModelValidator, ValidationResult

        class PostValidator(DomainModelValidator[Post]):
            MAX_TITLE_LEN: ClassVar[int] = 200

            def validate(self, value: Post) -> ValidationResult:
                result = ValidationResult.ok()
                if not value.title.strip():
                    result += ValidationResult.error(
                        "title must not be blank", field="title"
                    )
                if len(value.title) > self.MAX_TITLE_LEN:
                    result += ValidationResult.error(
                        f"title must be at most {self.MAX_TITLE_LEN} chars "
                        f"(got {len(value.title)})",
                        field="title",
                    )
                if value.published_at and value.published_at < value.created_at:
                    result += ValidationResult.error(
                        "published_at cannot be before created_at",
                        field="published_at",
                    )
                return result
    """

    @abstractmethod
    def validate(self, value: D) -> ValidationResult:
        """
        Check all business invariants for ``value`` and return a result.

        Implement using ``ValidationResult.ok()`` as the accumulator::

            def validate(self, value: Post) -> ValidationResult:
                result = ValidationResult.ok()
                if violation_condition:
                    result += ValidationResult.error("...", field="...")
                return result

        Args:
            value: The fully-assembled domain entity to validate.
                   When called from ``ValidatorServiceMixin``, ``value`` has
                   already passed through ``_prepare_for_create`` so cross-
                   cutting fields (``tenant_id``, ``owner_id``, etc.) are set.

        Returns:
            ``ValidationResult.ok()`` when all invariants hold.
            A result with one or more errors otherwise.

        Raises:
            Nothing — collect errors and return them; never raise directly.
            Call ``result.raise_if_invalid()`` at the call site to raise.

        Edge cases:
            - ``value._raw_orm`` is ``None`` during ``create`` — the entity has
              not yet been persisted.  Do not call ``value.raw()`` here.
            - For ``update``, ``value._raw_orm`` IS set — the entity exists.
              Use ``value.is_persisted()`` to distinguish if needed.
        """

    def _rule(
        self,
        condition: bool,
        message: str,
        *,
        field: str | None = None,
    ) -> ValidationResult:
        """
        Helper: return an error result when ``condition`` is ``True``.

        Reduces boilerplate for simple predicate-based rules::

            result += self._rule(
                value.quantity <= 0,
                f"quantity must be positive (got {value.quantity})",
                field="quantity",
            )

        Args:
            condition: Failure condition — ``True`` means the rule was violated.
            message:   Human-readable description of the violation.
            field:     Optional field name to attach to the error.

        Returns:
            ``ValidationResult.error(message, field=field)`` when ``condition``
            is ``True``; ``ValidationResult.ok()`` otherwise.

        Edge cases:
            - Passing a truthy non-bool (e.g. non-empty string) triggers the
              error — ``condition`` is coerced to bool by the ``if`` statement.
        """
        # Inlining the condition check avoids allocating a ValidationError
        # object on the common (passing) path.
        if condition:
            return ValidationResult.error(message, field=field)
        return VALID

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"


# ── AsyncValidator[T] ─────────────────────────────────────────────────────────


@runtime_checkable
class AsyncValidator(Protocol[T]):
    """
    Structural protocol for validators that require I/O.

    Any class with ``async def validate(value: T) -> ValidationResult``
    satisfies this protocol without inheriting from it.

    Use this protocol when validation must query the database (e.g. uniqueness
    checks) or call an external service.  For pure, stateless rules use the
    synchronous ``Validator[T]`` protocol instead — it is cheaper and easier
    to test.

    ``@runtime_checkable`` enables ``isinstance(obj, AsyncValidator)`` checks.

    DESIGN: separate protocol from ``Validator[T]``
        ✅ ``Validator[T].validate()`` is sync — callers need not ``await``.
        ✅ ``AsyncValidator[T].validate()`` is async — the distinction is
           explicit and enforced at the type level.
        ✅ Both protocols are structural — third-party validators need not
           import ``varco_core`` at all.
        ❌ Two protocols to learn — mitigated by clear naming and docstrings.

    Thread safety:  ✅ Protocol has no state.
    Async safety:   ✅ ``validate()`` is a coroutine.

    Example::

        class UniqueEmailValidator:
            def __init__(self, repo: UserRepository) -> None:
                self._repo = repo

            async def validate(self, value: User) -> ValidationResult:
                if await self._repo.exists_by_email(value.email):
                    return ValidationResult.error("email already taken", field="email")
                return ValidationResult.ok()

        isinstance(UniqueEmailValidator(repo), AsyncValidator)  # True
    """

    async def validate(self, value: T) -> ValidationResult:
        """
        Validate ``value`` asynchronously.

        Args:
            value: The value to validate.

        Returns:
            ``ValidationResult.ok()`` when all rules pass.
            A result with one or more errors otherwise.

        Raises:
            Nothing — collect errors and return; never raise directly.
            Callers raise by calling ``result.raise_if_invalid()``.
        """
        ...


# ── AsyncCompositeValidator[T] ────────────────────────────────────────────────


class AsyncCompositeValidator(Generic[T]):
    """
    Fan-out async validator that delegates to multiple ``AsyncValidator[T]``
    children and collects all their errors concurrently.

    All child validators run concurrently via ``asyncio.gather`` — there is
    no short-circuit on first failure.  This gives the caller a complete
    picture of all violations in one round-trip.

    DESIGN: ``asyncio.gather`` over sequential ``await``
        ✅ All validators run concurrently — total latency is ``max(child_latency)``
           rather than the sum.  Significant when validators hit the same DB.
        ✅ Collect-all semantics — same as ``CompositeValidator``.
        ❌ Validators must not share mutable state — concurrent execution is
           unsafe if they do.  By convention, ``AsyncValidator`` implementations
           are stateless (or use read-only injected repos).

    Thread safety:  ✅ ``_validators`` is a tuple — immutable after construction.
    Async safety:   ✅ ``validate()`` is a coroutine using ``asyncio.gather``.

    Edge cases:
        - Zero validators → returns ``ValidationResult.ok()`` with no I/O.
        - If a child validator raises an unexpected exception, ``asyncio.gather``
          propagates it immediately — no silent swallowing.  This is intentional:
          an unexpected exception indicates a bug in the validator.

    Example::

        validator = AsyncCompositeValidator(
            UniqueEmailValidator(repo),
            UniqueUsernameValidator(repo),
        )
        result = await validator.validate(user)
        result.raise_if_invalid()
    """

    def __init__(self, *validators: AsyncValidator[T]) -> None:
        """
        Args:
            validators: One or more ``AsyncValidator[T]`` instances to fan out to.
                        An empty composite always returns ``ValidationResult.ok()``.
        """
        self._validators: tuple[AsyncValidator[T], ...] = validators

    async def validate(self, value: T) -> ValidationResult:
        """
        Run all child validators concurrently and combine their results.

        Args:
            value: The value to validate.

        Returns:
            Combined ``ValidationResult`` with all errors from all children.

        Edge cases:
            - Empty composite: returns ``ok()`` immediately.
            - If any child raises, ``asyncio.gather`` propagates it.
        """
        if not self._validators:
            return VALID
        results: list[ValidationResult] = await asyncio.gather(
            *(v.validate(value) for v in self._validators)
        )
        combined = VALID
        for r in results:
            combined = combined + r
        return combined

    def __repr__(self) -> str:
        names = ", ".join(type(v).__name__ for v in self._validators)
        return f"AsyncCompositeValidator({names})"


# ── AsyncDomainModelValidator[D] ──────────────────────────────────────────────


class AsyncDomainModelValidator(ABC, Generic[D]):
    """
    Abstract base class for async domain-model business-invariant validators.

    Subclass this when validation requires I/O — e.g. a database uniqueness
    check or an external API call.  For pure, stateless rules use
    ``DomainModelValidator`` instead.

    Satisfies ``AsyncValidator[D]`` structurally.  Register instances in the
    DI container as ``AsyncValidator[D]`` and inject via
    ``AsyncValidatorServiceMixin``.

    DESIGN: parallel ABC to ``DomainModelValidator``
        ✅ Consistent API — same helper methods (``_rule``), same ``__repr__``.
        ✅ Clear separation — sync vs. async validators are distinct types;
           ``isinstance(obj, AsyncValidator)`` distinguishes them at runtime.
        ❌ Two base classes to document — justified by the sync/async split.

    Thread safety:  ✅ No shared mutable state in the base class.
    Async safety:   ✅ ``validate()`` is a coroutine.

    Example::

        from varco_core.validation import AsyncDomainModelValidator, ValidationResult

        class UniqueSlugValidator(AsyncDomainModelValidator[Article]):
            def __init__(self, repo: ArticleRepository) -> None:
                self._repo = repo

            async def validate(self, value: Article) -> ValidationResult:
                exists = await self._repo.slug_exists(value.slug)
                if exists:
                    return ValidationResult.error("slug already in use", field="slug")
                return ValidationResult.ok()
    """

    @abstractmethod
    async def validate(self, value: D) -> ValidationResult:
        """
        Check all business invariants for ``value`` asynchronously.

        Args:
            value: The fully-assembled domain entity to validate.
                   When called from ``AsyncValidatorServiceMixin``, ``value``
                   has already passed through ``_prepare_for_create``.

        Returns:
            ``ValidationResult.ok()`` when all invariants hold.
            A result with one or more errors otherwise.

        Raises:
            Nothing — collect errors and return; never raise directly.

        Edge cases:
            - ``value._raw_orm`` is ``None`` during ``create`` — the entity
              has not yet been persisted.
            - For ``update``, ``value._raw_orm`` IS set.
        """

    def _rule(
        self,
        condition: bool,
        message: str,
        *,
        field: str | None = None,
    ) -> ValidationResult:
        """
        Helper: return an error result when ``condition`` is ``True``.

        Identical to ``DomainModelValidator._rule`` — see that class for docs.
        """
        if condition:
            return ValidationResult.error(message, field=field)
        return VALID

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"


__all__ = [
    "DomainModelValidator",
    "AsyncDomainModelValidator",
    "CompositeValidator",
    "AsyncCompositeValidator",
    "Validator",
    "AsyncValidator",
    "ValidationError",
    "ValidationResult",
    "VALID",
]
