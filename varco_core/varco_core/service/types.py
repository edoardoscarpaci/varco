"""
varco_core.service.types
============================
Type aliases and structural protocols for the service layer.

Two exports:

``Assembler[D, C, R, U]``
    Short TypeAlias for ``AbstractDTOAssembler[D, C, R, U]``.  Reduces
    verbosity in service ``__init__`` signatures — especially useful when
    all four generics are already named by the class-level TypeVars.

``ServiceProtocol[D, PK, C, R, U]``
    Structural ``Protocol`` that describes the full public surface of
    ``AsyncService``.  Use it to type-hint any callable that accepts **a**
    service without coupling to ``AsyncService`` directly (e.g. in generic
    HTTP adapters, event bus adapters, or test doubles).

DESIGN: Protocol over ABC for the service contract
    ✅ Any object with the right methods satisfies ``ServiceProtocol`` —
       including legacy services that don't inherit from ``AsyncService``.
    ✅ Decouples consumers from the concrete base class — HTTP handlers can
       depend on the protocol, keeping the framework boundary thin.
    ✅ Works with ``isinstance()`` checks when ``@runtime_checkable`` is added
       (not included here because async methods in runtime protocols have
       subtle issues before Python 3.12 — add when targeting 3.12+).
    ❌ IDEs may show weaker "go to definition" support than ABC subclassing.

Thread safety:  ✅ Module-level type aliases — no mutable state.
Async safety:   ✅ Pure type-level constructs — no I/O.
"""

from __future__ import annotations

from typing import AsyncIterator, TypeAlias, TypeVar

from varco_core.assembler import AbstractDTOAssembler
from varco_core.auth import AuthContext
from varco_core.dto import CreateDTO, PagedReadDTO, ReadDTO, UpdateDTO
from varco_core.service.base import _ANON_CTX
from varco_core.query.params import QueryParams

# ── TypeVars ──────────────────────────────────────────────────────────────────

# Re-exported so consumers can use them when annotating generic services.
# Bound to the same base classes as AsyncService to maintain consistency.
from varco_core.model import DomainModel

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")
C = TypeVar("C", bound=CreateDTO)
R = TypeVar("R", bound=ReadDTO)
U = TypeVar("U", bound=UpdateDTO)


# ── Assembler alias ───────────────────────────────────────────────────────────

# DESIGN: alias rather than subclass — adds zero overhead, just improves
# readability at call sites by dropping 8 characters of prefix ("Abstract").
Assembler: TypeAlias = AbstractDTOAssembler


# ── ServiceProtocol ───────────────────────────────────────────────────────────

# Import Protocol here — no built-in replacement available in typing
from typing import Protocol  # noqa: E402  (kept together with ServiceProtocol)


class ServiceProtocol(Protocol[D, PK, C, R, U]):  # type: ignore[misc]
    """
    Structural protocol describing the full public API of ``AsyncService``.

    Any object that implements all five CRUD methods plus ``paged_list``
    satisfies this protocol, regardless of its inheritance hierarchy.

    Type parameters:
        D   — ``DomainModel`` subclass (e.g. ``Post``)
        PK  — Primary key type (e.g. ``int``, ``UUID``)
        C   — ``CreateDTO`` subclass
        R   — ``ReadDTO`` subclass
        U   — ``UpdateDTO`` subclass

    Usage::

        # HTTP adapter depends on the protocol — not the base class
        async def list_handler(
            service: ServiceProtocol[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO],
            params: QueryParams,
            ctx: AuthContext,
        ) -> list[PostReadDTO]:
            return await service.list(params, ctx)

    Thread safety:  ✅ Protocol class — no instance state.
    Async safety:   ✅ All methods are ``async def`` — callers must await.

    Edge cases:
        - Not ``@runtime_checkable`` — ``isinstance()`` checks will raise
          ``TypeError``.  Add ``@runtime_checkable`` if needed, but note
          that async-method structural checks have limitations before 3.12.
        - ``paged_list`` raw_query parameter is keyword-only — implementors
          must honour the same signature to satisfy the protocol.
    """

    async def get(self, pk: PK, ctx: AuthContext = _ANON_CTX) -> R:
        """
        Fetch a single entity by primary key.

        Args:
            pk:  Primary key to look up.
            ctx: Caller's identity and grants.

        Returns:
            The ``ReadDTO`` for the found entity.

        Raises:
            ServiceNotFoundError:      No entity with ``pk`` exists.
            ServiceAuthorizationError: Caller lacks READ permission.
        """
        ...

    async def list(self, params: QueryParams, ctx: AuthContext = _ANON_CTX) -> list[R]:
        """
        Return all entities matching ``params``.

        Args:
            params: Filter, sort, and pagination parameters.
            ctx:    Caller's identity and grants.

        Returns:
            Ordered list of ``ReadDTO`` objects (empty if no match).

        Raises:
            ServiceAuthorizationError: Caller lacks LIST permission.
        """
        ...

    async def count(self, params: QueryParams, ctx: AuthContext = _ANON_CTX) -> int:
        """
        Count entities matching ``params`` without fetching them.

        Args:
            params: Filter parameters (sort/pagination ignored for counting).
            ctx:    Caller's identity and grants.

        Returns:
            Total number of matching entities.

        Raises:
            ServiceAuthorizationError: Caller lacks LIST permission.
        """
        ...

    async def paged_list(
        self,
        params: QueryParams,
        ctx: AuthContext = _ANON_CTX,
        *,
        raw_query: str | None = None,
    ) -> PagedReadDTO[R]:
        """
        Return a page of results bundled with the total count.

        Args:
            params:    Filter, sort, and pagination parameters.
            ctx:       Caller's identity and grants.
            raw_query: Original raw query string for cursor construction.
                       ``None`` omits the ``raw_query`` field from the envelope.

        Returns:
            ``PagedReadDTO`` envelope with items and total count.

        Raises:
            ServiceAuthorizationError: Caller lacks LIST permission.
        """
        ...

    async def create(self, dto: C, ctx: AuthContext = _ANON_CTX) -> R:
        """
        Create a new entity from ``dto``.

        Args:
            dto: Validated create payload.
            ctx: Caller's identity and grants.

        Returns:
            The ``ReadDTO`` of the persisted entity.

        Raises:
            ServiceConflictError:      Entity with same unique key already exists.
            ServiceAuthorizationError: Caller lacks CREATE permission.
        """
        ...

    async def update(self, pk: PK, dto: U, ctx: AuthContext = _ANON_CTX) -> R:
        """
        Apply ``dto`` to the entity identified by ``pk``.

        Args:
            pk:  Primary key of the entity to update.
            dto: Validated update payload (partial updates supported via
                 ``UpdateOperation``).
            ctx: Caller's identity and grants.

        Returns:
            The ``ReadDTO`` of the updated entity.

        Raises:
            ServiceNotFoundError:      No entity with ``pk`` exists.
            ServiceAuthorizationError: Caller lacks UPDATE permission.
        """
        ...

    async def delete(self, pk: PK, ctx: AuthContext = _ANON_CTX) -> None:
        """
        Remove the entity identified by ``pk``.

        For ``SoftDeleteService`` subclasses this sets ``deleted_at``
        instead of physically removing the row.

        Args:
            pk:  Primary key of the entity to delete.
            ctx: Caller's identity and grants.

        Returns:
            ``None`` on success.

        Raises:
            ServiceNotFoundError:      No entity with ``pk`` exists.
            ServiceAuthorizationError: Caller lacks DELETE permission.
        """
        ...

    async def exists(self, pk: PK, ctx: AuthContext = _ANON_CTX) -> bool:
        """
        Return ``True`` if an entity with ``pk`` exists in the backing store.

        Does not apply service-layer hooks such as ``_check_entity`` — reports
        raw backing-store presence.  Authorization uses ``Action.READ`` at
        collection level.

        Args:
            pk:  Primary key to probe.
            ctx: Caller's identity and grants.

        Returns:
            ``True`` if the record exists; ``False`` otherwise.

        Raises:
            ServiceAuthorizationError: Caller lacks READ permission.
        """
        ...

    def stream(
        self,
        params: QueryParams,
        ctx: AuthContext,
    ) -> AsyncIterator[R]:
        """
        Yield ``ReadDTO``\\s one at a time without loading all results.

        Respects the same authorization and ``_scoped_params`` hooks as
        ``list()``.  Intended for large result sets.

        Args:
            params: Filter, sort, and pagination parameters.
            ctx:    Caller's identity and grants.

        Returns:
            ``AsyncIterator[R]`` — consume with ``async for dto in stream:``.

        Raises:
            ServiceAuthorizationError: Caller lacks LIST permission.
        """
        ...


# ── Public re-exports ─────────────────────────────────────────────────────────

__all__ = [
    "Assembler",
    "ServiceProtocol",
    # TypeVars — re-exported for consumers building generic service annotations
    "D",
    "PK",
    "C",
    "R",
    "U",
]
