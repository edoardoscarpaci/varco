"""
varco_fastapi.client.protocol
==============================
Structural ``Protocol`` for all varco client implementations.

``ClientProtocol[R]`` decouples consumers from the concrete httpx-backed
``AsyncVarcoClient`` — any object with matching method signatures satisfies it.

DESIGN: Protocol over ABC
    ✅ Structural typing — mock clients don't need a base class
    ✅ DI-compatible — ``Inject[ClientProtocol[OrderRouter]]`` works via providify
    ✅ Future ``RestClientBuilder`` products satisfy it without inheriting VarcoClient
    ✅ Test doubles are plain classes — no import of httpx or metaclass needed
    ❌ Not ``@runtime_checkable`` — async methods in runtime protocols have issues
       before Python 3.12 (coroutine annotation incompatibility with isinstance)

Thread safety:  ✅ Protocol is a type annotation tool; no runtime state.
Async safety:   ✅ All methods are declared async; callers must await them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, Self, TypeVar

if TYPE_CHECKING:
    pass

# TypeVar for the router type — bound at class-definition time by the metaclass
R_co = TypeVar("R_co", covariant=True)


class ClientProtocol(Protocol[R_co]):
    """
    Structural contract for any varco HTTP client.

    ``AsyncVarcoClient[R]`` satisfies this protocol.  Any custom client or
    test double that implements these methods also satisfies it.

    The generic parameter ``R`` is the ``VarcoRouter`` subclass the client
    targets.  It is covariant here — ``ClientProtocol[OrderRouter]`` is a
    structural supertype of any concrete client for ``OrderRouter``.

    CRUD methods below are the "base shape".  The metaclass on
    ``AsyncVarcoClient`` generates concrete overloads with typed DTOs
    (e.g. ``create(body: CreateOrderDTO) -> OrderReadDTO``).  The protocol
    captures the structural contract without requiring concrete types.

    Args:
        R_co: The ``VarcoRouter`` subclass this client targets.

    Thread safety:  ✅ Protocol defines no state.
    Async safety:   ✅ All CRUD methods are async coroutines.
    """

    async def create(self, body: Any, *, with_async: bool = False) -> Any:
        """
        POST to the router's create endpoint.

        Args:
            body:       Pydantic model or dict to serialize as the request body.
            with_async: If ``True``, adds ``?with_async=true`` and returns a
                        ``JobHandle`` for background job tracking.

        Returns:
            The created resource (deserialized ``ReadDTO``) or a ``JobHandle``.
        """
        ...

    async def read(self, pk: Any) -> Any:
        """
        GET the router's single-resource endpoint.

        Args:
            pk: The primary key value (will be URL-encoded into the path).

        Returns:
            The resource (deserialized ``ReadDTO``).
        """
        ...

    async def update(self, pk: Any, body: Any) -> Any:
        """
        PUT to the router's update endpoint (full replacement).

        Args:
            pk:   The primary key value.
            body: Pydantic model or dict to serialize as the request body.

        Returns:
            The updated resource (deserialized ``ReadDTO``).
        """
        ...

    async def patch(self, pk: Any, body: Any) -> Any:
        """
        PATCH the router's update endpoint (JSON Merge Patch, RFC 7386).

        Args:
            pk:   The primary key value.
            body: Partial Pydantic model or dict — only present fields are updated.

        Returns:
            The patched resource (deserialized ``ReadDTO``).
        """
        ...

    async def delete(self, pk: Any) -> None:
        """
        DELETE the router's single-resource endpoint.

        Args:
            pk: The primary key value.

        Returns:
            ``None`` — 204 No Content response.
        """
        ...

    async def list(
        self,
        *,
        q: str | None = None,
        sort: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        """
        GET the router's list endpoint with optional filtering, sorting, pagination.

        Args:
            q:      Filter expression (e.g. ``"status = 'active' AND total > 100"``).
            sort:   Sort directives (e.g. ``"+created_at,-name"``).
            limit:  Maximum number of results.
            offset: Number of results to skip.

        Returns:
            Paginated response (``PagedReadDTO[ReadDTO]``).
        """
        ...

    async def __aenter__(self) -> Self:
        """Enter async context manager (opens httpx.AsyncClient)."""
        ...

    async def __aexit__(self, *exc: Any) -> None:
        """Exit async context manager (closes httpx.AsyncClient)."""
        ...


__all__ = ["ClientProtocol"]
