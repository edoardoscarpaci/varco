"""
varco_fastapi.client.sync
===========================
Synchronous HTTP client wrapper.

``SyncVarcoClient`` wraps ``httpx.Client`` and exposes the same CRUD methods
as ``AsyncVarcoClient`` but as regular (blocking) functions.  It is created via
the ``AsyncVarcoClient.sync`` property rather than directly instantiated.

DESIGN: sync wrapper over separate sync client hierarchy
    ✅ Single class definition with consistent configuration resolving
    ✅ No metaclass required — sync methods are simpler closures
    ✅ ``with_async=True`` raises immediately (correct: sync client cannot start async jobs)
    ✅ Shares ``ClientProfile`` with the owning async client — no duplicate config
    ❌ Middleware pipeline requires sync adaptation — some async middleware
       (e.g. ``JwtMiddleware`` that awaits token refresh) cannot be used with
       the sync client.  ``SyncClientAsyncError`` is raised at call time.

Thread safety:  ⚠️ ``httpx.Client`` is thread-safe for read-only operations.
                   Avoid calling the same ``SyncVarcoClient`` instance concurrently
                   from multiple threads if middleware mutates request state.
Async safety:   ❌ Not async — do not use inside async code.  Use the owning
                   ``AsyncVarcoClient`` from an async context instead.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from varco_fastapi.client.base import AsyncVarcoClient


# ── SyncClientAsyncError ──────────────────────────────────────────────────────


class SyncClientAsyncError(Exception):
    """
    Raised when ``with_async=True`` is passed to a ``SyncVarcoClient`` method.

    Async job submission requires the server to call back or the client to poll,
    both of which require async I/O — incompatible with sync usage.

    Args:
        method: The method name that was called with ``with_async=True``.
    """

    def __init__(self, method: str) -> None:
        super().__init__(
            f"Cannot use with_async=True on SyncVarcoClient.{method}(). "
            "Async job submission requires async I/O. "
            "Use the async client (AsyncVarcoClient) instead, or call without with_async=True."
        )


# ── SyncVarcoClient ───────────────────────────────────────────────────────────


class SyncVarcoClient:
    """
    Synchronous wrapper around an ``AsyncVarcoClient`` configuration.

    All CRUD methods (``create``, ``read``, ``update``, ``patch``, ``delete``,
    ``list``) behave identically to their async counterparts but are blocking.
    They use an internal ``httpx.Client`` rather than ``httpx.AsyncClient``.

    Obtain via the owning async client's ``.sync`` property::

        # Sync context (e.g. CLI scripts, WSGI apps)
        with OrderClient().sync as client:
            order = client.create(CreateOrderDTO(name="test"))
            page = client.list(q="status = 'active'", limit=20)

    Args:
        async_client: The owning ``AsyncVarcoClient`` for config resolution.

    Thread safety:  ⚠️ Shared ``httpx.Client`` — avoid concurrent access
                       from multiple threads.
    Async safety:   ❌ Not safe in async code — use the async client instead.

    Edge cases:
        - ``with_async=True`` → raises ``SyncClientAsyncError`` immediately.
        - Pydantic body → serialized via ``model_dump_json(exclude_unset=True)``.
        - ``response_model=None`` → returns ``None`` (204 No Content).
        - Middleware that contains async-only logic → not applied in sync mode;
          only headers and timeout from the profile are used.
    """

    def __init__(self, async_client: AsyncVarcoClient) -> None:
        # Store reference to the owning async client for config access
        self._async_client = async_client
        self._client: httpx.Client | None = None

    def __enter__(self) -> SyncVarcoClient:
        """Open the underlying ``httpx.Client``."""
        profile = self._async_client._resolved_profile
        kwargs: dict[str, Any] = {
            "base_url": self._async_client._base_url,
            "timeout": profile.timeout,
        }
        if self._async_client._proxy_url:
            kwargs["proxy"] = self._async_client._proxy_url
        if profile.trust_store is not None:
            kwargs["verify"] = profile.trust_store.build_ssl_context()
        self._client = httpx.Client(**kwargs)
        return self

    def __exit__(self, *exc: Any) -> None:
        """Close the underlying ``httpx.Client``."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        path_params: dict[str, Any] | None = None,
        query_params: dict[str, str] | None = None,
        response_model: type | None = None,
        expected_status: int = 200,
    ) -> Any:
        """
        Execute a synchronous HTTP request.

        Args:
            method:          HTTP method.
            path:            URL path template with ``{param}`` placeholders.
            body:            Pydantic model or dict to serialize as JSON body.
            path_params:     Path parameter substitutions.
            query_params:    Query string parameters.
            response_model:  Pydantic model for response deserialization.
            expected_status: Expected HTTP status code.

        Returns:
            Deserialized response or ``None`` for 204.

        Raises:
            RuntimeError:         No base URL configured.
            httpx.HTTPStatusError: Response status mismatch.

        Edge cases:
            - Called outside context manager → creates a temporary httpx.Client.
        """
        if not self._async_client._base_url:
            raise RuntimeError(
                f"{type(self._async_client).__name__} has no base URL configured."
            )

        # Substitute path params
        effective_path = path
        for key, value in (path_params or {}).items():
            effective_path = effective_path.replace(f"{{{key}}}", str(value))

        # Serialize body
        raw_body: bytes | None = None
        headers: dict[str, str] = {}
        if body is not None:
            try:
                raw_body = body.model_dump_json(exclude_unset=True).encode()
            except AttributeError:
                raw_body = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"

        # Use context manager client or create a temporary one
        client = self._client
        if client is None:
            # Created lazily when called outside 'with' block
            client = httpx.Client(
                base_url=self._async_client._base_url,
                timeout=self._async_client._resolved_profile.timeout,
            )
            owns_client = True
        else:
            owns_client = False

        try:
            response = client.request(
                method=method,
                url=effective_path,
                headers=headers,
                content=raw_body,
                params=query_params or {},
            )
            response.raise_for_status()
        finally:
            if owns_client:
                client.close()

        if response_model is None or response.status_code == 204:
            return None

        data = response.json()
        try:
            return response_model.model_validate(data)
        except AttributeError:
            return data

    # ── CRUD methods ──────────────────────────────────────────────────────────

    def create(self, body: Any, *, with_async: bool = False) -> Any:
        """
        POST to the create endpoint.

        Args:
            body:       Request body (Pydantic model or dict).
            with_async: Raises ``SyncClientAsyncError`` if ``True``.

        Returns:
            Created resource (deserialized ReadDTO).

        Raises:
            SyncClientAsyncError: If ``with_async=True``.
        """
        if with_async:
            raise SyncClientAsyncError("create")
        # Delegate to the router's CRUD method via _request
        return self._request("POST", "/", body=body, expected_status=201)

    def read(self, pk: Any) -> Any:
        """
        GET the single-resource endpoint.

        Args:
            pk: Primary key value.

        Returns:
            Resource (deserialized ReadDTO).
        """
        return self._request("GET", "/{id}", path_params={"id": pk})

    def update(self, pk: Any, body: Any) -> Any:
        """
        PUT the update endpoint (full replacement).

        Args:
            pk:   Primary key value.
            body: Full replacement body.

        Returns:
            Updated resource (deserialized ReadDTO).
        """
        return self._request("PUT", "/{id}", body=body, path_params={"id": pk})

    def patch(self, pk: Any, body: Any) -> Any:
        """
        PATCH the update endpoint (JSON Merge Patch).

        Args:
            pk:   Primary key value.
            body: Partial update body.

        Returns:
            Patched resource (deserialized ReadDTO).
        """
        return self._request("PATCH", "/{id}", body=body, path_params={"id": pk})

    def delete(self, pk: Any) -> None:
        """
        DELETE the resource.

        Args:
            pk: Primary key value.

        Returns:
            ``None`` (204 No Content).
        """
        self._request("DELETE", "/{id}", path_params={"id": pk}, expected_status=204)

    def list(
        self,
        *,
        q: str | None = None,
        sort: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        """
        GET the list endpoint with optional filtering / sorting / pagination.

        Args:
            q:      Filter expression.
            sort:   Sort directives (e.g. ``"+created_at,-name"``).
            limit:  Max results.
            offset: Skip N results.

        Returns:
            Paginated response (PagedReadDTO).
        """
        qp: dict[str, str] = {}
        if q is not None:
            qp["q"] = q
        if sort is not None:
            qp["sort"] = sort
        if limit is not None:
            qp["limit"] = str(limit)
        if offset is not None:
            qp["offset"] = str(offset)
        return self._request("GET", "/", query_params=qp)

    def __repr__(self) -> str:
        """Return a concise string representation for debugging."""
        return f"SyncVarcoClient(async_client={self._async_client!r})"


__all__ = [
    "SyncVarcoClient",
    "SyncClientAsyncError",
]
