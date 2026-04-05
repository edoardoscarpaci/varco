"""
example.router
==============
FastAPI router for the ``Post`` entity.

``PostRouter`` extends ``VarcoCRUDRouter`` with the five standard CRUD
mixins and one custom endpoint (``GET /posts/{post_id}/summary``).

CRUD mixins included
--------------------
- ``CreateMixin``  → ``POST /posts``
- ``ReadMixin``    → ``GET  /posts/{id}``
- ``UpdateMixin``  → ``PUT  /posts/{id}``
- ``PatchMixin``   → ``PATCH /posts/{id}``
- ``DeleteMixin``  → ``DELETE /posts/{id}``
- ``ListMixin``    → ``GET  /posts``

Async mode support
------------------
When ``?with_async=true`` is appended, the router offloads the operation
to ``JobRunner`` and returns ``202 Accepted`` with a ``job_id``.  The
client polls ``GET /jobs/{job_id}`` for the result.  CRUD operations
register named tasks (``PostRouter.create``, ``PostRouter.read``, …) in
``TaskRegistry`` so ``JobRunner.recover()`` can re-submit PENDING jobs
after a process restart.

Custom endpoint — ``GET /posts/{post_id}/summary``
--------------------------------------------------
Demonstrates a non-CRUD ``@route`` handler.  Auth context is injected via
``AbstractServerAuth.get_ctx(request)`` in the handler parameter list —
this is the documented pattern for custom routes (GAP #6 solution).

DESIGN: router is thin — no business logic
    All logic lives in ``PostService`` (DI-injected via ``VarcoCRUDRouter``).
    The router only declares HTTP method, path, and response model.

    ✅ Router can be swapped for a CLI runner or async worker without
       changing the service.
    ✅ ``@route`` decorators are bus-agnostic metadata — no FastAPI imports
       in the service layer.
    ❌ One extra class per entity — necessary to express the routing contract.

Thread safety:  ✅ ClassVars read-only after ``build_router()`` returns.
Async safety:   ✅ ``build_router()`` is synchronous; handlers are async.
"""

from __future__ import annotations

import logging
from uuid import UUID

from varco_fastapi.router.crud import VarcoCRUDRouter
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.mixins import (
    CreateMixin,
    DeleteMixin,
    ListMixin,
    PatchMixin,
    ReadMixin,
    UpdateMixin,
)

from providify import Singleton

from example.dtos import PostCreate, PostRead, PostUpdate
from example.models import Post

_logger = logging.getLogger(__name__)


@Singleton
class PostRouter(
    # Mixin order follows MRO left-to-right.  Each mixin contributes one
    # @route entry via __init_subclass__ — order does not affect behaviour
    # here, but alphabetical-by-concern is conventional in this codebase.
    CreateMixin,
    ReadMixin,
    UpdateMixin,
    PatchMixin,
    DeleteMixin,
    ListMixin,
    VarcoCRUDRouter[Post, UUID, PostCreate, PostRead, PostUpdate],
):
    """
    FastAPI router for ``/posts``.

    Provides the following endpoints:
        POST   /v1/posts               — create a post (async: ?with_async=true)
        GET    /v1/posts/{id}          — fetch a post by UUID
        PUT    /v1/posts/{id}          — full update
        PATCH  /v1/posts/{id}          — partial update
        DELETE /v1/posts/{id}          — delete (async: ?with_async=true)
        GET    /v1/posts               — list with filtering and pagination
        GET    /v1/posts/{id}/summary  — custom endpoint: lightweight summary

    Service injection:
        ``_service`` is injected by providify via ``Inject[AsyncService[...]]``
        in ``VarcoCRUDRouter.__init__``.  The concrete type resolves to
        ``PostService`` because it is bound under the matching generic alias.

    Task recovery:
        ``build_router()`` registers tasks ``PostRouter.create``,
        ``PostRouter.read``, ``PostRouter.update``, ``PostRouter.patch``,
        ``PostRouter.delete`` in ``TaskRegistry`` (if available from DI).
        ``PostRouter.list`` is intentionally omitted (``QueryParams`` is not
        trivially serialisable).

    Thread safety:  ✅ ``_prefix`` and ``_tags`` are read-only ClassVars.
    Async safety:   ✅ All handlers are ``async def``.
    """

    _prefix = "/posts"
    _tags = ["posts"]
    _version = "v1"

    @route("GET", "/{post_id}/summary")
    async def get_summary(self, post_id: UUID) -> dict:
        """
        Lightweight post summary — only ``pk`` and ``title``.

        Demonstrates a custom ``@route`` endpoint alongside standard CRUD.
        This handler is NOT registered in ``TaskRegistry`` (no async recovery).

        Auth context is available via ``self._auth`` on the router — for a
        real handler that needs the caller's identity, inject:

            from varco_fastapi.context import get_request_context
            ctx = get_request_context().auth

        Args:
            post_id: UUID from the path parameter.

        Returns:
            ``{"pk": "<uuid>", "title": "<title>"}``

        Raises:
            404: Post not found (``ServiceNotFoundError`` → HTTP 404 via the
                 error middleware in ``varco_fastapi.middleware.error``).

        Edge cases:
            - This endpoint bypasses the cache — it calls ``_service.get()``
              directly (which internally goes through ``CacheServiceMixin.get``
              and benefits from caching).
            - ``with_async=true`` is NOT supported on custom ``@route`` methods
              unless you explicitly call ``_submit_job()`` here.
        """
        # The service.get() call goes through CacheServiceMixin — cached after
        # the first request for this post_id (within the TTL).
        #
        # Auth context: for a full implementation, resolve the context via:
        #   ctx = get_request_context().auth
        # For this example we pass a minimal auth context.
        from varco_core.auth.base import AuthContext  # noqa: PLC0415

        ctx = AuthContext(user_id="system")

        post = await self._service.get(post_id, ctx)
        return {"pk": str(post.pk), "title": post.title}


__all__ = ["PostRouter"]
