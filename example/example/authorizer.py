"""
example.authorizer
==================
Application-level authorization rules for the Post API.

``PostAuthorizer`` implements ``AbstractAuthorizer`` with the following policy:

+----------+--------+-------------------------------------------------------+
| Role     | Action | Allowed?                                              |
+==========+========+=======================================================+
| admin    | any    | Always ✅ (full access)                               |
+----------+--------+-------------------------------------------------------+
| editor   | list   | ✅ (see all posts)                                    |
| editor   | read   | ✅ (see any post)                                     |
| editor   | create | ✅ (create own posts)                                 |
| editor   | update | ✅ own posts only (``post.author_id == ctx.user_id``) |
| editor   | delete | ✅ own posts only                                     |
+----------+--------+-------------------------------------------------------+
| anonymous| list   | ✅ (read-only public access)                          |
| anonymous| read   | ✅                                                    |
| anonymous| *      | ❌ 403 Forbidden                                      |
+----------+--------+-------------------------------------------------------+

Registration
------------
``PostAuthorizer`` is registered with the DI container via ``@Singleton`` at
normal priority (0), which shadows the default ``BaseAuthorizer`` at priority
``-(2**31)``.  No explicit ``container.bind()`` is needed — the scanner picks
it up automatically.

DESIGN: role-based + ownership checks over pure grant-based authorization
    ✅ Roles (``"admin"``, ``"editor"``) are embedded in the JWT ``roles`` claim
       by the login handler — zero DB round-trips at authorization time.
    ✅ Ownership check (``author_id == user_id``) is a direct field comparison
       on the already-loaded entity — no extra queries.
    ✅ Anonymous read-only access works without authentication tokens.
    ❌ Ownership check requires the entity to be loaded first — the service
       must fetch the post BEFORE calling ``authorizer.authorize()`` for
       UPDATE/DELETE.  ``AsyncService`` already does this in ``update()`` and
       ``delete()`` via ``_check_entity()``.
    Alternative considered: pure ``ctx.can()`` grant checks — grants embedded
    in JWT would cover ownership, but would require issuing a new token every
    time the user creates a post (to add instance-level grants).  Role + ownership
    is simpler and more scalable.

Thread safety:  ✅ Stateless — no instance state beyond the class definition.
Async safety:   ✅ ``authorize`` is ``async def`` — safe to await.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from providify import Singleton

from varco_core.auth.base import AbstractAuthorizer, Action, AuthContext, Resource
from varco_core.exception.service import ServiceAuthorizationError

from example.models import Post

if TYPE_CHECKING:
    pass

_logger = logging.getLogger(__name__)

# ── Role constants ─────────────────────────────────────────────────────────────
# Defined here to avoid magic strings scattered across the authorizer logic.
# Must match the ``roles`` values in ``DEMO_USERS`` (example.auth).
_ROLE_ADMIN = "admin"
_ROLE_EDITOR = "editor"

# Actions that any authenticated user (editor or above) can perform freely.
# LIST and READ are also allowed anonymously (see _check_post_authorization).
_EDITOR_OPEN_ACTIONS: frozenset[Action] = frozenset(
    {Action.LIST, Action.READ, Action.CREATE}
)


@Singleton
class PostAuthorizer(AbstractAuthorizer):
    """
    Authorization gate for ``Post`` entities.

    Dispatches by ``resource.entity_type`` — currently only ``Post`` is
    handled.  Unrecognised entity types are denied (fail-closed posture).

    Thread safety:  ✅ Stateless singleton — no mutable state.
    Async safety:   ✅ No I/O — all checks are pure Python comparisons.

    Edge cases:
        - ``resource.entity`` is ``None`` for collection-level operations
          (CREATE, LIST).  Ownership checks are skipped for these.
        - If the entity has ``author_id=None`` (should never happen after a
          correct ``_prepare_for_create``), ownership checks fail safely — an
          unidentifiable owner is not the same as a matching owner.
        - This authorizer is registered at the default priority (0) and
          shadows ``BaseAuthorizer`` (priority ``-(2**31)``).  If another
          authorizer at priority > 0 is registered, it will shadow THIS
          class — intentional for overriding in tests.
    """

    async def authorize(
        self,
        ctx: AuthContext,
        action: Action,
        resource: Resource,
    ) -> None:
        """
        Assert that ``ctx`` may perform ``action`` on ``resource``.

        Routes to per-entity rule handlers based on ``resource.entity_type``.
        Unknown entity types are denied immediately (fail-closed).

        Args:
            ctx:      Caller identity + roles from the decoded JWT.
            action:   The ``Action`` being attempted (list, read, create, …).
            resource: What is being acted upon.

        Returns:
            ``None`` if allowed.

        Raises:
            ServiceAuthorizationError: The caller lacks permission.

        Edge cases:
            - Admins bypass all checks immediately.
            - Anonymous callers (``ctx.is_anonymous()``) may only LIST and READ.
            - Editors may CREATE freely but may only UPDATE/DELETE their own posts.
        """
        # Route by entity type — extensible to other entities without changing
        # the class structure.  Unknown types are denied (fail-closed posture).
        if resource.entity_type is Post:
            return await self._check_post_authorization(ctx, action, resource)

        # DESIGN: deny-by-default for unknown types.
        # If a new entity is added, the developer MUST add a case here before
        # the authorizer starts allowing operations on it.
        _logger.warning(
            "PostAuthorizer: unknown entity_type=%s — denying action=%s",
            resource.entity_type.__name__,
            action,
        )
        raise ServiceAuthorizationError(str(action), resource.entity_type)

    async def _check_post_authorization(
        self,
        ctx: AuthContext,
        action: Action,
        resource: Resource,
    ) -> None:
        """
        Enforce post-specific authorization rules.

        See module docstring for the full permission table.

        Args:
            ctx:      Caller's auth context.
            action:   Action being attempted.
            resource: Post resource (entity may be ``None`` for collection ops).

        Raises:
            ServiceAuthorizationError: Access denied.

        Edge cases:
            - ``resource.entity`` is ``None`` for LIST and CREATE — ownership
              checks cannot be applied and are skipped.
            - For UPDATE/DELETE on a post with ``author_id=None``, ownership
              check fails (``None != user_id``) — the editor is denied correctly.
        """
        # ── 1. Admins bypass everything ────────────────────────────────────────
        if ctx.has_role(_ROLE_ADMIN):
            return  # ✅ unrestricted

        # ── 2. Anonymous: read-only (list + read) ─────────────────────────────
        if ctx.is_anonymous():
            if action in (Action.LIST, Action.READ):
                return  # ✅ public read access

            # All other actions (create, update, delete) require authentication.
            raise ServiceAuthorizationError(
                f"Authentication required to perform '{action}' on posts.  "
                "Obtain a Bearer token from POST /auth/login.",
                Post,
            )

        # ── 3. Authenticated editors ───────────────────────────────────────────
        if not ctx.has_role(_ROLE_EDITOR):
            # Unknown role — deny rather than guess what access they should have.
            raise ServiceAuthorizationError(
                f"Role '{_ROLE_EDITOR}' or '{_ROLE_ADMIN}' required to "
                f"perform '{action}' on posts.  Current roles: {sorted(ctx.roles)}",
                Post,
            )

        # Editors can list, read, and create without restriction.
        if action in _EDITOR_OPEN_ACTIONS:
            return  # ✅

        # ── 4. Ownership check for UPDATE / DELETE ─────────────────────────────
        # resource.entity is None for collection-level operations (CREATE, LIST)
        # — covered above.  For instance-level ops it is the loaded entity.
        entity: Post | None = resource.entity  # type: ignore[assignment]

        if entity is None:
            # This branch should be unreachable because UPDATE/DELETE always
            # load the entity first.  Guard defensively.
            raise ServiceAuthorizationError(
                f"Cannot perform '{action}' without a loaded entity "
                "(internal error — entity should have been loaded by the service).",
                Post,
            )

        # Compare author_id (UUID) with the caller's user_id (str).
        # str(entity.author_id) normalises the UUID to a lowercase string
        # identical to the form stored in the JWT sub claim.
        caller_owns_post = entity.author_id is not None and str(
            entity.author_id
        ) == str(ctx.user_id)

        if caller_owns_post:
            return  # ✅ editor acting on own post

        raise ServiceAuthorizationError(
            f"Permission denied: you can only '{action}' posts you authored.  "
            f"Post author_id={entity.author_id!s}, your user_id={ctx.user_id!s}.",
            Post,
        )


__all__ = ["PostAuthorizer"]
