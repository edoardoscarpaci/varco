"""
fastrest_core.auth.helpers
============================
Concrete ``AbstractAuthorizer`` implementations for common RBAC patterns.

Three ready-to-use authorizers are provided.  Register the one that fits
your application's security model in the DI container:

``GrantBasedAuthorizer``
    Pure token-based: derives a resource key from the entity type name and
    checks ``ctx.can(action, key)``.  No entity field inspection.
    Best for applications where all permissions are encoded in the JWT.

``OwnershipAuthorizer``
    Token-based for collection ops; entity-field comparison for instance ops.
    Checks ``getattr(entity, owner_field) == ctx.user_id`` for READ / UPDATE
    / DELETE.  Best for user-owned resources (posts, uploads, profiles).

``RoleBasedAuthorizer``
    Checks ``ctx.roles`` against a declarative permission table supplied at
    construction time.  Best for applications with named roles (admin,
    editor, viewer) where permissions are defined once and apply to all
    entity types.

Composing authorizers
---------------------
None of these are mutually exclusive.  Layer them by subclassing::

    class MyAuthorizer(OwnershipAuthorizer):
        async def authorize(self, ctx, action, resource):
            # Layer 1: ownership check
            await super().authorize(ctx, action, resource)
            # Layer 2: additional grant check (skipped for admins)
            if not ctx.has_role("admin"):
                await grant_check(ctx, action, resource)

Thread safety:  ✅ All three implementations are stateless after __init__.
Async safety:   ✅ All ``authorize`` methods are ``async def``.
"""

from __future__ import annotations

from typing import Any, ClassVar

from fastrest_core.auth.base import (
    AbstractAuthorizer,
    Action,
    AuthContext,
    Resource,
)
from fastrest_core.exception.service import ServiceAuthorizationError


# ── Shared key derivation helper ──────────────────────────────────────────────


def _default_resource_key(
    entity_type: type,
    entity: Any | None,
) -> str:
    """
    Derive a resource key from an entity type and optional entity instance.

    Convention (mirrors ``ResourceGrant`` docs):

    - Collection-level (``entity`` is ``None``): ``"<typename>s"``
      e.g. ``Post → "posts"``
    - Instance-level: ``"<typename>s:<pk>"``
      e.g. ``Post(pk=42) → "posts:42"``

    Args:
        entity_type: The domain class being acted upon.
        entity:      Specific entity, or ``None`` for collection ops.

    Returns:
        Canonical resource key string.

    Edge cases:
        - Domain classes with irregular plurals (e.g. ``Category → "categorys"``)
          should override ``_resource_key()`` instead of relying on this helper.
        - ``entity.pk`` is coerced to ``str`` — works for ``int``, ``UUID``, etc.
    """
    # Lowercase plural by appending "s" — simple convention, overridable
    base = entity_type.__name__.lower() + "s"
    if entity is None:
        # Collection-level: no specific instance yet
        return base
    # Instance-level: append the primary key for fine-grained ACLs
    return f"{base}:{entity.pk}"


# ── GrantBasedAuthorizer ──────────────────────────────────────────────────────


class GrantBasedAuthorizer(AbstractAuthorizer):
    """
    Authorizer that resolves permissions entirely from ``ctx.grants``.

    Derives a resource key from the entity type name and checks
    ``ctx.can(action, key)`` against the caller's token-encoded grants.
    No entity field is inspected — decisions are made purely from the JWT.

    Resource key derivation:
        Override ``_resource_key()`` to customize the key format.
        The default produces ``"posts"`` (collection) or ``"posts:42"``
        (instance), matching the convention in ``ResourceGrant``.

    DESIGN: pure-token authorization
        ✅ Zero-latency — no DB roundtrips, no entity field reads.
        ✅ Works even when ``resource.entity`` is ``None`` (collection ops).
        ✅ Compatible with both RBAC (type-level grants) and ACL (instance-
           level grants) depending on how ``ResourceGrant`` is encoded.
        ❌ Revocation requires short-lived tokens or a server-side blocklist.
        ❌ Large ACL datasets produce large tokens — encode only non-default
           grants (deny-by-default model).

    Thread safety:  ✅ Stateless after construction — no mutable state.
    Async safety:   ✅ ``authorize`` reads only the frozen ``AuthContext``.

    Edge cases:
        - Anonymous callers (``ctx.user_id is None``) are denied unless an
          explicit wildcard grant ``"*"`` covers the action — ``ctx.can()``
          still evaluates their grants, but anonymous contexts typically have
          none.
        - Wildcard resource ``"*"`` grants in ``ctx.grants`` are matched by
          ``ctx.can()`` before the specific key — admin tokens work without
          any key override.

    Example::

        @Singleton
        class AppAuthorizer(GrantBasedAuthorizer):
            pass   # default key derivation is sufficient

        # In DI container:
        container.bind(AbstractAuthorizer, AppAuthorizer)
    """

    def _resource_key(
        self,
        entity_type: type,
        entity: Any | None,
    ) -> str:
        """
        Derive the resource key checked against ``ctx.grants``.

        Override this to change the naming convention without reimplementing
        the whole authorization logic.

        Args:
            entity_type: Domain class being acted upon.
            entity:      Entity instance, or ``None`` for collection ops.

        Returns:
            Resource key string (e.g. ``"posts"`` or ``"posts:42"``).
        """
        return _default_resource_key(entity_type, entity)

    async def authorize(
        self,
        ctx: AuthContext,
        action: Action,
        resource: Resource,
    ) -> None:
        """
        Authorize via ``ctx.can(action, resource_key)``.

        Args:
            ctx:      Caller's identity and grants.
            action:   The action being attempted.
            resource: What is being acted upon.

        Raises:
            ServiceAuthorizationError: Caller's grants do not permit the action.
        """
        key = self._resource_key(resource.entity_type, resource.entity)
        if not ctx.can(action, key):
            raise ServiceAuthorizationError(
                str(action),
                resource.entity_type,
                # Internal detail — logged server-side, never returned to client
                reason=(
                    f"user_id={ctx.user_id!r} has no grant for "
                    f"action={action!r} on resource={key!r}"
                ),
            )


# ── OwnershipAuthorizer ───────────────────────────────────────────────────────


class OwnershipAuthorizer(AbstractAuthorizer):
    """
    Authorizer that gates instance operations on entity ownership.

    For **collection-level** operations (CREATE, LIST — ``resource.entity``
    is ``None``): always allows authenticated callers.  Override
    ``_check_collection`` to tighten this if needed.

    For **instance-level** operations (READ, UPDATE, DELETE — ``resource.entity``
    is set): compares ``getattr(entity, owner_field) == ctx.user_id``.

    Class attributes:
        _owner_field: Name of the field on the domain model that stores the
                      owner's user ID.  Defaults to ``"owner_id"``.

    DESIGN: ownership over token grants for user-owned resources
        ✅ Simple model — no grant encoding in JWT needed for ownership.
        ✅ Works for soft-deleted entities because the entity is still fetched.
        ✅ Correct even when the user's ID changes — checked against live entity.
        ❌ Admin override requires combining with a role check (subclass or
           compose with ``RoleBasedAuthorizer``).
        ❌ Requires ``resource.entity`` to be set — collection ops cannot
           verify ownership, so they are allowed by default.
        ❌ Assumes ``owner_field`` is set at creation time — no check for
           ``None`` owner.  Set ``_owner_field`` to a field guaranteed non-null.

    Thread safety:  ✅ Stateless — ``_owner_field`` is a class variable.
    Async safety:   ✅ No I/O in ``authorize``.

    Edge cases:
        - Anonymous callers (``ctx.user_id is None``) will always fail the
          ownership comparison unless ``owner_field`` is also ``None``
          (which typically indicates a bug in the creation flow).
        - ``getattr(entity, _owner_field)`` raises ``AttributeError`` if the
          field does not exist on the entity — choose the field name carefully.
        - Collection-level CREATE is allowed for authenticated callers — add
          role checks in ``_check_collection`` if stricter control is needed.

    Example::

        @Singleton
        class PostAuthorizer(OwnershipAuthorizer):
            _owner_field: ClassVar[str] = "author_id"   # custom field name
    """

    # Name of the entity field that stores the owner's user ID.
    # Override in subclass: _owner_field: ClassVar[str] = "created_by"
    _owner_field: ClassVar[str] = "owner_id"

    def _check_collection(self, ctx: AuthContext, action: Action) -> None:
        """
        Authorize a collection-level operation.

        Default: deny anonymous callers, allow authenticated ones.
        Override to add role or scope requirements.

        Args:
            ctx:    Caller's identity.
            action: The collection-level action (LIST or CREATE).

        Raises:
            ServiceAuthorizationError: Caller is anonymous.
        """
        if ctx.is_anonymous():
            raise ServiceAuthorizationError(
                str(action),
                reason="anonymous callers are not allowed to perform collection operations",
            )

    async def authorize(
        self,
        ctx: AuthContext,
        action: Action,
        resource: Resource,
    ) -> None:
        """
        Authorize via ownership comparison for instance ops; allow for collection ops.

        Args:
            ctx:      Caller's identity and grants.
            action:   The action being attempted.
            resource: What is being acted upon.

        Raises:
            ServiceAuthorizationError: Ownership check fails or caller is
                anonymous on a collection operation.
        """
        if resource.is_collection:
            # No entity loaded yet — delegate to the overridable collection check
            self._check_collection(ctx, action)
            return

        entity = resource.entity  # not None — is_collection is False
        owner_value = getattr(entity, self._owner_field)

        if owner_value != ctx.user_id:
            raise ServiceAuthorizationError(
                str(action),
                resource.entity_type,
                # Internal detail only — must not be surfaced in API responses
                reason=(
                    f"user_id={ctx.user_id!r} is not the owner "
                    f"(entity.{self._owner_field}={owner_value!r})"
                ),
            )


# ── RoleBasedAuthorizer ───────────────────────────────────────────────────────


class RoleBasedAuthorizer(AbstractAuthorizer):
    """
    Authorizer that checks ``ctx.roles`` against a declarative permission table.

    Permissions are defined once at construction time as a mapping from role
    name to the set of ``Action``\\s that role may perform.  The check is
    entity-type agnostic — the same role table applies to every entity type
    handled by this authorizer.

    To apply different rules per entity type, use ``resource.entity_type``
    to dispatch to different ``RoleBasedAuthorizer`` instances (or override
    ``authorize`` and call ``super()`` with modified args).

    DESIGN: declarative permission table over inline logic
        ✅ All permissions visible in one place — easy to audit.
        ✅ Zero-latency — only reads ``ctx.roles``, no I/O.
        ✅ Testable without a running service — just construct and call.
        ❌ Entity-type agnostic — callers that need per-entity rules must
           subclass or compose with ``GrantBasedAuthorizer``.
        ❌ Table is defined at construction time — hot reload requires a
           new instance.  For dynamic rules, use a DB-backed authorizer.

    Thread safety:  ✅ Permission table is frozen after ``__init__``.
    Async safety:   ✅ ``authorize`` only reads ``ctx.roles`` — no I/O.

    Edge cases:
        - A caller with multiple roles needs only ONE role that grants the
          action to pass — permissions are additive (union of all role grants).
        - A role not in ``role_permissions`` has no implied permissions — it is
          silently treated as a role with an empty permission set.
        - Anonymous callers (``ctx.user_id is None``) can still be authorized
          if their empty ``roles`` frozenset somehow matches a ``""`` role key
          — but conventionally they will be denied.
        - Providing an empty ``role_permissions`` dict means ALL callers are
          denied — effectively a lockdown mode.

    Example::

        @Singleton
        class AppAuthorizer(RoleBasedAuthorizer):
            def __init__(self) -> None:
                super().__init__(
                    role_permissions={
                        "admin":  frozenset(Action),             # all actions
                        "editor": frozenset({Action.CREATE, Action.READ,
                                             Action.UPDATE, Action.LIST}),
                        "viewer": frozenset({Action.READ, Action.LIST}),
                    }
                )
    """

    def __init__(
        self,
        role_permissions: dict[str, frozenset[Action]],
    ) -> None:
        """
        Initialise with a role-to-actions permission table.

        Args:
            role_permissions: Mapping from role name to the set of ``Action``\\s
                              that role is allowed to perform.
                              Use ``frozenset(Action)`` to allow all actions.

        Edge cases:
            - An empty dict means all callers are denied.
            - Roles not in the dict are silently treated as having no permissions.
        """
        # Store as a plain dict — read-only after __init__, no copy needed.
        # Callers should not mutate this dict after passing it in — document
        # as an invariant rather than defensive-copying, to avoid overhead.
        self._role_permissions: dict[str, frozenset[Action]] = role_permissions

    def _allowed_actions(self, ctx: AuthContext) -> frozenset[Action]:
        """
        Return the union of all actions permitted by the caller's roles.

        Aggregates permitted actions across all roles the caller holds,
        so a caller with both ``"viewer"`` and ``"editor"`` roles gets
        the combined permissions of both.

        Args:
            ctx: Caller's identity (only ``ctx.roles`` is inspected).

        Returns:
            Union of all allowed ``Action``\\s for the caller's role set.
            Empty ``frozenset`` if no role matches.
        """
        combined: set[Action] = set()
        for role in ctx.roles:
            # Roles not in the table are silently ignored — no permissions granted
            combined |= self._role_permissions.get(role, frozenset())
        return frozenset(combined)

    async def authorize(
        self,
        ctx: AuthContext,
        action: Action,
        resource: Resource,
    ) -> None:
        """
        Authorize by checking whether any of the caller's roles permit ``action``.

        Args:
            ctx:      Caller's identity and roles.
            action:   The action being attempted.
            resource: What is being acted upon (entity_type is used for
                      error reporting only — no per-type dispatch).

        Raises:
            ServiceAuthorizationError: None of the caller's roles permit
                ``action``.
        """
        allowed = self._allowed_actions(ctx)
        if action not in allowed:
            raise ServiceAuthorizationError(
                str(action),
                resource.entity_type,
                # Internal detail — not exposed to API clients
                reason=(
                    f"user_id={ctx.user_id!r} with roles={set(ctx.roles)!r} "
                    f"is not permitted to perform action={action!r}. "
                    f"Allowed actions for their roles: {set(allowed)!r}"
                ),
            )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "GrantBasedAuthorizer",
    "OwnershipAuthorizer",
    "RoleBasedAuthorizer",
]
