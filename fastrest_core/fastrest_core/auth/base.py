"""
fastrest_core.auth
==================
Authorization primitives for the service layer.

Four concepts live here:

``Action``
    ``StrEnum`` of well-known CRUD operations.  Because ``Action`` is a
    ``StrEnum``, every value is also a ``str`` at runtime — stored verbatim
    in JWT claims and compared with ``==`` against raw strings.

``ResourceGrant``
    Immutable value object: a ``resource`` key (type-level ``"posts"`` or
    instance-level ``"posts:abc123"``) paired with the ``Action`` set that
    is allowed on that resource.  Serializes directly into a JWT claim.

``AuthContext``
    Immutable identity + permission snapshot.  Carries ``user_id``,
    ``roles``, ``scopes``, and a tuple of ``ResourceGrant`` objects.
    ``AuthContext.can(action, resource_key)`` is the fast-path check —
    zero DB roundtrips, token-derived, stateless.

``Resource``
    Non-generic descriptor of *what* is being acted upon.  Carries
    ``entity_type`` (the domain class) and an optional ``entity`` instance,
    giving implementations full runtime information for dispatch.

``AbstractAuthorizer``
    **Non-generic** async authorization gate.  A single implementation
    handles every entity type by inspecting ``resource.entity_type`` at
    runtime — one DI binding, no per-entity registration.

JWT encoding example::

    {
        "sub": "usr_123",
        "roles": ["editor"],
        "grants": [
            {"resource": "posts",        "actions": ["list", "create", "read"]},
            {"resource": "posts:abc123", "actions": ["update", "delete"]}
        ]
    }

    ctx = AuthContext(
        user_id="usr_123",
        roles=frozenset({"editor"}),
        grants=(
            ResourceGrant("posts",        frozenset({Action.LIST, Action.CREATE, Action.READ})),
            ResourceGrant("posts:abc123", frozenset({Action.UPDATE, Action.DELETE})),
        ),
    )

DESIGN: AbstractAuthorizer is non-generic
    ✅ One DI binding for the entire application — services inject a
       single ``AbstractAuthorizer`` regardless of their entity type.
    ✅ Implementations can route by ``resource.entity_type`` to apply
       per-entity rules without a separate class per entity.
    ✅ Mirrors Jakarta Security's model: one interceptor, dispatch by
       resource type, rather than one authorizer per resource class.
    ✅ A shared ``_check_grants(ctx, action, resource_key)`` helper in the
       implementation keeps the routing logic DRY.
    ❌ Type checker cannot verify that an authorizer is correctly
       specialized for a given entity type — callers must rely on tests.
       Use ``isinstance(resource.entity, MyEntity)`` guards in the
       implementation to make dispatch explicit.

DESIGN: grants inside AuthContext rather than a separate permission store
    ✅ Zero-latency checks — no DB lookup required at authorization time.
    ✅ Stateless — the token is the source of truth; services stay pure.
    ✅ Serializable — ResourceGrant maps 1-to-1 with a JWT claim entry.
    ✅ Supports RBAC (type-level key ``"posts"``) and ACL (instance-level
       key ``"posts:abc123"``) in one unified structure.
    ❌ Grants are frozen at token-issue time — revocation requires short-
       lived tokens or a server-side revocation list.
    ❌ Fine-grained ACLs on large datasets produce large tokens — encode
       only non-default grants (deny-by-default model).

DESIGN: Action typed as enum, not str
    ✅ IDE autocomplete and static analysis catch typos at write time.
    ✅ ``Action`` is a ``StrEnum`` — values compare equal to their string
       equivalents at runtime (``Action.READ == "read"`` is ``True``),
       so JWT round-trips are transparent.
    ❌ ``Action`` cannot be subclassed once it has members — callers that
       need domain-specific actions should define their own ``StrEnum``
       and annotate with ``Action | MyAction`` where both are accepted.

Thread safety:  ✅ All types here are frozen/stateless — safe to share.
Async safety:   ✅ ``AbstractAuthorizer.authorize`` is ``async def``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    # Imported only for type hints — avoids pulling in domain model machinery
    # for callers that only need AuthContext / ResourceGrant.
    from fastrest_core.model import DomainModel


# ── Action ────────────────────────────────────────────────────────────────────


class Action(StrEnum):
    """
    Well-known CRUD actions performed on domain resources.

    Extends ``StrEnum`` so every member is also a plain ``str`` at runtime:
    - Stored verbatim in ``ResourceGrant.actions`` and JWT ``grants`` claims.
    - Compared with ``==`` directly: ``Action.READ == "read"`` is ``True``.
    - Passed to ``ctx.can()`` and ``authorizer.authorize()`` without casting.

    Note on extensibility:
        ``StrEnum`` members cannot be added by subclassing a non-empty enum.
        Define a separate ``StrEnum`` for domain-specific actions and
        annotate with ``Action | MyAction`` where both are accepted::

            class PostAction(StrEnum):
                PUBLISH = "publish"
                ARCHIVE = "archive"

    Thread safety:  ✅ Enum members are module-level singletons — immutable.
    Async safety:   ✅ Pure value — no I/O.
    """

    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LIST = "list"


# ── ResourceGrant ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ResourceGrant:
    """
    Immutable grant: a set of allowed ``Action``\\s for one resource key.

    Resource key conventions:

    - **Type-level** (RBAC-like): ``"posts"``
      Applies to every instance of the resource.

    - **Instance-level** (ACL-like): ``"posts:<instance_id>"``
      Applies to one specific entity instance.

    - **Wildcard**: ``"*"``
      Applies to every resource.  Use for admin grants only.

    Attributes:
        resource: Canonical resource key.  Convention: lowercase plural of
                  the entity class name, with an optional ``:<id>`` suffix.
        actions:  Set of ``Action``\\s allowed on this resource.
                  ``frozenset`` keeps the dataclass hashable.

    Thread safety:  ✅ frozen=True — immutable after construction.
    Async safety:   ✅ Pure value object; no I/O.

    Edge cases:
        - Empty ``actions`` is valid but effectively a no-op grant.
        - ``"*"`` inside ``actions`` is NOT treated as an action wildcard —
          authorizer implementations must handle action wildcards explicitly
          if that is a requirement.

    Example::

        ResourceGrant("posts",        frozenset({Action.LIST, Action.READ}))
        ResourceGrant("posts:abc123", frozenset({Action.UPDATE, Action.DELETE}))
        ResourceGrant("*",            frozenset(Action))  # admin: all CRUD
    """

    # Canonical resource key — e.g. "posts", "posts:abc123", "*"
    resource: str

    # Allowed actions for this resource — frozenset for hashability
    actions: frozenset[Action]


# ── AuthContext ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AuthContext:
    """
    Immutable snapshot of the caller's identity and authorization grants.

    Created once per request (from a decoded JWT or session) and threaded
    through to every service call.  Never mutated in flight.

    Attributes:
        user_id:  Unique identifier of the authenticated caller.
                  ``None`` means the request is unauthenticated (anonymous).
        roles:    Role names held by the caller (for coarse-grained RBAC).
        scopes:   OAuth-style permission scopes granted by the token.
        grants:   Per-resource ``Action`` grants, serializable to/from JWT.
                  See ``ResourceGrant`` for the key convention.
        metadata: Arbitrary extra claims (tenant_id, request_id, etc.).
                  Excluded from equality and hashing.

    Thread safety:  ✅ frozen=True — all attributes are immutable.
    Async safety:   ✅ Safe to share across coroutines — no mutable state.

    Edge cases:
        - ``is_anonymous()`` is the authoritative check for unauthenticated
          callers — never test ``user_id is None`` directly in service code.
        - ``has_role()`` and ``has_scope()`` are case-sensitive.
        - ``can()`` checks in this order:
            1. Wildcard key ``"*"``
            2. The exact ``resource_key`` passed in
          Any matching grant that contains the action returns ``True``.
        - An empty ``grants`` tuple means the caller has no granted
          permissions — ``can()`` returns ``False`` for everything.

    Example::

        ctx = AuthContext(
            user_id="usr_123",
            roles=frozenset({"editor"}),
            grants=(
                ResourceGrant("posts", frozenset({Action.LIST, Action.READ})),
                ResourceGrant("posts:abc123", frozenset({Action.UPDATE})),
            ),
        )
        ctx.can(Action.READ,   "posts")        # True  — type-level grant
        ctx.can(Action.UPDATE, "posts")        # False — not in type grant
        ctx.can(Action.UPDATE, "posts:abc123") # True  — instance grant
    """

    # Authenticated caller identifier.  None = anonymous / unauthenticated.
    user_id: str | None = None

    # Role names — frozenset keeps AuthContext hashable
    roles: frozenset[str] = field(default_factory=frozenset)

    # OAuth-style scopes — frozenset for the same reason
    scopes: frozenset[str] = field(default_factory=frozenset)

    # Per-resource permission grants — serializable to/from JWT.
    # tuple (not list) so the frozen dataclass stays hashable.
    grants: tuple[ResourceGrant, ...] = field(default_factory=tuple)

    # Arbitrary extra claims — excluded from equality and hashing
    metadata: dict[str, Any] = field(default_factory=dict, compare=False, hash=False)

    # ── Identity helpers ──────────────────────────────────────────────────────

    def is_anonymous(self) -> bool:
        """
        Return ``True`` if the caller is not authenticated.

        Returns:
            ``True`` when ``user_id`` is ``None``; ``False`` otherwise.

        Edge cases:
            - An empty string ``""`` for ``user_id`` is still considered
              authenticated.  Use ``None`` explicitly for anonymous callers.
        """
        return self.user_id is None

    def has_role(self, role: str) -> bool:
        """
        Return ``True`` if the caller holds the given role name.

        Args:
            role: Role name to check.  Case-sensitive.

        Returns:
            ``True`` if ``role`` is in ``self.roles``; ``False`` otherwise.
        """
        return role in self.roles

    def has_scope(self, scope: str) -> bool:
        """
        Return ``True`` if the caller was granted the given OAuth scope.

        Args:
            scope: Scope string to check (e.g. ``"write:posts"``).

        Returns:
            ``True`` if ``scope`` is in ``self.scopes``; ``False`` otherwise.
        """
        return scope in self.scopes

    # ── Grant helpers ─────────────────────────────────────────────────────────

    def can(self, action: Action, resource_key: str) -> bool:
        """
        Return ``True`` if any grant permits ``action`` on ``resource_key``.

        Checks two candidate resource keys in priority order:

        1. ``"*"``          — wildcard, grants access to everything.
        2. ``resource_key`` — exact match (type-level or instance-level).

        A single matching grant that includes ``action`` short-circuits
        and returns ``True``.

        Args:
            action:       The ``Action`` to check (e.g. ``Action.READ``).
            resource_key: Canonical resource key to check against
                          (e.g. ``"posts"`` or ``"posts:abc123"``).

        Returns:
            ``True`` if at least one grant allows the action; ``False``
            otherwise.

        Edge cases:
            - Empty ``grants`` tuple always returns ``False``.
            - ``resource_key == "*"`` does NOT implicitly grant everything;
              only a ``ResourceGrant(resource="*", ...)`` does.
            - Type-level and instance-level keys do NOT cascade — passing
              ``"posts"`` does not match a ``ResourceGrant("posts:abc123")``.
              Query both keys explicitly if both levels are needed.
        """
        # Wildcard is checked first — a blanket admin grant short-circuits
        # before we evaluate the specific key.
        candidate_keys = {"*", resource_key}

        for grant in self.grants:
            if grant.resource in candidate_keys and action in grant.actions:
                return True
        return False

    def grants_for(self, resource_key: str) -> frozenset[Action]:
        """
        Return the union of all ``Action``\\s granted for ``resource_key``.

        Collects actions from the wildcard grant ``"*"`` and from any grant
        whose ``resource`` equals ``resource_key`` exactly.

        Args:
            resource_key: Canonical resource key (e.g. ``"posts"``).

        Returns:
            ``frozenset[Action]`` — empty if no grant matches.

        Edge cases:
            - Does NOT perform prefix matching — ``"posts"`` does not cover
              ``"posts:abc123"``.  Query both keys and union if needed.
        """
        candidate_keys = {"*", resource_key}
        combined: set[Action] = set()
        for grant in self.grants:
            if grant.resource in candidate_keys:
                combined |= grant.actions
        return frozenset(combined)


# ── Resource ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Resource:
    """
    Immutable descriptor of *what* is being acted upon.

    Carries ``entity_type`` (the domain class) and an optional ``entity``
    instance so authorizer implementations have full runtime information
    for both type-level and instance-level dispatch.

    Two modes:

    - **Collection-level** (``entity`` is ``None``) — for ``Action.CREATE``
      and ``Action.LIST``, where no specific entity has been loaded yet.
    - **Instance-level** (``entity`` is set) — for ``Action.READ``,
      ``Action.UPDATE``, and ``Action.DELETE``, after the entity has been
      fetched from the repository.

    DESIGN: non-generic Resource over Resource[D]
        ✅ One ``AbstractAuthorizer`` handles all entity types — no generic
           binding per entity in the DI container.
        ✅ Authorizer implementations check ``resource.entity_type`` at
           runtime for dispatch, keeping routing logic centralized.
        ❌ The type checker cannot verify that ``resource.entity`` is the
           correct subtype inside the authorizer — use ``isinstance``
           guards or trust the service to pass matching types.

    Attributes:
        entity_type: The domain class (e.g. ``Post``, ``User``).
        entity:      The specific entity, or ``None`` for collection ops.

    Thread safety:  ✅ frozen=True — safe to share across tasks.
    Async safety:   ✅ Pure value object; no I/O.

    Edge cases:
        - ``entity_type`` should match ``type(entity)`` when both are set.
          The authorizer may rely on this invariant for key derivation.
        - Passing a collection-level ``Resource`` to an authorizer that
          expects an entity (e.g. for an ownership check) will produce
          ``entity is None`` — implementations should raise ``RuntimeError``
          rather than silently granting access.

    Example::

        Resource(entity_type=Post)               # collection-level
        Resource(entity_type=Post, entity=post)  # instance-level
    """

    # The domain class being acted upon
    entity_type: type[DomainModel]

    # Specific entity instance — None for collection-level operations
    entity: DomainModel | None = None

    @property
    def is_collection(self) -> bool:
        """
        Return ``True`` if this resource targets the whole collection.

        Returns:
            ``True`` when ``entity`` is ``None``; ``False`` otherwise.
        """
        return self.entity is None


# ── AbstractAuthorizer ────────────────────────────────────────────────────────


class AbstractAuthorizer(ABC):
    """
    Abstract, non-generic authorization gate.

    A single ``AbstractAuthorizer`` implementation handles every entity
    type by inspecting ``resource.entity_type`` at runtime.  This means
    one DI binding covers the whole application::

        container.provide(my_authorizer_provider)
        # resolves for every service, regardless of entity type

    Implementations can route by entity type using a simple dispatch table::

        class AppAuthorizer(AbstractAuthorizer):
            _RULES: dict[type, Callable] = {
                Post: _check_post,
                User: _check_user,
            }

            async def authorize(self, ctx, action, resource):
                handler = self._RULES.get(resource.entity_type)
                if handler is None:
                    raise ServiceAuthorizationError(str(action), resource.entity_type)
                await handler(ctx, action, resource)

    Or use ``ctx.can()`` directly for grant-based authorization without
    any entity-type routing::

        class GrantBasedAuthorizer(AbstractAuthorizer):
            async def authorize(self, ctx, action, resource):
                key = (
                    f"{resource.entity_type.__name__.lower()}s"
                    f":{resource.entity.pk}"
                    if not resource.is_collection
                    else f"{resource.entity_type.__name__.lower()}s"
                )
                if not ctx.can(action, key):
                    raise ServiceAuthorizationError(str(action), resource.entity_type)

    The service layer injects this via ``Inject[AbstractAuthorizer]``::

        class PostService(AsyncService[Post, UUID, ...]):
            def __init__(
                self,
                uow_provider: Inject[IUoWProvider],
                authorizer: Inject[AbstractAuthorizer],
            ) -> None:
                super().__init__(uow_provider, authorizer)

    DESIGN: non-generic over Generic[D]
        ✅ One DI binding for all entity types — simpler container setup.
        ✅ A shared grant-based implementation requires zero entity-specific
           code — ``ctx.can(action, key)`` derived from the entity type name.
        ✅ Entity-specific rules can still be added by dispatching on
           ``resource.entity_type`` inside the implementation.
        ❌ No compile-time guarantee that the authorizer is specialized for
           a given entity type — rely on integration tests to verify.

    Thread safety:  ✅ Implementations must be stateless.
    Async safety:   ✅ ``authorize`` is ``async def`` — safe to await.

    Edge cases:
        - Implementations must **deny by default** (fail-closed posture)
          when no rule matches the given combination.
        - ``authorize()`` must always raise on denial — never return a bool
          that callers could accidentally ignore.
        - The service always passes a real ``AuthContext`` — implementations
          do not need to guard against ``ctx is None``.
    """

    @abstractmethod
    async def authorize(
        self,
        ctx: AuthContext,
        action: Action,
        resource: Resource,
    ) -> None:
        """
        Assert that ``ctx`` may perform ``action`` on ``resource``.

        Args:
            ctx:      The caller's identity, roles, scopes, and grants.
            action:   The ``Action`` being attempted.
            resource: What is being acted upon.  Inspect
                      ``resource.entity_type`` to route per-entity rules.
                      ``resource.entity`` is ``None`` for collection-level
                      operations (CREATE / LIST).

        Returns:
            ``None`` if the operation is allowed.

        Raises:
            ServiceAuthorizationError: The caller is not permitted to
                perform ``action`` on ``resource``.

        Edge cases:
            - When ``resource.is_collection`` is ``True``, ownership checks
              are not possible — only type-level and wildcard grants apply.
            - If no rule matches the combination, deny (fail-closed).
        """
