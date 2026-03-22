"""
fastrest_core.tenant
=====================
Multi-tenancy primitives — row-level and database-level isolation.

Two complementary strategies are provided:

**Row-level isolation** (shared schema, tenant column)
    ``TenantAwareService`` enforces tenant isolation across all five CRUD
    operations:

    - ``list``   — injects ``tenant_id = <tid>`` into every query.
    - ``get``    — raises ``ServiceNotFoundError`` (not 403) when the entity's
                   tenant does not match the caller's.
    - ``create`` — stamps ``tenant_id`` from ``ctx.metadata`` onto the entity
                   **after** assembly, using ``dataclasses.replace``.  The
                   assembler does not need to handle this field.
    - ``update`` — same tenant check before applying changes.
    - ``delete`` — same tenant check before deletion.

    Returning 404 for cross-tenant accesses is intentional: a 403 would
    reveal that the entity exists in another tenant's data (existence oracle).

    ``create`` stamps from ``ctx`` (not the DTO) to prevent a compromised
    HTTP adapter from injecting a foreign tenant ID.

**Database-level isolation** (separate DB or schema per tenant)
    ``TenantUoWProvider`` wraps one ``RepositoryProvider`` per tenant and
    routes ``make_uow()`` to the correct backend by reading the tenant ID
    from a request-scoped ``ContextVar``.  The HTTP adapter activates the
    context once per request via ``tenant_context()``.

    New tenants can be provisioned at runtime via ``register()``.
    A ``threading.Lock`` guards the providers dict.

Typical wiring
--------------
Row-level::

    @dataclass
    class Post(AuditedDomainModel):
        tenant_id: Annotated[str, FieldHint(index=True, nullable=False)]
        title: str

    @Singleton
    class PostService(
        TenantAwareService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO]
    ):
        def __init__(
            self,
            uow_provider: Inject[IUoWProvider],
            authorizer:   Inject[AbstractAuthorizer],
            assembler:    Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]],
        ) -> None:
            super().__init__(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

        def _get_repo(self, uow): return uow.posts

DB-level (static setup)::

    provider = TenantUoWProvider({
        "acme":   SQLAlchemyRepositoryProvider(base_acme, sessions_acme),
        "globex": BeanieRepositoryProvider(motor_client, "globex_db"),
    })

DB-level (runtime provisioning)::

    provider = TenantUoWProvider()                                     # start empty
    provider.register("acme", SQLAlchemyRepositoryProvider(...))       # add later

    # In the HTTP adapter:
    with tenant_context(ctx.metadata["tenant_id"]):
        result = await service.create(dto, ctx)

DESIGN: create() stamps tenant_id from ctx, not from the DTO
    ✅ Assembler stays unaware of tenancy — ``to_domain()`` maps only
       business fields; ``tenant_id`` is a cross-cutting concern.
    ✅ ``ctx`` is the authoritative, already-validated JWT identity.  The
       DTO comes from untrusted user input — if stamping were left to the
       HTTP adapter, a buggy adapter could inject a foreign tenant ID.
    ✅ ``dataclasses.replace`` preserves all other fields and ``_raw_orm``
       (inherited from the base) so the INSERT path is unaffected.
    ❌ Requires the domain model to declare ``_tenant_field`` as an ``init``
       parameter so ``dataclasses.replace`` can set it.  A missing field
       raises ``TypeError`` immediately at ``create()`` time (fail-fast).
    Alternative: require assembler to set it — rejected because it creates
       a hidden contract between the assembler and the tenancy layer that is
       easy to violate.

DESIGN: ContextVar over function-argument propagation
    ✅ ``IUoWProvider.make_uow()`` signature is unchanged — no ripple through
       every service, repository, and UoW class.
    ✅ Each asyncio.Task inherits its parent's context — variable set in the
       HTTP adapter propagates into sub-tasks automatically.
    ✅ Backend-agnostic — SA and Beanie providers work behind the same wrapper.
    ❌ Implicit contract — callers must set the ContextVar.  Forgetting raises
       ``RuntimeError`` at ``make_uow()`` time (fail-fast).

DESIGN: threading.Lock for TenantUoWProvider._providers
    ``register()`` may be called from any thread (admin endpoint, CLI, job).
    ``threading.Lock`` (not ``asyncio.Lock``) is used because the lock is
    never held across an ``await`` and must be acquirable without a running
    event loop.  The locked section is O(1) — no event-loop blocking.

DESIGN: 404 for cross-tenant access (not 403)
    A 403 would reveal that the entity exists in another tenant's data.
    From the caller's perspective, cross-tenant entities do not exist.

Thread safety:  ✅ ``ContextVar`` is task-local; ``threading.Lock`` guards writes.
Async safety:   ✅ Lock released before any ``await`` — event loop not blocked.
"""

from __future__ import annotations

import dataclasses
import threading
from abc import ABC
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, ClassVar, Generator, Generic, TypeVar

from fastrest_core.auth import AuthContext
from fastrest_core.dto import CreateDTO, ReadDTO, UpdateDTO
from fastrest_core.exception.service import (
    ServiceAuthorizationError,
    ServiceNotFoundError,
)
from fastrest_core.model import DomainModel
from fastrest_core.providers import RepositoryProvider
from fastrest_core.query.builder import QueryBuilder
from fastrest_core.query.params import QueryParams
from fastrest_core.service.base import AsyncService, IUoWProvider

D = TypeVar("D", bound=DomainModel)
PK = TypeVar("PK")
C = TypeVar("C", bound=CreateDTO)
R = TypeVar("R", bound=ReadDTO)
U = TypeVar("U", bound=UpdateDTO)


# ── Tenant context variable ────────────────────────────────────────────────────

# ContextVar instead of threading.local — async tasks share a thread, so
# threading.local would bleed the tenant ID across concurrent coroutines.
# Each asyncio.Task copies its parent's context, so the value set in the
# HTTP adapter propagates into any sub-tasks the service spawns.
_current_tenant: ContextVar[str | None] = ContextVar("_current_tenant", default=None)


def current_tenant() -> str | None:
    """
    Return the tenant ID active in the current asyncio task, or ``None``.

    Returns:
        The tenant ID set by the most recent enclosing ``tenant_context()``
        block, or ``None`` when called outside any tenant context.

    Async safety: ✅ Reads a ``ContextVar`` — isolated per asyncio.Task.
    """
    return _current_tenant.get()


@contextmanager
def tenant_context(tenant_id: str) -> Generator[None, None, None]:
    """
    Context manager that activates ``tenant_id`` for the duration of the block.

    Sets ``_current_tenant`` via a ``ContextVar`` token so the value is
    automatically restored even if an exception propagates.  Call once per
    request in the HTTP adapter before any service call.

    Args:
        tenant_id: Tenant identifier to activate.  Must match a key
                   registered in ``TenantUoWProvider`` when DB-level
                   isolation is in use.

    Edge cases:
        - Nesting two ``tenant_context()`` blocks is valid — the inner tenant
          overrides the outer for its duration, restored automatically via token.
        - The context manager is synchronous — use ``with``, not ``async with``.
          Tasks spawned inside inherit the tenant ID from their parent context.

    Example::

        with tenant_context("acme"):
            result = await service.create(dto, ctx)

    Async safety: ✅ ``ContextVar.set()`` / ``reset()`` are task-local.
    """
    # Store the token so we can revert without affecting sibling tasks
    # that may have set their own copy of the ContextVar.
    token = _current_tenant.set(tenant_id)
    try:
        yield
    finally:
        # Always reset — prevents the tenant ID from leaking into the next
        # request if this coroutine or thread is reused by the runtime.
        _current_tenant.reset(token)


# ── TenantUoWProvider ─────────────────────────────────────────────────────────


class TenantUoWProvider(IUoWProvider):
    """
    ``IUoWProvider`` that routes ``make_uow()`` to a per-tenant backend.

    Supports both **static** setup (all tenants known at startup) and
    **dynamic** registration (tenants added at runtime via ``register()``).
    Mixed SA/Beanie backends per tenant are supported.

    Args:
        providers: Optional initial mapping of tenant ID → configured
                   ``RepositoryProvider``.  Defaults to empty — add tenants
                   via ``register()`` after construction.

    Thread safety:  ✅ ``threading.Lock`` serialises all dict writes.
    Async safety:   ✅ Lock released before any ``await`` — no event-loop blocking.

    Edge cases:
        - ``make_uow()`` raises ``RuntimeError`` outside a ``tenant_context()``
          block — prevents silent cross-tenant data access.
        - ``make_uow()`` raises ``KeyError`` for an unregistered tenant.
        - ``register()`` with an existing tenant ID replaces the provider
          silently.  Check ``has_tenant()`` first for idempotency.
        - Tenant IDs are compared by exact string equality.

    Example::

        # Static
        provider = TenantUoWProvider({
            "acme": SQLAlchemyRepositoryProvider(base, sessions_acme),
        })

        # Runtime provisioning
        new_p = SQLAlchemyRepositoryProvider(base, sessions_new)
        await new_p.create_all()
        provider.register("new_tenant", new_p)
    """

    def __init__(
        self,
        providers: dict[str, RepositoryProvider] | None = None,
    ) -> None:
        # Mutable — guarded by _lock for all writes.
        self._providers: dict[str, RepositoryProvider] = dict(providers or {})

        # threading.Lock (not asyncio.Lock) — register() is synchronous and
        # must be callable outside a running event loop (CLI, admin thread).
        # Never held across an await, so it does not block the event loop.
        self._lock = threading.Lock()

    # ── Registration ──────────────────────────────────────────────────────────

    def register(self, tenant_id: str, provider: RepositoryProvider) -> None:
        """
        Register or replace the backend provider for a tenant.

        Safe to call at any time, including while requests are being served.

        Args:
            tenant_id: Unique tenant identifier.  Case-sensitive.
            provider:  Configured ``RepositoryProvider`` with ``register()``
                       already called.  For Beanie, ``init()`` must be awaited
                       separately before the first ``make_uow()`` for this tenant.

        Edge cases:
            - Replaces an existing provider silently.  In-flight ``AsyncUnitOfWork``
              instances from the old provider are unaffected.
            - There is no ``unregister()`` — decommission a tenant at the HTTP
              routing layer before removing it from the dict.

        Thread safety: ✅ Protected by ``threading.Lock``.
        """
        with self._lock:
            self._providers[tenant_id] = provider

    def has_tenant(self, tenant_id: str) -> bool:
        """
        Return ``True`` if a provider is registered for ``tenant_id``.

        Args:
            tenant_id: Tenant ID to check.

        Returns:
            ``True`` when the tenant is registered.

        Thread safety: ✅ Protected by ``threading.Lock``.
        """
        with self._lock:
            return tenant_id in self._providers

    def registered_tenants(self) -> list[str]:
        """
        Return a sorted list of all currently registered tenant IDs.

        Returns:
            Alphabetically sorted list.  Empty list when no tenants registered.

        Thread safety: ✅ Operates on a snapshot taken under the lock.
        """
        with self._lock:
            return sorted(self._providers)

    # ── IUoWProvider ──────────────────────────────────────────────────────────

    def make_uow(self) -> Any:
        """
        Return a fresh ``AsyncUnitOfWork`` for the currently active tenant.

        Reads the tenant ID from the request-scoped ``ContextVar`` and
        delegates to the matching backend provider.

        Returns:
            A fresh ``AsyncUnitOfWork`` from the active tenant's provider.

        Raises:
            RuntimeError: No tenant active (called outside ``tenant_context()``).
            KeyError:     Active tenant ID is not registered.

        Async safety: ✅ Lock released before delegate ``make_uow()`` call.
        """
        tid = _current_tenant.get()
        if tid is None:
            raise RuntimeError(
                "TenantUoWProvider.make_uow() called outside a tenant_context() block. "
                "Wrap each request with: with tenant_context(tenant_id): ..."
            )

        # Snapshot under lock — prevents a concurrent register() from causing
        # a spurious KeyError on a tenant being added at this exact moment.
        with self._lock:
            provider = self._providers.get(tid)

        if provider is None:
            raise KeyError(
                f"Tenant {tid!r} is not registered. "
                f"Registered tenants: {self.registered_tenants()!r}. "
                "Call provider.register(tenant_id, repository_provider) before "
                "routing requests for this tenant."
            )

        # Delegate outside the lock — provider.make_uow() may involve I/O
        # (e.g. acquiring from a connection pool) and must not hold the lock.
        return provider.make_uow()

    def __repr__(self) -> str:
        return f"TenantUoWProvider(tenants={self.registered_tenants()!r})"


# ── TenantAwareService ────────────────────────────────────────────────────────


class TenantAwareService(AsyncService[D, PK, C, R, U], ABC, Generic[D, PK, C, R, U]):
    """
    Abstract ``AsyncService`` mixin that enforces row-level tenant isolation.

    Implements the three ``AsyncService`` composable hooks to inject
    tenant scoping without duplicating CRUD method bodies:

    - ``_scoped_params`` — prepends ``tenant_id = <tid>`` to every query.
    - ``_check_entity``  — raises ``ServiceNotFoundError`` for cross-tenant access.
    - ``_prepare_for_create`` — stamps ``tenant_id`` from ``ctx`` after assembly.

    All five CRUD operations are tenant-safe via these hooks without
    overriding a single CRUD method.  The assembler does **not** need to
    handle ``tenant_id`` — ``_prepare_for_create`` stamps it from the
    authenticated JWT, making it impossible for a DTO to carry a foreign
    tenant ID.

    Cross-tenant access raises ``ServiceNotFoundError`` (404) rather than
    ``ServiceAuthorizationError`` (403) to prevent an existence oracle.

    Composable with other mixins via Python's MRO::

        # Tenant + soft-delete together — each hook chains via super()
        class PostService(TenantAwareService, SoftDeleteService, AsyncService[...]): ...

    Subclass contract
    -----------------
    1. Implement ``_get_repo(uow)`` — only required override.
    2. Optionally set ``_tenant_field`` when the model column name differs
       from ``"tenant_id"``.

    Class attributes:
        _tenant_field: Model column name for tenant scoping.
                       Defaults to ``"tenant_id"``.

    Thread safety:  ⚠️ Inherits ``AsyncService`` singleton contract —
                       each public method creates its own UoW.
    Async safety:   ✅ Reads immutable ``ctx.metadata`` — safe across tasks.

    Edge cases:
        - ``ctx.metadata["tenant_id"]`` absent → ``ServiceAuthorizationError``
          before any DB access.
        - ``_tenant_field`` absent on the domain model → ``TypeError`` from
          ``dataclasses.replace()`` at ``create()`` time (fail-fast).
        - ``params.node is None`` on ``list`` → only the tenant filter applies.
        - ``params.node`` set → ``tenant_id = <t> AND (<user_filter>)``.

    Example::

        @Singleton
        class PostService(
            TenantAwareService[Post, int, CreatePostDTO, PostReadDTO, UpdatePostDTO]
        ):
            def __init__(
                self,
                uow_provider: Inject[IUoWProvider],
                authorizer:   Inject[AbstractAuthorizer],
                assembler:    Inject[AbstractDTOAssembler[Post, CreatePostDTO, PostReadDTO, UpdatePostDTO]],
            ) -> None:
                super().__init__(uow_provider=uow_provider, authorizer=authorizer, assembler=assembler)

            def _get_repo(self, uow): return uow.posts
    """

    _tenant_field: ClassVar[str] = "tenant_id"

    # ── Composable hooks ──────────────────────────────────────────────────────

    def _scoped_params(self, params: QueryParams, ctx: AuthContext) -> QueryParams:
        """
        Prepend ``<_tenant_field> = <tid>`` to the query filter.

        Chains via ``super()`` so other mixins in the MRO (e.g.
        ``SoftDeleteService``) can also inject their own filters.

        Args:
            params: Incoming query parameters.
            ctx:    Caller's identity — ``ctx.metadata["tenant_id"]`` required.

        Returns:
            New ``QueryParams`` with tenant filter prepended.

        Raises:
            ServiceAuthorizationError: ``tenant_id`` absent or empty in
                ``ctx.metadata``.
        """
        tid = self._require_tenant(ctx)
        # AND the tenant filter onto the caller's filter — QueryBuilder.and_()
        # handles the ``params.node is None`` case without an explicit branch.
        scoped_node = (
            QueryBuilder()
            .eq(self._tenant_field, tid)
            .and_(QueryBuilder(params.node))
            .build()
        )
        scoped = dataclasses.replace(params, node=scoped_node)
        # Chain to super so additional mixins can also inject filters.
        return super()._scoped_params(scoped, ctx)

    def _check_entity(self, entity: D, ctx: AuthContext) -> None:
        """
        Raise ``ServiceNotFoundError`` when the entity belongs to a different tenant.

        Uses 404 (not 403) to prevent an existence oracle — a 403 would
        reveal that the entity exists in another tenant's data.

        Args:
            entity: Fetched domain entity.
            ctx:    Caller's identity — ``ctx.metadata["tenant_id"]`` required.

        Raises:
            ServiceAuthorizationError: ``tenant_id`` absent in ``ctx.metadata``.
            ServiceNotFoundError:      Entity's tenant field does not match.
        """
        tid = self._require_tenant(ctx)
        self._assert_same_tenant(entity, tid, entity.pk)
        # Chain to super so other mixins (e.g. SoftDeleteService) run too.
        super()._check_entity(entity, ctx)

    def _prepare_for_create(self, entity: D, ctx: AuthContext) -> D:
        """
        Stamp ``tenant_id`` from ``ctx`` onto the entity before save.

        The assembler's ``to_domain()`` is intentionally unaware of tenancy —
        this hook stamps it from the authenticated JWT so the DTO cannot
        supply a foreign tenant ID.

        Args:
            entity: Freshly assembled, unsaved domain entity.
            ctx:    Caller's identity — ``ctx.metadata["tenant_id"]`` required.

        Returns:
            New entity with ``_tenant_field`` set to the caller's tenant ID.

        Raises:
            ServiceAuthorizationError: ``tenant_id`` absent in ``ctx.metadata``.
            TypeError:                 ``_tenant_field`` not declared with
                ``init=True`` on the domain model — fix the declaration.
        """
        tid = self._require_tenant(ctx)
        # dataclasses.replace preserves _raw_orm (None on new entities) and
        # all other fields; only the named kwarg is changed.
        # type: ignore[return-value] because dataclasses.replace returns D but
        # mypy infers DomainModel for the **{field: val} unpacking pattern.
        stamped: D = dataclasses.replace(entity, **{self._tenant_field: tid})  # type: ignore[assignment]
        # Chain to super so additional mixins can stamp their own fields.
        return super()._prepare_for_create(stamped, ctx)

    def _pre_check(self, ctx: AuthContext) -> None:
        """
        Verify the tenant ID is present in ``ctx`` before any I/O.

        Fires before ``make_uow()`` in every CRUD operation.  This ensures
        that a missing tenant ID raises ``ServiceAuthorizationError`` without
        opening a DB connection — important both for efficiency and for
        deterministic test behaviour (UoW is never created on denied calls).

        DESIGN: ``_pre_check`` over duplicating the call in each CRUD override
            The 4th composable hook is the right place for "bail before I/O"
            checks.  Overriding individual CRUD methods would re-introduce
            the duplication the hook system was designed to eliminate.

        Args:
            ctx: Caller's identity — must carry ``tenant_id`` in ``metadata``.

        Raises:
            ServiceAuthorizationError: ``tenant_id`` is absent in
                ``ctx.metadata`` — the call is denied before any DB access.
        """
        # Calling _require_tenant() here causes a fast fail before make_uow().
        # The return value is intentionally discarded — we only want the check.
        self._require_tenant(ctx)
        # Chain to super so other mixins can also run their pre-checks.
        super()._pre_check(ctx)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _require_tenant(self, ctx: AuthContext) -> str:
        """
        Return the tenant ID from ``ctx.metadata`` or raise.

        Args:
            ctx: Caller's ``AuthContext``.

        Returns:
            The non-empty tenant ID string.

        Raises:
            ServiceAuthorizationError: ``tenant_id`` absent or falsy in
                ``ctx.metadata``.
        """
        tid = ctx.metadata.get("tenant_id")
        if not tid:
            raise ServiceAuthorizationError("access", self._entity_type())
        return tid

    def _assert_same_tenant(self, entity: DomainModel, tid: str, pk: Any) -> None:
        """
        Raise ``ServiceNotFoundError`` when the entity belongs to a different tenant.

        Uses 404 (not 403) to prevent an existence oracle — a 403 would
        reveal that the entity exists in another tenant's data.

        Args:
            entity: The fetched domain entity.
            tid:    Expected (caller's) tenant ID.
            pk:     Entity PK — passed to ``ServiceNotFoundError`` for its message.

        Raises:
            ServiceNotFoundError: Entity's ``_tenant_field`` does not match ``tid``.
        """
        entity_tid = getattr(entity, self._tenant_field, None)
        if entity_tid != tid:
            # Same error as "entity not found" — indistinguishable to the caller.
            raise ServiceNotFoundError(pk, type(entity))
