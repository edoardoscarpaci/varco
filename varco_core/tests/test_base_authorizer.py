"""
Unit tests for varco_core.base_authorizer
=============================================
Covers:
  - _FALLBACK_PRIORITY constant value and type
  - BaseAuthorizer.authorize() — permissive for every combination of
    ctx / action / resource (anonymous, authenticated, any action, collection
    and instance-level resources)
  - BaseAuthorizer is a proper AbstractAuthorizer subclass
  - DI injection via providify — BaseAuthorizer is resolved as AbstractAuthorizer
    when no other binding is registered
  - DI shadowing — a custom authorizer at default priority (0) is returned
    instead of BaseAuthorizer
  - Singleton scope — same instance is returned across multiple container.get() calls

DESIGN: why a fresh DIContainer per test
    Each test uses an isolated ``DIContainer`` so that a singleton cached
    during one test cannot pollute a later test.  Shared container state is
    the most common source of order-dependent test failures in DI-heavy code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pytest
from providify import DIContainer, Singleton

from varco_core.auth import (
    AbstractAuthorizer,
    Action,
    AuthContext,
    Resource,
    ResourceGrant,
)
from varco_core.auth.authorizer import BaseAuthorizer, _FALLBACK_PRIORITY
from varco_core.meta import PKStrategy, PrimaryKey, pk_field
from varco_core.model import DomainModel


# ── Test fixtures ─────────────────────────────────────────────────────────────


@dataclass
class Post(DomainModel):
    """
    Minimal domain entity used for resource construction in tests.

    Uses STR_ASSIGNED pk so pk can be passed directly in the constructor.
    pk is declared before title because pk_field(init=True) has default=None,
    and Python forbids a non-default field from following a default one.
    """

    # STR_ASSIGNED + init=True: pk is part of __init__ with default=None.
    pk: Annotated[str, PrimaryKey(strategy=PKStrategy.STR_ASSIGNED)] = pk_field(
        init=True
    )
    # Empty default keeps Post constructible without title for collection tests.
    title: str = ""

    class Meta:
        table = "posts"


@pytest.fixture()
def fresh_container() -> DIContainer:
    """
    Return a brand-new DIContainer with only BaseAuthorizer scanned.

    A fresh container is created for every test so that singleton instances
    cached by one test never leak into another — the most common source of
    order-dependent DI test failures.

    Returns:
        Empty ``DIContainer`` pre-scanned for ``base_authorizer`` module.
    """
    # Scan the module so the @Singleton(priority=_FALLBACK_PRIORITY) metadata
    # is registered — this is the path we actually want to test.
    container = DIContainer()
    container.scan("varco_core.auth.authorizer")
    return container


@pytest.fixture()
def anon_ctx() -> AuthContext:
    """Anonymous caller with no grants — verifies BaseAuthorizer ignores ctx."""
    return AuthContext()


@pytest.fixture()
def auth_ctx() -> AuthContext:
    """Authenticated caller with READ grant — also ignored by BaseAuthorizer."""
    return AuthContext(
        user_id="usr_1",
        grants=(ResourceGrant("posts", frozenset({Action.READ})),),
    )


@pytest.fixture()
def collection_resource() -> Resource:
    """Collection-level resource — entity is None (CREATE / LIST scenario)."""
    return Resource(entity_type=Post)


@pytest.fixture()
def instance_resource() -> Resource:
    """Instance-level resource — entity is set (READ / UPDATE / DELETE scenario)."""
    return Resource(entity_type=Post, entity=Post(pk="post-1", title="hello"))


# ── _FALLBACK_PRIORITY constant ───────────────────────────────────────────────


class TestFallbackPriority:
    """_FALLBACK_PRIORITY is the public contract for the absolute minimum priority."""

    def test_value_is_negative_2_to_the_31(self):
        # The constant must equal -(2**31) — documented as INT_MIN equivalent.
        # Any change to this value is a breaking change for downstream code
        # that registers intermediate-priority authorizers.
        assert _FALLBACK_PRIORITY == -(2**31)

    def test_value_is_int(self):
        # Must be a plain int — providify expects int for priority, not float.
        assert isinstance(_FALLBACK_PRIORITY, int)

    def test_lower_than_default_priority(self):
        # Default providify priority is 0.  _FALLBACK_PRIORITY must be strictly
        # lower so any default-priority binding shadows it.
        assert _FALLBACK_PRIORITY < 0

    def test_lower_than_minus_one(self):
        # Leaves the range -1 … -2_147_483_647 free for intermediate fallbacks
        # without any re-prioritization of existing bindings.
        assert _FALLBACK_PRIORITY < -1

    def test_representable_as_32_bit_signed_int(self):
        # -(2**31) is the minimum value of a 32-bit signed integer.
        # Values below this would be outside the conventional INT_MIN range.
        int32_min = -(2**31)
        assert _FALLBACK_PRIORITY >= int32_min


# ── BaseAuthorizer class-level properties ─────────────────────────────────────


class TestBaseAuthorizerClass:
    """BaseAuthorizer must be a valid AbstractAuthorizer subclass."""

    def test_is_subclass_of_abstract_authorizer(self):
        # BaseAuthorizer must satisfy the AbstractAuthorizer contract so it
        # can be injected wherever AbstractAuthorizer is expected.
        assert issubclass(BaseAuthorizer, AbstractAuthorizer)

    def test_can_be_instantiated(self):
        # It is concrete — no abstract methods left unimplemented.
        instance = BaseAuthorizer()
        assert isinstance(instance, BaseAuthorizer)
        assert isinstance(instance, AbstractAuthorizer)

    def test_authorize_is_a_coroutine_function(self):
        # authorize() must be async so it can be awaited by service code.
        import inspect

        assert inspect.iscoroutinefunction(BaseAuthorizer.authorize)


# ── BaseAuthorizer.authorize() — permissive behaviour ─────────────────────────


class TestBaseAuthorizerAuthorize:
    """
    authorize() must never raise for any combination of ctx, action, resource.
    Tests cover: anonymous vs authenticated, every Action, collection vs instance.
    """

    async def test_allows_anonymous_context_collection(
        self, anon_ctx: AuthContext, collection_resource: Resource
    ):
        # BaseAuthorizer must not care that the caller is anonymous —
        # it is a permissive no-op for ALL callers.
        authorizer = BaseAuthorizer()
        await authorizer.authorize(anon_ctx, Action.LIST, collection_resource)

    async def test_allows_anonymous_context_instance(
        self, anon_ctx: AuthContext, instance_resource: Resource
    ):
        authorizer = BaseAuthorizer()
        await authorizer.authorize(anon_ctx, Action.READ, instance_resource)

    async def test_allows_authenticated_context(
        self, auth_ctx: AuthContext, collection_resource: Resource
    ):
        authorizer = BaseAuthorizer()
        await authorizer.authorize(auth_ctx, Action.CREATE, collection_resource)

    async def test_allows_every_action_on_collection(
        self, anon_ctx: AuthContext, collection_resource: Resource
    ):
        # All five actions must succeed — this is the permissive guarantee.
        authorizer = BaseAuthorizer()
        for action in Action:
            # Must not raise ServiceAuthorizationError or any other exception.
            await authorizer.authorize(anon_ctx, action, collection_resource)

    async def test_allows_every_action_on_instance(
        self, anon_ctx: AuthContext, instance_resource: Resource
    ):
        authorizer = BaseAuthorizer()
        for action in Action:
            await authorizer.authorize(anon_ctx, action, instance_resource)

    async def test_returns_none(
        self, anon_ctx: AuthContext, collection_resource: Resource
    ):
        # authorize() must return None on success — callers treat non-None
        # returns as errors, so an accidental truthy return would be a bug.
        authorizer = BaseAuthorizer()
        result = await authorizer.authorize(anon_ctx, Action.READ, collection_resource)
        assert result is None

    async def test_allows_context_without_grants(self, collection_resource: Resource):
        # AuthContext with no grants at all — BaseAuthorizer still allows.
        ctx = AuthContext(user_id="usr_2", grants=())
        authorizer = BaseAuthorizer()
        await authorizer.authorize(ctx, Action.DELETE, collection_resource)

    async def test_allows_context_with_metadata(self, collection_resource: Resource):
        # Metadata in AuthContext must not affect the permissive decision.
        ctx = AuthContext(
            user_id="usr_3",
            metadata={"tenant_id": "tenant_abc", "request_id": "req_xyz"},
        )
        authorizer = BaseAuthorizer()
        await authorizer.authorize(ctx, Action.UPDATE, collection_resource)


# ── DI injection — BaseAuthorizer as fallback AbstractAuthorizer ───────────────


class TestBaseAuthorizerDI:
    """
    Verify that BaseAuthorizer is correctly wired into the DI container
    and that the priority-based shadowing mechanism works as documented.
    """

    def test_resolves_as_abstract_authorizer(self, fresh_container: DIContainer):
        # The container must resolve AbstractAuthorizer to a BaseAuthorizer
        # instance when no other binding is registered.
        # This validates that @Singleton(priority=_FALLBACK_PRIORITY) set up
        # the binding correctly and the class is reachable via its interface.
        authorizer = fresh_container.get(AbstractAuthorizer)
        assert isinstance(authorizer, BaseAuthorizer)

    def test_returns_same_instance_singleton(self, fresh_container: DIContainer):
        # @Singleton scope must return the exact same object every time —
        # not a new instance per call.  Sharing one authorizer across all
        # requests is the whole point of singleton scope.
        first = fresh_container.get(AbstractAuthorizer)
        second = fresh_container.get(AbstractAuthorizer)
        assert first is second

    def test_shadowed_by_higher_priority_authorizer(self):
        # This is the core promise of the framework: registering a custom
        # authorizer at the default priority (0) must shadow BaseAuthorizer.
        #
        # DESIGN: We define the replacement inside the test to avoid polluting
        # the module namespace, then scan/register it into a fresh container.

        @Singleton
        class AppAuthorizer(AbstractAuthorizer):
            """Custom authorizer that shadows the base implementation."""

            async def authorize(self, ctx, action, resource) -> None:
                # In a real app this would enforce business rules.
                pass

        container = DIContainer()
        # Scan base authorizer (priority = -(2**31)) — always registered first.
        container.scan("varco_core.auth.authorizer")
        # Manually register the higher-priority custom authorizer.
        # Default @Singleton priority is 0, which beats -(2**31).
        container.bind(AbstractAuthorizer, AppAuthorizer)

        resolved = container.get(AbstractAuthorizer)

        # The container must return the higher-priority AppAuthorizer,
        # NOT the fallback BaseAuthorizer.
        assert isinstance(resolved, AppAuthorizer)
        assert not isinstance(resolved, BaseAuthorizer)

    def test_base_authorizer_used_when_no_other_authorizer(self):
        # Explicitly verify the fallback path: a container with ONLY the
        # base_authorizer module scanned must resolve to BaseAuthorizer.
        container = DIContainer()
        container.scan("varco_core.auth.authorizer")
        resolved = container.get(AbstractAuthorizer)
        assert isinstance(resolved, BaseAuthorizer)

    async def test_di_resolved_instance_works_end_to_end(
        self, fresh_container: DIContainer
    ):
        # Full round-trip: resolve from container, then call authorize().
        # Validates that the DI-constructed instance is actually functional —
        # not just the right type.
        authorizer = fresh_container.get(AbstractAuthorizer)
        ctx = AuthContext()
        resource = Resource(entity_type=Post)

        # Must not raise — BaseAuthorizer allows unconditionally.
        await authorizer.authorize(ctx, Action.CREATE, resource)
