"""
Unit tests for fastrest_core.auth
===================================
Covers: Action, ResourceGrant, AuthContext (all helpers), Resource.

These are pure-value / pure-logic tests — no I/O, no async, no DI.
Every test creates fresh objects from scratch so there are no shared-state
side-effects between test cases.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pytest

from fastrest_core.auth import (
    AbstractAuthorizer,
    Action,
    AuthContext,
    Resource,
    ResourceGrant,
)
from fastrest_core.meta import PKStrategy, PrimaryKey, pk_field
from fastrest_core.model import DomainModel


# ── Fixtures ──────────────────────────────────────────────────────────────────


@dataclass
class Post(DomainModel):
    """
    Minimal domain entity used across all auth tests.

    Uses STR_ASSIGNED pk so the pk can be passed directly to the constructor
    without post-construction attribute assignment magic.
    pk must be declared before title because pk_field(init=True) has a
    default of None — Python dataclass rules forbid a non-default field
    (title) from following a field with a default (pk).
    """

    # STR_ASSIGNED + pk_field(init=True): pk is part of __init__ and has
    # default=None so it can precede title without causing a dataclass error.
    pk: Annotated[str, PrimaryKey(strategy=PKStrategy.STR_ASSIGNED)] = pk_field(
        init=True
    )
    # Default empty string keeps Post constructible without title when only
    # the type is needed (collection-level resource tests).
    title: str = ""

    class Meta:
        table = "posts"


@dataclass
class Comment(DomainModel):
    """Second domain entity to verify multi-type dispatch in test helpers."""

    body: str = ""

    class Meta:
        table = "comments"


# ── Action ────────────────────────────────────────────────────────────────────


class TestAction:
    """Action is a StrEnum — each member is simultaneously an Action and a str."""

    def test_action_values_are_strings(self):
        # StrEnum contract: every member compares equal to its string value.
        # This is critical for JWT round-trips where raw strings come back.
        assert Action.CREATE == "create"
        assert Action.READ == "read"
        assert Action.UPDATE == "update"
        assert Action.DELETE == "delete"
        assert Action.LIST == "list"

    def test_action_is_str_instance(self):
        # isinstance check guarantees no extra cast is needed in serializers.
        assert isinstance(Action.READ, str)

    def test_action_members_are_hashable(self):
        # Actions must be hashable — they are stored in frozenset[Action].
        # A frozenset constructor raises TypeError if any element is unhashable.
        s: frozenset[Action] = frozenset({Action.READ, Action.CREATE})
        assert Action.READ in s

    def test_action_all_members_covered(self):
        # Guard against silent omissions — if a new member is added without
        # a corresponding test, this assertion will fail as a reminder.
        assert set(Action) == {
            Action.CREATE,
            Action.READ,
            Action.UPDATE,
            Action.DELETE,
            Action.LIST,
        }


# ── ResourceGrant ─────────────────────────────────────────────────────────────


class TestResourceGrant:
    """ResourceGrant is a frozen dataclass — immutable, hashable, value-equal."""

    def test_equality_by_value(self):
        # Two grants with same resource and same actions are equal —
        # frozen=True dataclasses compare field-by-field.
        g1 = ResourceGrant("posts", frozenset({Action.READ}))
        g2 = ResourceGrant("posts", frozenset({Action.READ}))
        assert g1 == g2

    def test_different_actions_not_equal(self):
        g1 = ResourceGrant("posts", frozenset({Action.READ}))
        g2 = ResourceGrant("posts", frozenset({Action.CREATE}))
        assert g1 != g2

    def test_different_resource_not_equal(self):
        g1 = ResourceGrant("posts", frozenset({Action.READ}))
        g2 = ResourceGrant("comments", frozenset({Action.READ}))
        assert g1 != g2

    def test_hashable(self):
        # ResourceGrant must be hashable to be stored in sets and dict keys.
        grants = {
            ResourceGrant("posts", frozenset({Action.READ})),
            ResourceGrant(
                "posts", frozenset({Action.READ})
            ),  # duplicate — deduplicated
        }
        assert len(grants) == 1

    def test_immutable(self):
        # frozen=True raises FrozenInstanceError on any attribute write.
        # This is intentional — grants must never be mutated after JWT decode.
        grant = ResourceGrant("posts", frozenset({Action.READ}))
        with pytest.raises(Exception):
            grant.resource = "other"  # type: ignore[misc]

    def test_empty_actions_is_valid(self):
        # An empty actions set is a no-op grant — valid but grants nothing.
        # The authorizer interprets it, not the grant itself.
        grant = ResourceGrant("posts", frozenset())
        assert grant.actions == frozenset()

    def test_wildcard_resource_key(self):
        # "*" is a valid resource key — signals admin-level access.
        # The authorizer must explicitly handle it; ResourceGrant is unaware.
        grant = ResourceGrant("*", frozenset(Action))
        assert grant.resource == "*"
        assert grant.actions == frozenset(Action)


# ── AuthContext ───────────────────────────────────────────────────────────────


class TestAuthContextIdentity:
    """Tests for is_anonymous(), has_role(), has_scope() helpers."""

    def test_anonymous_when_user_id_is_none(self):
        # None user_id is the canonical "unauthenticated" sentinel.
        ctx = AuthContext()
        assert ctx.is_anonymous() is True

    def test_not_anonymous_with_user_id(self):
        # Any non-None user_id counts as authenticated — even empty string.
        ctx = AuthContext(user_id="usr_123")
        assert ctx.is_anonymous() is False

    def test_empty_string_user_id_is_not_anonymous(self):
        # Edge case: "" is still a user_id — callers must use None for anon.
        ctx = AuthContext(user_id="")
        assert ctx.is_anonymous() is False

    def test_has_role_present(self):
        ctx = AuthContext(roles=frozenset({"admin", "editor"}))
        assert ctx.has_role("admin") is True

    def test_has_role_absent(self):
        ctx = AuthContext(roles=frozenset({"editor"}))
        assert ctx.has_role("admin") is False

    def test_has_role_case_sensitive(self):
        # Role names are case-sensitive — "Admin" ≠ "admin".
        ctx = AuthContext(roles=frozenset({"Admin"}))
        assert ctx.has_role("admin") is False

    def test_has_scope_present(self):
        ctx = AuthContext(scopes=frozenset({"write:posts"}))
        assert ctx.has_scope("write:posts") is True

    def test_has_scope_absent(self):
        ctx = AuthContext(scopes=frozenset({"read:posts"}))
        assert ctx.has_scope("write:posts") is False

    def test_default_roles_and_scopes_are_empty(self):
        # Defaults must be empty frozensets — never None — so callers can
        # always safely iterate or call `in` without a None check.
        ctx = AuthContext()
        assert ctx.roles == frozenset()
        assert ctx.scopes == frozenset()


class TestAuthContextCan:
    """Tests for the central can(action, resource_key) permission check."""

    # ── wildcard grant ──────────────────────────────────────────────────

    def test_wildcard_grant_allows_all_actions(self):
        # "*" resource key grants everything regardless of the specific key.
        ctx = AuthContext(
            user_id="usr_1",
            grants=(ResourceGrant("*", frozenset(Action)),),
        )
        for action in Action:
            assert ctx.can(action, "posts") is True
            assert ctx.can(action, "posts:abc") is True
            assert ctx.can(action, "anything") is True

    def test_wildcard_grant_partial_actions(self):
        # "*" resource but only READ — should only grant READ, not DELETE.
        ctx = AuthContext(
            grants=(ResourceGrant("*", frozenset({Action.READ})),),
        )
        assert ctx.can(Action.READ, "posts") is True
        assert ctx.can(Action.DELETE, "posts") is False

    # ── type-level grant ────────────────────────────────────────────────

    def test_type_level_grant_allows_matching_action(self):
        ctx = AuthContext(
            grants=(ResourceGrant("posts", frozenset({Action.READ, Action.LIST})),),
        )
        assert ctx.can(Action.READ, "posts") is True
        assert ctx.can(Action.LIST, "posts") is True

    def test_type_level_grant_denies_ungranted_action(self):
        ctx = AuthContext(
            grants=(ResourceGrant("posts", frozenset({Action.READ})),),
        )
        assert ctx.can(Action.DELETE, "posts") is False

    def test_type_level_grant_does_not_apply_to_other_resource(self):
        # "posts" grant must not bleed into "comments".
        ctx = AuthContext(
            grants=(ResourceGrant("posts", frozenset({Action.READ})),),
        )
        assert ctx.can(Action.READ, "comments") is False

    # ── instance-level grant ────────────────────────────────────────────

    def test_instance_level_grant_allows_specific_key(self):
        ctx = AuthContext(
            grants=(ResourceGrant("posts:abc123", frozenset({Action.UPDATE})),),
        )
        assert ctx.can(Action.UPDATE, "posts:abc123") is True

    def test_instance_level_grant_does_not_apply_to_type_key(self):
        # "posts:abc123" should NOT grant access to all posts — only that one.
        ctx = AuthContext(
            grants=(ResourceGrant("posts:abc123", frozenset({Action.UPDATE})),),
        )
        assert ctx.can(Action.UPDATE, "posts") is False

    def test_instance_level_grant_does_not_apply_to_other_instance(self):
        ctx = AuthContext(
            grants=(ResourceGrant("posts:abc123", frozenset({Action.DELETE})),),
        )
        assert ctx.can(Action.DELETE, "posts:xyz999") is False

    # ── no grants ───────────────────────────────────────────────────────

    def test_no_grants_always_false(self):
        ctx = AuthContext()
        for action in Action:
            assert ctx.can(action, "posts") is False

    def test_empty_grant_actions_always_false(self):
        # A grant with an empty actions set is a no-op — can() returns False.
        ctx = AuthContext(
            grants=(ResourceGrant("posts", frozenset()),),
        )
        assert ctx.can(Action.READ, "posts") is False

    # ── multiple grants ─────────────────────────────────────────────────

    def test_multiple_grants_any_match_returns_true(self):
        # can() returns True as soon as any single grant matches — OR logic.
        ctx = AuthContext(
            grants=(
                ResourceGrant("posts", frozenset({Action.READ})),
                ResourceGrant("posts", frozenset({Action.DELETE})),
            ),
        )
        assert ctx.can(Action.READ, "posts") is True
        assert ctx.can(Action.DELETE, "posts") is True

    def test_resource_key_asterisk_not_special_without_grant(self):
        # Passing "*" as the resource_key argument to can() does NOT grant
        # everything — only a ResourceGrant with resource="*" does that.
        ctx = AuthContext(
            grants=(ResourceGrant("posts", frozenset({Action.READ})),),
        )
        # Checking against literal "*" key — no grant has resource="*",
        # but there IS a grant for "posts" which doesn't match "*".
        assert ctx.can(Action.READ, "*") is False


class TestAuthContextGrantsFor:
    """Tests for the grants_for(resource_key) union helper."""

    def test_returns_union_of_matching_grants(self):
        ctx = AuthContext(
            grants=(
                ResourceGrant("posts", frozenset({Action.READ, Action.LIST})),
                ResourceGrant("posts", frozenset({Action.CREATE})),
            ),
        )
        # Both grants apply to "posts" — their actions must be merged.
        result = ctx.grants_for("posts")
        assert result == frozenset({Action.READ, Action.LIST, Action.CREATE})

    def test_includes_wildcard_grant_actions(self):
        ctx = AuthContext(
            grants=(
                ResourceGrant("*", frozenset({Action.DELETE})),
                ResourceGrant("posts", frozenset({Action.READ})),
            ),
        )
        # "posts" grant + "*" wildcard grant both contribute.
        result = ctx.grants_for("posts")
        assert Action.DELETE in result
        assert Action.READ in result

    def test_returns_empty_frozenset_when_no_match(self):
        ctx = AuthContext(
            grants=(ResourceGrant("comments", frozenset({Action.READ})),),
        )
        result = ctx.grants_for("posts")
        assert result == frozenset()

    def test_no_prefix_matching(self):
        # "posts" should NOT match grants for "posts:abc123" — no prefix logic.
        ctx = AuthContext(
            grants=(ResourceGrant("posts:abc123", frozenset({Action.UPDATE})),),
        )
        result = ctx.grants_for("posts")
        assert result == frozenset()


# ── Resource ──────────────────────────────────────────────────────────────────


class TestResource:
    """Resource describes what is being acted upon — collection vs. instance."""

    def test_is_collection_when_entity_is_none(self):
        # Collection-level: no entity loaded yet — typical for LIST / CREATE.
        r = Resource(entity_type=Post)
        assert r.is_collection is True

    def test_is_not_collection_when_entity_is_set(self):
        # Instance-level: specific entity — typical for READ / UPDATE / DELETE.
        r = Resource(entity_type=Post, entity=Post(pk="post-1", title="hello"))
        assert r.is_collection is False

    def test_entity_type_and_entity_stored(self):
        post = Post(pk="post-1", title="hello")
        r = Resource(entity_type=Post, entity=post)
        assert r.entity_type is Post
        assert r.entity is post

    def test_resource_is_frozen(self):
        r = Resource(entity_type=Post)
        with pytest.raises(Exception):
            r.entity_type = Comment  # type: ignore[misc]

    def test_resource_equality_by_value(self):
        # Two collection-level resources for the same type are equal.
        assert Resource(entity_type=Post) == Resource(entity_type=Post)

    def test_resource_hashable(self):
        # Must be hashable — it may be used as a dict key or set member.
        s = {Resource(entity_type=Post), Resource(entity_type=Post)}
        assert len(s) == 1

    def test_different_entity_types_not_equal(self):
        assert Resource(entity_type=Post) != Resource(entity_type=Comment)


# ── AbstractAuthorizer ────────────────────────────────────────────────────────


class TestAbstractAuthorizer:
    """AbstractAuthorizer is an abstract class — cannot be instantiated directly."""

    def test_cannot_instantiate_directly(self):
        # AbstractAuthorizer has an abstract method — instantiating it must
        # raise TypeError from Python's ABC machinery.
        with pytest.raises(TypeError):
            AbstractAuthorizer()  # type: ignore[abstract]

    def test_concrete_subclass_must_implement_authorize(self):
        # A subclass that doesn't override authorize() is still abstract.
        class IncompleteAuthorizer(AbstractAuthorizer):
            pass

        with pytest.raises(TypeError):
            IncompleteAuthorizer()  # type: ignore[abstract]

    def test_concrete_subclass_can_be_instantiated(self):
        # A properly implemented subclass must be instantiable without error.
        class ConcreteAuthorizer(AbstractAuthorizer):
            async def authorize(self, ctx, action, resource) -> None:
                pass  # Minimal no-op implementation

        instance = ConcreteAuthorizer()
        assert isinstance(instance, AbstractAuthorizer)
