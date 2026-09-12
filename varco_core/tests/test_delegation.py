"""
Failing-first tests for varco_core.auth.delegation (Plan 033, Phase 6,
Step 37 — DROPPABLE per §D-S16-cut) — §D-S16-shape.
"""

from __future__ import annotations


def _mod():
    from varco_core.auth import delegation

    return delegation


class TestActorContextFromMetadata:
    def test_simple_actor_no_chain(self) -> None:
        mod = _mod()
        actor = mod.ActorContext.from_metadata({"actor": {"sub": "svc-a"}})

        assert actor is not None
        assert actor.subject == "svc-a"
        assert actor.chain == ()

    def test_nested_actor_chain_outermost_first(self) -> None:
        mod = _mod()
        actor = mod.ActorContext.from_metadata({"actor": {"sub": "svc-a", "act": {"sub": "svc-b"}}})

        assert actor is not None
        assert actor.chain == ("svc-a", "svc-b")

    def test_no_actor_claim_returns_none(self) -> None:
        mod = _mod()
        assert mod.ActorContext.from_metadata({}) is None

    def test_malformed_actor_string_returns_none_never_raises(self) -> None:
        mod = _mod()
        assert mod.ActorContext.from_metadata({"actor": "svc-a"}) is None

    def test_malformed_actor_list_returns_none_never_raises(self) -> None:
        mod = _mod()
        assert mod.ActorContext.from_metadata({"actor": ["svc-a"]}) is None

    def test_malformed_actor_dict_no_sub_returns_none_never_raises(self) -> None:
        mod = _mod()
        assert mod.ActorContext.from_metadata({"actor": {"no_sub": "x"}}) is None


class TestAllowlistDelegationPolicy:
    async def test_unlisted_actor_denied(self) -> None:
        mod = _mod()
        policy = mod.AllowlistDelegationPolicy(grants={"svc-billing": frozenset({"acme"})})
        actor = mod.ActorContext(subject="svc-unknown", chain=())

        assert await policy.allows(actor, "usr_1", "acme") is False

    async def test_listed_actor_unlisted_tenant_denied(self) -> None:
        mod = _mod()
        policy = mod.AllowlistDelegationPolicy(grants={"svc-billing": frozenset({"acme"})})
        actor = mod.ActorContext(subject="svc-billing", chain=())

        assert await policy.allows(actor, "usr_1", "beta") is False

    async def test_wildcard_allows(self) -> None:
        mod = _mod()
        policy = mod.AllowlistDelegationPolicy(grants={"svc-billing": "*"})
        actor = mod.ActorContext(subject="svc-billing", chain=())

        assert await policy.allows(actor, "usr_1", "any-tenant") is True

    async def test_empty_grants_denies_everything(self) -> None:
        mod = _mod()
        policy = mod.AllowlistDelegationPolicy(grants={})
        actor = mod.ActorContext(subject="svc-billing", chain=())

        assert await policy.allows(actor, "usr_1", "acme") is False
