"""
Failing tests for varco_core.query.applicator.tenant_guard (Plan 037 / Step
26, §D-S15-shape).

``assert_tenant_predicate()`` walks the AST (never compiled SQL) requiring
an equality comparison on ``tenant_field`` on the top-level ``AND`` spine.
Off by default, opt-in, dev-time-only — never a security control (Postgres
RLS, §S12, is). The wording-rule review gate (§D-S15-shape's ⛔) is asserted
mechanically here: the module's docstring must literally carry the sanctioned
disclaimer.
"""

from __future__ import annotations

import pytest
from varco_core.query.type import AndNode, ComparisonNode, Operation, OrNode


def _cmp(field: str, op: Operation, value: object = "acme") -> ComparisonNode:
    return ComparisonNode(field=field, op=op, value=value)


class TestAssertTenantPredicatePasses:
    def test_top_level_and_containing_tenant_equality_passes(self) -> None:
        from varco_core.query.applicator.tenant_guard import assert_tenant_predicate

        node = AndNode(
            left=_cmp("tenant_id", Operation.EQUAL),
            right=_cmp("status", Operation.EQUAL, "active"),
        )
        # Must not raise.
        assert_tenant_predicate(node, tenant_field="tenant_id", entity="Order")

    def test_bare_tenant_equality_with_no_and_passes(self) -> None:
        from varco_core.query.applicator.tenant_guard import assert_tenant_predicate

        node = _cmp("tenant_id", Operation.EQUAL)
        assert_tenant_predicate(node, tenant_field="tenant_id", entity="Order")

    def test_nested_and_under_top_level_and_passes(self) -> None:
        from varco_core.query.applicator.tenant_guard import assert_tenant_predicate

        inner = AndNode(
            left=_cmp("status", Operation.EQUAL, "active"),
            right=_cmp("age", Operation.GREATER_THAN, 18),
        )
        node = AndNode(left=_cmp("tenant_id", Operation.EQUAL), right=inner)
        assert_tenant_predicate(node, tenant_field="tenant_id", entity="Order")

    def test_custom_tenant_field_name_works(self) -> None:
        from varco_core.query.applicator.tenant_guard import assert_tenant_predicate

        node = AndNode(
            left=_cmp("org_id", Operation.EQUAL),
            right=_cmp("status", Operation.EQUAL, "active"),
        )
        assert_tenant_predicate(node, tenant_field="org_id", entity="Order")


class TestAssertTenantPredicateRaises:
    def test_no_tenant_predicate_at_all_raises_naming_entity_and_field(self) -> None:
        from varco_core.query.applicator.tenant_guard import (
            TenantFilterError,
            assert_tenant_predicate,
        )

        node = _cmp("status", Operation.EQUAL, "active")
        with pytest.raises(TenantFilterError) as exc:
            assert_tenant_predicate(node, tenant_field="tenant_id", entity="Order")
        message = str(exc.value)
        assert "Order" in message
        assert "tenant_id" in message

    def test_tenant_predicate_only_under_or_raises(self) -> None:
        """
        ``tenant_id = X OR status = 'public'`` contains a tenant predicate
        that constrains nothing — must raise, not pass (§D-S15-shape's
        false-positive guard).
        """
        from varco_core.query.applicator.tenant_guard import (
            TenantFilterError,
            assert_tenant_predicate,
        )

        node = OrNode(
            left=_cmp("tenant_id", Operation.EQUAL),
            right=_cmp("status", Operation.EQUAL, "public"),
        )
        with pytest.raises(TenantFilterError):
            assert_tenant_predicate(node, tenant_field="tenant_id", entity="Order")

    def test_not_equal_does_not_constrain_to_one_tenant_raises(self) -> None:
        from varco_core.query.applicator.tenant_guard import (
            TenantFilterError,
            assert_tenant_predicate,
        )

        node = _cmp("tenant_id", Operation.NOT_EQUAL)
        with pytest.raises(TenantFilterError):
            assert_tenant_predicate(node, tenant_field="tenant_id", entity="Order")

    def test_in_operator_does_not_constrain_to_one_tenant_raises(self) -> None:
        from varco_core.query.applicator.tenant_guard import (
            TenantFilterError,
            assert_tenant_predicate,
        )

        node = ComparisonNode(field="tenant_id", op=Operation.IN, value=["a", "b"])
        with pytest.raises(TenantFilterError):
            assert_tenant_predicate(node, tenant_field="tenant_id", entity="Order")

    def test_node_none_raises(self) -> None:
        from varco_core.query.applicator.tenant_guard import (
            TenantFilterError,
            assert_tenant_predicate,
        )

        with pytest.raises(TenantFilterError):
            assert_tenant_predicate(None, tenant_field="tenant_id", entity="Order")


class TestTenantGuardWordingRule:
    def test_module_docstring_carries_the_not_a_security_control_disclaimer(self) -> None:
        """
        §D-S15-shape's ⛔ wording rule is a review gate: this guard must
        never be described as enforcing/guaranteeing/securing tenant
        isolation. Mechanically asserted against the module's own docstring.
        """
        import varco_core.query.applicator.tenant_guard as module

        doc = module.__doc__ or ""
        assert "not a security control" in doc
        assert "RLS" in doc
