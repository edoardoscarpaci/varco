"""
Failing tests for varco_sa.rls_autogen (Plan 037 / Step 7, §D-S12-autogen).

RED until varco_sa/varco_sa/rls_autogen.py lands. No DB required — all
assertions are against ``plan_tenant_rls()``'s pure, inspectable output and
``render_tenant_rls_ddl()``'s string generation, exercised over a small
hand-built SQLAlchemy ``DeclarativeBase`` (no real domain models needed).
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase


class _TenantUuidModel:
    """Stand-in ParsedMeta-carrying domain class, UUID tenant column."""

    class Meta:
        tenant_scope = "tenant"


class _TenantStringModel:
    class Meta:
        tenant_scope = "tenant"


class _TenantIntModel:
    class Meta:
        tenant_scope = "tenant"


class _GlobalModel:
    class Meta:
        tenant_scope = "global"


class _NoTenantColumnModel:
    class Meta:
        tenant_scope = "tenant"


class _UnmappableColumnModel:
    class Meta:
        tenant_scope = "tenant"


def _table(base: type, name: str, *columns: sa.Column) -> sa.Table:
    return sa.Table(name, base.metadata, *columns)


@pytest.fixture
def base() -> type:
    """Fresh DeclarativeBase per test — avoids SA "Table already defined"."""

    class _Base(DeclarativeBase):
        pass

    return _Base


@pytest.fixture
def tables(base: type) -> dict[str, sa.Table]:
    """
    Physical tables backing the stand-in domain classes above, keyed by the
    same name ``plan_tenant_rls()`` is expected to resolve a domain class to
    (matching the provider's ``base.metadata`` resolution, Step 9).
    """
    return {
        "tenant_uuid": _table(
            base,
            "tenant_uuid",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("tenant_id", sa.Uuid(), nullable=False),
        ),
        "tenant_string": _table(
            base,
            "tenant_string",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("tenant_id", sa.String(255), nullable=False),
        ),
        "tenant_int": _table(
            base,
            "tenant_int",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        ),
        "global_table": _table(
            base,
            "global_table",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("tenant_id", sa.Uuid(), nullable=False),
        ),
        "no_tenant_column": _table(
            base,
            "no_tenant_column",
            sa.Column("id", sa.Integer, primary_key=True),
        ),
        "unmappable_column": _table(
            base,
            "unmappable_column",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("tenant_id", sa.JSON(), nullable=False),
        ),
        "nullable_tenant": _table(
            base,
            "nullable_tenant",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("tenant_id", sa.Uuid(), nullable=True),
        ),
        # varco_tenants: tenant_id is the PRIMARY KEY, never a policy
        # candidate (§D-S12-autogen).
        "varco_tenants": _table(
            base,
            "varco_tenants",
            sa.Column("tenant_id", sa.Uuid(), primary_key=True),
        ),
    }


def _domain_classes_map() -> dict[type, str]:
    """Maps the stand-in domain classes above to their table names."""
    return {
        _TenantUuidModel: "tenant_uuid",
        _TenantStringModel: "tenant_string",
        _TenantIntModel: "tenant_int",
        _GlobalModel: "global_table",
        _NoTenantColumnModel: "no_tenant_column",
        _UnmappableColumnModel: "unmappable_column",
    }


class TestPlanTenantRlsCastTypeDerivation:
    def test_uuid_column_derives_uuid_cast(self, tables: dict[str, sa.Table], base: type) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls

        plans = plan_tenant_rls(
            [_TenantUuidModel], base=base, table_lookup=lambda cls: tables["tenant_uuid"]
        )
        assert len(plans) == 1
        assert plans[0].table == "tenant_uuid"
        assert plans[0].cast_type == "uuid"
        assert plans[0].skipped_reason is None

    def test_string_column_derives_text_cast(self, tables: dict[str, sa.Table], base: type) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls

        plans = plan_tenant_rls(
            [_TenantStringModel], base=base, table_lookup=lambda cls: tables["tenant_string"]
        )
        assert plans[0].cast_type == "text"

    def test_integer_column_derives_bigint_cast(
        self, tables: dict[str, sa.Table], base: type
    ) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls

        plans = plan_tenant_rls(
            [_TenantIntModel], base=base, table_lookup=lambda cls: tables["tenant_int"]
        )
        assert plans[0].cast_type == "bigint"

    def test_unmappable_column_type_is_skipped_never_guessed(
        self, tables: dict[str, sa.Table], base: type
    ) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls

        plans = plan_tenant_rls(
            [_UnmappableColumnModel],
            base=base,
            table_lookup=lambda cls: tables["unmappable_column"],
        )
        assert len(plans) == 1
        assert plans[0].skipped_reason is not None
        assert plans[0].cast_type != "uuid"  # never a guessed default


class TestPlanTenantRlsScope:
    def test_global_scoped_model_is_absent_entirely(
        self, tables: dict[str, sa.Table], base: type
    ) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls

        plans = plan_tenant_rls(
            [_GlobalModel], base=base, table_lookup=lambda cls: tables["global_table"]
        )
        assert plans == []

    def test_tenant_model_with_no_tenant_column_is_present_with_skip_and_no_ddl(
        self, tables: dict[str, sa.Table], base: type
    ) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls, render_tenant_rls_ddl

        plans = plan_tenant_rls(
            [_NoTenantColumnModel],
            base=base,
            table_lookup=lambda cls: tables["no_tenant_column"],
        )
        assert len(plans) == 1
        assert plans[0].skipped_reason is not None

        ddl = render_tenant_rls_ddl(plans)
        assert ddl == []

    def test_varco_tenants_is_hard_excluded(self, tables: dict[str, sa.Table], base: type) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls

        class _TenantsCatalogModel:
            class Meta:
                tenant_scope = "tenant"

        plans = plan_tenant_rls(
            [_TenantsCatalogModel],
            base=base,
            table_lookup=lambda cls: tables["varco_tenants"],
        )
        assert plans == [] or all(p.skipped_reason for p in plans)
        names = {p.table for p in plans if p.skipped_reason is None}
        assert "varco_tenants" not in names


class TestPlanTenantRlsUnregisteredDomainClass:
    def test_unregistered_domain_class_is_skipped_not_a_key_error(self, base: type) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls

        class _NeverRegistered:
            class Meta:
                tenant_scope = "tenant"

        plans = plan_tenant_rls(
            [_NeverRegistered],
            base=base,
            table_lookup=lambda cls: (_ for _ in ()).throw(KeyError(cls)),
        )
        assert len(plans) == 1
        assert plans[0].skipped_reason is not None


class TestNullableTenantColumnPolicy:
    def test_nullable_tenant_column_raises_value_error_naming_table_column_and_remedies(
        self, tables: dict[str, sa.Table], base: type
    ) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls

        class _NullableModel:
            class Meta:
                tenant_scope = "tenant"

        with pytest.raises(ValueError) as exc:
            plan_tenant_rls(
                [_NullableModel],
                base=base,
                table_lookup=lambda cls: tables["nullable_tenant"],
            )
        message = str(exc.value)
        assert "nullable_tenant" in message
        assert "tenant_id" in message
        # Both remedies (VISIBLE and HIDDEN) named per §D-S12-nullable.
        assert "VISIBLE" in message
        assert "HIDDEN" in message

    def test_null_tenant_policy_visible_emits_or_is_null(
        self, tables: dict[str, sa.Table], base: type
    ) -> None:
        from varco_sa.rls_autogen import NullTenantPolicy, plan_tenant_rls, render_tenant_rls_ddl

        class _NullableModel:
            class Meta:
                tenant_scope = "tenant"

        plans = plan_tenant_rls(
            [_NullableModel],
            base=base,
            table_lookup=lambda cls: tables["nullable_tenant"],
            null_tenant=NullTenantPolicy.VISIBLE,
        )
        assert len(plans) == 1
        assert plans[0].skipped_reason is None
        assert plans[0].nullable is True

        ddl = render_tenant_rls_ddl(plans, null_tenant=NullTenantPolicy.VISIBLE)
        joined = "\n".join(ddl)
        assert "IS NULL" in joined

    def test_null_tenant_policy_hidden_does_not_emit_or_is_null(
        self, tables: dict[str, sa.Table], base: type
    ) -> None:
        from varco_sa.rls_autogen import NullTenantPolicy, plan_tenant_rls, render_tenant_rls_ddl

        class _NullableModel:
            class Meta:
                tenant_scope = "tenant"

        plans = plan_tenant_rls(
            [_NullableModel],
            base=base,
            table_lookup=lambda cls: tables["nullable_tenant"],
            null_tenant=NullTenantPolicy.HIDDEN,
        )
        ddl = render_tenant_rls_ddl(plans, null_tenant=NullTenantPolicy.HIDDEN)
        joined = "\n".join(ddl)
        assert "IS NULL" not in joined


class TestPlanTenantRlsDeterminism:
    def test_two_calls_produce_the_same_order(
        self, tables: dict[str, sa.Table], base: type
    ) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls

        classes = [_TenantUuidModel, _TenantStringModel, _TenantIntModel]

        def lookup(cls: type) -> sa.Table:
            return tables[_domain_classes_map()[cls]]

        first = plan_tenant_rls(classes, base=base, table_lookup=lookup)
        second = plan_tenant_rls(classes, base=base, table_lookup=lookup)

        assert [p.table for p in first] == [p.table for p in second]


class TestRenderTenantRlsDdlUsesRenderRlsDdlAsSingleSource:
    def test_generated_ddl_contains_the_initplan_form(
        self, tables: dict[str, sa.Table], base: type
    ) -> None:
        # "Every string comes from render_rls_ddl() — the InitPlan form is
        # never re-derived here" (Step 8).
        from varco_sa.rls_autogen import plan_tenant_rls, render_tenant_rls_ddl

        plans = plan_tenant_rls(
            [_TenantUuidModel], base=base, table_lookup=lambda cls: tables["tenant_uuid"]
        )
        ddl = render_tenant_rls_ddl(plans)
        joined = "\n".join(ddl)
        assert "(SELECT NULLIF(current_setting(" in joined


class TestRlsTablePlanIsFrozen:
    def test_rls_table_plan_is_frozen(self, tables: dict[str, sa.Table], base: type) -> None:
        from varco_sa.rls_autogen import plan_tenant_rls

        plans = plan_tenant_rls(
            [_TenantUuidModel], base=base, table_lookup=lambda cls: tables["tenant_uuid"]
        )
        with pytest.raises(Exception):  # noqa: B017 - FrozenInstanceError from dataclasses
            plans[0].table = "other"  # type: ignore[misc]
