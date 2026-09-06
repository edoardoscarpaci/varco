"""
Failing-first tests for varco_core.tenancy.provenance's AmbientVar wiring
(Plan 033, Phase 0, Step 5) — §D-S6-provenance.
"""

from __future__ import annotations

import asyncio


def _mod():
    from varco_core.tenancy import provenance

    return provenance


def _source_mod():
    from varco_core.tenancy import source

    return source


def _make_provenance(source_mod, tenant_id="acme"):
    return source_mod.TenantProvenance(
        tenant_id=tenant_id,
        winner=None,
        claims=(),
        conflict=None,
        mode=source_mod.CrossCheckMode.LENIENT,
    )


class TestCurrentTenantProvenanceOutsideContext:
    def test_none_outside_any_context(self) -> None:
        mod = _mod()
        assert mod.current_tenant_provenance() is None


class TestProvenanceContext:
    def test_sets_and_restores(self) -> None:
        mod = _mod()
        smod = _source_mod()
        prov = _make_provenance(smod)

        assert mod.current_tenant_provenance() is None
        with mod.provenance_context(prov):
            assert mod.current_tenant_provenance() is prov
        assert mod.current_tenant_provenance() is None

    def test_nesting_restores_outer_value(self) -> None:
        mod = _mod()
        smod = _source_mod()
        outer = _make_provenance(smod, "outer")
        inner = _make_provenance(smod, "inner")

        with mod.provenance_context(outer):
            with mod.provenance_context(inner):
                assert mod.current_tenant_provenance() is inner
            assert mod.current_tenant_provenance() is outer
        assert mod.current_tenant_provenance() is None


class TestTaskPropagation:
    async def test_value_set_in_parent_visible_in_child_task(self) -> None:
        mod = _mod()
        smod = _source_mod()
        prov = _make_provenance(smod, "parent")

        seen = {}

        async def child():
            seen["value"] = mod.current_tenant_provenance()

        with mod.provenance_context(prov):
            await asyncio.create_task(child())

        assert seen["value"] is prov

    async def test_value_not_visible_in_sibling_task(self) -> None:
        mod = _mod()
        smod = _source_mod()
        prov = _make_provenance(smod, "sibling-owner")

        sibling_seen = {}

        async def sibling():
            # Started before the context is entered; must never observe it.
            await asyncio.sleep(0.05)
            sibling_seen["value"] = mod.current_tenant_provenance()

        task = asyncio.create_task(sibling())
        await asyncio.sleep(0)  # let it start running (and copy context) first
        with mod.provenance_context(prov):
            await asyncio.sleep(0.1)
        await task

        assert sibling_seen["value"] is None
