"""
Plan 058 / Step 2 (vii) — red-first: the argument-conditional
``TenantProvisionConsumer(provisioner=, catalog=)`` shim moves to
``warn_deprecated(since="3.0.0", removed_in="5.0.0")``.
"""

from __future__ import annotations

import pytest
from varco_core.deprecation import fired_deprecations, reset_fired_deprecations

_SUBJECT = "TenantProvisionConsumer(provisioner=..., catalog=...)"


class _Provisioner:
    async def provision(self, tenant_id: str, **kwargs: object) -> None:
        return None

    async def deprovision(self, tenant_id: str, *, confirm_destroy: bool = False) -> None:
        return None


def _construct() -> None:
    from varco_core.tenancy.catalog import StaticTenantCatalog
    from varco_core.tenancy.control.consumer import TenantProvisionConsumer

    TenantProvisionConsumer(provisioner=_Provisioner(), catalog=StaticTenantCatalog())


def test_shim_message_names_since_3_0_0_and_removal_5_0_0() -> None:
    with pytest.warns(
        DeprecationWarning, match=r"deprecated since 3\.0\.0 and will be removed in 5\.0\.0"
    ):
        _construct()


def test_shim_lands_in_fired_deprecations_ledger() -> None:
    reset_fired_deprecations()
    with pytest.warns(DeprecationWarning):
        _construct()

    records = [r for r in fired_deprecations() if r.subject == _SUBJECT]
    assert len(records) == 1
    assert (records[0].since, records[0].removed_in) == ("3.0.0", "5.0.0")


def test_shim_warning_blames_the_constructing_caller() -> None:
    # The helper adds a frame; stacklevel must still point at the caller of
    # TenantProvisionConsumer(...) (here: _construct in this file).
    with pytest.warns(DeprecationWarning) as record:
        _construct()

    assert record[0].filename == __file__
