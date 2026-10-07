"""
Plan 058 / Step 2 (vi) — red-first: the ``varco_fastapi.auth.TrustStore`` shim
moves from an ad-hoc ``warnings.warn`` to ``warn_deprecated(since="3.1.0",
removed_in="5.0.0")`` so the floor test and 052's ledger both see it.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from varco_core.deprecation import fired_deprecations, reset_fired_deprecations


def test_trust_store_shim_message_names_since_and_5_0_0(tmp_path: Path) -> None:
    from varco_fastapi.auth.trust_store import TrustStore

    with pytest.warns(
        DeprecationWarning, match=r"deprecated since 3\.1\.0 and will be removed in 5\.0\.0"
    ):
        TrustStore(ca_cert=tmp_path / "ca.pem")


def test_trust_store_shim_lands_in_fired_deprecations_ledger(tmp_path: Path) -> None:
    # 052's ledger only records warn_deprecated() sites with metadata populated.
    from varco_fastapi.auth.trust_store import TrustStore

    reset_fired_deprecations()
    with pytest.warns(DeprecationWarning):
        TrustStore(ca_cert=tmp_path / "ca.pem")

    records = [r for r in fired_deprecations() if r.subject == "varco_fastapi.auth.TrustStore"]
    assert len(records) == 1
    assert (records[0].since, records[0].removed_in) == ("3.1.0", "5.0.0")


def test_trust_store_shim_warning_blames_the_constructing_caller(tmp_path: Path) -> None:
    # The helper adds a frame; stacklevel must still point at this test, not at
    # the dataclass-generated __init__ or __post_init__.
    from varco_fastapi.auth.trust_store import TrustStore

    with pytest.warns(DeprecationWarning) as record:
        TrustStore(ca_cert=tmp_path / "ca.pem")

    assert record[0].filename == __file__
