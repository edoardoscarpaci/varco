"""
Plan 050 / Step 12 (§D-trust) — `TrustStoreModule`, the lifecycle owner of a
`ReloadingTrustStore`. Docker-free.

RED MODE: `varco_core.tls.module` does not exist yet.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_core.di.module import CoreModule

# Expected to fail today: no such module.
from varco_core.tls.module import TrustStoreModule  # type: ignore[import-not-found]
from varco_core.tls.reload import ReloadingTrustStore
from varco_core.tls.store import TrustStore


async def test_spec_constructor_builds_a_reloading_trust_store_not_started_until_start(
    tmp_path: object,
) -> None:
    spec = TrustStore(ca_folders=tmp_path)  # type: ignore[arg-type]
    container = DIContainer()
    CoreModule().register(container)
    module = TrustStoreModule(spec=spec)
    module.register(container)

    store = container.get(ReloadingTrustStore)
    assert store.spec is spec

    with pytest.raises(Exception):  # ResourceNotLoadedError, name TBD
        _ = store.context  # type: ignore[attr-defined]

    await module.start(container)
    try:
        assert store.context is not None  # type: ignore[attr-defined]
    finally:
        await module.stop(container)


async def test_store_constructor_binds_the_given_instance(tmp_path: object) -> None:
    spec = TrustStore(ca_folders=tmp_path)  # type: ignore[arg-type]
    store = ReloadingTrustStore(spec)
    container = DIContainer()
    CoreModule().register(container)
    module = TrustStoreModule(store=store)
    module.register(container)

    assert container.get(ReloadingTrustStore) is store
