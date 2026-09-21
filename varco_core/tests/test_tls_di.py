"""
Plan 026 / Step 14 — failing-first tests for ``varco_core.tls.di.bind_trust_store`` (§D-T3-oq3).

``varco_core.tls.di`` does not exist yet — every test fails with ``ModuleNotFoundError`` until
Step 13 lands ``varco_core/varco_core/tls/di.py``.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_conformance.providify_health import assert_no_structural_di_issues


def test_bind_trust_store_resolves() -> None:
    from varco_core.tls.di import bind_trust_store
    from varco_core.tls.reload import ReloadingTrustStore
    from varco_core.tls.store import TrustStore

    container = DIContainer()
    spec = TrustStore()
    store = ReloadingTrustStore(spec)

    bind_trust_store(container, store)

    resolved = container.get(ReloadingTrustStore)
    assert resolved is store
    resolved_spec = container.get(TrustStore)
    assert resolved_spec is spec


def test_bind_trust_store_no_lifecycle_side_effect() -> None:
    # §D-T3-oq3's ❌: "no lifecycle side effect — the container never starts anything the
    # caller did not start". A store bound but never start()-ed must still be unstarted.
    from varco_core.tls.di import bind_trust_store
    from varco_core.tls.reload import ReloadingTrustStore
    from varco_core.tls.store import TrustStore

    container = DIContainer()
    store = ReloadingTrustStore(TrustStore())
    bind_trust_store(container, store)

    with pytest.raises(Exception):
        _ = store.context  # never started -> ResourceNotLoadedError, not a live context


def test_bind_trust_store_returns_the_container() -> None:
    from varco_core.tls.di import bind_trust_store
    from varco_core.tls.reload import ReloadingTrustStore
    from varco_core.tls.store import TrustStore

    container = DIContainer()
    store = ReloadingTrustStore(TrustStore())

    result = bind_trust_store(container, store)

    assert result is container


def test_bind_trust_store_twice_raises() -> None:
    from varco_core.di import AlreadyBoundError
    from varco_core.tls.di import bind_trust_store
    from varco_core.tls.reload import ReloadingTrustStore
    from varco_core.tls.store import TrustStore

    container = DIContainer()
    store1 = ReloadingTrustStore(TrustStore())
    store2 = ReloadingTrustStore(TrustStore())
    bind_trust_store(container, store1)

    with pytest.raises(AlreadyBoundError):
        bind_trust_store(container, store2)


def test_scan_varco_core_recursive_starts_no_watcher_and_registers_no_tls_binding() -> None:
    # The anti-implicit assertion (§D-T3-oq3): a documented, in-use scan pattern
    # (container.scan("varco_core", recursive=True)) must never auto-activate a
    # TlsConfiguration or otherwise register a TLS binding / start a watcher.
    import varco_core.tls  # noqa: F401 — ensure the package is importable before the scan
    from varco_core.tls.reload import ReloadingTrustStore
    from varco_core.tls.store import TrustStore

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    container.validate_bindings()
    assert_no_structural_di_issues(container)

    # No TrustStore / ReloadingTrustStore binding registered implicitly by the scan.
    with pytest.raises(Exception):
        container.get(ReloadingTrustStore)
    with pytest.raises(Exception):
        container.get(TrustStore)
