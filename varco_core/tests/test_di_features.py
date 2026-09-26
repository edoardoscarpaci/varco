"""
Plan 047 / Step 3 — `varco_core.di.features` (§D-features).

Covers `Feature` / `FEATURES` and `feature_default()`: twin active with
env, inactive without; `enable_*` after env → code wins + one
`FALLBACK_SHADOWED`; `enable_*` twice → `AlreadyBoundError` (046,
unchanged).

RED MODE: `varco_core/varco_core/di/features.py` does not exist yet —
every test here is expected to fail with `ModuleNotFoundError`/
`ImportError`/`AttributeError`.
"""

from __future__ import annotations

import pytest
from providify import DIContainer, IssueKind


def test_features_registry_has_the_three_binding_shaped_features() -> None:
    from varco_core.di.features import FEATURES

    names = {f.feature for f in FEATURES}
    assert {"feature_flags", "tenant_membership", "policy_authorizer"} <= names


def test_authorization_audit_is_the_documented_no_env_exception() -> None:
    from varco_core.di.features import FEATURES

    audit = next(f for f in FEATURES if f.feature == "authorization_audit")
    assert audit.shape == "wrapper"
    assert audit.env is None


def test_feature_default_decorator_gates_on_env_truthy() -> None:
    from providify import Fallback, Provider
    from varco_core.di.features import feature_default

    @Fallback
    @Provider(singleton=True, priority=100)
    @feature_default("feature_flags")
    def _fake_twin() -> object:
        return object()

    assert _fake_twin.__requires__[0] is not None


def test_feature_flags_twin_inactive_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_core.flags import AbstractFeatureFlags
    from varco_core.flags.null import NullFeatureFlags

    monkeypatch.delenv("VARCO_FEATURE_FLAGS_ENABLED", raising=False)
    container = DIContainer()
    container.scan("varco_core", recursive=True)

    resolved = container.get(AbstractFeatureFlags)
    assert isinstance(resolved, NullFeatureFlags)


def test_feature_flags_twin_active_with_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_core.di import bootstrap
    from varco_core.flags import AbstractFeatureFlags
    from varco_core.flags.memory import InMemoryFeatureFlags

    # Repair round, Decision 4: the twin is now hand-registered by
    # bootstrap() (mirroring the revocation twin's shape), not
    # scan-discovered — a bare scan() no longer activates it.
    monkeypatch.setenv("VARCO_FEATURE_FLAGS_ENABLED", "true")
    container = DIContainer()
    bootstrap(container)

    resolved = container.get(AbstractFeatureFlags)
    assert isinstance(resolved, InMemoryFeatureFlags)


def test_enable_feature_flags_after_env_code_wins_with_one_fallback_shadowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from varco_core.flags import AbstractFeatureFlags
    from varco_core.flags.di import enable_feature_flags
    from varco_core.flags.memory import InMemoryFeatureFlags

    monkeypatch.setenv("VARCO_FEATURE_FLAGS_ENABLED", "true")
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    enable_feature_flags(container)

    resolved = container.get(AbstractFeatureFlags)
    assert isinstance(resolved, InMemoryFeatureFlags)
    assert container.get_binding(AbstractFeatureFlags).fallback is False

    report = container.validate()
    assert any(issue.kind == IssueKind.FALLBACK_SHADOWED for issue in report.infos)


def test_enable_feature_flags_twice_raises_already_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_core.di import AlreadyBoundError
    from varco_core.flags.di import enable_feature_flags

    monkeypatch.delenv("VARCO_FEATURE_FLAGS_ENABLED", raising=False)
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    enable_feature_flags(container)

    with pytest.raises(AlreadyBoundError):
        enable_feature_flags(container)
