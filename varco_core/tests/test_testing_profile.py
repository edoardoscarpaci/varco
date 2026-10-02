"""
Red-mode tests for Plan 052 Step 10 — ``varco_core.testing.InMemoryProfile`` /
``in_memory_modules()`` / ``register_modules()`` / ``clear_backend_selection()``
(§D-testprofile).

``varco_core.testing`` does not exist yet, so every test here is expected to
fail with an ``ImportError`` until Plan 052 Step 10 lands the module.
"""

from __future__ import annotations

from providify import DIContainer
from varco_core.di.lifecycle import Phase, phased_components

# Primary red-mode probe.
from varco_core.testing import (  # noqa: F401
    InMemoryProfile,
    clear_backend_selection,
    in_memory_modules,
    register_modules,
)


def test_a_default_in_memory_container_validates_clean() -> None:
    c = DIContainer()
    modules = in_memory_modules()

    register_modules(c, modules)

    validation = c.validate(raise_on_error=False)
    assert validation.errors == ()
    assert validation.warnings == ()

    profile = next(m for m in modules if isinstance(m, InMemoryProfile))
    assert profile.validate(c) == ()


def test_a_every_seam_reports_default_backend() -> None:
    from varco_core.di import inspect_backend_selection

    c = DIContainer()
    register_modules(c, in_memory_modules())

    for selection in inspect_backend_selection(c):
        assert selection.source in ("default", "env")


def test_b_no_scheduler_phase_components_are_planned() -> None:
    c = DIContainer()
    modules = in_memory_modules()
    register_modules(c, modules)

    components = phased_components(modules, c)

    assert not any(comp.phase == Phase.SCHEDULERS for comp in components)


def test_d_clear_backend_selection_removes_known_env_vars(monkeypatch) -> None:
    monkeypatch.setenv("VARCO_EVENT_BUS_BACKEND", "kafka")
    monkeypatch.setenv("VARCO_FEATURE_FLAGS_ENABLED", "true")

    clear_backend_selection(monkeypatch)

    import os

    assert "VARCO_EVENT_BUS_BACKEND" not in os.environ
    assert "VARCO_FEATURE_FLAGS_ENABLED" not in os.environ


def test_f_in_memory_profile_carries_no_providify_scan_marker() -> None:
    # InMemoryProfile must not be discoverable by container.scan() — it is
    # an app-constructed VarcoModule like any other, never a scanned class.
    assert not hasattr(InMemoryProfile, "__providify_singleton__")
    assert not hasattr(InMemoryProfile, "__providify_provider__")
