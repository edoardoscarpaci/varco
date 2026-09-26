"""
Plan 047 / Step 12 — `create_varco_app()` calls `check_backend_selection(container)`
when a container is passed, before the lifespan is built.

RED MODE: `create_varco_app` does not call `check_backend_selection` yet, and
`varco_core.di.check_backend_selection`/`BackendSelectionError` do not exist
yet — expected failure is `ImportError`/`AttributeError`, or a construction
that does NOT raise where it should (an assertion failure).
"""

from __future__ import annotations

import pytest
from providify import DIContainer


def test_unknown_backend_value_raises_at_app_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from varco_core.di import bootstrap
    from varco_core.di.errors import BackendSelectionError
    from varco_fastapi.app import create_varco_app

    monkeypatch.setenv("VARCO_CACHE_BACKEND", "nope")
    container = DIContainer()
    bootstrap(container)

    with pytest.raises(BackendSelectionError):
        create_varco_app(container, validate=False)


def test_no_container_skips_the_check(monkeypatch: pytest.MonkeyPatch) -> None:
    # check_backend_selection(container) is only called "when a container is
    # passed" — a bare create_varco_app() call must not require it.
    from varco_fastapi.app import create_varco_app

    monkeypatch.setenv("VARCO_CACHE_BACKEND", "nope")

    app = create_varco_app(validate=False)
    assert app is not None


def test_valid_backend_value_does_not_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_core.di import bootstrap
    from varco_fastapi.app import create_varco_app

    monkeypatch.setenv("VARCO_CACHE_BACKEND", "memory")
    container = DIContainer()
    bootstrap(container)

    app = create_varco_app(container, validate=False)
    assert app is not None
