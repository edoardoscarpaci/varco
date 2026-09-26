"""
Plan 047 / Step 10 — `varco_core.di.introspect` (§D-introspect).

Covers `BindingRecord`/`inspect_bindings()`, `Catalogue`/`catalogue()`
bucket classification, `CatalogueError` on an unclassified fallback, and
the JSON schema round-trip (`to_dict()`/`from_dict()`).

RED MODE: `varco_core/varco_core/di/introspect.py` does not exist yet —
every test here is expected to fail with `ModuleNotFoundError`/
`ImportError`/`AttributeError`.
"""

from __future__ import annotations

import pytest
from providify import DIContainer


@pytest.fixture
def bootstrapped_container() -> DIContainer:
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    return container


def test_inspect_bindings_returns_a_record_per_registered_binding(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.di.introspect import BindingRecord, inspect_bindings

    records = inspect_bindings(bootstrapped_container)
    assert records
    assert all(isinstance(r, BindingRecord) for r in records)


def test_inspect_bindings_record_has_the_documented_fields(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.di.introspect import inspect_bindings

    record = inspect_bindings(bootstrapped_container)[0]
    for field in (
        "interface",
        "implementation",
        "owner",
        "qualifier",
        "priority",
        "fallback",
        "exact_only",
        "conditions",
        "active",
    ):
        assert hasattr(record, field), field


def test_inspect_bindings_is_the_testkit_shim_delegate(
    bootstrapped_container: DIContainer,
) -> None:
    # 046's testkit shim `fallback_bindings()` becomes a one-line alias
    # over `inspect_bindings()` — the private-read count stays at one.
    from varco_conformance.di_wiring import fallback_bindings
    from varco_core.di.introspect import inspect_bindings

    shim_result = fallback_bindings(bootstrapped_container)
    introspect_result = [r for r in inspect_bindings(bootstrapped_container) if r.fallback]
    assert len(shim_result) == len(introspect_result)


def test_catalogue_does_not_raise_on_the_varco_core_bootstrap(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.di.introspect import catalogue

    result = catalogue(bootstrapped_container)
    assert result is not None


def test_catalogue_classifies_every_expected_default(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.cache.base import CacheBackend
    from varco_core.di.introspect import catalogue

    result = catalogue(bootstrapped_container)
    seam_row = next(s for s in result.seams if s.seam == "cache")
    assert seam_row.default is not None
    assert seam_row.default.category in {"null", "in-memory", "production-safe"}
    assert CacheBackend  # sanity that the import above is used


def test_catalogue_raises_catalogue_error_on_unclassifiable_fallback() -> None:
    from providify import Fallback, Provider
    from varco_core.di.errors import CatalogueError
    from varco_core.di.introspect import catalogue

    class _WeirdInterface:
        pass

    @Fallback
    @Provider(singleton=True)
    def _weird_provider() -> _WeirdInterface:
        return _WeirdInterface()

    container = DIContainer()
    container.provide(_weird_provider, returns=_WeirdInterface)

    with pytest.raises(CatalogueError):
        catalogue(container)


def test_catalogue_to_dict_round_trips_through_from_dict(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.di.introspect import Catalogue, catalogue

    result = catalogue(bootstrapped_container)
    payload = result.to_dict()
    assert payload["schema_version"] == 1

    restored = Catalogue.from_dict(payload)
    assert restored.to_dict() == payload


def test_inspect_backend_selection_sources_named_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_core.di.select import inspect_backend_selection

    monkeypatch.setenv("VARCO_CACHE_BACKEND", "redis")
    container = DIContainer()
    container.scan("varco_core", recursive=True)

    selections = inspect_backend_selection(container)
    cache_selection = next(s for s in selections if s.seam == "cache")
    assert cache_selection.source == "env"


def test_inspect_backend_selection_sources_env_dotted(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module_dir = tmp_path / "dotted_pkg2"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text("")
    (module_dir / "mod.py").write_text(
        "from varco_core.event.memory import InMemoryEventBus\n"
        "class MyBus(InMemoryEventBus):\n    pass\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("VARCO_EVENT_BUS_BACKEND", "dotted_pkg2.mod:MyBus")

    from varco_core.di import bootstrap
    from varco_core.di.select import inspect_backend_selection

    container = DIContainer()
    bootstrap(container)

    selections = inspect_backend_selection(container)
    bus_selection = next(s for s in selections if s.seam == "event_bus")
    assert bus_selection.source == "env-dotted"


def test_inspect_backend_selection_sources_code(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_core.di import bind_instance
    from varco_core.di.select import inspect_backend_selection
    from varco_core.event.base import AbstractEventBus
    from varco_core.event.memory import InMemoryEventBus

    monkeypatch.delenv("VARCO_EVENT_BUS_BACKEND", raising=False)
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bind_instance(container, AbstractEventBus, InMemoryEventBus())

    selections = inspect_backend_selection(container)
    bus_selection = next(s for s in selections if s.seam == "event_bus")
    assert bus_selection.source == "code"


def test_inspect_backend_selection_sources_env_when_backend_package_not_bootstrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Repair round, Decision 3 — pins the owner-ratified contract: ``source``
    reflects the raw env-string's own shape, not ``get_binding().priority``.

    ``VARCO_CACHE_BACKEND=redis`` is set, but only ``varco_core`` is
    scanned/bootstrapped — ``varco_redis.di.bootstrap()`` never runs, so no
    ``@Fallback`` binding for the "redis" cache backend is ever registered.
    ``source`` must still report ``"env"`` (the operator's stated intent),
    even though the unqualified ``CacheBackend`` binding that actually
    resolves is the in-process default at priority 0 — the exact
    inconsistent state ``check_backend_selection()`` raises
    ``BackendSelectionError`` for. A future change that reads
    ``get_binding().priority`` instead would silently report
    ``source="default"`` here and must fail this test.
    """
    from varco_core.di.select import inspect_backend_selection

    monkeypatch.setenv("VARCO_CACHE_BACKEND", "redis")
    container = DIContainer()
    container.scan("varco_core", recursive=True)  # varco_redis never bootstrapped

    selections = inspect_backend_selection(container)
    cache_selection = next(s for s in selections if s.seam == "cache")

    assert cache_selection.source == "env"
    assert cache_selection.value == "redis"
    # live_owner still names whatever actually resolves (the in-process
    # default) — source and live_owner are allowed to disagree here.
    assert cache_selection.live_owner is not None
