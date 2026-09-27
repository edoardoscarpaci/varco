"""
Plan 049 / Step 16 — §D-scan (3): `container.scan("varco_fastapi",
recursive=True)` installs the renamed `VarcoFastAPIConfiguration`
providify module and **no** `VarcoModule`, Docker-free.

RED MODE: `VarcoFastAPIConfiguration` (the §D-names rename) and
`varco_fastapi.modules` do not exist yet — every test below is expected to
fail with `ImportError`.
"""

from __future__ import annotations

import importlib

from providify import DIContainer
from providify import metadata as providify_metadata
from varco_core.di.module import ModuleRegistry, VarcoModule


class TestScanActivatesOnlyTheRenamedConfiguration:
    async def test_scan_installs_varco_fastapi_configuration_and_no_module(self) -> None:
        # BUGFIX (test, not plan): `container.is_resolvable(<a @Configuration
        # class>)` is `False` even immediately after a direct
        # `container.install(VarcoFastAPIConfiguration)` — confirmed with a
        # bare providify repro with no varco_fastapi code involved.
        # providify's `install()` registers only the class's `@Provider`-
        # marked *methods* as bindings (scanner.py's own
        # `_autoregister_configurator` docstring: "Install a @Configuration
        # class into the container" refers to running its providers, not
        # binding the grouping class itself as a gettable type). The
        # scan-activated-something proof instead checks one of the
        # Configuration's own `@Fallback` bindings.
        from varco_core.job.task import TaskSerializer

        container = DIContainer()
        container.scan("varco_fastapi", recursive=True)

        assert container.is_resolvable(ModuleRegistry) is False
        assert container.is_resolvable(TaskSerializer)

        for binding in container.describe().bindings:
            impl = getattr(binding, "implementation", None) or getattr(binding, "cls", None)
            if isinstance(impl, type):
                assert not issubclass(impl, VarcoModule), (
                    f"{impl!r} was activated by scan — modules must never be scanned"
                )


class TestNoFastapiModuleSubclassCarriesAProvidifyMarker:
    async def test_every_varco_fastapi_module_subclass_is_marker_free(self) -> None:
        for mod_name in ("varco_fastapi.modules",):
            importlib.import_module(mod_name)

        def _all_subclasses(cls: type) -> set[type]:
            result = set()
            for sub in cls.__subclasses__():
                result.add(sub)
                result |= _all_subclasses(sub)
            return result

        for cls in _all_subclasses(VarcoModule):
            if not cls.__module__.startswith("varco_fastapi"):
                continue
            assert not providify_metadata._has_configuration_module(cls), (
                f"{cls!r} must not carry a providify @Configuration marker"
            )
