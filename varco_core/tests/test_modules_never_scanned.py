"""
Plan 049 / Step 16 — §D-scan (1)-(2): `varco_core`'s half of the
module-vs-scan rule made mechanical, Docker-free.

RED MODE: `varco_core.di.module`/`varco_core.di.bootstrap` do not carry the
Plan 049 shape yet — every test below is expected to fail with
`ImportError`/`AttributeError`.
"""

from __future__ import annotations

from providify import DIContainer
from providify import metadata as providify_metadata
from varco_core.di import bootstrap
from varco_core.di.module import ModuleRegistry, VarcoModule


class TestScanNeverActivatesAModule:
    async def test_bootstrap_binds_no_varco_module_and_registry_unresolvable(self) -> None:
        container = DIContainer()
        bootstrap(container)

        assert container.is_resolvable(ModuleRegistry) is False

        for binding in container.describe().bindings:
            impl = getattr(binding, "implementation", None) or getattr(binding, "cls", None)
            if isinstance(impl, type):
                assert not issubclass(impl, VarcoModule), (
                    f"{impl!r} was activated by scan/bootstrap — modules must be "
                    "constructed by the app, never scanned"
                )


class TestNoVarcoModuleSubclassCarriesAProvidifyMarker:
    async def test_every_varco_module_subclass_is_marker_free(self) -> None:
        # Import every package's module file so __subclasses__() sees them.
        import importlib

        for mod_name in ("varco_core.di.module",):
            importlib.import_module(mod_name)

        def _all_subclasses(cls: type) -> set[type]:
            result = set()
            for sub in cls.__subclasses__():
                result.add(sub)
                result |= _all_subclasses(sub)
            return result

        for cls in _all_subclasses(VarcoModule):
            assert not providify_metadata._has_configuration_module(cls), (
                f"{cls!r} must not carry a providify @Configuration marker"
            )
            for name, value in vars(cls).items():
                provider_meta_fn = getattr(providify_metadata, "_get_provider_metadata", None)
                if provider_meta_fn is not None:
                    assert provider_meta_fn(value) is None, (
                        f"{cls!r}.{name} must not be @Provider-marked"
                    )
