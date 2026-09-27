"""
Plan 049 / Step 14 — FLIP-1: `SecurityPostureModule` default-on
(§D-posture), Docker-free.

RED MODE: `create_varco_app(modules=...)` / `varco_fastapi.modules` do not
exist yet — every test below is expected to fail with `TypeError`/
`ImportError`.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_core.di.module import ModuleRegistry
from varco_fastapi.app import create_varco_app
from varco_fastapi.modules import SecurityPostureModule
from varco_fastapi.modules.defaults import default_modules

# BUGFIX (test, not plan): every create_varco_app(...) call below now
# passes `validate=False` — none of these tests bind AbstractServerAuth,
# which `validate_container_bindings()` requires unchanged from before
# this plan; see test_create_varco_app_modules.py's identical bugfix note.


class TestFlip1DefaultOn:
    async def test_default_app_has_enabled_security_posture_module_with_a_report(self) -> None:
        container = DIContainer()
        app = create_varco_app(container, validate=False)

        async with app.router.lifespan_context(app):
            registry = container.get(ModuleRegistry)
            posture_module = next(
                m for m in registry.modules if isinstance(m, SecurityPostureModule)
            )
            assert registry.enabled(posture_module) is True
            assert posture_module.report is not None

    async def test_env_var_disables_but_module_still_registered(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VARCO_SECURITY_POSTURE_ENABLED", "false")
        container = DIContainer()
        create_varco_app(container, validate=False)

        registry = container.get(ModuleRegistry)
        posture_module = next(m for m in registry.modules if isinstance(m, SecurityPostureModule))
        assert registry.enabled(posture_module) is False
        with pytest.raises(RuntimeError, match="not started"):
            _ = posture_module.report

    async def test_exclude_removes_it_entirely(self) -> None:
        container = DIContainer()
        create_varco_app(
            container,
            modules=default_modules(exclude=(SecurityPostureModule,)),
            validate=False,
        )

        registry = container.get(ModuleRegistry)
        assert not any(isinstance(m, SecurityPostureModule) for m in registry.modules)

    async def test_enforce_refuse_with_high_finding_raises_on_startup(self) -> None:
        from varco_fastapi.posture import SecurityPostureSettings

        def _high_finding_collector() -> list[object]:
            # BUGFIX (test, not plan): PostureFinding has no `message=`
            # field — its fields are `check`/`severity`/`detail`/
            # `remediation`/`suppressed` (varco_fastapi/varco_fastapi/
            # posture.py's PostureFinding dataclass).
            from varco_fastapi.posture import PostureFinding, PostureSeverity

            return [
                PostureFinding(
                    check="forced_high",
                    severity=PostureSeverity.HIGH,
                    detail="forced for test",
                    remediation="n/a — test-only forced finding",
                )
            ]

        container = DIContainer()
        app = create_varco_app(
            container,
            modules=[
                *default_modules(exclude=(SecurityPostureModule,)),
                SecurityPostureModule(
                    settings=SecurityPostureSettings(enforce="refuse"),
                    collectors=[_high_finding_collector],
                ),
            ],
            validate=False,
        )

        with pytest.raises(RuntimeError):
            async with app.router.lifespan_context(app):
                pass
