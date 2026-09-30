"""
Plan 051 (DI-5) / §D-plugin — the posture leg of the plugin proof: a
``PostureCollector`` contributed by ``testkit/varco_plugin_example`` (which
never imports ``varco_fastapi``) still joins ``SecurityPostureModule``'s
report, with no ``varco_fastapi`` edit.
"""

from __future__ import annotations

from providify import DIContainer
from varco_core.di import bootstrap
from varco_fastapi.modules.lifecycle import SecurityPostureModule


async def test_plugin_posture_collector_joins_the_report() -> None:
    container = DIContainer()
    bootstrap(container)
    container.scan("varco_plugin_example")

    module = SecurityPostureModule()
    module.register(container)
    module.configure_app(None, container)

    await module.start(container)

    report = module.report
    assert any(f.check == "plugin.example" for f in report.findings)
