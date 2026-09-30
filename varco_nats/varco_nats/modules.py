"""
varco_nats.modules
=====================
``NatsModule`` — ``varco_nats``'s one ``VarcoModule`` (Plan 049 / DI-1,
Step 8).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di.lifecycle import Phase
from varco_core.di.module import CoreModule, VarcoModule

if TYPE_CHECKING:
    from providify import DIContainer


class NatsModule(VarcoModule):
    """Registers ``varco_nats``'s ``@Fallback`` defaults (``bootstrap()``)."""

    name = "nats"
    phase = Phase.INFRA
    depends_on = (CoreModule,)

    def _register(self, container: DIContainer) -> None:
        from varco_nats.di import bootstrap  # noqa: PLC0415

        bootstrap(container)


__all__ = ["NatsModule"]
