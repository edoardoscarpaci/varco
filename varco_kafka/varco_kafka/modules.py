"""
varco_kafka.modules
======================
``KafkaModule`` — ``varco_kafka``'s one ``VarcoModule`` (Plan 049 / DI-1,
Step 8).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di.module import CoreModule, VarcoModule

if TYPE_CHECKING:
    from providify import DIContainer


class KafkaModule(VarcoModule):
    """Registers ``varco_kafka``'s ``@Fallback`` defaults (``bootstrap()``)."""

    name = "kafka"
    depends_on = (CoreModule,)

    def _register(self, container: DIContainer) -> None:
        from varco_kafka.di import bootstrap  # noqa: PLC0415

        bootstrap(container)


__all__ = ["KafkaModule"]
