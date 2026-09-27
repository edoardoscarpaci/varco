"""
varco_sa.modules
===================
``SAModule`` — ``varco_sa``'s one ``VarcoModule`` (Plan 049 / DI-1, Step 8).

Not to be confused with ``varco_sa.di.SAConfiguration`` (renamed from
``SAModule`` by §D-names) — that is the providify ``@Configuration``
grouping ``@Provider`` methods, scan-installed by ``bootstrap()`` below;
this ``SAModule`` is the app-constructed, never-scanned ``VarcoModule``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di.module import CoreModule, VarcoModule

if TYPE_CHECKING:
    from collections.abc import Sequence

    from providify import DIContainer
    from varco_core.model import DomainModel

    from varco_sa.config import SAConfig


class SAModule(VarcoModule):
    """
    Registers ``varco_sa``'s ``@Fallback`` defaults (``bootstrap()``), an
    optional ``SAConfig`` binding, and ``AsyncRepository[D]`` bindings for
    every entity passed.

    Args:
        config: A constructed ``SAConfig``. When given, bound
            (non-fallback) via ``bind_instance`` so
            ``container.get(SAConfig)`` returns it afterward.
        entities: Domain model classes to ``bind_repositories()`` for.
    """

    name = "sa"
    depends_on = (CoreModule,)

    def __init__(
        self,
        config: SAConfig | None = None,
        *,
        entities: Sequence[type[DomainModel]] = (),
        enabled: bool | None = None,
    ) -> None:
        super().__init__(enabled=enabled)
        self._config = config
        self._entities = tuple(entities)

    def _register(self, container: DIContainer) -> None:
        from varco_core.di import bind_instance  # noqa: PLC0415

        from varco_sa.config import SAConfig  # noqa: PLC0415
        from varco_sa.di import bind_repositories, bootstrap  # noqa: PLC0415

        bootstrap(container)
        if self._config is not None:
            bind_instance(container, SAConfig, self._config)
        if self._entities:
            bind_repositories(container, *self._entities)


__all__ = ["SAModule"]
