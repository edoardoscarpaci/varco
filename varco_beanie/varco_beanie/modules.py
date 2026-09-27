"""
varco_beanie.modules
=======================
``BeanieModule`` — ``varco_beanie``'s one ``VarcoModule`` (Plan 049 / DI-1,
Step 8).

Not to be confused with ``varco_beanie.di.BeanieConfiguration`` (renamed
from ``BeanieModule`` by §D-names) — that is the providify
``@Configuration``, scan-installed by ``bootstrap()`` below; this
``BeanieModule`` is the app-constructed, never-scanned ``VarcoModule``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di.module import CoreModule, VarcoModule

if TYPE_CHECKING:
    from collections.abc import Sequence

    from providify import DIContainer
    from varco_core.model import DomainModel

    from varco_beanie.config import BeanieSettings


class BeanieModule(VarcoModule):
    """
    Registers ``varco_beanie``'s ``@Fallback`` defaults (``bootstrap()``),
    an optional ``BeanieSettings`` binding, and ``AsyncRepository[D]``
    bindings for every entity passed.

    Args:
        settings: A constructed ``BeanieSettings``. When given, bound
            (non-fallback) via ``bind_instance`` so
            ``container.get(BeanieSettings)`` returns it afterward.
        entities: Domain model classes to ``bind_repositories()`` for.
    """

    name = "beanie"
    depends_on = (CoreModule,)

    def __init__(
        self,
        settings: BeanieSettings | None = None,
        *,
        entities: Sequence[type[DomainModel]] = (),
        enabled: bool | None = None,
    ) -> None:
        super().__init__(enabled=enabled)
        self._settings = settings
        self._entities = tuple(entities)

    def _register(self, container: DIContainer) -> None:
        from varco_core.di import bind_instance  # noqa: PLC0415

        from varco_beanie.config import BeanieSettings  # noqa: PLC0415
        from varco_beanie.di import bind_repositories, bootstrap  # noqa: PLC0415

        bootstrap(container)
        if self._settings is not None:
            bind_instance(container, BeanieSettings, self._settings)
        if self._entities:
            bind_repositories(container, *self._entities)


__all__ = ["BeanieModule"]
