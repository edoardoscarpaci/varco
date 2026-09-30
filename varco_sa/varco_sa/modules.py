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

from typing import TYPE_CHECKING, Any

from varco_core.di.lifecycle import Phase
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
    phase = Phase.INFRA
    depends_on = (CoreModule,)

    def __init__(
        self,
        config: SAConfig | None = None,
        *,
        entities: Sequence[type[DomainModel]] = (),
        enabled: bool | None = None,
        ensure_tables: bool = True,
    ) -> None:
        """
        Args:
            config: See class docstring.
            entities: See class docstring.
            enabled: The usual Plan 047 tri-state.
            ensure_tables: Whether ``start()`` (Plan 050 / §D-stores) calls
                ``start()``/``ensure_table()`` (``checkfirst`` DDL) on every
                resolvable SA-backed framework store. Set ``False`` when an
                Alembic migration already owns every framework table this
                deployment uses (``technical_docs/features/
                schema-migrations.md``'s ten-table story) — a duplicate
                ``checkfirst`` create is harmless but redundant, and this
                flag documents the intent explicitly rather than relying on
                that harmlessness.
        """
        super().__init__(enabled=enabled)
        self._config = config
        self._entities = tuple(entities)
        self._ensure_tables = ensure_tables

    def _register(self, container: DIContainer) -> None:
        from varco_core.di import bind_instance  # noqa: PLC0415

        from varco_sa.config import SAConfig  # noqa: PLC0415
        from varco_sa.di import bind_repositories, bootstrap  # noqa: PLC0415

        bootstrap(container)
        if self._config is not None:
            bind_instance(container, SAConfig, self._config)
        if self._entities:
            bind_repositories(container, *self._entities)

    async def start(self, container: DIContainer) -> None:
        """
        Own SA framework-table readiness (§D-stores): for each of
        ``AbstractIdempotencyStore``/``AbstractDeadLetterQueue``/
        ``AbstractJobStore``/``WebhookSubscriptionRepository`` that is
        resolvable **and** resolves to a ``varco_sa`` class, await
        ``start()`` if present, else ``ensure_table()`` if present and
        ``self._ensure_tables``.

        ``CoreModule``'s ``core.stores`` component starts the same seams
        *generically* (``start()`` only, never ``ensure_table()``) — the
        two never double-run because ``start()`` is idempotent and
        ``ensure_table()`` is ``checkfirst``.
        """
        from varco_core.event.dlq import AbstractDeadLetterQueue  # noqa: PLC0415
        from varco_core.idempotency.base import AbstractIdempotencyStore  # noqa: PLC0415
        from varco_core.job.base import AbstractJobStore  # noqa: PLC0415
        from varco_core.webhook.base import WebhookSubscriptionRepository  # noqa: PLC0415

        for seam in (
            AbstractIdempotencyStore,
            AbstractDeadLetterQueue,
            AbstractJobStore,
            WebhookSubscriptionRepository,
        ):
            if not container.is_resolvable(seam):
                continue
            obj: Any = await container.aget(seam)
            if type(obj).__module__.split(".")[0] != "varco_sa":
                continue
            start = getattr(obj, "start", None)
            if callable(start):
                await start()
            elif self._ensure_tables:
                ensure_table = getattr(obj, "ensure_table", None)
                if callable(ensure_table):
                    await ensure_table()


__all__ = ["SAModule"]
