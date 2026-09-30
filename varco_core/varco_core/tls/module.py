"""
varco_core.tls.module
========================
``TrustStoreModule`` — the lifecycle owner of a ``ReloadingTrustStore``
(Plan 050 / §D-trust, DI-10 row 17).

``bind_trust_store`` (``varco_core.tls.di``) deliberately never starts or
stops the store it binds (§D-T3-oq3's "no lifecycle side effect" rule) — a
FastAPI app previously had to `lifespan.register(store)` by hand. This
module is the ``VarcoModule``-shaped equivalent: it binds the store (via
`bind_trust_store`, unchanged) **and** owns its `start()`/`stop()` at the
``INFRA`` phase, so a trust store used by an HTTP client wrapper is loaded
before anything in a later phase (the bus, consumers) could try to use it.

Never a ``@Configuration``/scanned provider — same reason `tls/di.py`'s
module docstring gives: `container.scan("varco_core", recursive=True)`
must never auto-start a background filesystem watcher.

Thread safety:  ⚠️ Single-threaded startup only (same contract as every
                    ``VarcoModule``).
Async safety:   ✅ ``start``/``stop`` are ``async def``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from providify import Inject, Provider

from varco_core.di import bind_instance, is_user_bound
from varco_core.di.lifecycle import Phase
from varco_core.di.module import CoreModule, VarcoModule
from varco_core.tls.di import bind_trust_store
from varco_core.tls.reload import ReloadingTrustStore
from varco_core.tls.store import TrustStore
from varco_core.watch.base import AbstractPathWatcher

if TYPE_CHECKING:
    from providify import DIContainer

__all__ = ["TrustStoreModule"]


class TrustStoreModule(VarcoModule):
    """
    Binds and, at the ``INFRA`` phase, starts/stops a ``ReloadingTrustStore``
    (§D-trust).

    Args:
        store: An already-constructed, unstarted ``ReloadingTrustStore`` to
            bind and own (`bind_trust_store`'s exact shape). Mutually
            exclusive with *spec*.
        spec: A ``TrustStore`` to build a ``ReloadingTrustStore`` from via a
            provider (``spec: Inject[TrustStore]``) — lets the store be
            constructed lazily, from whatever ``TrustStore`` ends up bound
            (this module's own, or an app's / ``varco_fastapi.di``'s env
            fallback), rather than requiring the caller to build one by
            hand. Mutually exclusive with *store*.
        watcher: Forwarded to ``ReloadingTrustStore(spec, watcher=watcher)``
            when building from *spec*. Ignored when *store* is given.
        enabled: The usual Plan 047 tri-state.

    Raises:
        ValueError: Both or neither of *store*/*spec* were given.

    Example::

        app = create_varco_app(container, modules=[
            CoreModule(),
            TrustStoreModule(spec=TrustStore(ca_folders=Path("/etc/ssl/private-ca"))),
        ])
    """

    name = "trust_store"
    phase = Phase.INFRA
    depends_on = (CoreModule,)

    def __init__(
        self,
        *,
        store: ReloadingTrustStore | None = None,
        spec: TrustStore | None = None,
        watcher: AbstractPathWatcher | None = None,
        enabled: bool | None = None,
    ) -> None:
        if (store is None) == (spec is None):
            raise ValueError(
                "TrustStoreModule requires exactly one of store= or spec= — "
                f"got store={store!r}, spec={spec!r}."
            )
        super().__init__(enabled=enabled)
        self._given_store = store
        self._given_spec = spec
        self._watcher = watcher

    def _register(self, container: DIContainer) -> None:
        if self._given_store is not None:
            bind_trust_store(container, self._given_store)
            return

        if not is_user_bound(container, TrustStore):
            bind_instance(container, TrustStore, self._given_spec)

        watcher = self._watcher

        def _reloading_trust_store(spec: Inject[TrustStore]) -> ReloadingTrustStore:
            return ReloadingTrustStore(spec, watcher=watcher)

        container.provide(
            Provider(singleton=True)(_reloading_trust_store), returns=ReloadingTrustStore
        )

    async def start(self, container: DIContainer) -> None:
        """Start the resolved ``ReloadingTrustStore``."""
        store: ReloadingTrustStore = await container.aget(ReloadingTrustStore)
        await store.start()

    async def stop(self, container: DIContainer) -> None:
        """The exact reverse of ``start()``."""
        store: ReloadingTrustStore = await container.aget(ReloadingTrustStore)
        await store.stop()
