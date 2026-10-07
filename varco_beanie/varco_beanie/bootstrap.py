"""
varco_beanie.bootstrap
==========================
One-stop bootstrap for Beanie (pymongo / MongoDB) + varco applications.

``BeanieFastrestApp``
    Thin coordinator that wires a ``BeanieSettings`` into the provider,
    exposes the provider as ``uow_provider``, and provides an ``init()``
    coroutine that initialises Beanie with all registered Document classes.

**Changed in 3.0.0 (Plan 022 / AB-4).** This module used to define its own
``BeanieConfig`` frozen dataclass, field-for-field identical to
``varco_beanie.config.BeanieSettings`` — one concept under two names, which
KI-10 had to bridge with a manual remap. ``BeanieConfig`` is now a deprecated
alias of ``BeanieSettings`` (the identical object, so ``isinstance`` is
unaffected) exposed from the ``varco_beanie`` package root, and is removed in
4.0.0. Construct ``BeanieSettings`` directly.

Minimal usage::

    from pymongo import AsyncMongoClient
    from varco_beanie import BeanieSettings
    from varco_beanie.bootstrap import BeanieFastrestApp
    from myapp.domain import Post, User

    config = BeanieSettings(
        mongo_client=AsyncMongoClient("mongodb://localhost:27017"),
        db_name="myapp",
        entity_classes=(User, Post),
    )
    app = BeanieFastrestApp(config)
    await app.init()   # must be called once at startup

    # Inject app.uow_provider into services
    service = PostService(uow_provider=app.uow_provider, ...)

DESIGN: parallel structure to SAFastrestApp
    ✅ Consistent API across SA and Beanie backends — applications that swap
       backends only need to change the config and app types, not service code.
    ✅ ``uow_provider`` is the same interface (``IUoWProvider``) in both
       backends — services are backend-agnostic by design.
    ❌ Beanie requires ``await app.init()`` at startup (SA uses ``await
       app.create_all()`` instead) — callers must know which to call.
       Not avoidable: Beanie's ``init_beanie()`` is async; SA's
       ``create_all()`` maps naturally to the SA engine lifecycle.

Thread safety:  ⚠️ Construct once at startup; ``uow_provider`` is safe to
                share across concurrent request handlers after ``init()``.
Async safety:   ✅ ``init()`` is ``async def`` — safe to await.
"""

from __future__ import annotations

from varco_core.deprecation import deprecated_alias

from varco_beanie.config import BeanieSettings
from varco_beanie.provider import BeanieRepositoryProvider

# ── BeanieFastrestApp ─────────────────────────────────────────────────────────


class BeanieFastrestApp:
    """
    Bootstrap coordinator for Beanie (Motor / MongoDB) + varco applications.

    Wires ``BeanieSettings`` into ``BeanieRepositoryProvider``, registers all
    entity classes, and exposes the provider as ``uow_provider`` for injection
    into services.

    **Important**: call ``await app.init()`` once at application startup,
    after all entity classes are registered and before any UoW is created.
    Beanie's ``init_beanie()`` function must run before any Document query.

    Attributes:
        uow_provider: Ready-to-use ``BeanieRepositoryProvider``.
                      Inject this into ``AsyncService.__init__`` via
                      ``Inject[IUoWProvider]`` or pass directly.

    Thread safety:  ⚠️ Construct once at startup; ``uow_provider`` is safe
                    to share across concurrent request handlers after ``init()``.
    Async safety:   ✅ ``init()`` is ``async def`` — safe to await.

    Edge cases:
        - Calling ``make_uow()`` before ``await init()`` is called will raise
          a Beanie error on the first Document query — not at ``make_uow()``
          time.  Always call ``init()`` first.
        - ``init()`` is idempotent — Beanie handles repeated calls gracefully.

    Example::

        app = BeanieFastrestApp(config)
        await app.init()              # mandatory startup step

        # Non-raising schema validation: Beanie handles index creation in init()
        # No separate check_schema() needed for MongoDB.

        service = PostService(uow_provider=app.uow_provider, ...)
    """

    def __init__(self, config: BeanieSettings) -> None:
        """
        Construct the app and register all entity classes.

        Does NOT call ``init_beanie()`` — that happens in ``await init()``.

        Args:
            config: Fully specified ``BeanieSettings`` instance.

        Edge cases:
            - Registration calls ``BeanieModelFactory.build()`` for each
              entity class synchronously — O(n) at startup.
        """
        # Plan 022 / AB-4: ``BeanieConfig`` and ``BeanieSettings`` were one
        # concept under two names, so KI-10 had to remap the former onto the
        # latter field-for-field here. The duplicate is now collapsed and the
        # settings object is passed straight through — the non-DI path and the
        # DI path reach ``BeanieRepositoryProvider`` with the same object type.
        self._provider = BeanieRepositoryProvider(settings=config)
        # No separate `self._provider.register(*config.entity_classes)` call
        # here — BeanieSettings.entity_classes is already registered by the
        # provider's own __init__ (varco_beanie/provider.py:70-71). A second
        # registration call was redundant (idempotent, guarded by `cls not in
        # self._built`) but is removed to keep exactly one registration path.

    @property
    def uow_provider(self) -> BeanieRepositoryProvider:
        """
        Return the ready-to-use ``BeanieRepositoryProvider``.

        Must call ``await init()`` before using the returned provider to
        create UoWs — Beanie requires ``init_beanie()`` to run first.

        Returns:
            The configured ``BeanieRepositoryProvider``.
        """
        return self._provider

    async def init(self) -> None:
        """
        Initialise Beanie with all registered Document classes.

        Must be called once at application startup, after ``register()`` and
        before any ``make_uow()`` call.  Idempotent — safe to call multiple
        times (Beanie reinitialises without error).

        Creates any declared indexes defined on Beanie ``Document`` classes.

        Async safety:   ✅ Delegates directly to ``BeanieRepositoryProvider.init()``.

        Edge cases:
            - Index creation is performed by Beanie during this call — it may
              be slow on large existing collections.
            - If the pymongo client cannot reach the database, this will raise
              a connection error from the pymongo driver.
        """
        # Delegate to the provider — it knows all registered Document classes
        # and calls beanie.init_beanie() with the correct arguments.
        await self._provider.init()


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "BeanieFastrestApp",
]

# AB-4's back-compat seam for this module specifically (Plan 022). The alias
# also exists on the ``varco_beanie`` package root, but ``from
# varco_beanie.bootstrap import BeanieConfig`` was this module's own documented
# import path — so serving it only from the root would have been a hard break
# for exactly the callers who followed the docstring. Resolves to the identical
# ``BeanieSettings`` class; deliberately out of ``__all__``; removed in 5.0.0
# (deprecated since 3.0.0; the 12-month floor put 4.0.0 out of reach —
# CONTRIBUTING.md §Removal window).
__getattr__ = deprecated_alias(
    "BeanieConfig",
    BeanieSettings,
    since="3.0.0",
    removed_in="5.0.0",
)
