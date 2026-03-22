"""
fastrest_beanie.bootstrap
==========================
One-stop bootstrap for Beanie (Motor / MongoDB) + fastrest applications.

``BeanieConfig``
    Frozen value object describing everything needed to create a working
    ``BeanieRepositoryProvider``: Motor client, database name, entity classes,
    and whether to use MongoDB transactions.

``BeanieFastrestApp``
    Thin coordinator that wires ``BeanieConfig`` into the provider, exposes
    the provider as ``uow_provider``, and provides an ``init()`` coroutine
    that initialises Beanie with all registered Document classes.

Minimal usage::

    from motor.motor_asyncio import AsyncIOMotorClient
    from fastrest_beanie.bootstrap import BeanieConfig, BeanieFastrestApp
    from myapp.domain import Post, User

    config = BeanieConfig(
        motor_client=AsyncIOMotorClient("mongodb://localhost:27017"),
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

from dataclasses import dataclass
from typing import Any

from fastrest_core.model import DomainModel
from fastrest_beanie.provider import BeanieRepositoryProvider


# ── BeanieConfig ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BeanieConfig:
    """
    Immutable configuration for a Beanie-backed fastrest application.

    Attributes:
        motor_client:  Connected ``AsyncIOMotorClient`` instance.
        db_name:       MongoDB database name (e.g. ``"myapp"``).
        entity_classes: Domain model classes to register.  Each class is passed
                        to ``BeanieModelFactory.build()`` which generates a
                        Beanie ``Document`` subclass and a domain mapper.
        transactional: Enable MongoDB multi-document transactions.  Requires
                       a replica set or sharded cluster — single-node
                       deployments will raise at runtime.

    Thread safety:  ✅ frozen=True — immutable after construction.
    Async safety:   ✅ Pure configuration value object; no I/O.

    Edge cases:
        - ``motor_client`` must already be connected — no connection check is
          performed at config construction time.
        - Passing an empty ``entity_classes`` tuple is valid but results in
          no repositories being available through the UoW.
        - ``transactional=True`` on a standalone MongoDB node raises a
          ``pymongo.errors.OperationFailure`` from the driver at transaction
          start — not at construction time.

    Example::

        from motor.motor_asyncio import AsyncIOMotorClient

        BeanieConfig(
            motor_client=AsyncIOMotorClient("mongodb://localhost:27017"),
            db_name="myapp",
            entity_classes=(User, Post, Comment),
            transactional=False,
        )
    """

    # Connected Motor client — the source of database operations
    motor_client: Any  # AsyncIOMotorClient; Any avoids hard motor dep at type-check

    # MongoDB database name
    db_name: str

    # Domain classes to register at startup
    entity_classes: tuple[type[DomainModel], ...]

    # Whether to use MongoDB multi-document transactions (requires replica set)
    transactional: bool = False


# ── BeanieFastrestApp ─────────────────────────────────────────────────────────


class BeanieFastrestApp:
    """
    Bootstrap coordinator for Beanie (Motor / MongoDB) + fastrest applications.

    Wires ``BeanieConfig`` into ``BeanieRepositoryProvider``, registers all
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

    def __init__(self, config: BeanieConfig) -> None:
        """
        Construct the app and register all entity classes.

        Does NOT call ``init_beanie()`` — that happens in ``await init()``.

        Args:
            config: Fully specified ``BeanieConfig`` instance.

        Edge cases:
            - Registration calls ``BeanieModelFactory.build()`` for each
              entity class synchronously — O(n) at startup.
        """
        self._provider = BeanieRepositoryProvider(
            motor_client=config.motor_client,
            db_name=config.db_name,
            transactional=config.transactional,
        )
        # Register all entity classes immediately so the provider is ready
        # to build UoWs after init() is called.
        self._provider.register(*config.entity_classes)

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
            - If the Motor client cannot reach the database, this will raise
              a connection error from the Motor driver.
        """
        # Delegate to the provider — it knows all registered Document classes
        # and calls beanie.init_beanie() with the correct arguments.
        await self._provider.init()


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "BeanieConfig",
    "BeanieFastrestApp",
]
