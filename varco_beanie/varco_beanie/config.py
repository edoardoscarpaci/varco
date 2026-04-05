"""
varco_beanie.config
===================
Immutable settings bundle for the Beanie (Motor / MongoDB) backend.

``BeanieSettings`` is the single injectable configuration token for the
Beanie module.  It groups all connection and initialisation parameters so
the DI container can resolve them from a single ``Inject[BeanieSettings]``
declaration.

Kept in a separate module from ``di.py`` to break a potential circular import:
``provider.py`` → ``Inject[BeanieSettings]`` → must not import ``di.py``
(which imports ``provider.py``).  Both modules import from here instead.

DESIGN: frozen dataclass over pydantic/env settings
    ✅ No env-var parsing needed — MongoDB clients are always constructed
       explicitly (connection strings vary too much for env-var defaults).
    ✅ Frozen → hashable → safe as singleton DI token.
    ✅ Pure stdlib — no pydantic dependency at the settings layer.
    ❌ No automatic env-var injection — callers must build the client manually.
       Acceptable: ``AsyncMongoClient`` is typically wired from a factory.

Thread safety:  ✅ Immutable after construction — frozen=True.
Async safety:   ✅ No mutable state; no async operations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # AsyncMongoClient is only used for the type annotation — guard to avoid
    # importing pymongo at module level in environments that mock the client.
    pass

from varco_core.model import DomainModel


@dataclass(frozen=True)
class BeanieSettings:
    """
    Immutable configuration bundle for the Beanie DI module.

    Frozen so it can safely be shared across threads and cached as a
    singleton without defensive copying.

    DESIGN: separate settings object over constructor injection on
    ``BeanieRepositoryProvider``
      ✅ All config grouped in one place — easy to swap for tests.
      ✅ Frozen dataclass is hashable — can be used as a dict key if needed.
      ✅ Keeps provider ``__init__`` to a single ``Inject[BeanieSettings]`` param.
      ❌ One extra level of indirection vs passing ``mongo_client`` directly.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ No async state.

    Args:
        mongo_client:   A connected ``AsyncMongoClient`` instance (pymongo>=4.11).
        db_name:        MongoDB database name.
        entity_classes: Domain model classes to register at provider init time.
                        Can also be registered later via
                        ``provider.register(*classes)`` before first use.
        transactional:  Wrap all UoW operations in a MongoDB transaction.
                        Requires a replica set or sharded cluster — standalone
                        instances do not support multi-document transactions.

    Edge cases:
        - Empty ``entity_classes`` is allowed — register entities later via
          ``provider.register(*classes)`` before first ``make_uow()`` call.
        - ``transactional=True`` on a standalone node raises at runtime when
          the first transaction begins, not at construction time.
    """

    mongo_client: Any  # AsyncMongoClient — Any avoids hard pymongo import
    db_name: str
    entity_classes: tuple[type[DomainModel], ...] = field(default_factory=tuple)
    transactional: bool = False


__all__ = ["BeanieSettings"]
