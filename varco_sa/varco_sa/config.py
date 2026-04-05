"""
varco_sa.config
===============
Immutable configuration value object for the SQLAlchemy async backend.

``SAConfig`` is the single injectable configuration token for the SA module.
It groups all connection and initialisation parameters so the DI container
can resolve them from a single ``Inject[SAConfig]`` declaration.

Kept in a separate module from ``bootstrap.py`` to break a potential circular
import:

    ``provider.py`` → ``Inject[SAConfig]`` → must not import ``bootstrap.py``
    (which imports ``provider.py``).  Both modules now import from here.

DESIGN: frozen dataclass over env-var-parsed pydantic model
    ✅ SQLAlchemy engines are always constructed explicitly — there is no
       env-var scheme that covers driver, pool size, SSL, etc.
    ✅ Frozen → immutable, hashable, safe as DI singleton token.
    ✅ Pure stdlib — no pydantic dependency at the config layer.
    ❌ No automatic env-var injection — callers must build the engine manually.
       Acceptable: ``create_async_engine()`` is typically a one-liner in app
       bootstrap code.

Thread safety:  ✅ Immutable after construction — frozen=True.
Async safety:   ✅ No async operations; pure value object.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import DeclarativeBase

from varco_core.model import DomainModel


@dataclass(frozen=True)
class SAConfig:
    """
    Immutable configuration for an SA-backed varco application.

    Attributes:
        engine:           Async SQLAlchemy engine.  Use
                          ``create_async_engine("dialect+driver://...")`` to
                          create one.
        base:             Shared ``DeclarativeBase`` subclass.  All SA ORM
                          classes generated from ``entity_classes`` are
                          attached to this base.
        entity_classes:   Domain model classes to register.  Passed directly
                          to ``provider.register()`` at construction time.
        session_options:  Keyword arguments forwarded to ``async_sessionmaker``.
                          Common keys: ``expire_on_commit`` (default: ``False``),
                          ``autoflush``.

    Thread safety:  ✅ frozen=True — immutable after construction.
    Async safety:   ✅ Pure configuration value object; no I/O.

    Edge cases:
        - ``entity_classes`` is stored as a ``tuple`` — ordering matters only
          for ``SAModelFactory.build()`` call order, not for correctness.
        - ``session_options`` defaults to ``{"expire_on_commit": False}``
          which is the recommended setting for async SQLAlchemy (avoids lazy
          loading errors after commit).

    Example::

        SAConfig(
            engine=create_async_engine("postgresql+asyncpg://user:pw@host/db"),
            base=Base,
            entity_classes=(User, Post, Comment),
            session_options={"expire_on_commit": False, "autoflush": False},
        )
    """

    # Connected async engine — the source of database sessions
    engine: AsyncEngine

    # Shared declarative base — all generated ORM classes inherit from this
    base: type[DeclarativeBase]

    # Domain classes to register at startup
    entity_classes: tuple[type[DomainModel], ...]

    # Extra keyword args for async_sessionmaker.
    # DESIGN: default expire_on_commit=False — prevents implicit lazy-load
    # errors after commit in async contexts where I/O is not allowed.
    session_options: dict[str, Any] = field(
        default_factory=lambda: {"expire_on_commit": False}
    )


__all__ = ["SAConfig"]
