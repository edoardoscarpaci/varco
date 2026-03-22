"""
varco_core.providers
========================
``RepositoryProvider`` abstract base class + ``autodiscover()`` helper.

Concrete implementations live in the backend packages:
- ``varco_sa.provider.SQLAlchemyRepositoryProvider``
- ``varco_beanie.provider.BeanieRepositoryProvider``

Usage (in your app)::

    # Manual registration
    from varco_sa.provider import SQLAlchemyRepositoryProvider
    provider = SQLAlchemyRepositoryProvider(base=Base, session_factory=sessions)
    provider.register(User, Post)

    # Auto-discovery
    provider.autodiscover("myapp.models")

    async with provider.make_uow() as uow:
        user = await uow.users.save(User(name="Edo", email="..."))
"""

from __future__ import annotations

import importlib
import pkgutil
from abc import ABC, abstractmethod
from types import ModuleType
from typing import Any, TypeVar

from .model import DomainModel
from .registry import DomainModelRegistry
from .repository import AsyncRepository

D = TypeVar("D", bound=DomainModel)


class RepositoryProvider(ABC):
    """
    Abstract factory that produces ``AsyncRepository`` instances for any
    registered ``DomainModel`` subclass.

    Register as a ``@singleton`` in your DI container (e.g. ``providify``) —
    the factory and its internal ORM class cache are created once at startup.

    Two registration paths
    ----------------------
    **Manual** — explicit list at startup::

        provider.register(User, Post, Tag)

    **Auto-discovery** — scan a module / package for ``@register``-stamped
    classes::

        provider.autodiscover("myapp.models")

    Both paths can be combined freely.
    """

    @abstractmethod
    def register(self, *domain_classes: type[DomainModel]) -> None:
        """
        Register one or more ``DomainModel`` subclasses.

        Triggers ORM class generation for each class immediately.
        Call once at startup before any repository operation.

        Args:
            *domain_classes: Any number of ``DomainModel`` subclasses.
        """

    def autodiscover(self, *targets: str | ModuleType) -> None:
        """
        Import one or more modules / packages and register every
        ``DomainModel`` subclass decorated with ``@register``.

        Each target is a dotted module path string (e.g. ``"myapp.models"``)
        or an already-imported module object.  Package targets are scanned
        recursively so a single call covers an entire models directory.

        Args:
            *targets: Dotted module paths or module objects.

        Raises:
            ModuleNotFoundError: A string target cannot be imported.
            TypeError:           A target is neither a string nor a module.
            ValueError:          Called with no targets.

        Edge cases:
            - Modules not yet imported are imported as a side-effect —
              ``@register`` decorators run and classes enter
              ``DomainModelRegistry`` at import time.
            - Calling ``autodiscover()`` twice for the same target is safe —
              ``register()`` is idempotent.
            - Only classes whose ``__module__`` is inside the target package
              boundary are picked up — no accidental third-party captures.

        Example::

            provider.autodiscover("myapp.models")           # whole package
            provider.autodiscover("myapp.models.user")      # single module
            import myapp.models.post
            provider.autodiscover("myapp.models.user", myapp.models.post)
        """
        if not targets:
            raise ValueError(
                "autodiscover() requires at least one target. "
                "Example: provider.autodiscover('myapp.models')"
            )

        discovered: list[type[DomainModel]] = []
        for target in targets:
            if isinstance(target, str):
                module = importlib.import_module(target)
            elif isinstance(target, ModuleType):
                module = target
            else:
                raise TypeError(
                    f"autodiscover() targets must be dotted module path "
                    f"strings or module objects, got {type(target)!r}."
                )
            discovered.extend(_scan_module(module))

        if discovered:
            self.register(*discovered)

    @abstractmethod
    def get_repository(self, entity_cls: type[D]) -> AsyncRepository[D, Any]:
        """
        Return a ready-to-use repository for ``entity_cls``.

        Args:
            entity_cls: A previously registered ``DomainModel`` subclass.

        Raises:
            KeyError: ``entity_cls`` was not registered.
        """

    @abstractmethod
    def make_uow(self) -> Any:
        """
        Return a fresh ``AsyncUnitOfWork`` with all registered repositories
        pre-wired as attributes.

        Repository attribute names are derived automatically:
        ``User`` → ``uow.users``, ``Post`` → ``uow.posts``.
        """


# ── Module-level helper (used by autodiscover) ────────────────────────────────


def _scan_module(module: ModuleType) -> list[type[DomainModel]]:
    """
    Return all ``@register``-stamped ``DomainModel`` subclasses whose
    ``__module__`` belongs to ``module`` or any of its sub-modules.

    Recursively imports sub-modules of packages so that ``@register``
    decorators run before the filter is applied.

    Args:
        module: An already-imported module or package.

    Returns:
        De-duplicated list in ``DomainModelRegistry`` declaration order.

    Edge cases:
        - Sub-module import errors surface immediately (fail-fast).
        - Re-exports in ``__init__.py`` are handled correctly because we
          match on ``cls.__module__``, not on the module the class was
          imported from.
    """
    if hasattr(module, "__path__"):
        for info in pkgutil.walk_packages(
            module.__path__, prefix=module.__name__ + "."
        ):
            importlib.import_module(info.name)

    base = module.__name__
    return [
        cls
        for cls in DomainModelRegistry.all()
        if cls.__module__ == base or cls.__module__.startswith(base + ".")
    ]
