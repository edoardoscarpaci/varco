"""
di
==
Providify DI configuration module for the Product domain.

``ProductModule`` is a ``@Configuration`` that registers the in-memory
``IUoWProvider`` implementation.  ``ProductAssembler`` and ``ProductService``
are ``@Singleton``-decorated on their classes — they self-register when the
container scans the package.

What this module registers
--------------------------
    ``IUoWProvider`` → ``InMemoryUoWProvider`` (singleton, cached on first call)

What this module does NOT register
-----------------------------------
    ``AbstractDTOAssembler``  — ``ProductAssembler`` self-registers via ``@Singleton``.
    ``AsyncService``          — ``ProductService`` self-registers via ``@Singleton``.
    ``ProductRouter``         — self-registers via ``@Singleton``.
    ``AbstractAuthorizer``    — ``BaseAuthorizer`` (permissive) is varco_core's
                               ``@Fallback`` default, a binding only under
                               ``VARCO_SECURITY_ENV=development`` (4.0, Plan
                               053) — run this example with that variable set.

DESIGN: explicit @Provider for IUoWProvider over @Singleton on the class
    ``InMemoryUoWProvider`` is declared in ``repo.py`` which intentionally has
    no providify dependency (it is pure infrastructure).  The ``@Provider``
    here in ``di.py`` is the only place that couples the domain module to the
    DI framework.

    ✅ ``repo.py`` stays framework-free and portable.
    ✅ The cached ``_provider`` instance ensures one shared store across all
       UoW instances — data persists between requests.
    ❌ One ``@Provider`` to write — acceptable cost.

DESIGN: ``_provider`` cached on the module instance
    ``@Configuration`` creates a single module instance at ``container.install()``
    time.  Caching ``_provider`` on that instance guarantees the same
    ``InMemoryUoWProvider`` (and therefore the same backing dict) is returned on
    every call to ``uow_provider()``, regardless of how many times the container
    resolves ``IUoWProvider``.

    ✅ Same shared store across the process lifetime.
    ✅ Thread-safe at bootstrap — ``container.install()`` is synchronous and
       single-threaded; ``_provider`` is set before any request arrives.
    ❌ Cannot be reset between tests without creating a fresh container — use
       a fresh ``DIContainer()`` per test to get an isolated store.

Thread safety:  ✅ Registration is single-threaded at bootstrap time.
Async safety:   ✅ No async providers in this module.
"""

from __future__ import annotations

from providify import Configuration, Provider

# Import as private alias — callers should inject IUoWProvider, not _Impl.
from repo import InMemoryUoWProvider as _Impl
from varco_core.service.base import IUoWProvider


@Configuration
class ProductModule:
    """
    DI configuration module for the Product domain.

    Installing this module registers:
        ``IUoWProvider`` → ``InMemoryUoWProvider`` singleton

    Prerequisites (must be installed/scanned before the first request):
        - ``VarcoFastAPIConfiguration`` (``TaskRegistry``, ``AbstractJobRunner``, defaults)

    Usage::

        container = DIContainer()
        container.install(VarcoFastAPIConfiguration)
        container.install(ProductModule)
        container.scan(".", recursive=True)  # discovers @Singleton classes
        # ProductAssembler, ProductService, ProductRouter are now available.

    Thread safety:  ✅ Module instance is created once at install() time.
    Async safety:   ✅ No async providers.
    """

    # Cached ``InMemoryUoWProvider`` — one instance for the process lifetime.
    # Using ``None`` sentinel and lazy init so the instance is only created
    # on first DI resolution (not at module import time).
    _provider: IUoWProvider | None = None

    @Provider(singleton=True)
    def uow_provider(self) -> IUoWProvider:
        """
        Return the shared ``InMemoryUoWProvider`` singleton.

        The instance is created lazily on first call and cached on this
        module instance so every UoW returned by ``make_uow()`` shares
        the same backing dict.

        Returns:
            The shared ``InMemoryUoWProvider``.

        Edge cases:
            - Two concurrent first calls (very unlikely at bootstrap) could
              create two providers — safe because bootstrap is single-threaded
              in practice.
        """
        if self._provider is None:
            # Lazy creation — deferred until DI first resolves IUoWProvider
            # so the event loop is running if the provider ever needs it.
            self._provider = _Impl()
        return self._provider


__all__ = ["ProductModule"]
