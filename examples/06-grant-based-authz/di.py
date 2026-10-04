"""
di
==
Providify DI configuration module for the Document domain.

``DocumentModule`` is a ``@Configuration`` that registers the in-memory
``IUoWProvider`` implementation.  ``DocumentAssembler``, ``DocumentService``,
and ``DocumentAuthorizer`` are ``@Singleton``-decorated on their classes —
they self-register when the container scans the package.

What this module registers
--------------------------
    ``IUoWProvider`` → ``InMemoryUoWProvider`` (singleton, cached on first call)

What this module does NOT register
-----------------------------------
    ``AbstractDTOAssembler``  — ``DocumentAssembler`` self-registers via ``@Singleton``.
    ``AsyncService``          — ``DocumentService`` self-registers via ``@Singleton``.
    ``AbstractAuthorizer``    — ``DocumentAuthorizer`` self-registers via ``@Singleton``
                               — a non-fallback binding that wins over the
                               ``@Fallback`` ``BaseAuthorizer`` in every
                               environment (which, since 4.0, binds only under
                               ``VARCO_SECURITY_ENV=development``).

DESIGN: explicit @Provider for IUoWProvider over @Singleton on the class
    ``InMemoryUoWProvider`` is declared in ``repo.py`` which intentionally has
    no providify dependency (it is pure infrastructure).  The ``@Provider``
    here in ``di.py`` is the only place that couples the domain module to DI.

    ✅ ``repo.py`` stays framework-free and portable.
    ✅ The cached ``_provider`` instance ensures one shared store across all
       UoW instances — data persists between requests.
    ❌ One ``@Provider`` to write — acceptable cost.

Thread safety:  ✅ Registration is single-threaded at bootstrap time.
Async safety:   ✅ No async providers in this module.
"""

from __future__ import annotations

from providify import Configuration, Provider

# Import as private alias — callers should inject IUoWProvider, not _Impl.
from repo import InMemoryUoWProvider as _Impl
from varco_core.service.base import IUoWProvider


@Configuration
class DocumentModule:
    """
    DI configuration module for the Document domain.

    Installing this module registers:
        ``IUoWProvider`` → ``InMemoryUoWProvider`` singleton

    Prerequisites (must be installed/scanned before the first request):
        - varco_core must be scanned (registers ``BaseAuthorizer`` + defaults)
        - varco_fastapi must be scanned (registers framework defaults)

    Usage::

        container = DIContainer()
        container.scan("varco_core", recursive=True)
        container.scan("varco_fastapi", recursive=True)
        container.install(DocumentModule)
        container.scan("assembler")   # discovers DocumentAssembler
        container.scan("service")     # discovers DocumentService + DocumentAuthorizer

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

        The instance is created lazily on first call and cached so every
        UoW returned by ``make_uow()`` shares the same backing dict.

        Returns:
            The shared ``InMemoryUoWProvider``.

        Edge cases:
            - Two concurrent first calls (very unlikely at bootstrap) could
              create two providers — safe because bootstrap is single-threaded
              in practice.
        """
        if self._provider is None:
            self._provider = _Impl()
        return self._provider


__all__ = ["DocumentModule"]
