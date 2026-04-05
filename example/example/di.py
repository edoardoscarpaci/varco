"""
example.di
==========
Providify DI module for the Post domain.

``PostModule`` is a ``@Configuration`` class that wires together the Post
assembler, service, and consumer.  Install it alongside the backend and
framework modules during application bootstrap::

    container = DIContainer(scan=["example", "varco_sa", "varco_redis"], recursive=True)
    await container.ainstall(RedisEventBusConfiguration)
    await container.ainstall(RedisCacheConfiguration)
    container.install(SAModule)
    container.install(VarcoFastAPIModule)
    container.install(PostModule)            # ← this file
    bind_repositories(container, Post)

What this module registers
--------------------------
``PostAssembler`` and ``PostService`` are decorated with ``@Singleton``
so they self-register when the module is installed.  ``PostModule`` itself
exists to:

1. Express the Post domain as a named, importable unit that can be installed
   or replaced independently (e.g. swap to a test double in unit tests).
2. Serve as the single import point for the entire Post stack — the bootstrap
   only needs one ``container.install(PostModule)`` call.

What this module does NOT register
------------------------------------
- ``AbstractEventBus`` — provided by the bus module (Redis/Kafka).
- ``CacheBackend`` — provided by the cache module (Redis/In-Memory).
- ``AbstractEventProducer`` — picked up automatically by the DI container
  scan when ``BusEventProducer`` (or a subclass decorated with ``@Singleton``)
  is on the scan path.
- ``IUoWProvider`` — registered by ``SAModule.uow_provider``.
- ``AbstractAuthorizer`` — ``BaseAuthorizer`` (permissive) is auto-registered
  by varco_core at lowest priority; override with your own authorizer.

DESIGN: @Singleton on class over @Provider factory
    ``@Singleton`` on ``PostAssembler`` and ``PostService`` stamps the class
    with DI scope metadata at definition time.  When the container encounters
    them as injection targets, it constructs them once and caches the result.
    No explicit ``@Provider`` factory is needed.

    ✅ Less boilerplate — no factory functions per class.
    ✅ Scope is defined next to the class, not in a separate DI module.
    ❌ Requires providify to be installed when the module is imported.
       varco_core is zero-dependency; user-space code like this example
       accepts the dependency.

Thread safety:  ✅ Registration is single-threaded at bootstrap time.
Async safety:   ✅ No I/O during installation.
"""

from __future__ import annotations

from providify import Configuration

# Import the @Singleton-decorated classes so they are stamped and visible
# to the container when PostModule is installed.  These imports are the
# registration — no explicit container.bind() calls needed.


@Configuration
class PostModule:
    """
    DI configuration module for the Post domain.

    Installing this module registers:
        PostAssembler      → AbstractDTOAssembler[Post, PostCreate, PostRead, PostUpdate]
        PostService        → PostService (singleton; also satisfies CacheServiceMixin chain)
        PostEventConsumer  → PostEventConsumer (singleton; EventConsumer subclass)

    Prerequisites (must be installed before this module):
        - A bus module (AbstractEventBus binding)
        - A cache module (CacheBackend binding)
        - SAModule (IUoWProvider + RepositoryProvider bindings)
        - VarcoFastAPIModule (TaskRegistry, AbstractJobRunner bindings)

    Usage::

        container.install(PostModule)
        # All Post-related bindings are now available for injection.

    Thread safety:  ✅ Module instance is created once at install() time.
    Async safety:   ✅ No async providers in this module.
    """

    # No @Provider methods needed — PostAssembler, PostService, and
    # PostEventConsumer are all @Singleton-decorated on their classes.
    # The container discovers and resolves them via the @Singleton metadata
    # stamped at import time (triggered by the imports above).


__all__ = ["PostModule"]
