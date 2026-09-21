"""
varco_casbin.di
===============
Providify DI integration for ``varco_casbin``.

``CasbinPolicyEngine`` carries ``@Singleton`` and is discovered automatically by
``container.scan("varco_casbin", recursive=True)``.  Because it implements both
``PolicyEngine`` and ``PolicyManagement``, scanning binds it to both interfaces
— service code resolves ``PolicyEngine``, the REST router resolves
``PolicyManagement``.

``CasbinSettings`` is a pydantic ``BaseSettings`` and therefore cannot be a
providify ``@Singleton`` (providify would try to inject pydantic's ``**values``
constructor parameter).  ``bootstrap`` registers a clean provider for it instead.

Binding the *authorizer* is deliberately **opt-in**.  ``scan`` auto-registers
``@Singleton`` and ``@Configuration`` objects, so the authorizer is NOT a scanned
``@Configuration`` — it would silently shadow an application's own
``AbstractAuthorizer`` just by importing the package.  Instead call
``enable_policy_authorizer(container)`` explicitly; it registers a module-level
``@Provider`` (which ``scan`` does NOT auto-register) only when you ask for it.

Usage::

    from providify import DIContainer
    from varco_casbin.di import bootstrap, enable_policy_authorizer
    from varco_core.auth import PolicyEngine, PolicyManagement, AbstractAuthorizer

    container = bootstrap(DIContainer())            # engine + settings (no authorizer)
    enable_policy_authorizer(container)             # opt-in: bind the authorizer

    engine = await container.aget(PolicyEngine)     # CasbinPolicyEngine
    authz  = await container.aget(AbstractAuthorizer)  # PolicyEngineAuthorizer
"""

from __future__ import annotations

from providify import DIContainer, Inject, Provider
from varco_core.auth import (
    AbstractAuthorizer,
    PolicyEngine,
    PolicyEngineAuthorizer,
    RequestMapper,
)
from varco_core.di import AlreadyBoundError, is_user_bound

from varco_casbin.config import CasbinSettings


@Provider(singleton=True)
def _provide_casbin_settings() -> CasbinSettings:
    """
    Provider that builds ``CasbinSettings`` from the environment.

    Registered by ``bootstrap`` so the engine's ``Inject[CasbinSettings]``
    resolves to a ready instance.  This is the deterministic alternative to
    ``@Singleton`` on a pydantic ``BaseSettings`` (which providify cannot
    construct — see the note in ``varco_casbin.config``).

    Returns:
        A ``CasbinSettings`` populated from ``VARCO_CASBIN_*`` env vars.
    """
    return CasbinSettings.from_env()


@Provider(singleton=True)
def _provide_policy_authorizer(
    engine: Inject[PolicyEngine],
) -> AbstractAuthorizer:
    """
    Provider that binds ``PolicyEngineAuthorizer`` as the app ``AbstractAuthorizer``.

    Module-level so that ``scan`` does NOT auto-register it (scan only picks up
    ``@Singleton`` / ``@Configuration``).  It becomes active only when
    ``enable_policy_authorizer`` passes it to ``container.provide``.

    Args:
        engine: The scanned ``CasbinPolicyEngine`` (as ``PolicyEngine``).

    Returns:
        A ``PolicyEngineAuthorizer`` using the default ``RequestMapper``.
    """
    # Default mapper mirrors GrantBasedAuthorizer's key convention so token
    # grants and engine policy rules share one resource-key namespace.
    return PolicyEngineAuthorizer(engine, RequestMapper())


def bootstrap(container: DIContainer) -> DIContainer:
    """
    Bootstrap ``varco_casbin`` into a ``DIContainer``.

    Scans the package for ``@Singleton`` classes (``CasbinPolicyEngine``), binds
    the engine to both ``PolicyEngine`` and ``PolicyManagement``, and registers
    the ``CasbinSettings`` provider.  Does **not** bind ``AbstractAuthorizer`` —
    call ``enable_policy_authorizer`` for that.

    Args:
        container: The ``DIContainer`` to scan into. Required — this
            function never creates one and never reads
            the process-global container (Plan 046 §D-locator).

    Returns:
        The same ``container`` passed in, after scanning.

    Edge cases:
        - Calling twice is safe: scanning is idempotent, and the
          ``CasbinSettings`` provider is guarded by ``is_user_bound()`` so a
          second ``bootstrap()`` call registers no second provider (Plan 048
          replaces this hand guard with ``bind_settings()``'s own).
        - ``container.ashutdown()`` must be awaited at process exit so the
          engine's ``@PreDestroy`` runs.
    """
    container.scan("varco_casbin", recursive=True)
    # pydantic BaseSettings cannot be a @Singleton under providify — register a
    # clean provider (scan does not auto-register module-level @Provider
    # funcs). Guarded: container.provide() always appends, so an unguarded
    # second bootstrap() call would register a second, ambiguous provider —
    # 048 replaces this hand guard with bind_settings()'s own idempotency.
    if not is_user_bound(container, CasbinSettings):
        container.provide(_provide_casbin_settings)
    return container


def enable_policy_authorizer(container: DIContainer) -> DIContainer:
    """
    Opt in to Casbin-driven service-layer authorization.

    Registers ``PolicyEngineAuthorizer`` as the application ``AbstractAuthorizer``,
    shadowing the permissive ``BaseAuthorizer`` fallback (a ``@Fallback``
    binding — this registers a plain, non-fallback provider, which wins
    regardless of registration order).  Every existing ``AsyncService`` then
    enforces Casbin policy with no service-code changes.

    DESIGN: an explicit function rather than a scanned @Configuration
        ✅ Truly opt-in — ``scan`` auto-activates ``@Configuration`` objects, so
           a scanned config would shadow an app's own authorizer on import.  A
           module-level ``@Provider`` registered only on request cannot.
        ✅ One obvious call site documents that authorization behaviour changed.
        ❌ One extra call versus "magic on scan".  Intentional — explicit is safer.

    Args:
        container: The ``DIContainer`` already bootstrapped via ``bootstrap``.

    Returns:
        The same container, for chaining.

    Raises:
        AlreadyBoundError: A non-fallback ``AbstractAuthorizer`` binding
            already exists — either the application's own, or a previous
            call to this function (§D-enable). Bind your own authorizer
            through the application's normal DI wiring, or do not call this
            twice.

    Example::

        container = DIContainer()
        bootstrap(container)
        enable_policy_authorizer(container)
    """
    if is_user_bound(container, AbstractAuthorizer):
        raise AlreadyBoundError(
            "A non-fallback AbstractAuthorizer binding already exists — "
            "enable_policy_authorizer() shadows the @Fallback BaseAuthorizer default "
            "and cannot register a second competing binding."
        )
    container.provide(_provide_policy_authorizer)
    return container


__all__ = [
    "bootstrap",
    "enable_policy_authorizer",
]
