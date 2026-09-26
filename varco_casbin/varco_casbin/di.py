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

import weakref

from providify import DIContainer, Fallback, Inject, Provider
from varco_core.auth import (
    AbstractAuthorizer,
    PolicyEngine,
    PolicyEngineAuthorizer,
    RequestMapper,
)
from varco_core.di import BACKEND_PRIORITY, AlreadyBoundError, is_user_bound
from varco_core.di.features import feature_default

from varco_casbin.config import CasbinSettings

#: Per-container idempotency guard for the policy_authorizer env twin
#: (Plan 047 / Step 9) — see bootstrap()'s docstring/comment for why
#: get_all_bindings()-based fn-identity detection does not work here.
_CASBIN_TWIN_REGISTERED: weakref.WeakSet[DIContainer] = weakref.WeakSet()


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


@Fallback
@feature_default("policy_authorizer")
@Provider(singleton=True, priority=BACKEND_PRIORITY)
def _policy_authorizer_when_enabled(
    engine: Inject[PolicyEngine],
) -> AbstractAuthorizer:
    """
    Plan 047 / Step 9 — the env twin of ``enable_policy_authorizer()``.

    Gated by ``@feature_default("policy_authorizer")``
    (``VARCO_POLICY_AUTHORIZER_ENABLED``) — inert unless the operator
    opts in. Registered by ``bootstrap()`` via ``container.provide()``
    (guarded, idempotent) rather than left to ``scan()``: CLAUDE.md's
    "never a scanned ``@Configuration`` that shadows the app's
    authorizer" rule stays literally true — this stays a hand-registered
    fallback, never something that activates merely because
    ``varco_casbin`` was imported/scanned.

    Args:
        engine: The scanned ``CasbinPolicyEngine`` (as ``PolicyEngine``).

    Returns:
        A ``PolicyEngineAuthorizer`` using the default ``RequestMapper``.
    """
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
        - Calling twice is safe: scanning is idempotent, and
          ``CasbinSettings`` is bound via ``bind_settings()``'s own
          per-``(cls, qualifier)`` idempotency guard (Plan 048 / DI-6,
          Step 12) — a second ``bootstrap()`` call registers no second
          binding.
        - ``container.ashutdown()`` must be awaited at process exit so the
          engine's ``@PreDestroy`` runs.
    """
    # Plan 048 / DI-6, Step 12 — CasbinSettings goes through
    # bind_settings() (fail-fast, one recipe) BEFORE scan(), rather than
    # the module-level `_provide_casbin_settings` @Provider + hand-rolled
    # is_user_bound() idempotency guard this used to be.
    from varco_core.settings import bind_settings

    bind_settings(container, CasbinSettings)

    container.scan("varco_casbin", recursive=True)
    # Plan 047 / Step 9 — the policy_authorizer env twin. Guarded by a
    # per-container WeakSet, NOT `get_all_bindings()` (which is
    # activation-aware — `container.get_all_bindings(AbstractAuthorizer)`
    # excludes our own binding whenever VARCO_POLICY_AUTHORIZER_ENABLED is
    # unset, since @Requires-gated bindings are invisible while inactive.
    # Checking `already_registered` that way is *always* False when the
    # twin is inactive, so a repeat bootstrap() call would re-register it
    # every time — measured live while implementing this step).
    if container not in _CASBIN_TWIN_REGISTERED:
        container.provide(_policy_authorizer_when_enabled)
        _CASBIN_TWIN_REGISTERED.add(container)
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
