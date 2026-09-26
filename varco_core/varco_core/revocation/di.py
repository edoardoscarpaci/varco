"""
varco_core.revocation.di
===========================

Providify DI integration for ``varco_core.revocation`` (Plan 034 / S13a,
Step 19, §D-S13-di).

Mirrors ``varco_core.flags.di``'s ``enable_feature_flags`` precedent
verbatim: ``NullTokenRevocationStore`` is bound by default (a scanned
``@Singleton`` at the lowest priority — see ``null.py``), and
``enable_token_revocation(container)`` is the only way to swap in
``InMemoryTokenRevocationStore``. This is deliberately **not** a scanned
``@Configuration`` — ``scan`` auto-activates those, which would silently
turn revocation checking on for any app that merely imports
``varco_core.revocation`` or scans ``varco_core`` recursively.

⚠️ **Binding a store here does not by itself enable checking.** A store is
only consulted when it is *also* passed to
``TrustedIssuerRegistry(revocation_store=...)`` — §D-S13-hook forbids
``varco_core`` from ever reaching for the process-global container internally,
so that wiring is always the application's own explicit step. This two-step is the
single most likely way to think revocation is on when it is not; it is a
Pitfalls-table row and an ``inspect_revocation_posture()`` field
(``store_bound_but_registry_unwired`` — see ``varco_core.revocation.posture``).

Usage::

    from providify import DIContainer
    from varco_core.revocation import AbstractTokenRevocationStore
    from varco_core.revocation.di import enable_token_revocation

    container = DIContainer()
    container.scan("varco_core.revocation", recursive=True)  # NullTokenRevocationStore bound
    enable_token_revocation(container)                        # opt-in: InMemoryTokenRevocationStore

    store = await container.aget(AbstractTokenRevocationStore)  # InMemoryTokenRevocationStore

    # Still required — the two-step:
    registry = TrustedIssuerRegistry(revocation_store=store)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from providify import Fallback, Provider

from varco_core.di import BACKEND_PRIORITY
from varco_core.di.select import selected_by
from varco_core.revocation.base import AbstractTokenRevocationStore
from varco_core.revocation.memory import InMemoryTokenRevocationStore

if TYPE_CHECKING:
    from providify import DIContainer

__all__ = ["enable_token_revocation"]


@Fallback
@selected_by("revocation", "memory")
@Provider(singleton=True, priority=BACKEND_PRIORITY)
def _in_memory_revocation_store_when_selected() -> AbstractTokenRevocationStore:
    """
    Plan 047 N9 — the env twin of ``enable_token_revocation()``.

    Gated by ``@selected_by("revocation", "memory")`` (active only when
    ``VARCO_REVOCATION_BACKEND=memory``). Deliberately **not** left to
    ``container.scan()`` even though its name is public and would
    otherwise be picked up — ``varco_core.di.bootstrap()`` registers it
    explicitly via ``container.provide()`` instead, preserving the
    documented invariant that ``container.scan("varco_core.revocation",
    ...)`` alone never silently starts revocation checking (this module's
    own docstring, and CLAUDE.md's revocation two-step rule). The gate
    itself makes the binding inert unless the operator opts in either
    way, but the explicit-registration shape keeps that invariant
    literally, mechanically true rather than merely "true in practice".

    ⚠️ Same two-step caveat as ``enable_token_revocation()`` — binding
    this store does not, by itself, wire it into
    ``TrustedIssuerRegistry(revocation_store=...)``.

    Returns:
        An empty ``InMemoryTokenRevocationStore()``.
    """
    return InMemoryTokenRevocationStore()


@Provider(singleton=True)
def _provide_in_memory_revocation_store() -> AbstractTokenRevocationStore:
    """
    Module-level provider binding ``InMemoryTokenRevocationStore`` as the
    app ``AbstractTokenRevocationStore``.

    Module-level so ``scan`` does NOT auto-register it (scan only picks up
    ``@Singleton``/``@Configuration``) — it activates only when
    ``enable_token_revocation`` passes it to ``container.provide``.

    Returns:
        An empty ``InMemoryTokenRevocationStore()``.
    """
    return InMemoryTokenRevocationStore()


def enable_token_revocation(container: DIContainer) -> DIContainer:
    """
    Opt in to ``InMemoryTokenRevocationStore`` as the application's
    ``AbstractTokenRevocationStore``, shadowing the always-off
    ``NullTokenRevocationStore`` default.

    Args:
        container: The ``DIContainer`` already scanned via
            ``container.scan("varco_core.revocation", recursive=True)``.

    Returns:
        The same container, for chaining.

    Raises:
        AlreadyBoundError: A non-fallback ``AbstractTokenRevocationStore``
            binding already exists — either the application's own, or a
            previous call to this or
            ``varco_redis.di.enable_redis_token_revocation`` (both flip the
            same seam, §D-enable).

    Edge cases:
        - Calling before scanning is safe — ``container.provide`` does not
          require the scan to have happened first.
        - Does **not** wire the resulting store into any
          ``TrustedIssuerRegistry`` — see the module docstring's two-step
          warning.

    Example::

        container = DIContainer()
        container.scan("varco_core.revocation", recursive=True)
        enable_token_revocation(container)
    """
    from varco_core.di import AlreadyBoundError, is_user_bound  # noqa: PLC0415

    if is_user_bound(container, AbstractTokenRevocationStore):
        raise AlreadyBoundError(
            "A non-fallback AbstractTokenRevocationStore binding already exists — "
            "enable_token_revocation()/enable_redis_token_revocation() flip the same "
            "seam and only one may win."
        )
    container.provide(_provide_in_memory_revocation_store)
    return container
