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
``varco_core`` reaching for ``DIContainer.current()`` internally, so that
wiring is always the application's own explicit step. This two-step is the
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

from typing import Any

from providify import Provider

from varco_core.revocation.base import AbstractTokenRevocationStore
from varco_core.revocation.memory import InMemoryTokenRevocationStore

__all__ = ["enable_token_revocation"]


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


def enable_token_revocation(container: Any) -> Any:
    """
    Opt in to ``InMemoryTokenRevocationStore`` as the application's
    ``AbstractTokenRevocationStore``, shadowing the always-off
    ``NullTokenRevocationStore`` default.

    Args:
        container: The ``DIContainer`` already scanned via
            ``container.scan("varco_core.revocation", recursive=True)``.

    Returns:
        The same container, for chaining.

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
    container.provide(_provide_in_memory_revocation_store)
    return container
