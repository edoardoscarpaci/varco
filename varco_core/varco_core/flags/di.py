"""
varco_core.flags.di
=====================
Providify DI integration for ``varco_core.flags`` (Plan 032 / D7).

Mirrors ``varco_casbin.di``'s ``enable_policy_authorizer`` precedent:
``NullFeatureFlags`` is bound by default (a scanned ``@Fallback
@Singleton`` — see ``null.py``), and ``enable_feature_flags(container)``
is the only way to swap in a real backend. This is deliberately **not** a
scanned ``@Configuration`` — ``scan`` auto-activates those, which would
silently change flag-resolution behaviour for any app that merely imports
``varco_core.flags`` (or scans ``varco_core`` recursively).

Usage::

    from providify import DIContainer
    from varco_core.flags.di import enable_feature_flags
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    container.scan("varco_core.flags", recursive=True)  # NullFeatureFlags bound
    enable_feature_flags(container)                      # opt-in: InMemoryFeatureFlags

    flags = await container.aget(AbstractFeatureFlags)   # InMemoryFeatureFlags
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from providify import Fallback, Provider

from varco_core.di import BACKEND_PRIORITY
from varco_core.di.features import feature_default
from varco_core.flags.base import AbstractFeatureFlags
from varco_core.flags.memory import InMemoryFeatureFlags

if TYPE_CHECKING:
    from providify import DIContainer

__all__ = ["enable_feature_flags"]


@Fallback
@feature_default("feature_flags")
@Provider(singleton=True, priority=BACKEND_PRIORITY)
def _feature_flags_when_enabled() -> AbstractFeatureFlags:
    """
    Plan 047 §D-features — the env twin of ``enable_feature_flags()``.

    **Repair round, Decision 4:** a **private**, hand-registered
    ``@Provider`` function — mirrors ``varco_core.revocation.di``'s
    ``_in_memory_revocation_store_when_selected`` shape exactly, rather
    than relying on ``container.scan()`` to auto-discover a public
    module-level provider. ``varco_core.di.bootstrap()`` registers it
    explicitly via ``container.provide()`` (guarded by the same
    per-container idempotency set the revocation twin uses), so all three
    feature twins in this repo — this one, the tenancy one, and the
    revocation one — now share one registration shape instead of two. It
    is still gated by ``@feature_default("feature_flags")``
    (``VARCO_FEATURE_FLAGS_ENABLED``), so registering it changes nothing
    unless the operator opts in — the same "scan/bootstrap never silently
    changes behaviour" invariant ``NullFeatureFlags`` already relies on.

    Returns:
        An empty ``InMemoryFeatureFlags()``.
    """
    return InMemoryFeatureFlags()


@Provider(singleton=True)
def _provide_in_memory_feature_flags() -> AbstractFeatureFlags:
    """
    Module-level provider binding ``InMemoryFeatureFlags`` as the app
    ``AbstractFeatureFlags``.

    Module-level so ``scan`` does NOT auto-register it (scan only picks up
    ``@Singleton``/``@Configuration``) — it activates only when
    ``enable_feature_flags`` passes it to ``container.provide``.

    Returns:
        An empty ``InMemoryFeatureFlags()`` — every flag falls through to
        the caller's own default until the application populates it via its
        own wiring (this provider exists to demonstrate the opt-in seam,
        not to ship a pre-populated flag set).
    """
    return InMemoryFeatureFlags()


def enable_feature_flags(container: DIContainer) -> DIContainer:
    """
    Opt in to ``InMemoryFeatureFlags`` as the application's
    ``AbstractFeatureFlags``, shadowing the always-off ``NullFeatureFlags``
    default.

    Args:
        container: The ``DIContainer`` already scanned via
            ``container.scan("varco_core.flags", recursive=True)``.

    Returns:
        The same container, for chaining.

    Raises:
        AlreadyBoundError: A non-fallback ``AbstractFeatureFlags`` binding
            already exists — either the application's own, or a previous
            call to this function (§D-enable).

    Edge cases:
        - Calling before scanning is safe — ``container.provide`` does not
          require the scan to have happened first, but ``NullFeatureFlags``
          simply will not exist as a competing binding yet.

    Example::

        container = DIContainer()
        container.scan("varco_core.flags", recursive=True)
        enable_feature_flags(container)
    """
    from varco_core.di import AlreadyBoundError, is_user_bound  # noqa: PLC0415

    if is_user_bound(container, AbstractFeatureFlags):
        raise AlreadyBoundError(
            "A non-fallback AbstractFeatureFlags binding already exists — "
            "enable_feature_flags() shadows the @Fallback NullFeatureFlags default "
            "and cannot register a second competing binding."
        )
    container.provide(_provide_in_memory_feature_flags)
    return container
