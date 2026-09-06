"""
varco_core.revocation.posture
================================

``inspect_revocation_posture()`` — a **pure, read-only** introspection
function over revocation wiring (Plan 034 / Phase 4, §D-034-seam).

Reports facts. Plan 036 owns the judgement, the thresholds, and the
startup wiring — do not add a warning, a raise, or a lifespan hook here.
Mirrors 037's ``inspect_rls_posture()`` precedent (``varco_sa``) and this
plan's own ``varco_fastapi.auth.posture.inspect_auth_posture()``: each
source plan exports its own typed report in its own package; 036
aggregates them.

Thread safety:  ✅ Pure function — no shared state, no I/O.
Async safety:   ✅ Synchronous, no I/O — safe to call from anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from varco_core.revocation.null import NullTokenRevocationStore

if TYPE_CHECKING:
    from varco_core.authority.registry import TrustedIssuerRegistry
    from varco_core.jwt.config import JwtVerificationSettings
    from varco_core.revocation.base import AbstractTokenRevocationStore

__all__ = ["RevocationPostureReport", "inspect_revocation_posture"]


@dataclass(frozen=True)
class RevocationPostureReport:
    """
    A snapshot of facts about the revocation wiring an app has assembled.

    Attributes:
        store_bound:     ``True`` when a non-``Null`` store was passed as
                          ``store=`` — i.e. *something* obtained a real
                          store (e.g. from DI), independent of whether it
                          was ever given to a registry.
        store_kind:      The concrete class name of whichever store is
                          known (the explicit ``store=`` argument if given,
                          else the ``registry``'s own bound store, else
                          ``"NullTokenRevocationStore"``).
        registry_wired:  ``True`` when ``registry`` is given and its own
                          ``revocation_store`` is a non-``Null`` store —
                          the §D-S13-di two-step footgun, made reportable:
                          ``store_bound=True`` with ``registry_wired=False``
                          means a store was obtained but never passed to
                          ``TrustedIssuerRegistry(revocation_store=...)``.
        failure_mode:    ``"fail_closed"`` or ``"fail_open"`` — the
                          effective ``JwtVerificationSettings.revocation_failure_mode``.
        require_jti:     The effective ``revocation_require_jti`` setting.
        token_scope_usable: ``True`` when ``TOKEN``-scope revocation is
                          guaranteed usable — i.e. ``require_jti`` is
                          enforced. ``False`` does not mean tokens never
                          carry a ``jti``; it means this function cannot
                          tell, and reports the conservative fact.
    """

    store_bound: bool
    store_kind: str
    registry_wired: bool
    failure_mode: str
    require_jti: bool
    token_scope_usable: bool


def inspect_revocation_posture(
    registry: TrustedIssuerRegistry | None = None,
    store: AbstractTokenRevocationStore | None = None,
    settings: JwtVerificationSettings | None = None,
) -> RevocationPostureReport:
    """
    Report facts about the revocation wiring assembled so far.

    Args:
        registry: The application's ``TrustedIssuerRegistry``, if
                  constructed. ``None`` is a valid, common state (an app
                  that has not opted into revocation at all).
        store:    A revocation store obtained independently (e.g. from DI)
                  — used to detect the §D-S13-di two-step footgun even
                  when it was never passed to ``registry``.
        settings: ``JwtVerificationSettings`` to read failure-mode/jti
                  settings from. ``None`` (default) reads
                  ``JwtVerificationSettings.from_env()``.

    Returns:
        A ``RevocationPostureReport``. Never raises.

    Example::

        report = inspect_revocation_posture(registry=my_registry, store=my_store)
        if report.store_bound and not report.registry_wired:
            ...  # Plan 036's judgement, not this function's
    """
    if settings is None:
        from varco_core.jwt.config import JwtVerificationSettings as _Settings

        settings = _Settings.from_env()

    registry_store = getattr(registry, "_revocation_store", None) if registry is not None else None

    store_bound = store is not None and not isinstance(store, NullTokenRevocationStore)

    registry_wired = registry_store is not None and not isinstance(
        registry_store, NullTokenRevocationStore
    )

    # store_kind prefers the explicit store=, then the registry's own
    # store, then falls back to the Null Object's name — "no real store
    # known" is reported the same way a bound-but-inert Null store would be.
    known_store = store if store is not None else registry_store
    store_kind = (
        type(known_store).__name__ if known_store is not None else "NullTokenRevocationStore"
    )

    return RevocationPostureReport(
        store_bound=store_bound,
        store_kind=store_kind,
        registry_wired=registry_wired,
        failure_mode=settings.revocation_failure_mode.value,
        require_jti=settings.revocation_require_jti,
        token_scope_usable=settings.revocation_require_jti,
    )
