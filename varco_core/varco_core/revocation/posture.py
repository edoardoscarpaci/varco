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

from varco_core.revocation.null import is_null_revocation_store

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
        require_jti:     The effective ``revocation_require_jti`` — the
                          explicit setting when one is given, else (the 4.0
                          ``None`` default, Plan 053 / FLIP-034-3) derived
                          as ``registry_wired``: ``True`` iff the registry's
                          wired store is non-``Null``. Per-call ``verify()``
                          kwargs are invisible here.
        token_scope_usable: ``True`` when ``TOKEN``-scope revocation is
                          guaranteed usable — i.e. ``require_jti`` is
                          enforced. ``False`` does not mean tokens never
                          carry a ``jti``; it means this function cannot
                          tell, and reports the conservative fact.
        require_jti_derived: ``True`` when ``require_jti`` was derived from
                          the wired store (the setting is ``None``) rather
                          than set explicitly (Plan 053). Appended with a
                          default so the frozen dataclass stays additive.
    """

    store_bound: bool
    store_kind: str
    registry_wired: bool
    failure_mode: str
    require_jti: bool
    token_scope_usable: bool
    require_jti_derived: bool = False


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

    # One definition of "non-Null" shared with the registry's derivation
    # (Plan 053 §D-null).
    store_bound = not is_null_revocation_store(store)

    registry_wired = not is_null_revocation_store(registry_store)

    # Same derivation as TrustedIssuerRegistry._check_revocation(): an
    # explicit setting wins; None derives from the WIRED store.
    explicit = settings.revocation_require_jti
    require_jti = explicit if explicit is not None else registry_wired

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
        require_jti=require_jti,
        token_scope_usable=require_jti,
        require_jti_derived=explicit is None,
    )
