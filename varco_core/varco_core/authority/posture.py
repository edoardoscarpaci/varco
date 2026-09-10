"""
varco_core.authority.posture
=============================

``inspect_jwks_posture()`` — a **pure, read-only** introspection function over
``TrustedIssuerRegistry`` background-refresh wiring (Plan 041 / S22,
§D-S22-posture).

Reports facts. Plan 036 owns the judgement, the thresholds, and the startup
wiring — do not add a warning, a raise, or a lifespan hook here. Modelled
line-for-line on ``varco_core.revocation.posture.inspect_revocation_posture()``
(same module docstring shape, same "reports facts" discipline).

Thread safety:  ✅ Pure function — no shared state, no I/O.
Async safety:   ✅ Synchronous, no I/O — safe to call from anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_core.authority.registry import TrustedIssuerRegistry

__all__ = ["JwksPostureReport", "inspect_jwks_posture"]


@dataclass(frozen=True)
class JwksPostureReport:
    """
    A snapshot of facts about a ``TrustedIssuerRegistry``'s JWKS refresh wiring.

    Attributes:
        registry_present:   ``True`` when a ``registry`` was passed at all.
                             ``False`` is a valid, common state (no registry
                             constructed yet) and every other field is
                             reported as its own "nothing to report" value.
        remote_source_count: Number of registered entries whose source is a
                             remote fetch (``JwksUrlSource``/
                             ``OidcDiscoverySource``) — the sources a
                             background refresher actually matters for.
                             PEM-backed and in-memory (``AuthoritySource``)
                             entries never go stale on their own and are not
                             counted.
        ttl_seconds:         The registry's configured
                             ``VARCO_JWKS_TTL_SECONDS`` value (or the
                             constructor override) — "the age at which the
                             cached keyset is considered stale".
        min_refresh_interval: The registry's configured
                             ``VARCO_JWKS_MIN_REFRESH_SECONDS`` value (or the
                             constructor override) — the reactive-refresh
                             rate limit, and the floor a background period is
                             clamped up to.
        refresher_running:   ``True`` iff ``start_refresh()`` has an active
                             background task right now.
        effective_interval:  The background refresher's resolved tick period
                             — ``0.0`` when it has never run.
        keysets_loaded:      Number of registered entries whose keyset has
                             been loaded at least once (``entry._keyset is
                             not None``) — a zero-entry or never-``load_all()``ed
                             registry reports ``0`` here without raising.

    ✅ **The decisive finding this report exists to make visible**:
       ``refresher_running=False`` with ``remote_source_count > 0`` means a
       key an issuer removed from its JWKS stays trusted in this process
       indefinitely — nothing will ever re-fetch except a reactive
       kid-not-found miss, which by definition cannot detect a *removed* key.
    """

    registry_present: bool
    remote_source_count: int
    ttl_seconds: float
    min_refresh_interval: float
    refresher_running: bool
    effective_interval: float
    keysets_loaded: int


def inspect_jwks_posture(registry: TrustedIssuerRegistry | None = None) -> JwksPostureReport:
    """
    Report facts about a ``TrustedIssuerRegistry``'s JWKS background-refresh
    wiring.

    Args:
        registry: The application's ``TrustedIssuerRegistry``, if
                  constructed. ``None`` (the default) is a valid, common
                  state — every numeric field is reported as ``0``/``0.0``
                  and ``registry_present`` is ``False``.

    Returns:
        A ``JwksPostureReport``. Never raises, for any registry state
        (including zero registered entries, or a registry whose refresher
        has never been started).

    Example::

        report = inspect_jwks_posture(registry=my_registry)
        if report.remote_source_count > 0 and not report.refresher_running:
            ...  # Plan 036's judgement, not this function's
    """
    if registry is None:
        return JwksPostureReport(
            registry_present=False,
            remote_source_count=0,
            ttl_seconds=0.0,
            min_refresh_interval=0.0,
            refresher_running=False,
            effective_interval=0.0,
            keysets_loaded=0,
        )

    # Local imports — avoid a module-level import cycle with registry.py,
    # which does not import this module (posture.py is a leaf, same
    # discipline as varco_core.revocation.posture importing NullTokenRevocationStore
    # at module scope but the *registry* types only under TYPE_CHECKING).
    from varco_core.authority.sources.jwks_url import JwksUrlSource
    from varco_core.authority.sources.oidc import OidcDiscoverySource

    remote_source_count = sum(
        1
        for entry in registry._entries.values()
        if isinstance(entry.source, (JwksUrlSource, OidcDiscoverySource))
    )
    keysets_loaded = sum(1 for entry in registry._entries.values() if entry._keyset is not None)

    return JwksPostureReport(
        registry_present=True,
        remote_source_count=remote_source_count,
        ttl_seconds=registry._ttl_seconds,
        min_refresh_interval=registry._min_refresh_interval,
        refresher_running=registry.refresh_running,
        effective_interval=registry.refresh_interval,
        keysets_loaded=keysets_loaded,
    )
