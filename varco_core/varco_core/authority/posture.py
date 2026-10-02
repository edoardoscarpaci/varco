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

from varco_core.posture import PostureCollector, PostureFinding, PostureSeverity

if TYPE_CHECKING:
    from collections.abc import Callable

    from varco_core.authority.registry import TrustedIssuerRegistry

__all__ = ["JwksPostureCollector", "JwksPostureReport", "inspect_jwks_posture"]


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
                             ``VARCO_JWT_JWKS_TTL_SECONDS`` value (or the
                             constructor override) — "the age at which the
                             cached keyset is considered stale".
        min_refresh_interval: The registry's configured
                             ``VARCO_JWT_JWKS_MIN_REFRESH_SECONDS`` value (or the
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


class JwksPostureCollector(PostureCollector):
    """
    RO-2's first ``SecurityPosture`` collector — wraps
    :func:`inspect_jwks_posture` as a ``PostureCollector`` contribution
    (Plan 052 / §D-RO2).

    Args:
        registry_supplier: A zero-arg callable returning the app's
            ``TrustedIssuerRegistry``, or ``None`` if none is constructed
            yet. A **supplier**, not the registry itself — evaluated
            inside :meth:`collect`, so a registry built *after* this
            collector is contributed (the common case: ``CoreModule``
            contributes it at registration time, long before
            ``JwksRefreshModule`` — if present — starts the refresher at
            the ``EDGE`` phase) is still seen correctly.

    DESIGN: duck-types the registry's private attributes directly,
    falling back from :func:`inspect_jwks_posture` (§D-RO2 rationale)
        ✅ :func:`inspect_jwks_posture` is the single source of truth for a
           *real* ``TrustedIssuerRegistry`` (its own ``isinstance``-based
           remote/PEM source distinction). Tried first.
        ✅ A unit test exercising this collector should not have to
           construct a full ``TrustedIssuerRegistry`` with real
           ``JwksUrlSource`` entries just to assert a severity — a minimal
           stand-in exposing ``_entries``/``_refresh_task`` is enough. An
           ``AttributeError`` from the strict path (the stand-in lacks
           ``.refresh_running``/``entry.source``) falls back to a looser,
           purely duck-typed read (every ``_entries`` key counts, and
           ``_refresh_task is not None`` is "running" without calling
           ``.done()``).
        ❌ The fallback path over-counts PEM-backed (non-remote) sources as
           "remote" for a non-``TrustedIssuerRegistry`` stand-in — accepted,
           since it only ever engages for an object that is not the real
           registry type in the first place.
    """

    name = "jwks"

    def __init__(self, registry_supplier: Callable[[], TrustedIssuerRegistry | None]) -> None:
        self._registry_supplier = registry_supplier

    def collect(self) -> list[PostureFinding]:
        """
        Findings: ``auth.jwks_refresh_off`` (WARN) when remote sources
        exist with no running refresher; ``auth.jwks_keysets_unloaded``
        (INFO) when remote sources exist but nothing has been loaded yet.
        Empty when no registry is available, or nothing is remote.
        """
        registry = self._registry_supplier()
        if registry is None:
            return []

        try:
            report = inspect_jwks_posture(registry)
            remote_source_count = report.remote_source_count
            refresher_running = report.refresher_running
            keysets_loaded = report.keysets_loaded
        except AttributeError:
            entries = getattr(registry, "_entries", {}) or {}
            remote_source_count = len(entries)
            refresher_running = getattr(registry, "_refresh_task", None) is not None
            keysets_loaded = sum(
                1 for e in entries.values() if getattr(e, "_keyset", None) is not None
            )

        findings: list[PostureFinding] = []
        if remote_source_count > 0 and not refresher_running:
            findings.append(
                PostureFinding(
                    check="auth.jwks_refresh_off",
                    severity=PostureSeverity.WARN,
                    detail=(
                        f"{remote_source_count} remote JWKS source(s) and no background "
                        f"refresher — a key an issuer removes stays trusted."
                    ),
                    remediation=(
                        "JwksRefreshModule(registry) or VARCO_JWT_JWKS_TTL_SECONDS>0 + "
                        "start_refresh()"
                    ),
                )
            )
        if remote_source_count > 0 and keysets_loaded == 0:
            findings.append(
                PostureFinding(
                    check="auth.jwks_keysets_unloaded",
                    severity=PostureSeverity.INFO,
                    detail="no keyset loaded yet — load_all() has not run",
                    remediation="await registry.load_all()",
                )
            )
        return findings
