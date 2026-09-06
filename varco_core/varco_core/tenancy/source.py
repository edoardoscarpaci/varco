"""
varco_core.tenancy.source
==========================
Transport-neutral tenant-identity provenance primitives (Plan 033 / S6,
§D-S6-abc, §D-S6-chain, §D-S6-oq2).

``TenantSource`` is the seam an app configures to say *where a request's
tenant identity is allowed to come from*. ``TenantSourceChain`` runs an
ordered set of sources, cross-checks their claims, and returns a
``TenantProvenance`` verdict — never raises, never rejects a request that
carried no tenant signal at all.

DESIGN: a frozen ``TenantRequest`` record instead of the framework ``Request``
    ✅ Keeps this module free of ``starlette``/``fastapi`` — the same seam
       rule CLAUDE.md already states for ``varco_core.migration`` and
       ``varco_core.tenancy`` generally: ``varco_fastapi.tenancy`` may import
       ``varco_core.tenancy``, never the reverse.
    ✅ Every source is unit-testable with a two-line literal — no
       ``TestClient``, no ASGI scope.
    ✅ ``auth`` carries an already-**verified** ``AuthContext`` — the type
       itself says a source must never parse a raw token.
    ✅ Frozen + ``Mapping`` means a source physically cannot mutate the
       request.
    ❌ The HTTP adapter must normalise header case and strip the port before
       constructing one; a future gRPC adapter repeats those six lines.
       Accepted — the alternative leaks ``starlette`` into ``varco_core``.
    Rejected — ``resolve(request: Request)``: ❌ breaks the seam rule; ❌
    makes every source test an HTTP test.
    Rejected — ``async def resolve()``: ❌ nothing shipped needs I/O, and an
    async signature invites an out-of-tree source onto the request-routing
    hot path with a database lookup — exactly what the membership park
    rejects (``BACKLOG.md:97``).
    Rejected — letting a source raise to signal rejection: ❌ then rejection
    policy lives in N sources instead of one place, and a buggy out-of-tree
    source can 500 every request.

Thread safety:  ✅ Every type here is frozen or stateless.
Async safety:   ✅ No I/O, no ``await`` anywhere in this module.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from varco_core.auth.base import AuthContext
    from varco_core.auth.delegation import DelegationRecord
    from varco_core.tenancy.membership import MembershipDecision

logger = logging.getLogger(__name__)

__all__ = [
    "TenantTrust",
    "TenantRequest",
    "TenantClaim",
    "TenantSource",
    "CrossCheckMode",
    "TenantSourceChain",
    "TenantProvenance",
]


class TenantTrust(IntEnum):
    """
    How much a claim's origin should be trusted, ranked per brief 006 §1.

    Ordering is the whole point: the chain's winner is the highest-trust
    claim among those that agree, and a tie is broken by chain order only
    when trust is equal.
    """

    LOW = 10  # a bare client-supplied header, no auth binding      ("❌ LOWEST")
    MEDIUM = 20  # a trusted-proxy-supplied value (X-Forwarded-Host) ("⚠️ MEDIUM")
    HIGH = 30  # subdomain read from the connection's own Host       ("✅ HIGH")
    HIGHEST = 40  # a signed, issuer-bound JWT claim                  ("✅ HIGHEST")


@dataclass(frozen=True)
class TenantRequest:
    """
    Everything a ``TenantSource`` may look at. No HTTP types.

    Args:
        headers: Lower-cased, adapter-normalised header mapping.
        host: The connection's own ``Host``, with the port already stripped
            by the adapter. ``None`` when the transport has no such concept.
        path: The request path. Defaults to ``"/"``.
        auth: An already-**verified** ``AuthContext``, or ``None`` when no
            server-auth ran (or it ran and produced an anonymous context).
    """

    headers: Mapping[str, str]
    host: str | None = None
    path: str = "/"
    auth: AuthContext | None = None


@dataclass(frozen=True)
class TenantClaim:
    """A single source's opinion of the tenant, with its trust level."""

    tenant_id: str
    source: str
    trust: TenantTrust


class TenantSource(ABC):
    """
    A single, pluggable origin of tenant identity.

    Args (class-level contract data, declared by every subclass):
        name: A short, stable, unique identifier (e.g. ``"jwt"``).
        trust: The ``TenantTrust`` this source's claims carry.

    Edge cases (the implementer contract — this docstring is the contract's
    only home; §D-S6-conformance deliberately ships no conformance suite):
        - ``resolve()`` **must never raise**. Any internal error must be
          caught by the implementation itself, or (as a last resort) will be
          caught by ``TenantSourceChain``, logged, and treated as "no claim".
        - ``resolve()`` returns ``None`` for "I have nothing to say" — never
          ``""``. An empty string is not a valid tenant id.
        - ``resolve()`` must never mutate the ``TenantRequest`` it receives
          (it is frozen, but a source must not, e.g., append to a mutable
          object reachable through it).
        - ``name`` and ``trust`` are ``ClassVar`` contract data, not instance
          state — a subclass that forgets them fails with ``AttributeError``
          the first time either is read, rather than silently defaulting.

    Thread safety: implementations should be stateless or hold only
        immutable configuration — a chain may share one source instance
        across concurrent requests.
    """

    name: ClassVar[str]
    trust: ClassVar[TenantTrust]

    @abstractmethod
    def resolve(self, request: TenantRequest) -> TenantClaim | None:
        """
        Return this source's claim for ``request``, or ``None``.

        Args:
            request: The transport-neutral request snapshot.

        Returns:
            A ``TenantClaim`` naming this source's ``trust``, or ``None``
            when this source has nothing to say about ``request``.

        Raises:
            Nothing — see the class *Edge cases* above. A subclass that
            raises degrades the chain but never the request.
        """
        raise NotImplementedError


class CrossCheckMode(StrEnum):
    """
    §D-S6-oq2 — the two cross-check strictness levels.

    ``LENIENT`` (the default): two present values that DISAGREE reject.
    Absence never rejects.

    ``STRICT``: additionally, if ANY source produced a value, at least TWO
    must agree. Zero claims is *always* a pass-through in both modes — this
    is what makes ``STRICT`` deployable without a path allowlist for
    ``/health``, ``/metrics``, and every unauthenticated public route.
    """

    LENIENT = "lenient"
    STRICT = "strict"


@dataclass(frozen=True)
class TenantSourceChain:
    """
    Runs an ordered set of ``TenantSource``s and returns a verdict.

    §D-S6-chain: ``resolve()`` returns a verdict, never raises. The winner is
    the highest-``trust`` claim among the survivors; a tie between
    equal-trust claims is broken by chain order. Rejection is a *field* on
    the returned ``TenantProvenance`` — turning it into an HTTP status is the
    adapter's job.

    Args:
        sources: Ordered ``TenantSource`` instances. Order only matters for
            tie-breaking among equal-trust claims.
        mode: The cross-check strictness (§D-S6-oq2). Defaults to
            ``CrossCheckMode.LENIENT``.
        min_trust: Claims below this floor are discarded before any
            cross-check logic runs — they never become a "conflict".

    Thread safety: ✅ Frozen; ``resolve()`` is a pure function of its
        arguments plus each source's own state.
    Async safety: ✅ Synchronous throughout — no ``await`` anywhere.
    """

    sources: tuple[TenantSource, ...] = field(default_factory=tuple)
    mode: CrossCheckMode = CrossCheckMode.LENIENT
    min_trust: TenantTrust = TenantTrust.LOW

    def resolve(self, request: TenantRequest) -> TenantProvenance:
        """
        Run every source and return the resulting verdict.

        Args:
            request: The transport-neutral request snapshot.

        Returns:
            A ``TenantProvenance``. Never raises — a misbehaving source is
            caught, logged at ERROR, and treated as "no claim" (§D-S6-abc).

        Edge cases:
            - Zero claims (nothing spoke, or everything was below
              ``min_trust``) always resolves to ``tenant_id=None``,
              ``rejected=False``, in both modes.
            - Two claims naming different tenant ids reject in both modes,
              regardless of trust.
            - ``STRICT`` additionally rejects when exactly one source spoke.
        """
        claims: list[TenantClaim] = []
        for src in self.sources:
            try:
                claim = src.resolve(request)
            except Exception:  # noqa: BLE001 — a source must never 500 a request
                logger.exception(
                    "TenantSource %r raised during resolve(); treating as no claim.",
                    getattr(src, "name", src.__class__.__name__),
                )
                continue
            if claim is None:
                continue
            if claim.trust < self.min_trust:
                continue
            claims.append(claim)

        if not claims:
            return TenantProvenance(
                tenant_id=None,
                winner=None,
                claims=(),
                conflict=None,
                mode=self.mode,
            )

        distinct_ids = {c.tenant_id for c in claims}
        if len(distinct_ids) > 1:
            # First disagreeing pair, in chain order — deterministic.
            first = claims[0]
            conflicting = next(c for c in claims[1:] if c.tenant_id != first.tenant_id)
            return TenantProvenance(
                tenant_id=None,
                winner=None,
                claims=tuple(claims),
                conflict=(first, conflicting),
                mode=self.mode,
            )

        if self.mode is CrossCheckMode.STRICT and len(claims) < 2:
            return TenantProvenance(
                tenant_id=None,
                winner=None,
                claims=tuple(claims),
                conflict=None,
                mode=self.mode,
            )

        # All surviving claims agree — winner is the highest-trust one;
        # a tie is broken by chain order (max() is stable and scans forward,
        # so the first max-trust claim in chain order wins).
        winner = max(claims, key=lambda c: c.trust)
        return TenantProvenance(
            tenant_id=winner.tenant_id,
            winner=winner,
            claims=tuple(claims),
            conflict=None,
            mode=self.mode,
        )


@dataclass(frozen=True)
class TenantProvenance:
    """
    The verdict of a ``TenantSourceChain.resolve()`` call.

    Args:
        tenant_id: The resolved tenant id, or ``None`` when nothing spoke or
            the request was rejected.
        winner: The claim that decided ``tenant_id``, or ``None``.
        claims: Every claim that survived the ``min_trust`` floor, in chain
            order — including in a rejected verdict, for audit purposes.
        conflict: The first disagreeing pair, or ``None``.
        mode: The ``CrossCheckMode`` this verdict was computed under.
        membership: Filled by Phase 3 (``varco_core.tenancy.membership``),
            never by ``TenantSourceChain.resolve()`` itself.
        delegation: Filled by Phase 6 (``varco_core.auth.delegation``), never
            by ``TenantSourceChain.resolve()`` itself.
    """

    tenant_id: str | None
    winner: TenantClaim | None
    claims: tuple[TenantClaim, ...]
    conflict: tuple[TenantClaim, TenantClaim] | None
    mode: CrossCheckMode
    membership: MembershipDecision | None = None
    delegation: DelegationRecord | None = None

    @property
    def rejected(self) -> bool:
        """``True`` when the chain rejected the request outright."""
        return self.rejection_reason is not None

    @property
    def rejection_reason(self) -> str | None:
        """
        A stable, opaque-safe rejection token, or ``None`` when not rejected.

        Never a formatted string containing a tenant id — the HTTP body
        built from this value must be safe to return verbatim to an
        untrusted caller.
        """
        if self.conflict is not None:
            return "conflict"
        if self.membership is not None and not self.membership.allowed:
            return "not_a_member"
        if self.delegation is not None and not self.delegation.allowed:
            return "delegation_denied"
        if self.tenant_id is None and self.mode is CrossCheckMode.STRICT and len(self.claims) == 1:
            return "insufficient_sources"
        return None
