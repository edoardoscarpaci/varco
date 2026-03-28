"""
varco_core.health
=================
Liveness and readiness probe abstractions for varco backends.

``HealthStatus``
    Enum of possible component health states: HEALTHY, DEGRADED, UNHEALTHY.

``HealthResult``
    Immutable value object returned by every ``HealthCheck.check()`` call.
    Contains the status, component name, optional latency measurement, and
    a human-readable detail string for non-healthy states.

``HealthCheck``
    Abstract base class for liveness/readiness probes.  Every backend package
    (varco_kafka, varco_redis, varco_sa, varco_beanie) ships a concrete
    subclass that tests real connectivity to its respective broker.

``CompositeHealthCheck``
    Aggregates multiple ``HealthCheck`` instances.  Runs all probes concurrently
    via ``asyncio.gather`` and reduces results to a single worst-case status:
    UNHEALTHY > DEGRADED > HEALTHY.

Design
------
DESIGN: ABC over Protocol
    ✅ All health checks are explicitly wired in DI — structural duck-typing
       provides no benefit here.  An ABC gives us abstract method enforcement
       and a shared ``name`` property contract without extra machinery.
    ✅ ``HealthCheck`` is a thin, library-agnostic interface — backends depend
       only on varco_core, never on each other.
    ❌ Protocol would allow injecting mock objects without subclassing — not
       needed here since tests always subclass or monkeypatch.

DESIGN: Never raise inside check()
    ✅ A probe that raises crashes the health endpoint instead of reporting
       UNHEALTHY — unhelpful to operators and hard to distinguish from a bug.
    ✅ Consistent with ``AbstractDeadLetterQueue.push()`` which also must never
       raise — both are infrastructure "best effort" operations.
    ❌ Errors are swallowed rather than propagated — callers must inspect
       ``HealthResult.status`` and ``HealthResult.detail`` to detect failures.

Thread safety:  ✅ HealthResult and HealthStatus are immutable; no shared state.
Async safety:   ✅ check() is async def; CompositeHealthCheck uses gather().

📚 Docs
- 🐍 https://docs.python.org/3/library/enum.html — StrEnum vs str+Enum pattern
- 🐍 https://docs.python.org/3/library/dataclasses.html#frozen-instances
  frozen=True — immutable dataclass, hashable, safe to cache
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.gather
  asyncio.gather — concurrent execution of independent coroutines
"""

from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar


# ── HealthStatus ──────────────────────────────────────────────────────────────

# Module-level severity map.  Defined OUTSIDE the Enum class because Python's
# Enum metaclass processes all class-body assignments — even ClassVar-annotated
# ones — and may coerce dict values to strings when the class extends str.
# Using a module-level constant sidesteps this entirely.
_HEALTH_SEVERITY: dict[str, int] = {
    "healthy": 0,
    "degraded": 1,
    "unhealthy": 2,
}


class HealthStatus(str, Enum):
    """
    Enumeration of possible component health states.

    Ordered by severity: HEALTHY < DEGRADED < UNHEALTHY.
    ``severity`` delegates to the module-level ``_HEALTH_SEVERITY`` map so
    ``CompositeHealthCheck`` can reduce a list of statuses to the worst-case
    value without a chain of if/elif statements.

    Edge cases:
        - A DEGRADED status indicates the component is functional but
          operating below normal parameters (e.g. high latency, partial
          connectivity).  Readiness probes should typically reject DEGRADED.
        - UNHEALTHY indicates the component cannot serve requests and should
          be removed from the load balancer rotation.
    """

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

    @property
    def severity(self) -> int:
        """
        Return a numeric severity where higher means worse.

        Returns:
            0 for HEALTHY, 1 for DEGRADED, 2 for UNHEALTHY.
        """
        # Delegate to the module-level map — avoids ClassVar-in-Enum pitfalls.
        return _HEALTH_SEVERITY[self.value]


# ── HealthResult ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class HealthResult:
    """
    Immutable snapshot of a single health probe execution.

    Frozen dataclass — safe to cache, return from coroutines, and compare
    for equality in tests.

    Attributes:
        status:      Overall health of the component.
        component:   Human-readable component name (e.g. "kafka", "redis").
        latency_ms:  Round-trip probe latency in milliseconds, or ``None``
                     if the probe could not complete (e.g. timeout).
        detail:      Human-readable explanation for non-HEALTHY results.
                     ``None`` when status is HEALTHY (no extra context needed).

    Edge cases:
        - ``latency_ms`` is ``None`` for UNHEALTHY results caused by a timeout
          or connection error — the probe never finished, so no latency was
          recorded.
        - ``detail`` may be set on HEALTHY results if the probe wants to include
          informational context (e.g. "connected to 3/3 brokers").
    """

    status: HealthStatus
    component: str
    latency_ms: float | None = None
    detail: str | None = None

    def __repr__(self) -> str:
        parts = [f"status={self.status.value!r}", f"component={self.component!r}"]
        if self.latency_ms is not None:
            parts.append(f"latency_ms={self.latency_ms:.1f}")
        if self.detail is not None:
            parts.append(f"detail={self.detail!r}")
        return f"HealthResult({', '.join(parts)})"


# ── HealthCheck ABC ───────────────────────────────────────────────────────────


class HealthCheck(abc.ABC):
    """
    Abstract base class for liveness and readiness probes.

    Each backend package (varco_kafka, varco_redis, varco_sa, varco_beanie)
    provides a concrete subclass that issues a lightweight probe against its
    respective broker/database.

    Contract
    --------
    ``check()`` MUST NEVER raise.  Any error — connection refused, timeout,
    unexpected exception — must be caught and returned as a ``HealthResult``
    with ``status=HealthStatus.UNHEALTHY``.  This ensures that a single
    unhealthy backend cannot crash the health endpoint that aggregates probes.

    Thread safety:  ✅ Subclasses must document their own safety contract.
    Async safety:   ✅ check() is always async def.

    Edge cases:
        - Implementations should honour ``_timeout`` to avoid blocking the
          event loop indefinitely on a hung connection.
        - ``check()`` may be called concurrently from multiple tasks — backends
          must not share mutable state between calls.
    """

    # Default probe timeout in seconds.  Subclasses override at construction.
    # 5 s is generous enough for slow networks but tight enough for k8s probes.
    _default_timeout: ClassVar[float] = 5.0

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """
        Human-readable component name used in ``HealthResult.component``.

        Returns:
            A short, stable identifier string (e.g. ``"kafka"``, ``"redis"``).
        """

    @abc.abstractmethod
    async def check(self) -> HealthResult:
        """
        Execute a lightweight connectivity probe and return its result.

        Implementations must:
        - Catch ALL exceptions — never let them propagate to the caller.
        - Measure latency with ``time.monotonic()`` before/after the probe.
        - Respect the configured timeout via ``asyncio.wait_for``.

        Returns:
            A ``HealthResult`` describing the outcome.  Never raises.
        """


# ── CompositeHealthCheck ──────────────────────────────────────────────────────


class CompositeHealthCheck(HealthCheck):
    """
    Aggregates multiple ``HealthCheck`` instances into a single probe.

    All child probes are executed **concurrently** via ``asyncio.gather``.
    The aggregate ``status`` is the worst-case status across all probes:
    UNHEALTHY > DEGRADED > HEALTHY.

    DESIGN: asyncio.gather over TaskGroup
        ✅ ``gather`` is available on Python 3.10+; TaskGroup requires 3.11+.
           varco_core targets 3.10+ to stay broadly compatible.
        ✅ ``return_exceptions=True`` ensures one failing probe cannot cancel
           the others — all results are collected regardless.
        ❌ Exception objects in results require an isinstance check — handled
           internally in ``check_all()``.

    Thread safety:  ✅ No shared mutable state.
    Async safety:   ✅ All probes run concurrently; results are independent.

    Edge cases:
        - Empty ``checks`` list → returns HEALTHY with detail "no checks registered".
        - If a probe raises despite the contract (bug), ``check_all()`` wraps
          the exception as UNHEALTHY rather than propagating it.
    """

    def __init__(self, *checks: HealthCheck, name: str = "composite") -> None:
        """
        Initialise the composite probe.

        Args:
            *checks: Zero or more ``HealthCheck`` instances to aggregate.
            name:    Human-readable name for this composite (default: "composite").

        Edge cases:
            - Zero checks is allowed — returns HEALTHY with an informational note.
        """
        self._checks = checks
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def check_all(self) -> list[HealthResult]:
        """
        Execute all child probes concurrently and return every result.

        Individual probes that violate the "never raise" contract and raise
        an exception are caught here and returned as UNHEALTHY results so
        that one buggy probe cannot suppress results from the others.

        Returns:
            A list of ``HealthResult`` objects, one per registered check,
            in the same order as ``checks`` was provided.

        Async safety:  ✅ Uses asyncio.gather — all probes run concurrently.
        """
        if not self._checks:
            # Edge case: no registered checks — return a trivially healthy result
            # rather than confusing operators with an empty list.
            return [
                HealthResult(
                    HealthStatus.HEALTHY, self._name, detail="no checks registered"
                )
            ]

        # Run all probes concurrently.  return_exceptions=True ensures that if a
        # probe breaks the contract and raises, we collect the exception object
        # rather than cancelling sibling probes.
        raw_results = await asyncio.gather(
            *(c.check() for c in self._checks),
            return_exceptions=True,
        )

        results: list[HealthResult] = []
        for check, raw in zip(self._checks, raw_results):
            if isinstance(raw, HealthResult):
                results.append(raw)
            else:
                # The probe violated the "never raise" contract — wrap the
                # exception as UNHEALTHY so the composite still returns a result.
                results.append(
                    HealthResult(
                        HealthStatus.UNHEALTHY,
                        check.name,
                        detail=f"probe raised unexpectedly: {raw!r}",
                    )
                )
        return results

    async def check(self) -> HealthResult:
        """
        Execute all probes and return a single aggregate result.

        Status is the worst-case status across all child probes:
        UNHEALTHY > DEGRADED > HEALTHY.

        Returns:
            A ``HealthResult`` with the worst observed status and a summary
            detail line listing each component's status.

        Async safety:  ✅ Delegates to check_all() — all probes run concurrently.
        """
        results = await self.check_all()

        # Pick the worst status by numeric severity.
        worst = max(results, key=lambda r: r.status.severity)

        # Build a summary detail line so operators can see all component statuses
        # without needing to call check_all() separately.
        summary = ", ".join(f"{r.component}={r.status.value}" for r in results)

        return HealthResult(
            status=worst.status,
            component=self._name,
            # Latency is not meaningful for a composite — omit it.
            latency_ms=None,
            detail=summary,
        )


__all__ = [
    "CompositeHealthCheck",
    "HealthCheck",
    "HealthResult",
    "HealthStatus",
]
