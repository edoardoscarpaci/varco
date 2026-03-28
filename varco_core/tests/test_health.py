"""
Unit tests for varco_core.health
=================================
Covers HealthStatus, HealthResult, HealthCheck ABC, and CompositeHealthCheck
without any real network connections.

Sections
--------
- ``HealthStatus``             — severity ordering, string values
- ``HealthResult``             — frozen dataclass, repr
- ``CompositeHealthCheck``     — concurrent execution, worst-case aggregate,
                                  empty-checks guard, probe-that-raises guard
"""

from __future__ import annotations

import pytest

from varco_core.health import (
    CompositeHealthCheck,
    HealthCheck,
    HealthResult,
    HealthStatus,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _result(status: HealthStatus, component: str = "test") -> HealthResult:
    """Build a minimal HealthResult for assertions."""
    return HealthResult(status=status, component=component)


class _FixedCheck(HealthCheck):
    """
    Stub HealthCheck that always returns a pre-configured HealthResult.

    Used to inject deterministic outcomes into CompositeHealthCheck tests
    without any real I/O.
    """

    def __init__(self, result: HealthResult) -> None:
        self._result = result

    @property
    def name(self) -> str:
        return self._result.component

    async def check(self) -> HealthResult:
        return self._result


class _RaisingCheck(HealthCheck):
    """
    Stub HealthCheck that violates the "never raise" contract.

    Used to verify that CompositeHealthCheck wraps rogue probes instead of
    propagating the exception to the caller.
    """

    @property
    def name(self) -> str:
        return "raiser"

    async def check(self) -> HealthResult:  # type: ignore[return]  # intentional
        raise RuntimeError("probe bug")


# ── HealthStatus ───────────────────────────────────────────────────────────────


class TestHealthStatus:
    def test_string_values(self) -> None:
        # HealthStatus inherits from str — values are usable wherever str is expected.
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"

    def test_severity_ordering(self) -> None:
        # CompositeHealthCheck depends on severity ordering being strictly
        # HEALTHY < DEGRADED < UNHEALTHY.
        assert HealthStatus.HEALTHY.severity < HealthStatus.DEGRADED.severity
        assert HealthStatus.DEGRADED.severity < HealthStatus.UNHEALTHY.severity

    def test_severity_values(self) -> None:
        assert HealthStatus.HEALTHY.severity == 0
        assert HealthStatus.DEGRADED.severity == 1
        assert HealthStatus.UNHEALTHY.severity == 2

    def test_str_comparison(self) -> None:
        # Since HealthStatus extends str, it can be compared directly to strings.
        assert HealthStatus.HEALTHY == "healthy"
        assert HealthStatus.UNHEALTHY == "unhealthy"


# ── HealthResult ───────────────────────────────────────────────────────────────


class TestHealthResult:
    def test_required_fields(self) -> None:
        r = HealthResult(status=HealthStatus.HEALTHY, component="db")
        assert r.status is HealthStatus.HEALTHY
        assert r.component == "db"
        assert r.latency_ms is None
        assert r.detail is None

    def test_optional_fields(self) -> None:
        r = HealthResult(
            status=HealthStatus.UNHEALTHY,
            component="kafka",
            latency_ms=None,
            detail="connection refused",
        )
        assert r.detail == "connection refused"

    def test_frozen(self) -> None:
        # HealthResult must be immutable — mutations raise FrozenInstanceError.
        r = HealthResult(status=HealthStatus.HEALTHY, component="x")
        with pytest.raises(Exception):
            r.status = HealthStatus.UNHEALTHY  # type: ignore[misc]

    def test_latency_set(self) -> None:
        r = HealthResult(
            status=HealthStatus.HEALTHY,
            component="redis",
            latency_ms=3.7,
        )
        assert r.latency_ms == pytest.approx(3.7)

    def test_repr_healthy(self) -> None:
        r = HealthResult(status=HealthStatus.HEALTHY, component="pg", latency_ms=2.0)
        text = repr(r)
        assert "healthy" in text
        assert "pg" in text
        assert "2.0" in text

    def test_repr_unhealthy_with_detail(self) -> None:
        r = HealthResult(
            status=HealthStatus.UNHEALTHY,
            component="kafka",
            detail="timeout",
        )
        text = repr(r)
        assert "unhealthy" in text
        assert "timeout" in text

    def test_repr_no_latency_omits_field(self) -> None:
        r = HealthResult(status=HealthStatus.UNHEALTHY, component="x")
        # latency_ms is None — should not appear in repr to keep it clean
        assert "latency_ms" not in repr(r)

    def test_equality(self) -> None:
        # Frozen dataclasses implement __eq__ by value.
        r1 = HealthResult(HealthStatus.HEALTHY, "db", latency_ms=1.0)
        r2 = HealthResult(HealthStatus.HEALTHY, "db", latency_ms=1.0)
        assert r1 == r2

    def test_hashable(self) -> None:
        # Frozen dataclasses are hashable — safe to put in sets or dict keys.
        r = HealthResult(HealthStatus.HEALTHY, "db")
        assert hash(r) is not None
        {r}  # must not raise


# ── CompositeHealthCheck ───────────────────────────────────────────────────────


class TestCompositeHealthCheck:
    async def test_all_healthy_returns_healthy(self) -> None:
        composite = CompositeHealthCheck(
            _FixedCheck(_result(HealthStatus.HEALTHY, "a")),
            _FixedCheck(_result(HealthStatus.HEALTHY, "b")),
        )
        result = await composite.check()
        assert result.status is HealthStatus.HEALTHY

    async def test_one_unhealthy_returns_unhealthy(self) -> None:
        composite = CompositeHealthCheck(
            _FixedCheck(_result(HealthStatus.HEALTHY, "a")),
            _FixedCheck(_result(HealthStatus.UNHEALTHY, "b")),
        )
        result = await composite.check()
        assert result.status is HealthStatus.UNHEALTHY

    async def test_degraded_worst_when_no_unhealthy(self) -> None:
        composite = CompositeHealthCheck(
            _FixedCheck(_result(HealthStatus.HEALTHY, "a")),
            _FixedCheck(_result(HealthStatus.DEGRADED, "b")),
        )
        result = await composite.check()
        assert result.status is HealthStatus.DEGRADED

    async def test_unhealthy_beats_degraded(self) -> None:
        composite = CompositeHealthCheck(
            _FixedCheck(_result(HealthStatus.DEGRADED, "a")),
            _FixedCheck(_result(HealthStatus.UNHEALTHY, "b")),
        )
        result = await composite.check()
        assert result.status is HealthStatus.UNHEALTHY

    async def test_check_all_returns_all_results(self) -> None:
        composite = CompositeHealthCheck(
            _FixedCheck(_result(HealthStatus.HEALTHY, "a")),
            _FixedCheck(_result(HealthStatus.UNHEALTHY, "b")),
        )
        results = await composite.check_all()
        assert len(results) == 2
        statuses = {r.component: r.status for r in results}
        assert statuses["a"] is HealthStatus.HEALTHY
        assert statuses["b"] is HealthStatus.UNHEALTHY

    async def test_component_names_in_detail(self) -> None:
        composite = CompositeHealthCheck(
            _FixedCheck(_result(HealthStatus.HEALTHY, "kafka")),
            _FixedCheck(_result(HealthStatus.UNHEALTHY, "redis")),
            name="infra",
        )
        result = await composite.check()
        # The summary detail should mention both component names and statuses.
        assert "kafka" in (result.detail or "")
        assert "redis" in (result.detail or "")

    async def test_empty_checks_returns_healthy(self) -> None:
        # An empty composite should not raise or return UNHEALTHY — it simply
        # has nothing to fail.  Matches the IndexDriftReport(empty=clean) design.
        composite = CompositeHealthCheck()
        result = await composite.check()
        assert result.status is HealthStatus.HEALTHY

    async def test_check_all_empty_returns_single_healthy(self) -> None:
        composite = CompositeHealthCheck()
        results = await composite.check_all()
        assert len(results) == 1
        assert results[0].status is HealthStatus.HEALTHY

    async def test_rogue_probe_does_not_propagate(self) -> None:
        # A probe that violates the "never raise" contract must not crash the
        # composite — the exception is wrapped as UNHEALTHY.
        composite = CompositeHealthCheck(
            _FixedCheck(_result(HealthStatus.HEALTHY, "a")),
            _RaisingCheck(),
        )
        result = await composite.check()
        # The HEALTHY probe still ran; the rogue probe returns UNHEALTHY.
        assert result.status is HealthStatus.UNHEALTHY

    async def test_rogue_probe_wrapped_in_check_all(self) -> None:
        composite = CompositeHealthCheck(_RaisingCheck())
        results = await composite.check_all()
        assert len(results) == 1
        assert results[0].status is HealthStatus.UNHEALTHY
        # The wrapped detail should mention that the probe raised.
        assert "probe raised" in (results[0].detail or "")

    async def test_custom_name(self) -> None:
        composite = CompositeHealthCheck(name="my-cluster")
        result = await composite.check()
        assert result.component == "my-cluster"

    async def test_no_latency_in_aggregate(self) -> None:
        # CompositeHealthCheck does not compute a meaningful aggregate latency
        # (probes run in parallel with different durations).
        composite = CompositeHealthCheck(
            _FixedCheck(_result(HealthStatus.HEALTHY, "a"))
        )
        result = await composite.check()
        assert result.latency_ms is None

    async def test_three_checks_mixed(self) -> None:
        composite = CompositeHealthCheck(
            _FixedCheck(_result(HealthStatus.HEALTHY, "a")),
            _FixedCheck(_result(HealthStatus.DEGRADED, "b")),
            _FixedCheck(_result(HealthStatus.UNHEALTHY, "c")),
        )
        results = await composite.check_all()
        assert len(results) == 3
        result = await composite.check()
        assert result.status is HealthStatus.UNHEALTHY
