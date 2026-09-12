"""
Tests for Plan 041 / S22, §D-S22-lifecycle — ``JwksRefreshLifecycle``
(``varco_fastapi.jwks``, also exported from ``varco_fastapi`` top level) and
its wiring via ``create_varco_app(jwks_refresh=...)``.

RED until ``varco_fastapi/varco_fastapi/jwks.py`` exists, is exported from
``varco_fastapi.__init__``, and ``create_varco_app`` gains the
``jwks_refresh=`` keyword.
"""

from __future__ import annotations

from fastapi.testclient import TestClient
from varco_core.authority.registry import TrustedIssuerRegistry
from varco_core.jwk.model import JsonWebKeySet
from varco_fastapi.app import create_varco_app


class _AlwaysRaisingSource:
    """A fake IssuerSource whose load() AND refresh() both always raise —
    used to prove JwksRefreshLifecycle.start() never calls load_all()."""

    @property
    def source_id(self) -> str:
        return "fake::always-raising"

    async def load(self) -> JsonWebKeySet:
        raise RuntimeError("simulated JWKS endpoint permanently down")

    async def refresh(self) -> JsonWebKeySet:
        raise RuntimeError("simulated JWKS endpoint permanently down")


class TestJwksRefreshLifecycleStartStop:
    async def test_start_starts_the_refresher_and_stop_stops_it(self) -> None:
        from varco_fastapi.jwks import JwksRefreshLifecycle

        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        lifecycle = JwksRefreshLifecycle(registry, interval=0.05)

        await lifecycle.start()
        try:
            assert registry.refresh_running is True
        finally:
            await lifecycle.stop()

        assert registry.refresh_running is False

    async def test_startup_and_shutdown_aliases_work(self) -> None:
        from varco_fastapi.jwks import JwksRefreshLifecycle

        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        lifecycle = JwksRefreshLifecycle(registry, interval=0.05)

        await lifecycle.startup()
        try:
            assert registry.refresh_running is True
        finally:
            await lifecycle.shutdown()

        assert registry.refresh_running is False


class TestJwksRefreshLifecycleNeverLoadsAll:
    async def test_start_never_calls_load_all_even_with_an_always_raising_source(
        self,
    ) -> None:
        from varco_fastapi.jwks import JwksRefreshLifecycle

        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        registry.register("BAD", "bad-iss", _AlwaysRaisingSource())  # type: ignore[arg-type]
        lifecycle = JwksRefreshLifecycle(registry, interval=0.05)

        # Must not raise — §D-S22-failure: start() never calls load_all(),
        # so a permanently-down source cannot fail startup.
        await lifecycle.start()
        try:
            assert registry.refresh_running is True
        finally:
            await lifecycle.stop()


class TestJwksRefreshLifecycleOffByDefault:
    async def test_interval_none_with_ttl_seconds_zero_starts_nothing(self) -> None:
        from varco_fastapi.jwks import JwksRefreshLifecycle

        registry = TrustedIssuerRegistry(ttl_seconds=0.0)
        lifecycle = JwksRefreshLifecycle(registry, interval=None)

        await lifecycle.start()
        try:
            assert registry.refresh_running is False
        finally:
            await lifecycle.stop()


class TestCreateVarcoAppJwksRefreshLifespan:
    def test_full_lifespan_cycle_leaves_no_pending_task(self) -> None:
        from varco_fastapi.jwks import JwksRefreshLifecycle

        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        lifecycle = JwksRefreshLifecycle(registry, interval=0.05)

        app = create_varco_app(
            enable_tracing=False,
            enable_metrics=False,
            enable_logging=False,
            enable_error_middleware=False,
            security_headers=False,
            body_limit=False,
            configure_jwt=False,
            validate=False,
            jwks_refresh=lifecycle,
        )

        with TestClient(app) as client:
            assert registry.refresh_running is True
            response = client.get("/healthz")
            assert response.status_code in (200, 404)  # route existence not the point here

        # After the lifespan's shutdown, the refresher must be stopped and
        # no task left pending.
        assert registry.refresh_running is False
