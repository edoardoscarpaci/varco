"""
Red-mode integration tests for Plan 035 / Phase 5, Step 26 (S10) —
``RateLimitMiddleware`` backed by a real ``RedisRateLimiter``.

Namespaced with ``uuid4().hex[:8]`` per the shared-container rule (this
fixture is session-scoped and shared across the whole test session).

``varco_redis`` is imported ONLY inside this test module — it must never
appear in ``varco_fastapi``'s ``[project.dependencies]``, and
``test_layer_boundaries.py``'s ``FORBIDDEN_MODULES`` is unchanged.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from varco_core.resilience.rate_limit import RateLimitConfig
from varco_core.service.tenant import tenant_context
from varco_fastapi.middleware.error import ErrorMiddleware

pytest.importorskip("varco_redis")

from varco_fastapi.middleware.rate_limit import (  # type: ignore[attr-defined]  # noqa: E402
    RateLimitMiddleware,
    RateLimitRule,
    RateLimitScope,
    RateLimitStage,
)

pytestmark = pytest.mark.integration


def _make_app(limiter, tenant: str) -> FastAPI:
    from varco_core.service.tenant import current_tenant  # noqa: F401

    rule = RateLimitRule(scope=RateLimitScope.TENANT, limiter=limiter, name="t")
    app = FastAPI()
    # §D-S10-shape: TENANT is POST_AUTH-only (RateLimitMiddleware.__init__
    # refuses TENANT/SUBJECT at PRE_AUTH). This test drives current_tenant()
    # directly via tenant_context(), not through RequestContextMiddleware, so
    # the stage otherwise has no bearing on this test's mechanics.
    app.add_middleware(RateLimitMiddleware, rules=(rule,), stage=RateLimitStage.POST_AUTH)
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    return app


async def test_two_middleware_instances_share_budget_via_redis(redis_url: str) -> None:
    from varco_redis.config import RedisEventBusSettings
    from varco_redis.rate_limit import RedisRateLimiter

    run_id = uuid.uuid4().hex[:8]
    tenant = f"tenant-{run_id}"
    settings = RedisEventBusSettings(url=redis_url)
    config = RateLimitConfig(rate=1, period=5.0)

    limiter_a = RedisRateLimiter(config, settings=settings)
    limiter_b = RedisRateLimiter(config, settings=settings)
    async with limiter_a, limiter_b:
        app_a = _make_app(limiter_a, tenant)
        app_b = _make_app(limiter_b, tenant)

        transport_a = ASGITransport(app=app_a)
        transport_b = ASGITransport(app=app_b)

        with tenant_context(tenant):
            async with AsyncClient(transport=transport_a, base_url="http://test") as client_a:
                r1 = await client_a.get("/ok")
            async with AsyncClient(transport=transport_b, base_url="http://test") as client_b:
                r2 = await client_b.get("/ok")

        assert r1.status_code == 200
        # Independently constructed middleware/limiter instance -- only a
        # real shared backend (not InMemoryRateLimiter) can enforce this.
        assert r2.status_code == 429
        assert "Retry-After" in r2.headers
        assert int(r2.headers["Retry-After"]) >= 1


async def test_request_succeeds_again_after_period_elapses(redis_url: str) -> None:
    from varco_redis.config import RedisEventBusSettings
    from varco_redis.rate_limit import RedisRateLimiter

    run_id = uuid.uuid4().hex[:8]
    tenant = f"tenant-{run_id}"
    settings = RedisEventBusSettings(url=redis_url)
    config = RateLimitConfig(rate=1, period=1.0)

    limiter = RedisRateLimiter(config, settings=settings)
    async with limiter:
        app = _make_app(limiter, tenant)
        transport = ASGITransport(app=app)

        with tenant_context(tenant):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                r1 = await client.get("/ok")
                r2 = await client.get("/ok")
                await asyncio.sleep(1.2)
                r3 = await client.get("/ok")

        assert r1.status_code == 200
        assert r2.status_code == 429
        assert r3.status_code == 200
