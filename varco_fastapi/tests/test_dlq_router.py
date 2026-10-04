"""
tests.test_dlq_router
=======================
Plan 009, Phase 10 (R6) — varco_fastapi.admin.dlq_router / mount.

RED until ``varco_fastapi/admin/dlq_router.py`` and ``admin/mount.py`` land.

Covers RD-9 (``mount_reliability_admin`` refuses to mount without
``acknowledge_bundled_admin=True``) and the capability-gap → 501 mapping
(RD-4) for a stream-shaped DLQ.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from varco_core.event import Event
from varco_core.event.dlq import DeadLetterEntry, InMemoryDeadLetterQueue

from tests._admin_auth import HeaderAuth, ctx_auth

# Plan 054 / FLIP-3: server_auth is required (and admin_role enforced on every
# route); the pre-054 tests below use this stub. No cross-tenant role, so the
# tenant-scoping behaviour they pin is unchanged.
_ADMIN_AUTH = ctx_auth(["reliability-admin"])


class SampleEvent(Event):
    __event_type__ = "test.dlq_router.sample"


class TestMountReliabilityAdminAcknowledgement:
    def test_mount_without_acknowledgement_raises_value_error(self) -> None:
        from varco_fastapi.admin.mount import mount_reliability_admin

        app = FastAPI()
        dlq = InMemoryDeadLetterQueue()

        with pytest.raises(ValueError, match="acknowledge_bundled_admin"):
            mount_reliability_admin(app, dlq=dlq, server_auth=_ADMIN_AUTH)

    def test_mount_with_acknowledgement_succeeds(self) -> None:
        from varco_fastapi.admin.mount import mount_reliability_admin

        app = FastAPI()
        dlq = InMemoryDeadLetterQueue()

        mount_reliability_admin(
            app, dlq=dlq, acknowledge_bundled_admin=True, server_auth=_ADMIN_AUTH
        )

        client = TestClient(app)
        resp = client.get("/reliability/dlq/entries")
        assert resp.status_code != 404


class TestDlqRouterCapabilityGap:
    def test_redrive_route_absent_without_redriver(self) -> None:
        """RD-4/DESIGN: an absent capability should not appear in the OpenAPI
        schema at all -- not surface as 501."""
        from varco_fastapi.admin.dlq_router import build_dlq_router

        router = build_dlq_router(InMemoryDeadLetterQueue(), redriver=None, server_auth=_ADMIN_AUTH)
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        resp = client.post(f"/dlq/entries/{uuid.uuid4()}/redrive")
        assert resp.status_code == 404

    async def test_stream_backend_single_entry_get_returns_501(self) -> None:
        from varco_core.event.dlq import AbstractDeadLetterQueue
        from varco_fastapi.admin.dlq_router import build_dlq_router

        class _StreamDLQ(AbstractDeadLetterQueue):
            supports_random_access = False

            async def push(self, entry: DeadLetterEntry) -> None: ...
            async def pop_batch(self, *, limit: int = 10) -> list[DeadLetterEntry]:
                return []

            async def ack(self, entry_id) -> None: ...
            async def count(self) -> int:
                return 0

            async def get(self, entry_id):
                raise NotImplementedError("stream-backed DLQ has no random access")

        router = build_dlq_router(_StreamDLQ(), server_auth=_ADMIN_AUTH)
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        resp = client.get(f"/dlq/entries/{uuid.uuid4()}")
        assert resp.status_code == 501


class TestDlqRouterStats:
    async def test_stats_returns_count(self) -> None:
        from varco_fastapi.admin.dlq_router import build_dlq_router

        dlq = InMemoryDeadLetterQueue()
        await dlq.push(
            DeadLetterEntry(
                event=SampleEvent(),
                channel="orders",
                handler_name="H.h",
                error_type="E",
                error_message="msg",
                attempts=1,
            )
        )
        router = build_dlq_router(dlq, server_auth=_ADMIN_AUTH)
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        resp = client.get("/dlq/stats")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1


# ── Plan 054 / FLIP-3 (§D-flip3, §D-flip3-enforce) — required server_auth + per-route admin_role ──


# The Request-taking stub lives in the shared helper (Plan 054 Step 8).
_HeaderAuth = HeaderAuth


def _route_cases(router) -> list[tuple[str, str]]:
    """Every (method, concrete path) of *router* — built from ``router.routes``
    so a newly added route cannot dodge the guard."""
    import re as _re
    import uuid as _uuid

    from fastapi.routing import APIRoute

    cases: list[tuple[str, str]] = []
    for route in router.routes:
        if not isinstance(route, APIRoute):
            continue
        path = _re.sub(
            r"\{([a-z_]+)\}",
            lambda m: str(_uuid.UUID(int=1)) if m.group(1).endswith("id") else "x",
            route.path,
        )
        for method in sorted(route.methods):
            cases.append((method, path))
    return cases


_ADMIN_HEADERS = {"X-Test-Auth": "reliability-admin,cross-tenant-admin"}


def _full_dlq_router(server_auth):
    from varco_core.event import InMemoryEventBus
    from varco_core.event.redrive import DlqRedriver
    from varco_fastapi.admin.dlq_router import build_dlq_router

    dlq = InMemoryDeadLetterQueue()
    redriver = DlqRedriver(dlq, InMemoryEventBus())
    return build_dlq_router(dlq, redriver=redriver, server_auth=server_auth)


def _dlq_cases() -> list[tuple[str, str]]:
    return _route_cases(_full_dlq_router(_HeaderAuth()))


class TestFlip3DlqRouterServerAuthRequired:
    def test_build_dlq_router_refuses_server_auth_none(self) -> None:
        from varco_fastapi.admin.dlq_router import build_dlq_router

        with pytest.raises(ValueError, match="requires server_auth") as exc:
            build_dlq_router(InMemoryDeadLetterQueue(), server_auth=None)
        assert "build_dlq_router" in str(exc.value)

    def test_build_dlq_router_requires_server_auth_keyword(self) -> None:
        import inspect

        from varco_fastapi.admin.dlq_router import build_dlq_router

        param = inspect.signature(build_dlq_router).parameters["server_auth"]
        assert param.default is inspect.Parameter.empty
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        with pytest.raises(TypeError, match="server_auth"):
            build_dlq_router(InMemoryDeadLetterQueue())


class TestFlip3DlqRouterEnforcesAdminRoleOnEveryRoute:
    @pytest.mark.parametrize(("method", "path"), _dlq_cases())
    def test_no_credentials_is_server_auth_401(self, method: str, path: str) -> None:
        app = FastAPI()
        app.include_router(_full_dlq_router(_HeaderAuth()))

        resp = TestClient(app).request(method, path)

        assert resp.status_code == 401

    @pytest.mark.parametrize(("method", "path"), _dlq_cases())
    def test_credentials_without_role_is_403(self, method: str, path: str) -> None:
        app = FastAPI()
        app.include_router(_full_dlq_router(_HeaderAuth()))

        resp = TestClient(app).request(method, path, headers={"X-Test-Auth": "someone"})

        assert resp.status_code == 403
        assert resp.json()["detail"] == "'reliability-admin' role required."

    @pytest.mark.parametrize(("method", "path"), _dlq_cases())
    def test_with_role_passes_the_guard(self, method: str, path: str) -> None:
        from varco_core.service.tenant import tenant_context

        app = FastAPI()
        app.include_router(_full_dlq_router(_HeaderAuth()))

        with tenant_context("t1"):
            resp = TestClient(app).request(method, path, headers=_ADMIN_HEADERS)

        assert resp.status_code not in (401, 403)

    def test_server_auth_resolved_once_per_request(self) -> None:
        # The router guard stashes ctx on request.state; per-route reads must not re-call it.
        from varco_core.service.tenant import tenant_context

        auth = _HeaderAuth()
        app = FastAPI()
        app.include_router(_full_dlq_router(auth))

        with tenant_context("t1"):
            resp = TestClient(app).get("/dlq/entries", headers=_ADMIN_HEADERS)

        assert resp.status_code == 200
        assert auth.calls == 1
