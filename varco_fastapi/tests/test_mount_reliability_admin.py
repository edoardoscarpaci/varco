"""
tests.test_mount_reliability_admin
====================================
Plan 014 / audit finding F4 — characterization tests for
``varco_fastapi.admin.mount.mount_reliability_admin`` written against
**unmodified** production code (Phase 1 of Plan 014).

Originally ``mount_reliability_admin`` had no double-mount guard (unlike
``mount_tenant_admin``, which already had one). Plan 014 step 16 ported the
same ``_MOUNTED_APPS`` guard onto this module; the two tests that pinned the
prior defect (marked ⟳ in git history) were inverted accordingly.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from varco_core.event.dlq import InMemoryDeadLetterQueue

from tests._admin_auth import ctx_auth

# Plan 054 / FLIP-3: server_auth is required; every mount below uses this stub.
_ADMIN_AUTH = ctx_auth(["reliability-admin", "cross-tenant-admin"])


@pytest.fixture(autouse=True)
def _clear_mounted_apps():
    """
    ``mount_reliability_admin`` mounts no ``_MOUNTED_APPS`` guard today, so
    this fixture is a no-op against unmodified production code — it exists
    so this file is ready, unmodified, for Plan 014 step 16 (which adds the
    module-level ``_MOUNTED_APPS: set[int]``), and so tests in this file
    never depend on `id()` values leaked from a previous test once that
    guard exists.
    """
    from varco_fastapi.admin import mount as mount_module

    mounted_apps = getattr(mount_module, "_MOUNTED_APPS", None)
    if mounted_apps is not None:
        mounted_apps.clear()
    yield
    mounted_apps = getattr(mount_module, "_MOUNTED_APPS", None)
    if mounted_apps is not None:
        mounted_apps.clear()


@pytest.fixture
def dlq() -> InMemoryDeadLetterQueue:
    """Same fixture shape as ``varco_fastapi/tests/test_dlq_router.py``."""
    return InMemoryDeadLetterQueue()


# ── working contract that must not regress ──────────────────────────────────


class TestWorkingContract:
    def test_mount_without_acknowledgement_raises_value_error(
        self, dlq: InMemoryDeadLetterQueue
    ) -> None:
        from varco_fastapi.admin.mount import mount_reliability_admin

        app = FastAPI()

        with pytest.raises(ValueError, match="acknowledge_bundled_admin"):
            mount_reliability_admin(app, dlq=dlq, server_auth=_ADMIN_AUTH)

    def test_mount_with_acknowledgement_exposes_dlq_route(
        self, dlq: InMemoryDeadLetterQueue
    ) -> None:
        from varco_fastapi.admin.mount import mount_reliability_admin

        app = FastAPI()

        mount_reliability_admin(
            app, dlq=dlq, acknowledge_bundled_admin=True, server_auth=_ADMIN_AUTH
        )

        client = TestClient(app)
        resp = client.get("/reliability/dlq/entries")
        assert resp.status_code != 404

    # The 3.x "server_auth=None logs exactly one UNAUTHENTICATED warning" test
    # is superseded by TestFlip3ServerAuthRequired (Plan 054 / FLIP-3): the
    # branch is gone, None now raises ValueError.


# ── double-mount guard (Plan 014 step 16 inverted the two ⟳ tests below) ───


class TestDoubleMountGuard:
    def test_double_mount_same_prefix_raises(self, dlq: InMemoryDeadLetterQueue) -> None:
        """
        Plan 014 step 9, inverted by step 16.

        Calling ``mount_reliability_admin`` twice with the same ``prefix``
        now raises ``ValueError`` on the second call instead of silently
        doubling the routes under ``/reliability/dlq``.
        """
        from varco_fastapi.admin.mount import mount_reliability_admin

        app = FastAPI()

        mount_reliability_admin(
            app, dlq=dlq, acknowledge_bundled_admin=True, server_auth=_ADMIN_AUTH
        )

        with pytest.raises(ValueError, match="already"):
            mount_reliability_admin(
                app, dlq=dlq, acknowledge_bundled_admin=True, server_auth=_ADMIN_AUTH
            )

    def test_double_mount_different_prefix_raises(self, dlq: InMemoryDeadLetterQueue) -> None:
        """
        Plan 014 step 10, inverted by step 16.

        A second mount with a *different* ``prefix`` also raises
        ``ValueError`` — the guard is per-app, not per-prefix.
        """
        from varco_fastapi.admin.mount import mount_reliability_admin

        app = FastAPI()

        mount_reliability_admin(
            app, dlq=dlq, acknowledge_bundled_admin=True, server_auth=_ADMIN_AUTH
        )

        with pytest.raises(ValueError, match="already"):
            mount_reliability_admin(
                app,
                dlq=dlq,
                acknowledge_bundled_admin=True,
                server_auth=_ADMIN_AUTH,
                prefix="/reliability2",
            )


# ── invariants the future guard must preserve ───────────────────────────────


class TestInvariants:
    def test_mounting_on_two_different_apps_both_succeed(
        self, dlq: InMemoryDeadLetterQueue
    ) -> None:
        from varco_fastapi.admin.mount import mount_reliability_admin

        app_a = FastAPI()
        app_b = FastAPI()

        mount_reliability_admin(
            app_a, dlq=dlq, acknowledge_bundled_admin=True, server_auth=_ADMIN_AUTH
        )
        mount_reliability_admin(
            app_b, dlq=dlq, acknowledge_bundled_admin=True, server_auth=_ADMIN_AUTH
        )

        client_a = TestClient(app_a)
        client_b = TestClient(app_b)
        assert client_a.get("/reliability/dlq/entries").status_code != 404
        assert client_b.get("/reliability/dlq/entries").status_code != 404

    def test_mount_nothing_does_not_poison_app_for_a_later_real_mount(
        self, dlq: InMemoryDeadLetterQueue
    ) -> None:
        """
        Mounting with neither ``audit_repo`` nor ``dlq`` mounts nothing and
        does not raise — and a subsequent real mount on that same app still
        succeeds. This is the deliberate deviation from
        ``mount_tenant_admin`` described in the plan's Design section; it
        must stay green both before and after step 16.
        """
        from varco_fastapi.admin.mount import mount_reliability_admin

        app = FastAPI()

        # Mounts nothing — must not raise.
        mount_reliability_admin(app, acknowledge_bundled_admin=True, server_auth=_ADMIN_AUTH)

        # A later real mount on the same app must still succeed.
        mount_reliability_admin(
            app, dlq=dlq, acknowledge_bundled_admin=True, server_auth=_ADMIN_AUTH
        )

        client = TestClient(app)
        assert client.get("/reliability/dlq/entries").status_code != 404


# ── Plan 054 / FLIP-3 — server_auth is a required keyword; None → ValueError ──


class _RolesAuth:
    """A ``server_auth`` stand-in resolving to a fixed set of roles."""

    def __init__(self, roles: list[str]) -> None:
        self._roles = roles

    def __call__(self):
        roles = self._roles

        class _Ctx:
            pass

        ctx = _Ctx()
        ctx.roles = roles  # type: ignore[attr-defined]
        return ctx


class TestFlip3ServerAuthRequired:
    def test_mount_reliability_admin_refuses_server_auth_none(
        self, dlq: InMemoryDeadLetterQueue
    ) -> None:
        from varco_fastapi.admin.mount import mount_reliability_admin

        app = FastAPI()
        routes_before = list(app.routes)

        with pytest.raises(ValueError, match="requires server_auth") as exc:
            mount_reliability_admin(app, dlq=dlq, acknowledge_bundled_admin=True, server_auth=None)
        # The refusal answers the legitimate gateway shape with a real strategy.
        assert "PassthroughAuth(acknowledge_unverified=True)" in str(exc.value)
        # Nothing mounted, and the double-mount WeakSet was not poisoned.
        assert list(app.routes) == routes_before
        mount_reliability_admin(
            app,
            dlq=dlq,
            acknowledge_bundled_admin=True,
            server_auth=_RolesAuth(["reliability-admin"]),
        )

    def test_mount_reliability_admin_requires_server_auth_keyword(
        self, dlq: InMemoryDeadLetterQueue
    ) -> None:
        import inspect

        from varco_fastapi.admin.mount import mount_reliability_admin

        param = inspect.signature(mount_reliability_admin).parameters["server_auth"]
        assert param.default is inspect.Parameter.empty
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        with pytest.raises(TypeError, match="server_auth"):
            mount_reliability_admin(FastAPI(), dlq=dlq, acknowledge_bundled_admin=True)

    def test_mount_reliability_admin_does_not_log_unauthenticated_warning(
        self, dlq: InMemoryDeadLetterQueue, caplog: pytest.LogCaptureFixture
    ) -> None:
        from varco_fastapi.admin.mount import mount_reliability_admin

        with caplog.at_level("WARNING"):
            mount_reliability_admin(
                FastAPI(),
                dlq=dlq,
                acknowledge_bundled_admin=True,
                server_auth=_RolesAuth(["reliability-admin"]),
            )

        assert not any("UNAUTHENTICATED" in r.getMessage() for r in caplog.records)

    def test_acknowledge_check_still_runs_first(self, dlq: InMemoryDeadLetterQueue) -> None:
        # Order preserved: the RD-9 acknowledgement is reported before the server_auth refusal.
        from varco_fastapi.admin.mount import mount_reliability_admin

        with pytest.raises(ValueError, match="acknowledge_bundled_admin") as exc:
            mount_reliability_admin(
                FastAPI(), dlq=dlq, acknowledge_bundled_admin=False, server_auth=None
            )
        assert "requires server_auth" not in str(exc.value)

    def test_mount_dependencies_are_additive(self, dlq: InMemoryDeadLetterQueue) -> None:
        # The built-in admin_role guard does not replace the caller's own dependencies=.
        from fastapi import Depends, HTTPException
        from varco_fastapi.admin.mount import mount_reliability_admin

        def deny_all() -> None:
            raise HTTPException(status_code=418, detail="denied by deny_all")

        app = FastAPI()
        mount_reliability_admin(
            app,
            dlq=dlq,
            acknowledge_bundled_admin=True,
            server_auth=_RolesAuth(["reliability-admin", "cross-tenant-admin"]),
            dependencies=[Depends(deny_all)],
        )

        resp = TestClient(app).get("/reliability/dlq/entries")
        assert resp.status_code == 418
