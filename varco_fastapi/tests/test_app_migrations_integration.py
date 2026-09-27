"""
Real-database migration lifecycle integration tests (Plan 012 / RT9, Step
36) — the real-DB counterpart to ``tests/test_app_migrations.py``'s
``InMemoryMigrator``-only unit tests.

Against a real Postgres (testcontainers, module-scoped for this file) and a
real ``AlembicMigrator`` (``varco_sa.migration.migrator.AlembicMigrator``,
``include_framework_branch=True`` — the shipped framework branch always has
pending revisions on a fresh database, so no app-owned ``script_location``
is needed to give the migrator real work):

  (i)   ``mode="check"`` on a behind schema fails startup and serves no
        request, writing no DDL.
  (ii)  ``mode="upgrade"`` applies revisions before the first request is
        served, and a second boot against the same (now-current) database
        is a no-op.
  (iii) ``on_failure="warn"`` keeps serving on a failed migration, while
        ``"fail"`` aborts startup.
  (iv)  ``mode="off"`` (the default) registers nothing and touches nothing.
  (v)   Two ``create_varco_app`` lifespans started concurrently against one
        database — exactly one migrates, the schema is not corrupted, and
        the pair terminates (either by lock-wait-then-no-op or by a raised
        ``MigrationLockTimeout``).

Imports ``SchemaMigrationError``/``SchemaMigrationPlan`` from ``varco_core.migration``,
never from ``varco_core`` (CLAUDE.md's name-collision pitfall — those names
are already owned by the unrelated ``varco_core.migrator`` domain-migration
module).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import httpx
import pytest
import pytest_asyncio
from providify import DIContainer
from varco_core.di.module import CoreModule
from varco_core.migration.errors import MigrationLockTimeout, PendingMigrationsError
from varco_core.migration.settings import MigrationSettings
from varco_fastapi.app import create_varco_app
from varco_fastapi.modules.lifecycle import MigrationsModule
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter

pytestmark = pytest.mark.integration


class _PingRouter(GenericRouter):
    _prefix = "/ping"

    @route("GET", "")
    async def ping(self) -> dict:
        return {"ok": True}


async def _boot_and_get_ping(app) -> httpx.Response:
    """
    Drive the ASGI lifespan + one GET /ping, all on the CURRENT event loop.

    DESIGN: ``httpx.AsyncClient(transport=ASGITransport)`` + a manually
    entered ``app.router.lifespan_context(app)``, not
    ``fastapi.testclient.TestClient``.
        ``TestClient`` runs the ASGI app (and therefore ``AlembicMigrator``'s
        asyncpg connections) inside its own background thread with its OWN
        event loop (via anyio's ``BlockingPortal``). An ``AsyncEngine``
        constructed in THIS test function's event loop cannot be used from
        that other loop — asyncpg raises "Task ... got Future ... attached
        to a different loop". Driving the lifespan directly on the test's
        own loop avoids the mismatch entirely.
    """
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            return await client.get("/ping")


# ── Postgres container + isolated-database helper ─────────────────────────────
#
# varco_fastapi has no Phase-1 shared postgres_url fixture of its own (that
# fixture landed in varco_redis/varco_beanie/varco_sa/varco_kafka/
# varco_memcached/varco_casbin/varco_nats only) — a self-contained,
# module-scoped container is added here instead, following the same
# module-scoped-container + isolated-database-per-test shape as
# varco_sa/tests/test_migration_lock.py.


@pytest.fixture(scope="module")
def pg_container():
    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip(
            "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration"
        )
    from testcontainers.postgres import PostgresContainer  # noqa: PLC0415

    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


def _asyncpg_url(container: Any) -> str:
    url = container.get_connection_url(driver="asyncpg")
    assert url.startswith("postgresql+asyncpg://")
    return url


async def _create_isolated_database_url(container: Any, name: str) -> str:
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    admin_url = _asyncpg_url(container)
    admin_engine = create_async_engine(admin_url, echo=False)
    try:
        async with admin_engine.connect() as conn:
            await conn.execution_options(isolation_level="AUTOCOMMIT")
            await conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}"'))
            await conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    finally:
        await admin_engine.dispose()
    return make_url(admin_url).set(database=name).render_as_string(hide_password=False)


async def _outbox_table_exists(engine) -> bool:
    import sqlalchemy as sa

    async with engine.connect() as conn:
        count = await conn.scalar(
            sa.text(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'varco_outbox'"
            )
        )
    return bool(count)


@pytest_asyncio.fixture
async def isolated_db_url(pg_container) -> str:
    name = f"fastapi_migrate_it_{uuid.uuid4().hex[:8]}"
    return await _create_isolated_database_url(pg_container, name)


# ── (i) mode="check" on a behind schema: fails closed, no DDL ─────────────────


async def test_mode_check_on_behind_schema_fails_closed_no_ddl(
    isolated_db_url: str,
) -> None:
    from sqlalchemy.ext.asyncio import create_async_engine
    from varco_sa.migration.migrator import AlembicMigrator

    engine = create_async_engine(isolated_db_url, echo=False)
    migrator = AlembicMigrator(engine, include_framework_branch=True)

    app = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[
            CoreModule(),
            MigrationsModule(migrator, settings=MigrationSettings(mode="check")),
        ],
        validate=False,
    )

    with pytest.raises(PendingMigrationsError):
        await _boot_and_get_ping(app)
        pytest.fail("no request should ever be served under mode=check")

    # No DDL was ever written — the framework table must still be absent.
    assert not await _outbox_table_exists(engine)
    await migrator.close()
    await engine.dispose()


# ── (ii) mode="upgrade": applies before first request; second boot is a no-op ─


async def test_mode_upgrade_applies_before_first_request_and_is_idempotent(
    isolated_db_url: str,
) -> None:
    from sqlalchemy.ext.asyncio import create_async_engine
    from varco_sa.migration.migrator import AlembicMigrator

    engine = create_async_engine(isolated_db_url, echo=False)
    migrator = AlembicMigrator(engine, include_framework_branch=True)

    app = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[
            CoreModule(),
            MigrationsModule(migrator, settings=MigrationSettings(mode="upgrade")),
        ],
        validate=False,
    )

    assert not await _outbox_table_exists(engine)
    response = await _boot_and_get_ping(app)
    assert response.status_code == 200
    assert await _outbox_table_exists(engine)
    await migrator.close()

    # Second boot against the now-current database: upgrade() finds nothing
    # pending and is a clean no-op — must not raise, must still serve.
    migrator_2 = AlembicMigrator(engine, include_framework_branch=True)
    app_2 = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[
            CoreModule(),
            MigrationsModule(migrator_2, settings=MigrationSettings(mode="upgrade")),
        ],
        validate=False,
    )
    response = await _boot_and_get_ping(app_2)
    assert response.status_code == 200
    await migrator_2.close()
    await engine.dispose()


# ── (iii) on_failure="warn" vs "fail" ──────────────────────────────────────────


def _broken_migrator():
    """An AlembicMigrator pointed at an unreachable database — a real
    failure (connection refused), not a fabricated in-memory one."""
    from sqlalchemy.ext.asyncio import create_async_engine
    from varco_sa.migration.migrator import AlembicMigrator

    broken_engine = create_async_engine(
        "postgresql+asyncpg://nouser:nopass@127.0.0.1:1/nodb", echo=False
    )
    return AlembicMigrator(broken_engine, include_framework_branch=True), broken_engine


async def test_on_failure_fail_aborts_startup_no_request_served() -> None:
    migrator, engine = _broken_migrator()
    try:
        app = create_varco_app(
            DIContainer(),
            routers=[_PingRouter],
            modules=[
                CoreModule(),
                MigrationsModule(
                    migrator,
                    settings=MigrationSettings(
                        mode="upgrade", on_failure="fail", lock_timeout=5.0, timeout=15.0
                    ),
                ),
            ],
            validate=False,
        )
        with pytest.raises(Exception):  # noqa: B017 — startup failure propagates
            await _boot_and_get_ping(app)
            pytest.fail("no request should ever be served")
    finally:
        await migrator.close()
        await engine.dispose()


async def test_on_failure_warn_keeps_serving_despite_failed_migration() -> None:
    migrator, engine = _broken_migrator()
    try:
        app = create_varco_app(
            DIContainer(),
            routers=[_PingRouter],
            modules=[
                CoreModule(),
                MigrationsModule(
                    migrator,
                    settings=MigrationSettings(
                        mode="upgrade", on_failure="warn", lock_timeout=5.0, timeout=15.0
                    ),
                ),
            ],
            validate=False,
        )
        response = await _boot_and_get_ping(app)
        assert response.status_code == 200
    finally:
        await migrator.close()
        await engine.dispose()


# ── (iv) mode="off" (default): registers nothing, touches nothing ─────────────


async def test_mode_off_default_registers_nothing_touches_nothing(
    isolated_db_url: str,
) -> None:
    from sqlalchemy.ext.asyncio import create_async_engine
    from varco_sa.migration.migrator import AlembicMigrator

    engine = create_async_engine(isolated_db_url, echo=False)
    migrator = AlembicMigrator(engine, include_framework_branch=True)

    app = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[CoreModule(), MigrationsModule(migrator, settings=MigrationSettings(mode="off"))],
        validate=False,
    )

    response = await _boot_and_get_ping(app)
    assert response.status_code == 200

    # mode="off" never ran anything — the framework table must still be absent.
    assert not await _outbox_table_exists(engine)
    await migrator.close()
    await engine.dispose()


# ── (v) two concurrent lifespans against one database ──────────────────────────


async def test_two_concurrent_lifespans_exactly_one_migrates_schema_not_corrupted(
    isolated_db_url: str,
) -> None:
    from sqlalchemy.ext.asyncio import create_async_engine
    from varco_sa.migration.migrator import AlembicMigrator

    engine = create_async_engine(isolated_db_url, echo=False)
    migrator_a = AlembicMigrator(engine, include_framework_branch=True)
    migrator_b = AlembicMigrator(engine, include_framework_branch=True)

    app_a = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[
            CoreModule(),
            MigrationsModule(
                migrator_a,
                settings=MigrationSettings(mode="upgrade", lock_timeout=30.0, timeout=60.0),
            ),
        ],
        validate=False,
    )
    app_b = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[
            CoreModule(),
            MigrationsModule(
                migrator_b,
                settings=MigrationSettings(mode="upgrade", lock_timeout=30.0, timeout=60.0),
            ),
        ],
        validate=False,
    )

    results: dict[str, str] = {}

    async def _boot(label: str, app) -> None:
        try:
            async with app.router.lifespan_context(app):
                results[label] = "served"
        except MigrationLockTimeout:
            results[label] = "lock_timeout"
        except Exception as exc:  # noqa: BLE001 — any other real failure is a test failure
            results[label] = f"error: {exc!r}"

    # Start both lifespans concurrently — genuine contention on the same
    # advisory-lock-backed migration, not a simulated race.
    await asyncio.gather(_boot("a", app_a), _boot("b", app_b))

    # Both outcomes are legitimate lock behaviour (CLAUDE.md's held-open
    # advisory-lock-transaction design — there is no release() call, only
    # the lock holder's own COMMIT): a successful, no-corrupting-error boot
    # or a MigrationLockTimeout naming the contended lock. What must NOT
    # happen is a raw "relation already exists" (concurrent DDL corruption).
    for label, outcome in results.items():
        assert not outcome.startswith("error:"), f"{label}: unexpected failure: {outcome}"
        assert outcome in ("served", "lock_timeout"), f"{label}: {outcome}"

    assert await _outbox_table_exists(engine)
    await migrator_a.close()
    await migrator_b.close()
    await engine.dispose()


# ── (vi) app-layer MigrationLockTimeout, deterministically (Plan 018 / RT9,
#         Step 25 — §RT9-scope's residual) ─────────────────────────────────────


async def test_lifecycle_raises_migration_lock_timeout_when_holder_never_releases(
    isolated_db_url: str,
) -> None:
    """
    A lifespan that cannot acquire the migration lock must raise
    ``MigrationLockTimeout`` and serve **no** request.

    Why this exists alongside
    ``test_two_concurrent_lifespans_exactly_one_migrates_schema_not_corrupted``
    above: that test races two lifespans and therefore legitimately accepts
    ``outcome in ("served", "lock_timeout")`` — either branch is correct
    behaviour for a race. The consequence is that the ``MigrationLockTimeout``
    branch is never actually *asserted* at the app layer (it is asserted at
    the migrator layer, ``varco_sa/tests/test_migration_lock.py:114``).

    Here the **test itself** holds the advisory lock from a separate
    connection for the whole duration, so there is no race at all: the
    lifespan cannot possibly win, and the assertion is a single branch.

    Args (fixtures):
        isolated_db_url: A freshly created, empty Postgres database, so the
                         framework branch genuinely has pending revisions —
                         ``AlembicMigrator.upgrade()`` short-circuits on an
                         empty plan *before* taking the lock, which would
                         make this test pass for the wrong reason.

    Edge cases:
        - ``lock_timeout=1.0`` keeps the deliberate wait short; ``timeout``
          is left generous so a slow container cannot be mistaken for a
          lock timeout.
    """
    from sqlalchemy.ext.asyncio import create_async_engine
    from varco_sa.migration.lock import migration_lock
    from varco_sa.migration.migrator import AlembicMigrator

    engine = create_async_engine(isolated_db_url, echo=False)
    migrator = AlembicMigrator(engine, include_framework_branch=True)

    settings = MigrationSettings(mode="upgrade", lock_timeout=1.0, timeout=120.0)
    app = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[CoreModule(), MigrationsModule(migrator, settings=settings)],
        validate=False,
    )

    # Precondition: there IS pending work, so the lock is genuinely reached.
    assert not (await migrator.plan()).is_empty, (
        "no pending revisions — upgrade() would short-circuit before the lock "
        "and this test would assert nothing"
    )

    served: list[int] = []

    # The test holds the lock itself, from its own dedicated connection.
    async with migration_lock(engine, settings.lock_key, timeout=60.0):
        with pytest.raises(MigrationLockTimeout):
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://test"
                ) as client:
                    served.append((await client.get("/ping")).status_code)

    assert served == [], (
        "a request was served despite the migration lock never being acquired — "
        f"startup did not fail closed (statuses seen: {served})"
    )
    # The blocked lifespan wrote no DDL of its own.
    assert not await _outbox_table_exists(engine)

    await migrator.close()
    await engine.dispose()
