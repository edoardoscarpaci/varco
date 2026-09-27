"""
Crashed migration-lock-holder recovery (Plan 018 / RT9 + RT7b, Step 33 —
chaos tier).

The real operational question RT9 leaves unanswered: *"a pod died
mid-migration — is the next deploy wedged forever?"* CLAUDE.md's
held-open-advisory-lock-transaction design says no (there is no ``release()``
call; the lock is released by the holder's own COMMIT, and a dead connection
releases it automatically — the correct failure domain). **Nothing verified
that.**

Mechanism: kill the connection holding the advisory lock with
``pg_terminate_backend`` from a separate connection. That is the closest
in-database analogue of "the pod was OOM-killed", and it is deterministic —
the test controls both the holder and the killer. ``ChaosContainer.restart()``
is the documented fallback if ``pg_terminate_backend`` ever proves flaky
(plan Step 33), which is why this module still owns a chaos container.

Container scope (§chaos-fixture): a **module**-scoped
``postgres_container_chaos`` declared here, never in ``conftest.py``.
``varco_fastapi`` has no shared session-scoped Postgres fixture, but the
convention is held to anyway so the file reads identically to the other
chaos modules.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
from providify import DIContainer
from varco_chaos.containers import ChaosContainer
from varco_core.di.module import CoreModule
from varco_core.migration.settings import MigrationSettings
from varco_fastapi.app import create_varco_app
from varco_fastapi.modules.lifecycle import MigrationsModule
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter

pytestmark = [pytest.mark.integration, pytest.mark.chaos]


class _PingRouter(GenericRouter):
    _prefix = "/ping"

    @route("GET", "")
    async def ping(self) -> dict:
        return {"ok": True}


def _postgres_url(container) -> str:  # noqa: ANN001 — DockerContainer, PostgresContainer subclass
    url = container.get_connection_url(driver="asyncpg")
    assert url.startswith("postgresql+asyncpg://"), f"unexpected DSN shape: {url}"
    return url


@pytest.fixture(scope="module")
def postgres_container_chaos() -> Iterator[ChaosContainer]:
    """
    A Postgres container this module is allowed to break.

    Yields:
        A ``ChaosContainer`` wrapping ``postgres:16-alpine``, with a
        ``url_factory`` so ``.url`` always re-derives the current DSN
        (Plan 019 / §RT7b-port) rather than any caller trusting a value
        captured before a ``restart()`` — this module's primary scenario
        uses ``pg_terminate_backend`` instead, but ``chaos.restart()``
        remains the documented fallback (see the test docstring), so the
        fixture must be correct for that path too.
    """
    from testcontainers.postgres import PostgresContainer  # noqa: PLC0415

    with PostgresContainer("postgres:16-alpine") as container:
        _postgres_url(container)  # shape check at fixture setup, as before
        yield ChaosContainer(
            container,
            ready=lambda logs: "database system is ready to accept connections" in logs,
            url_factory=_postgres_url,
        )


async def _isolated_db_url(admin_url: str, name: str) -> str:
    """Create a fresh database so the framework branch has genuine pending work."""
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    admin_engine = create_async_engine(admin_url, echo=False)
    try:
        async with admin_engine.connect() as conn:
            await conn.execution_options(isolation_level="AUTOCOMMIT")
            await conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}"'))
            await conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    finally:
        await admin_engine.dispose()
    return make_url(admin_url).set(database=name).render_as_string(hide_password=False)


async def _outbox_table_exists(engine: Any) -> bool:
    import sqlalchemy as sa

    async with engine.connect() as conn:
        return bool(
            await conn.scalar(
                sa.text(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_name = 'varco_outbox'"
                )
            )
        )


async def test_crashed_lock_holder_releases_and_next_boot_proceeds(
    postgres_container_chaos: ChaosContainer,
) -> None:
    """
    A lifespan booted after the previous lock holder's connection was killed
    must acquire the lock and apply the pending revisions — not hang to
    ``lock_timeout``.

    Sequence:
      1. Hold the migration advisory lock from a dedicated connection and
         confirm a boot with a 1 s ``lock_timeout`` cannot get in.
      2. ``pg_terminate_backend`` that connection — the "pod died" moment.
      3. Boot again. It must acquire, apply, and serve.

    Step (1) is the control: without it, step (3) succeeding would prove
    nothing (the lock might never have been held in the first place).

    Edge cases:
        - The database is created fresh so the shipped framework branch has
          genuinely pending revisions; ``AlembicMigrator.upgrade()``
          short-circuits on an empty plan **before** taking the lock, which
          would make the whole test vacuous.
        - ``pg_terminate_backend`` is preferred over a full container restart
          because it kills exactly one backend and leaves the rest of the
          module's container healthy (module scope). ``chaos.restart()`` is
          the documented fallback if it proves flaky.
    """
    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import create_async_engine
    from varco_core.migration.errors import MigrationLockTimeout
    from varco_sa.migration.lock import migration_lock
    from varco_sa.migration.migrator import AlembicMigrator

    chaos = postgres_container_chaos
    assert chaos is not None  # the fallback handle; see the docstring
    run_id = uuid.uuid4().hex[:8]
    db_url = await _isolated_db_url(chaos.url, f"migchaos_{run_id}")

    settings = MigrationSettings(mode="upgrade", lock_timeout=1.0, timeout=180.0)

    engine = create_async_engine(db_url, echo=False)
    holder_engine = create_async_engine(db_url, echo=False)

    def _app() -> Any:
        return create_varco_app(
            DIContainer(),
            routers=[_PingRouter],
            modules=[
                CoreModule(),
                MigrationsModule(
                    AlembicMigrator(engine, include_framework_branch=True), settings=settings
                ),
            ],
            validate=False,
        )

    try:
        assert not await _outbox_table_exists(engine)

        # (1) Hold the lock; a boot must be refused.
        lock_cm = migration_lock(holder_engine, settings.lock_key, timeout=60.0)
        await lock_cm.__aenter__()
        try:
            with pytest.raises(MigrationLockTimeout):
                async with _app().router.lifespan_context(_app()):
                    pytest.fail("a lifespan acquired a lock that is deliberately held")
        finally:
            # (2) The holder "crashes": kill its backend from a third
            # connection. The held-open transaction dies with it, which is
            # what must release the advisory lock.
            killer = create_async_engine(db_url, echo=False)
            try:
                async with killer.connect() as conn:
                    await conn.execution_options(isolation_level="AUTOCOMMIT")
                    await conn.execute(
                        sa.text(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                            "WHERE datname = :db AND pid <> pg_backend_pid() "
                            "AND state = 'idle in transaction'"
                        ),
                        {"db": db_url.rsplit("/", 1)[-1]},
                    )
            finally:
                await killer.dispose()
            # The context manager's own exit will now fail on a dead
            # connection — that is the crash being simulated, not a problem.
            try:
                await lock_cm.__aexit__(None, None, None)
            except Exception:  # noqa: BLE001 — the holder is dead by design
                pass
            await holder_engine.dispose()

        # Give Postgres a moment to reap the terminated backend, polled.
        deadline = asyncio.get_event_loop().time() + 15.0
        while asyncio.get_event_loop().time() < deadline:
            async with engine.connect() as conn:
                stuck = await conn.scalar(
                    sa.text("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted")
                )
            if not stuck:
                break
            await asyncio.sleep(0.25)

        # (3) The next boot must proceed.
        app = _app()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/ping")

        assert response.status_code == 200, (
            "the boot after a crashed lock holder did not serve — the next deploy is wedged"
        )
        assert await _outbox_table_exists(engine), (
            "the boot after a crashed lock holder acquired the lock but applied no revisions"
        )
    finally:
        await engine.dispose()
