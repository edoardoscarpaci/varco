"""
Shared fixtures for varco_sa tests.

Uses an in-memory SQLite database (aiosqlite) so no external service is
required.  A fresh database and session are created for every test function.

DESIGN: fresh DeclarativeBase per test
    DeclarativeBase.metadata is a class-level object that persists for the
    lifetime of the class.  If the same module-level Base is shared across
    tests, the second test that calls SAModelFactory.build() for an already-
    registered table name will get an SA InvalidRequestError ("Table X is
    already defined for this MetaData instance").

    Creating a new DeclarativeBase subclass inside the fixture gives each test
    an isolated metadata namespace.  Python class creation is cheap (~µs) so
    there is no meaningful overhead.
"""

from __future__ import annotations

import os
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_sa.factory import SAModelFactory

# ── Session-scoped Postgres container (Plan 012 / RT1, Steps 7 & 9) ───────────
#
# `postgres_container`/`postgres_url` are started ONCE per test session and
# shared across every integration test in this package — a Postgres container
# previously started (module-scoped) in ~10 separate test files.
#
# Per-test namespacing rule: because the container is shared, every test that
# reads or writes rows must confine itself to a name it owns exclusively —
# a fresh schema (`create_isolated_database_url`), a uniquely-named database,
# or a `uuid4().hex[:8]`-suffixed table/role name. A test that needs a
# genuinely pristine server (e.g. asserting on `pg_stat_activity` globally)
# must declare its own function-scoped `postgres_container_fresh` fixture
# instead of relying on this shared one.
#
# `VARCO_TEST_POSTGRES_URL` overrides the container entirely (Open Question 1).
# When set, the value is used as-is and reported via `request.config.stash`
# rather than silently falling back to a container on a dead endpoint.


@pytest.fixture(scope="session")
def postgres_container() -> Any:
    """
    Session-scoped real Postgres container (or ``None`` under an override).

    Honors ``VARCO_TEST_POSTGRES_URL`` — when set, no container is started
    and this fixture yields ``None`` (callers needing the raw handle, e.g.
    RT7's chaos tests, must not run under an override).

    Yields:
        A started ``testcontainers.postgres.PostgresContainer``, or ``None``
        when ``VARCO_TEST_POSTGRES_URL`` is set.
    """
    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip(
            "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration"
        )
    if os.environ.get("VARCO_TEST_POSTGRES_URL"):
        yield None
        return

    from testcontainers.postgres import PostgresContainer  # noqa: PLC0415

    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def postgres_url(request: pytest.FixtureRequest, postgres_container: Any) -> str:
    """
    Session-scoped asyncpg DSN for the shared Postgres container.

    See the module-level docstring above for the per-test namespacing rule
    and the ``VARCO_TEST_POSTGRES_URL`` override contract.

    Returns:
        A DSN beginning with ``postgresql+asyncpg://``.
    """
    override = os.environ.get("VARCO_TEST_POSTGRES_URL")
    if override:
        request.config.stash.setdefault("varco_test_overrides", []).append(("postgres", override))
        return override
    return asyncpg_url(postgres_container)


@pytest.fixture
def base() -> type[DeclarativeBase]:
    """
    Return a fresh ``DeclarativeBase`` subclass per test.

    Each call produces a new class with an empty ``MetaData`` — no cross-test
    table-name collisions possible.

    Returns:
        A newly created ``DeclarativeBase`` subclass.
    """

    # Define the class inside the fixture so each call gets a brand-new class
    # object with its own MetaData instance — no shared state between tests.
    class _FreshBase(DeclarativeBase):
        pass

    return _FreshBase


@pytest.fixture
def factory(base: type[DeclarativeBase]) -> SAModelFactory:
    """
    Return an ``SAModelFactory`` bound to the per-test fresh ``Base``.

    Args:
        base: Fresh ``DeclarativeBase`` from the ``base`` fixture.

    Returns:
        A new ``SAModelFactory`` instance with an empty internal cache.
    """
    return SAModelFactory(base)


@pytest_asyncio.fixture
async def session(base: type[DeclarativeBase]) -> AsyncSession:
    """
    Fresh in-memory SQLite session per test.

    Creates all tables declared in ``base.metadata`` before yielding,
    then drops them and disposes the engine on teardown.

    Args:
        base: Fresh ``DeclarativeBase`` from the ``base`` fixture.

    Yields:
        An open ``AsyncSession`` backed by a new in-memory SQLite DB.
    """
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(base.metadata.create_all)
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    async with async_session() as s:
        yield s
    async with engine.begin() as conn:
        await conn.run_sync(base.metadata.drop_all)
    await engine.dispose()


def asyncpg_url(container: Any) -> str:
    """
    Return the container's connection URL on the **asyncpg** dialect.

    DESIGN: ask testcontainers for the driver instead of rewriting the string.
        ``PostgresContainer.get_connection_url()`` honours the constructor's
        ``driver`` (default ``"psycopg2"``), so it returns
        ``postgresql+psycopg2://…``.  Fixtures used to normalise that with
        ``.replace("postgresql://", "postgresql+asyncpg://")``, which matches
        nothing on that string and silently hands ``create_async_engine`` a
        sync-driver DSN — surfacing much later as
        ``ModuleNotFoundError: No module named 'psycopg2'``.
        ✅ No string surgery, so it cannot silently no-op on a URL shape
           change; one helper means the seven call sites cannot drift apart.
        ❌ Couples to the ``get_connection_url(driver=...)`` keyword, which is
           testcontainers-specific — acceptable, this is test-only code.

    Args:
        container: A started ``testcontainers.postgres.PostgresContainer``
            (typed ``Any`` so this module does not import testcontainers,
            which is an integration-only optional dependency).

    Returns:
        A DSN beginning with ``postgresql+asyncpg://``.

    Raises:
        AssertionError: If the container returned a non-asyncpg DSN — a loud
            failure here beats an obscure driver-import error later.

    Edge cases:
        A container constructed with ``driver=None`` or ``driver="asyncpg"``
        is handled identically; the explicit keyword always wins.
    """
    url = container.get_connection_url(driver="asyncpg")
    assert url.startswith("postgresql+asyncpg://"), (
        f"expected an asyncpg DSN from the container, got: {url}"
    )
    return url


class SyncOp:
    """
    Minimal Alembic-``op``-shaped facade over a **sync** SQLAlchemy connection.

    ``rls_upgrade``/``framework_rls_upgrade`` accept "any object exposing
    ``execute()``/``get_bind()``". Inside an async test the only way to reach a
    sync connection is ``await conn.run_sync(...)``, so this adapter is what
    lets an integration test drive the real migration op against real Postgres.

    DESIGN: shared in conftest rather than duplicated per test module.
        ✅ ``test_framework_rls.py`` previously referenced ``_SyncOp`` without
           defining or importing it (a ``NameError`` that meant two tests had
           never once run to completion); one shared definition makes that
           class of drift impossible.
        ❌ Slightly wider blast radius — a change here touches every RLS
           integration module at once. Acceptable: the surface is two methods.

    Async safety: ❌ Sync-only by construction — must be used inside
        ``await conn.run_sync(...)``.
    """

    def __init__(self, sync_conn: Any) -> None:
        self._conn = sync_conn

    def execute(self, stmt: Any) -> None:
        """Execute a raw SQL string, mirroring ``alembic.op.execute``."""
        import sqlalchemy as sa

        self._conn.execute(sa.text(str(stmt)))

    def get_bind(self) -> Any:
        """Return the underlying connection, mirroring ``alembic.op.get_bind``."""
        return self._conn


#: Login role used by the RLS integration fixtures. Deliberately NOT the
#: container's bootstrap role — see ``rls_app_engine``.
RLS_APP_ROLE = "varco_rls_app"
RLS_APP_PASSWORD = "varco_rls_pw"
RLS_READER_ROLE = "varco_rls_reader"
RLS_READER_PASSWORD = "varco_rls_reader_pw"


async def provision_rls_app_url(container: Any) -> str:
    """
    Create a non-superuser login role on ``container`` and return its DSN.

    DESIGN: RLS integration tests must NOT connect as the container's own role.
        Postgres exempts **superusers** and roles with ``BYPASSRLS`` from row
        security *unconditionally* — ``FORCE ROW LEVEL SECURITY`` only removes
        the separate, weaker exemption granted to a table's **owner**.
        ``PostgresContainer``'s bootstrap role is the cluster superuser
        (``rolsuper=True, rolbypassrls=True``), so a policy applied and queried
        over that connection is silently never enforced and every "tenant A
        cannot see tenant B" assertion fails while the shipped DDL is correct.
        ✅ Tests now exercise the role shape a real application uses, so they
           genuinely regress the ``FORCE`` clause in ``render_rls_ddl``.
        ✅ The role OWNS the tables it creates, which is precisely the
           owner-exemption case ``FORCE`` exists to close.
        ❌ Needs an explicit ``GRANT ... ON SCHEMA public`` (PostgreSQL 15
           revoked ``CREATE`` on ``public`` from ``PUBLIC``).

    Args:
        container: A started ``PostgresContainer``.

    Returns:
        An asyncpg DSN authenticating as the non-superuser role.

    Edge cases:
        Idempotent — safe to call once per test against a module-scoped
        container; an already-existing role is left as-is.
    """
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    admin_url = asyncpg_url(container)
    admin_engine = create_async_engine(admin_url, echo=False)
    try:
        async with admin_engine.begin() as conn:
            exists = await conn.scalar(
                sa.text("SELECT 1 FROM pg_roles WHERE rolname = :r"),
                {"r": RLS_APP_ROLE},
            )
            if not exists:
                await conn.execute(
                    sa.text(f"CREATE ROLE {RLS_APP_ROLE} LOGIN PASSWORD '{RLS_APP_PASSWORD}'")
                )
            # PG15+: CREATE on schema public is no longer granted to PUBLIC.
            await conn.execute(sa.text(f"GRANT CREATE, USAGE ON SCHEMA public TO {RLS_APP_ROLE}"))
    finally:
        await admin_engine.dispose()

    # NB: str(URL) renders the password as "***" — render_as_string with
    # hide_password=False is the only form that yields a connectable DSN.
    return (
        make_url(admin_url)
        .set(username=RLS_APP_ROLE, password=RLS_APP_PASSWORD)
        .render_as_string(hide_password=False)
    )


async def provision_rls_reader_url(container: Any) -> str:
    """
    Create a second non-superuser login role that **owns nothing**, and return its DSN.

    DESIGN: a non-owning observer is the only role that can witness the
    §D-S12-order ordering guarantee (Plan 037 / Step 6)
        Postgres exempts a table's **owner** from its RLS policies until
        ``FORCE ROW LEVEL SECURITY`` is applied. ``provision_rls_app_url``'s
        role owns every table it creates, so between ``ENABLE`` (statement 2)
        and ``FORCE`` (statement 3) it sees every row regardless of whether a
        policy exists — which makes it blind to exactly the failure the
        statement reorder exists to prevent.
        ✅ This role owns nothing, so RLS applies to it the moment
           ``ENABLE ROW LEVEL SECURITY`` runs. Under the OLD order
           (ENABLE before CREATE POLICY) it would observe the default-deny
           window as **zero rows**; under the new order it observes correct
           per-tenant scoping. That difference is the regression.
        ❌ Needs an explicit ``GRANT SELECT`` per table under test — it has no
           ownership privileges to fall back on.

    Args:
        container: A started ``PostgresContainer``.

    Returns:
        An asyncpg DSN authenticating as the non-owning, non-superuser role.

    Edge cases:
        Idempotent — an already-existing role is left as-is. The role is
        granted ``USAGE`` on ``public`` but deliberately **not** ``CREATE``,
        so it cannot become an owner by accident.
    """
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    admin_url = asyncpg_url(container)
    admin_engine = create_async_engine(admin_url, echo=False)
    try:
        async with admin_engine.begin() as conn:
            exists = await conn.scalar(
                sa.text("SELECT 1 FROM pg_roles WHERE rolname = :r"),
                {"r": RLS_READER_ROLE},
            )
            if not exists:
                await conn.execute(
                    sa.text(f"CREATE ROLE {RLS_READER_ROLE} LOGIN PASSWORD '{RLS_READER_PASSWORD}'")
                )
            # USAGE only — never CREATE, so this role can never own a table.
            await conn.execute(sa.text(f"GRANT USAGE ON SCHEMA public TO {RLS_READER_ROLE}"))
    finally:
        await admin_engine.dispose()

    return (
        make_url(admin_url)
        .set(username=RLS_READER_ROLE, password=RLS_READER_PASSWORD)
        .render_as_string(hide_password=False)
    )


async def create_isolated_database_url(container: Any, name: str) -> str:
    """
    Create a fresh database on ``container`` and return its asyncpg DSN.

    DESIGN: a test that migrates must not share the module's database.
        Applying real Alembic revisions writes ``alembic_version`` rows that
        outlive the test. A sibling test constructing a migrator with a
        *different* branch configuration then fails with
        ``No such revision or branch`` because it cannot resolve the revision
        ids the first test stamped. An isolated database removes the coupling
        entirely.
        ✅ Migration tests become order-independent and re-runnable.
        ❌ One extra database per call — negligible for a throwaway container.

    Args:
        container: A started ``PostgresContainer``.
        name:      Database name to create. Dropped and recreated if present,
                   so the helper is idempotent across re-runs.

    Returns:
        An asyncpg DSN pointing at the newly created database.

    Edge cases:
        ``CREATE DATABASE`` cannot run inside a transaction block, so the
        admin connection is switched to AUTOCOMMIT.
    """
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    admin_url = asyncpg_url(container)
    admin_engine = create_async_engine(admin_url, echo=False)
    try:
        async with admin_engine.connect() as conn:
            await conn.execution_options(isolation_level="AUTOCOMMIT")
            await conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}"'))
            await conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    finally:
        await admin_engine.dispose()

    return make_url(admin_url).set(database=name).render_as_string(hide_password=False)
