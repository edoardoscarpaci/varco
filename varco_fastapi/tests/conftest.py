"""
Shared pytest fixtures for varco_fastapi/tests.

Autouse fixture (Plan 002): resets the process-global JWT claim-transformer
and token-profile registries (``varco_core.jwt.transform.runtime`` /
``varco_core.jwt.profile``) before and after every test, mirroring
``varco_core/tests/conftest.py``.

Without this, tests in different files that each ``monkeypatch.setenv`` a
different ``VARCO_JWT_TRANSFORM_*``/``VARCO_JWT_PROFILE__*`` value would leak
state into each other via the lazily-built, cached process-global registry —
whichever test resolves it first "wins" for the rest of the test session.
"""

from __future__ import annotations

import os as _os

import pytest


@pytest.fixture(autouse=True)
def _reset_jwt_globals():
    """Reset JWT claim-transform + token-profile globals around every test."""
    _reset_all()
    yield
    _reset_all()


def _reset_all() -> None:
    from varco_core.jwt.profile import reset_token_profiles
    from varco_core.jwt.transform.runtime import reset_claim_transforms

    reset_claim_transforms()
    reset_token_profiles()


# ── redis_url — Plan 035 / Phase 5, Step 25 ─────────────────────────────────
#
# varco_fastapi has no shared session-scoped `redis_url` fixture of its own
# (that fixture lives in varco_redis/varco_beanie/varco_sa/varco_kafka/
# varco_memcached/varco_casbin/varco_nats only — see
# test_app_migrations_integration.py:82-104 for the matching Postgres
# precedent and its written rationale for a self-contained, module-local
# fixture rather than importing across a package boundary).
#
# CLAUDE.md's VARCO_TEST_<SERVICE>_URL contract: only the namespaced
# VARCO_TEST_REDIS_URL override is ever honoured. The bare REDIS_URL name is
# deliberately NEVER read — a developer with an unrelated REDIS_URL exported
# in their shell must never silently run destructive integration tests
# against their own dev Redis instance.


@pytest.fixture(scope="session")
def redis_url(request: pytest.FixtureRequest) -> str:
    """
    Session-scoped Redis connection URL — real container or override.

    ``VARCO_TEST_REDIS_URL`` overrides the container entirely: when set, no
    container is started, the value is used as-is, and it is reported via
    ``request.config.stash`` (the "NOT a clean-room run" signal). Bare
    ``REDIS_URL`` is never honoured.
    """
    if not _os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip(
            "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration"
        )

    override = _os.environ.get("VARCO_TEST_REDIS_URL")
    if override:
        request.config.stash.setdefault("varco_test_overrides", []).append(("redis", override))
        yield override
        return

    from testcontainers.redis import RedisContainer  # noqa: PLC0415

    with RedisContainer() as container:
        yield f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0"
