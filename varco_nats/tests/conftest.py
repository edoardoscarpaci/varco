"""
Shared fixtures for varco_nats integration tests (Plan 012 / RT1+RT2,
Steps 6-8 & 12).

``nats_url`` starts a single NATS (JetStream-enabled) container **once per
test session**, replacing the local generic-``DockerContainer`` fixture
previously declared at ``test_nats_integration.py:54-76``. Uses the
first-party ``testcontainers.nats.NatsContainer`` (v4.3+) instead of the
generic ``DockerContainer`` + manual log-wait.

Per-test namespacing rule: because the server is shared for the whole
session, every test MUST use a unique subject/stream name (a
``uuid4().hex[:8]`` run id, as this package's tests already do) — never
assume a subject/stream starts empty. A test that needs a pristine server
must declare its own function-scoped ``nats_container_fresh`` fixture
instead.

``VARCO_TEST_NATS_URL`` overrides the container entirely (Open Question 1)
— when set, no container is started; the value is used as-is and reported
via ``request.config.stash``.
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def nats_url(request: pytest.FixtureRequest) -> str:
    """
    Session-scoped NATS connection URL (JetStream enabled) — real container
    or override.

    See the module docstring for the per-test namespacing rule and the
    ``VARCO_TEST_NATS_URL`` override contract.

    Yields:
        A ``nats://`` connection URL for the shared broker.
    """
    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip(
            "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration"
        )

    override = os.environ.get("VARCO_TEST_NATS_URL")
    if override:
        request.config.stash.setdefault("varco_test_overrides", []).append(("nats", override))
        yield override
        return

    from testcontainers.nats import NatsContainer  # noqa: PLC0415

    with NatsContainer().with_command("-js") as container:
        yield container.nats_uri()


@pytest.fixture(autouse=True)
def _select_nats_backends(monkeypatch: pytest.MonkeyPatch) -> None:
    """Plan 047 §D-tests — see varco_kafka's twin fixture docstring."""
    monkeypatch.setenv("VARCO_EVENT_BUS_BACKEND", "nats")
    monkeypatch.setenv("VARCO_DLQ_BACKEND", "nats")
