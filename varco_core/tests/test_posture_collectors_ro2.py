"""
Red-mode tests for Plan 052 Step 9 (RO-2) — ``JwksPostureCollector`` and
``RetentionPostureCollector`` (§D-RO2), plus their contribution from
``CoreModule``.

Neither collector class exists yet, so every test here is expected to fail
with an ``ImportError``/``AttributeError`` until Plan 052 Step 9 lands them.
"""

from __future__ import annotations

from datetime import timedelta

from providify import DIContainer

# Primary red-mode probes.
from varco_core.authority.posture import JwksPostureCollector  # noqa: F401
from varco_core.di import CoreModule
from varco_core.di.collect import collect
from varco_core.posture import PostureCollector
from varco_core.retention.policy import RetentionPolicy, RetentionRegistry
from varco_core.retention.posture import RetentionPostureCollector  # noqa: F401


class _FakeJwksUrlSource:
    """Stand-in for a remote-fetch JWKS source entry."""


class _FakeRegistry:
    """Minimal stand-in exposing the two attributes inspect_jwks_posture() reads."""

    def __init__(self, *, remote: bool, refresh_task) -> None:
        self._entries = {"issuer-a": _FakeJwksUrlSource()} if remote else {}
        self._refresh_task = refresh_task
        self._ttl_seconds = 60.0
        self._min_refresh_interval = 10.0
        self._loaded_at = {}


class _FakeTarget:
    supports_older_than = True
    supports_dry_run = True
    kind = "dlq"

    async def purge(self, *, older_than, limit, dry_run):  # pragma: no cover - unused
        raise NotImplementedError


def _policy(name, *, dry_run, tenant_ids=None):
    return RetentionPolicy(
        name=name,
        target=_FakeTarget(),
        cron_expr="0 3 * * *",
        timezone="UTC",
        dry_run=dry_run,
        older_than=timedelta(days=30),
        tenant_ids=tenant_ids,
    )


def test_a_jwks_collector_warns_when_remote_sources_and_no_refresher() -> None:
    reg = _FakeRegistry(remote=True, refresh_task=None)

    findings = JwksPostureCollector(lambda: reg).collect()

    assert any(f.check == "auth.jwks_refresh_off" for f in findings)


def test_a_jwks_collector_silent_when_refresher_running() -> None:
    reg = _FakeRegistry(remote=True, refresh_task=object())

    findings = JwksPostureCollector(lambda: reg).collect()

    assert not any(f.check == "auth.jwks_refresh_off" for f in findings)


def test_a_jwks_collector_empty_when_no_registry() -> None:
    findings = JwksPostureCollector(lambda: None).collect()

    assert findings == []


def test_b_retention_collector_warns_on_platform_wide_policy() -> None:
    registry = RetentionRegistry([_policy("purge-audit", dry_run=False, tenant_ids=None)])

    findings = RetentionPostureCollector(lambda: registry).collect()

    finding = next(f for f in findings if f.check == "retention.platform_wide")
    assert "purge-audit" in finding.detail


def test_b_retention_collector_info_when_all_dry_run() -> None:
    registry = RetentionRegistry([_policy("preview-only", dry_run=True, tenant_ids=("tenant-a",))])

    findings = RetentionPostureCollector(lambda: registry).collect()

    assert any(f.check == "retention.dry_run_only" for f in findings)


def test_b_retention_collector_empty_when_no_registry() -> None:
    findings = RetentionPostureCollector(lambda: None).collect()

    assert findings == []


def test_c_core_module_contributes_both_collectors_by_name() -> None:
    container = DIContainer()
    CoreModule().register(container)

    names = {c.name for c in collect(container, PostureCollector)}

    assert "jwks" in names
    assert "retention" in names


def test_c_disabled_core_module_contributes_neither_collector() -> None:
    container = DIContainer()
    CoreModule(enabled=False).register(container)

    names = {c.name for c in collect(container, PostureCollector)}

    assert "jwks" not in names
    assert "retention" not in names
