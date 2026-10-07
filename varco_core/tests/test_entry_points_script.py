"""
Red-mode tests for ``scripts/entry_points.py`` (Plan 058 / Steps 4 + 6, §D-entrypoints).

Runs the script's functions in-process against ``varco_core`` only (the
047/048 script-test shape). The script does not exist yet — every test fails
with ``ModuleNotFoundError`` until Step 4 lands it; the taxonomy test also needs
Step 5's committed snapshot.

Assumed API (from the plan's Step 4 text): ``walk_entry_points(packages) ->
list[dict]`` rows with ``family``/``package``/``name``/``module``/``signature``
(functions) and ``phase``/``depends_on`` (module/configuration rows);
``write_snapshot(packages, path)``; ``check(packages, snapshot, *,
architecture=None) -> int``; ``render_block(rows) -> str``; ``EntryPointsError``;
``_NOT_ENTRY_POINTS``.
"""

from __future__ import annotations

import gc
import importlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import entry_points  # noqa: E402

_SNAPSHOT = REPO_ROOT / "design/api-freeze-and-standards/measurements/entry-points.json"


@pytest.fixture(scope="module")
def rows() -> list[dict[str, Any]]:
    return list(entry_points.walk_entry_points(["varco_core"]))


def _find(rows: list[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    return [r for r in rows if r["name"] == name]


@pytest.mark.parametrize(
    ("module", "name", "family"),
    [
        ("varco_core.di", "bootstrap", "bootstrap"),
        ("varco_core.di", "bind_instance", "bind"),
        ("varco_core.tls.di", "bind_trust_store", "bind"),
        ("varco_core.flags.di", "enable_feature_flags", "enable"),
        ("varco_core.tenancy.di", "enable_tenant_membership", "enable"),
        ("varco_core.revocation.di", "enable_token_revocation", "enable"),
        ("varco_core.auth.di", "enable_authorization_audit", "enable"),
        ("varco_core.event.cloudevents", "bind_cloudevents_serializer", "bind"),
    ],
)
def test_function_entry_points_have_family_and_signature(
    rows: list[dict[str, Any]], module: str, name: str, family: str
) -> None:
    matches = [r for r in _find(rows, name) if r["module"] == module]

    assert len(matches) == 1, matches
    assert matches[0]["family"] == family
    assert matches[0]["signature"]


@pytest.mark.parametrize(
    "name",
    [
        "CoreModule",
        "ObservabilityModule",
        "LifecycleModule",
        "WebhookModule",
        "TrustStoreModule",
        "InMemoryProfile",
    ],
)
def test_varco_modules_appear_as_module_rows_with_classvars(
    rows: list[dict[str, Any]], name: str
) -> None:
    matches = [r for r in _find(rows, name) if r["family"] == "module"]

    assert len(matches) == 1
    assert "phase" in matches[0] and "depends_on" in matches[0]
    # Classes never get a signature (interpreter-unstable synthesised __init__).
    assert not matches[0].get("signature")


def test_varco_module_base_itself_is_not_a_row(rows: list[dict[str, Any]]) -> None:
    assert _find(rows, "VarcoModule") == []


def test_otel_configuration_appears_as_configuration_row(rows: list[dict[str, Any]]) -> None:
    assert [r["family"] for r in _find(rows, "OtelConfiguration")] == ["configuration"]


def test_unreachable_varco_module_subclass_raises_entry_points_error_naming_it() -> None:
    # Completeness guard. __module__ is set inside the varco_core namespace but to
    # a module the walk never reaches, so unrelated test-local subclasses elsewhere
    # in the suite are not what trips it.
    from varco_core.di import VarcoModule

    def _make() -> None:
        cls = type(
            "UnreachableProbeModule",
            (VarcoModule,),
            {"__module__": "varco_core._unreachable_probe"},
        )
        with pytest.raises(entry_points.EntryPointsError) as exc:
            entry_points.walk_entry_points(["varco_core"])
        assert "UnreachableProbeModule" in str(exc.value)
        del cls

    _make()
    gc.collect()


def test_output_is_deterministic_and_sorted() -> None:
    first = list(entry_points.walk_entry_points(["varco_core"]))
    second = list(entry_points.walk_entry_points(["varco_core"]))

    assert first == second
    keys = [(r["family"], r["package"], r["module"], r["name"]) for r in first]
    assert keys == sorted(keys)


def test_check_returns_zero_on_match(tmp_path: Path) -> None:
    snap = tmp_path / "entry-points.json"
    entry_points.write_snapshot(["varco_core"], snap)

    assert entry_points.check(["varco_core"], snap) == 0


def test_check_returns_one_on_diff_naming_family_and_name(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    snap = tmp_path / "entry-points.json"
    entry_points.write_snapshot(["varco_core"], snap)
    data = json.loads(snap.read_text())
    text = json.dumps(data)
    snap.write_text(text.replace('"bind_instance"', '"bind_instance_renamed"'))

    assert entry_points.check(["varco_core"], snap) == 1
    out = capsys.readouterr()
    assert "bind_instance" in out.out + out.err


def test_check_returns_two_on_missing_snapshot(tmp_path: Path) -> None:
    assert entry_points.check(["varco_core"], tmp_path / "absent.json") == 2


def test_architecture_block_matches_render_block(tmp_path: Path) -> None:
    snap = tmp_path / "entry-points.json"
    entry_points.write_snapshot(["varco_core"], snap)
    block = entry_points.render_block(entry_points.walk_entry_points(["varco_core"]))
    arch = tmp_path / "ARCHITECTURE.md"
    arch.write_text(f"# A\n\n<!-- entry-points:begin -->\n{block}\n<!-- entry-points:end -->\n")

    assert entry_points.check(["varco_core"], snap, architecture=arch) == 0


def test_architecture_without_markers_is_an_error_naming_them(tmp_path: Path) -> None:
    snap = tmp_path / "entry-points.json"
    entry_points.write_snapshot(["varco_core"], snap)
    arch = tmp_path / "ARCHITECTURE.md"
    arch.write_text("# no markers here\n")

    with pytest.raises(entry_points.EntryPointsError) as exc:
        entry_points.check(["varco_core"], snap, architecture=arch)

    assert "entry-points:begin" in str(exc.value)


def test_not_entry_points_exclusions_all_resolve() -> None:
    # A stale exclusion is an error (048's exclusion-test shape).
    for key in entry_points._NOT_ENTRY_POINTS:
        module_name, _, attr = key.partition(":")
        module = importlib.import_module(module_name)
        obj: Any = module
        for part in attr.split("."):
            obj = getattr(obj, part)


# ── Step 6: the committed snapshot reproduces the DI-14 taxonomy ─────────────

_EXPECTED_NAMES = [
    # async_bootstrap
    "async_bootstrap",
    # bind_*
    "bind_instance",
    "bind_settings",
    "bind_core_settings",
    "bind_env_overrides",
    "bind_trust_store",
    "bind_cloudevents_serializer",
    "bind_repositories",
    "bind_clients",
    "bind_clients_from",
    "bind_peers",
    "bind_websocket_adapter",
    "bind_sse_adapter",
    "bind_mcp_adapter",
    "bind_skill_adapter",
    # enable_*
    "enable_policy_authorizer",
    "enable_token_revocation",
    "enable_redis_token_revocation",
    "enable_feature_flags",
    "enable_tenant_membership",
    "enable_authorization_audit",
    # mount_*
    "mount_tenant_admin",
    "mount_reliability_admin",
    "mount_webhook_admin",
    # install_*
    "install_cache_metrics",
    "install_reliability_metrics",
    "install_middleware_stack",
    "install_cors",
    "install_rls_tenant_hook",
    # modules
    "CoreModule",
    "ObservabilityModule",
    "LifecycleModule",
    "WebhookModule",
    "TrustStoreModule",
    "InMemoryProfile",
    "SAModule",
    "BeanieModule",
    "KafkaModule",
    "NatsModule",
    "RedisModule",
    "MemcachedModule",
    "WsModule",
    "CasbinModule",
    "FastAPIModule",
    "MigrationsModule",
    "TenancyModule",
    "ReliabilityModule",
    "RetentionModule",
    "JwksRefreshModule",
    "SecurityPostureModule",
    "MiddlewareModule",
    "CorsModule",
    "SecurityHeadersModule",
    "BodyLimitModule",
    "ErrorEnvelopeModule",
    "TracingModule",
    "LoggingModule",
    "MetricsModule",
    "ProfilingModule",
    "RateLimitModule",
    "LocalizationModule",
    # configurations
    "OtelConfiguration",
    "SAConfiguration",
    "BeanieConfiguration",
    "VarcoFastAPIConfiguration",
    "RedisCacheConfiguration",
    "MemcachedCacheConfiguration",
]


def test_entry_points_snapshot_covers_the_taxonomy() -> None:
    data = json.loads(_SNAPSHOT.read_text(encoding="utf-8"))
    text = json.dumps(data)

    missing = [n for n in _EXPECTED_NAMES if f'"{n}"' not in text]
    assert missing == []
    bootstrap_rows = [r for r in _iter_rows(data) if r.get("family") == "bootstrap"]
    async_rows = [r for r in _iter_rows(data) if r.get("family") == "async_bootstrap"]
    assert len(bootstrap_rows) == 10
    assert sorted(r["package"] for r in async_rows) == ["varco_memcached", "varco_redis"]


def _iter_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict) and "entry_points" in data:
        data = data["entry_points"]
    if isinstance(data, dict):
        return [r for v in data.values() if isinstance(v, list) for r in v]
    return list(data)
