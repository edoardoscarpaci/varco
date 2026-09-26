"""
Red-mode tests for ``varco_core.settings.bind_settings()`` (Plan 048 / Step 1).

These tests encode the recipe's full contract from plans/048-settings-
standardization.md §D-provider/§D-eager/§D-precedence/§D-named, cases
(a)-(l), plus the "no scanned markers" invariant for the package itself.

None of ``varco_core.settings.bind`` exists yet — every test here is expected
to fail with ``ImportError`` (or a collection error) until Step 2 lands the
package. This file is test-only; production code (``varco_core/varco_core/
settings/``) is not touched here.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from pydantic_settings import SettingsConfigDict
from varco_core.config import VarcoSettings
from varco_core.settings import (  # noqa: F401  (import is the red-mode probe)
    CommaSeparated,
    SettingsAlreadyBoundError,
    SettingsValidationError,
    bind_settings,
    provenance_of,
)


class _Demo(VarcoSettings):
    """Local fixture settings class used only by this test module."""

    model_config = SettingsConfigDict(env_prefix="VARCO_DEMO_", frozen=True)

    url: str = "x"
    port: int = 1
    enabled: bool = False
    origins: CommaSeparated = ()


@pytest.fixture
def demo_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure no stray VARCO_DEMO_* leaks in from the real environment."""
    for name in ("VARCO_DEMO_URL", "VARCO_DEMO_PORT", "VARCO_DEMO_ENABLED", "VARCO_DEMO_ORIGINS"):
        monkeypatch.delenv(name, raising=False)


class TestBindSettingsBasics:
    def test_a_returns_instance_and_container_get_agrees(
        self, di_container: DIContainer, demo_env: None
    ) -> None:
        result = bind_settings(di_container, _Demo)

        assert isinstance(result, _Demo)
        assert di_container.get(_Demo) is result

    def test_b_env_wins_over_default(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch, demo_env: None
    ) -> None:
        monkeypatch.setenv("VARCO_DEMO_PORT", "7")

        result = bind_settings(di_container, _Demo)

        assert result.port == 7

    def test_c_instance_kwargs_win_per_field_env_still_applies_to_others(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch, demo_env: None
    ) -> None:
        monkeypatch.setenv("VARCO_DEMO_PORT", "7")
        monkeypatch.setenv("VARCO_DEMO_URL", "from-env")

        result = bind_settings(di_container, _Demo, instance=_Demo(port=9))

        assert result.port == 9
        assert result.url == "from-env"

    def test_d_code_override_registered_before_bind_settings_never_reads_env(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch, demo_env: None
    ) -> None:
        # A value that would raise ValidationError if pydantic ever parsed it.
        monkeypatch.setenv("VARCO_DEMO_PORT", "notanint")

        user_instance = _Demo.model_construct(url="x", port=1, enabled=False, origins=())
        di_container.provide(lambda: user_instance, returns=_Demo)

        result = bind_settings(di_container, _Demo)

        assert result is user_instance

    def test_e_bad_env_raises_settings_validation_error_naming_var_and_class_not_value(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch, demo_env: None
    ) -> None:
        monkeypatch.setenv("VARCO_DEMO_PORT", "abc")

        with pytest.raises(SettingsValidationError) as exc:
            bind_settings(di_container, _Demo)

        message = str(exc.value)
        assert "VARCO_DEMO_PORT" in message
        assert "_Demo" in message
        assert "abc" not in message

    def test_f_binding_twice_is_idempotent_exactly_one_binding(
        self, di_container: DIContainer, demo_env: None
    ) -> None:
        first = bind_settings(di_container, _Demo)
        second = bind_settings(di_container, _Demo)

        assert first is second
        assert len(di_container.get_all_bindings(_Demo)) == 1

    def test_g_instance_bound_twice_raises_settings_already_bound_error(
        self, di_container: DIContainer, demo_env: None
    ) -> None:
        bind_settings(di_container, _Demo, instance=_Demo(port=1))

        with pytest.raises(SettingsAlreadyBoundError):
            bind_settings(di_container, _Demo, instance=_Demo(port=2))

    def test_h_env_prefix_without_qualifier_raises_value_error(
        self, di_container: DIContainer, demo_env: None
    ) -> None:
        with pytest.raises(ValueError):
            bind_settings(di_container, _Demo, env_prefix="VARCO_DEMO_B_")

    def test_i_qualified_binding_is_independent_of_unqualified(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch, demo_env: None
    ) -> None:
        monkeypatch.delenv("VARCO_DEMO_B_PORT", raising=False)
        monkeypatch.setenv("VARCO_DEMO_B_PORT", "3")

        bind_settings(di_container, _Demo)
        bind_settings(di_container, _Demo, qualifier="b", env_prefix="VARCO_DEMO_B_")

        assert di_container.get(_Demo, qualifier="b").port == 3
        assert di_container.get(_Demo).port == 1

    def test_j_validate_reports_no_fallback_shadowed_without_user_override(
        self, di_container: DIContainer, demo_env: None
    ) -> None:
        bind_settings(di_container, _Demo)

        report = di_container.validate(raise_on_error=False)

        from providify import IssueKind

        assert IssueKind.FALLBACK_SHADOWED not in {i.kind for i in report.infos}

    def test_j_validate_reports_exactly_one_fallback_shadowed_with_user_override(
        self, di_container: DIContainer, demo_env: None
    ) -> None:
        di_container.provide(lambda: _Demo(port=42), returns=_Demo)
        bind_settings(di_container, _Demo)

        report = di_container.validate(raise_on_error=False)

        from providify import IssueKind, Severity

        shadowed = [i for i in report.infos if i.kind == IssueKind.FALLBACK_SHADOWED]
        assert len(shadowed) == 1
        assert shadowed[0].severity == Severity.INFO

    def test_k_provenance_reports_per_field_source(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch, demo_env: None
    ) -> None:
        monkeypatch.setenv("VARCO_DEMO_PORT", "7")

        bind_settings(di_container, _Demo)

        prov = provenance_of(di_container, _Demo)
        assert prov.fields["url"] == "default"
        assert prov.fields["port"] == "env"

    def test_k_provenance_reports_code_for_instance_set_field(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch, demo_env: None
    ) -> None:
        monkeypatch.setenv("VARCO_DEMO_PORT", "7")

        bind_settings(di_container, _Demo, instance=_Demo(port=9))

        prov = provenance_of(di_container, _Demo)
        assert prov.fields["port"] == "code"

    def test_k_provenance_reports_external_for_pre_existing_user_binding(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch, demo_env: None
    ) -> None:
        monkeypatch.setenv("VARCO_DEMO_PORT", "notanint")
        user_instance = _Demo.model_construct(url="x", port=1, enabled=False, origins=())
        di_container.provide(lambda: user_instance, returns=_Demo)

        bind_settings(di_container, _Demo)

        prov = provenance_of(di_container, _Demo)
        assert prov.source == "external"

    def test_instance_of_wrong_type_raises_type_error_before_any_registration(
        self, di_container: DIContainer, demo_env: None
    ) -> None:
        """
        §Edge cases: "instance= of the wrong type -> TypeError before any
        registration." Asserts no binding is left behind by the rejected
        call, not just that the call itself raises.
        """

        class _NotADemo(VarcoSettings):
            model_config = SettingsConfigDict(env_prefix="VARCO_NOTDEMO_", frozen=True)

        with pytest.raises(TypeError):
            bind_settings(di_container, _Demo, instance=_NotADemo())  # type: ignore[arg-type]

        assert di_container.get_all_bindings(_Demo) == []

    def test_l_comma_separated_field_splits_and_strips(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch, demo_env: None
    ) -> None:
        monkeypatch.setenv("VARCO_DEMO_ORIGINS", "a, b ,c")

        result = bind_settings(di_container, _Demo)

        assert result.origins == ("a", "b", "c")


class TestSettingsPackageHasNoScannedMarkers:
    def test_settings_package_has_no_scanned_markers(self) -> None:
        """
        varco_core.settings must never carry a module-level @Provider/
        @Singleton/@Configuration — container.scan("varco_core",
        recursive=True) is a documented, in-use pattern (the cloudevents/tls
        rule), and a scanned marker here would auto-activate on every scan.
        """
        import ast
        import pathlib

        import varco_core.settings as settings_pkg

        pkg_dir = pathlib.Path(settings_pkg.__file__).parent
        forbidden = {"Provider", "Singleton", "Configuration"}
        offenders: list[str] = []

        for path in pkg_dir.rglob("*.py"):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    continue
                for deco in node.decorator_list:
                    name = deco.func.id if isinstance(deco, ast.Call) else getattr(deco, "id", "")
                    if name in forbidden:
                        offenders.append(f"{path}:{node.lineno} @{name}")

        assert offenders == [], f"scanned marker found in varco_core.settings: {offenders}"


class TestLegacyEnvNameWarns:
    """
    Repair round 1, item 3: §Risks' "renames break deployments silently"
    mitigation — bind_settings() logs one WARNING naming old -> new when a
    4.0.0-renamed env var's *old* name is still set.
    """

    def test_legacy_env_name_warns(
        self,
        di_container: DIContainer,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        demo_env: None,
    ) -> None:
        monkeypatch.setenv("VARCO_CA_CERT", "/tmp/ca.pem")

        with caplog.at_level("WARNING", logger="varco_core.settings.bind"):
            bind_settings(di_container, _Demo)

        assert any(
            "VARCO_CA_CERT" in record.message and "VARCO_TLS_CA_CERT" in record.message
            for record in caplog.records
        )

    def test_legacy_env_name_warns_prefixed(
        self,
        di_container: DIContainer,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        demo_env: None,
    ) -> None:
        monkeypatch.setenv("VARCO_PROFILER_ENABLED", "true")

        with caplog.at_level("WARNING", logger="varco_core.settings.bind"):
            bind_settings(di_container, _Demo)

        assert any(
            "VARCO_PROFILER_ENABLED" in record.message
            and "VARCO_PROFILING_ENABLED" in record.message
            for record in caplog.records
        )

    def test_legacy_env_name_warns_jwks(
        self,
        di_container: DIContainer,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        demo_env: None,
    ) -> None:
        monkeypatch.setenv("VARCO_JWKS_TTL_SECONDS", "300")

        with caplog.at_level("WARNING", logger="varco_core.settings.bind"):
            bind_settings(di_container, _Demo)

        assert any(
            "VARCO_JWKS_TTL_SECONDS" in record.message
            and "VARCO_JWT_JWKS_TTL_SECONDS" in record.message
            for record in caplog.records
        )

    def test_no_legacy_warning_when_no_old_names_set(
        self,
        di_container: DIContainer,
        caplog: pytest.LogCaptureFixture,
        demo_env: None,
    ) -> None:
        with caplog.at_level("WARNING", logger="varco_core.settings.bind"):
            bind_settings(di_container, _Demo)

        assert caplog.records == []

    def test_legacy_warning_logs_once_per_container(
        self,
        di_container: DIContainer,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        demo_env: None,
    ) -> None:
        monkeypatch.setenv("VARCO_CA_CERT", "/tmp/ca.pem")

        with caplog.at_level("WARNING", logger="varco_core.settings.bind"):
            bind_settings(di_container, _Demo)
            bind_settings(di_container, _Demo)

        hits = [r for r in caplog.records if "VARCO_CA_CERT" in r.message]
        assert len(hits) == 1
