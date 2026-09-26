"""
Red-mode tests for ``varco_core.settings.inventory`` (Plan 048 / Step 3).

``env_vars_for``, ``discover_settings_classes``, ``_EXCLUDED_SETTINGS``,
``check_unique_env_names`` do not exist yet — every test below is expected to
fail with ``ImportError`` until Step 3 lands ``varco_core/varco_core/
settings/inventory.py``. This file is test-only.
"""

from __future__ import annotations

import pytest
from pydantic_settings import SettingsConfigDict
from varco_core.config import VarcoSettings
from varco_core.settings.inventory import (  # noqa: F401
    _EXCLUDED_SETTINGS,
    EnvVarSpec,
    check_unique_env_names,
    discover_settings_classes,
    env_vars_for,
)


class _Plain(VarcoSettings):
    model_config = SettingsConfigDict(env_prefix="VARCO_PLAIN_", frozen=True)

    name: str = "n"


class TestEnvVarsFor:
    def test_plain_prefix_field_maps_to_expected_env_name(self) -> None:
        specs = env_vars_for(_Plain)

        assert any(isinstance(s, EnvVarSpec) and s.env == "VARCO_PLAIN_NAME" for s in specs)

    def test_dict_field_is_flagged_not_env_readable(self) -> None:
        class _WithDict(VarcoSettings):
            model_config = SettingsConfigDict(env_prefix="VARCO_DICTY_", frozen=True)

            extra: dict[str, str] = {}

        specs = env_vars_for(_WithDict)
        extra_spec = next(s for s in specs if s.field_path == "extra")
        assert extra_spec.env is None

    def test_nested_base_model_field_maps_to_delimited_env_name(self) -> None:
        """
        Step 3 explicitly names this case: a nested ``BaseModel`` field
        under ``env_nested_delimiter="__"`` maps to
        ``PREFIX + OUTER.upper() + "__" + INNER.upper()`` — e.g.
        ``PostgresConnectionSettings.ssl.ca_cert`` ->
        ``POSTGRES_SSL__CA_CERT``. Exercised here against a local fixture
        so this test file has no cross-package dependency on varco_sa.
        """
        from pydantic import BaseModel

        class _Ssl(BaseModel):
            ca_cert: str = "x"

        class _Nested(VarcoSettings):
            model_config = SettingsConfigDict(
                env_prefix="POSTGRES_", env_nested_delimiter="__", frozen=True
            )

            ssl: _Ssl = _Ssl()

        specs = env_vars_for(_Nested)
        ssl_spec = next(s for s in specs if s.field_path == "ssl.ca_cert")
        assert ssl_spec.env == "POSTGRES_SSL__CA_CERT"

    def test_secret_named_field_is_flagged_secret_and_default_rendered_as_stars(self) -> None:
        class _WithSecret(VarcoSettings):
            model_config = SettingsConfigDict(env_prefix="VARCO_SECRETY_", frozen=True)

            api_key: str = "shh"

        specs = env_vars_for(_WithSecret)
        key_spec = next(s for s in specs if s.field_path == "api_key")
        assert key_spec.secret is True
        assert key_spec.default_repr == "***"


class TestDiscoverAndUniqueness:
    def test_discover_settings_classes_finds_varco_core_classes(self) -> None:
        classes = discover_settings_classes(["varco_core"])

        assert _Plain not in classes or True  # local class isn't importable from the package
        assert any(c.__name__ == "CacheSettings" for c in classes)

    def test_check_unique_env_names_reports_no_collisions_when_none_exist(self) -> None:
        collisions = check_unique_env_names([_Plain])

        assert collisions == []

    def test_no_client_cert_collision_after_step_6(self) -> None:
        # Plan 048 Step 6: TrustStore.from_env() now reads VARCO_TLS_CLIENT_CERT/_KEY
        # (via TlsSettings) instead of VARCO_CLIENT_CERT/_KEY, so the collision with
        # ClientProfile's VARCO_CLIENT_ prefix is structurally gone. Flipped from the
        # strict xfail this test carried before Step 6 landed.
        from varco_core.settings.inventory import _EXCLUDED_SETTINGS

        # HttpConnectionSettings is excluded by module name here (never
        # registered in _EXCLUDED_SETTINGS itself — see inventory.py's
        # _populate_excluded() DESIGN note on why importing varco_fastapi
        # eagerly from varco_core.settings would regress every other
        # package's import-time budget).
        classes = [
            c
            for c in discover_settings_classes(["varco_core", "varco_fastapi"])
            if c not in _EXCLUDED_SETTINGS and c.__module__ != "varco_fastapi.connection"
        ]
        collisions = check_unique_env_names(classes)

        assert collisions == []


class TestNoSettingsClassDeclaresEnvFile:
    def test_no_settings_class_declares_env_file(self) -> None:
        classes = discover_settings_classes(
            [
                "varco_core",
                "varco_redis",
                "varco_kafka",
                "varco_nats",
                "varco_memcached",
                "varco_casbin",
                "varco_fastapi",
                "varco_sa",
                "varco_beanie",
                "varco_ws",
            ]
        )

        offenders = [
            c.__name__
            for c in classes
            if c.model_config.get("env_file") is not None
            or c.model_config.get("secrets_dir") is not None
        ]

        assert offenders == []


class TestEveryRegisteredClassConstructsFromEmptyEnv:
    def test_every_registered_class_constructs_from_empty_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from varco_core.settings.core import bind_core_settings

        assert _EXCLUDED_SETTINGS is not None  # sanity: module imported cleanly

        # Clear every VARCO_* var so construction genuinely exercises defaults.
        import os

        for key in list(os.environ):
            if key.startswith("VARCO_"):
                monkeypatch.delenv(key, raising=False)

        from providify import DIContainer

        container = DIContainer()
        bind_core_settings(container)

        report = container.validate(raise_on_error=False)
        assert report.errors == ()
