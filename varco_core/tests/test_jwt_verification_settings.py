"""
Unit tests for JwtVerificationSettings' revocation fields (Plan 034 / S13a,
Step 20/21) — one settings class, not a second one.
"""

from __future__ import annotations


class TestRevocationSettingsDefaults:
    def test_defaults_are_byte_identical_pre_034(self, monkeypatch):
        # Existing fields must be unaffected by the new ones.
        monkeypatch.delenv("VARCO_JWT_LEEWAY_SECONDS", raising=False)
        monkeypatch.delenv("VARCO_JWT_ENFORCE_ISS", raising=False)
        monkeypatch.delenv("VARCO_JWT_ALLOW_ANY_AUDIENCE", raising=False)
        from varco_core.jwt.config import JwtVerificationSettings

        settings = JwtVerificationSettings.from_env()
        assert settings.leeway_seconds == 0.0
        assert settings.enforce_issuer is True
        assert settings.allow_any_audience is False

    def test_revocation_failure_mode_defaults_to_fail_closed(self):
        from varco_core.jwt.config import JwtVerificationSettings
        from varco_core.revocation import RevocationFailureMode

        settings = JwtVerificationSettings.from_env()
        assert settings.revocation_failure_mode == RevocationFailureMode.FAIL_CLOSED

    def test_revocation_require_jti_defaults_false(self):
        from varco_core.jwt.config import JwtVerificationSettings

        assert JwtVerificationSettings.from_env().revocation_require_jti is False

    def test_revocation_skew_seconds_defaults_to_60(self):
        from varco_core.jwt.config import JwtVerificationSettings

        assert JwtVerificationSettings.from_env().revocation_skew_seconds == 60.0

    def test_revocation_enabled_defaults_true(self):
        from varco_core.jwt.config import JwtVerificationSettings

        assert JwtVerificationSettings.from_env().revocation_enabled is True


class TestRevocationSettingsEnvParsing:
    def test_failure_mode_env_var_parses_case_insensitively(self, monkeypatch):
        from varco_core.jwt.config import JwtVerificationSettings
        from varco_core.revocation import RevocationFailureMode

        monkeypatch.setenv("VARCO_JWT_REVOCATION_FAILURE_MODE", "FAIL_OPEN")
        settings = JwtVerificationSettings.from_env()
        assert settings.revocation_failure_mode == RevocationFailureMode.FAIL_OPEN

    def test_require_jti_env_var_parses(self, monkeypatch):
        from varco_core.jwt.config import JwtVerificationSettings

        monkeypatch.setenv("VARCO_JWT_REVOCATION_REQUIRE_JTI", "true")
        assert JwtVerificationSettings.from_env().revocation_require_jti is True

    def test_skew_seconds_env_var_parses(self, monkeypatch):
        from varco_core.jwt.config import JwtVerificationSettings

        monkeypatch.setenv("VARCO_JWT_REVOCATION_SKEW_SECONDS", "120")
        assert JwtVerificationSettings.from_env().revocation_skew_seconds == 120.0

    def test_enabled_env_var_parses(self, monkeypatch):
        from varco_core.jwt.config import JwtVerificationSettings

        monkeypatch.setenv("VARCO_JWT_REVOCATION_ENABLED", "false")
        assert JwtVerificationSettings.from_env().revocation_enabled is False
