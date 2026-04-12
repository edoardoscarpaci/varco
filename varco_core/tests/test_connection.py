"""
Tests for varco_core.connection — SSLConfig, BasicAuthConfig, OAuth2Config,
SaslConfig, and ConnectionSettings base.

All tests are synchronous (no async needed — connection configs have no I/O).
"""

from __future__ import annotations

import ssl
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from varco_core.connection.auth import BasicAuthConfig, OAuth2Config, SaslConfig
from varco_core.connection.base import ConnectionSettings
from varco_core.connection.ssl import SSLConfig
from pydantic_settings import SettingsConfigDict


# ══════════════════════════════════════════════════════════════════════════════
# SSLConfig
# ══════════════════════════════════════════════════════════════════════════════


class TestSSLConfig:
    def test_defaults(self) -> None:
        cfg = SSLConfig()
        assert cfg.ca_cert is None
        assert cfg.ca_folder is None
        assert cfg.client_cert is None
        assert cfg.client_key is None
        assert cfg.verify is True
        assert cfg.check_hostname is True

    def test_frozen(self) -> None:
        cfg = SSLConfig()
        with pytest.raises((TypeError, ValidationError)):
            cfg.verify = False  # type: ignore[misc]

    def test_validator_check_hostname_requires_verify(self) -> None:
        with pytest.raises(ValidationError, match="check_hostname"):
            SSLConfig(verify=False, check_hostname=True)

    def test_validator_partial_mtls_cert_only(self) -> None:
        with pytest.raises(ValidationError, match="both be set"):
            SSLConfig(client_cert=Path("/etc/ssl/client.crt"))

    def test_validator_partial_mtls_key_only(self) -> None:
        with pytest.raises(ValidationError, match="both be set"):
            SSLConfig(client_key=Path("/etc/ssl/client.key"))

    def test_verify_false_no_hostname(self) -> None:
        cfg = SSLConfig(verify=False, check_hostname=False)
        ctx = cfg.build_ssl_context()
        assert ctx.verify_mode == ssl.CERT_NONE
        assert not ctx.check_hostname

    def test_build_ssl_context_default(self) -> None:
        cfg = SSLConfig()
        ctx = cfg.build_ssl_context()
        # Default context from system CAs — verify_mode should be CERT_REQUIRED
        assert ctx.verify_mode == ssl.CERT_REQUIRED

    def test_build_ssl_context_with_ca_cert(self) -> None:
        # Write a minimal self-signed CA cert (Python stdlib includes a test cert)
        # We use a tmpdir + a real PEM to avoid network dependency.
        cert_source = Path(ssl.__file__).parent / "test" / "root_cert.pem"
        if not cert_source.exists():
            pytest.skip("stdlib SSL test cert not available in this environment")
        cfg = SSLConfig(ca_cert=cert_source)
        ctx = cfg.build_ssl_context()
        assert ctx.verify_mode == ssl.CERT_REQUIRED

    def test_build_ssl_context_with_ca_folder(self) -> None:
        # Create a tmpdir with a fake .pem extension (ssl.load_verify_locations
        # validates PEM format — we test that the glob logic runs correctly).
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_source = Path(ssl.__file__).parent / "test" / "root_cert.pem"
            if not cert_source.exists():
                pytest.skip("stdlib SSL test cert not available in this environment")
            import shutil

            shutil.copy(cert_source, Path(tmpdir) / "ca.pem")
            cfg = SSLConfig(ca_folder=Path(tmpdir))
            ctx = cfg.build_ssl_context()
            assert ctx.verify_mode == ssl.CERT_REQUIRED

    def test_from_env_reads_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_SSL__CA_CERT", "/tmp/ca.pem")
        monkeypatch.setenv("TEST_SSL__VERIFY", "true")
        monkeypatch.setenv("TEST_SSL__CHECK_HOSTNAME", "true")
        cfg = SSLConfig.from_env("TEST_")
        assert cfg.ca_cert == Path("/tmp/ca.pem")
        assert cfg.verify is True
        assert cfg.check_hostname is True

    def test_from_env_false_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_SSL__VERIFY", "false")
        monkeypatch.setenv("TEST_SSL__CHECK_HOSTNAME", "false")
        cfg = SSLConfig.from_env("TEST_")
        assert cfg.verify is False
        assert cfg.check_hostname is False

    def test_from_env_empty_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SSL__CA_CERT", "/path/to/ca.pem")
        cfg = SSLConfig.from_env("")
        assert cfg.ca_cert == Path("/path/to/ca.pem")

    def test_model_dump_round_trip(self) -> None:
        cfg = SSLConfig(
            ca_cert=Path("/etc/ssl/ca.pem"),
            verify=True,
            check_hostname=True,
        )
        data = cfg.model_dump()
        restored = SSLConfig.model_validate(data)
        assert restored == cfg


# ══════════════════════════════════════════════════════════════════════════════
# BasicAuthConfig
# ══════════════════════════════════════════════════════════════════════════════


class TestBasicAuthConfig:
    def test_to_header_known_value(self) -> None:
        auth = BasicAuthConfig(username="admin", password="s3cret")
        # echo -n "admin:s3cret" | base64 → YWRtaW46czNjcmV0
        assert auth.to_header() == "Basic YWRtaW46czNjcmV0"

    def test_to_tuple(self) -> None:
        auth = BasicAuthConfig(username="user", password="pass")
        assert auth.to_tuple() == ("user", "pass")

    def test_frozen(self) -> None:
        auth = BasicAuthConfig(username="u", password="p")
        with pytest.raises((TypeError, ValidationError)):
            auth.username = "other"  # type: ignore[misc]

    def test_type_discriminator(self) -> None:
        auth = BasicAuthConfig(username="u", password="p")
        assert auth.type == "basic"

    def test_empty_credentials_allowed(self) -> None:
        # Some backends accept empty password
        auth = BasicAuthConfig(username="user", password="")
        assert auth.password == ""


# ══════════════════════════════════════════════════════════════════════════════
# OAuth2Config
# ══════════════════════════════════════════════════════════════════════════════


class TestOAuth2Config:
    def test_static_token(self) -> None:
        auth = OAuth2Config(token="tok_abc123")
        assert auth.to_bearer_header() == "Bearer tok_abc123"

    def test_type_discriminator(self) -> None:
        auth = OAuth2Config(token="tok")
        assert auth.type == "oauth2"

    def test_no_credentials_raises(self) -> None:
        with pytest.raises(ValidationError, match="must provide either"):
            OAuth2Config()

    def test_client_credentials_complete(self) -> None:
        auth = OAuth2Config(
            token_url="https://auth.example.com/token",
            client_id="svc-a",
            client_secret="secret",
            scope="read:all",
        )
        assert auth.to_bearer_header() is None  # no static token
        assert auth.token_url == "https://auth.example.com/token"

    def test_partial_client_credentials_raises(self) -> None:
        with pytest.raises(ValidationError, match="partial client credentials"):
            OAuth2Config(token_url="https://auth.example.com/token")

    def test_partial_client_creds_missing_secret(self) -> None:
        with pytest.raises(ValidationError, match="partial client credentials"):
            OAuth2Config(
                token_url="https://auth.example.com/token",
                client_id="svc-a",
            )

    def test_static_token_takes_precedence(self) -> None:
        auth = OAuth2Config(
            token="existing-tok",
            token_url="https://auth.example.com/token",
            client_id="svc-a",
            client_secret="secret",
        )
        assert auth.to_bearer_header() == "Bearer existing-tok"

    def test_frozen(self) -> None:
        auth = OAuth2Config(token="tok")
        with pytest.raises((TypeError, ValidationError)):
            auth.token = "other"  # type: ignore[misc]


# ══════════════════════════════════════════════════════════════════════════════
# SaslConfig
# ══════════════════════════════════════════════════════════════════════════════


class TestSaslConfig:
    def test_plain_kwargs(self) -> None:
        sasl = SaslConfig(mechanism="PLAIN", username="alice", password="secret")
        kwargs = sasl.to_aiokafka_kwargs()
        assert kwargs == {
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "alice",
            "sasl_plain_password": "secret",
        }

    def test_scram_sha256_kwargs(self) -> None:
        sasl = SaslConfig(mechanism="SCRAM-SHA-256", username="bob", password="pw")
        kwargs = sasl.to_aiokafka_kwargs()
        assert kwargs["sasl_mechanism"] == "SCRAM-SHA-256"

    def test_gssapi_no_creds(self) -> None:
        sasl = SaslConfig(mechanism="GSSAPI")
        kwargs = sasl.to_aiokafka_kwargs()
        assert kwargs["sasl_mechanism"] == "GSSAPI"
        assert kwargs["sasl_plain_username"] is None
        assert kwargs["sasl_plain_password"] is None

    def test_type_discriminator(self) -> None:
        sasl = SaslConfig()
        assert sasl.type == "sasl"

    def test_frozen(self) -> None:
        sasl = SaslConfig(mechanism="PLAIN", username="u", password="p")
        with pytest.raises((TypeError, ValidationError)):
            sasl.mechanism = "SCRAM"  # type: ignore[misc]


# ══════════════════════════════════════════════════════════════════════════════
# ConnectionSettings
# ══════════════════════════════════════════════════════════════════════════════


# Minimal concrete subclass for testing the base
class _TestConnection(ConnectionSettings):
    model_config = SettingsConfigDict(
        env_prefix="TEST_CONN_",
        env_nested_delimiter="__",
        frozen=True,
    )
    port: int = 1234


class TestConnectionSettings:
    def test_env_prefix_classmethod(self) -> None:
        assert _TestConnection.env_prefix() == "TEST_CONN_"

    def test_defaults(self) -> None:
        conn = _TestConnection.model_validate({})
        assert conn.host == "localhost"
        assert conn.port == 1234
        assert conn.ssl is None

    def test_with_ssl_factory(self) -> None:
        ssl_cfg = SSLConfig(verify=False, check_hostname=False)
        conn = _TestConnection.with_ssl(ssl_cfg, host="myhost", port=9999)
        assert conn.host == "myhost"
        assert conn.port == 9999
        assert conn.ssl is not None
        assert conn.ssl.verify is False

    def test_with_ssl_does_not_read_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """with_ssl() must NOT pick up env vars — only the explicit kwargs."""
        monkeypatch.setenv("TEST_CONN_HOST", "env-host")
        ssl_cfg = SSLConfig(verify=False, check_hostname=False)
        # model_validate bypasses env sources — host should be "explicit", not "env-host"
        conn = _TestConnection.with_ssl(ssl_cfg, host="explicit")
        assert conn.host == "explicit"

    def test_from_env_reads_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_CONN_HOST", "env-host-value")
        conn = _TestConnection.from_env()
        assert conn.host == "env-host-value"

    def test_nested_ssl_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_CONN_SSL__VERIFY", "false")
        monkeypatch.setenv("TEST_CONN_SSL__CHECK_HOSTNAME", "false")
        conn = _TestConnection.from_env()
        assert conn.ssl is not None
        assert conn.ssl.verify is False

    def test_frozen_after_construction(self) -> None:
        conn = _TestConnection.model_validate({})
        with pytest.raises((TypeError, ValidationError)):
            conn.host = "other"  # type: ignore[misc]
