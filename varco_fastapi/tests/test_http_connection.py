"""
Tests for varco_fastapi.connection.HttpConnectionSettings
and the TrustStore.to_ssl_config() bridge.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from varco_core.connection.ssl import SSLConfig
from varco_fastapi.auth.trust_store import TrustStore
from varco_fastapi.connection import HttpConnectionSettings


# ══════════════════════════════════════════════════════════════════════════════
# HttpConnectionSettings
# ══════════════════════════════════════════════════════════════════════════════


class TestHttpConnectionSettings:
    def test_defaults(self) -> None:
        conn = HttpConnectionSettings.model_validate({})
        assert conn.host == "localhost"
        assert conn.port == 443
        assert conn.base_url == ""
        assert conn.timeout == 30.0
        assert conn.ssl is None
        assert conn.auth is None

    def test_effective_base_url_from_base_url_field(self) -> None:
        conn = HttpConnectionSettings.model_validate(
            {"base_url": "https://api.example.com/v1"}
        )
        assert conn._effective_base_url() == "https://api.example.com/v1"

    def test_effective_base_url_from_host_port_no_ssl(self) -> None:
        conn = HttpConnectionSettings.model_validate(
            {"host": "api.example.com", "port": 8080}
        )
        assert conn._effective_base_url() == "http://api.example.com:8080"

    def test_effective_base_url_from_host_port_with_ssl(self) -> None:
        conn = HttpConnectionSettings.model_validate(
            {
                "host": "api.example.com",
                "port": 443,
                "ssl": {"verify": False, "check_hostname": False},
            }
        )
        assert conn._effective_base_url() == "https://api.example.com:443"

    def test_to_httpx_kwargs_no_auth_no_ssl(self) -> None:
        conn = HttpConnectionSettings.model_validate(
            {"base_url": "https://api.example.com"}
        )
        kwargs = conn.to_httpx_kwargs()
        assert kwargs["base_url"] == "https://api.example.com"
        assert kwargs["timeout"] == 30.0
        assert "verify" not in kwargs
        assert "auth" not in kwargs

    def test_to_httpx_kwargs_basic_auth(self) -> None:
        conn = HttpConnectionSettings.model_validate(
            {
                "base_url": "https://api.example.com",
                "auth": {"type": "basic", "username": "user", "password": "pass"},
            }
        )
        kwargs = conn.to_httpx_kwargs()
        assert kwargs["auth"] == ("user", "pass")

    def test_to_httpx_kwargs_oauth2_no_auth_key(self) -> None:
        conn = HttpConnectionSettings.model_validate(
            {
                "base_url": "https://api.example.com",
                "auth": {"type": "oauth2", "token": "tok123"},
            }
        )
        kwargs = conn.to_httpx_kwargs()
        # OAuth2 static token: caller must handle Authorization header manually
        assert "auth" not in kwargs

    def test_to_httpx_kwargs_ssl_verify_false(self) -> None:
        conn = HttpConnectionSettings.model_validate(
            {
                "base_url": "https://api.example.com",
                "ssl": {"verify": False, "check_hostname": False},
            }
        )
        kwargs = conn.to_httpx_kwargs()
        assert kwargs["verify"] is False

    def test_to_httpx_kwargs_ssl_verify_true(self) -> None:
        conn = HttpConnectionSettings.model_validate(
            {
                "base_url": "https://api.example.com",
                "ssl": {"verify": True},
            }
        )
        kwargs = conn.to_httpx_kwargs()
        import ssl as _ssl

        assert isinstance(kwargs["verify"], _ssl.SSLContext)

    def test_to_trust_store_no_ssl(self) -> None:
        conn = HttpConnectionSettings.model_validate({})
        assert conn.to_trust_store() is None

    def test_to_trust_store_with_ssl(self) -> None:
        conn = HttpConnectionSettings.model_validate(
            {"ssl": {"ca_cert": "/tmp/ca.pem", "verify": True}}
        )
        ts = conn.to_trust_store()
        assert ts is not None
        assert isinstance(ts, TrustStore)
        assert ts.ca_cert == Path("/tmp/ca.pem")

    def test_with_ssl_factory(self) -> None:
        ssl_cfg = SSLConfig(verify=False, check_hostname=False)
        conn = HttpConnectionSettings.with_ssl(
            ssl_cfg, base_url="https://api.example.com"
        )
        assert conn.ssl is not None
        assert conn.ssl.verify is False

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # prefix is caller-supplied — use a realistic service-scoped prefix.
        monkeypatch.setenv("PAYMENT_API_BASE_URL", "https://env-api.example.com")
        monkeypatch.setenv("PAYMENT_API_TIMEOUT", "15.0")
        conn = HttpConnectionSettings.from_env(prefix="PAYMENT_API_")
        assert conn.base_url == "https://env-api.example.com"
        assert conn.timeout == 15.0

    def test_from_env_requires_non_empty_prefix(self) -> None:
        # An empty prefix would silently read from the global env namespace
        # with no scoping — it must raise immediately to prevent misconfiguration.
        with pytest.raises(ValueError, match="requires a non-empty prefix"):
            HttpConnectionSettings.from_env(prefix="")

    def test_from_env_multiple_clients_independent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Two HTTP clients with different prefixes must not bleed into each other —
        # the key feature that motivates removing the fixed HTTP_ prefix.
        monkeypatch.setenv("SVC_A_BASE_URL", "https://a.example.com")
        monkeypatch.setenv("SVC_B_BASE_URL", "https://b.example.com")
        conn_a = HttpConnectionSettings.from_env(prefix="SVC_A_")
        conn_b = HttpConnectionSettings.from_env(prefix="SVC_B_")
        assert conn_a.base_url == "https://a.example.com"
        assert conn_b.base_url == "https://b.example.com"

    def test_nested_ssl_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PAYMENT_API_SSL__CA_CERT", "/tmp/http-ca.pem")
        monkeypatch.setenv("PAYMENT_API_SSL__VERIFY", "true")
        conn = HttpConnectionSettings.from_env(prefix="PAYMENT_API_")
        assert conn.ssl is not None
        assert conn.ssl.ca_cert == Path("/tmp/http-ca.pem")

    def test_env_prefix_is_empty_on_base_class(self) -> None:
        # HttpConnectionSettings has no fixed prefix — it is always supplied
        # at from_env() call time.  The base class env_prefix() returns "".
        assert HttpConnectionSettings.env_prefix() == ""

    def test_from_env_result_is_http_connection_settings_instance(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The dynamic subclass created inside from_env() must be transparently
        # usable wherever HttpConnectionSettings is expected.
        monkeypatch.setenv("MY_SVC_BASE_URL", "https://svc.example.com")
        conn = HttpConnectionSettings.from_env(prefix="MY_SVC_")
        assert isinstance(conn, HttpConnectionSettings)

    def test_frozen(self) -> None:
        conn = HttpConnectionSettings.model_validate({})
        with pytest.raises((TypeError, ValidationError)):
            conn.timeout = 10.0  # type: ignore[misc]


# ══════════════════════════════════════════════════════════════════════════════
# TrustStore.to_ssl_config() bridge
# ══════════════════════════════════════════════════════════════════════════════


class TestTrustStoreToSslConfig:
    def test_path_ca_cert_preserved(self) -> None:
        ts = TrustStore(ca_cert=Path("/etc/ssl/ca.pem"))
        ssl_cfg = ts.to_ssl_config()
        assert ssl_cfg.ca_cert == Path("/etc/ssl/ca.pem")

    def test_bytes_ca_cert_becomes_none(self) -> None:
        ts = TrustStore(ca_cert=b"-----BEGIN CERTIFICATE-----\n...")
        ssl_cfg = ts.to_ssl_config()
        # bytes cannot be represented as a Path — should be None
        assert ssl_cfg.ca_cert is None

    def test_ca_folder_preserved(self) -> None:
        ts = TrustStore(ca_folder=Path("/etc/ssl/certs"))
        ssl_cfg = ts.to_ssl_config()
        assert ssl_cfg.ca_folder == Path("/etc/ssl/certs")

    def test_mtls_fields_preserved(self) -> None:
        ts = TrustStore(
            client_cert=Path("/etc/ssl/client.crt"),
            client_key=Path("/etc/ssl/client.key"),
        )
        ssl_cfg = ts.to_ssl_config()
        assert ssl_cfg.client_cert == Path("/etc/ssl/client.crt")
        assert ssl_cfg.client_key == Path("/etc/ssl/client.key")

    def test_verify_always_true(self) -> None:
        ts = TrustStore()
        ssl_cfg = ts.to_ssl_config()
        assert ssl_cfg.verify is True
        assert ssl_cfg.check_hostname is True

    def test_round_trip_path_ca(self) -> None:
        ts = TrustStore(ca_cert=Path("/etc/ssl/ca.pem"))
        ssl_cfg = ts.to_ssl_config()
        # Build a new TrustStore from the SSLConfig fields
        ts2 = TrustStore(
            ca_cert=ssl_cfg.ca_cert,
            ca_folder=ssl_cfg.ca_folder,
            client_cert=ssl_cfg.client_cert,
            client_key=ssl_cfg.client_key,
        )
        assert ts2.ca_cert == ts.ca_cert
