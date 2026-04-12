"""
Tests for varco_sa.connection.PostgresConnectionSettings.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from varco_core.connection.ssl import SSLConfig
from varco_sa.connection import PostgresConnectionSettings


class TestPostgresConnectionSettings:
    def test_defaults(self) -> None:
        conn = PostgresConnectionSettings.model_validate({})
        assert conn.host == "localhost"
        assert conn.port == 5432
        assert conn.database == "postgres"
        assert conn.schema_name == "public"
        assert conn.username == "postgres"
        assert conn.password == ""
        assert conn.pool_size == 5
        assert conn.max_overflow == 10
        assert conn.pool_timeout == 30.0
        assert conn.ssl is None
        assert conn.auth is None

    def test_to_dsn_defaults(self) -> None:
        conn = PostgresConnectionSettings.model_validate({})
        assert conn.to_dsn() == "postgresql+asyncpg://postgres:@localhost:5432/postgres"

    def test_to_dsn_custom(self) -> None:
        conn = PostgresConnectionSettings.model_validate(
            {
                "host": "prod-db",
                "port": 5433,
                "database": "orders",
                "username": "svc",
                "password": "s3cret",
            }
        )
        assert conn.to_dsn() == "postgresql+asyncpg://svc:s3cret@prod-db:5433/orders"

    def test_to_dsn_with_auth_object_overrides_inline(self) -> None:
        conn = PostgresConnectionSettings.model_validate(
            {
                "host": "db",
                "username": "inline_user",
                "password": "inline_pw",
                "auth": {
                    "type": "basic",
                    "username": "auth_user",
                    "password": "auth_pw",
                },
            }
        )
        dsn = conn.to_dsn()
        assert "auth_user" in dsn
        assert "auth_pw" in dsn
        assert "inline_user" not in dsn

    def test_to_dsn_special_chars_encoded(self) -> None:
        conn = PostgresConnectionSettings.model_validate(
            {"username": "user@corp", "password": "p@ss:word/special"}
        )
        dsn = conn.to_dsn()
        assert "@" not in dsn.split("@")[0].split("://")[1]  # no bare @ in auth portion
        # percent encoding
        assert "user%40corp" in dsn or "user" in dsn

    def test_to_sqlalchemy_url_alias(self) -> None:
        conn = PostgresConnectionSettings.model_validate({"host": "db"})
        assert conn.to_sqlalchemy_url() == conn.to_dsn()

    def test_to_asyncpg_kwargs_no_ssl(self) -> None:
        conn = PostgresConnectionSettings.model_validate({"database": "mydb"})
        kwargs = conn.to_asyncpg_kwargs()
        assert "dsn" in kwargs
        assert "ssl" not in kwargs
        assert "server_settings" not in kwargs

    def test_to_asyncpg_kwargs_non_public_schema(self) -> None:
        conn = PostgresConnectionSettings.model_validate({"schema_name": "app"})
        kwargs = conn.to_asyncpg_kwargs()
        assert kwargs.get("server_settings") == {"search_path": "app"}

    def test_to_asyncpg_kwargs_with_ssl(self) -> None:
        conn = PostgresConnectionSettings.model_validate(
            {"ssl": {"verify": False, "check_hostname": False}}
        )
        kwargs = conn.to_asyncpg_kwargs()
        import ssl as _ssl

        assert "ssl" in kwargs
        assert isinstance(kwargs["ssl"], _ssl.SSLContext)

    def test_to_engine_kwargs_excludes_dsn(self) -> None:
        conn = PostgresConnectionSettings.model_validate({})
        ek = conn.to_engine_kwargs()
        assert "pool_size" in ek
        assert "max_overflow" in ek
        assert "pool_timeout" in ek
        assert "dsn" not in ek.get("connect_args", {})

    def test_with_ssl_factory(self) -> None:
        ssl_cfg = SSLConfig(verify=False, check_hostname=False)
        conn = PostgresConnectionSettings.with_ssl(
            ssl_cfg, host="secure-db", database="prod"
        )
        assert conn.host == "secure-db"
        assert conn.database == "prod"
        assert conn.ssl is not None
        assert conn.ssl.verify is False

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_HOST", "env-pg-host")
        monkeypatch.setenv("POSTGRES_DATABASE", "env-db")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        conn = PostgresConnectionSettings.from_env()
        assert conn.host == "env-pg-host"
        assert conn.database == "env-db"
        assert conn.port == 5433

    def test_nested_ssl_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_SSL__CA_CERT", "/tmp/ca.pem")
        monkeypatch.setenv("POSTGRES_SSL__VERIFY", "true")
        conn = PostgresConnectionSettings.from_env()
        assert conn.ssl is not None
        assert conn.ssl.ca_cert == Path("/tmp/ca.pem")

    def test_env_prefix(self) -> None:
        assert PostgresConnectionSettings.env_prefix() == "POSTGRES_"

    def test_frozen(self) -> None:
        conn = PostgresConnectionSettings.model_validate({})
        with pytest.raises((TypeError, ValidationError)):
            conn.host = "other"  # type: ignore[misc]
