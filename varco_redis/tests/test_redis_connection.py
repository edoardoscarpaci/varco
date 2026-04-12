"""
Tests for varco_redis.connection.RedisConnectionSettings.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from varco_core.connection.ssl import SSLConfig
from varco_redis.connection import RedisConnectionSettings


class TestRedisConnectionSettings:
    def test_defaults(self) -> None:
        conn = RedisConnectionSettings.model_validate({})
        assert conn.host == "localhost"
        assert conn.port == 6379
        assert conn.db == 0
        assert conn.password is None
        assert conn.username is None
        assert conn.decode_responses is False
        assert conn.socket_timeout is None
        assert conn.ssl is None

    def test_to_url_no_ssl_no_auth(self) -> None:
        conn = RedisConnectionSettings.model_validate({})
        assert conn.to_url() == "redis://localhost:6379/0"

    def test_to_url_with_ssl(self) -> None:
        conn = RedisConnectionSettings.model_validate(
            {"ssl": {"verify": False, "check_hostname": False}}
        )
        assert conn.to_url().startswith("rediss://")

    def test_to_url_with_password_only(self) -> None:
        conn = RedisConnectionSettings.model_validate({"password": "secret"})
        assert ":secret@" in conn.to_url()

    def test_to_url_with_username_and_password(self) -> None:
        conn = RedisConnectionSettings.model_validate(
            {"username": "alice", "password": "secret"}
        )
        url = conn.to_url()
        assert "alice:secret@" in url

    def test_to_url_db_index(self) -> None:
        conn = RedisConnectionSettings.model_validate({"db": 3})
        assert conn.to_url().endswith("/3")

    def test_to_redis_kwargs_no_ssl(self) -> None:
        conn = RedisConnectionSettings.model_validate({})
        kwargs = conn.to_redis_kwargs()
        assert kwargs["decode_responses"] is False
        assert "ssl" not in kwargs
        assert "socket_timeout" not in kwargs

    def test_to_redis_kwargs_with_socket_timeout(self) -> None:
        conn = RedisConnectionSettings.model_validate({"socket_timeout": 5.0})
        kwargs = conn.to_redis_kwargs()
        assert kwargs["socket_timeout"] == 5.0

    def test_to_redis_kwargs_with_ssl(self) -> None:
        conn = RedisConnectionSettings.model_validate(
            {"ssl": {"verify": False, "check_hostname": False}}
        )
        kwargs = conn.to_redis_kwargs()
        import ssl as _ssl

        assert isinstance(kwargs["ssl"], _ssl.SSLContext)

    def test_with_ssl_factory(self) -> None:
        ssl_cfg = SSLConfig(verify=False, check_hostname=False)
        conn = RedisConnectionSettings.with_ssl(ssl_cfg, host="redis-prod", db=2)
        assert conn.host == "redis-prod"
        assert conn.db == 2
        assert conn.ssl is not None

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REDIS_HOST", "my-redis")
        monkeypatch.setenv("REDIS_PORT", "6380")
        monkeypatch.setenv("REDIS_DB", "1")
        conn = RedisConnectionSettings.from_env()
        assert conn.host == "my-redis"
        assert conn.port == 6380
        assert conn.db == 1

    def test_nested_ssl_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REDIS_SSL__CA_CERT", "/tmp/redis-ca.pem")
        conn = RedisConnectionSettings.from_env()
        assert conn.ssl is not None
        assert conn.ssl.ca_cert == Path("/tmp/redis-ca.pem")

    def test_env_prefix(self) -> None:
        assert RedisConnectionSettings.env_prefix() == "REDIS_"

    def test_frozen(self) -> None:
        conn = RedisConnectionSettings.model_validate({})
        with pytest.raises((TypeError, ValidationError)):
            conn.host = "other"  # type: ignore[misc]
