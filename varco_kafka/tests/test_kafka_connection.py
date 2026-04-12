"""
Tests for varco_kafka.connection.KafkaConnectionSettings.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from varco_core.connection.ssl import SSLConfig
from varco_kafka.connection import KafkaConnectionSettings


class TestKafkaConnectionSettings:
    def test_defaults(self) -> None:
        conn = KafkaConnectionSettings.model_validate({})
        assert conn.host == "localhost"
        assert conn.port == 9092
        assert conn.bootstrap_servers == "localhost:9092"
        assert conn.group_id == "varco-default"
        assert conn.ssl is None
        assert conn.auth is None

    def test_to_aiokafka_kwargs_plaintext(self) -> None:
        conn = KafkaConnectionSettings.model_validate({})
        kwargs = conn.to_aiokafka_kwargs()
        assert kwargs["security_protocol"] == "PLAINTEXT"
        assert kwargs["bootstrap_servers"] == "localhost:9092"
        assert "ssl_context" not in kwargs

    def test_to_aiokafka_kwargs_ssl_only(self) -> None:
        conn = KafkaConnectionSettings.model_validate(
            {"ssl": {"verify": False, "check_hostname": False}}
        )
        kwargs = conn.to_aiokafka_kwargs()
        assert kwargs["security_protocol"] == "SSL"
        import ssl as _ssl

        assert isinstance(kwargs["ssl_context"], _ssl.SSLContext)

    def test_to_aiokafka_kwargs_sasl_plaintext(self) -> None:
        conn = KafkaConnectionSettings.model_validate(
            {
                "auth": {
                    "type": "sasl",
                    "mechanism": "PLAIN",
                    "username": "u",
                    "password": "p",
                }
            }
        )
        kwargs = conn.to_aiokafka_kwargs()
        assert kwargs["security_protocol"] == "SASL_PLAINTEXT"
        assert kwargs["sasl_mechanism"] == "PLAIN"
        assert kwargs["sasl_plain_username"] == "u"
        assert kwargs["sasl_plain_password"] == "p"

    def test_to_aiokafka_kwargs_sasl_ssl(self) -> None:
        conn = KafkaConnectionSettings.model_validate(
            {
                "ssl": {"verify": False, "check_hostname": False},
                "auth": {
                    "type": "sasl",
                    "mechanism": "SCRAM-SHA-256",
                    "username": "alice",
                    "password": "secret",
                },
            }
        )
        kwargs = conn.to_aiokafka_kwargs()
        assert kwargs["security_protocol"] == "SASL_SSL"
        assert "ssl_context" in kwargs
        assert kwargs["sasl_mechanism"] == "SCRAM-SHA-256"

    def test_basic_auth_mapped_to_sasl_plain(self) -> None:
        conn = KafkaConnectionSettings.model_validate(
            {"auth": {"type": "basic", "username": "bob", "password": "pw"}}
        )
        kwargs = conn.to_aiokafka_kwargs()
        assert kwargs["sasl_mechanism"] == "PLAIN"
        assert kwargs["sasl_plain_username"] == "bob"
        assert kwargs["sasl_plain_password"] == "pw"

    def test_bootstrap_servers_explicit_overrides_host_port(self) -> None:
        conn = KafkaConnectionSettings.model_validate(
            {
                "host": "broker1",
                "port": 9093,
                "bootstrap_servers": "broker1:9093,broker2:9093",
            }
        )
        assert conn.bootstrap_servers == "broker1:9093,broker2:9093"

    def test_bootstrap_servers_synthesised_from_host_port(self) -> None:
        conn = KafkaConnectionSettings.model_validate(
            {"host": "my-broker", "port": 9093}
        )
        assert conn.bootstrap_servers == "my-broker:9093"

    def test_bootstrap_servers_not_synthesised_when_default_host_port(self) -> None:
        conn = KafkaConnectionSettings.model_validate({})
        # host and port are both defaults → bootstrap_servers stays as default
        assert conn.bootstrap_servers == "localhost:9092"

    def test_with_ssl_factory(self) -> None:
        ssl_cfg = SSLConfig(verify=False, check_hostname=False)
        conn = KafkaConnectionSettings.with_ssl(
            ssl_cfg,
            bootstrap_servers="broker:9093",
            group_id="my-svc",
        )
        assert conn.bootstrap_servers == "broker:9093"
        assert conn.ssl is not None

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")
        monkeypatch.setenv("KAFKA_GROUP_ID", "my-service")
        conn = KafkaConnectionSettings.from_env()
        assert conn.bootstrap_servers == "broker1:9092,broker2:9092"
        assert conn.group_id == "my-service"

    def test_nested_ssl_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_SSL__CA_CERT", "/tmp/kafka-ca.pem")
        conn = KafkaConnectionSettings.from_env()
        assert conn.ssl is not None
        assert conn.ssl.ca_cert == Path("/tmp/kafka-ca.pem")

    def test_env_prefix(self) -> None:
        assert KafkaConnectionSettings.env_prefix() == "KAFKA_"

    def test_frozen(self) -> None:
        conn = KafkaConnectionSettings.model_validate({})
        with pytest.raises((TypeError, ValidationError)):
            conn.group_id = "other"  # type: ignore[misc]
