"""
varco_kafka.connection
======================
``KafkaConnectionSettings`` — structured, env-var loadable configuration for a
Kafka connection via aiokafka.

Environment variables (prefix ``KAFKA_``)
-----------------------------------------
::

    KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092
    KAFKA_GROUP_ID=my-service

    # TLS (optional — SASL_SSL or SSL)
    KAFKA_SSL__CA_CERT=/etc/ssl/kafka-ca.pem
    KAFKA_SSL__CLIENT_CERT=/etc/ssl/client.crt
    KAFKA_SSL__CLIENT_KEY=/etc/ssl/client.key

    # SASL authentication (optional)
    KAFKA_AUTH__TYPE=sasl
    KAFKA_AUTH__MECHANISM=SCRAM-SHA-256
    KAFKA_AUTH__USERNAME=alice
    KAFKA_AUTH__PASSWORD=secret

    # Or Basic auth (mapped to SASL PLAIN)
    KAFKA_AUTH__TYPE=basic
    KAFKA_AUTH__USERNAME=alice
    KAFKA_AUTH__PASSWORD=secret

Construction patterns::

    # From env (production)
    conn = KafkaConnectionSettings.from_env()

    # Explicit
    conn = KafkaConnectionSettings(
        bootstrap_servers="broker1:9092,broker2:9092",
        group_id="my-service",
    )

    # Build aiokafka kwargs
    producer_kwargs = conn.to_aiokafka_kwargs()
    producer = AIOKafkaProducer(**producer_kwargs)

DESIGN: Separate from KafkaEventBusSettings
    ✅ KafkaConnectionSettings handles the connection/security layer.
    ✅ KafkaEventBusSettings remains unchanged for event bus configuration.
    ✅ Both coexist; they serve different concerns.
    ❌ Two Kafka config classes exist — users choose the appropriate one.

Thread safety:  ✅ frozen=True — immutable after construction.
Async safety:   ✅ No I/O; all methods are synchronous.

📚 Docs
- 🔍 https://aiokafka.readthedocs.io/en/stable/api.html
  aiokafka — AIOKafkaProducer/Consumer security_protocol, sasl_*, ssl_context
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import Field, model_validator
from pydantic_settings import SettingsConfigDict

from varco_core.connection.auth import BasicAuthConfig, SaslConfig
from varco_core.connection.base import ConnectionSettings


# ── KafkaConnectionSettings ───────────────────────────────────────────────────


class KafkaConnectionSettings(ConnectionSettings):
    """
    Immutable Kafka connection configuration.

    Reads from environment variables with the ``KAFKA_`` prefix.
    Nested SSL and auth config use the ``__`` delimiter::

        KAFKA_BOOTSTRAP_SERVERS=broker:9092
        KAFKA_SSL__CA_CERT=/etc/ssl/ca.pem
        KAFKA_AUTH__TYPE=sasl
        KAFKA_AUTH__MECHANISM=SCRAM-SHA-256

    Attributes:
        host:              Single broker hostname (used to synthesise
                           ``bootstrap_servers`` when the latter is not set).
                           Env: ``KAFKA_HOST``.
        port:              Single broker port.  Env: ``KAFKA_PORT``.
        bootstrap_servers: Comma-separated list of broker addresses.
                           Takes precedence over ``host``/``port``.
                           Env: ``KAFKA_BOOTSTRAP_SERVERS``.
        group_id:          Consumer group ID.  Env: ``KAFKA_GROUP_ID``.
        ssl:               Optional ``SSLConfig`` for TLS.
                           Populated from ``KAFKA_SSL__*`` env vars.
        auth:              Optional SASL or Basic auth config.
                           Populated from ``KAFKA_AUTH__*`` env vars.
                           ``BasicAuthConfig`` is treated as SASL PLAIN.

    Thread safety:  ✅ frozen=True — immutable.
    Async safety:   ✅ No I/O.

    Edge cases:
        - ``bootstrap_servers`` defaults to ``"localhost:9092"``.  When only
          ``host`` and ``port`` are changed from defaults, a validator synthesises
          ``bootstrap_servers`` from them.  This means you can configure a single
          broker via ``KAFKA_HOST=my-broker`` without also setting
          ``KAFKA_BOOTSTRAP_SERVERS``.
        - When ``auth`` is ``BasicAuthConfig``, it is mapped to SASL PLAIN in
          ``to_aiokafka_kwargs()``.
        - ``security_protocol`` depends on the combination of ``ssl`` and ``auth``:
          no ssl/auth → ``PLAINTEXT``; ssl only → ``SSL``;
          auth only → ``SASL_PLAINTEXT``; both → ``SASL_SSL``.
    """

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        env_nested_delimiter="__",
        frozen=True,
    )

    port: int = 9092
    """Single broker port.  Used to synthesise ``bootstrap_servers``.  Env: ``KAFKA_PORT``."""

    bootstrap_servers: str = "localhost:9092"
    """
    Comma-separated broker addresses.  Overrides ``host``/``port`` synthesis.
    Env: ``KAFKA_BOOTSTRAP_SERVERS``.
    """

    group_id: str = "varco-default"
    """Consumer group ID.  Env: ``KAFKA_GROUP_ID``."""

    auth: Annotated[
        SaslConfig | BasicAuthConfig | None, Field(discriminator="type")
    ] = None
    """
    Optional SASL or Basic authentication config.
    ``BasicAuthConfig`` is treated as SASL PLAIN in ``to_aiokafka_kwargs()``.
    Populated from ``KAFKA_AUTH__TYPE``, ``KAFKA_AUTH__MECHANISM``, etc.
    """

    # ── Validator ─────────────────────────────────────────────────────────────

    @model_validator(mode="after")
    def _synthesise_bootstrap_servers(self) -> "KafkaConnectionSettings":
        """
        When ``bootstrap_servers`` is the default value and ``host``/``port`` were
        explicitly set to non-default values, synthesise ``bootstrap_servers``.

        This allows single-broker configuration via ``KAFKA_HOST=my-broker`` +
        ``KAFKA_PORT=9093`` without requiring ``KAFKA_BOOTSTRAP_SERVERS``.

        DESIGN: model_validator(mode="after") on a frozen model requires
        object.__setattr__ to mutate before the frozen lock is applied.
        """
        _default_bs = "localhost:9092"
        _default_host = "localhost"
        _default_port = 9092

        bs_is_default = self.bootstrap_servers == _default_bs
        host_changed = self.host != _default_host
        port_changed = self.port != _default_port

        if bs_is_default and (host_changed or port_changed):
            # Use object.__setattr__ because the model is frozen.
            object.__setattr__(self, "bootstrap_servers", f"{self.host}:{self.port}")
        return self

    # ── Conversion methods ────────────────────────────────────────────────────

    def to_aiokafka_kwargs(self) -> dict[str, Any]:
        """
        Build kwargs for ``AIOKafkaProducer``/``AIOKafkaConsumer``.

        The returned dict is ready to unpack into the aiokafka constructor::

            producer = AIOKafkaProducer(**conn.to_aiokafka_kwargs())
            consumer = AIOKafkaConsumer("my-topic", **conn.to_aiokafka_kwargs())

        ``security_protocol`` is derived from the ssl/auth combination:

        +--------+-------+---------------------------+
        | ssl    | auth  | security_protocol         |
        +========+=======+===========================+
        | None   | None  | PLAINTEXT                 |
        +--------+-------+---------------------------+
        | set    | None  | SSL                       |
        +--------+-------+---------------------------+
        | None   | set   | SASL_PLAINTEXT            |
        +--------+-------+---------------------------+
        | set    | set   | SASL_SSL                  |
        +--------+-------+---------------------------+

        Returns:
            Dict of aiokafka-compatible kwargs.

        Edge cases:
            - ``BasicAuthConfig`` is mapped to SASL PLAIN: mechanism becomes
              ``"PLAIN"``, username/password forwarded via ``sasl_plain_*`` keys.
            - ``SaslConfig.to_aiokafka_kwargs()`` may include ``None`` values for
              GSSAPI — callers can filter these out if the driver rejects them.
        """
        has_ssl = self.ssl is not None
        has_auth = self.auth is not None

        if has_ssl and has_auth:
            security_protocol = "SASL_SSL"
        elif has_ssl:
            security_protocol = "SSL"
        elif has_auth:
            security_protocol = "SASL_PLAINTEXT"
        else:
            security_protocol = "PLAINTEXT"

        kwargs: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": security_protocol,
        }

        if self.ssl is not None:
            kwargs["ssl_context"] = self.ssl.build_ssl_context()

        if self.auth is not None:
            if isinstance(self.auth, BasicAuthConfig):
                # BasicAuth maps to SASL PLAIN
                kwargs["sasl_mechanism"] = "PLAIN"
                kwargs["sasl_plain_username"] = self.auth.username
                kwargs["sasl_plain_password"] = self.auth.password
            else:
                # SaslConfig — merge aiokafka-named kwargs
                kwargs.update(self.auth.to_aiokafka_kwargs())

        return kwargs


__all__ = ["KafkaConnectionSettings"]
