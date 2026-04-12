"""
varco_core.connection
=====================
Structured, env-var loadable connection and SSL/auth configuration for all
varco backends (Postgres, Redis, Kafka, HTTP).

Public API
----------
::

    from varco_core.connection import (
        SSLConfig,
        BasicAuthConfig,
        OAuth2Config,
        SaslConfig,
        ConnectionSettings,
    )

``SSLConfig``
    Immutable TLS/SSL configuration.  Embeds as a nested pydantic model in
    any ``ConnectionSettings`` subclass, populated from
    ``{PREFIX}SSL__CA_CERT``, ``{PREFIX}SSL__VERIFY``, etc.

``BasicAuthConfig``
    Username/password authentication (HTTP Basic, Redis AUTH, DB credentials).

``OAuth2Config``
    Static Bearer token or OAuth2 client-credentials flow configuration.

``SaslConfig``
    Kafka/LDAP SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI).

``ConnectionSettings``
    Pydantic BaseSettings base class for per-backend connection configs.
    Subclasses add ``env_prefix`` + backend-specific fields.

Backend-specific subclasses (in their own packages)
----------------------------------------------------
- ``varco_sa.connection.PostgresConnectionSettings``   — Postgres / asyncpg
- ``varco_redis.connection.RedisConnectionSettings``   — Redis / redis.asyncio
- ``varco_kafka.connection.KafkaConnectionSettings``   — Kafka / aiokafka
- ``varco_fastapi.connection.HttpConnectionSettings``  — HTTP / httpx
"""

from varco_core.connection.auth import BasicAuthConfig, OAuth2Config, SaslConfig
from varco_core.connection.base import ConnectionSettings
from varco_core.connection.ssl import SSLConfig

__all__ = [
    "SSLConfig",
    "BasicAuthConfig",
    "OAuth2Config",
    "SaslConfig",
    "ConnectionSettings",
]
