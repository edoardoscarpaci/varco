"""
example.connections
===================
Ready-to-use connection factories for every backend used in the example app,
built on top of the structured ``*ConnectionSettings`` classes from ``varco_core``
and the individual backend packages.

The structured settings (``PostgresConnectionSettings``, ``RedisConnectionSettings``,
etc.) replace raw URL strings with typed, env-var loadable, SSL/auth-aware config
objects.  This module wraps them into one-call factories so the rest of the app
doesn't need to import and call ``to_*()`` methods directly.

Quick reference — env vars read by each factory
------------------------------------------------
::

    # PostgreSQL (make_engine / make_sa_config)
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432
    POSTGRES_DATABASE=varco
    POSTGRES_USERNAME=varco
    POSTGRES_PASSWORD=varco
    POSTGRES_POOL_SIZE=5
    POSTGRES_MAX_OVERFLOW=10
    POSTGRES_SSL__CA_CERT=/etc/ssl/pg-ca.pem   # optional TLS
    POSTGRES_SSL__VERIFY=true

    # Redis (make_redis_client)
    REDIS_HOST=localhost
    REDIS_PORT=6379
    REDIS_DB=0
    REDIS_PASSWORD=                             # optional AUTH
    REDIS_SSL__CA_CERT=/etc/ssl/redis-ca.pem   # optional TLS

    # Kafka (make_kafka_producer / make_kafka_consumer)
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092
    KAFKA_GROUP_ID=varco-example
    KAFKA_AUTH__TYPE=sasl                       # optional SASL
    KAFKA_AUTH__MECHANISM=SCRAM-SHA-256
    KAFKA_AUTH__USERNAME=alice
    KAFKA_AUTH__PASSWORD=secret
    KAFKA_SSL__CA_CERT=/etc/ssl/kafka-ca.pem   # optional TLS

    # External HTTP clients — prefix is per-client (not a fixed "HTTP_")
    PAYMENT_API_BASE_URL=https://pay.example.com/v1
    PAYMENT_API_TIMEOUT=5.0
    PAYMENT_API_AUTH__TYPE=basic
    PAYMENT_API_AUTH__USERNAME=svc-user
    PAYMENT_API_AUTH__PASSWORD=secret

Usage::

    # In app bootstrap (sync, lazy — engine not created until first use)
    container.provide(make_di_sa_provider(Base, Post))

    # Direct engine creation (async-safe, used in scripts / tests)
    engine = make_engine()

    # Redis client
    redis_client = make_redis_client()

    # Kafka producer (ASYNC — must be awaited before sending)
    producer = make_kafka_producer()
    await producer.start()

    # External HTTP client (ASYNC context manager)
    async with make_http_client(prefix="PAYMENT_API_") as client:
        resp = await client.get("/charge")

DESIGN: factory functions over instantiating settings in each module
    ✅ Single import point — callers don't need to know which settings class
       to use or which to_*() method to call.
    ✅ Defaults sensible for the Docker Compose dev stack — all factories work
       with ``docker compose up`` without any extra configuration.
    ✅ SSL/auth activated by setting the relevant env vars — zero code change
       needed when promoting from dev to production.
    ❌ Factories read env vars at call time, not import time — if the env is
       not configured, the error is deferred until the factory is called.
       This is intentional (lazy) but can be surprising during debugging.

Thread safety:  ✅ All factories are stateless — safe to call from any thread.
Async safety:   ✅ ``make_engine()`` and ``make_redis_client()`` return sync
                objects; aiokafka and httpx objects are async but construction
                is sync — start/connect is the async step.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import redis.asyncio

from sqlalchemy.ext.asyncio import create_async_engine

from varco_fastapi.connection import HttpConnectionSettings
from varco_kafka.connection import KafkaConnectionSettings
from varco_redis.connection import RedisConnectionSettings
from varco_sa.connection import PostgresConnectionSettings

if TYPE_CHECKING:
    import httpx
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from sqlalchemy.ext.asyncio import AsyncEngine
    from varco_sa.config import SAConfig


# ── PostgreSQL ────────────────────────────────────────────────────────────────


def make_postgres_settings() -> PostgresConnectionSettings:
    """
    Load ``PostgresConnectionSettings`` from ``POSTGRES_*`` env vars.

    Returns:
        Fully populated, immutable ``PostgresConnectionSettings`` instance.

    Edge cases:
        - All fields have defaults matching the Docker Compose dev stack — the
          factory works without any env vars for local development.
        - Set ``POSTGRES_SSL__CA_CERT`` (and optionally other ``POSTGRES_SSL__*``
          vars) to enable TLS without changing this code.
    """
    return PostgresConnectionSettings.from_env()


def make_engine(*, echo: bool = False) -> "AsyncEngine":
    """
    Build a SQLAlchemy ``AsyncEngine`` from ``POSTGRES_*`` env vars.

    Reads connection settings (host, port, database, username, password, SSL)
    via ``PostgresConnectionSettings.from_env()``, then passes the DSN and
    pool/connect-args to ``create_async_engine``.

    Args:
        echo: Forward ``echo=True`` to SQLAlchemy for verbose SQL logging.
              Useful in development; keep ``False`` in production.

    Returns:
        A configured ``AsyncEngine`` instance.  The engine is lazy — no
        connection is opened until the first query runs.

    Edge cases:
        - TLS is activated by setting ``POSTGRES_SSL__CA_CERT`` — no code change
          needed; the SSL context is built and injected into ``connect_args``.
        - To use a raw ``DATABASE_URL`` string instead, call
          ``create_async_engine(os.environ["DATABASE_URL"])`` directly.

    Example::

        engine = make_engine(echo=True)      # dev — logs SQL
        engine = make_engine()               # prod — silent
        # Both use POSTGRES_* env vars.

    Thread safety:  ✅ ``AsyncEngine`` is thread-safe after creation.
    Async safety:   ✅ Construction is sync; I/O happens at first query.
    """
    conn = make_postgres_settings()
    # to_engine_kwargs() returns pool_size, max_overflow, pool_timeout, and
    # connect_args (which carries the ssl context when POSTGRES_SSL__* is set).
    return create_async_engine(
        conn.to_sqlalchemy_url(), echo=echo, **conn.to_engine_kwargs()
    )


def make_di_sa_provider(
    base: "type[Any]",
    *entity_classes: "type[Any]",
    echo: bool = False,
) -> Any:
    """
    Build a DI ``@Provider`` factory that creates an ``SAConfig`` using
    ``PostgresConnectionSettings`` instead of a raw ``DATABASE_URL`` env var.

    Drop-in replacement for ``make_sa_provider(Base, Post)`` from
    ``varco_sa.bootstrap``.  Use this when you want SSL/auth configuration
    via ``POSTGRES_SSL__*`` / ``POSTGRES_AUTH__*`` env vars rather than
    encoding credentials into the ``DATABASE_URL`` string.

    Args:
        base:            Shared ``DeclarativeBase`` subclass.
        *entity_classes: ``DomainModel`` subclasses to register.
        echo:            Verbose SQL logging (development only).

    Returns:
        A ``@Provider(singleton=True)``-decorated factory ready for
        ``container.provide(make_di_sa_provider(Base, Post))``.

    Edge cases:
        - Like ``make_sa_provider``, the provider is LAZY — settings are not
          read until the first DI resolution of ``SAConfig``.
        - Both ``make_sa_provider`` (uses DATABASE_URL) and this function
          (uses POSTGRES_*) produce an identical ``SAConfig`` — pick whichever
          env-var convention fits your deployment.

    Example::

        # In app bootstrap:
        container.provide(make_di_sa_provider(Base, Post))

        # Equivalent to (but structured):
        container.provide(make_sa_provider(Base, Post, env_var="DATABASE_URL"))

    Thread safety:  ✅ Provider is called once (singleton).
    Async safety:   ✅ Factory is synchronous.
    """
    from providify import (
        Provider,
    )  # noqa: PLC0415 — deferred to avoid hard dep at import

    from varco_sa.config import SAConfig  # noqa: PLC0415
    from sqlalchemy.ext.asyncio import async_sessionmaker  # noqa: PLC0415

    # Capture entity_classes and echo in the closure at call time.
    # If entity_classes were consumed lazily, late additions would be missed.
    _entity_classes = tuple(entity_classes)
    _echo = echo

    @Provider(singleton=True)
    def _sa_config_provider() -> "SAConfig":
        """DI provider: builds SAConfig from POSTGRES_* env vars."""
        engine = make_engine(echo=_echo)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        return SAConfig(
            engine=engine,
            base=base,
            entity_classes=_entity_classes,
            session_factory=session_factory,
        )

    return _sa_config_provider


# ── Redis ─────────────────────────────────────────────────────────────────────


def make_redis_settings() -> RedisConnectionSettings:
    """
    Load ``RedisConnectionSettings`` from ``REDIS_*`` env vars.

    Returns:
        Fully populated, immutable ``RedisConnectionSettings`` instance.

    Edge cases:
        - Defaults match the Docker Compose dev stack (``localhost:6379/0``).
        - Set ``REDIS_SSL__CA_CERT`` to enable TLS without changing code.
        - ``REDIS_PASSWORD`` / ``REDIS_USERNAME`` activate AUTH/ACL credentials.
    """
    return RedisConnectionSettings.from_env()


def make_redis_client() -> redis.asyncio.Redis:
    """
    Build a ``redis.asyncio.Redis`` client from ``REDIS_*`` env vars.

    The client connects lazily — no network I/O happens until the first
    command is issued.

    Returns:
        A configured ``redis.asyncio.Redis`` instance.

    Edge cases:
        - TLS is activated by ``REDIS_SSL__CA_CERT`` — the ``rediss://`` scheme
          and SSL context are set automatically.
        - The URL is built from individual ``REDIS_*`` fields, not a raw
          ``REDIS_URL`` string.  This makes SSL and auth composable from env vars
          without URL-encoding credentials manually.

    Example::

        client = make_redis_client()
        await client.set("key", "value")
        value = await client.get("key")
        await client.aclose()

        # Or as a context manager:
        async with make_redis_client() as client:
            await client.ping()

    Thread safety:  ✅ ``redis.asyncio.Redis`` is safe to share across async tasks.
    Async safety:   ✅ Construction is sync; commands are async.
    """
    conn = make_redis_settings()
    # from_url() accepts the URL for scheme/host/port/db and to_redis_kwargs()
    # carries ssl context, decode_responses, and socket_timeout.
    return redis.asyncio.from_url(conn.to_url(), **conn.to_redis_kwargs())


# ── Kafka ─────────────────────────────────────────────────────────────────────


def make_kafka_settings() -> KafkaConnectionSettings:
    """
    Load ``KafkaConnectionSettings`` from ``KAFKA_*`` env vars.

    Returns:
        Fully populated, immutable ``KafkaConnectionSettings`` instance.

    Edge cases:
        - Defaults to ``localhost:9092`` (plaintext).
        - Set ``KAFKA_AUTH__TYPE=sasl`` + credentials for SASL auth.
        - Set ``KAFKA_SSL__CA_CERT`` for TLS — combined with SASL it becomes
          ``SASL_SSL`` automatically in ``to_aiokafka_kwargs()``.
    """
    return KafkaConnectionSettings.from_env()


def make_kafka_producer() -> "AIOKafkaProducer":
    """
    Build an ``AIOKafkaProducer`` from ``KAFKA_*`` env vars.

    The producer is NOT started here — call ``await producer.start()`` (or use
    it as an async context manager) before sending any messages.

    Returns:
        A configured but unstarted ``AIOKafkaProducer``.

    Edge cases:
        - ``security_protocol`` (PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL)
          is derived automatically from the SSL and auth settings.
        - ``group_id`` is NOT set on the producer — it is a consumer-only concept.
          ``to_aiokafka_kwargs()`` still includes ``bootstrap_servers``.

    Example::

        producer = make_kafka_producer()
        await producer.start()
        try:
            await producer.send_and_wait("orders", b'{"order_id": "abc"}')
        finally:
            await producer.stop()

        # Or as an async context manager:
        async with make_kafka_producer() as producer:
            await producer.send_and_wait("orders", b"...")

    Thread safety:  ✅ ``AIOKafkaProducer`` is safe to use from one event loop.
    Async safety:   ✅ Construction is sync; ``start()`` and ``send()`` are async.
    """
    from aiokafka import AIOKafkaProducer  # noqa: PLC0415 — optional dep

    conn = make_kafka_settings()
    kwargs = conn.to_aiokafka_kwargs()
    # group_id is a consumer concept — remove it from the producer kwargs to
    # avoid the "unexpected keyword argument" error from aiokafka.
    kwargs.pop("group_id", None)
    return AIOKafkaProducer(**kwargs)


def make_kafka_consumer(*topics: str) -> "AIOKafkaConsumer":
    """
    Build an ``AIOKafkaConsumer`` subscribed to the given topics, from
    ``KAFKA_*`` env vars.

    The consumer is NOT started here — call ``await consumer.start()`` (or use
    it as an async context manager) before polling messages.

    Args:
        *topics: One or more Kafka topic names to subscribe to.

    Returns:
        A configured but unstarted ``AIOKafkaConsumer``.

    Edge cases:
        - ``group_id`` defaults to ``"varco-example"`` (``KAFKA_GROUP_ID``).
          Override via env var to avoid offset conflicts between services.
        - Consumer auto-commit is the aiokafka default (``enable_auto_commit=True``).

    Example::

        consumer = make_kafka_consumer("orders", "payments")
        await consumer.start()
        try:
            async for msg in consumer:
                print(msg.value)
        finally:
            await consumer.stop()

    Thread safety:  ✅ AIOKafkaConsumer is safe to use from one event loop.
    Async safety:   ✅ Construction is sync; ``start()`` and iteration are async.
    """
    from aiokafka import AIOKafkaConsumer  # noqa: PLC0415 — optional dep

    conn = make_kafka_settings()
    return AIOKafkaConsumer(*topics, **conn.to_aiokafka_kwargs())


# ── HTTP (external REST clients) ──────────────────────────────────────────────
#
# HTTP clients are different from the other backends: a single service typically
# calls many external APIs, each with its own prefix.  Pass a per-client prefix
# when loading from env.
#
# Example env layout:
#   PAYMENT_API_BASE_URL=https://pay.example.com/v1
#   PAYMENT_API_TIMEOUT=5.0
#   PAYMENT_API_AUTH__TYPE=basic
#   PAYMENT_API_AUTH__USERNAME=svc-user
#   PAYMENT_API_AUTH__PASSWORD=secret
#
#   NOTIF_API_BASE_URL=https://notify.example.com
#   NOTIF_API_SSL__CA_CERT=/etc/ssl/notify-ca.pem


def make_http_settings(prefix: str) -> HttpConnectionSettings:
    """
    Load ``HttpConnectionSettings`` from env vars with the given prefix.

    Args:
        prefix: Env-var prefix including the trailing underscore, e.g.
                ``"PAYMENT_API_"``.

    Returns:
        Fully populated, immutable ``HttpConnectionSettings`` instance.

    Raises:
        ValueError: If ``prefix`` is empty.

    Example::

        payment_settings = make_http_settings("PAYMENT_API_")
        # reads PAYMENT_API_BASE_URL, PAYMENT_API_TIMEOUT, etc.
    """
    return HttpConnectionSettings.from_env(prefix=prefix)


def make_http_client(prefix: str) -> "httpx.AsyncClient":
    """
    Build an ``httpx.AsyncClient`` from env vars with the given prefix.

    The client is ready to use immediately — no ``start()`` call needed.
    Use it as an async context manager to ensure the connection pool is closed::

        async with make_http_client("PAYMENT_API_") as client:
            resp = await client.post("/charge", json={"amount": 9.99})

    Args:
        prefix: Env-var prefix including the trailing underscore, e.g.
                ``"PAYMENT_API_"``.

    Returns:
        A configured ``httpx.AsyncClient``.

    Edge cases:
        - ``BasicAuthConfig`` auth is injected automatically as
          ``auth=(username, password)``.
        - ``OAuth2Config`` auth is NOT injected — add an ``Authorization``
          header in the request or use an httpx event hook for token refresh.
        - TLS verification is controlled by ``{PREFIX}SSL__VERIFY``.
          ``verify=false`` disables all cert checks (dev/testing only).

    Example::

        async with make_http_client("PAYMENT_API_") as client:
            resp = await client.get("/health")
            print(resp.status_code)

    Thread safety:  ✅ ``httpx.AsyncClient`` is safe to share across async tasks.
    Async safety:   ✅ Construction is sync; requests are async.
    """
    import httpx  # noqa: PLC0415 — optional dep

    conn = make_http_settings(prefix=prefix)
    return httpx.AsyncClient(**conn.to_httpx_kwargs())


__all__ = [
    # Postgres
    "make_postgres_settings",
    "make_engine",
    "make_di_sa_provider",
    # Redis
    "make_redis_settings",
    "make_redis_client",
    # Kafka
    "make_kafka_settings",
    "make_kafka_producer",
    "make_kafka_consumer",
    # HTTP
    "make_http_settings",
    "make_http_client",
]
