"""
varco_fastapi.auth.trust_store
==============================
TLS trust configuration for client connections.

``TrustStore`` is a frozen dataclass that captures CA certificates and optional
mTLS client identity.  It produces an ``ssl.SSLContext`` for use in httpx clients
(``AsyncVarcoClient``, ``SyncVarcoClient``) and ASGI server startup.

Env vars (read by ``TrustStore.from_env()``)::

    VARCO_TRUST_STORE_DIR   — directory of *.pem / *.crt CA files (all merged)
    VARCO_CA_CERT           — path to a single additional CA PEM file
    VARCO_CLIENT_CERT       — mTLS client certificate path
    VARCO_CLIENT_KEY        — mTLS client private key path

DESIGN: frozen dataclass + build_ssl_context() over raw ssl.SSLContext
    ✅ Testable — TrustStore is a plain data object; no side effects at construction
    ✅ Composable — can be passed to httpx, uvicorn, asyncio.start_server, etc.
    ✅ from_env() reads standard env vars without magic
    ❌ ssl.SSLContext is not easily JSON-serializable — keep it in process memory

Thread safety:  ✅ frozen=True — immutable after construction.
Async safety:   ✅ ``build_ssl_context()`` is synchronous (ssl module is sync).
"""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_core.connection.ssl import SSLConfig


@dataclass(frozen=True)
class TrustStore:
    """
    TLS trust configuration — merges system CAs, env-configured cert folders,
    explicit CA certs, and optional mTLS client identity.

    Attributes:
        ca_cert:           Explicit CA PEM bytes or path to PEM file.
                           Merged with system CAs (and any ``ca_folder`` certs).
        ca_folder:         Directory of ``*.pem`` / ``*.crt`` files to merge
                           with the CA trust chain.  All matching files are loaded.
        client_cert:       Path to mTLS client certificate file.
        client_key:        Path to mTLS client private key file.
        include_system_cas: Whether to include the OS CA bundle.
                            Set ``False`` for strict pinning (private PKI only).

    Thread safety:  ✅ frozen=True — safe to share across threads.
    Async safety:   ✅ ``build_ssl_context()`` is synchronous.

    Edge cases:
        - ``TrustStore()`` (default) → system CA bundle only.  Suitable for most
          production deployments using publicly trusted certificates.
        - ``include_system_cas=False`` with ``ca_cert=None`` and no ``ca_folder``
          creates an empty CA store — all TLS connections will fail.  Use only
          for strict pinning scenarios.
        - ``client_cert`` and ``client_key`` must both be set or both be ``None``.
          Partial mTLS config raises ``ValueError`` in ``build_ssl_context()``.

    Example::

        # Standard (system CAs only)
        ts = TrustStore()
        ctx = ts.build_ssl_context()

        # With a private CA
        ts = TrustStore(ca_cert=Path("/etc/ssl/my-ca.pem"))

        # mTLS
        ts = TrustStore(
            ca_cert=Path("/etc/ssl/my-ca.pem"),
            client_cert=Path("/etc/ssl/client.crt"),
            client_key=Path("/etc/ssl/client.key"),
        )

        # From env vars
        ts = TrustStore.from_env()
    """

    ca_cert: Path | bytes | None = None
    ca_folder: Path | None = None
    client_cert: Path | None = None
    client_key: Path | None = None
    include_system_cas: bool = True

    # ── Factory methods ────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> TrustStore:
        """
        Build a ``TrustStore`` from standard environment variables.

        Reads:
            ``VARCO_TRUST_STORE_DIR``  → ``ca_folder``
            ``VARCO_CA_CERT``          → ``ca_cert`` (file path)
            ``VARCO_CLIENT_CERT``      → ``client_cert``
            ``VARCO_CLIENT_KEY``       → ``client_key``

        Returns:
            A fully populated ``TrustStore``.

        Edge cases:
            - Missing env vars produce ``None`` values — the resulting
              ``TrustStore`` uses system CAs only.
            - Paths are not validated at construction time — ``build_ssl_context()``
              will raise if a path does not exist.
        """
        ca_cert: Path | None = (
            Path(v) if (v := os.environ.get("VARCO_CA_CERT")) else None
        )
        ca_folder: Path | None = (
            Path(v) if (v := os.environ.get("VARCO_TRUST_STORE_DIR")) else None
        )
        client_cert: Path | None = (
            Path(v) if (v := os.environ.get("VARCO_CLIENT_CERT")) else None
        )
        client_key: Path | None = (
            Path(v) if (v := os.environ.get("VARCO_CLIENT_KEY")) else None
        )
        return cls(
            ca_cert=ca_cert,
            ca_folder=ca_folder,
            client_cert=client_cert,
            client_key=client_key,
        )

    def to_ssl_config(self) -> "SSLConfig":
        """
        Convert this ``TrustStore`` to a ``varco_core.connection.SSLConfig``.

        Useful when integrating code that uses the newer ``SSLConfig``-based
        connection abstractions with older ``TrustStore``-based HTTP clients.

        Returns:
            ``SSLConfig`` with equivalent CA/client cert configuration.

        Raises:
            ImportError: If ``varco_core.connection`` is not available.

        Edge cases:
            - ``ca_cert`` as ``bytes`` cannot be expressed as a ``Path`` in
              ``SSLConfig`` — the returned config will have ``ca_cert=None``
              in that case.  The bytes-based CA is not transferred.
            - ``include_system_cas=False`` is not representable in ``SSLConfig``
              (it always uses system CAs when ``verify=True``) — this
              information is lost in the conversion.
            - ``verify=True`` and ``check_hostname=True`` are always set in the
              returned ``SSLConfig`` — ``TrustStore`` always verifies.

        Example::

            ts = TrustStore(ca_cert=Path("/etc/ssl/ca.pem"))
            ssl_cfg = ts.to_ssl_config()
            conn = PostgresConnectionSettings.with_ssl(ssl_cfg, host="my-db")
        """
        # Deferred import to avoid introducing a top-level circular dependency.
        # varco_fastapi already depends on varco_core, so this is fine at runtime.
        from varco_core.connection.ssl import SSLConfig  # noqa: PLC0415

        ca_cert_path: Path | None
        if isinstance(self.ca_cert, Path):
            ca_cert_path = self.ca_cert
        else:
            # bytes ca_cert has no Path representation — cannot round-trip
            ca_cert_path = None

        return SSLConfig(
            ca_cert=ca_cert_path,
            ca_folder=self.ca_folder,
            client_cert=self.client_cert,
            client_key=self.client_key,
            verify=True,
            check_hostname=True,
        )

    @classmethod
    def system(cls) -> TrustStore:
        """
        Return a ``TrustStore`` using only the OS CA bundle.

        This is the default for most deployments that use publicly signed certs.
        Equivalent to ``TrustStore()``.

        Returns:
            A ``TrustStore`` with ``include_system_cas=True`` and no extras.
        """
        return cls()

    # ── SSL context builder ────────────────────────────────────────────────────

    def build_ssl_context(self) -> ssl.SSLContext:
        """
        Build and return an ``ssl.SSLContext`` from this trust configuration.

        Steps:
        1. Create context from system CAs (``ssl.create_default_context()``) or
           blank context (``ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)``) based on
           ``include_system_cas``.
        2. Glob ``ca_folder`` for ``*.pem`` + ``*.crt`` → load each.
        3. Load explicit ``ca_cert`` (``Path`` → file, ``bytes`` → in-memory).
        4. Load ``client_cert`` + ``client_key`` for mTLS.

        Returns:
            A configured ``ssl.SSLContext``.

        Raises:
            ValueError: If only one of ``client_cert`` / ``client_key`` is set.
            FileNotFoundError: If any configured path does not exist.
            ssl.SSLError: If any certificate fails to load.

        Thread safety:  ✅ Creates a new context per call.
        Async safety:   ✅ Synchronous — call at startup, not per-request.
        """
        # Step 1: base context
        if self.include_system_cas:
            ctx = ssl.create_default_context()
        else:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            # Without system CAs, hostname verification still runs but against
            # the explicitly loaded certs only.
            ctx.check_hostname = True
            ctx.verify_mode = ssl.CERT_REQUIRED

        # Step 2: load ca_folder
        if self.ca_folder is not None:
            folder = Path(self.ca_folder)
            for cert_path in sorted(
                list(folder.glob("*.pem")) + list(folder.glob("*.crt"))
            ):
                ctx.load_verify_locations(cafile=str(cert_path))

        # Step 3: load explicit ca_cert
        if self.ca_cert is not None:
            if isinstance(self.ca_cert, bytes):
                ctx.load_verify_locations(cadata=self.ca_cert.decode("utf-8"))
            else:
                ctx.load_verify_locations(cafile=str(self.ca_cert))

        # Step 4: mTLS client identity
        if (self.client_cert is None) != (self.client_key is None):
            raise ValueError(
                "TrustStore: 'client_cert' and 'client_key' must both be set or "
                "both be None for mTLS.  Got: "
                f"client_cert={self.client_cert!r}, client_key={self.client_key!r}"
            )
        if self.client_cert is not None and self.client_key is not None:
            ctx.load_cert_chain(
                certfile=str(self.client_cert),
                keyfile=str(self.client_key),
            )

        return ctx


__all__ = ["TrustStore"]
