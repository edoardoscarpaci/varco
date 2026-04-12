"""
varco_core.connection.ssl
=========================
``SSLConfig`` — a structured, immutable SSL/TLS configuration that can be
embedded as a nested field inside any ``ConnectionSettings`` (pydantic
``BaseSettings``) subclass.

When embedded, pydantic-settings uses ``env_nested_delimiter="__"`` to populate
individual fields from environment variables::

    # POSTGRES_SSL__CA_CERT=/etc/ssl/ca.pem  → ssl.ca_cert = Path("/etc/ssl/ca.pem")
    # POSTGRES_SSL__VERIFY=false             → ssl.verify = False

``SSLConfig`` is a ``pydantic.BaseModel`` (NOT ``BaseSettings``) so it can be
embedded without triggering its own env-var scan — the parent settings class
controls that.

Standalone construction (without a parent ``ConnectionSettings``)::

    # From code
    ssl = SSLConfig(ca_cert=Path("/etc/ssl/ca.pem"), client_cert=Path("client.crt"), client_key=Path("client.key"))

    # From env vars directly (useful in bootstrap scripts)
    ssl = SSLConfig.from_env(prefix="POSTGRES_")

    # Produce an ssl.SSLContext for any async driver
    ctx = ssl.build_ssl_context()

DESIGN: pydantic BaseModel over frozen dataclass
    ✅ Embeds in ConnectionSettings (BaseSettings) with env_nested_delimiter —
       pydantic-settings handles nested population automatically.
    ✅ Validators via @model_validator catch misconfigurations at construction time.
    ✅ Frozen — immutable after construction.
    ✅ model_dump() / model_validate() round-trip cleanly for DI wiring.
    ❌ Adds pydantic as a dep — already required by varco_core, so no new dep.
    ❌ build_ssl_context() is a side-effectful method on a value object —
       acceptable because SSLContext creation is cheap and the method is synchronous.

Thread safety:  ✅ Frozen — safe to share across threads.
Async safety:   ✅ build_ssl_context() is synchronous (ssl module is sync).

📚 Docs
- 🔍 https://docs.pydantic.dev/latest/concepts/pydantic_settings/#nested-model
  Pydantic Settings — nested models + env_nested_delimiter
- 🐍 https://docs.python.org/3/library/ssl.html#ssl.SSLContext
  ssl.SSLContext — check_hostname, verify_mode, CERT_NONE
"""

from __future__ import annotations

import os
import ssl
from pathlib import Path
from typing import Self

from pydantic import ConfigDict, model_validator
from pydantic import BaseModel


# ── SSLConfig ─────────────────────────────────────────────────────────────────


class SSLConfig(BaseModel):
    """
    Structured SSL/TLS configuration for any async driver or HTTP client.

    Embeds as a nested pydantic model inside ``ConnectionSettings`` subclasses
    — pydantic-settings populates it from env vars using ``env_nested_delimiter``.

    Attributes:
        ca_cert:         Path to a single CA PEM file.  Merged with system CAs
                         (if ``verify=True``) or used as the sole CA bundle.
        ca_folder:       Directory of ``*.pem``/``*.crt`` CA files.  All matching
                         files are loaded into the trust chain.
        client_cert:     Path to the mTLS client certificate.
        client_key:      Path to the mTLS client private key.  Must be set
                         together with ``client_cert`` or neither.
        verify:          Whether to verify the server's certificate chain.
                         ``False`` disables all certificate validation — use
                         only in development/testing.
        check_hostname:  Whether to enforce hostname verification.  Must be
                         ``False`` when ``verify=False`` (ssl module requirement).

    Thread safety:  ✅ Frozen — safe to share across threads and event loops.
    Async safety:   ✅ build_ssl_context() is synchronous.

    Edge cases:
        - ``SSLConfig()`` (default) → system CA bundle, hostname + cert verified.
        - ``verify=False`` with ``check_hostname=True`` → ``ValueError`` at
          construction time (ssl module would raise at connect time; we raise early).
        - ``client_cert`` set without ``client_key`` (or vice versa) → ``ValueError``.
        - ``ca_cert`` + ``ca_folder`` can both be set — both are loaded.
        - Paths are validated for existence only at ``build_ssl_context()`` time —
          constructing an ``SSLConfig`` with a non-existent path does not raise.

    Example::

        # Standard — system CAs, full verification
        ssl_cfg = SSLConfig()

        # Private CA + mTLS
        ssl_cfg = SSLConfig(
            ca_cert=Path("/etc/ssl/my-ca.pem"),
            client_cert=Path("/etc/ssl/client.crt"),
            client_key=Path("/etc/ssl/client.key"),
        )

        # Skip verification (dev only)
        ssl_cfg = SSLConfig(verify=False, check_hostname=False)

        # From env vars (prefix = "POSTGRES_" → reads POSTGRES_SSL__CA_CERT etc.)
        ssl_cfg = SSLConfig.from_env("POSTGRES_")

        # Use with asyncpg
        ctx = ssl_cfg.build_ssl_context()
        conn = await asyncpg.connect(dsn, ssl=ctx)
    """

    model_config = ConfigDict(frozen=True)

    ca_cert: Path | None = None
    """Path to a single CA PEM file.  Env var (when nested): ``{PREFIX}SSL__CA_CERT``."""

    ca_folder: Path | None = None
    """Directory of CA ``*.pem``/``*.crt`` files.  Env var: ``{PREFIX}SSL__CA_FOLDER``."""

    client_cert: Path | None = None
    """mTLS client certificate path.  Env var: ``{PREFIX}SSL__CLIENT_CERT``."""

    client_key: Path | None = None
    """mTLS client private key path.  Env var: ``{PREFIX}SSL__CLIENT_KEY``."""

    verify: bool = True
    """Verify server certificate chain.  Env var: ``{PREFIX}SSL__VERIFY``."""

    check_hostname: bool = True
    """Enforce hostname verification.  Env var: ``{PREFIX}SSL__CHECK_HOSTNAME``."""

    # ── Validators ────────────────────────────────────────────────────────────

    @model_validator(mode="after")
    def _validate_ssl_flags(self) -> Self:
        """
        Guard against nonsensical SSL flag combinations.

        Raises:
            ValueError: If ``check_hostname=True`` and ``verify=False``.
            ValueError: If exactly one of ``client_cert``/``client_key`` is set.
        """
        if not self.verify and self.check_hostname:
            raise ValueError(
                "SSLConfig: 'check_hostname=True' requires 'verify=True'. "
                "The ssl module enforces this — set 'check_hostname=False' "
                "when disabling certificate verification."
            )
        if (self.client_cert is None) != (self.client_key is None):
            raise ValueError(
                "SSLConfig: 'client_cert' and 'client_key' must both be set "
                "or both be None for mTLS.  Got: "
                f"client_cert={self.client_cert!r}, client_key={self.client_key!r}"
            )
        return self

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def from_env(cls, prefix: str = "") -> SSLConfig:
        """
        Build an ``SSLConfig`` from environment variables.

        Each field maps to ``{prefix}SSL__{FIELD_NAME_UPPER}``.
        Missing env vars produce ``None`` / default values.

        Args:
            prefix: The connection prefix (e.g. ``"POSTGRES_"``).  Can be empty
                    to read from top-level vars like ``SSL__CA_CERT``.

        Returns:
            A populated ``SSLConfig``.

        Raises:
            ValueError: If the resulting config has inconsistent flags
                        (e.g. ``verify=False`` + ``check_hostname=True``).

        Edge cases:
            - Paths are not validated for existence at this stage.
            - ``verify`` and ``check_hostname`` are parsed as lowercase strings:
              ``"false"`` / ``"0"`` / ``"no"`` → ``False``; everything else → ``True``.

        Example::

            import os
            os.environ["POSTGRES_SSL__CA_CERT"] = "/etc/ssl/ca.pem"
            ssl_cfg = SSLConfig.from_env("POSTGRES_")
            # ssl_cfg.ca_cert == Path("/etc/ssl/ca.pem")
        """
        p = prefix  # shorthand

        def _path(key: str) -> Path | None:
            v = os.environ.get(f"{p}SSL__{key}")
            return Path(v) if v else None

        def _bool(key: str, default: bool) -> bool:
            v = os.environ.get(f"{p}SSL__{key}")
            if v is None:
                return default
            return v.strip().lower() not in ("false", "0", "no")

        return cls(
            ca_cert=_path("CA_CERT"),
            ca_folder=_path("CA_FOLDER"),
            client_cert=_path("CLIENT_CERT"),
            client_key=_path("CLIENT_KEY"),
            verify=_bool("VERIFY", True),
            check_hostname=_bool("CHECK_HOSTNAME", True),
        )

    # ── SSL context builder ───────────────────────────────────────────────────

    def build_ssl_context(self) -> ssl.SSLContext:
        """
        Build and return an ``ssl.SSLContext`` from this configuration.

        Steps:
        1. Create context from system CAs (``ssl.create_default_context()``) when
           ``verify=True``, or a blank client context when ``verify=False``.
        2. Disable hostname checking if ``check_hostname=False``.
        3. Glob ``ca_folder`` for ``*.pem``/``*.crt`` → load each file.
        4. Load explicit ``ca_cert`` file.
        5. Load ``client_cert`` + ``client_key`` for mTLS.

        Returns:
            A configured ``ssl.SSLContext``.

        Raises:
            FileNotFoundError: If any configured path does not exist.
            ssl.SSLError: If any certificate fails to load.

        Thread safety:  ✅ Creates a new context per call — no shared mutable state.
        Async safety:   ✅ Synchronous — call at startup, not per-request.

        Edge cases:
            - ``SSLConfig()`` → system CA bundle, hostname + cert verified (default).
            - ``verify=False`` → CERT_NONE, check_hostname disabled; all TLS connections
              succeed regardless of certificate validity.  Use only in dev/tests.
        """
        # Step 1: base context
        if self.verify:
            ctx = ssl.create_default_context()
        else:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        # Step 2: disable hostname checking when explicitly requested
        if not self.check_hostname and self.verify:
            # verify=True + check_hostname=False: valid — e.g. IP-based connections
            ctx.check_hostname = False

        # Step 3: load ca_folder
        if self.ca_folder is not None:
            folder = Path(self.ca_folder)
            for cert_path in sorted(
                list(folder.glob("*.pem")) + list(folder.glob("*.crt"))
            ):
                ctx.load_verify_locations(cafile=str(cert_path))

        # Step 4: load explicit ca_cert
        if self.ca_cert is not None:
            ctx.load_verify_locations(cafile=str(self.ca_cert))

        # Step 5: mTLS client identity
        # Validator already guarantees both or neither are set.
        if self.client_cert is not None and self.client_key is not None:
            ctx.load_cert_chain(
                certfile=str(self.client_cert),
                keyfile=str(self.client_key),
            )

        return ctx


__all__ = ["SSLConfig"]
