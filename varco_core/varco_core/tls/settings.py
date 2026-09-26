"""
varco_core.tls.settings
========================
``TlsSettings`` — the ``VARCO_TLS_*`` env-driven half of ``TrustStore``
(Plan 048 / DI-6, §D-convert). ``TrustStore.from_env()`` delegates to
``TrustStore.from_settings(TlsSettings())``.

DESIGN: a new class, not a retrofit of ``TrustStore`` itself
    ✅ ``TrustStore`` is a value object with non-env fields (``ca_folders``
       as a ``Sequence[Path]``, ``pkcs12_file``, ``key_password``,
       ``include_system_cas``, a ``build_ssl_context()`` method) — it stays
       exactly what §D-convert's Non-goals list calls out: not a
       ``VarcoSettings`` itself. ``TlsSettings`` isolates only the four
       flat env-readable fields.
    ✅ Renames the four legacy, non-namespaced env vars
       (``VARCO_CA_CERT``/``VARCO_CLIENT_CERT``/``VARCO_CLIENT_KEY``/
       ``VARCO_TRUST_STORE_DIR``) to ``VARCO_TLS_*`` (§D-prefix rule 2 —
       ``VARCO_CLIENT_CERT``/``VARCO_CLIENT_KEY`` collided outright with
       ``ClientProfile``'s ``VARCO_CLIENT_`` prefix).
    ❌ ``SSL_CERT_FILE``/``SSL_CERT_DIR`` (no ``VARCO_`` prefix at all) stay
       outside this class and outside ``bind_settings()`` entirely —
       they are OpenSSL/``requests``/``uv``-standard names varco reads
       additively on top of ``TlsSettings``' own, in
       ``TrustStore.from_settings()`` (unchanged from today's
       ``TrustStore.from_env()``).

Thread safety:  ✅ ``frozen=True`` — safe to share across coroutines/threads.
Async safety:   ✅ No I/O.
"""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings


class TlsSettings(VarcoSettings):
    """
    Env-driven half of ``TrustStore`` (Plan 026 / T3, folded into the
    ``bind_settings()`` recipe by Plan 048 / DI-6).

    Attributes:
        ca_cert: Extra single CA cert file, merged additively on top of the
            system trust store. Env: ``VARCO_TLS_CA_CERT`` (was
            ``VARCO_CA_CERT``).
        client_cert: mTLS client certificate file. Env:
            ``VARCO_TLS_CLIENT_CERT`` (was ``VARCO_CLIENT_CERT`` — which
            collided with ``ClientProfile``'s ``VARCO_CLIENT_`` prefix;
            this rename removes the collision).
        client_key: mTLS client private key file. Env:
            ``VARCO_TLS_CLIENT_KEY`` (was ``VARCO_CLIENT_KEY``).
        trust_store_dir: Extra CA directory, merged additively. Env:
            ``VARCO_TLS_TRUST_STORE_DIR`` (was ``VARCO_TRUST_STORE_DIR``).

    Edge cases:
        - Every field defaults to ``None`` — an empty environment yields
          the same "system CAs only" ``TrustStore`` as before this plan.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_TLS_", frozen=True)

    ca_cert: Path | None = None
    client_cert: Path | None = None
    client_key: Path | None = None
    trust_store_dir: Path | None = None


__all__ = ["TlsSettings"]
