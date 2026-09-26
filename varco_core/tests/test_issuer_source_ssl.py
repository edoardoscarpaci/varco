"""
Plan 026 / Step 19 + 24 — failing-first tests for ``ssl_context=`` on ``JwksUrlSource`` and
``OidcDiscoverySource`` (§D-T5).

``ssl_context`` is not yet an accepted keyword on either source, and
``TrustedIssuerRegistry.from_env()`` does not yet build one from CA env vars — every test here
fails today (``TypeError: unexpected keyword argument 'ssl_context'`` for the two loopback
tests, or an ``AttributeError``/assertion mismatch for the ``from_env()`` tests) until Steps
20-23 land.
"""

from __future__ import annotations

import json
import ssl
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from _tls_test_certs import mint_ca, write_pem


class _JwksHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 — stdlib method name
        if self.path == "/.well-known/openid-configuration":
            body = json.dumps(
                {"issuer": self.server.base_url, "jwks_uri": f"{self.server.base_url}/jwks.json"}  # type: ignore[attr-defined]
            ).encode()
        else:
            body = json.dumps({"keys": []}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 — stdlib signature
        pass  # silence test output


@pytest.fixture
def loopback_https_server(tmp_path):
    ca = mint_ca("loopback-ca")

    # Mint a leaf cert signed by the CA so trusting the CA trusts the leaf.
    import datetime
    import ipaddress

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    ca_key = serialization.load_pem_private_key(ca.key_pem, password=None)
    ca_cert = x509.load_pem_x509_certificate(ca.cert_pem)

    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.datetime.now(datetime.UTC)
    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")]))
        .issuer_name(ca_cert.subject)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    leaf_cert_pem = leaf_cert.public_bytes(serialization.Encoding.PEM)
    leaf_key_pem = leaf_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )

    cert_path = tmp_path / "leaf.pem"
    key_path = tmp_path / "leaf.key"
    ca_path = tmp_path / "ca.pem"
    cert_path.write_bytes(leaf_cert_pem)
    key_path.write_bytes(leaf_key_pem)
    ca_path.write_bytes(ca.cert_pem)

    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))

    httpd = HTTPServer(("127.0.0.1", 0), _JwksHandler)
    httpd.socket = server_ctx.wrap_socket(httpd.socket, server_side=True)
    port = httpd.server_address[1]
    httpd.base_url = f"https://127.0.0.1:{port}"  # type: ignore[attr-defined]

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield httpd.base_url, ca_path  # type: ignore[attr-defined]
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


# ── Step 19 ───────────────────────────────────────────────────────────────────


async def test_jwks_url_source_fails_untrusted_without_ssl_context(loopback_https_server) -> None:
    from varco_core.authority.exceptions import KeyLoadError
    from varco_core.authority.sources.jwks_url import JwksUrlSource

    base_url, _ca_path = loopback_https_server
    source = JwksUrlSource(f"{base_url}/jwks.json")

    with pytest.raises(KeyLoadError):
        await source.load()


async def test_jwks_url_source_succeeds_with_trusted_ssl_context(loopback_https_server) -> None:
    from varco_core.authority.sources.jwks_url import JwksUrlSource
    from varco_core.tls.store import TrustStore

    base_url, ca_path = loopback_https_server
    store = TrustStore(ca_cert=ca_path, include_system_cas=False)
    source = JwksUrlSource(f"{base_url}/jwks.json", ssl_context=store.build_ssl_context())

    keyset = await source.load()
    assert keyset.keys == ()


async def test_oidc_discovery_source_fails_untrusted_without_ssl_context(
    loopback_https_server,
) -> None:
    from varco_core.authority.exceptions import KeyLoadError
    from varco_core.authority.sources.oidc import OidcDiscoverySource

    base_url, _ca_path = loopback_https_server
    source = OidcDiscoverySource(base_url)

    with pytest.raises(KeyLoadError):
        await source.load()


async def test_oidc_discovery_source_succeeds_with_trusted_ssl_context(
    loopback_https_server,
) -> None:
    from varco_core.authority.sources.oidc import OidcDiscoverySource
    from varco_core.tls.store import TrustStore

    base_url, ca_path = loopback_https_server
    store = TrustStore(ca_cert=ca_path, include_system_cas=False)
    source = OidcDiscoverySource(base_url, ssl_context=store.build_ssl_context())

    keyset = await source.load()
    assert keyset.keys == ()


# ── Step 24 ───────────────────────────────────────────────────────────────────


def test_from_env_produces_none_ssl_context_with_no_ca_env_vars_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Byte-identical-default proof: with no CA env var set, urlopen(context=None) — today's
    # behaviour, unchanged.
    for name in (
        "VARCO_TLS_TRUST_STORE_DIR",
        "VARCO_TLS_CA_CERT",
        "VARCO_TLS_CLIENT_CERT",
        "VARCO_TLS_CLIENT_KEY",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "VARCO_AUTHORIZATION__SYS__URL",
        "VARCO_AUTHORIZATION__SYS__ISS",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("VARCO_AUTHORIZATION__SYS__URL", "jwks::https://example.com/jwks.json")
    monkeypatch.setenv("VARCO_AUTHORIZATION__SYS__ISS", "sys")

    from varco_core.authority.registry import TrustedIssuerRegistry

    registry = TrustedIssuerRegistry.from_env()
    entry = registry.entry("SYS")

    assert getattr(entry.source, "_ssl_context", "MISSING") is None


def test_from_env_produces_non_none_ssl_context_with_ca_cert_set(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    ca = mint_ca()
    ca_path = write_pem(tmp_path / "ca.pem", ca)

    for name in (
        "VARCO_TLS_TRUST_STORE_DIR",
        "VARCO_TLS_CLIENT_CERT",
        "VARCO_TLS_CLIENT_KEY",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("VARCO_TLS_CA_CERT", str(ca_path))
    monkeypatch.setenv("VARCO_AUTHORIZATION__SYS__URL", "jwks::https://example.com/jwks.json")
    monkeypatch.setenv("VARCO_AUTHORIZATION__SYS__ISS", "sys")

    from varco_core.authority.registry import TrustedIssuerRegistry

    registry = TrustedIssuerRegistry.from_env()
    entry = registry.entry("SYS")

    assert getattr(entry.source, "_ssl_context", None) is not None
