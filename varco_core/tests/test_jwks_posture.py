"""
Tests for Plan 041 / S22, §D-S22-posture — ``inspect_jwks_posture()``
(``varco_core.authority.posture``), modelled on
``varco_core/tests/test_revocation_posture.py``.

Pure read, never raises. RED until ``varco_core/varco_core/authority/posture.py``
and its ``varco_core.authority.__all__`` export exist.
"""

from __future__ import annotations


class TestInspectJwksPostureDefaults:
    def test_no_argument_never_raises_and_reports_registry_absent(self):
        from varco_core.authority.posture import inspect_jwks_posture

        report = inspect_jwks_posture()

        assert report is not None
        assert report.registry_present is False

    def test_zero_entry_registry_never_raises(self):
        from varco_core.authority.posture import inspect_jwks_posture
        from varco_core.authority.registry import TrustedIssuerRegistry

        registry = TrustedIssuerRegistry()
        report = inspect_jwks_posture(registry)

        assert report.registry_present is True
        assert report.remote_source_count == 0
        assert report.keysets_loaded == 0


class TestInspectJwksPosturePemOnly:
    def test_pem_only_registry_reports_zero_remote_sources(self, tmp_path):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from varco_core.authority.posture import inspect_jwks_posture
        from varco_core.authority.registry import TrustedIssuerRegistry
        from varco_core.authority.sources.pem_file import PemFileSource

        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem_bytes = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        pem_path = tmp_path / "pubkey.pem"
        pem_path.write_bytes(pem_bytes)

        registry = TrustedIssuerRegistry()
        registry.register(
            "SYSTEM", "system-iss", PemFileSource(path=pem_path, kid="k1", algorithm="RS256")
        )

        report = inspect_jwks_posture(registry)

        assert report.registry_present is True
        assert report.remote_source_count == 0


class TestInspectJwksPostureRemoteSourceNoRefresher:
    def test_remote_source_with_no_refresher_reports_finding_facts(self):
        from varco_core.authority.posture import inspect_jwks_posture
        from varco_core.authority.registry import TrustedIssuerRegistry
        from varco_core.authority.sources.jwks_url import JwksUrlSource

        registry = TrustedIssuerRegistry()
        registry.register(
            "REMOTE", "remote-iss", JwksUrlSource("https://example.com/.well-known/jwks.json")
        )

        report = inspect_jwks_posture(registry)

        assert report.refresher_running is False
        assert report.remote_source_count == 1


class TestInspectJwksPostureRunningRefresher:
    async def test_running_refresher_reports_refresher_running_true(self):
        from varco_core.authority.posture import inspect_jwks_posture
        from varco_core.authority.registry import TrustedIssuerRegistry
        from varco_core.jwk.model import JsonWebKeySet

        class _FakeSource:
            @property
            def source_id(self) -> str:
                return "fake::remote"

            async def load(self) -> JsonWebKeySet:
                return JsonWebKeySet(keys=())

            async def refresh(self) -> JsonWebKeySet:
                return JsonWebKeySet(keys=())

        registry = TrustedIssuerRegistry(ttl_seconds=0.0, min_refresh_interval=0.01)
        registry.register("FAKE", "fake-iss", _FakeSource())  # type: ignore[arg-type]

        try:
            await registry.start_refresh(interval=0.05)
            report = inspect_jwks_posture(registry)
            assert report.refresher_running is True
            assert report.effective_interval == 0.05
        finally:
            await registry.stop_refresh()


class TestInspectJwksPostureNeverRaises:
    def test_never_raises_for_any_registry_state(self):
        from varco_core.authority.posture import inspect_jwks_posture
        from varco_core.authority.registry import TrustedIssuerRegistry

        for registry in (None, TrustedIssuerRegistry()):
            report = inspect_jwks_posture(registry)
            assert report is not None
