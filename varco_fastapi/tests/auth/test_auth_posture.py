"""
Unit tests for varco_fastapi.auth.posture.inspect_auth_posture() (Plan 034 /
Phase 4, Step 36, §D-034-seam). Pure function, no side effects, walks
wrapper composition (CompositeServerAuth members, WebSocketAuth.inner).
"""

from __future__ import annotations

from varco_core.auth.base import AuthContext
from varco_fastapi.auth.server_auth import (
    ApiKeyAuth,
    CompositeServerAuth,
    PassthroughAuth,
    WebSocketAuth,
)


class TestInspectAuthPostureApiKey:
    def test_default_query_fallback_disabled(self):
        from varco_fastapi.auth.posture import inspect_auth_posture

        auth = ApiKeyAuth(keys={"k": AuthContext()})
        report = inspect_auth_posture(auth)
        assert report.api_key_query_fallback_enabled is False
        assert report.api_key_query_param_name is None

    def test_named_param_reports_enabled_and_name(self):
        from varco_fastapi.auth.posture import inspect_auth_posture

        auth = ApiKeyAuth(keys={"k": AuthContext()}, param="api_key")
        report = inspect_auth_posture(auth)
        assert report.api_key_query_fallback_enabled is True
        assert report.api_key_query_param_name == "api_key"

    def test_plaintext_keys_reports_plaintext_source_true(self):
        from varco_fastapi.auth.posture import inspect_auth_posture

        auth = ApiKeyAuth(keys={"k": AuthContext()})
        report = inspect_auth_posture(auth)
        assert report.api_key_plaintext_source is True

    def test_hashed_keys_reports_plaintext_source_false(self):
        from varco_core.auth.api_key import hash_api_key
        from varco_fastapi.auth.posture import inspect_auth_posture

        auth = ApiKeyAuth(hashed_keys={hash_api_key("k"): AuthContext()})
        report = inspect_auth_posture(auth)
        assert report.api_key_plaintext_source is False


class TestInspectAuthPosturePassthrough:
    def test_bare_passthrough_reports_bound_true(self):
        from varco_fastapi.auth.posture import inspect_auth_posture

        auth = PassthroughAuth()
        report = inspect_auth_posture(auth)
        assert report.passthrough_auth_bound is True

    def test_nested_passthrough_inside_composite_inside_websocket_still_true(self):
        # The recursion test — walks CompositeServerAuth members and
        # WebSocketAuth.inner.
        from varco_fastapi.auth.posture import inspect_auth_posture

        composite = CompositeServerAuth([PassthroughAuth()])
        auth = WebSocketAuth(inner=composite)
        report = inspect_auth_posture(auth)
        assert report.passthrough_auth_bound is True

    def test_components_lists_every_class_walked_in_order(self):
        from varco_fastapi.auth.posture import inspect_auth_posture

        composite = CompositeServerAuth([PassthroughAuth()])
        auth = WebSocketAuth(inner=composite)
        report = inspect_auth_posture(auth)
        assert report.components[0] == "WebSocketAuth"
        assert "CompositeServerAuth" in report.components
        assert "PassthroughAuth" in report.components


class TestInspectAuthPostureRobustness:
    def test_never_raises_on_unknown_server_auth_subclass(self):
        from varco_core.auth.base import AuthContext
        from varco_fastapi.auth.posture import inspect_auth_posture
        from varco_fastapi.auth.server_auth import AbstractServerAuth

        class _CustomAuth(AbstractServerAuth):
            async def __call__(self, request):
                return AuthContext()

        report = inspect_auth_posture(_CustomAuth())
        assert report is not None

    def test_terminates_on_cyclic_composite_via_identity_guard(self):
        # Edge case: an auth object containing itself must not infinite-loop.
        from varco_fastapi.auth.posture import inspect_auth_posture

        composite = CompositeServerAuth([PassthroughAuth()])
        composite._strategies.append(composite)  # type: ignore[attr-defined]
        report = inspect_auth_posture(composite)
        assert report is not None

    def test_never_logs(self, caplog):
        import logging

        from varco_fastapi.auth.posture import inspect_auth_posture

        auth = ApiKeyAuth(keys={"k": AuthContext()})
        with caplog.at_level(logging.DEBUG):
            inspect_auth_posture(auth)
        assert caplog.records == []
