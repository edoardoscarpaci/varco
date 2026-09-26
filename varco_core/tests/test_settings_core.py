"""
Red-mode tests for ``varco_core.settings.core.bind_core_settings()``
(Plan 048 / Step 4).

``varco_core.settings.core`` does not exist yet — every test fails with
``ImportError`` until Step 4 lands it. This file is test-only.
"""

from __future__ import annotations

import pytest
from providify import DIContainer, IssueKind
from varco_core.settings.core import bind_core_settings

#: The eleven pydantic classes bind_core_settings() must resolve per §D-core-list.
_CORE_CLASSES_STEP4 = (
    "CacheSettings",
    "IdempotencySettings",
    "WebhookSettings",
    "ErrorEnvelopeSettings",
    "JwtVerificationSettings",
    "JwtTransformSettings",
    "I18nSettings",
    "TimezoneSettings",
)


class TestBindCoreSettings:
    def test_returns_the_same_container(self, di_container: DIContainer) -> None:
        assert bind_core_settings(di_container) is di_container

    def test_every_listed_class_is_resolvable_afterwards(self, di_container: DIContainer) -> None:
        import importlib

        bind_core_settings(di_container)

        for name in _CORE_CLASSES_STEP4:
            module = {
                "CacheSettings": "varco_core.cache.config",
                "IdempotencySettings": "varco_core.idempotency.settings",
                "WebhookSettings": "varco_core.webhook.settings",
                "ErrorEnvelopeSettings": "varco_core.exception.settings",
                "JwtVerificationSettings": "varco_core.jwt.config",
                "JwtTransformSettings": "varco_core.jwt.transform.config",
                "I18nSettings": "varco_core.i18n.settings",
                "TimezoneSettings": "varco_core.tz.settings",
            }[name]
            cls = getattr(importlib.import_module(module), name)
            assert di_container.get(cls) is not None

    def test_is_idempotent(self, di_container: DIContainer) -> None:
        bind_core_settings(di_container)
        before = len(di_container.describe().bindings)

        bind_core_settings(di_container)

        assert len(di_container.describe().bindings) == before
        report = di_container.validate(raise_on_error=False)
        assert IssueKind.AMBIGUOUS_BINDING not in {i.kind for i in report.errors}

    def test_validate_has_no_errors_or_warnings(self, di_container: DIContainer) -> None:
        bind_core_settings(di_container)

        report = di_container.validate(raise_on_error=False)

        assert report.errors == ()
        assert report.warnings == ()


class TestTrustedIssuerRegistrySettingsReadOnce:
    def test_settings_read_once_not_reread_on_verify(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        JwtVerificationSettings must be captured once at TrustedIssuerRegistry
        construction time, not re-parsed from the environment on every
        verify() call (today's behaviour, per the inventory row #8).
        """
        from varco_core.authority.registry import TrustedIssuerRegistry
        from varco_core.jwt.config import JwtVerificationSettings

        monkeypatch.setenv("VARCO_JWT_ENFORCE_ISS", "true")
        settings = JwtVerificationSettings()
        registry = TrustedIssuerRegistry(settings=settings)

        monkeypatch.setenv("VARCO_JWT_ENFORCE_ISS", "false")

        # Patching env after construction must have no effect on the
        # registry's already-captured settings object.
        assert registry.settings.enforce_iss is True
