"""
varco_core.settings.core
===========================
``bind_core_settings()`` — binds every ``varco_core``-owned settings class
that has no package-specific home (Plan 048 / DI-6, §D-core-list).

``varco_core`` has no ``bootstrap()`` and, by locked decision, nothing in it
is scanned into activation — so something has to call ``bind_settings()``
for its own classes. Today's callers: ``varco_fastapi.di.bootstrap()``/
``create_varco_container()`` call this first; 049's core module will call it
from ``register()`` unchanged.

DESIGN: one function, one list, not per-class module-level providers
    ✅ A single audited list (§D-core-list) is either complete or wrong in
       an obviously reviewable diff — the opposite of "grep six files to
       find every settings provider" (§Context's "what is wrong,
       concretely" paragraph).
    ✅ Every listed class is guaranteed constructible from an empty
       environment (tested here and in
       ``test_every_registered_class_constructs_from_empty_env``) — a
       class that cannot (``CloudEventsSettings``, a required field) is
       deliberately absent and stays on its own explicit bind path.
    ❌ A class added to ``varco_core`` without updating this list is bound
       nowhere — mitigated by ``varco_core/tests/test_settings_inventory.
       py``'s "every discovered class is registered or excluded" rule.

Thread safety:  see ``bind_settings()``.
Async safety:   see ``bind_settings()``.
"""

from __future__ import annotations

from providify import DIContainer

from varco_core.settings.bind import bind_settings

#: Every class §D-core-list names. Each MUST construct from an empty
#: environment (tested by ``test_every_registered_class_constructs_from_
#: empty_env``) — a class with a required field never belongs here.
_CORE_SETTINGS_CLASSES: tuple[type, ...] = ()


def _classes() -> tuple[type, ...]:
    """
    Import and return §D-core-list's eleven pydantic classes, lazily —
    importing them at module scope would defeat ``varco_core.settings``'s
    own lazy-import budget (this module is only imported when
    ``bind_core_settings`` is actually called).
    """
    from varco_core.authority.config import AuthorizationConfig
    from varco_core.cache.config import CacheSettings
    from varco_core.di.report import DiagnoseSettings
    from varco_core.exception.settings import ErrorEnvelopeSettings
    from varco_core.i18n.settings import I18nSettings
    from varco_core.idempotency.settings import IdempotencySettings
    from varco_core.jwt.config import JwtVerificationSettings
    from varco_core.jwt.transform.config import JwtTransformSettings
    from varco_core.migration.settings import MigrationSettings
    from varco_core.tenancy.membership import TenantMembershipSettings
    from varco_core.tenancy.settings import TenancySettings, TenantProvenanceSettings
    from varco_core.tls.settings import TlsSettings
    from varco_core.tz.settings import TimezoneSettings
    from varco_core.webhook.settings import WebhookSettings

    return (
        CacheSettings,
        IdempotencySettings,
        WebhookSettings,
        ErrorEnvelopeSettings,
        JwtVerificationSettings,
        JwtTransformSettings,
        I18nSettings,
        TimezoneSettings,
        MigrationSettings,
        TenancySettings,
        TenantProvenanceSettings,
        TenantMembershipSettings,
        AuthorizationConfig,
        TlsSettings,
        DiagnoseSettings,
    )


def bind_core_settings(container: DIContainer) -> DIContainer:
    """
    Register every ``varco_core``-owned settings class on *container*.

    Args:
        container: The container to bind into (typically the one a
            package's ``bootstrap()`` is building).

    Returns:
        The same *container*, for chaining — matches every other
        ``bind_*``/``bootstrap`` helper's return-the-container convention.

    Raises:
        SettingsValidationError: If any listed class fails to construct
            from the current environment.

    Edge cases:
        - Safe to call twice — ``bind_settings()`` is itself idempotent
          per ``(cls, qualifier)``.
    """
    for cls in _classes():
        bind_settings(container, cls)
    return container


__all__ = ["bind_core_settings"]
