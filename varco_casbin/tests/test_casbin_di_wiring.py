"""
Plan 046 / Step 7 — `varco_casbin`'s `DIWiringConformance` subclass
(§D-suite), plus the `bootstrap()`/`enable_policy_authorizer()` idempotency
regressions (premise 6 — the `_provide_casbin_settings` double-registration
bug, `varco_casbin/varco_casbin/di.py:123`).

`expected_defaults` measured directly (Step 0(d) preflight): B13
(`PolicyEngine`, `"casbin"`) and (`PolicyManagement`, `"casbin"`).
`CasbinSettings` is **not** a `@Fallback` default — it is a plain
non-fallback `@Provider` registered by hand inside `bootstrap()`, which is
exactly why it is not idempotent today (§Context row #11) and needs the
`is_user_bound()` guard this plan adds.

RED MODE: `varco_core.di.bootstrap` does not exist yet — every test in the
inherited suite is expected to fail with `ImportError`. The idempotency
regressions fail today with a real `AMBIGUOUS_BINDING` (confirmed live,
Step 0(d)) because `bootstrap()` calls `container.provide(_provide_casbin_settings)`
unconditionally on every call.
"""

from __future__ import annotations

import pytest
from providify import DIContainer, IssueKind
from varco_conformance.di_wiring import DIWiringConformance
from varco_core.auth.authorizer import AbstractAuthorizer
from varco_core.auth.policy import PolicyEngine, PolicyManagement
from varco_core.cache.base import CacheBackend
from varco_core.event import AbstractEventBus
from varco_core.event.base import Event
from varco_core.event.deduplication import AbstractDeduplicator
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_core.event.producer import AbstractEventProducer
from varco_core.event.serializer import Serializer
from varco_core.flags import AbstractFeatureFlags
from varco_core.lock import AbstractDistributedLock
from varco_core.observability import OtelConfig
from varco_core.revocation import AbstractTokenRevocationStore
from varco_core.service.conversation import AbstractConversationStore
from varco_core.tenancy.membership import AbstractTenantMembership

CORE_DEFAULTS = (
    (CacheBackend, "in_memory"),
    (AbstractEventBus, "in_memory"),
    (AbstractDeadLetterQueue, "in_memory"),
    (AbstractDeduplicator, "in_memory"),
    (AbstractDistributedLock, "in_memory"),
    (AbstractConversationStore, "in_memory"),
    (AbstractFeatureFlags, None),
    (AbstractTokenRevocationStore, None),
    (AbstractTenantMembership, None),
    (AbstractEventProducer, None),
    (Serializer[Event], None),
    (AbstractAuthorizer, None),
    (OtelConfig, None),
)


class TestVarcoCasbinDIWiringConformance(DIWiringConformance):
    expected_defaults = CORE_DEFAULTS + (
        (PolicyEngine, "casbin"),
        (PolicyManagement, "casbin"),
    )

    known_warnings = frozenset(
        {
            (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
        }
    )

    @pytest.fixture
    def bootstrap(self):
        def _bootstrap(container: DIContainer) -> DIContainer:
            from varco_casbin.di import bootstrap as casbin_bootstrap
            from varco_core.di import bootstrap as core_bootstrap

            core_bootstrap(container)
            casbin_bootstrap(container)
            return container

        return _bootstrap


class TestCasbinBootstrapIdempotency:
    """Regression: `bootstrap()` must be safe to call twice — today it is
    not (`_provide_casbin_settings` is re-`provide()`d unconditionally,
    producing a real `AMBIGUOUS_BINDING` at `CasbinSettings`)."""

    def test_bootstrap_twice_registers_one_casbin_settings_binding(self) -> None:
        from varco_casbin.config import CasbinSettings
        from varco_casbin.di import bootstrap as casbin_bootstrap
        from varco_core.di import bootstrap as core_bootstrap

        container = DIContainer()
        core_bootstrap(container)
        casbin_bootstrap(container)
        casbin_bootstrap(container)

        report = container.validate(raise_on_error=False)
        ambiguous = [i for i in report.errors if i.kind is IssueKind.AMBIGUOUS_BINDING]
        assert ambiguous == [], ambiguous

        all_bindings = container.get_all_bindings(CasbinSettings)
        assert len(all_bindings) == 1


class TestEnablePolicyAuthorizerAlreadyBound:
    """Regression: `enable_policy_authorizer()` called twice, or after the
    app bound its own `AbstractAuthorizer`, must raise `AlreadyBoundError`
    rather than silently registering a second competing provider."""

    def test_enable_policy_authorizer_twice_raises(self) -> None:
        from varco_casbin.di import bootstrap as casbin_bootstrap
        from varco_casbin.di import enable_policy_authorizer
        from varco_core.di import AlreadyBoundError
        from varco_core.di import bootstrap as core_bootstrap

        container = DIContainer()
        core_bootstrap(container)
        casbin_bootstrap(container)
        enable_policy_authorizer(container)

        with pytest.raises(AlreadyBoundError):
            enable_policy_authorizer(container)

    def test_enable_policy_authorizer_after_app_binding_raises(self) -> None:
        from providify import Singleton
        from varco_casbin.di import bootstrap as casbin_bootstrap
        from varco_casbin.di import enable_policy_authorizer
        from varco_core.auth.authorizer import AbstractAuthorizer
        from varco_core.di import AlreadyBoundError
        from varco_core.di import bootstrap as core_bootstrap

        class UserAuthorizer(AbstractAuthorizer):
            async def authorize(self, *args: object, **kwargs: object) -> None:
                return None

        Singleton(UserAuthorizer)

        container = DIContainer()
        core_bootstrap(container)
        casbin_bootstrap(container)
        container.bind(AbstractAuthorizer, UserAuthorizer)

        with pytest.raises(AlreadyBoundError):
            enable_policy_authorizer(container)
