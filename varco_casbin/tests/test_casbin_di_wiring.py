"""
Plan 046 / Step 7 — `varco_casbin`'s `DIWiringConformance` subclass
(§D-suite), plus the `bootstrap()`/`enable_policy_authorizer()` idempotency
regressions (premise 6 — the `_provide_casbin_settings` double-registration
bug, `varco_casbin/varco_casbin/di.py:123`).

`expected_defaults` measured directly (Step 0(d) preflight): B13
(`PolicyEngine`, `"casbin"`) and (`PolicyManagement`, `"casbin"`).

**Updated by Plan 048 / DI-6, Step 12**: `CasbinSettings` is now bound via
`bind_settings()` — a `@Fallback` binding, registered idempotently by
`bind_settings()`'s own per-`(cls, qualifier)` guard rather than the
hand-rolled `is_user_bound()` check this module used to need (the plain,
non-fallback `@Provider` + guard history is preserved in
`TestCasbinBootstrapIdempotency`'s docstring below).
"""

from __future__ import annotations

import pytest
from providify import DIContainer, IssueKind
from varco_casbin.config import CasbinSettings
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
from varco_core.idempotency.base import AbstractIdempotencyStore
from varco_core.job.base import AbstractJobStore
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
    # Plan 047 §D-vocab — the idempotency/job_store seam defaults.
    (AbstractIdempotencyStore, "in_memory"),
    (AbstractJobStore, "in_memory"),
)


class TestVarcoCasbinDIWiringConformance(DIWiringConformance):
    settings_classes = (CasbinSettings,)
    expected_defaults = CORE_DEFAULTS + (
        (PolicyEngine, "casbin"),
        (PolicyManagement, "casbin"),
        # Plan 048 / DI-6, Step 12 — CasbinSettings is now bound via
        # bind_settings(), a @Fallback binding (it used to be a plain,
        # non-fallback @Provider — see the module docstring's history).
        (CasbinSettings, None),
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


class TestPolicyAuthorizerEnvTwin:
    """Plan 047 / Step 9 — `policy_authorizer_when_enabled` twin, gated by
    `@feature_default("policy_authorizer")` (§D-features), registered by
    `bootstrap()` via `provide()` (idempotent guard). CLAUDE.md's "never a
    scanned `@Configuration` that shadows the app's authorizer" rule stays
    literally true — this is a gated fallback, inert unless the operator
    sets `VARCO_POLICY_AUTHORIZER_ENABLED`.
    """

    async def test_env_enabled_activates_the_twin_and_shadows_base_authorizer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Repair: CasbinPolicyEngine has an async @PostConstruct (`start`)
        # — resolving AbstractAuthorizer through the twin's
        # Inject[PolicyEngine] dependency requires the async path
        # (container.aget()); the original sync container.get() raised
        # "@PostConstruct method 'start' is async" unconditionally, which
        # this test's own name/intent never meant to exercise.
        from varco_casbin.di import bootstrap as casbin_bootstrap
        from varco_core.auth.authorizer import AbstractAuthorizer, BaseAuthorizer
        from varco_core.di import bootstrap as core_bootstrap

        monkeypatch.setenv("VARCO_POLICY_AUTHORIZER_ENABLED", "true")
        container = DIContainer()
        core_bootstrap(container)
        casbin_bootstrap(container)

        resolved = await container.aget(AbstractAuthorizer)
        assert not isinstance(resolved, BaseAuthorizer)
        await container.ashutdown()

    def test_enable_policy_authorizer_after_env_code_wins(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from varco_casbin.di import bootstrap as casbin_bootstrap
        from varco_casbin.di import enable_policy_authorizer
        from varco_core.auth.authorizer import AbstractAuthorizer
        from varco_core.di import bootstrap as core_bootstrap

        monkeypatch.setenv("VARCO_POLICY_AUTHORIZER_ENABLED", "true")
        container = DIContainer()
        core_bootstrap(container)
        casbin_bootstrap(container)
        enable_policy_authorizer(container)

        assert container.get_binding(AbstractAuthorizer).fallback is False

    def test_app_bound_authorizer_plus_env_wins_no_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # §D-features: "the env form never raises where a code form would be
        # AlreadyBoundError" — an app-bound AbstractAuthorizer plus the env
        # var set is not an error, the app simply wins.
        from providify import Singleton
        from varco_casbin.di import bootstrap as casbin_bootstrap
        from varco_core.auth.authorizer import AbstractAuthorizer
        from varco_core.di import bootstrap as core_bootstrap

        class UserAuthorizer(AbstractAuthorizer):
            async def authorize(self, *args: object, **kwargs: object) -> None:
                return None

        Singleton(UserAuthorizer)

        monkeypatch.setenv("VARCO_POLICY_AUTHORIZER_ENABLED", "true")
        container = DIContainer()
        core_bootstrap(container)
        casbin_bootstrap(container)
        container.bind(AbstractAuthorizer, UserAuthorizer)

        resolved = container.get(AbstractAuthorizer)
        assert isinstance(resolved, UserAuthorizer)
