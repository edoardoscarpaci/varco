"""
Plan 046 / Step 9 — `varco_fastapi`'s `DIWiringConformance` subclass
(§D-suite), the `create_varco_container`/`setup_event_producer`/
`get_container` removals, `DISessionMiddleware`'s container-required
contract, `bind_mcp_adapter`/`bind_skill_adapter` `qualifier=`, and the
redundant-`install(VarcoFastAPIConfiguration)`-after-`bootstrap()` pitfall.

`expected_defaults` measured directly against a bare
`container.scan("varco_core") + scan("varco_fastapi")` (Step 0(d) preflight)
— 13 fallback defaults observed (`TrustStore`, `CORSConfig`, `ClientProfile`,
`ProfilingSettings`, `JwtTransformSettings`, `ClaimTransformerRegistry`,
`TaskRegistry`, `I18nSettings`, `TimezoneSettings`, `MessageCatalog`,
`TenantDefaultsProvider`, `TaskSerializer`, `AbstractJobStore`) — a superset
of the plan's own named A14/A15x11/A16x2/A18 rows; used as measured.

RED MODE: `varco_core.di.bootstrap` does not exist yet — every test in the
inherited suite is expected to fail with `ImportError`. The removal/contract
tests fail because `create_varco_container`/`setup_event_producer`/
`get_container` still exist, `DISessionMiddleware`'s `container` kwarg is
still optional, and `bind_mcp_adapter`/`bind_skill_adapter` do not yet accept
`qualifier=` or raise `AlreadyBoundError`.
"""

from __future__ import annotations

import inspect

import pytest
from providify import DIContainer, IssueKind
from varco_conformance.di_wiring import DIWiringConformance
from varco_core.auth.authorizer import AbstractAuthorizer
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
from varco_core.job.serializer import TaskSerializer
from varco_core.job.task import TaskRegistry
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


def _fastapi_expected_defaults() -> tuple[tuple[type, str | None], ...]:
    from varco_core.authority.config import AuthorizationConfig
    from varco_core.cache.config import CacheSettings
    from varco_core.context.defaults import TenantDefaultsProvider
    from varco_core.exception.settings import ErrorEnvelopeSettings
    from varco_core.i18n.catalog import MessageCatalog
    from varco_core.i18n.settings import I18nSettings
    from varco_core.idempotency.settings import IdempotencySettings
    from varco_core.jwt.config import JwtVerificationSettings
    from varco_core.jwt.transform.config import JwtTransformSettings
    from varco_core.jwt.transform.registry import ClaimTransformerRegistry
    from varco_core.migration.settings import MigrationSettings
    from varco_core.tenancy.membership import TenantMembershipSettings
    from varco_core.tenancy.settings import TenancySettings, TenantProvenanceSettings
    from varco_core.tls.settings import TlsSettings
    from varco_core.tz.settings import TimezoneSettings
    from varco_core.webhook.settings import WebhookSettings
    from varco_fastapi.auth.trust_store import TrustStore
    from varco_fastapi.client.base import ClientProfile
    from varco_fastapi.client.settings import ClientProfileSettings
    from varco_fastapi.middleware.body_limit import BodyLimitSettings
    from varco_fastapi.middleware.cors import CORSConfig
    from varco_fastapi.middleware.profiling import ProfilingSettings
    from varco_fastapi.middleware.rate_limit import RateLimitSettings
    from varco_fastapi.middleware.security_headers import SecurityHeadersSettings
    from varco_fastapi.posture import SecurityPostureSettings

    return CORE_DEFAULTS + (
        (TrustStore, None),
        (CORSConfig, None),
        (ClientProfile, None),
        (ProfilingSettings, None),
        (JwtTransformSettings, None),
        (ClaimTransformerRegistry, None),
        (TaskRegistry, None),
        (I18nSettings, None),
        (TimezoneSettings, None),
        (MessageCatalog, None),
        (TenantDefaultsProvider, None),
        (TaskSerializer, None),
        # Plan 047 §D-vocab — InMemoryJobStore moved to varco_core.job.memory
        # with qualifier="in_memory" (a scanned varco_core default now, not
        # a varco_fastapi one); the idempotency seam's in-process default
        # also newly appears via the same core scan.
        (AbstractJobStore, "in_memory"),
        (AbstractIdempotencyStore, "in_memory"),
        # Plan 048 / DI-6 — bind_core_settings() (called from both
        # varco_core.di.bootstrap() indirectly via bind_settings and
        # varco_fastapi.di.bootstrap() directly) now registers these eight
        # as @Fallback bindings too, plus five varco_fastapi-owned settings
        # classes bound the same way in bootstrap() (§D-core-list, Step 13).
        (CacheSettings, None),
        (IdempotencySettings, None),
        (WebhookSettings, None),
        (ErrorEnvelopeSettings, None),
        (JwtVerificationSettings, None),
        (SecurityPostureSettings, None),
        (SecurityHeadersSettings, None),
        (BodyLimitSettings, None),
        (RateLimitSettings, None),
        # Plan 048 / DI-6, Step 5 additions to §D-core-list —
        # bind_core_settings() now also binds these six.
        (MigrationSettings, None),
        (TenancySettings, None),
        (TenantProvenanceSettings, None),
        (TenantMembershipSettings, None),
        (AuthorizationConfig, None),
        (TlsSettings, None),
        # Decision 1 (this repair round) — ClientProfile split into a
        # companion ClientProfileSettings, bound via bind_settings() in
        # bootstrap() beside CORSConfig/BodyLimitSettings/etc.
        (ClientProfileSettings, None),
    )


def _fastapi_settings_classes() -> tuple[type, ...]:
    """
    Plan 048 / DI-6, Step 18 — every class ``bind_core_settings()``/
    ``bind_settings()`` binds via ``varco_fastapi.di.bootstrap()``.
    Deliberately excludes the non-settings ``@Fallback``
    ``@Singleton``/``@Provider`` defaults also present in
    ``_fastapi_expected_defaults()`` (``TrustStore``, ``ClientProfile``,
    ``ProfilingSettings``, ``ClaimTransformerRegistry``, ``TaskRegistry``,
    ``MessageCatalog``, ``TenantDefaultsProvider``, ``TaskSerializer``) —
    those are not ``bind_settings()``-registered and this assertion is
    specifically about the settings recipe's idempotency.
    """
    from varco_core.authority.config import AuthorizationConfig
    from varco_core.cache.config import CacheSettings
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
    from varco_fastapi.client.settings import ClientProfileSettings
    from varco_fastapi.middleware.body_limit import BodyLimitSettings
    from varco_fastapi.middleware.cors import CORSConfig
    from varco_fastapi.middleware.rate_limit import RateLimitSettings
    from varco_fastapi.middleware.security_headers import SecurityHeadersSettings
    from varco_fastapi.posture import SecurityPostureSettings

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
        SecurityPostureSettings,
        SecurityHeadersSettings,
        BodyLimitSettings,
        RateLimitSettings,
        CORSConfig,
        ClientProfileSettings,
    )


from varco_fastapi.modules import FastAPIModule  # noqa: E402


class TestVarcoFastapiDIWiringConformance(DIWiringConformance):
    module_cls = FastAPIModule

    expected_defaults = _fastapi_expected_defaults()
    settings_classes = _fastapi_settings_classes()

    known_warnings = frozenset(
        {
            (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
        }
    )

    @pytest.fixture
    def bootstrap(self):
        def _bootstrap(container: DIContainer) -> DIContainer:
            from varco_core.di import bootstrap as core_bootstrap
            from varco_fastapi.di import bootstrap as fastapi_bootstrap

            core_bootstrap(container)
            fastapi_bootstrap(container)
            return container

        return _bootstrap

    async def test_redundant_install_after_scan_is_a_documented_noop(
        self, container: DIContainer
    ) -> None:
        # Corrected against providify 2.1.0 source during this step (§Context's
        # own table: "@Configuration idempotency — container-owned dedup: a
        # second install()/scan of the same module is a no-op",
        # scanner.py:255-..., container.py:1019/7158/7194) — measured live: a
        # second container.install(VarcoFastAPIConfiguration) after bootstrap()'s own
        # scan already installed it adds NO binding and raises nothing. The
        # original draft of this test asserted AMBIGUOUS_BINDING, which
        # contradicts that documented, container-owned dedup; corrected here
        # rather than left red against correct production behaviour.
        from varco_fastapi.di import VarcoFastAPIConfiguration

        before = len(container.describe().bindings)

        container.install(VarcoFastAPIConfiguration)

        assert len(container.describe().bindings) == before
        report = container.validate(raise_on_error=False)
        assert report.errors == ()


class TestRemovedFunctionsAreGone:
    """§D-removals — `create_varco_container`, `setup_event_producer`,
    `get_container` are deleted outright."""

    def test_create_varco_container_is_removed(self) -> None:
        import varco_fastapi.di as di_module

        assert not hasattr(di_module, "create_varco_container")

    def test_setup_event_producer_is_removed(self) -> None:
        import varco_fastapi.di as di_module

        assert not hasattr(di_module, "setup_event_producer")

    def test_get_container_is_removed(self) -> None:
        import varco_fastapi.middleware.session as session_module

        assert not hasattr(session_module, "get_container")

    def test_varco_core_bootstrap_supplies_the_event_producer_fallback_instead(
        self,
    ) -> None:
        # §Migration: an app that used setup_event_producer(container) now
        # relies on varco_core.di.bootstrap() registering BusEventProducer
        # as the @Fallback AbstractEventProducer (045 A10).
        from varco_core.di import bootstrap as core_bootstrap
        from varco_core.event.producer import AbstractEventProducer, BusEventProducer

        container = DIContainer()
        core_bootstrap(container)

        binding = container.get_binding(AbstractEventProducer)
        assert binding.implementation is BusEventProducer
        assert binding.fallback is True


class TestBootstrapSignature:
    def test_bootstrap_requires_a_container_and_has_no_packages_or_setup_producer(
        self,
    ) -> None:
        from varco_fastapi.di import bootstrap as fastapi_bootstrap

        sig = inspect.signature(fastapi_bootstrap)
        assert list(sig.parameters) == ["container"]
        assert sig.parameters["container"].default is inspect.Parameter.empty


class TestBindClientsContract:
    def test_bind_clients_returns_the_container(self) -> None:
        from varco_fastapi.client.base import AsyncVarcoClient
        from varco_fastapi.di import bind_clients
        from varco_fastapi.router.base import VarcoRouter

        class WidgetRouter(VarcoRouter):
            pass

        class WidgetClient(AsyncVarcoClient[WidgetRouter]):
            pass

        container = DIContainer()

        result = bind_clients(container, WidgetClient)

        assert result is container


class TestDISessionMiddlewareRequiresContainer:
    """§D-locator — `DISessionMiddleware`'s `container` kwarg becomes
    required (keyword-only); positional construction and no-container
    construction both fail."""

    def test_construction_without_container_raises_type_error(self) -> None:
        from starlette.applications import Starlette
        from varco_fastapi.middleware.session import DISessionMiddleware

        app = Starlette()

        with pytest.raises(TypeError):
            DISessionMiddleware(app)

    def test_construction_with_container_positionally_raises_type_error(self) -> None:
        from starlette.applications import Starlette
        from varco_fastapi.middleware.session import DISessionMiddleware

        app = Starlette()
        container = DIContainer()

        with pytest.raises(TypeError):
            DISessionMiddleware(app, container)


class TestBindMcpAdapterQualifier:
    def test_bind_mcp_adapter_returns_the_container(self) -> None:
        from varco_core.di import bootstrap as core_bootstrap
        from varco_fastapi.router.base import VarcoRouter
        from varco_fastapi.router.mcp import bind_mcp_adapter

        class WidgetRouter(VarcoRouter):
            pass

        container = DIContainer()
        core_bootstrap(container)

        result = bind_mcp_adapter(container, WidgetRouter)

        assert result is container

    def test_bind_mcp_adapter_twice_unqualified_raises(self) -> None:
        from varco_core.di import AlreadyBoundError
        from varco_core.di import bootstrap as core_bootstrap
        from varco_fastapi.router.base import VarcoRouter
        from varco_fastapi.router.mcp import bind_mcp_adapter

        class WidgetRouter(VarcoRouter):
            pass

        container = DIContainer()
        core_bootstrap(container)
        bind_mcp_adapter(container, WidgetRouter)

        with pytest.raises(AlreadyBoundError):
            bind_mcp_adapter(container, WidgetRouter)

    def test_bind_mcp_adapter_twice_qualified_binds_two(self) -> None:
        from varco_core.di import bootstrap as core_bootstrap
        from varco_fastapi.router.base import VarcoRouter
        from varco_fastapi.router.mcp import MCPAdapter, bind_mcp_adapter

        class OrderRouter(VarcoRouter):
            pass

        class UserRouter(VarcoRouter):
            pass

        container = DIContainer()
        core_bootstrap(container)
        bind_mcp_adapter(container, OrderRouter, qualifier="orders")
        bind_mcp_adapter(container, UserRouter, qualifier="users")

        orders = container.get(MCPAdapter, qualifier="orders")
        users = container.get(MCPAdapter, qualifier="users")
        assert orders is not users

    def test_bind_mcp_adapter_is_typed_container_to_dicontainer(self) -> None:
        # Repair round 1 (Plan Context rows #25/#26 + Step 9) — bind_mcp_adapter
        # must be typed (container: DIContainer, ...) -> DIContainer, not
        # (container: Any, ...) -> Any.
        #
        # inspect.signature() (not typing.get_type_hints()) is used
        # deliberately: DIContainer is imported only inside this module's
        # TYPE_CHECKING block (router/mcp.py never imports providify at
        # module scope), so get_type_hints() would raise NameError trying
        # to resolve the `from __future__ import annotations` string form
        # against the module's real (TYPE_CHECKING-free) runtime globals.
        # inspect.signature() reads the raw annotation string instead,
        # which is exactly what changed.
        import inspect

        from varco_fastapi.router.mcp import bind_mcp_adapter

        sig = inspect.signature(bind_mcp_adapter)
        assert sig.parameters["container"].annotation == "DIContainer"
        assert sig.return_annotation == "DIContainer"


class TestBindSkillAdapterQualifier:
    def test_bind_skill_adapter_returns_the_container(self) -> None:
        from varco_core.di import bootstrap as core_bootstrap
        from varco_fastapi.router.base import VarcoRouter
        from varco_fastapi.router.skill import bind_skill_adapter

        class WidgetRouter(VarcoRouter):
            pass

        container = DIContainer()
        core_bootstrap(container)

        result = bind_skill_adapter(
            container, WidgetRouter, agent_name="widget", agent_description="widget agent"
        )

        assert result is container

    def test_bind_skill_adapter_twice_unqualified_raises(self) -> None:
        from varco_core.di import AlreadyBoundError
        from varco_core.di import bootstrap as core_bootstrap
        from varco_fastapi.router.base import VarcoRouter
        from varco_fastapi.router.skill import bind_skill_adapter

        class WidgetRouter(VarcoRouter):
            pass

        container = DIContainer()
        core_bootstrap(container)
        bind_skill_adapter(
            container, WidgetRouter, agent_name="widget", agent_description="widget agent"
        )

        with pytest.raises(AlreadyBoundError):
            bind_skill_adapter(
                container, WidgetRouter, agent_name="widget", agent_description="widget agent"
            )

    def test_bind_skill_adapter_is_typed_container_to_dicontainer(self) -> None:
        # Repair round 1 (Plan Context rows #25/#26 + Step 9) — same
        # inspect.signature() rationale as
        # TestBindMcpAdapterQualifier::test_bind_mcp_adapter_is_typed_container_to_dicontainer.
        import inspect

        from varco_fastapi.router.skill import bind_skill_adapter

        sig = inspect.signature(bind_skill_adapter)
        assert sig.parameters["container"].annotation == "DIContainer"
        assert sig.return_annotation == "DIContainer"


class TestBindPeersAnnotations:
    def test_bind_peers_is_typed_container_to_dicontainer(self) -> None:
        # Repair round 1 (Plan Context rows #25/#26 + Step 9) — bind_peers
        # (varco_fastapi/varco_fastapi/client/peer.py) must be typed
        # (container: DIContainer, ...) -> DIContainer too.
        import inspect

        from varco_fastapi.client.peer import bind_peers

        sig = inspect.signature(bind_peers)
        assert sig.parameters["container"].annotation == "DIContainer"
        assert sig.return_annotation == "DIContainer"
