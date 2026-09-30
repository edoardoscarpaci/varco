"""
Plan 046 / Step 8 — `varco_beanie`'s `DIWiringConformance` subclass
(§D-suite), plus `bind_repositories` regressions.

`expected_defaults` measured directly against a bare
`container.scan("varco_core") + scan("varco_beanie")` with a `bind_instance`d
`BeanieSettings` whose `AsyncMongoClient` targets an unroutable address with a
1ms timeout (Step 0(d) preflight — confirmed live, no server needed:
`pymongo.AsyncMongoClient()` connects lazily, and `container.validate()`
instantiates nothing).

Measured beyond the plan's own B12 count of 2: `BeanieRepositoryProvider`
also registers `QueryApplicator` (`"beanie_aggregation"`) and two query
visitor fallbacks (`BinaryWalkingVisitor`, `ASTVisitor`, both `"beanie"`) —
flagged as an ambiguity in the handback.

RED MODE: `varco_core.di.bootstrap` does not exist yet — every test in the
inherited suite is expected to fail with `ImportError`. `bind_repositories`
regressions fail because the production signature does not yet return
`DIContainer` / raise `AlreadyBoundError`.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from providify import DIContainer, IssueKind, Provider
from pymongo import AsyncMongoClient
from varco_beanie.config import BeanieSettings
from varco_conformance.di_wiring import DIWiringConformance, Selectable
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
from varco_core.lock import AbstractDistributedLock
from varco_core.observability import OtelConfig
from varco_core.providers import RepositoryProvider
from varco_core.query.applicator.applicator import QueryApplicator
from varco_core.query.visitor.ast_visitor import ASTVisitor
from varco_core.query.visitor.walking import BinaryWalkingVisitor
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


def _make_beanie_settings_provider() -> Provider:
    @Provider(singleton=True)
    def _beanie_settings() -> BeanieSettings:
        return BeanieSettings(
            mongo_client=AsyncMongoClient(
                "mongodb://127.0.0.1:1/?connectTimeoutMS=1&serverSelectionTimeoutMS=1"
            ),
            db_name="x",
        )

    return _beanie_settings


def _test_fernet_key() -> bytes:
    """A throwaway Fernet key for the conformance suite's FieldEncryptor bind."""
    from cryptography.fernet import Fernet

    return Fernet.generate_key()


from varco_beanie.modules import BeanieModule  # noqa: E402


def _beanie_module_factory() -> BeanieModule:
    return BeanieModule(
        settings=BeanieSettings(
            mongo_client=AsyncMongoClient(
                "mongodb://127.0.0.1:1/?connectTimeoutMS=1&serverSelectionTimeoutMS=1"
            ),
            db_name="x",
        )
    )


class TestVarcoBeanieDIWiringConformance(DIWiringConformance):
    module_cls = BeanieModule
    module_factory = staticmethod(_beanie_module_factory)

    expected_defaults = CORE_DEFAULTS + (
        (AbstractDeadLetterQueue, "beanie"),
        (RepositoryProvider, "beanie"),
        (QueryApplicator, "beanie_aggregation"),
        (BinaryWalkingVisitor, "beanie"),
        (ASTVisitor, "beanie"),
        # Plan 047 N4/N5 — gated beanie-backed idempotency/job_store.
        (AbstractIdempotencyStore, None),
        (AbstractJobStore, None),
    )

    # Plan 047 §D-suite — N4-N5 (idempotency/job_store, beanie-backed);
    # dlq/beanie already exists (qualifier="beanie"), gated too.
    selectable = (
        Selectable("dlq", "beanie", AbstractDeadLetterQueue, "beanie", "BeanieDeadLetterQueue"),
        Selectable("idempotency", "beanie", AbstractIdempotencyStore, None, "idempotency_store"),
        Selectable("job_store", "beanie", AbstractJobStore, None, "job_store"),
    )

    known_warnings = frozenset(
        {
            (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
        }
    )

    @pytest.fixture
    def bootstrap(self):
        def _bootstrap(container: DIContainer) -> DIContainer:
            from varco_beanie.di import bootstrap as beanie_bootstrap
            from varco_core.di import bind_instance, is_user_bound
            from varco_core.di import bootstrap as core_bootstrap
            from varco_core.encryption import FernetFieldEncryptor, FieldEncryptor

            # Guard against a second BeanieSettings provider on a repeated
            # bootstrap() call — the fixture itself must be idempotent for
            # test_bootstrap_is_idempotent, which calls it twice on the
            # same container.
            if not is_user_bound(container, BeanieSettings):
                container.provide(_make_beanie_settings_provider())
            # Plan 050 / §D-wire rows 4-5 — webhook_repository's
            # `encryptor` parameter is required, no default. providify's
            # validate() checks every registered provider's parameters
            # structurally, regardless of whether its own @Requires
            # condition is currently satisfied — same reasoning as
            # varco_sa's twin fixture.
            if not is_user_bound(container, FieldEncryptor):
                bind_instance(container, FieldEncryptor, FernetFieldEncryptor(_test_fernet_key()))
            core_bootstrap(container)
            beanie_bootstrap(container)
            return container

        return _bootstrap

    async def test_package_module_registers_cleanly(
        self, bootstrap: Callable[[DIContainer], DIContainer]
    ) -> None:
        """
        Override (Plan 050 repair): ``BeanieConfiguration.webhook_repository()``
        requires a ``FieldEncryptor`` structurally, same reasoning as
        ``varco_sa``'s twin override — see that docstring.
        """
        if self.module_cls is None:
            pytest.skip("no module_cls declared")

        from varco_core.di import bind_instance
        from varco_core.di.module import CoreModule
        from varco_core.encryption import FernetFieldEncryptor, FieldEncryptor

        c = DIContainer()
        if self.module_cls is not CoreModule:
            CoreModule().register(c)
        bind_instance(c, FieldEncryptor, FernetFieldEncryptor(_test_fernet_key()))
        module = self.module_factory() if self.module_factory is not None else self.module_cls()
        module.register(c)

        report = c.validate(raise_on_error=False)
        assert report.errors == ()
        observed_warnings = {(i.kind, getattr(i, "owner", "")) for i in report.warnings}
        assert observed_warnings - self.known_warnings == set()

    async def test_declared_defaults_are_complete(self, container: DIContainer) -> None:
        """
        Override (Plan 050 repair): ``webhook_repository`` is a
        ``@Fallback`` binding gated on ``VARCO_WEBHOOK_BACKEND=beanie``
        with no unconditional in-process default in this package's own
        ``bootstrap()`` — see ``varco_sa``'s twin override for the full
        reasoning.
        """
        from varco_conformance.di_wiring import fallback_bindings
        from varco_core.webhook.base import WebhookSubscriptionRepository

        observed = {
            (b.interface, b.qualifier)
            for b in fallback_bindings(container)
            if not getattr(b, "exact_only", False)
        }
        assert observed == set(self.expected_defaults) | {(WebhookSubscriptionRepository, None)}


class TestBindRepositoriesContract:
    """§D-bind — `bind_repositories` returns the container and raises
    `AlreadyBoundError` per alias on a repeat."""

    def _bootstrapped_container(self) -> DIContainer:
        from varco_beanie.di import bootstrap as beanie_bootstrap
        from varco_core.di import bootstrap as core_bootstrap

        container = DIContainer()
        container.provide(_make_beanie_settings_provider())
        core_bootstrap(container)
        beanie_bootstrap(container)
        return container

    def test_bind_repositories_returns_the_container(self) -> None:
        from varco_beanie.di import bind_repositories
        from varco_core.model import DomainModel

        class Widget(DomainModel):
            pass

        container = self._bootstrapped_container()

        result = bind_repositories(container, Widget)

        assert result is container

    def test_bind_repositories_same_entity_twice_raises(self) -> None:
        from varco_beanie.di import bind_repositories
        from varco_core.di import AlreadyBoundError
        from varco_core.model import DomainModel

        class Gadget(DomainModel):
            pass

        container = self._bootstrapped_container()
        bind_repositories(container, Gadget)

        with pytest.raises(AlreadyBoundError):
            bind_repositories(container, Gadget)


class TestBootstrapNoLongerAcceptsEntityClasses:
    def test_bootstrap_signature_has_no_entity_classes_varargs(self) -> None:
        import inspect

        from varco_beanie.di import bootstrap as beanie_bootstrap

        sig = inspect.signature(beanie_bootstrap)
        assert list(sig.parameters) == ["container"]
