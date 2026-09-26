"""
Plan 046 / Step 8 — `varco_sa`'s `DIWiringConformance` subclass (§D-suite),
plus `bind_repositories`/`create_tables` regressions.

`expected_defaults` measured directly against a bare
`container.scan("varco_core") + scan("varco_sa")` with a `bind_instance`d
sqlite `SAConfig` (Step 0(d) preflight, no live DB needed —
`create_async_engine("sqlite+aiosqlite:///:memory:")` opens nothing until
first use): B7 (`IUoWProvider`), B8 (`AbstractDistributedLock`, unqualified),
B6 (`RepositoryProvider`, `"sa"`) — plus one measured extra not named in the
plan's B-table: `SAXactAdvisoryLock` self-registers as its own fallback
default (a `@Provider` with no `exact_only` concept, unlike a scanned
`ClassBinding`'s self-binding) — flagged as an ambiguity in the handback.

RED MODE: `varco_core.di.bootstrap` does not exist yet — every test in the
inherited suite is expected to fail with `ImportError`. `bind_repositories`/
`create_tables` regressions fail because the production signatures do not
yet return `DIContainer` / raise `AlreadyBoundError` / drop `*entity_classes`.
"""

from __future__ import annotations

import pytest
from providify import DIContainer, IssueKind, Provider
from sqlalchemy.orm import DeclarativeBase
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
from varco_core.revocation import AbstractTokenRevocationStore
from varco_core.service.base import IUoWProvider
from varco_core.service.conversation import AbstractConversationStore
from varco_core.tenancy.membership import AbstractTenantMembership
from varco_sa.advisory_lock import SAXactAdvisoryLock
from varco_sa.bootstrap import SAConfig
from varco_sa.di import SAModule

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


class _Base(DeclarativeBase):
    pass


def _make_sqlite_sa_config() -> Provider:
    from sqlalchemy.ext.asyncio import create_async_engine

    @Provider(singleton=True)
    def _sa_config() -> SAConfig:
        return SAConfig(
            engine=create_async_engine("sqlite+aiosqlite:///:memory:"),
            base=_Base,
            entity_classes=(),
        )

    return _sa_config


class TestVarcoSaDIWiringConformance(DIWiringConformance):
    expected_defaults = CORE_DEFAULTS + (
        (IUoWProvider, None),
        (AbstractDistributedLock, None),
        (SAXactAdvisoryLock, None),
        (RepositoryProvider, "sa"),
        # Plan 047 N1-N3 — gated (and therefore @Fallback-marked
        # regardless of activation) sa-backed dlq/idempotency/job_store.
        (AbstractDeadLetterQueue, None),
        (AbstractIdempotencyStore, None),
        (AbstractJobStore, None),
    )

    # Plan 047 §D-suite — N1-N3 (dlq/idempotency/job_store, sa-backed).
    selectable = (
        Selectable("dlq", "sa", AbstractDeadLetterQueue, None, "dead_letter_queue"),
        Selectable("idempotency", "sa", AbstractIdempotencyStore, None, "idempotency_store"),
        Selectable("job_store", "sa", AbstractJobStore, None, "job_store"),
    )

    known_warnings = frozenset(
        {
            (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
        }
    )

    @pytest.fixture
    def bootstrap(self):
        def _bootstrap(container: DIContainer) -> DIContainer:
            from varco_core.di import bootstrap as core_bootstrap
            from varco_core.di import is_user_bound
            from varco_sa.di import bootstrap as sa_bootstrap

            # Guard against a second SAConfig provider on a repeated
            # bootstrap() call — the fixture itself must be idempotent for
            # test_bootstrap_is_idempotent, which calls it twice on the same
            # container.
            if not is_user_bound(container, SAConfig):
                container.provide(_make_sqlite_sa_config())
            core_bootstrap(container)
            sa_bootstrap(container)
            return container

        return _bootstrap


class TestBindRepositoriesContract:
    """§D-bind — `bind_repositories` returns the container and raises
    `AlreadyBoundError` per alias on a repeat."""

    def _bootstrapped_container(self) -> DIContainer:
        from varco_core.di import bootstrap as core_bootstrap
        from varco_sa.di import bootstrap as sa_bootstrap

        container = DIContainer()
        container.provide(_make_sqlite_sa_config())
        core_bootstrap(container)
        sa_bootstrap(container)
        return container

    def test_bind_repositories_returns_the_container(self) -> None:
        from varco_core.model import DomainModel
        from varco_sa.di import bind_repositories

        class Widget(DomainModel):
            pass

        container = self._bootstrapped_container()

        result = bind_repositories(container, Widget)

        assert result is container

    def test_bind_repositories_same_entity_twice_raises(self) -> None:
        from varco_core.di import AlreadyBoundError
        from varco_core.model import DomainModel
        from varco_sa.di import bind_repositories

        class Gadget(DomainModel):
            pass

        container = self._bootstrapped_container()
        bind_repositories(container, Gadget)

        with pytest.raises(AlreadyBoundError):
            bind_repositories(container, Gadget)


class TestBootstrapNoLongerAcceptsEntityClasses:
    """§D-contract / §D-removals — `bootstrap()` drops the `*entity_classes`
    convenience; callers use `bind_repositories()` separately."""

    def test_bootstrap_signature_has_no_entity_classes_varargs(self) -> None:
        import inspect

        from varco_sa.di import bootstrap as sa_bootstrap

        sig = inspect.signature(sa_bootstrap)
        assert list(sig.parameters) == ["container"]


class TestCreateTablesRequiresContainer:
    def test_create_tables_signature_requires_container(self) -> None:
        import inspect

        from varco_sa.di import create_tables

        sig = inspect.signature(create_tables)
        param = sig.parameters["container"]
        assert param.default is inspect.Parameter.empty


# ── Absorbed from 045's test_sa_fallback_tier.py (repair round 1) ────────────
# Folded here rather than dropped: these two assertions (exact priority
# value + provider-fn identity) are NOT covered by the generic
# DIWiringConformance suite above — test_declared_defaults_are_fallbacks
# only asserts `.fallback is True`, never the specific BACKEND_PRIORITY
# value or which @Provider function produced the binding.


async def test_sa_advisory_lock_outranks_in_memory_default_by_backend_priority() -> None:
    from varco_core.di import BACKEND_PRIORITY

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    await container.ainstall(SAModule)

    binding = container.get_binding(AbstractDistributedLock)

    assert binding.fallback is True
    assert binding.priority == BACKEND_PRIORITY
    # ProviderBinding (the @Provider shape SAModule uses) has no
    # `.implementation` attribute — that's a ClassBinding-only field.
    # `.fn.__name__` is the provider-shape equivalent identity check.
    assert binding.fn.__name__ == "sa_advisory_lock"


# ── Absorbed from 045's test_sa_fallback_tier.py — Class C never @Fallback ──
# HealthCheck is a collection contribution: a @Fallback binding is excluded
# from get_all() the instant any non-fallback sibling exists, which would
# silently empty /health the moment an app adds its own check. Not covered
# by DIWiringConformance (which only inspects expected_defaults, i.e.
# @Fallback bindings) — HealthCheck must never appear there.


def test_health_check_contributions_are_never_fallback() -> None:
    from varco_core.health import HealthCheck

    container = DIContainer()
    container.scan("varco_sa", recursive=True)

    bindings = container.get_all_bindings(HealthCheck)
    assert bindings, "expected at least one varco_sa HealthCheck binding"
    assert all(b.fallback is False for b in bindings)


def test_health_check_stays_present_after_a_user_adds_their_own() -> None:
    from providify import Singleton
    from varco_core.health import HealthCheck

    class UserHealthCheck(HealthCheck):
        async def check(self):  # pragma: no cover - stub
            raise NotImplementedError

    Singleton(UserHealthCheck)

    container = DIContainer()
    container.scan("varco_sa", recursive=True)
    container.bind(HealthCheck, UserHealthCheck)

    bindings = container.get_all_bindings(HealthCheck)
    implementations = {b.implementation for b in bindings}

    assert UserHealthCheck in implementations
    assert len(implementations) >= 2, (
        "a varco HealthCheck must never vanish from get_all() just because "
        "a user added their own — it must never be @Fallback"
    )
