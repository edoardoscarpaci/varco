"""
DI wiring tests for ``varco_sa``.

Why this file exists
---------------------
Audit finding F9 (``audits/001-audit-di-wiring.md``, Plan 013) observed that
``varco_sa`` — unlike ``varco_redis``/``varco_kafka``/``varco_nats`` — has no
named, canonically-located ``test_sa_di.py`` asserting the *core* wiring
(``SAConfiguration``, ``bind_repositories``, ``bootstrap()``) by name.

Spot-check finding recorded in Plan 013's Design section: raw
``container.scan("varco_sa", recursive=True); container.validate_bindings()``
coverage of this package already exists today, but only *incidentally* — as
a side effect of ``test_sa_tenancy_di.py`` and ``test_migration_di.py``,
neither of which is named for nor asserts anything about ``SAConfiguration`` /
``bind_repositories`` / ``bootstrap``. If either of those sub-area test files
were deleted or refactored, the package's core DI bootstrap safety net would
silently disappear with them. This file makes that coverage explicit, named,
and independent of those sub-areas' lifetimes.

No database, no Docker: only container registration and annotation
resolution are exercised — nothing is instantiated.
"""

from __future__ import annotations

from dataclasses import dataclass

from providify import DIContainer, Provider
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_conformance.providify_health import assert_no_structural_di_issues
from varco_core.model import DomainModel
from varco_core.repository import AsyncRepository
from varco_sa.config import SAConfig
from varco_sa.di import bind_repositories


@dataclass
class _Entity(DomainModel):
    name: str = ""


@dataclass
class _OtherEntity(DomainModel):
    title: str = ""


@Provider(singleton=True, priority=100)
def _sa_config_for_test() -> SAConfig:
    """
    In-memory sqlite ``SAConfig`` — ``create_async_engine`` does not open a
    connection until the engine is actually used, so this requires no Docker
    / real database, matching this file's "no database, no Docker" rule.

    A fresh ``DeclarativeBase`` subclass is built per call — each test builds
    its own container/provider, and ``SAModelFactory`` registers ORM tables
    on the base's shared ``MetaData``; a module-scope base would collide
    across tests ("Table '_entity' is already defined").
    """

    class _Base(DeclarativeBase):
        pass

    return SAConfig(
        engine=create_async_engine("sqlite+aiosqlite:///:memory:"),
        base=_Base,
        entity_classes=(_Entity, _OtherEntity),
    )


class TestSAContainerValidates:
    def test_regression_scan_validates_bindings(self) -> None:
        """
        User-visible symptom: an app calling ``varco_sa.di.bootstrap()`` and
        then resolving anything died at startup with an
        ``AnnotationResolutionError`` for a binding contributed by
        ``varco_sa`` (e.g. a future quoted ``@Provider`` return annotation
        silently disabling injection container-wide — see CLAUDE.md's pitfall
        table). Correct behaviour is a container that validates cleanly.
        """
        container = DIContainer()
        container.scan("varco_sa", recursive=True)

        container.validate_bindings()
        assert_no_structural_di_issues(container)

    def test_regression_core_sa_module_implementations_discovered(self) -> None:
        """
        ``SAConfiguration`` is a scan-marker ``@Configuration`` — its providers
        (``uow_provider``/``sa_advisory_lock``/``sa_xact_advisory_lock``) and
        the ``@Singleton``-decorated ``SQLAlchemyRepositoryProvider`` must
        all be discovered by ``scan("varco_sa", recursive=True)``. A future
        refactor accidentally excluding ``varco_sa.di``/``varco_sa.provider``
        from the scan path would leave ``IUoWProvider``/
        ``AbstractDistributedLock`` unresolvable at app startup — this test
        pins that they are actually registered.
        """
        container = DIContainer()
        container.scan("varco_sa", recursive=True)

        implementations = {
            getattr(b, "implementation", None).__name__
            for b in container._bindings
            if getattr(b, "implementation", None) is not None
        }
        # SAConfiguration's @Provider factories (uow_provider/sa_advisory_lock/
        # sa_xact_advisory_lock) bind an *interface* directly rather than an
        # @Singleton class — providify does not populate `.implementation`
        # for those, only `.interface`, so they are asserted separately.
        interfaces = {
            getattr(b, "interface", None).__name__
            for b in container._bindings
            if getattr(b, "interface", None) is not None
        }

        assert "SQLAlchemyRepositoryProvider" in implementations
        assert "AbstractDistributedLock" in interfaces
        assert "SAXactAdvisoryLock" in interfaces
        container.validate_bindings()
        assert_no_structural_di_issues(container)

    def test_regression_bind_repositories_against_real_container_validates(
        self,
    ) -> None:
        """
        No existing ``varco_sa`` test calls ``bind_repositories()`` against a
        real ``DIContainer`` — only mock-based coverage exists elsewhere. A
        per-entity ``AsyncRepository[D]`` provider is generated via runtime
        annotation patching (``_make_repo_provider`` — see its DESIGN block
        in ``varco_sa/varco_sa/di.py``); a real container is required to
        prove that patched annotation is actually something providify can
        resolve type hints for, which a ``MagicMock()`` container can never
        catch.
        """
        container = DIContainer()
        container.scan("varco_sa", recursive=True)

        bind_repositories(container, _Entity)

        container.validate_bindings()
        assert_no_structural_di_issues(container)

    def test_regression_bound_repository_resolves_through_get(self) -> None:
        """
        Plan 014 / Step 11 — characterization for site 5 before the
        ``@Provider`` annotation-patch extraction (the now-deleted
        ``varco_core.providify_compat.provide_factory()`` shim, Plan 014,
        Step 21). ``validate_bindings()`` resolves annotations without
        constructing anything; this actually builds the repository through
        ``container.get()`` to prove the runtime-patched
        ``AsyncRepository[entity_cls]`` alias is genuinely resolvable, not
        just annotation-clean.
        """
        container = DIContainer()
        container.scan("varco_sa", recursive=True)
        container.provide(_sa_config_for_test)
        bind_repositories(container, _Entity, _OtherEntity)

        repo = container.get(AsyncRepository[_Entity])

        assert repo is not None

    def test_regression_two_entities_do_not_shadow_each_other(self) -> None:
        """Two distinct entity classes bind to two distinct, independently
        resolvable generic aliases — the closure-capture bug ``Step 21``'s
        extraction must not reintroduce."""
        container = DIContainer()
        container.scan("varco_sa", recursive=True)
        container.provide(_sa_config_for_test)
        bind_repositories(container, _Entity, _OtherEntity)

        entity_repo = container.get(AsyncRepository[_Entity])
        other_repo = container.get(AsyncRepository[_OtherEntity])

        assert entity_repo is not other_repo

    def test_regression_bound_repository_is_dependent_scoped_not_singleton(
        self,
    ) -> None:
        """
        Pins ``Scope.DEPENDENT`` (``varco_sa/varco_sa/di.py`` deliberately
        does not pass ``singleton=True``) — two resolutions of the same
        generic alias must return two different instances, or a per-request
        repository (and its ``AsyncSession``) would leak across requests.
        """
        container = DIContainer()
        container.scan("varco_sa", recursive=True)
        container.provide(_sa_config_for_test)
        bind_repositories(container, _Entity)

        first = container.get(AsyncRepository[_Entity])
        second = container.get(AsyncRepository[_Entity])

        assert first is not second
