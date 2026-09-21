"""
Plan 045 / Step 10 — the varco_core default-binding rule (§D-rule), proven.

For every Class A in-process default (§Context's site table, A1-A12): a
scanned `container.scan("varco_core", recursive=True)` resolves the varco
default; an application `container.bind(ABC, UserImpl)` registered AFTER
that scan wins outright, regardless of registration order — because the
varco default is `@Fallback` and the user binding is not. The five qualified
defaults (A1-A5) also keep resolving via the `qualifier="in_memory"` escape
hatch even when a user binding shadows the unqualified request (§D-qualifier).
A13 (`OtelConfiguration.otel_config`, a `@Provider`-in-`@Configuration`
default) is pinned separately because its natural request IS the ABC
(`OtelConfig`) — the `FALLBACK_SHADOWED` INFO fires for it, unlike the
scanned-class shapes (§D-proof).

These are passing contract pins, proven against the sites Steps 7-9 declared
`@Fallback`. `test_fallback_shadowed_scanned_class_blind_spot` at the bottom
is the one exception — a `strict=True` xfail pinning the P28 gap filed in
`design/upstream-gaps/providify-fallback-shadowed-scanned-class.md`.
"""

from __future__ import annotations

import pytest
from providify import DIContainer, Fallback, IssueKind, Provider, Singleton
from providify.metadata import _is_fallback
from varco_core.auth.authorizer import AbstractAuthorizer, BaseAuthorizer
from varco_core.cache.base import CacheBackend
from varco_core.cache.memory import InMemoryCache
from varco_core.event import AbstractEventBus
from varco_core.event.base import Event
from varco_core.event.deduplication import AbstractDeduplicator, InMemoryDeduplicator
from varco_core.event.dlq import AbstractDeadLetterQueue, InMemoryDeadLetterQueue
from varco_core.event.memory import InMemoryEventBus
from varco_core.event.producer import AbstractEventProducer, BusEventProducer
from varco_core.event.serializer import JsonEventSerializer, Serializer
from varco_core.flags import AbstractFeatureFlags, NullFeatureFlags
from varco_core.lock import AbstractDistributedLock, InMemoryLock
from varco_core.observability import OtelConfig
from varco_core.observability.di import OtelConfiguration
from varco_core.revocation import AbstractTokenRevocationStore
from varco_core.revocation.null import NullTokenRevocationStore
from varco_core.service.conversation import AbstractConversationStore, InMemoryConversationStore
from varco_core.tenancy.membership import AbstractTenantMembership, NullTenantMembership


@Provider(singleton=True)
def _user_otel_config() -> OtelConfig:
    """App-supplied OtelConfig override — module scope so its lazy PEP-563
    return annotation resolves against this module's globals (the same
    function-local-provider pitfall CLAUDE.md's DI table warns about)."""
    return OtelConfig(service_name="user-service")


QUALIFIED_DEFAULTS = [
    pytest.param(CacheBackend, InMemoryCache, "in_memory", id="CacheBackend"),
    pytest.param(AbstractEventBus, InMemoryEventBus, "in_memory", id="AbstractEventBus"),
    pytest.param(
        AbstractDeadLetterQueue, InMemoryDeadLetterQueue, "in_memory", id="AbstractDeadLetterQueue"
    ),
    pytest.param(
        AbstractDeduplicator, InMemoryDeduplicator, "in_memory", id="AbstractDeduplicator"
    ),
    pytest.param(AbstractDistributedLock, InMemoryLock, "in_memory", id="AbstractDistributedLock"),
]

UNQUALIFIED_DEFAULTS = [
    pytest.param(
        AbstractConversationStore, InMemoryConversationStore, id="AbstractConversationStore"
    ),
    pytest.param(AbstractFeatureFlags, NullFeatureFlags, id="AbstractFeatureFlags"),
    pytest.param(
        AbstractTokenRevocationStore, NullTokenRevocationStore, id="AbstractTokenRevocationStore"
    ),
    pytest.param(AbstractTenantMembership, NullTenantMembership, id="AbstractTenantMembership"),
    pytest.param(AbstractEventProducer, BusEventProducer, id="AbstractEventProducer"),
    pytest.param(AbstractAuthorizer, BaseAuthorizer, id="AbstractAuthorizer"),
]

ALL_DEFAULTS = [
    pytest.param(*p.values[:2], id=p.id) for p in QUALIFIED_DEFAULTS
] + UNQUALIFIED_DEFAULTS


@pytest.mark.parametrize("interface, default_cls", ALL_DEFAULTS)
def test_default_resolves_from_a_bare_scan(interface: type, default_cls: type) -> None:
    container = DIContainer()
    container.scan("varco_core", recursive=True)

    binding = container.get_binding(interface)

    assert binding.implementation is default_cls


@pytest.mark.parametrize("interface, default_cls", ALL_DEFAULTS)
def test_default_carries_the_fallback_marker(interface: type, default_cls: type) -> None:
    assert _is_fallback(default_cls) is True


@pytest.mark.parametrize("interface, default_cls", ALL_DEFAULTS)
def test_user_binding_registered_after_scan_wins(interface: type, default_cls: type) -> None:
    class UserImpl(interface):  # type: ignore[misc, valid-type]
        pass

    Singleton(UserImpl)

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    container.bind(interface, UserImpl)

    binding = container.get_binding(interface)
    assert binding.implementation is UserImpl

    all_bindings = container.get_all_bindings(interface)
    assert [b.implementation for b in all_bindings] == [UserImpl]


@pytest.mark.parametrize("interface, default_cls, qualifier", QUALIFIED_DEFAULTS)
def test_qualified_default_still_resolves_when_shadowed(
    interface: type, default_cls: type, qualifier: str
) -> None:
    class UserImpl(interface):  # type: ignore[misc, valid-type]
        pass

    Singleton(UserImpl)

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    container.bind(interface, UserImpl)

    qualified_binding = container.get_binding(interface, qualifier=qualifier)
    assert qualified_binding.implementation is default_cls


def test_json_event_serializer_is_fallback_default() -> None:
    # A11 — a generic-interface fallback, kept separate to avoid parametrize
    # complications with Serializer[Event]'s subscripted alias.
    container = DIContainer()
    container.scan("varco_core", recursive=True)

    binding = container.get_binding(Serializer[Event])
    assert binding.implementation is JsonEventSerializer
    assert _is_fallback(JsonEventSerializer) is True


class TestOtelConfigurationFallbackShape:
    """A13 — a `@Provider`-in-`@Configuration` default; its natural request
    IS the ABC (`OtelConfig`), so `FALLBACK_SHADOWED` fires for it, unlike
    the scanned-class shapes above (§D-proof)."""

    async def test_user_provider_after_install_wins_and_is_reported_shadowed(self) -> None:
        container = DIContainer()
        await container.ainstall(OtelConfiguration)
        container.provide(_user_otel_config)

        resolved = container.get(OtelConfig)
        assert resolved.service_name == "user-service"

        report = container.validate(raise_on_error=False)

        shadow_issues = [i for i in report.issues if i.kind == IssueKind.FALLBACK_SHADOWED]
        assert len(shadow_issues) == 1
        assert "_user_otel_config" in str(shadow_issues[0])

        await container.ashutdown()


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "GAP: P28-FALLBACK-SHADOWED-SCANNED-CLASS — providify never emits "
        "FALLBACK_SHADOWED for a scanned-class default shadowed at its ABC "
        "(a scanned @Singleton's own interface is the concrete class, never "
        "the ABC) — see "
        "design/upstream-gaps/providify-fallback-shadowed-scanned-class.md"
    ),
)
def test_fallback_shadowed_scanned_class_blind_spot() -> None:
    """§D-proof: unlike A13's provider-shaped default (above), a scanned-class
    default's `FALLBACK_SHADOWED` count is 0 even when its ABC is shadowed by
    an active application binding — the exact reproduction from the P28
    report's §2. This assertion encodes the FIXED behaviour (count == 1);
    it fails today, which is what makes the xfail honoured."""

    class Base:
        pass

    @Fallback
    @Singleton
    class Impl(Base):
        pass

    class Other(Base):
        pass

    Singleton(Other)

    container = DIContainer()
    container.register(Impl)
    container.bind(Base, Other)

    report = container.validate(raise_on_error=False)
    shadowed = [i for i in report.infos if i.kind == IssueKind.FALLBACK_SHADOWED]
    assert len(shadowed) == 1
