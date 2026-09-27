"""
Plan 046 / Step 5 — `varco_redis`'s `DIWiringConformance` subclass (§D-suite).

Absorbs 045's `test_redis_fallback_tier.py` (the B3/B4/A2 three-level
fallback shape — its assertions are now
`test_declared_defaults_are_fallbacks`/
`test_every_default_is_overridable_after_bootstrap`, plus the two dedicated
tests below preserving the "selector wins unqualified, qualifier= still
reaches the concrete class" behaviour with its full DESIGN rationale) —
deleted by this step.

Confirms `RedisEventBus`'s `@Singleton(qualifier="redis")` deliberately
carries NO `priority=BACKEND_PRIORITY` (unlike Kafka/NATS, which have no
separate selector) — giving it `BACKEND_PRIORITY` would tie it with
`RedisEventBusSelectorConfiguration.bus()` for the *unqualified* injection
point (providify's qualified-binding-also-matches-unqualified-request rule,
`container.py:2096-2101`), making "which one wins" registration-order
dependent instead of deterministic. Confirmed against source during this
step — not changed.

`RedisBackplaneSettings` used to NOT register via `bootstrap()`'s scan (a
pre-existing gap) — closed by Decision 2 of the settings-standardization
repair round: `RedisCacheSettings`/`LayeredCacheSettings`/
`RedisBackplaneSettings` are now all registered by `bootstrap()` via
`bind_settings()`, deleting the last three per-``@Configuration``/
module-level `@Fallback @Provider` settings factories in `varco_redis`.
"""

from __future__ import annotations

import os

import pytest
from providify import DIContainer, IssueKind, Singleton
from varco_conformance.di_wiring import DIWiringConformance, Selectable
from varco_core.auth.authorizer import AbstractAuthorizer
from varco_core.cache.base import CacheBackend
from varco_core.di import BACKEND_PRIORITY
from varco_core.event import AbstractEventBus
from varco_core.event.base import Event
from varco_core.event.channel import ChannelManager
from varco_core.event.deduplication import AbstractDeduplicator
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_core.event.memory import InMemoryEventBus
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
from varco_redis.backplane import RedisBackplaneSettings
from varco_redis.bus import RedisEventBus
from varco_redis.cache import LayeredCacheSettings, RedisCacheSettings
from varco_redis.config import RedisEventBusSettings
from varco_redis.modules import RedisModule


class TestVarcoRedisDIWiringConformance(DIWiringConformance):
    module_cls = RedisModule

    settings_classes = (
        RedisEventBusSettings,
        RedisCacheSettings,
        LayeredCacheSettings,
        RedisBackplaneSettings,
    )
    expected_defaults = (
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
        (AbstractEventBus, "redis"),
        (AbstractEventBus, None),
        (RedisCacheSettings, None),
        (CacheBackend, None),
        (LayeredCacheSettings, None),
        (ChannelManager, "redis"),
        (RedisEventBusSettings, None),
        (RedisBackplaneSettings, None),
        # Plan 047 N6/N7/N8 — new gated Redis stores; @Fallback-marked
        # (and therefore visible to fallback_bindings()) regardless of
        # whether VARCO_<SEAM>_BACKEND=redis is actually set.
        (AbstractIdempotencyStore, None),
        (AbstractJobStore, None),
        (AbstractDeadLetterQueue, None),
    )

    # Plan 047 §D-suite — the redis three-level shape: both the selector
    # (unqualified) and RedisEventBus (qualifier="redis") gate on the same
    # ("event_bus", "redis") value.
    selectable = (
        Selectable("event_bus", "redis", AbstractEventBus, None, "bus"),
        Selectable("event_bus", "redis", AbstractEventBus, "redis", "RedisEventBus"),
        Selectable("dlq", "redis", AbstractDeadLetterQueue, None, "dlq"),
        Selectable("dlq", "redis-streams", AbstractDeadLetterQueue, None, "dlq"),
        Selectable("cache", "redis", CacheBackend, None, "redis_cache"),
        Selectable("cache", "layered", CacheBackend, None, "layered_cache"),
        Selectable("idempotency", "redis", AbstractIdempotencyStore, None, "idempotency_store"),
        Selectable("job_store", "redis", AbstractJobStore, None, "job_store"),
        Selectable("revocation", "redis", AbstractTokenRevocationStore, None, "revocation_store"),
    )

    known_warnings = frozenset(
        {
            (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
            (IssueKind.MISSING_BINDING_DEFAULTED, "RedisEventBus.__init__"),
        }
    )

    @pytest.fixture
    def bootstrap(self):
        def _bootstrap(container: DIContainer) -> DIContainer:
            from varco_core.di import bootstrap as core_bootstrap
            from varco_redis.di import bootstrap as redis_bootstrap

            core_bootstrap(container)
            redis_bootstrap(container)
            return container

        return _bootstrap

    async def test_selected_backend_is_a_fallback_at_backend_priority(self, bootstrap) -> None:
        """
        Override: legitimate backend capability divergence (CLAUDE.md's
        conformance-findings decision table) — ``RedisEventBus``
        (qualifier="redis") deliberately carries NO ``priority=
        BACKEND_PRIORITY`` (see this module's own docstring and
        ``varco_redis/varco_redis/bus.py``): giving it one would tie it
        with ``RedisEventBusSelectorConfiguration.bus()`` for the
        *unqualified* injection point. So the qualified ``"redis"`` entry
        is asserted at priority ``0``; every other selectable entry keeps
        the base class's ``BACKEND_PRIORITY`` assertion.
        """
        import os

        from varco_core.di import BACKEND_PRIORITY
        from varco_core.di.seams import seam_by_name

        for entry in self.selectable:
            seam = seam_by_name(entry.seam)
            old = os.environ.get(seam.env)
            os.environ[seam.env] = entry.value
            try:
                c = DIContainer()
                bootstrap(c)

                binding = c.get_binding(entry.interface, qualifier=entry.qualifier)
                assert binding.fallback is True
                expected_priority = (
                    0
                    if (entry.seam, entry.qualifier) == ("event_bus", "redis")
                    else BACKEND_PRIORITY
                )
                assert binding.priority == expected_priority
                assert entry.owner_substr in str(getattr(binding, "owner", binding))
            finally:
                if old is None:
                    os.environ.pop(seam.env, None)
                else:
                    os.environ[seam.env] = old


def test_redis_selector_is_the_unqualified_winner() -> None:
    from varco_redis.di import bootstrap

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bootstrap(container)

    binding = container.get_binding(AbstractEventBus)

    assert binding.fallback is True
    assert binding.priority == BACKEND_PRIORITY


def test_redis_qualifier_still_resolves_the_concrete_class() -> None:
    from varco_redis.di import bootstrap

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bootstrap(container)

    binding = container.get_binding(AbstractEventBus, qualifier="redis")

    assert binding.implementation is RedisEventBus


def test_in_memory_qualifier_still_resolves_the_in_process_default() -> None:
    from varco_redis.di import bootstrap

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bootstrap(container)

    binding = container.get_binding(AbstractEventBus, qualifier="in_memory")

    assert binding.implementation is InMemoryEventBus


def test_user_binding_registered_after_bootstrap_wins() -> None:
    from varco_redis.di import bootstrap

    class UserBus(AbstractEventBus):
        pass

    Singleton(UserBus)

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bootstrap(container)
    container.bind(AbstractEventBus, UserBus)

    binding = container.get_binding(AbstractEventBus)

    assert binding.implementation is UserBus


def test_streams_replacement_selects_stream_bus_without_mutating_environ() -> None:
    # §D-streams — bootstrap(streams=True) is gone; an explicit non-fallback
    # RedisEventBusSettings(use_streams=True) registered before bootstrap()
    # is the replacement, and it must never touch os.environ.
    from varco_core.di import bind_instance
    from varco_redis.di import bootstrap
    from varco_redis.streams import RedisStreamEventBus

    assert "VARCO_REDIS_USE_STREAMS" not in os.environ

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bind_instance(container, RedisEventBusSettings, RedisEventBusSettings(use_streams=True))
    bootstrap(container)

    bus = container.get(AbstractEventBus)

    assert isinstance(bus, RedisStreamEventBus)
    assert "VARCO_REDIS_USE_STREAMS" not in os.environ
