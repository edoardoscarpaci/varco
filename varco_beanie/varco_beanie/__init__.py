"""
varco_beanie
================
Beanie (pymongo / MongoDB) async backend for varco.

Quick start::

    from pymongo import AsyncMongoClient
    from varco_beanie import BeanieRepositoryProvider, BeanieDocRegistry
    from varco_core.query.builder import QueryBuilder
    from varco_core.query.params import QueryParams
    from varco_core.query.type import SortField, SortOrder

    provider = BeanieRepositoryProvider(mongo_client=AsyncMongoClient(...), db_name="mydb")
    provider.register(User, Post)
    await provider.init()

    async with provider.make_uow() as uow:
        recent_posts = await uow.posts.find_by_query(
            QueryParams(
                node=QueryBuilder().eq("published", True).build(),
                sort=[SortField("created_at", SortOrder.DESC)],
                limit=10,
            )
        )

providify DI integration::

    from pymongo import AsyncMongoClient
    from varco_beanie import BeanieConfiguration, BeanieSettings, bind_repositories
    from providify import DIContainer, Provider

    container = DIContainer()

    @Provider(singleton=True)
    def settings() -> BeanieSettings:
        return BeanieSettings(mongo_client=AsyncMongoClient(...), db_name="mydb", entity_classes=(User,))

    container.provide(settings)
    container.install(BeanieConfiguration)
    bind_repositories(container, User)

    repo = await container.aget(AsyncRepository[User])
"""

from varco_core.deprecation import deprecated_alias

from varco_beanie.bootstrap import BeanieFastrestApp
from varco_beanie.conversation import BeanieConversationStore, ConversationTurnDocument
from varco_beanie.deduplication import BeanieDeduplicator, DeduplicationDocument
from varco_beanie.di import BeanieConfiguration, BeanieSettings, bind_repositories
from varco_beanie.dlq import BeanieDeadLetterQueue, DeadLetterDocument
from varco_beanie.factory import BeanieDocRegistry, BeanieModelFactory
from varco_beanie.health import BeanieHealthCheck
from varco_beanie.idempotency import BeanieIdempotencyStore, IdempotencyDocument
from varco_beanie.inbox import BeanieInboxRepository, InboxDocument
from varco_beanie.index_guard import BeanieIndexGuard, IndexDrift, IndexDriftReport
from varco_beanie.job_store import BeanieJobStore, JobDocument
from varco_beanie.migration import (
    BeanieMigrator,
    IndexReconciler,
    Migration,
    MigrationRegistry,
)
from varco_beanie.outbox import BeanieOutboxRepository, OutboxDocument
from varco_beanie.provider import BeanieRepositoryProvider
from varco_beanie.query.aggregation import BeanieAggregationApplicator
from varco_beanie.repository import AsyncBeanieRepository
from varco_beanie.saga import BeanieSagaRepository, SagaDocument
from varco_beanie.uow import BeanieUnitOfWork

__all__ = [
    # Core backend classes
    "BeanieModelFactory",
    "BeanieDocRegistry",
    "AsyncBeanieRepository",
    "BeanieUnitOfWork",
    "BeanieRepositoryProvider",
    # DI integration
    "BeanieSettings",
    "BeanieConfiguration",
    "bind_repositories",
    # Bootstrap
    "BeanieFastrestApp",
    # ── Conversation store (multi-turn A2A) ───────────────────────────────────
    "ConversationTurnDocument",
    "BeanieConversationStore",
    # ── Inbox pattern ─────────────────────────────────────────────────────────
    "InboxDocument",
    "BeanieInboxRepository",
    # ── Job store ─────────────────────────────────────────────────────────────
    "JobDocument",
    "BeanieJobStore",
    # ── Outbox pattern ────────────────────────────────────────────────────────
    "OutboxDocument",
    "BeanieOutboxRepository",
    # ── Dead letter queue ─────────────────────────────────────────────────────
    "DeadLetterDocument",
    "BeanieDeadLetterQueue",
    # ── Query (aggregation) ───────────────────────────────────────────────────
    "BeanieAggregationApplicator",
    # ── Saga repository ───────────────────────────────────────────────────────
    "SagaDocument",
    "BeanieSagaRepository",
    # ── Index drift detection ─────────────────────────────────────────────────
    "BeanieIndexGuard",
    "IndexDrift",
    "IndexDriftReport",
    # ── Health probe ──────────────────────────────────────────────────────────
    "BeanieHealthCheck",
    # ── Deduplication ─────────────────────────────────────────────────────────
    "BeanieDeduplicator",
    "DeduplicationDocument",
    # ── Idempotency store (Plan 029 / D1b) ────────────────────────────────────
    "BeanieIdempotencyStore",
    "IdempotencyDocument",
    # ── Migrations ────────────────────────────────────────────────────────────
    "Migration",
    "MigrationRegistry",
    "BeanieMigrator",
    "IndexReconciler",
]

# AB-4's back-compat seam (Plan 022). ``BeanieConfig`` was a second name for
# ``BeanieSettings`` — the same four fields, bridged by KI-10's manual remap.
# The alias resolves to the *identical* class, so `isinstance(x, BeanieConfig)`
# and any existing construction keep working. Deliberately kept out of
# ``__all__``: a deprecated name should not be advertised. Removed in 4.0.0.
__getattr__ = deprecated_alias(
    "BeanieConfig",
    BeanieSettings,
    since="3.0.0",
    removed_in="4.0.0",
)
