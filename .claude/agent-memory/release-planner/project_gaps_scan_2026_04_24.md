---
name: Architectural gaps scan 2026-04-24
description: Concrete missing implementations found during full codebase scan for release planning
type: project
---

Key architectural gaps discovered during codebase scan (2026-04-24):

**Why:** These gaps were found by scanning every package, all di.py files, all test files, and comparing documented ABCs against concrete implementations.

**How to apply:** Use this as a checklist when planning features and reviewing PRs. Each item is a concrete, implementable scope.

---

## Missing Backend Implementations (documented but not yet implemented)

### InboxRepository — zero concrete implementations
- `InboxRepository` (ABC) is in `varco_core.service.inbox`
- `inbox.py` docstring explicitly mentions `SAInboxRepository` and `BeanieInboxRepository` — NEITHER exists
- No implementation in `varco_sa/`, `varco_beanie/`, `varco_redis/`

### AbstractSagaRepository — only InMemory exists
- `AbstractSagaRepository` (ABC) in `varco_core.service.saga`
- `InMemorySagaRepository` exists for tests
- No `SASagaRepository`, `BeanieSagaRepository`, or `RedisSagaRepository`
- SagaOrchestrator crash-safe resume is impossible without a durable repo

### AbstractJobStore — only InMemory exists
- `InMemoryJobStore` in `varco_fastapi.job.store`
- `store.py` docstring explicitly mentions `SaJobStore` and `RedisJobStore` — NEITHER exists
- `poller.py` explicitly references `SAJobStore`, `RedisJobStore` — NEITHER exists
- Job recovery across process restarts is impossible without a durable store

### AbstractConversationStore — only InMemory exists
- `AbstractConversationStore` (ABC) in `varco_core.service.conversation`
- `conversation.py` docstring mentions `SAConversationStore` (varco_sa) and `RedisConversationStore` (varco_redis)
- NEITHER exists — all SkillAdapter multi-turn conversations are lost on restart

### varco_memcached — missing HealthCheck
- `MemcachedCache` exists with full DI wiring (`MemcachedCacheConfiguration`)
- No `MemcachedHealthCheck` implementing `HealthCheck` ABC
- All other backends (Kafka, Redis, SA, Beanie) have health checks
- varco_memcached is also completely absent from ARCHITECTURE.md

### AbstractDistributedLock — no SA/PostgreSQL advisory lock
- `RedisLock` exists in `varco_redis`
- No `SAAdvisoryLock` using PostgreSQL `pg_try_advisory_lock` / `pg_advisory_unlock`
- Teams using SA without Redis have no distributed lock option

## Cross-Backend Parity Gaps

### AuditRepository — SA has it, Beanie has it, Redis has neither
- `SAAuditRepository` exists in `varco_sa.audit`
- `BeanieAuditRepository` exists in `varco_beanie.audit`
- PARITY: good between SA and Beanie

### Deduplication — Redis has it, SA/Beanie do not
- `RedisDeduplicator` exists in `varco_redis`
- `InMemoryDeduplicator` in `varco_core`
- No `SADeduplicator` or `BeanieDeduplicator` (reasonable — Redis is the right backend)

### AggregationApplicator — SA has it, Beanie does NOT
- `SQLAlchemyAggregationApplicator` exists in `varco_core.query.aggregation`
- No `BeanieAggregationApplicator` mapping `AggregationQuery` → MongoDB `$group` pipeline
- `AggregationQuery` is a fully modeled AST but Beanie backend cannot execute it

## Architecture Documentation Gaps

### varco_memcached missing from ARCHITECTURE.md
- Package exists at `/home/edoardo/projects/varco/varco_memcached/`
- Not mentioned anywhere in ARCHITECTURE.md
- Full feature: `MemcachedCache`, `MemcachedCacheSettings`, `MemcachedCacheConfiguration`
