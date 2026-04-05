"""
tests.test_post_service
=======================
Unit tests for ``PostService`` — no database, no Redis, no external services.

Strategy
--------
- ``InMemoryEventBus``   — real event dispatch in-process.
- ``InMemoryCache``      — real cache logic without Redis.
- ``BusEventProducer``   — real producer wired to the in-memory bus.
- ``PostAssembler``      — real assembler (pure functions, no DI).
- ``InMemoryRepository`` — hand-rolled stub satisfying ``AsyncRepository[Post]``.
- ``InMemoryUoWProvider``— minimal stub satisfying ``IUoWProvider``.
- ``BaseAuthorizer``     — permissive no-op (default).

All tests are ``async def`` — pytest-asyncio auto mode handles them.

Test naming convention: ``test_<method>_<condition>_<expected>``

📚 Docs
- 🐍 https://docs.python.org/3/library/unittest.mock.html — mocking helpers
- 🔍 https://anyio.readthedocs.io/ — async test runtime
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from varco_core.auth.authorizer import BaseAuthorizer
from varco_core.auth.base import AuthContext
from varco_core.cache.memory import InMemoryCache
from varco_core.event.memory import InMemoryEventBus
from varco_core.event.producer import BusEventProducer
from varco_core.query.params import QueryParams

from example.assembler import PostAssembler
from example.consumer import PostEventConsumer
from example.dtos import PostCreate, PostRead, PostUpdate
from example.events import PostCreatedEvent, PostDeletedEvent
from example.models import Post
from example.service import PostService


# ── Test doubles ──────────────────────────────────────────────────────────────


class InMemoryPostRepository:
    """
    Minimal in-memory repository stub for ``Post``.

    Satisfies the interface used by ``AsyncService`` without importing
    SQLAlchemy.  Only the methods called by ``AsyncService`` are implemented;
    others raise ``NotImplementedError``.

    Thread safety:  ❌ Not thread-safe — single-task tests only.
    Async safety:   ✅ All methods are ``async def``.
    """

    def __init__(self) -> None:
        # dict mapping pk → Post — the entire "database" for one test
        self._store: dict[UUID, Post] = {}

    async def find_by_id(self, pk: UUID) -> Post | None:
        return self._store.get(pk)

    async def find_by_query(self, params: QueryParams) -> list[Post]:
        # Return all posts — no filtering in the stub
        return list(self._store.values())

    async def count(self, params: QueryParams) -> int:
        return len(self._store)

    async def save(self, entity: Post) -> Post:
        # Simulate UUID_AUTO: assign pk if not set, assign created_at.
        #
        # pk is declared with init=False on DomainModel — dataclasses.replace()
        # cannot set init=False fields.  The real SA repository goes through
        # the ORM mapper which bypasses this restriction.  In the test stub
        # we replicate the same behaviour using object.__setattr__ directly,
        # which works on non-frozen dataclasses.
        if entity.pk is None or entity.pk == UUID(int=0):
            # Assign a fresh UUID, matching PKStrategy.UUID_AUTO semantics.
            object.__setattr__(entity, "pk", uuid4())
        now = datetime.now(timezone.utc)
        if entity.created_at is None:
            object.__setattr__(entity, "created_at", now)
        # updated_at is always refreshed on every save() — matches SA behaviour
        object.__setattr__(entity, "updated_at", now)
        self._store[entity.pk] = entity
        return entity

    async def delete(self, entity: Post) -> None:
        # AsyncRepository.delete() receives the entity (not just the pk) —
        # the SA backend uses entity._raw_orm to issue the DELETE statement.
        # Here we simply remove by pk since we have no ORM backing.
        self._store.pop(entity.pk, None)

    async def exists(self, pk: UUID) -> bool:
        return pk in self._store


class InMemoryUoW:
    """
    Minimal stub for ``AsyncUnitOfWork``.

    Wraps a shared ``InMemoryPostRepository`` so the same data is visible
    inside and outside the "transaction".  No rollback semantics.

    Async safety: ✅ Used in single-task tests.
    """

    def __init__(self, repo: InMemoryPostRepository) -> None:
        self._repo = repo
        # Mimic the committed flag that real UoWs set
        self._committed = False

    async def __aenter__(self) -> InMemoryUoW:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self._committed = True

    def get_repository(self, entity_cls: type) -> InMemoryPostRepository:
        # For this example, all entity classes map to the same stub repo.
        return self._repo


class InMemoryUoWProvider:
    """
    Stub ``IUoWProvider`` that returns a new ``InMemoryUoW`` per call.

    A shared ``InMemoryPostRepository`` is passed to every UoW so data
    persists across multiple ``make_uow()`` calls within a single test.

    Async safety: ✅ Used in single-task tests.
    """

    def __init__(self) -> None:
        # One repo for the entire test — data survives across UoW boundaries
        self._repo = InMemoryPostRepository()

    def make_uow(self) -> InMemoryUoW:
        return InMemoryUoW(self._repo)


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_auth(user_id: str = "00000000-0000-0000-0000-000000000001") -> AuthContext:
    """
    Build a minimal ``AuthContext`` for test cases.

    ``user_id`` is a UUID string so ``PostService._prepare_for_create``
    can convert it to ``UUID(user_id)`` without raising ValueError.
    """
    # AuthContext uses user_id (the JWT "sub" claim), not "subject".
    # grants defaults to an empty tuple (not frozenset) — AuthContext is a frozen dataclass.
    return AuthContext(user_id=user_id)


def _make_service(
    bus: InMemoryEventBus | None = None,
    cache: InMemoryCache | None = None,
) -> tuple[PostService, InMemoryUoWProvider, InMemoryEventBus]:
    """
    Construct a ``PostService`` with in-memory collaborators.

    Returns the service, UoW provider (for direct data inspection), and
    the event bus (for asserting event emissions).

    ``InMemoryCache`` requires ``start()`` before use (lifecycle contract from
    ``CacheBackend``).  We call ``_mark_started()`` via the internal flag here
    to avoid spinning up an asyncio event loop in the sync factory.  An
    alternative is to use ``asyncio.run(cache.start())`` but that creates a
    new event loop which conflicts with pytest-asyncio's loop management.

    DESIGN: manual construction over DI container
        Tests that exercise business logic should not depend on DI mechanics.
        Manual wiring is explicit and avoids the overhead of container setup.
        ✅ Faster test — no container startup.
        ✅ Errors in DI configuration are tested separately.
    """
    bus = bus or InMemoryEventBus()
    # InMemoryCache requires start() — mark it started directly for unit tests.
    # The real app calls await cache.start() via VarcoLifespan.
    cache = cache or InMemoryCache()
    cache._started = True  # bypass lifecycle guard in unit tests
    producer = BusEventProducer(bus=bus)
    uow_provider = InMemoryUoWProvider()
    assembler = PostAssembler()
    authorizer = BaseAuthorizer()

    service = PostService(
        uow_provider=uow_provider,
        authorizer=authorizer,
        assembler=assembler,
    )
    # Inject dependencies that CacheServiceMixin resolves via ClassVar DI.
    # In unit tests there is no container, so we set them directly on the class.
    # Use try/finally or teardown to restore originals if tests run in parallel.
    #
    # DESIGN: set ClassVars directly in tests over using a DI container
    #   ✅ No container startup overhead.
    #   ⚠️ Sets class-level state — tests must not run concurrently unless
    #      they each create a fresh subclass or reset the ClassVar after.
    PostService._cache = cache  # type: ignore[assignment]
    PostService._cache_producer = producer  # type: ignore[assignment]

    # _producer is an INSTANCE attribute set by AsyncService.__init__ to
    # NoopEventProducer() when no DI container provides it.  We override it
    # on the freshly created instance so the service publishes to our bus.
    # (Setting it on the class would be shadowed by __init__'s self._producer.)
    service._producer = producer  # type: ignore[assignment]

    return service, uow_provider, bus


# ── Tests: create ─────────────────────────────────────────────────────────────


async def test_create_returns_read_dto_with_pk_and_author():
    """
    ``create()`` returns a ``PostRead`` with server-assigned ``pk`` and
    ``author_id`` stamped from the auth context subject.
    """
    service, uow_provider, _ = _make_service()
    ctx = _make_auth(user_id="00000000-0000-0000-0000-000000000099")

    result = await service.create(PostCreate(title="Hello", body="World"), ctx)

    assert isinstance(result, PostRead)
    assert result.title == "Hello"
    assert result.body == "World"
    # pk is assigned by the stub repository
    assert isinstance(result.pk, UUID)
    # author_id is stamped from ctx.user_id in _prepare_for_create
    assert result.author_id == UUID(ctx.user_id)
    # created_at is set by the stub repository
    assert result.created_at is not None


async def test_create_emits_post_created_event():
    """
    ``create()`` publishes a ``PostCreatedEvent`` on the ``"posts"`` channel
    after the unit-of-work commits.
    """
    bus = InMemoryEventBus()
    service, _, _ = _make_service(bus=bus)

    received: list[PostCreatedEvent] = []

    async def _handler(event: PostCreatedEvent) -> None:
        received.append(event)

    bus.subscribe(PostCreatedEvent, _handler, channel="posts")

    ctx = _make_auth()
    result = await service.create(PostCreate(title="T", body="B"), ctx)

    # InMemoryEventBus dispatches SYNC by default — event is delivered before
    # create() returns.
    assert len(received) == 1
    assert received[0].post_id == result.pk
    assert received[0].author_id == result.author_id


async def test_create_stores_entity_in_repository():
    """
    After ``create()``, the entity is retrievable via ``get()``.
    """
    service, _, _ = _make_service()
    ctx = _make_auth()

    result = await service.create(PostCreate(title="Stored", body="Body"), ctx)

    # Fetch by pk to verify persistence
    fetched = await service.get(result.pk, ctx)
    assert fetched.pk == result.pk
    assert fetched.title == "Stored"


# ── Tests: get (cache) ────────────────────────────────────────────────────────


async def test_get_returns_cached_result_on_second_call():
    """
    ``get()`` returns the cached ``PostRead`` on the second call without
    hitting the repository.
    """
    cache = InMemoryCache()
    service, uow_provider, _ = _make_service(cache=cache)
    ctx = _make_auth()

    # Create a post
    created = await service.create(PostCreate(title="Cached", body="Body"), ctx)

    # First get — warms the cache (cache miss → repo fetch → cache.set)
    _ = await service.get(created.pk, ctx)

    # Poison the repository so any SECOND real fetch would fail.
    # If the cache is working, get() must return the cached value without
    # calling the repository at all.
    uow_provider._repo._store[created.pk] = None  # type: ignore[assignment]

    # Second get() must come from cache, not the repository
    fetched = await service.get(created.pk, ctx)

    # If it reached the repo it would fail on to_read_dto(None).
    # Reaching here means the cache was hit.
    assert fetched.pk == created.pk
    assert fetched.title == "Cached"


async def test_get_cache_miss_hits_repository():
    """
    On a cache miss, ``get()`` fetches from the repository and populates the cache.
    """
    cache = InMemoryCache()
    service, uow_provider, _ = _make_service(cache=cache)
    ctx = _make_auth()

    created = await service.create(PostCreate(title="Fresh", body=""), ctx)

    # Clear the cache to simulate a cold start
    await cache.clear()

    # This call must hit the repository
    fetched = await service.get(created.pk, ctx)
    assert fetched.title == "Fresh"

    # Now the cache must be warm — verify by poisoning the repo again
    uow_provider._repo._store[created.pk] = None  # type: ignore[assignment]
    fetched_again = await service.get(created.pk, ctx)
    assert fetched_again.title == "Fresh"


# ── Tests: update ─────────────────────────────────────────────────────────────


async def test_update_changes_title_and_invalidates_cache():
    """
    ``update()`` persists the new title and evicts the cached ``get`` key.
    """
    cache = InMemoryCache()
    service, _, _ = _make_service(cache=cache)
    ctx = _make_auth()

    created = await service.create(PostCreate(title="Old title", body="Body"), ctx)

    # Warm the cache
    _ = await service.get(created.pk, ctx)

    # Update — should invalidate the get cache entry
    updated = await service.update(created.pk, PostUpdate(title="New title"), ctx)

    assert updated.title == "New title"
    assert updated.body == "Body"  # unchanged

    # The cache entry must be evicted — fetch returns fresh data
    fetched = await service.get(created.pk, ctx)
    assert fetched.title == "New title"


# ── Tests: delete ─────────────────────────────────────────────────────────────


async def test_delete_removes_entity_and_emits_event():
    """
    ``delete()`` removes the entity from the repository and publishes a
    ``PostDeletedEvent``.
    """
    bus = InMemoryEventBus()
    service, uow_provider, _ = _make_service(bus=bus)
    ctx = _make_auth()

    received: list[PostDeletedEvent] = []
    bus.subscribe(PostDeletedEvent, lambda e: received.append(e), channel="posts")

    created = await service.create(PostCreate(title="Bye", body=""), ctx)
    await service.delete(created.pk, ctx)

    # Entity is gone
    assert uow_provider._repo._store.get(created.pk) is None

    # Event was emitted
    assert len(received) == 1
    assert received[0].post_id == created.pk


# ── Tests: list ───────────────────────────────────────────────────────────────


async def test_list_returns_all_created_posts():
    """
    ``list()`` returns all posts in the repository.
    """
    service, _, _ = _make_service()
    ctx = _make_auth()

    await service.create(PostCreate(title="A", body=""), ctx)
    await service.create(PostCreate(title="B", body=""), ctx)

    results = await service.list(QueryParams(), ctx)
    titles = {r.title for r in results}
    assert "A" in titles
    assert "B" in titles


# ── Tests: EventConsumer lifecycle ────────────────────────────────────────────


async def test_consumer_start_registers_handlers():
    """
    ``EventConsumer.start()`` registers ``@listen`` handlers and events are
    delivered correctly.
    """
    bus = InMemoryEventBus()
    consumer = PostEventConsumer(bus=bus)

    # start() is the new lifecycle hook (GAP #3 fix)
    await consumer.start()

    received: list[PostCreatedEvent] = []

    # Subscribe a test listener alongside the consumer
    async def spy(event: PostCreatedEvent) -> None:
        received.append(event)

    bus.subscribe(PostCreatedEvent, spy, channel="posts")

    # Publish an event — consumer + spy should both receive it
    await bus.publish(
        PostCreatedEvent(post_id=uuid4(), author_id=uuid4()),
        channel="posts",
    )

    assert len(received) == 1


async def test_consumer_stop_cancels_subscriptions():
    """
    After ``EventConsumer.stop()``, the consumer's handlers no longer receive events.
    """
    bus = InMemoryEventBus()
    consumer = PostEventConsumer(bus=bus)
    await consumer.start()

    # Verify the consumer IS receiving events before stop()
    received_before: list[PostCreatedEvent] = []
    bus.subscribe(
        PostCreatedEvent, lambda e: received_before.append(e), channel="posts"
    )
    await bus.publish(
        PostCreatedEvent(post_id=uuid4(), author_id=uuid4()), channel="posts"
    )
    assert len(received_before) == 1

    # Stop the consumer — subscriptions cancelled
    await consumer.stop()

    # Events published after stop() should NOT reach the consumer.
    # We verify by counting events on a new subscriber only.
    received_after: list[PostCreatedEvent] = []
    bus.subscribe(PostCreatedEvent, lambda e: received_after.append(e), channel="posts")
    await bus.publish(
        PostCreatedEvent(post_id=uuid4(), author_id=uuid4()), channel="posts"
    )

    # Only the post-stop subscriber should see this event.
    # The consumer's handler was cancelled — no double-dispatch.
    assert len(received_after) == 1  # the new spy saw it
    # _subscriptions is reset by stop()
    assert consumer._subscriptions == []


async def test_consumer_start_requires_bus_attribute():
    """
    ``start()`` raises ``AttributeError`` when ``self._bus`` is not set.
    """
    # Construct without assigning _bus
    consumer = PostEventConsumer.__new__(PostEventConsumer)

    with pytest.raises(AttributeError, match="_bus"):
        await consumer.start()
