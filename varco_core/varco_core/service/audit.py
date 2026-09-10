"""
varco_core.service.audit
========================
Audit / Mutation Log infrastructure for the varco service layer.

Provides three components that together implement a complete audit trail:

``AuditEntry``
    Frozen value object.  Stores the persisted form of an ``AuditEvent``.

``AuditRepository``
    ABC.  Implement against your storage backend (SA, Beanie, etc.).

``AuditLogMixin``
    Service mixin.  Overrides ``_after_create``, ``_after_update``, and
    ``_after_delete`` to emit ``AuditEvent`` instances via the service's
    existing ``AbstractEventProducer``.  Compose via MRO with ``AsyncService``.

``AuditConsumer``
    ``EventConsumer`` subclass.  Listens to ``AuditEvent`` on the
    ``"varco.audit"`` channel and persists each event as an ``AuditEntry``
    via an injected ``AuditRepository``.

Wiring example::

    class OrderService(
        AuditLogMixin,
        AsyncService[Order, UUID, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
    ):
        def _get_repo(self, uow): return uow.orders

        def _get_audit_actor(self, ctx): return ctx.sub  # JWT subject

    # Wire the consumer
    audit_consumer = AuditConsumer(bus=event_bus, audit_repo=SAuditRepository(session))
    audit_consumer.register_to(event_bus)

DESIGN: emit via AbstractEventProducer (not direct AuditRepository write)
    ✅ Decoupled — the service does not need to know the AuditRepository type.
    ✅ Retry + DLQ + inbox pattern all apply to AuditConsumer's @listen handler.
    ✅ Works with existing event bus infrastructure; no new write path needed.
    ❌ Adds latency — audit record is eventually consistent (async consumer).
       For synchronous audit needs, override ``_after_create`` directly.

DESIGN: AuditLogMixin calls super() on each hook
    ✅ MRO-safe — multiple mixins can chain audit hooks without overriding
       each other.  Always call ``await super()._after_create(...)`` at the END
       of each override so the MRO chain completes.

Thread safety:  ❌ Not thread-safe.  Use from a single event loop.
Async safety:   ✅ All mixin hooks and consumer methods are ``async def``.

📚 Docs
- 🐍 https://docs.python.org/3/library/dataclasses.html
  dataclasses — used for ``AuditEntry`` value object.
- 📐 https://microservices.io/patterns/data/event-sourcing.html
  Event Sourcing / Audit Log pattern.
"""

from __future__ import annotations

import builtins
import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID, uuid4

from varco_core.event.audit_event import AuditEvent
from varco_core.event.base import AbstractEventBus, Event, Subscription
from varco_core.event.consumer import EventConsumer, listen
from varco_core.redaction import redact_mapping
from varco_core.resilience import RetryPolicy
from varco_core.service.mixin import ServiceMixin

if TYPE_CHECKING:
    from varco_core.auth import AuthContext
    from varco_core.dto import ReadDTO
    from varco_core.event.dlq import AbstractDeadLetterQueue
    from varco_core.model import DomainModel
    from varco_core.redaction import Redactor

# Sentinel distinguishing "retry_policy/dlq kwarg omitted" (apply the
# class-level safe-by-default policy) from "explicitly passed None" (a
# deliberate opt-out restoring fire-and-forget) — same pattern as
# varco_fastapi.auth.server_auth's _AUDIENCE_UNSET (Plan 005 Phase 2).
_UNSET: Any = object()

_logger = logging.getLogger(__name__)

# Channel for AuditConsumer.  Using a dedicated channel avoids interference
# with domain event channels and lets operators route audit events separately.
_AUDIT_CHANNEL: str = "varco.audit"


# ── AuditEntry ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AuditEntry:
    """
    Immutable value object representing a persisted audit record.

    Constructed from an ``AuditEvent`` by ``AuditEntry.from_event()``.
    Stored by ``AuditRepository.save()`` in the backing DB table.

    DESIGN: frozen dataclass over a Pydantic model
        ✅ No Pydantic dependency in the service layer.
        ✅ Hashable — safe to use in sets or as dict keys.
        ❌ No built-in JSON serialization — backends use their own ORM mapping.

    Thread safety:  ✅ Frozen — immutable after construction.
    Async safety:   ✅ Pure value object; no I/O.

    Attributes:
        entry_id:       Unique identifier for this audit row.
        entity_type:    Name of the mutated entity class (e.g. ``"Order"``).
        entity_id:      String representation of the entity primary key.
        action:         One of ``"create"``, ``"update"``, ``"delete"``.
        actor_id:       Identity of the caller.  ``None`` for system-initiated.
        diff:           Field-level change data.  Structure varies by action.
        occurred_at:    UTC timestamp when the audit event was emitted.
        correlation_id: Optional request-tracing identifier.
        tenant_id:      Optional tenant identifier.

    Example::

        entry = AuditEntry.from_event(audit_event)
        await audit_repo.save(entry)
    """

    entry_id: UUID = field(default_factory=uuid4)
    entity_type: str = ""
    entity_id: str = ""
    action: str = ""  # "create" | "update" | "delete"
    actor_id: str | None = None
    diff: dict[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    correlation_id: str | None = None
    tenant_id: str | None = None

    prev_hash: str | None = None
    """Plan 009, Phase 12 (R8) — the previous chain entry's ``entry_hash()``.
    ``None`` for the genesis entry (or any entry outside a chained
    deployment — ``hash_chain=False`` is the default). Established by the
    repository's ``save()`` under a backend-level serialization guarantee
    (RD-8), never computed by the consumer."""

    seq: int | None = None
    """Plan 009, Phase 12 (R8) — monotone sequence number establishing total
    order for the chain. ``None`` outside a chained deployment."""

    def entry_hash(self) -> str:
        """
        SHA-256 over a canonical JSON encoding of this entry's chain-relevant
        fields (Plan 009, Phase 12 / R8).

        Canonical form: sorted keys, no whitespace, RFC 3339 UTC timestamps.
        Fields hashed, in order: ``entry_id``, ``occurred_at``, ``action``,
        ``entity_type``, ``entity_id``, ``actor_id``, ``tenant_id``,
        ``correlation_id``, ``diff``, ``prev_hash``. The genesis entry's
        ``prev_hash=None`` hashes as the JSON literal ``null``.

        Returns:
            Lowercase hex SHA-256 digest.
        """
        import hashlib
        import json

        payload = {
            "entry_id": str(self.entry_id),
            "occurred_at": (
                self.occurred_at.astimezone(UTC).isoformat() if self.occurred_at else None
            ),
            "action": self.action,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "actor_id": self.actor_id,
            "tenant_id": self.tenant_id,
            "correlation_id": self.correlation_id,
            "diff": self.diff,
            "prev_hash": self.prev_hash,
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def from_event(cls, event: AuditEvent) -> AuditEntry:
        """
        Construct an ``AuditEntry`` from an ``AuditEvent``.

        Args:
            event: The ``AuditEvent`` to convert.

        Returns:
            A fully populated ``AuditEntry`` ready for persistence.

        Edge cases:
            - ``occurred_at`` is set to ``now()`` at conversion time —
              NOT to ``event.event_time``.  This records when the consumer
              persisted the event, not when the service emitted it.  Use
              ``event.event_time`` for the emission timestamp if needed.
        """
        return cls(
            entity_type=event.entity_type,
            entity_id=event.entity_id,
            action=event.action,
            actor_id=event.actor_id,
            # Copy diff by value — AuditEvent.diff is a mutable dict even
            # though AuditEvent itself is immutable.  dict() ensures the
            # AuditEntry's diff is independent.
            diff=dict(event.diff),
            correlation_id=event.correlation_id,
            tenant_id=event.tenant_id,
        )


@dataclass(frozen=True)
class ChainGap:
    """A missing ``seq`` in an audit hash chain — e.g. a deleted row (Phase 12)."""

    expected_seq: int
    found_seq: int | None


@dataclass(frozen=True)
class HashMismatch:
    """A ``prev_hash`` that does not match the prior entry's actual hash — e.g. an edited row (Phase 12)."""

    seq: int | None
    expected_prev_hash: str | None
    actual_prev_hash: str | None


# ── AuditRepository ───────────────────────────────────────────────────────────


class AuditRepository(ABC):
    """
    Abstract persistence contract for the audit log store.

    Implement this against your DB backend:
    - ``varco_sa``: ``SAAuditRepository`` using ``AsyncSession``.

    Thread safety:  ⚠️ Implementations must not share state between concurrent
                       callers — use one repository instance per task/request.
    Async safety:   ✅ All methods are ``async def``.
    """

    @abstractmethod
    async def save(self, entry: AuditEntry) -> None:
        """
        Persist an audit entry.

        Args:
            entry: The ``AuditEntry`` to persist.

        Raises:
            Exception: Any DB-level exception from the underlying driver
                propagates unchanged.

        Edge cases:
            - Should be idempotent on ``entry_id`` collision — use INSERT
              OR IGNORE / INSERT ON CONFLICT DO NOTHING.
        """

    @abstractmethod
    async def list_for_entity(
        self,
        entity_type: str,
        entity_id: str,
        *,
        limit: int = 100,
        tenant_id: str | None = None,
    ) -> list[AuditEntry]:
        """
        Return audit entries for a specific entity, newest-first.

        Args:
            entity_type: Entity class name to filter by.
            entity_id:   Entity primary key string to filter by.
            limit:       Maximum number of entries to return.  Default ``100``.
            tenant_id:   Plan 009 (R4) — **breaking** keyword-only addition.
                         ``None`` (default) means no tenant filter — the
                         pre-existing, unscoped behaviour. An out-of-tree
                         subclass that does not accept this parameter breaks
                         loudly (``TypeError``) at call time rather than
                         silently ignoring the tenant filter, which is the
                         security bug this change exists to fix.

        Returns:
            List of ``AuditEntry`` objects ordered by ``occurred_at DESC``.
            Empty list if no audit records exist for the given entity.

        Edge cases:
            - Sorting without a DB index on ``(entity_type, entity_id, occurred_at)``
              is O(N) — add an index in production schemas.
        """

    async def list(
        self,
        *,
        actor_id: str | None = None,
        action: str | None = None,
        entity_type: str | None = None,
        entity_id: str | None = None,
        tenant_id: str | None = None,
        correlation_id: str | None = None,
        occurred_from: datetime | None = None,
        occurred_to: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[AuditEntry]:
        """
        General-purpose filtered scan of the audit log (Plan 009, Phase 10 / R6).

        Concrete-but-raising — no portable scan primitive exists on the ABC
        to build a default from (unlike ``list_for_entity``, which every
        backend already indexes on ``(entity_type, entity_id)``). Drives
        ``build_audit_router()``'s ``GET /audit/entries``.

        Args:
            actor_id: Optional equality filter on the acting principal.
            action: Optional equality filter on the mutation kind.
            entity_type: Optional equality filter on the entity's type.
            entity_id: Optional equality filter on the entity's primary key.
            tenant_id: Optional equality filter on the owning tenant.
            correlation_id: Optional equality filter on the correlation id.
            occurred_from: Optional lower bound of the ``occurred_at`` range.
            occurred_to: Optional upper bound of the ``occurred_at`` range.
            limit: Maximum entries to return.
            offset: Pagination offset.

        Raises:
            NotImplementedError: unless overridden.
        """
        raise NotImplementedError(f"{type(self).__name__} does not implement list().")

    async def delete_where(
        self,
        *,
        older_than: datetime | None = None,
        entity_type: str | None = None,
        tenant_id: str | None = None,
        limit: int | None = None,
        allow_chain_break: bool = False,
    ) -> int:
        """
        Bulk-delete audit entries matching every given predicate (retention).

        Concrete-but-raising, same reasoning as
        ``AbstractDeadLetterQueue.delete_where`` — a destructive bulk
        operation has no safe portable default.

        Args:
            older_than:  Only entries with ``occurred_at`` before this time.
            entity_type: Match one entity type.
            tenant_id:   Match one tenant.
            limit:       Cap on rows deleted — chunk a large sweep.
            allow_chain_break: Plan 009, Phase 12 (R8) — on a hash-chained
                table, pruning breaks the chain by construction (a deleted
                row is indistinguishable from a ``ChainGap`` at
                ``verify_chain()`` time). Backends with ``hash_chain=True``
                must raise ``ValueError`` unless this is explicitly ``True``
                — a defaulted parameter, so a non-chained backend (or one
                that never implemented hash-chaining at all) is unaffected.

        Returns:
            Number of entries deleted.

        Raises:
            NotImplementedError: unless overridden.
            ValueError: no predicate at all was given, OR the backend is
                hash-chained and ``allow_chain_break`` was not set.
        """
        if older_than is None and entity_type is None and tenant_id is None:
            raise ValueError(
                "delete_where() requires at least one predicate "
                "(older_than/entity_type/tenant_id) — refusing to delete "
                "every entry."
            )
        raise NotImplementedError(f"{type(self).__name__} does not implement delete_where().")

    @staticmethod
    def verify_chain(
        entries: Sequence[AuditEntry],
    ) -> Literal[True] | builtins.list[ChainGap | HashMismatch]:
        # `builtins.list`, not `list`: this class defines an instance method
        # named `list()` (above), which shadows the builtin `list` name for
        # mypy's annotation resolution within this class body — same footgun
        # class as the `object`-shadowing fix in varco_casbin/router.py.
        """
        Verify a hash chain over already-fetched entries (Plan 009, Phase 12 / R8).

        Portable default (pure, ``@staticmethod``) — pure recomputation over
        value objects already returned by ``list()``/``list_for_entity()``,
        correct for every backend by construction: ``entry_hash()`` is a
        deterministic pure function of an entry's own fields.

        Args:
            entries: Entries in ``seq`` ascending order.

        Returns:
            ``True`` when the chain is fully unbroken (including the
            vacuous empty-list case). Otherwise a non-empty list of
            findings — ``ChainGap`` (a missing ``seq``, e.g. a deleted row)
            and ``HashMismatch`` (a ``prev_hash`` that does not match the
            prior entry's actual hash, e.g. an edited row) are reported as
            distinct finding types since they are different incidents.
        """
        if not entries:
            return True

        # Callers commonly fetch via list()/list_for_entity(), which order
        # newest-first for browsing — sort into seq-ascending chain order
        # here so verify_chain() never depends on the caller's own query
        # ordering. Entries with seq=None (unchained deployment) sort last,
        # stably, in whatever relative order they arrived.
        entries = sorted(entries, key=lambda e: (e.seq is None, e.seq if e.seq is not None else 0))

        findings: list[ChainGap | HashMismatch] = []
        prev_entry: AuditEntry | None = None
        prev_computed_hash: str | None = None

        for entry in entries:
            if prev_entry is not None and prev_entry.seq is not None and entry.seq is not None:
                expected_seq = prev_entry.seq + 1
                if entry.seq != expected_seq:
                    findings.append(ChainGap(expected_seq=expected_seq, found_seq=entry.seq))

            expected_prev_hash = prev_computed_hash if prev_entry is not None else None
            if entry.prev_hash != expected_prev_hash:
                findings.append(
                    HashMismatch(
                        seq=entry.seq,
                        expected_prev_hash=expected_prev_hash,
                        actual_prev_hash=entry.prev_hash,
                    )
                )

            prev_entry = entry
            prev_computed_hash = entry.entry_hash()

        return findings or True


# ── AuditLogMixin ─────────────────────────────────────────────────────────────


class AuditLogMixin(ServiceMixin):
    """
    Service mixin that emits ``AuditEvent`` on each entity mutation.

    Overrides ``_after_create``, ``_after_update``, and ``_after_delete``
    (added to ``AsyncService`` as no-op hooks) to publish an ``AuditEvent``
    via the service's ``_producer`` (``AbstractEventProducer``).

    Compose via MRO — place ``AuditLogMixin`` to the LEFT of ``AsyncService``
    in the class header so its hooks run before the base no-op::

        class OrderService(
            AuditLogMixin,
            AsyncService[Order, UUID, ...],
        ):
            ...

    DESIGN: publish via _producer (not write directly to AuditRepository)
        ✅ The mixin has no DB coupling — it only needs an AbstractEventProducer.
        ✅ The audit event flows through retry + DLQ + inbox wrappers on
           AuditConsumer — reliable persistence without mixin complexity.
        ❌ Audit record is eventually consistent — not in the same transaction
           as the entity write.  For strict consistency, override the hook to
           write to AuditRepository directly inside the same UoW instead.

    DESIGN: _get_audit_actor() override pattern
        ✅ Provides a safe default (None) so the mixin works out of the box.
        ✅ Concrete services override only what they need — no forced dependency
           on a specific AuthContext schema.
        ❌ Must be overridden to get meaningful actor_id — base returns None.

    Thread safety:  ❌ Inherits from AsyncService — use from a single event loop.
    Async safety:   ✅ All override hooks are ``async def``.

    Edge cases:
        - If ``_produce`` raises, the exception propagates to the caller —
          the entity IS already persisted (hooks run after commit).
        - Chain ALL hooks with ``await super()._after_*()`` — other mixins
          in the MRO may also override these hooks.
        - ``_get_audit_actor`` is NOT async — keep it pure / synchronous.
    """

    #: Plan 040 / S21, §D-S21-audit — the docstring's promise, finally kept.
    #: ``None`` (default) means "no redaction" — every ``diff`` is written
    #: byte-identical to pre-3.2 behaviour. Set to a ``Redactor`` (e.g.
    #: ``PolicyRedactor()``) on a concrete service to redact sensitive
    #: fields in that service's audit diffs going forward.
    #:
    #: A class attribute, not a constructor parameter: ``AuditLogMixin`` has
    #: no ``__init__``, and adding one would break MRO composition with
    #: ``ValidatorServiceMixin``/``TenantAwareService``/``SoftDeleteService``
    #: (CLAUDE.md's mixin-composition rule). Deliberately **opt-in**, not
    #: on by default — three independent reasons, all argued in
    #: §D-S21-audit: (1) the incumbent substring matcher has real false
    #: positives on domain-named fields (``"pin"`` matches
    #: ``shipping_address``) that would corrupt audit data by default; (2)
    #: flipping this changes what is *persisted*, which is irreversible for
    #: rows already written — it fails the "cheap caller-side fix" test a
    #: safe default must pass; (3) it interacts with the hash chain
    #: (§D-S21-hashchain) and deserves the deliberateness of an explicit
    #: opt-in rather than a silent default flip.
    _audit_redactor: Redactor | None = None

    def _audit_diff(self, action: str, diff: dict[str, Any]) -> dict[str, Any]:
        """
        Transform a diff before it is emitted as an ``AuditEvent`` — the
        real hook the previous docstring here promised as
        ``_get_audit_diff_create()``, a method that never existed anywhere
        in the repo (Plan 040 / S21, §D-S21-audit).

        DESIGN: one hook for all three actions, not three
            ✅ ``_after_delete`` writes ``{}`` and ``_after_update`` writes a
               two-key ``{"before": ..., "after": ...}`` nesting — three
               hooks named after the docstring's phantom shape would be
               three near-identical bodies. One hook, dispatched on
               ``action``, covers all three.
            ✅ Overriding this hook wins outright — the default body below
               is the ONLY caller of ``self._audit_redactor``, so a
               subclass that overrides ``_audit_diff`` controls its own
               redaction entirely, including ignoring the class attribute.

        DESIGN: called BEFORE ``_produce`` — the only legal redaction point
        (§D-S21-hashchain)
            ✅ This is the only point where the diff has not yet crossed the
               event bus and may not yet sit in a broker/DLQ/outbox row.
               Redacting in ``AuditConsumer`` or a repository's ``save()``
               would leave the secret on the wire even if the stored row
               were clean.
            ✅ Because redaction happens before ``AuditEntry.from_event()``,
               it happens before ``seq``/``prev_hash``/``entry_hash()``
               exist — the chain always hashes exactly what is stored, so a
               chain spanning the redactor's enablement verifies as
               ``True`` (asserted:
               ``varco_core/tests/test_audit_redaction.py``).
            ⛔ **Never call this — or apply any redaction — on the READ
               path** (``list()``/``list_for_entity()``/an admin router).
               ``AuditRepository.verify_chain()`` recomputes ``entry_hash()``
               from returned fields; mutating a returned entry's ``diff``
               before verification produces a spurious ``HashMismatch`` on
               every row that looks like tampering.

        Args:
            action: ``"create"``, ``"update"``, or ``"delete"``.
            diff: The freshly built diff dict for this emission.

        Returns:
            ``diff`` unchanged when ``self._audit_redactor is None``
            (byte-identical to pre-3.2); otherwise
            ``redact_mapping(diff, self._audit_redactor)``.
        """
        if self._audit_redactor is None:
            return diff
        return redact_mapping(diff, self._audit_redactor)

    def _get_audit_actor(self, ctx: AuthContext) -> str | None:
        """
        Extract the actor identity from the auth context.

        Override in concrete service classes to return the caller's identifier
        (e.g. ``ctx.sub`` for JWT subject, ``ctx.metadata.get("user_id")``).

        Args:
            ctx: Caller's ``AuthContext``.

        Returns:
            Actor identifier string, or ``None`` if not determinable.

        Edge cases:
            - Returns ``None`` by default — override or the audit record will
              have ``actor_id=None`` (acceptable for system-initiated mutations).
        """
        # Base: no actor extraction — override in concrete service.
        return None

    async def _after_create(
        self,
        entity: DomainModel,
        read_dto: ReadDTO,
        ctx: AuthContext,
    ) -> None:
        """
        Emit an ``AuditEvent`` for the entity creation.

        Args:
            entity:   The newly created domain entity (with pk assigned).
            read_dto: The ``ReadDTO`` returned to the caller.
            ctx:      Caller's identity.

        Edge cases:
            - ``diff`` contains the full ``read_dto.model_dump()`` — every field
              visible to the caller is recorded, UNLESS redacted: set
              ``self._audit_redactor`` (e.g. ``PolicyRedactor()``) or
              override ``_audit_diff()`` (Plan 040 / S21, §D-S21-audit).
              Default (``_audit_redactor is None``) is byte-identical to
              pre-3.2 — the full, unredacted diff.
        """
        await self._producer._produce(  # type: ignore[attr-defined]
            AuditEvent(
                entity_type=type(entity).__name__,
                entity_id=str(entity.pk),
                action="create",
                actor_id=self._get_audit_actor(ctx),
                # Record the full read_dto fields as the creation diff,
                # through _audit_diff() (no-op unless a redactor is set).
                diff=self._audit_diff("create", read_dto.model_dump()),
                tenant_id=ctx.metadata.get("tenant_id") if ctx else None,
            ),
            channel=_AUDIT_CHANNEL,
        )
        await super()._after_create(entity, read_dto, ctx)  # type: ignore[misc]

    async def _after_update(
        self,
        before_dto: ReadDTO,
        entity: DomainModel,
        read_dto: ReadDTO,
        ctx: AuthContext,
    ) -> None:
        """
        Emit an ``AuditEvent`` for the entity update, including before/after diff.

        Args:
            before_dto: ``ReadDTO`` before the update was applied.
            entity:     The saved entity after update.
            read_dto:   The ``ReadDTO`` returned to the caller (post-update).
            ctx:        Caller's identity.

        Edge cases:
            - ``diff["before"]`` and ``diff["after"]`` contain the full
              ``model_dump()`` of each DTO — not just changed fields.  A
              field-level diff can be computed by comparing the two dicts.
            - Redaction (``self._audit_redactor``/``_audit_diff()``) is
              applied to the assembled ``{"before": ..., "after": ...}``
              dict, so a sensitive field is redacted in both halves
              (Plan 040 / S21, §D-S21-audit).
        """
        await self._producer._produce(  # type: ignore[attr-defined]
            AuditEvent(
                entity_type=type(entity).__name__,
                entity_id=str(entity.pk),
                action="update",
                actor_id=self._get_audit_actor(ctx),
                diff=self._audit_diff(
                    "update",
                    {
                        "before": before_dto.model_dump(),
                        "after": read_dto.model_dump(),
                    },
                ),
                tenant_id=ctx.metadata.get("tenant_id") if ctx else None,
            ),
            channel=_AUDIT_CHANNEL,
        )
        await super()._after_update(before_dto, entity, read_dto, ctx)  # type: ignore[misc]

    async def _after_delete(self, pk: Any, ctx: AuthContext) -> None:
        """
        Emit an ``AuditEvent`` for the entity deletion.

        Args:
            pk:  Primary key of the deleted entity.
            ctx: Caller's identity.

        Edge cases:
            - ``diff`` is empty (``{}``) — the entity is gone at hook call time.
              If you need to record the final state, fetch it in ``delete()``
              before the UoW commits and pass it here via a subclass override.
        """
        await self._producer._produce(  # type: ignore[attr-defined]
            AuditEvent(
                entity_type=self._entity_type().__name__,  # type: ignore[attr-defined]
                entity_id=str(pk),
                action="delete",
                actor_id=self._get_audit_actor(ctx),
                # Entity is gone — no fields to record; _audit_diff({}) is
                # a no-op through redact_mapping's empty-mapping fast path.
                diff=self._audit_diff("delete", {}),
                tenant_id=ctx.metadata.get("tenant_id") if ctx else None,
            ),
            channel=_AUDIT_CHANNEL,
        )
        await super()._after_delete(pk, ctx)  # type: ignore[misc]


# ── AuditConsumer ─────────────────────────────────────────────────────────────


class AuditConsumer(EventConsumer):
    """
    ``EventConsumer`` that persists ``AuditEvent`` instances to an ``AuditRepository``.

    Wire via ``register_to(bus)`` after constructing with an ``AuditRepository``
    implementation.  The ``@listen`` decorator subscribes only to events whose
    ``__event_type__`` matches ``"varco.audit"`` on the ``"varco.audit"`` channel.

    DESIGN: dedicated consumer over inline repository write in AuditLogMixin
        ✅ Separation of concerns — the mixin emits; the consumer persists.
        ✅ Retry + DLQ wrappers on ``@listen`` provide reliable persistence.
        ✅ Consumer can be deployed on a separate process / pod from the service.
        ❌ Eventual consistency — audit record lands after the event is consumed,
           not atomically with the entity write.

    DESIGN: safe-by-default retry + DLQ (Plan 005 Phase 3, U-6 §2)
        "Safe-by-default is the right polarity for an audit trail" — losing an
        audit record silently is worse than retrying too eagerly. ``register_to()``
        applies ``_default_retry_policy = RetryPolicy.durable_delivery()`` and
        the constructor's ``dlq`` unless the caller explicitly overrides either.
        ✅ A transient DB error no longer silently drops an audit record.
        ✅ Fire-and-forget is still available, explicitly: pass
           ``retry_policy=None`` to ``register_to()``.
        ❌ Changes behaviour on upgrade for callers who never configured
           retry — the failure path only (a succeeding handler is untouched).

    Thread safety:  ❌ Not thread-safe.  Use from a single event loop.
    Async safety:   ✅ Handler is ``async def``.

    Args:
        audit_repo: ``AuditRepository`` implementation for persistence.
        dlq:        Optional ``AbstractDeadLetterQueue`` — entries that
                    exhaust ``_default_retry_policy`` (or an override passed
                    to ``register_to()``) are pushed here with
                    ``source=DeadLetterSource.CONSUMER``. ``None`` (default)
                    — exhausted retries raise ``RetryExhaustedError`` per the
                    normal ``@listen`` DLQ-less contract.

    Edge cases:
        - ``register_to`` must be called before any events are published —
          the subscription is created at wiring time, not at handler call time.
        - Pass ``retry_policy=None`` to ``register_to()`` **explicitly** to
          restore fire-and-forget (single attempt, no DLQ) for this one
          registration — distinct from omitting the kwarg, which applies the
          safe-by-default policy.

    Example::

        consumer = AuditConsumer(audit_repo=SAuditRepository(session), dlq=my_dlq)
        consumer.register_to(event_bus)   # retries + DLQs by default

        # Explicit fire-and-forget opt-out:
        consumer.register_to(event_bus, retry_policy=None)
    """

    # Safe-by-default retry policy — applied by register_to() unless the
    # caller passes retry_policy=None explicitly (see _UNSET sentinel above).
    _default_retry_policy: RetryPolicy = RetryPolicy.durable_delivery()

    def __init__(
        self, *, audit_repo: AuditRepository, dlq: AbstractDeadLetterQueue | None = None
    ) -> None:
        """
        Args:
            audit_repo: Repository implementation used to persist audit entries.
            dlq:        Optional DLQ applied by ``register_to()`` unless the
                        caller overrides it there.
        """
        # Stored as an instance attribute so the @listen handler can access it
        # via self — the handler is a bound method resolved at register_to time.
        self._audit_repo = audit_repo
        self._dlq = dlq

    def register_to(
        self,
        bus: AbstractEventBus,
        *,
        retry_policy: RetryPolicy | None = _UNSET,
        dlq: AbstractDeadLetterQueue | None = _UNSET,
    ) -> list[Subscription]:
        """
        Wire this consumer to ``bus``, applying the safe-by-default
        ``_default_retry_policy`` / constructor ``dlq`` unless overridden.

        Args:
            bus:          The ``AbstractEventBus`` to register against.
            retry_policy: ``_UNSET`` (default, i.e. omitted) applies
                          ``_default_retry_policy`` (``RetryPolicy.
                          durable_delivery()``). Pass ``None`` **explicitly**
                          to opt out (fire-and-forget, today's pre-Phase-3
                          behaviour). Pass any other ``RetryPolicy`` to use it
                          instead of the default.
            dlq:          Same ``_UNSET``-vs-``None`` distinction, independently
                          — ``_UNSET`` uses ``self._dlq`` (from the
                          constructor); explicit ``None`` disables the DLQ for
                          this registration only.
        """
        effective_dlq = self._dlq if dlq is _UNSET else dlq

        if retry_policy is None and effective_dlq is None:
            # Explicit, dlq-less opt-out — true fire-and-forget: the handler
            # is attempted once and any failure is logged, never propagated.
            # This is NOT the same as the base EventConsumer's "no wrapper"
            # path (which re-raises through bus.publish() per the bus's
            # error_policy) — an audit sink flaking must never break the
            # caller's write path, which is the whole point of "restores
            # fire-and-forget" rather than "restores raw/unwrapped".
            return [self._subscribe_fire_and_forget(bus)]

        effective_retry_policy = (
            self._default_retry_policy if retry_policy is _UNSET else retry_policy
        )
        return super().register_to(bus, retry_policy=effective_retry_policy, dlq=effective_dlq)

    def _subscribe_fire_and_forget(self, bus: AbstractEventBus) -> Subscription:
        """Subscribe ``on_audit_event`` wrapped to swallow-and-log any
        failure — used only by the explicit ``retry_policy=None`` opt-out
        path in ``register_to()``."""

        async def _safe_on_audit_event(event: Event) -> None:
            try:
                await self.on_audit_event(event)
            except Exception as exc:  # noqa: BLE001
                _logger.warning(
                    "AuditConsumer: fire-and-forget handler failed for "
                    "event_id=%s — swallowed (retry_policy=None opt-out): %s",
                    getattr(event, "event_id", None),
                    exc,
                    exc_info=True,
                )

        return bus.subscribe(AuditEvent, _safe_on_audit_event, channel=_AUDIT_CHANNEL)

    @listen(AuditEvent, channel=_AUDIT_CHANNEL)
    async def on_audit_event(self, event: Event) -> None:
        """
        Persist an incoming ``AuditEvent`` as an ``AuditEntry``.

        Args:
            event: The incoming event — narrowed to ``AuditEvent`` by the
                   ``@listen(AuditEvent, ...)`` dispatch.

        Edge cases:
            - Non-``AuditEvent`` events cannot reach this handler because the
              bus filters by ``__event_type__ == "varco.audit"`` before dispatch.
            - If ``audit_repo.save`` fails, the exception propagates to the bus.
              Add ``retry_policy=RetryPolicy(...)`` to the ``@listen`` call for
              automatic retries on transient DB errors.
        """
        # The @listen(AuditEvent) filter guarantees this is always an AuditEvent.
        # The isinstance check is defensive and helps type narrowing.
        if not isinstance(event, AuditEvent):
            _logger.warning(
                "AuditConsumer received non-AuditEvent type=%r — ignoring",
                type(event).__name__,
            )
            return

        entry = AuditEntry.from_event(event)
        await self._audit_repo.save(entry)
        _logger.debug(
            "AuditConsumer: persisted audit entry_id=%s entity=%s/%s action=%s",
            entry.entry_id,
            entry.entity_type,
            entry.entity_id,
            entry.action,
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "AuditEntry",
    "AuditRepository",
    "AuditLogMixin",
    "AuditConsumer",
]
