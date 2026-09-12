"""
Plan 036 (S11) / Phase 4, Step 26 — red-mode tests for
``varco_core.auth.audit.AuditingAuthorizer`` (§D-S11-shape, §D-S11-payload,
§D-S11-policy).

``varco_core/varco_core/auth/audit.py`` does not exist yet — every test
below is expected to fail with ``ModuleNotFoundError`` on first import.

Uses ``InMemoryEventBus`` + ``BusEventProducer`` per CLAUDE.md's Test
Conventions — a capturing ``AbstractEventProducer`` subclass is used
instead where the test needs to inspect exactly what was produced without
depending on how the audit event routes into ``AuditConsumer``/
``AuditRepository`` (open question 1 — unresolved at plan-writing time).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from varco_core.auth.base import AbstractAuthorizer, Action, AuthContext, Resource
from varco_core.event.base import Event
from varco_core.event.producer import AbstractEventProducer
from varco_core.exception.service import ServiceAuthorizationError
from varco_core.model import DomainModel


@dataclass
class _Widget(DomainModel):
    pk: int = 0
    name: str = ""


class _CapturingProducer(AbstractEventProducer):
    """Captures every produced event without touching a real bus."""

    def __init__(self) -> None:
        self.produced: list[tuple[Event, str]] = []

    async def _produce(self, event: Event, *, channel: str = "default") -> None:
        self.produced.append((event, channel))

    async def _produce_many(self, events: list[tuple[Event, str]]) -> None:
        self.produced.extend(events)


class _AllowAuthorizer(AbstractAuthorizer):
    async def authorize(self, ctx: AuthContext, action: Action, resource: Resource) -> None:
        return None


class _DenyAuthorizer(AbstractAuthorizer):
    async def authorize(self, ctx: AuthContext, action: Action, resource: Resource) -> None:
        raise ServiceAuthorizationError("read", reason="no grant")


def _payload_of(producer: _CapturingProducer) -> Any:
    """Extract the recorded audit payload from the single produced event,
    tolerant of exactly where the plan ends up storing the field dict
    (``event.payload`` is the existing ``Event`` convention)."""
    assert len(producer.produced) == 1, "expected exactly one audit record"
    event, _channel = producer.produced[0]
    return event.payload


async def test_denial_is_recorded_and_original_exception_reraised_unchanged() -> None:
    from varco_core.auth.audit import AuditingAuthorizer

    producer = _CapturingProducer()
    authorizer = AuditingAuthorizer(_DenyAuthorizer(), producer)

    ctx = AuthContext(user_id="usr_1")
    resource = Resource(entity_type=_Widget, entity=_Widget(pk=1, name="a"))

    with pytest.raises(ServiceAuthorizationError) as exc_info:
        await authorizer.authorize(ctx, Action.READ, resource)

    # The original exception propagates byte-identical.
    assert isinstance(exc_info.value, ServiceAuthorizationError)

    assert len(producer.produced) == 1
    payload = _payload_of(producer)
    assert payload["decision"] == "deny"


async def test_allow_with_no_actor_is_not_recorded_under_denials_policy() -> None:
    """§D-S11-policy default (DENIALS): an allow whose ctx carries no actor
    must not be recorded at all."""
    from varco_core.auth.audit import AuditingAuthorizer

    producer = _CapturingProducer()
    authorizer = AuditingAuthorizer(_AllowAuthorizer(), producer)

    ctx = AuthContext(user_id="usr_1")  # no "actor" in metadata
    resource = Resource(entity_type=_Widget, entity=_Widget(pk=1, name="a"))

    await authorizer.authorize(ctx, Action.READ, resource)

    assert producer.produced == []


async def test_allow_with_actor_is_recorded_under_denials_policy() -> None:
    """§D-S11-policy default (DENIALS): an allow whose ctx DOES carry an
    actor (delegation) must be recorded — the CVE-2025-55241 property."""
    from varco_core.auth.audit import AuditingAuthorizer

    producer = _CapturingProducer()
    authorizer = AuditingAuthorizer(_AllowAuthorizer(), producer)

    ctx = AuthContext(user_id="usr_1", metadata={"actor": "svc_backend"})
    resource = Resource(entity_type=_Widget, entity=_Widget(pk=1, name="a"))

    await authorizer.authorize(ctx, Action.READ, resource)

    assert len(producer.produced) == 1
    payload = _payload_of(producer)
    assert payload["decision"] == "allow"
    assert payload["actor_id"] == "svc_backend"


async def test_policy_all_records_every_decision_including_plain_allow() -> None:
    from varco_core.auth.audit import AuditDecisionPolicy, AuditingAuthorizer

    producer = _CapturingProducer()
    authorizer = AuditingAuthorizer(_AllowAuthorizer(), producer, policy=AuditDecisionPolicy.ALL)

    ctx = AuthContext(user_id="usr_1")  # no actor
    resource = Resource(entity_type=_Widget, entity=_Widget(pk=1, name="a"))

    await authorizer.authorize(ctx, Action.READ, resource)

    assert len(producer.produced) == 1
    payload = _payload_of(producer)
    assert payload["decision"] == "allow"


async def test_policy_none_records_nothing_but_delegate_behaviour_is_identical() -> None:
    from varco_core.auth.audit import AuditDecisionPolicy, AuditingAuthorizer

    producer = _CapturingProducer()

    allow_authorizer = AuditingAuthorizer(
        _AllowAuthorizer(), producer, policy=AuditDecisionPolicy.NONE
    )
    deny_authorizer = AuditingAuthorizer(
        _DenyAuthorizer(), producer, policy=AuditDecisionPolicy.NONE
    )

    ctx = AuthContext(user_id="usr_1", metadata={"actor": "svc_backend"})
    resource = Resource(entity_type=_Widget, entity=_Widget(pk=1, name="a"))

    # Allow still allows.
    await allow_authorizer.authorize(ctx, Action.READ, resource)
    # Deny still denies.
    with pytest.raises(ServiceAuthorizationError):
        await deny_authorizer.authorize(ctx, Action.READ, resource)

    assert producer.produced == []


async def test_payload_never_contains_excluded_fields() -> None:
    """§D-S11-payload: written as a field ALLOWLIST so a future field
    addition fails this test until it is deliberately argued into the
    allowlist. Excluded: denial reason/str(exc), resource.entity,
    ctx.grants, ctx.roles, raw token, ctx.metadata wholesale, vars(exc)."""
    from varco_core.auth.audit import AuditingAuthorizer

    producer = _CapturingProducer()
    authorizer = AuditingAuthorizer(_DenyAuthorizer(), producer)

    ctx = AuthContext(
        user_id="usr_1",
        roles=frozenset({"editor"}),
        metadata={"actor": "svc_backend", "raw_token": "eyJhbGciOi...super-secret"},
    )
    resource = Resource(entity_type=_Widget, entity=_Widget(pk=1, name="sensitive-name"))

    with pytest.raises(ServiceAuthorizationError):
        await authorizer.authorize(ctx, Action.READ, resource)

    payload = _payload_of(producer)

    allowlist = {
        "principal_id",
        "actor_id",
        "tenant_id",
        "action",
        "entity_type",
        "entity_pk",
        "is_collection",
        "decision",
        "denial_type",
        "correlation_id",
    }
    assert set(payload.keys()) <= allowlist

    # Explicit negative checks for the named exfiltration surfaces.
    assert "reason" not in payload
    assert "no grant" not in str(payload.values())  # denial reason / str(exc)
    assert "sensitive-name" not in str(payload.values())  # resource.entity
    assert "roles" not in payload
    assert "grants" not in payload
    assert "eyJhbGciOi...super-secret" not in str(payload.values())  # raw token
    assert "metadata" not in payload


async def test_collection_resource_payload_has_none_entity_pk_and_is_collection_true() -> None:
    """Edge case from the plan: entity_pk is None and is_collection is True
    for a collection-level resource (entity=None)."""
    from varco_core.auth.audit import AuditDecisionPolicy, AuditingAuthorizer

    producer = _CapturingProducer()
    authorizer = AuditingAuthorizer(_AllowAuthorizer(), producer, policy=AuditDecisionPolicy.ALL)

    ctx = AuthContext(user_id="usr_1")
    resource = Resource(entity_type=_Widget)  # collection-level, entity=None

    await authorizer.authorize(ctx, Action.LIST, resource)

    payload = _payload_of(producer)
    assert payload["is_collection"] is True
    assert payload["entity_pk"] is None
