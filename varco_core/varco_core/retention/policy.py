"""
varco_core.retention.policy
==============================
``RetentionPolicy`` + ``RetentionRegistry`` + ``RetentionOutcome`` /
``RetentionResult`` — Plan 039 (S20) / Steps 3-4.

DESIGN: a policy holds a ``RetentionTarget`` object, not a ``module:callable``
string (§D-S20-shape)
    ✅ ``varco_core`` never imports a backend — the app constructs
       ``DlqRetentionTarget(dlq=my_redis_dlq, ...)`` at wiring time and hands
       it to a ``RetentionPolicy``; the registry is an ordinary object the app
       owns, identical to ``bind_trust_store``'s already-constructed-object
       model.
    ✅ Only ``RetentionPolicy.name`` (a ``str``) is ever serialized into a
       ``Job`` payload (``{"policy": name}``) — ``TaskPayload``'s JSON
       constraint (``job/task.py:97-101``) is met with nothing to serialize.
    ✅ ``name`` doubles as the ``uuid5`` seed for the policy's
       ``Schedule.schedule_id`` (``schedule_id_for()``), which is what makes
       the materializer's cross-process ``uuid5`` convergence work across
       pods that never share memory (§D-S20-multiproc).
    ❌ A policy is not declarable from pure configuration (a YAML file cannot
       name a target). Accepted — the same is true of every ``bind_*`` verb,
       and a target needs a live repository handle anyway.
  Rejected — ``target: str`` resolved like the CLI's ``module:callable``:
    ❌ moves a startup wiring error to 03:00 in production; ❌ imports
    arbitrary modules inside a background task; ❌ the CLI does this only
    because a CLI has no container — a running app does.
  Rejected — a module-global ``_REGISTRY`` populated by a decorator:
    ⛔ process-global mutable state, wrong under two apps in one process.

DESIGN: §D-S20-safety's eight guards — a misconfigured window deletes
production data irreversibly, so most guards fire at construction/wiring,
never at 03:00
    ✅ ``dry_run: bool`` has **no default at all** — deliberately, over
       ``dry_run=True``. A default of ``True`` invites "everyone flips it in
       the first hour"; a required field makes every policy in every repo
       state its own blast radius in the source, greppable, forever.
    ✅ ``older_than <= 0`` → ``ValueError``, no escape hatch — a
       non-positive window is never valid.
    ✅ ``older_than < 24h`` → ``ValueError`` naming
       ``acknowledge_short_retention`` unless it is explicitly ``True`` —
       per-policy (Open question 3), not registry-wide, because a
       registry-wide flag would license every future policy in that
       registry to skip the floor.
    ✅ ``tenant_ids=()`` → ``ValueError`` — an empty scope is always a typo,
       never "no tenants" (there is no such state — see §D-S20-tenancy).
    ✅ ``older_than`` capability mismatch (present on a
       ``supports_older_than=False`` target, or absent on a
       ``supports_older_than=True`` one) → ``ValueError`` at
       ``RetentionRegistry.register()`` — wiring time, not run time.
    ✅ Duplicate policy ``name`` in one registry → ``ValueError`` at
       ``register()`` — a policy name doubles as the ``uuid5`` seed for a
       ``Schedule``; two policies with the same name would silently
       materialize onto the same schedule row.
    ❌ Eight guards (four more live in ``RetentionScheduler``/the six
       adapters) is a lot of ceremony for "delete old rows". Accepted: this
       is the only feature in the 3.2 extension set whose failure mode is
       irreversible data loss.

Thread safety:  ⚠️ ``RetentionRegistry.register()`` mutates a plain dict —
                   expected to run once, at startup, single-threaded (same
                   convention as ``TaskRegistry.register()``).
Async safety:   ✅ Everything here is synchronous — no I/O.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import NAMESPACE_URL, UUID, uuid5

if TYPE_CHECKING:
    from varco_core.retention.base import RetentionTarget

__all__ = ["RetentionOutcome", "RetentionPolicy", "RetentionRegistry", "RetentionResult"]

_SHORT_RETENTION_FLOOR = timedelta(hours=24)


@dataclass(frozen=True)
class RetentionPolicy:
    """
    A declared "prune X older than Y, on this schedule" intent.

    Attributes:
        name: Stable policy name — the ``uuid5`` seed for the materialized
            ``Schedule.schedule_id`` (``RetentionRegistry.schedule_id_for``)
            and the only value serialized into a ``Job`` payload. Renaming a
            policy between materialization and execution orphans an
            in-flight job (Pitfalls table) — treat it as stable identity,
            not a display label.
        target: A ``RetentionTarget`` the caller already constructed —
            never a string (§D-S20-shape above).
        cron_expr: A 5-field cron expression, unchanged semantics —
            parsed by ``varco_core.schedule.cron``, exactly like any other
            ``Schedule``.
        timezone: IANA zone name the cron expression is interpreted in.
        dry_run: **Required, no default.** ``True`` previews only
            (``RetentionOutcome.would_delete`` populated, ``deleted=0``);
            ``False`` actually deletes.
        older_than: The age cutoff, or ``None`` for a target whose verb has
            no cutoff concept (``delete_expired()``-backed targets). Its
            presence/absence is validated against the target's
            ``supports_older_than`` at ``RetentionRegistry.register()``, not
            here — this dataclass alone cannot see the target's class
            attribute values until construction is complete.
        enabled: A disabled policy is never materialized as a ``Schedule``.
        batch_size: Rows requested per ``RetentionTarget.purge()`` call —
            forwarded as ``limit=``.
        max_batches: Upper bound on ``purge()`` calls per occurrence — the
            blast-radius ceiling for one materialized run
            (``batch_size × max_batches``).
        acknowledge_short_retention: Required ``True`` to accept
            ``older_than < 24h`` — the per-policy escape hatch for guard 3.
        tenant_ids: ``None`` = an explicit, documented **platform-wide**
            purge (``tenant_id=None`` reaches the verb via ambient
            ``current_tenant()`` inside the target, since no
            ``tenant_context()`` is entered). A non-empty tuple scopes the
            sweep to exactly those tenants, one ``tenant_context()`` block
            each (§D-S20-tenancy). ``()`` is rejected — see the class
            docstring's guard list.

    Raises:
        ValueError: ``older_than`` is non-positive; ``older_than`` is under
            24h without ``acknowledge_short_retention=True``; or
            ``tenant_ids == ()``.

    Thread safety:  ✅ ``frozen=True`` — immutable after construction.
    """

    name: str
    target: RetentionTarget
    cron_expr: str
    timezone: str
    dry_run: bool

    older_than: timedelta | None = None
    enabled: bool = True
    batch_size: int = 1000
    max_batches: int = 100
    acknowledge_short_retention: bool = False
    tenant_ids: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if self.tenant_ids is not None and len(self.tenant_ids) == 0:
            raise ValueError(
                f"RetentionPolicy {self.name!r}: tenant_ids=() is never valid — "
                "an empty tuple is always a typo. Use tenant_ids=None for a "
                "platform-wide purge, or a non-empty tuple to scope it."
            )

        if self.older_than is not None:
            if self.older_than <= timedelta(0):
                raise ValueError(
                    f"RetentionPolicy {self.name!r}: older_than must be a positive "
                    f"timedelta, got {self.older_than!r} — there is no acknowledgement "
                    "escape hatch for a non-positive window."
                )
            if self.older_than < _SHORT_RETENTION_FLOOR and not self.acknowledge_short_retention:
                raise ValueError(
                    f"RetentionPolicy {self.name!r}: older_than={self.older_than!r} is "
                    "under 24h. Pass acknowledge_short_retention=True to confirm this "
                    "is intentional — a sub-day retention window is unusual enough to "
                    "require an explicit, greppable acknowledgement."
                )


@dataclass(frozen=True)
class RetentionOutcome:
    """
    The result of one ``RetentionTarget.purge()`` call — a single batch.

    Attributes:
        kind: The target's ``kind`` (``"dlq"``, ``"audit"``, ...).
        examined: Rows the target looked at, or ``None`` when the backend
            genuinely cannot report this (a ``delete_expired()``-backed
            target — Open question 2's answer: never a fake ``0``).
        deleted: Rows actually deleted this call. Always ``0`` when
            ``dry_run=True`` or when ``skipped_reason`` is set.
        would_delete: Rows that *would* have been deleted, populated only
            under ``dry_run=True`` on a ``supports_dry_run=True`` target —
            the real preview the CLI's whole-store ``count()`` never had.
        dry_run: Whether this call ran in preview mode.
        skipped_reason: Set (and ``deleted=0``, ``would_delete=0``) when the
            target refused to run under ``dry_run=True`` because it has no
            preview capability (``supports_dry_run=False``) — the single
            most important safety property in this plan: a dry run must
            never delete.

    Thread safety:  ✅ ``frozen=True``.
    """

    kind: str
    examined: int | None = None
    deleted: int = 0
    would_delete: int = 0
    dry_run: bool = False
    skipped_reason: str | None = None


@dataclass(frozen=True)
class RetentionResult:
    """
    The aggregate result of executing one ``RetentionPolicy`` once — across
    every batch and every tenant, up to ``max_batches``.

    Attributes:
        policy: The policy name this result belongs to.
        kind: The target's ``kind``.
        examined: Summed ``RetentionOutcome.examined`` across every batch,
            or ``None`` if any batch reported ``None`` (§Open question 2).
        deleted: Summed ``RetentionOutcome.deleted``.
        would_delete: Summed ``RetentionOutcome.would_delete``.
        batches: Number of ``purge()`` calls made.
        truncated: ``True`` when ``max_batches`` was reached while every
            batch was still full — the sweep stopped early and more rows
            may remain; the next scheduled occurrence continues.
        dry_run: The policy's own ``dry_run`` value.
        skipped_reason: Set when the target skipped (dry-run-unsupported).
        duration_s: Wall-clock seconds spent executing this policy.
        error: The ``str(exc)`` of any exception raised mid-sweep, or
            ``None``. The sweep does not re-raise — a broken policy must not
            take down an unrelated one in the same materializer pass; see
            ``RetentionScheduler``'s DESIGN block.

    Thread safety:  ✅ ``frozen=True``.
    """

    policy: str
    kind: str
    examined: int | None = None
    deleted: int = 0
    would_delete: int = 0
    batches: int = 0
    truncated: bool = False
    dry_run: bool = False
    skipped_reason: str | None = None
    duration_s: float = 0.0
    error: str | None = None


class RetentionRegistry:
    """
    An app-owned collection of named ``RetentionPolicy`` objects.

    Not a process-global singleton — construct one per application (same
    shape as ``TaskRegistry``), and pass it to ``bind_retention_registry``
    and/or ``RetentionScheduler`` explicitly.

    Thread safety:  ⚠️ ``register()`` mutates a plain dict — expected to run
                       once, at startup, single-threaded.
    Async safety:   ✅ Synchronous, no I/O.
    """

    def __init__(self) -> None:
        self._policies: dict[str, RetentionPolicy] = {}

    def register(self, policy: RetentionPolicy) -> None:
        """
        Add ``policy`` to the registry, validating its target-capability
        fit and name uniqueness (guards 4 and the duplicate-name guard —
        both wiring-time, not construction-time, because only the registry
        can see whether a name already exists).

        Args:
            policy: The ``RetentionPolicy`` to register.

        Raises:
            ValueError: ``policy.older_than`` is set but
                ``policy.target.supports_older_than`` is ``False``;
                ``policy.older_than`` is ``None`` but
                ``policy.target.supports_older_than`` is ``True``; or
                ``policy.name`` is already registered.
        """
        target = policy.target
        if policy.older_than is not None and not target.supports_older_than:
            raise ValueError(
                f"RetentionPolicy {policy.name!r}: older_than is set but "
                f"{type(target).__name__} (kind={target.kind!r}) does not support "
                "it — supports_older_than=False. This would be a silently "
                "ignored field; refusing at registration instead."
            )
        if policy.older_than is None and target.supports_older_than:
            raise ValueError(
                f"RetentionPolicy {policy.name!r}: older_than is required for "
                f"{type(target).__name__} (kind={target.kind!r}) — "
                "supports_older_than=True."
            )
        if policy.name in self._policies:
            raise ValueError(
                f"A RetentionPolicy named {policy.name!r} is already registered — "
                "policy names must be unique within one registry (the name is "
                "the uuid5 seed for the materialized Schedule)."
            )
        self._policies[policy.name] = policy

    def get(self, name: str) -> RetentionPolicy | None:
        """Return the policy named ``name``, or ``None`` if not registered."""
        return self._policies.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._policies

    def __iter__(self) -> Iterator[str]:
        """Iterate registered policy names."""
        return iter(self._policies)

    def schedule_id_for(self, name: str) -> UUID:
        """
        Deterministic ``schedule_id`` for policy ``name`` — the same
        ``uuid5`` scheme the materializer itself uses for occurrence job
        ids, applied one layer up: two ``RetentionScheduler`` instances
        (any number of pods) that both call ``ensure_schedules()`` converge
        on the identical ``Schedule.schedule_id``, which is what makes the
        materializer's own ``uuid5`` + upsert convergence (§D-S20-multiproc)
        apply to retention schedules with zero new coordination.

        Args:
            name: A policy name.

        Returns:
            A stable ``UUID``, deterministic for a given ``name``.
        """
        return uuid5(NAMESPACE_URL, f"varco:retention:policy:{name}")
